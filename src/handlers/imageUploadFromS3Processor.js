const { S3Client, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { Upload } = require('@aws-sdk/lib-storage');
const logger = require('../utils/logger');
const dynamoService = require('../services/dynamoService');
const { COMPLETED, FAILED } = require('../utils/config');
const { fetchExpiresAt } = require('../utils/api/api');
const JobProgressService = require('../services/jobProgressService');
const { JobProcessingError } = require('../utils/errors');
const secretsService = require('../services/secretsService');

// Constants for batch processing and retries
const BATCH_SIZE = 25;
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

const jobProgressService = new JobProgressService(docClient, null, {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
});

exports.handler = async (event) => {
    logger.info('S3 Image upload processor started', {
        event,
        recordsCount: event.Records?.length || 0,
        timestamp: new Date().toISOString()
    });

    const results = await Promise.allSettled(event.Records.map(processRecord));

    // Log summary of processing results
    const summary = results.reduce((acc, result) => {
        acc[result.status]++;
        return acc;
    }, { fulfilled: 0, rejected: 0 });

    logger.info('Processing complete', { summary });

    async function processRecord(record) {
        try {
            logger.info('Starting to process record', {
                recordId: record.messageId,
                timestamp: new Date().toISOString()
            });

            const parsedBody = JSON.parse(record.body);
            logger.info('Successfully parsed record body', {
                recordId: record.messageId,
                parsedBodyKeys: Object.keys(parsedBody)
            });

            const {
                userId,
                projectId,
                jobId,
                projectSettingId,
                bucketName,
                region,
                importAll,
                folderPaths,
                projectSetting,
                expressProcessing
            } = parsedBody;

            logger.info('Processing record', {
                userId,
                projectId,
                jobId,
                bucketName,
                importAll,
                folderPaths,
                expressProcessing
            });

            // Initialize source S3 client with user's bucket region and acceleration if needed
            const sourceS3Client = new S3Client({
                region,
                // useAccelerateEndpoint: expressProcessing
            });
            console.log('----> sourceS3Client', sourceS3Client);

            // Initialize destination S3 client with acceleration if needed
            const destinationS3Client = new S3Client({
                useAccelerateEndpoint: expressProcessing
            });
            console.log('----> destinationS3Client', destinationS3Client);

            const destinationBucket = process.env.IMAGE_BUCKET;

            logger.info('Initializing S3 clients', {
                sourceRegion: region,
                expressProcessing,
                destinationBucket: process.env.IMAGE_BUCKET
            });

            // After S3 client initialization
            logger.info('S3 clients initialized successfully', {
                sourceClientConfig: sourceS3Client.config,
                destinationClientConfig: destinationS3Client.config
            });

            // Before fetching expiration
            logger.info('Fetching job expiration time', { jobId });
            const expiresAt = await fetchExpiresAt(jobId);
            logger.info('Job expiration time fetched', { jobId, expiresAt });

            // Process images in batches
            let continuationToken = undefined;
            let successCount = 0;
            let failedUploads = [];
            let skippedUploads = [];

            do {
                logger.info('Starting batch processing', {
                    jobId,
                    continuationToken,
                    timestamp: new Date().toISOString()
                });

                const listResult = await listImagesFromBucket(
                    sourceS3Client,
                    bucketName,
                    continuationToken,
                    importAll ? null : folderPaths,
                    region
                );

                logger.info('Listed images from bucket', {
                    imageCount: listResult.images?.length || 0,
                    hasNextToken: !!listResult.nextToken,
                    error: listResult.error
                });

                if (listResult.error) {
                    logger.error('listImagesFromBucket error', {
                        images: listResult.images,
                        nextToken: listResult.nextToken,
                        error: listResult.error
                    });
                }

                if (listResult.images.length === 0) {
                    // logger.info('No images found in the bucket/folders');
                    console.log('----> No images found in the bucket/folders');
                    break;
                }

                // Process batch of images
                const batchResults = await processImageBatch(
                    sourceS3Client,
                    destinationS3Client,
                    listResult.images,
                    {
                        sourceBucket: bucketName,
                        destinationBucket,
                        userId,
                        projectId,
                        projectSettingId,
                        jobId
                    }
                );

                // Aggregate results
                successCount += batchResults.successCount;
                failedUploads.push(...batchResults.failedUploads);
                skippedUploads.push(...batchResults.skippedUploads);

                continuationToken = listResult.nextToken;

                // console.log('----> batchResults', {
                //     successCount: batchResults.successCount,
                //     failedUploads: batchResults.failedUploads,
                //     skippedUploads: batchResults.skippedUploads,
                //     hasMoreImages: !!nextToken
                // });
                logger.info('Batch processing complete', {
                    batchSize: listResult.images.length,
                    successCount: batchResults.successCount,
                    failedCount: batchResults.failedUploads.length,
                    skippedCount: batchResults.skippedUploads.length,
                    hasMoreImages: !!listResult.nextToken
                });

            } while (continuationToken);

            // Update failed tasks status
            const failedImages = [...failedUploads, ...skippedUploads];
            if (failedImages.length > 0) {
                await updateFailedTasksStatus(failedImages, jobId, expiresAt);
            }

            logger.info('Processing completed', {
                totalSuccess: successCount,
                totalFailed: failedUploads.length,
                totalSkipped: skippedUploads.length
            });

        } catch (error) {
            logger.error('Critical error in processRecord', {
                error: {
                    name: error.name,
                    message: error.message,
                    stack: error.stack,
                    code: error.code,
                    metadata: error.$metadata,
                },
                record: {
                    id: record.messageId,
                    body: record.body
                },
                timestamp: new Date().toISOString()
            });

            jobProgressService.updateJobProgress(jobId, {
                systemErrors: [{
                    timestamp: Date.now().toString(),
                    error: error.message,
                    code: error.code
                }]
            });
            throw error;
        }
    }
}

async function listImagesFromBucket(s3Client, bucket, continuationToken, folderPaths, region) {
    logger.info('Starting listImagesFromBucket', {
        bucket,
        continuationToken,
        folderPathsCount: folderPaths?.length,
        region,
        timestamp: new Date().toISOString()
    });

    try {
        // Get credentials using the shared service
        const credentials = await secretsService.getCredentials();

        // Create cross-account client
        const crossAccountS3Client = new S3Client({
            credentials: {
                accessKeyId: credentials.accessKeyId,
                secretAccessKey: credentials.secretAccessKey
            },
            region
        });

        const baseCommand = {
            Bucket: bucket,
            MaxKeys: BATCH_SIZE,
            ContinuationToken: continuationToken
        };

        console.log('----> folderPaths', folderPaths);

        // If specific folders are provided, process them one at a time
        if (folderPaths?.length > 0) {
            const allImages = [];
            let nextContinuationToken = continuationToken;

            for (const folderPath of folderPaths) {
                let folderContinuationToken = nextContinuationToken;
                console.log('----> current folderPath', folderPath);

                do {
                    const command = new ListObjectsV2Command({
                        ...baseCommand,
                        Prefix: folderPath.endsWith('/') ? folderPath : `${folderPath}/`,
                        ContinuationToken: folderContinuationToken
                    });

                    const response = await crossAccountS3Client.send(command);
                    const folderImages = response.Contents?.filter(obj =>
                        obj.Key.match(/\.(jpg|jpeg|png|gif|webp)$/i)
                    ) || [];

                    allImages.push(...folderImages);
                    folderContinuationToken = response.NextContinuationToken;
                    console.log('----> folderContinuationToken', folderContinuationToken);
                } while (folderContinuationToken);
            }

            // Return batch-sized chunk of images
            const startIndex = 0;
            const endIndex = Math.min(BATCH_SIZE, allImages.length);
            const nextBatch = allImages.slice(startIndex, endIndex);
            const remainingImages = allImages.length > BATCH_SIZE;
            console.log('----> remainingImages', remainingImages);
            return {
                images: nextBatch,
                nextToken: remainingImages ? 'continue' : null
            };
        }

        // If no specific folders, list all objects
        const command = new ListObjectsV2Command(baseCommand);
        const response = await crossAccountS3Client.send(command);
        const images = response.Contents?.filter(obj =>
            obj.Key.match(/\.(jpg|jpeg|png|webp)$/i)
        ) || [];

        return {
            images,
            nextToken: response.NextContinuationToken
        };

    } catch (error) {
        logger.error('Error in listImagesFromBucket', {
            error: {
                name: error.name,
                message: error.message,
                stack: error.stack,
                metadata: error.$metadata,
                requestId: error.$metadata?.requestId,
                httpStatusCode: error.$metadata?.httpStatusCode
            },
            context: {
                bucket,
                folderPaths,
                region
            },
            timestamp: new Date().toISOString()
        });

        if (error.name === 'AccessDenied' || error.$metadata?.httpStatusCode === 403) {
            throw new JobProcessingError(403, 'No permission to access the specified bucket or folder');
        }

        throw error;
    }
}

async function processImageBatch(sourceS3Client, destS3Client, images, config) {
    // Get credentials from Secrets Manager for cross-account access
    const credentials = await secretsService.getCredentials();

    // Create cross-account client
    const crossAccountS3Client = new S3Client({
        credentials: {
            accessKeyId: credentials.accessKeyId,
            secretAccessKey: credentials.secretAccessKey
        },
        region: sourceS3Client.config.region
    });

    logger.info('Created cross-account S3 client for individual image processing', {
        hasCredentials: !!crossAccountS3Client.config.credentials,
        region: crossAccountS3Client.config.region
    });

    logger.info('Starting processImageBatch', {
        imageCount: images.length,
        config: {
            ...config,
            sourceBucket: config.sourceBucket,
            destinationBucket: config.destinationBucket
        }
    });

    const results = {
        successCount: 0,
        failedUploads: [],
        skippedUploads: []
    };

    console.log('----> processImageBatch', {
        images,
        config
    });

    await Promise.all(images.map(async (image) => {
        logger.info('Processing individual image', {
            imageKey: image.Key,
            attempt: 1,
            timestamp: new Date().toISOString()
        });

        let attempt = 1;
        let success = false;

        while (attempt <= MAX_RETRIES && !success) {
            try {
                logger.info('Attempting image upload', {
                    imageKey: image.Key,
                    attempt,
                    timestamp: new Date().toISOString()
                });

                const sourceKey = image.Key;
                const destinationKey = `uploads/${config.userId}/${config.projectId}/` +
                    `${config.projectSettingId}/${config.jobId}/${sourceKey}`;

                // Get image from source bucket
                const getCommand = new GetObjectCommand({
                    Bucket: config.sourceBucket,
                    Key: sourceKey
                });

                const sourceObject = await crossAccountS3Client.send(getCommand);

                // Upload to destination bucket
                const upload = new Upload({
                    client: destS3Client,
                    params: {
                        Bucket: config.destinationBucket,
                        Key: destinationKey,
                        Body: sourceObject.Body,
                        ContentType: sourceObject.ContentType
                    }
                });

                await upload.done();
                results.successCount++;
                success = true;

                console.log('----> success', {
                    successCount: results.successCount
                });

                logger.info('Successfully uploaded image', {
                    imageKey: image.Key,
                    attempt,
                    destinationKey
                });

            } catch (error) {
                if (attempt === MAX_RETRIES) {
                    const failedUpload = {
                        fileName: image.Key,
                        reason: error.message,
                        attempt
                    };

                    if (error.name === 'NoSuchBucket' || error.name === 'AccessDenied') {
                        results.skippedUploads.push(failedUpload);
                    } else {
                        results.failedUploads.push(failedUpload);
                    }
                }

                attempt++;
                if (attempt <= MAX_RETRIES) {
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * attempt));
                }

                logger.error('Error processing image', {
                    error: {
                        name: error.name,
                        message: error.message,
                        stack: error.stack
                    },
                    context: {
                        imageKey: image.Key,
                        attempt,
                        maxRetries: MAX_RETRIES
                    }
                });

                //TODO: update to task table for each item as failed
                await updateFailedTasksStatus([
                    {
                        fileName: image.Key,
                        reason: error.message,
                        attempt
                    }
                ], config.jobId, config.expiresAt);
            }
        }
    }));

    return results;
}

async function updateFailedTasksStatus(failedImages, jobId, expiresAt) {
    const updatePromises = failedImages.map(upload =>
        dynamoService.updateTaskStatus({
            jobId,
            taskId: upload.fileName,
            status: COMPLETED,
            evaluation: FAILED,
            reason: `${upload.reason} - ${upload.attempt} attempts`,
            expirationTime: expiresAt
        })
    );

    await Promise.all(updatePromises);
}
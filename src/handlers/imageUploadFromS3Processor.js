const { S3Client, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const logger = require('../utils/logger');
const dynamoService = require('../services/dynamoService');
const { COMPLETED, FAILED } = require('../utils/config');
const { fetchExpiresAt } = require('../utils/api/api');

// Constants for batch processing and retries
const BATCH_SIZE = 25;
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

// Initialize S3 client (for our bucket)
const s3Client = new S3Client();

exports.handler = async (event) => {
    logger.info('S3 Image upload processor started', { event });
    const results = await Promise.allSettled(event.Records.map(processRecord));

    // Log summary of processing results
    const summary = results.reduce((acc, result) => {
        acc[result.status]++;
        return acc;
    }, { fulfilled: 0, rejected: 0 });

    logger.info('Processing complete', { summary });
};

async function processRecord(record) {
    const parsedBody = JSON.parse(record.body);
    if (!parsedBody) return;

    const { userId, projectId, jobId, projectSettingId, s3Connection } = parsedBody;
    logger.info('Processing record', { userId, projectId, jobId, s3Connection });

    try {
        // Initialize source S3 client with user's bucket region
        const sourceS3Client = new S3Client({ region: s3Connection.region });
        const destinationBucket = process.env.IMAGE_BUCKET;

        // Fetch job expiration time
        const expiresAt = await fetchExpiresAt(jobId);

        // Process images in batches
        let continuationToken = undefined;
        let successCount = 0;
        let failedUploads = [];
        let skippedUploads = [];

        do {
            const { images, nextToken } = await listImagesFromBucket(
                sourceS3Client,
                s3Connection.bucketName,
                continuationToken
            );

            if (images.length === 0) {
                logger.info('No images found in the bucket');
                break;
            }

            // Process batch of images
            const batchResults = await processImageBatch(
                sourceS3Client,
                s3Client,
                images,
                {
                    sourceBucket: s3Connection.bucketName,
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

            continuationToken = nextToken;

            logger.info('Batch processing complete', {
                batchSize: images.length,
                successCount: batchResults.successCount,
                failedCount: batchResults.failedUploads.length,
                skippedCount: batchResults.skippedUploads.length,
                hasMoreImages: !!nextToken
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
        logger.error('Error processing S3 upload', {
            error: {
                message: error.message,
                stack: error.stack,
                name: error.name
            },
            parsedBody
        });
        throw error;
    }
}

async function listImagesFromBucket(s3Client, bucket, continuationToken) {
    try {
        const command = new ListObjectsV2Command({
            Bucket: bucket,
            MaxKeys: BATCH_SIZE,
            ContinuationToken: continuationToken
        });

        const response = await s3Client.send(command);
        const images = response.Contents.filter(obj =>
            obj.Key.match(/\.(jpg|jpeg|png|gif|webp)$/i)
        );

        return {
            images,
            nextToken: response.NextContinuationToken
        };
    } catch (error) {
        logger.error('Error listing images from bucket', {
            error: error.message,
            bucket
        });
        throw error;
    }
}

async function processImageBatch(sourceS3Client, destS3Client, images, config) {
    const results = {
        successCount: 0,
        failedUploads: [],
        skippedUploads: []
    };

    await Promise.all(images.map(async (image) => {
        let attempt = 1;
        let success = false;

        while (attempt <= MAX_RETRIES && !success) {
            try {
                const sourceKey = image.Key;
                const destinationKey = `uploads/${config.userId}/${config.projectId}/` +
                    `${config.projectSettingId}/${config.jobId}/${sourceKey}`;

                // Get image from source bucket
                const getCommand = new GetObjectCommand({
                    Bucket: config.sourceBucket,
                    Key: sourceKey
                });

                const sourceObject = await sourceS3Client.send(getCommand);

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
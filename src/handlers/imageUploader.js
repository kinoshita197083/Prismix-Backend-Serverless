const { S3Client } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { createClient } = require('@supabase/supabase-js')
const logger = require('../utils/logger');
const { fetchGoogleRefreshToken, getAllImagesFromDrive, processImageBatch, setUpGoogleDriveClient } = require('../utils/googleDrive/googleDrive');
const dynamoService = require('../services/dynamoService');
const { FAILED, COMPLETED } = require('../utils/config');
const { fetchExpiresAt } = require('../utils/api/api');
const { parseRecordBody } = require('../utils/helpers');
const { Upload } = require('@aws-sdk/lib-storage');

const s3Client = new S3Client();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

const dynamoDb = new DynamoDBClient({ region: process.env.AWS_REGION });
const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoDb, {
    marshallOptions: {
        removeUndefinedValues: true
    }
});

// Configuration constants
const BATCH_SIZE = 150;          // Increased from original
const CONCURRENT_UPLOADS = 40;    // Added parallel processing
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000;

exports.handler = async (event) => {
    logger.info('Image uploader started', { event });
    // await Promise.all(event.Records.map(processRecord));
    for (const record of event.Records) {
        await processRecord(record);
    }
};

async function processRecord(record) {
    const parsedBody = parseRecordBody(record);
    if (!parsedBody) return;

    const { userId, projectId, jobId, projectSettingId, driveIds, folderIds, provider } = parsedBody;
    console.log('----> Extracted values:', { userId, projectId, jobId, projectSettingId, driveIds, folderIds, provider });

    try {
        const bucketName = process.env.IMAGE_BUCKET;
        const googleRefreshToken = await fetchGoogleRefreshToken(userId, supabase);
        const drive = setUpGoogleDriveClient(googleRefreshToken);

        const images = await getAllImagesFromDrive({ drive, driveIds, folderIds });
        if (images.length === 0) {
            return logger.info('No images found in the specified drive');
        }

        const results = await processImageBatches(images, drive, s3Client, userId, projectId, projectSettingId, jobId, bucketName);
        const { successCount, failedUploads, skippedUploads } = analyzeResults(results);

        // await updateJobProgress(jobId, failedUploads, skippedUploads, images.length);
        // Update task status as COMPLETED with FAILED evaluation for downstream aggregation in job summary
        const failedImages = [...failedUploads, ...skippedUploads];

        if (failedImages.length > 0) {
            console.log('----> Updating task status as COMPLETED with FAILED evaluation for downstream aggregation in job summary', { failedImages });

            // Fetch the expiresAt for the job from the job progress table
            const expiresAt = await fetchExpiresAt(jobId);

            const updatePromises = failedImages.map(async (upload) => {
                return dynamoService.updateTaskStatus({
                    jobId,
                    taskId: upload.fileName,
                    status: COMPLETED,
                    evaluation: FAILED,
                    reason: `${upload.reason} - ${upload.attempt} attempts`,
                    expirationTime: expiresAt
                });
            });
            await Promise.all(updatePromises);
        }

        await updateProcessedDriveInfo(jobId, driveIds, folderIds);

        console.log(`----> Image uploader processed successfully for ${successCount.length} images, failed ${failedUploads.length} and skipped ${skippedUploads.length}`);
    } catch (error) {
        console.error('Error processing image upload', {
            error: {
                message: error.message,
                stack: error.stack,
                name: error.name
            },
            parsedBody
        });
    }
}

async function processImageBatches(images, drive, s3Client, userId, projectId, projectSettingId, jobId, bucketName) {
    const results = [];
    const chunks = chunk(images, BATCH_SIZE);

    for (const batch of chunks) {
        try {
            // Process images in concurrent groups
            const concurrentGroups = chunk(batch, CONCURRENT_UPLOADS);

            for (const group of concurrentGroups) {
                const batchPromises = group.map(image =>
                    processImageWithRetry(
                        image,
                        drive,
                        s3Client,
                        userId,
                        projectId,
                        projectSettingId,
                        jobId,
                        bucketName
                    )
                );

                const batchResults = await Promise.all(batchPromises);
                results.push(...batchResults);
            }
        } catch (error) {
            logger.error('Error processing image batch', { error, batchSize: batch.length });
        }
    }

    return results;
}

async function processImageWithRetry(image, drive, s3Client, userId, projectId, projectSettingId, jobId, bucketName) {
    let attempt = 1;

    while (attempt <= MAX_RETRIES) {
        try {
            const fileStream = await drive.files.get(
                { fileId: image.id, alt: 'media' },
                { responseType: 'stream' }
            );

            const key = `uploads/${userId}/${projectId}/${projectSettingId}/${jobId}/${image.name}`;

            // Use multipart upload for better performance
            const upload = new Upload({
                client: s3Client,
                params: {
                    Bucket: bucketName,
                    Key: key,
                    Body: fileStream.data,
                    ContentType: image.mimeType
                },
                queueSize: 4,               // Number of concurrent multipart uploads
                partSize: 5 * 1024 * 1024   // 5MB parts for better throughput
            });

            await upload.done();

            return {
                success: true,
                fileName: image.name,
                attemptCount: attempt
            };

        } catch (error) {
            if (attempt === MAX_RETRIES) {
                return {
                    success: false,
                    fileName: image.name,
                    error: error.message,
                    attemptCount: attempt
                };
            }

            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * attempt));
            attempt++;
        }
    }
}

// Helper function to chunk array
function chunk(array, size) {
    return Array.from(
        { length: Math.ceil(array.length / size) },
        (_, index) => array.slice(index * size, (index + 1) * size)
    );
}

// Optimize the results analysis
function analyzeResults(results) {
    return results.reduce((acc, result) => {
        const category = result.success ? 'successCount' :
            (result.skipped ? 'skippedUploads' : 'failedUploads');

        acc[category].push({
            fileName: result.fileName,
            ...(result.success ? { attempt: result.attemptCount } : {
                reason: result.error,
                attempt: result.attemptCount
            })
        });

        return acc;
    }, { successCount: [], failedUploads: [], skippedUploads: [] });
}

// Optimize the drive info update
async function updateProcessedDriveInfo(jobId, driveIds, folderIds) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET processedDriveIds = :d, processedFolderIds = :f',
        ExpressionAttributeValues: {
            ':d': driveIds,
            ':f': folderIds
        }
    };

    try {
        await dynamoDbDocumentClient.send(new UpdateCommand(params));
    } catch (error) {
        logger.error('Failed to update processed drive info', { error, jobId });
        // Continue processing even if update fails
    }
}

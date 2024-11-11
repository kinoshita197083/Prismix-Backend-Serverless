const { S3Client } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { createClient } = require('@supabase/supabase-js')
const logger = require('../utils/logger');
const { fetchGoogleRefreshToken, getAllImagesFromDrive, processImageBatch, setUpGoogleDriveClient } = require('../utils/googleDrive/googleDrive');
const dynamoService = require('../services/dynamoService');
const { FAILED, COMPLETED } = require('../utils/config');
const { fetchPreserveFileDays } = require('../utils/api/api');

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

exports.handler = async (event) => {
    logger.info('Image uploader started', { event });
    await Promise.all(event.Records.map(processRecord));
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

            // Fetch the preserve file days for the job from the job progress table
            const preserveFileDays = await fetchPreserveFileDays(jobId); // User defined TTL for the file

            const updatePromises = failedImages.map(async (upload) => {
                return dynamoService.updateTaskStatus({
                    jobId,
                    taskId: upload.fileName,
                    status: COMPLETED,
                    evaluation: FAILED,
                    reason: `${upload.reason} - ${upload.attempt} attempts`,
                    preserveFileDays: preserveFileDays
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

function parseRecordBody(record) {
    try {
        return JSON.parse(record.body);
    } catch (error) {
        logger.error('Failed to parse record body', { error, record });
        return null;
    }
}

async function processImageBatches(images, drive, s3Client, userId, projectId, projectSettingId, jobId, bucketName) {
    const batchSize = 10; // Adjust this value based on your requirements
    const results = [];

    for (let i = 0; i < images.length; i += batchSize) {
        const batch = images.slice(i, i + batchSize);
        try {
            const batchResults = await processImageBatch(batch, drive, s3Client, userId, projectId, projectSettingId, jobId, bucketName);
            results.push(...batchResults);
        } catch (error) {
            logger.error('Error processing image batch', { error, batchStart: i, batchSize });
        }
    }

    return results;
}

function analyzeResults(results) {
    return results.reduce((acc, result) => {
        console.log('----> Analyzing result', { result });
        if (result.success) {
            acc.successCount.push({
                fileName: result.fileName,
                attempt: result.attemptCount,
            });
        } else if (result.skipped) {
            acc.skippedUploads.push({
                fileName: result.fileName,
                error: result.error,
                reason: result.reason,
                attempt: result.attemptCount,
            });
        } else {
            acc.failedUploads.push({
                fileName: result.fileName,
                reason: result.error,
                attempt: result.attemptCount,
            });
        }
        return acc;
    }, { successCount: [], failedUploads: [], skippedUploads: [] });
}

// async function updateJobProgress(jobId, failedUploads, skippedUploads, totalImages) {
//     const uploadDetails = {
//         failedUploads,
//         skippedUploads
//     }
//     const params = {
//         TableName: process.env.JOB_PROGRESS_TABLE,
//         Key: { JobId: jobId },
//         UpdateExpression: 'SET uploadDetails = :ud',
//         ExpressionAttributeValues: {
//             ':ud': uploadDetails
//         }
//     };

//     try {
//         await dynamoDbDocumentClient.send(new UpdateCommand(params));
//     } catch (error) {
//         logger.error('Failed to update job progress', { error, jobId });
//     }
// }

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
    }
}

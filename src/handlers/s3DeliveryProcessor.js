const { S3Client, GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const JobProgressService = require('../services/jobProgressService');
const createTaskService = require('../services/taskService');
const { FAILED, COMPLETED, IN_PROGRESS } = require('../utils/config');
const secretsService = require('../services/secretsService');
const supabaseService = require('../services/supabaseService');
const { Upload } = require('@aws-sdk/lib-storage');

// Enhanced constants with documentation
const CONFIG = {
    BATCH_SIZE: 50,
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000,
    CONCURRENT_UPLOADS: 10, // Control parallel uploads
    CHUNK_SIZE: 5 * 1024 * 1024 // 5MB chunks for multipart upload
};

// Initialize clients
const s3Client = new S3Client();
const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

// Initialize services
const jobProgressService = new JobProgressService(docClient, null, {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
});
const taskService = createTaskService();

exports.handler = async (event) => {
    logger.info('S3 delivery processor triggered', event);
    logger.info('Processing S3 delivery messages', { messageCount: event.Records.length });
    const batchItemFailures = [];

    for (const record of event.Records) {
        try {
            await processRecord(record);
        } catch (error) {
            logger.error('Fatal error processing record', { error, record });
            batchItemFailures.push({ itemIdentifier: record.messageId });
        }
    }

    return { batchItemFailures };
};

async function processRecord(record) {
    const body = JSON.parse(record.body);
    const message = JSON.parse(body.Message);
    const { jobId } = message;

    logger.info('Starting record processing', {
        jobId,
        messageId: record.messageId,
        timestamp: new Date().toISOString()
    });

    try {
        // Fetch job details and validate
        const jobDetails = await getJobProgressDetails(jobId);
        if (!jobDetails) {
            throw new Error(`No job details found for ID: ${jobId}`);
        }

        if (jobDetails.outputConnection !== 's3') {
            logger.info('Skipping non-S3 job', { jobId, outputConnection: jobDetails.outputConnection });
            return;
        }

        // Get destination bucket details
        const destinationDetails = await getDestinationDetails(jobDetails.userId);
        if (!destinationDetails?.s3Connection) {
            await handleFatalError(jobId, 'No S3 connection configuration found');
            return;
        }

        const { bucketName, region } = destinationDetails.s3Connection;
        if (!bucketName || !region) {
            await handleFatalError(jobId, 'Invalid S3 configuration - missing bucket name or region');
            return;
        }
        console.log('destinationDetails', destinationDetails);

        // Create destination directory name with timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const destinationPrefix = `prismix-${jobId}-${timestamp}`;

        // Create destination S3 client
        // Get credentials from Secrets Manager
        const credentials = await secretsService.getCredentials();

        // Destination S3 Bcuket config
        const s3Config = {
            bucketName,
            region,
            destinationPrefix
        };

        // Enhanced S3 client creation with retry strategy
        const destinationS3Client = new S3Client({
            credentials: {
                accessKeyId: credentials.accessKeyId,
                secretAccessKey: credentials.secretAccessKey
            },
            region: s3Config.region,
            maxAttempts: CONFIG.MAX_RETRIES,
            retryMode: 'adaptive'
        });

        // Validate S3 access before processing
        try {
            await validateS3Access(destinationS3Client, bucketName);
        } catch (error) {
            if (error.name === 'AccessDenied') {
                await handleFatalError(jobId, 'S3 bucket access denied - please check bucket policy');
            } else if (error.name === 'NoSuchBucket') {
                await handleFatalError(jobId, 'S3 bucket not found - please check bucket configuration');
            } else {
                await handleFatalError(jobId, `S3 access validation failed: ${error.message}`);
            }
            return;
        }

        // Process tasks in batches
        const { processedCount, failedCount, failedTasks } = await processTasks({ jobId, s3Config, destinationS3Client });

        // Update final job status
        await updateFinalJobStatus(jobId, processedCount, failedCount, failedTasks);

    } catch (error) {
        logger.error('Record processing failed', {
            jobId,
            error: error.message,
            stackTrace: error.stack
        });
        throw error;
    }
}

async function getJobProgressDetails(jobId) {
    const command = new GetCommand({
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId }
    });

    const result = await docClient.send(command);
    return result.Item;
}

async function getDestinationDetails(userId) {
    try {
        const userData = await supabaseService.getUserData(userId);

        logger.info('Retrieved user data', {
            userId,
            hasUserData: userData,
            hasS3Connection: !!(userData && userData.s3Connection)
        });

        if (!userData) {
            throw new Error('User not found');
        }

        if (!userData.s3Connection) {
            throw new Error('No S3 connection configuration found');
        }

        const { s3Connection } = userData;

        // Validate required fields
        if (!s3Connection.bucketName || !s3Connection.region) {
            throw new Error('Invalid S3 connection configuration - missing required fields');
        }

        return {
            s3Connection: {
                bucketName: s3Connection.bucketName,
                region: s3Connection.region
            }
        };
    } catch (error) {
        logger.error('Error getting destination details', {
            userId,
            error: error.message
        });
        throw error;
    }
}

async function validateS3Access(destinationS3Client, bucketName) {

    await destinationS3Client.send(new ListObjectsV2Command({
        Bucket: bucketName,
        MaxKeys: 1
    }));
}

// Optimized task processing with controlled concurrency
async function processTasks({ jobId, s3Config, destinationS3Client }) {
    const deliveryDetails = {
        processedCount: 0,
        failedCount: 0,
        failedTasks: [],
        startTime: Date.now(),
        lastProcessedKey: null
    };

    const taskGenerator = taskService.fetchAllEligibleTasks(jobId);

    for await (const batch of taskGenerator) {
        await Promise.all(
            chunk(batch.items, CONFIG.BATCH_SIZE).map(async taskBatch => {
                const batchResults = await Promise.allSettled(
                    taskBatch.map(task => deliverTaskToS3({
                        task,
                        s3Config,
                        destinationS3Client
                    }))
                );

                // Process batch results
                batchResults.forEach((result, index) => {
                    const task = taskBatch[index];
                    if (result.status === 'fulfilled') {
                        deliveryDetails.processedCount++;
                    } else {
                        deliveryDetails.failedCount++;
                        deliveryDetails.failedTasks.push({
                            taskId: task.TaskID,
                            s3Key: task.ImageS3Key,
                            error: result.reason.message
                        });
                    }
                });

                // Log batch progress
                logger.info('Batch processing complete', {
                    jobId,
                    batchSize: taskBatch.length,
                    successCount: deliveryDetails.processedCount,
                    failureCount: deliveryDetails.failedCount,
                    elapsedTimeMs: Date.now() - deliveryDetails.startTime
                });

                // Update progress with all stats nested under deliveryDetails
                await jobProgressService.updateJobProgress(jobId, {
                    deliveryDetails: {
                        ...deliveryDetails,
                        lastProcessedKey: batch.lastEvaluatedKey,
                        lastUpdated: Date.now().toString()
                    },
                    deliveryStatus: IN_PROGRESS
                });
            })
        );
    }

    return deliveryDetails;
}

// Optimized S3 delivery with proper streaming upload
async function deliverTaskToS3({ task, s3Config, destinationS3Client }) {
    const sourceKey = task.ImageS3Key;
    const fileName = sourceKey.split('/').pop();
    const destinationKey = `${s3Config.destinationPrefix}/${fileName}`;

    try {
        const getObjectResponse = await s3Client.send(new GetObjectCommand({
            Bucket: process.env.SOURCE_BUCKET,
            Key: sourceKey
        }));

        // Use Upload instead of PutObjectCommand for streaming
        const upload = new Upload({
            client: destinationS3Client,
            params: {
                Bucket: s3Config.bucketName,
                Key: destinationKey,
                Body: getObjectResponse.Body,
                ContentType: getObjectResponse.ContentType,
                ACL: 'bucket-owner-full-control'
            },
            queueSize: 4, // number of concurrent upload parts
            partSize: CONFIG.CHUNK_SIZE
        });

        await upload.done();

        logger.info('File delivery successful', {
            taskId: task.TaskID,
            sourceKey,
            destinationKey,
            contentLength: getObjectResponse.ContentLength
        });

        return true;
    } catch (error) {
        logger.error('File delivery failed', {
            taskId: task.TaskID,
            sourceKey,
            error: error.message,
            errorCode: error.code,
            stackTrace: error.stack
        });
        throw error;
    }
}

// Helper function to chunk arrays
function chunk(array, size) {
    return Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
        array.slice(i * size, i * size + size)
    );
}

async function handleFatalError(jobId, errorMessage) {
    logger.error('Fatal error in job processing', { jobId, error: errorMessage });

    await jobProgressService.updateJobProgress(jobId, {
        status: FAILED,
        deliveryError: [{
            message: errorMessage,
            timestamp: new Date().toISOString()
        }],
        deliveryStatus: FAILED,
        completedAt: new Date().toISOString()
    });

    // await supabaseService.updateJobStatus(jobId, FAILED);
}

async function updateFinalJobStatus(jobId, processedCount, failedCount, failedTasks) {
    const status = COMPLETED;
    const now = Date.now().toString();

    await jobProgressService.updateJobProgress(jobId, {
        status,
        deliveryDetails: {
            processedCount,
            failedCount,
            failedTasks,
            lastUpdated: now
        },
        completedAt: now,
        deliveryStatus: COMPLETED,
        deliveryError: failedCount > 0 ? [{
            message: `Failed to deliver ${failedCount} images`,
            timestamp: now
        }] : undefined
    });
} 
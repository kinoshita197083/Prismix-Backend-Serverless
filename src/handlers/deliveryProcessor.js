const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const { createClient } = require('@supabase/supabase-js');
const { fetchGoogleRefreshToken, setUpGoogleDriveClient } = require('../utils/googleDrive/googleDrive');
const { ELIGIBLE, COMPLETED, FAILED } = require('../utils/config');
const s3Service = require('../services/s3Service');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const dynamoService = require('../services/dynamoService');
const lambdaClient = new LambdaClient();
const createTaskService = require('../services/taskService');
const JobProgressService = require('../services/jobProgressService');

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

// Add constants for batch processing
const BATCH_SIZE = 200; // Adjust based on performance testing
const TIMEOUT_BUFFER_MS = 30000; // 30 seconds buffer before Lambda timeout
const LAMBDA_TIMEOUT_MS = 900000; // 15 minutes in milliseconds

// TESTING
// const TIMEOUT_BUFFER_MS = 1000; // 30 seconds buffer before Lambda timeout
// const LAMBDA_TIMEOUT_MS = 3000; // 1 minute in milliseconds

// Add configuration constants
const UPLOAD_CONFIG = {
    maxConcurrency: 50,     // Maximum concurrent uploads
    retryAttempts: 3,       // Number of retry attempts per file
    retryDelay: 1000,       // Delay between retries in ms
    chunkDelay: 2000,       // Delay between chunks in ms
    chunkSize: 200           // Number of files per chunk
};

// Add pagination constants
const PAGINATION_CONFIG = {
    dynamoPageSize: 1000,    // Number of items per DynamoDB query
    s3ListLimit: 1000,       // Number of objects per S3 list request
    maxPages: 100           // Safety limit for pagination loops
};

// Initialize with custom pagination config
const taskService = createTaskService({
    dynamoPageSize: PAGINATION_CONFIG.dynamoPageSize,
    maxPages: PAGINATION_CONFIG.maxPages
});

const jobProgressService = new JobProgressService(docClient, null, {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
});

exports.handler = async (event) => {
    const startTime = Date.now();
    logger.info('Delivery processor started', { event });

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        const { jobId } = message;

        console.log('----> jobId', jobId);

        const provider = await fetchProvider(jobId);

        console.log('----> provider', provider);

        if (provider !== 'google-drive') return console.log('----> provider is not google-drive');

        let currentBatch = [];
        let lastTaskBatch = null;

        try {
            // Fetch job state including LastEvaluatedKey and folder ID
            const jobState = await fetchJobProgressData(jobId);
            const {
                userId,
                processedFolderIds,
                processedDriveIds,
                lastEvaluatedKey = null,
                destinationDriveFolderId = null  // Get existing folder ID
            } = jobState;
            console.log('----> jobState', jobState);

            if (!userId) {
                logger.error(`User ID not found for job ${jobId}`);
                continue;
            }

            // Get or create Google Drive folder, passing existing folder ID
            const drive = await setupGoogleDrive(userId);
            const folderId = await getOrCreateFolder(drive, jobId, processedDriveIds, destinationDriveFolderId);
            console.log('----> folderId', folderId);

            // Fetch eligible tasks using LastEvaluatedKey for pagination
            const taskGenerator = taskService.fetchAllEligibleTasks(jobId, lastEvaluatedKey);

            // Set job delivery status to IN_PROGRESS
            await jobProgressService.updateJobProgress(jobId, {
                deliveryStatus: 'IN_PROGRESS'
            });

            for await (const taskBatch of taskGenerator) {
                lastTaskBatch = taskBatch;

                // Add tasks to current batch
                for (const task of taskBatch.items) {
                    currentBatch.push(task);

                    // Process batch when it reaches BATCH_SIZE or it's the last batch
                    if (currentBatch.length >= BATCH_SIZE ||
                        (!taskBatch.lastEvaluatedKey && task === taskBatch.items[taskBatch.items.length - 1])) {

                        const results = await rateLimitedProcessBatch(
                            drive,
                            currentBatch,
                            folderId,
                            startTime,
                            UPLOAD_CONFIG
                        );

                        // Handle results and update progress
                        await handleBatchResults(
                            jobId,
                            results,
                            lastTaskBatch.lastEvaluatedKey
                        );

                        // Check if we hit timeout
                        if (results.timeoutReached) {
                            logger.warn('Timeout reached, invoking next batch', {
                                remainingTasks: results.remainingTasks.length
                            });
                            await invokeNextBatch(event, jobId);
                            return;
                        }

                        // Reset batch
                        currentBatch = [];
                    }
                }
            }

            // Set job delivery status to COMPLETED
            handleDeliveryCompletion(jobId);

        } catch (error) {
            logger.error(`Error processing delivery for job ${jobId}:`, {
                error: error.message,
                stack: error.stack
            });

            // Update failed images if batch processing fails
            if (currentBatch.length > 0) {
                const failedTasks = currentBatch.map(task => ({
                    taskId: task.TaskID,
                    fileName: task.ImageS3Key.split('/').pop(),
                    s3Key: task.ImageS3Key,
                    error: 'Delivery failed: ' + error.message
                }));
            }

            // Set job delivery status to FAILED
            await jobProgressService.updateJobProgress(jobId, {
                deliveryStatus: 'FAILED'
            });
        }
    }

    logger.info('Delivery processor finished');
};

async function handleDeliveryCompletion(jobId) {
    await jobProgressService.updateJobProgress(jobId, {
        deliveryStatus: COMPLETED
    });
}

async function fetchProvider(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'outputConnection'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);

        if (!result.Item || !result.Item.outputConnection) {
            logger.warn(`No outputConnection found for job ${jobId}`);
            return null;
        }

        return result.Item.outputConnection;
    } catch (error) {
        logger.error(`Error fetching provider for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function getImageFromS3(key) {
    logger.debug(`Fetching image from S3: ${key}`);
    const getObjectParams = {
        Bucket: process.env.BUCKET_NAME,
        Key: key
    };

    try {
        const response = await s3Service.getFile(getObjectParams);
        const buffer = await streamToBuffer(response);
        return buffer;
    } catch (error) {
        logger.error(`getImageFromS3(): ${key}`, { error: error.message });
        throw error;
    }
}

function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', (error) => {
            logger.error('Error in stream to buffer conversion', { error: error.message });
            reject(error);
        });
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function uploadToGoogleDrive(drive, buffer, fileName, folderId) {
    const fileMetadata = {
        name: fileName,
        parents: [folderId]
    };

    const media = {
        mimeType: 'image/jpeg', // Adjust based on your image types
        body: bufferToStream(buffer)
    };

    try {
        await drive.files.create({
            resource: fileMetadata,
            media: media,
            fields: 'id'
        });
        logger.debug(`Uploaded ${fileName} to Google Drive`);
    } catch (error) {
        logger.error(`Error uploading ${fileName} to Google Drive:`, { error: error.message });
        throw error;
    }
}

function bufferToStream(buffer) {
    const stream = new require('stream').Readable();
    stream.push(buffer);
    stream.push(null);
    return stream;
}

async function fetchJobProgressData(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'userId, processedFolderIds, processedDriveIds, lastEvaluatedKey, destinationDriveFolderId'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);

        if (!result.Item) {
            logger.warn(`No job progress data found for jobId: ${jobId}`);
            return null;
        }

        return {
            userId: result.Item.userId,
            processedFolderIds: result.Item.processedFolderIds || [],
            processedDriveIds: result.Item.processedDriveIds || [],
            lastEvaluatedKey: result.Item.lastEvaluatedKey || null,
            destinationDriveFolderId: result.Item.destinationDriveFolderId || null  // Include folder ID
        };
    } catch (error) {
        logger.error('Error fetching job progress data:', { error, jobId });
        throw error;
    }
}

async function setupGoogleDrive(userId) {
    // Fetch Google refresh token from Supabase
    const googleRefreshToken = await fetchGoogleRefreshToken(userId, supabase);

    // Pass in the refresh token to the Google Drive client
    const drive = setUpGoogleDriveClient(googleRefreshToken);

    return drive;
}

async function getOrCreateFolder(drive, jobId, processedDriveIds, existingFolderId = null) {
    // If we already have a folder ID, use it
    if (existingFolderId) {
        logger.info(`Using existing folder for job ${jobId}`, { folderId: existingFolderId });
        return existingFolderId;
    }

    const folderMetadata = {
        name: `Prismix-Job_${jobId}`,
        mimeType: 'application/vnd.google-apps.folder',
        parents: processedDriveIds
    };

    try {
        const response = await drive.files.create({
            resource: folderMetadata,
            fields: 'id'
        });
        const folderId = response.data.id;

        // Store the folder ID in JobProgress
        await updateJobFolderId(jobId, folderId);

        logger.info(`Created Google Drive folder for job ${jobId}`, { folderId });
        return folderId;
    } catch (error) {
        logger.error(`Error creating Google Drive folder for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function updateJobFolderId(jobId, folderId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET destinationDriveFolderId = :folderId',
        ExpressionAttributeValues: {
            ':folderId': folderId
        }
    };

    try {
        const command = new UpdateCommand(params);
        await docClient.send(command);
        logger.info(`Updated drive folder ID for job ${jobId}`, { folderId });
    } catch (error) {
        logger.error(`Error updating drive folder ID for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function rateLimitedProcessBatch(drive, tasks, folderId, startTime, config = UPLOAD_CONFIG) {
    const results = {
        failed: [],
        timeoutReached: false,
        remainingTasks: []  // Add this to track unprocessed tasks
    };

    logger.info('Starting rate-limited batch processing', {
        totalTasks: tasks.length,
        concurrency: config.maxConcurrency,
        chunkSize: config.chunkSize
    });

    // Check timeout before starting
    if (Date.now() - startTime > LAMBDA_TIMEOUT_MS - TIMEOUT_BUFFER_MS) {
        logger.warn('Approaching Lambda timeout before processing batch');
        results.timeoutReached = true;
        results.remainingTasks = tasks;  // Store tasks for next invocation
        return results;
    }

    // Split tasks into chunks
    const chunks = [];
    for (let i = 0; i < tasks.length; i += config.chunkSize) {
        chunks.push(tasks.slice(i, i + config.chunkSize));
    }

    logger.info(`Split tasks into ${chunks.length} chunks`);

    for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
        // Check for timeout before processing each chunk
        if (Date.now() - startTime > LAMBDA_TIMEOUT_MS - TIMEOUT_BUFFER_MS) {
            logger.warn('Approaching Lambda timeout, stopping chunk processing');
            const remainingTasks = tasks.slice(chunkIndex * config.chunkSize);
            results.failed.push(...remainingTasks.map(task => ({
                taskId: task.TaskID,
                fileName: task.ImageS3Key.split('/').pop(),
                s3Key: task.ImageS3Key,
                error: 'Task skipped due to Lambda timeout approaching'
            })));
            break;
        }

        const chunk = chunks[chunkIndex];
        logger.info(`Processing chunk ${chunkIndex + 1}/${chunks.length}`, {
            chunkSize: chunk.length
        });

        // Process chunk with retry logic
        const chunkResults = await processChunkWithRetry(
            drive,
            chunk,
            folderId,
            config
        );

        results.failed.push(...chunkResults.failed);

        // Add delay between chunks to prevent rate limiting
        if (chunkIndex < chunks.length - 1) {
            logger.debug(`Waiting ${config.chunkDelay}ms before next chunk`);
            await new Promise(resolve => setTimeout(resolve, config.chunkDelay));
        }
    }

    logger.info('Rate-limited batch processing completed', {
        totalProcessed: tasks.length,
        // successful: results.successful.length,
        failed: results.failed.length,
        timeElapsed: Date.now() - startTime
    });

    return results;
}

async function processChunkWithRetry(drive, tasks, folderId, config) {
    const results = {
        // successful: [], // Commented out successful tracking
        failed: []
    };

    // Create a pool of promises with limited concurrency
    const pool = new Set();
    const taskQueue = [...tasks];

    while (taskQueue.length > 0 || pool.size > 0) {
        // Fill the pool up to maxConcurrency
        while (pool.size < config.maxConcurrency && taskQueue.length > 0) {
            const task = taskQueue.shift();
            const promise = processTaskWithRetry(drive, task, folderId, config)
                .then(result => {
                    pool.delete(promise);
                    if (result.status === 'fulfilled') {
                        // results.successful.push(result.value);
                    } else {
                        results.failed.push(result.reason);
                    }
                });
            pool.add(promise);
        }

        // Wait for at least one promise to complete
        if (pool.size > 0) {
            await Promise.race(pool);
        }
    }

    return results;
}

async function processTaskWithRetry(drive, task, folderId, config) {
    let lastError;

    for (let attempt = 1; attempt <= config.retryAttempts; attempt++) {
        try {
            const imageData = await getImageFromS3(task.ImageS3Key);
            const fileName = task.ImageS3Key.split('/').pop();
            await uploadToGoogleDrive(drive, imageData, fileName, folderId);

            logger.debug(`Successfully processed task on attempt ${attempt}`, {
                taskId: task.TaskID,
                fileName
            });

            return {
                status: 'fulfilled',
                value: {
                    taskId: task.TaskID,
                    fileName,
                    s3Key: task.ImageS3Key,
                    attempts: attempt
                }
            };
        } catch (error) {
            lastError = error;
            logger.warn(`Failed attempt ${attempt}/${config.retryAttempts}`, {
                taskId: task.TaskID,
                error: error.message
            });

            if (attempt < config.retryAttempts) {
                const delay = config.retryDelay * attempt; // Exponential backoff
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }

    return {
        status: 'rejected',
        reason: {
            taskId: task.TaskID,
            fileName: task.ImageS3Key.split('/').pop(),
            s3Key: task.ImageS3Key,
            error: lastError.message,
            errorDetails: {
                code: lastError.code,
                statusCode: lastError.statusCode,
                attempts: config.retryAttempts,
                time: new Date().toISOString()
            }
        }
    };
}

async function invokeNextBatch(originalEvent, jobId) {
    const command = new InvokeCommand({
        FunctionName: process.env.DELIVERY_LAMBDA_PROCESSOR_NAME,
        InvocationType: 'Event',
        Payload: JSON.stringify(originalEvent)
    });

    console.log('----> FunctionName', process.env.DELIVERY_LAMBDA_PROCESSOR_NAME);

    try {
        await lambdaClient.send(command);
        logger.info(`Invoked next batch processing for job ${jobId}`);
    } catch (error) {
        logger.error(`Failed to invoke next batch for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function handleBatchResults(jobId, results, lastEvaluatedKey) {
    // Don't update failed tasks if we hit a timeout
    if (results.timeoutReached) {
        // Only update the lastEvaluatedKey if we have one
        if (lastEvaluatedKey) {
            const params = {
                TableName: process.env.JOB_PROGRESS_TABLE,
                Key: { JobId: jobId },
                UpdateExpression: 'SET lastEvaluatedKey = :key',
                ExpressionAttributeValues: {
                    ':key': lastEvaluatedKey
                }
            };

            try {
                const command = new UpdateCommand(params);
                await docClient.send(command);
                logger.info(`Updated lastEvaluatedKey for job ${jobId}`, {
                    lastEvaluatedKey,
                    remainingTasks: results.remainingTasks.length
                });
            } catch (error) {
                logger.error(`Error updating lastEvaluatedKey for job ${jobId}:`, {
                    error: error.message
                });
                throw error;
            }
        }
        return;
    }

    // Normal case - update failed tasks
    let updateExpression = 'SET deliveryDetails = :details';
    let expressionAttributeValues = {
        ':details': {
            failedImages: results.failed,
            lastUpdated: new Date().toISOString(),
            totalFailures: results.failed.length
        }
    };

    if (lastEvaluatedKey) {
        updateExpression += ', lastEvaluatedKey = :key';
        expressionAttributeValues[':key'] = lastEvaluatedKey;
    }

    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: updateExpression,
        ExpressionAttributeValues: expressionAttributeValues
    };

    try {
        const command = new UpdateCommand(params);
        const result = await docClient.send(command);
        logger.info(`Updated batch results for job ${jobId}`, {
            lastEvaluatedKey: lastEvaluatedKey || 'none',
            failedCount: results.failed.length,
            updatedAttributes: result.Attributes
        });
    } catch (error) {
        logger.error(`Error updating batch results for job ${jobId}:`, {
            error: error.message,
            params: JSON.stringify(params)
        });
        throw error;
    }
}

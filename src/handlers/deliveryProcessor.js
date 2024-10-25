const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const { createClient } = require('@supabase/supabase-js');
const { fetchGoogleRefreshToken, setUpGoogleDriveClient } = require('../utils/googleDrive/googleDrive');
const { ELIGIBLE } = require('../utils/config');
const s3Service = require('../services/s3Service');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const lambdaClient = new LambdaClient();

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

// Add constants for batch processing
const BATCH_SIZE = 50; // Adjust based on performance testing
const TIMEOUT_BUFFER_MS = 30000; // 30 seconds buffer before Lambda timeout
const LAMBDA_TIMEOUT_MS = 900000; // 15 minutes in milliseconds

// Add configuration constants
const UPLOAD_CONFIG = {
    maxConcurrency: 10,     // Maximum concurrent uploads
    retryAttempts: 3,       // Number of retry attempts per file
    retryDelay: 1000,       // Delay between retries in ms
    chunkDelay: 2000,       // Delay between chunks in ms
    chunkSize: 10           // Number of files per chunk
};

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

        try {
            // Fetch job state including last processed index
            const jobState = await fetchJobProgressData(jobId);
            const { userId, processedFolderIds, processedDriveIds, lastProcessedIndex = 0 } = jobState;
            console.log('----> jobState', jobState);

            if (!userId) {
                logger.error(`User ID not found for job ${jobId}`);
                continue;
            }

            // Fetch eligible tasks starting from lastProcessedIndex
            const eligibleTasks = await fetchEligibleTasks(jobId);
            console.log('----> eligibleTasks', eligibleTasks);

            if (eligibleTasks.length === 0) {
                logger.warn(`No eligible tasks found for job ${jobId}`);
                continue;
            }

            // Get or create Google Drive folder
            const drive = await setupGoogleDrive(userId);
            const folderId = await getOrCreateFolder(drive, jobId, processedDriveIds);
            console.log('----> folderId', folderId);
            // Process tasks in batches
            const remainingTasks = eligibleTasks.slice(lastProcessedIndex);
            const tasksToProcess = remainingTasks.slice(0, BATCH_SIZE);
            console.log('----> tasksToProcess', tasksToProcess);

            if (tasksToProcess.length === 0) {
                logger.info(`All tasks completed for job ${jobId}`);
                continue;
            }

            logger.info(`Processing batch of ${tasksToProcess.length} tasks`, {
                jobId,
                startIndex: lastProcessedIndex,
                totalTasks: eligibleTasks.length
            });

            // Use rateLimitedProcessBatch instead of processBatch
            const results = await rateLimitedProcessBatch(
                drive,
                tasksToProcess,
                folderId,
                startTime,
                UPLOAD_CONFIG
            );

            console.log('----> results', results);

            // Handle failed uploads
            if (results.failed.length > 0) {
                logger.warn(`Failed to upload ${results.failed.length} images`, {
                    jobId,
                    failedCount: results.failed.length
                });

                await updateFailedImagesToJobProgress(jobId, results.failed);
            }

            // Update job progress with results
            await updateJobProgress(jobId, {
                lastProcessedIndex: lastProcessedIndex + tasksToProcess.length,
                processedResults: results,
                folderId
            });

            // Check if we need to continue processing
            if (lastProcessedIndex + tasksToProcess.length < eligibleTasks.length) {
                await invokeNextBatch(event, jobId);
            }

        } catch (error) {
            logger.error(`Error processing delivery for job ${jobId}:`, {
                error: error.message,
                stack: error.stack
            });

            // Update failed images even if the batch processing fails
            const failedTasks = tasksToProcess.map(task => ({
                taskId: task.TaskID,
                fileName: task.ImageS3Key.split('/').pop(),
                s3Key: task.ImageS3Key,
                error: 'Batch processing failed: ' + error.message
            }));

            await updateFailedImagesToJobProgress(jobId, failedTasks);
        }
    }

    logger.info('Delivery processor finished');
};

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

async function fetchEligibleTasks(jobId) {
    logger.info(`Fetching eligible tasks for job ${jobId}`);
    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'Evaluation = :evaluation',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':evaluation': ELIGIBLE
        }
    };

    try {
        const command = new QueryCommand(params);
        const result = await docClient.send(command);
        logger.info(`Fetched ${result.Items.length} eligible tasks for job ${jobId}`);
        return result.Items;
    } catch (error) {
        logger.error(`Error fetching eligible tasks for job ${jobId}:`, { error: error.message, params });
        throw error;
    }
}

async function updateFailedImagesToJobProgress(jobId, failedImages) {
    if (!failedImages || failedImages.length === 0) {
        logger.info('No failed images to update');
        return;
    }

    logger.info(`Updating ${failedImages.length} failed images to job progress`, {
        jobId,
        failedCount: failedImages.length
    });

    const deliveryDetails = {
        failedImages,
        lastUpdated: new Date().toISOString(),
        totalFailures: failedImages.length
    };

    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET deliveryDetails = if_not_exists(deliveryDetails, :empty_list) ADD totalFailedUploads :failedCount',
        ExpressionAttributeValues: {
            ':empty_list': deliveryDetails,
            ':failedCount': failedImages.length
        },
        ReturnValues: 'UPDATED_NEW'
    };

    try {
        const command = new UpdateCommand(params);
        const result = await docClient.send(command);

        logger.info(`Successfully updated failed images for job ${jobId}`, {
            updatedAttributes: result.Attributes
        });
    } catch (error) {
        logger.error(`Error updating failed images for job ${jobId}:`, {
            error: error.message,
            failedImagesCount: failedImages.length
        });
        // Don't throw here to prevent the entire process from failing
        // but make sure to log the error for monitoring
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
        ProjectionExpression: 'userId, processedFolderIds, processedDriveIds, lastProcessedIndex'
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
            lastProcessedIndex: result.Item.lastProcessedIndex || 0
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

async function getOrCreateFolder(drive, jobId, processedDriveIds) {
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
        logger.info(`Created Google Drive folder for job ${jobId}`);
        return response.data.id;
    } catch (error) {
        logger.error(`Error creating Google Drive folder for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function rateLimitedProcessBatch(drive, tasks, folderId, startTime, config = UPLOAD_CONFIG) {
    logger.info('Starting rate-limited batch processing', {
        totalTasks: tasks.length,
        concurrency: config.maxConcurrency,
        chunkSize: config.chunkSize
    });

    const results = {
        successful: [],
        failed: []
    };

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

        results.successful.push(...chunkResults.successful);
        results.failed.push(...chunkResults.failed);

        // Add delay between chunks to prevent rate limiting
        if (chunkIndex < chunks.length - 1) {
            logger.debug(`Waiting ${config.chunkDelay}ms before next chunk`);
            await new Promise(resolve => setTimeout(resolve, config.chunkDelay));
        }
    }

    logger.info('Rate-limited batch processing completed', {
        totalProcessed: tasks.length,
        successful: results.successful.length,
        failed: results.failed.length,
        timeElapsed: Date.now() - startTime
    });

    return results;
}

async function processChunkWithRetry(drive, tasks, folderId, config) {
    const results = {
        successful: [],
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
                        results.successful.push(result.value);
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
        FunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
        InvocationType: 'Event',
        Payload: JSON.stringify(originalEvent)
    });

    try {
        await lambdaClient.send(command);
        logger.info(`Invoked next batch processing for job ${jobId}`);
    } catch (error) {
        logger.error(`Failed to invoke next batch for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function updateJobProgress(jobId, { lastProcessedIndex, processedResults, folderId }) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET lastProcessedIndex = :idx, processedResults = list_append(if_not_exists(processedResults, :empty_list), :results), folderId = :folderId',
        ExpressionAttributeValues: {
            ':idx': lastProcessedIndex,
            ':results': [processedResults],
            ':empty_list': [],
            ':folderId': folderId
        }
    };

    try {
        const command = new UpdateCommand(params);
        await docClient.send(command);
        logger.info(`Updated job progress for ${jobId}`, { lastProcessedIndex, resultsCount: processedResults.successful.length });
    } catch (error) {
        logger.error(`Error updating job progress for ${jobId}:`, { error: error.message });
        throw error;
    }
}

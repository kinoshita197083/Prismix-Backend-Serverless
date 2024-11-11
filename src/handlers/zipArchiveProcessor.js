const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const logger = require('../utils/logger');
const { createZipStream, uploadZipToS3 } = require('../utils/s3Streams');
const ZipArchiveProgressService = require('../services/zipArchiveProgressService');
const ZipMergeService = require('../services/zipMergeService');

// Initialize AWS clients
const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);
const s3Client = new S3Client();
const sqsClient = new SQSClient();

// Initialize services
const zipArchiveProgressService = new ZipArchiveProgressService(docClient);
const zipMergeService = new ZipMergeService(s3Client);

/**
 * Configuration for pagination and batch processing
 */
const PAGINATION_CONFIG = {
    maxBatchSize: 100,
    maxPages: 1000,
    scanIndexForward: true
};

/**
 * Generator function to fetch all eligible tasks in batches
 * @param {string} jobId - The ID of the job
 * @param {Object} lastEvaluatedKey - Key for pagination
 */
async function* fetchAllEligibleTasks(jobId, lastEvaluatedKey) {
    let pageCount = 0;

    do {
        if (pageCount >= PAGINATION_CONFIG.maxPages) {
            logger.warn(`Reached maximum page limit for job ${jobId}`, {
                maxPages: PAGINATION_CONFIG.maxPages
            });
            break;
        }

        const result = await fetchEligibleTasks(jobId, lastEvaluatedKey);
        lastEvaluatedKey = result.lastEvaluatedKey;
        pageCount++;

        logger.debug('Fetched batch of eligible tasks', {
            jobId,
            batchSize: result.items.length,
            pageCount,
            hasMorePages: !!lastEvaluatedKey
        });

        yield {
            items: result.items,
            lastEvaluatedKey
        };
    } while (lastEvaluatedKey);

    logger.info(`Completed fetching all eligible tasks`, {
        jobId,
        totalPages: pageCount
    });
}

/**
 * Fetches a single batch of eligible tasks
 * @param {string} jobId - The ID of the job
 * @param {Object} lastEvaluatedKey - Key for pagination
 */
async function fetchEligibleTasks(jobId, lastEvaluatedKey = null) {
    logger.debug('Fetching batch of eligible tasks', {
        jobId,
        lastEvaluatedKey
    });

    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobId = :jobId',
        FilterExpression: 'Evaluation = :evaluation',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':evaluation': 'ELIGIBLE'
        },
        Limit: PAGINATION_CONFIG.maxBatchSize,
        ExclusiveStartKey: lastEvaluatedKey
    };

    try {
        const command = new QueryCommand(params);
        const result = await docClient.send(command);

        logger.debug('Successfully fetched tasks batch', {
            jobId,
            itemCount: result.Items.length,
            hasMoreItems: !!result.LastEvaluatedKey
        });

        return {
            items: result.Items,
            lastEvaluatedKey: result.LastEvaluatedKey
        };
    } catch (error) {
        logger.error('Error fetching eligible tasks', {
            jobId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Processes a single chunk of files
 * @param {string} jobId - The ID of the job
 * @param {Object} chunk - The chunk metadata
 * @param {string[]} allImageKeys - Array of all image S3 keys
 */
async function processChunk(jobId, chunk, allImageKeys) {
    const { startIndex, endIndex } = chunk;
    const chunkImageKeys = allImageKeys.slice(startIndex, endIndex);

    logger.info('Starting chunk processing', {
        jobId,
        chunkId: chunk.chunkId,
        filesCount: chunkImageKeys.length,
        startIndex,
        endIndex
    });

    // Create chunk-specific ZIP file name
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const zipKey = `archives/${jobId}/chunks/${chunk.chunkId}_${timestamp}.zip`;

    try {
        const archive = await createZipStream(
            s3Client,
            process.env.SOURCE_BUCKET,
            chunkImageKeys,
            (progress) => {
                logger.info(`ZIP progress for chunk ${chunk.chunkId}:`, {
                    jobId,
                    filesProcessed: progress.entries.processed,
                    totalBytes: progress.fs.processedBytes
                });
            }
        );

        archive.finalize();

        await uploadZipToS3(
            s3Client,
            archive,
            process.env.DELIVERY_BUCKET,
            zipKey
        );

        logger.info('Successfully processed chunk', {
            jobId,
            chunkId: chunk.chunkId,
            zipKey
        });

        await zipArchiveProgressService.updateChunkStatus(jobId, chunk.chunkId, 'COMPLETED', null, zipKey);

        // Check if all chunks are complete
        const isComplete = await zipArchiveProgressService.isJobComplete(jobId);
        if (isComplete) {
            await handleJobCompletion(jobId);
        }

        return zipKey;
    } catch (error) {
        logger.error(`Error processing chunk`, {
            jobId,
            chunkId: chunk.chunkId,
            error: error.message,
            stack: error.stack
        });

        await zipArchiveProgressService.updateChunkStatus(
            jobId,
            chunk.chunkId,
            'FAILED',
            error.message
        );

        throw error;
    }
}

/**
 * Handles the completion of all chunks for a job
 * @param {string} jobId - The ID of the job
 */
async function handleJobCompletion(jobId) {
    logger.info(`Starting final merge process for completed job`, { jobId });

    try {
        const chunkKeys = await zipArchiveProgressService.getAllCompletedChunkKeys(jobId);

        logger.info('Retrieved all completed chunk keys', {
            jobId,
            chunkCount: chunkKeys.length
        });

        const finalZipKey = await zipMergeService.mergeChunks(
            jobId,
            chunkKeys,
            process.env.DELIVERY_BUCKET
        );

        await zipArchiveProgressService.updateFinalZipLocation(jobId, finalZipKey);

        logger.info(`Successfully completed ZIP archive creation`, {
            jobId,
            finalZipKey
        });

        // Here you might want to trigger a notification or update other systems
        // about the availability of the final ZIP file

    } catch (error) {
        logger.error(`Error during final merge`, {
            jobId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Re-enqueues a job for processing its next chunk
 * @param {string} jobId - The ID of the job
 */
async function reEnqueueForNextChunk(jobId) {
    logger.info('Re-enqueueing job for next chunk processing', { jobId });

    try {
        const command = new SendMessageCommand({
            QueueUrl: process.env.ZIP_DELIVERY_QUEUE_URL,
            MessageBody: JSON.stringify({
                jobId,
                action: 'PROCESS_NEXT_CHUNK'
            }),
            DelaySeconds: 0
        });

        await sqsClient.send(command);
        logger.info('Successfully re-enqueued job', { jobId });
    } catch (error) {
        logger.error('Failed to re-enqueue job', {
            jobId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Main Lambda handler
 */
exports.handler = async (event) => {
    logger.info('Received event', {
        recordCount: event.Records.length
    });

    console.log('event', event);

    const batchItemFailures = [];

    for (const record of event.Records) {
        try {
            const body = JSON.parse(record.body);

            // Handle both SNS notifications and direct SQS messages
            const message = body.Message ? JSON.parse(body.Message) : body;
            const { jobId, status, action } = message;

            logger.info('Processing message', {
                jobId,
                status,
                action,
                messageId: record.messageId
            });

            if (action === 'PROCESS_NEXT_CHUNK') {
                logger.info(`Processing next chunk for job`, { jobId });
                const taskIterator = fetchAllEligibleTasks(jobId);
                await processEligibleTasks(jobId, taskIterator);
                continue;
            }

            if (status !== 'COMPLETED') {
                logger.info(`Skipping non-completed job status`, {
                    jobId,
                    status
                });
                continue;
            }

            logger.info(`Starting ZIP archive creation for completed job`, { jobId });
            const taskIterator = fetchAllEligibleTasks(jobId);
            await processEligibleTasks(jobId, taskIterator);

        } catch (error) {
            logger.error('Error processing record', {
                messageId: record.messageId,
                error: error.message,
                stack: error.stack
            });
            batchItemFailures.push({
                itemIdentifier: record.messageId
            });
        }
    }

    return { batchItemFailures };
}; 
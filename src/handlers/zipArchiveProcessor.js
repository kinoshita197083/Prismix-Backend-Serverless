const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const logger = require('../utils/logger');
const { createZipStream, uploadZipToS3 } = require('../utils/s3Streams');
const ZipArchiveProgressService = require('../services/zipArchiveProgressService');
const ZipMergeService = require('../services/zipMergeService');
const { PassThrough } = require('stream');
const { Upload } = require('@aws-sdk/lib-storage');
const archiver = require('archiver');
const JobProgressService = require('../services/jobProgressService');

// Initialize AWS clients
const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);
const s3Client = new S3Client();
const sqsClient = new SQSClient();

// Initialize services
const zipArchiveProgressService = new ZipArchiveProgressService(docClient);
const zipMergeService = new ZipMergeService(s3Client);
const jobProgressService = new JobProgressService(docClient, null, {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
});

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
            batchSize: result?.items?.length,
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
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'Evaluation = :evaluation',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':evaluation': 'ELIGIBLE'
        },
        Limit: PAGINATION_CONFIG.maxBatchSize,
        ...(lastEvaluatedKey && { ExclusiveStartKey: lastEvaluatedKey })
    };

    try {
        const command = new QueryCommand(params);
        const result = await docClient.send(command);

        logger.debug('Successfully fetched tasks batch', {
            jobId,
            itemCount: result?.Items?.length,
            hasMoreItems: !!result?.LastEvaluatedKey
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
        chunkId: chunk?.chunkId,
        filesCount: chunkImageKeys?.length,
        startIndex,
        endIndex
    });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const zipKey = `archives/${jobId}/chunks/${chunk.chunkId}_${timestamp}.zip`;

    try {
        // Create a PassThrough stream to pipe data through
        const passThrough = new PassThrough();

        // Create and configure the archiver
        const archive = archiver('zip', {
            zlib: { level: 9 }
        });

        // Handle archive warnings
        archive.on('warning', (err) => {
            if (err.code === 'ENOENT') {
                logger.warn('Archive warning', { warning: err.message });
            } else {
                throw err;
            }
        });

        // Handle archive errors
        archive.on('error', (err) => {
            throw err;
        });

        // Pipe archive data to the PassThrough stream
        archive.pipe(passThrough);

        // Add files to archive
        for (const key of chunkImageKeys) {
            const s3Stream = await getS3ReadStream(s3Client, process.env.SOURCE_BUCKET, key);
            archive.append(s3Stream, { name: key.split('/').pop() });
        }

        // Create upload manager with the PassThrough stream
        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.DELIVERY_BUCKET,
                Key: zipKey,
                Body: passThrough
            },
            tags: [{ Key: 'jobId', Value: jobId }],
            queueSize: 4, // number of concurrent uploads
            partSize: 1024 * 1024 * 5, // 5MB part size
        });

        // Handle archive events
        archive.on('progress', (progress) => {
            logger.info(`ZIP progress for chunk ${chunk.chunkId}:`, {
                jobId,
                filesProcessed: progress.entries.processed,
                totalBytes: progress.fs.processedBytes
            });
        });

        // Create promises for both the upload and archive completion
        const uploadPromise = upload.done();
        const archivePromise = new Promise((resolve, reject) => {
            archive.on('end', resolve);
            archive.on('error', reject);
        });

        // Finalize the archive (this is important!)
        archive.finalize();

        // Wait for both the upload and archive to complete
        await Promise.all([uploadPromise, archivePromise]);

        logger.info('Successfully processed chunk', {
            jobId,
            chunkId: chunk.chunkId,
            zipKey
        });

        await zipArchiveProgressService.updateChunkStatus(
            jobId,
            chunk.chunkId,
            'COMPLETED',
            null,
            zipKey
        );

        const isComplete = await zipArchiveProgressService.isJobComplete(jobId);
        if (isComplete) {
            await handleJobCompletion(jobId);
        }

        return zipKey;
    } catch (error) {
        logger.error('Error processing chunk', {
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

// Helper function to get S3 read stream
async function getS3ReadStream(s3Client, bucket, key) {
    const { Body } = await s3Client.send(new GetObjectCommand({
        Bucket: bucket,
        Key: key
    }));
    return Body;
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
            chunkCount: chunkKeys?.length
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
 * Process eligible tasks from the iterator
 * @param {string} jobId - The ID of the job
 * @param {AsyncGenerator} taskIterator - Iterator for eligible tasks
 */
async function processEligibleTasks(jobId, allImageKeys) {
    const chunks = [];
    let currentChunk;

    // First, collect all pending chunks
    while ((currentChunk = await zipArchiveProgressService.getNextPendingChunk(jobId)) !== false) {
        if (!currentChunk) continue;

        // Mark chunk as IN_PROGRESS to prevent other processes from picking it up
        await zipArchiveProgressService.updateChunkStatus(
            jobId,
            currentChunk.chunkId,
            'IN_PROGRESS'
        );

        chunks.push(currentChunk);
    }

    logger.info('Collected chunks for parallel processing', {
        jobId,
        chunkCount: chunks.length
    });

    if (chunks.length === 0) {
        logger.info('No chunks to process', { jobId });
        return;
    }

    // Process all chunks in parallel with Promise.all
    await Promise.all(
        chunks.map(chunk =>
            processChunk(jobId, chunk, allImageKeys).catch(error => {
                logger.error('Chunk processing failed', {
                    jobId,
                    chunkId: chunk.chunkId,
                    error: error.message,
                    stack: error.stack
                });
                // Update chunk status to FAILED
                return zipArchiveProgressService.updateChunkStatus(
                    jobId,
                    chunk.chunkId,
                    'FAILED',
                    error.message
                );
            })
        )
    );

    logger.info('Completed parallel processing of all chunks', {
        jobId,
        processedChunks: chunks.length
    });
}

/**
 * Collects all eligible tasks into an array
 * @param {string} jobId - The ID of the job
 * @returns {Promise<string[]>} Array of image S3 keys
 */
async function collectEligibleTasks(jobId) {
    const imageKeys = [];
    const taskIterator = fetchAllEligibleTasks(jobId);

    try {
        for await (const result of taskIterator) {
            const keys = result.items.map(task => task.ImageS3Key);
            imageKeys.push(...keys);
        }

        logger.info('Collected all eligible tasks', {
            jobId,
            totalKeys: imageKeys.length
        });

        return imageKeys;
    } catch (error) {
        logger.error('Error collecting eligible tasks', {
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
        recordCount: event?.Records?.length
    });

    console.log('event', JSON.stringify(event, null, 2));

    const batchItemFailures = [];

    for (const record of event.Records) {
        try {
            const body = JSON.parse(record.body);

            // Handle both SNS notifications and direct SQS messages
            const message = body.Message ? JSON.parse(body.Message) : body;
            const { jobId, status, action, additionalData } = message;

            console.log('additionalData', additionalData);

            let totalFiles = additionalData?.finalStatistics?.eligible;

            if (!totalFiles) {
                console.log('no totalFiles being passed in, fetching from DB');
                const currentJobProgress = await jobProgressService.getCurrentJobProgress(jobId);
                totalFiles = currentJobProgress?.statistics?.eligible || 0;
                console.log('totalFiles from DB', { currentJobProgress, totalFiles });
            }

            // Skip non-completed job statuses
            if (status !== 'COMPLETED') {
                logger.info('Skipping non-completed job status', {
                    jobId,
                    status,
                    action,
                    messageId: record.messageId
                });
                continue;
            }

            logger.info('Processing message', {
                jobId,
                status,
                action,
                totalFiles,
                messageId: record.messageId
            });

            // First check if metadata exists
            const metadata = await zipArchiveProgressService.getJobMetadata(jobId);

            // If no metadata and job is COMPLETED, initialize it
            if (!metadata) {
                logger.info('Initializing metadata for completed job', {
                    jobId,
                    totalFiles
                });

                if (!totalFiles) {
                    throw new Error('totalFiles is required for job initialization');
                }

                await zipArchiveProgressService.initializeProgress(jobId, totalFiles);
                console.log('metadata initialized');
            }

            if (action === 'PROCESS_NEXT_CHUNK') {
                logger.info(`Processing next chunk for job`, { jobId });
                const allImageKeys = await collectEligibleTasks(jobId);
                await processEligibleTasks(jobId, allImageKeys);
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
            const allImageKeys = await collectEligibleTasks(jobId);
            await processEligibleTasks(jobId, allImageKeys);

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
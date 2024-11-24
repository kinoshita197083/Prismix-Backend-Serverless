const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { S3Client } = require('@aws-sdk/client-s3');
const { SQSClient } = require('@aws-sdk/client-sqs');
const logger = require('../utils/logger');
const ZipArchiveProgressService = require('../services/zipArchiveProgressService');
const ZipMergeService = require('../services/zipMergeService');
const JobService = require('../services/jobService');
const JobProgressService = require('../services/jobProgressService');
const s3Service = require('../services/s3Service');
const ArchiveService = require('../services/archiveService');
const { PassThrough } = require('stream');
const { GetObjectCommand } = require('@aws-sdk/client-s3');
const archiver = require('archiver');
const { Upload } = require("@aws-sdk/lib-storage");

let pLimit;
(async () => {
    const module = await import('p-limit');
    pLimit = module.default;
})();

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
const jobService = new JobService(jobProgressService, zipArchiveProgressService, docClient);
const archiveService = new ArchiveService(s3Service);

const CONCURRENT_S3_OPERATIONS = 8;  // Increased from 4
const CONCURRENT_CHUNK_PROCESSING = 4;  // For batch processing
const ARCHIVER_HIGH_WATER_MARK = 4 * 1024 * 1024;  // Increased to 4MB

const FILE_PROCESSING_TIMEOUT = 180000;  // Increased to 3 minutes per file
const CHUNK_PROCESSING_TIMEOUT = 780000; // 13 minutes (leaving 2-minute buffer)
const BATCH_TIMEOUT = 780000;  // Matching chunk timeout

/**
 * Processes a single chunk of files
 * @param {string} jobId - The ID of the job
 * @param {Object} chunk - The chunk metadata
 * @param {string[]} allImageKeys - Array of all image S3 keys
 */
async function processChunk(jobId, chunk, allImageKeys) {
    // Ensure pLimit is loaded
    if (!pLimit) {
        const module = await import('p-limit');
        pLimit = module.default;
    }

    const { startIndex, endIndex, chunkId } = chunk;
    const chunkImageKeys = allImageKeys.slice(startIndex, endIndex);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const zipKey = `archives/${jobId}/chunks/${chunkId}_${timestamp}.zip`;

    // Step 1: Set up streams and error handling
    const passThrough = new PassThrough();
    const archive = archiver('zip', {
        zlib: { level: 2 },  // Decreased compression level for better speed
        store: true,
        forceZip64: true,
        statConcurrency: 8,  // Increased from 4
        highWaterMark: ARCHIVER_HIGH_WATER_MARK
    });

    // Step 2: Set up S3 upload
    const upload = new Upload({
        client: s3Client,
        params: {
            Bucket: process.env.DELIVERY_BUCKET,
            Key: zipKey,
            Body: passThrough,
            ContentType: 'application/zip'
        },
        queueSize: 8,  // Increased from 4
        partSize: 10 * 1024 * 1024,  // Increased to 10MB
        leavePartsOnError: false
    });

    // Log when passThrough is closed
    passThrough.on('close', () => {
        logger.info('PassThrough stream closed.');
    });

    // Function to cleanly end passThrough
    const endStream = () => {
        if (!passThrough.destroyed) {
            passThrough.end();
            logger.info('PassThrough stream ended.');
        }
    };

    try {
        // Step 3: Set up error handling and progress tracking
        // Log archive progress - should be removed when lambda is fully tested
        archive.on('progress', progress => {
            logger.debug('Archive progress', {
                jobId,
                chunkId,
                entries: progress.entries.processed,
                bytes: progress.fs.processedBytes
            });
        });

        // Step 4: Pipe archive to PassThrough
        archive.pipe(passThrough);

        // Step 5: Set up concurrent processing
        const limit = pLimit(CONCURRENT_S3_OPERATIONS);
        const processImage = async (imageKey) => {
            try {
                const fileName = imageKey.split('/').pop();
                const s3Stream = await getS3ObjectStream(process.env.SOURCE_BUCKET, imageKey);

                await new Promise((resolve, reject) => {
                    let bytesProcessed = 0;

                    s3Stream.on('data', chunk => {
                        bytesProcessed += chunk.length;
                    });

                    s3Stream.on('end', () => {
                        logger.debug('File processed', { jobId, chunkId, fileName, bytesProcessed });
                        resolve();
                    });

                    s3Stream.on('error', reject);

                    archive.append(s3Stream, { name: fileName })
                        .on('error', reject);
                });
            } catch (error) {
                logger.error('Error processing image', { jobId, chunkId, imageKey, error: error.message });
                throw error;
            }
        };

        // Wrapper with retry logic
        const processImageWithRetry = async (imageKey, retries = 3) => {
            for (let i = 0; i < retries; i++) {
                try {
                    await processImage(imageKey);
                    return;  // Exit if successful
                } catch (error) {
                    logger.warn(`Retrying ${imageKey} (${i + 1}/${retries}) due to error: ${error.message}`);
                    if (i === retries - 1) throw error; // Re-throw after final attempt
                }
            }
        };

        // Step 6: Process all images with controlled concurrency
        await Promise.all(
            chunkImageKeys.map(imageKey =>
                limit(() =>
                    Promise.race([
                        processImageWithRetry(imageKey),
                        new Promise((_, reject) =>
                            setTimeout(() => reject(new Error('File processing timeout')), FILE_PROCESSING_TIMEOUT)
                        )
                    ])
                )
            )
        );

        // Step 7: Finalize archive and wait for upload
        await Promise.all([
            new Promise((resolve, reject) => {
                archive.on('end', resolve);
                archive.on('error', reject);
                archive.finalize();
            }),
            upload.done()
        ]);

        // Step 8: Update chunk status
        await zipArchiveProgressService.updateChunkStatus(
            jobId, chunkId, 'COMPLETED', null, zipKey
        );

        return zipKey;
    } catch (error) {
        logger.error('Error in processChunk', {
            jobId,
            chunkId,
            error: error.message,
            stack: error.stack
        });

        if (archive && !archive.closed) {
            archive.abort();
        }

        await zipArchiveProgressService.updateChunkStatus(
            jobId, chunkId, 'FAILED', error.message, null
        );

        throw error;
    } finally {
        // Ensure passThrough is properly closed
        endStream();
    }
}

// Helper function with better error handling
async function getS3ObjectStream(bucket, key) {
    try {
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key
        });

        const response = await s3Client.send(command);

        logger.debug('Retrieved S3 object metadata', {
            bucket,
            key,
            contentLength: response.ContentLength,
            contentType: response.ContentType
        });

        // Don't pipe through PassThrough, return the body stream directly
        return response.Body;
    } catch (error) {
        logger.error('Error getting S3 object stream', {
            bucket,
            key,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Handles the completion of all chunks for a job
 * @param {string} jobId - The ID of the job
 */
async function handleJobCompletion(jobId) {
    try {
        logger.info('Starting final merge process', { jobId });

        const chunkKeys = await zipArchiveProgressService.getAllCompletedChunkKeys(jobId);

        if (!chunkKeys || chunkKeys.length === 0) {
            logger.error('No completed chunks found for merge', { jobId });
            return;
        }

        logger.info('Retrieved chunk keys for merge', {
            jobId,
            chunkCount: chunkKeys.length,
            chunkKeys
        });

        // If there's only one chunk, we can just use it as the final zip
        if (chunkKeys.length === 1) {
            const finalZipKey = chunkKeys[0];
            logger.info('Single chunk detected, using as final ZIP', {
                jobId,
                finalZipKey
            });

            await zipArchiveProgressService.updateFinalZipLocation(jobId, finalZipKey);

            logger.info('Successfully completed final merge', {
                jobId,
                finalZipKey
            });
            return;
        }

        // Otherwise, proceed with merge
        const finalZipKey = await zipMergeService.mergeChunks(
            jobId,
            chunkKeys,
            process.env.DELIVERY_BUCKET
        );

        await zipArchiveProgressService.updateFinalZipLocation(jobId, finalZipKey);

        await jobProgressService.updateJobProgress(jobId, {
            deliveryStatus: 'COMPLETED'
        });

        logger.info('Successfully completed final merge', {
            jobId,
            finalZipKey
        });

    } catch (error) {
        logger.error('Error during final merge', {
            jobId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

async function processChunkWithTimeout(jobId, chunk, allImageKeys) {
    return Promise.race([
        processChunk(jobId, chunk, allImageKeys),
        new Promise((_, reject) =>
            setTimeout(() => {
                const err = new Error('Chunk processing timeout');
                logger.error('Chunk processing timeout', {
                    jobId,
                    chunkId: chunk.chunkId
                });
                reject(err);
            }, CHUNK_PROCESSING_TIMEOUT)
        )
    ]);
}

async function processWithTimeout(promise, timeoutMs, operationName) {
    let timeoutId;

    const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
            reject(new Error(`${operationName} operation timed out after ${timeoutMs}ms`));
        }, timeoutMs);
    });

    try {
        const result = await Promise.race([promise, timeoutPromise]);
        clearTimeout(timeoutId);
        return result;
    } catch (error) {
        clearTimeout(timeoutId);
        throw error;
    }
}

async function processEligibleTasks(jobId, allImageKeys) {
    const logTaskStart = () => logger.info('Starting processEligibleTasks', {
        jobId,
        imageKeysCount: allImageKeys.length
    });

    const collectAndBatchChunks = async () => {
        const chunks = await jobService.collectPendingChunks(jobId);
        if (chunks.length === 0) {
            logger.info('No chunks to process', { jobId });
            return [];
        }
        return jobService.chunk(chunks, 3);
    };

    const processBatch = async (batch) => {
        logger.info('Processing batch', {
            jobId,
            batchSize: batch.length
        });

        await processWithTimeout(
            Promise.all(batch.map(chunk => processChunkWithTimeout(jobId, chunk, allImageKeys))),
            BATCH_TIMEOUT,
            'Batch processing'
        );

        logger.info('Batch processing completed', {
            jobId,
            batchSize: batch.length
        });
    };

    const checkAllChunksStatus = async () => {
        const allChunksStatus = await zipArchiveProgressService.getAllChunksStatus(jobId);
        logger.info('Retrieved chunks status', {
            jobId,
            chunksCount: allChunksStatus.length,
            statuses: allChunksStatus.map(chunk => ({
                chunkId: chunk.chunkId,
                status: chunk.status
            }))
        });

        const allCompleted = allChunksStatus.every(chunk => chunk.status === 'COMPLETED');
        const hasFailures = allChunksStatus.some(chunk => chunk.status === 'FAILED');

        if (hasFailures) {
            logger.error('Some chunks failed processing', {
                jobId,
                failedChunks: allChunksStatus.filter(chunk => chunk.status === 'FAILED').map(chunk => chunk.chunkId)
            });
            throw new Error('Some chunks failed processing');
        }

        return allCompleted;
    };

    try {
        logTaskStart();
        const chunkBatches = await collectAndBatchChunks();
        if (chunkBatches.length === 0) return;

        for (const batch of chunkBatches) {
            await processBatch(batch);
        }

        logger.info('Completed processing all chunks', {
            jobId,
            processedChunks: chunkBatches.flat().length
        });

        if (await checkAllChunksStatus()) {
            logger.info('All chunks completed, starting final merge', { jobId });
            await handleJobCompletion(jobId);
            logger.info('Job fully completed, exiting', { jobId });
            return true;
        } else {
            logger.info('Not all chunks completed yet', { jobId });
            return false;
        }
    } catch (error) {
        logger.error('Error in processEligibleTasks', {
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
    const logHandlerStart = () => logger.info('Starting handler execution', {
        recordCount: event?.Records?.length,
        timestamp: new Date().toISOString()
    });

    console.log('Received event:', JSON.stringify(event, null, 2));

    const parseRecord = (record) => {
        const body = JSON.parse(record.body);
        return body.Message ? JSON.parse(body.Message) : body;
    };

    const processRecord = async (record) => {
        logger.info('Processing record', {
            messageId: record.messageId,
            timestamp: new Date().toISOString()
        });

        try {
            const message = parseRecord(record);
            const { jobId, status, action, additionalData } = message;

            logger.info('Parsed message details', {
                jobId,
                status,
                action,
                timestamp: new Date().toISOString()
            });

            if (status !== 'COMPLETED') {
                logger.info('Skipping non-completed job', { jobId, status });
                return;
            }

            // TODO: check output connection in job progress table and skip if not for local delivery

            // const totalFiles = await fetchTotalFiles(jobId, additionalData);
            const { statistics, outputConnection } = await jobProgressService.getCurrentJobProgress(jobId);
            const totalFiles = statistics?.eligible;

            console.log('[processRecord] totalFiles', totalFiles);
            console.log('[processRecord] outputConnection', outputConnection);

            if (!totalFiles) {
                logger.info('No totalFiles found, skipping job', { jobId });
                await jobProgressService.updateJobProgress(jobId, {
                    deliveryStatus: 'SKIPPED'
                });
                return;
            }

            if (outputConnection !== 'local') {
                logger.info('Skipping non-local job', { jobId, outputConnection });
                await jobProgressService.updateJobProgress(jobId, {
                    deliveryStatus: 'SKIPPED'
                });
                return;
            }

            if (!await jobService.initializeJobIfNeeded(jobId, totalFiles)) {
                logger.info('Job initialization skipped', { jobId });
                return;
            }

            // Set job delivery status to IN_PROGRESS
            await jobProgressService.updateJobProgress(jobId, {
                deliveryStatus: 'IN_PROGRESS'
            });

            if (action === 'PROCESS_NEXT_CHUNK' || status === 'COMPLETED') {
                logger.info('Starting eligible tasks processing', { jobId });
                const allImageKeys = await jobService.collectEligibleTasks(jobId);
                const completed = await processEligibleTasks(jobId, allImageKeys);

                if (completed) {
                    logger.info('Job processing completed successfully', { jobId });
                }
            }
        } catch (error) {
            logger.error('Error processing record', {
                messageId: record.messageId,
                error: error.message,
                stack: error.stack,
                code: error.code,
                name: error.name,
                details: JSON.stringify(error, Object.getOwnPropertyNames(error)),
                timestamp: new Date().toISOString()
            });

            // Set job delivery status to FAILED
            await jobProgressService.updateJobProgress(jobId, {
                deliveryStatus: 'FAILED'
            });

            return { itemIdentifier: record.messageId };
        }
    };

    logHandlerStart();
    const batchItemFailures = await Promise.all(event.Records.map(processRecord)).then(results => results.filter(Boolean));

    logger.info('Handler execution completed', {
        batchItemFailuresCount: batchItemFailures.length,
        timestamp: new Date().toISOString()
    });

    return { batchItemFailures };
}; 
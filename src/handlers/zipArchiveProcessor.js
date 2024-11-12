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

/**
 * Processes a single chunk of files
 * @param {string} jobId - The ID of the job
 * @param {Object} chunk - The chunk metadata
 * @param {string[]} allImageKeys - Array of all image S3 keys
 */
async function processChunk(jobId, chunk, allImageKeys) {
    const { startIndex, endIndex, chunkId } = chunk;
    const chunkImageKeys = allImageKeys.slice(startIndex, endIndex);
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const zipKey = `archives/${jobId}/chunks/${chunkId}_${timestamp}.zip`;

    const { archive, upload } = await archiveService.createArchiveStream(
        jobId,
        zipKey,
        process.env.DELIVERY_BUCKET
    );

    try {
        await archiveService.streamFilesToArchive(archive, chunkImageKeys, process.env.SOURCE_BUCKET);
        await archiveService.finalizeArchive(archive, upload);

        await zipArchiveProgressService.updateChunkStatus(
            jobId, chunkId, 'COMPLETED', null, zipKey
        );

        const isComplete = await zipArchiveProgressService.isJobComplete(jobId);
        if (isComplete) {
            await handleJobCompletion(jobId);
        }

        return zipKey;
    } catch (error) {
        await zipArchiveProgressService.updateChunkStatus(
            jobId, chunkId, 'FAILED', error.message
        );
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

        const finalZipKey = await zipMergeService.mergeChunks(
            jobId,
            chunkKeys,
            process.env.DELIVERY_BUCKET
        );

        await zipArchiveProgressService.updateFinalZipLocation(jobId, finalZipKey);

        logger.info('Successfully completed final merge', {
            jobId,
            finalZipKey
        });

        // Here you might want to trigger a notification or update other systems
        // about the availability of the final ZIP file

    } catch (error) {
        logger.error('Error during final merge', {
            jobId,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

async function processChunkWithRetry(jobId, chunk, allImageKeys, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await processChunk(jobId, chunk, allImageKeys);
        } catch (error) {
            logger.warn('Chunk processing attempt failed', {
                jobId,
                chunkId: chunk.chunkId,
                attempt,
                error: error.message
            });

            if (attempt === retries) {
                logger.error('All retry attempts failed', {
                    jobId,
                    chunkId: chunk.chunkId,
                    error: error.message,
                    stack: error.stack
                });
                throw error;
            }

            // Exponential backoff
            await new Promise(resolve =>
                setTimeout(resolve, Math.pow(2, attempt) * 1000)
            );
        }
    }
}

async function processEligibleTasks(jobId, allImageKeys) {
    const chunks = await jobService.collectPendingChunks(jobId);

    if (chunks.length === 0) {
        logger.info('No chunks to process', { jobId });
        return;
    }

    const concurrencyLimit = 3;
    const chunkBatches = jobService.chunk(chunks, concurrencyLimit);

    for (const batch of chunkBatches) {
        await Promise.all(
            batch.map(chunk =>
                processChunkWithRetry(jobId, chunk, allImageKeys)
            )
        );
    }

    logger.info('Completed processing all chunks', {
        jobId,
        processedChunks: chunks.length
    });
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

            if (status !== 'COMPLETED') {
                console.log('job not completed, skipping');
                continue;
            }

            let totalFiles = additionalData?.finalStatistics?.eligible;

            if (!totalFiles) {
                console.log('no totalFiles being passed in, fetching from DB');
                const currentJobProgress = await jobProgressService.getCurrentJobProgress(jobId);
                totalFiles = currentJobProgress?.statistics?.eligible || 0;
                console.log('totalFiles from DB', { currentJobProgress, totalFiles });
            }

            if (!await jobService.initializeJobIfNeeded(jobId, totalFiles)) {
                continue;
            }

            if (action === 'PROCESS_NEXT_CHUNK' || status === 'COMPLETED') {
                const allImageKeys = await jobService.collectEligibleTasks(jobId);
                await processEligibleTasks(jobId, allImageKeys);
            }

        } catch (error) {
            logger.error('Error processing record', {
                messageId: record.messageId,
                error: error.message,
                stack: error.stack
            });
            batchItemFailures.push({ itemIdentifier: record.messageId });
        }
    }

    return { batchItemFailures };
}; 
const { Consumer } = require('sqs-consumer');
const { S3Client } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const logger = require('../../utils/logger');
const { ZipMergeService } = require('../../services/zipMergeService');
const { JobProgressService } = require('../../services/jobProgressService');
const { JobService } = require('../../services/jobService');
const { ArchiveService } = require('../../services/archiveService');

// Initialize AWS clients
const s3Client = new S3Client();
const docClient = DynamoDBDocumentClient.from(new DynamoDBClient());

// Initialize services
const zipMergeService = new ZipMergeService(s3Client);
const jobProgressService = new JobProgressService(docClient, null, {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
});
const jobService = new JobService(jobProgressService, zipArchiveProgressService, docClient);
const archiveService = new ArchiveService(s3Service);

// Constants for archive processing
const CONCURRENT_S3_OPERATIONS = 16;  // Increased for ECS
const CONCURRENT_CHUNK_PROCESSING = 8;  // Increased for ECS
const ARCHIVER_HIGH_WATER_MARK = 8 * 1024 * 1024;  // 8MB buffer

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.ARCHIVE_QUEUE_URL,
    handleMessage: async (message) => {
        const { jobId, userId, projectId } = JSON.parse(message.Body);
        logger.info('Starting archive processing', { jobId, userId, projectId });

        try {
            // Get all image keys for the job
            const imageKeys = await jobProgressService.getSuccessfulTaskKeys(jobId);

            if (!imageKeys.length) {
                throw new Error('No successful tasks found for archiving');
            }

            // Process archive in chunks
            const chunks = chunk(imageKeys, parseInt(process.env.CHUNK_SIZE, 10));
            logger.info('Processing archive chunks', {
                totalChunks: chunks.length,
                totalImages: imageKeys.length
            });

            for (const [index, chunkKeys] of chunks.entries()) {
                const chunkId = `${jobId}-chunk-${index}`;
                await processArchiveChunk({
                    jobId,
                    chunkId,
                    imageKeys: chunkKeys,
                    userId,
                    projectId
                });

                // Update progress
                await jobProgressService.updateArchiveProgress(jobId, {
                    processedChunks: index + 1,
                    totalChunks: chunks.length
                });
            }

            // Merge chunks if necessary
            if (chunks.length > 1) {
                await zipMergeService.mergeChunks(jobId, chunks.length);
            }

            // Mark job as complete
            await jobProgressService.markArchiveComplete(jobId);
            logger.info('Archive processing completed', { jobId });

        } catch (error) {
            logger.error('Archive processing failed', {
                jobId,
                error: error.message
            });
            await jobProgressService.markArchiveFailed(jobId, error.message);
            throw error; // Let SQS handle the retry
        }
    },
    batchSize: 1, // Process one archive job at a time
    visibilityTimeout: 7200, // 2 hours
    messageAttributeNames: ['All']
});

// Error handling
app.on('error', (err) => {
    logger.error('Consumer error', { error: err.message });
});

app.on('processing_error', (err) => {
    logger.error('Processing error', { error: err.message });
});

// Start the consumer
app.start();
logger.info('Archive processor started');

// Helper function to chunk arrays
function chunk(array, size) {
    return Array.from(
        { length: Math.ceil(array.length / size) },
        (_, index) => array.slice(index * size, (index + 1) * size)
    );
}

// Helper function to process archive chunks
async function processArchiveChunk({ jobId, chunkId, imageKeys, userId, projectId }) {
    logger.info('Processing archive chunk', {
        jobId,
        chunkId,
        imageCount: imageKeys.length
    });

    const archiveKey = `archives/${userId}/${projectId}/${jobId}/${chunkId}.zip`;

    await archiveService.createArchive({
        imageKeys,
        destinationKey: archiveKey,
        concurrentOperations: CONCURRENT_S3_OPERATIONS,
        highWaterMark: ARCHIVER_HIGH_WATER_MARK
    });

    return archiveKey;
} 
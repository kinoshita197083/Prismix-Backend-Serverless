
const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

/**
 * Configuration for pagination and batch processing
 */
const PAGINATION_CONFIG = {
    maxBatchSize: 100,
    maxPages: 1000,
    scanIndexForward: true
};

class JobService {
    constructor(jobProgressService, zipArchiveProgressService, docClient) {
        this.jobProgressService = jobProgressService;
        this.zipArchiveProgressService = zipArchiveProgressService;
        this.docClient = docClient;
    }

    async initializeJobIfNeeded(jobId, totalFiles) {
        const metadata = await this.zipArchiveProgressService.getJobMetadata(jobId);

        if (!metadata && totalFiles) {
            logger.info('Initializing metadata for job', { jobId, totalFiles });
            await this.zipArchiveProgressService.initializeProgress(jobId, totalFiles);
            return true;
        }

        if (metadata?.Status === 'COMPLETED') {
            logger.info('Job already completed', { jobId });
            return false;
        }

        return true;
    }

    /**
     * Generator function to fetch all eligible tasks in batches
     * @param {string} jobId - The ID of the job
     * @param {Object} lastEvaluatedKey - Key for pagination
     */
    async *fetchAllEligibleTasks(jobId, lastEvaluatedKey) {
        let pageCount = 0;

        do {
            if (pageCount >= PAGINATION_CONFIG.maxPages) {
                logger.warn(`Reached maximum page limit for job ${jobId}`, {
                    maxPages: PAGINATION_CONFIG.maxPages
                });
                break;
            }

            const result = await this.fetchEligibleTasks(jobId, lastEvaluatedKey);
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
    async fetchEligibleTasks(jobId, lastEvaluatedKey = null) {
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
            const result = await this.docClient.send(command);

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
     * Collects all eligible tasks into an array
     * @param {string} jobId - The ID of the job
     * @returns {Promise<string[]>} Array of image S3 keys
     */
    async collectEligibleTasks(jobId) {
        const imageKeys = [];
        const taskIterator = this.fetchAllEligibleTasks(jobId);

        try {
            for await (const result of taskIterator) {
                const keys = result.items.map(task => task.ImageS3Key);
                imageKeys.push(...keys);
            }

            logger.info('Collected eligible tasks', {
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

    async collectPendingChunks(jobId) {
        const chunks = [];
        let currentChunk;

        while ((currentChunk = await this.zipArchiveProgressService.getNextPendingChunk(jobId)) !== false) {
            if (!currentChunk) continue;

            await this.zipArchiveProgressService.updateChunkStatus(
                jobId,
                currentChunk.chunkId,
                'IN_PROGRESS'
            );

            chunks.push(currentChunk);
        }

        logger.info('Collected pending chunks', {
            jobId,
            chunkCount: chunks.length
        });

        return chunks;
    }

    chunk(array, size) {
        const chunks = [];
        for (let i = 0; i < array.length; i += size) {
            chunks.push(array.slice(i, i + size));
        }
        return chunks;
    }
}

module.exports = JobService;

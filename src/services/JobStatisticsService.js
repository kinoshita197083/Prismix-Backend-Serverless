const { QueryCommand } = require("@aws-sdk/lib-dynamodb");

const createJobStatisticsService = (jobProgressService, cloudWatchService) => {
    const PAGINATION_CONFIG = {
        maxPages: 100,
        maxBatchSize: 100,
        scanIndexForward: true
    };

    const getJobStatisticsWithPagination = async (jobId, jobProgress) => {
        const stats = {
            totalProcessed: 0,
            eligible: 0,
            excluded: 0,
            duplicates: 0,
            waitingForReview: 0,
            lastEvaluatedKey: null
        };

        let lastEvaluatedKey = jobProgress.lastProcessedKey;
        let pageCount = 0;

        try {
            do {
                if (pageCount >= PAGINATION_CONFIG.maxPages) {
                    console.log(`[getJobStatisticsWithPagination] Reached maximum page limit (${PAGINATION_CONFIG.maxPages}) for job ${jobId}`);
                    break;
                }

                const result = await fetchTasksBatch(jobId, lastEvaluatedKey);

                if (!result.Items || result.Items.length === 0) {
                    console.log('[getJobStatisticsWithPagination] No items found in batch');
                    break;
                }

                updateStatsFromItems(stats, result.Items);

                lastEvaluatedKey = result.LastEvaluatedKey;
                pageCount++;

                console.log('[getJobStatisticsWithPagination] Processed batch:', {
                    batchSize: result.Items.length,
                    currentStats: { ...stats },
                    hasMore: !!lastEvaluatedKey
                });

                stats.lastEvaluatedKey = lastEvaluatedKey;

                if (!lastEvaluatedKey) {
                    break;
                }
            } while (true);

            return stats;
        } catch (error) {
            console.error('[getJobStatisticsWithPagination] Error getting job statistics:', error);
            throw error;
        }
    };

    const fetchTasksBatch = async (jobId, exclusiveStartKey = null) => {
        const params = {
            TableName: process.env.TASKS_TABLE,
            KeyConditionExpression: 'JobID = :jobId',
            ExpressionAttributeValues: {
                ':jobId': jobId
            },
            Limit: PAGINATION_CONFIG.maxBatchSize,
            ScanIndexForward: PAGINATION_CONFIG.scanIndexForward
        };

        if (exclusiveStartKey) {
            params.ExclusiveStartKey = exclusiveStartKey;
        }

        return await jobProgressService.dynamoDB.send(new QueryCommand(params));
    };

    const updateStatsFromItems = (stats, items) => {
        console.log('Updating stats from items:', items);
        for (const item of items) {
            stats.totalProcessed++;

            // Update stats based on evaluation
            switch (item.Evaluation) {
                case 'ELIGIBLE':
                    stats.eligible++;
                    break;
                case 'EXCLUDED':
                    stats.excluded++;
                    break;
                case 'DUPLICATE':
                    stats.duplicates++;
                    break;
            }

            // Update stats based on task status
            switch (item.TaskStatus) {
                case 'WAITING_FOR_REVIEW':
                    stats.waitingForReview++;
                    break;
            }
        }
    };

    const recordJobMetrics = async (jobId, stats) => {
        const metrics = {
            totalProcessed: stats.totalProcessed,
            processingRate: stats.totalProcessed / (Date.now() - new Date(stats.startTime).getTime()) * 1000,
            errorRate: stats.errors / stats.totalProcessed || 0,
            memoryUsage: process.memoryUsage().heapUsed / process.memoryUsage().heapTotal
        };

        await cloudWatchService.recordMetrics('JobProgress', {
            ...metrics,
            jobId,
            success: true
        });
    };

    return {
        getJobStatisticsWithPagination,
        recordJobMetrics
    };
};

module.exports = { createJobStatisticsService }; 
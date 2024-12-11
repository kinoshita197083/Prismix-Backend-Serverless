const { DynamoDBDocumentClient, GetCommand, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { createClient } = require('@supabase/supabase-js');
const { sleep } = require("../utils/helpers");

class JobProgressService {
    constructor(dynamoDB, jobStatisticsService, config, notificationService) {
        this.dynamoDB = DynamoDBDocumentClient.from(dynamoDB, {
            marshallOptions: {
                removeUndefinedValues: true,
            }
        });
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_API_KEY
        );
        this.tasksTable = config.tasksTable;
        this.jobProgressTable = config.jobProgressTable;
        this.jobStatisticsService = jobStatisticsService;
        this.notificationService = notificationService;
    }

    async updateJobStatusAndNotify(jobId, status) {
        console.log('[JobProgressService.updateJobStatusAndNotify] Updating job status and notifying:', {
            jobId,
            status
        });

        try {
            await Promise.all([
                this.notificationService.publishJobStatus(jobId, status, {
                    completedAt: Date.now().toString(),
                    status: status
                }),
                this.updateJobStatusRDS(jobId, status)
            ]);
        } catch (error) {
            console.error('[JobProgressService.updateJobStatusAndNotify] Error updating job status and notifying:', error);
            throw error;
        }
    }

    async getCurrentJobProgress(jobId) {
        console.log('[JobProgressService.getCurrentJobProgress] Fetching progress for jobId:', jobId);

        const result = await this.dynamoDB.send(new GetCommand({
            TableName: this.jobProgressTable,
            Key: { JobId: jobId }
        }));

        console.log('[JobProgressService.getCurrentJobProgress] Result:', result);

        if (!result.Item) {
            console.warn('[JobProgressService.getCurrentJobProgress] No progress found for jobId:', jobId);
            throw new Error('Job progress not found');
        }

        console.log('[JobProgressService.getCurrentJobProgress] Returning progress:', result.Item);
        return result.Item;
    }

    async updateJobStatusRDS(jobId, status, maxRetries = 3, baseDelay = 1000) {
        let retryCount = 0;

        while (true) {
            try {
                console.log('[JobProgressService.updateJobStatusRDS] Updating job status:', {
                    jobId,
                    status,
                    attempt: retryCount + 1
                });

                const { data, error } = await this.supabase
                    .from('Job')
                    .update({ jobStatus: status })
                    .eq('id', jobId)
                    .single();

                if (error) throw error;
                return data;

            } catch (error) {
                retryCount++;

                // If we've exhausted all retries, throw the last error
                if (retryCount >= maxRetries) {
                    console.error(`[JobProgressService.updateJobStatusRDS] Failed after ${maxRetries} attempts:`, error);
                    throw error;
                }

                // Calculate delay with exponential backoff: 1s, 2s, 4s
                const delay = baseDelay * Math.pow(2, retryCount - 1);

                console.warn(`[JobProgressService.updateJobStatusRDS] Attempt ${retryCount} failed. Retrying in ${delay}ms:`, error);

                // Wait before the next retry
                await sleep(delay);
            }
        }
    }

    async markJobAsStale(jobId) {
        console.log('[JobProgressService.markJobAsStale] Marking job as stale:', jobId);

        const params = {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: 'SET #status = :status, staleReason = :reason, updatedAt = :updatedAt',
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: {
                ':status': 'STALE',
                ':reason': 'Job inactive for too long',
                ':updatedAt': Date.now().toString()
            }
        };

        await this.dynamoDB.send(new UpdateCommand(params));
        console.log('[JobProgressService.markJobAsStale] Job marked as stale successfully');
    }

    async adjustJobStatistics(jobId) {
        // Update final statistics considering auto-reviewed items
        const finalStats = await this.jobStatisticsService.getJobStatisticsWithPagination(jobId);
        const adjustedStats = {
            ...finalStats,
            // Move waiting for review count to excluded count
            excluded: finalStats.excluded + finalStats.waitingForReview,
            waitingForReview: 0,
            autoReviewed: finalStats.waitingForReview // Track how many were auto-reviewed
        };

        // Update job progress with adjusted statistics
        await this.updateJobProgress(jobId, {
            statistics: adjustedStats
        });
        return adjustedStats;
    }

    async getJobStatistics(jobId) {
        const params = {
            TableName: this.tasksTable,
            KeyConditionExpression: 'JobID = :jobId',
            ExpressionAttributeValues: {
                ':jobId': jobId
            }
        };

        const result = await this.dynamoDB.send(new QueryCommand(params));
        const tasks = result.Items || [];

        return tasks.reduce((stats, task) => {
            stats.totalProcessed++;

            switch (task.Evaluation) {
                case 'ELIGIBLE':
                    stats.eligible++;
                    break;
                case 'EXCLUDED':
                    stats.excluded++;
                    break;
                case 'WAITING_FOR_REVIEW':
                    stats.waitingForReview++;
                    break;
            }

            // Count duplicates if present
            if (task.isDuplicate) {
                stats.duplicates++;
            }

            return stats;
        }, {
            totalProcessed: 0,
            eligible: 0,
            excluded: 0,
            waitingForReview: 0,
            duplicates: 0
        });
    }

    async updateJobProgress(jobId, updates) {
        const updateExpressions = [];
        const expressionAttributeNames = {};
        const expressionAttributeValues = {};

        // Build dynamic update expression
        Object.entries(updates).forEach(([key, value]) => {
            if (value === undefined || value === null) return console.log(`[JobProgressService.updateJobProgress] Skipping update for key: ${key} with value: ${value}`);
            const attributeName = `#${key}`;
            const attributeValue = `:${key}`;
            updateExpressions.push(`${attributeName} = ${attributeValue}`);
            expressionAttributeNames[attributeName] = key;
            expressionAttributeValues[attributeValue] = value;
        });

        // Always update the timestamp
        updateExpressions.push('#updatedAt = :updatedAt');
        expressionAttributeNames['#updatedAt'] = 'updatedAt';
        expressionAttributeValues[':updatedAt'] = new Date().toISOString();

        const params = {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: `SET ${updateExpressions.join(', ')}`,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
            ConditionExpression: updates.currentVersion ? 'attribute_not_exists(version) OR version = :currentVersion' : undefined,
            ReturnValues: 'ALL_NEW'
        };

        try {
            const result = await this.dynamoDB.send(new UpdateCommand(params));
            return result.Attributes;
        } catch (error) {
            console.error('Failed to update job progress', error);
            throw error;
        }
    }

    async getCircuitBreakerState(jobId) {
        console.log('[JobProgressService.getCircuitBreakerState] Fetching circuit breaker state for jobId:', jobId);

        const result = await this.dynamoDB.send(new GetCommand({
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            ProjectionExpression: 'circuitBreakerState'
        }));

        if (!result.Item?.circuitBreakerState) {
            return {
                state: 'CLOSED',
                failures: 0,
                lastFailure: null
            };
        }

        return result.Item.circuitBreakerState;
    }

    async updateCircuitBreakerState(jobId, newState) {
        console.log('[JobProgressService.updateCircuitBreakerState] Updating circuit breaker state:', {
            jobId,
            newState
        });

        const params = {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: 'SET circuitBreakerState = :state, updatedAt = :updatedAt',
            ExpressionAttributeValues: {
                ':state': newState,
                ':updatedAt': Date.now().toString()
            },
            ReturnValues: 'ALL_NEW'
        };

        try {
            const result = await this.dynamoDB.send(new UpdateCommand(params));
            console.log('[JobProgressService.updateCircuitBreakerState] Update successful:', result.Attributes);
            return result.Attributes.circuitBreakerState;
        } catch (error) {
            console.error('[JobProgressService.updateCircuitBreakerState] Update failed:', error);
            throw error;
        }
    }
}

module.exports = JobProgressService; 
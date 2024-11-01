const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { SNSClient } = require('@aws-sdk/client-sns');
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { EventBridgeClient } = require("@aws-sdk/client-eventbridge");
const { createClient } = require('@supabase/supabase-js');
const JobProgressService = require('../services/jobProgressService');
const NotificationService = require('../services/notificationService');
const EventBridgeService = require('../services/eventBridgeService');
const { AppError, ErrorCodes } = require('../utils/errorHandler');
const { CloudWatchClient } = require('@aws-sdk/client-cloudwatch');

// Initialize CloudWatch client
const cloudWatch = new CloudWatchClient();

const sqs = new SQSClient();

// Initialize clients
const dynamoDBClient = new DynamoDBClient();
const documentClient = DynamoDBDocumentClient.from(dynamoDBClient, {
    marshallOptions: {
        removeUndefinedValues: true,
    },
});

// Initialize services
const jobProgressService = new JobProgressService(
    documentClient,
    createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY),
    {
        tasksTable: process.env.TASKS_TABLE,
        jobProgressTable: process.env.JOB_PROGRESS_TABLE
    }
);

const notificationService = new NotificationService(new SNSClient(), process.env.JOB_COMPLETION_TOPIC_ARN);
const eventBridgeService = new EventBridgeService(new EventBridgeClient());

// Error types that should not trigger a retry
const TERMINAL_ERRORS = [
    ErrorCodes.DATABASE.DYNAMO_QUERY_ERROR,
    'JobNotFound',
    'InvalidJobState'
];

// Add these constants at the top
const JOB_TIMEOUTS = {
    PROCESSING: 24 * 60 * 60 * 1000,    // 24 hours for processing
    REVIEW: 72 * 60 * 60 * 1000,        // 72 hours for initial review period
    REVIEW_EXTENSION: 24 * 60 * 60 * 1000, // 24 hours extension period
    INACTIVITY: 30 * 60 * 1000          // 30 minutes of inactivity
};

// Add pagination configuration
const PAGINATION_CONFIG = {
    maxBatchSize: 100,
    maxPages: 10,  // Limit total pages per execution
    scanIndexForward: true
};

// Add performance metrics and monitoring
const PERFORMANCE_METRICS = {
    SLOW_QUERY_THRESHOLD: 1000, // 1 second
    MAX_RETRIES_PER_BATCH: 3,
    BATCH_TIMEOUT: 5000, // 5 seconds
};

// Add custom error types
class JobProcessingError extends Error {
    constructor(code, message, details = {}) {
        super(message);
        this.name = 'JobProcessingError';
        this.code = code;
        this.details = details;
    }
}

// Add performance monitoring wrapper
async function withPerformanceTracking(operationName, fn) {
    const startTime = Date.now();
    try {
        const result = await fn();
        const duration = Date.now() - startTime;

        // Log performance metrics
        if (duration > PERFORMANCE_METRICS.SLOW_QUERY_THRESHOLD) {
            console.warn('Slow operation detected:', {
                operation: operationName,
                duration,
                threshold: PERFORMANCE_METRICS.SLOW_QUERY_THRESHOLD
            });
        }

        // Track metrics
        await recordMetrics(operationName, {
            duration,
            success: true
        });

        return result;
    } catch (error) {
        const duration = Date.now() - startTime;

        // Track error metrics
        await recordMetrics(operationName, {
            duration,
            success: false,
            errorCode: error.code
        });

        throw error;
    }
}

async function recordMetrics(operationName, metrics) {
    try {
        // Create CloudWatch metrics
        const timestamp = new Date();
        const dimensions = [
            { Name: 'Operation', Value: operationName },
            { Name: 'Environment', Value: process.env.STAGE }
        ];

        const metricData = [
            {
                MetricName: 'OperationDuration',
                Value: metrics.duration,
                Unit: 'Milliseconds',
                Timestamp: timestamp,
                Dimensions: dimensions
            },
            {
                MetricName: 'OperationSuccess',
                Value: metrics.success ? 1 : 0,
                Unit: 'Count',
                Timestamp: timestamp,
                Dimensions: dimensions
            }
        ];

        if (!metrics.success) {
            metricData.push({
                MetricName: 'OperationError',
                Value: 1,
                Unit: 'Count',
                Timestamp: timestamp,
                Dimensions: [...dimensions, { Name: 'ErrorCode', Value: metrics.errorCode }]
            });
        }

        await cloudWatch.putMetricData({
            Namespace: 'JobProgress',
            MetricData: metricData
        });
    } catch (error) {
        console.error('Error recording metrics:', error);
        // Don't throw - metrics recording should not affect main flow
    }
}

// Add batch processing with retries and circuit breaker
class CircuitBreaker {
    constructor(jobId) {
        this.jobId = jobId;
        this.stateKey = `circuit-breaker-${jobId}`;
    }

    async getState() {
        try {
            const result = await jobProgressService.dynamoDB.send(new QueryCommand({
                TableName: process.env.JOB_PROGRESS_TABLE,
                Key: { JobId: this.jobId },
                ProjectionExpression: 'circuitBreakerState'
            }));
            return result.Item?.circuitBreakerState || { state: 'CLOSED', failures: 0, lastFailure: null };
        } catch (error) {
            console.error('Error fetching circuit breaker state:', error);
            return { state: 'CLOSED', failures: 0, lastFailure: null };
        }
    }

    async setState(newState) {
        await jobProgressService.dynamoDB.send(new UpdateCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: this.jobId },
            UpdateExpression: 'SET circuitBreakerState = :state',
            ExpressionAttributeValues: {
                ':state': newState
            }
        }));
    }

    async execute(operation) {
        const state = await this.getState();
        if (state.state === 'OPEN') {
            if (Date.now() - state.lastFailure < 30000) { // 30 seconds cooling period
                throw new Error('Circuit breaker is OPEN');
            }
            await this.setState({ state: 'HALF_OPEN', lastFailure: Date.now() });
        }

        try {
            const result = await operation();
            if (state.state === 'HALF_OPEN') {
                await this.setState({ state: 'CLOSED', failures: 0 });
            }
            return result;
        } catch (error) {
            await this.setState({ state: 'OPEN', lastFailure: Date.now() });
            throw error;
        }
    }
}

const circuitBreaker = new CircuitBreaker();

// Enhanced batch processing
async function processBatchWithRetries(jobId, batch, retryCount = 0) {
    try {
        return await circuitBreaker.execute(async () => {
            const result = await withPerformanceTracking(
                'ProcessBatch',
                () => processTaskBatch(jobId, batch)
            );
            return result;
        });
    } catch (error) {
        if (retryCount < PERFORMANCE_METRICS.MAX_RETRIES_PER_BATCH) {
            console.log(`Retrying batch for job ${jobId}, attempt ${retryCount + 1}`);
            await new Promise(resolve => setTimeout(resolve, Math.pow(2, retryCount) * 1000));
            return processBatchWithRetries(jobId, batch, retryCount + 1);
        }
        throw error;
    }
}

// Add health check functionality
async function performHealthCheck(jobId) {
    const healthStatus = {
        jobId,
        timestamp: new Date().toISOString(),
        checks: {}
    };

    try {
        // Check DynamoDB access
        const startTime = Date.now();
        await jobProgressService.getCurrentJobProgress(jobId);
        healthStatus.checks.dynamodb = {
            status: 'OK',
            latency: Date.now() - startTime
        };
    } catch (error) {
        healthStatus.checks.dynamodb = {
            status: 'ERROR',
            error: error.message
        };
    }

    // Add memory usage check
    const memoryUsage = process.memoryUsage();
    healthStatus.checks.memory = {
        status: memoryUsage.heapUsed / memoryUsage.heapTotal < MEMORY_THRESHOLD ? 'OK' : 'WARNING',
        usage: {
            used: Math.round(memoryUsage.heapUsed / 1024 / 1024),
            total: Math.round(memoryUsage.heapTotal / 1024 / 1024),
            percentage: Math.round((memoryUsage.heapUsed / memoryUsage.heapTotal) * 100)
        }
    };

    // Log health check results
    console.log('Health check results:', healthStatus);
    return healthStatus;
}

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    const batchItemFailures = [];

    for (const record of event.Records) {
        const messageId = record.messageId;
        try {
            const body = JSON.parse(record.body);
            const jobId = body.jobId;
            const eventType = record.messageAttributes?.eventType?.stringValue;

            if (!jobId) {
                console.error('Invalid message format - missing jobId:', body);
                continue;
            }

            // Wrap the entire processing in performance tracking
            await withPerformanceTracking('ProcessJobMessage', async () => {
                // Handle different event types
                if (eventType === 'REVIEW_COMPLETED') {
                    await handleReviewCompletion(jobId);
                } else {
                    await handleRegularProgressCheck(jobId);
                }
            });

        } catch (error) {
            console.error('Error processing record:', error);
            // ... error handling ...
        }
    }

    return { batchItemFailures };
};

async function handleRegularProgressCheck(jobId) {
    try {
        // Perform health check
        const healthStatus = await performHealthCheck(jobId);
        if (Object.values(healthStatus.checks).some(check => check.status === 'ERROR')) {
            throw new JobProcessingError('HEALTH_CHECK_FAILED', 'System health check failed', healthStatus);
        }

        const jobProgress = await jobProgressService.getCurrentJobProgress(jobId);

        // Check for timeout
        const timeoutStatus = isJobTimedOut(jobProgress);
        if (timeoutStatus.timedOut) {
            await handleJobTimeout(jobId, jobProgress, timeoutStatus);
            return;
        }

        // Process job progress with circuit breaker
        const stats = await circuitBreaker.execute(async () => {
            return await processJobProgress(jobId, jobProgress);
        });

        // Record job progress metrics
        await recordJobMetrics(jobId, stats);

        const memoryCheckResult = await checkMemoryUsage(jobId, stats.lastEvaluatedKey);

        // If memory usage is high, schedule immediate continuation
        if (memoryCheckResult && stats.lastEvaluatedKey) {
            await scheduleImmediateContinuation(jobId, stats.lastEvaluatedKey);
            return;
        }

        const newStatus = determineJobStatus(stats, jobProgress);

        if (['IN_PROGRESS', 'WAITING_FOR_REVIEW'].includes(newStatus)) {
            await scheduleNextCheck(jobId);
        } else if (['COMPLETED', 'FAILED'].includes(newStatus)) {
            await handleJobCompletion(jobId, newStatus);
        }

    } catch (error) {
        await handleProcessingError(jobId, error);
        throw error;
    }
}

async function handleReviewCompletion(jobId) {
    console.log(`[handleReviewCompletion] Processing review completion for job: ${jobId}`);

    try {
        // 1. Verify all tasks are properly reviewed
        const pendingReviews = await verifyAllTasksReviewed(jobId);

        if (pendingReviews > 0) {
            throw new AppError(
                'ReviewIncomplete',
                `Job still has ${pendingReviews} tasks pending review`
            );
        }

        // 2. Get final job statistics
        const stats = await jobProgressService.getJobStatistics(jobId);

        // 3. Update job progress with final stats and status
        const finalStatus = 'COMPLETED';
        await jobProgressService.updateJobProgress(jobId, {
            processedImages: stats.totalProcessed,
            eligibleImages: stats.eligible,
            excludedImages: stats.excluded,
            duplicateImages: stats.duplicates,
            waitingForReview: 0,
            status: finalStatus,
            completedAt: new Date().toISOString()
        });

        // 4. Notify completion
        await notificationService.publishJobStatus(jobId, finalStatus);

        // 5. Clean up any remaining event rules
        await eventBridgeService.disableAndDeleteRule(jobId);

        console.log(`[handleReviewCompletion] Successfully completed review for job: ${jobId}`);

    } catch (error) {
        console.error(`[handleReviewCompletion] Error processing review completion:`, error);
        throw error;
    }
}

async function verifyAllTasksReviewed(jobId) {
    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'TaskStatus = :status',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':status': 'WAITING_FOR_REVIEW'
        }
    };

    const result = await jobProgressService.dynamoDB.send(new QueryCommand(params));
    return result.Items?.length || 0;
}

async function handleJobCompletion(jobId, status) {
    try {
        // Update completion timestamp
        await jobProgressService.updateJobProgress(jobId, {
            completedAt: new Date().toISOString()
        });

        // Notify completion
        await notificationService.publishJobStatus(jobId, status);

        // Clean up event rules
        await eventBridgeService.disableAndDeleteRule(jobId);

    } catch (error) {
        console.error('Error handling job completion:', error);
        throw error;
    }
}

async function processJobProgress(jobId, jobProgress) {
    try {
        // Get task statistics using paginated queries
        const stats = await getJobStatisticsWithPagination(jobId, jobProgress);

        // Update job progress with latest stats
        await jobProgressService.updateJobProgress(jobId, {
            processedImages: stats.totalProcessed,
            eligibleImages: stats.eligible,
            excludedImages: stats.excluded,
            duplicateImages: stats.duplicates,
            waitingForReview: stats.waitingForReview,
            lastProcessedKey: stats.lastEvaluatedKey, // Save pagination state
            status: determineJobStatus(stats, jobProgress)
        });

        return stats;
    } catch (error) {
        console.error('Error processing job progress:', error);
        throw new AppError(
            ErrorCodes.DATABASE.DYNAMO_UPDATE_ERROR,
            'Failed to process job progress'
        );
    }
}

async function getJobStatisticsWithPagination(jobId, jobProgress) {
    const stats = {
        totalProcessed: 0,
        eligible: 0,
        excluded: 0,
        duplicates: 0,
        waitingForReview: 0,
        lastEvaluatedKey: null
    };

    // Start from last processed key if exists
    let lastEvaluatedKey = jobProgress.lastProcessedKey;
    let pageCount = 0;

    try {
        for await (const page of fetchTasksGenerator(jobId, lastEvaluatedKey)) {
            // Update statistics from this page
            updateStatsFromPage(stats, page.items);

            // Save the last evaluated key
            lastEvaluatedKey = page.lastEvaluatedKey;
            pageCount++;

            // Check if we've hit the page limit
            if (pageCount >= PAGINATION_CONFIG.maxPages) {
                console.log(`Reached maximum page limit (${PAGINATION_CONFIG.maxPages}) for job ${jobId}`);
                break;
            }
        }

        stats.lastEvaluatedKey = lastEvaluatedKey;
        return stats;
    } catch (error) {
        console.error('Error fetching job statistics:', error);
        throw error;
    }
}

async function* fetchTasksGenerator(jobId, startKey = null) {
    let lastEvaluatedKey = startKey;

    do {
        const result = await fetchTasksBatch(jobId, lastEvaluatedKey);
        lastEvaluatedKey = result.LastEvaluatedKey;

        yield {
            items: result.Items || [],
            lastEvaluatedKey
        };
    } while (lastEvaluatedKey);
}

async function fetchTasksBatch(jobId, exclusiveStartKey = null) {
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
}

function updateStatsFromPage(stats, items) {
    for (const item of items) {
        stats.totalProcessed++;

        switch (item.Evaluation) {
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

        if (item.isDuplicate) {
            stats.duplicates++;
        }
    }
}

// Update determineJobStatus to handle partial progress
function determineJobStatus(stats, jobProgress) {
    // If we have a lastEvaluatedKey, we're not done processing all items
    if (stats.lastEvaluatedKey) {
        return 'IN_PROGRESS';
    }

    // If all images are processed
    if (stats.totalProcessed === jobProgress.totalImages) {
        // If there are tasks waiting for review
        if (stats.waitingForReview > 0) {
            return 'WAITING_FOR_REVIEW';
        }
        return 'COMPLETED';
    }

    return 'IN_PROGRESS';
}

// Add memory monitoring
const MEMORY_THRESHOLD = 0.8; // 80% of available memory
async function checkMemoryUsage(jobId, lastEvaluatedKey) {
    const used = process.memoryUsage().heapUsed;
    const total = process.memoryUsage().heapTotal;
    const percentage = used / total;

    if (percentage > MEMORY_THRESHOLD) {
        // Use conditional update to prevent race conditions
        const params = {
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: jobId },
            UpdateExpression: 'SET lastProcessedKey = :key',
            ConditionExpression: 'attribute_not_exists(lastProcessedKey) OR lastProcessedKey < :key',
            ExpressionAttributeValues: {
                ':key': lastEvaluatedKey
            }
        };

        try {
            await jobProgressService.dynamoDB.send(new UpdateCommand(params));
            return true;
        } catch (error) {
            if (error.name === 'ConditionalCheckFailedException') {
                return false;
            }
            throw error;
        }
    }
    return false;
}

async function handleJobTimeout(jobId, jobProgress, timeoutInfo) {
    console.log(`Job ${jobId} has timed out:`, timeoutInfo);

    try {
        // 1. Update job status with detailed timeout information
        await jobProgressService.updateJobProgress(jobId, {
            status: 'FAILED',
            error: {
                code: 'TIMEOUT',
                reason: timeoutInfo.reason,
                details: timeoutInfo.details,
                timeoutAt: new Date().toISOString()
            },
            completedAt: new Date().toISOString()
        });

        // 2. Collect and store final statistics
        const finalStats = await jobProgressService.getJobStatistics(jobId);
        await jobProgressService.updateJobProgress(jobId, {
            finalStatistics: {
                ...finalStats,
                timeoutReason: timeoutInfo.reason,
                incompleteTaskCount: jobProgress.totalImages - finalStats.totalProcessed
            }
        });

        // 3. Clean up resources
        await cleanupJobResources(jobId);

        // 4. Notify about timeout
        await notificationService.publishJobStatus(jobId, 'FAILED', {
            error: 'TIMEOUT',
            reason: timeoutInfo.reason,
            details: timeoutInfo.details
        });

    } catch (error) {
        console.error('Error handling job timeout:', error);
        throw error;
    }
}

async function cleanupJobResources(jobId) {
    try {
        // 1. Clean up EventBridge rules
        await eventBridgeService.disableAndDeleteRule(jobId);

        // 2. Cancel any pending SQS messages
        // Note: We can't actually delete messages, but we can update job state
        // to ensure they're ignored if processed later
        await jobProgressService.updateJobProgress(jobId, {
            cleanupStatus: 'COMPLETED',
            cleanupTimestamp: new Date().toISOString()
        });

        // 3. Log cleanup completion
        console.log(`Cleanup completed for job ${jobId}`);
    } catch (error) {
        console.error(`Error during cleanup for job ${jobId}:`, error);
        // Don't throw here, as cleanup is best-effort
    }
}

function isJobTimedOut(jobProgress) {
    const currentTime = new Date().getTime();
    const startTime = new Date(jobProgress.createdAt).getTime();
    const lastUpdateTime = new Date(jobProgress.updatedAt || jobProgress.createdAt).getTime();

    if (jobProgress.status === 'WAITING_FOR_REVIEW') {
        return handleReviewTimeout(jobProgress, currentTime, lastUpdateTime);
    }

    // Check for overall timeout based on status
    const maxDuration = jobProgress.status === 'WAITING_FOR_REVIEW'
        ? JOB_TIMEOUTS.REVIEW
        : JOB_TIMEOUTS.PROCESSING;

    if (currentTime - startTime > maxDuration) {
        return {
            timedOut: true,
            reason: 'MAX_DURATION_EXCEEDED',
            details: `Job exceeded maximum ${jobProgress.status === 'WAITING_FOR_REVIEW' ? 'review' : 'processing'} time`
        };
    }

    // Check for inactivity
    if (currentTime - lastUpdateTime > JOB_TIMEOUTS.INACTIVITY) {
        return {
            timedOut: true,
            reason: 'INACTIVITY',
            details: 'No activity detected for 30 minutes'
        };
    }

    return { timedOut: false };
}

async function handleReviewTimeout(jobProgress, currentTime, lastUpdateTime) {
    const reviewStartTime = new Date(jobProgress.reviewStartedAt || jobProgress.updatedAt).getTime();
    const timeInReview = currentTime - reviewStartTime;

    // Check if we've sent a reminder
    const shouldSendReminder = !jobProgress.reviewReminders?.lastSent ||
        (currentTime - new Date(jobProgress.reviewReminders.lastSent).getTime() > 24 * 60 * 60 * 1000); // 24 hours since last reminder

    // If within initial review period but needs reminder
    if (timeInReview < JOB_TIMEOUTS.REVIEW && shouldSendReminder) {
        return {
            timedOut: false,
            action: 'SEND_REMINDER',
            details: 'Review period active but requires reminder'
        };
    }

    // If exceeded initial review period but not yet extended
    if (timeInReview >= JOB_TIMEOUTS.REVIEW && !jobProgress.reviewExtended) {
        return {
            timedOut: false,
            action: 'EXTEND_REVIEW',
            details: 'Initial review period expired, eligible for extension'
        };
    }

    // If in extension period
    if (jobProgress.reviewExtended) {
        const extensionStartTime = new Date(jobProgress.reviewExtendedAt).getTime();
        if (currentTime - extensionStartTime >= JOB_TIMEOUTS.REVIEW_EXTENSION) {
            return {
                timedOut: true,
                reason: 'REVIEW_EXTENSION_EXPIRED',
                details: 'Review extension period has expired'
            };
        }
    }

    return { timedOut: false };
}

async function handleReviewTimeoutActions(jobId, jobProgress, timeoutInfo) {
    switch (timeoutInfo.action) {
        case 'SEND_REMINDER':
            await sendReviewReminder(jobId, jobProgress);
            break;
        case 'EXTEND_REVIEW':
            await extendReviewPeriod(jobId, jobProgress);
            break;
        default:
            if (timeoutInfo.timedOut) {
                await handleFinalReviewTimeout(jobId, jobProgress, timeoutInfo);
            }
    }
}

async function sendReviewReminder(jobId, jobProgress) {
    const reminderCount = (jobProgress.reviewReminders?.count || 0) + 1;

    // Update reminder tracking
    await jobProgressService.updateJobProgress(jobId, {
        reviewReminders: {
            count: reminderCount,
            lastSent: new Date().toISOString()
        }
    });

    // Send reminder notification
    await notificationService.publishJobStatus(jobId, 'REVIEW_REMINDER', {
        reminderCount,
        remainingTime: formatRemainingTime(jobProgress),
        reviewUrl: `${process.env.APP_URL}/jobs/${jobId}/review`
    });
}

async function extendReviewPeriod(jobId, jobProgress) {
    // Update job progress with extension
    await jobProgressService.updateJobProgress(jobId, {
        reviewExtended: true,
        reviewExtendedAt: new Date().toISOString(),
        reviewExtensionExpiresAt: new Date(Date.now() + JOB_TIMEOUTS.REVIEW_EXTENSION).toISOString()
    });

    // Notify user about extension
    await notificationService.publishJobStatus(jobId, 'REVIEW_EXTENDED', {
        extensionPeriod: '24 hours',
        reviewUrl: `${process.env.APP_URL}/jobs/${jobId}/review`
    });
}

async function handleFinalReviewTimeout(jobId, jobProgress, timeoutInfo) {
    try {
        // Get final statistics
        const stats = await jobProgressService.getJobStatistics(jobId);

        // Auto-approve all remaining items in WAITING_FOR_REVIEW
        await autoApproveRemainingItems(jobId);

        // Update job status
        await jobProgressService.updateJobProgress(jobId, {
            status: 'COMPLETED',
            completedAt: new Date().toISOString(),
            reviewTimeoutDetails: {
                timeoutReason: timeoutInfo.reason,
                autoApprovedCount: stats.waitingForReview,
                finalStats: stats
            }
        });

        // Send final notification
        await notificationService.publishJobStatus(jobId, 'REVIEW_TIMEOUT_COMPLETION', {
            autoApprovedCount: stats.waitingForReview,
            totalProcessed: stats.totalProcessed,
            completionTime: new Date().toISOString()
        });

    } catch (error) {
        console.error('Error handling final review timeout:', error);
        throw error;
    }
}

async function autoApproveRemainingItems(jobId) {
    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'TaskStatus = :status',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':status': 'WAITING_FOR_REVIEW'
        }
    };

    const result = await jobProgressService.dynamoDB.send(new QueryCommand(params));
    const tasks = result.Items || [];

    // Auto-approve each task
    for (const task of tasks) {
        await jobProgressService.updateTaskStatus(jobId, task.TaskID, {
            status: 'COMPLETED',
            evaluation: 'ELIGIBLE', // Default to accepting the image
            autoApproved: true,
            autoApprovedAt: new Date().toISOString()
        });
    }
}

function formatRemainingTime(jobProgress) {
    const currentTime = new Date().getTime();
    const reviewStartTime = new Date(jobProgress.reviewStartedAt || jobProgress.updatedAt).getTime();
    const timeInReview = currentTime - reviewStartTime;

    if (jobProgress.reviewExtended) {
        const extensionEndTime = new Date(jobProgress.reviewExtensionExpiresAt).getTime();
        return formatDuration(extensionEndTime - currentTime);
    }

    const remainingTime = JOB_TIMEOUTS.REVIEW - timeInReview;
    return formatDuration(remainingTime);
}

function formatDuration(ms) {
    const hours = Math.floor(ms / (1000 * 60 * 60));
    const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60));
    return `${hours} hours and ${minutes} minutes`;
}

async function scheduleImmediateContinuation(jobId, lastEvaluatedKey) {
    await sqs.send(new SendMessageCommand({
        QueueUrl: process.env.JOB_PROGRESS_QUEUE_URL,
        MessageBody: JSON.stringify({
            jobId,
            action: 'PROGRESS_CHECK',
            lastEvaluatedKey,
            timestamp: new Date().toISOString()
        }),
        MessageAttributes: {
            eventType: {
                DataType: 'String',
                StringValue: 'PROGRESS_CHECK'
            }
        },
        DelaySeconds: 0 // Immediate processing
    }));
}

async function recordJobMetrics(jobId, stats) {
    const metrics = {
        totalProcessed: stats.totalProcessed,
        processingRate: stats.totalProcessed / (Date.now() - new Date(stats.startTime).getTime()) * 1000,
        errorRate: stats.errors / stats.totalProcessed || 0,
        memoryUsage: process.memoryUsage().heapUsed / process.memoryUsage().heapTotal
    };

    await recordMetrics('JobProgress', {
        ...metrics,
        jobId,
        success: true
    });
}

// Update processBatchWithRetries to use the circuit breaker
async function processTaskBatch(jobId, jobProgress) {
    const batches = await fetchTasksGenerator(jobId, jobProgress.lastProcessedKey);
    const stats = {
        totalProcessed: 0,
        errors: 0,
        startTime: jobProgress.startedAt || new Date().toISOString()
    };

    for await (const batch of batches) {
        try {
            const batchStats = await processBatchWithRetries(jobId, batch.items);
            stats.totalProcessed += batchStats.totalProcessed;
        } catch (error) {
            stats.errors++;
            if (stats.errors >= PERFORMANCE_METRICS.MAX_RETRIES_PER_BATCH) {
                throw new JobProcessingError('MAX_BATCH_RETRIES_EXCEEDED',
                    'Exceeded maximum retry attempts for batch processing');
            }
        }
    }

    return stats;
}

// Update handleProcessingError to use the error tracking
async function handleProcessingError(jobId, error) {
    try {
        // Record error metrics
        await recordMetrics('JobProcessingError', {
            duration: 0,
            success: false,
            errorCode: error.code
        });

        // Update job progress with error information
        await jobProgressService.updateJobProgress(jobId, {
            lastError: {
                code: error.code,
                message: error.message,
                timestamp: new Date().toISOString(),
                details: error.details
            },
            errorCount: jobProgress.errorCount ? jobProgress.errorCount + 1 : 1
        });

        // If error is terminal, update job status
        if (TERMINAL_ERRORS.includes(error.code)) {
            await jobProgressService.updateJobProgress(jobId, {
                status: 'FAILED',
                completedAt: new Date().toISOString()
            });
        }
    } catch (updateError) {
        console.error('Error handling processing error:', updateError);
        // Don't throw here to avoid infinite error handling loop
    }
}

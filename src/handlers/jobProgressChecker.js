const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { SNSClient } = require('@aws-sdk/client-sns');
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { EventBridgeClient } = require("@aws-sdk/client-eventbridge");
const { CloudWatchClient } = require('@aws-sdk/client-cloudwatch');

// Import services
const { createCircuitBreaker } = require('../services/CircuitBreakerService');
const { createJobHealthService } = require('../services/JobHealthService');
const { createJobTimeoutService } = require('../services/JobTimeoutService');
const { createJobStatisticsService } = require('../services/JobStatisticsService');
const { createJobCompletionService } = require('../services/JobCompletionService');
const { createReviewCompletionService } = require('../services/ReviewCompletionService');
const { createErrorHandlingService } = require('../services/ErrorHandlingService');
const JobProgressService = require('../services/jobProgressService');
const NotificationService = require('../services/notificationService');
const EventBridgeService = require('../services/eventBridgeService');
const CloudWatchService = require("../services/cloudwatchService");
const { createJobSchedulingService } = require('../services/JobSchedulingService');
const { JobProcessingError } = require("../utils/errors");
const supabaseService = require("../services/supabaseService");

// Initialize AWS clients
const dynamoDBClient = new DynamoDBClient();
const documentClient = DynamoDBDocumentClient.from(dynamoDBClient, {
    marshallOptions: { removeUndefinedValues: true }
});
const cloudWatch = new CloudWatchClient();
const sqs = new SQSClient();
const sns = new SNSClient();
const eventBridge = new EventBridgeClient();

// Initialize base configuration
const config = {
    tasksTable: process.env.TASKS_TABLE,
    jobProgressTable: process.env.JOB_PROGRESS_TABLE
};

const eventSchedulingConfig = {
    region: process.env.AWS_REGION,
    accountId: process.env.AWS_ACCOUNT_ID,
    lambdaArn: process.env.JOB_PROGRESS_CHECKER_LAMBDA_ARN,
    jobProgressQueueUrl: process.env.JOB_PROGRESS_QUEUE_URL
}

// Initialize base services
const cloudWatchService = new CloudWatchService(cloudWatch, config);
const notificationService = new NotificationService(sns, process.env.JOB_COMPLETION_TOPIC_ARN);
const eventBridgeService = new EventBridgeService(eventBridge, eventSchedulingConfig,);

const jobStatisticsService = createJobStatisticsService(documentClient, cloudWatchService);
const jobProgressService = new JobProgressService(documentClient, jobStatisticsService, config, notificationService);
const jobHealthService = createJobHealthService(jobProgressService);
const jobSchedulingService = createJobSchedulingService(
    eventBridgeService,
    sqs,
    eventSchedulingConfig,
    jobProgressService
);
const jobTimeoutService = createJobTimeoutService(jobProgressService, notificationService, jobSchedulingService);
const jobCompletionService = createJobCompletionService(
    jobProgressService,
    notificationService,
    eventBridgeService,
    cloudWatchService,
    jobSchedulingService,
    supabaseService,
    jobStatisticsService
);
const reviewCompletionService = createReviewCompletionService(
    jobProgressService,
    jobCompletionService
);


// Error handling service
const errorHandlingService = createErrorHandlingService(
    jobProgressService,
    cloudWatchService,
    jobSchedulingService,
    jobCompletionService
);

// Main handler
exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    const batchItemFailures = [];

    for (const record of event.Records) {
        const messageId = record.messageId;
        console.log('Processing message with ID:', messageId);

        try {
            const body = JSON.parse(record.body);
            const jobId = body.jobId;
            const eventType = record.messageAttributes?.eventType?.stringValue;

            console.log('Parsed jobId:', jobId);
            console.log('Parsed eventType:', eventType);

            if (!jobId) {
                console.error('Invalid message format - missing jobId:', body);
                continue;
            }

            await handleJobMessage(jobId, eventType, record.messageAttributes);

            console.log('[Main handler] Finished processing message for jobId:', jobId);
        } catch (error) {
            console.error('Error processing record:', error);
            batchItemFailures.push({ itemIdentifier: messageId });
        }
    }

    return { batchItemFailures };
};

// Main message handler
const handleJobMessage = async (jobId, eventType, messageAttributes) => {
    const isUserTriggered = messageAttributes?.priority?.stringValue === 'high';
    console.log('[handleJobMessage] Processing message:', { jobId, eventType, isUserTriggered });

    try {
        const jobProgress = await jobProgressService.getCurrentJobProgress(jobId);

        // Early exit for terminal states
        if (!jobProgress ||
            ['COMPLETED', 'FAILED', 'STALE'].includes(jobProgress?.status) ||
            jobProgress?.schedulingStatus === 'CLEANUP_REQUESTED') {
            console.log('[handleJobMessage] Skipping processing for completed/cleaned up job:', jobId);
            await jobSchedulingService.cleanupScheduledChecks(jobId);
            return;
        }

        // Skip if in review state and not a review-related event
        const isReviewRelatedEvent = ['REVIEW_COMPLETED', 'TIMEOUT_CHECK'].includes(eventType);
        const isWaitingForReview = jobProgress?.status === 'WAITING_FOR_REVIEW';

        if (isWaitingForReview && !isReviewRelatedEvent) {
            console.log('[handleJobMessage] Skipping non-review event for job in WAITING_FOR_REVIEW:', {
                jobId,
                eventType
            });
            return;
        }

        // Handle different event types
        switch (eventType) {
            case 'REVIEW_COMPLETED':
                console.log('[handleJobMessage] Handling review completion for jobId:', jobId);
                await reviewCompletionService.handleReviewCompletion(jobId);
                break;
            case 'TIMEOUT_CHECK':
                console.log('[handleJobMessage] Handling timeout check for jobId:', jobId);
                await handleTimeoutCheck(jobId, jobProgress);
                break;
            case 'PROGRESS_CHECK':
                console.log('[handleJobMessage] Handling progress check for jobId:', jobId);
                await handleRegularProgressCheck(jobId, jobProgress);
                break;
            default:
                console.warn('[handleJobMessage] Unknown event type:', eventType);
                break;
        }

        console.log('[handleJobMessage] Finished processing message for jobId:', jobId);
    } catch (error) {
        console.error('[handleJobMessage] Error processing message:', error);
        await errorHandlingService.handleProcessingError(jobId, error);
        throw error;
    }
};

const handleTimeoutCheck = async (jobId, jobProgress) => {
    console.log(`Checking timeout for job: ${jobId}`);

    const timeoutStatus = jobTimeoutService.isJobTimedOut(jobProgress);

    if (timeoutStatus.timedOut) {
        console.log('Job timed out, handling timeout');
        await Promise.all([
            jobTimeoutService.handleJobTimeout(jobId, jobProgress, timeoutStatus),
            timeoutStatus.reason !== 'REVIEW_INACTIVITY' && timeoutStatus.canExtend === false ?
                jobSchedulingService.cleanupScheduledChecks(jobId) :
                Promise.resolve()
        ]);
        return true;
    }
    console.log('Job not timed out for jobId:', jobId);
    return false;
};

// Regular progress check handler
const handleRegularProgressCheck = async (jobId, jobProgress) => {
    const circuitBreaker = createCircuitBreaker(jobId, jobProgressService);
    console.log('Circuit breaker created for jobId:', jobId);

    try {
        console.log('Performing health check for jobId:', jobId);
        // Health check
        const healthStatus = await jobHealthService.performHealthCheck(jobId);
        if (Object.values(healthStatus.checks).some(check => check.status === 'ERROR')) {
            throw new JobProcessingError('HEALTH_CHECK_FAILED', 'System health check failed', healthStatus);
        }
        console.log('Health check completed for jobId:', jobId);

        // const jobProgress = await jobProgressService.getCurrentJobProgress(jobId);

        // Check timeout first
        if (await handleTimeoutCheck(jobId, jobProgress)) {
            console.log('Job timed out, skipping progress check');
            return;
        }
        console.log('Job not timed out for jobId:', jobId);

        // Process job progress with circuit breaker
        console.log('Getting job statistics for jobId:', jobId);
        const stats = await circuitBreaker.execute(async () => {
            return await jobStatisticsService.getJobStatisticsWithPagination(jobId, jobProgress);
        });
        console.log('Job statistics retrieved for jobId:', jobId);
        console.log('Updated stats:', stats);

        // Record metrics
        console.log('Recording job metrics for jobId:', jobId);
        await jobStatisticsService.recordJobMetrics(jobId, stats);

        // Check memory usage and handle continuation if needed
        if (stats.lastEvaluatedKey) {
            console.log('Checking memory usage for jobId:', jobId);
            const memoryCheckResult = await jobHealthService.checkMemoryUsage(jobId, stats.lastEvaluatedKey);
            console.log('Memory check result:', memoryCheckResult);

            if (memoryCheckResult) {
                const continuationCount = jobProgress.continuationCount || 0;
                const MAX_CONTINUATIONS = 10;

                if (continuationCount >= MAX_CONTINUATIONS) {
                    console.warn(`Maximum continuations reached for job ${jobId}`);
                    await jobProgressService.updateJobProgress(jobId, {
                        status: 'FAILED',
                        error: {
                            code: 'MAX_CONTINUATIONS_EXCEEDED',
                            message: 'Job exceeded maximum number of consecutive continuations'
                        }
                    });
                    return;
                }

                await jobProgressService.updateJobProgress(jobId, {
                    continuationCount: continuationCount + 1
                });

                console.log('Scheduling immediate continuation for jobId:', jobId);
                await scheduleImmediateContinuation(jobId, stats.lastEvaluatedKey);
                return;
            }
        }
        console.log('Memory check completed for jobId:', jobId);

        // Determine job status
        const newStatus = jobCompletionService.determineJobStatus(stats, jobProgress);
        console.log('Determined job status:', newStatus);

        // Break if the job is corrupted 
        const isCorrupted = determineIfJobIsCorrupted(stats, jobProgress);
        if (isCorrupted) {
            console.log('Job is corrupted, breaking');
            await jobCompletionService.handleJobCompletion(jobId, 'FAILED');
            return;
        }

        // Update job progress with latest statistics to allow frontend to display correct stats
        const currentVersion = jobProgress.version || 0;
        const newVersion = currentVersion + 1;

        // TODO: refactor this to use updateJobProgress
        // const completedAt = Date.now().toString();
        // const updatedAt = Date.now().toString();
        // const statistics = stats;

        const updateParams = {
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: jobId },
            UpdateExpression: 'SET #status = :status, completedAt = :completedAt, statistics = :stats, updatedAt = :updatedAt, version = :newVersion',
            ConditionExpression: 'attribute_not_exists(version) OR version = :currentVersion',
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: {
                ':status': newStatus,
                ':completedAt': Date.now().toString(),
                ':stats': {
                    totalProcessed: stats.totalProcessed,
                    eligible: stats.eligible,
                    excluded: stats.excluded,
                    duplicates: stats.duplicates,
                    failed: stats.failed,
                    waitingForReview: stats.waitingForReview,
                    failedLogs: stats.failedLogs,
                    lastEvaluatedKey: stats.lastEvaluatedKey
                },
                ':updatedAt': Date.now().toString(),
                ':currentVersion': currentVersion,
                ':newVersion': newVersion
            }
        };

        await jobProgressService.dynamoDB.send(new UpdateCommand(updateParams));

        // Handle next steps based on status
        if (['IN_PROGRESS', 'WAITING_FOR_REVIEW'].includes(newStatus)) {
            console.log('Scheduling next check for jobId:', jobId);
            await jobSchedulingService.scheduleNextCheck(jobId, newStatus, undefined, jobProgressService);
        } else if (['COMPLETED', 'FAILED'].includes(newStatus)) {
            console.log('Handling job completion for jobId:', jobId);
            await jobCompletionService.handleJobCompletion(jobId, newStatus);
        }

        // If we complete a full check without needing continuation, reset the counter
        if (!stats.lastEvaluatedKey && jobProgress.continuationCount) {
            await jobProgressService.updateJobProgress(jobId, {
                continuationCount: 0
            });
        }
    } catch (error) {
        console.error('[handleRegularProgressCheck] Error during progress check:', error);
        throw error;
    }
};

const determineIfJobIsCorrupted = (stats, jobProgress) => {
    if (!stats || !jobProgress) {
        console.error("[determineIfJobIsCorrupted]: Missing or invalid inputs", { stats, jobProgress });
        return true; // Treat as corrupted if inputs are invalid
    }

    const { createdAt, updatedAt, totalImages } = jobProgress;
    const { totalProcessed } = stats;

    if (!createdAt || !updatedAt || !totalImages || totalProcessed == null) {
        console.error("[determineIfJobIsCorrupted]: Missing required job progress or stats properties", {
            createdAt, updatedAt, totalImages, totalProcessed
        });
        return true;
    }

    const currentTime = Date.now();
    const timeSinceLastUpdate = currentTime - updatedAt;
    const hasBeenTenMinutes = timeSinceLastUpdate > 1000 * 60 * 10; // 10 minutes
    const statsAreEmpty = totalProcessed === 0;

    // Avoid division by zero and check processing progress
    const processingMightBeStuck = totalImages > 0 && totalProcessed / totalImages < 0.2;

    console.log(`[determineIfJobIsCorrupted]:`, {
        createdAt,
        updatedAt,
        currentTime,
        timeSinceLastUpdate,
        hasBeenTenMinutes,
        statsAreEmpty,
        totalImages,
        totalProcessed,
        processingMightBeStuck
    });

    if (hasBeenTenMinutes || statsAreEmpty || processingMightBeStuck) {
        console.log("Job is corrupted, returning true");
        return true;
    }

    console.log("Job is not corrupted, returning false");
    return false;
};


// Helper function for scheduling immediate continuation
const scheduleImmediateContinuation = async (jobId, lastEvaluatedKey) => {
    console.log('Scheduling immediate continuation for jobId:', jobId);
    const MIN_DELAY_SECONDS = 5; // Add minimum 5-second delay

    await sqs.send(new SendMessageCommand({
        QueueUrl: process.env.JOB_PROGRESS_QUEUE_URL,
        MessageBody: JSON.stringify({
            jobId,
            action: 'PROGRESS_CHECK',
            lastEvaluatedKey,
            timestamp: Date.now().toString()
        }),
        MessageAttributes: {
            eventType: {
                DataType: 'String',
                StringValue: 'PROGRESS_CHECK'
            }
        },
        DelaySeconds: MIN_DELAY_SECONDS
    }));
};
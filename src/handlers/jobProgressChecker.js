const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { SNSClient } = require('@aws-sdk/client-sns');
const { SQSClient } = require("@aws-sdk/client-sqs");
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
const jobProgressService = new JobProgressService(documentClient, config);
const cloudWatchService = new CloudWatchService(cloudWatch, config);
const notificationService = new NotificationService(sns, process.env.JOB_COMPLETION_TOPIC_ARN);
const eventBridgeService = new EventBridgeService(eventBridge, eventSchedulingConfig,);

// Initialize functional services
const jobHealthService = createJobHealthService(jobProgressService);
const jobTimeoutService = createJobTimeoutService(jobProgressService, notificationService);
const jobStatisticsService = createJobStatisticsService(jobProgressService, cloudWatchService);
const jobSchedulingService = createJobSchedulingService(
    eventBridgeService,
    sqs,
    eventSchedulingConfig,
    jobProgressService
);
const jobCompletionService = createJobCompletionService(
    jobProgressService,
    notificationService,
    eventBridgeService,
    cloudWatchService,
    jobSchedulingService
);
const reviewCompletionService = createReviewCompletionService(
    jobProgressService,
    jobCompletionService
);


// Error handling service
const errorHandlingService = createErrorHandlingService(
    jobProgressService,
    cloudWatchService,
    jobSchedulingService
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
            return;
        }

        // Handle different event types
        switch (eventType) {
            case 'REVIEW_COMPLETED':
                await reviewCompletionService.handleReviewCompletion(jobId);
                break;
            case 'TIMEOUT_CHECK':
                await handleTimeoutCheck(jobId, jobProgress);
                break;
            case 'PROGRESS_CHECK':
                await handleRegularProgressCheck(jobId, jobProgress);
                break;
            default:
                console.warn('[handleJobMessage] Unknown event type:', eventType);
                break;
        }
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
            jobSchedulingService.cleanupScheduledChecks(jobId)
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
                console.log('Scheduling immediate continuation for jobId:', jobId);
                await scheduleImmediateContinuation(jobId, stats.lastEvaluatedKey);
                return;
            }
        }
        console.log('Memory check completed for jobId:', jobId);

        // Determine job status
        const newStatus = jobCompletionService.determineJobStatus(stats, jobProgress);
        console.log('Determined job status:', newStatus);

        // Update job progress with latest statistics to allow frontend to display correct stats
        const currentVersion = jobProgress.version || 0;
        const newVersion = currentVersion + 1;

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
    } catch (error) {
        console.error('[handleRegularProgressCheck] Error during progress check:', error);
        throw error;
    }
};

// Helper function for scheduling immediate continuation
const scheduleImmediateContinuation = async (jobId, lastEvaluatedKey) => {
    console.log('Scheduling immediate continuation for jobId:', jobId);
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
        DelaySeconds: 0
    }));
};
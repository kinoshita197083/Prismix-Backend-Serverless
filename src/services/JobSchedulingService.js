const { PutRuleCommand, PutTargetsCommand } = require('@aws-sdk/client-eventbridge');
const { SendMessageCommand } = require('@aws-sdk/client-sqs');
const { WAITING_FOR_REVIEW, COMPLETED, FAILED } = require('../utils/config');

const createJobSchedulingService = (eventBridgeService, sqs, config, jobProgressService) => {
    // Constants for scheduling logic
    const SCHEDULE_CONFIG = {
        INITIAL_INTERVAL: 60, // 1 minute in seconds
        MAX_INTERVAL: 300,    // 5 minutes in seconds
        REVIEW_INTERVAL: 900, // 15 minutes in seconds
        REVIEW_TIMEOUT_CHECK_INTERVAL: 30,    // 30 minutes
        PROCESSING_TIMEOUT_CHECK_INTERVAL: 15, // 15 minutes
        MAX_ATTEMPTS: 3,
        BACKOFF_MULTIPLIER: 2
    };

    const determineNextCheckInterval = async (jobId, currentStatus, attemptCount = 0) => {
        try {
            // Get job progress to determine appropriate interval
            const progress = await jobProgressService.getCurrentJobProgress(jobId);

            // If waiting for review, use longer interval
            if (currentStatus === 'WAITING_FOR_REVIEW') {
                return SCHEDULE_CONFIG.REVIEW_INTERVAL;
            }

            // Calculate processing rate and adjust interval accordingly
            const processingRate = calculateProcessingRate(progress);

            // Use exponential backoff if processing is slow or has failures
            if (attemptCount > 0) {
                const backoffInterval = SCHEDULE_CONFIG.INITIAL_INTERVAL *
                    Math.pow(SCHEDULE_CONFIG.BACKOFF_MULTIPLIER, attemptCount);
                return Math.min(backoffInterval, SCHEDULE_CONFIG.MAX_INTERVAL);
            }

            // Default interval based on processing rate
            return determineIntervalFromRate(processingRate);
        } catch (error) {
            console.error('Error determining next check interval:', error);
            return SCHEDULE_CONFIG.INITIAL_INTERVAL;
        }
    };

    const createEventBridgeRule = async (ruleName, interval, jobId, action, status) => {
        try {
            await eventBridge.send(new PutRuleCommand({
                Name: ruleName,
                ScheduleExpression: `rate(${interval})`,
                State: 'ENABLED',
                Description: `${action} schedule for job ${jobId}`
            }));

            await eventBridge.send(new PutTargetsCommand({
                Rule: ruleName,
                Targets: [{
                    Id: `${action}Target-${jobId}`,
                    Arn: config.lambdaArn,
                    Input: JSON.stringify({
                        jobId,
                        action,
                        status,
                        timestamp: new Date().toISOString()
                    })
                }]
            }));

            console.log(`Scheduled ${action} for job ${jobId}`);
            return true;
        } catch (error) {
            console.error(`Error scheduling ${action}:`, error);
            return false;
        }
    };

    const scheduleTimeoutCheck = async (jobId, status) => {
        const timeoutConfig = status === 'WAITING_FOR_REVIEW'
            ? SCHEDULE_CONFIG.REVIEW_TIMEOUT_CHECK_INTERVAL
            : SCHEDULE_CONFIG.PROCESSING_TIMEOUT_CHECK_INTERVAL;

        return createEventBridgeRule(
            `JobTimeout-${jobId}`,
            `${timeoutConfig} minutes`,
            jobId,
            'TIMEOUT_CHECK',
            status
        );
    };

    const scheduleWithEventBridge = async (jobId, delaySeconds, currentStatus) => {
        return createEventBridgeRule(
            `JobProgressCheck-${jobId}`,
            `${delaySeconds} seconds`,
            jobId,
            'PROGRESS_CHECK',
            currentStatus
        );
    };

    const scheduleWithSQS = async (jobId, delaySeconds, currentStatus, options = {}) => {
        const { eventType = 'PROGRESS_CHECK', priority = 'normal' } = options;

        const messageAttributes = {
            eventType: {
                DataType: 'String',
                StringValue: eventType
            },
            priority: {
                DataType: 'String',
                StringValue: priority
            }
        };

        try {
            await sqs.send(new SendMessageCommand({
                QueueUrl: config.jobProgressQueueUrl,
                MessageBody: JSON.stringify({
                    jobId,
                    action: eventType,
                    status: currentStatus,
                    timestamp: new Date().toISOString(),
                    isUserTriggered: priority === 'high',
                }),
                MessageAttributes: messageAttributes,
                DelaySeconds: Math.min(delaySeconds, 900)
            }));

            console.log(`Scheduled ${eventType} check with SQS for job ${jobId} in ${delaySeconds} seconds`);
            return true;
        } catch (error) {
            console.error('Error scheduling with SQS:', error);
            return false;
        }
    };

    const scheduleNextCheck = async (jobId, currentStatus, options = {}, jobProgressService) => {
        const {
            isUserTriggered = false,
            reviewCompleted = false,
            minimumDelay = 30 // Enforce minimum delay of 30 seconds
        } = options;

        console.log('[JobSchedulingService.scheduleNextCheck] Scheduling next check for job:', jobId);
        console.log('[JobSchedulingService.scheduleNextCheck] Current status:', currentStatus);
        console.log('[JobSchedulingService.scheduleNextCheck] Review completed:', reviewCompleted);
        console.log('[JobSchedulingService.scheduleNextCheck] Is user triggered:', isUserTriggered);

        try {
            // If review is completed by user, no need to schedule next check
            // Prevent scheduling if job is complete or failed
            if ([COMPLETED, FAILED, 'STALE'].includes(currentStatus)) {
                console.log('[JobSchedulingService.scheduleNextCheck] Job is in final state, no scheduling needed');
                // await cleanupScheduledChecks(jobId);
                return;
            }

            // If review is completed, clean up and exit
            if (reviewCompleted) {
                await cleanupScheduledChecks(jobId);
                return;
            }

            // For WAITING_FOR_REVIEW status, only update RDS and exit
            if (currentStatus === WAITING_FOR_REVIEW) {
                console.log('[JobSchedulingService.scheduleNextCheck] Job is waiting for review, scheduling timeout check');
                await jobProgressService.updateJobStatusRDS(jobId, WAITING_FOR_REVIEW);
                await scheduleTimeoutCheck(jobId, currentStatus);
                return;
            }

            // Calculate delay with minimum threshold
            let delaySeconds = await determineNextCheckInterval(jobId, currentStatus);
            delaySeconds = Math.max(delaySeconds, minimumDelay);

            // Handle user-triggered actions with minimum delay
            if (isUserTriggered) {
                console.log('[JobSchedulingService.scheduleNextCheck] User triggered action, scheduling review completed check');
                await scheduleWithSQS(jobId, minimumDelay, currentStatus, {
                    eventType: 'REVIEW_COMPLETED',
                    priority: 'high'
                });
                return;
            }

            // Regular scheduling logic with enforced minimum delay
            if (delaySeconds > 900) {
                console.log('[JobSchedulingService.scheduleNextCheck] Scheduling with EventBridge');
                const scheduled = await scheduleWithEventBridge(jobId, delaySeconds, currentStatus);
                if (scheduled) return;
            }

            console.log('[JobSchedulingService.scheduleNextCheck] Scheduling with SQS');
            await scheduleWithSQS(jobId, delaySeconds, currentStatus);

        } catch (error) {
            console.error('Error in scheduleNextCheck:', error);
            // Use minimum delay for error cases
            await scheduleWithSQS(jobId, minimumDelay, currentStatus);
        }
    };

    // Helper functions
    const calculateProcessingRate = (progress) => {
        const now = Date.now();
        const startTime = new Date(+progress.createdAt).getTime();
        const elapsed = (now - startTime) / 1000; // seconds
        return progress.processedImages / elapsed;
    };

    const determineIntervalFromRate = (rate) => {
        if (rate <= 0) return SCHEDULE_CONFIG.MAX_INTERVAL;
        if (rate < 1) return 300;  // 5 minutes
        if (rate < 10) return 180; // 3 minutes
        if (rate < 50) return 120; // 2 minutes
        return SCHEDULE_CONFIG.INITIAL_INTERVAL;
    };

    const cleanupScheduledChecks = async (jobId) => {
        console.log('[JobSchedulingService.cleanupScheduledChecks] Cleaning up scheduled checks for job:', jobId);

        try {
            // Clean up both progress check and timeout check EventBridge rules
            const ruleNames = [
                `JobProgressCheck-${jobId}`,
                `JobTimeout-${jobId}`
            ];

            // Run rule deletions in parallel
            await Promise.all(
                ruleNames.map(async ruleName => {
                    try {
                        await eventBridgeService.disableAndDeleteRule(jobId, ruleName);
                        console.log('[JobSchedulingService.cleanupScheduledChecks] Deleted EventBridge rule:', ruleName);
                    } catch (error) {
                        // Don't throw if rule doesn't exist
                        if (error.name !== 'ResourceNotFoundException') {
                            console.error('[JobSchedulingService.cleanupScheduledChecks] Error cleaning up EventBridge rule:', error);
                        }
                    }
                })
            );

            console.log('[JobSchedulingService.cleanupScheduledChecks] Successfully cleaned up EventBridge rules');

            // Purge any pending SQS messages for this job
            // Note: We can't selectively delete messages, but we can mark them for non-processing
            console.log('[JobSchedulingService.cleanupScheduledChecks] Purging pending SQS messages for job:', jobId);
            await jobProgressService.updateJobProgress(jobId, {
                schedulingStatus: 'CLEANUP_REQUESTED',
                cleanupTimestamp: new Date().toISOString()
            });

            console.log('[JobSchedulingService.cleanupScheduledChecks] Successfully marked job for cleanup');
            return true;

        } catch (error) {
            console.error('[JobSchedulingService.cleanupScheduledChecks] Error during cleanup:', error);
            throw error;
        }
    };

    return {
        scheduleNextCheck,
        cleanupScheduledChecks,
        determineNextCheckInterval,
        scheduleTimeoutCheck
    };
};

module.exports = { createJobSchedulingService }; 
const { COMPLETED, FAILED } = require("../utils/config");
const dynamoService = require("./dynamoService");

const createJobTimeoutService = (jobProgressService, notificationService, jobSchedulingService) => {
    // Centralize all timeout configurations
    const JOB_TIMEOUTS = {
        PROCESSING: {
            DURATION: 24 * 60 * 60 * 1000, // 24 hours
            INACTIVITY: 30 * 60 * 1000,    // 30 minutes
        },
        REVIEW: {
            // DURATION: 0.05 * 60 * 60 * 1000, // 5 minutes TESTING TIMEOUT
            // INACTIVITY: 0.01 * 60 * 60 * 1000, // 5 minutes TESTING TIMEOUT
            // EXTENSION: 0.05 * 60 * 60 * 1000  // 5 minutes extension TESTING TIMEOUT
            DURATION: 72 * 60 * 60 * 1000, // 72 hours
            INACTIVITY: 8 * 60 * 60 * 1000, // 8 hours
            EXTENSION: 24 * 60 * 60 * 1000  // 24 hours extension if needed
        }
    };

    const isJobTimedOut = (jobProgress) => {
        // Convert timestamps safely, handling both numeric and ISO string formats
        const parseTimestamp = (timestamp) => {
            if (!timestamp) return Date.now(); // Default to current time if no timestamp
            try {
                // Handle string timestamps (both ISO and numeric strings)
                if (typeof timestamp === 'string') {
                    // If it's a numeric string, convert directly
                    if (!isNaN(timestamp)) return parseInt(timestamp, 10);
                    // Otherwise treat as ISO string
                    return new Date(timestamp).getTime();
                }
                // Handle numeric timestamps
                return typeof timestamp === 'number' ? timestamp : Date.now();
            } catch (error) {
                console.error('[parseTimestamp] Error parsing timestamp:', { timestamp, error });
                return Date.now(); // Fallback to current time
            }
        };

        const currentTime = Date.now();
        const startTime = parseTimestamp(jobProgress.createdAt);
        const lastUpdateTime = parseTimestamp(jobProgress.updatedAt);

        // Add detailed logging to help debug
        console.log('[isJobTimedOut] Timestamp details:', {
            rawCreatedAt: jobProgress.createdAt,
            rawUpdatedAt: jobProgress.updatedAt,
            parsedStartTime: startTime,
            parsedLastUpdateTime: lastUpdateTime,
            currentTime: currentTime,
            timeSinceLastUpdate: (currentTime - lastUpdateTime) / (60 * 1000) // in minutes
        });

        // Get appropriate timeout config based on job status
        const timeoutConfig = jobProgress.status === 'WAITING_FOR_REVIEW'
            ? JOB_TIMEOUTS.REVIEW
            : JOB_TIMEOUTS.PROCESSING;

        console.log('[isJobTimedOut] Using timeout config:', timeoutConfig);
        // TODO: fixed the timeout extension grace period

        // Check max duration timeout
        const maxDuration = timeoutConfig.DURATION;

        // Check inactivity timeout - different thresholds for different statuses
        const inactivityThreshold = timeoutConfig.INACTIVITY;
        const timeUntilMaxDuration = (startTime + maxDuration) - currentTime;

        // Check if we're in the review state and within 8 hours of max duration
        if (jobProgress.status === 'WAITING_FOR_REVIEW' &&
            timeUntilMaxDuration <= inactivityThreshold &&
            timeUntilMaxDuration > 0) {

            const inactiveHours = (currentTime - lastUpdateTime) / (60 * 60 * 1000);

            // Only offer extension if there's been inactivity in the last 8 hours
            if (currentTime - lastUpdateTime > inactivityThreshold) {
                console.log('[isJobTimedOut] Job approaching max duration with inactivity:', {
                    inactiveHours,
                    timeUntilMaxDuration: timeUntilMaxDuration / (60 * 60 * 1000)
                });

                return {
                    timedOut: true,
                    reason: 'REVIEW_INACTIVITY',
                    details: `No review activity for ${inactiveHours.toFixed(1)} hours`,
                    canExtend: !jobProgress.reviewExtended
                };
            }
        }

        // Check max duration timeout
        if (currentTime - startTime > maxDuration) {
            console.log('[isJobTimedOut] Job timed out due to max duration');
            return {
                timedOut: true,
                reason: 'MAX_DURATION_EXCEEDED',
                details: `Job exceeded maximum ${jobProgress.status === 'WAITING_FOR_REVIEW' ? 'review' : 'processing'} time of ${maxDuration / (60 * 60 * 1000)} hours`
            };
        }

        // Check general inactivity timeout
        if (currentTime - lastUpdateTime > inactivityThreshold) {
            return {
                timedOut: true,
                reason: 'INACTIVITY',
                details: `No activity detected for ${inactivityThreshold / (60 * 1000)} minutes`
            };
        }

        return { timedOut: false };
    };

    const handleJobTimeout = async (jobId, jobProgress, timeoutInfo) => {
        console.log('[handleJobTimeout] Handling timeout for job:', {
            jobId,
            status: jobProgress.status,
            timeoutInfo
        });

        try {
            // Check if job is already in terminal state
            if (['COMPLETED', 'FAILED', 'STALE'].includes(jobProgress.status)) {
                console.log('[handleJobTimeout] Job already in terminal state:', jobProgress.status);
                return;
            }

            // Special handling for review timeouts with extension possibility
            if (timeoutInfo.reason === 'REVIEW_INACTIVITY' && timeoutInfo.canExtend) {
                console.log('[handleJobTimeout] Handling review extension for job:', { jobId });
                await handleReviewExtension(jobId);
                return;
            }

            // Handling inactivity timeout
            if (timeoutInfo.reason === 'INACTIVITY') {
                console.log('[handleJobTimeout] Handling inactivity timeout for job:', { jobId });
                //TODO: await handleInactivityTimeout(jobId);
                // return;
            }

            // Cleanup scheduled checks
            await jobSchedulingService.cleanupScheduledChecks(jobId);

            // Auto-review remaining tasks
            await dynamoService.autoReviewAllRemainingTasks(jobId);

            const updates = {
                status: FAILED,
                error: {
                    code: 'TIMEOUT',
                    reason: timeoutInfo.reason,
                    details: timeoutInfo.details,
                    timeoutAt: Date.now().toString()
                },
                completedAt: Date.now().toString()
            };

            // Add auto-review flag if timing out during review
            if (jobProgress.status === 'WAITING_FOR_REVIEW') {
                updates.autoReviewedAsExcluded = true;
                updates.autoReviewedAt = Date.now().toString();
                updates.autoReviewReason = 'REVIEW_TIMEOUT';
                updates.status = COMPLETED;
                // TODO: refactor error fields
            }

            // // Update final statistics considering auto-reviewed items
            const adjustedStats = await jobProgressService.adjustJobStatistics(jobId);
            updates.statistics = adjustedStats;

            console.log('[handleJobTimeout] Updating job progress:', updates);

            // Update final statistics to job progress (DynamoDB)
            await jobProgressService.updateJobProgress(jobId, updates);

            // Update job status to RDS
            await jobProgressService.updateJobStatusRDS(jobId, COMPLETED);
            console.log('[handleJobTimeout] Job statistics updated successfully again');

            if (jobProgress.status === FAILED) {
                await notificationService.publishJobStatus(jobId, 'FAILED', {
                    error: 'TIMEOUT',
                    reason: timeoutInfo.reason,
                    details: timeoutInfo.details,
                    autoReviewed: 0,
                    MessageGroupId: `${jobId}-FAILED`
                });
            } else {
                await notificationService.publishJobStatus(jobId, 'COMPLETED', {
                    finalStatistics: adjustedStats,
                    autoReviewed: 0,
                    MessageGroupId: `${jobId}-COMPLETED`
                });
            }

        } catch (error) {
            console.error('[handleJobTimeout] Error handling job timeout:', error);
            throw error;
        }
    };

    const handleReviewExtension = async (jobId) => {
        console.log('[handleReviewExtension] Handling review extension for job:', { jobId });
        try {
            await jobProgressService.updateJobProgress(jobId, {
                reviewExtended: true,
                reviewExtendedAt: Date.now().toString()
            });

            console.log('[handleReviewExtension] Job progress updated with review extension');

            await notificationService.publishJobStatus(jobId, 'REVIEW_EXTENDED', {
                extendedUntil: new Date(Date.now() + JOB_TIMEOUTS.REVIEW.EXTENSION).toISOString()
            });

            console.log('[handleReviewExtension] Notification published for review extension');
        } catch (error) {
            console.error('Error handling review extension:', error);
            throw error;
        }
    };

    return {
        isJobTimedOut,
        handleJobTimeout,
        JOB_TIMEOUTS // Exposed for testing and reference
    };
};

module.exports = { createJobTimeoutService }; 
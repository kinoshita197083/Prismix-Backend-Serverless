const { COMPLETED, FAILED } = require("../utils/config");

const createJobTimeoutService = (jobProgressService, notificationService) => {
    // Centralize all timeout configurations
    const JOB_TIMEOUTS = {
        PROCESSING: {
            DURATION: 24 * 60 * 60 * 1000, // 24 hours
            INACTIVITY: 30 * 60 * 1000,    // 30 minutes
        },
        REVIEW: {
            // DURATION: 72 * 60 * 60 * 1000, // 72 hours
            DURATION: 1 * 60 * 60 * 1000, // 1 hours TESTING
            INACTIVITY: 24 * 60 * 60 * 1000, // 24 hours
            EXTENSION: 24 * 60 * 60 * 1000  // 24 hours extension if needed
        }
    };

    const isJobTimedOut = (jobProgress) => {
        const currentTime = new Date().getTime();
        const startTime = new Date(jobProgress.createdAt).getTime();
        const lastUpdateTime = new Date(jobProgress.updatedAt || jobProgress.createdAt).getTime();

        // Get appropriate timeout config based on job status
        const timeoutConfig = jobProgress.status === 'WAITING_FOR_REVIEW'
            ? JOB_TIMEOUTS.REVIEW
            : JOB_TIMEOUTS.PROCESSING;

        // Check max duration timeout
        const maxDuration = timeoutConfig.DURATION;
        if (currentTime - startTime > maxDuration) {
            return {
                timedOut: true,
                reason: 'MAX_DURATION_EXCEEDED',
                details: `Job exceeded maximum ${jobProgress.status === 'WAITING_FOR_REVIEW' ? 'review' : 'processing'} time of ${maxDuration / (60 * 60 * 1000)} hours`
            };
        }

        // Check inactivity timeout - different thresholds for different statuses
        const inactivityThreshold = timeoutConfig.INACTIVITY;
        if (currentTime - lastUpdateTime > inactivityThreshold) {
            // For review status, we might want to send notifications before timing out
            if (jobProgress.status === 'WAITING_FOR_REVIEW') {
                const inactiveHours = (currentTime - lastUpdateTime) / (60 * 60 * 1000);
                return {
                    timedOut: true,
                    reason: 'REVIEW_INACTIVITY',
                    details: `No review activity for ${inactiveHours.toFixed(1)} hours`,
                    canExtend: !jobProgress.reviewExtended
                };
            }

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
                await jobSchedulingService.cleanupScheduledChecks(jobId);
                return;
            }

            // Special handling for review timeouts with extension possibility
            if (timeoutInfo.reason === 'REVIEW_INACTIVITY' && timeoutInfo.canExtend) {
                await handleReviewExtension(jobId, jobProgress);
                return;
            }

            const updates = {
                status: FAILED,
                error: {
                    code: 'TIMEOUT',
                    reason: timeoutInfo.reason,
                    details: timeoutInfo.details,
                    timeoutAt: new Date().toISOString()
                },
                completedAt: new Date().toISOString()
            };

            // Add auto-review flag if timing out during review
            if (jobProgress.status === 'WAITING_FOR_REVIEW') {
                updates.autoReviewedAsExcluded = true;
                updates.autoReviewedAt = new Date().toISOString();
                updates.autoReviewReason = 'REVIEW_TIMEOUT';
                updates.status = COMPLETED;
                // TODO: refactor error fields
            }

            await jobProgressService.updateJobProgress(jobId, updates);

            // Update final statistics considering auto-reviewed items
            const finalStats = await jobProgressService.getJobStatistics(jobId);
            const adjustedStats = {
                ...finalStats,
                // Move waiting for review count to excluded count
                excluded: finalStats.excluded + finalStats.waitingForReview,
                waitingForReview: 0,
                timeoutReason: timeoutInfo.reason,
                autoReviewed: finalStats.waitingForReview // Track how many were auto-reviewed
            };

            await jobProgressService.updateJobProgress(jobId, {
                statistics: adjustedStats
            });

            if (jobProgress.status === FAILED) {
                await notificationService.publishJobStatus(jobId, 'FAILED', {
                    error: 'TIMEOUT',
                    reason: timeoutInfo.reason,
                    details: timeoutInfo.details,
                    autoReviewed: finalStats.waitingForReview > 0 ? {
                        count: finalStats.waitingForReview,
                        action: 'EXCLUDED'
                    } : undefined
                });
            } else {
                await notificationService.publishJobStatus(jobId, 'COMPLETED', {
                    finalStatistics: adjustedStats,
                    autoReviewed: finalStats.waitingForReview > 0 ? {
                        count: finalStats.waitingForReview,
                        action: 'EXCLUDED'
                    } : undefined
                });
            }

        } catch (error) {
            console.error('[handleJobTimeout] Error handling job timeout:', error);
            throw error;
        }
    };

    const handleReviewExtension = async (jobId, jobProgress) => {
        try {
            await jobProgressService.updateJobProgress(jobId, {
                reviewExtended: true,
                reviewExtendedAt: new Date().toISOString()
            });

            await notificationService.publishJobStatus(jobId, 'REVIEW_EXTENDED', {
                extendedUntil: new Date(Date.now() + JOB_TIMEOUTS.REVIEW.EXTENSION).toISOString()
            });
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
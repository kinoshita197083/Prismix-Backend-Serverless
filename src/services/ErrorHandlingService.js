const { FAILED } = require("../utils/config");

const createErrorHandlingService = (jobProgressService, cloudWatchService, jobSchedulingService, jobCompletionService) => {
    const TERMINAL_ERRORS = ['SYSTEM_ERROR', 'CONFIGURATION_ERROR'];
    const RETRYABLE_ERRORS = ['HEALTH_CHECK_FAILED', 'TEMPORARY_ERROR'];
    const MAX_HEALTH_CHECK_RETRIES = 3;

    const handleProcessingError = async (jobId, error) => {
        console.log('[handleProcessingError] Handling processing error for jobId:', jobId);
        console.log('[handleProcessingError] Error:', error);

        try {
            await cloudWatchService.recordMetrics('JobProcessingError', {
                duration: 0,
                success: false,
                errorCode: error.code || 'UNKNOWN_ERROR'
            });

            const currentJobProgress = await jobProgressService.getCurrentJobProgress(jobId);
            const currentSchedulingStatus = currentJobProgress?.schedulingStatus;
            const currentStatus = currentJobProgress?.status;
            const currentHealthCheckRetries = currentJobProgress?.healthCheckRetries || 0;

            console.log('[handleProcessingError] Current health check retries:', {
                currentHealthCheckRetries,
                currentSchedulingStatus,
                currentStatus
            });

            if (error.code === 'HEALTH_CHECK_FAILED' && currentHealthCheckRetries < MAX_HEALTH_CHECK_RETRIES) {
                // Implement exponential backoff for health check retries
                const MAX_BACKOFF_DELAY = 300000; // 5 minutes
                const backoffDelay = Math.min(
                    Math.pow(2, currentHealthCheckRetries) * 1000,
                    MAX_BACKOFF_DELAY
                );

                await jobProgressService.updateJobProgress(jobId, {
                    // status: 'IN_PROGRESS',
                    healthCheckErrors: {
                        lastError: {
                            code: error.code,
                            message: error.message,
                            timestamp: Date.now().toString(),
                            details: error.details || {}
                        },
                        healthCheckRetries: currentHealthCheckRetries + 1,
                        lastHealthCheckAttempt: Date.now().toString()
                    }
                });

                console.log('[handleProcessingError] Job progress updated with new health check retry');

                // Schedule retry with backoff
                await jobSchedulingService.scheduleNextCheck(jobId, currentStatus, {
                    minimumDelay: backoffDelay / 1000
                }, jobProgressService);

                console.log('[handleProcessingError] Next check scheduled with backoff');
                return;
            }

            // If max retries exceeded or other error, handle as terminal
            await jobCompletionService.handleJobCompletion(jobId, FAILED);

        } catch (handlingError) {
            console.error('[handleProcessingError] Error handling job error:', handlingError);
            // throw handlingError;
        }
    };

    return {
        handleProcessingError,
        TERMINAL_ERRORS,
        RETRYABLE_ERRORS,
        MAX_HEALTH_CHECK_RETRIES
    };
};

module.exports = { createErrorHandlingService }; 
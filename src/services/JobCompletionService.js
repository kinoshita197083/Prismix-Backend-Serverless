const createJobCompletionService = (
    jobProgressService,
    notificationService,
    eventBridgeService,
    cloudWatchService,
    jobSchedulingService
) => {
    const handleJobCompletion = async (jobId, newStatus, retryCount = 0) => {
        const MAX_RETRIES = 3;

        console.log('[handleJobCompletion] Processing completion:', {
            jobId,
            newStatus,
            retryCount
        });

        try {
            // const currentJob = await jobProgressService.getCurrentJobProgress(jobId);

            // if (!currentJob) {
            //     throw new Error(`Job ${jobId} not found`);
            // }

            try {
                console.log('[handleJobCompletion] Publishing job completion event');

                if (newStatus === 'COMPLETED') {
                    await Promise.all([
                        jobSchedulingService.cleanupScheduledChecks(jobId),
                        notificationService.publishJobStatus(jobId, newStatus, {
                            completedAt: new Date().toISOString(),
                            status: newStatus,
                        }),
                        jobProgressService.updateJobStatusRDS(jobId, newStatus)
                    ]);
                }

                console.log('[handleJobCompletion] Job completion processed successfully');

                return true;

            } catch (error) {
                if (error.name === 'ConditionalCheckFailedException') {
                    if (retryCount < MAX_RETRIES) {
                        console.log('[handleJobCompletion] Optimistic lock failed, retrying...', {
                            jobId,
                            retryCount: retryCount + 1
                        });

                        const delay = Math.pow(2, retryCount) * 100; // 100ms, 200ms, 400ms
                        await new Promise(resolve => setTimeout(resolve, delay));

                        return handleJobCompletion(jobId, newStatus, retryCount + 1);
                    }

                    throw new Error(`Failed to update job ${jobId} after ${MAX_RETRIES} retries due to concurrent modifications`);
                }

                throw error;
            }

        } catch (error) {
            console.error('[handleJobCompletion] Error updating job completion:', error);

            await cloudWatchService.recordMetrics('JobCompletionError', {
                jobId,
                error: error.message,
                retryCount
            });

            throw error;
        }
    };

    const determineJobStatus = (stats, jobProgress) => {
        console.log('[determineJobStatus] Evaluating status with:', {
            stats,
            totalImages: jobProgress.totalImages,
            currentStatus: jobProgress.status
        });

        if (stats.lastEvaluatedKey) {
            return 'IN_PROGRESS';
        }

        if (stats.waitingForReview > 0) {
            return 'WAITING_FOR_REVIEW';
        }

        const allImagesProcessed = stats.totalProcessed === jobProgress.totalImages;
        const allImagesAccounted = (stats.eligible + stats.excluded + stats.duplicates + stats.failed) === jobProgress.totalImages;

        if (!allImagesProcessed || !allImagesAccounted) {
            console.log('[determineJobStatus] Not all images processed:', {
                processed: stats.totalProcessed,
                total: jobProgress.totalImages,
                eligible: stats.eligible,
                excluded: stats.excluded,
                duplicates: stats.duplicates,
                failed: stats.failed
            });
            return 'IN_PROGRESS';
        }

        if (allImagesProcessed && allImagesAccounted && stats.waitingForReview === 0) {
            return 'COMPLETED';
        }

        return 'IN_PROGRESS';
    };

    const validateJobVersion = (currentJob, expectedVersion) => {
        if (!currentJob) {
            throw new Error('Job not found');
        }

        if (currentJob.version !== expectedVersion) {
            throw new Error(`Version mismatch. Expected ${expectedVersion}, got ${currentJob.version}`);
        }

        return true;
    };

    return {
        handleJobCompletion,
        determineJobStatus,
        validateJobVersion
    };
};

module.exports = { createJobCompletionService }; 
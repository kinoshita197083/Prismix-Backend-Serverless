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
            // First ensure the job exists and isn't already completed
            const currentJob = await jobProgressService.getCurrentJobProgress(jobId);
            if (!currentJob) {
                throw new Error(`Job ${jobId} not found`);
            }

            if (['COMPLETED', 'FAILED'].includes(currentJob.status)) {
                console.log('[handleJobCompletion] entered terminal state check');
                // return true;
            }

            try {
                // Step 1: Clean up scheduled checks first
                console.log('[handleJobCompletion] Job already in terminal state:', currentJob.status);
                await jobSchedulingService.cleanupScheduledChecks(jobId);

                // Step 2: Update status and notify in parallel
                await jobProgressService.updateJobStatusAndNotify(jobId, newStatus);

                // Step 3: Record completion metrics
                await cloudWatchService.recordMetrics('JobCompletion', {
                    status: newStatus,
                    duration: Date.now() - new Date(currentJob.createdAt).getTime()
                });

                console.log('[handleJobCompletion] Job completion processed successfully');
                return true;

            } catch (error) {
                if (error.name === 'ConditionalCheckFailedException' && retryCount < MAX_RETRIES) {
                    console.log('[handleJobCompletion] Optimistic lock failed, retrying...', {
                        retryCount: retryCount + 1
                    });

                    const delay = Math.min(Math.pow(2, retryCount) * 100, 1000); // Max 1 second delay
                    await new Promise(resolve => setTimeout(resolve, delay));
                    return handleJobCompletion(jobId, newStatus, retryCount + 1);
                }
                throw error;
            }
        } catch (error) {
            console.error('[handleJobCompletion] Error completing job:', error);
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

        const allImagesProcessed = stats.totalProcessed >= jobProgress.totalImages;
        const allImagesAccounted = (stats.eligible + stats.excluded + stats.duplicates + stats.failed) >= jobProgress.totalImages;

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
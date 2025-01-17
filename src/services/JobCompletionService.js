const { UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { calculateJobCost } = require("../utils/helpers");

const createJobCompletionService = (
    jobProgressService,
    notificationService,
    eventBridgeService,
    cloudWatchService,
    jobSchedulingService,
    supabaseService,
    jobStatisticsService
) => {
    const handleJobCompletion = async (jobId, newStatus, retryCount = 0) => {
        const MAX_RETRIES = 3;
        let statusBeforeUpdate;

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
            statusBeforeUpdate = currentJob.status;

            try {
                // Step 1: Clean up scheduled checks first
                console.log('[handleJobCompletion] Job already in terminal state:', currentJob.status);
                await jobSchedulingService.cleanupScheduledChecks(jobId);

                // Step 2: Update status and notify in parallel
                await jobProgressService.updateJobStatusAndNotify(jobId, newStatus, currentJob.outputConnection);
                console.log('[handleJobCompletion] Job status updated and notified');

                // Step 3: Record completion metrics
                await cloudWatchService.recordMetrics('JobCompletion', {
                    status: newStatus,
                    duration: Date.now() - new Date(currentJob.createdAt).getTime()
                });
                console.log('[handleJobCompletion] Job completion metrics recorded');

                // Step 4: recaculate job statistics after user has reviewed the tasks
                if (statusBeforeUpdate === 'WAITING_FOR_REVIEW') {
                    const stats = await jobStatisticsService.getJobStatisticsWithPagination(jobId, currentJob.jobProgress);

                    const currentVersion = currentJob.version || 0;
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
                                ...stats,
                            },
                            ':updatedAt': Date.now().toString(),
                            ':currentVersion': currentVersion,
                            ':newVersion': newVersion
                        }
                    };

                    const result = await jobProgressService.dynamoDB.send(new UpdateCommand(updateParams));
                    console.log('[handleJobCompletion] Job statistics finalized: ', result);
                }

                // Step 5: Check if refund is needed
                switch (newStatus) {
                    case 'COMPLETED':
                        // Check if there's failed tasks
                        const failedTasks = +currentJob.statistics?.failed || 0;

                        if (failedTasks) {
                            const projectSettings = currentJob.projectSetting;
                            const job = currentJob.job;
                            const imageCount = failedTasks;
                            const cost = calculateJobCost({ imageCount, projectSettings, job });
                            await supabaseService.refundUserCreditBalance(currentJob.userId, cost, 'Job completed with failed tasks', jobId);
                        }
                        break;
                    case 'FAILED':
                        // Refund user credits if job failed
                        const creditsTransactions = await supabaseService.getAllCreditsTransactionsOfJob(jobId);
                        const totalCreditsSpent = creditsTransactions.reduce((acc, transaction) => {
                            console.log('[handleJobCompletion] Credits transaction:', transaction);
                            // preAuth transactions are the ones that are refunded
                            if (transaction.type === 'preAuth') {
                                return acc + transaction.credits;
                            }
                            return acc;
                        }, 0);
                        await supabaseService.refundUserCreditBalance(currentJob.userId, totalCreditsSpent, 'Job failed', jobId);
                        break;
                    default:
                        break;
                }

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
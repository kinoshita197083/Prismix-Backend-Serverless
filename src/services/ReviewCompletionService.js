const { QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { COMPLETED } = require("../utils/config");


const createReviewCompletionService = (
    jobProgressService,
    jobCompletionService
) => {
    const verifyAllTasksReviewed = async (jobId) => {
        console.log(`[verifyAllTasksReviewed] Verifying review completion for job: ${jobId}`);

        try {
            // Get current job progress to check auto-review status
            const jobProgress = await jobProgressService.getCurrentJobProgress(jobId);

            // If job was auto-reviewed due to timeout, consider all tasks reviewed
            if (jobProgress.autoReviewedAsExcluded) {
                console.log(`[verifyAllTasksReviewed] Job ${jobId} was auto-reviewed, considering all tasks reviewed`);
                return 0;
            }

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
            const pendingCount = result.Items?.length || 0;

            console.log(`[verifyAllTasksReviewed] Found ${pendingCount} tasks pending review for job: ${jobId}`);
            return pendingCount;

        } catch (error) {
            console.error(`[verifyAllTasksReviewed] Error verifying tasks:`, error);
            throw new Error(`Failed to verify task review status: ${error.message}`);
        }
    };

    const handleReviewCompletion = async (jobId) => {
        console.log(`[handleReviewCompletion] Processing review completion for job: ${jobId}`);

        try {
            const pendingReviews = await verifyAllTasksReviewed(jobId);

            console.log('[handleReviewCompletion] Pending reviews:', pendingReviews);

            if (pendingReviews > 0) {
                throw new AppError(
                    'ReviewIncomplete',
                    `Job still has ${pendingReviews} tasks pending review`
                );
            }

            // Let jobCompletionService handle the completion logic
            await jobCompletionService.handleJobCompletion(jobId, COMPLETED);

            console.log(`[handleReviewCompletion] Successfully completed review for job: ${jobId}`);

        } catch (error) {
            console.error(`[handleReviewCompletion] Error processing review completion:`, error);
            throw error;
        }
    };

    return {
        handleReviewCompletion,
        verifyAllTasksReviewed
    };
};

module.exports = { createReviewCompletionService }; 
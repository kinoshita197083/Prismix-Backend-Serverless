const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const { SNSClient } = require('@aws-sdk/client-sns');
const { EventBridgeClient } = require("@aws-sdk/client-eventbridge");
const { createClient } = require('@supabase/supabase-js');
const JobProgressService = require('../services/jobProgressService');
const NotificationService = require('../services/notificationService');
const EventBridgeService = require('../services/eventBridgeService');

// Initialize services
const dynamoDBClient = new DynamoDBClient();
const documentClient = DynamoDBDocumentClient.from(dynamoDBClient, {
    marshallOptions: {
        removeUndefinedValues: true,
    },
});

const jobProgressService = new JobProgressService(
    documentClient,
    createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY),
    {
        tasksTable: process.env.TASKS_TABLE,
        jobProgressTable: process.env.JOB_PROGRESS_TABLE
    }
);

const notificationService = new NotificationService(new SNSClient(), process.env.JOB_COMPLETION_TOPIC_ARN);
const eventBridgeService = new EventBridgeService(new EventBridgeClient());

const MAX_RETRY_COUNT = 3;
const RETRY_INTERVAL = 5 * 60 * 1000; // 5 minutes in milliseconds

async function processJobProgress(jobId) {
    console.log('[processJobProgress] Starting job progress check for jobId:', jobId);

    try {
        // Get current job progress first to check for inactivity
        console.log('[processJobProgress] Fetching current job progress...');
        const currentProgress = await jobProgressService.getCurrentJobProgress(jobId);
        console.log('[processJobProgress] Current progress:', currentProgress);

        // Get current job stats from tasks table
        console.log('[processJobProgress] Fetching job stats...');
        const jobStats = await jobProgressService.getJobStats(jobId);
        console.log('[processJobProgress] Job stats retrieved:', jobStats);

        // Check if all images are processed
        const totalProcessed = jobStats.eligibleImages +
            jobStats.duplicateImages +
            jobStats.excludedImages;

        const isComplete = jobStats.processedImages === totalProcessed;

        console.log('[processJobProgress] Processing status:', {
            isComplete,
            processedImages: jobStats.processedImages,
            totalProcessed,
            manualReviewRequired: currentProgress.manualReviewRequired
        });

        let status;
        // If manual review is required and not completed yet
        if (currentProgress.manualReviewRequired === true) {
            status = 'WAITING_FOR_REVIEW';
            console.log('[processJobProgress] Job requires manual review');
        }
        // If manual review was required but now completed (manualReviewRequired set to false by API)
        else if (currentProgress.manualReviewRequired === false && isComplete) {
            status = 'COMPLETED';
            console.log('[processJobProgress] Manual review completed, job is complete');
        }
        // Normal processing flow
        else {
            status = isComplete ? 'COMPLETED' : 'IN_PROGRESS';
            console.log('[processJobProgress] Normal flow status:', status);
        }

        // Prepare update data
        const updateData = {
            ...jobStats,
            status,
            version: currentProgress.version + 1
        };
        console.log('[processJobProgress] Prepared update data:', updateData);

        // Update job progress
        await jobProgressService.updateJobProgress(jobId, updateData, currentProgress.version);
        console.log('[processJobProgress] Job progress updated successfully');

        // Update RDS if status changed
        if (status !== currentProgress.status) {
            console.log('[processJobProgress] Status changed, updating RDS...', {
                previousStatus: currentProgress.status,
                newStatus: status
            });
            await jobProgressService.updateJobStatusRDS(jobId, status);
            console.log('[processJobProgress] RDS status updated successfully');
        }

        return status;

    } catch (error) {
        console.error('[processJobProgress] Error processing job:', error);
        throw error;
    }
}

async function handleJobStatus(jobId, status) {
    console.log('[handleJobStatus] Handling job status:', { jobId, status });

    // Only cleanup EventBridge rule when job is truly completed
    // Do not cleanup for WAITING_FOR_REVIEW status
    if (status === 'COMPLETED') {
        console.log('[handleJobStatus] Job completed, cleaning up...');

        try {
            await Promise.all([
                notificationService.publishJobStatus(jobId, status),
                eventBridgeService.disableAndDeleteRule(jobId)
            ]);
            console.log('[handleJobStatus] Cleanup completed successfully');
        } catch (error) {
            console.error('[handleJobStatus] Error during cleanup:', error);
            // Continue execution even if cleanup fails
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify({
            jobId,
            status,
            message: `Job progress check completed. Status: ${status}`
        })
    };
}

function handleError(error) {
    console.error('[handleError] Error in job progress checker:', {
        error: error.message,
        stack: error.stack,
        name: error.name
    });

    return {
        statusCode: 500,
        body: JSON.stringify({
            message: 'Error processing job progress',
            error: error.message
        })
    };
}

exports.handler = async (event) => {
    console.log('[handler] Received event:', JSON.stringify(event, null, 2));

    try {
        const { jobId } = event;
        if (!jobId) {
            console.error('[handler] Invalid event structure - missing jobId');
            throw new Error('Invalid event structure');
        }

        console.log('[handler] Processing job:', jobId);
        const jobStatus = await processJobProgress(jobId);
        console.log('[handler] Job processing completed with status:', jobStatus);

        return await handleJobStatus(jobId, jobStatus);
    } catch (error) {
        return handleError(error);
    }
};

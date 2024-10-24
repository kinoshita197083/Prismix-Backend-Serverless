const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { createClient } = require('@supabase/supabase-js');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const logger = require('../utils/logger');
const { EventBridgeClient, DisableRuleCommand, DeleteRuleCommand,
    ListTargetsByRuleCommand, RemoveTargetsCommand
} = require("@aws-sdk/client-eventbridge");

const client = new DynamoDBClient({});
const dynamoDB = DynamoDBDocumentClient.from(client);
const snsClient = new SNSClient();
const eventBridgeClient = new EventBridgeClient();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

const TASKS_TABLE = process.env.TASKS_TABLE;
const JOB_PROGRESS_TABLE = process.env.JOB_PROGRESS_TABLE;

exports.handler = async (event) => {
    try {
        console.log('----> Event received: ', event);

        const { jobId } = event;

        if (!jobId) {
            throw new Error('Invalid event structure');
        }

        // Check job status and update progress
        const jobCompleted = await checkAndUpdateJobStatus(jobId);

        // If job is completed, update RDS and SNS and disable the EventBridge rule
        if (jobCompleted) {
            await updateJobStatusRDS(jobId, 'COMPLETED');
            // logger.info('Job status updated in RDS', { jobId, status: 'COMPLETED' });
            console.log('Job status updated in RDS', { jobId, status: 'COMPLETED' });

            await publishToSNS(jobId);
            // logger.info('Job completion published to SNS', { jobId });
            console.log('Job completion published to SNS', { jobId });

            await disableEventBridgeRule(jobId);
            console.log(`Job completed, disabling EventBridge rule for job ${jobId}`);
        }

        console.log(`Successfully processed event for jobId: ${jobId}`);
        return { statusCode: 200, body: 'Event processed successfully' };
    } catch (error) {
        console.error('Error processing event:', error);

        // Determine if the error is retryable
        if (isRetryableError(error)) {
            // For retryable errors, we throw the error to trigger a retry
            throw error;
        } else {
            // For non-retryable errors, we log the error and return a failure response
            return {
                statusCode: 500,
                body: 'Error processing event: ' + error.message
            };
        }
    }
};

function isRetryableError(error) {
    // List of error types that are typically transient and benefit from retries
    const retryableErrors = [
        'ProvisionedThroughputExceededException',
        'ThrottlingException',
        'RequestLimitExceeded',
        'InternalServerError',
        'ServiceUnavailable'
    ];

    return retryableErrors.includes(error.name);
}

// Check job status and update it with version control
async function checkAndUpdateJobStatus(jobId) {
    // logger.info('Starting job status check and update', { jobId });
    console.log('Starting job status check and update', { jobId });

    try {
        const { processedImages,
            eligibleImages,
            duplicateImages,
            failedImages,
            excludedImages } = await getJobStats(jobId);
        // logger.debug('Retrieved job stats', { jobId, processedImages, eligibles, duplicates, failedImages, excludedImages });
        console.log('Retrieved job stats', { jobId, processedImages, eligibleImages, duplicateImages, failedImages, excludedImages });

        const { version,
            status,
            totalImages,
            processedImages: currentProcessedImages,
            eligibleImages: currentEligibles,
            duplicateImages: currentDuplicates,
            failedImages: currentFailedImages,
            excludedImages: currentExcludedImages,
        } = await getCurrentJobProgress(jobId);
        // logger.debug('Retrieved current job progress', { jobId, currentProcessedImages, currentEligibles, currentDuplicates, currentFailedImages, currentExcludedImages });
        console.log('Retrieved current job progress', { jobId, currentProcessedImages, currentEligibles, currentDuplicates, currentFailedImages, currentExcludedImages, totalImages });

        // Only update if there's real progress
        if (
            processedImages !== currentProcessedImages ||
            eligibleImages !== currentEligibles ||
            failedImages !== currentFailedImages ||
            excludedImages !== currentExcludedImages ||
            duplicateImages !== currentDuplicates ||
            status !== 'COMPLETED'
        ) {
            // logger.info('Progress detected, updating job status', { jobId });
            console.log('Progress detected, updating job status', { jobId });

            // Update job progress with optimistic locking based on version
            if (processedImages < totalImages) {
                console.log('Job not completed yet, updating progress');
                await updateJobProgress(jobId, processedImages, eligibleImages, failedImages, excludedImages, duplicateImages, 'IN_PROGRESS', version);
                // logger.info('Job progress updated', { jobId, status: 'IN_PROGRESS' });
                console.log('Job progress updated', { jobId, status: 'IN_PROGRESS' });
                return false;
            } else if (processedImages === totalImages) {
                console.log('Job completed, updating progress to COMPLETED');
                await updateJobProgress(jobId, processedImages, eligibleImages, failedImages, excludedImages, duplicateImages, 'COMPLETED', version);
                // logger.info('Job completed, progress updated', { jobId, status: 'COMPLETED' });
                console.log('Job completed, progress updated', { jobId, status: 'COMPLETED' });
                return true;
            }
        } else {
            // logger.info('No progress detected, skipping update', { jobId });
            console.log('No progress detected, skipping update', { jobId });
            return false;
        }
    } catch (error) {
        // logger.error('Error in checkAndUpdateJobStatus', {
        console.log('Error in checkAndUpdateJobStatus', {
            jobId,
            error: error.message,
            stack: error.stack
        });

        if (error.name === 'ConditionalCheckFailedException') {
            // logger.warn('Version conflict detected, job update will be retried', { jobId });
            console.log('Version conflict detected, job update will be retried', { jobId });
            throw error; // Rethrow to trigger retry
        }

        // Handle other specific errors as needed
        if (error.name === 'ResourceNotFoundException') {
            // logger.error('Job or related resource not found', { jobId });
            console.log('Job or related resource not found', { jobId });
            // Handle accordingly (e.g., mark job as failed, notify user)
        }

        throw error; // Rethrow other errors for general error handling
    }
}

// Fetch job stats from tasks table
async function getJobStats(jobId) {
    const params = {
        TableName: TASKS_TABLE,
        KeyConditionExpression: "JobID = :jobId",
        ExpressionAttributeValues: { ':jobId': jobId }
    };

    try {
        const result = await dynamoDB.send(new QueryCommand(params));

        // console.log('Query result for Task Table: ', result);

        return result.Items.reduce((acc, task) => {
            acc.processedImages += task.TaskStatus === 'COMPLETED' ? 1 : 0;
            acc.eligibleImages += task.Evaluation === 'ELIGIBLE' ? 1 : 0;
            acc.duplicateImages += task.Evaluation === 'DUPLICATE' ? 1 : 0;
            acc.failedImages += task.TaskStatus === 'FAILED' ? 1 : 0;
            acc.excludedImages += task.Evaluation === 'EXCLUDED' ? 1 : 0;
            return acc;
        }, { processedImages: 0, eligibleImages: 0, duplicateImages: 0, failedImages: 0, excludedImages: 0 });
    } catch (error) {
        // logger.error('Error in getJobStats', { error: error.message, stack: error.stack });
        console.log('777 ', error);
        throw error;
    }
}

// Fetch the current job progress including the version
async function getCurrentJobProgress(jobId) {
    const params = {
        TableName: JOB_PROGRESS_TABLE,
        Key: { JobId: jobId }
    };

    const result = await dynamoDB.send(new GetCommand(params));
    return result.Item || { processedImages: 0, eligibleImages: 0, duplicateImages: 0, failedImages: 0, excludedImages: 0, status: 'IN_PROGRESS', version: 1 };
}

// Update job progress with version check (optimistic locking)
async function updateJobProgress(jobId, processedImages, eligibleImages, failedImages, excludedImages, duplicateImages, status, currentVersion) {
    const params = {
        TableName: JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET ' + [
            'processedImages = :processedImages',
            'eligibleImages = :eligibleImages',
            'failedImages = :failedImages',
            'excludedImages = :excludedImages',
            'duplicateImages = :duplicateImages',
            '#status = :status',
            'version = :newVersion',
            'updatedAt = :updatedAt'
        ].join(', '),
        ConditionExpression: 'version = :currentVersion', // Optimistic locking
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: {
            ':processedImages': processedImages,
            ':eligibleImages': eligibleImages,
            ':failedImages': failedImages,
            ':excludedImages': excludedImages,
            ':duplicateImages': duplicateImages,
            ':status': status,
            ':newVersion': currentVersion + 1, // Increment version
            ':currentVersion': currentVersion,
            ':updatedAt': new Date().toISOString()
        }
    };

    try {
        const result = await dynamoDB.send(new UpdateCommand(params));
        console.log('Job progress updated', { result });
    } catch (error) {
        if (error.name === 'ConditionalCheckFailedException') {
            console.log(`Version mismatch for jobId: ${jobId}, retrying update...`);
            throw error;
        }
        throw error;
    }
}

async function publishToSNS(jobId) {
    const params = {
        Message: JSON.stringify({ jobId }),
        TopicArn: process.env.JOB_COMPLETION_TOPIC_ARN
    };

    // logger.info(`Publishing job completion to SNS for job ${jobId}`, { params });
    console.log(`Publishing job completion to SNS for job ${jobId}`, { params });

    try {
        const command = new PublishCommand(params);
        const result = await snsClient.send(command);
        // logger.info(`Successfully published job completion for ${jobId} to SNS`, { messageId: result.MessageId });
        console.log(`Successfully published job completion for ${jobId} to SNS`, { messageId: result.MessageId });
    } catch (error) {
        // logger.error(`Error publishing to SNS for job ${jobId}:`, { error: error.message, params });
        console.log(`Error publishing to SNS for job ${jobId}:`, { error: error.message, params });
        throw error;
    }
}

async function updateJobStatusRDS(jobId, status) {
    console.log(`Updating job status in RDS to ${status} for Job ID: ${jobId}`);
    try {
        const { data, error } = await supabase
            .from('Job')
            .update({ jobStatus: status })
            .eq('id', jobId)
            .single();

        if (error) throw error;

        console.log(`Successfully updated job status in RDS to ${status} for Job ID: ${jobId}`);
        return data;
    } catch (error) {
        console.error(`Error updating job status in RDS for Job ID ${jobId}:`, error);
        throw error;
    }
}

async function disableAndDeleteEventBridgeRule(ruleName) {
    try {
        // Disable the rule
        await eventBridgeClient.send(new DisableRuleCommand({ Name: ruleName }));
        console.log(`Disabled EventBridge rule: ${ruleName}`);

        // List targets for the rule
        const listTargetsResponse = await eventBridgeClient.send(new ListTargetsByRuleCommand({ Rule: ruleName }));
        const targets = listTargetsResponse.Targets;

        if (targets && targets.length > 0) {
            // Remove all targets
            const targetIds = targets.map(target => target.Id);
            await eventBridgeClient.send(new RemoveTargetsCommand({
                Rule: ruleName,
                Ids: targetIds
            }));
            console.log(`Removed ${targetIds.length} targets from rule: ${ruleName}`);
        }

        // Delete the rule
        await eventBridgeClient.send(new DeleteRuleCommand({ Name: ruleName }));
        console.log(`Deleted EventBridge rule: ${ruleName}`);
    } catch (error) {
        console.error(`Error disabling and deleting EventBridge rule ${ruleName}:`, error);
        throw error;
    }
}

async function disableEventBridgeRule(jobId) {
    const ruleName = `JobProgressCheck-${jobId}`;
    await disableAndDeleteEventBridgeRule(ruleName);
    console.log(`Disabled and deleted EventBridge rule for job ${jobId}`);
}

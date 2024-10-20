const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

const snsClient = new SNSClient();
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

exports.handler = async (event) => {
    logger.info('Job completion checker started', event);

    const uniqueRecords = removeDuplicateRecords(event.Records);

    console.log('Original records: ', event.Records.length);
    console.log('Unique records: ', uniqueRecords.length);

    for (const record of uniqueRecords) {
        if (record.eventName === 'MODIFY') {
            const newImage = record.dynamodb.NewImage;
            const keys = record.dynamodb.Keys;
            const jobId = keys.JobId?.S;

            if (!jobId) {
                logger.error('JobId is missing in the new image', { newImage });
                continue; // Skip this record if JobId is not present
            }

            const jobCompletionCommand = new GetCommand({
                TableName: process.env.JOB_PROGRESS_TABLE,
                Key: { JobId: jobId }
            });

            const jobCompletionResult = await docClient.send(jobCompletionCommand);
            const jobCompletionItem = jobCompletionResult.Item;
            const jobCompleted = jobCompletionItem.JobCompleted?.BOOL;

            console.log('----> jobCompleted: ', jobCompleted);

            if (jobCompleted) {
                logger.info(`Job ${jobId} is already completed. Skipping.`);
                return;
            }

            // Check if Status field exists in the new image
            if (newImage.Status && newImage.Status.S) {
                const status = newImage.Status.S;

                logger.info(`Processing job ${jobId} with status ${status}`);

                if (status === 'COMPLETED') {
                    logger.info(`Job ${jobId} is completed. Initiating post-processing.`);
                    try {
                        const jobCompleted = await completeJob(jobId);
                        await publishToSNS(jobId);
                        console.log('----> jobCompleted: ', jobCompleted);
                    } catch (error) {
                        logger.error(`Failed to publish completion for job ${jobId}`, { error: error.message, stack: error.stack });
                    }
                } else {
                    logger.info(`Job ${jobId} status is ${status}. No action needed.`);
                }
            } else {
                logger.info(`Job ${jobId} update does not include Status field. Skipping.`);
            }
        } else {
            logger.info(`Ignoring non-MODIFY event: ${record.eventName}`);
        }
    }

    logger.info('Job completion checker finished');
};

async function publishToSNS(jobId) {
    const params = {
        Message: JSON.stringify({ jobId }),
        TopicArn: process.env.JOB_COMPLETION_TOPIC_ARN
    };

    logger.info(`Publishing job completion to SNS for job ${jobId}`, { params });

    try {
        const command = new PublishCommand(params);
        const result = await snsClient.send(command);
        logger.info(`Successfully published job completion for ${jobId} to SNS`, { messageId: result.MessageId });
    } catch (error) {
        logger.error(`Error publishing to SNS for job ${jobId}:`, { error: error.message, params });
        throw error;
    }
}

async function completeJob(jobId) {
    logger.info(`Completing job ${jobId}`);

    // IMPORTANT: This function should never modify or update TotalImages
    const updateCommand = new UpdateCommand({
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET #JobCompleted = :completed',
        ExpressionAttributeNames: { '#JobCompleted': 'JobCompleted' },
        ExpressionAttributeValues: { ':completed': true },
        ReturnValues: 'ALL_NEW'  // This will return the updated item
    });

    try {
        const result = await docClient.send(updateCommand);
        const updatedItem = result.Attributes;

        if (!updatedItem) {
            logger.error(`No item returned after update for job ${jobId}`);
            return false;
        }

        if (typeof updatedItem.JobCompleted === 'undefined') {
            logger.error(`JobCompleted field is undefined for job ${jobId}`, { updatedItem });
            return false;
        }

        const jobCompleted = updatedItem.JobCompleted;
        logger.info(`Job ${jobId} completed successfully`, { jobCompleted, updatedItem });
        return jobCompleted;
    } catch (error) {
        logger.error(`Error completing job ${jobId}:`, { error: error.message, stack: error.stack });
        return false;
    }
}

// Utils
function removeDuplicateRecords(records) {
    const seenRecords = new Set();
    return records.filter(record => {
        try {
            const newImage = record.dynamodb.NewImage;
            const status = newImage.Status.S;
            const jobId = newImage.JobId.S;

            if (status === "COMPLETED") {
                const uniqueKey = `${status}-${jobId}`;
                if (seenRecords.has(uniqueKey)) {
                    return false;
                } else {
                    seenRecords.add(uniqueKey);
                    return true;
                }
            }
        } catch (error) {
            console.warn('Error processing record:', error);
            // If any key is missing, skip the record
            return false;
        }
    });
}

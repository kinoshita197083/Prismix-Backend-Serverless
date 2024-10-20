const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

const snsClient = new SNSClient();
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

exports.handler = async (event) => {
    logger.info('Job completion checker started', event);

    for (const record of event.Records) {
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
                        await publishToSNS(jobId);
                        await completeJob(jobId);
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

    const updateCommand = new UpdateItemCommand({
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET JobCompleted = :completed',
        ExpressionAttributeValues: { ':completed': { BOOL: true } }
    });

    try {
        const result = await docClient.send(updateCommand);
        logger.info(`Job ${jobId} completed successfully`, { result });
    } catch (error) {
        logger.error(`Error completing job ${jobId}:`, { error: error.message });
        throw error;
    }
}

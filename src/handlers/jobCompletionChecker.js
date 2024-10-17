const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const logger = require('../utils/logger');

const snsClient = new SNSClient();

exports.handler = async (event) => {
    logger.info('Job completion checker started', { event: JSON.stringify(event) });

    for (const record of event.Records) {
        if (record.eventName === 'MODIFY') {
            const newImage = record.dynamodb.NewImage;
            const oldImage = record.dynamodb.OldImage;
            const jobId = newImage.JobId.S;
            const userId = oldImage.UserId?.S;

            console.log('----> userId: ', userId);

            // Check if Status field exists in the new image
            if (newImage.Status && newImage.Status.S) {
                const status = newImage.Status.S;

                logger.info(`Processing job ${jobId} with status ${status}`);

                if (status === 'COMPLETED') {
                    logger.info(`Job ${jobId} is completed. Initiating post-processing.`);
                    try {
                        await publishToSNS(jobId);
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

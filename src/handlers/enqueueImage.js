const s3Service = require('../services/s3Service');
const logger = require('../utils/logger');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const sns = new SNSClient();

exports.handler = async (event) => {
    const { Records } = event;

    // console.log('----> Event: ', event);
    logger.info('Enqueue image event:', event);

    for (const record of Records) {
        const { bucket, object } = record.s3;

        // Get object metadata
        const metadata = await s3Service.getObjectMetadata(bucket.name, object.key);

        // Check if this is an update or new creation
        // Skip if this is a processed image
        if (metadata.resized || metadata.resized === "true") {
            logger.info(`Object ${object.key} is already processed - skipping`);
            continue;
        }

        console.log(`Processing image ${object.key} from bucket ${bucket.name} and record: `, record.s3);

        try {
            // Extract userId, projectId, and jobId from the object key
            // Key format: uploads/${userId}/${projectId}/${projectSettingId}/${jobId}/${Date.now()}-${i + index}
            const [type, userId, projectId, projectSettingId, jobId, imageId] = object.key.split('/');
            logger.info(`type: ${type}, userId: ${userId}, projectId: ${projectId}, projectSettingId: ${projectSettingId}, jobId: ${jobId}`);

            if (!jobId || !projectSettingId) {
                throw new Error(`Job or project settings not found for key: ${object.key}`);
            }

            const message = {
                bucket: bucket.name,
                key: object.key,
                userId,
                projectId,
                jobId,
                projectSettingId
            };

            const publishCommand = new PublishCommand({
                TopicArn: process.env.SNS_TOPIC_ARN,
                Message: JSON.stringify(message),
            });

            await sns.send(publishCommand);
            logger.info(`Successfully published message to SNS: ${object.key}`);
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket: bucket.name,
                key: object.key
            });
        }
    }
};
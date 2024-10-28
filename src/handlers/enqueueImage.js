const s3Service = require('../services/s3Service');
const logger = require('../utils/logger');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const sns = new SNSClient();

exports.handler = async (event) => {
    const { Records } = event;

    console.log('----> Event: ', event);

    for (const record of Records) {
        const { bucket, object } = record.s3;
        const eventName = record.eventName; // Get the event name

        console.log(`Processing image ${object.key} from bucket ${bucket.name} and record: `, record.s3);
        console.log(`Event type: ${eventName}`);

        // Check if this is an update or new creation
        // eventName will be something like "ObjectCreated:Put" or "ObjectCreated:CompleteMultipartUpload"
        const isUpdate = await s3Service.isObjectUpdate(bucket.name, object.key, eventName);

        if (isUpdate) {
            logger.info(`Object ${object.key} is being updated - skipping processing`);
            continue; // Skip processing if it's an update
        }

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
                settingValue: projectSetting.settingValue
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
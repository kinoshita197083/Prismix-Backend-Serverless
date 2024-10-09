const logger = require('../utils/logger');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { PrismaClient } = require('@prisma/client');

const sns = new SNSClient();
const prisma = new PrismaClient();

exports.handler = async (event) => {
    const { Records } = event;

    console.log('----> Event: ', event);

    for (const record of Records) {
        const { bucket, object } = record.s3;
        console.log(`Processing image ${object.key} from bucket ${bucket.name} and record: `, record.s3);

        try {
            // Extract userId, projectId, and jobId from the object key
            // Key format: uploads/${userId}/${projectId}/${projectSettingId}/${jobId}/${Date.now()}-${i + index}
            const [type, userId, projectId, projectSettingId, jobId, imageId] = object.key.split('/');
            logger.info(`type: ${type}, userId: ${userId}, projectId: ${projectId}, projectSettingId: ${projectSettingId}, jobId: ${jobId}`);

            if (!jobId || !projectSettingId) {
                throw new Error(`Job or project settings not found for key: ${object.key}`);
            }

            // Fetch the project settings
            const projectSetting = await prisma.projectSetting.findUnique({
                where: { id: projectSettingId },
                select: { settingValue: true }
            });

            logger.info('----> Project setting: ', { projectSetting });

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
                Message: message,
            });

            // Publish the message to SNS
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
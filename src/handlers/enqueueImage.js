const AWS = require('aws-sdk');
const logger = require('../utils/logger');
const sns = new AWS.SNS();

exports.handler = async (event) => {
    const { Records } = event;

    for (const record of Records) {
        const { bucket, object } = record.s3;
        console.log(`Processing image ${object.key} from bucket ${bucket.name} and record: `, record.s3);

        try {
            // Extract userId, projectId, and jobId from the object key
            // Key format: uploads/${userId}/${projectId}/${jobId}/${Date.now()}-${i + index}
            const [type, userId, projectId, projectSettingId, jobId, imageId] = object.key.split('/');
            console.log(`type: ${type}, userId: ${userId}, projectId: ${projectId}, projectSettingId: ${projectSettingId}, jobId: ${jobId}`);

            if (!job) {
                throw new Error(`Job not found for key: ${object.key}`);
            }

            // Fetch the project settings
            const projectSetting = await prisma.projectSetting.findUnique({
                where: { id: job.projectSettingId },
                select: { settingValue: true }
            });

            if (!projectSetting) {
                throw new Error(`Project settings not found for job: ${jobId}`);
            }

            // Prepare the message for SNS
            const message = JSON.stringify({
                bucket: bucket.name,
                key: object.key,
                userId,
                projectId,
                jobId,
                settingValue: projectSetting.settingValue
            });

            // Publish the message to SNS
            await sns.publish({
                TopicArn: process.env.SNS_TOPIC_ARN,
                Message: message,
            }).promise();

            console.log(`Successfully published message to SNS: ${object.key}`);
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket: bucket.name,
                key: object.key,
                projectId,
                jobId,
                imageId
            });
        }
    }
};
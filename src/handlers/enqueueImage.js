const logger = require('../utils/logger');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { createClient } = require('@supabase/supabase-js')

const sns = new SNSClient();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

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
            const projectSetting = await fetchProjectSetting(projectSettingId)

            console.log('----> Project setting: ', projectSetting);

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
                Message: JSON.stringify(message),
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

async function fetchProjectSetting(projectSettingId) {
    try {
        const { data, error } = await supabase
            .from('ProjectSetting')
            .select('settingValue, settingType, settingName')
            .eq('id', projectSettingId)
            .single();

        if (error) throw error;

        if (!data) {
            throw new Error(`No project setting found with id: ${projectSettingId}`);
        }

        console.log('Fetched project setting:', data);

        return {
            settingValue: data.settingValue,
            settingType: data.settingType,
            settingName: data.settingName
        };
    } catch (error) {
        console.error('Error fetching project setting:', error);
        throw error;
    }
}
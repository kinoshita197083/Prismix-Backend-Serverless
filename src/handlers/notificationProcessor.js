const { createClient } = require('@supabase/supabase-js');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const EmailTemplateService = require('../services/EmailTemplateService');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const ses = new SESClient();

const processNotification = async (message) => {
    const { jobId, status, additionalData = {} } = message;

    try {
        const jobData = await fetchDataFromJobProgress(jobId);
        if (!jobData || jobData.EmailSent) {
            logger.info(`Skipping notification for job ${jobId}`);
            return;
        }

        const userData = await fetchUserData(jobData.userId);
        if (!userData) {
            throw new Error(`No user data found for ID: ${jobData.userId}`);
        }

        await sendEmailNotification(userData, jobId, status, additionalData, jobData.projectId);

        if (['COMPLETED', 'FAILED'].includes(status)) {
            await updateEmailSent(jobId);
        }

        logger.info(`Notification sent successfully for job ${jobId}`);
    } catch (error) {
        logger.error('Error processing notification:', { error, jobId });
        throw error;
    }
};

const sendEmailNotification = async (user, jobId, status, additionalData, projectId) => {
    const template = EmailTemplateService.getEmailTemplate(status);
    const userName = user.preferredName || user.name || user.email;
    const content = template.getContent(userName, jobId, additionalData, projectId);

    const params = {
        Destination: { ToAddresses: [user.email] },
        Message: {
            Body: {
                Html: { Charset: "UTF-8", Data: content.html },
                Text: { Charset: "UTF-8", Data: content.text }
            },
            Subject: { Charset: "UTF-8", Data: template.subject }
        },
        Source: process.env.SENDER_EMAIL_ADDRESS
    };

    await ses.send(new SendEmailCommand(params));
};

exports.handler = async (event) => {
    logger.info('Notification processor event:', { event });

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        await processNotification(message);
    }
};

async function updateEmailSent(jobId) {
    logger.info(`Updating email sent status for job ${jobId}`);
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET EmailSent = :emailSent',
        ExpressionAttributeValues: { ':emailSent': true }
    };

    try {
        await docClient.send(new UpdateCommand(params));
        logger.info(`Email sent status updated for job ${jobId}`);
    } catch (error) {
        console.log('----> Update Email sent error: ', error);
        logger.error(`Error updating email sent status for job ${jobId}:`, { error });
        throw error;
    }
}

async function fetchDataFromJobProgress(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId }
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);
        return result.Item ? result.Item : null;
    } catch (error) {
        logger.error('Error fetching userId from JobProgress:', { error, jobId });
        throw error;
    }
}

async function fetchUserData(userId) {
    const { data: user, error } = await supabase
        .from('User')
        .select('name, email')
        .eq('id', userId)
        .single();

    if (error) {
        logger.error('Error fetching user data:', { error, userId });
        throw error;
    }

    return user;
}

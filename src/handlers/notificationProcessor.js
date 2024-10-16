const { createClient } = require('@supabase/supabase-js');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const ses = new SESClient();

exports.handler = async (event) => {
    logger.info('Notification processor event:', { event });

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        const jobId = message.jobId;
        const userId = message.userId; // Assuming the userId is included in the message

        logger.info(`Processing notification for job ${jobId} and user ${userId}`);

        try {
            // Fetch userId from JobProgress table
            const userId = await fetchUserIdFromJobProgress(jobId);
            console.log('----> userId: ', userId);

            // Retrieve user email from Supabase
            const { data: user, error } = await supabase
                .from('User')
                .select('name, email')
                .eq('id', userId)
                .single();

            if (error) {
                logger.error('Error fetching user data:', { error, jobId, userId });
                throw new Error(`Error fetching user data: ${error.message}`);
            }

            if (!user || !user.email) {
                logger.error('No email found for user', { jobId, userId });
                throw new Error(`No email found for user ${userId}`);
            }

            console.log('User information retrieved successfully:', user);

            // Send email notification
            await sendEmailNotification(user.email, jobId, user.name);

            logger.info(`Notification sent for job ${jobId} to user ${userId}`);
        } catch (error) {
            logger.error('Error processing notification:', { error, jobId, userId });
        }
    }
};

async function sendEmailNotification(email, jobId, name) {
    const params = {
        Destination: {
            ToAddresses: [email],
        },
        Message: {
            Body: {
                Text: {
                    Charset: "UTF-8",
                    Data: `Hello ${name}, your job ${jobId} has been completed successfully.`
                }
            },
            Subject: {
                Charset: "UTF-8",
                Data: "Job Completion Notification"
            }
        },
        Source: process.env.SENDER_EMAIL_ADDRESS,
    };

    try {
        const command = new SendEmailCommand(params);
        await ses.send(command);
        logger.info(`Email sent successfully to ${email} for job ${jobId}`);
    } catch (error) {
        logger.error('Error sending email:', { error, email, jobId });
        throw error;
    }
}

async function fetchUserIdFromJobProgress(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'UserId'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);
        return result.Item ? result.Item.UserId : null;
    } catch (error) {
        logger.error('Error fetching userId from JobProgress:', { error, jobId });
        throw error;
    }
}
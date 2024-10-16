const { createClient } = require('@supabase/supabase-js');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const logger = require('../utils/logger');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);
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
            // Retrieve user email from Supabase
            const { data: user, error } = await supabase
                .from('users')
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

            // Send email notification
            await sendEmailNotification(user.email, jobId);

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

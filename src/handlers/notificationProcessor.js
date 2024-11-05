const { createClient } = require('@supabase/supabase-js');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const ses = new SESClient();

const EMAIL_TEMPLATES = {
    COMPLETED: {
        subject: "Your Job Has Been Completed - Prismix Notification",
        getContent: (name, jobId) => ({
            html: `
                <html>
                    <head>
                        <style>
                            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                            .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                            .header { background-color: #4CAF50; color: white; padding: 10px; text-align: center; }
                            .content { padding: 20px; background-color: #f9f9f9; }
                            .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <div class="header">
                                <h1>Job Completion Notification</h1>
                            </div>
                            <div class="content">
                                <p>Dear ${name},</p>
                                <p>We are pleased to inform you that your job has been completed successfully.</p>
                                <p><strong>Job ID:</strong> ${jobId}</p>
                                <p>You can now access and review the results of your job through our platform. If you have any questions or need further assistance, please don't hesitate to contact our support team.</p>
                                <p>Thank you for using our services.</p>
                                <p>Best regards,<br>The Prismix Team</p>
                            </div>
                            <div class="footer">
                                <p>This is an automated message. Please do not reply to this email.</p>
                            </div>
                        </div>
                    </body>
                </html>
            `,
            text: `
Dear ${name},

We are pleased to inform you that your job has been completed successfully.

Job ID: ${jobId}

You can now access and review the results of your job through our platform. If you have any questions or need further assistance, please don't hesitate to contact our support team.

Thank you for using our services.

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
            `
        })
    },
    WAITING_FOR_REVIEW: {
        subject: "Your Job Requires Manual Review - Prismix Notification",
        getContent: (name, jobId) => ({
            html: `
                <html>
                    <head>
                        <style>
                            body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                            .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                            .header { background-color: #FFA500; color: white; padding: 10px; text-align: center; }
                            .content { padding: 20px; background-color: #f9f9f9; }
                            .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <div class="header">
                                <h1>Manual Review Required</h1>
                            </div>
                            <div class="content">
                                <p>Dear ${name},</p>
                                <p>Your job has been processed and requires manual review before completion.</p>
                                <p><strong>Job ID:</strong> ${jobId}</p>
                                <p>Our team has identified some aspects of your job that need additional attention to ensure the highest quality results. Our review team will examine your job carefully and process it as soon as possible.</p>
                                <p>You will receive another notification once the review is complete and your job is finalized.</p>
                                <p>If you have any questions about this process, please don't hesitate to reach out to our support team.</p>
                                <p>Thank you for your patience and understanding.</p>
                                <p>Best regards,<br>The Prismix Team</p>
                            </div>
                            <div class="footer">
                                <p>This is an automated message. Please do not reply to this email.</p>
                            </div>
                        </div>
                    </body>
                </html>
            `,
            text: `
Dear ${name},

Your job has been processed and requires manual review before completion.

Job ID: ${jobId}

Our team has identified some aspects of your job that need additional attention to ensure the highest quality results. Our review team will examine your job carefully and process it as soon as possible.

You will receive another notification once the review is complete and your job is finalized.

If you have any questions about this process, please don't hesitate to reach out to our support team.

Thank you for your patience and understanding.

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
            `
        })
    }
};

exports.handler = async (event) => {
    logger.info('Notification processor event:', { event });

    // const uniqueRecords = removeDuplicateRecords(event.Records);

    console.log('Original records: ', event.Records.length);
    // console.log('Unique records: ', uniqueRecords.length);

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        const jobId = message.jobId;
        const status = message.status;

        try {
            // Fetch userId from JobProgress table
            const data = await fetchDataFromJobProgress(jobId);
            const userId = data.userId; // Ensure userId is defined here
            const isEmailSent = data.EmailSent;
            console.log('----> data: ', data);
            console.log('----> isEmailSent: ', isEmailSent);

            console.log('userId: ', userId);
            console.log('jobId: ', jobId);

            logger.info(`Processing notification for job ${jobId} and user ${userId}`);

            if (isEmailSent) {
                logger.info(`Email already sent for job ${jobId}. Skipping notification.`);
                return;
            }

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
            await sendEmailNotification(user.email, user.name, jobId, status);

            // Update EmailSent to true
            await updateEmailSent(jobId);

            logger.info(`Notification sent for job ${jobId} to user ${userId}`);
        } catch (error) {
            logger.error('Error processing notification:', { error, jobId });
        }
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

async function sendEmailNotification(email, name, jobId, status) {
    if (!EMAIL_TEMPLATES[status]) {
        logger.error('Invalid email status template:', { status, jobId });
        throw new Error(`No email template found for status: ${status}`);
    }

    const template = EMAIL_TEMPLATES[status];
    const content = template.getContent(name, jobId);

    const params = {
        Destination: {
            ToAddresses: [email],
        },
        Message: {
            Body: {
                Html: {
                    Charset: "UTF-8",
                    Data: content.html
                },
                Text: {
                    Charset: "UTF-8",
                    Data: content.text
                }
            },
            Subject: {
                Charset: "UTF-8",
                Data: template.subject
            }
        },
        Source: process.env.SENDER_EMAIL_ADDRESS,
    };

    try {
        const command = new SendEmailCommand(params);
        await ses.send(command);
        logger.info(`Email sent successfully to ${email} for job ${jobId} with status ${status}`);
    } catch (error) {
        logger.error('Error sending email:', { error, email, jobId, status });
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

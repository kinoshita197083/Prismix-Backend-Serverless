const EMAIL_TEMPLATES = {
    COMPLETED: {
        subject: "Your Job Has Been Completed - Prismix Notification",
        getContent: (name, jobId) => ({
            html: generateCompletedEmailHtml(name, jobId),
            text: generateCompletedEmailText(name, jobId)
        })
    },
    WAITING_FOR_REVIEW: {
        subject: "Your Job Requires Manual Review - Prismix Notification",
        getContent: (name, jobId) => ({
            html: generateReviewEmailHtml(name, jobId),
            text: generateReviewEmailText(name, jobId)
        })
    },
    REVIEW_EXTENDED: {
        subject: "Job Review Period Extended - Prismix Notification",
        getContent: (name, jobId, extendedUntil) => ({
            html: generateReviewExtendedHtml(name, jobId, extendedUntil),
            text: generateReviewExtendedText(name, jobId, extendedUntil)
        })
    },
    FAILED: {
        subject: "Job Processing Failed - Prismix Notification",
        getContent: (name, jobId, error) => ({
            html: generateFailedEmailHtml(name, jobId, error),
            text: generateFailedEmailText(name, jobId, error)
        })
    }
};

// Completed Job Email
const generateCompletedEmailHtml = (name, jobId) => `
    <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
                .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #4CAF50; color: white; padding: 20px; text-align: center; border-radius: 4px 4px 0 0; }
                .content { padding: 30px; background-color: #f9f9f9; border: 1px solid #ddd; border-top: none; border-radius: 0 0 4px 4px; }
                .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                .button { display: inline-block; padding: 12px 24px; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 4px; margin-top: 20px; }
                .job-id { background-color: #f0f0f0; padding: 8px 12px; border-radius: 4px; font-family: monospace; }
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
                    <p><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    <p>You can now access and review the results of your job through our platform. If you have any questions or need further assistance, please don't hesitate to contact our support team.</p>
                    <a href="${process.env.DASHBOARD_URL}/jobs/${jobId}" class="button">View Job Results</a>
                    <p>Thank you for using our services.</p>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
    </html>
`;

const generateCompletedEmailText = (name, jobId) => `
Dear ${name},

We are pleased to inform you that your job has been completed successfully.

Job ID: ${jobId}

You can now access and review the results of your job through our platform. If you have any questions or need further assistance, please don't hesitate to contact our support team.

To view your job results, visit: ${process.env.DASHBOARD_URL}/jobs/${jobId}

Thank you for using our services.

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Review Required Email
const generateReviewEmailHtml = (name, jobId) => `
    <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
                .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #FF9800; color: white; padding: 20px; text-align: center; border-radius: 4px 4px 0 0; }
                .content { padding: 30px; background-color: #f9f9f9; border: 1px solid #ddd; border-top: none; border-radius: 0 0 4px 4px; }
                .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                .button { display: inline-block; padding: 12px 24px; background-color: #FF9800; color: white; text-decoration: none; border-radius: 4px; margin-top: 20px; }
                .job-id { background-color: #f0f0f0; padding: 8px 12px; border-radius: 4px; font-family: monospace; }
                .warning { color: #FF9800; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Manual Review Required</h1>
                </div>
                <div class="content">
                    <p>Dear ${name},</p>
                    <p>Your job requires manual review before it can be completed.</p>
                    <p><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    <p class="warning">Please review and approve the processed items within the next 48 hours.</p>
                    <p>If no action is taken within this timeframe, the system will automatically process the remaining items according to default settings.</p>
                    <a href="${process.env.DASHBOARD_URL}/jobs/${jobId}/review" class="button">Review Now</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
    </html>
`;

const generateReviewEmailText = (name, jobId) => `
Dear ${name},

Your job requires manual review before it can be completed.

Job ID: ${jobId}

Please review and approve the processed items within the next 48 hours.
If no action is taken within this timeframe, the system will automatically process the remaining items according to default settings.

To review your job, visit: ${process.env.DASHBOARD_URL}/jobs/${jobId}/review

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Review Period Extended Email
const generateReviewExtendedHtml = (name, jobId, extendedUntil) => `
    <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
                .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #2196F3; color: white; padding: 20px; text-align: center; border-radius: 4px 4px 0 0; }
                .content { padding: 30px; background-color: #f9f9f9; border: 1px solid #ddd; border-top: none; border-radius: 0 0 4px 4px; }
                .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                .button { display: inline-block; padding: 12px 24px; background-color: #2196F3; color: white; text-decoration: none; border-radius: 4px; margin-top: 20px; }
                .job-id { background-color: #f0f0f0; padding: 8px 12px; border-radius: 4px; font-family: monospace; }
                .deadline { color: #2196F3; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Review Period Extended</h1>
                </div>
                <div class="content">
                    <p>Dear ${name},</p>
                    <p>The review period for your job has been extended.</p>
                    <p><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    <p>New deadline: <span class="deadline">${new Date(extendedUntil).toLocaleString()}</span></p>
                    <p>Please complete your review before the new deadline. After this time, remaining items will be processed automatically.</p>
                    <a href="${process.env.DASHBOARD_URL}/jobs/${jobId}/review" class="button">Continue Review</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
    </html>
`;

const generateReviewExtendedText = (name, jobId, extendedUntil) => `
Dear ${name},

The review period for your job has been extended.

Job ID: ${jobId}
New deadline: ${new Date(extendedUntil).toLocaleString()}

Please complete your review before the new deadline. After this time, remaining items will be processed automatically.

To continue your review, visit: ${process.env.DASHBOARD_URL}/jobs/${jobId}/review

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Failed Job Email
const generateFailedEmailHtml = (name, jobId, error) => `
    <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }
                .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                .header { background-color: #F44336; color: white; padding: 20px; text-align: center; border-radius: 4px 4px 0 0; }
                .content { padding: 30px; background-color: #f9f9f9; border: 1px solid #ddd; border-top: none; border-radius: 0 0 4px 4px; }
                .footer { text-align: center; margin-top: 20px; font-size: 0.8em; color: #777; }
                .button { display: inline-block; padding: 12px 24px; background-color: #F44336; color: white; text-decoration: none; border-radius: 4px; margin-top: 20px; }
                .job-id { background-color: #f0f0f0; padding: 8px 12px; border-radius: 4px; font-family: monospace; }
                .error-details { background-color: #ffebee; padding: 15px; border-radius: 4px; margin: 15px 0; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Job Processing Failed</h1>
                </div>
                <div class="content">
                    <p>Dear ${name},</p>
                    <p>We regret to inform you that your job has encountered an error during processing.</p>
                    <p><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    <div class="error-details">
                        <p><strong>Error Details:</strong></p>
                        <p>${error.message || 'An unexpected error occurred'}</p>
                        ${error.details ? `<p><strong>Additional Information:</strong> ${error.details}</p>` : ''}
                    </div>
                    <p>Our technical team has been notified and will investigate the issue. You may need to resubmit your job once the issue is resolved.</p>
                    <a href="${process.env.DASHBOARD_URL}/jobs/${jobId}" class="button">View Job Details</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated message. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
    </html>
`;

const generateFailedEmailText = (name, jobId, error) => `
Dear ${name},

We regret to inform you that your job has encountered an error during processing.

Job ID: ${jobId}

Error Details:
${error.message || 'An unexpected error occurred'}
${error.details ? `Additional Information: ${error.details}` : ''}

Our technical team has been notified and will investigate the issue. You may need to resubmit your job once the issue is resolved.

To view your job details, visit: ${process.env.DASHBOARD_URL}/jobs/${jobId}

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

module.exports = {
    getEmailTemplate: (type, params) => {
        const template = EMAIL_TEMPLATES[type];
        if (!template) {
            throw new Error(`No email template found for type: ${type}`);
        }
        return template;
    }
}; 
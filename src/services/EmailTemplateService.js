const EMAIL_TEMPLATES = {
    COMPLETED: {
        subject: "Your Job Has Been Completed - Prismix Notification",
        getContent: (name, jobId, projectId) => ({
            html: generateCompletedEmailHtml(name, jobId, projectId),
            text: generateCompletedEmailText(name, jobId, projectId)
        })
    },
    WAITING_FOR_REVIEW: {
        subject: "Your Job Requires Manual Review - Prismix Notification",
        getContent: (name, jobId, projectId) => ({
            html: generateReviewEmailHtml(name, jobId, projectId),
            text: generateReviewEmailText(name, jobId, projectId)
        })
    },
    REVIEW_EXTENDED: {
        subject: "Job Review Period Extended - Prismix Notification",
        getContent: (name, jobId, extendedUntil, projectId) => ({
            html: generateReviewExtendedHtml(name, jobId, extendedUntil, projectId),
            text: generateReviewExtendedText(name, jobId, extendedUntil, projectId)
        })
    },
    FAILED: {
        subject: "Job Processing Failed - Prismix Notification",
        getContent: (name, jobId, error, projectId) => ({
            html: generateFailedEmailHtml(name, jobId, error, projectId),
            text: generateFailedEmailText(name, jobId, error, projectId)
        })
    }
};

// Common header component
const getEmailHeader = (title) => `
    <div class="header">
        <img src="https://anythingfrenkie.s3.ap-southeast-2.amazonaws.com/logo_black.svg" alt="Prismix" class="logo" />
        <h1>${title}</h1>
    </div>
`;

// Common footer component
const getEmailFooter = () => `
    <div class="footer">
        <div class="footer-links">
            <a href="#">Privacy Policy</a> • 
            <a href="#">Terms of Service</a> • 
            <a href="#">Contact Support</a>
        </div>
        <p class="footer-copyright">© ${new Date().getFullYear()} Prismix. All rights reserved.</p>
        <p class="footer-note">This is an automated message. Please do not reply to this email.</p>
    </div>
`;

const getEmailStyles = () => `
    body { 
        font-family: Arial, sans-serif; 
        line-height: 1.6; 
        color: #1C1C1C; 
        margin: 0; 
        padding: 0; 
        background-color: #F6F6F6;
    }
    .container { 
        max-width: 640px; 
        margin: 32px auto; 
        background-color: #FFFFFF;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    .header { 
        background-color: #121212; 
        color: #FFFFFF; 
        padding: 32px 40px; 
        text-align: left;
    }
    .logo {
        height: 32px;
        margin-bottom: 24px;
        display: block;
    }
    .content { 
        padding: 40px;
        background-color: #FFFFFF;
    }
    .footer { 
        text-align: center; 
        padding: 32px 40px;
        font-size: 12px; 
        color: #666666;
        background-color: #F9F9F9;
        border-top: 1px solid #E0E0E0;
    }
    .footer-links {
        margin-bottom: 16px;
    }
    .footer-links a {
        color: #666666;
        text-decoration: none;
    }
    .footer-links a:hover {
        text-decoration: underline;
    }
    .footer-copyright {
        margin: 8px 0;
    }
    .footer-note {
        color: #999999;
        margin: 8px 0;
    }
    .job-id { 
        background-color: #F5F5F5; 
        padding: 8px 12px; 
        border-radius: 3px; 
        font-family: 'Courier New', monospace;
        font-size: 14px;
        color: #333333;
        border: 1px solid #E0E0E0;
        display: inline-block;
    }
    .status-box {
        padding: 24px;
        background-color: #F9F9F9;
        border-radius: 4px;
        margin: 24px 0;
    }
    h1 { 
        font-size: 24px; 
        font-weight: 700; 
        margin: 0; 
        letter-spacing: -0.5px;
    }
    p { 
        margin: 16px 0; 
        font-size: 15px;
        color: #333333;
    }
    .warning { 
        color: #D32F2F;
        font-weight: 500;
        font-size: 15px;
    }
    .deadline { 
        color: #1C1C1C; 
        font-weight: 600;
        background-color: #FFF9C4;
        padding: 2px 4px;
    }
    .error-details { 
        background-color: #FFEBEE; 
        padding: 24px; 
        border-radius: 4px; 
        margin: 24px 0;
        border-left: 4px solid #EF5350;
    }
    .button {
        display: inline-block;
        padding: 12px 24px;
        background-color: #121212;
        color: #FFFFFF;
        text-decoration: none;
        border-radius: 4px;
        margin: 16px 0;
        font-weight: 500;
    }
    .button:hover {
        background-color: #2C2C2C;
    }
`;

// Completed Job Email
const generateCompletedEmailHtml = (name, jobId) => `
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>${getEmailStyles()}</style>
        </head>
        <body>
            <div class="container">
                ${getEmailHeader('Job Completion Notification')}
                <div class="content">
                    <p>Dear ${name},</p>
                    <div class="status-box">
                        <p style="margin: 0;"><strong>Status:</strong> Completed</p>
                        <p style="margin: 8px 0 0;"><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    </div>
                    <p>Your job has been completed successfully. You can now access and review the results through our platform.</p>
                    <a href="#" class="button">View Results</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                ${getEmailFooter()}
            </div>
        </body>
    </html>
`;

const generateCompletedEmailText = (name, jobId) => `
Dear ${name},

Your job has been completed successfully.

Job ID: ${jobId}

You can now access and review the results of your job through our platform.

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Review Required Email
const generateReviewEmailHtml = (name, jobId) => `
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>${getEmailStyles()}</style>
        </head>
        <body>
            <div class="container">
                ${getEmailHeader('Manual Review Required')}
                <div class="content">
                    <p>Dear ${name},</p>
                    <div class="status-box">
                        <p style="margin: 0;"><strong>Status:</strong> Awaiting Review</p>
                        <p style="margin: 8px 0 0;"><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    </div>
                    <p>Your job requires manual review before it can be completed.</p>
                    <p class="warning">⚠️ Please review and approve the processed items within the next 48 hours.</p>
                    <p>If no action is taken within this timeframe, the system will automatically process the remaining items according to default settings.</p>
                    <a href="#" class="button">Review Now</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                ${getEmailFooter()}
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

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Review Period Extended Email
const generateReviewExtendedHtml = (name, jobId, extendedUntil, projectId) => `
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>${getEmailStyles()}</style>
        </head>
        <body>
            <div class="container">
                ${getEmailHeader('Review Period Extended')}
                <div class="content">
                    <p>Dear ${name},</p>
                    <div class="status-box">
                        <p style="margin: 0;"><strong>Status:</strong> Review Extended</p>
                        <p style="margin: 8px 0 0;"><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                        <p style="margin: 8px 0 0;"><strong>New Deadline:</strong> <span class="deadline">${new Date(+extendedUntil).toLocaleString()}</span></p>
                    </div>
                    <p>The review period for your job has been extended. Please complete your review before the new deadline.</p>
                    <p>After this time, remaining items will be processed automatically.</p>
                    <a href="${process.env.FRONTEND_URL}/project/${projectId}/job/${jobId}" class="button">Continue Review</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                ${getEmailFooter()}
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

Best regards,
The Prismix Team

This is an automated message. Please do not reply to this email.
`;

// Failed Job Email
const generateFailedEmailHtml = (name, jobId, error) => `
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>${getEmailStyles()}</style>
        </head>
        <body>
            <div class="container">
                ${getEmailHeader('Job Processing Failed')}
                <div class="content">
                    <p>Dear ${name},</p>
                    <div class="status-box">
                        <p style="margin: 0;"><strong>Status:</strong> Failed</p>
                        <p style="margin: 8px 0 0;"><strong>Job ID:</strong> <span class="job-id">${jobId}</span></p>
                    </div>
                    <p>We regret to inform you that your job has encountered an error during processing.</p>
                    <div class="error-details">
                        <p style="margin-top: 0;"><strong>Error Details:</strong></p>
                        <p style="margin-bottom: 0;">${error.message || 'An unexpected error occurred'}</p>
                        ${error.details ? `<p style="margin-top: 8px; margin-bottom: 0;"><strong>Additional Information:</strong> ${error.details}</p>` : ''}
                    </div>
                    <p>Our technical team has been notified and will investigate the issue. You may need to resubmit your job once the issue is resolved.</p>
                    <a href="#" class="button">Contact Support</a>
                    <p>Best regards,<br>The Prismix Team</p>
                </div>
                ${getEmailFooter()}
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
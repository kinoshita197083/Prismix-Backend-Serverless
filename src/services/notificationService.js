const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

class NotificationService {
    constructor(snsClient, topicArn) {
        this.snsClient = snsClient;
        this.topicArn = topicArn;
    }

    async publishJobStatus(jobId, status) {
        console.log('[NotificationService.publishJobStatus] Publishing status:', {
            jobId,
            status,
            topicArn: this.topicArn
        });

        const params = {
            Message: JSON.stringify({ jobId, status }),
            TopicArn: this.topicArn
        };

        try {
            const command = new PublishCommand(params);
            const result = await this.snsClient.send(command);
            console.log('[NotificationService.publishJobStatus] Published successfully:', result);
            return result;
        } catch (error) {
            console.error('[NotificationService.publishJobStatus] Error publishing:', error);
            throw error;
        }
    }
}

module.exports = NotificationService; 
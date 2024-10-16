const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const snsClient = new SNSClient();

exports.handler = async (event) => {
    console.log('Event received:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        if (record.eventName === 'MODIFY') {
            const newImage = record.dynamodb.NewImage;
            const jobId = newImage.JobId.S;
            const userId = newImage.UserId.S;
            const status = newImage.Status.S;

            if (status === 'COMPLETED') {
                console.log(`Job ${jobId} is completed. Publishing to SNS.`);
                await publishToSNS({ jobId, userId });
            }
        }
    }
};

async function publishToSNS({ jobId, userId }) {
    const params = {
        Message: JSON.stringify({ jobId, userId }),
        TopicArn: process.env.JOB_COMPLETION_TOPIC_ARN
    };

    try {
        const command = new PublishCommand(params);
        await snsClient.send(command);
        console.log(`Successfully published job completion for ${jobId} to SNS`);
    } catch (error) {
        console.error('Error publishing to SNS:', error);
        throw error;
    }
}


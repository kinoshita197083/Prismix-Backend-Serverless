const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);
const sqsClient = new SQSClient();

exports.handler = async (event) => {
    console.log('Received event from DLQ:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const messageBody = JSON.parse(record.body);
            console.log('Processing DLQ message:', messageBody);

            // Attempt to resolve the issue
            await attemptToResolveIssue(messageBody);

            // Update DynamoDB to mark the job as retried
            // await updateJobStatus(messageBody.jobId, 'RETRIED_FROM_DLQ');

            // Optionally, send the message back to the original queue for reprocessing
            // await sendToOriginalQueue(messageBody);

            console.log(`Successfully processed DLQ message for job ${messageBody.jobId}`);
        } catch (error) {
            console.error('Error processing DLQ message:', error);
            // Update DynamoDB to mark the job as failed
            // await updateJobStatus(messageBody.jobId, 'FAILED');
            // In a DLQ processor, we typically don't throw errors to avoid infinite loops
        }
    }
};

async function attemptToResolveIssue(messageBody) {
    // Implement your logic to attempt resolving the issue
    // This could involve retrying the operation, applying fixes, etc.
    console.log('Attempting to resolve issue for job:', messageBody.jobId);
    // Simulating some resolution attempt
    await new Promise(resolve => setTimeout(resolve, 1000));
}

async function updateJobStatus(jobId, status) {
    const params = {
        TableName: process.env.DYNAMODB_TABLE,
        Key: {
            PK: `JOB#${jobId}`,
            SK: `METADATA#${jobId}`
        },
        UpdateExpression: 'SET #status = :status, updatedAt = :updatedAt',
        ExpressionAttributeNames: {
            '#status': 'status'
        },
        ExpressionAttributeValues: {
            ':status': status,
            ':updatedAt': new Date().toISOString()
        }
    };

    try {
        await docClient.send(new UpdateCommand(params));
    } catch (error) {
        console.error('Error updating DynamoDB:', error);
        throw error;
    }
}

async function sendToOriginalQueue(messageBody) {
    const params = {
        QueueUrl: process.env.ORIGINAL_QUEUE_URL, // Make sure to set this in your Lambda environment variables
        MessageBody: JSON.stringify(messageBody),
        DelaySeconds: 60 // Add a delay before reprocessing
    };

    try {
        await sqsClient.send(new SendMessageCommand(params));
        console.log(`Message for job ${messageBody.jobId} sent back to original queue for reprocessing`);
    } catch (error) {
        console.error('Error sending message to original queue:', error);
        throw error;
    }
}
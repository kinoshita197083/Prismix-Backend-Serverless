const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const messageBody = JSON.parse(record.body);
            console.log('Processing message:', messageBody);

            // Simulate processing time and potential failure
            await simulateProcessing();

            // Update DynamoDB
            // await updateJobStatus(messageBody.jobId, 'COMPLETED');

            console.log(`Successfully processed message for job ${messageBody.jobId}`);
        } catch (error) {
            console.error('Error processing message:', error);
            // If we don't throw here, the message will be deleted from the queue
            // Throwing will cause the message to be retried or sent to DLQ
            throw error;
        }
    }
};

async function simulateProcessing() {
    const processingTime = Math.random() * 1000; // Random time up to 1 second
    await new Promise(resolve => setTimeout(resolve, processingTime));

    // Simulate occasional failures
    if (Math.random() < 0.1) { // 10% chance of failure
        throw new Error('Random processing failure');
    }
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
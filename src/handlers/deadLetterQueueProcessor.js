const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { SQSClient, DeleteMessageCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');
const logger = require('../utils/logger');

const sqs = new SQSClient();
const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

const MAX_RETRIES = 3;
const INITIAL_BACKOFF = 1000; // 1 second

exports.handler = async (event) => {
    logger.info('Processing Dead Letter Queue messages', { messageCount: event.Records.length });

    for (const record of event.Records) {
        const originalMessage = JSON.parse(record.body);
        const { jobId, taskId } = originalMessage;

        try {
            logger.info('Processing failed message', { jobId, taskId });

            const retryCount = parseInt(record.attributes.ApproximateReceiveCount, 10);

            if (retryCount > MAX_RETRIES) {
                await handleMaxRetriesExceeded(jobId, taskId, originalMessage);
            } else {
                await retryProcessing(jobId, taskId, originalMessage, retryCount);
            }

            // Delete the message from the DLQ after processing
            await deleteMessageFromDLQ(record.receiptHandle);

        } catch (error) {
            logger.error('Error processing DLQ message', { error, jobId, taskId });
        }
    }
};

async function retryProcessing(jobId, taskId, originalMessage, retryCount) {
    const backoffTime = INITIAL_BACKOFF * Math.pow(2, retryCount - 1);
    logger.info('Retrying message processing', { jobId, taskId, retryCount, backoffTime });

    await new Promise(resolve => setTimeout(resolve, backoffTime));

    const sendMessageCommand = new SendMessageCommand({
        QueueUrl: process.env.ORIGINAL_QUEUE_URL,
        MessageBody: JSON.stringify(originalMessage),
        DelaySeconds: 0
    });

    await sqs.send(sendMessageCommand);
    logger.info('Message sent back to original queue for retry', { jobId, taskId, retryCount });

    await updateTaskStatus(jobId, taskId, 'RETRYING', retryCount);
}

async function handleMaxRetriesExceeded(jobId, taskId, originalMessage) {
    logger.warn('Max retries exceeded for message', { jobId, taskId });
    await updateTaskStatus(jobId, taskId, 'FAILED', MAX_RETRIES);
    // Implement notification logic here (e.g., SNS, email)
}

async function updateTaskStatus(jobId, taskId, status, retryCount) {
    const updateCommand = new UpdateCommand({
        TableName: process.env.TASKS_TABLE,
        Key: { JobID: jobId, TaskID: taskId },
        UpdateExpression: 'SET TaskStatus = :status, RetryCount = :retryCount, UpdatedAt = :updatedAt',
        ExpressionAttributeValues: {
            ':status': status,
            ':retryCount': retryCount,
            ':updatedAt': Date.now().toString()
        }
    });

    await docClient.send(updateCommand);
    logger.info('Task status updated', { jobId, taskId, status, retryCount });
}

async function deleteMessageFromDLQ(receiptHandle) {
    const deleteCommand = new DeleteMessageCommand({
        QueueUrl: process.env.DEAD_LETTER_QUEUE_URL,
        ReceiptHandle: receiptHandle
    });

    await sqs.send(deleteCommand);
    logger.info('Message deleted from DLQ', { receiptHandle });
}

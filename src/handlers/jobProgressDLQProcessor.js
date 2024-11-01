const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { SQSClient, DeleteMessageCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');
const logger = require('../utils/logger');
const { ErrorCodes } = require('../utils/errorHandler');

const sqs = new SQSClient();
const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

const MAX_RETRIES = 3;
const INITIAL_BACKOFF = 1000; // 1 second

exports.handler = async (event) => {
    logger.info('Processing Job Progress DLQ messages', { messageCount: event.Records.length });

    for (const record of event.Records) {
        try {
            const dlqMessage = JSON.parse(record.body);
            const originalMessage = JSON.parse(dlqMessage.originalMessage);
            const { jobId } = originalMessage;
            const error = dlqMessage.error;

            logger.info('Processing failed job progress check', { jobId, error });

            // Update job progress with error information
            await updateJobProgressWithError(jobId, error);

            // Determine if we should retry based on error type and retry count
            const retryCount = getRetryCount(record);

            if (shouldRetry(error.code, retryCount)) {
                await retryJobProgressCheck(originalMessage, retryCount);
            } else {
                await handleTerminalFailure(jobId, error);
            }

            // Delete the message from DLQ after processing
            await deleteMessageFromDLQ(record.receiptHandle);

        } catch (error) {
            logger.error('Error processing DLQ message', { error, record });
            // Don't throw here to continue processing other messages
        }
    }
};

async function updateJobProgressWithError(jobId, error) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET processingErrors = list_append(if_not_exists(processingErrors, :empty), :error)',
        ExpressionAttributeValues: {
            ':error': [{
                timestamp: new Date().toISOString(),
                error: error.message,
                code: error.code
            }],
            ':empty': []
        }
    };

    try {
        await docClient.send(new UpdateCommand(params));
    } catch (error) {
        logger.error('Failed to update job progress with error', { error, jobId });
    }
}

async function retryJobProgressCheck(originalMessage, retryCount) {
    const backoff = INITIAL_BACKOFF * Math.pow(2, retryCount);
    const delaySeconds = Math.min(Math.floor(backoff / 1000), 900); // Max 15 minutes

    const params = {
        QueueUrl: process.env.JOB_PROGRESS_QUEUE_URL,
        MessageBody: JSON.stringify(originalMessage),
        DelaySeconds: delaySeconds,
        MessageAttributes: {
            RetryCount: {
                DataType: 'Number',
                StringValue: (retryCount + 1).toString()
            }
        }
    };

    await sqs.send(new SendMessageCommand(params));
    logger.info('Scheduled retry for job progress check', {
        jobId: originalMessage.jobId,
        retryCount: retryCount + 1,
        delaySeconds
    });
}

async function handleTerminalFailure(jobId, error) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET #status = :failed, terminalError = :error',
        ExpressionAttributeNames: {
            '#status': 'status'
        },
        ExpressionAttributeValues: {
            ':failed': 'FAILED',
            ':error': {
                message: error.message,
                code: error.code,
                timestamp: new Date().toISOString()
            }
        }
    };

    try {
        await docClient.send(new UpdateCommand(params));
        logger.info('Updated job status to FAILED', { jobId, error });
    } catch (updateError) {
        logger.error('Failed to update job status for terminal failure', {
            error: updateError,
            jobId
        });
    }
}

async function deleteMessageFromDLQ(receiptHandle) {
    await sqs.send(new DeleteMessageCommand({
        QueueUrl: process.env.JOB_PROGRESS_DLQ_URL,
        ReceiptHandle: receiptHandle
    }));
}

function getRetryCount(record) {
    return parseInt(record.attributes.ApproximateReceiveCount, 10) - 1;
}

function shouldRetry(errorCode, retryCount) {
    const RETRYABLE_ERRORS = [
        ErrorCodes.DATABASE.DYNAMO_UPDATE_ERROR,
        'TemporaryFailure',
        'ServiceUnavailable'
    ];

    return RETRYABLE_ERRORS.includes(errorCode) && retryCount < MAX_RETRIES;
} 
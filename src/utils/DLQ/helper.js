const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const sqs = new SQSClient();

const sendToDLQ = async ({ queueUrl, message, error, logger }) => {
    try {
        const dlqMessage = {
            originalMessage: JSON.stringify(message),
            error: {
                message: error.message,
                stack: error.stack,
                timestamp: new Date().toISOString()
            }
        };

        await sqs.send(new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(dlqMessage),
            MessageAttributes: {
                ErrorType: {
                    DataType: 'String',
                    StringValue: error.name || 'ProcessingError'
                },
                OriginalMessageId: {
                    DataType: 'String',
                    StringValue: message.MessageId
                }
            }
        }));

        logger.info('Message sent to DLQ', {
            messageId: message.MessageId,
            error: error.message
        });
    } catch (dlqError) {
        logger.error('Failed to send message to DLQ', {
            originalError: error.message,
            dlqError: dlqError.message,
            messageId: message.MessageId
        });
    }
};

module.exports = {
    sendToDLQ
};
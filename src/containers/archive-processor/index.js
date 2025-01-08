const { Consumer } = require('sqs-consumer');
const { zipArchiveProcessor } = require('../../handlers/zipArchiveProcessor');
const logger = require('../../utils/logger');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.ARCHIVE_QUEUE_URL,
    handleMessage: async (message) => {
        // Transform the message into Lambda-style event
        const event = {
            Records: [{
                ...message,
            }]
        };

        logger.info('Processing archive message', {
            messageId: message.MessageId,
            timestamp: new Date().toISOString()
        });

        await zipArchiveProcessor(event);
    },
    batchSize: 1, // Process one archive job at a time
    visibilityTimeout: 7200, // 2 hours
    messageAttributeNames: ['All']
});

app.on('error', (err) => {
    logger.error('Consumer error', {
        error: err.message,
        stack: err.stack
    });
});

app.on('processing_error', (err) => {
    logger.error('Processing error', {
        error: err.message,
        stack: err.stack
    });
});

app.on('started', () => {
    logger.info('Archive processor started');
});

app.on('stopped', () => {
    logger.info('Archive processor stopped');
});

app.start(); 
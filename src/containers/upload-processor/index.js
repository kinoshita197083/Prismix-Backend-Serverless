const { Consumer } = require('sqs-consumer');
const { uploadProcessor } = require('./src/handlers/imageUploadFromS3Processor');
const logger = require('./src/utils/logger');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.QUEUE_URL,
    handleMessage: async (message) => {
        // Transform the message into Lambda-style event
        const event = {
            Records: [{
                ...message,
            }]
        };

        logger.info('Processing message', {
            messageId: message.MessageId,
            timestamp: new Date().toISOString()
        });

        await uploadProcessor(event);
    },
    batchSize: 10,
    visibilityTimeout: 7200, // 2 hours for ECS
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
    logger.info('Consumer started');
});

app.on('stopped', () => {
    logger.info('Consumer stopped');
});

app.start(); 
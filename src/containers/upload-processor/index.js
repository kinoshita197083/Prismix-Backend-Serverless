const { Consumer } = require('sqs-consumer');
const { uploadProcessor } = require('./src/handlers/imageUploadFromS3Processor');
const logger = require('./src/utils/logger');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.QUEUE_URL,
    handleMessage: async (message) => {
        try {
            // Transform the message into Lambda-style event
            const event = {
                Records: [
                    {
                        ...message,
                    }
                ]
            };

            logger.info('Processing message', {
                messageId: message.MessageId,
                body: event.Records[0].body,
                timestamp: new Date().toISOString()
            });

            await uploadProcessor(event);
        } catch (error) {
            logger.error('Error processing message', {
                messageId: message.MessageId,
                error: error.message,
                stack: error.stack,
                timestamp: new Date().toISOString()
            });
            throw error; // Rethrow to trigger message retry
        }
    },
    batchSize: 10,
    visibilityTimeout: 7200, // 2 hours for ECS
    messageAttributeNames: ['All']
});

// Error handling
app.on('error', (err) => {
    logger.error('Consumer error', {
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
    });
});

app.on('processing_error', (err) => {
    logger.error('Processing error', {
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
    });
});

// Lifecycle events
app.on('started', () => {
    logger.info('Consumer started', {
        timestamp: new Date().toISOString(),
        queueUrl: process.env.QUEUE_URL
    });
});

app.on('stopped', () => {
    logger.info('Consumer stopped', {
        timestamp: new Date().toISOString()
    });
});

// Start the consumer
app.start(); 
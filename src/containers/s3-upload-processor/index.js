const { Consumer } = require('sqs-consumer');
const { handler: uploadProcessor } = require('./src/handlers/imageUploadFromS3Processor');
const logger = require('./src/utils/logger');

// Helper function to send message to DLQ
const { sendToDLQ } = require('./src/utils/DLQ/helper');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.QUEUE_URL,
    handleMessage: async (message) => {
        try {
            logger.info('Starting message processing', {
                messageId: message.MessageId,
                queueUrl: process.env.QUEUE_URL,
                timestamp: new Date().toISOString()
            });

            // Transform the message into Lambda-style event
            const event = {
                Records: [{
                    messageId: message.MessageId,
                    body: message.Body,
                    attributes: message.Attributes,
                    messageAttributes: message.MessageAttributes,
                    md5OfBody: message.MD5OfBody,
                    eventSource: 'aws:sqs',
                    eventSourceARN: process.env.QUEUE_URL,
                    awsRegion: process.env.AWS_REGION
                }]
            };

            const { batchItemFailures } = await uploadProcessor(event);

            if (batchItemFailures.length > 0) {
                logger.error('Failed messages', { batchItemFailures });

                // Send failed messages to DLQ
                const error = new Error('Processing failed');
                await sendToDLQ({
                    queueUrl: process.env.DEAD_LETTER_QUEUE_URL,
                    message,
                    error,
                    logger
                });
                throw error; // Throw error to prevent message deletion from source queue
            }

            logger.info('Message processing completed', {
                messageId: message.MessageId,
                body: event?.Records?.[0]?.body,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            logger.error('Error processing message', {
                error: error.message,
                stack: error.stack,
                messageId: message.MessageId,
                timestamp: new Date().toISOString()
            });

            // Send failed message to DLQ
            await sendToDLQ(message, error);
            throw error; // Rethrow to prevent message deletion from source queue
        }
    },
    batchSize: 1,  // Process one message at a time initially
    visibilityTimeout: 7200,
    messageAttributeNames: ['All']
});

// Add startup logging
process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception', {
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString()
    });
    process.exit(1);
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

// Start the consumer with error handling
try {
    app.start();
    logger.info('Consumer started successfully', {
        queueUrl: process.env.QUEUE_URL,
        timestamp: new Date().toISOString()
    });
} catch (error) {
    logger.error('Failed to start consumer', {
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString()
    });
    process.exit(1);
} 

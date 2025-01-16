const { Consumer } = require('sqs-consumer');
const { zipArchiveProcessor } = require('./src/handlers/zipArchiveProcessor');
const logger = require('./src/utils/logger');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.ARCHIVE_QUEUE_URL,
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

            logger.info('Processing archive message', {
                messageId: message.MessageId,
                body: event.Records[0].body,
                timestamp: new Date().toISOString()
            });

            await zipArchiveProcessor(event);

            logger.info('Successfully processed archive message', {
                messageId: message.MessageId,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            logger.error('Error processing archive message', {
                messageId: message.MessageId,
                error: error.message,
                stack: error.stack,
                timestamp: new Date().toISOString()
            });
            throw error; // Rethrow to trigger message retry
        }
    },
    batchSize: 1, // Process one archive job at a time
    visibilityTimeout: 7200, // 2 hours
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
    logger.info('Archive processor started', {
        timestamp: new Date().toISOString(),
        queueUrl: process.env.ARCHIVE_QUEUE_URL,
        environment: {
            NODE_ENV: process.env.NODE_ENV,
            AWS_REGION: process.env.AWS_REGION,
            TASKS_TABLE: process.env.TASKS_TABLE,
            JOB_PROGRESS_TABLE: process.env.JOB_PROGRESS_TABLE,
            IMAGE_BUCKET: process.env.IMAGE_BUCKET,
            CONCURRENT_S3_OPERATIONS: process.env.CONCURRENT_S3_OPERATIONS,
            CONCURRENT_CHUNK_PROCESSING: process.env.CONCURRENT_CHUNK_PROCESSING,
            ARCHIVER_HIGH_WATER_MARK: process.env.ARCHIVER_HIGH_WATER_MARK
        }
    });
});

app.on('stopped', () => {
    logger.info('Archive processor stopped', {
        timestamp: new Date().toISOString()
    });
});

// Handle graceful shutdown
process.on('SIGTERM', async () => {
    logger.info('SIGTERM received, stopping consumer', {
        timestamp: new Date().toISOString()
    });
    await app.stop();
    process.exit(0);
});

process.on('SIGINT', async () => {
    logger.info('SIGINT received, stopping consumer', {
        timestamp: new Date().toISOString()
    });
    await app.stop();
    process.exit(0);
});

// Start the consumer
app.start(); 
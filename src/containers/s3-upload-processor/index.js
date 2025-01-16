const { Consumer } = require('sqs-consumer');
const { handler: uploadProcessor } = require('./src/handlers/imageUploadFromS3Processor');
const logger = require('./src/utils/logger');
// const http = require('http');

// // Create HTTP server for health checks
// const server = http.createServer((req, res) => {
//     if (req.url === '/health') {
//         res.writeHead(200);
//         res.end('OK');
//     } else {
//         res.writeHead(404);
//         res.end();
//     }
// });

// server.listen(80, () => {
//     logger.info('Health check server started on port 80');
// });

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
                // Records: [{
                //     ...message,
                // }]
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

            await uploadProcessor(event);

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
            throw error;
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

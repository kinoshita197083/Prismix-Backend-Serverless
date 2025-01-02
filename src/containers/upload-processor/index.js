const { Consumer } = require('sqs-consumer');
const { uploadProcessor } = require('./src/handlers/imageUploadFromS3Processor');

// Create SQS consumer
const app = Consumer.create({
    queueUrl: process.env.QUEUE_URL,
    handleMessage: async (message) => {
        // Your existing Lambda logic here
        await uploadProcessor(JSON.parse(message.Body));
    },
    batchSize: 10,
    visibilityTimeout: 900 // 15 minutes
});

app.on('error', (err) => {
    console.error('Error:', err.message);
});

app.on('processing_error', (err) => {
    console.error('Processing error:', err.message);
});

app.start(); 
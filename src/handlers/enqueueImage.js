const AWS = require('aws-sdk');
const sqsService = require('../services/sqsService');
const { handleError } = require('../utils/errorHandler');

exports.handler = async (event) => {
    const { Records } = event;

    for (const record of Records) {
        const { bucket, object } = record.s3;
        console.log(`Records: `, Records);
        console.log(`Processing image ${object.key} from bucket ${bucket.name}`);

        try {
            // Download the image from S3
            // const s3Object = await s3.getObject({ Bucket: bucket.name, Key: object.key }).promise();

            // Resize the image to a maximum of 1024x1024 while maintaining aspect ratio
            // const resizedImage = await sharp(s3Object.Body)
            //     .resize({ width: 1024, height: 1024, fit: 'inside' })
            //     .toBuffer();

            // Upload the resized image back to S3
            // await s3.putObject({
            //     Bucket: bucket.name,
            //     Key: `resized-${object.key}`,
            //     Body: resizedImage,
            //     ContentType: 'image/jpeg',
            // }).promise();

            // Enqueue the image for further processing
            await sqsService.sendMessage({
                QueueUrl: process.env.SQS_QUEUE_URL,
                MessageBody: JSON.stringify({
                    bucket: bucket.name,
                    key: object.key,
                    resizedKey: `resized-${object.key}`,
                    labels: rekognitionResult.Labels,
                }),
            });

            console.log(`Successfully processed and enqueued image: ${object.key}`);
        } catch (error) {
            console.error(`Error processing image ${object.key}:`, error);

            // Pass the error to the Dead Letter Queue
            await passToDeadLetterQueue(process.env.DEAD_LETTER_QUEUE_URL, error, bucket.name, object.key, record);

            return handleError(error, context);
        }
    }
};

const passToDeadLetterQueue = async (queueURL, error, bucketName, objectKey, record) => {
    // Move message to Dead Letter Queue
    try {
        await sqsService.sendMessage({
            QueueUrl: queueURL,
            MessageBody: JSON.stringify({
                error: error.message,
                bucket: bucketName,
                key: objectKey,
                event: record, // Optionally include the entire event record
            }),
        });
        console.log(`Message moved to Dead Letter Queue for image: ${objectKey}`);
    } catch (dlqError) {
        console.error(`Failed to send message to Dead Letter Queue for image ${objectKey}:`, dlqError);
    }
};
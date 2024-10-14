const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { RekognitionClient, DetectLabelsCommand } = require('@aws-sdk/client-rekognition');
const { DynamoDBClient, PutItemCommand, GetItemCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const sharp = require('sharp');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');
const dynamoService = require('../services/dynamoService');

const s3Client = new S3Client();
const sqsClient = new SQSClient();
const rekognitionClient = new RekognitionClient();
const dynamoClient = new DynamoDBClient();

async function calculateImageHash(bucket, key) {
    try {
        console.log('Start calculating image hash...');
        const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
        const { Body } = await s3Client.send(getObjectCommand);
        const buffer = await streamToBuffer(Body);

        // Resize image to a standard size for consistent hashing
        const resizedBuffer = await sharp(buffer)
            .resize(256, 256, { fit: 'inside' })
            .grayscale()
            .raw()
            .toBuffer();

        // Calculate perceptual hash
        const hash = crypto.createHash('md5').update(resizedBuffer).digest('hex');
        return hash;
    } catch (error) {
        logger.error('Error calculating image hash', { error });
        throw new AppError('Error calculating image hash', 500);
    }
}

async function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function checkForDuplicate(hash, jobId) {
    console.log('Checking for duplicates...');
    const getItemCommand = new GetItemCommand({
        TableName: process.env.IMAGE_HASH_TABLE,
        Key: {
            HashValue: { S: hash },
            JobId: { S: jobId }
        }
    });

    try {
        const { Item } = await dynamoClient.send(getItemCommand);
        return Item ? Item.ImageId.S : null;
    } catch (error) {
        logger.error('Error checking for duplicate', { error });
        return null;
    }
}

async function storeImageHash(hash, jobId, imageId) {
    console.log('Storing image hash...');
    const expirationTime = Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60); // Current time + 3 days in seconds

    const putItemCommand = new PutItemCommand({
        TableName: process.env.IMAGE_HASH_TABLE,
        Item: {
            HashValue: { S: hash },
            JobId: { S: jobId },
            ImageId: { S: imageId },
            Timestamp: { N: Date.now().toString() },
            ExpirationTime: { N: expirationTime.toString() } // Set TTL
        }
    });

    try {
        await dynamoClient.send(putItemCommand);
    } catch (error) {
        logger.error('Error storing image hash', { error });
    }
}

exports.handler = async (event, context) => {
    console.log('----> Event:', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    await Promise.all(Records.map(async (record) => {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);

        console.log('----> Message:', message);

        const { bucket, key: s3ObjectKey, projectId, jobId, userId, settingValue } = message;
        const [type, , , projectSettingId, , imageId] = s3ObjectKey.split('/');

        try {
            // Calculate image hash
            const imageHash = await calculateImageHash(bucket, s3ObjectKey);

            // Check for duplicate
            const duplicateImageId = await checkForDuplicate(imageHash, jobId);

            if (duplicateImageId) {
                logger.info('Duplicate image found', { originalImageId: duplicateImageId, currentImageId: imageId });

                // Update task status to COMPLETED and mark as duplicate
                await dynamoService.updateTaskStatus({
                    jobId,
                    taskId: imageId,
                    imageS3Key: s3ObjectKey,
                    status: 'COMPLETED',
                    evaluation: 'DUPLICATE',
                });

                return; // Skip further processing for this image
            }

            // Store the hash for future duplicate checks
            await storeImageHash(imageHash, jobId, imageId);

            // Perform object detection
            const detectLabelsCommand = new DetectLabelsCommand({
                Image: {
                    S3Object: {
                        Bucket: bucket,
                        Name: s3ObjectKey,
                    },
                },
                MaxLabels: 10,
                MinConfidence: 70,
            });

            const rekognitionResult = await rekognitionClient.send(detectLabelsCommand);
            const labels = rekognitionResult.Labels;

            console.log('Updating task status to COMPLETED and storing results...');
            // Update task status to COMPLETED and store results
            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: s3ObjectKey,
                status: 'COMPLETED',
                labels,
                evaluation: evaluate(labels, settingValue)
            });

            logger.info('Successfully processed image', { imageId, projectId, jobId });
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
            });

            // Update task status to WAITING_FOR_RETRY
            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: s3ObjectKey,
                status: 'WAITING_FOR_RETRY'
            });

            // Send error to Dead Letter Queue
            const sendMessageCommand = new SendMessageCommand({
                QueueUrl: process.env.DEAD_LETTER_QUEUE_URL,
                MessageBody: JSON.stringify({
                    error: error.message,
                    originalMessage: record
                })
            });
            await sqsClient.send(sendMessageCommand);
        }
    }));
};

function evaluate(labels, projectSettings) {
    console.log('Evaluating labels...');
    // Placeholder evaluation logic
    return 'ELIGIBLE';
}

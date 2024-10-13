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
    console.log('Start Calculating image hash ...')
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
}

async function streamToBuffer(stream) {
    console.log('Streaming to buffer ...')
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function checkForDuplicate(hash, jobId) {
    console.log('Checking for duplications...')
    const getItemCommand = new GetItemCommand({
        TableName: process.env.IMAGE_HASHES_TABLE,
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
    console.log('No duplication found: storing image hash...')
    const expirationTime = Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60); // Current time + 3 days in seconds

    const putItemCommand = new PutItemCommand({
        TableName: process.env.IMAGE_HASHES_TABLE,
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
    console.log('----> Event: ', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    for (const record of Records) {
        let bucket, object, projectId, jobId, imageId, projectSetting;

        // Parse the record (SQS event or local test event)
        if (record.eventSource === 'aws:sqs') {
            const body = JSON.parse(record.body);
            const message = JSON.parse(body.Message);
            bucket = message.bucket.name;
            object = { key: message.key };
            projectId = message.projectId;
            jobId = message.jobId;
            imageId = message.imageId;
        } else {
            const parsedRecord = JSON.parse(record.body);
            const { bucket: parsedBucket, object: parsedObject } = parsedRecord;
            const objectKey = parsedObject.key;
            const [type, userId, receivedProjectId, receivedProjectSettingId, receivedJobId, receivedImageId] = objectKey.split('/');
            projectId = receivedProjectId;
            jobId = receivedJobId;
            imageId = receivedImageId;
            bucket = parsedBucket.name;
            object = parsedObject;
        }

        logger.info('Processing image', { bucket, key: object.key, projectId, jobId, imageId });

        try {
            // Calculate image hash
            const imageHash = await calculateImageHash(bucket, object.key);

            // Check for duplicate
            const duplicateImageId = await checkForDuplicate(imageHash, jobId);

            if (duplicateImageId) {
                logger.info('Duplicate image found', { originalImageId: duplicateImageId, currentImageId: imageId });

                // Update task status to COMPLETED and mark as duplicate
                await dynamoService.updateTaskStatus({
                    JobID: jobId,
                    TaskID: imageId,
                    ImageS3Key: object.key,
                    TaskStatus: 'COMPLETED',
                    isDuplicate: true,
                    duplicateOf: duplicateImageId
                });

                continue; // Skip further processing for this image
            }

            // Store the hash for future duplicate checks
            await storeImageHash(imageHash, jobId, imageId);

            // Perform object detection
            const detectLabelsCommand = new DetectLabelsCommand({
                Image: {
                    S3Object: {
                        Bucket: bucket,
                        Name: object.key,
                    },
                },
                MaxLabels: 10,
                MinConfidence: 70,
            });

            const rekognitionResult = await rekognitionClient.send(detectLabelsCommand);

            const labels = rekognitionResult.Labels;

            logger.info('Rekognition Result', { result: labels, imageId, jobId });

            // Here you would implement your filtering logic based on projectSettings and labels

            // Update task status to COMPLETED and store results
            await dynamoService.updateTaskStatus({
                JobID: jobId,
                TaskID: imageId,
                ImageS3Key: object.key,
                TaskStatus: 'COMPLETED',
                ProcessingResult: labels,
                Evaluation: evaluate(labels, {})
            });

            logger.info('Successfully processed image', { imageId, projectId, jobId });
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: object.key,
            });

            // Update task status to FAILED
            await dynamoService.updateTaskStatus({
                JobID: jobId,
                TaskID: imageId,
                ImageS3Key: object.key,
                TaskStatus: 'FAILED'
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
    }
};

function evaluate(labels, projectSettings) {
    console.log('----> Evaluating .....');
    return 'ELIGIBLE';
}

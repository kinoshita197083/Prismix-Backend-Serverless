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
            // Get the existing task item
            const getItemParams = {
                TableName: process.env.TASKS_TABLE,
                Key: {
                    JobID: { S: jobId },
                    TaskID: { S: imageId }
                }
            };
            const { Item: existingItem } = await dynamoClient.send(new GetItemCommand(getItemParams));

            // Calculate image hash
            const imageHash = await calculateImageHash(bucket, s3ObjectKey);

            // Check for duplicate
            const duplicateImageId = await checkForDuplicate(imageHash, jobId);

            let status = 'COMPLETED';
            let isDuplicate = false;
            let isEligible = false;
            let isFailed = false;

            if (duplicateImageId) {
                logger.info('Duplicate image found', { originalImageId: duplicateImageId, currentImageId: imageId });
                isDuplicate = true;
            } else {
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

                // Evaluate the image based on labels and settingValue
                const evaluation = evaluate(labels, settingValue);
                isEligible = evaluation === 'ELIGIBLE';
            }

            // Update the task item
            const updateParams = {
                TableName: process.env.TASKS_TABLE,
                Key: {
                    JobID: { S: jobId },
                    TaskID: { S: imageId }
                },
                UpdateExpression: 'SET ProcessedImages = :p, #status = :s, LastUpdateTime = :t',
                ExpressionAttributeNames: { '#status': 'Status' },
                ExpressionAttributeValues: {
                    ':p': { N: '1' },
                    ':s': { S: status },
                    ':t': { N: Date.now().toString() }
                }
            };

            if (isDuplicate) {
                updateParams.UpdateExpression += ', DuplicateImages = :d';
                updateParams.ExpressionAttributeValues[':d'] = { N: '1' };
            } else if (isEligible) {
                updateParams.UpdateExpression += ', EligibleImages = :e';
                updateParams.ExpressionAttributeValues[':e'] = { N: '1' };
            } else if (!isDuplicate && !isEligible) {
                updateParams.UpdateExpression += ', ExcludedImages = :ex';
                updateParams.ExpressionAttributeValues[':ex'] = { N: '1' };
            }

            if (isFailed) {
                updateParams.UpdateExpression += ', FailedImages = :f';
                updateParams.ExpressionAttributeValues[':f'] = { N: '1' };
            }

            await dynamoClient.send(new UpdateItemCommand(updateParams));

            logger.info('Successfully processed image', { imageId, projectId, jobId });
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
            });

            // Update task status to WAITING_FOR_RETRY and increment FailedImages
            const updateParams = {
                TableName: process.env.TASKS_TABLE,
                Key: {
                    JobID: { S: jobId },
                    TaskID: { S: imageId }
                },
                UpdateExpression: 'SET #status = :s, LastUpdateTime = :t, FailedImages = FailedImages + :one',
                ExpressionAttributeNames: { '#status': 'Status' },
                ExpressionAttributeValues: {
                    ':s': { S: 'WAITING_FOR_RETRY' },
                    ':t': { N: Date.now().toString() },
                    ':one': { N: '1' }
                }
            };
            await dynamoClient.send(new UpdateItemCommand(updateParams));

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
    // Implement your evaluation logic here
    // For now, we'll just return 'ELIGIBLE' as a placeholder
    return 'ELIGIBLE';
}

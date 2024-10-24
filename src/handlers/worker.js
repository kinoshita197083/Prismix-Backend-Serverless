const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { RekognitionClient, DetectLabelsCommand } = require('@aws-sdk/client-rekognition');
const { DynamoDBClient, PutItemCommand, GetItemCommand, UpdateCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const sharp = require('sharp');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');
const dynamoService = require('../services/dynamoService');

const s3Client = new S3Client();
const rekognitionClient = new RekognitionClient();
const dynamoClient = new DynamoDBClient();
const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoClient, {
    marshallOptions: {
        removeUndefinedValues: true
    }
});

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second delay between retries

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function calculateImageHash(bucket, key) {
    try {
        console.log('Start calculating image hash...');
        const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
        const { Body } = await s3Client.send(getObjectCommand);
        console.log('Image retrieved from S3 successfully');
        const buffer = await streamToBuffer(Body);
        console.log('buffer processed successfully');

        // Resize image to a standard size for consistent hashing
        const resizedBuffer = await sharp(buffer)
            .resize(256, 256, { fit: 'inside' })
            .grayscale()
            .raw()
            .toBuffer();

        console.log('resizedBuffer: ', resizedBuffer);

        // Calculate perceptual hash
        const hash = crypto.createHash('md5').update(resizedBuffer).digest('hex');
        console.log('hash: ', hash);
        return hash;
    } catch (error) {
        logger.error('Error calculating image hash', { error });
        throw new AppError('Error calculating image hash', 500);
    }
}

async function streamToBuffer(stream) {
    console.log('Streaming to buffer...');
    try {
        return new Promise((resolve, reject) => {
            const chunks = [];
            stream.on('data', (chunk) => chunks.push(chunk));
            stream.on('error', reject);
            stream.on('end', () => resolve(Buffer.concat(chunks)));
        });
    } catch (error) {
        logger.error('Error streaming to buffer', { error });
        throw new AppError('Error streaming to buffer', 500);
    }
}

async function checkAndStoreImageHash(hash, jobId, imageId, s3Key) {
    console.log('Checking and storing image hash...', { hash, jobId, imageId, s3Key });
    const getItemCommand = new GetItemCommand({
        TableName: process.env.IMAGE_HASH_TABLE,
        Key: {
            HashValue: { S: hash },
            JobId: { S: jobId }
        }
    });

    try {
        const { Item } = await dynamoClient.send(getItemCommand);

        if (Item) {
            console.log('Hash already exists, this is a duplicate');
            return {
                isDuplicate: true,
                originalImageId: Item.ImageId.S,
                originalImageS3Key: Item.ImageS3Key.S
            };
        }

        // If no item found, store the new hash
        const putItemCommand = new PutItemCommand({
            TableName: process.env.IMAGE_HASH_TABLE,
            Item: {
                HashValue: { S: hash },
                JobId: { S: jobId },
                ImageId: { S: imageId },
                ImageS3Key: { S: s3Key },
                Timestamp: { N: Date.now().toString() },
                ExpirationTime: { N: (Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60)).toString() }
            },
            ConditionExpression: 'attribute_not_exists(HashValue) AND attribute_not_exists(JobId)'
        });

        await dynamoClient.send(putItemCommand);
        console.log('Successfully stored new hash');
        return { isDuplicate: false };
    } catch (error) {
        if (error.name === 'ConditionalCheckFailedException') {
            console.log('Race condition: Hash was stored by another process');
            return {
                isDuplicate: true,
                originalImageId: 'Unknown due to race condition',
                originalImageS3Key: 'Unknown due to race condition'
            };
        }
        logger.error('Error checking and storing image hash', { error });
        throw error;
    }
}

function evaluate(labels, projectSettings) {
    console.log('Evaluating labels...');
    // Placeholder evaluation logic
    return 'ELIGIBLE';
}

async function processImageWithRetry(message, attempt = 1) {
    const { bucket, key: s3ObjectKey, projectId, jobId, userId, settingValue } = message;
    const [type, , , projectSettingId, , imageId] = s3ObjectKey.split('/');

    try {
        // Calculate image hash
        const imageHash = await calculateImageHash(bucket, s3ObjectKey);
        console.log('Calculated image hash:', imageHash);

        // Check and store the hash
        const { isDuplicate, originalImageId, originalImageS3Key } = await checkAndStoreImageHash(imageHash, jobId, imageId, s3ObjectKey);

        if (isDuplicate) {
            logger.info('Duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });

            // Update task status to COMPLETED and mark as duplicate
            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: s3ObjectKey,
                status: 'COMPLETED',
                evaluation: 'DUPLICATE',
                updatedAt: new Date().toISOString(),
                duplicateOf: originalImageId,
                duplicateOfS3Key: originalImageS3Key
            });

            return; // Skip further processing for this image
        }

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
        const labels = rekognitionResult.Labels || [];
        console.log('Rekognition labels:', JSON.stringify(labels, null, 2));

        console.log('Updating task status to COMPLETED and storing results...');
        console.log('Labels:', JSON.stringify(labels, null, 2));

        const evaluation = evaluate(labels, settingValue);
        console.log('Evaluation result:', evaluation);

        // Update task status to COMPLETED and store results
        await dynamoService.updateTaskStatus({
            jobId,
            taskId: imageId,
            imageS3Key: s3ObjectKey,
            status: 'COMPLETED',
            labels,
            evaluation
        });
        logger.info('Successfully processed image', { imageId, projectId, jobId });

        return { success: true, imageId, s3ObjectKey };
    } catch (error) {
        if (attempt < MAX_RETRIES) {
            logger.warn(`Retry attempt ${attempt + 1} for image ${imageId}`, { error: error.message });
            await sleep(RETRY_DELAY * attempt); // Exponential backoff
            return processImageWithRetry(message, attempt + 1);
        }

        logger.error('Error processing image after max retries', {
            error: error.message,
            stack: error.stack,
            bucket,
            key: s3ObjectKey,
            attempts: attempt
        });

        // Update task status to FAILED
        await dynamoService.updateTaskStatus({
            jobId,
            taskId: imageId,
            imageS3Key: s3ObjectKey,
            status: 'FAILED',
            evaluation: 'FAILED'
        });

        return {
            success: false,
            imageId,
            s3ObjectKey,
            error: error.message,
            attempts: attempt
        };
    }
}

exports.handler = async (event, context) => {
    console.log('----> Event:', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    const results = await Promise.all(Records.map(async (record) => {
        try {
            const body = JSON.parse(record.body);
            const message = JSON.parse(body.Message);

            console.log('----> Message:', message);

            return await processImageWithRetry(message);
        } catch (error) {
            logger.error('Error processing record', { error: error.message, record });
            return { success: false, error: error.message };
        }
    }));

    const failedProcessing = results.filter(r => !r.success);

    // Update DynamoDB if there are failed processings
    if (failedProcessing.length > 0) {
        const failedProcessingDetails = failedProcessing.map(f => ({
            imageId: f.imageId,
            s3ObjectKey: f.s3ObjectKey,
            error: f.error,
            attempts: f.attempts
        }));

        const updateItemCommand = new UpdateCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: message.jobId },
            UpdateExpression: 'SET failedImages = :fi, updatedAt = :lut, failedProcessing = :fp',
            ExpressionAttributeValues: {
                ':fi': failedProcessing.length,
                ':lut': Date.now(),
                ':fp': failedProcessingDetails
            }
        });

        try {
            await dynamoDbDocumentClient.send(updateItemCommand);
            console.log('Successfully updated job progress with failed processing details');
        } catch (error) {
            console.error('Error updating job progress table:', error);
        }

        console.log('Failed processings:', failedProcessingDetails);
    }

    logger.info('Image processing finished', {
        message: `Processed ${Records.length} images`,
        successCount: results.filter(r => r.success).length,
        failedCount: failedProcessing.length
    });

};

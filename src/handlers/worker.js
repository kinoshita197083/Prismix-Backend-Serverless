const { RekognitionClient, DetectLabelsCommand } = require('@aws-sdk/client-rekognition');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const sharp = require('sharp');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');
const dynamoService = require('../services/dynamoService');
const s3Service = require('../services/s3Service');
const { COMPLETED, DUPLICATE } = require('../utils/config');

const rekognitionClient = new RekognitionClient();
const dynamoClient = new DynamoDBClient();
const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoClient);

async function calculateImageHash(bucket, key) {
    try {
        console.log('Start calculating image hash...');
        const params = { Bucket: bucket, Key: key };
        const body = await s3Service.getFile(params);
        const buffer = await streamToBuffer(body);
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
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function checkAndStoreImageHash(hash, jobId, imageId, s3Key, maxRetries = 3) {
    console.log('Checking and storing image hash...', { hash, jobId, imageId, s3Key });
    let retryCount = 0;
    let lastError;

    while (retryCount <= maxRetries) {
        try {
            const getCommand = new GetCommand({
                TableName: process.env.IMAGE_HASH_TABLE,
                Key: {
                    HashValue: hash,
                    JobId: jobId
                },
                ConsistentRead: true
            });

            const { Item } = await dynamoDbDocumentClient.send(getCommand);

            if (Item) {
                console.log('Hash already exists, this is a duplicate');
                return {
                    isDuplicate: true,
                    originalImageId: Item.ImageId,
                    originalImageS3Key: Item.ImageS3Key
                };
            }

            // If no item found, store the new hash
            const putCommand = new PutCommand({
                TableName: process.env.IMAGE_HASH_TABLE,
                Item: {
                    HashValue: hash,
                    JobId: jobId,
                    ImageId: imageId,
                    ImageS3Key: s3Key,
                    Timestamp: Date.now().toString(),
                    ExpirationTime: (Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60)).toString()
                },
                ConditionExpression: 'attribute_not_exists(HashValue) AND attribute_not_exists(JobId)'
            });

            await dynamoDbDocumentClient.send(putCommand);
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

            lastError = error;
            retryCount++;

            if (retryCount <= maxRetries) {
                const delay = Math.min(1000 * Math.pow(2, retryCount), 8000); // Exponential backoff with max 8s delay
                console.log(`Retry attempt ${retryCount}/${maxRetries} after ${delay}ms delay`);
                await new Promise(resolve => setTimeout(resolve, delay));
                continue;
            }
        }
    }

    logger.error('Error checking and storing image hash after retries', {
        error: lastError,
        retryAttempts: retryCount
    });
    throw lastError;
}

function evaluate(labels, projectSettings) {
    console.log('Evaluating labels...');
    // Placeholder evaluation logic
    return 'ELIGIBLE';
}

async function duplicateImageDetection({ bucket, s3ObjectKey, jobId, imageId }) {
    // Calculate image hash
    const imageHash = await calculateImageHash(bucket, s3ObjectKey);
    console.log('Calculated image hash:', imageHash);

    // Check and store the hash
    const {
        isDuplicate,
        originalImageId,
        originalImageS3Key
    } = await checkAndStoreImageHash(imageHash, jobId, imageId, s3ObjectKey);

    if (isDuplicate) {
        logger.info('Duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });
        const response = await updateTaskStatusAsDuplicate({ jobId, imageId, s3ObjectKey, originalImageId, originalImageS3Key });
        console.log('Task status updated to COMPLETED and marked as duplicate', response);
        return true;
    } else {
        logger.info('No duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });
        return false;
    }
}

async function updateTaskStatusAsDuplicate({ jobId, imageId, s3ObjectKey, originalImageId, originalImageS3Key }) {
    console.log('Updating task status to COMPLETED and marking as duplicate...',
        { jobId, imageId, s3ObjectKey, originalImageId, originalImageS3Key });

    // Update task status to COMPLETED and mark as duplicate
    return await dynamoService.updateTaskStatus({
        jobId,
        taskId: imageId,
        status: COMPLETED,
        evaluation: DUPLICATE,
        imageS3Key: s3ObjectKey,
        duplicateOf: originalImageId,
        duplicateOfS3Key: originalImageS3Key,
        updatedAt: new Date().toISOString(),
    });
}

async function labelDetection({ bucket, s3ObjectKey, jobId, imageId, settingValue }) {
    // Perform object detection
    console.log('Performing object detection...');
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
    console.log('Object detection labels:', labels);

    return labels;
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
            // Fetch project setting rules to see what task the worker is supposed to do
            const projectSettingRules = await fetchProjectSettingRules(jobId);
            console.log('Project setting rules:', projectSettingRules);

            const isDuplicate = await duplicateImageDetection({ bucket, s3ObjectKey, jobId, imageId });

            if (isDuplicate) {
                logger.info('Duplicate image found, skipping further processing', { originalImageId, originalImageS3Key, currentImageId: imageId });
                return; // Skip further processing for this image
            }

            const labels = await labelDetection({
                bucket,
                s3ObjectKey,
                jobId,
                imageId,
                settingValue
            });

            console.log('Rekognition labels:', JSON.stringify(labels, null, 2));

            console.log('Updating task status to COMPLETED and storing results...');
            console.log('Labels:', JSON.stringify(labels, null, 2));

            const evaluation = evaluate(labels, settingValue);
            console.log('Evaluation result:', evaluation);

            // Update task status to COMPLETED and store results
            try {
                await dynamoService.updateTaskStatus({
                    jobId,
                    taskId: imageId,
                    imageS3Key: s3ObjectKey,
                    status: 'COMPLETED',
                    labels,
                    evaluation
                });
                logger.info('Successfully processed image', { imageId, projectId, jobId });
            } catch (updateError) {
                logger.error('Error updating task status', {
                    error: updateError.message,
                    stack: updateError.stack,
                    jobId,
                    taskId: imageId,
                    labels: JSON.stringify(labels),
                    evaluation
                });
                throw updateError; // Re-throw to be caught by the outer catch block
            }
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
            });

            // Update task status to WAITING_FOR_RETRY
            try {
                await dynamoService.updateTaskStatus({
                    jobId,
                    taskId: imageId,
                    imageS3Key: s3ObjectKey,
                    status: 'FAILED',
                    evaluation: 'COMPLETED',
                    reason: error.message
                });
            } catch (retryUpdateError) {
                logger.error('Error updating task status to FAILED in TASK_TABLE', { error: retryUpdateError.message, jobId, taskId: imageId });
            }
        }
    }));
};

async function fetchProjectSettingRules(jobId) {
    console.log('Fetching project setting rules...', { jobId });
    try {
        const response = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        console.log('Fetch project setting rules response:', response);
        return response?.projectSetting;
    } catch (error) {
        // logger.error('Error fetching project setting rules', { error, jobId });
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { RekognitionClient, DetectLabelsCommand } = require('@aws-sdk/client-rekognition');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand, TransactWriteCommand, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');
const sharp = require('sharp');
const crypto = require('crypto');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');
const { sleep } = require('../utils/helpers');
const { MAX_RETRIES, RETRY_DELAY } = require('../utils/googleDrive/config');

const s3Client = new S3Client();
const rekognitionClient = new RekognitionClient();
const dynamoClient = new DynamoDBClient();
const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoClient, {
    marshallOptions: {
        removeUndefinedValues: true
    }
});

const CONSTANTS = {
    MAX_IMAGE_SIZE_BYTES: 10 * 1024 * 1024, // 10MB
    STALE_THRESHOLD_MS: 5 * 60 * 1000, // 5 minutes
    RETRY_DELAY_MS: RETRY_DELAY, // Fix the naming inconsistency
    PROCESSING_STATUSES: {
        COMPLETED: 'COMPLETED',
        IN_PROGRESS: 'IN_PROGRESS',
        FAILED: 'FAILED'
    }
};

async function calculateImageHash(bucket, key) {
    console.log('Start calculating image hash...');
    try {
        const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
        const { Body } = await s3Client.send(getObjectCommand);
        console.log('Image retrieved from S3 successfully');

        const buffer = await streamToBuffer(Body);
        console.log('Buffer processed successfully');

        // Resize image to a standard size for consistent hashing
        const resizedBuffer = await sharp(buffer)
            .resize(256, 256, { fit: 'inside' })
            .grayscale()
            .raw()
            .toBuffer();
        console.log('Image resized successfully');

        // Calculate perceptual hash
        const hash = crypto.createHash('md5').update(resizedBuffer).digest('hex');
        console.log('Hash calculated:', hash);

        return hash;
    } catch (error) {
        logger.error('Error calculating image hash', { error });
        if (error instanceof sharp.SharpError) {
            throw new AppError('Image processing error', 500);
        } else if (error.name === 'NoSuchKey') {
            throw new AppError('Image not found in S3', 404);
        } else {
            throw new AppError('Unexpected error calculating image hash', 500);
        }
    }
}

async function streamToBuffer(stream) {
    console.log('Streaming to buffer...');
    return new Promise((resolve, reject) => {
        let size = 0;
        const chunks = [];

        stream.on('data', (chunk) => {
            size += chunk.length;
            if (size > CONSTANTS.MAX_IMAGE_SIZE_BYTES) {
                stream.destroy();
                reject(new AppError('Image size exceeds maximum allowed size', 413));
                return;
            }
            chunks.push(chunk);
        });

        stream.on('error', (error) => {
            logger.error('Error streaming to buffer', { error });
            reject(new AppError('Error streaming to buffer', 500));
        });

        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function checkAndStoreImageHash(hash, jobId, imageId, s3Key, retryCount = 0) {
    console.log('Checking and storing image hash...', { hash, jobId, imageId, s3Key, retryCount });

    try {
        // Try to create new entry first with atomic operation
        try {
            await dynamoDbDocumentClient.send(new PutCommand({
                TableName: process.env.IMAGE_HASH_TABLE,
                Item: {
                    HashValue: hash,
                    JobId: jobId,
                    ImageId: imageId,
                    ImageS3Key: s3Key,
                    UpdatedAt: Date.now(),
                    ExpirationTime: Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60),
                    ProcessingStatus: CONSTANTS.PROCESSING_STATUSES.IN_PROGRESS
                },
                ConditionExpression: 'attribute_not_exists(HashValue) AND attribute_not_exists(JobId)'
            }));

            return { isDuplicate: false };
        } catch (error) {
            if (error.name !== 'ConditionalCheckFailedException') {
                throw error;
            }
            // Entry exists, continue with checking existing entry
        }

        // Get existing entry
        const { Item } = await dynamoDbDocumentClient.send(new GetCommand({
            TableName: process.env.IMAGE_HASH_TABLE,
            Key: { HashValue: hash, JobId: jobId },
            ConsistentRead: true
        }));

        if (!Item) {
            // Race condition where item was deleted between our put and get
            if (retryCount >= MAX_RETRIES) {
                throw new AppError('Max retries exceeded during hash check', 500);
            }
            await sleep(CONSTANTS.RETRY_DELAY_MS);
            return checkAndStoreImageHash(hash, jobId, imageId, s3Key, retryCount + 1);
        }

        const status = Item.ProcessingStatus;
        const updatedAt = Item.UpdatedAt || 0;

        switch (status) {
            case CONSTANTS.PROCESSING_STATUSES.COMPLETED:
                return {
                    isDuplicate: true,
                    originalImageId: Item.ImageId,
                    originalImageS3Key: Item.ImageS3Key
                };

            case CONSTANTS.PROCESSING_STATUSES.IN_PROGRESS:
            case CONSTANTS.PROCESSING_STATUSES.FAILED:
                // Attempt to take ownership with a single atomic operation
                try {
                    await dynamoDbDocumentClient.send(new UpdateCommand({
                        TableName: process.env.IMAGE_HASH_TABLE,
                        Key: { HashValue: hash, JobId: jobId },
                        UpdateExpression: 'SET ProcessingStatus = :newStatus, ImageId = :newImageId, ImageS3Key = :newS3Key, UpdatedAt = :now',
                        ConditionExpression: '(ProcessingStatus = :failedStatus OR (ProcessingStatus = :inProgressStatus AND UpdatedAt < :staleTime))',
                        ExpressionAttributeValues: {
                            ':newStatus': CONSTANTS.PROCESSING_STATUSES.IN_PROGRESS,
                            ':newImageId': imageId,
                            ':newS3Key': s3Key,
                            ':now': Date.now(),
                            ':failedStatus': CONSTANTS.PROCESSING_STATUSES.FAILED,
                            ':inProgressStatus': CONSTANTS.PROCESSING_STATUSES.IN_PROGRESS,
                            ':staleTime': Date.now() - CONSTANTS.STALE_THRESHOLD_MS
                        }
                    }));
                    return { isDuplicate: false };
                } catch (updateError) {
                    if (updateError.name === 'ConditionalCheckFailedException') {
                        if (retryCount >= MAX_RETRIES) {
                            throw new AppError('Max retries exceeded waiting for hash processing', 500);
                        }
                        await sleep(CONSTANTS.RETRY_DELAY_MS);
                        return checkAndStoreImageHash(hash, jobId, imageId, s3Key, retryCount + 1);
                    }
                    throw updateError;
                }

            default:
                logger.warn('Unknown processing status', { status, hash, jobId, imageId });
                throw new AppError('Unknown processing status', 500);
        }
    } catch (error) {
        const enhancedError = new AppError('Error checking and storing image hash', 500);
        enhancedError.details = {
            originalError: error.message,
            hash,
            jobId,
            imageId,
            retryCount
        };
        throw enhancedError;
    }
}

function evaluate(labels, projectSettings) {
    console.log('Evaluating labels...');
    // Placeholder evaluation logic
    return 'ELIGIBLE';
}

async function processImageWithRetry(message, attempt = 1) {
    const { bucket, key: s3ObjectKey, projectId, jobId, userId, settingValue } = message;
    const [type, , , projectSettingId, , imageName] = s3ObjectKey.split('/');
    const imageId = imageName + '-' + crypto.randomBytes(10).toString('hex');
    let imageHash;

    try {
        // Calculate image hash
        imageHash = await calculateImageHash(bucket, s3ObjectKey);
        console.log('Calculated image hash:', imageHash);

        // Check and store the hash
        const { isDuplicate, originalImageId, originalImageS3Key } = await checkAndStoreImageHash(imageHash, jobId, imageId, s3ObjectKey);

        if (isDuplicate) {
            logger.info('Duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });

            // Use transactWrite to update both tables atomically
            const transactItems = [
                {
                    Update: {
                        TableName: process.env.TASKS_TABLE,
                        Key: {
                            JobID: jobId,
                            TaskID: imageId
                        },
                        UpdateExpression: 'SET TaskStatus = :status, Evaluation = :evaluation, UpdatedAt = :updatedAt, DuplicateOf = :duplicateOf, DuplicateOfS3Key = :duplicateOfS3Key',
                        ExpressionAttributeValues: {
                            ':status': CONSTANTS.PROCESSING_STATUSES.COMPLETED,
                            ':evaluation': 'DUPLICATE',
                            ':updatedAt': new Date().toISOString(),
                            ':duplicateOf': originalImageId,
                            ':duplicateOfS3Key': originalImageS3Key
                        }
                    }
                },
                {
                    Update: {
                        TableName: process.env.IMAGE_HASH_TABLE,
                        Key: {
                            HashValue: imageHash,
                            JobId: jobId
                        },
                        UpdateExpression: 'SET ProcessingStatus = :status',
                        ExpressionAttributeValues: {
                            ':status': CONSTANTS.PROCESSING_STATUSES.COMPLETED
                        }
                    }
                }
            ];

            await dynamoDbDocumentClient.send(new TransactWriteCommand({ TransactItems: transactItems }));
            return { success: true, imageId, s3ObjectKey, isDuplicate: true };
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
        const evaluation = evaluate(labels, settingValue);

        // Use transactWrite to update both tables atomically
        const transactItems = [
            {
                Update: {
                    TableName: process.env.TASKS_TABLE,
                    Key: {
                        JobID: jobId,
                        TaskID: imageId
                    },
                    UpdateExpression: 'SET TaskStatus = :status, Labels = :labels, Evaluation = :evaluation, UpdatedAt = :updatedAt',
                    ExpressionAttributeValues: {
                        ':status': CONSTANTS.PROCESSING_STATUSES.COMPLETED,
                        ':labels': labels,
                        ':evaluation': evaluation,
                        ':updatedAt': new Date().toISOString()
                    }
                }
            },
            {
                Update: {
                    TableName: process.env.IMAGE_HASH_TABLE,
                    Key: {
                        HashValue: imageHash,
                        JobId: jobId
                    },
                    UpdateExpression: 'SET ProcessingStatus = :status',
                    ConditionExpression: 'ProcessingStatus = :currentStatus',
                    ExpressionAttributeValues: {
                        ':status': CONSTANTS.PROCESSING_STATUSES.COMPLETED,
                        ':currentStatus': CONSTANTS.PROCESSING_STATUSES.IN_PROGRESS
                    }
                }
            }
        ];

        await dynamoDbDocumentClient.send(new TransactWriteCommand({ TransactItems: transactItems }));
        logger.info('Successfully processed image', { imageId, projectId, jobId });

        return { success: true, imageId, s3ObjectKey };
    } catch (error) {
        if (attempt < MAX_RETRIES) {
            logger.warn(`Retry attempt ${attempt + 1} for image ${imageId}`, {
                error: error.message,
                details: error.details // Include additional error context
            });
            await sleep(CONSTANTS.RETRY_DELAY_MS * attempt);
            return processImageWithRetry(message, attempt + 1);
        }

        // Use a single update instead of transaction if in error state
        try {
            await dynamoDbDocumentClient.send(new UpdateCommand({
                TableName: process.env.TASKS_TABLE,
                Key: {
                    JobID: jobId,
                    TaskID: imageId
                },
                UpdateExpression: 'SET TaskStatus = :status, Evaluation = :evaluation, UpdatedAt = :updatedAt, ErrorMessage = :errorMessage, ErrorDetails = :errorDetails',
                ExpressionAttributeValues: {
                    ':status': CONSTANTS.PROCESSING_STATUSES.FAILED,
                    ':evaluation': CONSTANTS.PROCESSING_STATUSES.FAILED,
                    ':updatedAt': new Date().toISOString(),
                    ':errorMessage': error.message,
                    ':errorDetails': error.details || {}
                }
            }));

            // If we have a hash, update its status separately
            if (imageHash) {
                await dynamoDbDocumentClient.send(new UpdateCommand({
                    TableName: process.env.IMAGE_HASH_TABLE,
                    Key: {
                        HashValue: imageHash,
                        JobId: jobId
                    },
                    UpdateExpression: 'SET ProcessingStatus = :status',
                    ExpressionAttributeValues: {
                        ':status': CONSTANTS.PROCESSING_STATUSES.FAILED
                    }
                }));
            }
        } catch (updateError) {
            logger.error('Failed to update error status', {
                updateError,
                originalError: error,
                imageId,
                jobId
            });
        }

        return {
            success: false,
            imageId,
            s3ObjectKey,
            error: error.message,
            details: error.details,
            attempts: attempt
        };
    }
}

async function processImageWithTimeout(message, context) {  // Add context parameter
    // For local development/testing, use a default timeout
    const defaultTimeout = 60000; // 1 minute
    const LAMBDA_TIMEOUT_BUFFER = 10000; // 10 seconds buffer

    // Use context.getRemainingTimeInMillis() if available, otherwise use default
    const timeout = context?.getRemainingTimeInMillis
        ? (context.getRemainingTimeInMillis() - LAMBDA_TIMEOUT_BUFFER)
        : defaultTimeout;

    return Promise.race([
        processImageWithRetry(message),
        new Promise((_, reject) => {
            setTimeout(() => {
                reject(new AppError('Processing timeout exceeded', 408));
            }, timeout);
        })
    ]);
}

exports.handler = async (event, context) => {
    console.log('----> Event:', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context?.awsRequestId });

    const results = await Promise.allSettled(Records.map(async (record) => {
        try {
            const body = JSON.parse(record.body);
            const message = JSON.parse(body.Message);
            console.log('----> Message:', message);

            const result = await processImageWithTimeout(message, context);  // Pass context here
            if (result === undefined) {
                throw new Error('processImageWithRetry returned undefined');
            }
            return result;
        } catch (error) {
            logger.error('Error processing record', { error: error.message, stack: error.stack, record });
            return { success: false, error: error.message, stack: error.stack };
        }
    }));

    console.log('----> Results:', results);

    logger.info('Image processing finished', {
        message: `Processed ${Records.length} images`,
        results
    });
};

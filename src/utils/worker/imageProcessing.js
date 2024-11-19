const { DetectLabelsCommand, DetectTextCommand, RekognitionClient } = require('@aws-sdk/client-rekognition');
const dynamoService = require('../../services/dynamoService');

const rekognitionClient = new RekognitionClient({ region: process.env.AWS_REGION });

async function labelDetection({ bucket, s3ObjectKey }) {
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

async function detectTextsFromImage({ bucket, s3ObjectKey }) {
    // Perform text detection
    console.log('Performing text detection...');
    const detectTextCommand = new DetectTextCommand({
        Image: {
            S3Object: {
                Bucket: bucket,
                Name: s3ObjectKey,
            },
        },
    });

    const rekognitionResult = await rekognitionClient.send(detectTextCommand);
    const text = rekognitionResult.TextDetections || [];
    console.log('Text detection:', text);

    return text;
}

async function checkAndStoreImageHash(hash, jobId, imageId, s3Key, maxRetries = 3) {
    console.log('Checking and storing image hash...', { hash, jobId, imageId, s3Key });
    let retryCount = 2;
    let lastError;

    while (retryCount <= maxRetries) {
        try {
            const hashItem = await dynamoService.getImageHash(hash, jobId);

            if (hashItem) {
                console.log('Hash already exists, this is a duplicate: ', {
                    originalImageId: hashItem.ImageId,
                    originalImageS3Key: hashItem.ImageS3Key
                });
                return {
                    isDuplicate: true,
                    originalImageId: hashItem.ImageId,
                    originalImageS3Key: hashItem.ImageS3Key
                };
            }

            // If no item found, store the new hash
            const response = await dynamoService.putImageHash(hash, jobId, imageId, s3Key);

            console.log('Successfully stored new hash', response);

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

    console.log('Error checking and storing image hash after retries', {
        error: lastError,
        retryAttempts: retryCount
    });
    throw lastError;
}


async function duplicateImageDetection({ bucket, s3ObjectKey, jobId, imageId, expiresAt }) {
    const { calculateImageHash } = require('../helpers');

    // Calculate image hash
    const imageHash = await calculateImageHash(bucket, s3ObjectKey);
    console.log('Calculated image hash:', imageHash);
    console.log('expiresAt', expiresAt);

    // Check and store the hash
    const {
        isDuplicate,
        originalImageId,
        originalImageS3Key
    } = await checkAndStoreImageHash(imageHash, jobId, imageId, s3ObjectKey);

    if (isDuplicate) {
        console.log('Duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });
        const response = await dynamoService.updateTaskStatusAsDuplicate({
            jobId,
            imageId,
            s3ObjectKey,
            originalImageId,
            originalImageS3Key,
            expirationTime: expiresAt
        });
        console.log('Task status updated to COMPLETED and marked as duplicate', response);
        return true;
    } else {
        console.log('No duplicate image found', { originalImageId, originalImageS3Key, currentImageId: imageId });
        return false;
    }
}

module.exports = {
    labelDetection,
    detectTextsFromImage,
    duplicateImageDetection
};

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const JSZip = require('jszip');
const stream = require('stream');
const logger = require('../utils/logger');

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const s3Client = new S3Client();

exports.handler = async (event) => {
    logger.info('Delivery processor started', { event });

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        const jobId = message.jobId;

        logger.info(`Processing delivery for job ${jobId}`);

        try {
            // Fetch eligible tasks
            const eligibleTasks = await fetchEligibleTasks(jobId);

            if (eligibleTasks.length === 0) {
                logger.warn(`No eligible tasks found for job ${jobId}`);
                continue;
            }

            const userId = await fetchUserIdFromJobProgress(jobId);

            if (!userId) {
                logger.error(`User ID not found for job ${jobId}`);
                continue;
            }

            logger.info(`Found ${eligibleTasks.length} eligible tasks for job ${jobId}`);

            // Create zip file
            const zip = new JSZip();
            let processedImages = 0;
            let failedImages = 0;

            for (const task of eligibleTasks) {
                try {
                    const imageData = await getImageFromS3(task.ImageS3Key);
                    const fileName = task.ImageS3Key.split('/').pop();
                    zip.file(fileName, imageData);
                    processedImages++;
                    logger.debug(`Added image to zip: ${fileName}`);
                } catch (error) {
                    failedImages++;
                    logger.error(`Failed to process image for task`, { taskId: task.TaskID, imageKey: task.ImageS3Key, error: error.message });
                }
            }

            logger.info(`Processed ${processedImages} images, ${failedImages} failed`);

            if (processedImages === 0) {
                throw new Error(`No images were successfully processed for job ${jobId}`);
            }

            // Generate zip file
            logger.info(`Generating zip file for job ${jobId}`);
            const zipBuffer = await zip.generateAsync({ type: 'nodebuffer' });

            // Store zip file in S3
            const zipKey = `download/${userId}/${jobId}/images.zip`;
            await uploadToS3(zipBuffer, zipKey);

            logger.info(`Delivery processed for job ${jobId}. Zip file stored at ${zipKey}`);
        } catch (error) {
            logger.error(`Error processing delivery for job ${jobId}:`, { error: error.message, stack: error.stack });
        }
    }

    logger.info('Delivery processor finished');
};

async function fetchEligibleTasks(jobId) {
    logger.info(`Fetching eligible tasks for job ${jobId}`);
    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'Evaluation = :evaluation',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':evaluation': 'ELIGIBLE'
        }
    };

    try {
        const command = new QueryCommand(params);
        const result = await docClient.send(command);
        logger.info(`Fetched ${result.Items.length} eligible tasks for job ${jobId}`);
        return result.Items;
    } catch (error) {
        logger.error(`Error fetching eligible tasks for job ${jobId}:`, { error: error.message, params });
        throw error;
    }
}

async function getImageFromS3(key) {
    logger.debug(`Fetching image from S3: ${key}`);
    const getObjectParams = {
        Bucket: process.env.BUCKET_NAME,
        Key: key
    };

    try {
        const command = new GetObjectCommand(getObjectParams);
        const response = await s3Client.send(command);
        const buffer = await streamToBuffer(response.Body);
        logger.debug(`Successfully fetched image from S3: ${key}`);
        return buffer;
    } catch (error) {
        logger.error(`Error fetching image from S3: ${key}`, { error: error.message });
        throw error;
    }
}

function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', (error) => {
            logger.error('Error in stream to buffer conversion', { error: error.message });
            reject(error);
        });
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function uploadToS3(buffer, key) {
    logger.info(`Uploading zip file to S3: ${key}`);
    const putObjectParams = {
        Bucket: process.env.BUCKET_NAME,
        Key: key,
        Body: buffer
    };

    try {
        const command = new PutObjectCommand(putObjectParams);
        await s3Client.send(command);
        logger.info(`Successfully uploaded zip file to S3: ${key}`);
    } catch (error) {
        logger.error(`Error uploading zip file to S3: ${key}`, { error: error.message });
        throw error;
    }
}

async function fetchUserIdFromJobProgress(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'UserId'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);
        return result.Item ? result.Item.UserId : null;
    } catch (error) {
        logger.error('Error fetching userId from JobProgress:', { error, jobId });
        throw error;
    }
}
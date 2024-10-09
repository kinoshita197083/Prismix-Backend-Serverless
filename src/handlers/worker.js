const sharp = require('sharp');
const s3Service = require('../services/s3Service');
const sqsService = require('../services/sqsService');
const dynamoService = require('../services/dynamoService');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');

const { RekognitionClient } = require("@aws-sdk/client-rekognition");

const rekognition = new RekognitionClient();
exports.handler = async (event, context) => {
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    for (const record of Records) {
        let bucket, s3ObjectKey, projectId, jobId, projectSettings, imageId, taskId;

        // Parse the record based on its source
        if (record.eventSource === 'aws:sqs') {
            const body = JSON.parse(record.body);
            bucket = body.bucket;
            s3ObjectKey = body.key;
            projectId = body.projectId;
            jobId = body.jobId;
            projectSettings = body.settingValue;
            taskId = body.taskId; // Assuming taskId is passed in the message
        } else {
            // Local test event parsing
            const parsed = JSON.parse(record.body);
            bucket = parsed.bucket;
            s3ObjectKey = parsed.key;
            const [, , receivedProjectId, , receivedJobId, receivedImageId] = s3ObjectKey.split('/');
            projectId = receivedProjectId;
            jobId = receivedJobId;
            imageId = receivedImageId;
            taskId = `task-${imageId}`; // Generate a taskId if not provided
        }

        logger.info('Processing image', { bucket, key: s3ObjectKey, projectId, jobId, imageId, taskId });

        try {
            // Update task status to PROCESSING
            await updateTaskStatus(jobId, taskId, 'PROCESSING');

            // Download the image from S3
            // const s3Object = await s3Service.getFile({
            //     Bucket: bucket,
            //     Key: s3ObjectKey
            // });

            // Perform object detection
            const rekognitionResult = await rekognition.detectLabels({
                Image: {
                    S3Object: {
                        Bucket: bucket,
                        Name: s3ObjectKey,
                    },
                },
                MaxLabels: 10,
                MinConfidence: 70,
            }).promise();

            const labels = rekognitionResult.Labels;

            logger.info('Rekognition Result', { labels, imageId, jobId, taskId });

            // Here you would implement your filtering logic based on projectSettings and labels

            // Update task status to COMPLETED and store results
            await updateTaskStatus(jobId, taskId, 'COMPLETED', labels);

            logger.info('Successfully processed image', { imageId, projectId, jobId, taskId });
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
                projectId,
                jobId,
                imageId,
                taskId
            });

            // Update task status to FAILED
            await updateTaskStatus(jobId, taskId, 'FAILED', { error: error.message });

            // Send error to Dead Letter Queue
            await sqsService.sendMessage({
                QueueUrl: process.env.DEAD_LETTER_QUEUE_URL,
                MessageBody: JSON.stringify({
                    error: error.message,
                    originalMessage: record
                })
            });
        }
    }
};

async function updateTaskStatus(jobId, taskId, status, result = null) {
    const updateParams = {
        TableName: process.env.TASKS_TABLE,
        Key: {
            JobID: jobId,
            TaskID: taskId
        },
        UpdateExpression: 'SET TaskStatus = :status, UpdatedAt = :time',
        ExpressionAttributeValues: {
            ':status': status,
            ':time': new Date().toISOString()
        }
    };

    if (result) {
        updateParams.UpdateExpression += ', ProcessingResult = :result';
        updateParams.ExpressionAttributeValues[':result'] = result;
    }

    try {
        await dynamoService.updateItem(updateParams);
        logger.info(`Updated task status`, { jobId, taskId, status });
    } catch (error) {
        logger.error('Error updating task status in DynamoDB', { error: error.message, jobId, taskId, status });
        throw new AppError('Failed to update task status', 500);
    }
}
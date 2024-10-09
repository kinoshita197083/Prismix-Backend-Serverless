const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');
const { RekognitionClient, DetectLabelsCommand } = require('@aws-sdk/client-rekognition');
const { PrismaClient } = require('@prisma/client');
const sharp = require('sharp');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');
const dynamoService = require('../services/dynamoService');

const s3Client = new S3Client();
const sqsClient = new SQSClient();
const rekognitionClient = new RekognitionClient();
const prisma = new PrismaClient();

exports.handler = async (event, context) => {
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });
    logger.info('----> Event', event);

    for (const record of Records) {
        let bucket, object, projectId, jobId, imageId;

        console.log('----> Original Each Record', record);

        // Check if this is a real SQS event or our local test event
        if (record.eventSource === 'aws:sqs') {
            const body = JSON.parse(record.body);
            bucket = body.bucket;
            object = body.object;
            projectId = body.projectId;
            jobId = body.jobId;
            imageId = body.imageId;
        } else {
            // This is our local test event
            const parsedRecord = JSON.parse(record.body);

            console.log('777 Parsed Record', parsedRecord);
            const { bucket: parsedBucket, object: parsedObject } = parsedRecord;
            const objectKey = parsedObject.key;
            const [type, userId, receivedProjectId, receivedProjectSettingId, receivedJobId, receivedImageId] = objectKey.split('/');
            projectId = receivedProjectId;
            jobId = receivedJobId;
            imageId = receivedImageId;
            bucket = parsedBucket.name;
            object = parsedObject;
        }

        // logger.info('777 Processing image', { bucket, key: object.key, projectId, jobId, imageId });
        console.log('777 Processing image', { bucket, key: object.key, projectId, jobId, imageId });

        try {
            // // Fetch project settings
            // const projectSettings = await prisma.projectSetting.findUnique({
            //     where: { id: projectSettingId },
            // });

            // Download the image from S3
            // const getObjectCommand = new GetObjectCommand({
            //     Bucket: bucket,
            //     Key: object.key
            // });

            // const s3Object = await s3Client.send(getObjectCommand);

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

            console.log('777 Rekognition Result', labels);

            // logger.info('Rekognition Result', { result: labels, imageId, jobId });

            // Here you would implement your filtering logic based on projectSettings and labels

            // Update task status to COMPLETED and store results
            await dynamoService.updateTaskStatus({
                jobId,
                imageS3Key: object.key,
                taskId: imageId,
                status: 'COMPLETED',
                labels,
                evaluation: evaluate(labels, {})
            });

            // logger.info('Successfully processed image', { imageId, projectId, jobId });
            console.log('777 Successfully processed image', { imageId, projectId, jobId });
        } catch (error) {
            console.log('777 Error', error);

            // logger.error('Error processing image', {
            //     error: error.message,
            //     stack: error.stack,
            //     bucket,
            //     key: object.key,
            // });

            // Update task status to FAILED
            await dynamoService.updateTaskStatus({
                jobId,
                imageS3Key: object.key,
                taskId: imageId,
                status: 'FAILED',
            });

            console.log('777 Error processing image')

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
    console.log('----> Evaluating ....');
    return 'ELIGIBLE';
}
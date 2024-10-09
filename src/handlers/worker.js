const AWS = require('aws-sdk');
const sharp = require('sharp');
const s3Service = require('../services/s3Service');
const sqsService = require('../services/sqsService');
const dynamoService = require('../services/dynamoService');
const logger = require('../utils/logger');
const { AppError, handleError } = require('../utils/errorHandler');

const rekognition = new AWS.Rekognition();
const prisma = new PrismaClient();

exports.handler = async (event, context) => {
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    for (const record of Records) {
        const { bucket, object } = record.s3;
        logger.info('S3 Record: ', record.s3);
        const { projectId, jobId, imageId } = JSON.parse(record.body);

        logger.info('Processing image', { bucket: bucket.name, key: object.key, projectId, jobId, imageId });

        try {
            // Fetch project settings
            const projectSettings = await prisma.projectSetting.findMany({
                where: { projectId },
            });

            // Download the image from S3
            const s3Object = await s3Service.getFile({
                Bucket: bucket.name,
                Key: object.key
            });

            // Resize the image
            // const resizedImage = await sharp(s3Object)
            //     .resize({ width: 1024, height: 1024, fit: 'inside' })
            //     .toBuffer();

            // Upload resized image
            // const resizedKey = `resized-${object.key}`;
            // await s3Service.uploadFile({
            //     Bucket: bucket.name,
            //     Key: resizedKey,
            //     Body: resizedImage,
            //     ContentType: 'image/jpeg'
            // });

            // Perform object detection
            const rekognitionResult = await rekognition.detectLabels({
                Image: {
                    S3Object: {
                        Bucket: bucket.name,
                        Name: resizedKey,
                    },
                },
                MaxLabels: 10,
                MinConfidence: 70,
            }).promise();

            const labels = rekognitionResult.Labels;

            logger.info('Rekognition Result: ', rekognitionResult);

            // Apply filtering logic
            // const includeImage = applyFilteringLogic(labels, projectSettings);

            // Update DynamoDB tables
            // await updateDynamoDBTables(imageId, projectId, includeImage, labels);

            // Update job progress
            // await updateJobProgress(projectId, jobId);

            logger.info('Successfully processed image', { imageId, projectId, jobId });
        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket: bucket.name,
                key: object.key,
                projectId,
                jobId,
                imageId
            });

            // Pass the error to the Dead Letter Queue
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

// function applyFilteringLogic(labels, projectSettings) {
//     // Implement your filtering logic here based on project settings and labels
//     // Return true if the image should be included, false otherwise
//     // This is a placeholder implementation
//     return true;
// }

// async function updateDynamoDBTables(imageId, projectId, includeImage, labels) {
//     const imageItem = {
//         imageId,
//         jobId: projectId,
//         included: includeImage,
//         labels: labels.map(label => label.Name),
//         createdAt: new Date().toISOString(),
//     };

//     await dynamoService.putItem({
//         TableName: process.env.IMAGES_TABLE,
//         Item: imageItem,
//     });

//     const resultItem = {
//         imageId,
//         jobId: projectId,
//         included: includeImage,
//         labels: labels,
//     };

//     await dynamoService.putItem({
//         TableName: process.env.PROCESSED_RESULTS_TABLE,
//         Item: resultItem,
//     });
// }

// async function updateJobProgress(projectId, jobId) {
//     try {
//         const job = await prisma.job.findFirst({
//             where: { projectId, id: jobId },
//         });

//         if (job) {
//             await prisma.job.update({
//                 where: { id: job.id },
//                 data: {
//                     processedImageCount: { increment: 1 },
//                     jobStatus: job.processedImageCount + 1 >= job.imageCount ? 'COMPLETED' : 'IN_PROGRESS',
//                 },
//             });
//         }
//     } catch (error) {
//         logger.error('Error updating job progress', { error: error.message, projectId, jobId });
//         throw new AppError('Failed to update job progress', 500);
//     }
// }
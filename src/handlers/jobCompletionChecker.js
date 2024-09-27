const AWS = require('aws-sdk');
const { PrismaClient } = require('@prisma/client');

const dynamodb = new AWS.DynamoDB.DocumentClient();
const lambda = new AWS.Lambda();
const prisma = new PrismaClient();

exports.handler = async (event) => {
    for (const record of event.Records) {
        if (record.eventName === 'INSERT' || record.eventName === 'MODIFY') {
            const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

            // Check if this is the last image for the job
            const { jobId } = newImage;

            const job = await prisma.job.findUnique({
                where: { id: jobId },
                include: { project: true },
            });

            if (!job) continue;

            const processedImagesCount = await dynamodb.query({
                TableName: process.env.IMAGES_TABLE,
                IndexName: 'JobImageIndex',
                KeyConditionExpression: 'jobId = :jobId',
                ExpressionAttributeValues: { ':jobId': jobId },
                Select: 'COUNT',
            }).promise();

            if (processedImagesCount.Count === job.imageCount && job.jobStatus !== 'COMPLETED') {
                // Update job status
                await prisma.job.update({
                    where: { id: jobId },
                    data: { jobStatus: 'COMPLETED' },
                });

                // Trigger ZIP creation
                await lambda.invoke({
                    FunctionName: process.env.CREATE_ZIP_LAMBDA,
                    InvocationType: 'Event',
                    Payload: JSON.stringify({ userId: job.project.userId, projectId: job.projectId, jobId }),
                }).promise();
            }
        }
    }
};
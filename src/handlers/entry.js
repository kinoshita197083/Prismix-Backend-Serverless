const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const { PrismaClient } = require('@prisma/client');

const s3 = new AWS.S3();
const prisma = new PrismaClient();

exports.handler = async (event) => {
    try {
        const { userId, projectId, projectName, projectDescription, imageCount } = JSON.parse(event.body);

        // Debugging - example using logger
        logger.info('Lambda function invoked', {
            functionName: context.functionName,
            awsRequestId: context.awsRequestId,
            event
        });

        // Create or update a project
        const [project, job] = await prisma.$transaction([
            prisma.project.upsert({
                where: {
                    id: projectId, // PK
                },
                update: {
                    projectName,
                    projectDescription,
                    updatedAt: new Date().toISOString(),
                    userId,
                },
                create: {
                    userId,
                    projectName,
                    projectDescription,
                },
            }),
            prisma.job.create({
                data: {
                    userId,
                    projectId,
                    imageCount,
                    jobStatus: 'INPROGRESS',
                },
            }),
        ]);

        // Generate pre-signed URLs for image uploads
        const presignedUrls = [];
        for (let i = 0; i < imageCount; i++) {
            const imageId = uuidv4();
            const params = {
                Bucket: process.env.BUCKET_NAME,
                Key: `${userId}/${project.id}/${job.id}/${imageId}`,
                Expires: 3600, // URL expires in 1 hour
                ContentType: 'image/*',
            };
            const uploadUrl = await s3.getSignedUrlPromise('putObject', params);
            presignedUrls.push({ imageId, uploadUrl });
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Project created and pre-signed URLs generated',
                projectId: project.id,
                presignedUrls,
            }),
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal server error' }),
        };
    }
};
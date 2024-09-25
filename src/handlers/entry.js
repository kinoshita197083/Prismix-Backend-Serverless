const AWS = require('aws-sdk');
const { PrismaClient } = require('@prisma/client');

const s3 = new AWS.S3();
const prisma = new PrismaClient();

exports.handler = async (event) => {
    try {
        const { userId, projectName, projectDescription, imageCount } = JSON.parse(event.body);

        // Create or update project in PostgreSQL
        const project = await prisma.project.upsert({
            where: {
                userId_projectName: {
                    userId: userId,
                    projectName: projectName
                }
            },
            update: { projectDescription },
            create: {
                userId,
                projectName,
                projectDescription,
                dynamodbReference: `project_${Date.now()}`
            },
        });

        // Generate pre-signed URLs for S3 upload
        const preSignedUrls = [];
        for (let i = 0; i < imageCount; i++) {
            const key = `${userId}/${project.id}/${Date.now()}_${i}.jpg`;
            const url = s3.getSignedUrl('putObject', {
                Bucket: process.env.BUCKET_NAME,
                Key: key,
                Expires: 3600, // URL expires in 1 hour
                ContentType: 'image/jpeg'
            });
            preSignedUrls.push({ url, key });
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Project created/updated and pre-signed URLs generated',
                projectId: project.id,
                preSignedUrls
            })
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal server error' })
        };
    }
};
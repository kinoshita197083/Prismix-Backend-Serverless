const AWS = require('aws-sdk');
const { PrismaClient } = require('@prisma/client');
const { parseBody } = require('../utils/helpers');
const { getUserId } = require('../utils/user');

const s3 = new AWS.S3();
const prisma = new PrismaClient();

exports.handler = async (event) => {
    try {
        const userId = getUserId(event);
        const { projectId, projectName, projectDescription } = parseBody(event.body);

        // Create or update a project
        const project = await prisma.project.create({
            data: {
                userId,
                projectId,
                projectName,
                projectDescription,
            },
        })

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Project created successfully',
                projectId: project.id,
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
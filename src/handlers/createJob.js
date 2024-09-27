const AWS = require('aws-sdk');
const { PrismaClient } = require('@prisma/client');
const { parseBody } = require('../utils/helpers');
const { getUserId } = require('../utils/user');

const prisma = new PrismaClient();

exports.createJob = async (event) => {
    try {
        const userId = await getUserId(event);
        const projectId = event.pathParameters.id;
        const { imageCount } = parseBody(event.body);

        const project = await prisma.project.findUnique({
            where: { id: projectId, userId },
        });

        if (!project) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Project not found or unauthorized' }),
            };
        }

        const job = await prisma.job.create({
            data: {
                projectId,
                imageCount,
                jobStatus: 'IDLE',
            },
        });

        return {
            statusCode: 201,
            body: JSON.stringify(job),
        };
    } catch (error) {
        console.error('Error creating job:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error creating job' }),
        };
    }
};

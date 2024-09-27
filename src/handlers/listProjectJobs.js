const AWS = require('aws-sdk');
const { PrismaClient } = require('@prisma/client');
const { parseBody } = require('../utils/helpers');
const { getUserId } = require('../utils/user');

const prisma = new PrismaClient();

// GET /projects/{id}/jobs - List jobs for a project
exports.listProjectJobs = async (event) => {
    try {
        const userId = await getUserId(event);
        const projectId = event.pathParameters.id;

        const project = await prisma.project.findUnique({
            where: { id: projectId, userId },
        });

        if (!project) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Project not found or unauthorized' }),
            };
        }

        const jobs = await prisma.job.findMany({
            where: { projectId },
            orderBy: { createdAt: 'desc' },
        });

        return {
            statusCode: 200,
            body: JSON.stringify(jobs),
        };
    } catch (error) {
        console.error('Error listing project jobs:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error listing project jobs' }),
        };
    }
};
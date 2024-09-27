const { getUserId } = require("../utils/user");
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

exports.handler = async (event) => {
    const userId = await getUserId(event);
    const jobId = event.pathParameters.id;

    try {
        const job = await prisma.job.findUnique({
            where: { id: jobId },
            select: { jobStatus: true, processedImageCount: true, imageCount: true },
        });

        if (!job) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Job not found' }),
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                jobStatus: job.jobStatus,
                processedImageCount: job.processedImageCount,
                totalImageCount: job.imageCount,
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
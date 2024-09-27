const { getUserId } = require("../utils/user");
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

// GET /projects/{id} - Get project details
exports.getProject = async (event) => {
    try {
        const userId = getUserId(event);
        const projectId = event.pathParameters.id;

        const project = await prisma.project.findFirst({
            where: {
                id: projectId,
                userId,
            },
            include: {
                jobs: true,
                projectSettings: true,
            },
        });

        if (!project) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Project not found' }),
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify(project),
        };
    } catch (error) {
        console.error('Error getting project:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error getting project details' }),
        };
    }
};
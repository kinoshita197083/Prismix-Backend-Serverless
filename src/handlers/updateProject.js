const { getUserId } = require("../utils/user");
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

// PUT /projects/{id} - Update project details
exports.updateProject = async (event) => {
    try {
        const userId = getUserId(event);
        const projectId = event.pathParameters.id;
        const { projectName, projectDescription } = parseBody(event.body);

        const updatedProject = await prisma.project.updateMany({
            where: {
                id: projectId,
                userId,
            },
            data: {
                projectName,
                projectDescription,
            },
        });

        if (updatedProject.count === 0) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Project not found or unauthorized' }),
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Project updated successfully' }),
        };
    } catch (error) {
        console.error('Error updating project:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error updating project' }),
        };
    }
};
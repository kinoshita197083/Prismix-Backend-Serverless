const { getUserId } = require("../utils/user");
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

// DELETE /projects/{id} - Delete a project
exports.deleteProject = async (event) => {
    try {
        const userId = getUserId(event);
        const projectId = event.pathParameters.id;

        const deletedProject = await prisma.project.deleteMany({
            where: {
                id: projectId,
                userId,
            },
        });

        if (deletedProject.count === 0) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: 'Project not found or unauthorized' }),
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Project deleted successfully' }),
        };
    } catch (error) {
        console.error('Error deleting project:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error deleting project' }),
        };
    }
};
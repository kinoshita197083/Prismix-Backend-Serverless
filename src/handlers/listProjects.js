const { getUserId } = require("../utils/user");

// GET /projects - List user's projects
exports.listProjects = async (event) => {
    try {
        const userId = getUserId(event);

        const projects = await prisma.project.findMany({
            where: { userId },
            select: {
                id: true,
                projectName: true,
                projectDescription: true,
                createdAt: true,
            },
        });

        return {
            statusCode: 200,
            body: JSON.stringify(projects),
        };
    } catch (error) {
        console.error('Error listing projects:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Error listing projects' }),
        };
    }
};
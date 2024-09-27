// Helper function to parse and validate the event body
exports.parseBody = (body) => {
    try {
        return JSON.parse(body);
    } catch (e) {
        throw new Error('Invalid request body');
    }
};
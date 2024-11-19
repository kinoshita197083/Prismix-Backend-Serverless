const dynamoService = require("../../services/dynamoService");


async function fetchProjectSettingRules(jobId) {
    console.log('Fetching project setting rules...', { jobId });
    try {
        dynamoService
        const response = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        console.log('Fetch project setting rules response:', response);
        return response?.projectSetting;
    } catch (error) {
        // logger.error('Error fetching project setting rules', { error, jobId });
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}

async function fetchExpiresAt(jobId) {
    try {
        const jobProgress = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        return jobProgress?.expiresAt;
    } catch (error) {
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}

module.exports = {
    fetchProjectSettingRules,
    fetchExpiresAt
};

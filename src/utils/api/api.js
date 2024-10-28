const dynamoService = require('../services/dynamoService');

async function fetchProjectSettingRules(jobId) {
    console.log('Fetching project setting rules...', { jobId });
    try {
        const response = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        console.log('Fetch project setting rules response:', response);
        return response?.projectSetting;
    } catch (error) {
        // logger.error('Error fetching project setting rules', { error, jobId });
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}

module.exports = {
    fetchProjectSettingRules
};

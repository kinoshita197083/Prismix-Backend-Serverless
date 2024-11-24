const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
);

const supabaseService = {
    async updateJobStatus(jobId, status) {
        const { data, error } = await supabase.from('Job').update({ jobStatus: status }).eq('id', jobId).single();
        if (error) {
            logger.error('[SupabaseService] Error updating job status', { error, jobId, status });
            throw new AppError('[SupabaseService] Failed to update job status', 500);
        }
        return data;
    },

    async getUserData(userId) {
        const { data, error } = await supabase.from('User').select('name, preferredName, email, s3Connection').eq('id', userId).single();
        if (error) {
            logger.error('[SupabaseService] Error fetching user data', { error, userId });
            throw new AppError('[SupabaseService] Failed to fetch user data', 500);
        }
        return data;
    }
};

module.exports = supabaseService;
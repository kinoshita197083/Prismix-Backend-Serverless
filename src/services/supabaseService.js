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
        const { data, error } = await supabase.from('User').select('*').eq('id', userId).single();
        if (error) {
            logger.error('[SupabaseService] Error fetching user data', { error, userId });
            throw new AppError('[SupabaseService] Failed to fetch user data', 500);
        }
        return data;
    },

    async refundUserCreditBalance(userId, amount, reason) {
        // Increment user credits
        const { data: userData, error: userError } = await supabase
            .from('User')
            .update({ credits: supabase.rpc('increment', { amount, column: 'credits' }) })
            .eq('id', userId)
            .select();

        if (userError) {
            logger.error('[SupabaseService] Error refunding user credit balance', { error: userError, userId, amount });
            throw new AppError('[SupabaseService] Failed to refund user credit balance', 500);
        }

        // Record refund transaction
        const { error: transactionError } = await supabase
            .from('CreditTransaction')
            .insert({
                userId,
                credits: amount,
                type: 'refund',
                description: reason || 'Refund for failed & skipped jobs'
            });

        if (transactionError) {
            logger.error('[SupabaseService] Error recording refund transaction', { error: transactionError, userId, amount });
            throw new AppError('[SupabaseService] Failed to record refund transaction', 500);
        }

        return userData;
    },


    async getAllCreditsTransactionsOfJob(jobId) {
        const { data, error } = await supabase.from('CreditTransaction').select('*').eq('jobId', jobId);
        if (error) {
            logger.error('[SupabaseService] Error fetching all credits transactions of job', { error, jobId });
            throw new AppError('[SupabaseService] Failed to fetch all credits transactions of job', 500);
        }
        return data;
    }
};

module.exports = supabaseService;
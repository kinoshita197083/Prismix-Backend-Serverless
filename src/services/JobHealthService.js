const MEMORY_THRESHOLD = 0.8;

const createJobHealthService = (jobProgressService) => {
    const performHealthCheck = async (jobId) => {
        const healthStatus = {
            jobId,
            timestamp: Date.now().toString(),
            checks: {}
        };

        try {
            const startTime = Date.now();
            await jobProgressService.getCurrentJobProgress(jobId);
            healthStatus.checks.dynamodb = {
                status: 'OK',
                latency: Date.now() - startTime
            };
            console.log('Health check completed for jobId:', jobId);
        } catch (error) {
            healthStatus.checks.dynamodb = {
                status: 'ERROR',
                error: error.message
            };
            console.error('Health check failed for jobId:', jobId, error);
        }

        const memoryUsage = process.memoryUsage();
        healthStatus.checks.memory = {
            status: memoryUsage.heapUsed / memoryUsage.heapTotal < MEMORY_THRESHOLD ? 'OK' : 'WARNING',
            usage: {
                used: Math.round(memoryUsage.heapUsed / 1024 / 1024),
                total: Math.round(memoryUsage.heapTotal / 1024 / 1024),
                percentage: Math.round((memoryUsage.heapUsed / memoryUsage.heapTotal) * 100)
            }
        };

        return healthStatus;
    };

    const checkMemoryUsage = async (jobId, lastEvaluatedKey) => {
        const used = process.memoryUsage().heapUsed;
        const total = process.memoryUsage().heapTotal;
        const percentage = used / total;

        return percentage > MEMORY_THRESHOLD;
    };

    return {
        performHealthCheck,
        checkMemoryUsage
    };
};

module.exports = { createJobHealthService }; 
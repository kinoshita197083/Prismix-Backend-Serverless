const { JobProcessingError } = require('../utils/errors')

const createCircuitBreaker = (jobId, jobProgressService) => {
    const getState = async () => {
        try {
            const result = await jobProgressService.getCircuitBreakerState(jobId);
            return result || {
                state: 'CLOSED',
                failures: 0,
                lastFailure: null
            };
        } catch (error) {
            console.error('Error fetching circuit breaker state:', error);
            return {
                state: 'CLOSED',
                failures: 0,
                lastFailure: null
            };
        }
    };

    const setState = async (newState) =>
        await jobProgressService.updateCircuitBreakerState(jobId, newState);

    const execute = async (operation) => {
        const state = await getState();

        if (state.state === 'OPEN' &&
            Date.now() - state.lastFailure < 30000) {
            throw new JobProcessingError(
                'CIRCUIT_BREAKER_OPEN',
                'Circuit breaker is OPEN',
                { cooldownRemaining: 30000 - (Date.now() - state.lastFailure) }
            );
        }

        if (state.state === 'OPEN') {
            await setState({
                state: 'HALF_OPEN',
                lastFailure: Date.now(),
                failures: state.failures
            });
        }

        try {
            const result = await operation();
            if (state.state === 'HALF_OPEN') {
                await setState({
                    state: 'CLOSED',
                    failures: 0,
                    lastFailure: null
                });
            }
            return result;
        } catch (error) {
            const newFailures = state.failures + 1;
            await setState({
                state: newFailures >= 5 ? 'OPEN' : state.state,
                failures: newFailures,
                lastFailure: Date.now()
            });
            throw error;
        }
    };

    return { execute };
};

module.exports = { createCircuitBreaker }; 
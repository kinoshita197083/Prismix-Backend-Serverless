const logger = require('./logger');

class AppError extends Error {
    constructor(message, statusCode) {
        super(message);
        this.statusCode = statusCode;
        this.isOperational = true;
        Error.captureStackTrace(this, this.constructor);
    }
}

const handleError = (err, lambdaContext) => {
    logger.error({
        message: err.message,
        stack: err.stack,
        functionName: lambdaContext.functionName,
        awsRequestId: lambdaContext.awsRequestId,
    });

    if (err instanceof AppError) {
        return {
            statusCode: err.statusCode,
            body: JSON.stringify({ message: err.message }),
        };
    } else {
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Internal server error' }),
        };
    }
};

module.exports = {
    AppError,
    handleError,
};
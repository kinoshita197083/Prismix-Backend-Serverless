const logger = require('./logger');

class AppError extends Error {
    constructor(code, message) {
        super(message);
        this.code = code;
    }
}

const ErrorCodes = {
    IMAGE_PROCESSING: {
        RESIZE_ERROR: 'RESIZE_ERROR',
        FORMAT_ERROR: 'FORMAT_ERROR',
        QUALITY_CHECK_ERROR: 'QUALITY_CHECK_ERROR',
        BLUR_DETECTION_ERROR: 'BLUR_DETECTION_ERROR',
    },
    STORAGE: {
        S3_UPLOAD_ERROR: 'S3_UPLOAD_ERROR',
        S3_DOWNLOAD_ERROR: 'S3_DOWNLOAD_ERROR',
    },
    DETECTION: {
        REKOGNITION_ERROR: 'REKOGNITION_ERROR',
        INVALID_IMAGE: 'INVALID_IMAGE',
    },
    DATABASE: {
        DYNAMO_UPDATE_ERROR: 'DYNAMO_UPDATE_ERROR',
        DYNAMO_QUERY_ERROR: 'DYNAMO_QUERY_ERROR',
    }
};

function handleError(error, context = {}) {
    let errorResponse = {
        success: false,
        error: {
            code: error.code || 'UNKNOWN_ERROR',
            message: error.message,
            context
        }
    };

    // Log error with context
    logger.error('Operation failed', {
        errorCode: error.code,
        errorMessage: error.message,
        context,
        stack: error.stack
    });

    return errorResponse;
}

module.exports = {
    AppError,
    ErrorCodes,
    handleError
};

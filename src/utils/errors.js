class JobProcessingError extends Error {
    constructor(code, message, details = {}) {
        super(message);
        this.name = 'JobProcessingError';
        this.code = code;
        this.details = details;

        // Maintains proper stack trace for where our error was thrown
        Error.captureStackTrace(this, JobProcessingError);
    }
}

module.exports = {
    JobProcessingError
}; 
// Evaluation constants
const ELIGIBLE = 'ELIGIBLE';
const EXCLUDED = 'EXCLUDED';

// Status constants
const PENDING = 'PENDING';
const IN_PROGRESS = 'IN_PROGRESS';
const COMPLETED = 'COMPLETED';
const DUPLICATE = 'DUPLICATE';
const CANCELLED = 'CANCELLED';

// Share constants
const FAILED = 'FAILED';

// Expiration time for image hash in seconds
const IMAGE_HASH_EXPIRATION_TIME = (Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60)).toString() // 3 days
const RANDOM_TIME_DELAY_MAX_EIGHT_SECONDS = Math.min(1000 * Math.pow(2, retryCount), 8000);
const NOW = Date.now().toString()

// Resolution thresholds for image quality
const RESOLUTION_THRESHOLDS = {
    'sd': 480,
    'hd': 720,
    'fhd': 1080,
    '4k': 2160
};

module.exports = {
    ELIGIBLE,
    FAILED,
    EXCLUDED,
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    DUPLICATE,
    CANCELLED,
    IMAGE_HASH_EXPIRATION_TIME,
    RANDOM_TIME_DELAY_MAX_EIGHT_SECONDS,
    NOW,
    RESOLUTION_THRESHOLDS
};

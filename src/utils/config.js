// Evaluation constants
const ELIGIBLE = 'ELIGIBLE';
const EXCLUDED = 'EXCLUDED';

// Status constants
const PENDING = 'PENDING';
const IN_PROGRESS = 'IN_PROGRESS';
const COMPLETED = 'COMPLETED';
const DUPLICATE = 'DUPLICATE';
const CANCELLED = 'CANCELLED';
const WAITING_FOR_REVIEW = 'WAITING_FOR_REVIEW';

// Share constants
const FAILED = 'FAILED';

// Expiration time for image hash in seconds
const IMAGE_HASH_EXPIRATION_TIME = (Math.floor(Date.now() / 1000) + (3 * 24 * 60 * 60)).toString() // 3 days

// Validate and map resizeMode to Sharp's supported fit options
const validFitOptions = {
    'cover': 'cover',
    // 'contain': 'contain',
    'contain': 'inside',
    'fill': 'fill',
    'inside': 'inside',
    'outside': 'outside'
};

// Resolution thresholds for image quality
const RESOLUTION_THRESHOLDS = {
    'sd': 480,
    'hd': 720,
    'fhd': 1080,
    '4k': 2160
};

const DEFAULT_EXPIRATION_TIME = Date.now().toString() + 1000 * 60 * 60 * 24 * 180 // 180 days from now

const PricingTable = {
    expressProcessing: 2,
    removeAllText: 2,
    removeLowResolution: 1,
    removeNoisyImages: 1,
    removeBlurryImages: 1,
    detectDuplicates: 1,
    contentTags: 3,
    textTags: 3,
}

module.exports = {
    ELIGIBLE,
    FAILED,
    EXCLUDED,
    ELIGIBLE,
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    DUPLICATE,
    CANCELLED,
    WAITING_FOR_REVIEW,
    IMAGE_HASH_EXPIRATION_TIME,
    RESOLUTION_THRESHOLDS,
    validFitOptions,
    DEFAULT_EXPIRATION_TIME,
    PricingTable
};

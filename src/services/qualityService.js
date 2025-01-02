const sharp = require('sharp');
const s3Service = require('./s3Service');
const { RESOLUTION_THRESHOLDS } = require('../utils/config');
const { calculateSNR } = require('../utils/helpers');
const logger = require('../utils/logger');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambdaClient = new LambdaClient();

// Increased concurrency while maintaining rate control
const CONCURRENT_QUALITY_CHECKS = 50;  // Increased from 20 to 50
const BASE_DELAY = 50;                 // Reduced from 100ms to 50ms
const MAX_RETRIES = 3;
const MAX_BACKOFF = 1000;             // Reduced from 2000ms to 1000ms

// Adjusted rate limiting window
const requestWindow = {
    timestamps: [],
    windowSize: 1000,    // 1 second window
    maxRequests: 100     // Increased from 50 to 100 requests per second
};

/**
 * Detects image blurriness using the blur detection Lambda
 * @param {Buffer} imageBuffer - Raw image buffer
 * @returns {Promise<number>} Blur score (lower means more blurry)
 */
async function detectBlurriness(imageBuffer, retryCount = 0) {
    try {
        // Check and update rate limiting window
        await checkRateLimit();

        const payload = {
            body: {
                imageBuffer: imageBuffer.toString('base64')
            }
        };

        const command = new InvokeCommand({
            FunctionName: process.env.BLUR_DETECTION_FUNCTION_NAME,
            InvocationType: 'RequestResponse',
            Payload: JSON.stringify(payload)
        });

        const response = await lambdaClient.send(command);
        const result = JSON.parse(Buffer.from(response.Payload).toString());

        if (result.statusCode === 200) {
            const { combinedScore } = JSON.parse(result.body);
            return combinedScore;
        } else {
            const error = JSON.parse(result.body);
            throw new Error(`Blur detection failed: ${error.details || error.error}`);
        }
    } catch (error) {
        if (error.message.includes('Rate Exceeded') && retryCount < MAX_RETRIES) {
            // Calculate dynamic backoff with jitter
            const backoff = Math.min(
                BASE_DELAY * Math.pow(1.5, retryCount) + Math.random() * 100,
                MAX_BACKOFF
            );

            logger.warn('[detectBlurriness] Rate limit hit, retrying...', {
                retryCount: retryCount + 1,
                backoff
            });

            await new Promise(resolve => setTimeout(resolve, backoff));
            return detectBlurriness(imageBuffer, retryCount + 1);
        }
        throw error;
    }
}

// Rate limiting helper function
async function checkRateLimit() {
    const now = Date.now();

    // Remove old timestamps
    requestWindow.timestamps = requestWindow.timestamps.filter(
        timestamp => now - timestamp < requestWindow.windowSize
    );

    // Check if we're at the limit
    if (requestWindow.timestamps.length >= requestWindow.maxRequests) {
        const oldestTimestamp = requestWindow.timestamps[0];
        const waitTime = requestWindow.windowSize - (now - oldestTimestamp);
        if (waitTime > 0) {
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
    }

    // Add current timestamp
    requestWindow.timestamps.push(now);
}

/**
 * Validates image quality based on specified settings
 * @param {Object} params - Validation parameters
 * @param {string} params.bucket - S3 bucket name
 * @param {string} params.key - S3 object key
 * @param {Object} params.settings - Quality check settings
 * @returns {Promise<Object>} Validation results
 */
exports.validateImageQuality = async ({ bucket, key, settings }) => {
    try {
        const imageBuffer = await s3Service.getFileBuffer(bucket, key);
        const metadata = await sharp(imageBuffer).metadata();
        const qualityChecks = [];

        // Group quality checks to minimize concurrent Lambda invocations
        const checks = {
            resolution: settings.checkResolution && (async () => {
                const minHeight = RESOLUTION_THRESHOLDS[settings.minResolution];
                return metadata.height < minHeight ?
                    `Resolution below ${settings.minResolution} standard` : null;
            }),

            blurriness: settings.checkBlurriness && (async () => {
                const blurScore = await detectBlurriness(imageBuffer);
                return blurScore < settings.blurThreshold ?
                    `Image is likely blurry (score: ${blurScore.toFixed(2)}, threshold: ${settings.blurThreshold})` : null;
            }),

            noise: settings.checkNoise && (async () => {
                const snr = await calculateSNR(imageBuffer);
                return snr < settings.noiseThreshold ?
                    `Image noise level is too high (SNR: ${snr.toFixed(2)}, threshold: ${settings.noiseThreshold})` : null;
            })
        };

        // Execute checks in parallel but with controlled concurrency
        const results = await Promise.all(
            Object.values(checks)
                .filter(Boolean)
                .map(check => check())
        );

        const issues = results.filter(Boolean);

        return {
            isValid: issues.length === 0,
            issues
        };

    } catch (error) {
        logger.error('[validateImageQuality] Error validating image quality', {
            error: error.message,
            bucket,
            key
        });
        throw error;
    }
};

// Helper function to chunk array
function chunk(array, size) {
    return Array.from(
        { length: Math.ceil(array.length / size) },
        (_, index) => array.slice(index * size, (index + 1) * size)
    );
}

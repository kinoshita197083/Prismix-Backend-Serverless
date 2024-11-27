const sharp = require('sharp');
const s3Service = require('./s3Service');
const { RESOLUTION_THRESHOLDS } = require('../utils/config');
const { calculateSNR, detectBlurriness } = require('../utils/helpers');
const logger = require('../utils/logger');

// Default values for quality checks
// const DEFAULT_IMAGE_QUALITY_SETTINGS = {
//     checkNoise: true,
//     noiseThreshold: 20, // SNR threshold
//     checkBlurriness: true,
//     blurThreshold: 1000, // Variance of sharp threshold
// };

exports.validateImageQuality = async ({ bucket, key, settings }) => {
    const {
        checkResolution,
        minResolution,
        checkNoise,
        noiseThreshold,
        checkBlurriness,
        blurThreshold
    } = settings;

    const issues = [];

    try {
        const imageBuffer = await s3Service.getFileBuffer(bucket, key);
        const metadata = await sharp(imageBuffer).metadata();

        // This part has already been implemented, ignore for now
        if (checkResolution) {
            console.log('[validateImageQuality] Checking resolution...');
            const minHeight = RESOLUTION_THRESHOLDS[minResolution];
            if (metadata.height < minHeight) {
                issues.push(`Resolution below ${minResolution} standard`);
            }
        }

        // Blurriness check
        if (checkBlurriness) {
            const blurScore = await detectBlurriness(imageBuffer);
            if (blurScore < blurThreshold) {
                issues.push(`Image is likely blurry (score: ${blurScore}, threshold: ${DEFAULT_IMAGE_QUALITY_SETTINGS.blurThreshold})`);
            }
        }

        // Noise check
        if (checkNoise) {
            const snr = await calculateSNR(imageBuffer);
            if (snr < noiseThreshold) {
                issues.push(`Image noise level is too high (SNR: ${snr.toFixed(2)}, threshold: ${noiseThreshold})`);
            }
        }

        return {
            isValid: issues.length === 0,
            issues
        };
    } catch (error) {
        const errorDetails = {
            message: error.message,
            stack: error.stack,
            name: error.name,
            code: error.code
        };

        logger.error('[qualityService] Error validating image quality', {
            error: errorDetails,
            bucket,
            key
        });

        return {
            isValid: false,
            issues: [`Failed to validate image quality: ${error.message}`]
        };
    }
};

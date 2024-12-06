const sharp = require('sharp');
const s3Service = require('./s3Service');
const { RESOLUTION_THRESHOLDS } = require('../utils/config');
const { calculateSNR } = require('../utils/helpers');
const logger = require('../utils/logger');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambdaClient = new LambdaClient();

/**
 * Detects image blurriness using the blur detection Lambda
 * @param {Buffer} imageBuffer - Raw image buffer
 * @returns {Promise<number>} Blur score (lower means more blurry)
 */
async function detectBlurriness(imageBuffer) {
    try {
        logger.info('[detectBlurriness] Starting blur detection');

        // Create payload with base64 encoded image
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

        logger.debug('[detectBlurriness] Invoking blur detection Lambda');
        const response = await lambdaClient.send(command);

        // Parse Lambda response
        const result = JSON.parse(Buffer.from(response.Payload).toString());
        logger.debug('[detectBlurriness] Lambda response:', { result });

        if (result.statusCode === 200) {
            const { blurScore } = JSON.parse(result.body);
            logger.info('[detectBlurriness] Blur detection completed', { blurScore });
            return blurScore;
        } else {
            const error = JSON.parse(result.body);
            throw new Error(`Blur detection failed: ${error.details || error.error}`);
        }
    } catch (error) {
        logger.error('[detectBlurriness] Error in blur detection:', {
            error: {
                message: error.message,
                stack: error.stack,
                name: error.name
            }
        });
        throw error;
    }
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
        logger.info('[validateImageQuality] Starting image quality validation', {
            bucket,
            key,
            settings
        });

        const imageBuffer = await s3Service.getFileBuffer(bucket, key);
        const metadata = await sharp(imageBuffer).metadata();

        // Resolution check
        if (checkResolution) {
            logger.debug('[validateImageQuality] Checking resolution');
            const minHeight = RESOLUTION_THRESHOLDS[minResolution];
            if (metadata.height < minHeight) {
                issues.push(`Resolution below ${minResolution} standard`);
            }
            logger.debug('[validateImageQuality] Resolution check completed', {
                minHeight,
                actualHeight: metadata.height
            });
        }

        // Blurriness check
        if (checkBlurriness) {
            logger.debug('[validateImageQuality] Starting blurriness check');
            const blurScore = await detectBlurriness(imageBuffer);
            if (blurScore > blurThreshold) {
                issues.push(`Image is likely blurry (score: ${blurScore.toFixed(2)}, threshold: ${blurThreshold})`);
            }
            logger.debug('[validateImageQuality] Blurriness check completed', {
                blurScore,
                blurThreshold
            });
        }

        // Noise check
        if (checkNoise) {
            logger.debug('[validateImageQuality] Starting noise check');
            const snr = await calculateSNR(imageBuffer);
            if (snr < noiseThreshold) {
                issues.push(`Image noise level is too high (SNR: ${snr.toFixed(2)}, threshold: ${noiseThreshold})`);
            }
            logger.debug('[validateImageQuality] Noise check completed', {
                snr,
                noiseThreshold
            });
        }

        logger.info('[validateImageQuality] All checks completed', {
            issueCount: issues.length,
            issues
        });

        return {
            isValid: issues.length === 0,
            issues
        };
    } catch (error) {
        logger.error('[validateImageQuality] Error validating image quality', {
            error: {
                message: error.message,
                stack: error.stack,
                name: error.name,
                code: error.code
            },
            bucket,
            key
        });

        return {
            isValid: false,
            issues: [`Failed to validate image quality: ${error.message}`]
        };
    }
};

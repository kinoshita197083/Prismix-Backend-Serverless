const sharp = require('sharp');
const s3Service = require('./s3Service');
const { RESOLUTION_THRESHOLDS } = require('../utils/config');

exports.validateImageQuality = async ({ bucket, key, settings }) => {
    const { checkBlur, checkResolution, minResolution } = settings;
    const issues = [];

    const imageBuffer = await s3Service.getFileBuffer(bucket, key);
    const metadata = await sharp(imageBuffer).metadata();

    if (checkResolution) {
        const minHeight = RESOLUTION_THRESHOLDS[minResolution];
        if (metadata.height < minHeight) {
            issues.push(`Resolution below ${minResolution} standard`);
        }
    }

    if (checkBlur) {
        // Implement blur detection logic here
        // You can use sharp's stats() to analyze image entropy
        // or implement more sophisticated blur detection algorithms
    }

    return {
        isValid: issues.length === 0,
        issues
    };
};
// Helper function to parse and validate the event body
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper function to convert a stream to a buffer
async function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function calculateImageHash(bucket, key) {
    const sharp = require('sharp');
    const crypto = require('crypto');
    const s3Service = require('../services/s3Service');
    try {
        console.log('Start calculating image hash...');
        const params = { Bucket: bucket, Key: key };
        const body = await s3Service.getFile(params);
        const buffer = await streamToBuffer(body);
        console.log('buffer processed successfully');

        // Resize image to a standard size for consistent hashing
        const resizedBuffer = await sharp(buffer)
            .resize(256, 256, { fit: 'inside' })
            .grayscale()
            .raw()
            .toBuffer();

        console.log('resizedBuffer processed successfully');

        // Calculate perceptual hash
        const hash = crypto.createHash('md5').update(resizedBuffer).digest('hex');
        console.log('hash: ', hash);
        return hash;
    } catch (error) {
        console.log('Error calculating image hash', { error });
        throw new AppError('Error calculating image hash', 500);
    }
}

const formatLabels = (labels) => {
    if (labels && Array.isArray(labels) && labels.length > 0) {
        return labels.map(label => ({
            name: label.Name || '',
            confidence: label.Confidence || 0,
            categories: Array.isArray(label.Categories) ? label.Categories.map(cat => cat.Name).join(', ') : '',
            parents: Array.isArray(label.Parents) && label.Parents.length > 0 ? label.Parents.map(parent => parent.Name).join(', ') : 'None',
            aliases: Array.isArray(label.Aliases) && label.Aliases.length > 0 ? label.Aliases.map(alias => alias.Name).join(', ') : 'None'
        }));
    }
    return [];
}

const formatTexts = (detectedTexts) => {
    if (detectedTexts && Array.isArray(detectedTexts) && detectedTexts.length > 0) {
        // Process text detections
        return detectedTexts.map((detection) => ({
            text: detection.DetectedText,
            confidence: detection.Confidence,
            type: detection.Type,
        }));
    }

    return [];
}

const createHash = (value) => {
    const crypto = require('crypto');
    return crypto.createHash('md5').update(value).digest('hex');
}

// Helper function to chunk array into batches
const chunkArray = (array, size) => {
    return Array.from(
        { length: Math.ceil(array.length / size) },
        (_, index) => array.slice(index * size, (index + 1) * size)
    );
};

function parseRecordBody(record) {
    try {
        return JSON.parse(record.body);
    } catch (error) {
        console.error('Failed to parse record body', { error, record });
        return null;
    }
}

module.exports = {
    sleep,
    streamToBuffer,
    calculateImageHash,
    formatLabels,
    createHash,
    chunkArray,
    formatTexts,
    parseRecordBody
};

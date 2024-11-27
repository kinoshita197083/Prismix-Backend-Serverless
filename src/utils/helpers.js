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

/**
 * Calculates the variance of the Laplacian of an image to measure blurriness.
 * @param {Buffer} imageBuffer - The buffer containing the image data.
 * @returns {number} - The variance of the Laplacian, higher means sharper image.
 */
const calculateVarianceOfLaplacian = (imageBuffer) => {
    const cv = require('opencv4nodejs');

    // Read the image from the buffer
    const mat = cv.imdecode(imageBuffer); // Decode image buffer into a Mat object
    const grayMat = mat.bgrToGray();     // Convert to grayscale

    // Apply Laplacian operator
    const laplacian = grayMat.laplacian(cv.CV_64F);

    // Calculate variance of the Laplacian
    const mean = laplacian.mean().w; // .w holds the scalar mean
    const variance = laplacian.sub(mean).pow(2).mean().w;

    return variance; // Higher values indicate sharper images
};

/**
 * Detect blurriness by calculating the variance of edges using sharp.
 * @param {Buffer} imageBuffer - The image buffer to process.
 * @returns {Promise<number>} - The calculated edge variance, indicating blurriness.
 */
async function detectBlurriness(imageBuffer) {
    const sharp = require('sharp');
    try {
        // Process the image using sharp
        const { data, info } = await sharp(imageBuffer)
            .greyscale() // Convert to grayscale
            .convolve({
                width: 3,
                height: 3,
                kernel: [
                    -1, -1, -1,
                    -1, 8, -1,
                    -1, -1, -1, // Laplacian-like kernel for edge detection
                ],
            })
            .raw()
            .toBuffer({ resolveWithObject: true });

        // Calculate the mean of pixel intensities
        const mean =
            data.reduce((sum, value) => sum + value, 0) / (info.width * info.height);

        // Calculate the variance of pixel intensities
        const variance =
            data.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) /
            (info.width * info.height);

        return variance; // Higher variance = sharper image
    } catch (error) {
        console.error('Error detecting blurriness:', error);
        throw error;
    }
}

async function calculateSNR(imageBuffer) {
    const sharp = require('sharp');
    return sharp(imageBuffer)
        .raw()
        .toBuffer({ resolveWithObject: true })
        .then(({ data }) => {
            const mean = data.reduce((sum, val) => sum + val, 0) / data.length;
            const noise = data.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / data.length;
            return mean / Math.sqrt(noise); // SNR formula
        });
};

// async function calculateNoise(sharp, imageBuffer) {
//     const { data, info } = await sharp(imageBuffer).raw().toBuffer({ resolveWithObject: true });
//     const diffs = [];

//     for (let i = 0; i < data.length - 1; i++) {
//         diffs.push(Math.abs(data[i] - data[i + 1]));
//     }

//     const noiseScore = diffs.reduce((sum, value) => sum + value, 0) / diffs.length;
//     return noiseScore;
// };

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
    parseRecordBody,
    detectBlurriness,
    calculateSNR
    // calculateNoise
};
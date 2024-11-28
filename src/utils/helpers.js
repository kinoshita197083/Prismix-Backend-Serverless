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
                    0, -1, 0,
                    -1, 4, -1,
                    0, -1, 0  // Using a simpler Laplacian kernel for better sensitivity
                ],
            })
            .raw()
            .toBuffer({ resolveWithObject: true });

        // Calculate normalized variance with enhanced scaling
        const mean = data.reduce((sum, value) => sum + value, 0) / data.length;
        const variance = data.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / data.length;

        // Apply stronger normalization and scaling
        const normalizedVariance = (variance / (info.width * info.height)) * 10000000;

        return normalizedVariance;
    } catch (error) {
        console.error('Error detecting blurriness:', error);
        throw error;
    }
}

async function calculateSNR(imageBuffer) {
    const sharp = require('sharp');
    try {
        const { data, info } = await sharp(imageBuffer)
            .greyscale()
            .raw()
            .toBuffer({ resolveWithObject: true });

        // Use smaller window size for more localized noise detection
        const windowSize = 2;
        const signal = new Array(data.length).fill(0);
        const noise = new Array(data.length).fill(0);

        for (let y = windowSize; y < info.height - windowSize; y++) {
            for (let x = windowSize; x < info.width - windowSize; x++) {
                const idx = y * info.width + x;
                const window = [];

                for (let wy = -windowSize; wy <= windowSize; wy++) {
                    for (let wx = -windowSize; wx <= windowSize; wx++) {
                        const widx = (y + wy) * info.width + (x + wx);
                        window.push(data[widx]);
                    }
                }

                const localMean = window.reduce((a, b) => a + b, 0) / window.length;
                signal[idx] = localMean;
                // Enhanced noise calculation with weighted difference
                noise[idx] = window.reduce((sum, val) => {
                    const diff = Math.abs(val - localMean);
                    return sum + (diff * diff);
                }, 0) / window.length;
            }
        }

        const avgSignal = signal.reduce((a, b) => a + b, 0) / signal.length;
        const avgNoise = Math.sqrt(noise.reduce((a, b) => a + b, 0) / noise.length);

        // Apply scaling factor to make the SNR more intuitive
        return (avgSignal / avgNoise) * 2;
    } catch (error) {
        console.error('Error calculating SNR:', error);
        throw error;
    }
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
    parseRecordBody,
    detectBlurriness,
    calculateSNR
    // calculateNoise
};
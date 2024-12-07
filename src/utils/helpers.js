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
 * Detects image blurriness using Variance of Laplacian method
 * This method is widely used in computer vision for blur detection
 * Higher values indicate sharper images
 * 
 * @param {Buffer} imageBuffer - The image buffer to process
 * @returns {Promise<number>} - The sharpness score
 */
async function detectBlurriness(imageBuffer) {
    const sharp = require('sharp');
    try {
        // Convert to grayscale and get image data
        const { data, info } = await sharp(imageBuffer)
            .greyscale()
            .raw()
            .toBuffer({ resolveWithObject: true });

        const width = info.width;
        const height = info.height;

        // Laplacian kernel for edge detection
        const laplacian = [
            [0, 1, 0],
            [1, -4, 1],
            [0, 1, 0]
        ];

        let sumSquaredVariance = 0;
        const border = 1;  // Border size due to kernel

        // Apply Laplacian operator and calculate variance
        for (let y = border; y < height - border; y++) {
            for (let x = border; x < width - border; x++) {
                let sum = 0;

                // Apply convolution
                for (let ky = -1; ky <= 1; ky++) {
                    for (let kx = -1; kx <= 1; kx++) {
                        const idx = ((y + ky) * width + (x + kx));
                        sum += data[idx] * laplacian[ky + 1][kx + 1];
                    }
                }

                sumSquaredVariance += sum * sum;
            }
        }

        // Calculate normalized variance
        const pixelCount = (width - 2 * border) * (height - 2 * border);
        const variance = Math.sqrt(sumSquaredVariance / pixelCount);

        // Scale the result to a more intuitive range (0-1000)
        return variance * 10;

    } catch (error) {
        console.error('Error detecting blurriness:', error);
        throw error;
    }
}

/**
 * Calculates Signal-to-Noise Ratio using Block-based method
 * This implementation uses local variance analysis to separate signal from noise
 * Higher values indicate cleaner images
 * 
 * @param {Buffer} imageBuffer - The image buffer to process
 * @returns {Promise<number>} - The SNR value
 */
async function calculateSNR(imageBuffer) {
    const sharp = require('sharp');
    try {
        const { data, info } = await sharp(imageBuffer)
            .greyscale()
            .raw()
            .toBuffer({ resolveWithObject: true });

        const width = info.width;
        const height = info.height;

        // Block size for local analysis
        const blockSize = 8;
        const blocks = [];

        // Divide image into blocks and calculate local statistics
        for (let y = 0; y < height - blockSize; y += blockSize) {
            for (let x = 0; x < width - blockSize; x += blockSize) {
                const block = [];

                // Extract block data
                for (let by = 0; by < blockSize; by++) {
                    for (let bx = 0; bx < blockSize; bx++) {
                        const idx = (y + by) * width + (x + bx);
                        block.push(data[idx]);
                    }
                }

                // Calculate block statistics
                const blockMean = block.reduce((sum, val) => sum + val, 0) / block.length;
                const blockVariance = block.reduce((sum, val) => {
                    const diff = val - blockMean;
                    return sum + (diff * diff);
                }, 0) / block.length;

                blocks.push({ mean: blockMean, variance: blockVariance });
            }
        }

        // Sort blocks by variance to separate signal from noise
        blocks.sort((a, b) => b.variance - a.variance);

        // Use top 20% of blocks as signal and bottom 20% as noise
        const signalBlocks = blocks.slice(0, Math.floor(blocks.length * 0.2));
        const noiseBlocks = blocks.slice(Math.floor(blocks.length * 0.8));

        // Calculate final SNR
        const signalPower = signalBlocks.reduce((sum, block) => sum + block.variance, 0) / signalBlocks.length;
        const noisePower = noiseBlocks.reduce((sum, block) => sum + block.variance, 0) / noiseBlocks.length;

        // Prevent division by zero and apply logarithmic scaling
        const snr = noisePower === 0 ? 100 : 10 * Math.log10(signalPower / noisePower);

        // Normalize to a more intuitive range
        return Math.max(0, Math.min(20, snr));

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

const removeAllTextHelper = (formattedTexts, config = { confidenceThreshold: 0.9 }) => {
    const { confidenceThreshold } = config;
    if (confidenceThreshold) {
        return formattedTexts.filter(text => {
            // Some text detections return a confidence value between 0 and 100
            if (text.confidence > 1) {
                const confidence = text.confidence / 100;
                return confidence >= confidenceThreshold;
            }
            // Some text detections return a confidence value between 0 and 1
            return text.confidence >= confidenceThreshold;
        });
    }
    // If no confidence threshold is provided, return all texts
    return formattedTexts;
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
    calculateSNR,
    removeAllTextHelper
};
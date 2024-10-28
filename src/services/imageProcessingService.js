const sharp = require('sharp');
const s3Service = require('./s3Service');
const { validFitOptions } = require('../utils/config');

exports.processImageProperties = async ({ bucket, s3ObjectKey, settings }) => {
    const { maxWidth, maxHeight, resizeMode, quality, outputFormat = 'original' } = settings;

    // If the output format is original and no dimensions are specified, return the original image
    if (outputFormat === 'original' && !maxWidth && !maxHeight) {
        return s3ObjectKey;
    }

    // Get original image
    const imageBuffer = await s3Service.getFileBuffer(bucket, s3ObjectKey);

    // Initialize sharp pipeline
    let pipeline = sharp(imageBuffer);

    // Apply resize if dimensions are specified
    if (maxWidth || maxHeight) {
        // Validate and map resizeMode to Sharp's supported fit options
        const fit = validFitOptions[resizeMode] || 'contain'; // Default to 'contain' if invalid

        pipeline = pipeline.resize(maxWidth, maxHeight, {
            fit,
            withoutEnlargement: true
        });
    }

    // Set format and quality
    if (outputFormat !== 'original') {
        pipeline = pipeline.toFormat(outputFormat, {
            quality: quality
        });
    }

    // Process image
    const processedBuffer = await pipeline.toBuffer();

    // Generate new key for processed image
    const newKey = s3ObjectKey.replace(
        /([^\/]+)$/,
        `processed_${Date.now()}_$1`
    );

    // Upload processed image
    await s3Service.uploadFile({
        Bucket: bucket,
        Key: newKey,
        Body: processedBuffer,
        ContentType: `image/${outputFormat}`,
        Metadata: {
            resized: "true"  // When upload to S3, it does not trigger subsequent processing
        }
    });

    return newKey;
};

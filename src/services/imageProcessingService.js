const sharp = require('sharp');
const s3Service = require('./s3Service');
const { validFitOptions } = require('../utils/config');

exports.processImageProperties = async ({ bucket, s3ObjectKey, settings }) => {
    const { maxWidth, maxHeight, resizeMode, quality, outputFormat = 'original' } = settings;

    // If the output format is original and no dimensions are specified, return the original image
    if (outputFormat === 'original' && !maxWidth && !maxHeight) {
        console.log('[processImageProperties] No dimensions specified, returning original image');
        return s3ObjectKey;
    }

    // Get original image
    const imageBuffer = await s3Service.getFileBuffer(bucket, s3ObjectKey);

    // Initialize sharp pipeline
    let pipeline = sharp(imageBuffer);

    // Apply resize if dimensions are specified
    if (maxWidth || maxHeight) {
        const fit = validFitOptions[resizeMode] || 'contain';

        // Get image metadata to check orientation
        const metadata = await pipeline.metadata();
        const isPortrait = metadata.height > metadata.width;

        // If image is portrait, swap maxWidth and maxHeight to maintain orientation
        const resizeOptions = {
            fit,
            withoutEnlargement: true
        };

        if (isPortrait) {
            // For portrait images, ensure the height is larger than width
            pipeline = pipeline.resize(
                Math.min(maxWidth || metadata.width, maxHeight || metadata.height),
                Math.max(maxWidth || metadata.width, maxHeight || metadata.height),
                resizeOptions
            );
        } else {
            // For landscape images, use dimensions as provided
            pipeline = pipeline.resize(maxWidth, maxHeight, resizeOptions);
        }
    }

    // Set format and quality
    if (outputFormat !== 'original') {
        console.log('[processImageProperties] Setting format and quality');
        pipeline = pipeline.toFormat(outputFormat, {
            quality: quality
        });
    }

    // Process image
    const processedBuffer = await pipeline.toBuffer();
    console.log('[processImageProperties] Image processed');

    // Generate new key for processed image
    const newKey = s3ObjectKey.replace(
        /([^\/]+)$/,
        `processed_${Date.now()}_$1`
    );
    console.log('[processImageProperties] New key generated');

    // Get existing metadata from the original object
    const originalMetadata = await s3Service.getObjectMetadata(bucket, s3ObjectKey);

    // Merge existing metadata with new metadata
    const mergedMetadata = {
        ...originalMetadata,
        resized: "true"  // Add or update the resized flag
    };

    // Upload with merged metadata
    await s3Service.uploadFile({
        Bucket: bucket,
        Key: newKey,
        Body: processedBuffer,
        ContentType: `image/${outputFormat}`,
        Metadata: mergedMetadata // When upload to S3, it does not trigger subsequent processing
    });

    return newKey;
};

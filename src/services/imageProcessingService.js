const sharp = require('sharp');
const s3Service = require('./s3Service');

exports.processImageProperties = async ({ bucket, s3ObjectKey, settings }) => {
    const { maxWidth, maxHeight, resizeMode, quality, outputFormat } = settings;

    // Get original image
    const imageBuffer = await s3Service.getFileBuffer(bucket, s3ObjectKey);

    // Initialize sharp pipeline
    let pipeline = sharp(imageBuffer);

    // Apply resize if dimensions are specified
    if (maxWidth || maxHeight) {
        pipeline = pipeline.resize(maxWidth, maxHeight, {
            fit: resizeMode,
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
        ContentType: `image/${outputFormat}`
    });

    return newKey;
};

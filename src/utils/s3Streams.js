const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const archiver = require('archiver');
const logger = require('./logger');

/**
 * Creates a ZIP archive stream from multiple S3 objects
 * @param {S3Client} s3Client - AWS S3 client instance
 * @param {string} sourceBucket - Source S3 bucket name
 * @param {string[]} keys - Array of S3 object keys to include in ZIP
 * @param {Function} progressCallback - Callback function for progress updates
 * @returns {Promise<archiver.Archiver>} Archive stream
 */
async function createZipStream(s3Client, sourceBucket, keys, progressCallback) {
    logger.info('Creating ZIP stream', {
        sourceBucket,
        fileCount: keys.length
    });

    // Initialize archiver with compression
    const archive = archiver('zip', {
        zlib: { level: 9 } // Balanced compression level
    });

    // Set up error handling
    archive.on('error', (err) => {
        logger.error('Archive creation error', {
            error: err.message,
            stack: err.stack
        });
        throw err;
    });

    // Set up warning handling
    archive.on('warning', (err) => {
        if (err.code === 'ENOENT') {
            logger.warn('Archive warning', {
                warning: err.message
            });
        } else {
            logger.error('Archive error', {
                error: err.message,
                stack: err.stack
            });
            throw err;
        }
    });

    // Set up progress tracking
    if (progressCallback) {
        archive.on('progress', (progress) => {
            logger.debug('Archive progress', {
                filesProcessed: progress.entries.processed,
                totalFiles: progress.entries.total,
                bytesProcessed: progress.fs.processedBytes
            });
            progressCallback(progress);
        });
    }

    // Process each file
    for (const key of keys) {
        try {
            const command = new GetObjectCommand({
                Bucket: sourceBucket,
                Key: key
            });

            const response = await s3Client.send(command);
            const fileName = key.split('/').pop(); // Extract filename from key

            logger.debug('Adding file to archive', {
                fileName,
                key
            });

            archive.append(response.Body, { name: fileName });
        } catch (error) {
            logger.error('Error processing file for archive', {
                key,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    return archive;
}

/**
 * Uploads a ZIP archive stream to S3
 * @param {S3Client} s3Client - AWS S3 client instance
 * @param {archiver.Archiver} archive - Archive stream
 * @param {string} bucket - Destination S3 bucket
 * @param {string} key - Destination S3 key
 * @returns {Promise<void>}
 */
async function uploadZipToS3(s3Client, archive, bucket, key) {
    logger.info('Starting ZIP upload to S3', {
        bucket,
        key
    });

    try {
        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: archive,
            ContentType: 'application/zip'
        });

        await s3Client.send(command);

        logger.info('Successfully uploaded ZIP to S3', {
            bucket,
            key
        });
    } catch (error) {
        logger.error('Error uploading ZIP to S3', {
            bucket,
            key,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

module.exports = {
    createZipStream,
    uploadZipToS3
}; 
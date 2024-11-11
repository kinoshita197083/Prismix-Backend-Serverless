const { S3Client, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { Readable, PassThrough } = require('stream');
const archiver = require('archiver');
const logger = require('../utils/logger');
const { uploadZipToS3 } = require('../utils/s3Streams');
const { Upload } = require('@aws-sdk/lib-storage');

/**
 * Service responsible for merging multiple ZIP chunks into a single final ZIP file
 * Handles streaming of large files and cleanup of temporary chunks
 */
class ZipMergeService {
    constructor(s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Merges multiple ZIP chunks into a single final ZIP file
     * @param {string} jobId - The ID of the job
     * @param {string[]} chunkKeys - Array of S3 keys for the chunk files
     * @param {string} deliveryBucket - The S3 bucket where the final ZIP will be stored
     * @returns {Promise<string>} The S3 key of the final merged ZIP file
     */
    async mergeChunks(jobId, chunkKeys, deliveryBucket) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const finalZipKey = `archives/${jobId}/final/prismix-job-results-${timestamp}.zip`;

        try {
            // Create a PassThrough stream
            const passThrough = new PassThrough();

            // Create and configure archive
            const archive = archiver('zip', {
                zlib: { level: 9 }
            });

            // Handle warnings
            archive.on('warning', (err) => {
                if (err.code === 'ENOENT') {
                    logger.warn('Archive warning', { warning: err.message });
                } else {
                    throw err;
                }
            });

            // Handle errors
            archive.on('error', (err) => {
                throw err;
            });

            // Pipe archive to PassThrough
            archive.pipe(passThrough);

            // Create upload manager
            const upload = new Upload({
                client: this.s3Client,
                params: {
                    Bucket: deliveryBucket,
                    Key: finalZipKey,
                    Body: passThrough
                },
                tags: [{ Key: 'jobId', Value: jobId }],
                queueSize: 4,
                partSize: 1024 * 1024 * 5
            });

            // Process each chunk
            for (const chunkKey of chunkKeys) {
                const chunkStream = await this.getS3ReadStream(deliveryBucket, chunkKey);
                archive.append(chunkStream, { name: chunkKey.split('/').pop() });
            }

            // Create promises for both operations
            const uploadPromise = upload.done();
            const archivePromise = new Promise((resolve, reject) => {
                archive.on('end', resolve);
                archive.on('error', reject);
            });

            // Finalize the archive
            archive.finalize();

            // Wait for both operations to complete
            await Promise.all([uploadPromise, archivePromise]);

            logger.info('Successfully created final ZIP', {
                jobId,
                finalZipKey
            });

            return finalZipKey;
        } catch (error) {
            logger.error('Failed to upload final ZIP', {
                jobId,
                finalZipKey,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    /**
     * Appends a single chunk file to the archive
     * @param {Archiver} archive - The archive instance
     * @param {string} bucket - The S3 bucket containing the chunk
     * @param {string} chunkKey - The S3 key of the chunk
     */
    async appendChunkToArchive(archive, bucket, chunkKey) {
        logger.debug('Fetching chunk from S3', {
            bucket,
            chunkKey
        });

        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: chunkKey
        });

        try {
            const response = await this.s3Client.send(command);
            const stream = response.Body;

            if (stream instanceof Readable) {
                // Append the chunk to the archive with a meaningful name
                archive.append(stream, { name: `chunk_${chunkKey}` });
                logger.debug('Successfully appended chunk to archive', {
                    chunkKey
                });
            } else {
                const error = new Error(`Invalid stream type for chunk ${chunkKey}`);
                logger.error('Stream type error', {
                    chunkKey,
                    streamType: typeof stream
                });
                throw error;
            }
        } catch (error) {
            logger.error('Failed to append chunk to archive', {
                bucket,
                chunkKey,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    /**
     * Cleans up temporary chunk files after successful merge
     * @param {string} bucket - The S3 bucket containing the chunks
     * @param {string[]} chunkKeys - Array of S3 keys for the chunk files
     */
    async cleanupChunks(bucket, chunkKeys) {
        logger.info('Starting cleanup of chunk files', {
            bucket,
            numberOfChunks: chunkKeys.length
        });

        const deletePromises = chunkKeys.map(key => {
            const command = new DeleteObjectCommand({
                Bucket: bucket,
                Key: key
            });
            return this.s3Client.send(command)
                .then(() => {
                    logger.debug('Successfully deleted chunk', { key });
                })
                .catch(error => {
                    logger.warn('Failed to delete chunk', {
                        key,
                        error: error.message
                    });
                    // Don't throw error to continue with other deletions
                });
        });

        try {
            await Promise.all(deletePromises);
            logger.info('Successfully completed chunk cleanup', {
                bucket,
                numberOfChunks: chunkKeys.length
            });
        } catch (error) {
            logger.warn('Error during chunk cleanup', {
                error: error.message,
                stack: error.stack
            });
            // Don't throw as the main operation was successful
        }
    }

    // Helper method to get S3 read stream
    async getS3ReadStream(bucket, key) {
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key
        });
        const response = await this.s3Client.send(command);
        return response.Body;
    }
}

module.exports = ZipMergeService; 
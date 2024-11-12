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
            // Create a PassThrough stream for piping
            const passThrough = new PassThrough();

            // Set up the S3 upload
            const upload = new Upload({
                client: this.s3Client,
                params: {
                    Bucket: deliveryBucket,
                    Key: finalZipKey,
                    Body: passThrough
                },
                queueSize: 4, // Parallel upload parts
                partSize: 5 * 1024 * 1024 // 5MB parts
            });

            // Create and configure archive
            const archive = archiver('zip', {
                zlib: { level: 6 }
            });

            // Pipe archive to the upload stream
            archive.pipe(passThrough);

            // Log archive progress
            archive.on('progress', (progress) => {
                logger.info('Merge progress', {
                    jobId,
                    entriesProcessed: progress.entries.processed,
                    bytesProcessed: progress.fs.processedBytes
                });
            });

            // Process chunks in parallel
            await Promise.all(chunkKeys.map(async (key) => {
                logger.info('Adding chunk to final ZIP', { jobId, chunkKey: key });
                const stream = await this.getS3ReadStream(deliveryBucket, key);
                archive.append(stream, { name: key.split('/').pop() });
            }));

            logger.info('Finalizing archive', { jobId });

            // Important: Wait for archive to finalize
            await archive.finalize();

            // Important: Wait for upload to complete
            await upload.done();

            logger.info('Successfully merged chunks', {
                jobId,
                finalZipKey,
                chunkCount: chunkKeys.length
            });

            // Clean up chunk files
            await this.cleanupChunks(deliveryBucket, chunkKeys);

            return finalZipKey;
        } catch (error) {
            logger.error('Failed to merge chunks', {
                jobId,
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
        try {
            const command = new GetObjectCommand({
                Bucket: bucket,
                Key: key
            });
            const response = await this.s3Client.send(command);
            return response.Body;
        } catch (error) {
            logger.error('Failed to get S3 read stream', {
                bucket,
                key,
                error: error.message
            });
            throw error;
        }
    }
}

module.exports = ZipMergeService; 
const { S3Client, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');
const archiver = require('archiver');
const logger = require('../utils/logger');
const { uploadZipToS3 } = require('../utils/s3Streams');

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
        logger.info('Starting ZIP merge process', {
            jobId,
            numberOfChunks: chunkKeys.length,
            deliveryBucket
        });

        // Initialize archiver with compression level 5 (balanced between speed and size)
        const archive = archiver('zip', {
            zlib: { level: 5 }
        });

        // Set up error handling for the archive creation process
        archive.on('error', (err) => {
            logger.error('Archive creation error:', {
                jobId,
                error: err.message,
                stack: err.stack
            });
            throw err;
        });

        // Set up progress tracking for monitoring the merge process
        archive.on('progress', (progress) => {
            logger.info('ZIP merge progress:', {
                jobId,
                entriesProcessed: progress.entries.processed,
                entriesTotal: progress.entries.total,
                bytesProcessed: progress.fs.processedBytes,
                bytesTotal: progress.fs.totalBytes
            });
        });

        // Process each chunk sequentially to maintain order and manage memory
        for (const [index, chunkKey] of chunkKeys.entries()) {
            logger.debug(`Processing chunk ${index + 1}/${chunkKeys.length}`, {
                jobId,
                chunkKey
            });

            try {
                await this.appendChunkToArchive(archive, deliveryBucket, chunkKey);
                logger.debug(`Successfully appended chunk to archive`, {
                    jobId,
                    chunkKey,
                    chunkNumber: index + 1
                });
            } catch (error) {
                logger.error(`Failed to process chunk`, {
                    jobId,
                    chunkKey,
                    chunkNumber: index + 1,
                    error: error.message,
                    stack: error.stack
                });
                throw error;
            }
        }

        // Generate final ZIP key with timestamp for uniqueness
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const finalZipKey = `archives/${jobId}/final/${timestamp}_complete.zip`;

        logger.info('Finalizing archive and starting upload', {
            jobId,
            finalZipKey,
            deliveryBucket
        });

        // Finalize the archive (required before upload)
        archive.finalize();

        // Upload the final ZIP to S3
        try {
            await uploadZipToS3(
                this.s3Client,
                archive,
                deliveryBucket,
                finalZipKey
            );

            logger.info('Successfully uploaded final ZIP', {
                jobId,
                finalZipKey,
                deliveryBucket
            });

            // Clean up temporary chunk files
            await this.cleanupChunks(deliveryBucket, chunkKeys);

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
}

module.exports = ZipMergeService; 
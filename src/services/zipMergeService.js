const { GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { Readable, PassThrough } = require('stream');
const archiver = require('archiver');
const logger = require('../utils/logger');
// const { uploadZipToS3 } = require('../utils/s3Streams');
const { Upload } = require('@aws-sdk/lib-storage');

let pLimit;
(async () => {
    const module = await import('p-limit');
    pLimit = module.default;
})();

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

        const passThrough = new PassThrough();
        const archive = archiver('zip', {
            zlib: { level: 2 },
            highWaterMark: 4 * 1024 * 1024
        });

        try {
            // Configure S3 multipart upload
            const upload = new Upload({
                client: this.s3Client,
                params: {
                    Bucket: deliveryBucket,
                    Key: finalZipKey,
                    Body: passThrough
                },
                queueSize: 8,
                partSize: 10 * 1024 * 1024
            });

            archive.pipe(passThrough);

            // Track archive progress
            archive.on('progress', (progress) => {
                logger.info('Merge progress', {
                    jobId,
                    entriesProcessed: progress.entries.processed,
                    bytesProcessed: progress.fs.processedBytes
                });
            });

            // Handle errors on archive and passThrough streams
            archive.on('error', (err) => {
                logger.error('Archive error', { jobId, error: err.message });
                throw err;
            });
            passThrough.on('error', (err) => {
                logger.error('PassThrough stream error', { jobId, error: err.message });
                throw err;
            });

            // Process chunks concurrently with controlled concurrency
            const limit = pLimit(8); // Increased from 5
            await Promise.all(chunkKeys.map((key) =>
                limit(async () => {
                    logger.info('Adding chunk to final ZIP', { jobId, chunkKey: key });
                    const stream = await this.getS3ReadStream(deliveryBucket, key);

                    // Handle stream errors individually
                    stream.on('error', (err) => {
                        logger.error('S3 stream error', { jobId, chunkKey: key, error: err.message });
                        throw err;
                    });

                    archive.append(stream, { name: key.split('/').pop() });
                })
            ));

            logger.info('Finalizing archive', { jobId });

            // Finalize archive and complete upload
            await archive.finalize();

            // Ensure the upload finishes successfully
            await upload.done();

            logger.info('Successfully merged chunks', {
                jobId,
                finalZipKey,
                chunkCount: chunkKeys.length
            });

            // Cleanup temporary chunk files after successful upload
            await this.cleanupChunks(deliveryBucket, chunkKeys);

            return finalZipKey;

        } catch (error) {
            logger.error('Failed to merge chunks', {
                jobId,
                error: error.message,
                stack: error.stack
            });

            if (!archive.closed) {
                archive.abort();
            }

            throw error;
        } finally {
            // Always end the passThrough stream in both success and error cases
            passThrough.end();
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

            if (!(response.Body instanceof Readable)) {
                const error = new Error(`Invalid stream type for S3 object ${key}`);
                logger.error('Stream type error', {
                    bucket,
                    key,
                    streamType: typeof response.Body
                });
                throw error;
            }

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

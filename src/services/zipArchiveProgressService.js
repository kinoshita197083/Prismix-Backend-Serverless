const { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');

/**
 * Configuration constants for chunk management
 */
const CHUNK_SIZE = 1000; // Number of files per chunk
const MAX_CHUNK_RETRIES = 3;
const TTL_DAYS = 7;
const CHUNK_TTL_DAYS = 90;

/**
 * Service responsible for managing and tracking the progress of ZIP archive creation
 * Handles chunk management, progress tracking, and state persistence
 */
class ZipArchiveProgressService {
    constructor(docClient) {
        this.docClient = docClient;
        this.tableName = process.env.ZIP_ARCHIVE_PROGRESS_TABLE;
    }

    /**
     * Initializes progress tracking for a new ZIP archive job
     * @param {string} jobId - The ID of the job
     * @param {number} totalFiles - Total number of files to be processed
     * @returns {Promise<Object>} The initialized progress record
     */
    async initializeProgress(jobId, totalFiles) {
        logger.info('Initializing ZIP archive progress', {
            jobId,
            totalFiles
        });

        // Calculate number of chunks needed based on total files
        const totalChunks = Math.ceil(totalFiles / CHUNK_SIZE);
        logger.debug('Calculated chunk distribution', {
            jobId,
            totalChunks,
            filesPerChunk: CHUNK_SIZE
        });

        // Create chunk metadata array
        const chunks = Array.from({ length: totalChunks }, (_, i) => {
            const startIndex = i * CHUNK_SIZE;
            const endIndex = Math.min((i + 1) * CHUNK_SIZE, totalFiles);

            return {
                chunkId: `chunk_${i}`,
                status: 'PENDING',
                startIndex,
                endIndex,
                retryCount: 0,
                filesCount: endIndex - startIndex
            };
        });

        // Calculate TTL timestamp (7 days from now)
        const expirationTime = Math.floor(Date.now() / 1000) + (TTL_DAYS * 24 * 60 * 60);

        const progressRecord = {
            JobId: jobId,
            ChunkId: 'metadata',
            TotalFiles: totalFiles,
            TotalChunks: totalChunks,
            CompletedChunks: 0,
            Status: 'IN_PROGRESS',
            CreatedAt: Date.now().toString(),
            ExpirationTime: expirationTime,
            Chunks: chunks
        };

        try {
            await this.docClient.send(new PutCommand({
                TableName: this.tableName,
                Item: progressRecord
            }));

            logger.info('Successfully initialized progress tracking', {
                jobId,
                totalChunks,
                totalFiles
            });

            return progressRecord;
        } catch (error) {
            logger.error('Failed to initialize progress tracking', {
                jobId,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    /**
     * Retrieves the next pending chunk for processing
     * @param {string} jobId - The ID of the job
     * @returns {Promise<Object|null>} The next chunk to process or null if none available
     */
    async getNextPendingChunk(jobId) {
        logger.debug('Fetching next pending chunk', { jobId });

        try {
            const result = await this.docClient.send(new QueryCommand({
                TableName: this.tableName,
                KeyConditionExpression: 'JobId = :jobId',
                ExpressionAttributeValues: {
                    ':jobId': jobId
                }
            }));

            const metadata = result.Items.find(item => item.ChunkId === 'metadata');

            if (!metadata) {
                logger.warn('No metadata found for job', { jobId });
                return null;
            }

            if (metadata.Status === 'COMPLETED') {
                logger.info('Job already completed', { jobId });
                return null;
            }

            // Find first pending chunk that hasn't exceeded retry limit
            const pendingChunk = metadata.Chunks.find(chunk =>
                chunk.status === 'PENDING' && chunk.retryCount < MAX_CHUNK_RETRIES
            );

            if (pendingChunk) {
                logger.info('Found pending chunk', {
                    jobId,
                    chunkId: pendingChunk.chunkId,
                    retryCount: pendingChunk.retryCount
                });
            } else {
                logger.info('No pending chunks available', { jobId });
            }

            return pendingChunk;
        } catch (error) {
            logger.error('Error fetching pending chunk', {
                jobId,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    /**
     * Updates the status of a specific chunk
     * @param {string} jobId - The ID of the job
     * @param {string} chunkId - The ID of the chunk
     * @param {string} status - The new status
     * @param {string} [error] - Optional error message
     * @param {string} [zipKey] - Optional S3 key of the chunk ZIP
     */
    async updateChunkStatus(jobId, chunkId, status, error = null, zipKey = null) {
        try {
            // Calculate expiration timestamp (90 days from now)
            const expirationTime = Math.floor(Date.now() / 1000) + (CHUNK_TTL_DAYS * 24 * 60 * 60);

            // First update the chunk record
            const params = {
                TableName: this.tableName,
                Key: {
                    JobId: jobId,
                    ChunkId: chunkId
                },
                UpdateExpression: 'SET #st = :status, UpdatedAt = :updatedAt, ExpirationTime = :expTime',
                ExpressionAttributeNames: {
                    '#st': 'Status'
                },
                ExpressionAttributeValues: {
                    ':status': status,
                    ':updatedAt': Date.now().toString(),
                    ':expTime': expirationTime
                }
            };

            if (zipKey) {
                params.UpdateExpression += ', ZipKey = :zipKey';
                params.ExpressionAttributeValues[':zipKey'] = zipKey;
            }

            if (error) {
                params.UpdateExpression += ', ErrorMessage = :error';
                params.ExpressionAttributeValues[':error'] = error;
            }

            await this.docClient.send(new UpdateCommand(params));

            // Then update the metadata record
            // First get the current metadata to update the Chunks array
            const result = await this.docClient.send(new QueryCommand({
                TableName: this.tableName,
                KeyConditionExpression: 'JobId = :jobId AND ChunkId = :metadataId',
                ExpressionAttributeValues: {
                    ':jobId': jobId,
                    ':metadataId': 'metadata'
                }
            }));

            const metadata = result.Items[0];
            if (metadata && metadata.Chunks) {
                const chunkIndex = parseInt(chunkId.split('_')[1]);
                const updatedChunks = [...metadata.Chunks];
                updatedChunks[chunkIndex] = {
                    ...updatedChunks[chunkIndex],
                    status,
                    updatedAt: Date.now().toString()
                };

                if (zipKey) {
                    updatedChunks[chunkIndex].zipKey = zipKey;
                }

                if (error) {
                    updatedChunks[chunkIndex].error = error;
                }

                // Update the entire Chunks array
                await this.docClient.send(new UpdateCommand({
                    TableName: this.tableName,
                    Key: {
                        JobId: jobId,
                        ChunkId: 'metadata'
                    },
                    UpdateExpression: 'SET Chunks = :chunks',
                    ExpressionAttributeValues: {
                        ':chunks': updatedChunks
                    }
                }));
            }

            logger.info('Successfully updated chunk status in both records', {
                jobId,
                chunkId,
                status,
                zipKey: zipKey || 'none',
                expirationTime: new Date(expirationTime * 1000).toISOString()
            });
        } catch (error) {
            logger.error('Failed to update chunk status', {
                jobId,
                chunkId,
                status,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    /**
     * Checks if all chunks for a job are completed
     * @param {string} jobId - The ID of the job
     * @returns {Promise<boolean>} Whether the job is complete
     */
    async isJobComplete(jobId) {
        try {
            const result = await this.docClient.send(new QueryCommand({
                TableName: this.tableName,
                KeyConditionExpression: 'JobId = :jobId AND ChunkId = :metadataId',
                ExpressionAttributeValues: {
                    ':jobId': jobId,
                    ':metadataId': 'metadata'
                }
            }));

            const metadata = result.Items[0];
            if (!metadata || !metadata.Chunks) {
                return false;
            }

            // Count completed chunks
            const completedChunks = metadata.Chunks.filter(
                chunk => chunk.status === 'COMPLETED'
            ).length;

            const isComplete = completedChunks === metadata.TotalChunks;

            // If all chunks are complete but job status isn't updated yet
            if (isComplete && metadata.Status !== 'COMPLETED') {
                try {
                    // Update the metadata record with completed status
                    await this.docClient.send(new UpdateCommand({
                        TableName: this.tableName,
                        Key: {
                            JobId: jobId,
                            ChunkId: 'metadata'
                        },
                        UpdateExpression: 'SET #st = :status, CompletedChunks = :completedChunks, UpdatedAt = :updatedAt',
                        ExpressionAttributeNames: {
                            '#st': 'Status'  // Use ExpressionAttributeNames for Status
                        },
                        ExpressionAttributeValues: {
                            ':status': 'COMPLETED',
                            ':completedChunks': completedChunks,
                            ':updatedAt': Date.now().toString()
                        }
                    }));

                    logger.info('Updated job status to COMPLETED', {
                        jobId,
                        completedChunks,
                        totalChunks: metadata.TotalChunks
                    });
                } catch (error) {
                    logger.error('Failed to update job completion status', {
                        jobId,
                        error: error.message,
                        stack: error.stack
                    });
                    throw error;
                }
            }

            return isComplete;
        } catch (error) {
            logger.error('Error checking job completion', {
                jobId,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    async getAllCompletedChunkKeys(jobId) {
        const result = await this.docClient.send(new QueryCommand({
            TableName: this.tableName,
            KeyConditionExpression: 'JobId = :jobId AND ChunkId = :metadataId',
            ExpressionAttributeValues: {
                ':jobId': jobId,
                ':metadataId': 'metadata'
            }
        }));

        const metadata = result.Items[0];
        if (!metadata) {
            throw new Error(`No metadata found for job ${jobId}`);
        }

        return metadata.Chunks
            .filter(chunk => chunk.status === 'COMPLETED')
            .map(chunk => chunk.zipKey);
    }

    async updateFinalZipLocation(jobId, finalZipKey) {
        try {
            // Modify the finalZipKey to use the standardized name
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const standardizedZipKey = `archives/${jobId}/final/prismix-job-results-${timestamp}.zip`;

            await this.docClient.send(new UpdateCommand({
                TableName: this.tableName,
                Key: {
                    JobId: jobId,
                    ChunkId: 'metadata'
                },
                UpdateExpression: 'SET #st = :status, FinalZipKey = :zipKey, CompletedAt = :completedAt',
                ExpressionAttributeNames: {
                    '#st': 'Status'
                },
                ExpressionAttributeValues: {
                    ':status': 'COMPLETED',
                    ':zipKey': standardizedZipKey,
                    ':completedAt': Date.now().toString()
                }
            }));

            logger.info('Successfully updated final ZIP location', {
                jobId,
                finalZipKey: standardizedZipKey
            });
        } catch (error) {
            logger.error('Failed to update final ZIP location', {
                jobId,
                finalZipKey,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }

    // Add to ZipArchiveProgressService class
    async updateJobStatus(jobId, status, version) {
        const params = {
            TableName: this.tableName,
            Key: {
                JobId: jobId,
                ChunkId: 'metadata'
            },
            UpdateExpression: 'SET #st = :status, version = :newVersion, updatedAt = :updatedAt',
            ExpressionAttributeNames: {
                '#st': 'Status'
            },
            ExpressionAttributeValues: {
                ':status': status,
                ':newVersion': (version || 0) + 1,
                ':updatedAt': Date.now().toString()
            }
        };

        if (version !== undefined) {
            params.ConditionExpression = 'attribute_not_exists(version) OR version = :currentVersion';
            params.ExpressionAttributeValues[':currentVersion'] = version;
        }

        try {
            await this.docClient.send(new UpdateCommand(params));
            logger.info('Successfully updated job status', {
                jobId,
                status,
                version: (version || 0) + 1
            });
        } catch (error) {
            logger.error('Failed to update job status', {
                jobId,
                status,
                error: error.message,
                stack: error.stack
            });
            throw error;
        }
    }
}

module.exports = ZipArchiveProgressService; 
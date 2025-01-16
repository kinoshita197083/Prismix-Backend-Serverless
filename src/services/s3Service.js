const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, ListObjectsV2Command, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { streamToBuffer } = require('../utils/helpers');
const { Upload } = require("@aws-sdk/lib-storage");
const { PassThrough } = require('stream');

const s3 = new S3Client();

class S3Service {
    constructor(s3Client) {
        this.s3Client = s3Client;
        this.uploadOptions = {
            queueSize: 4,
            partSize: 5 * 1024 * 1024,
            leavePartsOnError: false
        };
    }

    async uploadFile(params) {
        try {
            logger.info('Uploading file to S3', { params });
            const command = new PutObjectCommand(params);
            const result = await this.s3Client.send(command);
            return `https://${params.Bucket}.s3.amazonaws.com/${params.Key}`;
        } catch (error) {
            logger.error('Error uploading file to S3', { error, params });
            throw new AppError('Failed to upload file', 500);
        }
    }

    async getFile(params) {
        try {
            logger.info('Getting file from S3', { params });
            const command = new GetObjectCommand(params);
            const result = await this.s3Client.send(command);
            logger.debug(`Successfully fetched image from S3: ${params.Key}`);
            return result.Body;
        } catch (error) {
            logger.error('Error getting file from S3', { error, params });
            throw new AppError('Failed to retrieve file', 500);
        }
    }

    async deleteFile(params) {
        try {
            logger.info('Deleting file from S3', { params });
            const command = new DeleteObjectCommand(params);
            await this.s3Client.send(command);
        } catch (error) {
            logger.error('Error deleting file from S3', { error, params });
            throw new AppError('Failed to delete file', 500);
        }
    }

    async listFiles(params) {
        try {
            logger.info('Listing files in S3', { params });
            const command = new ListObjectsV2Command(params);
            const result = await this.s3Client.send(command);
            return result.Contents;
        } catch (error) {
            logger.error('Error listing files in S3', { error, params });
            throw new AppError('Failed to list files', 500);
        }
    }

    async getSignedUrl(operation, params) {
        try {
            logger.info('Getting signed URL for S3 operation', { operation, params });
            let command;
            switch (operation) {
                case 'getObject':
                    command = new GetObjectCommand(params);
                    break;
                case 'putObject':
                    command = new PutObjectCommand(params);
                    break;
                default:
                    throw new Error(`Unsupported operation: ${operation}`);
            }
            return await getSignedUrl(this.s3Client, command, { expiresIn: 3600 }); // URL expires in 1 hour
        } catch (error) {
            logger.error('Error getting signed URL for S3 operation', { error, operation, params });
            throw new AppError('Failed to generate signed URL', 500);
        }
    }

    async getFileBuffer(bucket, key) {
        try {
            logger.info('Getting file buffer from S3', { bucket, key });
            const params = {
                Bucket: bucket,
                Key: key
            };
            const response = await this.getFile(params);
            const buffer = await streamToBuffer(response);
            return buffer;
        } catch (error) {
            logger.error('Error getting file buffer from S3', { error, bucket, key });
            throw new AppError('Failed to get file buffer', 500);
        }
    }

    async getObjectMetadata(bucket, key) {
        try {
            const { Metadata } = await this.s3Client.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key
            }));
            return Metadata || {};
        } catch (error) {
            logger.error('Error getting object metadata', { error, bucket, key });
            return {};
        }
    }

    async uploadStream(bucket, key, stream) {
        const upload = new Upload({
            client: this.s3Client,
            params: { Bucket: bucket, Key: key, Body: stream },
            ...this.uploadOptions
        });

        return upload.done();
    }

    async downloadStream(bucket, key) {
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key
        });
        return (await this.s3Client.send(command)).Body;
    }

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

    createUploadStream(bucket, key) {
        const passThrough = new PassThrough();

        const upload = new Upload({
            client: this.s3Client,
            params: {
                Bucket: bucket,
                Key: key,
                Body: passThrough
            },
            queueSize: 4,
            partSize: 5 * 1024 * 1024
        });

        return { passThrough, upload };
    }

};

module.exports = new S3Service(s3);

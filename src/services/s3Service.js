const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, ListObjectsV2Command, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const { streamToBuffer } = require('../utils/helpers');

const s3 = new S3Client();

const s3Service = {
    async uploadFile(params) {
        try {
            logger.info('Uploading file to S3', { params });
            const command = new PutObjectCommand(params);
            const result = await s3.send(command);
            return `https://${params.Bucket}.s3.amazonaws.com/${params.Key}`;
        } catch (error) {
            logger.error('Error uploading file to S3', { error, params });
            throw new AppError('Failed to upload file', 500);
        }
    },

    async getFile(params) {
        try {
            logger.info('Getting file from S3', { params });
            const command = new GetObjectCommand(params);
            const result = await s3.send(command);
            logger.debug(`Successfully fetched image from S3: ${params.Key}`);
            return result.Body;
        } catch (error) {
            logger.error('Error getting file from S3', { error, params });
            throw new AppError('Failed to retrieve file', 500);
        }
    },

    async deleteFile(params) {
        try {
            logger.info('Deleting file from S3', { params });
            const command = new DeleteObjectCommand(params);
            await s3.send(command);
        } catch (error) {
            logger.error('Error deleting file from S3', { error, params });
            throw new AppError('Failed to delete file', 500);
        }
    },

    async listFiles(params) {
        try {
            logger.info('Listing files in S3', { params });
            const command = new ListObjectsV2Command(params);
            const result = await s3.send(command);
            return result.Contents;
        } catch (error) {
            logger.error('Error listing files in S3', { error, params });
            throw new AppError('Failed to list files', 500);
        }
    },

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
            return await getSignedUrl(s3, command, { expiresIn: 3600 }); // URL expires in 1 hour
        } catch (error) {
            logger.error('Error getting signed URL for S3 operation', { error, operation, params });
            throw new AppError('Failed to generate signed URL', 500);
        }
    },

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
    },

    async getObjectMetadata(bucket, key) {
        try {
            const { Metadata } = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key
            }));
            return Metadata || {};
        } catch (error) {
            logger.error('Error getting object metadata', { error, bucket, key });
            return {};
        }
    },
};

module.exports = s3Service;

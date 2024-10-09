const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { S3Client } = require("@aws-sdk/client-s3");

const s3 = new S3Client();

const s3Service = {
    async uploadFile(params) {
        try {
            logger.info('Uploading file to S3', { params });
            const result = await s3.upload(params).promise();
            return result.Location;
        } catch (error) {
            logger.error('Error uploading file to S3', { error, params });
            throw new AppError('Failed to upload file', 500);
        }
    },

    async getFile(params) {
        try {
            logger.info('Getting file from S3', { params });
            const result = await s3.getObject(params).promise();
            return result.Body;
        } catch (error) {
            logger.error('Error getting file from S3', { error, params });
            throw new AppError('Failed to retrieve file', 500);
        }
    },

    async deleteFile(params) {
        try {
            logger.info('Deleting file from S3', { params });
            await s3.deleteObject(params).promise();
        } catch (error) {
            logger.error('Error deleting file from S3', { error, params });
            throw new AppError('Failed to delete file', 500);
        }
    },

    async listFiles(params) {
        try {
            logger.info('Listing files in S3', { params });
            const result = await s3.listObjectsV2(params).promise();
            return result.Contents;
        } catch (error) {
            logger.error('Error listing files in S3', { error, params });
            throw new AppError('Failed to list files', 500);
        }
    },

    async getSignedUrl(operation, params) {
        try {
            logger.info('Getting signed URL for S3 operation', { operation, params });
            return s3.getSignedUrlPromise(operation, params);
        } catch (error) {
            logger.error('Error getting signed URL for S3 operation', { error, operation, params });
            throw new AppError('Failed to generate signed URL', 500);
        }
    }
};

module.exports = s3Service;
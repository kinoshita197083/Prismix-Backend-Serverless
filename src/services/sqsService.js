const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { SQSClient } = require("@aws-sdk/client-sqs");

const sqs = new SQSClient();

const sqsService = {
    async sendMessage(params) {
        try {
            logger.info('Sending message to SQS', { params });
            const result = await sqs.sendMessage(params).promise();
            return result.MessageId;
        } catch (error) {
            logger.error('Error sending message to SQS', { error, params });
            throw new AppError('Failed to send message to queue', 500);
        }
    },

    async receiveMessage(params) {
        try {
            logger.info('Receiving messages from SQS', { params });
            const result = await sqs.receiveMessage(params).promise();
            return result.Messages;
        } catch (error) {
            logger.error('Error receiving messages from SQS', { error, params });
            throw new AppError('Failed to receive messages from queue', 500);
        }
    },

    async deleteMessage(params) {
        try {
            logger.info('Deleting message from SQS', { params });
            await sqs.deleteMessage(params).promise();
        } catch (error) {
            logger.error('Error deleting message from SQS', { error, params });
            throw new AppError('Failed to delete message from queue', 500);
        }
    },

    async sendBatchMessages(params) {
        try {
            logger.info('Sending batch messages to SQS', { params });
            const result = await sqs.sendMessageBatch(params).promise();
            return result.Successful;
        } catch (error) {
            logger.error('Error sending batch messages to SQS', { error, params });
            throw new AppError('Failed to send batch messages to queue', 500);
        }
    }
};

module.exports = sqsService;
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');

const dynamoDb = new DynamoDBClient();

const dynamoService = {
    async getItem(params) {
        try {
            logger.info('Getting item from DynamoDB', { params });
            const result = await dynamoDb.get(params).promise();
            return result.Item;
        } catch (error) {
            logger.error('Error getting item from DynamoDB', { error, params });
            throw new AppError('Failed to retrieve item from database', 500);
        }
    },

    async putItem(params) {
        try {
            logger.info('Putting item into DynamoDB', { params });
            await dynamoDb.put(params).promise();
        } catch (error) {
            logger.error('Error putting item into DynamoDB', { error, params });
            throw new AppError('Failed to save item to database', 500);
        }
    },

    async queryItems(params) {
        try {
            logger.info('Querying items from DynamoDB', { params });
            const result = await dynamoDb.query(params).promise();
            return result.Items;
        } catch (error) {
            logger.error('Error querying items from DynamoDB', { error, params });
            throw new AppError('Failed to query items from database', 500);
        }
    },

    async updateItem(params) {
        try {
            logger.info('Updating item in DynamoDB', { params });
            await dynamoDb.update(params).promise();
        } catch (error) {
            logger.error('Error updating item in DynamoDB', { error, params });
            throw new AppError('Failed to update item in database', 500);
        }
    },

    async deleteItem(params) {
        try {
            logger.info('Deleting item from DynamoDB', { params });
            await dynamoDb.delete(params).promise();
        } catch (error) {
            logger.error('Error deleting item from DynamoDB', { error, params });
            throw new AppError('Failed to delete item from database', 500);
        }
    }
};

module.exports = dynamoService;
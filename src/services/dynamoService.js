const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand, DeleteCommand } = require("@aws-sdk/lib-dynamodb");
const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const dynamoService = {
    async getItem(params) {
        try {
            logger.info('Getting item from DynamoDB', { params });
            const command = new GetCommand(params);
            const result = await docClient.send(command);
            return result.Item;
        } catch (error) {
            logger.error('Error getting item from DynamoDB', { error, params });
            throw new AppError('Failed to retrieve item from database', 500);
        }
    },

    async putItem(params) {
        try {
            logger.info('Putting item into DynamoDB', { params });
            const command = new PutCommand(params);
            await docClient.send(command);
        } catch (error) {
            logger.error('Error putting item into DynamoDB', { error, params });
            throw new AppError('Failed to save item to database', 500);
        }
    },

    async queryItems(params) {
        try {
            logger.info('Querying items from DynamoDB', { params });
            const command = new QueryCommand(params);
            const result = await docClient.send(command);
            return result.Items;
        } catch (error) {
            logger.error('Error querying items from DynamoDB', { error, params });
            throw new AppError('Failed to query items from database', 500);
        }
    },

    async updateItem(params) {
        try {
            logger.info('Updating item in DynamoDB', JSON.stringify(params, null, 2));
            const command = new UpdateCommand(params);
            await docClient.send(command);
        } catch (error) {
            logger.error('Error updating item in DynamoDB', error);
            throw new AppError(`Failed to update item in database: ${error.message}`, 500);
        }
    },

    async deleteItem(params) {
        try {
            logger.info('Deleting item from DynamoDB', { params });
            const command = new DeleteCommand(params);
            await docClient.send(command);
        } catch (error) {
            logger.error('Error deleting item from DynamoDB', { error, params });
            throw new AppError('Failed to delete item from database', 500);
        }
    },

    async updateTaskStatus({ jobId, imageS3Key, taskId, status, labels, evaluation }) {
        logger.info('Updating task status', { jobId, imageS3Key, taskId, status, labels, evaluation });

        const updateParams = {
            TableName: process.env.TASKS_TABLE || 'prismix-serverless-dev-Tasks',
            Key: {
                JobID: jobId,
                TaskID: taskId
            },
            UpdateExpression: 'SET TaskStatus = :status, UpdatedAt = :time',
            ExpressionAttributeValues: {
                ':status': status,
                ':time': new Date().toISOString()
            }
        };

        if (labels) {
            updateParams.UpdateExpression += ', ProcessingResult = :result';
            updateParams.ExpressionAttributeValues[':result'] = labels;
        }

        if (evaluation) {
            updateParams.UpdateExpression += ', Evaluation = :evaluation';
            updateParams.ExpressionAttributeValues[':evaluation'] = evaluation;
        }

        if (imageS3Key) {
            updateParams.UpdateExpression += ', ImageS3Key = :imageS3Key';
            updateParams.ExpressionAttributeValues[':imageS3Key'] = imageS3Key;
        }

        try {
            logger.info('Updating task status in DynamoDB ...');
            await this.updateItem(updateParams);
            logger.info('Successfully updated task status');
        } catch (error) {
            logger.error('Error updating task status in DynamoDB', error);
            throw new AppError('Failed to update task status', 500);
        }
    }
};

module.exports = dynamoService;
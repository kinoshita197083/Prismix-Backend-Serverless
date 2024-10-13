const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand, DeleteCommand, UpdateItemCommand } = require("@aws-sdk/lib-dynamodb");
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

    async updateTaskStatus({ jobId, imageS3Key, taskId, status, labels = [], evaluation = '', isDuplicate = false, duplicateOf = null }) {
        const params = {
            TableName: process.env.TASKS_TABLE,
            Key: {
                JobID: { S: jobId },
                TaskID: { S: taskId }
            },
            UpdateExpression: 'SET TaskStatus = :status, ImageS3Key = :imageS3Key, ProcessingResult = :labels, Evaluation = :evaluation, UpdatedAt = :updatedAt',
            ExpressionAttributeValues: {
                ':status': { S: status },
                ':imageS3Key': { S: imageS3Key },
                ':labels': { L: labels },
                ':evaluation': { S: evaluation },
                ':updatedAt': { S: new Date().toISOString() }
            },
            ReturnValues: 'ALL_NEW'
        };

        if (isDuplicate) {
            params.UpdateExpression += ', IsDuplicate = :isDuplicate, DuplicateOf = :duplicateOf';
            params.ExpressionAttributeValues[':isDuplicate'] = { BOOL: true };
            params.ExpressionAttributeValues[':duplicateOf'] = { S: duplicateOf };
        }

        try {
            const command = new UpdateItemCommand(params);
            const result = await docClient.send(command);
            logger.info('Task status updated successfully', { jobId, taskId, status });
            return result.Attributes;
        } catch (error) {
            logger.error('Error updating task status', { error, jobId, taskId, status });
            throw new AppError('Failed to update task status', 500);
        }
    }
};

module.exports = dynamoService;

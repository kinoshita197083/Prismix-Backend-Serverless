const { DynamoDBClient, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand, DeleteCommand } = require("@aws-sdk/lib-dynamodb");
const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

const dynamoService = {
    async getItem(tableName, key) {
        try {
            logger.info('Getting item from DynamoDB', { tableName, key });
            const command = new GetCommand({ TableName: tableName, Key: key });
            const result = await docClient.send(command);
            return result.Item;
        } catch (error) {
            logger.error('Error getting item from DynamoDB', { error, tableName, key });
            throw new AppError('Failed to retrieve item from database', 500);
        }
    },

    async putItem(tableName, item) {
        try {
            logger.info('Putting item into DynamoDB', { tableName, item });
            const command = new PutCommand({ TableName: tableName, Item: item });
            await docClient.send(command);
        } catch (error) {
            logger.error('Error putting item into DynamoDB', { error, tableName, item });
            throw new AppError('Failed to save item to database', 500);
        }
    },

    async queryItems(tableName, keyConditionExpression, expressionAttributeValues) {
        try {
            logger.info('Querying items from DynamoDB', { tableName, keyConditionExpression, expressionAttributeValues });
            const command = new QueryCommand({
                TableName: tableName,
                KeyConditionExpression: keyConditionExpression,
                ExpressionAttributeValues: expressionAttributeValues
            });
            const result = await docClient.send(command);
            return result.Items;
        } catch (error) {
            logger.error('Error querying items from DynamoDB', { error, tableName, keyConditionExpression, expressionAttributeValues });
            throw new AppError('Failed to query items from database', 500);
        }
    },

    async updateItem(tableName, key, updateExpression, expressionAttributeValues, expressionAttributeNames) {
        try {
            logger.info('Updating item in DynamoDB', { tableName, key, updateExpression, expressionAttributeValues, expressionAttributeNames });
            const command = new UpdateCommand({
                TableName: tableName,
                Key: key,
                UpdateExpression: updateExpression,
                ExpressionAttributeValues: expressionAttributeValues,
                ExpressionAttributeNames: expressionAttributeNames,
                ReturnValues: 'ALL_NEW'
            });
            const result = await docClient.send(command);
            return result.Attributes;
        } catch (error) {
            logger.error('Error updating item in DynamoDB', { error, tableName, key, updateExpression });
            throw new AppError(`Failed to update item in database: ${error.message}`, 500);
        }
    },

    async deleteItem(tableName, key) {
        try {
            logger.info('Deleting item from DynamoDB', { tableName, key });
            const command = new DeleteCommand({ TableName: tableName, Key: key });
            await docClient.send(command);
        } catch (error) {
            logger.error('Error deleting item from DynamoDB', { error, tableName, key });
            throw new AppError('Failed to delete item from database', 500);
        }
    },

    async updateTaskStatus({ jobId, taskId, status, imageS3Key, labels = [], evaluation = '', isDuplicate = false, duplicateOf = null }) {
        const key = { JobID: jobId, TaskID: taskId };
        let updateExpression = 'SET TaskStatus = :status, ImageS3Key = :imageS3Key, ProcessingResult = :labels, Evaluation = :evaluation, UpdatedAt = :updatedAt';
        let expressionAttributeValues = {
            ':status': status,
            ':imageS3Key': imageS3Key,
            ':labels': labels,
            ':evaluation': evaluation,
            ':updatedAt': new Date().toISOString()
        };

        if (isDuplicate) {
            updateExpression += ', IsDuplicate = :isDuplicate, DuplicateOf = :duplicateOf';
            expressionAttributeValues[':isDuplicate'] = true;
            expressionAttributeValues[':duplicateOf'] = duplicateOf || null;
        }

        try {
            return await this.updateItem(process.env.TASKS_TABLE, key, updateExpression, expressionAttributeValues);
        } catch (error) {
            logger.error('Error updating task status', { error: error.message, jobId, taskId, status });
            throw new AppError('Failed to update task status', 500);
        }
    },

    async updateJobProgress(jobId, evaluation) {
        const key = { JobId: jobId };
        let updateExpression = 'SET ProcessedImages = ProcessedImages + :inc, #LastUpdateTime = :now';
        let expressionAttributeValues = {
            ':inc': 1,
            ':now': Date.now()
        };
        let expressionAttributeNames = {
            '#LastUpdateTime': 'LastUpdateTime'
        };

        switch (evaluation) {
            case 'ELIGIBLE':
                updateExpression += ', EligibleImages = EligibleImages + :inc';
                break;
            case 'EXCLUDED':
                updateExpression += ', ExcludedImages = ExcludedImages + :inc';
                break;
            case 'DUPLICATE':
                updateExpression += ', DuplicateImages = DuplicateImages + :inc';
                break;
            case 'FAILED':
                updateExpression += ', FailedImages = FailedImages + :inc';
                break;
        }

        try {
            return await this.updateItem(process.env.JOB_PROGRESS_TABLE, key, updateExpression, expressionAttributeValues, expressionAttributeNames);
        } catch (error) {
            logger.error('Error updating job progress', { error: error.message, jobId, evaluation });
            throw new AppError('Failed to update job progress', 500);
        }
    }
};

module.exports = dynamoService;

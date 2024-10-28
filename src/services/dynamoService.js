const { DynamoDBClient, GetItemCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand, DeleteCommand } = require("@aws-sdk/lib-dynamodb");
const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { IMAGE_HASH_EXPIRATION_TIME, NOW, DUPLICATE, COMPLETED, FAILED } = require('../utils/config');

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

    async updateTaskStatus({
        jobId,
        taskId,
        imageS3Key,
        status,
        labels,
        evaluation,
        duplicateOf,
        duplicateOfS3Key,
        reason,
        processingDetails
    }) {
        console.log('Updating task status with:', { jobId, taskId, imageS3Key, status, labels, evaluation, duplicateOf, duplicateOfS3Key, reason });

        const updateExpression = [
            'TaskStatus = :status',
            'ImageS3Key = :imageS3Key',
            'Evaluation = :evaluation',
            'ProcessingDetails = :processingDetails',
            'DuplicateOf = :duplicateOf',
            'DuplicateOfS3Key = :duplicateOfS3Key',
            'Reason = :reason',
            'UpdatedAt = :updatedAt'
        ];
        const expressionAttributeValues = {
            ':status': status,
            ':imageS3Key': imageS3Key,
            ':evaluation': evaluation,
            ':processingDetails': {
                ...processingDetails,
                processedAt: NOW,
                originalS3Key: imageS3Key
            },
            ':duplicateOf': duplicateOf,
            ':duplicateOfS3Key': duplicateOfS3Key,
            ':reason': reason,
            ':updatedAt': NOW
        };

        const params = {
            TableName: process.env.TASKS_TABLE,
            Key: { JobID: jobId, TaskID: taskId },
            UpdateExpression: 'SET ' + updateExpression.join(', '),
            ExpressionAttributeValues: {
                ...expressionAttributeValues,
                ':completed': COMPLETED  // For conditional expression
            },
            ConditionExpression: '(attribute_not_exists(Evaluation) OR Evaluation <> :completed)',
            ReturnValues: 'ALL_NEW'
        };

        // console.log('DynamoDB update params:', JSON.stringify(params, null, 2));

        try {
            const command = new UpdateCommand(params);
            const result = await docClient.send(command);
            logger.info('Task status updated successfully', { jobId, taskId, status });
            return result.Attributes;
        } catch (error) {
            logger.error('Error updating task status', {
                error: error.message,
                stack: error.stack,
                jobId,
                taskId,
                status,
                params: JSON.stringify(params)
            });
            throw new Error('DynamoService: Failed to update task status');
        }
    },

    async getImageHash(hash, jobId) {
        const command = new GetCommand({
            TableName: process.env.IMAGE_HASH_TABLE,
            Key: { HashValue: hash, JobId: jobId },
            ConsistentRead: true
        });
        const result = await docClient.send(command);
        return result.Item;
    },

    async putImageHash(hash, jobId, imageId, imageS3Key) {
        const command = new PutCommand({
            TableName: process.env.IMAGE_HASH_TABLE,
            Item: {
                HashValue: hash,
                JobId: jobId,
                ImageId: imageId,
                ImageS3Key: imageS3Key,
                Timestamp: NOW,
                ExpirationTime: IMAGE_HASH_EXPIRATION_TIME // 3 days
            },
            ConditionExpression: 'attribute_not_exists(HashValue) AND attribute_not_exists(JobId)'
        });
        await docClient.send(command);
    },

    async updateTaskStatusAsFailed({ jobId, imageId, s3ObjectKey, reason }) {
        return await this.updateTaskStatus({
            jobId,
            taskId: imageId,
            status: COMPLETED,
            evaluation: FAILED,
            imageS3Key: s3ObjectKey,
            reason,
            updatedAt: NOW
        });
    },

    async updateTaskStatusAsDuplicate({
        jobId,
        imageId,
        s3ObjectKey,
        originalImageId,
        originalImageS3Key
    }) {
        console.log('Updating task status to COMPLETED and marking as duplicate...',
            { jobId, imageId, s3ObjectKey, originalImageId, originalImageS3Key });

        // Update task status to COMPLETED and mark as duplicate
        return await this.updateTaskStatus({
            jobId,
            taskId: imageId,
            status: COMPLETED,
            evaluation: DUPLICATE,
            imageS3Key: s3ObjectKey,
            duplicateOf: originalImageId,
            duplicateOfS3Key: originalImageS3Key,
            updatedAt: NOW,
        });
    },

    async updateTaskStatusWithQualityIssues({
        jobId,
        imageId,
        s3ObjectKey,
        qualityIssues
    }) {
        console.log('Updating task status with quality issues...', { jobId, imageId, s3ObjectKey, qualityIssues });

        return await this.updateTaskStatus({
            jobId,
            taskId: imageId,
            status: COMPLETED,
            evaluation: FAILED,
            imageS3Key: s3ObjectKey,
            reason: qualityIssues,
            updatedAt: NOW,
        });
    }
};

module.exports = dynamoService;

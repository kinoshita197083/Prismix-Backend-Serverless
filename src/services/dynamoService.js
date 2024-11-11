const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, UpdateCommand, DeleteCommand, BatchWriteCommand } = require("@aws-sdk/lib-dynamodb");
const logger = require('../utils/logger');
const { AppError } = require('../utils/errorHandler');
const { IMAGE_HASH_EXPIRATION_TIME, DUPLICATE, COMPLETED, FAILED, EXCLUDED, ELIGIBLE } = require('../utils/config');
const { chunkArray } = require('../utils/helpers');

const client = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(client, {
    marshallOptions: {
        removeUndefinedValues: true
    }
});

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
        console.log('Updating task status with:', { jobId, taskId, imageS3Key, status, labels, processingDetails, evaluation, duplicateOf, duplicateOfS3Key, reason, expirationTime });

        const updateExpression = [
            'TaskStatus = :status',
            'Evaluation = :evaluation',
            'UpdatedAt = :updatedAt'
        ];
        const expressionAttributeValues = {
            ':status': status,
            ':evaluation': evaluation,
            ':updatedAt': Date.now().toString()
        };

        if (imageS3Key) {
            updateExpression.push('ImageS3Key = :imageS3Key')
            expressionAttributeValues[':imageS3Key'] = imageS3Key;
        }

        if (duplicateOf) {
            updateExpression.push('DuplicateOf = :duplicateOf')
            expressionAttributeValues[':duplicateOf'] = duplicateOf;
        }

        if (duplicateOfS3Key) {
            updateExpression.push('DuplicateOfS3Key = :duplicateOfS3Key')
            expressionAttributeValues[':duplicateOfS3Key'] = duplicateOfS3Key;
        }

        if (reason) {
            updateExpression.push('Reason = :reason')
            expressionAttributeValues[':reason'] = reason;
        }

        if (expirationTime) {
            updateExpression.push('ExpirationTime = :expirationTime')
            expressionAttributeValues[':expirationTime'] = expirationTime;
        }

        if (processingDetails) {
            updateExpression.push('ProcessingDetails = :processingDetails')
            expressionAttributeValues[':processingDetails'] = {
                ...processingDetails,
                processedAt: Date.now().toString(),
                originalS3Key: imageS3Key
            };
        }

        const params = {
            TableName: process.env.TASKS_TABLE,
            Key: { JobID: jobId, TaskID: taskId },
            UpdateExpression: 'SET ' + updateExpression.join(', '),
            ExpressionAttributeValues: {
                ...expressionAttributeValues,
                ':completed': COMPLETED  // For conditional expression
            },
            ConditionExpression: 'attribute_not_exists(Evaluation) OR (TaskStatus <> :completed)',
            ReturnValues: 'ALL_NEW'
        };

        try {
            const command = new UpdateCommand(params);
            const result = await docClient.send(command);
            logger.info('Task status updated successfully', { jobId, taskId, status });
            return result.Attributes;
        } catch (error) {
            // Ignore ConditionalCheckFailedException as it may be due to race conditions
            if (error.name === 'ConditionalCheckFailedException') {
                logger.warn('Conditional check failed while updating task status - likely due to race condition or this task has already been processed', {
                    jobId,
                    taskId,
                    status,
                    evaluation
                });
                // Fetch and return the current state of the item
                const currentItem = await this.getItem(process.env.TASKS_TABLE, { JobID: jobId, TaskID: taskId });
                return currentItem;
            }

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
                Timestamp: Date.now().toString(),
                ExpirationTime: IMAGE_HASH_EXPIRATION_TIME // 3 days
            },
            ConditionExpression: 'attribute_not_exists(HashValue) AND attribute_not_exists(JobId)'
        });
        await docClient.send(command);
    },

    async updateTaskStatusAsFailed({ jobId, taskId, imageS3Key, reason, preserveFileDays }) {
        return await this.updateTaskStatus({
            jobId,
            taskId,
            status: COMPLETED,
            evaluation: FAILED,
            imageS3Key,
            reason,
            expirationTime: (Date.now() + preserveFileDays * 24 * 60 * 60 * 1000).toString(), // User defined number of days to preserve the file
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
            updatedAt: Date.now().toString(),
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
            evaluation: EXCLUDED,
            imageS3Key: s3ObjectKey,
            reason: qualityIssues,
            updatedAt: Date.now().toString(),
        });
    },

    async getTaskCount(jobId, options = { status: COMPLETED }) {
        try {
            const params = {
                TableName: process.env.TASKS_TABLE,
                KeyConditionExpression: 'JobID = :jobId',
                FilterExpression: 'TaskStatus = :status',
                ExpressionAttributeValues: {
                    ':jobId': jobId,
                    ':status': options.status
                },
                Select: 'COUNT' // Only get the count of matching items
            };

            const result = await docClient.send(new QueryCommand(params));
            return result.Count;
        } catch (error) {
            logger.error('Error getting task count', { error, jobId, options });
            throw error;
        }
    },

    async autoReviewAllRemainingTasks(jobId) {
        try {
            const queryCommand = new QueryCommand({
                TableName: process.env.TASKS_TABLE,
                KeyConditionExpression: "JobID = :jobId",
                FilterExpression: "Evaluation <> :eligible AND Evaluation <> :failed",
                ExpressionAttributeValues: {
                    ":jobId": jobId,
                    ":eligible": ELIGIBLE,
                    ":failed": FAILED
                }
            });
            const result = await docClient.send(queryCommand);
            const tasks = result.Items || [];

            const batchedTasks = chunkArray(tasks, 25);

            // Process each batch
            await Promise.all(batchedTasks.map(async (batch) => {
                const batchWriteCommand = new BatchWriteCommand({
                    RequestItems: {
                        [process.env.TASKS_TABLE]: batch.map(task => ({
                            PutRequest: {
                                Item: {
                                    ...task,
                                    TaskStatus: "REVIEWED",
                                    UpdatedAt: Date.now().toString()
                                }
                            }
                        }))
                    }
                });

                await docClient.send(batchWriteCommand);
            }));
        } catch (error) {
            logger.error('Error auto-reviewing all remaining tasks', { error, jobId });
            throw error;
        }
    }
};

module.exports = dynamoService;

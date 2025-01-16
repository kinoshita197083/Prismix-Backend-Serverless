const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const { ELIGIBLE } = require('../utils/config');

const ddbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(ddbClient);

// Default pagination configuration
const DEFAULT_PAGINATION_CONFIG = {
    dynamoPageSize: 1000,
    maxPages: 100
};

const createTaskService = (config = DEFAULT_PAGINATION_CONFIG) => {
    const fetchEligibleTasks = async (jobId, lastEvaluatedKey = null) => {
        logger.info(`Fetching eligible tasks for job ${jobId}`, {
            lastEvaluatedKey: lastEvaluatedKey ? JSON.stringify(lastEvaluatedKey) : null
        });

        const params = {
            TableName: process.env.TASKS_TABLE,
            KeyConditionExpression: 'JobID = :jobId',
            FilterExpression: 'Evaluation = :evaluation',
            ExpressionAttributeValues: {
                ':jobId': jobId,
                ':evaluation': ELIGIBLE
            },
            Limit: config.dynamoPageSize
        };

        if (lastEvaluatedKey && typeof lastEvaluatedKey === 'object') {
            params.ExclusiveStartKey = lastEvaluatedKey;
        }

        try {
            const command = new QueryCommand(params);
            const result = await docClient.send(command);

            logger.info(`Fetched batch of eligible tasks`, {
                count: result.Items.length,
                hasMore: !!result.LastEvaluatedKey,
                lastEvaluatedKey: result.LastEvaluatedKey
            });

            return {
                items: result.Items,
                lastEvaluatedKey: result.LastEvaluatedKey
            };
        } catch (error) {
            logger.error(`Error fetching eligible tasks for job ${jobId}:`, {
                error: error.message,
                params
            });
            throw error;
        }
    };

    async function* fetchAllEligibleTasks(jobId, lastEvaluatedKey) {
        let pageCount = 0;

        do {
            if (pageCount >= config.maxPages) {
                logger.warn(`Reached maximum page limit for job ${jobId}`);
                break;
            }

            const result = await fetchEligibleTasks(jobId, lastEvaluatedKey);
            lastEvaluatedKey = result.lastEvaluatedKey;
            pageCount++;

            yield {
                items: result.items,
                lastEvaluatedKey
            };
        } while (lastEvaluatedKey);

        logger.info(`Completed fetching all eligible tasks`, {
            jobId,
            totalPages: pageCount
        });
    }

    return {
        fetchEligibleTasks,
        fetchAllEligibleTasks
    };
};

module.exports = createTaskService; 
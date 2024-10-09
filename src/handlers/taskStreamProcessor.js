const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const { PrismaClient } = require('@prisma/client');

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const prisma = new PrismaClient();

exports.handler = async (event) => {
    try {
        for (const record of event.Records) {
            if (record.eventName === 'MODIFY') {
                const newImage = unmarshall(record.dynamodb.NewImage);

                if (newImage.TaskStatus === 'COMPLETED') {
                    await checkJobCompletion(newImage.JobID);
                }
            }
        }
    } catch (error) {
        console.error('Error:', error);
        throw error;
    } finally {
        await prisma.$disconnect();
    }
};

async function checkJobCompletion(jobId) {
    const params = {
        TableName: 'Tasks',
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'TaskStatus <> :completedStatus',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':completedStatus': 'COMPLETED'
        },
        Limit: 1
    };

    const command = new QueryCommand(params);
    const result = await docClient.send(command);

    if (result.Items.length === 0) {
        // All tasks are completed, update job status in RDS
        await updateJobStatus(jobId, 'COMPLETED');
        await triggerJobCompletionActions(jobId);
    }
}

async function updateJobStatus(jobId, status) {
    try {
        const job = await prisma.job.update({
            where: { id: jobId },
            data: { jobStatus: status }
        });

        if (!job) {
            throw new Error(`Job not found for id: ${jobId}`);
        }
    } catch (error) {
        console.error('Error updating job status:', error);
        throw error;
    }
}

async function triggerJobCompletionActions(jobId) {
    // Implement any additional actions here, such as:
    // - Sending notifications
    // - Triggering post-processing workflows
    // - Updating other systems
    console.log(`Job ${jobId} completed. Triggering completion actions.`);
}
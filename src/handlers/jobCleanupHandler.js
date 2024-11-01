const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, UpdateCommand, ScanCommand } = require("@aws-sdk/lib-dynamodb");
const { SNSClient, PublishCommand } = require("@aws-sdk/client-sns");
const { CloudWatchClient, PutMetricDataCommand } = require("@aws-sdk/client-cloudwatch");

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const snsClient = new SNSClient({});
const cloudWatch = new CloudWatchClient({});

const STALE_JOB_THRESHOLDS = {
    IN_PROGRESS: 24 * 60 * 60 * 1000,    // 24 hours
    WAITING_FOR_REVIEW: 96 * 60 * 60 * 1000, // 96 hours (72h + 24h extension)
    COMPLETED: 30 * 24 * 60 * 60 * 1000,  // 30 days
    FAILED: 7 * 24 * 60 * 60 * 1000      // 7 days
};

exports.handler = async (event) => {
    console.log('Starting job cleanup process');

    try {
        const cleanupStats = {
            staleJobsFound: 0,
            jobsCleaned: 0,
            errors: 0
        };

        // Find and process stale jobs
        const staleJobs = await findStaleJobs();
        cleanupStats.staleJobsFound = staleJobs.length;

        for (const job of staleJobs) {
            try {
                await handleStaleJob(job);
                cleanupStats.jobsCleaned++;
            } catch (error) {
                console.error('Error handling stale job:', error);
                cleanupStats.errors++;
            }
        }

        // Record cleanup metrics
        await recordCleanupMetrics(cleanupStats);

        return {
            statusCode: 200,
            body: JSON.stringify(cleanupStats)
        };
    } catch (error) {
        console.error('Error during cleanup process:', error);
        throw error;
    }
};

async function findStaleJobs() {
    const staleJobs = [];
    const currentTime = new Date().getTime();

    const scanParams = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        FilterExpression: 'attribute_exists(updatedAt)'
    };

    try {
        const result = await docClient.send(new ScanCommand(scanParams));

        for (const job of result.Items || []) {
            const threshold = STALE_JOB_THRESHOLDS[job.status] || STALE_JOB_THRESHOLDS.COMPLETED;
            const lastUpdateTime = new Date(job.updatedAt).getTime();

            if (currentTime - lastUpdateTime > threshold) {
                staleJobs.push(job);
            }
        }

        return staleJobs;
    } catch (error) {
        console.error('Error finding stale jobs:', error);
        throw error;
    }
}

async function handleStaleJob(job) {
    const currentTime = new Date().toISOString();

    // Different handling based on job status
    switch (job.status) {
        case 'IN_PROGRESS':
            await markJobAsFailed(job.JobId, 'STALE_JOB', 'Job exceeded maximum processing time');
            break;

        case 'WAITING_FOR_REVIEW':
            await autoApproveAndComplete(job);
            break;

        case 'COMPLETED':
        case 'FAILED':
            await archiveJob(job);
            break;
    }

    // Notify about job cleanup
    await notifyJobCleanup(job);
}

async function markJobAsFailed(jobId, reason, details) {
    const updateParams = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        UpdateExpression: 'SET #status = :status, cleanupDetails = :details, cleanedUpAt = :time',
        ExpressionAttributeNames: {
            '#status': 'status'
        },
        ExpressionAttributeValues: {
            ':status': 'FAILED',
            ':details': {
                reason,
                details,
                timestamp: new Date().toISOString()
            },
            ':time': new Date().toISOString()
        }
    };

    await docClient.send(new UpdateCommand(updateParams));
}

async function autoApproveAndComplete(job) {
    // Auto-approve remaining tasks
    const tasksParams = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: '#status = :waitingStatus',
        ExpressionAttributeNames: {
            '#status': 'TaskStatus'
        },
        ExpressionAttributeValues: {
            ':jobId': job.JobId,
            ':waitingStatus': 'WAITING_FOR_REVIEW'
        }
    };

    const tasks = await docClient.send(new QueryCommand(tasksParams));

    for (const task of tasks.Items || []) {
        await docClient.send(new UpdateCommand({
            TableName: process.env.TASKS_TABLE,
            Key: { JobID: job.JobId, TaskID: task.TaskID },
            UpdateExpression: 'SET TaskStatus = :status, autoApproved = :auto, autoApprovedAt = :time',
            ExpressionAttributeValues: {
                ':status': 'COMPLETED',
                ':auto': true,
                ':time': new Date().toISOString()
            }
        }));
    }

    // Mark job as completed
    await markJobAsCompleted(job.JobId, 'AUTO_APPROVED', tasks.Items.length);
}

async function archiveJob(job) {
    // Archive job data or perform cleanup
    // This could involve moving data to a cheaper storage solution
    // or adding archive flags for filtering
    const updateParams = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: job.JobId },
        UpdateExpression: 'SET archived = :archived, archivedAt = :time',
        ExpressionAttributeValues: {
            ':archived': true,
            ':time': new Date().toISOString()
        }
    };

    await docClient.send(new UpdateCommand(updateParams));
}

async function notifyJobCleanup(job) {
    const message = {
        jobId: job.JobId,
        action: 'CLEANUP',
        previousStatus: job.status,
        cleanupReason: job.cleanupDetails?.reason,
        timestamp: new Date().toISOString()
    };

    await snsClient.send(new PublishCommand({
        TopicArn: process.env.JOB_COMPLETION_TOPIC_ARN,
        Message: JSON.stringify(message)
    }));
}

async function recordCleanupMetrics(stats) {
    const timestamp = new Date();
    const metricData = [
        {
            MetricName: 'StaleJobsFound',
            Value: stats.staleJobsFound,
            Unit: 'Count',
            Timestamp: timestamp
        },
        {
            MetricName: 'JobsCleaned',
            Value: stats.jobsCleaned,
            Unit: 'Count',
            Timestamp: timestamp
        },
        {
            MetricName: 'CleanupErrors',
            Value: stats.errors,
            Unit: 'Count',
            Timestamp: timestamp
        }
    ];

    await cloudWatch.send(new PutMetricDataCommand({
        Namespace: 'JobCleanup',
        MetricData: metricData
    }));
} 
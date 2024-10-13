const { DynamoDBClient, UpdateItemCommand, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const { createClient } = require('@supabase/supabase-js')

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

// Initialize Dynamodb client
const dynamoClient = new DynamoDBClient();

exports.handler = async (event) => {
    for (const record of event.Records) {
        if (record.eventName === 'INSERT') {
            const newImage = record.dynamodb.NewImage;
            const jobId = newImage.JobId.S;
            const evaluation = newImage.Evaluation.S;

            await updateJobProgress(jobId, evaluation);
        }
    }
};

async function updateJobProgress(jobId, evaluation) {
    const updateExpression = [
        'SET ProcessedImages = ProcessedImages + :inc',
        '#LastUpdateTime = :now'
    ];
    const expressionAttributeValues = {
        ':inc': { N: '1' },
        ':now': { N: Date.now().toString() }
    };
    const expressionAttributeNames = {
        '#LastUpdateTime': 'LastUpdateTime'
    };

    switch (evaluation) {
        case 'ELIGIBLE':
            updateExpression.push('EligibleImages = EligibleImages + :inc');
            break;
        case 'EXCLUDED':
            updateExpression.push('ExcludedImages = ExcludedImages + :inc');
            break;
        case 'DUPLICATE':
            updateExpression.push('DuplicateImages = DuplicateImages + :inc');
            break;
        case 'FAILED':
            updateExpression.push('FailedImages = FailedImages + :inc');
            break;
    }

    const updateItemCommand = new UpdateItemCommand({
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: { S: jobId } },
        UpdateExpression: 'SET ' + updateExpression.join(', '),
        ExpressionAttributeValues: expressionAttributeValues,
        ExpressionAttributeNames: expressionAttributeNames,
        ReturnValues: 'ALL_NEW'
    });

    const result = await dynamoClient.send(updateItemCommand);
    const updatedItem = result.Attributes;

    if (parseInt(updatedItem.ProcessedImages.N) >= parseInt(updatedItem.TotalImages.N)) {
        try {
            await updateJobStatus(jobId, 'COMPLETED');
            await updateJobStatusRDS(jobId, 'COMPLETED');
        } catch (error) {
            console.log(`*** Error: `, error)
        }
    }
}

async function updateJobStatus(jobId, status) {
    const updateItemCommand = new UpdateItemCommand({
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: { S: jobId } },
        UpdateExpression: 'SET #Status = :status',
        ExpressionAttributeValues: {
            ':status': { S: status }
        },
        ExpressionAttributeNames: {
            '#Status': 'Status'
        }
    });

    await dynamoClient.send(updateItemCommand);
}

// High-level job data stores in RDS
async function updateJobStatusRDS(jobId, status) {

    const { data, error } = await supabase
        .from('Job')
        .update({ jobStatus: status })
        .eq('id', jobId)
        .single();

    if (error) throw error;

    if (!data) {
        throw new Error(`No job found with id: ${projectSettingId}`);
    }

    return data;
}
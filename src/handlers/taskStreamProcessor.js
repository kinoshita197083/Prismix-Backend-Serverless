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
    console.log('----> Event received: ', JSON.stringify(event, null, 2));

    try {
        for (const record of event.Records) {
            if (record.eventName === 'INSERT') {
                const newImage = record.dynamodb.NewImage;
                const jobId = newImage.JobID.S;
                const evaluation = newImage.Evaluation.S;

                console.log(`Processing new task for Job ID: ${jobId}, Evaluation: ${evaluation}`);
                await updateJobProgress(jobId, evaluation);
            }
        }
        console.log('All records processed successfully');
    } catch (error) {
        console.error('Error in handler:', error);
        throw error; // Re-throw to ensure AWS Lambda marks the execution as failed
    }
};

async function updateJobProgress(jobId, evaluation) {
    console.log(`Updating job progress for Job ID: ${jobId}, Evaluation: ${evaluation}`);

    try {
        const updateExpression = ['SET #LastUpdateTime = :now'];
        const expressionAttributeValues = {
            ':now': { N: Date.now().toString() },
            ':inc': { N: '1' }
        };
        const expressionAttributeNames = {
            '#LastUpdateTime': 'LastUpdateTime'
        };

        // Always update ProcessedImages
        updateExpression.push('ProcessedImages = if_not_exists(ProcessedImages, :zero) + :inc');
        expressionAttributeValues[':zero'] = { N: '0' };

        // Update the specific evaluation type count
        if (['Duplicate', 'Eligible', 'Excluded', 'Failed'].includes(capitalizeFirstLetter(evaluation))) {
            const attributeName = `${capitalizeFirstLetter(evaluation)}Images`;
            updateExpression.push(`${attributeName} = if_not_exists(${attributeName}, :zero) + :inc`);
        } else {
            console.warn(`Unexpected evaluation type: ${capitalizeFirstLetter(evaluation)}`);
        }

        const updateItemCommand = new UpdateItemCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: { S: jobId } },
            UpdateExpression: updateExpression.join(', '),
            ExpressionAttributeValues: expressionAttributeValues,
            ExpressionAttributeNames: expressionAttributeNames,
            ReturnValues: 'ALL_NEW'
        });

        console.log('Sending update command:', JSON.stringify(updateItemCommand, null, 2));

        const updateResult = await dynamoClient.send(updateItemCommand);
        const updatedItem = updateResult.Attributes;

        console.log(`Updated item for Job ID ${jobId}:`, JSON.stringify(updatedItem, null, 2));

        if (updatedItem.ProcessedImages && updatedItem.TotalImages &&
            parseInt(updatedItem.ProcessedImages.N) >= parseInt(updatedItem.TotalImages.N)) {
            console.log(`Job ${jobId} has processed all images. Marking as COMPLETED.`);
            await updateJobStatus(jobId, 'COMPLETED');
            await updateJobStatusRDS(jobId, 'COMPLETED');
        }
    } catch (error) {
        console.error(`Error updating job progress for Job ID ${jobId}:`, error);
        throw error;
    }
}

async function updateJobStatus(jobId, status) {
    console.log(`Updating job status to ${status} for Job ID: ${jobId}`);
    try {
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
        console.log(`Successfully updated job status to ${status} for Job ID: ${jobId}`);
    } catch (error) {
        console.error(`Error updating job status for Job ID ${jobId}:`, error);
        throw error;
    }
}

async function updateJobStatusRDS(jobId, status) {
    console.log(`Updating job status in RDS to ${status} for Job ID: ${jobId}`);
    try {
        const { data, error } = await supabase
            .from('Job')
            .update({ jobStatus: status })
            .eq('id', jobId)
            .single();

        if (error) throw error;

        console.log(`Successfully updated job status in RDS to ${status} for Job ID: ${jobId}`);
        return data;
    } catch (error) {
        console.error(`Error updating job status in RDS for Job ID ${jobId}:`, error);
        throw error;
    }
}

function capitalizeFirstLetter(input) {
    if (typeof input !== 'string' || input === null) {
        return '';
    }

    input = input.trim();

    if (input.length === 0) {
        return '';
    }

    return input.charAt(0).toUpperCase() + input.slice(1).toLowerCase();
}
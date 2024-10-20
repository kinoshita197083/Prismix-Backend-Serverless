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
            if (record.eventName === 'INSERT' || record.eventName === 'MODIFY') {
                const newImage = record.dynamodb.NewImage;
                const jobId = newImage.JobID.S;
                console.log('----> Job ID: ', jobId);

                // Check if Evaluation exists before accessing it
                const evaluation = newImage.Evaluation ? newImage.Evaluation.S : null;
                console.log('----> Evaluation: ', evaluation);

                if (evaluation) {
                    console.log(`Processing task for Job ID: ${jobId}, Evaluation: ${evaluation}`);
                    await updateJobProgress(jobId, evaluation);
                } else {
                    console.log(`Skipping task for Job ID: ${jobId} - No evaluation yet`);
                }
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
        const setExpressions = [
            '#LastUpdateTime = :now',
            '#Status = if_not_exists(#Status, :processing)'
        ];
        const addExpressions = [
            '#ProcessedImages :inc'
        ];
        const expressionAttributeValues = {
            ':now': { N: Date.now().toString() },
            ':inc': { N: '1' },
            ':processing': { S: 'PROCESSING' },
        };
        const expressionAttributeNames = {
            '#LastUpdateTime': 'LastUpdateTime',
            '#ProcessedImages': 'ProcessedImages',
            '#TotalImages': 'TotalImages',
            '#Status': 'Status'
        };

        // Update the specific evaluation type count
        const capitalizedEvaluation = capitalizeFirstLetter(evaluation);
        if (['Duplicate', 'Eligible', 'Excluded', 'Failed'].includes(capitalizedEvaluation)) {
            const attributeName = `${capitalizedEvaluation}Images`;
            addExpressions.push(`#${attributeName} :inc`);
            expressionAttributeNames[`#${attributeName}`] = attributeName;
        } else {
            console.warn(`Unexpected evaluation type: ${capitalizedEvaluation}`);
        }

        const updateItemCommand = new UpdateItemCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: { S: jobId } },
            UpdateExpression: `SET ${setExpressions.join(', ')} ADD ${addExpressions.join(', ')}`,
            ConditionExpression: 'attribute_exists(#TotalImages)',
            ExpressionAttributeValues: expressionAttributeValues,
            ExpressionAttributeNames: expressionAttributeNames,
            ReturnValues: 'ALL_NEW'
        });

        console.log('Sending update command:', JSON.stringify(updateItemCommand, null, 2));

        const updateResult = await dynamoClient.send(updateItemCommand);
        const updatedItem = updateResult.Attributes;

        console.log(`Updated item for Job ID ${jobId}:`, JSON.stringify(updatedItem, null, 2));

        // Check if job is completed
        const processedImages = parseInt(updatedItem.ProcessedImages.N, 10);
        const totalImages = parseInt(updatedItem.TotalImages.N, 10);
        const processingEnded = updatedItem.ProcessingEnded ? updatedItem.ProcessingEnded.BOOL : false;

        if (!processingEnded && processedImages >= totalImages) {
            console.log(`Job ${jobId} has processed all images. Marked as COMPLETED.`);
            await updateJobStatus(jobId, 'COMPLETED');
            await updateJobStatusRDS(jobId, 'COMPLETED');
        }
    } catch (error) {
        if (error.name === 'ConditionalCheckFailedException') {
            console.log(`Conditional update failed for Job ID ${jobId}. This is likely due to concurrent updates or the job already being completed.`);
        } else {
            console.error(`Error updating job progress for Job ID ${jobId}:`, error);
            throw error;
        }
    }
}

async function updateJobStatus(jobId, status) {
    console.log(`Updating job status to ${status} for Job ID: ${jobId}`);
    try {
        const updateExpression = ['#Status = :status', '#processingEnded = :isEnded'];
        const expressionAttributeValues = {
            ':status': { S: status },
            ':isEnded': { BOOL: true }
        };
        const expressionAttributeNames = {
            '#Status': 'Status',
            '#processingEnded': 'ProcessingEnded'
        };
        const updateItemCommand = new UpdateItemCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: { S: jobId } },
            UpdateExpression: 'SET ' + updateExpression.join(', '),
            ExpressionAttributeValues: expressionAttributeValues,
            ExpressionAttributeNames: expressionAttributeNames
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

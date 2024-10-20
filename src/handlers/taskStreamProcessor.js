const { DynamoDBClient, UpdateItemCommand } = require('@aws-sdk/client-dynamodb');
const { createClient } = require('@supabase/supabase-js');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
);

// Initialize DynamoDB client
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

exports.handler = async (event) => {
    console.log('----> Event received: ', JSON.stringify(event, null, 2));

    try {
        const updatePromises = event.Records.map(record => {
            if (record.eventName === 'INSERT' || record.eventName === 'MODIFY') {
                const newImage = record.dynamodb.NewImage;
                const jobId = newImage.JobID.S;
                const evaluation = newImage.Evaluation ? newImage.Evaluation.S : null;

                if (evaluation) {
                    return updateJobProgress(jobId, evaluation);
                }
            }
            return Promise.resolve();
        });

        await Promise.all(updatePromises);
        console.log('All records processed successfully');
    } catch (error) {
        console.error('Error in handler:', error);
    }
};

async function updateJobProgress(jobId, evaluation) {
    console.log(`Updating job progress for Job ID: ${jobId}, Evaluation: ${evaluation}`);

    try {
        const updateExpression = ['SET #LastUpdateTime = :now'];
        const expressionAttributeValues = {
            ':now': Date.now(),
            ':inc': 1,
            ':completed': 'COMPLETED',
            ':inProgress': 'IN_PROGRESS',
            ':zero': 0
        };
        const expressionAttributeNames = {
            '#LastUpdateTime': 'LastUpdateTime',
            '#Status': 'Status'
        };

        // Increment ProcessedImages
        updateExpression.push('ProcessedImages = if_not_exists(ProcessedImages, :zero) + :inc');

        // Update evaluation-specific image count
        if (['Duplicate', 'Eligible', 'Excluded', 'Failed'].includes(capitalizeFirstLetter(evaluation))) {
            const attributeName = `${capitalizeFirstLetter(evaluation)}Images`;
            updateExpression.push(`${attributeName} = if_not_exists(${attributeName}, :zero) + :inc`);
            expressionAttributeNames[`#${attributeName}`] = attributeName;
        } else {
            console.warn(`Unexpected evaluation type: ${capitalizeFirstLetter(evaluation)}`);
        }

        // Conditionally update status based on ProcessedImages and TotalImages
        updateExpression.push(
            '#Status = if_not_exists(#Status, if(ProcessedImages + :inc >= TotalImages, :completed, :inProgress))'
        );

        const updateCommand = new UpdateCommand({
            TableName: process.env.JOB_PROGRESS_TABLE,
            Key: { JobId: jobId },
            UpdateExpression: updateExpression.join(', '),
            ExpressionAttributeValues: expressionAttributeValues,
            ExpressionAttributeNames: expressionAttributeNames,
            ReturnValues: 'ALL_NEW'
        });

        const updateResult = await docClient.send(updateCommand);
        const updatedItem = updateResult.Attributes;

        console.log(`Updated item for Job ID ${jobId}:`, JSON.stringify(updatedItem, null, 2));

        // Check if the job was just marked as completed
        if (updatedItem.Status === 'COMPLETED') {
            console.log(`Job ${jobId} has processed all images and was just marked as COMPLETED.`);
            await updateJobStatusRDS(jobId, 'COMPLETED');
        }
    } catch (error) {
        console.error(`Error updating job progress for Job ID ${jobId}:`, error);
        // Continue on error without throwing to avoid lambda failures
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
    if (typeof input !== 'string') {
        return '';
    }
    input = input.trim();
    return input.length === 0 ? '' : input.charAt(0).toUpperCase() + input.slice(1).toLowerCase();
}

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const logger = require('../utils/logger');
const { google } = require('googleapis');
const { createClient } = require('@supabase/supabase-js');
const { fetchGoogleRefreshToken, setUpGoogleDriveClient } = require('../utils/googleDrive/googleDrive');
const { ELIGIBLE } = require('../utils/config');

const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

exports.handler = async (event) => {
    logger.info('Delivery processor started', { event });

    for (const record of event.Records) {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        console.log('message', message);
        const { jobId } = message;
        console.log('extracted jobId and provider', { jobId });

        const provider = await fetchProvider(jobId);
        console.log('provider', provider);

        if (provider !== 'google-drive') return;

        // TODO: JSON.parse() the message body to get the provider

        logger.info(`Processing delivery for job ${jobId}`);

        try {
            // Fetch eligible tasks
            const eligibleTasks = await fetchEligibleTasks(jobId);

            // If no eligible tasks, skip the job
            if (eligibleTasks.length === 0) {
                logger.warn(`No eligible tasks found for job ${jobId}`);
                continue;
            }

            // Fetch user ID from job progress table
            const { userId, processedFolderIds, processedDriveIds } = await fetchJobProgressData(jobId);

            // User ID is required for Google Drive operations
            if (!userId) {
                logger.error(`User ID not found for job ${jobId}`);
                continue;
            }

            logger.info(`Found ${eligibleTasks.length} eligible tasks for job ${jobId}`);

            // Fetch Google refresh token from Supabase
            const googleRefreshToken = await fetchGoogleRefreshToken(userId, supabase);

            // Pass in the refresh token to the Google Drive client
            const drive = setUpGoogleDriveClient(googleRefreshToken);


            // Create folder in Google Drive
            const folderId = await createGoogleDriveFolder(jobId, processedDriveIds);

            let processedImages = 0;
            const failedImages = [];

            for (const task of eligibleTasks) {
                try {
                    const imageData = await getImageFromS3(task.ImageS3Key);
                    const fileName = task.ImageS3Key.split('/').pop();
                    await uploadToGoogleDrive(imageData, fileName, folderId);
                    processedImages++;
                    logger.debug(`Uploaded image to Google Drive: ${fileName}`);
                } catch (error) {
                    failedImages.push({
                        s3ObjectKey: task.ImageS3Key,
                        error: error.message
                    });
                    logger.error(`Failed to process image for task`, { taskId: task.TaskID, imageKey: task.ImageS3Key, error: error.message });
                }
            }

            logger.info(`Processed ${processedImages} images, ${failedImages.length} failed`);

            if (processedImages === 0) {
                throw new Error(`No images were successfully processed for job ${jobId}`);
            }

            logger.info(`Delivery processed for job ${jobId}. Images uploaded to Google Drive folder.`);
        } catch (error) {
            logger.error(`Error processing delivery for job ${jobId}:`, { error: error.message, stack: error.stack });
        }
    }

    logger.info('Delivery processor finished');
};

async function fetchProvider(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'inputConnection'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);
        return result.Item.inputConnection;
    } catch (error) {
        logger.error(`Error fetching provider for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function fetchEligibleTasks(jobId) {
    logger.info(`Fetching eligible tasks for job ${jobId}`);
    const params = {
        TableName: process.env.TASKS_TABLE,
        KeyConditionExpression: 'JobID = :jobId',
        FilterExpression: 'Evaluation = :evaluation',
        ExpressionAttributeValues: {
            ':jobId': jobId,
            ':evaluation': ELIGIBLE
        }
    };

    try {
        const command = new QueryCommand(params);
        const result = await docClient.send(command);
        logger.info(`Fetched ${result.Items.length} eligible tasks for job ${jobId}`);
        return result.Items;
    } catch (error) {
        logger.error(`Error fetching eligible tasks for job ${jobId}:`, { error: error.message, params });
        throw error;
    }
}

async function getImageFromS3(key) {
    logger.debug(`Fetching image from S3: ${key}`);
    const getObjectParams = {
        Bucket: process.env.BUCKET_NAME,
        Key: key
    };

    try {
        const response = await s3Service.getFile(getObjectParams);
        const buffer = await streamToBuffer(response);
        return buffer;
    } catch (error) {
        logger.error(`getImageFromS3(): ${key}`, { error: error.message });
        throw error;
    }
}

function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', (error) => {
            logger.error('Error in stream to buffer conversion', { error: error.message });
            reject(error);
        });
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function fetchUserIdFromJobProgress(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'UserId'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);
        return result.Item ? result.Item.UserId : null;
    } catch (error) {
        logger.error('Error fetching userId from JobProgress:', { error, jobId });
        throw error;
    }
}

async function createGoogleDriveFolder(jobId, processedDriveIds) {
    const folderMetadata = {
        name: `Prismix-Job_${jobId}`,
        mimeType: 'application/vnd.google-apps.folder',
        parents: processedDriveIds
    };

    try {
        const response = await drive.files.create({
            resource: folderMetadata,
            fields: 'id'
        });
        logger.info(`Created Google Drive folder for job ${jobId}`);
        return response.data.id;
    } catch (error) {
        logger.error(`Error creating Google Drive folder for job ${jobId}:`, { error: error.message });
        throw error;
    }
}

async function uploadToGoogleDrive(buffer, fileName, folderId) {
    const fileMetadata = {
        name: fileName,
        parents: [folderId]
    };

    const media = {
        mimeType: 'image/jpeg', // Adjust based on your image types
        body: bufferToStream(buffer)
    };

    try {
        await drive.files.create({
            resource: fileMetadata,
            media: media,
            fields: 'id'
        });
        logger.debug(`Uploaded ${fileName} to Google Drive`);
    } catch (error) {
        logger.error(`Error uploading ${fileName} to Google Drive:`, { error: error.message });
        throw error;
    }
}

function bufferToStream(buffer) {
    const stream = new require('stream').Readable();
    stream.push(buffer);
    stream.push(null);
    return stream;
}

function getUserRootFolderId(userId) {
    // Implement logic to get or create a root folder for the user
    // This might involve storing folder IDs in your database
}

async function fetchJobProgressData(jobId) {
    const params = {
        TableName: process.env.JOB_PROGRESS_TABLE,
        Key: { JobId: jobId },
        ProjectionExpression: 'UserId, ProcessedFolderIds, ProcessedDriveIds'
    };

    try {
        const command = new GetCommand(params);
        const result = await docClient.send(command);

        if (!result.Item) {
            logger.warn(`No job progress data found for jobId: ${jobId}`);
            return null;
        }

        return {
            userId: result.Item.userId,
            processedFolderIds: result.Item.processedFolderIds || [],
            processedDriveIds: result.Item.processedDriveIds || []
        };
    } catch (error) {
        logger.error('Error fetching job progress data:', { error, jobId });
        throw error;
    }
}

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { createClient } = require('@supabase/supabase-js')
const { google } = require('googleapis');
const logger = require('../utils/logger');

const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second delay between retries

const s3Client = new S3Client();

// Initialize Supabase client
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_API_KEY
)

const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
);

exports.handler = async (event) => {
    logger.info('Image uploader started', { event });

    await Promise.all(
        event.Records.map(async (record) => {
            const { body } = record;
            let parsedBody = {};

            try {
                parsedBody = JSON.parse(body);
            } catch (error) {
                logger.error('Failed to parse message body', { error, body });
                return; // Continue with other records
            }

            const { userId, projectId, jobId, projectSettingId, driveIds } = parsedBody;

            try {
                const googleRefreshToken = await fetchGoogleRefreshToken(userId);
                oauth2Client.setCredentials({
                    refresh_token: googleRefreshToken?.googleDriveRefreshToken
                });

                const drive = google.drive({ version: 'v3', auth: oauth2Client });

                const images = await getAllImagesFromDrive(drive, driveIds);
                const imageCount = images.length;

                if (imageCount === 0) {
                    return logger.info('No images found in the specified drive');
                }

                // Process images in batches of 100
                const batchSize = 10;
                const results = [];

                for (let i = 0; i < images.length; i += batchSize) {
                    const batch = images.slice(i, i + batchSize);
                    try {
                        const batchResults = await processImageBatch(
                            batch,
                            drive,
                            s3Client,
                            userId,
                            projectId,
                            projectSettingId,
                            jobId
                        );
                        results.push(...batchResults);
                    } catch (batchError) {
                        logger.error('Error processing batch of images', { batchError, batch });
                    }
                }

                const successCount = results.filter(r => r.success).length;
                const failedUploads = results.filter(r => !r.success);
                const skippedUploads = results.filter(r => r.skipReason);

                // Update DynamoDB if there are failed or skipped uploads
                if (failedUploads.length > 0 || skippedUploads.length > 0) {
                    const dynamoDb = new DynamoDBClient({ region: process.env.AWS_REGION });
                    const dynamoDbDocumentClient = DynamoDBDocumentClient.from(dynamoDb);

                    const updateItemCommand = new UpdateCommand({
                        TableName: process.env.JOB_PROGRESS_TABLE,
                        Key: { JobId: jobId },
                        UpdateExpression: 'SET failedImages = :fi, skippedImages = :si, updatedAt = :lut',
                        ExpressionAttributeValues: {
                            ':fi': failedUploads.length.toString(),
                            ':si': skippedUploads.length.toString(),
                            ':lut': Date.now() // Use a number type for timestamp
                        }
                    });

                    await dynamoDbDocumentClient.send(updateItemCommand);

                    console.log('Failed/Skipped uploads:', [...failedUploads, ...skippedUploads].map(f => ({
                        fileName: f.fileName,
                        error: f.error,
                        skipReason: f.skipReason,
                        attempts: f.attemptCount
                    })));
                }

                logger.info('Image uploader finished', {
                    message: `Successfully imported ${successCount} images`,
                    totalProcessed: images.length,
                    successCount,
                    failedCount: failedUploads.length,
                    skippedCount: skippedUploads.length
                });
            } catch (error) {
                logger.error('Error processing image upload', { error, parsedBody });
            }
        })
    );
};


const fetchGoogleRefreshToken = async (userId) => {
    const { data, error } = await supabase
        .from('User')
        .select('googleRefreshToken')
        .eq('id', userId)
        .single();

    if (error) throw error;

    return data.googleRefreshToken;
}

async function getAllImagesFromDrive(drive, driveIds) {
    const images = [];

    async function fetchImagesRecursively(folderId) {
        let pageToken = null;
        do {
            const response = await drive.files.list({
                q: `'${folderId}' in parents and (mimeType contains 'image/' or mimeType = 'application/vnd.google-apps.folder')`,
                fields: 'nextPageToken, files(id, name, mimeType, capabilities(canDownload), permissionIds)',
                pageToken: pageToken || undefined,
            });

            if (response?.data?.files) {
                // Process files concurrently using Promise.all for better performance
                await Promise.all(response.data.files.map(async (file) => {
                    if (file.mimeType === 'application/vnd.google-apps.folder') {
                        try {
                            // Recursively fetch images from subfolders
                            await fetchImagesRecursively(file.id);
                        } catch (error) {
                            console.error(`Error fetching images from folder ${file.name}:`, error);
                        }
                    } else if (file.capabilities?.canDownload !== false) {
                        images.push(file);
                    }
                }));
            }

            pageToken = response.data.nextPageToken || null;
        } while (pageToken);
    }

    try {
        // Process driveIds concurrently using Promise.all
        await Promise.all(driveIds.map((driveId) => fetchImagesRecursively(driveId)));

        return images;
    } catch (error) {
        console.error('Error fetching images from Drive:', error);
        throw new Error('Failed to fetch images from Google Drive');
    }
}



const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function uploadImageWithRetry(
    image,
    drive,
    s3Client,
    s3Key,
    attempt
) {
    // First check if we still have access to the file
    const hasAccess = await checkFileAccess(drive, image.id);
    if (!hasAccess) {
        return {
            success: false,
            fileName: image.name,
            skipReason: 'No download access to file',
            attemptCount: attempt
        };
    }

    try {
        const imageContent = await drive.files.get({
            fileId: image.id,
            alt: 'media',
            supportsAllDrives: true,
        }, { responseType: 'arraybuffer' });

        const command = new PutObjectCommand({
            Bucket: process.env.AWS_S3_BUCKET_NAME,
            Key: s3Key,
            Body: imageContent.data,
            ContentType: image.mimeType,
            Metadata: {
                'google-drive-id': image.id,
                'upload-date': new Date().toISOString(),
            }
        });

        await s3Client.send(command);
        console.log(`Successfully uploaded ${image.name} to S3 (attempt ${attempt})`);

        return {
            success: true,
            fileName: image.name,
            attemptCount: attempt
        };
    } catch (error) {
        // Check if error is permission-related
        if (error instanceof Error &&
            (error.message.includes('insufficient permissions') ||
                error.message.includes('access denied'))) {
            return {
                success: false,
                fileName: image.name,
                skipReason: 'Permission denied during download',
                attemptCount: attempt
            };
        }

        if (attempt < MAX_RETRIES) {
            console.log(`Retry attempt ${attempt + 1} for ${image.name}`);
            await sleep(RETRY_DELAY * attempt); // Exponential backoff
            return uploadImageWithRetry(image, drive, s3Client, s3Key, attempt + 1);
        }

        return {
            success: false,
            fileName: image.name,
            error: error instanceof Error ? error.message : 'Unknown error',
            attemptCount: attempt
        };
    }
}

async function processImageBatch(
    batch,
    drive,
    s3Client,
    userId,
    projectId,
    projectSettingId,
    jobId
) {
    const uploadPromises = batch.map(async (image) => {
        try {
            const suffix = image.name.split('.').pop() || '';
            const imageName = image.name.split('.').slice(0, -1).join('.');
            const s3Key = `uploads/${userId}/${projectId}/${projectSettingId}/${jobId}/${imageName}.${suffix}`;

            console.log(`Starting upload for ${image.name} to S3 at key: ${s3Key}`);

            return await uploadImageWithRetry(image, drive, s3Client, s3Key);
        } catch (error) {
            console.error(`Unexpected error processing ${image.name}:`, error);
            return {
                success: false,
                fileName: image.name,
                error: error instanceof Error ? error.message : 'Unknown error',
                attemptCount: 1
            };
        }
    });

    return Promise.all(uploadPromises);
}

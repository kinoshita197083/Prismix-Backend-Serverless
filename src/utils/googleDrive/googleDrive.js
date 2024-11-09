const { sleep } = require('../helpers');
const { MAX_RETRIES, RETRY_DELAY } = require('./config');
const logger = require('../logger');
const { google } = require('googleapis');

const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
);

const setUpGoogleDriveClient = (refreshToken) => {
    console.log('----> Setting up Google Drive client with refresh token');
    console.log('----> Has refresh token:', !!refreshToken);
    console.log('----> Has env: ', !!process.env.GOOGLE_CLIENT_ID, !!process.env.GOOGLE_CLIENT_SECRET);
    oauth2Client.setCredentials({ refresh_token: refreshToken });
    return google.drive({ version: 'v3', auth: oauth2Client });
}

const fetchGoogleRefreshToken = async (userId, supabaseClient) => {

    try {
        const { data, error } = await supabaseClient
            .from('User')
            .select('googleDriveRefreshToken')
            .eq('id', userId)
            .single();

        if (error) throw error;

        return data.googleDriveRefreshToken;
    } catch (error) {
        logger.error('Error fetching Google refresh token', { error, userId });
        throw error;
    }
}

async function getAllImagesFromDrive({ drive, driveIds, folderIds }) {
    const images = [];

    if (!folderIds || folderIds.length === 0) {
        console.log('No folders specified. Skipping image search.');
        return images;
    }

    async function fetchImagesRecursively({ driveId, folderId }) {
        if (!driveId || !folderId) {
            console.warn('Skipping null or undefined drive ID or folder ID');
            return;
        }

        let pageToken = null;
        do {
            try {
                const response = await drive.files.list({
                    q: `'${folderId}' in parents and (mimeType contains 'image/' or mimeType = 'application/vnd.google-apps.folder')`,
                    fields: 'nextPageToken, files(id, name, mimeType, capabilities(canDownload), permissionIds)',
                    pageToken: pageToken || undefined,
                    supportsAllDrives: true,
                    includeItemsFromAllDrives: true,
                });

                if (response?.data?.files) {
                    // Process files concurrently using Promise.all for better performance
                    await Promise.all(response.data.files.map(async (file) => {
                        if (file.mimeType === 'application/vnd.google-apps.folder') {
                            try {
                                // Recursively fetch images from subfolders
                                await fetchImagesRecursively({ driveId, folderId: file.id });
                            } catch (error) {
                                console.error(`Error fetching images from folder ${file.name}:`, error);
                            }
                        } else if (file.capabilities?.canDownload !== false) {
                            images.push(file);
                        }
                    }));
                }

                pageToken = response.data.nextPageToken || null;
            } catch (error) {
                console.error(`Error fetching files for drive ${driveId}, folder ${folderId}:`, error);
                pageToken = null; // Stop pagination on error
            }
        } while (pageToken);
    }

    try {
        // Filter out null or undefined driveIds and folderIds
        const validDriveIds = driveIds.filter(id => id);
        const validFolderIds = folderIds.filter(id => id);

        if (validDriveIds.length === 0) {
            console.warn('No valid drive IDs provided');
            return images;
        }

        if (validFolderIds.length === 0) {
            console.warn('No valid folder IDs provided');
            return images;
        }

        // Process driveIds and folderIds concurrently using Promise.all
        await Promise.all(validDriveIds.map(async (driveId) => {
            await Promise.all(validFolderIds.map(folderId => fetchImagesRecursively({ driveId, folderId })));
        }));

        console.log(`Total images found: ${images.length}`);
        return images;
    } catch (error) {
        console.error('Error fetching images from Drive:', error);
        throw new Error('Failed to fetch images from Google Drive');
    }
}

async function uploadImageWithRetry(
    image,
    drive,
    s3Client,
    s3Key,
    attempt,
    bucket
) {
    const { PutObjectCommand } = require('@aws-sdk/client-s3');

    try {
        const imageContent = await drive.files.get({
            fileId: image.id,
            alt: 'media',
            supportsAllDrives: true,
        }, { responseType: 'arraybuffer' });

        if (!imageContent.data) {
            return {
                skipped: true,
                fileName: image.name,
                reason: 'No image content found',
                attemptCount: attempt
            };
        }

        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: s3Key,
            Body: imageContent.data,
            ContentType: image.mimeType,
            Metadata: {
                'google-drive-id': image.id,
                'upload-date': Date.now().toString(),
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
        if (attempt < MAX_RETRIES) {
            console.log(`Retry attempt ${attempt + 1} for ${image.name}`);
            await sleep(RETRY_DELAY * attempt); // Exponential backoff
            return uploadImageWithRetry(image, drive, s3Client, s3Key, attempt + 1, bucket);
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
    jobId,
    bucket
) {
    const uploadPromises = batch.map(async (image, index) => {
        const crypto = require('crypto');
        try {
            const suffix = image.name.split('.').pop() || '';
            const imageId = crypto.createHash('md5')
                .update(`${image.id}_${index}`)  // Using Drive's file ID and position in batch
                .digest('hex');
            const s3Key = `uploads/${userId}/${projectId}/${projectSettingId}/${jobId}/${imageId}.${suffix}`;

            console.log(`*** Starting upload for ${image.name} with id: ${imageId} to S3 at key: ${s3Key}`);

            return await uploadImageWithRetry(image, drive, s3Client, s3Key, 1, bucket);
        } catch (error) {
            console.error(`Unexpected error processing ${image.name}:`, error);
            return {
                success: false,
                s3Key,
                imageId: image.id,
                fileName: image.name,
                error: error instanceof Error ? error.message : 'Unknown error',
                reason: 'Process image batch failed',
                attemptCount: 1
            };
        }
    });

    return Promise.all(uploadPromises);
}

module.exports = {
    fetchGoogleRefreshToken,
    getAllImagesFromDrive,
    uploadImageWithRetry,
    processImageBatch,
    setUpGoogleDriveClient
};

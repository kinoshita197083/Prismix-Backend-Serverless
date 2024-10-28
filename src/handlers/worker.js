const logger = require('../utils/logger');
const dynamoService = require('../services/dynamoService');
const { COMPLETED } = require('../utils/config');
const { processImageProperties } = require('../services/imageProcessingService');
const { validateImageQuality } = require('../services/qualityService');
const { fetchProjectSettingRules } = require('../utils/api/api');
const { formatLabels } = require('../utils/helpers');
const { duplicateImageDetection } = require('../utils/worker/imageProcessing');


function evaluate(labels, projectSettings) {
    console.log('Evaluating labels...');
    // Placeholder evaluation logic
    return 'ELIGIBLE';
}

exports.handler = async (event, context) => {
    console.log('----> Event:', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    await Promise.all(Records.map(async (record) => {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        const { bucket, key: s3ObjectKey, jobId } = message;
        const imageId = s3ObjectKey.split('/').pop();

        try {
            const projectSettings = await fetchProjectSettingRules(jobId);
            console.log('projectSettings', projectSettings);

            // Step 1: Check for duplicates if enabled
            if (projectSettings.detectDuplicates) {
                const isDuplicate = await duplicateImageDetection({
                    bucket,
                    s3ObjectKey,
                    jobId,
                    imageId
                });
                if (isDuplicate) return;
            }

            // Step 2: Process image properties (resize, compress, format)
            const processedImageKey = await processImageProperties({
                bucket,
                s3ObjectKey,
                settings: {
                    maxWidth: projectSettings.maxWidth,
                    maxHeight: projectSettings.maxHeight,
                    resizeMode: projectSettings.resizeMode,
                    quality: projectSettings.compressionQuality,
                    outputFormat: projectSettings.outputFormat
                }
            });

            // Step 3: Validate image quality if enabled
            if (projectSettings.blurryImages || projectSettings.lowResolution) {
                const qualityResult = await validateImageQuality({
                    bucket,
                    key: processedImageKey,
                    settings: {
                        checkBlur: projectSettings.blurryImages,
                        checkResolution: projectSettings.lowResolution,
                        minResolution: projectSettings.minimumResolution
                    }
                });

                if (!qualityResult.isValid) {
                    await updateTaskStatusWithQualityIssues({
                        jobId,
                        imageId,
                        s3ObjectKey: processedImageKey,
                        qualityIssues: qualityResult.issues
                    });
                    return;
                }
            }

            // Step 4: Perform content detection if any detection settings are enabled
            let labels = [];
            if (projectSettings.contentTags.length > 0 || projectSettings.textTags.length > 0) {
                labels = await labelDetection({
                    bucket,
                    s3ObjectKey: processedImageKey,
                    jobId,
                    imageId,
                    settingValue: {
                        ...projectSettings,
                        minConfidence: parseFloat(projectSettings.detectionConfidence) * 100
                    }
                });
            }
            const formattedLabels = formatLabels(labels);

            // Step 5: Evaluate results and update status
            const evaluation = evaluate(labels, projectSettings);

            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: processedImageKey,
                status: COMPLETED,
                labels,
                evaluation,
                processingDetails: {
                    wasResized: processedImageKey !== s3ObjectKey,
                    qualityChecked: projectSettings.blurryImages || projectSettings.lowResolution,
                    detectionPerformed: labels.length > 0,
                    formattedLabels
                }
            });

        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
            });

            try {
                await dynamoService.updateTaskStatusAsFailed({
                    jobId,
                    taskId: imageId,
                    imageS3Key: s3ObjectKey,
                    reason: error.message
                });
            } catch (retryUpdateError) {
                logger.error('Error updating task status to FAILED in TASK_TABLE', { error: retryUpdateError.message, jobId, taskId: imageId });
            }
        }
    }));
};

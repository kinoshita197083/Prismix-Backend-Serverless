const logger = require('../utils/logger');
const { COMPLETED, WAITING_FOR_REVIEW, EXCLUDED } = require('../utils/config');
const { processImageProperties } = require('../services/imageProcessingService');
const { validateImageQuality } = require('../services/qualityService');
const { formatLabels, createHash, formatTexts } = require('../utils/helpers');
const { duplicateImageDetection, labelDetection } = require('../utils/worker/imageProcessing');
const dynamoService = require('../services/dynamoService');
const { evaluationMapper, evaluate } = require('../utils/worker/evaluation');

exports.handler = async (event, context) => {
    console.log('----> Event:', event);
    const { Records } = event;
    logger.info('Processing image batch', { recordCount: Records.length, awsRequestId: context.awsRequestId });

    await Promise.all(Records.map(async (record) => {
        const body = JSON.parse(record.body);
        const message = JSON.parse(body.Message);
        // const message = record.body.Message; // FOR TESTING
        const { bucket, key: s3ObjectKey, jobId } = message;
        const imageId = createHash(s3ObjectKey.split('/').pop());

        console.log('999 Processing image:', { bucket, s3ObjectKey, jobId, imageId });

        try {
            const projectSettings = await fetchProjectSettingRules(jobId);
            console.log('projectSettings', projectSettings);

            const manualReviewRequired = projectSettings.manualReviewRequired;
            console.log('manualReviewRequired', manualReviewRequired);

            const preserveFileDays = projectSettings.preserveFileDays;
            console.log('preserveFileDays', preserveFileDays);

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

            console.log('999 Deduplication passed. Processing image properties...');

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

            console.log('999 Image properties processed. Validating image quality...');

            // Step 3: Validate image quality if enabled
            if (projectSettings.blurryImages || projectSettings.removeLowResolution) {
                const qualityResult = await validateImageQuality({
                    bucket,
                    key: processedImageKey,
                    settings: {
                        checkBlur: projectSettings.blurryImages,
                        checkResolution: projectSettings.removeLowResolution,
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

            console.log('999 Image quality validated. Performing content detection...');

            // Step 4: Perform content detection if any detection settings are enabled
            let labels = [];
            if (projectSettings?.contentTags?.length > 0) {
                labels = await labelDetection({
                    bucket,
                    s3ObjectKey: processedImageKey,
                });
            }
            const formattedLabels = formatLabels(labels);

            console.log('Content detection performed successfully. Performing text detection...');

            let detectedTexts = [];
            if (projectSettings?.textTags?.length > 0) {
                detectedTexts = await detectText({
                    bucket,
                    s3ObjectKey: processedImageKey,
                });
            }
            const formattedTexts = formatTexts(detectedTexts);

            console.log('999 Text detection performed successfully. Evaluating results...');

            // Step 5: Evaluate results and update status
            const evaluation = await evaluate(formattedLabels, formattedTexts, projectSettings);
            let finalEvaluation = evaluationMapper[evaluation];
            let status = COMPLETED;
            let reason = finalEvaluation === EXCLUDED ? 'Labels excluded by project settings' : undefined;

            // If manual review is required and the evaluation would be EXCLUDED,
            // change status to WAITING_FOR_REVIEW instead
            if (manualReviewRequired && finalEvaluation === EXCLUDED) {
                status = WAITING_FOR_REVIEW;
                console.log('----> manualReviewRequired && finalEvaluation === EXCLUDED', status);
            }

            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: processedImageKey,
                status,
                labels,
                evaluation: finalEvaluation,
                reason,
                processingDetails: {
                    wasResized: processedImageKey !== s3ObjectKey,
                    qualityChecked: projectSettings.blurryImages || projectSettings.removeLowResolution,
                    detectionPerformed: labels.length > 0 || detectedTexts.length > 0,
                    formattedLabels,
                    formattedTexts,
                    needsReview: status === WAITING_FOR_REVIEW
                },
                expirationTime: (Date.now() + preserveFileDays * 24 * 60 * 60 * 1000).toString(), // User defined number of days to preserve the file
            });

        } catch (error) {
            logger.error('Error processing image', {
                error: error.message,
                stack: error.stack,
                bucket,
                key: s3ObjectKey,
            });

            try {
                console.log('999 Updating task status to FAILED: ', { jobId, taskId: imageId, imageS3Key: s3ObjectKey, reason: error.message });
                await dynamoService.updateTaskStatusAsFailed({
                    jobId,
                    taskId: imageId,
                    imageS3Key: s3ObjectKey,
                    reason: error.message,
                    preserveFileDays: projectSettings.preserveFileDays
                });
            } catch (retryUpdateError) {
                logger.error('Error updating task status to FAILED in TASK_TABLE', { error: retryUpdateError.message, jobId, taskId: imageId });
            }
        }
    }));
};

async function fetchProjectSettingRules(jobId) {
    console.log('Fetching project setting rules...', { jobId });
    try {
        const response = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        console.log('Fetch project setting rules response:', response);
        const projectSettingsWithManualReview = {
            ...response?.projectSetting,
            manualReviewRequired: response?.manualReviewRequired,
            preserveFileDays: response?.preserveFileDays
        };
        return projectSettingsWithManualReview;
    } catch (error) {
        // logger.error('Error fetching project setting rules', { error, jobId });
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}
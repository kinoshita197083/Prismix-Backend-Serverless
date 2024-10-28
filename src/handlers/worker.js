const logger = require('../utils/logger');
const { COMPLETED } = require('../utils/config');
const { processImageProperties } = require('../services/imageProcessingService');
const { validateImageQuality } = require('../services/qualityService');
const { formatLabels, createHash } = require('../utils/helpers');
const { duplicateImageDetection, labelDetection } = require('../utils/worker/imageProcessing');
const dynamoService = require('../services/dynamoService');

const evaluationMapper = {
    false: 'ELIGIBLE',
    true: 'EXCLUDED'
};

async function evaluate(labels, projectSettings) {
    const { detectionConfidence, contentTags } = projectSettings;
    const minConfidence = detectionConfidence;

    // Normalize content tags for case-insensitive comparison
    const normalizedContentTags = new Set(
        contentTags.map(tag => tag.value.toLowerCase())
    );

    // Create a stemming/normalization function for better matching
    const normalizeWord = (word) => {
        // Remove common suffixes and standardize terms
        return word.toLowerCase()
            .replace(/[^\w\s]/g, '') // Remove punctuation
            .replace(/(?:s|es|ing|ed)$/, ''); // Remove common endings
    };

    // Create normalized versions of content tags for fuzzy matching
    const normalizedContentTagsSet = new Set(
        [...normalizedContentTags].map(normalizeWord)
    );

    // Check each label
    for (const label of labels) {
        const confidence = parseFloat(label.confidence);

        // Skip if confidence is too low
        if (confidence < minConfidence) {
            continue;
        }

        const normalizedLabelName = label.name.toLowerCase();

        // Direct match check
        if (normalizedContentTags.has(normalizedLabelName)) {
            console.log('Direct match found:', label.name);
            return true;
        }

        // Normalized/fuzzy match check
        const normalizedLabel = normalizeWord(normalizedLabelName);
        if (normalizedContentTagsSet.has(normalizedLabel)) {
            console.log('Normalized match found:', label.name);
            return true;
        }

        // Word-by-word check for multi-word labels
        const labelWords = normalizedLabelName.split(/\s+/);
        for (const word of labelWords) {
            const normalizedWord = normalizeWord(word);
            if (normalizedContentTagsSet.has(normalizedWord)) {
                console.log('Partial match found:', label.name);
                return true;
            }
        }
    }

    console.log('No match found');
    return false;
}

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

            console.log('999 Image quality validated. Performing content detection...');

            // Step 4: Perform content detection if any detection settings are enabled
            let labels = [];
            if (projectSettings?.contentTags?.length > 0 || projectSettings?.textTags?.length > 0) {
                labels = await labelDetection({
                    bucket,
                    s3ObjectKey: processedImageKey,
                });
            }
            const formattedLabels = formatLabels(labels);

            console.log('999 Content detection performed. Evaluating results...');

            // Step 5: Evaluate results and update status
            const evaluation = await evaluate(formattedLabels, projectSettings);
            const finalEvaluation = evaluationMapper[evaluation];

            await dynamoService.updateTaskStatus({
                jobId,
                taskId: imageId,
                imageS3Key: processedImageKey,
                status: COMPLETED,
                labels,
                evaluation: finalEvaluation,
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
                console.log('999 Updating task status to FAILED: ', { jobId, taskId: imageId, imageS3Key: s3ObjectKey, reason: error.message });
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

async function fetchProjectSettingRules(jobId) {
    console.log('Fetching project setting rules...', { jobId });
    try {
        const response = await dynamoService.getItem(process.env.JOB_PROGRESS_TABLE, { JobId: jobId });
        console.log('Fetch project setting rules response:', response);
        return response?.projectSetting;
    } catch (error) {
        // logger.error('Error fetching project setting rules', { error, jobId });
        console.log('dynamoService.getItem() failed', { error, jobId });
        throw error;
    }
}
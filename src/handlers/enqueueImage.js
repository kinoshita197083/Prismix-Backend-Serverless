const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const s3Service = require('../services/s3Service');
const logger = require('../utils/logger');

// Initialize SNS client outside the handler for reuse
const sns = new SNSClient();

// Batch size for concurrent SNS publishing
const CONCURRENT_BATCH_SIZE = 25;

exports.handler = async (event) => {
    const { Records } = event;
    logger.info('Enqueue image event:', { recordCount: Records.length });

    try {
        // Process records in batches for better throughput
        const batches = chunk(Records, CONCURRENT_BATCH_SIZE);
        const results = [];

        for (const batch of batches) {
            const batchPromises = batch.map(async (record) => {
                try {
                    const { bucket, object } = record.s3;

                    // Get object metadata with retry logic
                    const metadata = await getObjectMetadataWithRetry(bucket.name, object.key);

                    // Skip if already processed
                    if (metadata.resized || metadata.resized === "true") {
                        logger.info(`Object ${object.key} is already processed - skipping`);
                        return { skipped: true, key: object.key };
                    }

                    // Parse key information
                    const [type, userId, projectId, projectSettingId, jobId, imageId] = object.key.split('/');

                    if (!jobId || !projectSettingId) {
                        throw new Error(`Invalid key format: ${object.key}`);
                    }

                    // Prepare SNS message
                    const message = {
                        bucket: bucket.name,
                        key: object.key,
                        userId,
                        projectId,
                        jobId,
                        projectSettingId
                    };

                    const publishCommand = new PublishCommand({
                        TopicArn: process.env.SNS_TOPIC_ARN,
                        Message: JSON.stringify(message),
                    });

                    await sns.send(publishCommand);
                    logger.debug(`Published message for: ${object.key}`);
                    return { success: true, key: object.key };

                } catch (error) {
                    logger.error('Error processing record', {
                        error: error.message,
                        key: record.s3.object.key
                    });
                    return { error: true, key: record.s3.object.key, message: error.message };
                }
            });

            const batchResults = await Promise.all(batchPromises);
            results.push(...batchResults);
        }

        // Log summary
        const summary = summarizeResults(results);
        logger.info('Processing summary:', summary);

        return {
            statusCode: 200,
            body: JSON.stringify(summary)
        };

    } catch (error) {
        logger.error('Fatal error in handler', { error: error.message });
        throw error;
    }
};

// Helper function to chunk array for batch processing
function chunk(array, size) {
    return Array.from(
        { length: Math.ceil(array.length / size) },
        (_, index) => array.slice(index * size, (index + 1) * size)
    );
}

// Retry logic for metadata retrieval
async function getObjectMetadataWithRetry(bucket, key, maxRetries = 3) {
    let attempt = 0;
    while (attempt < maxRetries) {
        try {
            return await s3Service.getObjectMetadata(bucket, key);
        } catch (error) {
            attempt++;
            if (attempt === maxRetries) throw error;
            await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, attempt)));
        }
    }
}

// Helper to summarize results
function summarizeResults(results) {
    return results.reduce((acc, result) => {
        if (result.success) acc.successful++;
        else if (result.skipped) acc.skipped++;
        else if (result.error) acc.failed++;
        return acc;
    }, { successful: 0, skipped: 0, failed: 0 });
}
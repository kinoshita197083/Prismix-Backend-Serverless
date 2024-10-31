const { DynamoDBDocumentClient, GetCommand, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { createClient } = require('@supabase/supabase-js');

class JobProgressService {
    constructor(dynamoDB, supabase, config) {
        this.dynamoDB = DynamoDBDocumentClient.from(dynamoDB, {
            marshallOptions: {
                removeUndefinedValues: true,
            }
        });
        this.supabase = supabase;
        this.tasksTable = config.tasksTable;
        this.jobProgressTable = config.jobProgressTable;
    }

    async getJobStats(jobId) {
        console.log('[JobProgressService.getJobStats] Fetching stats for jobId:', jobId);

        const params = {
            TableName: this.tasksTable,
            KeyConditionExpression: "JobID = :jobId",
            ExpressionAttributeValues: { ':jobId': jobId }
        };

        console.log('[JobProgressService.getJobStats] Query params:', params);
        const result = await this.dynamoDB.send(new QueryCommand(params));
        console.log('[JobProgressService.getJobStats] Query result:', result);

        const stats = this._aggregateJobStats(result.Items);
        console.log('[JobProgressService.getJobStats] Aggregated stats:', stats);
        return stats;
    }

    async getCurrentJobProgress(jobId) {
        console.log('[JobProgressService.getCurrentJobProgress] Fetching progress for jobId:', jobId);

        const result = await this.dynamoDB.send(new GetCommand({
            TableName: this.jobProgressTable,
            Key: { JobId: jobId }
        }));

        console.log('[JobProgressService.getCurrentJobProgress] Result:', result);

        if (!result.Item) {
            console.warn('[JobProgressService.getCurrentJobProgress] No progress found for jobId:', jobId);
            throw new Error('Job progress not found');
        }

        // Add last activity check
        const lastUpdated = new Date(result.Item.updatedAt || result.Item.createdAt).getTime();
        const currentTime = new Date().getTime();
        const inactiveThreshold = 30 * 60 * 1000; // 30 minutes in milliseconds

        if (currentTime - lastUpdated > inactiveThreshold) {
            console.warn('[JobProgressService.getCurrentJobProgress] Job appears to be inactive:', {
                jobId,
                lastUpdated: new Date(lastUpdated).toISOString(),
                timeSinceLastUpdate: Math.floor((currentTime - lastUpdated) / 1000 / 60) + ' minutes'
            });
            throw new Error('Job appears to be inactive');
        }

        console.log('[JobProgressService.getCurrentJobProgress] Returning progress:', result.Item);
        return result.Item;
    }

    async updateJobProgress(jobId, data, currentVersion) {
        console.log('[JobProgressService.updateJobProgress] Updating progress:', {
            jobId,
            data,
            currentVersion
        });

        const cleanData = {
            status: data.status || 'IN_PROGRESS',
            processedImages: data.processedImages || 0,
            eligibleImages: data.eligibleImages || 0,
            duplicateImages: data.duplicateImages || 0,
            excludedImages: data.excludedImages || 0,
            reviewedImages: data.reviewedImages || 0,
            processingDetails: data.processingDetails || { failedProcessedImages: [] },
            requiredManualReview: data.requiredManualReview || false
        };

        const { regularParams, manualReviewParams } = this._buildUpdateParams(jobId, cleanData, currentVersion);

        console.log('[JobProgressService.updateJobProgress] Built update params:', {
            regularParams,
            manualReviewParams
        });

        await this.dynamoDB.send(new UpdateCommand(regularParams));

        if (manualReviewParams) {
            try {
                await this.dynamoDB.send(new UpdateCommand(manualReviewParams));
            } catch (error) {
                if (error.name !== 'ConditionalCheckFailedException') {
                    throw error;
                }
            }
        }
    }

    async updateJobStatusRDS(jobId, status) {
        const { data, error } = await this.supabase
            .from('Job')
            .update({ jobStatus: status })
            .eq('id', jobId)
            .single();

        if (error) throw error;
        return data;
    }

    _aggregateJobStats(items) {
        console.log('[JobProgressService._aggregateJobStats] Aggregating stats for items:', items);

        const stats = {
            processedImages: 0,
            eligibleImages: 0,
            duplicateImages: 0,
            excludedImages: 0,
            reviewedImages: 0,
            processingDetails: { failedProcessedImages: [] },
            requiredManualReview: false
        };

        if (!items || !items.length) {
            console.log('[JobProgressService._aggregateJobStats] No items to aggregate');
            return stats;
        }

        return items.reduce((acc, task) => {
            // Count processed images
            if (['COMPLETED', 'REVIEWED'].includes(task.TaskStatus)) {
                acc.processedImages++;
            }

            // Count by evaluation type
            switch (task.Evaluation) {
                case 'ELIGIBLE':
                    acc.eligibleImages++;
                    break;
                case 'DUPLICATE':
                    acc.duplicateImages++;
                    break;
                case 'EXCLUDED':
                    acc.excludedImages++;
                    break;
                case 'FAILED':
                    if (task.ImageS3Key) {
                        acc.processingDetails.failedProcessedImages.push({
                            imageS3Key: task.ImageS3Key,
                            reason: task.Reason || 'Unknown'
                        });
                    }
                    break;
            }

            // Track reviewed images separately
            if (task.TaskStatus === 'REVIEWED') {
                acc.reviewedImages++;
            }

            // Check if any task still needs review
            if (task.TaskStatus === 'WAITING_FOR_REVIEW') {
                acc.requiredManualReview = true;
            }

            return acc;
        }, stats);
    }

    _buildUpdateParams(jobId, data, currentVersion) {
        const regularUpdateValues = {
            ':jobStatus': data.status,
            ':processedImages': data.processedImages,
            ':eligibleImages': data.eligibleImages,
            ':duplicateImages': data.duplicateImages,
            ':excludedImages': data.excludedImages,
            ':reviewedImages': data.reviewedImages,
            ':processingDetails': data.processingDetails,
            ':newVersion': currentVersion + 1,
            ':currentVersion': currentVersion,
            ':updatedAt': new Date().toISOString()
        };

        const regularParams = {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: `
                SET #status = :jobStatus,
                    processedImages = :processedImages,
                    eligibleImages = :eligibleImages,
                    duplicateImages = :duplicateImages,
                    excludedImages = :excludedImages,
                    reviewedImages = :reviewedImages,
                    processingDetails = :processingDetails,
                    version = :newVersion,
                    updatedAt = :updatedAt
            `,
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: regularUpdateValues,
            ConditionExpression: 'version = :currentVersion'
        };

        const manualReviewParams = data.requiredManualReview ? {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: 'SET requiredManualReview = :review',
            ExpressionAttributeValues: {
                ':review': true,
                ':false': false
            },
            ConditionExpression: 'attribute_not_exists(requiredManualReview) OR requiredManualReview = :false'
        } : null;

        return { regularParams, manualReviewParams };
    }

    async markJobAsStale(jobId) {
        console.log('[JobProgressService.markJobAsStale] Marking job as stale:', jobId);

        const params = {
            TableName: this.jobProgressTable,
            Key: { JobId: jobId },
            UpdateExpression: 'SET #status = :status, staleReason = :reason, updatedAt = :updatedAt',
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: {
                ':status': 'STALE',
                ':reason': 'Job inactive for too long',
                ':updatedAt': new Date().toISOString()
            }
        };

        await this.dynamoDB.send(new UpdateCommand(params));
        console.log('[JobProgressService.markJobAsStale] Job marked as stale successfully');
    }
}

module.exports = JobProgressService; 
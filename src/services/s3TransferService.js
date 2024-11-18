const {
  ListObjectsV2Command,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const { AppError } = require("../utils/errors/customErrors");
const logger = require("../utils/logger");
const s3Utils = require("../utils/s3/s3Utils");
const { S3Error } = require("../utils/errors/customErrors");

class S3TransferService {
  constructor(sourceS3Client, destinationS3Client, destinationBucket) {
    this.sourceS3Client = sourceS3Client;
    this.destinationS3Client = destinationS3Client;
    this.destinationBucket = destinationBucket;
    this.BATCH_SIZE = 1000;
    this.CONCURRENT_TRANSFERS = 10;
  }

  async startTransfer({ sourceBucket, userId, jobId }) {
    let continuationToken = null;
    let totalProcessed = 0;

    do {
      const objects = await this.listObjects(sourceBucket, continuationToken);

      if (!objects.Contents?.length) {
        break;
      }

      // Process batch of objects
      await this.processBatch({
        objects: objects.Contents,
        sourceBucket,
        userId,
        jobId,
      });

      totalProcessed += objects.Contents.length;
      continuationToken = objects.NextContinuationToken;

      logger.info("Batch processing complete", {
        processedCount: totalProcessed,
        hasMore: !!continuationToken,
      });
    } while (continuationToken);
  }

  async listObjects(bucket, continuationToken = null) {
    try {
      const command = new ListObjectsV2Command({
        Bucket: bucket,
        MaxKeys: this.BATCH_SIZE,
        ContinuationToken: continuationToken,
      });

      return await this.sourceS3Client.send(command);
    } catch (error) {
      throw new AppError(`Failed to list objects: ${error.message}`, 500);
    }
  }

  async processBatch({ objects, sourceBucket, userId, jobId }) {
    const transfers = objects.map((object) =>
      this.transferObject({
        sourceBucket,
        sourceKey: object.Key,
        userId,
        jobId,
      })
    );

    // Process transfers with concurrency limit
    for (let i = 0; i < transfers.length; i += this.CONCURRENT_TRANSFERS) {
      const batch = transfers.slice(i, i + this.CONCURRENT_TRANSFERS);
      await Promise.allSettled(batch);
    }
  }

  async transferObject({ sourceBucket, sourceKey, userId, jobId }) {
    try {
      // Validate image type
      if (!s3Utils.isValidImageKey(sourceKey)) {
        throw new S3Error(`Invalid image type: ${sourceKey}`, 400);
      }

      // Generate destination key
      const destinationKey = s3Utils.generateDestinationKey({
        userId,
        jobId,
        originalKey: sourceKey,
      });

      // Create metadata
      const metadata = s3Utils.createObjectMetadata({
        sourceBucket,
        sourceKey,
        userId,
        jobId,
      });

      // Get object from source bucket
      const getCommand = new GetObjectCommand({
        Bucket: sourceBucket,
        Key: sourceKey,
      });

      const sourceObject = await this.sourceS3Client.send(getCommand);

      // Use multipart upload for large files
      const upload = new Upload({
        client: this.destinationS3Client,
        params: {
          Bucket: this.destinationBucket,
          Key: destinationKey,
          Body: sourceObject.Body,
          ContentType: sourceObject.ContentType,
          Metadata: metadata,
        },
        queueSize: 4,
        partSize: 5 * 1024 * 1024,
      });

      await upload.done();

      logger.info("Object transferred successfully", {
        sourceKey,
        destinationKey,
      });
    } catch (error) {
      logger.error("Failed to transfer object", {
        sourceKey,
        error: error.message,
      });
      throw new AppError(
        `Failed to transfer object ${sourceKey}: ${error.message}`,
        500
      );
    }
  }
}

module.exports = { S3TransferService };

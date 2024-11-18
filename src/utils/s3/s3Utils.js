const { S3Error } = require("../errors/customErrors");

const s3Utils = {
  /**
   * Validates S3 key to ensure it's a valid image
   */
  isValidImageKey: (key) => {
    const validExtensions = [".jpg", ".jpeg", ".png", ".gif", ".webp"];
    const ext = key.toLowerCase().split(".").pop();
    return validExtensions.includes(`.${ext}`);
  },

  /**
   * Generates a unique destination key
   */
  generateDestinationKey: ({ userId, jobId, originalKey }) => {
    return `uploads/${userId}/${jobId}/${originalKey}`;
  },

  /**
   * Extracts content type from key
   */
  getContentType: (key) => {
    const extension = key.toLowerCase().split(".").pop();
    const contentTypes = {
      jpg: "image/jpeg",
      jpeg: "image/jpeg",
      png: "image/png",
      gif: "image/gif",
      webp: "image/webp",
    };
    return contentTypes[extension] || "application/octet-stream";
  },

  /**
   * Validates bucket name
   */
  validateBucketName: (bucketName) => {
    if (!bucketName || typeof bucketName !== "string") {
      throw new S3Error("Invalid bucket name", 400);
    }

    // AWS bucket naming rules
    const bucketNameRegex = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
    if (!bucketNameRegex.test(bucketName)) {
      throw new S3Error("Invalid bucket name format", 400);
    }
  },

  /**
   * Calculates optimal part size for multipart upload based on file size
   */
  calculatePartSize: (fileSize) => {
    const minimumPartSize = 5 * 1024 * 1024; // 5MB
    const maximumPartSize = 5 * 1024 * 1024 * 1024; // 5GB
    const targetPartCount = 10000; // AWS limit is 10,000 parts

    let partSize = Math.ceil(fileSize / targetPartCount);
    partSize = Math.max(partSize, minimumPartSize);
    partSize = Math.min(partSize, maximumPartSize);

    return partSize;
  },

  /**
   * Creates metadata object for destination object
   */
  createObjectMetadata: ({ sourceBucket, sourceKey, userId, jobId }) => {
    return {
      "original-bucket": sourceBucket,
      "original-key": sourceKey,
      "user-id": userId,
      "job-id": jobId,
      "transfer-date": new Date().toISOString(),
    };
  },
};

module.exports = s3Utils;

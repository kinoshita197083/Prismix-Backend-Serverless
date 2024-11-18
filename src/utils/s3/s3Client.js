const { S3Client } = require("@aws-sdk/client-s3");
const { S3Error } = require("../errors/customErrors");

const createS3Client = (config = {}) => {
  try {
    return new S3Client({
      region: config.region || process.env.AWS_REGION,
      credentials: config.credentials,
      maxAttempts: 3,
      retryMode: "adaptive",
      requestTimeout: 300000, // 5 minutes
      ...config,
    });
  } catch (error) {
    throw new S3Error(`Failed to create S3 client: ${error.message}`);
  }
};

module.exports = {
  createS3Client,
};

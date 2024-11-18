const { S3Client } = require("@aws-sdk/client-s3");
const { RDSService } = require("../services/rdsService");
const { S3TransferService } = require("../services/s3TransferService");
const { UserService } = require("../services/userService");
const logger = require("../utils/logger");
const { createS3Client } = require("../utils/s3/s3Client");

exports.handler = async (event) => {
  logger.info("Starting S3 bucket transfer process", { event });

  try {
    const { userId, jobId } = JSON.parse(event.body);

    // Initialize services
    const rdsService = new RDSService();
    const userService = new UserService(rdsService);

    // Fetch user's S3 connection details
    const s3Connection = await userService.getS3Connection(userId);

    if (!s3Connection) {
      throw new Error("S3 connection details not found");
    }

    // Create S3 clients for source and destination
    const sourceS3Client = createS3Client({
      region: s3Connection.region,
    });

    const destinationS3Client = new S3Client({
      region: process.env.AWS_REGION,
    });

    const s3TransferService = new S3TransferService(
      sourceS3Client,
      destinationS3Client,
      process.env.DESTINATION_BUCKET
    );

    // Start the transfer process
    await s3TransferService.startTransfer({
      sourceBucket: s3Connection.bucketName,
      userId,
      jobId,
    });

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Transfer process initiated successfully",
        jobId,
      }),
    };
  } catch (error) {
    logger.error("Error in S3 bucket transfer", {
      error: error.message,
      stack: error.stack,
    });

    return {
      statusCode: error.statusCode || 500,
      body: JSON.stringify({
        message: error.message,
      }),
    };
  }
};

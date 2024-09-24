const AWS = require('aws-sdk');
const sqs = new AWS.SQS();

exports.handler = async (event) => {
    // Process S3 event
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));

    // Process the image (implementation details depend on your requirements)
    console.log(`Processing image: ${bucket}/${key}`);

    // Send message to SQS
    const params = {
        MessageBody: JSON.stringify({ bucket, key }),
        QueueUrl: process.env.SQS_QUEUE_URL
    };

    await sqs.sendMessage(params).promise();

    return {
        statusCode: 200,
        body: JSON.stringify('Processing complete'),
    };
};
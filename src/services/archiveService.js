const archiver = require('archiver');
const { Upload } = require("@aws-sdk/lib-storage");
const { PassThrough } = require('stream');
const { GetObjectCommand } = require("@aws-sdk/client-s3");

class ArchiveService {
    constructor(s3Client) {
        this.s3Client = s3Client;
    }

    async streamFilesToArchive(archive, imageKeys, sourceBucket) {
        for (const imageKey of imageKeys) {
            const command = new GetObjectCommand({
                Bucket: sourceBucket,
                Key: imageKey
            });

            const response = await this.s3Client.send(command);
            archive.append(response.Body, { name: imageKey.split('/').pop() });
        }
    }
}

module.exports = ArchiveService;
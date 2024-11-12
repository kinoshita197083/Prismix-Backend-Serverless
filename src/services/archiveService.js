const archiver = require('archiver');
const logger = require('../utils/logger');

class ArchiveService {
    constructor(s3Service) {
        this.s3Service = s3Service;
    }

    async createArchiveStream(jobId, zipKey, bucket) {
        const { passThrough, upload } = this.s3Service.createUploadStream(bucket, zipKey);

        const archive = archiver('zip', {
            zlib: { level: 6 }
        });

        archive.pipe(passThrough);

        archive.on('progress', (progress) => {
            logger.info('Archive progress', {
                jobId,
                entriesProcessed: progress.entries.processed,
                bytesProcessed: progress.fs.processedBytes
            });
        });

        return { archive, upload };
    }

    async streamFilesToArchive(archive, imageKeys, sourceBucket) {
        await Promise.all(imageKeys.map(async (key) => {
            const stream = await this.s3Service.getS3ReadStream(sourceBucket, key);
            archive.append(stream, { name: key.split('/').pop() });
        }));
    }

    async finalizeArchive(archive, upload) {
        await archive.finalize();
        await upload.done();
    }
}

module.exports = ArchiveService;
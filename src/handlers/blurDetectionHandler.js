const cv = require('@u4/opencv4nodejs');

exports.handler = async (event) => {
    try {
        const { imageBuffer } = JSON.parse(event.body);
        const buffer = Buffer.from(imageBuffer, 'base64');

        // Read the image from buffer
        const mat = cv.imdecode(buffer);
        const grayMat = mat.bgrToGray();

        // Apply Laplacian operator
        const laplacian = grayMat.laplacian(cv.CV_64F);

        // Calculate variance
        const mean = laplacian.mean().w;
        const variance = laplacian.sub(mean).pow(2).mean().w;

        return {
            statusCode: 200,
            body: JSON.stringify({
                blurScore: variance
            })
        };
    } catch (error) {
        console.error('Error in blur detection:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                error: 'Failed to process image',
                details: error.message
            })
        };
    }
} 
const cv = require('@u4/opencv4nodejs');

// Constants for blur detection
const BLUR_THRESHOLD = 100.0; // Adjust based on your needs
const MIN_IMAGE_SIZE = 200; // Minimum dimension size

const validateImage = (mat) => {
    const { rows, cols } = mat;
    if (rows < MIN_IMAGE_SIZE || cols < MIN_IMAGE_SIZE) {
        throw new Error(`Image too small. Minimum dimension is ${MIN_IMAGE_SIZE}px`);
    }
    return mat;
};

const calculateBlurScore = (mat) => {
    const grayMat = mat.bgrToGray();
    const laplacian = grayMat.laplacian(cv.CV_64F);
    const mean = laplacian.mean().w;
    const variance = laplacian.sub(mean).pow(2).mean().w;

    // Normalize by image size
    const normalizedVariance = variance / (mat.rows * mat.cols);
    return normalizedVariance;
};

const interpretBlurScore = (score) => ({
    blurScore: score,
    isBlurry: score < BLUR_THRESHOLD,
    threshold: BLUR_THRESHOLD
});

exports.handler = async (event) => {
    try {
        const { imageBuffer } = JSON.parse(event.body);
        const buffer = Buffer.from(imageBuffer, 'base64');

        const mat = validateImage(cv.imdecode(buffer));
        const blurScore = calculateBlurScore(mat);
        const result = interpretBlurScore(blurScore);

        return {
            statusCode: 200,
            body: JSON.stringify(result)
        };
    } catch (error) {
        console.error('Error in blur detection:', error);
        return {
            statusCode: error.message.includes('Image too small') ? 400 : 500,
            body: JSON.stringify({
                error: 'Failed to process image',
                details: error.message
            })
        };
    }
}; 
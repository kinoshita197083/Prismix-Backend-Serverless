const QUALITY_CONFIG = {
    // Noise thresholds (Enhanced SNR with multiple methods)
    NOISE: {
        SEVERE: 8,       // Very noisy images
        MODERATE: 15,    // Moderately noisy
        MINIMAL: 25,     // Low noise
        EXCELLENT: 35    // Very clean images
    },

    // Blurriness thresholds (Combined methods score)
    BLUR: {
        SEVERE: 200,      // Very blurry
        MODERATE: 400,    // Moderately sharp
        SHARP: 600,       // Sharp
        VERY_SHARP: 800   // Very sharp
    },

    // Frontend configuration
    FRONTEND: {
        noise: {
            min: 8,
            max: 35,
            step: 1,
            presets: {
                low: 8,      // Accept only very clean images
                medium: 15,  // Accept moderately clean images
                high: 25     // More tolerant of noise
            }
        },
        blur: {
            min: 200,
            max: 800,
            step: 50,
            presets: {
                low: 600,    // Accept only very sharp images
                medium: 400, // Accept moderately sharp images
                high: 200    // More tolerant of blur
            }
        }
    }
};

module.exports = QUALITY_CONFIG; 
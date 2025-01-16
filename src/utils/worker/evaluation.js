const { EXCLUDED, ELIGIBLE } = require("../config");
const pluralize = require('pluralize');

const evaluationMapper = {
    false: ELIGIBLE,
    true: EXCLUDED
};

const debug = (message, ...args) => {
    console.log(`[Evaluation] ${message}`, ...args);
};

// Helper functions for text normalization
const normalizeWord = (word) => {
    // Handle empty or non-string inputs
    if (!word || typeof word !== 'string') return '';

    const singularForm = pluralize.singular(word.toLowerCase());

    // Enhanced special character handling
    const normalized = singularForm
        .replace(/[^\w\s-]/g, '') // Keep hyphens but remove other special chars
        .replace(/\s+/g, ' ')     // Normalize multiple spaces
        .trim()                   // Remove leading/trailing spaces
        .replace(/(?:ing|ed)$/, '');

    debug('Normalized word:', {
        original: word,
        singular: singularForm,
        normalized
    });
    return normalized;
};

const createNormalizedSet = (contentTags) => {
    if (!Array.isArray(contentTags)) {
        debug('[createNormalizedSet] Invalid contentTags format:', contentTags);
        return { direct: new Set(), fuzzy: new Set() };
    }

    const validTags = contentTags.filter(tag =>
        tag && typeof tag === 'object' &&
        typeof tag.value === 'string' &&
        tag.value.trim().length > 0
    );

    if (validTags.length !== contentTags.length) {
        debug('[createNormalizedSet] Some tags were invalid:', {
            original: contentTags,
            valid: validTags
        });
    }

    debug('[createNormalizedSet] Creating normalized tag sets from:', contentTags);
    const normalizedTags = new Set(validTags.map(tag => tag.value.toLowerCase()));
    const fuzzyTags = new Set([...normalizedTags].map(normalizeWord));

    debug('[createNormalizedSet] Created tag sets:', {
        direct: [...normalizedTags],
        fuzzy: [...fuzzyTags]
    });

    return {
        direct: normalizedTags,
        fuzzy: fuzzyTags
    };
};

// Matching functions
const handleCompoundWords = (text) => {
    // Handle both hyphenated and space-separated compound words
    const variations = [
        text,
        text.replace(/\s+/g, '-'),  // space to hyphen
        text.replace(/-/g, ' ')      // hyphen to space
    ];
    return variations;
};

const hasDirectMatch = (text, tagSet) => {
    const normalizedText = text.toLowerCase();
    const variations = handleCompoundWords(normalizedText);

    const hasMatch = variations.some(variant => tagSet.has(variant));
    debug('Direct match check:', {
        text,
        normalizedText,
        variations,
        hasMatch
    });
    return hasMatch;
};

const hasFuzzyMatch = (text, tagSet) => {
    const normalizedText = normalizeWord(text.toLowerCase());
    const hasMatch = tagSet.has(normalizedText);
    debug('Fuzzy match check:', {
        text,
        normalizedText,
        hasMatch
    });
    return hasMatch;
};

const hasPartialMatch = (text, tagSet) => {
    const words = text.toLowerCase().split(/\s+/);
    debug('Partial match check - words:', { text, words });

    const hasMatch = words.some(word => {
        const normalizedWord = normalizeWord(word);
        const matched = tagSet.has(normalizedWord);
        debug('Partial match word check:', {
            word,
            normalizedWord,
            matched
        });
        return matched;
    });

    return hasMatch;
};

const checkMatches = (text, tagSets, source) => {
    debug(`Checking matches for ${source}:`, { text });

    if (hasDirectMatch(text, tagSets.direct)) {
        debug(`Direct ${source} match found:`, text);
        return true;
    }
    if (hasFuzzyMatch(text, tagSets.fuzzy)) {
        debug(`Normalized ${source} match found:`, text);
        return true;
    }
    if (hasPartialMatch(text, tagSets.fuzzy)) {
        debug(`Partial ${source} match found:`, text);
        return true;
    }

    debug(`No ${source} matches found for:`, text);
    return false;
};

// Main evaluation functions
const evaluateDetectedTexts = (formattedDetectedTexts, tagSets, projectSettings) => {
    debug('Evaluating detected texts:', formattedDetectedTexts);

    const result = formattedDetectedTexts.some(detection => {
        const confidence = parseFloat(detection.confidence);
        const meetsConfidence = confidence >= projectSettings.detectionConfidence;

        debug('Checking detected text:', {
            text: detection.text,
            confidence,
            meetsConfidence
        });

        return meetsConfidence && checkMatches(detection.text, tagSets, 'text');
    });

    debug('Detected texts evaluation result:', result);
    return result;
};

const evaluateLabels = (labels, tagSets, minConfidence) => {
    debug('Evaluating labels:', { labels, minConfidence });

    const result = labels.some(label => {
        const confidence = parseFloat(label.confidence);
        const meetsConfidence = confidence >= minConfidence;

        debug('Checking label:', {
            label: label.name,
            confidence,
            meetsConfidence
        });

        return meetsConfidence && checkMatches(label.name, tagSets, 'label');
    });

    debug('Labels evaluation result:', result);
    return result;
};

// Main evaluation function
async function evaluate(labels = [], formattedDetectedTexts = [], projectSettings) {
    debug('[evaluate] Starting evaluation with:', {
        labelsCount: labels.length,
        textsCount: formattedDetectedTexts.length,
        settings: projectSettings
    });

    const { detectionConfidence, contentTags, textTags } = projectSettings;
    debug('[evaluate] projectSettings: ', { detectionConfidence, contentTags, textTags });

    // Early return if both tag sets are empty
    if ((!contentTags?.length) && (!textTags?.length)) {
        debug('Both contentTags and textTags are empty, skipping evaluation');
        return {
            result: false,
            reason: 'No content or text tags configured for evaluation'
        };
    }

    const contentTagSets = createNormalizedSet(contentTags || []);
    const textTagSets = createNormalizedSet(textTags || []);

    const textMatchResult = evaluateDetectedTexts(formattedDetectedTexts, textTagSets, projectSettings);
    const labelMatchResult = evaluateLabels(labels, contentTagSets, detectionConfidence);

    debug('[evaluate] Evaluation results:', {
        textMatch: textMatchResult,
        labelMatch: labelMatchResult,
        contentTagSets,
        textTagSets,
        formattedDetectedTexts,
        labels
    });

    const finalResult = textMatchResult || labelMatchResult;
    const reason = generateEvaluationReason(textMatchResult, labelMatchResult, contentTags, textTags);

    debug(`Evaluation complete: ${finalResult ? 'Matches found' : 'No matches found'}, Reason: ${reason}`);

    return {
        result: finalResult,
        reason
    };
}

function generateEvaluationReason(textMatch, labelMatch, contentTags, textTags) {
    const reasons = [];

    if (contentTags?.length > 0 && labelMatch) {
        reasons.push('Content matches excluded by configured content tags');
    }

    if (textTags?.length > 0 && textMatch) {
        reasons.push('Text matches excluded by configured text tags');
    }

    return reasons.length > 0 ? reasons.join(' and ') : 'No exclusion criteria met';
}

module.exports = { evaluationMapper, evaluate };
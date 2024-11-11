const { EXCLUDED, ELIGIBLE } = require("../config");

const evaluationMapper = {
    false: ELIGIBLE,
    true: EXCLUDED
};

const debug = (message, ...args) => {
    console.log(`[Evaluation] ${message}`, ...args);
};

// Helper functions for text normalization
const normalizeWord = (word) => {
    const normalized = word.toLowerCase()
        .replace(/[^\w\s]/g, '')
        .replace(/(?:s|es|ing|ed)$/, '');
    debug('Normalized word:', { original: word, normalized });
    return normalized;
};

const createNormalizedSet = (contentTags) => {
    debug('Creating normalized tag sets from:', contentTags);
    const normalizedTags = new Set(contentTags.map(tag => tag.value.toLowerCase()));
    const fuzzyTags = new Set([...normalizedTags].map(normalizeWord));

    debug('Created tag sets:', {
        direct: [...normalizedTags],
        fuzzy: [...fuzzyTags]
    });

    return {
        direct: normalizedTags,
        fuzzy: fuzzyTags
    };
};

// Matching functions
const hasDirectMatch = (text, tagSet) => {
    const normalizedText = text.toLowerCase();
    const hasMatch = tagSet.has(normalizedText);
    debug('Direct match check:', {
        text,
        normalizedText,
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
const evaluateDetectedTexts = (texts, tagSets) => {
    debug('Evaluating detected texts:', texts);
    const result = texts.some(text => checkMatches(text, tagSets, 'text'));
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
    debug('Starting evaluation with:', {
        labelsCount: labels.length,
        textsCount: formattedDetectedTexts.length,
        settings: projectSettings
    });

    const { detectionConfidence, contentTags, textTags } = projectSettings;
    console.log('detectionConfidence: ', detectionConfidence);

    const contentTagSets = createNormalizedSet(contentTags);
    const textTagSets = createNormalizedSet(textTags);

    const textMatchResult = formattedDetectedTexts.length === 0 || evaluateDetectedTexts(formattedDetectedTexts, textTagSets);
    const labelMatchResult = labels.length === 0 || evaluateLabels(labels, contentTagSets, detectionConfidence);

    debug('Evaluation results:', {
        textMatch: textMatchResult,
        labelMatch: labelMatchResult
    });

    const finalResult = textMatchResult && labelMatchResult;
    debug(`Evaluation complete: ${finalResult ? 'Matches found' : 'No matches found'}`);

    return finalResult;
}

module.exports = { evaluationMapper, evaluate };
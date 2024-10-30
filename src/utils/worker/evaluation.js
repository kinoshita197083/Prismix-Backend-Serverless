const evaluationMapper = {
    false: 'ELIGIBLE',
    true: 'EXCLUDED'
};

async function evaluate(labels, projectSettings) {
    const { detectionConfidence, contentTags } = projectSettings;
    const minConfidence = detectionConfidence;

    // Normalize content tags for case-insensitive comparison
    const normalizedContentTags = new Set(
        contentTags.map(tag => tag.value.toLowerCase())
    );

    // Create a stemming/normalization function for better matching
    const normalizeWord = (word) => {
        // Remove common suffixes and standardize terms
        return word.toLowerCase()
            .replace(/[^\w\s]/g, '') // Remove punctuation
            .replace(/(?:s|es|ing|ed)$/, ''); // Remove common endings
    };

    // Create normalized versions of content tags for fuzzy matching
    const normalizedContentTagsSet = new Set(
        [...normalizedContentTags].map(normalizeWord)
    );

    // Check each label
    for (const label of labels) {
        const confidence = parseFloat(label.confidence);

        // Skip if confidence is too low
        if (confidence < minConfidence) {
            continue;
        }

        const normalizedLabelName = label.name.toLowerCase();

        // Direct match check
        if (normalizedContentTags.has(normalizedLabelName)) {
            console.log('Direct match found:', label.name);
            return true;
        }

        // Normalized/fuzzy match check
        const normalizedLabel = normalizeWord(normalizedLabelName);
        if (normalizedContentTagsSet.has(normalizedLabel)) {
            console.log('Normalized match found:', label.name);
            return true;
        }

        // Word-by-word check for multi-word labels
        const labelWords = normalizedLabelName.split(/\s+/);
        for (const word of labelWords) {
            const normalizedWord = normalizeWord(word);
            if (normalizedContentTagsSet.has(normalizedWord)) {
                console.log('Partial match found:', label.name);
                return true;
            }
        }
    }

    console.log('No match found');
    return false;
}

module.exports = { evaluationMapper, evaluate };
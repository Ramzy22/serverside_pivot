const REPORT_EDITOR_DEFAULT_SEGMENT = 'default';
const REPORT_EDITOR_BRANCH_PREFIX = 'branch:';

/** @param {any} [index] @returns {any} */
const createReportEditorBranchSegment = (index) => `${REPORT_EDITOR_BRANCH_PREFIX}${index}`;

/** @param {any} [value] @returns {any} */
const normalizeReportEditorRowValue = (value) => (
    value === undefined || value === null ? '' : String(value)
);

/** @param {any} [branch] @param {any} [field] @param {any} [value] @returns {any} */
const reportEditorBranchMatchesRowValue = (branch, field, value) => {
    if (!branch || !field) return false;
    /** @type {any} */
    const expected = normalizeReportEditorRowValue(value);
    /** @type {any} */
    const clauses = branch.condition && Array.isArray(branch.condition.clauses)
        ? branch.condition.clauses
        : [];
    return clauses.some(/** @param {any} [clause] @returns {any} */ (clause) => {
        if (!clause || clause.field !== field) return false;
        if (clause.operator === 'eq' || clause.operator === '=') {
            return normalizeReportEditorRowValue(clause.value) === expected;
        }
        if (clause.operator === 'in') {
            return Array.isArray(clause.values)
                && clause.values.map(normalizeReportEditorRowValue).includes(expected);
        }
        return false;
    });
};

/** @param {any} [node] @returns {any} */
const getReportNodeChildCandidates = (node) => {
    /** @type {any[]} */
    const candidates = [];
    if (!node || typeof node !== 'object') return candidates;

    if (node.defaultChild && typeof node.defaultChild === 'object') {
        candidates.push({
            kind: 'default',
            segment: REPORT_EDITOR_DEFAULT_SEGMENT,
            child: node.defaultChild,
        });
    }

    if (Array.isArray(node.branches)) {
        node.branches.forEach(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => {
            if (branch && branch.child && typeof branch.child === 'object') {
                candidates.push({
                    kind: 'branch',
                    segment: createReportEditorBranchSegment(index),
                    child: branch.child,
                    branch,
                });
            }
        });
        return candidates;
    }

    if (node.childrenByValue && typeof node.childrenByValue === 'object') {
        Object.entries(node.childrenByValue).forEach(([valueKey, child], index) => {
            if (child && typeof child === 'object') {
                candidates.push({
                    kind: 'value',
                    segment: createReportEditorBranchSegment(index),
                    child,
                    valueKey,
                });
            }
        });
    }

    return candidates;
};

/** @param {any} [rowData] @returns {any} */
const parseReportRowPathValues = (rowData) => {
    if (!rowData || typeof rowData !== 'object') return [];
    if (Array.isArray(rowData._pathValues)) {
        return rowData._pathValues.map(normalizeReportEditorRowValue);
    }
    if (typeof rowData._path === 'string' && rowData._path && rowData._path !== '__grand_total__') {
        return rowData._path.split('|||').map(normalizeReportEditorRowValue);
    }
    return [];
};

/** @param {any} [rowData] @param {any} [fieldSequence] @param {any} [index] @returns {any} */
const getReportRowPathValueAt = (rowData, fieldSequence, index) => {
    /** @type {any} */
    const pathValues = parseReportRowPathValues(rowData);
    if (index >= 0 && index < pathValues.length) return pathValues[index];
    /** @type {any} */
    const field = Array.isArray(fieldSequence) ? fieldSequence[index] : null;
    if (field && Object.prototype.hasOwnProperty.call(rowData || {}, field)) {
        return rowData[field];
    }
    if (index === (Array.isArray(fieldSequence) ? fieldSequence.length - 1 : -1)) {
        return rowData && rowData._id !== undefined && rowData._id !== null ? rowData._id : rowData && rowData._label;
    }
    return undefined;
};

/** @param {any} [root] @param {any} [fieldSequence] @param {any} [targetIndex] @param {any} [rowData] @returns {any} */
const findReportEditorNodePathForRow = (root, fieldSequence, targetIndex, rowData) => {
    if (!root || typeof root !== 'object' || !Array.isArray(fieldSequence) || targetIndex < 0) {
        return null;
    }

    let current = root;
    /** @type {any[]} */
    const currentPath = [];

    for (let fieldIndex = 0; fieldIndex <= targetIndex; fieldIndex += 1) {
        if (!current || typeof current !== 'object') return null;
        const expectedField = fieldSequence[fieldIndex];
        if (expectedField && current.field && current.field !== expectedField) return null;
        if (fieldIndex === targetIndex) return currentPath;

        /** @type {any} */
        const currentField = current.field || expectedField;
        /** @type {any} */
        const currentValue = getReportRowPathValueAt(rowData, fieldSequence, fieldIndex);
        const nextField = fieldSequence[fieldIndex + 1];
        /** @type {any} */
        const candidates = getReportNodeChildCandidates(current);
        if (candidates.length === 0) return null;

        /** @type {any} */
        const valueMatches = candidates.filter(/** @param {any} [candidate] @returns {any} */ (candidate) => {
            if (candidate.kind === 'branch') {
                return reportEditorBranchMatchesRowValue(candidate.branch, currentField, currentValue);
            }
            if (candidate.kind === 'value') {
                return normalizeReportEditorRowValue(candidate.valueKey) === normalizeReportEditorRowValue(currentValue);
            }
            return false;
        });
        /** @type {any} */
        const valueAndFieldMatches = valueMatches.filter(/** @param {any} p0 @returns {any} */ ({ child }) => (
            !nextField || !child || child.field === nextField
        ));
        /** @type {any} */
        const fieldMatches = candidates.filter(/** @param {any} p0 @returns {any} */ ({ child }) => (
            !nextField || !child || child.field === nextField
        ));
        /** @type {any} */
        const ordered = valueAndFieldMatches.length > 0
            ? valueAndFieldMatches
            : (valueMatches.length > 0 ? valueMatches : fieldMatches);
        /** @type {any} */
        const selected = ordered[0] || candidates[0];
        if (!selected) return null;

        currentPath.push(selected.segment);
        current = selected.child;
    }

    return currentPath;
};

/** @param {any} [root] @param {any} [targetField] @param {any} [targetDepth] @returns {any} */
const findReportEditorNodePathByField = (root, targetField, targetDepth = null) => {
    if (!root || typeof root !== 'object' || !targetField) return null;
    /** @type {any[]} */
    const matches = [];
    /** @param {any} [node] @param {any} [path] @param {any} [depth] @returns {any} */
    const visit = (node, path, depth) => {
        if (!node || typeof node !== 'object') return;
        if (node.field === targetField) {
            matches.push({
                path,
                score: Number.isFinite(Number(targetDepth))
                    ? Math.abs(depth - Number(targetDepth))
                    : depth,
            });
        }
        getReportNodeChildCandidates(node).forEach(/** @param {any} p0 @returns {any} */ ({ segment, child }) => {
            visit(child, [...path, segment], depth + 1);
        });
    };
    visit(root, [], 0);
    if (matches.length === 0) return null;
    matches.sort((left, right) => left.score - right.score || left.path.length - right.path.length);
    return matches[0].path;
};

/** @param {any} [reportDef] @param {any} [rowData] @returns {any} */
export const buildReportEditorSelectionFromRow = (reportDef, rowData) => {
    /** @type {any} */
    const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
    if (!root || typeof root !== 'object' || !rowData || typeof rowData !== 'object') return null;
    if (rowData._isOther) return null;

    /** @type {any} */
    const pathFields = Array.isArray(rowData._pathFields)
        ? rowData._pathFields.filter(/** @param {any} [field] @returns {any} */ (field) => typeof field === 'string' && field.trim())
        : [];
    /** @type {any} */
    const numericDepth = Number.isFinite(Number(rowData.depth))
        ? Math.max(0, Math.floor(Number(rowData.depth)))
        : null;
    /** @type {any} */
    const levelField = typeof rowData._levelField === 'string' && rowData._levelField.trim()
        ? rowData._levelField.trim()
        : (
            pathFields.length > 0
                ? pathFields[Math.min(
                    pathFields.length - 1,
                    numericDepth !== null ? numericDepth : (pathFields.length - 1)
                )]
                : (typeof root.field === 'string' ? root.field : '')
        );

    if (!levelField) return null;

    /** @type {any} */
    const fieldSequence = pathFields.length > 0 ? pathFields : [levelField];
    /** @type {any} */
    const targetIndex = numericDepth !== null && numericDepth < fieldSequence.length
        ? numericDepth
        : fieldSequence.lastIndexOf(levelField);

    if (targetIndex < 0) return null;

    /** @param {any} [node] @param {any} [fieldIndex] @param {any} [currentPath] @returns {any} */
    const visit = (node, fieldIndex, currentPath) => {
        if (!node || typeof node !== 'object') return null;

        const expectedField = fieldSequence[fieldIndex];
        if (expectedField && node.field && node.field !== expectedField) return null;
        if (fieldIndex === targetIndex) return currentPath;

        const nextField = fieldSequence[fieldIndex + 1];
        /** @type {any} */
        const candidates = getReportNodeChildCandidates(node);
        /** @type {any} */
        const prioritized = candidates.filter(/** @param {any} p0 @returns {any} */ ({ child }) => child && child.field === nextField);
        /** @type {any} */
        const ordered = prioritized.length > 0 ? prioritized : candidates;

        for (const candidate of ordered) {
            /** @type {any} */
            const matchedPath = visit(candidate.child, fieldIndex + 1, [...currentPath, candidate.segment]);
            if (matchedPath) return matchedPath;
        }

        return null;
    };

    /** @type {any} */
    const matchedPath = findReportEditorNodePathForRow(root, fieldSequence, targetIndex, rowData)
        || visit(root, 0, []);
    /** @type {any} */
    const fallbackPath = matchedPath || findReportEditorNodePathByField(root, levelField, numericDepth);
    if (!fallbackPath) return null;

    /** @type {any} */
    const pathValues = parseReportRowPathValues(rowData);
    /** @type {any} */
    const pathValueAtLevel = targetIndex >= 0 && targetIndex < pathValues.length
        ? pathValues[targetIndex]
        : undefined;
    /** @type {any} */
    const rowValue = Object.prototype.hasOwnProperty.call(rowData, levelField)
        ? rowData[levelField]
        : (
            pathValueAtLevel !== undefined && pathValueAtLevel !== null && pathValueAtLevel !== ''
                ? pathValueAtLevel
                : (
                    rowData._id !== undefined && rowData._id !== null && rowData._id !== ''
                        ? rowData._id
                        : rowData._label
                )
        );
    if (rowValue === undefined || rowValue === null || rowValue === '') {
        return { type: 'node', path: fallbackPath };
    }

    return {
        type: 'rowPath',
        nodePath: fallbackPath,
        field: levelField,
        value: rowValue,
        label: rowData._reportDisplayLabel || rowData._label || rowData._id || String(rowValue),
        levelNumber: targetIndex + 1,
        pathValues,
        pathFields,
        debug: rowData._reportDebug || null,
    };
};

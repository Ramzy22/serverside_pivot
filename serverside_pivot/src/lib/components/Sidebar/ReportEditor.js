import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Icons from '../../utils/Icons';
import { formatAggLabel, formatDisplayLabel } from '../../utils/helpers';
import { formatCustomAwareFieldLabel, isCustomCategoryField } from '../../hooks/usePivotNormalization';
import { applySavedReport, captureReportLayout } from '../../utils/savedReports';

/** @returns {any} */
const uid = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

/** @type {any[]} */
const OPERATORS = [
    { value: 'eq', label: 'Equals' },
    { value: 'not_eq', label: 'Not equals' },
    { value: 'in', label: 'In list' },
    { value: 'not_in', label: 'Not in list' },
    { value: 'contains', label: 'Contains' },
    { value: 'not_contains', label: 'Does not contain' },
    { value: 'gt', label: 'Greater than' },
    { value: 'gte', label: 'Greater or equal' },
    { value: 'lt', label: 'Less than' },
    { value: 'lte', label: 'Less or equal' },
];

/** @type {any} */
const LIST_OPS = new Set(['in', 'not_in']);
/** @type {Record<string, any>} */
const OP_SYMBOL = {
    eq: '=',
    not_eq: '!=',
    in: 'in',
    not_in: 'not in',
    contains: 'contains',
    not_contains: 'not contains',
    gt: '>',
    gte: '>=',
    lt: '<',
    lte: '<=',
};

/** @type {any} */
const REPORT_META = new Set([
    'depth',
    '_id',
    '_path',
    '_levelLabel',
    '_levelField',
    '_pathFields',
    '_has_children',
    '_is_expanded',
    '__col_schema',
]);

const DEFAULT_SEGMENT = 'default';
const BRANCH_PREFIX = 'branch:';
/** @type {any[]} */
const BORDER_STYLES = ['', 'solid', 'dashed', 'dotted', 'double'];

/** @type {Record<string, any>} */
const DEFAULT_REPORT_FORMAT = {
    indent: '',
    bold: false,
    showSubtotal: true,
    rowColor: '',
    labelPrefix: '',
    labelSuffix: '',
    numberFormat: '',
    borderStyle: '',
    borderColor: '',
    borderWidth: '',
};

/** @param {any} [field] @returns {any} */
const createClause = (field = '') => ({ _id: uid(), field, operator: 'eq', value: '', values: [] });
/** @returns {any} */
const createCondition = () => ({ op: 'AND', clauses: [] });
/** @returns {any} */
const createBranch = () => ({ _id: uid(), label: '', condition: createCondition(), child: null });
/** @param {any} [format] @returns {any} */
const normalizeReportFormat = (format = {}) => {
    /** @type {any} */
    const source = format && typeof format === 'object' ? format : {};
    /** @type {any} */
    const indent = Number(source.indent);
    /** @type {any} */
    const borderWidth = Number(source.borderWidth);
    /** @type {any} */
    const borderStyle = typeof source.borderStyle === 'string' && BORDER_STYLES.includes(source.borderStyle)
        ? source.borderStyle
        : '';
    return {
        indent: Number.isFinite(indent) && indent >= 0 ? Math.floor(indent) : '',
        bold: source.bold === true,
        showSubtotal: source.showSubtotal === false ? false : true,
        rowColor: typeof source.rowColor === 'string' ? source.rowColor : '',
        labelPrefix: typeof source.labelPrefix === 'string' ? source.labelPrefix : '',
        labelSuffix: typeof source.labelSuffix === 'string' ? source.labelSuffix : '',
        numberFormat: typeof source.numberFormat === 'string' ? source.numberFormat : '',
        borderStyle,
        borderColor: typeof source.borderColor === 'string' ? source.borderColor : '',
        borderWidth: Number.isFinite(borderWidth) && borderWidth > 0 ? Math.floor(borderWidth) : '',
    };
};
/** @param {any} [field] @returns {any} */
const createNode = (field = '') => ({
    _id: uid(),
    field,
    label: '',
    topN: null,
    sortBy: null,
    sortDir: 'desc',
    sortAbs: false,
    filters: null,
    format: normalizeReportFormat(),
    defaultChild: null,
    branches: [],
});

/** @param {any} [clause] @returns {any} */
const normalizeClause = (clause) => {
    if (!clause || typeof clause.field !== 'string' || !clause.field) return null;
    return {
        _id: typeof clause._id === 'string' ? clause._id : uid(),
        field: clause.field,
        operator: OPERATORS.some((operator) => operator.value === clause.operator) ? clause.operator : 'eq',
        value: clause.value !== undefined && clause.value !== null ? String(clause.value) : '',
        values: Array.isArray(clause.values) ? clause.values.map(String).filter(Boolean) : [],
    };
};

/** @param {any} [condition] @returns {any} */
const normalizeCondition = (condition) => ({
    op: condition && condition.op === 'OR' ? 'OR' : 'AND',
    clauses: Array.isArray(condition && condition.clauses)
        ? condition.clauses.map(normalizeClause).filter(Boolean)
        : [],
});

/** @param {any} [branch] @returns {any} */
const normalizeBranch = (branch) => {
    if (!branch || typeof branch !== 'object') return null;
    return {
        _id: typeof branch._id === 'string' ? branch._id : uid(),
        label: typeof branch.label === 'string' ? branch.label : '',
        condition: normalizeCondition(branch.condition),
        sourceRowPath: Array.isArray(branch.sourceRowPath) ? branch.sourceRowPath.map(String).filter(Boolean) : [],
        sourcePathFields: Array.isArray(branch.sourcePathFields) ? branch.sourcePathFields.map(String).filter(Boolean) : [],
        child: branch.child ? normalizeNode(branch.child) : null,
    };
};

/** @param {any} [node] @returns {any} */
function normalizeNode(node) {
    if (!node || typeof node !== 'object') return null;

    /** @type {any} */
    let branches = Array.isArray(node.branches) ? node.branches.map(normalizeBranch).filter(Boolean) : [];
    if (branches.length === 0 && node.childrenByValue && typeof node.childrenByValue === 'object') {
        branches = Object.entries(node.childrenByValue)
            .filter(([key]) => key)
            .map(([key, child]) => normalizeBranch({
                label: key,
                condition: { op: 'AND', clauses: [{ field: node.field || '', operator: 'eq', value: key, values: [] }] },
                child,
            }))
            .filter(Boolean);
    }

    /** @type {any} */
    const rawFilters = node.filters && typeof node.filters === 'object' ? normalizeCondition(node.filters) : null;
    return {
        _id: typeof node._id === 'string' ? node._id : uid(),
        field: typeof node.field === 'string' ? node.field : '',
        label: typeof node.label === 'string' ? node.label : '',
        topN: Number.isFinite(Number(node.topN)) && Number(node.topN) > 0 ? Math.floor(Number(node.topN)) : null,
        sortBy: typeof node.sortBy === 'string' && node.sortBy.trim() ? node.sortBy.trim() : null,
        sortDir: node.sortDir === 'asc' ? 'asc' : 'desc',
        sortAbs: node.sortAbs === true,
        filters: rawFilters && rawFilters.clauses.length > 0 ? rawFilters : null,
        format: normalizeReportFormat(node.format),
        defaultChild: node.defaultChild ? normalizeNode(node.defaultChild) : null,
        branches,
    };
}

/** @param {any} [index] @returns {any} */
const createBranchSegment = (index) => `${BRANCH_PREFIX}${index}`;

/** @param {any} [segment] @returns {any} */
const parseBranchIndex = (segment) => {
    if (typeof segment !== 'string' || !segment.startsWith(BRANCH_PREFIX)) return -1;
    /** @type {any} */
    const value = parseInt(segment.slice(BRANCH_PREFIX.length), 10);
    return Number.isFinite(value) ? value : -1;
};

/** @param {any} [availableFields] @param {any} [valConfigs] @returns {any} */
const getSelectableFields = (availableFields, valConfigs) => {
    /** @type {any} */
    const aggIds = new Set(
        (valConfigs || [])
            .filter(/** @param {any} [config] @returns {any} */ (config) => config && config.field && config.agg)
            .map(/** @param {any} [config] @returns {any} */ (config) => (config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`))
    );

    return (availableFields || []).filter(/** @param {any} [field] @returns {any} */ (field) => (
        field &&
        typeof field === 'string' &&
        !REPORT_META.has(field) &&
        (!field.startsWith('_') || isCustomCategoryField(field)) &&
        !aggIds.has(field)
    ));
};

/** @param {any} [valConfigs] @returns {any} */
const buildSortOptions = (valConfigs) => {
    /** @type {any[]} */
    const options = [
        { value: '', label: '(default)' },
        { value: '__field__', label: 'Field value (A→Z / Z→A)' },
    ];
    (valConfigs || []).forEach(/** @param {any} [config] @returns {any} */ (config) => {
        if (!config || !config.field || !config.agg) return;
        /** @type {any} */
        const value = config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`;
        /** @type {any} */
        const label = config.agg === 'formula'
            ? (config.label || formatDisplayLabel(config.field))
            : `${formatDisplayLabel(config.field)} (${formatAggLabel(config.agg, config.weightField)})`;
        options.push({ value, label });
    });
    return options;
};

/** @param {any} [condition] @returns {any} */
const formatConditionSummary = (condition) => {
    if (!condition || !condition.clauses || condition.clauses.length === 0) return 'No filter';

    return condition.clauses.map(/** @param {any} [clause] @returns {any} */ (clause) => {
        /** @type {any} */
        const field = formatDisplayLabel(clause.field);
        /** @type {any} */
        const operator = OP_SYMBOL[clause.operator] || clause.operator;
        if (LIST_OPS.has(clause.operator)) {
            /** @type {any} */
            const shown = clause.values.slice(0, 3);
            /** @type {any} */
            const extra = clause.values.length > 3 ? ` +${clause.values.length - 3}` : '';
            return `${field} ${operator} [${shown.join(', ')}${extra}]`;
        }
        return `${field} ${operator} ${clause.value || '...'}`;
    }).join(` ${condition.op} `);
};

/** @param {any} [value] @param {any} [sortByOptions] @returns {any} */
const getSortOptionLabel = (value, sortByOptions) => {
    if (!value) return '';
    /** @type {any} */
    const option = (sortByOptions || []).find(/** @param {any} [item] @returns {any} */ (item) => item.value === value);
    return option ? option.label : formatDisplayLabel(value);
};

/** @param {any} [node] @returns {any} */
const getNodeHeaderLabel = (node) => {
    if (!node) return 'Select field';
    return node.label && node.label.trim() ? node.label.trim() : (node.field ? formatDisplayLabel(node.field) : 'Select field');
};

/** @param {any} [clause] @returns {any} */
const isClauseComplete = (clause) => {
    if (!clause || !clause.field) return false;
    if (LIST_OPS.has(clause.operator)) return Array.isArray(clause.values) && clause.values.length > 0;
    return clause.value !== undefined && clause.value !== null && String(clause.value).trim() !== '';
};

/** @param {any} [condition] @returns {any} */
const conditionHasCompleteClauses = (condition) => (
    Boolean(condition && Array.isArray(condition.clauses) && condition.clauses.length > 0)
    && condition.clauses.every(isClauseComplete)
);

/** @param {any} [clause] @param {any} [row] @returns {any} */
const evaluateConditionClause = (clause, row) => {
    if (!clause || !row || typeof row !== 'object') return false;
    const rawValue = row[clause.field];
    /** @type {any} */
    const value = rawValue === undefined || rawValue === null ? '' : String(rawValue);
    /** @type {any} */
    const clauseValue = clause.value === undefined || clause.value === null ? '' : String(clause.value);
    /** @type {any} */
    const values = Array.isArray(clause.values) ? clause.values.map(String) : [];
    /** @type {any} */
    const numberValue = Number(value);
    /** @type {any} */
    const numberClauseValue = Number(clauseValue);

    switch (clause.operator) {
    case 'not_eq':
        return value !== clauseValue;
    case 'in':
        return values.includes(value);
    case 'not_in':
        return !values.includes(value);
    case 'contains':
        return value.toLowerCase().includes(clauseValue.toLowerCase());
    case 'not_contains':
        return !value.toLowerCase().includes(clauseValue.toLowerCase());
    case 'gt':
        return Number.isFinite(numberValue) && Number.isFinite(numberClauseValue) && numberValue > numberClauseValue;
    case 'gte':
        return Number.isFinite(numberValue) && Number.isFinite(numberClauseValue) && numberValue >= numberClauseValue;
    case 'lt':
        return Number.isFinite(numberValue) && Number.isFinite(numberClauseValue) && numberValue < numberClauseValue;
    case 'lte':
        return Number.isFinite(numberValue) && Number.isFinite(numberClauseValue) && numberValue <= numberClauseValue;
    case 'eq':
    default:
        return value === clauseValue;
    }
};

/** @param {any} [condition] @param {any} [data] @returns {any} */
const conditionMatchesAnyRow = (condition, data) => {
    if (!conditionHasCompleteClauses(condition) || !Array.isArray(data) || data.length === 0) return true;
    /** @type {any} */
    const usableRows = data.filter((row) => row && typeof row === 'object' && !row._isTotal && !row._isOther);
    /** @type {any} */
    const conditionFieldsAreLoaded = usableRows.some(/** @param {any} [row] @returns {any} */ (row) => (
        condition.clauses.every(/** @param {any} [clause] @returns {any} */ (clause) => Object.prototype.hasOwnProperty.call(row, clause.field))
    ));
    if (!conditionFieldsAreLoaded) return true;
    return usableRows.some(/** @param {any} [row] @returns {any} */ (row) => {
        if (condition.op === 'OR') return condition.clauses.some(/** @param {any} [clause] @returns {any} */ (clause) => evaluateConditionClause(clause, row));
        return condition.clauses.every(/** @param {any} [clause] @returns {any} */ (clause) => evaluateConditionClause(clause, row));
    });
};

/** @param {any} [sourceRowPath] @param {any} [data] @returns {any} */
const rowPathExistsInData = (sourceRowPath, data) => {
    if (!Array.isArray(sourceRowPath) || sourceRowPath.length === 0 || !Array.isArray(data) || data.length === 0) return true;
    /** @type {any} */
    const serialized = sourceRowPath.map(String).join('|||');
    return data.some((row) => row && typeof row === 'object' && row._path === serialized);
};

/** @param {any} [root] @param {any} [p1] @returns {any} */
const validateReportDefinition = (root, {
    allFields = [],
    valConfigs = [],
    data = [],
} = {}) => {
    /** @type {any[]} */
    const errors = [];
    /** @type {any[]} */
    const warnings = [];
    /** @type {any} */
    const hasMeasure = (valConfigs || []).some(/** @param {any} [config] @returns {any} */ (config) => config && config.field && config.agg);
    /** @type {any} */
    const knownFields = new Set(Array.isArray(allFields) ? allFields : []);
    /** @type {any} */
    const headerMap = new Map();
    /** @type {any} */
    const reportRows = Array.isArray(data) ? data : [];

    /** @param {any} [header] @param {any} [pathLabel] @returns {any} */
    const pushDuplicateHeader = (header, pathLabel) => {
        /** @type {any} */
        const key = header.trim().toLowerCase();
        if (!key) return;
        if (headerMap.has(key)) {
            errors.push(`Duplicate header "${header}" at ${pathLabel}; already used at ${headerMap.get(key)}.`);
            return;
        }
        headerMap.set(key, pathLabel);
    };

    /** @param {any} [node] @param {any} [pathLabel] @param {any} [stack] @returns {any} */
    const validateNode = (node, pathLabel, stack = new Set()) => {
        if (!node) return;
        /** @type {any} */
        const nodeId = node._id || `${pathLabel}:${node.field || ''}`;
        if (stack.has(nodeId)) {
            errors.push(`Circular row override detected at ${pathLabel}.`);
            return;
        }
        /** @type {any} */
        const nextStack = new Set(stack);
        nextStack.add(nodeId);

        if (!node.field) {
            errors.push(`Missing field at ${pathLabel}.`);
        } else if (knownFields.size > 0 && !knownFields.has(node.field)) {
            errors.push(`Field "${formatDisplayLabel(node.field)}" at ${pathLabel} is no longer available.`);
        }

        pushDuplicateHeader(getNodeHeaderLabel(node), pathLabel);

        if (node.topN && !hasMeasure) {
            errors.push(`Top-N at ${pathLabel} needs at least one measure selected.`);
        }

        if (node.filters && !conditionHasCompleteClauses(node.filters)) {
            errors.push(`Level filter at ${pathLabel} has incomplete rules.`);
        } else if (node.filters && !conditionMatchesAnyRow(node.filters, reportRows)) {
            errors.push(`Level filter at ${pathLabel} does not match any loaded rows.`);
        }

        if (node.defaultChild) validateNode(node.defaultChild, `${pathLabel} > Default children`, nextStack);

        (node.branches || []).forEach(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => {
            /** @type {any} */
            const branchLabel = branch.label && branch.label.trim() ? branch.label.trim() : `Row override ${index + 1}`;
            const branchPathLabel = `${pathLabel} > ${branchLabel}`;
            if (!conditionHasCompleteClauses(branch.condition)) {
                errors.push(`Row override "${branchLabel}" needs a complete row rule.`);
            } else if (!conditionMatchesAnyRow(branch.condition, reportRows)) {
                errors.push(`Row override "${branchLabel}" does not match any loaded rows.`);
            }
            if (
                Array.isArray(branch.sourcePathFields)
                && branch.sourcePathFields.length > 0
                && branch.sourcePathFields[branch.sourcePathFields.length - 1] !== node.field
            ) {
                errors.push(`Row override "${branchLabel}" was created for another level and no longer points to this row path.`);
            }
            if (!rowPathExistsInData(branch.sourceRowPath, reportRows)) {
                warnings.push(`Row override "${branchLabel}" points to a row path that is not in the current loaded rows.`);
            }
            if (branch.child) validateNode(branch.child, `${branchPathLabel} > Custom children`, nextStack);
            if (!branch.child) {
                warnings.push(`Row override "${branchLabel}" has no Custom children; it only renames the matching row.`);
            }
        });
    };

    if (!root) {
        errors.push('Report needs at least Level 1.');
    } else {
        validateNode(root, 'Level 1');
    }

    return { errors, warnings, valid: errors.length === 0 };
};

/** @param {any} [item] @returns {any} */
const getNodeTitle = (item) => {
    if (!item || !item.branchBadge) return `Level ${item ? item.levelNumber : 1}`;
    if (item.branchDepth === 0) return `Override children ${item.branchBadge}`;
    return `Override child level ${item.branchBadge}.${item.branchDepth + 1}`;
};

/** @param {any} [root] @param {any} [path] @returns {any} */
const getNodeAtPath = (root, path) => {
    let current = root;
    for (let index = 0; current && index < path.length; index += 1) {
        const segment = path[index];
        if (segment === DEFAULT_SEGMENT) {
            current = current.defaultChild || null;
        } else {
            /** @type {any} */
            const branchIndex = parseBranchIndex(segment);
            current = branchIndex >= 0 && current.branches[branchIndex] ? current.branches[branchIndex].child : null;
        }
    }
    return current || null;
};

/** @param {any} [root] @param {any} [nodePath] @param {any} [branchIndex] @returns {any} */
const getBranchAtPath = (root, nodePath, branchIndex) => {
    /** @type {any} */
    const node = getNodeAtPath(root, nodePath);
    return node && node.branches[branchIndex] ? node.branches[branchIndex] : null;
};

/** @param {any} [root] @param {any} [path] @param {any} [updater] @returns {any} */
const updateNodeAtPath = (root, path, updater) => {
    if (!root) return root;

    /** @param {any} [current] @param {any} [depth] @returns {any} */
    const visit = (current, depth) => {
        if (!current) return current;
        if (depth >= path.length) return normalizeNode(updater(current));

        const segment = path[depth];
        if (segment === DEFAULT_SEGMENT) {
            return normalizeNode({
                ...current,
                defaultChild: visit(current.defaultChild, depth + 1),
            });
        }

        /** @type {any} */
        const branchIndex = parseBranchIndex(segment);
        if (branchIndex < 0 || branchIndex >= current.branches.length) return current;

        return normalizeNode({
            ...current,
            branches: current.branches.map(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => (
                index === branchIndex
                    ? { ...branch, child: visit(branch.child, depth + 1) }
                    : branch
            )),
        });
    };

    return visit(root, 0);
};

/** @param {any} [root] @param {any} [nodePath] @param {any} [branchIndex] @param {any} [updater] @returns {any} */
const updateBranchAtPath = (root, nodePath, branchIndex, updater) => {
    if (!root) return root;
    return updateNodeAtPath(root, nodePath, /** @param {any} [node] @returns {any} */ (node) => {
        if (branchIndex < 0 || branchIndex >= node.branches.length) return node;
        return {
            ...node,
            branches: node.branches.map(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => (
                index === branchIndex ? normalizeBranch(updater(branch)) : branch
            )),
        };
    });
};

/** @param {any} [root] @returns {any} */
const buildOutlineItems = (root) => {
    if (!root) return [];

    /** @type {any[]} */
    const items = [];
    /** @param {any} [node] @param {any} [meta] @returns {any} */
    const visitNode = (node, meta) => {
        if (!node) return;

        items.push({
            kind: 'node',
            node,
            nodePath: meta.nodePath,
            depth: meta.depth,
            levelNumber: meta.levelNumber,
            branchBadge: meta.branchBadge,
            branchDepth: meta.branchDepth,
        });

        node.branches.forEach(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => {
            const badge = `${meta.levelNumber}.${index + 1}`;
            items.push({
                kind: 'branch',
                branch,
                nodePath: meta.nodePath,
                branchIndex: index,
                depth: meta.depth + 1,
                badge,
            });

            if (branch.child) {
                visitNode(branch.child, {
                    nodePath: [...meta.nodePath, createBranchSegment(index)],
                    depth: meta.depth + 2,
                    levelNumber: meta.levelNumber + 1,
                    branchBadge: badge,
                    branchDepth: 0,
                });
            }
        });

        if (node.defaultChild) {
            visitNode(node.defaultChild, {
                nodePath: [...meta.nodePath, DEFAULT_SEGMENT],
                depth: meta.depth + 1,
                levelNumber: meta.levelNumber + 1,
                branchBadge: meta.branchBadge,
                branchDepth: meta.branchBadge ? meta.branchDepth + 1 : 0,
            });
        }
    };

    visitNode(root, {
        nodePath: [],
        depth: 0,
        levelNumber: 1,
        branchBadge: null,
        branchDepth: 0,
    });

    return items;
};

/** @param {any} [path] @returns {any} */
const pathKey = (path = []) => path.join('>');

/** @param {any} [left] @param {any} [right] @returns {any} */
const isSamePath = (left = [], right = []) => pathKey(left) === pathKey(right);

/** @param {any} [value] @returns {any} */
const normalizeRowPathValue = (value) => (
    value === undefined || value === null ? '' : String(value)
);

/** @param {any} [selection] @returns {any} */
const selectionKey = (selection) => {
    if (!selection) return '';
    if (selection.type === 'node') return `node:${pathKey(selection.path)}`;
    if (selection.type === 'branch') return `branch:${pathKey(selection.nodePath)}:${selection.branchIndex}`;
    if (selection.type === 'rowPath') return `rowPath:${pathKey(selection.nodePath)}:${selection.field}:${normalizeRowPathValue(selection.value)}`;
    return '';
};

/** @param {any} [items] @param {any} [path] @returns {any} */
const findNodeItem = (items, path) => (
    (items || []).find(/** @param {any} [item] @returns {any} */ (item) => item.kind === 'node' && isSamePath(item.nodePath, path)) || null
);

/** @param {any} [items] @param {any} [nodePath] @param {any} [branchIndex] @returns {any} */
const findBranchItem = (items, nodePath, branchIndex) => (
    (items || []).find(/** @param {any} [item] @returns {any} */ (item) => (
        item.kind === 'branch' &&
        isSamePath(item.nodePath, nodePath) &&
        item.branchIndex === branchIndex
    )) || null
);

/** @param {any} [branch] @param {any} [field] @param {any} [value] @returns {any} */
const branchMatchesRowPath = (branch, field, value) => {
    if (!branch || !field) return false;
    /** @type {any} */
    const expected = normalizeRowPathValue(value);
    /** @type {any} */
    const clauses = branch.condition && Array.isArray(branch.condition.clauses)
        ? branch.condition.clauses
        : [];
    return clauses.some(/** @param {any} [clause] @returns {any} */ (clause) => {
        if (!clause || clause.field !== field) return false;
        if (clause.operator === 'eq' || clause.operator === '=') {
            return normalizeRowPathValue(clause.value) === expected;
        }
        if (clause.operator === 'in') {
            return Array.isArray(clause.values) && clause.values.map(normalizeRowPathValue).includes(expected);
        }
        return false;
    });
};

/** @param {any} [field] @param {any} [value] @param {any} [label] @param {any} [childField] @returns {any} */
const createRowPathBranch = (field, value, label, childField = '') => ({
    ...createBranch(),
    label: normalizeRowPathValue(label || value),
    condition: {
        op: 'AND',
        clauses: [{
            ...createClause(field),
            operator: 'eq',
            value: normalizeRowPathValue(value),
            values: [],
        }],
    },
    child: createNode(childField),
});

/** @param {any} [node] @returns {any} */
const cloneNode = (node) => {
    if (!node) return null;
    try {
        return normalizeNode(JSON.parse(JSON.stringify(node)));
    } catch (error) {
        return normalizeNode(node);
    }
};

/** @param {any} [index] @returns {any} */
const levelPathForIndex = (index) => (
    Array.from({ length: Math.max(0, index) }, () => DEFAULT_SEGMENT)
);

/** @param {any} [root] @param {any} [startLevelNumber] @param {any} [pathPrefix] @returns {any} */
const buildDefaultLevelItems = (root, startLevelNumber = 1, pathPrefix = []) => {
    /** @type {any[]} */
    const items = [];
    let current = root;
    let index = 0;
    /** @type {any} */
    let currentPath = Array.isArray(pathPrefix) ? [...pathPrefix] : [];
    while (current) {
        items.push({
            node: current,
            path: [...currentPath],
            index,
            levelNumber: startLevelNumber + index,
        });
        current = current.defaultChild || null;
        currentPath = [...currentPath, DEFAULT_SEGMENT];
        index += 1;
    }
    return items;
};

/** @param {any} [node] @returns {any} */
const stripDefaultChild = (node) => (
    node ? { ...cloneNode(node), defaultChild: null } : null
);

/** @param {any} [nodes] @returns {any} */
const rebuildDefaultChain = (nodes) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return null;
    /** @type {any} */
    let child = null;
    for (let index = nodes.length - 1; index >= 0; index -= 1) {
        child = normalizeNode({
            ...stripDefaultChild(nodes[index]),
            defaultChild: child,
        });
    }
    return child;
};

/** @param {any} [root] @param {any} [fromIndex] @param {any} [toIndex] @returns {any} */
const reorderDefaultChain = (root, fromIndex, toIndex) => {
    /** @type {any} */
    const chain = buildDefaultLevelItems(root).map(/** @param {any} [item] @returns {any} */ (item) => item.node);
    if (
        fromIndex < 0 ||
        toIndex < 0 ||
        fromIndex >= chain.length ||
        toIndex >= chain.length ||
        fromIndex === toIndex
    ) {
        return root;
    }
    /** @type {any[]} */
    const next = [...chain];
    /** @type {any} */
    const [moved] = next.splice(fromIndex, 1);
    next.splice(toIndex, 0, moved);
    return rebuildDefaultChain(next);
};

/** @param {any} [node] @returns {any} */
const isUsableNode = (node) => Boolean(node && node.field);

/** @param {any} [node] @returns {any} */
const pruneDraftChild = (node) => {
    /** @type {any} */
    const normalized = cloneNode(node);
    if (!normalized || !normalized.field) return null;
    return normalized;
};

/** @param {any} [items] @param {any} [selection] @returns {any} */
const findSelectionItem = (items, selection) => {
    if (!selection) return null;
    if (selection.type === 'node') return findNodeItem(items, selection.path);
    if (selection.type === 'branch') return findBranchItem(items, selection.nodePath, selection.branchIndex);
    return null;
};

/** @param {any} [items] @param {any} [selection] @returns {any} */
const buildTrailItems = (items, selection) => {
    if (!selection) return [];

    /** @type {any[]} */
    const trail = [];
    /** @param {any} [path] @returns {any} */
    const pushNode = (path) => {
        /** @type {any} */
        const item = findNodeItem(items, path);
        if (item) trail.push(item);
    };
    /** @param {any} [nodePath] @param {any} [branchIndex] @returns {any} */
    const pushBranch = (nodePath, branchIndex) => {
        /** @type {any} */
        const item = findBranchItem(items, nodePath, branchIndex);
        if (item) trail.push(item);
    };

    /** @param {any} [nodePath] @returns {any} */
    const walkNodePath = (nodePath) => {
        pushNode([]);
        if (!Array.isArray(nodePath) || nodePath.length === 0) return;

        /** @type {any[]} */
        let currentPath = [];
        nodePath.forEach((segment) => {
            if (segment === DEFAULT_SEGMENT) {
                currentPath = [...currentPath, segment];
                pushNode(currentPath);
                return;
            }

            /** @type {any} */
            const branchIndex = parseBranchIndex(segment);
            if (branchIndex >= 0) {
                pushBranch(currentPath, branchIndex);
                currentPath = [...currentPath, segment];
                pushNode(currentPath);
            }
        });
    };

    if (selection.type === 'node') {
        walkNodePath(selection.path);
        return trail;
    }

    walkNodePath(selection.nodePath);
    pushBranch(selection.nodePath, selection.branchIndex);
    return trail;
};

/** @param {any} [items] @param {any} [selection] @returns {any} */
const buildDirectChildItems = (items, selection) => {
    if (!selection) return [];

    if (selection.type === 'node') {
        /** @type {any[]} */
        const children = [];
        /** @type {any} */
        const defaultChild = findNodeItem(items, [...selection.path, DEFAULT_SEGMENT]);
        if (defaultChild) children.push(defaultChild);

        /** @type {any} */
        const branchChildren = (items || [])
            .filter(/** @param {any} [item] @returns {any} */ (item) => item.kind === 'branch' && isSamePath(item.nodePath, selection.path))
            .sort(/** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => left.branchIndex - right.branchIndex);

        return [...children, ...branchChildren];
    }

    /** @type {any} */
    const customChildren = findNodeItem(items, [...selection.nodePath, createBranchSegment(selection.branchIndex)]);
    return customChildren ? [customChildren] : [];
};

/** @param {any} [theme] @param {any} [extra] @returns {any} */
const inp = (theme, extra = {}) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: '10px',
    padding: '8px 10px',
    fontSize: '12px',
    lineHeight: 1.4,
    background: theme.background || '#fff',
    color: theme.text,
    outline: 'none',
    width: '100%',
    ...extra,
});

/** @param {any} [theme] @param {any} [active] @param {any} [danger] @returns {any} */
const ghostBtn = (theme, active = false, danger = false) => ({
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '6px',
    background: danger ? '#FEF2F2' : active ? `${theme.primary || '#2563EB'}12` : (theme.hover || '#F9FAFB'),
    border: `1px solid ${danger ? '#FECACA' : active ? `${theme.primary || '#2563EB'}45` : theme.border}`,
    borderRadius: '9px',
    padding: '7px 11px',
    fontSize: '11px',
    fontWeight: 700,
    color: danger ? '#B91C1C' : active ? (theme.primary || '#2563EB') : theme.textSec,
    cursor: 'pointer',
});

/** @param {any} [theme] @returns {any} */
const sectionLabelStyle = (theme) => ({
    fontSize: '9px',
    fontWeight: 800,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    color: theme.textSec,
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    marginBottom: '10px',
});

/** @param {any} [theme] @returns {any} */
const panelCardStyle = (theme) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: '14px',
    background: theme.surfaceBg || theme.background || '#fff',
    padding: '14px',
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
});

/** @param {any} [theme] @returns {any} */
const reportSectionStyle = (theme) => ({
    borderTop: `2px solid ${theme.textSec || '#334155'}`,
    paddingTop: '14px',
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
});

/** @param {any} [theme] @param {any} [selected] @param {any} [depth] @returns {any} */
const outlineItemStyle = (theme, selected, depth) => ({
    width: `calc(100% - ${depth * 14}px)`,
    marginLeft: `${depth * 14}px`,
    border: `1px solid ${selected ? (theme.primary || '#2563EB') : theme.border}`,
    borderRadius: '12px',
    background: selected ? (theme.select || `${theme.primary || '#2563EB'}14`) : (theme.surfaceBg || theme.background || '#fff'),
    padding: '12px',
    textAlign: 'left',
    cursor: 'pointer',
    display: 'flex',
    flexDirection: 'column',
    gap: '7px',
});

/** @param {any} p0 @returns {any} */
function SummaryChip({ children, theme, tone = 'neutral' }) {
    /** @type {any} */
    const palette = tone === 'primary'
        ? { background: `${theme.primary || '#2563EB'}12`, border: `${theme.primary || '#2563EB'}30`, color: theme.primary || '#2563EB' }
        : { background: theme.hover || '#F3F4F6', border: theme.border, color: theme.textSec };

    return (
        <span
            style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '4px',
                padding: '3px 7px',
                borderRadius: '999px',
                fontSize: '10px',
                fontWeight: 700,
                border: `1px solid ${palette.border}`,
                background: palette.background,
                color: palette.color,
            }}
        >
            {children}
        </span>
    );
}

/** @param {any} p0 @returns {any} */
function ReportSection({
    theme,
    title,
    description,
    icon,
    action = null,
    sectionId,
    children,
}) {
    return (
        <section data-report-section={sectionId || title} style={reportSectionStyle(theme)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '8px', minWidth: 0 }}>
                    <div style={{ color: theme.textSec, display: 'flex', alignItems: 'center', paddingTop: '1px', flexShrink: 0 }}>
                        {icon}
                    </div>
                    <div style={{ minWidth: 0 }}>
                        <div style={{ fontSize: '13px', fontWeight: 900, color: theme.text }}>
                            {title}
                        </div>
                        {description && (
                            <div style={{ fontSize: '11px', color: theme.textSec, lineHeight: 1.45, marginTop: '2px' }}>
                                {description}
                            </div>
                        )}
                    </div>
                </div>
                {action && <div style={{ flexShrink: 0 }}>{action}</div>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {children}
            </div>
        </section>
    );
}

/** @param {any} p0 @returns {any} */
function ConditionModal({ condition, allFields, getFieldLabel = formatDisplayLabel, onSave, onClose, theme, title, description }) {
    /** @type {[any, (...args: any[]) => any]} */
    const [local, setLocal] = useState(() => normalizeCondition(condition));
    /** @type {any} */
    const inputStyle = inp(theme);

    /** @type {(...args: any[]) => any} */
    const addClause = useCallback(() => {
        setLocal(/** @param {any} [prev] @returns {any} */ (prev) => ({ ...prev, clauses: [...prev.clauses, createClause(allFields[0] || '')] }));
    }, [allFields]);

    /** @type {(...args: any[]) => any} */
    const updateClause = useCallback((index, patch) => {
        setLocal(/** @param {any} [prev] @returns {any} */ (prev) => ({
            ...prev,
            clauses: prev.clauses.map(/** @param {any} [clause] @param {any} [clauseIndex] @returns {any} */ (clause, clauseIndex) => (clauseIndex === index ? { ...clause, ...patch } : clause)),
        }));
    }, []);

    /** @type {(...args: any[]) => any} */
    const removeClause = useCallback((index) => {
        setLocal(/** @param {any} [prev] @returns {any} */ (prev) => ({
            ...prev,
            clauses: prev.clauses.filter(/** @param {any} [_] @param {any} [clauseIndex] @returns {any} */ (_, clauseIndex) => clauseIndex !== index),
        }));
    }, []);

    return (
        <div
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 9999,
                background: 'rgba(15, 23, 42, 0.45)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
            }}
            onClick={onClose}
        >
            <div
                style={{
                    ...panelCardStyle(theme),
                    width: '520px',
                    maxWidth: '95vw',
                    maxHeight: '85vh',
                    overflowY: 'auto',
                    boxShadow: '0 24px 64px rgba(15, 23, 42, 0.22)',
                }}
                onClick={(event) => event.stopPropagation()}
            >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '10px' }}>
                    <div>
                        <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{title || 'Edit row override rule'}</div>
                        <div style={{ fontSize: '12px', color: theme.textSec, marginTop: '2px' }}>
                            {description || 'Add one or more clauses to decide when this row override should be used.'}
                        </div>
                    </div>
                    <button type="button" onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textSec }}>
                        <Icons.Close />
                    </button>
                </div>

                {local.clauses.length > 1 && (
                    <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
                        <span style={{ fontSize: '11px', color: theme.textSec, fontWeight: 700 }}>Match:</span>
                        {['AND', 'OR'].map((operator) => (
                            <button
                                key={operator}
                                type="button"
                                onClick={() => setLocal(/** @param {any} [prev] @returns {any} */ (prev) => ({ ...prev, op: operator }))}
                                style={{
                                    padding: '5px 12px',
                                    borderRadius: '999px',
                                    fontSize: '11px',
                                    fontWeight: 800,
                                    border: 'none',
                                    cursor: 'pointer',
                                    background: local.op === operator ? (theme.primary || '#2563EB') : (theme.hover || '#F3F4F6'),
                                    color: local.op === operator ? '#fff' : theme.textSec,
                                }}
                            >
                                {operator}
                            </button>
                        ))}
                    </div>
                )}

                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {local.clauses.length === 0 && (
                        <div style={{ fontSize: '12px', color: theme.textSec, fontStyle: 'italic' }}>
                            No clauses means this row override matches every row at this level.
                        </div>
                    )}

                    {local.clauses.map(/** @param {any} [clause] @param {any} [index] @returns {any} */ (clause, index) => (
                        <div
                            key={clause._id}
                            style={{
                                border: `1px solid ${theme.border}`,
                                borderRadius: '12px',
                                background: theme.hover || '#F8FAFC',
                                padding: '10px',
                                display: 'flex',
                                gap: '8px',
                                alignItems: 'flex-start',
                            }}
                        >
                            <div style={{ width: '32px', paddingTop: '6px', textAlign: 'center', fontSize: '10px', fontWeight: 800, color: theme.textSec }}>
                                {index > 0 ? local.op : ''}
                            </div>

                            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '8px' }}>
                                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                                    <select
                                        value={clause.field}
                                        onChange={(event) => updateClause(index, { field: event.target.value })}
                                        style={{ ...inputStyle, flex: 1, minWidth: '140px' }}
                                    >
                                        {allFields.map(/** @param {any} [field] @returns {any} */ (field) => (
                                            <option key={field} value={field}>{getFieldLabel(field)}</option>
                                        ))}
                                    </select>
                                    <select
                                        value={clause.operator}
                                        onChange={(event) => {
                                            const next = event.target.value;
                                            /** @type {any} */
                                            const nextIsList = LIST_OPS.has(next);
                                            /** @type {any} */
                                            const prevIsList = LIST_OPS.has(clause.operator);
                                            updateClause(index, {
                                                operator: next,
                                                values: nextIsList
                                                    ? (prevIsList ? clause.values : (clause.value ? [clause.value] : []))
                                                    : clause.values,
                                                value: nextIsList
                                                    ? clause.value
                                                    : (clause.values[0] || clause.value || ''),
                                            });
                                        }}
                                        style={{ ...inputStyle, width: '160px', flexShrink: 0 }}
                                    >
                                        {OPERATORS.map((operator) => (
                                            <option key={operator.value} value={operator.value}>{operator.label}</option>
                                        ))}
                                    </select>
                                </div>

                                {LIST_OPS.has(clause.operator) ? (
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                                        {clause.values.length > 0 && (
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                                                {clause.values.map(/** @param {any} [value] @param {any} [valueIndex] @returns {any} */ (value, valueIndex) => (
                                                    <SummaryChip key={`${clause._id}-${valueIndex}`} theme={theme} tone="primary">
                                                        <span>{value}</span>
                                                        <button
                                                            type="button"
                                                            onClick={() => updateClause(index, {
                                                                values: clause.values.filter(/** @param {any} [_] @param {any} [currentIndex] @returns {any} */ (_, currentIndex) => currentIndex !== valueIndex),
                                                            })}
                                                            style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'inherit', lineHeight: 1 }}
                                                        >
                                                            x
                                                        </button>
                                                    </SummaryChip>
                                                ))}
                                            </div>
                                        )}
                                        <input
                                            placeholder="Type a value and press Enter"
                                            style={inputStyle}
                                            onKeyDown={/** @param {any} event */ (event) => {
                                                /** @type {any} */
                                                const value = event.target.value.trim();
                                                if (event.key === 'Enter' && value) {
                                                    if (!clause.values.includes(value)) {
                                                        updateClause(index, { values: [...clause.values, value] });
                                                    }
                                                    event.target.value = '';
                                                    event.preventDefault();
                                                }
                                            }}
                                        />
                                    </div>
                                ) : (
                                    <input
                                        value={clause.value}
                                        onChange={(event) => updateClause(index, { value: event.target.value })}
                                        placeholder="Value"
                                        style={inputStyle}
                                    />
                                )}
                            </div>

                            <button
                                type="button"
                                onClick={() => removeClause(index)}
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#B91C1C', padding: '6px' }}
                            >
                                <Icons.Delete />
                            </button>
                        </div>
                    ))}
                </div>

                <button
                    type="button"
                    onClick={addClause}
                    style={{
                        ...ghostBtn(theme),
                        width: '100%',
                        borderStyle: 'dashed',
                    }}
                >
                    <Icons.Add /> Add clause
                </button>

                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', borderTop: `1px solid ${theme.border}`, paddingTop: '12px' }}>
                    <button type="button" onClick={onClose} style={ghostBtn(theme)}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        onClick={() => {
                            onSave(local);
                            onClose();
                        }}
                        style={{
                            background: theme.primary || '#2563EB',
                            border: 'none',
                            borderRadius: '9px',
                            padding: '8px 14px',
                            fontSize: '12px',
                            fontWeight: 800,
                            color: '#fff',
                            cursor: 'pointer',
                        }}
                    >
                        Apply
                    </button>
                </div>
            </div>
        </div>
    );
}

/** @param {any} p0 @returns {any} */
function ReportFormatEditor({ value, onChange, theme }) {
    /** @type {any} */
    const format = normalizeReportFormat(value);
    /** @type {any} */
    const inputStyle = inp(theme);
    /** @param {any} [patch] @returns {any} */
    const updateFormat = (patch) => onChange(normalizeReportFormat({ ...format, ...patch }));

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div style={sectionLabelStyle(theme)}>Presentation</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Indent px
                    <input type="number" min="0" value={format.indent} onChange={(event) => updateFormat({ indent: event.target.value })} placeholder="Auto" style={inputStyle} />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Row color
                    <input value={format.rowColor} onChange={(event) => updateFormat({ rowColor: event.target.value })} placeholder="#F8FAFC" style={inputStyle} />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Label prefix
                    <input value={format.labelPrefix} onChange={(event) => updateFormat({ labelPrefix: event.target.value })} placeholder="Optional" style={inputStyle} />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Label suffix
                    <input value={format.labelSuffix} onChange={(event) => updateFormat({ labelSuffix: event.target.value })} placeholder="Optional" style={inputStyle} />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec, gridColumn: '1 / -1' }}>
                    Number format override
                    <input value={format.numberFormat} onChange={(event) => updateFormat({ numberFormat: event.target.value })} placeholder="fixed:2, percent, currency:$" style={inputStyle} />
                </label>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px' }}>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Border
                    <select value={format.borderStyle} onChange={(event) => updateFormat({ borderStyle: event.target.value })} style={inputStyle}>
                        {BORDER_STYLES.map((style) => (
                            <option key={style || 'none'} value={style}>{style || 'None'}</option>
                        ))}
                    </select>
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Border color
                    <input value={format.borderColor} onChange={(event) => updateFormat({ borderColor: event.target.value })} placeholder="#CBD5E1" style={inputStyle} />
                </label>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec }}>
                    Width
                    <input type="number" min="1" value={format.borderWidth} onChange={(event) => updateFormat({ borderWidth: event.target.value })} placeholder="1" style={inputStyle} />
                </label>
            </div>
            <div style={{ display: 'flex', gap: '14px', flexWrap: 'wrap' }}>
                <label style={{ display: 'inline-flex', alignItems: 'center', gap: '7px', fontSize: '12px', color: theme.text, fontWeight: 700 }}>
                    <input type="checkbox" checked={format.bold} onChange={(event) => updateFormat({ bold: event.target.checked })} />
                    Bold row
                </label>
                <label style={{ display: 'inline-flex', alignItems: 'center', gap: '7px', fontSize: '12px', color: theme.text, fontWeight: 700 }}>
                    <input type="checkbox" checked={format.showSubtotal} onChange={(event) => updateFormat({ showSubtotal: event.target.checked })} />
                    Show subtotal/other rows
                </label>
            </div>
        </div>
    );
}

/** @param {any} p0 @returns {any} */
function ValidationPanel({ validation, theme }) {
    /** @type {any} */
    const errors = validation && Array.isArray(validation.errors) ? validation.errors : [];
    /** @type {any} */
    const warnings = validation && Array.isArray(validation.warnings) ? validation.warnings : [];
    if (errors.length === 0 && warnings.length === 0) return null;

    return (
        <div data-report-validation="true" style={{ border: `1px solid ${errors.length > 0 ? '#FCA5A5' : '#FDE68A'}`, borderRadius: '12px', padding: '10px 12px', background: errors.length > 0 ? '#FEF2F2' : '#FFFBEB', color: errors.length > 0 ? '#991B1B' : '#92400E', fontSize: '11px', lineHeight: 1.45, display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {errors.length > 0 && (
                <div>
                    <div style={{ fontWeight: 900, marginBottom: '4px' }}>Fix before applying or saving</div>
                    {errors.map(/** @param {any} [message] @returns {any} */ (message) => <div key={`err:${message}`}>{message}</div>)}
                </div>
            )}
            {warnings.length > 0 && (
                <div>
                    <div style={{ fontWeight: 900, marginBottom: '4px' }}>Warnings</div>
                    {warnings.map(/** @param {any} [message] @returns {any} */ (message) => <div key={`warn:${message}`}>{message}</div>)}
                </div>
            )}
        </div>
    );
}

/** @param {any} p0 @returns {any} */
function RowDebugInspector({ debug, theme }) {
    if (!debug || typeof debug !== 'object') return null;
    /** @type {any} */
    const details = [
        ['Matched level', debug.matchedLevel || debug.levelLabel || ''],
        ['Field', debug.levelField || ''],
        ['Row path', Array.isArray(debug.pathValues) ? debug.pathValues.join(' > ') : (debug.rowPath || '')],
        ['Children source', debug.childrenSource || 'Default children'],
        ['Rule', debug.matchedCondition || ''],
    ].filter(([, value]) => value);
    if (details.length === 0) return null;

    return (
        <div data-report-debug-inspector="true" style={{ border: `1px solid ${theme.border}`, borderRadius: '12px', padding: '10px', background: theme.hover || '#F8FAFC', display: 'flex', flexDirection: 'column', gap: '7px' }}>
            <div style={sectionLabelStyle(theme)}>Why Is This Row Here?</div>
            {details.map(/** @param {any} p0 @returns {any} */ ([label, value]) => (
                <div key={label} style={{ display: 'flex', justifyContent: 'space-between', gap: '10px', fontSize: '11px' }}>
                    <span style={{ color: theme.textSec, fontWeight: 800 }}>{label}</span>
                    <span style={{ color: theme.text, textAlign: 'right', overflowWrap: 'anywhere' }}>{value}</span>
                </div>
            ))}
        </div>
    );
}

/** @param {any} p0 @returns {any} */
function CollapsibleLevelSection({
    title,
    summary,
    theme,
    defaultOpen = false,
    children,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [open, setOpen] = useState(defaultOpen);

    return (
        <div
            data-report-level-collapsible="true"
            data-report-level-collapsed={open ? 'false' : 'true'}
            style={{
                border: `1px solid ${theme.border}`,
                borderRadius: '12px',
                background: theme.surfaceBg || theme.background || '#fff',
                overflow: 'hidden',
            }}
        >
            <button
                type="button"
                onClick={() => setOpen(/** @param {any} [current] @returns {any} */ (current) => !current)}
                style={{
                    width: '100%',
                    border: 'none',
                    background: open ? (theme.hover || '#F8FAFC') : 'transparent',
                    color: theme.text,
                    cursor: 'pointer',
                    padding: '10px 12px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    textAlign: 'left',
                }}
                aria-expanded={open}
            >
                <span style={{ display: 'inline-flex', color: theme.textSec, flexShrink: 0 }}>
                    {open ? <Icons.ChevronDown /> : <Icons.ChevronRight />}
                </span>
                <span style={{ flex: 1, minWidth: 0 }}>
                    <span style={{ display: 'block', fontSize: '12px', fontWeight: 900, color: theme.text }}>
                        {title}
                    </span>
                    {summary && (
                        <span style={{ display: 'block', fontSize: '11px', color: theme.textSec, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginTop: '2px' }}>
                            {summary}
                        </span>
                    )}
                </span>
            </button>
            {open && (
                <div style={{ borderTop: `1px solid ${theme.border}`, padding: '12px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    {children}
                </div>
            )}
        </div>
    );
}

/** @param {any} p0 @returns {any} */
function OutlineNodeRow({ item, selected, theme, sortByOptions, getFieldLabel = formatDisplayLabel, onSelect }) {
    const node = item.node;
    /** @type {any} */
    const title = getNodeTitle(item);
    /** @type {any} */
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
    /** @type {any} */
    const headerLabel = getNodeHeaderLabel(node);
    /** @type {any} */
    const sortLabel = getSortOptionLabel(node.sortBy, sortByOptions);

    return (
        <button type="button" onClick={onSelect} style={outlineItemStyle(theme, selected, item.depth)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '10px' }}>
                <div style={{ minWidth: 0, display: 'flex', flexDirection: 'column', gap: '7px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
                        <SummaryChip theme={theme} tone={item.branchBadge ? 'primary' : 'neutral'}>{title}</SummaryChip>
                        <span style={{ fontSize: '12px', fontWeight: 800, color: theme.text }}>{fieldLabel}</span>
                    </div>
                    <div style={{ fontSize: '11px', color: theme.textSec }}>
                        Header: {headerLabel}
                    </div>
                    <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                        <SummaryChip theme={theme}>{node.topN ? `Top ${node.topN}` : 'All rows'}</SummaryChip>
                        <SummaryChip theme={theme}>{sortLabel ? `Sort ${node.sortDir}${node.sortAbs ? ' |abs|' : ''} by ${sortLabel}` : 'Default sort'}</SummaryChip>
                        {node.defaultChild && <SummaryChip theme={theme}>Has next level</SummaryChip>}
                        {node.branches.length > 0 && (
                            <SummaryChip theme={theme} tone="primary">
                                {node.branches.length} row override{node.branches.length === 1 ? '' : 's'}
                            </SummaryChip>
                        )}
                    </div>
                </div>
                <span style={{ color: selected ? (theme.primary || '#2563EB') : theme.textSec, flexShrink: 0 }}>
                    <Icons.ChevronRight />
                </span>
            </div>
        </button>
    );
}

/** @param {any} p0 @returns {any} */
function OutlineBranchRow({ item, selected, theme, onSelect }) {
    const branch = item.branch;
    /** @type {any} */
    const name = branch.label && branch.label.trim() ? branch.label.trim() : 'Display original label';

    return (
        <button type="button" onClick={onSelect} style={outlineItemStyle(theme, selected, item.depth)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '10px' }}>
                <div style={{ minWidth: 0, display: 'flex', flexDirection: 'column', gap: '7px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
                        <SummaryChip theme={theme} tone="primary">{`Row override ${item.badge}`}</SummaryChip>
                        <span style={{ fontSize: '12px', fontWeight: 800, color: theme.text }}>{name}</span>
                    </div>
                    <div style={{ fontSize: '11px', color: theme.textSec, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {formatConditionSummary(branch.condition)}
                    </div>
                    <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                        <SummaryChip theme={theme}>{branch.child ? 'Custom children' : 'Default children'}</SummaryChip>
                    </div>
                </div>
                <span style={{ color: selected ? (theme.primary || '#2563EB') : theme.textSec, flexShrink: 0 }}>
                    <Icons.ChevronRight />
                </span>
            </div>
        </button>
    );
}

/** @param {any} p0 @returns {any} */
function LevelRow({
    item,
    selected,
    theme,
    sortByOptions,
    getFieldLabel = formatDisplayLabel,
    onSelect,
    onDragStart,
    onDragOver,
    onDrop,
}) {
    const node = item.node;
    /** @type {any} */
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
    /** @type {any} */
    const title = node.label && node.label.trim() ? node.label.trim() : fieldLabel;
    /** @type {any} */
    const sortLabel = getSortOptionLabel(node.sortBy, sortByOptions);

    return (
        <button
            type="button"
            draggable
            onDragStart={onDragStart}
            onDragOver={onDragOver}
            onDrop={onDrop}
            onClick={onSelect}
            style={{
                width: '100%',
                border: `1px solid ${selected ? (theme.primary || '#2563EB') : theme.border}`,
                borderRadius: '12px',
                background: selected ? (theme.select || `${theme.primary || '#2563EB'}14`) : (theme.surfaceBg || theme.background || '#fff'),
                padding: '10px 12px',
                textAlign: 'left',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
            }}
        >
            <span style={{ cursor: 'grab', display: 'inline-flex' }}>
                <Icons.DragIndicator />
            </span>
            <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '7px', minWidth: 0 }}>
                    <span style={{ fontSize: '11px', fontWeight: 800, color: theme.textSec, flexShrink: 0 }}>
                        Level {item.levelNumber}
                    </span>
                    <span style={{ fontSize: '12px', fontWeight: 800, color: theme.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {title}
                    </span>
                </div>
                <div style={{ fontSize: '11px', color: theme.textSec, marginTop: '3px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {fieldLabel}
                    {sortLabel ? ` · ${node.sortDir === 'asc' ? 'Asc' : 'Desc'}${node.sortAbs ? ' |abs|' : ''} by ${sortLabel}` : ''}
                    {node.topN ? ` · Top ${node.topN}` : ''}
                    {node.filters && node.filters.clauses && node.filters.clauses.length > 0 ? ` · ${node.filters.clauses.length} filter${node.filters.clauses.length === 1 ? '' : 's'}` : ''}
                </div>
            </div>
            <span style={{ color: selected ? (theme.primary || '#2563EB') : theme.textSec, flexShrink: 0 }}>
                <Icons.ChevronRight />
            </span>
        </button>
    );
}

/** @param {any} p0 @returns {any} */
function NodeInspector({
    item,
    node,
    allFields,
    getFieldLabel = formatDisplayLabel,
    sortByOptions,
    theme,
    onUpdate,
    onApply,
    onRemove,
    onEditFilter,
    titleLabel = 'Title',
    canRemove = true,
}) {
    /** @type {any} */
    const inputStyle = inp(theme);
    /** @type {any} */
    const title = item && item.levelNumber ? `Level ${item.levelNumber}` : 'Level';
    /** @type {any} */
    const hasFilter = Boolean(node.filters && node.filters.clauses && node.filters.clauses.length > 0);
    /** @type {any} */
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
    /** @type {any} */
    const summaryParts = [
        fieldLabel,
        node.label && node.label.trim() ? `Title: ${node.label.trim()}` : null,
        node.topN ? `Top ${node.topN}` : null,
        hasFilter ? `${node.filters.clauses.length} filter${node.filters.clauses.length === 1 ? '' : 's'}` : null,
    ].filter(Boolean);

    return (
        <div style={panelCardStyle(theme)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                <div>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{title}</div>
                </div>
                <div style={{ display: 'flex', gap: '8px', flexShrink: 0 }}>
                    <button
                        type="button"
                        onClick={onApply}
                        style={{
                            background: theme.primary || '#2563EB',
                            border: 'none',
                            borderRadius: '9px',
                            padding: '8px 14px',
                            fontSize: '12px',
                            fontWeight: 800,
                            color: '#fff',
                            cursor: 'pointer',
                        }}
                    >
                        Apply
                    </button>
                    {canRemove && (
                        <button type="button" onClick={onRemove} style={ghostBtn(theme, false, true)}>
                            <Icons.Delete /> {item && item.path && item.path.length === 0 ? 'Clear report' : 'Remove level'}
                        </button>
                    )}
                </div>
            </div>

            <CollapsibleLevelSection
                title={`${title} settings`}
                summary={summaryParts.join(' · ')}
                theme={theme}
                defaultOpen={false}
            >
                <div>
                    <div style={sectionLabelStyle(theme)}>Break Down By</div>
                    <select
                        value={node.field}
                        onChange={(event) => onUpdate({ ...node, field: event.target.value })}
                        style={inputStyle}
                    >
                        <option value="">Select field...</option>
                        {allFields.map(/** @param {any} [field] @returns {any} */ (field) => (
                            <option key={field} value={field}>{getFieldLabel(field)}</option>
                        ))}
                    </select>
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>{titleLabel}</div>
                    <input
                        value={node.label}
                        onChange={(event) => onUpdate({ ...node, label: event.target.value })}
                        placeholder={node.field ? getFieldLabel(node.field) : 'Label for this level'}
                        style={inputStyle}
                    />
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>Ranking And Sort</div>
                    <div style={{ display: 'grid', gridTemplateColumns: '82px 1fr 42px 42px', gap: '8px', alignItems: 'stretch' }}>
                        <input
                            type="number"
                            min="1"
                            value={node.topN || ''}
                            onChange={(event) => {
                                /** @type {any} */
                                const value = event.target.value === '' ? null : parseInt(event.target.value, 10);
                                onUpdate({ ...node, topN: value && value > 0 ? value : null });
                            }}
                            placeholder="All"
                            style={{ ...inputStyle, textAlign: 'center' }}
                        />
                        <select
                            value={node.sortBy || ''}
                            onChange={(event) => onUpdate({ ...node, sortBy: event.target.value || null })}
                            style={inputStyle}
                        >
                            {sortByOptions.map(/** @param {any} [option] @returns {any} */ (option) => (
                                <option key={option.value} value={option.value}>{option.label}</option>
                            ))}
                        </select>
                        <button
                            type="button"
                            onClick={() => onUpdate({ ...node, sortAbs: !node.sortAbs })}
                            style={{
                                border: `1px solid ${node.sortAbs ? (theme.primary || '#2563EB') : theme.border}`,
                                borderRadius: '10px',
                                background: node.sortAbs ? `${theme.primary || '#2563EB'}18` : (theme.hover || '#F3F4F6'),
                                color: node.sortAbs ? (theme.primary || '#2563EB') : theme.text,
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                fontSize: '11px',
                                fontWeight: 700,
                            }}
                            title="Sort by absolute value (ignores sign)"
                        >
                            |a|
                        </button>
                        <button
                            type="button"
                            onClick={() => onUpdate({ ...node, sortDir: node.sortDir === 'asc' ? 'desc' : 'asc' })}
                            style={{
                                border: `1px solid ${theme.border}`,
                                borderRadius: '10px',
                                background: theme.hover || '#F3F4F6',
                                color: theme.text,
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}
                            title={node.sortDir === 'asc' ? 'Ascending' : 'Descending'}
                        >
                            {node.sortDir === 'asc' ? <Icons.SortAsc /> : <Icons.SortDesc />}
                        </button>
                    </div>
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>Level Filter</div>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'stretch' }}>
                        <button
                            type="button"
                            onClick={onEditFilter}
                            style={inp(theme, {
                                flex: 1,
                                display: 'flex',
                                alignItems: 'center',
                                gap: '8px',
                                textAlign: 'left',
                                cursor: 'pointer',
                                background: hasFilter ? `${theme.primary || '#2563EB'}08` : (theme.hover || '#F8FAFC'),
                                borderColor: hasFilter ? `${theme.primary || '#2563EB'}45` : undefined,
                            })}
                        >
                            <Icons.Filter />
                            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                                {hasFilter ? formatConditionSummary(node.filters) : 'No filter (show all rows)'}
                            </span>
                        </button>
                        {hasFilter && (
                            <button
                                type="button"
                                onClick={() => onUpdate({ ...node, filters: null })}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    borderRadius: '10px',
                                    background: theme.hover || '#F3F4F6',
                                    color: '#B91C1C',
                                    cursor: 'pointer',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    padding: '0 10px',
                                    flexShrink: 0,
                                }}
                                title="Clear filter"
                            >
                                <Icons.Close />
                            </button>
                        )}
                    </div>
                    <div style={{ fontSize: '11px', color: theme.textSec, marginTop: '6px', lineHeight: 1.4 }}>
                        Rows that don't match the filter are grouped into "Others" to keep totals intact.
                    </div>
                </div>

                <ReportFormatEditor
                    value={node.format}
                    theme={theme}
                    onChange={/** @param {any} [format] @returns {any} */ (format) => onUpdate({ ...node, format })}
                />
            </CollapsibleLevelSection>

        </div>
    );
}

/** @param {any} p0 @returns {any} */
function RowOverrideInspector({
    draft,
    allFields,
    getFieldLabel = formatDisplayLabel,
    sortByOptions,
    theme,
    onChange,
    onApply,
    onCancel,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [dragIndex, setDragIndex] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [rowFilterModalOpen, setRowFilterModalOpen] = useState(false);
    /** @type {any} */
    const childRoot = draft && draft.child ? draft.child : createNode('');
    /** @type {any} */
    const selectedPath = Array.isArray(draft && draft.selectedPath) ? draft.selectedPath : [];
    /** @type {any} */
    const selectedNode = getNodeAtPath(childRoot, selectedPath) || childRoot;
    /** @type {any} */
    const rowHasFilter = Boolean(selectedNode.filters && selectedNode.filters.clauses && selectedNode.filters.clauses.length > 0);
    /** @type {any} */
    const levelItems = buildDefaultLevelItems(childRoot, draft.childLevelNumber || 2);
    /** @type {any} */
    const selectedIndex = Math.max(0, selectedPath.length);
    /** @type {any} */
    const selectedLevelNumber = (draft && Number.isFinite(Number(draft.childLevelNumber))
        ? Number(draft.childLevelNumber)
        : 2) + selectedPath.length;
    /** @type {any} */
    const inputStyle = inp(theme);
    /** @type {any} */
    const rowOverrideSummaryParts = [
        selectedNode.field ? getFieldLabel(selectedNode.field) : 'Select field',
        draft.title && draft.title.trim() ? `Title: ${draft.title.trim()}` : null,
        selectedNode.topN ? `Top ${selectedNode.topN}` : null,
        rowHasFilter ? `${selectedNode.filters.clauses.length} filter${selectedNode.filters.clauses.length === 1 ? '' : 's'}` : null,
    ].filter(Boolean);
    /** @param {any} [nextNode] @returns {any} */
    const updateSelectedNode = (nextNode) => {
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, () => nextNode),
        });
    };

    /** @param {any} [index] @returns {any} */
    const openDraftLevel = (index) => {
        onChange({
            ...draft,
            selectedPath: levelPathForIndex(index),
        });
    };

    /** @returns {any} */
    const addNextLevel = () => {
        /** @type {any[]} */
        const nextPath = [...selectedPath, DEFAULT_SEGMENT];
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, /** @param {any} [node] @returns {any} */ (node) => ({
                ...node,
                defaultChild: node.defaultChild || createNode(''),
            })),
            selectedPath: nextPath,
        });
    };

    /** @returns {any} */
    const removeNextLevel = () => {
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, /** @param {any} [node] @returns {any} */ (node) => ({
                ...node,
                defaultChild: null,
            })),
        });
    };

    /** @param {any} [fromIndex] @param {any} [toIndex] @returns {any} */
    const reorderDraftLevels = (fromIndex, toIndex) => {
        if (fromIndex === null || fromIndex === toIndex) return;
        onChange({
            ...draft,
            child: reorderDefaultChain(childRoot, fromIndex, toIndex),
            selectedPath: levelPathForIndex(toIndex),
        });
        setDragIndex(null);
    };

    return (
    <>
        <div style={panelCardStyle(theme)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                <div>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>
                        {draft.rowLabel || 'Selected row'}
                    </div>
                    <div style={{ fontSize: '12px', color: theme.textSec, marginTop: '2px' }}>
                        Override applies to this row only.
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '8px', flexShrink: 0 }}>
                    <button type="button" onClick={onCancel} style={ghostBtn(theme)}>
                        <Icons.Close /> Close
                    </button>
                    <button
                        type="button"
                        onClick={onApply}
                        style={{
                            background: theme.primary || '#2563EB',
                            border: 'none',
                            borderRadius: '9px',
                            padding: '8px 14px',
                            fontSize: '12px',
                            fontWeight: 800,
                            color: '#fff',
                            cursor: 'pointer',
                        }}
                    >
                        Apply
                    </button>
                </div>
            </div>

            <div>
                <div style={sectionLabelStyle(theme)}>Level</div>
                <div style={{ fontSize: '12px', fontWeight: 800, color: theme.text }}>
                    {`Level ${selectedLevelNumber}`}
                </div>
            </div>

            <RowDebugInspector debug={draft.request && draft.request.debug} theme={theme} />

            {levelItems.length > 1 && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {levelItems.map(/** @param {any} [item] @returns {any} */ (item) => (
                        <LevelRow
                            key={`draft-level:${item.index}:${item.node._id}`}
                            item={item}
                            selected={selectedIndex === item.index}
                            theme={theme}
                            sortByOptions={sortByOptions}
                            getFieldLabel={getFieldLabel}
                            onSelect={() => openDraftLevel(item.index)}
                            onDragStart={/** @param {any} [event] @returns {any} */ (event) => {
                                setDragIndex(item.index);
                                event.dataTransfer.effectAllowed = 'move';
                            }}
                            onDragOver={/** @param {any} [event] @returns {any} */ (event) => event.preventDefault()}
                            onDrop={/** @param {any} [event] @returns {any} */ (event) => {
                                event.preventDefault();
                                reorderDraftLevels(dragIndex, item.index);
                            }}
                        />
                    ))}
                </div>
            )}

            <CollapsibleLevelSection
                title={`Level ${selectedLevelNumber} settings`}
                summary={rowOverrideSummaryParts.join(' · ')}
                theme={theme}
                defaultOpen={false}
            >
                <div>
                    <div style={sectionLabelStyle(theme)}>Title</div>
                    <input
                        value={draft.title}
                        onChange={(event) => onChange({ ...draft, title: event.target.value })}
                        placeholder={draft.rowLabel || 'Row title'}
                        style={inputStyle}
                    />
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>Break Down By</div>
                    <select
                        value={selectedNode.field}
                        onChange={(event) => updateSelectedNode({ ...selectedNode, field: event.target.value })}
                        style={inputStyle}
                    >
                        <option value="">Select field...</option>
                        {allFields.map(/** @param {any} [field] @returns {any} */ (field) => (
                            <option key={field} value={field}>{getFieldLabel(field)}</option>
                        ))}
                    </select>
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>Ranking And Sort</div>
                    <div style={{ display: 'grid', gridTemplateColumns: '82px 1fr 42px 42px', gap: '8px', alignItems: 'stretch' }}>
                        <input
                            type="number"
                            min="1"
                            value={selectedNode.topN || ''}
                            onChange={(event) => {
                                /** @type {any} */
                                const value = event.target.value === '' ? null : parseInt(event.target.value, 10);
                                updateSelectedNode({ ...selectedNode, topN: value && value > 0 ? value : null });
                            }}
                            placeholder="All"
                            style={{ ...inputStyle, textAlign: 'center' }}
                        />
                        <select
                            value={selectedNode.sortBy || ''}
                            onChange={(event) => updateSelectedNode({ ...selectedNode, sortBy: event.target.value || null })}
                            style={inputStyle}
                        >
                            {sortByOptions.map(/** @param {any} [option] @returns {any} */ (option) => (
                                <option key={option.value} value={option.value}>{option.label}</option>
                            ))}
                        </select>
                        <button
                            type="button"
                            onClick={() => updateSelectedNode({ ...selectedNode, sortAbs: !selectedNode.sortAbs })}
                            style={{
                                border: `1px solid ${selectedNode.sortAbs ? (theme.primary || '#2563EB') : theme.border}`,
                                borderRadius: '10px',
                                background: selectedNode.sortAbs ? `${theme.primary || '#2563EB'}18` : (theme.hover || '#F3F4F6'),
                                color: selectedNode.sortAbs ? (theme.primary || '#2563EB') : theme.text,
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                fontSize: '11px',
                                fontWeight: 700,
                            }}
                            title="Sort by absolute value (ignores sign)"
                        >
                            |a|
                        </button>
                        <button
                            type="button"
                            onClick={() => updateSelectedNode({ ...selectedNode, sortDir: selectedNode.sortDir === 'asc' ? 'desc' : 'asc' })}
                            style={{
                                border: `1px solid ${theme.border}`,
                                borderRadius: '10px',
                                background: theme.hover || '#F3F4F6',
                                color: theme.text,
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}
                            title={selectedNode.sortDir === 'asc' ? 'Ascending' : 'Descending'}
                        >
                            {selectedNode.sortDir === 'asc' ? <Icons.SortAsc /> : <Icons.SortDesc />}
                        </button>
                    </div>
                </div>

                <div>
                    <div style={sectionLabelStyle(theme)}>Filter (this row's children only)</div>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'stretch' }}>
                        <button
                            type="button"
                            onClick={() => setRowFilterModalOpen(true)}
                            style={inp(theme, {
                                flex: 1,
                                display: 'flex',
                                alignItems: 'center',
                                gap: '8px',
                                textAlign: 'left',
                                cursor: 'pointer',
                                background: rowHasFilter ? `${theme.primary || '#2563EB'}08` : (theme.hover || '#F8FAFC'),
                                borderColor: rowHasFilter ? `${theme.primary || '#2563EB'}45` : undefined,
                            })}
                        >
                            <Icons.Filter />
                            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                                {rowHasFilter ? formatConditionSummary(selectedNode.filters) : 'No filter (show all children)'}
                            </span>
                        </button>
                        {rowHasFilter && (
                            <button
                                type="button"
                                onClick={() => updateSelectedNode({ ...selectedNode, filters: null })}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    borderRadius: '10px',
                                    background: theme.hover || '#F3F4F6',
                                    color: '#B91C1C',
                                    cursor: 'pointer',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    padding: '0 10px',
                                    flexShrink: 0,
                                }}
                                title="Clear filter"
                            >
                                <Icons.Close />
                            </button>
                        )}
                    </div>
                    <div style={{ fontSize: '11px', color: theme.textSec, marginTop: '6px', lineHeight: 1.4 }}>
                        Filtered-out children are grouped into "Others" to keep totals intact.
                    </div>
                </div>

                <ReportFormatEditor
                    value={selectedNode.format}
                    theme={theme}
                    onChange={/** @param {any} [format] @returns {any} */ (format) => updateSelectedNode({ ...selectedNode, format })}
                />

                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                    <button
                        type="button"
                        onClick={addNextLevel}
                        disabled={!selectedNode.field}
                        style={{ ...ghostBtn(theme, Boolean(selectedNode.defaultChild)), opacity: selectedNode.field ? 1 : 0.5 }}
                    >
                        <Icons.Add /> {selectedNode.defaultChild ? 'Open next level' : 'Add next level'}
                    </button>
                    {selectedNode.defaultChild && (
                        <button type="button" onClick={removeNextLevel} style={ghostBtn(theme, false, true)}>
                            <Icons.Delete /> Remove next level
                        </button>
                    )}
                </div>
            </CollapsibleLevelSection>

        </div>

        {rowFilterModalOpen && (
            <ConditionModal
                title={`Filter children of "${draft.rowLabel || 'this row'}"`}
                description="Only children matching this filter are shown. Filtered-out rows are grouped into 'Others' to keep totals intact."
                condition={selectedNode.filters || createCondition()}
                allFields={allFields}
                getFieldLabel={getFieldLabel}
                theme={theme}
                onSave={/** @param {any} [nextCondition] @returns {any} */ (nextCondition) => {
                    updateSelectedNode({
                        ...selectedNode,
                        filters: nextCondition.clauses.length > 0 ? nextCondition : null,
                    });
                }}
                onClose={() => setRowFilterModalOpen(false)}
            />
        )}
    </>
    );
}

/** @param {any} p0 @returns {any} */
function BranchInspector({
    item,
    branch,
    theme,
    onUpdate,
    onEditCondition,
    onOpenCustomChildren,
    onRemoveCustomChildren,
    onRemove,
}) {
    /** @type {any} */
    const inputStyle = inp(theme);
    /** @type {any} */
    const summary = formatConditionSummary(branch.condition);

    return (
        <div style={panelCardStyle(theme)}>
            <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                <div>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{`Row override ${item.badge}`}</div>
                    <div style={{ fontSize: '12px', color: theme.textSec, marginTop: '2px' }}>
                        Use a row override when matching rows need a different label or child breakdown.
                    </div>
                </div>
                <button type="button" onClick={onRemove} style={ghostBtn(theme, false, true)}>
                    <Icons.Delete /> Remove override
                </button>
            </div>

            <div>
                <div style={sectionLabelStyle(theme)}>Display As</div>
                <input
                    value={branch.label}
                    onChange={(event) => onUpdate({ ...branch, label: event.target.value })}
                    placeholder="Label to show for matching rows"
                    style={inputStyle}
                />
            </div>

            <div>
                <div style={sectionLabelStyle(theme)}>Rule</div>
                <button
                    type="button"
                    onClick={onEditCondition}
                    title={summary}
                    style={{
                        ...inp(theme),
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        textAlign: 'left',
                        cursor: 'pointer',
                        background: theme.hover || '#F8FAFC',
                    }}
                >
                    <Icons.Filter />
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{summary}</span>
                </button>
            </div>

            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                <button type="button" onClick={onOpenCustomChildren} style={ghostBtn(theme, Boolean(branch.child))}>
                    <Icons.Add /> {branch.child ? 'Open Custom children' : 'Add Custom children'}
                </button>
                {branch.child && (
                    <button type="button" onClick={onRemoveCustomChildren} style={ghostBtn(theme, false, true)}>
                        <Icons.Delete /> Remove Custom children
                    </button>
                )}
            </div>

            <div
                style={{
                    borderRadius: '10px',
                    padding: '10px 12px',
                    background: theme.hover || '#F8FAFC',
                    color: theme.textSec,
                    fontSize: '11px',
                    lineHeight: 1.5,
                }}
            >
                Override applies to this row only. Rows without this override continue with Default children.
            </div>
        </div>
    );
}

/** @param {{ setShowReportConfigColumn?: { (updater: (current: any) => any): any; (val?: any): any }, [key: string]: any }} p0 @returns {any} */
export function ReportEditor({
    reportDef,
    setReportDef,
    availableFields,
    customDimensions = [],
    theme,
    data = [],
    valConfigs,
    colFields,
    setColFields,
    setValConfigs,
    savedReports,
    setSavedReports,
    activeReportId,
    setActiveReportId,
    showNotification,
    requestedSelection = null,
    showReportConfigColumn = true,
    setShowReportConfigColumn,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [reportName, setReportName] = useState('');
    /** @type {[any, (...args: any[]) => any]} */
    const [selection, setSelection] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [editingCondition, setEditingCondition] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [editingNodeFilter, setEditingNodeFilter] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [rowOverrideDraft, setRowOverrideDraft] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [draggingLevelIndex, setDraggingLevelIndex] = useState(null);
    /** @type {any} */
    const selectedLineRef = useRef(null);
    /** @type {any} */
    const pendingExternalSelectionScrollRef = useRef(false);
    /** @type {any} */
    const lastProcessedRequestedSelectionRef = useRef(null);

    /** @type {any} */
    const root = useMemo(
        () => (reportDef && reportDef.root ? normalizeNode(reportDef.root) : null),
        [reportDef]
    );
    /** @type {any} */
    const allFields = useMemo(
        () => getSelectableFields(availableFields, valConfigs),
        [availableFields, valConfigs]
    );
    /** @type {(...args: any[]) => any} */
    const getFieldLabel = useCallback(
        (field) => formatCustomAwareFieldLabel(field, customDimensions),
        [customDimensions]
    );
    /** @type {any} */
    const sortByOptions = useMemo(
        () => buildSortOptions(valConfigs),
        [valConfigs]
    );
    /** @type {any} */
    const reportValidation = useMemo(
        () => validateReportDefinition(root, { allFields, valConfigs, data }),
        [root, allFields, valConfigs, data]
    );

    /** @type {(...args: any[]) => any} */
    const setRoot = useCallback((nextRootOrUpdater) => {
        setReportDef(/** @param {any} [prev] @returns {any} */ (prev) => {
            /** @type {any} */
            const prevDef = prev && typeof prev === 'object' ? prev : {};
            /** @type {any} */
            const prevRoot = prevDef.root ? normalizeNode(prevDef.root) : null;
            /** @type {any} */
            const nextRoot = typeof nextRootOrUpdater === 'function'
                ? nextRootOrUpdater(prevRoot)
                : nextRootOrUpdater;
            return { ...prevDef, root: nextRoot };
        });
    }, [setReportDef]);

    /** @type {any} */
    const outlineItems = useMemo(() => buildOutlineItems(root), [root]);
    /** @type {any} */
    const defaultLevelItems = useMemo(() => buildDefaultLevelItems(root), [root]);
    /** @type {any} */
    const selectedOutlineItem = useMemo(
        () => findSelectionItem(outlineItems, selection),
        [outlineItems, selection]
    );
    /** @type {any} */
    const trailItems = useMemo(
        () => buildTrailItems(outlineItems, selection),
        [outlineItems, selection]
    );
    /** @type {any} */
    const directChildItems = useMemo(
        () => buildDirectChildItems(outlineItems, selection),
        [outlineItems, selection]
    );

    /** @type {any} */
    const selectedItem = useMemo(() => {
        if (!root || !selection) return null;
        if (selection.type === 'node') {
            /** @type {any} */
            const node = getNodeAtPath(root, selection.path);
            return node ? { type: 'node', node } : null;
        }
        if (selection.type === 'branch') {
            /** @type {any} */
            const branch = getBranchAtPath(root, selection.nodePath, selection.branchIndex);
            return branch ? { type: 'branch', branch } : null;
        }
        return null;
    }, [root, selection]);

    /** @type {any} */
    const editingBranch = useMemo(() => {
        if (!editingCondition || !root) return null;
        return getBranchAtPath(root, editingCondition.nodePath, editingCondition.branchIndex);
    }, [editingCondition, root]);

    /** @type {(...args: any[]) => any} */
    const openRowPathOverride = useCallback((request) => {
        if (!request || request.type !== 'rowPath' || !root) return false;

        /** @type {any} */
        const nodePath = Array.isArray(request.nodePath) ? request.nodePath : [];
        /** @type {any} */
        const node = getNodeAtPath(root, nodePath);
        if (!node) return false;

        /** @type {any} */
        const existingBranches = Array.isArray(node.branches) ? node.branches : [];
        /** @type {any} */
        const existingIndex = existingBranches.findIndex(/** @param {any} [branch] @returns {any} */ (branch) => (
            branchMatchesRowPath(branch, request.field, request.value)
        ));
        /** @type {any} */
        const existingBranch = existingIndex >= 0 ? existingBranches[existingIndex] : null;
        /** @type {any} */
        const baseChild = existingBranch && existingBranch.child
            ? existingBranch.child
            : (node.defaultChild || createNode(''));
        /** @type {any} */
        const rowLabel = normalizeRowPathValue(request.label || request.value);

        setRowOverrideDraft({
            request,
            nodePath,
            branchIndex: existingIndex,
            rowLabel,
            title: existingBranch && existingBranch.label ? existingBranch.label : rowLabel,
            child: cloneNode(baseChild) || createNode(''),
            selectedPath: [],
            childLevelNumber: Number.isFinite(Number(request.levelNumber)) ? Number(request.levelNumber) + 1 : 2,
        });
        setSelection({ type: 'node', path: nodePath });
        return true;
    }, [root]);

    useEffect(() => {
        if (!root) {
            if (selection !== null) setSelection(null);
            return;
        }

        if (!selection) return;

        if (selection.type === 'node') {
            if (!getNodeAtPath(root, selection.path)) {
                setSelection({ type: 'node', path: [] });
            }
            return;
        }

        if (!getBranchAtPath(root, selection.nodePath, selection.branchIndex)) {
            setSelection({ type: 'node', path: selection.nodePath });
        }
    }, [root, selection]);

    /** @type {(...args: any[]) => any} */
    const scrollSelectedLineIntoView = useCallback(() => {
        const target = selectedLineRef.current;
        if (!target || typeof target.scrollIntoView !== 'function') return;
        window.requestAnimationFrame(() => {
            target.scrollIntoView({ block: 'start', behavior: 'smooth' });
        });
    }, []);

    useEffect(() => {
        if (!requestedSelection) return;
        if (lastProcessedRequestedSelectionRef.current === requestedSelection) return;
        lastProcessedRequestedSelectionRef.current = requestedSelection;
        pendingExternalSelectionScrollRef.current = true;
        if (requestedSelection.type === 'rowPath') {
            if (!openRowPathOverride(requestedSelection)) {
                pendingExternalSelectionScrollRef.current = false;
            }
            return;
        }
        if (selectionKey(requestedSelection) === selectionKey(selection)) {
            pendingExternalSelectionScrollRef.current = false;
            scrollSelectedLineIntoView();
            return;
        }
        setSelection(requestedSelection);
    }, [openRowPathOverride, requestedSelection, scrollSelectedLineIntoView, selection]);

    useEffect(() => {
        if (!pendingExternalSelectionScrollRef.current) return;
        pendingExternalSelectionScrollRef.current = false;
        scrollSelectedLineIntoView();
    }, [scrollSelectedLineIntoView, selection]);

    /** @type {(...args: any[]) => any} */
    const updateNode = useCallback((path, nextNode) => {
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, path, () => nextNode));
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const updateBranch = useCallback((nodePath, branchIndex, nextBranch) => {
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, () => nextBranch));
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const addRoot = useCallback((field) => {
        /** @type {any} */
        const nextRoot = createNode(field);
        setRoot(nextRoot);
        setRowOverrideDraft(null);
        setSelection({ type: 'node', path: [] });
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const openDefaultChild = useCallback((nodePath, hasChild) => {
        if (!hasChild) {
            setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, nodePath, /** @param {any} [node] @returns {any} */ (node) => ({
                ...node,
                defaultChild: createNode(''),
            })));
        }
        setSelection({ type: 'node', path: [...nodePath, DEFAULT_SEGMENT] });
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const removeDefaultChild = useCallback((nodePath) => {
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, nodePath, /** @param {any} [node] @returns {any} */ (node) => ({
            ...node,
            defaultChild: null,
        })));
        setSelection({ type: 'node', path: nodePath });
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const openCustomChildren = useCallback((nodePath, branchIndex, hasChild) => {
        if (!hasChild) {
            setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, /** @param {any} [branch] @returns {any} */ (branch) => ({
                ...branch,
                child: createNode(allFields[0] || ''),
            })));
        }
        setSelection({ type: 'node', path: [...nodePath, createBranchSegment(branchIndex)] });
    }, [allFields, setRoot]);

    /** @type {(...args: any[]) => any} */
    const removeCustomChildren = useCallback((nodePath, branchIndex) => {
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, /** @param {any} [branch] @returns {any} */ (branch) => ({
            ...branch,
            child: null,
        })));
        setSelection({ type: 'branch', nodePath, branchIndex });
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const removeNode = useCallback((path) => {
        if (path.length === 0) {
            setRoot(null);
            setRowOverrideDraft(null);
            setSelection(null);
            return;
        }

        /** @type {any} */
        const parentPath = path.slice(0, -1);
        const segment = path[path.length - 1];

        if (segment === DEFAULT_SEGMENT) {
            setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, parentPath, /** @param {any} [node] @returns {any} */ (node) => ({
                ...node,
                defaultChild: null,
            })));
            setSelection({ type: 'node', path: parentPath });
            return;
        }

        /** @type {any} */
        const branchIndex = parseBranchIndex(segment);
        if (branchIndex >= 0) {
            setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateBranchAtPath(prevRoot, parentPath, branchIndex, /** @param {any} [branch] @returns {any} */ (branch) => ({
                ...branch,
                child: null,
            })));
            setSelection({ type: 'branch', nodePath: parentPath, branchIndex });
        }
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const removeBranch = useCallback((nodePath, branchIndex) => {
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, nodePath, /** @param {any} [node] @returns {any} */ (node) => ({
            ...node,
            branches: node.branches.filter(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => index !== branchIndex),
        })));
        setSelection({ type: 'node', path: nodePath });
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const selectDefaultLevel = useCallback((path) => {
        setRowOverrideDraft(null);
        setSelection({ type: 'node', path });
    }, []);

    /** @type {(...args: any[]) => any} */
    const selectOutlineItem = useCallback((item) => {
        if (!item) return;
        setRowOverrideDraft(null);
        if (item.kind === 'branch') {
            setSelection({ type: 'branch', nodePath: item.nodePath, branchIndex: item.branchIndex });
            return;
        }
        setSelection({ type: 'node', path: item.nodePath });
    }, []);

    /** @type {(...args: any[]) => any} */
    const addNextDefaultLevel = useCallback(() => {
        if (!root || !defaultLevelItems.length) return;
        const deepestItem = defaultLevelItems[defaultLevelItems.length - 1];
        if (!deepestItem || !deepestItem.node || !deepestItem.node.field) return;
        /** @type {any[]} */
        const nextPath = [...deepestItem.path, DEFAULT_SEGMENT];
        setRowOverrideDraft(null);
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, deepestItem.path, /** @param {any} [node] @returns {any} */ (node) => ({
            ...node,
            defaultChild: node.defaultChild || createNode(''),
        })));
        setSelection({ type: 'node', path: nextPath });
    }, [defaultLevelItems, root, setRoot]);

    /** @type {(...args: any[]) => any} */
    const reorderLevels = useCallback((fromIndex, toIndex) => {
        if (fromIndex === null || fromIndex === toIndex) return;
        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => reorderDefaultChain(prevRoot, fromIndex, toIndex));
        setSelection({ type: 'node', path: levelPathForIndex(toIndex) });
        setDraggingLevelIndex(null);
    }, [setRoot]);

    /** @type {(...args: any[]) => any} */
    const closeRowOverrideDraft = useCallback(() => {
        setRowOverrideDraft(null);
        setSelection(null);
    }, []);

    /** @type {(...args: any[]) => any} */
    const applySelectedLevel = useCallback(() => {
        if (!reportValidation.valid) {
            if (showNotification) showNotification(reportValidation.errors[0] || 'Fix report validation errors before applying.', 'error');
            return;
        }
        setSelection(null);
    }, [reportValidation, showNotification]);

    /** @type {(...args: any[]) => any} */
    const applyRowOverrideDraft = useCallback(() => {
        if (!rowOverrideDraft || !rowOverrideDraft.request) return;
        const request = rowOverrideDraft.request;
        /** @type {any} */
        const nodePath = Array.isArray(rowOverrideDraft.nodePath) ? rowOverrideDraft.nodePath : [];
        /** @type {any} */
        const nextChild = pruneDraftChild(rowOverrideDraft.child);
        /** @type {Record<string, any>} */
        const nextBranch = {
            ...createRowPathBranch(request.field, request.value, rowOverrideDraft.title || rowOverrideDraft.rowLabel, ''),
            label: normalizeRowPathValue(rowOverrideDraft.title || rowOverrideDraft.rowLabel || request.value),
            sourceRowPath: Array.isArray(request.pathValues) ? request.pathValues.map(String).filter(Boolean) : [],
            sourcePathFields: Array.isArray(request.pathFields) ? request.pathFields.map(String).filter(Boolean) : [],
            child: nextChild,
        };

        /** @param {any} [node] @returns {any} */
        const applyBranch = (node) => {
            /** @type {any} */
            const branches = Array.isArray(node.branches) ? node.branches : [];
            /** @type {any} */
            const existingIndex = branches.findIndex(/** @param {any} [branch] @returns {any} */ (branch) => (
                branchMatchesRowPath(branch, request.field, request.value)
            ));
            if (existingIndex >= 0) {
                return {
                    ...node,
                    branches: branches.map(/** @param {any} [branch] @param {any} [index] @returns {any} */ (branch, index) => (
                        index === existingIndex ? { ...nextBranch, _id: branch._id || nextBranch._id } : branch
                    )),
                };
            }
            return {
                ...node,
                branches: [...branches, nextBranch],
            };
        };
        /** @type {any} */
        const projectedRoot = updateNodeAtPath(root, nodePath, applyBranch);
        /** @type {any} */
        const validation = validateReportDefinition(projectedRoot, { allFields, valConfigs, data });
        if (!validation.valid) {
            if (showNotification) showNotification(validation.errors[0] || 'Fix report validation errors before applying.', 'error');
            return;
        }

        setRoot(/** @param {any} [prevRoot] @returns {any} */ (prevRoot) => updateNodeAtPath(prevRoot, nodePath, applyBranch));

        setRowOverrideDraft(null);
        setSelection(null);
        if (showNotification) showNotification(`Applied settings for ${rowOverrideDraft.rowLabel || 'row'}`, 'info');
    }, [allFields, data, root, rowOverrideDraft, setRoot, showNotification, valConfigs]);

    /** @type {(...args: any[]) => any} */
    const saveReport = useCallback(() => {
        if (!root || !root.field) return;
        if (!reportValidation.valid) {
            if (showNotification) showNotification(reportValidation.errors[0] || 'Fix report validation errors before saving.', 'error');
            return;
        }

        const id = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        /** @type {any} */
        const name = reportName.trim() || `Report ${(savedReports || []).length + 1}`;
        setSavedReports([
            ...(savedReports || []),
            {
                id,
                name,
                reportDef: JSON.parse(JSON.stringify(reportDef)),
                layout: captureReportLayout(colFields, valConfigs),
                createdAt: new Date().toISOString(),
            },
        ]);
        setActiveReportId(id);
        setReportName('');
        if (showNotification) showNotification(`Report "${name}" saved`, 'info');
    }, [colFields, valConfigs, reportDef, reportName, reportValidation, root, savedReports, setActiveReportId, setSavedReports, showNotification]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '18px', padding: '2px 0 8px' }}>
            {(savedReports || []).length > 0 && (
                <div>
                    <div style={sectionLabelStyle(theme)}><Icons.Save /> Saved Reports</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                        {(savedReports || []).map(/** @param {any} [report] @returns {any} */ (report) => (
                            <div
                                key={report.id}
                                onClick={() => {
                                    applySavedReport(report, { setReportDef, setActiveReportId, setColFields, setValConfigs });
                                    setRowOverrideDraft(null);
                                    setSelection({ type: 'node', path: [] });
                                }}
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '8px',
                                    padding: '8px 10px',
                                    borderRadius: '10px',
                                    cursor: 'pointer',
                                    background: report.id === activeReportId ? (theme.select || `${theme.primary || '#2563EB'}14`) : (theme.hover || '#F9FAFB'),
                                    border: `1px solid ${report.id === activeReportId ? (theme.primary || '#2563EB') : theme.border}`,
                                }}
                            >
                                <Icons.Report />
                                <div style={{ flex: 1, minWidth: 0, fontSize: '12px', fontWeight: 700, color: theme.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                    {report.name}
                                </div>
                                <button
                                    type="button"
                                    onClick={(event) => {
                                        event.stopPropagation();
                                        setSavedReports(/** @param {any} [prev] @returns {any} */ (prev) => (prev || []).filter(/** @param {any} [current] @returns {any} */ (current) => current.id !== report.id));
                                        if (activeReportId === report.id) setActiveReportId(null);
                                    }}
                                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#B91C1C', padding: '2px', flexShrink: 0 }}
                                >
                                    <Icons.Delete />
                                </button>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            <ReportSection
                theme={theme}
                sectionId="report-levels"
                title="Report Levels"
                description="Default hierarchy applied to every row unless a row override changes its children."
                icon={<Icons.ReportLevel />}
                action={root && (
                    <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        {typeof setShowReportConfigColumn === 'function' && (
                            <button
                                type="button"
                                onClick={() => setShowReportConfigColumn((current) => (current === false ? true : false))}
                                style={ghostBtn(theme, showReportConfigColumn !== false, false)}
                                title={showReportConfigColumn !== false ? 'Hide row gear column' : 'Show row gear column'}
                            >
                                <Icons.Settings /> {showReportConfigColumn !== false ? 'Hide row gears' : 'Show row gears'}
                            </button>
                        )}
                        <button
                            type="button"
                            onClick={addNextDefaultLevel}
                            disabled={!defaultLevelItems.length || !defaultLevelItems[defaultLevelItems.length - 1].node.field}
                            style={{
                                ...ghostBtn(theme, false, false),
                                opacity: (!defaultLevelItems.length || !defaultLevelItems[defaultLevelItems.length - 1].node.field) ? 0.5 : 1,
                                pointerEvents: (!defaultLevelItems.length || !defaultLevelItems[defaultLevelItems.length - 1].node.field) ? 'none' : 'auto',
                            }}
                        >
                            <Icons.Add /> Add next level
                        </button>
                    </div>
                    )}
            >
                {!root ? (
                    <div style={panelCardStyle(theme)}>
                        <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>Level 1</div>
                        <div>
                            <div style={sectionLabelStyle(theme)}>Break Down By</div>
                            <select
                                value=""
                                onChange={(event) => {
                                    if (event.target.value) addRoot(event.target.value);
                                }}
                                style={inp(theme)}
                            >
                                <option value="">Select field...</option>
                                {allFields.map(/** @param {any} [field] @returns {any} */ (field) => (
                                    <option key={field} value={field}>{getFieldLabel(field)}</option>
                                ))}
                            </select>
                        </div>
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {defaultLevelItems.map(/** @param {any} [item] @returns {any} */ (item) => (
                            <LevelRow
                                key={`default-level:${item.index}:${item.node._id}`}
                                item={item}
                                selected={!rowOverrideDraft && selectionKey(selection) === selectionKey({ type: 'node', path: item.path })}
                                theme={theme}
                                sortByOptions={sortByOptions}
                                getFieldLabel={getFieldLabel}
                                onSelect={() => selectDefaultLevel(item.path)}
                                onDragStart={/** @param {any} [event] @returns {any} */ (event) => {
                                    setDraggingLevelIndex(item.index);
                                    event.dataTransfer.effectAllowed = 'move';
                                }}
                                onDragOver={/** @param {any} [event] @returns {any} */ (event) => event.preventDefault()}
                                onDrop={/** @param {any} [event] @returns {any} */ (event) => {
                                    event.preventDefault();
                                    reorderLevels(draggingLevelIndex, item.index);
                                }}
                            />
                        ))}
                    </div>
                )}
            </ReportSection>

            {root && (
                <ReportSection
                    theme={theme}
                    sectionId="report-outline"
                    title="Report Outline"
                    description="Default levels plus every row override, shown as a separate map of Default children."
                    icon={<Icons.Report />}
                >
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {outlineItems.map(/** @param {any} [item] @returns {any} */ (item) => {
                            if (item.kind === 'branch') {
                                return (
                                    <OutlineBranchRow
                                        key={`outline-branch:${pathKey(item.nodePath)}:${item.branchIndex}:${item.branch._id}`}
                                        item={item}
                                        selected={!rowOverrideDraft && selectionKey(selection) === selectionKey({
                                            type: 'branch',
                                            nodePath: item.nodePath,
                                            branchIndex: item.branchIndex,
                                        })}
                                        theme={theme}
                                        onSelect={() => selectOutlineItem(item)}
                                    />
                                );
                            }

                            return (
                                <OutlineNodeRow
                                    key={`outline-node:${pathKey(item.nodePath)}:${item.node._id}`}
                                    item={item}
                                    selected={!rowOverrideDraft && selectionKey(selection) === selectionKey({ type: 'node', path: item.nodePath })}
                                    theme={theme}
                                    sortByOptions={sortByOptions}
                                    getFieldLabel={getFieldLabel}
                                    onSelect={() => selectOutlineItem(item)}
                                />
                            );
                        })}
                    </div>
                </ReportSection>
            )}

            {root && rowOverrideDraft && (
                <div ref={selectedLineRef}>
                    <ReportSection
                        theme={theme}
                        sectionId="selected-row"
                        title="Selected Row"
                        description="Configure Custom children for the row gear you clicked."
                        icon={<Icons.Settings />}
                    >
                        <RowOverrideInspector
                            draft={rowOverrideDraft}
                            allFields={allFields}
                            getFieldLabel={getFieldLabel}
                            sortByOptions={sortByOptions}
                            theme={theme}
                            onChange={setRowOverrideDraft}
                            onApply={applyRowOverrideDraft}
                            onCancel={closeRowOverrideDraft}
                        />
                    </ReportSection>
                </div>
            )}

            {root && !rowOverrideDraft && selection && selectedItem && selectedItem.type === 'node' && (
                <div ref={selectedLineRef}>
                    <ReportSection
                        theme={theme}
                        sectionId="selected-level"
                        title="Selected Level"
                        description="Edit the selected default level or override child level."
                        icon={<Icons.Edit />}
                    >
                        <NodeInspector
                            item={defaultLevelItems.find(/** @param {any} [item] @returns {any} */ (item) => isSamePath(item.path, selection.path)) || (selectedOutlineItem
                                ? { ...selectedOutlineItem, path: selectedOutlineItem.nodePath }
                                : { path: selection.path, levelNumber: selection.path.length + 1 })}
                            node={selectedItem.node}
                            allFields={allFields}
                            getFieldLabel={getFieldLabel}
                            sortByOptions={sortByOptions}
                            theme={theme}
                            onUpdate={/** @param {any} [nextNode] @returns {any} */ (nextNode) => updateNode(selection.path, nextNode)}
                            onApply={applySelectedLevel}
                            onRemove={() => removeNode(selection.path)}
                            onEditFilter={() => setEditingNodeFilter(selection.path)}
                            titleLabel="Title"
                        />
                    </ReportSection>
                </div>
            )}

            {root && !rowOverrideDraft && selection && selectedItem && selectedItem.type === 'branch' && selectedOutlineItem && (
                <div ref={selectedLineRef}>
                    <ReportSection
                        theme={theme}
                        sectionId="row-override"
                        title="Row Override"
                        description="Override applies to this row only. Configure its rule and Custom children."
                        icon={<Icons.Settings />}
                    >
                        <BranchInspector
                            item={selectedOutlineItem}
                            branch={selectedItem.branch}
                            theme={theme}
                            onUpdate={/** @param {any} [nextBranch] @returns {any} */ (nextBranch) => updateBranch(selection.nodePath, selection.branchIndex, nextBranch)}
                            onEditCondition={() => setEditingCondition({ nodePath: selection.nodePath, branchIndex: selection.branchIndex })}
                            onOpenCustomChildren={() => openCustomChildren(selection.nodePath, selection.branchIndex, Boolean(selectedItem.branch.child))}
                            onRemoveCustomChildren={() => removeCustomChildren(selection.nodePath, selection.branchIndex)}
                            onRemove={() => removeBranch(selection.nodePath, selection.branchIndex)}
                        />
                    </ReportSection>
                </div>
            )}

            <ReportSection
                theme={theme}
                sectionId="save-report"
                title="Save Report"
                description="Store the current report levels and row overrides."
                icon={<Icons.Save />}
            >
                <ValidationPanel validation={reportValidation} theme={theme} />
                <div style={{ display: 'flex', gap: '8px' }}>
                    <input
                        value={reportName}
                        onChange={(event) => setReportName(event.target.value)}
                        onKeyDown={(event) => {
                            if (event.key === 'Enter') saveReport();
                        }}
                        placeholder="Report name..."
                        style={{ ...inp(theme), flex: 1 }}
                    />
                    <button
                        type="button"
                        onClick={saveReport}
                        disabled={!root || !root.field || !reportValidation.valid}
                        style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '6px',
                            background: theme.primary || '#2563EB',
                            border: 'none',
                            borderRadius: '9px',
                            padding: '8px 14px',
                            fontSize: '12px',
                            fontWeight: 800,
                            color: '#fff',
                            cursor: 'pointer',
                            opacity: (!root || !root.field || !reportValidation.valid) ? 0.5 : 1,
                        }}
                    >
                        <Icons.Save /> Save
                    </button>
                </div>
            </ReportSection>

            {editingCondition && editingBranch && (
                <ConditionModal
                    condition={editingBranch.condition}
                    allFields={allFields}
                    getFieldLabel={getFieldLabel}
                    theme={theme}
                    onSave={/** @param {any} [nextCondition] @returns {any} */ (nextCondition) => {
                        updateBranch(editingCondition.nodePath, editingCondition.branchIndex, {
                            ...editingBranch,
                            condition: nextCondition,
                        });
                    }}
                    onClose={() => setEditingCondition(null)}
                />
            )}

            {editingNodeFilter && (() => {
                /** @type {any} */
                const filterNode = getNodeAtPath(root, editingNodeFilter);
                if (!filterNode) return null;
                return (
                    <ConditionModal
                        title="Edit level filter"
                        description="Rows that don't match are grouped into 'Others' to keep totals intact. Applies to this level and all its children."
                        condition={filterNode.filters || createCondition()}
                        allFields={allFields}
                        getFieldLabel={getFieldLabel}
                        theme={theme}
                        onSave={/** @param {any} [nextCondition] @returns {any} */ (nextCondition) => {
                            updateNode(editingNodeFilter, {
                                ...filterNode,
                                filters: nextCondition.clauses.length > 0 ? nextCondition : null,
                            });
                        }}
                        onClose={() => setEditingNodeFilter(null)}
                    />
                );
            })()}
        </div>
    );
}

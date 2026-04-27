import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Icons from '../../utils/Icons';
import { formatAggLabel, formatDisplayLabel } from '../../utils/helpers';
import { formatCustomAwareFieldLabel, isCustomCategoryField } from '../../hooks/usePivotNormalization';

const uid = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

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

const LIST_OPS = new Set(['in', 'not_in']);
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
const BORDER_STYLES = ['', 'solid', 'dashed', 'dotted', 'double'];

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

const createClause = (field = '') => ({ _id: uid(), field, operator: 'eq', value: '', values: [] });
const createCondition = () => ({ op: 'AND', clauses: [] });
const createBranch = () => ({ _id: uid(), label: '', condition: createCondition(), child: null });
const normalizeReportFormat = (format = {}) => {
    const source = format && typeof format === 'object' ? format : {};
    const indent = Number(source.indent);
    const borderWidth = Number(source.borderWidth);
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

const normalizeCondition = (condition) => ({
    op: condition && condition.op === 'OR' ? 'OR' : 'AND',
    clauses: Array.isArray(condition && condition.clauses)
        ? condition.clauses.map(normalizeClause).filter(Boolean)
        : [],
});

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

function normalizeNode(node) {
    if (!node || typeof node !== 'object') return null;

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

const createBranchSegment = (index) => `${BRANCH_PREFIX}${index}`;

const parseBranchIndex = (segment) => {
    if (typeof segment !== 'string' || !segment.startsWith(BRANCH_PREFIX)) return -1;
    const value = parseInt(segment.slice(BRANCH_PREFIX.length), 10);
    return Number.isFinite(value) ? value : -1;
};

const getSelectableFields = (availableFields, valConfigs) => {
    const aggIds = new Set(
        (valConfigs || [])
            .filter((config) => config && config.field && config.agg)
            .map((config) => (config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`))
    );

    return (availableFields || []).filter((field) => (
        field &&
        typeof field === 'string' &&
        !REPORT_META.has(field) &&
        (!field.startsWith('_') || isCustomCategoryField(field)) &&
        !aggIds.has(field)
    ));
};

const buildSortOptions = (valConfigs) => {
    const options = [
        { value: '', label: '(default)' },
        { value: '__field__', label: 'Field value (A→Z / Z→A)' },
    ];
    (valConfigs || []).forEach((config) => {
        if (!config || !config.field || !config.agg) return;
        const value = config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`;
        const label = config.agg === 'formula'
            ? (config.label || formatDisplayLabel(config.field))
            : `${formatDisplayLabel(config.field)} (${formatAggLabel(config.agg, config.weightField)})`;
        options.push({ value, label });
    });
    return options;
};

const formatConditionSummary = (condition) => {
    if (!condition || !condition.clauses || condition.clauses.length === 0) return 'No filter';

    return condition.clauses.map((clause) => {
        const field = formatDisplayLabel(clause.field);
        const operator = OP_SYMBOL[clause.operator] || clause.operator;
        if (LIST_OPS.has(clause.operator)) {
            const shown = clause.values.slice(0, 3);
            const extra = clause.values.length > 3 ? ` +${clause.values.length - 3}` : '';
            return `${field} ${operator} [${shown.join(', ')}${extra}]`;
        }
        return `${field} ${operator} ${clause.value || '...'}`;
    }).join(` ${condition.op} `);
};

const getSortOptionLabel = (value, sortByOptions) => {
    if (!value) return '';
    const option = (sortByOptions || []).find((item) => item.value === value);
    return option ? option.label : formatDisplayLabel(value);
};

const getNodeHeaderLabel = (node) => {
    if (!node) return 'Select field';
    return node.label && node.label.trim() ? node.label.trim() : (node.field ? formatDisplayLabel(node.field) : 'Select field');
};

const isClauseComplete = (clause) => {
    if (!clause || !clause.field) return false;
    if (LIST_OPS.has(clause.operator)) return Array.isArray(clause.values) && clause.values.length > 0;
    return clause.value !== undefined && clause.value !== null && String(clause.value).trim() !== '';
};

const conditionHasCompleteClauses = (condition) => (
    Boolean(condition && Array.isArray(condition.clauses) && condition.clauses.length > 0)
    && condition.clauses.every(isClauseComplete)
);

const evaluateConditionClause = (clause, row) => {
    if (!clause || !row || typeof row !== 'object') return false;
    const rawValue = row[clause.field];
    const value = rawValue === undefined || rawValue === null ? '' : String(rawValue);
    const clauseValue = clause.value === undefined || clause.value === null ? '' : String(clause.value);
    const values = Array.isArray(clause.values) ? clause.values.map(String) : [];
    const numberValue = Number(value);
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

const conditionMatchesAnyRow = (condition, data) => {
    if (!conditionHasCompleteClauses(condition) || !Array.isArray(data) || data.length === 0) return true;
    const usableRows = data.filter((row) => row && typeof row === 'object' && !row._isTotal && !row._isOther);
    const conditionFieldsAreLoaded = usableRows.some((row) => (
        condition.clauses.every((clause) => Object.prototype.hasOwnProperty.call(row, clause.field))
    ));
    if (!conditionFieldsAreLoaded) return true;
    return usableRows.some((row) => {
        if (condition.op === 'OR') return condition.clauses.some((clause) => evaluateConditionClause(clause, row));
        return condition.clauses.every((clause) => evaluateConditionClause(clause, row));
    });
};

const rowPathExistsInData = (sourceRowPath, data) => {
    if (!Array.isArray(sourceRowPath) || sourceRowPath.length === 0 || !Array.isArray(data) || data.length === 0) return true;
    const serialized = sourceRowPath.map(String).join('|||');
    return data.some((row) => row && typeof row === 'object' && row._path === serialized);
};

const validateReportDefinition = (root, {
    allFields = [],
    valConfigs = [],
    data = [],
} = {}) => {
    const errors = [];
    const warnings = [];
    const hasMeasure = (valConfigs || []).some((config) => config && config.field && config.agg);
    const knownFields = new Set(Array.isArray(allFields) ? allFields : []);
    const headerMap = new Map();
    const reportRows = Array.isArray(data) ? data : [];

    const pushDuplicateHeader = (header, pathLabel) => {
        const key = header.trim().toLowerCase();
        if (!key) return;
        if (headerMap.has(key)) {
            errors.push(`Duplicate header "${header}" at ${pathLabel}; already used at ${headerMap.get(key)}.`);
            return;
        }
        headerMap.set(key, pathLabel);
    };

    const validateNode = (node, pathLabel, stack = new Set()) => {
        if (!node) return;
        const nodeId = node._id || `${pathLabel}:${node.field || ''}`;
        if (stack.has(nodeId)) {
            errors.push(`Circular row override detected at ${pathLabel}.`);
            return;
        }
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

        (node.branches || []).forEach((branch, index) => {
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

const getNodeTitle = (item) => {
    if (!item || !item.branchBadge) return `Level ${item ? item.levelNumber : 1}`;
    if (item.branchDepth === 0) return `Override children ${item.branchBadge}`;
    return `Override child level ${item.branchBadge}.${item.branchDepth + 1}`;
};

const getNodeAtPath = (root, path) => {
    let current = root;
    for (let index = 0; current && index < path.length; index += 1) {
        const segment = path[index];
        if (segment === DEFAULT_SEGMENT) {
            current = current.defaultChild || null;
        } else {
            const branchIndex = parseBranchIndex(segment);
            current = branchIndex >= 0 && current.branches[branchIndex] ? current.branches[branchIndex].child : null;
        }
    }
    return current || null;
};

const getBranchAtPath = (root, nodePath, branchIndex) => {
    const node = getNodeAtPath(root, nodePath);
    return node && node.branches[branchIndex] ? node.branches[branchIndex] : null;
};

const updateNodeAtPath = (root, path, updater) => {
    if (!root) return root;

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

        const branchIndex = parseBranchIndex(segment);
        if (branchIndex < 0 || branchIndex >= current.branches.length) return current;

        return normalizeNode({
            ...current,
            branches: current.branches.map((branch, index) => (
                index === branchIndex
                    ? { ...branch, child: visit(branch.child, depth + 1) }
                    : branch
            )),
        });
    };

    return visit(root, 0);
};

const updateBranchAtPath = (root, nodePath, branchIndex, updater) => {
    if (!root) return root;
    return updateNodeAtPath(root, nodePath, (node) => {
        if (branchIndex < 0 || branchIndex >= node.branches.length) return node;
        return {
            ...node,
            branches: node.branches.map((branch, index) => (
                index === branchIndex ? normalizeBranch(updater(branch)) : branch
            )),
        };
    });
};

const buildOutlineItems = (root) => {
    if (!root) return [];

    const items = [];
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

        node.branches.forEach((branch, index) => {
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

const pathKey = (path = []) => path.join('>');

const isSamePath = (left = [], right = []) => pathKey(left) === pathKey(right);

const normalizeRowPathValue = (value) => (
    value === undefined || value === null ? '' : String(value)
);

const selectionKey = (selection) => {
    if (!selection) return '';
    if (selection.type === 'node') return `node:${pathKey(selection.path)}`;
    if (selection.type === 'branch') return `branch:${pathKey(selection.nodePath)}:${selection.branchIndex}`;
    if (selection.type === 'rowPath') return `rowPath:${pathKey(selection.nodePath)}:${selection.field}:${normalizeRowPathValue(selection.value)}`;
    return '';
};

const findNodeItem = (items, path) => (
    (items || []).find((item) => item.kind === 'node' && isSamePath(item.nodePath, path)) || null
);

const findBranchItem = (items, nodePath, branchIndex) => (
    (items || []).find((item) => (
        item.kind === 'branch' &&
        isSamePath(item.nodePath, nodePath) &&
        item.branchIndex === branchIndex
    )) || null
);

const branchMatchesRowPath = (branch, field, value) => {
    if (!branch || !field) return false;
    const expected = normalizeRowPathValue(value);
    const clauses = branch.condition && Array.isArray(branch.condition.clauses)
        ? branch.condition.clauses
        : [];
    return clauses.some((clause) => {
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

const cloneNode = (node) => {
    if (!node) return null;
    try {
        return normalizeNode(JSON.parse(JSON.stringify(node)));
    } catch (error) {
        return normalizeNode(node);
    }
};

const levelPathForIndex = (index) => (
    Array.from({ length: Math.max(0, index) }, () => DEFAULT_SEGMENT)
);

const buildDefaultLevelItems = (root, startLevelNumber = 1, pathPrefix = []) => {
    const items = [];
    let current = root;
    let index = 0;
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

const stripDefaultChild = (node) => (
    node ? { ...cloneNode(node), defaultChild: null } : null
);

const rebuildDefaultChain = (nodes) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return null;
    let child = null;
    for (let index = nodes.length - 1; index >= 0; index -= 1) {
        child = normalizeNode({
            ...stripDefaultChild(nodes[index]),
            defaultChild: child,
        });
    }
    return child;
};

const reorderDefaultChain = (root, fromIndex, toIndex) => {
    const chain = buildDefaultLevelItems(root).map((item) => item.node);
    if (
        fromIndex < 0 ||
        toIndex < 0 ||
        fromIndex >= chain.length ||
        toIndex >= chain.length ||
        fromIndex === toIndex
    ) {
        return root;
    }
    const next = [...chain];
    const [moved] = next.splice(fromIndex, 1);
    next.splice(toIndex, 0, moved);
    return rebuildDefaultChain(next);
};

const isUsableNode = (node) => Boolean(node && node.field);

const pruneDraftChild = (node) => {
    const normalized = cloneNode(node);
    if (!normalized || !normalized.field) return null;
    return normalized;
};

const findSelectionItem = (items, selection) => {
    if (!selection) return null;
    if (selection.type === 'node') return findNodeItem(items, selection.path);
    if (selection.type === 'branch') return findBranchItem(items, selection.nodePath, selection.branchIndex);
    return null;
};

const buildTrailItems = (items, selection) => {
    if (!selection) return [];

    const trail = [];
    const pushNode = (path) => {
        const item = findNodeItem(items, path);
        if (item) trail.push(item);
    };
    const pushBranch = (nodePath, branchIndex) => {
        const item = findBranchItem(items, nodePath, branchIndex);
        if (item) trail.push(item);
    };

    const walkNodePath = (nodePath) => {
        pushNode([]);
        if (!Array.isArray(nodePath) || nodePath.length === 0) return;

        let currentPath = [];
        nodePath.forEach((segment) => {
            if (segment === DEFAULT_SEGMENT) {
                currentPath = [...currentPath, segment];
                pushNode(currentPath);
                return;
            }

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

const buildDirectChildItems = (items, selection) => {
    if (!selection) return [];

    if (selection.type === 'node') {
        const children = [];
        const defaultChild = findNodeItem(items, [...selection.path, DEFAULT_SEGMENT]);
        if (defaultChild) children.push(defaultChild);

        const branchChildren = (items || [])
            .filter((item) => item.kind === 'branch' && isSamePath(item.nodePath, selection.path))
            .sort((left, right) => left.branchIndex - right.branchIndex);

        return [...children, ...branchChildren];
    }

    const customChildren = findNodeItem(items, [...selection.nodePath, createBranchSegment(selection.branchIndex)]);
    return customChildren ? [customChildren] : [];
};

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

const panelCardStyle = (theme) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: '14px',
    background: theme.surfaceBg || theme.background || '#fff',
    padding: '14px',
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
});

const reportSectionStyle = (theme) => ({
    borderTop: `2px solid ${theme.textSec || '#334155'}`,
    paddingTop: '14px',
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
});

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

function SummaryChip({ children, theme, tone = 'neutral' }) {
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

function ConditionModal({ condition, allFields, getFieldLabel = formatDisplayLabel, onSave, onClose, theme, title, description }) {
    const [local, setLocal] = useState(() => normalizeCondition(condition));
    const inputStyle = inp(theme);

    const addClause = useCallback(() => {
        setLocal((prev) => ({ ...prev, clauses: [...prev.clauses, createClause(allFields[0] || '')] }));
    }, [allFields]);

    const updateClause = useCallback((index, patch) => {
        setLocal((prev) => ({
            ...prev,
            clauses: prev.clauses.map((clause, clauseIndex) => (clauseIndex === index ? { ...clause, ...patch } : clause)),
        }));
    }, []);

    const removeClause = useCallback((index) => {
        setLocal((prev) => ({
            ...prev,
            clauses: prev.clauses.filter((_, clauseIndex) => clauseIndex !== index),
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
                                onClick={() => setLocal((prev) => ({ ...prev, op: operator }))}
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

                    {local.clauses.map((clause, index) => (
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
                                        {allFields.map((field) => (
                                            <option key={field} value={field}>{getFieldLabel(field)}</option>
                                        ))}
                                    </select>
                                    <select
                                        value={clause.operator}
                                        onChange={(event) => {
                                            const next = event.target.value;
                                            const nextIsList = LIST_OPS.has(next);
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
                                                {clause.values.map((value, valueIndex) => (
                                                    <SummaryChip key={`${clause._id}-${valueIndex}`} theme={theme} tone="primary">
                                                        <span>{value}</span>
                                                        <button
                                                            type="button"
                                                            onClick={() => updateClause(index, {
                                                                values: clause.values.filter((_, currentIndex) => currentIndex !== valueIndex),
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
                                            onKeyDown={(event) => {
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

function ReportFormatEditor({ value, onChange, theme }) {
    const format = normalizeReportFormat(value);
    const inputStyle = inp(theme);
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

function ValidationPanel({ validation, theme }) {
    const errors = validation && Array.isArray(validation.errors) ? validation.errors : [];
    const warnings = validation && Array.isArray(validation.warnings) ? validation.warnings : [];
    if (errors.length === 0 && warnings.length === 0) return null;

    return (
        <div data-report-validation="true" style={{ border: `1px solid ${errors.length > 0 ? '#FCA5A5' : '#FDE68A'}`, borderRadius: '12px', padding: '10px 12px', background: errors.length > 0 ? '#FEF2F2' : '#FFFBEB', color: errors.length > 0 ? '#991B1B' : '#92400E', fontSize: '11px', lineHeight: 1.45, display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {errors.length > 0 && (
                <div>
                    <div style={{ fontWeight: 900, marginBottom: '4px' }}>Fix before applying or saving</div>
                    {errors.map((message) => <div key={`err:${message}`}>{message}</div>)}
                </div>
            )}
            {warnings.length > 0 && (
                <div>
                    <div style={{ fontWeight: 900, marginBottom: '4px' }}>Warnings</div>
                    {warnings.map((message) => <div key={`warn:${message}`}>{message}</div>)}
                </div>
            )}
        </div>
    );
}

function RowDebugInspector({ debug, theme }) {
    if (!debug || typeof debug !== 'object') return null;
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
            {details.map(([label, value]) => (
                <div key={label} style={{ display: 'flex', justifyContent: 'space-between', gap: '10px', fontSize: '11px' }}>
                    <span style={{ color: theme.textSec, fontWeight: 800 }}>{label}</span>
                    <span style={{ color: theme.text, textAlign: 'right', overflowWrap: 'anywhere' }}>{value}</span>
                </div>
            ))}
        </div>
    );
}

function CollapsibleLevelSection({
    title,
    summary,
    theme,
    defaultOpen = false,
    children,
}) {
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
                onClick={() => setOpen((current) => !current)}
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

function OutlineNodeRow({ item, selected, theme, sortByOptions, getFieldLabel = formatDisplayLabel, onSelect }) {
    const node = item.node;
    const title = getNodeTitle(item);
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
    const headerLabel = getNodeHeaderLabel(node);
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

function OutlineBranchRow({ item, selected, theme, onSelect }) {
    const branch = item.branch;
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
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
    const title = node.label && node.label.trim() ? node.label.trim() : fieldLabel;
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
    const inputStyle = inp(theme);
    const title = item && item.levelNumber ? `Level ${item.levelNumber}` : 'Level';
    const hasFilter = Boolean(node.filters && node.filters.clauses && node.filters.clauses.length > 0);
    const fieldLabel = node.field ? getFieldLabel(node.field) : 'Select field';
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
                        {allFields.map((field) => (
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
                            {sortByOptions.map((option) => (
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
                    onChange={(format) => onUpdate({ ...node, format })}
                />
            </CollapsibleLevelSection>

        </div>
    );
}

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
    const [dragIndex, setDragIndex] = useState(null);
    const [rowFilterModalOpen, setRowFilterModalOpen] = useState(false);
    const childRoot = draft && draft.child ? draft.child : createNode('');
    const selectedPath = Array.isArray(draft && draft.selectedPath) ? draft.selectedPath : [];
    const selectedNode = getNodeAtPath(childRoot, selectedPath) || childRoot;
    const rowHasFilter = Boolean(selectedNode.filters && selectedNode.filters.clauses && selectedNode.filters.clauses.length > 0);
    const levelItems = buildDefaultLevelItems(childRoot, draft.childLevelNumber || 2);
    const selectedIndex = Math.max(0, selectedPath.length);
    const selectedLevelNumber = (draft && Number.isFinite(Number(draft.childLevelNumber))
        ? Number(draft.childLevelNumber)
        : 2) + selectedPath.length;
    const inputStyle = inp(theme);
    const rowOverrideSummaryParts = [
        selectedNode.field ? getFieldLabel(selectedNode.field) : 'Select field',
        draft.title && draft.title.trim() ? `Title: ${draft.title.trim()}` : null,
        selectedNode.topN ? `Top ${selectedNode.topN}` : null,
        rowHasFilter ? `${selectedNode.filters.clauses.length} filter${selectedNode.filters.clauses.length === 1 ? '' : 's'}` : null,
    ].filter(Boolean);
    const updateSelectedNode = (nextNode) => {
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, () => nextNode),
        });
    };

    const openDraftLevel = (index) => {
        onChange({
            ...draft,
            selectedPath: levelPathForIndex(index),
        });
    };

    const addNextLevel = () => {
        const nextPath = [...selectedPath, DEFAULT_SEGMENT];
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, (node) => ({
                ...node,
                defaultChild: node.defaultChild || createNode(''),
            })),
            selectedPath: nextPath,
        });
    };

    const removeNextLevel = () => {
        onChange({
            ...draft,
            child: updateNodeAtPath(childRoot, selectedPath, (node) => ({
                ...node,
                defaultChild: null,
            })),
        });
    };

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
                    {levelItems.map((item) => (
                        <LevelRow
                            key={`draft-level:${item.index}:${item.node._id}`}
                            item={item}
                            selected={selectedIndex === item.index}
                            theme={theme}
                            sortByOptions={sortByOptions}
                            getFieldLabel={getFieldLabel}
                            onSelect={() => openDraftLevel(item.index)}
                            onDragStart={(event) => {
                                setDragIndex(item.index);
                                event.dataTransfer.effectAllowed = 'move';
                            }}
                            onDragOver={(event) => event.preventDefault()}
                            onDrop={(event) => {
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
                        {allFields.map((field) => (
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
                            {sortByOptions.map((option) => (
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
                    onChange={(format) => updateSelectedNode({ ...selectedNode, format })}
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
                onSave={(nextCondition) => {
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
    const inputStyle = inp(theme);
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

export function ReportEditor({
    reportDef,
    setReportDef,
    availableFields,
    customDimensions = [],
    theme,
    data = [],
    valConfigs,
    savedReports,
    setSavedReports,
    activeReportId,
    setActiveReportId,
    showNotification,
    requestedSelection = null,
    showReportConfigColumn = true,
    setShowReportConfigColumn,
}) {
    const [reportName, setReportName] = useState('');
    const [selection, setSelection] = useState(null);
    const [editingCondition, setEditingCondition] = useState(null);
    const [editingNodeFilter, setEditingNodeFilter] = useState(null);
    const [rowOverrideDraft, setRowOverrideDraft] = useState(null);
    const [draggingLevelIndex, setDraggingLevelIndex] = useState(null);
    const selectedLineRef = useRef(null);
    const pendingExternalSelectionScrollRef = useRef(false);
    const lastProcessedRequestedSelectionRef = useRef(null);

    const root = useMemo(
        () => (reportDef && reportDef.root ? normalizeNode(reportDef.root) : null),
        [reportDef]
    );
    const allFields = useMemo(
        () => getSelectableFields(availableFields, valConfigs),
        [availableFields, valConfigs]
    );
    const getFieldLabel = useCallback(
        (field) => formatCustomAwareFieldLabel(field, customDimensions),
        [customDimensions]
    );
    const sortByOptions = useMemo(
        () => buildSortOptions(valConfigs),
        [valConfigs]
    );
    const reportValidation = useMemo(
        () => validateReportDefinition(root, { allFields, valConfigs, data }),
        [root, allFields, valConfigs, data]
    );

    const setRoot = useCallback((nextRootOrUpdater) => {
        setReportDef((prev) => {
            const prevDef = prev && typeof prev === 'object' ? prev : {};
            const prevRoot = prevDef.root ? normalizeNode(prevDef.root) : null;
            const nextRoot = typeof nextRootOrUpdater === 'function'
                ? nextRootOrUpdater(prevRoot)
                : nextRootOrUpdater;
            return { ...prevDef, root: nextRoot };
        });
    }, [setReportDef]);

    const outlineItems = useMemo(() => buildOutlineItems(root), [root]);
    const defaultLevelItems = useMemo(() => buildDefaultLevelItems(root), [root]);
    const selectedOutlineItem = useMemo(
        () => findSelectionItem(outlineItems, selection),
        [outlineItems, selection]
    );
    const trailItems = useMemo(
        () => buildTrailItems(outlineItems, selection),
        [outlineItems, selection]
    );
    const directChildItems = useMemo(
        () => buildDirectChildItems(outlineItems, selection),
        [outlineItems, selection]
    );

    const selectedItem = useMemo(() => {
        if (!root || !selection) return null;
        if (selection.type === 'node') {
            const node = getNodeAtPath(root, selection.path);
            return node ? { type: 'node', node } : null;
        }
        if (selection.type === 'branch') {
            const branch = getBranchAtPath(root, selection.nodePath, selection.branchIndex);
            return branch ? { type: 'branch', branch } : null;
        }
        return null;
    }, [root, selection]);

    const editingBranch = useMemo(() => {
        if (!editingCondition || !root) return null;
        return getBranchAtPath(root, editingCondition.nodePath, editingCondition.branchIndex);
    }, [editingCondition, root]);

    const openRowPathOverride = useCallback((request) => {
        if (!request || request.type !== 'rowPath' || !root) return false;

        const nodePath = Array.isArray(request.nodePath) ? request.nodePath : [];
        const node = getNodeAtPath(root, nodePath);
        if (!node) return false;

        const existingBranches = Array.isArray(node.branches) ? node.branches : [];
        const existingIndex = existingBranches.findIndex((branch) => (
            branchMatchesRowPath(branch, request.field, request.value)
        ));
        const existingBranch = existingIndex >= 0 ? existingBranches[existingIndex] : null;
        const baseChild = existingBranch && existingBranch.child
            ? existingBranch.child
            : (node.defaultChild || createNode(''));
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

    const updateNode = useCallback((path, nextNode) => {
        setRoot((prevRoot) => updateNodeAtPath(prevRoot, path, () => nextNode));
    }, [setRoot]);

    const updateBranch = useCallback((nodePath, branchIndex, nextBranch) => {
        setRoot((prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, () => nextBranch));
    }, [setRoot]);

    const addRoot = useCallback((field) => {
        const nextRoot = createNode(field);
        setRoot(nextRoot);
        setRowOverrideDraft(null);
        setSelection({ type: 'node', path: [] });
    }, [setRoot]);

    const openDefaultChild = useCallback((nodePath, hasChild) => {
        if (!hasChild) {
            setRoot((prevRoot) => updateNodeAtPath(prevRoot, nodePath, (node) => ({
                ...node,
                defaultChild: createNode(''),
            })));
        }
        setSelection({ type: 'node', path: [...nodePath, DEFAULT_SEGMENT] });
    }, [setRoot]);

    const removeDefaultChild = useCallback((nodePath) => {
        setRoot((prevRoot) => updateNodeAtPath(prevRoot, nodePath, (node) => ({
            ...node,
            defaultChild: null,
        })));
        setSelection({ type: 'node', path: nodePath });
    }, [setRoot]);

    const openCustomChildren = useCallback((nodePath, branchIndex, hasChild) => {
        if (!hasChild) {
            setRoot((prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, (branch) => ({
                ...branch,
                child: createNode(allFields[0] || ''),
            })));
        }
        setSelection({ type: 'node', path: [...nodePath, createBranchSegment(branchIndex)] });
    }, [allFields, setRoot]);

    const removeCustomChildren = useCallback((nodePath, branchIndex) => {
        setRoot((prevRoot) => updateBranchAtPath(prevRoot, nodePath, branchIndex, (branch) => ({
            ...branch,
            child: null,
        })));
        setSelection({ type: 'branch', nodePath, branchIndex });
    }, [setRoot]);

    const removeNode = useCallback((path) => {
        if (path.length === 0) {
            setRoot(null);
            setRowOverrideDraft(null);
            setSelection(null);
            return;
        }

        const parentPath = path.slice(0, -1);
        const segment = path[path.length - 1];

        if (segment === DEFAULT_SEGMENT) {
            setRoot((prevRoot) => updateNodeAtPath(prevRoot, parentPath, (node) => ({
                ...node,
                defaultChild: null,
            })));
            setSelection({ type: 'node', path: parentPath });
            return;
        }

        const branchIndex = parseBranchIndex(segment);
        if (branchIndex >= 0) {
            setRoot((prevRoot) => updateBranchAtPath(prevRoot, parentPath, branchIndex, (branch) => ({
                ...branch,
                child: null,
            })));
            setSelection({ type: 'branch', nodePath: parentPath, branchIndex });
        }
    }, [setRoot]);

    const removeBranch = useCallback((nodePath, branchIndex) => {
        setRoot((prevRoot) => updateNodeAtPath(prevRoot, nodePath, (node) => ({
            ...node,
            branches: node.branches.filter((_, index) => index !== branchIndex),
        })));
        setSelection({ type: 'node', path: nodePath });
    }, [setRoot]);

    const selectDefaultLevel = useCallback((path) => {
        setRowOverrideDraft(null);
        setSelection({ type: 'node', path });
    }, []);

    const selectOutlineItem = useCallback((item) => {
        if (!item) return;
        setRowOverrideDraft(null);
        if (item.kind === 'branch') {
            setSelection({ type: 'branch', nodePath: item.nodePath, branchIndex: item.branchIndex });
            return;
        }
        setSelection({ type: 'node', path: item.nodePath });
    }, []);

    const addNextDefaultLevel = useCallback(() => {
        if (!root || !defaultLevelItems.length) return;
        const deepestItem = defaultLevelItems[defaultLevelItems.length - 1];
        if (!deepestItem || !deepestItem.node || !deepestItem.node.field) return;
        const nextPath = [...deepestItem.path, DEFAULT_SEGMENT];
        setRowOverrideDraft(null);
        setRoot((prevRoot) => updateNodeAtPath(prevRoot, deepestItem.path, (node) => ({
            ...node,
            defaultChild: node.defaultChild || createNode(''),
        })));
        setSelection({ type: 'node', path: nextPath });
    }, [defaultLevelItems, root, setRoot]);

    const reorderLevels = useCallback((fromIndex, toIndex) => {
        if (fromIndex === null || fromIndex === toIndex) return;
        setRoot((prevRoot) => reorderDefaultChain(prevRoot, fromIndex, toIndex));
        setSelection({ type: 'node', path: levelPathForIndex(toIndex) });
        setDraggingLevelIndex(null);
    }, [setRoot]);

    const closeRowOverrideDraft = useCallback(() => {
        setRowOverrideDraft(null);
        setSelection(null);
    }, []);

    const applySelectedLevel = useCallback(() => {
        if (!reportValidation.valid) {
            if (showNotification) showNotification(reportValidation.errors[0] || 'Fix report validation errors before applying.', 'error');
            return;
        }
        setSelection(null);
    }, [reportValidation, showNotification]);

    const applyRowOverrideDraft = useCallback(() => {
        if (!rowOverrideDraft || !rowOverrideDraft.request) return;
        const request = rowOverrideDraft.request;
        const nodePath = Array.isArray(rowOverrideDraft.nodePath) ? rowOverrideDraft.nodePath : [];
        const nextChild = pruneDraftChild(rowOverrideDraft.child);
        const nextBranch = {
            ...createRowPathBranch(request.field, request.value, rowOverrideDraft.title || rowOverrideDraft.rowLabel, ''),
            label: normalizeRowPathValue(rowOverrideDraft.title || rowOverrideDraft.rowLabel || request.value),
            sourceRowPath: Array.isArray(request.pathValues) ? request.pathValues.map(String).filter(Boolean) : [],
            sourcePathFields: Array.isArray(request.pathFields) ? request.pathFields.map(String).filter(Boolean) : [],
            child: nextChild,
        };

        const applyBranch = (node) => {
            const branches = Array.isArray(node.branches) ? node.branches : [];
            const existingIndex = branches.findIndex((branch) => (
                branchMatchesRowPath(branch, request.field, request.value)
            ));
            if (existingIndex >= 0) {
                return {
                    ...node,
                    branches: branches.map((branch, index) => (
                        index === existingIndex ? { ...nextBranch, _id: branch._id || nextBranch._id } : branch
                    )),
                };
            }
            return {
                ...node,
                branches: [...branches, nextBranch],
            };
        };
        const projectedRoot = updateNodeAtPath(root, nodePath, applyBranch);
        const validation = validateReportDefinition(projectedRoot, { allFields, valConfigs, data });
        if (!validation.valid) {
            if (showNotification) showNotification(validation.errors[0] || 'Fix report validation errors before applying.', 'error');
            return;
        }

        setRoot((prevRoot) => updateNodeAtPath(prevRoot, nodePath, applyBranch));

        setRowOverrideDraft(null);
        setSelection(null);
        if (showNotification) showNotification(`Applied settings for ${rowOverrideDraft.rowLabel || 'row'}`, 'info');
    }, [allFields, data, root, rowOverrideDraft, setRoot, showNotification, valConfigs]);

    const saveReport = useCallback(() => {
        if (!root || !root.field) return;
        if (!reportValidation.valid) {
            if (showNotification) showNotification(reportValidation.errors[0] || 'Fix report validation errors before saving.', 'error');
            return;
        }

        const id = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        const name = reportName.trim() || `Report ${(savedReports || []).length + 1}`;
        setSavedReports([
            ...(savedReports || []),
            {
                id,
                name,
                reportDef: JSON.parse(JSON.stringify(reportDef)),
                createdAt: new Date().toISOString(),
            },
        ]);
        setActiveReportId(id);
        setReportName('');
        if (showNotification) showNotification(`Report "${name}" saved`, 'info');
    }, [reportDef, reportName, reportValidation, root, savedReports, setActiveReportId, setSavedReports, showNotification]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '18px', padding: '2px 0 8px' }}>
            {(savedReports || []).length > 0 && (
                <div>
                    <div style={sectionLabelStyle(theme)}><Icons.Save /> Saved Reports</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                        {(savedReports || []).map((report) => (
                            <div
                                key={report.id}
                                onClick={() => {
                                    setReportDef(JSON.parse(JSON.stringify(report.reportDef)));
                                    setActiveReportId(report.id);
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
                                        setSavedReports((prev) => (prev || []).filter((current) => current.id !== report.id));
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
                                {allFields.map((field) => (
                                    <option key={field} value={field}>{getFieldLabel(field)}</option>
                                ))}
                            </select>
                        </div>
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {defaultLevelItems.map((item) => (
                            <LevelRow
                                key={`default-level:${item.index}:${item.node._id}`}
                                item={item}
                                selected={!rowOverrideDraft && selectionKey(selection) === selectionKey({ type: 'node', path: item.path })}
                                theme={theme}
                                sortByOptions={sortByOptions}
                                getFieldLabel={getFieldLabel}
                                onSelect={() => selectDefaultLevel(item.path)}
                                onDragStart={(event) => {
                                    setDraggingLevelIndex(item.index);
                                    event.dataTransfer.effectAllowed = 'move';
                                }}
                                onDragOver={(event) => event.preventDefault()}
                                onDrop={(event) => {
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
                        {outlineItems.map((item) => {
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
                            item={defaultLevelItems.find((item) => isSamePath(item.path, selection.path)) || (selectedOutlineItem
                                ? { ...selectedOutlineItem, path: selectedOutlineItem.nodePath }
                                : { path: selection.path, levelNumber: selection.path.length + 1 })}
                            node={selectedItem.node}
                            allFields={allFields}
                            getFieldLabel={getFieldLabel}
                            sortByOptions={sortByOptions}
                            theme={theme}
                            onUpdate={(nextNode) => updateNode(selection.path, nextNode)}
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
                            onUpdate={(nextBranch) => updateBranch(selection.nodePath, selection.branchIndex, nextBranch)}
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
                    onSave={(nextCondition) => {
                        updateBranch(editingCondition.nodePath, editingCondition.branchIndex, {
                            ...editingBranch,
                            condition: nextCondition,
                        });
                    }}
                    onClose={() => setEditingCondition(null)}
                />
            )}

            {editingNodeFilter && (() => {
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
                        onSave={(nextCondition) => {
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

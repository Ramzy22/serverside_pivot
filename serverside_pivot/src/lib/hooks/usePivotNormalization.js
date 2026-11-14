/**
 * Pivot Normalization Utilities
 * 
 * Pure utility functions for normalizing and validating pivot table inputs.
 * These functions have no React dependencies and can be used anywhere.
 * 
 * Extracted from DashTanstackPivot.react.js (Phase 1 of refactoring)
 */

import { formatDisplayLabel, formatAggLabel } from '../utils/helpers';

// ─── Chart Constants ───────────────────────────────────────────────────────

export const DEFAULT_CHART_PANEL_ROW_LIMIT = 50;
export const DEFAULT_CHART_PANEL_COLUMN_LIMIT = 10;
export const DEFAULT_CHART_GRAPH_HEIGHT = 320;
export const DEFAULT_FLOATING_CHART_PANEL_HEIGHT = 520;
export const CHART_PANEL_VERTICAL_CHROME_HEIGHT = 220;
export const MIN_CHART_PANEL_WIDTH = 280;
export const MAX_CHART_PANEL_WIDTH = 960;
export const MIN_FLOATING_CHART_PANEL_HEIGHT = 280;
export const MIN_TABLE_PANEL_WIDTH = 0;
export const MIN_CHART_CANVAS_PANE_WIDTH = 320;
export const DEFAULT_DOCKED_CHART_PANE_HEIGHT = 420;
export const MIN_DOCKED_CHART_PANE_HEIGHT = 180;
export const MIN_TABLE_PANEL_HEIGHT = 200;
/** @type {any} */
export const VALID_CHART_DOCK_POSITIONS = new Set(['left', 'right', 'top', 'bottom']);
export const DEFAULT_TABLE_CANVAS_SIZE = 1.4;
export const TABLE_OVERLAY_CHART_PANE_ID = '__table_overlay_chart__';
export const MAX_AUTO_SIZE_SAMPLE_ROWS = 300;
export const MAX_PENDING_ROW_TRANSITIONS = 128;
/** @type {any} */
export const VALID_CHART_TYPES = new Set(['bar', 'bar3d', 'line3d', 'scatter3d', 'range', 'line', 'area', 'sparkline', 'combo', 'pie', 'donut', 'scatter', 'waterfall', 'icicle', 'sunburst', 'sankey', 'heatmap', 'bubble', 'radar', 'funnel', 'histogram', 'boxplot', 'nightingale']);
/** @type {any} */
export const VALID_CHART_SORT_MODES = new Set(['natural', 'value_desc', 'value_asc', 'label_asc', 'label_desc']);
/** @type {any} */
export const VALID_CHART_INTERACTION_MODES = new Set(['focus', 'filter', 'event']);
/** @type {any} */
export const VALID_CHART_SERVER_SCOPES = new Set(['viewport', 'root']);

// ─── View/Detail Mode Constants ────────────────────────────────────────────

/** @type {any} */
export const VALID_VIEW_MODES = new Set(['pivot', 'report', 'tree', 'table']);
/** @type {any} */
export const VALID_DETAIL_MODES = new Set(['none', 'inline', 'sidepanel', 'drawer']);
/** @type {any} */
export const VALID_TREE_DISPLAY_MODES = new Set(['singleColumn', 'multipleColumns']);
/** @type {any} */
export const VALID_DETAIL_REFRESH_STRATEGIES = new Set(['rows', 'everything', 'nothing']);
/** @type {any} */
export const VALID_TRANSACTION_REFRESH_MODES = new Set(['none', 'viewport', 'smart', 'structural', 'full', 'patch']);
/** @type {any} */
export const VALID_TRANSACTION_EVENT_ACTIONS = new Set(['undo', 'redo', 'revert', 'replace']);
export const MAX_TRANSACTION_HISTORY_ENTRIES = 100;
export const SOFT_CENTER_COLUMN_WARNING_THRESHOLD = 2000;
export const HARD_CENTER_COLUMN_WARNING_THRESHOLD = 10000;
/** @type {any} */
export const SUPPORTED_DEFAULT_NUMBER_FORMATS = new Set(['', 'currency', 'accounting', 'percent', 'scientific']);
export const GRAND_TOTAL_ROW_ID = '__grand_total__';

/** @param {any} [row] @returns {any} */
export const isGrandTotalRow = (row) => {
    if (!row || typeof row !== 'object') return false;
    /** @type {any} */
    const target = row.original && typeof row.original === 'object' ? row.original : row;
    return !!(
        target.__isGrandTotal__ === true ||
        target._path === GRAND_TOTAL_ROW_ID ||
        target._id === 'Grand Total' ||
        row.id === GRAND_TOTAL_ROW_ID ||
        (target._isTotal && !target.__isSubtotal__ && !target._isTabularSubtotal && !target._isSubtotalFooter && (!target._path || target._path === GRAND_TOTAL_ROW_ID))
    );
};

/** @param {any} [row] @returns {any} */
export const isSubtotalRow = (row) => {
    if (!row || typeof row !== 'object') return false;
    /** @type {any} */
    const target = row.original && typeof row.original === 'object' ? row.original : row;
    return !!(
        target.__isSubtotal__ === true ||
        target._isTabularSubtotal === true ||
        target._isSubtotalFooter === true ||
        (target._path && String(target._path).includes('__subtotal__')) ||
        (target._isTotal && !isGrandTotalRow(target))
    );
};
/** @type {any} */
export const MISSING_PERSISTED_VALUE = Symbol('missing-persisted-value');
export const CUSTOM_CATEGORY_FIELD_PREFIX = '__custom_category__';
/** @type {any} */
export const CUSTOM_DIMENSION_KINDS = new Set(['category', 'date_group', 'number_group']);
/** @type {any} */
export const DATE_GROUP_GRANULARITIES = new Set(['year', 'quarter', 'month', 'week', 'day']);
/** @type {any} */
export const CUSTOM_CATEGORY_OPERATORS = new Set([
    'eq',
    'ne',
    'not_eq',
    'contains',
    'starts_with',
    'ends_with',
    'gt',
    'gte',
    'lt',
    'lte',
    'in',
    'not_in',
    'is_null',
    'is_not_null',
]);

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeCustomCategoryId = (value, fallback = 'category') => {
    /** @type {any} */
    const raw = value === undefined || value === null ? '' : String(value);
    /** @type {any} */
    const normalized = raw
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/^_+|_+$/g, '');
    /** @type {any} */
    const fallbackBase = String(fallback || 'category')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/^_+|_+$/g, '') || 'category';
    /** @type {any} */
    const base = normalized || fallbackBase;
    return /^[a-z_]/.test(base) ? base : `c_${base}`;
};

/** @param {any} [id] @returns {any} */
export const buildCustomCategoryFieldId = (id) => (
    `${CUSTOM_CATEGORY_FIELD_PREFIX}${normalizeCustomCategoryId(id)}`
);

/** @param {any} [field] @returns {any} */
export const isCustomCategoryField = (field) => (
    typeof field === 'string' && field.startsWith(CUSTOM_CATEGORY_FIELD_PREFIX)
);

/** @param {any} [value] @returns {any} */
const normalizeCustomDimensionKind = (value) => {
    /** @type {any} */
    const normalized = String(value || 'category').trim().toLowerCase().replace(/[\s-]+/g, '_');
    if (normalized === 'date' || normalized === 'date_bucket' || normalized === 'date_grouping') return 'date_group';
    if (normalized === 'number' || normalized === 'numeric' || normalized === 'numeric_bucket' || normalized === 'number_bucket' || normalized === 'number_grouping') return 'number_group';
    return CUSTOM_DIMENSION_KINDS.has(normalized) ? normalized : 'category';
};

/** @param {any} [value] @returns {any} */
const normalizeDateGroupGranularity = (value) => {
    /** @type {any} */
    const normalized = String(value || 'month').trim().toLowerCase().replace(/[\s-]+/g, '_');
    if (normalized === 'yyyy' || normalized === 'yr') return 'year';
    if (normalized === 'q' || normalized === 'qtr') return 'quarter';
    if (normalized === 'mon' || normalized === 'year_month' || normalized === 'ym') return 'month';
    if (normalized === 'weekday') return 'day';
    return DATE_GROUP_GRANULARITIES.has(normalized) ? normalized : 'month';
};

/** @param {any} [value] @returns {any} */
const normalizeDateGroupFiscalStartMonth = (value) => {
    /** @type {any} */
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return 1;
    return Math.max(1, Math.min(12, Math.floor(numeric)));
};

/** @param {any} [value] @returns {any} */
const parseCustomGroupDate = (value) => {
    if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
    if (value === null || value === undefined || value === '') return null;
    /** @type {any} */
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
};

/** @param {any} [value] @returns {any} */
const pad2 = (value) => String(value).padStart(2, '0');

/** @param {any} [date] @returns {any} */
const getIsoWeekParts = (date) => {
    /** @type {any} */
    const utcDate = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    /** @type {any} */
    const dayNum = utcDate.getUTCDay() || 7;
    utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
    /** @type {any} */
    const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
    /** @type {any} */
    const week = Math.ceil((((utcDate - yearStart) / 86400000) + 1) / 7);
    return { year: utcDate.getUTCFullYear(), week };
};

/** @param {any} [value] @param {any} [precision] @returns {any} */
const formatNumberGroupBoundary = (value, precision = null) => {
    /** @type {any} */
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return '';
    /** @type {any} */
    const resolvedPrecision = precision === null || precision === undefined
        ? (Math.abs(numeric) < 10 && !Number.isInteger(numeric) ? 2 : 0)
        : Math.max(0, Math.min(8, Math.floor(Number(precision) || 0)));
    return numeric.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: resolvedPrecision,
    });
};

/** @param {any} [row] @param {any} [dimension] @returns {any} */
export const evaluateCustomDateGroupDimension = (row, dimension) => {
    /** @type {any} */
    const sourceField = dimension && dimension.sourceField;
    /** @type {any} */
    const date = parseCustomGroupDate(sourceField ? row && row[sourceField] : null);
    if (!date) return (dimension && dimension.fallbackLabel) || 'No date';
    /** @type {any} */
    const granularity = normalizeDateGroupGranularity(dimension && dimension.granularity);
    /** @type {any} */
    const fiscalStartMonth = normalizeDateGroupFiscalStartMonth(dimension && dimension.fiscalYearStartMonth);
    /** @type {any} */
    const year = date.getFullYear();
    /** @type {any} */
    const monthIndex = date.getMonth();
    /** @type {any} */
    const month = monthIndex + 1;
    if (granularity === 'year') return String(year);
    if (granularity === 'quarter') {
        if (fiscalStartMonth !== 1) {
            /** @type {any} */
            const fiscalMonthIndex = (monthIndex - (fiscalStartMonth - 1) + 12) % 12;
            /** @type {any} */
            const fiscalQuarter = Math.floor(fiscalMonthIndex / 3) + 1;
            /** @type {any} */
            const fiscalYear = month >= fiscalStartMonth ? year + 1 : year;
            return `FY${fiscalYear} Q${fiscalQuarter}`;
        }
        return `${year} Q${Math.floor(monthIndex / 3) + 1}`;
    }
    if (granularity === 'week') {
        /** @type {any} */
        const { year: weekYear, week } = getIsoWeekParts(date);
        return `${weekYear} W${pad2(week)}`;
    }
    if (granularity === 'day') return `${year}-${pad2(month)}-${pad2(date.getDate())}`;
    return `${year}-${pad2(month)}`;
};

/** @param {any} [row] @param {any} [dimension] @returns {any} */
export const evaluateCustomNumberGroupDimension = (row, dimension) => {
    /** @type {any} */
    const sourceField = dimension && dimension.sourceField;
    /** @type {any} */
    const value = Number(sourceField ? row && row[sourceField] : null);
    if (!Number.isFinite(value)) return (dimension && dimension.fallbackLabel) || 'No value';
    /** @type {any} */
    const binSize = Math.abs(Number(dimension && dimension.binSize));
    if (!Number.isFinite(binSize) || binSize <= 0) return (dimension && dimension.fallbackLabel) || 'No value';
    /** @type {any} */
    const precision = dimension && dimension.precision !== null && dimension.precision !== undefined
        ? Math.max(0, Math.min(8, Math.floor(Number(dimension.precision) || 0)))
        : null;
    /** @type {any} */
    const hasMin = dimension && dimension.minValue !== null && dimension.minValue !== undefined && Number.isFinite(Number(dimension.minValue));
    /** @type {any} */
    const hasMax = dimension && dimension.maxValue !== null && dimension.maxValue !== undefined && Number.isFinite(Number(dimension.maxValue));
    /** @type {any} */
    const minValue = hasMin ? Number(dimension.minValue) : null;
    /** @type {any} */
    const maxValue = hasMax ? Number(dimension.maxValue) : null;
    if (hasMin && value < minValue) return `< ${formatNumberGroupBoundary(minValue, precision)}`;
    if (hasMax && value >= maxValue) return `>= ${formatNumberGroupBoundary(maxValue, precision)}`;
    /** @type {any} */
    const anchor = hasMin ? minValue : (Number.isFinite(Number(dimension && dimension.startValue)) ? Number(dimension.startValue) : 0);
    /** @type {any} */
    const binStart = anchor + Math.floor((value - anchor) / binSize) * binSize;
    /** @type {any} */
    const binEnd = binStart + binSize;
    return `${formatNumberGroupBoundary(binStart, precision)} - < ${formatNumberGroupBoundary(binEnd, precision)}`;
};

/** @param {any} [dimension] @returns {any} */
export const getCustomDimensionFieldId = (dimension) => {
    if (!dimension || typeof dimension !== 'object') return '';
    if (typeof dimension.field === 'string' && isCustomCategoryField(dimension.field)) {
        return dimension.field;
    }
    return buildCustomCategoryFieldId(dimension.id || dimension.name || dimension.label || 'category');
};

/** @param {any} [value] @returns {any} */
const normalizeCustomCategoryOperator = (value) => {
    /** @type {any} */
    const raw = String(value || 'eq').trim();
    if (raw === '=') return 'eq';
    if (raw === '!=') return 'ne';
    /** @type {any} */
    const normalized = raw.replace(/\s+/g, '_').toLowerCase();
    if (normalized === 'startswith') return 'starts_with';
    if (normalized === 'endswith') return 'ends_with';
    if (normalized === 'notin') return 'not_in';
    if (normalized === 'isnull') return 'is_null';
    if (normalized === 'isnotnull') return 'is_not_null';
    return CUSTOM_CATEGORY_OPERATORS.has(normalized) ? normalized : 'eq';
};

/** @param {any} [clause] @param {any} [fallbackField] @returns {any} */
const normalizeCustomCategoryClause = (clause, fallbackField = '') => {
    if (!clause || typeof clause !== 'object') return null;
    /** @type {any} */
    const field = typeof clause.field === 'string' && clause.field.trim()
        ? clause.field.trim()
        : fallbackField;
    if (!field) return null;
    /** @type {any} */
    const operator = normalizeCustomCategoryOperator(clause.operator || clause.op || clause.type);
    /** @type {any} */
    const rawValues = Array.isArray(clause.values)
        ? clause.values
        : (Array.isArray(clause.value)
            ? clause.value
            : (['in', 'not_in'].includes(operator) && typeof clause.value === 'string'
                ? clause.value.split(',')
                : []));
    /** @type {any} */
    const values = rawValues
        .map(/** @param {any} [item] @returns {any} */ (item) => (item === undefined || item === null ? '' : String(item).trim()))
        .filter(/** @param {any} [item] @returns {any} */ (item) => item !== '');
    /** @type {any} */
    const value = clause.value === undefined || clause.value === null ? '' : String(clause.value);
    return {
        field,
        operator,
        value,
        values,
        caseSensitive: clause.caseSensitive === true,
    };
};

/** @param {any} [condition] @param {any} [fallbackField] @returns {any} */
export const normalizeCustomCategoryCondition = (condition, fallbackField = '') => {
    /** @type {any} */
    const rawCondition = condition && typeof condition === 'object' ? condition : {};
    /** @type {any} */
    const op = String(rawCondition.op || rawCondition.operator || 'AND').trim().toUpperCase() === 'OR' ? 'OR' : 'AND';
    /** @type {any} */
    const sourceClauses = Array.isArray(rawCondition.clauses)
        ? rawCondition.clauses
        : (Array.isArray(rawCondition.conditions) ? rawCondition.conditions : []);
    /** @type {any} */
    const clauses = sourceClauses
        .map(/** @param {any} [clause] @returns {any} */ (clause) => normalizeCustomCategoryClause(clause, fallbackField))
        .filter(Boolean);
    if (clauses.length === 0 && (rawCondition.field || fallbackField)) {
        /** @type {any} */
        const singleClause = normalizeCustomCategoryClause(rawCondition, fallbackField);
        if (singleClause) clauses.push(singleClause);
    }
    return { op, clauses };
};

/** @param {any} [value] @returns {any} */
export const normalizeCustomDimensionsValue = (value) => {
    if (!Array.isArray(value)) return [];
    /** @type {any} */
    const usedFields = new Set();
    return value.map((entry, index) => {
        if (!entry || typeof entry !== 'object') return null;
        /** @type {any} */
        const kind = normalizeCustomDimensionKind(entry.kind || entry.type || entry.mode);
        /** @type {any} */
        const rawName = typeof entry.name === 'string' && entry.name.trim()
            ? entry.name.trim()
            : (typeof entry.label === 'string' && entry.label.trim() ? entry.label.trim() : `Custom Category ${index + 1}`);
        /** @type {any} */
        const id = normalizeCustomCategoryId(entry.id || rawName, `category_${index + 1}`);
        /** @type {any} */
        let field = getCustomDimensionFieldId({ ...entry, id, name: rawName });
        if (usedFields.has(field)) {
            field = buildCustomCategoryFieldId(`${id}_${index + 1}`);
        }
        usedFields.add(field);
        /** @type {any} */
        const fallbackLabel = typeof entry.fallbackLabel === 'string' && entry.fallbackLabel.trim()
            ? entry.fallbackLabel.trim()
            : (typeof entry.defaultLabel === 'string' && entry.defaultLabel.trim() ? entry.defaultLabel.trim() : (kind === 'date_group' ? 'No date' : (kind === 'number_group' ? 'No value' : 'Other')));
        if (kind === 'date_group') {
            /** @type {any} */
            const sourceField = typeof entry.sourceField === 'string' && entry.sourceField.trim()
                ? entry.sourceField.trim()
                : (typeof entry.baseField === 'string' && entry.baseField.trim() ? entry.baseField.trim() : '');
            if (!sourceField) return null;
            return {
                id,
                field,
                name: rawName,
                kind,
                sourceField,
                granularity: normalizeDateGroupGranularity(entry.granularity || entry.datePart),
                fiscalYearStartMonth: normalizeDateGroupFiscalStartMonth(entry.fiscalYearStartMonth),
                fallbackLabel,
                order: 'calendar',
            };
        }
        if (kind === 'number_group') {
            /** @type {any} */
            const sourceField = typeof entry.sourceField === 'string' && entry.sourceField.trim()
                ? entry.sourceField.trim()
                : (typeof entry.baseField === 'string' && entry.baseField.trim() ? entry.baseField.trim() : '');
            if (!sourceField) return null;
            /** @type {any} */
            const binSize = Math.abs(Number(entry.binSize || entry.interval || entry.step));
            if (!Number.isFinite(binSize) || binSize <= 0) return null;
            /** @param {any} [candidate] @returns {any} */
            const parseOptionalNumber = (candidate) => {
                if (candidate === '' || candidate === null || candidate === undefined) return null;
                /** @type {any} */
                const numeric = Number(candidate);
                return Number.isFinite(numeric) ? numeric : null;
            };
            return {
                id,
                field,
                name: rawName,
                kind,
                sourceField,
                binSize,
                startValue: parseOptionalNumber(entry.startValue),
                minValue: parseOptionalNumber(entry.minValue),
                maxValue: parseOptionalNumber(entry.maxValue),
                precision: parseOptionalNumber(entry.precision),
                fallbackLabel,
                order: 'numeric',
            };
        }
        /** @type {any} */
        const rules = (Array.isArray(entry.rules) ? entry.rules : [])
            .map(/** @param {any} [rule] @param {any} [ruleIndex] @returns {any} */ (rule, ruleIndex) => {
                if (!rule || typeof rule !== 'object') return null;
                /** @type {any} */
                const label = typeof rule.label === 'string' && rule.label.trim()
                    ? rule.label.trim()
                    : `Rule ${ruleIndex + 1}`;
                /** @type {any} */
                const condition = normalizeCustomCategoryCondition(rule.condition || rule, rule.field || '');
                if (!condition.clauses.length) return null;
                return {
                    id: typeof rule.id === 'string' && rule.id.trim()
                        ? rule.id.trim()
                        : `${id}_rule_${ruleIndex + 1}`,
                    label,
                    condition,
                };
            })
            .filter(Boolean);
        /** @type {any[]} */
        const VALID_ORDERS = ['creation', 'alpha', 'alpha_desc', 'numeric', 'numeric_desc'];
        return {
            id,
            field,
            name: rawName,
            kind: 'category',
            fallbackLabel: typeof entry.fallbackLabel === 'string' && entry.fallbackLabel.trim()
                ? entry.fallbackLabel.trim()
                : (typeof entry.defaultLabel === 'string' && entry.defaultLabel.trim() ? entry.defaultLabel.trim() : 'Other'),
            rules,
            order: typeof entry.order === 'string' && VALID_ORDERS.includes(entry.order) ? entry.order : 'creation',
        };
    }).filter(Boolean);
};

/** @param {any} [customDimensions] @param {any} [field] @returns {any} */
export const getCustomDimensionLabel = (customDimensions, field) => {
    if (!isCustomCategoryField(field)) return '';
    /** @type {any} */
    const match = (Array.isArray(customDimensions) ? customDimensions : [])
        .find((dimension) => getCustomDimensionFieldId(dimension) === field);
    return match && match.name ? match.name : '';
};

/** @param {any} [field] @param {any} [customDimensions] @returns {any} */
export const formatCustomAwareFieldLabel = (field, customDimensions = []) => (
    getCustomDimensionLabel(customDimensions, field) || formatDisplayLabel(field)
);

/** @param {any} [value] @returns {any} */
const isBlankCustomCategoryValue = (value) => (
    value === null || value === undefined || (typeof value === 'string' && value.trim() === '')
);

/** @param {any} [value] @param {any} [caseSensitive] @returns {any} */
const normalizeCustomCategoryComparable = (value, caseSensitive = false) => {
    /** @type {any} */
    const text = value === null || value === undefined ? '' : String(value);
    return caseSensitive ? text : text.toLowerCase();
};

/** @param {any} [row] @param {any} [clause] @returns {any} */
const evaluateCustomCategoryClause = (row, clause) => {
    if (!row || !clause || !clause.field) return false;
    /** @type {any} */
    const operator = normalizeCustomCategoryOperator(clause.operator || clause.op || clause.type);
    const rowValue = row[clause.field];

    if (operator === 'is_null') return isBlankCustomCategoryValue(rowValue);
    if (operator === 'is_not_null') return !isBlankCustomCategoryValue(rowValue);

    /** @type {any} */
    const caseSensitive = clause.caseSensitive === true;
    const rawValue = clause.value;
    /** @type {any} */
    const left = normalizeCustomCategoryComparable(rowValue, caseSensitive);
    /** @type {any} */
    const right = normalizeCustomCategoryComparable(rawValue, caseSensitive);

    if (operator === 'eq') return left === right;
    if (operator === 'ne' || operator === 'not_eq') return left !== right;
    if (operator === 'contains') return left.includes(right);
    if (operator === 'starts_with') return left.startsWith(right);
    if (operator === 'ends_with') return left.endsWith(right);

    if (operator === 'in' || operator === 'not_in') {
        /** @type {any} */
        const rawValues = Array.isArray(clause.values)
            ? clause.values
            : (Array.isArray(clause.value) ? clause.value : []);
        /** @type {any} */
        const candidates = rawValues.map(/** @param {any} [item] @returns {any} */ (item) => normalizeCustomCategoryComparable(item, caseSensitive));
        /** @type {any} */
        const matched = candidates.includes(left);
        return operator === 'not_in' ? !matched : matched;
    }

    /** @type {any} */
    const leftNumber = Number(rowValue);
    /** @type {any} */
    const rightNumber = Number(rawValue);
    /** @type {any} */
    const hasNumericPair = Number.isFinite(leftNumber) && Number.isFinite(rightNumber);
    /** @type {any} */
    const compareLeft = hasNumericPair ? leftNumber : left;
    /** @type {any} */
    const compareRight = hasNumericPair ? rightNumber : right;
    if (operator === 'gt') return compareLeft > compareRight;
    if (operator === 'gte') return compareLeft >= compareRight;
    if (operator === 'lt') return compareLeft < compareRight;
    if (operator === 'lte') return compareLeft <= compareRight;

    return false;
};

/** @param {any} [row] @param {any} [condition] @returns {any} */
export const evaluateCustomCategoryCondition = (row, condition) => {
    /** @type {any} */
    const normalized = normalizeCustomCategoryCondition(condition);
    if (!normalized.clauses.length) return false;
    /** @type {any} */
    const results = normalized.clauses.map(/** @param {any} [clause] @returns {any} */ (clause) => evaluateCustomCategoryClause(row, clause));
    return normalized.op === 'OR' ? results.some(Boolean) : results.every(Boolean);
};

/** @param {any} [row] @param {any} [dimension] @returns {any} */
export const evaluateCustomCategoryDimension = (row, dimension) => {
    const normalized = normalizeCustomDimensionsValue([dimension])[0];
    if (!normalized) return '';
    if (normalized.kind === 'date_group') {
        return evaluateCustomDateGroupDimension(row, normalized);
    }
    if (normalized.kind === 'number_group') {
        return evaluateCustomNumberGroupDimension(row, normalized);
    }
    /** @type {any} */
    const matchingRule = normalized.rules.find(/** @param {any} [rule] @returns {any} */ (rule) => (
        evaluateCustomCategoryCondition(row, rule.condition)
    ));
    return matchingRule ? matchingRule.label : normalized.fallbackLabel;
};

/** @param {any} [rows] @param {any} [customDimensions] @returns {any} */
export const applyCustomDimensionsToRows = (rows, customDimensions = []) => {
    if (!Array.isArray(rows) || rows.length === 0) return Array.isArray(rows) ? rows : [];
    /** @type {any} */
    const dimensions = normalizeCustomDimensionsValue(customDimensions);
    if (!dimensions.length) return rows;
    return rows.map((row) => {
        if (!row || typeof row !== 'object') return row;
        /** @type {Record<string, any>} */
        const nextRow = { ...row };
        dimensions.forEach(/** @param {any} [dimension] @returns {any} */ (dimension) => {
            nextRow[dimension.field] = evaluateCustomCategoryDimension(nextRow, dimension);
        });
        return nextRow;
    });
};

/** @param {any} [availableFields] @param {any} [customDimensions] @returns {any} */
export const decorateAvailableFieldsWithCustomDimensions = (availableFields, customDimensions = []) => {
    /** @type {any} */
    const fields = Array.isArray(availableFields) ? availableFields.filter(Boolean) : [];
    /** @type {any[]} */
    const next = [...fields];
    /** @type {any} */
    const seen = new Set(next);
    (Array.isArray(customDimensions) ? customDimensions : []).forEach((dimension) => {
        /** @type {any} */
        const field = getCustomDimensionFieldId(dimension);
        if (field && !seen.has(field)) {
            seen.add(field);
            next.push(field);
        }
    });
    return next;
};

// ─── Chart Utilities ───────────────────────────────────────────────────────

/** @param {any} [columnFields] @returns {any} */
export const getPreferredChartOrientation = (columnFields) => (
    Array.isArray(columnFields) && columnFields.length > 0 ? 'columns' : 'rows'
);

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const sanitizeChartDefinitionName = (value, fallback = 'Chart') => {
    /** @type {any} */
    const text = typeof value === 'string' ? value.trim() : '';
    return text || fallback;
};

/** @param {any} [prefix] @returns {any} */
export const createChartDefinitionId = (prefix = 'chart') => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const cloneSerializable = (value, fallback = null) => {
    if (value === undefined) return fallback;
    try {
        return JSON.parse(JSON.stringify(value));
    } catch (error) {
        return fallback;
    }
};

// ─── View/Detail Mode Normalization ────────────────────────────────────────

/** @param {any} [value] @returns {any} */
export const normalizeViewModeValue = (value) => (
    VALID_VIEW_MODES.has(value) ? value : null
);

/** @param {any} [value] @returns {any} */
export const normalizeDetailModeValue = (value) => (
    VALID_DETAIL_MODES.has(value) ? value : null
);

/** @param {any} [value] @returns {any} */
export const normalizeTreeSourceTypeValue = (value) => {
    /** @type {any} */
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (normalized === 'path' || normalized === 'data_path' || normalized === 'datapath') return 'path';
    if (normalized === 'parentid' || normalized === 'parent_id' || normalized === 'adjacency') return 'adjacency';
    if (normalized === 'nested' || normalized === 'children') return 'nested';
    return 'adjacency';
};

/** @param {any} [value] @returns {any} */
export const normalizeTreeDisplayModeValue = (value) => {
    if (typeof value !== 'string') return 'singleColumn';
    /** @type {any} */
    const normalized = value.trim().toLowerCase();
    if (normalized === 'multiplecolumns' || normalized === 'multiple_columns' || normalized === 'multiple' || normalized === 'tabular' || normalized === 'outline') {
        return 'multipleColumns';
    }
    return 'singleColumn';
};

/** @param {any} [value] @returns {any} */
export const normalizeTreeGroupDefaultExpandedValue = (value) => {
    /** @type {any} */
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return 0;
    /** @type {any} */
    const rounded = Math.floor(normalized);
    if (rounded < -1) return -1;
    return rounded;
};

/** @param {any} [value] @returns {any} */
export const normalizeTreeDefaultOpenPathsValue = (value) => {
    if (!Array.isArray(value)) return [];
    /** @type {any[]} */
    const normalizedPaths = [];
    value.forEach((entry) => {
        if (typeof entry === 'string' && entry.trim()) {
            normalizedPaths.push(entry.trim());
            return;
        }
        if (Array.isArray(entry)) {
            /** @type {any} */
            const normalizedEntry = entry
                .map((part) => (part === undefined || part === null ? '' : String(part).trim()))
                .filter(Boolean)
                .join('|||');
            if (normalizedEntry) normalizedPaths.push(normalizedEntry);
        }
    });
    return Array.from(new Set(normalizedPaths));
};

/** @param {any} [value] @returns {any} */
export const normalizeTreeLevelLabelsValue = (value) => (
    Array.isArray(value)
        ? value
            .map((label) => (typeof label === 'string' ? label.trim() : ''))
            .filter(Boolean)
        : []
);

/** @param {any} [value] @returns {any} */
export const normalizeDetailRefreshStrategyValue = (value) => {
    /** @type {any} */
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_DETAIL_REFRESH_STRATEGIES.has(normalized) ? normalized : 'rows';
};

// ─── Transaction Normalization ─────────────────────────────────────────────

/** @param {any} [transaction] @returns {any} */
export const hasTransactionEntries = (transaction) => (
    Boolean(transaction && typeof transaction === 'object')
    && ['add', 'remove', 'update', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0)
);

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeTransactionRefreshModeValue = (value, fallback = 'smart') => {
    /** @type {any} */
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_REFRESH_MODES.has(normalized) ? normalized : fallback;
};

/** @param {any} [value] @returns {any} */
export const normalizeTransactionEventActionValue = (value) => {
    /** @type {any} */
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_EVENT_ACTIONS.has(normalized) ? normalized : null;
};

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizePropagationFormulaValue = (value, fallback = 'equal') => {
    /** @type {any} */
    const normalized = typeof value === 'string'
        ? value.trim().toLowerCase().replace(/[\s-]+/g, '_')
        : '';
    if (['equal', 'even', 'default', 'delta', 'uniform', 'equal_delta', 'balanced_delta', 'uniform_shift', 'balanced_shift'].includes(normalized)) {
        return 'equal';
    }
    if (['proportional', 'ratio', 'scale', 'scaled'].includes(normalized)) {
        return 'proportional';
    }
    if (fallback === null || fallback === undefined) return null;
    if (fallback === value) return null;
    return normalizePropagationFormulaValue(fallback, null);
};

/** @param {any} [update] @param {any} [groupingFields] @returns {any} */
export const isParentAggregatePropagationEdit = (update, groupingFields) => {
    if (!update || typeof update !== 'object') return false;
    /** @type {any} */
    const aggregation = update.aggregation && typeof update.aggregation === 'object'
        ? update.aggregation
        : null;
    /** @type {any} */
    const aggregationFn = aggregation && aggregation.agg
        ? String(aggregation.agg).trim().toLowerCase()
        : '';
    if (!aggregationFn || aggregation.windowFn) return false;
    /** @type {any} */
    const rowPath = update.rowPath || update.rowId;
    /** @type {any} */
    const normalizedRowPath = rowPath === null || rowPath === undefined ? '' : String(rowPath).trim();
    if (!normalizedRowPath || normalizedRowPath === GRAND_TOTAL_ROW_ID) return false;
    const pathDepth = normalizedRowPath.split('|||').filter(Boolean).length;
    /** @type {any} */
    const groupingDepth = Array.isArray(groupingFields) ? groupingFields.length : 0;
    return groupingDepth > 0 && pathDepth > 0 && pathDepth < groupingDepth;
};

/** @param {any} [transaction] @param {any} [overrides] @returns {any} */
export const normalizeTransactionHistoryPayload = (transaction, overrides = {}) => {
    if (!transaction || typeof transaction !== 'object') return null;
    /** @type {any} */
    const normalizedEventAction = normalizeTransactionEventActionValue(
        overrides.eventAction !== undefined ? overrides.eventAction : (transaction.eventAction || transaction.event_action)
    );
    /** @type {any} */
    const normalized = cloneSerializable({
        add: Array.isArray(transaction.add) ? transaction.add : [],
        remove: Array.isArray(transaction.remove) ? transaction.remove : [],
        update: Array.isArray(transaction.update) ? transaction.update : [],
        upsert: Array.isArray(transaction.upsert) ? transaction.upsert : [],
        keyFields: Array.isArray(transaction.keyFields) ? transaction.keyFields : [],
        refreshMode: normalizeTransactionRefreshModeValue(
            overrides.refreshMode !== undefined ? overrides.refreshMode : transaction.refreshMode,
            'smart'
        ),
        source: overrides.source !== undefined ? overrides.source : transaction.source,
        eventAction: normalizedEventAction || undefined,
        eventId: transaction.eventId !== undefined ? transaction.eventId : transaction.event_id,
        eventIds: Array.isArray(transaction.eventIds)
            ? transaction.eventIds
            : (Array.isArray(transaction.event_ids) ? transaction.event_ids : []),
        propagationStrategy: (() => {
            /** @type {any} */
            const propagationValue = transaction.propagationStrategy !== undefined
                ? transaction.propagationStrategy
                : transaction.propagation_strategy;
            /** @type {any} */
            const normalizedPropagationValue = normalizePropagationFormulaValue(propagationValue, null);
            return normalizedPropagationValue || propagationValue;
        })(),
    }, null);
    return hasTransactionEntries(normalized) || normalizedEventAction ? normalized : null;
};

// ─── Transaction Helpers ───────────────────────────────────────────────────

/** @param {any} [expectedValue] @param {any} [actualValue] @returns {any} */
export const cellValuesMatch = (expectedValue, actualValue) => {
    if (Object.is(expectedValue, actualValue)) return true;
    /** @type {any} */
    const expectedNumber = Number(expectedValue);
    /** @type {any} */
    const actualNumber = Number(actualValue);
    if (Number.isFinite(expectedNumber) && Number.isFinite(actualNumber)) {
        return expectedNumber === actualNumber;
    }
    return String(expectedValue) === String(actualValue);
};

/** @param {any} [transactionResult] @returns {any} */
export const hasAppliedTransactionWork = (transactionResult) => (
    Boolean(transactionResult && typeof transactionResult === 'object')
    && Object.values((transactionResult.applied && typeof transactionResult.applied === 'object') ? transactionResult.applied : {})
        .some((count) => Number(count) > 0)
);

/** @param {any} [transaction] @returns {any} */
export const shouldShowTransactionLoading = (transaction) => {
    if (!transaction || typeof transaction !== 'object') return true;
    /** @type {any} */
    const refreshMode = normalizeTransactionRefreshModeValue(transaction.refreshMode, 'smart');
    if (refreshMode === 'structural' || refreshMode === 'full' || refreshMode === 'smart_structural') {
        return true;
    }
    return ['add', 'remove', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0);
};

/** @param {any} [entry] @returns {any} */
export const describeTransactionPropagation = (entry) => {
    if (!entry || typeof entry !== 'object') return '';
    /** @type {any} */
    const updatedRowCount = Math.max(0, Number(entry.updatedRowCount) || 0);
    /** @type {any} */
    const targetColumn = entry.targetColumn ? formatDisplayLabel(entry.targetColumn) : 'value';
    /** @type {any} */
    const aggregationLabel = formatAggLabel(entry.aggregationFn || 'sum', entry.weightField);
    /** @type {any} */
    const strategy = normalizePropagationFormulaValue(entry.strategy, null);
    /** @type {any} */
    const strategyLabel = strategy ? ` using ${strategy} formula` : '';
    if (updatedRowCount > 0) {
        return `Propagated ${aggregationLabel} on ${targetColumn} to ${updatedRowCount} source row${updatedRowCount === 1 ? '' : 's'}${strategyLabel}.`;
    }
    return `Applied ${aggregationLabel} edit on ${targetColumn}${strategyLabel}.`;
};

/** @param {any} [target] @returns {any} */
export const isEditableKeyboardTarget = (target) => {
    if (!target || typeof target !== 'object') return false;
    /** @type {any} */
    const tagName = typeof target.tagName === 'string' ? target.tagName.toUpperCase() : '';
    /** @type {any} */
    const editableAncestor = typeof target.closest === 'function'
        ? target.closest('input, textarea, select, [contenteditable="true"], [contenteditable="plaintext-only"]')
        : null;
    return Boolean(
        target.isContentEditable
        || editableAncestor
        || tagName === 'INPUT'
        || tagName === 'TEXTAREA'
        || tagName === 'SELECT'
    );
};

/** @param {any} [event] @returns {any} */
export const isEditableKeyboardEvent = (event) => {
    if (isEditableKeyboardTarget(event?.target)) return true;
    /** @type {any} */
    const path = typeof event?.composedPath === 'function' ? event.composedPath() : [];
    return Array.isArray(path) && path.some(isEditableKeyboardTarget);
};

// ─── Tree Config Normalization ─────────────────────────────────────────────

/** @param {any} [value] @returns {any} */
export const normalizeTreeConfigValue = (value) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const sourceType = normalizeTreeSourceTypeValue(
        source.sourceType
        || source.source_type
        || source.mode
        || source.treeDataMode
        || source.tree_data_mode
    );
    /** @type {any} */
    const valueFields = Array.isArray(source.valueFields)
        ? source.valueFields.filter(/** @param {any} [field] @returns {any} */ (field) => typeof field === 'string' && field)
        : [];
    /** @type {any} */
    const extraFields = Array.isArray(source.extraFields)
        ? source.extraFields.filter(/** @param {any} [field] @returns {any} */ (field) => typeof field === 'string' && field)
        : [];
    /** @type {any} */
    const defaultOpenValue = Object.prototype.hasOwnProperty.call(source, 'openByDefault')
        ? source.openByDefault
        : source.open_by_default;
    /** @type {any} */
    const groupDefaultExpandedSource = Array.isArray(defaultOpenValue)
        ? undefined
        : (
            defaultOpenValue !== undefined
                ? defaultOpenValue
                : (
                    source.groupDefaultExpanded !== undefined
                        ? source.groupDefaultExpanded
                        : (
                            source.group_default_expanded !== undefined
                                ? source.group_default_expanded
                                : source.defaultOpenDepth
                        )
                )
        );
    /** @type {any} */
    const defaultOpenPaths = normalizeTreeDefaultOpenPathsValue(
        Array.isArray(defaultOpenValue)
            ? defaultOpenValue
            : (source.defaultOpenPaths || source.default_open_paths)
    );
    return {
        sourceType,
        idField: typeof (source.idField || source.id_field) === 'string' && (source.idField || source.id_field) ? (source.idField || source.id_field) : 'id',
        parentIdField: typeof (source.parentIdField || source.parent_id_field || source.treeDataParentIdField) === 'string' && (source.parentIdField || source.parent_id_field || source.treeDataParentIdField)
            ? (source.parentIdField || source.parent_id_field || source.treeDataParentIdField)
            : 'parent_id',
        pathField: typeof (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField) === 'string' && (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField)
            ? (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField)
            : 'path',
        pathSeparator: typeof (source.pathSeparator || source.path_separator) === 'string' && (source.pathSeparator || source.path_separator) ? (source.pathSeparator || source.path_separator) : '|||',
        childrenField: typeof (source.childrenField || source.children_field || source.treeDataChildrenField) === 'string' && (source.childrenField || source.children_field || source.treeDataChildrenField)
            ? (source.childrenField || source.children_field || source.treeDataChildrenField)
            : 'children',
        labelField: typeof (source.labelField || source.label_field) === 'string' && (source.labelField || source.label_field) ? (source.labelField || source.label_field) : 'name',
        sortBy: typeof (source.sortBy || source.sort_by) === 'string' && (source.sortBy || source.sort_by) ? (source.sortBy || source.sort_by) : null,
        sortDir: (source.sortDir || source.sort_dir) === 'desc' ? 'desc' : 'asc',
        valueFields,
        extraFields,
        displayMode: normalizeTreeDisplayModeValue(source.displayMode || source.display_mode || source.treeDataDisplayType || source.tree_data_display_type),
        groupDefaultExpanded: normalizeTreeGroupDefaultExpandedValue(groupDefaultExpandedSource),
        defaultOpenPaths,
        suppressGroupRowsSticky: Boolean(
            source.suppressGroupRowsSticky
            || source.suppress_group_rows_sticky
            || source.disableStickyGroups
            || source.disable_sticky_groups
        ),
        levelLabels: normalizeTreeLevelLabelsValue(source.levelLabels || source.level_labels),
    };
};

// ─── Detail Config Normalization ───────────────────────────────────────────

/** @param {any} [value] @param {any} [min] @param {any} [max] @returns {any} */
export const clampOptionalInteger = (value, min, max) => {
    /** @type {any} */
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return null;
    return Math.max(min, Math.min(max, Math.floor(normalized)));
};

/** @param {any} [value] @returns {any} */
export const normalizeDetailConfigValue = (value) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const defaultKind = typeof source.defaultKind === 'string' && source.defaultKind.trim()
        ? source.defaultKind.trim().toLowerCase()
        : 'records';
    return {
        enabled: source.enabled !== false,
        defaultKind,
        allowPerRowKind: (source.allowPerRowKind !== false) && (source.allow_per_row_kind !== false),
        inlineHeight: Number.isFinite(Number(source.inlineHeight || source.inline_height)) ? Math.max(220, Math.floor(Number(source.inlineHeight || source.inline_height))) : 280,
        sidepanelWidth: Number.isFinite(Number(source.sidepanelWidth || source.sidepanel_width)) ? Math.max(320, Math.floor(Number(source.sidepanelWidth || source.sidepanel_width))) : 480,
        drawerHeight: Number.isFinite(Number(source.drawerHeight || source.drawer_height)) ? Math.max(240, Math.floor(Number(source.drawerHeight || source.drawer_height))) : 320,
        keepDetailRows: Boolean(source.keepDetailRows || source.keep_detail_rows),
        keepDetailRowsCount: clampOptionalInteger(
            source.keepDetailRowsCount !== undefined
                ? source.keepDetailRowsCount
                : source.keep_detail_rows_count,
            1,
            1000
        ) || 10,
        refreshStrategy: normalizeDetailRefreshStrategyValue(source.refreshStrategy || source.refresh_strategy),
    };
};

/** @param {any} [treeConfig] @returns {any} */
export const hasTreeDefaultExpansionConfig = (treeConfig) => {
    if (!treeConfig || typeof treeConfig !== 'object') return false;
    if (Number.isFinite(Number(treeConfig.groupDefaultExpanded)) && Number(treeConfig.groupDefaultExpanded) !== 0) {
        return true;
    }
    return Array.isArray(treeConfig.defaultOpenPaths) && treeConfig.defaultOpenPaths.length > 0;
};

// ─── Report Normalization ──────────────────────────────────────────────────

/** @param {any} [value] @returns {any} */
export const normalizeLegacyPivotModeValue = (value) => (
    value === 'report' || value === 'pivot' ? value : null
);

/** @param {any} [value] @returns {any} */
export const normalizeReportTopN = (value) => (
    Number.isFinite(Number(value)) && Number(value) > 0
        ? Math.floor(Number(value))
        : null
);

/** @param {any} [value] @returns {any} */
export const normalizeReportNodeValue = (value) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {Record<string, any>} */
    const normalized = {
        ...source,
        field: typeof source.field === 'string' ? source.field : '',
        label: typeof source.label === 'string' ? source.label : '',
        topN: normalizeReportTopN(source.topN),
        sortBy: typeof source.sortBy === 'string' && source.sortBy.trim()
            ? source.sortBy.trim()
            : null,
        sortDir: source.sortDir === 'asc' ? 'asc' : 'desc',
    };

    /** @type {any} */
    const childrenSource = source.childrenByValue && typeof source.childrenByValue === 'object'
        ? source.childrenByValue
        : (
            source.conditionalChildren && typeof source.conditionalChildren === 'object'
                ? source.conditionalChildren
                : null
        );
    if (childrenSource) {
        /** @type {any} */
        const normalizedChildrenByValue = Object.entries(childrenSource).reduce((acc, [key, rule]) => {
            if (key === undefined || key === null) return acc;
            if (String(key) === '*') return acc;
            acc[String(key)] = normalizeReportNodeValue(rule);
            return acc;
        }, /** @type {Record<string, any>} */ ({}));
        if (Object.keys(normalizedChildrenByValue).length > 0) {
            normalized.childrenByValue = normalizedChildrenByValue;
        } else {
            delete normalized.childrenByValue;
        }
    } else {
        delete normalized.childrenByValue;
    }

    /** @type {any} */
    const defaultChildSource = source.defaultChild && typeof source.defaultChild === 'object'
        ? source.defaultChild
        : (
            childrenSource && childrenSource['*'] && typeof childrenSource['*'] === 'object'
                ? childrenSource['*']
                : null
        );
    if (defaultChildSource) {
        normalized.defaultChild = normalizeReportNodeValue(defaultChildSource);
    } else {
        delete normalized.defaultChild;
    }

    if (Array.isArray(source.branches)) {
        /** @type {any} */
        const normalizedBranches = source.branches
            .filter(/** @param {any} [branch] @returns {any} */ (branch) => branch && typeof branch === 'object')
            .map(/** @param {any} [branch] @returns {any} */ (branch) => ({
                ...branch,
                label: typeof branch.label === 'string' ? branch.label : '',
                condition: branch.condition && typeof branch.condition === 'object' ? branch.condition : { op: 'AND', clauses: [] },
                child: branch.child && typeof branch.child === 'object'
                    ? normalizeReportNodeValue(branch.child)
                    : null,
            }));
        if (normalizedBranches.length > 0) {
            normalized.branches = normalizedBranches;
        } else {
            delete normalized.branches;
        }
    } else {
        delete normalized.branches;
    }

    delete normalized.manualRows;

    return normalized;
};

/** @param {any} [levels] @param {any} [index] @param {any} [overrideRule] @returns {any} */
export const convertLegacyLevelsToReportNode = (levels, index = 0, overrideRule = null) => {
    /** @type {any} */
    const baseRule = overrideRule && typeof overrideRule === 'object'
        ? overrideRule
        : (Array.isArray(levels) ? levels[index] : null);
    if (!baseRule || typeof baseRule !== 'object') return null;

    /** @type {any} */
    const normalizedBase = normalizeReportNodeValue(baseRule);
    /** @type {any} */
    const nextDefaultNode = index + 1 < (Array.isArray(levels) ? levels.length : 0)
        ? convertLegacyLevelsToReportNode(levels, index + 1)
        : null;
    /** @type {any} */
    const legacyConditional = baseRule.conditionalChildren && typeof baseRule.conditionalChildren === 'object'
        ? baseRule.conditionalChildren
        : null;

    if (legacyConditional) {
        /** @type {any} */
        const childrenByValue = Object.entries(legacyConditional).reduce((acc, [key, childRule]) => {
            if (key === '*' || !childRule || typeof childRule !== 'object') return acc;
            /** @type {any} */
            const childNode = convertLegacyLevelsToReportNode(levels, index + 1, childRule);
            if (childNode) acc[String(key)] = childNode;
            return acc;
        }, /** @type {Record<string, any>} */ ({}));
        if (Object.keys(childrenByValue).length > 0) {
            normalizedBase.childrenByValue = childrenByValue;
        }
        if (legacyConditional['*'] && typeof legacyConditional['*'] === 'object') {
            normalizedBase.defaultChild = convertLegacyLevelsToReportNode(levels, index + 1, legacyConditional['*']);
        } else if (nextDefaultNode) {
            normalizedBase.defaultChild = nextDefaultNode;
        }
    } else if (nextDefaultNode) {
        normalizedBase.defaultChild = nextDefaultNode;
    }

    return normalizedBase;
};

/** @param {any} [value] @returns {any} */
export const normalizeReportDefValue = (value) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const normalizedLevels = Array.isArray(source.levels)
        ? source.levels.map(/** @param {any} [level] @returns {any} */ (level) => normalizeReportNodeValue(level))
        : [];
    /** @type {any} */
    const normalizedRootFromProp = source.root && typeof source.root === 'object'
        ? normalizeReportNodeValue(source.root)
        : null;
    /** @type {any} */
    const normalizedRoot = normalizedRootFromProp && normalizedRootFromProp.field
        ? normalizedRootFromProp
        : (normalizedLevels.length > 0 ? convertLegacyLevelsToReportNode(source.levels) : null);
    return {
        ...source,
        root: normalizedRoot,
        levels: normalizedLevels,
    };
};

/** @param {any} [reportDef] @returns {any} */
export const collectReportFields = (reportDef) => {
    /** @type {any[]} */
    const ordered = [];
    /** @type {any} */
    const seen = new Set();
    /** @param {any} [node] @returns {any} */
    const visit = (node) => {
        if (!node || typeof node !== 'object') return;
        if (node.field && !seen.has(node.field)) {
            seen.add(node.field);
            ordered.push(node.field);
        }
        if (node.defaultChild) visit(node.defaultChild);
        if (Array.isArray(node.branches)) {
            node.branches.forEach(/** @param {any} [branch] @returns {any} */ (branch) => {
                if (branch && branch.child) visit(branch.child);
            });
        }
        if (node.childrenByValue && typeof node.childrenByValue === 'object') {
            Object.values(node.childrenByValue).forEach((childNode) => visit(childNode));
        }
    };
    if (reportDef && typeof reportDef === 'object') visit(reportDef.root);
    return ordered;
};

/** @param {any} [reportDef] @returns {any} */
export const countReportNodes = (reportDef) => {
    let count = 0;
    /** @param {any} [node] @returns {any} */
    const visit = (node) => {
        if (!node || typeof node !== 'object') return;
        count += 1;
        if (node.defaultChild) visit(node.defaultChild);
        if (Array.isArray(node.branches)) {
            node.branches.forEach(/** @param {any} [branch] @returns {any} */ (branch) => {
                if (branch && branch.child) visit(branch.child);
            });
        }
        if (node.childrenByValue && typeof node.childrenByValue === 'object') {
            Object.values(node.childrenByValue).forEach((childNode) => visit(childNode));
        }
    };
    if (reportDef && typeof reportDef === 'object') visit(reportDef.root);
    return count;
};

/** @param {any} [reportDef] @returns {any} */
export const getReportHeaderLabel = (reportDef) => {
    /** @type {any} */
    const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
    if (!root || typeof root !== 'object') return 'Report';
    if (root.label && root.label.trim()) return root.label.trim();
    if (root.field && root.field.trim()) return formatDisplayLabel(root.field);
    return 'Report';
};

/** @param {any} [value] @returns {any} */
export const normalizeSavedReportsValue = (value) => (
    Array.isArray(value)
        ? value
            .filter((report) => report && typeof report === 'object')
            .map((report, index) => ({
                ...report,
                id: typeof report.id === 'string' ? report.id : `report-${index + 1}`,
                name: typeof report.name === 'string' && report.name.trim()
                    ? report.name
                    : 'Saved Report',
                reportDef: normalizeReportDefValue(report.reportDef),
            }))
        : []
);

/** @param {any} [value] @returns {any} */
export const normalizeActiveReportIdValue = (value) => (
    typeof value === 'string' && value.trim() ? value : null
);

// ─── Chart Definition Normalization ────────────────────────────────────────

/** @param {any} [rect] @param {any} [containerRect] @returns {any} */
export const clampFloatingChartRect = (rect, containerRect) => {
    /** @type {any} */
    const source = rect && typeof rect === 'object' ? rect : {};
    /** @type {any} */
    const rawWidth = Number(source.width);
    /** @type {any} */
    const rawHeight = Number(source.height);
    /** @type {any} */
    const rawLeft = Number(source.left);
    /** @type {any} */
    const rawTop = Number(source.top);
    /** @type {any} */
    const width = Number.isFinite(rawWidth) ? Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(rawWidth)) : 430;
    /** @type {any} */
    const height = Number.isFinite(rawHeight) ? Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.floor(rawHeight)) : DEFAULT_FLOATING_CHART_PANEL_HEIGHT;
    /** @type {any} */
    const left = Number.isFinite(rawLeft) ? Math.floor(rawLeft) : 24;
    /** @type {any} */
    const top = Number.isFinite(rawTop) ? Math.floor(rawTop) : 24;

    if (!containerRect || !Number.isFinite(containerRect.width) || !Number.isFinite(containerRect.height)) {
        return { left: Math.max(0, left), top: Math.max(0, top), width, height };
    }

    /** @type {any} */
    const maxWidth = Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(containerRect.width) - 12);
    /** @type {any} */
    const maxHeight = Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.floor(containerRect.height) - 12);
    /** @type {any} */
    const safeWidth = Math.max(MIN_CHART_PANEL_WIDTH, Math.min(maxWidth, width));
    /** @type {any} */
    const safeHeight = Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.min(maxHeight, height));
    /** @type {any} */
    const maxLeft = Math.max(0, Math.floor(containerRect.width) - safeWidth);
    /** @type {any} */
    const maxTop = Math.max(0, Math.floor(containerRect.height) - safeHeight);

    return {
        left: Math.max(0, Math.min(maxLeft, left)),
        top: Math.max(0, Math.min(maxTop, top)),
        width: safeWidth,
        height: safeHeight,
    };
};

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeChartDefinition = (value, fallback = {}) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const fallbackSource = fallback && typeof fallback === 'object' ? fallback : {};
    /** @type {any} */
    const chartType = VALID_CHART_TYPES.has(source.chartType) ? source.chartType : (VALID_CHART_TYPES.has(fallbackSource.chartType) ? fallbackSource.chartType : 'bar');
    /** @type {any} */
    const barLayout = source.barLayout === 'stacked' || source.barLayout === 'grouped'
        ? source.barLayout
        : (fallbackSource.barLayout === 'stacked' ? 'stacked' : 'grouped');
    /** @type {any} */
    const axisMode = source.axisMode === 'horizontal' || source.axisMode === 'vertical'
        ? source.axisMode
        : (fallbackSource.axisMode === 'horizontal' ? 'horizontal' : 'vertical');
    /** @type {any} */
    const orientation = source.orientation === 'columns' || source.orientation === 'rows'
        ? source.orientation
        : (fallbackSource.orientation === 'columns' ? 'columns' : 'rows');
    /** @type {any} */
    const interactionMode = VALID_CHART_INTERACTION_MODES.has(source.interactionMode)
        ? source.interactionMode
        : (VALID_CHART_INTERACTION_MODES.has(fallbackSource.interactionMode) ? fallbackSource.interactionMode : 'focus');
    /** @type {any} */
    const sortMode = VALID_CHART_SORT_MODES.has(source.sortMode)
        ? source.sortMode
        : (VALID_CHART_SORT_MODES.has(fallbackSource.sortMode) ? fallbackSource.sortMode : 'natural');
    /** @type {any} */
    const serverScope = VALID_CHART_SERVER_SCOPES.has(source.serverScope)
        ? source.serverScope
        : (VALID_CHART_SERVER_SCOPES.has(fallbackSource.serverScope) ? fallbackSource.serverScope : 'viewport');
    /** @type {any} */
    const hierarchyLevel = source.hierarchyLevel === 'all' || (typeof source.hierarchyLevel === 'number' && source.hierarchyLevel >= 1)
        ? source.hierarchyLevel
        : (fallbackSource.hierarchyLevel === 'all' || (typeof fallbackSource.hierarchyLevel === 'number' && fallbackSource.hierarchyLevel >= 1)
            ? fallbackSource.hierarchyLevel
            : 'all');
    /** @type {any} */
    const rowLimit = Number.isFinite(Number(source.rowLimit))
        ? Math.max(1, Math.floor(Number(source.rowLimit)))
        : (Number.isFinite(Number(fallbackSource.rowLimit)) ? Math.max(1, Math.floor(Number(fallbackSource.rowLimit))) : DEFAULT_CHART_PANEL_ROW_LIMIT);
    /** @type {any} */
    const columnLimit = Number.isFinite(Number(source.columnLimit))
        ? Math.max(1, Math.floor(Number(source.columnLimit)))
        : (Number.isFinite(Number(fallbackSource.columnLimit)) ? Math.max(1, Math.floor(Number(fallbackSource.columnLimit))) : DEFAULT_CHART_PANEL_COLUMN_LIMIT);
    /** @type {any} */
    const width = Number.isFinite(Number(source.width))
        ? Math.max(MIN_CHART_PANEL_WIDTH, Math.min(MAX_CHART_PANEL_WIDTH, Math.floor(Number(source.width))))
        : (Number.isFinite(Number(fallbackSource.width))
            ? Math.max(MIN_CHART_PANEL_WIDTH, Math.min(MAX_CHART_PANEL_WIDTH, Math.floor(Number(fallbackSource.width))))
            : 430);
    /** @type {any} */
    const chartHeight = Number.isFinite(Number(source.chartHeight))
        ? Math.max(180, Math.floor(Number(source.chartHeight)))
        : (Number.isFinite(Number(fallbackSource.chartHeight))
            ? Math.max(180, Math.floor(Number(fallbackSource.chartHeight)))
            : DEFAULT_CHART_GRAPH_HEIGHT);

    return {
        id: sanitizeChartDefinitionName(source.id || fallbackSource.id || createChartDefinitionId('chart'), 'chart'),
        name: sanitizeChartDefinitionName(source.name || fallbackSource.name || 'Live Chart', 'Live Chart'),
        chartTitle: sanitizeChartDefinitionName(source.chartTitle || fallbackSource.chartTitle || source.name || fallbackSource.name || 'Chart', 'Chart'),
        source: source.source === 'selection' || source.source === 'pivot'
            ? source.source
            : (fallbackSource.source === 'selection' ? 'selection' : 'pivot'),
        chartType,
        barLayout,
        axisMode,
        orientation,
        hierarchyLevel,
        rowLimit,
        columnLimit,
        width,
        chartHeight,
        sortMode,
        interactionMode,
        serverScope,
        chartLayers: cloneSerializable(
            Array.isArray(source.chartLayers)
                ? source.chartLayers
                : (Array.isArray(fallbackSource.chartLayers) ? fallbackSource.chartLayers : []),
            []
        ),
    };
};

/** @param {any} [definitions] @param {any} [fallbackDefinition] @returns {any} */
export const sanitizeChartDefinitions = (definitions, fallbackDefinition) => {
    /** @type {any} */
    const sourceDefinitions = Array.isArray(definitions) ? definitions : [];
    /** @type {any} */
    const normalized = sourceDefinitions
        .filter(/** @param {any} [item] @returns {any} */ (item) => item && typeof item === 'object')
        .map(/** @param {any} [item] @param {any} [index] @returns {any} */ (item, index) => normalizeChartDefinition(item, {
            ...fallbackDefinition,
            id: item.id || `chart-${index + 1}`,
            name: item.name || `Chart ${index + 1}`,
        }));

    if (normalized.length > 0) return normalized;
    return [normalizeChartDefinition(fallbackDefinition, fallbackDefinition)];
};

// ─── Chart Column Utilities ────────────────────────────────────────────────

/** @param {any} [column] @returns {any} */
export const serializeChartColumn = (column) => {
    if (!column || typeof column !== 'object') return null;
    /** @type {any} */
    const columnMeta = (column.columnDef && column.columnDef.meta) || column.meta || null;
    if (columnMeta && columnMeta.isSparklineSummary) return null;
    return {
        id: column.id || null,
        headerVal: column.headerVal !== undefined ? column.headerVal : null,
        columnDef: column.columnDef
            ? {
                header: typeof column.columnDef.header === 'string' ? column.columnDef.header : null,
                headerVal: column.columnDef.headerVal !== undefined ? column.columnDef.headerVal : null,
            }
            : null,
        parent: column.parent ? serializeChartColumn(column.parent) : null,
    };
};

/** @param {any} [columns] @returns {any} */
export const serializeChartColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => serializeChartColumn(column)).filter(Boolean)
        : []
);

/** @param {any} [chartType] @param {any} [chartLayers] @returns {any} */
export const getRequestedChartSeriesColumnIds = (chartType, chartLayers) => (
    chartType === 'combo'
        ? Array.from(new Set(
            (Array.isArray(chartLayers) ? chartLayers : [])
                .map((layer) => (layer && typeof layer.columnId === 'string' ? layer.columnId.trim() : ''))
                .filter(Boolean)
        ))
        : []
);

/** @param {any} [column] @param {any} [fallbackId] @returns {any} */
export const normalizeChartResponseColumn = (column, fallbackId = null) => {
    /** @type {any} */
    const source = column && typeof column === 'object' ? column : {};
    /** @type {any} */
    const columnId = source.id || (typeof column === 'string' ? column : fallbackId) || null;
    if (!columnId) return null;
    /** @type {any} */
    const headerVal = source.headerVal !== undefined
        ? source.headerVal
        : (source.header !== undefined ? source.header : null);
    /** @type {any} */
    const header = source.columnDef && typeof source.columnDef === 'object'
        ? (typeof source.columnDef.header === 'string'
            ? source.columnDef.header
            : (typeof source.header === 'string' ? source.header : (headerVal !== null && headerVal !== undefined ? String(headerVal) : String(columnId))))
        : (typeof source.header === 'string' ? source.header : (headerVal !== null && headerVal !== undefined ? String(headerVal) : String(columnId)));
    return {
        id: columnId,
        headerVal,
        columnDef: {
            header,
            headerVal: source.columnDef && source.columnDef.headerVal !== undefined
                ? source.columnDef.headerVal
                : headerVal,
        },
        parent: source.parent ? normalizeChartResponseColumn(source.parent) : null,
    };
};

/** @param {any} [columns] @returns {any} */
export const normalizeChartResponseColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => normalizeChartResponseColumn(column)).filter(Boolean)
        : []
);

/** @param {any} [colSchema] @returns {any} */
export const buildChartColumnsFromSchema = (colSchema) => (
    colSchema && Array.isArray(colSchema.columns)
        ? colSchema.columns
            .map(/** @param {any} [column] @returns {any} */ (column) => normalizeChartResponseColumn({
                id: column && column.id ? column.id : null,
                header: column && typeof column.header === 'string' ? column.header : null,
                headerVal: column && column.headerVal !== undefined ? column.headerVal : (column && column.id ? column.id : null),
            }))
            .filter(Boolean)
        : []
);

/** @param {any} [chartDataEntry] @param {any} [fallbackColumns] @returns {any} */
export const resolveChartModelColumns = (chartDataEntry, fallbackColumns = []) => {
    /** @type {any} */
    const responseColumns = normalizeChartResponseColumns(chartDataEntry && chartDataEntry.columns);
    return responseColumns.length > 0 ? responseColumns : serializeChartColumns(fallbackColumns);
};

/** @param {any} [chartDataEntry] @param {any} [fallbackColumns] @returns {any} */
export const resolveChartAvailableColumns = (chartDataEntry, fallbackColumns = []) => {
    /** @type {any} */
    const responseColumns = normalizeChartResponseColumns(chartDataEntry && chartDataEntry.columns);
    /** @type {any} */
    const schemaColumns = buildChartColumnsFromSchema(chartDataEntry && chartDataEntry.colSchema);
    /** @type {any} */
    const fallbackSerialized = serializeChartColumns(fallbackColumns);
    if (schemaColumns.length === 0) {
        return responseColumns.length > 0 ? responseColumns : fallbackSerialized;
    }
    /** @type {any} */
    const responseById = new Map(responseColumns.map(/** @param {any} [column] @returns {any} */ (column) => [column.id, column]));
    /** @type {any} */
    const fallbackById = new Map(fallbackSerialized.map(/** @param {any} [column] @returns {any} */ (column) => [column.id, column]));
    /** @type {any} */
    const merged = schemaColumns.map(/** @param {any} [column] @returns {any} */ (column) => responseById.get(column.id) || fallbackById.get(column.id) || column);
    /** @type {any} */
    const knownIds = new Set(merged.map(/** @param {any} [column] @returns {any} */ (column) => column.id));
    responseColumns.forEach(/** @param {any} [column] @returns {any} */ (column) => {
        if (!knownIds.has(column.id)) {
            merged.push(column);
            knownIds.add(column.id);
        }
    });
    fallbackSerialized.forEach(/** @param {any} [column] @returns {any} */ (column) => {
        if (!knownIds.has(column.id)) {
            merged.push(column);
            knownIds.add(column.id);
        }
    });
    return merged;
};

// ─── Transport Data Normalization ──────────────────────────────────────────

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const coerceTransportNumber = (value, fallback = null) => {
    if (value === null || value === undefined || value === '') return fallback;
    /** @type {any} */
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
};

/** @param {any} [column] @returns {any} */
export const isColSchemaTransportColumn = (column) => {
    /** @type {any} */
    const columnId = column && typeof column === 'object' ? column.id : column;
    return String(columnId || '').trim() === '__col_schema';
};

/** @param {any} [columns] @returns {any} */
export const extractColSchemaFromTransportColumns = (columns) => {
    if (!Array.isArray(columns)) return null;
    /** @type {any} */
    const schemaEntry = columns.find((column) => (
        column
        && typeof column === 'object'
        && column.id === '__col_schema'
        && column.col_schema
        && typeof column.col_schema === 'object'
    ));
    return schemaEntry ? schemaEntry.col_schema : null;
};

/** @param {any} [columns] @param {any} [fallbackColumns] @returns {any} */
export const normalizeTransportColumns = (columns, fallbackColumns = []) => {
    /** @type {any} */
    const sourceColumns = Array.isArray(columns)
        ? columns
        : (Array.isArray(fallbackColumns) ? fallbackColumns : []);
    return sourceColumns.filter(/** @param {any} [column] @returns {any} */ (column) => !isColSchemaTransportColumn(column));
};

/** @param {any} [payload] @param {any} [fallback] @returns {any} */
export const normalizeRuntimeDataEnvelope = (payload, fallback = {}) => {
    /** @type {any} */
    const source = payload && typeof payload === 'object' ? payload : {};
    /** @type {any} */
    const sourceStateEpoch = source.stateEpoch !== undefined ? source.stateEpoch : source.state_epoch;
    /** @type {any} */
    const fallbackStateEpoch = fallback.stateEpoch !== undefined ? fallback.stateEpoch : fallback.state_epoch;
    const colSchema = (
        source.colSchema
        || source.col_schema
        || extractColSchemaFromTransportColumns(source.columns)
        || fallback.colSchema
        || fallback.col_schema
        || extractColSchemaFromTransportColumns(fallback.columns)
        || null
    );
    return {
        data: Array.isArray(source.data) ? source.data : (Array.isArray(fallback.data) ? fallback.data : []),
        rowCount: coerceTransportNumber(source.rowCount, coerceTransportNumber(fallback.rowCount, null)),
        columns: normalizeTransportColumns(source.columns, fallback.columns),
        colSchema,
        dataOffset: coerceTransportNumber(source.dataOffset, coerceTransportNumber(fallback.dataOffset, 0)),
        dataVersion: coerceTransportNumber(source.dataVersion, coerceTransportNumber(fallback.dataVersion, 0)),
        stateEpoch: coerceTransportNumber(sourceStateEpoch, coerceTransportNumber(fallbackStateEpoch, null)),
    };
};

/** @param {any} [row] @returns {any} */
export const resolveTransportRowId = (row) => {
    if (!row || typeof row !== 'object') return null;
    if (isGrandTotalRow(row)) {
        return '__grand_total__';
    }
    if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
    if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
    if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
    return null;
};

/** @param {any} [patch] @param {any} [fallback] @returns {any} */
export const applyRuntimePatchEnvelope = (patch, fallback = {}) => {
    /** @type {any} */
    const source = patch && typeof patch === 'object' ? patch : {};
    /** @type {any} */
    const previousState = fallback && typeof fallback === 'object' ? fallback : {};
    /** @type {any} */
    const previousData = Array.isArray(previousState.data) ? previousState.data : [];
    /** @type {any} */
    const patchRows = Array.isArray(source.rows) ? source.rows.filter(/** @param {any} [row] @returns {any} */ (row) => row && typeof row === 'object') : [];

    if (patchRows.length === 0) {
        return normalizeRuntimeDataEnvelope({
            data: previousData,
            rowCount: source.rowCount,
            columns: source.columns,
            colSchema: source.colSchema || source.col_schema,
            dataOffset: source.dataOffset,
            dataVersion: source.dataVersion,
            stateEpoch: source.stateEpoch !== undefined ? source.stateEpoch : source.state_epoch,
        }, previousState);
    }

    /** @type {any} */
    const patchByRowId = new Map();
    patchRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
        /** @type {any} */
        const rowId = resolveTransportRowId(row);
        if (!rowId) return;
        patchByRowId.set(rowId, row);
    });

    /** @type {any} */
    const mergedData = previousData.map(/** @param {any} [row] @returns {any} */ (row) => {
        /** @type {any} */
        const rowId = resolveTransportRowId(row);
        if (!rowId || !patchByRowId.has(rowId)) return row;
        return { ...row, ...patchByRowId.get(rowId) };
    });

    return normalizeRuntimeDataEnvelope({
        data: mergedData,
        rowCount: source.rowCount,
        columns: source.columns,
        colSchema: source.colSchema || source.col_schema,
        dataOffset: source.dataOffset,
        dataVersion: source.dataVersion,
        stateEpoch: source.stateEpoch !== undefined ? source.stateEpoch : source.state_epoch,
    }, previousState);
};

// ─── Chart Canvas/Pane Normalization ───────────────────────────────────────

/** @param {any} [value] @returns {any} */
export const normalizeLockedChartRequest = (value) => {
    if (!value || typeof value !== 'object') return null;
    return {
        request: value.request && typeof value.request === 'object' ? cloneSerializable(value.request, null) : null,
        stateOverride: value.stateOverride && typeof value.stateOverride === 'object' ? cloneSerializable(value.stateOverride, null) : null,
        visibleColumns: Array.isArray(value.visibleColumns) ? cloneSerializable(value.visibleColumns, []) : [],
        requestSignature: typeof value.requestSignature === 'string' ? value.requestSignature : null,
    };
};

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizeChartDockPosition = (value, fallback = 'right') => (
    VALID_CHART_DOCK_POSITIONS.has(value) ? value : fallback
);

/** @type {any} */
const VALID_MARKER_SHAPES = new Set(['none', 'circle', 'square', 'diamond', 'triangle', 'cross']);
/** @type {any} */
const VALID_TRENDLINE_MODES = new Set(['linear', 'moving_avg', 'exponential']);
/** @type {any} */
const VALID_LEGEND_POSITIONS = new Set(['bottom', 'top', 'none']);
/** @type {any} */
const VALID_LABEL_POSITIONS = new Set(['outside', 'inside', 'center']);
/** @type {any} */
const VALID_VALUE_FORMATS = new Set(['auto', 'number', 'compact', 'percent', 'currency']);
/** @type {any} */
const VALID_SERIES_SORT_MODES = new Set(['none', 'value_desc', 'value_asc', 'alpha_asc', 'alpha_desc']);

/** @param {any} [value] @returns {any} */
export const normalizeChartSettings = (value) => {
    /** @type {any} */
    const s = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const yAxisMin = s.yAxisMin !== undefined ? String(s.yAxisMin) : '';
    /** @type {any} */
    const yAxisMax = s.yAxisMax !== undefined ? String(s.yAxisMax) : '';
    /** @type {any} */
    const yAxisMinRight = s.yAxisMinRight !== undefined ? String(s.yAxisMinRight) : '';
    /** @type {any} */
    const yAxisMaxRight = s.yAxisMaxRight !== undefined ? String(s.yAxisMaxRight) : '';
    return {
        chartSubtitle: typeof s.chartSubtitle === 'string' ? s.chartSubtitle : '',
        yAxisTitle: typeof s.yAxisTitle === 'string' ? s.yAxisTitle : '',
        yAxisMin,
        yAxisMax,
        yAxisMinRight,
        yAxisMaxRight,
        tickCount: Number.isFinite(Number(s.tickCount)) ? Math.max(2, Math.min(20, Math.floor(Number(s.tickCount)))) : 5,
        labelAngle: Number.isFinite(Number(s.labelAngle)) ? Math.max(0, Math.min(90, Math.floor(Number(s.labelAngle)))) : 0,
        colorPalette: typeof s.colorPalette === 'string' && s.colorPalette ? s.colorPalette : 'default',
        valueFormat: VALID_VALUE_FORMATS.has(s.valueFormat) ? s.valueFormat : 'auto',
        showDataLabels: Boolean(s.showDataLabels),
        decimalPlaces: Number.isFinite(Number(s.decimalPlaces)) ? Math.max(-1, Math.floor(Number(s.decimalPlaces))) : -1,
        labelPosition: VALID_LABEL_POSITIONS.has(s.labelPosition) ? s.labelPosition : 'outside',
        legendPosition: VALID_LEGEND_POSITIONS.has(s.legendPosition) ? s.legendPosition : 'bottom',
        markerShape: VALID_MARKER_SHAPES.has(s.markerShape) ? s.markerShape : 'circle',
        markerSize: Number.isFinite(Number(s.markerSize)) ? Math.max(1, Math.min(20, Math.floor(Number(s.markerSize)))) : 4,
        showGradient: Boolean(s.showGradient),
        gradientDirection: s.gradientDirection === 'horizontal' ? 'horizontal' : 'vertical',
        gradientOpacity: Number.isFinite(Number(s.gradientOpacity)) ? Math.max(0, Math.min(100, Math.floor(Number(s.gradientOpacity)))) : 70,
        showAnimations: s.showAnimations !== false,
        showCalloutLabels: Boolean(s.showCalloutLabels),
        showTrendline: Boolean(s.showTrendline),
        trendlineMode: VALID_TRENDLINE_MODES.has(s.trendlineMode) ? s.trendlineMode : 'linear',
        trendlinePeriod: Number.isFinite(Number(s.trendlinePeriod)) ? Math.max(2, Math.min(50, Math.floor(Number(s.trendlinePeriod)))) : 3,
        trendlineColor: typeof s.trendlineColor === 'string' ? s.trendlineColor : '',
        trendlineWidth: Number.isFinite(Number(s.trendlineWidth)) ? Math.max(1, Math.min(8, Math.floor(Number(s.trendlineWidth)))) : 2,
        seriesSort: VALID_SERIES_SORT_MODES.has(s.seriesSort) ? s.seriesSort : 'none',
        seriesColors: cloneSerializable(s.seriesColors && typeof s.seriesColors === 'object' && !Array.isArray(s.seriesColors) ? s.seriesColors : {}, {}),
        customPaletteColors: cloneSerializable(Array.isArray(s.customPaletteColors) ? s.customPaletteColors : [], []),
        lineDashStyles: cloneSerializable(s.lineDashStyles && typeof s.lineDashStyles === 'object' && !Array.isArray(s.lineDashStyles) ? s.lineDashStyles : {}, {}),
        referenceLines: cloneSerializable(Array.isArray(s.referenceLines) ? s.referenceLines : [], []),
    };
};

/** @param {any} [value] @param {any} [fallbackDefinition] @param {any} [index] @returns {any} */
export const normalizeChartCanvasPane = (value, fallbackDefinition, index = 0) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    /** @type {any} */
    const normalizedDefinition = normalizeChartDefinition(source, {
        ...fallbackDefinition,
        id: source.id || `chart-pane-${index + 1}`,
        name: source.name || `Chart Pane ${index + 1}`,
    });
    /** @type {any} */
    const numericSize = Number(source.size);
    /** @type {any} */
    const rawSettingsPanelWidth = Number(source.settingsPanelWidth);
    return {
        ...normalizedDefinition,
        size: Number.isFinite(numericSize) && numericSize > 0 ? numericSize : 1,
        dockPosition: normalizeChartDockPosition(source.dockPosition || source.dock_position, 'right'),
        floating: Boolean(source.floating),
        floatingRect: clampFloatingChartRect(source.floatingRect, null),
        locked: Boolean(source.locked),
        lockedModel: cloneSerializable(source.lockedModel, null),
        lockedRequest: normalizeLockedChartRequest(source.lockedRequest),
        immersiveMode: Boolean(source.immersiveMode),
        chartSettings: normalizeChartSettings(source.chartSettings),
        settingsPanelWidth: Number.isFinite(rawSettingsPanelWidth) && rawSettingsPanelWidth >= 300
            ? Math.min(680, Math.floor(rawSettingsPanelWidth))
            : null,
    };
};

/** @param {any} [panes] @param {any} [fallbackDefinition] @returns {any} */
export const sanitizeChartCanvasPanes = (panes, fallbackDefinition) => (
    Array.isArray(panes)
        ? panes
            .filter((pane) => pane && typeof pane === 'object')
            .map((pane, index) => normalizeChartCanvasPane(pane, fallbackDefinition, index))
        : []
);

/** @param {any} [layoutWidth] @returns {any} */
export const getChartPanelWidthBounds = (layoutWidth) => {
    if (!Number.isFinite(layoutWidth) || layoutWidth <= 0) {
        return { minWidth: MIN_CHART_PANEL_WIDTH, maxWidth: MAX_CHART_PANEL_WIDTH };
    }

    /** @type {any} */
    const minWidth = Math.min(320, Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(layoutWidth * 0.3)));
    /** @type {any} */
    const maxWidth = Math.min(
        MAX_CHART_PANEL_WIDTH,
        Math.max(minWidth, Math.floor(layoutWidth - Math.min(MIN_TABLE_PANEL_WIDTH, layoutWidth * 0.45)))
    );

    return { minWidth, maxWidth };
};

/** @param {any} [value] @returns {any} */
export const normalizeChartServerWindowConfig = (value) => {
    if (!value || typeof value !== 'object') {
        return { enabled: false, rows: null, columns: null, scope: 'viewport' };
    }
    return {
        enabled: value.enabled !== false,
        rows: Number.isFinite(Number(value.rows)) ? Math.max(1, Math.floor(Number(value.rows))) : null,
        columns: Number.isFinite(Number(value.columns)) ? Math.max(1, Math.floor(Number(value.columns))) : null,
        scope: VALID_CHART_SERVER_SCOPES.has(value.scope) ? value.scope : 'viewport',
    };
};

// ─── Session/ID Utilities ──────────────────────────────────────────────────

/** @param {any} [componentId] @returns {any} */
export const getOrCreateSessionId = (componentId = 'pivot-grid') => {
    if (typeof window === 'undefined') return `${componentId}-anonymous`;
    const storageKey = `pivot-session-${componentId}`;
    try {
        /** @type {any} */
        const existing = window.sessionStorage.getItem(storageKey);
        if (existing) return existing;
    } catch (_ignored) { /* storage unavailable */ }
    const newId = `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    try {
        window.sessionStorage.setItem(storageKey, newId);
    } catch (_ignored) { /* storage full or unavailable */ }
    return newId;
};

/** @param {any} [componentId] @returns {any} */
export const createClientInstanceId = (componentId = 'pivot-grid') => (
    `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
);

// ─── UI Helper Utilities ───────────────────────────────────────────────────

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const clampSidebarWidth = (value, fallback = 288) => {
    /** @type {any} */
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return fallback;
    return Math.max(200, Math.min(600, Math.floor(normalized)));
};

/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const clampDecimalPlaces = (value, fallback = 0) => {
    /** @type {any} */
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return fallback;
    return Math.max(0, Math.min(6, Math.floor(normalized)));
};

/** @param {any} [value] @returns {any} */
export const normalizeDefaultValueFormat = (value) => {
    /** @type {any} */
    const normalized = typeof value === 'string' ? value.trim() : '';
    if (normalized.startsWith('currency:') || normalized.startsWith('accounting:')) return normalized;
    return SUPPORTED_DEFAULT_NUMBER_FORMATS.has(normalized) ? normalized : '';
};

/** @param {any} [previousSchema] @param {any} [incomingSchema] @param {any} [fallbackSize] @returns {any} */
export const mergeSparseColSchema = (previousSchema, incomingSchema, fallbackSize = 140) => {
    if (!incomingSchema || typeof incomingSchema !== 'object') return previousSchema;
    /** @type {any} */
    const totalRaw = Number(incomingSchema.total_center_cols);
    if (!Number.isFinite(totalRaw) || totalRaw < 0) return previousSchema;

    /** @type {any} */
    const total = Math.max(0, Math.floor(totalRaw));
    /** @type {any} */
    const sameLayout = !incomingSchema.layoutRevision
        || incomingSchema.layoutRevision === previousSchema?.layoutRevision;
    /** @type {any} */
    const nextColumns = Array.from({ length: total }, (_, index) => {
        /** @type {any} */
        const previousColumn = sameLayout && previousSchema
            && previousSchema.total_center_cols === total
            && Array.isArray(previousSchema.columns)
            ? previousSchema.columns[index]
            : null;
        return previousColumn || null;
    });

    for (const rawColumn of (incomingSchema.columns || [])) {
        if (!rawColumn || typeof rawColumn !== 'object') continue;
        /** @type {any} */
        const indexRaw = Number(rawColumn.index);
        if (!Number.isFinite(indexRaw)) continue;
        /** @type {any} */
        const index = Math.floor(indexRaw);
        if (index < 0 || index >= total) continue;
        nextColumns[index] = {
            ...rawColumn,
            index,
            size: Number.isFinite(Number(rawColumn.size))
                ? Number(rawColumn.size)
                : fallbackSize,
        };
    }

    return {
        total_center_cols: total,
        layoutRevision: incomingSchema.layoutRevision || previousSchema?.layoutRevision || null,
        columns: nextColumns,
    };
};

/** @param {any} [schema] @param {any} [start] @param {any} [end] @returns {any} */
export const isSparseSchemaRangeLoaded = (schema, start, end) => {
    if (!schema || !Array.isArray(schema.columns)) return false;
    /** @type {any} */
    const total = Number.isFinite(Number(schema.total_center_cols))
        ? Math.max(0, Math.floor(Number(schema.total_center_cols)))
        : schema.columns.length;
    if (total === 0) return true;
    if (start === null || start === undefined || end === null || end === undefined) return false;
    /** @type {any} */
    const safeStart = Math.max(0, Math.min(Math.floor(start), total - 1));
    /** @type {any} */
    const safeEnd = Math.max(safeStart, Math.min(Math.floor(end), total - 1));
    for (let index = safeStart; index <= safeEnd; index += 1) {
        const entry = schema.columns[index];
        if (!entry || typeof entry.id !== 'string' || !entry.id) return false;
    }
    return true;
};

/** @param {any} [value] @returns {any} */
export const normalizePerformanceConfigValue = (value) => {
    /** @type {any} */
    const source = value && typeof value === 'object' ? value : {};
    return {
        cacheBlockSize: clampOptionalInteger(source.cacheBlockSize, 16, 1024),
        maxBlocksInCache: clampOptionalInteger(source.maxBlocksInCache, 8, 5000),
        blockLoadDebounceMs: clampOptionalInteger(source.blockLoadDebounceMs, 0, 500),
        rowOverscan: clampOptionalInteger(source.rowOverscan, 0, 64),
        columnOverscan: clampOptionalInteger(source.columnOverscan, 0, 16),
        prefetchColumns: clampOptionalInteger(source.prefetchColumns, 0, 32),
        executionMode: typeof source.executionMode === 'string' && ['auto', 'client', 'server'].includes(source.executionMode.trim().toLowerCase()) ? source.executionMode.trim().toLowerCase() : undefined,
        clientEngine: typeof source.clientEngine === 'string' && ['auto', 'duckdb-wasm', 'memory'].includes(source.clientEngine.trim().toLowerCase()) ? source.clientEngine.trim().toLowerCase() : undefined,
        duckdbWasmThreshold: clampOptionalInteger(source.duckdbWasmThreshold, 0, 10000000),
        clientRowThreshold: clampOptionalInteger(source.clientRowThreshold, 1, 10000000),
        dataUrl: typeof source.dataUrl === 'string' && source.dataUrl.trim() ? source.dataUrl.trim() : undefined,
        duckdbWasmCdnUrl: typeof source.duckdbWasmCdnUrl === 'string' && source.duckdbWasmCdnUrl.trim() ? source.duckdbWasmCdnUrl.trim() : undefined,
        duckdbWasmInstance: source.duckdbWasmInstance || undefined,
        legacyClientTable: Boolean(source.legacyClientTable),
    };
};

// ─── Advisory/Filter Utilities ─────────────────────────────────────────────

/** @param {any} p0 @returns {any} */
export const buildLargeColumnAdvisory = ({ serverSide, totalCenterCols, colFields }) => {
    /** @type {any} */
    const normalizedCount = Number(totalCenterCols);
    if (!serverSide || !Number.isFinite(normalizedCount) || normalizedCount < SOFT_CENTER_COLUMN_WARNING_THRESHOLD) {
        return null;
    }

    /** @type {any} */
    const safeCount = Math.max(0, Math.floor(normalizedCount));
    /** @type {any} */
    const tone = safeCount >= HARD_CENTER_COLUMN_WARNING_THRESHOLD ? 'warning' : 'info';
    /** @type {any} */
    const formattedCount = new Intl.NumberFormat().format(safeCount);
    /** @type {any} */
    const fieldLabel = Array.isArray(colFields) && colFields.length > 0
        ? colFields.map((field) => formatDisplayLabel(field)).join(' × ')
        : 'the current column shape';

    return {
        tone,
        label: tone === 'warning' ? `Wide pivot: ${formattedCount} cols` : `${formattedCount} cols`,
        detail: `${fieldLabel} creates ${formattedCount} center columns. Bucket numeric fields, roll up dates, or move one field to Rows or Filters for smoother interaction.`,
        notification: `${fieldLabel} creates ${formattedCount} center columns. For smoother interaction, bucket numeric fields, roll up dates, or move one field to Rows or Filters.`,
    };
};

/** @param {any} [value] @returns {any} */
export const hasActiveFilterValue = (value) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'string') return value.trim().length > 0;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'object') {
        return Object.keys(value).length > 0;
    }
    return true;
};

/** @param {any} [filters] @returns {any} */
export const countActiveFilters = (filters) => {
    if (!filters || typeof filters !== 'object') return 0;
    return Object.values(filters).reduce(
        (count, value) => count + (hasActiveFilterValue(value) ? 1 : 0),
        0
    );
};

// ─── Row Pinning Utilities ─────────────────────────────────────────────────

/** @param {any} [value] @returns {any} */
export const normalizeRowPinningState = (value) => ({
    top: Array.isArray(value && value.top) ? value.top : [],
    bottom: Array.isArray(value && value.bottom) ? value.bottom : [],
});

/** @param {any} [rowPinning] @param {any} [rowId] @returns {any} */
export const getPinnedSideForRow = (rowPinning, rowId) => {
    /** @type {any} */
    const normalized = normalizeRowPinningState(rowPinning);
    if (normalized.top.includes(rowId)) return 'top';
    if (normalized.bottom.includes(rowId)) return 'bottom';
    return null;
};

/** @param {any} [value] @returns {any} */
export const sanitizeGrandTotalPinOverride = (value) => (
    value === 'top' || value === 'bottom' || value === false ? value : null
);

/** @param {any} [showColTotals] @param {any} [grandTotalPosition] @param {any} [grandTotalPinOverride] @returns {any} */
export const resolveGrandTotalPinState = (showColTotals, grandTotalPosition, grandTotalPinOverride) => {
    if (!showColTotals) return false;
    if (grandTotalPinOverride === false || grandTotalPinOverride === 'top' || grandTotalPinOverride === 'bottom') {
        return grandTotalPinOverride;
    }
    return grandTotalPosition === 'bottom' ? 'bottom' : 'top';
};

/** @param {any} [rowPinning] @param {any} [rowId] @param {any} [pinState] @returns {any} */
export const applyRowPinning = (rowPinning, rowId, pinState) => {
    /** @type {any} */
    const normalized = normalizeRowPinningState(rowPinning);
    /** @type {Record<string, any>} */
    const next = {
        ...normalized,
        top: normalized.top.filter(/** @param {any} [id] @returns {any} */ id => id !== rowId),
        bottom: normalized.bottom.filter(/** @param {any} [id] @returns {any} */ id => id !== rowId),
    };
    if (pinState === 'top') next.top.push(rowId);
    if (pinState === 'bottom') next.bottom.push(rowId);
    return next;
};

import { formatDisplayLabel } from '../../utils/helpers';
import {
    applyCustomDimensionsToRows,
    buildCustomCategoryFieldId,
    evaluateCustomCategoryCondition,
    normalizeCustomCategoryId,
    normalizeCustomDimensionsValue,
} from '../../hooks/usePivotNormalization';

export const CUSTOM_CATEGORY_OPERATORS = [
    { value: 'eq', label: 'Equals' }, { value: 'ne', label: 'Does not equal' },
    { value: 'contains', label: 'Contains' }, { value: 'starts_with', label: 'Starts with' },
    { value: 'ends_with', label: 'Ends with' }, { value: 'gt', label: '>' },
    { value: 'gte', label: '>=' }, { value: 'lt', label: '<' }, { value: 'lte', label: '<=' },
    { value: 'in', label: 'In list' }, { value: 'not_in', label: 'Not in list' },
    { value: 'is_null', label: 'Is empty' }, { value: 'is_not_null', label: 'Is not empty' },
];
export const CUSTOM_CATEGORY_LIST_OPERATORS = new Set(['in', 'not_in']);
export const CUSTOM_CATEGORY_VALUELESS_OPERATORS = new Set(['is_null', 'is_not_null']);

export function createCustomCategoryClause(availableFields) {
    return { field: (availableFields && availableFields[0]) || '', operator: 'eq', value: '', values: [], caseSensitive: false };
}

export function createCustomCategoryRule(availableFields, index = 0) {
    return {
        id: `rule-${Date.now().toString(36)}-${index}`,
        label: index === 0 ? 'Group A' : `Group ${index + 1}`,
        condition: { op: 'AND', clauses: [createCustomCategoryClause(availableFields)] },
    };
}

export function createCustomCategoryDraft(availableFields, existing = null) {
    if (existing) {
        const normalized = normalizeCustomDimensionsValue([existing])[0] || existing;
        return { ...JSON.parse(JSON.stringify(normalized)), __editingField: normalized.field, __editingId: normalized.id };
    }
    const id = normalizeCustomCategoryId(`category_${Date.now().toString(36)}`);
    return {
        id,
        field: buildCustomCategoryFieldId(id),
        name: 'New Category',
        fallbackLabel: 'Other',
        order: 'creation',
        rules: [createCustomCategoryRule(availableFields, 0)],
        __isNew: true,
    };
}

export function inferFieldTypeFromRows(rows, field) {
    if (!field) return 'string';
    const sampleRows = Array.isArray(rows) ? rows.slice(0, 250) : [];
    for (let index = 0; index < sampleRows.length; index += 1) {
        const row = sampleRows[index];
        if (!row || typeof row !== 'object') continue;
        const value = row[field];
        if (value === null || value === undefined || value === '') continue;
        if (typeof value === 'number' && Number.isFinite(value)) return 'number';
        if (value instanceof Date && !Number.isNaN(value.getTime())) return 'date';
        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (!trimmed) continue;
            if (Number.isFinite(Number(trimmed))) return 'number';
            const parsedDate = Date.parse(trimmed);
            if (!Number.isNaN(parsedDate) && /[-/T:]/.test(trimmed)) return 'date';
            return 'string';
        }
    }
    return 'string';
}

export const getTypedFields = (rows, fields, targetType) => (
    (Array.isArray(fields) ? fields : []).filter((field) => inferFieldTypeFromRows(rows, field) === targetType)
);

export function createDateGroupDraft(availableFields, data) {
    const dateFields = getTypedFields(data, availableFields, 'date');
    const sourceField = dateFields[0] || availableFields[0] || '';
    const id = normalizeCustomCategoryId(`${sourceField || 'date'}_month`);
    return {
        id, field: buildCustomCategoryFieldId(id), kind: 'date_group',
        name: sourceField ? `${formatDisplayLabel(sourceField)} Month` : 'Date Month',
        sourceField, granularity: 'month', fiscalYearStartMonth: 1,
        fallbackLabel: 'No date', __isNew: true,
    };
}

export function createNumberGroupDraft(availableFields, data) {
    const numberFields = getTypedFields(data, availableFields, 'number');
    const sourceField = numberFields[0] || availableFields[0] || '';
    const id = normalizeCustomCategoryId(`${sourceField || 'number'}_bins`);
    return {
        id, field: buildCustomCategoryFieldId(id), kind: 'number_group',
        name: sourceField ? `${formatDisplayLabel(sourceField)} Bins` : 'Number Bins',
        sourceField, binSize: 10, minValue: '', maxValue: '', precision: 0,
        fallbackLabel: 'No value', __isNew: true,
    };
}

export const splitListInput = (value) => String(value || '').split(',').map((item) => item.trim()).filter(Boolean);
export const normalizeCustomCategoryNameKey = (value) => String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();
export const getCustomCategoryDraftField = (draft) => (draft && (draft.__editingField || draft.field) ? (draft.__editingField || draft.field) : '');

export function getAllowedCustomCategoryDependencies(normalizedDimensions, draft) {
    const dimensions = Array.isArray(normalizedDimensions) ? normalizedDimensions : [];
    const editingField = getCustomCategoryDraftField(draft);
    if (!editingField) return dimensions;
    const editingIndex = dimensions.findIndex((dimension) => dimension && dimension.field === editingField);
    return editingIndex >= 0 ? dimensions.slice(0, editingIndex) : dimensions;
}

export function collectCustomCategoryClauseFields(dimension) {
    const fields = [];
    (dimension && Array.isArray(dimension.rules) ? dimension.rules : []).forEach((rule) => {
        const clauses = rule && rule.condition && Array.isArray(rule.condition.clauses) ? rule.condition.clauses : [];
        clauses.forEach((clause) => { if (clause && clause.field) fields.push(clause.field); });
    });
    return Array.from(new Set(fields));
}

export function getCustomCategoryValidationRows(data, dependencyDimensions) {
    const rows = (Array.isArray(data) ? data : [])
        .filter((row) => row && typeof row === 'object')
        .filter((row) => !row._isTotal && row._path !== '__grand_total__' && row._path !== '__color_scale_stats__');
    return applyCustomDimensionsToRows(rows, dependencyDimensions || []);
}

export function canValidateCustomCategoryRows(rows, clauseFields) {
    if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(clauseFields) || clauseFields.length === 0) return false;
    return clauseFields.every((field) => rows.some((row) => row && Object.prototype.hasOwnProperty.call(row, field)));
}

export const getCustomCategoryRuleMatchSummary = (rows, dimension) => (
    (dimension && Array.isArray(dimension.rules) ? dimension.rules : []).map((rule) => ({
        label: rule && rule.label ? rule.label : 'Rule',
        count: rows.reduce((matches, row) => (evaluateCustomCategoryCondition(row, rule.condition) ? matches + 1 : matches), 0),
    }))
);

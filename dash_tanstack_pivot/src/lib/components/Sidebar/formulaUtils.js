/** Pure formula parsing and value-sparkline defaults used by sidebar editors. */

export const FORMULA_REFERENCE_RE = /\b[A-Za-z_][A-Za-z0-9_]*\b/g;
export const SIMPLE_FORMULA_REFERENCE_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
export const COLUMN_FORMULA_REFERENCE_RE = /\[([^\]]+)\]/g;
export const FORMULA_FUNCTION_NAMES = new Set(['abs', 'round', 'ceil', 'floor', 'max', 'min', 'int', 'float']);
export const FORMULA_RESERVED_REFERENCES = new Set(['True', 'False', 'None', 'and', 'or', 'not', 'if', 'else', 'in', 'is']);

export const DEFAULT_VALUE_SPARKLINE_CONFIG = {
    type: 'line',
    metric: 'last',
    showCurrentValue: true,
    showDelta: true,
    compact: false,
    hideColumns: false,
    placement: 'before',
    source: 'pivot',
    displayMode: 'trend',
};

export function escapeRegExp(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function normalizeFormulaReferenceKey(value, fallback = 'formula') {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '')
        .replace(/[^a-z0-9_]/g, '');
    const base = normalized || String(fallback || 'formula').toLowerCase().replace(/[^a-z0-9_]/g, '') || 'formula';
    return /^[a-z_]/.test(base) ? base : `f_${base}`;
}

export function getFormulaReferenceKey(config) {
    if (!config || typeof config !== 'object') return '';
    return String(
        config.formulaRef
        || normalizeFormulaReferenceKey(config.label || '', config.field || 'formula')
        || config.field
        || ''
    ).trim();
}

export function normalizeFormulaScope(value) {
    const scope = String(value || '').trim().toLowerCase();
    return ['columns', 'display', 'displayed', 'displayed_columns', 'rendered', 'rendered_columns'].includes(scope)
        ? 'columns'
        : 'measures';
}

export function isColumnFormulaScope(value) {
    return normalizeFormulaScope(value) === 'columns';
}

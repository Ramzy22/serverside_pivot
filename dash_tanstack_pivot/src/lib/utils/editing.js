const VALID_EDITOR_TYPES = new Set([
    'text',
    'number',
    'select',
    'richSelect',
    'checkbox',
    'date',
    'textarea',
]);

const DISTINCT_OPTION_KINDS = new Set(['distinct', 'asyncDistinct', 'serverDistinct']);

const defaultValidationMessage = (rule, columnLabel = 'Value') => {
    switch (String(rule && rule.type || '').trim().toLowerCase()) {
    case 'required':
        return `${columnLabel} is required.`;
    case 'numeric':
        return `${columnLabel} must be numeric.`;
    case 'integer':
        return `${columnLabel} must be an integer.`;
    case 'min':
        return `${columnLabel} must be at least ${rule.value}.`;
    case 'max':
        return `${columnLabel} must be at most ${rule.value}.`;
    case 'minlength':
        return `${columnLabel} must be at least ${rule.value} characters.`;
    case 'maxlength':
        return `${columnLabel} must be at most ${rule.value} characters.`;
    case 'regex':
        return `${columnLabel} has an invalid format.`;
    case 'date':
        return `${columnLabel} must be a valid date.`;
    case 'oneof':
        return `${columnLabel} must match one of the allowed values.`;
    case 'ltfield':
        return `${columnLabel} must be less than ${rule.field}.`;
    case 'ltefield':
        return `${columnLabel} must be less than or equal to ${rule.field}.`;
    case 'gtfield':
        return `${columnLabel} must be greater than ${rule.field}.`;
    case 'gtefield':
        return `${columnLabel} must be greater than or equal to ${rule.field}.`;
    case 'eqfield':
        return `${columnLabel} must match ${rule.field}.`;
    case 'neqfield':
        return `${columnLabel} must differ from ${rule.field}.`;
    default:
        return `${columnLabel} is invalid.`;
    }
};

const pushLookupKey = (set, rawValue) => {
    if (rawValue === null || rawValue === undefined) return;
    const normalized = String(rawValue).trim();
    if (!normalized) return;
    set.add(normalized);
};

export const normalizeEditingConfig = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const normalizedMode = String(source.mode || source.editMode || 'hybrid').trim().toLowerCase();
    const mode = ['cell', 'row', 'hybrid'].includes(normalizedMode) ? normalizedMode : 'hybrid';
    const columns = source.columns && typeof source.columns === 'object' ? source.columns : {};
    return {
        mode,
        rowActions: source.rowActions !== false,
        validateOnChange: source.validateOnChange !== false,
        validateOnBlur: source.validateOnBlur !== false,
        columns,
    };
};

export const getColumnEditLookupKeys = (columnId, columnConfig = null) => {
    const lookupKeys = new Set();
    pushLookupKey(lookupKeys, columnId);
    if (columnConfig && typeof columnConfig === 'object') {
        pushLookupKey(lookupKeys, columnConfig.id);
        pushLookupKey(lookupKeys, columnConfig.field);
        const agg = columnConfig.agg ? String(columnConfig.agg).trim().toLowerCase() : '';
        if (columnConfig.field && agg) {
            pushLookupKey(lookupKeys, `${columnConfig.field}_${agg}`);
        }
    }
    return Array.from(lookupKeys);
};

export const normalizeEditorOptions = (options) => {
    if (!Array.isArray(options)) return [];
    return options.reduce((acc, entry) => {
        if (entry === undefined || entry === null) return acc;
        if (typeof entry === 'object' && !Array.isArray(entry)) {
            const optionValue = Object.prototype.hasOwnProperty.call(entry, 'value') ? entry.value : entry.id;
            if (optionValue === undefined) return acc;
            acc.push({
                value: optionValue,
                label: entry.label !== undefined && entry.label !== null ? String(entry.label) : String(optionValue),
            });
            return acc;
        }
        acc.push({
            value: entry,
            label: String(entry),
        });
        return acc;
    }, []);
};

export const normalizeEditorType = (value, fallbackValue = undefined) => {
    const normalized = String(value || '').trim().toLowerCase();
    if (VALID_EDITOR_TYPES.has(normalized)) return normalized;
    if (normalized === 'dropdown') return 'select';
    if (normalized === 'toggle' || normalized === 'boolean') return 'checkbox';
    if (normalized === 'multiline') return 'textarea';
    if (typeof fallbackValue === 'boolean') return 'checkbox';
    if (typeof fallbackValue === 'number') return 'number';
    return 'text';
};

export const resolveEditorOptionsSource = (editorConfig, columnId, columnConfig = null) => {
    if (!editorConfig || typeof editorConfig !== 'object' || !editorConfig.optionsSource || typeof editorConfig.optionsSource !== 'object') {
        return null;
    }
    const source = editorConfig.optionsSource;
    const kind = String(source.kind || '').trim();
    if (!DISTINCT_OPTION_KINDS.has(kind)) return null;
    return {
        kind: 'distinct',
        columnId: String(
            source.columnId
            || source.field
            || source.sourceColumnId
            || (columnConfig && columnConfig.field)
            || columnId
            || ''
        ).trim(),
    };
};

export const resolveColumnValidationRules = (validationRules, columnId, columnConfig = null, extraRules = null) => {
    const lookupKeys = getColumnEditLookupKeys(columnId, columnConfig);
    let resolved = [];
    if (validationRules && typeof validationRules === 'object') {
        for (const key of lookupKeys) {
            if (Array.isArray(validationRules[key])) {
                resolved = validationRules[key];
                break;
            }
        }
    }
    if (Array.isArray(extraRules) && extraRules.length > 0) {
        resolved = [...resolved, ...extraRules];
    }
    return resolved.filter((rule) => rule && typeof rule === 'object');
};

export const resolveColumnEditSpec = ({
    editingConfig,
    validationRules,
    columnId,
    columnConfig = null,
    currentValue = undefined,
    defaultEditable = false,
}) => {
    const normalizedConfig = normalizeEditingConfig(editingConfig);
    const lookupKeys = getColumnEditLookupKeys(columnId, columnConfig);
    let rawColumnConfig = null;
    for (const key of lookupKeys) {
        if (normalizedConfig.columns && normalizedConfig.columns[key] && typeof normalizedConfig.columns[key] === 'object') {
            rawColumnConfig = normalizedConfig.columns[key];
            break;
        }
    }

    if (!rawColumnConfig && !defaultEditable) {
        return null;
    }

    const editor = normalizeEditorType(rawColumnConfig && rawColumnConfig.editor, currentValue);
    const options = normalizeEditorOptions(rawColumnConfig && rawColumnConfig.options);
    const optionsSource = resolveEditorOptionsSource(rawColumnConfig, columnId, columnConfig);
    const mergedValidationRules = resolveColumnValidationRules(
        validationRules,
        columnId,
        columnConfig,
        rawColumnConfig && Array.isArray(rawColumnConfig.validationRules)
            ? rawColumnConfig.validationRules
            : (rawColumnConfig && Array.isArray(rawColumnConfig.validation) ? rawColumnConfig.validation : null),
    );

    return {
        editor,
        editable: rawColumnConfig ? rawColumnConfig.editable !== false : true,
        placeholder: rawColumnConfig && rawColumnConfig.placeholder ? String(rawColumnConfig.placeholder) : '',
        min: rawColumnConfig && rawColumnConfig.min !== undefined ? rawColumnConfig.min : undefined,
        max: rawColumnConfig && rawColumnConfig.max !== undefined ? rawColumnConfig.max : undefined,
        step: rawColumnConfig && rawColumnConfig.step !== undefined ? rawColumnConfig.step : undefined,
        rows: rawColumnConfig && rawColumnConfig.rows !== undefined ? rawColumnConfig.rows : 3,
        options,
        optionsSource,
        allowCustomValue: rawColumnConfig ? rawColumnConfig.allowCustomValue !== false : editor === 'richSelect',
        saveOnChange: rawColumnConfig && rawColumnConfig.saveOnChange !== undefined
            ? Boolean(rawColumnConfig.saveOnChange)
            : (editor === 'checkbox' || editor === 'select'),
        validationRules: mergedValidationRules,
    };
};

export const findEditorOption = (options, value) => {
    const normalized = normalizeEditorOptions(options);
    return normalized.find((option) => Object.is(option.value, value) || String(option.value) === String(value)) || null;
};

export const formatEditorDisplayValue = (value, editorConfig, fallbackFormatter) => {
    if (value === undefined || value === null) return '';
    const editor = normalizeEditorType(editorConfig && editorConfig.editor, value);
    if (editor === 'checkbox') {
        return value ? 'True' : 'False';
    }
    if (editor === 'select' || editor === 'richSelect') {
        const matched = findEditorOption(editorConfig && editorConfig.options, value);
        return matched ? matched.label : String(value);
    }
    return typeof fallbackFormatter === 'function' ? fallbackFormatter(value) : String(value);
};

export const coerceEditorValue = (value, editorConfig, currentValue = undefined) => {
    const editor = normalizeEditorType(editorConfig && editorConfig.editor, currentValue);
    if (editor === 'checkbox') {
        if (typeof value === 'boolean') return value;
        return value === 'true' || value === '1' || value === 1;
    }
    if (editor === 'number') {
        if (value === '' || value === null || value === undefined) return null;
        const numericValue = Number(value);
        return Number.isNaN(numericValue) ? value : numericValue;
    }
    if (editor === 'date') {
        return value ? String(value) : '';
    }
    return value;
};

export const toEditorInputValue = (value, editorConfig) => {
    const editor = normalizeEditorType(editorConfig && editorConfig.editor, value);
    if (editor === 'checkbox') return Boolean(value);
    if (value === undefined || value === null) return '';
    if (editor === 'date') return String(value).slice(0, 10);
    return value;
};

const isEmptyValue = (value) => (
    value === null
    || value === undefined
    || value === ''
);

const resolveComparisonValue = (rowValues, field) => {
    if (!rowValues || typeof rowValues !== 'object' || !field) return undefined;
    return rowValues[field];
};

export const validateEditorValue = (value, rules, context = {}) => {
    const normalizedRules = Array.isArray(rules) ? rules : [];
    const columnLabel = context.columnLabel || 'Value';
    for (const rule of normalizedRules) {
        const type = String(rule && rule.type || '').trim().toLowerCase();
        if (!type) continue;
        if (type === 'required' && isEmptyValue(value)) {
            return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
        }
        if (isEmptyValue(value)) continue;

        if (type === 'numeric') {
            const numericValue = Number(value);
            if (!Number.isFinite(numericValue)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'integer') {
            const numericValue = Number(value);
            if (!Number.isInteger(numericValue)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'min') {
            if (Number(value) < Number(rule.value)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'max') {
            if (Number(value) > Number(rule.value)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'minlength') {
            if (String(value).length < Number(rule.value)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'maxlength') {
            if (String(value).length > Number(rule.value)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'regex') {
            const regex = new RegExp(rule.pattern || '');
            if (!regex.test(String(value))) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'date') {
            const parsedDate = Date.parse(String(value));
            if (Number.isNaN(parsedDate)) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (type === 'oneof') {
            const allowedValues = Array.isArray(rule.values) ? rule.values : [];
            const matched = allowedValues.some((candidate) => Object.is(candidate, value) || String(candidate) === String(value));
            if (!matched) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
            continue;
        }
        if (['ltfield', 'ltefield', 'gtfield', 'gtefield', 'eqfield', 'neqfield'].includes(type)) {
            const comparisonValue = resolveComparisonValue(context.rowValues, rule.field);
            if (comparisonValue === undefined) continue;
            const leftNumeric = Number(value);
            const rightNumeric = Number(comparisonValue);
            const useNumeric = Number.isFinite(leftNumeric) && Number.isFinite(rightNumeric);
            const left = useNumeric ? leftNumeric : String(value);
            const right = useNumeric ? rightNumeric : String(comparisonValue);
            const passes = (
                (type === 'ltfield' && left < right)
                || (type === 'ltefield' && left <= right)
                || (type === 'gtfield' && left > right)
                || (type === 'gtefield' && left >= right)
                || (type === 'eqfield' && left === right)
                || (type === 'neqfield' && left !== right)
            );
            if (!passes) {
                return { valid: false, error: rule.message || defaultValidationMessage(rule, columnLabel) };
            }
        }
    }
    return { valid: true, error: null };
};

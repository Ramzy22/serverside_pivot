import React from 'react';

/** @param {any} [prefix] @param {any} [field] @param {any} [agg] @returns {any} */
export const getKey = (prefix, field, agg) => prefix ? `${prefix}_${field}_${agg}` : `${field}_${agg}`;

/** @param {any} [value] @returns {any} */
export const formatDisplayLabel = (value) => {
    if (value === null || value === undefined) return '';
    /** @type {any} */
    const raw = String(value).trim();
    if (!raw) return '';

    return raw
        .replace(/[_]+/g, ' ')
        .replace(/\s+/g, ' ')
        .split(' ')
        .map(/** @param {any} [word] @returns {any} */ (word) => {
            if (!word) return word;
            if (/^[A-Z0-9]+$/.test(word)) return word;
            return word.charAt(0).toUpperCase() + word.slice(1);
        })
        .join(' ');
};

/** @param {any} [agg] @returns {any} */
export const isWeightedAverageAgg = (agg) => {
    /** @type {any} */
    const normalized = String(agg || '').trim().toLowerCase();
    return normalized === 'weighted_avg' || normalized === 'wavg' || normalized === 'weighted_mean';
};

/** @param {any} [agg] @param {any} [weightField] @returns {any} */
export const formatAggLabel = (agg, weightField = null) => {
    /** @type {any} */
    const normalized = String(agg || '').trim().toLowerCase();
    /** @type {Record<string, any>} */
    const labels = {
        sum: 'Sum',
        avg: 'Avg',
        count: 'Cnt',
        min: 'Min',
        max: 'Max',
        weighted_avg: 'Weighted Avg',
        wavg: 'Weighted Avg',
        weighted_mean: 'Weighted Avg',
    };

    /** @type {any} */
    const baseLabel = labels[normalized] || formatDisplayLabel(normalized);
    if (isWeightedAverageAgg(normalized) && weightField) {
        return `${baseLabel} by ${formatDisplayLabel(weightField)}`;
    }
    return baseLabel;
};

export const DEFAULT_NUMBER_GROUP_SEPARATOR = 'thin_space';
export const DEFAULT_CURRENCY_SYMBOL = '$';

/** @type {any} */
const NUMBER_GROUP_SEPARATOR_CHAR_MAP = Object.freeze({
    comma: ',',
    space: '\u00A0',
    thin_space: '\u202F',
    // Export/copy-only internal value. Keep the user-facing "space" option
    // non-breaking in the grid, but make exported text portable plain text.
    plain_space: ' ',
    apostrophe: '\'',
    none: '',
});

/** @param {any} [value] @returns {any} */
export const normalizeNumberGroupSeparator = (value) => (
    Object.prototype.hasOwnProperty.call(NUMBER_GROUP_SEPARATOR_CHAR_MAP, value)
        ? value
        : DEFAULT_NUMBER_GROUP_SEPARATOR
);

/** @param {any} [str] @param {any} [groupSeparator] @returns {any} */
const applyGroupSeparator = (str, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR) => {
    /** @type {any} */
    const normalized = normalizeNumberGroupSeparator(groupSeparator);
    return str.replace(/,/g, NUMBER_GROUP_SEPARATOR_CHAR_MAP[normalized]);
};

/** @param {any} [value] @param {any} [options] @param {any} [groupSeparator] @returns {any} */
const formatNumberWithOptions = (value, options = {}, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR) => (
    applyGroupSeparator(new Intl.NumberFormat('en-US', options).format(value), groupSeparator)
);

/** @param {any} [fmt] @returns {any} */
const parseCurrencyFormat = (fmt) => {
    /** @type {any} */
    const normalized = typeof fmt === 'string' ? fmt.trim() : '';
    if (normalized === 'currency' || normalized === 'accounting') {
        return {
            type: normalized,
            symbol: DEFAULT_CURRENCY_SYMBOL,
        };
    }
    if (normalized.startsWith('currency:')) {
        return {
            type: 'currency',
            symbol: normalized.slice('currency:'.length),
        };
    }
    if (normalized.startsWith('accounting:')) {
        return {
            type: 'accounting',
            symbol: normalized.slice('accounting:'.length),
        };
    }
    return null;
};

/** @param {any} [value] @param {any} [symbol] @param {any} [decimalPlaces] @param {any} [groupSeparator] @param {any} [accounting] @returns {any} */
const formatCurrencyWithSymbol = (value, symbol, decimalPlaces, groupSeparator, accounting = false) => {
    /** @type {any} */
    const safeSymbol = symbol || DEFAULT_CURRENCY_SYMBOL;
    /** @type {any} */
    const absFormatted = formatNumberWithOptions(Math.abs(value), {
        minimumFractionDigits: decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2,
        maximumFractionDigits: decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2,
    }, groupSeparator);

    if (value < 0) {
        return accounting
            ? `(${safeSymbol}${absFormatted})`
            : `-${safeSymbol}${absFormatted}`;
    }
    return `${safeSymbol}${absFormatted}`;
};

/** @param {any} [value] @returns {any} */
const formatNonScalarValue = (value) => {
    if (typeof value === 'string') return value;
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (Array.isArray(value)) {
        if (value.length === 0) return '';
        const first = value[0];
        if (first && typeof first === 'object' && ('x' in first || 'y' in first || 'value' in first)) {
            return `${value.length} points`;
        }
        return `${value.length} items`;
    }
    if (value && typeof value === 'object') {
        if ('x' in value && 'y' in value) return `${value.x}: ${value.y}`;
        if ('label' in value && 'value' in value) return `${value.label}: ${value.value}`;
        try {
            return JSON.stringify(value);
        } catch (error) {
            return String(value);
        }
    }
    return String(value);
};

/** @param {any} [value] @returns {any} */
export const normalizeNegativeZero = (value) => (Object.is(value, -0) || value === 0 ? 0 : value);

/** @param {any} [value] @param {any} [valueScale] @returns {any} */
export const scaleValueForDisplay = (value, valueScale = null) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return value;
    /** @type {any} */
    const scaleNum = Number(valueScale);
    if (Number.isFinite(scaleNum) && scaleNum !== 0 && scaleNum !== 1) {
        return normalizeNegativeZero(value / scaleNum);
    }
    return normalizeNegativeZero(value);
};

/** @param {any} [value] @param {any} [decimals] @returns {any} */
const roundHalfAwayFromZero = (value, decimals) => {
    if (!Number.isFinite(value)) return value;
    /** @type {any} */
    const d = Math.max(0, Math.min(20, Math.floor(Number(decimals))));
    /** @type {any} */
    const factor = 10 ** d;
    /** @type {any} */
    const scaled = value * factor;
    /** @type {any} */
    const rounded = value < 0 ? -Math.round(-scaled) : Math.round(scaled);
    return normalizeNegativeZero(rounded / factor);
};

/** @param {any} [value] @param {any} [decimalPlaces] @returns {any} */
const resolveCompactSpec = (value, decimalPlaces) => {
    /** @type {any} */
    const abs = Math.abs(value);
    let divisor = 1;
    let suffix = '';
    if (abs >= 1e9) {
        divisor = 1e9;
        suffix = 'B';
    } else if (abs >= 1e6) {
        divisor = 1e6;
        suffix = 'M';
    } else if (abs >= 1e3) {
        divisor = 1e3;
        suffix = 'K';
    }
    /** @type {any} */
    const scaled = value / divisor;
    /** @type {any} */
    const hasExplicitDecimals = decimalPlaces !== undefined && decimalPlaces !== null && Number.isFinite(Number(decimalPlaces));
    /** @type {any} */
    const maxDecimals = hasExplicitDecimals ? Math.max(0, Math.min(20, Math.floor(Number(decimalPlaces)))) : (divisor > 1 ? 1 : 0);
    /** @type {any} */
    const minDecimals = hasExplicitDecimals ? maxDecimals : 0;
    /** @type {any} */
    const rounded = roundHalfAwayFromZero(scaled, maxDecimals);
    // Re-evaluate threshold after rounding (e.g. 999.96K -> 1.0M or 999.9 -> 1K)
    if (divisor === 1 && Math.abs(rounded) >= 1000) {
        return resolveCompactSpec(rounded, decimalPlaces);
    }
    return { value: rounded, suffix, minDecimals, maxDecimals };
};

/** @param {any} [value] @param {any} [fmt] @param {any} [decimalPlaces] @param {any} [options] @returns {any} */
export const resolveNumericDisplayValue = (value, fmt, decimalPlaces, options = {}) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return value;
    /** @type {any} */
    const scaled = scaleValueForDisplay(value, options && options.valueScale);
    /** @type {any} */
    const fmtStr = typeof fmt === 'string' ? fmt.trim() : '';
    /** @type {any} */
    const hasExplicitDecimals = decimalPlaces !== undefined && decimalPlaces !== null && Number.isFinite(Number(decimalPlaces));

    if (fmtStr === 'compact' || fmtStr.startsWith('compact:')) {
        /** @type {any} */
        const compactDec = hasExplicitDecimals
            ? Number(decimalPlaces)
            : (fmtStr.startsWith('compact:') ? parseInt(fmtStr.slice('compact:'.length), 10) : null);
        /** @type {any} */
        const spec = resolveCompactSpec(scaled, Number.isFinite(compactDec) ? compactDec : null);
        return normalizeNegativeZero(spec.value);
    }
    if (fmtStr === 'percent') {
        /** @type {any} */
        const dec = hasExplicitDecimals ? Math.max(0, Math.min(20, Math.floor(Number(decimalPlaces)))) : 2;
        /** @type {any} */
        const pctRounded = roundHalfAwayFromZero(scaled * 100, dec);
        return normalizeNegativeZero(pctRounded / 100);
    }
    if (fmtStr === 'scientific') {
        return normalizeNegativeZero(scaled);
    }
    /** @type {any} */
    const currencyFormat = parseCurrencyFormat(fmtStr);
    if (currencyFormat) {
        /** @type {any} */
        const dec = hasExplicitDecimals ? Math.max(0, Math.min(20, Math.floor(Number(decimalPlaces)))) : 2;
        return roundHalfAwayFromZero(scaled, dec);
    }
    if (fmtStr.startsWith('fixed')) {
        /** @type {any} */
        const parts = fmtStr.split(':');
        /** @type {any} */
        const parsed = parts.length > 1 ? parseInt(parts[1], 10) : 2;
        /** @type {any} */
        const dec = hasExplicitDecimals
            ? Math.max(0, Math.min(20, Math.floor(Number(decimalPlaces))))
            : (Number.isFinite(parsed) ? Math.max(0, Math.min(20, parsed)) : 2);
        return roundHalfAwayFromZero(scaled, dec);
    }
    /** @type {any} */
    const defaultDec = hasExplicitDecimals ? Math.max(0, Math.min(20, Math.floor(Number(decimalPlaces)))) : 3;
    /** @type {any} */
    const checkZero = roundHalfAwayFromZero(scaled, defaultDec);
    if (checkZero === 0) return 0;
    return hasExplicitDecimals ? checkZero : normalizeNegativeZero(scaled);
};

/** @param {any} [value] @param {any} [fmt] @param {any} [decimalPlaces] @param {any} [options] @returns {any} */
export const isDisplayNegative = (value, fmt, decimalPlaces, options = {}) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return false;
    /** @type {any} */
    const displayNum = resolveNumericDisplayValue(value, fmt, decimalPlaces, options);
    return typeof displayNum === 'number' && displayNum < 0;
};

/** @param {any} [value] @param {any} [fmt] @param {any} [decimalPlaces] @param {any} [groupSeparator] @param {any} [formatOptions] @returns {any} */
export const formatValue = (value, fmt, decimalPlaces, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR, formatOptions = null) => {
    if (value === null || value === undefined) return '';
    if (typeof value !== 'number') return formatNonScalarValue(value);
    if (Number.isNaN(value)) return '';
    /** @type {any} */
    const extraOpts = formatOptions && typeof formatOptions === 'object' ? formatOptions : {};
    /** @type {any} */
    const unitSuffix = typeof extraOpts.unitSuffix === 'string' ? extraOpts.unitSuffix : '';
    /** @param {any} [str] @returns {any} */
    const appendUnit = (str) => (unitSuffix && str !== '' ? `${str}${unitSuffix}` : str);
    /** @type {any} */
    const scaled = scaleValueForDisplay(value, extraOpts.valueScale);
    /** @type {any} */
    const displayValue = resolveNumericDisplayValue(value, fmt, decimalPlaces, extraOpts);

    if (!fmt) {
        if (decimalPlaces !== undefined && decimalPlaces !== null) {
            return appendUnit(formatNumberWithOptions(displayValue, {
                minimumFractionDigits: decimalPlaces,
                maximumFractionDigits: decimalPlaces,
            }, groupSeparator));
        }
        return appendUnit(formatNumberWithOptions(displayValue, {}, groupSeparator));
    }

    try {
        /** @type {any} */
        const fmtStr = String(fmt).trim();
        if (fmtStr === 'compact' || fmtStr.startsWith('compact:')) {
            /** @type {any} */
            const hasExplicitDecimals = decimalPlaces !== undefined && decimalPlaces !== null && Number.isFinite(Number(decimalPlaces));
            /** @type {any} */
            const compactDec = hasExplicitDecimals
                ? Number(decimalPlaces)
                : (fmtStr.startsWith('compact:') ? parseInt(fmtStr.slice('compact:'.length), 10) : null);
            /** @type {any} */
            const spec = resolveCompactSpec(scaled, Number.isFinite(compactDec) ? compactDec : null);
            /** @type {any} */
            const numText = formatNumberWithOptions(spec.value, {
                minimumFractionDigits: spec.minDecimals,
                maximumFractionDigits: spec.maxDecimals,
            }, groupSeparator);
            return appendUnit(`${numText}${spec.suffix}`);
        }
        /** @type {any} */
        const currencyFormat = parseCurrencyFormat(fmtStr);
        if (currencyFormat) {
            return appendUnit(formatCurrencyWithSymbol(
                displayValue,
                currencyFormat.symbol,
                decimalPlaces,
                groupSeparator,
                currencyFormat.type === 'accounting'
            ));
        }
        if (fmtStr === 'percent') {
            /** @type {Record<string, any>} */
            const opts = { style: 'percent', maximumFractionDigits: decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2 };
            if (decimalPlaces !== undefined && decimalPlaces !== null) opts.minimumFractionDigits = decimalPlaces;
            return appendUnit(formatNumberWithOptions(displayValue, opts, groupSeparator));
        }
        if (fmtStr === 'scientific') {
            return appendUnit(displayValue.toExponential(decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2));
        }
        if (fmtStr.startsWith('fixed')) {
            /** @type {any} */
            const parts = fmtStr.split(':');
            /** @type {any} */
            const decimals = decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : (parts.length > 1 ? parseInt(parts[1], 10) : 2);
            return appendUnit(formatNumberWithOptions(displayValue, {
                minimumFractionDigits: decimals,
                maximumFractionDigits: decimals,
            }, groupSeparator));
        }
    } catch (e) {
        console.warn('Format error', e);
    }
    if (decimalPlaces !== undefined && decimalPlaces !== null) {
        return appendUnit(formatNumberWithOptions(displayValue, {
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces,
        }, groupSeparator));
    }
    return appendUnit(formatNumberWithOptions(displayValue, {}, groupSeparator));
};

/** @param {any} p0 @returns {any} */
export const Sparkline = ({ data = [], width = 100, height = 30, color = '#1976d2' }) => {
    if (!data || data.length < 2) return null;
    /** @type {any} */
    const min = Math.min(...data);
    /** @type {any} */
    const max = Math.max(...data);
    /** @type {any} */
    const range = max - min || 1;
    const padding = 2;
    /** @type {any} */
    const innerWidth = width - padding * 2;
    /** @type {any} */
    const innerHeight = height - padding * 2;

    // Calculate points with padding
    /** @type {any} */
    const points = data.map(/** @param {any} [d] @param {any} [i] @returns {any} */ (d, i) => ({
        x: padding + (i / (data.length - 1)) * innerWidth,
        y: padding + innerHeight - ((d - min) / range) * innerHeight
    }));

    return (
        <svg width={width} height={height} style={{ overflow: 'hidden' }}>
            <path
                d={`M ${points.map(/** @param {any} [p] @returns {any} */ p => `${p.x},${p.y}`).join(' L ')}`}
                fill="none"
                stroke={color}
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );
};

/** @param {any} [rowA] @param {any} [rowB] @param {any} [columnId] @returns {any} */
export const alphanumeric = (rowA, rowB, columnId) => {
    /** @type {any} */
    const a = rowA.getValue(columnId);
    /** @type {any} */
    const b = rowB.getValue(columnId);
    // Use localeCompare for natural alphanumeric sort
    // sensitivity: 'base' ignores case (default behavior often desired)
    // We can make this configurable later via sortOptions
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
};

/** @param {any} [column] @returns {any} */
export const isGroupColumn = (column) => {
    return column.columns && column.columns.length > 0;
};

/** @param {any} [col] @param {any} [zone] @returns {any} */
export const hasChildrenInZone = (col, zone) => {
    /** @type {any} */
    const pin = col.getIsPinned();
    if (!col.columns || col.columns.length === 0) {
        return pin === zone || (zone === 'unpinned' && !pin);
    }
    return col.columns.some(/** @param {any} [child] @returns {any} */ child => hasChildrenInZone(child, zone));
};

/** @param {any} [col] @returns {any} */
export const getAllLeafColumns = (col) => {
    if (!col.columns || col.columns.length === 0) return [col];
    return col.columns.flatMap(getAllLeafColumns);
};

/** @param {any} [column] @returns {any} */
export const getAllLeafIdsFromColumn = (column) => {
    return getAllLeafColumns(column).map(/** @param {any} [c] @returns {any} */ c => c.id);
};

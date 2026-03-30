import React from 'react';

export const getKey = (prefix, field, agg) => prefix ? `${prefix}_${field}_${agg}` : `${field}_${agg}`;

export const formatDisplayLabel = (value) => {
    if (value === null || value === undefined) return '';
    const raw = String(value).trim();
    if (!raw) return '';

    return raw
        .replace(/[_]+/g, ' ')
        .replace(/\s+/g, ' ')
        .split(' ')
        .map((word) => {
            if (!word) return word;
            if (/^[A-Z0-9]+$/.test(word)) return word;
            return word.charAt(0).toUpperCase() + word.slice(1);
        })
        .join(' ');
};

export const isWeightedAverageAgg = (agg) => {
    const normalized = String(agg || '').trim().toLowerCase();
    return normalized === 'weighted_avg' || normalized === 'wavg' || normalized === 'weighted_mean';
};

export const formatAggLabel = (agg, weightField = null) => {
    const normalized = String(agg || '').trim().toLowerCase();
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

    const baseLabel = labels[normalized] || formatDisplayLabel(normalized);
    if (isWeightedAverageAgg(normalized) && weightField) {
        return `${baseLabel} by ${formatDisplayLabel(weightField)}`;
    }
    return baseLabel;
};

export const DEFAULT_NUMBER_GROUP_SEPARATOR = 'thin_space';
export const DEFAULT_CURRENCY_SYMBOL = '$';

const NUMBER_GROUP_SEPARATOR_CHAR_MAP = Object.freeze({
    comma: ',',
    space: '\u00A0',
    thin_space: '\u202F',
    apostrophe: '\'',
    none: '',
});

export const normalizeNumberGroupSeparator = (value) => (
    Object.prototype.hasOwnProperty.call(NUMBER_GROUP_SEPARATOR_CHAR_MAP, value)
        ? value
        : DEFAULT_NUMBER_GROUP_SEPARATOR
);

const applyGroupSeparator = (str, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR) => {
    const normalized = normalizeNumberGroupSeparator(groupSeparator);
    return str.replace(/,/g, NUMBER_GROUP_SEPARATOR_CHAR_MAP[normalized]);
};

const formatNumberWithOptions = (value, options = {}, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR) => (
    applyGroupSeparator(new Intl.NumberFormat('en-US', options).format(value), groupSeparator)
);

const parseCurrencyFormat = (fmt) => {
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

const formatCurrencyWithSymbol = (value, symbol, decimalPlaces, groupSeparator, accounting = false) => {
    const safeSymbol = symbol || DEFAULT_CURRENCY_SYMBOL;
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

export const formatValue = (value, fmt, decimalPlaces, groupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR) => {
    if (value === null || value === undefined) return '';
    if (typeof value !== 'number') return value;
    if (!fmt) {
        if (decimalPlaces !== undefined && decimalPlaces !== null) {
            return formatNumberWithOptions(value, {
                minimumFractionDigits: decimalPlaces,
                maximumFractionDigits: decimalPlaces,
            }, groupSeparator);
        }
        return formatNumberWithOptions(value, {}, groupSeparator);
    }

    try {
        const currencyFormat = parseCurrencyFormat(fmt);
        if (currencyFormat) {
            return formatCurrencyWithSymbol(
                value,
                currencyFormat.symbol,
                decimalPlaces,
                groupSeparator,
                currencyFormat.type === 'accounting'
            );
        }
        if (fmt === 'percent') {
            const opts = { style: 'percent', maximumFractionDigits: decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2 };
            if (decimalPlaces !== undefined && decimalPlaces !== null) opts.minimumFractionDigits = decimalPlaces;
            return formatNumberWithOptions(value, opts, groupSeparator);
        }
        if (fmt === 'scientific') return value.toExponential(decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : 2);
        if (fmt.startsWith('fixed')) {
            const parts = fmt.split(':');
            const decimals = decimalPlaces !== undefined && decimalPlaces !== null ? decimalPlaces : (parts.length > 1 ? parseInt(parts[1], 10) : 2);
            return formatNumberWithOptions(value, {
                minimumFractionDigits: decimals,
                maximumFractionDigits: decimals,
            }, groupSeparator);
        }
    } catch (e) {
        console.warn('Format error', e);
    }
    if (decimalPlaces !== undefined && decimalPlaces !== null) {
        return formatNumberWithOptions(value, {
            minimumFractionDigits: decimalPlaces,
            maximumFractionDigits: decimalPlaces,
        }, groupSeparator);
    }
    return formatNumberWithOptions(value, {}, groupSeparator);
};

export const Sparkline = ({ data = [], width = 100, height = 30, color = '#1976d2' }) => {
    if (!data || data.length < 2) return null;
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    const padding = 2;
    const innerWidth = width - padding * 2;
    const innerHeight = height - padding * 2;

    // Calculate points with padding
    const points = data.map((d, i) => ({
        x: padding + (i / (data.length - 1)) * innerWidth,
        y: padding + innerHeight - ((d - min) / range) * innerHeight
    }));

    return (
        <svg width={width} height={height} style={{ overflow: 'hidden' }}>
            <path
                d={`M ${points.map(p => `${p.x},${p.y}`).join(' L ')}`}
                fill="none"
                stroke={color}
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );
};

export const alphanumeric = (rowA, rowB, columnId) => {
    const a = rowA.getValue(columnId);
    const b = rowB.getValue(columnId);
    // Use localeCompare for natural alphanumeric sort
    // sensitivity: 'base' ignores case (default behavior often desired)
    // We can make this configurable later via sortOptions
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
};

export const isGroupColumn = (column) => {
    return column.columns && column.columns.length > 0;
};

export const hasChildrenInZone = (col, zone) => {
    const pin = col.getIsPinned();
    if (!col.columns || col.columns.length === 0) {
        return pin === zone || (zone === 'unpinned' && !pin);
    }
    return col.columns.some(child => hasChildrenInZone(child, zone));
};

export const getAllLeafColumns = (col) => {
    if (!col.columns || col.columns.length === 0) return [col];
    return col.columns.flatMap(getAllLeafColumns);
};

export const getAllLeafIdsFromColumn = (column) => {
    return getAllLeafColumns(column).map(c => c.id);
};

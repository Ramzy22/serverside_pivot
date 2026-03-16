import React from 'react';

export const getKey = (prefix, field, agg) => prefix ? `${prefix}_${field}_${agg}` : `${field}_${agg}`;

export const formatValue = (value, fmt) => {
    if (value === null || value === undefined) return '';
    if (typeof value !== 'number') return value;
    if (!fmt) return value.toLocaleString();
    
    try {
        if (fmt === 'currency') return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value);
        if (fmt === 'accounting') return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', currencySign: 'accounting' }).format(value);
        if (fmt === 'percent') return new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 }).format(value);
        if (fmt === 'scientific') return value.toExponential(2);
        if (fmt.startsWith('fixed')) {
            const parts = fmt.split(':');
            const decimals = parts.length > 1 ? parseInt(parts[1]) : 2;
            return value.toFixed(decimals);
        }
    } catch (e) {
        console.warn('Format error', e);
    }
    return value.toLocaleString();
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

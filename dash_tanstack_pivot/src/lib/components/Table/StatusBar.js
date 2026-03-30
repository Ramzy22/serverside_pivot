import React, { useMemo } from 'react';
import { formatValue } from '../../utils/helpers';

const fmt = (n, decimals = 2, numberGroupSeparator) =>
    formatValue(n, null, decimals, numberGroupSeparator);

const StatusBar = ({ selectedCells, rowCount, visibleRowsCount, theme, isLoading = false, numberGroupSeparator }) => {
    const stats = useMemo(() => {
        const values = Object.values(selectedCells).map(v => parseFloat(v)).filter(v => !isNaN(v));
        const count = Object.keys(selectedCells).length;
        if (values.length === 0) return { count };
        
        const sum = values.reduce((a, b) => a + b, 0);
        const avg = sum / values.length;
        const min = Math.min(...values);
        const max = Math.max(...values);
        
        // Advanced stats
        const sqDiffs = values.map(v => Math.pow(v - avg, 2));
        const variance = sqDiffs.reduce((a, b) => a + b, 0) / values.length;
        const stdDev = Math.sqrt(variance);
        
        return { count, sum, avg, min, max, variance, stdDev };
    }, [selectedCells]);

    return (
        <div style={{
            height: '36px',
            borderTop: `1px solid ${theme.border}`,
            background: theme.headerBg,
            display: 'flex',
            alignItems: 'center',
            padding: '0 16px',
            justifyContent: 'space-between',
            fontSize: '12px',
            color: theme.textSec,
            boxShadow: theme.shadowInset || 'none'
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                {isLoading && (
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', color: theme.primary, fontWeight: 600 }}>
                        <span
                            aria-hidden="true"
                            style={{
                                width: '10px',
                                height: '10px',
                                border: `2px solid ${theme.primary}`,
                                borderTopColor: 'transparent',
                                borderRadius: '50%',
                                animation: 'pivot-spinner-rotate 0.75s linear infinite'
                            }}
                        />
                        Loading...
                    </span>
                )}
                {rowCount ? `Total: ${fmt(rowCount, 0, numberGroupSeparator)}` : 'Total: --'}
                {visibleRowsCount && ` | Visible: ${fmt(visibleRowsCount, 0, numberGroupSeparator)}`}
            </div>
            <div style={{display: 'flex', gap: '16px', overflowX: 'auto'}}>
                <span>Count: {fmt(stats.count, 0, numberGroupSeparator)}</span>
                {stats.sum !== undefined && (
                    <>
                        <span>Sum: {fmt(stats.sum, 2, numberGroupSeparator)}</span>
                        <span>Avg: {fmt(stats.avg, 2, numberGroupSeparator)}</span>
                        <span>Min: {fmt(stats.min, 2, numberGroupSeparator)}</span>
                        <span>Max: {fmt(stats.max, 2, numberGroupSeparator)}</span>
                        <span>Var: {fmt(stats.variance, 2, numberGroupSeparator)}</span>
                        <span>StdDev: {fmt(stats.stdDev, 2, numberGroupSeparator)}</span>
                    </>
                )}
            </div>
        </div>
    );
};

export default StatusBar;

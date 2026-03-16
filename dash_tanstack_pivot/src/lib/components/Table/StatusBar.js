import React, { useMemo } from 'react';

const fmt = (n, decimals = 2) =>
    new Intl.NumberFormat('en-US', { maximumFractionDigits: decimals }).format(n).replace(/,/g, '\u202F');

const StatusBar = ({ selectedCells, rowCount, visibleRowsCount, theme, isLoading = false }) => {
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
        <div style={{ height: '32px', borderTop: `1px solid ${theme.border}`, background: theme.headerBg, display: 'flex', alignItems: 'center', padding: '0 16px', justifyContent: 'space-between', fontSize: '12px', color: theme.textSec }}>
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
                {rowCount ? `Total: ${fmt(rowCount, 0)}` : 'Total: --'}
                {visibleRowsCount && ` | Visible: ${visibleRowsCount}`}
            </div>
            <div style={{display: 'flex', gap: '16px', overflowX: 'auto'}}>
                <span>Count: {stats.count}</span>
                {stats.sum !== undefined && (
                    <>
                        <span>Sum: {fmt(stats.sum)}</span>
                        <span>Avg: {fmt(stats.avg)}</span>
                        <span>Min: {fmt(stats.min)}</span>
                        <span>Max: {fmt(stats.max)}</span>
                        <span>Var: {fmt(stats.variance)}</span>
                        <span>StdDev: {fmt(stats.stdDev)}</span>
                    </>
                )}
            </div>
        </div>
    );
};

export default StatusBar;

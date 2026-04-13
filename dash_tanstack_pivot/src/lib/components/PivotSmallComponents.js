/**
 * PivotSmallComponents
 * 
 * Small, reusable UI components extracted from DashTanstackPivot.react.js
 * 
 * Contains:
 * - PaginationBar: Client-side pagination controls
 * 
 * Extracted during Phase 4A refactoring
 */

import React from 'react';
import PropTypes from 'prop-types';

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100, 200];

/**
 * PaginationBar - Excel-style pagination controls for client-side mode
 */
export const PaginationBar = React.memo(function PaginationBar({ table, theme, pageSize, onPageSizeChange }) {
    const pageCount = table.getPageCount();
    const pageIndex = table.getState().pagination?.pageIndex ?? 0;
    const totalRows = table.getPrePaginationRowModel().rows.length;
    const rangeStart = pageIndex * pageSize + 1;
    const rangeEnd = Math.min((pageIndex + 1) * pageSize, totalRows);
    
    const btnStyle = (disabled) => ({
        border: `1px solid ${theme.border}`,
        borderRadius: '4px',
        background: disabled ? 'transparent' : (theme.headerSubtleBg || theme.hover),
        color: disabled ? (theme.textSec || '#999') : theme.text,
        padding: '3px 10px',
        fontSize: '11px',
        fontWeight: 600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.5 : 1,
    });
    
    return (
        <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '6px 12px', borderTop: `1px solid ${theme.border}`,
            background: theme.headerBg || theme.background, flexShrink: 0,
            fontSize: '11px', color: theme.text, gap: '8px',
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ color: theme.textSec }}>Rows per page</span>
                <select
                    value={pageSize}
                    onChange={(e) => onPageSizeChange(Number(e.target.value))}
                    style={{
                        border: `1px solid ${theme.border}`, borderRadius: '4px',
                        background: theme.surfaceBg || theme.background, color: theme.text,
                        padding: '2px 4px', fontSize: '11px',
                    }}
                >
                    {PAGE_SIZE_OPTIONS.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
            </div>
            <span style={{ color: theme.textSec }}>
                {totalRows > 0 ? `${rangeStart}–${rangeEnd} of ${totalRows}` : '0 rows'}
            </span>
            <div style={{ display: 'flex', gap: '4px' }}>
                <button 
                    style={btnStyle(!table.getCanPreviousPage())} 
                    onClick={() => table.setPageIndex(0)} 
                    disabled={!table.getCanPreviousPage()}
                >
                    {'«'}
                </button>
                <button 
                    style={btnStyle(!table.getCanPreviousPage())} 
                    onClick={() => table.previousPage()} 
                    disabled={!table.getCanPreviousPage()}
                >
                    {'‹'}
                </button>
                <span style={{ padding: '3px 8px', fontSize: '11px', fontWeight: 600 }}>
                    {pageIndex + 1} / {pageCount || 1}
                </span>
                <button 
                    style={btnStyle(!table.getCanNextPage())} 
                    onClick={() => table.nextPage()} 
                    disabled={!table.getCanNextPage()}
                >
                    {'›'}
                </button>
                <button 
                    style={btnStyle(!table.getCanNextPage())} 
                    onClick={() => table.setPageIndex(pageCount - 1)} 
                    disabled={!table.getCanNextPage()}
                >
                    {'»'}
                </button>
            </div>
        </div>
    );
});

PaginationBar.propTypes = {
    table: PropTypes.object.isRequired,
    theme: PropTypes.object.isRequired,
    pageSize: PropTypes.number.isRequired,
    onPageSizeChange: PropTypes.func.isRequired,
};

export default PaginationBar;

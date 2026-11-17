import React, { useState, useEffect } from 'react';
import Icons from '../../utils/Icons';

/** @param {any} p0 @returns {any} */
const DrillThroughModal = ({ drillState, onClose, onPageChange, onSort, onFilter }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [filterInput, setFilterInput] = useState('');

    useEffect(() => {
        if (!drillState) return undefined;
        /** @param {any} [e] @returns {any} */
        const handleKey = (e) => {
            if (e.key === 'Escape' && typeof onClose === 'function') {
                onClose();
            }
        };
        document.addEventListener('keydown', handleKey);
        return () => document.removeEventListener('keydown', handleKey);
    }, [drillState, onClose]);

    if (!drillState) return null;

    const { loading, rows, page, totalRows, sortCol, sortDir } = drillState;
    const pageSize = 100;
    /** @type {any} */
    const totalPages = Math.ceil((totalRows || 0) / pageSize);
    /** @type {any} */
    const columns = rows && rows.length > 0 ? Object.keys(rows[0]).filter(k => !k.startsWith('_')) : [];

    /** @param {any} [e] @returns {any} */
    const handleFilterKeyDown = (e) => {
        if (e.key === 'Enter') {
            // Parent's onFilter always resets page to 0 before fetching
            onFilter(filterInput);
        }
    };

    /** @param {any} [col] @returns {any} */
    const handleSortClick = (col) => {
        if (sortCol === col) {
            onSort(col, sortDir === 'asc' ? 'desc' : 'asc');
        } else {
            onSort(col, 'asc');
        }
    };

    // Overlay styles — full-screen frosted backdrop
    /** @type {Record<string, any>} */
    const overlayStyle = {
        position: 'fixed', zIndex: 10003, top: 0, left: 0,
        width: '100%', height: '100%',
        backgroundColor: 'rgba(15,23,42,0.38)',
        backdropFilter: 'blur(6px)',
        WebkitBackdropFilter: 'blur(6px)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        padding: '20px',
        boxSizing: 'border-box',
    };
    /** @type {Record<string, any>} */
    const modalStyle = {
        background: 'var(--sp-surface-bg, #fff)',
        color: 'var(--sp-text, #0f172a)',
        border: '1px solid var(--sp-border, #e2e8f0)',
        borderRadius: '14px',
        padding: '18px 20px',
        width: 'min(1100px, 92vw)',
        maxHeight: '82vh',
        display: 'flex',
        flexDirection: 'column',
        fontFamily: 'var(--sp-font-sans, system-ui, -apple-system, sans-serif)',
        boxShadow: 'var(--sp-shadow, 0 24px 64px rgba(0,0,0,0.28))',
        overflow: 'hidden',
    };

    return (
        <div className="sp-dialog-backdrop" style={overlayStyle} onClick={onClose}>
            <div className="sp-dialog-surface" role="dialog" aria-modal="true" style={modalStyle} onClick={e => e.stopPropagation()}>
                {/* Header */}
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:'12px', gap: '12px' }}>
                    <h3 style={{ margin:0, fontSize:'15px', fontWeight: 700, letterSpacing: '-0.01em', color: 'var(--sp-text, #0f172a)' }}>
                        Drill-Through: {drillState.path || 'Source Rows'}
                    </h3>
                    <button
                        type="button"
                        className="sp-btn sp-btn-ghost"
                        aria-label="Close drill-through dialog"
                        onClick={onClose}
                        style={{
                            border: 'none',
                            background: 'transparent',
                            color: 'var(--sp-text-sec, #64748b)',
                            borderRadius: '6px',
                            width: '28px',
                            height: '28px',
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            padding: 0,
                            cursor: 'pointer',
                        }}
                    >
                        <Icons.Close />
                    </button>
                </div>

                {/* Filter input */}
                <div style={{ marginBottom:'12px' }}>
                    <div className="sp-search-shell" style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: '8px',
                        width: 'min(320px, 100%)',
                        padding: '6px 10px',
                        background: 'var(--sp-header-subtle-bg, rgba(15,23,42,0.03))',
                        border: '1px solid var(--sp-border, #cbd5e1)',
                        borderRadius: '8px',
                    }}>
                        <span style={{ color: 'var(--sp-text-sec, #64748b)', display: 'inline-flex', alignItems: 'center' }}><Icons.Search /></span>
                        <input
                            type="text"
                            placeholder="Filter rows... (press Enter)"
                            value={filterInput}
                            onChange={e => setFilterInput(e.target.value)}
                            onKeyDown={handleFilterKeyDown}
                            style={{ border: 'none', background: 'transparent', outline: 'none', width: '100%', color: 'var(--sp-text, #0f172a)', fontSize: '13px' }}
                        />
                    </div>
                </div>

                {/* Table */}
                {loading ? (
                    <div style={{ padding:'28px', textAlign:'center', color:'var(--sp-text-sec, #64748b)', fontSize: '13px' }}>Loading rows...</div>
                ) : (
                    <div style={{ overflow: 'auto', flex: 1, minHeight: 0, border: '1px solid var(--sp-border, #e2e8f0)', borderRadius: '10px' }}>
                        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'13px' }}>
                            <thead>
                                <tr>
                                    {columns.map(/** @param {any} [col] @returns {any} */ col => (
                                        <th
                                            key={col}
                                            onClick={() => handleSortClick(col)}
                                            style={{
                                                padding:'8px 10px', textAlign:'left', cursor:'pointer',
                                                background:'var(--sp-header-bg, #f8fafc)',
                                                color: sortCol === col ? 'var(--sp-primary, #2563eb)' : 'var(--sp-text-sec, #64748b)',
                                                fontWeight: 600,
                                                fontSize: '11px',
                                                borderBottom:'1px solid var(--sp-border, #e2e8f0)',
                                                userSelect:'none', whiteSpace:'nowrap',
                                                position: 'sticky', top: 0,
                                                zIndex: 1,
                                            }}
                                        >
                                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                                                <span>{col}</span>
                                                {sortCol === col && (sortDir === 'asc' ? <Icons.SortAsc /> : <Icons.SortDesc />)}
                                            </span>
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {(rows || []).map(/** @param {any} [row] @param {any} [i] @returns {any} */ (row, i) => (
                                    <tr key={i} style={{ background: i % 2 === 0 ? 'var(--sp-surface-bg, transparent)' : 'var(--sp-header-subtle-bg, rgba(15,23,42,0.02))' }}>
                                        {columns.map(/** @param {any} [col] @returns {any} */ col => (
                                            <td key={col} style={{ padding:'7px 10px', borderBottom:'1px solid var(--sp-border, #e2e8f0)', color: 'var(--sp-text, #0f172a)' }}>
                                                {row[col] !== null && row[col] !== undefined ? String(row[col]) : ''}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                                {(!rows || rows.length === 0) && (
                                    <tr><td colSpan={columns.length || 1} style={{ padding:'20px', textAlign:'center', color:'var(--sp-text-sec, #64748b)' }}>No rows found</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                )}

                {/* Pagination */}
                <div style={{ display:'flex', alignItems:'center', gap:'10px', marginTop:'14px', fontSize:'12px', color:'var(--sp-text-sec, #64748b)', fontVariantNumeric: 'tabular-nums' }}>
                    <button
                        type="button"
                        className="sp-btn sp-btn-subtle"
                        onClick={() => onPageChange(page - 1)}
                        disabled={page === 0 || loading}
                        style={{
                            padding:'6px 12px',
                            borderRadius:'8px',
                            border:'1px solid var(--sp-border, #cbd5e1)',
                            background:'var(--sp-header-subtle-bg, transparent)',
                            color:'var(--sp-text, #0f172a)',
                            fontWeight: 600,
                            fontSize: '12px',
                            cursor: (page === 0 || loading) ? 'default' : 'pointer',
                            opacity: (page === 0 || loading) ? 0.45 : 1,
                        }}
                    >Previous</button>
                    <span>Page {page + 1} of {totalPages || 1}</span>
                    <span style={{ opacity: 0.75 }}>({totalRows || 0} total rows)</span>
                    <button
                        type="button"
                        className="sp-btn sp-btn-subtle"
                        onClick={() => onPageChange(page + 1)}
                        disabled={page >= (totalPages - 1) || loading}
                        style={{
                            padding:'6px 12px',
                            borderRadius:'8px',
                            border:'1px solid var(--sp-border, #cbd5e1)',
                            background:'var(--sp-header-subtle-bg, transparent)',
                            color:'var(--sp-text, #0f172a)',
                            fontWeight: 600,
                            fontSize: '12px',
                            cursor: (page >= (totalPages - 1) || loading) ? 'default' : 'pointer',
                            opacity: (page >= (totalPages - 1) || loading) ? 0.45 : 1,
                        }}
                    >Next</button>
                </div>
            </div>
        </div>
    );
};

export default DrillThroughModal;

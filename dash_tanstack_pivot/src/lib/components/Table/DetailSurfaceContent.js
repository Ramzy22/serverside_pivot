import React, { useEffect, useState } from 'react';
import Icons from '../Icons';

export function DetailSurfaceContent({
    detailState,
    onClose,
    onPageChange,
    onSort,
    onFilter,
    theme,
    compact = false,
}) {
    const [filterInput, setFilterInput] = useState('');

    useEffect(() => {
        setFilterInput(detailState && typeof detailState.filterText === 'string' ? detailState.filterText : '');
    }, [detailState && detailState.filterText]); // eslint-disable-line react-hooks/exhaustive-deps

    if (!detailState) return null;

    const {
        loading,
        rows = [],
        columns = [],
        page = 0,
        pageSize = 100,
        totalRows = 0,
        sortCol = null,
        sortDir = 'asc',
        title = 'Detail',
        rowPath = '',
    } = detailState;

    const resolvedColumns = Array.isArray(columns) && columns.length > 0
        ? columns.map((column) => (typeof column === 'string' ? column : column.id)).filter(Boolean)
        : (rows[0] ? Object.keys(rows[0]).filter((key) => !key.startsWith('_')) : []);
    const totalPages = Math.max(1, Math.ceil((totalRows || 0) / Math.max(1, pageSize || 100)));

    const handleSortClick = (columnId) => {
        if (!onSort) return;
        if (sortCol === columnId) {
            onSort(columnId, sortDir === 'asc' ? 'desc' : 'asc');
            return;
        }
        onSort(columnId, 'asc');
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
            <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '12px',
                padding: compact ? '10px 12px' : '14px 16px',
                borderBottom: `1px solid ${theme.border}`,
                background: theme.headerBg,
            }}>
                <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: compact ? '12px' : '13px', fontWeight: 700, color: theme.text }}>
                        {title}
                    </div>
                    {rowPath && (
                        <div style={{
                            fontSize: '11px',
                            color: theme.textSec,
                            whiteSpace: 'nowrap',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            maxWidth: compact ? '320px' : '460px',
                        }}>
                            {rowPath}
                        </div>
                    )}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexShrink: 0 }}>
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '6px',
                        padding: '0 10px',
                        border: `1px solid ${theme.border}`,
                        borderRadius: '999px',
                        background: theme.surfaceBg || theme.background,
                    }}>
                        <Icons.Search />
                        <input
                            type="text"
                            value={filterInput}
                            placeholder="Filter rows..."
                            onChange={(event) => setFilterInput(event.target.value)}
                            onKeyDown={(event) => {
                                if (event.key === 'Enter' && onFilter) onFilter(filterInput);
                            }}
                            style={{
                                border: 'none',
                                outline: 'none',
                                background: 'transparent',
                                color: theme.text,
                                fontSize: '12px',
                                width: compact ? '140px' : '180px',
                                padding: '8px 0',
                            }}
                        />
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        style={{
                            width: '28px',
                            height: '28px',
                            borderRadius: '999px',
                            border: `1px solid ${theme.border}`,
                            background: theme.surfaceBg || theme.background,
                            color: theme.textSec,
                            cursor: 'pointer',
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                        }}
                        title="Close detail"
                    >
                        <Icons.Close />
                    </button>
                </div>
            </div>

            <div style={{ flex: 1, minHeight: 0, overflow: 'auto', background: theme.surfaceBg || theme.background }}>
                {loading ? (
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        height: '100%',
                        color: theme.textSec,
                        fontSize: '13px',
                    }}>
                        Loading detail...
                    </div>
                ) : (
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                        <thead>
                            <tr>
                                {resolvedColumns.map((columnId) => (
                                    <th
                                        key={columnId}
                                        onClick={() => handleSortClick(columnId)}
                                        style={{
                                            position: 'sticky',
                                            top: 0,
                                            zIndex: 1,
                                            textAlign: 'left',
                                            padding: compact ? '8px 10px' : '10px 12px',
                                            background: theme.headerBg,
                                            color: theme.text,
                                            borderBottom: `1px solid ${theme.border}`,
                                            cursor: onSort ? 'pointer' : 'default',
                                            whiteSpace: 'nowrap',
                                        }}
                                    >
                                        {columnId}
                                        {sortCol === columnId ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ''}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {rows.length === 0 ? (
                                <tr>
                                    <td
                                        colSpan={Math.max(resolvedColumns.length, 1)}
                                        style={{
                                            padding: '18px 12px',
                                            textAlign: 'center',
                                            color: theme.textSec,
                                            borderBottom: `1px solid ${theme.border}`,
                                        }}
                                    >
                                        No detail rows
                                    </td>
                                </tr>
                            ) : rows.map((row, index) => (
                                <tr key={`${detailState.rowKey || rowPath || 'detail'}:${index}`} style={{
                                    background: index % 2 === 0 ? (theme.surfaceBg || theme.background) : theme.headerSubtleBg,
                                }}>
                                    {resolvedColumns.map((columnId) => (
                                        <td
                                            key={`${columnId}:${index}`}
                                            style={{
                                                padding: compact ? '7px 10px' : '9px 12px',
                                                borderBottom: `1px solid ${theme.border}`,
                                                color: theme.text,
                                                whiteSpace: 'nowrap',
                                            }}
                                        >
                                            {row[columnId] !== undefined && row[columnId] !== null ? String(row[columnId]) : ''}
                                        </td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>

            <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '12px',
                padding: compact ? '10px 12px' : '12px 16px',
                borderTop: `1px solid ${theme.border}`,
                background: theme.headerBg,
                fontSize: '12px',
                color: theme.textSec,
            }}>
                <span>
                    Page {page + 1} of {totalPages}
                    {' '}
                    ({totalRows || 0} rows)
                </span>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <button
                        type="button"
                        onClick={() => onPageChange && onPageChange(Math.max(page - 1, 0))}
                        disabled={loading || page <= 0}
                        style={{
                            padding: '6px 10px',
                            borderRadius: '8px',
                            border: `1px solid ${theme.border}`,
                            background: theme.surfaceBg || theme.background,
                            color: theme.text,
                            cursor: page <= 0 ? 'default' : 'pointer',
                            opacity: page <= 0 ? 0.45 : 1,
                        }}
                    >
                        Previous
                    </button>
                    <button
                        type="button"
                        onClick={() => onPageChange && onPageChange(page + 1)}
                        disabled={loading || page >= totalPages - 1}
                        style={{
                            padding: '6px 10px',
                            borderRadius: '8px',
                            border: `1px solid ${theme.border}`,
                            background: theme.surfaceBg || theme.background,
                            color: theme.text,
                            cursor: page >= totalPages - 1 ? 'default' : 'pointer',
                            opacity: page >= totalPages - 1 ? 0.45 : 1,
                        }}
                    >
                        Next
                    </button>
                </div>
            </div>
        </div>
    );
}

export default DetailSurfaceContent;

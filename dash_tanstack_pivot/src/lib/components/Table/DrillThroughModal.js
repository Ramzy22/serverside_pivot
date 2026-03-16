import React, { useState } from 'react';

const DrillThroughModal = ({ drillState, onClose, onPageChange, onSort, onFilter }) => {
    const [filterInput, setFilterInput] = useState('');

    if (!drillState) return null;

    const { loading, rows, page, totalRows, sortCol, sortDir } = drillState;
    const pageSize = 100;
    const totalPages = Math.ceil((totalRows || 0) / pageSize);
    const columns = rows && rows.length > 0 ? Object.keys(rows[0]).filter(k => !k.startsWith('_')) : [];

    const handleFilterKeyDown = (e) => {
        if (e.key === 'Enter') {
            // Parent's onFilter always resets page to 0 before fetching
            onFilter(filterInput);
        }
    };

    const handleSortClick = (col) => {
        if (sortCol === col) {
            onSort(col, sortDir === 'asc' ? 'desc' : 'asc');
        } else {
            onSort(col, 'asc');
        }
    };

    // Overlay styles — full-screen semi-transparent backdrop
    const overlayStyle = {
        position: 'fixed', zIndex: 10003, top: 0, left: 0,
        width: '100%', height: '100%',
        backgroundColor: 'rgba(0,0,0,0.5)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
    };
    const modalStyle = {
        background: '#fff', borderRadius: '8px', padding: '20px',
        width: '85%', maxWidth: '1100px', maxHeight: '80vh',
        overflowY: 'auto', boxShadow: '0 4px 24px rgba(0,0,0,0.25)',
    };

    return (
        <div style={overlayStyle} onClick={onClose}>
            <div style={modalStyle} onClick={e => e.stopPropagation()}>
                {/* Header */}
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:'12px' }}>
                    <h3 style={{ margin:0, fontSize:'16px' }}>
                        Drill-Through: {drillState.path || 'Source Rows'}
                    </h3>
                    <button onClick={onClose} style={{ border:'none', background:'none', fontSize:'20px', cursor:'pointer' }}>×</button>
                </div>

                {/* Filter input */}
                <div style={{ marginBottom:'10px' }}>
                    <input
                        type="text"
                        placeholder="Filter rows... (press Enter)"
                        value={filterInput}
                        onChange={e => setFilterInput(e.target.value)}
                        onKeyDown={handleFilterKeyDown}
                        style={{ padding:'6px 10px', width:'280px', border:'1px solid #ccc', borderRadius:'4px', fontSize:'13px' }}
                    />
                </div>

                {/* Table */}
                {loading ? (
                    <div style={{ padding:'20px', textAlign:'center', color:'#888' }}>Loading...</div>
                ) : (
                    <div style={{ overflowX: 'auto' }}>
                        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'13px' }}>
                            <thead>
                                <tr>
                                    {columns.map(col => (
                                        <th
                                            key={col}
                                            onClick={() => handleSortClick(col)}
                                            style={{
                                                padding:'8px', textAlign:'left', cursor:'pointer',
                                                background:'#f5f5f5', borderBottom:'2px solid #ddd',
                                                userSelect:'none', whiteSpace:'nowrap',
                                                position: 'sticky', top: 0,
                                            }}
                                        >
                                            {col}
                                            {sortCol === col ? (sortDir === 'asc' ? ' \u25b2' : ' \u25bc') : ''}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {(rows || []).map((row, i) => (
                                    <tr key={i} style={{ background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                                        {columns.map(col => (
                                            <td key={col} style={{ padding:'7px 8px', borderBottom:'1px solid #eee' }}>
                                                {row[col] !== null && row[col] !== undefined ? String(row[col]) : ''}
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                                {(!rows || rows.length === 0) && (
                                    <tr><td colSpan={columns.length || 1} style={{ padding:'16px', textAlign:'center', color:'#888' }}>No rows found</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                )}

                {/* Pagination */}
                <div style={{ display:'flex', alignItems:'center', gap:'12px', marginTop:'12px', fontSize:'13px', color:'#555' }}>
                    <button
                        onClick={() => onPageChange(page - 1)}
                        disabled={page === 0 || loading}
                        style={{ padding:'5px 12px', cursor: page === 0 ? 'default' : 'pointer', opacity: page === 0 ? 0.4 : 1 }}
                    >Previous</button>
                    <span>Page {page + 1} of {totalPages || 1}</span>
                    <span style={{ color:'#aaa' }}>({totalRows || 0} total rows)</span>
                    <button
                        onClick={() => onPageChange(page + 1)}
                        disabled={page >= (totalPages - 1) || loading}
                        style={{ padding:'5px 12px', cursor: (page >= totalPages - 1) ? 'default' : 'pointer', opacity: (page >= totalPages - 1) ? 0.4 : 1 }}
                    >Next</button>
                </div>
            </div>
        </div>
    );
};

export default DrillThroughModal;

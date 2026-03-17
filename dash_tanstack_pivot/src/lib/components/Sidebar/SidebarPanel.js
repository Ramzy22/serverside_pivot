import React from 'react';
import FilterPopover from '../Filters/FilterPopover';
import SidebarFilterItem from './SidebarFilterItem';
import ToolPanelSection from './ToolPanelSection';
import ColumnTreeItem from './ColumnTreeItem';
import Icons from '../Icons';
import { isGroupColumn, getAllLeafIdsFromColumn, hasChildrenInZone } from '../../utils/helpers';

export function SidebarPanel({
    sidebarTab, setSidebarTab,
    rowFields, setRowFields,
    colFields, setColFields,
    valConfigs, setValConfigs,
    filters, setFilters,
    columnVisibility, setColumnVisibility,
    columnPinning, setColumnPinning,
    availableFields,
    table,
    pinningPresets,
    theme, styles,
    showNotification,
    filterAnchorEl, setFilterAnchorEl,
    colSearch, setColSearch,
    colTypeFilter, setColTypeFilter,
    selectedCols, setSelectedCols,
    dropLine, onDragStart, onDragOver, onDrop,
    handleHeaderFilter, handleFilterClick,
    handleExpandAllRows, handlePinColumn,
    toggleAllColumnsPinned,
    activeFilterCol, closeFilterPopover, filterOptions,
    data,
}) {
    return (
                <div style={styles.sidebar} role="complementary" aria-label="Tool Panel">
                    <div style={{display: 'flex', borderBottom: `1px solid ${theme.border}`, marginBottom: '16px'}}>
                        <div 
                            onClick={() => setSidebarTab('fields')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer', 
                                borderBottom: sidebarTab === 'fields' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'fields' ? 600 : 400,
                                color: sidebarTab === 'fields' ? theme.primary : theme.textSec
                            }}
                        >Fields</div>
                        <div 
                            onClick={() => setSidebarTab('filters')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer', 
                                borderBottom: sidebarTab === 'filters' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'filters' ? 600 : 400,
                                color: sidebarTab === 'filters' ? theme.primary : theme.textSec,
                                display: 'flex', alignItems: 'center', gap: '6px'
                            }}
                        >
                            Filters
                            {Object.keys(filters).length > 0 && (
                                <div style={{width: '6px', height: '6px', borderRadius: '50%', background: '#d32f2f'}} />
                            )}
                        </div>
                        <div 
                            onClick={() => setSidebarTab('columns')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer',
                                borderBottom: sidebarTab === 'columns' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'columns' ? 600 : 400,
                                color: sidebarTab === 'columns' ? theme.primary : theme.textSec
                            }}
                        >Columns</div>
                    </div>

                    {sidebarTab === 'filters' ? (
                        <div style={{flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', padding: '8px'}}>
                            <div style={{marginBottom: '10px', display: 'flex', alignItems: 'center', background: theme.background, borderRadius: '6px', padding: '4px 8px', border: `1px solid ${theme.border}`}}>
                                <Icons.Search />
                                <input 
                                    placeholder="Search columns..."
                                    value={colSearch} 
                                    onChange={e => setColSearch(e.target.value)} 
                                    style={{border:'none', background:'transparent', marginLeft:'10px', outline:'none', width:'100%', color: theme.text, fontSize: '13px'}}
                                />
                            </div>
                            {(() => {
                                const allFields = availableFields;
                                const colsForDisplay = allFields.map(field => {
                                    const tableCol = table.getColumn(field);
                                    if (tableCol) return tableCol;
                                    return { id: field, header: field, columnDef: { header: field } };
                                });
                                const filtered = colsForDisplay.filter(col => {
                                    const header = (col.columnDef && typeof col.columnDef.header === 'string') ? col.columnDef.header : (typeof col.header === 'string' ? col.header : col.id);
                                    return String(header).toLowerCase().includes(colSearch.toLowerCase()) || col.id.toLowerCase().includes(colSearch.toLowerCase());
                                });
                                return (
                                    <div style={{display: 'flex', flexDirection: 'column'}}>
                                        {filtered.map(col => (
                                            <SidebarFilterItem
                                                key={col.id}
                                                column={col}
                                                theme={theme}
                                                styles={styles}
                                                onFilter={(val) => handleHeaderFilter(col.id, val)}
                                                currentFilter={filters[col.id]}
                                                options={[]}
                                            />
                                        ))}
                                    </div>
                                );
                            })()}
                        </div>
                    ) : sidebarTab !== 'columns' ? (
                        <>
                            {sidebarTab === 'fields' && (
                                <div>
                                    <div style={styles.sectionTitle}>Available Fields</div>
                                    <div style={{maxHeight: '160px', overflowY: 'auto'}}>
                                        {availableFields.map(f => (
                                            <div key={f} draggable onDragStart={e=>onDragStart(e,f,'pool')} style={styles.chip}>
                                                <div style={{display:'flex',gap:'6px'}}><Icons.DragIndicator/> {f}</div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                            {[{id:'rows', label:'Rows'}, {id:'cols', label:'Columns'}, {id:'vals', label:'Values'}, {id:'filter', label:'Filters'}].map(zone => (
                                <div key={zone.id}>
                                    <div style={styles.sectionTitle}>{zone.label}</div>
                                    {zone.id === 'rows' && rowFields.length > 0 && (
                                        <div style={{display:'flex', gap:'4px', padding:'0 4px 6px 4px'}}>
                                            <button
                                                title="Expand all hierarchy levels"
                                                onClick={() => handleExpandAllRows(true)}
                                                style={{flex:1, border:`1px solid ${theme.primary}`, background:theme.select, cursor:'pointer', padding:'3px 6px', fontSize:'11px', color:theme.primary, borderRadius:'4px', fontWeight:600}}
                                            >+ Expand All</button>
                                            <button
                                                title="Collapse all rows"
                                                onClick={() => handleExpandAllRows(false)}
                                                style={{flex:1, border:`1px solid ${theme.border}`, background:theme.background, cursor:'pointer', padding:'3px 6px', fontSize:'11px', color:theme.textSec, borderRadius:'4px'}}
                                            >- Collapse All</button>
                                        </div>
                                    )}
                                    <div style={styles.dropZone} onDragOver={e=>e.preventDefault()} onDrop={e=>onDrop(e, zone.id)}>
                                        {(zone.id==='filter' ? Object.keys(filters).filter(k=>k!=='global') : zone.id==='rows'?rowFields:zone.id==='cols'?colFields:valConfigs).map((item, idx) => {
                                            const label = zone.id==='vals' ? item.field : item;
                                            return (
                                                                                        <div key={idx} draggable onDragStart={e=>onDragStart(e,item,zone.id,idx)} onDragOver={e=>onDragOver(e,zone.id,idx)} >
                                                                                            <div style={styles.chip}>
                                                                                                {dropLine && dropLine.zone===zone.id && dropLine.idx===idx && <div style={{...styles.dropLine,top:-2}}/>}
                                                                                                <div style={{display:'flex',gap:'6px'}}><Icons.DragIndicator/> <b>{label}</b></div>
                                                                                                {zone.id === 'vals' && (
                                                                                                    <div style={{display:'flex',flexDirection:'column', gap:2}}>
                                                                                                        <div style={{display:'flex', gap:2}}>
                                                                                                            <select value={item.agg} onChange={e=>{const n=[...valConfigs];n[idx].agg=e.target.value;setValConfigs(n)}} style={{border:'none',background:'transparent',color:theme.primary,cursor:'pointer',maxWidth:'50px',fontSize:'11px'}}><option value="sum">Sum</option><option value="avg">Avg</option><option value="count">Cnt</option><option value="min">Min</option><option value="max">Max</option></select>
                                                                                                            <select value={item.windowFn || 'none'} onChange={e=>{const n=[...valConfigs];n[idx].windowFn=e.target.value==='none'?null:e.target.value;setValConfigs(n)}} style={{border:'none',background:'transparent',color:theme.primary,cursor:'pointer',maxWidth:'60px',fontSize:'11px'}}><option value="none">Norm</option><option value="percent_of_row">%Row</option><option value="percent_of_col">%Col</option><option value="percent_of_grand_total">%Tot</option></select>
                                                                                                        </div>
                                                                                                        <input placeholder="Fmt (currency)" value={item.format || ''} onChange={e=>{const n=[...valConfigs];n[idx].format=e.target.value;setValConfigs(n)}} style={{border:'1px solid #eee', fontSize:'10px', padding:'2px', width:'100%'}}/>
                                                                                                    </div>
                                                                                                )}
                                                                                                <div style={{display:'flex', gap:'4px', marginLeft:'auto', alignItems: 'center'}}>
                                                                                                    {zone.id==='filter' && (
                                                                                                        <div 
                                                                                                            onClick={(e) => handleFilterClick(e, label)} 
                                                                                                            style={{
                                                                                                                cursor:'pointer', 
                                                                                                                display:'flex', 
                                                                                                                alignItems:'center',
                                                                                                                padding: '2px',
                                                                                                                borderRadius: '4px',
                                                                                                                background: (filters[label] && ((filters[label].conditions && filters[label].conditions.length > 0) || (typeof filters[label] === 'string' && filters[label].length > 0))) ? theme.select : 'transparent',
                                                                                                                color: (filters[label] && ((filters[label].conditions && filters[label].conditions.length > 0) || (typeof filters[label] === 'string' && filters[label].length > 0))) ? theme.primary : 'inherit'
                                                                                                            }}
                                                                                                        >
                                                                                                            <Icons.Filter />
                                                                                                        </div>
                                                                                                    )}
                                                                                                    <span onClick={()=>{
                                                                                                        if (zone.id==='filter'){const n={...filters};delete n[label];setFilters(n)}
                                                                                                        if (zone.id==='rows') setRowFields(p=>p.filter(x=>x!==label))
                                                                                                        if (zone.id==='cols') setColFields(p=>p.filter(x=>x!==label))
                                                                                                        if (zone.id==='vals') setValConfigs(p=>p.filter((_,i)=>i!==idx))
                                                                                                    }} style={{cursor:'pointer'}}><Icons.Close/></span>
                                                                                                </div>
                                                                                                                                                {zone.id === 'filter' && activeFilterCol === label && (
                                                                                                                    <FilterPopover 
                                                                                                                        column={{header: label, id: label}} 
                                                                                                                        anchorEl={filterAnchorEl}
                                                                                                                        onClose={closeFilterPopover}
                                                                                                                        onFilter={(filterValue) => handleHeaderFilter(label, filterValue)}
                                                                                                                        currentFilter={filters[label]}
                                                                                                                        options={filterOptions[label] || []}
                                                                                                                        theme={theme}
                                                                                                                    />
                                                                                                                                                )}
                                                                                                                                    {dropLine && dropLine.zone===zone.id && dropLine.idx===idx+1 && <div style={{...styles.dropLine,bottom:-2}}/>}
                                                                                            </div>
                                                                                                                                        {zone.id ==='filter' && filters[label] && filters[label].conditions && (
                                                                                                                                            <div style={{fontSize: '10px', color: theme.primary, padding: '0 8px 4px 8px', marginTop: '-4px'}}>
                                                                                                                                                {filters[label].conditions.map(c => `${c.type}: ${c.value}${c.caseSensitive ? ' (Match Case)' : ''}`).join(` ${filters[label].operator} `)}
                                                                                                                                            </div>
                                                                                                                                        )}
                                                                                                                        </div>
                                            )
                                        })}
                                        {(zone.id==='filter' ? Object.keys(filters).filter(k=>k!=='global') : zone.id==='rows'?rowFields:zone.id==='cols'?colFields:valConfigs).length === 0 && (
                                            <div style={{opacity:0.5, fontSize:'11px', padding:'8px', textAlign:'center', pointerEvents:'none'}}>Drag fields here</div>
                                        )}
                                        <div style={{height:20}} onDragOver={e=>onDragOver(e,zone.id,(zone.id==='rows'?rowFields:zone.id==='cols'?colFields:zone.id==='vals'?valConfigs:Object.keys(filters).filter(k=>k!=='global')).length)} />
                                    </div>
                                </div>
                            ))}
                        </>
                    ) : (
                        <div style={{display: 'flex', flexDirection: 'column', gap: '16px', height: '100%', overflow: 'hidden'}}>
                            {/* Enhanced Search Header */}
                            <div style={{display: 'flex', flexDirection: 'column', gap: '10px', padding: '8px', background: theme.headerBg, borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.08)'}}>
                                <div style={{display: 'flex', alignItems: 'center', background: theme.background, borderRadius: '6px', padding: '8px 12px', border: `2px solid ${theme.border}`, transition: 'border-color 0.2s'}}>
                                    <Icons.Search />
                                    <input 
                                        placeholder="Search columns..."
                                        value={colSearch} 
                                        onChange={e => setColSearch(e.target.value)} 
                                        style={{
                                            border:'none', 
                                            background:'transparent', 
                                            marginLeft:'10px', 
                                            outline:'none', 
                                            width:'100%', 
                                            color: theme.text, 
                                            fontSize: '13px',
                                            fontWeight: 500
                                        }} 
                                    />
                                    {colSearch && (
                                        <span 
                                            onClick={() => setColSearch('')} 
                                            style={{
                                                cursor: 'pointer', 
                                                display: 'flex', 
                                                padding: '4px',
                                                borderRadius: '4px',
                                                background: theme.hover,
                                                transition: 'background 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.background = theme.select}
                                            onMouseLeave={e => e.currentTarget.style.background = theme.hover}
                                        >
                                            <Icons.Close />
                                        </span>
                                    )}
                                </div>
                                
                                {/* Type Filter Pills */}
                                <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                                    {[{"value": "all", "label": "All", "icon": "📊"},
                                        {"value": "number", "label": "Numbers", "icon": "🔢"},
                                        {"value": "string", "label": "Text", "icon": "📝"},
                                        {"value": "date", "label": "Dates", "icon": "📅"}
                                    ].map(type => (
                                        <button
                                            key={type.value}
                                            onClick={() => setColTypeFilter(type.value)}
                                            style={{
                                                padding: '6px 12px',
                                                borderRadius: '6px',
                                                border: 'none',
                                                background: colTypeFilter === type.value ? theme.primary : theme.background,
                                                color: colTypeFilter === type.value ? '#fff' : theme.text,
                                                cursor: 'pointer',
                                                fontSize: '11px',
                                                fontWeight: 600,
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: '4px',
                                                transition: 'all 0.2s',
                                                boxShadow: colTypeFilter === type.value ? '0 2px 4px rgba(0,0,0,0.1)' : 'none'
                                            }}
                                            onMouseEnter={e => {
                                                if (colTypeFilter !== type.value) {
                                                    e.currentTarget.style.background = theme.hover;
                                                }
                                            }}
                                            onMouseLeave={e => {
                                                if (colTypeFilter !== type.value) {
                                                    e.currentTarget.style.background = theme.background;
                                                }
                                            }}
                                        >
                                            <span>{type.icon}</span>
                                            <span>{type.label}</span>
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Enhanced Action Buttons */}
                            <div style={{display: 'flex', gap: '6px', padding: '0 8px'}}>
                                <button 
                                    onClick={() => table.toggleAllColumnsVisible(true)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.Visibility style={{fontSize: '14px'}} />
                                    <span>Show All</span>
                                </button>
                                <button 
                                    onClick={() => table.toggleAllColumnsVisible(false)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.VisibilityOff style={{fontSize: '14px'}} />
                                    <span>Hide All</span>
                                </button>
                                <button 
                                    onClick={() => toggleAllColumnsPinned(false)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.Unpin style={{fontSize: '14px'}} />
                                    <span>Unpin All</span>
                                </button>
                            </div>

                            {/* Enhanced Selection Bar */}
                            {selectedCols.size > 0 && (
                                <div style={{
                                    display: 'flex', 
                                    flexDirection: 'column',
                                    gap: '8px', 
                                    background: `linear-gradient(135deg, ${theme.select}ee, ${theme.select}dd)`, 
                                    padding: '12px', 
                                    borderRadius: '8px', 
                                    border: `2px solid ${theme.primary}66`,
                                    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
                                    margin: '0 8px'
                                }}>
                                    <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                                        <span style={{
                                            fontSize: '12px', 
                                            fontWeight: 700, 
                                            padding: '4px 10px', 
                                            color: theme.primary, 
                                            background: theme.background,
                                            borderRadius: '6px',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
                                        }}>
                                            {selectedCols.size} Column{selectedCols.size > 1 ? 's' : ''} Selected
                                        </span>
                                        <button 
                                            onClick={() => setSelectedCols(new Set())} 
                                            style={{
                                                border: 'none', 
                                                background: theme.background, 
                                                cursor: 'pointer', 
                                                display: 'flex', 
                                                color: theme.textSec,
                                                padding: '4px',
                                                borderRadius: '4px',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.background = theme.hover}
                                            onMouseLeave={e => e.currentTarget.style.background = theme.background}
                                        >
                                            <Icons.Close/>
                                        </button>
                                    </div>
                                    <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, 'left')); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.PinLeft />
                                            <span>Pin Left</span>
                                        </button>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, false)); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.Unpin />
                                            <span>Unpin</span>
                                        </button>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, 'right')); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.PinRight />
                                            <span>Pin Right</span>
                                        </button>
                                    </div>
                                </div>
                            )}

                            <div style={{flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column'}}>
                                {(() => {
                                    const allCols = table.getAllColumns().filter(c => !c.parent && c.id !== 'no_data');
                                    
                                    const filteredCols = allCols.filter(col => {
                                        if (colTypeFilter === 'all') return true;
                                        let type = 'string';
                                        
                                        // Check if it's a value column (aggregated)
                                        const config = valConfigs.find(v => col.id.includes(v.field));
                                        
                                        if (config) {
                                            type = 'number';
                                        } else if (data && data.length > 0) {
                                            // Check the first row's data for this column
                                            // Handle both flat data and nested/grouped data structures
                                            const firstRow = data[0];
                                            let val = firstRow[col.id];
                                            
                                            // If value is undefined in flat data, try to find it via accessor if possible, or skip
                                            if (val === undefined && col.accessorFn) {
                                                try {
                                                    val = col.accessorFn(firstRow);
                                                } catch (e) { /* ignore */ }
                                            }

                                            if (typeof val === 'number') {
                                                type = 'number';
                                            } else if (val instanceof Date) {
                                                type = 'date';
                                            } else if (typeof val === 'string') {
                                                if (!isNaN(Number(val)) && val.trim() !== '') {
                                                    type = 'number';
                                                } else if (!isNaN(Date.parse(val)) && val.includes('-')) {
                                                    type = 'date';
                                                }
                                            }
                                        }
                                        return type === colTypeFilter;
                                    });

                                    const leftPinned = filteredCols.filter(c => hasChildrenInZone(c, 'left'));
                                    const rightPinned = filteredCols.filter(c => hasChildrenInZone(c, 'right'));
                                    const unpinned = filteredCols.filter(c => hasChildrenInZone(c, 'unpinned'));

                                    const renderColList = (cols, sectionId) => cols.map(column => (
                                        <ColumnTreeItem 
                                            key={column.id} 
                                            column={column} 
                                            level={0} 
                                            theme={theme} 
                                            styles={styles} 
                                            handlePinColumn={handlePinColumn}
                                            colSearch={colSearch}
                                            selectedCols={selectedCols}
                                            setSelectedCols={setSelectedCols}
                                            onDrop={handleToolPanelDrop}
                                            sectionId={sectionId}
                                        />
                                    ));

                                    const handleToolPanelDrop = (colId, sectionId, targetColId) => {
                                        let targetIndex = undefined;
                                        const currentPinning = columnPinning || { left: [], right: [] };

                                        if (targetColId) {
                                            const list = sectionId === 'left' ? currentPinning.left : (sectionId === 'right' ? currentPinning.right : null);
                                            if (list) {
                                                targetIndex = list.indexOf(targetColId);
                                                
                                                // If not found, maybe it's a group?
                                                if (targetIndex === -1) {
                                                    const targetCol = table.getColumn(targetColId);
                                                    if (targetCol && isGroupColumn(targetCol)) {
                                                        const leaves = getAllLeafIdsFromColumn(targetCol);
                                                        const firstPinnedLeaf = leaves.find(id => list.includes(id));
                                                        if (firstPinnedLeaf) {
                                                            targetIndex = list.indexOf(firstPinnedLeaf);
                                                        }
                                                    }
                                                }

                                                if (targetIndex === -1) targetIndex = undefined;
                                            }
                                        }

                                        if (sectionId === 'left') handlePinColumn(colId, 'left', targetIndex);
                                        else if (sectionId === 'right') handlePinColumn(colId, 'right', targetIndex);
                                        else handlePinColumn(colId, false);
                                    };

                                    return (
                                        <>
                                            {leftPinned.length > 0 && (
                                                <ToolPanelSection 
                                                    title="Pinned Left" 
                                                    count={leftPinned.length} 
                                                    theme={theme} 
                                                    styles={styles}
                                                    sectionId="left"
                                                    onDrop={handleToolPanelDrop}
                                                >
                                                    {renderColList(leftPinned, 'left')}
                                                </ToolPanelSection>
                                            )}

                                            {rightPinned.length > 0 && (
                                                <ToolPanelSection 
                                                    title="Pinned Right" 
                                                    count={rightPinned.length} 
                                                    theme={theme} 
                                                    styles={styles}
                                                    sectionId="right"
                                                    onDrop={handleToolPanelDrop}
                                                >
                                                    {renderColList(rightPinned, 'right')}
                                                </ToolPanelSection>
                                            )}
                                            
                                            <ToolPanelSection 
                                                title="Columns" 
                                                count={unpinned.length} 
                                                theme={theme} 
                                                styles={styles}
                                                sectionId="unpinned"
                                                onDrop={handleToolPanelDrop}
                                            >
                                                {renderColList(unpinned, 'unpinned')}
                                            </ToolPanelSection>
                                        </>
                                    );
                                })()}
                            </div>
                            
                            {pinningPresets && pinningPresets.length > 0 && (
                                <div style={{padding: '8px', borderTop: `1px solid ${theme.border}`}}>
                                    <div style={styles.sectionTitle}>Pinning Presets</div>
                                    <div style={{display: 'flex', gap: '4px', flexWrap: 'wrap'}}>
                                        {pinningPresets.map((preset, i) => (
                                            <button 
                                                key={i}
                                                onClick={() => {
                                                    setColumnPinning(preset.config);
                                                    showNotification(`Applied preset: ${preset.name}`);
                                                }}
                                                style={{...styles.btn, fontSize: '11px', background: theme.headerBg}}
                                            >
                                                {preset.name}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div style={{fontSize: '11px', color: theme.textSec, fontStyle: 'italic', padding: '8px', background: theme.headerBg, borderTop: `1px solid ${theme.border}44`}}>
                                <Icons.Lock style={{ verticalAlign: 'middle', marginRight: '4px', opacity: 0.5 }} />
                                Drag columns or use pin icons to freeze areas.
                            </div>
                        </div>
                    )}
                </div>
    );
}

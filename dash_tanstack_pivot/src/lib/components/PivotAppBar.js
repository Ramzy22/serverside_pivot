import React, { useState, useRef, useEffect } from 'react';
import Icons from './Icons';
import { themes } from '../utils/styles';

const COLOR_PALETTE_OPTIONS = [
    { value: 'redGreen',   label: 'Red → Green' },
    { value: 'greenRed',   label: 'Green → Red' },
    { value: 'blueWhite',  label: 'Blue → White' },
    { value: 'yellowBlue', label: 'Yellow → Blue' },
    { value: 'orangePurp', label: 'Orange → Purple' },
];

const FONT_FAMILY_OPTIONS = [
    { value: "'Inter', system-ui, sans-serif", label: 'Inter' },
    { value: "'JetBrains Mono', monospace", label: 'JetBrains Mono' },
    { value: "'Plus Jakarta Sans', sans-serif", label: 'Plus Jakarta Sans' },
    { value: 'system-ui', label: 'System UI' },
    { value: 'Arial, sans-serif', label: 'Arial' },
    { value: 'Helvetica, sans-serif', label: 'Helvetica' },
    { value: 'Georgia, serif', label: 'Georgia' },
    { value: '"Times New Roman", serif', label: 'Times New Roman' },
    { value: '"Courier New", monospace', label: 'Courier New' },
    { value: '"Trebuchet MS", sans-serif', label: 'Trebuchet MS' },
    { value: 'Verdana, sans-serif', label: 'Verdana' },
    { value: 'Tahoma, sans-serif', label: 'Tahoma' },
    { value: '"Palatino Linotype", serif', label: 'Palatino Linotype' },
    { value: '"Book Antiqua", serif', label: 'Book Antiqua' },
    { value: '"Comic Sans MS", cursive', label: 'Comic Sans MS' },
    { value: 'Impact, fantasy', label: 'Impact' },
    { value: '"Lucida Console", monospace', label: 'Lucida Console' },
    { value: '"Lucida Sans Unicode", sans-serif', label: 'Lucida Sans Unicode' },
    { value: '"MS Sans Serif", sans-serif', label: 'MS Sans Serif' },
];

const FONT_SIZE_OPTIONS = ['8px', '9px', '10px', '11px', '12px', '13px', '14px', '15px', '16px', '18px', '20px', '24px'];

const DECIMAL_MIN = 0;
const DECIMAL_MAX = 6;

function CellFormatPopover({ theme, styles, cellFormatRules, setCellFormatRules, selectedCellKeys, onClose, anchorRef }) {
    const firstKey = selectedCellKeys && selectedCellKeys.length > 0 ? selectedCellKeys[0] : null;
    const current = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
    const [bg, setBg] = useState(current.bg || '#ffffff');
    const [color, setColor] = useState(current.color || '#000000');
    const [bold, setBold] = useState(current.bold || false);
    const [italic, setItalic] = useState(current.italic || false);
    const popoverRef = useRef(null);

    // Sync local state when selection changes
    useEffect(() => {
        const rule = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
        setBg(rule.bg || '#ffffff');
        setColor(rule.color || '#000000');
        setBold(rule.bold || false);
        setItalic(rule.italic || false);
    }, [firstKey, cellFormatRules]);

    // Close on outside click
    useEffect(() => {
        const handleOutside = (e) => {
            if (popoverRef.current && !popoverRef.current.contains(e.target) &&
                anchorRef.current && !anchorRef.current.contains(e.target)) {
                onClose();
            }
        };
        document.addEventListener('mousedown', handleOutside);
        return () => document.removeEventListener('mousedown', handleOutside);
    }, [onClose, anchorRef]);

    const cellCount = selectedCellKeys ? selectedCellKeys.length : 0;

    const apply = () => {
        if (cellCount === 0) return;
        setCellFormatRules(prev => {
            const next = { ...prev };
            selectedCellKeys.forEach(k => { next[k] = { bg, color, bold, italic }; });
            return next;
        });
    };

    const clear = () => {
        if (cellCount === 0) return;
        setCellFormatRules(prev => {
            const next = { ...prev };
            selectedCellKeys.forEach(k => { delete next[k]; });
            return next;
        });
    };

    return (
        <div ref={popoverRef} style={{
            position: 'absolute',
            top: '100%',
            right: 0,
            zIndex: 9999,
            background: theme.background || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: '6px',
            boxShadow: '0 4px 16px rgba(0,0,0,0.18)',
            padding: '12px',
            minWidth: '220px',
            marginTop: '4px',
        }}>
            <div style={{fontWeight: 600, fontSize: '13px', color: theme.text, marginBottom: '8px'}}>
                Format Cell{cellCount > 1 ? ` (${cellCount} cells)` : ''}
            </div>
            {cellCount === 0 ? (
                <div style={{fontSize: '12px', color: theme.textSec, marginBottom: '8px'}}>Select cells to format</div>
            ) : (
                <>
                    <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginBottom: '8px'}}>
                        <div>
                            <label style={{fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '3px'}}>Background</label>
                            <div style={{display: 'flex', gap: '4px', alignItems: 'center'}}>
                                <input type="color" value={bg} onChange={e => setBg(e.target.value)}
                                    style={{width: '28px', height: '28px', border: 'none', padding: 0, cursor: 'pointer', borderRadius: '3px'}} />
                                <span style={{fontSize: '11px', color: theme.textSec}}>{bg}</span>
                            </div>
                        </div>
                        <div>
                            <label style={{fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '3px'}}>Text Color</label>
                            <div style={{display: 'flex', gap: '4px', alignItems: 'center'}}>
                                <input type="color" value={color} onChange={e => setColor(e.target.value)}
                                    style={{width: '28px', height: '28px', border: 'none', padding: 0, cursor: 'pointer', borderRadius: '3px'}} />
                                <span style={{fontSize: '11px', color: theme.textSec}}>{color}</span>
                            </div>
                        </div>
                    </div>
                    <div style={{display: 'flex', gap: '8px', marginBottom: '10px'}}>
                        <button onClick={() => setBold(b => !b)}
                            style={{...styles.btn, fontWeight: 'bold', background: bold ? theme.select : theme.hover, minWidth: '36px'}}
                            title="Bold">B</button>
                        <button onClick={() => setItalic(i => !i)}
                            style={{...styles.btn, fontStyle: 'italic', background: italic ? theme.select : theme.hover, minWidth: '36px'}}
                            title="Italic"><em>I</em></button>
                    </div>
                    <div style={{display: 'flex', gap: '6px'}}>
                        <button onClick={apply} style={{...styles.btn, background: theme.primary, color: '#fff', flex: 1}}>
                            Apply{cellCount > 1 ? ` (${cellCount})` : ''}
                        </button>
                        <button onClick={clear} style={{...styles.btn, background: theme.hover, flex: 1}}>Clear</button>
                    </div>
                </>
            )}
        </div>
    );
}

export function PivotAppBar({
    sidebarOpen, setSidebarOpen,
    themeName, setThemeName,
    showRowNumbers, setShowRowNumbers,
    showFloatingFilters, setShowFloatingFilters,
    showRowTotals, setShowRowTotals,
    showColTotals, setShowColTotals,
    spacingMode, setSpacingMode, spacingLabels,
    layoutMode, setLayoutMode,
    colorScaleMode, setColorScaleMode,
    colorPalette, setColorPalette,
    rowCount, exportPivot,
    theme, styles,
    filters, setFilters,
    onSaveView,
    pivotTitle,
    fontFamily, setFontFamily,
    fontSize, setFontSize,
    displayDecimal, onDecimalChange, hasSelection,
    cellFormatRules, setCellFormatRules,
    selectedCellKeys,
    dataBarsColumns, setDataBarsColumns,
    selectedCellColIds,
}) {
    const [rowFmtOpen, setRowFmtOpen] = useState(false);
    const rowFmtBtnRef = useRef(null);

    // Base styles for different button variants
    const btnBase = {
        ...styles.btn,
        borderRadius: '6px',
        padding: '4px 10px',
        fontSize: '12px',
        fontWeight: 500,
        lineHeight: 1.5,
    };
    const btnGhost = {
        ...btnBase,
        background: 'transparent',
        border: `1px solid transparent`,
    };
    const btnSubtle = {
        ...btnBase,
        background: theme.hover,
        border: `1px solid ${theme.border}`,
    };
    const btnActive = {
        ...btnBase,
        background: theme.select,
        border: `1px solid ${theme.primary}`,
        color: theme.primary,
    };
    const btnPrimary = {
        ...btnBase,
        background: theme.primary,
        color: '#fff',
        border: `1px solid ${theme.primary}`,
    };
    // Save View: animated shimmer gradient (same keyframe as skeleton loader)
    const btnSaveView = {
        ...btnBase,
        background: 'linear-gradient(90deg, rgba(75,139,245,0.15) 0%, rgba(120,175,255,0.38) 45%, rgba(75,139,245,0.15) 100%)',
        backgroundSize: '220% 100%',
        border: `1px solid rgba(75,139,245,0.45)`,
        color: theme.primary,
        animation: 'pivot-skeleton-shimmer 2.8s linear infinite',
        fontWeight: 600,
    };

    const sep = <div style={{width:'1px', height:'18px', background: theme.border, flexShrink: 0}} />;

    return (
        <div style={{...styles.appBar, height: 'auto', minHeight: '48px', padding: '6px 12px', gap: '6px', flexWrap: 'wrap'}}>
            {/* Left: sidebar toggle + title */}
            <div style={{display:'flex', alignItems:'center', gap:'8px', flexShrink: 0}}>
                <button onClick={() => setSidebarOpen(!sidebarOpen)} style={{...btnGhost, padding:'5px', color: theme.textSec}}>
                    <Icons.Menu />
                </button>
                <div style={{fontWeight:600, fontSize:'14px', color:theme.primary, whiteSpace:'nowrap'}}>{pivotTitle || 'Analytics Pivot'}</div>
            </div>

            {/* Search */}
            <div style={{...styles.searchBox, flex:'1', minWidth:'120px', maxWidth:'220px', borderRadius:'6px', border:`1px solid ${theme.border}`}}>
                <Icons.Search />
                <input
                    style={{border:'none',background:'transparent',marginLeft:'6px',outline:'none',width:'100%', color: theme.text, fontSize:'12px'}}
                    placeholder="Search…"
                    value={(filters && filters.global) || ''}
                    onChange={e => {
                        const val = e.target.value;
                        setFilters(p => {
                            const next = {...p};
                            if (val) next.global = val;
                            else delete next.global;
                            return next;
                        });
                    }}
                />
            </div>

            {/* Controls */}
            <div style={{display:'flex', gap:'4px', flexWrap:'wrap', alignItems:'center'}}>
                {/* Row # toggle */}
                <button style={showRowNumbers ? btnActive : btnSubtle} onClick={() => setShowRowNumbers(!showRowNumbers)} title="Toggle row numbers">Row #</button>

                {sep}

                {/* Decimal controls */}
                <div style={{display:'flex', alignItems:'center', gap:'2px', background: theme.hover, border:`1px solid ${theme.border}`, borderRadius:'6px', padding:'2px 4px'}}>
                    <button
                        title={hasSelection ? 'Decrease decimals for selected cells' : 'Decrease decimal places'}
                        style={{...btnGhost, padding:'2px 6px', fontFamily:'monospace', fontSize:'11px', minWidth:'28px', opacity: displayDecimal <= DECIMAL_MIN ? 0.35 : 1}}
                        onClick={() => onDecimalChange(-1)}
                        disabled={displayDecimal <= DECIMAL_MIN}
                    >.0←</button>
                    <span style={{fontSize:'11px', color: hasSelection ? theme.primary : theme.textSec, minWidth:'14px', textAlign:'center', fontWeight: hasSelection ? 700 : 400}}>{displayDecimal}</span>
                    <button
                        title={hasSelection ? 'Increase decimals for selected cells' : 'Increase decimal places'}
                        style={{...btnGhost, padding:'2px 6px', fontFamily:'monospace', fontSize:'11px', minWidth:'28px', opacity: displayDecimal >= DECIMAL_MAX ? 0.35 : 1}}
                        onClick={() => onDecimalChange(1)}
                        disabled={displayDecimal >= DECIMAL_MAX}
                    >.00→</button>
                </div>

                {sep}

                {/* View toggles */}
                <button style={showFloatingFilters ? btnActive : btnSubtle} onClick={() => setShowFloatingFilters(!showFloatingFilters)}>Filters</button>
                <button style={showRowTotals ? btnActive : btnSubtle} onClick={() => setShowRowTotals(!showRowTotals)}>Row Total</button>
                <button style={showColTotals ? btnActive : btnSubtle} onClick={() => setShowColTotals(!showColTotals)}>Col Total</button>

                {sep}

                {/* Spacing & layout */}
                <button style={btnSubtle} onClick={() => setSpacingMode((spacingMode + 1) % 3)} title="Cycle row density">
                    <Icons.Spacing/> {spacingLabels[spacingMode]}
                </button>
                <button style={btnSubtle} onClick={() => setLayoutMode(prev => prev === 'hierarchy' ? 'outline' : prev === 'outline' ? 'tabular' : 'hierarchy')} title="Cycle layout mode">
                    {layoutMode === 'hierarchy' ? 'Hierarchy' : layoutMode === 'outline' ? 'Outline' : 'Tabular'}
                </button>

                {sep}

                {/* Color scale */}
                <select
                    value={colorScaleMode}
                    onChange={e => setColorScaleMode(e.target.value)}
                    title="Color scale"
                    style={{...btnSubtle, outline:'none', cursor:'pointer', ...(colorScaleMode !== 'off' ? {background: theme.select, border:`1px solid ${theme.primary}`, color: theme.primary} : {})}}
                >
                    <option value="off">Color: Off</option>
                    <option value="row">By Row</option>
                    <option value="col">By Col</option>
                    <option value="table">By Table</option>
                </select>
                {colorScaleMode !== 'off' && (
                    <select
                        value={colorPalette}
                        onChange={e => setColorPalette(e.target.value)}
                        style={{...btnSubtle, outline:'none', cursor:'pointer', background: theme.select, border:`1px solid ${theme.primary}`, color: theme.primary}}
                    >
                        {COLOR_PALETTE_OPTIONS.map(opt => (
                            <option key={opt.value} value={opt.value}>{opt.label}</option>
                        ))}
                    </select>
                )}

                {/* Data Bars */}
                {(() => {
                    const hasCellSel = selectedCellColIds && selectedCellColIds.size > 0;
                    const hasBars = hasCellSel && [...selectedCellColIds].some(id => dataBarsColumns && dataBarsColumns.has(id));
                    const anyBars = dataBarsColumns && dataBarsColumns.size > 0;
                    const isActive = hasBars || anyBars;
                    return (
                        <button
                            title={hasCellSel ? 'Toggle data bars for selected columns' : 'Data Bars — select cells to target columns'}
                            style={isActive ? btnActive : btnSubtle}
                            onClick={() => {
                                if (!setDataBarsColumns) return;
                                if (hasCellSel) {
                                    setDataBarsColumns(prev => {
                                        const next = new Set(prev);
                                        const allOn = [...selectedCellColIds].every(id => next.has(id));
                                        selectedCellColIds.forEach(id => allOn ? next.delete(id) : next.add(id));
                                        return next;
                                    });
                                } else {
                                    setDataBarsColumns(new Set());
                                }
                            }}
                        >
                            📊 Data Bars{anyBars ? ` (${dataBarsColumns.size})` : ''}
                        </button>
                    );
                })()}

                {sep}

                {/* Font controls */}
                <select value={fontFamily} onChange={e => setFontFamily(e.target.value)} title="Font family"
                    style={{...btnSubtle, outline:'none', cursor:'pointer', fontFamily}}>
                    {FONT_FAMILY_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value} style={{fontFamily: opt.value}}>{opt.label}</option>
                    ))}
                </select>
                <select value={fontSize} onChange={e => setFontSize(e.target.value)} title="Font size"
                    style={{...btnSubtle, outline:'none', cursor:'pointer'}}>
                    {FONT_SIZE_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
                </select>

                {sep}

                {/* Format Cell */}
                <div style={{position:'relative'}}>
                    <button
                        ref={rowFmtBtnRef}
                        style={rowFmtOpen || Object.keys(cellFormatRules || {}).length > 0 ? btnActive : btnSubtle}
                        onClick={() => setRowFmtOpen(o => !o)}
                        title="Format Cell — select cells first"
                    >
                        Format Cell{Object.keys(cellFormatRules || {}).length > 0 ? ` (${Object.keys(cellFormatRules).length})` : ''}
                    </button>
                    {rowFmtOpen && (
                        <CellFormatPopover
                            theme={theme}
                            styles={styles}
                            cellFormatRules={cellFormatRules}
                            setCellFormatRules={setCellFormatRules}
                            selectedCellKeys={selectedCellKeys}
                            onClose={() => setRowFmtOpen(false)}
                            anchorRef={rowFmtBtnRef}
                        />
                    )}
                </div>

                <button style={btnSaveView} onClick={onSaveView}><Icons.Save /> Save View</button>

                {sep}

                {/* Theme */}
                <select value={themeName} onChange={e => setThemeName(e.target.value)}
                    style={{...btnSubtle, outline:'none', cursor:'pointer'}}>
                    {Object.keys(themes).map(t => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
                </select>

                {/* Export */}
                <button style={btnPrimary} onClick={exportPivot}>
                    <Icons.Export/> {(rowCount || 0) > 500000 ? 'Export CSV' : 'Export'}
                </button>
            </div>
        </div>
    );
}

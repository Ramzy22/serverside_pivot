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
    { value: 'system-ui', label: 'System UI' },
    { value: 'Arial, sans-serif', label: 'Arial' },
    { value: 'Georgia, serif', label: 'Georgia' },
    { value: '"Courier New", monospace', label: 'Courier New' },
    { value: '"Trebuchet MS", sans-serif', label: 'Trebuchet MS' },
];

const FONT_SIZE_OPTIONS = ['11px', '12px', '13px', '14px', '16px'];

const DECIMAL_MIN = 0;
const DECIMAL_MAX = 6;

function RowFormatPopover({ theme, styles, rowFormatRules, setRowFormatRules, hoveredRowPath, onClose, anchorRef }) {
    const [path, setPath] = useState(hoveredRowPath || '');
    const current = (path && rowFormatRules[path]) || {};
    const [bg, setBg] = useState(current.bg || '#ffffff');
    const [color, setColor] = useState(current.color || '#000000');
    const [bold, setBold] = useState(current.bold || false);
    const [italic, setItalic] = useState(current.italic || false);
    const popoverRef = useRef(null);

    // Update local state when path changes
    useEffect(() => {
        const rule = (path && rowFormatRules[path]) || {};
        setBg(rule.bg || '#ffffff');
        setColor(rule.color || '#000000');
        setBold(rule.bold || false);
        setItalic(rule.italic || false);
    }, [path, rowFormatRules]);

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

    const apply = () => {
        if (!path) return;
        setRowFormatRules(prev => ({
            ...prev,
            [path]: { bg, color, bold, italic }
        }));
    };

    const clear = () => {
        if (!path) return;
        setRowFormatRules(prev => {
            const next = { ...prev };
            delete next[path];
            return next;
        });
    };

    const inputStyle = {
        border: `1px solid ${theme.border}`,
        borderRadius: '4px',
        padding: '3px 6px',
        background: theme.bg || theme.surface || '#fff',
        color: theme.text,
        fontSize: '12px',
        width: '100%',
        boxSizing: 'border-box',
    };

    return (
        <div ref={popoverRef} style={{
            position: 'absolute',
            top: '100%',
            right: 0,
            zIndex: 9999,
            background: theme.bg || theme.surface || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: '6px',
            boxShadow: '0 4px 16px rgba(0,0,0,0.18)',
            padding: '12px',
            minWidth: '240px',
            marginTop: '4px',
        }}>
            <div style={{fontWeight: 600, fontSize: '13px', color: theme.text, marginBottom: '10px'}}>Format Row</div>
            <div style={{marginBottom: '8px'}}>
                <label style={{fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '3px'}}>Row Path</label>
                <input
                    style={inputStyle}
                    value={path}
                    onChange={e => setPath(e.target.value)}
                    placeholder="e.g. North|Electronics"
                />
                {hoveredRowPath && hoveredRowPath !== path && (
                    <button
                        onClick={() => setPath(hoveredRowPath)}
                        style={{...styles.btn, background: 'transparent', fontSize: '11px', color: theme.primary, padding: '2px 0', marginTop: '3px'}}
                    >
                        Use last clicked: {hoveredRowPath}
                    </button>
                )}
            </div>
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
                <button
                    onClick={() => setBold(b => !b)}
                    style={{...styles.btn, fontWeight: 'bold', background: bold ? theme.select : theme.hover, minWidth: '36px'}}
                    title="Bold"
                >B</button>
                <button
                    onClick={() => setItalic(i => !i)}
                    style={{...styles.btn, fontStyle: 'italic', background: italic ? theme.select : theme.hover, minWidth: '36px'}}
                    title="Italic"
                ><em>I</em></button>
            </div>
            <div style={{display: 'flex', gap: '6px'}}>
                <button onClick={apply} style={{...styles.btn, background: theme.primary, color: '#fff', flex: 1}}>Apply</button>
                <button onClick={clear} style={{...styles.btn, background: theme.hover, flex: 1}}>Clear</button>
            </div>
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
    decimalPlaces, setDecimalPlaces,
    rowFormatRules, setRowFormatRules,
    hoveredRowPath,
}) {
    const [rowFmtOpen, setRowFmtOpen] = useState(false);
    const rowFmtBtnRef = useRef(null);

    return (
        <div style={styles.appBar}>
            <div style={{display:'flex', alignItems:'center', gap:'12px'}}>
                <button onClick={() => setSidebarOpen(!sidebarOpen)} style={{border:'none', background:'transparent', cursor:'pointer', padding:'4px', borderRadius:'4px', display:'flex', color: theme.textSec}}>
                    <Icons.Menu />
                </button>
                <div style={{fontWeight:500,fontSize:'16px',color:theme.primary}}>{pivotTitle || 'Analytics Pivot'}</div>
            </div>
            <div style={styles.searchBox}>
                <Icons.Search />
                <input
                    style={{border:'none',background:'transparent',marginLeft:'8px',outline:'none',width:'100%', color: theme.text}}
                    placeholder="Global Search..."
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
            <div style={{display:'flex',gap:'8px',flexWrap:'wrap',alignItems:'center'}}>
                <button style={{...styles.btn, background: showRowNumbers ? theme.select : 'transparent'}} onClick={() => setShowRowNumbers(!showRowNumbers)}>Row #</button>
                <button style={{...styles.btn, background: showFloatingFilters ? theme.select : 'transparent'}} onClick={() => setShowFloatingFilters(!showFloatingFilters)}>Filters</button>
                <button style={{...styles.btn, background: showRowTotals ? theme.select : 'transparent'}} onClick={() => setShowRowTotals(!showRowTotals)}>Row Totals</button>
                <button style={{...styles.btn, background: showColTotals ? theme.select : 'transparent'}} onClick={() => setShowColTotals(!showColTotals)}>Col Totals</button>
                <button style={{...styles.btn, background: theme.hover}} onClick={() => setSpacingMode((spacingMode + 1) % 3)}>
                    <Icons.Spacing/> {spacingLabels[spacingMode]}
                </button>
                <button style={{...styles.btn, background: theme.hover}} onClick={() => setLayoutMode(prev => prev === 'hierarchy' ? 'outline' : prev === 'outline' ? 'tabular' : 'hierarchy')}>
                    {layoutMode === 'hierarchy' ? 'Hierarchy' : layoutMode === 'outline' ? 'Outline' : 'Tabular'}
                </button>
                <select
                    value={colorScaleMode}
                    onChange={e => setColorScaleMode(e.target.value)}
                    title="Color scale mode"
                    style={{
                        ...styles.btn,
                        background: colorScaleMode !== 'off' ? theme.select : theme.hover,
                        outline: 'none',
                        cursor: 'pointer',
                        padding: '4px 8px',
                    }}
                >
                    <option value="off">Color: Off</option>
                    <option value="row">Color: By Row</option>
                    <option value="col">Color: By Column</option>
                    <option value="table">Color: By Table</option>
                </select>
                {colorScaleMode !== 'off' && (
                    <select
                        value={colorPalette}
                        onChange={e => setColorPalette(e.target.value)}
                        title="Color palette"
                        style={{
                            ...styles.btn,
                            background: theme.select,
                            outline: 'none',
                            cursor: 'pointer',
                            padding: '4px 8px',
                        }}
                    >
                        {COLOR_PALETTE_OPTIONS.map(opt => (
                            <option key={opt.value} value={opt.value}>{opt.label}</option>
                        ))}
                    </select>
                )}

                {/* Font family selector */}
                <select
                    value={fontFamily}
                    onChange={e => setFontFamily(e.target.value)}
                    title="Font family"
                    style={{
                        ...styles.btn,
                        background: theme.hover,
                        outline: 'none',
                        cursor: 'pointer',
                        padding: '4px 8px',
                        fontFamily: fontFamily,
                    }}
                >
                    {FONT_FAMILY_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value} style={{fontFamily: opt.value}}>{opt.label}</option>
                    ))}
                </select>

                {/* Font size selector */}
                <select
                    value={fontSize}
                    onChange={e => setFontSize(e.target.value)}
                    title="Font size"
                    style={{
                        ...styles.btn,
                        background: theme.hover,
                        outline: 'none',
                        cursor: 'pointer',
                        padding: '4px 8px',
                    }}
                >
                    {FONT_SIZE_OPTIONS.map(s => (
                        <option key={s} value={s}>{s}</option>
                    ))}
                </select>

                {/* Decimal place controls */}
                <div style={{display:'flex', alignItems:'center', gap:'2px'}}>
                    <button
                        title="Decrease decimal places"
                        style={{...styles.btn, background: theme.hover, fontFamily: 'monospace', minWidth: '32px'}}
                        onClick={() => setDecimalPlaces(p => Math.max(DECIMAL_MIN, p - 1))}
                        disabled={decimalPlaces <= DECIMAL_MIN}
                    >.0</button>
                    <span style={{fontSize:'11px', color: theme.textSec, minWidth:'16px', textAlign:'center'}}>{decimalPlaces}</span>
                    <button
                        title="Increase decimal places"
                        style={{...styles.btn, background: theme.hover, fontFamily: 'monospace', minWidth: '32px'}}
                        onClick={() => setDecimalPlaces(p => Math.min(DECIMAL_MAX, p + 1))}
                        disabled={decimalPlaces >= DECIMAL_MAX}
                    >.00</button>
                </div>

                {/* Format Row button */}
                <div style={{position: 'relative'}}>
                    <button
                        ref={rowFmtBtnRef}
                        style={{
                            ...styles.btn,
                            background: rowFmtOpen || Object.keys(rowFormatRules || {}).length > 0 ? theme.select : theme.hover
                        }}
                        onClick={() => setRowFmtOpen(o => !o)}
                        title="Format Row"
                    >
                        Format Row {Object.keys(rowFormatRules || {}).length > 0 ? `(${Object.keys(rowFormatRules).length})` : ''}
                    </button>
                    {rowFmtOpen && (
                        <RowFormatPopover
                            theme={theme}
                            styles={styles}
                            rowFormatRules={rowFormatRules}
                            setRowFormatRules={setRowFormatRules}
                            hoveredRowPath={hoveredRowPath}
                            onClose={() => setRowFmtOpen(false)}
                            anchorRef={rowFmtBtnRef}
                        />
                    )}
                </div>

                <button style={{...styles.btn, background: theme.hover}} onClick={onSaveView}>Save View</button>

                <div style={{width: '1px', height: '20px', background: theme.border, margin: '0 4px'}} />
                <select value={themeName} onChange={e => setThemeName(e.target.value)} style={{...styles.btn, background: theme.hover, padding: '4px 8px', outline: 'none'}}>
                    {Object.keys(themes).map(t => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
                </select>

                <button style={{
                    ...styles.btn,
                    background: theme.primary,
                    color: '#fff',
                    border: `1px solid ${theme.primary}`,
                    '&:hover': {
                        background: theme.primary,
                        filter: 'brightness(1.1)'
                    }
                }} onClick={exportPivot}>
                    <Icons.Export/> {(rowCount || 0) > 500000 ? 'Export CSV' : 'Export'}
                </button>
            </div>
        </div>
    );
}

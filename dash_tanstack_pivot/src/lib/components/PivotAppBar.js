import React, { useState, useRef, useEffect, useMemo } from 'react';
import Icons from './Icons';
import { themes } from '../utils/styles';

const COLOR_PALETTE_OPTIONS = [
    { value: 'redGreen',   label: 'Red to Green' },
    { value: 'greenRed',   label: 'Green to Red' },
    { value: 'blueWhite',  label: 'Blue to White' },
    { value: 'yellowBlue', label: 'Yellow to Blue' },
    { value: 'orangePurp', label: 'Orange to Purple' },
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
const THEME_ORDER = ['flash', 'dark', 'bloomblerg_black', 'blooomberg'];
const THEME_LABELS = {
    flash: 'Flash',
    dark: 'Dark',
    bloomblerg_black: 'Bloomblerg Black',
    blooomberg: 'Blooomberg',
    light: 'Light',
    material: 'Material',
    balham: 'Balham',
    strata: 'Strata',
    crystal: 'Crystal',
    alabaster: 'Alabaster',
    satin: 'Satin',
};

const DECIMAL_MIN = 0;
const DECIMAL_MAX = 6;
const ZOOM_MIN = 60;
const ZOOM_MAX = 160;
const ZOOM_STEP = 10;

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

const THEME_COLOR_FIELDS = [
    { key: 'primary', label: 'Primary' },
    { key: 'pageBg', label: 'Page Bg' },
    { key: 'surfaceBg', label: 'Surface Bg' },
    { key: 'surfaceInset', label: 'Inset Bg' },
    { key: 'headerBg', label: 'Header Bg' },
    { key: 'headerSubtleBg', label: 'Soft Header' },
    { key: 'border', label: 'Border' },
    { key: 'text', label: 'Text' },
    { key: 'textSec', label: 'Muted Text' },
    { key: 'totalBgStrong', label: 'Total Bg' },
];

function ThemeEditorPopover({ theme, themeName, themeOverrides, setThemeOverrides, onClose, anchorRef }) {
    const popoverRef = useRef(null);

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

    const resetField = (key) => {
        setThemeOverrides(prev => {
            const next = { ...prev };
            delete next[key];
            return next;
        });
    };

    const clearAll = () => setThemeOverrides({});

    return (
        <div ref={popoverRef} style={{
            position: 'absolute',
            top: '100%',
            right: 0,
            zIndex: 9999,
            background: theme.surfaceBg || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: theme.radiusSm || '10px',
            boxShadow: theme.shadowMd || '0 12px 28px rgba(0,0,0,0.12)',
            padding: '12px',
            minWidth: '280px',
            marginTop: '6px',
        }}>
            <div style={{ fontWeight: 700, fontSize: '13px', color: theme.text, marginBottom: '4px' }}>Theme Colors</div>
            <div style={{ fontSize: '11px', color: theme.textSec, marginBottom: '10px' }}>{themeName} with saved overrides</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                {THEME_COLOR_FIELDS.map(({ key, label }) => (
                    <div key={key} style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                        <label style={{ fontSize: '11px', color: theme.textSec }}>{label}</label>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <input
                                type="color"
                                value={theme[key] || '#000000'}
                                onChange={(e) => setThemeOverrides(prev => ({ ...prev, [key]: e.target.value }))}
                                style={{ width: '28px', height: '28px', border: 'none', padding: 0, background: 'transparent', cursor: 'pointer' }}
                            />
                            <button onClick={() => resetField(key)} style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.headerSubtleBg || theme.hover,
                                color: theme.textSec,
                                borderRadius: '8px',
                                fontSize: '10px',
                                padding: '5px 6px',
                                cursor: 'pointer'
                            }}>
                                Reset
                            </button>
                        </div>
                    </div>
                ))}
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
                <button onClick={clearAll} style={{ border: `1px solid ${theme.border}`, background: theme.headerSubtleBg || theme.hover, color: theme.text, borderRadius: '8px', padding: '7px 10px', cursor: 'pointer', flex: 1 }}>Clear Overrides</button>
                <button onClick={onClose} style={{ border: `1px solid ${theme.primary}`, background: theme.primary, color: '#fff', borderRadius: '8px', padding: '7px 10px', cursor: 'pointer', flex: 1 }}>Done</button>
            </div>
        </div>
    );
}

export function PivotAppBar({
    sidebarOpen, setSidebarOpen,
    themeName, setThemeName,
    themeOverrides, setThemeOverrides,
    showRowNumbers, setShowRowNumbers,
    showFloatingFilters, setShowFloatingFilters,
    stickyHeaders, setStickyHeaders,
    showRowTotals, setShowRowTotals,
    showColTotals, setShowColTotals,
    spacingMode, setSpacingMode, spacingLabels,
    layoutMode, setLayoutMode,
    onAutoSizeColumns,
    colorScaleMode, setColorScaleMode,
    colorPalette, setColorPalette,
    rowCount, exportPivot,
    onTransposePivot,
    canTranspose,
    theme, styles,
    filters, setFilters,
    onSaveView,
    pivotTitle,
    fontFamily, setFontFamily,
    fontSize, setFontSize,
    zoomLevel, setZoomLevel,
    decimalPlaces, setDecimalPlaces,
    columnDecimalOverrides, setColumnDecimalOverrides,
    cellFormatRules, setCellFormatRules,
    selectedCells,
    dataBarsColumns, setDataBarsColumns,
    canCreateSelectionChart,
    onCreateSelectionChart,
    onAddChartPane,
}) {
    // Derive all selection-dependent state directly from selectedCells
    // inside AppBar to avoid stale prop issues from parent renders
    const selectedCellKeys = useMemo(() => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return [];
        return keys.map(key => {
            const colonIdx = key.indexOf(':');
            const rowId = key.substring(0, colonIdx);
            const colId = key.substring(colonIdx + 1);
            return `${rowId}:::${colId}`;
        });
    }, [selectedCells]);

    const selectedCellColIds = useMemo(() => {
        const ids = new Set();
        Object.keys(selectedCells || {}).forEach(key => {
            const idx = key.indexOf(':');
            if (idx >= 0) ids.add(key.substring(idx + 1));
        });
        return ids;
    }, [selectedCells]);

    const hasSelection = Object.keys(selectedCells || {}).length > 0;

    const handleDecimalChange = (delta) => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) {
            setDecimalPlaces(prev => Math.max(0, Math.min(6, prev + delta)));
            return;
        }
        const colIds = [...new Set(keys.map(k => {
            const idx = k.indexOf(':');
            return idx >= 0 ? k.substring(idx + 1) : k;
        }))];
        setColumnDecimalOverrides(prev => {
            const next = { ...prev };
            colIds.forEach(colId => {
                const cur = next[colId] !== undefined ? next[colId] : decimalPlaces;
                next[colId] = Math.max(0, Math.min(6, cur + delta));
            });
            return next;
        });
    };

    const displayDecimal = useMemo(() => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return decimalPlaces;
        const k = keys[0];
        const idx = k.indexOf(':');
        const colId = idx >= 0 ? k.substring(idx + 1) : k;
        return columnDecimalOverrides[colId] !== undefined ? columnDecimalOverrides[colId] : decimalPlaces;
    }, [selectedCells, columnDecimalOverrides, decimalPlaces]);
    const [cellFormatOpen, setCellFormatOpen] = useState(false);
    const cellFormatBtnRef = useRef(null);
    const [themeEditorOpen, setThemeEditorOpen] = useState(false);
    const themeEditorBtnRef = useRef(null);
    const [openToolbarSections, setOpenToolbarSections] = useState({
        view: true,
        format: false,
        charts: true,
        theme: false,
    });
    const hasSavedCellFormats = Object.keys(cellFormatRules || {}).length > 0;
    const handleZoomChange = (delta) => {
        setZoomLevel(prev => Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, prev + delta)));
    };
    const toggleToolbarSection = (sectionId) => {
        setOpenToolbarSections(prev => ({ ...prev, [sectionId]: !prev[sectionId] }));
    };

    // Base styles for different button variants
    const btnBase = {
        ...styles.btn,
        borderRadius: theme.radiusSm || '10px',
        padding: '7px 12px',
        fontSize: '12px',
        fontWeight: 600,
        lineHeight: 1.5,
        minHeight: '36px',
    };
    const btnGhost = {
        ...btnBase,
        background: 'transparent',
        border: `1px solid transparent`,
    };
    const btnSubtle = {
        ...btnBase,
        background: theme.headerSubtleBg || theme.hover,
        border: `1px solid ${theme.border}`,
        boxShadow: theme.shadowInset || 'none',
    };
    const btnActive = {
        ...btnBase,
        background: theme.select,
        border: `1px solid ${theme.primary}`,
        color: theme.primary,
        boxShadow: theme.shadowInset || 'none',
    };
    const btnPrimary = {
        ...btnBase,
        background: theme.primary,
        color: '#fff',
        border: `1px solid ${theme.primary}`,
        boxShadow: '0 8px 18px rgba(79,70,229,0.18)',
    };
    // Save View: animated shimmer gradient (same keyframe as skeleton loader)
    const btnSaveView = {
        ...btnBase,
        background: 'linear-gradient(90deg, rgba(79,70,229,0.10) 0%, rgba(129,140,248,0.30) 45%, rgba(79,70,229,0.10) 100%)',
        backgroundSize: '220% 100%',
        border: `1px solid rgba(99,102,241,0.28)`,
        color: theme.primary,
        animation: 'pivot-skeleton-shimmer 2.8s linear infinite',
        fontWeight: 600,
        boxShadow: theme.shadowInset || 'none',
    };

    const firstLineStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        rowGap: '8px',
        flexWrap: 'wrap',
        width: '100%',
        minHeight: '40px',
    };
    const innerDividerStyle = {
        width: '1px',
        alignSelf: 'center',
        minHeight: '24px',
        background: theme.border,
        opacity: 0.8,
        flexShrink: 0,
    };
    const secondLineStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        rowGap: '8px',
        flexWrap: 'wrap',
        width: '100%',
        paddingTop: '8px',
        borderTop: `1px solid ${theme.border}`,
    };
    const sectionBlockStyle = (isLast = false) => ({
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        rowGap: '8px',
        flexWrap: 'wrap',
        minHeight: '36px',
        paddingRight: isLast ? 0 : '12px',
        marginRight: isLast ? 0 : '2px',
        borderRight: isLast ? 'none' : `1px solid ${theme.border}`,
    });
    const sectionLabelStyle = {
        fontSize: '10px',
        fontWeight: 800,
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: theme.textSec,
        whiteSpace: 'nowrap',
        paddingRight: '4px',
        display: 'inline-flex',
        alignItems: 'center',
    };
    const compactSelectStyle = {
        ...btnSubtle,
        outline: 'none',
        cursor: 'pointer',
    };
    const sectionToggleButtonStyle = (active) => (active ? btnActive : btnSubtle);

    const viewControls = (
        <>
            <button style={showRowNumbers ? btnActive : btnSubtle} onClick={() => setShowRowNumbers(!showRowNumbers)} title="Toggle row numbers">Row #</button>
            <button style={stickyHeaders ? btnActive : btnSubtle} onClick={() => setStickyHeaders(!stickyHeaders)}>Sticky Header</button>
            <button style={showFloatingFilters ? btnActive : btnSubtle} onClick={() => setShowFloatingFilters(!showFloatingFilters)}>Filters</button>
            <button style={showRowTotals ? btnActive : btnSubtle} onClick={() => setShowRowTotals(!showRowTotals)}>Row Total</button>
            <button style={showColTotals ? btnActive : btnSubtle} onClick={() => setShowColTotals(!showColTotals)}>Col Total</button>
            <button
                style={canTranspose ? btnSubtle : { ...btnSubtle, opacity: 0.45, cursor: 'not-allowed' }}
                onClick={onTransposePivot}
                title="Swap row fields and column fields"
                disabled={!canTranspose}
            >
                <Icons.Transpose /> Transpose
            </button>
            <button style={btnSubtle} onClick={() => setSpacingMode((spacingMode + 1) % 3)} title="Cycle row density">
                <Icons.Spacing/> {spacingLabels[spacingMode]}
            </button>
            <button style={btnSubtle} onClick={() => setLayoutMode(prev => prev === 'hierarchy' ? 'outline' : prev === 'outline' ? 'tabular' : 'hierarchy')} title="Cycle layout mode">
                {layoutMode === 'hierarchy' ? 'Hierarchy' : layoutMode === 'outline' ? 'Outline' : 'Tabular'}
            </button>
            <button style={btnSubtle} onClick={onAutoSizeColumns} title="Auto size visible columns">
                Auto Size
            </button>
        </>
    );

    const formatControls = (
        <>
            <div style={{display:'flex', alignItems:'center', gap:'2px', background: theme.hover, border:`1px solid ${theme.border}`, borderRadius:'6px', padding:'2px 4px'}}>
                <button
                    title={hasSelection ? 'Decrease decimals for selected cells' : 'Decrease decimal places'}
                    style={{...btnGhost, padding:'2px 6px', fontFamily:'monospace', fontSize:'11px', minWidth:'28px', opacity: displayDecimal <= DECIMAL_MIN ? 0.35 : 1}}
                    onClick={() => handleDecimalChange(-1)}
                    disabled={displayDecimal <= DECIMAL_MIN}
                >-.0</button>
                <span style={{fontSize:'11px', color: hasSelection ? theme.primary : theme.textSec, minWidth:'14px', textAlign:'center', fontWeight: hasSelection ? 700 : 400}}>{displayDecimal}</span>
                <button
                    title={hasSelection ? 'Increase decimals for selected cells' : 'Increase decimal places'}
                    style={{...btnGhost, padding:'2px 6px', fontFamily:'monospace', fontSize:'11px', minWidth:'28px', opacity: displayDecimal >= DECIMAL_MAX ? 0.35 : 1}}
                    onClick={() => handleDecimalChange(1)}
                    disabled={displayDecimal >= DECIMAL_MAX}
                >+.00</button>
            </div>
            <div style={{ position:'relative' }}>
                <button
                    ref={cellFormatBtnRef}
                    style={cellFormatOpen || hasSelection || hasSavedCellFormats ? btnActive : btnSubtle}
                    onClick={() => setCellFormatOpen(open => !open)}
                    title={hasSelection ? 'Format selected cells' : 'Open cell formatting'}
                >
                    Format Cells{hasSelection ? ` (${selectedCellKeys.length})` : ''}
                </button>
                {cellFormatOpen && (
                    <CellFormatPopover
                        theme={theme}
                        styles={styles}
                        cellFormatRules={cellFormatRules}
                        setCellFormatRules={setCellFormatRules}
                        selectedCellKeys={selectedCellKeys}
                        onClose={() => setCellFormatOpen(false)}
                        anchorRef={cellFormatBtnRef}
                    />
                )}
            </div>
            <select
                value={colorScaleMode}
                onChange={e => setColorScaleMode(e.target.value)}
                title="Color scale"
                style={{...compactSelectStyle, ...(colorScaleMode !== 'off' ? {background: theme.select, border:`1px solid ${theme.primary}`, color: theme.primary} : {})}}
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
                    style={{...compactSelectStyle, background: theme.select, border:`1px solid ${theme.primary}`, color: theme.primary}}
                >
                    {COLOR_PALETTE_OPTIONS.map(opt => (
                        <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                </select>
            )}
            {(() => {
                const hasCellSel = selectedCellColIds && selectedCellColIds.size > 0;
                const hasBars = hasCellSel && [...selectedCellColIds].some(id => dataBarsColumns && dataBarsColumns.has(id));
                const anyBars = dataBarsColumns && dataBarsColumns.size > 0;
                const isActive = hasBars || anyBars;
                return (
                    <button
                        title={hasCellSel ? 'Toggle data bars for selected columns' : 'Data Bars - select cells to target columns'}
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
                        <><Icons.DataBars /> Data Bars{anyBars ? ` (${dataBarsColumns.size})` : ''}</>
                    </button>
                );
            })()}
            <select value={fontFamily} onChange={e => setFontFamily(e.target.value)} title="Font family"
                style={{...compactSelectStyle, fontFamily}}>
                {FONT_FAMILY_OPTIONS.map(opt => (
                    <option key={opt.value} value={opt.value} style={{fontFamily: opt.value}}>{opt.label}</option>
                ))}
            </select>
            <select value={fontSize} onChange={e => setFontSize(e.target.value)} title="Font size"
                style={compactSelectStyle}>
                {FONT_SIZE_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <div style={{display:'flex', alignItems:'center', gap:'2px', background: theme.hover, border:`1px solid ${theme.border}`, borderRadius:'6px', padding:'2px 4px'}}>
                <button
                    title="Zoom out"
                    style={{...btnGhost, padding:'2px 6px', fontSize:'12px', minWidth:'28px', opacity: zoomLevel <= ZOOM_MIN ? 0.35 : 1}}
                    onClick={() => handleZoomChange(-ZOOM_STEP)}
                    disabled={zoomLevel <= ZOOM_MIN}
                >
                    -
                </button>
                <span style={{fontSize:'11px', color: theme.textSec, minWidth:'42px', textAlign:'center', fontWeight: 600}}>{zoomLevel}%</span>
                <button
                    title="Zoom in"
                    style={{...btnGhost, padding:'2px 6px', fontSize:'12px', minWidth:'28px', opacity: zoomLevel >= ZOOM_MAX ? 0.35 : 1}}
                    onClick={() => handleZoomChange(ZOOM_STEP)}
                    disabled={zoomLevel >= ZOOM_MAX}
                >
                    +
                </button>
            </div>
        </>
    );

    const chartControls = (
        <>
            <button
                style={canCreateSelectionChart ? { ...btnSubtle, display: 'inline-flex', alignItems: 'center', gap: '6px' } : { ...btnSubtle, display: 'inline-flex', alignItems: 'center', gap: '6px', opacity: 0.45, cursor: 'not-allowed' }}
                onClick={onCreateSelectionChart}
                title={canCreateSelectionChart ? 'Create a range chart from the current cell selection' : 'Select cells to create a range chart'}
                disabled={!canCreateSelectionChart}
            >
                <Icons.Chart /> Range Chart
            </button>
            {typeof onAddChartPane === 'function' ? (
                <button
                    style={{ ...btnSubtle, display: 'inline-flex', alignItems: 'center', gap: '6px' }}
                    onClick={onAddChartPane}
                    title="Add a resizable chart pane beside the pivot table"
                >
                    <Icons.Columns /> New Chart Pane
                </button>
            ) : null}
        </>
    );

    const themeControls = (
        <>
            <div style={{ position:'relative' }}>
                <button
                    ref={themeEditorBtnRef}
                    style={Object.keys(themeOverrides || {}).length > 0 ? btnActive : btnSubtle}
                    onClick={() => setThemeEditorOpen(open => !open)}
                    title="Edit theme colors"
                >
                    Theme Colors{Object.keys(themeOverrides || {}).length > 0 ? ` (${Object.keys(themeOverrides).length})` : ''}
                </button>
                {themeEditorOpen && (
                    <ThemeEditorPopover
                        theme={theme}
                        themeName={themeName}
                        themeOverrides={themeOverrides}
                        setThemeOverrides={setThemeOverrides}
                        onClose={() => setThemeEditorOpen(false)}
                        anchorRef={themeEditorBtnRef}
                    />
                )}
            </div>
            <select value={themeName} onChange={e => setThemeName(e.target.value)}
                style={compactSelectStyle}>
                {[...THEME_ORDER, ...Object.keys(themes).filter(t => !THEME_ORDER.includes(t))]
                    .map(t => <option key={t} value={t}>{THEME_LABELS[t] || t.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}</option>)}
            </select>
            <button style={btnSaveView} onClick={onSaveView}><Icons.Save /> Save View</button>
            <button style={btnPrimary} onClick={exportPivot}>
                <Icons.Export/> {(rowCount || 0) > 500000 ? 'Export CSV' : 'Export'}
            </button>
        </>
    );

    const activeSections = [
        openToolbarSections.view ? { id: 'view', label: 'View', controls: viewControls } : null,
        openToolbarSections.format ? { id: 'format', label: 'Format', controls: formatControls } : null,
        openToolbarSections.charts ? { id: 'charts', label: 'Charts', controls: chartControls } : null,
        openToolbarSections.theme ? { id: 'theme', label: 'Theme', controls: themeControls } : null,
    ].filter(Boolean);

    return (
        <div style={{
            ...styles.appBar,
            height: 'auto',
            minHeight: '64px',
            padding: '12px 20px',
            gap: '10px',
            flexDirection: 'column',
            overflowX: 'visible',
            overflowY: 'visible',
            justifyContent: 'flex-start',
            alignContent: 'stretch',
            alignItems: 'stretch'
        }}>
            <div style={firstLineStyle}>
                <button 
                    onClick={() => setSidebarOpen(!sidebarOpen)} 
                    style={{...btnGhost, padding:'6px 10px', color: theme.text, display: 'flex', alignItems: 'center', gap: '8px'}}
                >
                    <Icons.Menu />
                    <span style={{fontSize: '13px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em'}}>Menu</span>
                </button>
                <div style={{fontWeight:800, fontSize:'15px', color:theme.text, whiteSpace:'nowrap', opacity: 0.92, minWidth: 0}}>
                    {pivotTitle || ''}
                </div>
                <div style={innerDividerStyle} />
                <div style={{display:'flex', alignItems:'center', flex:'1 1 250px', minWidth:'220px', maxWidth:'360px'}}>
                    <div style={{
                        ...styles.searchBox,
                        width:'100%',
                        borderRadius:theme.radiusSm || '10px',
                        border:`1px solid ${theme.border}`,
                        height: '36px',
                        padding: '0 12px'
                    }}>
                        <Icons.Search />
                        <input
                            style={{border:'none',background:'transparent',marginLeft:'8px',outline:'none',width:'100%', color: theme.text, fontSize:'13px'}}
                            placeholder="Search records..."
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
                </div>
                <div style={innerDividerStyle} />
                <button style={sectionToggleButtonStyle(openToolbarSections.view)} onClick={() => toggleToolbarSection('view')}>View</button>
                <button style={sectionToggleButtonStyle(openToolbarSections.format)} onClick={() => toggleToolbarSection('format')}>Format</button>
                <button style={sectionToggleButtonStyle(openToolbarSections.charts)} onClick={() => toggleToolbarSection('charts')}>Charts</button>
                <button style={sectionToggleButtonStyle(openToolbarSections.theme)} onClick={() => toggleToolbarSection('theme')}>Theme</button>
            </div>

            {activeSections.length > 0 ? (
                <div style={secondLineStyle}>
                    {activeSections.map((section, index) => (
                        <div key={section.id} style={sectionBlockStyle(index === activeSections.length - 1)}>
                            <span style={sectionLabelStyle}>{section.label}</span>
                            {section.controls}
                        </div>
                    ))}
                </div>
            ) : null}
        </div>
    );
}

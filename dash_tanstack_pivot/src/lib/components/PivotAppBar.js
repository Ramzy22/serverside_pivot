import React, { useState, useRef, useEffect, useMemo, useLayoutEffect } from 'react';
import Icons from '../utils/Icons';
import { themes, buildEditedCellVisualStyle, colorToInputHex } from '../utils/styles';
import { DEFAULT_CURRENCY_SYMBOL, formatValue, normalizeNumberGroupSeparator } from '../utils/helpers';
import { EDITED_CELL_FORMAT_KEY } from '../utils/formatting';
import { usePivotTheme } from '../contexts/PivotThemeContext';
import { usePivotConfig } from '../contexts/PivotConfigContext';
import { usePivotRenderCounter } from '../hooks/usePivotRenderCounter';

const SHOW_PIVOT_MODE_TOGGLE = true;

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
const NUMBER_GROUP_SEPARATOR_OPTIONS = [
    { value: 'comma', label: 'Comma' },
    { value: 'space', label: 'Space' },
    { value: 'thin_space', label: 'Thin Space' },
    { value: 'apostrophe', label: 'Apostrophe' },
    { value: 'none', label: 'None' },
];
const NUMBER_FORMAT_CATEGORY_OPTIONS = [
    { value: 'number', label: 'Number' },
    { value: 'currency', label: 'Currency' },
    { value: 'accounting', label: 'Accounting' },
    { value: 'percentage', label: 'Percentage' },
    { value: 'scientific', label: 'Scientific' },
];
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
const ZOOM_MIN = 40;
const ZOOM_MAX = 200;
const ZOOM_STEP = 10;

const getNumberFormatCategory = (formatValue) => {
    const normalized = typeof formatValue === 'string' ? formatValue.trim() : '';
    if (normalized === 'currency' || normalized.startsWith('currency:')) return 'currency';
    if (normalized === 'accounting' || normalized.startsWith('accounting:')) return 'accounting';
    if (normalized === 'percent') return 'percentage';
    if (normalized === 'scientific') return 'scientific';
    return 'number';
};

const getCurrencySymbolFromFormat = (formatValue) => {
    const normalized = typeof formatValue === 'string' ? formatValue.trim() : '';
    if (normalized.startsWith('currency:')) return normalized.slice('currency:'.length);
    if (normalized.startsWith('accounting:')) return normalized.slice('accounting:'.length);
    if (normalized === 'currency' || normalized === 'accounting') return DEFAULT_CURRENCY_SYMBOL;
    return DEFAULT_CURRENCY_SYMBOL;
};

const getFormatValueForCategory = (category, currencySymbol = DEFAULT_CURRENCY_SYMBOL) => {
    const safeSymbol = typeof currencySymbol === 'string' && currencySymbol.length > 0
        ? currencySymbol
        : DEFAULT_CURRENCY_SYMBOL;
    switch (category) {
    case 'currency':
        return `currency:${safeSymbol}`;
    case 'accounting':
        return `accounting:${safeSymbol}`;
    case 'percentage':
        return 'percent';
    case 'scientific':
        return 'scientific';
    default:
        return '';
    }
};

function NumberFormatDialog({
    open,
    theme,
    styles,
    onClose,
    onApply,
    initialCategory,
    initialDecimalPlaces,
    initialGroupSeparator,
    initialCurrencySymbol,
    hasSelection,
    canApplyCategory,
}) {
    const [category, setCategory] = useState(initialCategory);
    const [localDecimals, setLocalDecimals] = useState(initialDecimalPlaces);
    const [useThousandsSeparator, setUseThousandsSeparator] = useState(initialGroupSeparator !== 'none');
    const [separatorStyle, setSeparatorStyle] = useState(initialGroupSeparator === 'none' ? 'comma' : initialGroupSeparator);
    const [currencySymbol, setCurrencySymbol] = useState(initialCurrencySymbol || DEFAULT_CURRENCY_SYMBOL);

    useEffect(() => {
        if (!open) return;
        setCategory(initialCategory);
        setLocalDecimals(initialDecimalPlaces);
        setUseThousandsSeparator(initialGroupSeparator !== 'none');
        setSeparatorStyle(initialGroupSeparator === 'none' ? 'comma' : initialGroupSeparator);
        setCurrencySymbol(initialCurrencySymbol || DEFAULT_CURRENCY_SYMBOL);
    }, [open, initialCategory, initialDecimalPlaces, initialGroupSeparator, initialCurrencySymbol]);

    useEffect(() => {
        if (!open) return undefined;
        const handleEscape = (event) => {
            if (event.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', handleEscape);
        return () => document.removeEventListener('keydown', handleEscape);
    }, [open, onClose]);

    if (!open) return null;

    const activeSeparator = useThousandsSeparator ? separatorStyle : 'none';
    const previewFormat = getFormatValueForCategory(category, currencySymbol) || null;
    const previewSeed = category === 'percentage' ? 0.12345 : 1234567.891;
    const previewPositive = formatValue(previewSeed, previewFormat, localDecimals, activeSeparator);
    const previewNegative = formatValue(-previewSeed, previewFormat, localDecimals, activeSeparator);
    const scopeText = hasSelection
        ? 'Category format will apply only to the selected columns.'
        : 'Category format will become the default for numeric values without an explicit field format.';

    return (
        <div
            role="presentation"
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 10020,
                background: 'rgba(15, 23, 42, 0.34)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '24px',
            }}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-label="Format Cells"
                onClick={(event) => event.stopPropagation()}
                style={{
                    width: 'min(760px, 96vw)',
                    maxHeight: 'min(720px, 90vh)',
                    background: theme.surfaceBg || theme.background || '#fff',
                    border: `1px solid ${theme.border}`,
                    borderRadius: '14px',
                    boxShadow: '0 20px 48px rgba(15, 23, 42, 0.24)',
                    display: 'flex',
                    flexDirection: 'column',
                    overflow: 'hidden',
                }}
            >
                <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '14px 18px',
                    borderBottom: `1px solid ${theme.border}`,
                    background: theme.headerSubtleBg || theme.hover,
                }}>
                    <div>
                        <div style={{ fontSize: '15px', fontWeight: 700, color: theme.text }}>Format Cells</div>
                        <div style={{ fontSize: '11px', color: theme.textSec, marginTop: '2px' }}>Number</div>
                    </div>
                    <button
                        onClick={onClose}
                        style={{ ...styles.btn, minWidth: '36px', padding: '6px', background: 'transparent', border: `1px solid ${theme.border}` }}
                        title="Close"
                    >
                        <Icons.Close />
                    </button>
                </div>

                <div style={{ display: 'flex', minHeight: 0, flex: 1 }}>
                    <div style={{
                        width: '190px',
                        borderRight: `1px solid ${theme.border}`,
                        background: theme.headerSubtleBg || theme.hover,
                        padding: '16px 10px',
                        overflowY: 'auto',
                    }}>
                        <div style={{ fontSize: '11px', fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, padding: '0 8px 10px' }}>
                            Category
                        </div>
                        {NUMBER_FORMAT_CATEGORY_OPTIONS.map((option) => {
                            const active = category === option.value;
                            return (
                                <button
                                    key={option.value}
                                    onClick={() => setCategory(option.value)}
                                    style={{
                                        width: '100%',
                                        textAlign: 'left',
                                        padding: '10px 12px',
                                        borderRadius: '8px',
                                        border: 'none',
                                        background: active ? (theme.select || '#E8F0FE') : 'transparent',
                                        color: active ? (theme.primary || theme.text) : theme.text,
                                        fontSize: '12px',
                                        fontWeight: active ? 700 : 500,
                                        cursor: 'pointer',
                                        opacity: 1,
                                    }}
                                >
                                    {option.label}
                                </button>
                            );
                        })}
                    </div>

                    <div style={{ flex: 1, padding: '18px 20px', overflowY: 'auto' }}>
                        <div style={{ fontSize: '11px', fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, marginBottom: '8px' }}>
                            Preview
                        </div>
                        <div style={{
                            border: `1px solid ${theme.border}`,
                            borderRadius: '10px',
                            padding: '14px 16px',
                            marginBottom: '18px',
                            background: theme.background || '#fff',
                        }}>
                            <div style={{ fontSize: '12px', color: theme.textSec, marginBottom: '8px' }}>{scopeText}</div>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', fontFamily: "'JetBrains Mono', monospace", fontSize: '15px', color: theme.text }}>
                                <span>{previewPositive}</span>
                                <span style={{ color: '#c62828' }}>{previewNegative}</span>
                            </div>
                        </div>

                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '16px' }}>
                            <div>
                                <label style={{ fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '6px' }}>Decimal places</label>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <button
                                        onClick={() => setLocalDecimals((prev) => Math.max(0, prev - 1))}
                                        style={{ ...styles.btn, minWidth: '32px', padding: '6px', border: `1px solid ${theme.border}` }}
                                        disabled={localDecimals <= DECIMAL_MIN}
                                    >
                                        -
                                    </button>
                                    <input
                                        type="number"
                                        min={DECIMAL_MIN}
                                        max={DECIMAL_MAX}
                                        value={localDecimals}
                                        onChange={(event) => {
                                            const nextValue = Number(event.target.value);
                                            if (!Number.isFinite(nextValue)) return;
                                            setLocalDecimals(Math.max(DECIMAL_MIN, Math.min(DECIMAL_MAX, Math.floor(nextValue))));
                                        }}
                                        style={{
                                            width: '72px',
                                            border: `1px solid ${theme.border}`,
                                            borderRadius: '8px',
                                            padding: '8px 10px',
                                            background: theme.background || '#fff',
                                            color: theme.text,
                                        }}
                                    />
                                    <button
                                        onClick={() => setLocalDecimals((prev) => Math.min(DECIMAL_MAX, prev + 1))}
                                        style={{ ...styles.btn, minWidth: '32px', padding: '6px', border: `1px solid ${theme.border}` }}
                                        disabled={localDecimals >= DECIMAL_MAX}
                                    >
                                        +
                                    </button>
                                </div>
                            </div>

                            <div>
                                <label style={{ fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '6px' }}>Separator style</label>
                                <select
                                    value={separatorStyle}
                                    onChange={(event) => setSeparatorStyle(normalizeNumberGroupSeparator(event.target.value))}
                                    disabled={!useThousandsSeparator}
                                    style={{
                                        width: '100%',
                                        border: `1px solid ${theme.border}`,
                                        borderRadius: '8px',
                                        padding: '8px 10px',
                                        background: useThousandsSeparator ? (theme.background || '#fff') : (theme.headerSubtleBg || theme.hover),
                                        color: theme.text,
                                        cursor: useThousandsSeparator ? 'pointer' : 'not-allowed',
                                        opacity: useThousandsSeparator ? 1 : 0.55,
                                    }}
                                >
                                    {NUMBER_GROUP_SEPARATOR_OPTIONS.filter((option) => option.value !== 'none').map((option) => (
                                        <option key={option.value} value={option.value}>{option.label}</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        {(category === 'currency' || category === 'accounting') && (
                            <div style={{ marginTop: '16px', maxWidth: '220px' }}>
                                <label style={{ fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '6px' }}>Currency symbol</label>
                                <input
                                    type="text"
                                    value={currencySymbol}
                                    onChange={(event) => setCurrencySymbol(event.target.value)}
                                    placeholder="$"
                                    style={{
                                        width: '100%',
                                        border: `1px solid ${theme.border}`,
                                        borderRadius: '8px',
                                        padding: '8px 10px',
                                        background: theme.background || '#fff',
                                        color: theme.text,
                                    }}
                                />
                            </div>
                        )}

                        <label style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '10px',
                            marginTop: '18px',
                            fontSize: '12px',
                            color: theme.text,
                        }}>
                            <input
                                type="checkbox"
                                checked={useThousandsSeparator}
                                onChange={(event) => setUseThousandsSeparator(event.target.checked)}
                            />
                            Use 1000 separator
                        </label>
                    </div>
                </div>

                <div style={{
                    display: 'flex',
                    justifyContent: 'flex-end',
                    gap: '10px',
                    padding: '14px 18px',
                    borderTop: `1px solid ${theme.border}`,
                    background: theme.headerSubtleBg || theme.hover,
                }}>
                    <button
                        onClick={onClose}
                        style={{ ...styles.btn, border: `1px solid ${theme.border}`, background: theme.background || '#fff' }}
                    >
                        Cancel
                    </button>
                    <button
                        onClick={() => onApply({
                            category,
                            decimalPlaces: localDecimals,
                            groupSeparator: activeSeparator,
                            currencySymbol,
                        })}
                        style={{ ...styles.btn, border: `1px solid ${theme.primary}`, background: theme.primary, color: '#fff' }}
                    >
                        OK
                    </button>
                </div>
            </div>
        </div>
    );
}

function CellFormatPopover({ theme, styles, cellFormatRules, setCellFormatRules, selectedCellKeys, onClose, anchorRef }) {
    const firstKey = selectedCellKeys && selectedCellKeys.length > 0 ? selectedCellKeys[0] : null;
    const current = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
    const editedCurrent = (cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY]) || {};
    const [bg, setBg] = useState(current.bg || '#ffffff');
    const [color, setColor] = useState(current.color || '#000000');
    const [bold, setBold] = useState(current.bold || false);
    const [italic, setItalic] = useState(current.italic || false);
    const [editedBg, setEditedBg] = useState(editedCurrent.bg || theme.editedCellBg || '#E8F1FF');
    const [editedColor, setEditedColor] = useState(editedCurrent.color || theme.editedCellText || theme.text || '#111827');
    const [editedBold, setEditedBold] = useState(editedCurrent.bold || false);
    const [editedItalic, setEditedItalic] = useState(editedCurrent.italic || false);
    const popoverRef = useRef(null);

    // Sync local state when selection changes
    useEffect(() => {
        const rule = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
        setBg(rule.bg || '#ffffff');
        setColor(rule.color || '#000000');
        setBold(rule.bold || false);
        setItalic(rule.italic || false);
    }, [firstKey, cellFormatRules]);

    useEffect(() => {
        const rule = (cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY]) || {};
        setEditedBg(rule.bg || theme.editedCellBg || '#E8F1FF');
        setEditedColor(rule.color || theme.editedCellText || theme.text || '#111827');
        setEditedBold(rule.bold || false);
        setEditedItalic(rule.italic || false);
    }, [cellFormatRules, theme.editedCellBg, theme.editedCellText, theme.text]);

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

    const applyEditedStyle = () => {
        setCellFormatRules((prev) => ({
            ...prev,
            [EDITED_CELL_FORMAT_KEY]: {
                bg: editedBg,
                color: editedColor,
                bold: editedBold,
                italic: editedItalic,
            },
        }));
    };

    const clearEditedStyle = () => {
        setCellFormatRules((prev) => {
            const next = { ...prev };
            delete next[EDITED_CELL_FORMAT_KEY];
            return next;
        });
    };
    const editedPreviewStyle = buildEditedCellVisualStyle(
        theme,
        { bg: editedBg, color: editedColor, bold: editedBold, italic: editedItalic },
        { direct: true, propagated: false },
        { emphasizeText: true }
    ) || {};

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
                <div style={{fontSize: '12px', color: theme.textSec, marginBottom: '10px'}}>Select cells to format, or tune the edited-cell highlight below.</div>
            ) : (
                <>
                    <div style={{fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '6px'}}>Selection</div>
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
                    <div style={{display: 'flex', gap: '6px', marginBottom: '12px'}}>
                        <button onClick={apply} style={{...styles.btn, background: theme.primary, color: '#fff', flex: 1}}>
                            Apply{cellCount > 1 ? ` (${cellCount})` : ''}
                        </button>
                        <button onClick={clear} style={{...styles.btn, background: theme.hover, flex: 1}}>Clear</button>
                    </div>
                </>
            )}
            <div style={{ borderTop: `1px solid ${theme.border}`, paddingTop: '10px' }}>
                <div style={{fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '4px'}}>Edited Cells</div>
                <div style={{fontSize: '11px', color: theme.textSec, marginBottom: '8px'}}>Used for direct edits and visible aggregate cells affected by the edit.</div>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginBottom: '8px'}}>
                    <div>
                        <label style={{fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '3px'}}>Background</label>
                        <div style={{display: 'flex', gap: '4px', alignItems: 'center'}}>
                            <input type="color" value={editedBg} onChange={e => setEditedBg(e.target.value)}
                                style={{width: '28px', height: '28px', border: 'none', padding: 0, cursor: 'pointer', borderRadius: '3px'}} />
                            <span style={{fontSize: '11px', color: theme.textSec}}>{editedBg}</span>
                        </div>
                    </div>
                    <div>
                        <label style={{fontSize: '11px', color: theme.textSec, display: 'block', marginBottom: '3px'}}>Text Color</label>
                        <div style={{display: 'flex', gap: '4px', alignItems: 'center'}}>
                            <input type="color" value={editedColor} onChange={e => setEditedColor(e.target.value)}
                                style={{width: '28px', height: '28px', border: 'none', padding: 0, cursor: 'pointer', borderRadius: '3px'}} />
                            <span style={{fontSize: '11px', color: theme.textSec}}>{editedColor}</span>
                        </div>
                    </div>
                </div>
                <div style={{display: 'flex', gap: '8px', marginBottom: '10px'}}>
                    <button onClick={() => setEditedBold(b => !b)}
                        style={{...styles.btn, fontWeight: 'bold', background: editedBold ? theme.select : theme.hover, minWidth: '36px'}}
                        title="Bold edited cells">B</button>
                    <button onClick={() => setEditedItalic(i => !i)}
                        style={{...styles.btn, fontStyle: 'italic', background: editedItalic ? theme.select : theme.hover, minWidth: '36px'}}
                        title="Italic edited cells"><em>I</em></button>
                </div>
                <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'flex-end',
                    padding: '7px 10px',
                    borderRadius: '8px',
                    marginBottom: '10px',
                    minHeight: '36px',
                    ...editedPreviewStyle,
                }}>
                    12,480
                </div>
                <div style={{display: 'flex', gap: '6px'}}>
                    <button onClick={applyEditedStyle} style={{...styles.btn, background: theme.primary, color: '#fff', flex: 1}}>
                        Save Edited Style
                    </button>
                    <button onClick={clearEditedStyle} style={{...styles.btn, background: theme.hover, flex: 1}}>Use Theme Default</button>
                </div>
            </div>
        </div>
    );
}

const THEME_CORE_COLOR_FIELDS = [
    { key: 'primary', label: 'Primary' },
    { key: 'pageBg', label: 'Page Bg' },
    { key: 'surfaceBg', label: 'Surface Bg' },
    { key: 'surfaceInset', label: 'Inset Bg' },
    { key: 'headerBg', label: 'Header Bg' },
    { key: 'headerSubtleBg', label: 'Soft Header' },
    { key: 'border', label: 'Border' },
    { key: 'text', label: 'Text' },
    { key: 'textSec', label: 'Muted Text' },
];

const THEME_STRUCTURE_COLOR_FIELDS = [
    { key: 'hierarchyBg', label: 'Hierarchy Bg' },
    { key: 'totalBg', label: 'Total Bg' },
    { key: 'totalBgStrong', label: 'Grand Total Bg' },
    { key: 'totalText', label: 'Total Text' },
    { key: 'totalTextStrong', label: 'Grand Total Text' },
];

const THEME_EDITED_COLOR_FIELDS = [
    { key: 'editedCellBg', label: 'Edited Background' },
    { key: 'editedCellBorder', label: 'Edited Border' },
    { key: 'editedCellText', label: 'Edited Text' },
];

function ThemeEditorPopover({ theme, themeName, themeOverrides, setThemeOverrides, onClose, anchorRef }) {
    const popoverRef = useRef(null);
    const [popoverLayout, setPopoverLayout] = useState({ top: 12, left: 12, width: 440, maxHeight: 560 });
    const editedPreviewStyle = buildEditedCellVisualStyle(
        theme,
        null,
        { direct: true, propagated: false },
        { emphasizeText: true }
    ) || {};

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

    useLayoutEffect(() => {
        if (typeof window === 'undefined') return undefined;

        const updateLayout = () => {
            const viewportWidth = window.innerWidth || 1280;
            const viewportHeight = window.innerHeight || 720;
            const availableWidth = Math.max(280, viewportWidth - 24);
            const width = Math.min(460, availableWidth);
            const preferredHeight = 560;
            const anchorRect = anchorRef.current && typeof anchorRef.current.getBoundingClientRect === 'function'
                ? anchorRef.current.getBoundingClientRect()
                : null;
            const preferredLeft = anchorRect ? (anchorRect.right - width) : (viewportWidth - width - 12);
            const left = Math.min(
                Math.max(12, preferredLeft),
                Math.max(12, viewportWidth - width - 12)
            );
            const belowTop = anchorRect ? (anchorRect.bottom + 8) : 12;
            const aboveTop = anchorRect ? Math.max(12, anchorRect.top - preferredHeight - 8) : 12;
            const shouldOpenAbove = Boolean(
                anchorRect
                && (belowTop + preferredHeight > viewportHeight - 12)
                && anchorRect.top > (viewportHeight - anchorRect.bottom)
            );
            const top = shouldOpenAbove ? aboveTop : Math.max(12, belowTop);
            const maxHeight = Math.max(240, viewportHeight - top - 12);
            setPopoverLayout({ top, left, width, maxHeight });
        };

        updateLayout();
        window.addEventListener('resize', updateLayout);
        window.addEventListener('scroll', updateLayout, true);
        return () => {
            window.removeEventListener('resize', updateLayout);
            window.removeEventListener('scroll', updateLayout, true);
        };
    }, [anchorRef]);

    const resetField = (key) => {
        setThemeOverrides(prev => {
            const next = { ...prev };
            delete next[key];
            return next;
        });
    };

    const clearAll = () => setThemeOverrides({});
    const controlGridColumns = popoverLayout.width < 420 ? '1fr' : 'repeat(2, minmax(0, 1fr))';

    const renderColorField = ({ key, label }) => (
        <div key={key} style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '6px',
            padding: '8px',
            borderRadius: '10px',
            border: `1px solid ${theme.border}`,
            background: theme.headerSubtleBg || theme.hover,
        }}>
            <label style={{ fontSize: '11px', color: theme.textSec, fontWeight: 600 }}>{label}</label>
            <div style={{ display: 'grid', gridTemplateColumns: '28px minmax(0, 1fr) auto', alignItems: 'center', gap: '8px' }}>
                <input
                    type="color"
                    value={colorToInputHex(theme[key], '#000000')}
                    onChange={(e) => setThemeOverrides(prev => ({ ...prev, [key]: e.target.value }))}
                    style={{ width: '28px', height: '28px', border: 'none', padding: 0, background: 'transparent', cursor: 'pointer' }}
                />
                <span style={{
                    fontSize: '11px',
                    color: theme.text,
                    fontFamily: 'monospace',
                    whiteSpace: 'nowrap',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                }}>
                    {theme[key] || '#000000'}
                </span>
                <button onClick={() => resetField(key)} style={{
                    border: `1px solid ${theme.border}`,
                    background: theme.surfaceBg || '#fff',
                    color: theme.textSec,
                    borderRadius: '8px',
                    fontSize: '10px',
                    padding: '5px 7px',
                    cursor: 'pointer'
                }}>
                    Reset
                </button>
            </div>
        </div>
    );

    return (
        <div ref={popoverRef} style={{
            position: 'fixed',
            top: `${popoverLayout.top}px`,
            left: `${popoverLayout.left}px`,
            zIndex: 9999,
            background: theme.surfaceBg || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: theme.radiusSm || '10px',
            boxShadow: theme.shadowMd || '0 12px 28px rgba(0,0,0,0.12)',
            padding: '12px',
            width: `${popoverLayout.width}px`,
            maxWidth: 'calc(100vw - 24px)',
            maxHeight: `${popoverLayout.maxHeight}px`,
            overflowY: 'auto',
            boxSizing: 'border-box',
        }}>
            <div style={{ fontWeight: 700, fontSize: '13px', color: theme.text, marginBottom: '4px' }}>Theme Colors</div>
            <div style={{ fontSize: '11px', color: theme.textSec, marginBottom: '10px' }}>{themeName} with saved overrides</div>
            <div style={{ fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '8px' }}>
                Core Colors
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: controlGridColumns, gap: '8px' }}>
                {THEME_CORE_COLOR_FIELDS.map(renderColorField)}
            </div>
            <div style={{
                marginTop: '12px',
                paddingTop: '12px',
                borderTop: `1px solid ${theme.border}`,
            }}>
                <div style={{ fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '8px' }}>
                    Structure
                </div>
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: controlGridColumns,
                    gap: '8px',
                    marginBottom: '10px',
                }}>
                    <div style={{
                        borderRadius: '10px',
                        border: `1px solid ${theme.border}`,
                        overflow: 'hidden',
                        background: theme.surfaceBg || theme.background || '#fff',
                    }}>
                        <div style={{
                            padding: '8px 10px',
                            background: theme.headerBg,
                            color: theme.text,
                            borderBottom: `1px solid ${theme.border}`,
                            fontSize: '11px',
                            fontWeight: 700,
                        }}>
                            Column Header
                        </div>
                        <div style={{
                            padding: '8px 10px',
                            background: theme.hierarchyBg,
                            color: theme.text,
                            fontSize: '11px',
                        }}>
                            Hierarchy Column
                        </div>
                    </div>
                    <div style={{
                        borderRadius: '10px',
                        border: `1px solid ${theme.border}`,
                        overflow: 'hidden',
                        background: theme.surfaceBg || theme.background || '#fff',
                    }}>
                        <div style={{
                            padding: '8px 10px',
                            background: theme.totalBg,
                            color: theme.totalText,
                            borderBottom: `1px solid ${theme.border}`,
                            fontSize: '11px',
                            fontWeight: 600,
                        }}>
                            Total
                        </div>
                        <div style={{
                            padding: '8px 10px',
                            background: theme.totalBgStrong,
                            color: theme.totalTextStrong,
                            fontSize: '11px',
                            fontWeight: 700,
                        }}>
                            Grand Total
                        </div>
                    </div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: controlGridColumns, gap: '8px' }}>
                    {THEME_STRUCTURE_COLOR_FIELDS.map(renderColorField)}
                </div>
            </div>
            <div style={{
                marginTop: '12px',
                paddingTop: '12px',
                borderTop: `1px solid ${theme.border}`,
            }}>
                <div style={{ fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '8px' }}>
                    Edited Cells
                </div>
                <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: '12px',
                    padding: '10px 12px',
                    marginBottom: '10px',
                    borderRadius: '10px',
                    ...editedPreviewStyle,
                }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
                        <div style={{ fontSize: '11px', fontWeight: 700 }}>Edited Cell Preview</div>
                        <div style={{ fontSize: '10px', color: theme.editedCellText || theme.textSec }}>Background, border, and text shown together</div>
                    </div>
                    <div style={{ fontWeight: 700 }}>12,480</div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: controlGridColumns, gap: '8px' }}>
                    {THEME_EDITED_COLOR_FIELDS.map(renderColorField)}
                </div>
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
                <button onClick={clearAll} style={{ border: `1px solid ${theme.border}`, background: theme.headerSubtleBg || theme.hover, color: theme.text, borderRadius: '8px', padding: '7px 10px', cursor: 'pointer', flex: 1 }}>Clear Overrides</button>
                <button onClick={onClose} style={{ border: `1px solid ${theme.primary}`, background: theme.primary, color: '#fff', borderRadius: '8px', padding: '7px 10px', cursor: 'pointer', flex: 1 }}>Done</button>
            </div>
        </div>
    );
}

export const PivotAppBar = React.memo(function PivotAppBar({
    immersiveMode, setImmersiveMode,
    sidebarOpen, setSidebarOpen,
    themeName, setThemeName,
    themeOverrides, setThemeOverrides,
    spacingMode, setSpacingMode, spacingLabels,
    layoutMode, setLayoutMode,
    onAutoSizeColumns,
    autoSizeIncludesHeaderNext,
    colorScaleMode, setColorScaleMode,
    colorPalette, setColorPalette,
    rowCount, exportPivot, isExporting,
    onTransposePivot,
    canTranspose,
    onSaveView,
    pivotTitle,
    fontFamily, setFontFamily,
    fontSize, setFontSize,
    zoomLevel, setZoomLevel,
    decimalPlaces, setDecimalPlaces,
    defaultValueFormat, setDefaultValueFormat,
    columnDecimalOverrides, setColumnDecimalOverrides,
    columnFormatOverrides, setColumnFormatOverrides,
    columnGroupSeparatorOverrides, setColumnGroupSeparatorOverrides,
    cellFormatRules, setCellFormatRules,
    selectedCells,
    selectionValueFormat,
    canApplySelectionValueFormat,
    onApplySelectionValueFormat,
    dataBarsColumns, setDataBarsColumns,
    canUndoTransactions,
    canRedoTransactions,
    transactionHistoryPending,
    onUndoTransaction,
    onRedoTransaction,
    editValueDisplayMode,
    setEditValueDisplayMode,
    hasComparedValues,
    canCreateSelectionChart,
    onCreateSelectionChart,
    onAddChartPane,
    uiConfig = {},
}) {
    usePivotRenderCounter('PivotAppBar');
    const { theme, styles } = usePivotTheme();
    const {
        filters, setFilters,
        pivotMode, setPivotMode,
        reportDef, setReportDef,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        showColTotals, setShowColTotals,
        showRowTotals, setShowRowTotals,
        showSubtotals, setShowSubtotals,
        showRowNumbers, setShowRowNumbers,
        numberGroupSeparator, setNumberGroupSeparator,
    } = usePivotConfig();
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
    const activeSelectionColumnIds = useMemo(() => Array.from(selectedCellColIds || []), [selectedCellColIds]);
    const displayedNumberGroupSeparator = useMemo(() => {
        const fallback = normalizeNumberGroupSeparator(numberGroupSeparator);
        if (activeSelectionColumnIds.length === 0) return fallback;
        const resolved = activeSelectionColumnIds.map((colId) => normalizeNumberGroupSeparator(
            columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[colId] !== undefined
                ? columnGroupSeparatorOverrides[colId]
                : fallback
        ));
        return new Set(resolved).size === 1 ? resolved[0] : fallback;
    }, [activeSelectionColumnIds, columnGroupSeparatorOverrides, numberGroupSeparator]);
    const displayedSelectionFormat = useMemo(() => {
        if (activeSelectionColumnIds.length === 0) return defaultValueFormat;
        const fallback = selectionValueFormat || defaultValueFormat || '';
        const resolved = activeSelectionColumnIds.map((colId) => (
            columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, colId)
                ? columnFormatOverrides[colId]
                : fallback
        ));
        return new Set(resolved).size === 1 ? resolved[0] : fallback;
    }, [activeSelectionColumnIds, columnFormatOverrides, selectionValueFormat, defaultValueFormat]);
    const activeFormatCategory = useMemo(() => (
        getNumberFormatCategory(hasSelection ? displayedSelectionFormat : defaultValueFormat)
    ), [hasSelection, displayedSelectionFormat, defaultValueFormat]);
    const activeCurrencySymbol = useMemo(() => (
        getCurrencySymbolFromFormat(hasSelection ? displayedSelectionFormat : defaultValueFormat)
    ), [hasSelection, displayedSelectionFormat, defaultValueFormat]);
    const [cellFormatOpen, setCellFormatOpen] = useState(false);
    const cellFormatBtnRef = useRef(null);
    const [numberFormatDialogOpen, setNumberFormatDialogOpen] = useState(false);
    const [themeEditorOpen, setThemeEditorOpen] = useState(false);
    const themeEditorBtnRef = useRef(null);
    const [openToolbarSections, setOpenToolbarSections] = useState({
        view: false,
        format: true,
        charts: true,
        theme: false,
    });
    const hasSavedCellFormats = Object.keys(cellFormatRules || {}).length > 0;
    const handleZoomChange = (delta) => {
        setZoomLevel(prev => Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, prev + delta)));
    };
    const handleSetDecimalPlacesValue = (nextValue) => {
        const normalized = Math.max(DECIMAL_MIN, Math.min(DECIMAL_MAX, Math.floor(Number(nextValue) || 0)));
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) {
            setDecimalPlaces(normalized);
            return;
        }
        const colIds = [...new Set(keys.map(k => {
            const idx = k.indexOf(':');
            return idx >= 0 ? k.substring(idx + 1) : k;
        }))];
        setColumnDecimalOverrides(prev => {
            const next = { ...prev };
            colIds.forEach(colId => {
                next[colId] = normalized;
            });
            return next;
        });
    };
    const handleSetGroupSeparatorValue = (nextSeparator) => {
        const normalized = normalizeNumberGroupSeparator(nextSeparator);
        if (activeSelectionColumnIds.length === 0 || typeof setColumnGroupSeparatorOverrides !== 'function') {
            setNumberGroupSeparator(normalized);
            return;
        }
        setColumnGroupSeparatorOverrides(prev => {
            const next = { ...(prev || {}) };
            activeSelectionColumnIds.forEach((colId) => {
                if (normalized === normalizeNumberGroupSeparator(numberGroupSeparator)) {
                    delete next[colId];
                } else {
                    next[colId] = normalized;
                }
            });
            return next;
        });
    };
    const handleSetColumnFormatValue = (nextFormat) => {
        if (activeSelectionColumnIds.length === 0 || typeof setColumnFormatOverrides !== 'function') {
            setDefaultValueFormat(nextFormat);
            return;
        }
        setColumnFormatOverrides((prev) => {
            const next = { ...(prev || {}) };
            activeSelectionColumnIds.forEach((colId) => {
                next[colId] = nextFormat;
            });
            return next;
        });
    };
    const handleApplyNumberFormatDialog = ({ category, decimalPlaces: nextDecimals, groupSeparator, currencySymbol }) => {
        handleSetDecimalPlacesValue(nextDecimals);
        handleSetGroupSeparatorValue(groupSeparator);
        const nextFormat = getFormatValueForCategory(category, currencySymbol);
        handleSetColumnFormatValue(nextFormat);
        setNumberFormatDialogOpen(false);
    };
    const canApplyNumberFormatCategory = true;
    const activeFormatCategoryLabel = useMemo(() => {
        const match = NUMBER_FORMAT_CATEGORY_OPTIONS.find((option) => option.value === activeFormatCategory);
        return match ? match.label : 'Number';
    }, [activeFormatCategory]);
    const displayedSeparatorLabel = useMemo(() => {
        const match = NUMBER_GROUP_SEPARATOR_OPTIONS.find((option) => option.value === displayedNumberGroupSeparator);
        return match ? match.label : 'Comma';
    }, [displayedNumberGroupSeparator]);
    const hasNumberFormatCustomization = (
        Boolean(defaultValueFormat) ||
        decimalPlaces !== 0 ||
        normalizeNumberGroupSeparator(numberGroupSeparator) !== 'comma' ||
        Object.keys(columnDecimalOverrides || {}).length > 0 ||
        Object.keys(columnFormatOverrides || {}).length > 0 ||
        Object.keys(columnGroupSeparatorOverrides || {}).length > 0
    );
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
    const valueModeFrameStyle = {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        padding: '3px',
        borderRadius: theme.radiusSm || '10px',
        background: theme.headerSubtleBg || theme.hover,
        border: `1px solid ${theme.border}`,
        boxShadow: theme.shadowInset || 'none',
    };
    const valueModeButtonStyle = (mode, disabled = false) => ({
        ...btnBase,
        minHeight: '30px',
        padding: '4px 10px',
        borderRadius: '8px',
        background: editValueDisplayMode === mode ? theme.select : 'transparent',
        border: `1px solid ${editValueDisplayMode === mode ? theme.primary : 'transparent'}`,
        color: editValueDisplayMode === mode ? theme.primary : theme.textSec,
        opacity: disabled ? 0.45 : 1,
        cursor: disabled ? 'not-allowed' : 'pointer',
        boxShadow: 'none',
    });
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
        width: '1.5px',
        alignSelf: 'stretch',
        minHeight: '22px',
        margin: '7px 2px',
        background: theme.isDark ? 'rgba(255,255,255,0.28)' : 'rgba(0,0,0,0.28)',
        flexShrink: 0,
        borderRadius: '1px',
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
    const themeSelectStyle = {
        ...compactSelectStyle,
        background: theme.themeSelectorBg || theme.headerSubtleBg || theme.hover,
        color: theme.themeSelectorText || theme.text,
    };
    const sectionToggleButtonStyle = (active) => (active ? btnActive : btnSubtle);

    const viewControls = (
        <>
            <button style={showRowNumbers ? btnActive : btnSubtle} onClick={() => setShowRowNumbers(!showRowNumbers)} title="Toggle row numbers">Row #</button>
            <button style={stickyHeaders ? btnActive : btnSubtle} onClick={() => setStickyHeaders(!stickyHeaders)}>Sticky Header</button>
            {uiConfig.showFilters !== false && <button style={showFloatingFilters ? btnActive : btnSubtle} onClick={() => setShowFloatingFilters(!showFloatingFilters)}>Filters</button>}
            <button style={showRowTotals ? btnActive : btnSubtle} onClick={() => setShowRowTotals(!showRowTotals)}>Row Total</button>
            <button style={showColTotals ? btnActive : btnSubtle} onClick={() => setShowColTotals(!showColTotals)}>Col Total</button>
            <button style={showSubtotals ? btnActive : btnSubtle} onClick={() => setShowSubtotals(prev => !prev)} title="Toggle hierarchy subtotal rows">Subtotals</button>
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
            <button
                style={btnSubtle}
                onClick={onAutoSizeColumns}
                title="Auto-size all visible columns to fit their content"
            >
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
            <button
                style={numberFormatDialogOpen || hasSelection || hasNumberFormatCustomization ? {
                    ...btnActive,
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '8px',
                    minWidth: '220px',
                    justifyContent: 'space-between',
                } : {
                    ...btnSubtle,
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '8px',
                    minWidth: '220px',
                    justifyContent: 'space-between',
                }}
                onClick={() => setNumberFormatDialogOpen(true)}
                title="Open number format options"
            >
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                    <span style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: '28px',
                        height: '28px',
                        borderRadius: '8px',
                        background: theme.background || '#fff',
                        border: `1px solid ${theme.border}`,
                        flexShrink: 0,
                    }}>
                        <Icons.NumberFormat />
                    </span>
                    <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', minWidth: 0 }}>
                        <span style={{ fontSize: '12px', fontWeight: 700, color: theme.text, lineHeight: 1.1 }}>Number Format</span>
                        <span style={{ fontSize: '10px', color: theme.textSec, lineHeight: 1.2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '138px' }}>
                            {`${activeFormatCategoryLabel} · ${displayDecimal} dp · ${displayedSeparatorLabel}`}
                        </span>
                    </span>
                </span>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', flexShrink: 0 }}>
                    <Icons.ChevronDown />
                </span>
            </button>
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
                    data-add-chart-pane="true"
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
                style={themeSelectStyle}>
                {[...THEME_ORDER, ...Object.keys(themes).filter(t => !THEME_ORDER.includes(t))]
                    .map(t => (
                        <option
                            key={t}
                            value={t}
                            style={{
                                background: theme.themeSelectorMenuBg || theme.surfaceBg || theme.background,
                                color: theme.themeSelectorMenuText || theme.text,
                            }}
                        >
                            {THEME_LABELS[t] || t.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                        </option>
                    ))}
            </select>
            <button style={btnSaveView} onClick={onSaveView}><Icons.Save /> Save View</button>
        </>
    );

    const activeSections = [
        openToolbarSections.view ? { id: 'view', label: 'View', controls: viewControls } : null,
        openToolbarSections.format ? { id: 'format', label: 'Format', controls: formatControls } : null,
        openToolbarSections.charts && uiConfig.showCharts !== false ? { id: 'charts', label: 'Charts', controls: chartControls } : null,
        openToolbarSections.theme ? { id: 'theme', label: 'Theme', controls: themeControls } : null,
    ].filter(Boolean);

    return (
        <>
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
                {uiConfig.showSidebar !== false && <button
                    onClick={() => setSidebarOpen(!sidebarOpen)}
                    style={{...btnGhost, padding:'6px 10px', color: theme.text, display: 'flex', alignItems: 'center', gap: '8px'}}
                >
                    <Icons.Menu />
                    <span style={{fontSize: '13px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em'}}>Menu</span>
                </button>}
                <button
                    style={{...(canUndoTransactions && !transactionHistoryPending ? btnGhost : { ...btnGhost, opacity: 0.35, cursor: 'not-allowed' }), padding:'6px 8px', display:'inline-flex', alignItems:'center'}}
                    onClick={onUndoTransaction}
                    title="Undo the last edit or layout change (Ctrl/Cmd+Z)"
                    disabled={!canUndoTransactions || transactionHistoryPending}
                ><Icons.Undo /></button>
                <button
                    style={{...(canRedoTransactions && !transactionHistoryPending ? btnGhost : { ...btnGhost, opacity: 0.35, cursor: 'not-allowed' }), padding:'6px 8px', display:'inline-flex', alignItems:'center'}}
                    onClick={onRedoTransaction}
                    title="Redo the last edit or layout change (Ctrl+Y or Cmd/Ctrl+Shift+Z)"
                    disabled={!canRedoTransactions || transactionHistoryPending}
                ><Icons.Redo /></button>
                {SHOW_PIVOT_MODE_TOGGLE ? (
                    <div style={{
                        display: 'flex',
                        background: theme.hover,
                        border: `1px solid ${theme.border}`,
                        borderRadius: theme.radiusSm || '10px',
                        padding: '2px',
                        gap: '2px',
                    }}>
                        <button
                            style={{
                                ...btnBase,
                                padding: '5px 12px',
                                minHeight: '30px',
                                fontSize: '11px',
                                background: pivotMode === 'pivot' ? (theme.primary || '#4F46E5') : 'transparent',
                                color: pivotMode === 'pivot' ? '#fff' : theme.text,
                                border: 'none',
                                fontWeight: pivotMode === 'pivot' ? 700 : 500,
                            }}
                            onClick={() => setPivotMode('pivot')}
                            title="Pivot Mode"
                        >
                            <Icons.Columns /> Pivot
                        </button>
                        <button
                            style={{
                                ...btnBase,
                                padding: '5px 12px',
                                minHeight: '30px',
                                fontSize: '11px',
                                background: pivotMode === 'report' ? (theme.primary || '#4F46E5') : 'transparent',
                                color: pivotMode === 'report' ? '#fff' : theme.text,
                                border: 'none',
                                fontWeight: pivotMode === 'report' ? 700 : 500,
                            }}
                            onClick={() => setPivotMode('report')}
                            title="Report Mode"
                        >
                            <Icons.Report /> Report
                        </button>
                    </div>
                ) : null}
                <div style={{fontWeight:800, fontSize:'15px', color:theme.text, whiteSpace:'nowrap', opacity: 0.92, minWidth: 0}}>
                    {pivotTitle || ''}
                </div>
                {/* Report selector dropdown */}
                {pivotMode === 'report' && savedReports && savedReports.length > 0 && (
                    <select
                        value={activeReportId || ''}
                        onChange={e => {
                            const id = e.target.value;
                            if (!id) return;
                            const report = savedReports.find(r => r.id === id);
                            if (report && report.reportDef && setReportDef) {
                                setReportDef(JSON.parse(JSON.stringify(report.reportDef)));
                                setActiveReportId(id);
                            }
                        }}
                        style={{
                            ...btnSubtle,
                            outline: 'none',
                            cursor: 'pointer',
                            fontSize: '12px',
                            padding: '5px 10px',
                            minHeight: '30px',
                            minWidth: '140px',
                            maxWidth: '220px',
                        }}
                        title="Switch saved report"
                    >
                        <option value="">Select report...</option>
                        {savedReports.map(r => (
                            <option key={r.id} value={r.id}>{r.name}</option>
                        ))}
                    </select>
                )}
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
                {uiConfig.showEditing !== false && <>
                <div style={innerDividerStyle} />
                <div style={valueModeFrameStyle} title="Switch between current edited values and the original values captured before this session's active edits">
                    <button
                        type="button"
                        data-edit-value-mode="edited"
                        aria-pressed={editValueDisplayMode === 'edited'}
                        style={valueModeButtonStyle('edited')}
                        onClick={() => setEditValueDisplayMode('edited')}
                    >
                        Edited
                    </button>
                    <button
                        type="button"
                        data-edit-value-mode="original"
                        aria-pressed={editValueDisplayMode === 'original'}
                        style={valueModeButtonStyle('original')}
                        onClick={() => setEditValueDisplayMode('original')}
                    >
                        Original
                    </button>
                </div>
                </>}
                <div style={innerDividerStyle} />
                <button data-toolbar-section-toggle="view" style={sectionToggleButtonStyle(openToolbarSections.view)} onClick={() => toggleToolbarSection('view')}>View</button>
                <button data-toolbar-section-toggle="format" style={sectionToggleButtonStyle(openToolbarSections.format)} onClick={() => toggleToolbarSection('format')}>Format</button>
                {uiConfig.showCharts !== false && <button data-toolbar-section-toggle="charts" style={sectionToggleButtonStyle(openToolbarSections.charts)} onClick={() => toggleToolbarSection('charts')}>Charts</button>}
                <button data-toolbar-section-toggle="theme" style={sectionToggleButtonStyle(openToolbarSections.theme)} onClick={() => toggleToolbarSection('theme')}>Theme</button>
                <div style={innerDividerStyle} />
                <div style={{display:'flex', alignItems:'center', gap:'2px', background: theme.hover, border:`1px solid ${theme.border}`, borderRadius:'6px', padding:'2px 4px'}}>
                    <button
                        title="Zoom out"
                        style={{...btnGhost, padding:'2px 6px', fontSize:'12px', minWidth:'28px', opacity: zoomLevel <= ZOOM_MIN ? 0.35 : 1}}
                        onClick={() => handleZoomChange(-ZOOM_STEP)}
                        disabled={zoomLevel <= ZOOM_MIN}
                    >-</button>
                    <span style={{fontSize:'11px', color: theme.textSec, minWidth:'42px', textAlign:'center', fontWeight: 600}}>{zoomLevel}%</span>
                    <button
                        title="Zoom in"
                        style={{...btnGhost, padding:'2px 6px', fontSize:'12px', minWidth:'28px', opacity: zoomLevel >= ZOOM_MAX ? 0.35 : 1}}
                        onClick={() => handleZoomChange(ZOOM_STEP)}
                        disabled={zoomLevel >= ZOOM_MAX}
                    >+</button>
                </div>
                <div style={innerDividerStyle} />
                {!uiConfig.lockImmersiveMode ? <>
                    <div style={{...innerDividerStyle, marginLeft:'auto'}} />
                    <button
                        style={{...(immersiveMode ? btnActive : btnSubtle), display:'inline-flex', alignItems:'center', gap:'5px'}}
                        onClick={() => setImmersiveMode(!immersiveMode)}
                        title="Immersive Mode — hide all controls and show only the table"
                    >
                        {immersiveMode ? <Icons.FullscreenExit /> : <Icons.Fullscreen />} Immersive
                    </button>
                    <div style={innerDividerStyle} />
                    <button style={btnPrimary} onClick={exportPivot} disabled={isExporting}>
                        <Icons.Export/> {isExporting ? 'Exporting…' : (rowCount || 0) > 500000 ? 'Export CSV' : 'Export'}
                    </button>
                </> : <>
                    <div style={{marginLeft:'auto'}} />
                    <div style={innerDividerStyle} />
                    <button style={btnPrimary} onClick={exportPivot} disabled={isExporting}>
                        <Icons.Export/> {isExporting ? 'Exporting…' : (rowCount || 0) > 500000 ? 'Export CSV' : 'Export'}
                    </button>
                </>}
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
        <NumberFormatDialog
            open={numberFormatDialogOpen}
            theme={theme}
            styles={styles}
            onClose={() => setNumberFormatDialogOpen(false)}
            onApply={handleApplyNumberFormatDialog}
            initialCategory={activeFormatCategory}
            initialDecimalPlaces={displayDecimal}
            initialGroupSeparator={displayedNumberGroupSeparator}
            initialCurrencySymbol={activeCurrencySymbol}
            hasSelection={hasSelection}
            canApplyCategory={canApplyNumberFormatCategory}
        />
        </>
    );
});

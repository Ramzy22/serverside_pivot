import React, { useState, useRef, useEffect, useMemo, useLayoutEffect } from 'react';
import ReactDOM from 'react-dom';
import Icons from '../utils/Icons';
import { themes, buildEditedCellVisualStyle, colorToInputHex } from '../utils/styles';
import { DEFAULT_CURRENCY_SYMBOL, formatValue, normalizeNumberGroupSeparator } from '../utils/helpers';
import { EDITED_CELL_FORMAT_KEY } from '../utils/formatting';
import { usePivotTheme } from '../contexts/PivotThemeContext';
import { usePivotConfig } from '../contexts/PivotConfigContext';
import { usePivotRenderCounter } from '../hooks/usePivotRenderCounter';
import { applySavedReport } from '../utils/savedReports';
import { isEventInsideElement } from '../utils/domEvents';
import { updateDataBarMaximums } from '../utils/dataBars';

const SHOW_PIVOT_MODE_TOGGLE = true;

/** @type {any[]} */
const COLOR_PALETTE_OPTIONS = [
    { value: 'redGreen',   label: 'Red to Green' },
    { value: 'greenRed',   label: 'Green to Red' },
    { value: 'blueWhite',  label: 'Blue to White' },
    { value: 'yellowBlue', label: 'Yellow to Blue' },
    { value: 'orangePurp', label: 'Orange to Purple' },
];

/** @type {any[]} */
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

/** @type {any[]} */
const FONT_SIZE_OPTIONS = ['8px', '9px', '10px', '11px', '12px', '13px', '14px', '15px', '16px', '18px', '20px', '24px'];
/** @type {any[]} */
const NUMBER_GROUP_SEPARATOR_OPTIONS = [
    { value: 'comma', label: 'Comma' },
    { value: 'space', label: 'Space' },
    { value: 'thin_space', label: 'Thin Space' },
    { value: 'apostrophe', label: 'Apostrophe' },
    { value: 'none', label: 'None' },
];
/** @type {any[]} */
const NUMBER_FORMAT_CATEGORY_OPTIONS = [
    { value: 'number', label: 'Number' },
    { value: 'currency', label: 'Currency' },
    { value: 'accounting', label: 'Accounting' },
    { value: 'percentage', label: 'Percentage' },
    { value: 'scientific', label: 'Scientific' },
];
/** @type {any[]} */
const THEME_ORDER = ['flash', 'dark', 'bloomberg_black', 'bloomberg'];
/** @type {Record<string, any>} */
const THEME_LABELS = {
    flash: 'Flash',
    dark: 'Dark',
    bloomberg_black: 'Bloomberg Black',
    bloomberg: 'Bloomberg',
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

/** @param {any} [formatValue] @returns {any} */
const getNumberFormatCategory = (formatValue) => {
    /** @type {any} */
    const normalized = typeof formatValue === 'string' ? formatValue.trim() : '';
    if (normalized === 'currency' || normalized.startsWith('currency:')) return 'currency';
    if (normalized === 'accounting' || normalized.startsWith('accounting:')) return 'accounting';
    if (normalized === 'percent') return 'percentage';
    if (normalized === 'scientific') return 'scientific';
    return 'number';
};

/** @param {any} [formatValue] @returns {any} */
const getCurrencySymbolFromFormat = (formatValue) => {
    /** @type {any} */
    const normalized = typeof formatValue === 'string' ? formatValue.trim() : '';
    if (normalized.startsWith('currency:')) return normalized.slice('currency:'.length);
    if (normalized.startsWith('accounting:')) return normalized.slice('accounting:'.length);
    if (normalized === 'currency' || normalized === 'accounting') return DEFAULT_CURRENCY_SYMBOL;
    return DEFAULT_CURRENCY_SYMBOL;
};

/** @param {any} [category] @param {any} [currencySymbol] @returns {any} */
const getFormatValueForCategory = (category, currencySymbol = DEFAULT_CURRENCY_SYMBOL) => {
    /** @type {any} */
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

/** @param {any} p0 @returns {any} */
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
    /** @type {[any, (...args: any[]) => any]} */
    const [category, setCategory] = useState(initialCategory);
    /** @type {[any, (...args: any[]) => any]} */
    const [localDecimals, setLocalDecimals] = useState(initialDecimalPlaces);
    /** @type {[any, (...args: any[]) => any]} */
    const [useThousandsSeparator, setUseThousandsSeparator] = useState(initialGroupSeparator !== 'none');
    /** @type {[any, (...args: any[]) => any]} */
    const [separatorStyle, setSeparatorStyle] = useState(initialGroupSeparator === 'none' ? 'comma' : initialGroupSeparator);
    /** @type {[any, (...args: any[]) => any]} */
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
        /** @param {any} [event] @returns {any} */
        const handleEscape = (event) => {
            if (event.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', handleEscape);
        return () => document.removeEventListener('keydown', handleEscape);
    }, [open, onClose]);

    if (!open) return null;

    /** @type {any} */
    const activeSeparator = useThousandsSeparator ? separatorStyle : 'none';
    /** @type {any} */
    const previewFormat = getFormatValueForCategory(category, currencySymbol) || null;
    /** @type {any} */
    const previewSeed = category === 'percentage' ? 0.12345 : 1234567.891;
    /** @type {any} */
    const previewPositive = formatValue(previewSeed, previewFormat, localDecimals, activeSeparator);
    /** @type {any} */
    const previewNegative = formatValue(-previewSeed, previewFormat, localDecimals, activeSeparator);
    /** @type {any} */
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
                            /** @type {any} */
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
                                        onClick={() => setLocalDecimals(/** @param {any} [prev] @returns {any} */ (prev) => Math.max(0, prev - 1))}
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
                                            /** @type {any} */
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
                                        onClick={() => setLocalDecimals(/** @param {any} [prev] @returns {any} */ (prev) => Math.min(DECIMAL_MAX, prev + 1))}
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

/** @type {any[]} */
const CELL_BORDER_STYLES = [
    { value: '', label: 'No custom border' },
    { value: 'solid', label: 'Solid' },
    { value: 'dashed', label: 'Dashed' },
    { value: 'dotted', label: 'Dotted' },
    { value: 'double', label: 'Double' },
];

/** @param {any} p0 @returns {any} */
function CellColorField({ label, value, onChange, fallback, theme }) {
    return (
        <label style={{display: 'flex', flexDirection: 'column', gap: '5px', minWidth: 0}}>
            <span style={{fontSize: '11px', color: theme.textSec, fontWeight: 600}}>{label}</span>
            <div style={{display: 'flex', alignItems: 'center', gap: '6px'}}>
                <input
                    type="color"
                    aria-label={`${label} color picker`}
                    value={colorToInputHex(value, fallback)}
                    onChange={(event) => onChange(event.target.value)}
                    style={{width: '34px', height: '30px', border: `1px solid ${theme.border}`, padding: '2px', cursor: 'pointer', borderRadius: '6px', background: theme.background}}
                />
                <input
                    type="text"
                    aria-label={label}
                    value={value}
                    onChange={(event) => onChange(event.target.value)}
                    placeholder={fallback}
                    spellCheck={false}
                    style={{flex: 1, width: 0, minWidth: 0, height: '30px', boxSizing: 'border-box', border: `1px solid ${theme.border}`, borderRadius: '6px', padding: '4px 7px', background: theme.background, color: theme.text, fontFamily: 'monospace', fontSize: '11px'}}
                />
            </div>
        </label>
    );
}

/** @param {any} p0 @returns {any} */
function CellFormatPopover({ theme, styles, cellFormatRules, setCellFormatRules, selectedCellKeys, onClose, anchorRef }) {
    /** @type {any} */
    const firstKey = selectedCellKeys && selectedCellKeys.length > 0 ? selectedCellKeys[0] : null;
    /** @type {any} */
    const current = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
    /** @type {any} */
    const editedCurrent = (cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY]) || {};
    /** @type {[any, (...args: any[]) => any]} */
    const [bg, setBg] = useState(current.bg || '');
    /** @type {[any, (...args: any[]) => any]} */
    const [color, setColor] = useState(current.color || '');
    /** @type {[any, (...args: any[]) => any]} */
    const [bold, setBold] = useState(current.bold || false);
    /** @type {[any, (...args: any[]) => any]} */
    const [italic, setItalic] = useState(current.italic || false);
    /** @type {[any, (...args: any[]) => any]} */
    const [textAlign, setTextAlign] = useState(current.textAlign || '');
    /** @type {[any, (...args: any[]) => any]} */
    const [borderStyle, setBorderStyle] = useState(current.borderStyle || '');
    /** @type {[any, (...args: any[]) => any]} */
    const [borderColor, setBorderColor] = useState(current.borderColor || '');
    /** @type {[any, (...args: any[]) => any]} */
    const [borderWidth, setBorderWidth] = useState(current.borderWidth || 1);
    /** @type {[any, (...args: any[]) => any]} */
    const [editedBg, setEditedBg] = useState(editedCurrent.bg || theme.editedCellBg || '#E8F1FF');
    /** @type {[any, (...args: any[]) => any]} */
    const [editedColor, setEditedColor] = useState(editedCurrent.color || theme.editedCellText || theme.text || '#111827');
    /** @type {[any, (...args: any[]) => any]} */
    const [editedBold, setEditedBold] = useState(editedCurrent.bold || false);
    /** @type {[any, (...args: any[]) => any]} */
    const [editedItalic, setEditedItalic] = useState(editedCurrent.italic || false);
    /** @type {any} */
    const popoverRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [popoverPosition, setPopoverPosition] = useState(null);

    useLayoutEffect(() => {
        /** @returns {any} */
        const updatePosition = () => {
            /** @type {any} */
            const anchor = anchorRef && anchorRef.current;
            if (!anchor || typeof window === 'undefined') return;
            /** @type {any} */
            const rect = anchor.getBoundingClientRect();
            const viewportMargin = 12;
            const gap = 6;
            /** @type {any} */
            const width = Math.min(360, Math.max(280, window.innerWidth - (viewportMargin * 2)));
            /** @type {any} */
            const left = Math.max(
                viewportMargin,
                Math.min(rect.left, window.innerWidth - width - viewportMargin),
            );
            /** @type {any} */
            const spaceBelow = window.innerHeight - rect.bottom - viewportMargin - gap;
            /** @type {any} */
            const spaceAbove = rect.top - viewportMargin - gap;
            /** @type {any} */
            const openAbove = spaceBelow < 360 && spaceAbove > spaceBelow;
            /** @type {any} */
            const maxHeight = Math.max(240, openAbove ? spaceAbove : spaceBelow);
            /** @type {any} */
            const top = openAbove
                ? Math.max(viewportMargin, rect.top - Math.min(640, maxHeight) - gap)
                : rect.bottom + gap;
            setPopoverPosition({ left, top, width, maxHeight: Math.min(640, maxHeight) });
        };

        updatePosition();
        window.addEventListener('resize', updatePosition);
        window.addEventListener('scroll', updatePosition, true);
        return () => {
            window.removeEventListener('resize', updatePosition);
            window.removeEventListener('scroll', updatePosition, true);
        };
    }, [anchorRef]);

    // Sync local state when selection changes
    useEffect(() => {
        /** @type {any} */
        const rule = (firstKey && cellFormatRules && cellFormatRules[firstKey]) || {};
        setBg(rule.bg || '');
        setColor(rule.color || '');
        setBold(rule.bold || false);
        setItalic(rule.italic || false);
        setTextAlign(rule.textAlign || '');
        setBorderStyle(rule.borderStyle || '');
        setBorderColor(rule.borderColor || '');
        setBorderWidth(rule.borderWidth || 1);
    }, [firstKey, cellFormatRules, theme.border]);

    useEffect(() => {
        /** @type {any} */
        const rule = (cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY]) || {};
        setEditedBg(rule.bg || theme.editedCellBg || '#E8F1FF');
        setEditedColor(rule.color || theme.editedCellText || theme.text || '#111827');
        setEditedBold(rule.bold || false);
        setEditedItalic(rule.italic || false);
    }, [cellFormatRules, theme.editedCellBg, theme.editedCellText, theme.text]);

    // Close on outside click
    useEffect(() => {
        /** @param {any} [e] @returns {any} */
        const handleOutside = (e) => {
            if (popoverRef.current && anchorRef.current &&
                !isEventInsideElement(e, popoverRef.current) && !isEventInsideElement(e, anchorRef.current)) {
                onClose();
            }
        };
        document.addEventListener('mousedown', handleOutside);
        return () => document.removeEventListener('mousedown', handleOutside);
    }, [onClose, anchorRef]);

    /** @type {any} */
    const cellCount = selectedCellKeys ? selectedCellKeys.length : 0;

    /** @returns {any} */
    const apply = () => {
        if (cellCount === 0) return;
        setCellFormatRules(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            selectedCellKeys.forEach(/** @param {any} [k] @returns {any} */ k => {
                next[k] = {
                    bg,
                    color,
                    bold,
                    italic,
                    textAlign,
                    borderStyle,
                    borderColor,
                    borderWidth: Math.max(1, Math.min(5, Number(borderWidth) || 1)),
                };
            });
            return next;
        });
    };

    /** @returns {any} */
    const clear = () => {
        if (cellCount === 0) return;
        setCellFormatRules(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            selectedCellKeys.forEach(/** @param {any} [k] @returns {any} */ k => { delete next[k]; });
            return next;
        });
    };

    /** @returns {any} */
    const applyEditedStyle = () => {
        setCellFormatRules(/** @param {any} [prev] @returns {any} */ (prev) => ({
            ...prev,
            [EDITED_CELL_FORMAT_KEY]: {
                bg: editedBg,
                color: editedColor,
                bold: editedBold,
                italic: editedItalic,
            },
        }));
    };

    /** @returns {any} */
    const clearEditedStyle = () => {
        setCellFormatRules(/** @param {any} [prev] @returns {any} */ (prev) => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            delete next[EDITED_CELL_FORMAT_KEY];
            return next;
        });
    };
    /** @type {any} */
    const editedPreviewStyle = buildEditedCellVisualStyle(
        theme,
        { bg: editedBg, color: editedColor, bold: editedBold, italic: editedItalic },
        { direct: true, propagated: false },
        { emphasizeText: true }
    ) || {};
    /** @type {Record<string, any>} */
    const selectionPreviewStyle = {
        background: bg || theme.background,
        color: color || theme.text,
        fontWeight: bold ? 700 : 400,
        fontStyle: italic ? 'italic' : 'normal',
        textAlign: textAlign || 'right',
        border: borderStyle
            ? `${Math.max(1, Math.min(5, Number(borderWidth) || 1))}px ${borderStyle} ${borderColor || theme.border}`
            : `1px solid ${theme.border}`,
    };

    const popover = (
        <div ref={popoverRef} data-cell-format-popover="true" style={{
            position: 'fixed',
            left: popoverPosition ? `${popoverPosition.left}px` : '12px',
            top: popoverPosition ? `${popoverPosition.top}px` : '12px',
            zIndex: 9999,
            background: theme.background || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: '10px',
            boxShadow: '0 4px 16px rgba(0,0,0,0.18)',
            padding: '14px',
            boxSizing: 'border-box',
            width: popoverPosition ? `${popoverPosition.width}px` : 'min(360px, calc(100vw - 24px))',
            maxWidth: 'calc(100vw - 24px)',
            maxHeight: popoverPosition ? `${popoverPosition.maxHeight}px` : 'calc(100vh - 24px)',
            overflowX: 'hidden',
            overflowY: 'auto',
            visibility: popoverPosition ? 'visible' : 'hidden',
            overscrollBehavior: 'contain',
        }}>
            <div style={{display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px', marginBottom: '12px'}}>
                <div>
                    <div style={{fontWeight: 750, fontSize: '14px', color: theme.text}}>
                        Format cells
                    </div>
                    <div style={{fontSize: '11px', color: theme.textSec, marginTop: '2px'}}>
                        {cellCount > 0 ? `${cellCount} selected cell${cellCount === 1 ? '' : 's'}` : 'No cells selected'}
                    </div>
                </div>
                <button type="button" aria-label="Close cell formatting" onClick={onClose} style={{...styles.btn, padding: '3px 8px', background: theme.hover}}>×</button>
            </div>
            {cellCount === 0 ? (
                <div style={{fontSize: '12px', color: theme.textSec, marginBottom: '10px'}}>Select cells to format, or tune the edited-cell highlight below.</div>
            ) : (
                <>
                    <div style={{fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '8px'}}>Fill and text</div>
                    <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', marginBottom: '10px'}}>
                        <CellColorField label="Background" value={bg} onChange={setBg} fallback={colorToInputHex(theme.surfaceBg || theme.background, '#FFFFFF')} theme={theme} />
                        <CellColorField label="Text color" value={color} onChange={setColor} fallback={colorToInputHex(theme.text, '#111827')} theme={theme} />
                    </div>
                    <div style={{display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '10px', marginBottom: '12px'}}>
                        <div style={{display: 'flex', gap: '5px'}}>
                            <button type="button" aria-pressed={bold} onClick={() => setBold(/** @param {any} [b] @returns {any} */ b => !b)} style={{...styles.btn, fontWeight: 'bold', background: bold ? theme.select : theme.hover, minWidth: '36px'}} title="Bold">B</button>
                            <button type="button" aria-pressed={italic} onClick={() => setItalic(/** @param {any} [i] @returns {any} */ i => !i)} style={{...styles.btn, fontStyle: 'italic', background: italic ? theme.select : theme.hover, minWidth: '36px'}} title="Italic"><em>I</em></button>
                        </div>
                        <div role="group" aria-label="Cell alignment" style={{display: 'flex', gap: '3px'}}>
                            {[['left', 'Left'], ['center', 'Center'], ['right', 'Right']].map(([value, label]) => (
                                <button key={value} type="button" aria-label={`${label} align`} aria-pressed={textAlign === value} onClick={() => setTextAlign(textAlign === value ? '' : value)} style={{...styles.btn, background: textAlign === value ? theme.select : theme.hover, minWidth: '34px', padding: '4px 7px'}}>{label[0]}</button>
                            ))}
                        </div>
                    </div>
                    <div style={{fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '8px'}}>Border</div>
                    <div style={{display: 'grid', gridTemplateColumns: '1.25fr 0.9fr 58px', gap: '8px', alignItems: 'end', marginBottom: '12px'}}>
                        <label style={{display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec, fontWeight: 600}}>
                            Style
                            <select aria-label="Border style" value={borderStyle} onChange={(event) => setBorderStyle(event.target.value)} style={{height: '30px', border: `1px solid ${theme.border}`, borderRadius: '6px', background: theme.background, color: theme.text, fontSize: '11px'}}>
                                {CELL_BORDER_STYLES.map((option) => <option key={option.value || 'none'} value={option.value}>{option.label}</option>)}
                            </select>
                        </label>
                        <CellColorField label="Color" value={borderColor} onChange={setBorderColor} fallback="#CBD5E1" theme={theme} />
                        <label style={{display: 'flex', flexDirection: 'column', gap: '5px', fontSize: '11px', color: theme.textSec, fontWeight: 600}}>
                            Width
                            <input aria-label="Border width" type="number" min="1" max="5" value={borderWidth} onChange={(event) => setBorderWidth(event.target.value)} disabled={!borderStyle} style={{height: '30px', width: '100%', boxSizing: 'border-box', border: `1px solid ${theme.border}`, borderRadius: '6px', padding: '4px 6px', background: theme.background, color: theme.text}} />
                        </label>
                    </div>
                    <div aria-label="Cell formatting preview" style={{...selectionPreviewStyle, borderRadius: '6px', minHeight: '38px', boxSizing: 'border-box', padding: '9px 12px', marginBottom: '12px', fontVariantNumeric: 'tabular-nums'}}>12,480.00</div>
                    <div style={{display: 'flex', gap: '6px', marginBottom: '14px'}}>
                        <button type="button" onClick={apply} style={{...styles.btn, background: theme.primary, color: '#fff', flex: 1}}>Apply to {cellCount}</button>
                        <button type="button" onClick={clear} style={{...styles.btn, background: theme.hover}}>Clear formatting</button>
                    </div>
                </>
            )}
            <div style={{ borderTop: `1px solid ${theme.border}`, paddingTop: '10px' }}>
                <div style={{fontSize: '11px', fontWeight: 700, color: theme.textSec, letterSpacing: '0.04em', textTransform: 'uppercase', marginBottom: '4px'}}>Edited cells</div>
                <div style={{fontSize: '11px', color: theme.textSec, marginBottom: '8px'}}>Used for direct edits and visible aggregate cells affected by the edit.</div>
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', marginBottom: '8px'}}>
                    <CellColorField label="Background" value={editedBg} onChange={setEditedBg} fallback="#E8F1FF" theme={theme} />
                    <CellColorField label="Text color" value={editedColor} onChange={setEditedColor} fallback="#111827" theme={theme} />
                </div>
                <div style={{display: 'flex', gap: '8px', marginBottom: '10px'}}>
                    <button onClick={() => setEditedBold(/** @param {any} [b] @returns {any} */ b => !b)}
                        style={{...styles.btn, fontWeight: 'bold', background: editedBold ? theme.select : theme.hover, minWidth: '36px'}}
                        title="Bold edited cells">B</button>
                    <button onClick={() => setEditedItalic(/** @param {any} [i] @returns {any} */ i => !i)}
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

    return typeof document !== 'undefined'
        ? ReactDOM.createPortal(popover, document.body)
        : popover;
}

/** @param {any} p0 @returns {any} */
function DataBarBoundsDialog({ columns, maximums, onApply, onClose, theme, styles }) {
    /** @type {any} */
    const values = columns.map(/** @param {any} [column] @returns {any} */ column => maximums && maximums[column]);
    /** @type {any} */
    const commonMaximum = values.every(/** @param {any} [value] @returns {any} */ value => value === values[0]) ? values[0] : undefined;
    /** @type {[any, (...args: any[]) => any]} */
    const [maximum, setMaximum] = useState(commonMaximum === undefined ? '' : String(commonMaximum));
    /** @type {any} */
    const parsedMaximum = maximum.trim() === '' ? null : Number(maximum);
    /** @type {any} */
    const valid = parsedMaximum === null || (Number.isFinite(parsedMaximum) && parsedMaximum > 0);

    useEffect(() => {
        /** @param {any} [event] @returns {any} */
        const handleKey = event => { if (event.key === 'Escape') onClose(); };
        document.addEventListener('keydown', handleKey);
        return () => document.removeEventListener('keydown', handleKey);
    }, [onClose]);

    return (
        <div role="presentation" onClick={onClose} style={{ position: 'fixed', inset: 0, zIndex: 10020, background: 'rgba(15,23,42,0.34)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px' }}>
            <div role="dialog" aria-modal="true" aria-label="Data Bar Bounds" onClick={event => event.stopPropagation()} style={{ width: 'min(380px, 90vw)', padding: '18px', borderRadius: '10px', border: `1px solid ${theme.border}`, background: theme.surfaceBg || theme.background, color: theme.text, boxShadow: theme.shadowMd }}>
                <div style={{ fontWeight: 700, marginBottom: '10px' }}>Data Bar Bounds</div>
                <p style={{ fontSize: '12px', lineHeight: 1.5 }}>Bars start at zero. Negative values use their magnitude and a red tint. Values above the maximum fill the cell.</p>
                <label style={{ display: 'flex', flexDirection: 'column', gap: '6px', fontSize: '12px' }}>
                    Maximum bar magnitude
                    <input autoFocus aria-label="Maximum bar magnitude" type="number" min="0" step="any" placeholder="Auto" value={maximum} onChange={event => setMaximum(event.target.value)} style={{ padding: '8px', borderRadius: '6px', border: `1px solid ${theme.border}`, color: theme.text, background: theme.background }} />
                </label>
                <p style={{ fontSize: '11px', color: theme.textSec }}>Leave blank to use automatic column bounds. Applies to {columns.length} selected column{columns.length === 1 ? '' : 's'}.</p>
                {!valid && <div role="alert" style={{ fontSize: '12px' }}>Enter a positive finite maximum, or leave blank for Auto.</div>}
                <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end', marginTop: '14px' }}>
                    <button type="button" onClick={onClose} style={styles.btn}>Cancel</button>
                    <button type="button" disabled={!valid} onClick={() => onApply(parsedMaximum)} style={{ ...styles.btn, background: theme.primary, color: '#fff' }}>Apply bounds</button>
                </div>
            </div>
        </div>
    );
}

/** @type {any[]} */
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

/** @type {any[]} */
const THEME_STRUCTURE_COLOR_FIELDS = [
    { key: 'hierarchyBg', label: 'Hierarchy Bg' },
    { key: 'totalBg', label: 'Total Bg' },
    { key: 'totalBgStrong', label: 'Grand Total Bg' },
    { key: 'totalText', label: 'Total Text' },
    { key: 'totalTextStrong', label: 'Grand Total Text' },
];

/** @type {any[]} */
const THEME_EDITED_COLOR_FIELDS = [
    { key: 'editedCellBg', label: 'Edited Background' },
    { key: 'editedCellBorder', label: 'Edited Border' },
    { key: 'editedCellText', label: 'Edited Text' },
];

/** @param {any} p0 @returns {any} */
function ThemeEditorPopover({ theme, themeName, themeOverrides, setThemeOverrides, onClose, anchorRef }) {
    /** @type {any} */
    const popoverRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [popoverLayout, setPopoverLayout] = useState({ top: 12, left: 12, width: 440, maxHeight: 560 });
    /** @type {any} */
    const editedPreviewStyle = buildEditedCellVisualStyle(
        theme,
        null,
        { direct: true, propagated: false },
        { emphasizeText: true }
    ) || {};

    useEffect(() => {
        /** @param {any} [e] @returns {any} */
        const handleOutside = (e) => {
            if (popoverRef.current && anchorRef.current &&
                !isEventInsideElement(e, popoverRef.current) && !isEventInsideElement(e, anchorRef.current)) {
                onClose();
            }
        };
        document.addEventListener('mousedown', handleOutside);
        return () => document.removeEventListener('mousedown', handleOutside);
    }, [onClose, anchorRef]);

    useLayoutEffect(() => {
        if (typeof window === 'undefined') return undefined;

        /** @returns {any} */
        const updateLayout = () => {
            /** @type {any} */
            const viewportWidth = window.innerWidth || 1280;
            /** @type {any} */
            const viewportHeight = window.innerHeight || 720;
            /** @type {any} */
            const availableWidth = Math.max(280, viewportWidth - 24);
            /** @type {any} */
            const width = Math.min(460, availableWidth);
            const preferredHeight = 560;
            /** @type {any} */
            const anchorRect = anchorRef.current && typeof anchorRef.current.getBoundingClientRect === 'function'
                ? anchorRef.current.getBoundingClientRect()
                : null;
            /** @type {any} */
            const preferredLeft = anchorRect ? (anchorRect.right - width) : (viewportWidth - width - 12);
            /** @type {any} */
            const left = Math.min(
                Math.max(12, preferredLeft),
                Math.max(12, viewportWidth - width - 12)
            );
            /** @type {any} */
            const belowTop = anchorRect ? (anchorRect.bottom + 8) : 12;
            /** @type {any} */
            const aboveTop = anchorRect ? Math.max(12, anchorRect.top - preferredHeight - 8) : 12;
            /** @type {any} */
            const shouldOpenAbove = Boolean(
                anchorRect
                && (belowTop + preferredHeight > viewportHeight - 12)
                && anchorRect.top > (viewportHeight - anchorRect.bottom)
            );
            /** @type {any} */
            const top = shouldOpenAbove ? aboveTop : Math.max(12, belowTop);
            /** @type {any} */
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

    /** @param {any} [key] @returns {any} */
    const resetField = (key) => {
        setThemeOverrides(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            delete next[key];
            return next;
        });
    };

    /** @returns {any} */
    const clearAll = () => setThemeOverrides({});
    /** @type {any} */
    const controlGridColumns = popoverLayout.width < 420 ? '1fr' : 'repeat(2, minmax(0, 1fr))';

    /** @param {any} p0 @returns {any} */
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
                    onChange={(e) => setThemeOverrides(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, [key]: e.target.value }))}
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

/** @param {any} p0 @returns {any} */
function PivotExportControl({ exportPivot, isExporting, buttonStyle, theme }) {
    /** @type {any} */
    const menuRef = useRef(null);
    /** @param {any} [format] @returns {any} */
    const runExport = (format) => {
        if (format === 'csv') exportPivot('csv');
        else exportPivot();
        if (menuRef.current) menuRef.current.removeAttribute('open');
    };
    /** @type {Record<string, any>} */
    const menuButtonStyle = {
        width: '100%',
        border: 'none',
        background: 'transparent',
        color: theme.text,
        cursor: isExporting ? 'not-allowed' : 'pointer',
        padding: '8px 12px',
        textAlign: 'left',
        whiteSpace: 'nowrap',
        opacity: isExporting ? 0.55 : 1,
    };

    return (
        <div role="group" aria-label="Export table" style={{ display: 'inline-flex', alignItems: 'stretch' }}>
            <button
                type="button"
                data-testid="pivot-export-xlsx"
                aria-label="Export XLSX"
                style={{ ...buttonStyle, borderTopRightRadius: 0, borderBottomRightRadius: 0 }}
                onClick={() => exportPivot()}
                disabled={isExporting}
            >
                <Icons.Export/> {isExporting ? 'Exporting...' : 'Export XLSX'}
            </button>
            <details ref={menuRef} style={{ position: 'relative', display: 'flex' }}>
                <summary
                    aria-label="Choose export format"
                    aria-disabled={isExporting}
                    onClick={event => { if (isExporting) event.preventDefault(); }}
                    style={{
                        ...buttonStyle,
                        alignItems: 'center',
                        borderTopLeftRadius: 0,
                        borderBottomLeftRadius: 0,
                        borderLeft: `1px solid ${theme.border}`,
                        boxSizing: 'border-box',
                        cursor: isExporting ? 'not-allowed' : 'pointer',
                        display: 'inline-flex',
                        height: '100%',
                        listStyle: 'none',
                        opacity: isExporting ? 0.55 : 1,
                        paddingLeft: '8px',
                        paddingRight: '8px',
                    }}
                >
                    <span aria-hidden="true">{'\u25BE'}</span>
                </summary>
                <ul
                    role="list"
                    aria-label="Export formats"
                    style={{
                        position: 'absolute',
                        right: 0,
                        top: 'calc(100% + 4px)',
                        zIndex: 50,
                        minWidth: '150px',
                        margin: 0,
                        padding: '4px',
                        listStyle: 'none',
                        border: `1px solid ${theme.border}`,
                        borderRadius: theme.radiusSm || '8px',
                        background: theme.surfaceBg || theme.headerBg,
                        boxShadow: theme.shadowMd || theme.shadow,
                    }}
                >
                    <li>
                        <button type="button" aria-label="Export XLSX" style={menuButtonStyle} onClick={() => runExport('xlsx')} disabled={isExporting}>
                            Export XLSX
                        </button>
                    </li>
                    <li>
                        <button type="button" data-testid="pivot-export-csv" aria-label="Export CSV" style={menuButtonStyle} onClick={() => runExport('csv')} disabled={isExporting}>
                            Export CSV
                        </button>
                    </li>
                </ul>
            </details>
        </div>
    );
}

/** @type {any} */
export const PivotAppBar = React.memo(/** @param {{ setShowSubtotals?: { (updater: (prev: any) => any): any; (val?: any): any }, [key: string]: any }} p0 @returns {any} */ function PivotAppBar({
    immersiveMode, setImmersiveMode,
    sidebarOpen, setSidebarOpen,
    themeName, setThemeName,
    themeOverrides, setThemeOverrides,
    spacingMode, setSpacingMode, spacingLabels,
    layoutMode, setLayoutMode,
    onAutoSizeColumns,
    autoSizeIncludesHeaderNext,
    colorScaleMode, setColorScaleMode,
    onReturnFocusToGrid,
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
    dataBarMaximums, setDataBarMaximums,
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
    /** @type {any} */
    const { theme, styles } = usePivotTheme();
    /** @type {any} */
    const {
        filters, setFilters,
        pivotMode, setPivotMode,
        reportDef, setReportDef,
        setColFields, setValConfigs,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        showColTotals, setShowColTotals,
        showRowTotals, setShowRowTotals,
        showSubtotals, setShowSubtotals: rawSetShowSubtotals,
        showRowNumbers, setShowRowNumbers,
        numberGroupSeparator, setNumberGroupSeparator,
    } = usePivotConfig();
    const setShowSubtotals = /** @type {{ (updater: (prev: any) => any): any; (val?: any): any }} */ (rawSetShowSubtotals);
    // Derive all selection-dependent state directly from selectedCells
    // inside AppBar to avoid stale prop issues from parent renders
    /** @type {any} */
    const selectedCellKeys = useMemo(() => {
        /** @type {any} */
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return [];
        return keys.map(/** @param {any} [key] @returns {any} */ key => {
            /** @type {any} */
            const colonIdx = key.indexOf(':');
            /** @type {any} */
            const rowId = key.substring(0, colonIdx);
            /** @type {any} */
            const colId = key.substring(colonIdx + 1);
            return `${rowId}:::${colId}`;
        });
    }, [selectedCells]);

    /** @type {any} */
    const selectedCellColIds = useMemo(() => {
        /** @type {any} */
        const ids = new Set();
        Object.keys(selectedCells || {}).forEach(key => {
            /** @type {any} */
            const idx = key.indexOf(':');
            if (idx >= 0) ids.add(key.substring(idx + 1));
        });
        return ids;
    }, [selectedCells]);

    /** @type {any} */
    const hasSelection = Object.keys(selectedCells || {}).length > 0;

    /** @param {any} [delta] @returns {any} */
    const handleDecimalChange = (delta) => {
        /** @type {any} */
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) {
            setDecimalPlaces(/** @param {any} [prev] @returns {any} */ prev => Math.max(0, Math.min(6, prev + delta)));
            return;
        }
        /** @type {any[]} */
        const colIds = [...new Set(keys.map(/** @param {any} [k] @returns {any} */ k => {
            /** @type {any} */
            const idx = k.indexOf(':');
            return idx >= 0 ? k.substring(idx + 1) : k;
        }))];
        setColumnDecimalOverrides(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            colIds.forEach(colId => {
                /** @type {any} */
                const cur = next[colId] !== undefined ? next[colId] : decimalPlaces;
                next[colId] = Math.max(0, Math.min(6, cur + delta));
            });
            return next;
        });
    };

    /** @type {any} */
    const displayDecimal = useMemo(() => {
        /** @type {any} */
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return decimalPlaces;
        const k = keys[0];
        /** @type {any} */
        const idx = k.indexOf(':');
        /** @type {any} */
        const colId = idx >= 0 ? k.substring(idx + 1) : k;
        return columnDecimalOverrides[colId] !== undefined ? columnDecimalOverrides[colId] : decimalPlaces;
    }, [selectedCells, columnDecimalOverrides, decimalPlaces]);
    /** @type {any} */
    const activeSelectionColumnIds = useMemo(() => Array.from(selectedCellColIds || []), [selectedCellColIds]);
    /** @type {any} */
    const selectedNumericColumnIds = useMemo(() => [...new Set(Object.entries(selectedCells || {})
        .filter(([, value]) => typeof value === 'number' && Number.isFinite(value))
        .map(([key]) => key.slice(key.lastIndexOf(':') + 1)))] , [selectedCells]);
    /** @type {any} */
    const displayedNumberGroupSeparator = useMemo(() => {
        /** @type {any} */
        const fallback = normalizeNumberGroupSeparator(numberGroupSeparator);
        if (activeSelectionColumnIds.length === 0) return fallback;
        /** @type {any} */
        const resolved = activeSelectionColumnIds.map(/** @param {any} [colId] @returns {any} */ (colId) => normalizeNumberGroupSeparator(
            columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[colId] !== undefined
                ? columnGroupSeparatorOverrides[colId]
                : fallback
        ));
        return new Set(resolved).size === 1 ? resolved[0] : fallback;
    }, [activeSelectionColumnIds, columnGroupSeparatorOverrides, numberGroupSeparator]);
    /** @type {any} */
    const displayedSelectionFormat = useMemo(() => {
        if (activeSelectionColumnIds.length === 0) return defaultValueFormat;
        /** @type {any} */
        const fallback = selectionValueFormat || defaultValueFormat || '';
        /** @type {any} */
        const resolved = activeSelectionColumnIds.map(/** @param {any} [colId] @returns {any} */ (colId) => (
            columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, colId)
                ? columnFormatOverrides[colId]
                : fallback
        ));
        return new Set(resolved).size === 1 ? resolved[0] : fallback;
    }, [activeSelectionColumnIds, columnFormatOverrides, selectionValueFormat, defaultValueFormat]);
    /** @type {any} */
    const activeFormatCategory = useMemo(() => (
        getNumberFormatCategory(hasSelection ? displayedSelectionFormat : defaultValueFormat)
    ), [hasSelection, displayedSelectionFormat, defaultValueFormat]);
    /** @type {any} */
    const activeCurrencySymbol = useMemo(() => (
        getCurrencySymbolFromFormat(hasSelection ? displayedSelectionFormat : defaultValueFormat)
    ), [hasSelection, displayedSelectionFormat, defaultValueFormat]);
    /** @type {[any, (...args: any[]) => any]} */
    const [cellFormatOpen, setCellFormatOpen] = useState(false);
    /** @type {any} */
    const cellFormatBtnRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [numberFormatDialogOpen, setNumberFormatDialogOpen] = useState(false);
    // Capture scope when opening, so later selection changes cannot retarget Apply.
    /** @type {[any, (...args: any[]) => any]} */
    const [dataBarBoundsColumns, setDataBarBoundsColumns] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [themeEditorOpen, setThemeEditorOpen] = useState(false);
    /** @type {any} */
    const themeEditorBtnRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [openToolbarSections, setOpenToolbarSections] = useState({
        view: false,
        format: true,
        charts: true,
        theme: false,
    });
    /** @type {any} */
    const hasSavedCellFormats = Object.keys(cellFormatRules || {}).length > 0;
    /** @param {any} [delta] @returns {any} */
    const handleZoomChange = (delta) => {
        setZoomLevel(/** @param {any} [prev] @returns {any} */ prev => Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, prev + delta)));
    };
    /** @param {any} [nextValue] @returns {any} */
    const handleSetDecimalPlacesValue = (nextValue) => {
        /** @type {any} */
        const normalized = Math.max(DECIMAL_MIN, Math.min(DECIMAL_MAX, Math.floor(Number(nextValue) || 0)));
        /** @type {any} */
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) {
            setDecimalPlaces(normalized);
            return;
        }
        /** @type {any[]} */
        const colIds = [...new Set(keys.map(/** @param {any} [k] @returns {any} */ k => {
            /** @type {any} */
            const idx = k.indexOf(':');
            return idx >= 0 ? k.substring(idx + 1) : k;
        }))];
        setColumnDecimalOverrides(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...prev };
            colIds.forEach(colId => {
                next[colId] = normalized;
            });
            return next;
        });
    };
    /** @param {any} [nextSeparator] @returns {any} */
    const handleSetGroupSeparatorValue = (nextSeparator) => {
        /** @type {any} */
        const normalized = normalizeNumberGroupSeparator(nextSeparator);
        if (activeSelectionColumnIds.length === 0 || typeof setColumnGroupSeparatorOverrides !== 'function') {
            setNumberGroupSeparator(normalized);
            return;
        }
        setColumnGroupSeparatorOverrides(/** @param {any} [prev] @returns {any} */ prev => {
            /** @type {Record<string, any>} */
            const next = { ...(prev || {}) };
            activeSelectionColumnIds.forEach(/** @param {any} [colId] @returns {any} */ (colId) => {
                if (normalized === normalizeNumberGroupSeparator(numberGroupSeparator)) {
                    delete next[colId];
                } else {
                    next[colId] = normalized;
                }
            });
            return next;
        });
    };
    /** @param {any} [nextFormat] @returns {any} */
    const handleSetColumnFormatValue = (nextFormat) => {
        if (activeSelectionColumnIds.length === 0 || typeof setColumnFormatOverrides !== 'function') {
            setDefaultValueFormat(nextFormat);
            return;
        }
        setColumnFormatOverrides(/** @param {any} [prev] @returns {any} */ (prev) => {
            /** @type {Record<string, any>} */
            const next = { ...(prev || {}) };
            activeSelectionColumnIds.forEach(/** @param {any} [colId] @returns {any} */ (colId) => {
                next[colId] = nextFormat;
            });
            return next;
        });
    };
    /** @param {any} p0 @returns {any} */
    const handleApplyNumberFormatDialog = ({ category, decimalPlaces: nextDecimals, groupSeparator, currencySymbol }) => {
        handleSetDecimalPlacesValue(nextDecimals);
        handleSetGroupSeparatorValue(groupSeparator);
        /** @type {any} */
        const nextFormat = getFormatValueForCategory(category, currencySymbol);
        handleSetColumnFormatValue(nextFormat);
        setNumberFormatDialogOpen(false);
    };
    const canApplyNumberFormatCategory = true;
    /** @type {any} */
    const activeFormatCategoryLabel = useMemo(() => {
        /** @type {any} */
        const match = NUMBER_FORMAT_CATEGORY_OPTIONS.find((option) => option.value === activeFormatCategory);
        return match ? match.label : 'Number';
    }, [activeFormatCategory]);
    /** @type {any} */
    const displayedSeparatorLabel = useMemo(() => {
        /** @type {any} */
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
    /** @param {any} [sectionId] @returns {any} */
    const toggleToolbarSection = (sectionId) => {
        setOpenToolbarSections(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, [sectionId]: !prev[sectionId] }));
    };

    // Base styles for different button variants
    /** @type {Record<string, any>} */
    const btnBase = {
        ...styles.btn,
        borderRadius: theme.radiusSm || '10px',
        padding: '7px 12px',
        fontSize: '12px',
        fontWeight: 600,
        lineHeight: 1.5,
        minHeight: '36px',
    };
    /** @type {Record<string, any>} */
    const btnGhost = {
        ...btnBase,
        background: 'transparent',
        border: `1px solid transparent`,
    };
    /** @type {Record<string, any>} */
    const btnSubtle = {
        ...btnBase,
        background: theme.headerSubtleBg || theme.hover,
        border: `1px solid ${theme.border}`,
        boxShadow: theme.shadowInset || 'none',
    };
    /** @type {Record<string, any>} */
    const btnActive = {
        ...btnBase,
        background: theme.select,
        border: `1px solid ${theme.primary}`,
        color: theme.primary,
        boxShadow: theme.shadowInset || 'none',
    };
    /** @type {Record<string, any>} */
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
    /** @param {any} [mode] @param {any} [disabled] @returns {any} */
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
    /** @type {Record<string, any>} */
    const btnPrimary = {
        ...btnBase,
        background: theme.primary,
        color: '#fff',
        border: `1px solid ${theme.primary}`,
        boxShadow: '0 8px 18px rgba(79,70,229,0.18)',
    };
    // Save View: animated shimmer gradient (same keyframe as skeleton loader)
    /** @type {Record<string, any>} */
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

    /** @type {Record<string, any>} */
    const firstLineStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        rowGap: '8px',
        flexWrap: 'wrap',
        width: '100%',
        minHeight: '40px',
    };
    /** @type {Record<string, any>} */
    const innerDividerStyle = {
        width: '1.5px',
        alignSelf: 'stretch',
        minHeight: '22px',
        margin: '7px 2px',
        background: theme.isDark ? 'rgba(255,255,255,0.28)' : 'rgba(0,0,0,0.28)',
        flexShrink: 0,
        borderRadius: '1px',
    };
    /** @type {Record<string, any>} */
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
    /** @param {any} [isLast] @returns {any} */
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
    /** @type {Record<string, any>} */
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
    /** @type {Record<string, any>} */
    const compactSelectStyle = {
        ...btnSubtle,
        outline: 'none',
        cursor: 'pointer',
    };
    /** @type {Record<string, any>} */
    const themeSelectStyle = {
        ...compactSelectStyle,
        background: theme.themeSelectorBg || theme.headerSubtleBg || theme.hover,
        color: theme.themeSelectorText || theme.text,
    };
    /** @param {any} [active] @returns {any} */
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
            <button style={btnSubtle} onClick={() => setLayoutMode(/** @param {any} [prev] @returns {any} */ prev => prev === 'hierarchy' ? 'outline' : prev === 'outline' ? 'tabular' : 'hierarchy')} title="Cycle layout mode">
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
                    onClick={() => setCellFormatOpen(/** @param {any} [open] @returns {any} */ open => !open)}
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
                        onClose={() => { setCellFormatOpen(false); onReturnFocusToGrid?.(); }}
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
                onChange={e => setColorScaleMode(e.target.value, Array.from(selectedCellColIds))}
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
                /** @type {any} */
                const hasCellSel = selectedCellColIds && selectedCellColIds.size > 0;
                /** @type {any} */
                const hasBars = hasCellSel && [...selectedCellColIds].some(id => dataBarsColumns && dataBarsColumns.has(id));
                /** @type {any} */
                const anyBars = dataBarsColumns && dataBarsColumns.size > 0;
                /** @type {any} */
                const isActive = hasBars || anyBars;
                return (
                    <button
                        title={hasCellSel ? 'Toggle data bars for selected columns' : 'Data Bars - select cells to target columns'}
                        style={isActive ? btnActive : btnSubtle}
                        onClick={() => {
                            if (!setDataBarsColumns) return;
                            if (hasCellSel) {
                                setDataBarsColumns(/** @param {any} [prev] @returns {any} */ prev => {
                                    /** @type {any} */
                                    const next = new Set(prev);
                                    /** @type {any} */
                                    const allOn = [...selectedCellColIds].every(id => next.has(id));
                                    selectedCellColIds.forEach(/** @param {any} [id] @returns {any} */ id => allOn ? next.delete(id) : next.add(id));
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
            <button
                type="button"
                title={selectedNumericColumnIds.length ? 'Set data bar bounds for selected columns' : 'Select numeric cells to set data bar bounds'}
                disabled={selectedNumericColumnIds.length === 0}
                onClick={() => setDataBarBoundsColumns([...selectedNumericColumnIds])}
                style={{ ...btnSubtle, opacity: selectedNumericColumnIds.length ? 1 : 0.45 }}
            >Bar Bounds</button>
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
                    onClick={() => setThemeEditorOpen(/** @param {any} [open] @returns {any} */ open => !open)}
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
                            {THEME_LABELS[t] || t.replace(/_/g, ' ').replace(/\b\w/g, /** @param {any} [c] @returns {any} */ c => c.toUpperCase())}
                        </option>
                    ))}
            </select>
            <button style={btnSaveView} onClick={onSaveView}><Icons.Save /> Save View</button>
        </>
    );

    /** @type {any} */
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
                    data-testid="pivot-toggle-sidebar"
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
                            data-testid="pivot-mode-pivot"
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
                            data-testid="pivot-mode-report"
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
                            /** @type {any} */
                            const report = savedReports.find(/** @param {any} [r] @returns {any} */ r => r.id === id);
                            if (report && report.reportDef && setReportDef) {
                                applySavedReport(report, { setReportDef, setActiveReportId, setColFields, setValConfigs });
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
                        {savedReports.map(/** @param {any} [r] @returns {any} */ r => (
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
                            data-testid="pivot-global-search"
                            style={{border:'none',background:'transparent',marginLeft:'8px',outline:'none',width:'100%', color: theme.text, fontSize:'13px'}}
                            placeholder="Search records..."
                            value={(filters && filters.global) || ''}
                            onChange={e => {
                                const val = e.target.value;
                                setFilters(/** @param {any} [p] @returns {any} */ p => {
                                    /** @type {Record<string, any>} */
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
                <button data-testid="pivot-toolbar-view" data-toolbar-section-toggle="view" style={sectionToggleButtonStyle(openToolbarSections.view)} onClick={() => toggleToolbarSection('view')}>View</button>
                <button data-testid="pivot-toolbar-format" data-toolbar-section-toggle="format" style={sectionToggleButtonStyle(openToolbarSections.format)} onClick={() => toggleToolbarSection('format')}>Format</button>
                {uiConfig.showCharts !== false && <button data-testid="pivot-toolbar-charts" data-toolbar-section-toggle="charts" style={sectionToggleButtonStyle(openToolbarSections.charts)} onClick={() => toggleToolbarSection('charts')}>Charts</button>}
                <button data-testid="pivot-toolbar-theme" data-toolbar-section-toggle="theme" style={sectionToggleButtonStyle(openToolbarSections.theme)} onClick={() => toggleToolbarSection('theme')}>Theme</button>
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
                </> : <>
                    <div style={{marginLeft:'auto'}} />
                    <div style={innerDividerStyle} />
                </>}
                <PivotExportControl exportPivot={exportPivot} isExporting={isExporting} buttonStyle={btnPrimary} theme={theme} />
            </div>

            {activeSections.length > 0 ? (
                <div style={secondLineStyle}>
                    {activeSections.map(/** @param {any} [section] @param {any} [index] @returns {any} */ (section, index) => (
                        <div key={section.id} style={sectionBlockStyle(index === activeSections.length - 1)}>
                            <span style={sectionLabelStyle}>{section.label}</span>
                            {section.controls}
                        </div>
                    ))}
                </div>
            ) : null}
        </div>
        {dataBarBoundsColumns && <DataBarBoundsDialog
            columns={dataBarBoundsColumns}
            maximums={dataBarMaximums}
            theme={theme}
            styles={styles}
            onClose={() => setDataBarBoundsColumns(null)}
            onApply={/** @param {any} [maximum] @returns {any} */ maximum => {
                setDataBarMaximums(/** @param {any} [previous] @returns {any} */ previous => updateDataBarMaximums(previous, dataBarBoundsColumns, maximum));
                setDataBarBoundsColumns(null);
                if (onReturnFocusToGrid) onReturnFocusToGrid();
            }}
        />}
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

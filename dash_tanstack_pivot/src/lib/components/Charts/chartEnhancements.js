/**
 * Chart enhancement helpers — components and utilities for extended chart settings.
 * Imported by PivotCharts.js to keep that file from growing further.
 */
import React, { useState } from 'react';

// ─── Dash pattern helpers ──────────────────────────────────────────────────

export const DASH_STYLES = [
    { value: 'solid',  label: 'Solid',  dash: 'none' },
    { value: 'dashed', label: 'Dashed', dash: '8 4' },
    { value: 'dotted', label: 'Dotted', dash: '2 4' },
    { value: 'dot-dash', label: 'Dot-Dash', dash: '8 4 2 4' },
];

export const getDashArray = (style) => {
    const found = DASH_STYLES.find(d => d.value === style);
    return found && found.dash !== 'none' ? found.dash : undefined;
};

// ─── Gradient helpers ──────────────────────────────────────────────────────

export const GRADIENT_DIRECTIONS = [
    { value: 'vertical',   label: 'Vertical',   x1: '0', y1: '0', x2: '0', y2: '1' },
    { value: 'horizontal', label: 'Horizontal', x1: '0', y1: '0', x2: '1', y2: '0' },
    { value: 'diagonal',   label: 'Diagonal',   x1: '0', y1: '0', x2: '1', y2: '1' },
];

export const buildGradientStops = (color, opacity) => {
    const op = (Number(opacity) || 70) / 100;
    return [
        { offset: '0%',   stopOpacity: String(Math.min(1, op + 0.2)) },
        { offset: '100%', stopOpacity: String(op) },
    ];
};

// ─── Legend position ───────────────────────────────────────────────────────

export const LEGEND_POSITIONS = ['bottom', 'top', 'none'];

export const ChartLegendPositionButtons = ({ legendPosition, onChange, theme }) => {
    const btn = (val, label) => ({
        border: `1px solid ${val === legendPosition ? theme.primary : theme.border}`,
        background: val === legendPosition ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: val === legendPosition ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '5px 10px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });
    return (
        <div style={{ display: 'flex', gap: '5px' }}>
            {LEGEND_POSITIONS.map(pos => (
                <button key={pos} type="button" onClick={() => onChange(pos)} style={btn(pos, pos)}>
                    {pos.charAt(0).toUpperCase() + pos.slice(1)}
                </button>
            ))}
        </div>
    );
};

// ─── Series sort ───────────────────────────────────────────────────────────

export const SERIES_SORT_MODES = [
    { value: 'none',       label: 'Natural' },
    { value: 'value_desc', label: 'Val ↓' },
    { value: 'value_asc',  label: 'Val ↑' },
    { value: 'alpha_asc',  label: 'A→Z' },
    { value: 'alpha_desc', label: 'Z→A' },
];

export const applySeriesSort = (series, mode) => {
    if (!Array.isArray(series) || mode === 'none' || !mode) return series;
    const clone = [...series];
    if (mode === 'alpha_asc')  return clone.sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
    if (mode === 'alpha_desc') return clone.sort((a, b) => String(b.name || '').localeCompare(String(a.name || '')));
    const sum = (s) => (s.values || []).reduce((acc, v) => acc + (Number.isFinite(Number(v)) ? Number(v) : 0), 0);
    if (mode === 'value_desc') return clone.sort((a, b) => sum(b) - sum(a));
    if (mode === 'value_asc')  return clone.sort((a, b) => sum(a) - sum(b));
    return clone;
};

// ─── Data limit warning ────────────────────────────────────────────────────

export const ChartDataLimitWarning = ({ model, maxCategories, maxSeries, theme }) => {
    const catCount = Array.isArray(model && model.categories) ? model.categories.length : 0;
    const serCount = Array.isArray(model && model.series) ? model.series.length : 0;
    const catWarn = catCount >= maxCategories * 0.9;
    const serWarn = serCount >= maxSeries * 0.9;
    const catAt   = catCount >= maxCategories;
    const serAt   = serCount >= maxSeries;
    if (!catWarn && !serWarn) return null;

    const chipStyle = (atLimit) => ({
        display: 'inline-flex', alignItems: 'center', gap: '4px',
        padding: '3px 8px',
        borderRadius: theme.radiusSm || '8px',
        fontSize: '10px', fontWeight: 700,
        background: atLimit ? (theme.danger || '#ef4444') + '22' : (theme.warning || '#f59e0b') + '22',
        color: atLimit ? (theme.danger || '#ef4444') : (theme.warning || '#ca8a04'),
        border: `1px solid ${atLimit ? (theme.danger || '#ef4444') : (theme.warning || '#f59e0b')}44`,
    });

    return (
        <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
            {catWarn && (
                <span style={chipStyle(catAt)}>
                    {catAt ? '⚠ ' : '○ '}{catCount}/{maxCategories} categories
                </span>
            )}
            {serWarn && (
                <span style={chipStyle(serAt)}>
                    {serAt ? '⚠ ' : '○ '}{serCount}/{maxSeries} series
                </span>
            )}
        </div>
    );
};

// ─── Line dash editor (per-series) ────────────────────────────────────────

export const ChartLineDashEditor = ({ series, lineDashStyles, onChange, seriesColors, paletteColors, getColorForIndex, theme, compactInputStyle }) => {
    if (!Array.isArray(series) || series.length === 0) return null;
    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
            {series.slice(0, 16).map((ser, si) => {
                const color = (seriesColors && seriesColors[ser.name]) || getColorForIndex(si, theme, paletteColors);
                const current = (lineDashStyles && lineDashStyles[ser.name]) || 'solid';
                return (
                    <div key={ser.name || si} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                        <span style={{ width: '10px', height: '10px', borderRadius: '50%', background: color, flexShrink: 0 }} />
                        <span style={{ fontSize: '11px', color: theme.text, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{ser.name || `Series ${si + 1}`}</span>
                        <select
                            value={current}
                            onChange={e => onChange({ ...lineDashStyles, [ser.name]: e.target.value })}
                            style={{ ...compactInputStyle, width: '82px', flexShrink: 0 }}
                        >
                            {DASH_STYLES.map(d => <option key={d.value} value={d.value}>{d.label}</option>)}
                        </select>
                    </div>
                );
            })}
        </div>
    );
};

// ─── Custom palette editor ─────────────────────────────────────────────────

export const ChartCustomPaletteEditor = ({ customColors, onChange, theme, compactInputStyle }) => {
    const colors = Array.isArray(customColors) && customColors.length > 0
        ? customColors
        : ['#2563EB', '#F97316', '#0F766E', '#7C3AED', '#DC2626', '#0891B2'];

    const updateColor = (idx, hex) => {
        const next = [...colors];
        next[idx] = hex;
        onChange(next);
    };
    const addColor = () => onChange([...colors, '#888888']);
    const removeColor = (idx) => onChange(colors.filter((_, i) => i !== idx));

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', alignItems: 'center' }}>
                {colors.map((hex, i) => (
                    <div key={i} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '2px' }}>
                        <input
                            type="color"
                            value={hex}
                            onChange={e => updateColor(i, e.target.value)}
                            style={{ width: '28px', height: '28px', padding: 0, border: `1px solid ${theme.border}`, borderRadius: '4px', cursor: 'pointer', background: 'none' }}
                            title={`Color ${i + 1}`}
                        />
                        {colors.length > 2 && (
                            <button type="button" onClick={() => removeColor(i)} style={{ border: 'none', background: 'none', color: theme.textSec, cursor: 'pointer', fontSize: '9px', padding: 0, lineHeight: 1 }}>✕</button>
                        )}
                    </div>
                ))}
                {colors.length < 12 && (
                    <button type="button" onClick={addColor} style={{ ...compactInputStyle, padding: '5px 8px', flexShrink: 0 }}>+</button>
                )}
            </div>
        </div>
    );
};

// ─── Chart type gallery ────────────────────────────────────────────────────

const S = 16; // icon viewBox size

const BarIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="1" y="9" width="4" height="6" fill={color} opacity="0.85" rx="0.5"/>
        <rect x="6" y="5" width="4" height="10" fill={color} opacity="0.85" rx="0.5"/>
        <rect x="11" y="7" width="4" height="8" fill={color} opacity="0.85" rx="0.5"/>
    </svg>
);
const LineIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <polyline points="1,13 5,7 9,10 15,3" stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
        <circle cx="1" cy="13" r="1.4" fill={color}/><circle cx="5" cy="7" r="1.4" fill={color}/><circle cx="9" cy="10" r="1.4" fill={color}/><circle cx="15" cy="3" r="1.4" fill={color}/>
    </svg>
);
const PieIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <path d="M8,8 L8,1 A7,7 0 0,1 15,8 Z" fill={color} opacity="0.9"/>
        <path d="M8,8 L15,8 A7,7 0 0,1 6,15 Z" fill={color} opacity="0.55"/>
        <path d="M8,8 L6,15 A7,7 1 1,1 8,1 Z" fill={color} opacity="0.3"/>
    </svg>
);
const ScatterIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <circle cx="3" cy="12" r="1.8" fill={color} opacity="0.85"/>
        <circle cx="7" cy="5" r="1.8" fill={color} opacity="0.85"/>
        <circle cx="12" cy="9" r="1.8" fill={color} opacity="0.85"/>
        <circle cx="13" cy="3" r="1.8" fill={color} opacity="0.85"/>
        <circle cx="5" cy="11" r="1.8" fill={color} opacity="0.85"/>
    </svg>
);
const ComboIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="1" y="9" width="4" height="6" fill={color} opacity="0.5" rx="0.5"/>
        <rect x="6" y="5" width="4" height="10" fill={color} opacity="0.5" rx="0.5"/>
        <rect x="11" y="8" width="4" height="7" fill={color} opacity="0.5" rx="0.5"/>
        <polyline points="1,9 5,4 9,7 15,2" stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
    </svg>
);
const WaterfallIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="1" y="4" width="3" height="7" fill={color} opacity="0.85" rx="0.5"/>
        <rect x="5" y="8" width="3" height="3" fill={color} opacity="0.5" rx="0.5"/>
        <rect x="9" y="5" width="3" height="6" fill={color} opacity="0.85" rx="0.5"/>
        <rect x="13" y="10" width="3" height="1" fill={color} opacity="0.4" rx="0.5"/>
        <line x1="0" y1="15" x2="16" y2="15" stroke={color} strokeWidth="0.7" opacity="0.35"/>
    </svg>
);
const StatIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="2" y="6" width="12" height="5" fill="none" stroke={color} strokeWidth="1.2" rx="1" opacity="0.85"/>
        <line x1="8" y1="6" x2="8" y2="11" stroke={color} strokeWidth="1.2" opacity="0.85"/>
        <line x1="5" y1="3" x2="5" y2="13" stroke={color} strokeWidth="1.1" opacity="0.55"/>
        <line x1="11" y1="3" x2="11" y2="13" stroke={color} strokeWidth="1.1" opacity="0.55"/>
        <line x1="4" y1="3" x2="6" y2="3" stroke={color} strokeWidth="1.1" opacity="0.55"/>
        <line x1="10" y1="3" x2="12" y2="3" stroke={color} strokeWidth="1.1" opacity="0.55"/>
        <line x1="4" y1="13" x2="6" y2="13" stroke={color} strokeWidth="1.1" opacity="0.55"/>
        <line x1="10" y1="13" x2="12" y2="13" stroke={color} strokeWidth="1.1" opacity="0.55"/>
    </svg>
);
const RadialIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <polygon points="8,2 14,6 12,13 4,13 2,6" fill="none" stroke={color} strokeWidth="1" opacity="0.35"/>
        <polygon points="8,5 11,7.5 9.5,11 6.5,11 5,7.5" fill={color} opacity="0.65"/>
    </svg>
);
const HeatmapIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="4" height="4" fill={color} opacity="0.9" rx="0.5"/>
        <rect x="6" y="1" width="4" height="4" fill={color} opacity="0.4" rx="0.5"/>
        <rect x="11" y="1" width="4" height="4" fill={color} opacity="0.7" rx="0.5"/>
        <rect x="1" y="6" width="4" height="4" fill={color} opacity="0.3" rx="0.5"/>
        <rect x="6" y="6" width="4" height="4" fill={color} opacity="0.85" rx="0.5"/>
        <rect x="11" y="6" width="4" height="4" fill={color} opacity="0.5" rx="0.5"/>
        <rect x="1" y="11" width="4" height="4" fill={color} opacity="0.6" rx="0.5"/>
        <rect x="6" y="11" width="4" height="4" fill={color} opacity="0.95" rx="0.5"/>
        <rect x="11" y="11" width="4" height="4" fill={color} opacity="0.35" rx="0.5"/>
    </svg>
);
const ThreeDIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="1" y="9" width="5" height="6" fill={color} opacity="0.65" rx="0.5"/>
        <rect x="7" y="4" width="5" height="11" fill={color} opacity="0.85" rx="0.5"/>
        <polygon points="1,9 4,5 9,5 7,9" fill={color} opacity="0.45"/>
        <polygon points="7,4 12,4 12,9 7,9" fill={color} opacity="0.55"/>
    </svg>
);
const HierarchyIcon = ({ color }) => (
    <svg width={S} height={S} viewBox="0 0 16 16" fill="none">
        <rect x="4" y="1" width="8" height="5" fill={color} opacity="0.85" rx="1"/>
        <rect x="1" y="10" width="6" height="5" fill={color} opacity="0.6" rx="1"/>
        <rect x="9" y="10" width="6" height="5" fill={color} opacity="0.6" rx="1"/>
        <line x1="8" y1="6" x2="4" y2="10" stroke={color} strokeWidth="1" opacity="0.45"/>
        <line x1="8" y1="6" x2="12" y2="10" stroke={color} strokeWidth="1" opacity="0.45"/>
    </svg>
);

const CHART_FAMILIES = [
    { name: 'Bar',           Icon: BarIcon,       types: [{ value: 'bar', label: 'Bar / Column' }] },
    { name: 'Line / Area',   Icon: LineIcon,      types: [{ value: 'line', label: 'Line' }, { value: 'area', label: 'Area' }, { value: 'sparkline', label: 'Sparkline' }] },
    { name: 'Pie / Donut',   Icon: PieIcon,       types: [{ value: 'pie', label: 'Pie' }, { value: 'donut', label: 'Donut' }] },
    { name: 'Scatter',       Icon: ScatterIcon,   types: [{ value: 'scatter', label: 'Scatter' }, { value: 'bubble', label: 'Bubble' }] },
    { name: 'Combo',         Icon: ComboIcon,     types: [{ value: 'combo', label: 'Combo' }] },
    { name: 'Waterfall',     Icon: WaterfallIcon, types: [{ value: 'waterfall', label: 'Waterfall' }, { value: 'range', label: 'Range' }] },
    { name: 'Statistical',   Icon: StatIcon,      types: [{ value: 'histogram', label: 'Histogram' }, { value: 'boxplot', label: 'Box Plot' }, { value: 'funnel', label: 'Funnel' }] },
    { name: 'Radial',        Icon: RadialIcon,    types: [{ value: 'radar', label: 'Radar' }, { value: 'nightingale', label: 'Nightingale' }] },
    { name: 'Heatmap',       Icon: HeatmapIcon,   types: [{ value: 'heatmap', label: 'Heatmap' }] },
    { name: '3D',            Icon: ThreeDIcon,    types: [{ value: 'bar3d', label: '3D Bar' }, { value: 'line3d', label: '3D Line' }, { value: 'scatter3d', label: '3D Scatter' }] },
    { name: 'Hierarchical',  Icon: HierarchyIcon, types: [{ value: 'icicle', label: 'Icicle' }, { value: 'sunburst', label: 'Sunburst' }, { value: 'sankey', label: 'Sankey' }], hierarchyOnly: true },
];

export const ChartTypeGallery = ({ chartType, onChange, theme, includeHierarchyCharts = true }) => {
    const [expandedFamily, setExpandedFamily] = useState(null);
    const families = includeHierarchyCharts ? CHART_FAMILIES : CHART_FAMILIES.filter(f => !f.hierarchyOnly);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
            {families.map(({ name, Icon, types }) => {
                const isActive = types.some(t => t.value === chartType);
                const activeType = types.find(t => t.value === chartType);
                const single = types.length === 1;
                const isExpanded = expandedFamily === name || isActive;
                const iconColor = isActive ? theme.primary : (theme.textSec || theme.text);
                return (
                    <div key={name}>
                        <button
                            type="button"
                            aria-expanded={!single ? isExpanded : undefined}
                            onClick={() => {
                                if (single) { onChange(types[0].value); }
                                else { setExpandedFamily(isExpanded && !isActive ? null : name); }
                            }}
                            style={{
                                width: '100%',
                                display: 'flex',
                                alignItems: 'center',
                                gap: '7px',
                                padding: '5px 8px',
                                border: `1px solid ${isActive ? theme.primary : theme.border}`,
                                background: isActive ? theme.select : (theme.headerSubtleBg || theme.hover),
                                color: isActive ? theme.primary : theme.text,
                                borderRadius: theme.radiusSm || '8px',
                                fontSize: '11px',
                                fontWeight: isActive ? 700 : 600,
                                cursor: 'pointer',
                                textAlign: 'left',
                            }}
                        >
                            <Icon color={iconColor} />
                            <span style={{ flex: 1 }}>{activeType ? activeType.label : name}</span>
                            {!single && (
                                <span style={{ fontSize: '8px', color: iconColor, transform: isExpanded ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.12s', flexShrink: 0 }}>▼</span>
                            )}
                        </button>
                        {!single && isExpanded && (
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px', padding: '4px 0 2px 24px' }}>
                                {types.map(t => (
                                    <button
                                        key={t.value}
                                        type="button"
                                        aria-pressed={chartType === t.value}
                                        onClick={() => { onChange(t.value); setExpandedFamily(null); }}
                                        style={{
                                            border: `1px solid ${chartType === t.value ? theme.primary : theme.border}`,
                                            background: chartType === t.value ? theme.select : (theme.headerSubtleBg || theme.hover),
                                            color: chartType === t.value ? theme.primary : theme.text,
                                            borderRadius: theme.radiusSm || '8px',
                                            padding: '4px 9px',
                                            fontSize: '10px',
                                            fontWeight: chartType === t.value ? 700 : 600,
                                            cursor: 'pointer',
                                        }}
                                    >
                                        {t.label}
                                    </button>
                                ))}
                            </div>
                        )}
                    </div>
                );
            })}
        </div>
    );
};

// ─── Reference line row with color + dash ─────────────────────────────────

export const ChartReflineRow = ({ line, onUpdate, onRemove, exportButtonStyle, compactInputStyle, theme }) => {
    return (
        <div style={{ display: 'flex', gap: '4px', alignItems: 'center', flexWrap: 'wrap' }}>
            <button
                type="button"
                onClick={() => onUpdate({ ...line, orient: line.orient === 'v' ? 'h' : 'v' })}
                style={{ ...exportButtonStyle, padding: '4px 7px', flexShrink: 0, fontWeight: 800, color: line.orient === 'v' ? theme.primary : theme.textSec }}
                title={line.orient === 'v' ? 'Vertical (click for horizontal)' : 'Horizontal (click for vertical)'}
            >
                {line.orient === 'v' ? '|' : '—'}
            </button>
            <input
                type="number"
                value={line.value}
                onChange={e => onUpdate({ ...line, value: e.target.value })}
                placeholder={line.orient === 'v' ? 'Cat index' : 'Value'}
                style={{ ...compactInputStyle, width: '60px', flexShrink: 0 }}
            />
            <input
                type="text"
                value={line.label}
                onChange={e => onUpdate({ ...line, label: e.target.value })}
                placeholder="Label"
                style={{ ...compactInputStyle, flex: 1, minWidth: 0 }}
            />
            <input
                type="color"
                value={line.color || theme.primary || '#2563EB'}
                onChange={e => onUpdate({ ...line, color: e.target.value })}
                style={{ width: '28px', height: '28px', padding: 0, border: `1px solid ${theme.border}`, borderRadius: '4px', cursor: 'pointer', background: 'none', flexShrink: 0 }}
                title="Line color"
            />
            <select
                value={line.dash || 'dashed'}
                onChange={e => onUpdate({ ...line, dash: e.target.value })}
                style={{ ...compactInputStyle, width: '74px', flexShrink: 0 }}
                title="Line style"
            >
                {DASH_STYLES.map(d => <option key={d.value} value={d.value}>{d.label}</option>)}
            </select>
            <button type="button" onClick={onRemove} style={{ ...exportButtonStyle, padding: '4px 7px', color: theme.danger || '#ef4444', flexShrink: 0 }}>✕</button>
        </div>
    );
};

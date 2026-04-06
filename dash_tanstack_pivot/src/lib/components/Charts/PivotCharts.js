import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Icons from '../Icons';
import { formatDisplayLabel } from '../../utils/helpers';
import {
    INTERNAL_COL_IDS, MAX_PANEL_CATEGORIES, MAX_PANEL_SERIES,
    MAX_SELECTION_CATEGORIES, MAX_SELECTION_SERIES,
    CHART_HEIGHT, CHART_WIDTH, DEFAULT_COLORS, COLOR_PALETTES,
    COLOR_PALETTE_NAMES, resolvePalette, VALID_COMBO_LAYER_TYPES,
    VALID_COMBO_LAYER_AXES, DEFAULT_COMBO_LAYER_SEQUENCE,
    VALUE_FORMAT_MODES,
    normalizePositiveLimit, formatChartNumber, formatChartValue,
    estimateTextWidth, truncateChartLabel,
    getColumnLabel, getColumnHeaderSegments, getColumnValueSegments,
    buildColumnLabelResolver, buildColumnFieldValues,
    getChartableColumns, getComboLayerDefaultName,
    buildDefaultCartesianLayers, buildGroupedChartColumnOptions,
    normalizeComboLayer, normalizeComboLayers,
    getRowLabel, getRowDepth, getRowLevel, getRowPath,
    buildRowFieldValues, buildRowTarget, buildColumnTarget,
    isDescendantPath, isTotalRow, getCellValue,
    getAvailableLevels, getHierarchyDisplayRows,
    getColorForIndex, buildSelectionBounds,
    buildCategoryBandsForRows, buildStackedGroupsForRows,
    buildFrontierStackedGroups, buildIcicleNodes,
    cloneHierarchyNodes, sortHierarchyNodes, sortChartModel,
    polarToCartesian, describeDonutArc, layoutSunburstNodes,
    buildHierarchySankey, layoutSankey,
    buildSeriesFromRowCategories, buildSeriesFromColumnCategories,
    buildSelectionChartModel, buildPivotChartModel,
    buildComboLayersForRows, buildComboEmptyModel,
    buildComboPivotChartModel, buildComboSelectionChartModel,
    buildLinePath, buildAreaBandPath, niceTickValues, getAxisMetrics,
    layoutIcicleNodes,
} from './chartModelBuilders';

// Re-export model builders consumed by DashTanstackPivot
export {
    buildDefaultCartesianLayers,
    normalizeComboLayers,
    buildPivotChartModel,
    buildComboPivotChartModel,
    buildComboSelectionChartModel,
    buildSelectionChartModel,
};

const ChartTypeButtons = ({ chartType, onChange, theme, includeHierarchyCharts = true }) => {
    const buttonStyle = (type) => ({
        border: `1px solid ${type === chartType ? theme.primary : theme.border}`,
        background: type === chartType ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: type === chartType ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button type="button" data-chart-type="bar" aria-pressed={chartType === 'bar'} onClick={() => onChange('bar')} style={buttonStyle('bar')}>Bar</button>
            <button type="button" data-chart-type="line" aria-pressed={chartType === 'line'} onClick={() => onChange('line')} style={buttonStyle('line')}>Line</button>
            <button type="button" data-chart-type="area" aria-pressed={chartType === 'area'} onClick={() => onChange('area')} style={buttonStyle('area')}>Area</button>
            <button type="button" data-chart-type="sparkline" aria-pressed={chartType === 'sparkline'} onClick={() => onChange('sparkline')} style={buttonStyle('sparkline')}>Sparkline</button>
            <button type="button" data-chart-type="combo" aria-pressed={chartType === 'combo'} onClick={() => onChange('combo')} style={buttonStyle('combo')}>Combo</button>
            <button type="button" data-chart-type="pie" aria-pressed={chartType === 'pie'} onClick={() => onChange('pie')} style={buttonStyle('pie')}>Pie</button>
            <button type="button" data-chart-type="donut" aria-pressed={chartType === 'donut'} onClick={() => onChange('donut')} style={buttonStyle('donut')}>Donut</button>
            <button type="button" data-chart-type="scatter" aria-pressed={chartType === 'scatter'} onClick={() => onChange('scatter')} style={buttonStyle('scatter')}>Scatter</button>
            <button type="button" data-chart-type="waterfall" aria-pressed={chartType === 'waterfall'} onClick={() => onChange('waterfall')} style={buttonStyle('waterfall')}>Waterfall</button>
            {includeHierarchyCharts ? (
                <>
                    <button type="button" data-chart-type="icicle" aria-pressed={chartType === 'icicle'} onClick={() => onChange('icicle')} style={buttonStyle('icicle')}>Icicle</button>
                    <button type="button" data-chart-type="sunburst" aria-pressed={chartType === 'sunburst'} onClick={() => onChange('sunburst')} style={buttonStyle('sunburst')}>Sunburst</button>
                    <button type="button" data-chart-type="sankey" aria-pressed={chartType === 'sankey'} onClick={() => onChange('sankey')} style={buttonStyle('sankey')}>Sankey</button>
                </>
            ) : null}
        </div>
    );
};

const canStackAreaSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
export const canStackBarSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
export const canStackBarLayout = (model) => (
    (Array.isArray(model && model.stackedGroups) && model.stackedGroups.length > 0)
    || canStackBarSeries(model)
);

const ChartLayoutButtons = ({ chartType, barLayout, onChange, canStack, theme }) => {
    const buttonStyle = (value, disabled = false) => ({
        border: `1px solid ${value === barLayout ? theme.primary : theme.border}`,
        background: value === barLayout ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === barLayout ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.45 : 1,
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('grouped')} style={buttonStyle('grouped')}>
                {chartType === 'area' ? 'Normal' : 'Grouped'}
            </button>
            <button
                onClick={() => {
                    if (!canStack) return;
                    onChange('stacked');
                }}
                style={buttonStyle('stacked', !canStack)}
                title={
                    canStack
                        ? (chartType === 'area' ? 'Stack area series' : 'Stack series or visible child rows')
                        : (chartType === 'area'
                            ? 'Stacked area needs at least two visible series'
                            : 'Stacked bars need multiple visible series or visible child groups')
                }
            >
                Stacked
            </button>
        </div>
    );
};

const ChartOrientationButtons = ({ orientation, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === orientation ? theme.primary : theme.border}`,
        background: value === orientation ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === orientation ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('rows')} style={buttonStyle('rows')}>By Rows</button>
            <button onClick={() => onChange('columns')} style={buttonStyle('columns')}>By Columns</button>
        </div>
    );
};

const ChartAxisButtons = ({ axisMode, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === axisMode ? theme.primary : theme.border}`,
        background: value === axisMode ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === axisMode ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('vertical')} style={buttonStyle('vertical')}>Vertical</button>
            <button onClick={() => onChange('horizontal')} style={buttonStyle('horizontal')}>Horizontal</button>
        </div>
    );
};

const ChartHierarchyButtons = ({ level, onChange, maxLevel, theme }) => {
    if (!maxLevel || maxLevel <= 1) return null;

    const buttonStyle = (value) => ({
        border: `1px solid ${value === level ? theme.primary : theme.border}`,
        background: value === level ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === level ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
        minWidth: '34px',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('all')} style={buttonStyle('all')}>All</button>
            {Array.from({ length: maxLevel }, (_, index) => index + 1).map((numericLevel) => (
                <button key={numericLevel} onClick={() => onChange(numericLevel)} style={buttonStyle(numericLevel)}>
                    {numericLevel}
                </button>
            ))}
        </div>
    );
};

const ChartHeader = ({ title, subtitle, note, theme }) => (
    !title && !subtitle && !note ? null : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            {title ? <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{title}</div> : null}
            {subtitle ? <div style={{ fontSize: '11px', color: theme.textSec }}>{subtitle}</div> : null}
            {note ? <div style={{ fontSize: '11px', color: theme.textSec }}>{note}</div> : null}
        </div>
    )
);

const ChartConfigSection = ({ title, theme, children, defaultCollapsed = true }) => {
    const [collapsed, setCollapsed] = useState(defaultCollapsed);
    return (
        <div style={{
            display: 'flex',
            flexDirection: 'column',
            gap: collapsed ? 0 : '8px',
            padding: collapsed ? '6px 10px' : '10px',
            border: `1px solid ${theme.border}`,
            borderRadius: theme.radiusSm || '8px',
            background: theme.surfaceBg || theme.background || '#fff',
            boxShadow: theme.shadowInset || 'none',
        }}>
            {title ? (
                <div
                    onClick={() => setCollapsed((v) => !v)}
                    style={{
                        paddingBottom: collapsed ? 0 : '6px',
                        borderBottom: collapsed ? 'none' : `1px solid ${theme.border}`,
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        userSelect: 'none',
                    }}
                >
                    <div style={{
                        fontSize: '10px',
                        fontWeight: 800,
                        letterSpacing: '0.08em',
                        textTransform: 'uppercase',
                        color: theme.textSec,
                    }}>
                        {title}
                    </div>
                    <span style={{
                        fontSize: '9px',
                        color: theme.textSec,
                        transform: collapsed ? 'rotate(-90deg)' : 'rotate(0deg)',
                        transition: 'transform 0.15s ease',
                    }}>
                        ▼
                    </span>
                </div>
            ) : null}
            {!collapsed ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {children}
                </div>
            ) : null}
        </div>
    );
};

const ChartConfigField = ({ label, theme, children, controlMinWidth = '180px' }) => (
    <div style={{
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        gap: '10px',
        flexWrap: 'wrap',
    }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '3px', flex: '1 1 180px', minWidth: '132px' }}>
            <div style={{ fontSize: '11px', fontWeight: 700, color: theme.text }}>
                {label}
            </div>
        </div>
        <div style={{
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'flex-start',
            flex: '1 1 210px',
            minWidth: controlMinWidth,
        }}>
            {children}
        </div>
    </div>
);

const ChartLimitInputs = ({
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    theme,
}) => {
    const inputStyle = {
        width: '58px',
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };
    const labelStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        color: theme.textSec,
        fontSize: '11px',
        fontWeight: 700,
    };
    const handleLimitChange = (callback, fallback) => (event) => {
        if (!callback) return;
        callback(normalizePositiveLimit(event.target.value, fallback));
    };

    return (
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
            <label style={labelStyle}>
                Rows
                <input
                    type="number"
                    min="1"
                    step="1"
                    value={rowLimit}
                    onChange={handleLimitChange(onRowLimitChange, MAX_PANEL_CATEGORIES)}
                    style={inputStyle}
                />
            </label>
            <label style={labelStyle}>
                Cols
                <input
                    type="number"
                    min="1"
                    step="1"
                    value={columnLimit}
                    onChange={handleLimitChange(onColumnLimitChange, MAX_PANEL_SERIES)}
                    style={inputStyle}
                />
            </label>
        </div>
    );
};

const ChartLayerEditor = ({
    layers,
    onChange,
    availableColumns,
    theme,
}) => {
    const chartableColumns = getChartableColumns(availableColumns);
    const normalizedLayers = normalizeComboLayers(layers, chartableColumns);
    const [searchValue, setSearchValue] = useState('');
    const columnOptions = useMemo(
        () => buildGroupedChartColumnOptions(chartableColumns),
        [chartableColumns]
    );
    const optionById = useMemo(
        () => new Map(columnOptions.map((option) => [option.id, option])),
        [columnOptions]
    );
    const normalizedSearchValue = searchValue.trim().toLowerCase();
    const filteredOptions = useMemo(
        () => (
            normalizedSearchValue
                ? columnOptions.filter((option) => option.searchText.includes(normalizedSearchValue))
                : columnOptions
        ),
        [columnOptions, normalizedSearchValue]
    );
    const groupedFilteredOptions = useMemo(() => {
        const groups = new Map();
        filteredOptions.forEach((option) => {
            if (!groups.has(option.groupLabel)) groups.set(option.groupLabel, []);
            groups.get(option.groupLabel).push(option);
        });
        return Array.from(groups.entries()).map(([label, options]) => ({ label, options }));
    }, [filteredOptions]);
    const inputStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };
    const updateLayer = (layerId, patch) => {
        if (typeof onChange !== 'function') return;
        onChange(normalizedLayers.map((layer) => (
            layer.id === layerId ? { ...layer, ...(patch || {}) } : layer
        )));
    };
    const moveLayer = (layerId, direction) => {
        if (typeof onChange !== 'function') return;
        const layerIndex = normalizedLayers.findIndex((layer) => layer.id === layerId);
        const nextIndex = layerIndex + direction;
        if (layerIndex < 0 || nextIndex < 0 || nextIndex >= normalizedLayers.length) return;
        const nextLayers = [...normalizedLayers];
        const [movedLayer] = nextLayers.splice(layerIndex, 1);
        nextLayers.splice(nextIndex, 0, movedLayer);
        onChange(nextLayers);
    };
    const removeLayer = (layerId) => {
        if (typeof onChange !== 'function') return;
        const nextLayers = normalizedLayers.filter((layer) => layer.id !== layerId);
        onChange(nextLayers.length > 0 ? nextLayers : buildDefaultCartesianLayers(chartableColumns, 1));
    };
    const addLayer = () => {
        if (typeof onChange !== 'function') return;
        const usedColumnIds = new Set(normalizedLayers.map((layer) => layer.columnId));
        const nextColumn = chartableColumns.find((column) => !usedColumnIds.has(column.id)) || chartableColumns[0];
        if (!nextColumn) return;
        const nextLayer = normalizeComboLayer({
            id: `layer-${normalizedLayers.length + 1}`,
            type: DEFAULT_COMBO_LAYER_SEQUENCE[normalizedLayers.length % DEFAULT_COMBO_LAYER_SEQUENCE.length],
            columnId: nextColumn.id,
            axis: normalizedLayers.length === 1 ? 'right' : 'left',
            name: getColumnLabel(nextColumn),
        }, chartableColumns, normalizedLayers.length);
        onChange([...normalizedLayers, nextLayer]);
    };

    if (chartableColumns.length === 0) {
        return (
            <div style={{ fontSize: '11px', color: theme.textSec }}>
                No visible numeric/value columns are available for combo layers.
            </div>
        );
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', width: '100%' }}>
            <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '8px',
                padding: '10px',
                border: `1px solid ${theme.border}`,
                borderRadius: theme.radiusSm || '8px',
                background: theme.headerSubtleBg || theme.hover,
            }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '3px', flex: '1 1 240px', minWidth: '220px' }}>
                    <div style={{ fontSize: '11px', fontWeight: 800, color: theme.text }}>
                        Measure Search
                    </div>
                    <div style={{ fontSize: '10px', color: theme.textSec }}>
                        Search across all available chart measures and grouped pivot columns.
                    </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: '1 1 320px', minWidth: '240px', justifyContent: 'flex-end' }}>
                    <input
                        type="search"
                        value={searchValue}
                        onChange={(event) => setSearchValue(event.target.value)}
                        placeholder="Search measures, groups, or column ids"
                        style={{ ...inputStyle, width: '100%', maxWidth: '360px' }}
                    />
                    <div style={{ fontSize: '10px', fontWeight: 700, color: theme.textSec, whiteSpace: 'nowrap' }}>
                        {filteredOptions.length} / {columnOptions.length}
                    </div>
                </div>
            </div>
            {filteredOptions.length === 0 ? (
                <div style={{ fontSize: '11px', color: theme.textSec, padding: '2px 2px 0 2px' }}>
                    No measures match the current search. Clear the search to see the full catalog.
                </div>
            ) : null}
            {normalizedLayers.map((layer, layerIndex) => {
                const selectedOption = optionById.get(layer.columnId) || null;
                const selectedVisible = groupedFilteredOptions.some((group) => (
                    group.options.some((option) => option.id === layer.columnId)
                ));
                return (
                <div
                    key={layer.id}
                    style={{
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '10px',
                        width: '100%',
                        padding: '12px',
                        border: `1px solid ${theme.border}`,
                        borderRadius: theme.radiusSm || '8px',
                        background: theme.surfaceBg || theme.background || '#fff',
                        boxShadow: theme.shadowInset || 'none',
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '10px', flexWrap: 'wrap' }}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', minWidth: '180px' }}>
                            <div style={{ fontSize: '11px', fontWeight: 800, color: theme.text }}>
                                {`Layer ${layerIndex + 1}`}
                            </div>
                            <div style={{ fontSize: '11px', color: theme.textSec }}>
                                {selectedOption ? selectedOption.fullLabel : (layer.name || 'Select a measure')}
                            </div>
                            {selectedOption ? (
                                <div style={{ fontSize: '10px', color: theme.textSec }}>
                                    {`${selectedOption.groupLabel} | ${selectedOption.id}`}
                                </div>
                            ) : null}
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
                            <button
                                type="button"
                                onClick={() => moveLayer(layer.id, -1)}
                                disabled={layerIndex === 0}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    background: theme.headerSubtleBg || theme.hover,
                                    color: theme.textSec,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    cursor: layerIndex === 0 ? 'not-allowed' : 'pointer',
                                    opacity: layerIndex === 0 ? 0.45 : 1,
                                }}
                            >
                                Up
                            </button>
                            <button
                                type="button"
                                onClick={() => moveLayer(layer.id, 1)}
                                disabled={layerIndex === normalizedLayers.length - 1}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    background: theme.headerSubtleBg || theme.hover,
                                    color: theme.textSec,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    cursor: layerIndex === normalizedLayers.length - 1 ? 'not-allowed' : 'pointer',
                                    opacity: layerIndex === normalizedLayers.length - 1 ? 0.45 : 1,
                                }}
                            >
                                Down
                            </button>
                            <button
                                type="button"
                                onClick={() => removeLayer(layer.id)}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    background: theme.headerSubtleBg || theme.hover,
                                    color: theme.textSec,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    cursor: 'pointer',
                                }}
                            >
                                Remove
                            </button>
                        </div>
                    </div>
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                        gap: '8px',
                        alignItems: 'start',
                        width: '100%',
                    }}>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: '5px', minWidth: 0 }}>
                            <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: theme.textSec }}>
                                Measure
                            </span>
                            <select
                                value={layer.columnId || ''}
                                onChange={(event) => {
                                    const nextOption = optionById.get(event.target.value) || null;
                                    updateLayer(layer.id, {
                                        columnId: event.target.value,
                                        name: nextOption ? nextOption.fullLabel : layer.name,
                                    });
                                }}
                                style={inputStyle}
                            >
                                {!selectedVisible && selectedOption ? (
                                    <optgroup label="Current Layer Measure">
                                        <option value={selectedOption.id}>{selectedOption.fullLabel}</option>
                                    </optgroup>
                                ) : null}
                                {groupedFilteredOptions.map((group) => (
                                    <optgroup key={group.label} label={`${group.label} (${group.options.length})`}>
                                        {group.options.map((option) => (
                                            <option key={option.id} value={option.id}>
                                                {option.leafLabel}
                                            </option>
                                        ))}
                                    </optgroup>
                                ))}
                            </select>
                        </label>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                            <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: theme.textSec }}>
                                Layer Type
                            </span>
                            <select
                                value={layer.type}
                                onChange={(event) => updateLayer(layer.id, { type: event.target.value })}
                                style={inputStyle}
                            >
                                <option value="bar">Bar</option>
                                <option value="line">Line</option>
                                <option value="area">Area</option>
                            </select>
                        </label>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                            <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: theme.textSec }}>
                                Axis
                            </span>
                            <select
                                value={layer.axis}
                                onChange={(event) => updateLayer(layer.id, { axis: event.target.value })}
                                style={inputStyle}
                            >
                                <option value="left">Left Axis</option>
                                <option value="right">Right Axis</option>
                            </select>
                        </label>
                    </div>
                </div>
                );
            })}
            <div>
                <button
                    type="button"
                    onClick={addLayer}
                    style={{
                        border: `1px solid ${theme.primary}`,
                        background: theme.select,
                        color: theme.primary,
                        borderRadius: theme.radiusSm || '8px',
                        padding: '6px 10px',
                        fontSize: '11px',
                        fontWeight: 700,
                        cursor: 'pointer',
                    }}
                >
                    Add Layer
                </button>
            </div>
        </div>
    );
};

const ChartSortButtons = ({ sortMode, onChange, theme }) => {
    const options = [
        { value: 'natural', label: 'Natural' },
        { value: 'value_desc', label: 'Value Desc' },
        { value: 'value_asc', label: 'Value Asc' },
        { value: 'label_asc', label: 'Label A-Z' },
        { value: 'label_desc', label: 'Label Z-A' },
    ];

    return (
        <select
            value={sortMode}
            onChange={(event) => onChange(event.target.value)}
            style={{
                border: `1px solid ${theme.border}`,
                background: theme.surfaceBg || theme.background || '#fff',
                color: theme.text,
                borderRadius: theme.radiusSm || '8px',
                padding: '6px 8px',
                fontSize: '11px',
                fontWeight: 700,
                minWidth: '132px',
            }}
            title="Chart ordering"
        >
            {options.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
            ))}
        </select>
    );
};

const ChartInteractionButtons = ({ interactionMode, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === interactionMode ? theme.primary : theme.border}`,
        background: value === interactionMode ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === interactionMode ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('focus')} style={buttonStyle('focus')} title="Jump the grid to the matching row or column">Jump to Grid</button>
            <button onClick={() => onChange('filter')} style={buttonStyle('filter')} title="Apply the clicked chart item as a pivot filter">Filter Grid</button>
            <button onClick={() => onChange('event')} style={buttonStyle('event')} title="Send the chart click to Dash only and leave the grid unchanged">No Grid Change</button>
        </div>
    );
};

const ChartScopeButtons = ({ scope, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === scope ? theme.primary : theme.border}`,
        background: value === scope ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === scope ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('viewport')} style={buttonStyle('viewport')}>Viewport</button>
            <button onClick={() => onChange('root')} style={buttonStyle('root')}>From Start</button>
        </div>
    );
};

const downloadBlob = (blob, filename) => {
    if (typeof window === 'undefined' || !blob) return;
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    window.URL.revokeObjectURL(url);
};

const exportSvgNode = async (svgNode, fileStem) => {
    if (!svgNode || typeof window === 'undefined') return;
    const serializer = new XMLSerializer();
    const svgText = serializer.serializeToString(svgNode);
    const svgBlob = new Blob([svgText], { type: 'image/svg+xml;charset=utf-8' });
    downloadBlob(svgBlob, `${fileStem}.svg`);
};

const svgToCanvas = (svgNode, scale = 2) => new Promise((resolve) => {
    if (!svgNode) { resolve(null); return; }
    const serializer = new XMLSerializer();
    const svgText = serializer.serializeToString(svgNode);
    const viewBox = svgNode.getAttribute('viewBox');
    let w = svgNode.clientWidth || 800;
    let h = svgNode.clientHeight || 400;
    if (viewBox) {
        const parts = viewBox.split(/[\s,]+/).map(Number);
        if (parts.length === 4 && parts[2] > 0 && parts[3] > 0) { w = parts[2]; h = parts[3]; }
    }
    const canvas = document.createElement('canvas');
    canvas.width = w * scale;
    canvas.height = h * scale;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    const img = new Image();
    const blob = new Blob([svgText], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    img.onload = () => { ctx.drawImage(img, 0, 0, canvas.width, canvas.height); URL.revokeObjectURL(url); resolve(canvas); };
    img.onerror = () => { URL.revokeObjectURL(url); resolve(null); };
    img.src = url;
});

const exportSvgAsPng = async (svgNode, fileStem) => {
    const canvas = await svgToCanvas(svgNode);
    if (!canvas) return;
    canvas.toBlob((blob) => { if (blob) downloadBlob(blob, `${fileStem}.png`); }, 'image/png');
};

const copySvgToClipboard = async (svgNode) => {
    if (!navigator.clipboard || !navigator.clipboard.write) return false;
    const canvas = await svgToCanvas(svgNode);
    if (!canvas) return false;
    try {
        const blob = await new Promise((resolve) => canvas.toBlob(resolve, 'image/png'));
        if (!blob) return false;
        await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })]);
        return true;
    } catch (_) { return false; }
};

const exportChartCsv = (model, fileStem) => {
    if (!model) return;
    const categories = Array.isArray(model.categories) ? model.categories : [];
    const series = Array.isArray(model.series) ? model.series : [];
    if (categories.length === 0 || series.length === 0) return;
    const escape = (v) => { const s = String(v ?? ''); return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s; };
    const header = ['Category', ...series.map((s) => s.name)].map(escape).join(',');
    const rows = categories.map((cat, i) => [cat, ...series.map((s) => s.values[i] ?? '')].map(escape).join(','));
    const csv = [header, ...rows].join('\n');
    downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8' }), `${fileStem}.csv`);
};

const EmptyChartState = ({ message, theme, chartHeight = CHART_HEIGHT }) => (
    <div style={{
        minHeight: `${chartHeight}px`,
        border: `1px dashed ${theme.border}`,
        borderRadius: theme.radius || '14px',
        background: theme.headerSubtleBg || theme.hover,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '16px',
        color: theme.textSec,
        fontSize: '12px',
        textAlign: 'center',
    }}>
        {message}
    </div>
);

const ChartStats = ({ model, theme }) => {
    const metrics = useMemo(() => {
        const values = [];
        (model && model.series ? model.series : []).forEach((series) => {
            (series.values || []).forEach((value) => {
                if (typeof value === 'number' && Number.isFinite(value)) values.push(value);
            });
        });
        if (values.length === 0 && model && Array.isArray(model.icicleNodes)) {
            model.icicleNodes.forEach((node) => {
                const value = Number(node && node.value);
                if (Number.isFinite(value)) values.push(value);
            });
        }
        if (values.length === 0) return null;

        return {
            categories: Array.isArray(model.categories) ? model.categories.length : 0,
            seriesCount: Array.isArray(model.series) ? model.series.length : 0,
            hierarchyNodes: Array.isArray(model.icicleNodes) ? model.icicleNodes.length : 0,
            min: Math.min(...values),
            max: Math.max(...values),
        };
    }, [model]);

    if (!metrics) return null;

    const chipStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.hover,
        color: theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        padding: '5px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <span style={chipStyle}>{metrics.categories} categories</span>
            <span style={chipStyle}>{metrics.seriesCount} series</span>
            {metrics.hierarchyNodes > 0 ? <span style={chipStyle}>{metrics.hierarchyNodes} nodes</span> : null}
            <span style={chipStyle}>Min {formatChartNumber(metrics.min)}</span>
            <span style={chipStyle}>Max {formatChartNumber(metrics.max)}</span>
        </div>
    );
};

const ChartLegend = ({ items, theme, hiddenSet, onToggle }) => {
    if (!Array.isArray(items) || items.length === 0) return null;
    const canToggle = typeof onToggle === 'function' && hiddenSet;

    return (
        <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
            {items.map((item) => {
                const isHidden = canToggle && hiddenSet.has(item.label);
                return (
                    <div
                        key={item.label}
                        onClick={canToggle ? () => onToggle(item.label) : undefined}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '6px', fontSize: '11px',
                            color: theme.textSec,
                            cursor: canToggle ? 'pointer' : 'default',
                            opacity: isHidden ? 0.35 : 1,
                            textDecoration: isHidden ? 'line-through' : 'none',
                            userSelect: 'none',
                        }}
                    >
                        <span style={{ width: '10px', height: '10px', borderRadius: '999px', background: isHidden ? theme.border : item.color, display: 'inline-block' }} />
                        <span>{item.label}</span>
                    </div>
                );
            })}
        </div>
    );
};

const buildSparklineGeometry = (values, width, height, padding = 10) => {
    const numericValues = Array.isArray(values)
        ? values.map((value) => (Number.isFinite(Number(value)) ? Number(value) : null))
        : [];
    const definedPoints = numericValues
        .map((value, index) => (value === null ? null : { value, index }))
        .filter(Boolean);

    if (definedPoints.length === 0) {
        return {
            linePath: '',
            areaPath: '',
            currentPoint: null,
            minPoint: null,
            maxPoint: null,
            minValue: null,
            maxValue: null,
            firstValue: null,
            lastValue: null,
        };
    }

    const minValue = Math.min(...definedPoints.map((point) => point.value));
    const maxValue = Math.max(...definedPoints.map((point) => point.value));
    const valueRange = Math.max(1, maxValue - minValue);
    const stepX = definedPoints.length > 1
        ? (width - (padding * 2)) / (definedPoints.length - 1)
        : 0;

    const points = definedPoints.map((point, pointIndex) => {
        const x = padding + (pointIndex * stepX);
        const normalized = (point.value - minValue) / valueRange;
        const y = height - padding - (normalized * Math.max(1, height - (padding * 2)));
        return {
            ...point,
            x,
            y,
        };
    });

    const linePath = points
        .map((point, pointIndex) => `${pointIndex === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
        .join(' ');
    const areaPath = points.length > 1
        ? `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(height - padding).toFixed(2)} L ${points[0].x.toFixed(2)} ${(height - padding).toFixed(2)} Z`
        : '';
    const minPoint = points.find((point) => point.value === minValue) || points[0];
    const maxPoint = points.find((point) => point.value === maxValue) || points[0];
    const currentPoint = points[points.length - 1];

    return {
        linePath,
        areaPath,
        currentPoint,
        minPoint,
        maxPoint,
        minValue,
        maxValue,
        firstValue: points[0] ? points[0].value : null,
        lastValue: currentPoint ? currentPoint.value : null,
    };
};

const SparklineBoard = ({
    model,
    theme,
    chartHeight,
    paletteColors,
    valueFormat,
}) => {
    const seriesList = Array.isArray(model && model.series) ? model.series : [];
    const categories = Array.isArray(model && model.categories) ? model.categories : [];

    if (seriesList.length === 0) {
        return <EmptyChartState message={model && model.emptyMessage ? model.emptyMessage : 'No sparkline data available.'} theme={theme} chartHeight={chartHeight} />;
    }

    const cardHeight = 110;
    const sparkWidth = 240;
    const sparkHeight = 64;

    return (
        <div
            data-chart-sparkline-board="true"
            style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
                gap: '10px',
                minHeight: 0,
                maxHeight: `${chartHeight}px`,
                overflowY: 'auto',
                paddingRight: '2px',
            }}
        >
            {seriesList.map((series, index) => {
                const color = getColorForIndex(index, theme, paletteColors);
                const geometry = buildSparklineGeometry(series.values || [], sparkWidth, sparkHeight, 10);
                const previousValue = Array.isArray(series.values) && series.values.length > 1
                    ? Number(series.values[series.values.length - 2])
                    : null;
                const currentValue = geometry.lastValue;
                const delta = Number.isFinite(previousValue) && Number.isFinite(currentValue)
                    ? currentValue - previousValue
                    : null;
                const deltaTone = delta === null
                    ? theme.textSec
                    : delta >= 0
                        ? (theme.isDark ? '#7DD3A7' : '#2E7D5B')
                        : (theme.isDark ? '#F7A3A3' : '#B45353');

                return (
                    <div
                        key={series.id || series.name || `spark-${index}`}
                        data-chart-sparkline-card={series.id || series.name || `spark-${index}`}
                        style={{
                            minHeight: `${cardHeight}px`,
                            border: `1px solid ${theme.border}`,
                            borderRadius: theme.radiusSm || '10px',
                            background: theme.surfaceBg || theme.background || '#fff',
                            boxShadow: theme.shadowInset || 'none',
                            padding: '10px 12px',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '8px',
                        }}
                    >
                        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '10px' }}>
                            <div style={{ minWidth: 0 }}>
                                <div style={{ fontSize: '12px', fontWeight: 800, color: theme.text, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                    {series.name || `Series ${index + 1}`}
                                </div>
                                <div style={{ fontSize: '10px', color: theme.textSec }}>
                                    {categories.length > 0 ? `${categories.length} points` : 'Trend'}
                                </div>
                            </div>
                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '2px', flexShrink: 0 }}>
                                <div style={{ fontSize: '12px', fontWeight: 800, color: theme.text }}>
                                    {currentValue === null ? '—' : formatChartValue(currentValue, valueFormat)}
                                </div>
                                <div style={{ fontSize: '10px', fontWeight: 700, color: deltaTone }}>
                                    {delta === null ? 'No delta' : `${delta >= 0 ? '+' : ''}${formatChartValue(delta, valueFormat)}`}
                                </div>
                            </div>
                        </div>
                        <svg
                            viewBox={`0 0 ${sparkWidth} ${sparkHeight}`}
                            style={{ width: '100%', height: '64px', display: 'block' }}
                            aria-label={`${series.name || `Series ${index + 1}`} sparkline`}
                        >
                            {geometry.areaPath ? <path d={geometry.areaPath} fill={color} opacity="0.12" /> : null}
                            {geometry.linePath ? (
                                <path
                                    d={geometry.linePath}
                                    fill="none"
                                    stroke={color}
                                    strokeWidth="2.5"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                />
                            ) : null}
                            {geometry.minPoint ? <circle cx={geometry.minPoint.x} cy={geometry.minPoint.y} r="2.6" fill={theme.surfaceBg || theme.background || '#fff'} stroke={color} strokeWidth="1.4" opacity="0.7" /> : null}
                            {geometry.maxPoint ? <circle cx={geometry.maxPoint.x} cy={geometry.maxPoint.y} r="2.8" fill={theme.surfaceBg || theme.background || '#fff'} stroke={color} strokeWidth="1.6" opacity="0.9" /> : null}
                            {geometry.currentPoint ? <circle cx={geometry.currentPoint.x} cy={geometry.currentPoint.y} r="3.2" fill={color} /> : null}
                        </svg>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', fontSize: '10px', color: theme.textSec }}>
                            <span>{categories[0] ? truncateChartLabel(String(categories[0]), 14) : 'Start'}</span>
                            <span>
                                Min {geometry.minValue === null ? '—' : formatChartValue(geometry.minValue, valueFormat)}
                                {' · '}
                                Max {geometry.maxValue === null ? '—' : formatChartValue(geometry.maxValue, valueFormat)}
                            </span>
                            <span>{categories.length > 0 ? truncateChartLabel(String(categories[categories.length - 1]), 14) : 'End'}</span>
                        </div>
                    </div>
                );
            })}
        </div>
    );
};

const SvgChart = ({
    model,
    chartType,
    barLayout,
    axisMode,
    theme,
    chartHeight = CHART_HEIGHT,
    showLegend = true,
    showDataLabels = false,
    onCategoryActivate = null,
    svgRef = null,
    colorPalette = 'default',
    valueFormat = 'auto',
    yAxisTitle = '',
    chartTitle = '',
    onTitleChange = null,
}) => {
    const paletteColors = resolvePalette(colorPalette);
    const fmt = (v) => formatChartValue(v, valueFormat);
    const stackedChildBarMode = chartType === 'bar' && barLayout === 'stacked' && Array.isArray(model.stackedGroups) && model.stackedGroups.length > 0;
    const stackedSeriesBarMode = chartType === 'bar' && barLayout === 'stacked' && !stackedChildBarMode && canStackBarSeries(model);
    const stackedAreaMode = chartType === 'area' && barLayout === 'stacked' && canStackAreaSeries(model);
    const horizontalBarMode = chartType === 'bar' && axisMode === 'horizontal';
    const effectiveChartHeight = chartHeight;
    const hierarchyNodes = Array.isArray(model && model.icicleNodes) ? model.icicleNodes : [];
    const icicleDepth = Math.max(1, Number(model && model.icicleDepth) || 1);
    const chartContainerRef = useRef(null);
    const [measuredChartWidth, setMeasuredChartWidth] = useState(CHART_WIDTH);
    const resolvedChartWidth = Math.max(CHART_WIDTH, Number.isFinite(measuredChartWidth) ? Math.floor(measuredChartWidth) : CHART_WIDTH);

    useEffect(() => {
        const element = chartContainerRef.current;
        if (!element) return undefined;
        const updateWidth = () => {
            const nextWidth = element.clientWidth;
            if (nextWidth > 0) setMeasuredChartWidth(nextWidth);
        };
        updateWidth();
        if (typeof ResizeObserver === 'undefined') return undefined;
        const observer = new ResizeObserver(updateWidth);
        observer.observe(element);
        return () => observer.disconnect();
    }, []);

    const [hiddenSeries, setHiddenSeries] = useState(new Set());
    const toggleHiddenSeries = useCallback((label) => {
        setHiddenSeries((prev) => {
            const next = new Set(prev);
            if (next.has(label)) next.delete(label); else next.add(label);
            return next;
        });
    }, []);
    const [tooltipInfo, setTooltipInfo] = useState(null);
    const handleChartMouseMove = useCallback((e) => {
        const target = e.target;
        const category = target.getAttribute('data-tip-cat');
        if (!category) {
            setTooltipInfo((prev) => prev ? null : prev);
            return;
        }
        const container = e.currentTarget;
        const rect = container.getBoundingClientRect();
        setTooltipInfo({
            x: e.clientX - rect.left,
            y: e.clientY - rect.top,
            category,
            series: target.getAttribute('data-tip-ser') || '',
            value: target.getAttribute('data-tip-val') || '',
            color: target.getAttribute('data-tip-color') || '',
        });
    }, []);
    const handleChartMouseLeave = useCallback(() => setTooltipInfo(null), []);
    const tooltipElement = tooltipInfo ? (
        <div style={{
            position: 'absolute',
            left: Math.min(tooltipInfo.x + 14, resolvedChartWidth - 160),
            top: Math.max(0, tooltipInfo.y - 10),
            pointerEvents: 'none',
            zIndex: 10,
            background: theme.surfaceBg || theme.background || '#fff',
            border: `1px solid ${theme.border}`,
            borderRadius: theme.radiusSm || '8px',
            padding: '6px 10px',
            fontSize: '11px',
            fontWeight: 600,
            color: theme.text,
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            whiteSpace: 'nowrap',
        }}>
            <div style={{ fontWeight: 800, marginBottom: tooltipInfo.series ? '2px' : 0 }}>{tooltipInfo.category}</div>
            {tooltipInfo.series ? (
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                    {tooltipInfo.color ? (
                        <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: tooltipInfo.color, flexShrink: 0 }} />
                    ) : null}
                    <span>{tooltipInfo.series}: {tooltipInfo.value}</span>
                </div>
            ) : (
                tooltipInfo.value ? <div>{tooltipInfo.value}</div> : null
            )}
        </div>
    ) : null;
    const [zoomScale, setZoomScale] = useState(1);
    const [panOffset, setPanOffset] = useState({ x: 0, y: 0 });
    const panDrag = useRef(null);
    const [crosshairPos, setCrosshairPos] = useState(null);

    const handleChartWheel = useCallback((e) => {
        e.preventDefault();
        setZoomScale((prev) => {
            const next = e.deltaY < 0 ? prev * 1.15 : prev / 1.15;
            const clamped = Math.min(10, Math.max(1, next));
            if (clamped === 1) setPanOffset({ x: 0, y: 0 });
            return clamped;
        });
    }, []);
    const handlePanStart = useCallback((e) => {
        if (zoomScale <= 1 || e.button !== 0) return;
        panDrag.current = { startX: e.clientX, startY: e.clientY, startPan: { ...panOffset } };
    }, [zoomScale, panOffset]);
    const handlePanMove = useCallback((e) => {
        if (!panDrag.current) return;
        const dx = e.clientX - panDrag.current.startX;
        const dy = e.clientY - panDrag.current.startY;
        setPanOffset({ x: panDrag.current.startPan.x + dx, y: panDrag.current.startPan.y + dy });
    }, []);
    const handlePanEnd = useCallback(() => { panDrag.current = null; }, []);
    const handleDblClick = useCallback(() => { setZoomScale(1); setPanOffset({ x: 0, y: 0 }); }, []);

    const wrappedMouseMove = useCallback((e) => {
        handleChartMouseMove(e);
        handlePanMove(e);
        const svgEl = svgRef && svgRef.current;
        if (svgEl && typeof svgEl.getScreenCTM === 'function') {
            const ctm = svgEl.getScreenCTM();
            if (ctm) {
                const pt = svgEl.createSVGPoint();
                pt.x = e.clientX; pt.y = e.clientY;
                const svgPt = pt.matrixTransform(ctm.inverse());
                setCrosshairPos({ x: svgPt.x, y: svgPt.y });
            }
        }
    }, [handleChartMouseMove, handlePanMove]);
    const wrappedMouseLeave = useCallback(() => {
        handleChartMouseLeave();
        handlePanEnd();
        setCrosshairPos(null);
    }, [handleChartMouseLeave, handlePanEnd]);

    const chartContainerEvents = {
        onMouseMove: wrappedMouseMove,
        onMouseLeave: wrappedMouseLeave,
        onWheel: handleChartWheel,
        onMouseDown: handlePanStart,
        onMouseUp: handlePanEnd,
        onDoubleClick: handleDblClick,
    };

    const zoomTransform = zoomScale > 1 ? `scale(${zoomScale}) translate(${panOffset.x / zoomScale}px, ${panOffset.y / zoomScale}px)` : undefined;
    const zoomContainerStyle = zoomScale > 1 ? { overflow: 'hidden', cursor: 'grab' } : undefined;
    const zoomSvgStyle = zoomScale > 1 ? { transform: zoomTransform, transformOrigin: 'center center' } : undefined;
    const crosshairElement = crosshairPos && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' ? (
        <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none', zIndex: 5 }} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} preserveAspectRatio="xMidYMid meet">
            <line x1={crosshairPos.x} y1={0} x2={crosshairPos.x} y2={effectiveChartHeight} stroke={theme.textSec} strokeOpacity="0.3" strokeDasharray="4 3" />
            <line x1={0} y1={crosshairPos.y} x2={resolvedChartWidth} y2={crosshairPos.y} stroke={theme.textSec} strokeOpacity="0.3" strokeDasharray="4 3" />
        </svg>
    ) : null;
    const zoomBadge = zoomScale > 1.05 ? (
        <div style={{ position: 'absolute', top: 4, right: 4, zIndex: 12, background: theme.surfaceBg || '#fff', border: `1px solid ${theme.border}`, borderRadius: theme.radiusSm || '8px', padding: '2px 7px', fontSize: '10px', fontWeight: 700, color: theme.textSec, pointerEvents: 'auto', cursor: 'pointer' }} onClick={handleDblClick} title="Reset zoom (or double-click chart)">
            {Math.round(zoomScale * 100)}% ✕
        </div>
    ) : null;
    const [editingTitle, setEditingTitle] = useState(false);
    const [editTitleDraft, setEditTitleDraft] = useState('');
    const titleInputRef = useRef(null);
    const [titlePos, setTitlePos] = useState(null);
    const titleDragRef = useRef(null);

    useEffect(() => {
        const handleDragMove = (event) => {
            if (!titleDragRef.current) return;
            const { startX, startY, startPos, containerRect } = titleDragRef.current;
            const dx = event.clientX - startX;
            const dy = event.clientY - startY;
            const nextX = Math.max(0, Math.min(containerRect.width - 40, startPos.x + dx));
            const nextY = Math.max(0, Math.min(containerRect.height - 20, startPos.y + dy));
            setTitlePos({ x: nextX, y: nextY });
        };
        const handleDragEnd = () => { titleDragRef.current = null; };
        window.addEventListener('mousemove', handleDragMove);
        window.addEventListener('mouseup', handleDragEnd);
        return () => {
            window.removeEventListener('mousemove', handleDragMove);
            window.removeEventListener('mouseup', handleDragEnd);
        };
    }, []);

    const handleTitleDragStart = (event) => {
        if (editingTitle) return;
        event.preventDefault();
        const container = event.currentTarget.parentElement;
        if (!container) return;
        const containerRect = container.getBoundingClientRect();
        const currentPos = titlePos || {
            x: (containerRect.width / 2) - 40,
            y: 6,
        };
        titleDragRef.current = {
            startX: event.clientX,
            startY: event.clientY,
            startPos: currentPos,
            containerRect,
        };
    };

    const handleTitleDoubleClick = () => {
        if (typeof onTitleChange !== 'function') return;
        setEditTitleDraft(chartTitle || '');
        setEditingTitle(true);
    };
    const commitTitleEdit = () => {
        if (typeof onTitleChange === 'function') onTitleChange(editTitleDraft);
        setEditingTitle(false);
    };

    useEffect(() => {
        if (editingTitle && titleInputRef.current) titleInputRef.current.focus();
    }, [editingTitle]);

    const titleStyle = titlePos
        ? { left: `${titlePos.x}px`, top: `${titlePos.y}px` }
        : { left: '50%', top: '6px', transform: 'translateX(-50%)' };

    const chartTitleOverlay = chartTitle || editingTitle ? (
        <div
            onMouseDown={handleTitleDragStart}
            onDoubleClick={handleTitleDoubleClick}
            style={{
                position: 'absolute',
                ...titleStyle,
                zIndex: 6,
                cursor: editingTitle ? 'text' : 'grab',
                userSelect: editingTitle ? 'auto' : 'none',
            }}
        >
            {editingTitle ? (
                <input
                    ref={titleInputRef}
                    type="text"
                    value={editTitleDraft}
                    onChange={(event) => setEditTitleDraft(event.target.value)}
                    onBlur={commitTitleEdit}
                    onKeyDown={(event) => {
                        if (event.key === 'Enter') commitTitleEdit();
                        if (event.key === 'Escape') setEditingTitle(false);
                    }}
                    onMouseDown={(event) => event.stopPropagation()}
                    style={{
                        fontSize: '13px',
                        fontWeight: 700,
                        color: theme.text,
                        background: theme.surfaceBg || theme.background || '#fff',
                        border: `1.5px solid ${theme.primary}`,
                        borderRadius: theme.radiusSm || '6px',
                        padding: '2px 10px',
                        outline: 'none',
                        minWidth: '80px',
                    }}
                />
            ) : (
                <span style={{
                    fontSize: '13px',
                    fontWeight: 700,
                    color: theme.text,
                    background: `${theme.surfaceBg || theme.background || '#fff'}cc`,
                    padding: '2px 10px',
                    borderRadius: theme.radiusSm || '6px',
                    whiteSpace: 'nowrap',
                    display: 'inline-block',
                }}>{chartTitle}</span>
            )}
        </div>
    ) : null;

    const geometry = useMemo(() => {
        if (chartType === 'icicle' || chartType === 'sunburst' || chartType === 'sankey' || chartType === 'pie' || chartType === 'donut' || chartType === 'scatter' || chartType === 'waterfall' || chartType === 'combo' || chartType === 'sparkline') return null;
        const flatValues = stackedChildBarMode
            ? (model.stackedGroups || []).reduce((acc, group) => {
                let positiveTotal = 0;
                let negativeTotal = 0;
                (group.segments || []).forEach((segment) => {
                    const value = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                    if (value === null) return;
                    if (value >= 0) positiveTotal += value;
                    else negativeTotal += value;
                });
                acc.push(positiveTotal, negativeTotal);
                return acc;
            }, [])
            : stackedSeriesBarMode
                ? (model.categories || []).reduce((acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach((series) => {
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : stackedAreaMode
                ? model.categories.reduce((acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach((series) => {
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : (model.series || []).reduce((acc, series) => {
                (series.values || []).forEach((value) => {
                    if (typeof value === 'number' && Number.isFinite(value)) acc.push(value);
                });
                return acc;
            }, []);

        if (flatValues.length === 0) return null;

        const hasHierarchicalBands = !stackedChildBarMode && chartType === 'bar'
            && Array.isArray(model.categoryBands)
            && model.categoryBands.length === model.categories.length
            && model.categoryBands.some((band) => band && band.outerLabel);
        let minValue = Math.min(...flatValues, 0);
        let maxValue = Math.max(...flatValues, 0);
        if (minValue === maxValue) {
            const padding = Math.abs(maxValue || 1) * 0.15 || 1;
            minValue -= padding;
            maxValue += padding;
        }

        const tickLabels = niceTickValues(minValue, maxValue, 5).map((v) => fmt(v));
        const maxTickLabelWidth = tickLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);

        const horizontalInnerLabels = stackedChildBarMode
            ? (model.stackedGroups || []).map((group) => truncateChartLabel(group && group.label, 22))
            : (
                hasHierarchicalBands
                    ? (model.categoryBands || []).map((band) => truncateChartLabel(band && band.innerLabel, 18))
                    : (model.categories || []).map((category) => truncateChartLabel(category, 22))
            );
        const horizontalOuterLabels = hasHierarchicalBands
            ? (model.categoryBands || []).map((band) => truncateChartLabel(band && band.outerLabel, 18))
            : [];
        const maxInnerLabelWidth = horizontalInnerLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);
        const maxOuterLabelWidth = horizontalOuterLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label, 11)), 0);

        const leftMargin = horizontalBarMode
            ? Math.max(
                hasHierarchicalBands ? 176 : 132,
                hasHierarchicalBands
                    ? maxOuterLabelWidth + maxInnerLabelWidth + 38
                    : maxInnerLabelWidth + 22
            )
            : Math.max(66, maxTickLabelWidth + 18);

        const margin = horizontalBarMode
            ? { top: 24, right: 18, bottom: 42, left: leftMargin }
            : { top: 24, right: 18, bottom: hasHierarchicalBands ? 118 : 84, left: leftMargin };
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        const baseline = chartType === 'bar' ? 0 : Math.min(Math.max(0, minValue), maxValue);
        const safeSpan = maxValue - minValue || 1;
        const scaleY = horizontalBarMode ? null : ((value) => margin.top + ((maxValue - value) / safeSpan) * plotHeight);
        const scaleX = horizontalBarMode
            ? ((value) => margin.left + ((value - minValue) / safeSpan) * plotWidth)
            : null;
        const baselineY = scaleY ? scaleY(baseline) : null;
        const baselineX = scaleX ? scaleX(baseline) : null;
        const categoryCount = Math.max(model.categories.length, 1);
        const step = (horizontalBarMode ? plotHeight : plotWidth) / categoryCount;
        const groupWidth = Math.max(10, step * 0.72);

        return {
            margin,
            plotWidth,
            plotHeight,
            baselineY,
            baselineX,
            scaleY,
            scaleX,
            step,
            groupWidth,
            minValue,
            maxValue,
            hasHierarchicalBands,
            horizontalBarMode,
        };
    }, [axisMode, model, chartType, stackedAreaMode, stackedChildBarMode, stackedSeriesBarMode, horizontalBarMode, effectiveChartHeight, resolvedChartWidth]);

    if (chartType === 'sparkline') {
        return (
            <div
                ref={chartContainerRef}
                data-chart-sparkline-surface="true"
                style={{
                    position: 'relative',
                    minHeight: `${effectiveChartHeight}px`,
                    height: `${effectiveChartHeight}px`,
                    minWidth: 0,
                }}
            >
                <SparklineBoard
                    model={model}
                    theme={theme}
                    chartHeight={effectiveChartHeight}
                    paletteColors={paletteColors}
                    valueFormat={valueFormat}
                />
                {chartTitleOverlay}
            </div>
        );
    }

    if (chartType === 'icicle') {
        if (hierarchyNodes.length === 0) {
            return <EmptyChartState message="Icicle charts need hierarchical rows and a numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const bandHeight = effectiveChartHeight / icicleDepth;
        const layoutNodes = layoutIcicleNodes(hierarchyNodes, 0, resolvedChartWidth, 0, bandHeight, []);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Icicle chart'}
                    >
                        {layoutNodes.map((node, index) => {
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme, paletteColors);
                            const textShouldShow = node.width >= 54;
                            return (
                                <g
                                    key={`icicle-node-${node.rowPath || index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                    }}
                                    style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                                >
                                    <rect
                                        x={node.x}
                                        y={node.y}
                                        width={Math.max(1, node.width - 1)}
                                        height={Math.max(20, node.height - 2)}
                                        rx="4"
                                        fill={fill}
                                        opacity="0.86"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1"
                                        data-tip-cat={node.label || ''}
                                        data-tip-val={typeof node.value === 'number' ? fmt(node.value) : ''}
                                        data-tip-color={fill}
                                    />
                                    {textShouldShow ? (
                                        <text
                                            x={node.x + 8}
                                            y={node.y + Math.min(node.height - 10, 22)}
                                            textAnchor="start"
                                            fontSize="11"
                                            fontWeight="700"
                                            fill="#fff"
                                            style={{ pointerEvents: 'none' }}
                                        >
                                            {truncateChartLabel(node.label, Math.max(6, Math.floor(node.width / 9)))}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
            </div>
        );
    }

    if (chartType === 'sunburst') {
        if (hierarchyNodes.length === 0) {
            return <EmptyChartState message="Sunburst charts need hierarchical rows and a numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const radius = Math.min(resolvedChartWidth, effectiveChartHeight) / 2 - 18;
        const centerX = resolvedChartWidth / 2;
        const centerY = effectiveChartHeight / 2;
        const ringWidth = radius / Math.max(icicleDepth, 1);
        const layoutNodes = layoutSunburstNodes(hierarchyNodes, -Math.PI / 2, (Math.PI * 3) / 2, 1, ringWidth, []);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Sunburst chart'}
                    >
                        {layoutNodes.map((node, index) => {
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme, paletteColors);
                            const path = describeDonutArc(centerX, centerY, node.innerRadius, node.outerRadius, node.startAngle, node.endAngle);
                            const midAngle = node.startAngle + ((node.endAngle - node.startAngle) / 2);
                            const labelRadius = node.innerRadius + ((node.outerRadius - node.innerRadius) / 2);
                            const labelPos = polarToCartesian(centerX, centerY, labelRadius, midAngle);
                            const span = node.endAngle - node.startAngle;
                            const canShowText = span > 0.2 && (node.outerRadius - node.innerRadius) >= 18;

                            return (
                                <g
                                    key={`sunburst-node-${node.rowPath || index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                    }}
                                    style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                                >
                                    <path
                                        d={path}
                                        fill={fill}
                                        opacity="0.88"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1"
                                        data-tip-cat={node.label || ''}
                                        data-tip-val={typeof node.value === 'number' ? fmt(node.value) : ''}
                                        data-tip-color={fill}
                                    />
                                    {canShowText ? (
                                        <text
                                            x={labelPos.x}
                                            y={labelPos.y}
                                            textAnchor="middle"
                                            fontSize="10"
                                            fontWeight="700"
                                            fill="#fff"
                                            style={{ pointerEvents: 'none' }}
                                        >
                                            {truncateChartLabel(node.label, Math.max(5, Math.floor((span * 24))))}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
            </div>
        );
    }

    if (chartType === 'sankey') {
        const sankeyNodes = Array.isArray(model && model.sankeyNodes) ? model.sankeyNodes : [];
        const sankeyLinks = Array.isArray(model && model.sankeyLinks) ? model.sankeyLinks : [];
        if (sankeyNodes.length === 0 || sankeyLinks.length === 0) {
            return <EmptyChartState message="Sankey charts need expanded hierarchical rows with child flows." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const sankeyLayout = layoutSankey(sankeyNodes, sankeyLinks, resolvedChartWidth, effectiveChartHeight);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Sankey chart'}
                    >
                        {sankeyLayout.links.map((link, index) => {
                            const sourceX = link.sourceNode.x + link.sourceNode.width;
                            const targetX = link.targetNode.x;
                            const midX = sourceX + ((targetX - sourceX) / 2);
                            const path = [
                                `M ${sourceX} ${link.sourceY}`,
                                `C ${midX} ${link.sourceY}, ${midX} ${link.targetY}, ${targetX} ${link.targetY}`,
                            ].join(' ');
                            return (
                                <path
                                    key={`sankey-link-${link.source}-${link.target}-${index}`}
                                    d={path}
                                    fill="none"
                                    stroke={theme.primary}
                                    strokeOpacity="0.22"
                                    strokeWidth={link.thickness}
                                    data-tip-cat={`${link.sourceNode.label || ''} → ${link.targetNode.label || ''}`}
                                    data-tip-val={typeof link.value === 'number' ? fmt(link.value) : ''}
                                />
                            );
                        })}
                        {sankeyLayout.nodes.map((node, index) => {
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme, paletteColors);
                            return (
                                <g
                                    key={`sankey-node-${node.rowPath || index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                    }}
                                    style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                                >
                                    <rect
                                        x={node.x}
                                        y={node.y}
                                        width={node.width}
                                        height={node.height}
                                        rx="4"
                                        fill={fill}
                                        opacity="0.92"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1"
                                        data-tip-cat={node.label || ''}
                                        data-tip-val={typeof node.value === 'number' ? fmt(node.value) : ''}
                                        data-tip-color={fill}
                                    />
                                    <text
                                        x={node.x + node.width + 6}
                                        y={node.y + Math.min(node.height / 2 + 4, node.height - 4)}
                                        textAnchor="start"
                                        fontSize="11"
                                        fill={theme.text}
                                        fontWeight="700"
                                        style={{ pointerEvents: 'none' }}
                                    >
                                        {truncateChartLabel(node.label, 24)}
                                    </text>
                                </g>
                            );
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
            </div>
        );
    }

    if (chartType === 'pie' || chartType === 'donut') {
        const pieSeries = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        const pieCategories = Array.isArray(model && model.categories) ? model.categories : [];
        if (!pieSeries || pieCategories.length === 0) {
            return <EmptyChartState message="Pie charts need categories and at least one numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const rawSlices = pieCategories.map((category, index) => {
            const rawValue = pieSeries.values[index];
            const value = typeof rawValue === 'number' && Number.isFinite(rawValue) ? rawValue : 0;
            return { category, value: Math.abs(value), originalValue: value, index };
        }).filter((slice) => slice.value > 0 && !hiddenSeries.has(slice.category));
        const totalValue = rawSlices.reduce((sum, slice) => sum + slice.value, 0);
        if (totalValue === 0) {
            return <EmptyChartState message="All values are zero — nothing to display." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const isDonut = chartType === 'donut';
        const radius = Math.min(resolvedChartWidth, effectiveChartHeight) / 2 - 24;
        const innerRadius = isDonut ? radius * 0.52 : 0;
        const centerX = resolvedChartWidth / 2;
        const centerY = effectiveChartHeight / 2;
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        let angleCursor = -Math.PI / 2;
        const sliceData = rawSlices.map((slice) => {
            const fraction = slice.value / totalValue;
            const sweep = fraction * Math.PI * 2;
            const startAngle = angleCursor;
            const endAngle = angleCursor + sweep;
            angleCursor = endAngle;
            return { ...slice, startAngle, endAngle, fraction };
        });
        const legendItems = sliceData.map((slice) => ({
            label: `${slice.category} (${(slice.fraction * 100).toFixed(1)}%)`,
            color: getColorForIndex(slice.index, theme, paletteColors),
        }));

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || (isDonut ? 'Donut chart' : 'Pie chart')}
                    >
                        {sliceData.map((slice) => {
                            const fill = getColorForIndex(slice.index, theme, paletteColors);
                            const path = describeDonutArc(centerX, centerY, innerRadius, radius, slice.startAngle, slice.endAngle);
                            const midAngle = slice.startAngle + ((slice.endAngle - slice.startAngle) / 2);
                            const labelRadius = isDonut
                                ? innerRadius + ((radius - innerRadius) / 2)
                                : radius * 0.62;
                            const labelPos = polarToCartesian(centerX, centerY, labelRadius, midAngle);
                            const canShowLabel = (slice.endAngle - slice.startAngle) > 0.3;
                            const outerLabelPos = polarToCartesian(centerX, centerY, radius + 14, midAngle);
                            const canShowOuterLabel = (slice.endAngle - slice.startAngle) > 0.15;
                            const target = categoryTargets[slice.index] || null;

                            return (
                                <g
                                    key={`pie-slice-${slice.index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function' && target) onCategoryActivate(target);
                                    }}
                                    style={typeof onCategoryActivate === 'function' && target ? { cursor: 'pointer' } : undefined}
                                >
                                    <path
                                        d={path}
                                        fill={fill}
                                        opacity="0.9"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="2"
                                        data-tip-cat={slice.category}
                                        data-tip-ser={pieSeries.name}
                                        data-tip-val={`${fmt(slice.originalValue)} (${(slice.fraction * 100).toFixed(1)}%)`}
                                        data-tip-color={fill}
                                    />
                                    {canShowLabel ? (
                                        <text
                                            x={labelPos.x}
                                            y={labelPos.y}
                                            textAnchor="middle"
                                            dominantBaseline="central"
                                            fontSize="10"
                                            fontWeight="700"
                                            fill="#fff"
                                            style={{ pointerEvents: 'none' }}
                                        >
                                            {(slice.fraction * 100).toFixed(1)}%
                                        </text>
                                    ) : null}
                                    {showDataLabels && canShowOuterLabel ? (
                                        <text
                                            x={outerLabelPos.x}
                                            y={outerLabelPos.y}
                                            textAnchor={midAngle > Math.PI / 2 && midAngle < (3 * Math.PI / 2) ? 'end' : 'start'}
                                            dominantBaseline="central"
                                            fontSize="9"
                                            fontWeight="700"
                                            fill={theme.textSec}
                                            style={{ pointerEvents: 'none' }}
                                        >
                                            {fmt(slice.originalValue)}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                        {isDonut ? (
                            <text
                                x={centerX}
                                y={centerY}
                                textAnchor="middle"
                                dominantBaseline="central"
                                fontSize="16"
                                fontWeight="800"
                                fill={theme.text}
                                style={{ pointerEvents: 'none' }}
                            >
                                {fmt(totalValue)}
                            </text>
                        ) : null}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} /> : null}
            </div>
        );
    }

    if (chartType === 'scatter') {
        const allSeries = Array.isArray(model && model.series) ? model.series.filter((s) => !hiddenSeries.has(s.name)) : [];
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        if (allSeries.length < 2) {
            return <EmptyChartState message="Scatter charts need at least two numeric measures (X and Y)." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const xSeries = allSeries[0];
        const ySeries = allSeries.slice(1);
        const xValues = (xSeries.values || []).filter((v) => typeof v === 'number' && Number.isFinite(v));
        const allYValues = ySeries.flatMap((s) => (s.values || []).filter((v) => typeof v === 'number' && Number.isFinite(v)));
        if (xValues.length === 0 || allYValues.length === 0) {
            return <EmptyChartState message="Not enough numeric data for a scatter chart." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        let minX = Math.min(...xValues);
        let maxX = Math.max(...xValues);
        let minY = Math.min(...allYValues);
        let maxY = Math.max(...allYValues);
        if (minX === maxX) { const pad = Math.abs(maxX || 1) * 0.15 || 1; minX -= pad; maxX += pad; }
        if (minY === maxY) { const pad = Math.abs(maxY || 1) * 0.15 || 1; minY -= pad; maxY += pad; }
        const xPad = (maxX - minX) * 0.06;
        const yPad = (maxY - minY) * 0.06;
        minX -= xPad; maxX += xPad; minY -= yPad; maxY += yPad;
        const leftMargin = Math.max(52, estimateTextWidth(fmt(maxY), 11) + 16);
        const margin = { top: 24, right: 20, bottom: 64, left: leftMargin };
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        const scaleX = (v) => margin.left + ((v - minX) / (maxX - minX)) * plotWidth;
        const scaleY = (v) => margin.top + ((maxY - v) / (maxY - minY)) * plotHeight;
        const xTicks = niceTickValues(minX, maxX, 5);
        const yTicks = niceTickValues(minY, maxY, 5);
        const legendItems = ySeries.map((s, i) => ({ label: s.name, color: getColorForIndex(i, theme, paletteColors) }));
        const canClick = typeof onCategoryActivate === 'function';
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ position: 'relative', width: '100%' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        width="100%"
                        preserveAspectRatio="xMidYMid meet"
                        style={{ display: 'block', overflow: 'visible' }}
                    >
                        {yTicks.map((tick, i) => (
                            <g key={`y-tick-${i}`}>
                                <line x1={margin.left} y1={scaleY(tick)} x2={resolvedChartWidth - margin.right} y2={scaleY(tick)} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={margin.left - 8} y={scaleY(tick) + 4} textAnchor="end" fontSize="11" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{fmt(tick)}</text>
                            </g>
                        ))}
                        {xTicks.map((tick, i) => (
                            <g key={`x-tick-${i}`}>
                                <line x1={scaleX(tick)} y1={margin.top} x2={scaleX(tick)} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={scaleX(tick)} y={effectiveChartHeight - margin.bottom + 18} textAnchor="middle" fontSize="11" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{fmt(tick)}</text>
                            </g>
                        ))}
                        <line x1={margin.left} y1={effectiveChartHeight - margin.bottom} x2={resolvedChartWidth - margin.right} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} />
                        <line x1={margin.left} y1={margin.top} x2={margin.left} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} />
                        <text x={resolvedChartWidth / 2} y={effectiveChartHeight - 8} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{xSeries.name}</text>
                        {yAxisTitle ? (
                            <text x={14} y={(margin.top + effectiveChartHeight - margin.bottom) / 2} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} transform={`rotate(-90, 14, ${(margin.top + effectiveChartHeight - margin.bottom) / 2})`} style={{ pointerEvents: 'none' }}>{yAxisTitle}</text>
                        ) : null}
                        {ySeries.map((series, seriesIndex) => {
                            const color = getColorForIndex(seriesIndex, theme, paletteColors);
                            return (series.values || []).map((yVal, pointIndex) => {
                                const xVal = xSeries.values[pointIndex];
                                if (typeof xVal !== 'number' || !Number.isFinite(xVal) || typeof yVal !== 'number' || !Number.isFinite(yVal)) return null;
                                const cx = scaleX(xVal);
                                const cy = scaleY(yVal);
                                const label = categories[pointIndex] || `Point ${pointIndex + 1}`;
                                const target = categoryTargets[pointIndex];
                                return (
                                    <circle
                                        key={`scatter-${seriesIndex}-${pointIndex}`}
                                        cx={cx}
                                        cy={cy}
                                        r="5"
                                        fill={color}
                                        opacity="0.82"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1.5"
                                        data-tip-cat={label}
                                        data-tip-ser={`${series.name} (${xSeries.name}=${fmt(xVal)})`}
                                        data-tip-val={fmt(yVal)}
                                        data-tip-color={color}
                                        style={canClick && target ? { cursor: 'pointer' } : undefined}
                                        onClick={canClick && target ? () => onCategoryActivate(target) : undefined}
                                    />
                                );
                            });
                        })}
                        {showDataLabels ? ySeries.map((series, seriesIndex) => {
                            const color = getColorForIndex(seriesIndex, theme, paletteColors);
                            return (series.values || []).map((yVal, pointIndex) => {
                                const xVal = xSeries.values[pointIndex];
                                if (typeof xVal !== 'number' || !Number.isFinite(xVal) || typeof yVal !== 'number' || !Number.isFinite(yVal)) return null;
                                return (
                                    <text
                                        key={`scatter-label-${seriesIndex}-${pointIndex}`}
                                        x={scaleX(xVal)}
                                        y={scaleY(yVal) - 8}
                                        textAnchor="middle"
                                        fontSize="10"
                                        fontWeight="700"
                                        fill={color}
                                        style={{ pointerEvents: 'none' }}
                                    >
                                        {fmt(yVal)}
                                    </text>
                                );
                            });
                        }) : null}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} /> : null}
            </div>
        );
    }

    if (chartType === 'waterfall') {
        const wfSeries = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        const wfCategories = Array.isArray(model && model.categories) ? model.categories : [];
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        if (!wfSeries || wfCategories.length === 0) {
            return <EmptyChartState message="Waterfall charts need categories and at least one numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const increments = wfCategories.map((_, i) => {
            const v = wfSeries.values[i];
            return typeof v === 'number' && Number.isFinite(v) ? v : 0;
        });
        const bars = [];
        let running = 0;
        for (let i = 0; i < increments.length; i++) {
            const start = running;
            running += increments[i];
            bars.push({ label: wfCategories[i], start, end: running, increment: increments[i], type: increments[i] >= 0 ? 'positive' : 'negative' });
        }
        bars.push({ label: 'Total', start: 0, end: running, increment: running, type: 'total' });
        const allEnds = bars.map((b) => b.start).concat(bars.map((b) => b.end));
        let minVal = Math.min(...allEnds, 0);
        let maxVal = Math.max(...allEnds, 0);
        if (minVal === maxVal) { const pad = Math.abs(maxVal || 1) * 0.15 || 1; minVal -= pad; maxVal += pad; }
        const yPad = (maxVal - minVal) * 0.08;
        minVal -= yPad; maxVal += yPad;
        const wfTicks = niceTickValues(minVal, maxVal, 5);
        const leftMargin = Math.max(52, estimateTextWidth(fmt(maxVal), 11) + 16);
        const margin = { top: 24, right: 20, bottom: 84, left: leftMargin };
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        const scaleY = (v) => margin.top + ((maxVal - v) / (maxVal - minVal)) * plotHeight;
        const step = plotWidth / bars.length;
        const barWidth = Math.max(14, Math.min(60, step * 0.6));
        const positiveColor = '#059669';
        const negativeColor = '#DC2626';
        const totalColor = getColorForIndex(0, theme, paletteColors);
        const canClick = typeof onCategoryActivate === 'function';
        const legendItems = [
            { label: 'Increase', color: positiveColor },
            { label: 'Decrease', color: negativeColor },
            { label: 'Total', color: totalColor },
        ];
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative' }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        width="100%"
                        preserveAspectRatio="xMidYMid meet"
                        style={{ display: 'block', overflow: 'visible' }}
                    >
                        {wfTicks.map((tick, i) => (
                            <g key={`wf-tick-${i}`}>
                                <line x1={margin.left} y1={scaleY(tick)} x2={resolvedChartWidth - margin.right} y2={scaleY(tick)} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={margin.left - 8} y={scaleY(tick) + 4} textAnchor="end" fontSize="11" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{fmt(tick)}</text>
                            </g>
                        ))}
                        <line x1={margin.left} y1={scaleY(0)} x2={resolvedChartWidth - margin.right} y2={scaleY(0)} stroke={theme.textSec} strokeOpacity="0.5" />
                        {yAxisTitle ? (
                            <text x={14} y={(margin.top + effectiveChartHeight - margin.bottom) / 2} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} transform={`rotate(-90, 14, ${(margin.top + effectiveChartHeight - margin.bottom) / 2})`} style={{ pointerEvents: 'none' }}>{yAxisTitle}</text>
                        ) : null}
                        {bars.map((bar, i) => {
                            const cx = margin.left + i * step + step / 2;
                            const x = cx - barWidth / 2;
                            const yTop = scaleY(Math.max(bar.start, bar.end));
                            const yBot = scaleY(Math.min(bar.start, bar.end));
                            const height = Math.max(1, yBot - yTop);
                            const fill = bar.type === 'total' ? totalColor : bar.type === 'positive' ? positiveColor : negativeColor;
                            const target = i < categoryTargets.length ? categoryTargets[i] : null;
                            return (
                                <g key={`wf-bar-${i}`}>
                                    {i > 0 && i < bars.length - 1 ? (
                                        <line
                                            x1={margin.left + (i - 1) * step + step / 2 + barWidth / 2}
                                            y1={scaleY(bar.start)}
                                            x2={x}
                                            y2={scaleY(bar.start)}
                                            stroke={theme.textSec}
                                            strokeDasharray="2 2"
                                            strokeOpacity="0.5"
                                        />
                                    ) : null}
                                    <rect
                                        x={x}
                                        y={yTop}
                                        width={barWidth}
                                        height={height}
                                        rx="3"
                                        fill={fill}
                                        opacity="0.9"
                                        data-tip-cat={bar.label}
                                        data-tip-ser={bar.type === 'total' ? 'Total' : (bar.increment >= 0 ? '+' : '') + fmt(bar.increment)}
                                        data-tip-val={fmt(bar.end)}
                                        data-tip-color={fill}
                                        style={canClick && target ? { cursor: 'pointer' } : undefined}
                                        onClick={canClick && target ? () => onCategoryActivate(target) : undefined}
                                    />
                                    {showDataLabels ? (
                                        <text
                                            x={cx}
                                            y={yTop - 5}
                                            textAnchor="middle"
                                            fontSize="9"
                                            fontWeight="700"
                                            fill={theme.textSec}
                                            style={{ pointerEvents: 'none' }}
                                        >
                                            {bar.type === 'total' ? fmt(bar.end) : (bar.increment >= 0 ? '+' : '') + fmt(bar.increment)}
                                        </text>
                                    ) : null}
                                    <text
                                        x={cx}
                                        y={effectiveChartHeight - margin.bottom + 16}
                                        textAnchor="middle"
                                        fontSize="10"
                                        fontWeight="600"
                                        fill={theme.textSec}
                                        style={{ pointerEvents: 'none' }}
                                    >
                                        {truncateChartLabel(bar.label, 12)}
                                    </text>
                                </g>
                            );
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} /> : null}
            </div>
        );
    }

    if (chartType === 'combo') {
        const comboLayers = (Array.isArray(model && model.layers) ? model.layers : [])
            .filter((layer) => layer && !layer.hidden && !hiddenSeries.has(layer.name) && Array.isArray(layer.values) && layer.values.some((value) => value !== null));
        if (comboLayers.length === 0) {
            return <EmptyChartState message={model.emptyMessage || 'No combo layers are configured.'} theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const hasRightAxis = comboLayers.some((layer) => layer.axis === 'right');
        const margin = { top: 24, right: hasRightAxis ? 76 : 20, bottom: 96, left: 72 };
        const categoryCount = Math.max(1, Array.isArray(model.categories) ? model.categories.length : 0);
        const step = Math.max(1, (resolvedChartWidth - margin.left - margin.right) / categoryCount);
        const groupWidth = Math.max(22, Math.min(72, step * 0.72));
        const barLayers = comboLayers.filter((layer) => layer.type === 'bar');
        const areaLayers = comboLayers.filter((layer) => layer.type === 'area');
        const lineLayers = comboLayers.filter((layer) => layer.type === 'line');
        const leftMetrics = getAxisMetrics(
            comboLayers.filter((layer) => layer.axis !== 'right').flatMap((layer) => layer.values),
            effectiveChartHeight,
            margin
        );
        const rightMetrics = hasRightAxis
            ? getAxisMetrics(
                comboLayers.filter((layer) => layer.axis === 'right').flatMap((layer) => layer.values),
                effectiveChartHeight,
                margin
            )
            : leftMetrics;
        const categoryTargets = Array.isArray(model.categoryTargets) ? model.categoryTargets : [];
        const legendItems = comboLayers.map((layer, index) => ({
            label: layer.name,
            color: getColorForIndex(index, theme, paletteColors),
        }));
        const getLayerScale = (layer) => (layer.axis === 'right' ? rightMetrics.scaleY : leftMetrics.scaleY);
        const getLayerBaseline = (layer) => (layer.axis === 'right' ? rightMetrics.baselineY : leftMetrics.baselineY);
        const barGap = 4;
        const barWidth = Math.max(8, (groupWidth - (Math.max(barLayers.length, 1) - 1) * barGap) / Math.max(barLayers.length, 1));

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                            ...zoomSvgStyle,
                        }}
                        role="img"
                        aria-label={model.title || 'Combo chart'}
                    >
                        {leftMetrics.tickValues.map((tickValue, index) => {
                            const y = leftMetrics.scaleY(tickValue);
                            return (
                                <g key={`combo-left-tick-${index}`}>
                                    <line
                                        x1={margin.left}
                                        x2={resolvedChartWidth - margin.right}
                                        y1={y}
                                        y2={y}
                                        stroke={theme.border}
                                        strokeOpacity="0.7"
                                        strokeDasharray="4 4"
                                    />
                                    <text
                                        x={margin.left - 10}
                                        y={y + 4}
                                        textAnchor="end"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {fmt(tickValue)}
                                    </text>
                                </g>
                            );
                        })}

                        {hasRightAxis ? rightMetrics.tickValues.map((tickValue, index) => {
                            const y = rightMetrics.scaleY(tickValue);
                            return (
                                <text
                                    key={`combo-right-tick-${index}`}
                                    x={resolvedChartWidth - margin.right + 10}
                                    y={y + 4}
                                    textAnchor="start"
                                    fontSize="11"
                                    fill={theme.textSec}
                                >
                                    {fmt(tickValue)}
                                </text>
                            );
                        }) : null}

                        {yAxisTitle ? (
                            <text x={14} y={(margin.top + effectiveChartHeight - margin.bottom) / 2} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} transform={`rotate(-90, 14, ${(margin.top + effectiveChartHeight - margin.bottom) / 2})`} style={{ pointerEvents: 'none' }}>{yAxisTitle}</text>
                        ) : null}

                        <line
                            x1={margin.left}
                            x2={resolvedChartWidth - margin.right}
                            y1={leftMetrics.baselineY}
                            y2={leftMetrics.baselineY}
                            stroke={theme.textSec}
                            strokeOpacity="0.45"
                        />

                        {barLayers.map((layer, barLayerIndex) => (
                            model.categories.map((category, categoryIndex) => {
                                const value = layer.values[categoryIndex];
                                if (value === null || value === undefined) return null;
                                const slotStart = margin.left + (categoryIndex * step) + ((step - groupWidth) / 2);
                                const scaleY = getLayerScale(layer);
                                const baselineY = getLayerBaseline(layer);
                                const y = scaleY(value);
                                const color = getColorForIndex(barLayerIndex, theme, paletteColors);
                                const barX = slotStart + (barLayerIndex * (barWidth + barGap));
                                return (
                                    <React.Fragment key={`${layer.id}-${category}-${barLayerIndex}`}>
                                        <rect
                                            x={barX}
                                            y={Math.min(y, baselineY)}
                                            width={barWidth}
                                            height={Math.max(1, Math.abs(baselineY - y))}
                                            rx="3"
                                            fill={color}
                                            opacity="0.9"
                                            data-tip-cat={category}
                                            data-tip-ser={layer.name}
                                            data-tip-val={fmt(value)}
                                            data-tip-color={color}
                                        />
                                        {showDataLabels ? (
                                            <text
                                                x={barX + barWidth / 2}
                                                y={value >= 0 ? Math.min(y, baselineY) - 4 : Math.max(y, baselineY) + 12}
                                                textAnchor="middle"
                                                fontSize="9"
                                                fontWeight="700"
                                                fill={theme.textSec}
                                                style={{ pointerEvents: 'none' }}
                                            >
                                                {fmt(value)}
                                            </text>
                                        ) : null}
                                    </React.Fragment>
                                );
                            })
                        ))}

                        {areaLayers.map((layer, layerIndex) => {
                            const points = model.categories.map((_, categoryIndex) => {
                                const rawValue = layer.values[categoryIndex];
                                if (rawValue === null || rawValue === undefined) return null;
                                const x = margin.left + (categoryIndex * step) + (step / 2);
                                const scaleY = getLayerScale(layer);
                                return {
                                    x,
                                    y: scaleY(rawValue),
                                    baseY: getLayerBaseline(layer),
                                    rawValue,
                                };
                            });
                            const areaPath = buildAreaBandPath(points);
                            const linePath = buildLinePath(points);
                            const color = getColorForIndex(barLayers.length + layerIndex, theme, paletteColors);
                            return (
                                <g key={`combo-area-${layer.id}`}>
                                    {areaPath ? <path d={areaPath} fill={color} opacity="0.16" style={{ pointerEvents: 'none' }} /> : null}
                                    {linePath ? <path d={linePath} fill="none" stroke={color} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" style={{ pointerEvents: 'none' }} /> : null}
                                    {points.map((point, pointIndex) => {
                                        if (!point) return null;
                                        return (
                                            <React.Fragment key={`${layer.id}-area-pt-${pointIndex}`}>
                                                <circle
                                                    cx={point.x}
                                                    cy={point.y}
                                                    r="4"
                                                    fill={color}
                                                    stroke={theme.surfaceBg || theme.background || '#fff'}
                                                    strokeWidth="2"
                                                    data-tip-cat={model.categories[pointIndex]}
                                                    data-tip-ser={layer.name}
                                                    data-tip-val={fmt(point.rawValue)}
                                                    data-tip-color={color}
                                                />
                                                {showDataLabels ? (
                                                    <text x={point.x} y={point.y - 8} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>
                                                        {fmt(point.rawValue)}
                                                    </text>
                                                ) : null}
                                            </React.Fragment>
                                        );
                                    })}
                                </g>
                            );
                        })}

                        {lineLayers.map((layer, layerIndex) => {
                            const points = model.categories.map((_, categoryIndex) => {
                                const rawValue = layer.values[categoryIndex];
                                if (rawValue === null || rawValue === undefined) return null;
                                const x = margin.left + (categoryIndex * step) + (step / 2);
                                return {
                                    x,
                                    y: getLayerScale(layer)(rawValue),
                                    rawValue,
                                };
                            });
                            const linePath = buildLinePath(points);
                            const color = getColorForIndex(barLayers.length + areaLayers.length + layerIndex, theme, paletteColors);
                            return (
                                <g key={`combo-line-${layer.id}`}>
                                    {linePath ? <path d={linePath} fill="none" stroke={color} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" style={{ pointerEvents: 'none' }} /> : null}
                                    {points.map((point, pointIndex) => {
                                        if (!point) return null;
                                        return (
                                            <React.Fragment key={`${layer.id}-point-${pointIndex}`}>
                                                <circle
                                                    cx={point.x}
                                                    cy={point.y}
                                                    r="4"
                                                    fill={color}
                                                    stroke={theme.surfaceBg || theme.background || '#fff'}
                                                    strokeWidth="2"
                                                    data-tip-cat={model.categories[pointIndex]}
                                                    data-tip-ser={layer.name}
                                                    data-tip-val={fmt(point.rawValue)}
                                                    data-tip-color={color}
                                                />
                                                {showDataLabels ? (
                                                    <text x={point.x} y={point.y - 8} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>
                                                        {fmt(point.rawValue)}
                                                    </text>
                                                ) : null}
                                            </React.Fragment>
                                        );
                                    })}
                                </g>
                            );
                        })}

                        {model.categories.map((category, categoryIndex) => {
                            const x = margin.left + (categoryIndex * step) + (step / 2);
                            return (
                                <text
                                    key={`combo-label-${categoryIndex}`}
                                    x={x}
                                    y={effectiveChartHeight - margin.bottom + 28}
                                    textAnchor="middle"
                                    fontSize="11"
                                    fill={theme.textSec}
                                >
                                    {truncateChartLabel(category, 18)}
                                </text>
                            );
                        })}

                        {typeof onCategoryActivate === 'function' ? model.categories.map((_, categoryIndex) => {
                            const target = categoryTargets[categoryIndex];
                            if (!target) return null;
                            return (
                                <rect
                                    key={`combo-hit-${categoryIndex}`}
                                    x={margin.left + (categoryIndex * step)}
                                    y={margin.top}
                                    width={step}
                                    height={effectiveChartHeight - margin.top - margin.bottom}
                                    fill="transparent"
                                    style={{ cursor: 'pointer' }}
                                    onClick={() => onCategoryActivate(target)}
                                />
                            );
                        }) : null}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} /> : null}
            </div>
        );
    }

    if (!geometry) {
        return <EmptyChartState message="No numeric data available for this chart." theme={theme} chartHeight={effectiveChartHeight} />;
    }

    const ticks = niceTickValues(geometry.minValue, geometry.maxValue, 5);
    const stackedLegendLabels = stackedChildBarMode
        ? Array.from(new Set(
            (model.stackedGroups || []).reduce((labels, group) => {
                (group.segments || []).forEach((segment) => {
                    if (segment && segment.label) labels.push(segment.label);
                });
                return labels;
            }, [])
        ))
        : [];
    const stackedColorByLabel = stackedLegendLabels.reduce((acc, label, index) => {
        acc[label] = getColorForIndex(index, theme, paletteColors);
        return acc;
    }, {});
    const legendItems = stackedChildBarMode
        ? stackedLegendLabels.map((label) => ({ label, color: stackedColorByLabel[label] || getColorForIndex(0, theme, paletteColors) }))
        : (model.series || []).map((series, index) => ({
            label: series.name,
            color: getColorForIndex(index, theme, paletteColors),
        }));
    const groupedBands = geometry.hasHierarchicalBands
        ? model.categoryBands.reduce((groups, band, index) => {
            const prev = groups[groups.length - 1];
            if (prev && prev.groupKey === band.groupKey) {
                prev.endIndex = index;
                return groups;
            }
            groups.push({
                groupKey: band.groupKey,
                outerLabel: band.outerLabel,
                startIndex: index,
                endIndex: index,
            });
            return groups;
        }, [])
        : [];
    const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
    const canActivateCategories = typeof onCategoryActivate === 'function' && categoryTargets.length === model.categories.length;

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                <svg
                    ref={svgRef}
                    viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`}
                    preserveAspectRatio="xMidYMid meet"
                    style={{
                        width: '100%',
                        height: 'auto',
                        display: 'block',
                        maxWidth: '100%',
                        ...zoomSvgStyle,
                    }}
                    role="img"
                    aria-label={model.title}
                >
                    {ticks.map((tickValue, index) => {
                        const y = geometry.scaleY ? geometry.scaleY(tickValue) : null;
                        const x = geometry.scaleX ? geometry.scaleX(tickValue) : null;
                        return (
                            <g key={`tick-${index}`}>
                                {geometry.horizontalBarMode ? (
                                    <>
                                        <line x1={x} y1={geometry.margin.top} x2={x} y2={effectiveChartHeight - geometry.margin.bottom} stroke={theme.border} strokeDasharray="3 4" />
                                        <text x={x} y={effectiveChartHeight - geometry.margin.bottom + 18} textAnchor="middle" fontSize="11" fill={theme.textSec}>
                                            {fmt(tickValue)}
                                        </text>
                                    </>
                                ) : (
                                    <>
                                        <line x1={geometry.margin.left} y1={y} x2={resolvedChartWidth - geometry.margin.right} y2={y} stroke={theme.border} strokeDasharray="3 4" />
                                        <text x={geometry.margin.left - 8} y={y + 4} textAnchor="end" fontSize="11" fill={theme.textSec}>
                                            {fmt(tickValue)}
                                        </text>
                                    </>
                                )}
                            </g>
                        );
                    })}
                    <line
                        x1={geometry.horizontalBarMode ? geometry.baselineX : geometry.margin.left}
                        y1={geometry.horizontalBarMode ? geometry.margin.top : geometry.baselineY}
                        x2={geometry.horizontalBarMode ? geometry.baselineX : resolvedChartWidth - geometry.margin.right}
                        y2={geometry.horizontalBarMode ? effectiveChartHeight - geometry.margin.bottom : geometry.baselineY}
                        stroke={theme.textSec}
                        strokeWidth="1"
                    />
                    {yAxisTitle ? (
                        <text x={14} y={(geometry.margin.top + effectiveChartHeight - geometry.margin.bottom) / 2} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} transform={`rotate(-90, 14, ${(geometry.margin.top + effectiveChartHeight - geometry.margin.bottom) / 2})`} style={{ pointerEvents: 'none' }}>{yAxisTitle}</text>
                    ) : null}

                    {geometry.hasHierarchicalBands && groupedBands.map((group, groupIndex) => {
                        if (groupIndex === groupedBands.length - 1) return null;
                        const divider = geometry.horizontalBarMode
                            ? geometry.margin.top + ((group.endIndex + 1) * geometry.step)
                            : geometry.margin.left + ((group.endIndex + 1) * geometry.step);
                        return (
                            <line
                                key={`group-divider-${group.groupKey}-${groupIndex}`}
                                x1={geometry.horizontalBarMode ? geometry.margin.left : divider}
                                y1={geometry.horizontalBarMode ? divider : geometry.margin.top}
                                x2={geometry.horizontalBarMode ? resolvedChartWidth - geometry.margin.right : divider}
                                y2={geometry.horizontalBarMode ? divider : effectiveChartHeight - geometry.margin.bottom + 58}
                                stroke={theme.border}
                                strokeDasharray="4 4"
                            />
                        );
                    })}

                    {stackedChildBarMode && (model.stackedGroups || []).map((group, groupIndex) => {
                        const slotStart = geometry.horizontalBarMode
                            ? geometry.margin.top + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2)
                            : geometry.margin.left + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2);
                        const barThickness = Math.max(14, geometry.groupWidth * 0.64);
                        let positiveOffset = 0;
                        let negativeOffset = 0;

                        return (
                            <g key={`stacked-group-${group.key}`}>
                                {(group.segments || []).map((segment) => {
                                    const rawValue = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                                    if (rawValue === null) return null;
                                    const startValue = rawValue >= 0 ? positiveOffset : negativeOffset;
                                    const endValue = startValue + rawValue;
                                    if (rawValue >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    const segColor = stackedColorByLabel[segment.label] || getColorForIndex(0, theme, paletteColors);
                                    if (geometry.horizontalBarMode) {
                                        const x1 = geometry.scaleX(startValue);
                                        const x2 = geometry.scaleX(endValue);
                                        return (
                                            <rect
                                                key={`${group.key}-${segment.path || segment.label}`}
                                                x={Math.min(x1, x2)}
                                                y={slotStart}
                                                width={Math.max(1, Math.abs(x2 - x1))}
                                                height={barThickness}
                                                rx="3"
                                                fill={segColor}
                                                opacity="0.94"
                                                data-tip-cat={group.label}
                                                data-tip-ser={segment.label}
                                                data-tip-val={fmt(rawValue)}
                                                data-tip-color={segColor}
                                            />
                                        );
                                    }
                                    const y1 = geometry.scaleY(startValue);
                                    const y2 = geometry.scaleY(endValue);
                                    const y = Math.min(y1, y2);
                                    const height = Math.max(1, Math.abs(y2 - y1));
                                    return (
                                        <rect
                                            key={`${group.key}-${segment.path || segment.label}`}
                                            x={slotStart}
                                            y={y}
                                            width={barThickness}
                                            height={height}
                                            rx="3"
                                            fill={segColor}
                                            opacity="0.94"
                                            data-tip-cat={group.label}
                                            data-tip-ser={segment.label}
                                            data-tip-val={fmt(rawValue)}
                                            data-tip-color={segColor}
                                        />
                                    );
                                })}
                                {geometry.horizontalBarMode ? (
                                    <text
                                        x={geometry.margin.left - 10}
                                        y={slotStart + (barThickness / 2) + 4}
                                        textAnchor="end"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(group.label, 22)}
                                    </text>
                                ) : (
                                    <text
                                        x={slotStart + (barThickness / 2)}
                                        y={effectiveChartHeight - geometry.margin.bottom + 30}
                                        textAnchor="middle"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(group.label, 20)}
                                    </text>
                                )}
                            </g>
                        );
                    })}

                {!stackedChildBarMode && chartType === 'bar' && model.categories.map((category, categoryIndex) => {
                    const slotStart = geometry.horizontalBarMode
                        ? geometry.margin.top + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2
                        : geometry.margin.left + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2;
                    const barThickness = stackedSeriesBarMode
                        ? Math.max(14, geometry.groupWidth * 0.64)
                        : Math.max(6, (geometry.groupWidth / Math.max(model.series.length, 1)) - 4);
                    let positiveOffset = 0;
                    let negativeOffset = 0;
                    return (
                        <g key={`bar-group-${category}`}>
                            {model.series.map((series, seriesIndex) => {
                                if (hiddenSeries.has(series.name)) return null;
                                const value = series.values[categoryIndex];
                                if (value === null || value === undefined) return null;
                                const barColor = getColorForIndex(seriesIndex, theme, paletteColors);
                                if (stackedSeriesBarMode) {
                                    const startValue = value >= 0 ? positiveOffset : negativeOffset;
                                    const endValue = startValue + value;
                                    if (value >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    const tipAttrs = { 'data-tip-cat': category, 'data-tip-ser': series.name, 'data-tip-val': fmt(value), 'data-tip-color': barColor };
                                    if (geometry.horizontalBarMode) {
                                        const x1 = geometry.scaleX(startValue);
                                        const x2 = geometry.scaleX(endValue);
                                        return (
                                            <rect
                                                key={`${series.name}-${category}`}
                                                x={Math.min(x1, x2)}
                                                y={slotStart}
                                                width={Math.max(1, Math.abs(x2 - x1))}
                                                height={barThickness}
                                                rx="3"
                                                fill={barColor}
                                                opacity="0.92"
                                                {...tipAttrs}
                                            />
                                        );
                                    }
                                    const y1 = geometry.scaleY(startValue);
                                    const y2 = geometry.scaleY(endValue);
                                    return (
                                        <rect
                                            key={`${series.name}-${category}`}
                                            x={slotStart}
                                            y={Math.min(y1, y2)}
                                            width={barThickness}
                                            height={Math.max(1, Math.abs(y2 - y1))}
                                            rx="3"
                                            fill={barColor}
                                            opacity="0.92"
                                            {...tipAttrs}
                                        />
                                    );
                                }
                                if (geometry.horizontalBarMode) {
                                    const x = geometry.scaleX(value);
                                    const y = slotStart + seriesIndex * (barThickness + 4);
                                    return (
                                        <React.Fragment key={`${series.name}-${category}`}>
                                            <rect
                                                x={Math.min(x, geometry.baselineX)}
                                                y={y}
                                                width={Math.max(1, Math.abs(geometry.baselineX - x))}
                                                height={barThickness}
                                                rx="3"
                                                fill={barColor}
                                                opacity="0.9"
                                                data-tip-cat={category}
                                                data-tip-ser={series.name}
                                                data-tip-val={fmt(value)}
                                                data-tip-color={barColor}
                                            />
                                            {showDataLabels ? (
                                                <text
                                                    x={value >= 0 ? Math.max(x, geometry.baselineX) + 4 : Math.min(x, geometry.baselineX) - 4}
                                                    y={y + barThickness / 2 + 3}
                                                    textAnchor={value >= 0 ? 'start' : 'end'}
                                                    fontSize="9"
                                                    fontWeight="700"
                                                    fill={theme.textSec}
                                                    style={{ pointerEvents: 'none' }}
                                                >
                                                    {fmt(value)}
                                                </text>
                                            ) : null}
                                        </React.Fragment>
                                    );
                                }
                                const y = geometry.scaleY(value);
                                const height = Math.max(1, Math.abs(geometry.baselineY - y));
                                const x = slotStart + seriesIndex * (barThickness + 4);
                                return (
                                    <React.Fragment key={`${series.name}-${category}`}>
                                        <rect
                                            x={x}
                                            y={Math.min(y, geometry.baselineY)}
                                            width={barThickness}
                                            height={height}
                                            rx="3"
                                            fill={barColor}
                                            opacity="0.9"
                                            data-tip-cat={category}
                                            data-tip-ser={series.name}
                                            data-tip-val={fmt(value)}
                                            data-tip-color={barColor}
                                        />
                                        {showDataLabels ? (
                                            <text
                                                x={x + barThickness / 2}
                                                y={value >= 0 ? Math.min(y, geometry.baselineY) - 4 : Math.max(y, geometry.baselineY) + 12}
                                                textAnchor="middle"
                                                fontSize="9"
                                                fontWeight="700"
                                                fill={theme.textSec}
                                                style={{ pointerEvents: 'none' }}
                                            >
                                                {fmt(value)}
                                            </text>
                                        ) : null}
                                    </React.Fragment>
                                );
                            })}
                        </g>
                    );
                })}

                {(chartType === 'line' || chartType === 'area') && (() => {
                    const positiveOffsets = model.categories.map(() => 0);
                    const negativeOffsets = model.categories.map(() => 0);
                    return model.series.map((series, seriesIndex) => {
                        if (hiddenSeries.has(series.name)) return null;
                        const stroke = getColorForIndex(seriesIndex, theme, paletteColors);
                        const points = model.categories.map((_, categoryIndex) => {
                            const rawValue = series.values[categoryIndex];
                            if (rawValue === null || rawValue === undefined) return null;
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            if (stackedAreaMode) {
                                const startValue = rawValue >= 0 ? positiveOffsets[categoryIndex] : negativeOffsets[categoryIndex];
                                const endValue = startValue + rawValue;
                                if (rawValue >= 0) positiveOffsets[categoryIndex] = endValue;
                                else negativeOffsets[categoryIndex] = endValue;
                                return {
                                    x,
                                    y: geometry.scaleY(endValue),
                                    baseY: geometry.scaleY(startValue),
                                    rawValue,
                                };
                            }
                            return {
                                x,
                                y: geometry.scaleY(rawValue),
                                baseY: geometry.baselineY,
                                rawValue,
                            };
                        });
                        const linePath = buildLinePath(points);
                        const areaPath = chartType === 'area' ? buildAreaBandPath(points) : '';
                        return (
                            <g key={`${series.name}-${chartType}`}>
                                {chartType === 'area' && areaPath && (
                                    <path d={areaPath} fill={stroke} opacity={stackedAreaMode ? '0.46' : '0.16'} style={{ pointerEvents: 'none' }} />
                                )}
                                {linePath && (
                                    <path d={linePath} fill="none" stroke={stroke} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" style={{ pointerEvents: 'none' }} />
                                )}
                                {points.map((point, pointIndex) => {
                                    if (!point) return null;
                                    return (
                                        <React.Fragment key={`${series.name}-${pointIndex}`}>
                                            <circle
                                                cx={point.x}
                                                cy={point.y}
                                                r="4"
                                                fill={stroke}
                                                stroke={theme.surfaceBg || theme.background || '#fff'}
                                                strokeWidth="2"
                                                data-tip-cat={model.categories[pointIndex]}
                                                data-tip-ser={series.name}
                                                data-tip-val={fmt(point.rawValue)}
                                                data-tip-color={stroke}
                                            />
                                            {showDataLabels ? (
                                                <text
                                                    x={point.x}
                                                    y={point.y - 8}
                                                    textAnchor="middle"
                                                    fontSize="9"
                                                    fontWeight="700"
                                                    fill={theme.textSec}
                                                    style={{ pointerEvents: 'none' }}
                                                >
                                                    {fmt(point.rawValue)}
                                                </text>
                                            ) : null}
                                        </React.Fragment>
                                    );
                                })}
                            </g>
                        );
                    });
                })()}

                    {canActivateCategories && model.categories.map((_, categoryIndex) => {
                        const target = categoryTargets[categoryIndex];
                        if (!target) return null;
                        if (geometry.horizontalBarMode) {
                            return (
                                <rect
                                    key={`category-hit-horizontal-${categoryIndex}`}
                                    x={geometry.margin.left}
                                    y={geometry.margin.top + (categoryIndex * geometry.step)}
                                    width={resolvedChartWidth - geometry.margin.left - geometry.margin.right}
                                    height={geometry.step}
                                    fill="transparent"
                                    style={{ cursor: 'pointer' }}
                                    onClick={() => onCategoryActivate(target)}
                                />
                            );
                        }
                        return (
                            <rect
                                key={`category-hit-vertical-${categoryIndex}`}
                                x={geometry.margin.left + (categoryIndex * geometry.step)}
                                y={geometry.margin.top}
                                width={geometry.step}
                                height={effectiveChartHeight - geometry.margin.top - geometry.margin.bottom}
                                fill="transparent"
                                style={{ cursor: 'pointer' }}
                                onClick={() => onCategoryActivate(target)}
                            />
                        );
                    })}

                    {stackedChildBarMode ? null : geometry.horizontalBarMode ? (
                    geometry.hasHierarchicalBands ? (
                        <>
                            {groupedBands.map((group) => {
                                const yStart = geometry.margin.top + (group.startIndex * geometry.step);
                                const yEnd = geometry.margin.top + ((group.endIndex + 1) * geometry.step);
                                const centerY = (yStart + yEnd) / 2;
                                return (
                                    <g key={`outer-label-horizontal-${group.groupKey}`}>
                                        <text
                                            x={10}
                                            y={centerY + 4}
                                        textAnchor="start"
                                        fontSize="11"
                                        fontWeight="700"
                                        fill={theme.text}
                                    >
                                            {truncateChartLabel(group.outerLabel, 18)}
                                        </text>
                                    </g>
                                );
                            })}
                            {model.categoryBands.map((band, categoryIndex) => {
                                const y = geometry.margin.top + (categoryIndex * geometry.step) + (geometry.step / 2);
                                const innerLabel = band && band.innerLabel ? String(band.innerLabel) : '';
                                return innerLabel ? (
                                    <text
                                        key={`inner-label-horizontal-${categoryIndex}`}
                                        x={geometry.margin.left - 10}
                                        y={y + 4}
                                        textAnchor="end"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(innerLabel, 18)}
                                    </text>
                                ) : null;
                            })}
                        </>
                    ) : (
                        model.categories.map((category, categoryIndex) => {
                            const y = geometry.margin.top + (categoryIndex * geometry.step) + (geometry.step / 2);
                            return (
                                <text
                                    key={`label-horizontal-${categoryIndex}`}
                                    x={geometry.margin.left - 10}
                                    y={y + 4}
                                    textAnchor="end"
                                    fontSize="11"
                                    fill={theme.textSec}
                                >
                                    {truncateChartLabel(category, 22)}
                                </text>
                            );
                        })
                    )
                ) : geometry.hasHierarchicalBands ? (
                    <>
                        {groupedBands.map((group) => {
                            const xStart = geometry.margin.left + (group.startIndex * geometry.step);
                            const xEnd = geometry.margin.left + ((group.endIndex + 1) * geometry.step);
                            const centerX = (xStart + xEnd) / 2;
                            return (
                                <g key={`outer-label-${group.groupKey}`}>
                                    <text
                                        x={centerX}
                                        y={effectiveChartHeight - geometry.margin.bottom + 68}
                                        textAnchor="middle"
                                        fontSize="11"
                                        fontWeight="700"
                                        fill={theme.text}
                                    >
                                        {truncateChartLabel(group.outerLabel, 22)}
                                    </text>
                                </g>
                            );
                        })}
                        {model.categoryBands.map((band, categoryIndex) => {
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            const innerLabel = band && band.innerLabel ? String(band.innerLabel) : '';
                            return (
                                <g key={`inner-label-${categoryIndex}`}>
                                    {innerLabel ? (
                                        <text
                                            x={x}
                                            y={effectiveChartHeight - geometry.margin.bottom + 30}
                                            textAnchor="middle"
                                            fontSize="11"
                                            fill={theme.textSec}
                                        >
                                            {truncateChartLabel(innerLabel, 18)}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </>
                ) : (
                    model.categories.map((category, categoryIndex) => {
                        const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                        return (
                            <g key={`label-${categoryIndex}`} transform={`translate(${x}, ${effectiveChartHeight - geometry.margin.bottom + 18}) rotate(28)`}>
                                <text textAnchor="start" fontSize="11" fill={theme.textSec}>
                                    {truncateChartLabel(category, 20)}
                                </text>
                            </g>
                        );
                    })
                    )}
                </svg>
                {chartTitleOverlay}{tooltipElement}
            </div>
            {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} /> : null}
        </div>
    );
};

const ChartSurface = ({
    model,
    chartType,
    chartLayers,
    onChartLayersChange,
    availableColumns,
    barLayout,
    axisMode,
    onChartTypeChange,
    onBarLayoutChange,
    onAxisModeChange,
    orientation,
    onOrientationChange,
    hierarchyLevel,
    onHierarchyLevelChange,
    theme,
    title,
    onTitleChange,
    subtitle,
    note,
    chartHeight: chartHeightProp,
    onChartHeightChange,
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    sortMode,
    onSortModeChange,
    interactionMode,
    onInteractionModeChange,
    serverScope,
    onServerScopeChange,
    showServerScope = false,
    floating = false,
    cinemaMode = false,
    onCategoryActivate,
    allowHierarchyCharts = true,
    locked = false,
    onToggleLock,
    configOpen: controlledConfigOpen,
    onConfigChange,
    settingsPanelWidth: controlledSettingsPanelWidth,
    onSettingsPanelWidthChange,
}) => {
    const isComboChart = chartType === 'combo';
    const isSparklineChart = chartType === 'sparkline';
    const canStack = isComboChart
        ? false
        : chartType === 'area'
        ? canStackAreaSeries(model)
        : canStackBarLayout(model);
    const resolvedTitle = title !== undefined ? title : (model && model.title);
    const resolvedSubtitle = subtitle !== undefined ? subtitle : (model && model.subtitle);
    const resolvedNote = note !== undefined
        ? note
        : locked
            ? (floating ? 'Locked to the current chart request and floating frame' : 'Locked to the current chart request')
            : (model && model.note);
    const [uncontrolledConfigOpen, setUncontrolledConfigOpen] = useState(false);
    const configOpen = typeof controlledConfigOpen === 'boolean' ? controlledConfigOpen : uncontrolledConfigOpen;
    const setConfigOpen = typeof onConfigChange === 'function' ? onConfigChange : setUncontrolledConfigOpen;
    const [showDataLabels, setShowDataLabels] = useState(false);
    const [colorPalette, setColorPalette] = useState('default');
    const [valueFormat, setValueFormat] = useState('auto');
    const [yAxisTitle, setYAxisTitle] = useState('');
    const chartHeightResizeRef = useRef(null);
    const settingsPaneResizeRef = useRef(null);
    const cinemaContainerRef = useRef(null);
    const [cinemaAutoHeight, setCinemaAutoHeight] = useState(null);
    const [uncontrolledSettingsPanelWidth, setUncontrolledSettingsPanelWidth] = useState(348);
    const settingsPanelWidth = Number.isFinite(Number(controlledSettingsPanelWidth))
        ? Math.max(300, Math.min(520, Math.floor(Number(controlledSettingsPanelWidth))))
        : uncontrolledSettingsPanelWidth;
    const setSettingsPanelWidth = typeof onSettingsPanelWidthChange === 'function'
        ? onSettingsPanelWidthChange
        : setUncontrolledSettingsPanelWidth;
    const showChrome = !cinemaMode;
    const chartHeight = cinemaMode && cinemaAutoHeight
        ? Math.max(180, cinemaAutoHeight)
        : Math.max(180, Number.isFinite(Number(chartHeightProp)) ? Number(chartHeightProp) : CHART_HEIGHT);
    const svgRef = useRef(null);
    const maxHierarchyLevel = (model && model.maxHierarchyLevel) || 1;
    const showHierarchySection = typeof onHierarchyLevelChange === 'function' && maxHierarchyLevel > 1;
    const configButtonStyle = {
        width: '32px',
        height: '32px',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: `1px solid ${configOpen ? theme.primary : theme.border}`,
        background: configOpen ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: configOpen ? theme.primary : theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        cursor: 'pointer',
        flexShrink: 0,
    };
    const lockButtonStyle = {
        ...configButtonStyle,
        border: `1px solid ${locked ? theme.primary : theme.border}`,
        background: locked ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: locked ? theme.primary : theme.textSec,
    };
    const exportButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.hover,
        color: theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    };
    const compactInputStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 600,
        minWidth: '68px',
    };
    const applyChartPreset = useCallback((preset) => {
        if (preset === 'trend') {
            onChartTypeChange('line');
            if (typeof onOrientationChange === 'function') onOrientationChange('rows');
            if (typeof onSortModeChange === 'function') onSortModeChange('natural');
            return;
        }
        if (preset === 'compare') {
            onChartTypeChange('bar');
            if (typeof onBarLayoutChange === 'function') onBarLayoutChange('grouped');
            if (typeof onAxisModeChange === 'function') onAxisModeChange('vertical');
            return;
        }
        if (preset === 'composition') {
            onChartTypeChange(showHierarchySection ? 'sunburst' : 'donut');
            return;
        }
        if (preset === 'flow') {
            onChartTypeChange('waterfall');
            return;
        }
        if (preset === 'sparkline') {
            onChartTypeChange('sparkline');
        }
    }, [
        onAxisModeChange,
        onBarLayoutChange,
        onChartTypeChange,
        onOrientationChange,
        onSortModeChange,
        showHierarchySection,
    ]);

    useEffect(() => {
        if (!cinemaMode || !configOpen) return;
        setConfigOpen(false);
    }, [cinemaMode, configOpen]);

    useEffect(() => {
        if (!cinemaMode) { setCinemaAutoHeight(null); return; }
        const el = cinemaContainerRef.current;
        if (!el) return;
        const update = () => { const h = el.clientHeight; if (h > 0) setCinemaAutoHeight(h); };
        update();
        if (typeof ResizeObserver === 'undefined') return;
        const ro = new ResizeObserver(update);
        ro.observe(el);
        return () => ro.disconnect();
    }, [cinemaMode]);

    useEffect(() => {
        const handlePointerMove = (event) => {
            if (!chartHeightResizeRef.current || typeof onChartHeightChange !== 'function') return;
            const resizeState = chartHeightResizeRef.current;
            const nextHeight = Math.max(180, resizeState.startHeight + (event.clientY - resizeState.startY));
            onChartHeightChange(nextHeight);
        };

        const stopResize = () => {
            chartHeightResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('mouseleave', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('mouseleave', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, [onChartHeightChange]);

    useEffect(() => {
        const handlePointerMove = (event) => {
            if (!settingsPaneResizeRef.current) return;
            const resizeState = settingsPaneResizeRef.current;
            const delta = resizeState.direction === 'left'
                ? (resizeState.startX - event.clientX)
                : (event.clientX - resizeState.startX);
            const nextWidth = Math.max(300, Math.min(520, resizeState.startWidth + delta));
            setSettingsPanelWidth(nextWidth);
        };

        const stopResize = () => {
            settingsPaneResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('mouseleave', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('mouseleave', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, []);

    return (
        <div
            ref={cinemaContainerRef}
            style={{
                display: 'flex',
                flexDirection: 'column',
                gap: showChrome ? '14px' : '10px',
                flex: '1 1 auto',
                minHeight: 0,
                overflow: 'hidden',
            }}
        >
            {showChrome ? (
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                    <ChartHeader
                        subtitle={resolvedSubtitle}
                        note={resolvedNote}
                        theme={theme}
                    />
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        <button
                            type="button"
                            onClick={() => exportSvgNode(svgRef.current, 'pivot-chart')}
                            style={exportButtonStyle}
                            title="Export chart as SVG"
                        >
                            SVG
                        </button>
                        <button
                            type="button"
                            onClick={() => exportSvgAsPng(svgRef.current, 'pivot-chart')}
                            style={exportButtonStyle}
                            title="Export chart as PNG"
                        >
                            PNG
                        </button>
                        <button
                            type="button"
                            onClick={() => copySvgToClipboard(svgRef.current)}
                            style={exportButtonStyle}
                            title="Copy chart to clipboard"
                        >
                            Copy
                        </button>
                        <button
                            type="button"
                            onClick={() => exportChartCsv(model, 'pivot-chart')}
                            style={exportButtonStyle}
                            title="Export chart data as CSV"
                        >
                            CSV
                        </button>
                        {typeof onToggleLock === 'function' ? (
                            <button
                                type="button"
                                onClick={onToggleLock}
                                style={lockButtonStyle}
                                title={locked ? (floating ? 'Unlock chart request and floating frame' : 'Unlock chart request') : (floating ? 'Lock current chart request and floating frame' : 'Lock current chart request')}
                                aria-label={locked ? (floating ? 'Unlock chart request and floating frame' : 'Unlock chart request') : (floating ? 'Lock current chart request and floating frame' : 'Lock current chart request')}
                            >
                                <Icons.Lock />
                            </button>
                        ) : null}
                        <button
                            type="button"
                            onClick={() => setConfigOpen(!configOpen)}
                            data-chart-settings-toggle="true"
                            style={configButtonStyle}
                            title={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                            aria-label={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                        >
                            <Icons.Settings />
                        </button>
                    </div>
                </div>
            ) : null}
        <div style={{ display: 'flex', minHeight: 0, gap: 0, overflow: 'hidden', flex: '1 1 auto' }}>
        <div
            data-chart-surface-main="true"
            data-chart-surface-scroll="true"
            style={{
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
                minWidth: 0,
                flex: '1 1 auto',
                overflowY: 'auto',
                overflowX: 'hidden',
                overscrollBehavior: 'contain',
                paddingRight: configOpen ? '4px' : '0',
            }}
        >
            {!model
                ? <EmptyChartState message="No chart data available." theme={theme} chartHeight={chartHeight} />
                    : ((!Array.isArray(model.series) || model.series.length === 0)
                    && chartType !== 'icicle'
                    && chartType !== 'sunburst'
                    && chartType !== 'sankey'
                    ? <EmptyChartState message={model.emptyMessage || 'No chart data available.'} theme={theme} chartHeight={chartHeight} />
                    : <SvgChart model={model} chartType={chartType} barLayout={barLayout} axisMode={axisMode} theme={theme} chartHeight={chartHeight} showLegend={!isSparklineChart} showDataLabels={showDataLabels} onCategoryActivate={onCategoryActivate} svgRef={svgRef} colorPalette={colorPalette} valueFormat={valueFormat} yAxisTitle={yAxisTitle} chartTitle={resolvedTitle} onTitleChange={onTitleChange} />)}
            {!cinemaMode && typeof onChartHeightChange === 'function' ? (
                <div
                    onMouseDown={(event) => {
                        event.preventDefault();
                        chartHeightResizeRef.current = {
                            startY: event.clientY,
                            startHeight: chartHeight,
                        };
                    }}
                    style={{
                        height: '22px',
                        cursor: 'ns-resize',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        gap: '8px',
                        marginTop: '6px',
                        color: theme.textSec,
                        fontSize: '11px',
                        fontWeight: 700,
                    }}
                    title="Drag to resize the graph height"
                >
                    <span>Graph Height</span>
                    <div style={{ width: '72px', height: '5px', borderRadius: '999px', background: theme.border, opacity: 0.95 }} />
                </div>
            ) : null}
        </div>
        {showChrome && configOpen ? (
            <>
                <div
                    data-chart-settings-resizer="true"
                    onMouseDown={(event) => {
                        event.preventDefault();
                        settingsPaneResizeRef.current = {
                            startX: event.clientX,
                            startWidth: settingsPanelWidth,
                            direction: 'right',
                        };
                    }}
                    style={{
                        width: '8px',
                        cursor: 'col-resize',
                        background: 'transparent',
                        position: 'relative',
                        flexShrink: 0,
                    }}
                    title="Resize chart settings"
                >
                    <div style={{
                        position: 'absolute',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '3px',
                        height: '78px',
                        borderRadius: '999px',
                        background: theme.border,
                        opacity: 0.92,
                    }} />
                </div>
                <aside
                    data-chart-settings-pane="true"
                    style={{
                        width: `${settingsPanelWidth}px`,
                        minWidth: '300px',
                        maxWidth: '520px',
                        flexShrink: 0,
                        minHeight: 0,
                        overflow: 'hidden',
                        borderLeft: `1px solid ${theme.border}`,
                        background: theme.sidebarBg || theme.surfaceBg || theme.background,
                        display: 'flex',
                        flexDirection: 'column',
                    }}
                >
                <div
                    data-chart-settings-scroll="true"
                    style={{
                        overflowY: 'auto',
                        overflowX: 'hidden',
                        overscrollBehavior: 'contain',
                        padding: '14px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '10px',
                        flex: 1,
                        minHeight: 0,
                        background: theme.sidebarBg || theme.surfaceBg || theme.background,
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '6px' }}>
                        <div style={{ fontSize: '15px', fontWeight: 800, color: theme.text }}>Chart Settings</div>
                        <button
                            type="button"
                            onClick={() => setConfigOpen(false)}
                            style={exportButtonStyle}
                        >
                            Close
                        </button>
                    </div>
                <ChartConfigSection title="Presets" theme={theme} defaultCollapsed>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '6px' }}>
                        {[
                            { key: 'trend', label: 'Trend' },
                            { key: 'compare', label: 'Compare' },
                            { key: 'composition', label: 'Composition' },
                            { key: 'flow', label: 'Flow' },
                            { key: 'sparkline', label: 'Sparkline' },
                        ].map((preset) => (
                            <button
                                key={preset.key}
                                type="button"
                                onClick={() => applyChartPreset(preset.key)}
                                style={{
                                    ...exportButtonStyle,
                                    textAlign: 'left',
                                    justifyContent: 'flex-start',
                                }}
                            >
                                {preset.label}
                            </button>
                        ))}
                    </div>
                </ChartConfigSection>
                <ChartConfigSection title="General" theme={theme} defaultCollapsed>
                    {typeof onTitleChange === 'function' ? (
                        <ChartConfigField label="Title" theme={theme} controlMinWidth="100%">
                            <input
                                type="text"
                                value={resolvedTitle || ''}
                                onChange={(event) => onTitleChange(event.target.value)}
                                placeholder="Chart title"
                                style={{ ...compactInputStyle, width: '100%' }}
                            />
                        </ChartConfigField>
                    ) : null}
                    {typeof onChartHeightChange === 'function' ? (
                        <ChartConfigField label="Height" theme={theme} controlMinWidth="96px">
                            <input
                                type="number"
                                min="180"
                                step="20"
                                value={chartHeight}
                                onChange={(event) => onChartHeightChange(event.target.value)}
                                style={compactInputStyle}
                            />
                        </ChartConfigField>
                    ) : null}
                    <ChartConfigField label="Limits" theme={theme} controlMinWidth="100%">
                        <ChartLimitInputs
                            rowLimit={rowLimit}
                            onRowLimitChange={onRowLimitChange}
                            columnLimit={columnLimit}
                            onColumnLimitChange={onColumnLimitChange}
                            theme={theme}
                        />
                    </ChartConfigField>
                </ChartConfigSection>

                <ChartConfigSection title="Type" theme={theme} defaultCollapsed={false}>
                    <ChartConfigField label="Type" theme={theme} controlMinWidth="100%">
                        <ChartTypeButtons chartType={chartType} onChange={onChartTypeChange} theme={theme} includeHierarchyCharts={allowHierarchyCharts} />
                    </ChartConfigField>
                </ChartConfigSection>

                <ChartConfigSection title="View" theme={theme} defaultCollapsed={false}>
                    {(chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'scatter' && chartType !== 'waterfall' && !isComboChart) ? (
                        <ChartConfigField label="Source" theme={theme}>
                            <ChartOrientationButtons orientation={orientation} onChange={onOrientationChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                    {(chartType === 'bar') ? (
                        <ChartConfigField label="Direction" theme={theme}>
                            <ChartAxisButtons axisMode={axisMode} onChange={onAxisModeChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                    {(chartType === 'bar' || chartType === 'area') && !isComboChart && (
                        <ChartConfigField label="Layout" theme={theme}>
                            <ChartLayoutButtons chartType={chartType} barLayout={barLayout} onChange={onBarLayoutChange} canStack={canStack} theme={theme} />
                        </ChartConfigField>
                    )}
                    {typeof onSortModeChange === 'function' ? (
                        <ChartConfigField label="Sort" theme={theme} controlMinWidth="100%">
                            <ChartSortButtons sortMode={sortMode} onChange={onSortModeChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'sparkline' ? (
                        <ChartConfigField label="Labels" theme={theme}>
                            <button
                                type="button"
                                onClick={() => setShowDataLabels((v) => !v)}
                                style={{
                                    border: `1px solid ${showDataLabels ? theme.primary : theme.border}`,
                                    background: showDataLabels ? theme.select : (theme.headerSubtleBg || theme.hover),
                                    color: showDataLabels ? theme.primary : theme.text,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    cursor: 'pointer',
                                }}
                            >
                                {showDataLabels ? 'Values On' : 'Values Off'}
                            </button>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' ? (
                        <ChartConfigField label="Format" theme={theme} controlMinWidth="100%">
                            <select
                                value={valueFormat}
                                onChange={(event) => setValueFormat(event.target.value)}
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    background: theme.surfaceBg || theme.background || '#fff',
                                    color: theme.text,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    width: '100%',
                                }}
                                title="Value format for axis labels"
                            >
                                {VALUE_FORMAT_MODES.map((opt) => (
                                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                                ))}
                            </select>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'sparkline' ? (
                        <ChartConfigField label="Y-Axis" theme={theme} controlMinWidth="100%">
                            <input
                                type="text"
                                value={yAxisTitle}
                                onChange={(event) => setYAxisTitle(event.target.value)}
                                placeholder="Y-axis title"
                                style={{
                                    border: `1px solid ${theme.border}`,
                                    background: theme.surfaceBg || theme.background || '#fff',
                                    color: theme.text,
                                    borderRadius: theme.radiusSm || '8px',
                                    padding: '6px 8px',
                                    fontSize: '11px',
                                    fontWeight: 700,
                                    width: '100%',
                                }}
                            />
                        </ChartConfigField>
                    ) : null}
                </ChartConfigSection>

                <ChartConfigSection title="Colors" theme={theme}>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                        {COLOR_PALETTE_NAMES.map((name) => {
                            const active = colorPalette === name;
                            const swatches = COLOR_PALETTES[name];
                            return (
                                <button
                                    key={name}
                                    type="button"
                                    onClick={() => setColorPalette(name)}
                                    title={name.charAt(0).toUpperCase() + name.slice(1)}
                                    style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '3px',
                                        padding: '3px 6px',
                                        border: `1.5px solid ${active ? theme.primary : theme.border}`,
                                        background: active ? theme.select : (theme.headerSubtleBg || theme.hover),
                                        borderRadius: theme.radiusSm || '8px',
                                        cursor: 'pointer',
                                    }}
                                >
                                    {swatches.slice(0, 4).map((hex, i) => (
                                        <span
                                            key={i}
                                            style={{
                                                width: '8px',
                                                height: '8px',
                                                borderRadius: '50%',
                                                background: hex,
                                                flexShrink: 0,
                                            }}
                                        />
                                    ))}
                                    <span style={{
                                        fontSize: '9px',
                                        fontWeight: active ? 700 : 600,
                                        color: active ? theme.primary : theme.textSec,
                                        marginLeft: '1px',
                                        textTransform: 'capitalize',
                                    }}>
                                        {name === 'a11y' ? 'A11y' : name}
                                    </span>
                                </button>
                            );
                        })}
                    </div>
                </ChartConfigSection>

                {isComboChart ? (
                    <ChartConfigSection title="Layers" theme={theme}>
                        <ChartConfigField label="Stack" theme={theme} controlMinWidth="100%">
                            <ChartLayerEditor layers={chartLayers} onChange={onChartLayersChange} availableColumns={availableColumns} theme={theme} />
                        </ChartConfigField>
                    </ChartConfigSection>
                ) : null}

                {showHierarchySection ? (
                    <ChartConfigSection title="Level" theme={theme}>
                        <ChartConfigField label="Level" theme={theme}>
                            <ChartHierarchyButtons level={hierarchyLevel} onChange={onHierarchyLevelChange} maxLevel={maxHierarchyLevel} theme={theme} />
                        </ChartConfigField>
                    </ChartConfigSection>
                ) : null}

                <ChartConfigSection title="Actions" theme={theme}>
                    {typeof onInteractionModeChange === 'function' ? (
                        <ChartConfigField label="Click" theme={theme}>
                            <ChartInteractionButtons interactionMode={interactionMode} onChange={onInteractionModeChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                    {showServerScope && typeof onServerScopeChange === 'function' ? (
                        <ChartConfigField label="Scope" theme={theme}>
                            <ChartScopeButtons scope={serverScope} onChange={onServerScopeChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                </ChartConfigSection>
            </div>
            </aside>
            </>
        ) : null}
        </div>
        {showChrome ? <ChartStats model={model} theme={theme} /> : null}
    </div>
    );
};

export const PivotChartPanel = ({
    open,
    onClose,
    source,
    onSourceChange,
    chartType,
    onChartTypeChange,
    chartLayers = [],
    onChartLayersChange,
    availableColumns = [],
    barLayout,
    onBarLayoutChange,
    axisMode,
    onAxisModeChange,
    orientation,
    onOrientationChange,
    hierarchyLevel,
    onHierarchyLevelChange,
    model,
    theme,
    width,
    chartTitle,
    onChartTitleChange,
    chartHeight,
    onChartHeightChange,
    onResizeStart,
    floating = false,
    onToggleFloating,
    floatingRect = null,
    onFloatingDragStart,
    onFloatingResizeStart,
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    sortMode,
    onSortModeChange,
    interactionMode,
    onInteractionModeChange,
    serverScope,
    onServerScopeChange,
    chartDefinitions = [],
    activeChartId = null,
    onActiveChartChange,
    onCreateChart,
    onDuplicateChart,
    onDeleteChart,
    onRenameChart,
    onCategoryActivate,
    allowHierarchyCharts = true,
    showServerScope = false,
    locked = false,
    onToggleLock,
    dockPosition = 'right',
    onDockPositionChange,
    cinemaMode: controlledCinemaMode,
    onCinemaModeChange,
    onSettingsWidthBudgetChange,
    standalone = false,
    showResizeHandle = true,
    title = 'Chart Panel',
    showDefinitionManager = true,
}) => {
    const [fullscreenMode, setFullscreenMode] = useState(false);
    const [uncontrolledCinemaMode, setUncontrolledCinemaMode] = useState(false);
    const [settingsPaneOpen, setSettingsPaneOpen] = useState(false);
    const [settingsPanelWidth, setSettingsPanelWidth] = useState(348);
    const panelAsideRef = useRef(null);
    const baseDockedPanelWidthRef = useRef(Math.max(width || 430, 430));
    if (!open) return null;
    const cinemaMode = typeof controlledCinemaMode === 'boolean' ? controlledCinemaMode : uncontrolledCinemaMode;
    const toggleCinemaMode = () => {
        const nextValue = !cinemaMode;
        if (typeof onCinemaModeChange === 'function') {
            onCinemaModeChange(nextValue);
            return;
        }
        setUncontrolledCinemaMode(nextValue);
    };
    const floatingInteractionLocked = floating && locked;

    const sourceButtonStyle = (value) => ({
        border: `1px solid ${value === source ? theme.primary : theme.border}`,
        background: value === source ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === source ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });
    const actionButtonStyle = (active = false) => ({
        border: `1px solid ${active ? theme.primary : theme.border}`,
        background: active ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: active ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '6px',
        minWidth: '32px',
    });
    const dockButtonStyle = (value) => ({
        ...actionButtonStyle(dockPosition === value),
        minWidth: '56px',
        padding: '6px 10px',
    });
    const visibleSettingsWidth = (!cinemaMode && settingsPaneOpen) ? (settingsPanelWidth + 9) : 0;
    const basePanelWidth = Math.max(width || 430, 430);
    const floatingPanelWidth = (floatingRect && floatingRect.width) || basePanelWidth;

    const panelContainerStyle = fullscreenMode
        ? {
            position: 'fixed',
            inset: '16px',
            zIndex: 10040,
            display: 'flex',
            minWidth: 0,
            minHeight: 0,
        }
        : floating
            ? {
                position: 'absolute',
                left: `${(floatingRect && floatingRect.left) || 24}px`,
                top: `${(floatingRect && floatingRect.top) || 24}px`,
                width: `${floatingPanelWidth + visibleSettingsWidth}px`,
                height: `${(floatingRect && floatingRect.height) || 520}px`,
                zIndex: 240,
                display: 'flex',
                minWidth: 0,
                minHeight: 0,
            }
            : standalone
                ? { display: 'flex', flex: '1 1 auto', minWidth: 0, minHeight: 0, overflow: 'hidden' }
                : {
                    display: 'flex',
                    width: `${basePanelWidth + visibleSettingsWidth}px`,
                    minWidth: `${basePanelWidth + visibleSettingsWidth}px`,
                    maxWidth: `${basePanelWidth + visibleSettingsWidth}px`,
                    flexShrink: 0,
                };

    useEffect(() => {
        if (!standalone || floating || fullscreenMode) return undefined;
        const element = panelAsideRef.current;
        if (!element) return undefined;
        const updateWidth = () => {
            const nextWidth = element.clientWidth;
            if (nextWidth > 0 && !settingsPaneOpen) {
                baseDockedPanelWidthRef.current = nextWidth;
            }
        };
        updateWidth();
        if (typeof ResizeObserver === 'undefined') return undefined;
        const observer = new ResizeObserver(updateWidth);
        observer.observe(element);
        return () => observer.disconnect();
    }, [floating, fullscreenMode, settingsPaneOpen, standalone]);

    useEffect(() => {
        if (typeof onSettingsWidthBudgetChange !== 'function') return undefined;
        if (!standalone || floating || fullscreenMode || cinemaMode || !settingsPaneOpen) {
            onSettingsWidthBudgetChange(null);
            return undefined;
        }
        const baseWidthHint = baseDockedPanelWidthRef.current || basePanelWidth;
        onSettingsWidthBudgetChange(Math.max(320, Math.ceil(baseWidthHint + visibleSettingsWidth)));
        return () => {
            onSettingsWidthBudgetChange(null);
        };
    }, [
        basePanelWidth,
        cinemaMode,
        floating,
        fullscreenMode,
        onSettingsWidthBudgetChange,
        settingsPaneOpen,
        standalone,
        visibleSettingsWidth,
    ]);

    return (
        <div style={panelContainerStyle}>
            {!fullscreenMode && !floating && !standalone && showResizeHandle ? (
                <div
                    onMouseDown={(event) => {
                        event.preventDefault();
                        if (onResizeStart) onResizeStart();
                    }}
                    style={{
                        width: '8px',
                        cursor: 'col-resize',
                        background: 'transparent',
                        position: 'relative',
                        flexShrink: 0,
                    }}
                    title="Resize chart panel"
                >
                    <div style={{
                        position: 'absolute',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '3px',
                        height: '72px',
                        borderRadius: '999px',
                        background: theme.border,
                        opacity: 0.9,
                    }} />
                </div>
            ) : null}
            <aside ref={panelAsideRef} style={{
                width: fullscreenMode || floating || standalone ? '100%' : `calc(100% - ${showResizeHandle ? 8 : 0}px)`,
                height: fullscreenMode || floating || standalone ? '100%' : 'auto',
                minWidth: 0,
                minHeight: 0,
                borderLeft: standalone || floating || fullscreenMode ? 'none' : `1px solid ${theme.border}`,
                border: floating || fullscreenMode ? `1px solid ${theme.border}` : 'none',
                borderRadius: floating || fullscreenMode ? (theme.radius || '16px') : 0,
                background: theme.sidebarBg || theme.surfaceBg || theme.background,
                padding: fullscreenMode ? '12px' : '14px',
                overflow: 'hidden',
                boxShadow: floating || fullscreenMode
                    ? (theme.shadowMd || '0 18px 40px rgba(15,23,42,0.24)')
                    : standalone ? 'none' : `-10px 0 24px ${theme.pinnedBoundaryShadow || 'rgba(15,23,42,0.12)'}`,
                display: 'flex',
                flexDirection: 'column',
                gap: cinemaMode ? '8px' : '0',
                position: 'relative',
            }}>
                <div
                    onMouseDown={(event) => {
                        if (floating && !fullscreenMode && !floatingInteractionLocked && typeof onFloatingDragStart === 'function') {
                            onFloatingDragStart(event);
                        }
                    }}
                    style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: cinemaMode ? '0' : '12px', cursor: floating && !fullscreenMode && !floatingInteractionLocked ? 'move' : 'default' }}
                >
                    {cinemaMode ? <div /> : (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', minWidth: 0 }}>
                            <div style={{ fontSize: '15px', fontWeight: 800, color: theme.text }}>{title}</div>
                        </div>
                    )}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }} onMouseDown={(event) => event.stopPropagation()}>
                        {typeof onToggleFloating === 'function' ? (
                            <button
                                type="button"
                                onClick={onToggleFloating}
                                style={actionButtonStyle(floating)}
                                title={floating ? 'Dock chart panel' : 'Float chart over table'}
                                aria-label={floating ? 'Dock chart panel' : 'Float chart over table'}
                            >
                                {floating ? 'Dock' : 'Float'}
                            </button>
                        ) : null}
                        <button
                            type="button"
                            onClick={toggleCinemaMode}
                            style={actionButtonStyle(cinemaMode)}
                            title={cinemaMode ? 'Exit cinema mode' : 'Enter cinema mode'}
                            aria-label={cinemaMode ? 'Exit cinema mode' : 'Enter cinema mode'}
                        >
                            Cinema
                        </button>
                        <button
                            type="button"
                            onClick={() => setFullscreenMode((isActive) => !isActive)}
                            style={actionButtonStyle(fullscreenMode)}
                            title={fullscreenMode ? 'Exit full screen' : 'Open chart full screen'}
                            aria-label={fullscreenMode ? 'Exit full screen' : 'Open chart full screen'}
                        >
                            {fullscreenMode ? <Icons.FullscreenExit /> : <Icons.Fullscreen />}
                        </button>
                        <button
                            onClick={onClose}
                            style={actionButtonStyle(false)}
                            title={standalone ? 'Close chart pane' : 'Close chart panel'}
                            aria-label={standalone ? 'Close chart pane' : 'Close chart panel'}
                        >
                            {fullscreenMode ? <Icons.Close /> : 'Close'}
                        </button>
                    </div>
                </div>
                {!cinemaMode && showDefinitionManager ? (
                    <div style={{ display: 'flex', gap: '8px', marginBottom: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
                        {Array.isArray(chartDefinitions) && chartDefinitions.length > 0 ? (
                            <>
                                <select
                                    value={activeChartId || ''}
                                    onChange={(event) => {
                                        if (typeof onActiveChartChange === 'function') onActiveChartChange(event.target.value);
                                    }}
                                    style={{
                                        border: `1px solid ${theme.border}`,
                                        background: theme.surfaceBg || theme.background || '#fff',
                                        color: theme.text,
                                        borderRadius: theme.radiusSm || '8px',
                                        padding: '6px 8px',
                                        fontSize: '11px',
                                        fontWeight: 700,
                                        minWidth: '160px',
                                    }}
                                >
                                    {chartDefinitions.map((definition) => (
                                        <option key={definition.id} value={definition.id}>{definition.name || definition.id}</option>
                                    ))}
                                </select>
                                <input
                                    type="text"
                                    value={(chartDefinitions.find((definition) => definition.id === activeChartId) || {}).name || ''}
                                    onChange={(event) => {
                                        if (typeof onRenameChart === 'function') onRenameChart(event.target.value);
                                    }}
                                    placeholder="Chart name"
                                    style={{
                                        border: `1px solid ${theme.border}`,
                                        background: theme.surfaceBg || theme.background || '#fff',
                                        color: theme.text,
                                        borderRadius: theme.radiusSm || '8px',
                                        padding: '6px 8px',
                                        fontSize: '11px',
                                        fontWeight: 700,
                                        minWidth: '180px',
                                    }}
                                />
                            </>
                        ) : null}
                        {typeof onCreateChart === 'function' ? <button type="button" style={actionButtonStyle(false)} onClick={onCreateChart}>New</button> : null}
                        {typeof onDuplicateChart === 'function' ? <button type="button" style={actionButtonStyle(false)} onClick={onDuplicateChart}>Duplicate</button> : null}
                        {typeof onDeleteChart === 'function' ? (
                            <button
                                type="button"
                                style={actionButtonStyle(false)}
                                onClick={onDeleteChart}
                                disabled={!Array.isArray(chartDefinitions) || chartDefinitions.length <= 1}
                            >
                                Delete
                            </button>
                        ) : null}
                    </div>
                ) : null}
                {!cinemaMode && standalone && !floating && typeof onDockPositionChange === 'function' ? (
                    <div style={{ display: 'flex', gap: '6px', marginBottom: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
                        <button type="button" onClick={() => onDockPositionChange('left')} style={dockButtonStyle('left')} title="Dock chart pane to the left">Left</button>
                        <button type="button" onClick={() => onDockPositionChange('right')} style={dockButtonStyle('right')} title="Dock chart pane to the right">Right</button>
                        <button type="button" onClick={() => onDockPositionChange('top')} style={dockButtonStyle('top')} title="Dock chart pane to the top">Top</button>
                        <button type="button" onClick={() => onDockPositionChange('bottom')} style={dockButtonStyle('bottom')} title="Dock chart pane to the bottom">Bottom</button>
                    </div>
                ) : null}
                {!cinemaMode ? (
                    <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap' }}>
                        <button style={sourceButtonStyle('pivot')} onClick={() => onSourceChange('pivot')}>Pivot View</button>
                        <button style={sourceButtonStyle('selection')} onClick={() => onSourceChange('selection')}>Selection</button>
                    </div>
                ) : null}
                <ChartSurface
                    model={model}
                    chartType={chartType}
                    chartLayers={chartLayers}
                    onChartLayersChange={onChartLayersChange}
                    availableColumns={availableColumns}
                    barLayout={barLayout}
                    axisMode={axisMode}
                    onChartTypeChange={onChartTypeChange}
                    onBarLayoutChange={onBarLayoutChange}
                    onAxisModeChange={onAxisModeChange}
                    orientation={orientation}
                    onOrientationChange={onOrientationChange}
                    hierarchyLevel={hierarchyLevel}
                    onHierarchyLevelChange={onHierarchyLevelChange}
                    rowLimit={rowLimit}
                    onRowLimitChange={onRowLimitChange}
                    columnLimit={columnLimit}
                    onColumnLimitChange={onColumnLimitChange}
                    title={chartTitle}
                    onTitleChange={onChartTitleChange}
                    chartHeight={chartHeight}
                    onChartHeightChange={onChartHeightChange}
                    sortMode={sortMode}
                    onSortModeChange={onSortModeChange}
                    interactionMode={interactionMode}
                    onInteractionModeChange={onInteractionModeChange}
                    serverScope={serverScope}
                    onServerScopeChange={onServerScopeChange}
                    showServerScope={showServerScope}
                    theme={theme}
                    cinemaMode={cinemaMode}
                    onCategoryActivate={onCategoryActivate}
                    allowHierarchyCharts={allowHierarchyCharts}
                    floating={floating}
                    locked={locked}
                    onToggleLock={onToggleLock}
                    configOpen={settingsPaneOpen}
                    onConfigChange={setSettingsPaneOpen}
                    settingsPanelWidth={settingsPanelWidth}
                    onSettingsPanelWidthChange={setSettingsPanelWidth}
                />
                {floating && !fullscreenMode && !locked ? (
                    <>
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('right', event)}
                            style={{ position: 'absolute', top: 0, right: 0, width: '10px', height: '100%', cursor: 'ew-resize' }}
                        />
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('bottom', event)}
                            style={{ position: 'absolute', left: 0, bottom: 0, width: '100%', height: '10px', cursor: 'ns-resize' }}
                        />
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('corner', event)}
                            style={{ position: 'absolute', right: 0, bottom: 0, width: '18px', height: '18px', cursor: 'nwse-resize' }}
                        />
                    </>
                ) : null}
            </aside>
        </div>
    );
};

export const PivotChartModal = ({ chartState, onClose, theme, position = 'right', onPositionChange }) => {
    const [chartType, setChartType] = useState(chartState && chartState.chartType ? chartState.chartType : 'bar');
    const [barLayout, setBarLayout] = useState(chartState && chartState.barLayout ? chartState.barLayout : 'grouped');
    const [axisMode, setAxisMode] = useState(chartState && chartState.axisMode ? chartState.axisMode : 'vertical');
    const [orientation, setOrientation] = useState(chartState && chartState.defaultOrientation ? chartState.defaultOrientation : 'rows');
    const [hierarchyLevel, setHierarchyLevel] = useState(chartState && chartState.defaultHierarchyLevel !== undefined ? chartState.defaultHierarchyLevel : 'all');
    const [rowLimit, setRowLimit] = useState(chartState && chartState.rowLimit ? chartState.rowLimit : MAX_SELECTION_CATEGORIES);
    const [columnLimit, setColumnLimit] = useState(chartState && chartState.columnLimit ? chartState.columnLimit : MAX_SELECTION_SERIES);
    const [sortMode, setSortMode] = useState(chartState && chartState.sortMode ? chartState.sortMode : 'natural');
    const [chartLayers, setChartLayers] = useState(chartState && Array.isArray(chartState.chartLayers) ? chartState.chartLayers : []);
    const [settingsWidthBudget, setSettingsWidthBudget] = useState(null);

    useEffect(() => {
        setChartType(chartState && chartState.chartType ? chartState.chartType : 'bar');
        setBarLayout(chartState && chartState.barLayout ? chartState.barLayout : 'grouped');
        setAxisMode(chartState && chartState.axisMode ? chartState.axisMode : 'vertical');
        setOrientation(chartState && chartState.defaultOrientation ? chartState.defaultOrientation : 'rows');
        setHierarchyLevel(chartState && chartState.defaultHierarchyLevel !== undefined ? chartState.defaultHierarchyLevel : 'all');
        setRowLimit(chartState && chartState.rowLimit ? chartState.rowLimit : MAX_SELECTION_CATEGORIES);
        setColumnLimit(chartState && chartState.columnLimit ? chartState.columnLimit : MAX_SELECTION_SERIES);
        setSortMode(chartState && chartState.sortMode ? chartState.sortMode : 'natural');
        setChartLayers(chartState && Array.isArray(chartState.chartLayers) ? chartState.chartLayers : []);
    }, [chartState]);

    const availableColumns = useMemo(() => {
        if (!chartState || !Array.isArray(chartState.visibleCols)) return [];
        if (!(chartState.selectionMap && chartState.visibleRows)) {
            return getChartableColumns(chartState.visibleCols);
        }
        const bounds = buildSelectionBounds(chartState.selectionMap, chartState.visibleRows, chartState.visibleCols);
        if (!bounds) return [];
        const selectedColumnIds = new Set(
            (bounds.colIndexes || [])
                .map((index) => chartState.visibleCols[index])
                .filter((column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
                .map((column) => column.id)
        );
        return getChartableColumns(chartState.visibleCols).filter((column) => selectedColumnIds.has(column.id));
    }, [chartState]);

    const activeModel = useMemo(() => {
        if (!chartState) return null;
        if (chartState.selectionMap && chartState.visibleRows && chartState.visibleCols) {
            if (chartType === 'combo') {
                return buildComboSelectionChartModel(chartState.selectionMap, chartState.visibleRows, chartState.visibleCols, {
                    hierarchyLevel,
                    maxHierarchyLevel: chartState.maxHierarchyLevel || 1,
                    maxRows: rowLimit,
                    maxColumns: columnLimit,
                    rowFields: chartState.rowFields || [],
                    colFields: chartState.colFields || [],
                    sortMode,
                    layers: chartLayers,
                });
            }
            return buildSelectionChartModel(chartState.selectionMap, chartState.visibleRows, chartState.visibleCols, {
                orientation,
                hierarchyLevel,
                maxHierarchyLevel: chartState.maxHierarchyLevel || 1,
                maxRows: rowLimit,
                maxColumns: columnLimit,
                rowFields: chartState.rowFields || [],
                colFields: chartState.colFields || [],
                sortMode,
            });
        }
        return chartState.model;
    }, [chartLayers, chartState, chartType, hierarchyLevel, orientation, rowLimit, columnLimit, sortMode]);

    useEffect(() => {
        if (barLayout !== 'stacked') return;
        if (chartType === 'combo') {
            setBarLayout('grouped');
            return;
        }
        const canKeepStacked = chartType === 'area'
            ? (activeModel && Array.isArray(activeModel.series) && activeModel.series.length > 1)
            : canStackBarLayout(activeModel);
        if (canKeepStacked) return;
        setBarLayout('grouped');
    }, [activeModel, barLayout, chartType]);

    if (!chartState) return null;
    const isVerticalPosition = position === 'top' || position === 'bottom';
    const panelWidthBudget = Math.max(420, Number.isFinite(Number(settingsWidthBudget)) ? Math.floor(Number(settingsWidthBudget)) : 420);
    const panelHeightBudget = Math.max(360, Number(chartState.chartHeight || CHART_HEIGHT) + 220);

    const posBtn = (value, label) => ({
        border: `1px solid ${value === position ? theme.primary : theme.border}`,
        background: value === position ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === position ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '5px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <aside
            data-chart-modal-position={position}
            style={{
                display: 'flex',
                flexDirection: 'column',
                width: isVerticalPosition ? '100%' : `${panelWidthBudget}px`,
                minWidth: isVerticalPosition ? 0 : `${Math.max(320, panelWidthBudget)}px`,
                maxWidth: isVerticalPosition ? '100%' : '70%',
                height: isVerticalPosition ? `${panelHeightBudget}px` : undefined,
                minHeight: isVerticalPosition ? '320px' : 0,
                flexShrink: 0,
                overflow: 'hidden',
                borderLeft: position === 'right' ? `1px solid ${theme.border}` : 'none',
                borderRight: position === 'left' ? `1px solid ${theme.border}` : 'none',
                borderTop: position === 'bottom' ? `1px solid ${theme.border}` : 'none',
                borderBottom: position === 'top' ? `1px solid ${theme.border}` : 'none',
                background: theme.sidebarBg || theme.surfaceBg || theme.background,
            }}
        >
            <div style={{ padding: '12px 14px', overflow: 'hidden', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', gap: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' }}>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>
                        {chartState.title || 'Range Chart'}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        {typeof onPositionChange === 'function' ? (
                            <>
                                <button
                                    type="button"
                                    onClick={() => onPositionChange('left')}
                                    style={posBtn('left')}
                                    title="Dock chart pane to the left"
                                    aria-label="Dock left"
                                >
                                    Left
                                </button>
                                <button
                                    type="button"
                                    onClick={() => onPositionChange('right')}
                                    style={posBtn('right')}
                                    title="Dock chart pane to the right"
                                    aria-label="Dock right"
                                >
                                    Right
                                </button>
                                <button
                                    type="button"
                                    onClick={() => onPositionChange('top')}
                                    style={posBtn('top')}
                                    title="Dock chart pane to the top"
                                    aria-label="Dock top"
                                >
                                    Top
                                </button>
                                <button
                                    type="button"
                                    onClick={() => onPositionChange('bottom')}
                                    style={posBtn('bottom')}
                                    title="Dock chart pane to the bottom"
                                    aria-label="Dock bottom"
                                >
                                    Bottom
                                </button>
                            </>
                        ) : null}
                        <button
                            onClick={onClose}
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.headerSubtleBg || theme.hover,
                                color: theme.text,
                                borderRadius: theme.radiusSm || '8px',
                                padding: '5px 10px',
                                fontSize: '11px',
                                fontWeight: 700,
                                cursor: 'pointer',
                            }}
                        >
                            Close
                        </button>
                    </div>
                </div>
                <ChartSurface
                    model={activeModel}
                    chartType={chartType}
                    chartLayers={chartLayers}
                    onChartLayersChange={setChartLayers}
                    availableColumns={availableColumns}
                    barLayout={barLayout}
                    axisMode={axisMode}
                    onChartTypeChange={setChartType}
                    onBarLayoutChange={setBarLayout}
                    onAxisModeChange={setAxisMode}
                    orientation={orientation}
                    onOrientationChange={setOrientation}
                    hierarchyLevel={hierarchyLevel}
                    onHierarchyLevelChange={setHierarchyLevel}
                    title={chartState.title || 'Range Chart'}
                    rowLimit={rowLimit}
                    onRowLimitChange={setRowLimit}
                    columnLimit={columnLimit}
                    onColumnLimitChange={setColumnLimit}
                    sortMode={sortMode}
                    onSortModeChange={setSortMode}
                    theme={theme}
                    allowHierarchyCharts={!(chartState.selectionMap && chartState.visibleRows && chartState.visibleCols)}
                    onSettingsWidthBudgetChange={setSettingsWidthBudget}
                />
            </div>
        </aside>
    );
};

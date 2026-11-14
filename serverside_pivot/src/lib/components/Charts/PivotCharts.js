import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useChartExportMenu } from './useChartExportMenu';
import Icons from '../../utils/Icons';
import { usePivotRenderCounter } from '../../hooks/usePivotRenderCounter';
import { formatDisplayLabel } from '../../utils/helpers';
import {
    getDashArray, DASH_STYLES, GRADIENT_DIRECTIONS, buildGradientStops,
    LEGEND_POSITIONS, ChartLegendPositionButtons,
    SERIES_SORT_MODES, applySeriesSort,
    ChartDataLimitWarning, ChartLineDashEditor,
    ChartCustomPaletteEditor, ChartReflineRow,
    ChartTypeGallery,
} from './chartEnhancements';
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
    canStackBarSeries,
    canStackBarLayout,
} from '../../utils/chartModelBuilders';
import {
    CHART_PANEL_VERTICAL_CHROME_HEIGHT,
    DEFAULT_FLOATING_CHART_PANEL_HEIGHT,
    MIN_CHART_PANEL_WIDTH,
    MAX_CHART_PANEL_WIDTH,
    MIN_FLOATING_CHART_PANEL_HEIGHT,
} from '../../hooks/usePivotNormalization';
import {
    canStackAreaSeries,
    clampChartPanelBaseWidth,
    computeLinearTrendline,
    computeMovingAverage,
} from './chartMath';
import {
    downloadBlob,
    exportChartCsv,
    layoutLegendItems,
    normalizeChartPresentation,
    readChartPresentation,
} from './chartExportPresentation';
import { ChartStats, EmptyChartState, SparklineBoard, SvgChart } from './ChartRenderers';

/** @param {any} p0 @returns {any} */
const ChartLayoutButtons = ({ chartType, barLayout, onChange, canStack, theme }) => {
    /** @param {any} [value] @param {any} [disabled] @returns {any} */
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
            <button
                onClick={() => {
                    if (!canStack) return;
                    onChange('stacked100');
                }}
                style={buttonStyle('stacked100', !canStack)}
                title={canStack ? 'Normalize each category to 100%' : 'Needs multiple series'}
            >
                100%
            </button>
        </div>
    );
};

/** @param {any} p0 @returns {any} */
const ChartOrientationButtons = ({ orientation, onChange, theme }) => {
    /** @param {any} [value] @returns {any} */
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

/** @param {any} p0 @returns {any} */
const ChartAxisButtons = ({ axisMode, onChange, theme }) => {
    /** @param {any} [value] @returns {any} */
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

/** @param {any} p0 @returns {any} */
const ChartHierarchyButtons = ({ level, onChange, maxLevel, theme }) => {
    if (!maxLevel || maxLevel <= 1) return null;

    /** @param {any} [value] @returns {any} */
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

/** @param {any} p0 @returns {any} */
const ChartHeader = ({ title, subtitle, note, theme }) => (
    !title && !subtitle && !note ? null : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            {title ? <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{title}</div> : null}
            {subtitle ? <div style={{ fontSize: '11px', color: theme.textSec }}>{subtitle}</div> : null}
            {note ? <div style={{ fontSize: '11px', color: theme.textSec }}>{note}</div> : null}
        </div>
    )
);

/** @param {any} p0 @returns {any} */
const ChartConfigSection = ({ title, theme, children, defaultCollapsed = true, focusTrigger = 0 }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [collapsed, setCollapsed] = useState(defaultCollapsed);
    /** @type {any} */
    const sectionRef = useRef(null);
    /** @type {any} */
    const prevTrigger = useRef(0);
    useEffect(() => {
        if (focusTrigger > 0 && focusTrigger !== prevTrigger.current) {
            prevTrigger.current = focusTrigger;
            setCollapsed(false);
            setTimeout(() => sectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 60);
        }
    }, [focusTrigger]);
    return (
        <div ref={sectionRef} style={{
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
                    onClick={() => setCollapsed(/** @param {any} [v] @returns {any} */ (v) => !v)}
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

/** @param {any} p0 @returns {any} */
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

/** @param {any} p0 @returns {any} */
const ChartLimitInputs = ({
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    theme,
}) => {
    /** @type {Record<string, any>} */
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
    /** @type {Record<string, any>} */
    const labelStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        color: theme.textSec,
        fontSize: '11px',
        fontWeight: 700,
    };
    /** @param {any} [callback] @param {any} [fallback] @returns {any} */
    const handleLimitChange = (callback, fallback) => /** @param {any} [event] @returns {any} */ (event) => {
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

/** @param {any} p0 @returns {any} */
const ChartLayerEditor = ({
    layers,
    onChange,
    availableColumns,
    theme,
}) => {
    /** @type {any} */
    const chartableColumns = getChartableColumns(availableColumns);
    /** @type {any} */
    const normalizedLayers = normalizeComboLayers(layers, chartableColumns);
    /** @type {[any, (...args: any[]) => any]} */
    const [searchValue, setSearchValue] = useState('');
    /** @type {any} */
    const columnOptions = useMemo(
        () => buildGroupedChartColumnOptions(chartableColumns),
        [chartableColumns]
    );
    /** @type {any} */
    const optionById = useMemo(
        () => new Map(columnOptions.map(/** @param {any} [option] @returns {any} */ (option) => [option.id, option])),
        [columnOptions]
    );
    /** @type {any} */
    const normalizedSearchValue = searchValue.trim().toLowerCase();
    /** @type {any} */
    const filteredOptions = useMemo(
        () => (
            normalizedSearchValue
                ? columnOptions.filter(/** @param {any} [option] @returns {any} */ (option) => option.searchText.includes(normalizedSearchValue))
                : columnOptions
        ),
        [columnOptions, normalizedSearchValue]
    );
    /** @type {any} */
    const groupedFilteredOptions = useMemo(() => {
        /** @type {any} */
        const groups = new Map();
        filteredOptions.forEach(/** @param {any} [option] @returns {any} */ (option) => {
            if (!groups.has(option.groupLabel)) groups.set(option.groupLabel, []);
            groups.get(option.groupLabel).push(option);
        });
        return Array.from(groups.entries()).map(([label, options]) => ({ label, options }));
    }, [filteredOptions]);
    /** @type {Record<string, any>} */
    const inputStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };
    /** @param {any} [layerId] @param {any} [patch] @returns {any} */
    const updateLayer = (layerId, patch) => {
        if (typeof onChange !== 'function') return;
        onChange(normalizedLayers.map(/** @param {any} [layer] @returns {any} */ (layer) => (
            layer.id === layerId ? { ...layer, ...(patch || {}) } : layer
        )));
    };
    /** @param {any} [layerId] @param {any} [direction] @returns {any} */
    const moveLayer = (layerId, direction) => {
        if (typeof onChange !== 'function') return;
        /** @type {any} */
        const layerIndex = normalizedLayers.findIndex(/** @param {any} [layer] @returns {any} */ (layer) => layer.id === layerId);
        /** @type {any} */
        const nextIndex = layerIndex + direction;
        if (layerIndex < 0 || nextIndex < 0 || nextIndex >= normalizedLayers.length) return;
        /** @type {any[]} */
        const nextLayers = [...normalizedLayers];
        /** @type {any} */
        const [movedLayer] = nextLayers.splice(layerIndex, 1);
        nextLayers.splice(nextIndex, 0, movedLayer);
        onChange(nextLayers);
    };
    /** @param {any} [layerId] @returns {any} */
    const removeLayer = (layerId) => {
        if (typeof onChange !== 'function') return;
        /** @type {any} */
        const nextLayers = normalizedLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.id !== layerId);
        onChange(nextLayers.length > 0 ? nextLayers : buildDefaultCartesianLayers(chartableColumns, 1));
    };
    /** @returns {any} */
    const addLayer = () => {
        if (typeof onChange !== 'function') return;
        /** @type {any} */
        const usedColumnIds = new Set(normalizedLayers.map(/** @param {any} [layer] @returns {any} */ (layer) => layer.columnId));
        /** @type {any} */
        const nextColumn = chartableColumns.find(/** @param {any} [column] @returns {any} */ (column) => !usedColumnIds.has(column.id)) || chartableColumns[0];
        if (!nextColumn) return;
        /** @type {any} */
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
            {normalizedLayers.map(/** @param {any} [layer] @param {any} [layerIndex] @returns {any} */ (layer, layerIndex) => {
                /** @type {any} */
                const selectedOption = optionById.get(layer.columnId) || null;
                /** @type {any} */
                const selectedVisible = groupedFilteredOptions.some(/** @param {any} [group] @returns {any} */ (group) => (
                    group.options.some(/** @param {any} [option] @returns {any} */ (option) => option.id === layer.columnId)
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
                                    /** @type {any} */
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
                                {groupedFilteredOptions.map(/** @param {any} [group] @returns {any} */ (group) => (
                                    <optgroup key={group.label} label={`${group.label} (${group.options.length})`}>
                                        {group.options.map(/** @param {any} [option] @returns {any} */ (option) => (
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
                        <label style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                            <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: theme.textSec }}>
                                Format
                            </span>
                            <select
                                value={layer.valueFormat || 'auto'}
                                onChange={(event) => updateLayer(layer.id, { valueFormat: event.target.value })}
                                style={inputStyle}
                            >
                                <option value="auto">Auto</option>
                                <option value="number">Number</option>
                                <option value="compact">Compact (K/M)</option>
                                <option value="percent">Percent</option>
                                <option value="currency">Currency ($)</option>
                            </select>
                        </label>
                        <label style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                            <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.06em', textTransform: 'uppercase', color: theme.textSec }}>
                                Color
                            </span>
                            <input
                                type="color"
                                value={layer.color || '#6366f1'}
                                onChange={(event) => updateLayer(layer.id, { color: event.target.value })}
                                style={{ width: '100%', height: '34px', padding: '2px 4px', border: `1px solid ${theme.border}`, borderRadius: theme.radiusSm || '8px', cursor: 'pointer', background: 'none' }}
                                title={`Override color for this layer`}
                            />
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

/** @param {any} p0 @returns {any} */
const ChartSortButtons = ({ sortMode, onChange, theme }) => {
    /** @type {any[]} */
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

/** @param {any} p0 @returns {any} */
const ChartInteractionButtons = ({ interactionMode, onChange, theme }) => {
    /** @param {any} [value] @returns {any} */
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

/** @param {any} p0 @returns {any} */
const ChartScopeButtons = ({ scope, onChange, theme }) => {
    /** @param {any} [value] @returns {any} */
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

const SVG_NAMESPACE = 'http://www.w3.org/2000/svg';

/** @param {any} [svgNode] @param {any} [rawPresentation] @returns {any} */
const createPresentedSvgNode = (svgNode, rawPresentation) => {
    if (!svgNode || typeof document === 'undefined') return null;
    /** @type {any} */
    const presentation = normalizeChartPresentation(rawPresentation);
    /** @type {any} */
    const clone = svgNode.cloneNode(true);
    /** @type {any} */
    const rawViewBox = (clone.getAttribute('viewBox') || '').split(/[\s,]+/).map(Number);
    /** @type {any} */
    const sourceWidth = rawViewBox.length === 4 && rawViewBox[2] > 0
        ? rawViewBox[2]
        : (svgNode.clientWidth || 800);
    /** @type {any} */
    const sourceHeight = rawViewBox.length === 4 && rawViewBox[3] > 0
        ? rawViewBox[3]
        : (svgNode.clientHeight || 400);
    /** @type {any} */
    const titleHeight = presentation.title ? 42 : 0;
    /** @type {any} */
    const legendRows = layoutLegendItems(
        presentation.legendItems,
        sourceWidth,
        /** @param {any} [label] @returns {any} */ label => String(label || '').length * 6.4,
    );
    /** @type {any} */
    const legendHeight = legendRows.length > 0 ? legendRows.length * 22 + 14 : 0;
    /** @type {any} */
    const topLegendHeight = presentation.legendPosition === 'top' ? legendHeight : 0;
    /** @type {any} */
    const chartOffsetY = titleHeight + topLegendHeight;
    /** @type {any} */
    const totalHeight = titleHeight + sourceHeight + legendHeight;

    /** @type {any} */
    const existingChildren = Array.from(clone.childNodes);
    /** @type {any} */
    const chartGroup = document.createElementNS(SVG_NAMESPACE, 'g');
    chartGroup.setAttribute('transform', `translate(0 ${chartOffsetY})`);
    existingChildren.forEach(/** @param {any} [child] @returns {any} */ child => chartGroup.appendChild(child));
    clone.appendChild(chartGroup);
    clone.setAttribute('viewBox', `0 0 ${sourceWidth} ${totalHeight}`);
    clone.setAttribute('width', String(sourceWidth));
    clone.setAttribute('height', String(totalHeight));
    clone.setAttribute('xmlns', SVG_NAMESPACE);

    /** @type {any} */
    const background = document.createElementNS(SVG_NAMESPACE, 'rect');
    background.setAttribute('x', '0');
    background.setAttribute('y', '0');
    background.setAttribute('width', String(sourceWidth));
    background.setAttribute('height', String(totalHeight));
    background.setAttribute('fill', presentation.background);
    clone.insertBefore(background, chartGroup);

    if (presentation.title) {
        /** @type {any} */
        const titleNode = document.createElementNS(SVG_NAMESPACE, 'text');
        titleNode.setAttribute('x', '16');
        titleNode.setAttribute('y', '27');
        titleNode.setAttribute('fill', presentation.textColor);
        titleNode.setAttribute('font-size', '16');
        titleNode.setAttribute('font-weight', '700');
        titleNode.setAttribute('font-family', 'Inter, Arial, sans-serif');
        titleNode.textContent = presentation.title;
        clone.appendChild(titleNode);
    }

    if (legendRows.length > 0) {
        /** @type {any} */
        const legendStartY = presentation.legendPosition === 'top'
            ? titleHeight + 16
            : titleHeight + sourceHeight + 18;
        legendRows.forEach(/** @param {any} [row] @param {any} [rowIndex] @returns {any} */ (row, rowIndex) => {
            let x = 16;
            row.forEach(/** @param {any} [item] @returns {any} */ (item) => {
                /** @type {any} */
                const y = legendStartY + rowIndex * 22;
                /** @type {any} */
                const marker = document.createElementNS(SVG_NAMESPACE, 'circle');
                marker.setAttribute('cx', String(x + 5));
                marker.setAttribute('cy', String(y - 4));
                marker.setAttribute('r', '5');
                marker.setAttribute('fill', item.hidden ? presentation.secondaryTextColor : (item.color || '#2563eb'));
                marker.setAttribute('opacity', item.hidden ? '0.35' : '1');
                clone.appendChild(marker);
                /** @type {any} */
                const label = document.createElementNS(SVG_NAMESPACE, 'text');
                label.setAttribute('x', String(x + 16));
                label.setAttribute('y', String(y));
                label.setAttribute('fill', presentation.secondaryTextColor);
                label.setAttribute('font-size', '11');
                label.setAttribute('font-family', 'Inter, Arial, sans-serif');
                label.setAttribute('opacity', item.hidden ? '0.45' : '1');
                label.textContent = item.label;
                clone.appendChild(label);
                x += item.width;
            });
        });
    }
    return clone;
};

/** @param {any} [svgNode] @param {any} [fileStem] @param {any} [presentation] @returns {Promise<any>} */
const exportSvgNode = async (svgNode, fileStem, presentation) => {
    if (!svgNode || typeof window === 'undefined') return;
    /** @type {any} */
    const presentedNode = createPresentedSvgNode(svgNode, presentation);
    if (!presentedNode) return;
    /** @type {any} */
    const serializer = new XMLSerializer();
    /** @type {any} */
    const svgText = serializer.serializeToString(presentedNode);
    /** @type {any} */
    const svgBlob = new Blob([svgText], { type: 'image/svg+xml;charset=utf-8' });
    downloadBlob(svgBlob, `${fileStem}.svg`);
};

/** @param {any} [svgNode] @param {any} [scale] @returns {any} */
const svgToCanvas = (svgNode, scale = 2) => new Promise((resolve) => {
    if (!svgNode) { resolve(null); return; }
    /** @type {any} */
    const serializer = new XMLSerializer();
    /** @type {any} */
    const svgText = serializer.serializeToString(svgNode);
    /** @type {any} */
    const viewBox = svgNode.getAttribute('viewBox');
    /** @type {any} */
    let w = svgNode.clientWidth || 800;
    /** @type {any} */
    let h = svgNode.clientHeight || 400;
    if (viewBox) {
        /** @type {any} */
        const parts = viewBox.split(/[\s,]+/).map(Number);
        if (parts.length === 4 && parts[2] > 0 && parts[3] > 0) { w = parts[2]; h = parts[3]; }
    }
    /** @type {any} */
    const canvas = document.createElement('canvas');
    canvas.width = w * scale;
    canvas.height = h * scale;
    /** @type {any} */
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    /** @type {any} */
    const img = new Image();
    /** @type {any} */
    const blob = new Blob([svgText], { type: 'image/svg+xml;charset=utf-8' });
    /** @type {any} */
    const url = URL.createObjectURL(blob);
    img.onload = () => { ctx.drawImage(img, 0, 0, canvas.width, canvas.height); URL.revokeObjectURL(url); resolve(canvas); };
    img.onerror = () => { URL.revokeObjectURL(url); resolve(null); };
    img.src = url;
});

/** @param {any} [svgNode] @param {any} [fileStem] @param {any} [presentation] @returns {Promise<any>} */
const exportSvgAsPng = async (svgNode, fileStem, presentation) => {
    const canvas = await svgToCanvas(createPresentedSvgNode(svgNode, presentation));
    if (!canvas) return;
    canvas.toBlob(/** @param {any} [blob] @returns {any} */ (blob) => { if (blob) downloadBlob(blob, `${fileStem}.png`); }, 'image/png');
};

/** @param {any} [svgNode] @param {any} [presentation] @returns {Promise<any>} */
const copySvgToClipboard = async (svgNode, presentation) => {
    if (typeof navigator === 'undefined' || !navigator.clipboard || !navigator.clipboard.write) return false;
    const canvas = await svgToCanvas(createPresentedSvgNode(svgNode, presentation));
    if (!canvas) return false;
    try {
        const blob = await new Promise((resolve) => canvas.toBlob(resolve, 'image/png'));
        if (!blob) return false;
        await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })]);
        return true;
    } catch (_) { return false; }
};

/** @type {any} */
const ECHARTS_CHART_TYPES = new Set(['bar3d', 'line3d', 'scatter3d', 'range', 'boxplot', 'nightingale']);

/** @param {any} [url] @returns {any} */
const imageFromUrl = (url) => new Promise((resolve) => {
    /** @type {any} */
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => resolve(null);
    image.src = url;
});

/** @param {any} [echartsRef] @param {any} [rawPresentation] @returns {Promise<any>} */
const echartsToPresentedCanvas = async (echartsRef, rawPresentation) => {
    /** @type {any} */
    const instance = echartsRef && echartsRef.current && echartsRef.current.getEchartsInstance();
    if (!instance) return null;
    /** @type {any} */
    const presentation = normalizeChartPresentation(rawPresentation);
    /** @type {any} */
    const url = instance.getDataURL({ type: 'png', pixelRatio: 2 });
    const image = await imageFromUrl(url);
    if (!image) return null;
    /** @type {any} */
    const scale = Math.max(1, image.naturalWidth / Math.max(1, instance.getWidth()));
    /** @type {any} */
    const titleHeight = presentation.title ? Math.round(42 * scale) : 0;
    /** @type {any} */
    const measureCanvas = document.createElement('canvas');
    /** @type {any} */
    const measureContext = measureCanvas.getContext('2d');
    measureContext.font = '11px Inter, Arial, sans-serif';
    /** @type {any} */
    const legendRows = layoutLegendItems(
        presentation.legendItems,
        instance.getWidth(),
        /** @param {any} [label] @returns {any} */ label => measureContext.measureText(String(label || '')).width,
    );
    /** @type {any} */
    const legendHeight = legendRows.length > 0 ? Math.round((legendRows.length * 22 + 14) * scale) : 0;
    /** @type {any} */
    const topLegendHeight = presentation.legendPosition === 'top' ? legendHeight : 0;
    /** @type {any} */
    const chartOffsetY = titleHeight + topLegendHeight;
    /** @type {any} */
    const canvas = document.createElement('canvas');
    canvas.width = image.naturalWidth;
    canvas.height = titleHeight + image.naturalHeight + legendHeight;
    /** @type {any} */
    const context = canvas.getContext('2d');
    context.fillStyle = presentation.background;
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.drawImage(image, 0, chartOffsetY);

    if (presentation.title) {
        context.fillStyle = presentation.textColor;
        context.font = `700 ${Math.round(16 * scale)}px Inter, Arial, sans-serif`;
        context.textBaseline = 'alphabetic';
        context.fillText(presentation.title, Math.round(16 * scale), Math.round(27 * scale));
    }
    if (legendRows.length > 0) {
        /** @type {any} */
        const legendStartY = presentation.legendPosition === 'top'
            ? titleHeight + Math.round(16 * scale)
            : titleHeight + image.naturalHeight + Math.round(18 * scale);
        context.font = `${Math.round(11 * scale)}px Inter, Arial, sans-serif`;
        legendRows.forEach(/** @param {any} [row] @param {any} [rowIndex] @returns {any} */ (row, rowIndex) => {
            /** @type {any} */
            let x = Math.round(16 * scale);
            row.forEach(/** @param {any} [item] @returns {any} */ (item) => {
                /** @type {any} */
                const y = legendStartY + Math.round(rowIndex * 22 * scale);
                context.globalAlpha = item.hidden ? 0.35 : 1;
                context.fillStyle = item.hidden ? presentation.secondaryTextColor : (item.color || '#2563eb');
                context.beginPath();
                context.arc(x + Math.round(5 * scale), y - Math.round(4 * scale), Math.round(5 * scale), 0, Math.PI * 2);
                context.fill();
                context.globalAlpha = item.hidden ? 0.45 : 1;
                context.fillStyle = presentation.secondaryTextColor;
                context.fillText(item.label, x + Math.round(16 * scale), y);
                context.globalAlpha = 1;
                x += Math.round(item.width * scale);
            });
        });
    }
    return canvas;
};

/** @param {any} [echartsRef] @param {any} [fileStem] @param {any} [presentation] @returns {Promise<any>} */
const exportEchartsPng = async (echartsRef, fileStem, presentation) => {
    const canvas = await echartsToPresentedCanvas(echartsRef, presentation);
    if (!canvas) return;
    canvas.toBlob(/** @param {any} [blob] @returns {any} */ (blob) => { if (blob) downloadBlob(blob, `${fileStem}.png`); }, 'image/png');
};

/** @param {any} [echartsRef] @param {any} [presentation] @returns {Promise<any>} */
const copyEchartsToClipboard = async (echartsRef, presentation) => {
    /** @type {any} */
    const instance = echartsRef && echartsRef.current && echartsRef.current.getEchartsInstance();
    if (!instance || typeof navigator === 'undefined' || !navigator.clipboard || !navigator.clipboard.write) return false;
    try {
        const canvas = await echartsToPresentedCanvas(echartsRef, presentation);
        if (!canvas) return false;
        const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/png'));
        if (!blob) return false;
        await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })]);
        return true;
    } catch (_) {
        return false;
    }
};

/** @param {any} p0 @returns {any} */
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
    immersiveMode = false,
    onCategoryActivate,
    allowHierarchyCharts = true,
    locked = false,
    onToggleLock,
    configOpen: controlledConfigOpen,
    onConfigChange,
    settingsPanelWidth: controlledSettingsPanelWidth,
    onSettingsPanelWidthChange,
    initialSettings = {},
    onSettingsChange,
}) => {
    /** @type {any} */
    const isComboChart = chartType === 'combo';
    /** @type {any} */
    const isSparklineChart = chartType === 'sparkline';
    /** @type {any} */
    const canStack = isComboChart
        ? false
        : chartType === 'area'
        ? canStackAreaSeries(model)
        : canStackBarLayout(model);
    /** @type {any} */
    const resolvedTitle = title !== undefined ? title : (model && model.title);
    /** @type {any} */
    const resolvedSubtitle = subtitle !== undefined ? subtitle : (model && model.subtitle);
    /** @type {any} */
    const resolvedNote = note !== undefined
        ? note
        : locked
            ? (floating ? 'Locked to the current chart request and floating frame' : 'Locked to the current chart request')
            : (model && model.note);
    /** @type {[any, (...args: any[]) => any]} */
    const [uncontrolledConfigOpen, setUncontrolledConfigOpen] = useState(false);
    /** @type {any} */
    const configOpen = typeof controlledConfigOpen === 'boolean' ? controlledConfigOpen : uncontrolledConfigOpen;
    /** @type {{ (updater: (currentOpen: any) => any): any; (val?: any): any }} */
    const setConfigOpen = typeof onConfigChange === 'function' ? onConfigChange : setUncontrolledConfigOpen;
    /** @type {[any, (...args: any[]) => any]} */
    const [showDataLabels, setShowDataLabels] = useState(() => initialSettings?.showDataLabels ?? false);
    /** @type {[any, (...args: any[]) => any]} */
    const [colorPalette, setColorPalette] = useState(() => initialSettings?.colorPalette ?? 'default');
    /** @type {[any, (...args: any[]) => any]} */
    const [valueFormat, setValueFormat] = useState(() => initialSettings?.valueFormat ?? 'auto');
    /** @type {[any, (...args: any[]) => any]} */
    const [yAxisTitle, setYAxisTitle] = useState(() => initialSettings?.yAxisTitle ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [yAxisMin, setYAxisMin] = useState(() => initialSettings?.yAxisMin ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [yAxisMax, setYAxisMax] = useState(() => initialSettings?.yAxisMax ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [referenceLines, setReferenceLines] = useState(() => initialSettings?.referenceLines ?? []);
    /** @type {[any, (...args: any[]) => any]} */
    const [refLineDraft, setRefLineDraft] = useState({ value: '', label: '', orient: 'h' });
    /** @type {[any, (...args: any[]) => any]} */
    const [labelAngle, setLabelAngle] = useState(() => initialSettings?.labelAngle ?? 0);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartSubtitle, setChartSubtitle] = useState(() => initialSettings?.chartSubtitle ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [tickCount, setTickCount] = useState(() => initialSettings?.tickCount ?? 5);
    /** @type {[any, (...args: any[]) => any]} */
    const [markerShape, setMarkerShape] = useState(() => initialSettings?.markerShape ?? 'circle');
    /** @type {[any, (...args: any[]) => any]} */
    const [markerSize, setMarkerSize] = useState(() => initialSettings?.markerSize ?? 4);
    /** @type {[any, (...args: any[]) => any]} */
    const [showGradient, setShowGradient] = useState(() => initialSettings?.showGradient ?? false);
    /** @type {[any, (...args: any[]) => any]} */
    const [seriesColors, setSeriesColors] = useState(() => initialSettings?.seriesColors ?? {});
    /** @type {[any, (...args: any[]) => any]} */
    const [showAnimations, setShowAnimations] = useState(() => initialSettings?.showAnimations ?? true);
    /** @type {[any, (...args: any[]) => any]} */
    const [showCalloutLabels, setShowCalloutLabels] = useState(() => initialSettings?.showCalloutLabels ?? false);
    /** @type {[any, (...args: any[]) => any]} */
    const [showTrendline, setShowTrendline] = useState(() => initialSettings?.showTrendline ?? false);
    /** @type {[any, (...args: any[]) => any]} */
    const [trendlineMode, setTrendlineMode] = useState(() => initialSettings?.trendlineMode ?? 'linear');
    /** @type {[any, (...args: any[]) => any]} */
    const [trendlinePeriod, setTrendlinePeriod] = useState(() => initialSettings?.trendlinePeriod ?? 3);
    // new settings
    /** @type {[any, (...args: any[]) => any]} */
    const [legendPosition, setLegendPosition] = useState(() => initialSettings?.legendPosition ?? 'bottom');
    /** @type {[any, (...args: any[]) => any]} */
    const [labelPosition, setLabelPosition] = useState(() => initialSettings?.labelPosition ?? 'outside');
    /** @type {[any, (...args: any[]) => any]} */
    const [decimalPlaces, setDecimalPlaces] = useState(() => initialSettings?.decimalPlaces ?? -1);
    /** @type {[any, (...args: any[]) => any]} */
    const [gradientDirection, setGradientDirection] = useState(() => initialSettings?.gradientDirection ?? 'vertical');
    /** @type {[any, (...args: any[]) => any]} */
    const [gradientOpacity, setGradientOpacity] = useState(() => initialSettings?.gradientOpacity ?? 70);
    /** @type {[any, (...args: any[]) => any]} */
    const [trendlineColor, setTrendlineColor] = useState(() => initialSettings?.trendlineColor ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [trendlineWidth, setTrendlineWidth] = useState(() => initialSettings?.trendlineWidth ?? 2);
    /** @type {[any, (...args: any[]) => any]} */
    const [seriesSort, setSeriesSort] = useState(() => initialSettings?.seriesSort ?? 'none');
    /** @type {[any, (...args: any[]) => any]} */
    const [customPaletteColors, setCustomPaletteColors] = useState(() => initialSettings?.customPaletteColors ?? []);
    /** @type {[any, (...args: any[]) => any]} */
    const [lineDashStyles, setLineDashStyles] = useState(() => initialSettings?.lineDashStyles ?? {});
    /** @type {[any, (...args: any[]) => any]} */
    const [yAxisMinRight, setYAxisMinRight] = useState(() => initialSettings?.yAxisMinRight ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [yAxisMaxRight, setYAxisMaxRight] = useState(() => initialSettings?.yAxisMaxRight ?? '');
    /** @type {[any, (...args: any[]) => any]} */
    const [settingsTab, setSettingsTab] = useState('chart');
    /** @type {[any, (...args: any[]) => any]} */
    const [chartTypeOpen, setChartTypeOpen] = useState(true);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartParamsOpen, setChartParamsOpen] = useState(true);
    /** @type {(...args: any[]) => any} */
    const handleElementSelect = useCallback((type) => {
        /** @type {Record<string, any>} */
        const tabMap = { series: 'style', axis: 'axes', legend: 'labels', title: 'data' };
        const tab = tabMap[type];
        if (!tab) return;
        setConfigOpen(true);
        setSettingsTab(tab);
    }, [setConfigOpen]);
    /** @type {any} */
    const paletteColors = resolvePalette(colorPalette);
    /** @type {[any, (...args: any[]) => any]} */
    const [heightDraft, setHeightDraft] = useState(String(chartHeightProp || CHART_HEIGHT));
    /** @type {any} */
    const chartHeightResizeRef = useRef(null);
    /** @type {any} */
    const settingsPaneResizeRef = useRef(null);
    /** @type {any} */
    const immersiveContainerRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartCopyStatus, setChartCopyStatus] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [exportMenuOpen, setExportMenuOpen] = useState(false);
    /** @type {any} */
    const { anchorRef: exportButtonRef, menuRef: exportMenuRef, position: exportMenuPosition, portalTarget: exportMenuTarget } = useChartExportMenu(exportMenuOpen, setExportMenuOpen);
    /** @type {[any, (...args: any[]) => any]} */
    const [immersiveAutoHeight, setImmersiveAutoHeight] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [uncontrolledSettingsPanelWidth, setUncontrolledSettingsPanelWidth] = useState(348);
    /** @type {any} */
    const settingsPanelWidth = Number.isFinite(Number(controlledSettingsPanelWidth))
        ? Math.max(300, Math.min(680, Math.floor(Number(controlledSettingsPanelWidth))))
        : uncontrolledSettingsPanelWidth;
    /** @type {any} */
    const setSettingsPanelWidth = typeof onSettingsPanelWidthChange === 'function'
        ? onSettingsPanelWidthChange
        : setUncontrolledSettingsPanelWidth;
    const showChrome = !immersiveMode;
    /** @type {any} */
    const immersiveLegendReserve = immersiveMode && !isSparklineChart && legendPosition !== 'none' ? 44 : 0;
    /** @type {any} */
    const chartHeight = immersiveMode && immersiveAutoHeight
        ? Math.max(180, immersiveAutoHeight - immersiveLegendReserve - 44)
        : Math.max(180, Number.isFinite(Number(chartHeightProp)) ? Number(chartHeightProp) : CHART_HEIGHT);
    /** @type {any} */
    const svgRef = useRef(null);
    /** @type {any} */
    const echartsRef = useRef(null);
    /** @type {any} */
    const chartSurfaceMainRef = useRef(null);
    /** @type {any} */
    const maxHierarchyLevel = (model && model.maxHierarchyLevel) || 1;
    /** @type {any} */
    const showHierarchySection = typeof onHierarchyLevelChange === 'function' && maxHierarchyLevel > 1;
    /** @type {Record<string, any>} */
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
    /** @type {Record<string, any>} */
    const lockButtonStyle = {
        ...configButtonStyle,
        border: `1px solid ${locked ? theme.primary : theme.border}`,
        background: locked ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: locked ? theme.primary : theme.textSec,
    };
    /** @type {Record<string, any>} */
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
    /** @type {Record<string, any>} */
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
    /** @type {(...args: any[]) => any} */
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
        if (!immersiveMode || !configOpen) return;
        setConfigOpen(false);
    }, [immersiveMode, configOpen]);

    useEffect(() => {
        if (!chartCopyStatus) return undefined;
        /** @type {any} */
        const timer = setTimeout(() => setChartCopyStatus(null), 2500);
        return () => clearTimeout(timer);
    }, [chartCopyStatus]);

    /** @type {(...args: any[]) => any} */
    const getChartPresentation = useCallback(() => readChartPresentation(
        chartSurfaceMainRef.current,
        resolvedTitle,
        legendPosition,
        theme,
    ), [legendPosition, resolvedTitle, theme]);

    /** @type {(...args: any[]) => any} */
    const handleChartExport = useCallback(async (format) => {
        setExportMenuOpen(false);
        /** @type {any} */
        const presentation = getChartPresentation();
        if (format === 'svg') {
            if (!ECHARTS_CHART_TYPES.has(chartType)) await exportSvgNode(svgRef.current, 'pivot-chart', presentation);
            return;
        }
        if (format === 'png') {
            if (ECHARTS_CHART_TYPES.has(chartType)) await exportEchartsPng(echartsRef, 'pivot-chart', presentation);
            else await exportSvgAsPng(svgRef.current, 'pivot-chart', presentation);
            return;
        }
        if (format === 'csv') exportChartCsv(model, 'pivot-chart', presentation);
    }, [chartType, getChartPresentation, model]);

    /** @type {(...args: any[]) => any} */
    const handleCopyChart = useCallback(async () => {
        /** @type {any} */
        const presentation = getChartPresentation();
        /** @type {any} */
        const copied = ECHARTS_CHART_TYPES.has(chartType)
            ? await copyEchartsToClipboard(echartsRef, presentation)
            : await copySvgToClipboard(svgRef.current, presentation);
        setChartCopyStatus(copied
            ? { tone: 'success', text: 'Copied chart' }
            : { tone: 'error', text: 'Copy failed' });
    }, [chartType, getChartPresentation]);

    useEffect(() => {
        if (!immersiveMode) { setImmersiveAutoHeight(null); return; }
        const el = immersiveContainerRef.current;
        if (!el) return;
        /** @returns {any} */
        const update = () => { const h = el.clientHeight; if (h > 0) setImmersiveAutoHeight(h); };
        update();
        if (typeof ResizeObserver === 'undefined') return;
        /** @type {any} */
        const ro = new ResizeObserver(update);
        ro.observe(el);
        return () => ro.disconnect();
    }, [immersiveMode]);

    /** @type {any} */
    const onChartHeightChangeRef = useRef(onChartHeightChange);
    useEffect(() => { onChartHeightChangeRef.current = onChartHeightChange; });

    useEffect(() => {
        /** @param {any} [event] @returns {any} */
        const handlePointerMove = (event) => {
            if (!chartHeightResizeRef.current || typeof onChartHeightChangeRef.current !== 'function') return;
            const resizeState = chartHeightResizeRef.current;
            /** @type {any} */
            const nextHeight = Math.max(180, resizeState.startHeight + (event.clientY - resizeState.startY));
            onChartHeightChangeRef.current(nextHeight);
        };

        /** @returns {any} */
        const stopResize = () => {
            chartHeightResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, []);

    useEffect(() => {
        /** @param {any} [event] @returns {any} */
        const handlePointerMove = (event) => {
            if (!settingsPaneResizeRef.current) return;
            const resizeState = settingsPaneResizeRef.current;
            /** @type {any} */
            const delta = resizeState.direction === 'left'
                ? (resizeState.startX - event.clientX)
                : (event.clientX - resizeState.startX);
            /** @type {any} */
            const nextWidth = Math.max(300, Math.min(680, resizeState.startWidth + delta));
            setSettingsPanelWidth(nextWidth);
        };

        /** @returns {any} */
        const stopResize = () => {
            settingsPaneResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, []);

    useEffect(() => {
        const el = chartSurfaceMainRef.current;
        if (!el || typeof ResizeObserver === 'undefined') return undefined;
        let lastWidth = el.clientWidth;
        /** @type {any} */
        const observer = new ResizeObserver(() => {
            const nextWidth = el.clientWidth;
            if (nextWidth !== lastWidth) {
                lastWidth = nextWidth;
                /** @type {any} */
                const instance = echartsRef.current?.getEchartsInstance?.();
                if (instance) instance.resize();
            }
        });
        observer.observe(el);
        return () => observer.disconnect();
    }, []);

    useEffect(() => { setHeightDraft(String(chartHeight)); }, [chartHeight]);

    /** @type {any} */
    const onSettingsChangeRef = useRef(onSettingsChange);
    useEffect(() => { onSettingsChangeRef.current = onSettingsChange; });
    /** @type {any} */
    const isFirstSettingsSyncRef = useRef(true);

    useEffect(() => {
        if (isFirstSettingsSyncRef.current) { isFirstSettingsSyncRef.current = false; return; }
        if (typeof onSettingsChangeRef.current !== 'function') return;
        onSettingsChangeRef.current({
            showDataLabels, colorPalette, valueFormat, yAxisTitle,
            yAxisMin, yAxisMax, referenceLines, labelAngle, chartSubtitle,
            tickCount, markerShape, markerSize, showGradient, seriesColors,
            showAnimations, showCalloutLabels, showTrendline, trendlineMode, trendlinePeriod,
            legendPosition, labelPosition, decimalPlaces,
            gradientDirection, gradientOpacity,
            trendlineColor, trendlineWidth,
            seriesSort, customPaletteColors, lineDashStyles,
            yAxisMinRight, yAxisMaxRight,
        });
    }, [
        showDataLabels, colorPalette, valueFormat, yAxisTitle,
        yAxisMin, yAxisMax, referenceLines, labelAngle, chartSubtitle,
        tickCount, markerShape, markerSize, showGradient, seriesColors,
        showAnimations, showCalloutLabels, showTrendline, trendlineMode, trendlinePeriod,
        legendPosition, labelPosition, decimalPlaces,
        gradientDirection, gradientOpacity, trendlineColor, trendlineWidth,
        seriesSort, customPaletteColors, lineDashStyles, yAxisMinRight, yAxisMaxRight,
    ]);

    /** @type {any} */
    const yRange = useMemo(() => {
        if (!model || !Array.isArray(model.series)) return null;
        let min = Infinity, max = -Infinity;
        model.series.forEach(/** @param {any} [ser] @returns {any} */ (ser) => {
            (ser.values || []).forEach(/** @param {any} [v] @returns {any} */ (v) => {
                if (typeof v === 'number' && Number.isFinite(v)) {
                    min = Math.min(min, v);
                    max = Math.max(max, v);
                }
            });
        });
        return Number.isFinite(min) && min !== max ? { min, max } : null;
    }, [model]);

    return (
        <div
            ref={immersiveContainerRef}
            style={{
                display: 'flex',
                flexDirection: 'column',
                gap: showChrome ? '14px' : '10px',
                flex: '1 1 auto',
                minHeight: 0,
                overflow: 'hidden',
            }}
        >
            {(showChrome || immersiveMode) ? (
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                    <ChartHeader
                        title={immersiveMode ? resolvedTitle : null}
                        subtitle={resolvedSubtitle}
                        note={resolvedNote}
                        theme={theme}
                    />
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        <div style={{ position: 'relative' }}>
                            <button
                                type="button"
                                ref={exportButtonRef}
                                onClick={() => setExportMenuOpen(/** @param {any} [isOpen] @returns {any} */ (isOpen) => !isOpen)}
                                onKeyDown={(event) => {
                                    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                                        event.preventDefault();
                                        setExportMenuOpen(true);
                                    }
                                }}
                                style={exportButtonStyle}
                                title="Export chart"
                                aria-haspopup="menu"
                                aria-expanded={exportMenuOpen}
                            >
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '5px' }}>
                                    Export <Icons.ChevronDown />
                                </span>
                            </button>
                            {exportMenuOpen && exportMenuTarget ? createPortal(
                                <div
                                    ref={exportMenuRef}
                                    role="menu"
                                    aria-label="Chart export formats"
                                    style={{
                                        position: 'fixed',
                                        ...exportMenuPosition,
                                        visibility: exportMenuPosition ? 'visible' : 'hidden',
                                        zIndex: 10060,
                                        width: '190px',
                                        boxSizing: 'border-box',
                                        overflowY: 'auto',
                                        fontFamily: 'inherit',
                                        padding: '6px',
                                        border: `1px solid ${theme.border}`,
                                        borderRadius: theme.radiusSm || '8px',
                                        background: theme.surfaceBg || theme.background || '#fff',
                                        boxShadow: theme.shadowMd || '0 12px 28px rgba(15,23,42,0.18)',
                                    }}
                                >
                                    {[
                                        { format: 'svg', label: 'SVG image', hint: 'Title + legend included', disabled: ECHARTS_CHART_TYPES.has(chartType) },
                                        { format: 'png', label: 'PNG image', hint: 'Title + legend included', disabled: false },
                                        { format: 'csv', label: 'CSV data', hint: 'Title + legend metadata', disabled: false },
                                    ].map((item) => (
                                        <button
                                            key={item.format}
                                            type="button"
                                            role="menuitem"
                                            disabled={item.disabled}
                                            onClick={() => { exportButtonRef.current?.focus(); handleChartExport(item.format); }}
                                            title={item.disabled ? 'SVG export is unavailable for this chart type' : undefined}
                                            style={{
                                                width: '100%',
                                                display: 'flex',
                                                flexDirection: 'column',
                                                alignItems: 'flex-start',
                                                gap: '2px',
                                                border: 'none',
                                                borderRadius: theme.radiusSm || '7px',
                                                background: 'transparent',
                                                color: item.disabled ? theme.textSec : theme.text,
                                                opacity: item.disabled ? 0.45 : 1,
                                                padding: '8px 9px',
                                                cursor: item.disabled ? 'not-allowed' : 'pointer',
                                                textAlign: 'left',
                                            }}
                                            onMouseEnter={(event) => { if (!item.disabled) event.currentTarget.style.background = theme.hover; }}
                                            onMouseLeave={(event) => { event.currentTarget.style.background = 'transparent'; }}
                                        >
                                            <span style={{ fontSize: '11px', fontWeight: 800 }}>{item.label}</span>
                                            <span style={{ fontSize: '10px', color: theme.textSec }}>{item.hint}</span>
                                        </button>
                                    ))}
                                </div>, exportMenuTarget
                            ) : null}
                        </div>
                        <button
                            type="button"
                            onClick={handleCopyChart}
                            style={exportButtonStyle}
                            title="Copy chart with title and legend"
                        >
                            Copy
                        </button>
                        {chartCopyStatus ? (
                            <span
                                role="status"
                                style={{
                                    fontSize: '10px',
                                    fontWeight: 800,
                                    color: chartCopyStatus.tone === 'success' ? (theme.primary || '#047857') : '#B91C1C',
                                    border: `1px solid ${chartCopyStatus.tone === 'success' ? `${theme.primary || '#047857'}55` : '#FCA5A5'}`,
                                    borderRadius: '999px',
                                    padding: '3px 7px',
                                    background: chartCopyStatus.tone === 'success' ? `${theme.primary || '#047857'}10` : '#FEF2F2',
                                }}
                            >
                                {chartCopyStatus.text}
                            </span>
                        ) : null}
                        {typeof onToggleLock === 'function' ? (
                            <button
                                type="button"
                                onClick={onToggleLock}
                                style={{ ...lockButtonStyle, width: 'auto', padding: '0 10px', gap: '6px', fontSize: '11px', fontWeight: 800 }}
                                title={locked ? 'Data is fixed to the saved selection. Click to resume live pivot updates.' : 'Keep this chart fixed to the current data selection while the pivot changes.'}
                                aria-label={locked ? 'Unlock chart data and resume live updates' : 'Lock chart to the current data selection'}
                                aria-pressed={locked}
                            >
                                <Icons.Lock />
                                <span>{locked ? 'Data locked' : 'Lock data'}</span>
                            </button>
                        ) : null}
                        {showChrome ? <button
                            type="button"
                            onMouseDown={(event) => event.stopPropagation()}
                            onClick={(event) => {
                                event.stopPropagation();
                                setConfigOpen((currentOpen) => !currentOpen);
                            }}
                            data-chart-settings-toggle="true"
                            style={configButtonStyle}
                            title={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                            aria-label={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                        >
                            <Icons.Settings />
                        </button> : null}
                    </div>
                </div>
            ) : null}
        <div style={{ display: 'flex', minHeight: 0, gap: 0, overflow: 'hidden', flex: '1 1 auto' }}>
        <div
            ref={chartSurfaceMainRef}
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
                    : <SvgChart model={model} chartType={chartType} barLayout={barLayout} axisMode={axisMode} theme={theme} chartHeight={chartHeight} showLegend={!isSparklineChart} showDataLabels={showDataLabels} onCategoryActivate={onCategoryActivate} svgRef={svgRef} echartsRef={echartsRef} colorPalette={colorPalette} valueFormat={valueFormat} yAxisTitle={yAxisTitle} yAxisMin={yAxisMin} yAxisMax={yAxisMax} referenceLines={referenceLines} onReferenceLinesChange={setReferenceLines} labelAngle={labelAngle} chartTitle={immersiveMode ? '' : resolvedTitle} onTitleChange={onTitleChange} subtitle={chartSubtitle} tickCount={tickCount} markerShape={markerShape} markerSize={markerSize} showGradient={showGradient} seriesColors={seriesColors} showAnimations={showAnimations} showCalloutLabels={showCalloutLabels} showTrendline={showTrendline} trendlineMode={trendlineMode} trendlinePeriod={trendlinePeriod} legendPosition={legendPosition} labelPosition={labelPosition} decimalPlaces={decimalPlaces} gradientDirection={gradientDirection} gradientOpacity={gradientOpacity} trendlineColor={trendlineColor} trendlineWidth={trendlineWidth} seriesSort={seriesSort} lineDashStyles={lineDashStyles} yAxisMinRight={yAxisMinRight} yAxisMaxRight={yAxisMaxRight} customPaletteColors={customPaletteColors} onElementSelect={handleElementSelect} />)}
                    {chartSubtitle ? <div style={{ textAlign: 'center', fontSize: '11px', color: theme.textSec, padding: '4px 8px 0', fontStyle: 'italic' }}>{chartSubtitle}</div> : null}
            {!immersiveMode && typeof onChartHeightChange === 'function' ? (
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
                            direction: 'left',
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
                        maxWidth: '680px',
                        flexShrink: 0,
                        minHeight: 0,
                        overflow: 'hidden',
                        borderLeft: `1px solid ${theme.border}`,
                        background: theme.sidebarBg || theme.surfaceBg || theme.background,
                        display: 'flex',
                        flexDirection: 'column',
                    }}
                >
                <div style={{
                    display: 'flex',
                    alignItems: 'stretch',
                    borderBottom: `1px solid ${theme.border}`,
                    background: theme.sidebarBg || theme.surfaceBg || theme.background,
                    flexShrink: 0,
                }}>
                    {[
                        { id: 'chart', label: 'Chart' },
                        { id: 'data', label: 'Data' },
                        { id: 'axes', label: 'Axes' },
                        { id: 'style', label: 'Style' },
                        { id: 'labels', label: 'Labels' },
                    ].map(tab => (
                        <button
                            key={tab.id}
                            type="button"
                            data-chart-settings-tab={tab.id}
                            onClick={() => setSettingsTab(tab.id)}
                            style={{
                                flex: 1,
                                padding: '9px 4px 7px',
                                fontSize: '11px',
                                fontWeight: settingsTab === tab.id ? 800 : 600,
                                color: settingsTab === tab.id ? theme.primary : theme.textSec,
                                background: 'none',
                                border: 'none',
                                borderBottom: `2px solid ${settingsTab === tab.id ? theme.primary : 'transparent'}`,
                                cursor: 'pointer',
                                letterSpacing: '0.03em',
                            }}
                        >{tab.label}</button>
                    ))}
                    <button
                        type="button"
                        onClick={() => setConfigOpen(false)}
                        style={{
                            padding: '9px 12px',
                            background: 'none',
                            border: 'none',
                            borderBottom: '2px solid transparent',
                            color: theme.textSec,
                            cursor: 'pointer',
                            fontSize: '14px',
                            lineHeight: 1,
                        }}
                        title="Close settings"
                    >✕</button>
                </div>
                <div
                    data-chart-settings-scroll="true"
                    style={{
                        overflowY: 'auto',
                        overflowX: 'hidden',
                        overscrollBehavior: 'contain',
                        padding: '12px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '8px',
                        flex: 1,
                        minHeight: 0,
                        background: theme.sidebarBg || theme.surfaceBg || theme.background,
                    }}
                >
                {settingsTab === 'chart' && <>
                    {/* Chart Type collapsible section */}
                    <button type="button" onClick={() => setChartTypeOpen(/** @param {any} [o] @returns {any} */ o => !o)} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', background: 'none', border: 'none', borderBottom: `1px solid ${theme.border}`, padding: '4px 0', cursor: 'pointer', marginBottom: chartTypeOpen ? '6px' : 0 }}>
                        <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec }}>Chart Type</span>
                        <span style={{ fontSize: '10px', color: theme.textSec, transform: chartTypeOpen ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.15s' }}>▾</span>
                    </button>
                    {chartTypeOpen && <ChartTypeGallery chartType={chartType} onChange={onChartTypeChange} theme={theme} includeHierarchyCharts={allowHierarchyCharts} />}
                    {/* Chart Parameters collapsible section */}
                    <button type="button" onClick={() => setChartParamsOpen(/** @param {any} [o] @returns {any} */ o => !o)} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', background: 'none', border: 'none', borderBottom: `1px solid ${theme.border}`, padding: '4px 0', cursor: 'pointer', marginTop: '8px', marginBottom: chartParamsOpen ? '6px' : 0 }}>
                        <span style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec }}>Parameters</span>
                        <span style={{ fontSize: '10px', color: theme.textSec, transform: chartParamsOpen ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.15s' }}>▾</span>
                    </button>
                    {chartParamsOpen && <>
                        {(chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'scatter' && chartType !== 'waterfall' && !ECHARTS_CHART_TYPES.has(chartType) && !isComboChart) ? (
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
                        {showHierarchySection ? (
                            <ChartConfigField label="Level" theme={theme}>
                                <ChartHierarchyButtons level={hierarchyLevel} onChange={onHierarchyLevelChange} maxLevel={maxHierarchyLevel} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                        {isComboChart ? (<>
                            <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Layers</div>
                            <ChartLayerEditor layers={chartLayers} onChange={onChartLayersChange} availableColumns={availableColumns} theme={theme} />
                        </>) : null}
                        {!(chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'scatter' && chartType !== 'waterfall' && !ECHARTS_CHART_TYPES.has(chartType) && !isComboChart) && !(chartType === 'bar') && !((chartType === 'bar' || chartType === 'area') && !isComboChart) && !showHierarchySection && !isComboChart ? (
                            <div style={{ fontSize: '11px', color: theme.textSec, padding: '4px 0' }}>No parameters for this chart type.</div>
                        ) : null}
                    </>}
                </>}
                {settingsTab === 'data' && <>
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>General</div>
                    {typeof onTitleChange === 'function' ? (
                        <ChartConfigField label="Title" theme={theme} controlMinWidth="100%">
                            <input type="text" value={resolvedTitle || ''} onChange={(event) => onTitleChange(event.target.value)} placeholder="Chart title" style={{ ...compactInputStyle, width: '100%' }} />
                        </ChartConfigField>
                    ) : null}
                    <ChartConfigField label="Subtitle" theme={theme} controlMinWidth="100%">
                        <input type="text" value={chartSubtitle} onChange={e => setChartSubtitle(e.target.value)} placeholder="Chart subtitle" style={{ ...compactInputStyle, width: '100%' }} />
                    </ChartConfigField>
                    {typeof onChartHeightChange === 'function' ? (
                        <ChartConfigField label="Height" theme={theme} controlMinWidth="96px">
                            <input type="number" min="180" step="20" value={heightDraft} onChange={(event) => setHeightDraft(event.target.value)} onBlur={() => { if (typeof onChartHeightChange === 'function') onChartHeightChange(heightDraft); }} onKeyDown={(event) => { if (event.key === 'Enter' && typeof onChartHeightChange === 'function') onChartHeightChange(heightDraft); }} style={compactInputStyle} />
                        </ChartConfigField>
                    ) : null}
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Data & Actions</div>
                    <ChartConfigField label="Limits" theme={theme} controlMinWidth="100%">
                        <ChartLimitInputs rowLimit={rowLimit} onRowLimitChange={onRowLimitChange} columnLimit={columnLimit} onColumnLimitChange={onColumnLimitChange} theme={theme} />
                    </ChartConfigField>
                    {typeof onSortModeChange === 'function' ? (
                        <ChartConfigField label="Sort" theme={theme} controlMinWidth="100%">
                            <ChartSortButtons sortMode={sortMode} onChange={onSortModeChange} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                    {typeof onSortModeChange === 'function' ? (
                        <ChartConfigField label="Series Sort" theme={theme} controlMinWidth="100%">
                            <div style={{ display: 'flex', gap: '5px', flexWrap: 'wrap' }}>
                                {SERIES_SORT_MODES.map(m => (
                                    <button key={m.value} type="button" onClick={() => setSeriesSort(m.value)} style={{ border: `1px solid ${seriesSort === m.value ? theme.primary : theme.border}`, background: seriesSort === m.value ? theme.select : (theme.headerSubtleBg || theme.hover), color: seriesSort === m.value ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '5px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                        {m.label}
                                    </button>
                                ))}
                            </div>
                        </ChartConfigField>
                    ) : null}
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
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Presets</div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '6px' }}>
                        {[
                            { key: 'trend', label: 'Trend' },
                            { key: 'compare', label: 'Compare' },
                            { key: 'composition', label: 'Composition' },
                            { key: 'flow', label: 'Flow' },
                            { key: 'sparkline', label: 'Sparkline' },
                        ].map((preset) => (
                            <button key={preset.key} type="button" data-chart-preset={preset.key} onClick={() => applyChartPreset(preset.key)} style={{ ...exportButtonStyle, textAlign: 'left' }}>
                                {preset.label}
                            </button>
                        ))}
                    </div>
                </>}
                {settingsTab === 'axes' && <>
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'sparkline' ? (
                        <ChartConfigField label="Y-Axis Title" theme={theme} controlMinWidth="100%">
                            <input type="text" value={yAxisTitle} onChange={(event) => setYAxisTitle(event.target.value)} placeholder="Y-axis title" style={{ ...compactInputStyle, width: '100%' }} />
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'sparkline' && !ECHARTS_CHART_TYPES.has(chartType) ? (<>
                        <ChartConfigField label="Y Range" theme={theme} controlMinWidth="100%">
                            <div style={{ display: 'flex', gap: '6px' }}>
                                <input type="number" value={yAxisMin} onChange={(e) => setYAxisMin(e.target.value)} placeholder="Min" style={{ ...compactInputStyle, width: '50%' }} />
                                <input type="number" value={yAxisMax} onChange={(e) => setYAxisMax(e.target.value)} placeholder="Max" style={{ ...compactInputStyle, width: '50%' }} />
                            </div>
                        </ChartConfigField>
                        <ChartConfigField label="Y Ticks" theme={theme} controlMinWidth="80px">
                            <input type="number" min="2" max="20" value={tickCount} onChange={e => setTickCount(Math.max(2, Math.min(20, Number(e.target.value) || 5)))} style={{ ...compactInputStyle, width: '80px' }} />
                        </ChartConfigField>
                        <ChartConfigField label="X Angle" theme={theme} controlMinWidth="100%">
                            <div style={{ display: 'flex', gap: '4px', alignItems: 'center', flexWrap: 'wrap' }}>
                                <input type="number" min="0" max="90" value={labelAngle} onChange={(e) => setLabelAngle(Math.max(0, Math.min(90, Number(e.target.value) || 0)))} style={{ ...compactInputStyle, width: '58px' }} title="X-axis label angle (0–90°)" />
                                <span style={{ fontSize: '11px', color: theme.textSec }}>°</span>
                                {[0, 30, 45, 90].map(a => (
                                    <button key={a} type="button" onClick={() => setLabelAngle(a)} style={{ ...compactInputStyle, padding: '4px 6px', background: labelAngle === a ? theme.select : (theme.headerSubtleBg || theme.hover), color: labelAngle === a ? theme.primary : theme.text, border: `1px solid ${labelAngle === a ? theme.primary : theme.border}` }}>{a}°</button>
                                ))}
                            </div>
                        </ChartConfigField>
                    </>) : null}
                    {isComboChart ? (
                        <ChartConfigField label="Right Axis Range" theme={theme} controlMinWidth="100%">
                            <div style={{ display: 'flex', gap: '6px' }}>
                                <input type="number" value={yAxisMinRight} onChange={e => setYAxisMinRight(e.target.value)} placeholder="Min" style={{ ...compactInputStyle, width: '50%' }} />
                                <input type="number" value={yAxisMaxRight} onChange={e => setYAxisMaxRight(e.target.value)} placeholder="Max" style={{ ...compactInputStyle, width: '50%' }} />
                            </div>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && !ECHARTS_CHART_TYPES.has(chartType) ? (<>
                        <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Reference Lines</div>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                            {referenceLines.map(/** @param {any} [line] @param {any} [lineIdx] @returns {any} */ (line, lineIdx) => (
                                <ChartReflineRow key={line.id} line={line} onUpdate={/** @param {any} [updated] @returns {any} */ (updated) => setReferenceLines(/** @param {any} [prev] @returns {any} */ prev => prev.map(/** @param {any} [l] @param {any} [j] @returns {any} */ (l, j) => j === lineIdx ? updated : l))} onRemove={() => setReferenceLines(/** @param {any} [prev] @returns {any} */ prev => prev.filter(/** @param {any} [_] @param {any} [j] @returns {any} */ (_, j) => j !== lineIdx))} exportButtonStyle={exportButtonStyle} compactInputStyle={compactInputStyle} theme={theme} />
                            ))}
                            <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                                <button type="button" onClick={() => setRefLineDraft(/** @param {any} [d] @returns {any} */ d => ({ ...d, orient: d.orient === 'v' ? 'h' : 'v' }))} style={{ ...exportButtonStyle, padding: '4px 7px', flexShrink: 0, fontWeight: 800, color: refLineDraft.orient === 'v' ? theme.primary : theme.textSec }}>{refLineDraft.orient === 'v' ? '|' : '—'}</button>
                                <input type="number" value={refLineDraft.value} onChange={(e) => setRefLineDraft(/** @param {any} [d] @returns {any} */ d => ({ ...d, value: e.target.value }))} placeholder={refLineDraft.orient === 'v' ? 'Cat index' : 'Value'} style={{ ...compactInputStyle, width: '60px', flexShrink: 0 }} />
                                <input type="text" value={refLineDraft.label} onChange={(e) => setRefLineDraft(/** @param {any} [d] @returns {any} */ d => ({ ...d, label: e.target.value }))} placeholder="Label" style={{ ...compactInputStyle, flex: 1, minWidth: 0 }} />
                                <button type="button" onClick={() => { if (refLineDraft.value === '') return; setReferenceLines(/** @param {any} [prev] @returns {any} */ prev => [...prev, { id: Date.now(), value: refLineDraft.value, label: refLineDraft.label, orient: refLineDraft.orient, color: '', dash: 'dashed' }]); setRefLineDraft({ value: '', label: '', orient: refLineDraft.orient }); }} style={{ ...exportButtonStyle, padding: '4px 10px', flexShrink: 0 }}>+</button>
                            </div>
                            {yRange ? (
                                <div style={{ fontSize: '10px', color: theme.textSec }}>
                                    Data range: {formatChartValue(yRange.min, valueFormat)} — {formatChartValue(yRange.max, valueFormat)}
                                </div>
                            ) : null}
                        </div>
                    </>) : null}
                </>}
                {settingsTab === 'style' && <>
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}` }}>Colors</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                        {[...COLOR_PALETTE_NAMES, ...(customPaletteColors.length >= 2 ? ['custom'] : [])].map((name) => {
                            /** @type {any} */
                            const active = colorPalette === name;
                            /** @type {any} */
                            const swatches = name === 'custom' ? customPaletteColors : COLOR_PALETTES[name];
                            return (
                                <button key={name} type="button" onClick={() => setColorPalette(name)} title={name.charAt(0).toUpperCase() + name.slice(1)}
                                    style={{ display: 'flex', alignItems: 'center', gap: '3px', padding: '3px 6px', border: `1.5px solid ${active ? theme.primary : theme.border}`, background: active ? theme.select : (theme.headerSubtleBg || theme.hover), borderRadius: theme.radiusSm || '8px', cursor: 'pointer' }}
                                >
                                    {(swatches || []).slice(0, 4).map(/** @param {any} [hex] @param {any} [i] @returns {any} */ (hex, i) => (
                                        <span key={i} style={{ width: '8px', height: '8px', borderRadius: '50%', background: hex, flexShrink: 0 }} />
                                    ))}
                                    <span style={{ fontSize: '9px', fontWeight: active ? 700 : 600, color: active ? theme.primary : theme.textSec, marginLeft: '1px', textTransform: 'capitalize' }}>
                                        {name === 'a11y' ? 'A11y' : name}
                                    </span>
                                </button>
                            );
                        })}
                    </div>
                    {Array.isArray(model && model.series) && model.series.length > 0 ? (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                            <div style={{ fontSize: '10px', fontWeight: 700, color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '2px' }}>Override per series</div>
                            {model.series.slice(0, 20).map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => (
                                <div key={ser.name || si} style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                                    <div style={{ display: 'flex', gap: '2px', flexShrink: 0 }}>
                                        {paletteColors.slice(0, 6).map(/** @param {any} [hex] @param {any} [pi] @returns {any} */ (hex, pi) => (
                                            <button key={pi} type="button" onClick={() => setSeriesColors(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, [ser.name]: hex }))} style={{ width: '14px', height: '14px', borderRadius: '50%', background: hex, border: `2px solid ${(seriesColors[ser.name] || getColorForIndex(si, theme, paletteColors)) === hex ? theme.text : 'transparent'}`, cursor: 'pointer', padding: 0, flexShrink: 0 }} title={hex} />
                                        ))}
                                    </div>
                                    <input type="color" value={seriesColors[ser.name] || getColorForIndex(si, theme, paletteColors)} onChange={(e) => setSeriesColors(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, [ser.name]: e.target.value }))} style={{ width: '24px', height: '24px', padding: 0, border: `1px solid ${theme.border}`, borderRadius: '4px', cursor: 'pointer', background: 'none', flexShrink: 0 }} title={`Custom color for ${ser.name}`} />
                                    <span style={{ fontSize: '11px', color: theme.text, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{ser.name || `Series ${si + 1}`}</span>
                                    {seriesColors[ser.name] ? (
                                        <button type="button" onClick={() => setSeriesColors(/** @param {any} [prev] @returns {any} */ prev => { /** @type {Record<string, any>} */ const next = { ...prev }; delete next[ser.name]; return next; })} style={{ border: 'none', background: 'none', color: theme.textSec, cursor: 'pointer', fontSize: '10px', padding: '0 2px' }} title="Reset to palette">↺</button>
                                    ) : null}
                                </div>
                            ))}
                            {model.series.length > 20 ? <div style={{ fontSize: '10px', color: theme.textSec }}>{`…${model.series.length - 20} more`}</div> : null}
                        </div>
                    ) : null}
                    <div style={{ borderTop: `1px solid ${theme.border}`, paddingTop: '8px' }}>
                        <div style={{ fontSize: '10px', fontWeight: 700, color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '6px' }}>Custom Palette</div>
                        <ChartCustomPaletteEditor customColors={customPaletteColors} onChange={setCustomPaletteColors} theme={theme} compactInputStyle={compactInputStyle} />
                        {customPaletteColors.length >= 2 ? (
                            <button type="button" onClick={() => setColorPalette('custom')} style={{ ...exportButtonStyle, marginTop: '6px', background: colorPalette === 'custom' ? theme.select : undefined, color: colorPalette === 'custom' ? theme.primary : undefined, border: `1px solid ${colorPalette === 'custom' ? theme.primary : theme.border}` }}>
                                {colorPalette === 'custom' ? 'Custom active' : 'Use custom palette'}
                            </button>
                        ) : null}
                    </div>
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Effects</div>
                    <ChartConfigField label="Animate" theme={theme}>
                        <button type="button" onClick={() => setShowAnimations(/** @param {any} [v] @returns {any} */ v => !v)} style={{ border: `1px solid ${showAnimations ? theme.primary : theme.border}`, background: showAnimations ? theme.select : (theme.headerSubtleBg || theme.hover), color: showAnimations ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '6px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                            {showAnimations ? 'On' : 'Off'}
                        </button>
                    </ChartConfigField>
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' && chartType !== 'sparkline' && !ECHARTS_CHART_TYPES.has(chartType) ? (<>
                        <ChartConfigField label="Gradient" theme={theme}>
                            <button type="button" onClick={() => setShowGradient(/** @param {any} [v] @returns {any} */ v => !v)} style={{ border: `1px solid ${showGradient ? theme.primary : theme.border}`, background: showGradient ? theme.select : (theme.headerSubtleBg || theme.hover), color: showGradient ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '6px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                {showGradient ? 'On' : 'Off'}
                            </button>
                        </ChartConfigField>
                        {showGradient ? (<>
                            <ChartConfigField label="Grad Dir" theme={theme} controlMinWidth="100%">
                                <div style={{ display: 'flex', gap: '5px' }}>
                                    {GRADIENT_DIRECTIONS.map(d => (
                                        <button key={d.value} type="button" onClick={() => setGradientDirection(d.value)} style={{ border: `1px solid ${gradientDirection === d.value ? theme.primary : theme.border}`, background: gradientDirection === d.value ? theme.select : (theme.headerSubtleBg || theme.hover), color: gradientDirection === d.value ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '5px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                            {d.label}
                                        </button>
                                    ))}
                                </div>
                            </ChartConfigField>
                            <ChartConfigField label="Grad Opacity" theme={theme} controlMinWidth="80px">
                                <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                                    <input type="range" min="10" max="100" step="5" value={gradientOpacity} onChange={e => setGradientOpacity(Number(e.target.value))} style={{ flex: 1 }} />
                                    <span style={{ fontSize: '11px', color: theme.textSec, minWidth: '30px' }}>{gradientOpacity}%</span>
                                </div>
                            </ChartConfigField>
                        </>) : null}
                    </>) : null}
                    {(chartType === 'line' || chartType === 'scatter' || chartType === 'bubble' || chartType === 'area' || chartType === 'radar' || isComboChart) ? (<>
                        <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Markers</div>
                        <ChartConfigField label="Shape" theme={theme} controlMinWidth="100%">
                            <select value={markerShape} onChange={e => setMarkerShape(e.target.value)} style={{ ...compactInputStyle, width: '100%' }}>
                                <option value="none">None (line only)</option>
                                <option value="circle">Circle</option>
                                <option value="square">Square</option>
                                <option value="diamond">Diamond</option>
                                <option value="triangle">Triangle</option>
                                <option value="cross">Cross</option>
                            </select>
                        </ChartConfigField>
                        <ChartConfigField label="Size" theme={theme} controlMinWidth="80px">
                            <select value={markerSize} onChange={e => setMarkerSize(Number(e.target.value))} style={{ ...compactInputStyle, width: '80px' }}>
                                <option value={3}>Small</option>
                                <option value={5}>Medium</option>
                                <option value={7}>Large</option>
                                <option value={10}>XLarge</option>
                            </select>
                        </ChartConfigField>
                    </>) : null}
                    {(chartType === 'line' || chartType === 'area' || isComboChart) && Array.isArray(model && model.series) && model.series.length > 0 ? (<>
                        <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Line Style</div>
                        <ChartLineDashEditor series={model.series} lineDashStyles={lineDashStyles} onChange={setLineDashStyles} seriesColors={seriesColors} paletteColors={paletteColors} getColorForIndex={getColorForIndex} theme={theme} compactInputStyle={compactInputStyle} />
                    </>) : null}
                    {(chartType === 'bar' || chartType === 'line' || chartType === 'area' || chartType === 'scatter' || isComboChart) ? (<>
                        <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec, paddingBottom: '4px', borderBottom: `1px solid ${theme.border}`, marginTop: '4px' }}>Trendline</div>
                        <ChartConfigField label="Show" theme={theme}>
                            <button type="button" onClick={() => setShowTrendline(/** @param {any} [v] @returns {any} */ v => !v)} style={{ border: `1px solid ${showTrendline ? theme.primary : theme.border}`, background: showTrendline ? theme.select : (theme.headerSubtleBg || theme.hover), color: showTrendline ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '6px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                {showTrendline ? 'On' : 'Off'}
                            </button>
                        </ChartConfigField>
                        {showTrendline ? (<>
                            <ChartConfigField label="Type" theme={theme} controlMinWidth="100%">
                                <select value={trendlineMode} onChange={e => setTrendlineMode(e.target.value)} style={{ ...compactInputStyle, width: '100%' }}>
                                    <option value="linear">Linear Regression</option>
                                    <option value="moving_avg">Moving Average</option>
                                    <option value="exponential">Exponential Smoothing</option>
                                </select>
                            </ChartConfigField>
                            {trendlineMode === 'moving_avg' ? (
                                <ChartConfigField label="Period" theme={theme} controlMinWidth="80px">
                                    <input type="number" min="2" max="50" value={trendlinePeriod} onChange={e => setTrendlinePeriod(Math.max(2, Math.min(50, Number(e.target.value) || 3)))} style={{ ...compactInputStyle, width: '80px' }} />
                                </ChartConfigField>
                            ) : null}
                            <ChartConfigField label="Color" theme={theme}>
                                <input type="color" value={trendlineColor || '#888888'} onChange={e => setTrendlineColor(e.target.value)} style={{ width: '32px', height: '28px', padding: 0, border: `1px solid ${theme.border}`, borderRadius: '4px', cursor: 'pointer', background: 'none' }} title="Trendline color" />
                                {trendlineColor ? <button type="button" onClick={() => setTrendlineColor('')} style={{ border: 'none', background: 'none', color: theme.textSec, cursor: 'pointer', fontSize: '10px', padding: '0 4px' }} title="Reset to auto">↺</button> : null}
                            </ChartConfigField>
                            <ChartConfigField label="Width" theme={theme} controlMinWidth="80px">
                                <input type="number" min="1" max="8" value={trendlineWidth} onChange={e => setTrendlineWidth(Math.max(1, Math.min(8, Number(e.target.value) || 2)))} style={{ ...compactInputStyle, width: '80px' }} />
                            </ChartConfigField>
                        </>) : null}
                    </>) : null}
                </>}
                {settingsTab === 'labels' && <>
                    {chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'sparkline' ? (
                        <ChartConfigField label="Data Labels" theme={theme}>
                            <button type="button" onClick={() => setShowDataLabels(/** @param {any} [v] @returns {any} */ (v) => !v)} style={{ border: `1px solid ${showDataLabels ? theme.primary : theme.border}`, background: showDataLabels ? theme.select : (theme.headerSubtleBg || theme.hover), color: showDataLabels ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '6px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                {showDataLabels ? 'On' : 'Off'}
                            </button>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'pie' && chartType !== 'donut' ? (
                        <ChartConfigField label="Format" theme={theme} controlMinWidth="100%">
                            <select value={valueFormat} onChange={(event) => setValueFormat(event.target.value)} style={{ ...compactInputStyle, width: '100%' }} title="Value format for axis labels">
                                {VALUE_FORMAT_MODES.map((opt) => (
                                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                                ))}
                            </select>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'heatmap' && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' && chartType !== 'sparkline' && !ECHARTS_CHART_TYPES.has(chartType) ? (
                        <ChartConfigField label="Decimals" theme={theme} controlMinWidth="100%">
                            <select value={decimalPlaces} onChange={e => setDecimalPlaces(Number(e.target.value))} style={{ ...compactInputStyle, width: '100%' }}>
                                <option value={-1}>Auto</option>
                                <option value={0}>0  (e.g. 1,234)</option>
                                <option value={1}>1  (e.g. 1.2)</option>
                                <option value={2}>2  (e.g. 1.23)</option>
                                <option value={3}>3  (e.g. 1.234)</option>
                            </select>
                        </ChartConfigField>
                    ) : null}
                    {showDataLabels && chartType === 'bar' ? (
                        <ChartConfigField label="Position" theme={theme} controlMinWidth="100%">
                            <select value={labelPosition} onChange={e => setLabelPosition(e.target.value)} style={{ ...compactInputStyle, width: '100%' }}>
                                <option value="outside">Outside</option>
                                <option value="inside">Inside top</option>
                                <option value="center">Center</option>
                            </select>
                        </ChartConfigField>
                    ) : null}
                    {(chartType === 'pie' || chartType === 'donut') ? (
                        <ChartConfigField label="Callouts" theme={theme}>
                            <button type="button" onClick={() => setShowCalloutLabels(/** @param {any} [v] @returns {any} */ v => !v)} style={{ border: `1px solid ${showCalloutLabels ? theme.primary : theme.border}`, background: showCalloutLabels ? theme.select : (theme.headerSubtleBg || theme.hover), color: showCalloutLabels ? theme.primary : theme.text, borderRadius: theme.radiusSm || '8px', padding: '6px 8px', fontSize: '11px', fontWeight: 700, cursor: 'pointer' }}>
                                {showCalloutLabels ? 'On' : 'Off'}
                            </button>
                        </ChartConfigField>
                    ) : null}
                    {chartType !== 'sparkline' && !ECHARTS_CHART_TYPES.has(chartType) ? (
                        <ChartConfigField label="Legend" theme={theme} controlMinWidth="100%">
                            <ChartLegendPositionButtons legendPosition={legendPosition} onChange={setLegendPosition} theme={theme} />
                        </ChartConfigField>
                    ) : null}
                </>}
                </div>
                </aside>
            </>
        ) : null}
        </div>
        {showChrome ? (
            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', alignItems: 'center' }}>
                <ChartStats model={model} theme={theme} />
                <ChartDataLimitWarning model={model} maxCategories={MAX_PANEL_CATEGORIES} maxSeries={MAX_PANEL_SERIES} theme={theme} />
            </div>
        ) : null}
    </div>
    );
};

/** @param {any} p0 @returns {any} */
const PivotChartPanelContent = ({
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
    immersiveMode: controlledImmersiveMode,
    onImmersiveModeChange,
    onSettingsWidthBudgetChange,
    standalone = false,
    showResizeHandle = true,
    title = 'Chart Panel',
    showDefinitionManager = true,
    initialSettings = {},
    onSettingsChange,
    initialSettingsPaneOpen = false,
}) => {
    usePivotRenderCounter('PivotChartPanel', activeChartId);
    /** @type {[any, (...args: any[]) => any]} */
    const [fullscreenMode, setFullscreenMode] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [uncontrolledImmersiveMode, setUncontrolledImmersiveMode] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [settingsPaneOpen, setSettingsPaneOpen] = useState(initialSettingsPaneOpen);
    /** @type {[any, (...args: any[]) => any]} */
    const [settingsPanelWidth, setSettingsPanelWidth] = useState(348);
    /** @type {any} */
    const panelAsideRef = useRef(null);
    /** @type {any} */
    const baseDockedPanelWidthRef = useRef(clampChartPanelBaseWidth(
        width,
        430,
        MIN_CHART_PANEL_WIDTH,
        MAX_CHART_PANEL_WIDTH,
    ));
    /** @type {any} */
    const onSettingsWidthBudgetChangeRef = useRef(onSettingsWidthBudgetChange);
    /** @type {any} */
    const lastSettingsWidthBudgetRef = useRef(null);
    /** @type {any} */
    const immersiveMode = typeof controlledImmersiveMode === 'boolean' ? controlledImmersiveMode : uncontrolledImmersiveMode;
    /** @returns {any} */
    const toggleImmersiveMode = () => {
        const nextValue = !immersiveMode;
        if (typeof onImmersiveModeChange === 'function') {
            onImmersiveModeChange(nextValue);
            return;
        }
        setUncontrolledImmersiveMode(nextValue);
    };
    /** @type {any} */
    const floatingInteractionLocked = floating && locked;

    /** @param {any} [value] @returns {any} */
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
    /** @param {any} [active] @returns {any} */
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
    /** @param {any} [value] @returns {any} */
    const dockButtonStyle = (value) => ({
        ...actionButtonStyle(dockPosition === value),
        minWidth: '56px',
        padding: '6px 10px',
    });
    /** @type {any} */
    const visibleSettingsWidth = (!immersiveMode && settingsPaneOpen) ? (settingsPanelWidth + 9) : 0;
    /** @type {any} */
    const basePanelWidth = clampChartPanelBaseWidth(
        width,
        430,
        MIN_CHART_PANEL_WIDTH,
        MAX_CHART_PANEL_WIDTH,
    );
    /** @type {any} */
    const floatingPanelWidth = (floatingRect && Number.isFinite(Number(floatingRect.width)))
        ? Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(Number(floatingRect.width)))
        : basePanelWidth;
    /** @type {any} */
    const floatingPanelHeight = (floatingRect && Number.isFinite(Number(floatingRect.height)))
        ? Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.floor(Number(floatingRect.height)))
        : Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, (Number(chartHeight) || CHART_HEIGHT) + CHART_PANEL_VERTICAL_CHROME_HEIGHT);

    useEffect(() => {
        onSettingsWidthBudgetChangeRef.current = onSettingsWidthBudgetChange;
    }, [onSettingsWidthBudgetChange]);

    /** @type {(...args: any[]) => any} */
    const emitSettingsWidthBudget = useCallback((nextWidthHint, options = {}) => {
        /** @type {any} */
        const numericWidthHint = Number(nextWidthHint);
        /** @type {any} */
        const normalizedHint = Number.isFinite(numericWidthHint)
            ? Math.max(320, Math.ceil(numericWidthHint))
            : null;
        if (!options.force && lastSettingsWidthBudgetRef.current === normalizedHint) return;
        lastSettingsWidthBudgetRef.current = normalizedHint;
        const callback = onSettingsWidthBudgetChangeRef.current;
        if (typeof callback === 'function') {
            callback(normalizedHint);
        }
    }, []);

    /** @type {any} */
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
                height: `${floatingPanelHeight}px`,
                zIndex: 240,
                display: 'flex',
                minWidth: 0,
                minHeight: 0,
            }
            : standalone
                ? {
                    display: 'flex',
                    flexGrow: 1,
                    flexShrink: 1,
                    flexBasis: 'auto',
                    width: '100%',
                    height: '100%',
                    minWidth: 0,
                    minHeight: 0,
                    overflow: 'hidden',
                }
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
        /** @returns {any} */
        const updateWidth = () => {
            const nextWidth = element.clientWidth;
            if (nextWidth > 0 && !settingsPaneOpen) {
                baseDockedPanelWidthRef.current = Math.max(MIN_CHART_PANEL_WIDTH, nextWidth);
            }
        };
        updateWidth();
        if (typeof ResizeObserver === 'undefined') return undefined;
        /** @type {any} */
        const observer = new ResizeObserver(updateWidth);
        observer.observe(element);
        return () => observer.disconnect();
    }, [floating, fullscreenMode, settingsPaneOpen, standalone]);

    useEffect(() => {
        if (!standalone || floating || fullscreenMode || immersiveMode || !settingsPaneOpen) {
            emitSettingsWidthBudget(null);
            return;
        }
        /** @type {any} */
        const baseWidthHint = baseDockedPanelWidthRef.current || basePanelWidth;
        emitSettingsWidthBudget(baseWidthHint + visibleSettingsWidth);
    }, [
        basePanelWidth,
        emitSettingsWidthBudget,
        immersiveMode,
        floating,
        fullscreenMode,
        settingsPaneOpen,
        standalone,
        visibleSettingsWidth,
    ]);

    useEffect(() => () => {
        emitSettingsWidthBudget(null, { force: true });
    }, [emitSettingsWidthBudget]);

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
                gap: immersiveMode ? '8px' : '0',
                position: 'relative',
            }}>
                <div
                    onMouseDown={(event) => {
                        if (floating && !fullscreenMode && !floatingInteractionLocked && typeof onFloatingDragStart === 'function') {
                            onFloatingDragStart(event);
                        }
                    }}
                    style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: immersiveMode ? '0' : '12px', cursor: floating && !fullscreenMode && !floatingInteractionLocked ? 'move' : 'default' }}
                >
                    {immersiveMode ? <div /> : (
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
                            onClick={toggleImmersiveMode}
                            style={actionButtonStyle(immersiveMode)}
                            title={immersiveMode ? 'Exit immersive mode' : 'Enter immersive mode'}
                            aria-label={immersiveMode ? 'Exit immersive mode' : 'Enter immersive mode'}
                        >
                            Immersive
                        </button>
                        <button
                            type="button"
                            onClick={() => setFullscreenMode(/** @param {any} [isActive] @returns {any} */ (isActive) => !isActive)}
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
                {!immersiveMode && showDefinitionManager ? (
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
                {!immersiveMode && standalone && !floating && typeof onDockPositionChange === 'function' ? (
                    <div style={{ display: 'flex', gap: '6px', marginBottom: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
                        <button type="button" onClick={() => onDockPositionChange('left')} style={dockButtonStyle('left')} title="Dock chart pane to the left">Left</button>
                        <button type="button" onClick={() => onDockPositionChange('right')} style={dockButtonStyle('right')} title="Dock chart pane to the right">Right</button>
                        <button type="button" onClick={() => onDockPositionChange('top')} style={dockButtonStyle('top')} title="Dock chart pane to the top">Top</button>
                        <button type="button" onClick={() => onDockPositionChange('bottom')} style={dockButtonStyle('bottom')} title="Dock chart pane to the bottom">Bottom</button>
                    </div>
                ) : null}
                {!immersiveMode ? (
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
                    immersiveMode={immersiveMode}
                    onCategoryActivate={onCategoryActivate}
                    allowHierarchyCharts={allowHierarchyCharts}
                    floating={floating}
                    locked={locked}
                    onToggleLock={onToggleLock}
                    configOpen={settingsPaneOpen}
                    onConfigChange={setSettingsPaneOpen}
                    settingsPanelWidth={settingsPanelWidth}
                    onSettingsPanelWidthChange={setSettingsPanelWidth}
                    initialSettings={initialSettings}
                    onSettingsChange={onSettingsChange}
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

/** @param {any} [props] @returns {any} */
export const PivotChartPanel = (props) => {
    if (!props.open) return null;
    return <PivotChartPanelContent {...props} />;
};

/** @param {any} p0 @returns {any} */
export const PivotChartModal = ({ chartState, onClose, theme, position = 'right', onPositionChange }) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [chartType, setChartType] = useState(chartState && chartState.chartType ? chartState.chartType : 'bar');
    /** @type {[any, (...args: any[]) => any]} */
    const [barLayout, setBarLayout] = useState(chartState && chartState.barLayout ? chartState.barLayout : 'grouped');
    /** @type {[any, (...args: any[]) => any]} */
    const [axisMode, setAxisMode] = useState(chartState && chartState.axisMode ? chartState.axisMode : 'vertical');
    /** @type {[any, (...args: any[]) => any]} */
    const [orientation, setOrientation] = useState(chartState && chartState.defaultOrientation ? chartState.defaultOrientation : 'rows');
    /** @type {[any, (...args: any[]) => any]} */
    const [hierarchyLevel, setHierarchyLevel] = useState(chartState && chartState.defaultHierarchyLevel !== undefined ? chartState.defaultHierarchyLevel : 'all');
    /** @type {[any, (...args: any[]) => any]} */
    const [rowLimit, setRowLimit] = useState(chartState && chartState.rowLimit ? chartState.rowLimit : MAX_SELECTION_CATEGORIES);
    /** @type {[any, (...args: any[]) => any]} */
    const [columnLimit, setColumnLimit] = useState(chartState && chartState.columnLimit ? chartState.columnLimit : MAX_SELECTION_SERIES);
    /** @type {[any, (...args: any[]) => any]} */
    const [sortMode, setSortMode] = useState(chartState && chartState.sortMode ? chartState.sortMode : 'natural');
    /** @type {[any, (...args: any[]) => any]} */
    const [chartLayers, setChartLayers] = useState(chartState && Array.isArray(chartState.chartLayers) ? chartState.chartLayers : []);
    /** @type {[any, (...args: any[]) => any]} */
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

    /** @type {any} */
    const availableColumns = useMemo(() => {
        if (!chartState || !Array.isArray(chartState.visibleCols)) return [];
        if (!(chartState.selectionMap && chartState.visibleRows)) {
            return getChartableColumns(chartState.visibleCols);
        }
        /** @type {any} */
        const bounds = buildSelectionBounds(chartState.selectionMap, chartState.visibleRows, chartState.visibleCols);
        if (!bounds) return [];
        /** @type {any} */
        const selectedColumnIds = new Set(
            (bounds.colIndexes || [])
                .map(/** @param {any} [index] @returns {any} */ (index) => chartState.visibleCols[index])
                .filter(/** @param {any} [column] @returns {any} */ (column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
                .map(/** @param {any} [column] @returns {any} */ (column) => column.id)
        );
        return getChartableColumns(chartState.visibleCols).filter(/** @param {any} [column] @returns {any} */ (column) => selectedColumnIds.has(column.id));
    }, [chartState]);

    /** @type {any} */
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
        /** @type {any} */
        const canKeepStacked = chartType === 'area'
            ? (activeModel && Array.isArray(activeModel.series) && activeModel.series.length > 1)
            : canStackBarLayout(activeModel);
        if (canKeepStacked) return;
        setBarLayout('grouped');
    }, [activeModel, barLayout, chartType]);

    if (!chartState) return null;
    /** @type {any} */
    const isVerticalPosition = position === 'top' || position === 'bottom';
    /** @type {any} */
    const panelWidthBudget = Math.max(420, Number.isFinite(Number(settingsWidthBudget)) ? Math.floor(Number(settingsWidthBudget)) : 420);
    /** @type {any} */
    const panelHeightBudget = Math.max(360, Number(chartState.chartHeight || CHART_HEIGHT) + CHART_PANEL_VERTICAL_CHROME_HEIGHT);

    /** @param {any} [value] @param {any} [label] @returns {any} */
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

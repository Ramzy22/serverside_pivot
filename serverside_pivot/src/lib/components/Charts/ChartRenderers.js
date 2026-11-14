import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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

/** @type {any} */
const ECHARTS_CHART_TYPES = new Set(['bar3d', 'line3d', 'scatter3d', 'range', 'boxplot', 'nightingale']);
/** @type {any} */
const ECHARTS_3D_CHART_TYPES = new Set(['bar3d', 'line3d', 'scatter3d']);

// The SVG renderer covers the common chart types. The 2D ECharts subset and
// its much larger WebGL extension each load only for chart types that need it.
/** @type {any} */
const LazyECharts2dRenderer = React.lazy(() => import(/* webpackChunkName: "pivot-echarts-2d" */ './ECharts2dRenderer'));
/** @type {any} */
const LazyECharts3dRenderer = React.lazy(() => import(/* webpackChunkName: "pivot-echarts-3d" */ './ECharts3dRenderer'));

/** @param {any} p0 @returns {any} */
const DeferredEChartsRenderer = ({ chartType, echartsRef, option }) => {
    /** @type {any} */
    const Renderer = ECHARTS_3D_CHART_TYPES.has(chartType)
        ? LazyECharts3dRenderer
        : LazyECharts2dRenderer;
    return (
    <React.Suspense fallback={<div aria-label="Loading chart" style={{ width: '100%', height: '100%' }} />}>
        <Renderer echartsRef={echartsRef} option={option} />
    </React.Suspense>
    );
};

/** @param {any} p0 @returns {any} */
export const EmptyChartState = ({ message, theme, chartHeight = CHART_HEIGHT }) => (
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

/** @param {any} p0 @returns {any} */
export const ChartStats = ({ model, theme }) => {
    /** @type {any} */
    const metrics = useMemo(() => {
        /** @type {any[]} */
        const values = [];
        (model && model.series ? model.series : []).forEach(/** @param {any} [series] @returns {any} */ (series) => {
            (series.values || []).forEach(/** @param {any} [value] @returns {any} */ (value) => {
                if (typeof value === 'number' && Number.isFinite(value)) values.push(value);
            });
        });
        if (values.length === 0 && model && Array.isArray(model.icicleNodes)) {
            model.icicleNodes.forEach(/** @param {any} [node] @returns {any} */ (node) => {
                /** @type {any} */
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

    /** @type {Record<string, any>} */
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

/** @param {any} p0 @returns {any} */
export const ChartLegend = ({ items, theme, hiddenSet, onToggle, position = 'bottom', onElementSelect }) => {
    if (position === 'none') return null;
    if (!Array.isArray(items) || items.length === 0) return null;
    /** @type {any} */
    const canToggle = typeof onToggle === 'function' && hiddenSet;

    return (
        <div data-chart-export-legend="true" style={{
            display: 'flex',
            gap: '10px',
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: position === 'top' ? 'flex-start' : 'flex-start',
            order: position === 'top' ? -1 : 1,
            padding: position === 'top' ? '0 0 6px 0' : '6px 0 0 0',
        }}>
            {items.map((item) => {
                /** @type {any} */
                const isHidden = canToggle && hiddenSet.has(item.label);
                return (
                    <div
                        key={item.label}
                        data-chart-export-legend-item="true"
                        data-chart-export-label={item.label}
                        data-chart-export-color={isHidden ? theme.border : item.color}
                        data-chart-export-hidden={isHidden ? 'true' : 'false'}
                        onClick={() => {
                            if (canToggle) onToggle(item.label);
                            if (typeof onElementSelect === 'function') onElementSelect('legend', item.label);
                        }}
                        role="button"
                        tabIndex={0}
                        onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); if (canToggle) onToggle(item.label); if (typeof onElementSelect === 'function') onElementSelect('legend', item.label); } }}
                        aria-pressed={canToggle ? !isHidden : undefined}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '6px', fontSize: '11px',
                            color: theme.textSec,
                            cursor: 'pointer',
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

/** @param {any} [values] @param {any} [width] @param {any} [height] @param {any} [padding] @returns {any} */
const buildSparklineGeometry = (values, width, height, padding = 10) => {
    /** @type {any} */
    const numericValues = Array.isArray(values)
        ? values.map((value) => (Number.isFinite(Number(value)) ? Number(value) : null))
        : [];
    /** @type {any} */
    const definedPoints = numericValues
        .map(/** @param {any} [value] @param {any} [index] @returns {any} */ (value, index) => (value === null ? null : { value, index }))
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

    /** @type {any} */
    const minValue = Math.min(...definedPoints.map(/** @param {any} [point] @returns {any} */ (point) => point.value));
    /** @type {any} */
    const maxValue = Math.max(...definedPoints.map(/** @param {any} [point] @returns {any} */ (point) => point.value));
    /** @type {any} */
    const valueRange = Math.max(1, maxValue - minValue);
    /** @type {any} */
    const stepX = definedPoints.length > 1
        ? (width - (padding * 2)) / (definedPoints.length - 1)
        : 0;

    /** @type {any} */
    const points = definedPoints.map(/** @param {any} [point] @param {any} [pointIndex] @returns {any} */ (point, pointIndex) => {
        /** @type {any} */
        const x = padding + (pointIndex * stepX);
        /** @type {any} */
        const normalized = (point.value - minValue) / valueRange;
        /** @type {any} */
        const y = height - padding - (normalized * Math.max(1, height - (padding * 2)));
        return {
            ...point,
            x,
            y,
        };
    });

    /** @type {any} */
    const linePath = points
        .map(/** @param {any} [point] @param {any} [pointIndex] @returns {any} */ (point, pointIndex) => `${pointIndex === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
        .join(' ');
    /** @type {any} */
    const areaPath = points.length > 1
        ? `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(height - padding).toFixed(2)} L ${points[0].x.toFixed(2)} ${(height - padding).toFixed(2)} Z`
        : '';
    /** @type {any} */
    const minPoint = points.find(/** @param {any} [point] @returns {any} */ (point) => point.value === minValue) || points[0];
    /** @type {any} */
    const maxPoint = points.find(/** @param {any} [point] @returns {any} */ (point) => point.value === maxValue) || points[0];
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

/** @param {any} p0 @returns {any} */
export const SparklineBoard = ({
    model,
    theme,
    chartHeight,
    paletteColors,
    valueFormat,
    referenceLines = [],
}) => {
    /** @type {any} */
    const seriesList = Array.isArray(model && model.series) ? model.series : [];
    /** @type {any} */
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
            {seriesList.map(/** @param {any} [series] @param {any} [index] @returns {any} */ (series, index) => {
                /** @type {any} */
                const color = getColorForIndex(index, theme, paletteColors);
                /** @type {any} */
                const geometry = buildSparklineGeometry(series.values || [], sparkWidth, sparkHeight, 10);
                /** @type {any} */
                const previousValue = Array.isArray(series.values) && series.values.length > 1
                    ? Number(series.values[series.values.length - 2])
                    : null;
                const currentValue = geometry.lastValue;
                /** @type {any} */
                const delta = Number.isFinite(previousValue) && Number.isFinite(currentValue)
                    ? currentValue - previousValue
                    : null;
                /** @type {any} */
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
                            {referenceLines.map(/** @param {any} [rl] @param {any} [rli] @returns {any} */ (rl, rli) => {
                                /** @type {any} */
                                const numVal = Number(rl.value);
                                if (!Number.isFinite(numVal) || rl.orient === 'v') return null;
                                if (geometry.minValue === null || geometry.maxValue === null) return null;
                                /** @type {any} */
                                const span = (geometry.maxValue - geometry.minValue) || 1;
                                /** @type {any} */
                                const ry = sparkHeight - 10 - ((numVal - geometry.minValue) / span) * (sparkHeight - 20);
                                if (ry < 0 || ry > sparkHeight) return null;
                                /** @type {any} */
                                const rlColor = rl.color || theme.primary;
                                return (
                                    <g key={`spark-rl-${rli}`}>
                                        <line x1={10} y1={ry} x2={sparkWidth - 10} y2={ry} stroke={rlColor} strokeWidth="1" strokeDasharray="4 2" style={{ pointerEvents: 'none' }} />
                                        {rl.label ? <text x={sparkWidth - 11} y={ry - 2} textAnchor="end" fontSize="8" fontWeight="700" fill={rlColor} style={{ pointerEvents: 'none' }}>{rl.label}</text> : null}
                                    </g>
                                );
                            })}
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

/** @param {any} p0 @returns {any} */
export const SvgChart = ({
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
    echartsRef = null,
    colorPalette = 'default',
    valueFormat = 'auto',
    yAxisTitle = '',
    yAxisMin = '',
    yAxisMax = '',
    referenceLines = [],
    labelAngle = 0,
    chartTitle = '',
    onTitleChange = null,
    tickCount = 5,
    markerShape = 'circle',
    markerSize = 4,
    showGradient = false,
    seriesColors = {},
    showAnimations = false,
    showCalloutLabels = false,
    subtitle = '',
    onReferenceLinesChange = null,
    showTrendline = false,
    trendlineMode = 'linear',
    trendlinePeriod = 3,
    // new props
    legendPosition = 'bottom',
    labelPosition = 'outside',
    decimalPlaces = -1,
    gradientDirection = 'vertical',
    gradientOpacity = 70,
    trendlineColor = '',
    trendlineWidth = 2,
    seriesSort = 'none',
    lineDashStyles = {},
    yAxisMinRight = '',
    yAxisMaxRight = '',
    customPaletteColors = [],
    onElementSelect = null,
}) => {
    /** @type {any} */
    const paletteColors = colorPalette === 'custom' && customPaletteColors.length >= 2
        ? customPaletteColors
        : resolvePalette(colorPalette);
    /** @param {any} [v] @returns {any} */
    const fmt = (v) => {
        if (decimalPlaces >= 0 && typeof v === 'number' && Number.isFinite(v)) {
            /** @type {any} */
            const base = formatChartValue(v, valueFormat);
            // if auto-format produced a compact suffix keep it; otherwise apply fixed decimals
            if (!/[KMB%$]/.test(base) || valueFormat === 'number') {
                return v.toFixed(decimalPlaces);
            }
        }
        return formatChartValue(v, valueFormat);
    };
    // sorted model.series (non-mutating, applies seriesSort)
    /** @type {any} */
    const sortedModel = useMemo(() => {
        if (!model || !Array.isArray(model.series) || seriesSort === 'none') return model;
        return { ...model, series: applySeriesSort(model.series, seriesSort) };
    }, [model, seriesSort]);
    /** @type {any} */
    const effectiveModel = sortedModel || model;
    /** @type {any} */
    const stackedChildBarMode = chartType === 'bar' && barLayout === 'stacked' && Array.isArray(model.stackedGroups) && model.stackedGroups.length > 0;
    /** @type {any} */
    const stackedSeriesBarMode = chartType === 'bar' && barLayout === 'stacked' && !stackedChildBarMode && canStackBarSeries(model);
    /** @type {any} */
    const stackedAreaMode = chartType === 'area' && barLayout === 'stacked' && canStackAreaSeries(model);
    /** @type {any} */
    const horizontalBarMode = chartType === 'bar' && axisMode === 'horizontal';
    /** @type {any} */
    const stacked100Mode = barLayout === 'stacked100';
    /** @type {any} */
    const effectiveLabelAngle = Number.isFinite(Number(labelAngle)) ? Number(labelAngle) : 0;
    const effectiveChartHeight = chartHeight;

    /** @param {any} [name] @param {any} [idx] @returns {any} */
    const resolveSeriesColor = (name, idx) =>
        (seriesColors && seriesColors[name]) || getColorForIndex(idx, theme, paletteColors);

    /** @param {any} [cx] @param {any} [cy] @param {any} [fill] @param {any} [tipAttrs] @param {any} [clickProps] @returns {any} */
    const renderMarker = (cx, cy, fill, tipAttrs, clickProps = {}) => {
        if (markerShape === 'none') return null;
        /** @type {any} */
        const s = markerSize || 4;
        /** @type {any} */
        const sk = theme.surfaceBg || theme.background || '#fff';
        /** @type {Record<string, any>} */
        const base = { fill, stroke: sk, strokeWidth: '1.5', opacity: '0.82', ...tipAttrs, ...clickProps };
        switch (markerShape) {
            case 'square': return <rect x={cx - s} y={cy - s} width={s * 2} height={s * 2} rx="2" {...base} />;
            case 'diamond': return <polygon points={`${cx},${cy - s} ${cx + s},${cy} ${cx},${cy + s} ${cx - s},${cy}`} {...base} />;
            case 'cross': return <path d={`M${cx - s},${cy} L${cx + s},${cy} M${cx},${cy - s} L${cx},${cy + s}`} fill="none" stroke={fill} strokeWidth="2.5" strokeLinecap="round" {...tipAttrs} {...clickProps} />;
            case 'triangle': return <polygon points={`${cx},${cy - s} ${cx + s},${cy + s * 0.85} ${cx - s},${cy + s * 0.85}`} {...base} />;
            default: return <circle cx={cx} cy={cy} r={s} {...base} />;
        }
    };

    /** @type {any} */
    const norm100Totals = useMemo(() => {
        if (!stacked100Mode || !model || !Array.isArray(model.categories)) return null;
        return model.categories.map(/** @param {any} [_] @param {any} [catIndex] @returns {any} */ (_, catIndex) => {
            let total = 0;
            (model.series || []).forEach(/** @param {any} [s] @returns {any} */ (s) => {
                /** @type {any} */
                const v = s.values && s.values[catIndex];
                if (typeof v === 'number' && Number.isFinite(v)) total += Math.abs(v);
            });
            return total || 1;
        });
    }, [stacked100Mode, model]);
    /** @type {any} */
    const hierarchyNodes = Array.isArray(model && model.icicleNodes) ? model.icicleNodes : [];
    /** @type {any} */
    const icicleDepth = Math.max(1, Number(model && model.icicleDepth) || 1);
    /** @type {any} */
    const chartContainerRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [measuredChartWidth, setMeasuredChartWidth] = useState(CHART_WIDTH);
    /** @type {any} */
    const resolvedChartWidth = Math.max(CHART_WIDTH, Number.isFinite(measuredChartWidth) ? Math.floor(measuredChartWidth) : CHART_WIDTH);

    useEffect(() => {
        const element = chartContainerRef.current;
        if (!element) return undefined;
        /** @returns {any} */
        const updateWidth = () => {
            const nextWidth = element.clientWidth;
            if (nextWidth > 0) setMeasuredChartWidth(nextWidth);
        };
        updateWidth();
        if (typeof ResizeObserver === 'undefined') return undefined;
        /** @type {any} */
        const observer = new ResizeObserver(updateWidth);
        observer.observe(element);
        return () => observer.disconnect();
    }, []);

    /** @type {[any, (...args: any[]) => any]} */
    const [hiddenSeries, setHiddenSeries] = useState(new Set());
    /** @type {(...args: any[]) => any} */
    const toggleHiddenSeries = useCallback((label) => {
        setHiddenSeries(/** @param {any} [prev] @returns {any} */ (prev) => {
            /** @type {any} */
            const next = new Set(prev);
            if (next.has(label)) next.delete(label); else next.add(label);
            return next;
        });
    }, []);
    /** @type {[any, (...args: any[]) => any]} */
    const [tooltipInfo, setTooltipInfo] = useState(null);
    /** @type {(...args: any[]) => any} */
    const handleChartMouseMove = useCallback((e) => {
        const target = e.target;
        /** @type {any} */
        const category = target.getAttribute('data-tip-cat');
        if (!category) {
            setTooltipInfo(/** @param {any} [prev] @returns {any} */ (prev) => prev ? null : prev);
            return;
        }
        const container = e.currentTarget;
        /** @type {any} */
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
    /** @type {(...args: any[]) => any} */
    const handleChartMouseLeave = useCallback(() => setTooltipInfo(null), []);

    /** @type {(...args: any[]) => any} */
    const handleElementClick = useCallback((e) => {
        if (!onElementSelect) return;
        let el = e.target;
        while (el && el !== e.currentTarget) {
            /** @type {any} */
            const elType = el.getAttribute && el.getAttribute('data-chart-element-type');
            if (elType) {
                onElementSelect(elType, el.getAttribute('data-chart-element-id') || '');
                return;
            }
            el = el.parentElement;
        }
    }, [onElementSelect]);

    /** @type {any} */
    const tooltipElement = tooltipInfo ? (
        <div style={{
            position: 'absolute',
            left: tooltipInfo.x + 14 + 160 > resolvedChartWidth ? Math.max(0, tooltipInfo.x - 174) : tooltipInfo.x + 14,
            top: tooltipInfo.y - 10 < 0 ? tooltipInfo.y + 14 : tooltipInfo.y - 10,
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
    /** @type {[any, (...args: any[]) => any]} */
    const [zoomScale, setZoomScale] = useState(1);
    /** @type {[any, (...args: any[]) => any]} */
    const [panOffset, setPanOffset] = useState({ x: 0, y: 0 });
    /** @type {any} */
    const panDrag = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [crosshairPos, setCrosshairPos] = useState(null);

    /** @type {(...args: any[]) => any} */
    const handleChartWheel = useCallback((e) => {
        e.preventDefault();
        setZoomScale(/** @param {any} [prev] @returns {any} */ (prev) => {
            /** @type {any} */
            const next = e.deltaY < 0 ? prev * 1.15 : prev / 1.15;
            /** @type {any} */
            const clamped = Math.min(10, Math.max(1, next));
            if (clamped === 1) setPanOffset({ x: 0, y: 0 });
            return clamped;
        });
    }, []);
    /** @type {(...args: any[]) => any} */
    const handlePanStart = useCallback((e) => {
        if (zoomScale <= 1 || e.button !== 0) return;
        panDrag.current = { startX: e.clientX, startY: e.clientY, startPan: { ...panOffset } };
    }, [zoomScale, panOffset]);
    /** @type {(...args: any[]) => any} */
    const handlePanMove = useCallback((e) => {
        if (!panDrag.current) return;
        /** @type {any} */
        const dx = e.clientX - panDrag.current.startX;
        /** @type {any} */
        const dy = e.clientY - panDrag.current.startY;
        setPanOffset({ x: panDrag.current.startPan.x + dx, y: panDrag.current.startPan.y + dy });
    }, []);
    /** @type {(...args: any[]) => any} */
    const handlePanEnd = useCallback(() => { panDrag.current = null; }, []);
    /** @type {(...args: any[]) => any} */
    const handleDblClick = useCallback(() => { setZoomScale(1); setPanOffset({ x: 0, y: 0 }); }, []);

    /** @type {(...args: any[]) => any} */
    const wrappedMouseMove = useCallback((e) => {
        handleChartMouseMove(e);
        handlePanMove(e);
        /** @type {any} */
        const svgEl = svgRef && svgRef.current;
        if (svgEl && typeof svgEl.getScreenCTM === 'function') {
            /** @type {any} */
            const ctm = svgEl.getScreenCTM();
            if (ctm) {
                /** @type {any} */
                const pt = svgEl.createSVGPoint();
                pt.x = e.clientX; pt.y = e.clientY;
                /** @type {any} */
                const svgPt = pt.matrixTransform(ctm.inverse());
                setCrosshairPos({ x: svgPt.x, y: svgPt.y });
            }
        }
    }, [handleChartMouseMove, handlePanMove]);
    /** @type {(...args: any[]) => any} */
    const wrappedMouseLeave = useCallback(() => {
        handleChartMouseLeave();
        handlePanEnd();
        setCrosshairPos(null);
    }, [handleChartMouseLeave, handlePanEnd]);

    /** @type {Record<string, any>} */
    const chartContainerEvents = {
        onMouseMove: wrappedMouseMove,
        onMouseLeave: wrappedMouseLeave,
        onWheel: handleChartWheel,
        onMouseDown: handlePanStart,
        onMouseUp: handlePanEnd,
        onDoubleClick: handleDblClick,
    };

    /** @type {any} */
    const zoomTransform = zoomScale > 1 ? `scale(${zoomScale}) translate(${panOffset.x / zoomScale}px, ${panOffset.y / zoomScale}px)` : undefined;
    /** @type {any} */
    const zoomContainerStyle = zoomScale > 1 ? { overflow: 'hidden', cursor: 'grab' } : undefined;
    /** @type {any} */
    const zoomSvgStyle = zoomScale > 1 ? { transform: zoomTransform, transformOrigin: 'center center' } : undefined;
    /** @type {any} */
    const crosshairElement = crosshairPos && chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' ? (
        <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none', zIndex: 5 }} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} preserveAspectRatio="xMidYMid meet">
            <line x1={crosshairPos.x} y1={0} x2={crosshairPos.x} y2={effectiveChartHeight} stroke={theme.textSec} strokeOpacity="0.3" strokeDasharray="4 3" />
            <line x1={0} y1={crosshairPos.y} x2={resolvedChartWidth} y2={crosshairPos.y} stroke={theme.textSec} strokeOpacity="0.3" strokeDasharray="4 3" />
        </svg>
    ) : null;
    /** @type {any} */
    const zoomBadge = zoomScale > 1.05 ? (
        <div style={{ position: 'absolute', top: 4, right: 4, zIndex: 12, background: theme.surfaceBg || '#fff', border: `1px solid ${theme.border}`, borderRadius: theme.radiusSm || '8px', padding: '2px 7px', fontSize: '10px', fontWeight: 700, color: theme.textSec, pointerEvents: 'auto', cursor: 'pointer' }} onClick={handleDblClick} title="Reset zoom (or double-click chart)">
            {Math.round(zoomScale * 100)}% ✕
        </div>
    ) : null;
    /** @type {[any, (...args: any[]) => any]} */
    const [editingTitle, setEditingTitle] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [editTitleDraft, setEditTitleDraft] = useState('');
    /** @type {any} */
    const titleInputRef = useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [titlePos, setTitlePos] = useState(null);
    /** @type {any} */
    const titleDragRef = useRef(null);

    useEffect(() => {
        /** @param {any} [event] @returns {any} */
        const handleDragMove = (event) => {
            if (!titleDragRef.current) return;
            const { startX, startY, startPos, containerRect } = titleDragRef.current;
            /** @type {any} */
            const dx = event.clientX - startX;
            /** @type {any} */
            const dy = event.clientY - startY;
            /** @type {any} */
            const nextX = Math.max(0, Math.min(containerRect.width - 40, startPos.x + dx));
            /** @type {any} */
            const nextY = Math.max(0, Math.min(containerRect.height - 20, startPos.y + dy));
            setTitlePos({ x: nextX, y: nextY });
        };
        /** @returns {any} */
        const handleDragEnd = () => { titleDragRef.current = null; };
        window.addEventListener('mousemove', handleDragMove);
        window.addEventListener('mouseup', handleDragEnd);
        return () => {
            window.removeEventListener('mousemove', handleDragMove);
            window.removeEventListener('mouseup', handleDragEnd);
        };
    }, []);

    /** @param {any} [event] @returns {any} */
    const handleTitleDragStart = (event) => {
        if (editingTitle) return;
        event.preventDefault();
        const container = event.currentTarget.parentElement;
        if (!container) return;
        /** @type {any} */
        const containerRect = container.getBoundingClientRect();
        /** @type {any} */
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

    /** @returns {any} */
    const handleTitleDoubleClick = () => {
        if (typeof onTitleChange !== 'function') return;
        setEditTitleDraft(chartTitle || '');
        setEditingTitle(true);
    };
    /** @returns {any} */
    const commitTitleEdit = () => {
        if (typeof onTitleChange === 'function') onTitleChange(editTitleDraft);
        setEditingTitle(false);
    };

    useEffect(() => {
        if (editingTitle && titleInputRef.current) titleInputRef.current.focus();
    }, [editingTitle]);

    /** @type {any} */
    const titleStyle = titlePos
        ? { left: `${titlePos.x}px`, top: `${titlePos.y}px` }
        : { left: '50%', top: '6px', transform: 'translateX(-50%)' };

    /** @type {any} */
    const chartTitleOverlay = chartTitle || editingTitle ? (
        <div
            onMouseDown={handleTitleDragStart}
            onDoubleClick={handleTitleDoubleClick}
            onClick={onElementSelect && !editingTitle ? () => onElementSelect('title', '') : undefined}
            data-chart-element-type="title"
            style={{
                position: 'absolute',
                ...titleStyle,
                zIndex: 6,
                cursor: editingTitle ? 'text' : (onElementSelect ? 'pointer' : 'grab'),
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

    /** @type {any} */
    const geometry = useMemo(() => {
        if (chartType === 'heatmap' || chartType === 'icicle' || chartType === 'sunburst' || chartType === 'sankey' || chartType === 'pie' || chartType === 'donut' || chartType === 'scatter' || chartType === 'waterfall' || chartType === 'combo' || chartType === 'sparkline' || chartType === 'bubble' || chartType === 'radar' || chartType === 'funnel' || chartType === 'histogram' || ECHARTS_CHART_TYPES.has(chartType)) return null;
        /** @type {any} */
        const flatValues = stackedChildBarMode
            ? (model.stackedGroups || []).reduce(/** @param {any} [acc] @param {any} [group] @returns {any} */ (acc, group) => {
                let positiveTotal = 0;
                let negativeTotal = 0;
                (group.segments || []).forEach(/** @param {any} [segment] @returns {any} */ (segment) => {
                    /** @type {any} */
                    const value = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                    if (value === null) return;
                    if (value >= 0) positiveTotal += value;
                    else negativeTotal += value;
                });
                acc.push(positiveTotal, negativeTotal);
                return acc;
            }, [])
            : stackedSeriesBarMode
                ? (model.categories || []).reduce(/** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach(/** @param {any} [series] @returns {any} */ (series) => {
                        /** @type {any} */
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : stackedAreaMode
                ? model.categories.reduce(/** @param {any} [acc] @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach(/** @param {any} [series] @returns {any} */ (series) => {
                        /** @type {any} */
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : (model.series || []).reduce(/** @param {any} [acc] @param {any} [series] @returns {any} */ (acc, series) => {
                (series.values || []).forEach(/** @param {any} [value] @returns {any} */ (value) => {
                    if (typeof value === 'number' && Number.isFinite(value)) acc.push(value);
                });
                return acc;
            }, []);

        if (flatValues.length === 0) return null;

        /** @type {any} */
        const hasHierarchicalBands = !stackedChildBarMode && chartType === 'bar'
            && Array.isArray(model.categoryBands)
            && model.categoryBands.length === model.categories.length
            && model.categoryBands.some(/** @param {any} [band] @returns {any} */ (band) => band && band.outerLabel);
        /** @type {any} */
        let minValue = Math.min(...flatValues, 0);
        /** @type {any} */
        let maxValue = Math.max(...flatValues, 0);
        if (stacked100Mode) {
            minValue = 0;
            maxValue = 100;
        } else {
            if (minValue === maxValue) {
                /** @type {any} */
                const padding = Math.abs(maxValue || 1) * 0.15 || 1;
                minValue -= padding;
                maxValue += padding;
            }
            if (Number.isFinite(Number(yAxisMin)) && yAxisMin !== '') minValue = Number(yAxisMin);
            if (Number.isFinite(Number(yAxisMax)) && yAxisMax !== '') maxValue = Number(yAxisMax);
            if (minValue >= maxValue) maxValue = minValue + 1;
        }

        /** @type {any} */
        const tickLabels = niceTickValues(minValue, maxValue, 5).map(/** @param {any} [v] @returns {any} */ (v) => (stacked100Mode ? `${Math.round(v)}%` : fmt(v)));
        /** @type {any} */
        const maxTickLabelWidth = tickLabels.reduce(/** @param {any} [maxWidth] @param {any} [label] @returns {any} */ (maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);

        /** @type {any} */
        const horizontalInnerLabels = stackedChildBarMode
            ? (model.stackedGroups || []).map(/** @param {any} [group] @returns {any} */ (group) => truncateChartLabel(group && group.label, 22))
            : (
                hasHierarchicalBands
                    ? (model.categoryBands || []).map(/** @param {any} [band] @returns {any} */ (band) => truncateChartLabel(band && band.innerLabel, 18))
                    : (model.categories || []).map(/** @param {any} [category] @returns {any} */ (category) => truncateChartLabel(category, 22))
            );
        /** @type {any} */
        const horizontalOuterLabels = hasHierarchicalBands
            ? (model.categoryBands || []).map(/** @param {any} [band] @returns {any} */ (band) => truncateChartLabel(band && band.outerLabel, 18))
            : [];
        /** @type {any} */
        const maxInnerLabelWidth = horizontalInnerLabels.reduce(/** @param {any} [maxWidth] @param {any} [label] @returns {any} */ (maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);
        /** @type {any} */
        const maxOuterLabelWidth = horizontalOuterLabels.reduce(/** @param {any} [maxWidth] @param {any} [label] @returns {any} */ (maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label, 11)), 0);

        /** @type {any} */
        const leftMargin = horizontalBarMode
            ? Math.max(
                hasHierarchicalBands ? 176 : 132,
                hasHierarchicalBands
                    ? maxOuterLabelWidth + maxInnerLabelWidth + 38
                    : maxInnerLabelWidth + 22
            )
            : Math.max(66, maxTickLabelWidth + 18);

        /** @type {any} */
        const labelAngleBottom = effectiveLabelAngle === 0 ? 40
            : effectiveLabelAngle <= 30 ? 62
            : effectiveLabelAngle <= 45 ? 80
            : effectiveLabelAngle <= 60 ? 98
            : 118;
        /** @type {any} */
        const margin = horizontalBarMode
            ? { top: 24, right: 18, bottom: 42, left: leftMargin }
            : { top: 24, right: 18, bottom: hasHierarchicalBands ? 118 : labelAngleBottom, left: leftMargin };
        /** @type {any} */
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        /** @type {any} */
        const baseline = chartType === 'bar' ? 0 : Math.min(Math.max(0, minValue), maxValue);
        /** @type {any} */
        const safeSpan = maxValue - minValue || 1;
        /** @type {any} */
        const scaleY = horizontalBarMode ? null : (/** @param {any} [value] @returns {any} */ (value) => margin.top + ((maxValue - value) / safeSpan) * plotHeight);
        /** @type {any} */
        const scaleX = horizontalBarMode
            ? (/** @param {any} [value] @returns {any} */ (value) => margin.left + ((value - minValue) / safeSpan) * plotWidth)
            : null;
        /** @type {any} */
        const baselineY = scaleY ? scaleY(baseline) : null;
        /** @type {any} */
        const baselineX = scaleX ? scaleX(baseline) : null;
        /** @type {any} */
        const categoryCount = Math.max(model.categories.length, 1);
        /** @type {any} */
        const step = (horizontalBarMode ? plotHeight : plotWidth) / categoryCount;
        /** @type {any} */
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
    }, [axisMode, model, chartType, stackedAreaMode, stackedChildBarMode, stackedSeriesBarMode, horizontalBarMode, effectiveChartHeight, resolvedChartWidth, stacked100Mode, yAxisMin, yAxisMax, effectiveLabelAngle, fmt]);

    /** @type {any} */
    const categoryAxisLabelPlan = useMemo(() => {
        if (!geometry || geometry.horizontalBarMode || geometry.hasHierarchicalBands) {
            return { visibleIndexes: null, maxChars: 20, stride: 1 };
        }
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        if (categories.length <= 1) return { visibleIndexes: null, maxChars: 20, stride: 1 };

        /** @type {any} */
        const maxChars = geometry.step < 42 ? 12 : (geometry.step < 70 ? 16 : 20);
        /** @type {any} */
        const widestLabel = categories.reduce(/** @param {any} [width] @param {any} [category] @returns {any} */ (width, category) => (
            Math.max(width, estimateTextWidth(truncateChartLabel(category, maxChars), 11))
        ), 0);
        /** @type {any} */
        const radians = Math.abs(effectiveLabelAngle) * (Math.PI / 180);
        /** @type {any} */
        const projectedWidth = effectiveLabelAngle === 0
            ? widestLabel
            : (Math.abs(Math.cos(radians)) * widestLabel) + (Math.abs(Math.sin(radians)) * 12);
        /** @type {any} */
        const stride = Math.max(1, Math.ceil((projectedWidth + 12) / Math.max(geometry.step, 1)));
        if (stride === 1) return { visibleIndexes: null, maxChars, stride };

        /** @type {any} */
        const visibleIndexes = new Set();
        for (let index = 0; index < categories.length; index += stride) visibleIndexes.add(index);
        /** @type {any} */
        const finalIndex = categories.length - 1;
        /** @type {any} */
        const lastRegularIndex = Math.floor(finalIndex / stride) * stride;
        if (finalIndex - lastRegularIndex >= Math.ceil(stride * 0.6)) visibleIndexes.add(finalIndex);
        return { visibleIndexes, maxChars, stride };
    }, [effectiveLabelAngle, geometry, model]);

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
                    referenceLines={referenceLines}
                />
                {chartTitleOverlay}
            </div>
        );
    }

    if (chartType === 'icicle') {
        if (hierarchyNodes.length === 0) {
            return <EmptyChartState message="Icicle charts need hierarchical rows and a numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        /** @type {any} */
        const bandHeight = effectiveChartHeight / icicleDepth;
        /** @type {any} */
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
                        {layoutNodes.map(/** @param {any} [node] @param {any} [index] @returns {any} */ (node, index) => {
                            /** @type {any} */
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme, paletteColors);
                            /** @type {any} */
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

        /** @type {any} */
        const radius = Math.min(resolvedChartWidth, effectiveChartHeight) / 2 - 18;
        /** @type {any} */
        const centerX = resolvedChartWidth / 2;
        /** @type {any} */
        const centerY = effectiveChartHeight / 2;
        /** @type {any} */
        const ringWidth = radius / Math.max(icicleDepth, 1);
        /** @type {any} */
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
                        {layoutNodes.map(/** @param {any} [node] @param {any} [index] @returns {any} */ (node, index) => {
                            /** @type {any} */
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme, paletteColors);
                            /** @type {any} */
                            const path = describeDonutArc(centerX, centerY, node.innerRadius, node.outerRadius, node.startAngle, node.endAngle);
                            /** @type {any} */
                            const midAngle = node.startAngle + ((node.endAngle - node.startAngle) / 2);
                            /** @type {any} */
                            const labelRadius = node.innerRadius + ((node.outerRadius - node.innerRadius) / 2);
                            /** @type {any} */
                            const labelPos = polarToCartesian(centerX, centerY, labelRadius, midAngle);
                            /** @type {any} */
                            const span = node.endAngle - node.startAngle;
                            /** @type {any} */
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
        /** @type {any} */
        const sankeyNodes = Array.isArray(model && model.sankeyNodes) ? model.sankeyNodes : [];
        /** @type {any} */
        const sankeyLinks = Array.isArray(model && model.sankeyLinks) ? model.sankeyLinks : [];
        if (sankeyNodes.length === 0 || sankeyLinks.length === 0) {
            return <EmptyChartState message="Sankey charts need expanded hierarchical rows with child flows." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        /** @type {any} */
        const sankeyLayout = layoutSankey(sankeyNodes, sankeyLinks, resolvedChartWidth, effectiveChartHeight);
        /** @type {any} */
        const sankeyMaxNodeX = sankeyLayout.nodes.length > 0 ? Math.max(...sankeyLayout.nodes.map(/** @param {any} [n] @returns {any} */ (n) => n.x)) : 0;

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
                            overflow: 'visible',
                        }}
                        role="img"
                        aria-label={model.title || 'Sankey chart'}
                    >
                        {sankeyLayout.links.map(/** @param {any} [link] @param {any} [index] @returns {any} */ (link, index) => {
                            /** @type {any} */
                            const sourceX = link.sourceNode.x + link.sourceNode.width;
                            const targetX = link.targetNode.x;
                            /** @type {any} */
                            const midX = sourceX + ((targetX - sourceX) / 2);
                            /** @type {any} */
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
                        {sankeyLayout.nodes.map(/** @param {any} [node] @param {any} [index] @returns {any} */ (node, index) => {
                            /** @type {any} */
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
                                        x={node.x === sankeyMaxNodeX ? node.x - 6 : node.x + node.width + 6}
                                        y={node.y + Math.min(node.height / 2 + 4, node.height - 4)}
                                        textAnchor={node.x === sankeyMaxNodeX ? 'end' : 'start'}
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
        /** @type {any} */
        const pieSeries = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        /** @type {any} */
        const pieCategories = Array.isArray(model && model.categories) ? model.categories : [];
        if (!pieSeries || pieCategories.length === 0) {
            return <EmptyChartState message="Pie charts need categories and at least one numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const rawSlices = pieCategories.map(/** @param {any} [category] @param {any} [index] @returns {any} */ (category, index) => {
            const rawValue = pieSeries.values[index];
            /** @type {any} */
            const value = typeof rawValue === 'number' && Number.isFinite(rawValue) ? rawValue : 0;
            return { category, value: Math.abs(value), originalValue: value, index };
        }).filter(/** @param {any} [slice] @returns {any} */ (slice) => slice.value > 0 && !hiddenSeries.has(slice.category));
        /** @type {any} */
        const totalValue = rawSlices.reduce(/** @param {any} [sum] @param {any} [slice] @returns {any} */ (sum, slice) => sum + slice.value, 0);
        if (totalValue === 0) {
            return <EmptyChartState message="All values are zero — nothing to display." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const isDonut = chartType === 'donut';
        /** @type {any} */
        const radius = Math.min(resolvedChartWidth, effectiveChartHeight) / 2 - 24;
        /** @type {any} */
        const innerRadius = isDonut ? radius * 0.52 : 0;
        /** @type {any} */
        const centerX = resolvedChartWidth / 2;
        /** @type {any} */
        const centerY = effectiveChartHeight / 2;
        /** @type {any} */
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        /** @type {any} */
        let angleCursor = -Math.PI / 2;
        /** @type {any} */
        const sliceData = rawSlices.map(/** @param {any} [slice] @returns {any} */ (slice) => {
            /** @type {any} */
            const fraction = slice.value / totalValue;
            /** @type {any} */
            const sweep = fraction * Math.PI * 2;
            const startAngle = angleCursor;
            /** @type {any} */
            const endAngle = angleCursor + sweep;
            angleCursor = endAngle;
            return { ...slice, startAngle, endAngle, fraction };
        });
        /** @type {any} */
        const legendItems = sliceData.map(/** @param {any} [slice] @returns {any} */ (slice) => ({
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
                        {sliceData.map(/** @param {any} [slice] @returns {any} */ (slice) => {
                            /** @type {any} */
                            const fill = getColorForIndex(slice.index, theme, paletteColors);
                            /** @type {any} */
                            const path = describeDonutArc(centerX, centerY, innerRadius, radius, slice.startAngle, slice.endAngle);
                            /** @type {any} */
                            const midAngle = slice.startAngle + ((slice.endAngle - slice.startAngle) / 2);
                            /** @type {any} */
                            const labelRadius = isDonut
                                ? innerRadius + ((radius - innerRadius) / 2)
                                : radius * 0.62;
                            /** @type {any} */
                            const labelPos = polarToCartesian(centerX, centerY, labelRadius, midAngle);
                            /** @type {any} */
                            const canShowLabel = (slice.endAngle - slice.startAngle) > 0.3;
                            /** @type {any} */
                            const outerLabelPos = polarToCartesian(centerX, centerY, radius + (showCalloutLabels ? 26 : 14), midAngle);
                            /** @type {any} */
                            const canShowOuterLabel = (slice.endAngle - slice.startAngle) > 0.15;
                            /** @type {any} */
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
                                    {(showDataLabels || showCalloutLabels) && canShowOuterLabel ? (
                                        <g>
                                            {showCalloutLabels ? (
                                                <>
                                                    <line
                                                        x1={polarToCartesian(centerX, centerY, radius * 0.97, midAngle).x}
                                                        y1={polarToCartesian(centerX, centerY, radius * 0.97, midAngle).y}
                                                        x2={outerLabelPos.x}
                                                        y2={outerLabelPos.y}
                                                        stroke={fill}
                                                        strokeWidth="1"
                                                        style={{ pointerEvents: 'none' }}
                                                    />
                                                    <text
                                                        x={midAngle > Math.PI / 2 && midAngle < (3 * Math.PI / 2) ? outerLabelPos.x - 4 : outerLabelPos.x + 4}
                                                        y={outerLabelPos.y + 4}
                                                        textAnchor={midAngle > Math.PI / 2 && midAngle < (3 * Math.PI / 2) ? 'end' : 'start'}
                                                        fontSize="9"
                                                        fontWeight="700"
                                                        fill={theme.textSec}
                                                        style={{ pointerEvents: 'none' }}
                                                    >
                                                        {truncateChartLabel(slice.category, 14)}
                                                    </text>
                                                </>
                                            ) : null}
                                            {showDataLabels ? (
                                                <text
                                                    x={outerLabelPos.x}
                                                    y={showCalloutLabels ? outerLabelPos.y + 14 : outerLabelPos.y}
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
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'bubble') {
        /** @type {any} */
        const allSeries = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        if (allSeries.length < 2) {
            return <EmptyChartState message="Bubble charts need at least two measures (X and Y). A third measure sets bubble size." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const xSeries = allSeries[0];
        const ySeries = allSeries[1];
        /** @type {any} */
        const sizeSeries = allSeries[2] || null;
        /** @type {any} */
        const xValues = (xSeries.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v));
        /** @type {any} */
        const yValues = (ySeries.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v));
        if (xValues.length === 0 || yValues.length === 0) {
            return <EmptyChartState message="Not enough numeric data for a bubble chart." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        let minX = Math.min(...xValues), maxX = Math.max(...xValues);
        let minY = Math.min(...yValues), maxY = Math.max(...yValues);
        if (minX === maxX) { /** @type {any} */ const p = Math.abs(maxX || 1) * 0.15 || 1; minX -= p; maxX += p; }
        if (minY === maxY) { /** @type {any} */ const p = Math.abs(maxY || 1) * 0.15 || 1; minY -= p; maxY += p; }
        /** @type {any} */
        const xPad = (maxX - minX) * 0.08; /** @type {any} */ const yPad = (maxY - minY) * 0.08;
        minX -= xPad; maxX += xPad; minY -= yPad; maxY += yPad;
        /** @type {any} */
        const sizeValues = sizeSeries ? (sizeSeries.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v)) : [];
        /** @type {any} */
        const minSize = sizeValues.length ? Math.min(...sizeValues) : 0;
        /** @type {any} */
        const maxSize = sizeValues.length ? Math.max(...sizeValues) : 1;
        /** @type {any} */
        const sizeSpan = maxSize - minSize || 1;
        /** @param {any} [i] @returns {any} */
        const resolveRadius = (i) => {
            if (!sizeSeries) return 7;
            /** @type {any} */
            const sv = sizeSeries.values && sizeSeries.values[i];
            if (typeof sv !== 'number' || !Number.isFinite(sv)) return 7;
            return 4 + ((sv - minSize) / sizeSpan) * 20;
        };
        /** @type {any} */
        const leftMargin = Math.max(52, estimateTextWidth(fmt(maxY), 11) + 16);
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: 20, bottom: 64, left: leftMargin };
        /** @type {any} */
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        /** @param {any} [v] @returns {any} */
        const scaleX = v => margin.left + ((v - minX) / (maxX - minX)) * plotWidth;
        /** @param {any} [v] @returns {any} */
        const scaleY = v => margin.top + ((maxY - v) / (maxY - minY)) * plotHeight;
        /** @type {any} */
        const xTicks = niceTickValues(minX, maxX, 5);
        /** @type {any} */
        const yTicks = niceTickValues(minY, maxY, 5);
        /** @type {any} */
        const color = resolveSeriesColor(ySeries.name, 0);
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg ref={svgRef} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: 'block', overflow: 'visible', ...zoomSvgStyle }}>
                        {yTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
                            <g key={`y-${i}`}>
                                <line x1={margin.left} y1={scaleY(tick)} x2={resolvedChartWidth - margin.right} y2={scaleY(tick)} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={margin.left - 8} y={scaleY(tick) + 4} textAnchor="end" fontSize="11" fill={theme.textSec}>{fmt(tick)}</text>
                            </g>
                        ))}
                        {xTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
                            <g key={`x-${i}`}>
                                <line x1={scaleX(tick)} y1={margin.top} x2={scaleX(tick)} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={scaleX(tick)} y={effectiveChartHeight - margin.bottom + 18} textAnchor="middle" fontSize="11" fill={theme.textSec}>{fmt(tick)}</text>
                            </g>
                        ))}
                        <line x1={margin.left} y1={effectiveChartHeight - margin.bottom} x2={resolvedChartWidth - margin.right} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} />
                        <line x1={margin.left} y1={margin.top} x2={margin.left} y2={effectiveChartHeight - margin.bottom} stroke={theme.border} />
                        <text x={resolvedChartWidth / 2} y={effectiveChartHeight - 8} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec}>{xSeries.name}</text>
                        {yAxisTitle ? <text x={14} y={(margin.top + effectiveChartHeight - margin.bottom) / 2} textAnchor="middle" fontSize="11" fontWeight="700" fill={theme.textSec} transform={`rotate(-90, 14, ${(margin.top + effectiveChartHeight - margin.bottom) / 2})`}>{yAxisTitle}</text> : null}
                        {(xSeries.values || []).map(/** @param {any} [xVal] @param {any} [i] @returns {any} */ (xVal, i) => {
                            const yVal = ySeries.values[i];
                            if (typeof xVal !== 'number' || !Number.isFinite(xVal) || typeof yVal !== 'number' || !Number.isFinite(yVal)) return null;
                            /** @type {any} */
                            const r = resolveRadius(i);
                            /** @type {any} */
                            const label = categories[i] || `Point ${i + 1}`;
                            /** @type {any} */
                            const sizeLabel = sizeSeries ? ` | ${sizeSeries.name}: ${fmt(sizeSeries.values[i])}` : '';
                            /** @type {Record<string, any>} */
                            const tipAttrs = { 'data-tip-cat': label, 'data-tip-ser': `${ySeries.name} (X:${fmt(xVal)})${sizeLabel}`, 'data-tip-val': fmt(yVal), 'data-tip-color': color };
                            const target = categoryTargets[i];
                            return (
                                <circle
                                    key={`bubble-${i}`}
                                    cx={scaleX(xVal)}
                                    cy={scaleY(yVal)}
                                    r={r}
                                    fill={color}
                                    opacity="0.72"
                                    stroke={theme.surfaceBg || theme.background || '#fff'}
                                    strokeWidth="1.5"
                                    style={typeof onCategoryActivate === 'function' && target ? { cursor: 'pointer' } : undefined}
                                    onClick={typeof onCategoryActivate === 'function' && target ? () => onCategoryActivate(target) : undefined}
                                    {...tipAttrs}
                                />
                            );
                        })}
                        {showDataLabels && (xSeries.values || []).map(/** @param {any} [xVal] @param {any} [i] @returns {any} */ (xVal, i) => {
                            const yVal = ySeries.values[i];
                            if (typeof xVal !== 'number' || !Number.isFinite(xVal) || typeof yVal !== 'number' || !Number.isFinite(yVal)) return null;
                            /** @type {any} */
                            const r = resolveRadius(i);
                            return <text key={`bubble-lbl-${i}`} x={scaleX(xVal)} y={scaleY(yVal) - r - 4} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{categories[i] || fmt(yVal)}</text>;
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={[{ label: `${xSeries.name} vs ${ySeries.name}${sizeSeries ? ` (size: ${sizeSeries.name})` : ''}`, color }]} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} /> : null}
            </div>
        );
    }

    if (chartType === 'radar') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        if (categories.length < 3) {
            return <EmptyChartState message="Radar charts need at least 3 categories (spokes)." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        if (series.length === 0) {
            return <EmptyChartState message="Radar charts need at least one measure series." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const allVals = series.flatMap(/** @param {any} [s] @returns {any} */ s => (s.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v)));
        /** @type {any} */
        const maxVal = allVals.length ? Math.max(...allVals, 0) : 1;
        const padding = 60;
        /** @type {any} */
        const cx = resolvedChartWidth / 2;
        /** @type {any} */
        const cy = effectiveChartHeight / 2;
        /** @type {any} */
        const radius = Math.min(cx, cy) - padding;
        const n = categories.length;
        /** @type {any} */
        const angleStep = (2 * Math.PI) / n;
        /** @param {any} [i] @param {any} [val] @returns {any} */
        const getPoint = (i, val) => {
            /** @type {any} */
            const angle = i * angleStep - Math.PI / 2;
            /** @type {any} */
            const r = radius * (val / (maxVal || 1));
            return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
        };
        /** @param {any} [i] @returns {any} */
        const getLabelPoint = (i) => {
            /** @type {any} */
            const angle = i * angleStep - Math.PI / 2;
            /** @type {any} */
            const r = radius + 18;
            return { x: cx + r * Math.cos(angle), y: cy + r * Math.sin(angle) };
        };
        /** @type {any} */
        const legendItems = series.map(/** @param {any} [s] @param {any} [i] @returns {any} */ (s, i) => ({ label: s.name, color: resolveSeriesColor(s.name, i) }));
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg ref={svgRef} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: 'block', ...zoomSvgStyle }}>
                        {[0.25, 0.5, 0.75, 1.0].map((frac) => {
                            /** @type {any} */
                            const pts = categories.map(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => getPoint(i, maxVal * frac));
                            return <polygon key={`grid-${frac}`} points={pts.map(/** @param {any} [p] @returns {any} */ p => `${p.x},${p.y}`).join(' ')} fill="none" stroke={theme.border} strokeWidth="1" />;
                        })}
                        {categories.map(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => {
                            /** @type {any} */
                            const outer = getPoint(i, maxVal);
                            return <line key={`spoke-${i}`} x1={cx} y1={cy} x2={outer.x} y2={outer.y} stroke={theme.border} strokeWidth="1" />;
                        })}
                        {categories.map(/** @param {any} [cat] @param {any} [i] @returns {any} */ (cat, i) => {
                            /** @type {any} */
                            const lp = getLabelPoint(i);
                            /** @type {any} */
                            const anchor = Math.abs(lp.x - cx) < 10 ? 'middle' : lp.x < cx ? 'end' : 'start';
                            return <text key={`spoke-lbl-${i}`} x={lp.x} y={lp.y + 4} textAnchor={anchor} fontSize="10" fill={theme.textSec}>{truncateChartLabel(cat, 14)}</text>;
                        })}
                        {series.map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => {
                            /** @type {any} */
                            const color = resolveSeriesColor(ser.name, si);
                            /** @type {any} */
                            const pts = categories.map(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => {
                                /** @type {any} */
                                const v = ser.values && ser.values[i];
                                return getPoint(i, typeof v === 'number' && Number.isFinite(v) ? v : 0);
                            });
                            /** @type {any} */
                            const polyPts = pts.map(/** @param {any} [p] @returns {any} */ p => `${p.x},${p.y}`).join(' ');
                            return (
                                <g key={`radar-${si}`}>
                                    <polygon points={polyPts} fill={color} fillOpacity="0.18" stroke={color} strokeWidth="2" style={{ pointerEvents: 'none' }} />
                                    {pts.map(/** @param {any} [p] @param {any} [i] @returns {any} */ (p, i) => {
                                        /** @type {any} */
                                        const v = ser.values && ser.values[i];
                                        /** @type {Record<string, any>} */
                                        const tipAttrs = { 'data-tip-cat': categories[i], 'data-tip-ser': ser.name, 'data-tip-val': fmt(v), 'data-tip-color': color };
                                        return <circle key={`radar-dot-${si}-${i}`} cx={p.x} cy={p.y} r="4" fill={color} stroke={theme.surfaceBg || '#fff'} strokeWidth="1.5" {...tipAttrs} />;
                                    })}
                                </g>
                            );
                        })}
                        {[0.5, 1.0].map(frac => (
                            <text key={`scale-${frac}`} x={cx + 4} y={cy - radius * frac + 4} fontSize="9" fill={theme.textSec}>{fmt(maxVal * frac)}</text>
                        ))}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'funnel') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        if (!series || categories.length === 0) {
            return <EmptyChartState message="Funnel charts need categories and at least one measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const values = categories.map(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => {
            /** @type {any} */
            const v = series.values && series.values[i];
            return typeof v === 'number' && Number.isFinite(v) ? Math.abs(v) : 0;
        });
        /** @type {any} */
        const maxVal = Math.max(...values, 1);
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: 20, bottom: 24, left: 20 };
        /** @type {any} */
        const funnelW = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const funnelH = effectiveChartHeight - margin.top - margin.bottom;
        /** @type {any} */
        const sliceH = funnelH / categories.length;
        /** @type {any} */
        const legendItems = categories.map(/** @param {any} [cat] @param {any} [i] @returns {any} */ (cat, i) => ({ label: cat, color: resolveSeriesColor(cat, i) }));
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg ref={svgRef} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: 'block', ...zoomSvgStyle }}>
                        {categories.map(/** @param {any} [cat] @param {any} [i] @returns {any} */ (cat, i) => {
                            /** @type {any} */
                            const topFrac = values[i] / maxVal;
                            /** @type {any} */
                            const botFrac = i + 1 < categories.length ? values[i + 1] / maxVal : values[i] / maxVal * 0.5;
                            /** @type {any} */
                            const topW = funnelW * topFrac;
                            /** @type {any} */
                            const botW = funnelW * botFrac;
                            /** @type {any} */
                            const fcx = resolvedChartWidth / 2;
                            /** @type {any} */
                            const y = margin.top + i * sliceH;
                            /** @type {any} */
                            const color = resolveSeriesColor(cat, i);
                            /** @type {any} */
                            const points = [
                                `${fcx - topW / 2},${y}`,
                                `${fcx + topW / 2},${y}`,
                                `${fcx + botW / 2},${y + sliceH - 1}`,
                                `${fcx - botW / 2},${y + sliceH - 1}`,
                            ].join(' ');
                            return (
                                <g key={`funnel-${i}`}>
                                    <polygon
                                        points={points}
                                        fill={color}
                                        opacity="0.88"
                                        stroke={theme.surfaceBg || '#fff'}
                                        strokeWidth="1.5"
                                        data-tip-cat={cat}
                                        data-tip-ser={series.name}
                                        data-tip-val={fmt(values[i])}
                                        data-tip-color={color}
                                    />
                                    <text x={fcx} y={y + sliceH / 2 + 4} textAnchor="middle" fontSize="11" fontWeight="700" fill="#fff" style={{ pointerEvents: 'none' }}>
                                        {truncateChartLabel(cat, Math.max(4, Math.floor(topW / 7)))} {showDataLabels ? `\u2014 ${fmt(values[i])}` : ''}
                                    </text>
                                </g>
                            );
                        })}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'histogram') {
        /** @type {any} */
        const series = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        if (!series) {
            return <EmptyChartState message="Histogram needs at least one numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const rawValues = (series.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v));
        if (rawValues.length === 0) {
            return <EmptyChartState message="No numeric values for histogram." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const binCount = Math.max(4, Math.min(20, Math.ceil(1 + Math.log2(rawValues.length))));
        /** @type {any} */
        const minV = Math.min(...rawValues);
        /** @type {any} */
        const maxV = Math.max(...rawValues);
        /** @type {any} */
        const binWidth = (maxV - minV) / binCount || 1;
        /** @type {any} */
        const bins = Array.from({ length: binCount }, (_, i) => ({
            low: minV + i * binWidth,
            high: minV + (i + 1) * binWidth,
            count: 0,
        }));
        rawValues.forEach(/** @param {any} [v] @returns {any} */ v => {
            /** @type {any} */
            const idx = Math.min(Math.floor((v - minV) / binWidth), binCount - 1);
            bins[idx].count++;
        });
        /** @type {any} */
        const maxCount = Math.max(...bins.map(/** @param {any} [b] @returns {any} */ b => b.count), 1);
        /** @type {any} */
        const leftMargin = Math.max(46, estimateTextWidth(String(maxCount), 11) + 16);
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: 16, bottom: 44, left: leftMargin };
        /** @type {any} */
        const plotW = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const plotH = effectiveChartHeight - margin.top - margin.bottom;
        /** @type {any} */
        const barW = plotW / binCount;
        /** @param {any} [v] @returns {any} */
        const scaleY = v => margin.top + ((maxCount - v) / maxCount) * plotH;
        /** @type {any} */
        const color = resolveSeriesColor(series.name, 0);
        /** @type {any} */
        const yTicks = niceTickValues(0, maxCount, tickCount || 5);
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg ref={svgRef} viewBox={`0 0 ${resolvedChartWidth} ${effectiveChartHeight}`} width="100%" preserveAspectRatio="xMidYMid meet" style={{ display: 'block', ...zoomSvgStyle }}>
                        {yTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
                            <g key={`htick-${i}`}>
                                <line x1={margin.left} y1={scaleY(tick)} x2={resolvedChartWidth - margin.right} y2={scaleY(tick)} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={margin.left - 8} y={scaleY(tick) + 4} textAnchor="end" fontSize="11" fill={theme.textSec}>{tick}</text>
                            </g>
                        ))}
                        <line x1={margin.left} y1={margin.top + plotH} x2={resolvedChartWidth - margin.right} y2={margin.top + plotH} stroke={theme.textSec} strokeWidth="1" />
                        {bins.map(/** @param {any} [bin] @param {any} [i] @returns {any} */ (bin, i) => {
                            /** @type {any} */
                            const x = margin.left + i * barW;
                            /** @type {any} */
                            const y = scaleY(bin.count);
                            /** @type {any} */
                            const h = Math.max(1, (margin.top + plotH) - y);
                            return (
                                <g key={`hbar-${i}`}>
                                    <rect x={x} y={y} width={Math.max(1, barW - 1)} height={h} fill={color} opacity="0.88"
                                        data-tip-cat={`${fmt(bin.low)}\u2013${fmt(bin.high)}`}
                                        data-tip-ser="Count"
                                        data-tip-val={String(bin.count)}
                                        data-tip-color={color}
                                    />
                                    {showDataLabels && bin.count > 0 ? <text x={x + barW / 2} y={y - 4} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{bin.count}</text> : null}
                                </g>
                            );
                        })}
                        {bins.filter(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => i % Math.max(1, Math.floor(binCount / 6)) === 0 || i === binCount - 1).map(/** @param {any} [bin] @param {any} [i] @returns {any} */ (bin, i) => (
                            <text key={`hlbl-${i}`} x={margin.left + bins.indexOf(bin) * barW + barW / 2} y={margin.top + plotH + 18} textAnchor="middle" fontSize="10" fill={theme.textSec}>{fmt(bin.low)}</text>
                        ))}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
            </div>
        );
    }

    if (chartType === 'boxplot') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        /** @type {any} */
        const axisLineColor = theme.border || '#ccc';
        /** @type {any} */
        const gridLineColor = theme.gridLine || theme.border || '#eee';
        if (series.length < 5) {
            return <EmptyChartState message="Box Plot needs 5 measures: Min, Q1, Median, Q3, Max." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const boxData = categories.map(/** @param {any} [_] @param {any} [ci] @returns {any} */ (_, ci) => [
            series[0].values[ci] ?? 0,
            series[1].values[ci] ?? 0,
            series[2].values[ci] ?? 0,
            series[3].values[ci] ?? 0,
            series[4].values[ci] ?? 0,
        ]);
        /** @type {any} */
        const color = resolveSeriesColor(series[2].name, 0);
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            tooltip: { trigger: 'axis', /** @param {any} [params] @returns {any} */ formatter: /** @param {any} [params] @returns {any} */ (params) => {
                const p = params[0];
                return `${p.name}<br/>Min: ${fmt(p.data[1])}<br/>Q1: ${fmt(p.data[2])}<br/>Median: ${fmt(p.data[3])}<br/>Q3: ${fmt(p.data[4])}<br/>Max: ${fmt(p.data[5])}`;
            }},
            xAxis: { type: 'category', data: categories.map(/** @param {any} [c] @returns {any} */ c => truncateChartLabel(String(c), 14)), axisLabel: { color: textColor, fontSize: 10 }, axisLine: { lineStyle: { color: axisLineColor } } },
            yAxis: { type: 'value', axisLabel: { color: textColor, fontSize: 10, formatter: fmt }, axisLine: { lineStyle: { color: axisLineColor } }, splitLine: { lineStyle: { color: gridLineColor } }, name: yAxisTitle || '', nameTextStyle: { color: textColor } },
            grid: { left: 60, right: 20, top: 20, bottom: 60 },
            series: [{ type: 'boxplot', data: boxData, itemStyle: { color, borderColor: color } }],
        };
        return (
            <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
            </div>
        );
    }

    if (chartType === 'nightingale') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        if (categories.length === 0 || series.length === 0) {
            return <EmptyChartState message="Nightingale charts need categories and at least one measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const echartsData = categories.map(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => {
            /** @type {any} */
            const value = series[0] && series[0].values ? (series[0].values[ci] ?? 0) : 0;
            return { name: String(cat), value, itemStyle: { color: resolveSeriesColor(String(cat), ci) } };
        });
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            tooltip: { trigger: 'item', /** @param {any} [p] @returns {any} */ formatter: /** @param {any} [p] @returns {any} */ p => `${p.name}: ${fmt(p.value)}` },
            legend: { show: showLegend, textStyle: { color: textColor }, type: 'scroll' },
            series: [{
                type: 'bar',
                coordinateSystem: 'polar',
                data: echartsData,
                label: { show: showDataLabels, position: 'outside', /** @param {any} [p] @returns {any} */ formatter: /** @param {any} [p] @returns {any} */ p => fmt(p.value), color: textColor, fontSize: 10 },
            }],
            polar: { radius: ['10%', '75%'] },
            angleAxis: { type: 'category', data: categories.map(/** @param {any} [c] @returns {any} */ c => truncateChartLabel(String(c), 12)), axisLabel: { color: textColor, fontSize: 9 } },
            radiusAxis: { axisLabel: { color: textColor, fontSize: 9, formatter: fmt } },
        };
        return (
            <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
            </div>
        );
    }

    if (chartType === 'heatmap') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        if (categories.length === 0 || series.length === 0) {
            return <EmptyChartState message="Heatmap needs categories and at least one measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const allVals = series.flatMap(/** @param {any} [s] @returns {any} */ s => (s.values || []).filter(/** @param {any} [v] @returns {any} */ v => typeof v === 'number' && Number.isFinite(v)));
        /** @type {any} */
        const hmMin = allVals.length ? Math.min(...allVals) : 0;
        /** @type {any} */
        const hmMax = allVals.length ? Math.max(...allVals) : 1;
        /** @type {any} */
        const hmSpan = hmMax - hmMin || 1;
        /** @param {any} [value] @returns {any} */
        const heatColor = (value) => {
            /** @type {any} */
            const t = Math.max(0, Math.min(1, (value - hmMin) / hmSpan));
            /** @type {any} */
            const r = Math.round(59 + t * (239 - 59));
            /** @type {any} */
            const g = Math.round(130 - t * (130 - 68));
            /** @type {any} */
            const b = Math.round(246 - t * (246 - 68));
            return `rgb(${r},${g},${b})`;
        };
        const cellPadding = 2;
        /** @type {any} */
        const labelColWidth = Math.min(140, Math.max(60, series.reduce(/** @param {any} [m] @param {any} [s] @returns {any} */ (m, s) => Math.max(m, estimateTextWidth(s.name, 11)), 0) + 10));
        const labelRowHeight = 32;
        const svgW = resolvedChartWidth;
        const svgH = effectiveChartHeight;
        /** @type {any} */
        const gridW = svgW - labelColWidth - 8;
        /** @type {any} */
        const gridH = svgH - labelRowHeight - 8;
        /** @type {any} */
        const cellW = Math.max(20, gridW / categories.length);
        /** @type {any} */
        const cellH = Math.max(20, gridH / series.length);
        /** @type {any} */
        const legendItems = series.map(/** @param {any} [s] @param {any} [i] @returns {any} */ (s, i) => ({ label: s.name, color: getColorForIndex(i, theme, paletteColors) }));
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div ref={chartContainerRef} style={{ width: '100%', minWidth: 0, position: 'relative', ...zoomContainerStyle }} {...chartContainerEvents}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${svgW} ${svgH}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{ width: '100%', height: 'auto', display: 'block', maxWidth: '100%', ...zoomSvgStyle }}
                        role="img"
                        aria-label={model.title || 'Heatmap'}
                    >
                        {categories.map(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => (
                            <text
                                key={`hm-col-${ci}`}
                                x={labelColWidth + ci * cellW + cellW / 2}
                                y={labelRowHeight - 6}
                                textAnchor="middle"
                                fontSize="10"
                                fill={theme.textSec}
                                style={{ pointerEvents: 'none' }}
                            >
                                {truncateChartLabel(cat, Math.max(4, Math.floor(cellW / 6.5)))}
                            </text>
                        ))}
                        {series.map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => (
                            <text
                                key={`hm-row-${si}`}
                                x={labelColWidth - 6}
                                y={labelRowHeight + si * cellH + cellH / 2 + 4}
                                textAnchor="end"
                                fontSize="11"
                                fill={theme.textSec}
                                style={{ pointerEvents: 'none' }}
                            >
                                {truncateChartLabel(ser.name, Math.max(6, Math.floor(labelColWidth / 6.5)))}
                            </text>
                        ))}
                        {series.map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) =>
                            categories.map(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => {
                                /** @type {any} */
                                const rawVal = ser.values && ser.values[ci];
                                if (rawVal === null || rawVal === undefined) return null;
                                /** @type {any} */
                                const cellColor = heatColor(rawVal);
                                /** @type {any} */
                                const cx = labelColWidth + ci * cellW;
                                /** @type {any} */
                                const cy = labelRowHeight + si * cellH;
                                return (
                                    <rect
                                        key={`hm-cell-${si}-${ci}`}
                                        x={cx + cellPadding}
                                        y={cy + cellPadding}
                                        width={Math.max(1, cellW - cellPadding * 2)}
                                        height={Math.max(1, cellH - cellPadding * 2)}
                                        rx="3"
                                        fill={cellColor}
                                        data-tip-cat={cat}
                                        data-tip-ser={ser.name}
                                        data-tip-val={fmt(rawVal)}
                                        data-tip-color={cellColor}
                                    />
                                );
                            })
                        )}
                        {showDataLabels && series.map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) =>
                            categories.map(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => {
                                /** @type {any} */
                                const rawVal = ser.values && ser.values[ci];
                                if (rawVal === null || rawVal === undefined) return null;
                                if (cellW < 36 || cellH < 18) return null;
                                /** @type {any} */
                                const t = Math.max(0, Math.min(1, (rawVal - hmMin) / hmSpan));
                                /** @type {any} */
                                const textFill = t > 0.5 ? '#fff' : theme.text;
                                return (
                                    <text
                                        key={`hm-label-${si}-${ci}`}
                                        x={labelColWidth + ci * cellW + cellW / 2}
                                        y={labelRowHeight + si * cellH + cellH / 2 + 4}
                                        textAnchor="middle"
                                        fontSize="9"
                                        fontWeight="700"
                                        fill={textFill}
                                        style={{ pointerEvents: 'none' }}
                                    >
                                        {fmt(rawVal)}
                                    </text>
                                );
                            })
                        )}
                    </svg>
                    {chartTitleOverlay}{crosshairElement}{tooltipElement}{zoomBadge}
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'bar3d') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        /** @type {any} */
        const axisLineColor = theme.border || '#ccc';
        /** @type {any[]} */
        const data = [];
        series.forEach(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => {
            categories.forEach(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => {
                const val = ser.values[ci];
                if (val !== null && val !== undefined) {
                    data.push([ci, si, val]);
                }
            });
        });
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            legend: {
                show: showLegend,
                data: series.map(/** @param {any} [s] @returns {any} */ s => s.name),
                textStyle: { color: textColor },
                type: 'scroll',
            },
            tooltip: {
                /** @param {any} [params] @returns {any} */
                formatter: (params) => {
                    /** @type {any} */
                    const cat = categories[params.value[0]] || '';
                    /** @type {any} */
                    const ser = series[params.value[1]] ? series[params.value[1]].name : '';
                    return `${cat} / ${ser}: <b>${fmt(params.value[2])}</b>`;
                },
            },
            grid3D: {
                boxWidth: 200,
                boxDepth: 80,
                viewControl: { projection: 'perspective', autoRotate: false },
                light: { main: { intensity: 1.2, shadow: false }, ambient: { intensity: 0.4 } },
            },
            xAxis3D: {
                type: 'category',
                data: categories.map(/** @param {any} [c] @returns {any} */ c => truncateChartLabel(String(c), 12)),
                axisLabel: { color: textColor, fontSize: 10 },
                axisLine: { lineStyle: { color: axisLineColor } },
                name: '',
            },
            yAxis3D: {
                type: 'category',
                data: series.map(/** @param {any} [s] @returns {any} */ s => truncateChartLabel(String(s.name), 12)),
                axisLabel: { color: textColor, fontSize: 10 },
                axisLine: { lineStyle: { color: axisLineColor } },
            },
            zAxis3D: {
                type: 'value',
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                name: yAxisTitle || '',
                nameTextStyle: { color: textColor },
            },
            series: [{
                type: 'bar3D',
                data: data.map(item => ({
                    value: item,
                    itemStyle: { color: getColorForIndex(item[1], theme, paletteColors), opacity: 0.9 },
                })),
                shading: 'lambert',
                label: {
                    show: showDataLabels,
                    /** @param {any} [p] @returns {any} */
                    formatter: p => fmt(p.value[2]),
                    textStyle: { color: textColor, fontSize: 10 },
                },
                emphasis: { label: { show: true }, itemStyle: { opacity: 1 } },
            }],
        };
        return (
            <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
            </div>
        );
    }

    if (chartType === 'line3d') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        /** @type {any} */
        const axisLineColor = theme.border || '#ccc';
        /** @type {any} */
        const echartsSeriesList = series.map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => ({
            type: 'line3D',
            data: categories.map(/** @param {any} [_] @param {any} [ci] @returns {any} */ (_, ci) => {
                const val = ser.values[ci];
                return [ci, si, val !== null && val !== undefined ? val : 0];
            }),
            lineStyle: { color: getColorForIndex(si, theme, paletteColors), width: 3, opacity: 0.9 },
            name: ser.name,
        }));
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            legend: { show: showLegend, data: series.map(/** @param {any} [s] @returns {any} */ s => s.name), textStyle: { color: textColor }, type: 'scroll' },
            tooltip: {
                /** @param {any} [params] @returns {any} */
                formatter: (params) => {
                    /** @type {any} */
                    const cat = categories[params.value[0]] || '';
                    /** @type {any} */
                    const serName = series[params.value[1]] ? series[params.value[1]].name : '';
                    return `${cat} / ${serName}: <b>${fmt(params.value[2])}</b>`;
                },
            },
            grid3D: {
                boxWidth: 200, boxDepth: 80,
                viewControl: { projection: 'perspective', autoRotate: false },
                light: { main: { intensity: 1.2, shadow: false }, ambient: { intensity: 0.4 } },
            },
            xAxis3D: {
                type: 'category',
                data: categories.map(/** @param {any} [c] @returns {any} */ c => truncateChartLabel(String(c), 12)),
                axisLabel: { color: textColor, fontSize: 10 },
                axisLine: { lineStyle: { color: axisLineColor } },
            },
            yAxis3D: {
                type: 'category',
                data: series.map(/** @param {any} [s] @returns {any} */ s => truncateChartLabel(String(s.name), 12)),
                axisLabel: { color: textColor, fontSize: 10 },
                axisLine: { lineStyle: { color: axisLineColor } },
            },
            zAxis3D: {
                type: 'value',
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                name: yAxisTitle || '',
                nameTextStyle: { color: textColor },
            },
            series: echartsSeriesList,
        };
        return (
            <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
            </div>
        );
    }

    if (chartType === 'scatter3d') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        /** @type {any} */
        const axisLineColor = theme.border || '#ccc';
        if (series.length < 2) {
            return <EmptyChartState message="3D Scatter needs at least two numeric measures (X and Y)." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const xSeries = series[0];
        const ySeries = series[1];
        /** @type {any} */
        const zSeries = series[2] || null;
        /** @type {any} */
        const scatterData = categories.map(/** @param {any} [cat] @param {any} [ci] @returns {any} */ (cat, ci) => ({
            value: [xSeries.values[ci] ?? 0, ySeries.values[ci] ?? 0, zSeries ? (zSeries.values[ci] ?? 0) : ci],
            name: String(cat),
        }));
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            tooltip: {
                /** @param {any} [params] @returns {any} */
                formatter: (params) => {
                    /** @type {any} */
                    const cat = categories[params.dataIndex] || '';
                    return `${cat}<br/>X=${fmt(params.value[0])}, Y=${fmt(params.value[1])}, Z=${fmt(params.value[2])}`;
                },
            },
            grid3D: {
                boxWidth: 200, boxDepth: 80,
                viewControl: { projection: 'perspective', autoRotate: false },
                light: { main: { intensity: 1.2, shadow: false }, ambient: { intensity: 0.4 } },
            },
            xAxis3D: {
                type: 'value', name: xSeries.name,
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                nameTextStyle: { color: textColor },
            },
            yAxis3D: {
                type: 'value', name: ySeries.name,
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                nameTextStyle: { color: textColor },
            },
            zAxis3D: {
                type: 'value', name: zSeries ? zSeries.name : 'Index',
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                nameTextStyle: { color: textColor },
            },
            series: [{
                type: 'scatter3D',
                data: scatterData,
                symbolSize: 8,
                itemStyle: { color: getColorForIndex(0, theme, paletteColors), opacity: 0.85 },
                label: {
                    show: showDataLabels,
                    /** @param {any} [params] @returns {any} */
                    formatter: (params) => String(categories[params.dataIndex] || ''),
                    textStyle: { color: textColor, fontSize: 10 },
                },
                emphasis: { itemStyle: { opacity: 1 } },
            }],
        };
        return (
            <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
            </div>
        );
    }

    if (chartType === 'range') {
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const series = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const bg = theme.chartBg || theme.surface || '#fff';
        /** @type {any} */
        const textColor = theme.text || '#333';
        /** @type {any} */
        const axisLineColor = theme.border || '#ccc';
        /** @type {any} */
        const gridLineColor = theme.gridLine || theme.border || '#eee';
        if (series.length < 2) {
            return <EmptyChartState message="Range chart needs at least two measures (low and high values)." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any[]} */
        const pairs = [];
        for (let i = 0; i + 1 < series.length; i += 2) {
            pairs.push({ low: series[i], high: series[i + 1] });
        }
        /** @type {any[]} */
        const echartsSeriesList = [];
        pairs.forEach((pair, pi) => {
            /** @type {any} */
            const color = getColorForIndex(pi, theme, paletteColors);
            echartsSeriesList.push({
                type: 'bar', name: `_base_${pi}`,
                data: categories.map(/** @param {any} [_] @param {any} [ci] @returns {any} */ (_, ci) => Math.min(pair.low.values[ci] ?? 0, pair.high.values[ci] ?? 0)),
                stack: `range_${pi}`,
                itemStyle: { opacity: 0 },
                emphasis: { disabled: true },
                silent: true, legendHoverLink: false,
            });
            echartsSeriesList.push({
                type: 'bar', name: `${pair.low.name} – ${pair.high.name}`,
                data: categories.map(/** @param {any} [_] @param {any} [ci] @returns {any} */ (_, ci) => Math.abs((pair.high.values[ci] ?? 0) - (pair.low.values[ci] ?? 0))),
                stack: `range_${pi}`,
                itemStyle: { color, opacity: 0.9 },
                label: {
                    show: showDataLabels, position: 'inside',
                    /** @param {any} [params] @returns {any} */
                    formatter: (params) => {
                        const ci = params.dataIndex;
                        /** @type {any} */
                        const lo = pair.low.values[ci] ?? 0;
                        /** @type {any} */
                        const hi = pair.high.values[ci] ?? 0;
                        return `${fmt(Math.min(lo, hi))}–${fmt(Math.max(lo, hi))}`;
                    },
                    color: textColor, fontSize: 10,
                },
                tooltip: {
                    /** @param {any} [params] @returns {any} */
                    formatter: (params) => {
                        const ci = params.dataIndex;
                        return `${categories[ci]}<br/>${pair.low.name}: ${fmt(pair.low.values[ci] ?? 0)}<br/>${pair.high.name}: ${fmt(pair.high.values[ci] ?? 0)}`;
                    },
                },
            });
        });
        /** @type {any} */
        const legendItems = pairs.map((p, pi) => ({ label: `${p.low.name} – ${p.high.name}`, color: getColorForIndex(pi, theme, paletteColors) }));
        /** @type {Record<string, any>} */
        const option = {
            backgroundColor: bg,
            tooltip: { trigger: 'axis' },
            xAxis: {
                type: 'category',
                data: categories.map(/** @param {any} [c] @returns {any} */ c => truncateChartLabel(String(c), 14)),
                axisLabel: { color: textColor, fontSize: 10 },
                axisLine: { lineStyle: { color: axisLineColor } },
                splitLine: { show: false },
            },
            yAxis: {
                type: 'value', name: yAxisTitle || '',
                axisLabel: { color: textColor, fontSize: 10, formatter: fmt },
                axisLine: { lineStyle: { color: axisLineColor } },
                splitLine: { lineStyle: { color: gridLineColor } },
                nameTextStyle: { color: textColor },
            },
            grid: { left: 60, right: 20, top: 20, bottom: 60 },
            series: echartsSeriesList,
        };
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ width: '100%', height: chartHeight, background: bg, borderRadius: theme.radiusSm || '8px', overflow: 'hidden' }}>
                    <DeferredEChartsRenderer chartType={chartType} echartsRef={echartsRef} option={option} />
                </div>
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'scatter') {
        /** @type {any} */
        const allSeries = Array.isArray(model && model.series) ? model.series.filter(/** @param {any} [s] @returns {any} */ (s) => !hiddenSeries.has(s.name)) : [];
        /** @type {any} */
        const categories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        if (allSeries.length < 2) {
            return <EmptyChartState message="Scatter charts need at least two numeric measures (X and Y)." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        const xSeries = allSeries[0];
        /** @type {any} */
        const ySeries = allSeries.slice(1);
        /** @type {any} */
        const xValues = (xSeries.values || []).filter(/** @param {any} [v] @returns {any} */ (v) => typeof v === 'number' && Number.isFinite(v));
        /** @type {any} */
        const allYValues = ySeries.flatMap(/** @param {any} [s] @returns {any} */ (s) => (s.values || []).filter(/** @param {any} [v] @returns {any} */ (v) => typeof v === 'number' && Number.isFinite(v)));
        if (xValues.length === 0 || allYValues.length === 0) {
            return <EmptyChartState message="Not enough numeric data for a scatter chart." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        let minX = Math.min(...xValues);
        /** @type {any} */
        let maxX = Math.max(...xValues);
        /** @type {any} */
        let minY = Math.min(...allYValues);
        /** @type {any} */
        let maxY = Math.max(...allYValues);
        if (minX === maxX) { /** @type {any} */ const pad = Math.abs(maxX || 1) * 0.15 || 1; minX -= pad; maxX += pad; }
        if (minY === maxY) { /** @type {any} */ const pad = Math.abs(maxY || 1) * 0.15 || 1; minY -= pad; maxY += pad; }
        /** @type {any} */
        const xPad = (maxX - minX) * 0.06;
        /** @type {any} */
        const yPad = (maxY - minY) * 0.06;
        minX -= xPad; maxX += xPad; minY -= yPad; maxY += yPad;
        /** @type {any} */
        const leftMargin = Math.max(52, estimateTextWidth(fmt(maxY), 11) + 16);
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: 20, bottom: 64, left: leftMargin };
        /** @type {any} */
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        /** @param {any} [v] @returns {any} */
        const scaleX = (v) => margin.left + ((v - minX) / (maxX - minX)) * plotWidth;
        /** @param {any} [v] @returns {any} */
        const scaleY = (v) => margin.top + ((maxY - v) / (maxY - minY)) * plotHeight;
        /** @type {any} */
        const xTicks = niceTickValues(minX, maxX, 5);
        /** @type {any} */
        const yTicks = niceTickValues(minY, maxY, 5);
        /** @type {any} */
        const legendItems = ySeries.map(/** @param {any} [s] @param {any} [i] @returns {any} */ (s, i) => ({ label: s.name, color: resolveSeriesColor(s.name, i) }));
        /** @type {any} */
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
                        {yTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
                            <g key={`y-tick-${i}`}>
                                <line x1={margin.left} y1={scaleY(tick)} x2={resolvedChartWidth - margin.right} y2={scaleY(tick)} stroke={theme.border} strokeDasharray="3 4" />
                                <text x={margin.left - 8} y={scaleY(tick) + 4} textAnchor="end" fontSize="11" fill={theme.textSec} style={{ pointerEvents: 'none' }}>{fmt(tick)}</text>
                            </g>
                        ))}
                        {xTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
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
                        {ySeries.map(/** @param {any} [series] @param {any} [seriesIndex] @returns {any} */ (series, seriesIndex) => {
                            /** @type {any} */
                            const color = resolveSeriesColor(series.name, seriesIndex);
                            return (series.values || []).map(/** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */ (yVal, pointIndex) => {
                                const xVal = xSeries.values[pointIndex];
                                if (typeof xVal !== 'number' || !Number.isFinite(xVal) || typeof yVal !== 'number' || !Number.isFinite(yVal)) return null;
                                /** @type {any} */
                                const scx = scaleX(xVal);
                                /** @type {any} */
                                const scy = scaleY(yVal);
                                /** @type {any} */
                                const label = categories[pointIndex] || `Point ${pointIndex + 1}`;
                                const target = categoryTargets[pointIndex];
                                /** @type {Record<string, any>} */
                                const tipAttrs = {
                                    'data-tip-cat': label,
                                    'data-tip-ser': `${series.name} (${xSeries.name}=${fmt(xVal)})`,
                                    'data-tip-val': fmt(yVal),
                                    'data-tip-color': color,
                                };
                                return React.cloneElement(renderMarker(scx, scy, color, tipAttrs, {
                                    style: canClick && target ? { cursor: 'pointer' } : undefined,
                                    onClick: canClick && target ? () => onCategoryActivate(target) : undefined,
                                }), { key: `scatter-${seriesIndex}-${pointIndex}` });
                            });
                        })}
                        {showDataLabels ? ySeries.map(/** @param {any} [series] @param {any} [seriesIndex] @returns {any} */ (series, seriesIndex) => {
                            /** @type {any} */
                            const color = resolveSeriesColor(series.name, seriesIndex);
                            return (series.values || []).map(/** @param {any} [yVal] @param {any} [pointIndex] @returns {any} */ (yVal, pointIndex) => {
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
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'waterfall') {
        /** @type {any} */
        const wfSeries = Array.isArray(model && model.series) && model.series.length > 0 ? model.series[0] : null;
        /** @type {any} */
        const wfCategories = Array.isArray(model && model.categories) ? model.categories : [];
        /** @type {any} */
        const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
        if (!wfSeries || wfCategories.length === 0) {
            return <EmptyChartState message="Waterfall charts need categories and at least one numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }
        /** @type {any} */
        const increments = wfCategories.map(/** @param {any} [_] @param {any} [i] @returns {any} */ (_, i) => {
            const v = wfSeries.values[i];
            return typeof v === 'number' && Number.isFinite(v) ? v : 0;
        });
        /** @type {any[]} */
        const bars = [];
        let running = 0;
        for (let i = 0; i < increments.length; i++) {
            const start = running;
            running += increments[i];
            bars.push({ label: wfCategories[i], start, end: running, increment: increments[i], type: increments[i] >= 0 ? 'positive' : 'negative' });
        }
        bars.push({ label: 'Total', start: 0, end: running, increment: running, type: 'total' });
        /** @type {any} */
        const allEnds = bars.map((b) => b.start).concat(bars.map((b) => b.end));
        /** @type {any} */
        let minVal = Math.min(...allEnds, 0);
        /** @type {any} */
        let maxVal = Math.max(...allEnds, 0);
        if (minVal === maxVal) { /** @type {any} */ const pad = Math.abs(maxVal || 1) * 0.15 || 1; minVal -= pad; maxVal += pad; }
        /** @type {any} */
        const yPad = (maxVal - minVal) * 0.08;
        minVal -= yPad; maxVal += yPad;
        /** @type {any} */
        const wfTicks = niceTickValues(minVal, maxVal, 5);
        /** @type {any} */
        const leftMargin = Math.max(52, estimateTextWidth(fmt(maxVal), 11) + 16);
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: 20, bottom: 84, left: leftMargin };
        /** @type {any} */
        const plotWidth = resolvedChartWidth - margin.left - margin.right;
        /** @type {any} */
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        /** @param {any} [v] @returns {any} */
        const scaleY = (v) => margin.top + ((maxVal - v) / (maxVal - minVal)) * plotHeight;
        /** @type {any} */
        const step = plotWidth / bars.length;
        /** @type {any} */
        const barWidth = Math.max(14, Math.min(60, step * 0.6));
        const positiveColor = '#059669';
        const negativeColor = '#DC2626';
        /** @type {any} */
        const totalColor = getColorForIndex(0, theme, paletteColors);
        /** @type {any} */
        const canClick = typeof onCategoryActivate === 'function';
        /** @type {any[]} */
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
                        {wfTicks.map(/** @param {any} [tick] @param {any} [i] @returns {any} */ (tick, i) => (
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
                            /** @type {any} */
                            const cx = margin.left + i * step + step / 2;
                            /** @type {any} */
                            const x = cx - barWidth / 2;
                            /** @type {any} */
                            const yTop = scaleY(Math.max(bar.start, bar.end));
                            /** @type {any} */
                            const yBot = scaleY(Math.min(bar.start, bar.end));
                            /** @type {any} */
                            const height = Math.max(1, yBot - yTop);
                            /** @type {any} */
                            const fill = bar.type === 'total' ? totalColor : bar.type === 'positive' ? positiveColor : negativeColor;
                            /** @type {any} */
                            const target = i < categoryTargets.length ? categoryTargets[i] : null;
                            return (
                                <g key={`wf-bar-${i}`}>
                                    {i > 0 ? (
                                        <line
                                            x1={margin.left + (i - 1) * step + step / 2 + barWidth / 2}
                                            y1={i === bars.length - 1 ? scaleY(bar.end) : scaleY(bar.start)}
                                            x2={x}
                                            y2={i === bars.length - 1 ? scaleY(bar.end) : scaleY(bar.start)}
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
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (chartType === 'combo') {
        /** @type {any} */
        const comboLayers = (Array.isArray(model && model.layers) ? model.layers : [])
            .filter(/** @param {any} [layer] @returns {any} */ (layer) => layer && !layer.hidden && !hiddenSeries.has(layer.name) && Array.isArray(layer.values) && layer.values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null));
        if (comboLayers.length === 0) {
            return <EmptyChartState message={model.emptyMessage || 'No combo layers are configured.'} theme={theme} chartHeight={effectiveChartHeight} />;
        }

        /** @type {any} */
        const hasRightAxis = comboLayers.some(/** @param {any} [layer] @returns {any} */ (layer) => layer.axis === 'right');
        /** @type {Record<string, any>} */
        const margin = { top: 24, right: hasRightAxis ? 76 : 20, bottom: 96, left: 72 };
        /** @type {any} */
        const categoryCount = Math.max(1, Array.isArray(model.categories) ? model.categories.length : 0);
        /** @type {any} */
        const step = Math.max(1, (resolvedChartWidth - margin.left - margin.right) / categoryCount);
        /** @type {any} */
        const groupWidth = Math.max(22, Math.min(72, step * 0.72));
        /** @type {any} */
        const barLayers = comboLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.type === 'bar');
        /** @type {any} */
        const areaLayers = comboLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.type === 'area');
        /** @type {any} */
        const lineLayers = comboLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.type === 'line');
        /** @type {any} */
        const leftMetrics = getAxisMetrics(
            comboLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.axis !== 'right').flatMap(/** @param {any} [layer] @returns {any} */ (layer) => layer.values),
            effectiveChartHeight,
            margin
        );
        /** @type {any} */
        const rightMetrics = hasRightAxis
            ? getAxisMetrics(
                comboLayers.filter(/** @param {any} [layer] @returns {any} */ (layer) => layer.axis === 'right').flatMap(/** @param {any} [layer] @returns {any} */ (layer) => layer.values),
                effectiveChartHeight,
                margin
            )
            : leftMetrics;
        /** @type {any} */
        const categoryTargets = Array.isArray(model.categoryTargets) ? model.categoryTargets : [];
        /** @type {any} */
        const legendItems = comboLayers.map(/** @param {any} [layer] @param {any} [index] @returns {any} */ (layer, index) => ({
            label: layer.name,
            color: layer.color || getColorForIndex(index, theme, paletteColors),
        }));
        /** @param {any} [layer] @returns {any} */
        const getLayerScale = (layer) => (layer.axis === 'right' ? rightMetrics.scaleY : leftMetrics.scaleY);
        /** @param {any} [layer] @returns {any} */
        const getLayerBaseline = (layer) => (layer.axis === 'right' ? rightMetrics.baselineY : leftMetrics.baselineY);
        /** @param {any} [layer] @returns {any} */
        const makeLayerFmt = (layer) => /** @param {any} [v] @returns {any} */ (v) => formatChartValue(v, layer.valueFormat || valueFormat || 'auto');
        /** @type {any} */
        const firstRightLayer = comboLayers.find(/** @param {any} [layer] @returns {any} */ (layer) => layer.axis === 'right');
        /** @type {any} */
        const fmtRight = firstRightLayer ? makeLayerFmt(firstRightLayer) : fmt;
        const barGap = 4;
        /** @type {any} */
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
                        {leftMetrics.tickValues.map(/** @param {any} [tickValue] @param {any} [index] @returns {any} */ (tickValue, index) => {
                            /** @type {any} */
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

                        {hasRightAxis ? rightMetrics.tickValues.map(/** @param {any} [tickValue] @param {any} [index] @returns {any} */ (tickValue, index) => {
                            /** @type {any} */
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
                                    {fmtRight(tickValue)}
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

                        {barLayers.map(/** @param {any} [layer] @param {any} [barLayerIndex] @returns {any} */ (layer, barLayerIndex) => (
                            model.categories.map(/** @param {any} [category] @param {any} [categoryIndex] @returns {any} */ (category, categoryIndex) => {
                                const value = layer.values[categoryIndex];
                                if (value === null || value === undefined) return null;
                                /** @type {any} */
                                const slotStart = margin.left + (categoryIndex * step) + ((step - groupWidth) / 2);
                                /** @type {any} */
                                const scaleY = getLayerScale(layer);
                                /** @type {any} */
                                const baselineY = getLayerBaseline(layer);
                                /** @type {any} */
                                const y = scaleY(value);
                                /** @type {any} */
                                const color = layer.color || getColorForIndex(barLayerIndex, theme, paletteColors);
                                /** @type {any} */
                                const barX = slotStart + (barLayerIndex * (barWidth + barGap));
                                /** @type {any} */
                                const fmtLayer = makeLayerFmt(layer);
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
                                            data-tip-val={fmtLayer(value)}
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
                                                {fmtLayer(value)}
                                            </text>
                                        ) : null}
                                    </React.Fragment>
                                );
                            })
                        ))}

                        {areaLayers.map(/** @param {any} [layer] @param {any} [layerIndex] @returns {any} */ (layer, layerIndex) => {
                            /** @type {any} */
                            const points = model.categories.map(/** @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (_, categoryIndex) => {
                                const rawValue = layer.values[categoryIndex];
                                if (rawValue === null || rawValue === undefined) return null;
                                /** @type {any} */
                                const x = margin.left + (categoryIndex * step) + (step / 2);
                                /** @type {any} */
                                const scaleY = getLayerScale(layer);
                                return {
                                    x,
                                    y: scaleY(rawValue),
                                    baseY: getLayerBaseline(layer),
                                    rawValue,
                                };
                            });
                            /** @type {any} */
                            const areaPath = buildAreaBandPath(points);
                            /** @type {any} */
                            const linePath = buildLinePath(points);
                            /** @type {any} */
                            const color = layer.color || getColorForIndex(barLayers.length + layerIndex, theme, paletteColors);
                            /** @type {any} */
                            const fmtLayer = makeLayerFmt(layer);
                            return (
                                <g key={`combo-area-${layer.id}`}>
                                    {areaPath ? <path d={areaPath} fill={color} opacity="0.16" style={{ pointerEvents: 'none' }} /> : null}
                                    {linePath ? <path d={linePath} fill="none" stroke={color} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" strokeDasharray={getDashArray(lineDashStyles[layer.name])} style={{ pointerEvents: 'none' }} /> : null}
                                    {points.map(/** @param {any} [point] @param {any} [pointIndex] @returns {any} */ (point, pointIndex) => {
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
                                                    data-tip-val={fmtLayer(point.rawValue)}
                                                    data-tip-color={color}
                                                />
                                                {showDataLabels ? (
                                                    <text x={point.x} y={point.y - 8} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>
                                                        {fmtLayer(point.rawValue)}
                                                    </text>
                                                ) : null}
                                            </React.Fragment>
                                        );
                                    })}
                                </g>
                            );
                        })}

                        {lineLayers.map(/** @param {any} [layer] @param {any} [layerIndex] @returns {any} */ (layer, layerIndex) => {
                            /** @type {any} */
                            const points = model.categories.map(/** @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (_, categoryIndex) => {
                                const rawValue = layer.values[categoryIndex];
                                if (rawValue === null || rawValue === undefined) return null;
                                /** @type {any} */
                                const x = margin.left + (categoryIndex * step) + (step / 2);
                                return {
                                    x,
                                    y: getLayerScale(layer)(rawValue),
                                    rawValue,
                                };
                            });
                            /** @type {any} */
                            const linePath = buildLinePath(points);
                            /** @type {any} */
                            const color = layer.color || getColorForIndex(barLayers.length + areaLayers.length + layerIndex, theme, paletteColors);
                            /** @type {any} */
                            const fmtLayer = makeLayerFmt(layer);
                            return (
                                <g key={`combo-line-${layer.id}`}>
                                    {linePath ? <path d={linePath} fill="none" stroke={color} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" strokeDasharray={getDashArray(lineDashStyles[layer.name])} style={{ pointerEvents: 'none' }} /> : null}
                                    {points.map(/** @param {any} [point] @param {any} [pointIndex] @returns {any} */ (point, pointIndex) => {
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
                                                    data-tip-val={fmtLayer(point.rawValue)}
                                                    data-tip-color={color}
                                                />
                                                {showDataLabels ? (
                                                    <text x={point.x} y={point.y - 8} textAnchor="middle" fontSize="9" fontWeight="700" fill={theme.textSec} style={{ pointerEvents: 'none' }}>
                                                        {fmtLayer(point.rawValue)}
                                                    </text>
                                                ) : null}
                                            </React.Fragment>
                                        );
                                    })}
                                </g>
                            );
                        })}

                        {model.categories.map(/** @param {any} [category] @param {any} [categoryIndex] @returns {any} */ (category, categoryIndex) => {
                            /** @type {any} */
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

                        {typeof onCategoryActivate === 'function' ? model.categories.map(/** @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (_, categoryIndex) => {
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
                {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
            </div>
        );
    }

    if (!geometry) {
        return <EmptyChartState message="No numeric data available for this chart." theme={theme} chartHeight={effectiveChartHeight} />;
    }

    /** @type {any} */
    const ticks = niceTickValues(geometry.minValue, geometry.maxValue, tickCount || 5);
    /** @type {any} */
    const stackedLegendLabels = stackedChildBarMode
        ? Array.from(new Set(
            (model.stackedGroups || []).reduce(/** @param {any} [labels] @param {any} [group] @returns {any} */ (labels, group) => {
                (group.segments || []).forEach(/** @param {any} [segment] @returns {any} */ (segment) => {
                    if (segment && segment.label) labels.push(segment.label);
                });
                return labels;
            }, [])
        ))
        : [];
    /** @type {any} */
    const stackedColorByLabel = stackedLegendLabels.reduce(/** @param {any} [acc] @param {any} [label] @param {any} [index] @returns {any} */ (acc, label, index) => {
        acc[label] = getColorForIndex(index, theme, paletteColors);
        return acc;
    }, {});
    /** @type {any} */
    const legendItems = stackedChildBarMode
        ? stackedLegendLabels.map(/** @param {any} [label] @returns {any} */ (label) => ({ label, color: stackedColorByLabel[label] || getColorForIndex(0, theme, paletteColors) }))
        : (model.series || []).map(/** @param {any} [series] @param {any} [index] @returns {any} */ (series, index) => ({
            label: series.name,
            color: resolveSeriesColor(series.name, index),
        }));
    /** @type {any} */
    const groupedBands = geometry.hasHierarchicalBands
        ? model.categoryBands.reduce(/** @param {any} [groups] @param {any} [band] @param {any} [index] @returns {any} */ (groups, band, index) => {
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
    /** @type {any} */
    const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
    /** @type {any} */
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
                    onClick={onElementSelect ? handleElementClick : undefined}
                >
                    {showGradient ? (
                        <defs>
                            {(model.series || []).map(/** @param {any} [ser] @param {any} [si] @returns {any} */ (ser, si) => {
                                /** @type {any} */
                                const c = resolveSeriesColor(ser.name, si);
                                /** @type {any} */
                                const dir = GRADIENT_DIRECTIONS.find(d => d.value === gradientDirection) || GRADIENT_DIRECTIONS[0];
                                /** @type {any} */
                                const stops = buildGradientStops(c, gradientOpacity);
                                return (
                                    <linearGradient key={`grad-${si}`} id={`pg-grad-${si}`} x1={dir.x1} y1={dir.y1} x2={dir.x2} y2={dir.y2}>
                                        <stop offset={stops[0].offset} stopColor={c} stopOpacity={stops[0].stopOpacity} />
                                        <stop offset={stops[1].offset} stopColor={c} stopOpacity={stops[1].stopOpacity} />
                                    </linearGradient>
                                );
                            })}
                        </defs>
                    ) : null}
                    {ticks.map(/** @param {any} [tickValue] @param {any} [index] @returns {any} */ (tickValue, index) => {
                        /** @type {any} */
                        const y = geometry.scaleY ? geometry.scaleY(tickValue) : null;
                        /** @type {any} */
                        const x = geometry.scaleX ? geometry.scaleX(tickValue) : null;
                        /** @type {any} */
                        const tickLabel = stacked100Mode ? `${Math.round(tickValue)}%` : fmt(tickValue);
                        return (
                            <g key={`tick-${index}`}>
                                {geometry.horizontalBarMode ? (
                                    <>
                                        <line x1={x} y1={geometry.margin.top} x2={x} y2={effectiveChartHeight - geometry.margin.bottom} stroke={theme.border} strokeDasharray="3 4" />
                                                        <text x={x} y={effectiveChartHeight - geometry.margin.bottom + 18} textAnchor="middle" fontSize="11" fill={theme.textSec} data-chart-element-type="axis" data-chart-element-id="x" style={onElementSelect ? { cursor: 'pointer' } : undefined}>
                                            {tickLabel}
                                        </text>
                                    </>
                                ) : (
                                    <>
                                        <line x1={geometry.margin.left} y1={y} x2={resolvedChartWidth - geometry.margin.right} y2={y} stroke={theme.border} strokeDasharray="3 4" />
                                        <text x={geometry.margin.left - 8} y={y + 4} textAnchor="end" fontSize="11" fill={theme.textSec} data-chart-element-type="axis" data-chart-element-id="y" style={onElementSelect ? { cursor: 'pointer' } : undefined}>
                                            {tickLabel}
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
                    {Array.isArray(referenceLines) && referenceLines.length > 0 && referenceLines.map((refLine, refIdx) => {
                        /** @type {any} */
                        const numVal = Number(refLine.value);
                        if (!Number.isFinite(numVal)) return null;
                        /** @type {any} */
                        const lineColor = refLine.color || theme.primary;
                        /** @type {any} */
                        const isVertical = refLine.orient === 'v';
                        /** @type {any} */
                        const refDashArray = getDashArray(refLine.dash || 'dashed') || '6 3';
                        if (isVertical) {
                            /** @type {any} */
                            const rx = geometry.margin.left + numVal * geometry.step;
                            if (rx < geometry.margin.left - 2 || rx > resolvedChartWidth - geometry.margin.right + 2) return null;
                            return (
                                <g key={`ref-line-${refIdx}`}>
                                    <line x1={rx} y1={geometry.margin.top} x2={rx} y2={effectiveChartHeight - geometry.margin.bottom} stroke={lineColor} strokeWidth="1.5" strokeDasharray={refDashArray} style={{ pointerEvents: 'none' }} />
                                    {refLine.label ? <text x={rx + 4} y={geometry.margin.top + 12} textAnchor="start" fontSize="10" fontWeight="700" fill={lineColor} style={{ pointerEvents: 'none' }}>{refLine.label}</text> : null}
                                </g>
                            );
                        }
                        if (!geometry.scaleY) return null;
                        /** @type {any} */
                        const ry = geometry.scaleY(numVal);
                        if (ry < geometry.margin.top - 2 || ry > effectiveChartHeight - geometry.margin.bottom + 2) return null;
                        return (
                            <g key={`ref-line-${refIdx}`}>
                                <line x1={geometry.margin.left} y1={ry} x2={resolvedChartWidth - geometry.margin.right} y2={ry} stroke={lineColor} strokeWidth="1.5" strokeDasharray={refDashArray} style={{ pointerEvents: 'none' }} />
                                {refLine.label ? <text x={resolvedChartWidth - geometry.margin.right - 4} y={ry - 4} textAnchor="end" fontSize="10" fontWeight="700" fill={lineColor} style={{ pointerEvents: 'none' }}>{refLine.label}</text> : null}
                                {typeof onReferenceLinesChange === 'function' ? (
                                    <circle
                                        cx={geometry.margin.left + 22}
                                        cy={ry}
                                        r={6}
                                        fill={lineColor}
                                        stroke="white"
                                        strokeWidth="1.5"
                                        style={{ cursor: 'ns-resize' }}
                                        onMouseDown={(e) => {
                                            e.preventDefault();
                                            e.stopPropagation();
                                            const startY = e.clientY;
                                            const startVal = numVal;
                                            const { plotHeight, maxValue: gMax, minValue: gMin } = geometry;
                                            /** @type {any} */
                                            const span = gMax - gMin;
                                            if (span <= 0) return;
                                            /** @param {any} [me] @returns {any} */
                                            const move = (me) => {
                                                /** @type {any} */
                                                const nextVal = startVal - ((me.clientY - startY) / plotHeight) * span;
                                                onReferenceLinesChange(/** @param {any} [prev] @returns {any} */ (prev) => prev.map(/** @param {any} [l] @param {any} [j] @returns {any} */ (l, j) =>
                                                    j === refIdx ? { ...l, value: String(Math.round(nextVal * 1000) / 1000) } : l
                                                ));
                                            };
                                            /** @returns {any} */
                                            const up = () => {
                                                window.removeEventListener('mousemove', move);
                                                window.removeEventListener('mouseup', up);
                                            };
                                            window.addEventListener('mousemove', move);
                                            window.addEventListener('mouseup', up);
                                        }}
                                    />
                                ) : null}
                            </g>
                        );
                    })}

                    {showTrendline && !geometry.horizontalBarMode && (chartType === 'bar' || chartType === 'line' || chartType === 'area' || chartType === 'scatter') && Array.isArray(model.series) && model.series.length > 0 && geometry.scaleY ? (() => {
                        /** @type {any} */
                        const trendSeries = model.series.filter(/** @param {any} [s] @returns {any} */ s => !hiddenSeries.has(s.name));
                        return trendSeries.map(/** @param {any} [ser] @param {any} [tsi] @returns {any} */ (ser, tsi) => {
                            /** @type {any} */
                            const primaryValues = ser.values || [];
                            /** @type {any} */
                            const trendVals = trendlineMode === 'moving_avg'
                                ? computeMovingAverage(primaryValues, trendlinePeriod)
                                : computeLinearTrendline(primaryValues);
                            if (!trendVals) return null;
                            /** @type {any} */
                            const pts = trendVals.map(/** @param {any} [v] @param {any} [i] @returns {any} */ (v, i) => {
                                if (!Number.isFinite(v)) return null;
                                return { x: geometry.margin.left + i * geometry.step + geometry.step / 2, y: geometry.scaleY(v) };
                            });
                            /** @type {any} */
                            const d = buildLinePath(pts);
                            if (!d) return null;
                            /** @type {any} */
                            const fallbackColor = resolveSeriesColor(ser.name, tsi);
                            /** @type {any} */
                            const resolvedTrendColor = trendlineColor || fallbackColor;
                            return (
                                <path
                                    key={`trendline-${tsi}`}
                                    d={d}
                                    fill="none"
                                    stroke={resolvedTrendColor}
                                    strokeWidth={String(trendlineWidth || 2)}
                                    strokeDasharray="8 4"
                                    strokeLinecap="round"
                                    opacity="0.78"
                                    style={{ pointerEvents: 'none' }}
                                />
                            );
                        });
                    })() : null}

                    {geometry.hasHierarchicalBands && groupedBands.map(/** @param {any} [group] @param {any} [groupIndex] @returns {any} */ (group, groupIndex) => {
                        if (groupIndex === groupedBands.length - 1) return null;
                        /** @type {any} */
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

                    {stackedChildBarMode && (model.stackedGroups || []).map(/** @param {any} [group] @param {any} [groupIndex] @returns {any} */ (group, groupIndex) => {
                        /** @type {any} */
                        const slotStart = geometry.horizontalBarMode
                            ? geometry.margin.top + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2)
                            : geometry.margin.left + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2);
                        /** @type {any} */
                        const barThickness = Math.max(14, geometry.groupWidth * 0.64);
                        let positiveOffset = 0;
                        let negativeOffset = 0;

                        return (
                            <g key={`stacked-group-${group.key}`}>
                                {(group.segments || []).map(/** @param {any} [segment] @returns {any} */ (segment) => {
                                    /** @type {any} */
                                    const rawValue = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                                    if (rawValue === null) return null;
                                    /** @type {any} */
                                    const startValue = rawValue >= 0 ? positiveOffset : negativeOffset;
                                    /** @type {any} */
                                    const endValue = startValue + rawValue;
                                    if (rawValue >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    /** @type {any} */
                                    const segColor = stackedColorByLabel[segment.label] || getColorForIndex(0, theme, paletteColors);
                                    if (geometry.horizontalBarMode) {
                                        /** @type {any} */
                                        const x1 = geometry.scaleX(startValue);
                                        /** @type {any} */
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
                                    /** @type {any} */
                                    const y1 = geometry.scaleY(startValue);
                                    /** @type {any} */
                                    const y2 = geometry.scaleY(endValue);
                                    /** @type {any} */
                                    const y = Math.min(y1, y2);
                                    /** @type {any} */
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

                {!stackedChildBarMode && chartType === 'bar' && model.categories.map(/** @param {any} [category] @param {any} [categoryIndex] @returns {any} */ (category, categoryIndex) => {
                    /** @type {any} */
                    const slotStart = geometry.horizontalBarMode
                        ? geometry.margin.top + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2
                        : geometry.margin.left + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2;
                    /** @type {any} */
                    const barThickness = (stackedSeriesBarMode || stacked100Mode)
                        ? Math.max(14, geometry.groupWidth * 0.64)
                        : Math.max(6, (geometry.groupWidth / Math.max(model.series.length, 1)) - 4);
                    let positiveOffset = 0;
                    let negativeOffset = 0;
                    return (
                        <g key={`bar-group-${category}`}>
                            {model.series.map(/** @param {any} [series] @param {any} [seriesIndex] @returns {any} */ (series, seriesIndex) => {
                                if (hiddenSeries.has(series.name)) return null;
                                const value = series.values[categoryIndex];
                                if (value === null || value === undefined) return null;
                                /** @type {any} */
                                const barColor = resolveSeriesColor(series.name, seriesIndex);
                                /** @type {any} */
                                const barFill = showGradient ? `url(#pg-grad-${seriesIndex})` : barColor;
                                if (stackedSeriesBarMode || stacked100Mode) {
                                    /** @type {any} */
                                    const catTotal = stacked100Mode && norm100Totals ? norm100Totals[categoryIndex] : null;
                                    /** @type {any} */
                                    const plotValue = stacked100Mode && catTotal ? (value / catTotal) * 100 : value;
                                    /** @type {any} */
                                    const startValue = plotValue >= 0 ? positiveOffset : negativeOffset;
                                    /** @type {any} */
                                    const endValue = startValue + plotValue;
                                    if (plotValue >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    /** @type {any} */
                                    const tipVal = stacked100Mode ? `${(Math.abs(value / catTotal) * 100).toFixed(1)}% (${fmt(value)})` : fmt(value);
                                    /** @type {Record<string, any>} */
                                    const tipAttrs = { 'data-tip-cat': category, 'data-tip-ser': series.name, 'data-tip-val': tipVal, 'data-tip-color': barColor };
                                    if (geometry.horizontalBarMode) {
                                        /** @type {any} */
                                        const x1 = geometry.scaleX(startValue);
                                        /** @type {any} */
                                        const x2 = geometry.scaleX(endValue);
                                        return (
                                            <rect
                                                key={`${series.name}-${category}`}
                                                x={Math.min(x1, x2)}
                                                y={slotStart}
                                                width={Math.max(1, Math.abs(x2 - x1))}
                                                height={barThickness}
                                                rx="3"
                                                fill={barFill}
                                                opacity="0.92"
                                                style={{ transition: showAnimations ? 'width 0.35s ease, x 0.35s ease' : undefined }}
                                                {...tipAttrs}
                                            />
                                        );
                                    }
                                    /** @type {any} */
                                    const y1 = geometry.scaleY(startValue);
                                    /** @type {any} */
                                    const y2 = geometry.scaleY(endValue);
                                    return (
                                        <rect
                                            key={`${series.name}-${category}`}
                                            x={slotStart}
                                            y={Math.min(y1, y2)}
                                            width={barThickness}
                                            height={Math.max(1, Math.abs(y2 - y1))}
                                            rx="3"
                                            fill={barFill}
                                            opacity="0.92"
                                            style={{ transition: showAnimations ? 'height 0.35s ease, y 0.35s ease' : undefined }}
                                            {...tipAttrs}
                                        />
                                    );
                                }
                                if (geometry.horizontalBarMode) {
                                    /** @type {any} */
                                    const x = geometry.scaleX(value);
                                    /** @type {any} */
                                    const y = slotStart + seriesIndex * (barThickness + 4);
                                    return (
                                        <React.Fragment key={`${series.name}-${category}`}>
                                            <rect
                                                x={Math.min(x, geometry.baselineX)}
                                                y={y}
                                                width={Math.max(1, Math.abs(geometry.baselineX - x))}
                                                height={barThickness}
                                                rx="3"
                                                fill={barFill}
                                                opacity="0.9"
                                                style={{ transition: showAnimations ? 'width 0.35s ease, x 0.35s ease' : undefined }}
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
                                /** @type {any} */
                                const y = geometry.scaleY(value);
                                /** @type {any} */
                                const height = Math.max(1, Math.abs(geometry.baselineY - y));
                                /** @type {any} */
                                const x = slotStart + seriesIndex * (barThickness + 4);
                                return (
                                    <React.Fragment key={`${series.name}-${category}`}>
                                        <rect
                                            x={x}
                                            y={Math.min(y, geometry.baselineY)}
                                            width={barThickness}
                                            height={height}
                                            rx="3"
                                            fill={barFill}
                                            opacity="0.9"
                                            style={{ transition: showAnimations ? 'height 0.35s ease, y 0.35s ease' : undefined, cursor: onElementSelect ? 'pointer' : undefined }}
                                            data-tip-cat={category}
                                            data-tip-ser={series.name}
                                            data-tip-val={fmt(value)}
                                            data-tip-color={barColor}
                                            data-chart-element-type="series"
                                            data-chart-element-id={series.name}
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
                    /** @type {any} */
                    const positiveOffsets = model.categories.map(() => 0);
                    /** @type {any} */
                    const negativeOffsets = model.categories.map(() => 0);
                    return model.series.map(/** @param {any} [series] @param {any} [seriesIndex] @returns {any} */ (series, seriesIndex) => {
                        if (hiddenSeries.has(series.name)) return null;
                        /** @type {any} */
                        const stroke = resolveSeriesColor(series.name, seriesIndex);
                        /** @type {any} */
                        const points = model.categories.map(/** @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (_, categoryIndex) => {
                            const rawValue = series.values[categoryIndex];
                            if (rawValue === null || rawValue === undefined) return null;
                            /** @type {any} */
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            if (stackedAreaMode || stacked100Mode) {
                                /** @type {any} */
                                const catTotal = stacked100Mode && norm100Totals ? norm100Totals[categoryIndex] : null;
                                /** @type {any} */
                                const plotValue = stacked100Mode && catTotal ? (rawValue / catTotal) * 100 : rawValue;
                                /** @type {any} */
                                const startValue = plotValue >= 0 ? positiveOffsets[categoryIndex] : negativeOffsets[categoryIndex];
                                /** @type {any} */
                                const endValue = startValue + plotValue;
                                if (plotValue >= 0) positiveOffsets[categoryIndex] = endValue;
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
                        /** @type {any} */
                        const linePath = buildLinePath(points);
                        /** @type {any} */
                        const areaPath = chartType === 'area' ? buildAreaBandPath(points) : '';
                        return (
                            <g key={`${series.name}-${chartType}`}>
                                {chartType === 'area' && areaPath && (
                                    <path d={areaPath} fill={showGradient ? `url(#pg-grad-${seriesIndex})` : stroke} opacity={(stackedAreaMode || stacked100Mode) ? '0.46' : '0.16'} style={{ pointerEvents: 'none' }} />
                                )}
                                {linePath && (
                                    <path d={linePath} fill="none" stroke={stroke} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" strokeDasharray={getDashArray(lineDashStyles[series.name])} style={{ pointerEvents: 'none' }} />
                                )}
                                {points.map(/** @param {any} [point] @param {any} [pointIndex] @returns {any} */ (point, pointIndex) => {
                                    if (!point) return null;
                                    return (
                                        <React.Fragment key={`${series.name}-${pointIndex}`}>
                                            {React.cloneElement(renderMarker(point.x, point.y, stroke, {
                                                'data-tip-cat': model.categories[pointIndex],
                                                'data-tip-ser': series.name,
                                                'data-tip-val': fmt(point.rawValue),
                                                'data-tip-color': stroke,
                                            }), { key: `marker-${seriesIndex}-${pointIndex}` })}
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

                    {canActivateCategories && model.categories.map(/** @param {any} [_] @param {any} [categoryIndex] @returns {any} */ (_, categoryIndex) => {
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
                            {groupedBands.map(/** @param {any} [group] @returns {any} */ (group) => {
                                /** @type {any} */
                                const yStart = geometry.margin.top + (group.startIndex * geometry.step);
                                /** @type {any} */
                                const yEnd = geometry.margin.top + ((group.endIndex + 1) * geometry.step);
                                /** @type {any} */
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
                            {model.categoryBands.map(/** @param {any} [band] @param {any} [categoryIndex] @returns {any} */ (band, categoryIndex) => {
                                /** @type {any} */
                                const y = geometry.margin.top + (categoryIndex * geometry.step) + (geometry.step / 2);
                                /** @type {any} */
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
                        model.categories.map(/** @param {any} [category] @param {any} [categoryIndex] @returns {any} */ (category, categoryIndex) => {
                            /** @type {any} */
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
                        {groupedBands.map(/** @param {any} [group] @returns {any} */ (group) => {
                            /** @type {any} */
                            const xStart = geometry.margin.left + (group.startIndex * geometry.step);
                            /** @type {any} */
                            const xEnd = geometry.margin.left + ((group.endIndex + 1) * geometry.step);
                            /** @type {any} */
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
                        {model.categoryBands.map(/** @param {any} [band] @param {any} [categoryIndex] @returns {any} */ (band, categoryIndex) => {
                            /** @type {any} */
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            /** @type {any} */
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
                    model.categories.map(/** @param {any} [category] @param {any} [categoryIndex] @returns {any} */ (category, categoryIndex) => {
                        if (categoryAxisLabelPlan.visibleIndexes && !categoryAxisLabelPlan.visibleIndexes.has(categoryIndex)) return null;
                        /** @type {any} */
                        const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                        return (
                            <g key={`label-${categoryIndex}`} transform={`translate(${x}, ${effectiveChartHeight - geometry.margin.bottom + 18}) rotate(${effectiveLabelAngle})`}>
                                <text textAnchor={effectiveLabelAngle === 0 ? 'middle' : 'start'} fontSize="11" fill={theme.textSec} data-chart-element-type="axis" data-chart-element-id="x" style={onElementSelect ? { cursor: 'pointer' } : undefined}>
                                    <title>{String(category)}</title>
                                    {truncateChartLabel(category, categoryAxisLabelPlan.maxChars)}
                                </text>
                            </g>
                        );
                    })
                    )}
                </svg>
                {chartTitleOverlay}{tooltipElement}
            </div>
            {showLegend ? <ChartLegend items={legendItems} theme={theme} hiddenSet={hiddenSeries} onToggle={toggleHiddenSeries} position={legendPosition} onElementSelect={onElementSelect} /> : null}
        </div>
    );
};

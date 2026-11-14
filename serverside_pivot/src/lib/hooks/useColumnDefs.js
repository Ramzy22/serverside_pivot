import { useMemo } from 'react';
import React from 'react';
import Icons from '../utils/Icons';
import EditableCell from '../components/Table/EditableCell';
import SparklineCell from '../components/Table/SparklineCell';
import {
    formatValue,
    formatDisplayLabel,
    formatAggLabel,
    getKey,
    isDisplayNegative,
    resolveNumericDisplayValue,
} from '../utils/helpers';
import { CELL_CONTENT_RESET_STYLE } from '../utils/styles';
import { resolveColumnSortOptions, shouldUseAbsoluteSort } from '../utils/sorting';
import {
    buildPivotSparklinePoints,
    buildSparklineHeader,
    normalizeSparklineConfig,
    normalizeSparklinePoints,
    resolveSparklineDeltaValue,
    resolveSparklineMetricValue,
} from '../utils/sparklines';
import { formatCustomAwareFieldLabel, isGrandTotalRow } from './usePivotNormalization';
import {
    isBlankHierarchyValue,
    resolveTabularPathFieldValue,
    resolveTabularRowFieldValue,
} from '../utils/layoutRows';

/** @param {any} [cols] @param {any} [idPrefix] @returns {any} */
export const applyMeasureHeaderGroups = (cols, idPrefix = '') => {
    if (!Array.isArray(cols) || cols.length === 0) return cols;
    /** @type {any} */
    const hasAnyGroup = cols.some((col) => col && !col.columns && col.meta && col.meta.headerGroup);
    if (!hasAnyGroup) return cols;
    /** @type {any[]} */
    const grouped = [];
    let groupCounter = 0;
    let idx = 0;
    while (idx < cols.length) {
        const col = cols[idx];
        /** @type {any} */
        const groupLabel = col && !col.columns && col.meta ? col.meta.headerGroup : null;
        if (!groupLabel) {
            grouped.push(col);
            idx += 1;
            continue;
        }
        /** @type {any[]} */
        const runCols = [col];
        /** @type {any} */
        let nextIdx = idx + 1;
        while (nextIdx < cols.length) {
            const nextCol = cols[nextIdx];
            /** @type {any} */
            const nextLabel = nextCol && !nextCol.columns && nextCol.meta ? nextCol.meta.headerGroup : null;
            if (nextLabel !== groupLabel) break;
            runCols.push(nextCol);
            nextIdx += 1;
        }
        groupCounter += 1;
        grouped.push({
            id: `__measure_group__${idPrefix ? `${idPrefix}_` : ''}${groupCounter}_${groupLabel}`,
            header: groupLabel,
            headerVal: groupLabel,
            enablePinning: true,
            meta: {
                isMeasureGroup: true,
                preserveHeaderLabel: true,
                headerGroupLabel: groupLabel,
            },
            columns: runCols,
        });
        idx = nextIdx;
    }
    return grouped;
};

const HIERARCHY_INDENT_PX = 32;

/** @type {any} */
const debugLog = process.env.NODE_ENV !== 'production'
    ? /** @param {...any} args @returns {any} */ (...args) => console.log('[pivot-grid]', ...args)
    : () => {};

/**
 * useColumnDefs — extracts the columns useMemo from DashTanstackPivot.
 *
 * Closed-over values that are NOT in the dep array (intentionally excluded or
 * come from stable refs) are listed below for CODE-03 stale-closure audit:
 *   - renderedOffset: used inside a cell render fn (executes at render time,
 *     not at useMemo compute time), so the value seen is always fresh
 *   - handleContextMenu, handleRowSelect, handleRowRangeSelect,
 *     setIsRowSelecting, setRowDragStart: stable callbacks (useCallback)
 *     — not included in dep array but do not cause stale behaviour because
 *     the cell functions re-run every render pass from flexRender
 *   - defaultColumnWidths, debugLog, theme, validationRules, onCellEdit,
 *     isColExpanded, toggleCol, pendingRowTransitions: read at render time
 *     from current scope; same stable-ref / render-time-read pattern
  @param {any} p0 @returns {any} */
// Column renderers close over theme and edit helpers, so the dependency list
// below must stay complete or edited-cell styling can go stale.
export function useColumnDefs({
    sortOptions,
    sorting,
    serverSide,
    showRowNumbers,
    layoutMode,
    viewMode,
    rowFields,
    colFields,
    valConfigs,
    customDimensions,
    treeConfig,
    minMax,
    colorScaleMode,
    colExpanded,
    isRowSelecting,
    rowDragStart,
    props,
    measureAxis,
    cachedColSchema,
    responseColumns,
    filteredData,
    rowCount,
    // Render-time closures (not in dep array — stable refs or render-time reads)
    theme,
    defaultColumnWidths,
    validationRules,
    onCellEdit,
    onEditBlocked,
    resolveEditorPresentation,
    rowEditMode,
    getRowEditSession,
    onRequestRowStart,
    onRowDraftChange,
    onRequestRowSave,
    onRequestRowCancel,
    requestEditorOptions,
    renderRowEditActions,
    handleContextMenu,
    handleRowSelect,
    handleRowRangeSelect,
    setIsRowSelecting,
    setRowDragStart,
    renderedOffset,
    isColExpanded,
    toggleCol,
    pendingRowTransitions,
    decimalPlaces,
    defaultValueFormat,
    numberGroupSeparator,
    columnDecimalOverrides,
    columnFormatOverrides,
    columnGroupSeparatorOverrides,
    cellFormatRules,
    pivotMode,
    reportDef,
    showReportConfigColumn = true,
    onConfigureReportLine,
    detailMode,
    isDetailOpenForRow,
    onToggleDetail,
    resolveCellDisplayValue,
    editValueDisplayMode,
    // Stable ref — not in dep array; read at cell render time
    openSparklineDataModalRef,
    // Row checkbox selection
    rowCheckboxSelection,
    selectedRowKeys,
    toggleRowCheckbox,
    // Boost styling & formatting options
    themeRef,
    dataBarsColumnsRef,
    rowHeaderLabel,
    measureGroups,
    valueScale,
    unitSuffix,
}) {
    return useMemo(() => {
        /** @type {any} */
        const effectiveLayoutMode = (pivotMode === 'report' || viewMode === 'tree') ? 'hierarchy' : layoutMode;
        /** @param {any} [field] @returns {any} */
        const getFieldLabel = (field) => formatCustomAwareFieldLabel(field, customDimensions);
        const getTabularPathFieldValue = resolveTabularPathFieldValue;
        /** @param {any} [rowData] @param {any} [field] @param {any} [index] @param {any} [fallbackValue] @returns {any} */
        const getTabularRowFieldValue = (rowData, field, index, fallbackValue = undefined) => {
            return resolveTabularRowFieldValue(
                rowData,
                field,
                index,
                effectiveLayoutMode,
                fallbackValue,
            );
        };
        /** @param {any} [config] @returns {any} */
        const getConfigDisplayFormat = (config) => {
            if (!config) return null;
            if (config.format) return config.format;
            if (config.windowFn && String(config.windowFn).startsWith('percent_')) return 'percent';
            return null;
        };
        /** @param {any} [config] @returns {any} */
        const getSparklineConfig = (config) => normalizeSparklineConfig(config && config.sparkline);
        /** @param {any} [columnEntry] @param {any} [fallbackId] @returns {any} */
        const extractColumnHeaderText = (columnEntry, fallbackId = null) => {
            /** @type {any} */
            const fallbackText = fallbackId ? getFieldLabel(fallbackId) : '';
            if (!columnEntry) return fallbackText;
            if (typeof columnEntry === 'string') return getFieldLabel(columnEntry);
            if (columnEntry.columnDef && typeof columnEntry.columnDef === 'object') {
                if (typeof columnEntry.columnDef.header === 'string' && columnEntry.columnDef.header.trim()) {
                    return columnEntry.columnDef.header.trim();
                }
                if (columnEntry.columnDef.headerVal !== undefined && columnEntry.columnDef.headerVal !== null) {
                    return String(columnEntry.columnDef.headerVal);
                }
            }
            if (typeof columnEntry.header === 'string' && columnEntry.header.trim()) {
                return columnEntry.header.trim();
            }
            if (columnEntry.headerVal !== undefined && columnEntry.headerVal !== null) {
                return String(columnEntry.headerVal);
            }
            if (columnEntry.id) return getFieldLabel(columnEntry.id);
            return fallbackText;
        };
        /** @param {any} [value] @param {any} [config] @param {any} [columnId] @returns {any} */
        const formatSparklineMetricValue = (value, config, columnId) => {
            /** @type {any} */
            const numericValue = Number(value);
            if (!Number.isFinite(numericValue)) return '';
            /** @type {any} */
            const cellDec = columnId !== undefined && columnDecimalOverrides && columnDecimalOverrides[columnId] !== undefined
                ? columnDecimalOverrides[columnId]
                : decimalPlaces;
            /** @type {any} */
            const effectiveGroupSeparator = columnId !== undefined && columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[columnId] !== undefined
                ? columnGroupSeparatorOverrides[columnId]
                : numberGroupSeparator;
            /** @type {any} */
            const effectiveFormat = columnId !== undefined && columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, columnId)
                ? columnFormatOverrides[columnId]
                : (getConfigDisplayFormat(config) || defaultValueFormat || null);
            return formatValue(numericValue, effectiveFormat, cellDec, effectiveGroupSeparator);
        };
        /** @param {any} [headerLabel] @param {any} [points] @param {any} [currentLabel] @param {any} [deltaLabel] @returns {any} */
        const buildSparklineCellTitle = (headerLabel, points, currentLabel, deltaLabel) => {
            if (!Array.isArray(points) || points.length === 0) return headerLabel || '';
            const firstPoint = points[0];
            const lastPoint = points[points.length - 1];
            /** @type {any} */
            const firstLabel = firstPoint && firstPoint.label ? String(firstPoint.label) : 'Start';
            /** @type {any} */
            const lastLabel = lastPoint && lastPoint.label ? String(lastPoint.label) : 'End';
            /** @type {any} */
            const currentSuffix = currentLabel ? ` • ${currentLabel}` : '';
            /** @type {any} */
            const deltaSuffix = deltaLabel ? ` (${deltaLabel})` : '';
            return `${headerLabel || 'Trend'}: ${firstLabel} → ${lastLabel}${currentSuffix}${deltaSuffix}`;
        };

        /** @returns {any} */
        const getCurrentTheme = () => (themeRef && themeRef.current) || theme || {};
        /** @returns {any} */
        const getNumericPaddingRight = () => {
            const v = getCurrentTheme().numericPaddingRight;
            return v !== undefined && v !== null ? (typeof v === 'number' ? `${v}px` : String(v)) : '8px';
        };
        /** @returns {any} */
        const getChevronColor = () => {
            /** @type {any} */
            const t = getCurrentTheme();
            return t.chevronColor || t.primary || t.textSec;
        };
        /** @param {any} [row] @returns {any} */
        const resolveDepthFontWeight = (row) => {
            /** @type {any} */
            const t = getCurrentTheme();
            if (row && row.original && row.original._isTotal) {
                return t.totalFontWeight !== undefined && t.totalFontWeight !== null ? t.totalFontWeight : 700;
            }
            /** @type {any} */
            const depth = (row && row.original && row.original.depth !== undefined)
                ? Number(row.original.depth)
                : ((row && typeof row.depth === 'number') ? row.depth : 0);
            /** @type {any} */
            const hasChildren = Boolean(
                (row && row.original && row.original._has_children)
                || (row && Array.isArray(row.subRows) && row.subRows.length > 0)
            );
            if (hasChildren) {
                if (Array.isArray(t.groupFontWeight) && t.groupFontWeight.length > 0) {
                    /** @type {any} */
                    const idx = Math.min(Math.max(0, Math.floor(depth || 0)), t.groupFontWeight.length - 1);
                    return t.groupFontWeight[idx];
                }
                if (t.groupFontWeight !== undefined && t.groupFontWeight !== null && !Array.isArray(t.groupFontWeight)) {
                    return t.groupFontWeight;
                }
                return 400;
            }
            return t.leafFontWeight !== undefined && t.leafFontWeight !== null ? t.leafFontWeight : 400;
        };

        // Helper: return the data key for a valConfig
        /** @param {any} [c] @returns {any} */
        const getValKey = (c) => (c && c.agg === 'formula' ? (c.alias || c.id || c.field) : (c.alias || c.id || getKey('', c.field, c.agg)));
        // Helper: return display header for a valConfig
        /** @param {any} [c] @returns {any} */
        const getValHeader = (c) => {
            if (c && c.label) return String(c.label);
            return c && c.agg === 'formula'
                ? String(c.field)
                : `${getFieldLabel(c.field)} (${formatAggLabel(c.agg, c.weightField)})`;
        };
        /** @param {any} [columnId] @param {any} [config] @returns {any} */
        const matchesValueConfigColumnId = (columnId, config) => {
            if (!config || typeof columnId !== 'string') return false;
            /** @type {any} */
            const normalizedId = columnId.toLowerCase();
            if (config.agg === 'formula') {
                /** @type {any} */
                const candidates = [
                    config.alias,
                    config.id,
                    config.field,
                    config.field ? `${config.field}_formula` : null,
                ].filter(Boolean).map((entry) => String(entry).toLowerCase());
                return candidates.some(/** @param {any} [candidate] @returns {any} */ (candidate) => (
                    normalizedId === candidate || normalizedId.endsWith(`_${candidate}`)
                ));
            }
            if (!config.field) return false;
            /** @type {any} */
            const measureId = getValKey(config).toLowerCase();
            /** @type {any} */
            const measureSuffix = `_${config.field}_${config.agg}`.toLowerCase();
            return normalizedId === measureId || normalizedId.endsWith(`_${measureId}`) || normalizedId.endsWith(measureSuffix);
        };
        /** @param {any} [config] @param {any} [colId] @returns {any} */
        const resolveMeasureHeaderGroupLabel = (config, colId = null) => {
            if (config && typeof config.headerGroup === 'string' && config.headerGroup.trim()) {
                return config.headerGroup.trim();
            }
            if (Array.isArray(measureGroups) && measureGroups.length > 0) {
                /** @type {any} */
                const candidateKeys = [
                    colId,
                    config && getValKey(config),
                    config && config.id,
                    config && config.alias,
                    config && config.field,
                    config && config.label,
                ].filter(Boolean).map((v) => String(v));
                for (const group of measureGroups) {
                    if (!group || typeof group.label !== 'string' || !group.label.trim()) continue;
                    /** @type {any} */
                    const members = Array.isArray(group.members) ? group.members : (Array.isArray(group.fields) ? group.fields : []);
                    if (members.some(/** @param {any} [m] @returns {any} */ (m) => candidateKeys.includes(String(m)))) {
                        return group.label.trim();
                    }
                }
            }
            return null;
        };
        /** @type {any} */
        const measureAxisConfig = measureAxis && typeof measureAxis === 'object'
            ? measureAxis
            : props && props.measureAxis && typeof props.measureAxis === 'object'
                ? props.measureAxis
                : null;
        /** @type {any} */
        const measureAxisPlacement = measureAxisConfig && typeof measureAxisConfig.placement === 'string'
            ? measureAxisConfig.placement.toLowerCase()
            : 'none';
        /** @type {any} */
        const measureAxisValueField = measureAxisConfig && (measureAxisConfig.valueField || measureAxisConfig.value_field)
            ? String(measureAxisConfig.valueField || measureAxisConfig.value_field)
            : null;
        /** @param {any} [columnId] @returns {any} */
        const isMeasureAxisValueColumnId = (columnId) => {
            if (!measureAxisValueField || !['rows', 'columns'].includes(measureAxisPlacement) || typeof columnId !== 'string') {
                return false;
            }
            return columnId === measureAxisValueField
                || columnId.endsWith(`_${measureAxisValueField}`)
                || columnId.startsWith(`__RowTotal__${measureAxisValueField}`);
        };
        // Field-source sparkline configs: the field's cell value IS the series array.
        // These are matched by exact field name, not by pivot-suffix pattern.
        /** @type {any} */
        const fieldSparklineConfigs = (Array.isArray(valConfigs) ? valConfigs : []).filter(c => {
            /** @type {any} */
            const sc = normalizeSparklineConfig(c && c.sparkline);
            return sc && sc.source === 'field';
        });
        /** @param {any} [columnId] @returns {any} */
        const getConfigForColumnId = (columnId) => {
            if (!Array.isArray(valConfigs)) return null;
            /** @type {any} */
            const byAgg = valConfigs.find(config => matchesValueConfigColumnId(columnId, config));
            if (byAgg) return byAgg;
            // Field-source sparklines: match by exact field name (no agg suffix)
            return fieldSparklineConfigs.find(/** @param {any} [c] @returns {any} */ c => c.field === columnId) || null;
        };
        /** @param {any} [rowData] @returns {any} */
        const getMeasureAxisRowValueConfig = (rowData) => {
            if (
                measureAxisPlacement !== 'rows'
                || !measureAxisConfig
                || !rowData
                || typeof rowData !== 'object'
                || !Array.isArray(valConfigs)
            ) {
                return null;
            }
            /** @type {any} */
            const labelField = String(measureAxisConfig.labelField || measureAxisConfig.label_field || 'Measure Name');
            const rowLabel = rowData[labelField];
            if (rowLabel === undefined || rowLabel === null || rowLabel === '') return null;
            /** @type {any} */
            const rowLabelText = String(rowLabel);
            /** @type {any} */
            const members = Array.isArray(measureAxisConfig.members) ? measureAxisConfig.members : [];
            /** @type {any} */
            const member = members.find(/** @param {any} [candidate] @returns {any} */ (candidate) => {
                if (!candidate || typeof candidate !== 'object') return false;
                return String(candidate.label || candidate.name || '') === rowLabelText
                    || String(candidate.measureAlias || candidate.measure_alias || candidate.alias || candidate.id || '') === rowLabelText;
            });
            /** @type {any} */
            const memberAlias = member
                ? String(member.measureAlias || member.measure_alias || member.alias || member.id || '')
                : '';
            /** @type {any} */
            const memberSource = member
                ? String(member.sourceField || member.source_field || member.field || '')
                : '';
            /** @type {any} */
            const memberAgg = member
                ? String(member.agg || member.aggregation || '').toLowerCase()
                : '';
            return valConfigs.find((config) => {
                if (!config || typeof config !== 'object') return false;
                /** @type {any} */
                const configKey = getValKey(config);
                if (memberAlias && (config.alias === memberAlias || config.id === memberAlias || configKey === memberAlias)) return true;
                if (memberSource && config.field === memberSource) {
                    return !memberAgg || String(config.agg || '').toLowerCase() === memberAgg;
                }
                return String(config.label || '') === rowLabelText;
            }) || null;
        };
        /** @param {any} [config] @returns {any} */
        const buildMeasureHeaderStyle = (config) => {
            if (!config || typeof config !== 'object') return undefined;
            /** @type {any} */
            const explicit = config.headerStyle && typeof config.headerStyle === 'object' && !Array.isArray(config.headerStyle)
                ? { ...config.headerStyle }
                : {};
            /** @type {any} */
            const background = config.headerBg
                || config.headerBackground
                || config.headerBackgroundColor
                || config.headerFill
                || config.headerColor;
            /** @type {any} */
            const color = config.headerTextColor
                || config.headerText
                || config.headerForeground
                || config.headerFg;
            if (background !== undefined && background !== null && background !== '') {
                explicit.background = background;
            }
            if (color !== undefined && color !== null && color !== '') {
                explicit.color = color;
            }
            return Object.keys(explicit).length > 0 ? explicit : undefined;
        };
        // Pre-compute sparkline-hidden set; used in all column-building paths
        /** @type {any} */
        const sparklineHideValConfigs = (Array.isArray(valConfigs) ? valConfigs : [])
            .filter(c => { /** @type {any} */ const sc = normalizeSparklineConfig(c && c.sparkline); return sc && sc.hideColumns; });
        /** @param {any} [columnId] @returns {any} */
        const isSparklineHiddenColumn = (columnId) => (
            sparklineHideValConfigs.length > 0 &&
            sparklineHideValConfigs.some(/** @param {any} [config] @returns {any} */ config => matchesValueConfigColumnId(columnId, config))
        );
        /** @param {any} [config] @param {any} [columnId] @returns {any} */
        const isEditableMeasureConfig = (config, columnId) => {
            if (!config || typeof config !== 'object') return false;
            if (typeof columnId === 'string' && columnId.startsWith('__RowTotal__')) return false;
            /** @type {any} */
            const normalizedAgg = String(config.agg || '').trim().toLowerCase();
            if (!normalizedAgg || normalizedAgg === 'formula') return false;
            if (config.windowFn) return false;
            return !['count', 'count_distinct', 'distinct_count'].includes(normalizedAgg);
        };
        /** @param {any} [info] @returns {any} */
        const getCellRowId = (info) => {
            /** @type {any} */
            const row = info && info.row;
            if (!row) return null;
            return row.original && row.original._path ? row.original._path : row.id;
        };
        /** @param {any} [info] @returns {any} */
        const getResolvedCellValue = (info) => {
            if (!info) return undefined;
            /** @type {any} */
            const rawValue = info.getValue();
            if (typeof resolveCellDisplayValue !== 'function') return rawValue;
            return resolveCellDisplayValue(getCellRowId(info), info.column && info.column.id, rawValue);
        };
        /** @param {any} [info] @param {any} [config] @param {any} [columnId] @returns {any} */
        const getFieldSparklineRawValue = (info, config, columnId) => {
            /** @type {any} */
            const rowData = info && info.row ? info.row.original : null;
            if (!rowData) return getResolvedCellValue(info);
            /** @type {any} */
            const rawColumnValue = columnId ? rowData[columnId] : undefined;
            /** @type {any} */
            const rawFieldValue = config && config.field ? rowData[config.field] : undefined;
            /** @type {any} */
            const rawValue = rawColumnValue !== undefined && rawColumnValue !== null
                ? rawColumnValue
                : rawFieldValue;
            if (typeof resolveCellDisplayValue !== 'function') return rawValue;
            return resolveCellDisplayValue(getCellRowId(info), columnId, rawValue);
        };
        /** @param {any} p0 @returns {any} */
        const renderSparklineCellContent = ({
            info,
            points,
            config,
            sparklineConfig,
            columnId,
            compact = false,
        }) => {
            if (!Array.isArray(points) || points.length === 0) return null;
            /** @type {any} */
            const summaryMetric = resolveSparklineMetricValue(points, sparklineConfig.metric);
            /** @type {any} */
            const deltaMetric = resolveSparklineDeltaValue(points);
            /** @type {any} */
            const headerLabel = buildSparklineHeader(config, config && config.field ? config.field : columnId);
            return (
                <SparklineCell
                    points={points}
                    type={sparklineConfig.type}
                    theme={theme}
                    color={sparklineConfig.color}
                    positiveColor={sparklineConfig.positiveColor}
                    negativeColor={sparklineConfig.negativeColor}
                    areaOpacity={sparklineConfig.areaOpacity}
                    showCurrentValue={compact ? false : sparklineConfig.showCurrentValue}
                    showDelta={compact ? false : sparklineConfig.showDelta}
                    currentLabel={formatSparklineMetricValue(summaryMetric, config, columnId)}
                    deltaLabel={formatSparklineMetricValue(deltaMetric, config, columnId)}
                    deltaValue={deltaMetric}
                    compact={compact || sparklineConfig.compact}
                    title={buildSparklineCellTitle(
                        headerLabel,
                        points,
                        formatSparklineMetricValue(summaryMetric, config, columnId),
                        formatSparklineMetricValue(deltaMetric, config, columnId),
                    )}
                />
            );
        };
        /** @param {any} [info] @param {any} [config] @returns {any} */
        const renderConfiguredInlineSparkline = (info, config) => {
            /** @type {any} */
            const sparklineConfig = getSparklineConfig(config);
            if (!sparklineConfig || sparklineConfig.source !== 'field') return null;
            /** @type {any} */
            const columnId = info && info.column ? info.column.id : null;
            /** @type {any} */
            const points = normalizeSparklinePoints(getFieldSparklineRawValue(info, config, columnId));
            if (points.length === 0) return null;
            if (sparklineConfig.displayMode === 'value') {
                return renderSparklineValueCellContent({
                    info,
                    points,
                    config,
                    sparklineConfig,
                    columnId,
                });
            }
            return renderSparklineCellContent({
                info,
                points,
                config,
                sparklineConfig,
                columnId,
                compact: true,
            });
        };
        /**
         * resolveSparklineSourceColumnIds — finds all source column IDs that
         * match a sparkline-configured metric. Lifted to this scope so both
         * source-column hiding and buildSparklineSummaryColumns can use it.
          @param {any} [config] @param {any} [colSchema] @param {any} [auxIds] @returns {any} */
        const resolveSparklineSourceColumnIds = (config, colSchema, auxIds) => {
            if (!config) return [];
            /** @type {any[]} */
            const collected = [];
            /** @type {any} */
            const seen = new Set();
            /** @type {any} */
            const schemaCols = colSchema || [];
            /** @type {any} */
            const aux = auxIds || [];
            /** @param {any} [rawId] @returns {any} */
            const pushId = (rawId) => {
                /** @type {any} */
                const candidateId = typeof rawId === 'string' ? rawId : null;
                if (
                    !candidateId
                    || seen.has(candidateId)
                    || candidateId.startsWith('__RowTotal__')
                    || candidateId.startsWith('__sparkline__')
                    || !matchesValueConfigColumnId(candidateId, config)
                ) {
                    return;
                }
                seen.add(candidateId);
                collected.push(candidateId);
            };
            schemaCols.forEach(/** @param {any} [entry] @returns {any} */ (entry) => pushId(entry && entry.id));
            aux.forEach(pushId);
            return collected;
        };
        /** @param {any} [info] @param {any} [config] @param {any} [defaultEditable] @returns {any} */
        const resolveEditableCellState = (info, config, defaultEditable = false) => {
            if (typeof resolveEditorPresentation !== 'function') return null;
            return resolveEditorPresentation(
                getCellRowId(info),
                info && info.column ? info.column.id : null,
                config,
                getResolvedCellValue(info),
                defaultEditable,
            );
        };
        /** @param {any} [info] @param {any} [config] @param {any} [defaultEditable] @returns {any} */
        const renderEditableCell = (info, config, defaultEditable = false) => {
            /** @type {any} */
            const editableState = resolveEditableCellState(info, config, defaultEditable);
            if (!editableState || !editableState.editorConfig || editableState.editorConfig.editable === false) {
                return null;
            }
            return (
                <EditableCell
                    getValue={info.getValue}
                    row={info.row}
                    column={info.column}
                    columnConfig={config}
                    displayValue={getResolvedCellValue(info)}
                    format={getConfigDisplayFormat(config)}
                    numberGroupSeparator={numberGroupSeparator}
                    validationRules={validationRules}
                    onCellEdit={onCellEdit}
                    onEditBlocked={onEditBlocked}
                    handleContextMenu={handleContextMenu}
                    editingDisabled={Boolean(editableState.editingDisabled)}
                    editingDisabledReason={editableState.editingDisabledReason || null}
                    rowEditMode={rowEditMode}
                    editorConfig={editableState.editorConfig}
                    rowEditSession={typeof getRowEditSession === 'function' ? getRowEditSession(getCellRowId(info)) : null}
                    onRequestRowStart={onRequestRowStart}
                    onRowDraftChange={onRowDraftChange}
                    onRequestRowSave={onRequestRowSave}
                    onRequestRowCancel={onRequestRowCancel}
                    requestEditorOptions={requestEditorOptions}
                    editorOptions={editableState.options}
                    editorOptionsLoading={editableState.loading}
                />
            );
        };
        /** @returns {any} */
        const getReportHeaderLabel = () => {
            /** @type {any} */
            const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
            if (!root || typeof root !== 'object') return 'Report';
            if (root.label) return root.label;
            if (root.field) return formatDisplayLabel(root.field);
            return 'Report';
        };
        /** @returns {any} */
        const getTreeHeaderLabel = () => {
            /** @type {any} */
            const labelField = treeConfig && typeof treeConfig === 'object' && typeof treeConfig.labelField === 'string'
                ? treeConfig.labelField
                : '';
            return labelField ? formatDisplayLabel(labelField) : 'Tree';
        };
        /** @returns {any} */
        const getTreeDisplayMode = () => (
            treeConfig && typeof treeConfig === 'object' && treeConfig.displayMode === 'multipleColumns'
                ? 'multipleColumns'
                : 'singleColumn'
        );
        /** @param {any} [level] @returns {any} */
        const getTreeLevelLabel = (level) => {
            /** @type {any} */
            const configuredLabels = treeConfig && Array.isArray(treeConfig.levelLabels)
                ? treeConfig.levelLabels
                : [];
            if (configuredLabels[level]) return configuredLabels[level];
            if (level === 0) return getTreeHeaderLabel();
            return `${getTreeHeaderLabel()} ${level + 1}`;
        };
        /** @type {any} */
        const treeDisplayMode = getTreeDisplayMode();
        /** @type {any} */
        const maxTreeDepth = viewMode === 'tree'
            ? filteredData.reduce(/** @param {any} [maxDepth] @param {any} [row] @returns {any} */ (maxDepth, row) => {
                if (!row || typeof row !== 'object') return maxDepth;
                /** @type {any} */
                const depth = Number.isFinite(Number(row._depth))
                    ? Number(row._depth)
                    : (Number.isFinite(Number(row.depth)) ? Number(row.depth) : 0);
                return Math.max(maxDepth, depth);
            }, 0)
            : 0;
        /** @type {any} */
        const canShowDetailButton = detailMode && detailMode !== 'none' && typeof onToggleDetail === 'function';
        /** @param {any} [row] @returns {any} */
        const renderDetailToggle = (row) => {
            if (!canShowDetailButton || !row || !row.original) return null;
            /** @type {any} */
            const rowPath = row.original._path || row.id;
            /** @type {any} */
            const canDetail = row.original._can_detail !== false;
            if (!canDetail) return null;
            /** @type {any} */
            const isOpen = typeof isDetailOpenForRow === 'function' ? isDetailOpenForRow(rowPath) : false;
            return (
                <button
                    type="button"
                    onClick={(e) => {
                        e.stopPropagation();
                        onToggleDetail(row);
                    }}
                    onMouseDown={(e) => e.stopPropagation()}
                    style={{
                        border: 'none',
                        background: isOpen ? 'rgba(59, 130, 246, 0.14)' : 'transparent',
                        color: isOpen ? (theme.primary || '#2563EB') : theme.textSec,
                        cursor: 'pointer',
                        padding: '2px',
                        marginLeft: '8px',
                        borderRadius: '6px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        flexShrink: 0,
                    }}
                    title={isOpen ? 'Hide detail' : 'Show detail'}
                >
                    <Icons.Visibility />
                </button>
            );
        };

        /** @param {any} [rowData] @returns {any} */
        const getReportRowFormat = (rowData) => (
            pivotMode === 'report'
            && rowData
            && rowData._reportFormat
            && typeof rowData._reportFormat === 'object'
                ? rowData._reportFormat
                : null
        );
        /** @param {any} [rowData] @returns {any} */
        const getReportRowDisplayLabel = (rowData) => {
            if (!rowData) return '';
            return rowData._reportDisplayLabel || rowData._label || rowData._id || '';
        };
        /** @param {any} [rowData] @param {any} [depth] @returns {any} */
        const getReportIndentPx = (rowData, depth) => {
            /** @type {any} */
            const t = getCurrentTheme();
            /** @type {any} */
            const hasCustomIndent = t.treeIndent !== undefined && t.treeIndent !== null && Number.isFinite(Number(t.treeIndent)) && Number(t.treeIndent) >= 0;
            /** @type {any} */
            const reportFormat = getReportRowFormat(rowData);
            /** @type {any} */
            const indent = reportFormat && Number(reportFormat.indent);
            if (hasCustomIndent) {
                return (depth * Number(t.treeIndent)) + (Number.isFinite(indent) ? Math.max(0, indent) : 0);
            }
            return (depth * HIERARCHY_INDENT_PX) + (Number.isFinite(indent) ? Math.max(0, indent) : 0);
        };

        // Helper: render a numeric cell value with decimal precision, scaling, data-bar text priority, and negative/positive coloring
        /** @param {any} [value] @param {any} [fmt] @param {any} [rowPath] @param {any} [colId] @param {any} [rowData] @param {any} [colConfig] @returns {any} */
        const renderNumericCell = (value, fmt, rowPath, colId, rowData = null, colConfig = null) => {
            /** @type {any} */
            const t = getCurrentTheme();
            /** @type {any} */
            const matchedColConfig = colConfig || (colId ? getConfigForColumnId(colId) : null);
            /** @type {any} */
            const reportFormat = getReportRowFormat(rowData);
            /** @type {any} */
            const reportNumberFormat = reportFormat && typeof reportFormat.numberFormat === 'string' && reportFormat.numberFormat.trim()
                ? reportFormat.numberFormat.trim()
                : null;
            /** @type {any} */
            const cellDec = colId !== undefined && columnDecimalOverrides && columnDecimalOverrides[colId] !== undefined
                ? columnDecimalOverrides[colId]
                : decimalPlaces;
            /** @type {any} */
            const effectiveGroupSeparator = colId !== undefined && columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[colId] !== undefined
                ? columnGroupSeparatorOverrides[colId]
                : numberGroupSeparator;
            /** @type {any} */
            const effectiveFormat = colId !== undefined && columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, colId)
                ? columnFormatOverrides[colId]
                : (reportNumberFormat || fmt || defaultValueFormat || null);
            /** @type {any} */
            const effectiveScale = matchedColConfig && matchedColConfig.valueScale !== undefined
                ? matchedColConfig.valueScale
                : valueScale;
            /** @type {any} */
            const effectiveSuffix = matchedColConfig && matchedColConfig.unitSuffix !== undefined
                ? matchedColConfig.unitSuffix
                : unitSuffix;
            /** @type {Record<string, any>} */
            const formatOptions = {
                valueScale: effectiveScale,
                unitSuffix: effectiveSuffix,
            };
            /** @type {any} */
            const formatted = formatValue(value, effectiveFormat, cellDec, effectiveGroupSeparator, formatOptions);
            /** @type {any} */
            const isNegative = isDisplayNegative(value, effectiveFormat, cellDec, formatOptions);
            /** @type {any} */
            const displayNumeric = resolveNumericDisplayValue(value, effectiveFormat, cellDec, formatOptions);
            /** @type {any} */
            const isPositive = typeof displayNumeric === 'number' && Number.isFinite(displayNumeric) && displayNumeric > 0;
            /** @type {any} */
            const cellKey = rowPath && colId ? `${rowPath}:::${colId}` : null;
            /** @type {any} */
            const cellFmt = cellFormatRules && cellKey ? cellFormatRules[cellKey] : null;
            /** @type {any} */
            const activeDataBars = dataBarsColumnsRef && dataBarsColumnsRef.current;
            /** @type {any} */
            const hasDataBar = Boolean(
                activeDataBars && typeof activeDataBars.has === 'function'
                && colId && activeDataBars.has(colId)
                && typeof value === 'number' && !Number.isNaN(value)
                && !(rowData && rowData._isTotal)
            );
            /** @type {any} */
            let colorOverride = null;
            if (cellFmt && cellFmt.color) {
                colorOverride = null;
            } else if (hasDataBar && t.dataBarText !== undefined && t.dataBarText !== null) {
                colorOverride = t.dataBarText === 'inherit' ? null : t.dataBarText;
            } else if (isNegative) {
                colorOverride = t.negativeText || 'red';
            } else if (isPositive && t.positiveText) {
                colorOverride = t.positiveText;
            }
            /** @type {any} */
            const contentStyle = colorOverride
                ? { ...CELL_CONTENT_RESET_STYLE, color: colorOverride }
                : CELL_CONTENT_RESET_STYLE;
            return { formatted, contentStyle };
        };

        /** @param {any} p0 @returns {any} */
        const renderSparklineValueCellContent = ({
            info,
            points,
            config,
            sparklineConfig,
            columnId,
            onDoubleClick = null,
        }) => {
            /** @type {any} */
            const metricValue = resolveSparklineMetricValue(points, sparklineConfig.metric);
            /** @type {any} */
            const deltaMetric = resolveSparklineDeltaValue(points);
            /** @type {any} */
            const rowPath = getCellRowId(info);
            /** @type {any} */
            const { formatted, contentStyle } = renderNumericCell(metricValue, getConfigDisplayFormat(config), rowPath, columnId, info.row && info.row.original, config);
            /** @type {any} */
            const headerLabel = buildSparklineHeader(config, config && config.field ? config.field : columnId);
            return (
                <div
                    data-pivot-sparkline-value-cell="true"
                    data-pivot-sparkline-display="value"
                    data-pivot-sparkline-points={Array.isArray(points) ? points.length : 0}
                    style={{ width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), ...contentStyle }}
                    onDoubleClick={onDoubleClick || undefined}
                    title={buildSparklineCellTitle(
                        headerLabel,
                        points,
                        formatSparklineMetricValue(metricValue, config, columnId),
                        formatSparklineMetricValue(deltaMetric, config, columnId),
                    )}
                >
                    {formatted}
                </div>
            );
        };

        // Enhanced Sorting Logic (Tree-aware + Natural + Customization)
        /** @param {any} [rowA] @param {any} [rowB] @param {any} [columnId] @returns {any} */
        const customSortingFn = (rowA, rowB, columnId) => {
            try {
                // Safety check for loading rows (server-side)
                if (!rowA.original || !rowB.original) return 0;

                // 1. Special handling for grand total - it should always be at the end
                // Check multiple ways to identify the grand total
                /** @type {any} */
                const aIsGrandTotal = isGrandTotalRow(rowA);
                /** @type {any} */
                const bIsGrandTotal = isGrandTotalRow(rowB);

                // If one is grand total and the other is not, grand total goes last
                if (aIsGrandTotal && !bIsGrandTotal) return 1;
                if (!aIsGrandTotal && bIsGrandTotal) return -1;

                // If both are grand totals, they are equal
                if (aIsGrandTotal && bIsGrandTotal) return 0;

                // 2. Regular totals (but not grand total) should come after non-totals
                /** @type {any} */
                const aIsRegularTotal = (rowA.original && rowA.original._isTotal) && !aIsGrandTotal;
                /** @type {any} */
                const bIsRegularTotal = (rowB.original && rowB.original._isTotal) && !bIsGrandTotal;

                if (aIsRegularTotal && !bIsRegularTotal) return 1;
                if (!aIsRegularTotal && bIsRegularTotal) return -1;

                // Both are regular totals (not grand totals) - they can be equal for sorting purposes
                if (aIsRegularTotal && bIsRegularTotal) return 0;

                // 2. Column-Specific Customization
                /** @type {any} */
                const valA = rowA.getValue(columnId);
                /** @type {any} */
                const valB = rowB.getValue(columnId);
                /** @type {any} */
                const colSortOptions = resolveColumnSortOptions(sortOptions, columnId);
                /** @type {any} */
                const isNatural = colSortOptions.naturalSort !== undefined ? colSortOptions.naturalSort : (sortOptions.naturalSort !== false);
                /** @type {any} */
                const isCaseSensitive = colSortOptions.caseSensitive !== undefined ? colSortOptions.caseSensitive : sortOptions.caseSensitive;
                /** @type {any} */
                const useAbsoluteSort = shouldUseAbsoluteSort(sorting, sortOptions, columnId);

                if (useAbsoluteSort) {
                    /** @type {any} */
                    const numA = Number(valA);
                    /** @type {any} */
                    const numB = Number(valB);
                    /** @type {any} */
                    const aIsFinite = Number.isFinite(numA);
                    /** @type {any} */
                    const bIsFinite = Number.isFinite(numB);
                    if (aIsFinite && bIsFinite) {
                        /** @type {any} */
                        const absDiff = Math.abs(numA) - Math.abs(numB);
                        if (absDiff !== 0) return absDiff;
                        return numA - numB;
                    }
                    if (aIsFinite && !bIsFinite) return -1;
                    if (!aIsFinite && bIsFinite) return 1;
                }

                if (isNatural) {
                    /** @type {any} */
                    const sensitivity = isCaseSensitive ? 'variant' : 'base';
                    return new Intl.Collator(undefined, { numeric: true, sensitivity }).compare(String(valA || ''), String(valB || ''));
                }

                // Default Alphanumeric with configured sensitivity
                /** @type {any} */
                const defaultSensitivity = isCaseSensitive ? 'variant' : 'base';
                return String(valA || '').localeCompare(String(valB || ''), undefined, { numeric: true, sensitivity: defaultSensitivity });
            } catch (err) {
                console.error('Sorting error:', err);
                return 0;
            }
        };

        /** @type {any} */
        const sortingFn = serverSide ? 'auto' : customSortingFn;
        /** @type {any[]} */
        const hierarchyCols = [];
        /** @type {any} */
        const canConfigureReportLine = pivotMode === 'report' && showReportConfigColumn !== false && typeof onConfigureReportLine === 'function';

        if (rowCheckboxSelection) {
            /** @param {any} [row] @returns {any} */
            const getRowKey = (row) => (row.original && row.original._path) ? row.original._path : row.id;
            /** @param {any} [row] @returns {any} */
            const isDataRow = (row) => !!(row.original && !row.original._isTotal && row.original._path !== '__grand_total__');
            hierarchyCols.push({
                id: '__row_checkbox__',
                header: '__row_checkbox_header__',
                size: 36,
                minSize: 36,
                maxSize: 36,
                enableSorting: false,
                enableColumnFilter: false,
                enableResizing: false,
                enableHiding: false,
                enablePinning: false,
                meta: { rowCheckbox: true },
                /** @param {any} p0 @returns {any} */
                cell: ({ row }) => {
                    if (!isDataRow(row)) return null;
                    /** @type {any} */
                    const key = getRowKey(row);
                    /** @type {any} */
                    const checked = Boolean(selectedRowKeys && selectedRowKeys.has(key));
                    return (
                        <div style={{
                            width: '100%', height: '100%',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            cursor: 'pointer',
                        }}
                        onClick={(e) => { e.stopPropagation(); if (typeof toggleRowCheckbox === 'function') toggleRowCheckbox(key); }}
                        onKeyDown={(e) => { if (e.key === ' ' || e.key === 'Enter') { e.preventDefault(); if (typeof toggleRowCheckbox === 'function') toggleRowCheckbox(key); } }}
                        role="checkbox"
                        aria-checked={checked}
                        tabIndex={0}
                        >
                            <input
                                type="checkbox"
                                checked={checked}
                                onChange={() => {}}
                                onClick={(e) => { e.stopPropagation(); if (typeof toggleRowCheckbox === 'function') toggleRowCheckbox(key); }}
                                style={{ cursor: 'pointer', width: 14, height: 14 }}
                                tabIndex={-1}
                                aria-hidden="true"
                            />
                        </div>
                    );
                },
            });
        }

        if (showRowNumbers) {
                hierarchyCols.push({
                    id: '__row_number__',
                    header: '#',
                    size: defaultColumnWidths.rowNumber,
                    enableSorting: false,
                    enableColumnFilter: false,
                    enablePinning: false, // User Request: Cannot be changed
                /** @param {any} p0 @returns {any} */
                cell: ({ row }) => (
                    <div
                        style={{
                            width: '100%',
                            height: '100%',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            background: theme.headerSubtleBg || '#f5f5f5',
                            cursor: 'pointer',
                            fontSize: '11px',
                            color: theme.textSec,
                            borderRight: `1px solid ${theme.border}`,
                            userSelect: 'none'
                        }}
                        onMouseDown={(e) => {
                            if (e.button !== 0) return;
                            e.stopPropagation();
                            setIsRowSelecting(true);
                            setRowDragStart(row.index);
                            handleRowSelect(row, e.shiftKey, e.ctrlKey || e.metaKey);
                        }}
                        onMouseEnter={() => {
                            if (isRowSelecting && rowDragStart !== null) {
                                handleRowRangeSelect(rowDragStart, row.index);
                            }
                        }}
                    >
                        {isGrandTotalRow(row)
                            ? (rowCount != null ? rowCount : '∑')
                            : (row.index + 1 + (serverSide ? (renderedOffset || 0) : 0))}
                    </div>
                )
            });
        }

        if (canConfigureReportLine) {
            hierarchyCols.push({
                id: '__report_config__',
                header: '',
                size: 42,
                minSize: 42,
                maxSize: 42,
                enableSorting: false,
                enableColumnFilter: false,
                enableResizing: false,
                enableHiding: false,
                enablePinning: false,
                meta: {
                    reportConfigGutter: true,
                },
                /** @param {any} p0 @returns {any} */
                cell: ({ row }) => {
                    /** @type {any} */
                    const rowData = row.original || null;
                    const isConfigurable = !!(
                        rowData
                        && !rowData._isTotal
                        && !rowData._isOther
                        && rowData._path !== '__grand_total__'
                        && rowData._id !== 'Grand Total'
                    );
                    return (
                        <div
                            style={{
                                width: '100%',
                                height: '100%',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                paddingRight: '8px',
                            }}
                        >
                            {isConfigurable ? (
                                <button
                                    type="button"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        onConfigureReportLine(rowData);
                                    }}
                                    onMouseDown={(e) => e.stopPropagation()}
                                    title="Configure this report line"
                                    aria-label="Configure this report line"
                                    style={{
                                        border: 'none',
                                        background: 'transparent',
                                        cursor: 'pointer',
                                        padding: 0,
                                        width: '26px',
                                        height: '26px',
                                        borderRadius: '7px',
                                        color: theme.textSec,
                                        display: 'inline-flex',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        flexShrink: 0,
                                    }}
                                >
                                    <Icons.Settings />
                                </button>
                            ) : null}
                        </div>
                    );
                },
            });
        }

        if (viewMode === 'tree' && treeDisplayMode === 'multipleColumns') {
            Array.from({ length: Math.max(maxTreeDepth + 1, 1) }).forEach((_, level) => {
                hierarchyCols.push({
                    id: `__tree_level__${level}`,
                    /** @param {any} [row] @returns {any} */
                    accessorFn: row => {
                        /** @type {any} */
                        const depth = Number.isFinite(Number(row && row._depth))
                            ? Number(row._depth)
                            : (Number.isFinite(Number(row && row.depth)) ? Number(row.depth) : 0);
                        return depth === level ? getReportRowDisplayLabel(row) : '';
                    },
                    header: getTreeLevelLabel(level),
                    size: level === 0 ? defaultColumnWidths.hierarchy : defaultColumnWidths.dimension,
                    sortingFn,
                    /** @param {any} p0 @returns {any} */
                    cell: ({ row }) => {
                        /** @type {any} */
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        /** @type {any} */
                        const reportFormat = getReportRowFormat(row.original);
                        /** @type {any} */
                        const showValue = depth === level;
                        /** @type {any} */
                        const showExpander = showValue && row.getCanExpand() && !(row.original && row.original._isTotal);
                        return (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%',
                                    fontWeight: reportFormat && reportFormat.bold ? 700 : resolveDepthFontWeight(row),
                                }}
                            >
                                {showExpander ? (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            debugLog('Toggling expansion (tree-multi) for', row.id);
                                            row.toggleExpanded(!row.getIsExpanded());
                                        }}
                                        onMouseDown={(e) => e.stopPropagation()}
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:getChevronColor(),display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                ) : <span style={{width:'18px', flexShrink: 0}}/>}
                                {showValue ? getReportRowDisplayLabel(row.original) : ''}
                                {showValue && renderDetailToggle(row)}
                                {showValue && typeof renderRowEditActions === 'function' ? renderRowEditActions(row) : null}
                            </div>
                        );
                    }
                });
            });
        } else if (effectiveLayoutMode === 'hierarchy') {
            if (rowFields.length > 0 || viewMode === 'tree') {
                /** @type {any} */
                const resolvedRowHeaderLabel = typeof rowHeaderLabel === 'function'
                    ? rowHeaderLabel(rowFields)
                    : (rowHeaderLabel !== undefined && rowHeaderLabel !== null && String(rowHeaderLabel) !== ''
                        ? String(rowHeaderLabel)
                        : null);
                hierarchyCols.push({
                    id: 'hierarchy',
                    /** @param {any} [row] @returns {any} */
                    accessorFn: row => row._id,
                    header: resolvedRowHeaderLabel !== null
                        ? resolvedRowHeaderLabel
                        : (pivotMode === 'report'
                            ? getReportHeaderLabel()
                            : (viewMode === 'tree'
                                ? getTreeHeaderLabel()
                                : rowFields.map(getFieldLabel).join(' > '))),
                    meta: resolvedRowHeaderLabel !== null ? { preserveHeaderLabel: true } : undefined,
                    size: defaultColumnWidths.hierarchy,
                    sortingFn, // Apply sort
                    /** @param {any} p0 @returns {any} */
                    cell: ({ row }) => {
                        /** @type {any} */
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        /** @type {any} */
                        const rowData = row.original || {};
                        /** @type {any} */
                        const reportFormat = getReportRowFormat(rowData);
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        return (
                            <div
                                style={{
                                    paddingLeft: `${getReportIndentPx(rowData, depth)}px`,
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%'
                                    // isSelected styling will be applied in renderCell
                                }}
                            >
                                 {row.getCanExpand() && !(row.original && row.original._isTotal) ? (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            debugLog('Toggling expansion (hierarchy) for', row.id);
                                            row.toggleExpanded(!row.getIsExpanded());
                                        }}
                                        onMouseDown={(e) => e.stopPropagation()}
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:getChevronColor(),display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                ) : <span style={{width:'18px'}}/>}
                                <span style={{ fontWeight: reportFormat && reportFormat.bold ? 700 : resolveDepthFontWeight(row) }}>
                                    {getReportRowDisplayLabel(row.original)}
                                </span>
                                {renderDetailToggle(row)}
                                {typeof renderRowEditActions === 'function' ? renderRowEditActions(row) : null}
                                {/* Report mode: Top N of M indicator */}
                                {pivotMode === 'report' && row.original && row.original._levelTopN && (
                                    <span style={{
                                        fontSize: '9px',
                                        fontWeight: 500,
                                        color: theme.textSec,
                                        marginLeft: '4px',
                                        whiteSpace: 'nowrap',
                                        flexShrink: 0,
                                    }}>
                                        Top {row.original._levelTopN}{row.original._groupTotalCount ? ` of ${row.original._groupTotalCount}` : ''}
                                    </span>
                                )}
                            </div>
                        );
                    }
                });
            }
        } else {
            rowFields.forEach(/** @param {any} [field] @param {any} [i] @returns {any} */ (field, i) => {
                hierarchyCols.push({
                    id: field,
                    /** @param {any} [rowData] @returns {any} */
                    accessorFn: (rowData) => getTabularRowFieldValue(rowData, field, i),
                    header: getFieldLabel(field),
                    size: defaultColumnWidths.dimension,
                    enablePinning: true,
                    sortingFn,
                    meta: {
                        rowSpan: false,
                        rowSpanGroupIndex: i,
                        rowSpanScope: 'rowFields',
                        preserveAccessorDisplayValue: effectiveLayoutMode === 'tabular',
                    },
                    /** @param {any} p0 @returns {any} */
                    cell: ({ row, getValue }) => {
                        /** @type {any} */
                        const val = getTabularRowFieldValue(row.original, field, i, getValue());
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        /** @type {any} */
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        /** @type {any} */
                        const pathFallbackValue = getTabularPathFieldValue(row.original, field, i, depth);
                        /** @type {any} */
                        const displayValue = isBlankHierarchyValue(val)
                            ? (
                                effectiveLayoutMode === 'tabular' && i <= depth && !isBlankHierarchyValue(pathFallbackValue)
                                    ? pathFallbackValue
                                    : (i === 0 && row.original && row.original._isTotal ? row.original._id : val)
                            )
                            : val;

                        // Outline: Show only if current column matches depth (step layout)
                        // Tabular: Always show every dimension column so labels repeat on every row.
                        let showValue = true;
                        if (effectiveLayoutMode === 'outline') {
                            if (i !== depth) showValue = false;
                        } else {
                            // Tabular: suppress non-first columns only on the grand total row
                            /** @type {any} */
                            const isGrandTotal = isGrandTotalRow(row);
                            if (isGrandTotal && i > 0) showValue = false;
                        }

                        // Expander only on the active level column; never on subtotal/footer rows
                        /** @type {any} */
                        const isSubtotalFooter = row.original && row.original._isSubtotalFooter;
                        /** @type {any} */
                        const isTabularSubtotal = row.original && row.original._isTabularSubtotal;
                        /** @type {any} */
                        const isSubtotalRow = isSubtotalFooter || isTabularSubtotal;
                        /** @type {any} */
                        const showExpander = (i === depth) && row.getCanExpand()
                            && !(row.original && row.original._isTotal)
                            && !isSubtotalRow;

                        return (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%',
                                    fontWeight: isSubtotalRow ? 600 : resolveDepthFontWeight(row),
                                    fontStyle: isSubtotalRow ? 'italic' : undefined,
                                    // isSelected styling will be applied in renderCell
                                }}
                            >
                                {showExpander && (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            debugLog('Toggling expansion (mode) for', row.id);
                                            row.toggleExpanded(!row.getIsExpanded());
                                        }}
                                        onMouseDown={(e) => e.stopPropagation()}
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:getChevronColor(),display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                )}
                                {showValue ? displayValue : ''}
                                {/* Report mode: Top N indicator */}
                                {pivotMode === 'report' && showValue && row.original && row.original._levelTopN && (
                                    <span style={{
                                        fontSize: '9px',
                                        fontWeight: 500,
                                        color: theme.textSec,
                                        marginLeft: '4px',
                                        whiteSpace: 'nowrap',
                                        flexShrink: 0,
                                    }}>
                                        Top {row.original._levelTopN}{row.original._groupTotalCount ? ` of ${row.original._groupTotalCount}` : ''}
                                    </span>
                                )}
                                {showValue && renderDetailToggle(row)}
                                {showValue && i === 0 && typeof renderRowEditActions === 'function' ? renderRowEditActions(row) : null}
                            </div>
                        );
                    }
                });
            });
        }

        /** @type {any[]} */
        let dataCols = [];
        if (viewMode === 'tree') {
            /** @type {any} */
            const ignoreKeys = new Set([
                '_rowKey',
                '_parentKey',
                '_path',
                '_pathFields',
                '_depth',
                'depth',
                '_has_children',
                '_is_expanded',
                '_can_detail',
                '_detail_kind',
                '_label',
                '_id',
                '_reportFormat',
                '_reportDisplayLabel',
                '_reportDebug',
                '__virtualIndex',
                '__col_schema',
                treeConfig && treeConfig.idField,
                treeConfig && treeConfig.parentIdField,
                treeConfig && treeConfig.pathField,
                treeConfig && treeConfig.childrenField,
                treeConfig && treeConfig.labelField,
            ].filter(Boolean));

            /** @type {any[]} */
            const orderedIds = [];
            /** @type {any} */
            const seenIds = new Set();
            /** @param {any} [rawId] @returns {any} */
            const pushId = (rawId) => {
                /** @type {any} */
                const id = typeof rawId === 'string' ? rawId : null;
                if (!id || ignoreKeys.has(id) || seenIds.has(id)) return;
                seenIds.add(id);
                orderedIds.push(id);
            };

            /** @type {any[]} */
            const configuredIds = [
                ...(treeConfig && Array.isArray(treeConfig.valueFields) ? treeConfig.valueFields : []),
                ...(treeConfig && Array.isArray(treeConfig.extraFields) ? treeConfig.extraFields : []),
            ];
            configuredIds.forEach(pushId);
            if (props.columns && props.columns.length > 0) {
                props.columns.forEach(/** @param {any} [column] @returns {any} */ (column) => pushId(column && column.id));
            } else if (filteredData.length > 0) {
                filteredData.forEach(/** @param {any} [row] @returns {any} */ (row) => Object.keys(row || {}).forEach(pushId));
            }

            dataCols = applyMeasureHeaderGroups(orderedIds.filter(columnId => !isSparklineHiddenColumn(columnId)).map((columnId) => {
                /** @type {any} */
                const matchedCfg = getConfigForColumnId(columnId);
                return {
                    id: columnId,
                    /** @param {any} [row] @returns {any} */
                    accessorFn: (row) => row[columnId],
                    header: matchedCfg ? getValHeader(matchedCfg) : formatDisplayLabel(columnId),
                    size: defaultColumnWidths.measure,
                    enablePinning: true,
                    sortingFn,
                    meta: {
                        ...(matchedCfg ? { headerStyle: buildMeasureHeaderStyle(matchedCfg), measureField: matchedCfg.field } : {}),
                        preserveHeaderLabel: Boolean(matchedCfg && matchedCfg.label),
                        shortLabel: matchedCfg && matchedCfg.shortLabel ? String(matchedCfg.shortLabel) : null,
                        headerGroup: resolveMeasureHeaderGroupLabel(matchedCfg, columnId),
                    },
                    /** @param {any} [info] @returns {any} */
                    cell: (info) => {
                        /** @type {any} */
                        const sparklineCell = renderConfiguredInlineSparkline(info, matchedCfg);
                        if (sparklineCell) {
                            return sparklineCell;
                        }
                        /** @type {any} */
                        const editableCell = renderEditableCell(info, null, false);
                        if (editableCell) {
                            return editableCell;
                        }
                        /** @type {any} */
                        const value = getResolvedCellValue(info);
                        /** @type {any} */
                        const rowPath = getCellRowId(info);
                        if (typeof value === 'number') {
                            if (Number.isNaN(value)) {
                                return <div style={{ width:'100%', height:'100%' }} />;
                            }
                            /** @type {any} */
                            const { formatted, contentStyle } = renderNumericCell(value, matchedCfg ? getConfigDisplayFormat(matchedCfg) : null, rowPath, info.column.id, info.row && info.row.original, matchedCfg);
                            return (
                                <div style={{ width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), ...contentStyle }}>
                                    {formatted}
                                </div>
                            );
                        }
                        return (
                            <div style={{ width:'100%', height:'100%', display:'flex', alignItems:'center', paddingRight:getNumericPaddingRight() }}>
                                {value !== undefined && value !== null && String(value) !== 'NaN' ? String(value) : ''}
                            </div>
                        );
                    },
                };
            }));
        } else if (colFields.length === 0) {
            if (measureAxisPlacement === 'rows' && measureAxisValueField) {
                dataCols = [{
                    id: measureAxisValueField,
                    /** @param {any} [row] @returns {any} */
                    accessorFn: row => row[measureAxisValueField],
                    header: measureAxisValueField,
                    size: defaultColumnWidths.measure,
                    enablePinning: true,
                    sortingFn,
                    enableSorting: true,
                    /** @param {any} [info] @returns {any} */
                    cell: info => {
                        /** @type {any} */
                        const rowConfig = getMeasureAxisRowValueConfig(info.row && info.row.original);
                        /** @type {any} */
                        const sparklineCell = renderConfiguredInlineSparkline(info, rowConfig);
                        if (sparklineCell) {
                            return sparklineCell;
                        }
                        /** @type {any} */
                        const value = getResolvedCellValue(info);
                        /** @type {any} */
                        const rowPath = getCellRowId(info);
                        /** @type {any} */
                        const { formatted, contentStyle } = renderNumericCell(value, getConfigDisplayFormat(rowConfig), rowPath, info.column.id, info.row && info.row.original);
                        return (
                            <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                                {formatted}
                            </div>
                        );
                    }
                }];
            } else {
                dataCols = applyMeasureHeaderGroups(valConfigs.filter(/** @param {any} [c] @returns {any} */ c => !isSparklineHiddenColumn(getValKey(c))).map(/** @param {any} [c] @returns {any} */ c => {
                /** @type {any} */
                const fsc = getSparklineConfig(c);
                /** @type {any} */
                const isFieldSpark = Boolean(fsc && fsc.source === 'field');
                /** @type {any} */
                const isFieldSparkValueMode = Boolean(isFieldSpark && fsc.displayMode === 'value');
                // Use the agg-suffixed key so it matches what the backend returns
                // (e.g. price_norm_history_last). Fall back to bare field name when
                // data is passed directly without a backend aggregation step.
                /** @type {any} */
                const colId = getValKey(c);
                return {
                    id: colId,
                    /** @param {any} [row] @returns {any} */
                    accessorFn: row => {
                        /** @type {any} */
                        let v = row && row[colId] !== undefined ? row[colId] : undefined;
                        if (v === undefined && row && c.agg === 'formula' && !c.id && !c.alias && c.field) {
                            v = row[`${c.field}_formula`] !== undefined ? row[`${c.field}_formula`] : row[c.field];
                        }
                        /** @type {any} */
                        const rawFieldValue = row && row[c.field] !== undefined ? row[c.field] : undefined;
                        /** @type {any} */
                        const rawValue = (isFieldSpark && (v === undefined || v === null)) ? rawFieldValue : v;
                        return isFieldSparkValueMode
                            ? resolveSparklineMetricValue(normalizeSparklinePoints(rawValue), fsc.metric)
                            : rawValue;
                    },
                    header: isFieldSpark
                        ? (isFieldSparkValueMode ? (c.label || getValHeader(c)) : buildSparklineHeader(c, c.field))
                        : getValHeader(c),
                    size: isFieldSpark && !isFieldSparkValueMode ? defaultColumnWidths.measure + 64 : defaultColumnWidths.measure,
                    enablePinning: true,
                    sortingFn,
                    enableSorting: !isFieldSpark || isFieldSparkValueMode,
                    meta: {
                        headerStyle: buildMeasureHeaderStyle(c),
                        measureField: c.field,
                        preserveHeaderLabel: Boolean(c.label),
                        shortLabel: c.shortLabel ? String(c.shortLabel) : null,
                        headerGroup: resolveMeasureHeaderGroupLabel(c, colId),
                    },
                    /** @param {any} [info] @returns {any} */
                    cell: info => {
                        if (isFieldSpark) {
                            /** @type {any} */
                            const points = normalizeSparklinePoints(getFieldSparklineRawValue(info, c, colId));
                            if (points.length > 0) {
                                /** @param {any} [event] @returns {any} */
                                const openDataModal = (event) => {
                                    if (event && typeof event.stopPropagation === 'function') {
                                        event.stopPropagation();
                                    }
                                    if (openSparklineDataModalRef && openSparklineDataModalRef.current) {
                                        openSparklineDataModalRef.current({
                                            points,
                                            headerLabel: buildSparklineHeader(c, c.field),
                                            config: c,
                                            sparklineConfig: fsc,
                                        });
                                    }
                                };
                                if (isFieldSparkValueMode) {
                                    return renderSparklineValueCellContent({
                                        info,
                                        points,
                                        config: c,
                                        sparklineConfig: fsc,
                                        columnId: colId,
                                        onDoubleClick: openDataModal,
                                    });
                                }
                                return (
                                    <div
                                        style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center' }}
                                        onDoubleClick={openDataModal}
                                        title="Double-click to view data"
                                    >
                                        {renderSparklineCellContent({
                                            info, points, config: c, sparklineConfig: fsc,
                                            columnId: colId, compact: false,
                                        })}
                                    </div>
                                );
                            }
                            return null;
                        }
                        /** @type {any} */
                        const sparklineCell = renderConfiguredInlineSparkline(info, c);
                        if (sparklineCell) {
                            return sparklineCell;
                        }
                        /** @type {any} */
                        const editableCell = renderEditableCell(info, c, isEditableMeasureConfig(c, info.column.id));
                        if (editableCell) {
                            return editableCell;
                        }
                        /** @type {any} */
                        const value = getResolvedCellValue(info);
                        /** @type {any} */
                        const rowPath = getCellRowId(info);
                        /** @type {any} */
                        const { formatted, contentStyle } = renderNumericCell(value, getConfigDisplayFormat(c), rowPath, info.column.id, info.row && info.row.original, c);
                        return (
                            <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                                {formatted}
                            </div>
                        );
                    }
                };
            }));
            }
        } else if (serverSide) {
            /** @type {any} */
            const ignoreKeys = new Set(['_id', 'depth', '_isTotal', '_path', '_pathFields', 'uuid', ...rowFields, ...colFields]);
            /** @type {any} */
            const measureIds = new Set(valConfigs.map(/** @param {any} [v] @returns {any} */ v => getValKey(v)));
            /** @param {any} [columnId] @returns {any} */
            const isRelevantColumnId = (columnId) => {
                if (!columnId || ignoreKeys.has(columnId)) return false;
                if (isMeasureAxisValueColumnId(columnId)) return true;
                if (measureIds.has(columnId)) return true;
                if (columnId.startsWith('__RowTotal__')) return true;
                return Boolean(getConfigForColumnId(columnId));
            };

            /** @param {any} [columnId] @param {any} [sizeOverride] @returns {any} */
            const buildServerValueColumn = (columnId, sizeOverride = defaultColumnWidths.subtotal) => {
                if (!isRelevantColumnId(columnId)) return null;
                if (isSparklineHiddenColumn(columnId)) return null;
                /** @type {any} */
                const columnConfig = getConfigForColumnId(columnId);
                /** @type {any} */
                const columnSparklineConfig = columnConfig && getSparklineConfig(columnConfig);
                /** @type {any} */
                const isColumnFieldSparkline = Boolean(columnSparklineConfig && columnSparklineConfig.source === 'field');
                /** @type {any} */
                const isColumnFieldSparklineValueMode = Boolean(isColumnFieldSparkline && columnSparklineConfig.displayMode === 'value');
                // Use a wider default for field-array sparkline columns
                /** @type {any} */
                const effectiveSize = isColumnFieldSparkline && !isColumnFieldSparklineValueMode && sizeOverride === defaultColumnWidths.subtotal
                    ? defaultColumnWidths.subtotal + 64
                    : sizeOverride;
                return {
                    id: columnId,
                    /** @param {any} [row] @returns {any} */
                    accessorFn: row => {
                        /** @type {any} */
                        const rawValue = row && row[columnId] !== undefined ? row[columnId] : undefined;
                        return isColumnFieldSparklineValueMode
                            ? resolveSparklineMetricValue(normalizeSparklinePoints(rawValue), columnSparklineConfig.metric)
                            : rawValue;
                    },
                    header: columnConfig
                        ? (isColumnFieldSparkline
                            ? (isColumnFieldSparklineValueMode ? (columnConfig.label || getValHeader(columnConfig)) : buildSparklineHeader(columnConfig, columnConfig.field))
                            : getValHeader(columnConfig))
                        : columnId,
                    size: Number.isFinite(Number(effectiveSize)) ? Number(effectiveSize) : defaultColumnWidths.subtotal,
                    sortingFn,
                    enableSorting: !isColumnFieldSparkline || isColumnFieldSparklineValueMode,
                    meta: {
                        headerStyle: buildMeasureHeaderStyle(columnConfig),
                        measureField: columnConfig && columnConfig.field ? columnConfig.field : undefined,
                        preserveHeaderLabel: Boolean(columnConfig && columnConfig.label),
                        shortLabel: columnConfig && columnConfig.shortLabel ? String(columnConfig.shortLabel) : null,
                        headerGroup: resolveMeasureHeaderGroupLabel(columnConfig, columnId),
                    },
                    /** @param {any} [info] @returns {any} */
                    cell: info => {
                        /** @type {any} */
                        const matchedConfig = getMeasureAxisRowValueConfig(info.row && info.row.original) || columnConfig;
                        /** @type {any} */
                        const fieldSparklineConfig = matchedConfig && getSparklineConfig(matchedConfig);
                        /** @type {any} */
                        const isFieldSparkline = Boolean(fieldSparklineConfig && fieldSparklineConfig.source === 'field');
                        /** @type {any} */
                        const isFieldSparklineValueMode = Boolean(isFieldSparkline && fieldSparklineConfig.displayMode === 'value');
                        // Field-array sparkline: cell value IS the series, render full-size
                        if (isFieldSparkline) {
                            /** @type {any} */
                            const points = normalizeSparklinePoints(getFieldSparklineRawValue(info, matchedConfig, columnId));
                            if (points.length > 0) {
                                /** @param {any} [event] @returns {any} */
                                const openDataModal = (event) => {
                                    if (event && typeof event.stopPropagation === 'function') {
                                        event.stopPropagation();
                                    }
                                    if (openSparklineDataModalRef && openSparklineDataModalRef.current) {
                                        openSparklineDataModalRef.current({
                                            points,
                                            headerLabel: buildSparklineHeader(matchedConfig, columnId),
                                            config: matchedConfig,
                                            sparklineConfig: fieldSparklineConfig,
                                        });
                                    }
                                };
                                if (isFieldSparklineValueMode) {
                                    return renderSparklineValueCellContent({
                                        info,
                                        points,
                                        config: matchedConfig,
                                        sparklineConfig: fieldSparklineConfig,
                                        columnId,
                                        onDoubleClick: openDataModal,
                                    });
                                }
                                return (
                                    <div
                                        style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center' }}
                                        onDoubleClick={openDataModal}
                                        title="Double-click to view data"
                                    >
                                        {renderSparklineCellContent({
                                            info,
                                            points,
                                            config: matchedConfig,
                                            sparklineConfig: fieldSparklineConfig,
                                            columnId,
                                            compact: false,
                                        })}
                                    </div>
                                );
                            }
                            return null;
                        }
                        /** @type {any} */
                        const sparklineCell = renderConfiguredInlineSparkline(info, matchedConfig);
                        if (sparklineCell) {
                            return sparklineCell;
                        }
                        /** @type {any} */
                        const value = getResolvedCellValue(info);
                        /** @type {any} */
                        const editableCell = renderEditableCell(info, matchedConfig, isEditableMeasureConfig(matchedConfig, columnId));
                        if (editableCell) {
                            return editableCell;
                        }
                        /** @type {any} */
                        const fmt = getConfigDisplayFormat(matchedConfig);
                        /** @type {any} */
                        const rowPath = getCellRowId(info);
                        /** @type {any} */
                        const { formatted, contentStyle } = renderNumericCell(value, fmt, rowPath, info.column.id, info.row && info.row.original, matchedConfig);
                        return (
                            <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                                {formatted}
                            </div>
                        );
                    }
                };
            };

            /** @param {any} [schemaIndex] @param {any} [sizeOverride] @returns {any} */
            const buildPlaceholderColumn = (schemaIndex, sizeOverride = defaultColumnWidths.schemaFallback) => ({
                id: `__schema_placeholder__${schemaIndex}`,
                header: '',
                headerVal: '',
                accessorFn: () => '',
                size: Number.isFinite(Number(sizeOverride)) ? Number(sizeOverride) : defaultColumnWidths.schemaFallback,
                enableSorting: false,
                enableResizing: false,
                meta: {
                    isSchemaPlaceholder: true,
                    schemaIndex,
                },
                cell: () => <div style={{width:'100%', height:'100%'}} />,
            });

            /** @param {any} [cols] @returns {any} */
            const buildRecursiveTree = (cols) => {
                /** @type {Record<string, any>} */
                const root = { columns: [] };
                cols.forEach(/** @param {any} [col] @returns {any} */ col => {
                    const key = col.id;
                    if (!key) return;
                    // Sparkline summary columns are independent leaves — bypass hierarchy parsing
                    if (key.startsWith('__sparkline__')) {
                        root.columns.push({ ...col, enablePinning: true });
                        return;
                    }
                    let dimStr = key;
                    let measureStr = "";
                    /** @type {any} */
                    const matchedConfig = getConfigForColumnId(key);
                    if (matchedConfig) {
                        /** @type {any} */
                        const matchedSparklineConfig = getSparklineConfig(matchedConfig);
                        /** @type {any} */
                        const exactKeys = matchedConfig.agg === 'formula'
                            ? [
                                matchedConfig.alias,
                                matchedConfig.id,
                                matchedConfig.field ? `${matchedConfig.field}_formula` : null,
                                matchedConfig.field,
                              ].filter(Boolean)
                            : [
                                matchedConfig.alias,
                                matchedConfig.id,
                                matchedConfig.field ? `${matchedConfig.field}_${matchedConfig.agg}` : null,
                              ].filter(Boolean);
                        /** @type {any} */
                        const candidateSuffixes = exactKeys.map(/** @param {any} [entry] @returns {any} */ (entry) => `_${entry}`);
                        if (
                            matchedConfig.formulaScope === 'columns' ||
                            exactKeys.some(/** @param {any} [entry] @returns {any} */ (entry) => key.toLowerCase() === String(entry).toLowerCase())
                        ) {
                            dimStr = '';
                        } else {
                            /** @type {any} */
                            const matchedSuffix = candidateSuffixes.find(/** @param {any} [s] @returns {any} */ (s) => key.toLowerCase().endsWith(s.toLowerCase()));
                            if (matchedSuffix) {
                                dimStr = key.substring(0, key.length - matchedSuffix.length);
                            }
                        }
                        measureStr = matchedSparklineConfig && matchedSparklineConfig.source === 'field'
                            ? (matchedSparklineConfig.displayMode === 'value'
                                ? (matchedConfig.label || formatDisplayLabel(matchedConfig.field))
                                : buildSparklineHeader(matchedConfig, matchedConfig.field))
                            : getValHeader(matchedConfig);
                    } else if (isMeasureAxisValueColumnId(key)) {
                        const suffix = `_${measureAxisValueField}`;
                        if (key.endsWith(suffix)) {
                            dimStr = key.substring(0, key.length - suffix.length);
                        }
                        measureStr = measureAxisValueField;
                    }
                    if (!matchedConfig) {
                         /** @type {any} */
                         const parts = key.split('_');
                         if (parts.length > 1 && !isMeasureAxisValueColumnId(key)) {
                             dimStr = parts.slice(0, parts.length - 2).join('_');
                             measureStr = parts.slice(parts.length - 2).join(' ');
                             if (!dimStr) dimStr = "Total";
                         }
                    }
                    /** @type {any} */
                    const dimPath = dimStr ? dimStr.split('|') : [];
                    let current = root;
                    let pathKey = '';
                    let parentCollapsed = false;
                    for (let idx = 0; idx < dimPath.length; idx++) {
                        /** @type {any} */
                        const val = dimPath[idx].trim();
                        if (idx > 0 && !isColExpanded(pathKey)) {
                            parentCollapsed = true;
                            break;
                        }
                        /** @type {any} */
                        const currentPathKey = pathKey ? `${pathKey}|||${val}` : val;
                        pathKey = currentPathKey;
                        /** @type {any} */
                        let node = current.columns.find(/** @param {any} [c] @returns {any} */ c => c.groupValue === val);
                        if (!node) {
                            node = {
                                id: `group_${currentPathKey}`,
                                groupValue: val,
                                headerVal: formatDisplayLabel(val),
                                header: (
                                    <div style={{display:'flex', alignItems:'center', gap:4, width:'100%', overflow:'hidden'}}>
                                        <span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}} title={formatDisplayLabel(val)}>{formatDisplayLabel(val)}</span>
                                        <span onClick={(e) => { e.stopPropagation(); toggleCol(currentPathKey); }} style={{cursor:'pointer', display:'flex', opacity:0.6, flexShrink:0}}>
                                            {isColExpanded(currentPathKey) ? <Icons.ColCollapse/> : <Icons.ColExpand/>}
                                        </span>
                                    </div>
                                ),
                                columns: [],
                                enablePinning: true
                            };
                            current.columns.push(node);
                        }
                        current = node;
                    }
                    if (parentCollapsed) {
                         if (current.columns.length === 0) {
                             current.columns.push({
                                id: pathKey + "_collapsed",
                                header: "...",
                                size: defaultColumnWidths.collapsedPlaceholder,
                                accessorFn: () => "",
                                cell: () => <div style={{color:'#999', textAlign:'center'}}>...</div>
                            });
                         }
                         return;
                    }
                    if (isColExpanded(pathKey) || dimPath.length === 0) {
                        /** @type {Record<string, any>} */
                        const newCol = { ...col, header: measureStr || col.header, enablePinning: true };
                        if (resolveEditableCellState({ column: { id: key }, getValue: () => undefined }, matchedConfig, isEditableMeasureConfig(matchedConfig, key))) {
                            newCol.cell = /** @param {any} [info] @returns {any} */ info => {
                                /** @type {any} */
                                const editableCell = renderEditableCell(info, matchedConfig, isEditableMeasureConfig(matchedConfig, key));
                                if (editableCell) return editableCell;
                                return col.cell ? col.cell(info) : null;
                            };
                        }
                        current.columns.push(newCol);
                    } else if (current.columns.length === 0) {
                         current.columns.push({
                            id: pathKey + "_collapsed",
                            header: "...",
                            size: defaultColumnWidths.collapsedPlaceholder,
                            accessorFn: () => "",
                            cell: () => <div style={{color:'#999', textAlign:'center'}}>...</div>
                        });
                    }
                });
                /** @param {any} [colsList] @param {any} [prefix] @returns {any} */
                const applyGroupsRecursively = (colsList, prefix) => {
                    /** @type {any} */
                    const withSubGroups = (Array.isArray(colsList) ? colsList : []).map((node) => (
                        node && Array.isArray(node.columns)
                            ? { ...node, columns: applyGroupsRecursively(node.columns, node.id || prefix) }
                            : node
                    ));
                    return applyMeasureHeaderGroups(withSubGroups, prefix);
                };
                return applyGroupsRecursively(root.columns, 'pivot');
            };

            /** @param {any} [column] @returns {any} */
            const decorateRowTotalColumn = (column) => {
                if (!column) return null;
                /** @type {Record<string, any>} */
                const decorated = { ...column };
                if (typeof decorated.header === 'string' && decorated.header.startsWith('__RowTotal__')) {
                    decorated.header = decorated.header.replace('__RowTotal__', 'Total ');
                }
                decorated.cell = /** @param {any} [info] @returns {any} */ info => {
                    /** @type {any} */
                    const config = getConfigForColumnId(decorated.id)
                        || valConfigs.find(/** @param {any} [v] @returns {any} */ v => v && v.field && decorated.id.includes(v.field));
                    /** @type {any} */
                    const value = getResolvedCellValue(info);
                    /** @type {any} */
                    const rowPath = getCellRowId(info);
                    /** @type {any} */
                    const { formatted, contentStyle } = renderNumericCell(value, getConfigDisplayFormat(config), rowPath, info.column.id, info.row && info.row.original, config);
                    return (
                        <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:getNumericPaddingRight(), fontWeight:'bold', ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                            {formatted}
                        </div>
                    );
                };
                return decorated;
            };

            /** @type {any[]} */
            const auxiliaryIds = [];
            /** @type {any} */
            const seenAuxiliaryIds = new Set();
            /** @param {any} [columnId] @returns {any} */
            const isRowTotalColumnId = (columnId) => (
                typeof columnId === 'string' && columnId.startsWith('__RowTotal__')
            );
            /** @param {any} [rawId] @returns {any} */
            const pushAuxiliaryId = (rawId) => {
                /** @type {any} */
                const id = typeof rawId === 'string' ? rawId : null;
                if (!id || id === '__col_schema' || id.startsWith('_report') || seenAuxiliaryIds.has(id)) return;
                seenAuxiliaryIds.add(id);
                auxiliaryIds.push(id);
            };

            if (serverSide && Array.isArray(responseColumns) && responseColumns.length > 0) {
                responseColumns.forEach(c => pushAuxiliaryId(c && c.id));
            } else if (props.columns && props.columns.length > 0) {
                props.columns.forEach(/** @param {any} [c] @returns {any} */ c => pushAuxiliaryId(c && c.id));
            } else if (filteredData.length > 0) {
                filteredData.forEach(/** @param {any} [row] @returns {any} */ row => Object.keys(row || {}).forEach(pushAuxiliaryId));
            }

            /** @type {any} */
            const rowTotalCols = auxiliaryIds
                .filter(isRowTotalColumnId)
                .map(columnId => decorateRowTotalColumn(buildServerValueColumn(columnId)))
                .filter(Boolean);

            /** @type {any} */
            const schemaColumns = cachedColSchema && Array.isArray(cachedColSchema.columns)
                ? cachedColSchema.columns
                : [];
            /** @type {any} */
            const sparklineColumnLabelById = new Map();
            schemaColumns.forEach(/** @param {any} [entry] @returns {any} */ (entry) => {
                if (entry && entry.id) {
                    sparklineColumnLabelById.set(entry.id, extractColumnHeaderText(entry, entry.id));
                }
            });
            if (props.columns && props.columns.length > 0) {
                props.columns.forEach(/** @param {any} [entry] @returns {any} */ (entry) => {
                    if (entry && entry.id && !sparklineColumnLabelById.has(entry.id)) {
                        sparklineColumnLabelById.set(entry.id, extractColumnHeaderText(entry, entry.id));
                    }
                });
            }
            /** @type {any} */
            const totalSchemaColumns = cachedColSchema && Number.isFinite(Number(cachedColSchema.total_center_cols))
                ? Math.max(0, Math.floor(Number(cachedColSchema.total_center_cols)))
                : schemaColumns.length;
            /** @type {any} */
            const hasSparseSchemaCatalog = totalSchemaColumns > 0 && schemaColumns.length >= totalSchemaColumns;

            /** @returns {any} */
            const buildSparklineSummaryColumns = () => {
                /** @type {any[]} */
                const summaryColumns = [];
                (Array.isArray(valConfigs) ? valConfigs : []).forEach((config) => {
                    /** @type {any} */
                    const sparklineConfig = getSparklineConfig(config);
                    if (!sparklineConfig) return;
                    // Field-array sparklines render inline from cell value — no pivot collection needed
                    if (sparklineConfig.source === 'field') return;
                    // Pivot-source sparklines are linked summary columns. hideColumns only
                    // controls whether the numeric source columns remain visible.
                    /** @type {any} */
                    const sourceColumnIds = resolveSparklineSourceColumnIds(config, schemaColumns, auxiliaryIds);
                    if (sourceColumnIds.length === 0) return;
                    const sparklineColumnId = `__sparkline__${getValKey(config)}`;
                    /** @type {any} */
                    const headerLabel = buildSparklineHeader(config, config && config.field ? config.field : sparklineColumnId);
                    /** @type {Record<string, any>} */
                    const col = {
                        id: sparklineColumnId,
                        header: headerLabel,
                        /** @param {any} [row] @returns {any} */
                        accessorFn: (row) => resolveSparklineMetricValue(
                            buildPivotSparklinePoints({
                                rowData: row,
                                columnIds: sourceColumnIds,
                                /** @param {any} [sourceColumnId] @param {any} [rawValue] @returns {any} */
                                resolveValue: (sourceColumnId, rawValue) => {
                                    /** @type {any} */
                                    const rowPath = row && row._path ? row._path : null;
                                    return typeof resolveCellDisplayValue === 'function'
                                        ? resolveCellDisplayValue(rowPath, sourceColumnId, rawValue)
                                        : rawValue;
                                },
                                /** @param {any} [sourceColumnId] @returns {any} */
                                resolveLabel: (sourceColumnId) => sparklineColumnLabelById.get(sourceColumnId) || extractColumnHeaderText(sourceColumnId, sourceColumnId),
                            }),
                            sparklineConfig.metric,
                        ),
                        size: defaultColumnWidths.subtotal + 64,
                        enablePinning: true,
                        enableSorting: false,
                        meta: {
                            isSparklineSummary: true,
                            sparklineSourceColumnIds: sourceColumnIds,
                            sparklineConfig,
                            sparklineValueConfig: config,
                            headerStyle: buildMeasureHeaderStyle(config),
                        },
                        /** @param {any} [info] @returns {any} */
                        cell: (info) => {
                            /** @type {any} */
                            const rowData = info && info.row ? info.row.original : null;
                            /** @type {any} */
                            const rowPath = getCellRowId(info);
                            /** @type {any} */
                            const points = buildPivotSparklinePoints({
                                rowData,
                                columnIds: sourceColumnIds,
                                /** @param {any} [sourceColumnId] @param {any} [rawValue] @returns {any} */
                                resolveValue: (sourceColumnId, rawValue) => (
                                    typeof resolveCellDisplayValue === 'function'
                                        ? resolveCellDisplayValue(rowPath, sourceColumnId, rawValue)
                                        : rawValue
                                ),
                                /** @param {any} [sourceColumnId] @returns {any} */
                                resolveLabel: (sourceColumnId) => sparklineColumnLabelById.get(sourceColumnId) || extractColumnHeaderText(sourceColumnId, sourceColumnId),
                            });
                            /** @type {any} */
                            const content = renderSparklineCellContent({
                                info,
                                points,
                                config,
                                sparklineConfig,
                                columnId: sparklineColumnId,
                                compact: false,
                            });
                            return (
                                <div
                                    style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center' }}
                                    onDoubleClick={(event) => {
                                        if (event && typeof event.stopPropagation === 'function') {
                                            event.stopPropagation();
                                        }
                                        if (openSparklineDataModalRef && openSparklineDataModalRef.current) {
                                            openSparklineDataModalRef.current({
                                                points,
                                                headerLabel,
                                                config,
                                                sparklineConfig,
                                            });
                                        }
                                    }}
                                    title="Double-click to view data"
                                >
                                    {content}
                                </div>
                            );
                        },
                    };
                    summaryColumns.push(col);
                });
                return summaryColumns;
            };
            /** @type {any} */
            const sparklineSummaryCols = buildSparklineSummaryColumns();
            /** @type {any} */
            const sparklineSummaryMap = new Map(sparklineSummaryCols.map(/** @param {any} [col] @returns {any} */ col => [col.id, col]));

            // Build trigger maps so column-building loops can inject sparklines at the right slot.
            // placement='before' -> inject immediately before the first matching source column
            // placement='after'  -> legacy saved views; normalized configs no longer emit it
            // placement='end'    -> legacy fallback for configs without matching source columns
            /** @type {any} */
            const sparklineBeforeTriggers = new Map(); // firstMatchId -> [sparklineColumnId, ...]
            /** @type {any} */
            const sparklineAfterTriggers  = new Map(); // lastMatchId  -> [sparklineColumnId, ...]
            /** @type {any[]} */
            const sparklineEndList        = [];        // placement='end' or no source columns found
            sparklineSummaryMap.forEach(/** @param {any} [col] @param {any} [sparklineColumnId] @returns {any} */ (col, sparklineColumnId) => {
                /** @type {any} */
                const sparklineConfig = col.meta && col.meta.sparklineConfig;
                if (!sparklineConfig) return;
                /** @type {any} */
                const sourceColumnIds = col.meta && col.meta.sparklineSourceColumnIds ? col.meta.sparklineSourceColumnIds : [];
                /** @type {any} */
                const placement = sparklineConfig.placement || 'before';
                /** @type {any} */
                const firstMatchId = sourceColumnIds.length > 0 ? sourceColumnIds[0] : null;
                /** @type {any} */
                const lastMatchId  = sourceColumnIds.length > 0 ? sourceColumnIds[sourceColumnIds.length - 1] : null;
                if (placement === 'before' && firstMatchId) {
                    if (!sparklineBeforeTriggers.has(firstMatchId)) sparklineBeforeTriggers.set(firstMatchId, []);
                    sparklineBeforeTriggers.get(firstMatchId).push(sparklineColumnId);
                } else if (placement === 'after' && lastMatchId) {
                    if (!sparklineAfterTriggers.has(lastMatchId)) sparklineAfterTriggers.set(lastMatchId, []);
                    sparklineAfterTriggers.get(lastMatchId).push(sparklineColumnId);
                } else {
                    sparklineEndList.push(sparklineColumnId);
                }
            });

            if (hasSparseSchemaCatalog) {
                /** @type {any[]} */
                const sparseDataCols = [];
                /** @type {any[]} */
                let loadedSegment = [];
                /** @returns {any} */
                const flushLoadedSegment = () => {
                    if (loadedSegment.length === 0) return;
                    sparseDataCols.push(...buildRecursiveTree(loadedSegment));
                    loadedSegment = [];
                };
                /** @type {any} */
                const injectedSparklines = new Set();
                /** @param {any} [sparklineColumnIds] @returns {any} */
                const injectSparklineCols = (sparklineColumnIds) => {
                    (sparklineColumnIds || []).forEach(/** @param {any} [sparklineColumnId] @returns {any} */ sparklineColumnId => {
                        if (injectedSparklines.has(sparklineColumnId)) return;
                        /** @type {any} */
                        const sparklineCol = sparklineSummaryMap.get(sparklineColumnId);
                        if (sparklineCol) {
                            sparseDataCols.push(sparklineCol);
                            injectedSparklines.add(sparklineColumnId);
                        }
                    });
                };

                for (let index = 0; index < totalSchemaColumns; index += 1) {
                    const schemaEntry = schemaColumns[index];
                    /** @type {any} */
                    const schemaSize = schemaEntry && Number.isFinite(Number(schemaEntry.size))
                        ? Number(schemaEntry.size)
                        : defaultColumnWidths.schemaFallback;
                    if (schemaEntry && typeof schemaEntry.id === 'string' && schemaEntry.id) {
                        const entryId = schemaEntry.id;
                        // Skip sparkline-hidden columns entirely — no placeholder slot
                        if (isSparklineHiddenColumn(entryId)) continue;
                        // Row totals are rendered as auxiliary columns after the pivot tree.
                        // If they fall through here they become blank schema placeholders
                        // immediately before the real total columns.
                        if (isRowTotalColumnId(entryId)) {
                            flushLoadedSegment();
                            continue;
                        }
                        // Inject 'before' sparklines immediately before the first matching column
                        if (sparklineBeforeTriggers.has(entryId)) {
                            flushLoadedSegment();
                            injectSparklineCols(sparklineBeforeTriggers.get(entryId));
                        }
                        /** @type {any} */
                        const valueColumn = buildServerValueColumn(entryId, schemaSize);
                        if (valueColumn && !isRowTotalColumnId(valueColumn.id)) {
                            loadedSegment.push(valueColumn);
                            // Inject 'after' sparklines immediately after the last matching column
                            if (sparklineAfterTriggers.has(entryId)) {
                                flushLoadedSegment();
                                injectSparklineCols(sparklineAfterTriggers.get(entryId));
                            }
                            continue;
                        }
                    }
                    flushLoadedSegment();
                    sparseDataCols.push(buildPlaceholderColumn(index, schemaSize));
                }

                flushLoadedSegment();
                // Inject 'end' sparklines and any not yet placed (e.g. hideColumns=true with no visible source)
                injectSparklineCols(sparklineEndList);
                sparklineSummaryMap.forEach(/** @param {any} [sparklineCol] @param {any} [sparklineColumnId] @returns {any} */ (sparklineCol, sparklineColumnId) => {
                    if (!injectedSparklines.has(sparklineColumnId)) {
                        sparseDataCols.push(sparklineCol);
                        injectedSparklines.add(sparklineColumnId);
                    }
                });

                dataCols = [...sparseDataCols, ...rowTotalCols];
            } else {
                /** @type {any[]} */
                const orderedIds = [];
                /** @type {any} */
                const seenIds = new Set();
                /** @param {any} [rawId] @returns {any} */
                const pushId = (rawId) => {
                    /** @type {any} */
                    const id = typeof rawId === 'string' ? rawId : null;
                    if (!id || id === '__col_schema' || id.startsWith('_report') || seenIds.has(id)) return;
                    seenIds.add(id);
                    orderedIds.push(id);
                };

                if (serverSide && Array.isArray(responseColumns) && responseColumns.length > 0) {
                    responseColumns.forEach(c => pushId(c && c.id));
                } else if (props.columns && props.columns.length > 0) {
                    props.columns.forEach(/** @param {any} [c] @returns {any} */ c => pushId(c && c.id));
                } else if (filteredData.length > 0) {
                    filteredData.forEach(/** @param {any} [row] @returns {any} */ row => Object.keys(row || {}).forEach(pushId));
                }

                if (orderedIds.length > 0) {
                    // Weave sparkline column IDs into orderedIds at their placement positions
                    /** @type {any} */
                    const injectedSparklines = new Set();
                    /** @type {any[]} */
                    const orderedIdsWithSparklines = [];
                    orderedIds.forEach(id => {
                        if (sparklineBeforeTriggers.has(id)) {
                            sparklineBeforeTriggers.get(id).forEach(/** @param {any} [sparklineId] @returns {any} */ sparklineId => {
                                if (!injectedSparklines.has(sparklineId)) {
                                    orderedIdsWithSparklines.push(sparklineId);
                                    injectedSparklines.add(sparklineId);
                                }
                            });
                        }
                        orderedIdsWithSparklines.push(id);
                        if (sparklineAfterTriggers.has(id)) {
                            sparklineAfterTriggers.get(id).forEach(/** @param {any} [sparklineId] @returns {any} */ sparklineId => {
                                if (!injectedSparklines.has(sparklineId)) {
                                    orderedIdsWithSparklines.push(sparklineId);
                                    injectedSparklines.add(sparklineId);
                                }
                            });
                        }
                    });
                    // 'end' sparklines + any not yet placed
                    sparklineEndList.forEach(sparklineId => {
                        if (!injectedSparklines.has(sparklineId)) {
                            orderedIdsWithSparklines.push(sparklineId);
                            injectedSparklines.add(sparklineId);
                        }
                    });
                    sparklineSummaryMap.forEach(/** @param {any} [_] @param {any} [sparklineId] @returns {any} */ (_, sparklineId) => {
                        if (!injectedSparklines.has(sparklineId)) {
                            orderedIdsWithSparklines.push(sparklineId);
                            injectedSparklines.add(sparklineId);
                        }
                    });

                    /** @type {any[]} */
                    const flatCols = orderedIdsWithSparklines
                        .map(columnId => {
                            if (sparklineSummaryMap.has(columnId)) return sparklineSummaryMap.get(columnId);
                            return buildServerValueColumn(columnId);
                        })
                        .filter(Boolean);
                    /** @type {any} */
                    const pivotCols = flatCols.filter(c => !isRowTotalColumnId(c.id));
                    /** @type {any} */
                    const fallbackRowTotalCols = flatCols
                        .filter(c => isRowTotalColumnId(c.id))
                        .map(decorateRowTotalColumn)
                        .filter(Boolean);
                    dataCols = [...buildRecursiveTree(pivotCols), ...fallbackRowTotalCols];
                }
            }
        }
        if (hierarchyCols.length === 0 && dataCols.length === 0) {
             dataCols.push({ id: 'no_data', header: 'No Data', cell: () => 'No Data' });
        }

        /** @param {any} [cols] @returns {any} */
        const buildColumns = (cols) => {
            return cols.map(/** @param {any} [col] @returns {any} */ col => {
                if (col.columns) {
                    return {
                        ...col,
                        columns: buildColumns(col.columns)
                    };
                }
                return col;
            });
        };

        return buildColumns([...hierarchyCols, ...dataCols]);
    }, [
        sortOptions,
        sorting,
        serverSide,
        showRowNumbers,
        layoutMode,
        viewMode,
        rowFields,
        colFields,
        valConfigs,
        customDimensions,
        treeConfig,
        minMax,
        colorScaleMode,
        colExpanded,
        isRowSelecting,
        rowDragStart,
        props.columns,
        measureAxis,
        props.measureAxis,
        rowCount,
        cachedColSchema,
        responseColumns,
        filteredData,
        theme,
        defaultColumnWidths,
        validationRules,
        onCellEdit,
        onEditBlocked,
        resolveEditorPresentation,
        rowEditMode,
        getRowEditSession,
        onRequestRowStart,
        onRowDraftChange,
        onRequestRowSave,
        onRequestRowCancel,
        requestEditorOptions,
        renderRowEditActions,
        handleContextMenu,
        handleRowSelect,
        handleRowRangeSelect,
        setIsRowSelecting,
        setRowDragStart,
        renderedOffset,
        isColExpanded,
        toggleCol,
        pendingRowTransitions,
        decimalPlaces,
        defaultValueFormat,
        numberGroupSeparator,
        columnDecimalOverrides,
        columnFormatOverrides,
        columnGroupSeparatorOverrides,
        cellFormatRules,
        pivotMode,
        reportDef,
        showReportConfigColumn,
        onConfigureReportLine,
        detailMode,
        isDetailOpenForRow,
        onToggleDetail,
        resolveCellDisplayValue,
        editValueDisplayMode,
        rowCheckboxSelection,
        selectedRowKeys,
        toggleRowCheckbox,
        rowHeaderLabel,
        measureGroups,
        valueScale,
        unitSuffix,
    ]);
}

import { useMemo } from 'react';
import React from 'react';
import Icons from '../components/Icons';
import EditableCell from '../components/Table/EditableCell';
import SparklineCell from '../components/Table/SparklineCell';
import { formatValue, formatDisplayLabel, formatAggLabel, getKey } from '../utils/helpers';
import { CELL_CONTENT_RESET_STYLE } from '../utils/styles';
import {
    buildPivotSparklinePoints,
    buildSparklineHeader,
    normalizeSparklineConfig,
    normalizeSparklinePoints,
    resolveSparklineDeltaValue,
    resolveSparklineMetricValue,
} from '../utils/sparklines';

const debugLog = process.env.NODE_ENV !== 'production'
    ? (...args) => console.log('[pivot-grid]', ...args)
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
 */
// Column renderers close over theme and edit helpers, so the dependency list
// below must stay complete or edited-cell styling can go stale.
export function useColumnDefs({
    sortOptions,
    serverSide,
    showRowNumbers,
    layoutMode,
    viewMode,
    rowFields,
    colFields,
    valConfigs,
    treeConfig,
    minMax,
    colorScaleMode,
    colExpanded,
    isRowSelecting,
    rowDragStart,
    props,
    cachedColSchema,
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
    detailMode,
    isDetailOpenForRow,
    onToggleDetail,
    resolveCellDisplayValue,
    editValueDisplayMode,
}) {
    return useMemo(() => {
        const effectiveLayoutMode = (pivotMode === 'report' || viewMode === 'tree') ? 'hierarchy' : layoutMode;
        const getConfigDisplayFormat = (config) => {
            if (!config) return null;
            if (config.format) return config.format;
            if (config.windowFn && String(config.windowFn).startsWith('percent_')) return 'percent';
            return null;
        };
        const getSparklineConfig = (config) => normalizeSparklineConfig(config && config.sparkline);
        const extractColumnHeaderText = (columnEntry, fallbackId = null) => {
            const fallbackText = fallbackId ? formatDisplayLabel(fallbackId) : '';
            if (!columnEntry) return fallbackText;
            if (typeof columnEntry === 'string') return formatDisplayLabel(columnEntry);
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
            if (columnEntry.id) return formatDisplayLabel(columnEntry.id);
            return fallbackText;
        };
        const formatSparklineMetricValue = (value, config, columnId) => {
            const numericValue = Number(value);
            if (!Number.isFinite(numericValue)) return '';
            const cellDec = columnId !== undefined && columnDecimalOverrides && columnDecimalOverrides[columnId] !== undefined
                ? columnDecimalOverrides[columnId]
                : decimalPlaces;
            const effectiveGroupSeparator = columnId !== undefined && columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[columnId] !== undefined
                ? columnGroupSeparatorOverrides[columnId]
                : numberGroupSeparator;
            const effectiveFormat = columnId !== undefined && columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, columnId)
                ? columnFormatOverrides[columnId]
                : (getConfigDisplayFormat(config) || defaultValueFormat || null);
            return formatValue(numericValue, effectiveFormat, cellDec, effectiveGroupSeparator);
        };
        const buildSparklineCellTitle = (headerLabel, points, currentLabel, deltaLabel) => {
            if (!Array.isArray(points) || points.length === 0) return headerLabel || '';
            const firstPoint = points[0];
            const lastPoint = points[points.length - 1];
            const firstLabel = firstPoint && firstPoint.label ? String(firstPoint.label) : 'Start';
            const lastLabel = lastPoint && lastPoint.label ? String(lastPoint.label) : 'End';
            const currentSuffix = currentLabel ? ` • ${currentLabel}` : '';
            const deltaSuffix = deltaLabel ? ` (${deltaLabel})` : '';
            return `${headerLabel || 'Trend'}: ${firstLabel} → ${lastLabel}${currentSuffix}${deltaSuffix}`;
        };

        // Helper: return the data key for a valConfig (formula cols use field directly)
        const getValKey = (c) => c.agg === 'formula' ? c.field : getKey('', c.field, c.agg);
        // Helper: return display header for a valConfig
        const getValHeader = (c) => c.agg === 'formula'
            ? (c.label || c.field)
            : `${formatDisplayLabel(c.field)} (${formatAggLabel(c.agg, c.weightField)})`;
        const matchesValueConfigColumnId = (columnId, config) => {
            if (!config || typeof columnId !== 'string' || !config.field) return false;
            const normalizedId = columnId.toLowerCase();
            if (config.agg === 'formula') {
                const formulaId = config.field.toLowerCase();
                return normalizedId === formulaId || normalizedId.endsWith(`_${formulaId}`);
            }
            const measureId = getValKey(config).toLowerCase();
            const measureSuffix = `_${config.field}_${config.agg}`.toLowerCase();
            return normalizedId === measureId || normalizedId.endsWith(measureSuffix);
        };
        const getConfigForColumnId = (columnId) => (
            Array.isArray(valConfigs)
                ? (valConfigs.find(config => matchesValueConfigColumnId(columnId, config)) || null)
                : null
        );
        const isEditableMeasureConfig = (config, columnId) => {
            if (!config || typeof config !== 'object') return false;
            if (typeof columnId === 'string' && columnId.startsWith('__RowTotal__')) return false;
            const normalizedAgg = String(config.agg || '').trim().toLowerCase();
            if (!normalizedAgg || normalizedAgg === 'formula') return false;
            if (config.windowFn) return false;
            return !['count', 'count_distinct', 'distinct_count'].includes(normalizedAgg);
        };
        const getCellRowId = (info) => {
            const row = info && info.row;
            if (!row) return null;
            return row.original && row.original._path ? row.original._path : row.id;
        };
        const getResolvedCellValue = (info) => {
            if (!info) return undefined;
            const rawValue = info.getValue();
            if (typeof resolveCellDisplayValue !== 'function') return rawValue;
            return resolveCellDisplayValue(getCellRowId(info), info.column && info.column.id, rawValue);
        };
        const renderSparklineCellContent = ({
            info,
            points,
            config,
            sparklineConfig,
            columnId,
            compact = false,
        }) => {
            if (!Array.isArray(points) || points.length === 0) return null;
            const summaryMetric = resolveSparklineMetricValue(points, sparklineConfig.metric);
            const deltaMetric = resolveSparklineDeltaValue(points);
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
        const renderConfiguredInlineSparkline = (info, config) => {
            const sparklineConfig = getSparklineConfig(config);
            if (!sparklineConfig) return null;
            const points = normalizeSparklinePoints(getResolvedCellValue(info));
            if (points.length === 0) return null;
            return renderSparklineCellContent({
                info,
                points,
                config,
                sparklineConfig,
                columnId: info && info.column ? info.column.id : null,
                compact: true,
            });
        };
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
        const renderEditableCell = (info, config, defaultEditable = false) => {
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
        const getReportHeaderLabel = () => {
            const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
            if (!root || typeof root !== 'object') return 'Report';
            if (root.label) return root.label;
            if (root.field) return formatDisplayLabel(root.field);
            return 'Report';
        };
        const getTreeHeaderLabel = () => {
            const labelField = treeConfig && typeof treeConfig === 'object' && typeof treeConfig.labelField === 'string'
                ? treeConfig.labelField
                : '';
            return labelField ? formatDisplayLabel(labelField) : 'Tree';
        };
        const getTreeDisplayMode = () => (
            treeConfig && typeof treeConfig === 'object' && treeConfig.displayMode === 'multipleColumns'
                ? 'multipleColumns'
                : 'singleColumn'
        );
        const getTreeLevelLabel = (level) => {
            const configuredLabels = treeConfig && Array.isArray(treeConfig.levelLabels)
                ? treeConfig.levelLabels
                : [];
            if (configuredLabels[level]) return configuredLabels[level];
            if (level === 0) return getTreeHeaderLabel();
            return `${getTreeHeaderLabel()} ${level + 1}`;
        };
        const treeDisplayMode = getTreeDisplayMode();
        const maxTreeDepth = viewMode === 'tree'
            ? filteredData.reduce((maxDepth, row) => {
                if (!row || typeof row !== 'object') return maxDepth;
                const depth = Number.isFinite(Number(row._depth))
                    ? Number(row._depth)
                    : (Number.isFinite(Number(row.depth)) ? Number(row.depth) : 0);
                return Math.max(maxDepth, depth);
            }, 0)
            : 0;
        const canShowDetailButton = detailMode && detailMode !== 'none' && typeof onToggleDetail === 'function';
        const renderDetailToggle = (row) => {
            if (!canShowDetailButton || !row || !row.original) return null;
            const rowPath = row.original._path || row.id;
            const canDetail = row.original._can_detail !== false;
            if (!canDetail) return null;
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

        // Helper: render a numeric cell value with decimal precision and negative-red coloring
        const renderNumericCell = (value, fmt, rowPath, colId) => {
            const cellDec = colId !== undefined && columnDecimalOverrides && columnDecimalOverrides[colId] !== undefined
                ? columnDecimalOverrides[colId]
                : decimalPlaces;
            const effectiveGroupSeparator = colId !== undefined && columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[colId] !== undefined
                ? columnGroupSeparatorOverrides[colId]
                : numberGroupSeparator;
            const effectiveFormat = colId !== undefined && columnFormatOverrides && Object.prototype.hasOwnProperty.call(columnFormatOverrides, colId)
                ? columnFormatOverrides[colId]
                : (fmt || defaultValueFormat || null);
            const formatted = formatValue(value, effectiveFormat, cellDec, effectiveGroupSeparator);
            const isNegative = typeof value === 'number' && value < 0;
            const cellKey = rowPath && colId ? `${rowPath}:::${colId}` : null;
            const cellFmt = cellFormatRules && cellKey ? cellFormatRules[cellKey] : null;
            const contentStyle = isNegative && !(cellFmt && cellFmt.color)
                ? { ...CELL_CONTENT_RESET_STYLE, color: 'red' }
                : CELL_CONTENT_RESET_STYLE;
            return { formatted, contentStyle };
        };

        // Enhanced Sorting Logic (Tree-aware + Natural + Customization)
        const customSortingFn = (rowA, rowB, columnId) => {
            try {
                // Safety check for loading rows (server-side)
                if (!rowA.original || !rowB.original) return 0;

                // 1. Special handling for grand total - it should always be at the end
                // Check multiple ways to identify the grand total
                const aIsGrandTotal = rowA.id === '__grand_total__' ||
                                     rowA.original.__isGrandTotal__ ||
                                     rowA.original._path === '__grand_total__' ||
                                     rowA.original._id === 'Grand Total';
                const bIsGrandTotal = rowB.id === '__grand_total__' ||
                                     rowB.original.__isGrandTotal__ ||
                                     rowB.original._path === '__grand_total__' ||
                                     rowB.original._id === 'Grand Total';

                // If one is grand total and the other is not, grand total goes last
                if (aIsGrandTotal && !bIsGrandTotal) return 1;
                if (!aIsGrandTotal && bIsGrandTotal) return -1;

                // If both are grand totals, they are equal
                if (aIsGrandTotal && bIsGrandTotal) return 0;

                // 2. Regular totals (but not grand total) should come after non-totals
                const aIsRegularTotal = (rowA.original && rowA.original._isTotal) && !aIsGrandTotal;
                const bIsRegularTotal = (rowB.original && rowB.original._isTotal) && !bIsGrandTotal;

                if (aIsRegularTotal && !bIsRegularTotal) return 1;
                if (!aIsRegularTotal && bIsRegularTotal) return -1;

                // Both are regular totals (not grand totals) - they can be equal for sorting purposes
                if (aIsRegularTotal && bIsRegularTotal) return 0;

                const valA = rowA.getValue(columnId);
                const valB = rowB.getValue(columnId);

                // 2. Column-Specific Customization
                const colSortOptions = (sortOptions.columnOptions && sortOptions.columnOptions[columnId]) || {};
                const isNatural = colSortOptions.naturalSort !== undefined ? colSortOptions.naturalSort : (sortOptions.naturalSort !== false);
                const isCaseSensitive = colSortOptions.caseSensitive !== undefined ? colSortOptions.caseSensitive : sortOptions.caseSensitive;

                if (isNatural) {
                    const sensitivity = isCaseSensitive ? 'variant' : 'base';
                    return new Intl.Collator(undefined, { numeric: true, sensitivity }).compare(String(valA || ''), String(valB || ''));
                }

                // Default Alphanumeric with configured sensitivity
                const defaultSensitivity = isCaseSensitive ? 'variant' : 'base';
                return String(valA || '').localeCompare(String(valB || ''), undefined, { numeric: true, sensitivity: defaultSensitivity });
            } catch (err) {
                console.error('Sorting error:', err);
                return 0;
            }
        };

        const sortingFn = serverSide ? 'auto' : customSortingFn;
        const hierarchyCols = [];

        if (showRowNumbers) {
                hierarchyCols.push({
                    id: '__row_number__',
                    header: '#',
                    size: defaultColumnWidths.rowNumber,
                    enableSorting: false,
                    enableColumnFilter: false,
                    enablePinning: false, // User Request: Cannot be changed
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
                        {(row.original && (row.original._isTotal || row.original._path === '__grand_total__' || row.original._id === 'Grand Total'))
                            ? (rowCount != null ? rowCount : '∑')
                            : (row.index + 1 + (serverSide ? (renderedOffset || 0) : 0))}
                    </div>
                )
            });
        }

        if (viewMode === 'tree' && treeDisplayMode === 'multipleColumns') {
            Array.from({ length: Math.max(maxTreeDepth + 1, 1) }).forEach((_, level) => {
                hierarchyCols.push({
                    id: `__tree_level__${level}`,
                    accessorFn: row => {
                        const depth = Number.isFinite(Number(row && row._depth))
                            ? Number(row._depth)
                            : (Number.isFinite(Number(row && row.depth)) ? Number(row.depth) : 0);
                        return depth === level ? (row && (row._label || row._id)) : '';
                    },
                    header: getTreeLevelLabel(level),
                    size: level === 0 ? defaultColumnWidths.hierarchy : defaultColumnWidths.dimension,
                    sortingFn,
                    cell: ({ row }) => {
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        const showValue = depth === level;
                        const showExpander = showValue && row.getCanExpand() && !(row.original && row.original._isTotal);
                        return (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%',
                                    fontWeight: (row.original && row.original._isTotal) ? 700 : 400,
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
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:theme.textSec,display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                ) : <span style={{width:'18px', flexShrink: 0}}/>}
                                {showValue ? (row.original ? (row.original._label || row.original._id) : '') : ''}
                                {showValue && renderDetailToggle(row)}
                                {showValue && typeof renderRowEditActions === 'function' ? renderRowEditActions(row) : null}
                            </div>
                        );
                    }
                });
            });
        } else if (effectiveLayoutMode === 'hierarchy') {
            if (rowFields.length > 0 || viewMode === 'tree') {
                hierarchyCols.push({
                    id: 'hierarchy',
                    accessorFn: row => row._id,
                    header: pivotMode === 'report'
                        ? getReportHeaderLabel()
                        : (viewMode === 'tree'
                            ? getTreeHeaderLabel()
                            : rowFields.map(formatDisplayLabel).join(' > ')),
                    size: defaultColumnWidths.hierarchy,
                    sortingFn, // Apply sort
                    cell: ({ row }) => {
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        return (
                            <div
                                style={{
                                    paddingLeft: `${depth * 24}px`,
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
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:theme.textSec,display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                ) : <span style={{width:'18px'}}/>}
                                <span style={{ fontWeight: (row.original && row.original._isTotal) ? 700 : 400 }}>
                                    {row.original ? (row.original._label || row.original._id) : ''}
                                </span>
                                {renderDetailToggle(row)}
                                {typeof renderRowEditActions === 'function' ? renderRowEditActions(row) : null}
                                {/* Report mode: show level label badge */}
                                {pivotMode === 'report' && row.original && row.original._levelLabel && (
                                    <span style={{
                                        fontSize: '9px',
                                        fontWeight: 600,
                                        color: theme.primary || '#4F46E5',
                                        background: `${theme.primary || '#4F46E5'}14`,
                                        padding: '1px 6px',
                                        borderRadius: '4px',
                                        marginLeft: '8px',
                                        whiteSpace: 'nowrap',
                                        flexShrink: 0,
                                    }}>
                                        {row.original._levelLabel}
                                    </span>
                                )}
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
            rowFields.forEach((field, i) => {
                hierarchyCols.push({
                    id: field,
                    accessorKey: field,
                    header: formatDisplayLabel(field),
                    size: defaultColumnWidths.dimension,
                    enablePinning: true,
                    sortingFn,
                    cell: ({ row, getValue }) => {
                        const val = getValue();
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);

                        // Outline: Show only if current column matches depth (step layout)
                        // Tabular: Show if column is <= depth (repeat labels)
                        let showValue = true;
                        if (effectiveLayoutMode === 'outline') {
                            if (i !== depth) showValue = false;
                        } else {
                            // Tabular
                            if (i > depth) showValue = false;
                        }

                        // Expander only on the active level column
                        const showExpander = (i === depth) && row.getCanExpand() && !(row.original && row.original._isTotal);

                        return (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%',
                                    fontWeight: (row.original && row.original._isTotal) ? 700 : 400
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
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:theme.textSec,display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                )}
                                {showValue ? val : ''}
                                {/* Report mode: show level label badge */}
                                {pivotMode === 'report' && showValue && row.original && row.original._levelLabel && (
                                    <span style={{
                                        fontSize: '9px',
                                        fontWeight: 600,
                                        color: theme.primary || '#4F46E5',
                                        background: `${theme.primary || '#4F46E5'}14`,
                                        padding: '1px 6px',
                                        borderRadius: '4px',
                                        marginLeft: '6px',
                                        whiteSpace: 'nowrap',
                                        flexShrink: 0,
                                    }}>
                                        {row.original._levelLabel}
                                    </span>
                                )}
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

        let dataCols = [];
        if (viewMode === 'tree') {
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
                '__virtualIndex',
                '__col_schema',
                treeConfig && treeConfig.idField,
                treeConfig && treeConfig.parentIdField,
                treeConfig && treeConfig.pathField,
                treeConfig && treeConfig.childrenField,
                treeConfig && treeConfig.labelField,
            ].filter(Boolean));

            const orderedIds = [];
            const seenIds = new Set();
            const pushId = (rawId) => {
                const id = typeof rawId === 'string' ? rawId : null;
                if (!id || ignoreKeys.has(id) || seenIds.has(id)) return;
                seenIds.add(id);
                orderedIds.push(id);
            };

            const configuredIds = [
                ...(treeConfig && Array.isArray(treeConfig.valueFields) ? treeConfig.valueFields : []),
                ...(treeConfig && Array.isArray(treeConfig.extraFields) ? treeConfig.extraFields : []),
            ];
            configuredIds.forEach(pushId);
            if (props.columns && props.columns.length > 0) {
                props.columns.forEach((column) => pushId(column && column.id));
            } else if (filteredData.length > 0) {
                filteredData.forEach((row) => Object.keys(row || {}).forEach(pushId));
            }

            dataCols = orderedIds.map((columnId) => ({
                id: columnId,
                accessorFn: (row) => row[columnId],
                header: formatDisplayLabel(columnId),
                size: defaultColumnWidths.measure,
                enablePinning: true,
                sortingFn,
                cell: (info) => {
                    const sparklineCell = renderConfiguredInlineSparkline(info, getConfigForColumnId(columnId));
                    if (sparklineCell) {
                        return sparklineCell;
                    }
                    const editableCell = renderEditableCell(info, null, false);
                    if (editableCell) {
                        return editableCell;
                    }
                    const value = getResolvedCellValue(info);
                    const rowPath = getCellRowId(info);
                    if (typeof value === 'number') {
                        const { formatted, contentStyle } = renderNumericCell(value, null, rowPath, info.column.id);
                        return (
                            <div style={{ width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', ...contentStyle }}>
                                {formatted}
                            </div>
                        );
                    }
                    return (
                        <div style={{ width:'100%', height:'100%', display:'flex', alignItems:'center', paddingRight:'8px' }}>
                            {value !== undefined && value !== null ? String(value) : ''}
                        </div>
                    );
                },
            }));
        } else if (colFields.length === 0) {
            dataCols = valConfigs.map(c => ({
                id: getValKey(c),
                accessorFn: row => row[getValKey(c)],
                header: getValHeader(c),
                size: defaultColumnWidths.measure,
                enablePinning: true,
                sortingFn,
                cell: info => {
                    const sparklineCell = renderConfiguredInlineSparkline(info, c);
                    if (sparklineCell) {
                        return sparklineCell;
                    }
                    const editableCell = renderEditableCell(info, c, isEditableMeasureConfig(c, info.column.id));
                    if (editableCell) {
                        return editableCell;
                    }
                    const value = getResolvedCellValue(info);
                    const rowPath = getCellRowId(info);
                    const { formatted, contentStyle } = renderNumericCell(value, getConfigDisplayFormat(c), rowPath, info.column.id);
                    return (
                        <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                            {formatted}
                        </div>
                    );
                }
            }));
        } else if (serverSide) {
            const ignoreKeys = new Set(['_id', 'depth', '_isTotal', '_path', 'uuid', ...rowFields, ...colFields]);
            const measureIds = new Set(valConfigs.map(v => getValKey(v)));
            const isRelevantColumnId = (columnId) => {
                if (!columnId || ignoreKeys.has(columnId)) return false;
                if (measureIds.has(columnId)) return true;
                if (columnId.startsWith('__RowTotal__')) return true;
                return Boolean(getConfigForColumnId(columnId));
            };

            const buildServerValueColumn = (columnId, sizeOverride = defaultColumnWidths.subtotal) => {
                if (!isRelevantColumnId(columnId)) return null;
                return {
                    id: columnId,
                    accessorFn: row => row[columnId],
                    header: columnId,
                    size: Number.isFinite(Number(sizeOverride)) ? Number(sizeOverride) : defaultColumnWidths.subtotal,
                    sortingFn,
                    cell: info => {
                        const matchedConfig = getConfigForColumnId(columnId);
                        const sparklineCell = renderConfiguredInlineSparkline(info, matchedConfig);
                        if (sparklineCell) {
                            return sparklineCell;
                        }
                        const value = getResolvedCellValue(info);
                        const editableCell = renderEditableCell(info, matchedConfig, isEditableMeasureConfig(matchedConfig, columnId));
                        if (editableCell) {
                            return editableCell;
                        }
                        const fmt = getConfigDisplayFormat(matchedConfig);
                        const rowPath = getCellRowId(info);
                        const { formatted, contentStyle } = renderNumericCell(value, fmt, rowPath, info.column.id);
                        return (
                            <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                                {formatted}
                            </div>
                        );
                    }
                };
            };

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

            const buildRecursiveTree = (cols) => {
                const root = { columns: [] };
                cols.forEach(col => {
                    const key = col.id;
                    if (!key) return;
                    let dimStr = key;
                    let measureStr = "";
                    const matchedConfig = getConfigForColumnId(key);
                    if (matchedConfig) {
                        const suffix = matchedConfig.agg === 'formula'
                            ? `_${matchedConfig.field}`
                            : `_${matchedConfig.field}_${matchedConfig.agg}`;
                        if (key.toLowerCase().endsWith(suffix.toLowerCase())) {
                            dimStr = key.substring(0, key.length - suffix.length);
                        }
                        measureStr = matchedConfig.agg === 'formula'
                            ? (matchedConfig.label || matchedConfig.field)
                            : `${formatDisplayLabel(matchedConfig.field)} (${formatAggLabel(matchedConfig.agg, matchedConfig.weightField)})`;
                    }
                    if (!matchedConfig) {
                         const parts = key.split('_');
                         if (parts.length > 1) {
                             dimStr = parts.slice(0, parts.length - 2).join('_');
                             measureStr = parts.slice(parts.length - 2).join(' ');
                             if (!dimStr) dimStr = "Total";
                         }
                    }
                    const dimPath = dimStr ? dimStr.split('|') : [];
                    let current = root;
                    let pathKey = '';
                    let parentCollapsed = false;
                    for (let idx = 0; idx < dimPath.length; idx++) {
                        const val = dimPath[idx].trim();
                        if (idx > 0 && !isColExpanded(pathKey)) {
                            parentCollapsed = true;
                            break;
                        }
                        const currentPathKey = pathKey ? `${pathKey}|||${val}` : val;
                        pathKey = currentPathKey;
                        let node = current.columns.find(c => c.groupValue === val);
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
                        const newCol = { ...col, header: measureStr || col.header, enablePinning: true };
                        if (resolveEditableCellState({ column: { id: key }, getValue: () => undefined }, matchedConfig, isEditableMeasureConfig(matchedConfig, key))) {
                            newCol.cell = info => {
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
                return root.columns;
            };

            const decorateRowTotalColumn = (column) => {
                if (!column) return null;
                const decorated = { ...column };
                if (typeof decorated.header === 'string' && decorated.header.startsWith('__RowTotal__')) {
                    decorated.header = decorated.header.replace('__RowTotal__', 'Total ');
                }
                decorated.cell = info => {
                    const config = valConfigs.find(v => decorated.id.includes(v.field));
                    const value = getResolvedCellValue(info);
                    const rowPath = getCellRowId(info);
                    const { formatted, contentStyle } = renderNumericCell(value, getConfigDisplayFormat(config), rowPath, info.column.id);
                    return (
                        <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', fontWeight:'bold', ...contentStyle}} onContextMenu={e => handleContextMenu(e, value, info.column.id, info.row)}>
                            {formatted}
                        </div>
                    );
                };
                return decorated;
            };

            const auxiliaryIds = [];
            const seenAuxiliaryIds = new Set();
            const pushAuxiliaryId = (rawId) => {
                const id = typeof rawId === 'string' ? rawId : null;
                if (!id || id === '__col_schema' || seenAuxiliaryIds.has(id)) return;
                seenAuxiliaryIds.add(id);
                auxiliaryIds.push(id);
            };

            if (props.columns && props.columns.length > 0) {
                props.columns.forEach(c => pushAuxiliaryId(c && c.id));
            } else if (filteredData.length > 0) {
                filteredData.forEach(row => Object.keys(row || {}).forEach(pushAuxiliaryId));
            }

            const rowTotalCols = auxiliaryIds
                .filter(columnId => columnId.startsWith('__RowTotal__'))
                .map(columnId => decorateRowTotalColumn(buildServerValueColumn(columnId)))
                .filter(Boolean);

            const schemaColumns = cachedColSchema && Array.isArray(cachedColSchema.columns)
                ? cachedColSchema.columns
                : [];
            const sparklineColumnLabelById = new Map();
            schemaColumns.forEach((entry) => {
                if (entry && entry.id) {
                    sparklineColumnLabelById.set(entry.id, extractColumnHeaderText(entry, entry.id));
                }
            });
            if (props.columns && props.columns.length > 0) {
                props.columns.forEach((entry) => {
                    if (entry && entry.id && !sparklineColumnLabelById.has(entry.id)) {
                        sparklineColumnLabelById.set(entry.id, extractColumnHeaderText(entry, entry.id));
                    }
                });
            }
            const totalSchemaColumns = cachedColSchema && Number.isFinite(Number(cachedColSchema.total_center_cols))
                ? Math.max(0, Math.floor(Number(cachedColSchema.total_center_cols)))
                : schemaColumns.length;
            const hasSparseSchemaCatalog = totalSchemaColumns > 0 && schemaColumns.length >= totalSchemaColumns;
            const resolveSparklineSourceColumnIds = (config) => {
                if (!config) return [];
                const collected = [];
                const seen = new Set();
                const pushId = (rawId) => {
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
                schemaColumns.forEach((entry) => pushId(entry && entry.id));
                auxiliaryIds.forEach(pushId);
                return collected;
            };
            const buildSparklineSummaryColumns = () => {
                const summaryColumns = (Array.isArray(valConfigs) ? valConfigs : [])
                    .map((config) => {
                        const sparklineConfig = getSparklineConfig(config);
                        if (!sparklineConfig) return null;
                        const sourceColumnIds = resolveSparklineSourceColumnIds(config);
                        if (sourceColumnIds.length === 0) return null;
                        const sparklineColumnId = `__sparkline__${getValKey(config)}`;
                        const headerLabel = buildSparklineHeader(config, config && config.field ? config.field : sparklineColumnId);
                        return {
                            id: sparklineColumnId,
                            header: headerLabel,
                            accessorFn: (row) => resolveSparklineMetricValue(
                                buildPivotSparklinePoints({
                                    rowData: row,
                                    columnIds: sourceColumnIds,
                                    resolveValue: (sourceColumnId, rawValue) => {
                                        const rowPath = row && row._path ? row._path : null;
                                        return typeof resolveCellDisplayValue === 'function'
                                            ? resolveCellDisplayValue(rowPath, sourceColumnId, rawValue)
                                            : rawValue;
                                    },
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
                            },
                            cell: (info) => {
                                const rowData = info && info.row ? info.row.original : null;
                                const rowPath = getCellRowId(info);
                                const points = buildPivotSparklinePoints({
                                    rowData,
                                    columnIds: sourceColumnIds,
                                    resolveValue: (sourceColumnId, rawValue) => (
                                        typeof resolveCellDisplayValue === 'function'
                                            ? resolveCellDisplayValue(rowPath, sourceColumnId, rawValue)
                                            : rawValue
                                    ),
                                    resolveLabel: (sourceColumnId) => sparklineColumnLabelById.get(sourceColumnId) || extractColumnHeaderText(sourceColumnId, sourceColumnId),
                                });
                                return renderSparklineCellContent({
                                    info,
                                    points,
                                    config,
                                    sparklineConfig,
                                    columnId: sparklineColumnId,
                                    compact: false,
                                });
                            },
                        };
                    })
                    .filter(Boolean);
                if (summaryColumns.length === 0) return [];
                if (summaryColumns.length === 1) return summaryColumns;
                return [{
                    id: '__sparkline_group__',
                    header: 'Trends',
                    columns: summaryColumns,
                    enablePinning: true,
                }];
            };
            const sparklineSummaryCols = buildSparklineSummaryColumns();

            if (hasSparseSchemaCatalog) {
                const sparseDataCols = [];
                let loadedSegment = [];
                const flushLoadedSegment = () => {
                    if (loadedSegment.length === 0) return;
                    sparseDataCols.push(...buildRecursiveTree(loadedSegment));
                    loadedSegment = [];
                };

                for (let index = 0; index < totalSchemaColumns; index += 1) {
                    const schemaEntry = schemaColumns[index];
                    const schemaSize = schemaEntry && Number.isFinite(Number(schemaEntry.size))
                        ? Number(schemaEntry.size)
                        : defaultColumnWidths.schemaFallback;
                    if (schemaEntry && typeof schemaEntry.id === 'string' && schemaEntry.id) {
                        const valueColumn = buildServerValueColumn(schemaEntry.id, schemaSize);
                        if (valueColumn && !valueColumn.id.startsWith('__RowTotal__')) {
                            loadedSegment.push(valueColumn);
                            continue;
                        }
                    }
                    flushLoadedSegment();
                    sparseDataCols.push(buildPlaceholderColumn(index, schemaSize));
                }

                flushLoadedSegment();
                dataCols = [...sparklineSummaryCols, ...sparseDataCols, ...rowTotalCols];
            } else {
                const orderedIds = [];
                const seenIds = new Set();
                const pushId = (rawId) => {
                    const id = typeof rawId === 'string' ? rawId : null;
                    if (!id || id === '__col_schema' || seenIds.has(id)) return;
                    seenIds.add(id);
                    orderedIds.push(id);
                };

                if (props.columns && props.columns.length > 0) {
                    props.columns.forEach(c => pushId(c && c.id));
                } else if (filteredData.length > 0) {
                    filteredData.forEach(row => Object.keys(row || {}).forEach(pushId));
                }

                if (orderedIds.length > 0) {
                    const flatCols = orderedIds
                        .map(columnId => buildServerValueColumn(columnId))
                        .filter(Boolean);
                    const pivotCols = flatCols.filter(c => !c.id.startsWith('__RowTotal__'));
                    const fallbackRowTotalCols = flatCols
                        .filter(c => c.id.startsWith('__RowTotal__'))
                        .map(decorateRowTotalColumn)
                        .filter(Boolean);
                    dataCols = [...sparklineSummaryCols, ...buildRecursiveTree(pivotCols), ...fallbackRowTotalCols];
                }
            }
        }
        if (hierarchyCols.length === 0 && dataCols.length === 0) {
             dataCols.push({ id: 'no_data', header: 'No Data', cell: () => 'No Data' });
        }

        const buildColumns = (cols) => {
            return cols.map(col => {
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
        serverSide,
        showRowNumbers,
        layoutMode,
        viewMode,
        rowFields,
        colFields,
        valConfigs,
        treeConfig,
        minMax,
        colorScaleMode,
        colExpanded,
        isRowSelecting,
        rowDragStart,
        props.columns,
        rowCount,
        cachedColSchema,
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
        detailMode,
        isDetailOpenForRow,
        onToggleDetail,
        resolveCellDisplayValue,
        editValueDisplayMode,
    ]);
}

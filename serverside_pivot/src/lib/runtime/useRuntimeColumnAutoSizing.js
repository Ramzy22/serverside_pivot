import { useCallback } from 'react';
import { formatDisplayLabel, formatValue } from '../utils/helpers';
import { createTextWidthMeasurer, measureColumnFitMetric } from '../utils/pivotLayoutUtils';

const HIERARCHY_INDENT_PX = 24;
const HIERARCHY_EXPAND_PX = 22;

/** Owns measured column sizing and its toolbar command.  @param {any} p0 @returns {any} */
export function useRuntimeColumnAutoSizing({
  autoSizeBounds,
  columnDecimalOverrides,
  columnFormatOverrides,
  columnGroupSeparatorOverrides,
  decimalPlaces,
  defaultValueFormat,
  fontFamily,
  fontSize,
  getAutoSizeSampleRows,
  getDisplayHeaderTextStackForColumn,
  getRenderedCellWidthForColumn,
  getRenderedHeaderWidthForColumn,
  matchesSelectedColumnToValueConfig,
  numberGroupSeparator,
  queueHeaderFitValidation,
  table,
  valConfigs,
  valueScale,
  unitSuffix,
}) {
  /** @type {(...args: any[]) => any} */
  const autoSizeColumn = useCallback((columnId, includeHeader = true) => {
    if (!table) return;
    const rows = table.getRowModel().rows;
    const pinnedTop = typeof table.getTopRows === 'function' ? table.getTopRows() : [];
    const pinnedBottom = typeof table.getBottomRows === 'function' ? table.getBottomRows() : [];
    const sampledBody = getAutoSizeSampleRows(rows);
    const seenRowIds = new Set();
    /** @type {any[]} */
    const sampleRows = [];
    [...pinnedTop, ...sampledBody, ...pinnedBottom].forEach((row) => {
      if (!row || (row.id !== undefined && seenRowIds.has(row.id))) return;
      if (row.id !== undefined) seenRowIds.add(row.id);
      sampleRows.push(row);
    });

    /** @type {any} */
    const column = table.getColumn(columnId);
    if (!column || (column.columnDef && column.columnDef.meta && column.columnDef.meta.isSchemaPlaceholder)) return;

    const resolvedFont = fontFamily || "'Inter', system-ui, sans-serif";
    const resolvedSize = fontSize || '14px';
    const measureBodyText = createTextWidthMeasurer(`${resolvedSize} ${resolvedFont}`, 7.5);
    const measureHeaderText = createTextWidthMeasurer(`600 ${resolvedSize} ${resolvedFont}`, 7.5);

    /** @type {any} */
    const matchingValueConfig = Array.isArray(valConfigs)
      ? valConfigs.find(config => matchesSelectedColumnToValueConfig(columnId, config)) || null
      : null;
    const effectiveDecimalPlaces = columnDecimalOverrides && columnDecimalOverrides[columnId] !== undefined
      ? columnDecimalOverrides[columnId]
      : decimalPlaces;
    const effectiveFormat = Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, columnId)
      ? columnFormatOverrides[columnId]
      : ((matchingValueConfig && matchingValueConfig.format) || defaultValueFormat || null);
    const effectiveGroupSeparator = columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[columnId] !== undefined
      ? columnGroupSeparatorOverrides[columnId]
      : numberGroupSeparator;
    const effectiveValueScale = (matchingValueConfig && matchingValueConfig.valueScale !== undefined && matchingValueConfig.valueScale !== null)
      ? matchingValueConfig.valueScale
      : valueScale;
    const effectiveUnitSuffix = (matchingValueConfig && matchingValueConfig.unitSuffix !== undefined && matchingValueConfig.unitSuffix !== null)
      ? matchingValueConfig.unitSuffix
      : unitSuffix;
    const displayOptions = (effectiveValueScale !== undefined && effectiveValueScale !== null) || effectiveUnitSuffix
      ? { valueScale: effectiveValueScale, unitSuffix: effectiveUnitSuffix }
      : undefined;

    const headerLines = includeHeader ? getDisplayHeaderTextStackForColumn(column) : [];
    const metric = measureColumnFitMetric({
      columnId,
      baseSize: (column.columnDef && column.columnDef.size) || autoSizeBounds.minWidth,
      rows: sampleRows,
      /** @param {any} [row] @param {any} [colId] @returns {any} */
      getCellRawValue: (row, colId) => row.getValue(colId),
      /** @param {any} [val] @returns {any} */
      formatCellText: (val) => formatValue(val, effectiveFormat, effectiveDecimalPlaces, effectiveGroupSeparator, displayOptions),
      formatHierarchyLabel: formatDisplayLabel,
      headerLines,
      shortLabel: null,
      headerWrap: false,
      headerControlsMode: 'always',
      canSort: false,
      sortIndicator: 'none',
      measureBodyText,
      measureHeaderText,
      cellPaddingX: Math.max(0, Math.round((autoSizeBounds.cellPadding || 0) / 2)),
      headerPaddingX: Math.max(0, Math.round((autoSizeBounds.headerPadding || 0) / 2)),
      numericPaddingRight: 0,
      hierarchyIndentPx: HIERARCHY_INDENT_PX,
      hierarchyExpandPx: HIERARCHY_EXPAND_PX,
      cellOverscan: autoSizeBounds.cellOverscan || 0,
      headerOverscan: autoSizeBounds.headerOverscan || 0,
    });

    let maxWidth = Math.max(autoSizeBounds.minWidth, metric ? metric.contentSize : autoSizeBounds.minWidth);
    if (includeHeader) {
      /** @type {any} */
      const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
      if (Number.isFinite(renderedHeaderWidth)) maxWidth = Math.max(maxWidth, renderedHeaderWidth);
    }
    /** @type {any} */
    const renderedCellWidth = columnId !== 'hierarchy' && columnId !== '__row_number__' && columnId !== '__report_config__'
      ? getRenderedCellWidthForColumn(columnId)
      : null;
    if (Number.isFinite(renderedCellWidth)) maxWidth = Math.max(maxWidth, renderedCellWidth);
    maxWidth = Math.ceil(maxWidth);
    maxWidth = Math.min(maxWidth, autoSizeBounds.maxWidth);
    maxWidth = Math.max(maxWidth, autoSizeBounds.minWidth);
    table.setColumnSizing(/** @param {any} [previous] @returns {any} */ previous => ({ ...previous, [columnId]: maxWidth }));
    queueHeaderFitValidation(columnId, maxWidth);
  }, [
    autoSizeBounds,
    columnDecimalOverrides,
    columnFormatOverrides,
    columnGroupSeparatorOverrides,
    decimalPlaces,
    defaultValueFormat,
    fontFamily,
    fontSize,
    getAutoSizeSampleRows,
    getDisplayHeaderTextStackForColumn,
    getRenderedCellWidthForColumn,
    getRenderedHeaderWidthForColumn,
    matchesSelectedColumnToValueConfig,
    numberGroupSeparator,
    queueHeaderFitValidation,
    table,
    unitSuffix,
    valConfigs,
    valueScale,
  ]);

  /** @type {(...args: any[]) => any} */
  const autoSizeVisibleColumns = useCallback((includeHeader = true) => {
    if (!table || !table.getVisibleLeafColumns) return;
    table.getVisibleLeafColumns().forEach(/** @param {any} [column] @returns {any} */ column => autoSizeColumn(column.id, includeHeader));
  }, [autoSizeColumn, table]);

  /** @type {(...args: any[]) => any} */
  const handleAutoSizeToolbarClick = useCallback(() => {
    autoSizeVisibleColumns(true);
  }, [autoSizeVisibleColumns]);

  return {
    HIERARCHY_EXPAND_PX,
    HIERARCHY_INDENT_PX,
    autoSizeColumn,
    autoSizeVisibleColumns,
    handleAutoSizeToolbarClick,
  };
}

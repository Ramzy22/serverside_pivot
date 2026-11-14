import { useCallback } from 'react';
import { formatDisplayLabel, formatValue } from '../utils/helpers';

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
}) {
  /** @type {(...args: any[]) => any} */
  const autoSizeColumn = useCallback((columnId, includeHeader = true) => {
    if (!table) return;
    const rows = table.getRowModel().rows;
    /** @type {any} */
    const sampleRows = getAutoSizeSampleRows(rows);
    let maxWidth = autoSizeBounds.minWidth;
    /** @type {any} */
    const canvas = document.createElement('canvas');
    /** @type {any} */
    const context = canvas.getContext('2d');
    if (!context) return;
    const bodyFont = `${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;
    const headerFont = `600 ${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;
    /** @type {any} */
    const column = table.getColumn(columnId);
    if (!column || column.columnDef && column.columnDef.meta && column.columnDef.meta.isSchemaPlaceholder) return;
    /** @type {any} */
    const matchingValueConfig = Array.isArray(valConfigs) ? valConfigs.find(config => matchesSelectedColumnToValueConfig(columnId, config)) || null : null;
    /** @type {any} */
    const headerTextStack = getDisplayHeaderTextStackForColumn(column);
    if (includeHeader) {
      context.font = headerFont;
      headerTextStack.forEach(/** @param {any} [headerText] @returns {any} */ headerText => {
        maxWidth = Math.max(maxWidth, context.measureText(headerText).width + autoSizeBounds.headerPadding + autoSizeBounds.headerOverscan);
      });
      /** @type {any} */
      const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
      if (Number.isFinite(renderedHeaderWidth)) maxWidth = Math.max(maxWidth, renderedHeaderWidth);
    }
    context.font = bodyFont;
    sampleRows.forEach(/** @param {any} [row] @returns {any} */ row => {
      /** @type {any} */
      const cellValue = row.getValue(columnId);
      /** @type {any} */
      let width;
      if (columnId === 'hierarchy') {
        /** @type {any} */
        const depth = row.original && typeof row.original.depth === 'number' ? row.original.depth : row.depth || 0;
        /** @type {any} */
        const label = row.original ? formatDisplayLabel(String(row.original._label != null ? row.original._label : row.original._id != null ? row.original._id : '')) : '';
        width = context.measureText(label).width + depth * HIERARCHY_INDENT_PX + HIERARCHY_EXPAND_PX + autoSizeBounds.cellPadding + autoSizeBounds.cellOverscan;
      } else {
        /** @type {any} */
        const effectiveDecimalPlaces = columnDecimalOverrides && columnDecimalOverrides[columnId] !== undefined ? columnDecimalOverrides[columnId] : decimalPlaces;
        /** @type {any} */
        const effectiveFormat = Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, columnId) ? columnFormatOverrides[columnId] : matchingValueConfig && matchingValueConfig.format || defaultValueFormat || null;
        /** @type {any} */
        const effectiveGroupSeparator = columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[columnId] !== undefined ? columnGroupSeparatorOverrides[columnId] : numberGroupSeparator;
        width = context.measureText(formatValue(cellValue, effectiveFormat, effectiveDecimalPlaces, effectiveGroupSeparator)).width + autoSizeBounds.cellPadding + autoSizeBounds.cellOverscan;
      }
      if (width > maxWidth) maxWidth = width;
    });
    /** @type {any} */
    const renderedCellWidth = columnId !== 'hierarchy' && columnId !== '__row_number__' && columnId !== '__report_config__' ? getRenderedCellWidthForColumn(columnId) : null;
    if (Number.isFinite(renderedCellWidth)) maxWidth = Math.max(maxWidth, renderedCellWidth);
    maxWidth = Math.ceil(maxWidth);
    maxWidth = Math.min(maxWidth, autoSizeBounds.maxWidth);
    maxWidth = Math.max(maxWidth, autoSizeBounds.minWidth);
    table.setColumnSizing(/** @param {any} [previous] @returns {any} */ previous => ({ ...previous, [columnId]: maxWidth }));
    queueHeaderFitValidation(columnId, maxWidth);
  }, [autoSizeBounds, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, decimalPlaces, defaultValueFormat, fontFamily, fontSize, getAutoSizeSampleRows, getDisplayHeaderTextStackForColumn, getRenderedCellWidthForColumn, getRenderedHeaderWidthForColumn, matchesSelectedColumnToValueConfig, numberGroupSeparator, queueHeaderFitValidation, table, valConfigs]);

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

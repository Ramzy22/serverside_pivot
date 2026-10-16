import { useCallback } from 'react';

import { buildMergedHeaderRows, buildStyledHtmlTableExport, writeClipboardPayload } from '../utils/exportUtils';
import { formatDisplayLabel, formatValue } from '../utils/helpers';

const EXPORT_SKIP_COL_IDS = new Set(['__row_number__', '__report_config__']);

/**
 * Owns the client-side representation of clipboard and export data.
 *
 * This deliberately has no dependency on the legacy runtime environment. The
 * interaction stage supplies the individual data, formatting, and table
 * capabilities it needs, while export dispatch is owned separately by
 * createRuntimeExportCommands.
 */
export function useRuntimeClipboardExportPresentation({
  cachedColSchema,
  cellFormatRules,
  colorPalette,
  colorScaleMode,
  colorScaleStats,
  columnDecimalOverrides,
  columnFormatOverrides,
  columnGroupSeparatorOverrides,
  conditionalFormatting,
  decimalPlaces,
  defaultValueFormat,
  fontFamily,
  fontSize,
  getConditionalStyle,
  getDisplayHeaderTextForColumn,
  getDisplayHeaderTextStackForColumn,
  getDisplayRows,
  HIERARCHY_INDENT_PX,
  headerFormatting,
  loadedRows,
  matchesSelectedColumnToValueConfig,
  mergedHeaderSeparator,
  numberGroupSeparator,
  resolveDisplayedCellValue,
  rowHeight,
  selectedCells,
  serverSide,
  showNotification,
  table,
  theme,
  totalCenterCols,
  valConfigs,
}) {
  const isExportableColumnId = useCallback(columnId => {
    const id = String(columnId || '');
    return !!id && !EXPORT_SKIP_COL_IDS.has(id) && !id.startsWith('__schema_placeholder__');
  }, []);

  const getInstantiatedExportColIds = useCallback(() => {
    const columns = table && typeof table.getVisibleLeafColumns === 'function' ? table.getVisibleLeafColumns() : [];
    const ids = [];
    const seen = new Set();
    columns.forEach(column => {
      const columnId = column && column.id;
      if (!isExportableColumnId(columnId) || seen.has(columnId)) return;
      ids.push(columnId);
      seen.add(columnId);
    });
    return ids;
  }, [isExportableColumnId, table]);

  const getCompleteServerExportColIds = useCallback(() => {
    const knownIds = getInstantiatedExportColIds();
    if (!serverSide) return knownIds;
    const schemaTotal = cachedColSchema && Number.isFinite(Number(cachedColSchema.total_center_cols)) ? Math.max(0, Math.floor(Number(cachedColSchema.total_center_cols))) : null;
    const schemaColumns = cachedColSchema && Array.isArray(cachedColSchema.columns) ? cachedColSchema.columns : [];
    if (schemaTotal && schemaTotal > 0) {
      if (schemaColumns.length < schemaTotal) return null;
      const centerIds = [];
      const seenCenterIds = new Set();
      for (let index = 0; index < schemaTotal; index += 1) {
        const entry = schemaColumns[index];
        const columnId = entry && entry.id;
        if (!isExportableColumnId(columnId)) return null;
        if (seenCenterIds.has(columnId)) continue;
        centerIds.push(columnId);
        seenCenterIds.add(columnId);
      }
      const orderedIds = [];
      const seen = new Set();
      const pushId = columnId => {
        if (!isExportableColumnId(columnId) || seen.has(columnId)) return;
        orderedIds.push(columnId);
        seen.add(columnId);
      };
      knownIds.filter(columnId => !seenCenterIds.has(columnId) && !String(columnId).startsWith('__RowTotal__')).forEach(pushId);
      // TanStack's visible leaf list carries the user's live column order.
      // Preserve that order for instantiated center columns, then fill any
      // virtual/off-screen gaps from the complete cached schema.
      knownIds.filter(columnId => seenCenterIds.has(columnId)).forEach(pushId);
      centerIds.forEach(pushId);
      knownIds.filter(columnId => String(columnId).startsWith('__RowTotal__')).forEach(pushId);
      return orderedIds.length > 0 ? orderedIds : null;
    }
    return null;
  }, [cachedColSchema, getInstantiatedExportColIds, isExportableColumnId, serverSide]);

  const getServerExportColumnTarget = useCallback(({ currentTable = false } = {}) => {
    const knownColIds = getInstantiatedExportColIds();
    // Current-table copy is a representation of the grid at this moment:
    // preserve its visible columns and their actual order instead of filling
    // in off-screen virtual columns from the server schema.
    if (currentTable && knownColIds.length > 0) {
      return {
        mode: 'ids',
        colIds: knownColIds,
        knownColIds,
        totalCols: knownColIds.length,
      };
    }
    const completeColIds = getCompleteServerExportColIds();
    if (Array.isArray(completeColIds) && completeColIds.length > 0) {
      return {
        mode: 'ids',
        colIds: completeColIds,
        knownColIds,
        totalCols: completeColIds.length,
      };
    }
    const centerCount = Number.isFinite(Number(totalCenterCols)) ? Math.max(0, Math.floor(Number(totalCenterCols))) : 0;
    return {
      mode: 'window',
      colIds: null,
      knownColIds,
      totalCols: Math.max(centerCount, knownColIds.length, 1),
    };
  }, [getCompleteServerExportColIds, getInstantiatedExportColIds, totalCenterCols]);

  const getExportColumns = useCallback(columnsOverride => {
    const columns = Array.isArray(columnsOverride) ? columnsOverride : table && typeof table.getVisibleLeafColumns === 'function' ? table.getVisibleLeafColumns() : [];
    return columns.filter(column => column && isExportableColumnId(column.id));
  }, [isExportableColumnId, table]);

  const getExportHeaderLabel = useCallback((column, mergedHeaders = false, separator = mergedHeaderSeparator) => {
    if (!column) return '';
    if (!mergedHeaders) {
      return getDisplayHeaderTextForColumn(column) || (column.columnDef && typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id);
    }
    const stack = getDisplayHeaderTextStackForColumn(column);
    if (stack.length <= 1) return stack[0] || getDisplayHeaderTextForColumn(column) || column.id;
    const leafLabel = (stack[stack.length - 1] || '').replace(/\s*\([^)]+\)$/, '');
    return [...stack.slice(0, -1), leafLabel].filter(Boolean).join(separator || ' ');
  }, [getDisplayHeaderTextForColumn, getDisplayHeaderTextStackForColumn, mergedHeaderSeparator]);

  const getExportRowData = useCallback(rowLike => rowLike && rowLike.original !== undefined ? rowLike.original : rowLike, []);
  const getExportRowId = useCallback((rowLike, rowData, rowIndex) => {
    if (rowData && rowData._path !== undefined && rowData._path !== null && rowData._path !== '') return String(rowData._path);
    if (rowLike && rowLike.id !== undefined && rowLike.id !== null && rowLike.id !== '') return String(rowLike.id);
    if (rowData && rowData.id !== undefined && rowData.id !== null && rowData.id !== '') return String(rowData.id);
    if (rowData && rowData._id !== undefined && rowData._id !== null && rowData._id !== '') return String(rowData._id);
    return String(rowIndex);
  }, []);

  const getExportRawCellValue = useCallback((rowLike, column, rowIndex, overrideValue = undefined) => {
    if (overrideValue !== undefined) return overrideValue;
    if (!column) return '';
    const rowData = getExportRowData(rowLike) || {};
    if (column.id === 'hierarchy') {
      const label = rowData._label !== undefined && rowData._label !== null ? rowData._label : rowData._id !== undefined && rowData._id !== null ? rowData._id : '';
      return formatDisplayLabel(String(label));
    }
    if (rowLike && typeof rowLike.getValue === 'function') {
      const value = rowLike.getValue(column.id);
      return value === undefined || value === null ? '' : value;
    }
    const columnDef = column.columnDef || {};
    if (typeof columnDef.accessorFn === 'function') {
      const value = columnDef.accessorFn(rowData, rowIndex);
      return value === undefined || value === null ? '' : value;
    }
    if (columnDef.accessorKey && rowData) {
      const value = rowData[columnDef.accessorKey];
      return value === undefined || value === null ? '' : value;
    }
    if (rowData && column.id && Object.prototype.hasOwnProperty.call(rowData, column.id)) {
      const value = rowData[column.id];
      return value === undefined || value === null ? '' : value;
    }
    return '';
  }, [getExportRowData]);

  const getExportGroupSeparator = useCallback(groupSeparator => (
    groupSeparator === 'thin_space' || groupSeparator === 'space' ? 'plain_space' : groupSeparator
  ), []);

  const getExportCellDisplayValue = useCallback((rowLike, column, rowIndex, overrideValue = undefined, rawValues = false) => {
    if (!column) return '';
    const rowData = getExportRowData(rowLike) || {};
    const rawValue = getExportRawCellValue(rowLike, column, rowIndex, overrideValue);
    if (column.id === 'hierarchy') {
      const depth = Number(rowData.depth != null ? rowData.depth : rowLike && rowLike.depth != null ? rowLike.depth : 0) || 0;
      return `${'  '.repeat(Math.max(0, depth))}${rawValue === undefined || rawValue === null ? '' : String(rawValue)}`;
    }
    const rowId = getExportRowId(rowLike, rowData, rowIndex);
    const displayedValue = typeof resolveDisplayedCellValue === 'function' ? resolveDisplayedCellValue(rowId, column.id, rawValue) : rawValue;
    if (displayedValue === undefined || displayedValue === null) return '';
    if (rawValues && typeof displayedValue === 'number') return String(displayedValue);
    if (typeof displayedValue === 'number' && Number.isFinite(displayedValue)) {
      const matchingValueConfig = Array.isArray(valConfigs) ? valConfigs.find(config => matchesSelectedColumnToValueConfig(column.id, config)) || null : null;
      const effectiveDecimalPlaces = columnDecimalOverrides && columnDecimalOverrides[column.id] !== undefined ? columnDecimalOverrides[column.id] : decimalPlaces;
      const effectiveFormat = Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, column.id) ? columnFormatOverrides[column.id] : matchingValueConfig && matchingValueConfig.format || defaultValueFormat || null;
      const effectiveGroupSeparator = columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[column.id] !== undefined ? columnGroupSeparatorOverrides[column.id] : numberGroupSeparator;
      return formatValue(displayedValue, effectiveFormat, effectiveDecimalPlaces, getExportGroupSeparator(effectiveGroupSeparator));
    }
    return String(displayedValue);
  }, [columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, decimalPlaces, defaultValueFormat, getExportGroupSeparator, getExportRawCellValue, getExportRowData, getExportRowId, matchesSelectedColumnToValueConfig, numberGroupSeparator, resolveDisplayedCellValue, valConfigs]);

  const getExportHeaderStyle = useCallback(column => {
    const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
    const underGrandTotal = parentText === 'Grand Total' || parentText.startsWith('Grand Total');
    return {
      fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif",
      fontSize: fontSize || '13px', fontWeight: 700,
      color: underGrandTotal ? theme.totalText || theme.text : theme.headerText || theme.text,
      background: underGrandTotal ? theme.totalBgStrong || theme.totalBg || theme.select : theme.headerBg || theme.headerSubtleBg || theme.surfaceBg || theme.background,
      border: `1px solid ${theme.border}`, padding: '6px 10px', textAlign: 'center',
    };
  }, [fontFamily, fontSize, getDisplayHeaderTextForColumn, theme]);

  const getExportCellStyle = useCallback((rowLike, column, rowIndex, overrideValue = undefined) => {
    const rowData = getExportRowData(rowLike) || {};
    const rowId = getExportRowId(rowLike, rowData, rowIndex);
    const isHierarchy = column && column.id === 'hierarchy';
    const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
    const isTotalCol = !isHierarchy && parentText && (parentText === 'Grand Total' || parentText.startsWith('Grand Total'));
    const isGrandTotalRow = !!(rowData._isTotal || rowData.__isGrandTotal__ || rowData._path === '__grand_total__' || rowData._id === 'Grand Total');
    const rawValue = getExportRawCellValue(rowLike, column, rowIndex, overrideValue);
    const resolvedValue = typeof resolveDisplayedCellValue === 'function' ? resolveDisplayedCellValue(rowId, column ? column.id : '', rawValue) : rawValue;
    const condStyle = column && !isHierarchy && typeof getConditionalStyle === 'function' ? getConditionalStyle(column.id, resolvedValue, rowData, rowId) : {};
    const reportFormat = rowData._reportFormat && typeof rowData._reportFormat === 'object' ? rowData._reportFormat : null;
    const cellKey = `${rowId}:::${column ? column.id : ''}`;
    const cellFmt = cellFormatRules && cellFormatRules[cellKey];
    const depth = Number(rowData.depth != null ? rowData.depth : rowLike && rowLike.depth != null ? rowLike.depth : 0) || 0;
    const background = isGrandTotalRow ? theme.totalBgStrong || theme.totalBg || theme.select || theme.background : isTotalCol ? theme.totalBg || theme.select || theme.background : isHierarchy ? theme.hierarchyBg || theme.surfaceInset || theme.surfaceBg || theme.background : theme.surfaceBg || theme.background || '#fff';
    return {
      fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px', height: rowHeight || undefined,
      color: cellFmt && cellFmt.color ? cellFmt.color : isGrandTotalRow ? theme.totalText || theme.text : isHierarchy ? theme.text : theme.textSec || theme.text,
      background,
      border: cellFmt && cellFmt.borderStyle ? `${Math.max(1, Math.min(5, Number(cellFmt.borderWidth) || 1))}px ${cellFmt.borderStyle} ${cellFmt.borderColor || theme.border}` : reportFormat && reportFormat.borderStyle ? `${Math.max(1, Number(reportFormat.borderWidth) || 1)}px ${reportFormat.borderStyle} ${reportFormat.borderColor || theme.border}` : `1px solid ${theme.border}`,
      padding: isHierarchy ? `4px 10px 4px ${Math.max(10, 10 + depth * HIERARCHY_INDENT_PX)}px` : '4px 10px', textAlign: cellFmt && ['left', 'center', 'right'].includes(cellFmt.textAlign) ? cellFmt.textAlign : isHierarchy ? 'left' : 'right', whiteSpace: isHierarchy ? 'pre' : 'nowrap', fontVariantNumeric: isHierarchy ? undefined : 'tabular-nums', fontWeight: cellFmt && cellFmt.bold ? 700 : reportFormat && reportFormat.bold ? 700 : isGrandTotalRow ? 700 : isTotalCol ? 600 : 400, fontStyle: cellFmt && cellFmt.italic ? 'italic' : undefined,
      ...(reportFormat && reportFormat.rowColor ? { background: reportFormat.rowColor } : null), ...condStyle, ...(cellFmt && cellFmt.bg ? { background: cellFmt.bg } : null),
    };
  }, [HIERARCHY_INDENT_PX, cellFormatRules, fontFamily, fontSize, getConditionalStyle, getDisplayHeaderTextForColumn, getExportRawCellValue, getExportRowData, getExportRowId, resolveDisplayedCellValue, rowHeight, theme]);

  const buildTableClipboardPayload = useCallback(({
    withHeaders = true, mergedHeaders = false, rows = null, columns = null,
  } = {}) => {
    const exportColumns = getExportColumns(columns);
    const exportRows = Array.isArray(rows) ? rows : serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : getDisplayRows();
    if (!Array.isArray(exportRows) || exportRows.length === 0 || exportColumns.length === 0) return null;
    return buildStyledHtmlTableExport({
      table, rows: exportRows, columns: exportColumns, includeHeaders: withHeaders,
      getHeaderLabel: column => getExportHeaderLabel(column, mergedHeaders, mergedHeaderSeparator), getHeaderStyle: getExportHeaderStyle,
      getCellValue: getExportCellDisplayValue, getCellStyle: (rowLike, column, _value, rowIndex) => getExportCellStyle(rowLike, column, rowIndex),
      tableStyle: { fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px' },
    });
  }, [fontFamily, fontSize, getDisplayRows, getExportCellDisplayValue, getExportCellStyle, getExportColumns, getExportHeaderLabel, getExportHeaderStyle, loadedRows, mergedHeaderSeparator, serverSide, table]);

  const buildAllRowsClipboardPayload = useCallback(({
    withHeaders = true, mergedHeaders = false,
  } = {}) => {
    if (!table || typeof table.getPreExpandedRowModel !== 'function') {
      return buildTableClipboardPayload({ withHeaders, mergedHeaders });
    }
    const rowModel = table.getPreExpandedRowModel();
    const rows = rowModel && Array.isArray(rowModel.flatRows) ? rowModel.flatRows : [];
    return buildTableClipboardPayload({ withHeaders, mergedHeaders, rows });
  }, [buildTableClipboardPayload, table]);

  const buildSelectedClipboardPayload = useCallback((withHeaders = false, selectionMap = selectedCells, { rawValues = false } = {}) => {
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;
    const visibleRows = getDisplayRows();
    const visibleCols = table && typeof table.getVisibleLeafColumns === 'function' ? table.getVisibleLeafColumns() : [];
    const seenColumnIds = new Set();
    const orderedCols = visibleCols.filter(column => {
      if (!column || seenColumnIds.has(column.id)) return false;
      seenColumnIds.add(column.id);
      return true;
    });
    const rowIdMap = {}; visibleRows.forEach((row, index) => { rowIdMap[row.id] = index; });
    const colIdMap = {}; orderedCols.forEach((column, index) => { colIdMap[column.id] = index; });
    let minR = Infinity; let maxR = -1; let minC = Infinity; let maxC = -1;
    const selectedGrid = {};
    keys.forEach(key => {
      const separatorIndex = key.lastIndexOf(':'); if (separatorIndex <= 0) return;
      const rowIndex = rowIdMap[key.slice(0, separatorIndex)]; const colIndex = colIdMap[key.slice(separatorIndex + 1)];
      if (rowIndex === undefined || colIndex === undefined) return;
      minR = Math.min(minR, rowIndex); maxR = Math.max(maxR, rowIndex); minC = Math.min(minC, colIndex); maxC = Math.max(maxC, colIndex);
      if (!selectedGrid[rowIndex]) selectedGrid[rowIndex] = {}; selectedGrid[rowIndex][colIndex] = selectionMap[key];
    });
    if (minR === Infinity) return null;
    const exportRows = visibleRows.slice(minR, maxR + 1);
    const exportColumns = orderedCols.slice(minC, maxC + 1).filter(column => isExportableColumnId(column.id));
    if (exportRows.length === 0 || exportColumns.length === 0) return null;
    return buildStyledHtmlTableExport({
      rows: exportRows, columns: exportColumns, includeHeaders: withHeaders,
      getHeaderLabel: column => getExportHeaderLabel(column, false, mergedHeaderSeparator), getHeaderStyle: getExportHeaderStyle,
      getCellValue: (rowLike, column, rowIndex) => {
        const absoluteRowIndex = minR + rowIndex; const absoluteColIndex = colIdMap[column.id];
        if (!selectedGrid[absoluteRowIndex] || !Object.prototype.hasOwnProperty.call(selectedGrid[absoluteRowIndex], absoluteColIndex)) return '';
        const overrideValue = selectedGrid[absoluteRowIndex][absoluteColIndex];
        return getExportCellDisplayValue(rowLike, column, absoluteRowIndex, overrideValue, rawValues);
      },
      getCellStyle: (rowLike, column, _value, rowIndex) => {
        const absoluteRowIndex = minR + rowIndex; const absoluteColIndex = colIdMap[column.id];
        const overrideValue = selectedGrid[absoluteRowIndex] && Object.prototype.hasOwnProperty.call(selectedGrid[absoluteRowIndex], absoluteColIndex) ? selectedGrid[absoluteRowIndex][absoluteColIndex] : '';
        return getExportCellStyle(rowLike, column, absoluteRowIndex, overrideValue);
      },
      tableStyle: { fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px' },
    });
  }, [fontFamily, fontSize, getDisplayRows, getExportCellDisplayValue, getExportCellStyle, getExportHeaderLabel, getExportHeaderStyle, isExportableColumnId, mergedHeaderSeparator, selectedCells, table]);

  const buildExportStyleProfile = useCallback((colIds, { withHeaders = true, mergedHeaders = false } = {}) => {
    const columnsById = new Map();
    if (table && typeof table.getVisibleLeafColumns === 'function') table.getVisibleLeafColumns().forEach(column => { if (column && column.id) columnsById.set(column.id, column); });
    const requestedIds = Array.isArray(colIds) && colIds.length > 0 ? colIds.filter(isExportableColumnId) : getInstantiatedExportColIds();
    const headerLabels = {}; const columnWidths = {};
    const exportColumns = requestedIds.map(columnId => {
      const column = columnsById.get(columnId) || (table && typeof table.getColumn === 'function' ? table.getColumn(columnId) : null);
      const exportField = columnId === 'hierarchy' ? '_id' : columnId;
      const label = column ? getExportHeaderLabel(column, mergedHeaders, mergedHeaderSeparator) : String(columnId || '');
      headerLabels[columnId] = label; headerLabels[exportField] = label;
      const width = column && typeof column.getSize === 'function' ? Number(column.getSize()) : null;
      if (Number.isFinite(width) && width > 0) { columnWidths[columnId] = Math.round(width); columnWidths[exportField] = Math.round(width); }
      return column || { id: columnId, __pivotExportFallbackHeader: label };
    });
    const mergedHeaderRows = mergedHeaders ? buildMergedHeaderRows(exportColumns, {
      getHeaderStack: column => column && column.__pivotExportFallbackHeader
        ? [column.__pivotExportFallbackHeader]
        : getDisplayHeaderTextStackForColumn(column),
      getHeaderLabel: column => column && column.__pivotExportFallbackHeader
        ? column.__pivotExportFallbackHeader
        : getDisplayHeaderTextForColumn(column),
    }).rows.map(({ cells }) => cells.map(({ label, columnIds, colSpan, rowSpan, styleColumnId }) => ({
      label, columnIds, colSpan, rowSpan, styleColumnId,
    }))) : [];
    return {
      includeHeaders: withHeaders, mergedHeaders, mergedHeaderSeparator, fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px', rowHeight,
      theme: { text: theme.text, textSec: theme.textSec, border: theme.border, background: theme.background, surfaceBg: theme.surfaceBg, surfaceInset: theme.surfaceInset, headerBg: theme.headerBg, headerSubtleBg: theme.headerSubtleBg, headerText: theme.headerText, hierarchyBg: theme.hierarchyBg, totalBg: theme.totalBg, totalBgStrong: theme.totalBgStrong, totalText: theme.totalText, select: theme.select, primary: theme.primary },
      headerLabels, mergedHeaderRows, headerFormatting, columnWidths, decimalPlaces, defaultValueFormat, numberGroupSeparator, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, valConfigs, conditionalFormatting, cellFormatRules, colorScaleMode, colorPalette,
      colorScaleStats: colorScaleStats ? { byCol: colorScaleStats.byCol || {}, table: colorScaleStats.table || null } : null,
    };
  }, [cellFormatRules, colorPalette, colorScaleMode, colorScaleStats, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, conditionalFormatting, decimalPlaces, defaultValueFormat, fontFamily, fontSize, getDisplayHeaderTextForColumn, getDisplayHeaderTextStackForColumn, getExportHeaderLabel, getInstantiatedExportColIds, headerFormatting, isExportableColumnId, mergedHeaderSeparator, numberGroupSeparator, rowHeight, table, theme, valConfigs]);

  const copyToClipboard = useCallback(payload => writeClipboardPayload(payload && typeof payload === 'object' && (payload.html !== undefined || payload.text !== undefined) ? payload : { text: payload }), []);
  const copyTextToClipboard = useCallback((text, successMessage = 'Copied!') => {
    if (!text) return Promise.resolve(false);
    return copyToClipboard(text).then(() => { if (successMessage) showNotification(successMessage, 'success'); return true; }).catch(error => { console.warn('Copy to clipboard failed:', error); showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; });
  }, [copyToClipboard, showNotification]);
  const copyRichToClipboard = useCallback((payload, successMessage = 'Copied!') => {
    if (!payload || !payload.text && !payload.html) return Promise.resolve(false);
    return copyToClipboard(payload).then(() => { if (successMessage) showNotification(successMessage, 'success'); return true; }).catch(error => {
      console.warn('Rich copy to clipboard failed:', error);
      if (!payload.text) { showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; }
      return copyToClipboard(payload.text).then(() => { showNotification('Copied without formatting. Browser blocked HTML clipboard access.', 'warning'); return true; }).catch(fallbackError => { console.warn('Plain text clipboard fallback failed:', fallbackError); showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; });
    });
  }, [copyToClipboard, showNotification]);
  const triggerExportDownload = useCallback((url, filename) => {
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = filename || 'pivot_export.xlsx'; document.body.appendChild(anchor); anchor.click(); document.body.removeChild(anchor);
  }, []);

  return {
    EXPORT_SKIP_COL_IDS,
    buildAllRowsClipboardPayload, buildExportStyleProfile, buildSelectedClipboardPayload, buildTableClipboardPayload,
    copyRichToClipboard, copyTextToClipboard, copyToClipboard,
    getCompleteServerExportColIds, getExportCellDisplayValue, getExportCellStyle,
    getExportColumns, getExportGroupSeparator, getExportHeaderLabel, getExportHeaderStyle,
    getExportRawCellValue, getExportRowData, getExportRowId, getInstantiatedExportColIds,
    getServerExportColumnTarget, isExportableColumnId, triggerExportDownload,
  };
}

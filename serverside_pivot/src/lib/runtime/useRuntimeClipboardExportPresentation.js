import { useCallback } from 'react';

import { buildMergedHeaderRows, buildStyledHtmlTableExport, writeClipboardPayload } from '../utils/exportUtils';
import { formatDisplayLabel, formatValue, isDisplayNegative, resolveNumericDisplayValue } from '../utils/helpers';

/** @type {any} */
const EXPORT_SKIP_COL_IDS = new Set(['__row_number__', '__report_config__']);

/**
 * Owns the client-side representation of clipboard and export data.
 *
 * This deliberately has no dependency on the legacy runtime environment. The
 * interaction stage supplies the individual data, formatting, and table
 * capabilities it needs, while export dispatch is owned separately by
 * createRuntimeExportCommands.
  @param {any} p0 @returns {any} */
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
  unitSuffix,
  valConfigs,
  valueScale,
}) {
  /** @type {(...args: any[]) => any} */
  const isExportableColumnId = useCallback(columnId => {
    /** @type {any} */
    const id = String(columnId || '');
    return !!id && !EXPORT_SKIP_COL_IDS.has(id) && !id.startsWith('__schema_placeholder__');
  }, []);

  /** @type {(...args: any[]) => any} */
  const getDisplayOrderedLeafColumns = useCallback(() => {
    if (!table) return [];
    /** @type {any} */
    const hasPinnedRegions = ['getLeftVisibleLeafColumns', 'getCenterVisibleLeafColumns', 'getRightVisibleLeafColumns']
      .every(method => typeof table[method] === 'function');
    /** @type {any} */
    const columns = hasPinnedRegions
      ? [
        ...table.getLeftVisibleLeafColumns(),
        ...table.getCenterVisibleLeafColumns(),
        ...table.getRightVisibleLeafColumns(),
      ]
      : typeof table.getVisibleLeafColumns === 'function' ? table.getVisibleLeafColumns() : [];
    /** @type {any} */
    const seen = new Set();
    return columns.filter(/** @param {any} [column] @returns {any} */ column => {
      if (!column || !column.id || seen.has(column.id)) return false;
      seen.add(column.id);
      return true;
    });
  }, [table]);

  /** @type {(...args: any[]) => any} */
  const getInstantiatedExportColIds = useCallback(() => {
    return getDisplayOrderedLeafColumns().filter(/** @param {any} [column] @returns {any} */ column => isExportableColumnId(column.id)).map(/** @param {any} [column] @returns {any} */ column => column.id);
  }, [getDisplayOrderedLeafColumns, isExportableColumnId]);

  /** @type {(...args: any[]) => any} */
  const getCompleteServerExportColIds = useCallback(() => {
    /** @type {any} */
    const knownIds = getInstantiatedExportColIds();
    if (!serverSide) return knownIds;
    /** @type {any} */
    const rightPinnedIds = table && typeof table.getRightVisibleLeafColumns === 'function'
      ? table.getRightVisibleLeafColumns().map(/** @param {any} [column] @returns {any} */ column => column && column.id).filter(isExportableColumnId)
      : [];
    /** @type {any} */
    const rightPinnedSet = new Set(rightPinnedIds);
    /** @type {any} */
    const schemaTotal = cachedColSchema && Number.isFinite(Number(cachedColSchema.total_center_cols)) ? Math.max(0, Math.floor(Number(cachedColSchema.total_center_cols))) : null;
    /** @type {any} */
    const schemaColumns = cachedColSchema && Array.isArray(cachedColSchema.columns) ? cachedColSchema.columns : [];
    if (schemaTotal && schemaTotal > 0) {
      if (schemaColumns.length < schemaTotal) return null;
      /** @type {any[]} */
      const centerIds = [];
      /** @type {any} */
      const seenCenterIds = new Set();
      for (let index = 0; index < schemaTotal; index += 1) {
        const entry = schemaColumns[index];
        /** @type {any} */
        const columnId = entry && entry.id;
        if (!isExportableColumnId(columnId)) return null;
        if (seenCenterIds.has(columnId)) continue;
        centerIds.push(columnId);
        seenCenterIds.add(columnId);
      }
      /** @type {any[]} */
      const orderedIds = [];
      /** @type {any} */
      const seen = new Set();
      /** @param {any} [columnId] @returns {any} */
      const pushId = columnId => {
        if (!isExportableColumnId(columnId) || seen.has(columnId)) return;
        orderedIds.push(columnId);
        seen.add(columnId);
      };
      knownIds.filter(/** @param {any} [columnId] @returns {any} */ columnId => !rightPinnedSet.has(columnId) && !seenCenterIds.has(columnId)
        && !String(columnId).startsWith('__RowTotal__')).forEach(pushId);
      // Preserve live display order for instantiated center columns, then
      // fill virtual/off-screen gaps before right-pinned columns.
      knownIds.filter(/** @param {any} [columnId] @returns {any} */ columnId => !rightPinnedSet.has(columnId) && seenCenterIds.has(columnId)).forEach(pushId);
      centerIds.filter(columnId => !rightPinnedSet.has(columnId)).forEach(pushId);
      knownIds.filter(/** @param {any} [columnId] @returns {any} */ columnId => !rightPinnedSet.has(columnId) && String(columnId).startsWith('__RowTotal__')).forEach(pushId);
      rightPinnedIds.forEach(pushId);
      return orderedIds.length > 0 ? orderedIds : null;
    }
    return null;
  }, [cachedColSchema, getInstantiatedExportColIds, isExportableColumnId, serverSide, table]);

  /** @type {(...args: any[]) => any} */
  const getServerExportColumnTarget = useCallback(({ currentTable = false } = {}) => {
    /** @type {any} */
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
    /** @type {any} */
    const completeColIds = getCompleteServerExportColIds();
    if (Array.isArray(completeColIds) && completeColIds.length > 0) {
      return {
        mode: 'ids',
        colIds: completeColIds,
        knownColIds,
        totalCols: completeColIds.length,
      };
    }
    /** @type {any} */
    const centerCount = Number.isFinite(Number(totalCenterCols)) ? Math.max(0, Math.floor(Number(totalCenterCols))) : 0;
    return {
      mode: 'window',
      colIds: null,
      knownColIds,
      totalCols: Math.max(centerCount, knownColIds.length, 1),
    };
  }, [getCompleteServerExportColIds, getInstantiatedExportColIds, totalCenterCols]);

  /** @type {(...args: any[]) => any} */
  const getExportColumns = useCallback(columnsOverride => {
    /** @type {any} */
    const columns = Array.isArray(columnsOverride) ? columnsOverride : getDisplayOrderedLeafColumns();
    return columns.filter(/** @param {any} [column] @returns {any} */ column => column && isExportableColumnId(column.id));
  }, [getDisplayOrderedLeafColumns, isExportableColumnId]);

  /** @type {(...args: any[]) => any} */
  const getExportHeaderLabel = useCallback((column, mergedHeaders = false, separator = mergedHeaderSeparator) => {
    if (!column) return '';
    if (!mergedHeaders) {
      return getDisplayHeaderTextForColumn(column) || (column.columnDef && typeof column.columnDef.header === 'string' ? column.columnDef.header : column.id);
    }
    /** @type {any} */
    const stack = getDisplayHeaderTextStackForColumn(column);
    if (stack.length <= 1) return stack[0] || getDisplayHeaderTextForColumn(column) || column.id;
    /** @type {any} */
    const leafLabel = (stack[stack.length - 1] || '').replace(/\s*\([^)]+\)$/, '');
    return [...stack.slice(0, -1), leafLabel].filter(Boolean).join(separator || ' ');
  }, [getDisplayHeaderTextForColumn, getDisplayHeaderTextStackForColumn, mergedHeaderSeparator]);

  /** @type {(...args: any[]) => any} */
  const getExportRowData = useCallback(rowLike => rowLike && rowLike.original !== undefined ? rowLike.original : rowLike, []);
  /** @type {(...args: any[]) => any} */
  const getExportRowId = useCallback((rowLike, rowData, rowIndex) => {
    if (rowData && rowData._path !== undefined && rowData._path !== null && rowData._path !== '') return String(rowData._path);
    if (rowLike && rowLike.id !== undefined && rowLike.id !== null && rowLike.id !== '') return String(rowLike.id);
    if (rowData && rowData.id !== undefined && rowData.id !== null && rowData.id !== '') return String(rowData.id);
    if (rowData && rowData._id !== undefined && rowData._id !== null && rowData._id !== '') return String(rowData._id);
    return String(rowIndex);
  }, []);

  /** @type {(...args: any[]) => any} */
  const getExportRawCellValue = useCallback((rowLike, column, rowIndex, overrideValue = undefined) => {
    if (overrideValue !== undefined) return overrideValue;
    if (!column) return '';
    /** @type {any} */
    const rowData = getExportRowData(rowLike) || {};
    if (column.id === 'hierarchy') {
      /** @type {any} */
      const label = rowData._label !== undefined && rowData._label !== null ? rowData._label : rowData._id !== undefined && rowData._id !== null ? rowData._id : '';
      return formatDisplayLabel(String(label));
    }
    if (rowLike && typeof rowLike.getValue === 'function') {
      /** @type {any} */
      const value = rowLike.getValue(column.id);
      return value === undefined || value === null ? '' : value;
    }
    /** @type {any} */
    const columnDef = column.columnDef || {};
    if (typeof columnDef.accessorFn === 'function') {
      /** @type {any} */
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

  /** @type {(...args: any[]) => any} */
  const getExportGroupSeparator = useCallback(groupSeparator => (
    groupSeparator === 'thin_space' || groupSeparator === 'space' ? 'plain_space' : groupSeparator
  ), []);

  /** @type {(...args: any[]) => any} */
  const getExportCellDisplayValue = useCallback((rowLike, column, rowIndex, overrideValue = undefined, rawValues = false) => {
    if (!column) return '';
    /** @type {any} */
    const rowData = getExportRowData(rowLike) || {};
    /** @type {any} */
    const rawValue = getExportRawCellValue(rowLike, column, rowIndex, overrideValue);
    if (column.id === 'hierarchy') {
      /** @type {any} */
      const depth = Number(rowData.depth != null ? rowData.depth : rowLike && rowLike.depth != null ? rowLike.depth : 0) || 0;
      return `${'  '.repeat(Math.max(0, depth))}${rawValue === undefined || rawValue === null ? '' : String(rawValue)}`;
    }
    /** @type {any} */
    const rowId = getExportRowId(rowLike, rowData, rowIndex);
    /** @type {any} */
    const displayedValue = typeof resolveDisplayedCellValue === 'function' ? resolveDisplayedCellValue(rowId, column.id, rawValue) : rawValue;
    if (displayedValue === undefined || displayedValue === null) return '';
    if (rawValues && typeof displayedValue === 'number') return String(displayedValue);
    if (typeof displayedValue === 'number' && Number.isFinite(displayedValue)) {
      /** @type {any} */
      const matchingValueConfig = Array.isArray(valConfigs) ? valConfigs.find(config => matchesSelectedColumnToValueConfig(column.id, config)) || null : null;
      /** @type {any} */
      const effectiveDecimalPlaces = columnDecimalOverrides && columnDecimalOverrides[column.id] !== undefined ? columnDecimalOverrides[column.id] : decimalPlaces;
      /** @type {any} */
      const effectiveFormat = Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, column.id) ? columnFormatOverrides[column.id] : matchingValueConfig && matchingValueConfig.format || defaultValueFormat || null;
      /** @type {any} */
      const effectiveGroupSeparator = columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[column.id] !== undefined ? columnGroupSeparatorOverrides[column.id] : numberGroupSeparator;
      /** @type {any} */
      const effectiveScale = matchingValueConfig && matchingValueConfig.valueScale !== undefined ? matchingValueConfig.valueScale : valueScale;
      /** @type {any} */
      const effectiveSuffix = matchingValueConfig && matchingValueConfig.unitSuffix !== undefined ? matchingValueConfig.unitSuffix : unitSuffix;
      return formatValue(displayedValue, effectiveFormat, effectiveDecimalPlaces, getExportGroupSeparator(effectiveGroupSeparator), {
        valueScale: effectiveScale,
        unitSuffix: effectiveSuffix,
      });
    }
    return String(displayedValue);
  }, [columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, decimalPlaces, defaultValueFormat, getExportGroupSeparator, getExportRawCellValue, getExportRowData, getExportRowId, matchesSelectedColumnToValueConfig, numberGroupSeparator, resolveDisplayedCellValue, unitSuffix, valConfigs, valueScale]);

  /** @type {(...args: any[]) => any} */
  const getClipboardCellDisplayValue = useCallback((rowLike, column, rowIndex, overrideValue = undefined, rawValues = false) => {
    if (column && column.id === 'hierarchy') {
      /** @type {any} */
      const rowData = getExportRowData(rowLike) || {};
      /** @type {any} */
      const label = rowData._label !== undefined && rowData._label !== null ? rowData._label : rowData._id !== undefined && rowData._id !== null ? rowData._id : '';
      // Selected values may already be formatted. Recover source separators
      // only for clipboard payloads, retaining ordinary export formatting.
      if (/[\t\r\n]/.test(String(label))) overrideValue = String(label);
    }
    return getExportCellDisplayValue(rowLike, column, rowIndex, overrideValue, rawValues);
  }, [getExportCellDisplayValue, getExportRowData]);

  /** @type {(...args: any[]) => any} */
  const getExportHeaderStyle = useCallback(column => {
    /** @type {any} */
    const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
    /** @type {any} */
    const underGrandTotal = parentText === 'Grand Total' || parentText.startsWith('Grand Total');
    /** @type {any} */
    const isGroupCol = Boolean(column && Array.isArray(column.columns) && column.columns.length > 0);
    return {
      fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif",
      fontSize: fontSize || '13px', fontWeight: theme.headerFontWeight ?? 700,
      color: underGrandTotal
        ? (theme.totalTextStrong || theme.totalText || theme.text)
        : isGroupCol
          ? (theme.headerGroupText || theme.headerText || theme.text)
          : (theme.headerText || theme.text),
      background: underGrandTotal
        ? (theme.totalBgStrong || theme.totalBg || theme.select)
        : isGroupCol
          ? (theme.headerGroupBg || theme.headerBg || theme.headerSubtleBg || theme.surfaceBg || theme.background)
          : (theme.headerBg || theme.headerSubtleBg || theme.surfaceBg || theme.background),
      border: `1px solid ${theme.headerBorderBottom || theme.border}`, padding: '6px 10px', textAlign: 'center',
    };
  }, [fontFamily, fontSize, getDisplayHeaderTextForColumn, theme]);

  /** @type {(...args: any[]) => any} */
  const getExportCellStyle = useCallback((rowLike, column, rowIndex, overrideValue = undefined) => {
    /** @type {any} */
    const rowData = getExportRowData(rowLike) || {};
    /** @type {any} */
    const rowId = getExportRowId(rowLike, rowData, rowIndex);
    /** @type {any} */
    const isHierarchy = column && column.id === 'hierarchy';
    /** @type {any} */
    const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
    /** @type {any} */
    const isTotalCol = !isHierarchy && parentText && (parentText === 'Grand Total' || parentText.startsWith('Grand Total'));
    const isGrandTotalRow = !!(rowData._isTotal || rowData.__isGrandTotal__ || rowData._path === '__grand_total__' || rowData._id === 'Grand Total');
    /** @type {any} */
    const rawValue = getExportRawCellValue(rowLike, column, rowIndex, overrideValue);
    /** @type {any} */
    const resolvedValue = typeof resolveDisplayedCellValue === 'function' ? resolveDisplayedCellValue(rowId, column ? column.id : '', rawValue) : rawValue;
    /** @type {any} */
    const condStyle = column && !isHierarchy && typeof getConditionalStyle === 'function' ? getConditionalStyle(column.id, resolvedValue, rowData, rowId) : {};
    /** @type {any} */
    const reportFormat = rowData._reportFormat && typeof rowData._reportFormat === 'object' ? rowData._reportFormat : null;
    const cellKey = `${rowId}:::${column ? column.id : ''}`;
    /** @type {any} */
    const cellFmt = cellFormatRules && cellFormatRules[cellKey];
    /** @type {any} */
    const depth = Number(rowData.depth != null ? rowData.depth : rowLike && rowLike.depth != null ? rowLike.depth : 0) || 0;
    /** @type {any} */
    const subRows = rowLike && Array.isArray(rowLike.subRows) ? rowLike.subRows : null;
    /** @type {any} */
    const hasChildren = Boolean(rowData._has_children || (subRows && subRows.length > 0));
    /** @type {any} */
    const isLeafRow = !isGrandTotalRow && !hasChildren;
    /** @param {any} [arrOrVal] @param {any} [d] @returns {any} */
    const resolveDepthVal = (arrOrVal, d) => Array.isArray(arrOrVal) && arrOrVal.length > 0 ? arrOrVal[Math.min(Math.max(0, d), arrOrVal.length - 1)] : arrOrVal;
    /** @type {any} */
    const groupBgVal = resolveDepthVal(theme.groupBg, depth);
    /** @type {any} */
    const groupTextVal = resolveDepthVal(theme.groupText, depth);
    /** @type {any} */
    const groupWeightVal = resolveDepthVal(theme.groupFontWeight, depth);
    /** @type {any} */
    const background = isGrandTotalRow
      ? (theme.totalBgStrong || theme.totalBg || theme.select || theme.background)
      : isTotalCol
        ? (theme.totalBg || theme.select || theme.background)
        : !isLeafRow && groupBgVal
          ? groupBgVal
          : isLeafRow && theme.leafBg
            ? theme.leafBg
            : isHierarchy
              ? (theme.hierarchyBg || theme.surfaceInset || theme.surfaceBg || theme.background)
              : (theme.surfaceBg || theme.background || '#fff');
    /** @type {any} */
    const matchingValueConfig = !isHierarchy && Array.isArray(valConfigs) && column && column.id
      ? valConfigs.find(config => matchesSelectedColumnToValueConfig(column.id, config)) || null
      : null;
    /** @type {any} */
    const effectiveFormat = column && column.id && Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, column.id)
      ? columnFormatOverrides[column.id]
      : matchingValueConfig && matchingValueConfig.format || defaultValueFormat || null;
    /** @type {any} */
    const effectiveDec = column && column.id && columnDecimalOverrides && columnDecimalOverrides[column.id] !== undefined
      ? columnDecimalOverrides[column.id]
      : decimalPlaces;
    /** @type {Record<string, any>} */
    const exportScaleOpts = {
      valueScale: matchingValueConfig && matchingValueConfig.valueScale !== undefined ? matchingValueConfig.valueScale : valueScale,
      unitSuffix: matchingValueConfig && matchingValueConfig.unitSuffix !== undefined ? matchingValueConfig.unitSuffix : unitSuffix,
    };
    /** @type {any} */
    const displayNum = typeof resolvedValue === 'number'
      ? (typeof resolveNumericDisplayValue === 'function'
        ? resolveNumericDisplayValue(resolvedValue, effectiveFormat, effectiveDec, exportScaleOpts)
        : resolvedValue)
      : null;
    /** @type {any} */
    const isNegativeNum = typeof resolvedValue === 'number'
      && (typeof isDisplayNegative === 'function'
        ? isDisplayNegative(resolvedValue, effectiveFormat, effectiveDec, exportScaleOpts)
        : (typeof displayNum === 'number' && Number.isFinite(displayNum) && displayNum < 0));
    /** @type {any} */
    const isPositiveNum = typeof displayNum === 'number' && Number.isFinite(displayNum) && displayNum > 0;
    /** @type {any} */
    const defaultValueColor = isGrandTotalRow
      ? (theme.totalText || theme.text)
      : !isLeafRow && groupTextVal
        ? groupTextVal
        : (isHierarchy ? theme.text : (theme.textSec || theme.text));
    /** @type {any} */
    const signColor = !isHierarchy && isNegativeNum && theme.negativeText
      ? theme.negativeText
      : (!isHierarchy && isPositiveNum && theme.positiveText ? theme.positiveText : defaultValueColor);
    /** @type {any} */
    const resolvedWeight = cellFmt && cellFmt.bold ? 700
      : reportFormat && reportFormat.bold ? 700
      : isGrandTotalRow ? (theme.totalFontWeight ?? 700)
      : isTotalCol ? 600
      : (!isLeafRow && groupWeightVal !== undefined ? groupWeightVal : (isLeafRow && theme.leafFontWeight !== undefined ? theme.leafFontWeight : 400));
    /** @type {any} */
    const rowBorderColor = theme.rowBorder || theme.border;
    return {
      fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px', height: rowHeight || undefined,
      color: cellFmt && cellFmt.color ? cellFmt.color : signColor,
      background,
      border: cellFmt && cellFmt.borderStyle ? `${Math.max(1, Math.min(5, Number(cellFmt.borderWidth) || 1))}px ${cellFmt.borderStyle} ${cellFmt.borderColor || rowBorderColor}` : reportFormat && reportFormat.borderStyle ? `${Math.max(1, Number(reportFormat.borderWidth) || 1)}px ${reportFormat.borderStyle} ${reportFormat.borderColor || rowBorderColor}` : `1px solid ${rowBorderColor}`,
      padding: isHierarchy ? `4px 10px 4px ${Math.max(10, 10 + depth * (theme.treeIndent || HIERARCHY_INDENT_PX))}px` : '4px 10px', textAlign: cellFmt && ['left', 'center', 'right'].includes(cellFmt.textAlign) ? cellFmt.textAlign : isHierarchy ? 'left' : 'right', whiteSpace: isHierarchy ? 'pre' : 'nowrap', fontVariantNumeric: isHierarchy ? undefined : 'tabular-nums', fontWeight: resolvedWeight, fontStyle: cellFmt && cellFmt.italic ? 'italic' : undefined,
      ...(reportFormat && reportFormat.rowColor ? { background: reportFormat.rowColor } : null), ...condStyle, ...(cellFmt && cellFmt.bg ? { background: cellFmt.bg } : null),
    };
  }, [HIERARCHY_INDENT_PX, cellFormatRules, columnDecimalOverrides, columnFormatOverrides, decimalPlaces, defaultValueFormat, fontFamily, fontSize, getConditionalStyle, getDisplayHeaderTextForColumn, getExportRawCellValue, getExportRowData, getExportRowId, matchesSelectedColumnToValueConfig, resolveDisplayedCellValue, rowHeight, theme, unitSuffix, valConfigs, valueScale]);

  /** @type {(...args: any[]) => any} */
  const buildTableClipboardPayload = useCallback(({
    withHeaders = true, mergedHeaders = false, rows = null, columns = null,
  } = {}) => {
    /** @type {any} */
    const exportColumns = getExportColumns(columns);
    /** @type {any} */
    const exportRows = Array.isArray(rows) ? rows : serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : getDisplayRows();
    if (!Array.isArray(exportRows) || exportRows.length === 0 || exportColumns.length === 0) return null;
    return buildStyledHtmlTableExport({
      table, rows: exportRows, columns: exportColumns, includeHeaders: withHeaders,
      /** @param {any} [column] @returns {any} */
      getHeaderLabel: column => getExportHeaderLabel(column, mergedHeaders, mergedHeaderSeparator), getHeaderStyle: getExportHeaderStyle,
      getHeaderStack: getDisplayHeaderTextStackForColumn,
      /** @param {any} [rowLike] @param {any} [column] @param {any} [rowIndex] @returns {any} */
      getCellValue: (rowLike, column, rowIndex) => getClipboardCellDisplayValue(rowLike, column, rowIndex),
      /** @param {any} [rowLike] @param {any} [column] @param {any} [_value] @param {any} [rowIndex] @returns {any} */
      getCellStyle: (rowLike, column, _value, rowIndex) => getExportCellStyle(rowLike, column, rowIndex),
      tableStyle: { fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px' },
    });
  }, [fontFamily, fontSize, getDisplayRows, getDisplayHeaderTextStackForColumn, getClipboardCellDisplayValue, getExportCellStyle, getExportColumns, getExportHeaderLabel, getExportHeaderStyle, loadedRows, mergedHeaderSeparator, serverSide, table]);

  /** @type {(...args: any[]) => any} */
  const buildAllRowsClipboardPayload = useCallback(({
    withHeaders = true, mergedHeaders = false,
  } = {}) => {
    if (!table || typeof table.getPreExpandedRowModel !== 'function') {
      return buildTableClipboardPayload({ withHeaders, mergedHeaders });
    }
    /** @type {any} */
    const rowModel = table.getPreExpandedRowModel();
    /** @type {any} */
    const rows = rowModel && Array.isArray(rowModel.flatRows) ? rowModel.flatRows : [];
    return buildTableClipboardPayload({ withHeaders, mergedHeaders, rows });
  }, [buildTableClipboardPayload, table]);

  /** @type {(...args: any[]) => any} */
  const buildSelectedClipboardPayload = useCallback((withHeaders = false, selectionMap = selectedCells, { rawValues = false, mergedHeaders = false } = {}) => {
    /** @type {any} */
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;
    /** @type {any} */
    const visibleRows = getDisplayRows();
    /** @type {any} */
    const orderedCols = getDisplayOrderedLeafColumns();
    const rowIdMap = /** @type {Record<string, any>} */ ({}); visibleRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => { rowIdMap[row.id] = index; });
    const colIdMap = /** @type {Record<string, any>} */ ({}); orderedCols.forEach(/** @param {any} [column] @param {any} [index] @returns {any} */ (column, index) => { colIdMap[column.id] = index; });
    /** @returns {any} */
    const unavailable = () => {
      if (typeof showNotification === 'function') showNotification('Selected range unavailable: some rows are not loaded. Load the complete range before copying.', 'warning');
      return null;
    };
    if (serverSide && keys.some(/** @param {any} [key] @returns {any} */ key => {
      /** @type {any} */
      const separator = key.lastIndexOf(':');
      return separator > 0 && colIdMap[key.slice(separator + 1)] !== undefined
        && rowIdMap[key.slice(0, separator)] === undefined;
    })) return unavailable();
    let minR = Infinity; let maxR = -1; let minC = Infinity; let maxC = -1;
    const selectedGrid = /** @type {Record<string, any>} */ ({});
    keys.forEach(/** @param {any} [key] @returns {any} */ key => {
      /** @type {any} */
      const separatorIndex = key.lastIndexOf(':'); if (separatorIndex <= 0) return;
      const rowIndex = rowIdMap[key.slice(0, separatorIndex)]; const colIndex = colIdMap[key.slice(separatorIndex + 1)];
      if (rowIndex === undefined || colIndex === undefined) return;
      minR = Math.min(minR, rowIndex); maxR = Math.max(maxR, rowIndex); minC = Math.min(minC, colIndex); maxC = Math.max(maxC, colIndex);
      if (!selectedGrid[rowIndex]) selectedGrid[rowIndex] = {}; selectedGrid[rowIndex][colIndex] = selectionMap[key];
    });
    if (minR === Infinity) return null;
    /** @type {any} */
    const exportRows = visibleRows.slice(minR, maxR + 1);
    if (serverSide) {
      /** @type {any} */
      const indices = exportRows.map(/** @param {any} [row] @returns {any} */ row => (row.original || row).__virtualIndex).filter(Number.isFinite);
      if (indices.length > 1 && indices.some(/** @param {any} [index] @param {any} [offset] @returns {any} */ (index, offset) => offset > 0 && index !== indices[offset - 1] + 1)) return unavailable();
    }
    /** @type {any} */
    const exportColumns = orderedCols.slice(minC, maxC + 1).filter(/** @param {any} [column] @returns {any} */ column => isExportableColumnId(column.id));
    if (exportRows.length === 0 || exportColumns.length === 0) return null;
    return buildStyledHtmlTableExport({
      rows: exportRows, columns: exportColumns, includeHeaders: withHeaders,
      /** @param {any} [column] @returns {any} */
      getHeaderLabel: column => getExportHeaderLabel(column, false, mergedHeaderSeparator), getHeaderStyle: getExportHeaderStyle,
      getHeaderStack: mergedHeaders ? getDisplayHeaderTextStackForColumn : null,
      /** @param {any} [rowLike] @param {any} [column] @param {any} [rowIndex] @returns {any} */
      getCellValue: (rowLike, column, rowIndex) => {
        /** @type {any} */
        const absoluteRowIndex = minR + rowIndex; const absoluteColIndex = colIdMap[column.id];
        if (!selectedGrid[absoluteRowIndex] || !Object.prototype.hasOwnProperty.call(selectedGrid[absoluteRowIndex], absoluteColIndex)) return '';
        const overrideValue = selectedGrid[absoluteRowIndex][absoluteColIndex];
        return getClipboardCellDisplayValue(rowLike, column, absoluteRowIndex, overrideValue, rawValues);
      },
      /** @param {any} [rowLike] @param {any} [column] @param {any} [_value] @param {any} [rowIndex] @returns {any} */
      getCellStyle: (rowLike, column, _value, rowIndex) => {
        /** @type {any} */
        const absoluteRowIndex = minR + rowIndex; const absoluteColIndex = colIdMap[column.id];
        /** @type {any} */
        const overrideValue = selectedGrid[absoluteRowIndex] && Object.prototype.hasOwnProperty.call(selectedGrid[absoluteRowIndex], absoluteColIndex) ? selectedGrid[absoluteRowIndex][absoluteColIndex] : '';
        return getExportCellStyle(rowLike, column, absoluteRowIndex, overrideValue);
      },
      tableStyle: { fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px' },
    });
  }, [fontFamily, fontSize, getDisplayOrderedLeafColumns, getDisplayRows, getClipboardCellDisplayValue, getExportCellStyle, getExportHeaderLabel, getExportHeaderStyle, getDisplayHeaderTextStackForColumn, isExportableColumnId, mergedHeaderSeparator, selectedCells, serverSide, showNotification]);

  /** @type {(...args: any[]) => any} */
  const buildExportStyleProfile = useCallback((colIds, { withHeaders = true, mergedHeaders = false } = {}) => {
    /** @type {any} */
    const columnsById = new Map();
    getDisplayOrderedLeafColumns().forEach(/** @param {any} [column] @returns {any} */ column => { if (column && column.id) columnsById.set(column.id, column); });
    /** @type {any} */
    const requestedIds = Array.isArray(colIds) && colIds.length > 0 ? colIds.filter(isExportableColumnId) : getInstantiatedExportColIds();
    const headerLabels = /** @type {Record<string, any>} */ ({}); const columnWidths = /** @type {Record<string, any>} */ ({});
    /** @type {any} */
    const exportColumns = requestedIds.map(/** @param {any} [columnId] @returns {any} */ columnId => {
      /** @type {any} */
      const column = columnsById.get(columnId) || (table && typeof table.getColumn === 'function' ? table.getColumn(columnId) : null);
      /** @type {any} */
      const exportField = columnId === 'hierarchy' ? '_id' : columnId;
      /** @type {any} */
      const label = column ? getExportHeaderLabel(column, mergedHeaders, mergedHeaderSeparator) : String(columnId || '');
      headerLabels[columnId] = label; headerLabels[exportField] = label;
      /** @type {any} */
      const width = column && typeof column.getSize === 'function' ? Number(column.getSize()) : null;
      if (Number.isFinite(width) && width > 0) { columnWidths[columnId] = Math.round(width); columnWidths[exportField] = Math.round(width); }
      return column || { id: columnId, __pivotExportFallbackHeader: label };
    });
    /** @type {any} */
    const mergedHeaderRows = mergedHeaders ? buildMergedHeaderRows(exportColumns, {
      /** @param {any} [column] @returns {any} */
      getHeaderStack: column => column && column.__pivotExportFallbackHeader
        ? [column.__pivotExportFallbackHeader]
        : getDisplayHeaderTextStackForColumn(column),
      /** @param {any} [column] @returns {any} */
      getHeaderLabel: column => column && column.__pivotExportFallbackHeader
        ? column.__pivotExportFallbackHeader
        : getDisplayHeaderTextForColumn(column),
    }).rows.map(/** @param {any} p0 @returns {any} */ ({ cells }) => cells.map(/** @param {any} p0 @returns {any} */ ({ label, columnIds, colSpan, rowSpan, styleColumnId }) => ({
      label, columnIds, colSpan, rowSpan, styleColumnId,
    }))) : [];
    return {
      includeHeaders: withHeaders, mergedHeaders, mergedHeaderSeparator, fontFamily: fontFamily || "'Inter', ui-sans-serif, system-ui, sans-serif", fontSize: fontSize || '13px', rowHeight,
      theme: { text: theme.text, textSec: theme.textSec, border: theme.border, background: theme.background, surfaceBg: theme.surfaceBg, surfaceInset: theme.surfaceInset, headerBg: theme.headerBg, headerSubtleBg: theme.headerSubtleBg, headerText: theme.headerText, hierarchyBg: theme.hierarchyBg, totalBg: theme.totalBg, totalBgStrong: theme.totalBgStrong, totalText: theme.totalText, select: theme.select, primary: theme.primary, negativeText: theme.negativeText, positiveText: theme.positiveText, headerGroupBg: theme.headerGroupBg, headerGroupText: theme.headerGroupText, groupBg: theme.groupBg, groupText: theme.groupText, leafBg: theme.leafBg },
      headerLabels, mergedHeaderRows, headerFormatting, columnWidths, decimalPlaces, defaultValueFormat, numberGroupSeparator, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, valConfigs, conditionalFormatting, cellFormatRules, colorScaleMode, colorPalette,
      colorScaleStats: colorScaleStats ? { byCol: colorScaleStats.byCol || {}, table: colorScaleStats.table || null } : null,
    };
  }, [cellFormatRules, colorPalette, colorScaleMode, colorScaleStats, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, conditionalFormatting, decimalPlaces, defaultValueFormat, fontFamily, fontSize, getDisplayHeaderTextForColumn, getDisplayHeaderTextStackForColumn, getDisplayOrderedLeafColumns, getExportHeaderLabel, getInstantiatedExportColIds, headerFormatting, isExportableColumnId, mergedHeaderSeparator, numberGroupSeparator, rowHeight, table, theme, valConfigs]);

  /** @type {(...args: any[]) => any} */
  const copyToClipboard = useCallback(payload => writeClipboardPayload(payload && typeof payload === 'object' && (payload.html !== undefined || payload.text !== undefined) ? payload : { text: payload }), []);
  /** @type {(...args: any[]) => any} */
  const copyTextToClipboard = useCallback((text, successMessage = 'Copied!') => {
    if (!text) return Promise.resolve(false);
    return copyToClipboard(text).then(() => { if (successMessage) showNotification(successMessage, 'success'); return true; }).catch(/** @param {any} [error] @returns {any} */ error => { console.warn('Copy to clipboard failed:', error); showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; });
  }, [copyToClipboard, showNotification]);
  /** @type {(...args: any[]) => any} */
  const copyRichToClipboard = useCallback((payload, successMessage = 'Copied!') => {
    if (!payload || !payload.text && !payload.html) return Promise.resolve(false);
    return copyToClipboard(payload).then(() => { if (successMessage) showNotification(successMessage, 'success'); return true; }).catch(/** @param {any} [error] @returns {any} */ error => {
      console.warn('Rich copy to clipboard failed:', error);
      if (!payload.text) { showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; }
      return copyToClipboard(payload.text).then(() => { showNotification('Copied without formatting. Browser blocked HTML clipboard access.', 'warning'); return true; }).catch(/** @param {any} [fallbackError] @returns {any} */ fallbackError => { console.warn('Plain text clipboard fallback failed:', fallbackError); showNotification('Copy failed. Check browser clipboard permissions.', 'error'); return false; });
    });
  }, [copyToClipboard, showNotification]);
  /** @type {(...args: any[]) => any} */
  const triggerExportDownload = useCallback((url, filename) => {
    /** @type {any} */
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

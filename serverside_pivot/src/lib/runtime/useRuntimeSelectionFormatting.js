import { useCallback, useMemo } from 'react';

import { formatAggLabel, formatDisplayLabel } from '../utils/helpers';
import { normalizeSparklineValConfigsForView } from '../utils/sparklines';
import { getDisplayOrderedLeafColumns, isSelectableDisplayColumn } from './displayColumnOrder';

/** @type {any} */
const NON_VALUE_SELECTION_COLUMNS = new Set(['hierarchy', '__row_number__', '__report_config__']);

/**
 * Owns selection-derived data, number-format commands, and display labels.
 *
 * It exposes explicit selection capabilities to both the interaction layer
 * and the clipboard/export presentation instead of making either domain read
 * the mutable legacy runtime record.
  @param {any} p0 @returns {any} */
export function useRuntimeSelectionFormatting({
  getDisplayRows,
  maxAutoSizeSampleRows,
  normalizedInitialValConfigs,
  selectedCells,
  setColorScaleMode,
  setDataBarsColumns,
  setValConfigs,
  showNotification,
  table,
  valConfigs,
}) {
  /** @type {(...args: any[]) => any} */
  const getSelectedData = useCallback((withHeaders = false, selectionMap = selectedCells) => {
    /** @type {any} */
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;
    /** @type {any} */
    const visibleRows = getDisplayRows();
    /** @type {any} */
    const visibleColumns = getDisplayOrderedLeafColumns(table).filter(isSelectableDisplayColumn);
    /** @type {Record<string, any>} */
    const rowIdMap = {};
    visibleRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => { rowIdMap[row.id] = index; });
    /** @type {Record<string, any>} */
    const colIdMap = {};
    visibleColumns.forEach(/** @param {any} [column] @param {any} [index] @returns {any} */ (column, index) => { colIdMap[column.id] = index; });
    let minRow = Infinity;
    let maxRow = -1;
    let minCol = Infinity;
    let maxCol = -1;
    /** @type {Record<string, any>} */
    const selectedGrid = {};
    keys.forEach(/** @param {any} [key] @returns {any} */ key => {
      /** @type {any} */
      const separatorIndex = key.lastIndexOf(':');
      if (separatorIndex <= 0) return;
      const rowIndex = rowIdMap[key.slice(0, separatorIndex)];
      const colIndex = colIdMap[key.slice(separatorIndex + 1)];
      if (rowIndex === undefined || colIndex === undefined) return;
      minRow = Math.min(minRow, rowIndex);
      maxRow = Math.max(maxRow, rowIndex);
      minCol = Math.min(minCol, colIndex);
      maxCol = Math.max(maxCol, colIndex);
      if (!selectedGrid[rowIndex]) selectedGrid[rowIndex] = {};
      selectedGrid[rowIndex][colIndex] = selectionMap[key];
    });
    if (minRow === Infinity) return null;
    let tsv = '';
    if (withHeaders) {
      /** @type {any[]} */
      const headerRow = [];
      for (let columnIndex = minCol; columnIndex <= maxCol; columnIndex += 1) headerRow.push(visibleColumns[columnIndex].columnDef.header);
      tsv += `${headerRow.join('\t')}\n`;
    }
    for (let rowIndex = minRow; rowIndex <= maxRow; rowIndex += 1) {
      /** @type {any[]} */
      const rowValues = [];
      for (let columnIndex = minCol; columnIndex <= maxCol; columnIndex += 1) {
        /** @type {any} */
        const value = selectedGrid[rowIndex] && selectedGrid[rowIndex][columnIndex];
        rowValues.push(value !== undefined && value !== null ? String(value) : '');
      }
      tsv += `${rowValues.join('\t')}\n`;
    }
    return tsv;
  }, [getDisplayRows, selectedCells, table]);

  /** @type {(...args: any[]) => any} */
  const hasSelectionKey = useCallback((selectionMap, selectionKey) => (
    !!selectionMap && !!selectionKey && Object.prototype.hasOwnProperty.call(selectionMap, selectionKey)
  ), []);

  /** @type {(...args: any[]) => any} */
  const getSelectedColumnIds = useCallback((selectionMap = selectedCells) => {
    /** @type {any} */
    const colIds = new Set();
    Object.keys(selectionMap || {}).forEach(selectionKey => {
      /** @type {any} */
      const separatorIndex = selectionKey.lastIndexOf(':');
      if (separatorIndex <= 0) return;
      /** @type {any} */
      const colId = selectionKey.slice(separatorIndex + 1);
      if (colId) colIds.add(colId);
    });
    return Array.from(colIds);
  }, [selectedCells]);

  /** @type {(...args: any[]) => any} */
  const matchesSelectedColumnToValueConfig = useCallback((colId, config) => {
    if (!config || typeof colId !== 'string') return false;
    /** @type {any} */
    const normalizedColId = colId.toLowerCase();
    if (config.agg === 'formula') {
      /** @type {any} */
      const candidates = [
        config.alias,
        config.id,
        config.field,
        config.field ? `${config.field}_formula` : null,
      ].filter(Boolean).map(entry => String(entry).toLowerCase());
      return candidates.some(/** @param {any} [candidate] @returns {any} */ candidate => normalizedColId === candidate || normalizedColId.endsWith(`_${candidate}`));
    }
    if (!config.field) return false;
    /** @type {any} */
    const idCandidates = [config.alias, config.id, `${config.field}_${config.agg}`].filter(Boolean).map(v => String(v).toLowerCase());
    /** @type {any} */
    const suffix = `_${config.field}_${config.agg}`.toLowerCase();
    return idCandidates.includes(normalizedColId)
      || normalizedColId === String(config.field).toLowerCase()
      || normalizedColId.endsWith(suffix)
      || normalizedColId.includes(`_${String(config.field).toLowerCase()}_`);
  }, []);

  /** @type {(...args: any[]) => any} */
  const getSelectedMeasureIndexes = useCallback(selectedColumnIds => {
    /** @type {any[]} */
    const matchedIndexes = [];
    valConfigs.forEach(/** @param {any} [config] @param {any} [index] @returns {any} */ (config, index) => {
      if (selectedColumnIds.some(/** @param {any} [colId] @returns {any} */ colId => matchesSelectedColumnToValueConfig(colId, config))) matchedIndexes.push(index);
    });
    return matchedIndexes;
  }, [matchesSelectedColumnToValueConfig, valConfigs]);

  /** @type {(...args: any[]) => any} */
  const getValueSelectionColumnIds = useCallback(selectionMap => (
    getSelectedColumnIds(selectionMap).filter(/** @param {any} [columnId] @returns {any} */ columnId => !NON_VALUE_SELECTION_COLUMNS.has(columnId))
  ), [getSelectedColumnIds]);

  /** @type {(...args: any[]) => any} */
  const getDefaultFormatForSelection = useCallback(selectionMap => {
    /** @type {any} */
    const matchedIndexes = getSelectedMeasureIndexes(getValueSelectionColumnIds(selectionMap));
    if (matchedIndexes.length === 0) return 'fixed:2';
    /** @type {any} */
    const formats = matchedIndexes.map(/** @param {any} [index] @returns {any} */ index => valConfigs[index] && valConfigs[index].format)
      .filter(/** @param {any} [format] @returns {any} */ format => typeof format === 'string' && format.trim() !== '')
      .map(/** @param {any} [format] @returns {any} */ format => format.trim());
    return new Set(formats).size === 1 ? formats[0] : 'fixed:2';
  }, [getSelectedMeasureIndexes, getValueSelectionColumnIds, valConfigs]);

  /** @type {(...args: any[]) => any} */
  const getCurrentFormatForSelection = useCallback(selectionMap => {
    /** @type {any} */
    const matchedIndexes = getSelectedMeasureIndexes(getValueSelectionColumnIds(selectionMap));
    if (matchedIndexes.length === 0) return '';
    /** @type {any} */
    const formats = matchedIndexes.map(/** @param {any} [index] @returns {any} */ index => valConfigs[index] && typeof valConfigs[index].format === 'string' ? valConfigs[index].format.trim() : '')
      .filter(/** @param {any} [format] @param {any} [index] @param {any} [values] @returns {any} */ (format, index, values) => values.indexOf(format || '') === index);
    return formats.length === 1 ? formats[0] : '';
  }, [getSelectedMeasureIndexes, getValueSelectionColumnIds, valConfigs]);

  /** @type {(...args: any[]) => any} */
  const applyDataBarsFromSelection = useCallback((selectionMap, mode = 'col') => {
    /** @type {any} */
    const selectedColumnIds = getValueSelectionColumnIds(selectionMap);
    if (selectedColumnIds.length === 0) {
      showNotification('Select at least one value cell first.', 'warning');
      return;
    }
    if (mode === 'off') {
      setDataBarsColumns(new Set());
      setColorScaleMode('off');
      showNotification('Data bars disabled.', 'info');
      return;
    }
    setColorScaleMode(mode, selectedColumnIds);
    setDataBarsColumns(/** @param {any} [previous] @returns {any} */ previous => {
      /** @type {any} */
      const next = new Set(previous || []);
      selectedColumnIds.forEach(/** @param {any} [columnId] @returns {any} */ columnId => next.add(columnId));
      return next;
    });
    showNotification(
      mode === 'row' ? 'Data bars enabled (row mode).' : mode === 'table' ? 'Data bars enabled (table mode).' : 'Data bars enabled (column mode).',
      'success',
    );
  }, [getValueSelectionColumnIds, setColorScaleMode, setDataBarsColumns, showNotification]);

  /** @type {(...args: any[]) => any} */
  const applyFormatToSelection = useCallback((selectionMap, formatOverride = null) => {
    /** @type {any} */
    const selectedColumnIds = getValueSelectionColumnIds(selectionMap);
    if (selectedColumnIds.length === 0) {
      showNotification('Select at least one value cell first.', 'warning');
      return;
    }
    let normalizedFormat = '';
    if (formatOverride !== null && formatOverride !== undefined) {
      normalizedFormat = String(formatOverride).trim();
    } else {
      /** @type {any} */
      const formatInput = window.prompt('Format selected values.\nExamples: fixed:2, currency:$, accounting:€, percent, compact', getDefaultFormatForSelection(selectionMap));
      if (formatInput === null) return;
      normalizedFormat = String(formatInput).trim();
    }
    setValConfigs(/** @param {any} [previous] @returns {any} */ previous => {
      let matchedAny = false;
      /** @type {any[]} */
      const matchedIndexes = [];
      previous.forEach(/** @param {any} [config] @param {any} [index] @returns {any} */ (config, index) => {
        if (selectedColumnIds.some(/** @param {any} [colId] @returns {any} */ colId => matchesSelectedColumnToValueConfig(colId, config))) matchedIndexes.push(index);
      });
      /** @type {any} */
      const next = previous.map(/** @param {any} [config] @param {any} [index] @returns {any} */ (config, index) => {
        if (!matchedIndexes.includes(index)) return config;
        matchedAny = true;
        if (!normalizedFormat) {
          const { format, ...remainingConfig } = config;
          return remainingConfig;
        }
        return { ...config, format: normalizedFormat };
      });
      if (!matchedAny) {
        showNotification('No value columns matched the current selection.', 'warning');
        return previous;
      }
      return normalizeSparklineValConfigsForView(next, normalizedInitialValConfigs);
    });
    showNotification(normalizedFormat ? `Format applied: ${normalizedFormat}` : 'Format cleared for selected values.', normalizedFormat ? 'success' : 'info');
  }, [getDefaultFormatForSelection, getValueSelectionColumnIds, matchesSelectedColumnToValueConfig, normalizedInitialValConfigs, setValConfigs, showNotification]);

  /** @type {any} */
  const selectedValueColumnIds = useMemo(() => getValueSelectionColumnIds(selectedCells), [getValueSelectionColumnIds, selectedCells]);
  /** @type {any} */
  const canApplySelectionValueFormat = useMemo(() => getSelectedMeasureIndexes(selectedValueColumnIds).length > 0, [getSelectedMeasureIndexes, selectedValueColumnIds]);
  /** @type {any} */
  const selectionValueFormat = useMemo(() => getCurrentFormatForSelection(selectedCells), [getCurrentFormatForSelection, selectedCells]);
  /** @type {(...args: any[]) => any} */
  const applySelectionValueFormat = useCallback(formatOverride => applyFormatToSelection(selectedCells, formatOverride), [applyFormatToSelection, selectedCells]);

  /** @type {(...args: any[]) => any} */
  const getDisplayHeaderTextForColumn = useCallback(column => {
    if (!column) return '';
    /** @type {any} */
    const columnId = typeof column.id === 'string' ? column.id : '';
    /** @type {any} */
    const isGroupColumn = Array.isArray(column.columns) && column.columns.length > 0;
    /** @type {any} */
    const matchingValueConfig = !isGroupColumn && Array.isArray(valConfigs)
      ? valConfigs.find(config => matchesSelectedColumnToValueConfig(columnId, config)) || null
      : null;
    if (matchingValueConfig) {
      /** @type {any} */
      const totalPrefix = columnId.startsWith('__RowTotal__') ? 'Total ' : '';
      if (matchingValueConfig.label) return `${totalPrefix}${String(matchingValueConfig.label)}`;
      /** @type {any} */
      const baseText = matchingValueConfig.agg === 'formula'
        ? (matchingValueConfig.field || columnId)
        : `${formatDisplayLabel(matchingValueConfig.field)} (${formatAggLabel(matchingValueConfig.agg, matchingValueConfig.weightField)})`;
      return `${totalPrefix}${baseText}`;
    }
    /** @type {any} */
    const columnDef = column.columnDef || {};
    if (typeof columnDef.header === 'string' && columnDef.header.trim()) return columnDef.header;
    if (columnDef.headerVal !== undefined && columnDef.headerVal !== null) return String(columnDef.headerVal);
    return columnId ? formatDisplayLabel(columnId.replace(/^group_/, '').replace(/\|\|\|/g, ' > ')) : '';
  }, [matchesSelectedColumnToValueConfig, valConfigs]);

  /** @type {(...args: any[]) => any} */
  const getDisplayHeaderTextStackForColumn = useCallback(column => {
    if (!column) return [];
    /** @type {any[]} */
    const parts = [];
    /** @type {any} */
    const visited = new Set();
    let current = column;
    while (current && !visited.has(current.id)) {
      visited.add(current.id);
      /** @type {any} */
      const text = getDisplayHeaderTextForColumn(current);
      if (text) parts.unshift(text);
      current = current.parent || null;
    }
    return parts;
  }, [getDisplayHeaderTextForColumn]);

  /** @type {(...args: any[]) => any} */
  const getAutoSizeSampleRows = useCallback(rows => {
    if (!Array.isArray(rows) || rows.length === 0) return [];
    if (rows.length <= maxAutoSizeSampleRows) return rows;
    /** @type {any} */
    const lastIndex = rows.length - 1;
    /** @type {any} */
    const sampledIndices = new Set();
    for (let index = 0; index < maxAutoSizeSampleRows; index += 1) sampledIndices.add(Math.round(index * lastIndex / (maxAutoSizeSampleRows - 1)));
    return Array.from(sampledIndices).sort((left, right) => left - right).map(index => rows[index]).filter(Boolean);
  }, [maxAutoSizeSampleRows]);

  return {
    applyDataBarsFromSelection,
    applyFormatToSelection,
    applySelectionValueFormat,
    canApplySelectionValueFormat,
    getAutoSizeSampleRows,
    getCurrentFormatForSelection,
    getDefaultFormatForSelection,
    getDisplayHeaderTextForColumn,
    getDisplayHeaderTextStackForColumn,
    getSelectedColumnIds,
    getSelectedData,
    getSelectedMeasureIndexes,
    hasSelectionKey,
    matchesSelectedColumnToValueConfig,
    selectedValueColumnIds,
    selectionValueFormat,
  };
}

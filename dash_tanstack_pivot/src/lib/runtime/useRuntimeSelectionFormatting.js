import { useCallback, useMemo } from 'react';

import { formatAggLabel, formatDisplayLabel } from '../utils/helpers';
import { normalizeSparklineValConfigsForView } from '../utils/sparklines';
import { getDisplayOrderedLeafColumns, isSelectableDisplayColumn } from './displayColumnOrder';

const NON_VALUE_SELECTION_COLUMNS = new Set(['hierarchy', '__row_number__', '__report_config__']);

/**
 * Owns selection-derived data, number-format commands, and display labels.
 *
 * It exposes explicit selection capabilities to both the interaction layer
 * and the clipboard/export presentation instead of making either domain read
 * the mutable legacy runtime record.
 */
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
  const getSelectedData = useCallback((withHeaders = false, selectionMap = selectedCells) => {
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;
    const visibleRows = getDisplayRows();
    const visibleColumns = getDisplayOrderedLeafColumns(table).filter(isSelectableDisplayColumn);
    const rowIdMap = {};
    visibleRows.forEach((row, index) => { rowIdMap[row.id] = index; });
    const colIdMap = {};
    visibleColumns.forEach((column, index) => { colIdMap[column.id] = index; });
    let minRow = Infinity;
    let maxRow = -1;
    let minCol = Infinity;
    let maxCol = -1;
    const selectedGrid = {};
    keys.forEach(key => {
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
      const headerRow = [];
      for (let columnIndex = minCol; columnIndex <= maxCol; columnIndex += 1) headerRow.push(visibleColumns[columnIndex].columnDef.header);
      tsv += `${headerRow.join('\t')}\n`;
    }
    for (let rowIndex = minRow; rowIndex <= maxRow; rowIndex += 1) {
      const rowValues = [];
      for (let columnIndex = minCol; columnIndex <= maxCol; columnIndex += 1) {
        const value = selectedGrid[rowIndex] && selectedGrid[rowIndex][columnIndex];
        rowValues.push(value !== undefined && value !== null ? String(value) : '');
      }
      tsv += `${rowValues.join('\t')}\n`;
    }
    return tsv;
  }, [getDisplayRows, selectedCells, table]);

  const hasSelectionKey = useCallback((selectionMap, selectionKey) => (
    !!selectionMap && !!selectionKey && Object.prototype.hasOwnProperty.call(selectionMap, selectionKey)
  ), []);

  const getSelectedColumnIds = useCallback((selectionMap = selectedCells) => {
    const colIds = new Set();
    Object.keys(selectionMap || {}).forEach(selectionKey => {
      const separatorIndex = selectionKey.lastIndexOf(':');
      if (separatorIndex <= 0) return;
      const colId = selectionKey.slice(separatorIndex + 1);
      if (colId) colIds.add(colId);
    });
    return Array.from(colIds);
  }, [selectedCells]);

  const matchesSelectedColumnToValueConfig = useCallback((colId, config) => {
    if (!config || typeof colId !== 'string' || !config.field) return false;
    const normalizedColId = colId.toLowerCase();
    if (config.agg === 'formula') {
      const formulaId = String(config.field).toLowerCase();
      return normalizedColId === formulaId || normalizedColId.endsWith(`_${formulaId}`);
    }
    const suffix = `_${config.field}_${config.agg}`.toLowerCase();
    return normalizedColId === String(config.field).toLowerCase() || normalizedColId.endsWith(suffix) || normalizedColId.includes(`_${String(config.field).toLowerCase()}_`);
  }, []);

  const getSelectedMeasureIndexes = useCallback(selectedColumnIds => {
    const matchedIndexes = [];
    valConfigs.forEach((config, index) => {
      if (selectedColumnIds.some(colId => matchesSelectedColumnToValueConfig(colId, config))) matchedIndexes.push(index);
    });
    return matchedIndexes;
  }, [matchesSelectedColumnToValueConfig, valConfigs]);

  const getValueSelectionColumnIds = useCallback(selectionMap => (
    getSelectedColumnIds(selectionMap).filter(columnId => !NON_VALUE_SELECTION_COLUMNS.has(columnId))
  ), [getSelectedColumnIds]);

  const getDefaultFormatForSelection = useCallback(selectionMap => {
    const matchedIndexes = getSelectedMeasureIndexes(getValueSelectionColumnIds(selectionMap));
    if (matchedIndexes.length === 0) return 'fixed:2';
    const formats = matchedIndexes.map(index => valConfigs[index] && valConfigs[index].format)
      .filter(format => typeof format === 'string' && format.trim() !== '')
      .map(format => format.trim());
    return new Set(formats).size === 1 ? formats[0] : 'fixed:2';
  }, [getSelectedMeasureIndexes, getValueSelectionColumnIds, valConfigs]);

  const getCurrentFormatForSelection = useCallback(selectionMap => {
    const matchedIndexes = getSelectedMeasureIndexes(getValueSelectionColumnIds(selectionMap));
    if (matchedIndexes.length === 0) return '';
    const formats = matchedIndexes.map(index => valConfigs[index] && typeof valConfigs[index].format === 'string' ? valConfigs[index].format.trim() : '')
      .filter((format, index, values) => values.indexOf(format || '') === index);
    return formats.length === 1 ? formats[0] : '';
  }, [getSelectedMeasureIndexes, getValueSelectionColumnIds, valConfigs]);

  const applyDataBarsFromSelection = useCallback((selectionMap, mode = 'col') => {
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
    setDataBarsColumns(previous => {
      const next = new Set(previous || []);
      selectedColumnIds.forEach(columnId => next.add(columnId));
      return next;
    });
    showNotification(
      mode === 'row' ? 'Data bars enabled (row mode).' : mode === 'table' ? 'Data bars enabled (table mode).' : 'Data bars enabled (column mode).',
      'success',
    );
  }, [getValueSelectionColumnIds, setColorScaleMode, setDataBarsColumns, showNotification]);

  const applyFormatToSelection = useCallback((selectionMap, formatOverride = null) => {
    const selectedColumnIds = getValueSelectionColumnIds(selectionMap);
    if (selectedColumnIds.length === 0) {
      showNotification('Select at least one value cell first.', 'warning');
      return;
    }
    let normalizedFormat = '';
    if (formatOverride !== null && formatOverride !== undefined) {
      normalizedFormat = String(formatOverride).trim();
    } else {
      const formatInput = window.prompt('Format selected values.\nExamples: fixed:2, currency:$, accounting:€, percent, compact', getDefaultFormatForSelection(selectionMap));
      if (formatInput === null) return;
      normalizedFormat = String(formatInput).trim();
    }
    setValConfigs(previous => {
      let matchedAny = false;
      const matchedIndexes = [];
      previous.forEach((config, index) => {
        if (selectedColumnIds.some(colId => matchesSelectedColumnToValueConfig(colId, config))) matchedIndexes.push(index);
      });
      const next = previous.map((config, index) => {
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

  const selectedValueColumnIds = useMemo(() => getValueSelectionColumnIds(selectedCells), [getValueSelectionColumnIds, selectedCells]);
  const canApplySelectionValueFormat = useMemo(() => getSelectedMeasureIndexes(selectedValueColumnIds).length > 0, [getSelectedMeasureIndexes, selectedValueColumnIds]);
  const selectionValueFormat = useMemo(() => getCurrentFormatForSelection(selectedCells), [getCurrentFormatForSelection, selectedCells]);
  const applySelectionValueFormat = useCallback(formatOverride => applyFormatToSelection(selectedCells, formatOverride), [applyFormatToSelection, selectedCells]);

  const getDisplayHeaderTextForColumn = useCallback(column => {
    if (!column) return '';
    const columnId = typeof column.id === 'string' ? column.id : '';
    const matchingValueConfig = Array.isArray(valConfigs) ? valConfigs.find(config => matchesSelectedColumnToValueConfig(columnId, config)) || null : null;
    if (matchingValueConfig) {
      return matchingValueConfig.agg === 'formula' ? matchingValueConfig.label || matchingValueConfig.field || columnId : `${formatDisplayLabel(matchingValueConfig.field)} (${formatAggLabel(matchingValueConfig.agg, matchingValueConfig.weightField)})`;
    }
    const columnDef = column.columnDef || {};
    if (typeof columnDef.header === 'string' && columnDef.header.trim()) return columnDef.header;
    if (columnDef.headerVal !== undefined && columnDef.headerVal !== null) return String(columnDef.headerVal);
    return columnId ? formatDisplayLabel(columnId.replace(/^group_/, '').replace(/\|\|\|/g, ' > ')) : '';
  }, [matchesSelectedColumnToValueConfig, valConfigs]);

  const getDisplayHeaderTextStackForColumn = useCallback(column => {
    if (!column) return [];
    const parts = [];
    const visited = new Set();
    let current = column;
    while (current && !visited.has(current.id)) {
      visited.add(current.id);
      const text = getDisplayHeaderTextForColumn(current);
      if (text) parts.unshift(text);
      current = current.parent || null;
    }
    return parts;
  }, [getDisplayHeaderTextForColumn]);

  const getAutoSizeSampleRows = useCallback(rows => {
    if (!Array.isArray(rows) || rows.length === 0) return [];
    if (rows.length <= maxAutoSizeSampleRows) return rows;
    const lastIndex = rows.length - 1;
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

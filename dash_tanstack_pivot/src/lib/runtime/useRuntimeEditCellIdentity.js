import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID } from '../hooks/usePivotNormalization';

/**
 * Resolves stable edit-overlay cell identities and values. It is intentionally
 * independent of transaction dispatch so the editing state owner can be
 * migrated without pulling transport into setup.
 */
export function useRuntimeEditCellIdentity({
  comparisonCellOriginalValuesRef,
  editValueDisplayMode,
  optimisticCellValuesRef,
  tableDataRef,
}) {
  const getOptimisticCellKey = useCallback((rowId, colId) => {
    const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
    const normalizedColId = colId === null || colId === undefined ? '' : String(colId);
    if (!normalizedRowId || !normalizedColId) return null;
    return `${normalizedRowId}:::${normalizedColId}`;
  }, []);

  const splitOptimisticCellKey = useCallback(key => {
    if (typeof key !== 'string' || !key) return null;
    const separatorIndex = key.lastIndexOf(':::');
    if (separatorIndex <= 0) return null;
    return { rowId: key.slice(0, separatorIndex), colId: key.slice(separatorIndex + 3) };
  }, []);

  const normalizeTrackedEventIds = useCallback(eventIds => (
    Array.isArray(eventIds)
      ? Array.from(new Set(eventIds.map(value => String(value || '').trim()).filter(Boolean)))
      : []
  ), []);

  const resolveOptimisticCellValue = useCallback((rowId, colId, fallbackValue) => {
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return fallbackValue;
    const optimisticEntry = optimisticCellValuesRef.current.get(key);
    return optimisticEntry ? optimisticEntry.value : fallbackValue;
  }, [getOptimisticCellKey, optimisticCellValuesRef]);

  const resolveVisibleDataRowId = useCallback(row => {
    if (!row || typeof row !== 'object') return null;
    if (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total') return GRAND_TOTAL_ROW_ID;
    if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
    if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
    if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
    return null;
  }, []);

  const resolveCurrentCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
    if (rowId === null || rowId === undefined || colId === null || colId === undefined) return fallbackValue;
    const normalizedRowId = String(rowId);
    const normalizedColId = String(colId);
    const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
    const matchedRow = visibleRows.find(row => resolveVisibleDataRowId(row) === normalizedRowId) || null;
    const resolvedFallback = matchedRow && Object.prototype.hasOwnProperty.call(matchedRow, normalizedColId)
      ? matchedRow[normalizedColId]
      : fallbackValue;
    return resolveOptimisticCellValue(normalizedRowId, normalizedColId, resolvedFallback);
  }, [resolveOptimisticCellValue, resolveVisibleDataRowId, tableDataRef]);

  const resolveDisplayedCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
    const currentValue = resolveCurrentCellValue(rowId, colId, fallbackValue);
    if (editValueDisplayMode !== 'original') return currentValue;
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return currentValue;
    const originalEntry = comparisonCellOriginalValuesRef.current.get(key);
    return originalEntry ? originalEntry.value : currentValue;
  }, [comparisonCellOriginalValuesRef, editValueDisplayMode, getOptimisticCellKey, resolveCurrentCellValue]);

  return {
    getOptimisticCellKey,
    normalizeTrackedEventIds,
    resolveCurrentCellValue,
    resolveDisplayedCellValue,
    resolveOptimisticCellValue,
    resolveVisibleDataRowId,
    splitOptimisticCellKey,
  };
}

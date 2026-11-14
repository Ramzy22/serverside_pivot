import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID, isGrandTotalRow } from '../hooks/usePivotNormalization';

/**
 * Resolves stable edit-overlay cell identities and values. It is intentionally
 * independent of transaction dispatch so the editing state owner can be
 * migrated without pulling transport into setup.
  @param {any} p0 @returns {any} */
export function useRuntimeEditCellIdentity({
  comparisonCellOriginalValuesRef,
  editValueDisplayMode,
  optimisticCellValuesRef,
  tableDataRef,
}) {
  /** @type {(...args: any[]) => any} */
  const getOptimisticCellKey = useCallback((rowId, colId) => {
    /** @type {any} */
    const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
    /** @type {any} */
    const normalizedColId = colId === null || colId === undefined ? '' : String(colId);
    if (!normalizedRowId || !normalizedColId) return null;
    return `${normalizedRowId}:::${normalizedColId}`;
  }, []);

  /** @type {(...args: any[]) => any} */
  const splitOptimisticCellKey = useCallback(key => {
    if (typeof key !== 'string' || !key) return null;
    /** @type {any} */
    const separatorIndex = key.lastIndexOf(':::');
    if (separatorIndex <= 0) return null;
    return { rowId: key.slice(0, separatorIndex), colId: key.slice(separatorIndex + 3) };
  }, []);

  /** @type {(...args: any[]) => any} */
  const normalizeTrackedEventIds = useCallback(eventIds => (
    Array.isArray(eventIds)
      ? Array.from(new Set(eventIds.map(value => String(value || '').trim()).filter(Boolean)))
      : []
  ), []);

  /** @type {(...args: any[]) => any} */
  const resolveOptimisticCellValue = useCallback((rowId, colId, fallbackValue) => {
    /** @type {any} */
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return fallbackValue;
    /** @type {any} */
    const optimisticEntry = optimisticCellValuesRef.current.get(key);
    return optimisticEntry ? optimisticEntry.value : fallbackValue;
  }, [getOptimisticCellKey, optimisticCellValuesRef]);

  /** @type {(...args: any[]) => any} */
  const resolveVisibleDataRowId = useCallback(row => {
    if (!row || typeof row !== 'object') return null;
    if (isGrandTotalRow(row)) return GRAND_TOTAL_ROW_ID;
    if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
    if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
    if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
    return null;
  }, []);

  /** @type {(...args: any[]) => any} */
  const resolveCurrentCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
    if (rowId === null || rowId === undefined || colId === null || colId === undefined) return fallbackValue;
    /** @type {any} */
    const normalizedRowId = String(rowId);
    /** @type {any} */
    const normalizedColId = String(colId);
    /** @type {any} */
    const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
    /** @type {any} */
    const matchedRow = visibleRows.find(/** @param {any} [row] @returns {any} */ row => resolveVisibleDataRowId(row) === normalizedRowId) || null;
    /** @type {any} */
    const resolvedFallback = matchedRow && Object.prototype.hasOwnProperty.call(matchedRow, normalizedColId)
      ? matchedRow[normalizedColId]
      : fallbackValue;
    return resolveOptimisticCellValue(normalizedRowId, normalizedColId, resolvedFallback);
  }, [resolveOptimisticCellValue, resolveVisibleDataRowId, tableDataRef]);

  /** @type {(...args: any[]) => any} */
  const resolveDisplayedCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
    /** @type {any} */
    const currentValue = resolveCurrentCellValue(rowId, colId, fallbackValue);
    if (editValueDisplayMode !== 'original') return currentValue;
    /** @type {any} */
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return currentValue;
    /** @type {any} */
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

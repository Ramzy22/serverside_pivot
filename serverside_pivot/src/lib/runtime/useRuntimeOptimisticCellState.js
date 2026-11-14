import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID } from '../hooks/usePivotNormalization';

/** Owns optimistic values, request cleanup, and edit-display reset commands.  @param {any} p0 @returns {any} */
export function useRuntimeOptimisticCellState({
  bumpComparisonCellEpoch,
  bumpEditedCellEpoch,
  bumpOptimisticCellEpoch,
  comparisonCellCountsRef,
  comparisonCellOriginalValuesRef,
  editedCellMarkersRef,
  getDirectVisibleChildRowIds,
  getOptimisticCellKey,
  optimisticCellRequestsRef,
  optimisticCellValuesRef,
  resolveAggregationConfigForColumnId,
  resolveCurrentCellValue,
  syncComparisonValueState,
}) {
  /** @type {(...args: any[]) => any} */
  const clearAllOptimisticCellValues = useCallback(() => {
    /** @type {any} */
    const hadEntries = optimisticCellValuesRef.current.size > 0 || optimisticCellRequestsRef.current.size > 0;
    optimisticCellValuesRef.current.clear();
    optimisticCellRequestsRef.current.clear();
    if (hadEntries) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, optimisticCellRequestsRef, optimisticCellValuesRef]);

  /** @type {(...args: any[]) => any} */
  const captureOptimisticCellValues = useCallback((updates, requestId) => {
    /** @type {any} */
    const requestKeys = new Set();
    /** @param {any} [rowId] @param {any} [colId] @param {any} [value] @returns {any} */
    const storeOptimisticValue = (rowId, colId, value) => {
      /** @type {any} */
      const key = getOptimisticCellKey(rowId, colId);
      if (!key) return;
      optimisticCellValuesRef.current.set(key, {
        requestId,
        rowId: String(rowId),
        colId: String(colId),
        value
      });
      requestKeys.add(key);
    };
    (Array.isArray(updates) ? updates : []).forEach(update => {
      if (!update || typeof update !== 'object') return;
      /** @type {any} */
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      storeOptimisticValue(rowId, colId, update.value);
      /** @type {any} */
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      /** @type {any} */
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      /** @type {any} */
      const normalizedRowPath = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowPath || normalizedRowPath === GRAND_TOTAL_ROW_ID) return;
      /** @type {any} */
      const pathParts = normalizedRowPath.split('|||');
      /** @type {any[]} */
      const ancestorPaths = [];
      for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
        ancestorPaths.push(pathParts.slice(0, depth).join('|||'));
      }
      ancestorPaths.push(GRAND_TOTAL_ROW_ID);
      if (ancestorPaths.length === 0) return;
      /** @type {any} */
      const nextValue = Number(update.value);
      /** @type {any} */
      const previousValue = Number(update.oldValue);
      if (aggregationFn === 'sum') {
        if (!Number.isFinite(nextValue) || !Number.isFinite(previousValue)) return;
        /** @type {any} */
        const delta = nextValue - previousValue;
        if (!Number.isFinite(delta) || delta === 0) return;
        ancestorPaths.forEach(ancestorPath => {
          /** @type {any} */
          const ancestorValue = Number(resolveCurrentCellValue(ancestorPath, colId));
          if (!Number.isFinite(ancestorValue)) return;
          storeOptimisticValue(ancestorPath, colId, ancestorValue + delta);
        });
        return;
      }
      if (aggregationFn === 'min' || aggregationFn === 'max') {
        if (!Number.isFinite(nextValue)) return;
        /** @type {any} */
        const reducer = aggregationFn === 'min' ? Math.min : Math.max;
        ancestorPaths.forEach(ancestorPath => {
          /** @type {any} */
          const childRowIds = getDirectVisibleChildRowIds(ancestorPath);
          if (childRowIds.length === 0) return;
          /** @type {any} */
          const childValues = childRowIds.map(/** @param {any} [childRowId] @returns {any} */ childRowId => {
            if (childRowId === normalizedRowPath) return nextValue;
            return Number(resolveCurrentCellValue(childRowId, colId));
          }).filter(/** @param {any} [value] @returns {any} */ value => Number.isFinite(value));
          if (childValues.length === 0) return;
          storeOptimisticValue(ancestorPath, colId, reducer(...childValues));
        });
      }
    });
    if (!requestId || requestKeys.size === 0) return;
    optimisticCellRequestsRef.current.set(requestId, Array.from(requestKeys));
    bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, getDirectVisibleChildRowIds, getOptimisticCellKey, optimisticCellRequestsRef, optimisticCellValuesRef, resolveAggregationConfigForColumnId, resolveCurrentCellValue]);

  /** @type {(...args: any[]) => any} */
  const releaseOptimisticCellRequest = useCallback(requestId => {
    if (!requestId) return;
    optimisticCellRequestsRef.current.delete(requestId);
  }, [optimisticCellRequestsRef]);

  /** @type {(...args: any[]) => any} */
  const clearOptimisticCellValuesForRequest = useCallback(requestId => {
    if (!requestId) return;
    /** @type {any} */
    const requestKeys = optimisticCellRequestsRef.current.get(requestId);
    if (!Array.isArray(requestKeys) || requestKeys.length === 0) {
      optimisticCellRequestsRef.current.delete(requestId);
      return;
    }
    let didDelete = false;
    requestKeys.forEach(key => {
      /** @type {any} */
      const optimisticEntry = optimisticCellValuesRef.current.get(key);
      if (!optimisticEntry || optimisticEntry.requestId !== requestId) return;
      optimisticCellValuesRef.current.delete(key);
      didDelete = true;
    });
    optimisticCellRequestsRef.current.delete(requestId);
    if (didDelete) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, optimisticCellRequestsRef, optimisticCellValuesRef]);

  /** @type {(...args: any[]) => any} */
  const resolveEditedCellMarker = useCallback((rowId, colId) => {
    /** @type {any} */
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return null;
    /** @type {any} */
    const marker = editedCellMarkersRef.current.get(key);
    if (!marker || (marker.directCount || 0) <= 0 && (marker.propagatedCount || 0) <= 0) {
      return null;
    }
    return {
      direct: (marker.directCount || 0) > 0,
      propagated: (marker.propagatedCount || 0) > 0
    };
  }, [editedCellMarkersRef, getOptimisticCellKey]);

  /** @type {(...args: any[]) => any} */
  const clearAllEditedCellMarkers = useCallback(() => {
    if (editedCellMarkersRef.current.size === 0) return;
    editedCellMarkersRef.current.clear();
    bumpEditedCellEpoch();
  }, [bumpEditedCellEpoch, editedCellMarkersRef]);

  /** @type {(...args: any[]) => any} */
  const clearAllComparisonValueState = useCallback(() => {
    /** @type {any} */
    const hadEntries = comparisonCellCountsRef.current.size > 0 || comparisonCellOriginalValuesRef.current.size > 0;
    comparisonCellCountsRef.current.clear();
    comparisonCellOriginalValuesRef.current.clear();
    if (hadEntries) {
      syncComparisonValueState();
      bumpComparisonCellEpoch();
    }
  }, [bumpComparisonCellEpoch, comparisonCellCountsRef, comparisonCellOriginalValuesRef, syncComparisonValueState]);

  return {
    captureOptimisticCellValues,
    clearAllComparisonValueState,
    clearAllEditedCellMarkers,
    clearAllOptimisticCellValues,
    clearOptimisticCellValuesForRequest,
    releaseOptimisticCellRequest,
    resolveEditedCellMarker,
  };
}

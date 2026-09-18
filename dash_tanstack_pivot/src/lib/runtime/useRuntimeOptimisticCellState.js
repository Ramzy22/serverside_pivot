import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID } from '../hooks/usePivotNormalization';

/** Owns optimistic values, request cleanup, and edit-display reset commands. */
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
  const clearAllOptimisticCellValues = useCallback(() => {
    const hadEntries = optimisticCellValuesRef.current.size > 0 || optimisticCellRequestsRef.current.size > 0;
    optimisticCellValuesRef.current.clear();
    optimisticCellRequestsRef.current.clear();
    if (hadEntries) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, optimisticCellRequestsRef, optimisticCellValuesRef]);

  const captureOptimisticCellValues = useCallback((updates, requestId) => {
    const requestKeys = new Set();
    const storeOptimisticValue = (rowId, colId, value) => {
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
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      storeOptimisticValue(rowId, colId, update.value);
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      const normalizedRowPath = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowPath || normalizedRowPath === GRAND_TOTAL_ROW_ID) return;
      const pathParts = normalizedRowPath.split('|||');
      const ancestorPaths = [];
      for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
        ancestorPaths.push(pathParts.slice(0, depth).join('|||'));
      }
      ancestorPaths.push(GRAND_TOTAL_ROW_ID);
      if (ancestorPaths.length === 0) return;
      const nextValue = Number(update.value);
      const previousValue = Number(update.oldValue);
      if (aggregationFn === 'sum') {
        if (!Number.isFinite(nextValue) || !Number.isFinite(previousValue)) return;
        const delta = nextValue - previousValue;
        if (!Number.isFinite(delta) || delta === 0) return;
        ancestorPaths.forEach(ancestorPath => {
          const ancestorValue = Number(resolveCurrentCellValue(ancestorPath, colId));
          if (!Number.isFinite(ancestorValue)) return;
          storeOptimisticValue(ancestorPath, colId, ancestorValue + delta);
        });
        return;
      }
      if (aggregationFn === 'min' || aggregationFn === 'max') {
        if (!Number.isFinite(nextValue)) return;
        const reducer = aggregationFn === 'min' ? Math.min : Math.max;
        ancestorPaths.forEach(ancestorPath => {
          const childRowIds = getDirectVisibleChildRowIds(ancestorPath);
          if (childRowIds.length === 0) return;
          const childValues = childRowIds.map(childRowId => {
            if (childRowId === normalizedRowPath) return nextValue;
            return Number(resolveCurrentCellValue(childRowId, colId));
          }).filter(value => Number.isFinite(value));
          if (childValues.length === 0) return;
          storeOptimisticValue(ancestorPath, colId, reducer(...childValues));
        });
      }
    });
    if (!requestId || requestKeys.size === 0) return;
    optimisticCellRequestsRef.current.set(requestId, Array.from(requestKeys));
    bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, getDirectVisibleChildRowIds, getOptimisticCellKey, optimisticCellRequestsRef, optimisticCellValuesRef, resolveAggregationConfigForColumnId, resolveCurrentCellValue]);

  const releaseOptimisticCellRequest = useCallback(requestId => {
    if (!requestId) return;
    optimisticCellRequestsRef.current.delete(requestId);
  }, [optimisticCellRequestsRef]);

  const clearOptimisticCellValuesForRequest = useCallback(requestId => {
    if (!requestId) return;
    const requestKeys = optimisticCellRequestsRef.current.get(requestId);
    if (!Array.isArray(requestKeys) || requestKeys.length === 0) {
      optimisticCellRequestsRef.current.delete(requestId);
      return;
    }
    let didDelete = false;
    requestKeys.forEach(key => {
      const optimisticEntry = optimisticCellValuesRef.current.get(key);
      if (!optimisticEntry || optimisticEntry.requestId !== requestId) return;
      optimisticCellValuesRef.current.delete(key);
      didDelete = true;
    });
    optimisticCellRequestsRef.current.delete(requestId);
    if (didDelete) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, optimisticCellRequestsRef, optimisticCellValuesRef]);

  const resolveEditedCellMarker = useCallback((rowId, colId) => {
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return null;
    const marker = editedCellMarkersRef.current.get(key);
    if (!marker || (marker.directCount || 0) <= 0 && (marker.propagatedCount || 0) <= 0) {
      return null;
    }
    return {
      direct: (marker.directCount || 0) > 0,
      propagated: (marker.propagatedCount || 0) > 0
    };
  }, [editedCellMarkersRef, getOptimisticCellKey]);

  const clearAllEditedCellMarkers = useCallback(() => {
    if (editedCellMarkersRef.current.size === 0) return;
    editedCellMarkersRef.current.clear();
    bumpEditedCellEpoch();
  }, [bumpEditedCellEpoch, editedCellMarkersRef]);

  const clearAllComparisonValueState = useCallback(() => {
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

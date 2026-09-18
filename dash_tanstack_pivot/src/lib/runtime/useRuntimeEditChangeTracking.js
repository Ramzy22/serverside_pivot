import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID } from '../hooks/usePivotNormalization';

/** Tracks optimistic edit overlays, comparison values, and aggregate markers. */
export function useRuntimeEditChangeTracking({
  bumpComparisonCellEpoch,
  bumpEditedCellEpoch,
  bumpOptimisticCellEpoch,
  cellValuesMatch,
  comparisonCellCountsRef,
  comparisonCellOriginalValuesRef,
  editedCellMarkersRef,
  getOptimisticCellKey,
  optimisticCellValuesRef,
  resolveAggregationConfigForColumnId,
  resolveCurrentCellValue,
  resolveVisibleDataRowId,
  syncComparisonValueState,
  tableDataRef,
}) {
  const buildEditedCellMarkerPlan = useCallback(updates => {
    const directKeys = new Set();
    const propagatedKeys = new Set();
    const markCell = (rowId, colId, kind = 'propagated') => {
      const key = getOptimisticCellKey(rowId, colId);
      if (!key) return;
      if (kind === 'direct') {
        directKeys.add(key);
        propagatedKeys.delete(key);
        return;
      }
      if (!directKeys.has(key)) propagatedKeys.add(key);
    };
    const markVisibleDescendants = (ancestorRowId, colId) => {
      const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
      if (!normalizedAncestorId) return;
      const descendantPrefix = `${normalizedAncestorId}|||`;
      const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
      visibleRows.forEach(row => {
        const rowId = resolveVisibleDataRowId(row);
        if (!rowId || rowId === normalizedAncestorId) return;
        if (normalizedAncestorId === GRAND_TOTAL_ROW_ID) {
          if (rowId !== GRAND_TOTAL_ROW_ID) markCell(rowId, colId, 'propagated');
          return;
        }
        if (rowId.startsWith(descendantPrefix)) markCell(rowId, colId, 'propagated');
      });
    };
    (Array.isArray(updates) ? updates : []).forEach(update => {
      if (!update || typeof update !== 'object') return;
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      markCell(rowId, colId, 'direct');
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowId) return;
      const skipDescendants = String(update.propagationStrategy || '').trim().toLowerCase() === 'none';
      if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
        if (!skipDescendants) markVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
        return;
      }
      const pathParts = normalizedRowId.split('|||');
      for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
        markCell(pathParts.slice(0, depth).join('|||'), colId, 'propagated');
      }
      markCell(GRAND_TOTAL_ROW_ID, colId, 'propagated');
      if (!skipDescendants) markVisibleDescendants(normalizedRowId, colId);
    });
    if (directKeys.size === 0 && propagatedKeys.size === 0) return null;
    return {
      direct: Array.from(directKeys),
      propagated: Array.from(propagatedKeys).filter(key => !directKeys.has(key)),
    };
  }, [getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveVisibleDataRowId, tableDataRef]);

  const buildComparisonValuePlan = useCallback(updates => {
    const cells = new Map();
    const addCell = (rowId, colId, originalValue = undefined) => {
      const key = getOptimisticCellKey(rowId, colId);
      if (!key) return;
      const existingCell = cells.get(key);
      const nextCell = { key, rowId: String(rowId), colId: String(colId) };
      if (originalValue !== undefined) nextCell.originalValue = originalValue;
      else if (existingCell && Object.prototype.hasOwnProperty.call(existingCell, 'originalValue')) nextCell.originalValue = existingCell.originalValue;
      cells.set(key, nextCell);
    };
    const addVisibleDescendants = (ancestorRowId, colId) => {
      const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
      if (!normalizedAncestorId) return;
      const descendantPrefix = `${normalizedAncestorId}|||`;
      const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
      visibleRows.forEach(row => {
        const rowId = resolveVisibleDataRowId(row);
        if (!rowId || rowId === normalizedAncestorId) return;
        if (normalizedAncestorId === GRAND_TOTAL_ROW_ID) {
          if (rowId !== GRAND_TOTAL_ROW_ID) addCell(rowId, colId);
          return;
        }
        if (rowId.startsWith(descendantPrefix)) addCell(rowId, colId);
      });
    };
    (Array.isArray(updates) ? updates : []).forEach(update => {
      if (!update || typeof update !== 'object') return;
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      addCell(rowId, colId, update.oldValue);
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowId) return;
      const skipDescendants = String(update.propagationStrategy || '').trim().toLowerCase() === 'none';
      if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
        if (!skipDescendants) addVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
        return;
      }
      const pathParts = normalizedRowId.split('|||');
      for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) addCell(pathParts.slice(0, depth).join('|||'), colId);
      addCell(GRAND_TOTAL_ROW_ID, colId);
      if (!skipDescendants) addVisibleDescendants(normalizedRowId, colId);
    });
    return cells.size === 0 ? null : { cells: Array.from(cells.values()) };
  }, [getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveVisibleDataRowId, tableDataRef]);

  const applyComparisonValuePlan = useCallback((comparisonPlan, direction = 'forward') => {
    if (!comparisonPlan || typeof comparisonPlan !== 'object') return;
    const delta = direction === 'backward' ? -1 : 1;
    let didChange = false;
    (Array.isArray(comparisonPlan.cells) ? comparisonPlan.cells : []).forEach(cell => {
      if (!cell || typeof cell !== 'object') return;
      const key = getOptimisticCellKey(cell.rowId, cell.colId);
      if (!key) return;
      const existingCount = comparisonCellCountsRef.current.get(key) || 0;
      if (delta > 0 && existingCount === 0) {
        comparisonCellOriginalValuesRef.current.set(key, {
          rowId: String(cell.rowId), colId: String(cell.colId),
          value: Object.prototype.hasOwnProperty.call(cell, 'originalValue') ? cell.originalValue : resolveCurrentCellValue(cell.rowId, cell.colId),
        });
        didChange = true;
      }
      const nextCount = Math.max(0, existingCount + delta);
      if (nextCount === 0) {
        if (comparisonCellCountsRef.current.delete(key)) didChange = true;
        if (comparisonCellOriginalValuesRef.current.delete(key)) didChange = true;
        return;
      }
      if (nextCount !== existingCount) {
        comparisonCellCountsRef.current.set(key, nextCount);
        didChange = true;
      }
    });
    if (didChange) {
      syncComparisonValueState();
      bumpComparisonCellEpoch();
    }
  }, [bumpComparisonCellEpoch, comparisonCellCountsRef, comparisonCellOriginalValuesRef, getOptimisticCellKey, resolveCurrentCellValue, syncComparisonValueState]);

  const applyEditedCellMarkerPlan = useCallback((markerPlan, direction = 'forward') => {
    if (!markerPlan || typeof markerPlan !== 'object') return;
    const delta = direction === 'backward' ? -1 : 1;
    let didChange = false;
    const applyMarkerKeys = (keys, countField) => {
      (Array.isArray(keys) ? keys : []).forEach(key => {
        if (!key) return;
        const existingEntry = editedCellMarkersRef.current.get(key) || { directCount: 0, propagatedCount: 0 };
        const nextCount = Math.max(0, (existingEntry[countField] || 0) + delta);
        const nextEntry = { ...existingEntry, [countField]: nextCount };
        if ((nextEntry.directCount || 0) === 0 && (nextEntry.propagatedCount || 0) === 0) {
          if (editedCellMarkersRef.current.delete(key)) didChange = true;
          return;
        }
        editedCellMarkersRef.current.set(key, nextEntry);
        didChange = true;
      });
    };
    applyMarkerKeys(markerPlan.direct, 'directCount');
    applyMarkerKeys(markerPlan.propagated, 'propagatedCount');
    if (didChange) bumpEditedCellEpoch();
  }, [bumpEditedCellEpoch, editedCellMarkersRef]);

  const reconcileOptimisticCellValuesWithPayload = useCallback(payload => {
    const responseRows = Array.isArray(payload && payload.data) ? payload.data : [];
    if (responseRows.length === 0 || optimisticCellValuesRef.current.size === 0) return;
    const rowLookup = new Map();
    responseRows.forEach(row => {
      if (!row || typeof row !== 'object') return;
      const rowId = row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total'
        ? GRAND_TOTAL_ROW_ID
        : row._path !== undefined && row._path !== null && row._path !== '' ? String(row._path)
          : row.id !== undefined && row.id !== null && row.id !== '' ? String(row.id)
            : row._id !== undefined && row._id !== null && row._id !== '' ? String(row._id) : null;
      if (rowId) rowLookup.set(rowId, row);
    });
    let didDelete = false;
    optimisticCellValuesRef.current.forEach((entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      const row = rowLookup.get(entry.rowId);
      if (!row || !cellValuesMatch(entry.value, row[entry.colId])) return;
      optimisticCellValuesRef.current.delete(key);
      didDelete = true;
    });
    if (didDelete) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, cellValuesMatch, optimisticCellValuesRef]);

  return { applyComparisonValuePlan, applyEditedCellMarkerPlan, buildComparisonValuePlan, buildEditedCellMarkerPlan, reconcileOptimisticCellValuesWithPayload };
}

import { useCallback } from 'react';

import { GRAND_TOTAL_ROW_ID, isGrandTotalRow } from '../hooks/usePivotNormalization';

/** Tracks optimistic edit overlays, comparison values, and aggregate markers.  @param {any} p0 @returns {any} */
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
  /** @type {(...args: any[]) => any} */
  const buildEditedCellMarkerPlan = useCallback(updates => {
    /** @type {any} */
    const directKeys = new Set();
    /** @type {any} */
    const propagatedKeys = new Set();
    /** @param {any} [rowId] @param {any} [colId] @param {any} [kind] @returns {any} */
    const markCell = (rowId, colId, kind = 'propagated') => {
      /** @type {any} */
      const key = getOptimisticCellKey(rowId, colId);
      if (!key) return;
      if (kind === 'direct') {
        directKeys.add(key);
        propagatedKeys.delete(key);
        return;
      }
      if (!directKeys.has(key)) propagatedKeys.add(key);
    };
    /** @param {any} [ancestorRowId] @param {any} [colId] @returns {any} */
    const markVisibleDescendants = (ancestorRowId, colId) => {
      /** @type {any} */
      const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
      if (!normalizedAncestorId) return;
      const descendantPrefix = `${normalizedAncestorId}|||`;
      /** @type {any} */
      const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
      visibleRows.forEach(/** @param {any} [row] @returns {any} */ row => {
        /** @type {any} */
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
      /** @type {any} */
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      markCell(rowId, colId, 'direct');
      /** @type {any} */
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      /** @type {any} */
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      /** @type {any} */
      const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowId) return;
      /** @type {any} */
      const skipDescendants = String(update.propagationStrategy || '').trim().toLowerCase() === 'none';
      if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
        if (!skipDescendants) markVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
        return;
      }
      /** @type {any} */
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

  /** @type {(...args: any[]) => any} */
  const buildComparisonValuePlan = useCallback(updates => {
    /** @type {any} */
    const cells = new Map();
    /** @param {any} [rowId] @param {any} [colId] @param {any} [originalValue] @returns {any} */
    const addCell = (rowId, colId, originalValue = undefined) => {
      /** @type {any} */
      const key = getOptimisticCellKey(rowId, colId);
      if (!key) return;
      /** @type {any} */
      const existingCell = cells.get(key);
      /** @type {Record<string, any>} */
      const nextCell = { key, rowId: String(rowId), colId: String(colId) };
      if (originalValue !== undefined) nextCell.originalValue = originalValue;
      else if (existingCell && Object.prototype.hasOwnProperty.call(existingCell, 'originalValue')) nextCell.originalValue = existingCell.originalValue;
      cells.set(key, nextCell);
    };
    /** @param {any} [ancestorRowId] @param {any} [colId] @returns {any} */
    const addVisibleDescendants = (ancestorRowId, colId) => {
      /** @type {any} */
      const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
      if (!normalizedAncestorId) return;
      const descendantPrefix = `${normalizedAncestorId}|||`;
      /** @type {any} */
      const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
      visibleRows.forEach(/** @param {any} [row] @returns {any} */ row => {
        /** @type {any} */
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
      /** @type {any} */
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      addCell(rowId, colId, update.oldValue);
      /** @type {any} */
      const aggregation = update.aggregation && typeof update.aggregation === 'object' ? update.aggregation : resolveAggregationConfigForColumnId(colId);
      /** @type {any} */
      const aggregationFn = aggregation && aggregation.agg ? String(aggregation.agg).trim().toLowerCase() : '';
      if (!aggregationFn || aggregation.windowFn) return;
      /** @type {any} */
      const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
      if (!normalizedRowId) return;
      /** @type {any} */
      const skipDescendants = String(update.propagationStrategy || '').trim().toLowerCase() === 'none';
      if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
        if (!skipDescendants) addVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
        return;
      }
      /** @type {any} */
      const pathParts = normalizedRowId.split('|||');
      for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) addCell(pathParts.slice(0, depth).join('|||'), colId);
      addCell(GRAND_TOTAL_ROW_ID, colId);
      if (!skipDescendants) addVisibleDescendants(normalizedRowId, colId);
    });
    return cells.size === 0 ? null : { cells: Array.from(cells.values()) };
  }, [getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveVisibleDataRowId, tableDataRef]);

  /** @type {(...args: any[]) => any} */
  const applyComparisonValuePlan = useCallback((comparisonPlan, direction = 'forward') => {
    if (!comparisonPlan || typeof comparisonPlan !== 'object') return;
    /** @type {any} */
    const delta = direction === 'backward' ? -1 : 1;
    let didChange = false;
    (Array.isArray(comparisonPlan.cells) ? comparisonPlan.cells : []).forEach(/** @param {any} [cell] @returns {any} */ cell => {
      if (!cell || typeof cell !== 'object') return;
      /** @type {any} */
      const key = getOptimisticCellKey(cell.rowId, cell.colId);
      if (!key) return;
      /** @type {any} */
      const existingCount = comparisonCellCountsRef.current.get(key) || 0;
      if (delta > 0 && existingCount === 0) {
        comparisonCellOriginalValuesRef.current.set(key, {
          rowId: String(cell.rowId), colId: String(cell.colId),
          value: Object.prototype.hasOwnProperty.call(cell, 'originalValue') ? cell.originalValue : resolveCurrentCellValue(cell.rowId, cell.colId),
        });
        didChange = true;
      }
      /** @type {any} */
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

  /** @type {(...args: any[]) => any} */
  const applyEditedCellMarkerPlan = useCallback((markerPlan, direction = 'forward') => {
    if (!markerPlan || typeof markerPlan !== 'object') return;
    /** @type {any} */
    const delta = direction === 'backward' ? -1 : 1;
    let didChange = false;
    /** @param {any} [keys] @param {any} [countField] @returns {any} */
    const applyMarkerKeys = (keys, countField) => {
      (Array.isArray(keys) ? keys : []).forEach(key => {
        if (!key) return;
        /** @type {any} */
        const existingEntry = editedCellMarkersRef.current.get(key) || { directCount: 0, propagatedCount: 0 };
        /** @type {any} */
        const nextCount = Math.max(0, (existingEntry[countField] || 0) + delta);
        /** @type {Record<string, any>} */
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

  /** @type {(...args: any[]) => any} */
  const reconcileOptimisticCellValuesWithPayload = useCallback(payload => {
    /** @type {any} */
    const responseRows = Array.isArray(payload && payload.data) ? payload.data : [];
    if (responseRows.length === 0 || optimisticCellValuesRef.current.size === 0) return;
    /** @type {any} */
    const rowLookup = new Map();
    responseRows.forEach(/** @param {any} [row] @returns {any} */ row => {
      if (!row || typeof row !== 'object') return;
      /** @type {any} */
      const rowId = isGrandTotalRow(row)
        ? GRAND_TOTAL_ROW_ID
        : row._path !== undefined && row._path !== null && row._path !== '' ? String(row._path)
          : row.id !== undefined && row.id !== null && row.id !== '' ? String(row.id)
            : row._id !== undefined && row._id !== null && row._id !== '' ? String(row._id) : null;
      if (rowId) rowLookup.set(rowId, row);
    });
    let didDelete = false;
    optimisticCellValuesRef.current.forEach(/** @param {any} [entry] @param {any} [key] @returns {any} */ (entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      /** @type {any} */
      const row = rowLookup.get(entry.rowId);
      if (!row || !cellValuesMatch(entry.value, row[entry.colId])) return;
      optimisticCellValuesRef.current.delete(key);
      didDelete = true;
    });
    if (didDelete) bumpOptimisticCellEpoch();
  }, [bumpOptimisticCellEpoch, cellValuesMatch, optimisticCellValuesRef]);

  return { applyComparisonValuePlan, applyEditedCellMarkerPlan, buildComparisonValuePlan, buildEditedCellMarkerPlan, reconcileOptimisticCellValuesWithPayload };
}

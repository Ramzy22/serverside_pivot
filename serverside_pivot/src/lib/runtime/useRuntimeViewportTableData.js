import { useCallback, useMemo } from 'react';

import { GRAND_TOTAL_ROW_ID, isGrandTotalRow } from '../hooks/usePivotNormalization';

/** Builds the table's rendered rows while preserving server pinning and edit comparison state.  @param {any} p0 @returns {any} */
export function useRuntimeViewportTableData({
  comparisonCellCountsRef,
  comparisonCellEpoch,
  comparisonCellOriginalValuesRef,
  comparisonValueState,
  editValueDisplayMode,
  filteredData,
  grandTotalRow,
  nodes,
  pinnedRowCacheRef,
  renderedData,
  rowPinning,
  serverSide,
  serverSidePinsGrandTotal,
  showColTotals,
  tableDataRef,
  total,
}) {
  /** @type {(...args: any[]) => any} */
  const getStableDataRowId = useCallback(row => {
    if (!row) return null;
    if (isGrandTotalRow(row)) return GRAND_TOTAL_ROW_ID;
    if (row._path) return row._path;
    if (row.id) return row.id;
    if (typeof row.__virtualIndex === 'number') return String(row.__virtualIndex);
    return null;
  }, []);

  /** @type {any} */
  const rawTableData = useMemo(() => {
    if (serverSide) {
      /** @type {any} */
      const centerData = renderedData.filter(/** @param {any} [row] @returns {any} */ row => { if (!row) return false; if (isGrandTotalRow(row)) return !serverSidePinsGrandTotal; return true; });
      /** @type {any} */
      const seenIds = new Set(centerData.map(getStableDataRowId).filter(Boolean));
      /** @type {any} */
      const pinnedIds = Array.from(new Set([...(rowPinning && rowPinning.top || []), ...(rowPinning && rowPinning.bottom || [])]));
      /** @type {any} */
      const pinnedData = pinnedIds.map(/** @param {any} [rowId] @returns {any} */ rowId => {
        /** @type {any} */
        const cachedRow = pinnedRowCacheRef.current.get(rowId);
        return cachedRow && cachedRow.original ? cachedRow.original : null;
      }).filter(/** @param {any} [row] @returns {any} */ row => {
        /** @type {any} */
        const stableId = getStableDataRowId(row);
        if (!stableId || seenIds.has(stableId)) return false;
        seenIds.add(stableId);
        return true;
      });
      /** @type {any[]} */
      const nextData = [...centerData, ...pinnedData];
      if (serverSidePinsGrandTotal && grandTotalRow && !seenIds.has(GRAND_TOTAL_ROW_ID)) nextData.push(grandTotalRow);
      return nextData;
    }
    /** @type {any} */
    let baseData = filteredData.length ? [...nodes] : [];
    if (!showColTotals) baseData = baseData.filter(/** @param {any} [row] @returns {any} */ row => !row._isTotal);
    return showColTotals ? [...baseData, total] : baseData;
  }, [filteredData, getStableDataRowId, grandTotalRow, nodes, pinnedRowCacheRef, renderedData, rowPinning, serverSide, serverSidePinsGrandTotal, showColTotals, total]);

  if (tableDataRef) tableDataRef.current = rawTableData;

  /** @type {any} */
  const tableData = useMemo(() => {
    if (editValueDisplayMode !== 'original' || comparisonValueState.activeCount === 0) return rawTableData;
    /** @type {any} */
    const overrideRowsById = new Map();
    comparisonCellOriginalValuesRef.current.forEach(/** @param {any} [entry] @param {any} [key] @returns {any} */ (entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      /** @type {any} */
      const cellCount = comparisonCellCountsRef.current.get(key) || 0;
      if (cellCount <= 0) return;
      /** @type {any} */
      const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
      /** @type {any} */
      const colId = entry.colId === null || entry.colId === undefined ? '' : String(entry.colId);
      if (!rowId || !colId) return;
      /** @type {any} */
      const rowOverrides = overrideRowsById.get(rowId) || {};
      rowOverrides[colId] = entry.value;
      overrideRowsById.set(rowId, rowOverrides);
    });
    if (overrideRowsById.size === 0) return rawTableData;
    return rawTableData.map(/** @param {any} [row] @returns {any} */ row => {
      /** @type {any} */
      const rowId = getStableDataRowId(row);
      return !rowId || !overrideRowsById.has(rowId) ? row : { ...row, ...overrideRowsById.get(rowId) };
    });
  }, [comparisonCellCountsRef, comparisonCellEpoch, comparisonCellOriginalValuesRef, comparisonValueState.activeCount, editValueDisplayMode, getStableDataRowId, rawTableData]);

  return { getStableDataRowId, rawTableData, tableData };
}

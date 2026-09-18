import { useCallback, useMemo } from 'react';

const GRAND_TOTAL_ROW_ID = '__grand_total__';

/** Builds the table's rendered rows while preserving server pinning and edit comparison state. */
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
  const getStableDataRowId = useCallback(row => {
    if (!row) return null;
    if (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total') return GRAND_TOTAL_ROW_ID;
    if (row._path) return row._path;
    if (row.id) return row.id;
    if (typeof row.__virtualIndex === 'number') return String(row.__virtualIndex);
    return null;
  }, []);

  const rawTableData = useMemo(() => {
    if (serverSide) {
      const centerData = renderedData.filter(row => row && (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total' ? !serverSidePinsGrandTotal : !row._isTotal));
      const seenIds = new Set(centerData.map(getStableDataRowId).filter(Boolean));
      const pinnedIds = Array.from(new Set([...(rowPinning && rowPinning.top || []), ...(rowPinning && rowPinning.bottom || [])]));
      const pinnedData = pinnedIds.map(rowId => {
        const cachedRow = pinnedRowCacheRef.current.get(rowId);
        return cachedRow && cachedRow.original ? cachedRow.original : null;
      }).filter(row => {
        const stableId = getStableDataRowId(row);
        if (!stableId || seenIds.has(stableId)) return false;
        seenIds.add(stableId);
        return true;
      });
      const nextData = [...centerData, ...pinnedData];
      if (serverSidePinsGrandTotal && showColTotals && grandTotalRow && !seenIds.has(GRAND_TOTAL_ROW_ID)) nextData.push(grandTotalRow);
      return nextData;
    }
    let baseData = filteredData.length ? [...nodes] : [];
    if (!showColTotals) baseData = baseData.filter(row => !row._isTotal);
    return showColTotals ? [...baseData, total] : baseData;
  }, [filteredData, getStableDataRowId, grandTotalRow, nodes, pinnedRowCacheRef, renderedData, rowPinning, serverSide, serverSidePinsGrandTotal, showColTotals, total]);

  if (tableDataRef) tableDataRef.current = rawTableData;

  const tableData = useMemo(() => {
    if (editValueDisplayMode !== 'original' || comparisonValueState.activeCount === 0) return rawTableData;
    const overrideRowsById = new Map();
    comparisonCellOriginalValuesRef.current.forEach((entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      const cellCount = comparisonCellCountsRef.current.get(key) || 0;
      if (cellCount <= 0) return;
      const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
      const colId = entry.colId === null || entry.colId === undefined ? '' : String(entry.colId);
      if (!rowId || !colId) return;
      const rowOverrides = overrideRowsById.get(rowId) || {};
      rowOverrides[colId] = entry.value;
      overrideRowsById.set(rowId, rowOverrides);
    });
    if (overrideRowsById.size === 0) return rawTableData;
    return rawTableData.map(row => {
      const rowId = getStableDataRowId(row);
      return !rowId || !overrideRowsById.has(rowId) ? row : { ...row, ...overrideRowsById.get(rowId) };
    });
  }, [comparisonCellCountsRef, comparisonCellEpoch, comparisonCellOriginalValuesRef, comparisonValueState.activeCount, editValueDisplayMode, getStableDataRowId, rawTableData]);

  return { getStableDataRowId, rawTableData, tableData };
}

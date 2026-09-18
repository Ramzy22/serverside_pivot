import { useCallback, useEffect } from 'react';

/** Owns row-oriented selection and its document-level completion handler. */
export function useRuntimeRowSelection({
  getDisplayRows,
  lastSelected,
  resolveDisplayRowIndex,
  setIsRowSelecting,
  setLastSelected,
  setRowDragStart,
  setSelectedCells,
  tableRef,
}) {
  useEffect(() => {
    const handleMouseUp = () => {
      setIsRowSelecting(false);
      setRowDragStart(null);
    };
    window.addEventListener('mouseup', handleMouseUp);
    return () => window.removeEventListener('mouseup', handleMouseUp);
  }, [setIsRowSelecting, setRowDragStart]);

  const handleRowRangeSelect = useCallback((startIdx, endIdx) => {
    if (!tableRef.current) return;
    const visibleCols = tableRef.current.getVisibleLeafColumns();
    const rows = getDisplayRows();
    const min = Math.min(startIdx, endIdx);
    const max = Math.max(startIdx, endIdx);
    const rangeSelection = {};
    for (let i = min; i <= max; i++) {
      const row = rows[i];
      if (!row) continue;
      visibleCols.forEach(col => {
        rangeSelection[`${row.id}:${col.id}`] = row.getValue(col.id);
      });
    }
    setSelectedCells(rangeSelection);
  }, [getDisplayRows, setSelectedCells, tableRef]);

  const handleRowSelect = useCallback((row, isShift, isCtrl) => {
    if (!tableRef.current) return;
    const visibleCols = tableRef.current.getVisibleLeafColumns();
    const rowId = row.id;
    const displayRowIndex = resolveDisplayRowIndex(rowId, row.index);
    const newSelection = {};
    visibleCols.forEach(col => {
      newSelection[`${rowId}:${col.id}`] = row.getValue(col.id);
    });
    if (isCtrl) {
      setSelectedCells(previous => ({
        ...previous,
        ...newSelection
      }));
      setLastSelected({ rowIndex: displayRowIndex, colIndex: 0 });
      return;
    }
    if (isShift && lastSelected) {
      const startRowIndex = lastSelected.rowIndex;
      const rows = getDisplayRows();
      const min = Math.min(startRowIndex, displayRowIndex);
      const max = Math.max(startRowIndex, displayRowIndex);
      const rangeSelection = {};
      for (let i = min; i <= max; i++) {
        const rangeRow = rows[i];
        if (!rangeRow) continue;
        visibleCols.forEach(col => {
          rangeSelection[`${rangeRow.id}:${col.id}`] = rangeRow.getValue(col.id);
        });
      }
      setSelectedCells(rangeSelection);
      return;
    }
    setSelectedCells(newSelection);
    setLastSelected({ rowIndex: displayRowIndex, colIndex: 0 });
  }, [getDisplayRows, lastSelected, resolveDisplayRowIndex, setLastSelected, setSelectedCells, tableRef]);

  return { handleRowRangeSelect, handleRowSelect };
}

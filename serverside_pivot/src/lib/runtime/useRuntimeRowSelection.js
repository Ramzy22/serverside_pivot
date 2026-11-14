import { useCallback, useEffect } from 'react';

/** Owns row-oriented selection and its document-level completion handler.  @param {any} p0 @returns {any} */
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
    /** @returns {any} */
    const handleMouseUp = () => {
      setIsRowSelecting(false);
      setRowDragStart(null);
    };
    window.addEventListener('mouseup', handleMouseUp);
    return () => window.removeEventListener('mouseup', handleMouseUp);
  }, [setIsRowSelecting, setRowDragStart]);

  /** @type {(...args: any[]) => any} */
  const handleRowRangeSelect = useCallback((startIdx, endIdx) => {
    if (!tableRef.current) return;
    /** @type {any} */
    const visibleCols = tableRef.current.getVisibleLeafColumns();
    /** @type {any} */
    const rows = getDisplayRows();
    /** @type {any} */
    const min = Math.min(startIdx, endIdx);
    /** @type {any} */
    const max = Math.max(startIdx, endIdx);
    /** @type {Record<string, any>} */
    const rangeSelection = {};
    for (let i = min; i <= max; i++) {
      const row = rows[i];
      if (!row) continue;
      visibleCols.forEach(/** @param {any} [col] @returns {any} */ col => {
        rangeSelection[`${row.id}:${col.id}`] = row.getValue(col.id);
      });
    }
    setSelectedCells(rangeSelection);
  }, [getDisplayRows, setSelectedCells, tableRef]);

  /** @type {(...args: any[]) => any} */
  const handleRowSelect = useCallback((row, isShift, isCtrl) => {
    if (!tableRef.current) return;
    /** @type {any} */
    const visibleCols = tableRef.current.getVisibleLeafColumns();
    const rowId = row.id;
    /** @type {any} */
    const displayRowIndex = resolveDisplayRowIndex(rowId, row.index);
    /** @type {Record<string, any>} */
    const newSelection = {};
    visibleCols.forEach(/** @param {any} [col] @returns {any} */ col => {
      newSelection[`${rowId}:${col.id}`] = row.getValue(col.id);
    });
    if (isCtrl) {
      setSelectedCells(/** @param {any} [previous] @returns {any} */ previous => ({
        ...previous,
        ...newSelection
      }));
      setLastSelected({ rowIndex: displayRowIndex, colIndex: 0 });
      return;
    }
    if (isShift && lastSelected) {
      const startRowIndex = lastSelected.rowIndex;
      /** @type {any} */
      const rows = getDisplayRows();
      /** @type {any} */
      const min = Math.min(startRowIndex, displayRowIndex);
      /** @type {any} */
      const max = Math.max(startRowIndex, displayRowIndex);
      /** @type {Record<string, any>} */
      const rangeSelection = {};
      for (let i = min; i <= max; i++) {
        const rangeRow = rows[i];
        if (!rangeRow) continue;
        visibleCols.forEach(/** @param {any} [col] @returns {any} */ col => {
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

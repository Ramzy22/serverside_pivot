import { useCallback, useEffect } from 'react';
import { isEditableKeyboardEvent } from '../hooks/usePivotNormalization';
import { getDisplayOrderedLeafColumns, isSelectableDisplayColumn } from './displayColumnOrder';

/**
 * Owns cell-oriented selection: keyboard navigation, range dragging, fill,
 * and clipboard fallback. Row selection remains a separate domain because it
 * has different selection semantics and event ownership.
 */
export function useRuntimeCellSelection({
  buildSelectedClipboardPayload,
  copyRichToClipboard,
  copyCurrentTableToClipboard,
  dispatchBatchUpdateRequest,
  dragStart,
  fillRange,
  getDisplayRows,
  handleRedo,
  handleUndo,
  isDragging,
  isFilling,
  isSelectAllServerRef,
  lastSelected,
  loadedRows,
  scrollToDisplayColumn,
  scrollToDisplayRow,
  selectedCells,
  serverSide,
  setDragStart,
  setFillRange,
  setHoveredRowPath,
  setIsDragging,
  setIsFilling,
  setLastSelected,
  setSelectedCells,
  setProps,
  table,
  tableRef,
}) {
  const selectRange = useCallback((start, end) => {
    const rStart = Math.min(start.rowIndex, end.rowIndex);
    const rEnd = Math.max(start.rowIndex, end.rowIndex);
    const cStart = Math.min(start.colIndex, end.colIndex);
    const cEnd = Math.max(start.colIndex, end.colIndex);
    const visibleRows = getDisplayRows();
    const visibleCols = getDisplayOrderedLeafColumns(table);
    const newSelection = {};
    for (let r = rStart; r <= rEnd; r++) {
      for (let c = cStart; c <= cEnd; c++) {
        const row = visibleRows[r];
        const col = visibleCols[c];
        if (row && isSelectableDisplayColumn(col)) {
          newSelection[`${row.id}:${col.id}`] = row.getValue(col.id);
        }
      }
    }
    return newSelection;
  }, [getDisplayRows, table]);

  const handleKeyDown = useCallback(e => {
    // Preserve native editing shortcuts whenever the event originated in a
    // text control.
    if (isEditableKeyboardEvent(e)) return;
    if ((e.ctrlKey || e.metaKey) && !e.altKey && e.key) {
      const shortcutKey = e.key.toLowerCase();
      if (shortcutKey === 'z') {
        e.preventDefault();
        if (e.shiftKey) handleRedo();else handleUndo();
        return;
      }
      if (shortcutKey === 'y') {
        e.preventDefault();
        handleRedo();
        return;
      }
    }
    const displayColumns = getDisplayOrderedLeafColumns(tableRef.current);
    const visibleLeafColumnsAll = displayColumns.filter(isSelectableDisplayColumn);
    const visibleRowsAll = getDisplayRows();

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
      e.preventDefault();
      const allSelection = {};
      visibleRowsAll.forEach(row => {
        visibleLeafColumnsAll.forEach(col => {
          allSelection[`${row.id}:${col.id}`] = row.getValue(col.id);
        });
      });
      setSelectedCells(allSelection);
      isSelectAllServerRef.current = serverSide;
      if (visibleRowsAll.length > 0 && visibleLeafColumnsAll.length > 0) {
        const firstDisplayIndex = displayColumns.indexOf(visibleLeafColumnsAll[0]);
        setLastSelected({ rowIndex: 0, colIndex: firstDisplayIndex });
        setDragStart({ rowIndex: 0, colIndex: firstDisplayIndex });
      }
      return;
    }

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') {
      const keys = Object.keys(selectedCells);
      if (keys.length === 0) return;
      e.preventDefault();
      if (isSelectAllServerRef.current && serverSide) {
        // Ctrl/Cmd+A means the user selected the entire visible table, not
        // an arbitrary cell range. Keep it equivalent to the explicit Copy
        // Current Table command, including its labelled header row.
        copyCurrentTableToClipboard(true, false);
        return;
      }
      const payload = buildSelectedClipboardPayload(false, selectedCells);
      if (payload) copyRichToClipboard(payload);
      return;
    }
    if (!lastSelected) return;
    const { rowIndex } = lastSelected;
    // Navigate the data columns, then map back onto the rendered positional
    // axis so gutters never shift mouse, scroll, and keyboard anchors apart.
    const colIndex = visibleLeafColumnsAll.indexOf(displayColumns[lastSelected.colIndex]);
    if (colIndex < 0) return;
    let nextRow = rowIndex;
    let nextCol = colIndex;
    const visibleLeafColumns = visibleLeafColumnsAll;

    const ctrlArrowRow = dir => {
      const colId = visibleLeafColumns[colIndex] && visibleLeafColumns[colIndex].id;
      if (!colId) return dir > 0 ? visibleRowsAll.length - 1 : 0;
      const isEmpty = row => {
        const value = row && row.getValue(colId);
        return value === null || value === undefined || value === '';
      };
      const curEmpty = isEmpty(visibleRowsAll[rowIndex]);
      if (curEmpty) {
        for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
          if (!isEmpty(visibleRowsAll[i])) return i;
        }
        return dir > 0 ? visibleRowsAll.length - 1 : 0;
      }
      const nextIdx = rowIndex + dir;
      if (nextIdx < 0 || nextIdx >= visibleRowsAll.length) return dir > 0 ? visibleRowsAll.length - 1 : 0;
      if (isEmpty(visibleRowsAll[nextIdx])) {
        for (let i = nextIdx + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
          if (!isEmpty(visibleRowsAll[i])) return i;
        }
        return dir > 0 ? visibleRowsAll.length - 1 : 0;
      }
      let last = rowIndex;
      for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
        if (isEmpty(visibleRowsAll[i])) break;
        last = i;
      }
      return last;
    };
    const ctrlArrowCol = dir => {
      const row = visibleRowsAll[rowIndex];
      if (!row) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      const isEmpty = col => {
        const value = row.getValue(col && col.id);
        return value === null || value === undefined || value === '';
      };
      const curEmpty = isEmpty(visibleLeafColumns[colIndex]);
      if (curEmpty) {
        for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
          if (!isEmpty(visibleLeafColumns[i])) return i;
        }
        return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      }
      const nextIdx = colIndex + dir;
      if (nextIdx < 0 || nextIdx >= visibleLeafColumns.length) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      if (isEmpty(visibleLeafColumns[nextIdx])) {
        for (let i = nextIdx + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
          if (!isEmpty(visibleLeafColumns[i])) return i;
        }
        return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      }
      let last = colIndex;
      for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
        if (isEmpty(visibleLeafColumns[i])) break;
        last = i;
      }
      return last;
    };
    if (e.key.startsWith('Arrow')) {
      e.preventDefault();
      if (e.ctrlKey || e.metaKey) {
        if (e.key === 'ArrowDown') nextRow = ctrlArrowRow(1);else if (e.key === 'ArrowUp') nextRow = ctrlArrowRow(-1);else if (e.key === 'ArrowRight') nextCol = ctrlArrowCol(1);else if (e.key === 'ArrowLeft') nextCol = ctrlArrowCol(-1);
      } else {
        if (e.key === 'ArrowUp') nextRow = Math.max(0, rowIndex - 1);else if (e.key === 'ArrowDown') nextRow = Math.min(visibleRowsAll.length - 1, rowIndex + 1);else if (e.key === 'ArrowLeft') nextCol = Math.max(0, colIndex - 1);else if (e.key === 'ArrowRight') nextCol = Math.min(visibleLeafColumns.length - 1, colIndex + 1);
      }
    } else if (e.key === 'Tab') {
      e.preventDefault();
      nextCol = e.shiftKey ? Math.max(0, colIndex - 1) : Math.min(visibleLeafColumns.length - 1, colIndex + 1);
    } else {
      return;
    }
    const nextRowObj = visibleRowsAll[nextRow];
    const nextColObj = visibleLeafColumns[nextCol];
    if (nextRowObj && nextColObj) {
      const nextDisplayCol = displayColumns.indexOf(nextColObj);
      const key = `${nextRowObj.id}:${nextColObj.id}`;
      const value = nextRowObj.getValue(nextColObj.id);
      if (e.shiftKey && e.key.startsWith('Arrow')) {
        setSelectedCells(selectRange(dragStart || lastSelected, { rowIndex: nextRow, colIndex: nextDisplayCol }));
      } else {
        setSelectedCells({ [key]: value });
        setDragStart({ rowIndex: nextRow, colIndex: nextDisplayCol });
      }
      setLastSelected({ rowIndex: nextRow, colIndex: nextDisplayCol });
      scrollToDisplayRow(nextRow);
      scrollToDisplayColumn(nextDisplayCol);
    }
  }, [buildSelectedClipboardPayload, copyCurrentTableToClipboard, copyRichToClipboard, dragStart, getDisplayRows, handleRedo, handleUndo, isSelectAllServerRef, lastSelected, scrollToDisplayColumn, scrollToDisplayRow, selectedCells, selectRange, serverSide, setDragStart, setLastSelected, setSelectedCells, tableRef]);

  const handleCellMouseDown = useCallback((event, rowIndex, colIndex, rowId, colId, value) => {
    if (!isSelectableDisplayColumn({ id: colId })) return;
    if (event.button === 2) return;
    if (event.shiftKey) {
      event.preventDefault();
      const start = lastSelected || { rowIndex, colIndex };
      const newSelection = selectRange(start, { rowIndex, colIndex });
      if (event.ctrlKey || event.metaKey) {
        setSelectedCells(previous => ({ ...previous, ...newSelection }));
      } else {
        setSelectedCells(newSelection);
      }
      return;
    }
    isSelectAllServerRef.current = false;
    setIsDragging(true);
    setDragStart({ rowIndex, colIndex });
    setLastSelected({ rowIndex, colIndex });
    let clickedRow = null;
    if (tableRef.current && typeof tableRef.current.getRow === 'function') {
      try {
        clickedRow = tableRef.current.getRow(rowId, true);
      } catch (error) {
        clickedRow = null;
      }
    }
    if (!clickedRow && tableRef.current && tableRef.current.getRowModel) {
      clickedRow = getDisplayRows()[rowIndex] || null;
    }
    if (clickedRow && clickedRow.original && clickedRow.original._path) {
      setHoveredRowPath(clickedRow.original._path);
    }
    const key = `${rowId}:${colId}`;
    if (event.ctrlKey || event.metaKey) {
      setSelectedCells({ ...selectedCells, [key]: value });
    } else {
      setSelectedCells({ [key]: value });
    }
  }, [getDisplayRows, isSelectAllServerRef, lastSelected, selectRange, selectedCells, setDragStart, setHoveredRowPath, setIsDragging, setLastSelected, setSelectedCells, tableRef]);

  const handleCellMouseEnter = useCallback((rowIndex, colIndex) => {
    if (isDragging && dragStart) {
      setSelectedCells(selectRange(dragStart, { rowIndex, colIndex }));
    }
    if (isFilling && dragStart) {
      setFillRange({
        rStart: Math.min(dragStart.rowIndex, rowIndex),
        rEnd: Math.max(dragStart.rowIndex, rowIndex),
        cStart: Math.min(dragStart.colIndex, colIndex),
        cEnd: Math.max(dragStart.colIndex, colIndex),
      });
    }
  }, [dragStart, isDragging, isFilling, selectRange, setFillRange, setSelectedCells]);

  const handleFillMouseDown = useCallback(event => {
    event.stopPropagation();
    event.preventDefault();
    setIsFilling(true);
  }, [setIsFilling]);

  const handleFillMouseUp = useCallback(() => {
    if (isFilling && fillRange && setProps) {
      const displayRows = getDisplayRows();
      const startRow = displayRows[dragStart.rowIndex];
      const startValue = startRow ? startRow.getVisibleCells()[dragStart.colIndex].getValue() : undefined;
      const updates = [];
      const visibleCols = table.getVisibleLeafColumns();
      for (let r = fillRange.rStart; r <= fillRange.rEnd; r++) {
        for (let c = fillRange.cStart; c <= fillRange.cEnd; c++) {
          const row = displayRows[r];
          const col = visibleCols[c];
          if (row && col) updates.push({ rowId: row.id, colId: col.id, value: startValue });
        }
      }
      if (updates.length > 0) dispatchBatchUpdateRequest(updates, 'fill');
    }
    setIsFilling(false);
    setFillRange(null);
  }, [dispatchBatchUpdateRequest, dragStart, fillRange, getDisplayRows, isFilling, setFillRange, setIsFilling, setProps, table]);

  useEffect(() => {
    const handleMouseUp = () => {
      setIsDragging(false);
      setDragStart(null);
      if (isFilling) handleFillMouseUp();
    };
    window.addEventListener('mouseup', handleMouseUp);
    return () => window.removeEventListener('mouseup', handleMouseUp);
  }, [dragStart, fillRange, handleFillMouseUp, isDragging, isFilling, setDragStart, setIsDragging]);

  useEffect(() => {
    const handleGlobalCopy = e => {
      if (isEditableKeyboardEvent(e)) return;
      if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
        const keys = Object.keys(selectedCells);
        if (keys.length === 0) return;
        e.preventDefault();
        if (isSelectAllServerRef.current && serverSide) {
          copyCurrentTableToClipboard(true, false);
          return;
        }
        const payload = buildSelectedClipboardPayload(false, selectedCells);
        if (payload) copyRichToClipboard(payload);
      }
    };
    window.addEventListener('keydown', handleGlobalCopy);
    return () => window.removeEventListener('keydown', handleGlobalCopy);
  }, [buildSelectedClipboardPayload, copyCurrentTableToClipboard, copyRichToClipboard, isSelectAllServerRef, loadedRows, selectedCells, serverSide]);

  return {
    handleCellMouseDown,
    handleCellMouseEnter,
    handleFillMouseDown,
    handleFillMouseUp,
    handleKeyDown,
    selectRange,
  };
}

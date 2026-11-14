import { useCallback, useEffect } from 'react';
import { captureCellAnchor, resolveAnchorRow, resolveAnchorColumn } from './cellSelectionAnchor';
import { isEditableKeyboardEvent } from '../hooks/usePivotNormalization';
import { getDisplayOrderedLeafColumns, isSelectableDisplayColumn } from './displayColumnOrder';

/**
 * Owns cell-oriented selection: keyboard navigation, range dragging, fill,
 * and clipboard fallback. Row selection remains a separate domain because it
 * has different selection semantics and event ownership.
  @param {any} p0 @returns {any} */
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
  selectionScopeKey = '',
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
  /** @type {(...args: any[]) => any} */
  const captureAnchor = useCallback((rowIndex, colIndex) => captureCellAnchor(
    getDisplayRows(), getDisplayOrderedLeafColumns(table), rowIndex, colIndex, selectionScopeKey
  ), [getDisplayRows, table, selectionScopeKey]);

  /** @type {(...args: any[]) => any} */
  const selectRange = useCallback((start, end) => {
    /** @type {any} */
    const visibleRows = getDisplayRows();
    /** @type {any} */
    const visibleCols = getDisplayOrderedLeafColumns(table);
    if (start.scopeKey !== undefined && start.scopeKey !== selectionScopeKey) start = end;
    /** @type {any} */
    const startRow = resolveAnchorRow(start, visibleRows);
    /** @type {any} */
    const endRow = resolveAnchorRow(end, visibleRows);
    /** @type {any} */
    const startCol = resolveAnchorColumn(start, visibleCols);
    /** @type {any} */
    const endCol = resolveAnchorColumn(end, visibleCols);
    if (startCol < 0 || endCol < 0) return {};
    /** @type {any} */
    const cStart = Math.min(startCol, endCol);
    /** @type {any} */
    const cEnd = Math.max(startCol, endCol);
    /** @type {Record<string, any>} */
    const newSelection = {};
    /** @param {any} [rowId] @param {any} [row] @returns {any} */
    const addRow = (rowId, row) => {
      for (let c = cStart; c <= cEnd; c++) {
        const col = visibleCols[c];
        if (isSelectableDisplayColumn(col)) newSelection[`${rowId}:${col.id}`] = row?.getValue(col.id);
      }
    };
    if (startRow >= 0 && endRow >= 0) {
      for (let r = Math.min(startRow, endRow); r <= Math.max(startRow, endRow); r++) {
        const row = visibleRows[r];
        if (row) addRow(row.id, row);
      }
    } else {
      // Preserve missing endpoint IDs so clipboard delivery refuses the range.
      // Iterate only loaded rows, even when endpoints are trillions apart.
      /** @type {any} */
      const endPosition = end.virtualIndex ?? visibleRows[endRow]?.original?.__virtualIndex;
      if (Number.isFinite(start.virtualIndex) && Number.isFinite(endPosition)) {
        /** @type {any} */
        const low = Math.min(start.virtualIndex, endPosition);
        /** @type {any} */
        const high = Math.max(start.virtualIndex, endPosition);
        visibleRows.forEach(/** @param {any} [row] @returns {any} */ row => {
          const position = row.original?.__virtualIndex;
          if (Number.isFinite(position) && position >= low && position <= high) addRow(row.id, row);
        });
      }
      for (const [anchor, index] of [[start, startRow], [end, endRow]]) {
        const row = visibleRows[index];
        if (row) addRow(row.id, row);
        else if (anchor.rowId !== undefined) addRow(anchor.rowId, null);
      }
    }
    return newSelection;
  }, [getDisplayRows, table, selectionScopeKey]);

  useEffect(() => {
    setSelectedCells({});
    setLastSelected(null);
    setDragStart(null);
    setIsDragging(false);
    setIsFilling(false);
    setFillRange(null);
    isSelectAllServerRef.current = false;
  }, [selectionScopeKey, setSelectedCells, setLastSelected, setDragStart, setIsDragging, setIsFilling, setFillRange, isSelectAllServerRef]);

  /** @type {(...args: any[]) => any} */
  const handleKeyDown = useCallback(e => {
    // Preserve native editing shortcuts whenever the event originated in a
    // text control.
    if (isEditableKeyboardEvent(e)) return;
    if ((e.ctrlKey || e.metaKey) && !e.altKey && e.key) {
      /** @type {any} */
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
    /** @type {any} */
    const displayColumns = getDisplayOrderedLeafColumns(tableRef.current);
    /** @type {any} */
    const visibleLeafColumnsAll = displayColumns.filter(isSelectableDisplayColumn);
    /** @type {any} */
    const visibleRowsAll = getDisplayRows();

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
      e.preventDefault();
      /** @type {Record<string, any>} */
      const allSelection = {};
      visibleRowsAll.forEach(/** @param {any} [row] @returns {any} */ row => {
        visibleLeafColumnsAll.forEach(/** @param {any} [col] @returns {any} */ col => {
          allSelection[`${row.id}:${col.id}`] = row.getValue(col.id);
        });
      });
      setSelectedCells(allSelection);
      isSelectAllServerRef.current = serverSide;
      if (visibleRowsAll.length > 0 && visibleLeafColumnsAll.length > 0) {
        /** @type {any} */
        const firstDisplayIndex = displayColumns.indexOf(visibleLeafColumnsAll[0]);
        setLastSelected(captureAnchor(0, firstDisplayIndex));
        setDragStart(captureAnchor(0, firstDisplayIndex));
      }
      return;
    }

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') {
      /** @type {any} */
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
      /** @type {any} */
      const payload = buildSelectedClipboardPayload(false, selectedCells);
      if (payload) copyRichToClipboard(payload);
      return;
    }
    if (!lastSelected) return;
    if (lastSelected.scopeKey !== undefined && lastSelected.scopeKey !== selectionScopeKey) return;
    /** @type {any} */
    const rowIndex = resolveAnchorRow(lastSelected, visibleRowsAll);
    if (rowIndex < 0) return;
    // Navigate the data columns, then map back onto the rendered positional
    // axis so gutters never shift mouse, scroll, and keyboard anchors apart.
    /** @type {any} */
    const colIndex = visibleLeafColumnsAll.indexOf(displayColumns[resolveAnchorColumn(lastSelected, displayColumns)]);
    if (colIndex < 0) return;
    let nextRow = rowIndex;
    let nextCol = colIndex;
    const visibleLeafColumns = visibleLeafColumnsAll;

    /** @param {any} [dir] @returns {any} */
    const ctrlArrowRow = dir => {
      /** @type {any} */
      const colId = visibleLeafColumns[colIndex] && visibleLeafColumns[colIndex].id;
      if (!colId) return dir > 0 ? visibleRowsAll.length - 1 : 0;
      /** @param {any} [row] @returns {any} */
      const isEmpty = row => {
        /** @type {any} */
        const value = row && row.getValue(colId);
        return value === null || value === undefined || value === '';
      };
      /** @type {any} */
      const curEmpty = isEmpty(visibleRowsAll[rowIndex]);
      if (curEmpty) {
        for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
          if (!isEmpty(visibleRowsAll[i])) return i;
        }
        return dir > 0 ? visibleRowsAll.length - 1 : 0;
      }
      /** @type {any} */
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
    /** @param {any} [dir] @returns {any} */
    const ctrlArrowCol = dir => {
      const row = visibleRowsAll[rowIndex];
      if (!row) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      /** @param {any} [col] @returns {any} */
      const isEmpty = col => {
        /** @type {any} */
        const value = row.getValue(col && col.id);
        return value === null || value === undefined || value === '';
      };
      /** @type {any} */
      const curEmpty = isEmpty(visibleLeafColumns[colIndex]);
      if (curEmpty) {
        for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
          if (!isEmpty(visibleLeafColumns[i])) return i;
        }
        return dir > 0 ? visibleLeafColumns.length - 1 : 0;
      }
      /** @type {any} */
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
      /** @type {any} */
      const nextDisplayCol = displayColumns.indexOf(nextColObj);
      const key = `${nextRowObj.id}:${nextColObj.id}`;
      /** @type {any} */
      const value = nextRowObj.getValue(nextColObj.id);
      if (e.shiftKey && e.key.startsWith('Arrow')) {
        setSelectedCells(selectRange(dragStart || lastSelected, captureAnchor(nextRow, nextDisplayCol)));
      } else {
        setSelectedCells({ [key]: value });
        setDragStart(captureAnchor(nextRow, nextDisplayCol));
      }
      setLastSelected(captureAnchor(nextRow, nextDisplayCol));
      scrollToDisplayRow(nextRow);
      scrollToDisplayColumn(nextDisplayCol);
    }
  }, [captureAnchor, selectionScopeKey, buildSelectedClipboardPayload, copyCurrentTableToClipboard, copyRichToClipboard, dragStart, getDisplayRows, handleRedo, handleUndo, isSelectAllServerRef, lastSelected, scrollToDisplayColumn, scrollToDisplayRow, selectedCells, selectRange, serverSide, setDragStart, setLastSelected, setSelectedCells, tableRef]);

  /** @type {(...args: any[]) => any} */
  const handleCellMouseDown = useCallback((event, rowIndex, colIndex, rowId, colId, value) => {
    if (!isSelectableDisplayColumn({ id: colId })) return;
    if (event.button === 2) return;
    if (event.shiftKey) {
      isSelectAllServerRef.current = false;
      event.preventDefault();
      /** @type {any} */
      const end = captureAnchor(rowIndex, colIndex);
      /** @type {any} */
      const start = lastSelected || end;
      /** @type {any} */
      const newSelection = selectRange(start, end);
      if (event.ctrlKey || event.metaKey) {
        setSelectedCells(/** @param {any} [previous] @returns {any} */ previous => ({ ...previous, ...newSelection }));
      } else {
        setSelectedCells(newSelection);
      }
      return;
    }
    isSelectAllServerRef.current = false;
    setIsDragging(true);
    setDragStart(captureAnchor(rowIndex, colIndex));
    setLastSelected(captureAnchor(rowIndex, colIndex));
    /** @type {any} */
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
  }, [captureAnchor, getDisplayRows, isSelectAllServerRef, lastSelected, selectRange, selectedCells, setDragStart, setHoveredRowPath, setIsDragging, setLastSelected, setSelectedCells, tableRef]);

  /** @type {(...args: any[]) => any} */
  const handleCellMouseEnter = useCallback((rowIndex, colIndex) => {
    if (isDragging && dragStart) {
      setSelectedCells(selectRange(dragStart, captureAnchor(rowIndex, colIndex)));
    }
    if (isFilling && dragStart) {
      setFillRange({
        rStart: Math.min(dragStart.rowIndex, rowIndex),
        rEnd: Math.max(dragStart.rowIndex, rowIndex),
        cStart: Math.min(dragStart.colIndex, colIndex),
        cEnd: Math.max(dragStart.colIndex, colIndex),
      });
    }
  }, [captureAnchor, dragStart, isDragging, isFilling, selectRange, setFillRange, setSelectedCells]);

  /** @type {(...args: any[]) => any} */
  const handleFillMouseDown = useCallback(event => {
    event.stopPropagation();
    event.preventDefault();
    setIsFilling(true);
  }, [setIsFilling]);

  /** @type {(...args: any[]) => any} */
  const handleFillMouseUp = useCallback(() => {
    if (isFilling && fillRange && setProps && dragStart
      && (dragStart.scopeKey === undefined || dragStart.scopeKey === selectionScopeKey)) {
      /** @type {any} */
      const displayRows = getDisplayRows();
      const startRow = displayRows[dragStart.rowIndex];
      /** @type {any} */
      const startValue = startRow ? startRow.getVisibleCells()[dragStart.colIndex].getValue() : undefined;
      /** @type {any[]} */
      const updates = [];
      /** @type {any} */
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
  }, [selectionScopeKey, dispatchBatchUpdateRequest, dragStart, fillRange, getDisplayRows, isFilling, setFillRange, setIsFilling, setProps, table]);

  useEffect(() => {
    /** @returns {any} */
    const handleMouseUp = () => {
      setIsDragging(false);
      setDragStart(null);
      if (isFilling) handleFillMouseUp();
    };
    window.addEventListener('mouseup', handleMouseUp);
    return () => window.removeEventListener('mouseup', handleMouseUp);
  }, [dragStart, fillRange, handleFillMouseUp, isDragging, isFilling, setDragStart, setIsDragging]);

  useEffect(() => {
    /** @param {any} [e] @returns {any} */
    const handleGlobalCopy = e => {
      if (e.defaultPrevented || isEditableKeyboardEvent(e)) return;
      if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
        /** @type {any} */
        const keys = Object.keys(selectedCells);
        if (keys.length === 0) return;
        e.preventDefault();
        if (isSelectAllServerRef.current && serverSide) {
          copyCurrentTableToClipboard(true, false);
          return;
        }
        /** @type {any} */
        const payload = buildSelectedClipboardPayload(false, selectedCells);
        if (payload) copyRichToClipboard(payload);
      }
    };
    window.addEventListener('keydown', handleGlobalCopy);
    return () => window.removeEventListener('keydown', handleGlobalCopy);
  }, [buildSelectedClipboardPayload, copyCurrentTableToClipboard, copyRichToClipboard, isSelectAllServerRef, loadedRows, selectedCells, serverSide]);

  return {
    captureAnchor,
    handleCellMouseDown,
    handleCellMouseEnter,
    handleFillMouseDown,
    handleFillMouseUp,
    handleKeyDown,
    selectRange,
  };
}

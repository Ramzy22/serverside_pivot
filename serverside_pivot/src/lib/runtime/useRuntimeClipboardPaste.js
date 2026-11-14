import { useEffect } from 'react';

import { isEditableKeyboardEvent } from '../hooks/usePivotNormalization';

/** Owns spreadsheet-style paste from the window clipboard into selected cells.  @param {any} p0 @returns {any} */
export function useRuntimeClipboardPaste({
  dispatchBatchUpdateRequest,
  getDisplayRows,
  lastSelected,
  tableRef,
}) {
  useEffect(() => {
    /** @param {any} [e] @returns {any} */
    const handlePaste = e => {
      // Text controls own their clipboard behavior. Without this guard a
      // previously selected grid cell causes the window-level spreadsheet
      // paste handler to swallow paste in field search and other inputs.
      if (isEditableKeyboardEvent(e)) return;
      if (!lastSelected) return;
      e.preventDefault();
      /** @type {any} */
      const clipboardData = e.clipboardData.getData('text');
      /** @type {any} */
      const rows = clipboardData
        .split(/\r\n|\n/)
        .filter(/** @param {any} [row] @param {any} [index] @param {any} [sourceRows] @returns {any} */ (row, index, sourceRows) => row.length > 0 || index < sourceRows.length - 1)
        .map(/** @param {any} [row] @returns {any} */ row => row.split('\t'));
      if (lastSelected.rowIndex === undefined || lastSelected.colIndex === undefined) return;
      /** @type {any} */
      const visibleLeafColumns = tableRef.current && tableRef.current.getVisibleLeafColumns ? tableRef.current.getVisibleLeafColumns() : [];
      /** @type {any} */
      const visibleRows = getDisplayRows();
      /** @type {any[]} */
      const updates = [];
      rows.forEach(/** @param {any} [rowValues] @param {any} [rowOffset] @returns {any} */ (rowValues, rowOffset) => {
        rowValues.forEach(/** @param {any} [rawValue] @param {any} [colOffset] @returns {any} */ (rawValue, colOffset) => {
          const targetRow = visibleRows[lastSelected.rowIndex + rowOffset];
          const targetCol = visibleLeafColumns[lastSelected.colIndex + colOffset];
          if (!targetRow || !targetCol) return;
          /** @type {any} */
          const currentValue = targetRow.getValue(targetCol.id);
          let nextValue = rawValue;
          if (typeof currentValue === 'number' && rawValue !== '') {
            /** @type {any} */
            const numericValue = Number(rawValue);
            if (!Number.isNaN(numericValue)) nextValue = numericValue;
          }
          updates.push({ rowId: targetRow.id, colId: targetCol.id, value: nextValue });
        });
      });
      dispatchBatchUpdateRequest(updates, 'paste');
    };
    window.addEventListener('paste', handlePaste);
    return () => window.removeEventListener('paste', handlePaste);
  }, [dispatchBatchUpdateRequest, getDisplayRows, lastSelected, tableRef]);
}

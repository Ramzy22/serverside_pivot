import { useEffect } from 'react';

import { isEditableKeyboardTarget } from '../hooks/usePivotNormalization';

/** Owns spreadsheet-style paste from the window clipboard into selected cells. */
export function useRuntimeClipboardPaste({
  dispatchBatchUpdateRequest,
  getDisplayRows,
  lastSelected,
  tableRef,
}) {
  useEffect(() => {
    const handlePaste = e => {
      // Text controls own their clipboard behavior. Without this guard a
      // previously selected grid cell causes the window-level spreadsheet
      // paste handler to swallow paste in field search and other inputs.
      if (isEditableKeyboardTarget(e.target)) return;
      if (!lastSelected) return;
      e.preventDefault();
      const clipboardData = e.clipboardData.getData('text');
      const rows = clipboardData
        .split(/\r\n|\n/)
        .filter((row, index, sourceRows) => row.length > 0 || index < sourceRows.length - 1)
        .map(row => row.split('\t'));
      if (lastSelected.rowIndex === undefined || lastSelected.colIndex === undefined) return;
      const visibleLeafColumns = tableRef.current && tableRef.current.getVisibleLeafColumns ? tableRef.current.getVisibleLeafColumns() : [];
      const visibleRows = getDisplayRows();
      const updates = [];
      rows.forEach((rowValues, rowOffset) => {
        rowValues.forEach((rawValue, colOffset) => {
          const targetRow = visibleRows[lastSelected.rowIndex + rowOffset];
          const targetCol = visibleLeafColumns[lastSelected.colIndex + colOffset];
          if (!targetRow || !targetCol) return;
          const currentValue = targetRow.getValue(targetCol.id);
          let nextValue = rawValue;
          if (typeof currentValue === 'number' && rawValue !== '') {
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

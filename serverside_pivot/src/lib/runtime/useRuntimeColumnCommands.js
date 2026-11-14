import { useCallback } from 'react';

import { formatDisplayLabel } from '../utils/helpers';

/** Owns pin-state lookups and server-side pivot-column ordering commands.  @param {any} p0 @returns {any} */
export function useRuntimeColumnCommands({
  colFields,
  columnPinning,
  columnVirtualizerRef,
  effectiveSortOptions,
  parentRef,
  resetVisibleColRange,
  rowFields,
  serverSide,
  setPivotColumnSorting,
  showNotification,
  visibleColRange,
}) {
  /** @type {(...args: any[]) => any} */
  const getPinningState = useCallback(columnId => {
    const { left, right } = columnPinning;
    if ((left || []).includes(columnId)) return 'left';
    if ((right || []).includes(columnId)) return 'right';
    return false;
  }, [columnPinning]);

  /** @type {(...args: any[]) => any} */
  const getPivotColumnSortField = useCallback((header, level = 0) => {
    if (!serverSide || !Array.isArray(colFields) || colFields.length === 0 || !header || !header.column) return null;
    const columnId = header.column.id;
    if (!columnId || columnId === 'hierarchy' || columnId === '__row_number__' || columnId === '__report_config__' || columnId.startsWith('__RowTotal__')) return null;
    if (rowFields.includes(columnId)) return null;
    /** @type {any} */
    const isGroupHeader = header.column.columns && header.column.columns.length > 0;
    return isGroupHeader ? level >= 0 && level < colFields.length ? colFields[level] : null : colFields[colFields.length - 1] || null;
  }, [colFields, rowFields, serverSide]);

  /** @type {(...args: any[]) => any} */
  const getPivotColumnSortState = useCallback(field => {
    if (!field) return null;
    /** @type {any} */
    const fieldOptions = effectiveSortOptions && effectiveSortOptions.columnOptions && typeof effectiveSortOptions.columnOptions === 'object' ? effectiveSortOptions.columnOptions[field] : null;
    /** @type {any} */
    const direction = fieldOptions && fieldOptions.pivotDirection;
    /** @type {any} */
    const mode = fieldOptions && fieldOptions.pivotSortMode;
    if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) return { mode, direction };
    return direction === 'asc' || direction === 'desc' ? { mode: 'label', direction } : null;
  }, [effectiveSortOptions]);

  /** @type {(...args: any[]) => any} */
  const applyPivotColumnSort = useCallback((field, mode, direction) => {
    if (!field) return;
    setPivotColumnSorting(/** @param {any} [previous] @returns {any} */ previous => {
      /** @type {Record<string, any>} */
      const next = { ...(previous || {}) };
      if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) {
        next[field] = { ...(next[field] || {}), pivotSortMode: mode, pivotDirection: direction };
      } else {
        delete next[field];
      }
      return next;
    });
    if (parentRef.current) parentRef.current.scrollLeft = 0;
    if (columnVirtualizerRef.current && typeof columnVirtualizerRef.current.scrollToIndex === 'function') columnVirtualizerRef.current.scrollToIndex(0);
    /** @type {any} */
    let visibleCount = Math.max(1, visibleColRange.end - visibleColRange.start + 1);
    if (columnVirtualizerRef.current && typeof columnVirtualizerRef.current.getVirtualItems === 'function') {
      /** @type {any} */
      const virtualItems = columnVirtualizerRef.current.getVirtualItems() || [];
      if (virtualItems.length > 0) visibleCount = Math.max(visibleCount, virtualItems.length);
    }
    resetVisibleColRange(visibleCount);
    /** @type {any} */
    const fieldLabel = formatDisplayLabel(field);
    /** @type {any} */
    const message = mode === 'label' && direction === 'asc' ? `Header order for ${fieldLabel} set to A-Z.`
      : mode === 'label' && direction === 'desc' ? `Header order for ${fieldLabel} set to Z-A.`
        : mode === 'total' && direction === 'desc' ? `Header order for ${fieldLabel} set to largest totals first.`
          : mode === 'total' && direction === 'asc' ? `Header order for ${fieldLabel} set to smallest totals first.`
            : `Header order cleared for ${fieldLabel}.`;
    /** @type {any} */
    const isApplied = (direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total');
    showNotification(message, isApplied ? 'success' : 'info');
  }, [columnVirtualizerRef, parentRef, resetVisibleColRange, setPivotColumnSorting, showNotification, visibleColRange]);

  return { applyPivotColumnSort, getPinningState, getPivotColumnSortField, getPivotColumnSortState };
}

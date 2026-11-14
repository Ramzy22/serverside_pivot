import { useCallback } from 'react';

import { normalizeColumnOrderValue } from '../utils/columnOrder';
import { cloneSerializable } from '../hooks/usePivotNormalization';
import { normalizeSparklineValConfigsForView } from '../utils/sparklines';

/** @param {any} [value] @returns {any} */
const normalizeLayoutModeValue = value => {
  /** @type {any} */
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'outline' || normalized === 'tabular' ? normalized : 'hierarchy';
};

/**
 * Owns layout snapshot capture and every mutation that must be undoable as a
 * single layout-history operation. It is intentionally independent of the
 * mutable legacy environment.
  @param {any} p0 @returns {any} */
export function useRuntimeLayoutHistory({
  layoutHistorySnapshot,
  layoutHistorySnapshotRef,
  normalizedInitialValConfigs,
  pendingLayoutHistoryCaptureRef,
  pendingLayoutHistoryCaptureTimeoutRef,
  pushUnifiedHistoryEntry,
  setColExpanded,
  setColFields,
  setColumnOrder,
  setPivotLayoutOrder,
  setPivotPinnedLayoutIds,
  setColumnPinning,
  setExpanded,
  setFilters,
  setGrandTotalPinOverride,
  setLayoutMode,
  setRowFields,
  setRowPinning,
  setShowColTotals,
  setShowRowTotals,
  setShowSubtotals,
  setSorting,
  setValConfigs,
}) {
  /** @type {(...args: any[]) => any} */
  const clearPendingLayoutHistoryCapture = useCallback((token = null) => {
    const pendingCapture = pendingLayoutHistoryCaptureRef.current;
    if (!pendingCapture || token && pendingCapture.token !== token) return;
    pendingLayoutHistoryCaptureRef.current = null;
    if (pendingLayoutHistoryCaptureTimeoutRef.current !== null) {
      clearTimeout(pendingLayoutHistoryCaptureTimeoutRef.current);
      pendingLayoutHistoryCaptureTimeoutRef.current = null;
    }
  }, [pendingLayoutHistoryCaptureRef, pendingLayoutHistoryCaptureTimeoutRef]);

  /** @type {(...args: any[]) => any} */
  const requestLayoutHistoryCapture = useCallback((source = 'layout') => {
    /** @type {any} */
    const beforeSnapshot = layoutHistorySnapshotRef.current || layoutHistorySnapshot || {};
    const token = `${source}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;
    pendingLayoutHistoryCaptureRef.current = {
      token,
      source,
      beforeSnapshot,
      beforeKey: JSON.stringify(beforeSnapshot || {}),
    };
    if (pendingLayoutHistoryCaptureTimeoutRef.current !== null) clearTimeout(pendingLayoutHistoryCaptureTimeoutRef.current);
    pendingLayoutHistoryCaptureTimeoutRef.current = setTimeout(() => clearPendingLayoutHistoryCapture(token), 500);
    return token;
  }, [clearPendingLayoutHistoryCapture, layoutHistorySnapshot, layoutHistorySnapshotRef, pendingLayoutHistoryCaptureRef, pendingLayoutHistoryCaptureTimeoutRef]);

  /** @type {(...args: any[]) => any} */
  const setRowFieldsWithHistory = useCallback((updater, source = 'layout:rows') => {
    requestLayoutHistoryCapture(source);
    setRowFields(updater);
  }, [requestLayoutHistoryCapture, setRowFields]);

  /** @type {(...args: any[]) => any} */
  const setColFieldsWithHistory = useCallback((updater, source = 'layout:columns') => {
    requestLayoutHistoryCapture(source);
    setColFields(updater);
  }, [requestLayoutHistoryCapture, setColFields]);

  /** @type {(...args: any[]) => any} */
  const setValConfigsWithHistory = useCallback((updater, source = 'layout:values') => {
    requestLayoutHistoryCapture(source);
    setValConfigs(/** @param {any} [previousConfigs] @returns {any} */ previousConfigs => normalizeSparklineValConfigsForView(typeof updater === 'function' ? updater(previousConfigs) : updater, normalizedInitialValConfigs));
  }, [normalizedInitialValConfigs, requestLayoutHistoryCapture, setValConfigs]);

  /** @type {(...args: any[]) => any} */
  const pushImmediateLayoutHistoryEntry = useCallback((sliceKey, updater, source) => {
    /** @type {any} */
    const beforeSnapshot = cloneSerializable(layoutHistorySnapshotRef.current || layoutHistorySnapshot || {}, null) || {};
    const previousSlice = beforeSnapshot[sliceKey];
    /** @type {any} */
    const nextSlice = typeof updater === 'function' ? updater(previousSlice) : updater;
    /** @type {any} */
    const afterSnapshot = cloneSerializable({ ...beforeSnapshot, [sliceKey]: nextSlice }, null) || {};
    /** @type {any} */
    const beforeKey = JSON.stringify(beforeSnapshot || {});
    /** @type {any} */
    const afterKey = JSON.stringify(afterSnapshot || {});
    clearPendingLayoutHistoryCapture();
    layoutHistorySnapshotRef.current = afterSnapshot;
    if (beforeKey !== afterKey) {
      pushUnifiedHistoryEntry({ kind: 'layout', source, before: beforeSnapshot, after: afterSnapshot, createdAt: Date.now() });
    }
    return nextSlice;
  }, [clearPendingLayoutHistoryCapture, layoutHistorySnapshot, layoutHistorySnapshotRef, pushUnifiedHistoryEntry]);

  /** @type {(...args: any[]) => any} */
  const setExpandedWithHistory = useCallback((updater, source = 'layout:expanded') => {
    setExpanded(pushImmediateLayoutHistoryEntry('expanded', updater, source));
  }, [pushImmediateLayoutHistoryEntry, setExpanded]);

  /** @type {(...args: any[]) => any} */
  const setColExpandedWithHistory = useCallback((updater, source = 'layout:column-groups') => {
    setColExpanded(pushImmediateLayoutHistoryEntry('colExpanded', updater, source));
  }, [pushImmediateLayoutHistoryEntry, setColExpanded]);

  /** @type {(...args: any[]) => any} */
  const toggleCol = useCallback((key, source = 'layout:column-groups') => {
    setColExpandedWithHistory(/** @param {any} [previousExpanded] @returns {any} */ previousExpanded => ({
      ...previousExpanded,
      [key]: previousExpanded[key] === undefined ? false : !previousExpanded[key],
    }), source);
  }, [setColExpandedWithHistory]);

  /** @type {(...args: any[]) => any} */
  const setColumnPinningWithHistory = useCallback((updater, source = 'layout:column-pinning') => {
    requestLayoutHistoryCapture(source);
    setColumnPinning(updater);
  }, [requestLayoutHistoryCapture, setColumnPinning]);

  /** @type {(...args: any[]) => any} */
  const setColumnOrderWithHistory = useCallback((updater, source = 'layout:column-order') => {
    requestLayoutHistoryCapture(source);
    setColumnOrder(/** @param {any} [previousOrder] @returns {any} */ previousOrder => normalizeColumnOrderValue(typeof updater === 'function' ? updater(previousOrder) : updater));
  }, [requestLayoutHistoryCapture, setColumnOrder]);

  /** @type {(...args: any[]) => any} */
  const setRowPinningWithHistory = useCallback((updater, source = 'layout:row-pinning') => {
    requestLayoutHistoryCapture(source);
    setRowPinning(updater);
  }, [requestLayoutHistoryCapture, setRowPinning]);

  /** @type {(...args: any[]) => any} */
  const applyLayoutHistorySnapshot = useCallback(snapshot => {
    if (!snapshot || typeof snapshot !== 'object') return;
    setRowFields(Array.isArray(snapshot.rowFields) ? snapshot.rowFields : []);
    setColFields(Array.isArray(snapshot.colFields) ? snapshot.colFields : []);
    setValConfigs(normalizeSparklineValConfigsForView(Array.isArray(snapshot.valConfigs) ? snapshot.valConfigs : [], normalizedInitialValConfigs));
    setFilters(snapshot.filters && typeof snapshot.filters === 'object' ? snapshot.filters : {});
    setSorting(Array.isArray(snapshot.sorting) ? snapshot.sorting : []);
    setExpanded(snapshot.expanded && typeof snapshot.expanded === 'object' ? snapshot.expanded : {});
    setColExpanded(snapshot.colExpanded && typeof snapshot.colExpanded === 'object' ? snapshot.colExpanded : {});
    setColumnPinning(snapshot.columnPinning && typeof snapshot.columnPinning === 'object' ? snapshot.columnPinning : { left: ['hierarchy'], right: [] });
    setColumnOrder(Array.isArray(snapshot.columnOrder) ? normalizeColumnOrderValue(snapshot.columnOrder) : []);
    setPivotLayoutOrder(Array.isArray(snapshot.pivotLayoutOrder) ? normalizeColumnOrderValue(snapshot.pivotLayoutOrder) : []);
    setPivotPinnedLayoutIds(snapshot.pivotPinnedLayoutIds && typeof snapshot.pivotPinnedLayoutIds === 'object' ? snapshot.pivotPinnedLayoutIds : {});
    setRowPinning(snapshot.rowPinning && typeof snapshot.rowPinning === 'object' ? snapshot.rowPinning : { top: [], bottom: [] });
    setShowRowTotals(Boolean(snapshot.showRowTotals));
    setShowColTotals(Boolean(snapshot.showColTotals));
    setShowSubtotals(Object.prototype.hasOwnProperty.call(snapshot, 'showSubtotals') ? Boolean(snapshot.showSubtotals) : true);
    setLayoutMode(normalizeLayoutModeValue(snapshot.layoutMode));
    setGrandTotalPinOverride(snapshot.grandTotalPinOverride === 'top' || snapshot.grandTotalPinOverride === 'bottom' ? snapshot.grandTotalPinOverride : false);
  }, [normalizedInitialValConfigs, setColExpanded, setColFields, setColumnOrder, setPivotLayoutOrder, setPivotPinnedLayoutIds, setColumnPinning, setExpanded, setFilters, setGrandTotalPinOverride, setLayoutMode, setRowFields, setRowPinning, setShowColTotals, setShowRowTotals, setShowSubtotals, setSorting, setValConfigs]);

  return {
    applyLayoutHistorySnapshot,
    clearPendingLayoutHistoryCapture,
    pushImmediateLayoutHistoryEntry,
    requestLayoutHistoryCapture,
    setColExpandedWithHistory,
    setColFieldsWithHistory,
    setColumnOrderWithHistory,
    setColumnPinningWithHistory,
    setExpandedWithHistory,
    setRowFieldsWithHistory,
    setRowPinningWithHistory,
    setValConfigsWithHistory,
    toggleCol,
  };
}

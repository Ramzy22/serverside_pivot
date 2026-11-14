import { useCallback, useEffect, useMemo } from 'react';

/** Owns row-edit synchronization and the persisted comparison/marker state.  @param {any} p0 @returns {any} */
export function useRuntimeEditComparisonState({
  comparisonCellCountsRef,
  comparisonCellEpoch,
  comparisonCellOriginalValuesRef,
  comparisonValueState,
  editedCellEpoch,
  editedCellMarkersRef,
  getOptimisticCellKey,
  normalizeTrackedEventIds,
  rowEditSessions,
  rowEditSessionsRef,
  setComparisonCellEpoch,
  setComparisonValueState,
  setEditedCellEpoch,
  setOptimisticCellEpoch,
  setRowEditSessions,
}) {
  useEffect(() => {
    rowEditSessionsRef.current = rowEditSessions;
  }, [rowEditSessions, rowEditSessionsRef]);

  /** @type {(...args: any[]) => any} */
  const updateRowEditSessions = useCallback(updater => {
    let nextSnapshot = rowEditSessionsRef.current;
    setRowEditSessions(/** @param {any} [previousSessions] @returns {any} */ previousSessions => {
      /** @type {any} */
      const resolvedSessions = typeof updater === 'function' ? updater(previousSessions) : updater;
      nextSnapshot = resolvedSessions && typeof resolvedSessions === 'object' ? resolvedSessions : {};
      rowEditSessionsRef.current = nextSnapshot;
      return nextSnapshot;
    });
    return nextSnapshot;
  }, [rowEditSessionsRef, setRowEditSessions]);

  /** @type {(...args: any[]) => any} */
  const syncComparisonValueState = useCallback(() => {
    setComparisonValueState({
      activeCount: comparisonCellCountsRef.current.size
    });
  }, [comparisonCellCountsRef, setComparisonValueState]);

  /** @type {(...args: any[]) => any} */
  const bumpEditedCellEpoch = useCallback(() => {
    setEditedCellEpoch(/** @param {any} [previousEpoch] @returns {any} */ previousEpoch => previousEpoch + 1);
  }, [setEditedCellEpoch]);

  /** @type {(...args: any[]) => any} */
  const bumpOptimisticCellEpoch = useCallback(() => {
    setOptimisticCellEpoch(/** @param {any} [previousEpoch] @returns {any} */ previousEpoch => previousEpoch + 1);
  }, [setOptimisticCellEpoch]);

  /** @type {(...args: any[]) => any} */
  const bumpComparisonCellEpoch = useCallback(() => {
    setComparisonCellEpoch(/** @param {any} [previousEpoch] @returns {any} */ previousEpoch => previousEpoch + 1);
  }, [setComparisonCellEpoch]);

  /** @type {(...args: any[]) => any} */
  const serializeEditComparisonState = useCallback(() => {
    /** @type {any[]} */
    const cells = [];
    comparisonCellOriginalValuesRef.current.forEach(/** @param {any} [entry] @param {any} [key] @returns {any} */ (entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      /** @type {any} */
      const count = comparisonCellCountsRef.current.get(key) || 0;
      if (count <= 0) return;
      /** @type {any} */
      const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
      /** @type {any} */
      const colId = entry.colId === null || entry.colId === undefined ? '' : String(entry.colId);
      if (!rowId || !colId) return;
      cells.push({
        rowId,
        colId,
        originalValue: entry.value,
        comparisonCount: count,
        directEventIds: normalizeTrackedEventIds(entry.directEventIds),
        propagatedEventIds: normalizeTrackedEventIds(entry.propagatedEventIds)
      });
    });
    /** @type {any[]} */
    const markers = [];
    editedCellMarkersRef.current.forEach(/** @param {any} [entry] @param {any} [key] @returns {any} */ (entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      /** @type {any} */
      const direct = (entry.directCount || 0) > 0;
      /** @type {any} */
      const propagated = (entry.propagatedCount || 0) > 0;
      if (!direct && !propagated) return;
      markers.push({
        key: String(key),
        direct,
        propagated,
        directCount: Math.max(0, Number(entry.directCount) || 0),
        propagatedCount: Math.max(0, Number(entry.propagatedCount) || 0)
      });
    });
    if (cells.length === 0 && markers.length === 0) {
      return null;
    }
    cells.sort((left, right) => `${left.rowId}:::${left.colId}`.localeCompare(`${right.rowId}:::${right.colId}`));
    markers.sort((left, right) => left.key.localeCompare(right.key));
    return {
      version: 1,
      cells,
      markers
    };
  }, [comparisonCellCountsRef, comparisonCellOriginalValuesRef, editedCellMarkersRef, normalizeTrackedEventIds]);

  /** @type {(...args: any[]) => any} */
  const restoreSerializedEditComparisonState = useCallback(serializedState => {
    /** @type {any} */
    const hadComparisonEntries = comparisonCellCountsRef.current.size > 0 || comparisonCellOriginalValuesRef.current.size > 0;
    /** @type {any} */
    const hadEditedMarkers = editedCellMarkersRef.current.size > 0;
    comparisonCellCountsRef.current.clear();
    comparisonCellOriginalValuesRef.current.clear();
    editedCellMarkersRef.current.clear();
    let hasComparisonEntries = false;
    let hasEditedMarkers = false;
    /** @type {any} */
    const restored = serializedState && typeof serializedState === 'object' ? serializedState : null;
    (Array.isArray(restored && restored.cells) ? restored.cells : []).forEach(/** @param {any} [cell] @returns {any} */ cell => {
      if (!cell || typeof cell !== 'object') return;
      /** @type {any} */
      const key = getOptimisticCellKey(cell.rowId, cell.colId);
      if (!key) return;
      comparisonCellCountsRef.current.set(key, 1);
      comparisonCellOriginalValuesRef.current.set(key, {
        rowId: String(cell.rowId),
        colId: String(cell.colId),
        value: Object.prototype.hasOwnProperty.call(cell, 'originalValue') ? cell.originalValue : undefined,
        directEventIds: normalizeTrackedEventIds(cell.directEventIds),
        propagatedEventIds: normalizeTrackedEventIds(cell.propagatedEventIds)
      });
      /** @type {any} */
      const restoredCount = Math.max(1, Number(cell.comparisonCount) || 0, normalizeTrackedEventIds(cell.directEventIds).length + normalizeTrackedEventIds(cell.propagatedEventIds).length);
      comparisonCellCountsRef.current.set(key, restoredCount);
      hasComparisonEntries = true;
    });
    (Array.isArray(restored && restored.markers) ? restored.markers : []).forEach(/** @param {any} [marker] @returns {any} */ marker => {
      if (!marker || typeof marker !== 'object') return;
      /** @type {any} */
      const key = typeof marker.key === 'string' ? marker.key : getOptimisticCellKey(marker.rowId, marker.colId);
      if (!key) return;
      /** @type {Record<string, any>} */
      const nextEntry = {
        directCount: Math.max(0, Number(marker.directCount) || 0 || (marker.direct ? 1 : 0)),
        propagatedCount: Math.max(0, Number(marker.propagatedCount) || 0 || (marker.propagated ? 1 : 0))
      };
      if (nextEntry.directCount <= 0 && nextEntry.propagatedCount <= 0) return;
      editedCellMarkersRef.current.set(key, nextEntry);
      hasEditedMarkers = true;
    });
    syncComparisonValueState();
    if (hadComparisonEntries || hasComparisonEntries) {
      bumpComparisonCellEpoch();
    }
    if (hadEditedMarkers || hasEditedMarkers) {
      bumpEditedCellEpoch();
    }
  }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, comparisonCellCountsRef, comparisonCellOriginalValuesRef, editedCellMarkersRef, getOptimisticCellKey, normalizeTrackedEventIds, syncComparisonValueState]);

  /** @type {any} */
  const serializedEditComparisonState = useMemo(
    () => serializeEditComparisonState(),
    [comparisonCellEpoch, comparisonValueState.activeCount, editedCellEpoch, serializeEditComparisonState]
  );

  return {
    bumpComparisonCellEpoch,
    bumpEditedCellEpoch,
    bumpOptimisticCellEpoch,
    restoreSerializedEditComparisonState,
    serializeEditComparisonState,
    serializedEditComparisonState,
    syncComparisonValueState,
    updateRowEditSessions,
  };
}

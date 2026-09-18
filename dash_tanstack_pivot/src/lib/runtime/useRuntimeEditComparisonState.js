import { useCallback, useEffect, useMemo } from 'react';

/** Owns row-edit synchronization and the persisted comparison/marker state. */
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

  const updateRowEditSessions = useCallback(updater => {
    let nextSnapshot = rowEditSessionsRef.current;
    setRowEditSessions(previousSessions => {
      const resolvedSessions = typeof updater === 'function' ? updater(previousSessions) : updater;
      nextSnapshot = resolvedSessions && typeof resolvedSessions === 'object' ? resolvedSessions : {};
      rowEditSessionsRef.current = nextSnapshot;
      return nextSnapshot;
    });
    return nextSnapshot;
  }, [rowEditSessionsRef, setRowEditSessions]);

  const syncComparisonValueState = useCallback(() => {
    setComparisonValueState({
      activeCount: comparisonCellCountsRef.current.size
    });
  }, [comparisonCellCountsRef, setComparisonValueState]);

  const bumpEditedCellEpoch = useCallback(() => {
    setEditedCellEpoch(previousEpoch => previousEpoch + 1);
  }, [setEditedCellEpoch]);

  const bumpOptimisticCellEpoch = useCallback(() => {
    setOptimisticCellEpoch(previousEpoch => previousEpoch + 1);
  }, [setOptimisticCellEpoch]);

  const bumpComparisonCellEpoch = useCallback(() => {
    setComparisonCellEpoch(previousEpoch => previousEpoch + 1);
  }, [setComparisonCellEpoch]);

  const serializeEditComparisonState = useCallback(() => {
    const cells = [];
    comparisonCellOriginalValuesRef.current.forEach((entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      const count = comparisonCellCountsRef.current.get(key) || 0;
      if (count <= 0) return;
      const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
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
    const markers = [];
    editedCellMarkersRef.current.forEach((entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      const direct = (entry.directCount || 0) > 0;
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

  const restoreSerializedEditComparisonState = useCallback(serializedState => {
    const hadComparisonEntries = comparisonCellCountsRef.current.size > 0 || comparisonCellOriginalValuesRef.current.size > 0;
    const hadEditedMarkers = editedCellMarkersRef.current.size > 0;
    comparisonCellCountsRef.current.clear();
    comparisonCellOriginalValuesRef.current.clear();
    editedCellMarkersRef.current.clear();
    let hasComparisonEntries = false;
    let hasEditedMarkers = false;
    const restored = serializedState && typeof serializedState === 'object' ? serializedState : null;
    (Array.isArray(restored && restored.cells) ? restored.cells : []).forEach(cell => {
      if (!cell || typeof cell !== 'object') return;
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
      const restoredCount = Math.max(1, Number(cell.comparisonCount) || 0, normalizeTrackedEventIds(cell.directEventIds).length + normalizeTrackedEventIds(cell.propagatedEventIds).length);
      comparisonCellCountsRef.current.set(key, restoredCount);
      hasComparisonEntries = true;
    });
    (Array.isArray(restored && restored.markers) ? restored.markers : []).forEach(marker => {
      if (!marker || typeof marker !== 'object') return;
      const key = typeof marker.key === 'string' ? marker.key : getOptimisticCellKey(marker.rowId, marker.colId);
      if (!key) return;
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

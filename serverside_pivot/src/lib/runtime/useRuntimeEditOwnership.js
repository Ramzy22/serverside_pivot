import { useCallback } from 'react';

/**
 * Owns comparison-cell event ownership and server overlay hydration. This is
 * separate from edit dispatch: it only describes which active events own a
 * cell and updates local overlay state from server responses.
  @param {any} p0 @returns {any} */
export function useRuntimeEditOwnership({
  bumpComparisonCellEpoch,
  bumpEditedCellEpoch,
  comparisonCellCountsRef,
  comparisonCellOriginalValuesRef,
  editedCellMarkersRef,
  getOptimisticCellKey,
  normalizeTrackedEventIds,
  resolveCurrentCellValue,
  splitOptimisticCellKey,
  syncComparisonValueState,
}) {
  /** @type {(...args: any[]) => any} */
  const ensureTrackedComparisonCellEntry = useCallback((key, fallback = null) => {
    if (!key) return null;
    /** @type {any} */
    const existingEntry = comparisonCellOriginalValuesRef.current.get(key);
    if (existingEntry && typeof existingEntry === 'object') return existingEntry;
    /** @type {any} */
    const derived = splitOptimisticCellKey(key);
    /** @type {any} */
    const rowId = fallback && fallback.rowId !== undefined ? String(fallback.rowId) : derived ? derived.rowId : '';
    /** @type {any} */
    const colId = fallback && fallback.colId !== undefined ? String(fallback.colId) : derived ? derived.colId : '';
    if (!rowId || !colId) return null;
    /** @type {Record<string, any>} */
    const createdEntry = {
      rowId,
      colId,
      value: fallback && Object.prototype.hasOwnProperty.call(fallback, 'value') ? fallback.value : resolveCurrentCellValue(rowId, colId),
      directEventIds: [],
      propagatedEventIds: [],
    };
    comparisonCellOriginalValuesRef.current.set(key, createdEntry);
    return createdEntry;
  }, [comparisonCellOriginalValuesRef, resolveCurrentCellValue, splitOptimisticCellKey]);

  /** @type {(...args: any[]) => any} */
  const appendTrackedEventOwnership = useCallback((eventId, affectedCells) => {
    /** @type {any} */
    const normalizedEventId = String(eventId || '').trim();
    if (!normalizedEventId || !affectedCells || typeof affectedCells !== 'object') return;
    let didChange = false;
    /** @param {any} [keys] @param {any} [fieldName] @returns {any} */
    const appendKeys = (keys, fieldName) => {
      (Array.isArray(keys) ? keys : []).forEach(key => {
        /** @type {any} */
        const entry = ensureTrackedComparisonCellEntry(key);
        if (!entry) return;
        /** @type {any} */
        const currentIds = normalizeTrackedEventIds(entry[fieldName]);
        if (currentIds.includes(normalizedEventId)) {
          entry[fieldName] = currentIds;
          return;
        }
        entry[fieldName] = [...currentIds, normalizedEventId];
        didChange = true;
      });
    };
    appendKeys(affectedCells.direct, 'directEventIds');
    appendKeys(affectedCells.propagated, 'propagatedEventIds');
    if (didChange) bumpComparisonCellEpoch();
  }, [bumpComparisonCellEpoch, ensureTrackedComparisonCellEntry, normalizeTrackedEventIds]);

  /** @type {(...args: any[]) => any} */
  const detachTrackedEventOwnership = useCallback((eventIds, options = {}) => {
    /** @type {any} */
    const normalizedEventIds = normalizeTrackedEventIds(eventIds);
    if (normalizedEventIds.length === 0) return;
    /** @type {any} */
    const eventIdSet = new Set(normalizedEventIds);
    /** @type {any} */
    const adjustCounts = Boolean(options.adjustCounts);
    let didChange = false;
    comparisonCellOriginalValuesRef.current.forEach(/** @param {any} [entry] @param {any} [key] @returns {any} */ (entry, key) => {
      if (!entry || typeof entry !== 'object') return;
      /** @type {any} */
      const directEventIds = normalizeTrackedEventIds(entry.directEventIds);
      /** @type {any} */
      const propagatedEventIds = normalizeTrackedEventIds(entry.propagatedEventIds);
      /** @type {any} */
      const nextDirectEventIds = directEventIds.filter(/** @param {any} [eventId] @returns {any} */ eventId => !eventIdSet.has(eventId));
      /** @type {any} */
      const nextPropagatedEventIds = propagatedEventIds.filter(/** @param {any} [eventId] @returns {any} */ eventId => !eventIdSet.has(eventId));
      /** @type {any} */
      const removedDirectCount = directEventIds.length - nextDirectEventIds.length;
      /** @type {any} */
      const removedPropagatedCount = propagatedEventIds.length - nextPropagatedEventIds.length;
      if (removedDirectCount <= 0 && removedPropagatedCount <= 0) return;
      entry.directEventIds = nextDirectEventIds;
      entry.propagatedEventIds = nextPropagatedEventIds;
      didChange = true;
      if (!adjustCounts) return;
      /** @type {any} */
      const existingComparisonCount = comparisonCellCountsRef.current.get(key) || 0;
      /** @type {any} */
      const nextComparisonCount = Math.max(0, existingComparisonCount - removedDirectCount - removedPropagatedCount);
      if (nextComparisonCount > 0) comparisonCellCountsRef.current.set(key, nextComparisonCount);
      else {
        comparisonCellCountsRef.current.delete(key);
        comparisonCellOriginalValuesRef.current.delete(key);
      }
      /** @type {any} */
      const existingMarker = editedCellMarkersRef.current.get(key) || { directCount: 0, propagatedCount: 0 };
      /** @type {Record<string, any>} */
      const nextMarker = {
        directCount: Math.max(0, (existingMarker.directCount || 0) - removedDirectCount),
        propagatedCount: Math.max(0, (existingMarker.propagatedCount || 0) - removedPropagatedCount),
      };
      if (nextMarker.directCount > 0 || nextMarker.propagatedCount > 0) editedCellMarkersRef.current.set(key, nextMarker);
      else editedCellMarkersRef.current.delete(key);
    });
    if (didChange) {
      if (adjustCounts) {
        syncComparisonValueState();
        bumpEditedCellEpoch();
      }
      bumpComparisonCellEpoch();
    }
  }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, comparisonCellCountsRef, comparisonCellOriginalValuesRef, editedCellMarkersRef, normalizeTrackedEventIds, syncComparisonValueState]);

  /** @type {(...args: any[]) => any} */
  const resolveTrackedCellEventIds = useCallback((rowId, colId, options = {}) => {
    /** @type {any} */
    const key = getOptimisticCellKey(rowId, colId);
    if (!key) return [];
    /** @type {any} */
    const entry = comparisonCellOriginalValuesRef.current.get(key);
    if (!entry || typeof entry !== 'object') return [];
    /** @type {any[]} */
    const eventIds = [];
    if (options.includeDirect !== false) eventIds.push(...normalizeTrackedEventIds(entry.directEventIds));
    if (options.includePropagated !== false) eventIds.push(...normalizeTrackedEventIds(entry.propagatedEventIds));
    return Array.from(new Set(eventIds));
  }, [comparisonCellOriginalValuesRef, getOptimisticCellKey, normalizeTrackedEventIds]);

  /** @type {(...args: any[]) => any} */
  const resolveCellEditOwnershipState = useCallback((rowId, colId) => {
    /** @type {any} */
    const directEventIds = resolveTrackedCellEventIds(rowId, colId, { includeDirect: true, includePropagated: false });
    /** @type {any} */
    const propagatedEventIds = resolveTrackedCellEventIds(rowId, colId, { includeDirect: false, includePropagated: true });
    if (directEventIds.length > 0 && propagatedEventIds.length > 0) {
      return { mode: 'blocked', targetEventIds: [], reason: 'This cell has overlapping active edit ownership. Revert the existing edit before changing it again.' };
    }
    if (propagatedEventIds.length > 0) {
      return { mode: 'blocked', targetEventIds: [], reason: 'This cell is locked by an active parent or child edit. Revert that edit before modifying this cell directly.' };
    }
    if (directEventIds.length > 1) {
      return { mode: 'blocked', targetEventIds: [], reason: 'This cell has multiple active direct edits. Revert the existing edits before changing it again.' };
    }
    if (directEventIds.length === 1) return { mode: 'replace', targetEventIds: directEventIds, reason: null };
    return { mode: 'apply', targetEventIds: [], reason: null };
  }, [resolveTrackedCellEventIds]);

  /** @type {(...args: any[]) => any} */
  const hydrateVisibleEditOverlay = useCallback(editOverlay => {
    /** @type {any} */
    const overlayCells = Array.isArray(editOverlay && editOverlay.cells) ? editOverlay.cells : [];
    if (overlayCells.length === 0) return;
    let didComparisonChange = false;
    let didMarkerChange = false;
    overlayCells.forEach(/** @param {any} [cell] @returns {any} */ cell => {
      if (!cell || typeof cell !== 'object') return;
      /** @type {any} */
      const key = getOptimisticCellKey(cell.rowId, cell.colId);
      if (!key) return;
      /** @type {any} */
      const directEventIds = normalizeTrackedEventIds(cell.directEventIds);
      /** @type {any} */
      const propagatedEventIds = normalizeTrackedEventIds(cell.propagatedEventIds);
      /** @type {any} */
      const existingEntry = comparisonCellOriginalValuesRef.current.get(key);
      /** @type {any} */
      const nextOriginalValue = Object.prototype.hasOwnProperty.call(cell, 'originalValue') ? cell.originalValue : existingEntry ? existingEntry.value : undefined;
      /** @type {Record<string, any>} */
      const nextEntry = { rowId: String(cell.rowId), colId: String(cell.colId), value: nextOriginalValue, directEventIds, propagatedEventIds };
      /** @type {any} */
      const didEntryChange = !existingEntry || existingEntry.value !== nextEntry.value
        || JSON.stringify(normalizeTrackedEventIds(existingEntry.directEventIds)) !== JSON.stringify(directEventIds)
        || JSON.stringify(normalizeTrackedEventIds(existingEntry.propagatedEventIds)) !== JSON.stringify(propagatedEventIds);
      if (didEntryChange) {
        comparisonCellOriginalValuesRef.current.set(key, nextEntry);
        didComparisonChange = true;
      }
      /** @type {any} */
      const nextComparisonCount = Math.max(1, Number(cell.comparisonCount) || 0, directEventIds.length + propagatedEventIds.length);
      if ((comparisonCellCountsRef.current.get(key) || 0) !== nextComparisonCount) {
        comparisonCellCountsRef.current.set(key, nextComparisonCount);
        didComparisonChange = true;
      }
      /** @type {Record<string, any>} */
      const nextMarkerEntry = { directCount: directEventIds.length, propagatedCount: propagatedEventIds.length };
      /** @type {any} */
      const existingMarkerEntry = editedCellMarkersRef.current.get(key);
      if (!existingMarkerEntry || (existingMarkerEntry.directCount || 0) !== nextMarkerEntry.directCount || (existingMarkerEntry.propagatedCount || 0) !== nextMarkerEntry.propagatedCount) {
        editedCellMarkersRef.current.set(key, nextMarkerEntry);
        didMarkerChange = true;
      }
    });
    if (didComparisonChange) {
      syncComparisonValueState();
      bumpComparisonCellEpoch();
    }
    if (didMarkerChange) bumpEditedCellEpoch();
  }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, comparisonCellCountsRef, comparisonCellOriginalValuesRef, editedCellMarkersRef, getOptimisticCellKey, normalizeTrackedEventIds, syncComparisonValueState]);

  return {
    appendTrackedEventOwnership,
    detachTrackedEventOwnership,
    ensureTrackedComparisonCellEntry,
    hydrateVisibleEditOverlay,
    resolveCellEditOwnershipState,
    resolveTrackedCellEventIds,
  };
}

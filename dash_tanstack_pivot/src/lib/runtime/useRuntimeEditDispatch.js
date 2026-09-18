import { useCallback } from 'react';

/**
 * Owns edit request preparation and dispatch. Row-editor presentation remains
 * in the interaction stage; this hook owns the client/server transaction
 * boundary, including aggregate-propagation and replacement ownership rules.
 */
export function useRuntimeEditDispatch({
  applyComparisonValuePlan,
  applyEditedCellMarkerPlan,
  buildComparisonValuePlan,
  buildEditedCellMarkerPlan,
  captureOptimisticCellValues,
  comparisonCellOriginalValuesRef,
  dispatchTransactionRequest,
  editValueDisplayMode,
  editingEnabled,
  findTransactionHistoryEntryByEventId,
  getOptimisticCellKey,
  isParentAggregatePropagationEdit,
  lastPropagationFormulaRef,
  pendingPropagationUpdatesRef,
  queueTransactionHistoryRequest,
  resolveCellEditOwnershipState,
  rowFields,
  serverSide,
  setEditPanelOpen,
  setEditValueDisplayMode,
  setPendingPropagationUpdates,
  setPropagationMethodUI,
  setPropsRef,
  showNotification,
  sortEventIdsByActiveHistory,
  supportsPatchTransactionRefresh,
}) {
  const resolvePropagationFormulaUpdates = useCallback((updates, source, meta) => {
    const normalizedUpdates = Array.isArray(updates) ? updates : [];
    const needsPropagation = normalizedUpdates.some(update => isParentAggregatePropagationEdit(update, rowFields));
    if (!needsPropagation) return normalizedUpdates;
    if (pendingPropagationUpdatesRef.current) return null;
    pendingPropagationUpdatesRef.current = {
      updates: normalizedUpdates,
      source: source || 'batch',
      meta: meta || null,
    };
    setPendingPropagationUpdates(normalizedUpdates);
    setPropagationMethodUI(lastPropagationFormulaRef.current || 'equal');
    setEditPanelOpen(true);
    return null;
  }, [isParentAggregatePropagationEdit, lastPropagationFormulaRef, pendingPropagationUpdatesRef, rowFields, setEditPanelOpen, setPendingPropagationUpdates, setPropagationMethodUI]);

  const analyzeServerEditOwnership = useCallback(updates => {
    const replacementEventIds = [];
    for (const update of Array.isArray(updates) ? updates : []) {
      if (!update || typeof update !== 'object') continue;
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      if (rowId === null || rowId === undefined || colId === null || colId === undefined) continue;
      const ownershipState = resolveCellEditOwnershipState(String(rowId), String(colId));
      if (ownershipState.mode === 'blocked') {
        return {
          mode: 'blocked',
          targetEventIds: [],
          reason: ownershipState.reason || 'This cell cannot be edited while an overlapping edit is active.',
        };
      }
      if (ownershipState.mode === 'replace') replacementEventIds.push(...ownershipState.targetEventIds);
    }
    const targetEventIds = sortEventIdsByActiveHistory(replacementEventIds);
    return { mode: targetEventIds.length > 0 ? 'replace' : 'apply', targetEventIds, reason: null };
  }, [resolveCellEditOwnershipState, sortEventIdsByActiveHistory]);

  const dispatchServerEditUpdates = useCallback((updates, source = 'batch', meta = null) => {
    const ownershipState = analyzeServerEditOwnership(updates);
    if (ownershipState.mode === 'blocked') {
      showNotification(ownershipState.reason || 'This cell cannot be edited while an overlapping edit is active.', 'warning');
      return null;
    }
    const isReplacement = ownershipState.mode === 'replace';
    const effectiveUpdates = isReplacement ? updates.map(update => {
      if (!update || typeof update !== 'object') return update;
      const rowId = update.rowPath || update.rowId;
      const colId = update.colId;
      const key = getOptimisticCellKey(rowId, colId);
      const originalEntry = key ? comparisonCellOriginalValuesRef.current.get(key) : null;
      return originalEntry && Object.prototype.hasOwnProperty.call(originalEntry, 'value')
        ? { ...update, oldValue: originalEntry.value }
        : update;
    }) : updates;
    const comparisonPlan = buildComparisonValuePlan(effectiveUpdates);
    if (!isReplacement && comparisonPlan) applyComparisonValuePlan(comparisonPlan, 'forward');
    const editedMarkerPlan = buildEditedCellMarkerPlan(effectiveUpdates);
    const dispatched = dispatchTransactionRequest(isReplacement ? {
      eventAction: 'replace', eventIds: ownershipState.targetEventIds, update: effectiveUpdates,
      refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
    } : {
      update: effectiveUpdates, refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
    }, source);
    if (!dispatched || !dispatched.requestId) {
      if (!isReplacement && comparisonPlan) applyComparisonValuePlan(comparisonPlan, 'backward');
      return null;
    }
    if (!isReplacement) {
      captureOptimisticCellValues(effectiveUpdates, dispatched.requestId);
      if (editedMarkerPlan) applyEditedCellMarkerPlan(editedMarkerPlan, 'forward');
      queueTransactionHistoryRequest(dispatched.requestId, {
        action: 'apply', source, editedMarkerPlan, comparisonPlan,
        rowEditRowId: meta && meta.rowEditRowId ? String(meta.rowEditRowId) : null,
        dirtyColumns: meta && Array.isArray(meta.dirtyColumns) ? meta.dirtyColumns : [],
      });
      return dispatched;
    }
    queueTransactionHistoryRequest(dispatched.requestId, {
      action: 'replace', source, targetEventIds: ownershipState.targetEventIds,
      historyEntries: ownershipState.targetEventIds.map(eventId => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
      comparisonPlan, editedMarkerPlan,
      rowEditRowId: meta && meta.rowEditRowId ? String(meta.rowEditRowId) : null,
      dirtyColumns: meta && Array.isArray(meta.dirtyColumns) ? meta.dirtyColumns : [],
    }, { blocksHistory: true });
    return dispatched;
  }, [analyzeServerEditOwnership, applyComparisonValuePlan, applyEditedCellMarkerPlan, buildComparisonValuePlan, buildEditedCellMarkerPlan, captureOptimisticCellValues, comparisonCellOriginalValuesRef, dispatchTransactionRequest, findTransactionHistoryEntryByEventId, getOptimisticCellKey, queueTransactionHistoryRequest, showNotification, supportsPatchTransactionRefresh]);

  const dispatchBatchUpdateRequest = useCallback((updates, source = 'batch', meta = null) => {
    if (!editingEnabled) return null;
    const normalizedUpdates = Array.isArray(updates) ? updates.filter(update => update && typeof update === 'object' && update.rowId && update.colId) : [];
    if (normalizedUpdates.length === 0 || !setPropsRef.current) return null;
    if (!serverSide) {
      if (editValueDisplayMode === 'original') setEditValueDisplayMode('edited');
      setPropsRef.current({ cellUpdates: normalizedUpdates });
      return { requestId: null };
    }
    const preparedUpdates = resolvePropagationFormulaUpdates(normalizedUpdates, source, meta);
    if (!preparedUpdates || preparedUpdates.length === 0) return null;
    if (editValueDisplayMode === 'original') setEditValueDisplayMode('edited');
    return dispatchServerEditUpdates(preparedUpdates, source, meta);
  }, [dispatchServerEditUpdates, editValueDisplayMode, editingEnabled, resolvePropagationFormulaUpdates, serverSide, setEditValueDisplayMode, setPropsRef]);

  const dispatchInlineCellEdit = useCallback(update => {
    if (!update || typeof update !== 'object') return;
    dispatchBatchUpdateRequest([update], 'inline-edit');
  }, [dispatchBatchUpdateRequest]);

  return { analyzeServerEditOwnership, dispatchBatchUpdateRequest, dispatchInlineCellEdit, dispatchServerEditUpdates, resolvePropagationFormulaUpdates };
}

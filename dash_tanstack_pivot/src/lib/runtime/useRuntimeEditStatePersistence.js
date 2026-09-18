import { useEffect, useRef } from 'react';

/**
 * Owns edit-comparison state persistence and rehydration. The state store is
 * intentionally explicit: this hook has no dependency on the legacy runtime
 * record and can be moved behind the editing domain unchanged.
 */
export function useRuntimeEditStatePersistence({
  editValueDisplayMode,
  externalEditState,
  id,
  loadPersistedState,
  persistence,
  propagationLogEpoch,
  propagationLogRef,
  restoreSerializedEditComparisonState,
  savePersistedState,
  serializedEditComparisonState,
  setEditValueDisplayMode,
  setPropagationLogEpoch,
  setProps,
  viewState,
}) {
  const didAttemptEditComparisonRestoreRef = useRef(false);
  const didRestorePersistedEditComparisonRef = useRef(false);
  const lastRestoredViewEditComparisonKeyRef = useRef(null);
  const lastEmittedEditStateKeyRef = useRef(null);
  const editStateSessionKey = id ? `__pivot_editState_${typeof id === 'string' ? id : JSON.stringify(id)}` : null;
  const didRestoreEditStateFromPropRef = useRef(false);

  useEffect(() => {
    if (!persistence || !didAttemptEditComparisonRestoreRef.current) return;
    savePersistedState('editComparisonState', serializedEditComparisonState);
    savePersistedState('editValueDisplayMode', editValueDisplayMode);
  }, [didAttemptEditComparisonRestoreRef, editValueDisplayMode, persistence, savePersistedState, serializedEditComparisonState]);

  useEffect(() => {
    if (didRestoreEditStateFromPropRef.current) return;
    didRestoreEditStateFromPropRef.current = true;
    let stateToRestore = externalEditState;
    if ((!stateToRestore || typeof stateToRestore !== 'object') && editStateSessionKey) {
      try {
        const stored = sessionStorage.getItem(editStateSessionKey);
        if (stored) stateToRestore = JSON.parse(stored);
      } catch (_ignored) {/* parse error or unavailable */}
    }
    if (!stateToRestore || typeof stateToRestore !== 'object') return;
    if (!stateToRestore.cells && !stateToRestore.markers) return;
    restoreSerializedEditComparisonState(stateToRestore);
    if (stateToRestore.displayMode === 'edited' || stateToRestore.displayMode === 'original') {
      setEditValueDisplayMode(stateToRestore.displayMode);
    }
    if (Array.isArray(stateToRestore.propagationLog) && stateToRestore.propagationLog.length > 0) {
      propagationLogRef.current = stateToRestore.propagationLog;
      setPropagationLogEpoch(previousEpoch => previousEpoch + 1);
    }
  }, [editStateSessionKey, externalEditState, propagationLogRef, restoreSerializedEditComparisonState, setEditValueDisplayMode, setPropagationLogEpoch]);

  useEffect(() => {
    if (!didRestoreEditStateFromPropRef.current) return;
    const propagationLog = propagationLogRef.current.length > 0 ? propagationLogRef.current : undefined;
    const statePayload = serializedEditComparisonState ? {
      ...serializedEditComparisonState,
      displayMode: editValueDisplayMode,
      propagationLog,
    } : propagationLog ? {
      version: 1,
      cells: [],
      markers: [],
      displayMode: editValueDisplayMode,
      propagationLog,
    } : null;
    const stateKey = statePayload ? JSON.stringify(statePayload) : '__empty__';
    if (stateKey === lastEmittedEditStateKeyRef.current) return;
    lastEmittedEditStateKeyRef.current = stateKey;
    if (typeof setProps === 'function') setProps({ editState: statePayload });
    if (editStateSessionKey) {
      try {
        if (statePayload) sessionStorage.setItem(editStateSessionKey, JSON.stringify(statePayload));
        else sessionStorage.removeItem(editStateSessionKey);
      } catch (_ignored) {/* storage full or unavailable */}
    }
  }, [editStateSessionKey, editValueDisplayMode, propagationLogEpoch, propagationLogRef, serializedEditComparisonState, setProps]);

  useEffect(() => {
    if (!viewState || typeof viewState !== 'object') return;
    const restored = viewState.state && typeof viewState.state === 'object' ? viewState.state : viewState;
    const serializedState = restored.editComparisonState && typeof restored.editComparisonState === 'object' ? restored.editComparisonState : null;
    const restoreKey = JSON.stringify(serializedState);
    if (lastRestoredViewEditComparisonKeyRef.current === restoreKey) return;
    lastRestoredViewEditComparisonKeyRef.current = restoreKey;
    restoreSerializedEditComparisonState(serializedState);
    didAttemptEditComparisonRestoreRef.current = true;
  }, [restoreSerializedEditComparisonState, viewState]);

  useEffect(() => {
    if (!persistence || didRestorePersistedEditComparisonRef.current) return;
    if (viewState && typeof viewState === 'object') return;
    didRestorePersistedEditComparisonRef.current = true;
    restoreSerializedEditComparisonState(loadPersistedState('editComparisonState', null));
    const persistedDisplayMode = loadPersistedState('editValueDisplayMode', 'original');
    setEditValueDisplayMode(persistedDisplayMode === 'edited' ? 'edited' : 'original');
    didAttemptEditComparisonRestoreRef.current = true;
  }, [loadPersistedState, persistence, restoreSerializedEditComparisonState, setEditValueDisplayMode, viewState]);

  useEffect(() => {
    if (viewState && typeof viewState === 'object') return;
    if (!persistence) {
      didAttemptEditComparisonRestoreRef.current = true;
      return;
    }
    if (didRestorePersistedEditComparisonRef.current) {
      didAttemptEditComparisonRestoreRef.current = true;
    }
  }, [persistence, viewState]);

  return {
    didAttemptEditComparisonRestoreRef,
    didRestorePersistedEditComparisonRef,
    didRestoreEditStateFromPropRef,
    editStateSessionKey,
    lastEmittedEditStateKeyRef,
    lastRestoredViewEditComparisonKeyRef,
  };
}

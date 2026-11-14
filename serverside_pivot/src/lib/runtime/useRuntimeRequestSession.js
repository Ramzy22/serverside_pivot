import { useEffect, useRef, useState } from 'react';

import { createClientInstanceId, getOrCreateSessionId } from '../hooks/usePivotNormalization';
import { getPivotProfiler, isPivotProfilingEnabled } from '../utils/pivotProfiler';

const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;

/**
 * Owns runtime identity, request generations, cache-facing refs, and edit
 * overlay state shared by transport, response, and editing domains.
  @param {any} p0 @returns {any} */
export function useRuntimeRequestSession({ dataVersion, id }) {
  /** @type {any} */
  const pivotId = id || 'pivot-grid';
  /** @type {any} */
  const sessionIdRef = useRef(getOrCreateSessionId(pivotId));
  /** @type {any} */
  const clientInstanceRef = useRef(createClientInstanceId(pivotId));
  /** @type {any} */
  const requestVersionRef = useRef(Number(dataVersion) || 0);
  /** @type {any} */
  const latestDataVersionRef = useRef(Number(dataVersion) || 0);
  /** @type {any} */
  const stateEpochRef = useRef(0);
  /** @type {any} */
  const abortGenerationRef = useRef(0);
  /** @type {any} */
  const structuralPendingVersionRef = useRef(null);
  /** @type {any} */
  const expandAllDebounceRef = useRef(false);
  /** @type {any} */
  const latestViewportRef = useRef({
    start: 0,
    end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1,
    count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
  });
  /** @type {[any, (...args: any[]) => any]} */
  const [stateEpoch, setStateEpoch] = useState(0);
  /** @type {[any, (...args: any[]) => any]} */
  const [abortGeneration, setAbortGeneration] = useState(0);
  /** @type {[any, (...args: any[]) => any]} */
  const [structuralInFlight, setStructuralInFlight] = useState(false);
  /** @type {[any, (...args: any[]) => any]} */
  const [runtimeDataError, setRuntimeDataError] = useState(null);
  /** @type {[any, (...args: any[]) => any]} */
  const [pendingRowTransitions, setPendingRowTransitions] = useState(() => new Map());
  /** @type {[any, (...args: any[]) => any]} */
  const [pendingColumnSkeletonCount, setPendingColumnSkeletonCount] = useState(0);
  /** @type {any} */
  const tableDataRef = useRef([]);
  /** @type {any} */
  const requestUrgentColumnViewportRef = useRef(null);
  /** @type {any} */
  const pivotProfilerRef = useRef(getPivotProfiler());
  /** @type {any} */
  const profilingEnabledRef = useRef(isPivotProfilingEnabled());
  /** @type {any} */
  const pendingDataProfilerCommitRef = useRef(null);
  /** @type {any} */
  const activeRuntimePayloadFetchRef = useRef(null);
  /** @type {any} */
  const optimisticCellValuesRef = useRef(new Map());
  /** @type {any} */
  const optimisticCellRequestsRef = useRef(new Map());
  /** @type {any} */
  const visiblePatchRowPathsRef = useRef([]);
  /** @type {any} */
  const visiblePatchCenterIdsRef = useRef([]);
  /** @type {any} */
  const deferredViewportRefreshTimeoutRef = useRef(null);
  /** @type {any} */
  const editedCellMarkersRef = useRef(new Map());
  /** @type {any} */
  const comparisonCellOriginalValuesRef = useRef(new Map());
  /** @type {any} */
  const comparisonCellCountsRef = useRef(new Map());
  /** @type {any} */
  const rowEditSessionsRef = useRef({});
  /** @type {any} */
  const editorOptionsLoadingRef = useRef({});
  /** @type {any} */
  const lastPropagationFormulaRef = useRef('equal');
  /** @type {any} */
  const pendingLayoutHistoryCaptureRef = useRef(null);
  /** @type {any} */
  const pendingLayoutHistoryCaptureTimeoutRef = useRef(null);
  /** @type {[any, (...args: any[]) => any]} */
  const [, setOptimisticCellEpoch] = useState(0);
  /** @type {[any, (...args: any[]) => any]} */
  const [editedCellEpoch, setEditedCellEpoch] = useState(0);
  /** @type {[any, (...args: any[]) => any]} */
  const [comparisonCellEpoch, setComparisonCellEpoch] = useState(0);
  /** @type {[any, (...args: any[]) => any]} */
  const [rowEditSessions, setRowEditSessions] = useState({});
  /** @type {[any, (...args: any[]) => any]} */
  const [editValueDisplayMode, setEditValueDisplayMode] = useState('original');
  /** @type {any} */
  const propagationLogRef = useRef([]);
  /** @type {[any, (...args: any[]) => any]} */
  const [propagationLogEpoch, setPropagationLogEpoch] = useState(0);
  /** @type {any} */
  const pendingPropagationUpdatesRef = useRef(null);
  /** @type {[any, (...args: any[]) => any]} */
  const [pendingPropagationUpdates, setPendingPropagationUpdates] = useState(null);
  /** @type {[any, (...args: any[]) => any]} */
  const [editPanelOpen, setEditPanelOpen] = useState(false);
  /** @type {[any, (...args: any[]) => any]} */
  const [propagationMethodUI, setPropagationMethodUI] = useState('equal');
  /** @type {[any, (...args: any[]) => any]} */
  const [comparisonValueState, setComparisonValueState] = useState({ activeCount: 0 });
  /** @type {any} */
  const profilerComponentIdRef = useRef(pivotId);
  profilerComponentIdRef.current = pivotId;

  useEffect(() => {
    /** @type {any} */
    const numericVersion = Number(dataVersion);
    if (Number.isFinite(numericVersion)) {
      latestDataVersionRef.current = Math.max(latestDataVersionRef.current, numericVersion);
    }
  }, [dataVersion, latestDataVersionRef]);

  return {
    abortGeneration,
    abortGenerationRef,
    activeRuntimePayloadFetchRef,
    clientInstanceRef,
    comparisonCellCountsRef,
    comparisonCellEpoch,
    comparisonCellOriginalValuesRef,
    comparisonValueState,
    deferredViewportRefreshTimeoutRef,
    editPanelOpen,
    editedCellEpoch,
    editedCellMarkersRef,
    editorOptionsLoadingRef,
    editValueDisplayMode,
    expandAllDebounceRef,
    lastPropagationFormulaRef,
    latestDataVersionRef,
    latestViewportRef,
    optimisticCellRequestsRef,
    optimisticCellValuesRef,
    pendingColumnSkeletonCount,
    pendingDataProfilerCommitRef,
    pendingLayoutHistoryCaptureRef,
    pendingLayoutHistoryCaptureTimeoutRef,
    pendingPropagationUpdates,
    pendingPropagationUpdatesRef,
    pendingRowTransitions,
    pivotProfilerRef,
    profilerComponentIdRef,
    profilingEnabledRef,
    propagationLogEpoch,
    propagationLogRef,
    propagationMethodUI,
    requestUrgentColumnViewportRef,
    requestVersionRef,
    rowEditSessions,
    rowEditSessionsRef,
    runtimeDataError,
    sessionIdRef,
    setAbortGeneration,
    setComparisonCellEpoch,
    setComparisonValueState,
    setEditPanelOpen,
    setEditedCellEpoch,
    setEditValueDisplayMode,
    setOptimisticCellEpoch,
    setPendingColumnSkeletonCount,
    setPendingPropagationUpdates,
    setPendingRowTransitions,
    setPropagationLogEpoch,
    setPropagationMethodUI,
    setRowEditSessions,
    setRuntimeDataError,
    setStateEpoch,
    setStructuralInFlight,
    stateEpoch,
    stateEpochRef,
    structuralInFlight,
    structuralPendingVersionRef,
    tableDataRef,
    visiblePatchCenterIdsRef,
    visiblePatchRowPathsRef,
  };
}

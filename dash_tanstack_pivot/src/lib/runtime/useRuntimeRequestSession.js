import { useEffect, useRef, useState } from 'react';

import { createClientInstanceId, getOrCreateSessionId } from '../hooks/usePivotNormalization';
import { getPivotProfiler, isPivotProfilingEnabled } from '../utils/pivotProfiler';

const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;

/**
 * Owns runtime identity, request generations, cache-facing refs, and edit
 * overlay state shared by transport, response, and editing domains.
 */
export function useRuntimeRequestSession({ dataVersion, id }) {
  const pivotId = id || 'pivot-grid';
  const sessionIdRef = useRef(getOrCreateSessionId(pivotId));
  const clientInstanceRef = useRef(createClientInstanceId(pivotId));
  const requestVersionRef = useRef(Number(dataVersion) || 0);
  const latestDataVersionRef = useRef(Number(dataVersion) || 0);
  const stateEpochRef = useRef(0);
  const abortGenerationRef = useRef(0);
  const structuralPendingVersionRef = useRef(null);
  const expandAllDebounceRef = useRef(false);
  const latestViewportRef = useRef({
    start: 0,
    end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1,
    count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
  });
  const [stateEpoch, setStateEpoch] = useState(0);
  const [abortGeneration, setAbortGeneration] = useState(0);
  const [structuralInFlight, setStructuralInFlight] = useState(false);
  const [pendingRowTransitions, setPendingRowTransitions] = useState(() => new Map());
  const [pendingColumnSkeletonCount, setPendingColumnSkeletonCount] = useState(0);
  const tableDataRef = useRef([]);
  const requestUrgentColumnViewportRef = useRef(null);
  const pivotProfilerRef = useRef(getPivotProfiler());
  const profilingEnabledRef = useRef(isPivotProfilingEnabled());
  const pendingDataProfilerCommitRef = useRef(null);
  const activeRuntimePayloadFetchRef = useRef(null);
  const optimisticCellValuesRef = useRef(new Map());
  const optimisticCellRequestsRef = useRef(new Map());
  const visiblePatchRowPathsRef = useRef([]);
  const visiblePatchCenterIdsRef = useRef([]);
  const deferredViewportRefreshTimeoutRef = useRef(null);
  const editedCellMarkersRef = useRef(new Map());
  const comparisonCellOriginalValuesRef = useRef(new Map());
  const comparisonCellCountsRef = useRef(new Map());
  const rowEditSessionsRef = useRef({});
  const editorOptionsLoadingRef = useRef({});
  const lastPropagationFormulaRef = useRef('equal');
  const pendingLayoutHistoryCaptureRef = useRef(null);
  const pendingLayoutHistoryCaptureTimeoutRef = useRef(null);
  const [, setOptimisticCellEpoch] = useState(0);
  const [editedCellEpoch, setEditedCellEpoch] = useState(0);
  const [comparisonCellEpoch, setComparisonCellEpoch] = useState(0);
  const [rowEditSessions, setRowEditSessions] = useState({});
  const [editValueDisplayMode, setEditValueDisplayMode] = useState('original');
  const propagationLogRef = useRef([]);
  const [propagationLogEpoch, setPropagationLogEpoch] = useState(0);
  const pendingPropagationUpdatesRef = useRef(null);
  const [pendingPropagationUpdates, setPendingPropagationUpdates] = useState(null);
  const [editPanelOpen, setEditPanelOpen] = useState(false);
  const [propagationMethodUI, setPropagationMethodUI] = useState('equal');
  const [comparisonValueState, setComparisonValueState] = useState({ activeCount: 0 });
  const profilerComponentIdRef = useRef(pivotId);
  profilerComponentIdRef.current = pivotId;

  useEffect(() => {
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

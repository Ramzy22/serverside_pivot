// @ts-nocheck -- composed leaf hooks are progressively typed at their owners.
// Command-stage composition. Focused sub-hooks own individual state groups.
import React, { useMemo, useRef, useEffect, useCallback } from 'react';
import { buildMeasureAxisRuntimeConfig } from '../utils/measureAxis';
import { pivotLayoutRequestFields } from '../utils/columnOrder';
import Icons from '../utils/Icons';
import { DEFAULT_CHART_PANEL_ROW_LIMIT, DEFAULT_CHART_PANEL_COLUMN_LIMIT, VALID_CHART_SERVER_SCOPES, MAX_TRANSACTION_HISTORY_ENTRIES, normalizeCustomDimensionsValue, cloneSerializable, normalizeTransactionRefreshModeValue, isParentAggregatePropagationEdit, normalizeTransactionHistoryPayload, cellValuesMatch, hasAppliedTransactionWork, shouldShowTransactionLoading, isEditableKeyboardEvent, getRequestedChartSeriesColumnIds } from '../hooks/usePivotNormalization';
const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;
const EXPAND_ALL_STATE_KEY = '__ALL__';
const debugLog = (...args) => {
  const buildDebugEnabled = process.env.NODE_ENV !== 'production';
  let runtimeDebugEnabled = false;
  if (typeof window !== 'undefined') {
    try {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true || window.localStorage.getItem('pivot-debug') === '1';
    } catch (error) {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true;
    }
  }
  if (!buildDebugEnabled && !runtimeDebugEnabled) return;
  console.log('[pivot-grid]', ...args);
};
import { resolveEditorOptionsSource, validateEditorValue } from '../utils/editing';
import { useDetailDrillThrough } from '../hooks/useDetailDrillThrough';
import { useRuntimeChartCanvasCommands } from '../runtime/useRuntimeChartCanvasCommands';
import { useRuntimeEditChangeTracking } from '../runtime/useRuntimeEditChangeTracking';
import { useRuntimeEditDispatch } from '../runtime/useRuntimeEditDispatch';
import { useRuntimeEditStatePersistence } from '../runtime/useRuntimeEditStatePersistence';
import { useRuntimeRowEditing } from '../runtime/useRuntimeRowEditing';
import { useRuntimeLayoutHistory } from '../runtime/useRuntimeLayoutHistory';
import { createRuntimeRowPinCommand } from '../runtime/createRuntimeRowPinCommand';
import { normalizeSparklineValConfigsForView } from '../utils/sparklines';
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartModal
})));
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartPanel
})));
const PivotChartFallback = () => null;
const PivotChartModal = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartModal {...props} />
    </React.Suspense>;
const PivotChartPanel = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartPanel {...props} />
    </React.Suspense>;
const loadingAnimationStyles = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@300;500&family=Plus+Jakarta+Sans:wght@300;500;800&display=swap');
@keyframes pivot-row-loader-enter {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes pivot-skeleton-shimmer {
    0% { background-position: 220% 0; }
    100% { background-position: -220% 0; }
}
@keyframes pivot-spinner-rotate {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
`;
const FILTER_OPTION_PAGE_SIZE = 250;
export function useCommandRuntimeStage(input, transportDomain) {
  // Never write into a predecessor domain. This render-pass assembly is local
  // to the command stage and is frozen before it crosses the domain boundary.
  const stage = { ...input };
  // Transport is an event-time dependency, not a fragment to merge into the
  // interaction state. This keeps request dispatch and its imperative ref
  // narrowly scoped while the interaction hooks retain their own state.
  const {
    emit: emitRuntimeRequest
  } = transportDomain.port;
  const { publish: dispatchSetProps } = transportDomain.host;
  const transportSetPropsRef = useRef(dispatchSetProps);
  useEffect(() => {
    transportSetPropsRef.current = dispatchSetProps;
  }, [dispatchSetProps]);
  const editStatePersistence = useRuntimeEditStatePersistence({
    editValueDisplayMode: stage.editValueDisplayMode,
    externalEditState: stage.externalEditState,
    id: stage.id,
    loadPersistedState: stage.loadPersistedState,
    persistence: stage.persistence,
    propagationLogEpoch: stage.propagationLogEpoch,
    propagationLogRef: stage.propagationLogRef,
    restoreSerializedEditComparisonState: stage.restoreSerializedEditComparisonState,
    savePersistedState: stage.savePersistedState,
    serializedEditComparisonState: stage.serializedEditComparisonState,
    setEditValueDisplayMode: stage.setEditValueDisplayMode,
    setPropagationLogEpoch: stage.setPropagationLogEpoch,
    setProps: stage.setProps,
    viewState: stage.viewState,
  });
  for (const [key, value] of Object.entries(editStatePersistence)) stage[key] = value;
  const editChangeTracking = useRuntimeEditChangeTracking({
    bumpComparisonCellEpoch: stage.bumpComparisonCellEpoch,
    bumpEditedCellEpoch: stage.bumpEditedCellEpoch,
    bumpOptimisticCellEpoch: stage.bumpOptimisticCellEpoch,
    cellValuesMatch,
    comparisonCellCountsRef: stage.comparisonCellCountsRef,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    editedCellMarkersRef: stage.editedCellMarkersRef,
    getOptimisticCellKey: stage.getOptimisticCellKey,
    optimisticCellValuesRef: stage.optimisticCellValuesRef,
    resolveAggregationConfigForColumnId: stage.resolveAggregationConfigForColumnId,
    resolveCurrentCellValue: stage.resolveCurrentCellValue,
    resolveVisibleDataRowId: stage.resolveVisibleDataRowId,
    syncComparisonValueState: stage.syncComparisonValueState,
    tableDataRef: stage.tableDataRef,
  });
  for (const [key, value] of Object.entries(editChangeTracking)) stage[key] = value;
  const layoutHistoryCommands = useRuntimeLayoutHistory({
    layoutHistorySnapshot: stage.layoutHistorySnapshot,
    layoutHistorySnapshotRef: stage.layoutHistorySnapshotRef,
    normalizedInitialValConfigs: stage.normalizedInitialValConfigs,
    pendingLayoutHistoryCaptureRef: stage.pendingLayoutHistoryCaptureRef,
    pendingLayoutHistoryCaptureTimeoutRef: stage.pendingLayoutHistoryCaptureTimeoutRef,
    pushUnifiedHistoryEntry: stage.pushUnifiedHistoryEntry,
    setColExpanded: stage.setColExpanded,
    setColFields: stage.setColFields,
    setColumnOrder: stage.setColumnOrder,
    setPivotLayoutOrder: stage.setPivotLayoutOrder,
    setPivotPinnedLayoutIds: stage.setPivotPinnedLayoutIds,
    setColumnPinning: stage.setColumnPinning,
    setExpanded: stage.setExpanded,
    setFilters: stage.setFilters,
    setGrandTotalPinOverride: stage.setGrandTotalPinOverride,
    setLayoutMode: stage.setLayoutMode,
    setRowFields: stage.setRowFields,
    setRowPinning: stage.setRowPinning,
    setShowColTotals: stage.setShowColTotals,
    setShowRowTotals: stage.setShowRowTotals,
    setShowSubtotals: stage.setShowSubtotals,
    setSorting: stage.setSorting,
    setValConfigs: stage.setValConfigs
  });
  for (const [key, value] of Object.entries(layoutHistoryCommands)) stage[key] = value;
  stage.didMountClearTransactionRef = useRef(false);
  stage.prevServerSideRef = useRef(stage.serverSide);
  stage.prevTableNameRef = useRef(stage.tableName);
  useEffect(() => {
    if (!stage.didMountClearTransactionRef.current) {
      stage.didMountClearTransactionRef.current = true;
      return;
    }
    if (stage.prevServerSideRef.current !== stage.serverSide || stage.prevTableNameRef.current !== stage.tableName) {
      stage.prevServerSideRef.current = stage.serverSide;
      stage.prevTableNameRef.current = stage.tableName;
      console.log('[CLEAR-TX] clearing due to serverSide/tableName change');
      stage.clearTransactionHistory();
    }
  }, [stage.clearTransactionHistory, stage.serverSide, stage.tableName]);
  useEffect(() => {
    if (stage.comparisonValueState.activeCount === 0 && stage.editValueDisplayMode !== 'original') {
      stage.setEditValueDisplayMode('original');
    }
  }, [stage.comparisonValueState.activeCount, stage.editValueDisplayMode]);
  useEffect(() => {
    stage.layoutHistorySnapshotRef.current = stage.layoutHistorySnapshot;
    const pendingCapture = stage.pendingLayoutHistoryCaptureRef.current;
    if (!pendingCapture) return;
    if (stage.layoutHistorySnapshotKey === pendingCapture.beforeKey) return;
    stage.clearPendingLayoutHistoryCapture(pendingCapture.token);
    stage.pushUnifiedHistoryEntry({
      kind: 'layout',
      source: pendingCapture.source || 'layout',
      before: pendingCapture.beforeSnapshot,
      after: stage.layoutHistorySnapshot,
      createdAt: Date.now()
    });
  }, [stage.clearPendingLayoutHistoryCapture, stage.layoutHistorySnapshot, stage.layoutHistorySnapshotKey, stage.pushUnifiedHistoryEntry]);
  useEffect(() => () => {
    if (stage.pendingLayoutHistoryCaptureTimeoutRef.current !== null) {
      clearTimeout(stage.pendingLayoutHistoryCaptureTimeoutRef.current);
      stage.pendingLayoutHistoryCaptureTimeoutRef.current = null;
    }
  }, []);
  stage.beginStructuralTransaction = useCallback(() => {
    stage.stateEpochRef.current += 1;
    stage.abortGenerationRef.current += 1;
    const baselineVersion = Math.max(stage.requestVersionRef.current, stage.latestDataVersionRef.current);
    const nextVersion = baselineVersion + 1;
    stage.requestVersionRef.current = nextVersion;
    stage.setStateEpoch(stage.stateEpochRef.current);
    stage.setAbortGeneration(stage.abortGenerationRef.current);
    stage.setStructuralInFlight(true);
    stage.structuralPendingVersionRef.current = {
      version: nextVersion,
      startDataVersion: stage.latestDataVersionRef.current
    };
    return {
      stateEpoch: stage.stateEpochRef.current,
      abortGeneration: stage.abortGenerationRef.current,
      version: nextVersion
    };
  }, []);
  stage.beginExpansionRequest = useCallback(() => {
    stage.abortGenerationRef.current += 1;
    const newVersion = stage.requestVersionRef.current + 1;
    stage.requestVersionRef.current = newVersion;
    stage.setAbortGeneration(stage.abortGenerationRef.current);
    return {
      abortGeneration: stage.abortGenerationRef.current,
      stateEpoch: stage.stateEpochRef.current,
      version: newVersion
    };
  }, []);
  stage.requestEditorOptions = useCallback((editorConfig, columnId, columnConfig = null) => {
    const source = resolveEditorOptionsSource(editorConfig, columnId, columnConfig);
    if (!source || !source.columnId) return;
    if (Object.prototype.hasOwnProperty.call(stage.filterOptions || {}, source.columnId)) return;
    if (stage.editorOptionsLoadingRef.current[source.columnId]) return;
    stage.setEditorOptionsLoadingState(previousState => ({
      ...previousState,
      [source.columnId]: true
    }));
    stage.emitEditLifecycleEvent({
      kind: 'editor_options_request',
      columnId,
      sourceColumnId: source.columnId,
      editor: editorConfig && editorConfig.editor ? editorConfig.editor : null
    });
    emitRuntimeRequest('filter_options', {
      columnId: source.columnId,
      nonce: Date.now()
    });
  }, [stage.emitEditLifecycleEvent, emitRuntimeRequest, stage.filterOptions]);
  ({
    detailSurface: stage.detailSurface,
    setDetailSurface: stage.setDetailSurface,
    toggleDetailForRowRef: stage.toggleDetailForRowRef,
    pendingDetailRequestRef: stage.pendingDetailRequestRef,
    activeDetailDisplayMode: stage.activeDetailDisplayMode,
    fetchDetailData: stage.fetchDetailData,
    handleCellDrillThrough: stage.handleCellDrillThrough,
    toggleDetailForRow: stage.toggleDetailForRow,
    handleDetailPageChange: stage.handleDetailPageChange,
    handleDetailSort: stage.handleDetailSort,
    handleDetailFilter: stage.handleDetailFilter,
    handleDetailClose: stage.handleDetailClose
  } = useDetailDrillThrough({
    detailConfig: stage.detailConfig,
    detailMode: stage.detailMode,
    viewMode: stage.viewMode,
    reportDef: stage.reportDef,
    treeConfig: stage.treeConfig,
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    valConfigs: stage.valConfigs,
    tableName: stage.tableName,
    runtimeResponse: stage.runtimeResponse,
    emitRuntimeRequest,
    stateEpoch: stage.stateEpoch,
    abortGeneration: stage.abortGeneration,
    sessionIdRef: stage.sessionIdRef,
    clientInstanceRef: stage.clientInstanceRef,
    requestVersionRef: stage.requestVersionRef,
    setPropsRef: transportSetPropsRef,
    profilingEnabledRef: stage.profilingEnabledRef,
    pivotProfilerRef: stage.pivotProfilerRef,
    profilerComponentIdRef: stage.profilerComponentIdRef
  }));
  stage.supportsPatchTransactionRefresh = useMemo(() => {
    if (!stage.serverSide || stage.viewMode !== 'pivot' || stage.detailMode !== 'none') return false;
    if (!Array.isArray(stage.rowFields) || stage.rowFields.length === 0) return false;
    return !(Array.isArray(stage.valConfigs) && stage.valConfigs.some(config => {
      if (!config || typeof config !== 'object') return false;
      const aggregation = String(config.agg || '').trim().toLowerCase();
      return aggregation === 'formula' || Boolean(config.windowFn);
    }));
  }, [stage.detailMode, stage.rowFields, stage.serverSide, stage.valConfigs, stage.viewMode]);
  stage.configureTransactions({
    maxHistoryEntries: MAX_TRANSACTION_HISTORY_ENTRIES,
    normalizeTrackedEventIds: stage.normalizeTrackedEventIds,
    normalizeTransactionHistoryPayload,
    normalizeTransactionRefreshModeValue,
    shouldShowTransactionLoading,
    hasAppliedTransactionWork,
    setPropsRef: transportSetPropsRef,
    serverSide: stage.serverSide,
    showNotification: stage.showNotification,
    visiblePatchRowPathsRef: stage.visiblePatchRowPathsRef,
    visiblePatchCenterIdsRef: stage.visiblePatchCenterIdsRef,
    colFields: stage.colFields,
    latestRequestedViewportRef: stage.latestRequestedViewportRef,
    latestViewportRef: stage.latestViewportRef,
    defaultServerSideBlockSize: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
    resolveStableRequestedColumnWindow: stage.resolveStableRequestedColumnWindow,
    beginExpansionRequest: stage.beginExpansionRequest,
    markRequestPending: stage.markRequestPending,
    emitRuntimeRequest,
    tableName: stage.tableName,
    sessionIdRef: stage.sessionIdRef,
    clientInstanceRef: stage.clientInstanceRef,
    serverSidePinsGrandTotal: stage.serverSidePinsGrandTotal,
    immersiveMode: stage.immersiveMode,
    appendTrackedEventOwnership: stage.appendTrackedEventOwnership,
    applyComparisonValuePlan: stage.applyComparisonValuePlan,
    applyEditedCellMarkerPlan: stage.applyEditedCellMarkerPlan,
    clearAllComparisonValueState: stage.clearAllComparisonValueState,
    clearAllEditedCellMarkers: stage.clearAllEditedCellMarkers,
    clearAllOptimisticCellValues: stage.clearAllOptimisticCellValues,
    clearOptimisticCellValuesForRequest: stage.clearOptimisticCellValuesForRequest,
    closeRowEditSession: stage.closeRowEditSession,
    detachTrackedEventOwnership: stage.detachTrackedEventOwnership,
    emitEditLifecycleEvent: stage.emitEditLifecycleEvent,
    releaseOptimisticCellRequest: stage.releaseOptimisticCellRequest,
    updateRowEditSessions: stage.updateRowEditSessions,
    clearPendingLayoutHistoryCapture: stage.clearPendingLayoutHistoryCapture,
    applyLayoutHistorySnapshot: stage.applyLayoutHistorySnapshot,
    supportsPatchTransactionRefresh: stage.supportsPatchTransactionRefresh,
    setEditValueDisplayMode: stage.setEditValueDisplayMode,
    comparisonValueState: stage.comparisonValueState,
    recordPropagationEntries: entries => {
      entries.forEach(entry => {
        if (!entry || typeof entry !== 'object') return;
        stage.propagationLogRef.current = [...stage.propagationLogRef.current.slice(-49), {
          ...entry,
          timestamp: Date.now()
        }];
      });
      stage.setPropagationLogEpoch(previousEpoch => previousEpoch + 1);
      stage.setEditPanelOpen(true);
    },
    clearPropagationLog: () => {
      stage.propagationLogRef.current = [];
      stage.setPropagationLogEpoch(previousEpoch => previousEpoch + 1);
    }
  });
  const editDispatch = useRuntimeEditDispatch({
    applyComparisonValuePlan: stage.applyComparisonValuePlan,
    applyEditedCellMarkerPlan: stage.applyEditedCellMarkerPlan,
    buildComparisonValuePlan: stage.buildComparisonValuePlan,
    buildEditedCellMarkerPlan: stage.buildEditedCellMarkerPlan,
    captureOptimisticCellValues: stage.captureOptimisticCellValues,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    dispatchTransactionRequest: stage.dispatchTransactionRequest,
    editValueDisplayMode: stage.editValueDisplayMode,
    editingEnabled: stage.editingEnabled,
    findTransactionHistoryEntryByEventId: stage.findTransactionHistoryEntryByEventId,
    getOptimisticCellKey: stage.getOptimisticCellKey,
    isParentAggregatePropagationEdit,
    lastPropagationFormulaRef: stage.lastPropagationFormulaRef,
    pendingPropagationUpdatesRef: stage.pendingPropagationUpdatesRef,
    queueTransactionHistoryRequest: stage.queueTransactionHistoryRequest,
    resolveCellEditOwnershipState: stage.resolveCellEditOwnershipState,
    rowFields: stage.rowFields,
    serverSide: stage.serverSide,
    setEditPanelOpen: stage.setEditPanelOpen,
    setEditValueDisplayMode: stage.setEditValueDisplayMode,
    setPendingPropagationUpdates: stage.setPendingPropagationUpdates,
    setPropagationMethodUI: stage.setPropagationMethodUI,
    setPropsRef: transportSetPropsRef,
    showNotification: stage.showNotification,
    sortEventIdsByActiveHistory: stage.sortEventIdsByActiveHistory,
    supportsPatchTransactionRefresh: stage.supportsPatchTransactionRefresh,
  });
  for (const [key, value] of Object.entries(editDispatch)) stage[key] = value;
  const rowEditing = useRuntimeRowEditing({
    Icons,
    cancelRowEditSession: stage.cancelRowEditSession,
    closeRowEditSession: stage.closeRowEditSession,
    dispatchBatchUpdateRequest: stage.dispatchBatchUpdateRequest,
    emitEditLifecycleEvent: stage.emitEditLifecycleEvent,
    getRowEditMeta: stage.getRowEditMeta,
    normalizedEditingConfig: stage.normalizedEditingConfig,
    rowEditSessionsRef: stage.rowEditSessionsRef,
    serverSide: stage.serverSide,
    theme: stage.theme,
    updateRowEditSessions: stage.updateRowEditSessions,
    validateEditorValue,
  });
  for (const [key, value] of Object.entries(rowEditing)) stage[key] = value;
  useEffect(() => {
    const handleGlobalEditShortcut = event => {
      if (event.defaultPrevented || isEditableKeyboardEvent(event)) return;
      if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
      const key = event.key.toLowerCase();
      if (key === 'z') {
        event.preventDefault();
        if (event.shiftKey) stage.handleRedo();else stage.handleUndo();
        return;
      }
      if (key === 'y') {
        event.preventDefault();
        stage.handleRedo();
      }
    };
    window.addEventListener('keydown', handleGlobalEditShortcut);
    return () => window.removeEventListener('keydown', handleGlobalEditShortcut);
  }, [stage.handleRedo, stage.handleUndo]);
  stage.buildRuntimeRequestStateOverride = useCallback((source = null) => {
    const snapshot = source && typeof source === 'object' ? source : {};
    const resolvedValConfigs = Object.prototype.hasOwnProperty.call(snapshot, 'valConfigs') ? snapshot.valConfigs : stage.valConfigs;
    const resolvedMeasureAxis = Object.prototype.hasOwnProperty.call(snapshot, 'measureAxis') ? snapshot.measureAxis : stage.measureAxis;
    return cloneSerializable({
      viewMode: Object.prototype.hasOwnProperty.call(snapshot, 'viewMode') ? snapshot.viewMode : stage.viewMode,
      detailMode: Object.prototype.hasOwnProperty.call(snapshot, 'detailMode') ? snapshot.detailMode : stage.detailMode,
      treeConfig: Object.prototype.hasOwnProperty.call(snapshot, 'treeConfig') ? snapshot.treeConfig : stage.treeConfig,
      detailConfig: Object.prototype.hasOwnProperty.call(snapshot, 'detailConfig') ? snapshot.detailConfig : stage.detailConfig,
      reportDef: Object.prototype.hasOwnProperty.call(snapshot, 'reportDef') ? snapshot.reportDef : stage.reportDef,
      customDimensions: Object.prototype.hasOwnProperty.call(snapshot, 'customDimensions') ? normalizeCustomDimensionsValue(snapshot.customDimensions) : normalizeCustomDimensionsValue(stage.customDimensions),
      measureAxis: buildMeasureAxisRuntimeConfig(resolvedMeasureAxis, resolvedValConfigs),
      rowFields: Object.prototype.hasOwnProperty.call(snapshot, 'rowFields') ? snapshot.rowFields : stage.rowFields,
      colFields: Object.prototype.hasOwnProperty.call(snapshot, 'colFields') ? snapshot.colFields : stage.colFields,
      valConfigs: normalizeSparklineValConfigsForView(resolvedValConfigs, stage.normalizedInitialValConfigs),
      filters: Object.prototype.hasOwnProperty.call(snapshot, 'filters') ? {
        ...(snapshot.filters || {})
      } : {
        ...(stage.filters || {})
      },
      sorting: Object.prototype.hasOwnProperty.call(snapshot, 'sorting') ? snapshot.sorting : stage.sorting,
      sortOptions: Object.prototype.hasOwnProperty.call(snapshot, 'sortOptions') ? snapshot.sortOptions : stage.effectiveSortOptions,
      expanded: Object.prototype.hasOwnProperty.call(snapshot, 'expanded') ? snapshot.expanded : stage.expanded,
      immersiveMode: Object.prototype.hasOwnProperty.call(snapshot, 'immersiveMode') ? snapshot.immersiveMode : stage.immersiveMode,
      showRowTotals: Object.prototype.hasOwnProperty.call(snapshot, 'showRowTotals') ? snapshot.showRowTotals : stage.showRowTotals,
      showColTotals: Object.prototype.hasOwnProperty.call(snapshot, 'showColTotals') ? snapshot.showColTotals : stage.showColTotals,
      showSubtotals: Object.prototype.hasOwnProperty.call(snapshot, 'showSubtotals') ? snapshot.showSubtotals : stage.showSubtotals
    }, null);
  }, [stage.viewMode, stage.detailMode, stage.treeConfig, stage.detailConfig, stage.reportDef, stage.customDimensions, stage.measureAxis, stage.rowFields, stage.colFields, stage.valConfigs, stage.normalizedInitialValConfigs, stage.filters, stage.sorting, stage.effectiveSortOptions, stage.expanded, stage.immersiveMode, stage.showRowTotals, stage.showColTotals, stage.showSubtotals]);
  stage.buildChartStateOverrideSnapshot = useCallback(() => stage.buildRuntimeRequestStateOverride(), [stage.buildRuntimeRequestStateOverride]);
  stage.dispatchServerSideRuntimeSetProps = useCallback(nextProps => {
    const runtimeRequest = nextProps && typeof nextProps === 'object' ? nextProps.runtimeRequest : null;
    const payload = runtimeRequest && typeof runtimeRequest === 'object' && runtimeRequest.payload && typeof runtimeRequest.payload === 'object' ? runtimeRequest.payload : null;
    const exportStart = runtimeRequest?.kind === 'export' && (!payload?.action || payload.action === 'start');
    const scopeChanged = stage.layoutScopeCacheKeyRef.current !== stage.serverSideCacheKey;
    const layoutFields = payload && (runtimeRequest.kind === 'data' || exportStart)
      ? pivotLayoutRequestFields(stage.pivotLayoutOrderRef.current, stage.layoutRevisionRef.current,
        stage.layoutAcknowledgedManifestRef.current,
        [...(stage.columnPinning?.left || []), ...(stage.columnPinning?.right || [])], stage.cachedColSchema,
        exportStart, stage.pivotPinnedLayoutIds, scopeChanged)
      : {};
    if (payload && runtimeRequest.kind === 'data') {
      stage.latestLayoutSentManifestRef.current = stage.pivotLayoutOrderRef.current;
      stage.layoutScopeCacheKeyRef.current = stage.serverSideCacheKey;
      if (scopeChanged) stage.layoutRevisionRef.current = null;
      // A newly sent manifest makes the previous revision/manifest pair
      // provisional. If the user returns to the old order before the server
      // acknowledges this request, that old order must still be sent.
      if (layoutFields.layoutManifest !== undefined) {
        stage.layoutAcknowledgedManifestRef.current = '__layout_pending__';
      }
    }
    if (payload && !Object.prototype.hasOwnProperty.call(payload, 'state_override') && !Object.prototype.hasOwnProperty.call(payload, 'stateOverride')) {
      dispatchSetProps({
        ...nextProps,
        runtimeRequest: {
          ...runtimeRequest,
          payload: {
            ...payload,
            ...layoutFields,
            state_override: stage.buildRuntimeRequestStateOverride() || undefined
          }
        }
      });
      return;
    }
    dispatchSetProps(payload ? {
      ...nextProps,
      runtimeRequest: { ...runtimeRequest, payload: { ...payload, ...layoutFields } }
    } : nextProps);
  }, [stage.buildRuntimeRequestStateOverride, stage.columnPinning, stage.cachedColSchema, stage.pivotPinnedLayoutIds, dispatchSetProps]);
  stage.buildChartRequestBase = useCallback((config, stateOverride = null) => {
    if (!stage.serverSide || stage.needsColSchema || stage.totalCenterCols === null) return null;
    const requestedRowLimit = Number(config && config.rowLimit);
    const requestedColumnLimit = Number(config && config.columnLimit);
    const requestedSeriesColumnIds = getRequestedChartSeriesColumnIds(config && config.chartType, config && config.chartLayers);
    const needsChartColumnCatalog = Boolean(config && (config.needsColumnCatalog || config.chartType === 'combo'));
    const serverScope = config && VALID_CHART_SERVER_SCOPES.has(config.serverScope) ? config.serverScope : stage.normalizedChartServerWindow.scope || 'viewport';
    const viewportSnapshot = stage.latestViewportRef.current || {
      start: 0,
      end: Math.max((Number.isFinite(requestedRowLimit) ? requestedRowLimit : DEFAULT_CHART_PANEL_ROW_LIMIT) - 1, 0),
      count: Math.max(Number.isFinite(requestedRowLimit) ? requestedRowLimit : DEFAULT_CHART_PANEL_ROW_LIMIT, 1)
    };
    const rowStart = serverScope === 'root' ? 0 : Math.max(0, Number.isFinite(viewportSnapshot.start) ? viewportSnapshot.start : 0);
    const viewportRowCount = Number.isFinite(viewportSnapshot.count) ? Math.max(1, viewportSnapshot.count) : Math.max(1, (Number(viewportSnapshot.end) || rowStart) - rowStart + 1);
    const resolvedRowLimit = Math.max(viewportRowCount, Math.max(1, Number.isFinite(requestedRowLimit) ? Math.floor(requestedRowLimit) : DEFAULT_CHART_PANEL_ROW_LIMIT), stage.normalizedChartServerWindow.enabled && stage.normalizedChartServerWindow.rows ? stage.normalizedChartServerWindow.rows : 0);
    const rowEnd = rowStart + resolvedRowLimit - 1;
    const maxCenterIndex = Math.max(stage.totalCenterCols - 1, 0);
    const baseColStart = serverScope === 'root' ? 0 : stage.colRequestStart !== null ? stage.colRequestStart : Math.max(0, Math.min(stage.visibleColRange.start || 0, maxCenterIndex));
    const viewportColumnCount = stage.colRequestStart !== null && stage.colRequestEnd !== null ? Math.max(1, stage.colRequestEnd - stage.colRequestStart + 1) : Math.max(1, Number.isFinite(requestedColumnLimit) ? Math.floor(requestedColumnLimit) : DEFAULT_CHART_PANEL_COLUMN_LIMIT);
    const resolvedColumnLimit = Math.max(viewportColumnCount, Math.max(1, Number.isFinite(requestedColumnLimit) ? Math.floor(requestedColumnLimit) : DEFAULT_CHART_PANEL_COLUMN_LIMIT), stage.normalizedChartServerWindow.enabled && stage.normalizedChartServerWindow.columns ? stage.normalizedChartServerWindow.columns : 0);
    const colStart = Math.max(0, Math.min(baseColStart, maxCenterIndex));
    const colEnd = Math.max(colStart, Math.min(maxCenterIndex, colStart + resolvedColumnLimit - 1));
    return {
      table: stage.tableName || undefined,
      start: rowStart,
      end: rowEnd,
      count: resolvedRowLimit,
      col_start: colStart,
      col_end: colEnd,
      include_grand_total: stage.showColTotals || undefined,
      immersive_mode: stage.immersiveMode || undefined,
      needs_col_schema: needsChartColumnCatalog || undefined,
      row_limit: resolvedRowLimit,
      column_limit: resolvedColumnLimit,
      series_column_ids: requestedSeriesColumnIds.length > 0 ? requestedSeriesColumnIds : undefined,
      state_override: stateOverride || undefined
    };
  }, [stage.serverSide, stage.needsColSchema, stage.totalCenterCols, stage.normalizedChartServerWindow, stage.colRequestStart, stage.colRequestEnd, stage.visibleColRange.start, stage.tableName, stage.showColTotals]);
  const chartCanvasCommands = useRuntimeChartCanvasCommands({
    chartCanvasPanes: stage.chartCanvasPanes,
    chartPanelAxisMode: stage.chartPanelAxisMode,
    chartPanelBarLayout: stage.chartPanelBarLayout,
    chartPanelColumnLimit: stage.chartPanelColumnLimit,
    chartPanelGraphHeight: stage.chartPanelGraphHeight,
    chartPanelHierarchyLevel: stage.chartPanelHierarchyLevel,
    chartPanelInteractionMode: stage.chartPanelInteractionMode,
    chartPanelLayers: stage.chartPanelLayers,
    chartPanelOrientation: stage.chartPanelOrientation,
    chartPanelRowLimit: stage.chartPanelRowLimit,
    chartPanelServerScope: stage.chartPanelServerScope,
    chartPanelSortMode: stage.chartPanelSortMode,
    chartPanelSource: stage.chartPanelSource,
    chartPanelTitle: stage.chartPanelTitle,
    chartPanelType: stage.chartPanelType,
    chartPanelWidth: stage.chartPanelWidth,
    completedChartRequestSignaturesRef: stage.completedChartRequestSignaturesRef,
    initialChartDefinition: stage.initialChartDefinition,
    previewChartPaneId: stage.previewChartPaneId,
    setChartCanvasPanes: stage.setChartCanvasPanes,
    setChartPaneDataById: stage.setChartPaneDataById,
    setPreviewChartPaneId: stage.setPreviewChartPaneId,
    setTableCanvasSize: stage.setTableCanvasSize,
  });
  for (const [key, value] of Object.entries(chartCanvasCommands)) stage[key] = value;
  useEffect(() => {
    const handleClick = () => stage.setContextMenu(null);
    document.addEventListener('click', handleClick);
    return () => document.removeEventListener('click', handleClick);
  }, []);
  const rowPinCommand = createRuntimeRowPinCommand({
    requestLayoutHistoryCapture: stage.requestLayoutHistoryCapture,
    setGrandTotalPinOverride: stage.setGrandTotalPinOverride,
    setProps: stage.setProps,
    setRowPinningWithHistory: stage.setRowPinningWithHistory
  });
  for (const [key, value] of Object.entries(rowPinCommand)) stage[key] = value;
  return Object.freeze(stage);
}

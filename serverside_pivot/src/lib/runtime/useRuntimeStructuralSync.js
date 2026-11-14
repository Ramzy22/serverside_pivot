import { useEffect, useRef } from 'react';

const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;
/** @type {Record<string, any>} */
const DEFAULT_INITIAL_SYNC_STATE = { layoutMode: 'hierarchy' };
/** @type {any} */
const UI_ONLY_KEYS = new Set(['detailMode', 'detailConfig', 'columnPinning', 'rowPinning', 'grandTotalPinOverride', 'columnVisibility', 'columnOrder', 'columnSizing']);
/** @type {any} */
const SCHEMA_SHAPE_CHANGE_KEYS = new Set(['viewMode', 'detailMode', 'treeConfig', 'detailConfig', 'reportDef', 'customDimensions', 'measureAxis', 'rowFields', 'colFields', 'valConfigs', 'showRowTotals', 'showColTotals', 'showSubtotals', 'groupAllowUnbalanced', 'unbalancedValues', 'showGrandTotal', 'layoutMode', 'executionMode', 'clientRuntimeActive', 'clientEngine']);

/**
 * Synchronizes structural grid state with Dash and starts the corresponding
 * server-side data request. The three request paths are explicit: local UI,
 * hierarchy expansion, and cache-invalidating structural changes.
  @param {any} p0 @returns {any} */
export function useRuntimeStructuralSync({
  actions: {
    beginExpansionRequest,
    beginStructuralTransaction,
    buildRuntimeRequestStateOverride,
    buildLayoutRequestFields,
    markRequestPending,
    resolveStableRequestedColumnWindow,
    setPendingColumnSkeletonCount,
  },
  refs: {
    abortGenerationRef,
    clientInstanceRef,
    expansionScrollRestoreRafRef,
    expansionScrollRestoreRef,
    latestDataVersionRef,
    latestRequestedViewportRef,
    latestViewportRef,
    needsColSchemaRef,
    parentRef,
    pendingExpansionRef,
    sessionIdRef,
    setPropsRef,
  },
  state: {
    clientEngine,
    clientRuntimeActive,
    colFields,
    columnOrder,
    columnPinning,
    columnSizing,
    columnVisibility,
    customDimensions,
    detailConfig,
    detailMode,
    effectiveSortOptions,
    executionMode,
    expanded,
    filters,
    grandTotalPinOverride,
    immersiveMode,
    layoutMode,
    reportDef,
    rowFields,
    rowPinning,
    runtimeMeasureAxis,
    serverSide,
    serverSideBlockSize,
    serverSidePinsGrandTotal,
    showColTotals,
    showRowTotals,
    showSubtotals,
    groupAllowUnbalanced,
    unbalancedValues,
    showGrandTotal,
    sorting,
    tableName,
    treeConfig,
    valConfigs,
    viewMode,
  },
  initialState,
}) {
  /** @type {Record<string, any>} */
  const initialSyncState = { ...DEFAULT_INITIAL_SYNC_STATE, ...initialState };
  /** @type {any} */
  const lastPropsRef = useRef(initialSyncState);
  useEffect(() => {
    /** @type {Record<string, any>} */
    const nextProps = {
      viewMode, detailMode, treeConfig, detailConfig, reportDef, customDimensions,
      measureAxis: runtimeMeasureAxis, rowFields, colFields, valConfigs, filters,
      sorting, sortOptions: effectiveSortOptions, expanded, immersiveMode,
      showRowTotals, showColTotals, showSubtotals, groupAllowUnbalanced, unbalancedValues, showGrandTotal, columnPinning, rowPinning,
      columnVisibility, columnOrder, columnSizing,
    };
    /** @type {Record<string, any>} */
    const nextSyncState = {
      ...nextProps,
      layoutMode,
      grandTotalPinOverride,
      serverSidePinsGrandTotal,
      executionMode,
      clientRuntimeActive,
      clientEngine,
    };
    /** @type {any} */
    const colFieldsChanged = JSON.stringify(nextProps.colFields) !== JSON.stringify(lastPropsRef.current.colFields);
    /** @type {any} */
    const changedKeys = Object.keys(nextSyncState).filter(key => JSON.stringify(nextSyncState[key]) !== JSON.stringify(lastPropsRef.current[key]));
    if (!setPropsRef.current || changedKeys.length === 0) return;

    // A hierarchy toggle can coincide with local display updates during the
    // same React commit. It must still retain the cache and layout history;
    // only another data-shaping change upgrades it to a full transaction.
    /** @type {any} */
    const isExpansionOnly = serverSide && changedKeys.length > 0
      && changedKeys.every(/** @param {any} [key] @returns {any} */ key => key === 'expanded');
    /** @type {any} */
    const isSortingOnly = serverSide && changedKeys.length > 0 && changedKeys.every(/** @param {any} [key] @returns {any} */ key => key === 'sorting');
    /** @type {any} */
    const isUiOnlyChange = changedKeys.length > 0 && changedKeys.every(/** @param {any} [key] @returns {any} */ key => UI_ONLY_KEYS.has(key));
    /** @type {any} */
    const grandTotalPinModeChanged = JSON.stringify(serverSidePinsGrandTotal) !== JSON.stringify(lastPropsRef.current.serverSidePinsGrandTotal);
    /** @type {any} */
    const requestStateOverride = buildRuntimeRequestStateOverride(nextProps);
    if (isUiOnlyChange && !grandTotalPinModeChanged) {
      lastPropsRef.current = nextSyncState;
      setPropsRef.current(nextProps);
      return;
    }

    /** @returns {any} */
    const clearExpansionScrollRestore = () => {
      expansionScrollRestoreRef.current = null;
      if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
        cancelAnimationFrame(expansionScrollRestoreRafRef.current);
        expansionScrollRestoreRafRef.current = null;
      }
    };

    if (isExpansionOnly) {
      lastPropsRef.current = nextSyncState;
      clearExpansionScrollRestore();
      /** @type {any} */
      const tx = beginExpansionRequest();
      markRequestPending(tx.version);
      /** @type {any} */
      const viewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || {
        start: 0, end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1, count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
      };
      /** @type {any} */
      const expansionColumnWindow = resolveStableRequestedColumnWindow();
      /** @type {any} */
      const anchorBlockHint = pendingExpansionRef.current && pendingExpansionRef.current.anchorBlock != null ? pendingExpansionRef.current.anchorBlock : -1;
      const expansionBlockSize = serverSideBlockSize;
      /** @type {any} */
      const viewportAlignedStart = Math.max(0, Math.floor(Math.max(0, viewportSnapshot.start || 0) / expansionBlockSize) * expansionBlockSize);
      /** @type {any} */
      const alignedStart = anchorBlockHint >= 0 ? Math.min(viewportAlignedStart, anchorBlockHint * expansionBlockSize) : viewportAlignedStart;
      /** @type {any} */
      const extendedEnd = anchorBlockHint >= 0 ? Math.max(viewportSnapshot.end, (anchorBlockHint + 2) * expansionBlockSize - 1) : viewportSnapshot.end;
      /** @type {any} */
      const alignedEnd = Math.max(extendedEnd, Math.ceil((Math.max(alignedStart, extendedEnd) + 1) / expansionBlockSize) * expansionBlockSize - 1);
      /** @type {any} */
      const extendedCount = alignedEnd - alignedStart + 1;
      if (pendingExpansionRef.current) {
        pendingExpansionRef.current.extendedToBlock = anchorBlockHint >= 0 ? anchorBlockHint + 1 : -1;
        pendingExpansionRef.current.expectedVersion = tx.version;
      }
      markRequestPending({ version: tx.version, reqStart: alignedStart, reqEnd: alignedEnd, colStart: expansionColumnWindow.start, colEnd: expansionColumnWindow.end });
      setPropsRef.current({
        ...nextProps,
        runtimeRequest: {
          kind: 'data',
          payload: {
            table: tableName || undefined, start: alignedStart, end: alignedEnd, count: extendedCount,
            version: tx.version, window_seq: tx.version, state_epoch: tx.stateEpoch,
            session_id: sessionIdRef.current, client_instance: clientInstanceRef.current,
            abort_generation: tx.abortGeneration, intent: 'expansion',
            col_start: expansionColumnWindow.start !== null ? expansionColumnWindow.start : undefined,
            col_end: expansionColumnWindow.end !== null ? expansionColumnWindow.end : undefined,
            needs_col_schema: needsColSchemaRef.current && serverSide || undefined,
            include_grand_total: serverSidePinsGrandTotal || undefined,
            layout_mode: layoutMode, show_subtotal_footers: showSubtotals,
            state_override: requestStateOverride || undefined,
            ...buildLayoutRequestFields(false, true),
          },
        },
      });
      return;
    }

    if (isSortingOnly) {
      lastPropsRef.current = nextSyncState;
      clearExpansionScrollRestore();
      if (parentRef.current) parentRef.current.scrollTop = 0;
      /** @type {any} */
      const tx = beginStructuralTransaction();
      /** @type {any} */
      const sortingColumnWindow = resolveStableRequestedColumnWindow();
      /** @type {any} */
      const initialEnd = DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1;
      markRequestPending({ version: tx.version, reqStart: 0, reqEnd: initialEnd, colStart: sortingColumnWindow.start, colEnd: sortingColumnWindow.end });
      setPropsRef.current({
        ...nextProps,
        runtimeRequest: {
          kind: 'data',
          payload: {
            table: tableName || undefined, start: 0, end: initialEnd, count: initialEnd + 1,
            version: tx.version, window_seq: tx.version, state_epoch: tx.stateEpoch,
            session_id: sessionIdRef.current, client_instance: clientInstanceRef.current,
            abort_generation: tx.abortGeneration, intent: 'viewport',
            col_start: sortingColumnWindow.start !== null ? sortingColumnWindow.start : undefined,
            col_end: sortingColumnWindow.end !== null ? sortingColumnWindow.end : undefined,
            include_grand_total: serverSidePinsGrandTotal || undefined,
            immersive_mode: immersiveMode || undefined, layout_mode: layoutMode,
            show_subtotal_footers: showSubtotals, state_override: requestStateOverride || undefined,
            ...buildLayoutRequestFields(false, true),
          },
        },
      });
      return;
    }

    if (serverSide && colFieldsChanged) {
      /** @type {any} */
      const previousCount = Array.isArray(lastPropsRef.current.colFields) ? lastPropsRef.current.colFields.length : 0;
      /** @type {any} */
      const nextCount = Array.isArray(nextProps.colFields) ? nextProps.colFields.length : 0;
      setPendingColumnSkeletonCount(Math.max(0, nextCount - previousCount));
    } else {
      setPendingColumnSkeletonCount(0);
    }
    lastPropsRef.current = nextSyncState;
    if (pendingExpansionRef.current) pendingExpansionRef.current = null;
    /** @type {any} */
    const tx = beginStructuralTransaction();
    /** @type {any} */
    const structuralViewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || {
      start: 0, end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1, count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
    };
    /** @type {any} */
    const structuralWindowSize = Math.max(DEFAULT_SERVER_SIDE_BLOCK_SIZE, Math.max(1, Number.isFinite(Number(structuralViewportSnapshot.count)) ? Number(structuralViewportSnapshot.count) : 0));
    const structuralStart = 0;
    /** @type {any} */
    const structuralEnd = structuralWindowSize - 1;
    /** @type {any} */
    const structuralColumnWindow = resolveStableRequestedColumnWindow();
    /** @type {any} */
    const schemaShapeChanged = serverSide && changedKeys.some(/** @param {any} [key] @returns {any} */ key => SCHEMA_SHAPE_CHANGE_KEYS.has(key));
    /** @type {any} */
    const shouldUseFullInitialColumnWindow = serverSide && (schemaShapeChanged || Number(latestDataVersionRef.current || 0) === 0 && structuralColumnWindow.start === 0 && structuralColumnWindow.end === 0);
    /** @type {any} */
    const structuralColStart = shouldUseFullInitialColumnWindow ? null : structuralColumnWindow.start;
    /** @type {any} */
    const structuralColEnd = shouldUseFullInitialColumnWindow ? null : structuralColumnWindow.end;
    markRequestPending({ version: tx.version, reqStart: structuralStart, reqEnd: structuralEnd, colStart: structuralColStart, colEnd: structuralColEnd });
    setPropsRef.current({
      ...nextProps,
      runtimeRequest: {
        kind: 'data',
        payload: {
          table: tableName || undefined, start: structuralStart, end: structuralEnd, count: structuralWindowSize,
          version: tx.version, window_seq: tx.version, state_epoch: tx.stateEpoch,
          session_id: sessionIdRef.current, client_instance: clientInstanceRef.current,
          abort_generation: tx.abortGeneration, intent: 'structural',
          col_start: structuralColStart !== null ? structuralColStart : undefined,
          col_end: structuralColEnd !== null ? structuralColEnd : undefined,
          needs_col_schema: serverSide || undefined, include_grand_total: serverSidePinsGrandTotal || undefined,
          immersive_mode: immersiveMode || undefined, layout_mode: layoutMode,
          show_subtotal_footers: showSubtotals, state_override: requestStateOverride || undefined,
          ...buildLayoutRequestFields(schemaShapeChanged, true),
        },
      },
    });
  }, [beginExpansionRequest, beginStructuralTransaction, buildLayoutRequestFields, buildRuntimeRequestStateOverride, clientEngine, clientInstanceRef, clientRuntimeActive, colFields, columnOrder, columnPinning, columnSizing, columnVisibility, customDimensions, detailConfig, detailMode, effectiveSortOptions, executionMode, expanded, expansionScrollRestoreRafRef, expansionScrollRestoreRef, filters, grandTotalPinOverride, groupAllowUnbalanced, immersiveMode, lastPropsRef, latestDataVersionRef, latestRequestedViewportRef, latestViewportRef, layoutMode, markRequestPending, needsColSchemaRef, parentRef, pendingExpansionRef, reportDef, resolveStableRequestedColumnWindow, rowFields, rowPinning, runtimeMeasureAxis, serverSide, serverSideBlockSize, serverSidePinsGrandTotal, sessionIdRef, setPendingColumnSkeletonCount, setPropsRef, showColTotals, showGrandTotal, showRowTotals, showSubtotals, sorting, tableName, treeConfig, unbalancedValues, valConfigs, viewMode]);
}

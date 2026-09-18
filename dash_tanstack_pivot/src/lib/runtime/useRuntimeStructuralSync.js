import { useEffect, useRef } from 'react';

const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;
const DEFAULT_INITIAL_SYNC_STATE = { layoutMode: 'hierarchy' };
const UI_ONLY_KEYS = new Set(['detailMode', 'detailConfig', 'columnPinning', 'rowPinning', 'grandTotalPinOverride', 'columnVisibility', 'columnOrder', 'columnSizing']);
const SCHEMA_SHAPE_CHANGE_KEYS = new Set(['viewMode', 'detailMode', 'treeConfig', 'detailConfig', 'reportDef', 'customDimensions', 'measureAxis', 'rowFields', 'colFields', 'valConfigs', 'showRowTotals', 'showColTotals', 'showSubtotals', 'layoutMode']);

/**
 * Synchronizes structural grid state with Dash and starts the corresponding
 * server-side data request. The three request paths are explicit: local UI,
 * hierarchy expansion, and cache-invalidating structural changes.
 */
export function useRuntimeStructuralSync({
  actions: {
    beginExpansionRequest,
    beginStructuralTransaction,
    buildRuntimeRequestStateOverride,
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
    colFields,
    columnOrder,
    columnPinning,
    columnSizing,
    columnVisibility,
    customDimensions,
    detailConfig,
    detailMode,
    effectiveSortOptions,
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
    sorting,
    tableName,
    treeConfig,
    valConfigs,
    viewMode,
  },
  initialState,
}) {
  const initialSyncState = { ...DEFAULT_INITIAL_SYNC_STATE, ...initialState };
  const lastPropsRef = useRef(initialSyncState);
  useEffect(() => {
    const nextProps = {
      viewMode, detailMode, treeConfig, detailConfig, reportDef, customDimensions,
      measureAxis: runtimeMeasureAxis, rowFields, colFields, valConfigs, filters,
      sorting, sortOptions: effectiveSortOptions, expanded, immersiveMode,
      showRowTotals, showColTotals, showSubtotals, columnPinning, rowPinning,
      columnVisibility, columnOrder, columnSizing,
    };
    const nextSyncState = { ...nextProps, layoutMode, grandTotalPinOverride, serverSidePinsGrandTotal };
    const colFieldsChanged = JSON.stringify(nextProps.colFields) !== JSON.stringify(lastPropsRef.current.colFields);
    const changedKeys = Object.keys(nextSyncState).filter(key => JSON.stringify(nextSyncState[key]) !== JSON.stringify(lastPropsRef.current[key]));
    if (!setPropsRef.current || changedKeys.length === 0) return;

    // A hierarchy toggle can coincide with local display updates during the
    // same React commit. It must still retain the cache and layout history;
    // only another data-shaping change upgrades it to a full transaction.
    const isExpansionOnly = serverSide && changedKeys.length > 0
      && changedKeys.every(key => key === 'expanded');
    const isSortingOnly = serverSide && changedKeys.length > 0 && changedKeys.every(key => key === 'sorting');
    const isUiOnlyChange = changedKeys.length > 0 && changedKeys.every(key => UI_ONLY_KEYS.has(key));
    const grandTotalPinModeChanged = JSON.stringify(serverSidePinsGrandTotal) !== JSON.stringify(lastPropsRef.current.serverSidePinsGrandTotal);
    const requestStateOverride = buildRuntimeRequestStateOverride(nextProps);
    if (isUiOnlyChange && !grandTotalPinModeChanged) {
      lastPropsRef.current = nextSyncState;
      setPropsRef.current(nextProps);
      return;
    }

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
      const tx = beginExpansionRequest();
      markRequestPending(tx.version);
      const viewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || {
        start: 0, end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1, count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
      };
      const expansionColumnWindow = resolveStableRequestedColumnWindow();
      const anchorBlockHint = pendingExpansionRef.current && pendingExpansionRef.current.anchorBlock != null ? pendingExpansionRef.current.anchorBlock : -1;
      const expansionBlockSize = serverSideBlockSize;
      const viewportAlignedStart = Math.max(0, Math.floor(Math.max(0, viewportSnapshot.start || 0) / expansionBlockSize) * expansionBlockSize);
      const alignedStart = anchorBlockHint >= 0 ? Math.min(viewportAlignedStart, anchorBlockHint * expansionBlockSize) : viewportAlignedStart;
      const extendedEnd = anchorBlockHint >= 0 ? Math.max(viewportSnapshot.end, (anchorBlockHint + 2) * expansionBlockSize - 1) : viewportSnapshot.end;
      const alignedEnd = Math.max(extendedEnd, Math.ceil((Math.max(alignedStart, extendedEnd) + 1) / expansionBlockSize) * expansionBlockSize - 1);
      const extendedCount = alignedEnd - alignedStart + 1;
      if (pendingExpansionRef.current) pendingExpansionRef.current.extendedToBlock = anchorBlockHint >= 0 ? anchorBlockHint + 1 : -1;
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
          },
        },
      });
      return;
    }

    if (isSortingOnly) {
      lastPropsRef.current = nextSyncState;
      clearExpansionScrollRestore();
      if (parentRef.current) parentRef.current.scrollTop = 0;
      const tx = beginStructuralTransaction();
      const sortingColumnWindow = resolveStableRequestedColumnWindow();
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
          },
        },
      });
      return;
    }

    if (serverSide && colFieldsChanged) {
      const previousCount = Array.isArray(lastPropsRef.current.colFields) ? lastPropsRef.current.colFields.length : 0;
      const nextCount = Array.isArray(nextProps.colFields) ? nextProps.colFields.length : 0;
      setPendingColumnSkeletonCount(Math.max(0, nextCount - previousCount));
    } else {
      setPendingColumnSkeletonCount(0);
    }
    lastPropsRef.current = nextSyncState;
    const tx = beginStructuralTransaction();
    const structuralViewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || {
      start: 0, end: DEFAULT_SERVER_SIDE_BLOCK_SIZE - 1, count: DEFAULT_SERVER_SIDE_BLOCK_SIZE,
    };
    const structuralWindowSize = Math.max(DEFAULT_SERVER_SIDE_BLOCK_SIZE, Math.max(1, Number.isFinite(Number(structuralViewportSnapshot.count)) ? Number(structuralViewportSnapshot.count) : 0));
    const structuralStart = 0;
    const structuralEnd = structuralWindowSize - 1;
    const structuralColumnWindow = resolveStableRequestedColumnWindow();
    const schemaShapeChanged = serverSide && changedKeys.some(key => SCHEMA_SHAPE_CHANGE_KEYS.has(key));
    const shouldUseFullInitialColumnWindow = serverSide && (schemaShapeChanged || Number(latestDataVersionRef.current || 0) === 0 && structuralColumnWindow.start === 0 && structuralColumnWindow.end === 0);
    const structuralColStart = shouldUseFullInitialColumnWindow ? null : structuralColumnWindow.start;
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
        },
      },
    });
  }, [beginExpansionRequest, beginStructuralTransaction, buildRuntimeRequestStateOverride, clientInstanceRef, colFields, columnOrder, columnPinning, columnSizing, columnVisibility, customDimensions, detailConfig, detailMode, effectiveSortOptions, expanded, expansionScrollRestoreRafRef, expansionScrollRestoreRef, filters, grandTotalPinOverride, immersiveMode, lastPropsRef, latestDataVersionRef, latestRequestedViewportRef, latestViewportRef, layoutMode, markRequestPending, needsColSchemaRef, parentRef, pendingExpansionRef, reportDef, resolveStableRequestedColumnWindow, rowFields, rowPinning, runtimeMeasureAxis, serverSide, serverSideBlockSize, serverSidePinsGrandTotal, sessionIdRef, setPendingColumnSkeletonCount, setPropsRef, showColTotals, showRowTotals, showSubtotals, sorting, tableName, treeConfig, valConfigs, viewMode]);
}

// @ts-nocheck -- composed leaf hooks are progressively typed at their owners.
// Workspace-stage composition. Focused hooks own presentation state groups.
import { useMemo, useRef, useEffect, useCallback } from 'react';
import { isDarkTheme } from '../utils/styles';
import { areStringArraysEqual, mergeColumnOrderWithIds, moveColumnIdByDelta, moveColumnIdInOrder } from '../utils/columnOrder';
import { normalizeMeasureAxisValue } from '../utils/measureAxis';
import { TABLE_OVERLAY_CHART_PANE_ID, isCustomCategoryField, cloneSerializable, normalizePropagationFormulaValue, isParentAggregatePropagationEdit, hasTreeDefaultExpansionConfig, getRequestedChartSeriesColumnIds } from '../hooks/usePivotNormalization';
const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;
const EXPAND_ALL_STATE_KEY = '__ALL__';
// Editing is intentionally not a public feature. Keep its former internals
// unreachable while downstream presentation receives stable compatibility
// values until the obsolete implementation is physically removed.
const EDITING_COMPATIBILITY_ENABLED = false;
const EMPTY_EDIT_STATUS = Object.freeze({
  activeRowCount: 0,
  savingRowCount: 0,
  dirtyRowCount: 0,
  dirtyFieldCount: 0,
  invalidRowCount: 0,
  invalidFieldCount: 0,
});
const EMPTY_EDIT_CELLS = Object.freeze([]);
const unsupportedEditCommand = () => undefined;
const normalizeLayoutModeValue = value => {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'outline' || normalized === 'tabular' ? normalized : 'hierarchy';
};
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
import { formatDisplayLabel, getAllLeafIdsFromColumn } from '../utils/helpers';
import { buildComboSelectionChartModel, buildSelectionChartModel, canStackBarLayout } from '../utils/chartModelBuilders';
import { useRenderHelpers } from '../hooks/useRenderHelpers';
import { getStickyHeaderHeight } from '../utils/pivotLayoutUtils';
import { useChartWorkspacePresentation } from '../runtime/useChartWorkspacePresentation';
import { useWorkspaceStatusPresentation } from '../runtime/useWorkspaceStatusPresentation';
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
export function useWorkspaceRuntimeStage(input) {
  // Presentation assembly is local. Do not augment the viewport domain that
  // supplied it; return a frozen workspace snapshot instead.
  const workspace = { ...input };
  useEffect(() => {
    if (workspace.chartPanelBarLayout !== 'stacked') return;
    const canKeepStacked = workspace.chartPanelType === 'area' ? workspace.activeChartPanelModel && Array.isArray(workspace.activeChartPanelModel.series) && workspace.activeChartPanelModel.series.length > 1 : canStackBarLayout(workspace.activeChartPanelModel);
    if (canKeepStacked) return;
    workspace.setChartPanelBarLayout('grouped');
  }, [workspace.activeChartPanelModel, workspace.chartPanelBarLayout, workspace.chartPanelType]);
  workspace.openSelectionChart = useCallback((selectionMap = workspace.selectedCells) => {
    const resolvedSelectionMap = selectionMap && typeof selectionMap === 'object' && !('nativeEvent' in selectionMap) && typeof selectionMap.preventDefault !== 'function'
      ? selectionMap
      : workspace.selectedCells;
    const hasSelection = Object.keys(resolvedSelectionMap || {}).length > 0;
    if (!hasSelection) {
      workspace.showNotification('Select a range of value cells first.', 'warning');
      return;
    }
    const visibleRows = Array.isArray(workspace.chartDisplayRows) && workspace.chartDisplayRows.length > 0
      ? workspace.chartDisplayRows
      : [
        ...(workspace.effectiveTopRows || []),
        ...(workspace.effectiveCenterRows || []),
        ...(workspace.effectiveBottomRows || []),
      ];
    const modalChartType = workspace.chartPanelType === 'icicle' || workspace.chartPanelType === 'sunburst' || workspace.chartPanelType === 'sankey' ? 'bar' : workspace.chartPanelType;
    const buildModel = (orientationValue, hierarchyValue) => modalChartType === 'combo' ? buildComboSelectionChartModel(resolvedSelectionMap, visibleRows, workspace.visibleLeafColumns, {
      hierarchyLevel: hierarchyValue,
      maxHierarchyLevel: workspace.rowFields.length,
      maxRows: workspace.chartPanelRowLimit,
      maxColumns: workspace.chartPanelColumnLimit,
      rowFields: workspace.rowFields,
      colFields: workspace.colFields,
      sortMode: workspace.chartPanelSortMode,
      layers: workspace.chartPanelLayers
    }) : buildSelectionChartModel(resolvedSelectionMap, visibleRows, workspace.visibleLeafColumns, {
      orientation: orientationValue,
      hierarchyLevel: hierarchyValue,
      maxHierarchyLevel: workspace.rowFields.length,
      maxRows: workspace.chartPanelRowLimit,
      maxColumns: workspace.chartPanelColumnLimit,
      rowFields: workspace.rowFields,
      colFields: workspace.colFields,
      sortMode: workspace.chartPanelSortMode
    });
    const preferredOrientation = workspace.chartPanelOrientation === 'columns' ? 'columns' : 'rows';
    let defaultOrientation = preferredOrientation;
    let defaultHierarchyLevel = workspace.chartPanelHierarchyLevel;
    let defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
    if ((!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) && defaultHierarchyLevel !== 'all') {
      defaultHierarchyLevel = 'all';
      defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
    }
    if (!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) {
      defaultOrientation = defaultOrientation === 'columns' ? 'rows' : 'columns';
      defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
    }
    if ((!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) && defaultHierarchyLevel !== 'all') {
      defaultHierarchyLevel = 'all';
      defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
    }
    const canOpenStacked = modalChartType === 'area' ? defaultChartModel && Array.isArray(defaultChartModel.series) && defaultChartModel.series.length > 1 : canStackBarLayout(defaultChartModel);
    workspace.setChartModal({
      title: 'Range Chart',
      chartType: modalChartType,
      chartLayers: cloneSerializable(workspace.chartPanelLayers, []),
      barLayout: workspace.chartPanelBarLayout === 'stacked' && canOpenStacked ? 'stacked' : 'grouped',
      axisMode: workspace.chartPanelAxisMode,
      defaultOrientation,
      defaultHierarchyLevel,
      selectionMap: {
        ...(resolvedSelectionMap || {})
      },
      visibleRows,
      visibleCols: workspace.visibleLeafColumns,
      maxHierarchyLevel: workspace.rowFields.length,
      rowLimit: workspace.chartPanelRowLimit,
      columnLimit: workspace.chartPanelColumnLimit,
      rowFields: workspace.rowFields,
      colFields: workspace.colFields,
      sortMode: workspace.chartPanelSortMode
    });
  }, [workspace.selectedCells, workspace.chartDisplayRows, workspace.effectiveTopRows, workspace.effectiveCenterRows, workspace.effectiveBottomRows, workspace.visibleLeafColumns, workspace.chartPanelType, workspace.chartPanelLayers, workspace.chartPanelBarLayout, workspace.chartPanelAxisMode, workspace.chartPanelOrientation, workspace.chartPanelHierarchyLevel, workspace.rowFields.length, workspace.chartPanelRowLimit, workspace.chartPanelColumnLimit, workspace.rowFields, workspace.colFields, workspace.chartPanelSortMode, workspace.showNotification]);
  workspace.onCreateSelectionChart = useCallback(() => {
    workspace.openSelectionChart();
  }, [workspace.openSelectionChart]);
  workspace.virtualRows = workspace.activeRowVirtualizer.getVirtualItems();
  workspace.activeColumnSkeletonCount = Math.max(workspace.pendingColumnSkeletonCount, workspace.pendingHorizontalColumnCount);
  workspace.showColumnLoadingSkeletons = workspace.serverSide && (workspace.structuralInFlight && workspace.pendingColumnSkeletonCount > 0 || workspace.isHorizontalColumnRequestPending && workspace.pendingHorizontalColumnCount > 0);
  workspace.columnSkeletonWidth = workspace.defaultColumnWidths.schemaFallback;
  workspace.stickyHeaderHeight = getStickyHeaderHeight(workspace.table.getHeaderGroups().length, workspace.rowHeight, workspace.showFloatingFilters);
  workspace.bodyRowsTopOffset = workspace.stickyHeaderHeight + workspace.effectiveTopRows.length * workspace.rowHeight;
  useEffect(() => {
    if (!workspace.serverSide || workspace.virtualRows.length === 0) return;
    const firstRow = workspace.virtualRows[0].index;
    const lastRow = workspace.virtualRows[workspace.virtualRows.length - 1].index;
    workspace.latestViewportRef.current = {
      start: firstRow,
      end: lastRow,
      count: Math.max(1, lastRow - firstRow + 1)
    };
  }, [workspace.serverSide, workspace.virtualRows]);
  workspace.liveChartStateFingerprint = useMemo(() => JSON.stringify(workspace.buildChartStateOverrideSnapshot() || {}), [workspace.buildChartStateOverrideSnapshot]);
  workspace.chartRequestCandidates = useMemo(() => {
    if (!workspace.serverSide || workspace.structuralInFlight || workspace.needsColSchema || workspace.totalCenterCols === null) return [];
    const nextCandidates = [];
    if (workspace.chartPanelOpen && workspace.chartPanelSource === 'pivot') {
      if (!(workspace.chartPanelLocked && !workspace.chartPanelLockedRequest)) {
        const baseRequest = workspace.chartPanelLocked && workspace.chartPanelLockedRequest ? workspace.chartPanelLockedRequest.request : workspace.buildChartRequestBase({
          chartType: workspace.chartPanelType,
          chartLayers: workspace.chartPanelLayers,
          rowLimit: workspace.chartPanelRowLimit,
          columnLimit: workspace.chartPanelColumnLimit,
          serverScope: workspace.chartPanelServerScope
        });
        if (baseRequest) {
          const signature = workspace.chartPanelLocked && workspace.chartPanelLockedRequest ? workspace.chartPanelLockedRequest.requestSignature || JSON.stringify({
            paneId: TABLE_OVERLAY_CHART_PANE_ID,
            locked: true,
            request: baseRequest,
            stateOverride: workspace.chartPanelLockedRequest.stateOverride || null
          }) : JSON.stringify({
            paneId: TABLE_OVERLAY_CHART_PANE_ID,
            locked: false,
            request: baseRequest,
            state: workspace.liveChartStateFingerprint
          });
          nextCandidates.push({
            paneId: TABLE_OVERLAY_CHART_PANE_ID,
            signature,
            request: {
              ...baseRequest,
              state_override: workspace.chartPanelLocked && workspace.chartPanelLockedRequest ? workspace.chartPanelLockedRequest.stateOverride || undefined : undefined
            }
          });
        }
      }
    }
    workspace.chartCanvasPanes.forEach(pane => {
      if (!pane || pane.source !== 'pivot') return;
      if (pane.locked && !pane.lockedRequest) return;
      const baseRequest = pane.locked && pane.lockedRequest ? pane.lockedRequest.request : workspace.buildChartRequestBase({
        chartType: pane.chartType,
        chartLayers: pane.chartLayers,
        rowLimit: pane.rowLimit,
        columnLimit: pane.columnLimit,
        serverScope: pane.serverScope
      });
      if (!baseRequest) return;
      const signature = pane.locked && pane.lockedRequest ? pane.lockedRequest.requestSignature || JSON.stringify({
        paneId: pane.id,
        locked: true,
        request: baseRequest,
        stateOverride: pane.lockedRequest.stateOverride || null
      }) : JSON.stringify({
        paneId: pane.id,
        locked: false,
        request: baseRequest,
        state: workspace.liveChartStateFingerprint
      });
      nextCandidates.push({
        paneId: pane.id,
        signature,
        request: {
          ...baseRequest,
          state_override: pane.locked && pane.lockedRequest ? pane.lockedRequest.stateOverride || undefined : undefined
        }
      });
    });
    return nextCandidates;
  }, [workspace.buildChartRequestBase, workspace.chartCanvasPanes, workspace.chartPanelColumnLimit, workspace.chartPanelLayers, workspace.chartPanelLocked, workspace.chartPanelLockedRequest, workspace.chartPanelOpen, workspace.chartPanelRowLimit, workspace.chartPanelServerScope, workspace.chartPanelSource, workspace.chartPanelType, workspace.liveChartStateFingerprint, workspace.needsColSchema, workspace.serverSide, workspace.structuralInFlight, workspace.totalCenterCols]);
  workspace.previousChartRenderSignaturesRef = useRef({});
  useEffect(() => {
    const nextSignatures = {
      [TABLE_OVERLAY_CHART_PANE_ID]: JSON.stringify({
        open: workspace.chartPanelOpen,
        source: workspace.chartPanelSource,
        chartType: workspace.chartPanelType,
        seriesColumnIds: getRequestedChartSeriesColumnIds(workspace.chartPanelType, workspace.chartPanelLayers),
        locked: workspace.chartPanelLocked,
        request: workspace.chartPanelLockedRequest ? workspace.chartPanelLockedRequest.requestSignature : null,
        rowLimit: workspace.chartPanelRowLimit,
        columnLimit: workspace.chartPanelColumnLimit,
        orientation: workspace.chartPanelOrientation,
        hierarchyLevel: workspace.chartPanelHierarchyLevel,
        sortMode: workspace.chartPanelSortMode,
        serverScope: workspace.chartPanelServerScope,
        state: workspace.chartPanelLocked ? null : workspace.liveChartStateFingerprint
      })
    };
    workspace.chartCanvasPanes.forEach(pane => {
      nextSignatures[pane.id] = JSON.stringify({
        source: pane.source,
        chartType: pane.chartType,
        seriesColumnIds: getRequestedChartSeriesColumnIds(pane.chartType, pane.chartLayers),
        locked: pane.locked,
        request: pane.lockedRequest ? pane.lockedRequest.requestSignature : null,
        rowLimit: pane.rowLimit,
        columnLimit: pane.columnLimit,
        orientation: pane.orientation,
        hierarchyLevel: pane.hierarchyLevel,
        sortMode: pane.sortMode,
        serverScope: pane.serverScope,
        state: pane.locked ? null : workspace.liveChartStateFingerprint
      });
    });
    const previousSignatures = workspace.previousChartRenderSignaturesRef.current || {};
    const changedPaneIds = Object.keys(nextSignatures).filter(paneId => previousSignatures[paneId] && previousSignatures[paneId] !== nextSignatures[paneId]);
    workspace.previousChartRenderSignaturesRef.current = nextSignatures;
    if (changedPaneIds.length === 0) return;
    workspace.setChartPaneDataById(previousData => {
      let mutated = false;
      const nextData = {
        ...previousData
      };
      changedPaneIds.forEach(paneId => {
        if (Object.prototype.hasOwnProperty.call(nextData, paneId)) {
          delete nextData[paneId];
          mutated = true;
        }
      });
      return mutated ? nextData : previousData;
    });
  }, [workspace.chartCanvasPanes, workspace.chartPanelColumnLimit, workspace.chartPanelHierarchyLevel, workspace.chartPanelLayers, workspace.chartPanelLocked, workspace.chartPanelLockedRequest, workspace.chartPanelOpen, workspace.chartPanelOrientation, workspace.chartPanelRowLimit, workspace.chartPanelServerScope, workspace.chartPanelSortMode, workspace.chartPanelSource, workspace.chartPanelType, workspace.liveChartStateFingerprint]);
  workspace.treeHasDefaultExpansion = useMemo(() => workspace.viewMode === 'tree' && hasTreeDefaultExpansionConfig(workspace.treeConfig), [workspace.treeConfig, workspace.viewMode]);

  // Detail cache, surface sync, and reset effects provided by useDetailDrillThrough hook

  // Detail response handling effect provided by useDetailDrillThrough hook

  // Chart modeling supplies this list later in the viewport stage. Keep this
  // projection after that producer, while the scroll/table-owned horizontal
  // virtualizer itself remains in the viewport stage.
  workspace.visibleLeafColIndexMap = useMemo(
    () => new Map(workspace.visibleLeafColumns.map((column, index) => [column.id, index])),
    [workspace.visibleLeafColumns]
  );

  workspace.configureFieldInteractions({
    tableRef: workspace.tableRef,
    table: workspace.table,
    getAllLeafIdsFromColumn,
    setColumnPinningWithHistory: workspace.setColumnPinningWithHistory,
    layoutMode: workspace.layoutMode,
    rowFields: workspace.rowFields,
    colFields: workspace.colFields,
    valConfigs: workspace.valConfigs,
    filters: workspace.filters,
    measureAxis: workspace.measureAxis,
    sparklineFields: workspace.sparklineFields,
    isCustomCategoryField,
    normalizeMeasureAxisValue,
    requestLayoutHistoryCapture: workspace.requestLayoutHistoryCapture,
    setMeasureAxis: workspace.setMeasureAxis,
    setRowFieldsWithHistory: workspace.setRowFieldsWithHistory,
    setColFieldsWithHistory: workspace.setColFieldsWithHistory,
    setValConfigsWithHistory: workspace.setValConfigsWithHistory,
    setFilters: workspace.setFilters,
    showNotification: workspace.showNotification,
    parentRef: workspace.parentRef,
    getPinningState: workspace.getPinningState,
    availableColumnOrderIds: workspace.availableColumnOrderIds,
    effectiveColumnOrder: workspace.effectiveColumnOrder,
    columnOrder: workspace.columnOrder,
    mergeColumnOrderWithIds,
    moveColumnIdInOrder,
    moveColumnIdByDelta,
    areStringArraysEqual,
    setColumnOrderWithHistory: workspace.setColumnOrderWithHistory,
    formatDisplayLabel,
    getLocaleText: workspace.getLocaleText,
    setAnnouncement: workspace.setAnnouncement
  });

  ({
    renderCell: workspace.renderCell,
    renderVirtualColumnCell: workspace.renderVirtualColumnCell,
    renderHeaderCell: workspace.renderHeaderCell
  } = useRenderHelpers({
    // renderCell dependencies
    serverSide: workspace.serverSide,
    selectedCells: workspace.selectedCells,
    fillRange: workspace.fillRange,
    dragStart: workspace.dragStart,
    theme: workspace.theme,
    getStickyStyle: workspace.getStickyStyle,
    isDarkTheme,
    handleCellMouseDown: workspace.handleCellMouseDown,
    handleCellMouseEnter: workspace.handleCellMouseEnter,
    handleContextMenu: workspace.handleContextMenu,
    handleFillMouseDown: workspace.handleFillMouseDown,
    visibleLeafColIndexMap: workspace.visibleLeafColIndexMap,
    lastSelected: workspace.lastSelected,
    styles: workspace.styles,
    getConditionalStyle: workspace.getConditionalStyle,
    // renderHeaderCell dependencies (synchronous / render-time reads)
    rowHeight: workspace.rowHeight,
    rowFields: workspace.rowFields,
    tableRef: workspace.tableRef,
    leftCols: workspace.leftCols,
    centerCols: workspace.centerCols,
    rightCols: workspace.rightCols,
    columnPinning: workspace.columnPinning,
    sorting: workspace.sorting,
    handleHeaderContextMenu: workspace.handleHeaderContextMenu,
    autoSizeColumn: workspace.autoSizeColumn,
    autoSizeBounds: workspace.autoSizeBounds,
    hoveredHeaderIdRef: workspace.hoveredHeaderIdRef,
    setHoveredHeaderId: workspace.setHoveredHeaderId,
    focusedHeaderIdRef: workspace.focusedHeaderIdRef,
    setFocusedHeaderId: workspace.setFocusedHeaderId,
    onDragStart: workspace.onDragStart,
    onHeaderDrop: workspace.onHeaderDrop,
    onHeaderDragOver: workspace.onHeaderDragOver,
    onHeaderDragEnd: workspace.onHeaderDragEnd,
    onHeaderKeyboardMove: workspace.onHeaderKeyboardMove,
    onHeaderKeyboardPin: workspace.onHeaderKeyboardPin,
    dragOverHeaderId: workspace.dragOverHeaderId,
    dragOverSide: workspace.dragOverSide,
    handleFilterClick: workspace.handleFilterClick,
    handleHeaderFilter: workspace.handleHeaderFilter,
    activeFilterCol: workspace.activeFilterCol,
    filterAnchorEl: workspace.filterAnchorEl,
    closeFilterPopover: workspace.closeFilterPopover,
    activeFilterOptions: workspace.activeFilterOptions,
    activeFilterOptionMeta: workspace.activeFilterCol ? workspace.filterOptionMeta[workspace.activeFilterCol] : null,
    requestFilterOptions: workspace.requestFilterOptions,
    filters: workspace.filters,
    selectedCols: workspace.selectedCols,
    getHeaderStickyStyle: workspace.getHeaderStickyStyle,
    dataBarsColumns: workspace.dataBarsColumns,
    dataBarMaximums: workspace.dataBarMaximums,
    colorScaleStats: workspace.colorScaleStats,
    cellFormatRules: workspace.cellFormatRules,
    resolveEditedCellMarker: workspace.resolveEditedCellMarker,
    resolveCellDisplayValue: workspace.resolveDisplayedCellValue,
    editedCellEpoch: workspace.editedCellEpoch,
    headerFormatting: workspace.headerFormatting,
    getLocaleText: workspace.getLocaleText,
    wrapHeaders: workspace.uiConfig.wrapHeaders,
    table: workspace.table,
    selectedRowKeys: workspace.selectedRowKeys,
    toggleRowCheckbox: workspace.toggleRowCheckbox,
    rowCheckboxMode: workspace.rowCheckboxMode
  }));
  workspace.srOnly = {
    position: 'absolute',
    width: '1px',
    height: '1px',
    padding: 0,
    margin: '-1px',
    overflow: 'hidden',
    clip: 'rect(0, 0, 0, 0)',
    whiteSpace: 'nowrap',
    border: 0,
    pointerEvents: 'none'
  };
  workspace.focusStyle = {
    outline: `2px solid ${workspace.theme.primary}`,
    outlineOffset: '2px'
  };
  workspace.handleClearStatusSelection = useCallback(() => {
    workspace.setSelectedCells({});
    workspace.setLastSelected(null);
  }, [workspace.setLastSelected, workspace.setSelectedCells]);
  workspace.handleClearStatusFilters = useCallback(() => {
    workspace.setFilters({});
  }, [workspace.setFilters]);
  workspace.handleRefreshStatusViewport = useCallback(() => {
    if (!workspace.serverSide || typeof workspace.requestVisibleViewportRefresh !== 'function') return;
    workspace.requestVisibleViewportRefresh();
  }, [workspace.requestVisibleViewportRefresh, workspace.serverSide]);
  workspace.rowEditStatusSummary = EMPTY_EDIT_STATUS;
  workspace.editPanelCells = EMPTY_EDIT_CELLS;
  workspace.collectTrackedEventIdsForCells = unsupportedEditCommand;
  workspace.collectAllTrackedEventIds = unsupportedEditCommand;
  workspace.handleRevertCell = unsupportedEditCommand;
  workspace.handleConfirmPropagation = unsupportedEditCommand;
  workspace.handleCancelPropagation = unsupportedEditCommand;
  workspace.handlePropagationMethodChange = unsupportedEditCommand;
  workspace.handleReapplyPropagation = unsupportedEditCommand;
  workspace.handleRevertSelected = unsupportedEditCommand;
  workspace.handleRevertAll = unsupportedEditCommand;
  if (EDITING_COMPATIBILITY_ENABLED) {
  workspace.rowEditStatusSummary = useMemo(() => {
    const sessions = Object.values(workspace.rowEditSessions || {});
    return sessions.reduce((summary, session) => {
      if (!session || session.active !== true) return summary;
      summary.activeRowCount += 1;
      if (session.status === 'saving') summary.savingRowCount += 1;
      const dirtyFieldCount = Object.keys(session.drafts || {}).length;
      const invalidFieldCount = Object.keys(session.errors || {}).length;
      if (dirtyFieldCount > 0) {
        summary.dirtyRowCount += 1;
        summary.dirtyFieldCount += dirtyFieldCount;
      }
      if (invalidFieldCount > 0) {
        summary.invalidRowCount += 1;
        summary.invalidFieldCount += invalidFieldCount;
      }
      return summary;
    }, {
      activeRowCount: 0,
      savingRowCount: 0,
      dirtyRowCount: 0,
      dirtyFieldCount: 0,
      invalidRowCount: 0,
      invalidFieldCount: 0
    });
  }, [workspace.rowEditSessions]);
  workspace.editPanelCells = useMemo(() => {
    const cells = [];
    const selKeys = Object.keys(workspace.selectedCells || {});
    if (selKeys.length === 0) return cells;
    for (const selKey of selKeys) {
      const sepIdx = selKey.lastIndexOf(':');
      if (sepIdx < 0) continue;
      const rowId = selKey.slice(0, sepIdx);
      const colId = selKey.slice(sepIdx + 1);
      const marker = workspace.resolveEditedCellMarker(rowId, colId);
      if (!marker) continue;
      const key = workspace.getOptimisticCellKey(rowId, colId);
      const origEntry = key ? workspace.comparisonCellOriginalValuesRef.current.get(key) : null;
      cells.push({
        rowId,
        colId,
        direct: marker.direct,
        propagated: marker.propagated,
        originalValue: origEntry ? origEntry.value : undefined,
        currentValue: workspace.selectedCells[selKey]
      });
    }
    return cells;
  }, [workspace.selectedCells, workspace.editedCellEpoch, workspace.comparisonCellEpoch, workspace.resolveEditedCellMarker, workspace.getOptimisticCellKey]);

  // Auto-open panel when selected cells include edited ones (only if not already dismissed)
  useEffect(() => {
    if (workspace.editPanelCells.length > 0) workspace.setEditPanelOpen(true);
  }, [workspace.editPanelCells.length]);
  workspace.collectTrackedEventIdsForCells = useCallback((cells, options = {}) => {
    const rawEventIds = [];
    (Array.isArray(cells) ? cells : []).forEach(cell => {
      if (!cell || typeof cell !== 'object') return;
      rawEventIds.push(...workspace.resolveTrackedCellEventIds(cell.rowId, cell.colId, options));
    });
    return workspace.sortEventIdsByActiveHistory(rawEventIds);
  }, [workspace.resolveTrackedCellEventIds, workspace.sortEventIdsByActiveHistory]);
  workspace.collectAllTrackedEventIds = useCallback(() => {
    const rawEventIds = [];
    workspace.comparisonCellOriginalValuesRef.current.forEach(entry => {
      if (!entry || typeof entry !== 'object') return;
      rawEventIds.push(...workspace.normalizeTrackedEventIds(entry.directEventIds));
      rawEventIds.push(...workspace.normalizeTrackedEventIds(entry.propagatedEventIds));
    });
    return workspace.sortEventIdsByActiveHistory(rawEventIds);
  }, [workspace.normalizeTrackedEventIds, workspace.sortEventIdsByActiveHistory]);
  workspace.handleRevertCell = useCallback((rowId, colId) => {
    const key = workspace.getOptimisticCellKey(rowId, colId);
    if (!key) return;
    const targetEventIds = workspace.serverSide ? workspace.resolveTrackedCellEventIds(rowId, colId, {
      includeDirect: true,
      includePropagated: true
    }) : [];
    if (workspace.serverSide && targetEventIds.length > 0) {
      const dispatched = workspace.dispatchTransactionRequest({
        eventAction: 'revert',
        eventIds: targetEventIds,
        refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
      }, 'revert');
      if (!dispatched || !dispatched.requestId) return;
      workspace.queueTransactionHistoryRequest(dispatched.requestId, {
        action: 'revert',
        source: 'revert',
        targetEventIds,
        historyEntries: targetEventIds.map(eventId => workspace.findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
        successMessage: 'Reverted the selected cell.'
      }, {
        blocksHistory: true
      });
      return;
    }
    const originalEntry = workspace.comparisonCellOriginalValuesRef.current.get(key);
    if (originalEntry) {
      workspace.optimisticCellValuesRef.current.delete(key);
      workspace.editedCellMarkersRef.current.delete(key);
      workspace.comparisonCellCountsRef.current.delete(key);
      workspace.comparisonCellOriginalValuesRef.current.delete(key);
      workspace.bumpEditedCellEpoch();
      workspace.bumpComparisonCellEpoch();
      workspace.syncComparisonValueState();
      if (!workspace.serverSide && workspace.setPropsRef.current && originalEntry.value !== undefined) {
        workspace.setPropsRef.current({
          cellUpdates: [{
            rowId: originalEntry.rowId,
            colId: originalEntry.colId,
            value: originalEntry.value
          }]
        });
      }
    }
  }, [workspace.bumpComparisonCellEpoch, workspace.bumpEditedCellEpoch, workspace.dispatchTransactionRequest, workspace.findTransactionHistoryEntryByEventId, workspace.getOptimisticCellKey, workspace.resolveTrackedCellEventIds, workspace.serverSide, workspace.supportsPatchTransactionRefresh, workspace.syncComparisonValueState, workspace.queueTransactionHistoryRequest]);
  workspace.handleConfirmPropagation = useCallback(() => {
    const pending = workspace.pendingPropagationUpdatesRef.current;
    if (!pending || !pending.updates || pending.updates.length === 0) return;
    const formula = workspace.propagationMethodUI || 'equal';
    workspace.lastPropagationFormulaRef.current = formula;
    const preparedUpdates = pending.updates.map(update => {
      if (!isParentAggregatePropagationEdit(update, workspace.rowFields)) return update;
      return {
        ...update,
        propagationStrategy: formula
      };
    });
    workspace.pendingPropagationUpdatesRef.current = null;
    workspace.setPendingPropagationUpdates(null);
    workspace.setEditPanelOpen(false);
    if (workspace.editValueDisplayMode === 'original') {
      workspace.setEditValueDisplayMode('edited');
    }
    if (!workspace.serverSide) {
      if (workspace.setPropsRef.current) workspace.setPropsRef.current({
        cellUpdates: preparedUpdates
      });
      return;
    }
    workspace.dispatchServerEditUpdates(preparedUpdates, pending.source, pending.meta || null);
  }, [workspace.dispatchServerEditUpdates, workspace.editValueDisplayMode, workspace.propagationMethodUI, workspace.rowFields, workspace.serverSide, workspace.setEditValueDisplayMode]);
  workspace.handleCancelPropagation = useCallback(() => {
    workspace.pendingPropagationUpdatesRef.current = null;
    workspace.setPendingPropagationUpdates(null);
    workspace.setEditPanelOpen(false);
  }, []);
  workspace.handlePropagationMethodChange = useCallback(method => {
    workspace.setPropagationMethodUI(method);
  }, []);
  workspace.handleReapplyPropagation = useCallback((directCells, method) => {
    if (!Array.isArray(directCells) || directCells.length === 0) return;
    const normalizedMethod = normalizePropagationFormulaValue(method, 'equal') || 'equal';
    if (!workspace.serverSide) {
      if (workspace.setPropsRef.current) {
        workspace.setPropsRef.current({
          cellUpdates: directCells.map(cell => ({
            rowId: cell.rowId,
            colId: cell.colId,
            value: cell.currentValue,
            propagationStrategy: normalizedMethod
          }))
        });
      }
      return;
    }
    const targetEventIds = workspace.sortEventIdsByActiveHistory(directCells.flatMap(cell => workspace.resolveTrackedCellEventIds(cell.rowId, cell.colId, {
      includeDirect: true,
      includePropagated: false
    })));
    if (targetEventIds.length === 0) {
      workspace.showNotification('This edit does not have a tracked server event to replace.', 'warning');
      return;
    }
    const replacementUpdates = directCells.map(cell => {
      const key = workspace.getOptimisticCellKey(cell.rowId, cell.colId);
      const originalEntry = key ? workspace.comparisonCellOriginalValuesRef.current.get(key) : null;
      return {
        rowId: cell.rowId,
        colId: cell.colId,
        value: cell.currentValue,
        oldValue: originalEntry ? originalEntry.value : undefined,
        propagationStrategy: normalizedMethod
      };
    });
    const comparisonPlan = workspace.buildComparisonValuePlan(replacementUpdates);
    const editedMarkerPlan = workspace.buildEditedCellMarkerPlan(replacementUpdates);
    const dispatched = workspace.dispatchTransactionRequest({
      eventAction: 'replace',
      eventIds: targetEventIds,
      propagationStrategy: normalizedMethod,
      refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
    }, 'reapply-propagation');
    if (!dispatched || !dispatched.requestId) return;
    workspace.queueTransactionHistoryRequest(dispatched.requestId, {
      action: 'replace',
      source: 'reapply-propagation',
      targetEventIds,
      historyEntries: targetEventIds.map(eventId => workspace.findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
      comparisonPlan,
      editedMarkerPlan,
      successMessage: 'Updated propagation for the selected edit scope.'
    }, {
      blocksHistory: true
    });
  }, [workspace.buildComparisonValuePlan, workspace.buildEditedCellMarkerPlan, workspace.dispatchTransactionRequest, workspace.findTransactionHistoryEntryByEventId, workspace.getOptimisticCellKey, workspace.resolveTrackedCellEventIds, workspace.serverSide, workspace.showNotification, workspace.sortEventIdsByActiveHistory, workspace.supportsPatchTransactionRefresh, workspace.queueTransactionHistoryRequest]);
  workspace.handleRevertSelected = useCallback(cells => {
    if (!Array.isArray(cells) || cells.length === 0) return;
    if (workspace.serverSide) {
      const targetEventIds = workspace.collectTrackedEventIdsForCells(cells, {
        includeDirect: true,
        includePropagated: true
      });
      if (targetEventIds.length > 0) {
        const dispatched = workspace.dispatchTransactionRequest({
          eventAction: 'revert',
          eventIds: targetEventIds,
          refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
        }, 'revert-selected');
        if (!dispatched || !dispatched.requestId) return;
        workspace.queueTransactionHistoryRequest(dispatched.requestId, {
          action: 'revert',
          source: 'revert-selected',
          targetEventIds,
          historyEntries: targetEventIds.map(eventId => workspace.findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
          successMessage: `Reverted ${cells.length} selected edit${cells.length === 1 ? '' : 's'}.`
        }, {
          blocksHistory: true
        });
        return;
      }
    }
    const revertUpdates = [];
    cells.forEach(cell => {
      const key = workspace.getOptimisticCellKey(cell.rowId, cell.colId);
      if (!key) return;
      const originalEntry = workspace.comparisonCellOriginalValuesRef.current.get(key);
      if (originalEntry && originalEntry.value !== undefined) {
        revertUpdates.push({
          rowId: originalEntry.rowId,
          colId: originalEntry.colId,
          value: originalEntry.value
        });
      }
      workspace.optimisticCellValuesRef.current.delete(key);
      workspace.editedCellMarkersRef.current.delete(key);
      workspace.comparisonCellCountsRef.current.delete(key);
      workspace.comparisonCellOriginalValuesRef.current.delete(key);
    });
    workspace.bumpEditedCellEpoch();
    workspace.bumpComparisonCellEpoch();
    workspace.syncComparisonValueState();
    if (revertUpdates.length > 0) {
      if (workspace.serverSide) {
        workspace.dispatchTransactionRequest({
          update: revertUpdates,
          refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
        }, 'revert-selected-fallback');
      } else if (workspace.setPropsRef.current) {
        workspace.setPropsRef.current({
          cellUpdates: revertUpdates
        });
      }
    }
  }, [workspace.bumpComparisonCellEpoch, workspace.bumpEditedCellEpoch, workspace.collectTrackedEventIdsForCells, workspace.dispatchTransactionRequest, workspace.findTransactionHistoryEntryByEventId, workspace.getOptimisticCellKey, workspace.serverSide, workspace.supportsPatchTransactionRefresh, workspace.syncComparisonValueState, workspace.queueTransactionHistoryRequest]);
  workspace.handleRevertAll = useCallback(() => {
    const targetEventIds = workspace.serverSide ? workspace.collectAllTrackedEventIds() : [];
    if (workspace.serverSide && targetEventIds.length > 0) {
      const dispatched = workspace.dispatchTransactionRequest({
        eventAction: 'revert',
        eventIds: targetEventIds,
        refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
      }, 'revert-all');
      if (!dispatched || !dispatched.requestId) return;
      workspace.queueTransactionHistoryRequest(dispatched.requestId, {
        action: 'revert',
        source: 'revert-all',
        targetEventIds,
        historyEntries: targetEventIds.map(eventId => workspace.findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
        clearAllOnSuccess: true,
        successMessage: 'Reverted all active edits.'
      }, {
        blocksHistory: true
      });
      return;
    }
    const revertUpdates = [];
    workspace.comparisonCellOriginalValuesRef.current.forEach(entry => {
      if (!entry || entry.value === undefined) return;
      revertUpdates.push({
        rowId: entry.rowId,
        colId: entry.colId,
        value: entry.value
      });
    });
    workspace.clearAllOptimisticCellValues();
    workspace.clearAllEditedCellMarkers();
    workspace.clearAllComparisonValueState();
    workspace.propagationLogRef.current = [];
    workspace.setPropagationLogEpoch(previousEpoch => previousEpoch + 1);
    workspace.syncTransactionHistoryState();
    if (revertUpdates.length > 0) {
      if (workspace.serverSide) {
        workspace.dispatchTransactionRequest({
          update: revertUpdates,
          refreshMode: workspace.supportsPatchTransactionRefresh ? 'patch' : 'smart'
        }, 'revert-all-fallback');
      } else if (workspace.setPropsRef.current) {
        workspace.setPropsRef.current({
          cellUpdates: revertUpdates
        });
      }
    }
  }, [workspace.clearAllComparisonValueState, workspace.clearAllEditedCellMarkers, workspace.clearAllOptimisticCellValues, workspace.collectAllTrackedEventIds, workspace.dispatchTransactionRequest, workspace.findTransactionHistoryEntryByEventId, workspace.serverSide, workspace.supportsPatchTransactionRefresh, workspace.queueTransactionHistoryRequest]);
  }
  const statusPresentation = useWorkspaceStatusPresentation({
    chartCanvasPanes: workspace.chartCanvasPanes,
    clearAllComparisonValueState: workspace.clearAllComparisonValueState,
    clearAllEditedCellMarkers: workspace.clearAllEditedCellMarkers,
    clearAllOptimisticCellValues: workspace.clearAllOptimisticCellValues,
    colFields: workspace.colFields,
    columnAdvisory: workspace.columnAdvisory,
    comparisonCellOriginalValuesRef: workspace.comparisonCellOriginalValuesRef,
    comparisonValueState: workspace.comparisonValueState,
    dispatchTransactionRequest: workspace.dispatchTransactionRequest,
    editedCellMarkersRef: workspace.editedCellMarkersRef,
    editValueDisplayMode: workspace.editValueDisplayMode,
    filters: workspace.filters,
    handleClearStatusFilters: workspace.handleClearStatusFilters,
    handleClearStatusSelection: workspace.handleClearStatusSelection,
    handleRedo: workspace.handleRedo,
    handleRefreshStatusViewport: workspace.handleRefreshStatusViewport,
    handleUndo: workspace.handleUndo,
    isRequestPending: workspace.isRequestPending,
    loadedRows: workspace.loadedRows,
    managedChartDefinitions: workspace.managedChartDefinitions,
    openSelectionChart: workspace.openSelectionChart,
    propagationLogRef: workspace.propagationLogRef,
    propagationLogEpoch: workspace.propagationLogEpoch,
    requestVisibleViewportRefresh: workspace.requestVisibleViewportRefresh,
    rowEditStatusSummary: workspace.rowEditStatusSummary,
    rowFields: workspace.rowFields,
    rows: workspace.rows,
    selectedCells: workspace.selectedCells,
    serverSide: workspace.serverSide,
    setEditValueDisplayMode: workspace.setEditValueDisplayMode,
    setPropagationLogEpoch: workspace.setPropagationLogEpoch,
    setPropsRef: workspace.setPropsRef,
    showNotification: workspace.showNotification,
    sorting: workspace.sorting,
    statusRowCount: workspace.statusRowCount,
    supportsPatchTransactionRefresh: workspace.supportsPatchTransactionRefresh,
    syncTransactionHistoryState: workspace.syncTransactionHistoryState,
    totalCenterCols: workspace.totalCenterCols,
    transactionHistoryState: workspace.transactionHistoryState,
    valConfigs: workspace.valConfigs,
    viewMode: workspace.viewMode,
  });
  workspace.statusAccessoryModel = statusPresentation.statusAccessoryModel;
  workspace.statusAccessoryActions = statusPresentation.statusAccessoryActions;
  const chartWorkspace = useChartWorkspacePresentation({
    activateChartCategory: workspace.activateChartCategory,
    chartCanvasLayoutRef: workspace.chartCanvasLayoutRef,
    chartCanvasPaneModels: workspace.chartCanvasPaneModels,
    chartCanvasPanes: workspace.chartCanvasPanes,
    chartModal: workspace.chartModal,
    chartModalPosition: workspace.chartModalPosition,
    chartPaneDataById: workspace.chartPaneDataById,
    handleRemoveChartCanvasPane: workspace.handleRemoveChartCanvasPane,
    handleStartChartCanvasPaneFloatingDrag: workspace.handleStartChartCanvasPaneFloatingDrag,
    handleStartChartCanvasPaneFloatingResize: workspace.handleStartChartCanvasPaneFloatingResize,
    handleToggleChartCanvasPaneFloating: workspace.handleToggleChartCanvasPaneFloating,
    handleToggleChartCanvasPaneLock: workspace.handleToggleChartCanvasPaneLock,
    serverSide: workspace.serverSide,
    setChartCanvasPaneWidthHints: workspace.setChartCanvasPaneWidthHints,
    setChartModal: workspace.setChartModal,
    setChartModalPosition: workspace.setChartModalPosition,
    theme: workspace.theme,
    updateChartCanvasPane: workspace.updateChartCanvasPane,
    visibleLeafColumns: workspace.visibleLeafColumns,
  });
  workspace.handleChartCanvasPaneWidthHintChange = chartWorkspace.handleChartCanvasPaneWidthHintChange;
  workspace.chartWorkspacePanes = chartWorkspace.chartWorkspacePanes;
  workspace.floatingChartCanvasPanes = chartWorkspace.floatingChartCanvasPanes;
  workspace.dockedChartCanvasPanesByPosition = chartWorkspace.dockedChartCanvasPanesByPosition;
  workspace.renderChartCanvasPane = chartWorkspace.renderChartCanvasPane;
  workspace.renderChartWorkspaceModal = chartWorkspace.renderChartWorkspaceModal;
  workspace.renderFloatingChartCanvasPane = chartWorkspace.renderFloatingChartCanvasPane;
  return Object.freeze(workspace);
}

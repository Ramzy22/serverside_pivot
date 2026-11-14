import React, { useCallback, useEffect, useMemo } from 'react';

import {
  DEFAULT_CHART_GRAPH_HEIGHT,
  MIN_CHART_CANVAS_PANE_WIDTH,
  MIN_TABLE_PANEL_WIDTH,
  normalizeChartDockPosition,
  resolveChartAvailableColumns
} from '../hooks/usePivotNormalization';
import { groupChartWorkspacePanes } from '../components/Charts/ChartWorkspace';

/** @type {any} */
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartModal
})));
/** @type {any} */
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartPanel
})));
/** @returns {any} */
const PivotChartFallback = () => null;
/** @param {any} [props] @returns {any} */
const PivotChartModal = props => <React.Suspense fallback={<PivotChartFallback />}>
  <LazyPivotChartModal {...props} />
</React.Suspense>;
/** @param {any} [props] @returns {any} */
const PivotChartPanel = props => <React.Suspense fallback={<PivotChartFallback />}>
  <LazyPivotChartPanel {...props} />
</React.Suspense>;

/**
 * Owns chart-pane presentation: pane grouping, pane width persistence and the
 * render callbacks consumed by the workspace shell. The caller supplies the
 * chart model and commands as named dependencies; no legacy runtime bag is
 * passed into this hook.
  @param {any} p0 @returns {any} */
export function useChartWorkspacePresentation({
  activateChartCategory,
  chartCanvasLayoutRef,
  chartCanvasPaneModels,
  chartCanvasPanes,
  chartModal,
  chartModalPosition,
  chartPaneDataById,
  handleRemoveChartCanvasPane,
  handleStartChartCanvasPaneFloatingDrag,
  handleStartChartCanvasPaneFloatingResize,
  handleToggleChartCanvasPaneFloating,
  handleToggleChartCanvasPaneLock,
  serverSide,
  setChartCanvasPaneWidthHints,
  setChartModal,
  setChartModalPosition,
  theme,
  updateChartCanvasPane,
  visibleLeafColumns,
}) {
  /** @type {(...args: any[]) => any} */
  const handleChartCanvasPaneWidthHintChange = useCallback((paneId, nextWidthHint = null) => {
    setChartCanvasPaneWidthHints(/** @param {any} [previousHints] @returns {any} */ previousHints => {
      /** @type {any} */
      const currentValue = Object.prototype.hasOwnProperty.call(previousHints, paneId) ? previousHints[paneId] : undefined;
      if (!Number.isFinite(Number(nextWidthHint))) {
        if (currentValue === undefined) return previousHints;
        const { [paneId]: _removed, ...rest } = previousHints;
        return rest;
      }
      /** @type {any} */
      const layoutWidth = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect().width : 0;
      /** @type {any} */
      const maxChartCanvasPaneWidth = Number.isFinite(layoutWidth) && layoutWidth > 0 ? Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(layoutWidth - MIN_TABLE_PANEL_WIDTH - 16)) : Number.POSITIVE_INFINITY;
      /** @type {any} */
      const normalizedWidth = Math.min(maxChartCanvasPaneWidth, Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(nextWidthHint))));
      if (currentValue === normalizedWidth) return previousHints;
      return { ...previousHints, [paneId]: normalizedWidth };
    });
  }, [chartCanvasLayoutRef, setChartCanvasPaneWidthHints]);
  useEffect(() => {
    /** @type {any} */
    const activePaneIds = new Set(chartCanvasPanes.map(/** @param {any} [pane] @returns {any} */ pane => pane.id));
    setChartCanvasPaneWidthHints(/** @param {any} [previousHints] @returns {any} */ previousHints => {
      /** @type {any} */
      const nextHints = Object.entries(previousHints).reduce((acc, [paneId, widthHint]) => {
        if (activePaneIds.has(paneId)) acc[paneId] = widthHint;
        return acc;
      }, /** @type {Record<string, any>} */ ({}));
      return Object.keys(nextHints).length === Object.keys(previousHints).length ? previousHints : nextHints;
    });
  }, [chartCanvasPanes, setChartCanvasPaneWidthHints]);
  /** @type {any} */
  const chartWorkspacePanes = useMemo(() => groupChartWorkspacePanes(chartCanvasPanes), [chartCanvasPanes]);
  const floatingChartCanvasPanes = chartWorkspacePanes.floating;
  const dockedChartCanvasPanesByPosition = chartWorkspacePanes.dockedByPosition;
  /** @type {(...args: any[]) => any} */
  const paneProps = useCallback((pane, floating, initialSettingsPaneOpen) => {
    /** @type {Record<string, any>} */
    const props = {
    open: true,
    onClose: () => handleRemoveChartCanvasPane(pane.id),
    source: pane.source,
    /** @param {any} [value] @returns {any} */
    onSourceChange: value => updateChartCanvasPane(pane.id, { source: value }),
    chartType: pane.chartType,
    /** @param {any} [value] @returns {any} */
    onChartTypeChange: value => updateChartCanvasPane(pane.id, { chartType: value }),
    chartLayers: pane.chartLayers,
    /** @param {any} [value] @returns {any} */
    onChartLayersChange: value => updateChartCanvasPane(pane.id, { chartLayers: value }),
    availableColumns: resolveChartAvailableColumns(chartPaneDataById[pane.id], visibleLeafColumns),
    barLayout: pane.barLayout,
    /** @param {any} [value] @returns {any} */
    onBarLayoutChange: value => updateChartCanvasPane(pane.id, { barLayout: value }),
    axisMode: pane.axisMode,
    /** @param {any} [value] @returns {any} */
    onAxisModeChange: value => updateChartCanvasPane(pane.id, { axisMode: value }),
    orientation: pane.orientation,
    /** @param {any} [value] @returns {any} */
    onOrientationChange: value => updateChartCanvasPane(pane.id, { orientation: value }),
    hierarchyLevel: pane.hierarchyLevel,
    /** @param {any} [value] @returns {any} */
    onHierarchyLevelChange: value => updateChartCanvasPane(pane.id, { hierarchyLevel: value }),
    chartTitle: pane.chartTitle || pane.name,
    /** @param {any} [value] @returns {any} */
    onChartTitleChange: value => updateChartCanvasPane(pane.id, { chartTitle: value }),
    rowLimit: pane.rowLimit,
    /** @param {any} [value] @returns {any} */
    onRowLimitChange: value => updateChartCanvasPane(pane.id, { rowLimit: value }),
    columnLimit: pane.columnLimit,
    /** @param {any} [value] @returns {any} */
    onColumnLimitChange: value => updateChartCanvasPane(pane.id, { columnLimit: value }),
    chartHeight: pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT,
    /** @param {any} [value] @returns {any} */
    onChartHeightChange: value => {
      /** @type {any} */
      const nextHeight = Number(value);
      updateChartCanvasPane(pane.id, {
        chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT
      });
    },
    sortMode: pane.sortMode,
    /** @param {any} [value] @returns {any} */
    onSortModeChange: value => updateChartCanvasPane(pane.id, { sortMode: value }),
    interactionMode: pane.interactionMode,
    /** @param {any} [value] @returns {any} */
    onInteractionModeChange: value => updateChartCanvasPane(pane.id, { interactionMode: value }),
    serverScope: pane.serverScope,
    /** @param {any} [value] @returns {any} */
    onServerScopeChange: value => updateChartCanvasPane(pane.id, { serverScope: value }),
    showServerScope: serverSide,
    model: chartCanvasPaneModels[pane.id] || null,
    theme,
    /** @param {any} [target] @returns {any} */
    onCategoryActivate: target => activateChartCategory(pane.source, pane.interactionMode, target),
    floating,
    onToggleFloating: () => handleToggleChartCanvasPaneFloating(pane.id),
    floatingRect: pane.floatingRect,
    /** @param {any} [event] @returns {any} */
    onFloatingDragStart: event => handleStartChartCanvasPaneFloatingDrag(pane.id, event),
    /** @param {any} [direction] @param {any} [event] @returns {any} */
    onFloatingResizeStart: (direction, event) => handleStartChartCanvasPaneFloatingResize(pane.id, direction, event),
    initialSettings: pane.chartSettings || {},
    /** @param {any} [settings] @returns {any} */
    onSettingsChange: settings => updateChartCanvasPane(pane.id, { chartSettings: settings }),
    settingsPanelWidth: pane.settingsPanelWidth || undefined,
    standalone: true,
    showResizeHandle: false,
    title: pane.name,
    showDefinitionManager: false,
    locked: pane.locked,
    onToggleLock: () => handleToggleChartCanvasPaneLock(pane.id),
    immersiveMode: Boolean(pane.immersiveMode),
    /** @param {any} [value] @returns {any} */
    onImmersiveModeChange: value => updateChartCanvasPane(pane.id, { immersiveMode: Boolean(value) })
    };
    if (initialSettingsPaneOpen !== undefined) props.initialSettingsPaneOpen = initialSettingsPaneOpen;
    return props;
  }, [activateChartCategory, chartCanvasPaneModels, chartPaneDataById, handleRemoveChartCanvasPane, handleStartChartCanvasPaneFloatingDrag, handleStartChartCanvasPaneFloatingResize, handleToggleChartCanvasPaneFloating, handleToggleChartCanvasPaneLock, serverSide, theme, updateChartCanvasPane, visibleLeafColumns]);
  /** @type {(...args: any[]) => any} */
  const renderChartCanvasPane = useCallback((pane, normalizedDockPosition) => <PivotChartPanel {...paneProps(pane, false, Boolean(pane.openSettingsOnMount))} dockPosition={normalizedDockPosition} onDockPositionChange={/** @param {any} [value] @returns {any} */ value => updateChartCanvasPane(pane.id, {
    dockPosition: normalizeChartDockPosition(value, normalizedDockPosition)
  })} onSettingsWidthBudgetChange={/** @param {any} [nextWidthHint] @returns {any} */ nextWidthHint => handleChartCanvasPaneWidthHintChange(pane.id, normalizedDockPosition === 'left' || normalizedDockPosition === 'right' ? nextWidthHint : null)} />, [handleChartCanvasPaneWidthHintChange, paneProps, updateChartCanvasPane]);
  /** @param {any} [position] @returns {any} */
  const renderChartWorkspaceModal = position => chartModal && chartModalPosition === position ? <PivotChartModal chartState={chartModal} onClose={() => setChartModal(null)} theme={theme} position={chartModalPosition} onPositionChange={setChartModalPosition} /> : null;
  /** @type {(...args: any[]) => any} */
  const renderFloatingChartCanvasPane = useCallback(pane => <PivotChartPanel key={pane.id} {...paneProps(pane, true)} />, [paneProps]);

  return {
    chartWorkspacePanes,
    dockedChartCanvasPanesByPosition,
    floatingChartCanvasPanes,
    handleChartCanvasPaneWidthHintChange,
    renderChartCanvasPane,
    renderChartWorkspaceModal,
    renderFloatingChartCanvasPane
  };
}

import React, { useCallback, useEffect, useMemo } from 'react';

import {
  DEFAULT_CHART_GRAPH_HEIGHT,
  MIN_CHART_CANVAS_PANE_WIDTH,
  MIN_TABLE_PANEL_WIDTH,
  normalizeChartDockPosition,
  resolveChartAvailableColumns
} from '../hooks/usePivotNormalization';
import { groupChartWorkspacePanes } from '../components/Charts/ChartWorkspace';

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

/**
 * Owns chart-pane presentation: pane grouping, pane width persistence and the
 * render callbacks consumed by the workspace shell. The caller supplies the
 * chart model and commands as named dependencies; no legacy runtime bag is
 * passed into this hook.
 */
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
  const handleChartCanvasPaneWidthHintChange = useCallback((paneId, nextWidthHint = null) => {
    setChartCanvasPaneWidthHints(previousHints => {
      const currentValue = Object.prototype.hasOwnProperty.call(previousHints, paneId) ? previousHints[paneId] : undefined;
      if (!Number.isFinite(Number(nextWidthHint))) {
        if (currentValue === undefined) return previousHints;
        const { [paneId]: _removed, ...rest } = previousHints;
        return rest;
      }
      const layoutWidth = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect().width : 0;
      const maxChartCanvasPaneWidth = Number.isFinite(layoutWidth) && layoutWidth > 0 ? Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(layoutWidth - MIN_TABLE_PANEL_WIDTH - 16)) : Number.POSITIVE_INFINITY;
      const normalizedWidth = Math.min(maxChartCanvasPaneWidth, Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(nextWidthHint))));
      if (currentValue === normalizedWidth) return previousHints;
      return { ...previousHints, [paneId]: normalizedWidth };
    });
  }, [chartCanvasLayoutRef, setChartCanvasPaneWidthHints]);
  useEffect(() => {
    const activePaneIds = new Set(chartCanvasPanes.map(pane => pane.id));
    setChartCanvasPaneWidthHints(previousHints => {
      const nextHints = Object.entries(previousHints).reduce((acc, [paneId, widthHint]) => {
        if (activePaneIds.has(paneId)) acc[paneId] = widthHint;
        return acc;
      }, {});
      return Object.keys(nextHints).length === Object.keys(previousHints).length ? previousHints : nextHints;
    });
  }, [chartCanvasPanes, setChartCanvasPaneWidthHints]);
  const chartWorkspacePanes = useMemo(() => groupChartWorkspacePanes(chartCanvasPanes), [chartCanvasPanes]);
  const floatingChartCanvasPanes = chartWorkspacePanes.floating;
  const dockedChartCanvasPanesByPosition = chartWorkspacePanes.dockedByPosition;
  const paneProps = useCallback((pane, floating, initialSettingsPaneOpen) => {
    const props = {
    open: true,
    onClose: () => handleRemoveChartCanvasPane(pane.id),
    source: pane.source,
    onSourceChange: value => updateChartCanvasPane(pane.id, { source: value }),
    chartType: pane.chartType,
    onChartTypeChange: value => updateChartCanvasPane(pane.id, { chartType: value }),
    chartLayers: pane.chartLayers,
    onChartLayersChange: value => updateChartCanvasPane(pane.id, { chartLayers: value }),
    availableColumns: resolveChartAvailableColumns(chartPaneDataById[pane.id], visibleLeafColumns),
    barLayout: pane.barLayout,
    onBarLayoutChange: value => updateChartCanvasPane(pane.id, { barLayout: value }),
    axisMode: pane.axisMode,
    onAxisModeChange: value => updateChartCanvasPane(pane.id, { axisMode: value }),
    orientation: pane.orientation,
    onOrientationChange: value => updateChartCanvasPane(pane.id, { orientation: value }),
    hierarchyLevel: pane.hierarchyLevel,
    onHierarchyLevelChange: value => updateChartCanvasPane(pane.id, { hierarchyLevel: value }),
    chartTitle: pane.chartTitle || pane.name,
    onChartTitleChange: value => updateChartCanvasPane(pane.id, { chartTitle: value }),
    rowLimit: pane.rowLimit,
    onRowLimitChange: value => updateChartCanvasPane(pane.id, { rowLimit: value }),
    columnLimit: pane.columnLimit,
    onColumnLimitChange: value => updateChartCanvasPane(pane.id, { columnLimit: value }),
    chartHeight: pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT,
    onChartHeightChange: value => {
      const nextHeight = Number(value);
      updateChartCanvasPane(pane.id, {
        chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT
      });
    },
    sortMode: pane.sortMode,
    onSortModeChange: value => updateChartCanvasPane(pane.id, { sortMode: value }),
    interactionMode: pane.interactionMode,
    onInteractionModeChange: value => updateChartCanvasPane(pane.id, { interactionMode: value }),
    serverScope: pane.serverScope,
    onServerScopeChange: value => updateChartCanvasPane(pane.id, { serverScope: value }),
    showServerScope: serverSide,
    model: chartCanvasPaneModels[pane.id] || null,
    theme,
    onCategoryActivate: target => activateChartCategory(pane.source, pane.interactionMode, target),
    floating,
    onToggleFloating: () => handleToggleChartCanvasPaneFloating(pane.id),
    floatingRect: pane.floatingRect,
    onFloatingDragStart: event => handleStartChartCanvasPaneFloatingDrag(pane.id, event),
    onFloatingResizeStart: (direction, event) => handleStartChartCanvasPaneFloatingResize(pane.id, direction, event),
    initialSettings: pane.chartSettings || {},
    onSettingsChange: settings => updateChartCanvasPane(pane.id, { chartSettings: settings }),
    settingsPanelWidth: pane.settingsPanelWidth || undefined,
    standalone: true,
    showResizeHandle: false,
    title: pane.name,
    showDefinitionManager: false,
    locked: pane.locked,
    onToggleLock: () => handleToggleChartCanvasPaneLock(pane.id),
    immersiveMode: Boolean(pane.immersiveMode),
    onImmersiveModeChange: value => updateChartCanvasPane(pane.id, { immersiveMode: Boolean(value) })
    };
    if (initialSettingsPaneOpen !== undefined) props.initialSettingsPaneOpen = initialSettingsPaneOpen;
    return props;
  }, [activateChartCategory, chartCanvasPaneModels, chartPaneDataById, handleRemoveChartCanvasPane, handleStartChartCanvasPaneFloatingDrag, handleStartChartCanvasPaneFloatingResize, handleToggleChartCanvasPaneFloating, handleToggleChartCanvasPaneLock, serverSide, theme, updateChartCanvasPane, visibleLeafColumns]);
  const renderChartCanvasPane = useCallback((pane, normalizedDockPosition) => <PivotChartPanel {...paneProps(pane, false, Boolean(pane.openSettingsOnMount))} dockPosition={normalizedDockPosition} onDockPositionChange={value => updateChartCanvasPane(pane.id, {
    dockPosition: normalizeChartDockPosition(value, normalizedDockPosition)
  })} onSettingsWidthBudgetChange={nextWidthHint => handleChartCanvasPaneWidthHintChange(pane.id, normalizedDockPosition === 'left' || normalizedDockPosition === 'right' ? nextWidthHint : null)} />, [handleChartCanvasPaneWidthHintChange, paneProps, updateChartCanvasPane]);
  const renderChartWorkspaceModal = position => chartModal && chartModalPosition === position ? <PivotChartModal chartState={chartModal} onClose={() => setChartModal(null)} theme={theme} position={chartModalPosition} onPositionChange={setChartModalPosition} /> : null;
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

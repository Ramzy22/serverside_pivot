import { useCallback } from 'react';

import {
  CHART_PANEL_VERTICAL_CHROME_HEIGHT,
  DEFAULT_FLOATING_CHART_PANEL_HEIGHT,
  DEFAULT_TABLE_CANVAS_SIZE,
  clampFloatingChartRect,
  createChartDefinitionId,
  normalizeChartCanvasPane,
  normalizeChartDefinition,
} from '../hooks/usePivotNormalization';

/**
 * Owns the chart-canvas pane lifecycle. It deliberately receives state and
 * mutators explicitly so chart commands cannot reach into the legacy runtime
 * record for unrelated grid behaviour.
  @param {{ setTableCanvasSize: { (updater: (previousSize: any) => any): any; (val?: any): any }, [key: string]: any }} p0 @returns {any} */
export function useRuntimeChartCanvasCommands({
  chartCanvasPanes,
  chartPanelAxisMode,
  chartPanelBarLayout,
  chartPanelColumnLimit,
  chartPanelGraphHeight,
  chartPanelHierarchyLevel,
  chartPanelInteractionMode,
  chartPanelLayers,
  chartPanelOrientation,
  chartPanelRowLimit,
  chartPanelServerScope,
  chartPanelSortMode,
  chartPanelSource,
  chartPanelTitle,
  chartPanelType,
  chartPanelWidth,
  completedChartRequestSignaturesRef,
  initialChartDefinition,
  previewChartPaneId,
  setChartCanvasPanes,
  setChartPaneDataById,
  setPreviewChartPaneId,
  setTableCanvasSize,
}) {
  /** @type {(...args: any[]) => any} */
  const updateChartCanvasPane = useCallback((paneId, updater) => {
    setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.map(/** @param {any} [pane] @returns {any} */ pane => {
      if (pane.id !== paneId) return pane;
      /** @type {any} */
      const nextPane = typeof updater === 'function' ? updater(pane) : {
        ...pane,
        ...(updater || {}),
      };
      return normalizeChartCanvasPane(nextPane, initialChartDefinition);
    }));
  }, [initialChartDefinition, setChartCanvasPanes]);

  /** @type {(...args: any[]) => any} */
  const handleAddChartCanvasPane = useCallback(() => {
    /** @type {any} */
    const baseDefinition = normalizeChartDefinition({
      id: createChartDefinitionId('chart-pane'),
      name: `Chart Pane ${chartCanvasPanes.length + 1}`,
      chartTitle: chartPanelTitle,
      source: chartPanelSource,
      chartType: chartPanelType,
      chartLayers: chartPanelLayers,
      barLayout: chartPanelBarLayout,
      axisMode: chartPanelAxisMode,
      orientation: chartPanelOrientation,
      hierarchyLevel: chartPanelHierarchyLevel,
      rowLimit: chartPanelRowLimit,
      columnLimit: chartPanelColumnLimit,
      width: chartPanelWidth,
      chartHeight: chartPanelGraphHeight,
      sortMode: chartPanelSortMode,
      interactionMode: chartPanelInteractionMode,
      serverScope: chartPanelServerScope,
    }, initialChartDefinition);
    /** @type {Record<string, any>} */
    const newPane = {
      ...baseDefinition,
      size: 1,
      dockPosition: 'right',
      floating: false,
      floatingRect: clampFloatingChartRect({
        width: chartPanelWidth,
        height: Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, chartPanelGraphHeight + CHART_PANEL_VERTICAL_CHROME_HEIGHT),
        left: 48 + chartCanvasPanes.length * 24,
        top: 48 + chartCanvasPanes.length * 24,
      }, null),
      locked: false,
      lockedModel: null,
      lockedRequest: null,
      immersiveMode: false,
      chartSettings: {},
      isPreview: true,
    };
    setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => [...previousPanes, newPane]);
    setPreviewChartPaneId(newPane.id);
  }, [chartCanvasPanes.length, chartPanelAxisMode, chartPanelBarLayout, chartPanelColumnLimit, chartPanelGraphHeight, chartPanelHierarchyLevel, chartPanelInteractionMode, chartPanelLayers, chartPanelOrientation, chartPanelRowLimit, chartPanelServerScope, chartPanelSortMode, chartPanelSource, chartPanelTitle, chartPanelType, chartPanelWidth, initialChartDefinition, setChartCanvasPanes, setPreviewChartPaneId]);

  /** @type {(...args: any[]) => any} */
  const handleConfirmPreviewChartPane = useCallback(() => {
    if (!previewChartPaneId) return;
    setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.map(/** @param {any} [pane] @returns {any} */ pane => pane.id === previewChartPaneId ? {
      ...pane,
      isPreview: false,
      openSettingsOnMount: true,
    } : pane));
    setPreviewChartPaneId(null);
  }, [previewChartPaneId, setChartCanvasPanes, setPreviewChartPaneId]);

  /** @type {(...args: any[]) => any} */
  const handleCancelPreviewChartPane = useCallback(() => {
    if (!previewChartPaneId) return;
    setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.filter(/** @param {any} [pane] @returns {any} */ pane => pane.id !== previewChartPaneId));
    setPreviewChartPaneId(null);
  }, [previewChartPaneId, setChartCanvasPanes, setPreviewChartPaneId]);

  /** @type {(...args: any[]) => any} */
  const handleRemoveChartCanvasPane = useCallback(paneId => {
    setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => {
      /** @type {any} */
      const removedPane = previousPanes.find(/** @param {any} [pane] @returns {any} */ pane => pane.id === paneId) || null;
      /** @type {any} */
      const remainingPanes = previousPanes.filter(/** @param {any} [pane] @returns {any} */ pane => pane.id !== paneId);
      if (removedPane && !removedPane.floating) {
        /** @type {any} */
        const reclaimedSize = Number.isFinite(Number(removedPane.size)) && Number(removedPane.size) > 0 ? Number(removedPane.size) : 1;
        /** @type {any} */
        const firstDockedIndex = remainingPanes.findIndex(/** @param {any} [pane] @returns {any} */ pane => !pane.floating);
        if (firstDockedIndex >= 0) {
          /** @type {any[]} */
          const nextPanes = [...remainingPanes];
          nextPanes[firstDockedIndex] = {
            ...nextPanes[firstDockedIndex],
            size: (Number(nextPanes[firstDockedIndex].size) || 1) + reclaimedSize,
          };
          return nextPanes;
        }
        setTableCanvasSize(previousSize => Math.max(DEFAULT_TABLE_CANVAS_SIZE, (Number(previousSize) || DEFAULT_TABLE_CANVAS_SIZE) + reclaimedSize));
      }
      return remainingPanes;
    });
    setChartPaneDataById(/** @param {any} [previousData] @returns {any} */ previousData => {
      if (!Object.prototype.hasOwnProperty.call(previousData, paneId)) return previousData;
      /** @type {Record<string, any>} */
      const nextData = { ...previousData };
      delete nextData[paneId];
      return nextData;
    });
    delete completedChartRequestSignaturesRef.current[paneId];
  }, [completedChartRequestSignaturesRef, setChartCanvasPanes, setChartPaneDataById, setTableCanvasSize]);

  return {
    handleAddChartCanvasPane,
    handleCancelPreviewChartPane,
    handleConfirmPreviewChartPane,
    handleRemoveChartCanvasPane,
    updateChartCanvasPane,
  };
}

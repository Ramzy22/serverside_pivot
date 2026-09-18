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
 */
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
  const updateChartCanvasPane = useCallback((paneId, updater) => {
    setChartCanvasPanes(previousPanes => previousPanes.map(pane => {
      if (pane.id !== paneId) return pane;
      const nextPane = typeof updater === 'function' ? updater(pane) : {
        ...pane,
        ...(updater || {}),
      };
      return normalizeChartCanvasPane(nextPane, initialChartDefinition);
    }));
  }, [initialChartDefinition, setChartCanvasPanes]);

  const handleAddChartCanvasPane = useCallback(() => {
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
    setChartCanvasPanes(previousPanes => [...previousPanes, newPane]);
    setPreviewChartPaneId(newPane.id);
  }, [chartCanvasPanes.length, chartPanelAxisMode, chartPanelBarLayout, chartPanelColumnLimit, chartPanelGraphHeight, chartPanelHierarchyLevel, chartPanelInteractionMode, chartPanelLayers, chartPanelOrientation, chartPanelRowLimit, chartPanelServerScope, chartPanelSortMode, chartPanelSource, chartPanelTitle, chartPanelType, chartPanelWidth, initialChartDefinition, setChartCanvasPanes, setPreviewChartPaneId]);

  const handleConfirmPreviewChartPane = useCallback(() => {
    if (!previewChartPaneId) return;
    setChartCanvasPanes(previousPanes => previousPanes.map(pane => pane.id === previewChartPaneId ? {
      ...pane,
      isPreview: false,
      openSettingsOnMount: true,
    } : pane));
    setPreviewChartPaneId(null);
  }, [previewChartPaneId, setChartCanvasPanes, setPreviewChartPaneId]);

  const handleCancelPreviewChartPane = useCallback(() => {
    if (!previewChartPaneId) return;
    setChartCanvasPanes(previousPanes => previousPanes.filter(pane => pane.id !== previewChartPaneId));
    setPreviewChartPaneId(null);
  }, [previewChartPaneId, setChartCanvasPanes, setPreviewChartPaneId]);

  const handleRemoveChartCanvasPane = useCallback(paneId => {
    setChartCanvasPanes(previousPanes => {
      const removedPane = previousPanes.find(pane => pane.id === paneId) || null;
      const remainingPanes = previousPanes.filter(pane => pane.id !== paneId);
      if (removedPane && !removedPane.floating) {
        const reclaimedSize = Number.isFinite(Number(removedPane.size)) && Number(removedPane.size) > 0 ? Number(removedPane.size) : 1;
        const firstDockedIndex = remainingPanes.findIndex(pane => !pane.floating);
        if (firstDockedIndex >= 0) {
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
    setChartPaneDataById(previousData => {
      if (!Object.prototype.hasOwnProperty.call(previousData, paneId)) return previousData;
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

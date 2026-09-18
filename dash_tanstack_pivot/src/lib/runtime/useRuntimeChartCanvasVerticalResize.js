import { useCallback, useEffect } from 'react';

import {
  CHART_PANEL_VERTICAL_CHROME_HEIGHT,
  DEFAULT_DOCKED_CHART_PANE_HEIGHT,
  MIN_DOCKED_CHART_PANE_HEIGHT,
  MIN_TABLE_PANEL_HEIGHT,
} from '../hooks/usePivotNormalization';

/** Owns the vertical resize lifecycle for docked chart panes. */
export function useRuntimeChartCanvasVerticalResize({
  chartCanvasLayoutRef,
  chartCanvasPanes,
  chartCanvasVerticalResizeRef,
  setChartCanvasPanes,
}) {
  const handleStartChartCanvasVerticalResize = useCallback((paneId, position, event) => {
    const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
    if (!containerRect || containerRect.height <= 0) return;
    const pane = chartCanvasPanes.find(candidate => candidate.id === paneId);
    if (!pane) return;
    const paneEl = chartCanvasLayoutRef.current.querySelector(`[data-docked-chart-pane="${paneId}"]`);
    const startHeight = paneEl ? paneEl.getBoundingClientRect().height : DEFAULT_DOCKED_CHART_PANE_HEIGHT;
    chartCanvasVerticalResizeRef.current = {
      startY: event.clientY,
      paneId,
      position,
      startHeight,
      containerHeight: containerRect.height
    };
  }, [chartCanvasLayoutRef, chartCanvasPanes, chartCanvasVerticalResizeRef]);

  useEffect(() => {
    const handleVerticalMove = event => {
      const resizeState = chartCanvasVerticalResizeRef.current;
      if (!resizeState) return;
      const deltaY = event.clientY - resizeState.startY;
      const sign = resizeState.position === 'top' ? 1 : -1;
      const nextHeight = Math.max(
        MIN_DOCKED_CHART_PANE_HEIGHT,
        Math.min(resizeState.containerHeight - MIN_TABLE_PANEL_HEIGHT, resizeState.startHeight + deltaY * sign)
      );
      const nextChartHeight = Math.max(180, Math.floor(nextHeight - CHART_PANEL_VERTICAL_CHROME_HEIGHT));
      setChartCanvasPanes(previousPanes => previousPanes.map(pane => pane.id === resizeState.paneId ? { ...pane, chartHeight: nextChartHeight } : pane));
    };
    const stopVerticalResize = () => {
      chartCanvasVerticalResizeRef.current = null;
    };
    window.addEventListener('mousemove', handleVerticalMove);
    window.addEventListener('mouseup', stopVerticalResize);
    window.addEventListener('blur', stopVerticalResize);
    return () => {
      window.removeEventListener('mousemove', handleVerticalMove);
      window.removeEventListener('mouseup', stopVerticalResize);
      window.removeEventListener('blur', stopVerticalResize);
    };
  }, [chartCanvasVerticalResizeRef, setChartCanvasPanes]);

  return { handleStartChartCanvasVerticalResize };
}

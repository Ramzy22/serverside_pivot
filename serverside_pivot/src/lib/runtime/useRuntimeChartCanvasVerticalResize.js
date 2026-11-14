import { useCallback, useEffect } from 'react';

import {
  CHART_PANEL_VERTICAL_CHROME_HEIGHT,
  DEFAULT_DOCKED_CHART_PANE_HEIGHT,
  MIN_DOCKED_CHART_PANE_HEIGHT,
  MIN_TABLE_PANEL_HEIGHT,
} from '../hooks/usePivotNormalization';

/** Owns the vertical resize lifecycle for docked chart panes.  @param {any} p0 @returns {any} */
export function useRuntimeChartCanvasVerticalResize({
  chartCanvasLayoutRef,
  chartCanvasPanes,
  chartCanvasVerticalResizeRef,
  setChartCanvasPanes,
}) {
  /** @type {(...args: any[]) => any} */
  const handleStartChartCanvasVerticalResize = useCallback((paneId, position, event) => {
    /** @type {any} */
    const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
    if (!containerRect || containerRect.height <= 0) return;
    /** @type {any} */
    const pane = chartCanvasPanes.find(/** @param {any} [candidate] @returns {any} */ candidate => candidate.id === paneId);
    if (!pane) return;
    /** @type {any} */
    const paneEl = chartCanvasLayoutRef.current.querySelector(`[data-docked-chart-pane="${paneId}"]`);
    /** @type {any} */
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
    /** @param {any} [event] @returns {any} */
    const handleVerticalMove = event => {
      const resizeState = chartCanvasVerticalResizeRef.current;
      if (!resizeState) return;
      /** @type {any} */
      const deltaY = event.clientY - resizeState.startY;
      /** @type {any} */
      const sign = resizeState.position === 'top' ? 1 : -1;
      /** @type {any} */
      const nextHeight = Math.max(
        MIN_DOCKED_CHART_PANE_HEIGHT,
        Math.min(resizeState.containerHeight - MIN_TABLE_PANEL_HEIGHT, resizeState.startHeight + deltaY * sign)
      );
      /** @type {any} */
      const nextChartHeight = Math.max(180, Math.floor(nextHeight - CHART_PANEL_VERTICAL_CHROME_HEIGHT));
      setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.map(/** @param {any} [pane] @returns {any} */ pane => pane.id === resizeState.paneId ? { ...pane, chartHeight: nextChartHeight } : pane));
    };
    /** @returns {any} */
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

import { useCallback, useEffect, useLayoutEffect } from 'react';

import {
  CHART_PANEL_VERTICAL_CHROME_HEIGHT,
  DEFAULT_CHART_GRAPH_HEIGHT,
  DEFAULT_FLOATING_CHART_PANEL_HEIGHT,
  clampFloatingChartRect,
} from '../hooks/usePivotNormalization';

/** Owns floating chart-panel and chart-canvas pane gestures plus bounds syncing. */
export function useRuntimeChartFloatingInteractions({
  chartCanvasFloatingDragRef,
  chartCanvasFloatingResizeRef,
  chartCanvasLayoutRef,
  chartCanvasPanes,
  chartLayoutRef,
  chartPanelFloating,
  chartPanelFloatingDragRef,
  chartPanelFloatingRect,
  chartPanelFloatingResizeRef,
  chartPanelGraphHeight,
  chartPanelWidth,
  clampChartPanelWidth,
  setChartCanvasPanes,
  setChartPanelFloating,
  setChartPanelFloatingRect,
  setChartPanelGraphHeight,
  setChartPanelWidth,
  updateChartCanvasPane,
}) {
  const handleToggleChartPanelFloating = useCallback(() => {
    setChartPanelFloating(previousFloating => {
      const nextFloating = !previousFloating;
      if (nextFloating) {
        const containerRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
        setChartPanelFloatingRect(previousRect => clampFloatingChartRect({
          left: previousRect && Number.isFinite(previousRect.left) ? previousRect.left : 24,
          top: previousRect && Number.isFinite(previousRect.top) ? previousRect.top : 24,
          width: previousRect && Number.isFinite(previousRect.width) ? previousRect.width : chartPanelWidth,
          height: previousRect && Number.isFinite(previousRect.height) ? previousRect.height : Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, chartPanelGraphHeight + CHART_PANEL_VERTICAL_CHROME_HEIGHT)
        }, containerRect));
      } else {
        setChartPanelWidth(previousWidth => clampChartPanelWidth(chartPanelFloatingRect && Number.isFinite(chartPanelFloatingRect.width) ? chartPanelFloatingRect.width : previousWidth));
      }
      return nextFloating;
    });
  }, [chartLayoutRef, chartPanelFloatingRect, chartPanelGraphHeight, chartPanelWidth, clampChartPanelWidth, setChartPanelFloating, setChartPanelFloatingRect, setChartPanelWidth]);

  const handleStartChartPanelFloatingDrag = useCallback(event => {
    if (!chartPanelFloating || !chartLayoutRef.current) return;
    event.preventDefault();
    event.stopPropagation();
    chartPanelFloatingDragRef.current = {
      startX: event.clientX,
      startY: event.clientY,
      rect: chartPanelFloatingRect
    };
  }, [chartLayoutRef, chartPanelFloating, chartPanelFloatingDragRef, chartPanelFloatingRect]);

  const handleStartChartPanelFloatingResize = useCallback((direction, event) => {
    if (!chartPanelFloating || !chartLayoutRef.current) return;
    event.preventDefault();
    event.stopPropagation();
    chartPanelFloatingResizeRef.current = {
      direction,
      startX: event.clientX,
      startY: event.clientY,
      rect: chartPanelFloatingRect
    };
  }, [chartLayoutRef, chartPanelFloating, chartPanelFloatingRect, chartPanelFloatingResizeRef]);

  const handleToggleChartCanvasPaneFloating = useCallback(paneId => {
    const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
    updateChartCanvasPane(paneId, pane => {
      const nextFloating = !pane.floating;
      const baseRect = pane.floatingRect || {};
      return {
        ...pane,
        floating: nextFloating,
        floatingRect: clampFloatingChartRect({
          left: Number.isFinite(baseRect.left) ? baseRect.left : 36,
          top: Number.isFinite(baseRect.top) ? baseRect.top : 36,
          width: Number.isFinite(baseRect.width) ? baseRect.width : pane.width,
          height: Number.isFinite(baseRect.height) ? baseRect.height : Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, (pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + CHART_PANEL_VERTICAL_CHROME_HEIGHT)
        }, containerRect)
      };
    });
  }, [chartCanvasLayoutRef, updateChartCanvasPane]);

  const handleStartChartCanvasPaneFloatingDrag = useCallback((paneId, event) => {
    if (!chartCanvasLayoutRef.current) return;
    event.preventDefault();
    event.stopPropagation();
    const targetPane = chartCanvasPanes.find(pane => pane.id === paneId);
    if (!targetPane || !targetPane.floating || targetPane.locked) return;
    chartCanvasFloatingDragRef.current = {
      paneId,
      startX: event.clientX,
      startY: event.clientY,
      rect: targetPane.floatingRect || clampFloatingChartRect({}, null)
    };
  }, [chartCanvasFloatingDragRef, chartCanvasLayoutRef, chartCanvasPanes]);

  const handleStartChartCanvasPaneFloatingResize = useCallback((paneId, direction, event) => {
    if (!chartCanvasLayoutRef.current) return;
    event.preventDefault();
    event.stopPropagation();
    const targetPane = chartCanvasPanes.find(pane => pane.id === paneId);
    if (!targetPane || !targetPane.floating || targetPane.locked) return;
    chartCanvasFloatingResizeRef.current = {
      paneId,
      direction,
      startX: event.clientX,
      startY: event.clientY,
      rect: targetPane.floatingRect || clampFloatingChartRect({}, null)
    };
  }, [chartCanvasFloatingResizeRef, chartCanvasLayoutRef, chartCanvasPanes]);

  useEffect(() => {
    const handlePointerMove = event => {
      if (chartPanelFloatingDragRef.current) {
        const dragState = chartPanelFloatingDragRef.current;
        const containerRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
        setChartPanelFloatingRect(clampFloatingChartRect({
          ...dragState.rect,
          left: dragState.rect.left + (event.clientX - dragState.startX),
          top: dragState.rect.top + (event.clientY - dragState.startY)
        }, containerRect));
        return;
      }
      if (chartPanelFloatingResizeRef.current) {
        const resizeState = chartPanelFloatingResizeRef.current;
        const containerRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
        const deltaX = event.clientX - resizeState.startX;
        const deltaY = event.clientY - resizeState.startY;
        const nextRectDraft = { ...resizeState.rect };
        if (resizeState.direction === 'right' || resizeState.direction === 'corner') nextRectDraft.width = resizeState.rect.width + deltaX;
        if (resizeState.direction === 'bottom' || resizeState.direction === 'corner') nextRectDraft.height = resizeState.rect.height + deltaY;
        const nextRect = clampFloatingChartRect(nextRectDraft, containerRect);
        setChartPanelFloatingRect(nextRect);
        setChartPanelWidth(nextRect.width);
        setChartPanelGraphHeight(previousHeight => {
          const targetHeight = Math.max(180, nextRect.height - CHART_PANEL_VERTICAL_CHROME_HEIGHT);
          return previousHeight === targetHeight ? previousHeight : targetHeight;
        });
        return;
      }
      if (chartCanvasFloatingDragRef.current) {
        const dragState = chartCanvasFloatingDragRef.current;
        const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
        updateChartCanvasPane(dragState.paneId, {
          floatingRect: clampFloatingChartRect({
            ...dragState.rect,
            left: dragState.rect.left + (event.clientX - dragState.startX),
            top: dragState.rect.top + (event.clientY - dragState.startY)
          }, containerRect)
        });
        return;
      }
      if (chartCanvasFloatingResizeRef.current) {
        const resizeState = chartCanvasFloatingResizeRef.current;
        const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
        const deltaX = event.clientX - resizeState.startX;
        const deltaY = event.clientY - resizeState.startY;
        const nextRectDraft = { ...resizeState.rect };
        if (resizeState.direction === 'right' || resizeState.direction === 'corner') nextRectDraft.width = resizeState.rect.width + deltaX;
        if (resizeState.direction === 'bottom' || resizeState.direction === 'corner') nextRectDraft.height = resizeState.rect.height + deltaY;
        const nextRect = clampFloatingChartRect(nextRectDraft, containerRect);
        updateChartCanvasPane(resizeState.paneId, pane => ({
          ...pane,
          width: nextRect.width,
          chartHeight: Math.max(180, nextRect.height - CHART_PANEL_VERTICAL_CHROME_HEIGHT),
          floatingRect: nextRect
        }));
      }
    };
    const stopFloatingInteraction = () => {
      chartPanelFloatingDragRef.current = null;
      chartPanelFloatingResizeRef.current = null;
      chartCanvasFloatingDragRef.current = null;
      chartCanvasFloatingResizeRef.current = null;
    };
    window.addEventListener('mousemove', handlePointerMove);
    window.addEventListener('mouseup', stopFloatingInteraction);
    window.addEventListener('mouseleave', stopFloatingInteraction);
    window.addEventListener('blur', stopFloatingInteraction);
    return () => {
      window.removeEventListener('mousemove', handlePointerMove);
      window.removeEventListener('mouseup', stopFloatingInteraction);
      window.removeEventListener('mouseleave', stopFloatingInteraction);
      window.removeEventListener('blur', stopFloatingInteraction);
    };
  }, [chartCanvasFloatingDragRef, chartCanvasFloatingResizeRef, chartCanvasLayoutRef, chartLayoutRef, chartPanelFloatingDragRef, chartPanelFloatingResizeRef, setChartPanelFloatingRect, setChartPanelGraphHeight, setChartPanelWidth, updateChartCanvasPane]);

  useLayoutEffect(() => {
    if (!chartPanelFloating) return undefined;
    const syncFloatingRect = () => {
      const containerRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
      setChartPanelFloatingRect(previousRect => {
        const nextRect = clampFloatingChartRect(previousRect, containerRect);
        return JSON.stringify(previousRect) === JSON.stringify(nextRect) ? previousRect : nextRect;
      });
    };
    syncFloatingRect();
    if (typeof ResizeObserver === 'undefined' || !chartLayoutRef.current) {
      window.addEventListener('resize', syncFloatingRect);
      return () => window.removeEventListener('resize', syncFloatingRect);
    }
    const observer = new ResizeObserver(syncFloatingRect);
    observer.observe(chartLayoutRef.current);
    return () => observer.disconnect();
  }, [chartLayoutRef, chartPanelFloating, setChartPanelFloatingRect]);

  useLayoutEffect(() => {
    const hasFloatingPane = chartCanvasPanes.some(pane => pane.floating);
    if (!hasFloatingPane) return undefined;
    const syncFloatingPanes = () => {
      const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
      setChartCanvasPanes(previousPanes => {
        let mutated = false;
        const nextPanes = previousPanes.map(pane => {
          if (!pane.floating) return pane;
          const nextRect = clampFloatingChartRect(pane.floatingRect, containerRect);
          const previousRect = pane.floatingRect || {};
          const sameRect = previousRect.left === nextRect.left && previousRect.top === nextRect.top && previousRect.width === nextRect.width && previousRect.height === nextRect.height;
          if (sameRect) return pane;
          mutated = true;
          return { ...pane, floatingRect: nextRect };
        });
        return mutated ? nextPanes : previousPanes;
      });
    };
    syncFloatingPanes();
    if (typeof ResizeObserver === 'undefined' || !chartCanvasLayoutRef.current) {
      window.addEventListener('resize', syncFloatingPanes);
      return () => window.removeEventListener('resize', syncFloatingPanes);
    }
    const observer = new ResizeObserver(syncFloatingPanes);
    observer.observe(chartCanvasLayoutRef.current);
    return () => observer.disconnect();
  }, [chartCanvasLayoutRef, chartCanvasPanes, setChartCanvasPanes]);

  return {
    handleStartChartCanvasPaneFloatingDrag,
    handleStartChartCanvasPaneFloatingResize,
    handleStartChartPanelFloatingDrag,
    handleStartChartPanelFloatingResize,
    handleToggleChartCanvasPaneFloating,
    handleToggleChartPanelFloating,
  };
}

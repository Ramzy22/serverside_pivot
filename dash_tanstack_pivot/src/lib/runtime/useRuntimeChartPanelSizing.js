import { useCallback, useEffect, useLayoutEffect } from 'react';

import { getChartPanelWidthBounds } from '../hooks/usePivotNormalization';

/** Owns docked chart-panel width limits, drag resizing, and responsive sizing. */
export function useRuntimeChartPanelSizing({
  chartLayoutRef,
  chartPanelFloating,
  chartPanelOpen,
  isChartPanelResizing,
  setChartPanelWidth,
  setIsChartPanelResizing,
  sidebarOpen,
}) {
  const clampChartPanelWidth = useCallback(requestedWidth => {
    const layoutWidth = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect().width : 0;
    const { minWidth, maxWidth } = getChartPanelWidthBounds(layoutWidth);
    return Math.max(minWidth, Math.min(maxWidth, requestedWidth));
  }, [chartLayoutRef]);

  useEffect(() => {
    if (!isChartPanelResizing) return undefined;
    const previousUserSelect = document.body.style.userSelect;
    const previousCursor = document.body.style.cursor;
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'col-resize';
    const handlePointerMove = event => {
      const layoutRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
      if (!layoutRect || layoutRect.width <= 0) return;
      setChartPanelWidth(clampChartPanelWidth(layoutRect.right - event.clientX));
    };
    const stopResize = () => setIsChartPanelResizing(false);
    window.addEventListener('mousemove', handlePointerMove);
    window.addEventListener('mouseup', stopResize);
    window.addEventListener('mouseleave', stopResize);
    window.addEventListener('blur', stopResize);
    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.style.cursor = previousCursor;
      window.removeEventListener('mousemove', handlePointerMove);
      window.removeEventListener('mouseup', stopResize);
      window.removeEventListener('mouseleave', stopResize);
      window.removeEventListener('blur', stopResize);
    };
  }, [chartLayoutRef, clampChartPanelWidth, isChartPanelResizing, setChartPanelWidth, setIsChartPanelResizing]);

  useLayoutEffect(() => {
    if (!chartPanelOpen || chartPanelFloating) return undefined;
    const syncWidthToLayout = () => {
      setChartPanelWidth(previousWidth => {
        const nextWidth = clampChartPanelWidth(previousWidth);
        return nextWidth === previousWidth ? previousWidth : nextWidth;
      });
    };
    syncWidthToLayout();
    if (typeof ResizeObserver === 'undefined' || !chartLayoutRef.current) {
      window.addEventListener('resize', syncWidthToLayout);
      return () => window.removeEventListener('resize', syncWidthToLayout);
    }
    const observer = new ResizeObserver(syncWidthToLayout);
    observer.observe(chartLayoutRef.current);
    return () => observer.disconnect();
  }, [chartLayoutRef, chartPanelFloating, chartPanelOpen, clampChartPanelWidth, setChartPanelWidth, sidebarOpen]);

  return { clampChartPanelWidth };
}

import { useCallback, useEffect } from 'react';

import { MIN_CHART_CANVAS_PANE_WIDTH, MIN_TABLE_PANEL_WIDTH } from '../hooks/usePivotNormalization';

/** Owns the horizontal resize lifecycle for the table and docked chart panes. */
export function useRuntimeChartCanvasResize({
  chartCanvasLayoutRef,
  chartCanvasPanes,
  chartCanvasResizeRef,
  chartLayoutRef,
  setChartCanvasPaneWidthHints,
  setChartCanvasPanes,
  setTableCanvasSize,
  tableCanvasSize,
}) {
  const handleStartChartCanvasResize = useCallback((leftKey, rightKey, event) => {
    const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
    if (!containerRect || containerRect.width <= 0) return;
    const resolvePaneWidthPx = paneKey => {
      if (paneKey === 'table') {
        const tableRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
        return tableRect && tableRect.width > 0 ? tableRect.width : MIN_TABLE_PANEL_WIDTH;
      }
      const paneEl = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.querySelector(`[data-docked-chart-pane="${paneKey}"]`) : null;
      const paneRect = paneEl ? paneEl.getBoundingClientRect() : null;
      return paneRect && paneRect.width > 0 ? paneRect.width : MIN_CHART_CANVAS_PANE_WIDTH;
    };
    const chartPaneSizes = chartCanvasPanes.reduce((acc, pane) => {
      acc[pane.id] = pane.size;
      return acc;
    }, {});
    chartCanvasResizeRef.current = {
      startX: event.clientX,
      leftKey,
      rightKey,
      containerWidth: containerRect.width,
      totalUnits: tableCanvasSize + chartCanvasPanes.reduce((sum, pane) => sum + pane.size, 0),
      leftSize: leftKey === 'table' ? tableCanvasSize : chartPaneSizes[leftKey] || 1,
      rightSize: rightKey === 'table' ? tableCanvasSize : chartPaneSizes[rightKey] || 1,
      leftWidthPx: resolvePaneWidthPx(leftKey),
      rightWidthPx: resolvePaneWidthPx(rightKey)
    };
  }, [chartCanvasLayoutRef, chartCanvasPanes, chartCanvasResizeRef, chartLayoutRef, tableCanvasSize]);

  useEffect(() => {
    const handlePointerMove = event => {
      const resizeState = chartCanvasResizeRef.current;
      if (!resizeState) return;
      const totalUnits = resizeState.totalUnits > 0 ? resizeState.totalUnits : 1;
      const pxToUnits = totalUnits / Math.max(1, resizeState.containerWidth);
      const leftMinPx = resizeState.leftKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
      const rightMinPx = resizeState.rightKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
      const leftMinUnits = leftMinPx * pxToUnits;
      const rightMinUnits = rightMinPx * pxToUnits;
      const deltaPx = event.clientX - resizeState.startX;
      const deltaUnits = deltaPx / Math.max(1, resizeState.containerWidth) * totalUnits;
      const nextLeftSize = Math.max(leftMinUnits, resizeState.leftSize + deltaUnits);
      const nextRightSize = Math.max(rightMinUnits, resizeState.rightSize - deltaUnits);
      const consumed = nextLeftSize + nextRightSize;
      const targetConsumed = resizeState.leftSize + resizeState.rightSize;
      const correction = targetConsumed > 0 ? targetConsumed / Math.max(consumed, 0.0001) : 1;
      const correctedLeft = nextLeftSize * correction;
      const correctedRight = nextRightSize * correction;
      const nextLeftWidthPx = Math.max(leftMinPx, resizeState.leftWidthPx + deltaPx);
      const nextRightWidthPx = Math.max(rightMinPx, resizeState.rightWidthPx - deltaPx);
      const consumedWidthPx = nextLeftWidthPx + nextRightWidthPx;
      const targetConsumedWidthPx = resizeState.leftWidthPx + resizeState.rightWidthPx;
      const widthCorrection = targetConsumedWidthPx > 0 ? targetConsumedWidthPx / Math.max(consumedWidthPx, 0.0001) : 1;
      const correctedLeftWidthPx = nextLeftWidthPx * widthCorrection;
      const correctedRightWidthPx = nextRightWidthPx * widthCorrection;
      resizeState.lastLeftWidthPx = correctedLeftWidthPx;
      resizeState.lastRightWidthPx = correctedRightWidthPx;
      if (resizeState.leftKey === 'table') {
        setTableCanvasSize(correctedLeft);
      } else {
        setChartCanvasPanes(previousPanes => previousPanes.map(pane => pane.id === resizeState.leftKey ? { ...pane, size: correctedLeft } : pane));
      }
      if (resizeState.rightKey === 'table') {
        setTableCanvasSize(correctedRight);
      } else {
        setChartCanvasPanes(previousPanes => previousPanes.map(pane => pane.id === resizeState.rightKey ? { ...pane, size: correctedRight } : pane));
      }
      setChartCanvasPaneWidthHints(previousHints => {
        if (!previousHints || typeof previousHints !== 'object') return previousHints;
        let nextHints = previousHints;
        let changed = false;
        const syncHint = (paneId, paneWidthPx) => {
          if (paneId === 'table' || !Object.prototype.hasOwnProperty.call(previousHints, paneId)) return;
          const nextWidthPx = Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(paneWidthPx));
          if (previousHints[paneId] === nextWidthPx) return;
          if (nextHints === previousHints) nextHints = { ...previousHints };
          nextHints[paneId] = nextWidthPx;
          changed = true;
        };
        syncHint(resizeState.leftKey, correctedLeftWidthPx);
        syncHint(resizeState.rightKey, correctedRightWidthPx);
        return changed ? nextHints : previousHints;
      });
    };
    const stopResize = () => {
      const resizeState = chartCanvasResizeRef.current;
      chartCanvasResizeRef.current = null;
      if (!resizeState) return;
      const syncCommittedWidthHints = () => {
        setChartCanvasPaneWidthHints(previousHints => {
          if (!previousHints || typeof previousHints !== 'object') return previousHints;
          let nextHints = previousHints;
          let changed = false;
          const syncHint = (paneId, paneWidthPx) => {
            if (paneId === 'table' || !Object.prototype.hasOwnProperty.call(previousHints, paneId)) return;
            if (!Number.isFinite(Number(paneWidthPx))) return;
            const nextWidthPx = Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(paneWidthPx)));
            if (previousHints[paneId] === nextWidthPx) return;
            if (nextHints === previousHints) nextHints = { ...previousHints };
            nextHints[paneId] = nextWidthPx;
            changed = true;
          };
          syncHint(resizeState.leftKey, resizeState.lastLeftWidthPx);
          syncHint(resizeState.rightKey, resizeState.lastRightWidthPx);
          return changed ? nextHints : previousHints;
        });
      };
      if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
        window.requestAnimationFrame(() => window.requestAnimationFrame(syncCommittedWidthHints));
        return;
      }
      syncCommittedWidthHints();
    };
    window.addEventListener('mousemove', handlePointerMove);
    window.addEventListener('mouseup', stopResize);
    window.addEventListener('blur', stopResize);
    return () => {
      window.removeEventListener('mousemove', handlePointerMove);
      window.removeEventListener('mouseup', stopResize);
      window.removeEventListener('blur', stopResize);
    };
  }, [chartCanvasResizeRef, setChartCanvasPaneWidthHints, setChartCanvasPanes, setTableCanvasSize]);

  return { handleStartChartCanvasResize };
}

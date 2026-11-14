import { useCallback, useEffect } from 'react';

import { MIN_CHART_CANVAS_PANE_WIDTH, MIN_TABLE_PANEL_WIDTH } from '../hooks/usePivotNormalization';

/** Owns the horizontal resize lifecycle for the table and docked chart panes.  @param {any} p0 @returns {any} */
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
  /** @type {(...args: any[]) => any} */
  const handleStartChartCanvasResize = useCallback((leftKey, rightKey, event) => {
    /** @type {any} */
    const containerRect = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.getBoundingClientRect() : null;
    if (!containerRect || containerRect.width <= 0) return;
    /** @param {any} [paneKey] @returns {any} */
    const resolvePaneWidthPx = paneKey => {
      if (paneKey === 'table') {
        /** @type {any} */
        const tableRect = chartLayoutRef.current ? chartLayoutRef.current.getBoundingClientRect() : null;
        return tableRect && tableRect.width > 0 ? tableRect.width : MIN_TABLE_PANEL_WIDTH;
      }
      /** @type {any} */
      const paneEl = chartCanvasLayoutRef.current ? chartCanvasLayoutRef.current.querySelector(`[data-docked-chart-pane="${paneKey}"]`) : null;
      /** @type {any} */
      const paneRect = paneEl ? paneEl.getBoundingClientRect() : null;
      return paneRect && paneRect.width > 0 ? paneRect.width : MIN_CHART_CANVAS_PANE_WIDTH;
    };
    /** @type {any} */
    const chartPaneSizes = chartCanvasPanes.reduce(/** @param {any} [acc] @param {any} [pane] @returns {any} */ (acc, pane) => {
      acc[pane.id] = pane.size;
      return acc;
    }, {});
    chartCanvasResizeRef.current = {
      startX: event.clientX,
      leftKey,
      rightKey,
      containerWidth: containerRect.width,
      totalUnits: tableCanvasSize + chartCanvasPanes.reduce(/** @param {any} [sum] @param {any} [pane] @returns {any} */ (sum, pane) => sum + pane.size, 0),
      leftSize: leftKey === 'table' ? tableCanvasSize : chartPaneSizes[leftKey] || 1,
      rightSize: rightKey === 'table' ? tableCanvasSize : chartPaneSizes[rightKey] || 1,
      leftWidthPx: resolvePaneWidthPx(leftKey),
      rightWidthPx: resolvePaneWidthPx(rightKey)
    };
  }, [chartCanvasLayoutRef, chartCanvasPanes, chartCanvasResizeRef, chartLayoutRef, tableCanvasSize]);

  useEffect(() => {
    /** @param {any} [event] @returns {any} */
    const handlePointerMove = event => {
      const resizeState = chartCanvasResizeRef.current;
      if (!resizeState) return;
      /** @type {any} */
      const totalUnits = resizeState.totalUnits > 0 ? resizeState.totalUnits : 1;
      /** @type {any} */
      const pxToUnits = totalUnits / Math.max(1, resizeState.containerWidth);
      /** @type {any} */
      const leftMinPx = resizeState.leftKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
      /** @type {any} */
      const rightMinPx = resizeState.rightKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
      /** @type {any} */
      const leftMinUnits = leftMinPx * pxToUnits;
      /** @type {any} */
      const rightMinUnits = rightMinPx * pxToUnits;
      /** @type {any} */
      const deltaPx = event.clientX - resizeState.startX;
      /** @type {any} */
      const deltaUnits = deltaPx / Math.max(1, resizeState.containerWidth) * totalUnits;
      /** @type {any} */
      const nextLeftSize = Math.max(leftMinUnits, resizeState.leftSize + deltaUnits);
      /** @type {any} */
      const nextRightSize = Math.max(rightMinUnits, resizeState.rightSize - deltaUnits);
      /** @type {any} */
      const consumed = nextLeftSize + nextRightSize;
      /** @type {any} */
      const targetConsumed = resizeState.leftSize + resizeState.rightSize;
      /** @type {any} */
      const correction = targetConsumed > 0 ? targetConsumed / Math.max(consumed, 0.0001) : 1;
      /** @type {any} */
      const correctedLeft = nextLeftSize * correction;
      /** @type {any} */
      const correctedRight = nextRightSize * correction;
      /** @type {any} */
      const nextLeftWidthPx = Math.max(leftMinPx, resizeState.leftWidthPx + deltaPx);
      /** @type {any} */
      const nextRightWidthPx = Math.max(rightMinPx, resizeState.rightWidthPx - deltaPx);
      /** @type {any} */
      const consumedWidthPx = nextLeftWidthPx + nextRightWidthPx;
      /** @type {any} */
      const targetConsumedWidthPx = resizeState.leftWidthPx + resizeState.rightWidthPx;
      /** @type {any} */
      const widthCorrection = targetConsumedWidthPx > 0 ? targetConsumedWidthPx / Math.max(consumedWidthPx, 0.0001) : 1;
      /** @type {any} */
      const correctedLeftWidthPx = nextLeftWidthPx * widthCorrection;
      /** @type {any} */
      const correctedRightWidthPx = nextRightWidthPx * widthCorrection;
      resizeState.lastLeftWidthPx = correctedLeftWidthPx;
      resizeState.lastRightWidthPx = correctedRightWidthPx;
      if (resizeState.leftKey === 'table') {
        setTableCanvasSize(correctedLeft);
      } else {
        setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.map(/** @param {any} [pane] @returns {any} */ pane => pane.id === resizeState.leftKey ? { ...pane, size: correctedLeft } : pane));
      }
      if (resizeState.rightKey === 'table') {
        setTableCanvasSize(correctedRight);
      } else {
        setChartCanvasPanes(/** @param {any} [previousPanes] @returns {any} */ previousPanes => previousPanes.map(/** @param {any} [pane] @returns {any} */ pane => pane.id === resizeState.rightKey ? { ...pane, size: correctedRight } : pane));
      }
      setChartCanvasPaneWidthHints(/** @param {any} [previousHints] @returns {any} */ previousHints => {
        if (!previousHints || typeof previousHints !== 'object') return previousHints;
        let nextHints = previousHints;
        let changed = false;
        /** @param {any} [paneId] @param {any} [paneWidthPx] @returns {any} */
        const syncHint = (paneId, paneWidthPx) => {
          if (paneId === 'table' || !Object.prototype.hasOwnProperty.call(previousHints, paneId)) return;
          /** @type {any} */
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
    /** @returns {any} */
    const stopResize = () => {
      const resizeState = chartCanvasResizeRef.current;
      chartCanvasResizeRef.current = null;
      if (!resizeState) return;
      /** @returns {any} */
      const syncCommittedWidthHints = () => {
        setChartCanvasPaneWidthHints(/** @param {any} [previousHints] @returns {any} */ previousHints => {
          if (!previousHints || typeof previousHints !== 'object') return previousHints;
          let nextHints = previousHints;
          let changed = false;
          /** @param {any} [paneId] @param {any} [paneWidthPx] @returns {any} */
          const syncHint = (paneId, paneWidthPx) => {
            if (paneId === 'table' || !Object.prototype.hasOwnProperty.call(previousHints, paneId)) return;
            if (!Number.isFinite(Number(paneWidthPx))) return;
            /** @type {any} */
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

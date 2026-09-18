import { useEffect, useLayoutEffect } from 'react';

import { TABLE_OVERLAY_CHART_PANE_ID } from '../hooks/usePivotNormalization';

/** Keeps chart payload hydration and data-commit profiling with transport. */
export function useRuntimeResponsePostCommit({
  activeChartRequestRef,
  chartData,
  completedChartRequestSignaturesRef,
  dataOffset,
  dataVersion,
  pendingDataProfilerCommitRef,
  pivotProfilerRef,
  profilerComponentIdRef,
  profilingEnabledRef,
  responseColumns,
  rowCount,
  setChartPaneDataById,
}) {
  useEffect(() => {
    if (!chartData || typeof chartData !== 'object') return;
    const paneId = chartData.paneId || TABLE_OVERLAY_CHART_PANE_ID;
    setChartPaneDataById(previousData => ({
      ...previousData,
      [paneId]: chartData
    }));
    if (chartData.requestSignature) {
      completedChartRequestSignaturesRef.current[paneId] = chartData.requestSignature;
    }
    if (activeChartRequestRef.current && activeChartRequestRef.current.paneId === paneId && (!chartData.requestSignature || activeChartRequestRef.current.signature === chartData.requestSignature)) {
      activeChartRequestRef.current = null;
    }
  }, [chartData]);
  useLayoutEffect(() => {
    const pendingCommit = pendingDataProfilerCommitRef.current;
    if (!pendingCommit || !pendingCommit.requestId || !profilingEnabledRef.current || !pivotProfilerRef.current) {
      return undefined;
    }
    if (Number.isFinite(pendingCommit.dataVersion) && Number.isFinite(Number(dataVersion)) && Number(dataVersion) < pendingCommit.dataVersion) {
      return undefined;
    }
    pendingDataProfilerCommitRef.current = null;
    const layoutCommittedAt = Date.now();
    pivotProfilerRef.current.resolve({
      requestId: pendingCommit.requestId,
      componentId: profilerComponentIdRef.current,
      kind: 'data',
      status: 'data',
      stateQueuedAt: pendingCommit.stateQueuedAt,
      layoutCommittedAt,
      committedAt: layoutCommittedAt,
      profile: pendingCommit.clientProfile ? { client: pendingCommit.clientProfile } : undefined,
      meta: {
        dataVersion: Number(dataVersion),
        rowCount,
        dataOffset,
        columnsCount: Array.isArray(responseColumns) ? responseColumns.length : 0
      }
    });
    return undefined;
  }, [dataOffset, dataVersion, responseColumns, rowCount]);
}

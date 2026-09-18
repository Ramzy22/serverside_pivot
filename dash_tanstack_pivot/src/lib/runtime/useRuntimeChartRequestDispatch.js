import { useEffect } from 'react';

/**
 * Owns the chart-request queue after the grid and chart models have produced
 * candidates. It deliberately receives only chart transport dependencies.
 */
export function useRuntimeChartRequestDispatch({ getState }) {
  useEffect(() => {
    const {
      activeChartRequestRef,
      chartRequestCandidates,
    } = getState();
    if (activeChartRequestRef.current && !chartRequestCandidates.some(candidate => candidate.paneId === activeChartRequestRef.current.paneId && candidate.signature === activeChartRequestRef.current.signature)) {
      activeChartRequestRef.current = null;
    }
  }, [getState]);
  useEffect(() => {
    const {
      abortGeneration,
      activeChartRequestRef,
      chartRequestCandidates,
      chartRequestSeqRef,
      clientInstanceRef,
      completedChartRequestSignaturesRef,
      emitRuntimeRequest,
      needsColSchema,
      requestVersionRef,
      serverSide,
      sessionIdRef,
      setPropsRef,
      stateEpoch,
      structuralInFlight,
      totalCenterCols,
    } = getState();
    if (!serverSide || structuralInFlight || !setPropsRef.current || needsColSchema || totalCenterCols === null) return;
    if (activeChartRequestRef.current) return;
    const nextCandidate = chartRequestCandidates.find(candidate => completedChartRequestSignaturesRef.current[candidate.paneId] !== candidate.signature);
    if (!nextCandidate) return;
    chartRequestSeqRef.current = Math.max(chartRequestSeqRef.current, requestVersionRef.current) + 1;
    activeChartRequestRef.current = {
      paneId: nextCandidate.paneId,
      signature: nextCandidate.signature
    };
    emitRuntimeRequest('chart', {
      ...nextCandidate.request,
      pane_id: nextCandidate.paneId,
      request_signature: nextCandidate.signature,
      version: chartRequestSeqRef.current,
      window_seq: chartRequestSeqRef.current,
      state_epoch: stateEpoch,
      session_id: sessionIdRef.current,
      client_instance: clientInstanceRef.current,
      abort_generation: abortGeneration,
      intent: 'chart'
    });
  }, [getState]);
}

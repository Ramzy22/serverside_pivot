import { useCallback } from 'react';

/** User recovery always derives a fresh request from the current viewport.  @param {any} p0 @returns {any} */
export function useRuntimeDataRetry({ runtimeDataError, setRuntimeDataError, requestVisibleViewportRefresh,
  requestVersionRef, stateEpochRef, clientInstanceRef }) {
  const failed = runtimeDataError?.failedRequest;
  /** @returns {any} */
  const isCurrent = () => Boolean(failed && Number.isFinite(failed.windowSeq)
    && failed.windowSeq === requestVersionRef.current
    && failed.stateEpoch === stateEpochRef.current
    && failed.clientInstance === clientInstanceRef.current);
  /** @type {(...args: any[]) => any} */
  const retryRuntimeData = useCallback(() => {
    // Read refs at event time as well: a click can have been queued before the
    // render that notices an ordinary request has superseded this failure.
    if (!runtimeDataError || runtimeDataError.retrying || !isCurrent()) return false;
    /** @type {any} */
    const dispatched = requestVisibleViewportRefresh(undefined, undefined, { retry: failed });
    if (dispatched) {
      setRuntimeDataError(/** @param {any} [previous] @returns {any} */ previous => previous?.failedRequest === failed ? { ...previous, retrying: true } : previous);
    }
    return dispatched;
  }, [requestVisibleViewportRefresh, runtimeDataError, setRuntimeDataError, requestVersionRef, stateEpochRef, clientInstanceRef]);
  return {
    retryRuntimeData,
    runtimeDataError: runtimeDataError && !isCurrent() ? { ...runtimeDataError, retrying: true } : runtimeDataError,
  };
}

import { useCallback } from 'react';

/** User recovery always derives a fresh request from the current viewport. */
export function useRuntimeDataRetry({ runtimeDataError, setRuntimeDataError, requestVisibleViewportRefresh }) {
  return useCallback(() => {
    if (!runtimeDataError || runtimeDataError.retrying) return false;
    const dispatched = requestVisibleViewportRefresh(undefined, undefined, { retry: true });
    if (dispatched) {
      setRuntimeDataError(previous => previous ? { retrying: true } : previous);
    }
    return dispatched;
  }, [requestVisibleViewportRefresh, runtimeDataError, setRuntimeDataError]);
}

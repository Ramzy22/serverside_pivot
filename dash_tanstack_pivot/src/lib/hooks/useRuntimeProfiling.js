import { useCallback } from 'react';

/**
 * Owns client-side request profiling callbacks.
 *
 * Keeping these callbacks with transport makes the transport lifecycle
 * explicit: queue, send, and response records use the same profiler refs and
 * cannot be created as an incidental side effect of field/edit interactions.
 */
export function useRuntimeProfiling({
    detailMode,
    pivotProfilerRef,
    profilerComponentIdRef,
    profilingEnabledRef,
    tableName,
    viewMode,
}) {
    const recordProfilerQueuedRequest = useCallback(requestMeta => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !requestMeta || !requestMeta.requestId) return;
        profiler.queue({
            requestId: requestMeta.requestId,
            componentId: profilerComponentIdRef.current,
            kind: 'data',
            queuedAt: requestMeta.queuedAt,
            emittedAt: requestMeta.emittedAt,
            status: 'queued',
            meta: {
                table: tableName || null,
                viewMode,
                reqStart: requestMeta.reqStart,
                reqEnd: requestMeta.reqEnd,
                colStart: requestMeta.colStart,
                colEnd: requestMeta.colEnd,
                stateEpoch: requestMeta.stateEpoch,
                abortGeneration: requestMeta.abortGeneration,
                windowSeq: requestMeta.version,
                visibleColumnCount: requestMeta.visibleColumnCount,
                requestedColumnCount: requestMeta.requestedColumnCount,
                columnDeltaCount: requestMeta.columnDeltaCount,
            },
        });
    }, [pivotProfilerRef, profilerComponentIdRef, profilingEnabledRef, tableName, viewMode]);

    const recordProfilerRequestStart = useCallback(runtimeRequest => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !runtimeRequest || !runtimeRequest.requestId) return;
        const requestPayload = runtimeRequest.payload && typeof runtimeRequest.payload === 'object'
            ? runtimeRequest.payload
            : {};
        profiler.begin({
            requestId: runtimeRequest.requestId,
            componentId: profilerComponentIdRef.current,
            kind: runtimeRequest.kind || 'data',
            emittedAt: Date.now(),
            status: 'sent',
            meta: {
                table: runtimeRequest.table || tableName || null,
                viewMode,
                detailMode,
                windowSeq: runtimeRequest.window_seq,
                stateEpoch: runtimeRequest.state_epoch,
                abortGeneration: runtimeRequest.abort_generation,
                cacheKey: runtimeRequest.cache_key || runtimeRequest.cacheKey
                    || requestPayload.cache_key || requestPayload.cacheKey || null,
            },
        });
    }, [detailMode, pivotProfilerRef, profilerComponentIdRef, profilingEnabledRef, tableName, viewMode]);

    const recordProfilerResponse = useCallback(response => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !response || typeof response !== 'object' || !response.requestId) return;
        const payload = response.payload && typeof response.payload === 'object' ? response.payload : {};
        const requestProfile = response.profile && response.profile.request && typeof response.profile.request === 'object'
            ? response.profile.request
            : {};
        const adapterProfile = response.profile && response.profile.adapter && typeof response.profile.adapter === 'object'
            ? response.profile.adapter
            : {};
        profiler.response({
            requestId: response.requestId,
            componentId: profilerComponentIdRef.current,
            kind: response.kind || 'data',
            status: response.status || null,
            responseReceivedAt: Date.now(),
            profile: response.profile,
            meta: {
                table: response.table || tableName || null,
                stateEpoch: response.stateEpoch !== undefined ? response.stateEpoch : response.state_epoch,
                windowSeq: response.windowSeq !== undefined ? response.windowSeq : response.window_seq,
                rowCount: payload.rowCount,
                dataVersion: payload.dataVersion,
                cacheKey: requestProfile.cacheKey || adapterProfile.responseCacheKey || null,
                cancellationOutcome: requestProfile.cancellationOutcome || null,
                lifecycleLane: requestProfile.lifecycleLane || null,
            },
        });
    }, [pivotProfilerRef, profilerComponentIdRef, profilingEnabledRef, tableName]);

    return {
        recordProfilerQueuedRequest,
        recordProfilerRequestStart,
        recordProfilerResponse,
    };
}

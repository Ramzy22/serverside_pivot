import { useCallback, useEffect, useRef } from 'react';
import { cloneSerializable } from './usePivotNormalization';
import { createRuntimeRequest } from '../runtime/runtimeContract';

/**
 * Owns the client-to-Dash runtime request envelope.
 *
 * This hook deliberately stops at prop dispatch. Response handling, payload
 * fetching, row-cache updates, and profiler response commits remain in their
 * existing owners so request transport does not become a second data model.
 */
export function useRuntimeTransport({
    clientInstanceRef,
    detailConfig,
    detailMode,
    profilingEnabledRef,
    recordProfilerRequestStart,
    sessionIdRef,
    setProps,
    tableName,
    treeConfig,
    viewMode,
}) {
    const normalizeOutgoingRuntimeRequest = useCallback((runtimeRequest) => {
        if (!runtimeRequest || typeof runtimeRequest !== 'object') return null;
        const payload = runtimeRequest.payload && typeof runtimeRequest.payload === 'object'
            ? runtimeRequest.payload
            : runtimeRequest;
        return createRuntimeRequest({
            ...runtimeRequest,
            payload: cloneSerializable(payload, payload),
        }, {
            tableName,
            sessionId: sessionIdRef.current,
            clientInstance: clientInstanceRef.current,
            viewMode,
            detailMode,
            treeConfig,
            detailConfig,
            profile: profilingEnabledRef.current,
        });
    }, [clientInstanceRef, detailConfig, detailMode, profilingEnabledRef, sessionIdRef, tableName, treeConfig, viewMode]);

    const dispatchSetProps = useCallback((nextProps) => {
        if (typeof setProps !== 'function') return;
        if (!nextProps || typeof nextProps !== 'object') {
            setProps(nextProps);
            return;
        }
        const runtimeRequest = normalizeOutgoingRuntimeRequest(nextProps.runtimeRequest);
        if (runtimeRequest) {
            if (typeof recordProfilerRequestStart === 'function') {
                recordProfilerRequestStart(runtimeRequest);
            }
            setProps({ ...nextProps, runtimeRequest });
            return;
        }
        setProps(nextProps);
    }, [normalizeOutgoingRuntimeRequest, recordProfilerRequestStart, setProps]);

    const setPropsRef = useRef(dispatchSetProps);
    useEffect(() => {
        setPropsRef.current = dispatchSetProps;
    }, [dispatchSetProps]);

    const emitRuntimeRequest = useCallback((kind, payload) => {
        if (!setPropsRef.current || !payload || typeof payload !== 'object') return;
        const runtimeRequest = normalizeOutgoingRuntimeRequest({ kind, payload });
        if (!runtimeRequest) return;
        setPropsRef.current({
            runtimeRequest,
        });
    }, [normalizeOutgoingRuntimeRequest]);

    return {
        dispatchSetProps,
        emitRuntimeRequest,
        normalizeOutgoingRuntimeRequest,
        setPropsRef,
    };
}

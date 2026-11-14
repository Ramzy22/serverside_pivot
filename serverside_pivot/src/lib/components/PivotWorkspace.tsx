import React, { useCallback, useEffect, useRef, useState } from 'react';

import { PivotRuntimeRoot } from '../runtime/PivotRuntimeRoot';
import {
    createRuntimeRequest,
    isRuntimeRequestV1,
    type RuntimeRequestV1,
    type RuntimeResponseV1,
} from '../runtime/runtimeContract';
import type { PivotTransport } from '../runtime/httpTransport';
import { ClientPivotTransport, resolveExecutionMode } from '../runtime/client/index';
import type { PivotRuntimeProps } from '../runtime/runtimeDomains';

/**
 * Framework-neutral React API. A host supplies the V1 transport; it never has
 * to emulate Dash's callback graph or serialize row blocks through its state.
 */
export interface PivotWorkspaceProps extends Omit<
    PivotRuntimeProps,
    'setProps' | 'runtimeRequest' | 'runtimeResponse'
> {
    readonly transport?: PivotTransport;
    readonly onStateChange?: (state: Readonly<Partial<PivotRuntimeProps>>) => void;
}

// These are the controlled configuration values that are both meaningful to a
// host application and safe to send over a host state channel.  In particular,
// pane dimensions are intentionally absent: they change continuously while a
// user drags a resize handle and must remain local to the browser.
const DURABLE_HOST_STATE_KEYS = new Set<keyof PivotRuntimeProps>([
    'rowFields',
    'colFields',
    'valConfigs',
    'filters',
    'sorting',
    'expanded',
    'immersiveMode',
    'showRowTotals',
    'showColTotals',
    'showSubtotals',
    'viewMode',
    'detailMode',
    'treeConfig',
    'detailConfig',
    'columnPinning',
    'rowPinning',
    'columnVisibility',
    'columnOrder',
    'columnSizing',
    'decimalPlaces',
    'defaultValueFormat',
    'numberGroupSeparator',
    'customDimensions',
    'measureAxis',
    'reportDef',
    'savedReports',
    'activeReportId',
]);

const selectDurableHostState = (
    nextProps: Partial<PivotRuntimeProps>,
): Partial<PivotRuntimeProps> => Object.fromEntries(
    Object.entries(nextProps).filter(([key]) => (
        DURABLE_HOST_STATE_KEYS.has(key as keyof PivotRuntimeProps)
    )),
) as Partial<PivotRuntimeProps>;

const runtimeError = (request: RuntimeRequestV1, error: unknown): RuntimeResponseV1 => ({
    schemaVersion: 1,
    kind: request.kind,
    status: 'error',
    requestId: request.requestId,
    table: request.table,
    clientInstance: request.clientInstance,
    stateEpoch: request.stateEpoch,
    windowSeq: request.windowSeq,
    payload: {},
    message: error instanceof Error ? error.message : 'Runtime request failed.',
});

const runtimeDebug = (event: string, detail: Record<string, unknown>): void => {
    if (typeof window === 'undefined') return;
    const debugWindow = window as Window & { __PIVOT_DEBUG__?: boolean };
    if (debugWindow.__PIVOT_DEBUG__ !== true) return;
    // This is intentionally concise: it lets host applications diagnose the
    // direct transport without dumping row payloads into the browser console.
    console.info(`[serverside-pivot] ${event}`, detail);
};

/**
 * Render the pivot workspace in a normal React application.
 *
 * `PivotRuntimeRoot` remains the shared presentation/runtime implementation
 * during the migration, but its Dash-shaped `setProps` boundary is fulfilled
 * locally and runtime requests travel only through `transport`.
 */
export function PivotWorkspace({ transport, onStateChange, ...initialProps }: PivotWorkspaceProps) {
    const [runtimeProps, setRuntimeProps] = useState<PivotRuntimeProps>(() => initialProps);
    const clientTransportRef = useRef<ClientPivotTransport | null>(null);
    const resolvedMode = resolveExecutionMode(runtimeProps, {
        hasExternalServerTransport: Boolean(transport),
        performanceConfig: runtimeProps.performanceConfig as Record<string, unknown> | null | undefined,
    });
    const isPassedClientTransport = transport instanceof ClientPivotTransport
        || Boolean(transport && typeof (transport as { setData?: unknown }).setData === 'function');
    const useClientTransport = !transport || (!isPassedClientTransport && resolvedMode === 'client');
    const warnedExternalTransportRef = useRef(false);
    useEffect(() => {
        if (transport && !isPassedClientTransport && useClientTransport && !warnedExternalTransportRef.current) {
            warnedExternalTransportRef.current = true;
            console.warn(
                '[serverside-pivot] PivotWorkspace is running in client execution mode; ignoring the provided non-ClientPivotTransport `transport` and using internal ClientPivotTransport. Set `serverSide={true}` or `performanceConfig={{ executionMode: "server" }}` to use your external transport, or pass a `ClientPivotTransport` instance.'
            );
        }
    }, [transport, isPassedClientTransport, useClientTransport]);
    const duckdbBundleUrl = (runtimeProps.duckdbBundleUrl as string | undefined)
        || (runtimeProps.performanceConfig && typeof runtimeProps.performanceConfig === 'object'
            ? (runtimeProps.performanceConfig as { duckdbBundleUrl?: string }).duckdbBundleUrl
            : undefined);
    if (useClientTransport && !clientTransportRef.current) {
        clientTransportRef.current = new ClientPivotTransport({
            ...(runtimeProps.performanceConfig && typeof runtimeProps.performanceConfig === 'object' ? runtimeProps.performanceConfig : {}),
            ...(duckdbBundleUrl ? { duckdbBundleUrl } : {}),
            data: Array.isArray(runtimeProps.data) ? runtimeProps.data as Array<Record<string, unknown>> : [],
            tableName: runtimeProps.table || 'client_data',
        });
    }
    const activeTransport = (useClientTransport && clientTransportRef.current)
        ? clientTransportRef.current
        : (transport as PivotTransport);
    useEffect(() => {
        if (Array.isArray(runtimeProps.data) && typeof (activeTransport as { setData?: unknown }).setData === 'function') {
            const clientTransport = activeTransport as ClientPivotTransport & { getData?: () => unknown };
            if (typeof clientTransport.getData === 'function' && clientTransport.getData() === runtimeProps.data) {
                return;
            }
            clientTransport.setData(runtimeProps.data as Array<Record<string, unknown>>);
        }
    }, [activeTransport, runtimeProps.data]);
    useEffect(() => {
        if (duckdbBundleUrl && typeof (activeTransport as { updateConfig?: unknown }).updateConfig === 'function') {
            (activeTransport as ClientPivotTransport).updateConfig({ duckdbBundleUrl });
        }
    }, [activeTransport, duckdbBundleUrl]);
    const transportRef = useRef(activeTransport);
    const mountedRef = useRef(true);
    const pendingRequestsRef = useRef(new Set<AbortController>());
    // The public host props are JSON-shaped.  A stable serialization lets a
    // host update controlled configuration without feeding local runtime
    // mutations back into this effect after every response.
    const initialPropsSignature = JSON.stringify(initialProps);

    useEffect(() => {
        setRuntimeProps(previous => ({ ...previous, ...initialProps }));
    }, [initialPropsSignature, initialProps.rowHeaderLabel]); // eslint-disable-line react-hooks/exhaustive-deps

    useEffect(() => {
        transportRef.current = activeTransport;
    }, [activeTransport]);

    useEffect(() => {
        mountedRef.current = true;
        return () => {
            mountedRef.current = false;
            // The host owns transport; cancel only work dispatched by this consumer.
            pendingRequestsRef.current.forEach(controller => controller.abort());
            pendingRequestsRef.current.clear();
        };
    }, []);

    const dispatch = useCallback((candidate: unknown) => {
        const request = isRuntimeRequestV1(candidate)
            ? candidate
            : createRuntimeRequest(
                (candidate && typeof candidate === 'object' ? candidate : {}) as Record<string, unknown>,
                { tableName: runtimeProps.table },
            );
        runtimeDebug('runtime request', {
            requestId: request.requestId,
            kind: request.kind,
            table: request.table,
            intent: request.payload.intent,
            start: request.payload.start,
            end: request.payload.end,
            rowFields: request.state?.rowFields,
            colFields: request.state?.colFields,
            measureCount: request.state?.valConfigs?.length,
        });
        const controller = new AbortController();
        pendingRequestsRef.current.add(controller);
        transportRef.current.request(request, { signal: controller.signal }).then(response => {
            if (controller.signal.aborted) return;
            if (!mountedRef.current) return;
            runtimeDebug('runtime response', {
                requestId: response.requestId,
                kind: response.kind,
                status: response.status,
                rowCount: response.payload.rowCount,
                rows: Array.isArray(response.payload.data) ? response.payload.data.length : 0,
                columns: Array.isArray(response.payload.columns) ? response.payload.columns.length : 0,
                message: response.message,
            });
            setRuntimeProps(previous => ({ ...previous, runtimeResponse: response }));
        }).catch(error => {
            // A superseded viewport request is expected and must not replace a
            // fresher response with an error state.
            if (controller.signal.aborted) return;
            if (error && typeof error === 'object' && (error as { name?: string }).name === 'AbortError') return;
            if (!mountedRef.current) return;
            runtimeDebug('runtime error', {
                requestId: request.requestId,
                kind: request.kind,
                message: error instanceof Error ? error.message : String(error),
            });
            setRuntimeProps(previous => ({ ...previous, runtimeResponse: runtimeError(request, error) }));
        }).finally(() => {
            pendingRequestsRef.current.delete(controller);
        });
    }, [runtimeProps.table]);

    const publish = useCallback((nextProps: Partial<PivotRuntimeProps>) => {
        setRuntimeProps(previous => ({ ...previous, ...nextProps }));
        // Viewport traffic must stay inside the direct HTTP transport. Hosts
        // with rerun semantics (notably Streamlit) only receive durable
        // configuration changes. Resize/layout telemetry never crosses this
        // boundary. Invoke the callback outside React's state updater: render
        // scheduling may replay an updater in development, while host
        // notifications must be once.
        const durableState = selectDurableHostState(nextProps);
        if (onStateChange && Object.keys(durableState).length > 0) {
            onStateChange(durableState);
        }
        if (nextProps.runtimeRequest) dispatch(nextProps.runtimeRequest);
    }, [dispatch, onStateChange]);

    return <PivotRuntimeRoot {...runtimeProps} setProps={publish} payloadTransport={activeTransport} />;
}

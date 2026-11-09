import { useRuntimeProfiling } from '../hooks/useRuntimeProfiling';
import { useRuntimeDataCommit } from '../hooks/useRuntimeDataCommit';
import { useRuntimeTransport } from '../hooks/useRuntimeTransport';

import type { RuntimeRequestV1 } from './runtimeContract';
import type { CoreRuntimeDomain, ExportJobController, PivotObject, PivotRuntimeProps, TransportRuntimeDomain, ViewportRuntimeDomain, WorkspaceRuntimeDomain } from './runtimeDomains';
import { useRuntimeChartRequestDispatch } from './useRuntimeChartRequestDispatch';
import { useRuntimeResponseLifecycle } from './useRuntimeResponseLifecycle';
import { useRuntimeResponsePostCommit } from './useRuntimeResponsePostCommit';
import type { PivotTransport, RuntimePayloadTransport } from './httpTransport';

const asObject = (value: unknown): PivotObject => value && typeof value === 'object' && !Array.isArray(value)
    ? value as PivotObject
    : {};
const freeze = <T extends object>(value: T): Readonly<T> => Object.freeze(value);

type RuntimeCallback = (...args: unknown[]) => unknown;
type RuntimeRef<T = unknown> = { current: T };
type RuntimeResponseData = PivotObject;

/**
 * The remaining setup implementation is JavaScript while its state groups are
 * migrated. Transport reaches it only through this explicit response port;
 * none of these callbacks are exposed to renderers or other domains.
 */
interface CoreTransportState extends CoreRuntimeDomain {
    readonly tableName?: string;
    readonly viewMode?: string;
    readonly detailMode?: string;
    readonly setProps?: (...args: unknown[]) => unknown;
    readonly pivotProfilerRef: RuntimeRef<object | null>;
    readonly profilerComponentIdRef: RuntimeRef<string | null>;
    readonly profilingEnabledRef: RuntimeRef<boolean>;
    readonly clientInstanceRef: RuntimeRef<string | null>;
    readonly sessionIdRef: RuntimeRef<string | null>;
    readonly setPropsRef: RuntimeRef<unknown>;
    readonly detailConfig?: PivotObject | null;
    readonly treeConfig?: PivotObject | null;
    readonly runtimeRequest?: PivotObject;
    readonly runtimeResponse?: TransportRuntimeDomain['response']['current'];
    readonly isRequestPending?: boolean;
    readonly clientRuntimeActive?: boolean;
    readonly clientTransportRef?: RuntimeRef<PivotTransport | null>;
    readonly setLocalRuntimeResponse?: RuntimeCallback;
}

interface TransportResponseState extends CoreTransportState {
    readonly layoutRevisionRef: RuntimeRef<string | null>;
    readonly layoutAcknowledgedManifestRef: RuntimeRef<string>;
    readonly pivotLayoutOrderRef: RuntimeRef<readonly string[]>;
    readonly latestLayoutSentManifestRef: RuntimeRef<readonly string[] | null>;
    readonly activeChartRequestRef: RuntimeRef<PivotObject | null>;
    readonly activeExportRequestIdRef: RuntimeRef<string | null>;
    readonly exportJobControllerRef: RuntimeRef<ExportJobController | null>;
    readonly activeRuntimePayloadFetchRef: RuntimeRef<string | null>;
    readonly cancelledExportRequestIdsRef: RuntimeRef<ReadonlySet<string>>;
    readonly handledRuntimeResponseKeysRef: RuntimeRef<ReadonlySet<string>>;
    readonly pendingCopyIntentRef: RuntimeRef<PivotObject | null>;
    readonly pendingDataProfilerCommitRef: RuntimeRef<PivotObject | null>;
    readonly pendingServerFilterOptionsRef: RuntimeRef<PivotObject | null>;
    readonly chartData?: RuntimeResponseData;
    readonly dataOffset?: number;
    readonly dataVersion?: number;
    readonly responseColumns?: readonly PivotObject[];
    readonly rowCount?: number;
    readonly stateEpochRef: RuntimeRef<number>;
    readonly latestDataVersionRef: RuntimeRef<number>;
    readonly requestVersionRef: RuntimeRef<number>;
    readonly completedChartRequestSignaturesRef: RuntimeRef<Readonly<Record<string, string>>>;
    readonly clearPendingTransactionHistoryRequest: RuntimeCallback;
    readonly copyRichToClipboard: RuntimeCallback;
    readonly emitEditLifecycleEvent: RuntimeCallback;
    readonly finalizeTransactionHistoryResponse: RuntimeCallback;
    readonly hydrateVisibleEditOverlay: RuntimeCallback;
    readonly reconcileOptimisticCellValuesWithPayload: RuntimeCallback;
    readonly resolvePendingRequest: RuntimeCallback;
    readonly scheduleSilentViewportRefresh: RuntimeCallback;
    readonly setEditorOptionsLoadingState: RuntimeCallback;
    readonly setIsCopying: RuntimeCallback;
    readonly setIsExporting: RuntimeCallback;
    readonly setPartModal: RuntimeCallback;
    readonly setRuntimeDataError: RuntimeCallback;
    readonly setTransportChartDataState: RuntimeCallback;
    readonly setTransportDataState: RuntimeCallback;
    readonly setTransportFilterOptionMetaState: RuntimeCallback;
    readonly setTransportFilterOptionsState: RuntimeCallback;
    readonly setFormulaErrors: RuntimeCallback;
    readonly setChartPaneDataById: RuntimeCallback;
    readonly showNotification: RuntimeCallback;
    readonly triggerExportDownload: RuntimeCallback;
}

/**
 * Chart candidates are produced by the workspace during render. Transport
 * reads this deliberately narrow port from its post-render effect, rather
 * than making renderer state part of the transport domain's public surface.
 */
const responseCallback = <Name extends keyof TransportResponseState>(
    state: TransportResponseState,
    name: Name,
): RuntimeCallback => (...args) => {
    const callback = state[name];
    return typeof callback === 'function' ? callback(...args) : undefined;
};

/** Request envelopes, response lifecycle hooks, and profiler callbacks. */
export function useTransportRuntime(
    core: CoreRuntimeDomain,
): TransportRuntimeDomain {
    const runtime = core as CoreTransportState;
    const tableName = core.configuration.tableName ?? runtime.tableName;
    const viewMode = core.configuration.viewMode ?? runtime.viewMode;
    const detailMode = core.configuration.detailMode ?? runtime.detailMode;
    const rawSetProps = core.refs.setProps.current ?? runtime.setProps;
    const setProps = runtime.clientRuntimeActive && runtime.clientTransportRef
        ? (nextProps: unknown) => {
            const candidate = asObject(nextProps);
            if (candidate.runtimeRequest && runtime.clientTransportRef?.current) {
                const reqObj = asObject(candidate.runtimeRequest);
                const stateObj = asObject(reqObj.state);
                const mergedRequest = {
                    ...reqObj,
                    state: {
                        rowFields: core.state.rowFields,
                        colFields: core.state.colFields,
                        valConfigs: core.state.valConfigs,
                        filters: core.state.filters,
                        sorting: core.state.sorting,
                        expanded: core.state.expanded,
                        viewMode,
                        detailMode,
                        ...stateObj,
                    },
                } as unknown as RuntimeRequestV1;
                void runtime.clientTransportRef.current.request(mergedRequest).then(response => {
                    if (typeof runtime.setLocalRuntimeResponse === 'function') {
                        runtime.setLocalRuntimeResponse(response);
                    }
                });
                const rest = { ...candidate };
                delete rest.runtimeRequest;
                if (Object.keys(rest).length > 0 && typeof rawSetProps === 'function') {
                    rawSetProps(rest);
                }
                return;
            }
            if (typeof rawSetProps === 'function') {
                (rawSetProps as (...args: unknown[]) => unknown)(nextProps);
            }
        }
        : rawSetProps;
    const profiling = useRuntimeProfiling({
        detailMode,
        pivotProfilerRef: runtime.pivotProfilerRef,
        profilerComponentIdRef: runtime.profilerComponentIdRef,
        profilingEnabledRef: runtime.profilingEnabledRef,
        tableName,
        viewMode,
    });
    const transport = useRuntimeTransport({
        clientInstanceRef: runtime.clientInstanceRef,
        detailConfig: runtime.detailConfig,
        detailMode,
        profilingEnabledRef: runtime.profilingEnabledRef,
        recordProfilerRequestStart: profiling.recordProfilerRequestStart,
        sessionIdRef: runtime.sessionIdRef,
        setProps,
        tableName,
        treeConfig: runtime.treeConfig,
        viewMode,
    });
    // Core setup effects and later viewport hooks share this stable ref. Its
    // identity stays core-owned, while its callback is upgraded to dispatch
    // versioned request envelopes through the transport domain.
    runtime.setPropsRef.current = transport.dispatchSetProps;
    const emit = typeof transport.emitRuntimeRequest === 'function'
        ? transport.emitRuntimeRequest
        : () => undefined;
    const publishHostState = typeof transport.dispatchSetProps === 'function'
        ? transport.dispatchSetProps
        : () => undefined;
    const domain = freeze<TransportRuntimeDomain>({
        request: freeze({
            current: asObject(runtime.runtimeRequest),
            emit,
            normalize: typeof transport.normalizeOutgoingRuntimeRequest === 'function'
                ? transport.normalizeOutgoingRuntimeRequest as (request: PivotObject) => RuntimeRequestV1 | null
                : () => null,
        }),
        response: freeze({
            current: runtime.runtimeResponse as TransportRuntimeDomain['response']['current'],
            pending: runtime.isRequestPending === true,
        }),
        profiling: freeze({
            enabled: runtime.profilingEnabledRef?.current === true,
            recordQueued: typeof profiling.recordProfilerQueuedRequest === 'function'
                ? profiling.recordProfilerQueuedRequest
                : () => undefined,
            recordRequestStart: typeof profiling.recordProfilerRequestStart === 'function'
                ? profiling.recordProfilerRequestStart
                : () => undefined,
            recordResponse: typeof profiling.recordProfilerResponse === 'function'
                ? profiling.recordProfilerResponse
                : () => undefined,
        }),
        port: freeze({ emit }),
        host: freeze({ publish: publishHostState }),
    });
    return domain;
}

/** Response application runs after viewport/workspace have published their immutable stage results. */
export function useTransportResponseRuntime(
    core: CoreRuntimeDomain,
    transport: TransportRuntimeDomain,
    viewport: ViewportRuntimeDomain,
    payloadTransport?: RuntimePayloadTransport,
): void {
    const runtime = core as TransportResponseState;
    const response = runtime;
    const commit = useRuntimeDataCommit({
        clientInstanceRef: runtime.clientInstanceRef,
        stateEpochRef: runtime.stateEpochRef,
        latestDataVersionRef: runtime.latestDataVersionRef,
        requestVersionRef: runtime.requestVersionRef,
        pendingDataProfilerCommitRef: response.pendingDataProfilerCommitRef,
        finalizeTransactionHistoryResponse: responseCallback(response, 'finalizeTransactionHistoryResponse'),
        hydrateVisibleEditOverlay: responseCallback(response, 'hydrateVisibleEditOverlay'),
        reconcileOptimisticCellValuesWithPayload: responseCallback(response, 'reconcileOptimisticCellValuesWithPayload'),
        setFormulaErrors: responseCallback(response, 'setFormulaErrors'),
        setTransportDataState: responseCallback(response, 'setTransportDataState'),
        layoutRevisionRef: runtime.layoutRevisionRef,
        layoutAcknowledgedManifestRef: runtime.layoutAcknowledgedManifestRef,
        pivotLayoutOrderRef: runtime.pivotLayoutOrderRef,
        latestLayoutSentManifestRef: runtime.latestLayoutSentManifestRef,
    });
    useRuntimeResponseLifecycle({
        payloadTransport: payloadTransport ?? (runtime.clientRuntimeActive ? runtime.clientTransportRef?.current ?? undefined : undefined),
        activeChartRequestRef: response.activeChartRequestRef,
        activeExportRequestIdRef: response.activeExportRequestIdRef,
        exportJobControllerRef: response.exportJobControllerRef,
        activeRuntimePayloadFetchRef: response.activeRuntimePayloadFetchRef,
        cancelledExportRequestIdsRef: response.cancelledExportRequestIdsRef,
        clearPendingTransactionHistoryRequest: responseCallback(response, 'clearPendingTransactionHistoryRequest'),
        clientInstanceRef: runtime.clientInstanceRef,
        commitRuntimeDataPayload: commit.commitRuntimeDataPayload,
        copyRichToClipboard: viewport.delivery.copy,
        emitEditLifecycleEvent: responseCallback(response, 'emitEditLifecycleEvent'),
        finalizeTransactionHistoryResponse: responseCallback(response, 'finalizeTransactionHistoryResponse'),
        handledRuntimeResponseKeysRef: response.handledRuntimeResponseKeysRef,
        layoutAcknowledgedManifestRef: runtime.layoutAcknowledgedManifestRef,
        hydrateVisibleEditOverlay: responseCallback(response, 'hydrateVisibleEditOverlay'),
        pendingCopyIntentRef: response.pendingCopyIntentRef,
        pendingDataProfilerCommitRef: response.pendingDataProfilerCommitRef,
        pendingServerFilterOptionsRef: response.pendingServerFilterOptionsRef,
        pivotProfilerRef: runtime.pivotProfilerRef,
        profilerComponentIdRef: runtime.profilerComponentIdRef,
        profilingEnabledRef: runtime.profilingEnabledRef,
        reconcileOptimisticCellValuesWithPayload: responseCallback(response, 'reconcileOptimisticCellValuesWithPayload'),
        recordProfilerResponse: transport.profiling.recordResponse,
        requestVersionRef: runtime.requestVersionRef,
        resolvePendingRequest: responseCallback(response, 'resolvePendingRequest'),
        runtimePayloadCommitIsCurrent: commit.runtimePayloadCommitIsCurrent,
        runtimeResponse: runtime.runtimeResponse,
        scheduleSilentViewportRefresh: responseCallback(response, 'scheduleSilentViewportRefresh'),
        setEditorOptionsLoadingState: responseCallback(response, 'setEditorOptionsLoadingState'),
        setIsCopying: responseCallback(response, 'setIsCopying'),
        setIsExporting: responseCallback(response, 'setIsExporting'),
        setPartModal: responseCallback(response, 'setPartModal'),
        setRuntimeDataError: responseCallback(response, 'setRuntimeDataError'),
        setTransportChartDataState: responseCallback(response, 'setTransportChartDataState'),
        setTransportDataState: responseCallback(response, 'setTransportDataState'),
        setTransportFilterOptionMetaState: responseCallback(response, 'setTransportFilterOptionMetaState'),
        setTransportFilterOptionsState: responseCallback(response, 'setTransportFilterOptionsState'),
        showNotification: responseCallback(response, 'showNotification'),
        stateEpochRef: runtime.stateEpochRef,
        triggerExportDownload: viewport.delivery.download,
    });
    useRuntimeResponsePostCommit({
        activeChartRequestRef: response.activeChartRequestRef,
        chartData: response.chartData,
        completedChartRequestSignaturesRef: runtime.completedChartRequestSignaturesRef,
        dataOffset: response.dataOffset,
        dataVersion: response.dataVersion,
        pendingDataProfilerCommitRef: response.pendingDataProfilerCommitRef,
        pivotProfilerRef: runtime.pivotProfilerRef,
        profilerComponentIdRef: runtime.profilerComponentIdRef,
        profilingEnabledRef: runtime.profilingEnabledRef,
        responseColumns: response.responseColumns,
        rowCount: response.rowCount,
        setChartPaneDataById: responseCallback(response, 'setChartPaneDataById'),
    });
}

/**
 * Chart candidates exist only after workspace composition. Keeping this hook
 * in transport preserves request ownership while letting workspace return an
 * immutable workspace result instead of mutating core setup state.
 */
export function useTransportChartRequestDispatch(
    workspace: WorkspaceRuntimeDomain,
    transport: TransportRuntimeDomain,
): void {
    const emit = transport.port.emit;
    useRuntimeChartRequestDispatch({
        getState: () => {
            const chart = workspace.chartRequest;
            return {
                abortGeneration: chart.abortGeneration,
                activeChartRequestRef: chart.activeRequest,
                chartRequestCandidates: chart.candidates,
                chartRequestSeqRef: chart.requestSequence,
                clientInstanceRef: chart.clientInstance,
                completedChartRequestSignaturesRef: chart.completedSignatures,
                emitRuntimeRequest: emit,
                needsColSchema: chart.needsColumnSchema,
                requestVersionRef: chart.requestVersion,
                serverSide: chart.serverSide,
                sessionIdRef: chart.session,
                setPropsRef: chart.setProps,
                stateEpoch: chart.stateEpoch,
                structuralInFlight: chart.structuralInFlight,
                totalCenterCols: chart.totalCenterColumns,
            };
        },
    });
}

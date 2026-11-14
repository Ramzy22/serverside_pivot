import { useCallback } from 'react';
import { coerceTransportNumber, normalizeRuntimeDataEnvelope } from './usePivotNormalization';

/**
 * Owns freshness checks and atomic state commits for complete runtime-data
 * envelopes. Viewport-cache and request scheduling remain with their existing
 * server-side owners.
  @param {any} p0 @returns {any} */
export function useRuntimeDataCommit({
    clientInstanceRef,
    stateEpochRef,
    latestDataVersionRef,
    requestVersionRef = /** @type {{ current: number } | null} */ (null),
    pendingDataProfilerCommitRef,
    finalizeTransactionHistoryResponse,
    hydrateVisibleEditOverlay,
    reconcileOptimisticCellValuesWithPayload,
    setFormulaErrors,
    setTransportDataState,
    layoutRevisionRef,
    layoutAcknowledgedManifestRef,
    pivotLayoutOrderRef,
    latestLayoutSentManifestRef,
}) {
    /** @type {(...args: any[]) => any} */
    const runtimePayloadCommitIsCurrent = useCallback((response, payload, resolvedPayload = null) => {
        /** @type {any} */
        const payloadRef = payload && payload.payloadRef && typeof payload.payloadRef === 'object'
            ? payload.payloadRef
            : {};
        /** @type {any} */
        const responseRequestId = response && (response.requestId || response.request_id);
        /** @type {any} */
        const refRequestId = payloadRef.requestId || payloadRef.request_id;
        if (responseRequestId && refRequestId && String(responseRequestId) !== String(refRequestId)) {
            return false;
        }

        /** @type {any} */
        const responseClient = response && (response.clientInstance || response.client_instance);
        /** @type {any} */
        const refClient = payloadRef.clientInstance || payloadRef.client_instance;
        /** @type {any} */
        const expectedClient = responseClient || refClient;
        if (expectedClient && String(expectedClient) !== String(clientInstanceRef.current)) {
            return false;
        }

        /** @type {any} */
        const stateEpochValue = coerceTransportNumber(
            response && response.stateEpoch !== undefined
                ? response.stateEpoch
                : (response && response.state_epoch !== undefined ? response.state_epoch : payloadRef.stateEpoch),
            null
        );
        if (Number.isFinite(stateEpochValue) && stateEpochValue !== stateEpochRef.current) {
            return false;
        }

        // A user retry is a fresh viewport generation. Late responses from
        // superseded windows must neither replace its cells nor dismiss errors.
        /** @type {any} */
        const windowSeq = coerceTransportNumber(
            (response && (response.windowSeq ?? response.window_seq))
                ?? (payload && (payload.windowSeq ?? payload.window_seq ?? payload.version)),
            null
        );
        if (response && response.kind === 'data' && requestVersionRef
            && Number.isFinite(windowSeq) && windowSeq < requestVersionRef.current) {
            return false;
        }

        /** @type {any} */
        const committedPayload = resolvedPayload && typeof resolvedPayload === 'object' ? resolvedPayload : payload;
        /** @type {any} */
        const committedVersion = coerceTransportNumber(
            committedPayload && committedPayload.dataVersion !== undefined
                ? committedPayload.dataVersion
                : payloadRef.dataVersion,
            null
        );
        if (Number.isFinite(committedVersion) && committedVersion < latestDataVersionRef.current) {
            return false;
        }
        const layoutRevision = committedPayload?.layoutRevision;
        const schemaRevision = committedPayload?.colSchema?.layoutRevision;
        if (layoutRevision && schemaRevision && layoutRevision !== schemaRevision) return false;
        if (layoutRevision) {
            if (latestLayoutSentManifestRef.current
                && latestLayoutSentManifestRef.current !== pivotLayoutOrderRef.current) return false;
            const acknowledgedManifest = layoutAcknowledgedManifestRef.current;
            // A changed saved order needs a new revision. A worker cache miss
            // can legitimately return the old revision for the same manifest.
            if (layoutRevision === layoutRevisionRef.current
                && acknowledgedManifest !== '__layout_retry__'
                && acknowledgedManifest !== pivotLayoutOrderRef.current.join('\u001f')) return false;
        }

        return true;
    }, [clientInstanceRef, latestDataVersionRef, requestVersionRef, stateEpochRef,
        layoutRevisionRef, layoutAcknowledgedManifestRef, pivotLayoutOrderRef,
        latestLayoutSentManifestRef]);

    /** @type {(...args: any[]) => any} */
    const commitRuntimeDataPayload = useCallback((response, payload, clientProfile = null) => {
        finalizeTransactionHistoryResponse(response, payload);
        hydrateVisibleEditOverlay(payload.editOverlay);
        reconcileOptimisticCellValuesWithPayload(payload);
        pendingDataProfilerCommitRef.current = {
            requestId: response.requestId || null,
            dataVersion: coerceTransportNumber(payload.dataVersion, null),
            rowCount: coerceTransportNumber(payload.rowCount, null),
            stateQueuedAt: Date.now(),
            clientProfile: clientProfile && typeof clientProfile === 'object' ? clientProfile : null,
        };
        if (payload.formulaErrors && typeof payload.formulaErrors === 'object') {
            setFormulaErrors(payload.formulaErrors);
        } else if (payload.formulaErrors === null) {
            setFormulaErrors({});
        }
        if (typeof payload.layoutRevision === 'string' && payload.layoutRevision
            && latestLayoutSentManifestRef.current === pivotLayoutOrderRef.current) {
            layoutRevisionRef.current = payload.layoutRevision;
            layoutAcknowledgedManifestRef.current = pivotLayoutOrderRef.current.join('\u001f');
        }
        setTransportDataState(/** @param {any} [previousState] @returns {any} */ (previousState) => normalizeRuntimeDataEnvelope({
            ...payload,
            stateEpoch: response && response.stateEpoch !== undefined
                ? response.stateEpoch
                : (response && response.state_epoch !== undefined ? response.state_epoch : payload.stateEpoch),
            windowSeq: response && response.windowSeq !== undefined
                ? response.windowSeq
                : (response && response.window_seq !== undefined ? response.window_seq : payload.windowSeq),
        }, previousState));
    }, [
        finalizeTransactionHistoryResponse,
        hydrateVisibleEditOverlay,
        pendingDataProfilerCommitRef,
        reconcileOptimisticCellValuesWithPayload,
        setFormulaErrors,
        setTransportDataState,
        layoutRevisionRef,
        layoutAcknowledgedManifestRef,
        pivotLayoutOrderRef,
        latestLayoutSentManifestRef,
    ]);

    return {
        commitRuntimeDataPayload,
        runtimePayloadCommitIsCurrent,
    };
}

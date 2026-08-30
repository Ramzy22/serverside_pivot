import { useCallback } from 'react';
import { coerceTransportNumber, normalizeRuntimeDataEnvelope } from './usePivotNormalization';

/**
 * Owns freshness checks and atomic state commits for complete runtime-data
 * envelopes. Viewport-cache and request scheduling remain with their existing
 * server-side owners.
 */
export function useRuntimeDataCommit({
    clientInstanceRef,
    stateEpochRef,
    latestDataVersionRef,
    pendingDataProfilerCommitRef,
    finalizeTransactionHistoryResponse,
    hydrateVisibleEditOverlay,
    reconcileOptimisticCellValuesWithPayload,
    setFormulaErrors,
    setTransportDataState,
}) {
    const runtimePayloadCommitIsCurrent = useCallback((response, payload, resolvedPayload = null) => {
        const payloadRef = payload && payload.payloadRef && typeof payload.payloadRef === 'object'
            ? payload.payloadRef
            : {};
        const responseRequestId = response && (response.requestId || response.request_id);
        const refRequestId = payloadRef.requestId || payloadRef.request_id;
        if (responseRequestId && refRequestId && String(responseRequestId) !== String(refRequestId)) {
            return false;
        }

        const responseClient = response && (response.client_instance || response.clientInstance);
        const refClient = payloadRef.clientInstance || payloadRef.client_instance;
        const expectedClient = responseClient || refClient;
        if (expectedClient && String(expectedClient) !== String(clientInstanceRef.current)) {
            return false;
        }

        const stateEpochValue = coerceTransportNumber(
            response && response.state_epoch !== undefined ? response.state_epoch : payloadRef.stateEpoch,
            null
        );
        if (Number.isFinite(stateEpochValue) && stateEpochValue !== stateEpochRef.current) {
            return false;
        }

        const committedPayload = resolvedPayload && typeof resolvedPayload === 'object' ? resolvedPayload : payload;
        const committedVersion = coerceTransportNumber(
            committedPayload && committedPayload.dataVersion !== undefined
                ? committedPayload.dataVersion
                : payloadRef.dataVersion,
            null
        );
        if (Number.isFinite(committedVersion) && committedVersion < latestDataVersionRef.current) {
            return false;
        }

        return true;
    }, [clientInstanceRef, latestDataVersionRef, stateEpochRef]);

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
        setTransportDataState((previousState) => normalizeRuntimeDataEnvelope({
            ...payload,
            stateEpoch: response && response.state_epoch !== undefined ? response.state_epoch : payload.stateEpoch,
            windowSeq: response && response.window_seq !== undefined ? response.window_seq : payload.windowSeq,
        }, previousState));
    }, [
        finalizeTransactionHistoryResponse,
        hydrateVisibleEditOverlay,
        pendingDataProfilerCommitRef,
        reconcileOptimisticCellValuesWithPayload,
        setFormulaErrors,
        setTransportDataState,
    ]);

    return {
        commitRuntimeDataPayload,
        runtimePayloadCommitIsCurrent,
    };
}

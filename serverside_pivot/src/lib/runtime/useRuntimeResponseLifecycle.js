import { useEffect, useRef } from 'react';

import { htmlTableToTsv } from '../utils/exportUtils';
import { applyRuntimePatchEnvelope, coerceTransportNumber } from '../hooks/usePivotNormalization';
import { buildRuntimeResponseProcessingKey, markRuntimeResponseHandled } from './runtimeResponseDedup';
import { HttpPivotTransport } from './httpTransport';

const FILTER_OPTION_PAGE_SIZE = 250;
/** @returns {any} */
const ignoreDataError = () => {};
// Dash uses its callback graph for envelopes and same-origin HTTP for retained
// data. Direct hosts always provide their configured transport below.
/** @type {Record<string, any>} */
const dashPayloadTransport = {
  /** @param {any} [reference] @param {any} [options] @returns {any} */
  fetchPayload: (reference, options) => new HttpPivotTransport().fetchPayload(reference, options),
  /** @param {any} [reference] @param {any} [options] @returns {any} */
  fetchExportPayload: (reference, options) => new HttpPivotTransport().fetchExportPayload(reference, options),
};

/**
 * Applies one server response exactly once. This transport lifecycle owns data
 * commits, fetched payloads, filter options, exports, charts, and terminal
 * request states; workspace presentation only consumes the resulting models.
  @param {any} p0 @returns {any} */
export function useRuntimeResponseLifecycle({
  activeChartRequestRef,
  activeExportRequestIdRef,
  activeRuntimePayloadFetchRef,
  cancelledExportRequestIdsRef,
  exportJobControllerRef,
  clearPendingTransactionHistoryRequest,
  clientInstanceRef,
  commitRuntimeDataPayload,
  copyRichToClipboard,
  emitEditLifecycleEvent,
  finalizeTransactionHistoryResponse,
  handledRuntimeResponseKeysRef,
  layoutAcknowledgedManifestRef,
  hydrateVisibleEditOverlay,
  pendingCopyIntentRef,
  pendingDataProfilerCommitRef,
  pendingServerFilterOptionsRef,
  payloadTransport = /** @type {import('./httpTransport').RuntimePayloadTransport} */ (dashPayloadTransport),
  pivotProfilerRef,
  profilerComponentIdRef,
  profilingEnabledRef,
  reconcileOptimisticCellValuesWithPayload,
  recordProfilerResponse,
  requestVersionRef,
  resolvePendingRequest,
  runtimePayloadCommitIsCurrent,
  runtimeResponse,
  scheduleSilentViewportRefresh,
  setEditorOptionsLoadingState,
  setIsCopying,
  setIsExporting,
  setPartModal,
  setRuntimeDataError = ignoreDataError,
  setTransportChartDataState,
  setTransportDataState,
  setTransportFilterOptionMetaState,
  setTransportFilterOptionsState,
  showNotification,
  stateEpochRef,
  triggerExportDownload,
}) {
  /** @type {any} */
  const handledUnkeyedDataErrorsRef = useRef(new WeakSet());
  useEffect(() => {
    if (!runtimeResponse || typeof runtimeResponse !== 'object') return;
    /** @type {any} */
    const payload = runtimeResponse.payload && typeof runtimeResponse.payload === 'object' ? runtimeResponse.payload : {};
    /** @type {any} */
    const responseProcessingKey = buildRuntimeResponseProcessingKey(runtimeResponse, payload);
    if (responseProcessingKey && !markRuntimeResponseHandled(handledRuntimeResponseKeysRef, responseProcessingKey)) return;
    if (!responseProcessingKey && runtimeResponse.kind === 'data' && runtimeResponse.status === 'error') {
      // Optional V1 identity can be absent. Capture fallback generation only
      // once for this received object; callback rerenders cannot rebind it.
      if (handledUnkeyedDataErrorsRef.current.has(runtimeResponse)) return;
      handledUnkeyedDataErrorsRef.current.add(runtimeResponse);
    }
    recordProfilerResponse(runtimeResponse);
    if (runtimeResponse.kind === 'data' && runtimeResponse.status === 'layout_required') {
      resolvePendingRequest(runtimeResponse);
      layoutAcknowledgedManifestRef.current = '__layout_retry__';
      scheduleSilentViewportRefresh();
      return;
    }
    if ((runtimeResponse.kind === 'data' || runtimeResponse.kind === 'update' || runtimeResponse.kind === 'transaction') && runtimeResponse.status === 'data') {
      /** @type {any} */
      const payloadRef = payload.payloadRef && typeof payload.payloadRef === 'object' ? payload.payloadRef : null;
      if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
        /** @type {any} */
        const fetchToken = payloadRef.id || payloadRef.url;
        /** @type {any} */
        const payloadFetchStartedAt = Date.now();
        /** @type {any} */
        const abortController = new AbortController();
        let payloadFetchSettled = false;
        activeRuntimePayloadFetchRef.current = {
          token: fetchToken,
          requestId: runtimeResponse.requestId || null
        };
        payloadTransport.fetchPayload(payloadRef, {
          signal: abortController.signal
        }).then(/** @param {any} [remotePayload] @returns {any} */ remotePayload => {
          const activeFetch = activeRuntimePayloadFetchRef.current;
          if (abortController.signal.aborted || !activeFetch || activeFetch.token !== fetchToken) return;
          payloadFetchSettled = true;
          /** @type {Record<string, any>} */
          const resolvedPayload = {
            ...payload,
            ...(remotePayload && typeof remotePayload === 'object' ? remotePayload : {}),
            payloadRef
          };
          if (!runtimePayloadCommitIsCurrent(runtimeResponse, payload, resolvedPayload)) {
            resolvePendingRequest(runtimeResponse);
            return;
          }
          commitRuntimeDataPayload(runtimeResponse, resolvedPayload, {
            payloadFetchParseMs: Date.now() - payloadFetchStartedAt,
            payloadBytes: coerceTransportNumber(payloadRef.bytes, null),
            payloadUncompressedBytes: coerceTransportNumber(payloadRef.uncompressedBytes, null),
            payloadCompressed: payloadRef.contentEncoding === 'gzip'
          });
          if (runtimeResponse.kind === 'data') setRuntimeDataError(null);
        }).catch(/** @param {any} [error] @returns {any} */ error => {
          if (abortController.signal.aborted || error && error.name === 'AbortError') return;
          payloadFetchSettled = true;
          resolvePendingRequest(runtimeResponse);
          console.error('Runtime payload fetch failed:', error);
          showNotification('The server data payload expired before it could be loaded.', 'error');
          scheduleSilentViewportRefresh();
        });
        return () => {
          abortController.abort();
          // Effect dependencies can change while delivery is unfinished. Let
          // the replacement effect fetch this response again, while completed
          // deliveries remain deduplicated. An aborted fetch cannot commit.
          if (!payloadFetchSettled && responseProcessingKey) {
            handledRuntimeResponseKeysRef.current.delete(responseProcessingKey);
          }
          const activeFetch = activeRuntimePayloadFetchRef.current;
          if (activeFetch && activeFetch.token === fetchToken) {
            activeRuntimePayloadFetchRef.current = null;
          }
        };
      }
      if (!runtimePayloadCommitIsCurrent(runtimeResponse, payload, payload)) {
        resolvePendingRequest(runtimeResponse);
        return;
      }
      commitRuntimeDataPayload(runtimeResponse, payload);
      if (runtimeResponse.kind === 'data') setRuntimeDataError(null);
      return;
    }
    if (runtimeResponse.kind === 'transaction' && runtimeResponse.status === 'patched') {
      /** @type {any} */
      const patchPayload = payload.patch && typeof payload.patch === 'object' ? payload.patch : {};
      finalizeTransactionHistoryResponse(runtimeResponse, payload);
      hydrateVisibleEditOverlay(payload.editOverlay);
      reconcileOptimisticCellValuesWithPayload({
        data: patchPayload.rows || []
      });
      pendingDataProfilerCommitRef.current = {
        requestId: runtimeResponse.requestId || null,
        dataVersion: coerceTransportNumber(payload.dataVersion, null),
        rowCount: coerceTransportNumber(payload.rowCount, null),
        stateQueuedAt: Date.now()
      };
      setTransportDataState(/** @param {any} [previousState] @returns {any} */ previousState => applyRuntimePatchEnvelope({
        ...patchPayload,
        dataOffset: payload.dataOffset,
        dataVersion: payload.dataVersion,
        rowCount: payload.rowCount,
        columns: payload.columns,
        stateEpoch: runtimeResponse && runtimeResponse.stateEpoch !== undefined
          ? runtimeResponse.stateEpoch
          : (runtimeResponse && runtimeResponse.state_epoch !== undefined ? runtimeResponse.state_epoch : payload.stateEpoch)
      }, previousState));
      if (payload.transaction && payload.transaction.deferredViewportRefresh) {
        scheduleSilentViewportRefresh();
      }
      return;
    }
    if (runtimeResponse.kind === 'transaction' && runtimeResponse.status === 'transaction_applied') {
      finalizeTransactionHistoryResponse(runtimeResponse, payload);
      if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
        pivotProfilerRef.current.resolve({
          requestId: runtimeResponse.requestId,
          componentId: profilerComponentIdRef.current,
          kind: 'transaction',
          status: runtimeResponse.status
        });
      }
      return;
    }
    if (runtimeResponse.kind === 'filter_options' && runtimeResponse.status === 'ok') {
      /** @type {any} */
      const columnId = payload.columnId || payload.column_id;
      if (!columnId) return;
      /** @type {any} */
      const nextOptions = Array.isArray(payload.options) ? payload.options : [];
      /** @type {any} */
      const search = payload.search !== undefined && payload.search !== null ? String(payload.search) : '';
      /** @type {any} */
      const offset = coerceTransportNumber(payload.offset, 0) || 0;
      /** @type {any} */
      const limit = coerceTransportNumber(payload.limit, FILTER_OPTION_PAGE_SIZE) || FILTER_OPTION_PAGE_SIZE;
      /** @type {any} */
      const total = coerceTransportNumber(payload.total, nextOptions.length);
      /** @type {any} */
      const hasMore = payload.hasMore === true;
      const responseRequestKey = `${columnId}::${search}::${offset}`;
      if (pendingServerFilterOptionsRef.current === responseRequestKey || pendingServerFilterOptionsRef.current === columnId) {
        pendingServerFilterOptionsRef.current = null;
      }
      setTransportFilterOptionsState(/** @param {any} [previousState] @returns {any} */ previousState => {
        /** @type {any} */
        const previousOptions = previousState && previousState[columnId];
        /** @type {any} */
        const mergedOptions = offset > 0 && Array.isArray(previousOptions) ? [...previousOptions, ...nextOptions.filter(/** @param {any} [value] @returns {any} */ value => !previousOptions.some(existing => String(existing) === String(value)))] : nextOptions;
        if (Array.isArray(previousOptions) && previousOptions.length === mergedOptions.length && previousOptions.every((value, index) => value === mergedOptions[index])) {
          return previousState;
        }
        return {
          ...(previousState || {}),
          [columnId]: mergedOptions
        };
      });
      setTransportFilterOptionMetaState(/** @param {any} [previousState] @returns {any} */ previousState => ({
        ...(previousState || {}),
        [columnId]: {
          ...(previousState && previousState[columnId] || {}),
          search,
          offset,
          limit,
          total,
          hasMore,
          loading: false
        }
      }));
      setEditorOptionsLoadingState(/** @param {any} [previousState] @returns {any} */ previousState => {
        if (previousState && previousState[columnId] === false) return previousState;
        return {
          ...previousState,
          [columnId]: false
        };
      });
      emitEditLifecycleEvent({
        kind: 'editor_options_loaded',
        sourceColumnId: columnId,
        optionCount: nextOptions.length
      });
      if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
        pivotProfilerRef.current.resolve({
          requestId: runtimeResponse.requestId,
          componentId: profilerComponentIdRef.current,
          kind: 'filter_options',
          status: runtimeResponse.status
        });
      }
      return;
    }
    if (runtimeResponse.kind === 'filter_options' && runtimeResponse.status === 'error') {
      /** @type {any} */
      const columnId = payload.columnId || payload.column_id;
      if (columnId) {
        if (pendingServerFilterOptionsRef.current && String(pendingServerFilterOptionsRef.current).startsWith(`${columnId}::`)) {
          pendingServerFilterOptionsRef.current = null;
        }
        setTransportFilterOptionMetaState(/** @param {any} [previousState] @returns {any} */ previousState => ({
          ...(previousState || {}),
          [columnId]: {
            ...(previousState && previousState[columnId] || {}),
            loading: false
          }
        }));
        setEditorOptionsLoadingState(/** @param {any} [previousState] @returns {any} */ previousState => {
          if (previousState && previousState[columnId] === false) return previousState;
          return {
            ...previousState,
            [columnId]: false
          };
        });
      }
      return;
    }
    if (runtimeResponse.kind === 'export') {
      if (exportJobControllerRef?.current?.handleResponse(runtimeResponse, {
        triggerExportDownload, showNotification,
        /** @param {any} [reference] @param {any} [options] @returns {any} */
        fetchExportPayload: (reference, options) => payloadTransport.fetchExportPayload
          ? payloadTransport.fetchExportPayload(reference, options)
          : Promise.reject(new Error('The configured transport does not support export payload delivery.')),
      })) return;
      // Job progress is never a legacy inline export, including late responses
      // from a disposed or superseded controller.
      if (runtimeResponse.status === 'export_job' || payload.jobId) return;
      /** @type {any} */
      const responseRequestId = runtimeResponse.requestId ? String(runtimeResponse.requestId) : null;
      if (responseRequestId && cancelledExportRequestIdsRef.current.has(responseRequestId)) {
        cancelledExportRequestIdsRef.current.delete(responseRequestId);
        if (activeExportRequestIdRef.current === responseRequestId) {
          activeExportRequestIdRef.current = null;
        }
        return;
      }
      if (responseRequestId && activeExportRequestIdRef.current && activeExportRequestIdRef.current !== responseRequestId) {
        return;
      }
      /** @type {any} */
      const currentUnidentifiedPartFailure = !payload.partId && runtimeResponse.status !== 'ok'
        && responseRequestId && activeExportRequestIdRef.current === responseRequestId;
      if (!payload.partId && activeExportRequestIdRef.current === responseRequestId) {
        activeExportRequestIdRef.current = null;
      }
      const partId = payload.partId;
      const copyIntent = pendingCopyIntentRef.current;
      /** @type {any} */
      const payloadRef = payload.payloadRef && typeof payload.payloadRef === 'object' ? payload.payloadRef : null;
      /** @type {any} */
      const filename = payload.filename || payloadRef && payloadRef.filename || 'pivot_export.xlsx';
      /** @returns {any} */
      const isPartCurrent = () => !partId || !responseRequestId || (
        !cancelledExportRequestIdsRef.current.has(responseRequestId)
        && activeExportRequestIdRef.current === responseRequestId);
      /** @param {any} [completed] @returns {any} */
      const finishPart = completed => {
        if (!partId) return;
        if (!isPartCurrent()) return;
        setPartModal(/** @param {any} [prev] @returns {any} */ prev => prev && prev.processingPartId === partId ? {
          ...prev,
          processingPartId: null,
          completedParts: completed ? new Set([...prev.completedParts, partId]) : prev.completedParts
        } : prev);
        if (activeExportRequestIdRef.current === responseRequestId) {
          activeExportRequestIdRef.current = null;
        }
      };
      /** @param {any} [b64] @returns {any} */
      const decodeBase64Utf8 = b64 => {
        /** @type {any} */
        const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
        if (typeof TextDecoder === 'function') {
          return new TextDecoder('utf-8').decode(bytes);
        }
        return atob(b64);
      };
      /** @returns {any} */
      const fetchExportBlob = () => {
        if (payloadTransport && typeof payloadTransport.fetchExportPayload === 'function') {
          return payloadTransport.fetchExportPayload(payloadRef);
        }
        return fetch(payloadRef.url, {
          method: 'GET',
          headers: { Accept: '*/*' }
        }).then(response => {
          if (!response.ok) {
            throw new Error(`Export payload fetch failed with status ${response.status}`);
          }
          return response.blob();
        });
      };
      /** @returns {any} */
      const readExportPayloadText = () => {
        if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
          if (payloadTransport && typeof payloadTransport.fetchExportPayload === 'function') {
            return payloadTransport.fetchExportPayload(payloadRef).then(/** @param {any} [blob] @returns {any} */ blob => blob.text());
          }
          return fetch(payloadRef.url, {
            method: 'GET',
            headers: {
              Accept: 'text/html,text/csv,text/tab-separated-values,text/plain,*/*'
            }
          }).then(response => {
            if (!response.ok) {
              throw new Error(`Export payload fetch failed with status ${response.status}`);
            }
            return response.text();
          });
        }
        if (payload.data) {
          return Promise.resolve(decodeBase64Utf8(payload.data));
        }
        return Promise.resolve('');
      };
      /** @returns {any} */
      const downloadPayloadReference = () => {
        if (!payloadRef || typeof payloadRef.url !== 'string' || !payloadRef.url) {
          return Promise.reject(new Error('Export response did not include a download URL.'));
        }
        // Fetch the reference before reporting success. Besides surfacing an
        // expired/unauthorized payload clearly, this converts a cross-origin
        // runtime response (for example Streamlit :8502 -> runtime :8765)
        // into a same-document Blob download. Browsers may ignore the
        // `download` attribute on a cross-origin anchor otherwise.
        return fetchExportBlob().then(/** @param {any} [blob] @returns {any} */ blob => {
          if (!isPartCurrent()) return false;
          /** @type {any} */
          const url = URL.createObjectURL(blob);
          triggerExportDownload(url, filename);
          window.setTimeout(() => URL.revokeObjectURL(url), 1000);
          return true;
        });
      };
      /** @returns {any} */
      const downloadLegacyBase64 = () => {
        const b64 = payload.data;
        if (!b64) return false;
        /** @type {any} */
        const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
        /** @type {any} */
        const mimeType = payload.format === 'html' ? 'text/html' : payload.format === 'xlsx' || payload.format === 'xls' ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' : payload.format === 'tsv' ? 'text/tab-separated-values' : 'text/csv';
        /** @type {any} */
        const blob = new Blob([bytes], {
          type: `${mimeType};charset=utf-8;`
        });
        /** @type {any} */
        const url = URL.createObjectURL(blob);
        triggerExportDownload(url, filename);
        window.setTimeout(() => URL.revokeObjectURL(url), 1000);
        return true;
      };
      if (!partId) {
        setIsExporting(false);
        setIsCopying(false);
        pendingCopyIntentRef.current = null;
      }
      if (runtimeResponse.status === 'ok') {
        if (copyIntent) {
          Promise.resolve().then(readExportPayloadText).then(text => {
            if (!isPartCurrent()) return false;
            /** @type {any} */
            const responseContentType = payload.contentType || payloadRef && payloadRef.contentType;
            /** @type {any} */
            const isHtmlPayload = payload.format === 'html' || responseContentType && String(responseContentType).toLowerCase().includes('text/html') || /^\s*<!doctype html/i.test(text) || /^\s*<html[\s>]/i.test(text) || /^\s*<table[\s>]/i.test(text);
            if (isHtmlPayload) {
              return copyRichToClipboard({
                html: text,
                text: htmlTableToTsv(text)
              }, null);
            }
            /** @type {any} */
            const lines = text.split('\n').filter(/** @param {any} [l] @returns {any} */ l => l.length > 0);
            return copyRichToClipboard({
              text: lines.join('\n')
            }, null);
          }).then(copied => {
            if (!isPartCurrent()) return;
            if (copied) {
              finishPart(true);
              showNotification(`Copied ${payload.rows ?? ''} rows`, 'success');
            } else {
              finishPart(false);
              showNotification('Copy failed', 'error');
            }
          }).catch(error => {
            if (!isPartCurrent()) return;
            console.error('Copy export payload failed:', error);
            finishPart(false);
            showNotification('Copy failed', 'error');
          });
        } else {
          if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
            downloadPayloadReference().then(/** @param {any} [downloaded] @returns {any} */ downloaded => {
              if (!downloaded || !isPartCurrent()) return;
              finishPart(true);
              showNotification(`Downloaded ${payload.rows ?? ''} rows`, 'success');
            }).catch(/** @param {any} [error] @returns {any} */ error => {
              if (!isPartCurrent()) return;
              console.error('Export payload download failed:', error);
              finishPart(false);
              showNotification('Export failed', 'error');
            });
          } else {
            let downloaded = false;
            try {
              downloaded = downloadLegacyBase64();
            } catch (error) {
              console.error('Inline export download failed:', error);
            }
            finishPart(downloaded);
            showNotification(downloaded ? `Downloaded ${payload.rows ?? ''} rows` : 'Export failed', downloaded ? 'success' : 'error');
          }
        }
      } else {
        if (currentUnidentifiedPartFailure) {
          setPartModal(/** @param {any} [prev] @returns {any} */ prev => prev && prev.processingPartId ? {
            ...prev,
            processingPartId: null
          } : prev);
        } else {
          finishPart(false);
        }
        showNotification(copyIntent ? 'Copy failed' : 'Export failed', 'error');
      }
      return;
    }
    if (runtimeResponse.kind === 'chart') {
      if (runtimeResponse.status === 'chart_data') {
        setTransportChartDataState(payload);
        if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
          pivotProfilerRef.current.resolve({
            requestId: runtimeResponse.requestId,
            componentId: profilerComponentIdRef.current,
            kind: 'chart',
            status: runtimeResponse.status
          });
        }
        return;
      }
      if (runtimeResponse.status === 'error') {
        activeChartRequestRef.current = null;
        console.error('Chart request failed:', runtimeResponse.message || runtimeResponse);
        if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
          pivotProfilerRef.current.resolve({
            requestId: runtimeResponse.requestId,
            componentId: profilerComponentIdRef.current,
            kind: 'chart',
            status: runtimeResponse.status
          });
        }
      }
      return;
    }
    if ((runtimeResponse.status === 'stale' || runtimeResponse.status === 'error') && (runtimeResponse.kind === 'data' || runtimeResponse.kind === 'update' || runtimeResponse.kind === 'transaction')) {
      resolvePendingRequest(runtimeResponse);
      if (runtimeResponse.kind === 'data' && runtimeResponse.status === 'error'
        && runtimePayloadCommitIsCurrent(runtimeResponse, payload)) {
        // Backend messages may contain exception details or credentials. Only
        // presentation-owned copy crosses the inline grid error boundary.
        /** @type {Record<string, any>} */
        const failedRequest = {
          requestId: runtimeResponse.requestId || runtimeResponse.request_id || null,
          windowSeq: coerceTransportNumber(runtimeResponse.windowSeq ?? runtimeResponse.window_seq
            ?? payload.windowSeq ?? payload.window_seq ?? payload.version, requestVersionRef?.current ?? null),
          stateEpoch: coerceTransportNumber(runtimeResponse.stateEpoch ?? runtimeResponse.state_epoch,
            stateEpochRef?.current ?? null),
          clientInstance: runtimeResponse.clientInstance || runtimeResponse.client_instance || clientInstanceRef?.current || null,
        };
        setRuntimeDataError(/** @param {any} [previous] @returns {any} */ previous => previous && !previous.retrying
          && Object.keys(failedRequest).every(key => previous.failedRequest?.[key] === failedRequest[key])
          ? previous : { retrying: false, failedRequest });
      }
      if (runtimeResponse.status === 'stale') {
        scheduleSilentViewportRefresh();
      }
    }
    if (runtimeResponse.status === 'stale' || runtimeResponse.status === 'error') {
      clearPendingTransactionHistoryRequest(runtimeResponse, true);
    }
    if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current && (runtimeResponse.status === 'stale' || runtimeResponse.status === 'error')) {
      pivotProfilerRef.current.resolve({
        requestId: runtimeResponse.requestId,
        componentId: profilerComponentIdRef.current,
        kind: runtimeResponse.kind || 'data',
        status: runtimeResponse.status
      });
    }
  }, [clearPendingTransactionHistoryRequest, clientInstanceRef, commitRuntimeDataPayload, emitEditLifecycleEvent, finalizeTransactionHistoryResponse, hydrateVisibleEditOverlay, layoutAcknowledgedManifestRef, payloadTransport, reconcileOptimisticCellValuesWithPayload, recordProfilerResponse, requestVersionRef, resolvePendingRequest, runtimePayloadCommitIsCurrent, runtimeResponse, scheduleSilentViewportRefresh, setIsExporting, setIsCopying, setRuntimeDataError, showNotification, stateEpochRef]);
}

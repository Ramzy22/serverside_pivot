import { useEffect } from 'react';

import { htmlTableToTsv } from '../utils/exportUtils';
import { applyRuntimePatchEnvelope, coerceTransportNumber } from '../hooks/usePivotNormalization';
import { buildRuntimeResponseProcessingKey, markRuntimeResponseHandled } from './runtimeResponseDedup';
import { HttpPivotTransport } from './httpTransport';

const FILTER_OPTION_PAGE_SIZE = 250;
// Dash uses its callback graph for envelopes and same-origin HTTP for retained
// data. Direct hosts always provide their configured transport below.
const dashPayloadTransport = {
  fetchPayload: (reference, options) => new HttpPivotTransport().fetchPayload(reference, options)
};

/**
 * Applies one server response exactly once. This transport lifecycle owns data
 * commits, fetched payloads, filter options, exports, charts, and terminal
 * request states; workspace presentation only consumes the resulting models.
 */
export function useRuntimeResponseLifecycle({
  activeChartRequestRef,
  activeExportRequestIdRef,
  activeRuntimePayloadFetchRef,
  cancelledExportRequestIdsRef,
  clearPendingTransactionHistoryRequest,
  commitRuntimeDataPayload,
  copyRichToClipboard,
  emitEditLifecycleEvent,
  finalizeTransactionHistoryResponse,
  handledRuntimeResponseKeysRef,
  hydrateVisibleEditOverlay,
  pendingCopyIntentRef,
  pendingDataProfilerCommitRef,
  pendingServerFilterOptionsRef,
  payloadTransport = dashPayloadTransport,
  pivotProfilerRef,
  profilerComponentIdRef,
  profilingEnabledRef,
  reconcileOptimisticCellValuesWithPayload,
  recordProfilerResponse,
  resolvePendingRequest,
  runtimePayloadCommitIsCurrent,
  runtimeResponse,
  scheduleSilentViewportRefresh,
  setEditorOptionsLoadingState,
  setIsCopying,
  setIsExporting,
  setPartModal,
  setTransportChartDataState,
  setTransportDataState,
  setTransportFilterOptionMetaState,
  setTransportFilterOptionsState,
  showNotification,
  triggerExportDownload,
}) {
  useEffect(() => {
    if (!runtimeResponse || typeof runtimeResponse !== 'object') return;
    const payload = runtimeResponse.payload && typeof runtimeResponse.payload === 'object' ? runtimeResponse.payload : {};
    const responseProcessingKey = buildRuntimeResponseProcessingKey(runtimeResponse, payload);
    if (responseProcessingKey && !markRuntimeResponseHandled(handledRuntimeResponseKeysRef, responseProcessingKey)) return;
    recordProfilerResponse(runtimeResponse);
    if ((runtimeResponse.kind === 'data' || runtimeResponse.kind === 'update' || runtimeResponse.kind === 'transaction') && runtimeResponse.status === 'data') {
      const payloadRef = payload.payloadRef && typeof payload.payloadRef === 'object' ? payload.payloadRef : null;
      if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
        const fetchToken = payloadRef.id || payloadRef.url;
        const payloadFetchStartedAt = Date.now();
        const abortController = new AbortController();
        let payloadFetchSettled = false;
        activeRuntimePayloadFetchRef.current = {
          token: fetchToken,
          requestId: runtimeResponse.requestId || null
        };
        payloadTransport.fetchPayload(payloadRef, {
          signal: abortController.signal
        }).then(remotePayload => {
          const activeFetch = activeRuntimePayloadFetchRef.current;
          if (abortController.signal.aborted || !activeFetch || activeFetch.token !== fetchToken) return;
          payloadFetchSettled = true;
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
        }).catch(error => {
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
      return;
    }
    if (runtimeResponse.kind === 'transaction' && runtimeResponse.status === 'patched') {
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
      setTransportDataState(previousState => applyRuntimePatchEnvelope({
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
      const columnId = payload.columnId || payload.column_id;
      if (!columnId) return;
      const nextOptions = Array.isArray(payload.options) ? payload.options : [];
      const search = payload.search !== undefined && payload.search !== null ? String(payload.search) : '';
      const offset = coerceTransportNumber(payload.offset, 0) || 0;
      const limit = coerceTransportNumber(payload.limit, FILTER_OPTION_PAGE_SIZE) || FILTER_OPTION_PAGE_SIZE;
      const total = coerceTransportNumber(payload.total, nextOptions.length);
      const hasMore = payload.hasMore === true;
      const responseRequestKey = `${columnId}::${search}::${offset}`;
      if (pendingServerFilterOptionsRef.current === responseRequestKey || pendingServerFilterOptionsRef.current === columnId) {
        pendingServerFilterOptionsRef.current = null;
      }
      setTransportFilterOptionsState(previousState => {
        const previousOptions = previousState && previousState[columnId];
        const mergedOptions = offset > 0 && Array.isArray(previousOptions) ? [...previousOptions, ...nextOptions.filter(value => !previousOptions.some(existing => String(existing) === String(value)))] : nextOptions;
        if (Array.isArray(previousOptions) && previousOptions.length === mergedOptions.length && previousOptions.every((value, index) => value === mergedOptions[index])) {
          return previousState;
        }
        return {
          ...(previousState || {}),
          [columnId]: mergedOptions
        };
      });
      setTransportFilterOptionMetaState(previousState => ({
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
      setEditorOptionsLoadingState(previousState => {
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
      const columnId = payload.columnId || payload.column_id;
      if (columnId) {
        if (pendingServerFilterOptionsRef.current && String(pendingServerFilterOptionsRef.current).startsWith(`${columnId}::`)) {
          pendingServerFilterOptionsRef.current = null;
        }
        setTransportFilterOptionMetaState(previousState => ({
          ...(previousState || {}),
          [columnId]: {
            ...(previousState && previousState[columnId] || {}),
            loading: false
          }
        }));
        setEditorOptionsLoadingState(previousState => {
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
      if (activeExportRequestIdRef.current === responseRequestId) {
        activeExportRequestIdRef.current = null;
      }
      const partId = payload.partId;
      const copyIntent = pendingCopyIntentRef.current;
      const payloadRef = payload.payloadRef && typeof payload.payloadRef === 'object' ? payload.payloadRef : null;
      const filename = payload.filename || payloadRef && payloadRef.filename || 'pivot_export.xlsx';
      const readExportPayloadText = () => {
        if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
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
          return Promise.resolve(atob(payload.data));
        }
        return Promise.resolve('');
      };
      const downloadPayloadReference = () => {
        if (!payloadRef || typeof payloadRef.url !== 'string' || !payloadRef.url) {
          return Promise.reject(new Error('Export response did not include a download URL.'));
        }
        // Fetch the reference before reporting success. Besides surfacing an
        // expired/unauthorized payload clearly, this converts a cross-origin
        // runtime response (for example Streamlit :8502 -> runtime :8765)
        // into a same-document Blob download. Browsers may ignore the
        // `download` attribute on a cross-origin anchor otherwise.
        return fetch(payloadRef.url, {
          method: 'GET',
          headers: { Accept: '*/*' }
        }).then(response => {
          if (!response.ok) {
            throw new Error(`Export payload fetch failed with status ${response.status}`);
          }
          return response.blob();
        }).then(blob => {
          const url = URL.createObjectURL(blob);
          triggerExportDownload(url, filename);
          window.setTimeout(() => URL.revokeObjectURL(url), 1000);
        });
      };
      const downloadLegacyBase64 = () => {
        const b64 = payload.data;
        if (!b64) return false;
        const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
        const mimeType = payload.format === 'html' ? 'text/html' : payload.format === 'xlsx' || payload.format === 'xls' ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' : payload.format === 'tsv' ? 'text/tab-separated-values' : 'text/csv';
        const blob = new Blob([bytes], {
          type: `${mimeType};charset=utf-8;`
        });
        const url = URL.createObjectURL(blob);
        triggerExportDownload(url, filename);
        window.setTimeout(() => URL.revokeObjectURL(url), 1000);
        return true;
      };
      if (partId) {
        setPartModal(prev => prev ? {
          ...prev,
          processingPartId: null,
          completedParts: new Set([...prev.completedParts, partId])
        } : null);
      } else {
        setIsExporting(false);
        setIsCopying(false);
        pendingCopyIntentRef.current = null;
      }
      if (runtimeResponse.status === 'ok') {
        if (copyIntent) {
          readExportPayloadText().then(text => {
            const responseContentType = payload.contentType || payloadRef && payloadRef.contentType;
            const isHtmlPayload = payload.format === 'html' || responseContentType && String(responseContentType).toLowerCase().includes('text/html') || /^\s*<!doctype html/i.test(text) || /^\s*<html[\s>]/i.test(text) || /^\s*<table[\s>]/i.test(text);
            if (isHtmlPayload) {
              return copyRichToClipboard({
                html: text,
                text: htmlTableToTsv(text)
              }, null);
            }
            const lines = text.split('\n').filter(l => l.length > 0);
            return copyRichToClipboard({
              text: lines.join('\n')
            }, null);
          }).then(copied => {
            if (copied) {
              showNotification(`Copied ${payload.rows ?? ''} rows`, 'success');
            }
          }).catch(error => {
            console.error('Copy export payload failed:', error);
            showNotification('Copy failed', 'error');
          });
        } else {
          if (payloadRef && typeof payloadRef.url === 'string' && payloadRef.url) {
            downloadPayloadReference().then(() => {
              showNotification(`Downloaded ${payload.rows ?? ''} rows`, 'success');
            }).catch(error => {
              console.error('Export payload download failed:', error);
              showNotification('Export failed', 'error');
            });
          } else if (downloadLegacyBase64()) {
            showNotification(`Downloaded ${payload.rows ?? ''} rows`, 'success');
          } else {
            showNotification('Export failed', 'error');
          }
        }
      } else {
        if (partId) setPartModal(prev => prev ? {
          ...prev,
          processingPartId: null
        } : null);
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
  }, [clearPendingTransactionHistoryRequest, commitRuntimeDataPayload, emitEditLifecycleEvent, finalizeTransactionHistoryResponse, hydrateVisibleEditOverlay, payloadTransport, reconcileOptimisticCellValuesWithPayload, recordProfilerResponse, resolvePendingRequest, runtimePayloadCommitIsCurrent, runtimeResponse, scheduleSilentViewportRefresh, setIsExporting, setIsCopying, showNotification]);
}

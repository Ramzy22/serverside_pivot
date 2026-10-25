import { exportPivotTable, exportPivotTableCsv } from '../utils/exportUtils';

const terminalExportPhases = new Set(['cancelled', 'completed', 'failed', 'interrupted']);

/** One host-neutral owner for export identity, polling, and delivery. */
function createExportJobController(initialPorts) {
  let ports = initialPorts;
  let job = null;
  const ownedRequests = new Set();
  const isCurrent = candidate => job === candidate && !candidate.disposed;
  const clearPoll = candidate => {
    clearTimeout(candidate.pollTimer);
    candidate.pollTimer = null;
  };
  const clearRequests = candidate => {
    for (const request of candidate.requests.values()) clearTimeout(request.timer);
    candidate.requests.clear();
  };
  const publish = candidate => {
    if (!isCurrent(candidate)) return;
    const {phase, rowsWritten, bytesWritten, totalRows} = candidate;
    ports.setExportProgress({phase, rowsWritten, bytesWritten, totalRows,
      delivery: candidate.delivery, deliveryFailed: candidate.deliveryFailed,
      percent: totalRows > 0 ? Math.min(100, Math.floor(rowsWritten / totalRows * 100)) : null});
    ports.setIsExporting(candidate.delivery === 'pending' || !terminalExportPhases.has(phase));
  };
  const finishPart = (candidate, completed) => {
    const partId = candidate.payload.partId;
    if (partId && isCurrent(candidate)) ports.setPartModal(previous => previous && previous.processingPartId === partId
      ? {...previous, processingPartId: null, completedParts: completed ? new Set([...previous.completedParts, partId]) : previous.completedParts}
      : previous);
  };
  const stop = candidate => {
    if (!candidate) return;
    candidate.disposed = true;
    clearPoll(candidate);
    clearRequests(candidate);
    candidate.downloadAbort?.abort();
    if (candidate.downloadUrl) URL.revokeObjectURL(candidate.downloadUrl);
    clearTimeout(candidate.revokeTimer);
  };
  const terminal = (candidate, phase) => {
    candidate.phase = phase;
    clearPoll(candidate); clearRequests(candidate);
    ports.activeExportRequestIdRef.current = null;
    publish(candidate);
    if (phase !== 'completed') finishPart(candidate, false);
  };
  const schedulePoll = candidate => {
    clearPoll(candidate);
    if (isCurrent(candidate) && !terminalExportPhases.has(candidate.phase)) {
      candidate.pollTimer = setTimeout(() => poll(candidate.jobId), 500);
    }
  };
  const send = (candidate, action, payload, runtimeFields = {}, replayRequestId = null) => {
    clearPoll(candidate);
    let requestId;
    try {
      requestId = ports.dispatch(payload, runtimeFields, replayRequestId);
    } catch (error) {
      if (action === 'start') {
        candidate.pollTimer = setTimeout(() => send(candidate, 'start', payload, runtimeFields, candidate.startRequestId), 500);
        return;
      }
      if (candidate.cancelRequested && candidate.jobId) schedulePoll(candidate);
      else terminal(candidate, 'failed');
      return;
    }
    ownedRequests.add(requestId);
    // Bound historical correlation while preserving the latest generation.
    if (ownedRequests.size > 1024) ownedRequests.delete(ownedRequests.values().next().value);
    const timer = setTimeout(() => {
      if (!isCurrent(candidate) || !candidate.requests.has(requestId)) return;
      const timedOut = candidate.requests.get(requestId);
      timedOut.expired = true;
      // Keep correlation for a late authoritative response. Dash callbacks can
      // arrive after the client deadline, even when the server job completed.
      if (candidate.requests.size > 128) {
        const oldestExpired = [...candidate.requests].find(([, pending]) => pending.expired)?.[0];
        if (oldestExpired !== undefined) candidate.requests.delete(oldestExpired);
      }
      // A lost cancel response cannot establish server cleanup. Keep asking.
      if (action === 'start' && !candidate.jobId) {
        send(candidate, 'start', candidate.payload, candidate.runtimeFields, candidate.startRequestId);
      } else if (candidate.jobId) schedulePoll(candidate);
      else terminal(candidate, 'interrupted');
    }, 15000);
    candidate.requests.set(requestId, {action, timer});
    return requestId;
  };
  const poll = jobId => {
    if (!job || !isCurrent(job) || job.jobId !== jobId || terminalExportPhases.has(job.phase)) return;
    if ([...job.requests.values()].some(request => !request.expired && (request.action === 'status' || request.action === 'cancel'))) return;
    return send(job, 'status', {action: 'status', jobId, table: job.payload.table});
  };
  const cancel = jobId => {
    if (job && isCurrent(job) && job.delivery === 'pending') return true;
    if (!job || !isCurrent(job) || terminalExportPhases.has(job.phase) || (jobId && job.jobId !== jobId)) return false;
    if (job.cancelRequested) return true;
    job.cancelRequested = true;
    job.phase = 'cancelling';
    // Retire reads dispatched before the cancellation decision. The cancel
    // reply and subsequent polls are the authoritative server observations.
    for (const [requestId, request] of job.requests) {
      if (request.action === 'status') {
        clearTimeout(request.timer); job.requests.delete(requestId);
      }
    }
    clearPoll(job); publish(job);
    if (job.jobId) send(job, 'cancel', {action: 'cancel', jobId: job.jobId, table: job.payload.table});
    return true;
  };
  const start = (payload, runtimeFields = {}) => {
    if (job && isCurrent(job) && !terminalExportPhases.has(job.phase)) return job.startRequestId;
    stop(job);
    job = {payload: {...payload, action: 'start'}, runtimeFields, phase: 'queued', jobId: null,
      rowsWritten: 0, bytesWritten: 0, totalRows: null, requests: new Map(), cancelRequested: false,
      startRequestId: ports.allocateRequestId()};
    if (payload.partId) ports.setPartModal(previous => previous ? {...previous, processingPartId: payload.partId} : previous);
    ports.pendingCopyIntentRef.current = null;
    publish(job);
    return send(job, 'start', job.payload, runtimeFields, job.startRequestId);
  };
  const download = async (candidate, payload, delivery) => {
    const ref = payload.payloadRef;
    candidate.downloadAbort = new AbortController();
    try {
      if (!ref || !ref.url) throw new Error('Missing completed export payload.');
      const blob = await delivery.fetchExportPayload(ref, {signal: candidate.downloadAbort.signal});
      if (!isCurrent(candidate) || candidate.downloadAbort.signal.aborted) return;
      candidate.downloadUrl = URL.createObjectURL(blob);
      delivery.triggerExportDownload(candidate.downloadUrl, ref.filename || (candidate.payload.format === 'csv' ? 'pivot.csv' : 'pivot.xlsx'));
      candidate.revokeTimer = setTimeout(() => {
        URL.revokeObjectURL(candidate.downloadUrl); candidate.downloadUrl = null;
      }, 1000);
      finishPart(candidate, true);
      candidate.delivery = 'delivered';
      publish(candidate);
      delivery.showNotification(`Downloaded ${candidate.rowsWritten} rows`, 'success');
    } catch (error) {
      if (!isCurrent(candidate) || candidate.downloadAbort.signal.aborted) return;
      // Server completion is truthful even when browser delivery fails.
      candidate.delivery = 'failed';
      candidate.deliveryFailed = true;
      publish(candidate);
      finishPart(candidate, false);
      delivery.showNotification('Export download failed. Retry the export.', 'error');
    }
  };
  return {
    configure: nextPorts => {ports = nextPorts;}, start, poll, cancel,
    retry: () => job && start(job.payload, job.runtimeFields),
    dismiss: () => {
      if (job?.delivery === 'pending') return;
      stop(job); job = null; ports.setExportProgress(null); ports.setIsExporting(false);
    },
    dispose: () => {stop(job);},
    handleResponse: (response, delivery) => {
      const requestId = response.requestId;
      if (!ownedRequests.has(requestId)) return false;
      const candidate = job;
      if (!candidate || !isCurrent(candidate) || terminalExportPhases.has(candidate.phase)) return true;
      const payload = response.payload || {};
      const request = candidate.requests.get(requestId) || (
        !candidate.cancelRequested && response.status === 'export_job'
        && payload.phase === 'completed' && candidate.jobId === payload.jobId
        ? {action: 'status', timer: null} : null);
      if (!request) return true;
      clearTimeout(request.timer); candidate.requests.delete(requestId);
      if (response.status !== 'export_job') {
        if (candidate.cancelRequested && payload.code !== 'export_job_not_found') schedulePoll(candidate);
        else terminal(candidate, payload.code === 'export_job_not_found' ? 'interrupted' : 'failed');
        return true;
      }
      if (!payload.jobId || (candidate.jobId && candidate.jobId !== payload.jobId)) return true;
      candidate.jobId = payload.jobId;
      candidate.rowsWritten = Math.max(candidate.rowsWritten, Number(payload.rowsWritten) || 0);
      candidate.bytesWritten = Math.max(candidate.bytesWritten, Number(payload.bytesWritten) || 0);
      candidate.totalRows = Number.isFinite(payload.totalRows) && payload.totalRows >= candidate.rowsWritten ? payload.totalRows : null;
      if (candidate.cancelRequested && (request.action === 'start' ||
          request.action === 'status' && ['queued', 'running'].includes(payload.phase)) && !terminalExportPhases.has(payload.phase)) {
        // A timeout or failed dispatch may mean the first cancel never reached
        // the server. Repeat the idempotent action until its state acknowledges it.
        send(candidate, 'cancel', {action: 'cancel', jobId: candidate.jobId, table: candidate.payload.table});
      }
      if (terminalExportPhases.has(payload.phase)) {
        if (payload.phase === 'completed') candidate.delivery = 'pending';
        terminal(candidate, payload.phase);
        if (payload.phase === 'completed') void download(candidate, payload, delivery);
        return true;
      }
      if (!candidate.cancelRequested && payload.phase === 'running') candidate.phase = 'running';
      if (payload.phase === 'cancelling') {candidate.phase = 'cancelling'; candidate.cancelRequested = true;}
      publish(candidate);
      if (![...candidate.requests.values()].some(pending => !pending.expired && pending.action === 'cancel')) schedulePoll(candidate);
      return true;
    },
  };
}

/**
 * Creates the export request and download commands for the workspace.
 *
 * Payload construction lives in useRuntimeClipboardExportPresentation. This
 * module owns only command flow: request IDs, cancellation, part scheduling,
 * and the client-side XLSX/CSV fallback.
 */
export function createRuntimeExportCommands({
  abortGenerationRef,
  activeExportRequestIdRef,
  buildExportStyleProfile,
  cancelledExportRequestIdsRef,
  clientInstanceRef,
  COPY_ROW_LIMIT,
  dispatchServerSideRuntimeSetProps,
  EXPORT_ROW_LIMIT,
  exportRequestSequenceRef,
  exportJobControllerRef = {current: null},
  getDisplayHeaderTextForColumn,
  getDisplayHeaderTextStackForColumn,
  getDisplayRows,
  getExportCellDisplayValue,
  getExportColumns,
  getExportHeaderLabel,
  getExportRawCellValue,
  getServerExportColumnTarget,
  layoutMode,
  mergedHeaderSeparator,
  partModal,
  pendingCopyIntentRef,
  rowCount,
  serverSide,
  setExportProgress,
  setIsCopying,
  setIsExporting,
  setPartModal,
  table,
  tableName,
  theme,
}) {
  const getServerExportStyleIds = columnTarget => {
    if (columnTarget && Array.isArray(columnTarget.colIds) && columnTarget.colIds.length > 0) return columnTarget.colIds;
    return columnTarget && Array.isArray(columnTarget.knownColIds) ? columnTarget.knownColIds : [];
  };

  const buildServerExportPayload = ({
    format, includeHeaders, rowStart, rowEnd, columnTarget, styleOptions,
    partId, partLabel, colStart = null, colEnd = null, allRows = false,
  }) => {
    const payload = {
      format,
      includeHeaders,
      table: tableName,
      // The server cannot infer layout from the state override. Send it
      // explicitly so hierarchy, outline, and tabular exports project the
      // same rows as the active grid.
      layout_mode: layoutMode,
      rowStart,
      rowEnd,
      allRows: Boolean(allRows),
      style: buildExportStyleProfile(getServerExportStyleIds(columnTarget), styleOptions),
    };
    if (partId) payload.partId = partId;
    if (partLabel) payload.partLabel = partLabel;
    if (columnTarget && Array.isArray(columnTarget.colIds) && columnTarget.colIds.length > 0) payload.colIds = columnTarget.colIds;
    if (Number.isFinite(colStart) && Number.isFinite(colEnd) && colEnd >= colStart) {
      payload.col_start = colStart;
      payload.col_end = colEnd;
    }
    return payload;
  };

  const allocateRequestId = () => {
    exportRequestSequenceRef.current += 1;
    return `export:${clientInstanceRef.current}:${Date.now()}:${exportRequestSequenceRef.current}`;
  };
  const dispatchServerExportRequest = (payload, runtimeFields = {}, replayRequestId = null) => {
    const requestId = replayRequestId || allocateRequestId();
    activeExportRequestIdRef.current = requestId;
    dispatchServerSideRuntimeSetProps({
      runtimeRequest: {
        schemaVersion: 1, kind: 'export', requestId, abort_generation: abortGenerationRef.current,
        ...runtimeFields, payload,
      },
    });
    return requestId;
  };

  const jobPorts = {dispatch: dispatchServerExportRequest, allocateRequestId, activeExportRequestIdRef, pendingCopyIntentRef,
    setExportProgress, setIsExporting, setPartModal};
  if (!exportJobControllerRef.current) exportJobControllerRef.current = createExportJobController(jobPorts);
  const jobs = exportJobControllerRef.current;
  jobs.configure(jobPorts);
  const startExportJob = (format = 'xlsx', scope = {}) => {
    if (format !== 'xlsx' && format !== 'csv') throw new TypeError('Export format must be either xlsx or csv.');
    const allRows = scope.allRows !== false;
    return jobs.start(buildServerExportPayload({format, includeHeaders: true,
      rowStart: 0, rowEnd: rowCount, columnTarget: getServerExportColumnTarget({currentTable: !allRows}),
      styleOptions: {withHeaders: true, mergedHeaders: true}, ...scope, allRows}));
  };

  const cancelActiveExportRequest = () => {
    if (jobs.cancel()) return;
    jobs.dismiss();
    const activeRequestId = activeExportRequestIdRef.current;
    if (activeRequestId) {
      cancelledExportRequestIdsRef.current.add(activeRequestId);
      activeExportRequestIdRef.current = null;
    }
    pendingCopyIntentRef.current = null;
    setIsExporting(false); setIsCopying(false); setExportProgress(null); setPartModal(null);
  };

  const copyServerTableToClipboard = ({
    allRows = false,
    withHeaders = true,
    mergedHeaders = false,
  } = {}) => {
    const columnTarget = getServerExportColumnTarget({ currentTable: !allRows });
    // Current-table rows have an exact visible count and can be safely split.
    // An all-records request deliberately asks the backend to expand every
    // branch, so the visible count is not a valid partition boundary.
    if (!allRows && rowCount > COPY_ROW_LIMIT) {
      setPartModal({
        mode: 'copy', copyIntent: { allRows, withHeaders, mergedHeaders, mergedHeaderSeparator, richHtml: true },
        rowsPerPart: COPY_ROW_LIMIT, colsPerPart: columnTarget.totalCols, totalRows: rowCount, totalCols: columnTarget.totalCols,
        visibleColIds: columnTarget.colIds, knownColIds: columnTarget.knownColIds, columnMode: columnTarget.mode,
        processingPartId: null, completedParts: new Set(),
      });
      return;
    }
    pendingCopyIntentRef.current = { allRows, withHeaders, mergedHeaders, mergedHeaderSeparator, richHtml: true };
    setIsCopying(true);
    dispatchServerExportRequest(buildServerExportPayload({
      format: 'html', includeHeaders: withHeaders, rowStart: 0, rowEnd: rowCount, columnTarget,
      styleOptions: { withHeaders, mergedHeaders }, allRows,
    }));
  };

  // These names are deliberately about the user's intent. A current-table
  // copy preserves the current expansion, sort, filters, and layout through
  // the state_override injected by the command runtime. An all-records copy
  // opts into the backend's full-hierarchy path, including collapsed rows.
  const copyCurrentTableToClipboard = (withHeaders = true, mergedHeaders = false) => {
    copyServerTableToClipboard({ allRows: false, withHeaders, mergedHeaders });
  };

  const copyAllRecordsToClipboard = (withHeaders = true, mergedHeaders = false) => {
    copyServerTableToClipboard({ allRows: true, withHeaders, mergedHeaders });
  };

  const exportPivot = (format = 'xlsx') => {
    if (format !== 'xlsx' && format !== 'csv') {
      throw new TypeError('Export format must be either xlsx or csv.');
    }
    if (serverSide) {
      const columnTarget = getServerExportColumnTarget();
      if (rowCount > EXPORT_ROW_LIMIT) {
        setPartModal({
          mode: 'export', copyIntent: null, rowsPerPart: EXPORT_ROW_LIMIT, colsPerPart: columnTarget.totalCols,
          totalRows: rowCount, totalCols: columnTarget.totalCols, visibleColIds: columnTarget.colIds,
          knownColIds: columnTarget.knownColIds, columnMode: columnTarget.mode, exportFormat: format,
          processingPartId: null, completedParts: new Set(),
        });
        return;
      }
      startExportJob(format, {columnTarget, allRows: true});
      return;
    }

    setIsExporting(true); setExportProgress(null);
    // Defer export one tick so the overlay renders before loading and building
    // the on-demand writer.
    setTimeout(async () => {
      try {
        const displayRows = getDisplayRows();
        const exportWriter = format === 'csv' ? exportPivotTableCsv : exportPivotTable;
        await exportWriter(table, rowCount, displayRows.length > 0 ? displayRows : null, {
          filename: format === 'csv' ? 'pivot.csv' : 'pivot.xlsx', columns: getExportColumns(),
          getHeaderLabel: column => getExportHeaderLabel(column, true, mergedHeaderSeparator),
          getHeaderStack: column => getDisplayHeaderTextStackForColumn(column),
          getCellValue: getExportCellDisplayValue, getCellRawValue: getExportRawCellValue,
          isHierarchyCol: column => !!(column && column.id === 'hierarchy'),
          isHeaderTotalCol: column => {
            const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
            return !!(parentText && (parentText === 'Grand Total' || String(parentText).startsWith('Grand Total')));
          },
          isTotalRow: rowLike => {
            const data = (rowLike && rowLike.original !== undefined ? rowLike.original : rowLike) || {};
            return !!(data._isTotal || data.__isGrandTotal__ || data._path === '__grand_total__' || data._id === 'Grand Total');
          },
          isTotalCol: column => {
            const parentText = column && column.parent ? getDisplayHeaderTextForColumn(column.parent) : '';
            return !!(parentText && (parentText === 'Grand Total' || String(parentText).startsWith('Grand Total')));
          },
          themeColors: theme,
        });
      } finally {
        setIsExporting(false); setExportProgress(null);
      }
    }, 50);
  };

  const dispatchExportPart = (rowBatch, colBatch) => {
    if (!partModal) return;
    const { mode, copyIntent, rowsPerPart, colsPerPart, totalRows, totalCols, visibleColIds, knownColIds, columnMode, exportFormat = 'xlsx' } = partModal;
    const rowStart = rowBatch * rowsPerPart;
    const rowEnd = Math.min(rowStart + rowsPerPart, totalRows);
    const colStart = colBatch * colsPerPart;
    const colEnd = Math.min(colStart + colsPerPart, totalCols);
    const colBatches = colsPerPart < totalCols ? Math.ceil(totalCols / colsPerPart) : 1;
    const useIdColumns = Array.isArray(visibleColIds) && visibleColIds.length > 0;
    const colIds = useIdColumns ? visibleColIds.slice(colStart, colEnd) : null;
    const useColumnWindow = !useIdColumns && columnMode === 'window' && colBatches > 1;
    const partId = `r${rowBatch}_c${colBatch}`;
    const partLabel = colBatches > 1 ? `rows${rowStart + 1}-${rowEnd}_cols${colStart + 1}-${colEnd}` : `rows${rowStart + 1}-${rowEnd}`;
    setPartModal(previous => previous ? { ...previous, processingPartId: partId } : null);
    pendingCopyIntentRef.current = mode === 'copy' ? copyIntent : null;
    const isCopyMode = mode === 'copy';
    const partColumnTarget = { mode: useIdColumns ? 'ids' : 'window', colIds, knownColIds: Array.isArray(knownColIds) ? knownColIds : [] };
    const runtimeFields = useColumnWindow ? { col_start: colStart, col_end: colEnd - 1 } : {};
    const payload = buildServerExportPayload({
      format: isCopyMode ? 'html' : exportFormat, includeHeaders: isCopyMode ? !!(copyIntent && copyIntent.withHeaders) : true,
      rowStart, rowEnd, columnTarget: partColumnTarget, partId, partLabel,
      colStart: useColumnWindow ? colStart : null, colEnd: useColumnWindow ? colEnd - 1 : null,
      styleOptions: { withHeaders: isCopyMode ? !!(copyIntent && copyIntent.withHeaders) : true, mergedHeaders: isCopyMode ? !!(copyIntent && copyIntent.mergedHeaders) : true },
      allRows: false,
    });
    if (isCopyMode) dispatchServerExportRequest(payload, runtimeFields);
    else jobs.start(payload, runtimeFields);
  };

  return {
    buildServerExportPayload, cancelActiveExportRequest, copyAllRecordsToClipboard,
    copyCurrentTableToClipboard,
    dispatchExportPart, dispatchServerExportRequest, exportPivot, getServerExportStyleIds,
    startExportJob, pollExportJob: jobs.poll, cancelExportJob: jobs.cancel,
    retryExportJob: jobs.retry, dismissExportJob: jobs.dismiss,
  };
}

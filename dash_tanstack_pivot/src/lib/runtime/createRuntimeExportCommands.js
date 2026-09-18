import { exportPivotTable } from '../utils/exportUtils';

/**
 * Creates the export request and download commands for the workspace.
 *
 * Payload construction lives in useRuntimeClipboardExportPresentation. This
 * module owns only command flow: request IDs, cancellation, part scheduling,
 * and the client-side XLSX fallback.
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

  const dispatchServerExportRequest = (payload, runtimeFields = {}) => {
    exportRequestSequenceRef.current += 1;
    const requestId = `export:${clientInstanceRef.current}:${Date.now()}:${exportRequestSequenceRef.current}`;
    activeExportRequestIdRef.current = requestId;
    dispatchServerSideRuntimeSetProps({
      runtimeRequest: {
        kind: 'export', requestId, abort_generation: abortGenerationRef.current,
        ...runtimeFields, payload,
      },
    });
    return requestId;
  };

  const cancelActiveExportRequest = () => {
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

  const exportPivot = () => {
    if (serverSide) {
      const columnTarget = getServerExportColumnTarget();
      if (rowCount > EXPORT_ROW_LIMIT) {
        setPartModal({
          mode: 'export', copyIntent: null, rowsPerPart: EXPORT_ROW_LIMIT, colsPerPart: columnTarget.totalCols,
          totalRows: rowCount, totalCols: columnTarget.totalCols, visibleColIds: columnTarget.colIds,
          knownColIds: columnTarget.knownColIds, columnMode: columnTarget.mode, processingPartId: null, completedParts: new Set(),
        });
        return;
      }
      setIsExporting(true);
      dispatchServerExportRequest(buildServerExportPayload({
        format: 'xlsx', includeHeaders: true, rowStart: 0, rowEnd: rowCount, columnTarget,
        styleOptions: { withHeaders: true, mergedHeaders: true }, allRows: true,
      }));
      return;
    }

    setIsExporting(true); setExportProgress(null);
    // Defer export one tick so the overlay renders before loading and building
    // the on-demand XLSX writer.
    setTimeout(async () => {
      try {
        const displayRows = getDisplayRows();
        await exportPivotTable(table, rowCount, displayRows.length > 0 ? displayRows : null, {
          filename: 'pivot.xlsx', columns: getExportColumns(),
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
    const { mode, copyIntent, rowsPerPart, colsPerPart, totalRows, totalCols, visibleColIds, knownColIds, columnMode } = partModal;
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
    dispatchServerExportRequest(buildServerExportPayload({
      format: isCopyMode ? 'html' : 'xlsx', includeHeaders: isCopyMode ? !!(copyIntent && copyIntent.withHeaders) : true,
      rowStart, rowEnd, columnTarget: partColumnTarget, partId, partLabel,
      colStart: useColumnWindow ? colStart : null, colEnd: useColumnWindow ? colEnd - 1 : null,
      styleOptions: { withHeaders: isCopyMode ? !!(copyIntent && copyIntent.withHeaders) : true, mergedHeaders: isCopyMode ? !!(copyIntent && copyIntent.mergedHeaders) : true },
      allRows: false,
    }), runtimeFields);
  };

  return {
    buildServerExportPayload, cancelActiveExportRequest, copyAllRecordsToClipboard,
    copyCurrentTableToClipboard,
    dispatchExportPart, dispatchServerExportRequest, exportPivot, getServerExportStyleIds,
  };
}

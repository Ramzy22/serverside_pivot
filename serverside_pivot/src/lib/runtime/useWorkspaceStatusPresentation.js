import { useCallback, useMemo } from 'react';

import { countActiveFilters } from '../hooks/usePivotNormalization';
import { formatValue } from '../utils/helpers';

/** @param {string} [selectionKey] @returns {string} */
const selectionKeyColumnId = (selectionKey) => {
  const key = String(selectionKey || '');
  const separator = key.lastIndexOf(':');
  return separator >= 0 ? key.slice(separator + 1) : '';
};

/**
 * Builds the status-bar model and its commands from named runtime inputs.
 *
 * It has no dependency on the legacy mutable runtime record, which keeps
 * status presentation from becoming a back-channel between grid, editing,
 * transport and chart coordination.
  @param {any} p0 @returns {any} */
export function useWorkspaceStatusPresentation({
  chartCanvasPanes,
  clearAllComparisonValueState,
  clearAllEditedCellMarkers,
  clearAllOptimisticCellValues,
  colFields,
  columnAdvisory,
  comparisonCellOriginalValuesRef,
  comparisonValueState,
  dispatchTransactionRequest,
  editedCellMarkersRef,
  editValueDisplayMode,
  filters,
  handleClearStatusFilters,
  handleClearStatusSelection,
  handleRedo,
  handleRefreshStatusViewport,
  handleUndo,
  isRequestPending,
  loadedRows,
  managedChartDefinitions,
  propagationLogRef,
  propagationLogEpoch,
  requestVisibleViewportRefresh,
  resolveColumnValueFormat,
  numberGroupSeparator,
  rowEditStatusSummary,
  rowFields,
  rows,
  selectedCells,
  serverSide,
  setEditValueDisplayMode,
  setPropagationLogEpoch,
  setPropsRef,
  showNotification,
  sorting,
  statusRowCount,
  supportsPatchTransactionRefresh,
  syncComparisonValueState,
  syncTransactionHistoryState,
  totalCenterCols,
  transactionHistoryState,
  valConfigs,
  viewMode,
}) {
  // Selection statistics (sum, mean, …) read like the cells they summarise:
  // when every numeric selected cell shares one column format, the stats use
  // it; mixed formats fall back to a plain grouped number.
  /** @type {any} */
  const sharedSelectionFormat = useMemo(() => {
    if (typeof resolveColumnValueFormat !== 'function') return null;
    /** @type {Map<string, any>} */
    const formatsBySignature = new Map();
    for (const [selectionKey, value] of Object.entries(selectedCells || {})) {
      if (!Number.isFinite(Number(value)) || value === null || value === '') continue;
      const colId = selectionKeyColumnId(selectionKey);
      if (!colId) continue;
      const columnFormat = resolveColumnValueFormat(colId);
      formatsBySignature.set(JSON.stringify(columnFormat), columnFormat);
      if (formatsBySignature.size > 1) return null;
    }
    return formatsBySignature.size === 1 ? formatsBySignature.values().next().value : null;
  }, [resolveColumnValueFormat, selectedCells]);
  /** @type {(value: any, fallbackDecimals?: number) => string} */
  const formatSelectionValue = useCallback((value, fallbackDecimals = 2) => {
    if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '--';
    if (sharedSelectionFormat) {
      return formatValue(Number(value), sharedSelectionFormat.format, sharedSelectionFormat.decimalPlaces ?? fallbackDecimals, sharedSelectionFormat.groupSeparator, {
        valueScale: sharedSelectionFormat.valueScale,
        unitSuffix: sharedSelectionFormat.unitSuffix,
      });
    }
    return formatValue(Number(value), null, fallbackDecimals, numberGroupSeparator);
  }, [numberGroupSeparator, sharedSelectionFormat]);
  /** @type {any} */
  const statusAccessoryModel = useMemo(() => ({
    selection: { selectedCells, formatValue: formatSelectionValue },
    data: {
      viewMode,
      rowCount: statusRowCount,
      visibleRowsCount: serverSide ? loadedRows.length : rows.length,
      totalCenterColumns: totalCenterCols,
      rowFieldCount: rowFields.length,
      columnFieldCount: colFields.length,
      measureCount: valConfigs.length,
      sortingCount: Array.isArray(sorting) ? sorting.length : 0,
      activeFilterCount: countActiveFilters(filters),
      globalSearch: typeof (filters && filters.global) === 'string' ? filters.global.trim() : '',
      columnAdvisory
    },
    runtime: {
      loading: isRequestPending,
      serverSide,
      loadedRowCount: serverSide ? loadedRows.length : rows.length
    },
    editing: {
      displayMode: editValueDisplayMode,
      comparedValueCount: comparisonValueState.activeCount,
      undoCount: transactionHistoryState.undoCount,
      redoCount: transactionHistoryState.redoCount,
      pending: transactionHistoryState.pending,
      activeRowEditCount: rowEditStatusSummary.activeRowCount,
      savingRowCount: rowEditStatusSummary.savingRowCount,
      dirtyRowCount: rowEditStatusSummary.dirtyRowCount,
      dirtyFieldCount: rowEditStatusSummary.dirtyFieldCount,
      invalidRowCount: rowEditStatusSummary.invalidRowCount,
      invalidFieldCount: rowEditStatusSummary.invalidFieldCount,
      propagationLog: propagationLogRef.current,
      propagationLogCount: propagationLogRef.current.length
    },
    charts: {
      definitionCount: managedChartDefinitions.length,
      paneCount: chartCanvasPanes.length,
      dockedPaneCount: chartCanvasPanes.filter(/** @param {any} [pane] @returns {any} */ pane => !pane.floating).length,
      floatingPaneCount: chartCanvasPanes.filter(/** @param {any} [pane] @returns {any} */ pane => pane.floating).length
    }
  }), [chartCanvasPanes, colFields.length, columnAdvisory, comparisonValueState.activeCount, editValueDisplayMode, filters, isRequestPending, loadedRows.length, managedChartDefinitions.length, rowEditStatusSummary, rowFields.length, rows.length, serverSide, sorting, statusRowCount, totalCenterCols, transactionHistoryState.pending, transactionHistoryState.redoCount, transactionHistoryState.undoCount, propagationLogEpoch, valConfigs.length, viewMode, selectedCells, formatSelectionValue]);
  /** @type {any} */
  const statusAccessoryActions = useMemo(() => ({
    canClearSelection: Object.keys(selectedCells || {}).length > 0,
    canClearFilters: countActiveFilters(filters) > 0,
    canRefreshViewport: Boolean(serverSide && !isRequestPending && typeof requestVisibleViewportRefresh === 'function'),
    canUndo: transactionHistoryState.undoCount > 0 && !transactionHistoryState.pending,
    canRedo: transactionHistoryState.redoCount > 0 && !transactionHistoryState.pending,
    canToggleOriginal: comparisonValueState.activeCount > 0,
    canRevertAll: comparisonValueState.activeCount > 0 || editedCellMarkersRef.current.size > 0,
    isShowingOriginal: editValueDisplayMode === 'original' && comparisonValueState.activeCount > 0,
    onClearSelection: handleClearStatusSelection,
    onClearFilters: handleClearStatusFilters,
    onRefreshViewport: handleRefreshStatusViewport,
    onToggleOriginal: () => setEditValueDisplayMode(/** @param {any} [prev] @returns {any} */ prev => prev === 'original' ? 'edited' : 'original'),
    onRevertAll: () => {
      /** @type {any[]} */
      const revertUpdates = [];
      comparisonCellOriginalValuesRef.current.forEach(/** @param {any} [entry] @returns {any} */ entry => {
        if (!entry || entry.value === undefined) return;
        revertUpdates.push({ rowId: entry.rowId, colId: entry.colId, value: entry.value });
      });
      clearAllOptimisticCellValues();
      clearAllEditedCellMarkers();
      clearAllComparisonValueState();
      propagationLogRef.current = [];
      setPropagationLogEpoch(/** @param {any} [prev] @returns {any} */ prev => prev + 1);
      syncTransactionHistoryState();
      if (revertUpdates.length > 0) {
        if (serverSide) {
          dispatchTransactionRequest({
            update: revertUpdates,
            refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart'
          }, 'revert-all');
        } else if (setPropsRef.current) {
          setPropsRef.current({ cellUpdates: revertUpdates });
        }
      }
      showNotification('Reverted all edits to original values.', 'info');
    },
    onUndo: handleUndo,
    onRedo: handleRedo
  }), [filters, handleClearStatusFilters, handleClearStatusSelection, handleRedo, handleRefreshStatusViewport, handleUndo, isRequestPending, requestVisibleViewportRefresh, clearAllComparisonValueState, clearAllEditedCellMarkers, clearAllOptimisticCellValues, comparisonValueState.activeCount, editValueDisplayMode, selectedCells, serverSide, showNotification, syncTransactionHistoryState, dispatchTransactionRequest, supportsPatchTransactionRefresh, transactionHistoryState.pending, transactionHistoryState.redoCount, transactionHistoryState.undoCount, editedCellMarkersRef]);

  return { statusAccessoryActions, statusAccessoryModel };
}

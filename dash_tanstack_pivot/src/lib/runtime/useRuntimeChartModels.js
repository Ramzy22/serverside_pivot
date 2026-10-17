import { useCallback, useMemo } from 'react';
import { getDisplayOrderedLeafColumns } from './displayColumnOrder';

import {
  TABLE_OVERLAY_CHART_PANE_ID,
  cloneSerializable,
  resolveChartModelColumns
} from '../hooks/usePivotNormalization';
import {
  buildComboPivotChartModel,
  buildComboSelectionChartModel,
  buildPivotChartModel,
  buildSelectionChartModel
} from '../utils/chartModelBuilders';

/**
 * Builds the chart domain from its named dependencies.
 *
 * This hook intentionally accepts a narrow chart dependency object instead of
 * the mutable legacy runtime record.  Its result is a plain chart model and
 * chart-only callbacks; the viewport stage merely publishes that result while
 * the compatibility bridge remains in place.
 */
export function useRuntimeChartModels({
  buildChartRequestBase,
  buildChartStateOverrideSnapshot,
  chartCanvasPanes,
  chartPaneDataById,
  chartPanelColumnLimit,
  chartPanelHierarchyLevel,
  chartPanelInteractionMode,
  chartPanelLayers,
  chartPanelLocked,
  chartPanelLockedModel,
  chartPanelLockedRequest,
  chartPanelOpen,
  chartPanelOrientation,
  chartPanelRowLimit,
  chartPanelServerScope,
  chartPanelSource,
  chartPanelSortMode,
  chartPanelType,
  chartModal,
  colFields,
  columnOrderKey,
  columnPinning,
  columnSizing,
  columnVisibility,
  columns,
  effectiveBottomRows,
  effectiveCenterRows,
  effectiveTopRows,
  getDisplayRows,
  handleRowSelect,
  loadedRows,
  resolveDisplayRowIndex,
  rowFields,
  scrollToDisplayColumn,
  scrollToDisplayRow,
  serverSide,
  setChartPanelLocked,
  setChartPanelLockedModel,
  setChartPanelLockedRequest,
  setFilters,
  setPropsRef,
  showNotification,
  table,
  updateChartCanvasPane,
  selectedCells,
}) {
  const chartModelingActive = chartPanelOpen || chartCanvasPanes.length > 0 || !!chartModal || chartPanelLocked || Object.keys(chartPaneDataById).length > 0;
  const visibleLeafColumns = useMemo(() => getDisplayOrderedLeafColumns(table), [table, columns, columnVisibility, columnPinning, columnOrderKey, columnSizing]);
  const chartDisplayRows = useMemo(() => chartModelingActive ? [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows] : [], [chartModelingActive, effectiveTopRows, effectiveCenterRows, effectiveBottomRows]);
  const liveSelectionChartModel = useMemo(() => !chartModelingActive ? null : chartPanelType === 'combo' ? buildComboSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
    hierarchyLevel: chartPanelHierarchyLevel,
    maxHierarchyLevel: rowFields.length,
    maxRows: chartPanelRowLimit,
    maxColumns: chartPanelColumnLimit,
    rowFields,
    colFields,
    sortMode: chartPanelSortMode,
    layers: chartPanelLayers
  }) : buildSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
    orientation: chartPanelOrientation,
    hierarchyLevel: chartPanelHierarchyLevel,
    maxHierarchyLevel: rowFields.length,
    maxRows: chartPanelRowLimit,
    maxColumns: chartPanelColumnLimit,
    rowFields,
    colFields,
    sortMode: chartPanelSortMode
  }), [chartModelingActive, selectedCells, chartDisplayRows, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]);
  const livePivotChartModel = useMemo(() => !chartModelingActive ? null : chartPanelType === 'combo' ? buildComboPivotChartModel(serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows, visibleLeafColumns, {
    hierarchyLevel: chartPanelHierarchyLevel,
    maxHierarchyLevel: rowFields.length,
    maxRows: chartPanelRowLimit,
    maxColumns: chartPanelColumnLimit,
    rowFields,
    colFields,
    sortMode: chartPanelSortMode,
    layers: chartPanelLayers
  }) : buildPivotChartModel(serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows, visibleLeafColumns, {
    orientation: chartPanelOrientation,
    hierarchyLevel: chartPanelHierarchyLevel,
    maxHierarchyLevel: rowFields.length,
    maxRows: chartPanelRowLimit,
    maxColumns: chartPanelColumnLimit,
    rowFields,
    colFields,
    sortMode: chartPanelSortMode
  }), [chartModelingActive, chartDisplayRows, loadedRows, serverSide, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]);
  const overlayChartData = chartPaneDataById[TABLE_OVERLAY_CHART_PANE_ID] || null;
  const activeChartPanelModel = useMemo(() => {
    if (chartPanelLocked) {
      if (chartPanelSource === 'pivot' && overlayChartData && overlayChartData.requestSignature && chartPanelLockedRequest && overlayChartData.requestSignature === chartPanelLockedRequest.requestSignature && Array.isArray(overlayChartData.rows)) {
        return chartPanelType === 'combo' ? buildComboPivotChartModel(overlayChartData.rows, resolveChartModelColumns(overlayChartData, chartPanelLockedRequest.visibleColumns || visibleLeafColumns), {
          hierarchyLevel: chartPanelHierarchyLevel,
          maxHierarchyLevel: rowFields.length,
          maxRows: chartPanelRowLimit,
          maxColumns: chartPanelColumnLimit,
          rowFields,
          colFields,
          sortMode: chartPanelSortMode,
          layers: chartPanelLayers
        }) : buildPivotChartModel(overlayChartData.rows, resolveChartModelColumns(overlayChartData, chartPanelLockedRequest.visibleColumns || visibleLeafColumns), {
          orientation: chartPanelOrientation,
          hierarchyLevel: chartPanelHierarchyLevel,
          maxHierarchyLevel: rowFields.length,
          maxRows: chartPanelRowLimit,
          maxColumns: chartPanelColumnLimit,
          rowFields,
          colFields,
          sortMode: chartPanelSortMode
        });
      }
      return chartPanelLockedModel || (chartPanelSource === 'selection' ? liveSelectionChartModel : livePivotChartModel);
    }
    if (chartPanelSource === 'pivot' && overlayChartData && Array.isArray(overlayChartData.rows)) {
      const overlayModelColumns = resolveChartModelColumns(overlayChartData, visibleLeafColumns);
      return chartPanelType === 'combo' ? buildComboPivotChartModel(overlayChartData.rows, overlayModelColumns, {
        hierarchyLevel: chartPanelHierarchyLevel,
        maxHierarchyLevel: rowFields.length,
        maxRows: chartPanelRowLimit,
        maxColumns: chartPanelColumnLimit,
        rowFields,
        colFields,
        sortMode: chartPanelSortMode,
        layers: chartPanelLayers
      }) : buildPivotChartModel(overlayChartData.rows, overlayModelColumns, {
        orientation: chartPanelOrientation,
        hierarchyLevel: chartPanelHierarchyLevel,
        maxHierarchyLevel: rowFields.length,
        maxRows: chartPanelRowLimit,
        maxColumns: chartPanelColumnLimit,
        rowFields,
        colFields,
        sortMode: chartPanelSortMode
      });
    }
    return chartPanelSource === 'selection' ? liveSelectionChartModel : livePivotChartModel;
  }, [chartPanelLocked, chartPanelLayers, chartPanelLockedModel, chartPanelLockedRequest, chartPanelSource, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, chartPanelRowLimit, chartPanelColumnLimit, chartPanelSortMode, colFields, livePivotChartModel, liveSelectionChartModel, overlayChartData, rowFields, visibleLeafColumns]);
  const chartCanvasPaneModels = useMemo(() => {
    if (chartCanvasPanes.length === 0) return {};
    const nextModels = {};
    chartCanvasPanes.forEach(pane => {
      if (!pane || !pane.id) return;
      if (pane.locked) {
        if (pane.source === 'pivot' && pane.lockedRequest && pane.lockedRequest.requestSignature && chartPaneDataById[pane.id] && chartPaneDataById[pane.id].requestSignature === pane.lockedRequest.requestSignature && Array.isArray(chartPaneDataById[pane.id].rows)) {
          const paneModelColumns = resolveChartModelColumns(chartPaneDataById[pane.id], pane.lockedRequest && pane.lockedRequest.visibleColumns || visibleLeafColumns);
          nextModels[pane.id] = pane.chartType === 'combo' ? buildComboPivotChartModel(chartPaneDataById[pane.id].rows, paneModelColumns, {
            hierarchyLevel: pane.hierarchyLevel,
            maxHierarchyLevel: rowFields.length,
            maxRows: pane.rowLimit,
            maxColumns: pane.columnLimit,
            rowFields,
            colFields,
            sortMode: pane.sortMode,
            layers: pane.chartLayers
          }) : buildPivotChartModel(chartPaneDataById[pane.id].rows, paneModelColumns, {
            orientation: pane.orientation,
            hierarchyLevel: pane.hierarchyLevel,
            maxHierarchyLevel: rowFields.length,
            maxRows: pane.rowLimit,
            maxColumns: pane.columnLimit,
            rowFields,
            colFields,
            sortMode: pane.sortMode
          });
          return;
        }
        nextModels[pane.id] = pane.lockedModel || null;
        return;
      }
      if (pane.source === 'selection') {
        nextModels[pane.id] = pane.chartType === 'combo' ? buildComboSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
          hierarchyLevel: pane.hierarchyLevel,
          maxHierarchyLevel: rowFields.length,
          maxRows: pane.rowLimit,
          maxColumns: pane.columnLimit,
          rowFields,
          colFields,
          sortMode: pane.sortMode,
          layers: pane.chartLayers
        }) : buildSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
          orientation: pane.orientation,
          hierarchyLevel: pane.hierarchyLevel,
          maxHierarchyLevel: rowFields.length,
          maxRows: pane.rowLimit,
          maxColumns: pane.columnLimit,
          rowFields,
          colFields,
          sortMode: pane.sortMode
        });
        return;
      }
      const paneRows = serverSide && chartPaneDataById[pane.id] && Array.isArray(chartPaneDataById[pane.id].rows) ? chartPaneDataById[pane.id].rows : serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows;
      const paneModelColumns = resolveChartModelColumns(chartPaneDataById[pane.id], visibleLeafColumns);
      nextModels[pane.id] = pane.chartType === 'combo' ? buildComboPivotChartModel(paneRows, paneModelColumns, {
        hierarchyLevel: pane.hierarchyLevel,
        maxHierarchyLevel: rowFields.length,
        maxRows: pane.rowLimit,
        maxColumns: pane.columnLimit,
        rowFields,
        colFields,
        sortMode: pane.sortMode,
        layers: pane.chartLayers
      }) : buildPivotChartModel(paneRows, paneModelColumns, {
        orientation: pane.orientation,
        hierarchyLevel: pane.hierarchyLevel,
        maxHierarchyLevel: rowFields.length,
        maxRows: pane.rowLimit,
        maxColumns: pane.columnLimit,
        rowFields,
        colFields,
        sortMode: pane.sortMode
      });
    });
    return nextModels;
  }, [chartCanvasPanes, chartDisplayRows, chartPaneDataById, colFields, loadedRows, rowFields, selectedCells, serverSide, visibleLeafColumns]);
  const handleToggleChartPanelLock = useCallback(() => {
    if (chartPanelLocked) {
      setChartPanelLocked(false);
      setChartPanelLockedModel(null);
      setChartPanelLockedRequest(null);
      return;
    }
    const nextLockedModel = cloneSerializable(activeChartPanelModel, null);
    const stateOverride = buildChartStateOverrideSnapshot();
    const lockedRequestBase = serverSide && chartPanelSource === 'pivot' ? buildChartRequestBase({
      chartType: chartPanelType,
      chartLayers: chartPanelLayers,
      rowLimit: chartPanelRowLimit,
      columnLimit: chartPanelColumnLimit,
      serverScope: chartPanelServerScope
    }, stateOverride) : null;
    const requestSignature = lockedRequestBase ? JSON.stringify({
      paneId: TABLE_OVERLAY_CHART_PANE_ID,
      locked: true,
      request: lockedRequestBase,
      stateOverride
    }) : null;
    setChartPanelLockedModel(nextLockedModel);
    setChartPanelLockedRequest(lockedRequestBase ? {
      request: lockedRequestBase,
      stateOverride,
      visibleColumns: resolveChartModelColumns(overlayChartData, visibleLeafColumns),
      requestSignature
    } : null);
    setChartPanelLocked(true);
  }, [activeChartPanelModel, buildChartRequestBase, buildChartStateOverrideSnapshot, chartPanelColumnLimit, chartPanelLayers, chartPanelLocked, chartPanelRowLimit, chartPanelServerScope, chartPanelSource, chartPanelType, overlayChartData, serverSide, visibleLeafColumns]);
  const handleToggleChartCanvasPaneLock = useCallback(paneId => {
    const targetPane = chartCanvasPanes.find(pane => pane.id === paneId);
    if (!targetPane) return;
    if (targetPane.locked) {
      updateChartCanvasPane(paneId, {
        locked: false,
        lockedModel: null,
        lockedRequest: null
      });
      return;
    }
    const targetModel = cloneSerializable(chartCanvasPaneModels[paneId], null);
    const stateOverride = buildChartStateOverrideSnapshot();
    const lockedRequestBase = serverSide && targetPane.source === 'pivot' ? buildChartRequestBase({
      chartType: targetPane.chartType,
      chartLayers: targetPane.chartLayers,
      rowLimit: targetPane.rowLimit,
      columnLimit: targetPane.columnLimit,
      serverScope: targetPane.serverScope
    }, stateOverride) : null;
    const requestSignature = lockedRequestBase ? JSON.stringify({
      paneId,
      locked: true,
      request: lockedRequestBase,
      stateOverride
    }) : null;
    updateChartCanvasPane(paneId, {
      locked: true,
      lockedModel: targetModel,
      lockedRequest: lockedRequestBase ? {
        request: lockedRequestBase,
        stateOverride,
        visibleColumns: resolveChartModelColumns(chartPaneDataById[paneId], visibleLeafColumns),
        requestSignature
      } : null
    });
  }, [buildChartRequestBase, buildChartStateOverrideSnapshot, chartCanvasPaneModels, chartCanvasPanes, chartPaneDataById, serverSide, updateChartCanvasPane, visibleLeafColumns]);
  const applyChartTargetFilters = useCallback(target => {
    if (!target || typeof target !== 'object' || !target.fieldValues || typeof target.fieldValues !== 'object') return;
    const targetFields = target.kind === 'column' ? colFields : rowFields;
    if (!Array.isArray(targetFields) || targetFields.length === 0) return;
    setFilters(previousFilters => {
      const nextFilters = {
        ...(previousFilters || {})
      };
      targetFields.forEach(field => {
        if (Object.prototype.hasOwnProperty.call(target.fieldValues, field)) {
          nextFilters[field] = {
            operator: 'AND',
            conditions: [{
              type: 'eq',
              value: String(target.fieldValues[field]),
              caseSensitive: false
            }]
          };
        } else if (nextFilters[field]) {
          delete nextFilters[field];
        }
      });
      return nextFilters;
    });
  }, [colFields, rowFields, setFilters]);
  const activateChartCategory = useCallback((source, interactionMode, target) => {
    if (!target || typeof target !== 'object') return;
    if (setPropsRef.current) {
      setPropsRef.current({
        chartEvent: {
          type: 'category_activate',
          source,
          interactionMode,
          target,
          timestamp: Date.now()
        }
      });
    }
    if (interactionMode === 'event') return;
    if (interactionMode === 'filter') {
      applyChartTargetFilters(target);
      return;
    }
    if (target.kind === 'column' && target.columnId) {
      const targetColumnIndex = visibleLeafColumns.findIndex(column => column.id === target.columnId);
      if (targetColumnIndex >= 0) {
        scrollToDisplayColumn(targetColumnIndex);
      }
      return;
    }
    if (target.kind !== 'row') return;
    const displayRows = getDisplayRows();
    const matchingRow = displayRows.find(row => {
      if (!row) return false;
      const rowPath = row.original && row.original._path ? row.original._path : null;
      return target.rowPath && rowPath === target.rowPath || target.rowId && row.id === target.rowId;
    });
    if (!matchingRow) {
      if (serverSide) {
        showNotification('Chart item is outside the current loaded grid slice.', 'info');
      }
      return;
    }
    handleRowSelect(matchingRow, false, false);
    scrollToDisplayRow(resolveDisplayRowIndex(matchingRow.id, matchingRow.index || 0));
  }, [applyChartTargetFilters, getDisplayRows, handleRowSelect, resolveDisplayRowIndex, scrollToDisplayColumn, scrollToDisplayRow, serverSide, showNotification, visibleLeafColumns]);
  const handleChartCategoryActivate = useCallback(target => {
    activateChartCategory(chartPanelSource, chartPanelInteractionMode, target);
  }, [activateChartCategory, chartPanelInteractionMode, chartPanelSource]);

  return {
    activateChartCategory,
    activeChartPanelModel,
    applyChartTargetFilters,
    chartCanvasPaneModels,
    chartDisplayRows,
    chartModelingActive,
    handleChartCategoryActivate,
    handleToggleChartCanvasPaneLock,
    handleToggleChartPanelLock,
    livePivotChartModel,
    liveSelectionChartModel,
    overlayChartData,
    visibleLeafColumns,
  };
}

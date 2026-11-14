import { useCallback, useEffect, useRef } from 'react';

import { normalizeSparklineValConfigsForView } from '../utils/sparklines';

/**
 * Owns the serializable workspace snapshot and the public save-view trigger.
 *
 * A saved view includes viewport scroll position, so this belongs to the
 * viewport stage rather than the earlier interaction stage.  Its inputs are
 * intentionally explicit: this is the persistence boundary for user-visible
 * workspace state, not another consumer of the mutable legacy environment.
  @param {any} p0 @returns {any} */
export function useRuntimeViewSnapshot({
  activeChartDefinitionId,
  activeReportId,
  cellFormatRules,
  chartCanvasPanes,
  chartPanelAxisMode,
  chartPanelBarLayout,
  chartPanelColumnLimit,
  chartPanelFloating,
  chartPanelFloatingRect,
  chartPanelGraphHeight,
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
  chartPanelSortMode,
  chartPanelSource,
  chartPanelTitle,
  chartPanelType,
  chartPanelWidth,
  colExpanded,
  colFields,
  colSearch,
  colTypeFilter,
  colorPalette,
  colorScaleMode,
  colorScaleColumns,
  columnDecimalOverrides,
  columnFormatOverrides,
  columnGroupSeparatorOverrides,
  columnOrder,
  pivotLayoutOrder,
  pivotPinnedLayoutIds,
  columnPinning,
  columnSizing,
  columnVisibility,
  customDimensions,
  dataBarsColumns,
  dataBarMaximums,
  decimalPlaces,
  defaultValueFormat,
  detailConfig,
  detailMode,
  editValueDisplayMode,
  expanded,
  fieldPanelSizes,
  filters,
  frozenPivotConfig,
  grandTotalPinOverride,
  latestViewportRef,
  layoutMode,
  managedChartDefinitions,
  normalizedChartServerWindow,
  normalizedInitialValConfigs,
  numberGroupSeparator,
  parentRef,
  pivotColumnSorting,
  reportDef,
  rowFields,
  rowPinning,
  runtimeMeasureAxis,
  saveViewTrigger,
  savePersistedState,
  savedReports,
  serializedEditComparisonState,
  setPropsRef,
  showColTotals,
  showFloatingFilters,
  showNotification,
  showReportConfigColumn,
  showRowNumbers,
  showRowTotals,
  showSubtotals,
  sidebarOpen,
  sidebarTab,
  sidebarWidth,
  sorting,
  spacingMode,
  stickyHeaders,
  tableCanvasSize,
  tableName,
  themeName,
  themeOverrides,
  treeConfig,
  valConfigs,
  viewMode,
  zoomLevel,
}) {
  /** @type {(...args: any[]) => any} */
  const buildCurrentViewState = useCallback(() => {
    /** @type {Record<string, any>} */
    const normalizedFilters = {
      ...(filters || {}),
    };
    return {
      version: 1,
      table: tableName || null,
      viewport: latestViewportRef.current || null,
      state: {
        viewMode,
        detailMode,
        treeConfig,
        detailConfig,
        reportDef,
        savedReports,
        activeReportId,
        frozenPivotConfig,
        customDimensions,
        measureAxis: runtimeMeasureAxis,
        rowFields,
        colFields,
        valConfigs: normalizeSparklineValConfigsForView(valConfigs, normalizedInitialValConfigs),
        filters: normalizedFilters,
        sorting,
        expanded,
        showRowTotals,
        showColTotals,
        showSubtotals,
        showRowNumbers,
        showReportConfigColumn,
        sidebarOpen,
        sidebarWidth,
        sidebarTab,
        fieldPanelSizes,
        showFloatingFilters,
        stickyHeaders,
        colSearch,
        colTypeFilter,
        themeName,
        themeOverrides,
        editValueDisplayMode,
        editComparisonState: serializedEditComparisonState,
        layoutMode,
        colorScaleMode,
        colorScaleColumns,
        colorPalette,
        spacingMode,
        dataBarsColumns: Array.from(dataBarsColumns || []).sort(),
        dataBarMaximums: { ...(dataBarMaximums || {}) },
        pivotColumnSorting,
        columnPinning,
        rowPinning,
        grandTotalPinOverride,
        columnVisibility,
        columnOrder,
        pivotLayoutOrder,
        pivotPinnedLayoutIds,
        columnSizing,
        colExpanded,
        decimalPlaces,
        defaultValueFormat,
        numberGroupSeparator,
        zoomLevel,
        columnDecimalOverrides,
        columnFormatOverrides,
        columnGroupSeparatorOverrides,
        cellFormatRules,
        chartPanelOpen,
        chartPanelSource,
        chartPanelType,
        chartPanelBarLayout,
        chartPanelAxisMode,
        chartPanelOrientation,
        chartPanelHierarchyLevel,
        chartPanelTitle,
        chartPanelLayers,
        chartPanelRowLimit,
        chartPanelColumnLimit,
        chartPanelWidth,
        chartPanelGraphHeight,
        chartPanelFloating,
        chartPanelFloatingRect,
        chartPanelSortMode,
        chartPanelInteractionMode,
        chartPanelServerScope,
        chartPanelLocked,
        chartPanelLockedModel,
        chartPanelLockedRequest,
        activeChartDefinitionId,
        chartDefinitions: managedChartDefinitions.map(/** @param {any} [definition] @returns {any} */ definition => ({
          ...definition,
          id: definition.id,
          name: definition.name,
        })),
        chartCanvasPanes: chartCanvasPanes.map(/** @param {any} [pane] @returns {any} */ pane => ({ ...pane })),
        tableCanvasSize,
        chartServerWindow: normalizedChartServerWindow.enabled ? {
          rows: normalizedChartServerWindow.rows,
          columns: normalizedChartServerWindow.columns,
          scope: normalizedChartServerWindow.scope,
        } : null,
        scroll: parentRef.current ? {
          top: parentRef.current.scrollTop,
          left: parentRef.current.scrollLeft,
        } : null,
      },
    };
  }, [activeChartDefinitionId, activeReportId, cellFormatRules, chartCanvasPanes, chartPanelAxisMode, chartPanelBarLayout, chartPanelColumnLimit, chartPanelFloating, chartPanelFloatingRect, chartPanelGraphHeight, chartPanelHierarchyLevel, chartPanelInteractionMode, chartPanelLayers, chartPanelLocked, chartPanelLockedModel, chartPanelLockedRequest, chartPanelOpen, chartPanelOrientation, chartPanelRowLimit, chartPanelServerScope, chartPanelSortMode, chartPanelSource, chartPanelTitle, chartPanelType, chartPanelWidth, colExpanded, colFields, colSearch, colTypeFilter, colorPalette, colorScaleMode, colorScaleColumns, columnDecimalOverrides, columnFormatOverrides, columnGroupSeparatorOverrides, columnOrder, pivotLayoutOrder, pivotPinnedLayoutIds, columnPinning, columnSizing, columnVisibility, customDimensions, dataBarsColumns, dataBarMaximums, decimalPlaces, defaultValueFormat, detailConfig, detailMode, editValueDisplayMode, expanded, fieldPanelSizes, filters, frozenPivotConfig, grandTotalPinOverride, latestViewportRef, layoutMode, managedChartDefinitions, normalizedChartServerWindow, normalizedInitialValConfigs, numberGroupSeparator, parentRef, pivotColumnSorting, reportDef, rowFields, rowPinning, runtimeMeasureAxis, savedReports, serializedEditComparisonState, showColTotals, showFloatingFilters, showReportConfigColumn, showRowNumbers, showRowTotals, showSubtotals, sidebarOpen, sidebarTab, sidebarWidth, sorting, spacingMode, stickyHeaders, tableCanvasSize, tableName, themeName, themeOverrides, treeConfig, valConfigs, viewMode, zoomLevel]);

  /** @type {(...args: any[]) => any} */
  const handleSaveView = useCallback(() => {
    /** @type {any} */
    const snapshot = buildCurrentViewState();
    savePersistedState('savedView', snapshot);
    if (setPropsRef.current) {
      setPropsRef.current({ savedView: snapshot });
    }
    showNotification('View snapshot saved', 'success');
  }, [buildCurrentViewState, savePersistedState, setPropsRef, showNotification]);

  /** @type {any} */
  const saveViewTriggerRef = useRef(saveViewTrigger);
  useEffect(() => {
    if (saveViewTrigger === null || saveViewTrigger === undefined) {
      saveViewTriggerRef.current = saveViewTrigger;
      return;
    }
    if (saveViewTrigger !== saveViewTriggerRef.current) {
      saveViewTriggerRef.current = saveViewTrigger;
      handleSaveView();
    }
  }, [handleSaveView, saveViewTrigger]);

  return { buildCurrentViewState, handleSaveView };
}

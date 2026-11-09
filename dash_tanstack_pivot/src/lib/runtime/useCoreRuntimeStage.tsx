// @ts-nocheck -- composed leaf hooks are progressively typed at their owners.
// Core-stage composition. Focused sub-hooks own individual state groups.
import React, { useMemo, useState, useRef, useEffect, useCallback } from 'react';
import { themes, getStyles, isDarkTheme, gridDimensionTokens, deriveEditedCellThemeTokens, deriveStructuralThemeTokens } from '../utils/styles';
import { DEFAULT_FIELD_PANEL_SIZES, sanitizeFieldPanelSizes } from '../utils/fieldPanelLayout';
import { normalizeColumnOrderValue, areStringArraysEqual } from '../utils/columnOrder';
import { buildMeasureAxisRuntimeConfig, normalizeMeasureAxisValue } from '../utils/measureAxis';
import { buildClientFilterOptionMap, collectDataFieldIds, isAutoDataFieldId } from '../utils/filterOptions';
import { buildReportEditorSelectionFromRow } from '../utils/reportEditorSelection';
import { DEFAULT_CHART_PANEL_ROW_LIMIT, DEFAULT_CHART_PANEL_COLUMN_LIMIT, DEFAULT_CHART_GRAPH_HEIGHT, DEFAULT_FLOATING_CHART_PANEL_HEIGHT, DEFAULT_TABLE_CANVAS_SIZE, VALID_CHART_TYPES, VALID_CHART_SORT_MODES, VALID_CHART_INTERACTION_MODES, VALID_CHART_SERVER_SCOPES, GRAND_TOTAL_ROW_ID, MISSING_PERSISTED_VALUE, normalizeCustomDimensionsValue, applyCustomDimensionsToRows, decorateAvailableFieldsWithCustomDimensions, getPreferredChartOrientation, cloneSerializable, normalizeViewModeValue, normalizeDetailModeValue, normalizeTreeConfigValue, normalizeDetailConfigValue, normalizeLegacyPivotModeValue, normalizeReportDefValue, collectReportFields, normalizeSavedReportsValue, normalizeActiveReportIdValue, clampFloatingChartRect, normalizeChartDefinition, sanitizeChartDefinitions, normalizeLockedChartRequest, sanitizeChartCanvasPanes, normalizeChartServerWindowConfig, coerceTransportNumber, normalizeRuntimeDataEnvelope, clampSidebarWidth, clampDecimalPlaces, normalizeDefaultValueFormat, mergeSparseColSchema, isSparseSchemaRangeLoaded, normalizePerformanceConfigValue, buildLargeColumnAdvisory, normalizeRowPinningState, getPinnedSideForRow, sanitizeGrandTotalPinOverride, resolveGrandTotalPinState } from '../hooks/usePivotNormalization';
const EXPAND_ALL_STATE_KEY = '__ALL__';
const normalizeLayoutModeValue = value => {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'outline' || normalized === 'tabular' ? normalized : 'hierarchy';
};
const debugLog = (...args) => {
  const buildDebugEnabled = process.env.NODE_ENV !== 'production';
  let runtimeDebugEnabled = false;
  if (typeof window !== 'undefined') {
    try {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true || window.localStorage.getItem('pivot-debug') === '1';
    } catch (error) {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true;
    }
  }
  if (!buildDebugEnabled && !runtimeDebugEnabled) return;
  console.log('[pivot-grid]', ...args);
};
import { useServerSideViewportController } from '../hooks/useServerSideViewportController';
import { DEFAULT_NUMBER_GROUP_SEPARATOR, getKey, normalizeNumberGroupSeparator } from '../utils/helpers';
import { normalizeEditingConfig, resolveColumnEditSpec, resolveEditorOptionsSource } from '../utils/editing';
import { usePersistence, resolvePersistedViewState } from '../hooks/usePersistence';
import { useFilteredData } from '../hooks/useFilteredData';
import { useChartState } from '../hooks/useChartState';
import { useFormatState } from '../hooks/useFormatState';
import { useCellInteraction } from '../hooks/useCellInteraction';
import { useSidebarUI } from '../hooks/useSidebarUI';
import { useReportMode } from '../hooks/useReportMode';
import { useRuntimeExportState } from '../runtime/useRuntimeExportState';
import { useRuntimeEditCellIdentity } from '../runtime/useRuntimeEditCellIdentity';
import { useRuntimeEditComparisonState } from '../runtime/useRuntimeEditComparisonState';
import { useRuntimeEditOwnership } from '../runtime/useRuntimeEditOwnership';
import { useRuntimeEditPresentation } from '../runtime/useRuntimeEditPresentation';
import { useRuntimeRowEditSessions } from '../runtime/useRuntimeRowEditSessions';
import { useRuntimeChartPanelSizing } from '../runtime/useRuntimeChartPanelSizing';
import { useRuntimeChartCanvasResize } from '../runtime/useRuntimeChartCanvasResize';
import { useRuntimeChartCanvasVerticalResize } from '../runtime/useRuntimeChartCanvasVerticalResize';
import { useRuntimeChartDefinitionCommands } from '../runtime/useRuntimeChartDefinitionCommands';
import { createRuntimeCellPresentation } from '../runtime/createRuntimeCellPresentation';
import { useRuntimeOptimisticCellState } from '../runtime/useRuntimeOptimisticCellState';
import { useRuntimeRequestSession } from '../runtime/useRuntimeRequestSession';
import { useRuntimeRowSelection } from '../runtime/useRuntimeRowSelection';
import { usePivotTransactions } from '../hooks/usePivotTransactions';
import { usePivotFieldInteractions } from '../hooks/usePivotFieldInteractions';
import { getSparklineValueConfigIdentity, normalizeSparklineValConfigsForView } from '../utils/sparklines';
import { formatLocaleTextTemplate } from '../utils/pivotLayoutUtils';
import { ClientPivotTransport, resolveExecutionMode } from './client/index';
// Editing and transaction mutation are deliberately outside the public
// component contract. Legacy edit inputs remain accepted by Dash metadata but
// are ignored so an existing layout can upgrade without a prop-validation
// failure.
const EDITING_PUBLIC_FEATURE_SUPPORTED = false;
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartModal
})));
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartPanel
})));
const PivotChartFallback = () => null;
const PivotChartModal = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartModal {...props} />
    </React.Suspense>;
const PivotChartPanel = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartPanel {...props} />
    </React.Suspense>;
const loadingAnimationStyles = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@300;500&family=Plus+Jakarta+Sans:wght@300;500;800&display=swap');
@keyframes pivot-row-loader-enter {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes pivot-skeleton-shimmer {
    0% { background-position: 220% 0; }
    100% { background-position: -220% 0; }
}
@keyframes pivot-spinner-rotate {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
`;
const FILTER_OPTION_PAGE_SIZE = 250;
export function useCoreRuntimeStage(props) {
  // The setup domain owns the base stage model and its React hook state.
  const stage = { props };
  // One stable registry owns imperative chart coordination. Keeping it at the
  // start of the controller avoids coupling these refs to the renderable
  // chart-state hook order.
  const chartRuntimeRefs = useRef({
    chartPanelOrientationAutoRef: { current: true },
    chartLayoutRef: { current: null },
    chartCanvasLayoutRef: { current: null },
    chartCanvasResizeRef: { current: null },
    chartCanvasVerticalResizeRef: { current: null },
    chartPanelFloatingDragRef: { current: null },
    chartPanelFloatingResizeRef: { current: null },
    chartCanvasFloatingDragRef: { current: null },
    chartCanvasFloatingResizeRef: { current: null },
    chartRequestSeqRef: { current: 0 },
    activeChartRequestRef: { current: null },
    completedChartRequestSignaturesRef: { current: {} },
    applyingChartDefinitionRef: { current: false },
    lastChartDefinitionsPropRef: { current: null },
    lastChartCanvasPanesPropRef: { current: null }
  });
  ({
    id: stage.id,
    data: stage.inputData = [],
    style: stage.style = {},
    setProps: stage.setProps,
    serverSide: stage.serverSide = false,
    rowFields: stage.initialRowFields = [],
    colFields: stage.initialColFields = [],
    valConfigs: stage.initialValConfigs = [],
    filters: stage.initialFilters = {},
    sorting: stage.initialSorting = [],
    expanded: stage.initialExpanded = {},
    immersiveMode: stage.initialImmersiveMode = false,
    showRowTotals: stage.initialShowRowTotals = true,
    showColTotals: stage.initialShowColTotals = true,
    showSubtotals: stage.initialShowSubtotals = true,
    grandTotalPosition: stage.grandTotalPosition = 'top',
    conditionalFormatting: stage.conditionalFormatting = [],
    headerFormatting: stage.headerFormatting = {},
    validationRules: stage.validationRules = {},
    columnPinning: stage.initialColumnPinning = {
      left: ['hierarchy'],
      right: []
    },
    rowPinning: stage.initialRowPinning = {
      top: [],
      bottom: []
    },
    persistence: stage.persistence,
    persistence_type: stage.persistence_type = 'local',
    pinningOptions: stage.pinningOptions = {},
    pinningPresets: stage.pinningPresets = [],
    sortOptions: stage.sortOptions = {},
    columnVisibility: stage.initialColumnVisibility = {},
    columnOrder: stage.externalColumnOrder = null,
    decimalPlaces: stage.externalDecimalPlaces = 0,
    fieldPanelSizes: stage.externalFieldPanelSizes = null,
    defaultValueFormat: stage.externalDefaultValueFormat = '',
    numberGroupSeparator: stage.externalNumberGroupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR,
    reportDef: stage.externalReportDef,
    savedReports: stage.externalSavedReports,
    activeReportId: stage.externalActiveReportId,
    customDimensions: stage.externalCustomDimensions = [],
    measureAxis: stage.externalMeasureAxis = null,
    reset: stage.reset,
    sortLock: stage.sortLock = false,
    defaultTheme: stage.externalDefaultTheme,
    theme: stage.externalTheme,
    availableFieldList: stage.availableFieldList,
    sparklineFields: stage.sparklineFields = null,
    table: stage.tableName,
    runtimeResponse: stage.runtimeResponse = null,
    viewMode: stage.externalViewMode,
    detailMode: stage.externalDetailMode,
    treeConfig: stage.externalTreeConfig,
    detailConfig: stage.externalDetailConfig,
    chartServerWindow: stage.chartServerWindow = null,
    chartDefinitions: stage.chartDefinitions = null,
    chartDefaults: stage.chartDefaults = null,
    chartCanvasPanes: stage.externalChartCanvasPanes = null,
    tableCanvasSize: stage.externalTableCanvasSize = null,
    drillEndpoint: stage.drillEndpoint = '/api/drill-through',
    performanceConfig: stage.externalPerformanceConfig = null,
    viewState: stage.viewState = null,
    saveViewTrigger: stage.saveViewTrigger = null,
    uiConfig: stage.externalUiConfig = null,
    paginationConfig: stage.externalPagination = null,
    localeText: stage.externalLocaleText = null,
    mergedHeaderSeparator: stage.mergedHeaderSeparator = ' ',
    rowSelection: stage.externalRowSelection = false
  } = stage.props);
  // Deliberately do not hydrate legacy editing props. The component accepts
  // them for backward-compatible Dash layouts, but they cannot enable or
  // restore an unsupported editing workflow.
  stage.defaultTheme = stage.externalTheme || stage.externalDefaultTheme || 'flash';
  stage.externalEditingConfig = null;
  stage.externalEditState = null;
  // Setup effects publish before the transport domain is composed. Seed this
  // stable ref with Dash's direct callback; useTransportRuntime upgrades its
  // current callback to the versioned transport dispatcher later in the same
  // render without changing ref identity.
  stage.setPropsRef = useRef(stage.setProps);
  stage.setPropsRef.current = stage.setProps;
  stage.normalizedInitialChartServerWindow = normalizeChartServerWindowConfig(stage.chartServerWindow);
  stage.initialChartDefinition = useMemo(() => normalizeChartDefinition(stage.chartDefaults, {
    id: 'live-chart-panel',
    name: 'Live Chart',
    source: 'pivot',
    chartType: 'bar',
    barLayout: 'grouped',
    axisMode: 'vertical',
    orientation: getPreferredChartOrientation(stage.initialColFields),
    hierarchyLevel: 'all',
    rowLimit: DEFAULT_CHART_PANEL_ROW_LIMIT,
    columnLimit: DEFAULT_CHART_PANEL_COLUMN_LIMIT,
    width: 430,
    sortMode: 'natural',
    interactionMode: 'focus',
    serverScope: stage.normalizedInitialChartServerWindow.scope
  }), [stage.chartDefaults, stage.initialColFields, stage.normalizedInitialChartServerWindow.scope]);
  stage.initialChartDefinitions = useMemo(() => sanitizeChartDefinitions(stage.chartDefinitions, stage.initialChartDefinition), [stage.chartDefinitions, stage.initialChartDefinition]);
  stage.normalizedInitialFieldPanelSizes = useMemo(() => sanitizeFieldPanelSizes(stage.externalFieldPanelSizes && typeof stage.externalFieldPanelSizes === 'object' ? stage.externalFieldPanelSizes : DEFAULT_FIELD_PANEL_SIZES), [stage.externalFieldPanelSizes]);
  stage.normalizedInitialDecimalPlaces = useMemo(() => clampDecimalPlaces(stage.externalDecimalPlaces, 0), [stage.externalDecimalPlaces]);
  stage.normalizedInitialDefaultValueFormat = useMemo(() => normalizeDefaultValueFormat(stage.externalDefaultValueFormat), [stage.externalDefaultValueFormat]);
  stage.normalizedEditingConfig = useMemo(() => normalizeEditingConfig(stage.externalEditingConfig), [stage.externalEditingConfig]);
  stage.normalizedInitialNumberGroupSeparator = useMemo(() => normalizeNumberGroupSeparator(stage.externalNumberGroupSeparator), [stage.externalNumberGroupSeparator]);
  stage.uiConfig = useMemo(() => {
    const src = stage.externalUiConfig && typeof stage.externalUiConfig === 'object' ? stage.externalUiConfig : {};
    return {
      showToolbar: src.showToolbar !== false,
      showSidebar: src.showSidebar !== false,
      showFilters: src.showFilters !== false,
      showCharts: src.showCharts !== false,
      showEditing: EDITING_PUBLIC_FEATURE_SUPPORTED,
      showEditPanel: EDITING_PUBLIC_FEATURE_SUPPORTED,
      lockImmersiveMode: src.lockImmersiveMode === true,
      wrapHeaders: src.wrapHeaders === true
    };
  }, [stage.externalUiConfig]);
  stage.localeText = useMemo(() => stage.externalLocaleText && typeof stage.externalLocaleText === 'object' ? stage.externalLocaleText : {}, [stage.externalLocaleText]);
  stage.getLocaleText = useCallback((key, fallback, values = {}) => {
    const template = Object.prototype.hasOwnProperty.call(stage.localeText, key) ? stage.localeText[key] : fallback;
    return formatLocaleTextTemplate(template, values);
  }, [stage.localeText]);
  stage.editingEnabled = EDITING_PUBLIC_FEATURE_SUPPORTED && stage.uiConfig.showEditing;
  stage.editPanelEnabled = stage.editingEnabled && stage.uiConfig.showEditPanel;
  stage.rowCheckboxMode = stage.externalRowSelection === 'single' ? 'single' : stage.externalRowSelection === 'multiple' || stage.externalRowSelection === true ? 'multiple' : null;
  [stage.selectedRowKeys, stage.setSelectedRowKeys] = useState(() => new Set());
  stage.toggleRowCheckbox = useCallback(key => {
    stage.setSelectedRowKeys(prev => {
      const next = new Set(stage.rowCheckboxMode === 'multiple' ? prev : []);
      if (prev.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, [stage.rowCheckboxMode]);
  useEffect(() => {
    if (!stage.rowCheckboxMode) return;
    if (typeof stage.props.setProps === 'function') {
      stage.props.setProps({
        selectedRows: [...stage.selectedRowKeys]
      });
    }
  }, [stage.selectedRowKeys, stage.rowCheckboxMode, stage.props.setProps]);
  // Clear selection when row structure changes (new data, sort, filter)
  stage.rowStructureKey = [stage.rowFields, stage.colFields, stage.sorting, stage.filters].map(x => JSON.stringify(x)).join('|');
  useEffect(() => {
    if (stage.rowCheckboxMode) stage.setSelectedRowKeys(new Set());
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stage.rowStructureKey]);
  stage.paginationConfig = useMemo(() => {
    if (stage.serverSide) return {
      enabled: false,
      pageSize: 50
    };
    const src = stage.externalPagination && typeof stage.externalPagination === 'object' ? stage.externalPagination : {};
    const enabled = src.enabled === true;
    const pageSize = Number.isFinite(Number(src.pageSize)) && Number(src.pageSize) > 0 ? Math.floor(Number(src.pageSize)) : 50;
    return {
      enabled,
      pageSize
    };
  }, [stage.externalPagination, stage.serverSide]);
  stage.normalizedPerformanceConfig = useMemo(() => normalizePerformanceConfigValue(stage.externalPerformanceConfig), [stage.externalPerformanceConfig]);
  stage.executionMode = useMemo(() => resolveExecutionMode(stage.props, {
    performanceConfig: stage.normalizedPerformanceConfig
  }), [stage.props, stage.normalizedPerformanceConfig]);
  stage.clientRuntimeActive = Boolean(
    stage.executionMode === 'client'
    && !stage.normalizedPerformanceConfig.legacyClientTable
    && (
      stage.normalizedPerformanceConfig.executionMode === 'client'
      || stage.normalizedPerformanceConfig.executionMode === 'auto'
      || stage.normalizedPerformanceConfig.clientEngine
      || stage.normalizedPerformanceConfig.dataUrl
      || stage.props.serverSide === undefined
    )
  );
  if (stage.clientRuntimeActive) {
    stage.serverSide = true;
  }
  stage.clientTransportRef = useRef(null);
  if (stage.clientRuntimeActive && !stage.clientTransportRef.current) {
    stage.clientTransportRef.current = new ClientPivotTransport({
      ...stage.normalizedPerformanceConfig,
      data: Array.isArray(stage.inputData) ? stage.inputData : [],
      tableName: stage.tableName || 'client_data'
    });
  }
  [stage.localRuntimeResponse, stage.setLocalRuntimeResponse] = useState(null);
  if (stage.clientRuntimeActive && stage.localRuntimeResponse) {
    stage.runtimeResponse = stage.localRuntimeResponse;
  }
  stage.hasExternalViewMode = normalizeViewModeValue(stage.externalViewMode) !== null;
  stage.hasExternalDetailMode = normalizeDetailModeValue(stage.externalDetailMode) !== null;
  stage.hasExternalTreeConfig = stage.externalTreeConfig !== undefined;
  stage.hasExternalDetailConfig = stage.externalDetailConfig !== undefined;
  stage.normalizedInitialViewMode = useMemo(() => normalizeViewModeValue(stage.externalViewMode) || 'pivot', [stage.externalViewMode]);
  stage.normalizedInitialDetailMode = useMemo(() => normalizeDetailModeValue(stage.externalDetailMode) || 'none', [stage.externalDetailMode]);
  stage.normalizedInitialTreeConfig = useMemo(() => normalizeTreeConfigValue(stage.externalTreeConfig), [stage.externalTreeConfig]);
  stage.normalizedInitialDetailConfig = useMemo(() => normalizeDetailConfigValue(stage.externalDetailConfig), [stage.externalDetailConfig]);
  stage.hasExternalReportDef = stage.externalReportDef !== undefined;
  stage.hasExternalSavedReports = stage.externalSavedReports !== undefined;
  stage.hasExternalActiveReportId = stage.externalActiveReportId !== undefined;
  stage.hasExternalCustomDimensions = stage.props.customDimensions !== undefined;
  stage.hasExternalMeasureAxis = stage.props.measureAxis !== undefined;
  stage.normalizedInitialReportDef = useMemo(() => normalizeReportDefValue(stage.externalReportDef), [stage.externalReportDef]);
  stage.normalizedInitialSavedReports = useMemo(() => normalizeSavedReportsValue(stage.externalSavedReports), [stage.externalSavedReports]);
  stage.normalizedInitialActiveReportId = useMemo(() => normalizeActiveReportIdValue(stage.externalActiveReportId), [stage.externalActiveReportId]);
  stage.normalizedInitialCustomDimensions = useMemo(() => normalizeCustomDimensionsValue(stage.externalCustomDimensions), [stage.externalCustomDimensions]);
  stage.normalizedInitialMeasureAxis = useMemo(() => normalizeMeasureAxisValue(stage.externalMeasureAxis), [stage.externalMeasureAxis]);
  stage.normalizedInitialValConfigs = useMemo(() => normalizeSparklineValConfigsForView(stage.initialValConfigs), [stage.initialValConfigs]);
  stage.fixedSparklineValueKeys = useMemo(() => stage.normalizedInitialValConfigs.filter(config => config && config.sparkline).map(getSparklineValueConfigIdentity).filter(Boolean), [stage.normalizedInitialValConfigs]);

  // Register sortOptions with Dash's callback State store on mount.
  // Without this, State(id, "sortOptions") returns None in Python callbacks
  // because Dash only stores prop values that were explicitly pushed via setProps.
  useEffect(() => {
    if (stage.setProps && stage.sortOptions && Object.keys(stage.sortOptions).length > 0) {
      stage.setProps({
        sortOptions: stage.sortOptions
      });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // --- Persistence Helper ---
  ({
    load: stage.loadPersistedState,
    save: stage.savePersistedState
  } = usePersistence(stage.id, stage.persistence, stage.persistence_type));
  stage.viewState = useMemo(() => resolvePersistedViewState(
    stage.viewState, stage.loadPersistedState('savedView', null), stage.tableName
  ), [stage.viewState, stage.loadPersistedState, stage.tableName]);
  [stage.formulaErrors, stage.setFormulaErrors] = useState({});
  [stage.transportDataState, stage.setTransportDataState] = useState(() => normalizeRuntimeDataEnvelope({
    data: Array.isArray(stage.inputData) ? stage.inputData : [],
    rowCount: stage.serverSide ? null : Array.isArray(stage.inputData) ? stage.inputData.length : 0,
    columns: [],
    dataOffset: 0,
    dataVersion: 0,
    stateEpoch: 0
  }));
  [stage.transportFilterOptionsState, stage.setTransportFilterOptionsState] = useState(() => ({}));
  [stage.transportFilterOptionMetaState, stage.setTransportFilterOptionMetaState] = useState(() => ({}));
  [stage.editorOptionsLoadingState, stage.setEditorOptionsLoadingState] = useState(() => ({}));
  [stage.transportChartDataState, stage.setTransportChartDataState] = useState(() => null);
  stage.data = stage.transportDataState.data;
  stage.rowCount = stage.transportDataState.rowCount;
  stage.responseColumns = stage.transportDataState.columns;
  stage.responseColSchema = stage.transportDataState.colSchema;
  stage.dataOffset = stage.transportDataState.dataOffset;
  stage.dataVersion = stage.transportDataState.dataVersion;
  stage.responseStateEpoch = stage.transportDataState.stateEpoch;
  stage.filterOptions = stage.transportFilterOptionsState;
  stage.filterOptionMeta = stage.transportFilterOptionMetaState;
  stage.chartData = stage.transportChartDataState;
  [stage.notification, stage.setNotification] = useState(null);
  useEffect(() => {
    if (stage.serverSide) return;
    stage.setTransportDataState(previousState => normalizeRuntimeDataEnvelope({
      data: Array.isArray(stage.inputData) ? stage.inputData : [],
      rowCount: Array.isArray(stage.inputData) ? stage.inputData.length : 0,
      columns: previousState.columns,
      dataOffset: 0,
      dataVersion: previousState.dataVersion,
      stateEpoch: previousState.stateEpoch
    }, previousState));
  }, [stage.inputData, stage.serverSide]);
  useEffect(() => {
    if (stage.notification) {
      const timer = setTimeout(() => stage.setNotification(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [stage.notification]);
  stage.showNotification = React.useCallback((msg, type = 'info') => {
    stage.setNotification({
      message: msg,
      type
    });
  }, []);
  stage.emitEditLifecycleEvent = useCallback(event => {
    if (!EDITING_PUBLIC_FEATURE_SUPPORTED) return;
    if (typeof stage.setProps !== 'function' || !event || typeof event !== 'object') return;
    stage.setProps({
      editLifecycleEvent: {
        ...event,
        timestamp: Date.now()
      }
    });
  }, [stage.setProps]);
  stage.availableFields = useMemo(() => {
    if (stage.availableFieldList && stage.availableFieldList.length > 0) return stage.availableFieldList;
    if (stage.serverSide && stage.responseColumns) {
      return stage.responseColumns.map(c => c && typeof c === 'object' ? c.id : c).filter(isAutoDataFieldId);
    }
    return collectDataFieldIds(stage.data);
  }, [stage.data, stage.responseColumns, stage.serverSide, stage.availableFieldList]);
  [stage.themeName, stage.setThemeName] = useState(() => themes[stage.defaultTheme] ? stage.defaultTheme : 'flash');
  [stage.themeOverrides, stage.setThemeOverrides] = useState({});
  stage.theme = useMemo(() => {
    const nextTheme = {
      ...themes[stage.themeName],
      ...stage.themeOverrides
    };
    const structuralTheme = {
      ...nextTheme,
      ...deriveStructuralThemeTokens(nextTheme, stage.themeOverrides)
    };
    return {
      ...structuralTheme,
      ...deriveEditedCellThemeTokens(structuralTheme, stage.themeOverrides)
    };
  }, [stage.themeName, stage.themeOverrides]);
  stage.styles = useMemo(() => getStyles(stage.theme), [stage.theme]);
  stage.loadingCssVars = useMemo(() => {
    if (isDarkTheme(stage.theme)) {
      return {
        '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(57,88,132,0.62) 0%, rgba(88,126,178,0.9) 48%, rgba(57,88,132,0.62) 100%)',
        '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(50,77,116,0.58) 0%, rgba(82,118,170,0.86) 45%, rgba(50,77,116,0.58) 100%)',
        '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(35,53,82,0.88) 0%, rgba(49,74,112,0.95) 50%, rgba(35,53,82,0.88) 100%)',
        '--pivot-loading-border': 'rgba(130, 165, 215, 0.45)',
        '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(120,170,240,0) 0%, rgba(140,190,255,0.92) 45%, rgba(120,170,240,0) 100%)',
        '--pivot-loading-shimmer-duration': '2.8s'
      };
    }
    return {
      '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(233,243,255,0.92) 0%, rgba(193,220,255,0.98) 48%, rgba(233,243,255,0.92) 100%)',
      '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%)',
      '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%)',
      '--pivot-loading-border': 'rgba(153, 187, 238, 0.5)',
      '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(75,139,245,0) 0%, rgba(75,139,245,0.9) 45%, rgba(75,139,245,0) 100%)',
      '--pivot-loading-shimmer-duration': '2.8s'
    };
  }, [stage.theme]);
  [stage.rowFields, stage.setRowFields] = useState(stage.initialRowFields);
  [stage.colFields, stage.setColFields] = useState(stage.initialColFields);
  [stage.valConfigs, stage.setValConfigs] = useState(stage.normalizedInitialValConfigs);
  stage.valConfigsRef = useRef(stage.normalizedInitialValConfigs);
  stage.valConfigsRef.current = stage.valConfigs;
  [stage.customDimensions, stage.setCustomDimensions] = useState(() => stage.hasExternalCustomDimensions ? stage.normalizedInitialCustomDimensions : normalizeCustomDimensionsValue(stage.loadPersistedState('customDimensions', stage.normalizedInitialCustomDimensions)));
  [stage.measureAxis, stage.setMeasureAxis] = useState(() => stage.hasExternalMeasureAxis ? stage.normalizedInitialMeasureAxis : normalizeMeasureAxisValue(stage.loadPersistedState('measureAxis', stage.normalizedInitialMeasureAxis), stage.normalizedInitialMeasureAxis));
  stage.runtimeMeasureAxis = useMemo(() => buildMeasureAxisRuntimeConfig(stage.measureAxis, stage.valConfigs), [stage.measureAxis, stage.valConfigs]);
  stage.availableFieldsWithCustomDimensions = useMemo(() => decorateAvailableFieldsWithCustomDimensions(stage.availableFields, stage.customDimensions), [stage.availableFields, stage.customDimensions]);
  useEffect(() => {
    const normalized = normalizeMeasureAxisValue(stage.measureAxis);
    const labelField = normalized.labelField;
    if (!labelField) return;
    // Only remove the labelField from the wrong axis — never auto-append it to the end
    // of an axis. Position must be set explicitly: either via the rowFields/colFields
    // prop or via the drag interaction (placeMeasureAxisLabel handles insertion at the
    // correct index). Silently appending to the end corrupts hierarchy order when the
    // measure dimension is intended to sit between other row dimensions.
    if (normalized.placement === 'rows') {
      stage.setColFields(previous => (previous || []).filter(field => field !== labelField));
    } else if (normalized.placement === 'columns') {
      stage.setRowFields(previous => (previous || []).filter(field => field !== labelField));
    } else {
      stage.setRowFields(previous => (previous || []).filter(field => field !== labelField));
      stage.setColFields(previous => (previous || []).filter(field => field !== labelField));
    }
  }, [stage.measureAxis]);
  useEffect(() => {
    if (stage.hasExternalMeasureAxis) return;
    stage.setMeasureAxis(previous => {
      const normalizedPrevious = normalizeMeasureAxisValue(previous);
      if (normalizedPrevious.placement === 'none') return previous;
      const nextMeasureAxis = buildMeasureAxisRuntimeConfig(normalizedPrevious, stage.valConfigs);
      return JSON.stringify(normalizedPrevious) === JSON.stringify(nextMeasureAxis) ? previous : nextMeasureAxis;
    });
  }, [stage.hasExternalMeasureAxis, stage.valConfigs]);
  [stage.filters, stage.setFilters] = useState(stage.initialFilters);
  [stage.sorting, stage.setSorting] = useState(stage.initialSorting);
  [stage.expanded, stage.setExpanded] = useState(stage.initialExpanded);
  [stage.columnPinning, stage.setColumnPinning] = useState(() => stage.loadPersistedState('columnPinning', stage.initialColumnPinning));
  [stage.rowPinning, stage.setRowPinning] = useState(() => stage.loadPersistedState('rowPinning', stage.initialRowPinning));
  [stage.grandTotalPinOverride, stage.setGrandTotalPinOverride] = useState(() => {
    const persistedOverride = stage.loadPersistedState('grandTotalPinOverride', MISSING_PERSISTED_VALUE);
    if (persistedOverride !== MISSING_PERSISTED_VALUE) {
      return sanitizeGrandTotalPinOverride(persistedOverride);
    }
    const persistedRowPinning = stage.loadPersistedState('rowPinning', stage.initialRowPinning);
    return getPinnedSideForRow(persistedRowPinning, GRAND_TOTAL_ROW_ID);
  });
  [stage.layoutMode, stage.setLayoutMode] = useState(() => normalizeLayoutModeValue(stage.loadPersistedState('layoutMode', 'hierarchy')));
  ({
    viewMode: stage.viewMode,
    setViewMode: stage.setViewMode,
    reportDef: stage.reportDef,
    setReportDef: stage.setReportDef,
    savedReports: stage.savedReports,
    setSavedReports: stage.setSavedReports,
    activeReportId: stage.activeReportId,
    setActiveReportId: stage.setActiveReportId,
    frozenPivotConfig: stage.frozenPivotConfig,
    setFrozenPivotConfig: stage.setFrozenPivotConfig
  } = useReportMode({
    hasExternalViewMode: stage.hasExternalViewMode,
    normalizedInitialViewMode: stage.normalizedInitialViewMode,
    normalizeViewModeValue,
    normalizeLegacyPivotModeValue,
    hasExternalReportDef: stage.hasExternalReportDef,
    normalizedInitialReportDef: stage.normalizedInitialReportDef,
    normalizeReportDefValue,
    hasExternalSavedReports: stage.hasExternalSavedReports,
    normalizedInitialSavedReports: stage.normalizedInitialSavedReports,
    normalizeSavedReportsValue,
    hasExternalActiveReportId: stage.hasExternalActiveReportId,
    normalizedInitialActiveReportId: stage.normalizedInitialActiveReportId,
    normalizeActiveReportIdValue,
    loadPersistedState: stage.loadPersistedState
  }));
  [stage.reportEditorSelection, stage.setReportEditorSelection] = useState(null);
  [stage.detailMode, stage.setDetailMode] = useState(() => stage.hasExternalDetailMode ? stage.normalizedInitialDetailMode : normalizeDetailModeValue(stage.loadPersistedState('detailMode', 'none')) || 'none');
  [stage.treeConfig, stage.setTreeConfig] = useState(() => stage.hasExternalTreeConfig ? stage.normalizedInitialTreeConfig : normalizeTreeConfigValue(stage.loadPersistedState('treeConfig', {})));
  [stage.detailConfig, stage.setDetailConfig] = useState(() => stage.hasExternalDetailConfig ? stage.normalizedInitialDetailConfig : normalizeDetailConfigValue(stage.loadPersistedState('detailConfig', {})));
  [stage.columnVisibility, stage.setColumnVisibility] = useState(() => stage.loadPersistedState('columnVisibility', stage.initialColumnVisibility));
  [stage.columnOrder, stage.setColumnOrder] = useState(() => Array.isArray(stage.externalColumnOrder) ? normalizeColumnOrderValue(stage.externalColumnOrder) : normalizeColumnOrderValue(stage.loadPersistedState('columnOrder', [])));
  [stage.pivotLayoutOrder, stage.setPivotLayoutOrder] = useState(() => normalizeColumnOrderValue(stage.loadPersistedState('pivotLayoutOrder', [])));
  [stage.pivotPinnedLayoutIds, stage.setPivotPinnedLayoutIds] = useState(() => stage.loadPersistedState('pivotPinnedLayoutIds', {}));
  stage.pivotLayoutOrderRef = useRef(stage.pivotLayoutOrder);
  stage.pivotLayoutOrderRef.current = stage.pivotLayoutOrder;
  stage.layoutRevisionRef = useRef(null);
  stage.layoutAcknowledgedManifestRef = useRef('');
  stage.layoutScopeCacheKeyRef = useRef(null);
  stage.latestLayoutSentManifestRef = useRef(null);
  [stage.columnSizing, stage.setColumnSizing] = useState(() => stage.loadPersistedState('columnSizing', {}));
  stage.externalColumnOrderKey = useMemo(() => JSON.stringify(normalizeColumnOrderValue(stage.externalColumnOrder)), [stage.externalColumnOrder]);
  useEffect(() => {
    if (!Array.isArray(stage.externalColumnOrder)) return;
    stage.setColumnOrder(normalizeColumnOrderValue(JSON.parse(stage.externalColumnOrderKey)));
  }, [stage.externalColumnOrderKey]);
  [stage.autoSizeIncludesHeaderNext, stage.setAutoSizeIncludesHeaderNext] = useState(false);
  [stage.pivotColumnSorting, stage.setPivotColumnSorting] = useState({});
  [stage.announcement, stage.setAnnouncement] = useState("");
  // The chart domain owns these named values before later setup hooks consume
  // them. Keep the transfer local to setup rather than a shared publisher.
  const chartState = useChartState({
    initialChartDefinition: stage.initialChartDefinition,
    initialChartDefinitions: stage.initialChartDefinitions,
    externalChartCanvasPanes: stage.externalChartCanvasPanes,
    externalTableCanvasSize: stage.externalTableCanvasSize,
    loadPersistedState: stage.loadPersistedState,
    clampFloatingChartRect,
    normalizeLockedChartRequest,
    sanitizeChartCanvasPanes,
    DEFAULT_TABLE_CANVAS_SIZE
  });
  for (const [key, value] of Object.entries(chartState)) stage[key] = value;
  // Chart state owns renderable values; setup owns imperative coordination
  // refs so no state hook needs to leak mutable stage infrastructure.
  for (const [key, value] of Object.entries(chartRuntimeRefs.current)) stage[key] = value;
  [stage.chartCanvasPaneWidthHints, stage.setChartCanvasPaneWidthHints] = useState({});
  [stage.previewChartPaneId, stage.setPreviewChartPaneId] = useState(null);
  [stage.sparklineDataModal, stage.setSparklineDataModal] = useState(null);
  stage.openSparklineDataModalRef = useRef(null);
  stage.openSparklineDataModalRef.current = stage.setSparklineDataModal;
  stage.lastTableCanvasSizePropRef = useRef(null);
  stage.lastValConfigsPropRef = useRef(null);
  stage.lastDecimalPlacesPropRef = useRef(null);
  stage.lastFieldPanelSizesPropRef = useRef(null);
  stage.lastDefaultValueFormatPropRef = useRef(null);
  stage.lastNumberGroupSeparatorPropRef = useRef(null);
  stage.lastReportDefPropRef = useRef(null);
  stage.lastSavedReportsPropRef = useRef(null);
  stage.lastActiveReportIdPropRef = useRef(null);
  stage.lastViewModePropRef = useRef(null);
  stage.lastDetailModePropRef = useRef(null);
  stage.lastTreeConfigPropRef = useRef(null);
  stage.lastDetailConfigPropRef = useRef(null);
  stage.reportDefRef = useRef(stage.reportDef);
  stage.reportDefRef.current = stage.reportDef;
  stage.didPublishDecimalPlacesRef = useRef(false);
  stage.didPublishFieldPanelSizesRef = useRef(false);
  stage.didPublishDefaultValueFormatRef = useRef(false);
  stage.didPublishNumberGroupSeparatorRef = useRef(false);
  stage.didPublishReportStateRef = useRef(false);
  stage.tableRef = useRef(null);
  stage.displayRowsRef = useRef([]);
  stage.displayRowIndexRef = useRef(new Map());
  stage.pinnedDisplayMetaRef = useRef({
    topCount: 0,
    centerCount: 0
  });
  stage.activeRowVirtualizerRef = useRef(null);
  stage.pinnedRowCacheRef = useRef(new Map());
  stage.columnVirtualizerRef = useRef(null);
  stage.pinnedColumnMetaRef = useRef({
    leftCount: 0,
    centerCount: 0,
    rightCount: 0
  });
  stage.previousRowFieldsRef = useRef(stage.initialRowFields);
  stage.pendingServerFilterOptionsRef = useRef(null);
  stage.handledRuntimeResponseKeysRef = useRef(new Set());
  stage.normalizedChartServerWindow = useMemo(() => normalizeChartServerWindowConfig(stage.chartServerWindow), [stage.chartServerWindow]);
  stage.effectiveSortOptions = useMemo(() => {
    const baseOptions = stage.sortOptions && typeof stage.sortOptions === 'object' ? stage.sortOptions : {};
    const baseColumnOptions = baseOptions.columnOptions && typeof baseOptions.columnOptions === 'object' ? baseOptions.columnOptions : {};
    const mergedColumnOptions = {
      ...baseColumnOptions
    };
    Object.entries(stage.pivotColumnSorting || {}).forEach(([field, sortState]) => {
      if (!field || !sortState || typeof sortState !== 'object') return;
      const direction = sortState.pivotDirection;
      const mode = sortState.pivotSortMode;
      const existing = mergedColumnOptions[field] && typeof mergedColumnOptions[field] === 'object' ? mergedColumnOptions[field] : {};
      if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) {
        mergedColumnOptions[field] = {
          ...existing,
          pivotSortMode: mode,
          pivotDirection: direction
        };
      } else if (Object.prototype.hasOwnProperty.call(existing, 'pivotDirection') || Object.prototype.hasOwnProperty.call(existing, 'pivotSortMode')) {
        const {
          pivotDirection,
          pivotSortMode,
          ...rest
        } = existing;
        mergedColumnOptions[field] = rest;
      }
    });
    return {
      ...baseOptions,
      columnOptions: mergedColumnOptions
    };
  }, [stage.sortOptions, stage.pivotColumnSorting]);
  stage.getDisplayRows = useCallback(() => {
    if (stage.displayRowsRef.current.length > 0) return stage.displayRowsRef.current;
    if (stage.tableRef.current && stage.tableRef.current.getRowModel) {
      return stage.tableRef.current.getRowModel().rows || [];
    }
    return [];
  }, []);
  stage.resolveDisplayRowIndex = useCallback((rowId, fallbackIndex = 0) => {
    const mappedIndex = stage.displayRowIndexRef.current.get(rowId);
    return mappedIndex !== undefined ? mappedIndex : fallbackIndex;
  }, []);
  stage.pivotMode = stage.viewMode === 'report' ? 'report' : 'pivot';

  // Reset Effect
  useEffect(() => {
    if (stage.reset) {
      stage.savePersistedState('savedView', null);
      const resetReportDef = stage.hasExternalReportDef ? stage.normalizedInitialReportDef : {
        levels: []
      };
      const resetSavedReports = stage.hasExternalSavedReports ? stage.normalizedInitialSavedReports : [];
      const resetActiveReportId = stage.hasExternalActiveReportId ? stage.normalizedInitialActiveReportId : null;
      const resetCustomDimensions = stage.hasExternalCustomDimensions ? stage.normalizedInitialCustomDimensions : [];
      const resetMeasureAxis = stage.hasExternalMeasureAxis ? stage.normalizedInitialMeasureAxis : normalizeMeasureAxisValue(null);
      stage.setRowFields(stage.initialRowFields);
      stage.setColFields(stage.initialColFields);
      // Preserve user-added formula columns through resets (they survive dataframe switches)
      const formulaColsToPreserve = stage.valConfigsRef.current.filter(c => c && c.agg === 'formula');
      const resetValConfigs = normalizeSparklineValConfigsForView(formulaColsToPreserve.length > 0 ? [...stage.normalizedInitialValConfigs.filter(c => c && c.agg !== 'formula'), ...formulaColsToPreserve] : stage.normalizedInitialValConfigs);
      stage.setValConfigs(resetValConfigs);
      stage.setCustomDimensions(resetCustomDimensions);
      stage.setMeasureAxis(resetMeasureAxis);
      stage.setFilters({});
      stage.setSorting([]);
      stage.setExpanded({});
      stage.setLayoutMode('hierarchy');
      stage.setShowSubtotals(stage.initialShowSubtotals);
      stage.setColumnPinning(stage.initialColumnPinning);
      stage.setRowPinning(stage.initialRowPinning);
      stage.setGrandTotalPinOverride(getPinnedSideForRow(stage.initialRowPinning, GRAND_TOTAL_ROW_ID));
      stage.setPivotColumnSorting({});
      stage.setColumnVisibility({});
      stage.setColumnOrder([]);
      stage.setPivotLayoutOrder([]);
      stage.setPivotPinnedLayoutIds({});
      stage.layoutRevisionRef.current = null;
      stage.setColumnSizing({});
      stage.setChartPanelSource(stage.initialChartDefinition.source);
      stage.setChartPanelType(stage.initialChartDefinition.chartType);
      stage.setChartPanelBarLayout(stage.initialChartDefinition.barLayout);
      stage.setChartPanelAxisMode(stage.initialChartDefinition.axisMode);
      stage.chartPanelOrientationAutoRef.current = true;
      stage.setChartPanelOrientation(stage.initialChartDefinition.orientation);
      stage.setChartPanelHierarchyLevel(stage.initialChartDefinition.hierarchyLevel);
      stage.setChartPanelTitle(stage.initialChartDefinition.chartTitle);
      stage.setChartPanelRowLimit(stage.initialChartDefinition.rowLimit);
      stage.setChartPanelColumnLimit(stage.initialChartDefinition.columnLimit);
      stage.setChartPanelWidth(stage.initialChartDefinition.width);
      stage.setChartPanelGraphHeight(stage.initialChartDefinition.chartHeight);
      stage.setChartPanelFloating(false);
      stage.setChartPanelFloatingRect(clampFloatingChartRect({
        width: stage.initialChartDefinition.width,
        height: DEFAULT_FLOATING_CHART_PANEL_HEIGHT,
        left: 24,
        top: 24
      }, null));
      stage.setChartPanelSortMode(stage.initialChartDefinition.sortMode);
      stage.setChartPanelInteractionMode(stage.initialChartDefinition.interactionMode);
      stage.setChartPanelServerScope(stage.initialChartDefinition.serverScope);
      stage.setChartPanelLocked(false);
      stage.setChartPanelLockedModel(null);
      stage.setChartPanelLockedRequest(null);
      stage.setManagedChartDefinitions(stage.initialChartDefinitions);
      stage.setActiveChartDefinitionId(stage.initialChartDefinitions[0] ? stage.initialChartDefinitions[0].id : 'live-chart-panel');
      stage.setChartCanvasPanes([]);
      stage.setTableCanvasSize(DEFAULT_TABLE_CANVAS_SIZE);
      stage.setChartPaneDataById({});
      stage.setSidebarWidth(288);
      stage.setFieldPanelSizes(stage.normalizedInitialFieldPanelSizes);
      stage.setDecimalPlaces(stage.normalizedInitialDecimalPlaces);
      stage.setDefaultValueFormat(stage.normalizedInitialDefaultValueFormat);
      stage.setNumberGroupSeparator(stage.normalizedInitialNumberGroupSeparator);
      stage.setViewMode(stage.normalizedInitialViewMode);
      stage.setDetailMode(stage.normalizedInitialDetailMode);
      stage.setTreeConfig(stage.normalizedInitialTreeConfig);
      stage.setDetailConfig(stage.normalizedInitialDetailConfig);
      stage.setReportDef(resetReportDef);
      stage.setSavedReports(resetSavedReports);
      stage.setActiveReportId(resetActiveReportId);
      stage.setShowReportConfigColumn(true);
      stage.setFrozenPivotConfig(null);
      stage.setColumnFormatOverrides({});
      stage.setColumnGroupSeparatorOverrides({});
      if (stage.setPropsRef.current) {
        stage.setPropsRef.current({
          rowFields: stage.initialRowFields,
          colFields: stage.initialColFields,
          valConfigs: resetValConfigs,
          decimalPlaces: stage.normalizedInitialDecimalPlaces,
          fieldPanelSizes: stage.normalizedInitialFieldPanelSizes,
          defaultValueFormat: stage.normalizedInitialDefaultValueFormat,
          numberGroupSeparator: stage.normalizedInitialNumberGroupSeparator,
          viewMode: stage.normalizedInitialViewMode,
          detailMode: stage.normalizedInitialDetailMode,
          treeConfig: stage.normalizedInitialTreeConfig,
          detailConfig: stage.normalizedInitialDetailConfig,
          reportDef: resetReportDef,
          savedReports: resetSavedReports,
          activeReportId: resetActiveReportId,
          showReportConfigColumn: true,
          customDimensions: resetCustomDimensions,
          measureAxis: resetMeasureAxis,
          filters: {},
          sorting: [],
          expanded: {},
          columnPinning: stage.initialColumnPinning,
          rowPinning: stage.initialRowPinning,
          sortOptions: stage.sortOptions,
          columnVisibility: {},
          columnOrder: [],
          columnSizing: {},
          reset: null
        });
      }
    }
  }, [stage.reset, stage.initialRowFields, stage.initialColFields, stage.normalizedInitialValConfigs, stage.initialColumnPinning, stage.initialRowPinning, stage.initialChartDefinition, stage.initialChartDefinitions, stage.normalizedInitialFieldPanelSizes, stage.normalizedInitialDecimalPlaces, stage.normalizedInitialDefaultValueFormat, stage.normalizedInitialNumberGroupSeparator, stage.normalizedInitialViewMode, stage.normalizedInitialDetailMode, stage.normalizedInitialTreeConfig, stage.normalizedInitialDetailConfig, stage.hasExternalReportDef, stage.normalizedInitialReportDef, stage.hasExternalSavedReports, stage.normalizedInitialSavedReports, stage.hasExternalActiveReportId, stage.normalizedInitialActiveReportId, stage.hasExternalCustomDimensions, stage.normalizedInitialCustomDimensions, stage.hasExternalMeasureAxis, stage.normalizedInitialMeasureAxis]);

  // Save Persistence
  useEffect(() => {
    if (!stage.persistence) return;
    stage.savePersistedState('columnPinning', stage.columnPinning);
    stage.savePersistedState('rowPinning', stage.rowPinning);
    stage.savePersistedState('grandTotalPinOverride', stage.grandTotalPinOverride);
    stage.savePersistedState('columnVisibility', stage.columnVisibility);
    stage.savePersistedState('columnOrder', stage.columnOrder);
    stage.savePersistedState('pivotLayoutOrder', stage.pivotLayoutOrder);
    stage.savePersistedState('pivotPinnedLayoutIds', stage.pivotPinnedLayoutIds);
    stage.savePersistedState('columnSizing', stage.columnSizing);
    stage.savePersistedState('measureAxis', stage.measureAxis);
    stage.savePersistedState('sidebarWidth', stage.sidebarWidth);
    stage.savePersistedState('fieldPanelSizes', stage.fieldPanelSizes);
    stage.savePersistedState('chartCanvasPanes', stage.chartCanvasPanes.filter(p => !p.isPreview));
    stage.savePersistedState('tableCanvasSize', stage.tableCanvasSize);
    stage.savePersistedState('chartPanelFloatingLayout', {
      floating: stage.chartPanelFloating,
      rect: stage.chartPanelFloatingRect
    });
    stage.savePersistedState('chartPanelLockState', {
      locked: stage.chartPanelLocked,
      lockedModel: stage.chartPanelLockedModel,
      lockedRequest: stage.chartPanelLockedRequest
    });
    stage.savePersistedState('viewMode', stage.viewMode);
    stage.savePersistedState('detailMode', stage.detailMode);
    stage.savePersistedState('treeConfig', stage.treeConfig);
    stage.savePersistedState('detailConfig', stage.detailConfig);
    stage.savePersistedState('reportDef', stage.reportDef);
    stage.savePersistedState('savedReports', stage.savedReports);
    stage.savePersistedState('activeReportId', stage.activeReportId);
    stage.savePersistedState('customDimensions', stage.customDimensions);
    stage.savePersistedState('frozenPivotConfig', stage.frozenPivotConfig);
  }, [stage.id, stage.columnPinning, stage.rowPinning, stage.grandTotalPinOverride, stage.columnVisibility, stage.columnOrder, stage.pivotLayoutOrder, stage.pivotPinnedLayoutIds, stage.columnSizing, stage.sidebarWidth, stage.fieldPanelSizes, stage.chartCanvasPanes, stage.tableCanvasSize, stage.chartPanelFloating, stage.chartPanelFloatingRect, stage.chartPanelLocked, stage.chartPanelLockedModel, stage.chartPanelLockedRequest, stage.viewMode, stage.detailMode, stage.treeConfig, stage.detailConfig, stage.persistence, stage.persistence_type, stage.reportDef, stage.savedReports, stage.activeReportId, stage.customDimensions, stage.measureAxis, stage.frozenPivotConfig]);
  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth < 768 && stage.columnPinning.right && stage.columnPinning.right.length > 0) {
        stage.setColumnPinning(prev => ({
          ...prev,
          right: []
        }));
        stage.showNotification("Right pinned columns hidden due to screen size.", "warning");
      }
    };
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [stage.columnPinning.right, stage.showNotification]);
  [stage.immersiveModeState, stage.setImmersiveModeState] = useState(stage.initialImmersiveMode || stage.uiConfig.lockImmersiveMode);
  stage.immersiveMode = stage.uiConfig.lockImmersiveMode ? true : stage.immersiveModeState;
  stage.setImmersiveMode = stage.uiConfig.lockImmersiveMode ? () => {} : stage.setImmersiveModeState;
  [stage.immersiveModeHovering, stage.setImmersiveModeHovering] = useState(false);
  [stage.showReportConfigColumn, stage.setShowReportConfigColumn] = useState(() => stage.loadPersistedState('showReportConfigColumn', true) !== false);
  [stage.showRowTotals, stage.setShowRowTotals] = useState(stage.initialShowRowTotals);
  [stage.showColTotals, stage.setShowColTotals] = useState(stage.initialShowColTotals);
  [stage.showSubtotals, stage.setShowSubtotals] = useState(() => {
    const persistedShowSubtotals = stage.loadPersistedState('showSubtotals', stage.initialShowSubtotals);
    return typeof persistedShowSubtotals === 'boolean' ? persistedShowSubtotals : stage.initialShowSubtotals;
  });
  useEffect(() => {
    if (!stage.persistence) return;
    stage.savePersistedState('layoutMode', stage.layoutMode);
    stage.savePersistedState('showSubtotals', stage.showSubtotals);
  }, [stage.layoutMode, stage.persistence, stage.savePersistedState, stage.showSubtotals]);
  useEffect(() => {
    if (!stage.persistence) return;
    stage.savePersistedState('showReportConfigColumn', stage.showReportConfigColumn !== false);
  }, [stage.persistence, stage.savePersistedState, stage.showReportConfigColumn]);
  stage.effectiveGrandTotalPinState = useMemo(() => resolveGrandTotalPinState(stage.showColTotals, stage.grandTotalPosition, stage.grandTotalPinOverride), [stage.showColTotals, stage.grandTotalPosition, stage.grandTotalPinOverride]);
  stage.serverSidePinsGrandTotal = stage.serverSide && stage.showColTotals && (stage.effectiveGrandTotalPinState === 'top' || stage.effectiveGrandTotalPinState === 'bottom');
  [stage.showRowNumbers, stage.setShowRowNumbers] = useState(false);
  ({
    sidebarOpen: stage.sidebarOpen,
    setSidebarOpen: stage.setSidebarOpen,
    sidebarWidth: stage.sidebarWidth,
    setSidebarWidth: stage.setSidebarWidth,
    fieldPanelSizes: stage.fieldPanelSizes,
    setFieldPanelSizes: stage.setFieldPanelSizes,
    activeFilterCol: stage.activeFilterCol,
    setActiveFilterCol: stage.setActiveFilterCol,
    filterAnchorEl: stage.filterAnchorEl,
    setFilterAnchorEl: stage.setFilterAnchorEl,
    sidebarTab: stage.sidebarTab,
    setSidebarTab: stage.setSidebarTab,
    showFloatingFilters: stage.showFloatingFilters,
    setShowFloatingFilters: stage.setShowFloatingFilters,
    stickyHeaders: stage.stickyHeaders,
    setStickyHeaders: stage.setStickyHeaders,
    colSearch: stage.colSearch,
    setColSearch: stage.setColSearch,
    colTypeFilter: stage.colTypeFilter,
    setColTypeFilter: stage.setColTypeFilter,
    selectedCols: stage.selectedCols,
    setSelectedCols: stage.setSelectedCols
  } = useSidebarUI({
    loadPersistedState: stage.loadPersistedState,
    clampSidebarWidth,
    externalFieldPanelSizes: stage.externalFieldPanelSizes,
    normalizedInitialFieldPanelSizes: stage.normalizedInitialFieldPanelSizes,
    sanitizeFieldPanelSizes,
    DEFAULT_FIELD_PANEL_SIZES
  }));
  [stage.hoveredHeaderId, stage.setHoveredHeaderId] = useState(null);
  [stage.focusedHeaderId, stage.setFocusedHeaderId] = useState(null);
  stage.hoveredHeaderIdRef = useRef(null);
  stage.hoveredHeaderIdRef.current = stage.hoveredHeaderId;
  stage.focusedHeaderIdRef = useRef(null);
  stage.focusedHeaderIdRef.current = stage.focusedHeaderId;

  // Global Keyboard Shortcuts
  useEffect(() => {
    const handleGlobalKeyDown = e => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'b') {
        e.preventDefault();
        stage.setSidebarOpen(prev => !prev);
      }
    };
    window.addEventListener('keydown', handleGlobalKeyDown);
    return () => window.removeEventListener('keydown', handleGlobalKeyDown);
  }, [stage.normalizeTrackedEventIds]);
  stage.handleConfigureReportLine = useCallback(rowData => {
    stage.setSidebarOpen(true);
    const nextSelection = buildReportEditorSelectionFromRow(stage.reportDefRef.current, rowData) || {
      type: 'node',
      path: []
    };
    stage.setReportEditorSelection(nextSelection);
  }, [stage.setSidebarOpen]);
  ({
    colorScaleMode: stage.colorScaleMode,
    setColorScaleMode: stage.setColorScaleMode,
    colorScaleColumns: stage.colorScaleColumns,
    setColorScaleColumns: stage.setColorScaleColumns,
    colorPalette: stage.colorPalette,
    setColorPalette: stage.setColorPalette,
    dataBarsColumns: stage.dataBarsColumns,
    dataBarMaximums: stage.dataBarMaximums,
    setDataBarsColumns: stage.setDataBarsColumns,
    setDataBarMaximums: stage.setDataBarMaximums,
    fontFamily: stage.fontFamily,
    setFontFamily: stage.setFontFamily,
    fontSize: stage.fontSize,
    setFontSize: stage.setFontSize,
    decimalPlaces: stage.decimalPlaces,
    setDecimalPlaces: stage.setDecimalPlaces,
    defaultValueFormat: stage.defaultValueFormat,
    setDefaultValueFormat: stage.setDefaultValueFormat,
    numberGroupSeparator: stage.numberGroupSeparator,
    setNumberGroupSeparator: stage.setNumberGroupSeparator,
    columnDecimalOverrides: stage.columnDecimalOverrides,
    setColumnDecimalOverrides: stage.setColumnDecimalOverrides,
    columnFormatOverrides: stage.columnFormatOverrides,
    setColumnFormatOverrides: stage.setColumnFormatOverrides,
    columnGroupSeparatorOverrides: stage.columnGroupSeparatorOverrides,
    setColumnGroupSeparatorOverrides: stage.setColumnGroupSeparatorOverrides,
    cellFormatRules: stage.cellFormatRules,
    setCellFormatRules: stage.setCellFormatRules,
    hoveredRowPath: stage.hoveredRowPath,
    setHoveredRowPath: stage.setHoveredRowPath,
    zoomLevel: stage.zoomLevel,
    setZoomLevel: stage.setZoomLevel
  } = useFormatState({
    normalizedInitialDecimalPlaces: stage.normalizedInitialDecimalPlaces,
    normalizedInitialDefaultValueFormat: stage.normalizedInitialDefaultValueFormat,
    normalizedInitialNumberGroupSeparator: stage.normalizedInitialNumberGroupSeparator
  }));
  stage.applyViewMode = useCallback(nextMode => {
    const normalizedNextMode = normalizeViewModeValue(nextMode) || 'pivot';
    if (normalizedNextMode === stage.viewMode) return;
    const enteringReport = normalizedNextMode === 'report' && stage.viewMode !== 'report';
    const leavingReport = stage.viewMode === 'report' && normalizedNextMode !== 'report';
    if (enteringReport) {
      stage.setFrozenPivotConfig({
        rowFields: stage.rowFields,
        colFields: stage.colFields,
        valConfigs: stage.valConfigs.map(v => ({
          ...v
        }))
      });
      const levelFields = collectReportFields(stage.reportDef);
      if (levelFields.length > 0) {
        stage.setRowFields(levelFields);
      }
      stage.setExpanded({});
    } else if (leavingReport) {
      if (normalizedNextMode === 'pivot' && stage.frozenPivotConfig) {
        stage.setRowFields(stage.frozenPivotConfig.rowFields || []);
        stage.setColFields(stage.frozenPivotConfig.colFields || []);
        stage.setValConfigs(normalizeSparklineValConfigsForView(stage.frozenPivotConfig.valConfigs || [], stage.normalizedInitialValConfigs));
        stage.setFrozenPivotConfig(null);
      }
      stage.setExpanded({});
    }
    stage.setViewMode(normalizedNextMode);
  }, [stage.viewMode, stage.rowFields, stage.colFields, stage.valConfigs, stage.reportDef, stage.frozenPivotConfig, stage.normalizedInitialValConfigs]);
  stage.handleSetPivotMode = useCallback(nextMode => {
    stage.applyViewMode(nextMode === 'report' ? 'report' : 'pivot');
  }, [stage.applyViewMode]);
  useEffect(() => {
    const normalizedExternalMode = normalizeViewModeValue(stage.externalViewMode);
    if (!normalizedExternalMode) return;
    if (normalizedExternalMode === stage.lastViewModePropRef.current) return;
    stage.lastViewModePropRef.current = normalizedExternalMode;
    stage.applyViewMode(normalizedExternalMode);
  }, [stage.applyViewMode, stage.externalViewMode]);
  useEffect(() => {
    const normalizedExternalMode = normalizeDetailModeValue(stage.externalDetailMode);
    if (!normalizedExternalMode) return;
    if (normalizedExternalMode === stage.lastDetailModePropRef.current) return;
    stage.lastDetailModePropRef.current = normalizedExternalMode;
    stage.setDetailMode(previousMode => previousMode === normalizedExternalMode ? previousMode : normalizedExternalMode);
  }, [stage.externalDetailMode]);
  useEffect(() => {
    if (stage.externalTreeConfig === undefined) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialTreeConfig);
    if (serializedExternal === stage.lastTreeConfigPropRef.current) return;
    stage.lastTreeConfigPropRef.current = serializedExternal;
    stage.setTreeConfig(previousConfig => JSON.stringify(previousConfig || {}) === serializedExternal ? previousConfig : stage.normalizedInitialTreeConfig);
  }, [stage.externalTreeConfig, stage.normalizedInitialTreeConfig]);
  useEffect(() => {
    if (stage.externalDetailConfig === undefined) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialDetailConfig);
    if (serializedExternal === stage.lastDetailConfigPropRef.current) return;
    stage.lastDetailConfigPropRef.current = serializedExternal;
    stage.setDetailConfig(previousConfig => JSON.stringify(previousConfig || {}) === serializedExternal ? previousConfig : stage.normalizedInitialDetailConfig);
  }, [stage.externalDetailConfig, stage.normalizedInitialDetailConfig]);
  useEffect(() => {
    if (stage.externalReportDef === undefined) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialReportDef);
    if (serializedExternal === stage.lastReportDefPropRef.current) return;
    stage.lastReportDefPropRef.current = serializedExternal;
    stage.setReportDef(previousReportDef => JSON.stringify(previousReportDef || {
      levels: []
    }) === serializedExternal ? previousReportDef : stage.normalizedInitialReportDef);
  }, [stage.externalReportDef, stage.normalizedInitialReportDef]);
  useEffect(() => {
    if (stage.externalSavedReports === undefined) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialSavedReports);
    if (serializedExternal === stage.lastSavedReportsPropRef.current) return;
    stage.lastSavedReportsPropRef.current = serializedExternal;
    stage.setSavedReports(previousSavedReports => JSON.stringify(previousSavedReports || []) === serializedExternal ? previousSavedReports : stage.normalizedInitialSavedReports);
  }, [stage.externalSavedReports, stage.normalizedInitialSavedReports]);
  useEffect(() => {
    if (stage.externalActiveReportId === undefined) return;
    if (stage.normalizedInitialActiveReportId === stage.lastActiveReportIdPropRef.current) return;
    stage.lastActiveReportIdPropRef.current = stage.normalizedInitialActiveReportId;
    stage.setActiveReportId(previousActiveReportId => previousActiveReportId === stage.normalizedInitialActiveReportId ? previousActiveReportId : stage.normalizedInitialActiveReportId);
  }, [stage.externalActiveReportId, stage.normalizedInitialActiveReportId]);

  // When reportDef levels change in report mode, update rowFields to match
  useEffect(() => {
    if (stage.viewMode !== 'report') return;
    const levelFields = collectReportFields(stage.reportDef);
    if (levelFields.length > 0 && JSON.stringify(levelFields) !== JSON.stringify(stage.rowFields)) {
      stage.setRowFields(levelFields);
      stage.setExpanded({});
    }
  }, [stage.reportDef, stage.rowFields, stage.viewMode]); // eslint-disable-line react-hooks/exhaustive-deps

  // Push report-mode state to Dash props so server and persistence flows stay in sync
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    if (!stage.didPublishReportStateRef.current) {
      stage.didPublishReportStateRef.current = true;
      return;
    }
    stage.setPropsRef.current({
      viewMode: stage.viewMode,
      reportDef: stage.reportDef,
      savedReports: stage.savedReports,
      activeReportId: stage.activeReportId
    });
  }, [stage.viewMode, stage.reportDef, stage.savedReports, stage.activeReportId]);

  // selectedCellKeys and selectedCellColIds are now derived inside PivotAppBar
  // to avoid stale prop issues between parent and child renders.

  // Decimal formatting logic moved to PivotAppBar to use fresh selectedCells
  [stage.spacingMode, stage.setSpacingMode] = useState(1);
  stage.spacingLabels = gridDimensionTokens.density.spacingLabels;
  stage.rowHeights = gridDimensionTokens.density.rowHeights;
  stage.defaultColumnWidths = gridDimensionTokens.columnWidths;
  stage.autoSizeBounds = gridDimensionTokens.autoSize;
  [stage.colExpanded, stage.setColExpanded] = useState({});
  ({
    contextMenu: stage.contextMenu,
    setContextMenu: stage.setContextMenu,
    selectedCells: stage.selectedCells,
    setSelectedCells: stage.setSelectedCells,
    lastSelected: stage.lastSelected,
    setLastSelected: stage.setLastSelected,
    isDragging: stage.isDragging,
    setIsDragging: stage.setIsDragging,
    dragStart: stage.dragStart,
    setDragStart: stage.setDragStart,
    isFilling: stage.isFilling,
    setIsFilling: stage.setIsFilling,
    fillRange: stage.fillRange,
    setFillRange: stage.setFillRange,
    isRowSelecting: stage.isRowSelecting,
    setIsRowSelecting: stage.setIsRowSelecting,
    rowDragStart: stage.rowDragStart,
    setRowDragStart: stage.setRowDragStart
  } = useCellInteraction());
  stage.isSelectAllServerRef = useRef(false);
  const exportState = useRuntimeExportState();
  for (const [key, value] of Object.entries(exportState)) stage[key] = value;
  stage.transactionControls = usePivotTransactions();
  ({
    configure: stage.configureTransactions,
    transactionHistoryState: stage.transactionHistoryState,
    syncTransactionHistoryState: stage.syncTransactionHistoryState,
    findTransactionHistoryEntryByEventId: stage.findTransactionHistoryEntryByEventId,
    sortEventIdsByActiveHistory: stage.sortEventIdsByActiveHistory,
    pruneTransactionHistoryEntries: stage.pruneTransactionHistoryEntries,
    pushUnifiedHistoryEntry: stage.pushUnifiedHistoryEntry,
    queueTransactionHistoryRequest: stage.queueTransactionHistoryRequest,
    clearTransactionHistory: stage.clearTransactionHistory,
    dispatchTransactionRequest: stage.dispatchTransactionRequest,
    finalizeTransactionHistoryResponse: stage.finalizeTransactionHistoryResponse,
    clearPendingTransactionHistoryRequest: stage.clearPendingTransactionHistoryRequest,
    executeUndoTransaction: stage.executeUndoTransaction,
    executeRedoTransaction: stage.executeRedoTransaction
  } = stage.transactionControls);
  stage.handleUndo = useCallback(() => {
    stage.executeUndoTransaction();
  }, [stage.executeUndoTransaction]);
  stage.handleRedo = useCallback(() => {
    stage.executeRedoTransaction();
  }, [stage.executeRedoTransaction]);
  stage.handleRefresh = () => {
    if (stage.setProps) stage.setProps({
      refresh: Date.now()
    });
  };
  useEffect(() => {
    if (themes[stage.defaultTheme]) {
      stage.setThemeName(stage.defaultTheme);
    }
  }, [stage.defaultTheme]);
  useEffect(() => {
    const previousRowFields = stage.previousRowFieldsRef.current || [];
    if (JSON.stringify(previousRowFields) === JSON.stringify(stage.rowFields)) return;
    stage.previousRowFieldsRef.current = stage.rowFields;
    stage.pinnedRowCacheRef.current.clear();
    stage.setRowPinning(prev => {
      const normalized = normalizeRowPinningState(prev);
      if (normalized.top.length === 0 && normalized.bottom.length === 0) {
        return prev;
      }
      return {
        top: [],
        bottom: []
      };
    });
  }, [stage.rowFields]);
  useEffect(() => {
    stage.setPivotColumnSorting(prev => {
      const activeFields = new Set(stage.colFields || []);
      const next = Object.entries(prev || {}).reduce((acc, [field, sortState]) => {
        if (activeFields.has(field)) acc[field] = sortState;
        return acc;
      }, {});
      return JSON.stringify(next) === JSON.stringify(prev || {}) ? prev : next;
    });
  }, [stage.colFields]);
  useEffect(() => {
    if (stage.chartPanelHierarchyLevel === 'all') return;
    if (typeof stage.chartPanelHierarchyLevel === 'number' && stage.chartPanelHierarchyLevel <= Math.max(stage.rowFields.length, 0)) return;
    stage.setChartPanelHierarchyLevel('all');
  }, [stage.chartPanelHierarchyLevel, stage.rowFields.length]);
  useEffect(() => {
    if (!stage.chartPanelOrientationAutoRef.current) return;
    const preferredOrientation = getPreferredChartOrientation(stage.colFields);
    stage.setChartPanelOrientation(prev => prev === preferredOrientation ? prev : preferredOrientation);
  }, [stage.colFields]);
  stage.handleChartPanelOrientationChange = useCallback(nextOrientation => {
    stage.chartPanelOrientationAutoRef.current = false;
    stage.setChartPanelOrientation(nextOrientation);
  }, []);
  stage.currentChartDefinition = useMemo(() => normalizeChartDefinition({
    id: stage.activeChartDefinitionId || 'live-chart-panel',
    name: (stage.managedChartDefinitions.find(definition => definition.id === stage.activeChartDefinitionId) || {}).name || 'Live Chart',
    chartTitle: stage.chartPanelTitle,
    source: stage.chartPanelSource,
    chartType: stage.chartPanelType,
    barLayout: stage.chartPanelBarLayout,
    axisMode: stage.chartPanelAxisMode,
    orientation: stage.chartPanelOrientation,
    hierarchyLevel: stage.chartPanelHierarchyLevel,
    chartLayers: stage.chartPanelLayers,
    rowLimit: stage.chartPanelRowLimit,
    columnLimit: stage.chartPanelColumnLimit,
    width: stage.chartPanelWidth,
    chartHeight: stage.chartPanelGraphHeight,
    sortMode: stage.chartPanelSortMode,
    interactionMode: stage.chartPanelInteractionMode,
    serverScope: stage.chartPanelServerScope
  }, stage.initialChartDefinition), [stage.activeChartDefinitionId, stage.managedChartDefinitions, stage.chartPanelTitle, stage.chartPanelSource, stage.chartPanelType, stage.chartPanelBarLayout, stage.chartPanelAxisMode, stage.chartPanelOrientation, stage.chartPanelHierarchyLevel, stage.chartPanelLayers, stage.chartPanelRowLimit, stage.chartPanelColumnLimit, stage.chartPanelWidth, stage.chartPanelGraphHeight, stage.chartPanelSortMode, stage.chartPanelInteractionMode, stage.chartPanelServerScope, stage.initialChartDefinition]);
  useEffect(() => {
    const serializedExternal = JSON.stringify(stage.chartDefinitions || null);
    if (serializedExternal === stage.lastChartDefinitionsPropRef.current) return;
    stage.lastChartDefinitionsPropRef.current = serializedExternal;
    if (!Array.isArray(stage.chartDefinitions) || stage.chartDefinitions.length === 0) return;
    const nextDefinitions = sanitizeChartDefinitions(stage.chartDefinitions, stage.initialChartDefinition);
    stage.setManagedChartDefinitions(nextDefinitions);
    stage.setActiveChartDefinitionId(previousId => nextDefinitions.some(definition => definition.id === previousId) ? previousId : nextDefinitions[0].id);
  }, [stage.chartDefinitions, stage.initialChartDefinition]);
  useEffect(() => {
    if (Array.isArray(stage.chartDefinitions) && stage.chartDefinitions.length > 0) return;
    stage.setManagedChartDefinitions(stage.initialChartDefinitions);
    stage.setActiveChartDefinitionId(stage.initialChartDefinitions[0] ? stage.initialChartDefinitions[0].id : 'live-chart-panel');
  }, [stage.chartDefinitions, stage.initialChartDefinitions]);
  useEffect(() => {
    const serializedExternal = JSON.stringify(stage.externalChartCanvasPanes || null);
    if (serializedExternal === stage.lastChartCanvasPanesPropRef.current) return;
    stage.lastChartCanvasPanesPropRef.current = serializedExternal;
    if (!Array.isArray(stage.externalChartCanvasPanes)) return;
    stage.setChartCanvasPanes(sanitizeChartCanvasPanes(stage.externalChartCanvasPanes, stage.initialChartDefinition));
  }, [stage.externalChartCanvasPanes, stage.initialChartDefinition]);
  useEffect(() => {
    const normalizedExternalSize = Number(stage.externalTableCanvasSize);
    if (!Number.isFinite(normalizedExternalSize) || normalizedExternalSize <= 0) return;
    if (normalizedExternalSize === stage.lastTableCanvasSizePropRef.current) return;
    stage.lastTableCanvasSizePropRef.current = normalizedExternalSize;
    stage.setTableCanvasSize(normalizedExternalSize);
  }, [stage.externalTableCanvasSize]);
  useEffect(() => {
    const activeDefinition = stage.managedChartDefinitions.find(definition => definition.id === stage.activeChartDefinitionId);
    if (!activeDefinition || stage.applyingChartDefinitionRef.current) return;
    stage.applyingChartDefinitionRef.current = true;
    stage.setChartPanelSource(activeDefinition.source);
    stage.setChartPanelType(activeDefinition.chartType);
    stage.setChartPanelBarLayout(activeDefinition.barLayout);
    stage.setChartPanelAxisMode(activeDefinition.axisMode);
    stage.chartPanelOrientationAutoRef.current = false;
    stage.setChartPanelOrientation(activeDefinition.orientation);
    stage.setChartPanelHierarchyLevel(activeDefinition.hierarchyLevel);
    stage.setChartPanelTitle(activeDefinition.chartTitle || activeDefinition.name || 'Chart');
    stage.setChartPanelLayers(Array.isArray(activeDefinition.chartLayers) ? activeDefinition.chartLayers : []);
    stage.setChartPanelRowLimit(activeDefinition.rowLimit);
    stage.setChartPanelColumnLimit(activeDefinition.columnLimit);
    stage.setChartPanelWidth(activeDefinition.width);
    stage.setChartPanelGraphHeight(activeDefinition.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT);
    stage.setChartPanelSortMode(activeDefinition.sortMode || 'natural');
    stage.setChartPanelInteractionMode(activeDefinition.interactionMode || 'focus');
    stage.setChartPanelServerScope(activeDefinition.serverScope || stage.initialChartDefinition.serverScope);
    requestAnimationFrame(() => {
      stage.applyingChartDefinitionRef.current = false;
    });
  }, [stage.activeChartDefinitionId, stage.managedChartDefinitions, stage.initialChartDefinition.serverScope]);
  useEffect(() => {
    if (stage.applyingChartDefinitionRef.current) return;
    stage.setManagedChartDefinitions(previousDefinitions => {
      const existingDefinitions = Array.isArray(previousDefinitions) && previousDefinitions.length > 0 ? previousDefinitions : [stage.currentChartDefinition];
      const nextDefinitions = existingDefinitions.map(definition => definition.id === stage.currentChartDefinition.id ? {
        ...definition,
        ...stage.currentChartDefinition
      } : definition);
      if (!nextDefinitions.some(definition => definition.id === stage.currentChartDefinition.id)) {
        nextDefinitions.push(stage.currentChartDefinition);
      }
      const previousJson = JSON.stringify(existingDefinitions);
      const nextJson = JSON.stringify(nextDefinitions);
      if (previousJson === nextJson) return previousDefinitions;
      if (stage.setPropsRef.current) {
        // PivotWorkspace mirrors setProps locally for host-neutral usage.
        // Remember this write before it comes back through props so a chart
        // panel width drag is not reset by its own echoed definition.
        stage.lastChartDefinitionsPropRef.current = nextJson;
        stage.setPropsRef.current({
          chartDefinitions: nextDefinitions
        });
      }
      return nextDefinitions;
    });
  }, [stage.currentChartDefinition]);
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    const nextChartCanvasPanes = stage.chartCanvasPanes.map(pane => ({
        ...pane
    }));
    // A pane resize is local state. The public prop write is retained for
    // Dash and controlled React hosts, but must not be reapplied when the
    // host-neutral PivotWorkspace mirrors it into runtimeProps.
    stage.lastChartCanvasPanesPropRef.current = JSON.stringify(nextChartCanvasPanes);
    stage.lastTableCanvasSizePropRef.current = stage.tableCanvasSize;
    stage.setPropsRef.current({
      chartCanvasPanes: nextChartCanvasPanes,
      tableCanvasSize: stage.tableCanvasSize
    });
  }, [stage.chartCanvasPanes, stage.tableCanvasSize]);
  useEffect(() => {
    const serializedExternal = JSON.stringify(stage.normalizedInitialValConfigs || []);
    if (serializedExternal === stage.lastValConfigsPropRef.current) return;
    stage.lastValConfigsPropRef.current = serializedExternal;
    if (!Array.isArray(stage.initialValConfigs)) return;
    stage.setValConfigs(stage.normalizedInitialValConfigs);
  }, [stage.initialValConfigs, stage.normalizedInitialValConfigs]);
  stage.lastCustomDimensionsPropRef = useRef(JSON.stringify(stage.normalizedInitialCustomDimensions || []));
  useEffect(() => {
    if (!stage.hasExternalCustomDimensions) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialCustomDimensions || []);
    if (serializedExternal === stage.lastCustomDimensionsPropRef.current) return;
    stage.lastCustomDimensionsPropRef.current = serializedExternal;
    stage.setCustomDimensions(stage.normalizedInitialCustomDimensions);
  }, [stage.hasExternalCustomDimensions, stage.normalizedInitialCustomDimensions]);
  stage.lastMeasureAxisPropRef = useRef(JSON.stringify(stage.normalizedInitialMeasureAxis || {}));
  useEffect(() => {
    if (!stage.hasExternalMeasureAxis) return;
    const serializedExternal = JSON.stringify(stage.normalizedInitialMeasureAxis || {});
    if (serializedExternal === stage.lastMeasureAxisPropRef.current) return;
    stage.lastMeasureAxisPropRef.current = serializedExternal;
    stage.setMeasureAxis(stage.normalizedInitialMeasureAxis);
  }, [stage.hasExternalMeasureAxis, stage.normalizedInitialMeasureAxis]);
  useEffect(() => {
    const normalizedExternal = clampDecimalPlaces(stage.externalDecimalPlaces, 0);
    if (normalizedExternal === stage.lastDecimalPlacesPropRef.current) return;
    stage.lastDecimalPlacesPropRef.current = normalizedExternal;
    stage.setDecimalPlaces(normalizedExternal);
  }, [stage.externalDecimalPlaces]);
  useEffect(() => {
    const serializedExternal = stage.externalFieldPanelSizes && typeof stage.externalFieldPanelSizes === 'object' ? JSON.stringify(stage.normalizedInitialFieldPanelSizes) : null;
    if (serializedExternal === stage.lastFieldPanelSizesPropRef.current) return;
    stage.lastFieldPanelSizesPropRef.current = serializedExternal;
    if (!stage.externalFieldPanelSizes || typeof stage.externalFieldPanelSizes !== 'object') return;
    stage.setFieldPanelSizes(stage.normalizedInitialFieldPanelSizes);
  }, [stage.externalFieldPanelSizes, stage.normalizedInitialFieldPanelSizes]);
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    if (!stage.didPublishFieldPanelSizesRef.current) {
      stage.didPublishFieldPanelSizesRef.current = true;
      return;
    }
    stage.setPropsRef.current({
      fieldPanelSizes: stage.fieldPanelSizes
    });
  }, [stage.fieldPanelSizes]);
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    if (!stage.didPublishDecimalPlacesRef.current) {
      stage.didPublishDecimalPlacesRef.current = true;
      return;
    }
    stage.setPropsRef.current({
      decimalPlaces: stage.decimalPlaces
    });
  }, [stage.decimalPlaces]);
  useEffect(() => {
    const normalizedExternal = normalizeDefaultValueFormat(stage.externalDefaultValueFormat);
    if (normalizedExternal === stage.lastDefaultValueFormatPropRef.current) return;
    stage.lastDefaultValueFormatPropRef.current = normalizedExternal;
    stage.setDefaultValueFormat(normalizedExternal);
  }, [stage.externalDefaultValueFormat]);
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    if (!stage.didPublishDefaultValueFormatRef.current) {
      stage.didPublishDefaultValueFormatRef.current = true;
      return;
    }
    stage.setPropsRef.current({
      defaultValueFormat: stage.defaultValueFormat
    });
  }, [stage.defaultValueFormat]);
  useEffect(() => {
    const normalizedExternal = normalizeNumberGroupSeparator(stage.externalNumberGroupSeparator);
    if (normalizedExternal === stage.lastNumberGroupSeparatorPropRef.current) return;
    stage.lastNumberGroupSeparatorPropRef.current = normalizedExternal;
    if (typeof stage.externalNumberGroupSeparator !== 'string') return;
    stage.setNumberGroupSeparator(normalizedExternal);
  }, [stage.externalNumberGroupSeparator]);
  useEffect(() => {
    if (!stage.setPropsRef.current) return;
    if (!stage.didPublishNumberGroupSeparatorRef.current) {
      stage.didPublishNumberGroupSeparatorRef.current = true;
      return;
    }
    stage.setPropsRef.current({
      numberGroupSeparator: stage.numberGroupSeparator
    });
  }, [stage.numberGroupSeparator]);
  // Restore after initial prop synchronization so saved formatting and values win.
  const appliedViewStateRef = useRef(null);
  useEffect(() => {
    if (stage.viewState === appliedViewStateRef.current) return;
    appliedViewStateRef.current = stage.viewState;
    if (!stage.viewState || typeof stage.viewState !== 'object') return;
    const restored = stage.viewState.state && typeof stage.viewState.state === 'object' ? stage.viewState.state : stage.viewState;
    const restoredChartDefinitions = Array.isArray(restored.chartDefinitions) ? sanitizeChartDefinitions(restored.chartDefinitions, stage.initialChartDefinition) : null;
    const restoredActiveChartDefinitionId = typeof restored.activeChartDefinitionId === 'string' ? restored.activeChartDefinitionId : null;
    const restoredChartDefinition = Array.isArray(restoredChartDefinitions) ? restoredChartDefinitions.find(definition => definition.id === restoredActiveChartDefinitionId) || restoredChartDefinitions[0] || null : null;
    const sanitizedFilters = restored.filters && typeof restored.filters === 'object' ? {
      ...restored.filters
    } : null;
    if (restored.viewMode === 'pivot' || restored.viewMode === 'report' || restored.viewMode === 'tree' || restored.viewMode === 'table') {
      stage.setViewMode(restored.viewMode);
    } else if (restored.pivotMode === 'pivot' || restored.pivotMode === 'report') {
      stage.setViewMode(restored.pivotMode);
    }
    if (restored.detailMode === 'none' || restored.detailMode === 'inline' || restored.detailMode === 'sidepanel' || restored.detailMode === 'drawer') stage.setDetailMode(restored.detailMode);
    if (restored.treeConfig && typeof restored.treeConfig === 'object') stage.setTreeConfig(normalizeTreeConfigValue(restored.treeConfig));
    if (restored.detailConfig && typeof restored.detailConfig === 'object') stage.setDetailConfig(normalizeDetailConfigValue(restored.detailConfig));
    if (Object.prototype.hasOwnProperty.call(restored, 'reportDef')) {
      stage.setReportDef(normalizeReportDefValue(restored.reportDef));
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'savedReports')) {
      stage.setSavedReports(normalizeSavedReportsValue(restored.savedReports));
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'activeReportId')) {
      stage.setActiveReportId(normalizeActiveReportIdValue(restored.activeReportId));
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'frozenPivotConfig')) {
      stage.setFrozenPivotConfig(restored.frozenPivotConfig && typeof restored.frozenPivotConfig === 'object' ? restored.frozenPivotConfig : null);
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'customDimensions')) {
      stage.setCustomDimensions(normalizeCustomDimensionsValue(restored.customDimensions));
    }
    if (Array.isArray(restored.rowFields)) stage.setRowFields(restored.rowFields);
    if (Array.isArray(restored.colFields)) stage.setColFields(restored.colFields);
    if (Array.isArray(restored.valConfigs)) stage.setValConfigs(normalizeSparklineValConfigsForView(restored.valConfigs, stage.normalizedInitialValConfigs));
    if (sanitizedFilters) stage.setFilters(sanitizedFilters);
    if (Array.isArray(restored.sorting)) stage.setSorting(restored.sorting);
    if (restored.expanded === true || restored.expanded === false || restored.expanded && typeof restored.expanded === 'object') {
      stage.setExpanded(restored.expanded);
    }
    if (typeof restored.immersiveMode === 'boolean') stage.setImmersiveMode(restored.immersiveMode);
    if (typeof restored.showRowTotals === 'boolean') stage.setShowRowTotals(restored.showRowTotals);
    if (typeof restored.showColTotals === 'boolean') stage.setShowColTotals(restored.showColTotals);
    if (typeof restored.showSubtotals === 'boolean') stage.setShowSubtotals(restored.showSubtotals);
    if (typeof restored.showRowNumbers === 'boolean') stage.setShowRowNumbers(restored.showRowNumbers);
    if (typeof restored.showReportConfigColumn === 'boolean') stage.setShowReportConfigColumn(restored.showReportConfigColumn);
    if (typeof restored.sidebarOpen === 'boolean') stage.setSidebarOpen(restored.sidebarOpen);
    if (typeof restored.sidebarWidth === 'number') stage.setSidebarWidth(clampSidebarWidth(restored.sidebarWidth, 288));
    if (typeof restored.sidebarTab === 'string') stage.setSidebarTab(restored.sidebarTab);
    if (restored.fieldPanelSizes && typeof restored.fieldPanelSizes === 'object') {
      stage.setFieldPanelSizes(sanitizeFieldPanelSizes(restored.fieldPanelSizes));
    }
    if (typeof restored.showFloatingFilters === 'boolean') stage.setShowFloatingFilters(restored.showFloatingFilters);
    if (typeof restored.stickyHeaders === 'boolean') stage.setStickyHeaders(restored.stickyHeaders);
    if (typeof restored.colSearch === 'string') stage.setColSearch(restored.colSearch);
    if (typeof restored.colTypeFilter === 'string') stage.setColTypeFilter(restored.colTypeFilter);
    if (typeof restored.themeName === 'string' && themes[restored.themeName]) stage.setThemeName(restored.themeName);
    if (restored.themeOverrides && typeof restored.themeOverrides === 'object') stage.setThemeOverrides(restored.themeOverrides);
    if (restored.editValueDisplayMode === 'original' || restored.editValueDisplayMode === 'edited') {
      stage.setEditValueDisplayMode(restored.editValueDisplayMode);
    }
    if (typeof restored.layoutMode === 'string') stage.setLayoutMode(normalizeLayoutModeValue(restored.layoutMode));
    if (typeof restored.colorScaleMode === 'string') stage.setColorScaleMode(restored.colorScaleMode);
    stage.setColorScaleColumns(Array.isArray(restored.colorScaleColumns)
      ? restored.colorScaleColumns.filter(column => typeof column === 'string') : null);
    if (typeof restored.colorPalette === 'string') stage.setColorPalette(restored.colorPalette);
    if (typeof restored.spacingMode === 'number') stage.setSpacingMode(restored.spacingMode);
    if (Array.isArray(restored.dataBarsColumns)) stage.setDataBarsColumns(new Set(restored.dataBarsColumns));
    stage.setDataBarMaximums(restored.dataBarMaximums);
    if (restored.pivotColumnSorting && typeof restored.pivotColumnSorting === 'object') stage.setPivotColumnSorting(restored.pivotColumnSorting);
    if (restored.columnPinning && typeof restored.columnPinning === 'object') stage.setColumnPinning(restored.columnPinning);
    const restoredRowPinning = restored.rowPinning && typeof restored.rowPinning === 'object' ? restored.rowPinning : null;
    if (restoredRowPinning) stage.setRowPinning(restoredRowPinning);
    if (Object.prototype.hasOwnProperty.call(restored, 'grandTotalPinOverride')) {
      stage.setGrandTotalPinOverride(sanitizeGrandTotalPinOverride(restored.grandTotalPinOverride));
    } else {
      stage.setGrandTotalPinOverride(getPinnedSideForRow(restoredRowPinning, GRAND_TOTAL_ROW_ID));
    }
    if (restored.columnVisibility && typeof restored.columnVisibility === 'object') stage.setColumnVisibility(restored.columnVisibility);
    if (Array.isArray(restored.columnOrder)) stage.setColumnOrder(normalizeColumnOrderValue(restored.columnOrder));
    if (Array.isArray(restored.pivotLayoutOrder)) {
      const restoredOrder = normalizeColumnOrderValue(restored.pivotLayoutOrder);
      // Requests retain this immutable manifest as their layout token. A full
      // view restore can repeat the per-key persisted state during mounting.
      stage.setPivotLayoutOrder(previous => areStringArraysEqual(previous, restoredOrder) ? previous : restoredOrder);
      stage.layoutRevisionRef.current = null;
    }
    if (restored.pivotPinnedLayoutIds && typeof restored.pivotPinnedLayoutIds === 'object') {
      stage.setPivotPinnedLayoutIds(restored.pivotPinnedLayoutIds);
    }
    if (restored.columnSizing && typeof restored.columnSizing === 'object') stage.setColumnSizing(restored.columnSizing);
    if (restored.colExpanded && typeof restored.colExpanded === 'object') stage.setColExpanded(restored.colExpanded);
    if (typeof restored.decimalPlaces === 'number') stage.setDecimalPlaces(restored.decimalPlaces);
    if (typeof restored.defaultValueFormat === 'string') {
      stage.setDefaultValueFormat(normalizeDefaultValueFormat(restored.defaultValueFormat));
    }
    if (typeof restored.numberGroupSeparator === 'string') {
      stage.setNumberGroupSeparator(normalizeNumberGroupSeparator(restored.numberGroupSeparator));
    }
    if (typeof restored.zoomLevel === 'number') stage.setZoomLevel(Math.max(60, Math.min(160, restored.zoomLevel)));
    stage.setColumnDecimalOverrides(restored.columnDecimalOverrides && typeof restored.columnDecimalOverrides === 'object' ? restored.columnDecimalOverrides : {});
    stage.setColumnFormatOverrides(restored.columnFormatOverrides && typeof restored.columnFormatOverrides === 'object' ? restored.columnFormatOverrides : {});
    stage.setColumnGroupSeparatorOverrides(restored.columnGroupSeparatorOverrides && typeof restored.columnGroupSeparatorOverrides === 'object' ? restored.columnGroupSeparatorOverrides : {});
    if (restored.cellFormatRules && typeof restored.cellFormatRules === 'object') stage.setCellFormatRules(restored.cellFormatRules);
    if (restoredChartDefinitions) {
      stage.setManagedChartDefinitions(restoredChartDefinitions);
      stage.setActiveChartDefinitionId(restoredChartDefinition ? restoredChartDefinition.id : restoredChartDefinitions[0].id);
    }
    const restoredChartSource = restored.chartPanelSource !== undefined ? restored.chartPanelSource : restoredChartDefinition && restoredChartDefinition.source;
    if (restoredChartSource === 'pivot' || restoredChartSource === 'selection') stage.setChartPanelSource(restoredChartSource);
    const restoredChartType = restored.chartPanelType !== undefined ? restored.chartPanelType : restoredChartDefinition && restoredChartDefinition.chartType;
    if (VALID_CHART_TYPES.has(restoredChartType)) stage.setChartPanelType(restoredChartType);
    const restoredBarLayout = restored.chartPanelBarLayout !== undefined ? restored.chartPanelBarLayout : restoredChartDefinition && restoredChartDefinition.barLayout;
    if (restoredBarLayout === 'grouped' || restoredBarLayout === 'stacked') stage.setChartPanelBarLayout(restoredBarLayout);
    const restoredAxisMode = restored.chartPanelAxisMode !== undefined ? restored.chartPanelAxisMode : restoredChartDefinition && restoredChartDefinition.axisMode;
    if (restoredAxisMode === 'vertical' || restoredAxisMode === 'horizontal') stage.setChartPanelAxisMode(restoredAxisMode);
    if (restored.chartPanelOrientation === 'rows' || restored.chartPanelOrientation === 'columns') {
      stage.chartPanelOrientationAutoRef.current = false;
      stage.setChartPanelOrientation(restored.chartPanelOrientation);
    } else if (restoredChartDefinition && (restoredChartDefinition.orientation === 'rows' || restoredChartDefinition.orientation === 'columns')) {
      stage.chartPanelOrientationAutoRef.current = false;
      stage.setChartPanelOrientation(restoredChartDefinition.orientation);
    } else {
      stage.chartPanelOrientationAutoRef.current = true;
      stage.setChartPanelOrientation(getPreferredChartOrientation(restored.colFields || stage.colFields));
    }
    const restoredHierarchyLevel = restored.chartPanelHierarchyLevel !== undefined ? restored.chartPanelHierarchyLevel : restoredChartDefinition && restoredChartDefinition.hierarchyLevel;
    if (restoredHierarchyLevel === 'all' || typeof restoredHierarchyLevel === 'number' && restoredHierarchyLevel >= 1) {
      stage.setChartPanelHierarchyLevel(restoredHierarchyLevel);
    }
    const restoredChartTitle = restored.chartPanelTitle !== undefined ? restored.chartPanelTitle : restoredChartDefinition && restoredChartDefinition.chartTitle;
    if (typeof restoredChartTitle === 'string') {
      stage.setChartPanelTitle(restoredChartTitle);
    }
    const restoredChartLayers = Array.isArray(restored.chartPanelLayers) ? restored.chartPanelLayers : restoredChartDefinition && Array.isArray(restoredChartDefinition.chartLayers) ? restoredChartDefinition.chartLayers : null;
    if (Array.isArray(restoredChartLayers)) {
      stage.setChartPanelLayers(restoredChartLayers);
    }
    const restoredRowLimit = restored.chartPanelRowLimit !== undefined ? restored.chartPanelRowLimit : restoredChartDefinition && restoredChartDefinition.rowLimit;
    if (typeof restoredRowLimit === 'number') {
      stage.setChartPanelRowLimit(Math.max(1, Math.floor(restoredRowLimit)));
    }
    const restoredColumnLimit = restored.chartPanelColumnLimit !== undefined ? restored.chartPanelColumnLimit : restoredChartDefinition && restoredChartDefinition.columnLimit;
    if (typeof restoredColumnLimit === 'number') {
      stage.setChartPanelColumnLimit(Math.max(1, Math.floor(restoredColumnLimit)));
    }
    const restoredChartWidth = restored.chartPanelWidth !== undefined ? restored.chartPanelWidth : restoredChartDefinition && restoredChartDefinition.width;
    if (typeof restoredChartWidth === 'number') {
      stage.setChartPanelWidth(Math.max(320, Math.min(960, restoredChartWidth)));
    }
    const restoredChartGraphHeight = restored.chartPanelGraphHeight !== undefined ? restored.chartPanelGraphHeight : restoredChartDefinition && restoredChartDefinition.chartHeight;
    if (typeof restoredChartGraphHeight === 'number') {
      stage.setChartPanelGraphHeight(Math.max(180, Math.floor(restoredChartGraphHeight)));
    }
    const restoredChartSortMode = restored.chartPanelSortMode !== undefined ? restored.chartPanelSortMode : restoredChartDefinition && restoredChartDefinition.sortMode;
    if (VALID_CHART_SORT_MODES.has(restoredChartSortMode)) {
      stage.setChartPanelSortMode(restoredChartSortMode);
    }
    const restoredChartInteractionMode = restored.chartPanelInteractionMode !== undefined ? restored.chartPanelInteractionMode : restoredChartDefinition && restoredChartDefinition.interactionMode;
    if (VALID_CHART_INTERACTION_MODES.has(restoredChartInteractionMode)) {
      stage.setChartPanelInteractionMode(restoredChartInteractionMode);
    }
    const restoredChartServerScope = restored.chartPanelServerScope !== undefined ? restored.chartPanelServerScope : restoredChartDefinition && restoredChartDefinition.serverScope;
    if (VALID_CHART_SERVER_SCOPES.has(restoredChartServerScope)) {
      stage.setChartPanelServerScope(restoredChartServerScope);
    }
    if (typeof restored.chartPanelFloating === 'boolean') {
      stage.setChartPanelFloating(restored.chartPanelFloating);
    }
    if (restored.chartPanelFloatingRect && typeof restored.chartPanelFloatingRect === 'object') {
      stage.setChartPanelFloatingRect(clampFloatingChartRect(restored.chartPanelFloatingRect, null));
    }
    if (typeof restored.chartPanelLocked === 'boolean') {
      stage.setChartPanelLocked(restored.chartPanelLocked);
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'chartPanelLockedModel')) {
      stage.setChartPanelLockedModel(restored.chartPanelLockedModel && typeof restored.chartPanelLockedModel === 'object' ? restored.chartPanelLockedModel : null);
    }
    if (Object.prototype.hasOwnProperty.call(restored, 'chartPanelLockedRequest')) {
      stage.setChartPanelLockedRequest(normalizeLockedChartRequest(restored.chartPanelLockedRequest));
    }
    if (Array.isArray(restored.chartCanvasPanes)) {
      stage.setChartCanvasPanes(sanitizeChartCanvasPanes(restored.chartCanvasPanes, stage.initialChartDefinition));
    }
    if (typeof restored.tableCanvasSize === 'number' && Number.isFinite(restored.tableCanvasSize) && restored.tableCanvasSize > 0) {
      stage.setTableCanvasSize(restored.tableCanvasSize);
    }
    if (stage.viewState.viewport && typeof stage.viewState.viewport === 'object') {
      stage.latestViewportRef.current = {
        ...stage.latestViewportRef.current,
        ...stage.viewState.viewport
      };
    }
  }, [stage.viewState, stage.initialChartDefinition, stage.normalizedInitialValConfigs]);
  const chartDefinitionCommands = useRuntimeChartDefinitionCommands({
    activeChartDefinitionId: stage.activeChartDefinitionId,
    currentChartDefinition: stage.currentChartDefinition,
    initialChartDefinition: stage.initialChartDefinition,
    managedChartDefinitions: stage.managedChartDefinitions,
    setActiveChartDefinitionId: stage.setActiveChartDefinitionId,
    setManagedChartDefinitions: stage.setManagedChartDefinitions,
  });
  for (const [key, value] of Object.entries(chartDefinitionCommands)) stage[key] = value;
  const chartPanelSizing = useRuntimeChartPanelSizing({
    chartLayoutRef: stage.chartLayoutRef,
    chartPanelFloating: stage.chartPanelFloating,
    chartPanelOpen: stage.chartPanelOpen,
    isChartPanelResizing: stage.isChartPanelResizing,
    setChartPanelWidth: stage.setChartPanelWidth,
    setIsChartPanelResizing: stage.setIsChartPanelResizing,
    sidebarOpen: stage.sidebarOpen,
  });
  for (const [key, value] of Object.entries(chartPanelSizing)) stage[key] = value;
  const chartCanvasResize = useRuntimeChartCanvasResize({
    chartCanvasLayoutRef: stage.chartCanvasLayoutRef,
    chartCanvasPanes: stage.chartCanvasPanes,
    chartCanvasResizeRef: stage.chartCanvasResizeRef,
    chartLayoutRef: stage.chartLayoutRef,
    setChartCanvasPaneWidthHints: stage.setChartCanvasPaneWidthHints,
    setChartCanvasPanes: stage.setChartCanvasPanes,
    setTableCanvasSize: stage.setTableCanvasSize,
    tableCanvasSize: stage.tableCanvasSize,
  });
  for (const [key, value] of Object.entries(chartCanvasResize)) stage[key] = value;
  const chartCanvasVerticalResize = useRuntimeChartCanvasVerticalResize({
    chartCanvasLayoutRef: stage.chartCanvasLayoutRef,
    chartCanvasPanes: stage.chartCanvasPanes,
    chartCanvasVerticalResizeRef: stage.chartCanvasVerticalResizeRef,
    setChartCanvasPanes: stage.setChartCanvasPanes,
  });
  for (const [key, value] of Object.entries(chartCanvasVerticalResize)) stage[key] = value;
  const cellPresentation = createRuntimeCellPresentation({
    conditionalFormatting: stage.conditionalFormatting,
  });
  for (const [key, value] of Object.entries(cellPresentation)) stage[key] = value;
    // Helper: Excel-style Ctrl+Arrow — jumps to edge of contiguous data block.
    // Rules (mirroring Excel):
    //   - current cell empty  â†’ jump to next non-empty in direction (or edge if all empty)
    //   - current non-empty, next is empty â†’ jump to next non-empty past the gap (or edge)
    //   - current non-empty, next is non-empty â†’ jump to last non-empty before a gap (end of block)
        // next is empty — jump past gap to next non-empty
      // next is non-empty — find end of contiguous block
  const rowSelection = useRuntimeRowSelection({
    getDisplayRows: stage.getDisplayRows,
    lastSelected: stage.lastSelected,
    resolveDisplayRowIndex: stage.resolveDisplayRowIndex,
    setIsRowSelecting: stage.setIsRowSelecting,
    setLastSelected: stage.setLastSelected,
    setRowDragStart: stage.setRowDragStart,
    setSelectedCells: stage.setSelectedCells,
    tableRef: stage.tableRef,
  });
  for (const [key, value] of Object.entries(rowSelection)) stage[key] = value;
  stage.isColExpanded = key => stage.colExpanded[key] !== false;
  stage.fieldInteractionControls = usePivotFieldInteractions();
  ({
    configure: stage.configureFieldInteractions,
    dropLine: stage.dropLine,
    dragOverHeaderId: stage.dragOverHeaderId,
    dragOverSide: stage.dragOverSide,
    handlePinColumn: stage.handlePinColumn,
    toggleAllColumnsPinned: stage.toggleAllColumnsPinned,
    applyFieldZoneMove: stage.applyFieldZoneMove,
    onDragStart: stage.onDragStart,
    onDragOver: stage.onDragOver,
    onDrop: stage.onDrop,
    onHeaderDrop: stage.onHeaderDrop,
    onHeaderDragOver: stage.onHeaderDragOver,
    onHeaderDragEnd: stage.onHeaderDragEnd,
    onHeaderKeyboardMove: stage.onHeaderKeyboardMove,
    onHeaderKeyboardPin: stage.onHeaderKeyboardPin
  } = stage.fieldInteractionControls);
  const runtimeRequestSession = useRuntimeRequestSession({
    dataVersion: stage.dataVersion,
    id: stage.id,
  });
  for (const [key, value] of Object.entries(runtimeRequestSession)) stage[key] = value;
  useEffect(() => {
    if (!stage.clientRuntimeActive || !stage.clientTransportRef.current) return;
    stage.clientTransportRef.current.updateConfig({
      ...stage.normalizedPerformanceConfig,
      tableName: stage.tableName || 'client_data'
    });
    stage.clientTransportRef.current.setData(Array.isArray(stage.inputData) ? stage.inputData : []);
  }, [stage.clientRuntimeActive, stage.inputData, stage.normalizedPerformanceConfig, stage.tableName]);
  // Extract color scale stats sentinel injected by the server, strip it from rows.
  ({
    cleanData: stage.cleanData,
    serverColorScaleStats: stage.serverColorScaleStats
  } = useMemo(() => {
    const sentinelIdx = stage.data.findIndex(r => r && r._path === '__color_scale_stats__');
    if (sentinelIdx === -1) return {
      cleanData: stage.data,
      serverColorScaleStats: null
    };
    const stats = stage.data[sentinelIdx]._colorScaleStats || null;
    const rows = stage.data.filter((_, i) => i !== sentinelIdx);
    return {
      cleanData: rows,
      serverColorScaleStats: stats
    };
  }, [stage.data]));
  stage.customAwareData = useMemo(() => stage.serverSide ? stage.cleanData : applyCustomDimensionsToRows(stage.cleanData, stage.customDimensions), [stage.cleanData, stage.customDimensions, stage.serverSide]);
  stage.clientFilterOptionMap = useMemo(() => stage.serverSide ? {} : buildClientFilterOptionMap(stage.customAwareData, stage.availableFieldsWithCustomDimensions), [stage.availableFieldsWithCustomDimensions, stage.customAwareData, stage.serverSide]);
  stage.serverFilterOptionContextKey = useMemo(() => stage.serverSide ? JSON.stringify({
    table: stage.tableName || '',
    filters: stage.filters || {},
    customDimensions: stage.customDimensions || []
  }) : '', [stage.customDimensions, stage.filters, stage.serverSide, stage.tableName]);
  useEffect(() => {
    if (!stage.serverSide) return;
    stage.pendingServerFilterOptionsRef.current = null;
    stage.setTransportFilterOptionsState({});
    stage.setTransportFilterOptionMetaState({});
  }, [stage.serverFilterOptionContextKey, stage.serverSide]);
  stage.filteredData = useFilteredData(stage.customAwareData, stage.filters, stage.serverSide);
  ({
    cachedColSchema: stage.cachedColSchema,
    colRequestEnd: stage.colRequestEnd,
    colRequestStart: stage.colRequestStart,
    colRequestEndRef: stage.colRequestEndRef,
    colRequestStartRef: stage.colRequestStartRef,
    colSchemaEpochRef: stage.colSchemaEpochRef,
    columnRangeUrgencyToken: stage.columnRangeUrgencyToken,
    handleHorizontalScrollMetrics: stage.handleHorizontalScrollMetrics,
    isHorizontalColumnRequestPending: stage.isHorizontalColumnRequestPending,
    isRequestPending: stage.isRequestPending,
    latestRequestedColumnWindowRef: stage.latestRequestedColumnWindowRef,
    latestRequestedViewportRef: stage.latestRequestedViewportRef,
    markRequestPending: stage.markRequestPending,
    resolvePendingRequest: stage.resolvePendingRequest,
    needsColSchema: stage.needsColSchema,
    needsColSchemaRef: stage.needsColSchemaRef,
    pendingHorizontalColumnCount: stage.pendingHorizontalColumnCount,
    resetVisibleColRange: stage.resetVisibleColRange,
    responseSchemaWindow: stage.responseSchemaWindow,
    resolveStableRequestedColumnWindow: stage.resolveStableRequestedColumnWindow,
    serverSideBlockSize: stage.serverSideBlockSize,
    syncPreciseVisibleColRange: stage.syncPreciseVisibleColRange,
    totalCenterCols: stage.totalCenterCols,
    visibleColRange: stage.visibleColRange
  } = useServerSideViewportController({
    serverSide: stage.serverSide,
    effectiveRowCount: stage.serverSidePinsGrandTotal && stage.rowCount ? Math.max(stage.rowCount - 1, 0) : stage.rowCount,
    responseColumns: stage.responseColumns,
    responseColSchema: stage.responseColSchema,
    dataVersion: stage.dataVersion,
    stateEpoch: stage.stateEpoch,
    structuralInFlight: stage.structuralInFlight,
    structuralPendingVersionRef: stage.structuralPendingVersionRef,
    filteredData: stage.filteredData,
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    schemaFallbackWidth: stage.defaultColumnWidths.schemaFallback,
    requestVersionRef: stage.requestVersionRef,
    latestDataVersionRef: stage.latestDataVersionRef,
    requestUrgentColumnViewportRef: stage.requestUrgentColumnViewportRef,
    coerceTransportNumber,
    mergeSparseColSchema,
    isSparseSchemaRangeLoaded,
    performanceConfig: stage.normalizedPerformanceConfig
  }));
  stage.columnAdvisory = useMemo(() => buildLargeColumnAdvisory({
    serverSide: stage.serverSide,
    totalCenterCols: stage.totalCenterCols,
    colFields: stage.colFields
  }), [stage.colFields, stage.serverSide, stage.totalCenterCols]);
  stage.lastColumnAdvisoryRef = useRef('');
  stage.displayedFormulaColumnOptions = useMemo(() => {
    const groupingIds = new Set(Array.isArray(stage.rowFields) ? stage.rowFields : []);
    const options = [];
    const seen = new Set();
    const pushColumn = column => {
      const columnId = column && typeof column === 'object' ? column.id : column;
      const normalizedId = String(columnId || '').trim();
      if (!normalizedId || normalizedId === '__col_schema' || normalizedId.startsWith('__sparkline__') || groupingIds.has(normalizedId) || seen.has(normalizedId)) {
        return;
      }
      const label = column && typeof column === 'object' ? column.header || column.headerVal || column.accessorKey || normalizedId : normalizedId;
      seen.add(normalizedId);
      options.push({
        id: normalizedId,
        label: String(label || normalizedId)
      });
    };
    if (stage.cachedColSchema && Array.isArray(stage.cachedColSchema.columns)) {
      stage.cachedColSchema.columns.forEach(pushColumn);
    }
    (Array.isArray(stage.responseColumns) ? stage.responseColumns : []).forEach(pushColumn);
    return options;
  }, [stage.cachedColSchema, stage.responseColumns, stage.rowFields]);
  useEffect(() => {
    if (!stage.columnAdvisory) {
      stage.lastColumnAdvisoryRef.current = '';
      return;
    }
    const advisoryKey = `${stage.viewMode}|${stage.colFields.join('|')}|${stage.totalCenterCols}`;
    if (stage.lastColumnAdvisoryRef.current === advisoryKey) return;
    stage.lastColumnAdvisoryRef.current = advisoryKey;
    stage.showNotification(stage.columnAdvisory.notification, stage.columnAdvisory.tone);
  }, [stage.colFields, stage.columnAdvisory, stage.showNotification, stage.totalCenterCols, stage.viewMode]);
  stage.layoutHistorySnapshot = useMemo(() => cloneSerializable({
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    valConfigs: stage.valConfigs,
    filters: stage.filters,
    sorting: stage.sorting,
    expanded: stage.expanded,
    colExpanded: stage.colExpanded,
    columnPinning: stage.columnPinning,
    columnOrder: stage.columnOrder,
    pivotLayoutOrder: stage.pivotLayoutOrder,
    pivotPinnedLayoutIds: stage.pivotPinnedLayoutIds,
    rowPinning: stage.rowPinning,
    showRowTotals: stage.showRowTotals,
    showColTotals: stage.showColTotals,
    showSubtotals: stage.showSubtotals,
    layoutMode: stage.layoutMode,
    grandTotalPinOverride: stage.grandTotalPinOverride
  }, null), [stage.colExpanded, stage.colFields, stage.columnOrder, stage.pivotLayoutOrder, stage.pivotPinnedLayoutIds, stage.columnPinning, stage.expanded, stage.filters, stage.grandTotalPinOverride, stage.rowFields, stage.rowPinning, stage.showColTotals, stage.showRowTotals, stage.showSubtotals, stage.layoutMode, stage.sorting, stage.valConfigs]);
  stage.layoutHistorySnapshotRef = useRef(stage.layoutHistorySnapshot);
  stage.layoutHistorySnapshotKey = useMemo(() => JSON.stringify(stage.layoutHistorySnapshot || {}), [stage.layoutHistorySnapshot]);
  useEffect(() => {
    stage.editorOptionsLoadingRef.current = stage.editorOptionsLoadingState;
  }, [stage.editorOptionsLoadingState]);
  const editCellIdentity = useRuntimeEditCellIdentity({
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    editValueDisplayMode: stage.editValueDisplayMode,
    optimisticCellValuesRef: stage.optimisticCellValuesRef,
    tableDataRef: stage.tableDataRef,
  });
  for (const [key, value] of Object.entries(editCellIdentity)) stage[key] = value;
  const editComparisonState = useRuntimeEditComparisonState({
    comparisonCellCountsRef: stage.comparisonCellCountsRef,
    comparisonCellEpoch: stage.comparisonCellEpoch,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    comparisonValueState: stage.comparisonValueState,
    editedCellEpoch: stage.editedCellEpoch,
    editedCellMarkersRef: stage.editedCellMarkersRef,
    getOptimisticCellKey: stage.getOptimisticCellKey,
    normalizeTrackedEventIds: stage.normalizeTrackedEventIds,
    rowEditSessions: stage.rowEditSessions,
    rowEditSessionsRef: stage.rowEditSessionsRef,
    setComparisonCellEpoch: stage.setComparisonCellEpoch,
    setComparisonValueState: stage.setComparisonValueState,
    setEditedCellEpoch: stage.setEditedCellEpoch,
    setOptimisticCellEpoch: stage.setOptimisticCellEpoch,
    setRowEditSessions: stage.setRowEditSessions,
  });
  for (const [key, value] of Object.entries(editComparisonState)) stage[key] = value;
  const editOwnership = useRuntimeEditOwnership({
    bumpComparisonCellEpoch: stage.bumpComparisonCellEpoch,
    bumpEditedCellEpoch: stage.bumpEditedCellEpoch,
    comparisonCellCountsRef: stage.comparisonCellCountsRef,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    editedCellMarkersRef: stage.editedCellMarkersRef,
    getOptimisticCellKey: stage.getOptimisticCellKey,
    normalizeTrackedEventIds: stage.normalizeTrackedEventIds,
    resolveCurrentCellValue: stage.resolveCurrentCellValue,
    splitOptimisticCellKey: stage.splitOptimisticCellKey,
    syncComparisonValueState: stage.syncComparisonValueState,
  });
  for (const [key, value] of Object.entries(editOwnership)) stage[key] = value;
  const editPresentation = useRuntimeEditPresentation({
    editorOptionsLoadingRef: stage.editorOptionsLoadingRef,
    filterOptions: stage.filterOptions,
    getKey,
    normalizedEditingConfig: stage.normalizedEditingConfig,
    resolveCellEditOwnershipState: stage.resolveCellEditOwnershipState,
    resolveColumnEditSpec,
    resolveEditorOptionsSource,
    resolveVisibleDataRowId: stage.resolveVisibleDataRowId,
    serverSide: stage.serverSide,
    tableDataRef: stage.tableDataRef,
    valConfigs: stage.valConfigs,
    validationRules: stage.validationRules,
  });
  for (const [key, value] of Object.entries(editPresentation)) stage[key] = value;
  const rowEditSessions = useRuntimeRowEditSessions({
    emitEditLifecycleEvent: stage.emitEditLifecycleEvent,
    isEditableAggregationColumn: stage.isEditableAggregationColumn,
    resolveAggregationConfigForColumnId: stage.resolveAggregationConfigForColumnId,
    resolveCurrentCellValue: stage.resolveCurrentCellValue,
    resolveEditorPresentation: stage.resolveEditorPresentation,
    rowEditSessionsRef: stage.rowEditSessionsRef,
    showNotification: stage.showNotification,
    tableRef: stage.tableRef,
    updateRowEditSessions: stage.updateRowEditSessions,
  });
  for (const [key, value] of Object.entries(rowEditSessions)) stage[key] = value;
  const optimisticCellState = useRuntimeOptimisticCellState({
    bumpComparisonCellEpoch: stage.bumpComparisonCellEpoch,
    bumpEditedCellEpoch: stage.bumpEditedCellEpoch,
    bumpOptimisticCellEpoch: stage.bumpOptimisticCellEpoch,
    comparisonCellCountsRef: stage.comparisonCellCountsRef,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    editedCellMarkersRef: stage.editedCellMarkersRef,
    getDirectVisibleChildRowIds: stage.getDirectVisibleChildRowIds,
    getOptimisticCellKey: stage.getOptimisticCellKey,
    optimisticCellRequestsRef: stage.optimisticCellRequestsRef,
    optimisticCellValuesRef: stage.optimisticCellValuesRef,
    resolveAggregationConfigForColumnId: stage.resolveAggregationConfigForColumnId,
    resolveCurrentCellValue: stage.resolveCurrentCellValue,
    syncComparisonValueState: stage.syncComparisonValueState,
  });
  for (const [key, value] of Object.entries(optimisticCellState)) stage[key] = value;
  // Setup is the only stage that constructs this snapshot. Downstream stages
  // receive immutable inputs, so the completed core snapshot cannot be
  // extended later.
  return Object.freeze(stage);
}

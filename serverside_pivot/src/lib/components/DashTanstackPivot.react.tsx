// DashTanstackPivot - Dash public component and runtime composition root.
import React from 'react';
import PropTypes from 'prop-types';
import { PivotRuntimeRoot } from '../runtime/PivotRuntimeRoot';

/**
 * Dash metadata source for the public pivot component.
 *
 * Keep this interface colocated with the exported component: Dash's TypeScript
 * extractor intentionally stages just this file, so imported prop interfaces
 * are not visible to metadata generation.
 */
export interface DashTanstackPivotProps {
  id?: string;
  table?: string;
  pivotTitle?: string;
  data?: Array<object>;
  setProps?: (...args: Array<unknown>) => unknown;
  style?: object;
  serverSide?: boolean;
  rowFields?: Array<unknown>;
  colFields?: Array<unknown>;
  valConfigs?: Array<DashValueConfig>;
  measureAxis?: object;
  filters?: object;
  sorting?: Array<unknown>;
  expanded?: object | boolean;
  immersiveMode?: boolean;
  showRowTotals?: boolean;
  showColTotals?: boolean;
  showSubtotals?: boolean;
  grandTotalPosition?: string;
  runtimeRequest?: object;
  runtimeResponse?: object;
  viewMode?: string;
  detailMode?: string;
  treeConfig?: object;
  detailConfig?: object;
  chartEvent?: object;
  chartDefinitions?: Array<object>;
  chartDefaults?: object;
  chartCanvasPanes?: Array<DashChartCanvasPane>;
  tableCanvasSize?: number;
  performanceConfig?: object;
  chartServerWindow?: object;
  /** @deprecated Editing is unsupported. This legacy compatibility input is ignored. */
  cellUpdate?: object;
  /** @deprecated Editing is unsupported. This legacy compatibility input is ignored. */
  cellUpdates?: Array<object>;
  rowMove?: object;
  drillEndpoint?: string;
  viewState?: object;
  saveViewTrigger?: any;
  savedView?: object;
  conditionalFormatting?: Array<object>;
  headerFormatting?: object | Array<object>;
  validationRules?: object;
  /** @deprecated Editing is unsupported. This legacy compatibility input is ignored. */
  editingConfig?: object;
  /** @deprecated Editing is unsupported. This legacy compatibility input is ignored. */
  editLifecycleEvent?: object;
  /** @deprecated Editing is unsupported. This legacy compatibility input is ignored. */
  editState?: object;
  uiConfig?: object;
  paginationConfig?: object;
  mergedHeaderSeparator?: string;
  columnPinning?: object;
  rowPinning?: object;
  columnPinned?: object;
  rowPinned?: object;
  columnVisibility?: object;
  columnOrder?: Array<string>;
  columnSizing?: object;
  localeText?: object;
  decimalPlaces?: number;
  defaultValueFormat?: string;
  fieldPanelSizes?: object;
  numberGroupSeparator?: string;
  reset?: any;
  persistence?: boolean | string | number;
  persistence_type?: string;
  pinningOptions?: object;
  pinningPresets?: Array<object>;
  sortOptions?: object;
  sortLock?: boolean;
  defaultTheme?: string;
  themeOverrides?: object;
  loadWebFonts?: boolean;
  immersiveExitPosition?: string;
  groupAllowUnbalanced?: boolean;
  unbalancedValues?: Array<unknown>;
  showGrandTotal?: boolean;
  measureGroups?: Array<object>;
  rowHeaderLabel?: string;
  valueScale?: number;
  unitSuffix?: string;
  fitToWidth?: boolean;
  autoSizeStrategy?: string;
  duckdbBundleUrl?: string;
  sortEvent?: object;
  availableFieldList?: Array<string>;
  sparklineFields?: object;
  customDimensions?: Array<object>;
  reportDef?: object;
  savedReports?: Array<object>;
  activeReportId?: string;
  rowSelection?: string | boolean;
  selectedRows?: Array<string>;
}

/** Value-axis configuration accepted by the public Dash API. */
export interface DashValueConfig {
  id?: string;
  field?: string;
  agg?: string;
  weightField?: string;
  windowFn?: string;
  format?: string;
  percentile?: number;
  separator?: string;
  formula?: string;
  label?: string;
  shortLabel?: string;
  headerGroup?: string;
  valueScale?: number;
  unitSuffix?: string;
  formulaRef?: string;
  formulaScope?: string;
  headerStyle?: object;
  headerBg?: string;
  headerColor?: string;
  headerTextColor?: string;
  sparkline?: boolean | object;
}

/** Chart visual settings stored on a chart-canvas pane. */
export interface DashChartSettings {
  chartSubtitle?: string;
  yAxisTitle?: string;
  yAxisMin?: number;
  yAxisMax?: number;
  yAxisMinRight?: number;
  yAxisMaxRight?: number;
  tickCount?: number;
  labelAngle?: number;
  colorPalette?: string;
  valueFormat?: string;
  showDataLabels?: boolean;
  decimalPlaces?: number;
  labelPosition?: string;
  legendPosition?: string;
  markerShape?: string;
  markerSize?: number;
  showGradient?: boolean;
  gradientDirection?: string;
  gradientOpacity?: number;
  showAnimations?: boolean;
  showCalloutLabels?: boolean;
  showTrendline?: boolean;
  trendlineMode?: string;
  trendlinePeriod?: number;
  trendlineColor?: string;
  trendlineWidth?: number;
  seriesSort?: string;
  seriesColors?: object;
  customPaletteColors?: Array<string>;
  lineDashStyles?: object;
  referenceLines?: Array<object>;
}

/** A chart pane, including layout, data-source, and visual settings. */
export interface DashChartCanvasPane {
  id?: string;
  name?: string;
  chartTitle?: string;
  source?: string;
  chartType?: string;
  barLayout?: string;
  axisMode?: string;
  orientation?: string;
  hierarchyLevel?: number | string;
  rowLimit?: number;
  columnLimit?: number;
  width?: number;
  chartHeight?: number;
  sortMode?: string;
  interactionMode?: string;
  serverScope?: string;
  size?: number;
  floating?: boolean;
  settingsPanelWidth?: number;
  chartSettings?: DashChartSettings;
}

/** Editing and edit transactions are not a supported public capability. */
const EDITING_PUBLIC_FEATURE_SUPPORTED = false;

/** Dash public component and typed runtime composition root. */
export default function DashTanstackPivot(props: DashTanstackPivotProps) {
  return <PivotRuntimeRoot {...(props as any)} />;
}

DashTanstackPivot.propTypes = {
  id: PropTypes.string,
  table: PropTypes.string,
  pivotTitle: PropTypes.string,
  data: PropTypes.arrayOf(PropTypes.object),
  setProps: PropTypes.func,
  style: PropTypes.object,
  serverSide: PropTypes.bool,
  rowFields: PropTypes.array,
  colFields: PropTypes.array,
  valConfigs: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    field: PropTypes.string,
    agg: PropTypes.oneOf(['sum', 'avg', 'count', 'min', 'max', 'weighted_avg', 'wavg', 'weighted_mean', 'array_agg', 'formula']),
    weightField: PropTypes.string,
    windowFn: PropTypes.oneOf(['percent_of_row', 'percent_of_col', 'percent_of_grand_total']),
    format: PropTypes.string,
    percentile: PropTypes.number,
    separator: PropTypes.string,
    formula: PropTypes.string,
    label: PropTypes.string,
    shortLabel: PropTypes.string,
    headerGroup: PropTypes.string,
    valueScale: PropTypes.number,
    unitSuffix: PropTypes.string,
    formulaRef: PropTypes.string,
    formulaScope: PropTypes.oneOf(['measures', 'columns']),
    headerStyle: PropTypes.object,
    headerBg: PropTypes.string,
    headerColor: PropTypes.string,
    headerTextColor: PropTypes.string,
    sparkline: PropTypes.oneOfType([PropTypes.bool, PropTypes.object])
  })),
  measureAxis: PropTypes.shape({
    placement: PropTypes.oneOf(['none', 'rows', 'columns']),
    labelField: PropTypes.string,
    valueField: PropTypes.string,
    members: PropTypes.arrayOf(PropTypes.object),
    suppressEmptyMembers: PropTypes.bool,
    suppressZeroMembers: PropTypes.bool,
    totalsPolicy: PropTypes.string
  }),
  filters: PropTypes.object,
  sorting: PropTypes.array,
  expanded: PropTypes.oneOfType([PropTypes.object, PropTypes.bool]),
  immersiveMode: PropTypes.bool,
  showRowTotals: PropTypes.bool,
  showColTotals: PropTypes.bool,
  showSubtotals: PropTypes.bool,
  grandTotalPosition: PropTypes.oneOf(['top', 'bottom']),
  runtimeRequest: PropTypes.object,
  runtimeResponse: PropTypes.object,
  viewMode: PropTypes.oneOf(['pivot', 'report', 'tree', 'table']),
  detailMode: PropTypes.oneOf(['none', 'inline', 'sidepanel', 'drawer']),
  treeConfig: PropTypes.object,
  detailConfig: PropTypes.object,
  chartEvent: PropTypes.object,
  chartDefinitions: PropTypes.arrayOf(PropTypes.object),
  chartDefaults: PropTypes.object,
  chartCanvasPanes: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    name: PropTypes.string,
    chartTitle: PropTypes.string,
    source: PropTypes.oneOf(['pivot', 'selection']),
    chartType: PropTypes.string,
    barLayout: PropTypes.oneOf(['grouped', 'stacked']),
    axisMode: PropTypes.oneOf(['vertical', 'horizontal']),
    orientation: PropTypes.oneOf(['rows', 'columns']),
    hierarchyLevel: PropTypes.oneOfType([PropTypes.number, PropTypes.oneOf(['all'])]),
    rowLimit: PropTypes.number,
    columnLimit: PropTypes.number,
    width: PropTypes.number,
    chartHeight: PropTypes.number,
    sortMode: PropTypes.string,
    interactionMode: PropTypes.string,
    serverScope: PropTypes.oneOf(['viewport', 'root']),
    size: PropTypes.number,
    floating: PropTypes.bool,
    settingsPanelWidth: PropTypes.number,
    chartSettings: PropTypes.shape({
      chartSubtitle: PropTypes.string,
      yAxisTitle: PropTypes.string,
      yAxisMin: PropTypes.number,
      yAxisMax: PropTypes.number,
      yAxisMinRight: PropTypes.number,
      yAxisMaxRight: PropTypes.number,
      tickCount: PropTypes.number,
      labelAngle: PropTypes.number,
      colorPalette: PropTypes.string,
      valueFormat: PropTypes.string,
      showDataLabels: PropTypes.bool,
      decimalPlaces: PropTypes.number,
      labelPosition: PropTypes.string,
      legendPosition: PropTypes.string,
      markerShape: PropTypes.string,
      markerSize: PropTypes.number,
      showGradient: PropTypes.bool,
      gradientDirection: PropTypes.string,
      gradientOpacity: PropTypes.number,
      showAnimations: PropTypes.bool,
      showCalloutLabels: PropTypes.bool,
      showTrendline: PropTypes.bool,
      trendlineMode: PropTypes.string,
      trendlinePeriod: PropTypes.number,
      trendlineColor: PropTypes.string,
      trendlineWidth: PropTypes.number,
      seriesSort: PropTypes.string,
      seriesColors: PropTypes.object,
      customPaletteColors: PropTypes.arrayOf(PropTypes.string),
      lineDashStyles: PropTypes.object,
      referenceLines: PropTypes.arrayOf(PropTypes.object)
    })
  })),
  tableCanvasSize: PropTypes.number,
  performanceConfig: PropTypes.shape({
    cacheBlockSize: PropTypes.number,
    maxBlocksInCache: PropTypes.number,
    blockLoadDebounceMs: PropTypes.number,
    rowOverscan: PropTypes.number,
    columnOverscan: PropTypes.number,
    prefetchColumns: PropTypes.number,
    executionMode: PropTypes.oneOf(['auto', 'client', 'server']),
    clientEngine: PropTypes.oneOf(['auto', 'duckdb-wasm', 'memory']),
    duckdbWasmThreshold: PropTypes.number,
    clientRowThreshold: PropTypes.number,
    dataUrl: PropTypes.string,
    duckdbWasmCdnUrl: PropTypes.string,
    legacyClientTable: PropTypes.bool
  }),
  chartServerWindow: PropTypes.shape({
    enabled: PropTypes.bool,
    rows: PropTypes.number,
    columns: PropTypes.number,
    scope: PropTypes.oneOf(['viewport', 'root'])
  }),
  cellUpdate: PropTypes.object,
  cellUpdates: PropTypes.arrayOf(PropTypes.object),
  rowMove: PropTypes.object,
  drillEndpoint: PropTypes.string,
  viewState: PropTypes.object,
  saveViewTrigger: PropTypes.any,
  savedView: PropTypes.object,
  conditionalFormatting: PropTypes.arrayOf(PropTypes.object),
  headerFormatting: PropTypes.oneOfType([PropTypes.object, PropTypes.arrayOf(PropTypes.object)]),
  validationRules: PropTypes.object,
  editingConfig: PropTypes.object,
  editLifecycleEvent: PropTypes.object,
  editState: PropTypes.object,
  uiConfig: PropTypes.object,
  /**
   * Client-side pagination configuration: { enabled: true, pageSize: 50 }.
   * Only applies when serverSide is false.
   */
  paginationConfig: PropTypes.object,
  mergedHeaderSeparator: PropTypes.string,
  columnPinning: PropTypes.shape({
    left: PropTypes.arrayOf(PropTypes.string),
    right: PropTypes.arrayOf(PropTypes.string)
  }),
  rowPinning: PropTypes.shape({
    top: PropTypes.arrayOf(PropTypes.string),
    bottom: PropTypes.arrayOf(PropTypes.string)
  }),
  columnPinned: PropTypes.object,
  rowPinned: PropTypes.object,
  columnVisibility: PropTypes.object,
  columnOrder: PropTypes.arrayOf(PropTypes.string),
  columnSizing: PropTypes.object,
  localeText: PropTypes.object,
  decimalPlaces: PropTypes.number,
  defaultValueFormat: PropTypes.string,
  fieldPanelSizes: PropTypes.object,
  numberGroupSeparator: PropTypes.oneOf(['comma', 'space', 'thin_space', 'apostrophe', 'none']),
  reset: PropTypes.any,
  persistence: PropTypes.oneOfType([PropTypes.bool, PropTypes.string, PropTypes.number]),
  persistence_type: PropTypes.oneOf(['local', 'session', 'memory']),
  pinningOptions: PropTypes.shape({
    maxPinnedLeft: PropTypes.number,
    maxPinnedRight: PropTypes.number,
    suppressMovable: PropTypes.bool,
    lockPinned: PropTypes.bool
  }),
  pinningPresets: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string,
    config: PropTypes.object
  })),
  sortOptions: PropTypes.shape({
    naturalSort: PropTypes.bool,
    caseSensitive: PropTypes.bool,
    columnOptions: PropTypes.object
  }),
  sortLock: PropTypes.bool,
  defaultTheme: PropTypes.string,
  themeOverrides: PropTypes.object,
  loadWebFonts: PropTypes.bool,
  immersiveExitPosition: PropTypes.oneOf(['top-right', 'bottom-right', 'hidden']),
  groupAllowUnbalanced: PropTypes.bool,
  unbalancedValues: PropTypes.array,
  showGrandTotal: PropTypes.bool,
  measureGroups: PropTypes.arrayOf(PropTypes.object),
  rowHeaderLabel: PropTypes.string,
  valueScale: PropTypes.number,
  unitSuffix: PropTypes.string,
  fitToWidth: PropTypes.bool,
  autoSizeStrategy: PropTypes.string,
  duckdbBundleUrl: PropTypes.string,
  sortEvent: PropTypes.object,
  availableFieldList: PropTypes.arrayOf(PropTypes.string),
  sparklineFields: PropTypes.object,
  customDimensions: PropTypes.arrayOf(PropTypes.object),
  reportDef: PropTypes.object,
  savedReports: PropTypes.arrayOf(PropTypes.object),
  activeReportId: PropTypes.string,
  rowSelection: PropTypes.oneOfType([PropTypes.oneOf(['single', 'multiple']), PropTypes.bool]),
  selectedRows: PropTypes.arrayOf(PropTypes.string)
};

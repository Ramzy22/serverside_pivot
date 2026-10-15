import type React from 'react';

import type { JsonObject, RuntimeCommandKind, RuntimeRequestV1, RuntimeResponseV1 } from './runtimeContract';

/** Public Dash props accepted by the runtime root. */
export interface PivotRuntimeProps {
    id?: string;
    table?: string;
    pivotTitle?: string;
    data?: readonly PivotRuntimeRow[];
    setProps?: (nextProps: Partial<PivotRuntimeProps>) => void;
    style?: React.CSSProperties;
    serverSide?: boolean;
    rowFields?: readonly string[];
    colFields?: readonly string[];
    valConfigs?: readonly PivotValueConfig[];
    measureAxis?: PivotObject;
    filters?: PivotObject;
    sorting?: readonly PivotObject[];
    expanded?: boolean | PivotObject;
    immersiveMode?: boolean;
    showRowTotals?: boolean;
    showColTotals?: boolean;
    showSubtotals?: boolean;
    grandTotalPosition?: 'top' | 'bottom';
    runtimeRequest?: PivotObject;
    runtimeResponse?: RuntimeResponseV1 | PivotObject;
    viewMode?: 'pivot' | 'report' | 'tree' | 'table';
    detailMode?: 'none' | 'inline' | 'sidepanel' | 'drawer';
    treeConfig?: PivotObject;
    detailConfig?: PivotObject;
    chartEvent?: PivotObject;
    chartDefinitions?: readonly PivotObject[];
    chartDefaults?: PivotObject;
    chartCanvasPanes?: readonly PivotObject[];
    tableCanvasSize?: number;
    performanceConfig?: PivotObject;
    chartServerWindow?: PivotObject;
    rowMove?: PivotObject;
    drillEndpoint?: string;
    viewState?: PivotObject;
    saveViewTrigger?: unknown;
    savedView?: PivotObject;
    conditionalFormatting?: readonly PivotObject[];
    headerFormatting?: PivotObject | readonly PivotObject[];
    validationRules?: PivotObject;
    uiConfig?: PivotObject;
    paginationConfig?: PivotObject;
    mergedHeaderSeparator?: string;
    columnPinning?: PivotObject;
    rowPinning?: PivotObject;
    columnPinned?: PivotObject;
    rowPinned?: PivotObject;
    columnVisibility?: PivotObject;
    columnOrder?: readonly string[];
    columnSizing?: PivotObject;
    localeText?: PivotObject;
    decimalPlaces?: number;
    defaultValueFormat?: string;
    fieldPanelSizes?: PivotObject;
    numberGroupSeparator?: 'comma' | 'space' | 'thin_space' | 'apostrophe' | 'none';
    reset?: unknown;
    persistence?: boolean | string | number;
    persistence_type?: 'local' | 'session' | 'memory';
    pinningOptions?: PivotObject;
    pinningPresets?: readonly PivotObject[];
    sortOptions?: PivotObject;
    sortLock?: boolean;
    defaultTheme?: string;
    sortEvent?: PivotObject;
    availableFieldList?: readonly string[];
    sparklineFields?: PivotObject;
    customDimensions?: readonly PivotObject[];
    reportDef?: PivotObject;
    savedReports?: readonly PivotObject[];
    activeReportId?: string;
    rowSelection?: 'single' | 'multiple' | boolean;
    selectedRows?: readonly string[];
}

export interface PivotRuntimeRow {
    readonly id?: string | number;
    readonly _id?: string | number;
    readonly [field: string]: unknown;
}

export interface PivotValueConfig {
    readonly field?: string;
    readonly agg?: string;
    readonly weightField?: string;
    readonly windowFn?: string;
    readonly format?: string;
    readonly formula?: string;
    readonly label?: string;
    readonly [field: string]: unknown;
}

/** JSON-compatible object passed through the public Dash API. */
export interface PivotObject {
    readonly [field: string]: unknown;
}

export interface PivotRuntimeConfiguration {
    readonly componentId: string | null;
    readonly tableName: string | null;
    readonly serverSide: boolean;
    readonly viewMode: string;
    readonly detailMode: string;
    readonly persistence: boolean | string | number | undefined;
    readonly props: PivotRuntimeProps;
}

export interface CoreRuntimeState {
    readonly rowFields: readonly string[];
    readonly colFields: readonly string[];
    readonly valConfigs: readonly PivotValueConfig[];
    readonly filters: PivotObject;
    readonly sorting: readonly PivotObject[];
    readonly expanded: boolean | PivotObject;
    readonly columnOrder: readonly string[];
    readonly columnVisibility: PivotObject;
    readonly columnSizing: PivotObject;
    readonly columnPinning: PivotObject;
    readonly rowPinning: PivotObject;
}

export interface CoreRuntimeRefs {
    readonly table: React.MutableRefObject<unknown>;
    readonly setProps: React.MutableRefObject<PivotRuntimeProps['setProps']>;
    readonly clientInstance: React.MutableRefObject<string | null>;
    readonly session: React.MutableRefObject<string | null>;
}

export interface CorePersistencePort {
    readonly save: (state: PivotObject) => void;
    readonly load: () => PivotObject | null;
}

/** Immutable core domain: normalized configuration, React state, stable refs. */
export interface CoreRuntimeDomain {
    readonly configuration: PivotRuntimeConfiguration;
    readonly state: CoreRuntimeState;
    readonly refs: CoreRuntimeRefs;
    readonly persistence: CorePersistencePort;
}

export interface TransportDispatchPort {
    readonly emit: (kind: RuntimeCommandKind, payload: JsonObject) => unknown;
}

/** Host-owned controlled state. Dash implements this with ``setProps``. */
export interface HostStatePort {
    readonly publish: (nextState: PivotObject) => void;
}

export interface TransportRuntimeDomain {
    readonly request: {
        readonly current: PivotObject | undefined;
        readonly emit: TransportDispatchPort['emit'];
        readonly normalize: (request: PivotObject) => RuntimeRequestV1 | null;
    };
    readonly response: {
        readonly current: RuntimeResponseV1 | PivotObject | undefined;
        readonly pending: boolean;
    };
    readonly profiling: {
        readonly enabled: boolean;
        readonly recordQueued: (request: PivotObject) => void;
        readonly recordRequestStart: (request: PivotObject) => void;
        readonly recordResponse: (response: PivotObject) => void;
    };
    readonly port: TransportDispatchPort;
    readonly host: HostStatePort;
}

export type RuntimeCommand = (...args: unknown[]) => unknown;

/** Renderer callbacks are intentionally opaque at the domain boundary. */
export type WorkspaceRenderer = (...args: unknown[]) => React.ReactNode;
export type WorkspaceCollection = readonly PivotObject[];
export type WorkspaceRecord = PivotObject;

export interface CommandRuntimeDomain {
    readonly fields: {
        readonly move: RuntimeCommand;
        readonly expandAll: RuntimeCommand;
        readonly pinColumn: RuntimeCommand;
        readonly pinRow: RuntimeCommand;
    };
    readonly layout: {
        readonly undo: RuntimeCommand;
        readonly redo: RuntimeCommand;
        readonly setRowFields: RuntimeCommand;
        readonly setColFields: RuntimeCommand;
        readonly setValues: RuntimeCommand;
    };
    readonly editing: {
        readonly dispatchCellEdit: RuntimeCommand;
        readonly revertCell: RuntimeCommand;
        readonly revertAll: RuntimeCommand;
    };
    readonly selection: {
        readonly applyValueFormat: RuntimeCommand;
        readonly copy: RuntimeCommand;
    };
    readonly export: {
        readonly exportPivot: RuntimeCommand;
        readonly cancel: RuntimeCommand;
    };
    readonly charts: {
        readonly addPane: RuntimeCommand;
        readonly updatePane: RuntimeCommand;
        readonly removePane: RuntimeCommand;
        readonly openSelectionChart: RuntimeCommand;
    };
}

/** Event-time viewport operations. They deliberately contain no state or render data. */
export interface ViewportNavigationPort {
    readonly scrollToRow: (index: number) => void;
    readonly scrollToColumn: (index: number) => void;
    readonly refresh: () => void;
}

/**
 * Imperative delivery callbacks are constructed by viewport presentation but
 * consumed by the transport response lifecycle.  They deliberately carry no
 * renderer state: a runtime response can only ask to copy text or start a
 * browser download.
 */
export interface ViewportDeliveryPort {
    readonly copy: RuntimeCommand;
    readonly download: RuntimeCommand;
}

export interface ViewportRuntimeDomain {
    readonly table: {
        readonly instance: unknown;
        readonly renderCell: RuntimeCommand;
        readonly renderHeaderCell: RuntimeCommand;
    };
    readonly rows: {
        readonly count: number;
        readonly height: number;
        readonly visible: readonly unknown[];
        readonly virtual: readonly unknown[];
    };
    readonly columns: {
        readonly visible: readonly unknown[];
        readonly left: readonly unknown[];
        readonly center: readonly unknown[];
        readonly right: readonly unknown[];
        readonly totalCenter: number | null;
    };
    readonly virtualization: {
        readonly parent: React.RefObject<HTMLElement>;
        readonly navigation: ViewportNavigationPort;
    };
    readonly delivery: ViewportDeliveryPort;
}

/** Renderer-ready presentation model. Internal JavaScript renderers remain untyped consumers. */
export interface WorkspacePresentation {
    readonly COPY_ROW_LIMIT: number;
    readonly EXPORT_ROW_LIMIT: number;
    readonly activateChartCategory: RuntimeCommand;
    readonly activeChartDefinitionId: string | null;
    readonly activeColumnSkeletonCount: number;
    readonly activeDetailDisplayMode: string;
    readonly activeFilterCol: string | null;
    readonly activeReportId: string | null;
    readonly activeRowVirtualizer: object;
    readonly afterWidth: number;
    readonly announcement: string;
    readonly applyFieldZoneMove: RuntimeCommand;
    readonly applySelectionValueFormat: RuntimeCommand;
    readonly autoSizeIncludesHeaderNext: boolean;
    readonly availableFieldsWithCustomDimensions: WorkspaceCollection;
    readonly beforeWidth: number;
    readonly bodyRowsTopOffset: number;
    readonly canApplySelectionValueFormat: boolean;
    readonly cancelActiveExportRequest: RuntimeCommand;
    readonly cellFormatRules: WorkspaceCollection;
    readonly centerColIndexMap: Readonly<Record<string, number>>;
    readonly centerCols: WorkspaceCollection;
    readonly centerHeaderGroups: WorkspaceCollection;
    readonly chartCanvasLayoutRef: React.MutableRefObject<HTMLElement | null>;
    readonly chartCanvasPaneModels: WorkspaceCollection;
    readonly chartCanvasPaneWidthHints: WorkspaceRecord;
    readonly chartCanvasPanes: WorkspaceCollection;
    readonly chartLayoutRef: React.MutableRefObject<HTMLElement | null>;
    readonly chartPaneDataById: WorkspaceRecord;
    readonly clientFilterOptionMap: WorkspaceRecord;
    readonly closeFilterPopover: RuntimeCommand;
    readonly colFields: readonly string[];
    readonly colSearch: string;
    readonly colTypeFilter: string;
    readonly colorPalette: string;
    readonly colorScaleMode: string;
    readonly columnDecimalOverrides: WorkspaceRecord;
    readonly columnFormatOverrides: WorkspaceRecord;
    readonly columnGroupSeparatorOverrides: WorkspaceRecord;
    readonly columnPinning: WorkspaceRecord;
    readonly columnSkeletonWidth: number;
    readonly columnVisibility: WorkspaceRecord;
    readonly comparisonValueState: WorkspaceRecord;
    readonly contextMenu: WorkspaceRecord | null;
    readonly customAwareData: WorkspaceCollection;
    readonly customDimensions: WorkspaceCollection;
    readonly dataBarsColumns: readonly string[];
    readonly decimalPlaces: number;
    readonly defaultValueFormat: string;
    readonly detailConfig: WorkspaceRecord | null;
    readonly detailSurface: React.ReactNode | null;
    readonly dispatchExportPart: RuntimeCommand;
    readonly displayedFormulaColumnOptions: WorkspaceCollection;
    readonly dockedChartCanvasPanesByPosition: WorkspaceRecord;
    readonly dropLine: WorkspaceRecord | null;
    readonly editPanelCells: WorkspaceCollection;
    readonly editPanelEnabled: boolean;
    readonly editPanelOpen: boolean;
    readonly editValueDisplayMode: string;
    readonly effectiveBottomRows: WorkspaceCollection;
    readonly effectiveCenterRows: WorkspaceCollection;
    readonly effectiveTopRows: WorkspaceCollection;
    readonly exportPivot: RuntimeCommand;
    readonly exportProgress: WorkspaceRecord | null;
    readonly fieldPanelSizes: WorkspaceRecord;
    readonly filterAnchorEl: HTMLElement | null;
    readonly filterOptionMeta: WorkspaceRecord;
    readonly filterOptions: WorkspaceRecord;
    readonly filters: WorkspaceRecord;
    readonly fixedSparklineValueKeys: readonly string[];
    readonly floatingChartCanvasPanes: WorkspaceCollection;
    readonly fontFamily: string;
    readonly fontSize: number;
    readonly formulaErrors: WorkspaceRecord;
    readonly getRow: RuntimeCommand;
    readonly grandTotalPosition: 'top' | 'bottom';
    readonly handleAddChartCanvasPane: RuntimeCommand;
    readonly handleAutoSizeToolbarClick: RuntimeCommand;
    readonly handleCancelPreviewChartPane: RuntimeCommand;
    readonly handleCancelPropagation: RuntimeCommand;
    readonly handleConfirmPreviewChartPane: RuntimeCommand;
    readonly handleConfirmPropagation: RuntimeCommand;
    readonly handleDetailClose: RuntimeCommand;
    readonly handleDetailFilter: RuntimeCommand;
    readonly handleDetailPageChange: RuntimeCommand;
    readonly handleDetailSort: RuntimeCommand;
    readonly handleExpandAllRows: RuntimeCommand;
    readonly handleFilterClick: RuntimeCommand;
    readonly handleHeaderFilter: RuntimeCommand;
    readonly handleKeyDown: RuntimeCommand;
    readonly handlePinColumn: RuntimeCommand;
    readonly handlePropagationMethodChange: RuntimeCommand;
    readonly handleReapplyPropagation: RuntimeCommand;
    readonly handleRedo: RuntimeCommand;
    readonly handleRevertAll: RuntimeCommand;
    readonly handleRevertCell: RuntimeCommand;
    readonly handleRevertSelected: RuntimeCommand;
    readonly handleSaveView: RuntimeCommand;
    readonly handleSetPivotMode: RuntimeCommand;
    readonly handleStartChartCanvasResize: RuntimeCommand;
    readonly handleStartChartCanvasVerticalResize: RuntimeCommand;
    readonly handleTransposePivot: RuntimeCommand;
    readonly handleUndo: RuntimeCommand;
    readonly hasComparedValues: boolean;
    readonly id: string | null;
    readonly immersiveMode: boolean;
    readonly immersiveModeHovering: boolean;
    readonly isCopying: boolean;
    readonly isExporting: boolean;
    readonly isRequestPending: boolean;
    readonly runtimeDataError: {
        readonly retrying: boolean;
        readonly failedRequest: {
            readonly requestId: string | null;
            readonly windowSeq: number | null;
            readonly stateEpoch: number | null;
            readonly clientInstance: string | null;
        };
    } | null;
    readonly retryRuntimeData: RuntimeCommand;
    readonly layoutMode: string;
    readonly leftCols: WorkspaceCollection;
    readonly leftHeaderGroups: WorkspaceCollection;
    readonly loadingCssVars: React.CSSProperties;
    readonly measureAxis: WorkspaceRecord;
    readonly notification: WorkspaceRecord | null;
    readonly numberGroupSeparator: string;
    readonly onCreateSelectionChart: RuntimeCommand;
    readonly onDragOver: RuntimeCommand;
    readonly onDragStart: RuntimeCommand;
    readonly onDrop: RuntimeCommand;
    readonly onHeaderDragEnd: RuntimeCommand;
    readonly paginationConfig: WorkspaceRecord;
    readonly paginationState: WorkspaceRecord;
    readonly parentRef: React.RefObject<HTMLElement>;
    readonly partModal: WorkspaceRecord | null;
    readonly pendingPropagationUpdates: WorkspaceCollection;
    readonly pendingRowTransitions: WorkspaceRecord;
    readonly pinningPresets: WorkspaceCollection;
    readonly pivotMode: boolean;
    readonly pivotTitle: string;
    readonly previewChartPaneId: string | null;
    readonly propagationLogRef: React.MutableRefObject<WorkspaceCollection>;
    readonly propagationMethodUI: string;
    readonly renderCell: WorkspaceRenderer;
    readonly renderChartCanvasPane: WorkspaceRenderer;
    readonly renderChartWorkspaceModal: WorkspaceRenderer;
    readonly renderFloatingChartCanvasPane: WorkspaceRenderer;
    readonly renderHeaderCell: WorkspaceRenderer;
    readonly renderVirtualColumnCell: WorkspaceRenderer;
    readonly reportDef: WorkspaceRecord | null;
    readonly reportEditorSelection: WorkspaceRecord | null;
    readonly requestFilterOptions: RuntimeCommand;
    readonly resolveCurrentCellValue: RuntimeCommand;
    readonly resolveDisplayedCellValue: RuntimeCommand;
    readonly rightCols: WorkspaceCollection;
    readonly rightHeaderGroups: WorkspaceCollection;
    readonly rowCount: number;
    readonly rowFields: readonly string[];
    readonly rowHeight: number;
    readonly rowModelLookup: WorkspaceRecord;
    readonly rows: WorkspaceCollection;
    readonly savedReports: WorkspaceCollection;
    readonly selectedCells: WorkspaceRecord;
    readonly selectedCols: readonly string[];
    readonly selectionValueFormat: string;
    readonly serverSide: boolean;
    readonly serverSidePinsGrandTotal: boolean;
    readonly setActiveReportId: RuntimeCommand;
    readonly setCellFormatRules: RuntimeCommand;
    readonly setColFieldsWithHistory: RuntimeCommand;
    readonly setColSearch: RuntimeCommand;
    readonly setColTypeFilter: RuntimeCommand;
    readonly setColorPalette: RuntimeCommand;
    readonly setColorScaleMode: RuntimeCommand;
    readonly setColumnDecimalOverrides: RuntimeCommand;
    readonly setColumnFormatOverrides: RuntimeCommand;
    readonly setColumnGroupSeparatorOverrides: RuntimeCommand;
    readonly setColumnPinningWithHistory: RuntimeCommand;
    readonly setColumnVisibility: RuntimeCommand;
    readonly setContextMenu: RuntimeCommand;
    readonly setCustomDimensions: RuntimeCommand;
    readonly setDataBarsColumns: RuntimeCommand;
    readonly setDecimalPlaces: RuntimeCommand;
    readonly setDefaultValueFormat: RuntimeCommand;
    readonly setDetailSurface: RuntimeCommand;
    readonly setEditPanelOpen: RuntimeCommand;
    readonly setEditValueDisplayMode: RuntimeCommand;
    readonly setFieldPanelSizes: RuntimeCommand;
    readonly setFilterAnchorEl: RuntimeCommand;
    readonly setFilters: RuntimeCommand;
    readonly setFontFamily: RuntimeCommand;
    readonly setFontSize: RuntimeCommand;
    readonly setImmersiveMode: RuntimeCommand;
    readonly setImmersiveModeHovering: RuntimeCommand;
    readonly setLayoutMode: RuntimeCommand;
    readonly setMeasureAxis: RuntimeCommand;
    readonly setNotification: RuntimeCommand;
    readonly setNumberGroupSeparator: RuntimeCommand;
    readonly setPaginationState: RuntimeCommand;
    readonly setPartModal: RuntimeCommand;
    readonly setReportDef: RuntimeCommand;
    readonly setRowFieldsWithHistory: RuntimeCommand;
    readonly setSavedReports: RuntimeCommand;
    readonly setSelectedCols: RuntimeCommand;
    readonly setShowColTotals: RuntimeCommand;
    readonly setShowFloatingFilters: RuntimeCommand;
    readonly setShowReportConfigColumn: RuntimeCommand;
    readonly setShowRowNumbers: RuntimeCommand;
    readonly setShowRowTotals: RuntimeCommand;
    readonly setShowSubtotals: RuntimeCommand;
    readonly setSidebarOpen: RuntimeCommand;
    readonly setSidebarTab: RuntimeCommand;
    readonly setSidebarWidth: RuntimeCommand;
    readonly setSpacingMode: RuntimeCommand;
    readonly setSparklineDataModal: RuntimeCommand;
    readonly setStickyHeaders: RuntimeCommand;
    readonly setThemeName: RuntimeCommand;
    readonly setThemeOverrides: RuntimeCommand;
    readonly setValConfigsWithHistory: RuntimeCommand;
    readonly setZoomLevel: RuntimeCommand;
    readonly showColTotals: boolean;
    readonly showColumnLoadingSkeletons: boolean;
    readonly showFloatingFilters: boolean;
    readonly showNotification: RuntimeCommand;
    readonly showReportConfigColumn: boolean;
    readonly showRowNumbers: boolean;
    readonly showRowTotals: boolean;
    readonly showSubtotals: boolean;
    readonly sidebarOpen: boolean;
    readonly sidebarTab: string;
    readonly sidebarWidth: number;
    readonly spacingLabels: WorkspaceRecord;
    readonly spacingMode: string;
    readonly sparklineDataModal: WorkspaceRecord | null;
    readonly sparklineFields: WorkspaceRecord;
    readonly srOnly: React.CSSProperties;
    readonly statusAccessoryActions: WorkspaceCollection;
    readonly statusAccessoryModel: WorkspaceRecord | null;
    readonly statusRowCount: number;
    readonly stickyHeaderHeight: number;
    readonly stickyHeaders: boolean;
    readonly style: React.CSSProperties;
    readonly styles: WorkspaceRecord;
    readonly table: object;
    readonly tableCanvasSize: number;
    readonly theme: WorkspaceRecord;
    readonly themeName: string;
    readonly themeOverrides: WorkspaceRecord;
    readonly toggleAllColumnsPinned: RuntimeCommand;
    readonly totalCenterCols: number | null;
    readonly totalLayoutWidth: number;
    readonly transactionHistoryState: WorkspaceRecord;
    readonly treeConfig: WorkspaceRecord | null;
    readonly uiConfig: WorkspaceRecord;
    readonly updateChartCanvasPane: RuntimeCommand;
    readonly valConfigs: readonly PivotValueConfig[];
    readonly viewMode: PivotRuntimeProps['viewMode'];
    readonly virtualCenterCols: WorkspaceCollection;
    readonly virtualRows: WorkspaceCollection;
    readonly visibleLeafColumns: WorkspaceCollection;
    readonly zoomLevel: number;
}

/** Narrow workspace-to-transport port for server chart request scheduling. */
export interface WorkspaceChartRequestPort {
    readonly abortGeneration: number;
    readonly activeRequest: React.MutableRefObject<PivotObject | null>;
    readonly candidates: readonly PivotObject[];
    readonly requestSequence: React.MutableRefObject<number>;
    readonly clientInstance: React.MutableRefObject<string | null>;
    readonly completedSignatures: React.MutableRefObject<PivotObject>;
    readonly needsColumnSchema: boolean;
    readonly requestVersion: React.MutableRefObject<number>;
    readonly serverSide: boolean;
    readonly session: React.MutableRefObject<string | null>;
    readonly setProps: React.MutableRefObject<unknown>;
    readonly stateEpoch: number;
    readonly structuralInFlight: boolean;
    readonly totalCenterColumns: number | null;
}

export interface WorkspaceRuntimeDomain {
    readonly presentation: WorkspacePresentation;
    readonly toolbar: WorkspacePresentation;
    readonly sidebar: WorkspacePresentation;
    readonly canvas: WorkspacePresentation;
    readonly charts: {
        readonly panes: readonly unknown[];
        readonly renderPane: RuntimeCommand;
        readonly renderWorkspaceModal: RuntimeCommand;
    };
    readonly detail: {
        readonly surface: unknown;
        readonly close: RuntimeCommand;
    };
    readonly chartRequest: WorkspaceChartRequestPort;
}

export interface PivotRuntimeDomains {
    readonly core: CoreRuntimeDomain;
    readonly transport: TransportRuntimeDomain;
    readonly commands: CommandRuntimeDomain;
    readonly viewport: ViewportRuntimeDomain;
    readonly workspace: WorkspaceRuntimeDomain;
}

export interface WorkspaceViewModel {
    readonly presentation: WorkspacePresentation;
    readonly controls: {
        readonly toolbar: WorkspacePresentation;
        readonly sidebar: WorkspacePresentation;
    };
    readonly canvas: WorkspacePresentation;
}

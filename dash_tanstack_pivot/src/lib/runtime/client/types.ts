import type { PivotTransport } from '../httpTransport';
import type { RuntimeRequestV1 } from '../runtimeContract';

export type ExecutionMode = 'auto' | 'client' | 'server';
export type ClientEngineKind = 'auto' | 'duckdb-wasm' | 'memory';
export type PivotViewMode = 'pivot' | 'tree' | 'report' | 'table';
export type PivotLayoutMode = 'hierarchy' | 'outline' | 'tabular';

export interface ClientRuntimeConfig {
    executionMode?: ExecutionMode;
    clientEngine?: ClientEngineKind;
    duckdbWasmThreshold?: number;
    clientRowThreshold?: number;
    dataUrl?: string;
    duckdbWasmCdnUrl?: string;
    duckdbWasmInstance?: unknown;
    legacyClientTable?: boolean;
    tableName?: string;
}

export interface ClientEngineTelemetry {
    activeMode: 'client' | 'server';
    activeEngine: 'duckdb-wasm' | 'memory' | 'server';
    wasmReady: boolean;
    lastQuerySql: string;
    lastExecutionMs: number;
    rowCount: number;
    networkRoundtrips: number;
    lastUpdatedAt: number;
}

export interface NormalizedValConfig {
    field: string;
    agg: string;
    id: string;
    label: string;
    weightField?: string;
    windowFn?: string;
    formula?: string;
    formulaScope?: 'measures' | 'columns';
    percentile?: number;
    separator?: string;
    format?: string;
}

export interface NormalizedCustomDimension {
    id: string;
    field: string;
    name: string;
    defaultBucket: string;
    rules: Array<{
        label: string;
        op: string;
        value: unknown;
        value2?: unknown;
    }>;
}

export interface NormalizedFilterClause {
    type: string;
    value?: unknown;
    values?: unknown[];
    caseSensitive?: boolean;
}

export interface NormalizedFieldFilter {
    field: string;
    operator: 'AND' | 'OR';
    clauses: NormalizedFilterClause[];
}

export interface NormalizedSortRule {
    id: string;
    desc: boolean;
    sortType?: string;
    sortKeyField?: string;
    absoluteSort?: boolean;
}

export interface LogicalPivotPlan {
    kind: 'data' | 'filter_options' | 'chart' | 'detail' | 'drill' | 'update' | 'export' | 'transaction';
    table: string;
    viewMode: PivotViewMode;
    layoutMode: PivotLayoutMode;
    showSubtotalFooters: boolean;
    includeGrandTotal: boolean;
    showRowTotals: boolean;
    showColTotals: boolean;
    grandTotalPosition: 'top' | 'bottom';
    rowFields: string[];
    colFields: string[];
    valConfigs: NormalizedValConfig[];
    customDimensions: NormalizedCustomDimension[];
    filters: NormalizedFieldFilter[];
    sorting: NormalizedSortRule[];
    sortOptions: {
        columnSortBy?: string;
        columnSortOrder?: string;
        nulls?: string;
        naturalSort?: boolean;
        fieldOptions?: { [key: string]: { sortType?: string; sortKeyField?: string; absoluteSort?: boolean } };
    };
    expandedAll: boolean;
    expandedPaths: string[][];
    measureAxis: {
        placement: 'columns' | 'rows';
        position?: number;
        measureColId?: string;
        valueColId?: string;
        label?: string;
        valueLabel?: string;
    };
    treeConfig: { [key: string]: unknown } | null;
    reportDef: { [key: string]: unknown } | null;
    detailConfig: { [key: string]: unknown } | null;
    layoutManifest: Array<{ id?: string; layoutId?: string }> | null;
    start: number;
    end: number;
    colStart: number | null;
    colEnd: number | null;
    needsColSchema: boolean;
    version: number;
    windowSeq: number;
    stateEpoch: number;
    abortGeneration: number;
    sessionId: string;
    clientInstance: string;
    intent: string;
    rawRequest: RuntimeRequestV1;
    sql: {
        baseFilteredSql: string;
        groupingSql: string;
        colValuesSql: string;
        grandTotalSql: string;
        filterOptionsSql: string;
        drillSql: string;
    };
}

export interface AggregatedCellMetrics {
    sum: number;
    count: number;
    numericCount: number;
    min: number | null;
    max: number | null;
    weightedNumer: number;
    weightedDenom: number;
    values: unknown[];
    firstValue: unknown;
    lastValue: unknown;
}

export interface ClientExecutedDataset {
    filteredRows: Array<{ [key: string]: unknown }>;
    allRows: Array<{ [key: string]: unknown }>;
    colCombinations: string[];
    engineUsed: 'duckdb-wasm' | 'memory';
    executionMs: number;
    lastSql: string;
}

export interface DrillThroughQuery {
    table?: string;
    rowPath: string;
    rowFields: string[];
    colField?: string;
    colVal?: string;
    filters?: { [key: string]: unknown };
    customDimensions?: unknown[];
    page?: number;
    pageSize?: number;
    sortCol?: string;
    sortDir?: 'asc' | 'desc';
    filterText?: string;
}

export interface DrillThroughResult {
    rows: Array<{ [key: string]: unknown }>;
    totalRows: number;
    page: number;
    pageSize: number;
    columns: string[];
}

export interface ClientPivotTransportInterface extends PivotTransport {
    setData: (rows: Array<{ [key: string]: unknown }>) => void;
    updateConfig: (config: Partial<ClientRuntimeConfig>) => void;
    getTelemetry: () => ClientEngineTelemetry;
    queryDrillThrough: (query: DrillThroughQuery) => Promise<DrillThroughResult>;
}

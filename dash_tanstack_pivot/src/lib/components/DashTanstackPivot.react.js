// DashTanstackPivot - Enterprise Grade Pivot Table
import React, { useMemo, useState, useRef, useEffect, useLayoutEffect, useCallback } from 'react';
import PropTypes from 'prop-types';
import {
    useReactTable,
    getCoreRowModel,
    getExpandedRowModel,
    getGroupedRowModel,
} from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import * as XLSX from 'xlsx';
import { saveAs } from 'file-saver';
import { themes, getStyles, isDarkTheme, gridDimensionTokens } from '../utils/styles';
import Icons from './Icons';
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
import Notification from './Notification';
import useStickyStyles from '../hooks/useStickyStyles';
import { useServerSideRowModel } from '../hooks/useServerSideRowModel';
import { useColumnVirtualizer } from '../hooks/useColumnVirtualizer';
import { formatValue, formatDisplayLabel, getAllLeafIdsFromColumn, isGroupColumn } from '../utils/helpers';
import ContextMenu from './Table/ContextMenu';
import { PivotAppBar } from './PivotAppBar';
import { SidebarPanel } from './Sidebar/SidebarPanel';
import DrillThroughModal from './Table/DrillThroughModal';
import {
    buildPivotChartModel,
    buildComboPivotChartModel,
    buildComboSelectionChartModel,
    buildSelectionChartModel,
    canStackBarLayout,
    PivotChartModal,
    PivotChartPanel,
} from './Charts/PivotCharts';
import { useColumnDefs } from '../hooks/useColumnDefs';
import { useRenderHelpers } from '../hooks/useRenderHelpers';
import { PivotTableBody } from './Table/PivotTableBody';
import PivotErrorBoundary from './PivotErrorBoundary';
import { usePersistence } from '../hooks/usePersistence';
import { useFilteredData } from '../hooks/useFilteredData';

const DEFAULT_CHART_PANEL_ROW_LIMIT = 50;
const DEFAULT_CHART_PANEL_COLUMN_LIMIT = 10;
const DEFAULT_CHART_GRAPH_HEIGHT = 320;
const DEFAULT_FLOATING_CHART_PANEL_HEIGHT = 520;
const MIN_CHART_PANEL_WIDTH = 280;
const MAX_CHART_PANEL_WIDTH = 960;
const MIN_FLOATING_CHART_PANEL_HEIGHT = 280;
const MIN_TABLE_PANEL_WIDTH = 320;
const MIN_CHART_CANVAS_PANE_WIDTH = 320;
const DEFAULT_TABLE_CANVAS_SIZE = 1.4;
const TABLE_OVERLAY_CHART_PANE_ID = '__table_overlay_chart__';
const VALID_CHART_TYPES = new Set(['bar', 'line', 'area', 'combo', 'icicle', 'sunburst', 'sankey']);
const VALID_CHART_SORT_MODES = new Set(['natural', 'value_desc', 'value_asc', 'label_asc', 'label_desc']);
const VALID_CHART_INTERACTION_MODES = new Set(['focus', 'filter', 'event']);
const VALID_CHART_SERVER_SCOPES = new Set(['viewport', 'root']);
const getPreferredChartOrientation = (columnFields) => (
    Array.isArray(columnFields) && columnFields.length > 0 ? 'columns' : 'rows'
);

const sanitizeChartDefinitionName = (value, fallback = 'Chart') => {
    const text = typeof value === 'string' ? value.trim() : '';
    return text || fallback;
};

const createChartDefinitionId = (prefix = 'chart') => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const cloneSerializable = (value, fallback = null) => {
    if (value === undefined) return fallback;
    try {
        return JSON.parse(JSON.stringify(value));
    } catch (error) {
        return fallback;
    }
};

const clampFloatingChartRect = (rect, containerRect) => {
    const source = rect && typeof rect === 'object' ? rect : {};
    const rawWidth = Number(source.width);
    const rawHeight = Number(source.height);
    const rawLeft = Number(source.left);
    const rawTop = Number(source.top);
    const width = Number.isFinite(rawWidth) ? Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(rawWidth)) : 430;
    const height = Number.isFinite(rawHeight) ? Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.floor(rawHeight)) : DEFAULT_FLOATING_CHART_PANEL_HEIGHT;
    const left = Number.isFinite(rawLeft) ? Math.floor(rawLeft) : 24;
    const top = Number.isFinite(rawTop) ? Math.floor(rawTop) : 24;

    if (!containerRect || !Number.isFinite(containerRect.width) || !Number.isFinite(containerRect.height)) {
        return { left: Math.max(0, left), top: Math.max(0, top), width, height };
    }

    const maxWidth = Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(containerRect.width) - 12);
    const maxHeight = Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.floor(containerRect.height) - 12);
    const safeWidth = Math.max(MIN_CHART_PANEL_WIDTH, Math.min(maxWidth, width));
    const safeHeight = Math.max(MIN_FLOATING_CHART_PANEL_HEIGHT, Math.min(maxHeight, height));
    const maxLeft = Math.max(0, Math.floor(containerRect.width) - safeWidth);
    const maxTop = Math.max(0, Math.floor(containerRect.height) - safeHeight);

    return {
        left: Math.max(0, Math.min(maxLeft, left)),
        top: Math.max(0, Math.min(maxTop, top)),
        width: safeWidth,
        height: safeHeight,
    };
};

const normalizeChartDefinition = (value, fallback = {}) => {
    const source = value && typeof value === 'object' ? value : {};
    const fallbackSource = fallback && typeof fallback === 'object' ? fallback : {};
    const chartType = VALID_CHART_TYPES.has(source.chartType) ? source.chartType : (VALID_CHART_TYPES.has(fallbackSource.chartType) ? fallbackSource.chartType : 'bar');
    const barLayout = source.barLayout === 'stacked' || source.barLayout === 'grouped'
        ? source.barLayout
        : (fallbackSource.barLayout === 'stacked' ? 'stacked' : 'grouped');
    const axisMode = source.axisMode === 'horizontal' || source.axisMode === 'vertical'
        ? source.axisMode
        : (fallbackSource.axisMode === 'horizontal' ? 'horizontal' : 'vertical');
    const orientation = source.orientation === 'columns' || source.orientation === 'rows'
        ? source.orientation
        : (fallbackSource.orientation === 'columns' ? 'columns' : 'rows');
    const interactionMode = VALID_CHART_INTERACTION_MODES.has(source.interactionMode)
        ? source.interactionMode
        : (VALID_CHART_INTERACTION_MODES.has(fallbackSource.interactionMode) ? fallbackSource.interactionMode : 'focus');
    const sortMode = VALID_CHART_SORT_MODES.has(source.sortMode)
        ? source.sortMode
        : (VALID_CHART_SORT_MODES.has(fallbackSource.sortMode) ? fallbackSource.sortMode : 'natural');
    const serverScope = VALID_CHART_SERVER_SCOPES.has(source.serverScope)
        ? source.serverScope
        : (VALID_CHART_SERVER_SCOPES.has(fallbackSource.serverScope) ? fallbackSource.serverScope : 'viewport');
    const hierarchyLevel = source.hierarchyLevel === 'all' || (typeof source.hierarchyLevel === 'number' && source.hierarchyLevel >= 1)
        ? source.hierarchyLevel
        : (fallbackSource.hierarchyLevel === 'all' || (typeof fallbackSource.hierarchyLevel === 'number' && fallbackSource.hierarchyLevel >= 1)
            ? fallbackSource.hierarchyLevel
            : 'all');
    const rowLimit = Number.isFinite(Number(source.rowLimit))
        ? Math.max(1, Math.floor(Number(source.rowLimit)))
        : (Number.isFinite(Number(fallbackSource.rowLimit)) ? Math.max(1, Math.floor(Number(fallbackSource.rowLimit))) : DEFAULT_CHART_PANEL_ROW_LIMIT);
    const columnLimit = Number.isFinite(Number(source.columnLimit))
        ? Math.max(1, Math.floor(Number(source.columnLimit)))
        : (Number.isFinite(Number(fallbackSource.columnLimit)) ? Math.max(1, Math.floor(Number(fallbackSource.columnLimit))) : DEFAULT_CHART_PANEL_COLUMN_LIMIT);
    const width = Number.isFinite(Number(source.width))
        ? Math.max(MIN_CHART_PANEL_WIDTH, Math.min(MAX_CHART_PANEL_WIDTH, Math.floor(Number(source.width))))
        : (Number.isFinite(Number(fallbackSource.width))
            ? Math.max(MIN_CHART_PANEL_WIDTH, Math.min(MAX_CHART_PANEL_WIDTH, Math.floor(Number(fallbackSource.width))))
            : 430);
    const chartHeight = Number.isFinite(Number(source.chartHeight))
        ? Math.max(180, Math.floor(Number(source.chartHeight)))
        : (Number.isFinite(Number(fallbackSource.chartHeight))
            ? Math.max(180, Math.floor(Number(fallbackSource.chartHeight)))
            : DEFAULT_CHART_GRAPH_HEIGHT);

    return {
        id: sanitizeChartDefinitionName(source.id || fallbackSource.id || createChartDefinitionId('chart'), 'chart'),
        name: sanitizeChartDefinitionName(source.name || fallbackSource.name || 'Live Chart', 'Live Chart'),
        chartTitle: sanitizeChartDefinitionName(source.chartTitle || fallbackSource.chartTitle || source.name || fallbackSource.name || 'Chart', 'Chart'),
        source: source.source === 'selection' || source.source === 'pivot'
            ? source.source
            : (fallbackSource.source === 'selection' ? 'selection' : 'pivot'),
        chartType,
        barLayout,
        axisMode,
        orientation,
        hierarchyLevel,
        rowLimit,
        columnLimit,
        width,
        chartHeight,
        sortMode,
        interactionMode,
        serverScope,
        chartLayers: cloneSerializable(
            Array.isArray(source.chartLayers)
                ? source.chartLayers
                : (Array.isArray(fallbackSource.chartLayers) ? fallbackSource.chartLayers : []),
            []
        ),
    };
};

const sanitizeChartDefinitions = (definitions, fallbackDefinition) => {
    const sourceDefinitions = Array.isArray(definitions) ? definitions : [];
    const normalized = sourceDefinitions
        .filter((item) => item && typeof item === 'object')
        .map((item, index) => normalizeChartDefinition(item, {
            ...fallbackDefinition,
            id: item.id || `chart-${index + 1}`,
            name: item.name || `Chart ${index + 1}`,
        }));

    if (normalized.length > 0) return normalized;
    return [normalizeChartDefinition(fallbackDefinition, fallbackDefinition)];
};

const serializeChartColumn = (column) => {
    if (!column || typeof column !== 'object') return null;
    return {
        id: column.id || null,
        headerVal: column.headerVal !== undefined ? column.headerVal : null,
        columnDef: column.columnDef
            ? {
                header: typeof column.columnDef.header === 'string' ? column.columnDef.header : null,
                headerVal: column.columnDef.headerVal !== undefined ? column.columnDef.headerVal : null,
            }
            : null,
        parent: column.parent ? serializeChartColumn(column.parent) : null,
    };
};

const serializeChartColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => serializeChartColumn(column)).filter(Boolean)
        : []
);

const getRequestedChartSeriesColumnIds = (chartType, chartLayers) => (
    chartType === 'combo'
        ? Array.from(new Set(
            (Array.isArray(chartLayers) ? chartLayers : [])
                .map((layer) => (layer && typeof layer.columnId === 'string' ? layer.columnId.trim() : ''))
                .filter(Boolean)
        ))
        : []
);

const normalizeChartResponseColumn = (column, fallbackId = null) => {
    const source = column && typeof column === 'object' ? column : {};
    const columnId = source.id || (typeof column === 'string' ? column : fallbackId) || null;
    if (!columnId) return null;
    const headerVal = source.headerVal !== undefined
        ? source.headerVal
        : (source.header !== undefined ? source.header : null);
    const header = source.columnDef && typeof source.columnDef === 'object'
        ? (typeof source.columnDef.header === 'string'
            ? source.columnDef.header
            : (typeof source.header === 'string' ? source.header : (headerVal !== null && headerVal !== undefined ? String(headerVal) : String(columnId))))
        : (typeof source.header === 'string' ? source.header : (headerVal !== null && headerVal !== undefined ? String(headerVal) : String(columnId)));
    return {
        id: columnId,
        headerVal,
        columnDef: {
            header,
            headerVal: source.columnDef && source.columnDef.headerVal !== undefined
                ? source.columnDef.headerVal
                : headerVal,
        },
        parent: source.parent ? normalizeChartResponseColumn(source.parent) : null,
    };
};

const normalizeChartResponseColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => normalizeChartResponseColumn(column)).filter(Boolean)
        : []
);

const buildChartColumnsFromSchema = (colSchema) => (
    colSchema && Array.isArray(colSchema.columns)
        ? colSchema.columns
            .map((column) => normalizeChartResponseColumn({
                id: column && column.id ? column.id : null,
                header: column && typeof column.header === 'string' ? column.header : null,
                headerVal: column && column.headerVal !== undefined ? column.headerVal : (column && column.id ? column.id : null),
            }))
            .filter(Boolean)
        : []
);

const resolveChartModelColumns = (chartDataEntry, fallbackColumns = []) => {
    const responseColumns = normalizeChartResponseColumns(chartDataEntry && chartDataEntry.columns);
    return responseColumns.length > 0 ? responseColumns : serializeChartColumns(fallbackColumns);
};

const resolveChartAvailableColumns = (chartDataEntry, fallbackColumns = []) => {
    const responseColumns = normalizeChartResponseColumns(chartDataEntry && chartDataEntry.columns);
    const schemaColumns = buildChartColumnsFromSchema(chartDataEntry && chartDataEntry.colSchema);
    const fallbackSerialized = serializeChartColumns(fallbackColumns);
    if (schemaColumns.length === 0) {
        return responseColumns.length > 0 ? responseColumns : fallbackSerialized;
    }
    const responseById = new Map(responseColumns.map((column) => [column.id, column]));
    const fallbackById = new Map(fallbackSerialized.map((column) => [column.id, column]));
    const merged = schemaColumns.map((column) => responseById.get(column.id) || fallbackById.get(column.id) || column);
    const knownIds = new Set(merged.map((column) => column.id));
    responseColumns.forEach((column) => {
        if (!knownIds.has(column.id)) {
            merged.push(column);
            knownIds.add(column.id);
        }
    });
    fallbackSerialized.forEach((column) => {
        if (!knownIds.has(column.id)) {
            merged.push(column);
            knownIds.add(column.id);
        }
    });
    return merged;
};

const normalizeLockedChartRequest = (value) => {
    if (!value || typeof value !== 'object') return null;
    return {
        request: value.request && typeof value.request === 'object' ? cloneSerializable(value.request, null) : null,
        stateOverride: value.stateOverride && typeof value.stateOverride === 'object' ? cloneSerializable(value.stateOverride, null) : null,
        visibleColumns: Array.isArray(value.visibleColumns) ? cloneSerializable(value.visibleColumns, []) : [],
        requestSignature: typeof value.requestSignature === 'string' ? value.requestSignature : null,
    };
};

const normalizeChartCanvasPane = (value, fallbackDefinition, index = 0) => {
    const source = value && typeof value === 'object' ? value : {};
    const normalizedDefinition = normalizeChartDefinition(source, {
        ...fallbackDefinition,
        id: source.id || `chart-pane-${index + 1}`,
        name: source.name || `Chart Pane ${index + 1}`,
    });
    const numericSize = Number(source.size);
    return {
        ...normalizedDefinition,
        size: Number.isFinite(numericSize) && numericSize > 0 ? numericSize : 1,
        floating: Boolean(source.floating),
        floatingRect: clampFloatingChartRect(source.floatingRect, null),
        locked: Boolean(source.locked),
        lockedModel: cloneSerializable(source.lockedModel, null),
        lockedRequest: normalizeLockedChartRequest(source.lockedRequest),
        cinemaMode: Boolean(source.cinemaMode),
    };
};

const sanitizeChartCanvasPanes = (panes, fallbackDefinition) => (
    Array.isArray(panes)
        ? panes
            .filter((pane) => pane && typeof pane === 'object')
            .map((pane, index) => normalizeChartCanvasPane(pane, fallbackDefinition, index))
        : []
);

const getChartPanelWidthBounds = (layoutWidth) => {
    if (!Number.isFinite(layoutWidth) || layoutWidth <= 0) {
        return { minWidth: MIN_CHART_PANEL_WIDTH, maxWidth: MAX_CHART_PANEL_WIDTH };
    }

    const minWidth = Math.min(320, Math.max(MIN_CHART_PANEL_WIDTH, Math.floor(layoutWidth * 0.3)));
    const maxWidth = Math.min(
        MAX_CHART_PANEL_WIDTH,
        Math.max(minWidth, Math.floor(layoutWidth - Math.min(MIN_TABLE_PANEL_WIDTH, layoutWidth * 0.45)))
    );

    return { minWidth, maxWidth };
};

const normalizeChartServerWindowConfig = (value) => {
    if (!value || typeof value !== 'object') {
        return { enabled: false, rows: null, columns: null, scope: 'viewport' };
    }

    const normalizedRows = Number(value.rows);
    const normalizedColumns = Number(value.columns);
    const rows = Number.isFinite(normalizedRows) && normalizedRows > 0 ? Math.max(1, Math.floor(normalizedRows)) : null;
    const columns = Number.isFinite(normalizedColumns) && normalizedColumns > 0 ? Math.max(1, Math.floor(normalizedColumns)) : null;
    const enabled = value.enabled === undefined
        ? (rows !== null || columns !== null)
        : Boolean(value.enabled);

    return {
        enabled,
        rows: enabled ? rows : null,
        columns: enabled ? columns : null,
        scope: VALID_CHART_SERVER_SCOPES.has(value.scope) ? value.scope : 'viewport',
    };
};

const getOrCreateSessionId = (componentId = 'pivot-grid') => {
    if (typeof window === 'undefined') {
        return `${componentId}-server-session`;
    }

    const storageKey = `${componentId}-client-session-id`;
    try {
        const fromStorage = window.sessionStorage.getItem(storageKey);
        if (fromStorage) return fromStorage;
    } catch (e) {
        // no-op: storage may be blocked in some browser privacy modes
    }

    let generated = null;
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        generated = window.crypto.randomUUID();
    } else {
        generated = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    }

    try {
        window.sessionStorage.setItem(storageKey, generated);
    } catch (e) {
        // no-op
    }

    return generated;
};

const createClientInstanceId = (componentId = 'pivot-grid') => {
    if (typeof window !== 'undefined' && window.crypto && typeof window.crypto.randomUUID === 'function') {
        return `${componentId}-${window.crypto.randomUUID()}`;
    }
    return `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
};

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

const getStickyHeaderHeight = (headerGroupCount, rowHeight, showFloatingFilters) =>
    (headerGroupCount * rowHeight) + (showFloatingFilters ? rowHeight : 0);

const GRAND_TOTAL_ROW_ID = '__grand_total__';
const MISSING_PERSISTED_VALUE = Symbol('missing-persisted-value');

const normalizeRowPinningState = (value) => ({
    top: Array.isArray(value && value.top) ? value.top : [],
    bottom: Array.isArray(value && value.bottom) ? value.bottom : [],
});

const getPinnedSideForRow = (rowPinning, rowId) => {
    const normalized = normalizeRowPinningState(rowPinning);
    if (normalized.top.includes(rowId)) return 'top';
    if (normalized.bottom.includes(rowId)) return 'bottom';
    return null;
};

const sanitizeGrandTotalPinOverride = (value) => (
    value === 'top' || value === 'bottom' || value === false ? value : null
);

const resolveGrandTotalPinState = (showColTotals, grandTotalPosition, grandTotalPinOverride) => {
    if (!showColTotals) return false;
    if (grandTotalPinOverride === false || grandTotalPinOverride === 'top' || grandTotalPinOverride === 'bottom') {
        return grandTotalPinOverride;
    }
    return grandTotalPosition === 'bottom' ? 'bottom' : 'top';
};

const applyRowPinning = (rowPinning, rowId, pinState) => {
    const normalized = normalizeRowPinningState(rowPinning);
    const next = {
        ...normalized,
        top: normalized.top.filter(id => id !== rowId),
        bottom: normalized.bottom.filter(id => id !== rowId),
    };
    if (pinState === 'top') next.top.push(rowId);
    if (pinState === 'bottom') next.bottom.push(rowId);
    return next;
};

export default function DashTanstackPivot(props) {
    const { 
        id, 
        data = [], 
        style = {}, 
        setProps, 
        serverSide = false, 
        rowCount,
        rowFields: initialRowFields = [],
        colFields: initialColFields = [],
        valConfigs: initialValConfigs = [],
        filters: initialFilters = {},
        sorting: initialSorting = [],
        expanded: initialExpanded = {},
        cinemaMode: initialCinemaMode = false,
        showRowTotals: initialShowRowTotals = true,
        showColTotals: initialShowColTotals = true,
        grandTotalPosition = 'top',
        filterOptions = {},
        conditionalFormatting = [],
        validationRules = {},
        columnPinning: initialColumnPinning = { left: ['hierarchy'], right: [] },
        rowPinning: initialRowPinning = { top: [], bottom: [] },
        persistence,
        persistence_type = 'local',
        pinningOptions = {},
        pinningPresets = [],
        sortOptions = {},
        columnVisibility: initialColumnVisibility = {},
        reset,
        sortLock = false,
        defaultTheme = 'flash',
        availableFieldList,
        table: tableName,
        dataOffset = 0,
        dataVersion = 0,
        chartData = null,
        chartServerWindow = null,
        chartDefinitions = null,
        chartDefaults = null,
        chartCanvasPanes: externalChartCanvasPanes = null,
        tableCanvasSize: externalTableCanvasSize = null,
        drillEndpoint = '/api/drill-through',
        viewState = null,
        saveViewTrigger = null,
    } = props;

    const normalizedInitialChartServerWindow = normalizeChartServerWindowConfig(chartServerWindow);
    const initialChartDefinition = useMemo(() => normalizeChartDefinition(chartDefaults, {
        id: 'live-chart-panel',
        name: 'Live Chart',
        source: 'pivot',
        chartType: 'bar',
        barLayout: 'grouped',
        axisMode: 'vertical',
        orientation: getPreferredChartOrientation(initialColFields),
        hierarchyLevel: 'all',
        rowLimit: DEFAULT_CHART_PANEL_ROW_LIMIT,
        columnLimit: DEFAULT_CHART_PANEL_COLUMN_LIMIT,
        width: 430,
        sortMode: 'natural',
        interactionMode: 'focus',
        serverScope: normalizedInitialChartServerWindow.scope,
    }), [chartDefaults, initialColFields, normalizedInitialChartServerWindow.scope]);
    const initialChartDefinitions = useMemo(
        () => sanitizeChartDefinitions(chartDefinitions, initialChartDefinition),
        [chartDefinitions, initialChartDefinition]
    );

    // Register sortOptions with Dash's callback State store on mount.
    // Without this, State(id, "sortOptions") returns None in Python callbacks
    // because Dash only stores prop values that were explicitly pushed via setProps.
    useEffect(() => {
        if (setProps && sortOptions && Object.keys(sortOptions).length > 0) {
            setProps({ sortOptions });
        }
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    // --- Persistence Helper ---
    const { load: loadPersistedState, save: savePersistedState } = usePersistence(id, persistence, persistence_type);

        const [notification, setNotification] = useState(null);

        useEffect(() => {
            if (notification) {
                const timer = setTimeout(() => setNotification(null), 3000);
                return () => clearTimeout(timer);
            }
        }, [notification]);

        const showNotification = React.useCallback((msg, type='info') => {
            setNotification({ message: msg, type });
        }, []);

        // --- State ---

        const availableFields = useMemo(() => {
            if (availableFieldList && availableFieldList.length > 0) return availableFieldList;
            if (serverSide && props.columns) return props.columns.filter(c => c.id !== '__col_schema').map(c => c.id || c);

            return data && data.length ? Object.keys(data[0]) : [];

        }, [data, props.columns, serverSide, availableFieldList]);

        // Theme State
        const [themeName, setThemeName] = useState(() => (themes[defaultTheme] ? defaultTheme : 'flash'));
        const [themeOverrides, setThemeOverrides] = useState({});
        const theme = useMemo(() => ({ ...themes[themeName], ...themeOverrides }), [themeName, themeOverrides]);
        const styles = useMemo(() => getStyles(theme), [theme]);
        const loadingCssVars = useMemo(() => {
            if (isDarkTheme(theme)) {
                return {
                    '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(57,88,132,0.62) 0%, rgba(88,126,178,0.9) 48%, rgba(57,88,132,0.62) 100%)',
                    '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(50,77,116,0.58) 0%, rgba(82,118,170,0.86) 45%, rgba(50,77,116,0.58) 100%)',
                    '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(35,53,82,0.88) 0%, rgba(49,74,112,0.95) 50%, rgba(35,53,82,0.88) 100%)',
                    '--pivot-loading-border': 'rgba(130, 165, 215, 0.45)',
                    '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(120,170,240,0) 0%, rgba(140,190,255,0.92) 45%, rgba(120,170,240,0) 100%)',
                    '--pivot-loading-shimmer-duration': '2.8s',
                };
            }
            return {
                '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(233,243,255,0.92) 0%, rgba(193,220,255,0.98) 48%, rgba(233,243,255,0.92) 100%)',
                '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%)',
                '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%)',
                '--pivot-loading-border': 'rgba(153, 187, 238, 0.5)',
                '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(75,139,245,0) 0%, rgba(75,139,245,0.9) 45%, rgba(75,139,245,0) 100%)',
                '--pivot-loading-shimmer-duration': '2.8s',
            };
        }, [theme]);

        const [rowFields, setRowFields] = useState(initialRowFields);
        const [colFields, setColFields] = useState(initialColFields);
        const [valConfigs, setValConfigs] = useState(initialValConfigs);
        const [filters, setFilters] = useState(initialFilters);
        const [sorting, setSorting] = useState(initialSorting);
        const [expanded, setExpanded] = useState(initialExpanded);
        const [columnPinning, setColumnPinning] = useState(() => loadPersistedState('columnPinning', initialColumnPinning));
        const [rowPinning, setRowPinning] = useState(() => loadPersistedState('rowPinning', initialRowPinning));
        const [grandTotalPinOverride, setGrandTotalPinOverride] = useState(() => {
            const persistedOverride = loadPersistedState('grandTotalPinOverride', MISSING_PERSISTED_VALUE);
            if (persistedOverride !== MISSING_PERSISTED_VALUE) {
                return sanitizeGrandTotalPinOverride(persistedOverride);
            }
            const persistedRowPinning = loadPersistedState('rowPinning', initialRowPinning);
            return getPinnedSideForRow(persistedRowPinning, GRAND_TOTAL_ROW_ID);
        });
        const [layoutMode, setLayoutMode] = useState('hierarchy'); // hierarchy, tabular
        const [columnVisibility, setColumnVisibility] = useState(() => loadPersistedState('columnVisibility', initialColumnVisibility));
        const [columnSizing, setColumnSizing] = useState(() => loadPersistedState('columnSizing', {}));
        const [pivotColumnSorting, setPivotColumnSorting] = useState({});
        const [announcement, setAnnouncement] = useState("");
        const [drillModal, setDrillModal] = useState(null);
        // drillModal shape: { loading, rows, page, totalRows, path, sortCol, sortDir, filterText } | null
        const [chartPanelOpen, setChartPanelOpen] = useState(false);
        const [chartPanelSource, setChartPanelSource] = useState(initialChartDefinition.source);
        const [chartPanelType, setChartPanelType] = useState(initialChartDefinition.chartType);
        const [chartPanelBarLayout, setChartPanelBarLayout] = useState(initialChartDefinition.barLayout);
        const [chartPanelAxisMode, setChartPanelAxisMode] = useState(initialChartDefinition.axisMode);
        const [chartPanelOrientation, setChartPanelOrientation] = useState(initialChartDefinition.orientation);
        const [chartPanelHierarchyLevel, setChartPanelHierarchyLevel] = useState(initialChartDefinition.hierarchyLevel);
        const [chartPanelTitle, setChartPanelTitle] = useState(initialChartDefinition.chartTitle);
        const [chartPanelLayers, setChartPanelLayers] = useState(Array.isArray(initialChartDefinition.chartLayers) ? initialChartDefinition.chartLayers : []);
        const [chartPanelRowLimit, setChartPanelRowLimit] = useState(initialChartDefinition.rowLimit);
        const [chartPanelColumnLimit, setChartPanelColumnLimit] = useState(initialChartDefinition.columnLimit);
        const [chartPanelWidth, setChartPanelWidth] = useState(initialChartDefinition.width);
        const [chartPanelGraphHeight, setChartPanelGraphHeight] = useState(initialChartDefinition.chartHeight);
        const [chartPanelFloating, setChartPanelFloating] = useState(() => Boolean(loadPersistedState('chartPanelFloatingLayout', {}).floating));
        const [chartPanelFloatingRect, setChartPanelFloatingRect] = useState(() => {
            const persistedLayout = loadPersistedState('chartPanelFloatingLayout', {});
            return clampFloatingChartRect(persistedLayout.rect, null);
        });
        const [chartPanelSortMode, setChartPanelSortMode] = useState(initialChartDefinition.sortMode);
        const [chartPanelInteractionMode, setChartPanelInteractionMode] = useState(initialChartDefinition.interactionMode);
        const [chartPanelServerScope, setChartPanelServerScope] = useState(initialChartDefinition.serverScope);
        const [chartPanelLocked, setChartPanelLocked] = useState(() => Boolean(loadPersistedState('chartPanelLockState', {}).locked));
        const [chartPanelLockedModel, setChartPanelLockedModel] = useState(() => loadPersistedState('chartPanelLockState', {}).lockedModel || null);
        const [chartPanelLockedRequest, setChartPanelLockedRequest] = useState(() => normalizeLockedChartRequest(loadPersistedState('chartPanelLockState', {}).lockedRequest));
        const [isChartPanelResizing, setIsChartPanelResizing] = useState(false);
        const [chartModal, setChartModal] = useState(null);
        const chartPanelOrientationAutoRef = useRef(true);
        const chartLayoutRef = useRef(null);
        const chartCanvasLayoutRef = useRef(null);
        const chartCanvasResizeRef = useRef(null);
        const chartPanelFloatingDragRef = useRef(null);
        const chartPanelFloatingResizeRef = useRef(null);
        const chartCanvasFloatingDragRef = useRef(null);
        const chartCanvasFloatingResizeRef = useRef(null);
        const chartRequestSeqRef = useRef(0);
        const [managedChartDefinitions, setManagedChartDefinitions] = useState(initialChartDefinitions);
        const [activeChartDefinitionId, setActiveChartDefinitionId] = useState(() => initialChartDefinitions[0] ? initialChartDefinitions[0].id : 'live-chart-panel');
        const [chartCanvasPanes, setChartCanvasPanes] = useState(() => (
            Array.isArray(externalChartCanvasPanes)
                ? sanitizeChartCanvasPanes(externalChartCanvasPanes, initialChartDefinition)
                : sanitizeChartCanvasPanes(loadPersistedState('chartCanvasPanes', []), initialChartDefinition)
        ));
        const [tableCanvasSize, setTableCanvasSize] = useState(() => {
            const externalSize = Number(externalTableCanvasSize);
            if (Number.isFinite(externalSize) && externalSize > 0) {
                return externalSize;
            }
            const persistedSize = Number(loadPersistedState('tableCanvasSize', DEFAULT_TABLE_CANVAS_SIZE));
            return Number.isFinite(persistedSize) && persistedSize > 0 ? persistedSize : DEFAULT_TABLE_CANVAS_SIZE;
        });
        const [chartPaneDataById, setChartPaneDataById] = useState({});
        const activeChartRequestRef = useRef(null);
        const completedChartRequestSignaturesRef = useRef({});
        const applyingChartDefinitionRef = useRef(false);
        const lastChartDefinitionsPropRef = useRef(null);
        const lastChartCanvasPanesPropRef = useRef(null);
        const lastTableCanvasSizePropRef = useRef(null);
        const tableRef = useRef(null);
        const displayRowsRef = useRef([]);
        const displayRowIndexRef = useRef(new Map());
        const pinnedDisplayMetaRef = useRef({ topCount: 0, centerCount: 0 });
        const activeRowVirtualizerRef = useRef(null);
        const pinnedRowCacheRef = useRef(new Map());
        const columnVirtualizerRef = useRef(null);
        const pinnedColumnMetaRef = useRef({ leftCount: 0, centerCount: 0, rightCount: 0 });
        const previousRowFieldsRef = useRef(initialRowFields);
        const pendingServerFilterOptionsRef = useRef(null);
        const normalizedChartServerWindow = useMemo(
            () => normalizeChartServerWindowConfig(chartServerWindow),
            [chartServerWindow]
        );
        const effectiveSortOptions = useMemo(() => {
            const baseOptions = (sortOptions && typeof sortOptions === 'object') ? sortOptions : {};
            const baseColumnOptions = (baseOptions.columnOptions && typeof baseOptions.columnOptions === 'object')
                ? baseOptions.columnOptions
                : {};
            const mergedColumnOptions = { ...baseColumnOptions };

            Object.entries(pivotColumnSorting || {}).forEach(([field, sortState]) => {
                if (!field || !sortState || typeof sortState !== 'object') return;
                const direction = sortState.pivotDirection;
                const mode = sortState.pivotSortMode;
                const existing = mergedColumnOptions[field] && typeof mergedColumnOptions[field] === 'object'
                    ? mergedColumnOptions[field]
                    : {};
                if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) {
                    mergedColumnOptions[field] = {
                        ...existing,
                        pivotSortMode: mode,
                        pivotDirection: direction,
                    };
                } else if (
                    Object.prototype.hasOwnProperty.call(existing, 'pivotDirection')
                    || Object.prototype.hasOwnProperty.call(existing, 'pivotSortMode')
                ) {
                    const { pivotDirection, pivotSortMode, ...rest } = existing;
                    mergedColumnOptions[field] = rest;
                }
            });

            return {
                ...baseOptions,
                columnOptions: mergedColumnOptions,
            };
        }, [sortOptions, pivotColumnSorting]);

        const getDisplayRows = useCallback(() => {
            if (displayRowsRef.current.length > 0) return displayRowsRef.current;
            if (tableRef.current && tableRef.current.getRowModel) {
                return tableRef.current.getRowModel().rows || [];
            }
            return [];
        }, []);

        const resolveDisplayRowIndex = useCallback((rowId, fallbackIndex = 0) => {
            const mappedIndex = displayRowIndexRef.current.get(rowId);
            return mappedIndex !== undefined ? mappedIndex : fallbackIndex;
        }, []);

        const scrollToDisplayRow = useCallback((displayIndex) => {
            const currentRowVirtualizer = activeRowVirtualizerRef.current;
            if (!currentRowVirtualizer || !currentRowVirtualizer.scrollToIndex) return;
            const { topCount, centerCount } = pinnedDisplayMetaRef.current;
            if (displayIndex < topCount) {
                if (parentRef.current) parentRef.current.scrollTop = 0;
                return;
            }
            if (displayIndex >= topCount + centerCount) {
                currentRowVirtualizer.scrollToIndex(Math.max(centerCount - 1, 0));
                return;
            }
            currentRowVirtualizer.scrollToIndex(Math.max(displayIndex - topCount, 0));
        }, [parentRef]);

        const scrollToDisplayColumn = useCallback((displayIndex) => {
            const currentColumnVirtualizer = columnVirtualizerRef.current;
            if (!currentColumnVirtualizer || !currentColumnVirtualizer.scrollToIndex) return;
            const { leftCount, centerCount } = pinnedColumnMetaRef.current;
            if (displayIndex < leftCount) {
                if (parentRef.current) parentRef.current.scrollLeft = 0;
                return;
            }
            if (displayIndex >= leftCount + centerCount) {
                currentColumnVirtualizer.scrollToIndex(Math.max(centerCount - 1, 0));
                return;
            }
            currentColumnVirtualizer.scrollToIndex(Math.max(displayIndex - leftCount, 0));
        }, [parentRef]);

    // Reset Effect
    useEffect(() => {
        if (reset) {
            setRowFields(initialRowFields);
            setColFields(initialColFields);
            setValConfigs(initialValConfigs);
            setFilters({});
            setSorting([]);
            setExpanded({});
            setColumnPinning(initialColumnPinning);
            setRowPinning(initialRowPinning);
            setGrandTotalPinOverride(getPinnedSideForRow(initialRowPinning, GRAND_TOTAL_ROW_ID));
            setPivotColumnSorting({});
            setColumnVisibility({});
            setColumnSizing({});
            setChartPanelSource(initialChartDefinition.source);
            setChartPanelType(initialChartDefinition.chartType);
            setChartPanelBarLayout(initialChartDefinition.barLayout);
            setChartPanelAxisMode(initialChartDefinition.axisMode);
            chartPanelOrientationAutoRef.current = true;
            setChartPanelOrientation(initialChartDefinition.orientation);
            setChartPanelHierarchyLevel(initialChartDefinition.hierarchyLevel);
            setChartPanelTitle(initialChartDefinition.chartTitle);
            setChartPanelRowLimit(initialChartDefinition.rowLimit);
            setChartPanelColumnLimit(initialChartDefinition.columnLimit);
            setChartPanelWidth(initialChartDefinition.width);
            setChartPanelGraphHeight(initialChartDefinition.chartHeight);
            setChartPanelFloating(false);
            setChartPanelFloatingRect(clampFloatingChartRect({
                width: initialChartDefinition.width,
                height: DEFAULT_FLOATING_CHART_PANEL_HEIGHT,
                left: 24,
                top: 24,
            }, null));
            setChartPanelSortMode(initialChartDefinition.sortMode);
            setChartPanelInteractionMode(initialChartDefinition.interactionMode);
            setChartPanelServerScope(initialChartDefinition.serverScope);
            setChartPanelLocked(false);
            setChartPanelLockedModel(null);
            setChartPanelLockedRequest(null);
            setManagedChartDefinitions(initialChartDefinitions);
            setActiveChartDefinitionId(initialChartDefinitions[0] ? initialChartDefinitions[0].id : 'live-chart-panel');
            setChartCanvasPanes([]);
            setTableCanvasSize(DEFAULT_TABLE_CANVAS_SIZE);
            setChartPaneDataById({});

            if (setPropsRef.current) {
                setPropsRef.current({
                    rowFields: initialRowFields,
                    colFields: initialColFields,
                    valConfigs: initialValConfigs,
                    filters: {},
                    sorting: [],
                    expanded: {},
                    columnPinning: initialColumnPinning,
                    rowPinning: initialRowPinning,
                    sortOptions: sortOptions,
                    columnVisibility: {},
                    columnSizing: {},
                    reset: null
                });
            }
        }
    }, [reset, initialRowFields, initialColFields, initialValConfigs, initialColumnPinning, initialRowPinning, initialChartDefinition, initialChartDefinitions]);

        // Save Persistence
        useEffect(() => {
            if (!persistence) return;
            savePersistedState('columnPinning', columnPinning);
            savePersistedState('rowPinning', rowPinning);
            savePersistedState('grandTotalPinOverride', grandTotalPinOverride);
            savePersistedState('columnVisibility', columnVisibility);
            savePersistedState('columnSizing', columnSizing);
            savePersistedState('chartCanvasPanes', chartCanvasPanes);
            savePersistedState('tableCanvasSize', tableCanvasSize);
            savePersistedState('chartPanelFloatingLayout', {
                floating: chartPanelFloating,
                rect: chartPanelFloatingRect,
            });
            savePersistedState('chartPanelLockState', {
                locked: chartPanelLocked,
                lockedModel: chartPanelLockedModel,
                lockedRequest: chartPanelLockedRequest,
            });
        }, [
            id,
            columnPinning,
            rowPinning,
            grandTotalPinOverride,
            columnVisibility,
            columnSizing,
            chartCanvasPanes,
            tableCanvasSize,
            chartPanelFloating,
            chartPanelFloatingRect,
            chartPanelLocked,
            chartPanelLockedModel,
            chartPanelLockedRequest,
            persistence,
            persistence_type,
        ]);

        useEffect(() => {
            const handleResize = () => {
                if (window.innerWidth < 768 && columnPinning.right && columnPinning.right.length > 0) {
                     setColumnPinning(prev => ({ ...prev, right: [] }));
                     showNotification("Right pinned columns hidden due to screen size.", "warning");
                }
            };
            window.addEventListener('resize', handleResize);
            return () => window.removeEventListener('resize', handleResize);
        }, [columnPinning.right, showNotification]);

    const [cinemaMode, setCinemaMode] = useState(initialCinemaMode);
    const [showRowTotals, setShowRowTotals] = useState(initialShowRowTotals);
    const [showColTotals, setShowColTotals] = useState(initialShowColTotals);
    const effectiveGrandTotalPinState = useMemo(
        () => resolveGrandTotalPinState(showColTotals, grandTotalPosition, grandTotalPinOverride),
        [showColTotals, grandTotalPosition, grandTotalPinOverride]
    );
    const serverSidePinsGrandTotal = serverSide && showColTotals && (
        effectiveGrandTotalPinState === 'top' || effectiveGrandTotalPinState === 'bottom'
    );
    const [showRowNumbers, setShowRowNumbers] = useState(false);
    const [sidebarOpen, setSidebarOpen] = useState(true);
    const [sidebarWidth, setSidebarWidth] = useState(288);
    const [activeFilterCol, setActiveFilterCol] = useState(null);
    const [filterAnchorEl, setFilterAnchorEl] = useState(null);
    const [sidebarTab, setSidebarTab] = useState('fields'); // 'fields', 'columns'
    const [showFloatingFilters, setShowFloatingFilters] = useState(false);
    const [stickyHeaders, setStickyHeaders] = useState(true);
    const [colSearch, setColSearch] = useState('');
    const [colTypeFilter, setColTypeFilter] = useState('all');
    const [selectedCols, setSelectedCols] = useState(new Set());
    const [hoveredHeaderId, setHoveredHeaderId] = useState(null);
    const [focusedHeaderId, setFocusedHeaderId] = useState(null);
    
    // Global Keyboard Shortcuts
    useEffect(() => {
        const handleGlobalKeyDown = (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'b') {
                e.preventDefault();
                setSidebarOpen(prev => !prev);
            }
        };
        window.addEventListener('keydown', handleGlobalKeyDown);
        return () => window.removeEventListener('keydown', handleGlobalKeyDown);
    }, []);

    const [colorScaleMode, setColorScaleMode] = useState('off');
    const [colorPalette, setColorPalette] = useState('redGreen');
    const [dataBarsColumns, setDataBarsColumns] = useState(new Set());

    // Font / display controls
    const [fontFamily, setFontFamily] = useState("'Inter', system-ui, sans-serif");
    const [fontSize, setFontSize] = useState('14px');
    const [decimalPlaces, setDecimalPlaces] = useState(0);
    const [columnDecimalOverrides, setColumnDecimalOverrides] = useState({});
    const [cellFormatRules, setCellFormatRules] = useState({});
    const [hoveredRowPath, setHoveredRowPath] = useState(null);
    const [zoomLevel, setZoomLevel] = useState(100);

    // selectedCellKeys and selectedCellColIds are now derived inside PivotAppBar
    // to avoid stale prop issues between parent and child renders.

    // Decimal formatting logic moved to PivotAppBar to use fresh selectedCells

    const [spacingMode, setSpacingMode] = useState(1);
    const spacingLabels = gridDimensionTokens.density.spacingLabels;
    const rowHeights = gridDimensionTokens.density.rowHeights;
    const defaultColumnWidths = gridDimensionTokens.columnWidths;
    const autoSizeBounds = gridDimensionTokens.autoSize;
    
    const [colExpanded, setColExpanded] = useState({});
    const [contextMenu, setContextMenu] = useState(null);
    const [selectedCells, setSelectedCells] = useState({});
    const [lastSelected, setLastSelected] = useState(null);
    const [isDragging, setIsDragging] = useState(false);
    const [dragStart, setDragStart] = useState(null);
    const [isFilling, setIsFilling] = useState(false);
        const [fillRange, setFillRange] = useState(null);

        // --- Data Management State ---
    const [history, setHistory] = useState([]);
    const [future, setFuture] = useState([]);

    const handleUndo = () => {
        if (history.length === 0) return;
        const previous = history[history.length - 1];
        setHistory(history.slice(0, -1));
        setFuture([selectedCells, ...future]);
        if (setProps) setProps({ undo: true, timestamp: Date.now() });
    };

    const handleRedo = () => {
        if (future.length === 0) return;
        const next = future[0];
        setFuture(future.slice(1));
        setHistory([...history, selectedCells]);
        if (setProps) setProps({ redo: true, timestamp: Date.now() });
    };

    const handleRefresh = () => {
        if (setProps) setProps({ refresh: Date.now() });
    };

    useEffect(() => {
        if (!viewState || typeof viewState !== 'object') return;
        const restored = (viewState.state && typeof viewState.state === 'object')
            ? viewState.state
            : viewState;
        const restoredChartDefinitions = Array.isArray(restored.chartDefinitions)
            ? sanitizeChartDefinitions(restored.chartDefinitions, initialChartDefinition)
            : null;
        const restoredActiveChartDefinitionId = typeof restored.activeChartDefinitionId === 'string'
            ? restored.activeChartDefinitionId
            : null;
        const restoredChartDefinition = Array.isArray(restoredChartDefinitions)
            ? (restoredChartDefinitions.find((definition) => definition.id === restoredActiveChartDefinitionId) || restoredChartDefinitions[0] || null)
            : null;

        const sanitizedFilters = (() => {
            if (!restored.filters || typeof restored.filters !== 'object') return null;
            const next = { ...restored.filters };
            delete next.__request_unique__;
            return next;
        })();

        if (Array.isArray(restored.rowFields)) setRowFields(restored.rowFields);
        if (Array.isArray(restored.colFields)) setColFields(restored.colFields);
        if (Array.isArray(restored.valConfigs)) setValConfigs(restored.valConfigs);
        if (sanitizedFilters) setFilters(sanitizedFilters);
        if (Array.isArray(restored.sorting)) setSorting(restored.sorting);
        if (restored.expanded && typeof restored.expanded === 'object') setExpanded(restored.expanded);
        if (typeof restored.cinemaMode === 'boolean') setCinemaMode(restored.cinemaMode);
        if (typeof restored.showRowTotals === 'boolean') setShowRowTotals(restored.showRowTotals);
        if (typeof restored.showColTotals === 'boolean') setShowColTotals(restored.showColTotals);
        if (typeof restored.showRowNumbers === 'boolean') setShowRowNumbers(restored.showRowNumbers);
        if (typeof restored.sidebarOpen === 'boolean') setSidebarOpen(restored.sidebarOpen);
        if (typeof restored.sidebarTab === 'string') setSidebarTab(restored.sidebarTab);
        if (typeof restored.showFloatingFilters === 'boolean') setShowFloatingFilters(restored.showFloatingFilters);
        if (typeof restored.stickyHeaders === 'boolean') setStickyHeaders(restored.stickyHeaders);
        if (typeof restored.colSearch === 'string') setColSearch(restored.colSearch);
        if (typeof restored.colTypeFilter === 'string') setColTypeFilter(restored.colTypeFilter);
        if (typeof restored.themeName === 'string' && themes[restored.themeName]) setThemeName(restored.themeName);
        if (restored.themeOverrides && typeof restored.themeOverrides === 'object') setThemeOverrides(restored.themeOverrides);
        if (typeof restored.layoutMode === 'string') setLayoutMode(restored.layoutMode);
        if (typeof restored.colorScaleMode === 'string') setColorScaleMode(restored.colorScaleMode);
        if (typeof restored.colorPalette === 'string') setColorPalette(restored.colorPalette);
        if (typeof restored.spacingMode === 'number') setSpacingMode(restored.spacingMode);
        if (Array.isArray(restored.dataBarsColumns)) setDataBarsColumns(new Set(restored.dataBarsColumns));
        if (restored.pivotColumnSorting && typeof restored.pivotColumnSorting === 'object') setPivotColumnSorting(restored.pivotColumnSorting);
        if (restored.columnPinning && typeof restored.columnPinning === 'object') setColumnPinning(restored.columnPinning);
        const restoredRowPinning = restored.rowPinning && typeof restored.rowPinning === 'object'
            ? restored.rowPinning
            : null;
        if (restoredRowPinning) setRowPinning(restoredRowPinning);
        if (Object.prototype.hasOwnProperty.call(restored, 'grandTotalPinOverride')) {
            setGrandTotalPinOverride(sanitizeGrandTotalPinOverride(restored.grandTotalPinOverride));
        } else {
            setGrandTotalPinOverride(getPinnedSideForRow(restoredRowPinning, GRAND_TOTAL_ROW_ID));
        }
        if (restored.columnVisibility && typeof restored.columnVisibility === 'object') setColumnVisibility(restored.columnVisibility);
        if (restored.columnSizing && typeof restored.columnSizing === 'object') setColumnSizing(restored.columnSizing);
        if (restored.colExpanded && typeof restored.colExpanded === 'object') setColExpanded(restored.colExpanded);
        if (typeof restored.decimalPlaces === 'number') setDecimalPlaces(restored.decimalPlaces);
        if (typeof restored.zoomLevel === 'number') setZoomLevel(Math.max(60, Math.min(160, restored.zoomLevel)));
        if (restored.columnDecimalOverrides && typeof restored.columnDecimalOverrides === 'object') setColumnDecimalOverrides(restored.columnDecimalOverrides);
        if (restored.cellFormatRules && typeof restored.cellFormatRules === 'object') setCellFormatRules(restored.cellFormatRules);
        if (restoredChartDefinitions) {
            setManagedChartDefinitions(restoredChartDefinitions);
            setActiveChartDefinitionId(restoredChartDefinition ? restoredChartDefinition.id : restoredChartDefinitions[0].id);
        }
        const restoredChartSource = restored.chartPanelSource !== undefined ? restored.chartPanelSource : (restoredChartDefinition && restoredChartDefinition.source);
        if (restoredChartSource === 'pivot' || restoredChartSource === 'selection') setChartPanelSource(restoredChartSource);
        const restoredChartType = restored.chartPanelType !== undefined ? restored.chartPanelType : (restoredChartDefinition && restoredChartDefinition.chartType);
        if (VALID_CHART_TYPES.has(restoredChartType)) setChartPanelType(restoredChartType);
        const restoredBarLayout = restored.chartPanelBarLayout !== undefined ? restored.chartPanelBarLayout : (restoredChartDefinition && restoredChartDefinition.barLayout);
        if (restoredBarLayout === 'grouped' || restoredBarLayout === 'stacked') setChartPanelBarLayout(restoredBarLayout);
        const restoredAxisMode = restored.chartPanelAxisMode !== undefined ? restored.chartPanelAxisMode : (restoredChartDefinition && restoredChartDefinition.axisMode);
        if (restoredAxisMode === 'vertical' || restoredAxisMode === 'horizontal') setChartPanelAxisMode(restoredAxisMode);
        if (restored.chartPanelOrientation === 'rows' || restored.chartPanelOrientation === 'columns') {
            chartPanelOrientationAutoRef.current = false;
            setChartPanelOrientation(restored.chartPanelOrientation);
        } else if (restoredChartDefinition && (restoredChartDefinition.orientation === 'rows' || restoredChartDefinition.orientation === 'columns')) {
            chartPanelOrientationAutoRef.current = false;
            setChartPanelOrientation(restoredChartDefinition.orientation);
        } else {
            chartPanelOrientationAutoRef.current = true;
            setChartPanelOrientation(getPreferredChartOrientation(restored.colFields || colFields));
        }
        const restoredHierarchyLevel = restored.chartPanelHierarchyLevel !== undefined
            ? restored.chartPanelHierarchyLevel
            : (restoredChartDefinition && restoredChartDefinition.hierarchyLevel);
        if (restoredHierarchyLevel === 'all' || (typeof restoredHierarchyLevel === 'number' && restoredHierarchyLevel >= 1)) {
            setChartPanelHierarchyLevel(restoredHierarchyLevel);
        }
        const restoredChartTitle = restored.chartPanelTitle !== undefined
            ? restored.chartPanelTitle
            : (restoredChartDefinition && restoredChartDefinition.chartTitle);
        if (typeof restoredChartTitle === 'string') {
            setChartPanelTitle(restoredChartTitle);
        }
        const restoredChartLayers = Array.isArray(restored.chartPanelLayers)
            ? restored.chartPanelLayers
            : (restoredChartDefinition && Array.isArray(restoredChartDefinition.chartLayers) ? restoredChartDefinition.chartLayers : null);
        if (Array.isArray(restoredChartLayers)) {
            setChartPanelLayers(restoredChartLayers);
        }
        const restoredRowLimit = restored.chartPanelRowLimit !== undefined
            ? restored.chartPanelRowLimit
            : (restoredChartDefinition && restoredChartDefinition.rowLimit);
        if (typeof restoredRowLimit === 'number') {
            setChartPanelRowLimit(Math.max(1, Math.floor(restoredRowLimit)));
        }
        const restoredColumnLimit = restored.chartPanelColumnLimit !== undefined
            ? restored.chartPanelColumnLimit
            : (restoredChartDefinition && restoredChartDefinition.columnLimit);
        if (typeof restoredColumnLimit === 'number') {
            setChartPanelColumnLimit(Math.max(1, Math.floor(restoredColumnLimit)));
        }
        const restoredChartWidth = restored.chartPanelWidth !== undefined
            ? restored.chartPanelWidth
            : (restoredChartDefinition && restoredChartDefinition.width);
        if (typeof restoredChartWidth === 'number') {
            setChartPanelWidth(Math.max(320, Math.min(960, restoredChartWidth)));
        }
        const restoredChartGraphHeight = restored.chartPanelGraphHeight !== undefined
            ? restored.chartPanelGraphHeight
            : (restoredChartDefinition && restoredChartDefinition.chartHeight);
        if (typeof restoredChartGraphHeight === 'number') {
            setChartPanelGraphHeight(Math.max(180, Math.floor(restoredChartGraphHeight)));
        }
        const restoredChartSortMode = restored.chartPanelSortMode !== undefined
            ? restored.chartPanelSortMode
            : (restoredChartDefinition && restoredChartDefinition.sortMode);
        if (VALID_CHART_SORT_MODES.has(restoredChartSortMode)) {
            setChartPanelSortMode(restoredChartSortMode);
        }
        const restoredChartInteractionMode = restored.chartPanelInteractionMode !== undefined
            ? restored.chartPanelInteractionMode
            : (restoredChartDefinition && restoredChartDefinition.interactionMode);
        if (VALID_CHART_INTERACTION_MODES.has(restoredChartInteractionMode)) {
            setChartPanelInteractionMode(restoredChartInteractionMode);
        }
        const restoredChartServerScope = restored.chartPanelServerScope !== undefined
            ? restored.chartPanelServerScope
            : (restoredChartDefinition && restoredChartDefinition.serverScope);
        if (VALID_CHART_SERVER_SCOPES.has(restoredChartServerScope)) {
            setChartPanelServerScope(restoredChartServerScope);
        }
        if (typeof restored.chartPanelFloating === 'boolean') {
            setChartPanelFloating(restored.chartPanelFloating);
        }
        if (restored.chartPanelFloatingRect && typeof restored.chartPanelFloatingRect === 'object') {
            setChartPanelFloatingRect(clampFloatingChartRect(restored.chartPanelFloatingRect, null));
        }
        if (typeof restored.chartPanelLocked === 'boolean') {
            setChartPanelLocked(restored.chartPanelLocked);
        }
        if (Object.prototype.hasOwnProperty.call(restored, 'chartPanelLockedModel')) {
            setChartPanelLockedModel(restored.chartPanelLockedModel && typeof restored.chartPanelLockedModel === 'object'
                ? restored.chartPanelLockedModel
                : null);
        }
        if (Object.prototype.hasOwnProperty.call(restored, 'chartPanelLockedRequest')) {
            setChartPanelLockedRequest(normalizeLockedChartRequest(restored.chartPanelLockedRequest));
        }
        if (Array.isArray(restored.chartCanvasPanes)) {
            setChartCanvasPanes(sanitizeChartCanvasPanes(restored.chartCanvasPanes, initialChartDefinition));
        }
        if (typeof restored.tableCanvasSize === 'number' && Number.isFinite(restored.tableCanvasSize) && restored.tableCanvasSize > 0) {
            setTableCanvasSize(restored.tableCanvasSize);
        }

        if (viewState.viewport && typeof viewState.viewport === 'object') {
            latestViewportRef.current = {
                ...latestViewportRef.current,
                ...viewState.viewport,
            };
        }

        const savedScroll = restored.scroll && typeof restored.scroll === 'object' ? restored.scroll : null;
        if (savedScroll && parentRef.current) {
            requestAnimationFrame(() => {
                if (!parentRef.current) return;
                if (typeof savedScroll.top === 'number') parentRef.current.scrollTop = savedScroll.top;
                if (typeof savedScroll.left === 'number') parentRef.current.scrollLeft = savedScroll.left;
            });
        }
    }, [viewState, initialChartDefinition]);

    useEffect(() => {
        if (themes[defaultTheme]) {
            setThemeName(defaultTheme);
        }
    }, [defaultTheme]);

    useEffect(() => {
        const previousRowFields = previousRowFieldsRef.current || [];
        if (JSON.stringify(previousRowFields) === JSON.stringify(rowFields)) return;

        previousRowFieldsRef.current = rowFields;
        pinnedRowCacheRef.current.clear();
        setRowPinning(prev => {
            const normalized = normalizeRowPinningState(prev);
            if (normalized.top.length === 0 && normalized.bottom.length === 0) {
                return prev;
            }
            return { top: [], bottom: [] };
        });
    }, [rowFields]);

    useEffect(() => {
        setPivotColumnSorting(prev => {
            const activeFields = new Set(colFields || []);
            const next = Object.entries(prev || {}).reduce((acc, [field, sortState]) => {
                if (activeFields.has(field)) acc[field] = sortState;
                return acc;
            }, {});
            return JSON.stringify(next) === JSON.stringify(prev || {}) ? prev : next;
        });
    }, [colFields]);

    useEffect(() => {
        if (chartPanelHierarchyLevel === 'all') return;
        if (typeof chartPanelHierarchyLevel === 'number' && chartPanelHierarchyLevel <= Math.max(rowFields.length, 0)) return;
        setChartPanelHierarchyLevel('all');
    }, [chartPanelHierarchyLevel, rowFields.length]);

    useEffect(() => {
        if (!chartPanelOrientationAutoRef.current) return;
        const preferredOrientation = getPreferredChartOrientation(colFields);
        setChartPanelOrientation(prev => (prev === preferredOrientation ? prev : preferredOrientation));
    }, [colFields]);

    const handleChartPanelOrientationChange = useCallback((nextOrientation) => {
        chartPanelOrientationAutoRef.current = false;
        setChartPanelOrientation(nextOrientation);
    }, []);

    const currentChartDefinition = useMemo(() => normalizeChartDefinition({
        id: activeChartDefinitionId || 'live-chart-panel',
        name: (managedChartDefinitions.find((definition) => definition.id === activeChartDefinitionId) || {}).name || 'Live Chart',
        chartTitle: chartPanelTitle,
        source: chartPanelSource,
        chartType: chartPanelType,
        barLayout: chartPanelBarLayout,
        axisMode: chartPanelAxisMode,
        orientation: chartPanelOrientation,
        hierarchyLevel: chartPanelHierarchyLevel,
        chartLayers: chartPanelLayers,
        rowLimit: chartPanelRowLimit,
        columnLimit: chartPanelColumnLimit,
        width: chartPanelWidth,
        chartHeight: chartPanelGraphHeight,
        sortMode: chartPanelSortMode,
        interactionMode: chartPanelInteractionMode,
        serverScope: chartPanelServerScope,
    }, initialChartDefinition), [
        activeChartDefinitionId,
        managedChartDefinitions,
        chartPanelTitle,
        chartPanelSource,
        chartPanelType,
        chartPanelBarLayout,
        chartPanelAxisMode,
        chartPanelOrientation,
        chartPanelHierarchyLevel,
        chartPanelLayers,
        chartPanelRowLimit,
        chartPanelColumnLimit,
        chartPanelWidth,
        chartPanelGraphHeight,
        chartPanelSortMode,
        chartPanelInteractionMode,
        chartPanelServerScope,
        initialChartDefinition,
    ]);

    useEffect(() => {
        const serializedExternal = JSON.stringify(chartDefinitions || null);
        if (serializedExternal === lastChartDefinitionsPropRef.current) return;
        lastChartDefinitionsPropRef.current = serializedExternal;
        if (!Array.isArray(chartDefinitions) || chartDefinitions.length === 0) return;
        const nextDefinitions = sanitizeChartDefinitions(chartDefinitions, initialChartDefinition);
        setManagedChartDefinitions(nextDefinitions);
        setActiveChartDefinitionId((previousId) => (
            nextDefinitions.some((definition) => definition.id === previousId)
                ? previousId
                : nextDefinitions[0].id
        ));
    }, [chartDefinitions, initialChartDefinition]);

    useEffect(() => {
        if (Array.isArray(chartDefinitions) && chartDefinitions.length > 0) return;
        setManagedChartDefinitions(initialChartDefinitions);
        setActiveChartDefinitionId(initialChartDefinitions[0] ? initialChartDefinitions[0].id : 'live-chart-panel');
    }, [chartDefinitions, initialChartDefinitions]);

    useEffect(() => {
        const serializedExternal = JSON.stringify(externalChartCanvasPanes || null);
        if (serializedExternal === lastChartCanvasPanesPropRef.current) return;
        lastChartCanvasPanesPropRef.current = serializedExternal;
        if (!Array.isArray(externalChartCanvasPanes)) return;
        setChartCanvasPanes(sanitizeChartCanvasPanes(externalChartCanvasPanes, initialChartDefinition));
    }, [externalChartCanvasPanes, initialChartDefinition]);

    useEffect(() => {
        const normalizedExternalSize = Number(externalTableCanvasSize);
        if (!Number.isFinite(normalizedExternalSize) || normalizedExternalSize <= 0) return;
        if (normalizedExternalSize === lastTableCanvasSizePropRef.current) return;
        lastTableCanvasSizePropRef.current = normalizedExternalSize;
        setTableCanvasSize(normalizedExternalSize);
    }, [externalTableCanvasSize]);

    useEffect(() => {
        const activeDefinition = managedChartDefinitions.find((definition) => definition.id === activeChartDefinitionId);
        if (!activeDefinition || applyingChartDefinitionRef.current) return;
        applyingChartDefinitionRef.current = true;
        setChartPanelSource(activeDefinition.source);
        setChartPanelType(activeDefinition.chartType);
        setChartPanelBarLayout(activeDefinition.barLayout);
        setChartPanelAxisMode(activeDefinition.axisMode);
        chartPanelOrientationAutoRef.current = false;
        setChartPanelOrientation(activeDefinition.orientation);
        setChartPanelHierarchyLevel(activeDefinition.hierarchyLevel);
        setChartPanelTitle(activeDefinition.chartTitle || activeDefinition.name || 'Chart');
        setChartPanelLayers(Array.isArray(activeDefinition.chartLayers) ? activeDefinition.chartLayers : []);
        setChartPanelRowLimit(activeDefinition.rowLimit);
        setChartPanelColumnLimit(activeDefinition.columnLimit);
        setChartPanelWidth(activeDefinition.width);
        setChartPanelGraphHeight(activeDefinition.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT);
        setChartPanelSortMode(activeDefinition.sortMode || 'natural');
        setChartPanelInteractionMode(activeDefinition.interactionMode || 'focus');
        setChartPanelServerScope(activeDefinition.serverScope || initialChartDefinition.serverScope);
        requestAnimationFrame(() => {
            applyingChartDefinitionRef.current = false;
        });
    }, [activeChartDefinitionId, managedChartDefinitions, initialChartDefinition.serverScope]);

    useEffect(() => {
        if (applyingChartDefinitionRef.current) return;
        setManagedChartDefinitions((previousDefinitions) => {
            const existingDefinitions = Array.isArray(previousDefinitions) && previousDefinitions.length > 0
                ? previousDefinitions
                : [currentChartDefinition];
            const nextDefinitions = existingDefinitions.map((definition) => (
                definition.id === currentChartDefinition.id
                    ? { ...definition, ...currentChartDefinition }
                    : definition
            ));
            if (!nextDefinitions.some((definition) => definition.id === currentChartDefinition.id)) {
                nextDefinitions.push(currentChartDefinition);
            }
            const previousJson = JSON.stringify(existingDefinitions);
            const nextJson = JSON.stringify(nextDefinitions);
            if (previousJson === nextJson) return previousDefinitions;
            if (setPropsRef.current) {
                setPropsRef.current({ chartDefinitions: nextDefinitions });
            }
            return nextDefinitions;
        });
    }, [currentChartDefinition]);

    useEffect(() => {
        if (!setPropsRef.current) return;
        setPropsRef.current({
            chartCanvasPanes: chartCanvasPanes.map((pane) => ({ ...pane })),
            tableCanvasSize,
        });
    }, [chartCanvasPanes, tableCanvasSize]);

    const handleCreateChartDefinition = useCallback(() => {
        const nextDefinition = normalizeChartDefinition({
            ...currentChartDefinition,
            id: createChartDefinitionId('chart'),
            name: `Chart ${managedChartDefinitions.length + 1}`,
        }, initialChartDefinition);
        setManagedChartDefinitions((previousDefinitions) => [...previousDefinitions, nextDefinition]);
        setActiveChartDefinitionId(nextDefinition.id);
    }, [currentChartDefinition, managedChartDefinitions.length, initialChartDefinition]);

    const handleDuplicateChartDefinition = useCallback(() => {
        const sourceDefinition = managedChartDefinitions.find((definition) => definition.id === activeChartDefinitionId) || currentChartDefinition;
        const nextDefinition = normalizeChartDefinition({
            ...sourceDefinition,
            id: createChartDefinitionId('chart'),
            name: `${sourceDefinition.name || 'Chart'} Copy`,
        }, initialChartDefinition);
        setManagedChartDefinitions((previousDefinitions) => [...previousDefinitions, nextDefinition]);
        setActiveChartDefinitionId(nextDefinition.id);
    }, [activeChartDefinitionId, currentChartDefinition, managedChartDefinitions, initialChartDefinition]);

    const handleDeleteChartDefinition = useCallback(() => {
        setManagedChartDefinitions((previousDefinitions) => {
            if (!Array.isArray(previousDefinitions) || previousDefinitions.length <= 1) return previousDefinitions;
            const nextDefinitions = previousDefinitions.filter((definition) => definition.id !== activeChartDefinitionId);
            if (nextDefinitions.length > 0) {
                setActiveChartDefinitionId(nextDefinitions[0].id);
            }
            return nextDefinitions;
        });
    }, [activeChartDefinitionId]);

    const handleRenameChartDefinition = useCallback((name) => {
        setManagedChartDefinitions((previousDefinitions) => previousDefinitions.map((definition) => (
            definition.id === activeChartDefinitionId
                ? { ...definition, name: sanitizeChartDefinitionName(name, definition.name || 'Chart') }
                : definition
        )));
    }, [activeChartDefinitionId]);

    const clampChartPanelWidth = useCallback((requestedWidth) => {
        const layoutWidth = chartLayoutRef.current
            ? chartLayoutRef.current.getBoundingClientRect().width
            : 0;
        const { minWidth, maxWidth } = getChartPanelWidthBounds(layoutWidth);
        return Math.max(minWidth, Math.min(maxWidth, requestedWidth));
    }, []);

    useEffect(() => {
        if (!isChartPanelResizing) return undefined;

        const previousUserSelect = document.body.style.userSelect;
        const previousCursor = document.body.style.cursor;
        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'col-resize';

        const handlePointerMove = (event) => {
            const layoutRect = chartLayoutRef.current
                ? chartLayoutRef.current.getBoundingClientRect()
                : null;
            if (!layoutRect || layoutRect.width <= 0) return;
            const nextWidth = clampChartPanelWidth(layoutRect.right - event.clientX);
            setChartPanelWidth(nextWidth);
        };

        const stopResize = () => {
            setIsChartPanelResizing(false);
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('mouseleave', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            document.body.style.userSelect = previousUserSelect;
            document.body.style.cursor = previousCursor;
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('mouseleave', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, [isChartPanelResizing, clampChartPanelWidth]);

    useLayoutEffect(() => {
        if (!chartPanelOpen || chartPanelFloating) return undefined;

        const syncWidthToLayout = () => {
            setChartPanelWidth((prevWidth) => {
                const nextWidth = clampChartPanelWidth(prevWidth);
                return nextWidth === prevWidth ? prevWidth : nextWidth;
            });
        };

        syncWidthToLayout();

        if (typeof ResizeObserver === 'undefined' || !chartLayoutRef.current) {
            window.addEventListener('resize', syncWidthToLayout);
            return () => {
                window.removeEventListener('resize', syncWidthToLayout);
            };
        }

        const observer = new ResizeObserver(syncWidthToLayout);
        observer.observe(chartLayoutRef.current);
        return () => {
            observer.disconnect();
        };
    }, [chartPanelOpen, chartPanelFloating, sidebarOpen, clampChartPanelWidth]);

    const handleStartChartCanvasResize = useCallback((leftKey, rightKey, event) => {
        const containerRect = chartCanvasLayoutRef.current
            ? chartCanvasLayoutRef.current.getBoundingClientRect()
            : null;
        if (!containerRect || containerRect.width <= 0) return;
        const chartPaneSizes = chartCanvasPanes.reduce((acc, pane) => {
            acc[pane.id] = pane.size;
            return acc;
        }, {});
        chartCanvasResizeRef.current = {
            startX: event.clientX,
            leftKey,
            rightKey,
            containerWidth: containerRect.width,
            totalUnits: tableCanvasSize + chartCanvasPanes.reduce((sum, pane) => sum + pane.size, 0),
            leftSize: leftKey === 'table' ? tableCanvasSize : (chartPaneSizes[leftKey] || 1),
            rightSize: rightKey === 'table' ? tableCanvasSize : (chartPaneSizes[rightKey] || 1),
        };
    }, [chartCanvasPanes, tableCanvasSize]);

    useEffect(() => {
        const handlePointerMove = (event) => {
            const resizeState = chartCanvasResizeRef.current;
            if (!resizeState) return;
            const totalUnits = resizeState.totalUnits > 0 ? resizeState.totalUnits : 1;
            const minSizeUnits = (MIN_CHART_CANVAS_PANE_WIDTH / Math.max(1, resizeState.containerWidth)) * totalUnits;
            const deltaUnits = ((event.clientX - resizeState.startX) / Math.max(1, resizeState.containerWidth)) * totalUnits;
            const nextLeftSize = Math.max(minSizeUnits, resizeState.leftSize + deltaUnits);
            const nextRightSize = Math.max(minSizeUnits, resizeState.rightSize - deltaUnits);
            const consumed = nextLeftSize + nextRightSize;
            const targetConsumed = resizeState.leftSize + resizeState.rightSize;
            const correction = targetConsumed > 0 ? targetConsumed / Math.max(consumed, 0.0001) : 1;
            const correctedLeft = nextLeftSize * correction;
            const correctedRight = nextRightSize * correction;

            if (resizeState.leftKey === 'table') {
                setTableCanvasSize(correctedLeft);
            } else {
                setChartCanvasPanes((previousPanes) => previousPanes.map((pane) => (
                    pane.id === resizeState.leftKey ? { ...pane, size: correctedLeft } : pane
                )));
            }

            if (resizeState.rightKey === 'table') {
                setTableCanvasSize(correctedRight);
            } else {
                setChartCanvasPanes((previousPanes) => previousPanes.map((pane) => (
                    pane.id === resizeState.rightKey ? { ...pane, size: correctedRight } : pane
                )));
            }
        };

        const stopResize = () => {
            chartCanvasResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopResize);
        window.addEventListener('mouseleave', stopResize);
        window.addEventListener('blur', stopResize);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopResize);
            window.removeEventListener('mouseleave', stopResize);
            window.removeEventListener('blur', stopResize);
        };
    }, []);

    const handleToggleChartPanelFloating = useCallback(() => {
        setChartPanelFloating((previousFloating) => {
            const nextFloating = !previousFloating;
            if (nextFloating) {
                const containerRect = chartLayoutRef.current
                    ? chartLayoutRef.current.getBoundingClientRect()
                    : null;
                setChartPanelFloatingRect((previousRect) => clampFloatingChartRect({
                    left: previousRect && Number.isFinite(previousRect.left) ? previousRect.left : 24,
                    top: previousRect && Number.isFinite(previousRect.top) ? previousRect.top : 24,
                    width: previousRect && Number.isFinite(previousRect.width) ? previousRect.width : chartPanelWidth,
                    height: previousRect && Number.isFinite(previousRect.height) ? previousRect.height : Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, chartPanelGraphHeight + 180),
                }, containerRect));
            } else {
                setChartPanelWidth((previousWidth) => clampChartPanelWidth(
                    chartPanelFloatingRect && Number.isFinite(chartPanelFloatingRect.width)
                        ? chartPanelFloatingRect.width
                        : previousWidth
                ));
            }
            return nextFloating;
        });
    }, [chartPanelFloatingRect, chartPanelGraphHeight, chartPanelWidth, clampChartPanelWidth]);

    const handleStartChartPanelFloatingDrag = useCallback((event) => {
        if (!chartPanelFloating || !chartLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        chartPanelFloatingDragRef.current = {
            startX: event.clientX,
            startY: event.clientY,
            rect: chartPanelFloatingRect,
        };
    }, [chartPanelFloating, chartPanelFloatingRect]);

    const handleStartChartPanelFloatingResize = useCallback((direction, event) => {
        if (!chartPanelFloating || !chartLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        chartPanelFloatingResizeRef.current = {
            direction,
            startX: event.clientX,
            startY: event.clientY,
            rect: chartPanelFloatingRect,
        };
    }, [chartPanelFloating, chartPanelFloatingRect]);

    const handleToggleChartCanvasPaneFloating = useCallback((paneId) => {
        const containerRect = chartLayoutRef.current
            ? chartLayoutRef.current.getBoundingClientRect()
            : null;
        updateChartCanvasPane(paneId, (pane) => {
            const nextFloating = !pane.floating;
            const baseRect = pane.floatingRect || {};
            return {
                ...pane,
                floating: nextFloating,
                floatingRect: clampFloatingChartRect({
                    left: Number.isFinite(baseRect.left) ? baseRect.left : 36,
                    top: Number.isFinite(baseRect.top) ? baseRect.top : 36,
                    width: Number.isFinite(baseRect.width) ? baseRect.width : pane.width,
                    height: Number.isFinite(baseRect.height) ? baseRect.height : Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, (pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + 180),
                }, containerRect),
            };
        });
    }, [updateChartCanvasPane]);

    const handleStartChartCanvasPaneFloatingDrag = useCallback((paneId, event) => {
        if (!chartLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        const targetPane = chartCanvasPanes.find((pane) => pane.id === paneId);
        if (!targetPane || !targetPane.floating) return;
        chartCanvasFloatingDragRef.current = {
            paneId,
            startX: event.clientX,
            startY: event.clientY,
            rect: targetPane.floatingRect || clampFloatingChartRect({}, null),
        };
    }, [chartCanvasPanes]);

    const handleStartChartCanvasPaneFloatingResize = useCallback((paneId, direction, event) => {
        if (!chartLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        const targetPane = chartCanvasPanes.find((pane) => pane.id === paneId);
        if (!targetPane || !targetPane.floating) return;
        chartCanvasFloatingResizeRef.current = {
            paneId,
            direction,
            startX: event.clientX,
            startY: event.clientY,
            rect: targetPane.floatingRect || clampFloatingChartRect({}, null),
        };
    }, [chartCanvasPanes]);

    useEffect(() => {
        const handlePointerMove = (event) => {
            if (chartPanelFloatingDragRef.current) {
                const dragState = chartPanelFloatingDragRef.current;
                const containerRect = chartLayoutRef.current
                    ? chartLayoutRef.current.getBoundingClientRect()
                    : null;
                const nextRect = clampFloatingChartRect({
                    ...dragState.rect,
                    left: dragState.rect.left + (event.clientX - dragState.startX),
                    top: dragState.rect.top + (event.clientY - dragState.startY),
                }, containerRect);
                setChartPanelFloatingRect(nextRect);
                return;
            }

            if (chartPanelFloatingResizeRef.current) {
                const resizeState = chartPanelFloatingResizeRef.current;
                const containerRect = chartLayoutRef.current
                    ? chartLayoutRef.current.getBoundingClientRect()
                    : null;
                const deltaX = event.clientX - resizeState.startX;
                const deltaY = event.clientY - resizeState.startY;
                const nextRectDraft = { ...resizeState.rect };
                if (resizeState.direction === 'right' || resizeState.direction === 'corner') {
                    nextRectDraft.width = resizeState.rect.width + deltaX;
                }
                if (resizeState.direction === 'bottom' || resizeState.direction === 'corner') {
                    nextRectDraft.height = resizeState.rect.height + deltaY;
                }
                const nextRect = clampFloatingChartRect(nextRectDraft, containerRect);
                setChartPanelFloatingRect(nextRect);
                setChartPanelWidth(nextRect.width);
                setChartPanelGraphHeight((previousHeight) => {
                    const targetHeight = Math.max(180, nextRect.height - 180);
                    return previousHeight === targetHeight ? previousHeight : targetHeight;
                });
                return;
            }

            if (chartCanvasFloatingDragRef.current) {
                const dragState = chartCanvasFloatingDragRef.current;
                const containerRect = chartLayoutRef.current
                    ? chartLayoutRef.current.getBoundingClientRect()
                    : null;
                const nextRect = clampFloatingChartRect({
                    ...dragState.rect,
                    left: dragState.rect.left + (event.clientX - dragState.startX),
                    top: dragState.rect.top + (event.clientY - dragState.startY),
                }, containerRect);
                updateChartCanvasPane(dragState.paneId, { floatingRect: nextRect });
                return;
            }

            if (chartCanvasFloatingResizeRef.current) {
                const resizeState = chartCanvasFloatingResizeRef.current;
                const containerRect = chartLayoutRef.current
                    ? chartLayoutRef.current.getBoundingClientRect()
                    : null;
                const deltaX = event.clientX - resizeState.startX;
                const deltaY = event.clientY - resizeState.startY;
                const nextRectDraft = { ...resizeState.rect };
                if (resizeState.direction === 'right' || resizeState.direction === 'corner') {
                    nextRectDraft.width = resizeState.rect.width + deltaX;
                }
                if (resizeState.direction === 'bottom' || resizeState.direction === 'corner') {
                    nextRectDraft.height = resizeState.rect.height + deltaY;
                }
                const nextRect = clampFloatingChartRect(nextRectDraft, containerRect);
                updateChartCanvasPane(resizeState.paneId, (pane) => ({
                    ...pane,
                    width: nextRect.width,
                    chartHeight: Math.max(180, nextRect.height - 180),
                    floatingRect: nextRect,
                }));
            }
        };

        const stopFloatingInteraction = () => {
            chartPanelFloatingDragRef.current = null;
            chartPanelFloatingResizeRef.current = null;
            chartCanvasFloatingDragRef.current = null;
            chartCanvasFloatingResizeRef.current = null;
        };

        window.addEventListener('mousemove', handlePointerMove);
        window.addEventListener('mouseup', stopFloatingInteraction);
        window.addEventListener('mouseleave', stopFloatingInteraction);
        window.addEventListener('blur', stopFloatingInteraction);

        return () => {
            window.removeEventListener('mousemove', handlePointerMove);
            window.removeEventListener('mouseup', stopFloatingInteraction);
            window.removeEventListener('mouseleave', stopFloatingInteraction);
            window.removeEventListener('blur', stopFloatingInteraction);
        };
    }, [updateChartCanvasPane]);

    useLayoutEffect(() => {
        if (!chartPanelFloating) return undefined;
        const syncFloatingRect = () => {
            const containerRect = chartLayoutRef.current
                ? chartLayoutRef.current.getBoundingClientRect()
                : null;
            setChartPanelFloatingRect((previousRect) => {
                const nextRect = clampFloatingChartRect(previousRect, containerRect);
                const previousJson = JSON.stringify(previousRect);
                const nextJson = JSON.stringify(nextRect);
                return previousJson === nextJson ? previousRect : nextRect;
            });
        };

        syncFloatingRect();

        if (typeof ResizeObserver === 'undefined' || !chartLayoutRef.current) {
            window.addEventListener('resize', syncFloatingRect);
            return () => window.removeEventListener('resize', syncFloatingRect);
        }

        const observer = new ResizeObserver(syncFloatingRect);
        observer.observe(chartLayoutRef.current);
        return () => observer.disconnect();
    }, [chartPanelFloating]);

    useLayoutEffect(() => {
        const hasFloatingPane = chartCanvasPanes.some((pane) => pane.floating);
        if (!hasFloatingPane) return undefined;

        const syncFloatingPanes = () => {
            const containerRect = chartLayoutRef.current
                ? chartLayoutRef.current.getBoundingClientRect()
                : null;
            setChartCanvasPanes((previousPanes) => {
                let mutated = false;
                const nextPanes = previousPanes.map((pane) => {
                    if (!pane.floating) return pane;
                    const nextRect = clampFloatingChartRect(pane.floatingRect, containerRect);
                    const previousRect = pane.floatingRect || {};
                    const sameRect = previousRect.left === nextRect.left
                        && previousRect.top === nextRect.top
                        && previousRect.width === nextRect.width
                        && previousRect.height === nextRect.height;
                    if (sameRect) return pane;
                    mutated = true;
                    return { ...pane, floatingRect: nextRect };
                });
                return mutated ? nextPanes : previousPanes;
            });
        };

        syncFloatingPanes();

        if (typeof ResizeObserver === 'undefined' || !chartLayoutRef.current) {
            window.addEventListener('resize', syncFloatingPanes);
            return () => window.removeEventListener('resize', syncFloatingPanes);
        }

        const observer = new ResizeObserver(syncFloatingPanes);
        observer.observe(chartLayoutRef.current);
        return () => observer.disconnect();
    }, [chartCanvasPanes]);

    // Clipboard Paste
    useEffect(() => {
        const handlePaste = (e) => {
            if (!lastSelected) return;
            e.preventDefault();
            const clipboardData = e.clipboardData.getData('text');
            const rows = clipboardData.split(/\r\n|\n/).map(r => r.split('\t'));

            if (setPropsRef.current && lastSelected.rowIndex !== undefined && lastSelected.colIndex !== undefined) {
                const visibleLeafColumns = (tableRef.current && tableRef.current.getVisibleLeafColumns) ? tableRef.current.getVisibleLeafColumns() : [];
                const visibleRows = getDisplayRows();

                const startRow = visibleRows[lastSelected.rowIndex];
                const startCol = visibleLeafColumns[lastSelected.colIndex];

                if (startRow && startCol) {
                    setPropsRef.current({
                        paste: {
                            startRowId: startRow.id,
                            startColId: startCol.id,
                            data: rows
                        }
                    });
                }
            }
        };

        window.addEventListener('paste', handlePaste);
        return () => window.removeEventListener('paste', handlePaste);
    }, [lastSelected]);

    // Validation helper
    const validateCell = (val, rule) => {
        if (!rule) return true;
        if (rule.type === 'regex') return new RegExp(rule.pattern).test(val);
        if (rule.type === 'numeric') return !isNaN(parseFloat(val));
        if (rule.type === 'required') return val !== null && val !== '' && val !== undefined;
        return true;
    };

    const getRuleBasedStyle = (colId, value) => {
        if (typeof value !== 'number') return {};
        const rules = conditionalFormatting.filter(r => r.column === colId || !r.column);
        let style = {};
        for (const rule of rules) {
            let match = false;
            if (rule.condition === '>') match = value > rule.value;
            else if (rule.condition === '<') match = value < rule.value;
            else if (rule.condition === '>=') match = value >= rule.value;
            else if (rule.condition === '<=') match = value <= rule.value;
            else if (rule.condition === '==') match = value === rule.value;

            if (match) {
                style = { ...style, ...rule.style };
            }
        }
        return style;
    };

    const handleKeyDown = (e) => {
        const visibleLeafColumnsAll = (tableRef.current && tableRef.current.getVisibleLeafColumns) ? tableRef.current.getVisibleLeafColumns() : [];
        const visibleRowsAll = getDisplayRows();

        // Ctrl+A: select all visible rows and columns
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
            e.preventDefault();
            const allSelection = {};
            visibleRowsAll.forEach(r => {
                visibleLeafColumnsAll.forEach(c => {
                    allSelection[`${r.id}:${c.id}`] = r.getValue(c.id);
                });
            });
            setSelectedCells(allSelection);
            if (visibleRowsAll.length > 0 && visibleLeafColumnsAll.length > 0) {
                setLastSelected({ rowIndex: 0, colIndex: 0 });
                setDragStart({ rowIndex: 0, colIndex: 0 });
            }
            return;
        }

        // Ctrl+C: copy selected cells as TSV
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') {
            const keys = Object.keys(selectedCells);
            if (keys.length === 0) return;
            e.preventDefault();
            const data = getSelectedData(false);
            if (data) {
                copyToClipboard(data);
                showNotification('Copied!', 'success');
            }
            return;
        }

        if (!lastSelected) return;

        const { rowIndex, colIndex } = lastSelected;
        let nextRow = rowIndex;
        let nextCol = colIndex;

        const visibleLeafColumns = visibleLeafColumnsAll;

        // Helper: Excel-style Ctrl+Arrow — jumps to edge of contiguous data block.
        // Rules (mirroring Excel):
        //   - current cell empty  → jump to next non-empty in direction (or edge if all empty)
        //   - current non-empty, next is empty → jump to next non-empty past the gap (or edge)
        //   - current non-empty, next is non-empty → jump to last non-empty before a gap (end of block)
        const ctrlArrowRow = (dir) => {
            const colId = (visibleLeafColumns[colIndex] && visibleLeafColumns[colIndex].id);
            if (!colId) return dir > 0 ? visibleRowsAll.length - 1 : 0;
            const isEmpty = (r) => { const v = (r && r.getValue(colId)); return v === null || v === undefined || v === ''; };
            const curEmpty = isEmpty(visibleRowsAll[rowIndex]);
            if (curEmpty) {
                // jump to next non-empty
                for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleRowsAll[i])) return i;
                }
                return dir > 0 ? visibleRowsAll.length - 1 : 0;
            }
            const nextIdx = rowIndex + dir;
            if (nextIdx < 0 || nextIdx >= visibleRowsAll.length) return dir > 0 ? visibleRowsAll.length - 1 : 0;
            if (isEmpty(visibleRowsAll[nextIdx])) {
                // next is empty — jump past gap to next non-empty
                for (let i = nextIdx + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleRowsAll[i])) return i;
                }
                return dir > 0 ? visibleRowsAll.length - 1 : 0;
            }
            // next is non-empty — find end of contiguous block
            let last = rowIndex;
            for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                if (isEmpty(visibleRowsAll[i])) break;
                last = i;
            }
            return last;
        };

        const ctrlArrowCol = (dir) => {
            const rowObj = visibleRowsAll[rowIndex];
            if (!rowObj) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            const isEmpty = (c) => { const v = rowObj.getValue(c && c.id); return v === null || v === undefined || v === ''; };
            const curEmpty = isEmpty(visibleLeafColumns[colIndex]);
            if (curEmpty) {
                for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleLeafColumns[i])) return i;
                }
                return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            }
            const nextIdx = colIndex + dir;
            if (nextIdx < 0 || nextIdx >= visibleLeafColumns.length) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            if (isEmpty(visibleLeafColumns[nextIdx])) {
                for (let i = nextIdx + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleLeafColumns[i])) return i;
                }
                return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            }
            let last = colIndex;
            for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                if (isEmpty(visibleLeafColumns[i])) break;
                last = i;
            }
            return last;
        };

        if (e.key.startsWith('Arrow')) {
            e.preventDefault();
            if (e.ctrlKey || e.metaKey) {
                if (e.key === 'ArrowDown')  nextRow = ctrlArrowRow(1);
                else if (e.key === 'ArrowUp')   nextRow = ctrlArrowRow(-1);
                else if (e.key === 'ArrowRight') nextCol = ctrlArrowCol(1);
                else if (e.key === 'ArrowLeft')  nextCol = ctrlArrowCol(-1);
            } else {
                if (e.key === 'ArrowUp')    nextRow = Math.max(0, rowIndex - 1);
                else if (e.key === 'ArrowDown')  nextRow = Math.min(visibleRowsAll.length - 1, rowIndex + 1);
                else if (e.key === 'ArrowLeft')  nextCol = Math.max(0, colIndex - 1);
                else if (e.key === 'ArrowRight') nextCol = Math.min(visibleLeafColumns.length - 1, colIndex + 1);
            }
        } else if (e.key === 'Tab') {
            e.preventDefault();
            nextCol = e.shiftKey ? Math.max(0, colIndex - 1) : Math.min(visibleLeafColumns.length - 1, colIndex + 1);
        } else {
            return;
        }

        const nextRowObj = visibleRowsAll[nextRow];
        const nextColObj = visibleLeafColumns[nextCol];

        if (nextRowObj && nextColObj) {
            const key = `${nextRowObj.id}:${nextColObj.id}`;
            const val = nextRowObj.getValue(nextColObj.id);

            if (e.shiftKey && e.key.startsWith('Arrow')) {
                 const newRange = selectRange(dragStart || lastSelected, { rowIndex: nextRow, colIndex: nextCol });
                 setSelectedCells(newRange);
            } else {
                 setSelectedCells({ [key]: val });
                 setDragStart({ rowIndex: nextRow, colIndex: nextCol });
            }
            setLastSelected({ rowIndex: nextRow, colIndex: nextCol });

            // Scroll into view if needed
            scrollToDisplayRow(nextRow);
            scrollToDisplayColumn(nextCol);
        }
    };

    const selectRange = (start, end) => {
        const rStart = Math.min(start.rowIndex, end.rowIndex);
        const rEnd = Math.max(start.rowIndex, end.rowIndex);
        const cStart = Math.min(start.colIndex, end.colIndex);
        const cEnd = Math.max(start.colIndex, end.colIndex);

        const visibleRows = getDisplayRows();
        const visibleCols = table.getVisibleLeafColumns();
        const newSelection = {};

        for (let r = rStart; r <= rEnd; r++) {
            for (let c = cStart; c <= cEnd; c++) {
                const rRow = visibleRows[r];
                const cCol = visibleCols[c];
                if (rRow && cCol) {
                    newSelection[`${rRow.id}:${cCol.id}`] = rRow.getValue(cCol.id);
                }
            }
        }
        return newSelection;
    };

    const [isRowSelecting, setIsRowSelecting] = useState(false);
    const [rowDragStart, setRowDragStart] = useState(null);

    // Stop row selection on mouse up
    useEffect(() => {
        const handleMouseUp = () => {
            setIsRowSelecting(false);
            setRowDragStart(null);
        };
        window.addEventListener('mouseup', handleMouseUp);
        return () => window.removeEventListener('mouseup', handleMouseUp);
    }, []);

    const handleRowRangeSelect = useCallback((startIdx, endIdx) => {
        if (!tableRef.current) return;
        const visibleCols = tableRef.current.getVisibleLeafColumns();
        const rows = getDisplayRows();
        const min = Math.min(startIdx, endIdx);
        const max = Math.max(startIdx, endIdx);
        
        const rangeSelection = {};
        for(let i=min; i<=max; i++) {
            const r = rows[i];
            if(r) {
                visibleCols.forEach(col => {
                    rangeSelection[`${r.id}:${col.id}`] = r.getValue(col.id);
                });
            }
        }
        // Merge with existing if ctrl held? No, drag usually replaces or extends from anchor.
        // For simplicity, let's just set selection to this range.
        setSelectedCells(rangeSelection);
    }, [getDisplayRows]);

    const handleRowSelect = useCallback((row, isShift, isCtrl) => {
        if (!tableRef.current) return;
        const visibleCols = tableRef.current.getVisibleLeafColumns();
        const rowId = row.id;
        const displayRowIndex = resolveDisplayRowIndex(rowId, row.index);
        const newSelection = {};
        
        visibleCols.forEach((col) => {
            newSelection[`${rowId}:${col.id}`] = row.getValue(col.id);
        });

        if (isCtrl) {
            setSelectedCells(prev => ({...prev, ...newSelection}));
            setLastSelected({ rowIndex: displayRowIndex, colIndex: 0 });
        } else if (isShift && lastSelected) {
             const startRowIndex = lastSelected.rowIndex;
             const endRowIndex = displayRowIndex;
             const rows = getDisplayRows();
             const min = Math.min(startRowIndex, endRowIndex);
             const max = Math.max(startRowIndex, endRowIndex);
             
             const rangeSelection = {};
             for(let i=min; i<=max; i++) {
                 const r = rows[i];
                 if(r) {
                    visibleCols.forEach(col => {
                        rangeSelection[`${r.id}:${col.id}`] = r.getValue(col.id);
                    });
                 }
             }
             setSelectedCells(rangeSelection);
        } else {
            setSelectedCells(newSelection);
            setLastSelected({ rowIndex: displayRowIndex, colIndex: 0 });
        }
    }, [getDisplayRows, lastSelected, resolveDisplayRowIndex]);

    const handleCellMouseDown = useCallback((e, rowIndex, colIndex, rowId, colId, value) => {
        if (e.button === 2) return; // Ignore right-click

        if (e.shiftKey) {
            e.preventDefault(); // Prevent text selection
            const start = lastSelected || { rowIndex, colIndex };
            const newSelection = selectRange(start, { rowIndex, colIndex });
            // Merge if ctrl key, else replace
            if (e.ctrlKey || e.metaKey) {
                setSelectedCells(prev => ({...prev, ...newSelection}));
            } else {
                setSelectedCells(newSelection);
            }
            return;
        }

        setIsDragging(true);
        setDragStart({ rowIndex, colIndex });
        setLastSelected({ rowIndex, colIndex });
        // Track hovered row path for Format Row feature
        let clickedRow = null;
        if (tableRef.current && typeof tableRef.current.getRow === 'function') {
            try {
                clickedRow = tableRef.current.getRow(rowId, true);
            } catch (err) {
                clickedRow = null;
            }
        }
        if (!clickedRow && tableRef.current && tableRef.current.getRowModel) {
            clickedRow = getDisplayRows()[rowIndex] || null;
        }
        if (clickedRow && clickedRow.original && clickedRow.original._path) {
            setHoveredRowPath(clickedRow.original._path);
        }

        const key = `${rowId}:${colId}`;
        if (e.ctrlKey || e.metaKey) {
             const newSelection = { ...selectedCells };
             newSelection[key] = value;
             setSelectedCells(newSelection);
        } else {
            // Clear and start new
            setSelectedCells({ [key]: value });
        }
    }, [getDisplayRows, lastSelected, selectedCells]);

    const handleCellMouseEnter = (rowIndex, colIndex) => {
        if (isDragging && dragStart) {
             const newRange = selectRange(dragStart, { rowIndex, colIndex });
             setSelectedCells(newRange); 
        }
        if (isFilling && dragStart) {
            const rStart = Math.min(dragStart.rowIndex, rowIndex);
            const rEnd = Math.max(dragStart.rowIndex, rowIndex);
            const cStart = Math.min(dragStart.colIndex, colIndex);
            const cEnd = Math.max(dragStart.colIndex, colIndex);
            setFillRange({ rStart, rEnd, cStart, cEnd });
        }
    };

    const handleFillMouseDown = (e) => {
        e.stopPropagation();
        e.preventDefault();
        setIsFilling(true);
    };

    const handleFillMouseUp = () => {
        if (isFilling && fillRange && setProps) {
            const displayRows = getDisplayRows();
            const startRow = displayRows[dragStart.rowIndex];
            const startValue = startRow ? startRow.getVisibleCells()[dragStart.colIndex].getValue() : undefined;
            const updates = [];
            const visibleRows = displayRows;
            const visibleCols = table.getVisibleLeafColumns();
            for (let r = fillRange.rStart; r <= fillRange.rEnd; r++) {
                for (let c = fillRange.cStart; c <= fillRange.cEnd; c++) {
                    const row = visibleRows[r];
                    const col = visibleCols[c];
                    if (row && col) {
                        updates.push({ rowId: row.id, colId: col.id, value: startValue });
                    }
                }
            }
            if (updates.length > 0) {
                setProps({ cellUpdates: updates });
            }
        }
        setIsFilling(false);
        setFillRange(null);
    };

    useEffect(() => {
        const handleMouseUp = () => {
            setIsDragging(false);
            setDragStart(null);
            if (isFilling) {
                handleFillMouseUp();
            }
        };
        window.addEventListener('mouseup', handleMouseUp);
        return () => window.removeEventListener('mouseup', handleMouseUp);
    }, [isDragging, isFilling, fillRange, dragStart]);
    
    // Global Ctrl+C is handled inside handleKeyDown (attached to table container)
    // This window-level listener acts as fallback when the table isn't focused
    useEffect(() => {
        const handleGlobalCopy = (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
                const keys = Object.keys(selectedCells);
                if (keys.length === 0) return;
                e.preventDefault();
                const data = getSelectedData(false);
                if (data) {
                    copyToClipboard(data);
                    showNotification('Copied!', 'success');
                }
            }
        };
        window.addEventListener('keydown', handleGlobalCopy);
        return () => window.removeEventListener('keydown', handleGlobalCopy);
    }, [selectedCells]);

    const toggleCol = (key) => {
        setColExpanded(prev => ({
            ...prev,
            [key]: prev[key] === undefined ? false : !prev[key]
        }));
    };

    const isColExpanded = (key) => colExpanded[key] !== false;

    const [dragItem, setDragItem] = useState(null);
    const [dropLine, setDropLine] = useState(null);
    const sessionIdRef = useRef(getOrCreateSessionId(id || 'pivot-grid'));
    const clientInstanceRef = useRef(createClientInstanceId(id || 'pivot-grid'));
    const requestVersionRef = useRef(Number(dataVersion) || 0);
    const latestDataVersionRef = useRef(Number(dataVersion) || 0);
    const pendingRequestVersionsRef = useRef(new Set());
    const loadingDelayTimerRef = useRef(null);
    const stateEpochRef = useRef(0);
    const abortGenerationRef = useRef(0);
    const structuralPendingVersionRef = useRef(null);
    const expandAllDebounceRef = useRef(false);
    const latestViewportRef = useRef({ start: 0, end: 99, count: 100 });
    const [stateEpoch, setStateEpoch] = useState(0);
    const [cachedColSchema, setCachedColSchema] = useState(null);
    const colSchemaEpochRef = useRef(-1);
    const [visibleColRange, setVisibleColRange] = useState({ start: 0, end: 0 });
    const colRequestStartRef = useRef(null);
    const colRequestEndRef = useRef(null);
    const needsColSchemaRef = useRef(true);
    const [abortGeneration, setAbortGeneration] = useState(0);
    const [structuralInFlight, setStructuralInFlight] = useState(false);
    const [pendingRowTransitions, setPendingRowTransitions] = useState(() => new Map());
    const [pendingColumnSkeletonCount, setPendingColumnSkeletonCount] = useState(0);
    const pendingHorizontalRequestVersionsRef = useRef(new Set());
    const [pendingHorizontalColumnCount, setPendingHorizontalColumnCount] = useState(0);
    const [isHorizontalColumnRequestPending, setIsHorizontalColumnRequestPending] = useState(false);
    const [isRequestPending, setIsRequestPending] = useState(false);

    const markRequestPending = useCallback((requestMeta) => {
        const normalizedMeta = requestMeta && typeof requestMeta === 'object'
            ? requestMeta
            : { version: requestMeta };
        const numericVersion = Number(normalizedMeta.version);
        if (Number.isFinite(numericVersion)) {
            pendingRequestVersionsRef.current.add(numericVersion);
        }
        if (
            Number.isFinite(numericVersion) &&
            normalizedMeta.columnRangeChanged &&
            normalizedMeta.hasColumnWindow
        ) {
            pendingHorizontalRequestVersionsRef.current.add(numericVersion);
            setIsHorizontalColumnRequestPending(true);
            setPendingHorizontalColumnCount(Math.max(
                1,
                Math.min(
                    normalizedMeta.columnDeltaCount || normalizedMeta.visibleColumnCount || 1,
                    Math.max(normalizedMeta.visibleColumnCount || 1, 6)
                )
            ));
        }
        if (isRequestPending || loadingDelayTimerRef.current !== null) return;
        loadingDelayTimerRef.current = setTimeout(() => {
            loadingDelayTimerRef.current = null;
            if (pendingRequestVersionsRef.current.size > 0) {
                setIsRequestPending(true);
            }
        }, 200);
    }, [isRequestPending]);

    useEffect(() => {
        const numericVersion = Number(dataVersion);
        if (!Number.isFinite(numericVersion)) return;
        latestDataVersionRef.current = numericVersion;
        if (numericVersion > requestVersionRef.current) {
            requestVersionRef.current = numericVersion;
        }
        for (const pendingVersion of Array.from(pendingRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                pendingRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        for (const pendingVersion of Array.from(pendingHorizontalRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                pendingHorizontalRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        if (pendingRequestVersionsRef.current.size === 0) {
            if (loadingDelayTimerRef.current !== null) {
                clearTimeout(loadingDelayTimerRef.current);
                loadingDelayTimerRef.current = null;
            }
            setIsRequestPending(false);
        }
        if (pendingHorizontalRequestVersionsRef.current.size === 0) {
            setIsHorizontalColumnRequestPending(false);
            setPendingHorizontalColumnCount(0);
        }
    }, [dataVersion]);

    useEffect(() => {
        return () => {
            if (loadingDelayTimerRef.current !== null) {
                clearTimeout(loadingDelayTimerRef.current);
                loadingDelayTimerRef.current = null;
            }
        };
    }, []);

    useEffect(() => {
        if (!isRequestPending) return;
        const timeoutId = setTimeout(() => {
            pendingRequestVersionsRef.current.clear();
            pendingHorizontalRequestVersionsRef.current.clear();
            setIsRequestPending(false);
            setIsHorizontalColumnRequestPending(false);
            setPendingHorizontalColumnCount(0);
        }, 15000);
        return () => clearTimeout(timeoutId);
    }, [isRequestPending]);

    useEffect(() => {
        if (!serverSide) return;
        pendingHorizontalRequestVersionsRef.current.clear();
        setIsHorizontalColumnRequestPending(false);
        setPendingHorizontalColumnCount(0);
    }, [serverSide, stateEpoch]);

    // Clear schema on structural change so we re-derive from fresh data
    useEffect(() => {
        if (!serverSide) return;
        setCachedColSchema(null);
    }, [stateEpoch, serverSide]);

    // Extract authoritative col_schema embedded by the server as a sentinel entry in props.columns.
    // This is more robust than inferring schema from row keys (handles windowed responses correctly).
    useEffect(() => {
        if (!serverSide || !props.columns) return;
        const schemaEntry = props.columns.find(c => c.id === '__col_schema');
        if (schemaEntry && schemaEntry.col_schema) {
            setCachedColSchema(schemaEntry.col_schema);
            colSchemaEpochRef.current = stateEpoch;
        }
    }, [serverSide, props.columns, stateEpoch]);

    // Derive schema from row keys — only used in client-side mode.
    // In server-side mode the authoritative schema always comes from the __col_schema sentinel
    // embedded in props.columns by the server.  Allowing row-key inference in server-side mode
    // risks schema drift on windowed/partial payloads (only the visible col slice is present).
    useEffect(() => {
        if (serverSide || cachedColSchema) return;
        if (!filteredData || filteredData.length === 0) return;
        const rowMetaKeys = new Set(['_id', '_path', '_isTotal', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'depth', 'uuid', 'subRows', '__virtualIndex']);
        const ignoredIds = new Set([...rowFields, ...colFields, '_isTotal']);
        const colIds = [];
        const colIdSet = new Set();
        for (const row of filteredData) {
            if (!row) continue;
            for (const key of Object.keys(row)) {
                if (!colIdSet.has(key) && !rowMetaKeys.has(key) && !ignoredIds.has(key)) {
                    colIds.push(key);
                    colIdSet.add(key);
                }
            }
        }
        if (colIds.length > 0) {
            setCachedColSchema({
                total_center_cols: colIds.length,
                columns: colIds.map((id, i) => ({ index: i, id, size: defaultColumnWidths.schemaFallback }))
            });
            colSchemaEpochRef.current = stateEpoch;
        }
    }, [serverSide, filteredData, cachedColSchema, stateEpoch, rowFields, colFields, defaultColumnWidths.schemaFallback]);

    const needsColSchema = !cachedColSchema || colSchemaEpochRef.current !== stateEpoch;
    const totalCenterCols = cachedColSchema ? cachedColSchema.total_center_cols : null;

    // Always request all center columns to avoid feedback loop where fewer
    // columns → smaller table → smaller visible range → even fewer columns.
    // Column virtualization handles render-side efficiency.
    const colRequestStart = (serverSide && cachedColSchema && !needsColSchema && totalCenterCols !== null)
        ? Math.max(0, Math.min(visibleColRange.start, Math.max(totalCenterCols - 1, 0)))
        : null;

    const colRequestEnd = (serverSide && cachedColSchema && !needsColSchema && totalCenterCols !== null)
        ? Math.max(
            Math.max(0, Math.min(visibleColRange.start, Math.max(totalCenterCols - 1, 0))),
            Math.max(0, Math.min(visibleColRange.end, Math.max(totalCenterCols - 1, 0)))
        )
        : null;

    // Keep refs in sync for use in field-zone effect closures
    colRequestStartRef.current = colRequestStart;
    colRequestEndRef.current = colRequestEnd;
    needsColSchemaRef.current = needsColSchema;

    const beginStructuralTransaction = useCallback(() => {
        stateEpochRef.current += 1;
        abortGenerationRef.current += 1;
        const baselineVersion = Math.max(requestVersionRef.current, latestDataVersionRef.current);
        const nextVersion = baselineVersion + 1;
        requestVersionRef.current = nextVersion;

        setStateEpoch(stateEpochRef.current);
        setAbortGeneration(abortGenerationRef.current);
        setStructuralInFlight(true);
        structuralPendingVersionRef.current = {
            version: nextVersion,
            startDataVersion: latestDataVersionRef.current
        };

        return {
            stateEpoch: stateEpochRef.current,
            abortGeneration: abortGenerationRef.current,
            version: nextVersion
        };
    }, []);

    // Lightweight expansion request: clears inflight (via abortGeneration bump) but
    // does NOT change stateEpoch, so the existing cache stays valid and rows remain
    // visible instead of flashing to skeletons.
    const beginExpansionRequest = useCallback(() => {
        abortGenerationRef.current += 1;
        const newVersion = requestVersionRef.current + 1;
        requestVersionRef.current = newVersion;
        setAbortGeneration(abortGenerationRef.current);
        return {
            abortGeneration: abortGenerationRef.current,
            stateEpoch: stateEpochRef.current,
            version: newVersion
        };
    }, []);

    const setPropsRef = useRef(setProps);
    useEffect(() => {
        setPropsRef.current = setProps;
    }, [setProps]);

    const buildChartStateOverrideSnapshot = useCallback(() => {
        const normalizedFilters = (() => {
            const next = { ...(filters || {}) };
            delete next.__request_unique__;
            return next;
        })();
        return cloneSerializable({
            rowFields,
            colFields,
            valConfigs,
            filters: normalizedFilters,
            sorting,
            sortOptions: effectiveSortOptions,
            expanded,
            showRowTotals,
            showColTotals,
        }, null);
    }, [
        rowFields,
        colFields,
        valConfigs,
        filters,
        sorting,
        effectiveSortOptions,
        expanded,
        showRowTotals,
        showColTotals,
    ]);

    const buildChartRequestBase = useCallback((config, stateOverride = null) => {
        if (!serverSide || needsColSchema || totalCenterCols === null) return null;
        const requestedRowLimit = Number(config && config.rowLimit);
        const requestedColumnLimit = Number(config && config.columnLimit);
        const requestedSeriesColumnIds = getRequestedChartSeriesColumnIds(
            config && config.chartType,
            config && config.chartLayers
        );
        const needsChartColumnCatalog = Boolean(config && (config.needsColumnCatalog || config.chartType === 'combo'));
        const serverScope = (config && VALID_CHART_SERVER_SCOPES.has(config.serverScope))
            ? config.serverScope
            : (normalizedChartServerWindow.scope || 'viewport');
        const viewportSnapshot = latestViewportRef.current || {
            start: 0,
            end: Math.max((Number.isFinite(requestedRowLimit) ? requestedRowLimit : DEFAULT_CHART_PANEL_ROW_LIMIT) - 1, 0),
            count: Math.max(Number.isFinite(requestedRowLimit) ? requestedRowLimit : DEFAULT_CHART_PANEL_ROW_LIMIT, 1),
        };
        const rowStart = serverScope === 'root'
            ? 0
            : Math.max(0, Number.isFinite(viewportSnapshot.start) ? viewportSnapshot.start : 0);
        const viewportRowCount = Number.isFinite(viewportSnapshot.count)
            ? Math.max(1, viewportSnapshot.count)
            : Math.max(1, ((Number(viewportSnapshot.end) || rowStart) - rowStart + 1));
        const resolvedRowLimit = Math.max(
            viewportRowCount,
            Math.max(1, Number.isFinite(requestedRowLimit) ? Math.floor(requestedRowLimit) : DEFAULT_CHART_PANEL_ROW_LIMIT),
            normalizedChartServerWindow.enabled && normalizedChartServerWindow.rows
                ? normalizedChartServerWindow.rows
                : 0
        );
        const rowEnd = rowStart + resolvedRowLimit - 1;

        const maxCenterIndex = Math.max(totalCenterCols - 1, 0);
        const baseColStart = serverScope === 'root'
            ? 0
            : colRequestStart !== null
                ? colRequestStart
                : Math.max(0, Math.min(visibleColRange.start || 0, maxCenterIndex));
        const viewportColumnCount = (colRequestStart !== null && colRequestEnd !== null)
            ? Math.max(1, colRequestEnd - colRequestStart + 1)
            : Math.max(1, Number.isFinite(requestedColumnLimit) ? Math.floor(requestedColumnLimit) : DEFAULT_CHART_PANEL_COLUMN_LIMIT);
        const resolvedColumnLimit = Math.max(
            viewportColumnCount,
            Math.max(1, Number.isFinite(requestedColumnLimit) ? Math.floor(requestedColumnLimit) : DEFAULT_CHART_PANEL_COLUMN_LIMIT),
            normalizedChartServerWindow.enabled && normalizedChartServerWindow.columns
                ? normalizedChartServerWindow.columns
                : 0
        );
        const colStart = Math.max(0, Math.min(baseColStart, maxCenterIndex));
        const colEnd = Math.max(colStart, Math.min(maxCenterIndex, colStart + resolvedColumnLimit - 1));

        return {
            table: tableName || undefined,
            start: rowStart,
            end: rowEnd,
            count: resolvedRowLimit,
            col_start: colStart,
            col_end: colEnd,
            include_grand_total: showColTotals || undefined,
            cinema_mode: cinemaMode || undefined,
            needs_col_schema: needsChartColumnCatalog || undefined,
            row_limit: resolvedRowLimit,
            column_limit: resolvedColumnLimit,
            series_column_ids: requestedSeriesColumnIds.length > 0 ? requestedSeriesColumnIds : undefined,
            state_override: stateOverride || undefined,
        };
    }, [
        serverSide,
        needsColSchema,
        totalCenterCols,
        normalizedChartServerWindow,
        colRequestStart,
        colRequestEnd,
        visibleColRange.start,
        tableName,
        showColTotals,
    ]);

    const updateChartCanvasPane = useCallback((paneId, updater) => {
        setChartCanvasPanes((previousPanes) => previousPanes.map((pane) => {
            if (pane.id !== paneId) return pane;
            const nextPane = typeof updater === 'function' ? updater(pane) : { ...pane, ...(updater || {}) };
            return normalizeChartCanvasPane(nextPane, initialChartDefinition);
        }));
    }, [initialChartDefinition]);

    const handleAddChartCanvasPane = useCallback(() => {
        const baseDefinition = normalizeChartDefinition({
            id: createChartDefinitionId('chart-pane'),
            name: `Chart Pane ${chartCanvasPanes.length + 1}`,
            chartTitle: chartPanelTitle,
            source: chartPanelSource,
            chartType: chartPanelType,
            chartLayers: chartPanelLayers,
            barLayout: chartPanelBarLayout,
            axisMode: chartPanelAxisMode,
            orientation: chartPanelOrientation,
            hierarchyLevel: chartPanelHierarchyLevel,
            rowLimit: chartPanelRowLimit,
            columnLimit: chartPanelColumnLimit,
            width: chartPanelWidth,
            chartHeight: chartPanelGraphHeight,
            sortMode: chartPanelSortMode,
            interactionMode: chartPanelInteractionMode,
            serverScope: chartPanelServerScope,
        }, initialChartDefinition);
        setChartCanvasPanes((previousPanes) => [
            ...previousPanes,
            {
                ...baseDefinition,
                size: 1,
                floating: false,
                floatingRect: clampFloatingChartRect({
                    width: chartPanelWidth,
                    height: Math.max(DEFAULT_FLOATING_CHART_PANEL_HEIGHT, chartPanelGraphHeight + 180),
                    left: 48 + (chartCanvasPanes.length * 24),
                    top: 48 + (chartCanvasPanes.length * 24),
                }, null),
                locked: false,
                lockedModel: null,
                lockedRequest: null,
                cinemaMode: false,
            },
        ]);
    }, [
        chartCanvasPanes.length,
        chartPanelTitle,
        chartPanelSource,
        chartPanelType,
        chartPanelLayers,
        chartPanelBarLayout,
        chartPanelAxisMode,
        chartPanelOrientation,
        chartPanelHierarchyLevel,
        chartPanelRowLimit,
        chartPanelColumnLimit,
        chartPanelWidth,
        chartPanelGraphHeight,
        chartPanelSortMode,
        chartPanelInteractionMode,
        chartPanelServerScope,
        initialChartDefinition,
    ]);

    const handleRemoveChartCanvasPane = useCallback((paneId) => {
        setChartCanvasPanes((previousPanes) => previousPanes.filter((pane) => pane.id !== paneId));
        setChartPaneDataById((previousData) => {
            if (!Object.prototype.hasOwnProperty.call(previousData, paneId)) return previousData;
            const nextData = { ...previousData };
            delete nextData[paneId];
            return nextData;
        });
        delete completedChartRequestSignaturesRef.current[paneId];
    }, []);

    const buildCurrentViewState = useCallback(() => {
        const normalizedFilters = (() => {
            const next = { ...(filters || {}) };
            delete next.__request_unique__;
            return next;
        })();
        return {
            version: 1,
            table: tableName || null,
            viewport: latestViewportRef.current || null,
            state: {
                rowFields,
                colFields,
                valConfigs,
                filters: normalizedFilters,
                sorting,
                expanded,
                showRowTotals,
                showColTotals,
                showRowNumbers,
                sidebarOpen,
                sidebarTab,
                showFloatingFilters,
                stickyHeaders,
                colSearch,
                colTypeFilter,
                themeName,
                themeOverrides,
                layoutMode,
                colorScaleMode,
                colorPalette,
                spacingMode,
                dataBarsColumns: Array.from(dataBarsColumns || []).sort(),
                pivotColumnSorting,
                columnPinning,
                rowPinning,
                grandTotalPinOverride,
                columnVisibility,
                columnSizing,
                colExpanded,
                decimalPlaces,
                zoomLevel,
                columnDecimalOverrides,
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
                chartDefinitions: managedChartDefinitions.map((definition) => ({
                    ...definition,
                    id: definition.id,
                    name: definition.name,
                })),
                chartCanvasPanes: chartCanvasPanes.map((pane) => ({ ...pane })),
                tableCanvasSize,
                chartServerWindow: normalizedChartServerWindow.enabled
                    ? {
                        rows: normalizedChartServerWindow.rows,
                        columns: normalizedChartServerWindow.columns,
                        scope: normalizedChartServerWindow.scope,
                    }
                    : null,
                scroll: parentRef.current
                    ? { top: parentRef.current.scrollTop, left: parentRef.current.scrollLeft }
                    : null,
            }
        };
    }, [
        tableName,
        rowFields,
        colFields,
        valConfigs,
        filters,
        sorting,
        expanded,
        showRowTotals,
        showColTotals,
        showRowNumbers,
        sidebarOpen,
        sidebarTab,
        showFloatingFilters,
        stickyHeaders,
        colSearch,
        colTypeFilter,
        themeName,
        themeOverrides,
        layoutMode,
        colorScaleMode,
        colorPalette,
        spacingMode,
        dataBarsColumns,
        pivotColumnSorting,
        columnPinning,
        rowPinning,
        grandTotalPinOverride,
        columnVisibility,
        columnSizing,
        colExpanded,
        decimalPlaces,
        zoomLevel,
        columnDecimalOverrides,
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
        managedChartDefinitions,
        chartCanvasPanes,
        tableCanvasSize,
        normalizedChartServerWindow,
    ]);

    const handleSaveView = useCallback(() => {
        const snapshot = buildCurrentViewState();
        if (setPropsRef.current) {
            setPropsRef.current({ savedView: snapshot });
        }
        showNotification('View snapshot saved', 'success');
    }, [buildCurrentViewState, showNotification]);

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
    }, [saveViewTrigger, handleSaveView]);

    const lastPropsRef = useRef({
        rowFields: initialRowFields,
        colFields: initialColFields,
        valConfigs: initialValConfigs,
        filters: {},
        sorting: [],
        sortOptions: effectiveSortOptions,
        expanded: {},
        showRowTotals: initialShowRowTotals,
        showColTotals: initialShowColTotals,
        columnPinning: initialColumnPinning,
        rowPinning: initialRowPinning,
        grandTotalPinOverride,
        serverSidePinsGrandTotal,
        columnVisibility: {},
        columnSizing: {}
    });

    React.useEffect(() => {
        const nextProps = {
            rowFields, colFields, valConfigs, filters, sorting, sortOptions: effectiveSortOptions, expanded,
            cinemaMode, showRowTotals, showColTotals, columnPinning, rowPinning, columnVisibility, columnSizing
        };
        const nextSyncState = {
            ...nextProps,
            grandTotalPinOverride,
            serverSidePinsGrandTotal,
        };
        const colFieldsChanged = JSON.stringify(nextProps.colFields) !== JSON.stringify(lastPropsRef.current.colFields);

        const changedKeys = Object.keys(nextSyncState).filter(key => {
            const val = nextSyncState[key];
            const lastVal = lastPropsRef.current[key];
            return JSON.stringify(val) !== JSON.stringify(lastVal);
        });
        const changed = changedKeys.length > 0;

        if (setPropsRef.current && changed) {
            debugLog('Sync to Dash Triggered', nextProps);

            // Detect expansion-only: only `expanded` changed, no structural fields.
            // In that case we keep the existing cache (no stateEpoch bump) so rows
            // remain visible. A loading row appears below the expanded row via
            // pendingRowTransitions, and the viewport snaps in place.
            const uiOnlyKeys = new Set(['columnPinning', 'rowPinning', 'grandTotalPinOverride', 'columnVisibility', 'columnSizing']);
            const isExpansionOnly = serverSide && changedKeys.length > 0 && changedKeys.every(key => key === 'expanded');
            const isUiOnlyChange = changedKeys.length > 0 && changedKeys.every(key => uiOnlyKeys.has(key));
            const grandTotalPinModeChanged = JSON.stringify(serverSidePinsGrandTotal) !== JSON.stringify(lastPropsRef.current.serverSidePinsGrandTotal);

            lastPropsRef.current = nextSyncState;

            // Column resize/pin/visibility are local UI concerns and should not
            // trigger backend loading or viewport fetches.
            if (isUiOnlyChange && !grandTotalPinModeChanged) {
                setPropsRef.current(nextProps);
                return;
            }

            if (isExpansionOnly) {
                // Cancel any pending scroll restore — the viewport stays exactly in place.
                expansionScrollRestoreRef.current = null;
                if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
                    cancelAnimationFrame(expansionScrollRestoreRafRef.current);
                    expansionScrollRestoreRafRef.current = null;
                }
                const tx = beginExpansionRequest();
                markRequestPending(tx.version);
                const viewportSnapshot = latestViewportRef.current || { start: 0, end: 99, count: 100 };

                // Extend the row window to cover the block immediately after the anchor block.
                // When expanding a row near the END of its block (e.g. row 95 in block 0),
                // new children overflow into block N+1. Without this extension, those rows have
                // no cache entry → they flash with skeleton loaders until a follow-up fetch lands.
                // pendingExpansionRef.current is already set by onExpandedChange (same event,
                // before this effect runs), so anchorBlock is available here.
                const anchorBlockHint = (pendingExpansionRef.current && pendingExpansionRef.current.anchorBlock != null ? pendingExpansionRef.current.anchorBlock : -1);
                const expansionBlockSize = 100; // must match blockSize prop
                const extendedEnd = anchorBlockHint >= 0
                    ? Math.max(viewportSnapshot.end, (anchorBlockHint + 2) * expansionBlockSize - 1)
                    : viewportSnapshot.end;
                const extendedCount = extendedEnd - viewportSnapshot.start + 1;

                // Record the last block the expansion response will cover so the deferred
                // effect knows to start soft-invalidating from the block AFTER it, rather
                // than re-dirtying block N+1 that we just filled with fresh data.
                if (pendingExpansionRef.current) {
                    pendingExpansionRef.current.extendedToBlock =
                        anchorBlockHint >= 0 ? anchorBlockHint + 1 : -1;
                }

                setPropsRef.current({
                    ...nextProps,
                    viewport: {
                        table: tableName || undefined,
                        start: viewportSnapshot.start,
                        end: extendedEnd,
                        count: extendedCount,
                        version: tx.version,
                        window_seq: tx.version,
                        state_epoch: tx.stateEpoch,
                        session_id: sessionIdRef.current,
                        client_instance: clientInstanceRef.current,
                        abort_generation: tx.abortGeneration,
                        intent: 'expansion',
                        col_start: colRequestStartRef.current !== null ? colRequestStartRef.current : undefined,
                        col_end: colRequestEndRef.current !== null ? colRequestEndRef.current : undefined,
                        needs_col_schema: needsColSchemaRef.current && serverSide || undefined,
                        include_grand_total: serverSidePinsGrandTotal || undefined,
                    }
                });
                return;
            }

            // Structural change: full transaction (new stateEpoch clears cache).
            if (serverSide && colFieldsChanged) {
                const prevCount = Array.isArray(lastPropsRef.current.colFields) ? lastPropsRef.current.colFields.length : 0;
                const nextCount = Array.isArray(nextProps.colFields) ? nextProps.colFields.length : 0;
                setPendingColumnSkeletonCount(Math.max(0, nextCount - prevCount));
            } else {
                setPendingColumnSkeletonCount(0);
            }
            const tx = beginStructuralTransaction();
            markRequestPending(tx.version);
            const viewportSnapshot = latestViewportRef.current || { start: 0, end: 99, count: 100 };
            setPropsRef.current({
                ...nextProps,
                viewport: {
                    table: tableName || undefined,
                    start: viewportSnapshot.start,
                    end: viewportSnapshot.end,
                    count: viewportSnapshot.count,
                    version: tx.version,
                    window_seq: tx.version,
                    state_epoch: tx.stateEpoch,
                    session_id: sessionIdRef.current,
                    client_instance: clientInstanceRef.current,
                    abort_generation: tx.abortGeneration,
                    intent: 'structural',
                    needs_col_schema: serverSide || undefined,
                    include_grand_total: serverSidePinsGrandTotal || undefined,
                    cinema_mode: cinemaMode || undefined,
                }
            });
        }
    }, [rowFields, colFields, valConfigs, filters, sorting, effectiveSortOptions, expanded, cinemaMode, showRowTotals, showColTotals, columnPinning, rowPinning, grandTotalPinOverride, columnVisibility, columnSizing, beginStructuralTransaction, beginExpansionRequest, serverSide, tableName, serverSidePinsGrandTotal]);

    useEffect(() => {
        const handleClick = () => setContextMenu(null);
        document.addEventListener('click', handleClick);
        return () => document.removeEventListener('click', handleClick);
    }, []);

    const copyToClipboard = (text) => {
        navigator.clipboard.writeText(text);
    };

    const getSelectedData = (withHeaders = false, selectionMap = selectedCells) => {
        const keys = Object.keys(selectionMap || {});
        if (keys.length === 0) return null;

        const visibleRows = getDisplayRows();
        const visibleCols = table.getVisibleLeafColumns();
        
        // Map indices
        const rowIdMap = {};
        visibleRows.forEach((r, i) => rowIdMap[r.id] = i);
        const colIdMap = {};
        visibleCols.forEach((c, i) => colIdMap[c.id] = i);

        let minR = Infinity, maxR = -1, minC = Infinity, maxC = -1;
        const selectedGrid = {};

        keys.forEach(key => {
            const separatorIndex = key.lastIndexOf(':');
            if (separatorIndex <= 0) return;
            const rid = key.slice(0, separatorIndex);
            const cid = key.slice(separatorIndex + 1);
            const rIdx = rowIdMap[rid];
            const cIdx = colIdMap[cid];
            
            if (rIdx !== undefined && cIdx !== undefined) {
                minR = Math.min(minR, rIdx);
                maxR = Math.max(maxR, rIdx);
                minC = Math.min(minC, cIdx);
                maxC = Math.max(maxC, cIdx);
                if (!selectedGrid[rIdx]) selectedGrid[rIdx] = {};
                selectedGrid[rIdx][cIdx] = selectionMap[key];
            }
        });

        if (minR === Infinity) return null;

        let tsv = "";
        
        if (withHeaders) {
            const headerRow = [];
            for (let c = minC; c <= maxC; c++) {
                headerRow.push(visibleCols[c].columnDef.header);
            }
            tsv += headerRow.join("\t") + "\n";
        }

        for (let r = minR; r <= maxR; r++) {
            const rowVals = [];
            for (let c = minC; c <= maxC; c++) {
                const val = selectedGrid[r] && selectedGrid[r][c];
                rowVals.push(val !== undefined && val !== null ? String(val) : "");
            }
            tsv += rowVals.join("\t") + "\n";
        }
        return tsv;
    };

    const hasSelectionKey = useCallback((selectionMap, selectionKey) => {
        if (!selectionMap || !selectionKey) return false;
        return Object.prototype.hasOwnProperty.call(selectionMap, selectionKey);
    }, []);

    const getSelectedColumnIds = useCallback((selectionMap = selectedCells) => {
        const colIds = new Set();
        Object.keys(selectionMap || {}).forEach((selectionKey) => {
            const separatorIndex = selectionKey.lastIndexOf(':');
            if (separatorIndex <= 0) return;
            const colId = selectionKey.slice(separatorIndex + 1);
            if (colId) colIds.add(colId);
        });
        return Array.from(colIds);
    }, [selectedCells]);

    const getSelectedMeasureIndexes = useCallback((selectedColIds) => {
        const matchedIndexes = [];
        valConfigs.forEach((cfg, idx) => {
            const suffix = `_${cfg.field}_${cfg.agg}`;
            const matches = selectedColIds.some(colId => (
                colId === cfg.field ||
                colId.endsWith(suffix) ||
                colId.includes(`_${cfg.field}_`)
            ));
            if (matches) matchedIndexes.push(idx);
        });
        return matchedIndexes;
    }, [valConfigs]);

    const getDefaultFormatForSelection = useCallback((selectionMap) => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        const matchedIndexes = getSelectedMeasureIndexes(selectedColIds);
        if (matchedIndexes.length === 0) return 'fixed:2';

        const formats = matchedIndexes
            .map(idx => valConfigs[idx] && valConfigs[idx].format)
            .filter(fmt => typeof fmt === 'string' && fmt.trim() !== '')
            .map(fmt => fmt.trim());
        const uniqueFormats = Array.from(new Set(formats));
        if (uniqueFormats.length === 1) return uniqueFormats[0];
        return 'fixed:2';
    }, [getSelectedColumnIds, getSelectedMeasureIndexes, valConfigs]);

    const applyDataBarsFromSelection = useCallback((selectionMap, mode = 'col') => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        if (selectedColIds.length === 0) {
            showNotification('Select at least one value cell first.', 'warning');
            return;
        }

        if (mode === 'off') {
            setDataBarsColumns(new Set());
            setColorScaleMode('off');
            showNotification('Data bars disabled.', 'info');
            return;
        }

        setColorScaleMode(mode);
        setDataBarsColumns(prev => {
            const next = new Set(prev || []);
            selectedColIds.forEach(colId => next.add(colId));
            return next;
        });
        if (mode === 'row') {
            showNotification('Data bars enabled (row mode).', 'success');
        } else if (mode === 'table') {
            showNotification('Data bars enabled (table mode).', 'success');
        } else {
            showNotification('Data bars enabled (column mode).', 'success');
        }
    }, [getSelectedColumnIds, setColorScaleMode, setDataBarsColumns, showNotification]);

    const applyFormatToSelection = useCallback((selectionMap, formatOverride = null) => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        if (selectedColIds.length === 0) {
            showNotification('Select at least one value cell first.', 'warning');
            return;
        }

        let normalizedFormat = '';
        if (formatOverride !== null && formatOverride !== undefined) {
            normalizedFormat = String(formatOverride).trim();
        } else {
            const promptDefault = getDefaultFormatForSelection(selectionMap);
            const formatInput = window.prompt(
                'Format selected values.\nExamples: fixed:2, currency, percent, compact',
                promptDefault
            );
            if (formatInput === null) return;
            normalizedFormat = String(formatInput).trim();
        }

        setValConfigs(prev => {
            let matchedAny = false;
            const matchedIndexes = [];
            prev.forEach((cfg, idx) => {
                const suffix = `_${cfg.field}_${cfg.agg}`;
                const matches = selectedColIds.some(colId => (
                    colId === cfg.field ||
                    colId.endsWith(suffix) ||
                    colId.includes(`_${cfg.field}_`)
                ));
                if (matches) matchedIndexes.push(idx);
            });
            const next = prev.map((cfg, idx) => {
                const matches = matchedIndexes.includes(idx);
                if (!matches) return cfg;
                matchedAny = true;
                if (!normalizedFormat) {
                    const { format, ...rest } = cfg;
                    return rest;
                }
                return { ...cfg, format: normalizedFormat };
            });
            if (!matchedAny) {
                showNotification('No value columns matched the current selection.', 'warning');
                return prev;
            }
            return next;
        });

        if (!normalizedFormat) {
            showNotification('Format cleared for selected values.', 'info');
        } else {
            showNotification(`Format applied: ${normalizedFormat}`, 'success');
        }
    }, [getSelectedColumnIds, getDefaultFormatForSelection, setValConfigs, showNotification]);

    const getPinningState = (colId) => {
        const { left, right } = columnPinning;
        if ((left || []).includes(colId)) return 'left';
        if ((right || []).includes(colId)) return 'right';
        return false;
    };

    const getPivotColumnSortField = useCallback((header, level = 0) => {
        if (!serverSide || !Array.isArray(colFields) || colFields.length === 0 || !header || !header.column) return null;
        const columnId = header.column.id;
        if (!columnId || columnId === 'hierarchy' || columnId === '__row_number__' || columnId.startsWith('__RowTotal__')) return null;
        if (rowFields.includes(columnId)) return null;

        const isGroupHeader = header.column.columns && header.column.columns.length > 0;
        if (isGroupHeader) {
            return level >= 0 && level < colFields.length ? colFields[level] : null;
        }
        return colFields[colFields.length - 1] || null;
    }, [serverSide, colFields, rowFields]);

    const getPivotColumnSortState = useCallback((field) => {
        if (!field) return null;
        const fieldOptions = effectiveSortOptions
            && effectiveSortOptions.columnOptions
            && typeof effectiveSortOptions.columnOptions === 'object'
            ? effectiveSortOptions.columnOptions[field]
            : null;
        const direction = fieldOptions && fieldOptions.pivotDirection;
        const mode = fieldOptions && fieldOptions.pivotSortMode;
        if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) {
            return { mode, direction };
        }
        if (direction === 'asc' || direction === 'desc') {
            return { mode: 'label', direction };
        }
        return null;
    }, [effectiveSortOptions]);

    const applyPivotColumnSort = useCallback((field, mode, direction) => {
        if (!field) return;
        setPivotColumnSorting(prev => {
            const next = { ...(prev || {}) };
            if ((direction === 'asc' || direction === 'desc') && (mode === 'label' || mode === 'total')) {
                next[field] = {
                    ...(next[field] || {}),
                    pivotSortMode: mode,
                    pivotDirection: direction,
                };
            } else {
                delete next[field];
            }
            return next;
        });
        if (parentRef.current) {
            parentRef.current.scrollLeft = 0;
        }
        if (columnVirtualizerRef.current && typeof columnVirtualizerRef.current.scrollToIndex === 'function') {
            columnVirtualizerRef.current.scrollToIndex(0);
        }
        setVisibleColRange(prev => {
            let visibleCount = Math.max(1, prev.end - prev.start + 1);
            if (columnVirtualizerRef.current && typeof columnVirtualizerRef.current.getVirtualItems === 'function') {
                const virtualItems = columnVirtualizerRef.current.getVirtualItems() || [];
                if (virtualItems.length > 0) {
                    visibleCount = Math.max(visibleCount, virtualItems.length);
                }
            }
            const nextRange = { start: 0, end: Math.max(0, visibleCount - 1) };
            return prev.start === nextRange.start && prev.end === nextRange.end ? prev : nextRange;
        });
        const fieldLabel = formatDisplayLabel(field);
        if (mode === 'label' && direction === 'asc') {
            showNotification(`Header order for ${fieldLabel} set to A-Z.`, 'success');
        } else if (mode === 'label' && direction === 'desc') {
            showNotification(`Header order for ${fieldLabel} set to Z-A.`, 'success');
        } else if (mode === 'total' && direction === 'desc') {
            showNotification(`Header order for ${fieldLabel} set to largest totals first.`, 'success');
        } else if (mode === 'total' && direction === 'asc') {
            showNotification(`Header order for ${fieldLabel} set to smallest totals first.`, 'success');
        } else {
            showNotification(`Header order cleared for ${fieldLabel}.`, 'info');
        }
    }, [parentRef, showNotification]);

    const handlePinColumn = useCallback((columnId, side) => {
        const table = tableRef.current;
        if (!table) return;
        
        const col = table.getColumn(columnId);
        if (!col) return;

        // Get all IDs to pin/unpin (leaves + potential collapsed placeholder)
        const idsToUpdate = new Set();
        
        // 1. Add leaf IDs
        const isGroup = col.columns && col.columns.length > 0;
        const leafIds = isGroup ? getAllLeafIdsFromColumn(col) : [columnId];
        leafIds.forEach(id => idsToUpdate.add(id));

        // 2. Add collapsed placeholder ID if it's a pivot group
        if (columnId.startsWith('group_')) {
            const rawPathKey = columnId.replace('group_', '');
            idsToUpdate.add(`${rawPathKey}_collapsed`);
        }

        const idsArray = Array.from(idsToUpdate);

        setColumnPinning(prev => {
            const next = { left: [...(prev.left || [])], right: [...(prev.right || [])] };

            // Remove all relevant IDs from both sides first
            next.left = next.left.filter(id => !idsToUpdate.has(id));
            next.right = next.right.filter(id => !idsToUpdate.has(id));

            // Add to new side
            if (side === 'left') next.left.push(...idsArray);
            if (side === 'right') next.right.push(...idsArray);

            return next;
        });
    }, []);


    const handlePinRow = (rowId, pinState) => {
        if (rowId === GRAND_TOTAL_ROW_ID) {
            setGrandTotalPinOverride(pinState === 'top' || pinState === 'bottom' ? pinState : false);
        }
        setRowPinning(prev => applyRowPinning(prev, rowId, pinState));

        // Fire Pinning Event
        if (setProps) {
            setProps({
                rowPinned: {
                    rowId: rowId,
                    pinState: pinState,
                    timestamp: Date.now()
                }
            });
        }
    };

    useEffect(() => {
        setColumnPinning(prev => {
            let nextLeft = [...(prev.left || [])];
            let changed = false;

            // 1. Enforce Hierarchy Pinning
            if (layoutMode === 'hierarchy' && rowFields.length > 0) {
                if (!nextLeft.includes('hierarchy')) {
                     nextLeft = ['hierarchy', ...nextLeft];
                     changed = true;
                }
            }

            // 2. Enforce Row Number Pinning (User Request: Always utmost left)
            if (showRowNumbers) {
                if (!nextLeft.includes('__row_number__')) {
                    nextLeft = ['__row_number__', ...nextLeft];
                    changed = true;
                }
                // Ensure it is first (utmost left)
                const idx = nextLeft.indexOf('__row_number__');
                if (idx > 0) {
                    nextLeft.splice(idx, 1);
                    nextLeft.unshift('__row_number__');
                    changed = true;
                }
            } else {
                 // If hidden, remove from pinned? (Optional, but clean)
                 if (nextLeft.includes('__row_number__')) {
                     nextLeft = nextLeft.filter(id => id !== '__row_number__');
                     changed = true;
                 }
            }

            if (changed) {
                debugLog('Pinning Enforcement Triggered', nextLeft);
                return { ...prev, left: nextLeft };
            }
            return prev;
        });
    }, [layoutMode, rowFields.length, showRowNumbers]);

    // 4. FIXED: handleHeaderContextMenu with proper group detection
    const handleHeaderContextMenu = (e, colId, header = null, level = 0) => {
        e.preventDefault();
        const actions = [];
        const column = table.getColumn(colId);

        if (!column) {
            return;
        }

        const { left, right } = columnPinning;

        // Determine if this is a group column
        const isGroup = isGroupColumn(column);

        // Only show sort options for leaf columns
        if (!isGroup) {
            actions.push({
                label: 'Sort Ascending',
                icon: <Icons.SortAsc/>,
                onClick: () => column.toggleSorting(false)
            });
            actions.push({
                label: 'Sort Descending',
                icon: <Icons.SortDesc/>,
                onClick: () => column.toggleSorting(true)
            });
            actions.push({
                label: 'Clear Sort',
                onClick: () => column.clearSorting()
            });

            actions.push('separator');
            actions.push({
                label: 'Filter...',
                icon: <Icons.Filter/>,
                onClick: () => setActiveFilterCol(colId)
            });
            actions.push({
                label: 'Clear Filter',
                onClick: () => handleHeaderFilter(colId, null)
            });

            actions.push('separator');
        }

        const pivotSortField = getPivotColumnSortField(header, level);
        const pivotSortState = getPivotColumnSortState(pivotSortField);
        if (pivotSortField) {
            actions.push({
                label: 'Header Labels A-Z',
                icon: <Icons.SortAsc/>,
                onClick: () => applyPivotColumnSort(pivotSortField, 'label', 'asc')
            });
            actions.push({
                label: 'Header Labels Z-A',
                icon: <Icons.SortDesc/>,
                onClick: () => applyPivotColumnSort(pivotSortField, 'label', 'desc')
            });
            actions.push({
                label: 'Column Totals Largest First',
                icon: <Icons.SortDesc/>,
                onClick: () => applyPivotColumnSort(pivotSortField, 'total', 'desc')
            });
            actions.push({
                label: 'Column Totals Smallest First',
                icon: <Icons.SortAsc/>,
                onClick: () => applyPivotColumnSort(pivotSortField, 'total', 'asc')
            });
            if (pivotSortState) {
                actions.push({
                    label: 'Clear Header Order',
                    onClick: () => applyPivotColumnSort(pivotSortField, null, null)
                });
            }
            actions.push('separator');
        }

        // Pin options for both leaf and group columns
        let isPinned = false;

        if (isGroup) {
            // For group columns, check if ALL leaf columns are pinned
            const leafIds = getAllLeafIdsFromColumn(column);

            const allPinnedLeft = leafIds.length > 0 && leafIds.every(id => (left || []).includes(id));
            const allPinnedRight = leafIds.length > 0 && leafIds.every(id => (right || []).includes(id));

            if (allPinnedLeft) isPinned = 'left';
            else if (allPinnedRight) isPinned = 'right';

            actions.push({
                label: 'Pin All Children Left',
                onClick: () => handlePinColumn(colId, 'left')
            });
            actions.push({
                label: 'Pin All Children Right',
                onClick: () => handlePinColumn(colId, 'right')
            });
            if (isPinned) {
                actions.push({
                    label: 'Unpin All Children',
                    onClick: () => handlePinColumn(colId, false)
                });
            }
        } else {
            // For leaf columns, check directly
            if ((left || []).includes(colId)) isPinned = 'left';
            else if ((right || []).includes(colId)) isPinned = 'right';

            actions.push({
                label: 'Pin Column Left',
                onClick: () => handlePinColumn(colId, 'left')
            });
            actions.push({
                label: 'Pin Column Right',
                onClick: () => handlePinColumn(colId, 'right')
            });
            if (isPinned) {
                actions.push({
                    label: 'Unpin Column',
                    onClick: () => handlePinColumn(colId, false)
                });
            }
        }

        actions.push('separator');
        actions.push({
            label: 'Expand All Rows',
            onClick: () => handleExpandAllRows(true)
        });
        actions.push({
            label: 'Collapse All Rows',
            onClick: () => handleExpandAllRows(false)
        });

        actions.push('separator');
        actions.push({
            label: 'Auto-size Column',
            onClick: () => autoSizeColumn(colId)
        });
        actions.push({
            label: 'Export to Excel',
            icon: <Icons.Export/>,
            onClick: exportPivot
        });

        setContextMenu({
            x: e.clientX,
            y: e.clientY,
            actions: actions
        });
    };

    const handleContextMenu = (e, value, colId, row) => {
        e.preventDefault();
        const rowId = row ? row.id : null;
        const key = rowId ? `${rowId}:${colId}` : null;
        // Track the row path for "Format Row" feature
        if (row && row.original && row.original._path) {
            setHoveredRowPath(row.original._path);
        }

        // Right-click should operate on the clicked cell immediately.
        // If it was not selected yet, promote it to the active selection.
        let selectionForMenu = selectedCells;
        if (rowId && key && !hasSelectionKey(selectedCells, key)) {
            selectionForMenu = { [key]: value };
            setSelectedCells(selectionForMenu);
            const visibleCols = table.getVisibleLeafColumns();
            const colIndex = visibleCols.findIndex(c => c.id === colId);
            const displayRowIndex = row ? resolveDisplayRowIndex(row.id, row.index) : -1;
            if (row && displayRowIndex >= 0 && colIndex >= 0) {
                setLastSelected({ rowIndex: displayRowIndex, colIndex });
            }
        }

        const hasSelection = Object.keys(selectionForMenu).length > 0;
        
        const getTableData = (withHeaders) => {
            const visibleRows = getDisplayRows();
            const visibleCols = table.getVisibleLeafColumns();
            let tsv = "";
            if (withHeaders) {
                tsv += visibleCols.map(c => typeof c.columnDef.header === 'string' ? c.columnDef.header : c.id).join('\t') + '\n';
            }
            visibleRows.forEach(r => {
                const vals = visibleCols.map(c => {
                    const v = r.getValue(c.id);
                    return v !== undefined && v !== null ? String(v) : "";
                });
                tsv += vals.join('\t') + '\n';
            });
            return tsv;
        };

        const actions = [
            { label: 'Copy Table', icon: <Icons.DragIndicator/>, onClick: () => copyToClipboard(getTableData(false)) },
            { label: 'Copy Table with Headers', onClick: () => copyToClipboard(getTableData(true)) },
        ];

        if (hasSelection) {
            actions.push('separator');
            actions.push({ label: 'Copy Selection', onClick: () => {
                const data = getSelectedData(false, selectionForMenu);
                if (data) copyToClipboard(data);
            }});
            actions.push({ label: 'Copy Selection with Headers', onClick: () => {
                const data = getSelectedData(true, selectionForMenu);
                if (data) copyToClipboard(data);
            }});
            actions.push({ label: 'Create Range Chart', icon: <Icons.Chart/>, onClick: () => openSelectionChart(selectionForMenu) });
        }

        actions.push('separator');
        actions.push({ label: `Filter by "${value}"`, icon: <Icons.Filter/>, onClick: () => {
            handleHeaderFilter(colId, {
                operator: 'AND',
                conditions: [{ type: 'eq', value: String(value), caseSensitive: false }]
            });
        }});
        actions.push({ label: 'Clear Filter', onClick: () => handleHeaderFilter(colId, null) });

        actions.push('separator');
        actions.push({ label: 'Drill Through', icon: <Icons.Search/>, onClick: () => {
             if (row && row.original && row.original._path && row.original._path !== '__grand_total__' && !row.original._isTotal) {
                 fetchDrillData(row.original._path, 0, null, 'asc', '');
             }
        }});

        actions.push('separator');
        if (row && serverSide && row.getCanExpand() && row.original && row.original._path && rowFields.length > 1) {
            actions.push({
                label: 'Expand All Children',
                icon: <Icons.ChevronDown/>,
                onClick: () => {
                    const rowPath = row.original._path;
                    subtreeExpandRef.current = { path: rowPath, expandedPaths: new Set([rowPath]) };
                    captureExpansionScrollPosition();
                    clearCache();
                    setExpanded(prev => {
                        const base = (prev !== null && typeof prev === 'object') ? prev : {};
                        return { ...base, [rowPath]: true };
                    });
                }
            });
        }

        actions.push('separator');
        if (rowId) {
            const currentPinState = rowId === GRAND_TOTAL_ROW_ID
                ? effectiveGrandTotalPinState
                : getPinnedSideForRow(rowPinning, rowId);
            const isPinnedTop = currentPinState === 'top';
            const isPinnedBottom = currentPinState === 'bottom';

            actions.push({ label: 'Pin Row Top', onClick: () => handlePinRow(rowId, 'top') });
            actions.push({ label: 'Pin Row Bottom', onClick: () => handlePinRow(rowId, 'bottom') });
            if (isPinnedTop || isPinnedBottom) {
                actions.push({ label: 'Unpin Row', onClick: () => handlePinRow(rowId, false) });
            }
        }

        setContextMenu({
            x: e.clientX,
            y: e.clientY,
            actions: actions
        });
    };

    // Extract color scale stats sentinel injected by the server, strip it from rows
    const { cleanData, serverColorScaleStats } = useMemo(() => {
        const sentinelIdx = data.findIndex(r => r && r._path === '__color_scale_stats__');
        if (sentinelIdx === -1) return { cleanData: data, serverColorScaleStats: null };
        const stats = data[sentinelIdx]._colorScaleStats || null;
        const rows = data.filter((_, i) => i !== sentinelIdx);
        return { cleanData: rows, serverColorScaleStats: stats };
    }, [data]);

    const filteredData = useFilteredData(cleanData, filters, serverSide);

    const staticTotal = useMemo(() => ({ _isTotal: true, _path: '__grand_total__', _id: 'Grand Total', __isGrandTotal__: true }), []);
    const staticMinMax = useMemo(() => ({}), []);

    const { nodes, total, minMax } = useMemo(() => {
        return { nodes: filteredData, total: staticTotal, minMax: staticMinMax };
    }, [filteredData, staticTotal, staticMinMax]);

    const handleHeaderFilter = (columnId, filterValue) => {
        setFilters(prev => {
            const newFilters = {...prev};
            if (filterValue === null || filterValue.conditions.length === 0) {
                delete newFilters[columnId];
            } else {
                newFilters[columnId] = filterValue;
            }
            return newFilters;
        });
    };

    const autoSizeColumn = (columnId) => {
        const rows = table.getRowModel().rows;
        const sampleRows = rows.slice(0, 250);
        let maxWidth = 0;

        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        if (!context) return;
        const bodyFont = `${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;
        const headerFont = `600 ${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;

        const column = table.getColumn(columnId);
        if (!column) return;
        const header = column.columnDef.header;
        const headerText = formatDisplayLabel(typeof header === 'string' ? header : columnId);
        context.font = headerFont;
        maxWidth = context.measureText(headerText).width + autoSizeBounds.headerPadding;

        context.font = bodyFont;
        sampleRows.forEach(row => {
            const cellValue = row.getValue(columnId);
            let text;
            if (columnId === 'hierarchy') {
                const depth = row.original && typeof row.original.depth === 'number' ? row.original.depth : (row.depth || 0);
                const label = row.original && row.original._id != null ? formatDisplayLabel(String(row.original._id)) : '';
                text = `${' '.repeat(depth * 2)}${label}`;
            } else {
                text = formatValue(cellValue);
            }
            const width = context.measureText(text).width + autoSizeBounds.cellPadding;
            if (width > maxWidth) maxWidth = width;
        });

        maxWidth = Math.min(maxWidth, autoSizeBounds.maxWidth);
        maxWidth = Math.max(maxWidth, autoSizeBounds.minWidth);

        table.setColumnSizing(old => ({
            ...old,
            [columnId]: maxWidth
        }));
    };

    const autoSizeVisibleColumns = useCallback(() => {
        if (!table || !table.getVisibleLeafColumns) return;
        table.getVisibleLeafColumns().forEach((column) => {
            autoSizeColumn(column.id);
        });
    }, [table, autoSizeColumn]);

    const handleFilterClick = (e, columnId) => {
        e.stopPropagation();
        setActiveFilterCol(columnId);
        setFilterAnchorEl(e.currentTarget);
        if (setPropsRef.current) {
            setPropsRef.current({
                filters: { ...filters, '__request_unique__': columnId }
            });
        }
    };

    const requestFilterOptions = useCallback((columnId) => {
        if (!columnId || !setPropsRef.current) return;
        setPropsRef.current({
            filters: { ...filters, '__request_unique__': columnId }
        });
    }, [filters]);

    useEffect(() => {
        if (!serverSide || !activeFilterCol) {
            pendingServerFilterOptionsRef.current = null;
            return;
        }
        if (filterOptions && filterOptions[activeFilterCol]) {
            pendingServerFilterOptionsRef.current = null;
            return;
        }
        if (pendingServerFilterOptionsRef.current === activeFilterCol) {
            return;
        }
        pendingServerFilterOptionsRef.current = activeFilterCol;
        requestFilterOptions(activeFilterCol);
    }, [serverSide, activeFilterCol, filterOptions, requestFilterOptions]);

    const closeFilterPopover = () => {
        setActiveFilterCol(null);
        setFilterAnchorEl(null);
    };

    useEffect(() => {
        const activeLeafIds = new Set();
        const stack = [...columns];

        while (stack.length > 0) {
            const columnDef = stack.pop();
            if (!columnDef) continue;

            if (Array.isArray(columnDef.columns) && columnDef.columns.length > 0) {
                columnDef.columns.forEach(child => stack.push(child));
                continue;
            }

            const leafId = columnDef.id || columnDef.accessorKey;
            if (leafId) {
                activeLeafIds.add(String(leafId));
            }
        }

        setColumnSizing(prev => {
            if (!prev || typeof prev !== 'object' || Object.keys(prev).length === 0) {
                return prev;
            }

            let changed = false;
            const next = {};
            Object.keys(prev).forEach(key => {
                if (activeLeafIds.has(key)) {
                    next[key] = prev[key];
                } else {
                    changed = true;
                }
            });

            return changed ? next : prev;
        });
    }, [columns]);

    const parentRef = useRef(null);
    const expansionScrollRestoreRef = useRef(null);
    const expansionScrollRestoreRafRef = useRef(null);
    // Tracks an in-progress "expand all children" operation.
    // { path: string, expandedPaths: Set<string> }
    // We use a ref (not state) to avoid dependency cycles in the watcher effect.
    const subtreeExpandRef = useRef(null);
    // After a single-row expansion, holds the anchor block index so we can
    // invalidate subsequent blocks once the expansion response has landed.
    // Doing it after the response avoids a concurrent viewport request that
    // would race with (and stale-reject) the expansion request.
    const pendingExpansionRef = useRef(null);
    const rowHeight = rowHeights[spacingMode] || rowHeights[0];

    const handleTransposePivot = useCallback(() => {
        const nextRowFields = [...colFields];
        const nextColFields = [...rowFields];

        closeFilterPopover();
        setContextMenu(null);
        setExpanded({});
        setSorting([]);
        setColExpanded({});
        setSelectedCells({});
        setLastSelected(null);
        setSelectedCols(new Set());
        setHistory([]);
        setFuture([]);
        setDragStart(null);
        setFillRange(null);
        setIsDragging(false);
        setIsFilling(false);
        setShowRowTotals(showColTotals);
        setShowColTotals(showRowTotals);
        setGrandTotalPinOverride(null);
        setRowFields(nextRowFields);
        setColFields(nextColFields);

        if (parentRef.current) {
            parentRef.current.scrollTop = 0;
            parentRef.current.scrollLeft = 0;
        }

        showNotification('Pivot transposed', 'success');
    }, [
        closeFilterPopover,
        colFields,
        rowFields,
        showColTotals,
        showRowTotals,
        showNotification,
    ]);

    // Cache key: only structural changes that require a full cache wipe.
    // Expansion and rowCount are intentionally excluded — expansion uses targeted
    // block invalidation (invalidateFromBlock) so rows before the toggled node
    // stay cached, and rowCount is a derived result that changes with expansion.
    const serverSideCacheKey = useMemo(() => JSON.stringify({
        sorting,
        filters,
        rowFields,
        colFields,
        valConfigs,
    }), [sorting, filters, rowFields, colFields, valConfigs]);
    // Viewport reset key: only changes that semantically restart the user's view
    // (new sort/filter/fields). rowCount is excluded because it changes when rows
    // are expanded, which must NOT scroll back to the top.
    const serverSideViewportResetKey = useMemo(() => JSON.stringify({
        sorting,
        filters,
        rowFields,
        colFields,
        valConfigs,
    }), [sorting, filters, rowFields, colFields, valConfigs]);

    const effectiveRowCount = serverSidePinsGrandTotal && rowCount ? Math.max(rowCount - 1, 0) : rowCount;
    const statusRowCount = serverSidePinsGrandTotal && rowCount ? Math.max(rowCount - 1, 0) : rowCount;

    const captureExpansionScrollPosition = useCallback(() => {
        if (!serverSide || !parentRef.current) return;
        expansionScrollRestoreRef.current = {
            scrollTop: parentRef.current.scrollTop,
            restorePassesRemaining: 3
        };
    }, [serverSide]);

    const { rowVirtualizer, getRow, renderedData, renderedOffset, clearCache, invalidateFromBlock, softInvalidateFromBlock, grandTotalRow, loadedRows } = useServerSideRowModel({
        parentRef,
        serverSide,
        rowCount: effectiveRowCount,
        rowHeight,
        data: filteredData,
        dataOffset: dataOffset || 0,
        dataVersion: dataVersion || 0,
        setProps,
        blockSize: 100,
        cacheKey: serverSideCacheKey,
        excludeGrandTotal: serverSidePinsGrandTotal,
        cinemaMode,
        stateEpoch,
        sessionId: sessionIdRef.current,
        clientInstance: clientInstanceRef.current,
        abortGeneration,
        structuralInFlight,
        requestVersionRef,
        tableName,
        colStart: colRequestStart,
        colEnd: colRequestEnd,
        needsColSchema: needsColSchema && serverSide,
        onViewportRequest: markRequestPending,
    });

    const columns = useColumnDefs({
        sortOptions: effectiveSortOptions,
        serverSide,
        showRowNumbers,
        layoutMode,
        rowFields,
        colFields,
        valConfigs,
        minMax,
        colorScaleMode,
        colExpanded,
        isRowSelecting,
        rowDragStart,
        props,
        cachedColSchema,
        filteredData,
        // Render-time closures (stable refs or render-time reads)
        theme,
        defaultColumnWidths,
        validationRules,
        setProps,
        handleContextMenu,
        handleRowSelect,
        handleRowRangeSelect,
        setIsRowSelecting,
        setRowDragStart,
        renderedOffset,
        isColExpanded,
        toggleCol,
        pendingRowTransitions,
        decimalPlaces,
        columnDecimalOverrides,
        cellFormatRules,
    });

    // Auto-size new columns to fit their header text on spawn
    useEffect(() => {
        if (!table || !table.getVisibleLeafColumns) return;
        const visLeafCols = table.getVisibleLeafColumns();
        if (visLeafCols.length === 0) return;
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        ctx.font = `600 13px system-ui, Arial, sans-serif`;
        const newSizes = {};
        visLeafCols.forEach(col => {
            if (columnSizing[col.id] !== undefined) return; // already user-set
            const headerText = typeof col.columnDef.header === 'string' ? col.columnDef.header : col.id;
            const measured = ctx.measureText(headerText).width + autoSizeBounds.headerPadding;
            const clamped = Math.min(Math.max(measured, autoSizeBounds.minWidth), 300);
            if (clamped > (col.columnDef.size || autoSizeBounds.minWidth)) {
                newSizes[col.id] = clamped;
            }
        });
        if (Object.keys(newSizes).length > 0) {
            setColumnSizing(prev => ({ ...newSizes, ...prev }));
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [colFields, valConfigs, rowFields]);

    useEffect(() => {
        if (!serverSide || !structuralInFlight) return;
        const pending = structuralPendingVersionRef.current;
        const numericVersion = Number(dataVersion);
        if (!pending || !Number.isFinite(numericVersion)) return;
        if (numericVersion > pending.startDataVersion && numericVersion >= pending.version) {
            setStructuralInFlight(false);
            structuralPendingVersionRef.current = null;
            setPendingRowTransitions(new Map());
            setPendingColumnSkeletonCount(0);
        }
    }, [dataVersion, serverSide, structuralInFlight]);

    useEffect(() => {
        if (!serverSide || !structuralInFlight) return;
        const timeoutId = setTimeout(() => {
            setStructuralInFlight(false);
            structuralPendingVersionRef.current = null;
            setPendingRowTransitions(new Map());
            setPendingColumnSkeletonCount(0);
        }, 10000);
        return () => clearTimeout(timeoutId);
    }, [serverSide, structuralInFlight, stateEpoch]);

    const getStableDataRowId = useCallback((row) => {
        if (!row) return null;
        if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') {
            return '__grand_total__';
        }
        if (row._path) return row._path;
        if (row.id) return row.id;
        if (typeof row.__virtualIndex === 'number') return String(row.__virtualIndex);
        return null;
    }, []);

    const tableData = useMemo(() => {
        if (serverSide) {
             const centerData = renderedData.filter(row => (
                 row &&
                 (
                     (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total')
                         ? !serverSidePinsGrandTotal
                         : !row._isTotal
                 )
             ));
             const seenIds = new Set(centerData.map(getStableDataRowId).filter(Boolean));
             const pinnedIds = Array.from(new Set([
                 ...((rowPinning && rowPinning.top) || []),
                 ...((rowPinning && rowPinning.bottom) || []),
             ]));
             const pinnedData = pinnedIds
                 .map((rowId) => {
                     const cachedRow = pinnedRowCacheRef.current.get(rowId);
                     return cachedRow && cachedRow.original ? cachedRow.original : null;
                 })
                 .filter((row) => {
                     const stableId = getStableDataRowId(row);
                     if (!stableId || seenIds.has(stableId)) return false;
                     seenIds.add(stableId);
                     return true;
                 });
             const nextData = [...centerData, ...pinnedData];
             if (serverSidePinsGrandTotal && showColTotals && grandTotalRow && !seenIds.has(GRAND_TOTAL_ROW_ID)) {
                 nextData.push(grandTotalRow);
             }
             return nextData;
        }

        let baseData = (filteredData.length ? [...nodes] : []);

        if (!showColTotals) {
            baseData = baseData.filter(r => !r._isTotal);
        }

        // Add the grand total to the end of the data array to ensure it appears at the bottom
        // Only add if not in serverSide mode and showColTotals is true
        if (!serverSide && showColTotals) {
            baseData = [...baseData, total];
        }

        return baseData;
    }, [
        nodes,
        total,
        filteredData,
        serverSide,
        showColTotals,
        renderedData,
        grandTotalRow,
        rowPinning,
        getStableDataRowId,
        serverSidePinsGrandTotal,
    ]);

    // Color scale palettes: [lowColor, highColor, darkTextLow, darkTextHigh]
    const COLOR_PALETTES = {
        redGreen:   { low: [248,105,107], high: [99,190,123],  darkLow: '#5c0001', darkHigh: '#1a4d2a' },
        greenRed:   { low: [99,190,123],  high: [248,105,107], darkLow: '#1a4d2a', darkHigh: '#5c0001' },
        blueWhite:  { low: [65,105,225],  high: [255,255,255], darkLow: '#0d2b6e', darkHigh: '#333'    },
        yellowBlue: { low: [255,220,0],   high: [30,90,200],   darkLow: '#5a4700', darkHigh: '#0a2a6e' },
        orangePurp: { low: [255,140,0],   high: [120,50,200],  darkLow: '#5c3000', darkHigh: '#2a0060' },
    };

    // Client-side stats (non-server mode): exclude totals and row/col fields
    const clientColorScaleStats = useMemo(() => {
        const metaKeys = new Set([
            '_id', '_path', '_isTotal', 'depth', '_depth', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'subRows', 'uuid', '__virtualIndex',
            '__colPending', '__isGrandTotal__'
        ]);
        rowFields.forEach(f => metaKeys.add(f));
        colFields.forEach(f => metaKeys.add(f));

        const byCol = {};
        const byRow = {};
        let tableMin = Number.POSITIVE_INFINITY;
        let tableMax = Number.NEGATIVE_INFINITY;

        // Use filteredData (not tableData) so grand total synthetic row is never included
        const sourceData = filteredData.length > 0 ? filteredData : nodes;
        sourceData.forEach((row, idx) => {
            if (!row || typeof row !== 'object') return;
            // Skip all total rows
            if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return;

            const rowKey = String(row._path || row._id || idx);
            let rowMin = Number.POSITIVE_INFINITY;
            let rowMax = Number.NEGATIVE_INFINITY;

            Object.entries(row).forEach(([key, raw]) => {
                if (metaKeys.has(key)) return;
                if (typeof raw !== 'number' || Number.isNaN(raw)) return;

                if (!byCol[key]) {
                    byCol[key] = { min: raw, max: raw };
                } else {
                    byCol[key].min = Math.min(byCol[key].min, raw);
                    byCol[key].max = Math.max(byCol[key].max, raw);
                }

                rowMin = Math.min(rowMin, raw);
                rowMax = Math.max(rowMax, raw);
                tableMin = Math.min(tableMin, raw);
                tableMax = Math.max(tableMax, raw);
            });

            if (Number.isFinite(rowMin) && Number.isFinite(rowMax)) {
                byRow[rowKey] = { min: rowMin, max: rowMax };
            }
        });

        return {
            byCol,
            byRow,
            table: Number.isFinite(tableMin) && Number.isFinite(tableMax)
                ? { min: tableMin, max: tableMax }
                : null,
        };
    }, [filteredData, nodes, rowFields, colFields, serverSide]);

    // Use server-provided stats when available; fallback to client-computed stats
    // (needed for data bars in server-side virtual scroll mode where server may not emit stats)
    const colorScaleStats = useMemo(() => {
        if (serverColorScaleStats && clientColorScaleStats) {
            // Merge: server stats take priority, client fills gaps (e.g. data bars columns)
            return {
                byCol: { ...clientColorScaleStats.byCol, ...serverColorScaleStats.byCol },
                byRow: { ...clientColorScaleStats.byRow, ...(serverColorScaleStats.byRow || {}) },
                table: serverColorScaleStats.table || clientColorScaleStats.table,
            };
        }
        return serverColorScaleStats || clientColorScaleStats;
    }, [serverColorScaleStats, clientColorScaleStats]);

    const getConditionalStyle = useCallback((colId, value, rowData, rowId) => {
        const ruleStyle = getRuleBasedStyle(colId, value);
        if (colorScaleMode === 'off' || typeof value !== 'number' || Number.isNaN(value)) {
            return ruleStyle;
        }
        // Skip total rows in color scale
        if (rowData && (rowData._isTotal || rowData._path === '__grand_total__' || rowData._id === 'Grand Total')) {
            return ruleStyle;
        }

        let stats = null;
        if (colorScaleMode === 'col') {
            stats = (colorScaleStats.byCol && colorScaleStats.byCol[colId]) || null;
        } else if (colorScaleMode === 'row') {
            const rowKey = String((rowData && rowData._path) || rowId || '');
            stats = (colorScaleStats.byRow && colorScaleStats.byRow[rowKey]) || null;
        } else if (colorScaleMode === 'table') {
            stats = colorScaleStats.table || null;
        }

        if (!stats || !Number.isFinite(stats.min) || !Number.isFinite(stats.max) || stats.max === stats.min) {
            return ruleStyle;
        }

        const palette = COLOR_PALETTES[colorPalette] || COLOR_PALETTES.redGreen;
        const { low, high, darkLow, darkHigh } = palette;

        // Handle zero-crossing: when data spans negative and positive,
        // use 0 as the neutral (transparent) midpoint
        let posInRange; // 0=low extreme, 1=high extreme, 0.5=neutral
        const hasZeroCrossing = stats.min < 0 && stats.max > 0;
        if (hasZeroCrossing) {
            // Map negative values [min,0] → [0,0.5] and positive [0,max] → [0.5,1]
            if (value <= 0) {
                posInRange = 0.5 * (value - stats.min) / (0 - stats.min);
            } else {
                posInRange = 0.5 + 0.5 * value / stats.max;
            }
        } else {
            posInRange = (value - stats.min) / (stats.max - stats.min);
        }
        const clamped = Math.max(0, Math.min(1, posInRange));

        // Transparency: fully transparent at midpoint (0.5), max opacity at extremes
        // alpha range 0.06 → 0.82 for a clean gradient feel
        const distFromMid = Math.abs(clamped - 0.5) * 2; // 0 at mid, 1 at extremes
        const alpha = 0.06 + distFromMid * 0.76;

        const [r, g, b] = clamped <= 0.5 ? low : high;
        const darkText = clamped <= 0.5 ? darkLow : darkHigh;

        const heatStyle = {
            background: `rgba(${r},${g},${b},${alpha.toFixed(3)})`,
            color: alpha > 0.55 ? darkText : undefined,
        };
        return { ...heatStyle, ...ruleStyle };
    }, [colorScaleMode, colorPalette, colorScaleStats, getRuleBasedStyle]);

    const getRowId = useCallback((row, relativeIndex) => {
        if (!row) return `skeleton_${relativeIndex}`; // Handle skeleton rows
        if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return '__grand_total__';
        if (serverSide && typeof row.__virtualIndex === 'number') {
            return row._path || (row.id ? row.id : String(row.__virtualIndex));
        }
        
        // Use renderedOffset if available (from virtualizer cache), else fallback to dataOffset
        const effectiveOffset = (serverSide && renderedOffset !== undefined) ? renderedOffset : (dataOffset || 0);
        const actualIndex = serverSide ? relativeIndex + effectiveOffset : relativeIndex;
        
        return row._path || (row.id ? row.id : String(actualIndex));
    }, [serverSide, dataOffset, renderedOffset]);
    const getSubRows = useCallback(r => r ? r.subRows : undefined, []);
    const getRowCanExpand = useCallback(row => {
        if (!row.original) return false;
        // Prevent expansion of any total rows, including grand totals
        if (row.original && row.original._isTotal) return false;
        
        if (serverSide) {
             // Use server-provided flag if available for accurate child detection
             if (row.original._has_children !== undefined) return row.original._has_children;
             return (row.original.depth || 0) < rowFields.length - 1;
        }
        
        return row.subRows && row.subRows.length > 0;
    }, [serverSide, rowFields.length]);

    const getIsRowExpanded = useCallback(row => {
        if (!row.original) return false;
        if (row.original && row.original._isTotal) return false;

        if (serverSide) {
             // 1. "Expand All" mode
             if (expanded === true) return true;
             
             // 2. Explicit Local State (Optimistic)
             // We check if the key exists in the expanded object to respect user interactions
             if (expanded && Object.prototype.hasOwnProperty.call(expanded, row.id)) {
                 return !!expanded[row.id];
             }
             
             // 3. Server State (Fallback/Source of Truth)
             // If local state doesn't know about this row yet (e.g. initial load), trust the server
             if (row.original._is_expanded !== undefined) {
                 return row.original._is_expanded;
             }
        }

        // Standard Client-Side Logic
        if (expanded === true) return true;
        // Otherwise check if this specific row is expanded
        return !!expanded[row.id];
    }, [expanded, serverSide]);

    const tableState = useMemo(() => {
        const availableRowIds = new Set((tableData || []).map(getStableDataRowId).filter(Boolean));
        let finalRowPinning = {
            ...normalizeRowPinningState(rowPinning),
            top: ((rowPinning && rowPinning.top) || []).filter(id => availableRowIds.has(id)),
            bottom: ((rowPinning && rowPinning.bottom) || []).filter(id => availableRowIds.has(id)),
        };
        const topWithoutGrandTotal = (finalRowPinning.top || []).filter(id => id !== GRAND_TOTAL_ROW_ID);
        const bottomWithoutGrandTotal = (finalRowPinning.bottom || []).filter(id => id !== GRAND_TOTAL_ROW_ID);
        const hasGrandTotalRow = availableRowIds.has(GRAND_TOTAL_ROW_ID);

        finalRowPinning = {
            ...finalRowPinning,
            top: effectiveGrandTotalPinState === 'top' && hasGrandTotalRow
                ? [...topWithoutGrandTotal, GRAND_TOTAL_ROW_ID]
                : topWithoutGrandTotal,
            bottom: effectiveGrandTotalPinState === 'bottom' && hasGrandTotalRow
                ? [...bottomWithoutGrandTotal, GRAND_TOTAL_ROW_ID]
                : bottomWithoutGrandTotal,
        };

        return {
            sorting,
            expanded,
            columnPinning,
            rowPinning: finalRowPinning,
            grouping: rowFields,
            columnVisibility,
            columnSizing
        };
    }, [
        sorting,
        expanded,
        columnPinning,
        rowPinning,
        rowFields,
        columnVisibility,
        columnSizing,
        tableData,
        effectiveGrandTotalPinState,
        getStableDataRowId,
    ]);



    const handleExpandAllRows = (shouldExpand) => {
        // Guard against rapid double-clicks: second click before server responds
        // would call clearCache() again but the net expanded state change may be
        // batched away by React, leaving the cache empty with no request sent.
        if (expandAllDebounceRef.current) return;
        expandAllDebounceRef.current = true;
        setTimeout(() => { expandAllDebounceRef.current = false; }, 800);

        if (serverSide) {
            captureExpansionScrollPosition();
            // Expanding/collapsing ALL rows changes every row index — full cache wipe.
            // (This path bypasses onExpandedChange so invalidateFromBlock won't run.)
            clearCache();
            setExpanded(shouldExpand ? true : {});
            return;
        }

        if (shouldExpand) {
            // Expand all rows by creating an object with all row IDs set to true
            const allRows = table.getCoreRowModel().rows;
            const newExpanded = {};

            allRows.forEach(row => {
                // Only add rows that can be expanded and are not totals
                if (row.getCanExpand() && !(row.original && row.original._isTotal)) {
                    newExpanded[row.id] = true;

                    // Also expand sub-rows recursively
                    const expandSubRows = (subRows) => {
                        subRows.forEach(subRow => {
                            if (subRow.getCanExpand() && !(subRow.original && subRow.original._isTotal)) {
                                newExpanded[subRow.id] = true;
                                if (subRow.subRows && subRow.subRows.length > 0) {
                                    expandSubRows(subRow.subRows);
                                }
                            }
                        });
                    };

                    if (row.subRows && row.subRows.length > 0) {
                        expandSubRows(row.subRows);
                    }
                }
            });

            setExpanded(newExpanded);
        } else {
            // Collapse all by setting empty object
            setExpanded({});
        }
    };

    const handleSortingChange = (updater) => {
        const newSorting = typeof updater === 'function' ? updater(sorting) : updater;
        setSorting(newSorting);

        // Fire sort event to backend
        if (setPropsRef.current) {
            setPropsRef.current({
                sorting: newSorting,
                sortEvent: {
                    type: 'change',
                    status: 'applied',
                    sorting: newSorting,
                    timestamp: Date.now()
                }
            });
        }
    };

    const table = useReactTable({
        data: tableData,
        columns,
        state: tableState,
        onSortingChange: (updater) => { handleSortingChange(updater); },
        onExpandedChange: (updater) => {
            captureExpansionScrollPosition();
            const newExpanded = typeof updater === 'function' ? updater(expanded) : updater;

            if (serverSide) {
                // Find which path was toggled so we know which block to defer-invalidate
                // after the expansion response lands (see pendingExpansionRef effect).
                // Value-diff: detect any key whose boolean value flipped (covers
                // both key-add/remove AND false→true / true→false toggles).
                const oldExp = expanded || {};
                const newExp = newExpanded || {};
                const allKeys = new Set([...Object.keys(oldExp), ...Object.keys(newExp)]);
                const changedPath = [...allKeys].find(k => !!oldExp[k] !== !!newExp[k]);
                if (changedPath) {
                    const isNowExpanded = !!(newExpanded && newExpanded[changedPath]);
                    setPendingRowTransitions(prev => {
                        const next = new Map(prev);
                        next.set(changedPath, isNowExpanded ? 'expand' : 'collapse');
                        return next;
                    });
                }

                // -1 signals "row not in viewport — do a full cache clear".
                let anchorBlock = -1;
                let expandedRowVirtualIndex = undefined;
                if (changedPath) {
                    const toggledRow = renderedData.find(r => r && r._path === changedPath);
                    if (toggledRow && typeof toggledRow.__virtualIndex === 'number') {
                        anchorBlock = Math.floor(toggledRow.__virtualIndex / 100);
                        // Record the virtual index for viewport anchor preservation.
                        // When rows are inserted/removed ABOVE the current scroll position
                        // we adjust scrollTop so the same logical rows remain in view.
                        expandedRowVirtualIndex = toggledRow.__virtualIndex;
                    }
                    // Do NOT fall back to the scroll position when the row is not in the
                    // rendered viewport.  Using the viewport block as the anchor leaves all
                    // blocks between the expanded row and the viewport with stale (shifted)
                    // row indices.  A full cache clear (anchorBlock = -1) is safer.
                }
                // Don't invalidate now — doing so fires a concurrent viewport request
                // that races with the expanded sync request and causes a stale rejection.
                // Record the anchor so the deferred effect invalidates subsequent blocks
                // once the expansion response has landed (dataVersion bump).
                pendingExpansionRef.current = { anchorBlock, expandedRowVirtualIndex, oldRowCount: rowCount };
            }

            setExpanded(newExpanded);
        },
        onColumnPinningChange: (updater) => { debugLog('onColumnPinningChange'); setColumnPinning(updater); },
        onRowPinningChange: (updater) => { debugLog('onRowPinningChange'); setRowPinning(updater); },
        onColumnVisibilityChange: (updater) => { debugLog('onColumnVisibilityChange'); setColumnVisibility(updater); },
        onColumnSizingChange: (updater) => { debugLog('onColumnSizingChange'); setColumnSizing(updater); },
        getRowId,
        getCoreRowModel: getCoreRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getGroupedRowModel: getGroupedRowModel(),
        getSubRows,
        enableRowPinning: true, // Enable Row Pinning
        enableColumnResizing: true,
        enableMultiSort: true, // Explicitly enable multi-sort
        columnResizeMode: 'onChange',
        manualPagination: serverSide,
        manualSorting: serverSide,
        manualFiltering: serverSide,
        manualGrouping: serverSide,
        manualExpanding: serverSide,
        pageCount: serverSide ? Math.ceil((rowCount || 0) / 100) : undefined,
        getRowCanExpand,
        getIsRowExpanded,
        enableColumnPinning: true,
    });

    // Update the ref with the current table instance
    useEffect(() => {
        tableRef.current = table;
    }, [table]);

    // Accessibility & Event System Effect for Sorting
    useEffect(() => {
        if (sorting.length > 0) {
            const sortDesc = sorting[0].desc ? 'descending' : 'ascending';
            const colId = sorting[0].id;
            const col = tableRef.current.getColumn(colId);
            const colName = col ? (typeof col.columnDef.header === 'string' ? col.columnDef.header : colId) : colId;
            setAnnouncement(`Sorted by ${colName} ${sortDesc}`);
        } else {
            setAnnouncement("Sorting cleared");
        }
        
        // Fire sort event
        if (setPropsRef.current) {
             setPropsRef.current({
                sortEvent: {
                    type: 'change',
                    status: 'applied',
                    sorting: sorting,
                    timestamp: Date.now()
                }
            });
        }
    }, [sorting]); // Removed table and setProps from dependencies

    const toggleAllColumnsPinned = (pinState) => {
        const leafColumns = table.getAllLeafColumns();
        const newPinning = { left: [], right: [] };
        
        if (pinState === 'left') {
            newPinning.left = leafColumns.map(c => c.id).filter(id => id !== 'no_data');
        } else if (pinState === 'right') {
            newPinning.right = leafColumns.map(c => c.id).filter(id => id !== 'no_data');
        } else {
            if (layoutMode === 'hierarchy') {
                newPinning.left = ['hierarchy'];
            }
        }
        
        setColumnPinning(newPinning);
    };

    const activeFilterOptions = useMemo(() => {
        if (!activeFilterCol) return [];
        if (filterOptions[activeFilterCol]) return filterOptions[activeFilterCol];
        
        const col = table.getColumn(activeFilterCol);
        if (!col) return [];
        
        const unique = new Set();
        const rows = table.getCoreRowModel().rows;
        rows.forEach(row => {
            const val = row.getValue(activeFilterCol);
            if (val !== null && val !== undefined) unique.add(val);
        });
        
        return Array.from(unique).sort();
    }, [activeFilterCol, filterOptions, table]);

    const { rows } = table.getRowModel();
    let topRows = [];
    let bottomRows = [];
    let centerRows = rows;
    try {
        topRows = table.getTopRows();
        bottomRows = table.getBottomRows();
        centerRows = table.getCenterRows();
    } catch (error) {
        if (process.env.NODE_ENV !== 'production') {
            console.warn('[pivot-client] row pinning fallback', {
                error,
                rowPinning: tableState.rowPinning,
                tableDataSize: tableData.length,
            });
        }
        const rowLookup = new Map(rows.map((row) => [row.id, row]));
        topRows = ((tableState.rowPinning && tableState.rowPinning.top) || [])
            .map((id) => rowLookup.get(id))
            .filter(Boolean);
        bottomRows = ((tableState.rowPinning && tableState.rowPinning.bottom) || [])
            .map((id) => rowLookup.get(id))
            .filter(Boolean);
        const pinnedIds = new Set([...topRows, ...bottomRows].map((row) => row.id));
        centerRows = rows.filter((row) => !pinnedIds.has(row.id));
    }
    const lastStableRowModelRef = useRef({
        topRows: [],
        centerRows: [],
        bottomRows: []
    });
    const hasRenderedData = renderedData.some(Boolean);

    useEffect(() => {
        if (!serverSide) return;
        if (centerRows.length > 0 || topRows.length > 0 || bottomRows.length > 0) {
            lastStableRowModelRef.current = { topRows, centerRows, bottomRows };
        }
    }, [serverSide, topRows, centerRows, bottomRows]);

    useEffect(() => {
        [...topRows, ...centerRows, ...bottomRows].forEach((row) => {
            if (row && row.id) {
                pinnedRowCacheRef.current.set(row.id, row);
            }
        });
    }, [topRows, centerRows, bottomRows]);

    useEffect(() => {
        if (!serverSide) return;
        lastStableRowModelRef.current = {
            topRows: [],
            centerRows: [],
            bottomRows: []
        };
        // Expansion should still refetch server-side data, but it should not force the viewport back to the top.
        expansionScrollRestoreRef.current = null;
        if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
            cancelAnimationFrame(expansionScrollRestoreRafRef.current);
            expansionScrollRestoreRafRef.current = null;
        }
        if (parentRef.current) {
            parentRef.current.scrollTop = 0;
        }
    }, [serverSide, serverSideViewportResetKey, parentRef]);

    useLayoutEffect(() => {
        if (!serverSide || expansionScrollRestoreRef.current === null || !parentRef.current) return;
        if (!hasRenderedData && centerRows.length === 0 && topRows.length === 0 && bottomRows.length === 0) return;

        const restoreTarget = expansionScrollRestoreRef.current;
        if (!restoreTarget) return;

        const applyScrollRestore = () => {
            if (!parentRef.current || !expansionScrollRestoreRef.current) return;

            const nextTarget = expansionScrollRestoreRef.current;
            const targetScrollTop = nextTarget.scrollTop;
            if (rowVirtualizer.scrollToOffset) {
                rowVirtualizer.scrollToOffset(targetScrollTop);
            }
            if (Math.abs(parentRef.current.scrollTop - targetScrollTop) > 1) {
                parentRef.current.scrollTop = targetScrollTop;
            }

            if (nextTarget.restorePassesRemaining <= 1) {
                expansionScrollRestoreRef.current = null;
                expansionScrollRestoreRafRef.current = null;
                return;
            }

            expansionScrollRestoreRef.current = {
                ...nextTarget,
                restorePassesRemaining: nextTarget.restorePassesRemaining - 1
            };

            if (typeof requestAnimationFrame === 'function') {
                expansionScrollRestoreRafRef.current = requestAnimationFrame(applyScrollRestore);
            } else {
                applyScrollRestore();
            }
        };

        applyScrollRestore();

        return () => {
            if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
                cancelAnimationFrame(expansionScrollRestoreRafRef.current);
                expansionScrollRestoreRafRef.current = null;
            }
        };
    }, [serverSide, hasRenderedData, centerRows.length, topRows.length, bottomRows.length, renderedOffset, dataVersion, rowVirtualizer]);

    const appliedRowPinning = tableState.rowPinning || { top: [], bottom: [] };

    const resolvedTopRows = useMemo(() => {
        if (!(appliedRowPinning.top && appliedRowPinning.top.length > 0)) return topRows;
        return appliedRowPinning.top
            .map((id) =>
                topRows.find((row) => row && row.id === id) ||
                centerRows.find((row) => row && row.id === id) ||
                bottomRows.find((row) => row && row.id === id) ||
                pinnedRowCacheRef.current.get(id)
            )
            .filter(Boolean);
    }, [appliedRowPinning.top, topRows, centerRows, bottomRows]);
    const resolvedBottomRows = useMemo(() => {
        if (!(appliedRowPinning.bottom && appliedRowPinning.bottom.length > 0)) return bottomRows;
        return appliedRowPinning.bottom
            .map((id) =>
                bottomRows.find((row) => row && row.id === id) ||
                centerRows.find((row) => row && row.id === id) ||
                topRows.find((row) => row && row.id === id) ||
                pinnedRowCacheRef.current.get(id)
            )
            .filter(Boolean);
    }, [appliedRowPinning.bottom, topRows, centerRows, bottomRows]);
    const effectiveTopRows = (serverSide && hasRenderedData && resolvedTopRows.length === 0 && centerRows.length === 0)
        ? lastStableRowModelRef.current.topRows
        : resolvedTopRows;
    const effectiveCenterRows = (serverSide && hasRenderedData && centerRows.length === 0)
        ? lastStableRowModelRef.current.centerRows
        : centerRows;
    const effectiveBottomRows = (serverSide && hasRenderedData && centerRows.length === 0 && resolvedBottomRows.length === 0)
        ? lastStableRowModelRef.current.bottomRows
        : resolvedBottomRows;
    const clientRowVirtualizer = useVirtualizer({
        count: effectiveCenterRows.length,
        getScrollElement: () => parentRef.current,
        estimateSize: () => rowHeight,
        overscan: 12,
    });
    const activeRowVirtualizer = serverSide ? rowVirtualizer : clientRowVirtualizer;
    activeRowVirtualizerRef.current = activeRowVirtualizer;

    useLayoutEffect(() => {
        if (serverSide) return;
        activeRowVirtualizer.measure();
    }, [serverSide, activeRowVirtualizer, effectiveCenterRows.length, rowHeight]);

    const rowModelLookup = useMemo(() => {
        const lookup = new Map();
        [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows].forEach(row => {
            if (row && row.id) {
                lookup.set(row.id, row);
            }
        });
        return lookup;
    }, [effectiveTopRows, effectiveCenterRows, effectiveBottomRows]);

    useEffect(() => {
        const orderedRows = [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows];
        displayRowsRef.current = orderedRows;
        displayRowIndexRef.current = new Map(
            orderedRows
                .filter((row) => row && row.id)
                .map((row, index) => [row.id, index])
        );
        pinnedDisplayMetaRef.current = {
            topCount: effectiveTopRows.length,
            centerCount: effectiveCenterRows.length
        };
    }, [effectiveTopRows, effectiveCenterRows, effectiveBottomRows]);

    // Progressive subtree expansion: each time the backend returns new data,
    // scan it for descendants of the target path that still have children and
    // haven't been expanded yet. Auto-expand them and let the cycle continue
    // until every reachable descendant is expanded.
    useEffect(() => {
        if (!serverSide || !data || !subtreeExpandRef.current) return;
        const { path, expandedPaths } = subtreeExpandRef.current;

        const toExpand = data.filter(row => {
            if (!row || !row._path || !row._has_children) return false;
            const inSubtree = row._path === path || row._path.startsWith(path + '|||');
            return inSubtree && !expandedPaths.has(row._path);
        });

        if (toExpand.length === 0) {
            subtreeExpandRef.current = null;
            return;
        }

        toExpand.forEach(row => expandedPaths.add(row._path));

        captureExpansionScrollPosition();
        clearCache();
        setExpanded(prev => {
            const base = (prev !== null && typeof prev === 'object') ? prev : {};
            const next = { ...base };
            toExpand.forEach(row => { next[row._path] = true; });
            return next;
        });
    }, [data, serverSide]); // intentionally omits captureExpansionScrollPosition/clearCache/setExpanded — stable refs

    // Deferred block invalidation after single-row expansion.
    // We wait for the expansion response to land (dataVersion bumps) before
    // invalidating subsequent blocks. This ensures only ONE backend request fires
    // (the expanded sync), with no concurrent viewport request to race against it.
    // After the anchor block is updated with fresh data, blocks beyond it are
    // deleted so they get re-fetched on next scroll (their row indices shifted).
    useEffect(() => {
        if (!serverSide || !pendingExpansionRef.current) return;
        const { anchorBlock, expandedRowVirtualIndex, oldRowCount, extendedToBlock = -1 } = pendingExpansionRef.current;
        pendingExpansionRef.current = null;
        if (anchorBlock < 0) {
            // The expanded row was not in the viewport when the user toggled it.
            // We can't know which anchor block shifted, but a hard clear causes
            // a full skeleton flash.  Soft-invalidate all blocks from 0 instead
            // so existing rows stay visible (stale-while-revalidate) until fresh
            // data lands (finding #6).
            if (softInvalidateFromBlock) softInvalidateFromBlock(0);
        } else {
            // The expansion request was extended to cover through extendedToBlock
            // (anchorBlock + 1 when the anchor is known).  Those blocks were filled
            // with fresh data by the data-sync effect, so we must NOT re-dirty them.
            // Start soft-invalidating from the first block BEYOND the fresh coverage.
            const firstStaleBlock = extendedToBlock >= 0 ? extendedToBlock + 1 : anchorBlock + 1;
            if (softInvalidateFromBlock) softInvalidateFromBlock(firstStaleBlock);
        }
        // Clear the transition loader now that the expansion response has landed.
        setPendingRowTransitions(new Map());

        // Viewport anchor preservation.
        // When rows are inserted or removed ABOVE the current scroll position, the
        // virtualizer re-layouts and the same pixel offset now shows a different
        // logical row. Compensate by shifting scrollTop so that the user continues
        // to see the same rows they were looking at before the toggle.
        if (
            parentRef.current &&
            typeof expandedRowVirtualIndex === 'number' &&
            typeof oldRowCount === 'number' &&
            rowHeight > 0
        ) {
            const rowDelta = (rowCount || 0) - (oldRowCount || 0);
            if (rowDelta !== 0) {
                // Y position of the expanded/collapsed row (uniform row heights).
                const expandedRowY = expandedRowVirtualIndex * rowHeight;
                const currentScrollTop = parentRef.current.scrollTop;
                // Only compensate when the anchor row is entirely ABOVE the viewport.
                // If it is at or inside the viewport the inserted children appear
                // naturally below it and no scroll adjustment is needed.
                if (expandedRowY + rowHeight <= currentScrollTop) {
                    const newScrollTop = Math.max(0, currentScrollTop + rowDelta * rowHeight);
                    parentRef.current.scrollTop = newScrollTop;
                    if (rowVirtualizer.scrollToOffset) {
                        rowVirtualizer.scrollToOffset(newScrollTop);
                    }
                }
            }
        }
    }, [dataVersion, serverSide, rowCount, rowHeight, parentRef, rowVirtualizer]); // fires when expansion response arrives

    // Debug effect removed (finding #10 — hot-path logging).

    const visibleLeafColumns = table.getVisibleLeafColumns();
    const chartDisplayRows = useMemo(
        () => [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows],
        [effectiveTopRows, effectiveCenterRows, effectiveBottomRows]
    );
    const liveSelectionChartModel = useMemo(
        () => (
            chartPanelType === 'combo'
                ? buildComboSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
                    hierarchyLevel: chartPanelHierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                    layers: chartPanelLayers,
                })
                : buildSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
                    orientation: chartPanelOrientation,
                    hierarchyLevel: chartPanelHierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                })
        ),
        [selectedCells, chartDisplayRows, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]
    );
    const livePivotChartModel = useMemo(
        () => (
            chartPanelType === 'combo'
                ? buildComboPivotChartModel(
                    serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows,
                    visibleLeafColumns,
                    {
                        hierarchyLevel: chartPanelHierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: chartPanelRowLimit,
                        maxColumns: chartPanelColumnLimit,
                        rowFields,
                        colFields,
                        sortMode: chartPanelSortMode,
                        layers: chartPanelLayers,
                    }
                )
                : buildPivotChartModel(
                    serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows,
                    visibleLeafColumns,
                    {
                        orientation: chartPanelOrientation,
                        hierarchyLevel: chartPanelHierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: chartPanelRowLimit,
                        maxColumns: chartPanelColumnLimit,
                        rowFields,
                        colFields,
                        sortMode: chartPanelSortMode,
                    }
                )
        ),
        [chartDisplayRows, loadedRows, serverSide, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]
    );
    const overlayChartData = chartPaneDataById[TABLE_OVERLAY_CHART_PANE_ID] || null;
    const activeChartPanelModel = useMemo(() => {
        if (chartPanelLocked) {
            if (
                chartPanelSource === 'pivot'
                && overlayChartData
                && overlayChartData.requestSignature
                && chartPanelLockedRequest
                && overlayChartData.requestSignature === chartPanelLockedRequest.requestSignature
                && Array.isArray(overlayChartData.rows)
            ) {
                return chartPanelType === 'combo'
                    ? buildComboPivotChartModel(overlayChartData.rows, resolveChartModelColumns(overlayChartData, (chartPanelLockedRequest.visibleColumns || visibleLeafColumns)), {
                        hierarchyLevel: chartPanelHierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: chartPanelRowLimit,
                        maxColumns: chartPanelColumnLimit,
                        rowFields,
                        colFields,
                        sortMode: chartPanelSortMode,
                        layers: chartPanelLayers,
                    })
                    : buildPivotChartModel(overlayChartData.rows, resolveChartModelColumns(overlayChartData, (chartPanelLockedRequest.visibleColumns || visibleLeafColumns)), {
                        orientation: chartPanelOrientation,
                        hierarchyLevel: chartPanelHierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: chartPanelRowLimit,
                        maxColumns: chartPanelColumnLimit,
                        rowFields,
                        colFields,
                        sortMode: chartPanelSortMode,
                    });
            }
            return chartPanelLockedModel || (chartPanelSource === 'selection' ? liveSelectionChartModel : livePivotChartModel);
        }
        if (
            chartPanelSource === 'pivot'
            && overlayChartData
            && Array.isArray(overlayChartData.rows)
        ) {
            const overlayModelColumns = resolveChartModelColumns(overlayChartData, visibleLeafColumns);
            return chartPanelType === 'combo'
                ? buildComboPivotChartModel(overlayChartData.rows, overlayModelColumns, {
                    hierarchyLevel: chartPanelHierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                    layers: chartPanelLayers,
                })
                : buildPivotChartModel(overlayChartData.rows, overlayModelColumns, {
                    orientation: chartPanelOrientation,
                    hierarchyLevel: chartPanelHierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                });
        }
        return chartPanelSource === 'selection' ? liveSelectionChartModel : livePivotChartModel;
    }, [
        chartPanelLocked,
        chartPanelLayers,
        chartPanelLockedModel,
        chartPanelLockedRequest,
        chartPanelSource,
        chartPanelType,
        chartPanelOrientation,
        chartPanelHierarchyLevel,
        chartPanelRowLimit,
        chartPanelColumnLimit,
        chartPanelSortMode,
        colFields,
        livePivotChartModel,
        liveSelectionChartModel,
        overlayChartData,
        rowFields,
        visibleLeafColumns,
    ]);
    const chartCanvasPaneModels = useMemo(() => {
        const nextModels = {};
        chartCanvasPanes.forEach((pane) => {
            if (!pane || !pane.id) return;
            if (pane.locked) {
                if (
                    pane.source === 'pivot'
                    && pane.lockedRequest
                    && pane.lockedRequest.requestSignature
                    && chartPaneDataById[pane.id]
                    && chartPaneDataById[pane.id].requestSignature === pane.lockedRequest.requestSignature
                    && Array.isArray(chartPaneDataById[pane.id].rows)
                ) {
                    const paneModelColumns = resolveChartModelColumns(
                        chartPaneDataById[pane.id],
                        (pane.lockedRequest && pane.lockedRequest.visibleColumns) || visibleLeafColumns
                    );
                    nextModels[pane.id] = pane.chartType === 'combo'
                        ? buildComboPivotChartModel(
                            chartPaneDataById[pane.id].rows,
                            paneModelColumns,
                            {
                                hierarchyLevel: pane.hierarchyLevel,
                                maxHierarchyLevel: rowFields.length,
                                maxRows: pane.rowLimit,
                                maxColumns: pane.columnLimit,
                                rowFields,
                                colFields,
                                sortMode: pane.sortMode,
                                layers: pane.chartLayers,
                            }
                        )
                        : buildPivotChartModel(
                            chartPaneDataById[pane.id].rows,
                            paneModelColumns,
                            {
                                orientation: pane.orientation,
                                hierarchyLevel: pane.hierarchyLevel,
                                maxHierarchyLevel: rowFields.length,
                                maxRows: pane.rowLimit,
                                maxColumns: pane.columnLimit,
                                rowFields,
                                colFields,
                                sortMode: pane.sortMode,
                            }
                        );
                    return;
                }
                nextModels[pane.id] = pane.lockedModel || null;
                return;
            }

            if (pane.source === 'selection') {
                nextModels[pane.id] = pane.chartType === 'combo'
                    ? buildComboSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
                        hierarchyLevel: pane.hierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: pane.rowLimit,
                        maxColumns: pane.columnLimit,
                        rowFields,
                        colFields,
                        sortMode: pane.sortMode,
                        layers: pane.chartLayers,
                    })
                    : buildSelectionChartModel(selectedCells, chartDisplayRows, visibleLeafColumns, {
                        orientation: pane.orientation,
                        hierarchyLevel: pane.hierarchyLevel,
                        maxHierarchyLevel: rowFields.length,
                        maxRows: pane.rowLimit,
                        maxColumns: pane.columnLimit,
                        rowFields,
                        colFields,
                        sortMode: pane.sortMode,
                    });
                return;
            }

            const paneRows = (
                serverSide
                && chartPaneDataById[pane.id]
                && Array.isArray(chartPaneDataById[pane.id].rows)
            )
                ? chartPaneDataById[pane.id].rows
                : (serverSide && Array.isArray(loadedRows) && loadedRows.length > 0 ? loadedRows : chartDisplayRows);
            const paneModelColumns = resolveChartModelColumns(chartPaneDataById[pane.id], visibleLeafColumns);

            nextModels[pane.id] = pane.chartType === 'combo'
                ? buildComboPivotChartModel(paneRows, paneModelColumns, {
                    hierarchyLevel: pane.hierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: pane.rowLimit,
                    maxColumns: pane.columnLimit,
                    rowFields,
                    colFields,
                    sortMode: pane.sortMode,
                    layers: pane.chartLayers,
                })
                : buildPivotChartModel(paneRows, paneModelColumns, {
                    orientation: pane.orientation,
                    hierarchyLevel: pane.hierarchyLevel,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: pane.rowLimit,
                    maxColumns: pane.columnLimit,
                    rowFields,
                    colFields,
                    sortMode: pane.sortMode,
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
        const lockedRequestBase = (
            serverSide
            && chartPanelSource === 'pivot'
            ? buildChartRequestBase({
                chartType: chartPanelType,
                chartLayers: chartPanelLayers,
                rowLimit: chartPanelRowLimit,
                columnLimit: chartPanelColumnLimit,
                serverScope: chartPanelServerScope,
            }, stateOverride)
            : null
        );
        const requestSignature = lockedRequestBase
            ? JSON.stringify({
                paneId: TABLE_OVERLAY_CHART_PANE_ID,
                locked: true,
                request: lockedRequestBase,
                stateOverride,
            })
            : null;
        setChartPanelLockedModel(nextLockedModel);
        setChartPanelLockedRequest(lockedRequestBase ? {
            request: lockedRequestBase,
            stateOverride,
            visibleColumns: resolveChartModelColumns(overlayChartData, visibleLeafColumns),
            requestSignature,
        } : null);
        setChartPanelLocked(true);
    }, [
        activeChartPanelModel,
        buildChartRequestBase,
        buildChartStateOverrideSnapshot,
        chartPanelColumnLimit,
        chartPanelLayers,
        chartPanelLocked,
        chartPanelRowLimit,
        chartPanelServerScope,
        chartPanelSource,
        chartPanelType,
        overlayChartData,
        serverSide,
        visibleLeafColumns,
    ]);
    const handleToggleChartCanvasPaneLock = useCallback((paneId) => {
        const targetPane = chartCanvasPanes.find((pane) => pane.id === paneId);
        if (!targetPane) return;
        if (targetPane.locked) {
            updateChartCanvasPane(paneId, {
                locked: false,
                lockedModel: null,
                lockedRequest: null,
            });
            return;
        }
        const targetModel = cloneSerializable(chartCanvasPaneModels[paneId], null);
        const stateOverride = buildChartStateOverrideSnapshot();
        const lockedRequestBase = (
            serverSide
            && targetPane.source === 'pivot'
            ? buildChartRequestBase({
                chartType: targetPane.chartType,
                chartLayers: targetPane.chartLayers,
                rowLimit: targetPane.rowLimit,
                columnLimit: targetPane.columnLimit,
                serverScope: targetPane.serverScope,
            }, stateOverride)
            : null
        );
        const requestSignature = lockedRequestBase
            ? JSON.stringify({
                paneId,
                locked: true,
                request: lockedRequestBase,
                stateOverride,
            })
            : null;
        updateChartCanvasPane(paneId, {
            locked: true,
            lockedModel: targetModel,
            lockedRequest: lockedRequestBase ? {
                request: lockedRequestBase,
                stateOverride,
                visibleColumns: resolveChartModelColumns(chartPaneDataById[paneId], visibleLeafColumns),
                requestSignature,
            } : null,
        });
    }, [
        buildChartRequestBase,
        buildChartStateOverrideSnapshot,
        chartCanvasPaneModels,
        chartCanvasPanes,
        chartPaneDataById,
        serverSide,
        updateChartCanvasPane,
        visibleLeafColumns,
    ]);
    const applyChartTargetFilters = useCallback((target) => {
        if (!target || typeof target !== 'object' || !target.fieldValues || typeof target.fieldValues !== 'object') return;

        const targetFields = target.kind === 'column' ? colFields : rowFields;
        if (!Array.isArray(targetFields) || targetFields.length === 0) return;

        setFilters((previousFilters) => {
            const nextFilters = { ...(previousFilters || {}) };
            targetFields.forEach((field) => {
                if (Object.prototype.hasOwnProperty.call(target.fieldValues, field)) {
                    nextFilters[field] = {
                        operator: 'AND',
                        conditions: [{
                            type: 'eq',
                            value: String(target.fieldValues[field]),
                            caseSensitive: false,
                        }],
                    };
                } else if (nextFilters[field]) {
                    delete nextFilters[field];
                }
            });
            return nextFilters;
        });
    }, [colFields, rowFields]);

    const activateChartCategory = useCallback((source, interactionMode, target) => {
        if (!target || typeof target !== 'object') return;

        if (setPropsRef.current) {
            setPropsRef.current({
                chartEvent: {
                    type: 'category_activate',
                    source,
                    interactionMode,
                    target,
                    timestamp: Date.now(),
                },
            });
        }

        if (interactionMode === 'event') return;

        if (interactionMode === 'filter') {
            applyChartTargetFilters(target);
            return;
        }

        if (target.kind === 'column' && target.columnId) {
            const targetColumnIndex = visibleLeafColumns.findIndex((column) => column.id === target.columnId);
            if (targetColumnIndex >= 0) {
                scrollToDisplayColumn(targetColumnIndex);
            }
            return;
        }

        if (target.kind !== 'row') return;

        const displayRows = getDisplayRows();
        const matchingRow = displayRows.find((row) => {
            if (!row) return false;
            const rowPath = row.original && row.original._path ? row.original._path : null;
            return (
                (target.rowPath && rowPath === target.rowPath)
                || (target.rowId && row.id === target.rowId)
            );
        });

        if (!matchingRow) {
            if (serverSide) {
                showNotification('Chart item is outside the current loaded grid slice.', 'info');
            }
            return;
        }

        handleRowSelect(matchingRow, false, false);
        scrollToDisplayRow(resolveDisplayRowIndex(matchingRow.id, matchingRow.index || 0));
    }, [
        applyChartTargetFilters,
        getDisplayRows,
        handleRowSelect,
        resolveDisplayRowIndex,
        scrollToDisplayColumn,
        scrollToDisplayRow,
        serverSide,
        showNotification,
        visibleLeafColumns,
    ]);
    const handleChartCategoryActivate = useCallback((target) => {
        activateChartCategory(chartPanelSource, chartPanelInteractionMode, target);
    }, [activateChartCategory, chartPanelInteractionMode, chartPanelSource]);
    useEffect(() => {
        if (chartPanelBarLayout !== 'stacked') return;
        const canKeepStacked = chartPanelType === 'area'
            ? (activeChartPanelModel && Array.isArray(activeChartPanelModel.series) && activeChartPanelModel.series.length > 1)
            : canStackBarLayout(activeChartPanelModel);
        if (canKeepStacked) return;
        setChartPanelBarLayout('grouped');
    }, [activeChartPanelModel, chartPanelBarLayout, chartPanelType]);
    const openSelectionChart = useCallback((selectionMap = selectedCells) => {
        const hasSelection = Object.keys(selectionMap || {}).length > 0;
        if (!hasSelection) {
            showNotification('Select a range of value cells first.', 'warning');
            return;
        }
        const modalChartType = (chartPanelType === 'icicle' || chartPanelType === 'sunburst' || chartPanelType === 'sankey')
            ? 'bar'
            : chartPanelType;
        const buildModel = (orientationValue, hierarchyValue) => (
            modalChartType === 'combo'
                ? buildComboSelectionChartModel(selectionMap, chartDisplayRows, visibleLeafColumns, {
                    hierarchyLevel: hierarchyValue,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                    layers: chartPanelLayers,
                })
                : buildSelectionChartModel(selectionMap, chartDisplayRows, visibleLeafColumns, {
                    orientation: orientationValue,
                    hierarchyLevel: hierarchyValue,
                    maxHierarchyLevel: rowFields.length,
                    maxRows: chartPanelRowLimit,
                    maxColumns: chartPanelColumnLimit,
                    rowFields,
                    colFields,
                    sortMode: chartPanelSortMode,
                })
        );
        const preferredOrientation = chartPanelOrientation === 'columns' ? 'columns' : 'rows';
        let defaultOrientation = preferredOrientation;
        let defaultHierarchyLevel = chartPanelHierarchyLevel;
        let defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);

        if ((!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) && defaultHierarchyLevel !== 'all') {
            defaultHierarchyLevel = 'all';
            defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
        }
        if (!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) {
            defaultOrientation = defaultOrientation === 'columns' ? 'rows' : 'columns';
            defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
        }
        if ((!defaultChartModel || !Array.isArray(defaultChartModel.series) || defaultChartModel.series.length === 0) && defaultHierarchyLevel !== 'all') {
            defaultHierarchyLevel = 'all';
            defaultChartModel = buildModel(defaultOrientation, defaultHierarchyLevel);
        }
        const canOpenStacked = modalChartType === 'area'
            ? (defaultChartModel && Array.isArray(defaultChartModel.series) && defaultChartModel.series.length > 1)
            : canStackBarLayout(defaultChartModel);
        setChartModal({
            title: 'Range Chart',
            chartType: modalChartType,
            chartLayers: cloneSerializable(chartPanelLayers, []),
            barLayout: chartPanelBarLayout === 'stacked' && canOpenStacked ? 'stacked' : 'grouped',
            axisMode: chartPanelAxisMode,
            defaultOrientation,
            defaultHierarchyLevel,
            selectionMap: { ...(selectionMap || {}) },
            visibleRows: chartDisplayRows,
            visibleCols: visibleLeafColumns,
            maxHierarchyLevel: rowFields.length,
            rowLimit: chartPanelRowLimit,
            columnLimit: chartPanelColumnLimit,
            rowFields,
            colFields,
            sortMode: chartPanelSortMode,
        });
    }, [selectedCells, chartDisplayRows, visibleLeafColumns, chartPanelType, chartPanelLayers, chartPanelBarLayout, chartPanelAxisMode, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, showNotification]);

    // 1. Row Virtualizer (Managed by useServerSideRowModel)
    const virtualRows = activeRowVirtualizer.getVirtualItems();
    const activeColumnSkeletonCount = Math.max(pendingColumnSkeletonCount, pendingHorizontalColumnCount);
    const showColumnLoadingSkeletons = serverSide && (
        (structuralInFlight && pendingColumnSkeletonCount > 0) ||
        (isHorizontalColumnRequestPending && pendingHorizontalColumnCount > 0)
    );
    const columnSkeletonWidth = defaultColumnWidths.schemaFallback;
    const stickyHeaderHeight = getStickyHeaderHeight(table.getHeaderGroups().length, rowHeight, showFloatingFilters);
    const bodyRowsTopOffset = stickyHeaderHeight + (effectiveTopRows.length * rowHeight);

    useEffect(() => {
        if (!serverSide || virtualRows.length === 0) return;
        const firstRow = virtualRows[0].index;
        const lastRow = virtualRows[virtualRows.length - 1].index;
        latestViewportRef.current = {
            start: firstRow,
            end: lastRow,
            count: Math.max(1, lastRow - firstRow + 1)
        };
    }, [serverSide, virtualRows]);

    const liveChartStateFingerprint = useMemo(
        () => JSON.stringify(buildChartStateOverrideSnapshot() || {}),
        [buildChartStateOverrideSnapshot]
    );
    const chartRequestCandidates = useMemo(() => {
        if (!serverSide || structuralInFlight || needsColSchema || totalCenterCols === null) return [];
        const nextCandidates = [];

        if (chartPanelOpen && chartPanelSource === 'pivot') {
            if (!(chartPanelLocked && !chartPanelLockedRequest)) {
                const baseRequest = chartPanelLocked && chartPanelLockedRequest
                    ? chartPanelLockedRequest.request
                    : buildChartRequestBase({
                        chartType: chartPanelType,
                        chartLayers: chartPanelLayers,
                        rowLimit: chartPanelRowLimit,
                        columnLimit: chartPanelColumnLimit,
                        serverScope: chartPanelServerScope,
                    });
                if (baseRequest) {
                    const signature = chartPanelLocked && chartPanelLockedRequest
                        ? (chartPanelLockedRequest.requestSignature || JSON.stringify({
                            paneId: TABLE_OVERLAY_CHART_PANE_ID,
                            locked: true,
                            request: baseRequest,
                            stateOverride: chartPanelLockedRequest.stateOverride || null,
                        }))
                        : JSON.stringify({
                            paneId: TABLE_OVERLAY_CHART_PANE_ID,
                            locked: false,
                            request: baseRequest,
                            state: liveChartStateFingerprint,
                        });
                    nextCandidates.push({
                        paneId: TABLE_OVERLAY_CHART_PANE_ID,
                        signature,
                        request: {
                            ...baseRequest,
                            state_override: chartPanelLocked && chartPanelLockedRequest
                                ? chartPanelLockedRequest.stateOverride || undefined
                                : undefined,
                        },
                    });
                }
            }
        }

        chartCanvasPanes.forEach((pane) => {
            if (!pane || pane.source !== 'pivot') return;
            if (pane.locked && !pane.lockedRequest) return;
            const baseRequest = pane.locked && pane.lockedRequest
                ? pane.lockedRequest.request
                : buildChartRequestBase({
                    chartType: pane.chartType,
                    chartLayers: pane.chartLayers,
                    rowLimit: pane.rowLimit,
                    columnLimit: pane.columnLimit,
                    serverScope: pane.serverScope,
                });
            if (!baseRequest) return;
            const signature = pane.locked && pane.lockedRequest
                ? (pane.lockedRequest.requestSignature || JSON.stringify({
                    paneId: pane.id,
                    locked: true,
                    request: baseRequest,
                    stateOverride: pane.lockedRequest.stateOverride || null,
                }))
                : JSON.stringify({
                    paneId: pane.id,
                    locked: false,
                    request: baseRequest,
                    state: liveChartStateFingerprint,
                });
            nextCandidates.push({
                paneId: pane.id,
                signature,
                request: {
                    ...baseRequest,
                    state_override: pane.locked && pane.lockedRequest
                        ? pane.lockedRequest.stateOverride || undefined
                        : undefined,
                },
            });
        });

        return nextCandidates;
    }, [
        buildChartRequestBase,
        chartCanvasPanes,
        chartPanelColumnLimit,
        chartPanelLayers,
        chartPanelLocked,
        chartPanelLockedRequest,
        chartPanelOpen,
        chartPanelRowLimit,
        chartPanelServerScope,
        chartPanelSource,
        chartPanelType,
        liveChartStateFingerprint,
        needsColSchema,
        serverSide,
        structuralInFlight,
        totalCenterCols,
    ]);
    const previousChartRenderSignaturesRef = useRef({});
    useEffect(() => {
        const nextSignatures = {
            [TABLE_OVERLAY_CHART_PANE_ID]: JSON.stringify({
                open: chartPanelOpen,
                source: chartPanelSource,
                chartType: chartPanelType,
                seriesColumnIds: getRequestedChartSeriesColumnIds(chartPanelType, chartPanelLayers),
                locked: chartPanelLocked,
                request: chartPanelLockedRequest ? chartPanelLockedRequest.requestSignature : null,
                rowLimit: chartPanelRowLimit,
                columnLimit: chartPanelColumnLimit,
                orientation: chartPanelOrientation,
                hierarchyLevel: chartPanelHierarchyLevel,
                sortMode: chartPanelSortMode,
                serverScope: chartPanelServerScope,
                state: chartPanelLocked ? null : liveChartStateFingerprint,
            }),
        };
        chartCanvasPanes.forEach((pane) => {
            nextSignatures[pane.id] = JSON.stringify({
                source: pane.source,
                chartType: pane.chartType,
                seriesColumnIds: getRequestedChartSeriesColumnIds(pane.chartType, pane.chartLayers),
                locked: pane.locked,
                request: pane.lockedRequest ? pane.lockedRequest.requestSignature : null,
                rowLimit: pane.rowLimit,
                columnLimit: pane.columnLimit,
                orientation: pane.orientation,
                hierarchyLevel: pane.hierarchyLevel,
                sortMode: pane.sortMode,
                serverScope: pane.serverScope,
                state: pane.locked ? null : liveChartStateFingerprint,
            });
        });

        const previousSignatures = previousChartRenderSignaturesRef.current || {};
        const changedPaneIds = Object.keys(nextSignatures).filter((paneId) => previousSignatures[paneId] && previousSignatures[paneId] !== nextSignatures[paneId]);
        previousChartRenderSignaturesRef.current = nextSignatures;
        if (changedPaneIds.length === 0) return;
        setChartPaneDataById((previousData) => {
            let mutated = false;
            const nextData = { ...previousData };
            changedPaneIds.forEach((paneId) => {
                if (Object.prototype.hasOwnProperty.call(nextData, paneId)) {
                    delete nextData[paneId];
                    mutated = true;
                }
            });
            return mutated ? nextData : previousData;
        });
    }, [
        chartCanvasPanes,
        chartPanelColumnLimit,
        chartPanelHierarchyLevel,
        chartPanelLayers,
        chartPanelLocked,
        chartPanelLockedRequest,
        chartPanelOpen,
        chartPanelOrientation,
        chartPanelRowLimit,
        chartPanelServerScope,
        chartPanelSortMode,
        chartPanelSource,
        chartPanelType,
        liveChartStateFingerprint,
    ]);
    useEffect(() => {
        if (!chartData || typeof chartData !== 'object') return;
        const paneId = chartData.paneId || TABLE_OVERLAY_CHART_PANE_ID;
        setChartPaneDataById((previousData) => ({
            ...previousData,
            [paneId]: chartData,
        }));
        if (chartData.requestSignature) {
            completedChartRequestSignaturesRef.current[paneId] = chartData.requestSignature;
        }
        if (
            activeChartRequestRef.current
            && activeChartRequestRef.current.paneId === paneId
            && (
                !chartData.requestSignature
                || activeChartRequestRef.current.signature === chartData.requestSignature
            )
        ) {
            activeChartRequestRef.current = null;
        }
    }, [chartData]);

    useEffect(() => {
        if (
            activeChartRequestRef.current
            && !chartRequestCandidates.some((candidate) => (
                candidate.paneId === activeChartRequestRef.current.paneId
                && candidate.signature === activeChartRequestRef.current.signature
            ))
        ) {
            activeChartRequestRef.current = null;
        }
    }, [chartRequestCandidates]);

    useEffect(() => {
        if (!serverSide || structuralInFlight || !setPropsRef.current || needsColSchema || totalCenterCols === null) return;
        if (activeChartRequestRef.current) return;
        const nextCandidate = chartRequestCandidates.find((candidate) => (
            completedChartRequestSignaturesRef.current[candidate.paneId] !== candidate.signature
        ));
        if (!nextCandidate) return;
        chartRequestSeqRef.current = Math.max(chartRequestSeqRef.current, requestVersionRef.current) + 1;
        activeChartRequestRef.current = {
            paneId: nextCandidate.paneId,
            signature: nextCandidate.signature,
        };
        setPropsRef.current({
            chartRequest: {
                ...nextCandidate.request,
                pane_id: nextCandidate.paneId,
                request_signature: nextCandidate.signature,
                version: chartRequestSeqRef.current,
                window_seq: chartRequestSeqRef.current,
                state_epoch: stateEpoch,
                session_id: sessionIdRef.current,
                client_instance: clientInstanceRef.current,
                abort_generation: abortGeneration,
                intent: 'chart',
            },
        });
    }, [
        abortGeneration,
        chartRequestCandidates,
        needsColSchema,
        serverSide,
        stateEpoch,
        structuralInFlight,
        totalCenterCols,
    ]);

    // 2. Column Virtualizer (Extracted)
    const {
        columnVirtualizer,
        virtualCenterCols,
        beforeWidth,
        afterWidth,
        totalLayoutWidth,
        leftCols,
        rightCols,
        centerCols
    } = useColumnVirtualizer({
        parentRef,
        table
    });
    columnVirtualizerRef.current = columnVirtualizer;
    pinnedColumnMetaRef.current = {
        leftCount: leftCols.length,
        centerCount: centerCols.length,
        rightCount: rightCols.length
    };

    useLayoutEffect(() => {
        if (!parentRef.current || !columnVirtualizer) return;
        // Keep horizontal virtualization in sync after pivot group collapse/expand.
        // Without this clamp, right-edge stale indices can point past center columns
        // and render blank numeric cells.
        const scrollEl = parentRef.current;
        const maxScrollLeft = Math.max(0, scrollEl.scrollWidth - scrollEl.clientWidth);
        if (scrollEl.scrollLeft > maxScrollLeft) {
            // Clamp the DOM scroll position.
            scrollEl.scrollLeft = maxScrollLeft;
            // Synchronously update the virtualizer's internal scrollOffset BEFORE
            // calling measure(). measure() calls notify() which triggers a React
            // re-render; if scrollOffset is still the old out-of-bounds value at
            // that point the re-render will show only the last column (blank
            // numbers in all others) until the async DOM scroll event fires.
            // Direct mutation is safe here — scrollOffset is a plain instance
            // property that getScrollOffset() reads directly.
            columnVirtualizer.scrollOffset = maxScrollLeft;
        }
        columnVirtualizer.measure();
    }, [columnVirtualizer, centerCols.length, totalLayoutWidth]);

    // Memoized lookup structures for the header render path.
    // centerColIndexMap: O(1) id→index lookup; only rebuilt when the column list changes.
    // visibleLeafIndexSet: O(1) membership check; only rebuilt when the virtual window shifts.
    const centerColIndexMap = useMemo(
        () => new Map(centerCols.map((c, i) => [c.id, i])),
        [centerCols]
    );
    const visibleLeafIndexSet = useMemo(
        () => new Set(virtualCenterCols.map(v => v.index)),
        [virtualCenterCols]
    );

    // O(1) colId → visible-leaf-index map for renderCell; rebuilt only when column list changes.
    const visibleLeafColIndexMap = useMemo(
        () => new Map(table.getVisibleLeafColumns().map((c, i) => [c.id, i])),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [table.getVisibleLeafColumns()]
    );

    // Sync the column virtualizer's visible range into state so useServerSideRowModel
    // can detect column window changes and trigger re-fetches.
    useEffect(() => {
        if (virtualCenterCols.length === 0) return;
        const newStart = virtualCenterCols[0].index;
        const newEnd = virtualCenterCols[virtualCenterCols.length - 1].index;
        setVisibleColRange(prev => {
            // Opening or resizing the chart panel narrows the table viewport, which
            // can shrink the virtual end index even though the user did not
            // horizontally navigate. Preserve the wider loaded column window so
            // server-side chart data and already-loaded cell values do not vanish
            // simply because the panel consumes layout width.
            if ((chartPanelOpen || chartCanvasPanes.length > 0) && newStart === prev.start && newEnd < prev.end) {
                return prev;
            }
            if (prev.start === newStart && prev.end === newEnd) return prev;
            return { start: newStart, end: newEnd };
        });
    }, [virtualCenterCols, chartPanelOpen, chartCanvasPanes.length]);

    useEffect(() => {
        if (!serverSide || virtualCenterCols.length === 0) return;
        debugLog('horizontal-scroll-state', {
            scrollLeft: parentRef.current ? parentRef.current.scrollLeft : null,
            visibleColRange,
            virtualRange: {
                start: virtualCenterCols[0].index,
                end: virtualCenterCols[virtualCenterCols.length - 1].index,
                count: virtualCenterCols.length,
            },
            requestedColRange: {
                start: colRequestStart,
                end: colRequestEnd,
                needsColSchema,
            },
            totalCenterCols,
            pendingColumnSkeletonCount,
            pendingHorizontalColumnCount,
            showColumnLoadingSkeletons,
            isHorizontalColumnRequestPending,
            isRequestPending,
            structuralInFlight,
        });
    }, [
        serverSide,
        virtualCenterCols,
        visibleColRange,
        colRequestStart,
        colRequestEnd,
        needsColSchema,
        totalCenterCols,
        pendingColumnSkeletonCount,
        pendingHorizontalColumnCount,
        showColumnLoadingSkeletons,
        isHorizontalColumnRequestPending,
        isRequestPending,
        structuralInFlight,
        parentRef,
    ]);

    // Use the custom hook
    const { getHeaderStickyStyle, getStickyStyle } = useStickyStyles(
        theme,
        leftCols,
        rightCols
    );


    const getFieldZone = (id) => {
        if (id === 'hierarchy') return 'rows';
        if (rowFields.includes(id)) return 'rows';
        if (colFields.includes(id)) return 'cols';
        if (valConfigs.find(v => id.includes(v.field))) return 'vals';
        return null;
    };

    const getFieldIndex = (id, zone) => {
        if (zone === 'rows') return rowFields.indexOf(id);
        if (zone === 'cols') return colFields.indexOf(id);
        if (zone === 'vals') return valConfigs.findIndex(v => id.includes(v.field));
        return -1;
    };

    const onHeaderDrop = (e, targetColId) => {
        e.preventDefault();
        if (!dragItem) return;
        const { field, zone: srcZone } = dragItem;
        const fieldName = typeof field === 'string' ? field : field.field;
        
        // Handle dropping on the same column (no-op)
        if (fieldName === targetColId) {
            setDragItem(null);
            return;
        }

        const targetZone = getFieldZone(targetColId);
        if (!targetZone) {
            setDragItem(null);
            return;
        }

        // Check for Pinning (Drag-to-Pin)
        const targetIsPinned = getPinningState(targetColId);
        if (targetIsPinned) {
             handlePinColumn(fieldName, targetIsPinned);
        } else {
             // If dropping on unpinned, maybe unpin?
             handlePinColumn(fieldName, false);
        }

        // Reordering or Pivoting
        if (srcZone === targetZone) {
             const srcIdx = getFieldIndex(fieldName, srcZone);
             const targetIdx = getFieldIndex(targetColId, targetZone);
             if (srcIdx !== -1 && targetIdx !== -1 && srcIdx !== targetIdx) {
                 const move = (list, setList) => {
                    const n = [...list]; 
                    const [moved] = n.splice(srcIdx, 1);
                    n.splice(targetIdx, 0, moved); 
                    setList(n);
                };
                if (srcZone==='rows') move(rowFields, setRowFields);
                if (srcZone==='cols') move(colFields, setColFields);
                if (srcZone==='vals') move(valConfigs, setValConfigs);
             }
        } else {
            // Pivoting (Moving between zones)
            const targetIdx = getFieldIndex(targetColId, targetZone);
            // Remove from source
            if (srcZone==='rows') setRowFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='cols') setColFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='vals') setValConfigs(p=>p.filter(f=>f.field!==fieldName));

            // Insert into target
            const insert = (list, setList, item) => {
                const n = [...list];
                n.splice(targetIdx, 0, item);
                setList(n);
            };
            if (targetZone==='rows') insert(rowFields, setRowFields, fieldName);
            if (targetZone==='cols') insert(colFields, setColFields, fieldName);
            if (targetZone==='vals') insert(valConfigs, setValConfigs, {field: fieldName, agg:'sum'});
        }
        setDragItem(null);
    };

    const onDragStart = (e, field, zone, idx) => {
        setDragItem({ field, zone, idx });
        e.dataTransfer.effectAllowed = 'move';
    };
    const onDragOver = (e, zone, idx) => {
        e.preventDefault();
        // If hovering over a header, zone might be 'cols' or 'rows' derived from ID
        // For sidebar, we use dropLine logic
        if (['rows', 'cols', 'vals', 'filter'].includes(zone)) {
            const rect = e.currentTarget.getBoundingClientRect();
            const mid = rect.top + rect.height / 2;
            setDropLine({ zone, idx: e.clientY > mid ? idx + 1 : idx });
        }
    };
    const onDrop = (e, targetZone) => {
        e.preventDefault();
        if (!dragItem) return;
        const { field, zone: srcZone, idx: srcIdx } = dragItem;
        const targetIdx = (dropLine && dropLine.idx) || 0;
        const insertItem = (list, idx, item) => { const n = [...list]; n.splice(idx, 0, item); return n; };
        const fieldName = typeof field === 'string' ? field : field.field;
        if (!fieldName || typeof fieldName !== 'string') { setDragItem(null); setDropLine(null); return; }
        if (srcZone !== targetZone) {
            if (srcZone==='rows') setRowFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='cols') setColFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='vals') setValConfigs(p=>p.filter((_,i)=>i!==srcIdx));
            if (targetZone==='rows') setRowFields(p=>p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName));
            if (targetZone==='cols') setColFields(p=>p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName));
            if (targetZone==='vals') setValConfigs(p=>insertItem(p, targetIdx, {field: fieldName, agg:'sum'}));
            if (targetZone==='filter' && !filters.hasOwnProperty(fieldName)) setFilters(p=>({...p, [fieldName]: ''}));
        } else {
            const move = (list, setList) => {
                const n = [...list]; const [moved] = n.splice(srcIdx, 1);
                let ins = targetIdx; if (srcIdx < targetIdx) ins -= 1;
                n.splice(ins, 0, moved); setList(n);
            };
            if (targetZone==='rows') move(rowFields, setRowFields);
            if (targetZone==='cols') move(colFields, setColFields);
            if (targetZone==='vals') move(valConfigs, setValConfigs);
        }
        setDragItem(null); setDropLine(null);
    };





    const buildExportAoa = (allRows) => {
        // Use table.getHeaderGroups() so we get the real multi-level header structure
        // with correct parent/child relationships and colSpans set by TanStack.
        const headerGroups = table.getHeaderGroups();

        // Identify leaf (data) columns from the last header group, excluding
        // internal/UI-only columns that should not appear in the export.
        const SKIP_COL_IDS = new Set(['__row_number__']);
        const lastHeaderGroup = headerGroups[headerGroups.length - 1];
        const leafHeaders = (lastHeaderGroup && lastHeaderGroup.headers != null ? lastHeaderGroup.headers : [])
            .filter(h => !SKIP_COL_IDS.has(h.column.id) && !h.isPlaceholder);

        const leafCount = leafHeaders.length;

        // Build one AOA row per header group.
        // For each header row, we fill a flat array of length leafCount.
        // A header with colSpan > 1 occupies that many leaf slots; placeholders fill gaps.
        const headerAoaRows = [];
        const allMerges = [];

        headerGroups.forEach((hg, rowIdx) => {
            const aoaRow = new Array(leafCount).fill('');
            let leafPos = 0;
            hg.headers.forEach(h => {
                if (SKIP_COL_IDS.has(h.column.id)) return;
                const span = (h.colSpan != null ? h.colSpan : 1);
                if (!h.isPlaceholder) {
                    // Resolve header text — prefer columnDef.header string, fall back to id
                    const colDef = h.column.columnDef;
                    let headerText = '';
                    if (typeof colDef.header === 'string') {
                        headerText = colDef.header;
                    } else if (typeof h.column.id === 'string') {
                        // Strip group_ prefix and internal path separators for cleaner output
                        headerText = h.column.id
                            .replace(/^group_/, '')
                            .replace(/\|\|\|/g, ' > ');
                    }
                    aoaRow[leafPos] = headerText;
                    if (span > 1 && rowIdx < headerGroups.length - 1) {
                        // Merge across the span; row 0-indexed in the final aoa
                        allMerges.push({ s: { r: rowIdx, c: leafPos }, e: { r: rowIdx, c: leafPos + span - 1 } });
                    }
                }
                leafPos += span;
            });
            headerAoaRows.push(aoaRow);
        });

        // If there is only one header group and it looks identical to itself
        // (no real parent grouping), just keep one header row to avoid duplication.
        const dedupedHeaderRows = headerAoaRows.length > 1
            ? headerAoaRows
            : headerAoaRows;  // keep as-is for single group (flat table)

        // Build data rows — include ALL rows (totals + data rows)
        // Track max content width per column for auto-sizing.
        const colWidths = leafHeaders.map(h => {
            const colDef = h.column.columnDef;
            return typeof colDef.header === 'string' ? colDef.header.length : (h.column.id != null ? h.column.id : '').length;
        });

        const dataRows = allRows.map(r => {
            return leafHeaders.map((h, ci) => {
                const col = h.column;
                const colId = col.id;
                const colDef = col.columnDef;

                let val;
                if (colId === 'hierarchy') {
                    // Hierarchy column: indent using spaces to reflect depth
                    const depth = (r.original && r.original.depth != null ? r.original.depth : (r.depth != null ? r.depth : 0));
                    const label = (r.original && r.original._isTotal) ? ((r.original && r.original._id != null) ? r.original._id : 'Total') : ((r.original && r.original._id != null) ? r.original._id : '');
                    val = '\u00A0\u00A0'.repeat(depth) + label;  // non-breaking spaces for Excel visibility
                } else if (typeof colDef.accessorFn === 'function') {
                    // Use accessorFn to get the value (same as TanStack does internally)
                    val = colDef.accessorFn(r.original, r.index);
                } else if (colDef.accessorKey) {
                    val = (r.original && r.original[colDef.accessorKey]);
                } else {
                    val = '';
                }

                // Normalize: undefined/null → empty string; keep numbers as numbers
                if (val === undefined || val === null) val = '';

                // Track max width for column auto-sizing
                const cellLen = String(val).length;
                if (cellLen > colWidths[ci]) colWidths[ci] = cellLen;

                return val;
            });
        });

        // Build ws['!cols'] — cap at 60 chars to avoid overly wide columns
        const wsCols = colWidths.map(w => ({ wch: Math.min(Math.max(w + 2, 8), 60) }));

        return {
            aoa: [...dedupedHeaderRows, ...dataRows],
            merges: allMerges,
            wsCols,
            headerRowCount: dedupedHeaderRows.length,
        };
    };

    const fetchDrillData = useCallback(async (rowPath, page = 0, sortCol = null, sortDir = 'asc', filterText = '') => {
        const params = new URLSearchParams({
            table: tableName,
            row_path: rowPath,
            row_fields: rowFields.join(','),
            page: String(page),
            page_size: '100',
        });
        if (sortCol) { params.set('sort_col', sortCol); params.set('sort_dir', sortDir); }
        if (filterText) params.set('filter', filterText);

        setDrillModal(prev => ({ ...(prev || { path: rowPath, rows: [], page: 0, totalRows: 0, sortCol, sortDir, filterText }), loading: true }));
        try {
            const resp = await fetch(`${drillEndpoint}?${params.toString()}`);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const json = await resp.json();
            setDrillModal({ loading: false, path: rowPath, rows: json.rows || [], page: json.page || 0, totalRows: json.total_rows || 0, sortCol, sortDir, filterText });
        } catch (err) {
            console.error('Drill-through fetch failed:', err);
            setDrillModal(null);
        }
    }, [tableName, rowFields, drillEndpoint]);

    const handleCellDrillThrough = useCallback((row, colId) => {
        if (!row || !row.original) return;
        const rowPath = row.original._path;
        if (!rowPath || rowPath === '__grand_total__') return;  // skip total rows
        fetchDrillData(rowPath, 0, null, 'asc', '');
    }, [fetchDrillData]);

    const exportPivot = useCallback(() => {
        const XLSX_LIMIT = 500000;
        const allRows = table.getRowModel().rows;  // full row model, not just virtual window

        const isCSV = (rowCount || 0) > XLSX_LIMIT;

        if (isCSV) {
            // CSV path — flat, no merge support needed
            // Use TanStack visible leaf columns so we match what's shown on screen,
            // and skip internal UI-only columns.
            const SKIP_CSV = new Set(['__row_number__']);
            const leafCols = table.getVisibleLeafColumns().filter(c => !SKIP_CSV.has(c.id));

            const escape = (v) => {
                if (v == null) return '';
                const s = String(v);
                return (s.includes(',') || s.includes('"') || s.includes('\n'))
                    ? `"${s.replace(/"/g, '""')}"` : s;
            };
            const header = leafCols.map(c => {
                const h = (c.columnDef && c.columnDef.header);
                return escape(typeof h === 'string' ? h : (c.id != null ? c.id : ''));
            }).join(',');
            const lines = allRows.map(r =>
                leafCols.map(c => {
                    if (c.id === 'hierarchy') {
                        const depth = (r.original && r.original.depth != null ? r.original.depth : (r.depth != null ? r.depth : 0));
                        return escape('  '.repeat(depth) + (r.original && r.original._id != null ? r.original._id : ''));
                    }
                    const val = typeof (c.columnDef && c.columnDef.accessorFn) === 'function'
                        ? c.columnDef.accessorFn(r.original, r.index)
                        : ((c.columnDef && c.columnDef.accessorKey) ? (r.original && r.original[c.columnDef.accessorKey]) : '');
                    return escape(val != null ? val : '');
                }).join(',')
            );
            const blob = new Blob([[header, ...lines].join('\n')], { type: 'text/csv;charset=utf-8;' });
            saveAs(blob, 'pivot.csv');
        } else {
            // XLSX path — multi-level headers + hierarchy indent
            const { aoa, merges, wsCols } = buildExportAoa(allRows);
            const ws = XLSX.utils.aoa_to_sheet(aoa);
            if (merges.length > 0) ws['!merges'] = merges;
            if (wsCols && wsCols.length > 0) ws['!cols'] = wsCols;
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, 'Pivot');
            const buf = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
            saveAs(new Blob([buf], { type: 'application/octet-stream' }), 'pivot.xlsx');
        }
    }, [rows, columns, rowCount, table]);

    const { renderCell, renderHeaderCell } = useRenderHelpers({
        // renderCell dependencies
        serverSide,
        selectedCells,
        fillRange,
        dragStart,
        theme,
        getStickyStyle,
        isDarkTheme,
        handleCellMouseDown,
        handleCellMouseEnter,
        handleContextMenu,
        handleFillMouseDown,
        visibleLeafColIndexMap,
        lastSelected,
        styles,
        getConditionalStyle,
        // renderHeaderCell dependencies (synchronous / render-time reads)
        rowHeight,
        table,
        leftCols,
        centerCols,
        rightCols,
        columnPinning,
        sorting,
        handleHeaderContextMenu,
        autoSizeColumn,
        autoSizeBounds,
        hoveredHeaderId,
        setHoveredHeaderId,
        focusedHeaderId,
        setFocusedHeaderId,
        onDragStart,
        handleFilterClick,
        handleHeaderFilter,
        activeFilterCol,
        filterAnchorEl,
        closeFilterPopover,
        activeFilterOptions,
        filters,
        selectedCols,
        getHeaderStickyStyle,
        dataBarsColumns,
        colorScaleStats,
        cellFormatRules,
    });

    const srOnly = {
        position: 'absolute',
        width: '1px',
        height: '1px',
        padding: 0,
        margin: '-1px',
        overflow: 'hidden',
        clip: 'rect(0, 0, 0, 0)',
        whiteSpace: 'nowrap',
        border: 0,
        pointerEvents: 'none'
    };

    // Add focus styles to interactive elements
    const focusStyle = {
        outline: `2px solid ${theme.primary}`,
        outlineOffset: '2px'
    };
    const dockedChartCanvasPanes = chartCanvasPanes.filter((pane) => !pane.floating);
    const floatingChartCanvasPanes = chartCanvasPanes.filter((pane) => pane.floating);

    return (
        <div id={id} style={{ ...styles.root, ...loadingCssVars, position: 'relative', ...style }}>
            <style>{loadingAnimationStyles}</style>
            <div style={srOnly} role="status" aria-live="polite">{announcement}</div>
            {/* Cinema mode exit button — visible only when cinema mode is active */}
            {cinemaMode && (
                <button
                    onClick={() => setCinemaMode(false)}
                    title="Exit Cinema Mode"
                    style={{
                        position: 'absolute', top: 12, right: 12, zIndex: 9999,
                        background: 'rgba(0,0,0,0.55)', color: '#fff',
                        border: 'none', borderRadius: 8, padding: '6px 14px',
                        cursor: 'pointer', fontSize: 13, fontWeight: 600,
                        display: 'flex', alignItems: 'center', gap: 6,
                        backdropFilter: 'blur(4px)',
                        opacity: 0.7,
                        transition: 'opacity 0.15s',
                    }}
                    onMouseEnter={e => e.currentTarget.style.opacity = 1}
                    onMouseLeave={e => e.currentTarget.style.opacity = 0.7}
                >
                    ✕ Exit Cinema
                </button>
            )}
            {/* PivotAppBar is intentionally outside PivotErrorBoundary so that
                the global search input (and other toolbar controls) are not
                unmounted on every dataVersion change. */}
            {!cinemaMode && <PivotAppBar
                cinemaMode={cinemaMode} setCinemaMode={setCinemaMode}
                sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}
                themeName={themeName} setThemeName={setThemeName}
                themeOverrides={themeOverrides} setThemeOverrides={setThemeOverrides}
                showRowNumbers={showRowNumbers} setShowRowNumbers={setShowRowNumbers}
                showFloatingFilters={showFloatingFilters} setShowFloatingFilters={setShowFloatingFilters}
                stickyHeaders={stickyHeaders} setStickyHeaders={setStickyHeaders}
                showRowTotals={showRowTotals} setShowRowTotals={setShowRowTotals}
                showColTotals={showColTotals} setShowColTotals={setShowColTotals}
                spacingMode={spacingMode} setSpacingMode={setSpacingMode} spacingLabels={spacingLabels}
                layoutMode={layoutMode} setLayoutMode={setLayoutMode}
                onAutoSizeColumns={autoSizeVisibleColumns}
                onTransposePivot={handleTransposePivot}
                canTranspose={rowFields.length > 0 || colFields.length > 0}
                colorScaleMode={colorScaleMode} setColorScaleMode={setColorScaleMode}
                colorPalette={colorPalette} setColorPalette={setColorPalette}
                rowCount={rowCount} exportPivot={exportPivot}
                theme={theme} styles={styles}
                filters={filters} setFilters={setFilters}
                onSaveView={handleSaveView}
                pivotTitle={props.pivotTitle}
                fontFamily={fontFamily} setFontFamily={setFontFamily}
                fontSize={fontSize} setFontSize={setFontSize}
                zoomLevel={zoomLevel} setZoomLevel={setZoomLevel}
                decimalPlaces={decimalPlaces} setDecimalPlaces={setDecimalPlaces}
                columnDecimalOverrides={columnDecimalOverrides} setColumnDecimalOverrides={setColumnDecimalOverrides}
                cellFormatRules={cellFormatRules} setCellFormatRules={setCellFormatRules}
                selectedCells={selectedCells}
                dataBarsColumns={dataBarsColumns} setDataBarsColumns={setDataBarsColumns}
                canCreateSelectionChart={Object.keys(selectedCells || {}).length > 0}
                onCreateSelectionChart={() => openSelectionChart()}
                onAddChartPane={handleAddChartCanvasPane}
            />}
        <PivotErrorBoundary key={dataVersion}>
            <div style={{display:'flex', flex:1, overflow:'hidden', fontFamily: fontFamily, fontSize: fontSize, zoom: zoomLevel / 100}}>
                {!cinemaMode && sidebarOpen && (
                    <SidebarPanel
                        sidebarTab={sidebarTab} setSidebarTab={setSidebarTab}
                        rowFields={rowFields} setRowFields={setRowFields}
                        colFields={colFields} setColFields={setColFields}
                        valConfigs={valConfigs} setValConfigs={setValConfigs}
                        filters={filters} setFilters={setFilters}
                        columnVisibility={columnVisibility} setColumnVisibility={setColumnVisibility}
                        columnPinning={columnPinning} setColumnPinning={setColumnPinning}
                        availableFields={availableFields}
                        table={table}
                        pinningPresets={pinningPresets}
                        theme={theme} styles={styles}
                        showNotification={showNotification}
                        filterAnchorEl={filterAnchorEl} setFilterAnchorEl={setFilterAnchorEl}
                        colSearch={colSearch} setColSearch={setColSearch}
                        colTypeFilter={colTypeFilter} setColTypeFilter={setColTypeFilter}
                        selectedCols={selectedCols} setSelectedCols={setSelectedCols}
                        dropLine={dropLine}
                        onDragStart={onDragStart} onDragOver={onDragOver} onDrop={onDrop}
                        handleHeaderFilter={handleHeaderFilter}
                        handleFilterClick={handleFilterClick}
                        requestFilterOptions={requestFilterOptions}
                        handleExpandAllRows={handleExpandAllRows}
                        handlePinColumn={handlePinColumn}
                        toggleAllColumnsPinned={toggleAllColumnsPinned}
                        activeFilterCol={activeFilterCol}
                        closeFilterPopover={closeFilterPopover}
                        filterOptions={filterOptions}
                        data={data}
                        sidebarWidth={sidebarWidth}
                        setSidebarWidth={setSidebarWidth}
                    />
                )}
                <div ref={chartCanvasLayoutRef} style={{ display:'flex', flex:1, minWidth:0, minHeight:0, overflow:'hidden', position:'relative' }}>
                    <div
                        ref={chartLayoutRef}
                        style={{
                            display:'flex',
                            flexDirection:'column',
                            flexGrow: tableCanvasSize,
                            flexBasis: 0,
                            minWidth: `${MIN_TABLE_PANEL_WIDTH}px`,
                            minHeight: 0,
                            overflow:'hidden',
                            position:'relative',
                        }}
                    >
                        <div style={{ display:'flex', flex:1, minWidth:0, minHeight:0, overflow:'hidden' }}>
                            <PivotTableBody
                                parentRef={parentRef}
                                handleKeyDown={handleKeyDown}
                                rows={rows}
                                visibleLeafColumns={visibleLeafColumns}
                                totalLayoutWidth={totalLayoutWidth}
                                beforeWidth={beforeWidth}
                                afterWidth={afterWidth}
                                bodyRowsTopOffset={bodyRowsTopOffset}
                                stickyHeaderHeight={stickyHeaderHeight}
                                effectiveTopRows={effectiveTopRows}
                                effectiveBottomRows={effectiveBottomRows}
                                effectiveCenterRows={effectiveCenterRows}
                                virtualRows={virtualRows}
                                virtualCenterCols={virtualCenterCols}
                                rowVirtualizer={activeRowVirtualizer}
                                rowModelLookup={rowModelLookup}
                                getRow={getRow}
                                serverSide={serverSide}
                                serverSidePinsGrandTotal={serverSidePinsGrandTotal}
                                pendingRowTransitions={pendingRowTransitions}
                                table={table}
                                leftCols={leftCols}
                                centerCols={centerCols}
                                rightCols={rightCols}
                                centerColIndexMap={centerColIndexMap}
                                visibleLeafIndexSet={visibleLeafIndexSet}
                                rowHeight={rowHeight}
                                showFloatingFilters={showFloatingFilters}
                                stickyHeaders={stickyHeaders}
                                showColTotals={showColTotals}
                                grandTotalPosition={grandTotalPosition}
                                showColumnLoadingSkeletons={showColumnLoadingSkeletons}
                                pendingColumnSkeletonCount={activeColumnSkeletonCount}
                                columnSkeletonWidth={columnSkeletonWidth}
                                theme={theme}
                                styles={styles}
                                renderCell={renderCell}
                                renderHeaderCell={renderHeaderCell}
                                filters={filters}
                                handleHeaderFilter={handleHeaderFilter}
                                selectedCells={selectedCells}
                                rowCount={statusRowCount}
                                isRequestPending={isRequestPending}
                            />
                        </div>
                    </div>
                    {dockedChartCanvasPanes.map((pane, index) => (
                        <React.Fragment key={pane.id}>
                            <div
                                onMouseDown={(event) => {
                                    event.preventDefault();
                                    handleStartChartCanvasResize(index === 0 ? 'table' : dockedChartCanvasPanes[index - 1].id, pane.id, event);
                                }}
                                style={{
                                    width: '8px',
                                    cursor: 'col-resize',
                                    background: 'transparent',
                                    position: 'relative',
                                    flexShrink: 0,
                                }}
                                title="Resize workspace panes"
                            >
                                <div style={{
                                    position: 'absolute',
                                    top: '50%',
                                    left: '50%',
                                    transform: 'translate(-50%, -50%)',
                                    width: '3px',
                                    height: '78px',
                                    borderRadius: '999px',
                                    background: theme.border,
                                    opacity: 0.92,
                                }} />
                            </div>
                            <div
                                style={{
                                    display:'flex',
                                    flexGrow: pane.size,
                                    flexBasis: 0,
                                    minWidth: `${MIN_CHART_CANVAS_PANE_WIDTH}px`,
                                    minHeight: 0,
                                    overflow:'hidden',
                                    borderLeft: `1px solid ${theme.border}`,
                                }}
                            >
                                <PivotChartPanel
                                    open
                                    onClose={() => handleRemoveChartCanvasPane(pane.id)}
                                    source={pane.source}
                                    onSourceChange={(value) => updateChartCanvasPane(pane.id, { source: value })}
                                    chartType={pane.chartType}
                                    onChartTypeChange={(value) => updateChartCanvasPane(pane.id, { chartType: value })}
                                    chartLayers={pane.chartLayers}
                                    onChartLayersChange={(value) => updateChartCanvasPane(pane.id, { chartLayers: value })}
                                    availableColumns={resolveChartAvailableColumns(chartPaneDataById[pane.id], visibleLeafColumns)}
                                    barLayout={pane.barLayout}
                                    onBarLayoutChange={(value) => updateChartCanvasPane(pane.id, { barLayout: value })}
                                    axisMode={pane.axisMode}
                                    onAxisModeChange={(value) => updateChartCanvasPane(pane.id, { axisMode: value })}
                                    orientation={pane.orientation}
                                    onOrientationChange={(value) => updateChartCanvasPane(pane.id, { orientation: value })}
                                    hierarchyLevel={pane.hierarchyLevel}
                                    onHierarchyLevelChange={(value) => updateChartCanvasPane(pane.id, { hierarchyLevel: value })}
                                    chartTitle={pane.chartTitle || pane.name}
                                    onChartTitleChange={(value) => updateChartCanvasPane(pane.id, { chartTitle: value })}
                                    rowLimit={pane.rowLimit}
                                    onRowLimitChange={(value) => updateChartCanvasPane(pane.id, { rowLimit: value })}
                                    columnLimit={pane.columnLimit}
                                    onColumnLimitChange={(value) => updateChartCanvasPane(pane.id, { columnLimit: value })}
                                    chartHeight={pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT}
                                    onChartHeightChange={(value) => {
                                        const nextHeight = Number(value);
                                        updateChartCanvasPane(pane.id, {
                                            chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT,
                                        });
                                    }}
                                    sortMode={pane.sortMode}
                                    onSortModeChange={(value) => updateChartCanvasPane(pane.id, { sortMode: value })}
                                    interactionMode={pane.interactionMode}
                                    onInteractionModeChange={(value) => updateChartCanvasPane(pane.id, { interactionMode: value })}
                                    serverScope={pane.serverScope}
                                    onServerScopeChange={(value) => updateChartCanvasPane(pane.id, { serverScope: value })}
                                    showServerScope={serverSide}
                                    model={chartCanvasPaneModels[pane.id] || null}
                                    theme={theme}
                                    onCategoryActivate={(target) => activateChartCategory(pane.source, pane.interactionMode, target)}
                                    floating={false}
                                    onToggleFloating={() => handleToggleChartCanvasPaneFloating(pane.id)}
                                    floatingRect={pane.floatingRect}
                                    onFloatingDragStart={(event) => handleStartChartCanvasPaneFloatingDrag(pane.id, event)}
                                    onFloatingResizeStart={(direction, event) => handleStartChartCanvasPaneFloatingResize(pane.id, direction, event)}
                                    standalone
                                    showResizeHandle={false}
                                    title={pane.name}
                                    showDefinitionManager={false}
                                    locked={pane.locked}
                                    onToggleLock={() => handleToggleChartCanvasPaneLock(pane.id)}
                                    cinemaMode={Boolean(pane.cinemaMode)}
                                    onCinemaModeChange={(value) => updateChartCanvasPane(pane.id, { cinemaMode: Boolean(value) })}
                                />
                            </div>
                        </React.Fragment>
                    ))}
                    {floatingChartCanvasPanes.map((pane) => (
                        <PivotChartPanel
                            key={pane.id}
                            open
                            onClose={() => handleRemoveChartCanvasPane(pane.id)}
                            source={pane.source}
                            onSourceChange={(value) => updateChartCanvasPane(pane.id, { source: value })}
                            chartType={pane.chartType}
                            onChartTypeChange={(value) => updateChartCanvasPane(pane.id, { chartType: value })}
                            chartLayers={pane.chartLayers}
                            onChartLayersChange={(value) => updateChartCanvasPane(pane.id, { chartLayers: value })}
                            availableColumns={resolveChartAvailableColumns(chartPaneDataById[pane.id], visibleLeafColumns)}
                            barLayout={pane.barLayout}
                            onBarLayoutChange={(value) => updateChartCanvasPane(pane.id, { barLayout: value })}
                            axisMode={pane.axisMode}
                            onAxisModeChange={(value) => updateChartCanvasPane(pane.id, { axisMode: value })}
                            orientation={pane.orientation}
                            onOrientationChange={(value) => updateChartCanvasPane(pane.id, { orientation: value })}
                            hierarchyLevel={pane.hierarchyLevel}
                            onHierarchyLevelChange={(value) => updateChartCanvasPane(pane.id, { hierarchyLevel: value })}
                            chartTitle={pane.chartTitle || pane.name}
                            onChartTitleChange={(value) => updateChartCanvasPane(pane.id, { chartTitle: value })}
                            rowLimit={pane.rowLimit}
                            onRowLimitChange={(value) => updateChartCanvasPane(pane.id, { rowLimit: value })}
                            columnLimit={pane.columnLimit}
                            onColumnLimitChange={(value) => updateChartCanvasPane(pane.id, { columnLimit: value })}
                            chartHeight={pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT}
                            onChartHeightChange={(value) => {
                                const nextHeight = Number(value);
                                updateChartCanvasPane(pane.id, {
                                    chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT,
                                });
                            }}
                            sortMode={pane.sortMode}
                            onSortModeChange={(value) => updateChartCanvasPane(pane.id, { sortMode: value })}
                            interactionMode={pane.interactionMode}
                            onInteractionModeChange={(value) => updateChartCanvasPane(pane.id, { interactionMode: value })}
                            serverScope={pane.serverScope}
                            onServerScopeChange={(value) => updateChartCanvasPane(pane.id, { serverScope: value })}
                            showServerScope={serverSide}
                            model={chartCanvasPaneModels[pane.id] || null}
                            theme={theme}
                            onCategoryActivate={(target) => activateChartCategory(pane.source, pane.interactionMode, target)}
                            floating
                            onToggleFloating={() => handleToggleChartCanvasPaneFloating(pane.id)}
                            floatingRect={pane.floatingRect}
                            onFloatingDragStart={(event) => handleStartChartCanvasPaneFloatingDrag(pane.id, event)}
                            onFloatingResizeStart={(direction, event) => handleStartChartCanvasPaneFloatingResize(pane.id, direction, event)}
                            standalone
                            showResizeHandle={false}
                            title={pane.name}
                            showDefinitionManager={false}
                            locked={pane.locked}
                            onToggleLock={() => handleToggleChartCanvasPaneLock(pane.id)}
                            cinemaMode={Boolean(pane.cinemaMode)}
                            onCinemaModeChange={(value) => updateChartCanvasPane(pane.id, { cinemaMode: Boolean(value) })}
                        />
                    ))}
                </div>
            </div>
            {contextMenu && <ContextMenu {...contextMenu} theme={theme} onClose={() => setContextMenu(null)} />}
            {notification && <Notification message={notification.message} type={notification.type} onClose={() => setNotification(null)} />}
            <DrillThroughModal
                drillState={drillModal}
                onClose={() => setDrillModal(null)}
                onPageChange={(newPage) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, newPage, drillModal.sortCol, drillModal.sortDir, drillModal.filterText);
                }}
                onSort={(col, dir) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, 0, col, dir, drillModal.filterText);
                }}
                onFilter={(text) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, 0, drillModal.sortCol, drillModal.sortDir, text);
                }}
            />
            <PivotChartModal
                chartState={chartModal}
                onClose={() => setChartModal(null)}
                theme={theme}
            />
        </PivotErrorBoundary>
        </div>
    );
};

DashTanstackPivot.propTypes = {
    id: PropTypes.string,
    table: PropTypes.string,
    pivotTitle: PropTypes.string,
        data: PropTypes.arrayOf(PropTypes.object),
        setProps: PropTypes.func,
        style: PropTypes.object,
        serverSide: PropTypes.bool,
        rowCount: PropTypes.number,
        rowFields: PropTypes.array,
        colFields: PropTypes.array,
        valConfigs: PropTypes.arrayOf(PropTypes.shape({
            field: PropTypes.string,
            agg: PropTypes.oneOf([
                'sum',
                'avg',
                'count',
                'min',
                'max',
                'weighted_avg',
                'wavg',
                'weighted_mean',
                'formula',
            ]),
            weightField: PropTypes.string,
            windowFn: PropTypes.oneOf([
                'percent_of_row',
                'percent_of_col',
                'percent_of_grand_total',
            ]),
            format: PropTypes.string,
            percentile: PropTypes.number,
            separator: PropTypes.string,
            formula: PropTypes.string,
            label: PropTypes.string,
        })),
        filters: PropTypes.object,
        sorting: PropTypes.array,
        expanded: PropTypes.oneOfType([PropTypes.object, PropTypes.bool]),
        columns: PropTypes.array,
    
    cinemaMode: PropTypes.bool,
    showRowTotals: PropTypes.bool,
    showColTotals: PropTypes.bool,
    grandTotalPosition: PropTypes.oneOf(['top', 'bottom']),
    filterOptions: PropTypes.object,
    chartData: PropTypes.object,
    chartRequest: PropTypes.object,
    chartEvent: PropTypes.object,
    chartDefinitions: PropTypes.arrayOf(PropTypes.object),
    chartDefaults: PropTypes.object,
    chartCanvasPanes: PropTypes.arrayOf(PropTypes.object),
    tableCanvasSize: PropTypes.number,
    chartServerWindow: PropTypes.shape({
        enabled: PropTypes.bool,
        rows: PropTypes.number,
        columns: PropTypes.number,
        scope: PropTypes.oneOf(['viewport', 'root']),
    }),
    viewport: PropTypes.object,
    cellUpdate: PropTypes.object,
    cellUpdates: PropTypes.arrayOf(PropTypes.object),
    rowMove: PropTypes.object,
    drillThrough: PropTypes.object,
    drillEndpoint: PropTypes.string,
    viewState: PropTypes.object,
    saveViewTrigger: PropTypes.any,
    savedView: PropTypes.object,
    conditionalFormatting: PropTypes.arrayOf(PropTypes.object),
    validationRules: PropTypes.object,
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
    columnSizing: PropTypes.object,
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
    sortEvent: PropTypes.object,
    availableFieldList: PropTypes.arrayOf(PropTypes.string),
    dataOffset: PropTypes.number,
    dataVersion: PropTypes.number,
};


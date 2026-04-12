// DashTanstackPivot - Enterprise Grade Pivot Table
import React, { useMemo, useState, useRef, useEffect, useLayoutEffect, useCallback } from 'react';
import PropTypes from 'prop-types';
import {
    useReactTable,
    getCoreRowModel,
    getExpandedRowModel,
    getGroupedRowModel,
    getPaginationRowModel,
} from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import { themes, getStyles, isDarkTheme, gridDimensionTokens, deriveEditedCellThemeTokens, deriveStructuralThemeTokens } from '../utils/styles';
import { exportPivotTable } from '../utils/exportUtils';
import { DEFAULT_FIELD_PANEL_SIZES, sanitizeFieldPanelSizes } from '../utils/fieldPanelLayout';
import Icons from '../utils/Icons';
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
import { useServerSideViewportController } from '../hooks/useServerSideViewportController';
import {
    DEFAULT_NUMBER_GROUP_SEPARATOR,
    formatAggLabel,
    formatValue,
    formatDisplayLabel,
    getAllLeafIdsFromColumn,
    getKey,
    isGroupColumn,
    normalizeNumberGroupSeparator,
} from '../utils/helpers';
import {
    normalizeEditingConfig,
    resolveColumnEditSpec,
    resolveEditorOptionsSource,
    validateEditorValue,
} from '../utils/editing';
import { getPivotProfiler, isPivotProfilingEnabled } from '../utils/pivotProfiler';
import ContextMenu from './Table/ContextMenu';
import { PivotAppBar } from './PivotAppBar';
import { SidebarPanel } from './Sidebar/SidebarPanel';
import DetailSidePanel from './Table/DetailSidePanel';
import EditSidePanel from './Table/EditSidePanel';
import DetailDrawer from './Table/DetailDrawer';
import {
    buildPivotChartModel,
    buildComboPivotChartModel,
    buildComboSelectionChartModel,
    buildSelectionChartModel,
} from '../utils/chartModelBuilders';
import {
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
import { useChartState } from '../hooks/useChartState';
import { useFormatState } from '../hooks/useFormatState';
import { useCellInteraction } from '../hooks/useCellInteraction';
import { useSidebarUI } from '../hooks/useSidebarUI';
import { useReportMode } from '../hooks/useReportMode';
import { useDetailDrillThrough } from '../hooks/useDetailDrillThrough';
import { PivotThemeProvider } from '../contexts/PivotThemeContext';
import { PivotConfigProvider } from '../contexts/PivotConfigContext';
import { PivotValueDisplayProvider } from '../contexts/PivotValueDisplayContext';
import { normalizeSortingState, updateSortingForColumn } from '../utils/sorting';

const DEFAULT_CHART_PANEL_ROW_LIMIT = 50;
const DEFAULT_CHART_PANEL_COLUMN_LIMIT = 10;
const DEFAULT_CHART_GRAPH_HEIGHT = 320;
const DEFAULT_FLOATING_CHART_PANEL_HEIGHT = 520;
const MIN_CHART_PANEL_WIDTH = 280;
const MAX_CHART_PANEL_WIDTH = 960;
const MIN_FLOATING_CHART_PANEL_HEIGHT = 280;
const MIN_TABLE_PANEL_WIDTH = 0;
const MIN_CHART_CANVAS_PANE_WIDTH = 320;
const DEFAULT_DOCKED_CHART_PANE_HEIGHT = 420;
const MIN_DOCKED_CHART_PANE_HEIGHT = 180;
const MIN_TABLE_PANEL_HEIGHT = 200;
const VALID_CHART_DOCK_POSITIONS = new Set(['left', 'right', 'top', 'bottom']);
const DEFAULT_TABLE_CANVAS_SIZE = 1.4;
const TABLE_OVERLAY_CHART_PANE_ID = '__table_overlay_chart__';
const EDITING_TEMPORARILY_DISABLED = true;
const MAX_AUTO_SIZE_SAMPLE_ROWS = 300;
const MAX_PENDING_ROW_TRANSITIONS = 128;
const VALID_CHART_TYPES = new Set(['bar', 'line', 'area', 'sparkline', 'combo', 'pie', 'donut', 'scatter', 'waterfall', 'icicle', 'sunburst', 'sankey']);
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

const VALID_VIEW_MODES = new Set(['pivot', 'report', 'tree', 'table']);
const VALID_DETAIL_MODES = new Set(['none', 'inline', 'sidepanel', 'drawer']);
const SOFT_CENTER_COLUMN_WARNING_THRESHOLD = 2000;
const HARD_CENTER_COLUMN_WARNING_THRESHOLD = 10000;
const clampOptionalInteger = (value, min, max) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return null;
    return Math.max(min, Math.min(max, Math.floor(normalized)));
};

const VALID_TREE_DISPLAY_MODES = new Set(['singleColumn', 'multipleColumns']);
const VALID_DETAIL_REFRESH_STRATEGIES = new Set(['rows', 'everything', 'nothing']);
const VALID_TRANSACTION_REFRESH_MODES = new Set(['none', 'viewport', 'smart', 'structural', 'full', 'patch']);
const VALID_TRANSACTION_EVENT_ACTIONS = new Set(['undo', 'redo', 'revert', 'replace']);
const MAX_TRANSACTION_HISTORY_ENTRIES = 100;

const normalizeViewModeValue = (value) => (
    VALID_VIEW_MODES.has(value) ? value : null
);

const normalizeDetailModeValue = (value) => (
    VALID_DETAIL_MODES.has(value) ? value : null
);

const normalizeTreeSourceTypeValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (normalized === 'path' || normalized === 'data_path' || normalized === 'datapath') return 'path';
    if (normalized === 'parentid' || normalized === 'parent_id' || normalized === 'adjacency') return 'adjacency';
    if (normalized === 'nested' || normalized === 'children') return 'nested';
    return 'adjacency';
};

const normalizeTreeDisplayModeValue = (value) => {
    if (typeof value !== 'string') return 'singleColumn';
    const normalized = value.trim().toLowerCase();
    if (normalized === 'multiplecolumns' || normalized === 'multiple_columns' || normalized === 'multiple' || normalized === 'tabular' || normalized === 'outline') {
        return 'multipleColumns';
    }
    return 'singleColumn';
};

const normalizeTreeGroupDefaultExpandedValue = (value) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return 0;
    const rounded = Math.floor(normalized);
    if (rounded < -1) return -1;
    return rounded;
};

const normalizeTreeDefaultOpenPathsValue = (value) => {
    if (!Array.isArray(value)) return [];
    const normalizedPaths = [];
    value.forEach((entry) => {
        if (typeof entry === 'string' && entry.trim()) {
            normalizedPaths.push(entry.trim());
            return;
        }
        if (Array.isArray(entry)) {
            const normalizedEntry = entry
                .map((part) => (part === undefined || part === null ? '' : String(part).trim()))
                .filter(Boolean)
                .join('|||');
            if (normalizedEntry) normalizedPaths.push(normalizedEntry);
        }
    });
    return Array.from(new Set(normalizedPaths));
};

const normalizeTreeLevelLabelsValue = (value) => (
    Array.isArray(value)
        ? value
            .map((label) => (typeof label === 'string' ? label.trim() : ''))
            .filter(Boolean)
        : []
);

const normalizeDetailRefreshStrategyValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_DETAIL_REFRESH_STRATEGIES.has(normalized) ? normalized : 'rows';
};

const hasTransactionEntries = (transaction) => (
    Boolean(transaction && typeof transaction === 'object')
    && ['add', 'remove', 'update', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0)
);

const normalizeTransactionRefreshModeValue = (value, fallback = 'smart') => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_REFRESH_MODES.has(normalized) ? normalized : fallback;
};

const normalizeTransactionEventActionValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_EVENT_ACTIONS.has(normalized) ? normalized : null;
};

const normalizePropagationFormulaValue = (value, fallback = 'equal') => {
    const normalized = typeof value === 'string'
        ? value.trim().toLowerCase().replace(/[\s-]+/g, '_')
        : '';
    if (['equal', 'even', 'default', 'delta', 'uniform', 'equal_delta', 'balanced_delta', 'uniform_shift', 'balanced_shift'].includes(normalized)) {
        return 'equal';
    }
    if (['proportional', 'ratio', 'scale', 'scaled'].includes(normalized)) {
        return 'proportional';
    }
    if (fallback === null || fallback === undefined) return null;
    if (fallback === value) return null;
    return normalizePropagationFormulaValue(fallback, null);
};

const isParentAggregatePropagationEdit = (update, groupingFields) => {
    if (!update || typeof update !== 'object') return false;
    const aggregation = update.aggregation && typeof update.aggregation === 'object'
        ? update.aggregation
        : null;
    const aggregationFn = aggregation && aggregation.agg
        ? String(aggregation.agg).trim().toLowerCase()
        : '';
    if (!aggregationFn || aggregation.windowFn) return false;
    const rowPath = update.rowPath || update.rowId;
    const normalizedRowPath = rowPath === null || rowPath === undefined ? '' : String(rowPath).trim();
    if (!normalizedRowPath || normalizedRowPath === GRAND_TOTAL_ROW_ID) return false;
    const pathDepth = normalizedRowPath.split('|||').filter(Boolean).length;
    const groupingDepth = Array.isArray(groupingFields) ? groupingFields.length : 0;
    return groupingDepth > 0 && pathDepth > 0 && pathDepth < groupingDepth;
};

const normalizeTransactionHistoryPayload = (transaction, overrides = {}) => {
    if (!transaction || typeof transaction !== 'object') return null;
    const normalizedEventAction = normalizeTransactionEventActionValue(
        overrides.eventAction !== undefined ? overrides.eventAction : (transaction.eventAction || transaction.event_action)
    );
    const normalized = cloneSerializable({
        add: Array.isArray(transaction.add) ? transaction.add : [],
        remove: Array.isArray(transaction.remove) ? transaction.remove : [],
        update: Array.isArray(transaction.update) ? transaction.update : [],
        upsert: Array.isArray(transaction.upsert) ? transaction.upsert : [],
        keyFields: Array.isArray(transaction.keyFields) ? transaction.keyFields : [],
        refreshMode: normalizeTransactionRefreshModeValue(
            overrides.refreshMode !== undefined ? overrides.refreshMode : transaction.refreshMode,
            'smart'
        ),
        source: overrides.source !== undefined ? overrides.source : transaction.source,
        eventAction: normalizedEventAction || undefined,
        eventId: transaction.eventId !== undefined ? transaction.eventId : transaction.event_id,
        eventIds: Array.isArray(transaction.eventIds)
            ? transaction.eventIds
            : (Array.isArray(transaction.event_ids) ? transaction.event_ids : []),
        propagationStrategy: (() => {
            const propagationValue = transaction.propagationStrategy !== undefined
                ? transaction.propagationStrategy
                : transaction.propagation_strategy;
            const normalizedPropagationValue = normalizePropagationFormulaValue(propagationValue, null);
            return normalizedPropagationValue || propagationValue;
        })(),
    }, null);
    return hasTransactionEntries(normalized) || normalizedEventAction ? normalized : null;
};

const cellValuesMatch = (expectedValue, actualValue) => {
    if (Object.is(expectedValue, actualValue)) return true;
    const expectedNumber = Number(expectedValue);
    const actualNumber = Number(actualValue);
    if (Number.isFinite(expectedNumber) && Number.isFinite(actualNumber)) {
        return expectedNumber === actualNumber;
    }
    return String(expectedValue) === String(actualValue);
};

const hasAppliedTransactionWork = (transactionResult) => (
    Boolean(transactionResult && typeof transactionResult === 'object')
    && Object.values((transactionResult.applied && typeof transactionResult.applied === 'object') ? transactionResult.applied : {})
        .some((count) => Number(count) > 0)
);

const shouldShowTransactionLoading = (transaction) => {
    if (!transaction || typeof transaction !== 'object') return true;
    const refreshMode = normalizeTransactionRefreshModeValue(transaction.refreshMode, 'smart');
    if (refreshMode === 'structural' || refreshMode === 'full' || refreshMode === 'smart_structural') {
        return true;
    }
    return ['add', 'remove', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0);
};

const describeTransactionPropagation = (entry) => {
    if (!entry || typeof entry !== 'object') return '';
    const updatedRowCount = Math.max(0, Number(entry.updatedRowCount) || 0);
    const targetColumn = entry.targetColumn ? formatDisplayLabel(entry.targetColumn) : 'value';
    const aggregationLabel = formatAggLabel(entry.aggregationFn || 'sum', entry.weightField);
    const strategy = normalizePropagationFormulaValue(entry.strategy, null);
    const strategyLabel = strategy ? ` using ${strategy} formula` : '';
    if (updatedRowCount > 0) {
        return `Propagated ${aggregationLabel} on ${targetColumn} to ${updatedRowCount} source row${updatedRowCount === 1 ? '' : 's'}${strategyLabel}.`;
    }
    return `Applied ${aggregationLabel} edit on ${targetColumn}${strategyLabel}.`;
};

const isEditableKeyboardTarget = (target) => {
    if (!target || typeof target !== 'object') return false;
    const tagName = typeof target.tagName === 'string' ? target.tagName.toUpperCase() : '';
    return Boolean(
        target.isContentEditable
        || tagName === 'INPUT'
        || tagName === 'TEXTAREA'
        || tagName === 'SELECT'
    );
};

const normalizeTreeConfigValue = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const sourceType = normalizeTreeSourceTypeValue(
        source.sourceType
        || source.source_type
        || source.mode
        || source.treeDataMode
        || source.tree_data_mode
    );
    const valueFields = Array.isArray(source.valueFields)
        ? source.valueFields.filter((field) => typeof field === 'string' && field)
        : [];
    const extraFields = Array.isArray(source.extraFields)
        ? source.extraFields.filter((field) => typeof field === 'string' && field)
        : [];
    const defaultOpenValue = Object.prototype.hasOwnProperty.call(source, 'openByDefault')
        ? source.openByDefault
        : source.open_by_default;
    const groupDefaultExpandedSource = Array.isArray(defaultOpenValue)
        ? undefined
        : (
            defaultOpenValue !== undefined
                ? defaultOpenValue
                : (
                    source.groupDefaultExpanded !== undefined
                        ? source.groupDefaultExpanded
                        : (
                            source.group_default_expanded !== undefined
                                ? source.group_default_expanded
                                : source.defaultOpenDepth
                        )
                )
        );
    const defaultOpenPaths = normalizeTreeDefaultOpenPathsValue(
        Array.isArray(defaultOpenValue)
            ? defaultOpenValue
            : (source.defaultOpenPaths || source.default_open_paths)
    );
    return {
        sourceType,
        idField: typeof (source.idField || source.id_field) === 'string' && (source.idField || source.id_field) ? (source.idField || source.id_field) : 'id',
        parentIdField: typeof (source.parentIdField || source.parent_id_field || source.treeDataParentIdField) === 'string' && (source.parentIdField || source.parent_id_field || source.treeDataParentIdField)
            ? (source.parentIdField || source.parent_id_field || source.treeDataParentIdField)
            : 'parent_id',
        pathField: typeof (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField) === 'string' && (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField)
            ? (source.pathField || source.path_field || source.treeDataPathField || source.dataPathField)
            : 'path',
        pathSeparator: typeof (source.pathSeparator || source.path_separator) === 'string' && (source.pathSeparator || source.path_separator) ? (source.pathSeparator || source.path_separator) : '|||',
        childrenField: typeof (source.childrenField || source.children_field || source.treeDataChildrenField) === 'string' && (source.childrenField || source.children_field || source.treeDataChildrenField)
            ? (source.childrenField || source.children_field || source.treeDataChildrenField)
            : 'children',
        labelField: typeof (source.labelField || source.label_field) === 'string' && (source.labelField || source.label_field) ? (source.labelField || source.label_field) : 'name',
        sortBy: typeof (source.sortBy || source.sort_by) === 'string' && (source.sortBy || source.sort_by) ? (source.sortBy || source.sort_by) : null,
        sortDir: (source.sortDir || source.sort_dir) === 'desc' ? 'desc' : 'asc',
        valueFields,
        extraFields,
        displayMode: normalizeTreeDisplayModeValue(source.displayMode || source.display_mode || source.treeDataDisplayType || source.tree_data_display_type),
        groupDefaultExpanded: normalizeTreeGroupDefaultExpandedValue(groupDefaultExpandedSource),
        defaultOpenPaths,
        suppressGroupRowsSticky: Boolean(
            source.suppressGroupRowsSticky
            || source.suppress_group_rows_sticky
            || source.disableStickyGroups
            || source.disable_sticky_groups
        ),
        levelLabels: normalizeTreeLevelLabelsValue(source.levelLabels || source.level_labels),
    };
};

const normalizeDetailConfigValue = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const defaultKind = typeof source.defaultKind === 'string' && source.defaultKind.trim()
        ? source.defaultKind.trim().toLowerCase()
        : 'records';
    return {
        enabled: source.enabled !== false,
        defaultKind,
        allowPerRowKind: (source.allowPerRowKind !== false) && (source.allow_per_row_kind !== false),
        inlineHeight: Number.isFinite(Number(source.inlineHeight || source.inline_height)) ? Math.max(220, Math.floor(Number(source.inlineHeight || source.inline_height))) : 280,
        sidepanelWidth: Number.isFinite(Number(source.sidepanelWidth || source.sidepanel_width)) ? Math.max(320, Math.floor(Number(source.sidepanelWidth || source.sidepanel_width))) : 480,
        drawerHeight: Number.isFinite(Number(source.drawerHeight || source.drawer_height)) ? Math.max(240, Math.floor(Number(source.drawerHeight || source.drawer_height))) : 320,
        keepDetailRows: Boolean(source.keepDetailRows || source.keep_detail_rows),
        keepDetailRowsCount: clampOptionalInteger(
            source.keepDetailRowsCount !== undefined
                ? source.keepDetailRowsCount
                : source.keep_detail_rows_count,
            1,
            1000
        ) || 10,
        refreshStrategy: normalizeDetailRefreshStrategyValue(source.refreshStrategy || source.refresh_strategy),
    };
};

const hasTreeDefaultExpansionConfig = (treeConfig) => {
    if (!treeConfig || typeof treeConfig !== 'object') return false;
    if (Number.isFinite(Number(treeConfig.groupDefaultExpanded)) && Number(treeConfig.groupDefaultExpanded) !== 0) {
        return true;
    }
    return Array.isArray(treeConfig.defaultOpenPaths) && treeConfig.defaultOpenPaths.length > 0;
};

const normalizeLegacyPivotModeValue = (value) => (
    value === 'report' || value === 'pivot' ? value : null
);

const normalizeReportTopN = (value) => (
    Number.isFinite(Number(value)) && Number(value) > 0
        ? Math.floor(Number(value))
        : null
);

const normalizeReportNodeValue = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const normalized = {
        ...source,
        field: typeof source.field === 'string' ? source.field : '',
        label: typeof source.label === 'string' ? source.label : '',
        topN: normalizeReportTopN(source.topN),
        sortBy: typeof source.sortBy === 'string' && source.sortBy.trim()
            ? source.sortBy.trim()
            : null,
        sortDir: source.sortDir === 'asc' ? 'asc' : 'desc',
    };

    const childrenSource = source.childrenByValue && typeof source.childrenByValue === 'object'
        ? source.childrenByValue
        : (
            source.conditionalChildren && typeof source.conditionalChildren === 'object'
                ? source.conditionalChildren
                : null
        );
    if (childrenSource) {
        const normalizedChildrenByValue = Object.entries(childrenSource).reduce((acc, [key, rule]) => {
            if (key === undefined || key === null) return acc;
            if (String(key) === '*') return acc;
            acc[String(key)] = normalizeReportNodeValue(rule);
            return acc;
        }, {});
        if (Object.keys(normalizedChildrenByValue).length > 0) {
            normalized.childrenByValue = normalizedChildrenByValue;
        } else {
            delete normalized.childrenByValue;
        }
    } else {
        delete normalized.childrenByValue;
    }

    const defaultChildSource = source.defaultChild && typeof source.defaultChild === 'object'
        ? source.defaultChild
        : (
            childrenSource && childrenSource['*'] && typeof childrenSource['*'] === 'object'
                ? childrenSource['*']
                : null
        );
    if (defaultChildSource) {
        normalized.defaultChild = normalizeReportNodeValue(defaultChildSource);
    } else {
        delete normalized.defaultChild;
    }

    return normalized;
};

const convertLegacyLevelsToReportNode = (levels, index = 0, overrideRule = null) => {
    const baseRule = overrideRule && typeof overrideRule === 'object'
        ? overrideRule
        : (Array.isArray(levels) ? levels[index] : null);
    if (!baseRule || typeof baseRule !== 'object') return null;

    const normalizedBase = normalizeReportNodeValue(baseRule);
    const nextDefaultNode = index + 1 < (Array.isArray(levels) ? levels.length : 0)
        ? convertLegacyLevelsToReportNode(levels, index + 1)
        : null;
    const legacyConditional = baseRule.conditionalChildren && typeof baseRule.conditionalChildren === 'object'
        ? baseRule.conditionalChildren
        : null;

    if (legacyConditional) {
        const childrenByValue = Object.entries(legacyConditional).reduce((acc, [key, childRule]) => {
            if (key === '*' || !childRule || typeof childRule !== 'object') return acc;
            const childNode = convertLegacyLevelsToReportNode(levels, index + 1, childRule);
            if (childNode) acc[String(key)] = childNode;
            return acc;
        }, {});
        if (Object.keys(childrenByValue).length > 0) {
            normalizedBase.childrenByValue = childrenByValue;
        }
        if (legacyConditional['*'] && typeof legacyConditional['*'] === 'object') {
            normalizedBase.defaultChild = convertLegacyLevelsToReportNode(levels, index + 1, legacyConditional['*']);
        } else if (nextDefaultNode) {
            normalizedBase.defaultChild = nextDefaultNode;
        }
    } else if (nextDefaultNode) {
        normalizedBase.defaultChild = nextDefaultNode;
    }

    return normalizedBase;
};

const normalizeReportDefValue = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const normalizedLevels = Array.isArray(source.levels)
        ? source.levels.map((level) => normalizeReportNodeValue(level))
        : [];
    const normalizedRootFromProp = source.root && typeof source.root === 'object'
        ? normalizeReportNodeValue(source.root)
        : null;
    const normalizedRoot = normalizedRootFromProp && normalizedRootFromProp.field
        ? normalizedRootFromProp
        : (normalizedLevels.length > 0 ? convertLegacyLevelsToReportNode(source.levels) : null);
    return {
        ...source,
        root: normalizedRoot,
        levels: normalizedLevels,
    };
};

const collectReportFields = (reportDef) => {
    const ordered = [];
    const seen = new Set();
    const visit = (node) => {
        if (!node || typeof node !== 'object') return;
        if (node.field && !seen.has(node.field)) {
            seen.add(node.field);
            ordered.push(node.field);
        }
        if (node.defaultChild) visit(node.defaultChild);
        if (node.childrenByValue && typeof node.childrenByValue === 'object') {
            Object.values(node.childrenByValue).forEach((childNode) => visit(childNode));
        }
    };
    if (reportDef && typeof reportDef === 'object') visit(reportDef.root);
    return ordered;
};

const countReportNodes = (reportDef) => {
    let count = 0;
    const visit = (node) => {
        if (!node || typeof node !== 'object') return;
        count += 1;
        if (node.defaultChild) visit(node.defaultChild);
        if (node.childrenByValue && typeof node.childrenByValue === 'object') {
            Object.values(node.childrenByValue).forEach((childNode) => visit(childNode));
        }
    };
    if (reportDef && typeof reportDef === 'object') visit(reportDef.root);
    return count;
};

const getReportHeaderLabel = (reportDef) => {
    const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
    if (!root || typeof root !== 'object') return 'Report';
    if (root.label && root.label.trim()) return root.label.trim();
    if (root.field && root.field.trim()) return formatDisplayLabel(root.field);
    return 'Report';
};

const normalizeSavedReportsValue = (value) => (
    Array.isArray(value)
        ? value
            .filter((report) => report && typeof report === 'object')
            .map((report, index) => ({
                ...report,
                id: typeof report.id === 'string' ? report.id : `report-${index + 1}`,
                name: typeof report.name === 'string' && report.name.trim()
                    ? report.name
                    : 'Saved Report',
                reportDef: normalizeReportDefValue(report.reportDef),
            }))
        : []
);

const normalizeActiveReportIdValue = (value) => (
    typeof value === 'string' && value.trim() ? value : null
);

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
    const columnMeta = (column.columnDef && column.columnDef.meta) || column.meta || null;
    if (columnMeta && columnMeta.isSparklineSummary) return null;
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

const coerceTransportNumber = (value, fallback = null) => {
    if (value === null || value === undefined || value === '') return fallback;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
};

const isColSchemaTransportColumn = (column) => {
    const columnId = column && typeof column === 'object' ? column.id : column;
    return String(columnId || '').trim() === '__col_schema';
};

const extractColSchemaFromTransportColumns = (columns) => {
    if (!Array.isArray(columns)) return null;
    const schemaEntry = columns.find((column) => (
        column
        && typeof column === 'object'
        && column.id === '__col_schema'
        && column.col_schema
        && typeof column.col_schema === 'object'
    ));
    return schemaEntry ? schemaEntry.col_schema : null;
};

const normalizeTransportColumns = (columns, fallbackColumns = []) => {
    const sourceColumns = Array.isArray(columns)
        ? columns
        : (Array.isArray(fallbackColumns) ? fallbackColumns : []);
    return sourceColumns.filter((column) => !isColSchemaTransportColumn(column));
};

const normalizeRuntimeDataEnvelope = (payload, fallback = {}) => {
    const source = payload && typeof payload === 'object' ? payload : {};
    const colSchema = (
        source.colSchema
        || source.col_schema
        || extractColSchemaFromTransportColumns(source.columns)
        || fallback.colSchema
        || fallback.col_schema
        || extractColSchemaFromTransportColumns(fallback.columns)
        || null
    );
    return {
        data: Array.isArray(source.data) ? source.data : (Array.isArray(fallback.data) ? fallback.data : []),
        rowCount: coerceTransportNumber(source.rowCount, coerceTransportNumber(fallback.rowCount, null)),
        columns: normalizeTransportColumns(source.columns, fallback.columns),
        colSchema,
        dataOffset: coerceTransportNumber(source.dataOffset, coerceTransportNumber(fallback.dataOffset, 0)),
        dataVersion: coerceTransportNumber(source.dataVersion, coerceTransportNumber(fallback.dataVersion, 0)),
    };
};

const resolveTransportRowId = (row) => {
    if (!row || typeof row !== 'object') return null;
    if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total' || row.__isGrandTotal__) {
        return '__grand_total__';
    }
    if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
    if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
    if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
    return null;
};

const applyRuntimePatchEnvelope = (patch, fallback = {}) => {
    const source = patch && typeof patch === 'object' ? patch : {};
    const previousState = fallback && typeof fallback === 'object' ? fallback : {};
    const previousData = Array.isArray(previousState.data) ? previousState.data : [];
    const patchRows = Array.isArray(source.rows) ? source.rows.filter((row) => row && typeof row === 'object') : [];

    if (patchRows.length === 0) {
        return normalizeRuntimeDataEnvelope({
            data: previousData,
            rowCount: source.rowCount,
            columns: source.columns,
            colSchema: source.colSchema || source.col_schema,
            dataOffset: source.dataOffset,
            dataVersion: source.dataVersion,
        }, previousState);
    }

    const patchByRowId = new Map();
    patchRows.forEach((row) => {
        const rowId = resolveTransportRowId(row);
        if (!rowId) return;
        patchByRowId.set(rowId, row);
    });

    const mergedData = previousData.map((row) => {
        const rowId = resolveTransportRowId(row);
        if (!rowId || !patchByRowId.has(rowId)) return row;
        return { ...row, ...patchByRowId.get(rowId) };
    });

    return normalizeRuntimeDataEnvelope({
        data: mergedData,
        rowCount: source.rowCount,
        columns: source.columns,
        colSchema: source.colSchema || source.col_schema,
        dataOffset: source.dataOffset,
        dataVersion: source.dataVersion,
    }, previousState);
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

const normalizeChartDockPosition = (value, fallback = 'right') => (
    VALID_CHART_DOCK_POSITIONS.has(value) ? value : fallback
);

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
        dockPosition: normalizeChartDockPosition(source.dockPosition || source.dock_position, 'right'),
        floating: Boolean(source.floating),
        floatingRect: clampFloatingChartRect(source.floatingRect, null),
        locked: Boolean(source.locked),
        lockedModel: cloneSerializable(source.lockedModel, null),
        lockedRequest: normalizeLockedChartRequest(source.lockedRequest),
        immersiveMode: Boolean(source.immersiveMode),
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

const clampSidebarWidth = (value, fallback = 288) => {
    const normalized = Number(value);
    return Number.isFinite(normalized)
        ? Math.max(200, Math.min(520, Math.floor(normalized)))
        : fallback;
};

const SUPPORTED_DEFAULT_NUMBER_FORMATS = new Set(['', 'currency', 'accounting', 'percent', 'scientific']);

const clampDecimalPlaces = (value, fallback = 0) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return fallback;
    return Math.max(0, Math.min(6, Math.floor(normalized)));
};

const normalizeDefaultValueFormat = (value) => {
    const normalized = typeof value === 'string' ? value.trim() : '';
    if (normalized.startsWith('currency:') || normalized.startsWith('accounting:')) return normalized;
    return SUPPORTED_DEFAULT_NUMBER_FORMATS.has(normalized) ? normalized : '';
};

const mergeSparseColSchema = (previousSchema, incomingSchema, fallbackSize = 140) => {
    if (!incomingSchema || typeof incomingSchema !== 'object') return previousSchema;
    const totalRaw = Number(incomingSchema.total_center_cols);
    if (!Number.isFinite(totalRaw) || totalRaw < 0) return previousSchema;

    const total = Math.max(0, Math.floor(totalRaw));
    const nextColumns = Array.from({ length: total }, (_, index) => {
        const previousColumn = previousSchema
            && previousSchema.total_center_cols === total
            && Array.isArray(previousSchema.columns)
            ? previousSchema.columns[index]
            : null;
        return previousColumn || null;
    });

    for (const rawColumn of (incomingSchema.columns || [])) {
        if (!rawColumn || typeof rawColumn !== 'object') continue;
        const indexRaw = Number(rawColumn.index);
        if (!Number.isFinite(indexRaw)) continue;
        const index = Math.floor(indexRaw);
        if (index < 0 || index >= total) continue;
        nextColumns[index] = {
            ...rawColumn,
            index,
            size: Number.isFinite(Number(rawColumn.size))
                ? Number(rawColumn.size)
                : fallbackSize,
        };
    }

    return {
        total_center_cols: total,
        columns: nextColumns,
    };
};

const isSparseSchemaRangeLoaded = (schema, start, end) => {
    if (!schema || !Array.isArray(schema.columns)) return false;
    const total = Number.isFinite(Number(schema.total_center_cols))
        ? Math.max(0, Math.floor(Number(schema.total_center_cols)))
        : schema.columns.length;
    if (total === 0) return true;
    if (start === null || start === undefined || end === null || end === undefined) return false;
    const safeStart = Math.max(0, Math.min(Math.floor(start), total - 1));
    const safeEnd = Math.max(safeStart, Math.min(Math.floor(end), total - 1));
    for (let index = safeStart; index <= safeEnd; index += 1) {
        const entry = schema.columns[index];
        if (!entry || typeof entry.id !== 'string' || !entry.id) return false;
    }
    return true;
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

const normalizePerformanceConfigValue = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    return {
        cacheBlockSize: clampOptionalInteger(source.cacheBlockSize, 16, 1024),
        maxBlocksInCache: clampOptionalInteger(source.maxBlocksInCache, 8, 5000),
        blockLoadDebounceMs: clampOptionalInteger(source.blockLoadDebounceMs, 0, 500),
        rowOverscan: clampOptionalInteger(source.rowOverscan, 0, 64),
        columnOverscan: clampOptionalInteger(source.columnOverscan, 0, 16),
        prefetchColumns: clampOptionalInteger(source.prefetchColumns, 0, 32),
    };
};

const buildLargeColumnAdvisory = ({ serverSide, totalCenterCols, colFields }) => {
    const normalizedCount = Number(totalCenterCols);
    if (!serverSide || !Number.isFinite(normalizedCount) || normalizedCount < SOFT_CENTER_COLUMN_WARNING_THRESHOLD) {
        return null;
    }

    const safeCount = Math.max(0, Math.floor(normalizedCount));
    const tone = safeCount >= HARD_CENTER_COLUMN_WARNING_THRESHOLD ? 'warning' : 'info';
    const formattedCount = new Intl.NumberFormat().format(safeCount);
    const fieldLabel = Array.isArray(colFields) && colFields.length > 0
        ? colFields.map((field) => formatDisplayLabel(field)).join(' × ')
        : 'the current column shape';

    return {
        tone,
        label: tone === 'warning' ? `Wide pivot: ${formattedCount} cols` : `${formattedCount} cols`,
        detail: `${fieldLabel} creates ${formattedCount} center columns. Bucket numeric fields, roll up dates, or move one field to Rows or Filters for smoother interaction.`,
        notification: `${fieldLabel} creates ${formattedCount} center columns. For smoother interaction, bucket numeric fields, roll up dates, or move one field to Rows or Filters.`,
    };
};

const hasActiveFilterValue = (value) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'string') return value.trim().length > 0;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'object') {
        return Object.keys(value).length > 0;
    }
    return true;
};

const countActiveFilters = (filters) => {
    if (!filters || typeof filters !== 'object') return 0;
    return Object.values(filters).reduce(
        (count, value) => count + (hasActiveFilterValue(value) ? 1 : 0),
        0
    );
};

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100, 200];
const PaginationBar = React.memo(function PaginationBar({ table, theme, pageSize, onPageSizeChange }) {
    const pageCount = table.getPageCount();
    const pageIndex = table.getState().pagination?.pageIndex ?? 0;
    const totalRows = table.getPrePaginationRowModel().rows.length;
    const rangeStart = pageIndex * pageSize + 1;
    const rangeEnd = Math.min((pageIndex + 1) * pageSize, totalRows);
    const btnStyle = (disabled) => ({
        border: `1px solid ${theme.border}`,
        borderRadius: '4px',
        background: disabled ? 'transparent' : (theme.headerSubtleBg || theme.hover),
        color: disabled ? (theme.textSec || '#999') : theme.text,
        padding: '3px 10px',
        fontSize: '11px',
        fontWeight: 600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.5 : 1,
    });
    return (
        <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '6px 12px', borderTop: `1px solid ${theme.border}`,
            background: theme.headerBg || theme.background, flexShrink: 0,
            fontSize: '11px', color: theme.text, gap: '8px',
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ color: theme.textSec }}>Rows per page</span>
                <select
                    value={pageSize}
                    onChange={(e) => onPageSizeChange(Number(e.target.value))}
                    style={{
                        border: `1px solid ${theme.border}`, borderRadius: '4px',
                        background: theme.surfaceBg || theme.background, color: theme.text,
                        padding: '2px 4px', fontSize: '11px',
                    }}
                >
                    {PAGE_SIZE_OPTIONS.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
            </div>
            <span style={{ color: theme.textSec }}>
                {totalRows > 0 ? `${rangeStart}–${rangeEnd} of ${totalRows}` : '0 rows'}
            </span>
            <div style={{ display: 'flex', gap: '4px' }}>
                <button style={btnStyle(!table.getCanPreviousPage())} onClick={() => table.setPageIndex(0)} disabled={!table.getCanPreviousPage()}>{'«'}</button>
                <button style={btnStyle(!table.getCanPreviousPage())} onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()}>{'‹'}</button>
                <span style={{ padding: '3px 8px', fontSize: '11px', fontWeight: 600 }}>
                    {pageIndex + 1} / {pageCount || 1}
                </span>
                <button style={btnStyle(!table.getCanNextPage())} onClick={() => table.nextPage()} disabled={!table.getCanNextPage()}>{'›'}</button>
                <button style={btnStyle(!table.getCanNextPage())} onClick={() => table.setPageIndex(pageCount - 1)} disabled={!table.getCanNextPage()}>{'»'}</button>
            </div>
        </div>
    );
});

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
        data: inputData = [], 
        style = {}, 
        setProps, 
        serverSide = false, 
        rowFields: initialRowFields = [],
        colFields: initialColFields = [],
        valConfigs: initialValConfigs = [],
        filters: initialFilters = {},
        sorting: initialSorting = [],
        expanded: initialExpanded = {},
        immersiveMode: initialImmersiveMode = false,
        showRowTotals: initialShowRowTotals = true,
        showColTotals: initialShowColTotals = true,
        grandTotalPosition = 'top',
        conditionalFormatting = [],
        validationRules = {},
        editingConfig: externalEditingConfig = null,
        editState: externalEditState = null,
        columnPinning: initialColumnPinning = { left: ['hierarchy'], right: [] },
        rowPinning: initialRowPinning = { top: [], bottom: [] },
        persistence,
        persistence_type = 'local',
        pinningOptions = {},
        pinningPresets = [],
        sortOptions = {},
        columnVisibility: initialColumnVisibility = {},
        decimalPlaces: externalDecimalPlaces = 0,
        fieldPanelSizes: externalFieldPanelSizes = null,
        defaultValueFormat: externalDefaultValueFormat = '',
        numberGroupSeparator: externalNumberGroupSeparator = DEFAULT_NUMBER_GROUP_SEPARATOR,
        reportDef: externalReportDef,
        savedReports: externalSavedReports,
        activeReportId: externalActiveReportId,
        reset,
        sortLock = false,
        defaultTheme = 'flash',
        availableFieldList,
        table: tableName,
        runtimeResponse = null,
        viewMode: externalViewMode,
        detailMode: externalDetailMode,
        treeConfig: externalTreeConfig,
        detailConfig: externalDetailConfig,
        chartServerWindow = null,
        chartDefinitions = null,
        chartDefaults = null,
        chartCanvasPanes: externalChartCanvasPanes = null,
        tableCanvasSize: externalTableCanvasSize = null,
        drillEndpoint = '/api/drill-through',
        performanceConfig: externalPerformanceConfig = null,
        viewState = null,
        saveViewTrigger = null,
        uiConfig: externalUiConfig = null,
        paginationConfig: externalPagination = null,
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
    const normalizedInitialFieldPanelSizes = useMemo(
        () => sanitizeFieldPanelSizes(externalFieldPanelSizes && typeof externalFieldPanelSizes === 'object'
            ? externalFieldPanelSizes
            : DEFAULT_FIELD_PANEL_SIZES),
        [externalFieldPanelSizes]
    );
    const normalizedInitialDecimalPlaces = useMemo(
        () => clampDecimalPlaces(externalDecimalPlaces, 0),
        [externalDecimalPlaces]
    );
    const normalizedInitialDefaultValueFormat = useMemo(
        () => normalizeDefaultValueFormat(externalDefaultValueFormat),
        [externalDefaultValueFormat]
    );
    const normalizedEditingConfig = useMemo(
        () => normalizeEditingConfig(externalEditingConfig),
        [externalEditingConfig]
    );
    const normalizedInitialNumberGroupSeparator = useMemo(
        () => normalizeNumberGroupSeparator(externalNumberGroupSeparator),
        [externalNumberGroupSeparator]
    );
    const uiConfig = useMemo(() => {
        const src = externalUiConfig && typeof externalUiConfig === 'object' ? externalUiConfig : {};
        return {
            showToolbar: src.showToolbar !== false,
            showSidebar: src.showSidebar !== false,
            showFilters: src.showFilters !== false,
            showCharts: src.showCharts !== false,
            showEditing: !EDITING_TEMPORARILY_DISABLED && src.showEditing !== false,
            showEditPanel: !EDITING_TEMPORARILY_DISABLED && src.showEditPanel !== false,
            lockImmersiveMode: src.lockImmersiveMode === true,
        };
    }, [externalUiConfig]);
    const editingEnabled = !EDITING_TEMPORARILY_DISABLED && uiConfig.showEditing;
    const editPanelEnabled = editingEnabled && uiConfig.showEditPanel;
    const paginationConfig = useMemo(() => {
        if (serverSide) return { enabled: false, pageSize: 50 };
        const src = externalPagination && typeof externalPagination === 'object' ? externalPagination : {};
        const enabled = src.enabled === true;
        const pageSize = Number.isFinite(Number(src.pageSize)) && Number(src.pageSize) > 0
            ? Math.floor(Number(src.pageSize)) : 50;
        return { enabled, pageSize };
    }, [externalPagination, serverSide]);
    const normalizedPerformanceConfig = useMemo(
        () => normalizePerformanceConfigValue(externalPerformanceConfig),
        [externalPerformanceConfig]
    );
    const hasExternalViewMode = normalizeViewModeValue(externalViewMode) !== null;
    const hasExternalDetailMode = normalizeDetailModeValue(externalDetailMode) !== null;
    const hasExternalTreeConfig = externalTreeConfig !== undefined;
    const hasExternalDetailConfig = externalDetailConfig !== undefined;
    const normalizedInitialViewMode = useMemo(
        () => normalizeViewModeValue(externalViewMode) || 'pivot',
        [externalViewMode]
    );
    const normalizedInitialDetailMode = useMemo(
        () => normalizeDetailModeValue(externalDetailMode) || 'none',
        [externalDetailMode]
    );
    const normalizedInitialTreeConfig = useMemo(
        () => normalizeTreeConfigValue(externalTreeConfig),
        [externalTreeConfig]
    );
    const normalizedInitialDetailConfig = useMemo(
        () => normalizeDetailConfigValue(externalDetailConfig),
        [externalDetailConfig]
    );
    const hasExternalReportDef = externalReportDef !== undefined;
    const hasExternalSavedReports = externalSavedReports !== undefined;
    const hasExternalActiveReportId = externalActiveReportId !== undefined;
    const normalizedInitialReportDef = useMemo(
        () => normalizeReportDefValue(externalReportDef),
        [externalReportDef]
    );
    const normalizedInitialSavedReports = useMemo(
        () => normalizeSavedReportsValue(externalSavedReports),
        [externalSavedReports]
    );
    const normalizedInitialActiveReportId = useMemo(
        () => normalizeActiveReportIdValue(externalActiveReportId),
        [externalActiveReportId]
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

        const [transportDataState, setTransportDataState] = useState(() => normalizeRuntimeDataEnvelope({
            data: Array.isArray(inputData) ? inputData : [],
            rowCount: serverSide ? null : (Array.isArray(inputData) ? inputData.length : 0),
            columns: [],
            dataOffset: 0,
            dataVersion: 0,
        }));
        const [transportFilterOptionsState, setTransportFilterOptionsState] = useState(() => ({}));
        const [editorOptionsLoadingState, setEditorOptionsLoadingState] = useState(() => ({}));
        const [transportChartDataState, setTransportChartDataState] = useState(() => (null));

        const data = transportDataState.data;
        const rowCount = transportDataState.rowCount;
        const responseColumns = transportDataState.columns;
        const responseColSchema = transportDataState.colSchema;
        const dataOffset = transportDataState.dataOffset;
        const dataVersion = transportDataState.dataVersion;
        const filterOptions = transportFilterOptionsState;
        const chartData = transportChartDataState;

        const [notification, setNotification] = useState(null);

        useEffect(() => {
            if (serverSide) return;
            setTransportDataState((previousState) => normalizeRuntimeDataEnvelope({
                data: Array.isArray(inputData) ? inputData : [],
                rowCount: Array.isArray(inputData) ? inputData.length : 0,
                columns: previousState.columns,
                dataOffset: 0,
                dataVersion: previousState.dataVersion,
            }, previousState));
        }, [inputData, serverSide]);

        useEffect(() => {
            if (notification) {
                const timer = setTimeout(() => setNotification(null), 3000);
                return () => clearTimeout(timer);
            }
        }, [notification]);

        const showNotification = React.useCallback((msg, type='info') => {
            setNotification({ message: msg, type });
        }, []);
        const emitEditLifecycleEvent = useCallback((event) => {
            if (typeof setProps !== 'function' || !event || typeof event !== 'object') return;
            setProps({
                editLifecycleEvent: {
                    ...event,
                    timestamp: Date.now(),
                },
            });
        }, [setProps]);

        // --- State ---

        const availableFields = useMemo(() => {
            if (availableFieldList && availableFieldList.length > 0) return availableFieldList;
            if (serverSide && responseColumns) {
                return responseColumns
                    .map(c => (c && typeof c === 'object' ? c.id : c))
                    .filter(id => id && id !== '__col_schema');
            }

            return data && data.length ? Object.keys(data[0]) : [];

        }, [data, responseColumns, serverSide, availableFieldList]);

        // Theme State
        const [themeName, setThemeName] = useState(() => (themes[defaultTheme] ? defaultTheme : 'flash'));
        const [themeOverrides, setThemeOverrides] = useState({});
        const theme = useMemo(() => {
            const nextTheme = { ...themes[themeName], ...themeOverrides };
            const structuralTheme = {
                ...nextTheme,
                ...deriveStructuralThemeTokens(nextTheme, themeOverrides),
            };
            return {
                ...structuralTheme,
                ...deriveEditedCellThemeTokens(structuralTheme, themeOverrides),
            };
        }, [themeName, themeOverrides]);
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
        const valConfigsRef = useRef(initialValConfigs);
        valConfigsRef.current = valConfigs;
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
        const {
            viewMode, setViewMode,
            reportDef, setReportDef,
            savedReports, setSavedReports,
            activeReportId, setActiveReportId,
            frozenPivotConfig, setFrozenPivotConfig,
        } = useReportMode({
            hasExternalViewMode,
            normalizedInitialViewMode,
            normalizeViewModeValue,
            normalizeLegacyPivotModeValue,
            hasExternalReportDef,
            normalizedInitialReportDef,
            normalizeReportDefValue,
            hasExternalSavedReports,
            normalizedInitialSavedReports,
            normalizeSavedReportsValue,
            hasExternalActiveReportId,
            normalizedInitialActiveReportId,
            normalizeActiveReportIdValue,
            loadPersistedState,
        });
        const [detailMode, setDetailMode] = useState(() => (
            hasExternalDetailMode
                ? normalizedInitialDetailMode
                : (normalizeDetailModeValue(loadPersistedState('detailMode', 'none')) || 'none')
        ));
        const [treeConfig, setTreeConfig] = useState(() => (
            hasExternalTreeConfig
                ? normalizedInitialTreeConfig
                : normalizeTreeConfigValue(loadPersistedState('treeConfig', {}))
        ));
        const [detailConfig, setDetailConfig] = useState(() => (
            hasExternalDetailConfig
                ? normalizedInitialDetailConfig
                : normalizeDetailConfigValue(loadPersistedState('detailConfig', {}))
        ));

        // reportDef, savedReports, activeReportId, frozenPivotConfig provided by useReportMode above
        const [columnVisibility, setColumnVisibility] = useState(() => loadPersistedState('columnVisibility', initialColumnVisibility));
        const [columnSizing, setColumnSizing] = useState(() => loadPersistedState('columnSizing', {}));
        const [autoSizeIncludesHeaderNext, setAutoSizeIncludesHeaderNext] = useState(false);
        const [pivotColumnSorting, setPivotColumnSorting] = useState({});
        const [announcement, setAnnouncement] = useState("");
        // detailSurface, detailSurfaceCacheRef, pendingDetailRequestRef, toggleDetailForRowRef
        // provided by useDetailDrillThrough hook
        // --- Chart state extracted to useChartState hook ---
        const {
            chartPanelOpen, setChartPanelOpen,
            chartPanelSource, setChartPanelSource,
            chartPanelType, setChartPanelType,
            chartPanelBarLayout, setChartPanelBarLayout,
            chartPanelAxisMode, setChartPanelAxisMode,
            chartPanelOrientation, setChartPanelOrientation,
            chartPanelHierarchyLevel, setChartPanelHierarchyLevel,
            chartPanelTitle, setChartPanelTitle,
            chartPanelLayers, setChartPanelLayers,
            chartPanelRowLimit, setChartPanelRowLimit,
            chartPanelColumnLimit, setChartPanelColumnLimit,
            chartPanelWidth, setChartPanelWidth,
            chartPanelGraphHeight, setChartPanelGraphHeight,
            chartPanelFloating, setChartPanelFloating,
            chartPanelFloatingRect, setChartPanelFloatingRect,
            chartPanelSortMode, setChartPanelSortMode,
            chartPanelInteractionMode, setChartPanelInteractionMode,
            chartPanelServerScope, setChartPanelServerScope,
            chartPanelLocked, setChartPanelLocked,
            chartPanelLockedModel, setChartPanelLockedModel,
            chartPanelLockedRequest, setChartPanelLockedRequest,
            isChartPanelResizing, setIsChartPanelResizing,
            chartModal, setChartModal,
            chartModalPosition, setChartModalPosition,
            managedChartDefinitions, setManagedChartDefinitions,
            activeChartDefinitionId, setActiveChartDefinitionId,
            chartCanvasPanes, setChartCanvasPanes,
            tableCanvasSize, setTableCanvasSize,
            chartPaneDataById, setChartPaneDataById,
            chartPanelOrientationAutoRef,
            chartLayoutRef,
            chartCanvasLayoutRef,
            chartCanvasResizeRef,
            chartCanvasVerticalResizeRef,
            chartPanelFloatingDragRef,
            chartPanelFloatingResizeRef,
            chartCanvasFloatingDragRef,
            chartCanvasFloatingResizeRef,
            chartRequestSeqRef,
            activeChartRequestRef,
            completedChartRequestSignaturesRef,
            applyingChartDefinitionRef,
            lastChartDefinitionsPropRef,
            lastChartCanvasPanesPropRef,
        } = useChartState({
            initialChartDefinition,
            initialChartDefinitions,
            externalChartCanvasPanes,
            externalTableCanvasSize,
            loadPersistedState,
            clampFloatingChartRect,
            normalizeLockedChartRequest,
            sanitizeChartCanvasPanes,
            DEFAULT_TABLE_CANVAS_SIZE,
        });
        const [chartCanvasPaneWidthHints, setChartCanvasPaneWidthHints] = useState({});
        const [sparklineDataModal, setSparklineDataModal] = useState(null);
        const openSparklineDataModalRef = useRef(null);
        openSparklineDataModalRef.current = setSparklineDataModal;
        const lastTableCanvasSizePropRef = useRef(null);
        const lastValConfigsPropRef = useRef(null);
        const lastDecimalPlacesPropRef = useRef(null);
        const lastFieldPanelSizesPropRef = useRef(null);
        const lastDefaultValueFormatPropRef = useRef(null);
        const lastNumberGroupSeparatorPropRef = useRef(null);
        const lastReportDefPropRef = useRef(null);
        const lastSavedReportsPropRef = useRef(null);
        const lastActiveReportIdPropRef = useRef(null);
        const lastViewModePropRef = useRef(null);
        const lastDetailModePropRef = useRef(null);
        const lastTreeConfigPropRef = useRef(null);
        const lastDetailConfigPropRef = useRef(null);
        const didPublishDecimalPlacesRef = useRef(false);
        const didPublishFieldPanelSizesRef = useRef(false);
        const didPublishDefaultValueFormatRef = useRef(false);
        const didPublishNumberGroupSeparatorRef = useRef(false);
        const didPublishReportStateRef = useRef(false);
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

    const pivotMode = viewMode === 'report' ? 'report' : 'pivot';

    // Reset Effect
    useEffect(() => {
        if (reset) {
            const resetReportDef = hasExternalReportDef ? normalizedInitialReportDef : { levels: [] };
            const resetSavedReports = hasExternalSavedReports ? normalizedInitialSavedReports : [];
            const resetActiveReportId = hasExternalActiveReportId ? normalizedInitialActiveReportId : null;
            setRowFields(initialRowFields);
            setColFields(initialColFields);
            // Preserve user-added formula columns through resets (they survive dataframe switches)
            const formulaColsToPreserve = valConfigsRef.current.filter(c => c && c.agg === 'formula');
            const resetValConfigs = formulaColsToPreserve.length > 0
                ? [...initialValConfigs.filter(c => c && c.agg !== 'formula'), ...formulaColsToPreserve]
                : initialValConfigs;
            setValConfigs(resetValConfigs);
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
            setSidebarWidth(288);
            setFieldPanelSizes(normalizedInitialFieldPanelSizes);
            setDecimalPlaces(normalizedInitialDecimalPlaces);
            setDefaultValueFormat(normalizedInitialDefaultValueFormat);
            setNumberGroupSeparator(normalizedInitialNumberGroupSeparator);
            setViewMode(normalizedInitialViewMode);
            setDetailMode(normalizedInitialDetailMode);
            setTreeConfig(normalizedInitialTreeConfig);
            setDetailConfig(normalizedInitialDetailConfig);
            setReportDef(resetReportDef);
            setSavedReports(resetSavedReports);
            setActiveReportId(resetActiveReportId);
            setFrozenPivotConfig(null);
            setColumnFormatOverrides({});
            setColumnGroupSeparatorOverrides({});

            if (setPropsRef.current) {
                setPropsRef.current({
                    rowFields: initialRowFields,
                    colFields: initialColFields,
                    valConfigs: resetValConfigs,
                    decimalPlaces: normalizedInitialDecimalPlaces,
                    fieldPanelSizes: normalizedInitialFieldPanelSizes,
                    defaultValueFormat: normalizedInitialDefaultValueFormat,
                    numberGroupSeparator: normalizedInitialNumberGroupSeparator,
                    viewMode: normalizedInitialViewMode,
                    detailMode: normalizedInitialDetailMode,
                    treeConfig: normalizedInitialTreeConfig,
                    detailConfig: normalizedInitialDetailConfig,
                    reportDef: resetReportDef,
                    savedReports: resetSavedReports,
                    activeReportId: resetActiveReportId,
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
    }, [reset, initialRowFields, initialColFields, initialValConfigs, initialColumnPinning, initialRowPinning, initialChartDefinition, initialChartDefinitions, normalizedInitialFieldPanelSizes, normalizedInitialDecimalPlaces, normalizedInitialDefaultValueFormat, normalizedInitialNumberGroupSeparator, normalizedInitialViewMode, normalizedInitialDetailMode, normalizedInitialTreeConfig, normalizedInitialDetailConfig, hasExternalReportDef, normalizedInitialReportDef, hasExternalSavedReports, normalizedInitialSavedReports, hasExternalActiveReportId, normalizedInitialActiveReportId]);

        // Save Persistence
        useEffect(() => {
            if (!persistence) return;
            savePersistedState('columnPinning', columnPinning);
            savePersistedState('rowPinning', rowPinning);
            savePersistedState('grandTotalPinOverride', grandTotalPinOverride);
            savePersistedState('columnVisibility', columnVisibility);
            savePersistedState('columnSizing', columnSizing);
            savePersistedState('sidebarWidth', sidebarWidth);
            savePersistedState('fieldPanelSizes', fieldPanelSizes);
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
            savePersistedState('viewMode', viewMode);
            savePersistedState('detailMode', detailMode);
            savePersistedState('treeConfig', treeConfig);
            savePersistedState('detailConfig', detailConfig);
            savePersistedState('reportDef', reportDef);
            savePersistedState('savedReports', savedReports);
            savePersistedState('activeReportId', activeReportId);
            savePersistedState('frozenPivotConfig', frozenPivotConfig);
        }, [
            id,
            columnPinning,
            rowPinning,
            grandTotalPinOverride,
            columnVisibility,
            columnSizing,
            sidebarWidth,
            fieldPanelSizes,
            chartCanvasPanes,
            tableCanvasSize,
            chartPanelFloating,
            chartPanelFloatingRect,
            chartPanelLocked,
            chartPanelLockedModel,
            chartPanelLockedRequest,
            viewMode,
            detailMode,
            treeConfig,
            detailConfig,
            persistence,
            persistence_type,
            reportDef,
            savedReports,
            activeReportId,
            frozenPivotConfig,
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

    const [immersiveModeState, setImmersiveModeState] = useState(initialImmersiveMode || uiConfig.lockImmersiveMode);
    const immersiveMode = uiConfig.lockImmersiveMode ? true : immersiveModeState;
    const setImmersiveMode = uiConfig.lockImmersiveMode ? () => {} : setImmersiveModeState;
    const [immersiveModeHovering, setImmersiveModeHovering] = useState(false);
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
    const {
        sidebarOpen, setSidebarOpen,
        sidebarWidth, setSidebarWidth,
        fieldPanelSizes, setFieldPanelSizes,
        activeFilterCol, setActiveFilterCol,
        filterAnchorEl, setFilterAnchorEl,
        sidebarTab, setSidebarTab,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        colSearch, setColSearch,
        colTypeFilter, setColTypeFilter,
        selectedCols, setSelectedCols,
    } = useSidebarUI({
        loadPersistedState,
        clampSidebarWidth,
        externalFieldPanelSizes,
        normalizedInitialFieldPanelSizes,
        sanitizeFieldPanelSizes,
        DEFAULT_FIELD_PANEL_SIZES,
    });
    const [hoveredHeaderId, setHoveredHeaderId] = useState(null);
    const [focusedHeaderId, setFocusedHeaderId] = useState(null);
    // Refs mirroring hover/focus state for renderHeaderCell's useCallback
    // (reading from refs avoids putting fast-changing state in the dep array).
    const hoveredHeaderIdRef = useRef(null);
    hoveredHeaderIdRef.current = hoveredHeaderId;
    const focusedHeaderIdRef = useRef(null);
    focusedHeaderIdRef.current = focusedHeaderId;
    
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
    }, [normalizeTrackedEventIds]);

    // --- Formatting/display state extracted to useFormatState hook ---
    const {
        colorScaleMode, setColorScaleMode,
        colorPalette, setColorPalette,
        dataBarsColumns, setDataBarsColumns,
        fontFamily, setFontFamily,
        fontSize, setFontSize,
        decimalPlaces, setDecimalPlaces,
        defaultValueFormat, setDefaultValueFormat,
        numberGroupSeparator, setNumberGroupSeparator,
        columnDecimalOverrides, setColumnDecimalOverrides,
        columnFormatOverrides, setColumnFormatOverrides,
        columnGroupSeparatorOverrides, setColumnGroupSeparatorOverrides,
        cellFormatRules, setCellFormatRules,
        hoveredRowPath, setHoveredRowPath,
        zoomLevel, setZoomLevel,
    } = useFormatState({
        normalizedInitialDecimalPlaces,
        normalizedInitialDefaultValueFormat,
        normalizedInitialNumberGroupSeparator,
    });

    // --- Report / View Mode switching ---
    const applyViewMode = useCallback((nextMode) => {
        const normalizedNextMode = normalizeViewModeValue(nextMode) || 'pivot';
        if (normalizedNextMode === viewMode) return;

        const enteringReport = normalizedNextMode === 'report' && viewMode !== 'report';
        const leavingReport = viewMode === 'report' && normalizedNextMode !== 'report';

        if (enteringReport) {
            setFrozenPivotConfig({ rowFields, colFields, valConfigs: valConfigs.map(v => ({ ...v })) });
            const levelFields = collectReportFields(reportDef);
            if (levelFields.length > 0) {
                setRowFields(levelFields);
                setColFields([]);
            }
            setExpanded({});
        } else if (leavingReport) {
            if (normalizedNextMode === 'pivot' && frozenPivotConfig) {
                setRowFields(frozenPivotConfig.rowFields || []);
                setColFields(frozenPivotConfig.colFields || []);
                setValConfigs(frozenPivotConfig.valConfigs || []);
                setFrozenPivotConfig(null);
            }
            setExpanded({});
        }

        setViewMode(normalizedNextMode);
    }, [viewMode, rowFields, colFields, valConfigs, reportDef, frozenPivotConfig]);

    const handleSetPivotMode = useCallback((nextMode) => {
        applyViewMode(nextMode === 'report' ? 'report' : 'pivot');
    }, [applyViewMode]);

    useEffect(() => {
        const normalizedExternalMode = normalizeViewModeValue(externalViewMode);
        if (!normalizedExternalMode) return;
        if (normalizedExternalMode === lastViewModePropRef.current) return;
        lastViewModePropRef.current = normalizedExternalMode;
        applyViewMode(normalizedExternalMode);
    }, [applyViewMode, externalViewMode]);

    useEffect(() => {
        const normalizedExternalMode = normalizeDetailModeValue(externalDetailMode);
        if (!normalizedExternalMode) return;
        if (normalizedExternalMode === lastDetailModePropRef.current) return;
        lastDetailModePropRef.current = normalizedExternalMode;
        setDetailMode((previousMode) => (previousMode === normalizedExternalMode ? previousMode : normalizedExternalMode));
    }, [externalDetailMode]);

    useEffect(() => {
        if (externalTreeConfig === undefined) return;
        const serializedExternal = JSON.stringify(normalizedInitialTreeConfig);
        if (serializedExternal === lastTreeConfigPropRef.current) return;
        lastTreeConfigPropRef.current = serializedExternal;
        setTreeConfig((previousConfig) => (
            JSON.stringify(previousConfig || {}) === serializedExternal
                ? previousConfig
                : normalizedInitialTreeConfig
        ));
    }, [externalTreeConfig, normalizedInitialTreeConfig]);

    useEffect(() => {
        if (externalDetailConfig === undefined) return;
        const serializedExternal = JSON.stringify(normalizedInitialDetailConfig);
        if (serializedExternal === lastDetailConfigPropRef.current) return;
        lastDetailConfigPropRef.current = serializedExternal;
        setDetailConfig((previousConfig) => (
            JSON.stringify(previousConfig || {}) === serializedExternal
                ? previousConfig
                : normalizedInitialDetailConfig
        ));
    }, [externalDetailConfig, normalizedInitialDetailConfig]);

    useEffect(() => {
        if (externalReportDef === undefined) return;
        const serializedExternal = JSON.stringify(normalizedInitialReportDef);
        if (serializedExternal === lastReportDefPropRef.current) return;
        lastReportDefPropRef.current = serializedExternal;
        setReportDef((previousReportDef) => (
            JSON.stringify(previousReportDef || { levels: [] }) === serializedExternal
                ? previousReportDef
                : normalizedInitialReportDef
        ));
    }, [externalReportDef, normalizedInitialReportDef]);

    useEffect(() => {
        if (externalSavedReports === undefined) return;
        const serializedExternal = JSON.stringify(normalizedInitialSavedReports);
        if (serializedExternal === lastSavedReportsPropRef.current) return;
        lastSavedReportsPropRef.current = serializedExternal;
        setSavedReports((previousSavedReports) => (
            JSON.stringify(previousSavedReports || []) === serializedExternal
                ? previousSavedReports
                : normalizedInitialSavedReports
        ));
    }, [externalSavedReports, normalizedInitialSavedReports]);

    useEffect(() => {
        if (externalActiveReportId === undefined) return;
        if (normalizedInitialActiveReportId === lastActiveReportIdPropRef.current) return;
        lastActiveReportIdPropRef.current = normalizedInitialActiveReportId;
        setActiveReportId((previousActiveReportId) => (
            previousActiveReportId === normalizedInitialActiveReportId
                ? previousActiveReportId
                : normalizedInitialActiveReportId
        ));
    }, [externalActiveReportId, normalizedInitialActiveReportId]);

    // When reportDef levels change in report mode, update rowFields to match
    useEffect(() => {
        if (viewMode !== 'report') return;
        const levelFields = collectReportFields(reportDef);
        if (levelFields.length > 0 && JSON.stringify(levelFields) !== JSON.stringify(rowFields)) {
            setRowFields(levelFields);
            setColFields([]);
            setExpanded({});
        }
    }, [reportDef, rowFields, viewMode]); // eslint-disable-line react-hooks/exhaustive-deps

    // Push report-mode state to Dash props so server and persistence flows stay in sync
    useEffect(() => {
        if (!setPropsRef.current) return;
        if (!didPublishReportStateRef.current) {
            didPublishReportStateRef.current = true;
            return;
        }
        setPropsRef.current({ viewMode, reportDef, savedReports, activeReportId });
    }, [viewMode, reportDef, savedReports, activeReportId]);

    // selectedCellKeys and selectedCellColIds are now derived inside PivotAppBar
    // to avoid stale prop issues between parent and child renders.

    // Decimal formatting logic moved to PivotAppBar to use fresh selectedCells

    const [spacingMode, setSpacingMode] = useState(1);
    const spacingLabels = gridDimensionTokens.density.spacingLabels;
    const rowHeights = gridDimensionTokens.density.rowHeights;
    const defaultColumnWidths = gridDimensionTokens.columnWidths;
    const autoSizeBounds = gridDimensionTokens.autoSize;
    
    const [colExpanded, setColExpanded] = useState({});
    const {
        contextMenu, setContextMenu,
        selectedCells, setSelectedCells,
        lastSelected, setLastSelected,
        isDragging, setIsDragging,
        dragStart, setDragStart,
        isFilling, setIsFilling,
        fillRange, setFillRange,
        isRowSelecting, setIsRowSelecting,
        rowDragStart, setRowDragStart,
    } = useCellInteraction();

    const transactionUndoExecutorRef = useRef(() => {});
    const transactionRedoExecutorRef = useRef(() => {});

    const handleUndo = useCallback(() => {
        transactionUndoExecutorRef.current();
    }, []);

    const handleRedo = useCallback(() => {
        transactionRedoExecutorRef.current();
    }, []);

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

        const sanitizedFilters = (restored.filters && typeof restored.filters === 'object')
            ? { ...restored.filters }
            : null;

        if (restored.viewMode === 'pivot' || restored.viewMode === 'report' || restored.viewMode === 'tree' || restored.viewMode === 'table') {
            setViewMode(restored.viewMode);
        } else if (restored.pivotMode === 'pivot' || restored.pivotMode === 'report') {
            setViewMode(restored.pivotMode);
        }
        if (restored.detailMode === 'none' || restored.detailMode === 'inline' || restored.detailMode === 'sidepanel' || restored.detailMode === 'drawer') setDetailMode(restored.detailMode);
        if (restored.treeConfig && typeof restored.treeConfig === 'object') setTreeConfig(normalizeTreeConfigValue(restored.treeConfig));
        if (restored.detailConfig && typeof restored.detailConfig === 'object') setDetailConfig(normalizeDetailConfigValue(restored.detailConfig));
        if (Object.prototype.hasOwnProperty.call(restored, 'reportDef')) {
            setReportDef(normalizeReportDefValue(restored.reportDef));
        }
        if (Object.prototype.hasOwnProperty.call(restored, 'savedReports')) {
            setSavedReports(normalizeSavedReportsValue(restored.savedReports));
        }
        if (Object.prototype.hasOwnProperty.call(restored, 'activeReportId')) {
            setActiveReportId(normalizeActiveReportIdValue(restored.activeReportId));
        }
        if (Object.prototype.hasOwnProperty.call(restored, 'frozenPivotConfig')) {
            setFrozenPivotConfig(restored.frozenPivotConfig && typeof restored.frozenPivotConfig === 'object'
                ? restored.frozenPivotConfig
                : null);
        }
        if (Array.isArray(restored.rowFields)) setRowFields(restored.rowFields);
        if (Array.isArray(restored.colFields)) setColFields(restored.colFields);
        if (Array.isArray(restored.valConfigs)) setValConfigs(restored.valConfigs);
        if (sanitizedFilters) setFilters(sanitizedFilters);
        if (Array.isArray(restored.sorting)) setSorting(restored.sorting);
        if (restored.expanded && typeof restored.expanded === 'object') setExpanded(restored.expanded);
        if (typeof restored.immersiveMode === 'boolean') setImmersiveMode(restored.immersiveMode);
        if (typeof restored.showRowTotals === 'boolean') setShowRowTotals(restored.showRowTotals);
        if (typeof restored.showColTotals === 'boolean') setShowColTotals(restored.showColTotals);
        if (typeof restored.showRowNumbers === 'boolean') setShowRowNumbers(restored.showRowNumbers);
        if (typeof restored.sidebarOpen === 'boolean') setSidebarOpen(restored.sidebarOpen);
        if (typeof restored.sidebarWidth === 'number') setSidebarWidth(clampSidebarWidth(restored.sidebarWidth, 288));
        if (typeof restored.sidebarTab === 'string') setSidebarTab(restored.sidebarTab);
        if (restored.fieldPanelSizes && typeof restored.fieldPanelSizes === 'object') {
            setFieldPanelSizes(sanitizeFieldPanelSizes(restored.fieldPanelSizes));
        }
        if (typeof restored.showFloatingFilters === 'boolean') setShowFloatingFilters(restored.showFloatingFilters);
        if (typeof restored.stickyHeaders === 'boolean') setStickyHeaders(restored.stickyHeaders);
        if (typeof restored.colSearch === 'string') setColSearch(restored.colSearch);
        if (typeof restored.colTypeFilter === 'string') setColTypeFilter(restored.colTypeFilter);
        if (typeof restored.themeName === 'string' && themes[restored.themeName]) setThemeName(restored.themeName);
        if (restored.themeOverrides && typeof restored.themeOverrides === 'object') setThemeOverrides(restored.themeOverrides);
        if (restored.editValueDisplayMode === 'original' || restored.editValueDisplayMode === 'edited') {
            setEditValueDisplayMode(restored.editValueDisplayMode);
        }
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
        if (typeof restored.defaultValueFormat === 'string') {
            setDefaultValueFormat(normalizeDefaultValueFormat(restored.defaultValueFormat));
        }
        if (typeof restored.numberGroupSeparator === 'string') {
            setNumberGroupSeparator(normalizeNumberGroupSeparator(restored.numberGroupSeparator));
        }
        if (typeof restored.zoomLevel === 'number') setZoomLevel(Math.max(60, Math.min(160, restored.zoomLevel)));
        setColumnDecimalOverrides(restored.columnDecimalOverrides && typeof restored.columnDecimalOverrides === 'object' ? restored.columnDecimalOverrides : {});
        setColumnFormatOverrides(restored.columnFormatOverrides && typeof restored.columnFormatOverrides === 'object' ? restored.columnFormatOverrides : {});
        setColumnGroupSeparatorOverrides(
            restored.columnGroupSeparatorOverrides && typeof restored.columnGroupSeparatorOverrides === 'object'
                ? restored.columnGroupSeparatorOverrides
                : {}
        );
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

    useEffect(() => {
        const serializedExternal = JSON.stringify(initialValConfigs || []);
        if (serializedExternal === lastValConfigsPropRef.current) return;
        lastValConfigsPropRef.current = serializedExternal;
        if (!Array.isArray(initialValConfigs)) return;
        setValConfigs(initialValConfigs);
    }, [initialValConfigs]);

    useEffect(() => {
        const normalizedExternal = clampDecimalPlaces(externalDecimalPlaces, 0);
        if (normalizedExternal === lastDecimalPlacesPropRef.current) return;
        lastDecimalPlacesPropRef.current = normalizedExternal;
        setDecimalPlaces(normalizedExternal);
    }, [externalDecimalPlaces]);

    useEffect(() => {
        const serializedExternal = externalFieldPanelSizes && typeof externalFieldPanelSizes === 'object'
            ? JSON.stringify(normalizedInitialFieldPanelSizes)
            : null;
        if (serializedExternal === lastFieldPanelSizesPropRef.current) return;
        lastFieldPanelSizesPropRef.current = serializedExternal;
        if (!externalFieldPanelSizes || typeof externalFieldPanelSizes !== 'object') return;
        setFieldPanelSizes(normalizedInitialFieldPanelSizes);
    }, [externalFieldPanelSizes, normalizedInitialFieldPanelSizes]);

    useEffect(() => {
        if (!setPropsRef.current) return;
        if (!didPublishFieldPanelSizesRef.current) {
            didPublishFieldPanelSizesRef.current = true;
            return;
        }
        setPropsRef.current({ fieldPanelSizes });
    }, [fieldPanelSizes]);

    useEffect(() => {
        if (!setPropsRef.current) return;
        if (!didPublishDecimalPlacesRef.current) {
            didPublishDecimalPlacesRef.current = true;
            return;
        }
        setPropsRef.current({ decimalPlaces });
    }, [decimalPlaces]);

    useEffect(() => {
        const normalizedExternal = normalizeDefaultValueFormat(externalDefaultValueFormat);
        if (normalizedExternal === lastDefaultValueFormatPropRef.current) return;
        lastDefaultValueFormatPropRef.current = normalizedExternal;
        setDefaultValueFormat(normalizedExternal);
    }, [externalDefaultValueFormat]);

    useEffect(() => {
        if (!setPropsRef.current) return;
        if (!didPublishDefaultValueFormatRef.current) {
            didPublishDefaultValueFormatRef.current = true;
            return;
        }
        setPropsRef.current({ defaultValueFormat });
    }, [defaultValueFormat]);

    useEffect(() => {
        const normalizedExternal = normalizeNumberGroupSeparator(externalNumberGroupSeparator);
        if (normalizedExternal === lastNumberGroupSeparatorPropRef.current) return;
        lastNumberGroupSeparatorPropRef.current = normalizedExternal;
        if (typeof externalNumberGroupSeparator !== 'string') return;
        setNumberGroupSeparator(normalizedExternal);
    }, [externalNumberGroupSeparator]);

    useEffect(() => {
        if (!setPropsRef.current) return;
        if (!didPublishNumberGroupSeparatorRef.current) {
            didPublishNumberGroupSeparatorRef.current = true;
            return;
        }
        setPropsRef.current({ numberGroupSeparator });
    }, [numberGroupSeparator]);

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
            const pxToUnits = totalUnits / Math.max(1, resizeState.containerWidth);
            const leftMinPx = resizeState.leftKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
            const rightMinPx = resizeState.rightKey === 'table' ? MIN_TABLE_PANEL_WIDTH : MIN_CHART_CANVAS_PANE_WIDTH;
            const leftMinUnits = leftMinPx * pxToUnits;
            const rightMinUnits = rightMinPx * pxToUnits;
            const deltaUnits = ((event.clientX - resizeState.startX) / Math.max(1, resizeState.containerWidth)) * totalUnits;
            const nextLeftSize = Math.max(leftMinUnits, resizeState.leftSize + deltaUnits);
            const nextRightSize = Math.max(rightMinUnits, resizeState.rightSize - deltaUnits);
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

    const handleStartChartCanvasVerticalResize = useCallback((paneId, position, event) => {
        const containerRect = chartCanvasLayoutRef.current
            ? chartCanvasLayoutRef.current.getBoundingClientRect()
            : null;
        if (!containerRect || containerRect.height <= 0) return;
        const pane = chartCanvasPanes.find((p) => p.id === paneId);
        if (!pane) return;
        const paneEl = chartCanvasLayoutRef.current.querySelector(`[data-docked-chart-pane="${paneId}"]`);
        const startHeight = paneEl ? paneEl.getBoundingClientRect().height : DEFAULT_DOCKED_CHART_PANE_HEIGHT;
        chartCanvasVerticalResizeRef.current = {
            startY: event.clientY,
            paneId,
            position,
            startHeight,
            containerHeight: containerRect.height,
        };
    }, [chartCanvasPanes]);

    useEffect(() => {
        const handleVerticalMove = (event) => {
            const resizeState = chartCanvasVerticalResizeRef.current;
            if (!resizeState) return;
            const deltaY = event.clientY - resizeState.startY;
            const sign = resizeState.position === 'top' ? 1 : -1;
            const nextHeight = Math.max(
                MIN_DOCKED_CHART_PANE_HEIGHT,
                Math.min(
                    resizeState.containerHeight - MIN_TABLE_PANEL_HEIGHT,
                    resizeState.startHeight + (deltaY * sign)
                )
            );
            const nextChartHeight = Math.max(120, Math.floor(nextHeight - 188));
            setChartCanvasPanes((prev) => prev.map((p) =>
                p.id === resizeState.paneId ? { ...p, chartHeight: nextChartHeight } : p
            ));
        };
        const stopVerticalResize = () => {
            chartCanvasVerticalResizeRef.current = null;
        };
        window.addEventListener('mousemove', handleVerticalMove);
        window.addEventListener('mouseup', stopVerticalResize);
        window.addEventListener('mouseleave', stopVerticalResize);
        window.addEventListener('blur', stopVerticalResize);
        return () => {
            window.removeEventListener('mousemove', handleVerticalMove);
            window.removeEventListener('mouseup', stopVerticalResize);
            window.removeEventListener('mouseleave', stopVerticalResize);
            window.removeEventListener('blur', stopVerticalResize);
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
        const containerRect = chartCanvasLayoutRef.current
            ? chartCanvasLayoutRef.current.getBoundingClientRect()
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
    }, [chartCanvasLayoutRef, updateChartCanvasPane]);

    const handleStartChartCanvasPaneFloatingDrag = useCallback((paneId, event) => {
        if (!chartCanvasLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        const targetPane = chartCanvasPanes.find((pane) => pane.id === paneId);
        if (!targetPane || !targetPane.floating || targetPane.locked) return;
        chartCanvasFloatingDragRef.current = {
            paneId,
            startX: event.clientX,
            startY: event.clientY,
            rect: targetPane.floatingRect || clampFloatingChartRect({}, null),
        };
    }, [chartCanvasLayoutRef, chartCanvasPanes]);

    const handleStartChartCanvasPaneFloatingResize = useCallback((paneId, direction, event) => {
        if (!chartCanvasLayoutRef.current) return;
        event.preventDefault();
        event.stopPropagation();
        const targetPane = chartCanvasPanes.find((pane) => pane.id === paneId);
        if (!targetPane || !targetPane.floating || targetPane.locked) return;
        chartCanvasFloatingResizeRef.current = {
            paneId,
            direction,
            startX: event.clientX,
            startY: event.clientY,
            rect: targetPane.floatingRect || clampFloatingChartRect({}, null),
        };
    }, [chartCanvasLayoutRef, chartCanvasPanes]);

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
                const containerRect = chartCanvasLayoutRef.current
                    ? chartCanvasLayoutRef.current.getBoundingClientRect()
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
                const containerRect = chartCanvasLayoutRef.current
                    ? chartCanvasLayoutRef.current.getBoundingClientRect()
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
            const containerRect = chartCanvasLayoutRef.current
                ? chartCanvasLayoutRef.current.getBoundingClientRect()
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

        if (typeof ResizeObserver === 'undefined' || !chartCanvasLayoutRef.current) {
            window.addEventListener('resize', syncFloatingPanes);
            return () => window.removeEventListener('resize', syncFloatingPanes);
        }

        const observer = new ResizeObserver(syncFloatingPanes);
        observer.observe(chartCanvasLayoutRef.current);
        return () => observer.disconnect();
    }, [chartCanvasLayoutRef, chartCanvasPanes]);

    // Clipboard Paste
    useEffect(() => {
        const handlePaste = (e) => {
            if (!lastSelected) return;
            e.preventDefault();
            const clipboardData = e.clipboardData.getData('text');
            const rows = clipboardData
                .split(/\r\n|\n/)
                .filter((row, index, sourceRows) => row.length > 0 || index < sourceRows.length - 1)
                .map((row) => row.split('\t'));

            if (lastSelected.rowIndex === undefined || lastSelected.colIndex === undefined) return;
            const visibleLeafColumns = (tableRef.current && tableRef.current.getVisibleLeafColumns) ? tableRef.current.getVisibleLeafColumns() : [];
            const visibleRows = getDisplayRows();
            const updates = [];

            rows.forEach((rowValues, rowOffset) => {
                rowValues.forEach((rawValue, colOffset) => {
                    const targetRow = visibleRows[lastSelected.rowIndex + rowOffset];
                    const targetCol = visibleLeafColumns[lastSelected.colIndex + colOffset];
                    if (!targetRow || !targetCol) return;
                    const currentValue = targetRow.getValue(targetCol.id);
                    let nextValue = rawValue;
                    if (typeof currentValue === 'number' && rawValue !== '') {
                        const numericValue = Number(rawValue);
                        if (!Number.isNaN(numericValue)) {
                            nextValue = numericValue;
                        }
                    }
                    updates.push({
                        rowId: targetRow.id,
                        colId: targetCol.id,
                        value: nextValue,
                    });
                });
            });

            dispatchBatchUpdateRequest(updates, 'paste');
        };

        window.addEventListener('paste', handlePaste);
        return () => window.removeEventListener('paste', handlePaste);
    }, [dispatchBatchUpdateRequest, getDisplayRows, lastSelected]);

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
        if (
            !isEditableKeyboardTarget(e.target)
            && (e.ctrlKey || e.metaKey)
            && !e.altKey
            && e.key
        ) {
            const shortcutKey = e.key.toLowerCase();
            if (shortcutKey === 'z') {
                e.preventDefault();
                if (e.shiftKey) handleRedo();
                else handleUndo();
                return;
            }
            if (shortcutKey === 'y') {
                e.preventDefault();
                handleRedo();
                return;
            }
        }

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

    // isRowSelecting and rowDragStart are provided by useCellInteraction above

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
                dispatchBatchUpdateRequest(updates, 'fill');
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

    const isColExpanded = (key) => colExpanded[key] !== false;

    const [dragItem, setDragItem] = useState(null);
    const [dropLine, setDropLine] = useState(null);
    const sessionIdRef = useRef(getOrCreateSessionId(id || 'pivot-grid'));
    const clientInstanceRef = useRef(createClientInstanceId(id || 'pivot-grid'));
    const requestVersionRef = useRef(Number(dataVersion) || 0);
    const latestDataVersionRef = useRef(Number(dataVersion) || 0);
    const stateEpochRef = useRef(0);
    const abortGenerationRef = useRef(0);
    const structuralPendingVersionRef = useRef(null);
    const expandAllDebounceRef = useRef(false);
    const latestViewportRef = useRef({ start: 0, end: 99, count: 100 });
    const [stateEpoch, setStateEpoch] = useState(0);
    const [abortGeneration, setAbortGeneration] = useState(0);
    const [structuralInFlight, setStructuralInFlight] = useState(false);
    const [pendingRowTransitions, setPendingRowTransitions] = useState(() => new Map());
    const [pendingColumnSkeletonCount, setPendingColumnSkeletonCount] = useState(0);
    const tableDataRef = useRef([]);
    const requestUrgentColumnViewportRef = useRef(null);
    const pivotProfilerRef = useRef(getPivotProfiler());
    const profilingEnabledRef = useRef(isPivotProfilingEnabled());
    const pendingDataProfilerCommitRef = useRef(null);
    const pendingDataProfilerFrameRef = useRef(null);
    const transactionUndoStackRef = useRef([]);
    const transactionRedoStackRef = useRef([]);
    const pendingTransactionHistoryRef = useRef(new Map());
    const transactionHistoryPendingRequestRef = useRef(null);
    const editSessionStateRef = useRef({
        sessionId: sessionIdRef.current,
        clientInstance: clientInstanceRef.current,
        sessionKey: null,
        sessionVersion: 0,
    });
    const optimisticCellValuesRef = useRef(new Map());
    const optimisticCellRequestsRef = useRef(new Map());
    const visiblePatchRowPathsRef = useRef([]);
    const visiblePatchCenterIdsRef = useRef([]);
    const deferredViewportRefreshTimeoutRef = useRef(null);
    const editedCellMarkersRef = useRef(new Map());
    const comparisonCellOriginalValuesRef = useRef(new Map());
    const comparisonCellCountsRef = useRef(new Map());
    const rowEditSessionsRef = useRef({});
    const editorOptionsLoadingRef = useRef({});
    const lastPropagationFormulaRef = useRef('equal');
    const pendingLayoutHistoryCaptureRef = useRef(null);
    const pendingLayoutHistoryCaptureTimeoutRef = useRef(null);
    const [, setOptimisticCellEpoch] = useState(0);
    const [editedCellEpoch, setEditedCellEpoch] = useState(0);
    const [comparisonCellEpoch, setComparisonCellEpoch] = useState(0);
    const [rowEditSessions, setRowEditSessions] = useState({});
    const [editValueDisplayMode, setEditValueDisplayMode] = useState('original');
    const propagationLogRef = useRef([]);
    const [propagationLogEpoch, setPropagationLogEpoch] = useState(0);
    const pendingPropagationUpdatesRef = useRef(null);
    const [pendingPropagationUpdates, setPendingPropagationUpdates] = useState(null);
    const [editPanelOpen, setEditPanelOpen] = useState(false);
    const [propagationMethodUI, setPropagationMethodUI] = useState('equal');
    const [transactionHistoryState, setTransactionHistoryState] = useState({
        undoCount: 0,
        redoCount: 0,
        pending: false,
    });
    const [comparisonValueState, setComparisonValueState] = useState({
        activeCount: 0,
    });
    const profilerComponentIdRef = useRef(id || 'pivot-grid');
    profilerComponentIdRef.current = id || 'pivot-grid';
    const {
        cachedColSchema,
        colRequestEnd,
        colRequestStart,
        colRequestEndRef,
        colRequestStartRef,
        colSchemaEpochRef,
        columnRangeUrgencyToken,
        handleHorizontalScrollMetrics,
        isHorizontalColumnRequestPending,
        isRequestPending,
        latestRequestedColumnWindowRef,
        latestRequestedViewportRef,
        markRequestPending,
        needsColSchema,
        needsColSchemaRef,
        pendingHorizontalColumnCount,
        resetVisibleColRange,
        responseSchemaWindow,
        resolveStableRequestedColumnWindow,
        serverSideBlockSize,
        syncPreciseVisibleColRange,
        totalCenterCols,
        visibleColRange,
    } = useServerSideViewportController({
        serverSide,
        effectiveRowCount: serverSidePinsGrandTotal && rowCount ? Math.max(rowCount - 1, 0) : rowCount,
        responseColumns,
        responseColSchema,
        dataVersion,
        stateEpoch,
        structuralInFlight,
        structuralPendingVersionRef,
        filteredData,
        rowFields,
        colFields,
        schemaFallbackWidth: defaultColumnWidths.schemaFallback,
        requestVersionRef,
        latestDataVersionRef,
        requestUrgentColumnViewportRef,
        coerceTransportNumber,
        mergeSparseColSchema,
        isSparseSchemaRangeLoaded,
        performanceConfig: normalizedPerformanceConfig,
    });

    const columnAdvisory = useMemo(
        () => buildLargeColumnAdvisory({ serverSide, totalCenterCols, colFields }),
        [colFields, serverSide, totalCenterCols]
    );
    const lastColumnAdvisoryRef = useRef('');

    useEffect(() => {
        if (!columnAdvisory) {
            lastColumnAdvisoryRef.current = '';
            return;
        }
        const advisoryKey = `${viewMode}|${colFields.join('|')}|${totalCenterCols}`;
        if (lastColumnAdvisoryRef.current === advisoryKey) return;
        lastColumnAdvisoryRef.current = advisoryKey;
        showNotification(columnAdvisory.notification, columnAdvisory.tone);
    }, [colFields, columnAdvisory, showNotification, totalCenterCols, viewMode]);

    const layoutHistorySnapshot = useMemo(() => cloneSerializable({
        rowFields,
        colFields,
        valConfigs,
        filters,
        sorting,
        expanded,
        colExpanded,
        columnPinning,
        rowPinning,
        showRowTotals,
        showColTotals,
        grandTotalPinOverride,
    }, null), [
        colExpanded,
        colFields,
        columnPinning,
        expanded,
        filters,
        grandTotalPinOverride,
        rowFields,
        rowPinning,
        showColTotals,
        showRowTotals,
        sorting,
        valConfigs,
    ]);
    const layoutHistorySnapshotRef = useRef(layoutHistorySnapshot);
    const layoutHistorySnapshotKey = useMemo(
        () => JSON.stringify(layoutHistorySnapshot || {}),
        [layoutHistorySnapshot]
    );

    useEffect(() => {
        rowEditSessionsRef.current = rowEditSessions;
    }, [rowEditSessions]);

    useEffect(() => {
        editorOptionsLoadingRef.current = editorOptionsLoadingState;
    }, [editorOptionsLoadingState]);

    const updateRowEditSessions = useCallback((updater) => {
        let nextSnapshot = rowEditSessionsRef.current;
        setRowEditSessions((previousSessions) => {
            const resolvedSessions = typeof updater === 'function'
                ? updater(previousSessions)
                : updater;
            nextSnapshot = resolvedSessions && typeof resolvedSessions === 'object'
                ? resolvedSessions
                : {};
            rowEditSessionsRef.current = nextSnapshot;
            return nextSnapshot;
        });
        return nextSnapshot;
    }, []);

    const syncTransactionHistoryState = useCallback(() => {
        setTransactionHistoryState({
            undoCount: transactionUndoStackRef.current.length,
            redoCount: transactionRedoStackRef.current.length,
            pending: Boolean(transactionHistoryPendingRequestRef.current),
        });
    }, []);

    const syncComparisonValueState = useCallback(() => {
        setComparisonValueState({
            activeCount: comparisonCellCountsRef.current.size,
        });
    }, []);

    const serializeEditComparisonState = useCallback(() => {
        const cells = [];
        comparisonCellOriginalValuesRef.current.forEach((entry, key) => {
            if (!entry || typeof entry !== 'object') return;
            const count = comparisonCellCountsRef.current.get(key) || 0;
            if (count <= 0) return;
            const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
            const colId = entry.colId === null || entry.colId === undefined ? '' : String(entry.colId);
            if (!rowId || !colId) return;
            cells.push({
                rowId,
                colId,
                originalValue: entry.value,
                comparisonCount: count,
                directEventIds: normalizeTrackedEventIds(entry.directEventIds),
                propagatedEventIds: normalizeTrackedEventIds(entry.propagatedEventIds),
            });
        });

        const markers = [];
        editedCellMarkersRef.current.forEach((entry, key) => {
            if (!entry || typeof entry !== 'object') return;
            const direct = (entry.directCount || 0) > 0;
            const propagated = (entry.propagatedCount || 0) > 0;
            if (!direct && !propagated) return;
            markers.push({
                key: String(key),
                direct,
                propagated,
                directCount: Math.max(0, Number(entry.directCount) || 0),
                propagatedCount: Math.max(0, Number(entry.propagatedCount) || 0),
            });
        });

        if (cells.length === 0 && markers.length === 0) {
            return null;
        }

        cells.sort((left, right) => `${left.rowId}:::${left.colId}`.localeCompare(`${right.rowId}:::${right.colId}`));
        markers.sort((left, right) => left.key.localeCompare(right.key));

        return {
            version: 1,
            cells,
            markers,
        };
    }, []);

    const restoreSerializedEditComparisonState = useCallback((serializedState) => {
        const hadComparisonEntries = comparisonCellCountsRef.current.size > 0 || comparisonCellOriginalValuesRef.current.size > 0;
        const hadEditedMarkers = editedCellMarkersRef.current.size > 0;

        comparisonCellCountsRef.current.clear();
        comparisonCellOriginalValuesRef.current.clear();
        editedCellMarkersRef.current.clear();

        let hasComparisonEntries = false;
        let hasEditedMarkers = false;
        const restored = serializedState && typeof serializedState === 'object'
            ? serializedState
            : null;

        (Array.isArray(restored && restored.cells) ? restored.cells : []).forEach((cell) => {
            if (!cell || typeof cell !== 'object') return;
            const key = getOptimisticCellKey(cell.rowId, cell.colId);
            if (!key) return;
            comparisonCellCountsRef.current.set(key, 1);
            comparisonCellOriginalValuesRef.current.set(key, {
                rowId: String(cell.rowId),
                colId: String(cell.colId),
                value: Object.prototype.hasOwnProperty.call(cell, 'originalValue') ? cell.originalValue : undefined,
                directEventIds: normalizeTrackedEventIds(cell.directEventIds),
                propagatedEventIds: normalizeTrackedEventIds(cell.propagatedEventIds),
            });
            const restoredCount = Math.max(
                1,
                Number(cell.comparisonCount) || 0,
                normalizeTrackedEventIds(cell.directEventIds).length + normalizeTrackedEventIds(cell.propagatedEventIds).length
            );
            comparisonCellCountsRef.current.set(key, restoredCount);
            hasComparisonEntries = true;
        });

        (Array.isArray(restored && restored.markers) ? restored.markers : []).forEach((marker) => {
            if (!marker || typeof marker !== 'object') return;
            const key = typeof marker.key === 'string'
                ? marker.key
                : getOptimisticCellKey(marker.rowId, marker.colId);
            if (!key) return;
            const nextEntry = {
                directCount: Math.max(0, Number(marker.directCount) || 0 || (marker.direct ? 1 : 0)),
                propagatedCount: Math.max(0, Number(marker.propagatedCount) || 0 || (marker.propagated ? 1 : 0)),
            };
            if (nextEntry.directCount <= 0 && nextEntry.propagatedCount <= 0) return;
            editedCellMarkersRef.current.set(key, nextEntry);
            hasEditedMarkers = true;
        });

        syncComparisonValueState();
        if (hadComparisonEntries || hasComparisonEntries) {
            bumpComparisonCellEpoch();
        }
        if (hadEditedMarkers || hasEditedMarkers) {
            bumpEditedCellEpoch();
        }
    }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, getOptimisticCellKey, normalizeTrackedEventIds, syncComparisonValueState]);

    const serializedEditComparisonState = useMemo(
        () => serializeEditComparisonState(),
        [comparisonCellEpoch, comparisonValueState.activeCount, editedCellEpoch, serializeEditComparisonState]
    );

    const bumpEditedCellEpoch = useCallback(() => {
        setEditedCellEpoch((previousEpoch) => previousEpoch + 1);
    }, []);

    const bumpOptimisticCellEpoch = useCallback(() => {
        setOptimisticCellEpoch((previousEpoch) => previousEpoch + 1);
    }, []);

    const bumpComparisonCellEpoch = useCallback(() => {
        setComparisonCellEpoch((previousEpoch) => previousEpoch + 1);
    }, []);

    const getOptimisticCellKey = useCallback((rowId, colId) => {
        const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
        const normalizedColId = colId === null || colId === undefined ? '' : String(colId);
        if (!normalizedRowId || !normalizedColId) return null;
        return `${normalizedRowId}:::${normalizedColId}`;
    }, []);

    const splitOptimisticCellKey = useCallback((key) => {
        if (typeof key !== 'string' || !key) return null;
        const separatorIndex = key.lastIndexOf(':::');
        if (separatorIndex <= 0) return null;
        return {
            rowId: key.slice(0, separatorIndex),
            colId: key.slice(separatorIndex + 3),
        };
    }, []);

    const normalizeTrackedEventIds = useCallback((eventIds) => (
        Array.isArray(eventIds)
            ? Array.from(new Set(eventIds.map((value) => String(value || '').trim()).filter(Boolean)))
            : []
    ), []);

    const resolveOptimisticCellValue = useCallback((rowId, colId, fallbackValue) => {
        const key = getOptimisticCellKey(rowId, colId);
        if (!key) return fallbackValue;
        const optimisticEntry = optimisticCellValuesRef.current.get(key);
        return optimisticEntry ? optimisticEntry.value : fallbackValue;
    }, [getOptimisticCellKey]);

    const resolveVisibleDataRowId = useCallback((row) => {
        if (!row || typeof row !== 'object') return null;
        if (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total') return GRAND_TOTAL_ROW_ID;
        if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
        if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
        if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
        return null;
    }, []);

    const resolveCurrentCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
        if (rowId === null || rowId === undefined || colId === null || colId === undefined) return fallbackValue;
        const normalizedRowId = String(rowId);
        const normalizedColId = String(colId);
        const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
        const matchedRow = visibleRows.find((row) => resolveVisibleDataRowId(row) === normalizedRowId) || null;
        const resolvedFallback = matchedRow && Object.prototype.hasOwnProperty.call(matchedRow, normalizedColId)
            ? matchedRow[normalizedColId]
            : fallbackValue;
        return resolveOptimisticCellValue(normalizedRowId, normalizedColId, resolvedFallback);
    }, [resolveOptimisticCellValue, resolveVisibleDataRowId]);

    const resolveDisplayedCellValue = useCallback((rowId, colId, fallbackValue = undefined) => {
        const currentValue = resolveCurrentCellValue(rowId, colId, fallbackValue);
        if (editValueDisplayMode !== 'original') return currentValue;
        const key = getOptimisticCellKey(rowId, colId);
        if (!key) return currentValue;
        const originalEntry = comparisonCellOriginalValuesRef.current.get(key);
        return originalEntry ? originalEntry.value : currentValue;
    }, [editValueDisplayMode, getOptimisticCellKey, resolveCurrentCellValue]);

    const ensureTrackedComparisonCellEntry = useCallback((key, fallback = null) => {
        if (!key) return null;
        const existingEntry = comparisonCellOriginalValuesRef.current.get(key);
        if (existingEntry && typeof existingEntry === 'object') {
            return existingEntry;
        }
        const derived = splitOptimisticCellKey(key);
        const rowId = fallback && fallback.rowId !== undefined ? String(fallback.rowId) : (derived ? derived.rowId : '');
        const colId = fallback && fallback.colId !== undefined ? String(fallback.colId) : (derived ? derived.colId : '');
        if (!rowId || !colId) return null;
        const createdEntry = {
            rowId,
            colId,
            value: fallback && Object.prototype.hasOwnProperty.call(fallback, 'value')
                ? fallback.value
                : resolveCurrentCellValue(rowId, colId),
            directEventIds: [],
            propagatedEventIds: [],
        };
        comparisonCellOriginalValuesRef.current.set(key, createdEntry);
        return createdEntry;
    }, [resolveCurrentCellValue, splitOptimisticCellKey]);

    const appendTrackedEventOwnership = useCallback((eventId, affectedCells) => {
        const normalizedEventId = String(eventId || '').trim();
        if (!normalizedEventId || !affectedCells || typeof affectedCells !== 'object') return;
        let didChange = false;
        const appendKeys = (keys, fieldName) => {
            (Array.isArray(keys) ? keys : []).forEach((key) => {
                const entry = ensureTrackedComparisonCellEntry(key);
                if (!entry) return;
                const currentIds = normalizeTrackedEventIds(entry[fieldName]);
                if (currentIds.includes(normalizedEventId)) {
                    entry[fieldName] = currentIds;
                    return;
                }
                entry[fieldName] = [...currentIds, normalizedEventId];
                didChange = true;
            });
        };
        appendKeys(affectedCells.direct, 'directEventIds');
        appendKeys(affectedCells.propagated, 'propagatedEventIds');
        if (didChange) {
            bumpComparisonCellEpoch();
        }
    }, [bumpComparisonCellEpoch, ensureTrackedComparisonCellEntry, normalizeTrackedEventIds]);

    const detachTrackedEventOwnership = useCallback((eventIds, options = {}) => {
        const normalizedEventIds = normalizeTrackedEventIds(eventIds);
        if (normalizedEventIds.length === 0) return;
        const eventIdSet = new Set(normalizedEventIds);
        const adjustCounts = Boolean(options.adjustCounts);
        let didChange = false;

        comparisonCellOriginalValuesRef.current.forEach((entry, key) => {
            if (!entry || typeof entry !== 'object') return;
            const directEventIds = normalizeTrackedEventIds(entry.directEventIds);
            const propagatedEventIds = normalizeTrackedEventIds(entry.propagatedEventIds);
            const nextDirectEventIds = directEventIds.filter((eventId) => !eventIdSet.has(eventId));
            const nextPropagatedEventIds = propagatedEventIds.filter((eventId) => !eventIdSet.has(eventId));
            const removedDirectCount = directEventIds.length - nextDirectEventIds.length;
            const removedPropagatedCount = propagatedEventIds.length - nextPropagatedEventIds.length;
            if (removedDirectCount <= 0 && removedPropagatedCount <= 0) return;

            entry.directEventIds = nextDirectEventIds;
            entry.propagatedEventIds = nextPropagatedEventIds;
            didChange = true;

            if (!adjustCounts) return;

            const existingComparisonCount = comparisonCellCountsRef.current.get(key) || 0;
            const nextComparisonCount = Math.max(0, existingComparisonCount - removedDirectCount - removedPropagatedCount);
            if (nextComparisonCount > 0) {
                comparisonCellCountsRef.current.set(key, nextComparisonCount);
            } else {
                comparisonCellCountsRef.current.delete(key);
                comparisonCellOriginalValuesRef.current.delete(key);
            }

            const existingMarker = editedCellMarkersRef.current.get(key) || { directCount: 0, propagatedCount: 0 };
            const nextMarker = {
                directCount: Math.max(0, (existingMarker.directCount || 0) - removedDirectCount),
                propagatedCount: Math.max(0, (existingMarker.propagatedCount || 0) - removedPropagatedCount),
            };
            if (nextMarker.directCount > 0 || nextMarker.propagatedCount > 0) {
                editedCellMarkersRef.current.set(key, nextMarker);
            } else {
                editedCellMarkersRef.current.delete(key);
            }
        });

        if (didChange) {
            if (adjustCounts) {
                syncComparisonValueState();
                bumpEditedCellEpoch();
            }
            bumpComparisonCellEpoch();
        }
    }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, normalizeTrackedEventIds, syncComparisonValueState]);

    const resolveTrackedCellEventIds = useCallback((rowId, colId, options = {}) => {
        const key = getOptimisticCellKey(rowId, colId);
        if (!key) return [];
        const entry = comparisonCellOriginalValuesRef.current.get(key);
        if (!entry || typeof entry !== 'object') return [];
        const includeDirect = options.includeDirect !== false;
        const includePropagated = options.includePropagated !== false;
        const eventIds = [];
        if (includeDirect) {
            eventIds.push(...normalizeTrackedEventIds(entry.directEventIds));
        }
        if (includePropagated) {
            eventIds.push(...normalizeTrackedEventIds(entry.propagatedEventIds));
        }
        return Array.from(new Set(eventIds));
    }, [getOptimisticCellKey, normalizeTrackedEventIds]);

    const resolveCellEditOwnershipState = useCallback((rowId, colId) => {
        const directEventIds = resolveTrackedCellEventIds(rowId, colId, {
            includeDirect: true,
            includePropagated: false,
        });
        const propagatedEventIds = resolveTrackedCellEventIds(rowId, colId, {
            includeDirect: false,
            includePropagated: true,
        });
        if (directEventIds.length > 0 && propagatedEventIds.length > 0) {
            return {
                mode: 'blocked',
                targetEventIds: [],
                reason: 'This cell has overlapping active edit ownership. Revert the existing edit before changing it again.',
            };
        }
        if (propagatedEventIds.length > 0) {
            return {
                mode: 'blocked',
                targetEventIds: [],
                reason: 'This cell is locked by an active parent or child edit. Revert that edit before modifying this cell directly.',
            };
        }
        if (directEventIds.length > 1) {
            return {
                mode: 'blocked',
                targetEventIds: [],
                reason: 'This cell has multiple active direct edits. Revert the existing edits before changing it again.',
            };
        }
        if (directEventIds.length === 1) {
            return {
                mode: 'replace',
                targetEventIds: directEventIds,
                reason: null,
            };
        }
        return {
            mode: 'apply',
            targetEventIds: [],
            reason: null,
        };
    }, [resolveTrackedCellEventIds]);

    const findTransactionHistoryEntryByEventId = useCallback((eventId) => {
        const normalizedEventId = String(eventId || '').trim();
        if (!normalizedEventId) return null;
        const findInEntries = (entries) => (
            Array.isArray(entries)
                ? entries.find((entry) => entry && entry.kind === 'transaction' && entry.eventId === normalizedEventId) || null
                : null
        );
        return findInEntries(transactionUndoStackRef.current) || findInEntries(transactionRedoStackRef.current);
    }, []);

    const sortEventIdsByActiveHistory = useCallback((eventIds) => {
        const normalizedEventIds = normalizeTrackedEventIds(eventIds);
        if (normalizedEventIds.length <= 1) return normalizedEventIds;
        const orderMap = new Map();
        transactionUndoStackRef.current.forEach((entry, index) => {
            if (!entry || entry.kind !== 'transaction' || !entry.eventId) return;
            orderMap.set(entry.eventId, index);
        });
        return normalizedEventIds.sort((left, right) => {
            const leftIndex = orderMap.has(left) ? orderMap.get(left) : -1;
            const rightIndex = orderMap.has(right) ? orderMap.get(right) : -1;
            return rightIndex - leftIndex;
        });
    }, [normalizeTrackedEventIds]);

    const pruneTransactionHistoryEntries = useCallback((eventIds) => {
        const normalizedEventIds = new Set(normalizeTrackedEventIds(eventIds));
        if (normalizedEventIds.size === 0) return;
        transactionUndoStackRef.current = transactionUndoStackRef.current.filter((entry) => (
            !entry || entry.kind !== 'transaction' || !normalizedEventIds.has(entry.eventId)
        ));
        transactionRedoStackRef.current = transactionRedoStackRef.current.filter((entry) => (
            !entry || entry.kind !== 'transaction' || !normalizedEventIds.has(entry.eventId)
        ));
        syncTransactionHistoryState();
    }, [normalizeTrackedEventIds, syncTransactionHistoryState]);

    const hydrateVisibleEditOverlay = useCallback((editOverlay) => {
        const overlayCells = Array.isArray(editOverlay && editOverlay.cells) ? editOverlay.cells : [];
        if (overlayCells.length === 0) return;
        let didComparisonChange = false;
        let didMarkerChange = false;

        overlayCells.forEach((cell) => {
            if (!cell || typeof cell !== 'object') return;
            const key = getOptimisticCellKey(cell.rowId, cell.colId);
            if (!key) return;
            const directEventIds = normalizeTrackedEventIds(cell.directEventIds);
            const propagatedEventIds = normalizeTrackedEventIds(cell.propagatedEventIds);
            const existingEntry = comparisonCellOriginalValuesRef.current.get(key);
            const nextOriginalValue = Object.prototype.hasOwnProperty.call(cell, 'originalValue')
                ? cell.originalValue
                : (existingEntry ? existingEntry.value : undefined);
            const nextEntry = {
                rowId: String(cell.rowId),
                colId: String(cell.colId),
                value: nextOriginalValue,
                directEventIds,
                propagatedEventIds,
            };
            const didEntryChange = (
                !existingEntry
                || existingEntry.value !== nextEntry.value
                || JSON.stringify(normalizeTrackedEventIds(existingEntry.directEventIds)) !== JSON.stringify(directEventIds)
                || JSON.stringify(normalizeTrackedEventIds(existingEntry.propagatedEventIds)) !== JSON.stringify(propagatedEventIds)
            );
            if (didEntryChange) {
                comparisonCellOriginalValuesRef.current.set(key, nextEntry);
                didComparisonChange = true;
            }

            const nextComparisonCount = Math.max(
                1,
                Number(cell.comparisonCount) || 0,
                directEventIds.length + propagatedEventIds.length,
            );
            if ((comparisonCellCountsRef.current.get(key) || 0) !== nextComparisonCount) {
                comparisonCellCountsRef.current.set(key, nextComparisonCount);
                didComparisonChange = true;
            }

            const nextMarkerEntry = {
                directCount: directEventIds.length,
                propagatedCount: propagatedEventIds.length,
            };
            const existingMarkerEntry = editedCellMarkersRef.current.get(key);
            if (
                !existingMarkerEntry
                || (existingMarkerEntry.directCount || 0) !== nextMarkerEntry.directCount
                || (existingMarkerEntry.propagatedCount || 0) !== nextMarkerEntry.propagatedCount
            ) {
                editedCellMarkersRef.current.set(key, nextMarkerEntry);
                didMarkerChange = true;
            }
        });

        if (didComparisonChange) {
            syncComparisonValueState();
            bumpComparisonCellEpoch();
        }
        if (didMarkerChange) {
            bumpEditedCellEpoch();
        }
    }, [bumpComparisonCellEpoch, bumpEditedCellEpoch, getOptimisticCellKey, normalizeTrackedEventIds, syncComparisonValueState]);

    const getDirectVisibleChildRowIds = useCallback((ancestorRowId) => {
        const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
        if (ancestorRowId === GRAND_TOTAL_ROW_ID) {
            return visibleRows
                .map(resolveVisibleDataRowId)
                .filter((rowId) => typeof rowId === 'string' && rowId !== GRAND_TOTAL_ROW_ID && !rowId.includes('|||'));
        }
        const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
        if (!normalizedAncestorId) return [];
        const ancestorDepth = normalizedAncestorId.split('|||').length;
        const directChildPrefix = `${normalizedAncestorId}|||`;
        return visibleRows
            .map(resolveVisibleDataRowId)
            .filter((rowId) => (
                typeof rowId === 'string'
                && rowId.startsWith(directChildPrefix)
                && rowId.split('|||').length === ancestorDepth + 1
            ));
    }, [resolveVisibleDataRowId]);

    const resolveAggregationConfigForColumnId = useCallback((columnId) => {
        if (!Array.isArray(valConfigs) || !columnId) return null;
        const normalizedId = String(columnId).trim().toLowerCase();
        if (!normalizedId) return null;
        const matchedConfig = valConfigs.find((config) => {
            if (!config || typeof config !== 'object' || !config.field) return false;
            if (config.agg === 'formula') {
                const formulaId = String(config.field).trim().toLowerCase();
                return normalizedId === formulaId || normalizedId.endsWith(`_${formulaId}`);
            }
            const measureId = getKey('', config.field, config.agg).toLowerCase();
            const measureSuffix = `_${config.field}_${config.agg}`.toLowerCase();
            return normalizedId === measureId || normalizedId.endsWith(measureSuffix);
        }) || null;
        if (!matchedConfig) return null;
        return {
            field: matchedConfig.field || null,
            agg: matchedConfig.agg || null,
            weightField: matchedConfig.weightField || null,
            windowFn: matchedConfig.windowFn || null,
        };
    }, [valConfigs]);

    const isEditableAggregationColumn = useCallback((config, columnId) => {
        if (!config || typeof config !== 'object') return false;
        if (typeof columnId === 'string' && columnId.startsWith('__RowTotal__')) return false;
        const normalizedAgg = String(config.agg || '').trim().toLowerCase();
        if (!normalizedAgg || normalizedAgg === 'formula') return false;
        if (config.windowFn) return false;
        return !['count', 'count_distinct', 'distinct_count'].includes(normalizedAgg);
    }, []);

    const resolveEditorPresentation = useCallback((rowId, columnId, columnConfig = null, currentValue = undefined, defaultEditable = false) => {
        const editorConfig = resolveColumnEditSpec({
            editingConfig: normalizedEditingConfig,
            validationRules,
            columnId,
            columnConfig,
            currentValue,
            defaultEditable,
        });
        if (!editorConfig || editorConfig.editable === false) return null;
        const ownershipState = (
            serverSide
            && rowId !== null
            && rowId !== undefined
            && columnId !== null
            && columnId !== undefined
        )
            ? resolveCellEditOwnershipState(String(rowId), String(columnId))
            : { mode: 'apply', targetEventIds: [], reason: null };
        const optionsSource = editorConfig.optionsSource || resolveEditorOptionsSource(editorConfig, columnId, columnConfig);
        const optionsKey = optionsSource && optionsSource.columnId ? optionsSource.columnId : null;
        const options = optionsKey && Array.isArray(filterOptions[optionsKey])
            ? filterOptions[optionsKey]
            : (Array.isArray(editorConfig.options) ? editorConfig.options : []);
        const loading = Boolean(optionsKey && editorOptionsLoadingRef.current[optionsKey]);
        return {
            editorConfig: {
                ...editorConfig,
                optionsSource,
                options,
                editOwnershipMode: ownershipState.mode,
                replaceEventIds: ownershipState.targetEventIds,
            },
            options,
            loading,
            optionsKey,
            editingDisabled: ownershipState.mode === 'blocked',
            editingDisabledReason: ownershipState.reason,
        };
    }, [filterOptions, normalizedEditingConfig, resolveCellEditOwnershipState, serverSide, validationRules]);

    const getRowEditSession = useCallback((rowId) => {
        if (rowId === null || rowId === undefined) return null;
        return rowEditSessionsRef.current[String(rowId)] || null;
    }, []);

    const closeRowEditSession = useCallback((rowId) => {
        if (rowId === null || rowId === undefined) return;
        const normalizedRowId = String(rowId);
        updateRowEditSessions((previousSessions) => {
            if (!previousSessions[normalizedRowId]) return previousSessions;
            const nextSessions = { ...previousSessions };
            delete nextSessions[normalizedRowId];
            return nextSessions;
        });
    }, [updateRowEditSessions]);

    const resolveRowEditableColumns = useCallback((row) => {
        if (!row || !tableRef.current || typeof tableRef.current.getVisibleLeafColumns !== 'function') return [];
        const rowId = row && row.original && row.original._path ? row.original._path : row.id;
        return tableRef.current.getVisibleLeafColumns().reduce((acc, column) => {
            if (!column || !column.id) return acc;
            if (column.id === 'hierarchy' || column.id === '__row_number__' || String(column.id).startsWith('__tree_level__')) return acc;
            if (column.columnDef && column.columnDef.meta && column.columnDef.meta.isSchemaPlaceholder) return acc;
            const columnConfig = resolveAggregationConfigForColumnId(column.id);
            const currentValue = typeof row.getValue === 'function'
                ? row.getValue(column.id)
                : (row.original && Object.prototype.hasOwnProperty.call(row.original, column.id) ? row.original[column.id] : undefined);
            const editorState = resolveEditorPresentation(
                rowId,
                column.id,
                columnConfig,
                currentValue,
                isEditableAggregationColumn(columnConfig, column.id),
            );
            if (!editorState || !editorState.editorConfig || editorState.editorConfig.editable === false) {
                return acc;
            }
            if (editorState.editingDisabled) {
                return acc;
            }
            acc.push({
                colId: column.id,
                columnConfig,
                editorConfig: editorState.editorConfig,
                currentValue: resolveCurrentCellValue(rowId, column.id, currentValue),
            });
            return acc;
        }, []);
    }, [isEditableAggregationColumn, resolveAggregationConfigForColumnId, resolveCurrentCellValue, resolveEditorPresentation]);

    const startRowEditSession = useCallback((row, options = {}) => {
        if (!row) return;
        const rowId = row && row.original && row.original._path ? row.original._path : row.id;
        if (!rowId) return;
        const normalizedRowId = String(rowId);
        const editableColumns = resolveRowEditableColumns(row);
        if (editableColumns.length === 0) {
            showNotification('This row does not expose editable visible values.', 'warning');
            return;
        }
        const normalizedFocusColumnId = options && options.focusColumnId !== undefined && options.focusColumnId !== null
            ? String(options.focusColumnId)
            : null;
        const editableColumnMap = editableColumns.reduce((acc, entry) => {
            acc[entry.colId] = {
                columnConfig: entry.columnConfig,
                editorConfig: entry.editorConfig,
            };
            return acc;
        }, {});
        const originalValues = editableColumns.reduce((acc, entry) => {
            acc[entry.colId] = entry.currentValue;
            return acc;
        }, {});
        let createdSession = false;
        updateRowEditSessions((previousSessions) => {
            const previousSession = previousSessions[normalizedRowId] || null;
            if (!previousSession) createdSession = true;
            return {
                ...previousSessions,
                [normalizedRowId]: {
                    active: true,
                    status: previousSession && previousSession.status === 'saving' ? 'saving' : 'editing',
                    drafts: previousSession && previousSession.drafts ? { ...previousSession.drafts } : {},
                    errors: previousSession && previousSession.errors ? { ...previousSession.errors } : {},
                    touched: previousSession && previousSession.touched ? { ...previousSession.touched } : {},
                    originalValues: {
                        ...originalValues,
                        ...(previousSession && previousSession.originalValues ? previousSession.originalValues : {}),
                    },
                    editableColumns: {
                        ...(previousSession && previousSession.editableColumns ? previousSession.editableColumns : {}),
                        ...editableColumnMap,
                    },
                    autoSave: Boolean(options && options.autoSave),
                    focusColumnId: normalizedFocusColumnId,
                    trigger: options && options.trigger ? String(options.trigger) : (previousSession && previousSession.trigger ? previousSession.trigger : 'manual'),
                },
            };
        });
        if (createdSession) {
            emitEditLifecycleEvent({
                kind: 'row_edit_start',
                rowId: normalizedRowId,
                columnIds: editableColumns.map((entry) => entry.colId),
            });
        }
    }, [emitEditLifecycleEvent, resolveRowEditableColumns, showNotification, updateRowEditSessions]);

    const handleBlockedCellEdit = useCallback((payload) => {
        if (!payload || typeof payload !== 'object') return;
        showNotification(
            payload.reason || 'This cell cannot be edited while an overlapping edit is active.',
            'warning',
        );
    }, [showNotification]);

    const updateRowDraftValue = useCallback((rowId, colId, nextValue, meta = {}) => {
        if (rowId === null || rowId === undefined || !colId) return;
        const normalizedRowId = String(rowId);
        const normalizedColId = String(colId);
        let emittedValidationError = null;
        updateRowEditSessions((previousSessions) => {
            const session = previousSessions[normalizedRowId];
            if (!session) return previousSessions;
            const nextDrafts = { ...(session.drafts || {}) };
            const nextErrors = { ...(session.errors || {}) };
            const nextTouched = { ...(session.touched || {}), [normalizedColId]: true };
            const originalValue = session.originalValues ? session.originalValues[normalizedColId] : undefined;
            if (cellValuesMatch(nextValue, originalValue)) {
                delete nextDrafts[normalizedColId];
            } else {
                nextDrafts[normalizedColId] = nextValue;
            }
            const editorConfig = (
                meta && meta.editorConfig
                ? meta.editorConfig
                : (
                    session.editableColumns
                    && session.editableColumns[normalizedColId]
                    && session.editableColumns[normalizedColId].editorConfig
                )
            ) || {};
            const nextRowValues = {
                ...(session.originalValues || {}),
                ...nextDrafts,
                [normalizedColId]: Object.prototype.hasOwnProperty.call(nextDrafts, normalizedColId) ? nextDrafts[normalizedColId] : originalValue,
            };
            const validation = validateEditorValue(nextRowValues[normalizedColId], editorConfig.validationRules || [], {
                columnId: normalizedColId,
                columnLabel: normalizedColId,
                rowId: normalizedRowId,
                rowValues: nextRowValues,
            });
            if (!validation.valid) {
                nextErrors[normalizedColId] = validation.error || 'Invalid value';
                emittedValidationError = nextErrors[normalizedColId];
            } else {
                delete nextErrors[normalizedColId];
            }
            return {
                ...previousSessions,
                [normalizedRowId]: {
                    ...session,
                    status: 'editing',
                    drafts: nextDrafts,
                    errors: nextErrors,
                    touched: nextTouched,
                },
            };
        });
        if (emittedValidationError) {
            emitEditLifecycleEvent({
                kind: 'row_edit_validation_error',
                rowId: normalizedRowId,
                colId: normalizedColId,
                message: emittedValidationError,
            });
        }
    }, [emitEditLifecycleEvent, updateRowEditSessions]);

    const cancelRowEditSession = useCallback((rowId) => {
        if (rowId === null || rowId === undefined) return;
        closeRowEditSession(rowId);
        emitEditLifecycleEvent({
            kind: 'row_edit_cancel',
            rowId: String(rowId),
        });
    }, [closeRowEditSession, emitEditLifecycleEvent]);

    const getRowEditMeta = useCallback((row) => {
        if (!row) return {
            rowId: null,
            session: null,
            canEdit: false,
            dirtyCount: 0,
            errorCount: 0,
            saving: false,
        };
        const rowId = row && row.original && row.original._path ? row.original._path : row.id;
        const session = rowId ? getRowEditSession(rowId) : null;
        const editableColumns = resolveRowEditableColumns(row);
        return {
            rowId: rowId ? String(rowId) : null,
            session,
            canEdit: editableColumns.length > 0,
            dirtyCount: session ? Object.keys(session.drafts || {}).length : 0,
            errorCount: session ? Object.values(session.errors || {}).filter(Boolean).length : 0,
            saving: Boolean(session && session.status === 'saving'),
        };
    }, [getRowEditSession, resolveRowEditableColumns]);

    const clearAllOptimisticCellValues = useCallback(() => {
        const hadEntries = optimisticCellValuesRef.current.size > 0 || optimisticCellRequestsRef.current.size > 0;
        optimisticCellValuesRef.current.clear();
        optimisticCellRequestsRef.current.clear();
        if (hadEntries) bumpOptimisticCellEpoch();
    }, [bumpOptimisticCellEpoch]);

    const captureOptimisticCellValues = useCallback((updates, requestId) => {
        const requestKeys = new Set();
        const storeOptimisticValue = (rowId, colId, value) => {
            const key = getOptimisticCellKey(rowId, colId);
            if (!key) return;
            optimisticCellValuesRef.current.set(key, {
                requestId,
                rowId: String(rowId),
                colId: String(colId),
                value,
            });
            requestKeys.add(key);
        };
        (Array.isArray(updates) ? updates : []).forEach((update) => {
            if (!update || typeof update !== 'object') return;
            const rowId = update.rowPath || update.rowId;
            const colId = update.colId;
            storeOptimisticValue(rowId, colId, update.value);

            const aggregation = update.aggregation && typeof update.aggregation === 'object'
                ? update.aggregation
                : resolveAggregationConfigForColumnId(colId);
            const aggregationFn = aggregation && aggregation.agg
                ? String(aggregation.agg).trim().toLowerCase()
                : '';
            if (!aggregationFn || aggregation.windowFn) return;
            const normalizedRowPath = rowId === null || rowId === undefined ? '' : String(rowId);
            if (!normalizedRowPath || normalizedRowPath === GRAND_TOTAL_ROW_ID) return;

            const pathParts = normalizedRowPath.split('|||');
            const ancestorPaths = [];
            for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
                ancestorPaths.push(pathParts.slice(0, depth).join('|||'));
            }
            ancestorPaths.push(GRAND_TOTAL_ROW_ID);
            if (ancestorPaths.length === 0) return;

            const nextValue = Number(update.value);
            const previousValue = Number(update.oldValue);
            if (aggregationFn === 'sum') {
                if (!Number.isFinite(nextValue) || !Number.isFinite(previousValue)) return;
                const delta = nextValue - previousValue;
                if (!Number.isFinite(delta) || delta === 0) return;
                ancestorPaths.forEach((ancestorPath) => {
                    const ancestorValue = Number(resolveCurrentCellValue(ancestorPath, colId));
                    if (!Number.isFinite(ancestorValue)) return;
                    storeOptimisticValue(ancestorPath, colId, ancestorValue + delta);
                });
                return;
            }

            if (aggregationFn === 'min' || aggregationFn === 'max') {
                if (!Number.isFinite(nextValue)) return;
                const reducer = aggregationFn === 'min' ? Math.min : Math.max;
                ancestorPaths.forEach((ancestorPath) => {
                    const childRowIds = getDirectVisibleChildRowIds(ancestorPath);
                    if (childRowIds.length === 0) return;
                    const childValues = childRowIds
                        .map((childRowId) => {
                            if (childRowId === normalizedRowPath) return nextValue;
                            return Number(resolveCurrentCellValue(childRowId, colId));
                        })
                        .filter((value) => Number.isFinite(value));
                    if (childValues.length === 0) return;
                    storeOptimisticValue(ancestorPath, colId, reducer(...childValues));
                });
            }
        });
        if (!requestId || requestKeys.size === 0) return;
        optimisticCellRequestsRef.current.set(requestId, Array.from(requestKeys));
        bumpOptimisticCellEpoch();
    }, [bumpOptimisticCellEpoch, getDirectVisibleChildRowIds, getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveCurrentCellValue]);

    const releaseOptimisticCellRequest = useCallback((requestId) => {
        if (!requestId) return;
        optimisticCellRequestsRef.current.delete(requestId);
    }, []);

    const clearOptimisticCellValuesForRequest = useCallback((requestId) => {
        if (!requestId) return;
        const requestKeys = optimisticCellRequestsRef.current.get(requestId);
        if (!Array.isArray(requestKeys) || requestKeys.length === 0) {
            optimisticCellRequestsRef.current.delete(requestId);
            return;
        }
        let didDelete = false;
        requestKeys.forEach((key) => {
            const optimisticEntry = optimisticCellValuesRef.current.get(key);
            if (!optimisticEntry || optimisticEntry.requestId !== requestId) return;
            optimisticCellValuesRef.current.delete(key);
            didDelete = true;
        });
        optimisticCellRequestsRef.current.delete(requestId);
        if (didDelete) bumpOptimisticCellEpoch();
    }, [bumpOptimisticCellEpoch]);

    const resolveEditedCellMarker = useCallback((rowId, colId) => {
        const key = getOptimisticCellKey(rowId, colId);
        if (!key) return null;
        const marker = editedCellMarkersRef.current.get(key);
        if (!marker || (marker.directCount || 0) <= 0 && (marker.propagatedCount || 0) <= 0) {
            return null;
        }
        return {
            direct: (marker.directCount || 0) > 0,
            propagated: (marker.propagatedCount || 0) > 0,
        };
    }, [getOptimisticCellKey]);

    const clearAllEditedCellMarkers = useCallback(() => {
        if (editedCellMarkersRef.current.size === 0) return;
        editedCellMarkersRef.current.clear();
        bumpEditedCellEpoch();
    }, [bumpEditedCellEpoch]);

    const clearAllComparisonValueState = useCallback(() => {
        const hadEntries = comparisonCellCountsRef.current.size > 0 || comparisonCellOriginalValuesRef.current.size > 0;
        comparisonCellCountsRef.current.clear();
        comparisonCellOriginalValuesRef.current.clear();
        if (hadEntries) {
            syncComparisonValueState();
            bumpComparisonCellEpoch();
        }
    }, [bumpComparisonCellEpoch, syncComparisonValueState]);

    const didRestorePersistedEditComparisonRef = useRef(false);
    const didAttemptEditComparisonRestoreRef = useRef(false);
    const lastRestoredViewEditComparisonKeyRef = useRef(null);

    useEffect(() => {
        if (!persistence || !didAttemptEditComparisonRestoreRef.current) return;
        savePersistedState('editComparisonState', serializedEditComparisonState);
        savePersistedState('editValueDisplayMode', editValueDisplayMode);
    }, [editValueDisplayMode, persistence, savePersistedState, serializedEditComparisonState]);

    // Push edit state to Dash via setProps so the backend can persist/modify it
    const lastEmittedEditStateKeyRef = useRef(null);
    const editStateSessionKey = id ? `__pivot_editState_${typeof id === 'string' ? id : JSON.stringify(id)}` : null;
    const didRestoreEditStateFromPropRef = useRef(false);

    // Restore edit state FIRST: prefer prop, then sessionStorage
    useEffect(() => {
        if (didRestoreEditStateFromPropRef.current) return;
        didRestoreEditStateFromPropRef.current = true;
        let stateToRestore = externalEditState;
        if ((!stateToRestore || typeof stateToRestore !== 'object') && editStateSessionKey) {
            try {
                const stored = sessionStorage.getItem(editStateSessionKey);
                if (stored) stateToRestore = JSON.parse(stored);
            } catch (_ignored) { /* parse error or unavailable */ }
        }
        if (!stateToRestore || typeof stateToRestore !== 'object') return;
        if (!stateToRestore.cells && !stateToRestore.markers) return;
        restoreSerializedEditComparisonState(stateToRestore);
        if (stateToRestore.displayMode === 'edited' || stateToRestore.displayMode === 'original') {
            setEditValueDisplayMode(stateToRestore.displayMode);
        }
        if (Array.isArray(stateToRestore.propagationLog) && stateToRestore.propagationLog.length > 0) {
            propagationLogRef.current = stateToRestore.propagationLog;
            setPropagationLogEpoch((p) => p + 1);
        }
    }, [editStateSessionKey, externalEditState, restoreSerializedEditComparisonState]);

    // Save edit state to Dash and sessionStorage AFTER restore has been attempted
    useEffect(() => {
        if (!didRestoreEditStateFromPropRef.current) return;
        const propagationLog = propagationLogRef.current.length > 0
            ? propagationLogRef.current
            : undefined;
        const statePayload = serializedEditComparisonState
            ? { ...serializedEditComparisonState, displayMode: editValueDisplayMode, propagationLog }
            : (propagationLog ? { version: 1, cells: [], markers: [], displayMode: editValueDisplayMode, propagationLog } : null);
        const stateKey = statePayload ? JSON.stringify(statePayload) : '__empty__';
        if (stateKey === lastEmittedEditStateKeyRef.current) return;
        lastEmittedEditStateKeyRef.current = stateKey;
        if (typeof setProps === 'function') {
            setProps({ editState: statePayload });
        }
        // Auto-save to sessionStorage so edits survive page refresh
        if (editStateSessionKey) {
            try {
                if (statePayload) {
                    sessionStorage.setItem(editStateSessionKey, JSON.stringify(statePayload));
                } else {
                    sessionStorage.removeItem(editStateSessionKey);
                }
            } catch (_ignored) { /* storage full or unavailable */ }
        }
    }, [editStateSessionKey, editValueDisplayMode, propagationLogEpoch, serializedEditComparisonState, setProps]);

    const buildEditedCellMarkerPlan = useCallback((updates) => {
        const directKeys = new Set();
        const propagatedKeys = new Set();
        const markCell = (rowId, colId, kind = 'propagated') => {
            const key = getOptimisticCellKey(rowId, colId);
            if (!key) return;
            if (kind === 'direct') {
                directKeys.add(key);
                propagatedKeys.delete(key);
                return;
            }
            if (!directKeys.has(key)) {
                propagatedKeys.add(key);
            }
        };
        const markVisibleDescendants = (ancestorRowId, colId) => {
            const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
            if (!normalizedAncestorId) return;
            const descendantPrefix = `${normalizedAncestorId}|||`;
            const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
            visibleRows.forEach((row) => {
                const rowId = resolveVisibleDataRowId(row);
                if (!rowId || rowId === normalizedAncestorId) return;
                if (normalizedAncestorId === GRAND_TOTAL_ROW_ID) {
                    if (rowId !== GRAND_TOTAL_ROW_ID) markCell(rowId, colId, 'propagated');
                    return;
                }
                if (rowId.startsWith(descendantPrefix)) markCell(rowId, colId, 'propagated');
            });
        };

        (Array.isArray(updates) ? updates : []).forEach((update) => {
            if (!update || typeof update !== 'object') return;
            const rowId = update.rowPath || update.rowId;
            const colId = update.colId;
            markCell(rowId, colId, 'direct');

            const aggregation = update.aggregation && typeof update.aggregation === 'object'
                ? update.aggregation
                : resolveAggregationConfigForColumnId(colId);
            const aggregationFn = aggregation && aggregation.agg
                ? String(aggregation.agg).trim().toLowerCase()
                : '';
            if (!aggregationFn || aggregation.windowFn) return;

            const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
            if (!normalizedRowId) return;
            const strategy = String(update.propagationStrategy || '').trim().toLowerCase();
            const skipDescendants = strategy === 'none';

            if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
                if (!skipDescendants) markVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
                return;
            }

            // Mark ancestors as propagated (children→parent always happens)
            const pathParts = normalizedRowId.split('|||');
            for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
                markCell(pathParts.slice(0, depth).join('|||'), colId, 'propagated');
            }
            markCell(GRAND_TOTAL_ROW_ID, colId, 'propagated');
            // Mark visible descendants as propagated (skip for "none")
            if (!skipDescendants) markVisibleDescendants(normalizedRowId, colId);
        });

        if (directKeys.size === 0 && propagatedKeys.size === 0) return null;
        return {
            direct: Array.from(directKeys),
            propagated: Array.from(propagatedKeys).filter((key) => !directKeys.has(key)),
        };
    }, [getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveVisibleDataRowId]);

    const buildComparisonValuePlan = useCallback((updates) => {
        const cells = new Map();
        const addCell = (rowId, colId, originalValue = undefined) => {
            const key = getOptimisticCellKey(rowId, colId);
            if (!key) return;
            const existingCell = cells.get(key);
            const nextCell = {
                key,
                rowId: String(rowId),
                colId: String(colId),
            };
            if (originalValue !== undefined) {
                nextCell.originalValue = originalValue;
            } else if (existingCell && Object.prototype.hasOwnProperty.call(existingCell, 'originalValue')) {
                nextCell.originalValue = existingCell.originalValue;
            }
            cells.set(key, nextCell);
        };
        const addVisibleDescendants = (ancestorRowId, colId) => {
            const normalizedAncestorId = ancestorRowId === null || ancestorRowId === undefined ? '' : String(ancestorRowId);
            if (!normalizedAncestorId) return;
            const descendantPrefix = `${normalizedAncestorId}|||`;
            const visibleRows = Array.isArray(tableDataRef.current) ? tableDataRef.current : [];
            visibleRows.forEach((row) => {
                const rowId = resolveVisibleDataRowId(row);
                if (!rowId || rowId === normalizedAncestorId) return;
                if (normalizedAncestorId === GRAND_TOTAL_ROW_ID) {
                    if (rowId !== GRAND_TOTAL_ROW_ID) addCell(rowId, colId);
                    return;
                }
                if (rowId.startsWith(descendantPrefix)) addCell(rowId, colId);
            });
        };

        (Array.isArray(updates) ? updates : []).forEach((update) => {
            if (!update || typeof update !== 'object') return;
            const rowId = update.rowPath || update.rowId;
            const colId = update.colId;
            addCell(rowId, colId, update.oldValue);

            const aggregation = update.aggregation && typeof update.aggregation === 'object'
                ? update.aggregation
                : resolveAggregationConfigForColumnId(colId);
            const aggregationFn = aggregation && aggregation.agg
                ? String(aggregation.agg).trim().toLowerCase()
                : '';
            if (!aggregationFn || aggregation.windowFn) return;

            const normalizedRowId = rowId === null || rowId === undefined ? '' : String(rowId);
            if (!normalizedRowId) return;
            const strategy = String(update.propagationStrategy || '').trim().toLowerCase();
            const skipDescendants = strategy === 'none';

            if (normalizedRowId === GRAND_TOTAL_ROW_ID) {
                if (!skipDescendants) addVisibleDescendants(GRAND_TOTAL_ROW_ID, colId);
                return;
            }

            const pathParts = normalizedRowId.split('|||');
            for (let depth = pathParts.length - 1; depth >= 1; depth -= 1) {
                addCell(pathParts.slice(0, depth).join('|||'), colId);
            }
            addCell(GRAND_TOTAL_ROW_ID, colId);
            if (!skipDescendants) addVisibleDescendants(normalizedRowId, colId);
        });

        if (cells.size === 0) return null;
        return {
            cells: Array.from(cells.values()),
        };
    }, [getOptimisticCellKey, resolveAggregationConfigForColumnId, resolveVisibleDataRowId]);

    const applyComparisonValuePlan = useCallback((comparisonPlan, direction = 'forward') => {
        if (!comparisonPlan || typeof comparisonPlan !== 'object') return;
        const delta = direction === 'backward' ? -1 : 1;
        let didChange = false;

        (Array.isArray(comparisonPlan.cells) ? comparisonPlan.cells : []).forEach((cell) => {
            if (!cell || typeof cell !== 'object') return;
            const key = getOptimisticCellKey(cell.rowId, cell.colId);
            if (!key) return;
            const existingCount = comparisonCellCountsRef.current.get(key) || 0;
            if (delta > 0 && existingCount === 0) {
                comparisonCellOriginalValuesRef.current.set(key, {
                    rowId: String(cell.rowId),
                    colId: String(cell.colId),
                    value: Object.prototype.hasOwnProperty.call(cell, 'originalValue')
                        ? cell.originalValue
                        : resolveCurrentCellValue(cell.rowId, cell.colId),
                });
                didChange = true;
            }
            const nextCount = Math.max(0, existingCount + delta);
            if (nextCount === 0) {
                if (comparisonCellCountsRef.current.delete(key)) didChange = true;
                if (comparisonCellOriginalValuesRef.current.delete(key)) didChange = true;
                return;
            }
            if (nextCount !== existingCount) {
                comparisonCellCountsRef.current.set(key, nextCount);
                didChange = true;
            }
        });

        if (didChange) {
            syncComparisonValueState();
            bumpComparisonCellEpoch();
        }
    }, [bumpComparisonCellEpoch, getOptimisticCellKey, resolveCurrentCellValue, syncComparisonValueState]);

    const applyEditedCellMarkerPlan = useCallback((markerPlan, direction = 'forward') => {
        if (!markerPlan || typeof markerPlan !== 'object') return;
        const delta = direction === 'backward' ? -1 : 1;
        let didChange = false;
        const applyMarkerKeys = (keys, countField) => {
            (Array.isArray(keys) ? keys : []).forEach((key) => {
                if (!key) return;
                const existingEntry = editedCellMarkersRef.current.get(key) || { directCount: 0, propagatedCount: 0 };
                const nextCount = Math.max(0, (existingEntry[countField] || 0) + delta);
                const nextEntry = {
                    ...existingEntry,
                    [countField]: nextCount,
                };
                if ((nextEntry.directCount || 0) === 0 && (nextEntry.propagatedCount || 0) === 0) {
                    if (editedCellMarkersRef.current.delete(key)) {
                        didChange = true;
                    }
                    return;
                }
                editedCellMarkersRef.current.set(key, nextEntry);
                didChange = true;
            });
        };
        applyMarkerKeys(markerPlan.direct, 'directCount');
        applyMarkerKeys(markerPlan.propagated, 'propagatedCount');
        if (didChange) bumpEditedCellEpoch();
    }, [bumpEditedCellEpoch]);

    const reconcileOptimisticCellValuesWithPayload = useCallback((payload) => {
        const responseRows = Array.isArray(payload && payload.data) ? payload.data : [];
        if (responseRows.length === 0 || optimisticCellValuesRef.current.size === 0) return;

        const resolveResponseRowId = (row) => {
            if (!row || typeof row !== 'object') return null;
            if (row._isTotal || row._path === GRAND_TOTAL_ROW_ID || row._id === 'Grand Total') return GRAND_TOTAL_ROW_ID;
            if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
            if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
            if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
            return null;
        };

        const rowLookup = new Map();
        responseRows.forEach((row) => {
            const rowId = resolveResponseRowId(row);
            if (!rowId) return;
            rowLookup.set(rowId, row);
        });

        let didDelete = false;
        optimisticCellValuesRef.current.forEach((entry, key) => {
            if (!entry || typeof entry !== 'object') return;
            const row = rowLookup.get(entry.rowId);
            if (!row || !cellValuesMatch(entry.value, row[entry.colId])) return;
            optimisticCellValuesRef.current.delete(key);
            didDelete = true;
        });

        if (didDelete) bumpOptimisticCellEpoch();
    }, [bumpOptimisticCellEpoch]);

    const pushUnifiedHistoryEntry = useCallback((entry) => {
        if (!entry || typeof entry !== 'object') return;
        const nextUndoStack = [...transactionUndoStackRef.current, entry];
        if (nextUndoStack.length > MAX_TRANSACTION_HISTORY_ENTRIES) {
            nextUndoStack.splice(0, nextUndoStack.length - MAX_TRANSACTION_HISTORY_ENTRIES);
        }
        transactionUndoStackRef.current = nextUndoStack;
        transactionRedoStackRef.current = [];
        syncTransactionHistoryState();
    }, [syncTransactionHistoryState]);

    const clearPendingLayoutHistoryCapture = useCallback((token = null) => {
        const pendingCapture = pendingLayoutHistoryCaptureRef.current;
        if (!pendingCapture) return;
        if (token && pendingCapture.token !== token) return;
        pendingLayoutHistoryCaptureRef.current = null;
        if (pendingLayoutHistoryCaptureTimeoutRef.current !== null) {
            clearTimeout(pendingLayoutHistoryCaptureTimeoutRef.current);
            pendingLayoutHistoryCaptureTimeoutRef.current = null;
        }
    }, []);

    const requestLayoutHistoryCapture = useCallback((source = 'layout') => {
        const beforeSnapshot = layoutHistorySnapshotRef.current || layoutHistorySnapshot || {};
        const token = `${source}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}`;
        pendingLayoutHistoryCaptureRef.current = {
            token,
            source,
            beforeSnapshot,
            beforeKey: JSON.stringify(beforeSnapshot || {}),
        };
        if (pendingLayoutHistoryCaptureTimeoutRef.current !== null) {
            clearTimeout(pendingLayoutHistoryCaptureTimeoutRef.current);
        }
        pendingLayoutHistoryCaptureTimeoutRef.current = setTimeout(() => {
            clearPendingLayoutHistoryCapture(token);
        }, 500);
        return token;
    }, [clearPendingLayoutHistoryCapture, layoutHistorySnapshot]);

    const setRowFieldsWithHistory = useCallback((updater, source = 'layout:rows') => {
        requestLayoutHistoryCapture(source);
        setRowFields(updater);
    }, [requestLayoutHistoryCapture]);

    const setColFieldsWithHistory = useCallback((updater, source = 'layout:columns') => {
        requestLayoutHistoryCapture(source);
        setColFields(updater);
    }, [requestLayoutHistoryCapture]);

    const setValConfigsWithHistory = useCallback((updater, source = 'layout:values') => {
        requestLayoutHistoryCapture(source);
        setValConfigs(updater);
    }, [requestLayoutHistoryCapture]);

    const pushImmediateLayoutHistoryEntry = useCallback((sliceKey, updater, source) => {
        const beforeSnapshot = cloneSerializable(layoutHistorySnapshotRef.current || layoutHistorySnapshot || {}, null) || {};
        const previousSlice = beforeSnapshot[sliceKey];
        const nextSlice = typeof updater === 'function' ? updater(previousSlice) : updater;
        const afterSnapshot = cloneSerializable({
            ...beforeSnapshot,
            [sliceKey]: nextSlice,
        }, null) || {};
        const beforeKey = JSON.stringify(beforeSnapshot || {});
        const afterKey = JSON.stringify(afterSnapshot || {});
        clearPendingLayoutHistoryCapture();
        layoutHistorySnapshotRef.current = afterSnapshot;
        if (beforeKey !== afterKey) {
            pushUnifiedHistoryEntry({
                kind: 'layout',
                source,
                before: beforeSnapshot,
                after: afterSnapshot,
                createdAt: Date.now(),
            });
        }
        return nextSlice;
    }, [clearPendingLayoutHistoryCapture, layoutHistorySnapshot, pushUnifiedHistoryEntry]);

    const setExpandedWithHistory = useCallback((updater, source = 'layout:expanded') => {
        const nextExpanded = pushImmediateLayoutHistoryEntry('expanded', updater, source);
        setExpanded(nextExpanded);
    }, [pushImmediateLayoutHistoryEntry]);

    const setColExpandedWithHistory = useCallback((updater, source = 'layout:column-groups') => {
        const nextColExpanded = pushImmediateLayoutHistoryEntry('colExpanded', updater, source);
        setColExpanded(nextColExpanded);
    }, [pushImmediateLayoutHistoryEntry]);

    const toggleCol = useCallback((key, source = 'layout:column-groups') => {
        setColExpandedWithHistory((previousExpanded) => ({
            ...previousExpanded,
            [key]: previousExpanded[key] === undefined ? false : !previousExpanded[key],
        }), source);
    }, [setColExpandedWithHistory]);

    const setColumnPinningWithHistory = useCallback((updater, source = 'layout:column-pinning') => {
        requestLayoutHistoryCapture(source);
        setColumnPinning(updater);
    }, [requestLayoutHistoryCapture]);

    const setRowPinningWithHistory = useCallback((updater, source = 'layout:row-pinning') => {
        requestLayoutHistoryCapture(source);
        setRowPinning(updater);
    }, [requestLayoutHistoryCapture]);

    const applyLayoutHistorySnapshot = useCallback((snapshot) => {
        if (!snapshot || typeof snapshot !== 'object') return;
        setRowFields(Array.isArray(snapshot.rowFields) ? snapshot.rowFields : []);
        setColFields(Array.isArray(snapshot.colFields) ? snapshot.colFields : []);
        setValConfigs(Array.isArray(snapshot.valConfigs) ? snapshot.valConfigs : []);
        setFilters(snapshot.filters && typeof snapshot.filters === 'object' ? snapshot.filters : {});
        setSorting(Array.isArray(snapshot.sorting) ? snapshot.sorting : []);
        setExpanded(snapshot.expanded && typeof snapshot.expanded === 'object' ? snapshot.expanded : {});
        setColExpanded(snapshot.colExpanded && typeof snapshot.colExpanded === 'object' ? snapshot.colExpanded : {});
        setColumnPinning(
            snapshot.columnPinning && typeof snapshot.columnPinning === 'object'
                ? snapshot.columnPinning
                : { left: ['hierarchy'], right: [] }
        );
        setRowPinning(
            snapshot.rowPinning && typeof snapshot.rowPinning === 'object'
                ? snapshot.rowPinning
                : { top: [], bottom: [] }
        );
        setShowRowTotals(Boolean(snapshot.showRowTotals));
        setShowColTotals(Boolean(snapshot.showColTotals));
        setGrandTotalPinOverride(
            snapshot.grandTotalPinOverride === 'top' || snapshot.grandTotalPinOverride === 'bottom'
                ? snapshot.grandTotalPinOverride
                : false
        );
    }, []);

    const clearTransactionHistory = useCallback(() => {
        transactionUndoStackRef.current = [];
        transactionRedoStackRef.current = [];
        pendingTransactionHistoryRef.current.clear();
        transactionHistoryPendingRequestRef.current = null;
        editSessionStateRef.current = {
            sessionId: sessionIdRef.current,
            clientInstance: clientInstanceRef.current,
            sessionKey: null,
            sessionVersion: 0,
        };
        clearAllOptimisticCellValues();
        clearAllEditedCellMarkers();
        clearAllComparisonValueState();
        clearPendingLayoutHistoryCapture();
        syncTransactionHistoryState();
    }, [clearAllComparisonValueState, clearAllEditedCellMarkers, clearAllOptimisticCellValues, clearPendingLayoutHistoryCapture, syncTransactionHistoryState]);

    const didMountClearTransactionRef = useRef(false);
    const prevServerSideRef = useRef(serverSide);
    const prevTableNameRef = useRef(tableName);
    useEffect(() => {
        if (!didMountClearTransactionRef.current) {
            didMountClearTransactionRef.current = true;
            return;
        }
        if (prevServerSideRef.current !== serverSide || prevTableNameRef.current !== tableName) {
            prevServerSideRef.current = serverSide;
            prevTableNameRef.current = tableName;
            console.log('[CLEAR-TX] clearing due to serverSide/tableName change');
            clearTransactionHistory();
        }
    }, [clearTransactionHistory, serverSide, tableName]);

    useEffect(() => {
        if (!viewState || typeof viewState !== 'object') return;
        const restored = (viewState.state && typeof viewState.state === 'object')
            ? viewState.state
            : viewState;
        const serializedState = restored.editComparisonState && typeof restored.editComparisonState === 'object'
            ? restored.editComparisonState
            : null;
        const restoreKey = JSON.stringify(serializedState);
        if (lastRestoredViewEditComparisonKeyRef.current === restoreKey) return;
        lastRestoredViewEditComparisonKeyRef.current = restoreKey;
        restoreSerializedEditComparisonState(serializedState);
        didAttemptEditComparisonRestoreRef.current = true;
    }, [restoreSerializedEditComparisonState, viewState]);

    useEffect(() => {
        if (!persistence || didRestorePersistedEditComparisonRef.current) return;
        if (viewState && typeof viewState === 'object') return;
        didRestorePersistedEditComparisonRef.current = true;
        restoreSerializedEditComparisonState(loadPersistedState('editComparisonState', null));
        const persistedDisplayMode = loadPersistedState('editValueDisplayMode', 'original');
        setEditValueDisplayMode(persistedDisplayMode === 'edited' ? 'edited' : 'original');
        didAttemptEditComparisonRestoreRef.current = true;
    }, [loadPersistedState, persistence, restoreSerializedEditComparisonState, viewState]);

    useEffect(() => {
        if (viewState && typeof viewState === 'object') return;
        if (!persistence) {
            didAttemptEditComparisonRestoreRef.current = true;
            return;
        }
        if (didRestorePersistedEditComparisonRef.current) {
            didAttemptEditComparisonRestoreRef.current = true;
        }
    }, [persistence, viewState]);

    useEffect(() => {
        if (comparisonValueState.activeCount === 0 && editValueDisplayMode !== 'original') {
            setEditValueDisplayMode('original');
        }
    }, [comparisonValueState.activeCount, editValueDisplayMode]);

    useEffect(() => {
        layoutHistorySnapshotRef.current = layoutHistorySnapshot;
        const pendingCapture = pendingLayoutHistoryCaptureRef.current;
        if (!pendingCapture) return;
        if (layoutHistorySnapshotKey === pendingCapture.beforeKey) return;
        clearPendingLayoutHistoryCapture(pendingCapture.token);
        pushUnifiedHistoryEntry({
            kind: 'layout',
            source: pendingCapture.source || 'layout',
            before: pendingCapture.beforeSnapshot,
            after: layoutHistorySnapshot,
            createdAt: Date.now(),
        });
    }, [
        clearPendingLayoutHistoryCapture,
        layoutHistorySnapshot,
        layoutHistorySnapshotKey,
        pushUnifiedHistoryEntry,
    ]);

    useEffect(() => () => {
        if (pendingLayoutHistoryCaptureTimeoutRef.current !== null) {
            clearTimeout(pendingLayoutHistoryCaptureTimeoutRef.current);
            pendingLayoutHistoryCaptureTimeoutRef.current = null;
        }
    }, []);





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

    const recordProfilerQueuedRequest = useCallback((requestMeta) => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !requestMeta || !requestMeta.requestId) return;
        profiler.queue({
            requestId: requestMeta.requestId,
            componentId: profilerComponentIdRef.current,
            kind: 'data',
            queuedAt: requestMeta.queuedAt,
            emittedAt: requestMeta.emittedAt,
            status: 'queued',
            meta: {
                table: tableName || null,
                viewMode,
                reqStart: requestMeta.reqStart,
                reqEnd: requestMeta.reqEnd,
                colStart: requestMeta.colStart,
                colEnd: requestMeta.colEnd,
                stateEpoch: requestMeta.stateEpoch,
                abortGeneration: requestMeta.abortGeneration,
                windowSeq: requestMeta.version,
                visibleColumnCount: requestMeta.visibleColumnCount,
                requestedColumnCount: requestMeta.requestedColumnCount,
                columnDeltaCount: requestMeta.columnDeltaCount,
            },
        });
    }, [tableName, viewMode]);

    const recordProfilerRequestStart = useCallback((runtimeRequest) => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !runtimeRequest || !runtimeRequest.requestId) return;
        profiler.begin({
            requestId: runtimeRequest.requestId,
            componentId: profilerComponentIdRef.current,
            kind: runtimeRequest.kind || 'data',
            emittedAt: Date.now(),
            status: 'sent',
            meta: {
                table: runtimeRequest.table || tableName || null,
                viewMode,
                detailMode,
                windowSeq: runtimeRequest.window_seq,
                stateEpoch: runtimeRequest.state_epoch,
                abortGeneration: runtimeRequest.abort_generation,
            },
        });
    }, [detailMode, tableName, viewMode]);

    const recordProfilerResponse = useCallback((response) => {
        if (!profilingEnabledRef.current) return;
        const profiler = pivotProfilerRef.current;
        if (!profiler || !response || typeof response !== 'object' || !response.requestId) return;
        const payload = response.payload && typeof response.payload === 'object'
            ? response.payload
            : {};
        profiler.response({
            requestId: response.requestId,
            componentId: profilerComponentIdRef.current,
            kind: response.kind || 'data',
            status: response.status || null,
            responseReceivedAt: Date.now(),
            profile: response.profile,
            meta: {
                table: response.table || tableName || null,
                stateEpoch: response.state_epoch,
                windowSeq: response.window_seq,
                rowCount: payload.rowCount,
                dataVersion: payload.dataVersion,
            },
        });
    }, [tableName]);

    const normalizeOutgoingRuntimeRequest = useCallback((runtimeRequest) => {
        if (!runtimeRequest || typeof runtimeRequest !== 'object') return null;

        const kind = typeof runtimeRequest.kind === 'string' && runtimeRequest.kind.trim()
            ? runtimeRequest.kind.trim().toLowerCase()
            : 'data';
        const payload = runtimeRequest.payload && typeof runtimeRequest.payload === 'object'
            ? runtimeRequest.payload
            : runtimeRequest;
        const requestId = String(
            runtimeRequest.requestId
            || runtimeRequest.request_id
            || payload.requestId
            || payload.request_id
            || payload.request_signature
            || payload.version
            || payload.window_seq
            || payload.nonce
            || `${kind}:${Date.now()}`
        );

        return {
            ...runtimeRequest,
            kind,
            requestId,
            table: runtimeRequest.table || payload.table || tableName || undefined,
            session_id: runtimeRequest.session_id || payload.session_id || sessionIdRef.current,
            client_instance: runtimeRequest.client_instance || payload.client_instance || clientInstanceRef.current,
            state_epoch: runtimeRequest.state_epoch !== undefined ? runtimeRequest.state_epoch : payload.state_epoch,
            window_seq: runtimeRequest.window_seq !== undefined
                ? runtimeRequest.window_seq
                : (payload.window_seq !== undefined ? payload.window_seq : payload.version),
            abort_generation: runtimeRequest.abort_generation !== undefined
                ? runtimeRequest.abort_generation
                : payload.abort_generation,
            payload: cloneSerializable({
                ...payload,
                viewMode: payload.viewMode || viewMode,
                detailMode: payload.detailMode || detailMode,
                treeConfig: payload.treeConfig !== undefined ? payload.treeConfig : treeConfig,
                detailConfig: payload.detailConfig !== undefined ? payload.detailConfig : detailConfig,
                profile: payload.profile !== undefined ? payload.profile : (profilingEnabledRef.current || undefined),
            }, payload),
        };
    }, [detailConfig, detailMode, tableName, treeConfig, viewMode]);

    const dispatchSetProps = useCallback((nextProps) => {
        if (typeof setProps !== 'function') return;
        if (!nextProps || typeof nextProps !== 'object') {
            setProps(nextProps);
            return;
        }
        const runtimeRequest = normalizeOutgoingRuntimeRequest(nextProps.runtimeRequest);
        if (runtimeRequest) {
            recordProfilerRequestStart(runtimeRequest);
            setProps({
                ...nextProps,
                runtimeRequest,
            });
            return;
        }
        setProps(nextProps);
    }, [normalizeOutgoingRuntimeRequest, recordProfilerRequestStart, setProps]);

    const setPropsRef = useRef(dispatchSetProps);
    useEffect(() => {
        setPropsRef.current = dispatchSetProps;
    }, [dispatchSetProps]);

    const emitRuntimeRequest = useCallback((kind, payload) => {
        if (!setPropsRef.current || !payload || typeof payload !== 'object') return;
        setPropsRef.current({
            runtimeRequest: {
                kind,
                payload,
            },
        });
    }, []);

    const requestEditorOptions = useCallback((editorConfig, columnId, columnConfig = null) => {
        const source = resolveEditorOptionsSource(editorConfig, columnId, columnConfig);
        if (!source || !source.columnId) return;
        if (Object.prototype.hasOwnProperty.call(filterOptions || {}, source.columnId)) return;
        if (editorOptionsLoadingRef.current[source.columnId]) return;
        setEditorOptionsLoadingState((previousState) => ({
            ...previousState,
            [source.columnId]: true,
        }));
        emitEditLifecycleEvent({
            kind: 'editor_options_request',
            columnId,
            sourceColumnId: source.columnId,
            editor: editorConfig && editorConfig.editor ? editorConfig.editor : null,
        });
        emitRuntimeRequest('filter_options', { columnId: source.columnId, nonce: Date.now() });
    }, [emitEditLifecycleEvent, emitRuntimeRequest, filterOptions]);

    // --- Detail/drill-through extracted to useDetailDrillThrough hook ---
    const {
        detailSurface,
        setDetailSurface,
        toggleDetailForRowRef,
        pendingDetailRequestRef,
        activeDetailDisplayMode,
        fetchDetailData,
        handleCellDrillThrough,
        toggleDetailForRow,
        handleDetailPageChange,
        handleDetailSort,
        handleDetailFilter,
        handleDetailClose,
    } = useDetailDrillThrough({
        detailConfig,
        detailMode,
        viewMode,
        reportDef,
        treeConfig,
        rowFields,
        colFields,
        valConfigs,
        tableName,
        runtimeResponse,
        emitRuntimeRequest,
        stateEpoch,
        abortGeneration,
        sessionIdRef,
        clientInstanceRef,
        requestVersionRef,
        setPropsRef,
        profilingEnabledRef,
        pivotProfilerRef,
        profilerComponentIdRef,
    });

    const supportsPatchTransactionRefresh = useMemo(() => {
        if (!serverSide || viewMode !== 'pivot' || detailMode !== 'none') return false;
        if (!Array.isArray(rowFields) || rowFields.length === 0) return false;
        return !(Array.isArray(valConfigs) && valConfigs.some((config) => {
            if (!config || typeof config !== 'object') return false;
            const aggregation = String(config.agg || '').trim().toLowerCase();
            return aggregation === 'formula' || Boolean(config.windowFn);
        }));
    }, [detailMode, rowFields, serverSide, valConfigs, viewMode]);

    const dispatchTransactionRequest = useCallback((transactionPayload, source = 'transaction') => {
        let normalizedTransaction = normalizeTransactionHistoryPayload(transactionPayload, { source });
        if (!normalizedTransaction || !setPropsRef.current || !serverSide) return null;
        if (transactionHistoryPendingRequestRef.current) {
            showNotification('Wait for the current edit-history action to finish before applying another edit.', 'warning');
            return null;
        }
        const requestedRefreshMode = normalizeTransactionRefreshModeValue(normalizedTransaction.refreshMode, 'smart');
        const canUsePatchRefresh = (
            requestedRefreshMode === 'patch'
            && visiblePatchRowPathsRef.current.length > 0
            && (!colFields.length || visiblePatchCenterIdsRef.current.length > 0)
        );
        if (requestedRefreshMode === 'patch' && !canUsePatchRefresh) {
            normalizedTransaction = {
                ...normalizedTransaction,
                refreshMode: 'viewport',
            };
        }
        const showGlobalLoading = shouldShowTransactionLoading(normalizedTransaction);

        const viewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || { start: 0, end: 99, count: 100 };
        const nextStart = Math.max(0, Number.isFinite(Number(viewportSnapshot.start)) ? Number(viewportSnapshot.start) : 0);
        const nextEndCandidate = Number.isFinite(Number(viewportSnapshot.end))
            ? Number(viewportSnapshot.end)
            : (nextStart + Math.max(1, Number(viewportSnapshot.count) || 100) - 1);
        const nextEnd = Math.max(nextStart, nextEndCandidate);
        const nextCount = Math.max(1, nextEnd - nextStart + 1);
        const updateColumnWindow = resolveStableRequestedColumnWindow();
        const tx = beginExpansionRequest();
        const requestId = `${source}:${tx.version}`;
        markRequestPending({
            version: tx.version,
            reqStart: nextStart,
            reqEnd: nextEnd,
            colStart: updateColumnWindow.start,
            colEnd: updateColumnWindow.end,
            silent: !showGlobalLoading,
        });
        emitRuntimeRequest('transaction', {
            table: tableName || undefined,
            start: nextStart,
            end: nextEnd,
            count: nextCount,
            window_seq: tx.version,
            version: tx.version,
            requestId,
            state_epoch: tx.stateEpoch,
            session_id: sessionIdRef.current,
            client_instance: clientInstanceRef.current,
            abort_generation: tx.abortGeneration,
            intent: 'viewport',
            col_start: updateColumnWindow.start !== null ? updateColumnWindow.start : undefined,
            col_end: updateColumnWindow.end !== null ? updateColumnWindow.end : undefined,
            include_grand_total: serverSidePinsGrandTotal || undefined,
            immersive_mode: immersiveMode || undefined,
            ...normalizedTransaction,
            ...(normalizedTransaction.refreshMode === 'patch' ? {
                visibleRowPaths: visiblePatchRowPathsRef.current,
                visibleCenterColumnIds: visiblePatchCenterIdsRef.current,
            } : {}),
        });
        return { requestId };
    }, [beginExpansionRequest, immersiveMode, colFields.length, emitRuntimeRequest, markRequestPending, resolveStableRequestedColumnWindow, serverSide, serverSidePinsGrandTotal, showNotification, tableName]);

    const finalizeTransactionHistoryResponse = useCallback((response, payload) => {
        if (!response || typeof response !== 'object') return;
        const requestId = response.requestId || null;
        if (!requestId) return;
        const pendingEntry = pendingTransactionHistoryRef.current.get(requestId);
        if (!pendingEntry) return;
        pendingTransactionHistoryRef.current.delete(requestId);

        const transactionResult = payload && typeof payload === 'object' && payload.transaction && typeof payload.transaction === 'object'
            ? payload.transaction
            : {};
        const didApplyWork = hasAppliedTransactionWork(transactionResult);
        const transactionWarnings = Array.isArray(transactionResult.warnings) ? transactionResult.warnings : [];
        const propagationEntries = Array.isArray(transactionResult.propagation) ? transactionResult.propagation : [];
        const responseAffectedCells = transactionResult.affectedCells && typeof transactionResult.affectedCells === 'object'
            ? transactionResult.affectedCells
            : null;
        const responseEditSession = transactionResult.editSession && typeof transactionResult.editSession === 'object'
            ? transactionResult.editSession
            : null;
        if (responseEditSession) {
            editSessionStateRef.current = {
                sessionId: responseEditSession.sessionId || sessionIdRef.current,
                clientInstance: responseEditSession.clientInstance || clientInstanceRef.current,
                sessionKey: responseEditSession.sessionKey || null,
                sessionVersion: Number(responseEditSession.sessionVersion) || 0,
            };
        }

        const resolveTargetHistoryEntries = () => {
            const explicitEntries = Array.isArray(pendingEntry.historyEntries)
                ? pendingEntry.historyEntries.filter(Boolean)
                : [];
            if (explicitEntries.length > 0) return explicitEntries;
            const targetEventIds = Array.isArray(pendingEntry.targetEventIds) ? pendingEntry.targetEventIds : [];
            return targetEventIds
                .map((eventId) => findTransactionHistoryEntryByEventId(eventId))
                .filter(Boolean);
        };

        const rollbackHistoryEntries = (historyEntries) => {
            (Array.isArray(historyEntries) ? historyEntries : []).forEach((entry) => {
                if (!entry || typeof entry !== 'object') return;
                if (entry.editedMarkerPlan) {
                    applyEditedCellMarkerPlan(entry.editedMarkerPlan, 'backward');
                }
                if (entry.comparisonPlan) {
                    applyComparisonValuePlan(entry.comparisonPlan, 'backward');
                }
                if (entry.eventId) {
                    detachTrackedEventOwnership([entry.eventId]);
                }
            });
        };

        const pushCapturedHistoryEntry = (entry) => {
            if (!entry) return;
            pushUnifiedHistoryEntry(entry);
            if (entry.eventId && entry.affectedCells) {
                appendTrackedEventOwnership(entry.eventId, entry.affectedCells);
            }
        };

        if (pendingEntry.action === 'apply') {
            const captureable = Boolean(transactionResult.history && transactionResult.history.captureable);
            const undoTransaction = normalizeTransactionHistoryPayload(transactionResult.inverseTransaction);
            const redoTransaction = normalizeTransactionHistoryPayload(transactionResult.redoTransaction);
            if (response.status === 'data') releaseOptimisticCellRequest(requestId);
            if (!didApplyWork && pendingEntry.editedMarkerPlan) {
                applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'backward');
            }
            if (!didApplyWork && pendingEntry.comparisonPlan) {
                applyComparisonValuePlan(pendingEntry.comparisonPlan, 'backward');
            }
            if (didApplyWork && captureable && undoTransaction && redoTransaction) {
                pushCapturedHistoryEntry({
                    kind: 'transaction',
                    undoTransaction,
                    redoTransaction,
                    editedMarkerPlan: pendingEntry.editedMarkerPlan || null,
                    comparisonPlan: pendingEntry.comparisonPlan || null,
                    source: pendingEntry.source || transactionResult.source || 'transaction',
                    createdAt: Date.now(),
                    eventId: transactionResult.eventId || null,
                    sessionVersion: Number(transactionResult.sessionVersion) || 0,
                    affectedCells: responseAffectedCells,
                    impactedScopeIds: Array.isArray(transactionResult.impactedScopeIds) ? transactionResult.impactedScopeIds : [],
                });
            } else if (didApplyWork && transactionResult.history && Array.isArray(transactionResult.history.warnings) && transactionResult.history.warnings[0]) {
                showNotification(transactionResult.history.warnings[0], 'warning');
            } else if (!didApplyWork && transactionWarnings[0]) {
                showNotification(transactionWarnings[0], 'warning');
            }
            if (didApplyWork && propagationEntries.length > 0) {
                propagationEntries.forEach((entry) => {
                    if (!entry || typeof entry !== 'object') return;
                    propagationLogRef.current = [
                        ...propagationLogRef.current.slice(-49),
                        { ...entry, timestamp: Date.now() },
                    ];
                });
                setPropagationLogEpoch((prev) => prev + 1);
                setEditPanelOpen(true);
            }
            if (pendingEntry.rowEditRowId) {
                if (didApplyWork) {
                    closeRowEditSession(pendingEntry.rowEditRowId);
                    emitEditLifecycleEvent({
                        kind: 'row_edit_commit_success',
                        rowId: pendingEntry.rowEditRowId,
                        dirtyColumns: pendingEntry.dirtyColumns || [],
                    });
                } else {
                    updateRowEditSessions((previousSessions) => {
                        const previousSession = previousSessions[pendingEntry.rowEditRowId];
                        if (!previousSession) return previousSessions;
                        return {
                            ...previousSessions,
                            [pendingEntry.rowEditRowId]: {
                                ...previousSession,
                                status: 'editing',
                            },
                        };
                    });
                    emitEditLifecycleEvent({
                        kind: 'row_edit_commit_error',
                        rowId: pendingEntry.rowEditRowId,
                        message: transactionWarnings[0] || 'The row edit did not apply.',
                    });
                }
            }
            return;
        }

        if (pendingEntry.action === 'undo') {
            if (response.status === 'data') releaseOptimisticCellRequest(requestId);
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.editedMarkerPlan) {
                applyEditedCellMarkerPlan(pendingEntry.entry.editedMarkerPlan, 'backward');
            }
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.comparisonPlan) {
                applyComparisonValuePlan(pendingEntry.entry.comparisonPlan, 'backward');
            }
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.eventId) {
                detachTrackedEventOwnership([pendingEntry.entry.eventId]);
            }
            if (didApplyWork) {
                transactionUndoStackRef.current = transactionUndoStackRef.current.slice(0, -1);
                transactionRedoStackRef.current = [...transactionRedoStackRef.current, pendingEntry.entry];
            }
            transactionHistoryPendingRequestRef.current = null;
            syncTransactionHistoryState();
            if (didApplyWork) {
                showNotification('Undid the last transaction.', 'success');
            } else if (transactionWarnings[0]) {
                showNotification(transactionWarnings[0], 'warning');
            }
            return;
        }

        if (pendingEntry.action === 'redo') {
            if (response.status === 'data') releaseOptimisticCellRequest(requestId);
            if (!didApplyWork && pendingEntry.entry && pendingEntry.entry.comparisonPlan) {
                applyComparisonValuePlan(pendingEntry.entry.comparisonPlan, 'backward');
            }
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.editedMarkerPlan) {
                applyEditedCellMarkerPlan(pendingEntry.entry.editedMarkerPlan, 'forward');
            }
            if (didApplyWork && pendingEntry.entry && pendingEntry.entry.eventId && pendingEntry.entry.affectedCells) {
                appendTrackedEventOwnership(pendingEntry.entry.eventId, pendingEntry.entry.affectedCells);
            }
            if (didApplyWork) {
                transactionRedoStackRef.current = transactionRedoStackRef.current.slice(0, -1);
                transactionUndoStackRef.current = [...transactionUndoStackRef.current, pendingEntry.entry];
            }
            transactionHistoryPendingRequestRef.current = null;
            syncTransactionHistoryState();
            if (didApplyWork) {
                showNotification('Reapplied the last transaction.', 'success');
            } else if (transactionWarnings[0]) {
                showNotification(transactionWarnings[0], 'warning');
            }
            return;
        }

        if (pendingEntry.action === 'revert') {
            if (response.status === 'data') releaseOptimisticCellRequest(requestId);
            if (didApplyWork) {
                const targetEntries = resolveTargetHistoryEntries();
                if (targetEntries.length > 0) {
                    rollbackHistoryEntries(targetEntries);
                } else if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0) {
                    detachTrackedEventOwnership(pendingEntry.targetEventIds, { adjustCounts: true });
                }
                if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0) {
                    pruneTransactionHistoryEntries(pendingEntry.targetEventIds);
                }
                if (pendingEntry.clearAllOnSuccess) {
                    clearAllOptimisticCellValues();
                    clearAllEditedCellMarkers();
                    clearAllComparisonValueState();
                    propagationLogRef.current = [];
                    setPropagationLogEpoch((previousEpoch) => previousEpoch + 1);
                }
                showNotification(pendingEntry.successMessage || 'Reverted the selected edits.', 'success');
            } else if (transactionWarnings[0]) {
                showNotification(transactionWarnings[0], 'warning');
            }
            transactionHistoryPendingRequestRef.current = null;
            syncTransactionHistoryState();
            return;
        }

        if (pendingEntry.action === 'replace') {
            if (response.status === 'data') releaseOptimisticCellRequest(requestId);
            if (didApplyWork) {
                const targetEntries = resolveTargetHistoryEntries();
                if (targetEntries.length > 0) {
                    rollbackHistoryEntries(targetEntries);
                } else if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0) {
                    detachTrackedEventOwnership(pendingEntry.targetEventIds, { adjustCounts: true });
                }
                if (Array.isArray(pendingEntry.targetEventIds) && pendingEntry.targetEventIds.length > 0) {
                    pruneTransactionHistoryEntries(pendingEntry.targetEventIds);
                }
                if (pendingEntry.comparisonPlan) {
                    applyComparisonValuePlan(pendingEntry.comparisonPlan, 'forward');
                }
                if (pendingEntry.editedMarkerPlan) {
                    applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'forward');
                }
                const captureable = Boolean(transactionResult.history && transactionResult.history.captureable);
                const undoTransaction = normalizeTransactionHistoryPayload(transactionResult.inverseTransaction);
                const redoTransaction = normalizeTransactionHistoryPayload(transactionResult.redoTransaction);
                if (captureable && undoTransaction && redoTransaction) {
                    pushCapturedHistoryEntry({
                        kind: 'transaction',
                        undoTransaction,
                        redoTransaction,
                        editedMarkerPlan: pendingEntry.editedMarkerPlan || null,
                        comparisonPlan: pendingEntry.comparisonPlan || null,
                        source: pendingEntry.source || transactionResult.source || 'replace',
                        createdAt: Date.now(),
                        eventId: transactionResult.eventId || null,
                        sessionVersion: Number(transactionResult.sessionVersion) || 0,
                        affectedCells: responseAffectedCells,
                        impactedScopeIds: Array.isArray(transactionResult.impactedScopeIds) ? transactionResult.impactedScopeIds : [],
                    });
                }
                if (pendingEntry.rowEditRowId) {
                    closeRowEditSession(pendingEntry.rowEditRowId);
                    emitEditLifecycleEvent({
                        kind: 'row_edit_commit_success',
                        rowId: pendingEntry.rowEditRowId,
                        dirtyColumns: pendingEntry.dirtyColumns || [],
                    });
                }
                showNotification(pendingEntry.successMessage || 'Updated the selected edit.', 'success');
            } else if (transactionWarnings[0]) {
                if (pendingEntry.rowEditRowId) {
                    updateRowEditSessions((previousSessions) => {
                        const previousSession = previousSessions[pendingEntry.rowEditRowId];
                        if (!previousSession) return previousSessions;
                        return {
                            ...previousSessions,
                            [pendingEntry.rowEditRowId]: {
                                ...previousSession,
                                status: 'editing',
                            },
                        };
                    });
                    emitEditLifecycleEvent({
                        kind: 'row_edit_commit_error',
                        rowId: pendingEntry.rowEditRowId,
                        message: transactionWarnings[0] || 'The row edit did not apply.',
                    });
                }
                showNotification(transactionWarnings[0], 'warning');
            } else if (pendingEntry.rowEditRowId) {
                updateRowEditSessions((previousSessions) => {
                    const previousSession = previousSessions[pendingEntry.rowEditRowId];
                    if (!previousSession) return previousSessions;
                    return {
                        ...previousSessions,
                        [pendingEntry.rowEditRowId]: {
                            ...previousSession,
                            status: 'editing',
                        },
                    };
                });
                emitEditLifecycleEvent({
                    kind: 'row_edit_commit_error',
                    rowId: pendingEntry.rowEditRowId,
                    message: 'The row edit did not apply.',
                });
            }
            transactionHistoryPendingRequestRef.current = null;
            syncTransactionHistoryState();
        }
    }, [
        appendTrackedEventOwnership,
        applyComparisonValuePlan,
        applyEditedCellMarkerPlan,
        clearAllComparisonValueState,
        clearAllEditedCellMarkers,
        clearAllOptimisticCellValues,
        closeRowEditSession,
        detachTrackedEventOwnership,
        emitEditLifecycleEvent,
        findTransactionHistoryEntryByEventId,
        pruneTransactionHistoryEntries,
        pushUnifiedHistoryEntry,
        releaseOptimisticCellRequest,
        showNotification,
        syncTransactionHistoryState,
        updateRowEditSessions,
    ]);

    const clearPendingTransactionHistoryRequest = useCallback((response, notifyOnError = false) => {
        if (!response || typeof response !== 'object') return;
        const requestId = response.requestId || null;
        if (!requestId) return;
        const pendingEntry = pendingTransactionHistoryRef.current.get(requestId);
        if (!pendingEntry) return;
        pendingTransactionHistoryRef.current.delete(requestId);
        clearOptimisticCellValuesForRequest(requestId);
        if (pendingEntry.action === 'apply' && pendingEntry.editedMarkerPlan) {
            applyEditedCellMarkerPlan(pendingEntry.editedMarkerPlan, 'backward');
        }
        if ((pendingEntry.action === 'apply' || pendingEntry.action === 'redo') && pendingEntry.comparisonPlan) {
            applyComparisonValuePlan(pendingEntry.comparisonPlan, 'backward');
        }
        if ((pendingEntry.action === 'apply' || pendingEntry.action === 'replace') && pendingEntry.rowEditRowId) {
            updateRowEditSessions((previousSessions) => {
                const previousSession = previousSessions[pendingEntry.rowEditRowId];
                if (!previousSession) return previousSessions;
                return {
                    ...previousSessions,
                    [pendingEntry.rowEditRowId]: {
                        ...previousSession,
                        status: 'editing',
                    },
                };
            });
            emitEditLifecycleEvent({
                kind: 'row_edit_commit_error',
                rowId: pendingEntry.rowEditRowId,
                message: response.message || 'Unable to commit the row edit.',
            });
        }
        if (pendingEntry.action === 'undo' || pendingEntry.action === 'redo' || pendingEntry.action === 'revert' || pendingEntry.action === 'replace') {
            transactionHistoryPendingRequestRef.current = null;
            syncTransactionHistoryState();
            if (notifyOnError && response.status === 'error') {
                showNotification(`Unable to ${pendingEntry.action} the transaction.`, 'error');
            }
        }
    }, [applyComparisonValuePlan, applyEditedCellMarkerPlan, clearOptimisticCellValuesForRequest, emitEditLifecycleEvent, showNotification, syncTransactionHistoryState, updateRowEditSessions]);

    const resolvePropagationFormulaUpdates = useCallback((updates, source, meta) => {
        const normalizedUpdates = Array.isArray(updates) ? updates : [];
        const needsPropagation = normalizedUpdates.some((u) => isParentAggregatePropagationEdit(u, rowFields));
        if (!needsPropagation) return normalizedUpdates;
        // Block new propagation edits while one is already pending
        if (pendingPropagationUpdatesRef.current) return null;
        // Stash updates and open the panel for method selection instead of window.prompt
        pendingPropagationUpdatesRef.current = { updates: normalizedUpdates, source: source || 'batch', meta: meta || null };
        setPendingPropagationUpdates(normalizedUpdates);
        setPropagationMethodUI(lastPropagationFormulaRef.current || 'equal');
        setEditPanelOpen(true);
        return null; // Signal caller to abort — will resume after user picks method
    }, [rowFields]);

    const analyzeServerEditOwnership = useCallback((updates) => {
        const replacementEventIds = [];
        for (const update of (Array.isArray(updates) ? updates : [])) {
            if (!update || typeof update !== 'object') continue;
            const rowId = update.rowPath || update.rowId;
            const colId = update.colId;
            if (rowId === null || rowId === undefined || colId === null || colId === undefined) continue;
            const ownershipState = resolveCellEditOwnershipState(String(rowId), String(colId));
            if (ownershipState.mode === 'blocked') {
                return {
                    mode: 'blocked',
                    targetEventIds: [],
                    reason: ownershipState.reason || 'This cell cannot be edited while an overlapping edit is active.',
                };
            }
            if (ownershipState.mode === 'replace') {
                replacementEventIds.push(...ownershipState.targetEventIds);
            }
        }
        const targetEventIds = sortEventIdsByActiveHistory(replacementEventIds);
        return {
            mode: targetEventIds.length > 0 ? 'replace' : 'apply',
            targetEventIds,
            reason: null,
        };
    }, [resolveCellEditOwnershipState, sortEventIdsByActiveHistory]);

    const dispatchServerEditUpdates = useCallback((updates, source = 'batch', meta = null) => {
        const ownershipState = analyzeServerEditOwnership(updates);
        if (ownershipState.mode === 'blocked') {
            showNotification(ownershipState.reason || 'This cell cannot be edited while an overlapping edit is active.', 'warning');
            return null;
        }
        const isReplacement = ownershipState.mode === 'replace';
        const effectiveUpdates = isReplacement
            ? updates.map((update) => {
                if (!update || typeof update !== 'object') return update;
                const rowId = update.rowPath || update.rowId;
                const colId = update.colId;
                const key = getOptimisticCellKey(rowId, colId);
                const originalEntry = key ? comparisonCellOriginalValuesRef.current.get(key) : null;
                if (!originalEntry || !Object.prototype.hasOwnProperty.call(originalEntry, 'value')) {
                    return update;
                }
                return {
                    ...update,
                    oldValue: originalEntry.value,
                };
            })
            : updates;
        const comparisonPlan = buildComparisonValuePlan(effectiveUpdates);
        if (!isReplacement && comparisonPlan) {
            applyComparisonValuePlan(comparisonPlan, 'forward');
        }
        const editedMarkerPlan = buildEditedCellMarkerPlan(effectiveUpdates);
        const dispatched = dispatchTransactionRequest(isReplacement ? {
            eventAction: 'replace',
            eventIds: ownershipState.targetEventIds,
            update: effectiveUpdates,
            refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
        } : {
            update: effectiveUpdates,
            refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
        }, source);
        if (!dispatched || !dispatched.requestId) {
            if (!isReplacement && comparisonPlan) applyComparisonValuePlan(comparisonPlan, 'backward');
            return null;
        }
        if (!isReplacement) {
            captureOptimisticCellValues(effectiveUpdates, dispatched.requestId);
            if (editedMarkerPlan) {
                applyEditedCellMarkerPlan(editedMarkerPlan, 'forward');
            }
            pendingTransactionHistoryRef.current.set(dispatched.requestId, {
                action: 'apply',
                source,
                editedMarkerPlan,
                comparisonPlan,
                rowEditRowId: meta && meta.rowEditRowId ? String(meta.rowEditRowId) : null,
                dirtyColumns: meta && Array.isArray(meta.dirtyColumns) ? meta.dirtyColumns : [],
            });
            return dispatched;
        }
        pendingTransactionHistoryRef.current.set(dispatched.requestId, {
            action: 'replace',
            source,
            targetEventIds: ownershipState.targetEventIds,
            historyEntries: ownershipState.targetEventIds.map((eventId) => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
            comparisonPlan,
            editedMarkerPlan,
            rowEditRowId: meta && meta.rowEditRowId ? String(meta.rowEditRowId) : null,
            dirtyColumns: meta && Array.isArray(meta.dirtyColumns) ? meta.dirtyColumns : [],
        });
        transactionHistoryPendingRequestRef.current = dispatched.requestId;
        syncTransactionHistoryState();
        return dispatched;
    }, [
        analyzeServerEditOwnership,
        applyComparisonValuePlan,
        applyEditedCellMarkerPlan,
        buildComparisonValuePlan,
        buildEditedCellMarkerPlan,
        captureOptimisticCellValues,
        dispatchTransactionRequest,
        findTransactionHistoryEntryByEventId,
        getOptimisticCellKey,
        showNotification,
        supportsPatchTransactionRefresh,
        syncTransactionHistoryState,
    ]);

    const dispatchBatchUpdateRequest = useCallback((updates, source = 'batch', meta = null) => {
        if (!editingEnabled) return null;
        const normalizedUpdates = Array.isArray(updates)
            ? updates.filter((update) => update && typeof update === 'object' && update.rowId && update.colId)
            : [];
        if (normalizedUpdates.length === 0 || !setPropsRef.current) return null;

        if (!serverSide) {
            if (editValueDisplayMode === 'original') {
                setEditValueDisplayMode('edited');
            }
            setPropsRef.current({ cellUpdates: normalizedUpdates });
            return { requestId: null };
        }
        const preparedUpdates = resolvePropagationFormulaUpdates(normalizedUpdates, source, meta);
        if (!preparedUpdates || preparedUpdates.length === 0) return null;
        if (editValueDisplayMode === 'original') {
            setEditValueDisplayMode('edited');
        }
        return dispatchServerEditUpdates(preparedUpdates, source, meta);
    }, [dispatchServerEditUpdates, editValueDisplayMode, editingEnabled, resolvePropagationFormulaUpdates, serverSide, setEditValueDisplayMode]);

    const dispatchInlineCellEdit = useCallback((update) => {
        if (!update || typeof update !== 'object') return;
        dispatchBatchUpdateRequest([update], 'inline-edit');
    }, [dispatchBatchUpdateRequest]);

    const saveRowEditSession = useCallback((rowId) => {
        if (rowId === null || rowId === undefined) return;
        const normalizedRowId = String(rowId);
        const session = rowEditSessionsRef.current[normalizedRowId];
        if (!session || session.status === 'saving') return;
        const dirtyColumns = Object.keys(session.drafts || {});
        if (dirtyColumns.length === 0) {
            closeRowEditSession(normalizedRowId);
            emitEditLifecycleEvent({
                kind: 'row_edit_cancel',
                rowId: normalizedRowId,
                empty: true,
            });
            return;
        }

        const nextErrors = {};
        const rowValues = {
            ...(session.originalValues || {}),
            ...(session.drafts || {}),
        };
        dirtyColumns.forEach((colId) => {
            const editorConfig = session.editableColumns && session.editableColumns[colId]
                ? session.editableColumns[colId].editorConfig
                : {};
            const validation = validateEditorValue(rowValues[colId], editorConfig.validationRules || [], {
                columnId: colId,
                columnLabel: colId,
                rowId: normalizedRowId,
                rowValues,
            });
            if (!validation.valid) {
                nextErrors[colId] = validation.error || 'Invalid value';
            }
        });
        if (Object.keys(nextErrors).length > 0) {
            updateRowEditSessions((previousSessions) => {
                const previousSession = previousSessions[normalizedRowId];
                if (!previousSession) return previousSessions;
                return {
                    ...previousSessions,
                    [normalizedRowId]: {
                        ...previousSession,
                        status: 'editing',
                        errors: {
                            ...(previousSession.errors || {}),
                            ...nextErrors,
                        },
                    },
                };
            });
            emitEditLifecycleEvent({
                kind: 'row_edit_validation_error',
                rowId: normalizedRowId,
                errors: nextErrors,
            });
            return;
        }

        const updates = dirtyColumns.map((colId) => {
            const editableEntry = session.editableColumns && session.editableColumns[colId]
                ? session.editableColumns[colId]
                : null;
            const nextUpdate = {
                rowId: normalizedRowId,
                colId,
                value: rowValues[colId],
                oldValue: session.originalValues ? session.originalValues[colId] : undefined,
                rowPath: normalizedRowId,
                source: 'row-edit',
                timestamp: Date.now(),
            };
            if (editableEntry && editableEntry.columnConfig && editableEntry.columnConfig.field) {
                nextUpdate.aggregation = {
                    field: editableEntry.columnConfig.field,
                    agg: editableEntry.columnConfig.agg,
                    weightField: editableEntry.columnConfig.weightField || null,
                    windowFn: editableEntry.columnConfig.windowFn || null,
                };
            }
            return nextUpdate;
        });

        updateRowEditSessions((previousSessions) => {
            const previousSession = previousSessions[normalizedRowId];
            if (!previousSession) return previousSessions;
            return {
                ...previousSessions,
                [normalizedRowId]: {
                    ...previousSession,
                    status: 'saving',
                },
            };
        });
        emitEditLifecycleEvent({
            kind: 'row_edit_commit_start',
            rowId: normalizedRowId,
            dirtyColumns,
        });
        const dispatched = dispatchBatchUpdateRequest(updates, 'row-edit', {
            rowEditRowId: normalizedRowId,
            dirtyColumns,
        });
        if (!dispatched) {
            updateRowEditSessions((previousSessions) => {
                const previousSession = previousSessions[normalizedRowId];
                if (!previousSession) return previousSessions;
                return {
                    ...previousSessions,
                    [normalizedRowId]: {
                        ...previousSession,
                        status: 'editing',
                    },
                };
            });
            emitEditLifecycleEvent({
                kind: 'row_edit_commit_error',
                rowId: normalizedRowId,
                message: 'Unable to dispatch the row edit transaction.',
            });
            return;
        }
        if (!serverSide || !dispatched.requestId) {
            closeRowEditSession(normalizedRowId);
            emitEditLifecycleEvent({
                kind: 'row_edit_commit_success',
                rowId: normalizedRowId,
                dirtyColumns,
                local: true,
            });
        }
    }, [closeRowEditSession, dispatchBatchUpdateRequest, emitEditLifecycleEvent, serverSide, updateRowEditSessions]);

    const renderRowEditActions = useCallback((row) => {
        if (!row) return null;
        if (!normalizedEditingConfig.rowActions || !['row', 'hybrid'].includes(normalizedEditingConfig.mode)) {
            return null;
        }
        const meta = getRowEditMeta(row);
        if (!meta.canEdit) return null;
        const hasSession = Boolean(meta.session);
        const hasErrors = meta.errorCount > 0;
        const showSessionControls = hasSession && !meta.session.autoSave;
        const showSessionStatus = hasSession && (meta.dirtyCount > 0 || meta.saving);
        if (!showSessionControls && !showSessionStatus) {
            return null;
        }
        return (
            <span
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '4px',
                    marginLeft: '8px',
                    flexShrink: 0,
                }}
            >
                {showSessionControls ? (
                    <>
                        <button
                            type="button"
                            data-row-edit-action="save"
                            data-row-edit-rowid={meta.rowId}
                            onClick={(e) => {
                                e.stopPropagation();
                                saveRowEditSession(meta.rowId);
                            }}
                            onMouseDown={(e) => e.stopPropagation()}
                            disabled={meta.saving}
                            style={{
                                border: `1px solid ${hasErrors ? '#dc2626' : (theme.primary || '#2563EB')}`,
                                background: hasErrors ? '#fef2f2' : (theme.primary || '#2563EB'),
                                color: hasErrors ? '#991b1b' : '#fff',
                                cursor: meta.saving ? 'progress' : 'pointer',
                                opacity: meta.saving ? 0.7 : 1,
                                padding: '2px 4px',
                                borderRadius: '6px',
                                display: 'inline-flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}
                            title={hasErrors ? 'Fix validation errors before saving this row' : 'Save this row'}
                        >
                            <Icons.Save />
                        </button>
                        <button
                            type="button"
                            data-row-edit-action="cancel"
                            data-row-edit-rowid={meta.rowId}
                            onClick={(e) => {
                                e.stopPropagation();
                                cancelRowEditSession(meta.rowId);
                            }}
                            onMouseDown={(e) => e.stopPropagation()}
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.surfaceBg,
                                color: theme.textSec,
                                cursor: 'pointer',
                                padding: '2px 4px',
                                borderRadius: '6px',
                                display: 'inline-flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}
                            title="Cancel row editing"
                        >
                            <Icons.Close />
                        </button>
                    </>
                ) : null}
                {showSessionStatus ? (
                    <span
                        style={{
                            fontSize: '10px',
                            fontWeight: 700,
                            color: hasErrors ? '#991b1b' : theme.textSec,
                            background: hasErrors ? '#fee2e2' : theme.headerSubtleBg,
                            borderRadius: '999px',
                            padding: '1px 6px',
                        }}
                    >
                        {meta.saving ? 'Saving…' : `${meta.dirtyCount} dirty${meta.errorCount ? ` • ${meta.errorCount} invalid` : ''}`}
                    </span>
                ) : null}
            </span>
        );
    }, [cancelRowEditSession, getRowEditMeta, normalizedEditingConfig.mode, normalizedEditingConfig.rowActions, saveRowEditSession, theme.border, theme.headerSubtleBg, theme.primary, theme.surfaceBg, theme.textSec]);

    const executeUndoTransaction = useCallback(() => {
        if (transactionHistoryPendingRequestRef.current) return;
        const entry = transactionUndoStackRef.current[transactionUndoStackRef.current.length - 1];
        if (!entry || typeof entry !== 'object') return;
        if (entry.kind === 'layout') {
            transactionUndoStackRef.current = transactionUndoStackRef.current.slice(0, -1);
            transactionRedoStackRef.current = [...transactionRedoStackRef.current, entry];
            clearPendingLayoutHistoryCapture();
            applyLayoutHistorySnapshot(entry.before);
            syncTransactionHistoryState();
            showNotification('Undid the last layout change.', 'success');
            return;
        }
        if (!serverSide) return;
        const undoPayload = entry.eventId
            ? {
                eventAction: 'undo',
                eventId: entry.eventId,
                refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
            }
            : entry.undoTransaction;
        if (!undoPayload) return;
        const dispatched = dispatchTransactionRequest(undoPayload, 'undo');
        if (!dispatched || !dispatched.requestId) return;
        pendingTransactionHistoryRef.current.set(dispatched.requestId, {
            action: 'undo',
            entry,
            targetEventIds: entry.eventId ? [entry.eventId] : [],
        });
        transactionHistoryPendingRequestRef.current = dispatched.requestId;
        syncTransactionHistoryState();
    }, [applyLayoutHistorySnapshot, clearPendingLayoutHistoryCapture, dispatchTransactionRequest, serverSide, showNotification, supportsPatchTransactionRefresh, syncTransactionHistoryState]);

    const executeRedoTransaction = useCallback(() => {
        if (transactionHistoryPendingRequestRef.current) return;
        const entry = transactionRedoStackRef.current[transactionRedoStackRef.current.length - 1];
        if (!entry || typeof entry !== 'object') return;
        if (entry.kind === 'layout') {
            transactionRedoStackRef.current = transactionRedoStackRef.current.slice(0, -1);
            transactionUndoStackRef.current = [...transactionUndoStackRef.current, entry];
            clearPendingLayoutHistoryCapture();
            applyLayoutHistorySnapshot(entry.after);
            syncTransactionHistoryState();
            showNotification('Reapplied the last layout change.', 'success');
            return;
        }
        if (!serverSide) return;
        if (entry.comparisonPlan) {
            applyComparisonValuePlan(entry.comparisonPlan, 'forward');
        }
        setEditValueDisplayMode('edited');
        const redoPayload = entry.eventId
            ? {
                eventAction: 'redo',
                eventId: entry.eventId,
                refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
            }
            : entry.redoTransaction;
        if (!redoPayload) {
            if (entry.comparisonPlan) applyComparisonValuePlan(entry.comparisonPlan, 'backward');
            if (comparisonValueState.activeCount === 0) {
                setEditValueDisplayMode('original');
            }
            return;
        }
        const dispatched = dispatchTransactionRequest(redoPayload, 'redo');
        if (!dispatched || !dispatched.requestId) {
            if (entry.comparisonPlan) applyComparisonValuePlan(entry.comparisonPlan, 'backward');
            if (comparisonValueState.activeCount === 0) {
                setEditValueDisplayMode('original');
            }
            return;
        }
        pendingTransactionHistoryRef.current.set(dispatched.requestId, {
            action: 'redo',
            entry,
            comparisonPlan: entry.comparisonPlan || null,
            targetEventIds: entry.eventId ? [entry.eventId] : [],
        });
        transactionHistoryPendingRequestRef.current = dispatched.requestId;
        syncTransactionHistoryState();
    }, [applyComparisonValuePlan, applyLayoutHistorySnapshot, clearPendingLayoutHistoryCapture, comparisonValueState.activeCount, dispatchTransactionRequest, serverSide, setEditValueDisplayMode, showNotification, supportsPatchTransactionRefresh, syncTransactionHistoryState]);

    transactionUndoExecutorRef.current = executeUndoTransaction;
    transactionRedoExecutorRef.current = executeRedoTransaction;

    useEffect(() => {
        const handleGlobalEditShortcut = (event) => {
            if (event.defaultPrevented || isEditableKeyboardTarget(event.target)) return;
            if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
            const key = event.key.toLowerCase();
            if (key === 'z') {
                event.preventDefault();
                if (event.shiftKey) handleRedo();
                else handleUndo();
                return;
            }
            if (key === 'y') {
                event.preventDefault();
                handleRedo();
            }
        };
        window.addEventListener('keydown', handleGlobalEditShortcut);
        return () => window.removeEventListener('keydown', handleGlobalEditShortcut);
    }, [handleRedo, handleUndo]);

    const buildChartStateOverrideSnapshot = useCallback(() => {
        const normalizedFilters = { ...(filters || {}) };
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
            immersive_mode: immersiveMode || undefined,
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
                dockPosition: 'right',
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
                immersiveMode: false,
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
        setChartCanvasPanes((previousPanes) => {
            const removedPane = previousPanes.find((pane) => pane.id === paneId) || null;
            const remainingPanes = previousPanes.filter((pane) => pane.id !== paneId);

            if (removedPane && !removedPane.floating) {
                const reclaimedSize = Number.isFinite(Number(removedPane.size)) && Number(removedPane.size) > 0
                    ? Number(removedPane.size)
                    : 1;
                const firstDockedIndex = remainingPanes.findIndex((pane) => !pane.floating);
                if (firstDockedIndex >= 0) {
                    const nextPanes = [...remainingPanes];
                    nextPanes[firstDockedIndex] = {
                        ...nextPanes[firstDockedIndex],
                        size: (Number(nextPanes[firstDockedIndex].size) || 1) + reclaimedSize,
                    };
                    return nextPanes;
                }
                setTableCanvasSize((previousSize) => Math.max(DEFAULT_TABLE_CANVAS_SIZE, (Number(previousSize) || DEFAULT_TABLE_CANVAS_SIZE) + reclaimedSize));
            }

            return remainingPanes;
        });
        setChartPaneDataById((previousData) => {
            if (!Object.prototype.hasOwnProperty.call(previousData, paneId)) return previousData;
            const nextData = { ...previousData };
            delete nextData[paneId];
            return nextData;
        });
        delete completedChartRequestSignaturesRef.current[paneId];
    }, []);

    const buildCurrentViewState = useCallback(() => {
        const normalizedFilters = { ...(filters || {}) };
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
        viewMode,
        detailMode,
        treeConfig,
        detailConfig,
        reportDef,
        savedReports,
        activeReportId,
        frozenPivotConfig,
        filters,
        sorting,
        expanded,
        showRowTotals,
        showColTotals,
        showRowNumbers,
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
        serializedEditComparisonState,
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
        viewMode: normalizedInitialViewMode,
        detailMode: normalizedInitialDetailMode,
        treeConfig: normalizedInitialTreeConfig,
        detailConfig: normalizedInitialDetailConfig,
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
            viewMode,
            detailMode,
            treeConfig,
            detailConfig,
            rowFields, colFields, valConfigs, filters, sorting, sortOptions: effectiveSortOptions, expanded,
            immersiveMode, showRowTotals, showColTotals, columnPinning, rowPinning, columnVisibility, columnSizing
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
            const uiOnlyKeys = new Set(['detailMode', 'detailConfig', 'columnPinning', 'rowPinning', 'grandTotalPinOverride', 'columnVisibility', 'columnSizing']);
            const isExpansionOnly = serverSide && changedKeys.length > 0 && changedKeys.every(key => key === 'expanded');
            const isSortingOnly = serverSide && changedKeys.length > 0 && changedKeys.every(key => key === 'sorting');
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
                const viewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || { start: 0, end: 99, count: 100 };
                const expansionColumnWindow = resolveStableRequestedColumnWindow();

                // Extend the row window to cover the block immediately after the anchor block.
                // When expanding a row near the END of its block (e.g. row 95 in block 0),
                // new children overflow into block N+1. Without this extension, those rows have
                // no cache entry → they flash with skeleton loaders until a follow-up fetch lands.
                // pendingExpansionRef.current is already set by onExpandedChange (same event,
                // before this effect runs), so anchorBlock is available here.
                const anchorBlockHint = (pendingExpansionRef.current && pendingExpansionRef.current.anchorBlock != null ? pendingExpansionRef.current.anchorBlock : -1);
                const expansionBlockSize = serverSideBlockSize; // must match useServerSideRowModel blockSize
                const viewportAlignedStart = Math.max(
                    0,
                    Math.floor(Math.max(0, viewportSnapshot.start || 0) / expansionBlockSize) * expansionBlockSize
                );
                const alignedStart = anchorBlockHint >= 0
                    ? Math.min(viewportAlignedStart, anchorBlockHint * expansionBlockSize)
                    : viewportAlignedStart;
                const extendedEnd = anchorBlockHint >= 0
                    ? Math.max(viewportSnapshot.end, (anchorBlockHint + 2) * expansionBlockSize - 1)
                    : viewportSnapshot.end;
                const alignedEnd = Math.max(
                    extendedEnd,
                    Math.ceil((Math.max(alignedStart, extendedEnd) + 1) / expansionBlockSize) * expansionBlockSize - 1
                );
                const extendedCount = alignedEnd - alignedStart + 1;

                // Record the last block the expansion response will cover so the deferred
                // effect knows to start soft-invalidating from the block AFTER it, rather
                // than re-dirtying block N+1 that we just filled with fresh data.
                if (pendingExpansionRef.current) {
                    pendingExpansionRef.current.extendedToBlock =
                        anchorBlockHint >= 0 ? anchorBlockHint + 1 : -1;
                }

                markRequestPending({
                    version: tx.version,
                    reqStart: alignedStart,
                    reqEnd: alignedEnd,
                    colStart: expansionColumnWindow.start,
                    colEnd: expansionColumnWindow.end,
                });
                setPropsRef.current({
                    ...nextProps,
                    runtimeRequest: {
                        kind: 'data',
                        payload: {
                            table: tableName || undefined,
                            start: alignedStart,
                            end: alignedEnd,
                            count: extendedCount,
                            version: tx.version,
                            window_seq: tx.version,
                            state_epoch: tx.stateEpoch,
                            session_id: sessionIdRef.current,
                            client_instance: clientInstanceRef.current,
                            abort_generation: tx.abortGeneration,
                            intent: 'expansion',
                            col_start: expansionColumnWindow.start !== null ? expansionColumnWindow.start : undefined,
                            col_end: expansionColumnWindow.end !== null ? expansionColumnWindow.end : undefined,
                            needs_col_schema: needsColSchemaRef.current && serverSide || undefined,
                            include_grand_total: serverSidePinsGrandTotal || undefined,
                        },
                    },
                });
                return;
            }

            if (isSortingOnly) {
                expansionScrollRestoreRef.current = null;
                if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
                    cancelAnimationFrame(expansionScrollRestoreRafRef.current);
                    expansionScrollRestoreRafRef.current = null;
                }
                if (parentRef.current) {
                    parentRef.current.scrollTop = 0;
                }

                const tx = beginExpansionRequest();
                const sortingColumnWindow = resolveStableRequestedColumnWindow();
                const initialEnd = 99;
                markRequestPending({
                    version: tx.version,
                    reqStart: 0,
                    reqEnd: initialEnd,
                    colStart: sortingColumnWindow.start,
                    colEnd: sortingColumnWindow.end,
                });
                setPropsRef.current({
                    ...nextProps,
                    runtimeRequest: {
                        kind: 'data',
                        payload: {
                            table: tableName || undefined,
                            start: 0,
                            end: initialEnd,
                            count: initialEnd + 1,
                            version: tx.version,
                            window_seq: tx.version,
                            state_epoch: tx.stateEpoch,
                            session_id: sessionIdRef.current,
                            client_instance: clientInstanceRef.current,
                            abort_generation: tx.abortGeneration,
                            intent: 'viewport',
                            col_start: sortingColumnWindow.start !== null ? sortingColumnWindow.start : undefined,
                            col_end: sortingColumnWindow.end !== null ? sortingColumnWindow.end : undefined,
                            include_grand_total: serverSidePinsGrandTotal || undefined,
                            immersive_mode: immersiveMode || undefined,
                        },
                    },
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
            const structuralViewportSnapshot = latestRequestedViewportRef.current || latestViewportRef.current || { start: 0, end: 99, count: 100 };
            const structuralWindowSize = Math.max(
                100,
                Math.max(1, Number.isFinite(Number(structuralViewportSnapshot.count)) ? Number(structuralViewportSnapshot.count) : 0)
            );
            const structuralStart = 0;
            const structuralEnd = structuralWindowSize - 1;
            const structuralColumnWindow = resolveStableRequestedColumnWindow();
            markRequestPending({
                version: tx.version,
                reqStart: structuralStart,
                reqEnd: structuralEnd,
                colStart: structuralColumnWindow.start,
                colEnd: structuralColumnWindow.end,
            });
            setPropsRef.current({
                ...nextProps,
                runtimeRequest: {
                    kind: 'data',
                    payload: {
                        table: tableName || undefined,
                        start: structuralStart,
                        end: structuralEnd,
                        count: structuralWindowSize,
                        version: tx.version,
                        window_seq: tx.version,
                        state_epoch: tx.stateEpoch,
                        session_id: sessionIdRef.current,
                        client_instance: clientInstanceRef.current,
                        abort_generation: tx.abortGeneration,
                        intent: 'structural',
                        col_start: structuralColumnWindow.start !== null ? structuralColumnWindow.start : undefined,
                        col_end: structuralColumnWindow.end !== null ? structuralColumnWindow.end : undefined,
                        needs_col_schema: serverSide || undefined,
                        include_grand_total: serverSidePinsGrandTotal || undefined,
                        immersive_mode: immersiveMode || undefined,
                    },
                },
            });
        }
    }, [viewMode, detailMode, treeConfig, detailConfig, rowFields, colFields, valConfigs, filters, sorting, effectiveSortOptions, expanded, immersiveMode, showRowTotals, showColTotals, columnPinning, rowPinning, grandTotalPinOverride, columnVisibility, columnSizing, beginStructuralTransaction, beginExpansionRequest, markRequestPending, resolveStableRequestedColumnWindow, serverSide, serverSideBlockSize, tableName, serverSidePinsGrandTotal]);

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

    const matchesSelectedColumnToValueConfig = useCallback((colId, cfg) => {
        if (!cfg || typeof colId !== 'string' || !cfg.field) return false;
        const normalizedColId = colId.toLowerCase();
        if (cfg.agg === 'formula') {
            const formulaId = String(cfg.field).toLowerCase();
            return normalizedColId === formulaId || normalizedColId.endsWith(`_${formulaId}`);
        }
        const suffix = `_${cfg.field}_${cfg.agg}`.toLowerCase();
        return (
            normalizedColId === String(cfg.field).toLowerCase() ||
            normalizedColId.endsWith(suffix) ||
            normalizedColId.includes(`_${String(cfg.field).toLowerCase()}_`)
        );
    }, []);

    const getSelectedMeasureIndexes = useCallback((selectedColIds) => {
        const matchedIndexes = [];
        valConfigs.forEach((cfg, idx) => {
            const matches = selectedColIds.some((colId) => matchesSelectedColumnToValueConfig(colId, cfg));
            if (matches) matchedIndexes.push(idx);
        });
        return matchedIndexes;
    }, [matchesSelectedColumnToValueConfig, valConfigs]);

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

    const getCurrentFormatForSelection = useCallback((selectionMap) => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        const matchedIndexes = getSelectedMeasureIndexes(selectedColIds);
        if (matchedIndexes.length === 0) return '';

        const formats = matchedIndexes
            .map(idx => valConfigs[idx] && typeof valConfigs[idx].format === 'string' ? valConfigs[idx].format.trim() : '')
            .map(fmt => fmt || '')
            .filter((fmt, idx, arr) => arr.indexOf(fmt) === idx);

        return formats.length === 1 ? formats[0] : '';
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
                'Format selected values.\nExamples: fixed:2, currency:$, accounting:€, percent, compact',
                promptDefault
            );
            if (formatInput === null) return;
            normalizedFormat = String(formatInput).trim();
        }

        setValConfigs(prev => {
            let matchedAny = false;
            const matchedIndexes = [];
            prev.forEach((cfg, idx) => {
                const matches = selectedColIds.some((colId) => matchesSelectedColumnToValueConfig(colId, cfg));
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
    }, [getSelectedColumnIds, getDefaultFormatForSelection, matchesSelectedColumnToValueConfig, setValConfigs, showNotification]);

    const selectedValueColumnIds = useMemo(
        () => getSelectedColumnIds(selectedCells).filter(id => id !== 'hierarchy' && id !== '__row_number__'),
        [getSelectedColumnIds, selectedCells]
    );
    const canApplySelectionValueFormat = useMemo(
        () => getSelectedMeasureIndexes(selectedValueColumnIds).length > 0,
        [getSelectedMeasureIndexes, selectedValueColumnIds]
    );
    const selectionValueFormat = useMemo(
        () => getCurrentFormatForSelection(selectedCells),
        [getCurrentFormatForSelection, selectedCells]
    );
    const applySelectionValueFormat = useCallback((formatOverride) => {
        applyFormatToSelection(selectedCells, formatOverride);
    }, [applyFormatToSelection, selectedCells]);

    const getDisplayHeaderTextForColumn = useCallback((column) => {
        if (!column) return '';
        const columnId = typeof column.id === 'string' ? column.id : '';
        const matchingValueConfig = Array.isArray(valConfigs)
            ? (valConfigs.find((config) => matchesSelectedColumnToValueConfig(columnId, config)) || null)
            : null;

        if (matchingValueConfig) {
            return matchingValueConfig.agg === 'formula'
                ? (matchingValueConfig.label || matchingValueConfig.field || columnId)
                : `${formatDisplayLabel(matchingValueConfig.field)} (${formatAggLabel(matchingValueConfig.agg, matchingValueConfig.weightField)})`;
        }

        const columnDef = column.columnDef || {};
        if (typeof columnDef.header === 'string' && columnDef.header.trim()) {
            return columnDef.header;
        }
        if (columnDef.headerVal !== undefined && columnDef.headerVal !== null) {
            return String(columnDef.headerVal);
        }
        if (columnId) {
            return formatDisplayLabel(columnId.replace(/^group_/, '').replace(/\|\|\|/g, ' > '));
        }
        return '';
    }, [matchesSelectedColumnToValueConfig, valConfigs]);
    const getDisplayHeaderTextStackForColumn = useCallback((column) => {
        if (!column) return [];
        const parts = [];
        const visited = new Set();
        let current = column;

        while (current && !visited.has(current.id)) {
            visited.add(current.id);
            const text = getDisplayHeaderTextForColumn(current);
            if (text) parts.unshift(text);
            current = current.parent || null;
        }

        return parts;
    }, [getDisplayHeaderTextForColumn]);
    const getAutoSizeSampleRows = useCallback((rows) => {
        if (!Array.isArray(rows) || rows.length === 0) return [];
        if (rows.length <= MAX_AUTO_SIZE_SAMPLE_ROWS) return rows;
        const lastIndex = rows.length - 1;
        const sampledIndices = new Set();
        for (let index = 0; index < MAX_AUTO_SIZE_SAMPLE_ROWS; index += 1) {
            sampledIndices.add(Math.round((index * lastIndex) / (MAX_AUTO_SIZE_SAMPLE_ROWS - 1)));
        }
        return Array.from(sampledIndices)
            .sort((left, right) => left - right)
            .map((index) => rows[index])
            .filter(Boolean);
    }, []);
    const getRenderedHeaderWidthForColumn = useCallback((columnId) => {
        if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
        const scopedRoot = typeof id === 'string' && id ? document.getElementById(id) : null;
        const headerElements = Array.from((scopedRoot || document).querySelectorAll('[role="columnheader"][data-header-column-id]'))
            .filter((element) => element && element.dataset && element.dataset.headerColumnId === String(columnId));
        if (headerElements.length === 0) return null;

        let maxMeasuredWidth = null;
        headerElements.forEach((headerElement) => {
            const headerContent = headerElement.querySelector('[data-header-content="true"]');
            const headerStyle = window.getComputedStyle(headerElement);
            const horizontalChrome = (parseFloat(headerStyle.paddingLeft || '0') || 0)
                + (parseFloat(headerStyle.paddingRight || '0') || 0)
                + (parseFloat(headerStyle.borderLeftWidth || '0') || 0)
                + (parseFloat(headerStyle.borderRightWidth || '0') || 0);
            let naturalContentWidth = 0;
            if (headerContent && document.body) {
                const measurementClone = headerContent.cloneNode(true);
                measurementClone.style.position = 'absolute';
                measurementClone.style.left = '-100000px';
                measurementClone.style.top = '0';
                measurementClone.style.visibility = 'hidden';
                measurementClone.style.pointerEvents = 'none';
                measurementClone.style.width = 'max-content';
                measurementClone.style.minWidth = '0';
                measurementClone.style.maxWidth = 'none';
                measurementClone.style.overflow = 'visible';
                measurementClone.style.display = 'inline-flex';
                measurementClone.style.flex = '0 0 auto';
                measurementClone.querySelectorAll('[data-header-text="true"]').forEach((textElement) => {
                    if (!textElement || !textElement.style) return;
                    textElement.style.flex = '0 0 auto';
                    textElement.style.width = 'auto';
                    textElement.style.minWidth = '0';
                    textElement.style.maxWidth = 'none';
                    textElement.style.overflow = 'visible';
                    textElement.style.textOverflow = 'clip';
                    textElement.style.display = 'inline-block';
                });
                document.body.appendChild(measurementClone);
                naturalContentWidth = Math.ceil(
                    measurementClone.scrollWidth
                    || measurementClone.getBoundingClientRect().width
                    || 0
                );
                document.body.removeChild(measurementClone);
            }
            const candidateWidth = naturalContentWidth > 0
                ? naturalContentWidth + horizontalChrome + autoSizeBounds.headerOverscan
                : null;
            if (!Number.isFinite(candidateWidth)) return;
            maxMeasuredWidth = maxMeasuredWidth === null
                ? candidateWidth
                : Math.max(maxMeasuredWidth, candidateWidth);
        });

        return maxMeasuredWidth;
    }, [autoSizeBounds.headerOverscan, id]);
    const getRenderedCellWidthForColumn = useCallback((columnId) => {
        if (typeof document === 'undefined' || typeof window === 'undefined' || !columnId) return null;
        const scopedRoot = typeof id === 'string' && id ? document.getElementById(id) : null;
        const cellElements = Array.from((scopedRoot || document).querySelectorAll('[role="gridcell"][data-colid]'))
            .filter((element) => element && element.dataset && element.dataset.colid === String(columnId));
        if (cellElements.length === 0) return null;

        let maxMeasuredWidth = null;
        cellElements.forEach((cellElement) => {
            const cellStyle = window.getComputedStyle(cellElement);
            const horizontalChrome = (parseFloat(cellStyle.paddingLeft || '0') || 0)
                + (parseFloat(cellStyle.paddingRight || '0') || 0)
                + (parseFloat(cellStyle.borderLeftWidth || '0') || 0)
                + (parseFloat(cellStyle.borderRightWidth || '0') || 0);
            const contentContainer = Array.from(cellElement.children || []).find((childElement) => (
                childElement && childElement.tagName === 'SPAN'
            )) || cellElement.querySelector('span');
            let contentWidth = 0;
            if (contentContainer) {
                const contentStyle = window.getComputedStyle(contentContainer);
                const gap = parseFloat(contentStyle.columnGap || contentStyle.gap || '0') || 0;
                const horizontalPadding = (parseFloat(contentStyle.paddingLeft || '0') || 0)
                    + (parseFloat(contentStyle.paddingRight || '0') || 0);
                const childElements = Array.from(contentContainer.children || []);
                if (childElements.length > 0) {
                    const childWidths = childElements.reduce((sum, childElement) => (
                        sum + Math.ceil(childElement.scrollWidth || childElement.getBoundingClientRect().width || 0)
                    ), 0);
                    contentWidth = childWidths + horizontalPadding + (gap * Math.max(0, childElements.length - 1));
                }
                if (contentWidth <= 0) {
                    contentWidth = Math.ceil(contentContainer.scrollWidth || contentContainer.getBoundingClientRect().width || 0);
                }
            }
            const candidateWidth = contentWidth > 0
                ? Math.ceil(contentWidth + horizontalChrome + autoSizeBounds.cellOverscan)
                : null;
            if (!Number.isFinite(candidateWidth)) return;
            maxMeasuredWidth = maxMeasuredWidth === null
                ? candidateWidth
                : Math.max(maxMeasuredWidth, candidateWidth);
        });

        return maxMeasuredWidth;
    }, [autoSizeBounds.cellOverscan, id]);
    const queueHeaderFitValidation = useCallback((columnId, measuredWidth) => {
        if (typeof window === 'undefined' || !columnId || !table || typeof table.setColumnSizing !== 'function') return;
        const raf = window.requestAnimationFrame || ((callback) => window.setTimeout(callback, 0));
        raf(() => {
            raf(() => {
                const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
                if (!Number.isFinite(renderedHeaderWidth)) return;
                const currentColumn = typeof table.getColumn === 'function' ? table.getColumn(columnId) : null;
                const currentWidth = currentColumn && typeof currentColumn.getSize === 'function'
                    ? currentColumn.getSize()
                    : measuredWidth;
                const targetWidth = Math.min(
                    autoSizeBounds.maxWidth,
                    Math.max(autoSizeBounds.minWidth, Math.ceil(renderedHeaderWidth))
                );
                if (targetWidth > Math.max(measuredWidth, currentWidth) + 1) {
                    table.setColumnSizing((old) => ({
                        ...old,
                        [columnId]: targetWidth
                    }));
                }
            });
        });
    }, [autoSizeBounds.headerOverscan, autoSizeBounds.maxWidth, autoSizeBounds.minWidth, getRenderedHeaderWidthForColumn, table]);

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
        let visibleCount = Math.max(1, visibleColRange.end - visibleColRange.start + 1);
        if (columnVirtualizerRef.current && typeof columnVirtualizerRef.current.getVirtualItems === 'function') {
            const virtualItems = columnVirtualizerRef.current.getVirtualItems() || [];
            if (virtualItems.length > 0) {
                visibleCount = Math.max(visibleCount, virtualItems.length);
            }
        }
        resetVisibleColRange(visibleCount);
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

        setColumnPinningWithHistory(prev => {
            const next = { left: [...(prev.left || [])], right: [...(prev.right || [])] };

            // Remove all relevant IDs from both sides first
            next.left = next.left.filter(id => !idsToUpdate.has(id));
            next.right = next.right.filter(id => !idsToUpdate.has(id));

            // Add to new side
            if (side === 'left') next.left.push(...idsArray);
            if (side === 'right') next.right.push(...idsArray);

            return next;
        }, 'layout:column-pinning');
    }, [setColumnPinningWithHistory]);


    const handlePinRow = (rowId, pinState) => {
        requestLayoutHistoryCapture('layout:row-pinning');
        if (rowId === GRAND_TOTAL_ROW_ID) {
            setGrandTotalPinOverride(pinState === 'top' || pinState === 'bottom' ? pinState : false);
        }
        setRowPinningWithHistory(prev => applyRowPinning(prev, rowId, pinState), 'layout:row-pinning');

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

    const applyHeaderSort = (colId, desc, sortMetadata = {}, append = false) => {
        if (!colId || colId === '__row_number__') return;
        const nextSorting = updateSortingForColumn({
            sorting,
            columnId: colId,
            desc,
            sortMetadata,
            append,
        });
        setSorting(nextSorting);
        if (setPropsRef.current) {
            setPropsRef.current({
                sorting: nextSorting,
                sortEvent: {
                    type: 'change',
                    status: 'applied',
                    sorting: nextSorting,
                    timestamp: Date.now()
                }
            });
        }
    };

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
            const hasOtherSorts = Array.isArray(sorting) && sorting.some((sortSpec) => sortSpec && sortSpec.id !== colId);
            const existingSortIndex = Array.isArray(sorting)
                ? sorting.findIndex((sortSpec) => sortSpec && sortSpec.id === colId)
                : -1;
            const existingSortPriority = existingSortIndex >= 0 ? existingSortIndex + 1 : null;
            const multiSortActionVerb = existingSortPriority ? `Update Priority ${existingSortPriority}` : 'Add';
            const absoluteSortMetadata = { sortType: 'absolute', absoluteSort: true };
            actions.push({
                label: 'Sort Ascending',
                icon: <Icons.SortAsc/>,
                onClick: () => applyHeaderSort(colId, false)
            });
            actions.push({
                label: 'Sort Descending',
                icon: <Icons.SortDesc/>,
                onClick: () => applyHeaderSort(colId, true)
            });
            if (hasOtherSorts) {
                actions.push({
                    label: `${multiSortActionVerb} Ascending Sort`,
                    icon: <Icons.SortAsc/>,
                    onClick: () => applyHeaderSort(colId, false, {}, true)
                });
                actions.push({
                    label: `${multiSortActionVerb} Descending Sort`,
                    icon: <Icons.SortDesc/>,
                    onClick: () => applyHeaderSort(colId, true, {}, true)
                });
            }
            actions.push({
                label: 'Sort Absolute Ascending',
                icon: <Icons.SortAsc/>,
                onClick: () => applyHeaderSort(colId, false, absoluteSortMetadata)
            });
            actions.push({
                label: 'Sort Absolute Descending',
                icon: <Icons.SortDesc/>,
                onClick: () => applyHeaderSort(colId, true, absoluteSortMetadata)
            });
            if (hasOtherSorts) {
                actions.push({
                    label: `${multiSortActionVerb} Absolute Ascending Sort`,
                    icon: <Icons.SortAsc/>,
                    onClick: () => applyHeaderSort(colId, false, absoluteSortMetadata, true)
                });
                actions.push({
                    label: `${multiSortActionVerb} Absolute Descending Sort`,
                    icon: <Icons.SortDesc/>,
                    onClick: () => applyHeaderSort(colId, true, absoluteSortMetadata, true)
                });
            }
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
                 handleCellDrillThrough(row, colId);
             }
        }});

        actions.push('separator');
        if (row && serverSide && row.getCanExpand() && row.original && row.original._path && row.original._has_children) {
            actions.push({
                label: 'Expand All Children',
                icon: <Icons.ChevronDown/>,
                onClick: () => {
                    const rowPath = row.original._path;
                    subtreeExpandRef.current = { path: rowPath, expandedPaths: new Set([rowPath]) };
                    captureExpansionScrollPosition();
                    clearCache();
                    setExpandedWithHistory((prev) => {
                        const base = (prev !== null && typeof prev === 'object') ? prev : {};
                        return { ...base, [rowPath]: true };
                    }, 'layout:expand-subtree');
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

    useEffect(() => {
        const nextRowPaths = [];
        const seenPaths = new Set();
        cleanData.forEach((row) => {
            const rowId = resolveTransportRowId(row);
            if (!rowId || seenPaths.has(rowId)) return;
            seenPaths.add(rowId);
            nextRowPaths.push(rowId);
        });
        visiblePatchRowPathsRef.current = nextRowPaths;
    }, [cleanData]);

    useEffect(() => {
        const groupingIds = new Set(Array.isArray(rowFields) ? rowFields : []);
        const nextCenterIds = [];
        const seenIds = new Set();
        (Array.isArray(responseColumns) ? responseColumns : []).forEach((column) => {
            const columnId = column && typeof column === 'object' ? column.id : column;
            const normalizedId = String(columnId || '').trim();
            if (!normalizedId || normalizedId === '__col_schema' || groupingIds.has(normalizedId) || seenIds.has(normalizedId)) {
                return;
            }
            seenIds.add(normalizedId);
            nextCenterIds.push(normalizedId);
        });
        visiblePatchCenterIdsRef.current = nextCenterIds;
    }, [responseColumns, rowFields]);

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

    // px per depth level matching useColumnDefs paddingLeft: `${depth * 24}px`
    const HIERARCHY_INDENT_PX = 24;
    // expand button: 16px icon + 6px marginRight = 22px; spacer span: 18px — use 22 as worst case
    const HIERARCHY_EXPAND_PX = 22;

    const autoSizeColumn = useCallback((columnId, includeHeader = true) => {
        if (!table) return;
        const rows = table.getRowModel().rows;
        const sampleRows = getAutoSizeSampleRows(rows);
        let maxWidth = autoSizeBounds.minWidth;

        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        if (!context) return;
        const bodyFont = `${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;
        const headerFont = `600 ${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;

        const column = table.getColumn(columnId);
        if (!column) return;
        if (column.columnDef && column.columnDef.meta && column.columnDef.meta.isSchemaPlaceholder) return;
        const matchingValueConfig = Array.isArray(valConfigs)
            ? (valConfigs.find((config) => matchesSelectedColumnToValueConfig(columnId, config)) || null)
            : null;
        const headerTextStack = getDisplayHeaderTextStackForColumn(column);
        if (includeHeader) {
            context.font = headerFont;
            headerTextStack.forEach((headerText) => {
                maxWidth = Math.max(maxWidth, context.measureText(headerText).width + autoSizeBounds.headerPadding + autoSizeBounds.headerOverscan);
            });
            const renderedHeaderWidth = getRenderedHeaderWidthForColumn(columnId);
            if (Number.isFinite(renderedHeaderWidth)) {
                maxWidth = Math.max(maxWidth, renderedHeaderWidth);
            }
        }

        context.font = bodyFont;
        sampleRows.forEach(row => {
            const cellValue = row.getValue(columnId);
            let width;
            if (columnId === 'hierarchy') {
                const depth = row.original && typeof row.original.depth === 'number' ? row.original.depth : (row.depth || 0);
                const label = row.original
                    ? formatDisplayLabel(String(row.original._label != null ? row.original._label : (row.original._id != null ? row.original._id : '')))
                    : '';
                const textWidth = context.measureText(label).width;
                // actual indent = CSS paddingLeft (depth * 24px) + expand button (22px) + cell padding (24px)
                width = textWidth + depth * HIERARCHY_INDENT_PX + HIERARCHY_EXPAND_PX + autoSizeBounds.cellPadding + autoSizeBounds.cellOverscan;
            } else {
                const effectiveDecimalPlaces = columnDecimalOverrides && columnDecimalOverrides[columnId] !== undefined
                    ? columnDecimalOverrides[columnId]
                    : decimalPlaces;
                const effectiveFormat = Object.prototype.hasOwnProperty.call(columnFormatOverrides || {}, columnId)
                    ? columnFormatOverrides[columnId]
                    : ((matchingValueConfig && matchingValueConfig.format) || defaultValueFormat || null);
                const effectiveGroupSeparator = columnGroupSeparatorOverrides && columnGroupSeparatorOverrides[columnId] !== undefined
                    ? columnGroupSeparatorOverrides[columnId]
                    : numberGroupSeparator;
                const text = formatValue(cellValue, effectiveFormat, effectiveDecimalPlaces, effectiveGroupSeparator);
                width = context.measureText(text).width + autoSizeBounds.cellPadding + autoSizeBounds.cellOverscan;
            }
            if (width > maxWidth) maxWidth = width;
        });

        const renderedCellWidth = (columnId !== 'hierarchy' && columnId !== '__row_number__')
            ? getRenderedCellWidthForColumn(columnId)
            : null;
        if (Number.isFinite(renderedCellWidth)) {
            maxWidth = Math.max(maxWidth, renderedCellWidth);
        }

        maxWidth = Math.ceil(maxWidth);
        maxWidth = Math.min(maxWidth, autoSizeBounds.maxWidth);
        maxWidth = Math.max(maxWidth, autoSizeBounds.minWidth);

        table.setColumnSizing(old => ({
            ...old,
            [columnId]: maxWidth
        }));
        queueHeaderFitValidation(columnId, maxWidth);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [table, fontSize, fontFamily, valConfigs, decimalPlaces, columnDecimalOverrides, columnFormatOverrides,
        columnGroupSeparatorOverrides, numberGroupSeparator, defaultValueFormat, autoSizeBounds,
        matchesSelectedColumnToValueConfig, getAutoSizeSampleRows, getDisplayHeaderTextStackForColumn, getRenderedCellWidthForColumn, getRenderedHeaderWidthForColumn,
        queueHeaderFitValidation, formatDisplayLabel]);

    const autoSizeVisibleColumns = useCallback((includeHeader = true) => {
        if (!table || !table.getVisibleLeafColumns) return;
        table.getVisibleLeafColumns().forEach((column) => {
            autoSizeColumn(column.id, includeHeader);
        });
    }, [table, autoSizeColumn]);
    const handleAutoSizeToolbarClick = useCallback(() => {
        autoSizeVisibleColumns(true);
    }, [autoSizeVisibleColumns]);

    const handleFilterClick = (e, columnId) => {
        if (!uiConfig.showFilters) return;
        e.stopPropagation();
        setActiveFilterCol(columnId);
        setFilterAnchorEl(e.currentTarget);
        emitRuntimeRequest('filter_options', { columnId, nonce: Date.now() });
    };

    const requestFilterOptions = useCallback((columnId) => {
        if (!columnId) return;
        emitRuntimeRequest('filter_options', { columnId, nonce: Date.now() });
    }, [emitRuntimeRequest]);

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

        requestLayoutHistoryCapture('layout:transpose');
        closeFilterPopover();
        setContextMenu(null);
        setExpanded({});
        setSorting([]);
        setColExpanded({});
        setSelectedCells({});
        setLastSelected(null);
        setSelectedCols(new Set());
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
        requestLayoutHistoryCapture,
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

    const handleViewportRequestMeta = useCallback((requestMeta) => {
        markRequestPending(requestMeta);
        recordProfilerQueuedRequest(requestMeta);
    }, [markRequestPending, recordProfilerQueuedRequest]);

    const captureExpansionScrollPosition = useCallback(() => {
        if (!serverSide || !parentRef.current) return;
        expansionScrollRestoreRef.current = {
            scrollTop: parentRef.current.scrollTop,
            restorePassesRemaining: 3
        };
    }, [serverSide]);

    const { rowVirtualizer, getRow, renderedData, renderedOffset, clearCache, invalidateFromBlock, softInvalidateFromBlock, grandTotalRow, loadedRows, requestUrgentColumnViewport, requestVisibleViewportRefresh } = useServerSideRowModel({
        parentRef,
        serverSide,
        rowCount: effectiveRowCount,
        rowHeight,
        data: filteredData,
        dataOffset: dataOffset || 0,
        dataVersion: dataVersion || 0,
        setProps: dispatchSetProps,
        blockSize: serverSideBlockSize,
        maxBlocksInCache: normalizedPerformanceConfig.maxBlocksInCache || 500,
        blockLoadDebounceMs: normalizedPerformanceConfig.blockLoadDebounceMs,
        rowOverscan: normalizedPerformanceConfig.rowOverscan,
        prefetchColumns: normalizedPerformanceConfig.prefetchColumns,
        cacheKey: serverSideCacheKey,
        excludeGrandTotal: serverSidePinsGrandTotal,
        immersiveMode,
        stateEpoch,
        sessionId: sessionIdRef.current,
        clientInstance: clientInstanceRef.current,
        abortGeneration,
        structuralInFlight,
        requestVersionRef,
        tableName,
        colStart: colRequestStart,
        colEnd: colRequestEnd,
        columnRangeUrgencyToken,
        responseColStart: responseSchemaWindow.start,
        responseColEnd: responseSchemaWindow.end,
        needsColSchema: needsColSchema && serverSide,
        onViewportRequest: handleViewportRequestMeta,
    });
    requestUrgentColumnViewportRef.current = requestUrgentColumnViewport;

    const scheduleSilentViewportRefresh = useCallback(() => {
        if (!serverSide || typeof requestVisibleViewportRefresh !== 'function') return;
        if (deferredViewportRefreshTimeoutRef.current !== null) {
            clearTimeout(deferredViewportRefreshTimeoutRef.current);
        }
        deferredViewportRefreshTimeoutRef.current = setTimeout(() => {
            deferredViewportRefreshTimeoutRef.current = null;
            requestVisibleViewportRefresh();
        }, 0);
    }, [requestVisibleViewportRefresh, serverSide]);

    useEffect(() => () => {
        if (deferredViewportRefreshTimeoutRef.current !== null) {
            clearTimeout(deferredViewportRefreshTimeoutRef.current);
        }
    }, []);

    const columns = useColumnDefs({
        sortOptions: effectiveSortOptions,
        sorting,
        serverSide,
        showRowNumbers,
        layoutMode,
        viewMode,
        rowFields,
        colFields,
        valConfigs,
        treeConfig,
        minMax,
        colorScaleMode,
        colExpanded,
        isRowSelecting,
        rowDragStart,
        props,
        cachedColSchema,
        filteredData,
        rowCount,
        // Render-time closures (stable refs or render-time reads)
        theme,
        defaultColumnWidths,
        validationRules,
        onCellEdit: editingEnabled ? dispatchInlineCellEdit : undefined,
        onEditBlocked: editingEnabled ? handleBlockedCellEdit : undefined,
        resolveEditorPresentation: editingEnabled ? resolveEditorPresentation : undefined,
        rowEditMode: editingEnabled ? normalizedEditingConfig.mode : 'cell',
        getRowEditSession: editingEnabled ? getRowEditSession : undefined,
        onRequestRowStart: editingEnabled ? startRowEditSession : undefined,
        onRowDraftChange: editingEnabled ? updateRowDraftValue : undefined,
        onRequestRowSave: editingEnabled ? saveRowEditSession : undefined,
        onRequestRowCancel: editingEnabled ? cancelRowEditSession : undefined,
        requestEditorOptions: editingEnabled ? requestEditorOptions : undefined,
        renderRowEditActions: editingEnabled ? renderRowEditActions : undefined,
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
        defaultValueFormat,
        numberGroupSeparator,
        columnDecimalOverrides,
        columnFormatOverrides,
        columnGroupSeparatorOverrides,
        cellFormatRules,
        pivotMode,
        reportDef,
        detailMode,
        isDetailOpenForRow: (rowPath) => Boolean(detailSurface && detailSurface.rowPath === rowPath),
        onToggleDetail: (row) => {
            if (toggleDetailForRowRef.current) {
                toggleDetailForRowRef.current(row);
            }
        },
        resolveCellDisplayValue: resolveDisplayedCellValue,
        editValueDisplayMode,
        editingEnabled,
        openSparklineDataModalRef,
    });

    // Auto-size new columns to fit their header text on spawn
    useEffect(() => {
        if (!table || !table.getVisibleLeafColumns) return;
        const visLeafCols = table.getVisibleLeafColumns();
        if (visLeafCols.length === 0) return;
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        ctx.font = `600 ${fontSize || '14px'} ${fontFamily || "'Inter', system-ui, sans-serif"}`;
        const newSizes = {};
        visLeafCols.forEach(col => {
            if (columnSizing[col.id] !== undefined) return; // already user-set
            if (col.columnDef && col.columnDef.meta && col.columnDef.meta.isSchemaPlaceholder) return;
            const measuredFromText = getDisplayHeaderTextStackForColumn(col).reduce((maxWidth, headerText) => (
                Math.max(maxWidth, ctx.measureText(headerText).width + autoSizeBounds.headerPadding + autoSizeBounds.headerOverscan)
            ), autoSizeBounds.minWidth);
            const measuredFromDom = getRenderedHeaderWidthForColumn(col.id);
            const measuredFromCells = (col.id !== 'hierarchy' && col.id !== '__row_number__')
                ? getRenderedCellWidthForColumn(col.id)
                : null;
            const measured = Number.isFinite(measuredFromDom)
                ? Math.max(measuredFromText, measuredFromDom)
                : measuredFromText;
            const measuredWithCells = Number.isFinite(measuredFromCells)
                ? Math.max(measured, measuredFromCells)
                : measured;
            const clamped = Math.min(Math.max(measuredWithCells, autoSizeBounds.minWidth), autoSizeBounds.maxWidth);
            if (clamped > (col.columnDef.size || autoSizeBounds.minWidth)) {
                newSizes[col.id] = clamped;
            }
        });
        if (Object.keys(newSizes).length > 0) {
            setColumnSizing(prev => ({ ...prev, ...newSizes }));
            Object.entries(newSizes).forEach(([columnId, width]) => {
                queueHeaderFitValidation(columnId, width);
            });
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [autoSizeBounds.maxWidth, colFields, fontFamily, fontSize, getDisplayHeaderTextStackForColumn, getRenderedCellWidthForColumn, getRenderedHeaderWidthForColumn, queueHeaderFitValidation, rowFields, valConfigs]);

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

    useEffect(() => {
        if (!serverSide || pendingRowTransitions.size === 0) return;
        const timeoutId = setTimeout(() => {
            setPendingRowTransitions(new Map());
        }, 10000);
        return () => clearTimeout(timeoutId);
    }, [pendingRowTransitions, serverSide]);

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

    const rawTableData = useMemo(() => {
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
    tableDataRef.current = rawTableData;

    const tableData = useMemo(() => {
        if (editValueDisplayMode !== 'original' || comparisonValueState.activeCount === 0) {
            return rawTableData;
        }

        const overrideRowsById = new Map();
        comparisonCellOriginalValuesRef.current.forEach((entry, key) => {
            if (!entry || typeof entry !== 'object') return;
            const cellCount = comparisonCellCountsRef.current.get(key) || 0;
            if (cellCount <= 0) return;
            const rowId = entry.rowId === null || entry.rowId === undefined ? '' : String(entry.rowId);
            const colId = entry.colId === null || entry.colId === undefined ? '' : String(entry.colId);
            if (!rowId || !colId) return;
            const rowOverrides = overrideRowsById.get(rowId) || {};
            rowOverrides[colId] = entry.value;
            overrideRowsById.set(rowId, rowOverrides);
        });

        if (overrideRowsById.size === 0) {
            return rawTableData;
        }

        return rawTableData.map((row) => {
            const rowId = getStableDataRowId(row);
            if (!rowId || !overrideRowsById.has(rowId)) return row;
            return {
                ...row,
                ...overrideRowsById.get(rowId),
            };
        });
    }, [comparisonCellEpoch, comparisonValueState.activeCount, editValueDisplayMode, getStableDataRowId, rawTableData]);

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

    const normalizeExpandedStateForTreeDefaults = useCallback((nextExpanded, previousExpanded) => {
        if (!serverSide || viewMode !== 'tree' || !treeHasDefaultExpansion) return nextExpanded;
        if (nextExpanded === true || !nextExpanded || typeof nextExpanded !== 'object') return nextExpanded;

        const normalizedExpanded = { ...nextExpanded };
        const previous = previousExpanded && typeof previousExpanded === 'object' ? previousExpanded : {};
        const allKeys = new Set([...Object.keys(previous), ...Object.keys(normalizedExpanded)]);
        allKeys.forEach((key) => {
            if (!Object.prototype.hasOwnProperty.call(normalizedExpanded, key)) {
                normalizedExpanded[key] = false;
            }
        });
        return normalizedExpanded;
    }, [serverSide, treeHasDefaultExpansion, viewMode]);

    const [paginationState, setPaginationState] = useState({ pageIndex: 0, pageSize: paginationConfig.pageSize });
    useEffect(() => {
        setPaginationState(prev => prev.pageSize === paginationConfig.pageSize ? prev : { ...prev, pageIndex: 0, pageSize: paginationConfig.pageSize });
    }, [paginationConfig.pageSize]);

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

        const state = {
            sorting,
            expanded,
            columnPinning,
            rowPinning: finalRowPinning,
            grouping: viewMode === 'pivot' ? rowFields : [],
            columnVisibility,
            columnSizing
        };
        if (paginationConfig.enabled) {
            state.pagination = paginationState;
        }
        return state;
    }, [
        sorting,
        expanded,
        columnPinning,
        rowPinning,
        rowFields,
        viewMode,
        columnVisibility,
        columnSizing,
        tableData,
        effectiveGrandTotalPinState,
        getStableDataRowId,
        paginationConfig.enabled,
        paginationState,
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
            if (!shouldExpand && viewMode === 'tree' && treeHasDefaultExpansion) {
                const collapsedPaths = {};
                table.getRowModel().rows.forEach((row) => {
                    if (row && row.id && row.getCanExpand() && !(row.original && row.original._isTotal)) {
                        collapsedPaths[row.id] = false;
                    }
                });
                setExpandedWithHistory(collapsedPaths, 'layout:expand-all');
                return;
            }
            setExpandedWithHistory(shouldExpand ? true : {}, 'layout:expand-all');
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

            setExpandedWithHistory(newExpanded, 'layout:expand-all');
        } else {
            // Collapse all by setting empty object
            setExpandedWithHistory({}, 'layout:expand-all');
        }
    };

    const handleSortingChange = (updater) => {
        const rawNextSorting = typeof updater === 'function' ? updater(sorting) : updater;
        const newSorting = normalizeSortingState(rawNextSorting, sorting);
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
            const rawExpanded = typeof updater === 'function' ? updater(expanded) : updater;
            const newExpanded = normalizeExpandedStateForTreeDefaults(rawExpanded, expanded);

            if (serverSide) {
                // Find which path was toggled so we know which block to defer-invalidate
                // after the expansion response lands (see pendingExpansionRef effect).
                // Value-diff: detect any key whose boolean value flipped (covers
                // both key-add/remove AND false→true / true→false toggles).
                const oldExp = expanded || {};
                const newExp = newExpanded || {};
                const allKeys = new Set([...Object.keys(oldExp), ...Object.keys(newExp)]);
                const changedPath = [...allKeys].find((key) => (
                    Object.prototype.hasOwnProperty.call(oldExp, key) !== Object.prototype.hasOwnProperty.call(newExp, key)
                    || !!oldExp[key] !== !!newExp[key]
                ));
                if (changedPath) {
                    const isNowExpanded = !!(newExpanded && newExpanded[changedPath]);
                    setPendingRowTransitions(prev => {
                        const next = new Map(prev);
                        next.set(changedPath, isNowExpanded ? 'expand' : 'collapse');
                        while (next.size > MAX_PENDING_ROW_TRANSITIONS) {
                            const oldestKey = next.keys().next().value;
                            next.delete(oldestKey);
                        }
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

            setExpandedWithHistory(newExpanded, 'layout:expanded');
        },
        onColumnPinningChange: (updater) => { debugLog('onColumnPinningChange'); setColumnPinningWithHistory(updater, 'layout:column-pinning'); },
        onRowPinningChange: (updater) => { debugLog('onRowPinningChange'); setRowPinningWithHistory(updater, 'layout:row-pinning'); },
        onColumnVisibilityChange: (updater) => { debugLog('onColumnVisibilityChange'); setColumnVisibility(updater); },
        onColumnSizingChange: (updater) => { debugLog('onColumnSizingChange'); setColumnSizing(updater); },
        getRowId,
        getCoreRowModel: getCoreRowModel(),
        getExpandedRowModel: serverSide ? undefined : getExpandedRowModel(),
        getGroupedRowModel: serverSide ? undefined : getGroupedRowModel(),
        getPaginationRowModel: paginationConfig.enabled ? getPaginationRowModel() : undefined,
        onPaginationChange: paginationConfig.enabled ? setPaginationState : undefined,
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
        
        setColumnPinningWithHistory(newPinning, 'layout:toggle-all-columns-pinned');
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

    const chartModelingActive = chartPanelOpen
        || chartCanvasPanes.length > 0
        || !!chartModal
        || chartPanelLocked
        || Object.keys(chartPaneDataById).length > 0;
    // NOTE: `table` is intentionally excluded from the dep array — its identity
    // changes every render (useReactTable returns a new object when any state
    // changes).  The actual structural drivers of visible-leaf-columns are the
    // column definitions, visibility map, and pinning map — all listed below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const visibleLeafColumns = useMemo(
        () => table.getVisibleLeafColumns(),
        [columns, columnVisibility, columnPinning, columnSizing]
    );
    const chartDisplayRows = useMemo(
        () => (
            chartModelingActive
                ? [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows]
                : []
        ),
        [chartModelingActive, effectiveTopRows, effectiveCenterRows, effectiveBottomRows]
    );
    const liveSelectionChartModel = useMemo(
        () => (
            !chartModelingActive
                ? null
                : (
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
                )
        ),
        [chartModelingActive, selectedCells, chartDisplayRows, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]
    );
    const livePivotChartModel = useMemo(
        () => (
            !chartModelingActive
                ? null
                : (
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
                )
        ),
        [chartModelingActive, chartDisplayRows, loadedRows, serverSide, visibleLeafColumns, chartPanelType, chartPanelOrientation, chartPanelHierarchyLevel, rowFields.length, chartPanelRowLimit, chartPanelColumnLimit, rowFields, colFields, chartPanelSortMode, chartPanelLayers]
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
        if (chartCanvasPanes.length === 0) return {};
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
        if (!runtimeResponse || typeof runtimeResponse !== 'object') return;
        recordProfilerResponse(runtimeResponse);
        const payload = runtimeResponse.payload && typeof runtimeResponse.payload === 'object'
            ? runtimeResponse.payload
            : {};

        if ((runtimeResponse.kind === 'data' || runtimeResponse.kind === 'update' || runtimeResponse.kind === 'transaction') && runtimeResponse.status === 'data') {
            finalizeTransactionHistoryResponse(runtimeResponse, payload);
            hydrateVisibleEditOverlay(payload.editOverlay);
            reconcileOptimisticCellValuesWithPayload(payload);
            pendingDataProfilerCommitRef.current = {
                requestId: runtimeResponse.requestId || null,
                dataVersion: coerceTransportNumber(payload.dataVersion, null),
                rowCount: coerceTransportNumber(payload.rowCount, null),
            };
            setTransportDataState((previousState) => normalizeRuntimeDataEnvelope(payload, previousState));
            return;
        }

        if (runtimeResponse.kind === 'transaction' && runtimeResponse.status === 'patched') {
            const patchPayload = payload.patch && typeof payload.patch === 'object'
                ? payload.patch
                : {};
            finalizeTransactionHistoryResponse(runtimeResponse, payload);
            hydrateVisibleEditOverlay(payload.editOverlay);
            reconcileOptimisticCellValuesWithPayload({ data: patchPayload.rows || [] });
            pendingDataProfilerCommitRef.current = {
                requestId: runtimeResponse.requestId || null,
                dataVersion: coerceTransportNumber(payload.dataVersion, null),
                rowCount: coerceTransportNumber(payload.rowCount, null),
            };
            setTransportDataState((previousState) => applyRuntimePatchEnvelope({
                ...patchPayload,
                dataOffset: payload.dataOffset,
                dataVersion: payload.dataVersion,
                rowCount: payload.rowCount,
                columns: payload.columns,
            }, previousState));
            if (payload.transaction && payload.transaction.deferredViewportRefresh) {
                scheduleSilentViewportRefresh();
            }
            return;
        }

        if (runtimeResponse.kind === 'transaction' && runtimeResponse.status === 'transaction_applied') {
            finalizeTransactionHistoryResponse(runtimeResponse, payload);
            if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                pivotProfilerRef.current.resolve({
                    requestId: runtimeResponse.requestId,
                    componentId: profilerComponentIdRef.current,
                    kind: 'transaction',
                    status: runtimeResponse.status,
                });
            }
            return;
        }

        if (runtimeResponse.kind === 'filter_options' && runtimeResponse.status === 'ok') {
            const columnId = payload.columnId || payload.column_id;
            if (!columnId) return;
            setTransportFilterOptionsState((previousState) => ({
                ...(previousState || {}),
                [columnId]: Array.isArray(payload.options) ? payload.options : [],
            }));
            setEditorOptionsLoadingState((previousState) => ({
                ...previousState,
                [columnId]: false,
            }));
            emitEditLifecycleEvent({
                kind: 'editor_options_loaded',
                sourceColumnId: columnId,
                optionCount: Array.isArray(payload.options) ? payload.options.length : 0,
            });
            if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                pivotProfilerRef.current.resolve({
                    requestId: runtimeResponse.requestId,
                    componentId: profilerComponentIdRef.current,
                    kind: 'filter_options',
                    status: runtimeResponse.status,
                });
            }
            return;
        }
        if (runtimeResponse.kind === 'filter_options' && runtimeResponse.status === 'error') {
            const columnId = payload.columnId || payload.column_id;
            if (columnId) {
                setEditorOptionsLoadingState((previousState) => ({
                    ...previousState,
                    [columnId]: false,
                }));
            }
            return;
        }

        if (runtimeResponse.kind === 'chart') {
            if (runtimeResponse.status === 'chart_data') {
                setTransportChartDataState(payload);
                if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                    pivotProfilerRef.current.resolve({
                        requestId: runtimeResponse.requestId,
                        componentId: profilerComponentIdRef.current,
                        kind: 'chart',
                        status: runtimeResponse.status,
                    });
                }
                return;
            }
            if (runtimeResponse.status === 'error') {
                activeChartRequestRef.current = null;
                console.error('Chart request failed:', runtimeResponse.message || runtimeResponse);
                if (profilingEnabledRef.current && runtimeResponse.requestId && pivotProfilerRef.current) {
                    pivotProfilerRef.current.resolve({
                        requestId: runtimeResponse.requestId,
                        componentId: profilerComponentIdRef.current,
                        kind: 'chart',
                        status: runtimeResponse.status,
                    });
                }
            }
            return;
        }
        if (runtimeResponse.status === 'stale' || runtimeResponse.status === 'error') {
            clearPendingTransactionHistoryRequest(runtimeResponse, true);
        }
        if (
            profilingEnabledRef.current
            && runtimeResponse.requestId
            && pivotProfilerRef.current
            && (runtimeResponse.status === 'stale' || runtimeResponse.status === 'error')
        ) {
            pivotProfilerRef.current.resolve({
                requestId: runtimeResponse.requestId,
                componentId: profilerComponentIdRef.current,
                kind: runtimeResponse.kind || 'data',
                status: runtimeResponse.status,
            });
        }
    }, [clearPendingTransactionHistoryRequest, emitEditLifecycleEvent, finalizeTransactionHistoryResponse, hydrateVisibleEditOverlay, reconcileOptimisticCellValuesWithPayload, recordProfilerResponse, runtimeResponse]);

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

    useLayoutEffect(() => {
        const pendingCommit = pendingDataProfilerCommitRef.current;
        if (!pendingCommit || !pendingCommit.requestId || !profilingEnabledRef.current || !pivotProfilerRef.current) {
            return undefined;
        }
        if (
            Number.isFinite(pendingCommit.dataVersion)
            && Number.isFinite(Number(dataVersion))
            && Number(dataVersion) < pendingCommit.dataVersion
        ) {
            return undefined;
        }

        pendingDataProfilerCommitRef.current = null;
        if (pendingDataProfilerFrameRef.current !== null && typeof window !== 'undefined' && typeof window.cancelAnimationFrame === 'function') {
            window.cancelAnimationFrame(pendingDataProfilerFrameRef.current);
            pendingDataProfilerFrameRef.current = null;
        }

        const finalizeCommit = () => {
            pivotProfilerRef.current.resolve({
                requestId: pendingCommit.requestId,
                componentId: profilerComponentIdRef.current,
                kind: 'data',
                status: 'data',
                committedAt: Date.now(),
                meta: {
                    dataVersion: Number(dataVersion),
                    rowCount,
                    dataOffset,
                    columnsCount: Array.isArray(responseColumns) ? responseColumns.length : 0,
                },
            });
            pendingDataProfilerFrameRef.current = null;
        };

        if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
            pendingDataProfilerFrameRef.current = window.requestAnimationFrame(finalizeCommit);
            return () => {
                if (pendingDataProfilerFrameRef.current !== null && typeof window.cancelAnimationFrame === 'function') {
                    window.cancelAnimationFrame(pendingDataProfilerFrameRef.current);
                    pendingDataProfilerFrameRef.current = null;
                }
            };
        }

        finalizeCommit();
        return undefined;
    }, [dataOffset, dataVersion, responseColumns, rowCount]);

    useEffect(() => () => {
        if (pendingDataProfilerFrameRef.current !== null && typeof window !== 'undefined' && typeof window.cancelAnimationFrame === 'function') {
            window.cancelAnimationFrame(pendingDataProfilerFrameRef.current);
            pendingDataProfilerFrameRef.current = null;
        }
    }, []);

    const treeHasDefaultExpansion = useMemo(
        () => viewMode === 'tree' && hasTreeDefaultExpansionConfig(treeConfig),
        [treeConfig, viewMode]
    );

    // Detail cache, surface sync, and reset effects provided by useDetailDrillThrough hook

    // Detail response handling effect provided by useDetailDrillThrough hook

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
        emitRuntimeRequest('chart', {
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
        });
    }, [
        abortGeneration,
        chartRequestCandidates,
        emitRuntimeRequest,
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
        centerCols,
        preciseVisibleColRange,
    } = useColumnVirtualizer({
        parentRef,
        table,
        serverSide,
        totalCenterCols,
        columnOverscan: normalizedPerformanceConfig.columnOverscan,
        stateEpoch,
        estimateColumnWidth: defaultColumnWidths.schemaFallback,
        onHorizontalScrollMetrics: handleHorizontalScrollMetrics,
        onPreciseVisibleColRange: (nextRange) => syncPreciseVisibleColRange(nextRange, {
            preserveWiderRange: chartPanelOpen || chartCanvasPanes.length > 0,
        }),
        // Column-structure deps for memoized leaf-column lists
        columnVisibility,
        columnPinning,
        columnSizing,
        columns,
    });
    columnVirtualizerRef.current = columnVirtualizer;
    pinnedColumnMetaRef.current = {
        leftCount: leftCols.length,
        centerCount: centerCols.length,
        rightCount: rightCols.length
    };

    // Memoized lookup structure for the header render path.
    // centerColIndexMap: O(1) id→index lookup; only rebuilt when the column list changes.
    const centerColIndexMap = useMemo(
        () => new Map(centerCols.map((c, i) => [c.id, i])),
        [centerCols]
    );

    // O(1) colId → visible-leaf-index map for renderCell; rebuilt only when column list changes.
    const visibleLeafColIndexMap = useMemo(
        () => new Map(visibleLeafColumns.map((c, i) => [c.id, i])),
        [visibleLeafColumns]
    );

    // Pre-compute header groups so PivotTableBody doesn't need the `table` instance
    // (whose identity changes every render, defeating React.memo).
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const leftHeaderGroups = useMemo(() => table.getLeftHeaderGroups(), [columns, columnVisibility, columnPinning, columnSizing]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const rightHeaderGroups = useMemo(() => table.getRightHeaderGroups(), [columns, columnVisibility, columnPinning, columnSizing]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const centerHeaderGroups = useMemo(() => table.getCenterHeaderGroups(), [columns, columnVisibility, columnPinning, columnSizing]);

    useEffect(() => {
        if (!serverSide || virtualCenterCols.length === 0) return;
        debugLog('horizontal-scroll-state', {
            scrollLeft: parentRef.current ? parentRef.current.scrollLeft : null,
            visibleColRange,
            virtualRange: {
                start: preciseVisibleColRange.start,
                end: preciseVisibleColRange.end,
                count: Math.max(0, preciseVisibleColRange.end - preciseVisibleColRange.start + 1),
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
        preciseVisibleColRange.start,
        preciseVisibleColRange.end,
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


    const matchesValueConfigId = (columnId, valueConfig) => {
        if (!columnId || !valueConfig || !valueConfig.field) return false;
        if (valueConfig.agg === 'formula') {
            return columnId === valueConfig.field || columnId.endsWith(`_${valueConfig.field}`);
        }
        const flatMeasureId = `${valueConfig.field}_${valueConfig.agg}`;
        return columnId === flatMeasureId || columnId.endsWith(`_${valueConfig.field}_${valueConfig.agg}`);
    };

    const getFieldZone = (id) => {
        if (id === 'hierarchy') return 'rows';
        if (rowFields.includes(id)) return 'rows';
        if (colFields.includes(id)) return 'cols';
        if (valConfigs.find(v => matchesValueConfigId(id, v))) return 'vals';
        return null;
    };

    const getFieldIndex = (id, zone) => {
        if (zone === 'rows') return rowFields.indexOf(id);
        if (zone === 'cols') return colFields.indexOf(id);
        if (zone === 'vals') return valConfigs.findIndex(v => matchesValueConfigId(id, v));
        return -1;
    };

    const onHeaderDrop = (e, targetColId) => {
        e.preventDefault();
        if (!dragItem) return;
        const { field, zone: srcZone } = dragItem;
        const draggedValueConfig = typeof field === 'object' && field ? field : null;
        const isFormulaValue = Boolean(draggedValueConfig && draggedValueConfig.agg === 'formula');
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

        if (isFormulaValue) {
            showNotification('Formula columns can be reordered inside Values only.', 'warning');
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
                if (srcZone==='rows') move(rowFields, (next) => setRowFieldsWithHistory(next, 'layout:header-drop'));
                if (srcZone==='cols') move(colFields, (next) => setColFieldsWithHistory(next, 'layout:header-drop'));
                if (srcZone==='vals') move(valConfigs, (next) => setValConfigsWithHistory(next, 'layout:header-drop'));
             }
        } else {
            // Pivoting (Moving between zones)
            const targetIdx = getFieldIndex(targetColId, targetZone);
            // Remove from source
            if (srcZone==='rows') setRowFieldsWithHistory((p) => p.filter(f => f !== fieldName), 'layout:header-drop');
            if (srcZone==='cols') setColFieldsWithHistory((p) => p.filter(f => f !== fieldName), 'layout:header-drop');
            if (srcZone==='vals') setValConfigsWithHistory((p) => p.filter(f => f.field !== fieldName), 'layout:header-drop');

            // Insert into target
            const insert = (list, setList, item) => {
                const n = [...list];
                n.splice(targetIdx, 0, item);
                setList(n);
            };
            if (targetZone==='rows') insert(rowFields, (next) => setRowFieldsWithHistory(next, 'layout:header-drop'), fieldName);
            if (targetZone==='cols') insert(colFields, (next) => setColFieldsWithHistory(next, 'layout:header-drop'), fieldName);
            if (targetZone==='vals') insert(valConfigs, (next) => setValConfigsWithHistory(next, 'layout:header-drop'), {field: fieldName, agg:'sum'});
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
        const draggedValueConfig = typeof field === 'object' && field ? field : null;
        const isFormulaValue = Boolean(draggedValueConfig && draggedValueConfig.agg === 'formula');
        const targetIdx = (dropLine && dropLine.idx) || 0;
        const insertItem = (list, idx, item) => { const n = [...list]; n.splice(idx, 0, item); return n; };
        const fieldName = typeof field === 'string' ? field : field.field;
        if (!fieldName || typeof fieldName !== 'string') { setDragItem(null); setDropLine(null); return; }
        if (isFormulaValue && targetZone !== 'vals') {
            showNotification('Formula columns can only stay in Values.', 'warning');
            setDragItem(null);
            setDropLine(null);
            return;
        }
        if (srcZone !== targetZone) {
            if (srcZone==='rows') setRowFieldsWithHistory((p) => p.filter(f => f !== fieldName), 'layout:zone-drop');
            if (srcZone==='cols') setColFieldsWithHistory((p) => p.filter(f => f !== fieldName), 'layout:zone-drop');
            if (srcZone==='vals') setValConfigsWithHistory((p) => p.filter((_, i) => i !== srcIdx), 'layout:zone-drop');
            if (targetZone==='rows') setRowFieldsWithHistory((p) => (p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName)), 'layout:zone-drop');
            if (targetZone==='cols') setColFieldsWithHistory((p) => (p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName)), 'layout:zone-drop');
            if (targetZone==='vals') setValConfigsWithHistory((p) => insertItem(p, targetIdx, draggedValueConfig || {field: fieldName, agg:'sum'}), 'layout:zone-drop');
            if (targetZone==='filter' && !filters.hasOwnProperty(fieldName)) {
                requestLayoutHistoryCapture('layout:zone-drop');
                setFilters(p => ({ ...p, [fieldName]: '' }));
            }
        } else {
            const move = (list, setList) => {
                const n = [...list]; const [moved] = n.splice(srcIdx, 1);
                let ins = targetIdx; if (srcIdx < targetIdx) ins -= 1;
                n.splice(ins, 0, moved); setList(n);
            };
            if (targetZone==='rows') move(rowFields, (next) => setRowFieldsWithHistory(next, 'layout:zone-drop'));
            if (targetZone==='cols') move(colFields, (next) => setColFieldsWithHistory(next, 'layout:zone-drop'));
            if (targetZone==='vals') move(valConfigs, (next) => setValConfigsWithHistory(next, 'layout:zone-drop'));
        }
        setDragItem(null); setDropLine(null);
    };





    // buildExportAoa and exportPivot extracted to ../utils/exportUtils.js
    const exportPivot = useCallback(() => exportPivotTable(table, rowCount), [table, rowCount]);

    const { renderCell, renderVirtualColumnCell, renderHeaderCell } = useRenderHelpers({
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
        tableRef,
        leftCols,
        centerCols,
        rightCols,
        columnPinning,
        sorting,
        handleHeaderContextMenu,
        autoSizeColumn,
        autoSizeBounds,
        hoveredHeaderIdRef,
        setHoveredHeaderId,
        focusedHeaderIdRef,
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
        resolveEditedCellMarker,
        resolveCellDisplayValue: resolveDisplayedCellValue,
        editedCellEpoch,
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
    const handleClearStatusSelection = useCallback(() => {
        setSelectedCells({});
        setLastSelected(null);
    }, [setLastSelected, setSelectedCells]);

    const handleClearStatusFilters = useCallback(() => {
        setFilters({});
    }, [setFilters]);

    const handleRefreshStatusViewport = useCallback(() => {
        if (!serverSide || typeof requestVisibleViewportRefresh !== 'function') return;
        requestVisibleViewportRefresh();
    }, [requestVisibleViewportRefresh, serverSide]);

    const rowEditStatusSummary = useMemo(() => {
        const sessions = Object.values(rowEditSessions || {});
        return sessions.reduce((summary, session) => {
            if (!session || session.active !== true) return summary;
            summary.activeRowCount += 1;
            if (session.status === 'saving') summary.savingRowCount += 1;
            const dirtyFieldCount = Object.keys(session.drafts || {}).length;
            const invalidFieldCount = Object.keys(session.errors || {}).length;
            if (dirtyFieldCount > 0) {
                summary.dirtyRowCount += 1;
                summary.dirtyFieldCount += dirtyFieldCount;
            }
            if (invalidFieldCount > 0) {
                summary.invalidRowCount += 1;
                summary.invalidFieldCount += invalidFieldCount;
            }
            return summary;
        }, {
            activeRowCount: 0,
            savingRowCount: 0,
            dirtyRowCount: 0,
            dirtyFieldCount: 0,
            invalidRowCount: 0,
            invalidFieldCount: 0,
        });
    }, [rowEditSessions]);

    // Build edited cells list from current selection for the EditSidePanel
    const editPanelCells = useMemo(() => {
        const cells = [];
        const selKeys = Object.keys(selectedCells || {});
        if (selKeys.length === 0) return cells;
        for (const selKey of selKeys) {
            const sepIdx = selKey.lastIndexOf(':');
            if (sepIdx < 0) continue;
            const rowId = selKey.slice(0, sepIdx);
            const colId = selKey.slice(sepIdx + 1);
            const marker = resolveEditedCellMarker(rowId, colId);
            if (!marker) continue;
            const key = getOptimisticCellKey(rowId, colId);
            const origEntry = key ? comparisonCellOriginalValuesRef.current.get(key) : null;
            cells.push({
                rowId,
                colId,
                direct: marker.direct,
                propagated: marker.propagated,
                originalValue: origEntry ? origEntry.value : undefined,
                currentValue: selectedCells[selKey],
            });
        }
        return cells;
    }, [selectedCells, editedCellEpoch, comparisonCellEpoch, resolveEditedCellMarker, getOptimisticCellKey]);

    // Auto-open panel when selected cells include edited ones (only if not already dismissed)
    useEffect(() => {
        if (editPanelCells.length > 0) setEditPanelOpen(true);
    }, [editPanelCells.length]);

    const collectTrackedEventIdsForCells = useCallback((cells, options = {}) => {
        const rawEventIds = [];
        (Array.isArray(cells) ? cells : []).forEach((cell) => {
            if (!cell || typeof cell !== 'object') return;
            rawEventIds.push(...resolveTrackedCellEventIds(cell.rowId, cell.colId, options));
        });
        return sortEventIdsByActiveHistory(rawEventIds);
    }, [resolveTrackedCellEventIds, sortEventIdsByActiveHistory]);

    const collectAllTrackedEventIds = useCallback(() => {
        const rawEventIds = [];
        comparisonCellOriginalValuesRef.current.forEach((entry) => {
            if (!entry || typeof entry !== 'object') return;
            rawEventIds.push(...normalizeTrackedEventIds(entry.directEventIds));
            rawEventIds.push(...normalizeTrackedEventIds(entry.propagatedEventIds));
        });
        return sortEventIdsByActiveHistory(rawEventIds);
    }, [normalizeTrackedEventIds, sortEventIdsByActiveHistory]);

    const handleRevertCell = useCallback((rowId, colId) => {
        const key = getOptimisticCellKey(rowId, colId);
        if (!key) return;
        const targetEventIds = serverSide
            ? resolveTrackedCellEventIds(rowId, colId, { includeDirect: true, includePropagated: true })
            : [];
        if (serverSide && targetEventIds.length > 0) {
            const dispatched = dispatchTransactionRequest({
                eventAction: 'revert',
                eventIds: targetEventIds,
                refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
            }, 'revert');
            if (!dispatched || !dispatched.requestId) return;
            pendingTransactionHistoryRef.current.set(dispatched.requestId, {
                action: 'revert',
                source: 'revert',
                targetEventIds,
                historyEntries: targetEventIds.map((eventId) => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
                successMessage: 'Reverted the selected cell.',
            });
            transactionHistoryPendingRequestRef.current = dispatched.requestId;
            syncTransactionHistoryState();
            return;
        }

        const originalEntry = comparisonCellOriginalValuesRef.current.get(key);
        if (originalEntry) {
            optimisticCellValuesRef.current.delete(key);
            editedCellMarkersRef.current.delete(key);
            comparisonCellCountsRef.current.delete(key);
            comparisonCellOriginalValuesRef.current.delete(key);
            bumpEditedCellEpoch();
            bumpComparisonCellEpoch();
            syncComparisonValueState();
            if (!serverSide && setPropsRef.current && originalEntry.value !== undefined) {
                setPropsRef.current({ cellUpdates: [{ rowId: originalEntry.rowId, colId: originalEntry.colId, value: originalEntry.value }] });
            }
        }
    }, [
        bumpComparisonCellEpoch,
        bumpEditedCellEpoch,
        dispatchTransactionRequest,
        findTransactionHistoryEntryByEventId,
        getOptimisticCellKey,
        resolveTrackedCellEventIds,
        serverSide,
        supportsPatchTransactionRefresh,
        syncComparisonValueState,
        syncTransactionHistoryState,
    ]);

    const handleConfirmPropagation = useCallback(() => {
        const pending = pendingPropagationUpdatesRef.current;
        if (!pending || !pending.updates || pending.updates.length === 0) return;
        const formula = propagationMethodUI || 'equal';
        lastPropagationFormulaRef.current = formula;
        const preparedUpdates = pending.updates.map((update) => {
            if (!isParentAggregatePropagationEdit(update, rowFields)) return update;
            return { ...update, propagationStrategy: formula };
        });
        pendingPropagationUpdatesRef.current = null;
        setPendingPropagationUpdates(null);
        setEditPanelOpen(false);
        if (editValueDisplayMode === 'original') {
            setEditValueDisplayMode('edited');
        }
        if (!serverSide) {
            if (setPropsRef.current) setPropsRef.current({ cellUpdates: preparedUpdates });
            return;
        }
        dispatchServerEditUpdates(preparedUpdates, pending.source, pending.meta || null);
    }, [dispatchServerEditUpdates, editValueDisplayMode, propagationMethodUI, rowFields, serverSide, setEditValueDisplayMode]);

    const handleCancelPropagation = useCallback(() => {
        pendingPropagationUpdatesRef.current = null;
        setPendingPropagationUpdates(null);
        setEditPanelOpen(false);
    }, []);

    const handlePropagationMethodChange = useCallback((method) => {
        setPropagationMethodUI(method);
    }, []);

    const handleReapplyPropagation = useCallback((directCells, method) => {
        if (!Array.isArray(directCells) || directCells.length === 0) return;
        const normalizedMethod = normalizePropagationFormulaValue(method, 'equal') || 'equal';
        if (!serverSide) {
            if (setPropsRef.current) {
                setPropsRef.current({
                    cellUpdates: directCells.map((cell) => ({
                        rowId: cell.rowId,
                        colId: cell.colId,
                        value: cell.currentValue,
                        propagationStrategy: normalizedMethod,
                    })),
                });
            }
            return;
        }
        const targetEventIds = sortEventIdsByActiveHistory(
            directCells.flatMap((cell) => resolveTrackedCellEventIds(cell.rowId, cell.colId, {
                includeDirect: true,
                includePropagated: false,
            }))
        );
        if (targetEventIds.length === 0) {
            showNotification('This edit does not have a tracked server event to replace.', 'warning');
            return;
        }
        const replacementUpdates = directCells.map((cell) => {
            const key = getOptimisticCellKey(cell.rowId, cell.colId);
            const originalEntry = key ? comparisonCellOriginalValuesRef.current.get(key) : null;
            return {
                rowId: cell.rowId,
                colId: cell.colId,
                value: cell.currentValue,
                oldValue: originalEntry ? originalEntry.value : undefined,
                propagationStrategy: normalizedMethod,
            };
        });
        const comparisonPlan = buildComparisonValuePlan(replacementUpdates);
        const editedMarkerPlan = buildEditedCellMarkerPlan(replacementUpdates);
        const dispatched = dispatchTransactionRequest({
            eventAction: 'replace',
            eventIds: targetEventIds,
            propagationStrategy: normalizedMethod,
            refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
        }, 'reapply-propagation');
        if (!dispatched || !dispatched.requestId) return;
        pendingTransactionHistoryRef.current.set(dispatched.requestId, {
            action: 'replace',
            source: 'reapply-propagation',
            targetEventIds,
            historyEntries: targetEventIds.map((eventId) => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
            comparisonPlan,
            editedMarkerPlan,
            successMessage: 'Updated propagation for the selected edit scope.',
        });
        transactionHistoryPendingRequestRef.current = dispatched.requestId;
        syncTransactionHistoryState();
    }, [
        buildComparisonValuePlan,
        buildEditedCellMarkerPlan,
        dispatchTransactionRequest,
        findTransactionHistoryEntryByEventId,
        getOptimisticCellKey,
        resolveTrackedCellEventIds,
        serverSide,
        showNotification,
        sortEventIdsByActiveHistory,
        supportsPatchTransactionRefresh,
        syncTransactionHistoryState,
    ]);

    const handleRevertSelected = useCallback((cells) => {
        if (!Array.isArray(cells) || cells.length === 0) return;
        if (serverSide) {
            const targetEventIds = collectTrackedEventIdsForCells(cells, {
                includeDirect: true,
                includePropagated: true,
            });
            if (targetEventIds.length > 0) {
                const dispatched = dispatchTransactionRequest({
                    eventAction: 'revert',
                    eventIds: targetEventIds,
                    refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
                }, 'revert-selected');
                if (!dispatched || !dispatched.requestId) return;
                pendingTransactionHistoryRef.current.set(dispatched.requestId, {
                    action: 'revert',
                    source: 'revert-selected',
                    targetEventIds,
                    historyEntries: targetEventIds.map((eventId) => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
                    successMessage: `Reverted ${cells.length} selected edit${cells.length === 1 ? '' : 's'}.`,
                });
                transactionHistoryPendingRequestRef.current = dispatched.requestId;
                syncTransactionHistoryState();
                return;
            }
        }
        const revertUpdates = [];
        cells.forEach((cell) => {
            const key = getOptimisticCellKey(cell.rowId, cell.colId);
            if (!key) return;
            const originalEntry = comparisonCellOriginalValuesRef.current.get(key);
            if (originalEntry && originalEntry.value !== undefined) {
                revertUpdates.push({ rowId: originalEntry.rowId, colId: originalEntry.colId, value: originalEntry.value });
            }
            optimisticCellValuesRef.current.delete(key);
            editedCellMarkersRef.current.delete(key);
            comparisonCellCountsRef.current.delete(key);
            comparisonCellOriginalValuesRef.current.delete(key);
        });
        bumpEditedCellEpoch();
        bumpComparisonCellEpoch();
        syncComparisonValueState();
        if (revertUpdates.length > 0) {
            if (serverSide) {
                dispatchTransactionRequest({
                    update: revertUpdates,
                    refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
                }, 'revert-selected-fallback');
            } else if (setPropsRef.current) {
                setPropsRef.current({ cellUpdates: revertUpdates });
            }
        }
    }, [
        bumpComparisonCellEpoch,
        bumpEditedCellEpoch,
        collectTrackedEventIdsForCells,
        dispatchTransactionRequest,
        findTransactionHistoryEntryByEventId,
        getOptimisticCellKey,
        serverSide,
        supportsPatchTransactionRefresh,
        syncComparisonValueState,
        syncTransactionHistoryState,
    ]);

    const handleRevertAll = useCallback(() => {
        const targetEventIds = serverSide ? collectAllTrackedEventIds() : [];
        if (serverSide && targetEventIds.length > 0) {
            const dispatched = dispatchTransactionRequest({
                eventAction: 'revert',
                eventIds: targetEventIds,
                refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
            }, 'revert-all');
            if (!dispatched || !dispatched.requestId) return;
            pendingTransactionHistoryRef.current.set(dispatched.requestId, {
                action: 'revert',
                source: 'revert-all',
                targetEventIds,
                historyEntries: targetEventIds.map((eventId) => findTransactionHistoryEntryByEventId(eventId)).filter(Boolean),
                clearAllOnSuccess: true,
                successMessage: 'Reverted all active edits.',
            });
            transactionHistoryPendingRequestRef.current = dispatched.requestId;
            syncTransactionHistoryState();
            return;
        }
        const revertUpdates = [];
        comparisonCellOriginalValuesRef.current.forEach((entry) => {
            if (!entry || entry.value === undefined) return;
            revertUpdates.push({ rowId: entry.rowId, colId: entry.colId, value: entry.value });
        });
        clearAllOptimisticCellValues();
        clearAllEditedCellMarkers();
        clearAllComparisonValueState();
        propagationLogRef.current = [];
        setPropagationLogEpoch((previousEpoch) => previousEpoch + 1);
        syncTransactionHistoryState();
        if (revertUpdates.length > 0) {
            if (serverSide) {
                dispatchTransactionRequest({
                    update: revertUpdates,
                    refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
                }, 'revert-all-fallback');
            } else if (setPropsRef.current) {
                setPropsRef.current({ cellUpdates: revertUpdates });
            }
        }
    }, [
        clearAllComparisonValueState,
        clearAllEditedCellMarkers,
        clearAllOptimisticCellValues,
        collectAllTrackedEventIds,
        dispatchTransactionRequest,
        findTransactionHistoryEntryByEventId,
        serverSide,
        supportsPatchTransactionRefresh,
        syncTransactionHistoryState,
    ]);

    const statusAccessoryModel = useMemo(() => ({
        selection: {
            selectedCells,
        },
        data: {
            viewMode,
            rowCount: statusRowCount,
            visibleRowsCount: rows.length,
            totalCenterColumns: totalCenterCols,
            rowFieldCount: rowFields.length,
            columnFieldCount: colFields.length,
            measureCount: valConfigs.length,
            sortingCount: Array.isArray(sorting) ? sorting.length : 0,
            activeFilterCount: countActiveFilters(filters),
            globalSearch: typeof (filters && filters.global) === 'string' ? filters.global.trim() : '',
            columnAdvisory,
        },
        runtime: {
            loading: isRequestPending,
            serverSide,
            loadedRowCount: serverSide ? loadedRows.length : rows.length,
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
            propagationLogCount: propagationLogRef.current.length,
        },
        charts: {
            definitionCount: managedChartDefinitions.length,
            paneCount: chartCanvasPanes.length,
            dockedPaneCount: chartCanvasPanes.filter((pane) => !pane.floating).length,
            floatingPaneCount: chartCanvasPanes.filter((pane) => pane.floating).length,
        },
    }), [
        chartCanvasPanes,
        colFields.length,
        columnAdvisory,
        comparisonValueState.activeCount,
        editValueDisplayMode,
        filters,
        isRequestPending,
        loadedRows.length,
        managedChartDefinitions.length,
        rowEditStatusSummary,
        rowFields.length,
        rows.length,
        serverSide,
        sorting,
        statusRowCount,
        totalCenterCols,
        transactionHistoryState.pending,
        transactionHistoryState.redoCount,
        transactionHistoryState.undoCount,
        propagationLogEpoch,
        valConfigs.length,
        viewMode,
        selectedCells,
    ]);

    const statusAccessoryActions = useMemo(() => ({
        canClearSelection: Object.keys(selectedCells || {}).length > 0,
        canClearFilters: countActiveFilters(filters) > 0,
        canRefreshViewport: Boolean(serverSide && !isRequestPending && typeof requestVisibleViewportRefresh === 'function'),
        canCreateSelectionChart: Object.keys(selectedCells || {}).length > 0,
        canUndo: transactionHistoryState.undoCount > 0 && !transactionHistoryState.pending,
        canRedo: transactionHistoryState.redoCount > 0 && !transactionHistoryState.pending,
        canToggleOriginal: comparisonValueState.activeCount > 0,
        canRevertAll: comparisonValueState.activeCount > 0 || editedCellMarkersRef.current.size > 0,
        isShowingOriginal: editValueDisplayMode === 'original' && comparisonValueState.activeCount > 0,
        onClearSelection: handleClearStatusSelection,
        onClearFilters: handleClearStatusFilters,
        onRefreshViewport: handleRefreshStatusViewport,
        onCreateSelectionChart: () => openSelectionChart(),
        onToggleOriginal: () => setEditValueDisplayMode((prev) => prev === 'original' ? 'edited' : 'original'),
        onRevertAll: () => {
            const revertUpdates = [];
            comparisonCellOriginalValuesRef.current.forEach((entry) => {
                if (!entry || entry.value === undefined) return;
                revertUpdates.push({ rowId: entry.rowId, colId: entry.colId, value: entry.value });
            });
            clearAllOptimisticCellValues();
            clearAllEditedCellMarkers();
            clearAllComparisonValueState();
            propagationLogRef.current = [];
            setPropagationLogEpoch((prev) => prev + 1);
            syncTransactionHistoryState();
            if (revertUpdates.length > 0) {
                if (serverSide) {
                    dispatchTransactionRequest({
                        update: revertUpdates,
                        refreshMode: supportsPatchTransactionRefresh ? 'patch' : 'smart',
                    }, 'revert-all');
                } else if (setPropsRef.current) {
                    setPropsRef.current({ cellUpdates: revertUpdates });
                }
            }
            showNotification('Reverted all edits to original values.', 'info');
        },
        onUndo: handleUndo,
        onRedo: handleRedo,
    }), [
        filters,
        handleClearStatusFilters,
        handleClearStatusSelection,
        handleRedo,
        handleRefreshStatusViewport,
        handleUndo,
        isRequestPending,
        openSelectionChart,
        requestVisibleViewportRefresh,
        clearAllComparisonValueState,
        clearAllEditedCellMarkers,
        clearAllOptimisticCellValues,
        comparisonValueState.activeCount,
        editValueDisplayMode,
        selectedCells,
        serverSide,
        showNotification,
        syncTransactionHistoryState,
        dispatchTransactionRequest,
        supportsPatchTransactionRefresh,
        transactionHistoryState.pending,
        transactionHistoryState.redoCount,
        transactionHistoryState.undoCount,
    ]);

    const handleChartCanvasPaneWidthHintChange = useCallback((paneId, nextWidthHint = null) => {
        setChartCanvasPaneWidthHints((previousHints) => {
            const currentValue = Object.prototype.hasOwnProperty.call(previousHints, paneId)
                ? previousHints[paneId]
                : undefined;
            if (!Number.isFinite(Number(nextWidthHint))) {
                if (currentValue === undefined) return previousHints;
                const { [paneId]: _removed, ...rest } = previousHints;
                return rest;
            }
            const normalizedWidth = Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(nextWidthHint)));
            if (currentValue === normalizedWidth) return previousHints;
            return {
                ...previousHints,
                [paneId]: normalizedWidth,
            };
        });
    }, []);

    useEffect(() => {
        const activePaneIds = new Set(chartCanvasPanes.map((pane) => pane.id));
        setChartCanvasPaneWidthHints((previousHints) => {
            const nextHints = Object.entries(previousHints).reduce((acc, [paneId, widthHint]) => {
                if (activePaneIds.has(paneId)) acc[paneId] = widthHint;
                return acc;
            }, {});
            return Object.keys(nextHints).length === Object.keys(previousHints).length ? previousHints : nextHints;
        });
    }, [chartCanvasPanes]);

    const dockedChartCanvasPanes = chartCanvasPanes.filter((pane) => !pane.floating);
    const floatingChartCanvasPanes = chartCanvasPanes.filter((pane) => pane.floating);
    const dockedChartCanvasPanesByPosition = useMemo(() => ({
        left: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'left'),
        right: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'right'),
        top: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'top'),
        bottom: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'bottom'),
    }), [dockedChartCanvasPanes]);

    const renderChartCanvasPane = (pane, dockPosition) => {
        const normalizedDockPosition = normalizeChartDockPosition(dockPosition || pane.dockPosition, 'right');
        const widthHint = Number.isFinite(Number(chartCanvasPaneWidthHints[pane.id]))
            ? Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(chartCanvasPaneWidthHints[pane.id])))
            : null;
        const isVerticalDock = normalizedDockPosition === 'top' || normalizedDockPosition === 'bottom';
        const basePaneStyle = isVerticalDock
            ? {
                display: 'flex',
                width: '100%',
                height: `${Math.max(DEFAULT_DOCKED_CHART_PANE_HEIGHT, Math.floor((pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + 188))}px`,
                minHeight: `${Math.max(280, Math.floor((pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + 120))}px`,
                minWidth: 0,
                overflow: 'hidden',
                borderTop: normalizedDockPosition === 'bottom' ? `1px solid ${theme.border}` : 'none',
                borderBottom: normalizedDockPosition === 'top' ? `1px solid ${theme.border}` : 'none',
                flexShrink: 0,
            }
            : {
                display: 'flex',
                flexGrow: widthHint === null ? pane.size : 0,
                flexBasis: widthHint === null ? 0 : `${widthHint}px`,
                width: widthHint === null ? undefined : `${widthHint}px`,
                minWidth: widthHint === null ? `${MIN_CHART_CANVAS_PANE_WIDTH}px` : `${widthHint}px`,
                minHeight: 0,
                overflow: 'hidden',
                borderLeft: normalizedDockPosition === 'right' ? `1px solid ${theme.border}` : 'none',
                borderRight: normalizedDockPosition === 'left' ? `1px solid ${theme.border}` : 'none',
            };

        return (
            <div
                key={pane.id}
                data-docked-chart-pane={pane.id}
                data-docked-chart-pane-position={normalizedDockPosition}
                style={basePaneStyle}
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
                    immersiveMode={Boolean(pane.immersiveMode)}
                    onImmersiveModeChange={(value) => updateChartCanvasPane(pane.id, { immersiveMode: Boolean(value) })}
                    dockPosition={normalizedDockPosition}
                    onDockPositionChange={(value) => updateChartCanvasPane(pane.id, { dockPosition: normalizeChartDockPosition(value, normalizedDockPosition) })}
                    onSettingsWidthBudgetChange={(nextWidthHint) => handleChartCanvasPaneWidthHintChange(
                        pane.id,
                        normalizedDockPosition === 'left' || normalizedDockPosition === 'right'
                            ? nextWidthHint
                            : null
                    )}
                />
            </div>
        );
    };

    const renderHorizontalDockGroup = (panes, groupPosition) => {
        if (!uiConfig.showCharts) return null;
        if (!Array.isArray(panes) || panes.length === 0) return null;
        return (
            <div
                data-docked-chart-group={groupPosition}
                style={{ display: 'flex', minWidth: 0, minHeight: 0, overflow: 'hidden', flexShrink: 0 }}
            >
                {panes.map((pane, index) => {
                    const previousPane = index > 0 ? panes[index - 1] : null;
                    const nextPane = index < panes.length - 1 ? panes[index + 1] : null;
                    const resizeLeftKey = groupPosition === 'left'
                        ? pane.id
                        : (index === 0 ? 'table' : previousPane.id);
                    const resizeRightKey = groupPosition === 'left'
                        ? (nextPane ? nextPane.id : 'table')
                        : pane.id;
                    const shouldRenderBeforeHandle = groupPosition === 'right';
                    const shouldRenderAfterHandle = groupPosition === 'left';
                    const renderResizeHandle = (leftKey, rightKey) => (
                        <div
                            onMouseDown={(event) => {
                                event.preventDefault();
                                handleStartChartCanvasResize(leftKey, rightKey, event);
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
                    );

                    return (
                        <React.Fragment key={pane.id}>
                            {shouldRenderBeforeHandle ? renderResizeHandle(resizeLeftKey, resizeRightKey) : null}
                            {renderChartCanvasPane(pane, groupPosition)}
                            {shouldRenderAfterHandle ? renderResizeHandle(resizeLeftKey, resizeRightKey) : null}
                        </React.Fragment>
                    );
                })}
            </div>
        );
    };

    const renderVerticalDockGroup = (panes, groupPosition) => {
        if (!uiConfig.showCharts) return null;
        if (!Array.isArray(panes) || panes.length === 0) return null;
        const renderVerticalResizeHandle = (paneId) => (
            <div
                onMouseDown={(event) => {
                    event.preventDefault();
                    handleStartChartCanvasVerticalResize(paneId, groupPosition, event);
                }}
                style={{
                    height: '8px',
                    cursor: 'row-resize',
                    background: 'transparent',
                    position: 'relative',
                    flexShrink: 0,
                    width: '100%',
                }}
                title="Resize chart pane"
            >
                <div style={{
                    position: 'absolute',
                    top: '50%',
                    left: '50%',
                    transform: 'translate(-50%, -50%)',
                    height: '3px',
                    width: '78px',
                    borderRadius: '999px',
                    background: theme.border,
                    opacity: 0.92,
                }} />
            </div>
        );
        return (
            <div
                data-docked-chart-group={groupPosition}
                style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, overflow: 'hidden', flexShrink: 0 }}
            >
                {panes.map((pane) => (
                    <React.Fragment key={pane.id}>
                        {groupPosition === 'bottom' ? renderVerticalResizeHandle(pane.id) : null}
                        {renderChartCanvasPane(pane, groupPosition)}
                        {groupPosition === 'top' ? renderVerticalResizeHandle(pane.id) : null}
                    </React.Fragment>
                ))}
            </div>
        );
    };

    return (
        <div
            id={id}
            style={{ ...styles.root, ...loadingCssVars, position: 'relative', ...style }}
            onMouseEnter={() => setImmersiveModeHovering(true)}
            onMouseLeave={() => setImmersiveModeHovering(false)}
        >
            <style>{loadingAnimationStyles}</style>
            <div style={srOnly} role="status" aria-live="polite">{announcement}</div>
            <PivotThemeProvider theme={theme} styles={styles}>
            <PivotConfigProvider
                filters={filters} setFilters={setFilters}
                pivotMode={pivotMode} setPivotMode={handleSetPivotMode}
                reportDef={reportDef} setReportDef={setReportDef}
                savedReports={savedReports} setSavedReports={setSavedReports}
                activeReportId={activeReportId} setActiveReportId={setActiveReportId}
                showFloatingFilters={showFloatingFilters} setShowFloatingFilters={setShowFloatingFilters}
                stickyHeaders={stickyHeaders} setStickyHeaders={setStickyHeaders}
                showColTotals={showColTotals} setShowColTotals={setShowColTotals}
                showRowTotals={showRowTotals} setShowRowTotals={setShowRowTotals}
                showRowNumbers={showRowNumbers} setShowRowNumbers={setShowRowNumbers}
                numberGroupSeparator={numberGroupSeparator} setNumberGroupSeparator={setNumberGroupSeparator}
                viewMode={viewMode}
            >
            {/* Immersive mode exit button — visible only when immersive mode is active and not locked */}
            {immersiveMode && !uiConfig.lockImmersiveMode && (
                <button
                    data-immersive-exit-overlay
                    onClick={() => setImmersiveMode(false)}
                    title="Exit Immersive Mode"
                    style={{
                        position: 'absolute', top: 12, right: 12, zIndex: 9999,
                        background: 'rgba(0,0,0,0.55)', color: '#fff',
                        border: 'none', borderRadius: 8, padding: '6px 14px',
                        cursor: 'pointer', fontSize: 13, fontWeight: 600,
                        display: 'flex', alignItems: 'center', gap: 6,
                        backdropFilter: 'blur(4px)',
                        opacity: immersiveModeHovering ? 0.92 : 0,
                        pointerEvents: immersiveModeHovering ? 'auto' : 'none',
                        transform: immersiveModeHovering ? 'translateY(0)' : 'translateY(-6px)',
                        transition: 'opacity 0.18s ease, transform 0.18s ease',
                    }}
                >
                    ✕ Exit Immersive
                </button>
            )}
            <PivotValueDisplayProvider
                editValueDisplayMode={editValueDisplayMode}
                resolveCellDisplayValue={resolveDisplayedCellValue}
                resolveCurrentCellValue={resolveCurrentCellValue}
                setEditValueDisplayMode={setEditValueDisplayMode}
                hasComparedValues={comparisonValueState.activeCount > 0}
            >
            {/* PivotAppBar stays outside PivotErrorBoundary so toolbar controls
                remain usable even if the table renderer hits an error state. */}
            <div style={{display:'flex', flexDirection:'column', flex:1, overflow:'hidden', zoom: zoomLevel / 100}}>
            {!immersiveMode && uiConfig.showToolbar && <PivotAppBar
                immersiveMode={immersiveMode} setImmersiveMode={setImmersiveMode}
                sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}
                themeName={themeName} setThemeName={setThemeName}
                themeOverrides={themeOverrides} setThemeOverrides={setThemeOverrides}
                spacingMode={spacingMode} setSpacingMode={setSpacingMode} spacingLabels={spacingLabels}
                layoutMode={layoutMode} setLayoutMode={setLayoutMode}
                onAutoSizeColumns={handleAutoSizeToolbarClick}
                autoSizeIncludesHeaderNext={autoSizeIncludesHeaderNext}
                onTransposePivot={handleTransposePivot}
                canTranspose={rowFields.length > 0 || colFields.length > 0}
                colorScaleMode={colorScaleMode} setColorScaleMode={setColorScaleMode}
                colorPalette={colorPalette} setColorPalette={setColorPalette}
                rowCount={rowCount} exportPivot={exportPivot}
                onSaveView={handleSaveView}
                pivotTitle={props.pivotTitle}
                fontFamily={fontFamily} setFontFamily={setFontFamily}
                fontSize={fontSize} setFontSize={setFontSize}
                zoomLevel={zoomLevel} setZoomLevel={setZoomLevel}
                decimalPlaces={decimalPlaces} setDecimalPlaces={setDecimalPlaces}
                defaultValueFormat={defaultValueFormat} setDefaultValueFormat={setDefaultValueFormat}
                columnDecimalOverrides={columnDecimalOverrides} setColumnDecimalOverrides={setColumnDecimalOverrides}
                columnFormatOverrides={columnFormatOverrides} setColumnFormatOverrides={setColumnFormatOverrides}
                columnGroupSeparatorOverrides={columnGroupSeparatorOverrides} setColumnGroupSeparatorOverrides={setColumnGroupSeparatorOverrides}
                cellFormatRules={cellFormatRules} setCellFormatRules={setCellFormatRules}
                selectedCells={selectedCells}
                selectionValueFormat={selectionValueFormat}
                canApplySelectionValueFormat={canApplySelectionValueFormat}
                onApplySelectionValueFormat={applySelectionValueFormat}
                dataBarsColumns={dataBarsColumns} setDataBarsColumns={setDataBarsColumns}
                canUndoTransactions={transactionHistoryState.undoCount > 0}
                canRedoTransactions={transactionHistoryState.redoCount > 0}
                transactionHistoryPending={transactionHistoryState.pending}
                onUndoTransaction={handleUndo}
                onRedoTransaction={handleRedo}
                editValueDisplayMode={editValueDisplayMode}
                setEditValueDisplayMode={setEditValueDisplayMode}
                hasComparedValues={comparisonValueState.activeCount > 0}
                canCreateSelectionChart={Object.keys(selectedCells || {}).length > 0}
                onCreateSelectionChart={() => openSelectionChart()}
                onAddChartPane={handleAddChartCanvasPane}
                uiConfig={uiConfig}
            />}
        <PivotErrorBoundary>
            <div style={{display:'flex', flex:1, overflow:'hidden', fontFamily: fontFamily, fontSize: fontSize}}>
                {!immersiveMode && uiConfig.showSidebar && sidebarOpen && (
                    <SidebarPanel
                        sidebarTab={sidebarTab} setSidebarTab={setSidebarTab}
                        rowFields={rowFields} setRowFields={setRowFieldsWithHistory}
                        colFields={colFields} setColFields={setColFieldsWithHistory}
                        valConfigs={valConfigs} setValConfigs={setValConfigsWithHistory}
                        columnVisibility={columnVisibility} setColumnVisibility={setColumnVisibility}
                        columnPinning={columnPinning} setColumnPinning={setColumnPinningWithHistory}
                        availableFields={availableFields}
                        table={table}
                        pinningPresets={pinningPresets}
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
                        fieldPanelSizes={fieldPanelSizes}
                        setFieldPanelSizes={setFieldPanelSizes}
                    />
                )}
                <div ref={chartCanvasLayoutRef} style={{ display:'flex', flexDirection:'column', flex:1, minWidth:0, minHeight:0, overflowX:'hidden', overflowY: (dockedChartCanvasPanesByPosition.top.length > 0 || dockedChartCanvasPanesByPosition.bottom.length > 0) ? 'auto' : 'hidden', position:'relative' }}>
                    {uiConfig.showCharts && chartModal && chartModalPosition === 'top' ? (
                        <PivotChartModal
                            chartState={chartModal}
                            onClose={() => setChartModal(null)}
                            theme={theme}
                            position={chartModalPosition}
                            onPositionChange={setChartModalPosition}
                        />
                    ) : null}
                    {renderVerticalDockGroup(dockedChartCanvasPanesByPosition.top, 'top')}
                    <div style={{ display:'flex', flex:1, minWidth:0, minHeight:0, overflow:'hidden', position:'relative' }}>
                        {uiConfig.showCharts && chartModal && chartModalPosition === 'left' ? (
                            <PivotChartModal
                                chartState={chartModal}
                                onClose={() => setChartModal(null)}
                                theme={theme}
                                position={chartModalPosition}
                                onPositionChange={setChartModalPosition}
                            />
                        ) : null}
                        {renderHorizontalDockGroup(dockedChartCanvasPanesByPosition.left, 'left')}
                        <div
                            data-docked-table-canvas
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
                                    key={`pivot-body-${editValueDisplayMode}`}
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
                                    leftHeaderGroups={leftHeaderGroups}
                                    centerHeaderGroups={centerHeaderGroups}
                                    rightHeaderGroups={rightHeaderGroups}
                                    leftCols={leftCols}
                                    centerCols={centerCols}
                                    rightCols={rightCols}
                                    centerColIndexMap={centerColIndexMap}
                                    totalCenterColumns={totalCenterCols}
                                    rowHeight={rowHeight}
                                    layoutMode={layoutMode}
                                    grandTotalPosition={grandTotalPosition}
                                    showColumnLoadingSkeletons={showColumnLoadingSkeletons}
                                    pendingColumnSkeletonCount={activeColumnSkeletonCount}
                                    columnSkeletonWidth={columnSkeletonWidth}
                                    renderCell={renderCell}
                                    renderVirtualColumnCell={renderVirtualColumnCell}
                                    renderHeaderCell={renderHeaderCell}
                                    handleHeaderFilter={handleHeaderFilter}
                                    selectedCells={selectedCells}
                                    rowCount={statusRowCount}
                                    isRequestPending={isRequestPending}
                                    statusModel={statusAccessoryModel}
                                    statusActions={statusAccessoryActions}
                                    treeSuppressGroupRowsSticky={Boolean(viewMode === 'tree' && treeConfig && treeConfig.suppressGroupRowsSticky)}
                                    editValueDisplayMode={editValueDisplayMode}
                                    detailMode={activeDetailDisplayMode}
                                    detailState={activeDetailDisplayMode === 'inline' ? detailSurface : null}
                                    detailInlineHeight={detailConfig.inlineHeight}
                                    onDetailClose={handleDetailClose}
                                    onDetailPageChange={handleDetailPageChange}
                                    onDetailSort={handleDetailSort}
                                    onDetailFilter={handleDetailFilter}
                                />
                                {paginationConfig.enabled && !serverSide && (
                                    <PaginationBar
                                        table={table}
                                        theme={theme}
                                        pageSize={paginationState.pageSize}
                                        onPageSizeChange={(size) => setPaginationState({ pageIndex: 0, pageSize: size })}
                                    />
                                )}
                                {activeDetailDisplayMode === 'drawer' && detailSurface && (
                                    <DetailDrawer
                                        detailState={detailSurface}
                                        onClose={() => setDetailSurface(null)}
                                        onPageChange={handleDetailPageChange}
                                        onSort={handleDetailSort}
                                        onFilter={handleDetailFilter}
                                        theme={theme}
                                        height={detailConfig.drawerHeight}
                                    />
                                )}
                            </div>
                        </div>
                        {activeDetailDisplayMode === 'sidepanel' && detailSurface && (
                            <DetailSidePanel
                                detailState={detailSurface}
                                onClose={() => setDetailSurface(null)}
                                onPageChange={handleDetailPageChange}
                                onSort={handleDetailSort}
                                onFilter={handleDetailFilter}
                                theme={theme}
                                width={detailConfig.sidepanelWidth}
                            />
                        )}
                        {editPanelEnabled && ((editPanelOpen && editPanelCells.length > 0) || pendingPropagationUpdates) ? (
                            <EditSidePanel
                                editedCells={editPanelCells}
                                propagationLog={propagationLogRef.current}
                                onReapplyPropagation={handleReapplyPropagation}
                                onRevertCell={handleRevertCell}
                                onRevertSelected={handleRevertSelected}
                                onRevertAll={handleRevertAll}
                                onToggleDisplayMode={() => setEditValueDisplayMode((p) => p === 'original' ? 'edited' : 'original')}
                                displayMode={editValueDisplayMode}
                                onClose={() => { setEditPanelOpen(false); handleCancelPropagation(); }}
                                theme={theme}
                                width={320}
                                pendingPropagation={pendingPropagationUpdates}
                                propagationMethod={propagationMethodUI}
                                onPropagationMethodChange={handlePropagationMethodChange}
                                onConfirmPropagation={handleConfirmPropagation}
                                onCancelPropagation={handleCancelPropagation}
                            />
                        ) : null}
                        {renderHorizontalDockGroup(dockedChartCanvasPanesByPosition.right, 'right')}
                        {uiConfig.showCharts && chartModal && chartModalPosition === 'right' ? (
                            <PivotChartModal
                                chartState={chartModal}
                                onClose={() => setChartModal(null)}
                                theme={theme}
                                position={chartModalPosition}
                                onPositionChange={setChartModalPosition}
                            />
                        ) : null}
                    </div>
                    {renderVerticalDockGroup(dockedChartCanvasPanesByPosition.bottom, 'bottom')}
                    {uiConfig.showCharts && chartModal && chartModalPosition === 'bottom' ? (
                        <PivotChartModal
                            chartState={chartModal}
                            onClose={() => setChartModal(null)}
                            theme={theme}
                            position={chartModalPosition}
                            onPositionChange={setChartModalPosition}
                        />
                    ) : null}
                    {uiConfig.showCharts && floatingChartCanvasPanes.map((pane) => (
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
                            immersiveMode={Boolean(pane.immersiveMode)}
                            onImmersiveModeChange={(value) => updateChartCanvasPane(pane.id, { immersiveMode: Boolean(value) })}
                        />
                    ))}
                </div>
            </div>
            {contextMenu && <ContextMenu {...contextMenu} theme={theme} onClose={() => setContextMenu(null)} />}
            {notification && <Notification message={notification.message} type={notification.type} onClose={() => setNotification(null)} />}
            {sparklineDataModal && (
                <div
                    style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 10000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                    onClick={() => setSparklineDataModal(null)}
                >
                    <div
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background: theme.background,
                            border: `1px solid ${theme.border}`,
                            borderRadius: '12px',
                            boxShadow: '0 8px 32px rgba(0,0,0,0.22)',
                            minWidth: '300px',
                            maxWidth: '520px',
                            maxHeight: '70vh',
                            display: 'flex',
                            flexDirection: 'column',
                            fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                            overflow: 'hidden',
                        }}
                    >
                        {/* Modal header */}
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 18px 10px', borderBottom: `1px solid ${theme.border}` }}>
                            <span style={{ fontSize: '13px', fontWeight: 700, color: theme.text }}>
                                {sparklineDataModal.headerLabel || 'Trend Data'}
                            </span>
                            <button
                                type="button"
                                onClick={() => setSparklineDataModal(null)}
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textSec, fontSize: '18px', lineHeight: 1, padding: '2px 4px' }}
                            >×</button>
                        </div>
                        {/* Data table */}
                        <div style={{ overflow: 'auto', flex: 1 }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                                <thead>
                                    <tr style={{ background: theme.headerBg || theme.headerSubtleBg }}>
                                        <th style={{ textAlign: 'left', padding: '7px 14px', color: theme.textSec, fontWeight: 600, fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: `1px solid ${theme.border}` }}>Period</th>
                                        <th style={{ textAlign: 'right', padding: '7px 14px', color: theme.textSec, fontWeight: 600, fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: `1px solid ${theme.border}` }}>Value</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {(sparklineDataModal.points || []).map((point, i) => (
                                        <tr key={i} style={{ borderBottom: `1px solid ${theme.border}`, background: i % 2 === 0 ? 'transparent' : (theme.headerSubtleBg || 'rgba(0,0,0,0.02)') }}>
                                            <td style={{ padding: '6px 14px', color: theme.textSec }}>{point.label}</td>
                                            <td style={{ padding: '6px 14px', textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontWeight: 600, color: theme.text }}>{typeof point.value === 'number' ? point.value.toLocaleString(undefined, { maximumFractionDigits: 6 }) : point.value}</td>
                                        </tr>
                                    ))}
                                    {(sparklineDataModal.points || []).length === 0 && (
                                        <tr><td colSpan={2} style={{ padding: '16px 14px', color: theme.textSec, textAlign: 'center' }}>No data</td></tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                        {/* Footer summary */}
                        {(sparklineDataModal.points || []).length > 0 && (
                            <div style={{ borderTop: `1px solid ${theme.border}`, padding: '8px 14px', display: 'flex', gap: '16px', fontSize: '11px', color: theme.textSec, background: theme.headerSubtleBg || theme.headerBg }}>
                                <span>{(sparklineDataModal.points || []).length} points</span>
                                <span>Min: <b style={{ color: theme.text }}>{Math.min(...(sparklineDataModal.points || []).map(p => p.value)).toLocaleString(undefined, { maximumFractionDigits: 4 })}</b></span>
                                <span>Max: <b style={{ color: theme.text }}>{Math.max(...(sparklineDataModal.points || []).map(p => p.value)).toLocaleString(undefined, { maximumFractionDigits: 4 })}</b></span>
                                <span>Last: <b style={{ color: theme.primary }}>{(sparklineDataModal.points || [])[(sparklineDataModal.points || []).length - 1]?.value?.toLocaleString(undefined, { maximumFractionDigits: 4 })}</b></span>
                            </div>
                        )}
                    </div>
                </div>
            )}
        </PivotErrorBoundary>
            </div>{/* end zoom wrapper */}
            </PivotValueDisplayProvider>
            </PivotConfigProvider>
            </PivotThemeProvider>
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
            formulaRef: PropTypes.string,
            sparkline: PropTypes.oneOfType([PropTypes.bool, PropTypes.object]),
        })),
        filters: PropTypes.object,
        sorting: PropTypes.array,
        expanded: PropTypes.oneOfType([PropTypes.object, PropTypes.bool]),
    
    immersiveMode: PropTypes.bool,
    showRowTotals: PropTypes.bool,
    showColTotals: PropTypes.bool,
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
    chartCanvasPanes: PropTypes.arrayOf(PropTypes.object),
    tableCanvasSize: PropTypes.number,
    performanceConfig: PropTypes.shape({
        cacheBlockSize: PropTypes.number,
        maxBlocksInCache: PropTypes.number,
        blockLoadDebounceMs: PropTypes.number,
        rowOverscan: PropTypes.number,
        columnOverscan: PropTypes.number,
        prefetchColumns: PropTypes.number,
    }),
    chartServerWindow: PropTypes.shape({
        enabled: PropTypes.bool,
        rows: PropTypes.number,
        columns: PropTypes.number,
        scope: PropTypes.oneOf(['viewport', 'root']),
    }),
    cellUpdate: PropTypes.object,
    cellUpdates: PropTypes.arrayOf(PropTypes.object),
    rowMove: PropTypes.object,
    drillEndpoint: PropTypes.string,
    viewState: PropTypes.object,
    saveViewTrigger: PropTypes.any,
    savedView: PropTypes.object,
    conditionalFormatting: PropTypes.arrayOf(PropTypes.object),
    validationRules: PropTypes.object,
    editingConfig: PropTypes.object,
    editLifecycleEvent: PropTypes.object,

    /**
     * Persisted edit state: edited cell markers, comparison original values, display mode,
     * and propagation event log. Round-tripped via setProps so the Dash backend can
     * save/restore/modify it.
     */
    editState: PropTypes.object,
    uiConfig: PropTypes.object,
    /**
     * Client-side pagination configuration: { enabled: true, pageSize: 50 }.
     * Only applies when serverSide is false.
     */
    paginationConfig: PropTypes.object,
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
    sortEvent: PropTypes.object,
    availableFieldList: PropTypes.arrayOf(PropTypes.string),
    reportDef: PropTypes.object,
    savedReports: PropTypes.arrayOf(PropTypes.object),
    activeReportId: PropTypes.string,
};

/**
 * Pivot Normalization Utilities
 * 
 * Pure utility functions for normalizing and validating pivot table inputs.
 * These functions have no React dependencies and can be used anywhere.
 * 
 * Extracted from DashTanstackPivot.react.js (Phase 1 of refactoring)
 */

import { formatDisplayLabel, formatAggLabel } from '../utils/helpers';

// ─── Chart Constants ───────────────────────────────────────────────────────

export const DEFAULT_CHART_PANEL_ROW_LIMIT = 50;
export const DEFAULT_CHART_PANEL_COLUMN_LIMIT = 10;
export const DEFAULT_CHART_GRAPH_HEIGHT = 320;
export const DEFAULT_FLOATING_CHART_PANEL_HEIGHT = 520;
export const MIN_CHART_PANEL_WIDTH = 280;
export const MAX_CHART_PANEL_WIDTH = 960;
export const MIN_FLOATING_CHART_PANEL_HEIGHT = 280;
export const MIN_TABLE_PANEL_WIDTH = 0;
export const MIN_CHART_CANVAS_PANE_WIDTH = 320;
export const DEFAULT_DOCKED_CHART_PANE_HEIGHT = 420;
export const MIN_DOCKED_CHART_PANE_HEIGHT = 180;
export const MIN_TABLE_PANEL_HEIGHT = 200;
export const VALID_CHART_DOCK_POSITIONS = new Set(['left', 'right', 'top', 'bottom']);
export const DEFAULT_TABLE_CANVAS_SIZE = 1.4;
export const TABLE_OVERLAY_CHART_PANE_ID = '__table_overlay_chart__';
export const MAX_AUTO_SIZE_SAMPLE_ROWS = 300;
export const MAX_PENDING_ROW_TRANSITIONS = 128;
export const VALID_CHART_TYPES = new Set(['bar', 'line', 'area', 'sparkline', 'combo', 'pie', 'donut', 'scatter', 'waterfall', 'icicle', 'sunburst', 'sankey']);
export const VALID_CHART_SORT_MODES = new Set(['natural', 'value_desc', 'value_asc', 'label_asc', 'label_desc']);
export const VALID_CHART_INTERACTION_MODES = new Set(['focus', 'filter', 'event']);
export const VALID_CHART_SERVER_SCOPES = new Set(['viewport', 'root']);

// ─── View/Detail Mode Constants ────────────────────────────────────────────

export const VALID_VIEW_MODES = new Set(['pivot', 'report', 'tree', 'table']);
export const VALID_DETAIL_MODES = new Set(['none', 'inline', 'sidepanel', 'drawer']);
export const VALID_TREE_DISPLAY_MODES = new Set(['singleColumn', 'multipleColumns']);
export const VALID_DETAIL_REFRESH_STRATEGIES = new Set(['rows', 'everything', 'nothing']);
export const VALID_TRANSACTION_REFRESH_MODES = new Set(['none', 'viewport', 'smart', 'structural', 'full', 'patch']);
export const VALID_TRANSACTION_EVENT_ACTIONS = new Set(['undo', 'redo', 'revert', 'replace']);
export const MAX_TRANSACTION_HISTORY_ENTRIES = 100;
export const SOFT_CENTER_COLUMN_WARNING_THRESHOLD = 2000;
export const HARD_CENTER_COLUMN_WARNING_THRESHOLD = 10000;
export const SUPPORTED_DEFAULT_NUMBER_FORMATS = new Set(['', 'currency', 'accounting', 'percent', 'scientific']);
export const GRAND_TOTAL_ROW_ID = '__grand_total__';
export const MISSING_PERSISTED_VALUE = Symbol('missing-persisted-value');

// ─── Chart Utilities ───────────────────────────────────────────────────────

export const getPreferredChartOrientation = (columnFields) => (
    Array.isArray(columnFields) && columnFields.length > 0 ? 'columns' : 'rows'
);

export const sanitizeChartDefinitionName = (value, fallback = 'Chart') => {
    const text = typeof value === 'string' ? value.trim() : '';
    return text || fallback;
};

export const createChartDefinitionId = (prefix = 'chart') => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

export const cloneSerializable = (value, fallback = null) => {
    if (value === undefined) return fallback;
    try {
        return JSON.parse(JSON.stringify(value));
    } catch (error) {
        return fallback;
    }
};

// ─── View/Detail Mode Normalization ────────────────────────────────────────

export const normalizeViewModeValue = (value) => (
    VALID_VIEW_MODES.has(value) ? value : null
);

export const normalizeDetailModeValue = (value) => (
    VALID_DETAIL_MODES.has(value) ? value : null
);

export const normalizeTreeSourceTypeValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (normalized === 'path' || normalized === 'data_path' || normalized === 'datapath') return 'path';
    if (normalized === 'parentid' || normalized === 'parent_id' || normalized === 'adjacency') return 'adjacency';
    if (normalized === 'nested' || normalized === 'children') return 'nested';
    return 'adjacency';
};

export const normalizeTreeDisplayModeValue = (value) => {
    if (typeof value !== 'string') return 'singleColumn';
    const normalized = value.trim().toLowerCase();
    if (normalized === 'multiplecolumns' || normalized === 'multiple_columns' || normalized === 'multiple' || normalized === 'tabular' || normalized === 'outline') {
        return 'multipleColumns';
    }
    return 'singleColumn';
};

export const normalizeTreeGroupDefaultExpandedValue = (value) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return 0;
    const rounded = Math.floor(normalized);
    if (rounded < -1) return -1;
    return rounded;
};

export const normalizeTreeDefaultOpenPathsValue = (value) => {
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

export const normalizeTreeLevelLabelsValue = (value) => (
    Array.isArray(value)
        ? value
            .map((label) => (typeof label === 'string' ? label.trim() : ''))
            .filter(Boolean)
        : []
);

export const normalizeDetailRefreshStrategyValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_DETAIL_REFRESH_STRATEGIES.has(normalized) ? normalized : 'rows';
};

// ─── Transaction Normalization ─────────────────────────────────────────────

export const hasTransactionEntries = (transaction) => (
    Boolean(transaction && typeof transaction === 'object')
    && ['add', 'remove', 'update', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0)
);

export const normalizeTransactionRefreshModeValue = (value, fallback = 'smart') => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_REFRESH_MODES.has(normalized) ? normalized : fallback;
};

export const normalizeTransactionEventActionValue = (value) => {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
    return VALID_TRANSACTION_EVENT_ACTIONS.has(normalized) ? normalized : null;
};

export const normalizePropagationFormulaValue = (value, fallback = 'equal') => {
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

export const isParentAggregatePropagationEdit = (update, groupingFields) => {
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

export const normalizeTransactionHistoryPayload = (transaction, overrides = {}) => {
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

// ─── Transaction Helpers ───────────────────────────────────────────────────

export const cellValuesMatch = (expectedValue, actualValue) => {
    if (Object.is(expectedValue, actualValue)) return true;
    const expectedNumber = Number(expectedValue);
    const actualNumber = Number(actualValue);
    if (Number.isFinite(expectedNumber) && Number.isFinite(actualNumber)) {
        return expectedNumber === actualNumber;
    }
    return String(expectedValue) === String(actualValue);
};

export const hasAppliedTransactionWork = (transactionResult) => (
    Boolean(transactionResult && typeof transactionResult === 'object')
    && Object.values((transactionResult.applied && typeof transactionResult.applied === 'object') ? transactionResult.applied : {})
        .some((count) => Number(count) > 0)
);

export const shouldShowTransactionLoading = (transaction) => {
    if (!transaction || typeof transaction !== 'object') return true;
    const refreshMode = normalizeTransactionRefreshModeValue(transaction.refreshMode, 'smart');
    if (refreshMode === 'structural' || refreshMode === 'full' || refreshMode === 'smart_structural') {
        return true;
    }
    return ['add', 'remove', 'upsert'].some((kind) => Array.isArray(transaction[kind]) && transaction[kind].length > 0);
};

export const describeTransactionPropagation = (entry) => {
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

export const isEditableKeyboardTarget = (target) => {
    if (!target || typeof target !== 'object') return false;
    const tagName = typeof target.tagName === 'string' ? target.tagName.toUpperCase() : '';
    return Boolean(
        target.isContentEditable
        || tagName === 'INPUT'
        || tagName === 'TEXTAREA'
        || tagName === 'SELECT'
    );
};

// ─── Tree Config Normalization ─────────────────────────────────────────────

export const normalizeTreeConfigValue = (value) => {
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

// ─── Detail Config Normalization ───────────────────────────────────────────

export const clampOptionalInteger = (value, min, max) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return null;
    return Math.max(min, Math.min(max, Math.floor(normalized)));
};

export const normalizeDetailConfigValue = (value) => {
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

export const hasTreeDefaultExpansionConfig = (treeConfig) => {
    if (!treeConfig || typeof treeConfig !== 'object') return false;
    if (Number.isFinite(Number(treeConfig.groupDefaultExpanded)) && Number(treeConfig.groupDefaultExpanded) !== 0) {
        return true;
    }
    return Array.isArray(treeConfig.defaultOpenPaths) && treeConfig.defaultOpenPaths.length > 0;
};

// ─── Report Normalization ──────────────────────────────────────────────────

export const normalizeLegacyPivotModeValue = (value) => (
    value === 'report' || value === 'pivot' ? value : null
);

export const normalizeReportTopN = (value) => (
    Number.isFinite(Number(value)) && Number(value) > 0
        ? Math.floor(Number(value))
        : null
);

export const normalizeReportNodeValue = (value) => {
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
            normalizedBase.childrenByValue = normalizedChildrenByValue;
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

export const convertLegacyLevelsToReportNode = (levels, index = 0, overrideRule = null) => {
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

export const normalizeReportDefValue = (value) => {
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

export const collectReportFields = (reportDef) => {
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

export const countReportNodes = (reportDef) => {
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

export const getReportHeaderLabel = (reportDef) => {
    const root = reportDef && typeof reportDef === 'object' ? reportDef.root : null;
    if (!root || typeof root !== 'object') return 'Report';
    if (root.label && root.label.trim()) return root.label.trim();
    if (root.field && root.field.trim()) return formatDisplayLabel(root.field);
    return 'Report';
};

export const normalizeSavedReportsValue = (value) => (
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

export const normalizeActiveReportIdValue = (value) => (
    typeof value === 'string' && value.trim() ? value : null
);

// ─── Chart Definition Normalization ────────────────────────────────────────

export const clampFloatingChartRect = (rect, containerRect) => {
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

export const normalizeChartDefinition = (value, fallback = {}) => {
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

export const sanitizeChartDefinitions = (definitions, fallbackDefinition) => {
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

// ─── Chart Column Utilities ────────────────────────────────────────────────

export const serializeChartColumn = (column) => {
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

export const serializeChartColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => serializeChartColumn(column)).filter(Boolean)
        : []
);

export const getRequestedChartSeriesColumnIds = (chartType, chartLayers) => (
    chartType === 'combo'
        ? Array.from(new Set(
            (Array.isArray(chartLayers) ? chartLayers : [])
                .map((layer) => (layer && typeof layer.columnId === 'string' ? layer.columnId.trim() : ''))
                .filter(Boolean)
        ))
        : []
);

export const normalizeChartResponseColumn = (column, fallbackId = null) => {
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

export const normalizeChartResponseColumns = (columns) => (
    Array.isArray(columns)
        ? columns.map((column) => normalizeChartResponseColumn(column)).filter(Boolean)
        : []
);

export const buildChartColumnsFromSchema = (colSchema) => (
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

export const resolveChartModelColumns = (chartDataEntry, fallbackColumns = []) => {
    const responseColumns = normalizeChartResponseColumns(chartDataEntry && chartDataEntry.columns);
    return responseColumns.length > 0 ? responseColumns : serializeChartColumns(fallbackColumns);
};

export const resolveChartAvailableColumns = (chartDataEntry, fallbackColumns = []) => {
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

// ─── Transport Data Normalization ──────────────────────────────────────────

export const coerceTransportNumber = (value, fallback = null) => {
    if (value === null || value === undefined || value === '') return fallback;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
};

export const isColSchemaTransportColumn = (column) => {
    const columnId = column && typeof column === 'object' ? column.id : column;
    return String(columnId || '').trim() === '__col_schema';
};

export const extractColSchemaFromTransportColumns = (columns) => {
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

export const normalizeTransportColumns = (columns, fallbackColumns = []) => {
    const sourceColumns = Array.isArray(columns)
        ? columns
        : (Array.isArray(fallbackColumns) ? fallbackColumns : []);
    return sourceColumns.filter((column) => !isColSchemaTransportColumn(column));
};

export const normalizeRuntimeDataEnvelope = (payload, fallback = {}) => {
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

export const resolveTransportRowId = (row) => {
    if (!row || typeof row !== 'object') return null;
    if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total' || row.__isGrandTotal__) {
        return '__grand_total__';
    }
    if (row._path !== undefined && row._path !== null && row._path !== '') return String(row._path);
    if (row.id !== undefined && row.id !== null && row.id !== '') return String(row.id);
    if (row._id !== undefined && row._id !== null && row._id !== '') return String(row._id);
    return null;
};

export const applyRuntimePatchEnvelope = (patch, fallback = {}) => {
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

// ─── Chart Canvas/Pane Normalization ───────────────────────────────────────

export const normalizeLockedChartRequest = (value) => {
    if (!value || typeof value !== 'object') return null;
    return {
        request: value.request && typeof value.request === 'object' ? cloneSerializable(value.request, null) : null,
        stateOverride: value.stateOverride && typeof value.stateOverride === 'object' ? cloneSerializable(value.stateOverride, null) : null,
        visibleColumns: Array.isArray(value.visibleColumns) ? cloneSerializable(value.visibleColumns, []) : [],
        requestSignature: typeof value.requestSignature === 'string' ? value.requestSignature : null,
    };
};

export const normalizeChartDockPosition = (value, fallback = 'right') => (
    VALID_CHART_DOCK_POSITIONS.has(value) ? value : fallback
);

export const normalizeChartCanvasPane = (value, fallbackDefinition, index = 0) => {
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

export const sanitizeChartCanvasPanes = (panes, fallbackDefinition) => (
    Array.isArray(panes)
        ? panes
            .filter((pane) => pane && typeof pane === 'object')
            .map((pane, index) => normalizeChartCanvasPane(pane, fallbackDefinition, index))
        : []
);

export const getChartPanelWidthBounds = (layoutWidth) => {
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

export const normalizeChartServerWindowConfig = (value) => {
    if (!value || typeof value !== 'object') {
        return { enabled: false, rows: null, columns: null, scope: 'viewport' };
    }
    return {
        enabled: value.enabled !== false,
        rows: Number.isFinite(Number(value.rows)) ? Math.max(1, Math.floor(Number(value.rows))) : null,
        columns: Number.isFinite(Number(value.columns)) ? Math.max(1, Math.floor(Number(value.columns))) : null,
        scope: VALID_CHART_SERVER_SCOPES.has(value.scope) ? value.scope : 'viewport',
    };
};

// ─── Session/ID Utilities ──────────────────────────────────────────────────

export const getOrCreateSessionId = (componentId = 'pivot-grid') => {
    if (typeof window === 'undefined') return `${componentId}-anonymous`;
    const storageKey = `pivot-session-${componentId}`;
    try {
        const existing = window.sessionStorage.getItem(storageKey);
        if (existing) return existing;
    } catch (_ignored) { /* storage unavailable */ }
    const newId = `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    try {
        window.sessionStorage.setItem(storageKey, newId);
    } catch (_ignored) { /* storage full or unavailable */ }
    return newId;
};

export const createClientInstanceId = (componentId = 'pivot-grid') => (
    `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
);

// ─── UI Helper Utilities ───────────────────────────────────────────────────

export const clampSidebarWidth = (value, fallback = 288) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return fallback;
    return Math.max(200, Math.min(600, Math.floor(normalized)));
};

export const clampDecimalPlaces = (value, fallback = 0) => {
    const normalized = Number(value);
    if (!Number.isFinite(normalized)) return fallback;
    return Math.max(0, Math.min(6, Math.floor(normalized)));
};

export const normalizeDefaultValueFormat = (value) => {
    const normalized = typeof value === 'string' ? value.trim() : '';
    if (normalized.startsWith('currency:') || normalized.startsWith('accounting:')) return normalized;
    return SUPPORTED_DEFAULT_NUMBER_FORMATS.has(normalized) ? normalized : '';
};

export const mergeSparseColSchema = (previousSchema, incomingSchema, fallbackSize = 140) => {
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

export const isSparseSchemaRangeLoaded = (schema, start, end) => {
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

export const normalizePerformanceConfigValue = (value) => {
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

// ─── Advisory/Filter Utilities ─────────────────────────────────────────────

export const buildLargeColumnAdvisory = ({ serverSide, totalCenterCols, colFields }) => {
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

export const hasActiveFilterValue = (value) => {
    if (value === null || value === undefined) return false;
    if (typeof value === 'string') return value.trim().length > 0;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'object') {
        return Object.keys(value).length > 0;
    }
    return true;
};

export const countActiveFilters = (filters) => {
    if (!filters || typeof filters !== 'object') return 0;
    return Object.values(filters).reduce(
        (count, value) => count + (hasActiveFilterValue(value) ? 1 : 0),
        0
    );
};

// ─── Row Pinning Utilities ─────────────────────────────────────────────────

export const normalizeRowPinningState = (value) => ({
    top: Array.isArray(value && value.top) ? value.top : [],
    bottom: Array.isArray(value && value.bottom) ? value.bottom : [],
});

export const getPinnedSideForRow = (rowPinning, rowId) => {
    const normalized = normalizeRowPinningState(rowPinning);
    if (normalized.top.includes(rowId)) return 'top';
    if (normalized.bottom.includes(rowId)) return 'bottom';
    return null;
};

export const sanitizeGrandTotalPinOverride = (value) => (
    value === 'top' || value === 'bottom' || value === false ? value : null
);

export const resolveGrandTotalPinState = (showColTotals, grandTotalPosition, grandTotalPinOverride) => {
    if (!showColTotals) return false;
    if (grandTotalPinOverride === false || grandTotalPinOverride === 'top' || grandTotalPinOverride === 'bottom') {
        return grandTotalPinOverride;
    }
    return grandTotalPosition === 'bottom' ? 'bottom' : 'top';
};

export const applyRowPinning = (rowPinning, rowId, pinState) => {
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

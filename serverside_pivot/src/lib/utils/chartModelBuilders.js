/**
 * Chart model building utilities — pure functions, no React.
 * Extracted from PivotCharts.js for separation of concerns.
 */
import { formatDisplayLabel } from './helpers';
/** @type {any} */
export const INTERNAL_COL_IDS = new Set(['hierarchy', '__row_number__']);
export const MAX_PANEL_CATEGORIES = 50;
export const MAX_PANEL_SERIES = 12;
export const MAX_SELECTION_CATEGORIES = 50;
export const MAX_SELECTION_SERIES = 12;
export const CHART_HEIGHT = 320;
export const CHART_WIDTH = 760;

/** @type {any[]} */
export const DEFAULT_COLORS = ['#2563EB', '#F97316', '#0F766E', '#7C3AED', '#DC2626', '#0891B2'];
/** @type {Record<string, any>} */
export const COLOR_PALETTES = {
    default:  ['#2563EB', '#F97316', '#0F766E', '#7C3AED', '#DC2626', '#0891B2'],
    pastel:   ['#93C5FD', '#FDBA74', '#6EE7B7', '#C4B5FD', '#FCA5A5', '#67E8F9'],
    bold:     ['#1D4ED8', '#EA580C', '#047857', '#7E22CE', '#B91C1C', '#0E7490'],
    earth:    ['#92400E', '#78716C', '#166534', '#854D0E', '#9F1239', '#1E3A5F'],
    ocean:    ['#0284C7', '#0D9488', '#2563EB', '#0891B2', '#4F46E5', '#7C3AED'],
    warm:     ['#DC2626', '#EA580C', '#F97316', '#EAB308', '#D97706', '#CA8A04'],
    cool:     ['#2563EB', '#0891B2', '#0D9488', '#059669', '#7C3AED', '#4F46E5'],
    mono:     ['#1E293B', '#475569', '#64748B', '#94A3B8', '#CBD5E1', '#334155'],
    a11y:     ['#0077BB', '#EE7733', '#009988', '#CC3311', '#33BBEE', '#EE3377'],
};
/** @type {any} */
export const COLOR_PALETTE_NAMES = Object.keys(COLOR_PALETTES);
/** @param {any} [name] @returns {any} */
export const resolvePalette = (name) => COLOR_PALETTES[name] || DEFAULT_COLORS;
/** @type {any} */
export const VALID_COMBO_LAYER_TYPES = new Set(['bar', 'line', 'area']);
/** @type {any} */
export const VALID_COMBO_LAYER_AXES = new Set(['left', 'right']);
/** @type {any[]} */
export const DEFAULT_COMBO_LAYER_SEQUENCE = ['bar', 'line', 'area'];
/** @param {any} [value] @param {any} [fallback] @returns {any} */
export const normalizePositiveLimit = (value, fallback) => {
    /** @type {any} */
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue) || numericValue < 1) return fallback;
    return Math.max(1, Math.floor(numericValue));
};

/** @param {any} [value] @returns {any} */
export const parseNumeric = (value) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    /** @type {any} */
    const trimmed = value.trim();
    if (!trimmed) return null;
    /** @type {any} */
    const normalized = trimmed.replace(/,/g, '').replace(/%$/, '');
    /** @type {any} */
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
};

/** @param {any} [value] @returns {any} */
export const stringifyValue = (value) => {
    if (value === null || value === undefined) return '';
    return String(value);
};

/** @param {any} [value] @returns {any} */
export const formatChartNumber = (value) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '';
    /** @type {any} */
    const decimals = Math.abs(value) >= 100 ? 0 : 2;
    /** @type {any} */
    const fixed = value.toFixed(decimals).replace(/\.00$/, '');
    /** @type {any} */
    const parts = fixed.split('.');
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    return parts.join('.');
};

/** @type {any[]} */
export const VALUE_FORMAT_MODES = [
    { value: 'auto', label: 'Auto' },
    { value: 'number', label: 'Number' },
    { value: 'compact', label: 'Compact' },
    { value: 'percent', label: 'Percent' },
    { value: 'currency', label: 'Currency' },
];

/** @param {any} [value] @param {any} [mode] @returns {any} */
export const formatChartValue = (value, mode) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '';
    if (!mode || mode === 'auto') return formatChartNumber(value);
    if (mode === 'compact') {
        /** @type {any} */
        const abs = Math.abs(value);
        if (abs >= 1e9) return (value / 1e9).toFixed(1).replace(/\.0$/, '') + 'B';
        if (abs >= 1e6) return (value / 1e6).toFixed(1).replace(/\.0$/, '') + 'M';
        if (abs >= 1e3) return (value / 1e3).toFixed(1).replace(/\.0$/, '') + 'K';
        return formatChartNumber(value);
    }
    if (mode === 'percent') return formatChartNumber(value) + '%';
    if (mode === 'currency') return '$' + formatChartNumber(value);
    return formatChartNumber(value);
};

/** @param {any} [text] @param {any} [fontSize] @returns {any} */
export const estimateTextWidth = (text, fontSize = 11) => Math.ceil(String(text || '').length * fontSize * 0.62);

/** @param {any} [value] @param {any} [maxLength] @returns {any} */
export const truncateChartLabel = (value, maxLength) => {
    /** @type {any} */
    const label = String(value || '');
    return label.length > maxLength ? `${label.slice(0, maxLength)}...` : label;
};

/** @param {any} [column] @returns {any} */
export const getColumnLabel = (column) => {
    if (!column) return 'Value';
    /** @type {any} */
    const header = column.columnDef ? column.columnDef.header : null;
    if (typeof header === 'string' && header.trim()) return formatDisplayLabel(header);
    return formatDisplayLabel(column.id || 'Value');
};

/** @param {any} [column] @returns {any} */
export const getColumnHeaderSegments = (column) => {
    /** @type {any[]} */
    const segments = [];
    let current = column;

    while (current) {
        /** @type {any} */
        const header = current.columnDef ? current.columnDef.header : null;
        /** @type {any} */
        const headerVal = current.headerVal || (current.columnDef && current.columnDef.headerVal) || null;
        /** @type {any} */
        let resolvedHeader = null;

        if (typeof headerVal === 'string' && headerVal.trim() && headerVal.trim() !== '...') {
            resolvedHeader = headerVal;
        } else if (typeof header === 'string' && header.trim() && header.trim() !== '...') {
            resolvedHeader = header;
        } else if (typeof current.id === 'string' && current.id.startsWith('group_')) {
            resolvedHeader = current.id.replace('group_', '').split('|||').pop() || null;
        }

        if (resolvedHeader) {
            segments.unshift(formatDisplayLabel(resolvedHeader));
        }
        current = current.parent || null;
    }

    if (segments.length === 0) {
        return [getColumnLabel(column)];
    }

    return segments;
};

/** @param {any} [columns] @returns {any} */
export const buildColumnLabelResolver = (columns) => {
    /** @type {any} */
    const resolvedColumns = (columns || []).filter(Boolean);
    /** @type {any} */
    const segmentSets = resolvedColumns.map(/** @param {any} [column] @returns {any} */ (column) => getColumnHeaderSegments(column));
    /** @type {any} */
    const leafLabels = Array.from(new Set(
        segmentSets
            .map(/** @param {any} [segments] @returns {any} */ (segments) => segments[segments.length - 1])
            .filter(Boolean)
    ));
    /** @type {any} */
    const omitRepeatedLeaf = leafLabels.length === 1 && segmentSets.some(/** @param {any} [segments] @returns {any} */ (segments) => segments.length > 1);

    return /** @param {any} [column] @returns {any} */ (column) => {
        if (!column) return 'Value';
        /** @type {any} */
        const segments = getColumnHeaderSegments(column);
        /** @type {any} */
        const displaySegments = omitRepeatedLeaf && segments.length > 1
            ? segments.slice(0, -1)
            : segments;
        return (displaySegments.length > 0 ? displaySegments : segments).join(' > ') || getColumnLabel(column);
    };
};

/** @param {any} [column] @returns {any} */
export const getColumnValueSegments = (column) => {
    /** @type {any[]} */
    const segments = [];
    let current = column;

    while (current) {
        /** @type {any} */
        const header = current.columnDef ? current.columnDef.header : null;
        /** @type {any} */
        const headerVal = current.headerVal || (current.columnDef && current.columnDef.headerVal) || null;
        /** @type {any} */
        let resolvedValue = null;

        if (headerVal !== null && headerVal !== undefined && String(headerVal).trim() && String(headerVal).trim() !== '...') {
            resolvedValue = String(headerVal);
        } else if (typeof header === 'string' && header.trim() && header.trim() !== '...') {
            resolvedValue = header;
        } else if (typeof current.id === 'string' && current.id.startsWith('group_')) {
            resolvedValue = current.id.replace('group_', '').split('|||').pop() || null;
        }

        if (resolvedValue) {
            segments.unshift(resolvedValue);
        }
        current = current.parent || null;
    }

    return segments;
};

/** @param {any} [column] @param {any} [fields] @returns {any} */
export const buildColumnFieldValues = (column, fields = []) => {
    /** @type {any} */
    const rawSegments = getColumnValueSegments(column);
    /** @type {any} */
    const effectiveSegments = rawSegments.slice(0, Math.max(0, fields.length));
    return effectiveSegments.reduce(/** @param {any} [acc] @param {any} [value] @param {any} [index] @returns {any} */ (acc, value, index) => {
        const field = fields[index];
        if (field && value !== undefined && value !== null && String(value).trim() !== '') {
            acc[field] = String(value);
        }
        return acc;
    }, {});
};

/** @param {any} [visibleCols] @returns {any} */
export const getChartableColumns = (visibleCols = []) => (
    (visibleCols || []).filter(/** @param {any} [column] @returns {any} */ (column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
);

/** @param {any} [column] @param {any} [index] @returns {any} */
export const getComboLayerDefaultName = (column, index = 0) => (
    column && column.id ? getColumnLabel(column) : `Series ${index + 1}`
);

/** @param {any} [visibleCols] @param {any} [maxLayers] @returns {any} */
export const buildDefaultCartesianLayers = (visibleCols = [], maxLayers = 3) => {
    /** @type {any} */
    const chartableColumns = getChartableColumns(visibleCols).slice(0, Math.max(1, Math.floor(Number(maxLayers) || 1)));
    return chartableColumns.map(/** @param {any} [column] @param {any} [index] @returns {any} */ (column, index) => ({
        id: `layer-${index + 1}`,
        type: DEFAULT_COMBO_LAYER_SEQUENCE[index % DEFAULT_COMBO_LAYER_SEQUENCE.length],
        columnId: column.id,
        axis: index === 0 ? 'left' : (index === 1 ? 'right' : 'left'),
        name: getComboLayerDefaultName(column, index),
        hidden: false,
    }));
};

/** @param {any} [columns] @returns {any} */
export const buildGroupedChartColumnOptions = (columns = []) => {
    /** @type {any} */
    const resolveColumnLabel = buildColumnLabelResolver(columns);
    return columns
        .map(/** @param {any} [column] @returns {any} */ (column) => {
            /** @type {any} */
            const segments = getColumnHeaderSegments(column);
            /** @type {any} */
            const leafLabel = segments[segments.length - 1] || getColumnLabel(column);
            /** @type {any} */
            const groupLabel = segments.length > 1 ? segments.slice(0, -1).join(' / ') : 'Measures';
            /** @type {any} */
            const fullLabel = resolveColumnLabel(column);
            return {
                id: column.id,
                column,
                leafLabel,
                groupLabel,
                fullLabel,
                searchText: [
                    column.id,
                    leafLabel,
                    groupLabel,
                    fullLabel,
                    segments.join(' '),
                ].join(' ').toLowerCase(),
            };
        })
        .sort(/** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => (
            left.groupLabel.localeCompare(right.groupLabel, undefined, { numeric: true, sensitivity: 'base' })
            || left.fullLabel.localeCompare(right.fullLabel, undefined, { numeric: true, sensitivity: 'base' })
        ));
};

/** @param {any} [layer] @param {any} [availableColumns] @param {any} [index] @param {any} [fallbackLayer] @returns {any} */
export const normalizeComboLayer = (layer, availableColumns = [], index = 0, fallbackLayer = null) => {
    /** @type {any} */
    const source = layer && typeof layer === 'object' ? layer : {};
    /** @type {any} */
    const fallback = fallbackLayer && typeof fallbackLayer === 'object' ? fallbackLayer : {};
    /** @type {any} */
    const chartableColumns = getChartableColumns(availableColumns);
    /** @type {any} */
    const fallbackColumn = chartableColumns[0] || null;
    /** @type {any} */
    const requestedColumnId = source.columnId || fallback.columnId || (fallbackColumn && fallbackColumn.id) || null;
    /** @type {any} */
    const matchingColumn = chartableColumns.find(/** @param {any} [column] @returns {any} */ (column) => column.id === requestedColumnId) || null;
    return {
        id: typeof source.id === 'string' && source.id.trim()
            ? source.id
            : (typeof fallback.id === 'string' && fallback.id.trim() ? fallback.id : `layer-${index + 1}`),
        type: VALID_COMBO_LAYER_TYPES.has(source.type)
            ? source.type
            : (VALID_COMBO_LAYER_TYPES.has(fallback.type) ? fallback.type : DEFAULT_COMBO_LAYER_SEQUENCE[index % DEFAULT_COMBO_LAYER_SEQUENCE.length]),
        columnId: matchingColumn ? matchingColumn.id : requestedColumnId,
        axis: VALID_COMBO_LAYER_AXES.has(source.axis)
            ? source.axis
            : (VALID_COMBO_LAYER_AXES.has(fallback.axis) ? fallback.axis : (index === 1 ? 'right' : 'left')),
        name: typeof source.name === 'string' && source.name.trim()
            ? source.name.trim()
            : (typeof fallback.name === 'string' && fallback.name.trim()
                ? fallback.name.trim()
                : getComboLayerDefaultName(matchingColumn || fallbackColumn, index)),
        hidden: Boolean(source.hidden),
        color: typeof source.color === 'string' && source.color ? source.color : (typeof fallback.color === 'string' && fallback.color ? fallback.color : null),
        valueFormat: typeof source.valueFormat === 'string' && source.valueFormat ? source.valueFormat : (typeof fallback.valueFormat === 'string' && fallback.valueFormat ? fallback.valueFormat : 'auto'),
    };
};

/** @param {any} [layers] @param {any} [availableColumns] @param {any} [fallbackLayers] @returns {any} */
export const normalizeComboLayers = (layers, availableColumns = [], fallbackLayers = []) => {
    /** @type {any} */
    const sourceLayers = Array.isArray(layers) ? layers : [];
    /** @type {any} */
    const normalized = sourceLayers
        .filter(/** @param {any} [layer] @returns {any} */ (layer) => layer && typeof layer === 'object')
        .map(/** @param {any} [layer] @param {any} [index] @returns {any} */ (layer, index) => normalizeComboLayer(layer, availableColumns, index, Array.isArray(fallbackLayers) ? fallbackLayers[index] : null));

    if (normalized.length > 0) return normalized;
    /** @type {any} */
    const defaultLayers = Array.isArray(fallbackLayers) && fallbackLayers.length > 0
        ? fallbackLayers
        : buildDefaultCartesianLayers(availableColumns);
    return defaultLayers.map(/** @param {any} [layer] @param {any} [index] @returns {any} */ (layer, index) => normalizeComboLayer(layer, availableColumns, index, layer));
};

/** @param {any} [row] @param {any} [fallbackIndex] @returns {any} */
export const getRowLabel = (row, fallbackIndex = 0) => {
    if (!row) return `Row ${fallbackIndex + 1}`;
    if ((row.original && row.original._path === '__grand_total__') || row._path === '__grand_total__') return 'Grand Total';
    /** @type {any} */
    const explicitId = row.original && row.original._id ? row.original._id : row._id;
    if (explicitId !== null && explicitId !== undefined && String(explicitId).trim() !== '') {
        return formatDisplayLabel(String(explicitId));
    }
    if (typeof row.getValue === 'function') {
        /** @type {any} */
        const hierarchyValue = row.getValue('hierarchy');
        if (hierarchyValue !== null && hierarchyValue !== undefined && String(hierarchyValue).trim() !== '') {
            return formatDisplayLabel(String(hierarchyValue));
        }
    }
    if (row.hierarchy !== null && row.hierarchy !== undefined && String(row.hierarchy).trim() !== '') {
        return formatDisplayLabel(String(row.hierarchy));
    }
    return formatDisplayLabel(row.id || `Row ${fallbackIndex + 1}`);
};

/** @param {any} [row] @returns {any} */
export const getRowDepth = (row) => {
    if (!row) return 0;
    /** @type {any} */
    const explicitDepth = row.original && typeof row.original.depth === 'number'
        ? row.original.depth
        : (typeof row.depth === 'number' ? row.depth : null);
    if (explicitDepth !== null) return explicitDepth;
    return typeof row.depth === 'number' ? row.depth : 0;
};

/** @param {any} [row] @returns {any} */
export const getRowLevel = (row) => getRowDepth(row) + 1;
/** @param {any} [row] @returns {any} */
export const getRowPath = (row) => {
    if (!row) return '';
    if (row.original && row.original._path) return String(row.original._path);
    if (row._path) return String(row._path);
    return String(row.id || '');
};

/** @param {any} [rowPath] @param {any} [fields] @returns {any} */
export const buildRowFieldValues = (rowPath, fields = []) => {
    /** @type {any} */
    const pathParts = rowPath ? String(rowPath).split('|||') : [];
    return fields.reduce(/** @param {any} [acc] @param {any} [field] @param {any} [index] @returns {any} */ (acc, field, index) => {
        const value = pathParts[index];
        if (field && value !== undefined && value !== null && String(value).trim() !== '') {
            acc[field] = String(value);
        }
        return acc;
    }, {});
};

/** @param {any} [row] @param {any} [fallbackIndex] @param {any} [fields] @returns {any} */
export const buildRowTarget = (row, fallbackIndex = 0, fields = []) => {
    /** @type {any} */
    const rowPath = getRowPath(row);
    return {
        kind: 'row',
        rowId: row && row.id ? row.id : null,
        rowPath,
        label: getRowLabel(row, fallbackIndex),
        pathValues: rowPath ? rowPath.split('|||') : [],
        fieldValues: buildRowFieldValues(rowPath, fields),
    };
};

/** @param {any} [column] @param {any} [fields] @returns {any} */
export const buildColumnTarget = (column, fields = []) => ({
    kind: 'column',
    columnId: column && column.id ? column.id : null,
    label: buildColumnLabelResolver([column])(column),
    pathValues: getColumnValueSegments(column),
    fieldValues: buildColumnFieldValues(column, fields),
});
/** @param {any} [ancestorPath] @param {any} [candidatePath] @returns {any} */
export const isDescendantPath = (ancestorPath, candidatePath) => (
    Boolean(ancestorPath) &&
    Boolean(candidatePath) &&
    candidatePath.indexOf(`${ancestorPath}|||`) === 0
);

/** @param {any} [row] @returns {any} */
export const isTotalRow = (row) => Boolean(
    row &&
    (
        (row.original && (row.original._isTotal || row.original.__isGrandTotal__ || row.original._path === '__grand_total__'))
        || row._isTotal
        || row.__isGrandTotal__
        || row._path === '__grand_total__'
    )
);

/** @param {any} [row] @param {any} [columnId] @returns {any} */
export const getCellValue = (row, columnId) => {
    if (!row || !columnId) return null;
    if (typeof row.getValue === 'function') {
        /** @type {any} */
        const resolvedValue = row.getValue(columnId);
        if (resolvedValue !== undefined && resolvedValue !== null && resolvedValue !== '') {
            return resolvedValue;
        }
    }
    /** @type {any} */
    const rowCells = typeof row.getAllCells === 'function'
        ? row.getAllCells()
        : (typeof row.getVisibleCells === 'function' ? row.getVisibleCells() : []);
    if (Array.isArray(rowCells) && rowCells.length > 0) {
        /** @type {any} */
        const matchingCell = rowCells.find((cell) => cell && cell.column && cell.column.id === columnId);
        if (matchingCell) {
            if (typeof matchingCell.getValue === 'function') {
                /** @type {any} */
                const cellValue = matchingCell.getValue();
                if (cellValue !== undefined && cellValue !== null && cellValue !== '') {
                    return cellValue;
                }
            }
            if (typeof matchingCell.renderValue === 'function') {
                /** @type {any} */
                const renderedValue = matchingCell.renderValue();
                if (renderedValue !== undefined && renderedValue !== null && renderedValue !== '') {
                    return renderedValue;
                }
            }
        }
    }
    if (row.original && row.original[columnId] !== undefined && row.original[columnId] !== null && row.original[columnId] !== '') {
        return row.original[columnId];
    }
    if (row.values && row.values[columnId] !== undefined && row.values[columnId] !== null && row.values[columnId] !== '') {
        return row.values[columnId];
    }
    return row[columnId];
};

/** @param {any} [rows] @returns {any} */
export const getAvailableLevels = (rows) => Array.from(new Set(
    (rows || [])
        .filter(/** @param {any} [row] @returns {any} */ (row) => row && !isTotalRow(row))
        .map(/** @param {any} [row] @returns {any} */ (row) => getRowLevel(row))
)).sort((a, b) => a - b);

/** @param {any} [rows] @param {any} [hierarchyLevel] @returns {any} */
export const getHierarchyDisplayRows = (rows, hierarchyLevel) => {
    /** @type {any} */
    const orderedRows = (rows || []).filter(/** @param {any} [row] @returns {any} */ (row) => row && !isTotalRow(row));
    if (orderedRows.length === 0) return [];

    /** @type {any} */
    const pathToRow = new Map();
    orderedRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
        /** @type {any} */
        const path = getRowPath(row);
        if (path) pathToRow.set(path, row);
    });

    /** @type {any} */
    const frontierRows = orderedRows.filter(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
        /** @type {any} */
        const currentPath = getRowPath(row);
        const nextRow = orderedRows[index + 1];
        /** @type {any} */
        const nextPath = nextRow ? getRowPath(nextRow) : '';
        return !isDescendantPath(currentPath, nextPath);
    });

    if (hierarchyLevel === 'all') return frontierRows;

    /** @type {any} */
    const selectedPaths = new Set();
    frontierRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
        /** @type {any} */
        const rowPath = getRowPath(row);
        /** @type {any} */
        const pathParts = rowPath ? rowPath.split('|||') : [];
        /** @type {any} */
        const targetLevel = Math.min(Number(hierarchyLevel) || 1, Math.max(pathParts.length, 1));
        let resolvedPath = '';
        for (let level = targetLevel; level >= 1; level -= 1) {
            /** @type {any} */
            const candidatePath = pathParts.slice(0, level).join('|||');
            if (candidatePath && pathToRow.has(candidatePath)) {
                resolvedPath = candidatePath;
                break;
            }
        }
        selectedPaths.add(resolvedPath || rowPath);
    });

    return orderedRows.filter(/** @param {any} [row] @returns {any} */ (row) => selectedPaths.has(getRowPath(row)));
};

/** @param {any} [index] @param {any} [theme] @param {any} [colors] @returns {any} */
export const getColorForIndex = (index, theme, colors) => {
    /** @type {any} */
    const base = colors || DEFAULT_COLORS;
    /** @type {any[]} */
    const palette = [theme.primary, ...base.filter(/** @param {any} [color] @returns {any} */ (color) => color !== theme.primary)];
    return palette[index % palette.length];
};

/** @param {any} [selectionMap] @param {any} [visibleRows] @param {any} [visibleCols] @returns {any} */
export const buildSelectionBounds = (selectionMap, visibleRows, visibleCols) => {
    /** @type {any} */
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;

    /** @type {Record<string, any>} */
    const rowIndexById = {};
    visibleRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
        rowIndexById[row.id] = index;
    });
    /** @type {Record<string, any>} */
    const colIndexById = {};
    visibleCols.forEach(/** @param {any} [column] @param {any} [index] @returns {any} */ (column, index) => {
        colIndexById[column.id] = index;
    });

    let minRow = Infinity;
    let maxRow = -1;
    let minCol = Infinity;
    let maxCol = -1;
    /** @type {Record<string, any>} */
    const grid = {};

    keys.forEach(/** @param {any} [selectionKey] @returns {any} */ (selectionKey) => {
        /** @type {any} */
        const separatorIndex = selectionKey.lastIndexOf(':');
        if (separatorIndex <= 0) return;
        /** @type {any} */
        const rowId = selectionKey.slice(0, separatorIndex);
        /** @type {any} */
        const colId = selectionKey.slice(separatorIndex + 1);
        const rowIndex = rowIndexById[rowId];
        const colIndex = colIndexById[colId];
        if (rowIndex === undefined || colIndex === undefined) return;

        minRow = Math.min(minRow, rowIndex);
        maxRow = Math.max(maxRow, rowIndex);
        minCol = Math.min(minCol, colIndex);
        maxCol = Math.max(maxCol, colIndex);
        if (!grid[rowIndex]) grid[rowIndex] = {};
        grid[rowIndex][colIndex] = selectionMap[selectionKey];
    });

    if (minRow === Infinity || minCol === Infinity) return null;

    return {
        minRow,
        maxRow,
        minCol,
        maxCol,
        grid,
        rowIndexes: Array.from({ length: maxRow - minRow + 1 }, (_, idx) => minRow + idx),
        colIndexes: Array.from({ length: maxCol - minCol + 1 }, (_, idx) => minCol + idx),
    };
};

/** @param {any} [rowIndexes] @param {any} [visibleRows] @returns {any} */
export const buildCategoryBandsForRows = (rowIndexes, visibleRows) => rowIndexes.map(/** @param {any} [rowIndex] @param {any} [index] @returns {any} */ (rowIndex, index) => {
    const row = visibleRows[rowIndex];
    /** @type {any} */
    const path = getRowPath(row);
    /** @type {any} */
    const pathParts = path ? path.split('|||').map(/** @param {any} [part] @returns {any} */ (part) => formatDisplayLabel(String(part))) : [];
    /** @type {any} */
    const currentLabel = getRowLabel(row, index);
    if (pathParts.length <= 1) {
        return {
            outerLabel: currentLabel,
            innerLabel: '',
            groupKey: path || currentLabel,
        };
    }
    /** @type {any} */
    const outerLabel = pathParts[pathParts.length - 2] || currentLabel;
    /** @type {any} */
    const innerLabel = pathParts[pathParts.length - 1] || currentLabel;
    return {
        outerLabel,
        innerLabel,
        groupKey: pathParts.slice(0, pathParts.length - 1).join('|||') || outerLabel,
    };
});

/** @param {any} [displayRows] @param {any} [allVisibleRows] @param {any} [numericValues] @param {any} [maxGroups] @returns {any} */
export const buildStackedGroupsForRows = (displayRows, allVisibleRows, numericValues, maxGroups) => {
    if (!Array.isArray(displayRows) || displayRows.length === 0) return [];
    /** @type {any} */
    const orderedRows = (allVisibleRows || []).filter(/** @param {any} [row] @returns {any} */ (row) => row && !isTotalRow(row));
    /** @type {any} */
    const rowIndexByPath = new Map();
    orderedRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
        rowIndexByPath.set(getRowPath(row), index);
    });

    return displayRows.slice(0, maxGroups).map((row, index) => {
        /** @type {any} */
        const rowPath = getRowPath(row);
        /** @type {any} */
        const rowLevel = getRowLevel(row);
        /** @type {any} */
        const rowIndex = rowIndexByPath.get(rowPath);
        /** @type {any[]} */
        const segments = [];

        if (rowIndex !== undefined) {
            for (let nextIndex = rowIndex + 1; nextIndex < orderedRows.length; nextIndex += 1) {
                const candidate = orderedRows[nextIndex];
                /** @type {any} */
                const candidatePath = getRowPath(candidate);
                if (!isDescendantPath(rowPath, candidatePath)) break;
                if (getRowLevel(candidate) === rowLevel + 1) {
                    /** @type {any} */
                    const value = parseNumeric(numericValues[candidatePath]);
                    if (value !== null) {
                        segments.push({
                            label: getRowLabel(candidate, segments.length),
                            value,
                            path: candidatePath,
                        });
                    }
                }
            }
        }

        if (segments.length === 0) {
            /** @type {any} */
            const fallbackValue = parseNumeric(numericValues[rowPath]);
            segments.push({
                label: getRowLabel(row, index),
                value: fallbackValue !== null ? fallbackValue : 0,
                path: rowPath,
            });
        }

        return {
            key: rowPath || `group-${index}`,
            label: getRowLabel(row, index),
            segments,
        };
    });
};

/** @param {any} [displayRows] @param {any} [categoryBands] @param {any} [numericValues] @param {any} [maxGroups] @returns {any} */
export const buildFrontierStackedGroups = (displayRows, categoryBands, numericValues, maxGroups) => {
    /** @type {any} */
    const rows = Array.isArray(displayRows) ? displayRows.slice(0, maxGroups) : [];
    if (rows.length === 0) return [];

    return rows.reduce(/** @param {any} [groups] @param {any} [row] @param {any} [index] @returns {any} */ (groups, row, index) => {
        /** @type {any} */
        const band = Array.isArray(categoryBands) ? categoryBands[index] : null;
        /** @type {any} */
        const rowPath = getRowPath(row);
        /** @type {any} */
        const value = parseNumeric(numericValues[rowPath]);
        /** @type {Record<string, any>} */
        const segment = {
            label: band && band.innerLabel ? band.innerLabel : getRowLabel(row, index),
            value: value !== null ? value : 0,
            path: rowPath,
        };
        /** @type {any} */
        const groupKey = band && band.groupKey ? band.groupKey : (rowPath || `group-${index}`);
        /** @type {any} */
        const groupLabel = band && band.innerLabel
            ? band.outerLabel
            : getRowLabel(row, index);
        const previousGroup = groups[groups.length - 1];
        if (previousGroup && previousGroup.key === groupKey) {
            previousGroup.segments.push(segment);
            return groups;
        }
        groups.push({
            key: groupKey,
            label: groupLabel,
            segments: [segment],
        });
        return groups;
    }, []);
};

/** @param {any} [rows] @param {any} [valueColumn] @returns {any} */
export const buildIcicleNodes = (rows, valueColumn) => {
    if (!valueColumn || !Array.isArray(rows) || rows.length === 0) {
        return { nodes: [], maxDepth: 0 };
    }

    /** @type {any} */
    const sourceRows = rows.filter((row) => row && !isTotalRow(row));
    if (sourceRows.length === 0) {
        return { nodes: [], maxDepth: 0 };
    }

    /** @type {any} */
    const nodeMap = new Map();
    let maxDepth = 1;

    sourceRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
        /** @type {any} */
        const rowPath = getRowPath(row);
        if (!rowPath) return;
        /** @type {any} */
        const depth = Math.max(1, rowPath.split('|||').length);
        maxDepth = Math.max(maxDepth, depth);
        nodeMap.set(rowPath, {
            kind: 'row',
            rowId: row && row.id ? row.id : null,
            rowPath,
            label: getRowLabel(row, index),
            value: parseNumeric(getCellValue(row, valueColumn.id)) || 0,
            depth,
            children: [],
        });
    });

    /** @type {any[]} */
    const roots = [];
    nodeMap.forEach(/** @param {any} [node] @param {any} [rowPath] @returns {any} */ (node, rowPath) => {
        /** @type {any} */
        const parentPath = rowPath.includes('|||')
            ? rowPath.slice(0, rowPath.lastIndexOf('|||'))
            : null;
        if (parentPath && nodeMap.has(parentPath)) {
            nodeMap.get(parentPath).children.push(node);
            return;
        }
        roots.push(node);
    });

    return { nodes: roots, maxDepth };
};

/** @param {any} [nodes] @returns {any} */
export const cloneHierarchyNodes = (nodes) => (
    Array.isArray(nodes)
        ? nodes.map((node) => ({
            ...node,
            children: cloneHierarchyNodes(node.children),
        }))
        : []
);

/** @param {any} [nodes] @param {any} [sortMode] @returns {any} */
export const sortHierarchyNodes = (nodes, sortMode) => {
    if (!Array.isArray(nodes)) return [];
    /** @type {any} */
    const normalizedMode = String(sortMode || 'natural').toLowerCase();
    /** @type {any} */
    const children = nodes.map((node) => ({
        ...node,
        children: sortHierarchyNodes(node.children, normalizedMode),
    }));
    if (normalizedMode === 'natural' || normalizedMode === 'none') {
        return children;
    }

    /** @type {any} */
    const compare = normalizedMode === 'label_asc'
        ? /** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => String(left.label || '').localeCompare(String(right.label || ''))
        : normalizedMode === 'label_desc'
            ? /** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => String(right.label || '').localeCompare(String(left.label || ''))
            : normalizedMode === 'value_asc'
                ? /** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => Math.abs(Number(left.value) || 0) - Math.abs(Number(right.value) || 0)
                : /** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => Math.abs(Number(right.value) || 0) - Math.abs(Number(left.value) || 0);
    return [...children].sort(compare);
};

/** @param {any} [model] @param {any} [sortMode] @returns {any} */
export const sortChartModel = (model, sortMode) => {
    /** @type {any} */
    const normalizedMode = String(sortMode || 'natural').toLowerCase();
    if (!model || normalizedMode === 'natural' || normalizedMode === 'none') return model;
    if (!Array.isArray(model.categories) || !Array.isArray(model.series) || model.categories.length === 0) {
        return model;
    }

    /** @type {any} */
    const ordering = model.categories.map(/** @param {any} [category] @param {any} [index] @returns {any} */ (category, index) => ({
        category,
        index,
        total: (model.series || []).reduce(/** @param {any} [sum] @param {any} [series] @returns {any} */ (sum, series) => {
            /** @type {any} */
            const value = Array.isArray(series && series.values) ? series.values[index] : null;
            return sum + (typeof value === 'number' && Number.isFinite(value) ? value : 0);
        }, 0),
    }));

    ordering.sort(/** @param {any} [left] @param {any} [right] @returns {any} */ (left, right) => {
        if (normalizedMode === 'label_asc') {
            return String(left.category || '').localeCompare(String(right.category || ''));
        }
        if (normalizedMode === 'label_desc') {
            return String(right.category || '').localeCompare(String(left.category || ''));
        }
        if (normalizedMode === 'value_asc') {
            return left.total - right.total;
        }
        return right.total - left.total;
    });

    /** @type {any} */
    const reorderedIndexes = ordering.map(/** @param {any} [item] @returns {any} */ (item) => item.index);
    return {
        ...model,
        categories: reorderedIndexes.map(/** @param {any} [index] @returns {any} */ (index) => model.categories[index]),
        categoryTargets: Array.isArray(model.categoryTargets) ? reorderedIndexes.map(/** @param {any} [index] @returns {any} */ (index) => model.categoryTargets[index]) : model.categoryTargets,
        categoryBands: Array.isArray(model.categoryBands) ? reorderedIndexes.map(/** @param {any} [index] @returns {any} */ (index) => model.categoryBands[index]) : model.categoryBands,
        stackedGroups: Array.isArray(model.stackedGroups) && model.stackedGroups.length === model.categories.length
            ? reorderedIndexes.map(/** @param {any} [index] @returns {any} */ (index) => model.stackedGroups[index])
            : model.stackedGroups,
        series: (model.series || []).map(/** @param {any} [series] @returns {any} */ (series) => ({
            ...series,
            values: reorderedIndexes.map(/** @param {any} [index] @returns {any} */ (index) => series.values[index]),
        })),
    };
};

/** @param {any} [nodes] @returns {any} */
export const buildHierarchySankey = (nodes) => {
    if (!Array.isArray(nodes) || nodes.length === 0) {
        return { nodes: [], links: [], maxDepth: 0 };
    }

    /** @type {any[]} */
    const flatNodes = [];
    /** @type {any[]} */
    const links = [];
    let maxDepth = 1;

    /** @param {any} [node] @param {any} [parent] @returns {any} */
    const visit = (node, parent = null) => {
        if (!node || !node.rowPath) return;
        flatNodes.push({
            kind: 'row',
            rowId: node.rowId || null,
            rowPath: node.rowPath,
            label: node.label,
            value: Math.abs(Number(node.value) || 0),
            depth: node.depth || 1,
        });
        maxDepth = Math.max(maxDepth, node.depth || 1);
        if (parent && parent.rowPath) {
            links.push({
                source: parent.rowPath,
                target: node.rowPath,
                value: Math.abs(Number(node.value) || 0),
            });
        }
        (node.children || []).forEach(/** @param {any} [child] @returns {any} */ (child) => visit(child, node));
    };

    nodes.forEach((node) => visit(node, null));
    return { nodes: flatNodes, links, maxDepth };
};

/** @param {any} [cx] @param {any} [cy] @param {any} [radius] @param {any} [angle] @returns {any} */
export const polarToCartesian = (cx, cy, radius, angle) => ({
    x: cx + (radius * Math.cos(angle)),
    y: cy + (radius * Math.sin(angle)),
});

/** @param {any} [cx] @param {any} [cy] @param {any} [innerRadius] @param {any} [outerRadius] @param {any} [startAngle] @param {any} [endAngle] @returns {any} */
export const describeDonutArc = (cx, cy, innerRadius, outerRadius, startAngle, endAngle) => {
    /** @type {any} */
    const safeSpan = Math.max(0.0001, Math.min((Math.PI * 2) - 0.0001, endAngle - startAngle));
    /** @type {any} */
    const safeEnd = startAngle + safeSpan;
    /** @type {any} */
    const largeArc = safeSpan > Math.PI ? 1 : 0;
    /** @type {any} */
    const outerStart = polarToCartesian(cx, cy, outerRadius, startAngle);
    /** @type {any} */
    const outerEnd = polarToCartesian(cx, cy, outerRadius, safeEnd);

    if (innerRadius <= 0) {
        return [
            `M ${cx} ${cy}`,
            `L ${outerStart.x} ${outerStart.y}`,
            `A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${outerEnd.x} ${outerEnd.y}`,
            'Z',
        ].join(' ');
    }

    /** @type {any} */
    const innerStart = polarToCartesian(cx, cy, innerRadius, safeEnd);
    /** @type {any} */
    const innerEnd = polarToCartesian(cx, cy, innerRadius, startAngle);
    return [
        `M ${outerStart.x} ${outerStart.y}`,
        `A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${outerEnd.x} ${outerEnd.y}`,
        `L ${innerStart.x} ${innerStart.y}`,
        `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${innerEnd.x} ${innerEnd.y}`,
        'Z',
    ].join(' ');
};

/** @param {any} [nodes] @param {any} [startAngle] @param {any} [endAngle] @param {any} [depth] @param {any} [ringWidth] @param {any} [acc] @returns {any} */
export const layoutSunburstNodes = (nodes, startAngle, endAngle, depth, ringWidth, acc = []) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return acc;
    /** @type {any} */
    const weights = nodes.map((node) => Math.abs(Number(node.value) || 0));
    /** @type {any} */
    const totalWeight = weights.reduce(/** @param {any} [sum] @param {any} [value] @returns {any} */ (sum, value) => sum + value, 0);
    let angleCursor = startAngle;

    nodes.forEach((node, index) => {
        /** @type {any} */
        const fallbackSweep = (endAngle - startAngle) / Math.max(nodes.length, 1);
        /** @type {any} */
        const sweep = totalWeight > 0 ? ((weights[index] || 0) / totalWeight) * (endAngle - startAngle) : fallbackSweep;
        /** @type {any} */
        const nextAngle = angleCursor + sweep;
        /** @type {Record<string, any>} */
        const layoutNode = {
            ...node,
            startAngle: angleCursor,
            endAngle: nextAngle,
            innerRadius: Math.max(0, (depth - 1) * ringWidth),
            outerRadius: depth * ringWidth,
        };
        acc.push(layoutNode);
        if (Array.isArray(node.children) && node.children.length > 0) {
            layoutSunburstNodes(node.children, angleCursor, nextAngle, depth + 1, ringWidth, acc);
        }
        angleCursor = nextAngle;
    });
    return acc;
};

/** @param {any} [nodes] @param {any} [links] @param {any} [width] @param {any} [height] @returns {any} */
export const layoutSankey = (nodes, links, width, height) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return { nodes: [], links: [] };

    /** @type {Record<string, any>} */
    const margin = { top: 24, right: 20, bottom: 40, left: 20 };
    const nodeWidth = 18;
    /** @type {any} */
    const levelMap = new Map();
    nodes.forEach((node) => {
        /** @type {any} */
        const depth = Math.max(1, Number(node.depth) || 1);
        if (!levelMap.has(depth)) levelMap.set(depth, []);
        levelMap.get(depth).push(node);
    });

    /** @type {any} */
    const maxDepth = Math.max(...Array.from(levelMap.keys()));
    /** @type {any} */
    const usableWidth = Math.max(1, width - margin.left - margin.right - nodeWidth);
    /** @type {any} */
    const levelGap = maxDepth > 1 ? usableWidth / (maxDepth - 1) : 0;
    /** @type {any[]} */
    const positionedNodes = [];
    /** @type {any} */
    const nodeLookup = new Map();

    levelMap.forEach(/** @param {any} [levelNodes] @param {any} [depth] @returns {any} */ (levelNodes, depth) => {
        const gap = 10;
        /** @type {any} */
        const availableHeight = Math.max(1, height - margin.top - margin.bottom - (Math.max(levelNodes.length - 1, 0) * gap));
        /** @type {any} */
        const totalValue = levelNodes.reduce(/** @param {any} [sum] @param {any} [node] @returns {any} */ (sum, node) => sum + Math.max(Math.abs(Number(node.value) || 0), 1), 0);
        /** @type {any} */
        const scale = totalValue > 0 ? availableHeight / totalValue : 0;

        /** @type {any} */
        const rawHeights = levelNodes.map(/** @param {any} [node] @returns {any} */ (node) =>
            totalValue > 0
                ? Math.max(8, Math.abs(Number(node.value) || 0) * scale)
                : Math.max(8, availableHeight / Math.max(levelNodes.length, 1))
        );
        /** @type {any} */
        const totalRaw = rawHeights.reduce(/** @param {any} [s] @param {any} [h] @returns {any} */ (s, h) => s + h, 0);
        /** @type {any} */
        const heightScale = totalRaw > availableHeight ? availableHeight / totalRaw : 1;

        let cursorY = margin.top;
        levelNodes.forEach(/** @param {any} [node] @param {any} [i] @returns {any} */ (node, i) => {
            /** @type {any} */
            const finalHeight = Math.max(6, rawHeights[i] * heightScale);
            /** @type {Record<string, any>} */
            const positioned = {
                ...node,
                x: margin.left + ((depth - 1) * levelGap),
                y: cursorY,
                width: nodeWidth,
                height: finalHeight,
            };
            positionedNodes.push(positioned);
            nodeLookup.set(node.rowPath, positioned);
            cursorY += finalHeight + gap;
        });
    });

    /** @type {any} */
    const outgoingTotals = new Map();
    /** @type {any} */
    const incomingTotals = new Map();
    links.forEach(/** @param {any} [link] @returns {any} */ (link) => {
        outgoingTotals.set(link.source, (outgoingTotals.get(link.source) || 0) + Math.max(link.value || 0, 0));
        incomingTotals.set(link.target, (incomingTotals.get(link.target) || 0) + Math.max(link.value || 0, 0));
    });
    /** @type {any} */
    const sourceOffsets = new Map();
    /** @type {any} */
    const targetOffsets = new Map();

    /** @type {any} */
    const positionedLinks = links.map(/** @param {any} [link] @returns {any} */ (link) => {
        /** @type {any} */
        const sourceNode = nodeLookup.get(link.source);
        /** @type {any} */
        const targetNode = nodeLookup.get(link.target);
        if (!sourceNode || !targetNode) return null;

        /** @type {any} */
        const sourceTotal = Math.max(outgoingTotals.get(link.source) || sourceNode.value || 1, 1);
        /** @type {any} */
        const targetTotal = Math.max(incomingTotals.get(link.target) || targetNode.value || 1, 1);
        /** @type {any} */
        const sourceScale = sourceNode.height / sourceTotal;
        /** @type {any} */
        const targetScale = targetNode.height / targetTotal;
        /** @type {any} */
        const thickness = Math.max(2, Math.min(sourceScale, targetScale) * Math.max(link.value || 0, 0));
        /** @type {any} */
        const sourceOffset = sourceOffsets.get(link.source) || 0;
        /** @type {any} */
        const targetOffset = targetOffsets.get(link.target) || 0;
        sourceOffsets.set(link.source, sourceOffset + thickness);
        targetOffsets.set(link.target, targetOffset + thickness);

        return {
            ...link,
            sourceNode,
            targetNode,
            thickness,
            sourceY: sourceNode.y + sourceOffset + (thickness / 2),
            targetY: targetNode.y + targetOffset + (thickness / 2),
        };
    }).filter(Boolean);

    return { nodes: positionedNodes, links: positionedLinks };
};

/** @param {any} p0 @returns {any} */
export const buildSeriesFromRowCategories = ({
    rowIndexes,
    colIndexes,
    visibleRows,
    visibleCols,
    grid,
    maxRows,
    maxColumns,
    rowFields = [],
    colFields = [],
}) => {
    /** @type {any} */
    const numericColIndexes = colIndexes.filter(/** @param {any} [colIndex] @returns {any} */ (colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return rowIndexes.some(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]) !== null);
    });

    if (numericColIndexes.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    /** @type {any} */
    const labelColIndex = colIndexes.find(/** @param {any} [colIndex] @returns {any} */ (colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return !numericColIndexes.includes(colIndex);
    });

    /** @type {any} */
    const slicedRowIndexes = rowIndexes.slice(0, normalizePositiveLimit(maxRows, MAX_SELECTION_CATEGORIES));
    /** @type {any} */
    const slicedSeriesIndexes = numericColIndexes.slice(0, normalizePositiveLimit(maxColumns, MAX_SELECTION_SERIES));
    /** @type {any} */
    const resolveColumnLabel = buildColumnLabelResolver(
        slicedSeriesIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => visibleCols[colIndex]).filter(Boolean)
    );
    /** @type {any} */
    const categories = slicedRowIndexes.map(/** @param {any} [rowIndex] @param {any} [idx] @returns {any} */ (rowIndex, idx) => {
        if (labelColIndex !== undefined) {
            /** @type {any} */
            const cellValue = grid[rowIndex] && grid[rowIndex][labelColIndex];
            if (cellValue !== null && cellValue !== undefined && String(cellValue).trim() !== '') {
                return formatDisplayLabel(String(cellValue));
            }
        }
        return getRowLabel(visibleRows[rowIndex], idx);
    });

    /** @type {any} */
    const series = slicedSeriesIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => ({
        name: resolveColumnLabel(visibleCols[colIndex]),
        values: slicedRowIndexes.map(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex])),
    })).filter(/** @param {any} [entry] @returns {any} */ (entry) => entry.values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null));

    if (series.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    /** @type {any[]} */
    const notes = [];
    if (rowIndexes.length > slicedRowIndexes.length) {
        notes.push(`showing first ${slicedRowIndexes.length} selected rows`);
    }
    if (numericColIndexes.length > slicedSeriesIndexes.length) {
        notes.push(`showing first ${slicedSeriesIndexes.length} numeric columns`);
    }

    return {
        categories,
        categoryBands: buildCategoryBandsForRows(slicedRowIndexes, visibleRows),
        categoryTargets: slicedRowIndexes.map(/** @param {any} [rowIndex] @param {any} [idx] @returns {any} */ (rowIndex, idx) => buildRowTarget(visibleRows[rowIndex], idx, rowFields)),
        series,
        note: notes.length > 0 ? notes.join(', ') : null,
    };
};

/** @param {any} p0 @returns {any} */
export const buildSeriesFromColumnCategories = ({
    rowIndexes,
    colIndexes,
    visibleRows,
    visibleCols,
    grid,
    maxRows,
    maxColumns,
    colFields = [],
}) => {
    /** @type {any} */
    const numericColIndexes = colIndexes.filter(/** @param {any} [colIndex] @returns {any} */ (colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return rowIndexes.some(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]) !== null);
    });

    if (numericColIndexes.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    /** @type {any} */
    const slicedColIndexes = numericColIndexes.slice(0, normalizePositiveLimit(maxColumns, MAX_SELECTION_CATEGORIES));
    /** @type {any} */
    const slicedRowIndexes = rowIndexes.slice(0, normalizePositiveLimit(maxRows, MAX_SELECTION_SERIES));
    /** @type {any} */
    const resolveColumnLabel = buildColumnLabelResolver(
        slicedColIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => visibleCols[colIndex]).filter(Boolean)
    );
    /** @type {any} */
    const categories = slicedColIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => resolveColumnLabel(visibleCols[colIndex]));
    /** @type {any} */
    const series = slicedRowIndexes.map(/** @param {any} [rowIndex] @param {any} [rowSeriesIndex] @returns {any} */ (rowIndex, rowSeriesIndex) => ({
        name: getRowLabel(visibleRows[rowIndex], rowSeriesIndex),
        values: slicedColIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex])),
    })).filter(/** @param {any} [entry] @returns {any} */ (entry) => entry.values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null));

    if (series.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    /** @type {any[]} */
    const notes = [];
    if (numericColIndexes.length > slicedColIndexes.length) {
        notes.push(`showing first ${slicedColIndexes.length} numeric columns`);
    }
    if (rowIndexes.length > slicedRowIndexes.length) {
        notes.push(`showing first ${slicedRowIndexes.length} selected rows`);
    }

    return {
        categories,
        categoryTargets: slicedColIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => buildColumnTarget(visibleCols[colIndex], colFields)),
        series,
        note: notes.length > 0 ? notes.join(', ') : null,
    };
};

/** @param {any} [selectionMap] @param {any} [visibleRows] @param {any} [visibleCols] @param {any} [options] @returns {any} */
export const buildSelectionChartModel = (selectionMap, visibleRows, visibleCols, options = {}) => {
    /** @type {any} */
    const orientation = options.orientation || 'auto';
    /** @type {any} */
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    /** @type {any} */
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_SELECTION_CATEGORIES);
    /** @type {any} */
    const maxColumns = normalizePositiveLimit(options.maxColumns, MAX_SELECTION_SERIES);
    /** @type {any} */
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    /** @type {any} */
    const colFields = Array.isArray(options.colFields) ? options.colFields : [];
    /** @type {any} */
    const sortMode = options.sortMode || 'natural';
    /** @type {any} */
    const bounds = buildSelectionBounds(selectionMap, visibleRows, visibleCols);
    if (!bounds) {
        return {
            title: null,
            subtitle: null,
            categories: [],
            categoryTargets: [],
            series: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            emptyMessage: 'Select a range of value cells to build a chart.',
            availableLevels: [],
            maxHierarchyLevel: options.maxHierarchyLevel || 1,
            activeHierarchyLevel: hierarchyLevel,
        };
    }

    const { rowIndexes, colIndexes, grid } = bounds;
    /** @type {any} */
    const selectedRows = rowIndexes.map(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => visibleRows[rowIndex]).filter(Boolean);
    /** @type {any} */
    const availableLevels = getAvailableLevels(selectedRows);
    /** @type {any} */
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    /** @type {any} */
    const displayRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    /** @type {any} */
    const displayPaths = new Set(displayRows.map(/** @param {any} [row] @returns {any} */ (row) => getRowPath(row)));
    /** @type {any} */
    const filteredRowIndexes = rowIndexes.filter(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => displayPaths.has(getRowPath(visibleRows[rowIndex])));
    /** @type {any} */
    const effectiveOrientation = orientation === 'auto'
        ? (filteredRowIndexes.length <= 1 ? 'columns' : (colIndexes.length > filteredRowIndexes.length + 1 ? 'columns' : 'rows'))
        : orientation;

    if (filteredRowIndexes.length === 0) {
        return {
            title: null,
            subtitle: null,
            categories: [],
            categoryTargets: [],
            series: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            emptyMessage: `No selected rows found at hierarchy level ${hierarchyLevel}.`,
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
    }

    if (filteredRowIndexes.length === 1 && orientation !== 'rows') {
        const rowIndex = filteredRowIndexes[0];
        const row = visibleRows[rowIndex];
        /** @type {any} */
        const resolveColumnLabel = buildColumnLabelResolver(
            colIndexes.map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => visibleCols[colIndex]).filter(Boolean)
        );
        /** @type {any} */
        const entries = colIndexes
            .map(/** @param {any} [colIndex] @returns {any} */ (colIndex) => ({
                column: visibleCols[colIndex],
                label: resolveColumnLabel(visibleCols[colIndex]),
                value: parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]),
            }))
            .filter(/** @param {any} [entry] @returns {any} */ (entry) => entry.value !== null)
            .slice(0, maxColumns);

        if (entries.length === 0) {
            return {
                title: null,
                subtitle: null,
                categories: [],
                categoryTargets: [],
                series: [],
                stackedGroups: [],
                icicleNodes: [],
                sunburstNodes: [],
                sankeyNodes: [],
                sankeyLinks: [],
                emptyMessage: 'The selected row does not contain numeric values.',
                availableLevels,
                maxHierarchyLevel,
                activeHierarchyLevel: hierarchyLevel,
            };
        }

        /** @type {Record<string, any>} */
        const singleRowModel = {
            title: null,
            subtitle: null,
            categories: entries.map(/** @param {any} [entry] @returns {any} */ (entry) => entry.label),
            categoryTargets: entries.map(/** @param {any} [entry] @returns {any} */ (entry) => buildColumnTarget(entry.column, colFields)),
            categoryBands: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            series: [{
                name: getRowLabel(row, 0),
                values: entries.map(/** @param {any} [entry] @returns {any} */ (entry) => entry.value),
            }],
            note: colIndexes.length > entries.length ? `showing first ${entries.length} numeric columns` : null,
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
        return sortChartModel(singleRowModel, sortMode);
    }

    /** @type {any} */
    const builder = effectiveOrientation === 'columns'
        ? buildSeriesFromColumnCategories
        : buildSeriesFromRowCategories;
    /** @type {any} */
    const rowSeries = builder({
        rowIndexes: filteredRowIndexes,
        colIndexes,
        visibleRows,
        visibleCols,
        grid,
        maxRows,
        maxColumns,
        rowFields,
        colFields,
    });

    if (rowSeries.series.length === 0) {
        return {
            title: null,
            subtitle: null,
            categories: [],
            categoryTargets: [],
            series: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            emptyMessage: rowSeries.note || 'No numeric values found in the selected range.',
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
    }

    /** @type {any} */
    const hierarchySourceRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    /** @type {any} */
    const firstNumericColumn = visibleCols.find(/** @param {any} [column] @returns {any} */ (column) => (
        column
        && !INTERNAL_COL_IDS.has(column.id)
        && selectedRows.some(/** @param {any} [row] @returns {any} */ (row) => parseNumeric(getCellValue(row, column.id)) !== null)
    ));
    /** @type {any} */
    const hierarchyModel = firstNumericColumn
        ? buildIcicleNodes(hierarchySourceRows, firstNumericColumn)
        : { nodes: [], maxDepth: 0 };
    /** @type {any} */
    const sortedHierarchyNodes = sortHierarchyNodes(hierarchyModel.nodes, sortMode);
    /** @type {any} */
    const sankeyModel = buildHierarchySankey(sortedHierarchyNodes);

    /** @type {Record<string, any>} */
    const rawModel = {
        title: null,
        subtitle: null,
        categories: rowSeries.categories,
        categoryTargets: rowSeries.categoryTargets || [],
        categoryBands: rowSeries.categoryBands || [],
        stackedGroups: [],
        icicleNodes: sortedHierarchyNodes,
        sunburstNodes: sortedHierarchyNodes,
        sankeyNodes: sankeyModel.nodes,
        sankeyLinks: sankeyModel.links,
        icicleDepth: hierarchyModel.maxDepth,
        series: rowSeries.series,
        note: rowSeries.note,
        availableLevels,
        maxHierarchyLevel,
        activeHierarchyLevel: hierarchyLevel,
    };
    return sortChartModel(rawModel, sortMode);
};

/** @param {any} [displayRows] @param {any} [visibleCols] @param {any} [options] @returns {any} */
export const buildPivotChartModel = (displayRows, visibleCols, options = {}) => {
    /** @type {any} */
    const orientation = options.orientation || 'rows';
    /** @type {any} */
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    /** @type {any} */
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_PANEL_CATEGORIES);
    /** @type {any} */
    const maxColumns = normalizePositiveLimit(options.maxColumns, MAX_PANEL_SERIES);
    /** @type {any} */
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    /** @type {any} */
    const colFields = Array.isArray(options.colFields) ? options.colFields : [];
    /** @type {any} */
    const sortMode = options.sortMode || 'natural';
    /** @type {any} */
    const allVisibleRows = (displayRows || []).filter(/** @param {any} [row] @returns {any} */ (row) => !isTotalRow(row));
    const orderedRows = allVisibleRows;
    /** @type {any} */
    const availableLevels = getAvailableLevels(allVisibleRows);
    /** @type {any} */
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    /** @type {any} */
    const filteredVisibleRows = getHierarchyDisplayRows(allVisibleRows, hierarchyLevel);
    /** @type {any} */
    const chartRows = filteredVisibleRows.slice(0, maxRows);
    if (chartRows.length === 0) {
        return {
            title: null,
            subtitle: null,
            categories: [],
            categoryTargets: [],
            series: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            emptyMessage: hierarchyLevel === 'all'
                ? 'The current pivot view does not have visible data rows to chart.'
                : `No visible rows found at hierarchy level ${hierarchyLevel}.`,
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
    }

    /** @type {any} */
    const candidateColumns = (visibleCols || []).filter(/** @param {any} [column] @returns {any} */ (column) => !INTERNAL_COL_IDS.has(column.id));
    /** @type {any} */
    const numericColumns = candidateColumns.filter(/** @param {any} [column] @returns {any} */ (column) => (
        chartRows.some(/** @param {any} [row] @returns {any} */ (row) => parseNumeric(getCellValue(row, column.id)) !== null)
    ));
    /** @type {any} */
    const seriesColumns = numericColumns.slice(0, maxColumns);
    /** @type {any} */
    const resolveColumnLabel = buildColumnLabelResolver(seriesColumns);
    /** @type {any} */
    const icicleModel = seriesColumns.length > 0
        ? buildIcicleNodes(orderedRows, seriesColumns[0])
        : { nodes: [], maxDepth: 0 };
    /** @type {any} */
    const sortedHierarchyNodes = sortHierarchyNodes(icicleModel.nodes, sortMode);
    /** @type {any} */
    const sankeyModel = buildHierarchySankey(sortedHierarchyNodes);

    if (seriesColumns.length === 0) {
        return {
            title: null,
            subtitle: null,
            categories: [],
            categoryTargets: [],
            series: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            icicleDepth: 0,
            emptyMessage: 'No visible numeric columns are available for the live chart panel.',
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
    }

    /** @type {any[]} */
    const notes = [];
    if (filteredVisibleRows.length > chartRows.length) {
        notes.push(`showing first ${chartRows.length} visible rows`);
    }
    if (numericColumns.length > seriesColumns.length) {
        notes.push(`showing first ${seriesColumns.length} numeric columns`);
    }

    if (orientation === 'columns') {
        /** @type {any} */
        const categories = seriesColumns.map(/** @param {any} [column] @returns {any} */ (column) => resolveColumnLabel(column));
        /** @type {any} */
        const rowSeriesRows = chartRows.slice(0, maxRows);
        if (chartRows.length > rowSeriesRows.length) {
            notes.push(`showing first ${rowSeriesRows.length} visible rows as series`);
        }
        /** @type {any} */
        const rowSeries = rowSeriesRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => ({
            name: getRowLabel(row, index),
            values: seriesColumns.map(/** @param {any} [column] @returns {any} */ (column) => parseNumeric(getCellValue(row, column.id))),
        })).filter(/** @param {any} [entry] @returns {any} */ (entry) => entry.values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null));

        /** @type {Record<string, any>} */
        const rawModel = {
            title: null,
            subtitle: null,
            categories,
            categoryTargets: seriesColumns.map(/** @param {any} [column] @returns {any} */ (column) => ({
                ...buildColumnTarget(column, colFields),
                label: resolveColumnLabel(column),
            })),
            categoryBands: [],
            stackedGroups: [],
            icicleNodes: sortedHierarchyNodes,
            sunburstNodes: sortedHierarchyNodes,
            sankeyNodes: sankeyModel.nodes,
            sankeyLinks: sankeyModel.links,
            icicleDepth: icicleModel.maxDepth,
            series: rowSeries,
            note: notes.length > 0 ? notes.join(', ') : null,
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
        return sortChartModel(rawModel, sortMode);
    }

    /** @type {any} */
    const categories = chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => getRowLabel(row, index));
    /** @type {any} */
    const series = seriesColumns.map(/** @param {any} [column] @returns {any} */ (column) => ({
        name: resolveColumnLabel(column),
        values: chartRows.map(/** @param {any} [row] @returns {any} */ (row) => parseNumeric(getCellValue(row, column.id))),
    })).filter(/** @param {any} [entry] @returns {any} */ (entry) => entry.values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null));
    /** @type {any} */
    const firstSeries = series[0] || null;
    /** @type {Record<string, any>} */
    const numericValuesByPath = {};
    if (firstSeries) {
        chartRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
            numericValuesByPath[getRowPath(row)] = firstSeries.values[index];
        });
        orderedRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
            /** @type {any} */
            const rowPath = getRowPath(row);
            if (numericValuesByPath[rowPath] === undefined) {
                numericValuesByPath[rowPath] = getCellValue(row, seriesColumns[0].id);
            }
        });
    }
    /** @type {any} */
    const categoryBands = buildCategoryBandsForRows(chartRows.map(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => index), chartRows);
    /** @type {any} */
    const stackedGroups = (() => {
        if (!(series.length === 1 && firstSeries)) return [];
        if (hierarchyLevel === 'all') {
            /** @type {any} */
            const frontierGroups = buildFrontierStackedGroups(chartRows, categoryBands, numericValuesByPath, maxRows);
            return frontierGroups.some(/** @param {any} [group] @returns {any} */ (group) => Array.isArray(group.segments) && group.segments.length > 1)
                ? frontierGroups
                : [];
        }
        if (typeof hierarchyLevel === 'number' && hierarchyLevel < maxHierarchyLevel) {
            /** @type {any} */
            const nestedGroups = buildStackedGroupsForRows(chartRows, orderedRows, numericValuesByPath, maxRows);
            return nestedGroups.some(/** @param {any} [group] @returns {any} */ (group) => Array.isArray(group.segments) && group.segments.length > 1)
                ? nestedGroups
                : [];
        }
        return [];
    })();

    /** @type {Record<string, any>} */
    const rawModel = {
        title: null,
        subtitle: null,
        categories,
        categoryTargets: chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => buildRowTarget(row, index, rowFields)),
        categoryBands,
        stackedGroups,
        icicleNodes: sortedHierarchyNodes,
        sunburstNodes: sortedHierarchyNodes,
        sankeyNodes: sankeyModel.nodes,
        sankeyLinks: sankeyModel.links,
        icicleDepth: icicleModel.maxDepth,
        series,
        note: notes.length > 0 ? notes.join(', ') : null,
        availableLevels,
        maxHierarchyLevel,
        activeHierarchyLevel: hierarchyLevel,
    };
    return sortChartModel(rawModel, sortMode);
};

/** @param {any} [layers] @param {any} [chartRows] @param {any} [visibleCols] @returns {any} */
export const buildComboLayersForRows = (layers, chartRows, visibleCols) => {
    /** @type {any} */
    const chartableColumns = getChartableColumns(visibleCols);
    return normalizeComboLayers(layers, chartableColumns)
        .map(/** @param {any} [layer] @param {any} [index] @returns {any} */ (layer, index) => {
            /** @type {any} */
            const column = chartableColumns.find(/** @param {any} [candidate] @returns {any} */ (candidate) => candidate.id === layer.columnId) || null;
            if (!column) return null;
            /** @type {any} */
            const values = chartRows.map(/** @param {any} [row] @returns {any} */ (row) => parseNumeric(getCellValue(row, column.id)));
            if (!values.some(/** @param {any} [value] @returns {any} */ (value) => value !== null)) return null;
            return {
                ...layer,
                color: getColorForIndex(index, { primary: DEFAULT_COLORS[0] }),
                name: layer.name || getColumnLabel(column),
                columnId: column.id,
                values,
            };
        })
        .filter(Boolean);
};

/** @param {any} [message] @param {any} [hierarchyLevel] @param {any} [maxHierarchyLevel] @param {any} [availableLevels] @returns {any} */
export const buildComboEmptyModel = (message, hierarchyLevel, maxHierarchyLevel, availableLevels) => ({
    title: null,
    subtitle: null,
    categories: [],
    categoryTargets: [],
    categoryBands: [],
    layers: [],
    series: [],
    stackedGroups: [],
    icicleNodes: [],
    sunburstNodes: [],
    sankeyNodes: [],
    sankeyLinks: [],
    emptyMessage: message,
    availableLevels,
    maxHierarchyLevel,
    activeHierarchyLevel: hierarchyLevel,
});

/** @param {any} [displayRows] @param {any} [visibleCols] @param {any} [options] @returns {any} */
export const buildComboPivotChartModel = (displayRows, visibleCols, options = {}) => {
    /** @type {any} */
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    /** @type {any} */
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_PANEL_CATEGORIES);
    /** @type {any} */
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    /** @type {any} */
    const sortMode = options.sortMode || 'natural';
    /** @type {any} */
    const allVisibleRows = (displayRows || []).filter(/** @param {any} [row] @returns {any} */ (row) => !isTotalRow(row));
    /** @type {any} */
    const availableLevels = getAvailableLevels(allVisibleRows);
    /** @type {any} */
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    /** @type {any} */
    const filteredVisibleRows = getHierarchyDisplayRows(allVisibleRows, hierarchyLevel);
    /** @type {any} */
    const chartRows = filteredVisibleRows.slice(0, maxRows);
    if (chartRows.length === 0) {
        return buildComboEmptyModel(
            hierarchyLevel === 'all'
                ? 'The current pivot view does not have visible rows for a combo chart.'
                : `No visible rows found at hierarchy level ${hierarchyLevel}.`,
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    /** @type {any} */
    const modelLayers = buildComboLayersForRows(options.layers, chartRows, visibleCols);
    if (modelLayers.length === 0) {
        return buildComboEmptyModel(
            'No visible numeric columns are available for the combo chart layers.',
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    /** @type {any[]} */
    const notes = [];
    if (filteredVisibleRows.length > chartRows.length) {
        notes.push(`showing first ${chartRows.length} visible rows`);
    }

    /** @type {Record<string, any>} */
    const rawModel = {
        title: null,
        subtitle: null,
        categories: chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => getRowLabel(row, index)),
        categoryTargets: chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => buildRowTarget(row, index, rowFields)),
        categoryBands: buildCategoryBandsForRows(chartRows.map(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => index), chartRows),
        layers: modelLayers,
        series: modelLayers.map(/** @param {any} [layer] @returns {any} */ (layer) => ({ name: layer.name, values: layer.values })),
        stackedGroups: [],
        icicleNodes: [],
        sunburstNodes: [],
        sankeyNodes: [],
        sankeyLinks: [],
        note: notes.length > 0 ? notes.join(', ') : null,
        availableLevels,
        maxHierarchyLevel,
        activeHierarchyLevel: hierarchyLevel,
    };
    return sortChartModel(rawModel, sortMode);
};

/** @param {any} [selectionMap] @param {any} [visibleRows] @param {any} [visibleCols] @param {any} [options] @returns {any} */
export const buildComboSelectionChartModel = (selectionMap, visibleRows, visibleCols, options = {}) => {
    /** @type {any} */
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    /** @type {any} */
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_SELECTION_CATEGORIES);
    /** @type {any} */
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    /** @type {any} */
    const sortMode = options.sortMode || 'natural';
    /** @type {any} */
    const bounds = buildSelectionBounds(selectionMap, visibleRows, visibleCols);
    if (!bounds) {
        return buildComboEmptyModel(
            'Select a range of value cells to build a combo chart.',
            hierarchyLevel,
            options.maxHierarchyLevel || 1,
            []
        );
    }

    const { rowIndexes, colIndexes } = bounds;
    /** @type {any} */
    const selectedRows = rowIndexes.map(/** @param {any} [rowIndex] @returns {any} */ (rowIndex) => visibleRows[rowIndex]).filter(Boolean);
    /** @type {any} */
    const availableLevels = getAvailableLevels(selectedRows);
    /** @type {any} */
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    /** @type {any} */
    const displayRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    /** @type {any} */
    const chartRows = displayRows.slice(0, maxRows);
    if (chartRows.length === 0) {
        return buildComboEmptyModel(
            `No selected rows found at hierarchy level ${hierarchyLevel}.`,
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    /** @type {any} */
    const selectedColumnIds = new Set(
        colIndexes
            .map(/** @param {any} [index] @returns {any} */ (index) => visibleCols[index])
            .filter(/** @param {any} [column] @returns {any} */ (column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
            .map(/** @param {any} [column] @returns {any} */ (column) => column.id)
    );
    /** @type {any} */
    const selectionColumns = getChartableColumns(visibleCols).filter(/** @param {any} [column] @returns {any} */ (column) => selectedColumnIds.has(column.id));
    /** @type {any} */
    const modelLayers = buildComboLayersForRows(options.layers, chartRows, selectionColumns);
    if (modelLayers.length === 0) {
        return buildComboEmptyModel(
            'The selected range does not contain numeric columns that can be layered.',
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    /** @type {Record<string, any>} */
    const rawModel = {
        title: null,
        subtitle: null,
        categories: chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => getRowLabel(row, index)),
        categoryTargets: chartRows.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => buildRowTarget(row, index, rowFields)),
        categoryBands: buildCategoryBandsForRows(chartRows.map(/** @param {any} [_] @param {any} [index] @returns {any} */ (_, index) => index), chartRows),
        layers: modelLayers,
        series: modelLayers.map(/** @param {any} [layer] @returns {any} */ (layer) => ({ name: layer.name, values: layer.values })),
        stackedGroups: [],
        icicleNodes: [],
        sunburstNodes: [],
        sankeyNodes: [],
        sankeyLinks: [],
        note: selectedRows.length > chartRows.length ? `showing first ${chartRows.length} selected rows` : null,
        availableLevels,
        maxHierarchyLevel,
        activeHierarchyLevel: hierarchyLevel,
    };
    return sortChartModel(rawModel, sortMode);
};

/** @param {any} [points] @param {any} [closeToY] @returns {any} */
export const buildLinePath = (points, closeToY = null) => {
    /** @type {any} */
    const validPoints = points.filter(/** @param {any} [point] @returns {any} */ (point) => point && Number.isFinite(point.x) && Number.isFinite(point.y));
    if (validPoints.length === 0) return '';
    const head = validPoints[0];
    /** @type {any[]} */
    const segments = [`M ${head.x} ${head.y}`];
    for (let index = 1; index < validPoints.length; index += 1) {
        segments.push(`L ${validPoints[index].x} ${validPoints[index].y}`);
    }
    if (closeToY !== null) {
        const tail = validPoints[validPoints.length - 1];
        segments.push(`L ${tail.x} ${closeToY}`);
        segments.push(`L ${head.x} ${closeToY}`);
        segments.push('Z');
    }
    return segments.join(' ');
};

/** @param {any} [points] @returns {any} */
export const buildAreaBandPath = (points) => {
    /** @type {any} */
    const validPoints = points.filter(/** @param {any} [point] @returns {any} */ (point) => (
        point
        && Number.isFinite(point.x)
        && Number.isFinite(point.y)
        && Number.isFinite(point.baseY)
    ));
    if (validPoints.length === 0) return '';
    /** @type {any[]} */
    const segments = [`M ${validPoints[0].x} ${validPoints[0].y}`];
    for (let index = 1; index < validPoints.length; index += 1) {
        segments.push(`L ${validPoints[index].x} ${validPoints[index].y}`);
    }
    for (let index = validPoints.length - 1; index >= 0; index -= 1) {
        segments.push(`L ${validPoints[index].x} ${validPoints[index].baseY}`);
    }
    segments.push('Z');
    return segments.join(' ');
};

/** @param {any} [minVal] @param {any} [maxVal] @param {any} [count] @returns {any} */
export const niceTickValues = (minVal, maxVal, count) => {
    /** @type {any} */
    const rawSpan = maxVal - minVal;
    if (rawSpan === 0) return [minVal];
    /** @type {any} */
    const rawStep = rawSpan / Math.max(count - 1, 1);
    /** @type {any} */
    const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
    /** @type {any} */
    const residual = rawStep / magnitude;
    /** @type {any} */
    const niceStep = residual <= 1.5 ? magnitude : residual <= 3 ? 2 * magnitude : residual <= 7 ? 5 * magnitude : 10 * magnitude;
    /** @type {any} */
    const niceMin = Math.floor(minVal / niceStep) * niceStep;
    /** @type {any} */
    const niceMax = Math.ceil(maxVal / niceStep) * niceStep;
    /** @type {any[]} */
    const ticks = [];
    for (let v = niceMin; v <= niceMax + niceStep * 0.01; v += niceStep) {
        ticks.push(Math.round(v * 1e10) / 1e10);
    }
    return ticks;
};

/** @param {any} [values] @param {any} [chartHeight] @param {any} [margin] @returns {any} */
export const getAxisMetrics = (values, chartHeight, margin) => {
    /** @type {any} */
    const numericValues = (values || []).filter(/** @param {any} [value] @returns {any} */ (value) => typeof value === 'number' && Number.isFinite(value));
    /** @type {any} */
    const minValue = numericValues.length > 0 ? Math.min(...numericValues, 0) : 0;
    /** @type {any} */
    const maxValue = numericValues.length > 0 ? Math.max(...numericValues, 0) : 0;
    /** @type {any} */
    const span = maxValue - minValue || 1;
    /** @param {any} [value] @returns {any} */
    const scaleY = (value) => {
        /** @type {any} */
        const normalized = (value - minValue) / span;
        return chartHeight - margin.bottom - (normalized * (chartHeight - margin.top - margin.bottom));
    };
    /** @type {any} */
    const baselineY = scaleY(0);
    /** @type {any} */
    const tickValues = niceTickValues(minValue, maxValue, 5);
    return {
        minValue,
        maxValue,
        scaleY,
        baselineY,
        tickValues,
    };
};

/** @param {any} [nodes] @param {any} [x] @param {any} [width] @param {any} [level] @param {any} [bandHeight] @param {any} [acc] @returns {any} */
export const layoutIcicleNodes = (nodes, x, width, level, bandHeight, acc = []) => {
    if (!Array.isArray(nodes) || nodes.length === 0 || width <= 0 || bandHeight <= 0) {
        return acc;
    }

    /** @type {any} */
    const weights = nodes.map((node) => {
        /** @type {any} */
        const numericValue = typeof node.value === 'number' && Number.isFinite(node.value)
            ? Math.abs(node.value)
            : 0;
        return numericValue;
    });
    /** @type {any} */
    const totalWeight = weights.reduce(/** @param {any} [sum] @param {any} [value] @returns {any} */ (sum, value) => sum + value, 0);
    let cursor = x;

    nodes.forEach((node, index) => {
        /** @type {any} */
        const fallbackWidth = width / Math.max(nodes.length, 1);
        /** @type {any} */
        const nodeWidth = totalWeight > 0
            ? (weights[index] / totalWeight) * width
            : fallbackWidth;
        /** @type {any} */
        const safeWidth = Math.max(1, nodeWidth);
        /** @type {Record<string, any>} */
        const layoutNode = {
            ...node,
            x: cursor,
            y: level * bandHeight,
            width: safeWidth,
            height: bandHeight,
        };
        acc.push(layoutNode);
        if (Array.isArray(node.children) && node.children.length > 0) {
            layoutIcicleNodes(node.children, cursor, safeWidth, level + 1, bandHeight, acc);
        }
        cursor += safeWidth;
    });

    return acc;
};

/** @param {any} [model] @returns {any} */
export const canStackBarSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
/** @param {any} [model] @returns {any} */
export const canStackBarLayout = (model) => (
    (Array.isArray(model && model.stackedGroups) && model.stackedGroups.length > 0)
    || canStackBarSeries(model)
);

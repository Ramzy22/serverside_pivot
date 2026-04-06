/**
 * Chart model building utilities — pure functions, no React.
 * Extracted from PivotCharts.js for separation of concerns.
 */
import { formatDisplayLabel } from '../../utils/helpers';
export const INTERNAL_COL_IDS = new Set(['hierarchy', '__row_number__']);
export const MAX_PANEL_CATEGORIES = 50;
export const MAX_PANEL_SERIES = 12;
export const MAX_SELECTION_CATEGORIES = 50;
export const MAX_SELECTION_SERIES = 12;
export const CHART_HEIGHT = 320;
export const CHART_WIDTH = 760;

export const DEFAULT_COLORS = ['#2563EB', '#F97316', '#0F766E', '#7C3AED', '#DC2626', '#0891B2'];
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
export const COLOR_PALETTE_NAMES = Object.keys(COLOR_PALETTES);
export const resolvePalette = (name) => COLOR_PALETTES[name] || DEFAULT_COLORS;
export const VALID_COMBO_LAYER_TYPES = new Set(['bar', 'line', 'area']);
export const VALID_COMBO_LAYER_AXES = new Set(['left', 'right']);
export const DEFAULT_COMBO_LAYER_SEQUENCE = ['bar', 'line', 'area'];
export const normalizePositiveLimit = (value, fallback) => {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue) || numericValue < 1) return fallback;
    return Math.max(1, Math.floor(numericValue));
};

export const parseNumeric = (value) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(/,/g, '').replace(/%$/, '');
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
};

export const stringifyValue = (value) => {
    if (value === null || value === undefined) return '';
    return String(value);
};

export const formatChartNumber = (value) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '';
    const decimals = Math.abs(value) >= 100 ? 0 : 2;
    const fixed = value.toFixed(decimals).replace(/\.00$/, '');
    const parts = fixed.split('.');
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    return parts.join('.');
};

export const VALUE_FORMAT_MODES = [
    { value: 'auto', label: 'Auto' },
    { value: 'number', label: 'Number' },
    { value: 'compact', label: 'Compact' },
    { value: 'percent', label: 'Percent' },
    { value: 'currency', label: 'Currency' },
];

export const formatChartValue = (value, mode) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '';
    if (!mode || mode === 'auto') return formatChartNumber(value);
    if (mode === 'compact') {
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

export const estimateTextWidth = (text, fontSize = 11) => Math.ceil(String(text || '').length * fontSize * 0.62);

export const truncateChartLabel = (value, maxLength) => {
    const label = String(value || '');
    return label.length > maxLength ? `${label.slice(0, maxLength)}...` : label;
};

export const getColumnLabel = (column) => {
    if (!column) return 'Value';
    const header = column.columnDef ? column.columnDef.header : null;
    if (typeof header === 'string' && header.trim()) return formatDisplayLabel(header);
    return formatDisplayLabel(column.id || 'Value');
};

export const getColumnHeaderSegments = (column) => {
    const segments = [];
    let current = column;

    while (current) {
        const header = current.columnDef ? current.columnDef.header : null;
        const headerVal = current.headerVal || (current.columnDef && current.columnDef.headerVal) || null;
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

export const buildColumnLabelResolver = (columns) => {
    const resolvedColumns = (columns || []).filter(Boolean);
    const segmentSets = resolvedColumns.map((column) => getColumnHeaderSegments(column));
    const leafLabels = Array.from(new Set(
        segmentSets
            .map((segments) => segments[segments.length - 1])
            .filter(Boolean)
    ));
    const omitRepeatedLeaf = leafLabels.length === 1 && segmentSets.some((segments) => segments.length > 1);

    return (column) => {
        if (!column) return 'Value';
        const segments = getColumnHeaderSegments(column);
        const displaySegments = omitRepeatedLeaf && segments.length > 1
            ? segments.slice(0, -1)
            : segments;
        return (displaySegments.length > 0 ? displaySegments : segments).join(' > ') || getColumnLabel(column);
    };
};

export const getColumnValueSegments = (column) => {
    const segments = [];
    let current = column;

    while (current) {
        const header = current.columnDef ? current.columnDef.header : null;
        const headerVal = current.headerVal || (current.columnDef && current.columnDef.headerVal) || null;
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

export const buildColumnFieldValues = (column, fields = []) => {
    const rawSegments = getColumnValueSegments(column);
    const effectiveSegments = rawSegments.slice(0, Math.max(0, fields.length));
    return effectiveSegments.reduce((acc, value, index) => {
        const field = fields[index];
        if (field && value !== undefined && value !== null && String(value).trim() !== '') {
            acc[field] = String(value);
        }
        return acc;
    }, {});
};

export const getChartableColumns = (visibleCols = []) => (
    (visibleCols || []).filter((column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
);

export const getComboLayerDefaultName = (column, index = 0) => (
    column && column.id ? getColumnLabel(column) : `Series ${index + 1}`
);

export const buildDefaultCartesianLayers = (visibleCols = [], maxLayers = 3) => {
    const chartableColumns = getChartableColumns(visibleCols).slice(0, Math.max(1, Math.floor(Number(maxLayers) || 1)));
    return chartableColumns.map((column, index) => ({
        id: `layer-${index + 1}`,
        type: DEFAULT_COMBO_LAYER_SEQUENCE[index % DEFAULT_COMBO_LAYER_SEQUENCE.length],
        columnId: column.id,
        axis: index === 0 ? 'left' : (index === 1 ? 'right' : 'left'),
        name: getComboLayerDefaultName(column, index),
        hidden: false,
    }));
};

export const buildGroupedChartColumnOptions = (columns = []) => {
    const resolveColumnLabel = buildColumnLabelResolver(columns);
    return columns
        .map((column) => {
            const segments = getColumnHeaderSegments(column);
            const leafLabel = segments[segments.length - 1] || getColumnLabel(column);
            const groupLabel = segments.length > 1 ? segments.slice(0, -1).join(' / ') : 'Measures';
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
        .sort((left, right) => (
            left.groupLabel.localeCompare(right.groupLabel, undefined, { numeric: true, sensitivity: 'base' })
            || left.fullLabel.localeCompare(right.fullLabel, undefined, { numeric: true, sensitivity: 'base' })
        ));
};

export const normalizeComboLayer = (layer, availableColumns = [], index = 0, fallbackLayer = null) => {
    const source = layer && typeof layer === 'object' ? layer : {};
    const fallback = fallbackLayer && typeof fallbackLayer === 'object' ? fallbackLayer : {};
    const chartableColumns = getChartableColumns(availableColumns);
    const fallbackColumn = chartableColumns[0] || null;
    const requestedColumnId = source.columnId || fallback.columnId || (fallbackColumn && fallbackColumn.id) || null;
    const matchingColumn = chartableColumns.find((column) => column.id === requestedColumnId) || null;
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
    };
};

export const normalizeComboLayers = (layers, availableColumns = [], fallbackLayers = []) => {
    const sourceLayers = Array.isArray(layers) ? layers : [];
    const normalized = sourceLayers
        .filter((layer) => layer && typeof layer === 'object')
        .map((layer, index) => normalizeComboLayer(layer, availableColumns, index, Array.isArray(fallbackLayers) ? fallbackLayers[index] : null));

    if (normalized.length > 0) return normalized;
    const defaultLayers = Array.isArray(fallbackLayers) && fallbackLayers.length > 0
        ? fallbackLayers
        : buildDefaultCartesianLayers(availableColumns);
    return defaultLayers.map((layer, index) => normalizeComboLayer(layer, availableColumns, index, layer));
};

export const getRowLabel = (row, fallbackIndex = 0) => {
    if (!row) return `Row ${fallbackIndex + 1}`;
    if ((row.original && row.original._path === '__grand_total__') || row._path === '__grand_total__') return 'Grand Total';
    const explicitId = row.original && row.original._id ? row.original._id : row._id;
    if (explicitId !== null && explicitId !== undefined && String(explicitId).trim() !== '') {
        return formatDisplayLabel(String(explicitId));
    }
    if (typeof row.getValue === 'function') {
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

export const getRowDepth = (row) => {
    if (!row) return 0;
    const explicitDepth = row.original && typeof row.original.depth === 'number'
        ? row.original.depth
        : (typeof row.depth === 'number' ? row.depth : null);
    if (explicitDepth !== null) return explicitDepth;
    return typeof row.depth === 'number' ? row.depth : 0;
};

export const getRowLevel = (row) => getRowDepth(row) + 1;
export const getRowPath = (row) => {
    if (!row) return '';
    if (row.original && row.original._path) return String(row.original._path);
    if (row._path) return String(row._path);
    return String(row.id || '');
};

export const buildRowFieldValues = (rowPath, fields = []) => {
    const pathParts = rowPath ? String(rowPath).split('|||') : [];
    return fields.reduce((acc, field, index) => {
        const value = pathParts[index];
        if (field && value !== undefined && value !== null && String(value).trim() !== '') {
            acc[field] = String(value);
        }
        return acc;
    }, {});
};

export const buildRowTarget = (row, fallbackIndex = 0, fields = []) => {
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

export const buildColumnTarget = (column, fields = []) => ({
    kind: 'column',
    columnId: column && column.id ? column.id : null,
    label: buildColumnLabelResolver([column])(column),
    pathValues: getColumnValueSegments(column),
    fieldValues: buildColumnFieldValues(column, fields),
});
export const isDescendantPath = (ancestorPath, candidatePath) => (
    Boolean(ancestorPath) &&
    Boolean(candidatePath) &&
    candidatePath.indexOf(`${ancestorPath}|||`) === 0
);

export const isTotalRow = (row) => Boolean(
    row &&
    (
        (row.original && (row.original._isTotal || row.original.__isGrandTotal__ || row.original._path === '__grand_total__'))
        || row._isTotal
        || row.__isGrandTotal__
        || row._path === '__grand_total__'
    )
);

export const getCellValue = (row, columnId) => {
    if (!row || !columnId) return null;
    if (typeof row.getValue === 'function') {
        const resolvedValue = row.getValue(columnId);
        if (resolvedValue !== undefined && resolvedValue !== null && resolvedValue !== '') {
            return resolvedValue;
        }
    }
    const rowCells = typeof row.getAllCells === 'function'
        ? row.getAllCells()
        : (typeof row.getVisibleCells === 'function' ? row.getVisibleCells() : []);
    if (Array.isArray(rowCells) && rowCells.length > 0) {
        const matchingCell = rowCells.find((cell) => cell && cell.column && cell.column.id === columnId);
        if (matchingCell) {
            if (typeof matchingCell.getValue === 'function') {
                const cellValue = matchingCell.getValue();
                if (cellValue !== undefined && cellValue !== null && cellValue !== '') {
                    return cellValue;
                }
            }
            if (typeof matchingCell.renderValue === 'function') {
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

export const getAvailableLevels = (rows) => Array.from(new Set(
    (rows || [])
        .filter((row) => row && !isTotalRow(row))
        .map((row) => getRowLevel(row))
)).sort((a, b) => a - b);

export const getHierarchyDisplayRows = (rows, hierarchyLevel) => {
    const orderedRows = (rows || []).filter((row) => row && !isTotalRow(row));
    if (orderedRows.length === 0) return [];

    const pathToRow = new Map();
    orderedRows.forEach((row) => {
        const path = getRowPath(row);
        if (path) pathToRow.set(path, row);
    });

    const frontierRows = orderedRows.filter((row, index) => {
        const currentPath = getRowPath(row);
        const nextRow = orderedRows[index + 1];
        const nextPath = nextRow ? getRowPath(nextRow) : '';
        return !isDescendantPath(currentPath, nextPath);
    });

    if (hierarchyLevel === 'all') return frontierRows;

    const selectedPaths = new Set();
    frontierRows.forEach((row) => {
        const rowPath = getRowPath(row);
        const pathParts = rowPath ? rowPath.split('|||') : [];
        const targetLevel = Math.min(Number(hierarchyLevel) || 1, Math.max(pathParts.length, 1));
        let resolvedPath = '';
        for (let level = targetLevel; level >= 1; level -= 1) {
            const candidatePath = pathParts.slice(0, level).join('|||');
            if (candidatePath && pathToRow.has(candidatePath)) {
                resolvedPath = candidatePath;
                break;
            }
        }
        selectedPaths.add(resolvedPath || rowPath);
    });

    return orderedRows.filter((row) => selectedPaths.has(getRowPath(row)));
};

export const getColorForIndex = (index, theme, colors) => {
    const base = colors || DEFAULT_COLORS;
    const palette = [theme.primary, ...base.filter((color) => color !== theme.primary)];
    return palette[index % palette.length];
};

export const buildSelectionBounds = (selectionMap, visibleRows, visibleCols) => {
    const keys = Object.keys(selectionMap || {});
    if (keys.length === 0) return null;

    const rowIndexById = {};
    visibleRows.forEach((row, index) => {
        rowIndexById[row.id] = index;
    });
    const colIndexById = {};
    visibleCols.forEach((column, index) => {
        colIndexById[column.id] = index;
    });

    let minRow = Infinity;
    let maxRow = -1;
    let minCol = Infinity;
    let maxCol = -1;
    const grid = {};

    keys.forEach((selectionKey) => {
        const separatorIndex = selectionKey.lastIndexOf(':');
        if (separatorIndex <= 0) return;
        const rowId = selectionKey.slice(0, separatorIndex);
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

export const buildCategoryBandsForRows = (rowIndexes, visibleRows) => rowIndexes.map((rowIndex, index) => {
    const row = visibleRows[rowIndex];
    const path = getRowPath(row);
    const pathParts = path ? path.split('|||').map((part) => formatDisplayLabel(String(part))) : [];
    const currentLabel = getRowLabel(row, index);
    if (pathParts.length <= 1) {
        return {
            outerLabel: currentLabel,
            innerLabel: '',
            groupKey: path || currentLabel,
        };
    }
    const outerLabel = pathParts[pathParts.length - 2] || currentLabel;
    const innerLabel = pathParts[pathParts.length - 1] || currentLabel;
    return {
        outerLabel,
        innerLabel,
        groupKey: pathParts.slice(0, pathParts.length - 1).join('|||') || outerLabel,
    };
});

export const buildStackedGroupsForRows = (displayRows, allVisibleRows, numericValues, maxGroups) => {
    if (!Array.isArray(displayRows) || displayRows.length === 0) return [];
    const orderedRows = (allVisibleRows || []).filter((row) => row && !isTotalRow(row));
    const rowIndexByPath = new Map();
    orderedRows.forEach((row, index) => {
        rowIndexByPath.set(getRowPath(row), index);
    });

    return displayRows.slice(0, maxGroups).map((row, index) => {
        const rowPath = getRowPath(row);
        const rowLevel = getRowLevel(row);
        const rowIndex = rowIndexByPath.get(rowPath);
        const segments = [];

        if (rowIndex !== undefined) {
            for (let nextIndex = rowIndex + 1; nextIndex < orderedRows.length; nextIndex += 1) {
                const candidate = orderedRows[nextIndex];
                const candidatePath = getRowPath(candidate);
                if (!isDescendantPath(rowPath, candidatePath)) break;
                if (getRowLevel(candidate) === rowLevel + 1) {
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

export const buildFrontierStackedGroups = (displayRows, categoryBands, numericValues, maxGroups) => {
    const rows = Array.isArray(displayRows) ? displayRows.slice(0, maxGroups) : [];
    if (rows.length === 0) return [];

    return rows.reduce((groups, row, index) => {
        const band = Array.isArray(categoryBands) ? categoryBands[index] : null;
        const rowPath = getRowPath(row);
        const value = parseNumeric(numericValues[rowPath]);
        const segment = {
            label: band && band.innerLabel ? band.innerLabel : getRowLabel(row, index),
            value: value !== null ? value : 0,
            path: rowPath,
        };
        const groupKey = band && band.groupKey ? band.groupKey : (rowPath || `group-${index}`);
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

export const buildIcicleNodes = (rows, valueColumn) => {
    if (!valueColumn || !Array.isArray(rows) || rows.length === 0) {
        return { nodes: [], maxDepth: 0 };
    }

    const sourceRows = rows.filter((row) => row && !isTotalRow(row));
    if (sourceRows.length === 0) {
        return { nodes: [], maxDepth: 0 };
    }

    const nodeMap = new Map();
    let maxDepth = 1;

    sourceRows.forEach((row, index) => {
        const rowPath = getRowPath(row);
        if (!rowPath) return;
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

    const roots = [];
    nodeMap.forEach((node, rowPath) => {
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

export const cloneHierarchyNodes = (nodes) => (
    Array.isArray(nodes)
        ? nodes.map((node) => ({
            ...node,
            children: cloneHierarchyNodes(node.children),
        }))
        : []
);

export const sortHierarchyNodes = (nodes, sortMode) => {
    if (!Array.isArray(nodes)) return [];
    const normalizedMode = String(sortMode || 'natural').toLowerCase();
    const children = nodes.map((node) => ({
        ...node,
        children: sortHierarchyNodes(node.children, normalizedMode),
    }));
    if (normalizedMode === 'natural' || normalizedMode === 'none') {
        return children;
    }

    const compare = normalizedMode === 'label_asc'
        ? (left, right) => String(left.label || '').localeCompare(String(right.label || ''))
        : normalizedMode === 'label_desc'
            ? (left, right) => String(right.label || '').localeCompare(String(left.label || ''))
            : normalizedMode === 'value_asc'
                ? (left, right) => Math.abs(Number(left.value) || 0) - Math.abs(Number(right.value) || 0)
                : (left, right) => Math.abs(Number(right.value) || 0) - Math.abs(Number(left.value) || 0);
    return [...children].sort(compare);
};

export const sortChartModel = (model, sortMode) => {
    const normalizedMode = String(sortMode || 'natural').toLowerCase();
    if (!model || normalizedMode === 'natural' || normalizedMode === 'none') return model;
    if (!Array.isArray(model.categories) || !Array.isArray(model.series) || model.categories.length === 0) {
        return model;
    }

    const ordering = model.categories.map((category, index) => ({
        category,
        index,
        total: (model.series || []).reduce((sum, series) => {
            const value = Array.isArray(series && series.values) ? series.values[index] : null;
            return sum + (typeof value === 'number' && Number.isFinite(value) ? value : 0);
        }, 0),
    }));

    ordering.sort((left, right) => {
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

    const reorderedIndexes = ordering.map((item) => item.index);
    return {
        ...model,
        categories: reorderedIndexes.map((index) => model.categories[index]),
        categoryTargets: Array.isArray(model.categoryTargets) ? reorderedIndexes.map((index) => model.categoryTargets[index]) : model.categoryTargets,
        categoryBands: Array.isArray(model.categoryBands) ? reorderedIndexes.map((index) => model.categoryBands[index]) : model.categoryBands,
        stackedGroups: Array.isArray(model.stackedGroups) && model.stackedGroups.length === model.categories.length
            ? reorderedIndexes.map((index) => model.stackedGroups[index])
            : model.stackedGroups,
        series: (model.series || []).map((series) => ({
            ...series,
            values: reorderedIndexes.map((index) => series.values[index]),
        })),
    };
};

export const buildHierarchySankey = (nodes) => {
    if (!Array.isArray(nodes) || nodes.length === 0) {
        return { nodes: [], links: [], maxDepth: 0 };
    }

    const flatNodes = [];
    const links = [];
    let maxDepth = 1;

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
        (node.children || []).forEach((child) => visit(child, node));
    };

    nodes.forEach((node) => visit(node, null));
    return { nodes: flatNodes, links, maxDepth };
};

export const polarToCartesian = (cx, cy, radius, angle) => ({
    x: cx + (radius * Math.cos(angle)),
    y: cy + (radius * Math.sin(angle)),
});

export const describeDonutArc = (cx, cy, innerRadius, outerRadius, startAngle, endAngle) => {
    const safeSpan = Math.max(0.0001, Math.min((Math.PI * 2) - 0.0001, endAngle - startAngle));
    const safeEnd = startAngle + safeSpan;
    const largeArc = safeSpan > Math.PI ? 1 : 0;
    const outerStart = polarToCartesian(cx, cy, outerRadius, startAngle);
    const outerEnd = polarToCartesian(cx, cy, outerRadius, safeEnd);

    if (innerRadius <= 0) {
        return [
            `M ${cx} ${cy}`,
            `L ${outerStart.x} ${outerStart.y}`,
            `A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${outerEnd.x} ${outerEnd.y}`,
            'Z',
        ].join(' ');
    }

    const innerStart = polarToCartesian(cx, cy, innerRadius, safeEnd);
    const innerEnd = polarToCartesian(cx, cy, innerRadius, startAngle);
    return [
        `M ${outerStart.x} ${outerStart.y}`,
        `A ${outerRadius} ${outerRadius} 0 ${largeArc} 1 ${outerEnd.x} ${outerEnd.y}`,
        `L ${innerStart.x} ${innerStart.y}`,
        `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${innerEnd.x} ${innerEnd.y}`,
        'Z',
    ].join(' ');
};

export const layoutSunburstNodes = (nodes, startAngle, endAngle, depth, ringWidth, acc = []) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return acc;
    const weights = nodes.map((node) => Math.abs(Number(node.value) || 0));
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    let angleCursor = startAngle;

    nodes.forEach((node, index) => {
        const fallbackSweep = (endAngle - startAngle) / Math.max(nodes.length, 1);
        const sweep = totalWeight > 0 ? ((weights[index] || 0) / totalWeight) * (endAngle - startAngle) : fallbackSweep;
        const nextAngle = angleCursor + sweep;
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

export const layoutSankey = (nodes, links, width, height) => {
    if (!Array.isArray(nodes) || nodes.length === 0) return { nodes: [], links: [] };

    const margin = { top: 24, right: 20, bottom: 24, left: 20 };
    const nodeWidth = 18;
    const levelMap = new Map();
    nodes.forEach((node) => {
        const depth = Math.max(1, Number(node.depth) || 1);
        if (!levelMap.has(depth)) levelMap.set(depth, []);
        levelMap.get(depth).push(node);
    });

    const maxDepth = Math.max(...Array.from(levelMap.keys()));
    const usableWidth = Math.max(1, width - margin.left - margin.right - nodeWidth);
    const levelGap = maxDepth > 1 ? usableWidth / (maxDepth - 1) : 0;
    const positionedNodes = [];
    const nodeLookup = new Map();

    levelMap.forEach((levelNodes, depth) => {
        const gap = 16;
        const availableHeight = Math.max(1, height - margin.top - margin.bottom - (Math.max(levelNodes.length - 1, 0) * gap));
        const totalValue = levelNodes.reduce((sum, node) => sum + Math.max(Math.abs(Number(node.value) || 0), 1), 0);
        const scale = totalValue > 0 ? availableHeight / totalValue : 0;
        let cursorY = margin.top;

        levelNodes.forEach((node) => {
            const rawHeight = totalValue > 0
                ? Math.max(14, Math.abs(Number(node.value) || 0) * scale)
                : Math.max(18, availableHeight / Math.max(levelNodes.length, 1));
            const positioned = {
                ...node,
                x: margin.left + ((depth - 1) * levelGap),
                y: cursorY,
                width: nodeWidth,
                height: rawHeight,
            };
            positionedNodes.push(positioned);
            nodeLookup.set(node.rowPath, positioned);
            cursorY += rawHeight + gap;
        });
    });

    const outgoingTotals = new Map();
    const incomingTotals = new Map();
    links.forEach((link) => {
        outgoingTotals.set(link.source, (outgoingTotals.get(link.source) || 0) + Math.max(link.value || 0, 0));
        incomingTotals.set(link.target, (incomingTotals.get(link.target) || 0) + Math.max(link.value || 0, 0));
    });
    const sourceOffsets = new Map();
    const targetOffsets = new Map();

    const positionedLinks = links.map((link) => {
        const sourceNode = nodeLookup.get(link.source);
        const targetNode = nodeLookup.get(link.target);
        if (!sourceNode || !targetNode) return null;

        const sourceTotal = Math.max(outgoingTotals.get(link.source) || sourceNode.value || 1, 1);
        const targetTotal = Math.max(incomingTotals.get(link.target) || targetNode.value || 1, 1);
        const sourceScale = sourceNode.height / sourceTotal;
        const targetScale = targetNode.height / targetTotal;
        const thickness = Math.max(2, Math.min(sourceScale, targetScale) * Math.max(link.value || 0, 0));
        const sourceOffset = sourceOffsets.get(link.source) || 0;
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
    const numericColIndexes = colIndexes.filter((colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return rowIndexes.some((rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]) !== null);
    });

    if (numericColIndexes.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    const labelColIndex = colIndexes.find((colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return !numericColIndexes.includes(colIndex);
    });

    const slicedRowIndexes = rowIndexes.slice(0, normalizePositiveLimit(maxRows, MAX_SELECTION_CATEGORIES));
    const slicedSeriesIndexes = numericColIndexes.slice(0, normalizePositiveLimit(maxColumns, MAX_SELECTION_SERIES));
    const resolveColumnLabel = buildColumnLabelResolver(
        slicedSeriesIndexes.map((colIndex) => visibleCols[colIndex]).filter(Boolean)
    );
    const categories = slicedRowIndexes.map((rowIndex, idx) => {
        if (labelColIndex !== undefined) {
            const cellValue = grid[rowIndex] && grid[rowIndex][labelColIndex];
            if (cellValue !== null && cellValue !== undefined && String(cellValue).trim() !== '') {
                return formatDisplayLabel(String(cellValue));
            }
        }
        return getRowLabel(visibleRows[rowIndex], idx);
    });

    const series = slicedSeriesIndexes.map((colIndex) => ({
        name: resolveColumnLabel(visibleCols[colIndex]),
        values: slicedRowIndexes.map((rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex])),
    })).filter((entry) => entry.values.some((value) => value !== null));

    if (series.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

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
        categoryTargets: slicedRowIndexes.map((rowIndex, idx) => buildRowTarget(visibleRows[rowIndex], idx, rowFields)),
        series,
        note: notes.length > 0 ? notes.join(', ') : null,
    };
};

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
    const numericColIndexes = colIndexes.filter((colIndex) => {
        const column = visibleCols[colIndex];
        if (!column || INTERNAL_COL_IDS.has(column.id)) return false;
        return rowIndexes.some((rowIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]) !== null);
    });

    if (numericColIndexes.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    const slicedColIndexes = numericColIndexes.slice(0, normalizePositiveLimit(maxColumns, MAX_SELECTION_CATEGORIES));
    const slicedRowIndexes = rowIndexes.slice(0, normalizePositiveLimit(maxRows, MAX_SELECTION_SERIES));
    const resolveColumnLabel = buildColumnLabelResolver(
        slicedColIndexes.map((colIndex) => visibleCols[colIndex]).filter(Boolean)
    );
    const categories = slicedColIndexes.map((colIndex) => resolveColumnLabel(visibleCols[colIndex]));
    const series = slicedRowIndexes.map((rowIndex, rowSeriesIndex) => ({
        name: getRowLabel(visibleRows[rowIndex], rowSeriesIndex),
        values: slicedColIndexes.map((colIndex) => parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex])),
    })).filter((entry) => entry.values.some((value) => value !== null));

    if (series.length === 0) {
        return {
            categories: [],
            series: [],
            note: 'No numeric values found in the selected range.',
        };
    }

    const notes = [];
    if (numericColIndexes.length > slicedColIndexes.length) {
        notes.push(`showing first ${slicedColIndexes.length} numeric columns`);
    }
    if (rowIndexes.length > slicedRowIndexes.length) {
        notes.push(`showing first ${slicedRowIndexes.length} selected rows`);
    }

    return {
        categories,
        categoryTargets: slicedColIndexes.map((colIndex) => buildColumnTarget(visibleCols[colIndex], colFields)),
        series,
        note: notes.length > 0 ? notes.join(', ') : null,
    };
};

export const buildSelectionChartModel = (selectionMap, visibleRows, visibleCols, options = {}) => {
    const orientation = options.orientation || 'auto';
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_SELECTION_CATEGORIES);
    const maxColumns = normalizePositiveLimit(options.maxColumns, MAX_SELECTION_SERIES);
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    const colFields = Array.isArray(options.colFields) ? options.colFields : [];
    const sortMode = options.sortMode || 'natural';
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
    const selectedRows = rowIndexes.map((rowIndex) => visibleRows[rowIndex]).filter(Boolean);
    const availableLevels = getAvailableLevels(selectedRows);
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    const displayRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    const displayPaths = new Set(displayRows.map((row) => getRowPath(row)));
    const filteredRowIndexes = rowIndexes.filter((rowIndex) => displayPaths.has(getRowPath(visibleRows[rowIndex])));
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

    if (filteredRowIndexes.length === 1) {
        const rowIndex = filteredRowIndexes[0];
        const row = visibleRows[rowIndex];
        const resolveColumnLabel = buildColumnLabelResolver(
            colIndexes.map((colIndex) => visibleCols[colIndex]).filter(Boolean)
        );
        const entries = colIndexes
            .map((colIndex) => ({
                column: visibleCols[colIndex],
                label: resolveColumnLabel(visibleCols[colIndex]),
                value: parseNumeric(grid[rowIndex] && grid[rowIndex][colIndex]),
            }))
            .filter((entry) => entry.value !== null)
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

        const singleRowModel = {
            title: null,
            subtitle: null,
            categories: entries.map((entry) => entry.label),
            categoryTargets: entries.map((entry) => buildColumnTarget(entry.column, colFields)),
            categoryBands: [],
            stackedGroups: [],
            icicleNodes: [],
            sunburstNodes: [],
            sankeyNodes: [],
            sankeyLinks: [],
            series: [{
                name: getRowLabel(row, 0),
                values: entries.map((entry) => entry.value),
            }],
            note: colIndexes.length > entries.length ? `showing first ${entries.length} numeric columns` : null,
            availableLevels,
            maxHierarchyLevel,
            activeHierarchyLevel: hierarchyLevel,
        };
        return sortChartModel(singleRowModel, sortMode);
    }

    const builder = effectiveOrientation === 'columns'
        ? buildSeriesFromColumnCategories
        : buildSeriesFromRowCategories;
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

    const hierarchySourceRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    const firstNumericColumn = visibleCols.find((column) => (
        column
        && !INTERNAL_COL_IDS.has(column.id)
        && selectedRows.some((row) => parseNumeric(getCellValue(row, column.id)) !== null)
    ));
    const hierarchyModel = firstNumericColumn
        ? buildIcicleNodes(hierarchySourceRows, firstNumericColumn)
        : { nodes: [], maxDepth: 0 };
    const sortedHierarchyNodes = sortHierarchyNodes(hierarchyModel.nodes, sortMode);
    const sankeyModel = buildHierarchySankey(sortedHierarchyNodes);

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

export const buildPivotChartModel = (displayRows, visibleCols, options = {}) => {
    const orientation = options.orientation || 'rows';
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_PANEL_CATEGORIES);
    const maxColumns = normalizePositiveLimit(options.maxColumns, MAX_PANEL_SERIES);
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    const colFields = Array.isArray(options.colFields) ? options.colFields : [];
    const sortMode = options.sortMode || 'natural';
    const allVisibleRows = (displayRows || []).filter((row) => !isTotalRow(row));
    const orderedRows = allVisibleRows;
    const availableLevels = getAvailableLevels(allVisibleRows);
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    const filteredVisibleRows = getHierarchyDisplayRows(allVisibleRows, hierarchyLevel);
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

    const candidateColumns = (visibleCols || []).filter((column) => !INTERNAL_COL_IDS.has(column.id));
    const numericColumns = candidateColumns.filter((column) => (
        chartRows.some((row) => parseNumeric(getCellValue(row, column.id)) !== null)
    ));
    const seriesColumns = numericColumns.slice(0, maxColumns);
    const resolveColumnLabel = buildColumnLabelResolver(seriesColumns);
    const icicleModel = seriesColumns.length > 0
        ? buildIcicleNodes(orderedRows, seriesColumns[0])
        : { nodes: [], maxDepth: 0 };
    const sortedHierarchyNodes = sortHierarchyNodes(icicleModel.nodes, sortMode);
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

    const notes = [];
    if (filteredVisibleRows.length > chartRows.length) {
        notes.push(`showing first ${chartRows.length} visible rows`);
    }
    if (numericColumns.length > seriesColumns.length) {
        notes.push(`showing first ${seriesColumns.length} numeric columns`);
    }

    if (orientation === 'columns') {
        const categories = seriesColumns.map((column) => resolveColumnLabel(column));
        const rowSeriesRows = chartRows.slice(0, maxRows);
        if (chartRows.length > rowSeriesRows.length) {
            notes.push(`showing first ${rowSeriesRows.length} visible rows as series`);
        }
        const rowSeries = rowSeriesRows.map((row, index) => ({
            name: getRowLabel(row, index),
            values: seriesColumns.map((column) => parseNumeric(getCellValue(row, column.id))),
        })).filter((entry) => entry.values.some((value) => value !== null));

        const rawModel = {
            title: null,
            subtitle: null,
            categories,
            categoryTargets: seriesColumns.map((column) => ({
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

    const categories = chartRows.map((row, index) => getRowLabel(row, index));
    const series = seriesColumns.map((column) => ({
        name: resolveColumnLabel(column),
        values: chartRows.map((row) => parseNumeric(getCellValue(row, column.id))),
    })).filter((entry) => entry.values.some((value) => value !== null));
    const firstSeries = series[0] || null;
    const numericValuesByPath = {};
    if (firstSeries) {
        chartRows.forEach((row, index) => {
            numericValuesByPath[getRowPath(row)] = firstSeries.values[index];
        });
        orderedRows.forEach((row) => {
            const rowPath = getRowPath(row);
            if (numericValuesByPath[rowPath] === undefined) {
                numericValuesByPath[rowPath] = getCellValue(row, seriesColumns[0].id);
            }
        });
    }
    const categoryBands = buildCategoryBandsForRows(chartRows.map((_, index) => index), chartRows);
    const stackedGroups = (() => {
        if (!(series.length === 1 && firstSeries)) return [];
        if (hierarchyLevel === 'all') {
            const frontierGroups = buildFrontierStackedGroups(chartRows, categoryBands, numericValuesByPath, maxRows);
            return frontierGroups.some((group) => Array.isArray(group.segments) && group.segments.length > 1)
                ? frontierGroups
                : [];
        }
        if (typeof hierarchyLevel === 'number' && hierarchyLevel < maxHierarchyLevel) {
            const nestedGroups = buildStackedGroupsForRows(chartRows, orderedRows, numericValuesByPath, maxRows);
            return nestedGroups.some((group) => Array.isArray(group.segments) && group.segments.length > 1)
                ? nestedGroups
                : [];
        }
        return [];
    })();

    const rawModel = {
        title: null,
        subtitle: null,
        categories,
        categoryTargets: chartRows.map((row, index) => buildRowTarget(row, index, rowFields)),
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

export const buildComboLayersForRows = (layers, chartRows, visibleCols) => {
    const chartableColumns = getChartableColumns(visibleCols);
    return normalizeComboLayers(layers, chartableColumns)
        .map((layer, index) => {
            const column = chartableColumns.find((candidate) => candidate.id === layer.columnId) || null;
            if (!column) return null;
            const values = chartRows.map((row) => parseNumeric(getCellValue(row, column.id)));
            if (!values.some((value) => value !== null)) return null;
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

export const buildComboPivotChartModel = (displayRows, visibleCols, options = {}) => {
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_PANEL_CATEGORIES);
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    const sortMode = options.sortMode || 'natural';
    const allVisibleRows = (displayRows || []).filter((row) => !isTotalRow(row));
    const availableLevels = getAvailableLevels(allVisibleRows);
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    const filteredVisibleRows = getHierarchyDisplayRows(allVisibleRows, hierarchyLevel);
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

    const modelLayers = buildComboLayersForRows(options.layers, chartRows, visibleCols);
    if (modelLayers.length === 0) {
        return buildComboEmptyModel(
            'No visible numeric columns are available for the combo chart layers.',
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    const notes = [];
    if (filteredVisibleRows.length > chartRows.length) {
        notes.push(`showing first ${chartRows.length} visible rows`);
    }

    const rawModel = {
        title: null,
        subtitle: null,
        categories: chartRows.map((row, index) => getRowLabel(row, index)),
        categoryTargets: chartRows.map((row, index) => buildRowTarget(row, index, rowFields)),
        categoryBands: buildCategoryBandsForRows(chartRows.map((_, index) => index), chartRows),
        layers: modelLayers,
        series: modelLayers.map((layer) => ({ name: layer.name, values: layer.values })),
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

export const buildComboSelectionChartModel = (selectionMap, visibleRows, visibleCols, options = {}) => {
    const hierarchyLevel = options.hierarchyLevel === undefined ? 'all' : options.hierarchyLevel;
    const maxRows = normalizePositiveLimit(options.maxRows, MAX_SELECTION_CATEGORIES);
    const rowFields = Array.isArray(options.rowFields) ? options.rowFields : [];
    const sortMode = options.sortMode || 'natural';
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
    const selectedRows = rowIndexes.map((rowIndex) => visibleRows[rowIndex]).filter(Boolean);
    const availableLevels = getAvailableLevels(selectedRows);
    const maxHierarchyLevel = Math.max(
        Number(options.maxHierarchyLevel) || 0,
        availableLevels.length > 0 ? Math.max(...availableLevels) : 0,
        1
    );
    const displayRows = getHierarchyDisplayRows(selectedRows, hierarchyLevel);
    const chartRows = displayRows.slice(0, maxRows);
    if (chartRows.length === 0) {
        return buildComboEmptyModel(
            `No selected rows found at hierarchy level ${hierarchyLevel}.`,
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    const selectedColumnIds = new Set(
        colIndexes
            .map((index) => visibleCols[index])
            .filter((column) => column && column.id && !INTERNAL_COL_IDS.has(column.id))
            .map((column) => column.id)
    );
    const selectionColumns = getChartableColumns(visibleCols).filter((column) => selectedColumnIds.has(column.id));
    const modelLayers = buildComboLayersForRows(options.layers, chartRows, selectionColumns);
    if (modelLayers.length === 0) {
        return buildComboEmptyModel(
            'The selected range does not contain numeric columns that can be layered.',
            hierarchyLevel,
            maxHierarchyLevel,
            availableLevels
        );
    }

    const rawModel = {
        title: null,
        subtitle: null,
        categories: chartRows.map((row, index) => getRowLabel(row, index)),
        categoryTargets: chartRows.map((row, index) => buildRowTarget(row, index, rowFields)),
        categoryBands: buildCategoryBandsForRows(chartRows.map((_, index) => index), chartRows),
        layers: modelLayers,
        series: modelLayers.map((layer) => ({ name: layer.name, values: layer.values })),
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

export const buildLinePath = (points, closeToY = null) => {
    const validPoints = points.filter((point) => point && Number.isFinite(point.x) && Number.isFinite(point.y));
    if (validPoints.length === 0) return '';
    const head = validPoints[0];
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

export const buildAreaBandPath = (points) => {
    const validPoints = points.filter((point) => (
        point
        && Number.isFinite(point.x)
        && Number.isFinite(point.y)
        && Number.isFinite(point.baseY)
    ));
    if (validPoints.length === 0) return '';
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

export const niceTickValues = (minVal, maxVal, count) => {
    const rawSpan = maxVal - minVal;
    if (rawSpan === 0) return [minVal];
    const rawStep = rawSpan / Math.max(count - 1, 1);
    const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
    const residual = rawStep / magnitude;
    const niceStep = residual <= 1.5 ? magnitude : residual <= 3 ? 2 * magnitude : residual <= 7 ? 5 * magnitude : 10 * magnitude;
    const niceMin = Math.floor(minVal / niceStep) * niceStep;
    const niceMax = Math.ceil(maxVal / niceStep) * niceStep;
    const ticks = [];
    for (let v = niceMin; v <= niceMax + niceStep * 0.01; v += niceStep) {
        ticks.push(Math.round(v * 1e10) / 1e10);
    }
    return ticks;
};

export const getAxisMetrics = (values, chartHeight, margin) => {
    const numericValues = (values || []).filter((value) => typeof value === 'number' && Number.isFinite(value));
    const minValue = numericValues.length > 0 ? Math.min(...numericValues, 0) : 0;
    const maxValue = numericValues.length > 0 ? Math.max(...numericValues, 0) : 0;
    const span = maxValue - minValue || 1;
    const scaleY = (value) => {
        const normalized = (value - minValue) / span;
        return chartHeight - margin.bottom - (normalized * (chartHeight - margin.top - margin.bottom));
    };
    const baselineY = scaleY(0);
    const tickValues = niceTickValues(minValue, maxValue, 5);
    return {
        minValue,
        maxValue,
        scaleY,
        baselineY,
        tickValues,
    };
};

export const layoutIcicleNodes = (nodes, x, width, level, bandHeight, acc = []) => {
    if (!Array.isArray(nodes) || nodes.length === 0 || width <= 0 || bandHeight <= 0) {
        return acc;
    }

    const weights = nodes.map((node) => {
        const numericValue = typeof node.value === 'number' && Number.isFinite(node.value)
            ? Math.abs(node.value)
            : 0;
        return numericValue;
    });
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    let cursor = x;

    nodes.forEach((node, index) => {
        const fallbackWidth = width / Math.max(nodes.length, 1);
        const nodeWidth = totalWeight > 0
            ? (weights[index] / totalWeight) * width
            : fallbackWidth;
        const safeWidth = Math.max(1, nodeWidth);
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

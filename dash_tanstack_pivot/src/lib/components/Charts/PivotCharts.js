import React, { useEffect, useMemo, useRef, useState } from 'react';
import Icons from '../Icons';
import { formatDisplayLabel } from '../../utils/helpers';

const INTERNAL_COL_IDS = new Set(['hierarchy', '__row_number__']);
const MAX_PANEL_CATEGORIES = 18;
const MAX_PANEL_SERIES = 4;
const MAX_SELECTION_CATEGORIES = 24;
const MAX_SELECTION_SERIES = 6;
const CHART_HEIGHT = 320;
const CHART_WIDTH = 760;

const DEFAULT_COLORS = ['#2563EB', '#F97316', '#0F766E', '#7C3AED', '#DC2626', '#0891B2'];
const normalizePositiveLimit = (value, fallback) => {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue) || numericValue < 1) return fallback;
    return Math.max(1, Math.floor(numericValue));
};

const parseNumeric = (value) => {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(/,/g, '').replace(/%$/, '');
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
};

const stringifyValue = (value) => {
    if (value === null || value === undefined) return '';
    return String(value);
};

const formatChartNumber = (value) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '';
    return value.toFixed(Math.abs(value) >= 100 ? 0 : 2).replace(/\.00$/, '');
};

const estimateTextWidth = (text, fontSize = 11) => Math.ceil(String(text || '').length * fontSize * 0.62);

const truncateChartLabel = (value, maxLength) => {
    const label = String(value || '');
    return label.length > maxLength ? `${label.slice(0, maxLength)}...` : label;
};

const getColumnLabel = (column) => {
    if (!column) return 'Value';
    const header = column.columnDef ? column.columnDef.header : null;
    if (typeof header === 'string' && header.trim()) return formatDisplayLabel(header);
    return formatDisplayLabel(column.id || 'Value');
};

const getColumnHeaderSegments = (column) => {
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

const buildColumnLabelResolver = (columns) => {
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

const getColumnValueSegments = (column) => {
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

const buildColumnFieldValues = (column, fields = []) => {
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

const getRowLabel = (row, fallbackIndex = 0) => {
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

const getRowDepth = (row) => {
    if (!row) return 0;
    const explicitDepth = row.original && typeof row.original.depth === 'number'
        ? row.original.depth
        : (typeof row.depth === 'number' ? row.depth : null);
    if (explicitDepth !== null) return explicitDepth;
    return typeof row.depth === 'number' ? row.depth : 0;
};

const getRowLevel = (row) => getRowDepth(row) + 1;
const getRowPath = (row) => {
    if (!row) return '';
    if (row.original && row.original._path) return String(row.original._path);
    if (row._path) return String(row._path);
    return String(row.id || '');
};

const buildRowFieldValues = (rowPath, fields = []) => {
    const pathParts = rowPath ? String(rowPath).split('|||') : [];
    return fields.reduce((acc, field, index) => {
        const value = pathParts[index];
        if (field && value !== undefined && value !== null && String(value).trim() !== '') {
            acc[field] = String(value);
        }
        return acc;
    }, {});
};

const buildRowTarget = (row, fallbackIndex = 0, fields = []) => {
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

const buildColumnTarget = (column, fields = []) => ({
    kind: 'column',
    columnId: column && column.id ? column.id : null,
    label: buildColumnLabelResolver([column])(column),
    pathValues: getColumnValueSegments(column),
    fieldValues: buildColumnFieldValues(column, fields),
});
const isDescendantPath = (ancestorPath, candidatePath) => (
    Boolean(ancestorPath) &&
    Boolean(candidatePath) &&
    candidatePath.indexOf(`${ancestorPath}|||`) === 0
);

const isTotalRow = (row) => Boolean(
    row &&
    (
        (row.original && (row.original._isTotal || row.original.__isGrandTotal__ || row.original._path === '__grand_total__'))
        || row._isTotal
        || row.__isGrandTotal__
        || row._path === '__grand_total__'
    )
);

const getCellValue = (row, columnId) => {
    if (!row || !columnId) return null;
    if (typeof row.getValue === 'function') return row.getValue(columnId);
    return row[columnId];
};

const getAvailableLevels = (rows) => Array.from(new Set(
    (rows || [])
        .filter((row) => row && !isTotalRow(row))
        .map((row) => getRowLevel(row))
)).sort((a, b) => a - b);

const getHierarchyDisplayRows = (rows, hierarchyLevel) => {
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

const getColorForIndex = (index, theme) => {
    const palette = [theme.primary, ...DEFAULT_COLORS.filter((color) => color !== theme.primary)];
    return palette[index % palette.length];
};

const buildSelectionBounds = (selectionMap, visibleRows, visibleCols) => {
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

const buildCategoryBandsForRows = (rowIndexes, visibleRows) => rowIndexes.map((rowIndex, index) => {
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

const buildStackedGroupsForRows = (displayRows, allVisibleRows, numericValues, maxGroups) => {
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

const buildFrontierStackedGroups = (displayRows, categoryBands, numericValues, maxGroups) => {
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

const buildIcicleNodes = (rows, valueColumn) => {
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

const cloneHierarchyNodes = (nodes) => (
    Array.isArray(nodes)
        ? nodes.map((node) => ({
            ...node,
            children: cloneHierarchyNodes(node.children),
        }))
        : []
);

const sortHierarchyNodes = (nodes, sortMode) => {
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

const sortChartModel = (model, sortMode) => {
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

const buildHierarchySankey = (nodes) => {
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

const polarToCartesian = (cx, cy, radius, angle) => ({
    x: cx + (radius * Math.cos(angle)),
    y: cy + (radius * Math.sin(angle)),
});

const describeDonutArc = (cx, cy, innerRadius, outerRadius, startAngle, endAngle) => {
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

const layoutSunburstNodes = (nodes, startAngle, endAngle, depth, ringWidth, acc = []) => {
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

const layoutSankey = (nodes, links, width, height) => {
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

const buildSeriesFromRowCategories = ({
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

const buildSeriesFromColumnCategories = ({
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

const buildLinePath = (points, closeToY = null) => {
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

const buildAreaBandPath = (points) => {
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

const layoutIcicleNodes = (nodes, x, width, level, bandHeight, acc = []) => {
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

const ChartTypeButtons = ({ chartType, onChange, theme, includeHierarchyCharts = true }) => {
    const buttonStyle = (type) => ({
        border: `1px solid ${type === chartType ? theme.primary : theme.border}`,
        background: type === chartType ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: type === chartType ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('bar')} style={buttonStyle('bar')}>Bar</button>
            <button onClick={() => onChange('line')} style={buttonStyle('line')}>Line</button>
            <button onClick={() => onChange('area')} style={buttonStyle('area')}>Area</button>
            {includeHierarchyCharts ? (
                <>
                    <button onClick={() => onChange('icicle')} style={buttonStyle('icicle')}>Icicle</button>
                    <button onClick={() => onChange('sunburst')} style={buttonStyle('sunburst')}>Sunburst</button>
                    <button onClick={() => onChange('sankey')} style={buttonStyle('sankey')}>Sankey</button>
                </>
            ) : null}
        </div>
    );
};

const canStackAreaSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
export const canStackBarSeries = (model) => Array.isArray(model && model.series) && model.series.length > 1;
export const canStackBarLayout = (model) => (
    (Array.isArray(model && model.stackedGroups) && model.stackedGroups.length > 0)
    || canStackBarSeries(model)
);

const ChartLayoutButtons = ({ chartType, barLayout, onChange, canStack, theme }) => {
    const buttonStyle = (value, disabled = false) => ({
        border: `1px solid ${value === barLayout ? theme.primary : theme.border}`,
        background: value === barLayout ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === barLayout ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.45 : 1,
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('grouped')} style={buttonStyle('grouped')}>
                {chartType === 'area' ? 'Normal' : 'Grouped'}
            </button>
            <button
                onClick={() => {
                    if (!canStack) return;
                    onChange('stacked');
                }}
                style={buttonStyle('stacked', !canStack)}
                title={
                    canStack
                        ? (chartType === 'area' ? 'Stack area series' : 'Stack series or visible child rows')
                        : (chartType === 'area'
                            ? 'Stacked area needs at least two visible series'
                            : 'Stacked bars need multiple visible series or visible child groups')
                }
            >
                Stacked
            </button>
        </div>
    );
};

const ChartOrientationButtons = ({ orientation, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === orientation ? theme.primary : theme.border}`,
        background: value === orientation ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === orientation ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('rows')} style={buttonStyle('rows')}>By Rows</button>
            <button onClick={() => onChange('columns')} style={buttonStyle('columns')}>By Columns</button>
        </div>
    );
};

const ChartAxisButtons = ({ axisMode, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === axisMode ? theme.primary : theme.border}`,
        background: value === axisMode ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === axisMode ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('vertical')} style={buttonStyle('vertical')}>Vertical</button>
            <button onClick={() => onChange('horizontal')} style={buttonStyle('horizontal')}>Horizontal</button>
        </div>
    );
};

const ChartHierarchyButtons = ({ level, onChange, maxLevel, theme }) => {
    if (!maxLevel || maxLevel <= 1) return null;

    const buttonStyle = (value) => ({
        border: `1px solid ${value === level ? theme.primary : theme.border}`,
        background: value === level ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === level ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
        minWidth: '34px',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('all')} style={buttonStyle('all')}>All</button>
            {Array.from({ length: maxLevel }, (_, index) => index + 1).map((numericLevel) => (
                <button key={numericLevel} onClick={() => onChange(numericLevel)} style={buttonStyle(numericLevel)}>
                    {numericLevel}
                </button>
            ))}
        </div>
    );
};

const ChartHeader = ({ title, subtitle, note, theme }) => (
    !title && !subtitle && !note ? null : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            {title ? <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>{title}</div> : null}
            {subtitle ? <div style={{ fontSize: '11px', color: theme.textSec }}>{subtitle}</div> : null}
            {note ? <div style={{ fontSize: '11px', color: theme.textSec }}>{note}</div> : null}
        </div>
    )
);

const ChartConfigSection = ({ title, description, theme, children }) => (
    <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
        minWidth: '220px',
        padding: '10px',
        border: `1px solid ${theme.border}`,
        borderRadius: theme.radiusSm || '8px',
        background: theme.surfaceBg || theme.background || '#fff',
        boxShadow: theme.shadowInset || 'none',
    }}>
        <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            paddingBottom: '6px',
            borderBottom: `1px solid ${theme.border}`,
        }}>
            <div style={{
                fontSize: '10px',
                fontWeight: 800,
                letterSpacing: '0.08em',
                textTransform: 'uppercase',
                color: theme.textSec,
            }}>
                {title}
            </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {children}
        </div>
    </div>
);

const ChartConfigField = ({ label, description, theme, children, controlMinWidth = '180px' }) => (
    <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '8px',
        flexWrap: 'wrap',
    }}>
        <div style={{ display: 'flex', alignItems: 'center', flex: '0 0 auto', minWidth: '88px' }}>
            <div style={{ fontSize: '11px', fontWeight: 700, color: theme.text, whiteSpace: 'nowrap' }}>
                {label}
            </div>
        </div>
        <div style={{
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'center',
            flex: '1 1 150px',
            minWidth: controlMinWidth,
        }}>
            {children}
        </div>
    </div>
);

const ChartLimitInputs = ({
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    theme,
}) => {
    const inputStyle = {
        width: '58px',
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };
    const labelStyle = {
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        color: theme.textSec,
        fontSize: '11px',
        fontWeight: 700,
    };
    const handleLimitChange = (callback, fallback) => (event) => {
        if (!callback) return;
        callback(normalizePositiveLimit(event.target.value, fallback));
    };

    return (
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
            <label style={labelStyle}>
                Rows
                <input
                    type="number"
                    min="1"
                    step="1"
                    value={rowLimit}
                    onChange={handleLimitChange(onRowLimitChange, MAX_PANEL_CATEGORIES)}
                    style={inputStyle}
                />
            </label>
            <label style={labelStyle}>
                Cols
                <input
                    type="number"
                    min="1"
                    step="1"
                    value={columnLimit}
                    onChange={handleLimitChange(onColumnLimitChange, MAX_PANEL_SERIES)}
                    style={inputStyle}
                />
            </label>
        </div>
    );
};

const ChartSortButtons = ({ sortMode, onChange, theme }) => {
    const options = [
        { value: 'natural', label: 'Natural' },
        { value: 'value_desc', label: 'Value Desc' },
        { value: 'value_asc', label: 'Value Asc' },
        { value: 'label_asc', label: 'Label A-Z' },
        { value: 'label_desc', label: 'Label Z-A' },
    ];

    return (
        <select
            value={sortMode}
            onChange={(event) => onChange(event.target.value)}
            style={{
                border: `1px solid ${theme.border}`,
                background: theme.surfaceBg || theme.background || '#fff',
                color: theme.text,
                borderRadius: theme.radiusSm || '8px',
                padding: '6px 8px',
                fontSize: '11px',
                fontWeight: 700,
                minWidth: '132px',
            }}
            title="Chart ordering"
        >
            {options.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
            ))}
        </select>
    );
};

const ChartInteractionButtons = ({ interactionMode, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === interactionMode ? theme.primary : theme.border}`,
        background: value === interactionMode ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === interactionMode ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('focus')} style={buttonStyle('focus')} title="Jump the grid to the matching row or column">Jump to Grid</button>
            <button onClick={() => onChange('filter')} style={buttonStyle('filter')} title="Apply the clicked chart item as a pivot filter">Filter Grid</button>
            <button onClick={() => onChange('event')} style={buttonStyle('event')} title="Send the chart click to Dash only and leave the grid unchanged">No Grid Change</button>
        </div>
    );
};

const ChartScopeButtons = ({ scope, onChange, theme }) => {
    const buttonStyle = (value) => ({
        border: `1px solid ${value === scope ? theme.primary : theme.border}`,
        background: value === scope ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === scope ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <button onClick={() => onChange('viewport')} style={buttonStyle('viewport')}>Viewport</button>
            <button onClick={() => onChange('root')} style={buttonStyle('root')}>From Start</button>
        </div>
    );
};

const downloadBlob = (blob, filename) => {
    if (typeof window === 'undefined' || !blob) return;
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    window.URL.revokeObjectURL(url);
};

const exportSvgNode = async (svgNode, fileStem) => {
    if (!svgNode || typeof window === 'undefined') return;
    const serializer = new XMLSerializer();
    const svgText = serializer.serializeToString(svgNode);
    const svgBlob = new Blob([svgText], { type: 'image/svg+xml;charset=utf-8' });
    downloadBlob(svgBlob, `${fileStem}.svg`);
};

const EmptyChartState = ({ message, theme, chartHeight = CHART_HEIGHT }) => (
    <div style={{
        minHeight: `${chartHeight}px`,
        border: `1px dashed ${theme.border}`,
        borderRadius: theme.radius || '14px',
        background: theme.headerSubtleBg || theme.hover,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '16px',
        color: theme.textSec,
        fontSize: '12px',
        textAlign: 'center',
    }}>
        {message}
    </div>
);

const ChartStats = ({ model, theme }) => {
    const metrics = useMemo(() => {
        const values = [];
        (model && model.series ? model.series : []).forEach((series) => {
            (series.values || []).forEach((value) => {
                if (typeof value === 'number' && Number.isFinite(value)) values.push(value);
            });
        });
        if (values.length === 0 && model && Array.isArray(model.icicleNodes)) {
            model.icicleNodes.forEach((node) => {
                const value = Number(node && node.value);
                if (Number.isFinite(value)) values.push(value);
            });
        }
        if (values.length === 0) return null;

        return {
            categories: Array.isArray(model.categories) ? model.categories.length : 0,
            seriesCount: Array.isArray(model.series) ? model.series.length : 0,
            hierarchyNodes: Array.isArray(model.icicleNodes) ? model.icicleNodes.length : 0,
            min: Math.min(...values),
            max: Math.max(...values),
        };
    }, [model]);

    if (!metrics) return null;

    const chipStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.hover,
        color: theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        padding: '5px 8px',
        fontSize: '11px',
        fontWeight: 700,
    };

    return (
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
            <span style={chipStyle}>{metrics.categories} categories</span>
            <span style={chipStyle}>{metrics.seriesCount} series</span>
            {metrics.hierarchyNodes > 0 ? <span style={chipStyle}>{metrics.hierarchyNodes} nodes</span> : null}
            <span style={chipStyle}>Min {formatChartNumber(metrics.min)}</span>
            <span style={chipStyle}>Max {formatChartNumber(metrics.max)}</span>
        </div>
    );
};

const ChartLegend = ({ items, theme }) => {
    if (!Array.isArray(items) || items.length === 0) return null;

    return (
        <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
            {items.map((item) => (
                <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '11px', color: theme.textSec }}>
                    <span style={{ width: '10px', height: '10px', borderRadius: '999px', background: item.color, display: 'inline-block' }} />
                    <span>{item.label}</span>
                </div>
            ))}
        </div>
    );
};

const SvgChart = ({
    model,
    chartType,
    barLayout,
    axisMode,
    theme,
    chartHeight = CHART_HEIGHT,
    showLegend = true,
    onCategoryActivate = null,
    svgRef = null,
}) => {
    const stackedChildBarMode = chartType === 'bar' && barLayout === 'stacked' && Array.isArray(model.stackedGroups) && model.stackedGroups.length > 0;
    const stackedSeriesBarMode = chartType === 'bar' && barLayout === 'stacked' && !stackedChildBarMode && canStackBarSeries(model);
    const stackedAreaMode = chartType === 'area' && barLayout === 'stacked' && canStackAreaSeries(model);
    const horizontalBarMode = chartType === 'bar' && axisMode === 'horizontal';
    const effectiveChartHeight = chartHeight;
    const hierarchyNodes = Array.isArray(model && model.icicleNodes) ? model.icicleNodes : [];
    const icicleDepth = Math.max(1, Number(model && model.icicleDepth) || 1);

    if (chartType === 'icicle') {
        if (hierarchyNodes.length === 0) {
            return <EmptyChartState message="Icicle charts need hierarchical rows and a numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const bandHeight = effectiveChartHeight / icicleDepth;
        const layoutNodes = layoutIcicleNodes(hierarchyNodes, 0, CHART_WIDTH, 0, bandHeight, []);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ width: '100%', minWidth: 0 }}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${CHART_WIDTH} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Icicle chart'}
                    >
                        {layoutNodes.map((node, index) => {
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme);
                            const textShouldShow = node.width >= 54;
                            return (
                                <g
                                    key={`icicle-node-${node.rowPath || index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                    }}
                                    style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                                >
                                    <rect
                                        x={node.x}
                                        y={node.y}
                                        width={Math.max(1, node.width - 1)}
                                        height={Math.max(20, node.height - 2)}
                                        rx="4"
                                        fill={fill}
                                        opacity="0.86"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1"
                                    />
                                    {textShouldShow ? (
                                        <text
                                            x={node.x + 8}
                                            y={node.y + Math.min(node.height - 10, 22)}
                                            textAnchor="start"
                                            fontSize="11"
                                            fontWeight="700"
                                            fill="#fff"
                                        >
                                            {truncateChartLabel(node.label, Math.max(6, Math.floor(node.width / 9)))}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </svg>
                </div>
            </div>
        );
    }

    if (chartType === 'sunburst') {
        if (hierarchyNodes.length === 0) {
            return <EmptyChartState message="Sunburst charts need hierarchical rows and a numeric measure." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const radius = Math.min(CHART_WIDTH, effectiveChartHeight) / 2 - 18;
        const centerX = CHART_WIDTH / 2;
        const centerY = effectiveChartHeight / 2;
        const ringWidth = radius / Math.max(icicleDepth, 1);
        const layoutNodes = layoutSunburstNodes(hierarchyNodes, -Math.PI / 2, (Math.PI * 3) / 2, 1, ringWidth, []);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ width: '100%', minWidth: 0 }}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${CHART_WIDTH} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Sunburst chart'}
                    >
                        {layoutNodes.map((node, index) => {
                            const fill = getColorForIndex(Math.max(0, node.depth - 1), theme);
                            const path = describeDonutArc(centerX, centerY, node.innerRadius, node.outerRadius, node.startAngle, node.endAngle);
                            const midAngle = node.startAngle + ((node.endAngle - node.startAngle) / 2);
                            const labelRadius = node.innerRadius + ((node.outerRadius - node.innerRadius) / 2);
                            const labelPos = polarToCartesian(centerX, centerY, labelRadius, midAngle);
                            const span = node.endAngle - node.startAngle;
                            const canShowText = span > 0.2 && (node.outerRadius - node.innerRadius) >= 18;

                            return (
                                <g
                                    key={`sunburst-node-${node.rowPath || index}`}
                                    onClick={() => {
                                        if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                    }}
                                    style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                                >
                                    <path
                                        d={path}
                                        fill={fill}
                                        opacity="0.88"
                                        stroke={theme.surfaceBg || theme.background || '#fff'}
                                        strokeWidth="1"
                                    />
                                    {canShowText ? (
                                        <text
                                            x={labelPos.x}
                                            y={labelPos.y}
                                            textAnchor="middle"
                                            fontSize="10"
                                            fontWeight="700"
                                            fill="#fff"
                                        >
                                            {truncateChartLabel(node.label, Math.max(5, Math.floor((span * 24))))}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </svg>
                </div>
            </div>
        );
    }

    if (chartType === 'sankey') {
        const sankeyNodes = Array.isArray(model && model.sankeyNodes) ? model.sankeyNodes : [];
        const sankeyLinks = Array.isArray(model && model.sankeyLinks) ? model.sankeyLinks : [];
        if (sankeyNodes.length === 0 || sankeyLinks.length === 0) {
            return <EmptyChartState message="Sankey charts need expanded hierarchical rows with child flows." theme={theme} chartHeight={effectiveChartHeight} />;
        }

        const sankeyLayout = layoutSankey(sankeyNodes, sankeyLinks, CHART_WIDTH, effectiveChartHeight);

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                <div style={{ width: '100%', minWidth: 0 }}>
                    <svg
                        ref={svgRef}
                        viewBox={`0 0 ${CHART_WIDTH} ${effectiveChartHeight}`}
                        preserveAspectRatio="xMidYMid meet"
                        style={{
                            width: '100%',
                            height: 'auto',
                            display: 'block',
                            maxWidth: '100%',
                        }}
                        role="img"
                        aria-label={model.title || 'Sankey chart'}
                    >
                        {sankeyLayout.links.map((link, index) => {
                            const sourceX = link.sourceNode.x + link.sourceNode.width;
                            const targetX = link.targetNode.x;
                            const midX = sourceX + ((targetX - sourceX) / 2);
                            const path = [
                                `M ${sourceX} ${link.sourceY}`,
                                `C ${midX} ${link.sourceY}, ${midX} ${link.targetY}, ${targetX} ${link.targetY}`,
                            ].join(' ');
                            return (
                                <path
                                    key={`sankey-link-${link.source}-${link.target}-${index}`}
                                    d={path}
                                    fill="none"
                                    stroke={theme.primary}
                                    strokeOpacity="0.22"
                                    strokeWidth={link.thickness}
                                />
                            );
                        })}
                        {sankeyLayout.nodes.map((node, index) => (
                            <g
                                key={`sankey-node-${node.rowPath || index}`}
                                onClick={() => {
                                    if (typeof onCategoryActivate === 'function') onCategoryActivate(node);
                                }}
                                style={typeof onCategoryActivate === 'function' ? { cursor: 'pointer' } : undefined}
                            >
                                <rect
                                    x={node.x}
                                    y={node.y}
                                    width={node.width}
                                    height={node.height}
                                    rx="4"
                                    fill={getColorForIndex(Math.max(0, node.depth - 1), theme)}
                                    opacity="0.92"
                                    stroke={theme.surfaceBg || theme.background || '#fff'}
                                    strokeWidth="1"
                                />
                                <text
                                    x={node.x + node.width + 6}
                                    y={node.y + Math.min(node.height / 2 + 4, node.height - 4)}
                                    textAnchor="start"
                                    fontSize="11"
                                    fill={theme.text}
                                    fontWeight="700"
                                >
                                    {truncateChartLabel(node.label, 24)}
                                </text>
                            </g>
                        ))}
                    </svg>
                </div>
            </div>
        );
    }

    const geometry = useMemo(() => {
        const flatValues = stackedChildBarMode
            ? (model.stackedGroups || []).reduce((acc, group) => {
                let positiveTotal = 0;
                let negativeTotal = 0;
                (group.segments || []).forEach((segment) => {
                    const value = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                    if (value === null) return;
                    if (value >= 0) positiveTotal += value;
                    else negativeTotal += value;
                });
                acc.push(positiveTotal, negativeTotal);
                return acc;
            }, [])
            : stackedSeriesBarMode
                ? (model.categories || []).reduce((acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach((series) => {
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : stackedAreaMode
                ? model.categories.reduce((acc, _, categoryIndex) => {
                    let positiveTotal = 0;
                    let negativeTotal = 0;
                    (model.series || []).forEach((series) => {
                        const value = series && Array.isArray(series.values) ? series.values[categoryIndex] : null;
                        if (typeof value !== 'number' || !Number.isFinite(value)) return;
                        if (value >= 0) positiveTotal += value;
                        else negativeTotal += value;
                    });
                    acc.push(positiveTotal, negativeTotal);
                    return acc;
                }, [])
            : (model.series || []).reduce((acc, series) => {
                (series.values || []).forEach((value) => {
                    if (typeof value === 'number' && Number.isFinite(value)) acc.push(value);
                });
                return acc;
            }, []);

        if (flatValues.length === 0) return null;

        const hasHierarchicalBands = !stackedChildBarMode && chartType === 'bar'
            && Array.isArray(model.categoryBands)
            && model.categoryBands.length === model.categories.length
            && model.categoryBands.some((band) => band && band.outerLabel);
        let minValue = Math.min(...flatValues, 0);
        let maxValue = Math.max(...flatValues, 0);
        if (minValue === maxValue) {
            const padding = Math.abs(maxValue || 1) * 0.15 || 1;
            minValue -= padding;
            maxValue += padding;
        }

        const tickLabels = Array.from({ length: 5 }, (_, idx) => {
            const ratio = idx / 4;
            return formatChartNumber(maxValue - ((maxValue - minValue) * ratio));
        });
        const maxTickLabelWidth = tickLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);

        const horizontalInnerLabels = stackedChildBarMode
            ? (model.stackedGroups || []).map((group) => truncateChartLabel(group && group.label, 22))
            : (
                hasHierarchicalBands
                    ? (model.categoryBands || []).map((band) => truncateChartLabel(band && band.innerLabel, 18))
                    : (model.categories || []).map((category) => truncateChartLabel(category, 22))
            );
        const horizontalOuterLabels = hasHierarchicalBands
            ? (model.categoryBands || []).map((band) => truncateChartLabel(band && band.outerLabel, 18))
            : [];
        const maxInnerLabelWidth = horizontalInnerLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label)), 0);
        const maxOuterLabelWidth = horizontalOuterLabels.reduce((maxWidth, label) => Math.max(maxWidth, estimateTextWidth(label, 11)), 0);

        const leftMargin = horizontalBarMode
            ? Math.max(
                hasHierarchicalBands ? 176 : 132,
                hasHierarchicalBands
                    ? maxOuterLabelWidth + maxInnerLabelWidth + 38
                    : maxInnerLabelWidth + 22
            )
            : Math.max(66, maxTickLabelWidth + 18);

        const margin = horizontalBarMode
            ? { top: 24, right: 18, bottom: 42, left: leftMargin }
            : { top: 24, right: 18, bottom: hasHierarchicalBands ? 118 : 84, left: leftMargin };
        const plotWidth = CHART_WIDTH - margin.left - margin.right;
        const plotHeight = effectiveChartHeight - margin.top - margin.bottom;
        const baseline = chartType === 'bar' ? 0 : Math.min(Math.max(0, minValue), maxValue);
        const safeSpan = maxValue - minValue || 1;
        const scaleY = horizontalBarMode ? null : ((value) => margin.top + ((maxValue - value) / safeSpan) * plotHeight);
        const scaleX = horizontalBarMode
            ? ((value) => margin.left + ((value - minValue) / safeSpan) * plotWidth)
            : null;
        const baselineY = scaleY ? scaleY(baseline) : null;
        const baselineX = scaleX ? scaleX(baseline) : null;
        const categoryCount = Math.max(model.categories.length, 1);
        const step = (horizontalBarMode ? plotHeight : plotWidth) / categoryCount;
        const groupWidth = Math.max(10, step * 0.72);

        return {
            margin,
            plotWidth,
            plotHeight,
            baselineY,
            baselineX,
            scaleY,
            scaleX,
            step,
            groupWidth,
            minValue,
            maxValue,
            hasHierarchicalBands,
            horizontalBarMode,
        };
    }, [axisMode, model, chartType, stackedAreaMode, stackedChildBarMode, stackedSeriesBarMode, horizontalBarMode, effectiveChartHeight]);

    if (!geometry) {
        return <EmptyChartState message="No numeric data available for this chart." theme={theme} chartHeight={effectiveChartHeight} />;
    }

    const ticks = Array.from({ length: 5 }, (_, idx) => {
        const ratio = idx / 4;
        return geometry.maxValue - ((geometry.maxValue - geometry.minValue) * ratio);
    });
    const stackedLegendLabels = stackedChildBarMode
        ? Array.from(new Set(
            (model.stackedGroups || []).reduce((labels, group) => {
                (group.segments || []).forEach((segment) => {
                    if (segment && segment.label) labels.push(segment.label);
                });
                return labels;
            }, [])
        ))
        : [];
    const stackedColorByLabel = stackedLegendLabels.reduce((acc, label, index) => {
        acc[label] = getColorForIndex(index, theme);
        return acc;
    }, {});
    const legendItems = stackedChildBarMode
        ? stackedLegendLabels.map((label) => ({ label, color: stackedColorByLabel[label] || getColorForIndex(0, theme) }))
        : (model.series || []).map((series, index) => ({
            label: series.name,
            color: getColorForIndex(index, theme),
        }));
    const groupedBands = geometry.hasHierarchicalBands
        ? model.categoryBands.reduce((groups, band, index) => {
            const prev = groups[groups.length - 1];
            if (prev && prev.groupKey === band.groupKey) {
                prev.endIndex = index;
                return groups;
            }
            groups.push({
                groupKey: band.groupKey,
                outerLabel: band.outerLabel,
                startIndex: index,
                endIndex: index,
            });
            return groups;
        }, [])
        : [];
    const categoryTargets = Array.isArray(model && model.categoryTargets) ? model.categoryTargets : [];
    const canActivateCategories = typeof onCategoryActivate === 'function' && categoryTargets.length === model.categories.length;

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <div style={{ width: '100%', minWidth: 0 }}>
                <svg
                    ref={svgRef}
                    viewBox={`0 0 ${CHART_WIDTH} ${effectiveChartHeight}`}
                    preserveAspectRatio="xMidYMid meet"
                    style={{
                        width: '100%',
                        height: 'auto',
                        display: 'block',
                        maxWidth: '100%',
                    }}
                    role="img"
                    aria-label={model.title}
                >
                    {ticks.map((tickValue, index) => {
                        const y = geometry.scaleY ? geometry.scaleY(tickValue) : null;
                        const x = geometry.scaleX ? geometry.scaleX(tickValue) : null;
                        return (
                            <g key={`tick-${index}`}>
                                {geometry.horizontalBarMode ? (
                                    <>
                                        <line x1={x} y1={geometry.margin.top} x2={x} y2={effectiveChartHeight - geometry.margin.bottom} stroke={theme.border} strokeDasharray="3 4" />
                                        <text x={x} y={effectiveChartHeight - geometry.margin.bottom + 18} textAnchor="middle" fontSize="11" fill={theme.textSec}>
                                            {formatChartNumber(tickValue)}
                                        </text>
                                    </>
                                ) : (
                                    <>
                                        <line x1={geometry.margin.left} y1={y} x2={CHART_WIDTH - geometry.margin.right} y2={y} stroke={theme.border} strokeDasharray="3 4" />
                                        <text x={geometry.margin.left - 8} y={y + 4} textAnchor="end" fontSize="11" fill={theme.textSec}>
                                            {formatChartNumber(tickValue)}
                                        </text>
                                    </>
                                )}
                            </g>
                        );
                    })}
                    <line
                        x1={geometry.horizontalBarMode ? geometry.baselineX : geometry.margin.left}
                        y1={geometry.horizontalBarMode ? geometry.margin.top : geometry.baselineY}
                        x2={geometry.horizontalBarMode ? geometry.baselineX : CHART_WIDTH - geometry.margin.right}
                        y2={geometry.horizontalBarMode ? effectiveChartHeight - geometry.margin.bottom : geometry.baselineY}
                        stroke={theme.textSec}
                        strokeWidth="1"
                    />

                    {geometry.hasHierarchicalBands && groupedBands.map((group, groupIndex) => {
                        if (groupIndex === groupedBands.length - 1) return null;
                        const divider = geometry.horizontalBarMode
                            ? geometry.margin.top + ((group.endIndex + 1) * geometry.step)
                            : geometry.margin.left + ((group.endIndex + 1) * geometry.step);
                        return (
                            <line
                                key={`group-divider-${group.groupKey}-${groupIndex}`}
                                x1={geometry.horizontalBarMode ? geometry.margin.left : divider}
                                y1={geometry.horizontalBarMode ? divider : geometry.margin.top}
                                x2={geometry.horizontalBarMode ? CHART_WIDTH - geometry.margin.right : divider}
                                y2={geometry.horizontalBarMode ? divider : effectiveChartHeight - geometry.margin.bottom + 58}
                                stroke={theme.border}
                                strokeDasharray="4 4"
                            />
                        );
                    })}

                    {stackedChildBarMode && (model.stackedGroups || []).map((group, groupIndex) => {
                        const slotStart = geometry.horizontalBarMode
                            ? geometry.margin.top + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2)
                            : geometry.margin.left + (groupIndex * geometry.step) + ((geometry.step - geometry.groupWidth) / 2);
                        const barThickness = Math.max(14, geometry.groupWidth * 0.64);
                        let positiveOffset = 0;
                        let negativeOffset = 0;

                        return (
                            <g key={`stacked-group-${group.key}`}>
                                {(group.segments || []).map((segment) => {
                                    const rawValue = segment && typeof segment.value === 'number' && Number.isFinite(segment.value) ? segment.value : null;
                                    if (rawValue === null) return null;
                                    const startValue = rawValue >= 0 ? positiveOffset : negativeOffset;
                                    const endValue = startValue + rawValue;
                                    if (rawValue >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    if (geometry.horizontalBarMode) {
                                        const x1 = geometry.scaleX(startValue);
                                        const x2 = geometry.scaleX(endValue);
                                        return (
                                            <rect
                                                key={`${group.key}-${segment.path || segment.label}`}
                                                x={Math.min(x1, x2)}
                                                y={slotStart}
                                                width={Math.max(1, Math.abs(x2 - x1))}
                                                height={barThickness}
                                                rx="3"
                                                fill={stackedColorByLabel[segment.label] || getColorForIndex(0, theme)}
                                                opacity="0.94"
                                            />
                                        );
                                    }
                                    const y1 = geometry.scaleY(startValue);
                                    const y2 = geometry.scaleY(endValue);
                                    const y = Math.min(y1, y2);
                                    const height = Math.max(1, Math.abs(y2 - y1));
                                    return (
                                        <rect
                                            key={`${group.key}-${segment.path || segment.label}`}
                                            x={slotStart}
                                            y={y}
                                            width={barThickness}
                                            height={height}
                                            rx="3"
                                            fill={stackedColorByLabel[segment.label] || getColorForIndex(0, theme)}
                                            opacity="0.94"
                                        />
                                    );
                                })}
                                {geometry.horizontalBarMode ? (
                                    <text
                                        x={geometry.margin.left - 10}
                                        y={slotStart + (barThickness / 2) + 4}
                                        textAnchor="end"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(group.label, 22)}
                                    </text>
                                ) : (
                                    <text
                                        x={slotStart + (barThickness / 2)}
                                        y={effectiveChartHeight - geometry.margin.bottom + 30}
                                        textAnchor="middle"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(group.label, 20)}
                                    </text>
                                )}
                            </g>
                        );
                    })}

                {!stackedChildBarMode && chartType === 'bar' && model.categories.map((category, categoryIndex) => {
                    const slotStart = geometry.horizontalBarMode
                        ? geometry.margin.top + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2
                        : geometry.margin.left + categoryIndex * geometry.step + (geometry.step - geometry.groupWidth) / 2;
                    const barThickness = stackedSeriesBarMode
                        ? Math.max(14, geometry.groupWidth * 0.64)
                        : Math.max(6, (geometry.groupWidth / Math.max(model.series.length, 1)) - 4);
                    let positiveOffset = 0;
                    let negativeOffset = 0;
                    return (
                        <g key={`bar-group-${category}`}>
                            {model.series.map((series, seriesIndex) => {
                                const value = series.values[categoryIndex];
                                if (value === null || value === undefined) return null;
                                if (stackedSeriesBarMode) {
                                    const startValue = value >= 0 ? positiveOffset : negativeOffset;
                                    const endValue = startValue + value;
                                    if (value >= 0) positiveOffset = endValue;
                                    else negativeOffset = endValue;
                                    if (geometry.horizontalBarMode) {
                                        const x1 = geometry.scaleX(startValue);
                                        const x2 = geometry.scaleX(endValue);
                                        return (
                                            <rect
                                                key={`${series.name}-${category}`}
                                                x={Math.min(x1, x2)}
                                                y={slotStart}
                                                width={Math.max(1, Math.abs(x2 - x1))}
                                                height={barThickness}
                                                rx="3"
                                                fill={getColorForIndex(seriesIndex, theme)}
                                                opacity="0.92"
                                            />
                                        );
                                    }
                                    const y1 = geometry.scaleY(startValue);
                                    const y2 = geometry.scaleY(endValue);
                                    return (
                                        <rect
                                            key={`${series.name}-${category}`}
                                            x={slotStart}
                                            y={Math.min(y1, y2)}
                                            width={barThickness}
                                            height={Math.max(1, Math.abs(y2 - y1))}
                                            rx="3"
                                            fill={getColorForIndex(seriesIndex, theme)}
                                            opacity="0.92"
                                        />
                                    );
                                }
                                if (geometry.horizontalBarMode) {
                                    const x = geometry.scaleX(value);
                                    const y = slotStart + seriesIndex * (barThickness + 4);
                                    return (
                                        <rect
                                            key={`${series.name}-${category}`}
                                            x={Math.min(x, geometry.baselineX)}
                                            y={y}
                                            width={Math.max(1, Math.abs(geometry.baselineX - x))}
                                            height={barThickness}
                                            rx="3"
                                            fill={getColorForIndex(seriesIndex, theme)}
                                            opacity="0.9"
                                        />
                                    );
                                }
                                const y = geometry.scaleY(value);
                                const height = Math.max(1, Math.abs(geometry.baselineY - y));
                                const x = slotStart + seriesIndex * (barThickness + 4);
                                return (
                                    <rect
                                        key={`${series.name}-${category}`}
                                        x={x}
                                        y={Math.min(y, geometry.baselineY)}
                                        width={barThickness}
                                        height={height}
                                        rx="3"
                                        fill={getColorForIndex(seriesIndex, theme)}
                                        opacity="0.9"
                                    />
                                );
                            })}
                        </g>
                    );
                })}

                {(chartType === 'line' || chartType === 'area') && (() => {
                    const positiveOffsets = model.categories.map(() => 0);
                    const negativeOffsets = model.categories.map(() => 0);
                    return model.series.map((series, seriesIndex) => {
                        const stroke = getColorForIndex(seriesIndex, theme);
                        const points = model.categories.map((_, categoryIndex) => {
                            const rawValue = series.values[categoryIndex];
                            if (rawValue === null || rawValue === undefined) return null;
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            if (stackedAreaMode) {
                                const startValue = rawValue >= 0 ? positiveOffsets[categoryIndex] : negativeOffsets[categoryIndex];
                                const endValue = startValue + rawValue;
                                if (rawValue >= 0) positiveOffsets[categoryIndex] = endValue;
                                else negativeOffsets[categoryIndex] = endValue;
                                return {
                                    x,
                                    y: geometry.scaleY(endValue),
                                    baseY: geometry.scaleY(startValue),
                                };
                            }
                            return {
                                x,
                                y: geometry.scaleY(rawValue),
                                baseY: geometry.baselineY,
                            };
                        });
                        const linePath = buildLinePath(points);
                        const areaPath = chartType === 'area' ? buildAreaBandPath(points) : '';
                        return (
                            <g key={`${series.name}-${chartType}`}>
                                {chartType === 'area' && areaPath && (
                                    <path d={areaPath} fill={stroke} opacity={stackedAreaMode ? '0.46' : '0.16'} />
                                )}
                                {linePath && (
                                    <path d={linePath} fill="none" stroke={stroke} strokeWidth="3" strokeLinejoin="round" strokeLinecap="round" />
                                )}
                                {points.map((point, pointIndex) => {
                                    if (!point) return null;
                                    return (
                                        <circle
                                            key={`${series.name}-${pointIndex}`}
                                            cx={point.x}
                                            cy={point.y}
                                            r="4"
                                            fill={stroke}
                                            stroke={theme.surfaceBg || theme.background || '#fff'}
                                            strokeWidth="2"
                                        />
                                    );
                                })}
                            </g>
                        );
                    });
                })()}

                    {canActivateCategories && model.categories.map((_, categoryIndex) => {
                        const target = categoryTargets[categoryIndex];
                        if (!target) return null;
                        if (geometry.horizontalBarMode) {
                            return (
                                <rect
                                    key={`category-hit-horizontal-${categoryIndex}`}
                                    x={geometry.margin.left}
                                    y={geometry.margin.top + (categoryIndex * geometry.step)}
                                    width={CHART_WIDTH - geometry.margin.left - geometry.margin.right}
                                    height={geometry.step}
                                    fill="transparent"
                                    style={{ cursor: 'pointer' }}
                                    onClick={() => onCategoryActivate(target)}
                                />
                            );
                        }
                        return (
                            <rect
                                key={`category-hit-vertical-${categoryIndex}`}
                                x={geometry.margin.left + (categoryIndex * geometry.step)}
                                y={geometry.margin.top}
                                width={geometry.step}
                                height={effectiveChartHeight - geometry.margin.top - geometry.margin.bottom}
                                fill="transparent"
                                style={{ cursor: 'pointer' }}
                                onClick={() => onCategoryActivate(target)}
                            />
                        );
                    })}

                    {stackedChildBarMode ? null : geometry.horizontalBarMode ? (
                    geometry.hasHierarchicalBands ? (
                        <>
                            {groupedBands.map((group) => {
                                const yStart = geometry.margin.top + (group.startIndex * geometry.step);
                                const yEnd = geometry.margin.top + ((group.endIndex + 1) * geometry.step);
                                const centerY = (yStart + yEnd) / 2;
                                return (
                                    <g key={`outer-label-horizontal-${group.groupKey}`}>
                                        <text
                                            x={10}
                                            y={centerY + 4}
                                        textAnchor="start"
                                        fontSize="11"
                                        fontWeight="700"
                                        fill={theme.text}
                                    >
                                            {truncateChartLabel(group.outerLabel, 18)}
                                        </text>
                                    </g>
                                );
                            })}
                            {model.categoryBands.map((band, categoryIndex) => {
                                const y = geometry.margin.top + (categoryIndex * geometry.step) + (geometry.step / 2);
                                const innerLabel = band && band.innerLabel ? String(band.innerLabel) : '';
                                return innerLabel ? (
                                    <text
                                        key={`inner-label-horizontal-${categoryIndex}`}
                                        x={geometry.margin.left - 10}
                                        y={y + 4}
                                        textAnchor="end"
                                        fontSize="11"
                                        fill={theme.textSec}
                                    >
                                        {truncateChartLabel(innerLabel, 18)}
                                    </text>
                                ) : null;
                            })}
                        </>
                    ) : (
                        model.categories.map((category, categoryIndex) => {
                            const y = geometry.margin.top + (categoryIndex * geometry.step) + (geometry.step / 2);
                            return (
                                <text
                                    key={`label-horizontal-${categoryIndex}`}
                                    x={geometry.margin.left - 10}
                                    y={y + 4}
                                    textAnchor="end"
                                    fontSize="11"
                                    fill={theme.textSec}
                                >
                                    {truncateChartLabel(category, 22)}
                                </text>
                            );
                        })
                    )
                ) : geometry.hasHierarchicalBands ? (
                    <>
                        {groupedBands.map((group) => {
                            const xStart = geometry.margin.left + (group.startIndex * geometry.step);
                            const xEnd = geometry.margin.left + ((group.endIndex + 1) * geometry.step);
                            const centerX = (xStart + xEnd) / 2;
                            return (
                                <g key={`outer-label-${group.groupKey}`}>
                                    <text
                                        x={centerX}
                                        y={effectiveChartHeight - geometry.margin.bottom + 68}
                                        textAnchor="middle"
                                        fontSize="11"
                                        fontWeight="700"
                                        fill={theme.text}
                                    >
                                        {truncateChartLabel(group.outerLabel, 22)}
                                    </text>
                                </g>
                            );
                        })}
                        {model.categoryBands.map((band, categoryIndex) => {
                            const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                            const innerLabel = band && band.innerLabel ? String(band.innerLabel) : '';
                            return (
                                <g key={`inner-label-${categoryIndex}`}>
                                    {innerLabel ? (
                                        <text
                                            x={x}
                                            y={effectiveChartHeight - geometry.margin.bottom + 30}
                                            textAnchor="middle"
                                            fontSize="11"
                                            fill={theme.textSec}
                                        >
                                            {truncateChartLabel(innerLabel, 18)}
                                        </text>
                                    ) : null}
                                </g>
                            );
                        })}
                    </>
                ) : (
                    model.categories.map((category, categoryIndex) => {
                        const x = geometry.margin.left + (categoryIndex * geometry.step) + (geometry.step / 2);
                        return (
                            <g key={`label-${categoryIndex}`} transform={`translate(${x}, ${effectiveChartHeight - geometry.margin.bottom + 18}) rotate(28)`}>
                                <text textAnchor="start" fontSize="11" fill={theme.textSec}>
                                    {truncateChartLabel(category, 20)}
                                </text>
                            </g>
                        );
                    })
                    )}
                </svg>
            </div>
            {showLegend ? <ChartLegend items={legendItems} theme={theme} /> : null}
        </div>
    );
};

const ChartSurface = ({
    model,
    chartType,
    barLayout,
    axisMode,
    onChartTypeChange,
    onBarLayoutChange,
    onAxisModeChange,
    orientation,
    onOrientationChange,
    hierarchyLevel,
    onHierarchyLevelChange,
    theme,
    title,
    onTitleChange,
    subtitle,
    note,
    chartHeight: chartHeightProp,
    onChartHeightChange,
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    sortMode,
    onSortModeChange,
    interactionMode,
    onInteractionModeChange,
    serverScope,
    onServerScopeChange,
    showServerScope = false,
    cinemaMode = false,
    onCategoryActivate,
    allowHierarchyCharts = true,
    locked = false,
    onToggleLock,
}) => {
    const canStack = chartType === 'area'
        ? canStackAreaSeries(model)
        : canStackBarLayout(model);
    const resolvedTitle = title !== undefined ? title : (model && model.title);
    const resolvedSubtitle = subtitle !== undefined ? subtitle : (model && model.subtitle);
    const resolvedNote = note !== undefined
        ? note
        : locked
            ? 'Locked to the current chart request'
            : (model && model.note);
    const [configOpen, setConfigOpen] = useState(false);
    const chartHeightResizeRef = useRef(null);
    const showChrome = !cinemaMode;
    const chartHeight = Math.max(180, Number.isFinite(Number(chartHeightProp)) ? Number(chartHeightProp) : (cinemaMode ? 520 : CHART_HEIGHT));
    const svgRef = useRef(null);
    const maxHierarchyLevel = (model && model.maxHierarchyLevel) || 1;
    const showHierarchySection = typeof onHierarchyLevelChange === 'function' && maxHierarchyLevel > 1;
    const configButtonStyle = {
        width: '32px',
        height: '32px',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: `1px solid ${configOpen ? theme.primary : theme.border}`,
        background: configOpen ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: configOpen ? theme.primary : theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        cursor: 'pointer',
        flexShrink: 0,
    };
    const lockButtonStyle = {
        ...configButtonStyle,
        border: `1px solid ${locked ? theme.primary : theme.border}`,
        background: locked ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: locked ? theme.primary : theme.textSec,
    };
    const exportButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.hover,
        color: theme.textSec,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    };
    const compactInputStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.surfaceBg || theme.background || '#fff',
        color: theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 600,
        minWidth: '68px',
    };

    useEffect(() => {
        if (!cinemaMode || !configOpen) return;
        setConfigOpen(false);
    }, [cinemaMode, configOpen]);

    useEffect(() => {
        const handlePointerMove = (event) => {
            if (!chartHeightResizeRef.current || typeof onChartHeightChange !== 'function') return;
            const resizeState = chartHeightResizeRef.current;
            const nextHeight = Math.max(180, resizeState.startHeight + (event.clientY - resizeState.startY));
            onChartHeightChange(nextHeight);
        };

        const stopResize = () => {
            chartHeightResizeRef.current = null;
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
    }, [onChartHeightChange]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: showChrome ? '14px' : '10px', flex: cinemaMode ? '1 1 auto' : '0 0 auto', minHeight: 0 }}>
            {showChrome ? (
                <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
                    <ChartHeader
                        title={resolvedTitle}
                        subtitle={resolvedSubtitle}
                        note={resolvedNote}
                        theme={theme}
                    />
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        <button
                            type="button"
                            onClick={() => exportSvgNode(svgRef.current, 'pivot-chart')}
                            style={exportButtonStyle}
                            title="Export chart as SVG"
                        >
                            SVG
                        </button>
                        {typeof onToggleLock === 'function' ? (
                            <button
                                type="button"
                                onClick={onToggleLock}
                                style={lockButtonStyle}
                                title={locked ? 'Unlock chart request' : 'Lock current chart request'}
                                aria-label={locked ? 'Unlock chart request' : 'Lock current chart request'}
                            >
                                <Icons.Lock />
                            </button>
                        ) : null}
                        <button
                            type="button"
                            onClick={() => setConfigOpen((isOpen) => !isOpen)}
                            style={configButtonStyle}
                            title={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                            aria-label={configOpen ? 'Hide chart settings' : 'Show chart settings'}
                        >
                            <Icons.Settings />
                        </button>
                    </div>
                </div>
            ) : null}
            {showChrome && configOpen ? (
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
                    gap: '8px',
                    padding: '8px',
                    border: `1px solid ${theme.border}`,
                    borderRadius: theme.radiusSm || '8px',
                    background: theme.headerSubtleBg || theme.hover,
                }}>
                    <ChartConfigSection
                        title="Basics"
                        description="Name the chart and control how much source data it pulls into the current view."
                        theme={theme}
                    >
                        {typeof onTitleChange === 'function' ? (
                            <ChartConfigField
                                label="Chart Title"
                                description="Displayed above the chart panel and in the chart chrome."
                                theme={theme}
                                controlMinWidth="220px"
                            >
                                <input
                                    type="text"
                                    value={resolvedTitle || ''}
                                    onChange={(event) => onTitleChange(event.target.value)}
                                    placeholder="Chart title"
                                    style={{ ...compactInputStyle, minWidth: '220px', width: '100%' }}
                                />
                            </ChartConfigField>
                        ) : null}
                        {typeof onChartHeightChange === 'function' ? (
                            <ChartConfigField
                                label="Chart Height"
                                description="Changes the plotted graph height without resizing the whole panel."
                                theme={theme}
                                controlMinWidth="96px"
                            >
                                <input
                                    type="number"
                                    min="180"
                                    step="20"
                                    value={chartHeight}
                                    onChange={(event) => onChartHeightChange(event.target.value)}
                                    style={compactInputStyle}
                                />
                            </ChartConfigField>
                        ) : null}
                        <ChartConfigField
                            label="Rows and Columns"
                            description="Limits how many row categories and column series are included in the chart model."
                            theme={theme}
                            controlMinWidth="180px"
                        >
                            <ChartLimitInputs
                                rowLimit={rowLimit}
                                onRowLimitChange={onRowLimitChange}
                                columnLimit={columnLimit}
                                onColumnLimitChange={onColumnLimitChange}
                                theme={theme}
                            />
                        </ChartConfigField>
                    </ChartConfigSection>

                    <ChartConfigSection
                        title="Layout"
                        description="Control how pivot rows and columns are mapped into the chart and how bars are drawn."
                        theme={theme}
                    >
                        {chartType !== 'icicle' && chartType !== 'sunburst' && chartType !== 'sankey' ? (
                            <ChartConfigField
                                label="Pivot Mapping"
                                description="Choose whether chart categories come from pivot rows or from pivot columns."
                                theme={theme}
                            >
                                <ChartOrientationButtons orientation={orientation} onChange={onOrientationChange} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                        {(chartType === 'bar') ? (
                            <ChartConfigField
                                label="Bar Direction"
                                description="Switch between vertical columns and horizontal bars."
                                theme={theme}
                            >
                                <ChartAxisButtons axisMode={axisMode} onChange={onAxisModeChange} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                        {(chartType === 'bar' || chartType === 'area') && (
                            <ChartConfigField
                                label="Series Layout"
                                description={chartType === 'area'
                                    ? 'Use normal layered areas or stack the visible series.'
                                    : 'Use grouped bars for comparison or stacked bars for accumulation.'}
                                theme={theme}
                            >
                                <ChartLayoutButtons
                                    chartType={chartType}
                                    barLayout={barLayout}
                                    onChange={onBarLayoutChange}
                                    canStack={canStack}
                                    theme={theme}
                                />
                            </ChartConfigField>
                        )}
                        {typeof onSortModeChange === 'function' ? (
                            <ChartConfigField
                                label="Category Order"
                                description="Sort chart categories by their natural order, labels, or current values."
                                theme={theme}
                                controlMinWidth="150px"
                            >
                                <ChartSortButtons sortMode={sortMode} onChange={onSortModeChange} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                    </ChartConfigSection>

                    {showHierarchySection ? (
                        <ChartConfigSection
                            title="Hierarchy"
                            description="Decide which hierarchy depth is shown when the pivot contains grouped row levels."
                            theme={theme}
                        >
                            <ChartConfigField
                                label="Hierarchy Level"
                                description="Pick a specific level, or use All to chart the current visible frontier."
                                theme={theme}
                            >
                                <ChartHierarchyButtons
                                    level={hierarchyLevel}
                                    onChange={onHierarchyLevelChange}
                                    maxLevel={maxHierarchyLevel}
                                    theme={theme}
                                />
                            </ChartConfigField>
                        </ChartConfigSection>
                    ) : null}

                    <ChartConfigSection
                        title="Interaction"
                        description="Control what the chart does when clicked and how much server-side data it uses."
                        theme={theme}
                    >
                        {typeof onInteractionModeChange === 'function' ? (
                            <ChartConfigField
                                label="Click Action"
                                description="Choose whether chart clicks jump the grid, filter it, or only emit an event."
                                theme={theme}
                            >
                                <ChartInteractionButtons interactionMode={interactionMode} onChange={onInteractionModeChange} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                        {showServerScope && typeof onServerScopeChange === 'function' ? (
                            <ChartConfigField
                                label="Server Scope"
                                description="Use only the current viewport or fetch chart data from the start of the pivot result."
                                theme={theme}
                            >
                                <ChartScopeButtons scope={serverScope} onChange={onServerScopeChange} theme={theme} />
                            </ChartConfigField>
                        ) : null}
                    </ChartConfigSection>

                    <ChartConfigSection
                        title="Type"
                        description="Select the visual shape of the chart, including hierarchy-native layouts."
                        theme={theme}
                    >
                        <ChartConfigField
                            label="Chart Type"
                            description="Switch between comparison charts and hierarchy charts such as Icicle, Sunburst, or Sankey."
                            theme={theme}
                            controlMinWidth="220px"
                        >
                            <ChartTypeButtons chartType={chartType} onChange={onChartTypeChange} theme={theme} includeHierarchyCharts={allowHierarchyCharts} />
                        </ChartConfigField>
                    </ChartConfigSection>
                </div>
            ) : null}
        <div style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            {!model
                ? <EmptyChartState message="No chart data available." theme={theme} chartHeight={chartHeight} />
                : ((!Array.isArray(model.series) || model.series.length === 0)
                    && chartType !== 'icicle'
                    && chartType !== 'sunburst'
                    && chartType !== 'sankey'
                    ? <EmptyChartState message={model.emptyMessage || 'No chart data available.'} theme={theme} chartHeight={chartHeight} />
                    : <SvgChart model={model} chartType={chartType} barLayout={barLayout} axisMode={axisMode} theme={theme} chartHeight={chartHeight} showLegend={showChrome} onCategoryActivate={onCategoryActivate} svgRef={svgRef} />)}
            {typeof onChartHeightChange === 'function' ? (
                <div
                    onMouseDown={(event) => {
                        event.preventDefault();
                        chartHeightResizeRef.current = {
                            startY: event.clientY,
                            startHeight: chartHeight,
                        };
                    }}
                    style={{
                        height: '22px',
                        cursor: 'ns-resize',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        gap: '8px',
                        marginTop: '6px',
                        color: theme.textSec,
                        fontSize: '11px',
                        fontWeight: 700,
                    }}
                    title="Drag to resize the graph height"
                >
                    <span>Graph Height</span>
                    <div style={{ width: '72px', height: '5px', borderRadius: '999px', background: theme.border, opacity: 0.95 }} />
                </div>
            ) : null}
        </div>
        {showChrome ? <ChartStats model={model} theme={theme} /> : null}
    </div>
    );
};

export const PivotChartPanel = ({
    open,
    onClose,
    source,
    onSourceChange,
    chartType,
    onChartTypeChange,
    barLayout,
    onBarLayoutChange,
    axisMode,
    onAxisModeChange,
    orientation,
    onOrientationChange,
    hierarchyLevel,
    onHierarchyLevelChange,
    model,
    theme,
    width,
    chartTitle,
    onChartTitleChange,
    chartHeight,
    onChartHeightChange,
    onResizeStart,
    floating = false,
    onToggleFloating,
    floatingRect = null,
    onFloatingDragStart,
    onFloatingResizeStart,
    rowLimit,
    onRowLimitChange,
    columnLimit,
    onColumnLimitChange,
    sortMode,
    onSortModeChange,
    interactionMode,
    onInteractionModeChange,
    serverScope,
    onServerScopeChange,
    chartDefinitions = [],
    activeChartId = null,
    onActiveChartChange,
    onCreateChart,
    onDuplicateChart,
    onDeleteChart,
    onRenameChart,
    onCategoryActivate,
    allowHierarchyCharts = true,
    showServerScope = false,
    locked = false,
    onToggleLock,
    cinemaMode: controlledCinemaMode,
    onCinemaModeChange,
    standalone = false,
    showResizeHandle = true,
    title = 'Chart Panel',
    showDefinitionManager = true,
}) => {
    const [fullscreenMode, setFullscreenMode] = useState(false);
    const [uncontrolledCinemaMode, setUncontrolledCinemaMode] = useState(false);
    if (!open) return null;
    const cinemaMode = typeof controlledCinemaMode === 'boolean' ? controlledCinemaMode : uncontrolledCinemaMode;
    const toggleCinemaMode = () => {
        const nextValue = !cinemaMode;
        if (typeof onCinemaModeChange === 'function') {
            onCinemaModeChange(nextValue);
            return;
        }
        setUncontrolledCinemaMode(nextValue);
    };

    const sourceButtonStyle = (value) => ({
        border: `1px solid ${value === source ? theme.primary : theme.border}`,
        background: value === source ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: value === source ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
    });
    const actionButtonStyle = (active = false) => ({
        border: `1px solid ${active ? theme.primary : theme.border}`,
        background: active ? theme.select : (theme.headerSubtleBg || theme.hover),
        color: active ? theme.primary : theme.text,
        borderRadius: theme.radiusSm || '8px',
        padding: '6px 8px',
        fontSize: '11px',
        fontWeight: 700,
        cursor: 'pointer',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '6px',
        minWidth: '32px',
    });

    const panelContainerStyle = fullscreenMode
        ? {
            position: 'fixed',
            inset: '16px',
            zIndex: 10040,
            display: 'flex',
            minWidth: 0,
            minHeight: 0,
        }
        : floating
            ? {
                position: 'absolute',
                left: `${(floatingRect && floatingRect.left) || 24}px`,
                top: `${(floatingRect && floatingRect.top) || 24}px`,
                width: `${(floatingRect && floatingRect.width) || Math.max(width || 430, 430)}px`,
                height: `${(floatingRect && floatingRect.height) || 520}px`,
                zIndex: 240,
                display: 'flex',
                minWidth: 0,
                minHeight: 0,
            }
            : standalone
                ? { display: 'flex', flex: '1 1 auto', minWidth: 0, minHeight: 0, overflow: 'hidden' }
                : { display: 'flex', width: `${width || 430}px`, minWidth: `${width || 430}px`, maxWidth: `${width || 430}px`, flexShrink: 0 };

    return (
        <div style={panelContainerStyle}>
            {!fullscreenMode && !floating && !standalone && showResizeHandle ? (
                <div
                    onMouseDown={(event) => {
                        event.preventDefault();
                        if (onResizeStart) onResizeStart();
                    }}
                    style={{
                        width: '8px',
                        cursor: 'col-resize',
                        background: 'transparent',
                        position: 'relative',
                        flexShrink: 0,
                    }}
                    title="Resize chart panel"
                >
                    <div style={{
                        position: 'absolute',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '3px',
                        height: '72px',
                        borderRadius: '999px',
                        background: theme.border,
                        opacity: 0.9,
                    }} />
                </div>
            ) : null}
            <aside style={{
                width: fullscreenMode || floating || standalone ? '100%' : `calc(100% - ${showResizeHandle ? 8 : 0}px)`,
                height: fullscreenMode || floating ? '100%' : 'auto',
                minWidth: 0,
                minHeight: 0,
                borderLeft: standalone || floating || fullscreenMode ? 'none' : `1px solid ${theme.border}`,
                border: floating || fullscreenMode ? `1px solid ${theme.border}` : 'none',
                borderRadius: floating || fullscreenMode ? (theme.radius || '16px') : 0,
                background: theme.sidebarBg || theme.surfaceBg || theme.background,
                padding: fullscreenMode ? '12px' : '14px',
                overflowY: 'auto',
                boxShadow: floating || fullscreenMode
                    ? (theme.shadowMd || '0 18px 40px rgba(15,23,42,0.24)')
                    : standalone ? 'none' : `-10px 0 24px ${theme.pinnedBoundaryShadow || 'rgba(15,23,42,0.12)'}`,
                display: 'flex',
                flexDirection: 'column',
                gap: cinemaMode ? '8px' : '0',
                position: 'relative',
            }}>
                <div
                    onMouseDown={(event) => {
                        if (floating && !fullscreenMode && typeof onFloatingDragStart === 'function') {
                            onFloatingDragStart(event);
                        }
                    }}
                    style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: cinemaMode ? '0' : '12px', cursor: floating && !fullscreenMode ? 'move' : 'default' }}
                >
                    {cinemaMode ? <div /> : (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', minWidth: 0 }}>
                            <div style={{ fontSize: '15px', fontWeight: 800, color: theme.text }}>{title}</div>
                        </div>
                    )}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }} onMouseDown={(event) => event.stopPropagation()}>
                        {typeof onToggleFloating === 'function' ? (
                            <button
                                type="button"
                                onClick={onToggleFloating}
                                style={actionButtonStyle(floating)}
                                title={floating ? 'Dock chart panel' : 'Float chart over table'}
                                aria-label={floating ? 'Dock chart panel' : 'Float chart over table'}
                            >
                                {floating ? 'Dock' : 'Float'}
                            </button>
                        ) : null}
                        <button
                            type="button"
                            onClick={toggleCinemaMode}
                            style={actionButtonStyle(cinemaMode)}
                            title={cinemaMode ? 'Exit cinema mode' : 'Enter cinema mode'}
                            aria-label={cinemaMode ? 'Exit cinema mode' : 'Enter cinema mode'}
                        >
                            Cinema
                        </button>
                        <button
                            type="button"
                            onClick={() => setFullscreenMode((isActive) => !isActive)}
                            style={actionButtonStyle(fullscreenMode)}
                            title={fullscreenMode ? 'Exit full screen' : 'Open chart full screen'}
                            aria-label={fullscreenMode ? 'Exit full screen' : 'Open chart full screen'}
                        >
                            {fullscreenMode ? <Icons.FullscreenExit /> : <Icons.Fullscreen />}
                        </button>
                        <button
                            onClick={onClose}
                            style={actionButtonStyle(false)}
                            title={standalone ? 'Close chart pane' : 'Close chart panel'}
                            aria-label={standalone ? 'Close chart pane' : 'Close chart panel'}
                        >
                            {fullscreenMode ? <Icons.Close /> : 'Close'}
                        </button>
                    </div>
                </div>
                {!cinemaMode && showDefinitionManager ? (
                    <div style={{ display: 'flex', gap: '8px', marginBottom: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
                        {Array.isArray(chartDefinitions) && chartDefinitions.length > 0 ? (
                            <>
                                <select
                                    value={activeChartId || ''}
                                    onChange={(event) => {
                                        if (typeof onActiveChartChange === 'function') onActiveChartChange(event.target.value);
                                    }}
                                    style={{
                                        border: `1px solid ${theme.border}`,
                                        background: theme.surfaceBg || theme.background || '#fff',
                                        color: theme.text,
                                        borderRadius: theme.radiusSm || '8px',
                                        padding: '6px 8px',
                                        fontSize: '11px',
                                        fontWeight: 700,
                                        minWidth: '160px',
                                    }}
                                >
                                    {chartDefinitions.map((definition) => (
                                        <option key={definition.id} value={definition.id}>{definition.name || definition.id}</option>
                                    ))}
                                </select>
                                <input
                                    type="text"
                                    value={(chartDefinitions.find((definition) => definition.id === activeChartId) || {}).name || ''}
                                    onChange={(event) => {
                                        if (typeof onRenameChart === 'function') onRenameChart(event.target.value);
                                    }}
                                    placeholder="Chart name"
                                    style={{
                                        border: `1px solid ${theme.border}`,
                                        background: theme.surfaceBg || theme.background || '#fff',
                                        color: theme.text,
                                        borderRadius: theme.radiusSm || '8px',
                                        padding: '6px 8px',
                                        fontSize: '11px',
                                        fontWeight: 700,
                                        minWidth: '180px',
                                    }}
                                />
                            </>
                        ) : null}
                        {typeof onCreateChart === 'function' ? <button type="button" style={actionButtonStyle(false)} onClick={onCreateChart}>New</button> : null}
                        {typeof onDuplicateChart === 'function' ? <button type="button" style={actionButtonStyle(false)} onClick={onDuplicateChart}>Duplicate</button> : null}
                        {typeof onDeleteChart === 'function' ? (
                            <button
                                type="button"
                                style={actionButtonStyle(false)}
                                onClick={onDeleteChart}
                                disabled={!Array.isArray(chartDefinitions) || chartDefinitions.length <= 1}
                            >
                                Delete
                            </button>
                        ) : null}
                    </div>
                ) : null}
                {!cinemaMode ? (
                    <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap' }}>
                        <button style={sourceButtonStyle('pivot')} onClick={() => onSourceChange('pivot')}>Pivot View</button>
                        <button style={sourceButtonStyle('selection')} onClick={() => onSourceChange('selection')}>Selection</button>
                    </div>
                ) : null}
                <ChartSurface
                    model={model}
                    chartType={chartType}
                    barLayout={barLayout}
                    axisMode={axisMode}
                    onChartTypeChange={onChartTypeChange}
                    onBarLayoutChange={onBarLayoutChange}
                    onAxisModeChange={onAxisModeChange}
                    orientation={orientation}
                    onOrientationChange={onOrientationChange}
                    hierarchyLevel={hierarchyLevel}
                    onHierarchyLevelChange={onHierarchyLevelChange}
                    rowLimit={rowLimit}
                    onRowLimitChange={onRowLimitChange}
                    columnLimit={columnLimit}
                    onColumnLimitChange={onColumnLimitChange}
                    title={chartTitle}
                    onTitleChange={onChartTitleChange}
                    chartHeight={chartHeight}
                    onChartHeightChange={onChartHeightChange}
                    sortMode={sortMode}
                    onSortModeChange={onSortModeChange}
                    interactionMode={interactionMode}
                    onInteractionModeChange={onInteractionModeChange}
                    serverScope={serverScope}
                    onServerScopeChange={onServerScopeChange}
                    showServerScope={showServerScope}
                    theme={theme}
                    cinemaMode={cinemaMode}
                    onCategoryActivate={onCategoryActivate}
                    allowHierarchyCharts={allowHierarchyCharts}
                    locked={locked}
                    onToggleLock={onToggleLock}
                />
                {floating && !fullscreenMode ? (
                    <>
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('right', event)}
                            style={{ position: 'absolute', top: 0, right: 0, width: '10px', height: '100%', cursor: 'ew-resize' }}
                        />
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('bottom', event)}
                            style={{ position: 'absolute', left: 0, bottom: 0, width: '100%', height: '10px', cursor: 'ns-resize' }}
                        />
                        <div
                            onMouseDown={(event) => typeof onFloatingResizeStart === 'function' && onFloatingResizeStart('corner', event)}
                            style={{ position: 'absolute', right: 0, bottom: 0, width: '18px', height: '18px', cursor: 'nwse-resize' }}
                        />
                    </>
                ) : null}
            </aside>
        </div>
    );
};

export const PivotChartModal = ({ chartState, onClose, theme }) => {
    const [chartType, setChartType] = useState(chartState && chartState.chartType ? chartState.chartType : 'bar');
    const [barLayout, setBarLayout] = useState(chartState && chartState.barLayout ? chartState.barLayout : 'grouped');
    const [axisMode, setAxisMode] = useState(chartState && chartState.axisMode ? chartState.axisMode : 'vertical');
    const [orientation, setOrientation] = useState(chartState && chartState.defaultOrientation ? chartState.defaultOrientation : 'rows');
    const [hierarchyLevel, setHierarchyLevel] = useState(chartState && chartState.defaultHierarchyLevel !== undefined ? chartState.defaultHierarchyLevel : 'all');
    const [rowLimit, setRowLimit] = useState(chartState && chartState.rowLimit ? chartState.rowLimit : MAX_SELECTION_CATEGORIES);
    const [columnLimit, setColumnLimit] = useState(chartState && chartState.columnLimit ? chartState.columnLimit : MAX_SELECTION_SERIES);
    const [sortMode, setSortMode] = useState(chartState && chartState.sortMode ? chartState.sortMode : 'natural');

    useEffect(() => {
        setChartType(chartState && chartState.chartType ? chartState.chartType : 'bar');
        setBarLayout(chartState && chartState.barLayout ? chartState.barLayout : 'grouped');
        setAxisMode(chartState && chartState.axisMode ? chartState.axisMode : 'vertical');
        setOrientation(chartState && chartState.defaultOrientation ? chartState.defaultOrientation : 'rows');
        setHierarchyLevel(chartState && chartState.defaultHierarchyLevel !== undefined ? chartState.defaultHierarchyLevel : 'all');
        setRowLimit(chartState && chartState.rowLimit ? chartState.rowLimit : MAX_SELECTION_CATEGORIES);
        setColumnLimit(chartState && chartState.columnLimit ? chartState.columnLimit : MAX_SELECTION_SERIES);
        setSortMode(chartState && chartState.sortMode ? chartState.sortMode : 'natural');
    }, [chartState]);

    const activeModel = useMemo(() => {
        if (!chartState) return null;
        if (chartState.selectionMap && chartState.visibleRows && chartState.visibleCols) {
            return buildSelectionChartModel(chartState.selectionMap, chartState.visibleRows, chartState.visibleCols, {
                orientation,
                hierarchyLevel,
                maxHierarchyLevel: chartState.maxHierarchyLevel || 1,
                maxRows: rowLimit,
                maxColumns: columnLimit,
                rowFields: chartState.rowFields || [],
                colFields: chartState.colFields || [],
                sortMode,
            });
        }
        return chartState.model;
    }, [chartState, hierarchyLevel, orientation, rowLimit, columnLimit, sortMode]);

    useEffect(() => {
        if (barLayout !== 'stacked') return;
        const canKeepStacked = chartType === 'area'
            ? (activeModel && Array.isArray(activeModel.series) && activeModel.series.length > 1)
            : canStackBarLayout(activeModel);
        if (canKeepStacked) return;
        setBarLayout('grouped');
    }, [activeModel, barLayout, chartType]);

    if (!chartState) return null;

    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 10005,
                background: 'rgba(2, 6, 23, 0.52)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '24px',
            }}
        >
            <div
                onClick={(event) => event.stopPropagation()}
                style={{
                    width: 'min(1040px, 92vw)',
                    maxHeight: '88vh',
                    overflowY: 'auto',
                    background: theme.surfaceBg || theme.background || '#fff',
                    border: `1px solid ${theme.border}`,
                    borderRadius: theme.radius || '16px',
                    boxShadow: theme.shadowMd || '0 18px 40px rgba(0,0,0,0.28)',
                    padding: '18px',
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: '14px' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                        <div style={{ fontSize: '16px', fontWeight: 800, color: theme.text }}>
                            {chartState.title || 'Range Chart'}
                        </div>
                    </div>
                    <button
                        onClick={onClose}
                        style={{
                            border: `1px solid ${theme.border}`,
                            background: theme.headerSubtleBg || theme.hover,
                            color: theme.text,
                            borderRadius: theme.radiusSm || '8px',
                            padding: '7px 10px',
                            fontSize: '11px',
                            fontWeight: 700,
                            cursor: 'pointer',
                        }}
                    >
                        Close
                    </button>
                </div>
                <ChartSurface
                    model={activeModel}
                    chartType={chartType}
                    barLayout={barLayout}
                    axisMode={axisMode}
                    onChartTypeChange={setChartType}
                    onBarLayoutChange={setBarLayout}
                    onAxisModeChange={setAxisMode}
                    orientation={orientation}
                    onOrientationChange={setOrientation}
                    hierarchyLevel={hierarchyLevel}
                    onHierarchyLevelChange={setHierarchyLevel}
                    rowLimit={rowLimit}
                onRowLimitChange={setRowLimit}
                columnLimit={columnLimit}
                onColumnLimitChange={setColumnLimit}
                sortMode={sortMode}
                onSortModeChange={setSortMode}
                theme={theme}
                allowHierarchyCharts={!(chartState.selectionMap && chartState.visibleRows && chartState.visibleCols)}
            />
        </div>
        </div>
    );
};

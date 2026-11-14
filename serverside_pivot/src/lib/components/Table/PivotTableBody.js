import React from 'react';
import SkeletonRow from '../SkeletonRow';
import StatusBar from './StatusBar';
import { usePivotTheme } from '../../contexts/PivotThemeContext';
import { usePivotConfig } from '../../contexts/PivotConfigContext';
import { buildRowSpanPlan, collectRowSpanColumns } from '../../utils/rowSpanning';
import { getPivotPerformanceNow, recordPivotMeasure } from '../../utils/pivotProfiler';
import { usePivotRenderCounter } from '../../hooks/usePivotRenderCounter';
import { isGrandTotalRow } from '../../hooks/usePivotNormalization';
import { resolveDepthToken, resolveGridLineBorders } from '../../utils/styles';

/** @type {any} */
const LazyInlineDetailPanel = React.lazy(() => import(/* webpackChunkName: "pivot-detail-inline" */ './InlineDetailPanel'));

/** @param {any} [theme] @returns {any} */
const getTotalRowBackground = (theme) =>
    theme.totalBg || theme.select || theme.background;

/** @param {any} [groups] @param {any} [levelHeights] @param {any} [fallbackHeight] @returns {any} */
export const resolveSectionHeaderSpanMeta = (groups, levelHeights, fallbackHeight) => {
    /** @type {any[]} */
    const coveredByLevel = [];
    /** @type {any} */
    const spanHeightsByHeaderId = new Map();
    /** @type {any} */
    const coveredColIds = new Set();
    /** @param {any} [header] @returns {any} */
    const getRowSpan = (header) => {
        if (!header) return 1;
        if (Number.isFinite(header.rowSpan) && header.rowSpan > 1) return header.rowSpan;
        if (!header.isPlaceholder) return 1;
        let span = 1;
        let current = header;
        while (
            current
            && current.isPlaceholder
            && Array.isArray(current.subHeaders)
            && current.subHeaders.length === 1
        ) {
            span += 1;
            current = current.subHeaders[0];
        }
        return span;
    };
    (groups || []).forEach(/** @param {any} [group] @param {any} [levelIdx] @returns {any} */ (group, levelIdx) => {
        /** @type {any} */
        const coveredHere = new Set(coveredColIds);
        coveredByLevel.push(coveredHere);
        (group.headers || []).forEach(/** @param {any} [header] @returns {any} */ (header) => {
            /** @type {any} */
            const colId = header && header.column ? header.column.id : null;
            if (!colId || coveredHere.has(colId)) return;
            /** @type {any} */
            const span = getRowSpan(header);
            if (header.isPlaceholder && span > 1) {
                coveredColIds.add(colId);
                let totalSpanHeight = 0;
                for (let offset = 0; offset < span; offset += 1) {
                    /** @type {any} */
                    const rowH = (Array.isArray(levelHeights) && Number.isFinite(levelHeights[levelIdx + offset]) && levelHeights[levelIdx + offset] > 0)
                        ? levelHeights[levelIdx + offset]
                        : fallbackHeight;
                    totalSpanHeight += rowH;
                }
                spanHeightsByHeaderId.set(header.id, totalSpanHeight);
            }
        });
    });
    return { coveredByLevel, spanHeightsByHeaderId };
};

/** @param {any} [column] @param {any} [leafIdCache] @returns {any} */
const getLeafColumnIds = (column, leafIdCache) => {
    if (!column) return [];
    /** @type {any} */
    const cacheKey = column.id || column;
    if (leafIdCache.has(cacheKey)) return leafIdCache.get(cacheKey);

    /** @type {any[]} */
    const leafIds = [];
    /** @type {any[]} */
    const stack = [column];
    while (stack.length > 0) {
        /** @type {any} */
        const current = stack.pop();
        if (!current) continue;
        /** @type {any} */
        const children = Array.isArray(current.columns) ? current.columns : [];
        if (children.length > 0) {
            for (let index = children.length - 1; index >= 0; index -= 1) {
                stack.push(children[index]);
            }
        } else if (current.id) {
            leafIds.push(current.id);
        }
    }

    if (leafIds.length === 0 && typeof column.getLeafColumns === 'function') {
        column.getLeafColumns().forEach(/** @param {any} [leafColumn] @returns {any} */ (leafColumn) => {
            if (leafColumn && leafColumn.id) leafIds.push(leafColumn.id);
        });
    }
    if (leafIds.length === 0 && column.id) {
        leafIds.push(column.id);
    }

    leafIdCache.set(cacheKey, leafIds);
    return leafIds;
};

/** @param {any} [indices] @returns {any} */
const isContiguousIndexRange = (indices) => {
    if (!Array.isArray(indices) || indices.length <= 1) return true;
    for (let index = 1; index < indices.length; index += 1) {
        if (indices[index] !== indices[index - 1] + 1) return false;
    }
    return true;
};

/**
 * PivotTableBody — the main scroll container and virtual-scroll table body.
 *
 * Contains: sticky header (left/center/right), top-pinned rows, virtualized
 * center rows (with skeleton, expand/collapse loaders), bottom-pinned rows,
 * floating filters, column-loading skeletons, and the StatusBar.
  @type {any} */
export const PivotTableBody = React.memo(/** @param {any} p0 @returns {any} */ function PivotTableBody({
    // Scroll container
    parentRef,
    handleKeyDown,
    rows,
    visibleLeafColumns,
    // Layout widths
    totalLayoutWidth,
    beforeWidth,
    afterWidth,
    bodyRowsTopOffset,
    stickyHeaderHeight,
    // Pinned / virtual rows
    effectiveTopRows,
    effectiveBottomRows,
    effectiveCenterRows,
    virtualRows,
    virtualCenterCols,
    // Virtualizers
    rowVirtualizer,
    // Row model
    rowModelLookup,
    getRow,
    serverSide,
    serverSidePinsGrandTotal,
    pendingRowTransitions,
    // Pre-computed header groups (avoids passing unstable `table` instance)
    leftHeaderGroups,
    centerHeaderGroups: centerHeaderGroupsProp,
    rightHeaderGroups,
    leftCols,
    centerCols,
    rightCols,
    centerColIndexMap,
    totalCenterColumns,
    // Display options
    rowHeight,
    headerHeight: headerHeightProp,
    layoutMode,
    grandTotalPosition,
    showColumnLoadingSkeletons,
    pendingColumnSkeletonCount,
    columnSkeletonWidth,
    // Render helpers
    renderCell,
    renderVirtualColumnCell,
    renderHeaderCell,
    // Filter interaction
    handleHeaderFilter,
    // Status bar
    selectedCells,
    rowCount,
    isRequestPending,
    columnAdvisory,
    statusModel,
    statusActions,
    treeSuppressGroupRowsSticky,
    immersiveMode,
    editValueDisplayMode,
    detailMode,
    detailState,
    detailInlineHeight,
    onDetailClose,
    onDetailPageChange,
    onDetailSort,
    onDetailFilter,
}) {
    usePivotRenderCounter('PivotTableBody');
    /** @type {any} */
    const { theme, styles } = usePivotTheme();
    /** @type {any} */
    const {
        filters,
        pivotMode,
        reportDef,
        showFloatingFilters,
        stickyHeaders,
        showColTotals,
        numberGroupSeparator,
        viewMode,
    } = usePivotConfig();
    /** @type {any} */
    const pinnedRowIdSet = React.useMemo(() => {
        /** @type {any} */
        const ids = new Set();
        [...effectiveTopRows, ...effectiveBottomRows].forEach((row) => {
            if (row && row.id) ids.add(row.id);
        });
        return ids;
    }, [effectiveTopRows, effectiveBottomRows]);
    /** @type {any} */
    const pinnedVirtualIndexes = React.useMemo(() => {
        if (!serverSide) return [];
        /** @type {any} */
        const indexes = new Set();
        [...effectiveTopRows, ...effectiveBottomRows].forEach((row) => {
            /** @type {any} */
            const virtualIndex = row && row.original && typeof row.original.__virtualIndex === 'number'
                ? row.original.__virtualIndex
                : null;
            if (virtualIndex !== null) indexes.add(virtualIndex);
        });
        return Array.from(indexes).sort((a, b) => a - b);
    }, [serverSide, effectiveTopRows, effectiveBottomRows]);
    /** @type {any} */
    const pinnedVirtualIndexSet = React.useMemo(
        () => new Set(pinnedVirtualIndexes),
        [pinnedVirtualIndexes]
    );
    /** @type {(...args: any[]) => any} */
    const getPinnedRowsBefore = React.useCallback((virtualIndex) => {
        if (!serverSide || pinnedVirtualIndexes.length === 0) return 0;
        let count = 0;
        for (const pinnedIndex of pinnedVirtualIndexes) {
            if (pinnedIndex >= virtualIndex) break;
            count += 1;
        }
        return count;
    }, [serverSide, pinnedVirtualIndexes]);

    /** @type {any} */
    const centerRowLookup = React.useMemo(() => {
        /** @type {any} */
        const lookup = new Map();
        effectiveCenterRows.forEach(/** @param {any} [row] @returns {any} */ (row) => {
            if (row && row.id) lookup.set(row.id, row);
        });
        return lookup;
    }, [effectiveCenterRows]);

    /** @type {any} */
    const leftPinnedWidth = React.useMemo(
        () => leftCols.reduce(/** @param {any} [sum] @param {any} [column] @returns {any} */ (sum, column) => sum + column.getSize(), 0),
        [leftCols]
    );
    /** @type {any} */
    const rightPinnedWidth = React.useMemo(
        () => rightCols.reduce(/** @param {any} [sum] @param {any} [column] @returns {any} */ (sum, column) => sum + column.getSize(), 0),
        [rightCols]
    );
    /** @type {any} */
    const visibleCenterWidth = React.useMemo(
        () => virtualCenterCols.reduce(/** @param {any} [sum] @param {any} [virtualCol] @returns {any} */ (sum, virtualCol) => {
            const centerColumn = centerCols[virtualCol.index];
            return sum + (centerColumn ? centerColumn.getSize() : 0);
        }, 0),
        [virtualCenterCols, centerCols]
    );
    // centerHeaderGroups are passed as a pre-computed prop from the parent
    // to avoid passing the unstable `table` instance which defeats React.memo.
    const centerHeaderGroups = centerHeaderGroupsProp;
    /** @type {any} */
    const visibleCenterRange = React.useMemo(() => {
        if (virtualCenterCols.length === 0) return { start: -1, end: -1 };
        return {
            start: virtualCenterCols[0].index,
            end: virtualCenterCols[virtualCenterCols.length - 1].index,
        };
    }, [virtualCenterCols]);

    // Pre-compute header-to-center-leaf indexes keyed by column structure.
    // getLeafColumns() walks the column tree and is expensive with 1000+ columns;
    // caching it here avoids repeated header/leaf tree walks on every scroll frame.
    /** @type {any} */
    const centerColumnWidthPrefix = React.useMemo(() => {
        /** @type {any} */
        const prefix = new Array(centerCols.length + 1);
        prefix[0] = 0;
        for (let index = 0; index < centerCols.length; index += 1) {
            const column = centerCols[index];
            prefix[index + 1] = prefix[index] + (column ? column.getSize() : 0);
        }
        return prefix;
    }, [centerCols, totalLayoutWidth]);

    /** @type {any} */
    const headerLeafPairsMap = React.useMemo(() => {
        /** @type {any} */
        const startedAt = getPivotPerformanceNow();
        /** @type {any} */
        const map = new Map();
        /** @type {any} */
        const leafIdCache = new Map();
        for (const group of centerHeaderGroups) {
            for (const header of group.headers) {
                /** @type {any} */
                const leafIds = getLeafColumnIds(header.column, leafIdCache);
                /** @type {any[]} */
                const indices = [];
                for (let i = 0; i < leafIds.length; i += 1) {
                    const leafId = leafIds[i];
                    /** @type {any} */
                    const idx = centerColIndexMap.has(leafId) ? centerColIndexMap.get(leafId) : -1;
                    if (idx >= 0) {
                        indices.push(idx);
                    }
                }
                if (indices.length > 0) {
                    indices.sort((left, right) => left - right);
                    map.set(header.id, {
                        indices,
                        minIdx: indices[0],
                        maxIdx: indices[indices.length - 1],
                        contiguous: isContiguousIndexRange(indices),
                    });
                }
            }
        }
        recordPivotMeasure('PivotTableBody.headerLeafIndexMap', startedAt, {
            componentId: 'PivotTableBody',
            headerGroups: centerHeaderGroups.length,
            centerColumns: centerCols.length,
            cachedColumns: leafIdCache.size,
            mappedHeaders: map.size,
        });
        return map;
    }, [centerColIndexMap, centerCols.length, centerHeaderGroups]);

    /** @type {any} */
    const effectiveHeaderHeight = headerHeightProp || rowHeight;
    /** @type {any} */
    const gridBorders = React.useMemo(() => resolveGridLineBorders(theme), [theme]);
    /** @type {any} */
    const stickyHeaderRef = React.useRef(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [measuredHeaderMetrics, setMeasuredHeaderMetrics] = React.useState({ totalHeight: 0, levelHeights: [] });

    React.useEffect(() => {
        const el = stickyHeaderRef.current;
        if (!el) return undefined;
        /** @returns {any} */
        const measure = () => {
            /** @type {any} */
            const totalHeight = Math.ceil(el.getBoundingClientRect ? el.getBoundingClientRect().height : (el.offsetHeight || 0));
            /** @type {any[]} */
            const levelHeights = [];
            if (typeof el.querySelectorAll === 'function') {
                /** @type {any} */
                const rowEls = el.querySelectorAll('[data-header-level]');
                rowEls.forEach(/** @param {any} [rowEl] @returns {any} */ (rowEl) => {
                    /** @type {any} */
                    const idx = Number(rowEl.getAttribute('data-header-level'));
                    if (!Number.isFinite(idx) || idx < 0) return;
                    /** @type {any} */
                    const h = Math.ceil(rowEl.getBoundingClientRect ? rowEl.getBoundingClientRect().height : (rowEl.offsetHeight || 0));
                    if (h > 0) {
                        levelHeights[idx] = Math.max(levelHeights[idx] || effectiveHeaderHeight, h);
                    }
                });
            }
            setMeasuredHeaderMetrics(/** @param {any} [prev] @returns {any} */ (prev) => {
                /** @type {any} */
                const sameTotal = prev.totalHeight === totalHeight;
                /** @type {any} */
                const sameLevels = prev.levelHeights.length === levelHeights.length
                    && prev.levelHeights.every(/** @param {any} [val] @param {any} [i] @returns {any} */ (val, i) => val === levelHeights[i]);
                if (sameTotal && sameLevels) return prev;
                return { totalHeight, levelHeights };
            });
        };
        measure();
        if (typeof ResizeObserver !== 'undefined') {
            /** @type {any} */
            const ro = new ResizeObserver(measure);
            ro.observe(el);
            return () => ro.disconnect();
        }
        return undefined;
    }, [effectiveHeaderHeight, leftHeaderGroups, centerHeaderGroups, rightHeaderGroups, showFloatingFilters, theme.headerWrap, theme.headerFontSize, theme.headerLineHeight]);

    /** @type {any} */
    const effectiveStickyHeaderHeight = measuredHeaderMetrics.totalHeight > stickyHeaderHeight
        ? measuredHeaderMetrics.totalHeight
        : stickyHeaderHeight;
    /** @type {any} */
    const effectiveBodyRowsTopOffset = bodyRowsTopOffset + (effectiveStickyHeaderHeight - stickyHeaderHeight);
    /** @type {any} */
    const leftHeaderSpanMeta = React.useMemo(
        () => resolveSectionHeaderSpanMeta(leftHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight),
        [leftHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight]
    );
    /** @type {any} */
    const centerHeaderSpanMeta = React.useMemo(
        () => resolveSectionHeaderSpanMeta(centerHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight),
        [centerHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight]
    );
    /** @type {any} */
    const rightHeaderSpanMeta = React.useMemo(
        () => resolveSectionHeaderSpanMeta(rightHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight),
        [rightHeaderGroups, measuredHeaderMetrics.levelHeights, effectiveHeaderHeight]
    );

    // ── Memoized base styles to avoid creating new objects per row/render ──
    /** @type {any} */
    const baseRowStyle = React.useMemo(() => ({
        ...styles.row,
        width: `${totalLayoutWidth}px`,
        borderBottom: `1px solid ${gridBorders.rowBorderColor}`,
    }), [styles.row, totalLayoutWidth, gridBorders.rowBorderColor]);

    /** @type {any} */
    const skeletonRowStyle = React.useMemo(() => ({
        ...baseRowStyle,
        position: 'absolute',
        display: 'flex',
        alignItems: 'center',
        zIndex: 90,
        pointerEvents: 'none',
    }), [baseRowStyle]);

    /** @type {any} */
    const transitionLoaderStyle = React.useMemo(() => ({
        ...styles.row,
        pointerEvents: 'none',
        height: rowHeight,
        width: `${totalLayoutWidth}px`,
        position: 'absolute',
        background: 'var(--pivot-loading-row-gradient, linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%))',
        backgroundSize: '220% 100%',
        borderBottom: `1px dashed ${theme.border}`,
        display: 'flex',
        alignItems: 'center',
        overflow: 'hidden',
        opacity: 0.95,
        zIndex: 90,
        boxShadow: `0 4px 12px -8px ${theme.border}`,
        animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite',
    }), [styles.row, rowHeight, totalLayoutWidth, theme.border]);

    /** @type {any} */
    const defaultRowBg = theme.leafBg || theme.surfaceBg || theme.background;
    /** @type {any} */
    const totalRowBg = getTotalRowBackground(theme);
    /** @type {(...args: any[]) => any} */
    const getRowLevelBackground = React.useCallback((row) => {
        if (!row || !row.original) return defaultRowBg;
        if (row.original._isTotal) return theme.totalBgStrong || totalRowBg;
        /** @type {any} */
        const isGroup = Boolean(row.original._has_children) || (typeof row.getIsGrouped === 'function' && row.getIsGrouped()) || Boolean(row.subRows && row.subRows.length > 0);
        if (isGroup) {
            /** @type {any} */
            const depth = Number(row.original.depth !== undefined ? row.original.depth : (row.depth || 0));
            return resolveDepthToken(theme.groupBg, depth, null) || defaultRowBg;
        }
        return theme.leafBg || defaultRowBg;
    }, [defaultRowBg, theme.groupBg, theme.leafBg, theme.totalBgStrong, totalRowBg]);
    /** @type {(...args: any[]) => any} */
    const getRowDataAttributes = React.useCallback((row) => {
        if (!row || !row.original) return {};
        /** @type {any} */
        const isTotal = Boolean(row.original._isTotal);
        /** @type {any} */
        const depth = Number(row.original.depth !== undefined ? row.original.depth : (row.depth || 0));
        /** @type {any} */
        const isGroup = !isTotal && (Boolean(row.original._has_children) || (typeof row.getIsGrouped === 'function' && row.getIsGrouped()) || Boolean(row.subRows && row.subRows.length > 0));
        return {
            'data-depth': Number.isFinite(depth) ? depth : 0,
            'data-leaf': !isTotal && !isGroup ? 'true' : 'false',
            'data-total': isTotal ? 'true' : 'false',
        };
    }, []);

    /** @type {any} */
    const leftPinnedSectionStyle = React.useMemo(() => leftPinnedWidth > 0 ? ({
        display: 'flex',
        width: leftPinnedWidth,
        minWidth: leftPinnedWidth,
        flexShrink: 0,
        position: 'sticky',
        left: 0,
        zIndex: 72,
        boxShadow: `2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`,
    }) : null, [leftPinnedWidth, theme.pinnedBoundaryShadow]);

    /** @type {any} */
    const rightPinnedSectionStyle = React.useMemo(() => rightPinnedWidth > 0 ? ({
        display: 'flex',
        width: rightPinnedWidth,
        minWidth: rightPinnedWidth,
        flexShrink: 0,
        position: 'sticky',
        right: 0,
        zIndex: 72,
        boxShadow: `-2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`,
    }) : null, [rightPinnedWidth, theme.pinnedBoundaryShadow]);

    /** @type {any} */
    const centerFlexStyle = React.useMemo(() => ({
        display: 'flex', flexShrink: 0,
    }), []);

    /** @type {any} */
    const beforeSpacerStyle = React.useMemo(() => ({
        width: beforeWidth, flexShrink: 0,
    }), [beforeWidth]);

    /** @type {any} */
    const afterSpacerStyle = React.useMemo(() => ({
        width: afterWidth, flexShrink: 0,
    }), [afterWidth]);

    /** @type {any} */
    const headerRowStyle = React.useMemo(() => ({
        display: 'flex',
        minHeight: effectiveHeaderHeight,
        height: theme.headerWrap ? 'auto' : effectiveHeaderHeight,
        borderBottom: `1px solid ${gridBorders.headerBorderBottomColor}`,
    }), [effectiveHeaderHeight, theme.headerWrap, gridBorders.headerBorderBottomColor]);

    /** @type {any} */
    const centerHeaderRenderPlan = React.useMemo(
        () => {
            /** @type {any} */
            const startedAt = getPivotPerformanceNow();
            /** @type {any} */
            const plan = centerHeaderGroups.map(/** @param {any} [group] @returns {any} */ (group) => {
                /** @type {any[]} */
                const visibleHeaders = [];
                for (const header of group.headers) {
                    /** @type {any} */
                    const centerLeafEntry = headerLeafPairsMap.get(header.id);
                    if (!centerLeafEntry) continue;
                    if (
                        centerLeafEntry.maxIdx < visibleCenterRange.start
                        || centerLeafEntry.minIdx > visibleCenterRange.end
                    ) {
                        continue;
                    }

                    let visWidth = 0;
                    if (centerLeafEntry.contiguous) {
                        /** @type {any} */
                        const visibleStart = Math.max(centerLeafEntry.minIdx, visibleCenterRange.start);
                        /** @type {any} */
                        const visibleEnd = Math.min(centerLeafEntry.maxIdx, visibleCenterRange.end);
                        visWidth = centerColumnWidthPrefix[visibleEnd + 1] - centerColumnWidthPrefix[visibleStart];
                    } else {
                        for (const idx of centerLeafEntry.indices) {
                            if (idx < visibleCenterRange.start) continue;
                            if (idx > visibleCenterRange.end) break;
                            const centerColumn = centerCols[idx];
                            visWidth += centerColumn ? centerColumn.getSize() : 0;
                        }
                    }
                    if (visWidth <= 0) continue;
                    visibleHeaders.push({ header, visWidth });
                }
                return {
                    groupId: group.id,
                    visibleHeaders,
                };
            });
            recordPivotMeasure('PivotTableBody.centerHeaderRenderPlan', startedAt, {
                componentId: 'PivotTableBody',
                headerGroups: centerHeaderGroups.length,
                visibleStart: visibleCenterRange.start,
                visibleEnd: visibleCenterRange.end,
            });
            return plan;
        },
        [centerCols, centerColumnWidthPrefix, centerHeaderGroups, headerLeafPairsMap, visibleCenterRange]
    );

    /** @param {any} [row] @param {any} [fallbackIndex] @returns {any} */
    const getRenderedRowIndex = (row, fallbackIndex = 0) => {
        if (row && typeof row.index === 'number') return row.index;
        if (row && row.original && typeof row.original.__virtualIndex === 'number') {
            return row.original.__virtualIndex;
        }
        return fallbackIndex;
    };

    const topRowCount = effectiveTopRows.length;
    const centerRowCount = effectiveCenterRows.length;
    /** @type {any} */
    const virtualBodyHeight = Math.max(
        rowVirtualizer.getTotalSize() - (pinnedVirtualIndexes.length * rowHeight),
        0
    );
    /** @type {any} */
    const centerDisplayIndexById = React.useMemo(() => {
        /** @type {any} */
        const lookup = new Map();
        effectiveCenterRows.forEach(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
            if (row && row.id) lookup.set(row.id, topRowCount + index);
        });
        return lookup;
    }, [effectiveCenterRows, topRowCount]);

    /** @type {(...args: any[]) => any} */
    const resolveVirtualRow = React.useCallback((virtualIndex) => {
        if (serverSide) {
            /** @type {any} */
            const cachedData = getRow(virtualIndex);
            if (!cachedData) return null;
            /** @type {any} */
            const rowId = isGrandTotalRow(cachedData)
                ? '__grand_total__'
                : (cachedData._path || (cachedData.id ? cachedData.id : String(virtualIndex)));
            return centerRowLookup.get(rowId) || null;
        }
        return effectiveCenterRows[virtualIndex] || null;
    }, [centerRowLookup, effectiveCenterRows, getRow, serverSide]);

    /** @type {any} */
    const rowSpanColumns = React.useMemo(() => {
        if (layoutMode !== 'tabular') return [];
        return collectRowSpanColumns([...leftCols, ...centerCols, ...rightCols]);
    }, [layoutMode, leftCols, centerCols, rightCols]);
    /** @type {any} */
    const rowSpanEnabled = rowSpanColumns.length > 0;
    /** @type {any} */
    const rowSpanRowEntries = React.useMemo(() => {
        if (!rowSpanEnabled) return [];
        return virtualRows.reduce(/** @param {any} [entries] @param {any} [virtualRow] @returns {any} */ (entries, virtualRow) => {
            if (serverSide && pinnedVirtualIndexSet.has(virtualRow.index)) return entries;
            /** @type {any} */
            const row = resolveVirtualRow(virtualRow.index);
            if (row && row.original) {
                entries.push({
                    row,
                    size: Number(virtualRow.size) > 0 ? Number(virtualRow.size) : rowHeight,
                });
            }
            return entries;
        }, []);
    }, [rowSpanEnabled, virtualRows, serverSide, pinnedVirtualIndexSet, resolveVirtualRow, rowHeight]);
    /** @type {any} */
    const rowSpanPlan = React.useMemo(
        () => buildRowSpanPlan({ rowEntries: rowSpanRowEntries, rowSpanColumns }),
        [rowSpanRowEntries, rowSpanColumns]
    );
    /** @type {(...args: any[]) => any} */
    const getCellRenderOptions = React.useCallback((column, renderOptions = {}) => {
        /** @type {any} */
        const rowSpanByColumnId = renderOptions.rowSpanByColumnId || null;
        /** @type {any} */
        const rowSpan = rowSpanByColumnId ? rowSpanByColumnId.get(column.id) : null;
        return rowSpan ? { disableSticky: true, rowSpan } : { disableSticky: true };
    }, []);

    /** @type {any} */
    const stickyTreeRow = React.useMemo(() => {
        if (viewMode !== 'tree' || treeSuppressGroupRowsSticky || virtualRows.length === 0) return null;
        /** @type {any} */
        const firstVisibleRow = virtualRows
            .map(/** @param {any} [virtualRow] @returns {any} */ (virtualRow) => resolveVirtualRow(virtualRow.index))
            .find(/** @param {any} [row] @returns {any} */ (row) => row && row.original && !row.original._isTotal);
        if (!firstVisibleRow || !firstVisibleRow.original) return null;

        /** @type {any} */
        const path = typeof firstVisibleRow.original._path === 'string' ? firstVisibleRow.original._path : '';
        /** @type {any} */
        const pathParts = path.split('|||').filter(Boolean);
        if (pathParts.length <= 1) return null;

        for (let depth = pathParts.length - 1; depth > 0; depth -= 1) {
            /** @type {any} */
            const ancestorPath = pathParts.slice(0, depth).join('|||');
            /** @type {any} */
            const ancestorRow = centerRowLookup.get(ancestorPath);
            if (ancestorRow && ancestorRow.id !== firstVisibleRow.id) {
                return ancestorRow;
            }
        }

        return null;
    }, [centerRowLookup, resolveVirtualRow, treeSuppressGroupRowsSticky, viewMode, virtualRows]);

    /** @param {any} [row] @param {any} [virtualRowIndex] @param {any} [isVirtualRow] @param {any} [renderOptions] @returns {any} */
    const renderCenterVirtualCells = (row, virtualRowIndex, isVirtualRow, renderOptions = {}) => {
        return virtualCenterCols.map(/** @param {any} [virtualCol] @returns {any} */ virtualCol => {
            const centerColumn = centerCols[virtualCol.index];
            if (!centerColumn) return null;
            return renderVirtualColumnCell(row, centerColumn, virtualRowIndex, isVirtualRow, getCellRenderOptions(centerColumn, renderOptions));
        });
    };

    /** @param {any} [rowBackground] @param {any} [skeletonStyle] @returns {any} */
    const renderSkeletonSections = (rowBackground, skeletonStyle = {}) => (
        <>
            {leftPinnedSectionStyle && (
                <div style={{...leftPinnedSectionStyle, background: rowBackground}}>
                    <SkeletonRow style={{ width: '100%', ...skeletonStyle }} rowHeight={rowHeight} />
                </div>
            )}
            <div style={{ ...centerFlexStyle, background: rowBackground }}>
                <div style={beforeSpacerStyle} />
                <SkeletonRow
                    style={{
                        width: `${visibleCenterWidth}px`,
                        minWidth: `${visibleCenterWidth}px`,
                        flexShrink: 0,
                        ...skeletonStyle
                    }}
                    rowHeight={rowHeight}
                />
                <div style={afterSpacerStyle} />
            </div>
            {rightPinnedSectionStyle && (
                <div style={{...rightPinnedSectionStyle, background: rowBackground}}>
                    <SkeletonRow style={{ width: '100%', ...skeletonStyle }} rowHeight={rowHeight} />
                </div>
            )}
        </>
    );

    /** @param {any} [row] @param {any} [virtualRowIndex] @param {any} [isVirtualRow] @param {any} [renderOptions] @returns {any} */
    const renderRowSections = (row, virtualRowIndex, isVirtualRow, renderOptions = {}) => {
        /** @type {any} */
        const rowBackground = renderOptions.rowBackground || defaultRowBg;
        const disablePinnedColumnStickiness = !!renderOptions.disablePinnedColumnStickiness;

        return (
            <>
                {leftPinnedSectionStyle && (
                    <div
                        style={disablePinnedColumnStickiness
                            ? { display: 'flex', width: leftPinnedWidth, minWidth: leftPinnedWidth, flexShrink: 0, position: 'relative', background: `var(--sp-row-current-bg, ${rowBackground})`, boxShadow: 'none' }
                            : { ...leftPinnedSectionStyle, background: `var(--sp-row-current-bg, ${rowBackground})` }
                        }
                    >
                        {leftCols.map(/** @param {any} [column] @returns {any} */ (column) =>
                            renderVirtualColumnCell(row, column, virtualRowIndex, isVirtualRow, getCellRenderOptions(column, renderOptions))
                        )}
                    </div>
                )}
                <div style={{ ...centerFlexStyle, background: `var(--sp-row-current-bg, ${rowBackground})` }}>
                    <div style={beforeSpacerStyle} />
                    {renderCenterVirtualCells(row, virtualRowIndex, isVirtualRow, renderOptions)}
                    <div style={afterSpacerStyle} />
                </div>
                {rightPinnedSectionStyle && (
                    <div
                        style={disablePinnedColumnStickiness
                            ? { display: 'flex', width: rightPinnedWidth, minWidth: rightPinnedWidth, flexShrink: 0, position: 'relative', background: `var(--sp-row-current-bg, ${rowBackground})`, boxShadow: 'none' }
                            : { ...rightPinnedSectionStyle, background: `var(--sp-row-current-bg, ${rowBackground})` }
                        }
                    >
                        {rightCols.map(/** @param {any} [column] @returns {any} */ (column) =>
                            renderVirtualColumnCell(row, column, virtualRowIndex, isVirtualRow, getCellRenderOptions(column, renderOptions))
                        )}
                    </div>
                )}
            </>
        );
    };

    /** @param {any} [rowsForPin] @param {any} [pinPosition] @returns {any} */
    const renderPinnedRowGroup = (rowsForPin, pinPosition) => {
        if (!rowsForPin || rowsForPin.length === 0) return null;
        /** @type {any} */
        const isTop = pinPosition === 'top';
        /** @type {any} */
        const stickyOffset = isTop ? (stickyHeaders ? effectiveStickyHeaderHeight : 0) : 0;
        /** @type {any} */
        const groupHeight = rowsForPin.length * rowHeight;

        return (
            <div
                style={{
                    position: 'sticky',
                    top: isTop ? stickyOffset : undefined,
                    bottom: isTop ? undefined : 0,
                    zIndex: 80,
                    display: 'flex',
                    width: `${totalLayoutWidth}px`,
                    minWidth: `${totalLayoutWidth}px`,
                    height: `${groupHeight}px`,
                    flexDirection: 'column'
                }}
            >
                {rowsForPin.map(/** @param {any} [row] @param {any} [index] @returns {any} */ (row, index) => {
                    /** @type {any} */
                    const isEdgeRow = isTop ? index === rowsForPin.length - 1 : index === 0;
                    /** @type {any} */
                    const displayRowIndex = isTop ? index : topRowCount + centerRowCount + index;
                    /** @type {any} */
                    const rowBackground = getRowLevelBackground(row);
                    /** @type {any} */
                    const isTotalPinned = Boolean(row.original && row.original._isTotal);
                    /** @type {any} */
                    const rowBorderBottom = isTotalPinned && theme.totalBorder
                        ? (String(theme.totalBorder).includes(' ') ? theme.totalBorder : `1px solid ${theme.totalBorder}`)
                        : `1px solid ${gridBorders.rowBorderColor}`;
                    /** @type {any} */
                    const extraShadow = isEdgeRow
                        ? (isTop ? `0 2px 4px -2px ${theme.border}80` : `0 -4px 6px -1px rgba(15,23,42,0.06)`)
                        : 'none';
                    return (
                        <div
                            key={`${pinPosition}_${row.id}`}
                            role="row"
                            {...getRowDataAttributes(row)}
                            style={{
                                ...baseRowStyle,
                                position: 'relative',
                                minWidth: `${totalLayoutWidth}px`,
                                height: rowHeight,
                                '--sp-row-current-bg': rowBackground,
                                background: 'var(--sp-row-current-bg)',
                                borderBottom: rowBorderBottom,
                                boxShadow: extraShadow,
                            }}
                        >
                            {leftCols.map(/** @param {any} [column] @returns {any} */ (column) =>
                                renderVirtualColumnCell(row, column, displayRowIndex, false)
                            )}
                            <div style={beforeSpacerStyle} />
                            {renderCenterVirtualCells(row, displayRowIndex, false)}
                            <div style={afterSpacerStyle} />
                            {rightCols.map(/** @param {any} [column] @returns {any} */ (column) =>
                                renderVirtualColumnCell(row, column, displayRowIndex, false)
                            )}
                        </div>
                    );
                })}
            </div>
        );
    };

    return (
        <div style={styles.main}>
            {isRequestPending && (
                <div
                    aria-hidden="true"
                    data-pivot-loading-indicator="global"
                    style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        right: 0,
                        height: '3px',
                        zIndex: 120,
                        pointerEvents: 'none',
                        background: 'var(--pivot-loading-progress-gradient, linear-gradient(90deg, rgba(75,139,245,0) 0%, rgba(75,139,245,0.9) 45%, rgba(75,139,245,0) 100%))',
                        backgroundSize: '220% 100%',
                        animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                    }}
                />
            )}
            <div
                ref={parentRef}
                data-pivot-scroll-container="true"
                style={{...styles.scrollContainer, overflow: 'auto'}}
                onKeyDown={handleKeyDown}
                tabIndex={0}
                role="grid"
                aria-rowcount={rows.length}
                aria-colcount={visibleLeafColumns.length}
            >
                 <div style={{
                     width: `${totalLayoutWidth}px`,
                     minWidth:'100%',
                     height: `${virtualBodyHeight + effectiveStickyHeaderHeight + (effectiveTopRows.length + effectiveBottomRows.length) * rowHeight}px`,
                     position: 'relative',
                     borderRadius: `0 0 ${theme.radius || '16px'} ${theme.radius || '16px'}`,
                     overflow: 'visible'
                 }}>
                     {/* Sticky Header */}
                     <div
                         ref={stickyHeaderRef}
                         style={{
                             ...styles.headerSticky,
                             width: 'fit-content',
                             display: 'flex',
                             position: stickyHeaders ? 'sticky' : 'relative',
                             top: stickyHeaders ? 0 : undefined
                         }}
                         role="rowgroup"
                     >
                         {/* Left Section */}
                         <div style={{position: 'sticky', left: 0, zIndex: 4, background: theme.headerBg}}>
                             {leftHeaderGroups.map(/** @param {any} [group] @param {any} [level] @returns {any} */ (group, level) => (
                                 <div key={group.id} data-header-level={level} style={headerRowStyle}>
                                     {group.headers.map(/** @param {any} [header] @returns {any} */ (header) => renderHeaderCell(
                                         header,
                                         level,
                                         'left',
                                         null,
                                         true,
                                         {
                                             coveredByPlaceholder: Boolean(leftHeaderSpanMeta.coveredByLevel[level]?.has(header.column?.id)),
                                             spanHeight: leftHeaderSpanMeta.spanHeightsByHeaderId.get(header.id),
                                         }
                                     ))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{...headerRowStyle, background: theme.background}}>
                                     {leftCols.map(/** @param {any} [column] @param {any} [idx] @returns {any} */ (column, idx) => (
                                         <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: effectiveHeaderHeight, padding: '2px 4px', borderRight: idx === leftCols.length - 1 ? `1px solid ${gridBorders.pinnedColumnBorderColor}` : 'none'}}>
                                             {column.id !== 'hierarchy' && (
                                                 <input
                                                     style={{width: '100%', fontSize: '11px', padding: '2px 4px', border: `1px solid ${theme.border}`, borderRadius: '2px'}}
                                                     placeholder="Filter..."
                                                     value={(filters[column.id] && filters[column.id].conditions && filters[column.id].conditions[0]) ? filters[column.id].conditions[0].value : ''}
                                                     onChange={e => {
                                                         const val = e.target.value;
                                                         handleHeaderFilter(column.id, val
                                                             ? { operator: 'AND', conditions: [{ type: 'contains', value: val, caseSensitive: false }] }
                                                             : null);
                                                     }}
                                                     onClick={(e) => e.stopPropagation()}
                                                 />
                                             )}
                                         </div>
                                     ))}
                                 </div>
                             )}
                         </div>

                         {/* Center Section */}
                         <div style={{position: 'relative', zIndex: 1}}>
                              {centerHeaderRenderPlan.map(/** @param {any} p0 @param {any} [level] @returns {any} */ ({ groupId, visibleHeaders }, level) => {
                                  return (
                                      <div key={groupId} data-header-level={level} style={headerRowStyle}>
                                          <div style={beforeSpacerStyle} />
                                          {visibleHeaders.map(/** @param {any} p0 @returns {any} */ ({ header, visWidth }) =>
                                              renderHeaderCell(
                                                  header,
                                                  level,
                                                  'center',
                                                  visWidth,
                                                  false,
                                                  {
                                                      coveredByPlaceholder: Boolean(centerHeaderSpanMeta.coveredByLevel[level]?.has(header.column?.id)),
                                                      spanHeight: centerHeaderSpanMeta.spanHeightsByHeaderId.get(header.id),
                                                  }
                                              )
                                          )}
                                          <div style={afterSpacerStyle} />
                                     </div>
                                 );
                              })}
                             {showColumnLoadingSkeletons && (
                                 <div
                                     aria-hidden="true"
                                     style={{
                                         position: 'absolute',
                                         top: 0,
                                         right: 0,
                                         height: effectiveHeaderHeight,
                                         display: 'flex',
                                         alignItems: 'center',
                                         justifyContent: 'flex-end',
                                         gap: '8px',
                                         padding: '0 8px',
                                         pointerEvents: 'none',
                                         zIndex: 9
                                     }}
                                 >
                                     {Array.from({ length: pendingColumnSkeletonCount }).map((_, index) => (
                                         <div
                                             key={`col-header-skeleton-${index}`}
                                             style={{
                                                 width: `${columnSkeletonWidth}px`,
                                                 height: '60%',
                                                 borderRadius: '8px',
                                                 background: 'var(--pivot-loading-header-gradient, linear-gradient(90deg, rgba(233,243,255,0.92) 0%, rgba(193,220,255,0.98) 48%, rgba(233,243,255,0.92) 100%))',
                                                 backgroundSize: '220% 100%',
                                                 border: '1px solid var(--pivot-loading-border, rgba(153, 187, 238, 0.5))',
                                                 animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                                             }}
                                         />
                                     ))}
                                 </div>
                             )}
                             {showFloatingFilters && (
                                 <div style={{...headerRowStyle, background: theme.background}}>
                                     <div style={beforeSpacerStyle} />
                                     {virtualCenterCols.map(/** @param {any} [virtualCol] @returns {any} */ virtualCol => {
                                         const column = centerCols[virtualCol.index];
                                         if (!column) return null;
                                         return (
                                             <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: effectiveHeaderHeight, padding: '2px 4px'}}>
                                                 {column.id !== 'hierarchy' && (
                                                     <input
                                                         style={{width: '100%', fontSize: '11px', padding: '2px 4px', border: `1px solid ${theme.border}`, borderRadius: '2px'}}
                                                         placeholder="Filter..."
                                                         value={(filters[column.id] && filters[column.id].conditions && filters[column.id].conditions[0]) ? filters[column.id].conditions[0].value : ''}
                                                         onChange={e => {
                                                             const val = e.target.value;
                                                             handleHeaderFilter(column.id, val
                                                                 ? { operator: 'AND', conditions: [{ type: 'contains', value: val, caseSensitive: false }] }
                                                                 : null);
                                                         }}
                                                         onClick={(e) => e.stopPropagation()}
                                                     />
                                                 )}
                                             </div>
                                         );
                                     })}
                                     <div style={afterSpacerStyle} />
                                 </div>
                             )}
                         </div>

                         {/* Right Section */}
                         <div style={{position: 'sticky', right: 0, zIndex: 4, background: theme.headerBg}}>
                             {rightHeaderGroups.map(/** @param {any} [group] @param {any} [level] @returns {any} */ (group, level) => (
                                 <div key={group.id} data-header-level={level} style={headerRowStyle}>
                                     {group.headers.map(/** @param {any} [header] @returns {any} */ (header) => renderHeaderCell(
                                         header,
                                         level,
                                         'right',
                                         null,
                                         true,
                                         {
                                             coveredByPlaceholder: Boolean(rightHeaderSpanMeta.coveredByLevel[level]?.has(header.column?.id)),
                                             spanHeight: rightHeaderSpanMeta.spanHeightsByHeaderId.get(header.id),
                                         }
                                     ))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{...headerRowStyle, background: theme.background}}>
                                     {rightCols.map(/** @param {any} [column] @param {any} [idx] @returns {any} */ (column, idx) => (
                                         <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: effectiveHeaderHeight, padding: '2px 4px', borderLeft: idx === 0 ? `1px solid ${gridBorders.pinnedColumnBorderColor}` : 'none'}}>
                                             {column.id !== 'hierarchy' && (
                                                 <input
                                                     style={{width: '100%', fontSize: '11px', padding: '2px 4px', border: `1px solid ${theme.border}`, borderRadius: '2px'}}
                                                     placeholder="Filter..."
                                                     value={(filters[column.id] && filters[column.id].conditions && filters[column.id].conditions[0]) ? filters[column.id].conditions[0].value : ''}
                                                     onChange={e => {
                                                         const val = e.target.value;
                                                         handleHeaderFilter(column.id, val
                                                             ? { operator: 'AND', conditions: [{ type: 'contains', value: val, caseSensitive: false }] }
                                                             : null);
                                                     }}
                                                     onClick={(e) => e.stopPropagation()}
                                                 />
                                             )}
                                         </div>
                                     ))}
                                 </div>
                             )}
                         </div>
                     </div>

                     {/* Top Pinned Rows */}
                    {renderPinnedRowGroup(effectiveTopRows, 'top')}
                    {stickyTreeRow && (() => {
                        /** @type {any} */
                        const stickyRowBg = getRowLevelBackground(stickyTreeRow);
                        return (
                        <div
                            role="row"
                            {...getRowDataAttributes(stickyTreeRow)}
                            style={{
                                position: 'sticky',
                                top: `${(stickyHeaders ? effectiveStickyHeaderHeight : 0) + (effectiveTopRows.length * rowHeight)}px`,
                                zIndex: 79,
                                display: 'flex',
                                width: `${totalLayoutWidth}px`,
                                minWidth: `${totalLayoutWidth}px`,
                                height: `${rowHeight}px`,
                                '--sp-row-current-bg': stickyRowBg,
                                background: 'var(--sp-row-current-bg)',
                                borderBottom: `1px solid ${gridBorders.rowBorderColor}`,
                                boxShadow: `0 2px 4px -2px ${theme.border}80`,
                            }}
                        >
                            {renderRowSections(
                                stickyTreeRow,
                                centerDisplayIndexById.get(stickyTreeRow.id) ?? getRenderedRowIndex(stickyTreeRow, 0),
                                false,
                                { rowBackground: stickyRowBg }
                            )}
                        </div>
                        );
                    })()}

                     {showColumnLoadingSkeletons && (
                         <div
                             aria-hidden="true"
                                style={{
                                    position: 'absolute',
                                    top: `${effectiveBodyRowsTopOffset}px`,
                                    left: `${leftPinnedWidth}px`,
                                    right: `${rightPinnedWidth}px`,
                                    height: `${Math.max(virtualBodyHeight, rowHeight * 4)}px`,
                                    display: 'flex',
                                    gap: '8px',
                                    padding: '0 8px',
                                    justifyContent: 'flex-end',
                                    overflow: 'hidden',
                                    pointerEvents: 'none',
                                    zIndex: 3
                             }}
                         >
                             {Array.from({ length: pendingColumnSkeletonCount }).map((_, index) => (
                                 <div
                                     key={`col-body-skeleton-${index}`}
                                     style={{
                                         width: `${columnSkeletonWidth}px`,
                                         height: '100%',
                                         borderRadius: '8px',
                                         background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%))',
                                         backgroundSize: '220% 100%',
                                         border: '1px solid var(--pivot-loading-border, rgba(153, 187, 238, 0.5))',
                                         animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                                     }}
                                 />
                             ))}
                         </div>
                     )}

                     {/* Center Virtualized Rows */}
                     {virtualRows.map(/** @param {any} [virtualRow] @returns {any} */ virtualRow => {
                          const topOffset = effectiveBodyRowsTopOffset;
                          /** @type {any} */
                          const adjustedTop = virtualRow.start - (getPinnedRowsBefore(virtualRow.index) * rowHeight) + topOffset;

                          /** @type {any} */
                          let row;

                          if (serverSide) {
                              if (pinnedVirtualIndexSet.has(virtualRow.index)) {
                                  return null;
                              }

                              // 1. Fetch Data Directly from Cache (Source of Truth)
                              /** @type {any} */
                              const cachedData = getRow(virtualRow.index);

                              if (!cachedData) {
                                  // Data not loaded yet -> Skeleton
                                 return (
                                     <div
                                        key={`skeleton_${virtualRow.index}`}
                                        style={{
                                            ...skeletonRowStyle,
                                            height: virtualRow.size,
                                            top: `${adjustedTop}px`,
                                            background: defaultRowBg,
                                        }}>
                                            {renderSkeletonSections(defaultRowBg)}
                                        </div>                                 );
                             }

                             if (
                                 serverSidePinsGrandTotal &&
                                 isGrandTotalRow(cachedData)
                             ) {
                                 return null;
                             }

                             // 2. Resolve Row Object via ID (Decoupled from Index)
                             // We reconstruct the ID exactly as getRowId does, but using the global index directly.
                             // Global Index = virtualRow.index
                             /** @type {any} */
                             let rowId;
                             if (isGrandTotalRow(cachedData)) {
                                 rowId = '__grand_total__';
                             } else {
                                 rowId = cachedData._path || (cachedData.id ? cachedData.id : String(virtualRow.index));
                              }

                              row = centerRowLookup.get(rowId);
                              if (!row && pinnedRowIdSet.has(rowId)) {
                                  return null;
                              }

                              // 3. Synchronization Check
                              // If table hasn't updated yet, table.getRow might return old data or undefined.
                              // We verify the row's data matches our cache.
                             /** @type {any} */
                             const cachedPath = isGrandTotalRow(cachedData) ? '__grand_total__' : (cachedData._path || rowId);
                             /** @type {any} */
                             const rowPath = row && row.original
                                 ? (isGrandTotalRow(row.original) ? '__grand_total__' : (row.original._path || row.id))
                                 : null;
                             if (row && rowPath !== cachedPath) {
                                 row = undefined; // Stale row object
                             }
                          } else {
                              // Client-side mode: simple index access
                              row = effectiveCenterRows[virtualRow.index];
                          }

                          // 4. Fallback: If row object is missing (even if we had cache), show skeleton
                          if (!row || !row.original) {
                               /** @type {any} */
                               const skelBg = (row && row.original && row.original._isTotal) ? totalRowBg : defaultRowBg;
                               return (
                                 <div
                                    key={`skeleton_wait_${virtualRow.index}`}
                                    style={{
                                     ...skeletonRowStyle,
                                     height: virtualRow.size,
                                     top: `${adjustedTop}px`,
                                     background: skelBg,
                                 }}>
                                     {renderSkeletonSections(skelBg)}
                                 </div>
                             );
                         }

                         // 5. Feature: Hide Totals if requested (Server Side only workaround)
                         if (serverSide && !showColTotals && isGrandTotalRow(row.original)) {
                             return null;
                         }

                          /** @type {any} */
                          const pendingTransitionMode = pendingRowTransitions.get(row.id);
                          const showRowTransitionLoader = !!pendingTransitionMode;

                          // Stable key: use row path/id for loaded rows so expand/collapse
                          // does not remount rows that merely shifted index (AG Grid getRowId pattern).
                          /** @type {any} */
                          const stableRowKey = serverSide
                              ? (row.id || String(virtualRow.index))
                              : String(virtualRow.index);
                          /** @type {any} */
                          const displayRowIndex = centerDisplayIndexById.get(row.id) ?? (topRowCount + virtualRow.index);

                          /** @type {any} */
                          const rowBg = getRowLevelBackground(row);
                          /** @type {any} */
                          const isTotalVirtualRow = Boolean(row.original && row.original._isTotal);
                          /** @type {any} */
                          const rowSpanByColumnId = rowSpanEnabled && row.id != null
                              ? (rowSpanPlan.get(String(row.id)) || null)
                              : null;

                          return (
                              <React.Fragment key={stableRowKey}>
                                  <div
                                     role="row"
                                     aria-rowindex={virtualRow.index + 1}
                                     {...getRowDataAttributes(row)}
                                     style={{
                                      ...baseRowStyle,
                                      height: virtualRow.size,
                                      top: `${adjustedTop}px`,
                                      '--sp-row-current-bg': rowBg,
                                      background: 'var(--sp-row-current-bg)',
                                      ...(isTotalVirtualRow && theme.totalBorder ? {
                                          borderBottom: String(theme.totalBorder).includes(' ') ? theme.totalBorder : `1px solid ${theme.totalBorder}`,
                                      } : {}),
                                      transition: rowVirtualizer.isScrolling ? 'none' : 'background-color 0.2s'
                                   }}>
                                      {renderRowSections(row, displayRowIndex, true, {
                                          rowBackground: rowBg,
                                          rowSpanByColumnId,
                                      })}
                                  </div>
                                  {detailMode === 'inline' && detailState && detailState.anchorRowId === row.id && (
                                      <React.Suspense fallback={null}>
                                      <LazyInlineDetailPanel
                                          detailState={detailState}
                                          onClose={onDetailClose}
                                          onPageChange={onDetailPageChange}
                                          onSort={onDetailSort}
                                          onFilter={onDetailFilter}
                                          theme={theme}
                                          width={totalLayoutWidth}
                                          top={adjustedTop + virtualRow.size + 4}
                                          height={detailInlineHeight}
                                      />
                                      </React.Suspense>
                                  )}
                                  {showRowTransitionLoader && (
                                      <div
                                         role="row"
                                         aria-hidden="true"
                                         style={{
                                          ...transitionLoaderStyle,
                                          top: `${adjustedTop + virtualRow.size}px`,
                                      }}>
                                          {renderSkeletonSections(
                                              'var(--pivot-loading-row-gradient, linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%))',
                                              { opacity: 0.45 }
                                          )}
                                          <div
                                             style={{
                                              position: 'absolute',
                                              zIndex: 2,
                                              paddingLeft: `${((row.original && typeof row.original.depth === 'number' ? row.original.depth : row.depth || 0) + 1) * (Number(theme.treeIndent) || 24) + 8}px`,
                                              fontSize: '12px',
                                              color: theme.textSec,
                                              display: 'flex',
                                              alignItems: 'center',
                                              gap: '8px',
                                              fontWeight: 500
                                          }}
                                          >
                                              <span
                                                 aria-hidden="true"
                                                 style={{
                                                  width: '11px',
                                                  height: '11px',
                                                  border: `2px solid ${theme.primary}`,
                                                  borderTopColor: 'transparent',
                                                  borderRadius: '50%',
                                                  animation: 'pivot-spinner-rotate 0.75s linear infinite'
                                              }}
                                              />
                                              {pendingTransitionMode === 'collapse' ? 'Collapsing...' : 'Loading children...'}
                                          </div>
                                      </div>
                                  )}
                              </React.Fragment>
                          )
                      })}

                     {/* Spacer for all bottom-pinned rows.
                          Virtual rows use position:absolute (out of flow), so without this spacer
                          bottom sticky rows never reach their sticky activation point. */}
                     {effectiveBottomRows.length > 0 && (
                         <div style={{ height: `${virtualBodyHeight}px`, flexShrink: 0 }} />
                     )}
                    {/* Bottom Pinned Rows */}
                    {renderPinnedRowGroup(effectiveBottomRows, 'bottom')}
                 </div>
            </div>
            {!immersiveMode && (
                <StatusBar
                    statusModel={statusModel || {
                        selection: { selectedCells },
                        data: {
                            rowCount,
                            visibleRowsCount: rows.length,
                            totalCenterColumns,
                            columnAdvisory,
                        },
                        runtime: {
                            loading: isRequestPending,
                        },
                    }}
                    statusActions={statusActions}
                    theme={theme}
                    numberGroupSeparator={numberGroupSeparator}
                />
            )}
        </div>
    );
});


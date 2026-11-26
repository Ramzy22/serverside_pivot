import React from 'react';
import ShimmerFallback from '../ShimmerFallback';
import SkeletonRow from '../SkeletonRow';
import StatusBar from './StatusBar';
import { usePivotTheme } from '../../contexts/PivotThemeContext';
import { usePivotConfig } from '../../contexts/PivotConfigContext';
import { buildRowSpanPlan, collectRowSpanColumns } from '../../utils/rowSpanning';
import { getPivotPerformanceNow, recordPivotMeasure } from '../../utils/pivotProfiler';
import { usePivotRenderCounter } from '../../hooks/usePivotRenderCounter';
import { usePrefersReducedMotion } from '../../hooks/usePrefersReducedMotion';
import { isGrandTotalRow } from '../../hooks/usePivotNormalization';
import {
    MAX_FLIP_DISTANCE_PX,
    MAX_SHEET_TRAVEL_PX,
    ROW_LOADER_BAND_HEIGHT_PX,
    ROW_MOTION_EASING,
    SKELETON_FILL_DURATION_MS,
    buildExpandRevealPlan,
    buildSkeletonFillPlan,
    computeBottomPinnedLayoutTop,
    computeSlideDurationMs,
} from '../../utils/rowMotion';
import { resolveDepthToken, resolveGridLineBorders, withAlpha } from '../../utils/styles';

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
            // When serverSidePinsGrandTotal is active, effectiveRowCount and normalizedData
            // already exclude Grand Total from the virtualizer; never let a stale __virtualIndex
            // on Grand Total double-subtract virtualBodyHeight or hide a real data row at that slot.
            if (serverSidePinsGrandTotal && isGrandTotalRow(row && row.original)) return;
            /** @type {any} */
            const virtualIndex = row && row.original && typeof row.original.__virtualIndex === 'number'
                ? row.original.__virtualIndex
                : null;
            if (virtualIndex !== null) indexes.add(virtualIndex);
        });
        return Array.from(indexes).sort((a, b) => a - b);
    }, [serverSide, serverSidePinsGrandTotal, effectiveTopRows, effectiveBottomRows]);
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

    const prefersReducedMotion = usePrefersReducedMotion();
    const rowNodeByKeyRef = React.useRef(/** @type {Map<string, HTMLElement>} */ (new Map()));
    /** @type {(rowKey: string, node: HTMLElement | null) => void} */
    const registerRowNode = React.useCallback((rowKey, node) => {
        if (node) {
            rowNodeByKeyRef.current.set(rowKey, node);
        } else {
            rowNodeByKeyRef.current.delete(rowKey);
        }
    }, []);
    const wasScrollingRef = React.useRef(false);
    const hasMountedCenterRowsRef = React.useRef(false);
    const [exitingRowSnapshots, setExitingRowSnapshots] = React.useState(/** @type {any[]} */ ([]));
    const [collapseHoldHeight, setCollapseHoldHeight] = React.useState(0);
    const prevCommittedRowsRef = React.useRef(/** @type {any[]} */ ([]));
    const prevTopByKeyRef = React.useRef(/** @type {Map<string, number>} */ (new Map()));
    const prevVisualBodyHeightRef = React.useRef(0);
    const prevScrollTopRef = React.useRef(0);
    const prevModelRowCountRef = React.useRef(0);
    const prevColumnSignatureRef = React.useRef('');
    const exitUnderlayTimerRef = React.useRef(/** @type {any} */ (null));
    const prevExpandedByPathRef = React.useRef(/** @type {Map<string, boolean>} */ (new Map()));
    const expandedAtByPathRef = React.useRef(/** @type {Map<string, number>} */ (new Map()));
    const collapsedAtByPathRef = React.useRef(/** @type {Map<string, number>} */ (new Map()));
    const expandAllAtRef = React.useRef(0);
    const prevSkeletonIndicesRef = React.useRef(/** @type {Set<number>} */ (new Set()));
    const activeFlipAnimByKeyRef = React.useRef(/** @type {Map<string, { anim: any, targetTop: number, el: HTMLElement | null, restoreZIndex: boolean }>} */ (new Map()));
    const activeRevealAnimByKeyRef = React.useRef(/** @type {Map<string, { anim: any, targetEl: HTMLElement | null }>} */ (new Map()));
    const bottomPinnedGroupRef = React.useRef(/** @type {HTMLDivElement | null} */ (null));
    const activeBottomFlipAnimRef = React.useRef(/** @type {any} */ (null));
    const prevBottomPinnedRelTopRef = React.useRef(/** @type {number | null} */ (null));

    const cancelTrackedMotionAnimations = React.useCallback(() => {
        activeFlipAnimByKeyRef.current.forEach((record) => {
            if (record && record.anim && typeof record.anim.cancel === 'function') {
                record.anim.cancel();
            }
            if (record && record.el && record.el.style) {
                record.el.style.willChange = '';
                if (record.restoreZIndex) record.el.style.zIndex = '4';
            }
        });
        activeFlipAnimByKeyRef.current.clear();
        activeRevealAnimByKeyRef.current.forEach((record) => {
            if (record && record.anim && typeof record.anim.cancel === 'function') {
                record.anim.cancel();
            }
            if (record && record.targetEl && record.targetEl.style) {
                record.targetEl.style.willChange = '';
            }
        });
        activeRevealAnimByKeyRef.current.clear();
        if (activeBottomFlipAnimRef.current && typeof activeBottomFlipAnimRef.current.cancel === 'function') {
            activeBottomFlipAnimRef.current.cancel();
            activeBottomFlipAnimRef.current = null;
        }
        if (bottomPinnedGroupRef.current && bottomPinnedGroupRef.current.style) {
            bottomPinnedGroupRef.current.style.willChange = '';
        }
    }, []);

    React.useEffect(() => () => {
        if (exitUnderlayTimerRef.current) {
            clearTimeout(exitUnderlayTimerRef.current);
            exitUnderlayTimerRef.current = null;
        }
        cancelTrackedMotionAnimations();
    }, [cancelTrackedMotionAnimations]);

    // Scroll-aware elevation attributes updated directly on the scroll container DOM node
    // (zero React re-renders per scroll frame) to drive sticky header & pinned-edge shadows.
    React.useEffect(() => {
        const el = parentRef && parentRef.current;
        if (!el || typeof el.addEventListener !== 'function') return undefined;
        const syncScrollAttrs = () => {
            const scrolledY = el.scrollTop > 2 ? 'true' : 'false';
            const scrolledX = el.scrollLeft > 2 ? 'true' : 'false';
            if (el.dataset.scrolledY !== scrolledY) el.dataset.scrolledY = scrolledY;
            if (el.dataset.scrolledX !== scrolledX) el.dataset.scrolledX = scrolledX;
        };
        syncScrollAttrs();
        el.addEventListener('scroll', syncScrollAttrs, { passive: true });
        return () => el.removeEventListener('scroll', syncScrollAttrs);
    }, [parentRef]);

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
            // Use offsetHeight (unscaled CSS pixels) so ancestor CSS zoom does not inflate header offsets.
            /** @type {any} */
            const totalHeight = el.offsetHeight || Math.ceil(el.getBoundingClientRect ? el.getBoundingClientRect().height : 0);
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
                    const h = rowEl.offsetHeight || Math.ceil(rowEl.getBoundingClientRect ? rowEl.getBoundingClientRect().height : 0);
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
        top: 0,
        display: 'flex',
        alignItems: 'center',
        zIndex: 4,
        pointerEvents: 'none',
    }), [baseRowStyle]);

    /** @type {any} */
    const transitionLoaderStyle = React.useMemo(() => ({
        ...styles.row,
        pointerEvents: 'none',
        height: ROW_LOADER_BAND_HEIGHT_PX,
        width: `${totalLayoutWidth}px`,
        position: 'absolute',
        top: 0,
        left: 0,
        background: theme.surfaceMuted || theme.headerSubtleBg || 'var(--sp-surface-muted, #F8FAFC)',
        borderBottom: `1px solid ${theme.border}`,
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        overflow: 'hidden',
        zIndex: 10,
        boxShadow: `0 4px 12px -6px ${withAlpha(theme.border, 0.5, 'rgba(15,23,42,0.12)')}`,
        animation: 'pivot-band-enter 160ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both',
    }), [styles.row, totalLayoutWidth, theme.surfaceMuted, theme.headerSubtleBg, theme.border]);

    const isScrollingNowForMotion = Boolean(rowVirtualizer && rowVirtualizer.isScrolling);
    const disableRowMotion = prefersReducedMotion || !hasMountedCenterRowsRef.current || isScrollingNowForMotion || wasScrollingRef.current;

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
    }), [effectiveHeaderHeight, theme.headerWrap]);

    // Header cells draw their own separators so a tier boundary stops at a spanning cell.
    const floatingFilterRowStyle = React.useMemo(() => ({
        ...headerRowStyle,
        background: theme.background,
        borderBottom: `1px solid ${gridBorders.headerBorderBottomColor}`,
    }), [headerRowStyle, theme.background, gridBorders.headerBorderBottomColor]);

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
    const columnStructureSignature = React.useMemo(
        () => visibleLeafColumns.map(/** @param {any} [col] @returns {string} */ (col) => (col ? String(col.id) : '')).join('\u001f'),
        [visibleLeafColumns]
    );
    // Unified visual layout adjustments (bounded to virtualRows):
    // - Optimistic collapse: collapsing needs no new server data, so descendants of a
    //   row that just flipped expanded -> collapsed exit immediately and rows below
    //   slide up without waiting for the round-trip.
    // - Pending expand: a row whose children are still loading opens a band-height
    //   gap beneath it. Siblings below glide down into the gap (FLIP) and the
    //   "Loading breakdown" band occupies it, so no loaded row is ever covered.
    /** @type {{ hiddenKeys: Set<string>, topOffsetDeltaByIndex: Map<number, number>, totalCollapseShift: number, totalGapShift: number, bandGapKeys: Set<string> }} */
    const rowMotionLayoutPlan = (() => {
        const hiddenKeys = new Set();
        const topOffsetDeltaByIndex = new Map();
        const bandGapKeys = new Set();
        if (rowSpanEnabled || !hasMountedCenterRowsRef.current) {
            return { hiddenKeys, topOffsetDeltaByIndex, totalCollapseShift: 0, totalGapShift: 0, bandGapKeys };
        }
        const now = Date.now();
        /** @param {string} ancestorKey @param {any} ancestorRow @returns {boolean} */
        const isRecentlyCollapsed = (ancestorKey, ancestorRow) => {
            if (!serverSide) return false;
            const isExpanded = typeof ancestorRow.getIsExpanded === 'function'
                ? Boolean(ancestorRow.getIsExpanded())
                : Boolean(ancestorRow.original && ancestorRow.original._is_expanded);
            if (isExpanded) return false;
            if (prevExpandedByPathRef.current.get(ancestorKey) === true) return true;
            const collapsedAt = collapsedAtByPathRef.current.get(ancestorKey) || 0;
            return Boolean(collapsedAt > 0 && now - collapsedAt < (isRequestPending ? 4000 : 1200));
        };
        const hasPendingTransitions = Boolean(
            pendingRowTransitions && typeof pendingRowTransitions.get === 'function' && pendingRowTransitions.size > 0
        );
        let collapseShift = 0;
        let gapShift = 0;
        for (let i = 0; i < virtualRows.length; i += 1) {
            const virtualRow = virtualRows[i];
            if (!virtualRow) continue;
            if (collapseShift !== 0 || gapShift !== 0) {
                topOffsetDeltaByIndex.set(virtualRow.index, gapShift - collapseShift);
            }
            if (serverSide && pinnedVirtualIndexSet.has(virtualRow.index)) continue;
            const row = resolveVirtualRow(virtualRow.index);
            if (!row || !row.original) continue;
            const stableRowKey = row.id ? String(row.id) : String(virtualRow.index);
            const path = row.original._path ? String(row.original._path) : stableRowKey;
            let hidden = false;
            if (serverSide && path.includes('|||')) {
                const parts = path.split('|||');
                for (let depth = parts.length - 1; depth >= 1; depth -= 1) {
                    const ancestorKey = parts.slice(0, depth).join('|||');
                    const ancestorRow = centerRowLookup.get(ancestorKey);
                    if (ancestorRow && isRecentlyCollapsed(ancestorKey, ancestorRow)) {
                        hiddenKeys.add(stableRowKey);
                        collapseShift += Number(virtualRow.size) > 0 ? Number(virtualRow.size) : rowHeight;
                        hidden = true;
                        break;
                    }
                }
            }
            if (!hidden && hasPendingTransitions && pendingRowTransitions.get(path) === 'band') {
                bandGapKeys.add(stableRowKey);
                gapShift += ROW_LOADER_BAND_HEIGHT_PX;
            }
        }
        return {
            hiddenKeys,
            topOffsetDeltaByIndex,
            totalCollapseShift: collapseShift,
            totalGapShift: gapShift,
            bandGapKeys,
        };
    })();

    const effectiveVisualBodyHeight = Math.max(
        0,
        virtualBodyHeight - rowMotionLayoutPlan.totalCollapseShift + rowMotionLayoutPlan.totalGapShift
    );
    const effectiveContainerBodyHeight = collapseHoldHeight > effectiveVisualBodyHeight
        ? collapseHoldHeight
        : effectiveVisualBodyHeight;

    /** @type {any[]} */
    const committedRowSnapshots = (() => {
        /** @type {any[]} */
        const snapshots = [];
        const topRowCountLocal = effectiveTopRows.length;
        for (let i = 0; i < virtualRows.length; i += 1) {
            const virtualRow = virtualRows[i];
            if (!virtualRow) continue;
            if (serverSide && pinnedVirtualIndexSet.has(virtualRow.index)) continue;
            const row = resolveVirtualRow(virtualRow.index);
            if (!row || !row.original) continue;
            if (!serverSide && pinnedRowIdSet.has(row.id)) continue;
            if (serverSide && !showColTotals && isGrandTotalRow(row.original)) continue;
            const stableRowKey = row.id ? String(row.id) : String(virtualRow.index);
            if (rowMotionLayoutPlan.hiddenKeys.has(stableRowKey)) continue;
            const rowPath = (row.original && row.original._path) ? String(row.original._path) : stableRowKey;
            const offsetDelta = rowMotionLayoutPlan.topOffsetDeltaByIndex.get(virtualRow.index) || 0;
            const adjustedTop = virtualRow.start - (getPinnedRowsBefore(virtualRow.index) * rowHeight) + effectiveBodyRowsTopOffset + offsetDelta;
            const displayRowIndex = centerDisplayIndexById.get(row.id) ?? (topRowCountLocal + virtualRow.index);
            const rowBg = getRowLevelBackground(row);
            const rowSpanByColumnId = rowSpanEnabled && row.id != null
                ? (rowSpanPlan.get(String(row.id)) || null)
                : null;
            const rowDepthValue = Number((row.original && row.original.depth !== undefined) ? row.original.depth : (row.depth || 0));
            const isExpanded = typeof row.getIsExpanded === 'function'
                ? Boolean(row.getIsExpanded())
                : Boolean(row.original && row.original._is_expanded);
            snapshots.push({
                rowKey: stableRowKey,
                rowPath,
                row,
                virtualIndex: virtualRow.index,
                top: adjustedTop,
                size: virtualRow.size,
                displayRowIndex,
                rowBg,
                rowSpanByColumnId,
                depth: rowDepthValue,
                isExpanded,
            });
        }
        return snapshots;
    })();

    // Pre-paint motion controller (useLayoutEffect):
    // - Pure render (zero ref mutations during render)
    // - Captures genuinely collapsing child rows & bottom-fold pushed rows into the exit underlay
    // - Holds container height during collapse underlay to avoid layout-thrash height transitions
    // - Drives WAAPI FLIP for surviving rows, expand-gated reveals for newly expanded children,
    //   and crisp 100ms opacity crossfades for skeleton/stale replacements
    React.useLayoutEffect(() => {
        const now = Date.now();
        const isScrollingNow = Boolean(rowVirtualizer && rowVirtualizer.isScrolling);
        const currentScrollTop = parentRef && parentRef.current ? parentRef.current.scrollTop : 0;
        const currentModelCount = serverSide
            ? Number(rowCount || effectiveCenterRows.length || 0)
            : effectiveCenterRows.length;
        const scrollDelta = Math.abs(currentScrollTop - prevScrollTopRef.current);
        const columnsChanged = prevColumnSignatureRef.current !== '' && prevColumnSignatureRef.current !== columnStructureSignature;
        const activeSnapshotKeys = new Set(committedRowSnapshots.map(/** @param {any} [item] @returns {any} */ (item) => item.rowKey));

        // 1. Track expand/collapse intent transitions for expand-gated reveal & optimistic collapse
        let newlyExpandedParentCount = 0;
        const nextExpandedByPath = new Map();
        committedRowSnapshots.forEach(/** @param {any} [snap] */ (snap) => {
            if (!snap || !snap.rowPath) return;
            nextExpandedByPath.set(snap.rowPath, Boolean(snap.isExpanded));
            const prevExpanded = prevExpandedByPathRef.current.get(snap.rowPath);
            if (snap.isExpanded) {
                collapsedAtByPathRef.current.delete(snap.rowPath);
                if (prevExpanded === false) {
                    expandedAtByPathRef.current.set(snap.rowPath, now);
                    newlyExpandedParentCount += 1;
                }
            } else if (prevExpanded === true) {
                collapsedAtByPathRef.current.set(snap.rowPath, now);
                expandedAtByPathRef.current.delete(snap.rowPath);
            }
        });
        if (pendingRowTransitions && typeof pendingRowTransitions.forEach === 'function') {
            pendingRowTransitions.forEach(/** @param {any} [_phase] @param {string} [path] */ (_phase, path) => {
                if (path) expandedAtByPathRef.current.set(String(path), now);
            });
        }
        if (newlyExpandedParentCount > 1) {
            expandAllAtRef.current = now;
        }
        // Prune expired expand/collapse intent timestamps
        expandedAtByPathRef.current.forEach((ts, key) => {
            if (now - ts > 6000) expandedAtByPathRef.current.delete(key);
        });
        collapsedAtByPathRef.current.forEach((ts, key) => {
            if ((!isRequestPending && now - ts > 1200) || now - ts > 6000) {
                collapsedAtByPathRef.current.delete(key);
            }
        });

        // 2. Cancel exiting underlay on column change, reduced motion, or rapid re-expand
        setExitingRowSnapshots(/** @param {any} [prev] @returns {any} */ (prev) => {
            if (!Array.isArray(prev) || prev.length === 0) return prev;
            if (prefersReducedMotion || columnsChanged || rowSpanEnabled) return [];
            const remaining = prev.filter(/** @param {any} [snap] @returns {any} */ (snap) => !activeSnapshotKeys.has(snap.rowKey));
            return remaining.length === prev.length ? prev : remaining;
        });
        if (prefersReducedMotion || columnsChanged || rowSpanEnabled) {
            setCollapseHoldHeight(0);
        }

        /** @param {string} childPath @returns {boolean} */
        const hasCollapsedVisibleAncestor = (childPath) => {
            if (!childPath || !childPath.includes('|||')) return false;
            const parts = childPath.split('|||');
            for (let depth = parts.length - 1; depth >= 1; depth -= 1) {
                const ancestorKey = parts.slice(0, depth).join('|||');
                if (collapsedAtByPathRef.current.has(ancestorKey)) return true;
                const ancestorRow = centerRowLookup.get(ancestorKey);
                if (ancestorRow) {
                    const isExpanded = typeof ancestorRow.getIsExpanded === 'function'
                        ? Boolean(ancestorRow.getIsExpanded())
                        : Boolean(ancestorRow.original && ancestorRow.original._is_expanded);
                    if (!isExpanded) return true;
                }
            }
            return false;
        };

        /** @param {string} childPath @returns {boolean} */
        const hasRecentlyExpandedAncestor = (childPath) => {
            if (now - expandAllAtRef.current < 4000) return true;
            if (!childPath || !childPath.includes('|||')) return false;
            const parts = childPath.split('|||');
            for (let depth = parts.length - 1; depth >= 1; depth -= 1) {
                const ancestorKey = parts.slice(0, depth).join('|||');
                const expandedAt = expandedAtByPathRef.current.get(ancestorKey);
                if (expandedAt && now - expandedAt < 6000) return true;
            }
            return false;
        };

        // 3. Pre-compute unified batch timing across collapsing rows, expanding child sheets,
        //    surviving siblings, skeleton-to-data fills, and bottom-pinned Grand Total so every
        //    layer glides in exact lockstep.
        const baseMotionReady = Boolean(
            !prefersReducedMotion
            && !isScrollingNow
            && !wasScrollingRef.current
            && !columnsChanged
            && !rowSpanEnabled
            && scrollDelta < Math.max(12, rowHeight)
        );
        const layoutMotionEligible = Boolean(baseMotionReady && hasMountedCenterRowsRef.current);
        const isInitialViewportMount = !hasMountedCenterRowsRef.current && prevTopByKeyRef.current.size === 0;
        const skeletonFillEligible = Boolean(
            baseMotionReady
            && (prevSkeletonIndicesRef.current.size > 0 || isInitialViewportMount)
        );
        const isShrinkingVisually = (
            rowMotionLayoutPlan.hiddenKeys.size > 0
            || currentModelCount < prevModelRowCountRef.current
            || effectiveVisualBodyHeight < prevVisualBodyHeightRef.current
        );
        const vanishedRows = (layoutMotionEligible && isShrinkingVisually && prevCommittedRowsRef.current.length > 0)
            ? prevCommittedRowsRef.current
                .filter(/** @param {any} [snap] @returns {any} */ (snap) => (
                    snap
                    && snap.depth > 0
                    && !activeSnapshotKeys.has(snap.rowKey)
                    && hasCollapsedVisibleAncestor(snap.rowPath)
                ))
                .slice(0, 24)
            : [];
        const vanishedSpanPx = vanishedRows.reduce(
            /** @param {number} sum @param {any} snap */
            (sum, snap) => sum + (Number(snap && snap.size) > 0 ? Number(snap.size) : rowHeight),
            0
        );

        // When every visible row arrived from a full-viewport query (prevTopByKey empty),
        // let buildSkeletonFillPlan materialize every row cleanly as one cohesive cascade
        // instead of mixing depth-0 fade-in with depth>0 tucked sheet slides.
        const expandRevealPlan = (
            layoutMotionEligible
            && committedRowSnapshots.length > 0
            && prevTopByKeyRef.current.size > 0
        )
            ? buildExpandRevealPlan({
                committedSnapshots: committedRowSnapshots,
                prevTopByKey: prevTopByKeyRef.current,
                hasRecentlyExpandedAncestor,
                rowHeight,
            })
            : { revealByKey: new Map(), maxRevealTravelPx: 0 };

        const skeletonFillPlan = (skeletonFillEligible && committedRowSnapshots.length > 0)
            ? buildSkeletonFillPlan({
                committedSnapshots: committedRowSnapshots,
                prevSkeletonIndices: prevSkeletonIndicesRef.current,
                prevTopByKey: prevTopByKeyRef.current,
                expandRevealByKey: expandRevealPlan.revealByKey,
                isInitialMount: isInitialViewportMount,
            })
            : new Map();

        const bottomEl = bottomPinnedGroupRef.current;
        const containerEl = parentRef && parentRef.current;
        const hasBottomPinned = Boolean(bottomEl && containerEl && effectiveBottomRows.length > 0);
        const newBottomLayoutRelTop = hasBottomPinned
            ? computeBottomPinnedLayoutTop({
                stickyHeaderHeight: effectiveStickyHeaderHeight,
                topPinnedCount: effectiveTopRows.length,
                bottomPinnedCount: effectiveBottomRows.length,
                containerBodyHeight: effectiveContainerBodyHeight,
                rowHeight,
                scrollTop: currentScrollTop,
                clientHeight: containerEl ? containerEl.clientHeight : 0,
            })
            : null;
        const prevBottomLayoutRelTop = prevBottomPinnedRelTopRef.current;
        const bottomLayoutDelta = (
            hasBottomPinned
            && typeof newBottomLayoutRelTop === 'number'
            && typeof prevBottomLayoutRelTop === 'number'
        )
            ? (prevBottomLayoutRelTop - newBottomLayoutRelTop)
            : 0;

        let maxBatchShiftPx = Math.max(
            Math.min(vanishedSpanPx, MAX_SHEET_TRAVEL_PX),
            expandRevealPlan.maxRevealTravelPx,
            Math.abs(bottomLayoutDelta) <= MAX_FLIP_DISTANCE_PX ? Math.abs(bottomLayoutDelta) : 0
        );
        if (layoutMotionEligible && committedRowSnapshots.length > 0) {
            for (let i = 0; i < committedRowSnapshots.length; i += 1) {
                const snap = committedRowSnapshots[i];
                const prevTop = prevTopByKeyRef.current.get(snap.rowKey);
                if (typeof prevTop === 'number') {
                    const deltaAbs = Math.abs(prevTop - snap.top);
                    if (deltaAbs >= 1 && deltaAbs <= MAX_FLIP_DISTANCE_PX && deltaAbs > maxBatchShiftPx) {
                        maxBatchShiftPx = deltaAbs;
                    }
                }
            }
        }
        const batchDurationMs = computeSlideDurationMs(maxBatchShiftPx);

        if (vanishedRows.length > 0) {
            if (exitUnderlayTimerRef.current) clearTimeout(exitUnderlayTimerRef.current);
            const scrollEl = parentRef && parentRef.current;
            const newTotalContentHeight = (
                effectiveVisualBodyHeight
                + effectiveStickyHeaderHeight
                + ((effectiveTopRows.length + effectiveBottomRows.length) * rowHeight)
            );
            const wouldClampScroll = Boolean(
                scrollEl
                && scrollEl.scrollTop > 2
                && newTotalContentHeight < (scrollEl.scrollTop + scrollEl.clientHeight)
            );
            const holdHeight = (wouldClampScroll && prevVisualBodyHeightRef.current > effectiveVisualBodyHeight)
                ? prevVisualBodyHeightRef.current
                : 0;
            const exitShiftPx = Math.min(Math.max(vanishedSpanPx, 8), MAX_SHEET_TRAVEL_PX);
            const stampedVanished = vanishedRows.map(/** @param {any} snap */ (snap) => ({
                ...snap,
                exitShiftPx,
                exitDurationMs: batchDurationMs,
            }));
            setExitingRowSnapshots(stampedVanished);
            setCollapseHoldHeight(holdHeight);
            exitUnderlayTimerRef.current = setTimeout(() => {
                exitUnderlayTimerRef.current = null;
                setExitingRowSnapshots([]);
                setCollapseHoldHeight(0);
            }, batchDurationMs + 25);
        }

        // 4. Unified WAAPI FLIP + parent-anchored rigid-sheet expand reveal + 100ms fill crossfade
        // Track only our own WAAPI handles so unrelated re-renders (setExitingRowSnapshots,
        // pending transition clears, hover, server response settling) never cancel in-flight glides.
        /** @param {any} anim @returns {boolean} */
        const isAnimActive = (anim) => Boolean(
            anim && (anim.playState === 'running' || anim.playState === 'pending')
        );
        /** @param {HTMLElement} node @returns {number | null} */
        const readCurrentTranslateY = (node) => {
            if (typeof window === 'undefined' || typeof window.getComputedStyle !== 'function') return null;
            const computedTransform = window.getComputedStyle(node).transform;
            if (!computedTransform || computedTransform === 'none') return null;
            if (typeof DOMMatrix === 'function') {
                const matrix = new DOMMatrix(computedTransform);
                return Number.isFinite(matrix.m42) ? matrix.m42 : null;
            }
            const match = computedTransform.match(/matrix\([^,]+,[^,]+,[^,]+,[^,]+,[^,]+,\s*([^)]+)\)/);
            if (match) {
                const parsed = Number(match[1]);
                return Number.isFinite(parsed) ? parsed : null;
            }
            return null;
        };

        if (!layoutMotionEligible && !skeletonFillEligible) {
            cancelTrackedMotionAnimations();
        } else {
            // Prune tracking entries for unmounted rows
            activeFlipAnimByKeyRef.current.forEach((record, key) => {
                if (!activeSnapshotKeys.has(key)) {
                    if (record && record.anim && typeof record.anim.cancel === 'function') {
                        record.anim.cancel();
                    }
                    activeFlipAnimByKeyRef.current.delete(key);
                }
            });
            activeRevealAnimByKeyRef.current.forEach((record, key) => {
                if (!activeSnapshotKeys.has(key)) {
                    if (record && record.anim && typeof record.anim.cancel === 'function') {
                        record.anim.cancel();
                    }
                    activeRevealAnimByKeyRef.current.delete(key);
                }
            });

            if (committedRowSnapshots.length > 0) {
                const revealOpacityDurationMs = Math.round(batchDurationMs * 0.72);
                for (let i = 0; i < committedRowSnapshots.length; i += 1) {
                    const snap = committedRowSnapshots[i];
                    const el = rowNodeByKeyRef.current.get(snap.rowKey);
                    if (!el || typeof el.animate !== 'function') continue;
                    const innerEl = el.firstElementChild;

                    const prevTop = layoutMotionEligible ? prevTopByKeyRef.current.get(snap.rowKey) : undefined;
                    const revealSpec = layoutMotionEligible ? expandRevealPlan.revealByKey.get(snap.rowKey) : undefined;
                    const fillSpec = skeletonFillPlan.get(snap.rowKey);
                    if (typeof prevTop === 'number') {
                        const deltaY = prevTop - snap.top;
                        if (Math.abs(deltaY) >= 1 && Math.abs(deltaY) <= MAX_FLIP_DISTANCE_PX) {
                            let fromTop = prevTop;
                            const existingFlip = activeFlipAnimByKeyRef.current.get(snap.rowKey);
                            if (existingFlip && isAnimActive(existingFlip.anim)) {
                                const liveY = readCurrentTranslateY(el);
                                if (typeof liveY === 'number') {
                                    fromTop = liveY;
                                }
                                existingFlip.anim.cancel();
                                activeFlipAnimByKeyRef.current.delete(snap.rowKey);
                            }
                            if (Math.abs(fromTop - snap.top) >= 1 && Math.abs(fromTop - snap.top) <= MAX_FLIP_DISTANCE_PX) {
                                if (el.style) el.style.willChange = 'transform';
                                const anim = el.animate(
                                    [
                                        { transform: `translate3d(0, ${fromTop}px, 0)` },
                                        { transform: `translate3d(0, ${snap.top}px, 0)` },
                                    ],
                                    { duration: batchDurationMs, easing: ROW_MOTION_EASING }
                                );
                                const record = { anim, targetTop: snap.top, el, restoreZIndex: false };
                                activeFlipAnimByKeyRef.current.set(snap.rowKey, record);
                                const cleanup = () => {
                                    if (activeFlipAnimByKeyRef.current.get(snap.rowKey) === record) {
                                        activeFlipAnimByKeyRef.current.delete(snap.rowKey);
                                        if (el.style) el.style.willChange = '';
                                    }
                                };
                                anim.onfinish = cleanup;
                                anim.oncancel = cleanup;
                            }
                        }
                    } else if (revealSpec) {
                        // Parent-anchored rigid-sheet unfold: wrapper glides synchronously with
                        // descending siblings (zIndex: 2 behind opaque zIndex: 4 parent) while
                        // inner row materializes smoothly without stagger drift.
                        const existingFlip = activeFlipAnimByKeyRef.current.get(snap.rowKey);
                        if (existingFlip && existingFlip.anim && typeof existingFlip.anim.cancel === 'function') {
                            existingFlip.anim.cancel();
                        }
                        if (el.style) {
                            el.style.zIndex = '2';
                            el.style.willChange = 'transform';
                        }
                        const slideAnim = el.animate(
                            [
                                { transform: `translate3d(0, ${revealSpec.fromTop}px, 0)` },
                                { transform: `translate3d(0, ${revealSpec.toTop}px, 0)` },
                            ],
                            { duration: batchDurationMs, easing: ROW_MOTION_EASING }
                        );
                        const slideRecord = { anim: slideAnim, targetTop: revealSpec.toTop, el, restoreZIndex: true };
                        activeFlipAnimByKeyRef.current.set(snap.rowKey, slideRecord);
                        const cleanupSlide = () => {
                            if (activeFlipAnimByKeyRef.current.get(snap.rowKey) === slideRecord) {
                                activeFlipAnimByKeyRef.current.delete(snap.rowKey);
                                if (el.style) {
                                    el.style.zIndex = '4';
                                    el.style.willChange = '';
                                }
                            }
                        };
                        slideAnim.onfinish = cleanupSlide;
                        slideAnim.oncancel = cleanupSlide;

                        const targetEl = /** @type {HTMLElement} */ ((innerEl && typeof innerEl.animate === 'function') ? innerEl : el);
                        const prevReveal = activeRevealAnimByKeyRef.current.get(snap.rowKey);
                        if (prevReveal && prevReveal.anim && typeof prevReveal.anim.cancel === 'function') {
                            prevReveal.anim.cancel();
                        }
                        if (targetEl.style) targetEl.style.willChange = 'opacity';
                        const fadeAnim = targetEl.animate(
                            [{ opacity: 0 }, { opacity: 1 }],
                            { duration: revealOpacityDurationMs, easing: 'ease-out', fill: 'backwards' }
                        );
                        const fadeRecord = { anim: fadeAnim, targetEl };
                        activeRevealAnimByKeyRef.current.set(snap.rowKey, fadeRecord);
                        const cleanupFade = () => {
                            if (activeRevealAnimByKeyRef.current.get(snap.rowKey) === fadeRecord) {
                                activeRevealAnimByKeyRef.current.delete(snap.rowKey);
                                if (targetEl.style) targetEl.style.willChange = '';
                            }
                        };
                        fadeAnim.onfinish = cleanupFade;
                        fadeAnim.oncancel = cleanupFade;
                    } else if (fillSpec) {
                        // Smooth query/skeleton arrival: inner row settles gently into its final
                        // slot with a tight top-to-bottom cascade without disturbing outer wrapper Y.
                        const targetEl = /** @type {HTMLElement} */ ((innerEl && typeof innerEl.animate === 'function') ? innerEl : el);
                        const prevReveal = activeRevealAnimByKeyRef.current.get(snap.rowKey);
                        if (prevReveal && prevReveal.anim && typeof prevReveal.anim.cancel === 'function') {
                            prevReveal.anim.cancel();
                        }
                        const canTranslateInner = Boolean(innerEl && targetEl === innerEl);
                        if (targetEl.style) {
                            targetEl.style.willChange = canTranslateInner ? 'transform, opacity' : 'opacity';
                        }
                        const keyframes = canTranslateInner
                            ? [
                                { opacity: 0, transform: `translate3d(0, ${fillSpec.offsetYPx}px, 0)` },
                                { opacity: 1, transform: 'translate3d(0, 0, 0)' },
                            ]
                            : [{ opacity: 0 }, { opacity: 1 }];
                        const anim = targetEl.animate(
                            keyframes,
                            {
                                duration: fillSpec.durationMs,
                                delay: fillSpec.delayMs,
                                easing: ROW_MOTION_EASING,
                                fill: 'backwards',
                            }
                        );
                        const record = { anim, targetEl };
                        activeRevealAnimByKeyRef.current.set(snap.rowKey, record);
                        const cleanup = () => {
                            if (activeRevealAnimByKeyRef.current.get(snap.rowKey) === record) {
                                activeRevealAnimByKeyRef.current.delete(snap.rowKey);
                                if (targetEl.style) targetEl.style.willChange = '';
                            }
                        };
                        anim.onfinish = cleanup;
                        anim.oncancel = cleanup;
                    }
                }
            }
        }

        // 4b. GPU WAAPI FLIP (or smooth post-query fade-in) for bottom-pinned rows (Grand Total).
        if (hasBottomPinned && bottomEl && typeof newBottomLayoutRelTop === 'number') {
            if (
                layoutMotionEligible
                && typeof prevBottomLayoutRelTop === 'number'
                && typeof bottomEl.animate === 'function'
            ) {
                if (Math.abs(bottomLayoutDelta) >= 1.5 && Math.abs(bottomLayoutDelta) <= MAX_FLIP_DISTANCE_PX) {
                    const activeBottomAnim = activeBottomFlipAnimRef.current;
                    const currentBottomTransformY = isAnimActive(activeBottomAnim)
                        ? (readCurrentTranslateY(bottomEl) ?? 0)
                        : 0;
                    const fromOffset = (prevBottomLayoutRelTop + currentBottomTransformY) - newBottomLayoutRelTop;
                    if (activeBottomAnim && typeof activeBottomAnim.cancel === 'function') {
                        activeBottomAnim.cancel();
                        activeBottomFlipAnimRef.current = null;
                    }
                    if (Math.abs(fromOffset) >= 1.5 && Math.abs(fromOffset) <= MAX_FLIP_DISTANCE_PX) {
                        if (bottomEl.style) bottomEl.style.willChange = 'transform';
                        const anim = bottomEl.animate(
                            [
                                { transform: `translate3d(0, ${fromOffset}px, 0)` },
                                { transform: 'translate3d(0, 0, 0)' },
                            ],
                            { duration: batchDurationMs, easing: ROW_MOTION_EASING }
                        );
                        activeBottomFlipAnimRef.current = anim;
                        const cleanup = () => {
                            if (activeBottomFlipAnimRef.current === anim) {
                                activeBottomFlipAnimRef.current = null;
                                if (bottomEl.style) bottomEl.style.willChange = '';
                            }
                        };
                        anim.onfinish = cleanup;
                        anim.oncancel = cleanup;
                    }
                }
            } else if (
                skeletonFillEligible
                && prevBottomLayoutRelTop === null
                && skeletonFillPlan.size > 0
                && typeof bottomEl.animate === 'function'
            ) {
                const activeBottomAnim = activeBottomFlipAnimRef.current;
                if (activeBottomAnim && typeof activeBottomAnim.cancel === 'function') {
                    activeBottomAnim.cancel();
                    activeBottomFlipAnimRef.current = null;
                }
                if (bottomEl.style) bottomEl.style.willChange = 'opacity';
                const anim = bottomEl.animate(
                    [{ opacity: 0 }, { opacity: 1 }],
                    { duration: SKELETON_FILL_DURATION_MS, easing: 'ease-out', fill: 'backwards' }
                );
                activeBottomFlipAnimRef.current = anim;
                const cleanup = () => {
                    if (activeBottomFlipAnimRef.current === anim) {
                        activeBottomFlipAnimRef.current = null;
                        if (bottomEl.style) bottomEl.style.willChange = '';
                    }
                };
                anim.onfinish = cleanup;
                anim.oncancel = cleanup;
            }
            prevBottomPinnedRelTopRef.current = newBottomLayoutRelTop;
        } else {
            prevBottomPinnedRelTopRef.current = null;
        }

        // 5. Commit tracking state for the next frame
        const nextTopByKey = new Map();
        const nextSkeletonIndices = new Set();
        committedRowSnapshots.forEach(/** @param {any} [snap] */ (snap) => {
            nextTopByKey.set(snap.rowKey, snap.top);
        });
        if (serverSide) {
            const freshIndexSet = new Set();
            committedRowSnapshots.forEach(/** @param {any} [snap] */ (snap) => {
                if (snap && typeof snap.virtualIndex === 'number') {
                    freshIndexSet.add(snap.virtualIndex);
                }
            });
            virtualRows.forEach(/** @param {any} [virtualRow] */ (virtualRow) => {
                if (!virtualRow || pinnedVirtualIndexSet.has(virtualRow.index)) return;
                if (freshIndexSet.has(virtualRow.index)) return;
                const resolvedCurrent = resolveVirtualRow(virtualRow.index);
                const resolvedKey = resolvedCurrent && resolvedCurrent.id ? String(resolvedCurrent.id) : null;
                if (resolvedKey && rowMotionLayoutPlan.hiddenKeys.has(resolvedKey)) {
                    // Optimistically hidden collapsed descendant — not a missing skeleton slot.
                    return;
                }
                nextSkeletonIndices.add(virtualRow.index);
            });
        }

        prevTopByKeyRef.current = nextTopByKey;
        if (committedRowSnapshots.length > 0 || currentModelCount === 0) {
            prevCommittedRowsRef.current = committedRowSnapshots;
            prevExpandedByPathRef.current = nextExpandedByPath;
        }
        prevVisualBodyHeightRef.current = effectiveVisualBodyHeight;
        prevSkeletonIndicesRef.current = nextSkeletonIndices;
        prevColumnSignatureRef.current = columnStructureSignature;
        prevScrollTopRef.current = currentScrollTop;
        prevModelRowCountRef.current = currentModelCount;
        wasScrollingRef.current = isScrollingNow;
        if (committedRowSnapshots.length > 0 || effectiveCenterRows.length > 0) {
            hasMountedCenterRowsRef.current = true;
        }
    });
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
                        data-pinned-edge={disablePinnedColumnStickiness ? undefined : 'left'}
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
        const opaqueSurfaceBg = theme.surfaceBg || theme.background || '#FFFFFF';

        return (
            <div
                ref={isTop ? undefined : bottomPinnedGroupRef}
                style={{
                    position: 'sticky',
                    top: isTop ? stickyOffset : undefined,
                    bottom: isTop ? undefined : 0,
                    zIndex: 80,
                    isolation: 'isolate',
                    backgroundColor: opaqueSurfaceBg,
                    display: 'flex',
                    width: `${totalLayoutWidth}px`,
                    minWidth: '100%',
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
                        ? (isTop ? `0 2px 4px -2px ${withAlpha(theme.border, 0.5, 'rgba(148,163,184,0.35)')}` : '0 -4px 6px -1px rgba(15,23,42,0.06)')
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
                                background: `linear-gradient(var(--sp-row-current-bg, ${rowBackground}), var(--sp-row-current-bg, ${rowBackground})), ${opaqueSurfaceBg}`,
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
            <div
                ref={parentRef}
                data-pivot-scroll-container="true"
                style={{...styles.scrollContainer, overflow: 'auto', '--sp-left-pinned-width': `${leftPinnedWidth}px`}}
                onKeyDown={handleKeyDown}
                tabIndex={0}
                role="grid"
                aria-rowcount={rows.length}
                aria-colcount={visibleLeafColumns.length}
            >
                 <div style={{
                     width: `${totalLayoutWidth}px`,
                     minWidth:'100%',
                     height: `${effectiveContainerBodyHeight + effectiveStickyHeaderHeight + (effectiveTopRows.length + effectiveBottomRows.length) * rowHeight}px`,
                     position: 'relative',
                     borderRadius: `0 0 ${theme.radius || '8px'} ${theme.radius || '8px'}`,
                     overflow: 'visible',
                 }}>
                     {/* Sticky Header */}
                     <div
                         ref={stickyHeaderRef}
                         data-pivot-sticky-header="true"
                         style={{
                             ...styles.headerSticky,
                             background: `linear-gradient(${theme.headerBg}, ${theme.headerBg}), ${theme.surfaceBg || theme.background || '#FFFFFF'}`,
                             isolation: 'isolate',
                             width: 'fit-content',
                             display: 'flex',
                             position: stickyHeaders ? 'sticky' : 'relative',
                             top: stickyHeaders ? 0 : undefined
                         }}
                         role="rowgroup"
                     >
                         {/* Left Section */}
                         <div data-pinned-edge={leftPinnedWidth > 0 ? 'left' : undefined} style={{position: 'sticky', left: 0, zIndex: 4, background: `linear-gradient(${theme.headerBg}, ${theme.headerBg}), ${theme.surfaceBg || theme.background || '#FFFFFF'}`}}>
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
                                 <div style={floatingFilterRowStyle}>
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
                                                 animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 1.6s) linear infinite'
                                             }}
                                         />
                                     ))}
                                 </div>
                             )}
                             {showFloatingFilters && (
                                 <div style={floatingFilterRowStyle}>
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
                         <div style={{position: 'sticky', right: 0, zIndex: 4, background: `linear-gradient(${theme.headerBg}, ${theme.headerBg}), ${theme.surfaceBg || theme.background || '#FFFFFF'}`}}>
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
                                 <div style={floatingFilterRowStyle}>
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
                        const opaqueSurfaceBg = theme.surfaceBg || theme.background || '#FFFFFF';
                        return (
                        <div
                            role="row"
                            {...getRowDataAttributes(stickyTreeRow)}
                            style={{
                                position: 'sticky',
                                top: `${(stickyHeaders ? effectiveStickyHeaderHeight : 0) + (effectiveTopRows.length * rowHeight)}px`,
                                zIndex: 79,
                                isolation: 'isolate',
                                display: 'flex',
                                width: `${totalLayoutWidth}px`,
                                minWidth: '100%',
                                height: `${rowHeight}px`,
                                '--sp-row-current-bg': stickyRowBg,
                                background: `linear-gradient(var(--sp-row-current-bg, ${stickyRowBg}), var(--sp-row-current-bg, ${stickyRowBg})), ${opaqueSurfaceBg}`,
                                borderBottom: `1px solid ${gridBorders.rowBorderColor}`,
                                boxShadow: `0 2px 4px -2px ${withAlpha(theme.border, 0.5, 'rgba(148,163,184,0.35)')}`,
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
                                         background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(148,163,184,0.14) 0%, rgba(148,163,184,0.28) 45%, rgba(148,163,184,0.14) 100%))',
                                         backgroundSize: '220% 100%',
                                         border: '1px solid var(--pivot-loading-border, rgba(153, 187, 238, 0.5))',
                                         animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 1.6s) linear infinite'
                                     }}
                                 />
                             ))}
                         </div>
                     )}

                     {/* Exiting Collapsed Underlay Rows (AG Grid FLIP GPU slot) */}
                     {exitingRowSnapshots.map(/** @param {any} [snap] @returns {any} */ (snap) => {
                         if (!snap || !snap.row || typeof snap.row.getValue !== 'function') return null;
                         return (
                             <div
                                 key={`exit_${snap.rowKey}`}
                                 aria-hidden="true"
                                 style={{
                                     position: 'absolute',
                                     top: 0,
                                     left: 0,
                                     width: `${totalLayoutWidth}px`,
                                     height: snap.size,
                                     transform: `translate3d(0, ${snap.top}px, 0)`,
                                     zIndex: 1,
                                     pointerEvents: 'none',
                                 }}
                             >
                                 <div
                                     className="pivot-row-animated"
                                     style={{
                                         ...baseRowStyle,
                                         position: 'relative',
                                         top: undefined,
                                         left: undefined,
                                         width: '100%',
                                         height: '100%',
                                         '--sp-row-current-bg': snap.rowBg,
                                         '--sp-exit-shift': `-${snap.exitShiftPx || 8}px`,
                                         background: 'var(--sp-row-current-bg)',
                                         animation: `pivot-row-exit ${snap.exitDurationMs || 220}ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) forwards`,
                                     }}
                                 >
                                     {renderRowSections(snap.row, snap.displayRowIndex, true, {
                                         rowBackground: snap.rowBg,
                                         rowSpanByColumnId: snap.rowSpanByColumnId,
                                         disablePinnedColumnStickiness: true,
                                     })}
                                 </div>
                             </div>
                         );
                     })}

                     {/* Center Virtualized Rows */}
                     {virtualRows.map(/** @param {any} [virtualRow] @returns {any} */ virtualRow => {
                          const topOffset = effectiveBodyRowsTopOffset;
                          const offsetDelta = rowMotionLayoutPlan.topOffsetDeltaByIndex.get(virtualRow.index) || 0;
                          /** @type {any} */
                          const adjustedTop = virtualRow.start - (getPinnedRowsBefore(virtualRow.index) * rowHeight) + topOffset + offsetDelta;

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
                                            transform: `translate3d(0, ${adjustedTop}px, 0)`,
                                            background: defaultRowBg,
                                        }}>
                                            {renderSkeletonSections(defaultRowBg)}
                                        </div>
                                 );
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
                                     transform: `translate3d(0, ${adjustedTop}px, 0)`,
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

                          // Stable key: use row path/id for loaded rows in both server-side and
                          // client-side modes so expand/collapse does not remount rows that merely shifted index.
                          /** @type {any} */
                          const stableRowKey = (row && row.id)
                              ? String(row.id)
                              : String(virtualRow.index);
                          if (rowMotionLayoutPlan.hiddenKeys.has(stableRowKey)) {
                              return null;
                          }
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
                          const rowDepthValue = Number((row.original && row.original.depth !== undefined) ? row.original.depth : (row.depth || 0));
                          const transitionRowPath = (row.original && row.original._path) ? String(row.original._path) : stableRowKey;
                          const transitionPhase = (pendingRowTransitions && typeof pendingRowTransitions.get === 'function')
                              ? pendingRowTransitions.get(transitionRowPath)
                              : null;
                          const treeIndentPx = Number(theme.treeIndent) > 0 ? Number(theme.treeIndent) : 24;

                          return (
                              <React.Fragment key={stableRowKey}>
                                  <div
                                      ref={(el) => registerRowNode(stableRowKey, el)}
                                      style={rowSpanEnabled ? {
                                          position: 'absolute',
                                          top: `${adjustedTop}px`,
                                          left: 0,
                                          width: `${totalLayoutWidth}px`,
                                          height: virtualRow.size,
                                      } : {
                                          position: 'absolute',
                                          top: 0,
                                          left: 0,
                                          width: `${totalLayoutWidth}px`,
                                          height: virtualRow.size,
                                          transform: `translate3d(0, ${adjustedTop}px, 0)`,
                                          zIndex: 4,
                                          opacity: 1,
                                          transition: disableRowMotion ? 'none' : 'opacity var(--sp-dur-fast, 140ms) ease',
                                      }}
                                  >
                                  <div
                                     role="row"
                                     aria-rowindex={virtualRow.index + 1}
                                     className="pivot-row-animated"
                                     {...getRowDataAttributes(row)}
                                     style={{
                                      ...baseRowStyle,
                                      position: 'relative',
                                      top: undefined,
                                      left: undefined,
                                      width: '100%',
                                      height: '100%',
                                      '--sp-row-current-bg': rowBg,
                                      background: 'var(--sp-row-current-bg)',
                                      ...(isTotalVirtualRow && theme.totalBorder ? {
                                          borderBottom: String(theme.totalBorder).includes(' ') ? theme.totalBorder : `1px solid ${theme.totalBorder}`,
                                      } : {}),
                                      transition: disableRowMotion ? 'none' : 'background-color var(--sp-dur-fast, 140ms) ease',
                                   }}>
                                      {renderRowSections(row, displayRowIndex, true, {
                                          rowBackground: rowBg,
                                          rowSpanByColumnId,
                                      })}
                                      {(transitionPhase === 'hairline' || transitionPhase === 'band') && (
                                          <div
                                              aria-hidden="true"
                                              data-row-loader="hairline"
                                              style={{
                                                  position: 'absolute',
                                                  left: 0,
                                                  right: 0,
                                                  bottom: 0,
                                                  height: '2px',
                                                  overflow: 'hidden',
                                                  pointerEvents: 'none',
                                                  zIndex: 6,
                                                  background: withAlpha(theme.primary, 0.14, 'rgba(79,70,229,0.14)'),
                                              }}
                                          >
                                              <div
                                                  style={{
                                                      width: '36%',
                                                      height: '100%',
                                                      background: `linear-gradient(90deg, transparent, ${theme.primary || 'var(--sp-primary, #4F46E5)'}, transparent)`,
                                                      animation: 'pivot-hairline-sweep 900ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) infinite',
                                                  }}
                                              />
                                          </div>
                                      )}
                                  </div>
                                  </div>
                                  {transitionPhase === 'band' && rowMotionLayoutPlan.bandGapKeys.has(stableRowKey) && (
                                      <div
                                          aria-hidden="true"
                                          data-row-loader="band"
                                          style={{
                                              ...transitionLoaderStyle,
                                              transform: `translate3d(0, ${adjustedTop + virtualRow.size}px, 0)`,
                                              paddingLeft: `${28 + ((rowDepthValue + 1) * treeIndentPx)}px`,
                                          }}
                                      >
                                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: '3px', color: theme.primary || 'var(--sp-primary, #4F46E5)' }}>
                                              <span style={{ width: '4px', height: '4px', borderRadius: '50%', background: 'currentColor', animation: 'pivot-wave-dot 840ms ease-in-out infinite' }} />
                                              <span style={{ width: '4px', height: '4px', borderRadius: '50%', background: 'currentColor', animation: 'pivot-wave-dot 840ms ease-in-out 140ms infinite' }} />
                                              <span style={{ width: '4px', height: '4px', borderRadius: '50%', background: 'currentColor', animation: 'pivot-wave-dot 840ms ease-in-out 280ms infinite' }} />
                                          </span>
                                          <span style={{ fontSize: 'var(--sp-fs-sm, 11.5px)', fontWeight: 600, color: theme.primary || 'var(--sp-primary, #4F46E5)' }}>
                                              Loading breakdown…
                                          </span>
                                          <span style={{
                                              width: '140px',
                                              height: '3px',
                                              borderRadius: '999px',
                                              background: withAlpha(theme.primary, 0.14, 'rgba(79,70,229,0.14)'),
                                              overflow: 'hidden',
                                              position: 'relative',
                                          }}>
                                              <span style={{
                                                  position: 'absolute',
                                                  inset: 0,
                                                  width: '45%',
                                                  borderRadius: '999px',
                                                  background: theme.primary || 'var(--sp-primary, #4F46E5)',
                                                  animation: 'pivot-hairline-sweep 950ms ease-in-out infinite',
                                              }} />
                                          </span>
                                      </div>
                                  )}
                                  {detailMode === 'inline' && detailState && detailState.anchorRowId === row.id && (
                                      <React.Suspense fallback={<ShimmerFallback variant="inline" height={detailInlineHeight} />}>
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
                              </React.Fragment>
                          );
                      })}

                     {/* Spacer for all bottom-pinned rows.
                          Virtual rows use position:absolute (out of flow), so without this spacer
                          bottom sticky rows never reach their sticky activation point. */}
                     {effectiveBottomRows.length > 0 && (
                         <div style={{
                             height: `${effectiveContainerBodyHeight}px`,
                             flexShrink: 0,
                         }} />
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
                            visibleRowsCount: serverSide ? virtualRows.length : rows.length,
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
            {isRequestPending && (
                <div
                    aria-hidden="true"
                    data-pivot-loading-indicator="global"
                    style={{
                        position: 'absolute',
                        bottom: 0,
                        left: 0,
                        right: 0,
                        height: '3px',
                        zIndex: 120,
                        pointerEvents: 'none',
                        background: 'var(--pivot-loading-progress-gradient, linear-gradient(90deg, rgba(75,139,245,0) 0%, rgba(75,139,245,0.9) 45%, rgba(75,139,245,0) 100%))',
                        backgroundSize: '220% 100%',
                        animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 1.6s) linear infinite'
                    }}
                />
            )}
        </div>
    );
});


import { observeElementRect, useVirtualizer } from '@tanstack/react-virtual';
import {
    useCallback,
    useEffect,
    useLayoutEffect,
    useMemo,
    useRef,
} from 'react';

/** @type {Record<string, any>} */
const DEFAULT_VISIBLE_RANGE = { start: 0, end: 0 };
const SERVER_SIDE_HORIZONTAL_METRICS_DEBOUNCE_MS = 96;

// Stable-reference helper: returns the previous runtime-column array while both
// its physical layout and the source column definitions are unchanged.
//
// Outline and Tabular intentionally use the same column ids and widths, but
// their accessor/cell closures have different display contracts. Comparing
// only id:size therefore kept Outline column objects alive after switching to
// Tabular, so the toolbar showed Tabular while ancestor cells still executed
// Outline's depth-only renderer.
/** @param {any[]} columns @param {any} [columnDefinitions] @returns {any} */
const useStableColumnList = (columns, columnDefinitions) => {
    /** @type {any} */
    const prevRef = useRef(columns);
    /** @type {any} */
    const prevDefinitionsRef = useRef(columnDefinitions);
    /** @type {any} */
    const prevSignatureRef = useRef('');
    /** @type {any} */
    const signature = columns.map((column) => {
        /** @type {any} */
        const columnId = column && column.id ? column.id : '';
        /** @type {any} */
        const size = column && typeof column.getSize === 'function' ? column.getSize() : '';
        return `${columnId}:${size}`;
    }).join(',');
    if (
        signature !== prevSignatureRef.current
        || columnDefinitions !== prevDefinitionsRef.current
    ) {
        prevRef.current = columns;
        prevDefinitionsRef.current = columnDefinitions;
        prevSignatureRef.current = signature;
    }
    return prevRef.current;
};

/** @param {any} p0 @returns {any} */
export const useColumnVirtualizer = ({
    parentRef,
    table,
    estimateColumnWidth,
    serverSide = false,
    totalCenterCols = null,
    columnOverscan = null,
    stateEpoch = 0,
    onHorizontalScrollMetrics = null,
    onPreciseVisibleColRange = null,
    // Column-structure deps so we can memoize the leaf-column lists properly.
    // When omitted the lists still recompute every render (safe fallback).
    columnVisibility,
    columnPinning,
    columnOrder,
    columnSizing,
    columns,
}) => {
    /** @type {any} */
    const pendingHorizontalMetricsRef = useRef(null);
    /** @type {any} */
    const horizontalMetricsTimerRef = useRef(null);

    /** @type {(...args: any[]) => any} */
    const scheduleHorizontalMetrics = useCallback((metrics, options = {}) => {
        if (!serverSide || typeof onHorizontalScrollMetrics !== 'function') return;

        /** @type {any} */
        const immediate = Boolean(options.immediate);
        /** @type {any} */
        const debounceMs = Number.isFinite(Number(options.debounceMs))
            ? Math.max(0, Math.floor(Number(options.debounceMs)))
            : SERVER_SIDE_HORIZONTAL_METRICS_DEBOUNCE_MS;

        if (immediate) {
            if (horizontalMetricsTimerRef.current) {
                clearTimeout(horizontalMetricsTimerRef.current);
                horizontalMetricsTimerRef.current = null;
            }
            pendingHorizontalMetricsRef.current = null;
            onHorizontalScrollMetrics(metrics);
            return;
        }

        pendingHorizontalMetricsRef.current = metrics;
        if (horizontalMetricsTimerRef.current) return;

        horizontalMetricsTimerRef.current = setTimeout(() => {
            horizontalMetricsTimerRef.current = null;
            const latestMetrics = pendingHorizontalMetricsRef.current;
            pendingHorizontalMetricsRef.current = null;
            if (latestMetrics && typeof onHorizontalScrollMetrics === 'function') {
                onHorizontalScrollMetrics(latestMetrics);
            }
        }, debounceMs);
    }, [onHorizontalScrollMetrics, serverSide]);

    useEffect(() => () => {
        if (horizontalMetricsTimerRef.current) {
            clearTimeout(horizontalMetricsTimerRef.current);
            horizontalMetricsTimerRef.current = null;
        }
        pendingHorizontalMetricsRef.current = null;
    }, []);

    /* eslint-disable react-hooks/exhaustive-deps */
    /** @type {any} */
    const leftCols = useStableColumnList(
        useMemo(() => table.getLeftLeafColumns().filter(/** @param {any} [c] @returns {any} */ c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnOrder, columnSizing]),
        columns
    );
    /** @type {any} */
    const rightCols = useStableColumnList(
        useMemo(() => table.getRightLeafColumns().filter(/** @param {any} [c] @returns {any} */ c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnOrder, columnSizing]),
        columns
    );
    /** @type {any} */
    const centerCols = useStableColumnList(
        useMemo(() => table.getCenterLeafColumns().filter(/** @param {any} [c] @returns {any} */ c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnOrder, columnSizing]),
        columns
    );
    /* eslint-enable react-hooks/exhaustive-deps */

    /** @type {any} */
    const pinnedWidth = [...leftCols, ...rightCols].reduce((sum, column) => sum + column.getSize(), 0);
    /** @type {any} */
    const pinnedWidthRef = useRef(pinnedWidth);
    pinnedWidthRef.current = pinnedWidth;
    /** @type {any} */
    const publishCenterRectRef = useRef(null);
    /** @type {(...args: any[]) => any} */
    const observeCenterRect = useCallback((instance, notify) => {
        /** @type {any} */
        let lastRect = null;
        /** @returns {any} */
        const publish = () => {
            if (!lastRect) return;
            /** @type {any} */
            const width = instance.scrollElement?.clientWidth ?? lastRect.width;
            notify({ ...lastRect, width: Math.max(0, width - pinnedWidthRef.current) });
        };
        publishCenterRectRef.current = publish;
        /** @type {any} */
        const cleanup = observeElementRect(instance, rect => {
            lastRect = rect;
            publish();
        });
        return () => {
            if (publishCenterRectRef.current === publish) publishCenterRectRef.current = null;
            cleanup?.();
        };
    }, []);

    /** @type {any} */
    const horizontalOverscan = (
        columnOverscan !== null
        && columnOverscan !== undefined
        && Number.isFinite(Number(columnOverscan))
    )
        ? Math.max(0, Math.floor(Number(columnOverscan)))
        : (
            serverSide && Number.isFinite(Number(totalCenterCols)) && Number(totalCenterCols) >= 5000
                ? 1
                : 2
        );

    /** @type {any} */
    const columnVirtualizer = useVirtualizer({
        horizontal: true,
        count: centerCols.length,
        getScrollElement: () => parentRef.current,
        /** @param {any} [index] @returns {any} */
        estimateSize: (index) => centerCols[index].getSize(),
        // Center measurements start at zero; sticky pins occupy viewport space,
        // not measurement space. Auto alignment and its maximum offset must use
        // the unobstructed center width without shifting any render spacers.
        observeElementRect: observeCenterRect,
        // Small overscan to avoid blank flashes during fast horizontal scroll.
        // Extremely wide column sets benefit from a tighter overscan window.
        overscan: horizontalOverscan
    });

    // The core keeps its observer when the scroll element is unchanged. Pin or
    // pin-size changes therefore need to publish a fresh rect independently of
    // a physical element resize, using the same mounted virtualizer instance.
    useLayoutEffect(() => {
        publishCenterRectRef.current?.();
    }, [pinnedWidth, columnVirtualizer]);

    /** @type {any} */
    const rawVirtualCenterCols = columnVirtualizer.getVirtualItems();
    // TanStack can briefly report stale indices after a fast center-column count shrink
    // (e.g. collapsing a pivot group near the right edge). Filter them out so we
    // never address non-existent center cells.
    /** @type {any} */
    const virtualCenterCols = useMemo(
        () => rawVirtualCenterCols.filter(/** @param {any} [item] @returns {any} */ item => item.index >= 0 && item.index < centerCols.length),
        [rawVirtualCenterCols, centerCols.length]
    );
    /** @type {any} */
    const centerTotalWidth = columnVirtualizer.getTotalSize();

    // Calculate spacers for virtualized center
    /** @type {any} */
    const [beforeWidth, afterWidth] = useMemo(() => {
        if (virtualCenterCols.length > 0) {
            return [
                Math.max(0, virtualCenterCols[0].start),
                Math.max(0, centerTotalWidth - virtualCenterCols[virtualCenterCols.length - 1].end)
            ];
        }
        return [0, 0];
    }, [virtualCenterCols, centerTotalWidth]);

    /** @type {any} */
    const totalLayoutWidth = useMemo(() => {
        /** @type {any} */
        const leftWidth = leftCols.reduce(/** @param {any} [acc] @param {any} [col] @returns {any} */ (acc, col) => acc + col.getSize(), 0);
        /** @type {any} */
        const rightWidth = rightCols.reduce(/** @param {any} [acc] @param {any} [col] @returns {any} */ (acc, col) => acc + col.getSize(), 0);
        return leftWidth + centerTotalWidth + rightWidth;
    }, [leftCols, rightCols, centerTotalWidth]);

    /** @type {any} */
    const visibleColRange = useMemo(() => {
        if (virtualCenterCols.length === 0) return { start: 0, end: 0 };
        return {
            start: virtualCenterCols[0].index,
            end: virtualCenterCols[virtualCenterCols.length - 1].index
        };
    }, [virtualCenterCols]);

    /** @type {any} */
    const averageCenterColumnWidth = useMemo(() => {
        if (centerCols.length <= 0) return estimateColumnWidth || 140;
        /** @type {any} */
        const totalWidth = Number(centerTotalWidth);
        if (!Number.isFinite(totalWidth) || totalWidth <= 0) {
            return estimateColumnWidth || 140;
        }
        return Math.max(48, totalWidth / centerCols.length);
    }, [centerCols.length, centerTotalWidth, estimateColumnWidth]);

    useLayoutEffect(() => {
        if (!parentRef.current || !columnVirtualizer) return;
        const scrollEl = parentRef.current;
        /** @type {any} */
        const maxScrollLeft = Math.max(0, scrollEl.scrollWidth - scrollEl.clientWidth);
        if (scrollEl.scrollLeft > maxScrollLeft) {
            scrollEl.scrollLeft = maxScrollLeft;
            columnVirtualizer.scrollOffset = maxScrollLeft;
        }
        // Measuring on every horizontal virtual-index change makes wide pivots
        // jank badly. Re-measure only when the column structure/layout changes.
        columnVirtualizer.measure();

    }, [centerCols.length, columnVirtualizer, parentRef, totalLayoutWidth]);

    useLayoutEffect(() => {
        const scrollEl = parentRef.current;
        if (!scrollEl || !columnVirtualizer) return undefined;

        /** @returns {any} */
        const remeasure = () => {
            columnVirtualizer.measure();
            if (serverSide) {
                scheduleHorizontalMetrics({
                    scrollLeft: scrollEl.scrollLeft,
                    clientWidth: scrollEl.clientWidth,
                    scrollWidth: scrollEl.scrollWidth,
                    averageCenterColumnWidth,
                    centerColumnCount: centerCols.length,
                }, { immediate: true });
            }
        };

        remeasure();

        if (typeof ResizeObserver === 'undefined') {
            window.addEventListener('resize', remeasure);
            return () => {
                window.removeEventListener('resize', remeasure);
            };
        }

        /** @type {any} */
        const observer = new ResizeObserver(() => {
            remeasure();
        });
        observer.observe(scrollEl);
        return () => {
            observer.disconnect();
        };
    }, [
        averageCenterColumnWidth,
        centerCols.length,
        columnVirtualizer,
        parentRef,
        scheduleHorizontalMetrics,
        serverSide,
    ]);

    useEffect(() => {
        if (!serverSide || !parentRef.current || centerCols.length === 0 || typeof onHorizontalScrollMetrics !== 'function') {
            return;
        }

        const scrollEl = parentRef.current;
        /** @returns {any} */
        const handleHorizontalScroll = () => {
            scheduleHorizontalMetrics({
                scrollLeft: scrollEl.scrollLeft,
                clientWidth: scrollEl.clientWidth,
                scrollWidth: scrollEl.scrollWidth,
                averageCenterColumnWidth,
                centerColumnCount: centerCols.length,
            }, { debounceMs: SERVER_SIDE_HORIZONTAL_METRICS_DEBOUNCE_MS });
        };

        scrollEl.addEventListener('scroll', handleHorizontalScroll, { passive: true });
        return () => {
            scrollEl.removeEventListener('scroll', handleHorizontalScroll);
        };
    }, [
        averageCenterColumnWidth,
        centerCols.length,
        onHorizontalScrollMetrics,
        parentRef,
        scheduleHorizontalMetrics,
        serverSide,
    ]);

    useEffect(() => {
        if (typeof onPreciseVisibleColRange !== 'function') return;
        onPreciseVisibleColRange(centerCols.length > 0 ? visibleColRange : DEFAULT_VISIBLE_RANGE);
    }, [centerCols.length, onPreciseVisibleColRange, visibleColRange, stateEpoch]);

    return {
        columnVirtualizer,
        virtualCenterCols,
        centerTotalWidth,
        beforeWidth,
        afterWidth,
        totalLayoutWidth,
        leftCols,
        rightCols,
        centerCols,
        preciseVisibleColRange: visibleColRange,
    };
};

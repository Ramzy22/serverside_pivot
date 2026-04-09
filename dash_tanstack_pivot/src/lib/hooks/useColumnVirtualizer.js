import { useVirtualizer } from '@tanstack/react-virtual';
import {
    useEffect,
    useLayoutEffect,
    useMemo,
    useRef,
} from 'react';

const DEFAULT_VISIBLE_RANGE = { start: 0, end: 0 };

// Stable-reference helper: returns the previous array when the ids haven't changed.
const useStableColumnList = (columns) => {
    const prevRef = useRef(columns);
    const prevSignatureRef = useRef('');
    const signature = columns.map((column) => {
        const columnId = column && column.id ? column.id : '';
        const size = column && typeof column.getSize === 'function' ? column.getSize() : '';
        return `${columnId}:${size}`;
    }).join(',');
    if (signature !== prevSignatureRef.current) {
        prevRef.current = columns;
        prevSignatureRef.current = signature;
    }
    return prevRef.current;
};

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
    columnSizing,
    columns,
}) => {
    /* eslint-disable react-hooks/exhaustive-deps */
    const leftCols = useStableColumnList(
        useMemo(() => table.getLeftLeafColumns().filter(c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnSizing])
    );
    const rightCols = useStableColumnList(
        useMemo(() => table.getRightLeafColumns().filter(c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnSizing])
    );
    const centerCols = useStableColumnList(
        useMemo(() => table.getCenterLeafColumns().filter(c => c.getIsVisible()),
            [table, columns, columnVisibility, columnPinning, columnSizing])
    );
    /* eslint-enable react-hooks/exhaustive-deps */

    const horizontalOverscan = Number.isFinite(Number(columnOverscan))
        ? Math.max(0, Math.floor(Number(columnOverscan)))
        : (
            serverSide && Number.isFinite(Number(totalCenterCols)) && Number(totalCenterCols) >= 5000
                ? 1
                : 2
        );

    const columnVirtualizer = useVirtualizer({
        horizontal: true,
        count: centerCols.length,
        getScrollElement: () => parentRef.current,
        estimateSize: (index) => centerCols[index].getSize(),
        // Small overscan to avoid blank flashes during fast horizontal scroll.
        // Extremely wide column sets benefit from a tighter overscan window.
        overscan: horizontalOverscan
    });

    const rawVirtualCenterCols = columnVirtualizer.getVirtualItems();
    // TanStack can briefly report stale indices after a fast center-column count shrink
    // (e.g. collapsing a pivot group near the right edge). Filter them out so we
    // never address non-existent center cells.
    const virtualCenterCols = useMemo(
        () => rawVirtualCenterCols.filter(item => item.index >= 0 && item.index < centerCols.length),
        [rawVirtualCenterCols, centerCols.length]
    );
    const centerTotalWidth = columnVirtualizer.getTotalSize();

    // Calculate spacers for virtualized center
    const [beforeWidth, afterWidth] = useMemo(() => {
        if (virtualCenterCols.length > 0) {
            return [
                Math.max(0, virtualCenterCols[0].start),
                Math.max(0, centerTotalWidth - virtualCenterCols[virtualCenterCols.length - 1].end)
            ];
        }
        return [0, 0];
    }, [virtualCenterCols, centerTotalWidth]);

    const totalLayoutWidth = useMemo(() => {
        const leftWidth = leftCols.reduce((acc, col) => acc + col.getSize(), 0);
        const rightWidth = rightCols.reduce((acc, col) => acc + col.getSize(), 0);
        return leftWidth + centerTotalWidth + rightWidth;
    }, [leftCols, rightCols, centerTotalWidth]);

    const visibleColRange = useMemo(() => {
        if (virtualCenterCols.length === 0) return { start: 0, end: 0 };
        return {
            start: virtualCenterCols[0].index,
            end: virtualCenterCols[virtualCenterCols.length - 1].index
        };
    }, [virtualCenterCols]);

    const leftPinnedWidth = useMemo(
        () => leftCols.reduce((sum, column) => sum + column.getSize(), 0),
        [leftCols]
    );
    const averageCenterColumnWidth = useMemo(() => {
        if (centerCols.length <= 0) return estimateColumnWidth || 140;
        const totalWidth = Number(centerTotalWidth);
        if (!Number.isFinite(totalWidth) || totalWidth <= 0) {
            return estimateColumnWidth || 140;
        }
        return Math.max(48, totalWidth / centerCols.length);
    }, [centerCols.length, centerTotalWidth, estimateColumnWidth]);

    const lastVirtualCenterIndex = virtualCenterCols.length > 0
        ? virtualCenterCols[virtualCenterCols.length - 1].index
        : -1;

    useLayoutEffect(() => {
        if (!parentRef.current || !columnVirtualizer) return;
        const scrollEl = parentRef.current;
        const maxScrollLeft = Math.max(0, scrollEl.scrollWidth - scrollEl.clientWidth);
        if (scrollEl.scrollLeft > maxScrollLeft) {
            scrollEl.scrollLeft = maxScrollLeft;
            columnVirtualizer.scrollOffset = maxScrollLeft;
        }
        columnVirtualizer.measure();

        const lastCenterIndex = Math.max(centerCols.length - 1, 0);
        const atRightEdge = maxScrollLeft > 0
            && (scrollEl.scrollLeft + scrollEl.clientWidth) >= (scrollEl.scrollWidth - 1);

        if (
            atRightEdge
            && centerCols.length > 0
            && lastVirtualCenterIndex < lastCenterIndex
            && typeof columnVirtualizer.scrollToIndex === 'function'
        ) {
            columnVirtualizer.scrollToIndex(lastCenterIndex, { align: 'end' });
        }
    }, [centerCols.length, columnVirtualizer, lastVirtualCenterIndex, parentRef, totalLayoutWidth]);

    useLayoutEffect(() => {
        const scrollEl = parentRef.current;
        if (!scrollEl || !columnVirtualizer) return undefined;

        const remeasure = () => {
            columnVirtualizer.measure();
            if (typeof onHorizontalScrollMetrics === 'function' && serverSide) {
                onHorizontalScrollMetrics({
                    scrollLeft: scrollEl.scrollLeft,
                    clientWidth: scrollEl.clientWidth,
                    scrollWidth: scrollEl.scrollWidth,
                    leftPinnedWidth,
                    averageCenterColumnWidth,
                    centerColumnCount: centerCols.length,
                });
            }
        };

        remeasure();

        if (typeof ResizeObserver === 'undefined') {
            window.addEventListener('resize', remeasure);
            return () => {
                window.removeEventListener('resize', remeasure);
            };
        }

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
        leftPinnedWidth,
        onHorizontalScrollMetrics,
        parentRef,
        serverSide,
    ]);

    useEffect(() => {
        if (!serverSide || !parentRef.current || centerCols.length === 0 || typeof onHorizontalScrollMetrics !== 'function') {
            return;
        }

        const scrollEl = parentRef.current;
        const handleHorizontalScroll = () => {
            onHorizontalScrollMetrics({
                scrollLeft: scrollEl.scrollLeft,
                clientWidth: scrollEl.clientWidth,
                scrollWidth: scrollEl.scrollWidth,
                leftPinnedWidth,
                averageCenterColumnWidth,
                centerColumnCount: centerCols.length,
            });
        };

        scrollEl.addEventListener('scroll', handleHorizontalScroll, { passive: true });
        return () => {
            scrollEl.removeEventListener('scroll', handleHorizontalScroll);
        };
    }, [
        averageCenterColumnWidth,
        centerCols.length,
        leftPinnedWidth,
        onHorizontalScrollMetrics,
        parentRef,
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

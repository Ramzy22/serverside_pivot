import { useVirtualizer } from '@tanstack/react-virtual';
import { useMemo } from 'react';

export const useColumnVirtualizer = ({
    parentRef,
    table,
    estimateColumnWidth
}) => {
    const leftCols = table.getLeftLeafColumns().filter(c => c.getIsVisible());
    const rightCols = table.getRightLeafColumns().filter(c => c.getIsVisible());
    const centerCols = table.getCenterLeafColumns().filter(c => c.getIsVisible());

    const columnVirtualizer = useVirtualizer({
        horizontal: true,
        count: centerCols.length,
        getScrollElement: () => parentRef.current,
        estimateSize: (index) => centerCols[index].getSize(),
        // Strict visible-only column virtualization: no horizontal prefetch.
        overscan: 0
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

    return {
        columnVirtualizer,
        virtualCenterCols,
        beforeWidth,
        afterWidth,
        totalLayoutWidth,
        leftCols,
        rightCols,
        centerCols,
        visibleColRange
    };
};

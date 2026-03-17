import { useMemo, useCallback } from 'react';

const BODY_PINNED_Z_INDEX = 10;
const BODY_PINNED_EDGE_Z_INDEX = 11;
const HEADER_PINNED_Z_INDEX = 30;
const HEADER_PINNED_EDGE_Z_INDEX = 31;

const getLeafColumns = (column) => {
    if (!column) return [];
    if (typeof column.getLeafColumns === 'function') {
        return column.getLeafColumns();
    }
    if (Array.isArray(column.columns) && column.columns.length > 0) {
        return column.columns.flatMap((child) => getLeafColumns(child));
    }
    return [column];
};

const getPinnedLeafColumns = (column, side) =>
    getLeafColumns(column).filter((leaf) => (leaf && typeof leaf.getIsPinned === 'function' ? leaf.getIsPinned() : undefined) === side);

const getOffsetLeaf = (column, side) => {
    const pinnedLeaves = getPinnedLeafColumns(column, side);
    if (pinnedLeaves.length === 0) return null;
    return side === 'left' ? pinnedLeaves[0] : pinnedLeaves[pinnedLeaves.length - 1];
};

const getBoundaryLeaf = (column, side) => {
    const pinnedLeaves = getPinnedLeafColumns(column, side);
    if (pinnedLeaves.length === 0) return null;
    return side === 'left' ? pinnedLeaves[pinnedLeaves.length - 1] : pinnedLeaves[0];
};

const useStickyStyles = (theme, leftCols, rightCols) => {
    const stickyOffsets = useMemo(() => {
        const offsets = {};
        let leftAcc = 0;
        leftCols.forEach(col => {
            offsets[col.id] = leftAcc;
            leftAcc += col.getSize();
        });

        let rightAcc = 0;
        // Slice and reverse to calculate right offsets from the edge
        [...rightCols].reverse().forEach(col => {
            offsets[col.id] = rightAcc;
            rightAcc += col.getSize();
        });
        
        return offsets;
    }, [leftCols, rightCols]);

    const resolvePinnedSide = useCallback((column, renderSection) => {
        const pinState = column && typeof column.getIsPinned === 'function' ? column.getIsPinned() : undefined;
        if (pinState === 'left' || pinState === 'right') {
            return pinState;
        }
        if (renderSection === 'left' || renderSection === 'right') {
            return getPinnedLeafColumns(column, renderSection).length > 0 ? renderSection : false;
        }
        return false;
    }, []);

    const hasBoundary = useCallback((column, side) => {
        const boundaryLeaf = getBoundaryLeaf(column, side);
        if (!boundaryLeaf) return false;

        const boundaryByApi = side === 'left'
            ? (typeof boundaryLeaf.getIsLastColumn === 'function' ? boundaryLeaf.getIsLastColumn('left') : undefined)
            : (typeof boundaryLeaf.getIsFirstColumn === 'function' ? boundaryLeaf.getIsFirstColumn('right') : undefined);
        if (typeof boundaryByApi === 'boolean') return boundaryByApi;

        return side === 'left'
            ? (leftCols[leftCols.length - 1] ? leftCols[leftCols.length - 1].id === boundaryLeaf.id : false)
            : (rightCols[0] ? rightCols[0].id === boundaryLeaf.id : false);
    }, [leftCols, rightCols]);

    const getBoundaryStyle = useCallback((side) => ({
        boxShadow: side === 'left'
            ? `2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
            : `-2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`,
        borderRight: side === 'left' ? `1px solid ${theme.border}` : undefined,
        borderLeft: side === 'right' ? `1px solid ${theme.border}` : undefined
    }), [theme.border, theme.pinnedBoundaryShadow]);

    const getStickyStyle = useCallback((column, bg) => {
        const pinnedSide = resolvePinnedSide(column);
        if (!pinnedSide) return { background: bg || theme.background };

        const offsetLeaf = getOffsetLeaf(column, pinnedSide) || column;
        const offset = stickyOffsets[offsetLeaf.id] || 0;
        return {
            position: 'sticky',
            left: pinnedSide === 'left' ? `${offset}px` : undefined,
            right: pinnedSide === 'right' ? `${offset}px` : undefined,
            zIndex: hasBoundary(column, pinnedSide) ? BODY_PINNED_EDGE_Z_INDEX : BODY_PINNED_Z_INDEX,
            background: bg || theme.background // Important for sticky to cover scrolled content
            ,
            ...(hasBoundary(column, pinnedSide) ? getBoundaryStyle(pinnedSide) : {})
        };
    }, [stickyOffsets, theme, resolvePinnedSide, hasBoundary, getBoundaryStyle]);

    const getHeaderStickyStyle = useCallback((header, level, renderSection = 'center', backgroundOverride) => {
        const column = header.column;
        const pinnedSide = resolvePinnedSide(column, renderSection);
        const background = backgroundOverride || theme.headerBg;

        if (!pinnedSide) return { background };

        const offsetLeaf = getOffsetLeaf(column, pinnedSide) || column;
        const offset = stickyOffsets[offsetLeaf.id] || 0;
        const edgeBoundary = hasBoundary(column, pinnedSide);

        const style = {
            position: 'sticky',
            left: pinnedSide === 'left' ? `${offset}px` : undefined,
            right: pinnedSide === 'right' ? `${offset}px` : undefined,
            zIndex: edgeBoundary ? HEADER_PINNED_EDGE_Z_INDEX : HEADER_PINNED_Z_INDEX,
            background
        };

        if (edgeBoundary) {
            Object.assign(style, getBoundaryStyle(pinnedSide));
        }

        return style;
    }, [stickyOffsets, theme, resolvePinnedSide, hasBoundary, getBoundaryStyle]);

    return { getHeaderStickyStyle, getStickyStyle };
};

export default useStickyStyles;

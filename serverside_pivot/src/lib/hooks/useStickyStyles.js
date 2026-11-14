import { useMemo, useCallback } from 'react';

const BODY_PINNED_Z_INDEX = 60;
const BODY_PINNED_EDGE_Z_INDEX = 61;
const HEADER_PINNED_Z_INDEX = 110;
const HEADER_PINNED_EDGE_Z_INDEX = 111;

/** @param {any} [column] @returns {any} */
const getLeafColumns = (column) => {
    if (!column) return [];
    if (typeof column.getLeafColumns === 'function') {
        return column.getLeafColumns();
    }
    if (Array.isArray(column.columns) && column.columns.length > 0) {
        return column.columns.flatMap(/** @param {any} [child] @returns {any} */ (child) => getLeafColumns(child));
    }
    return [column];
};

/** @param {any} [column] @param {any} [side] @returns {any} */
const getPinnedLeafColumns = (column, side) =>
    getLeafColumns(column).filter(/** @param {any} [leaf] @returns {any} */ (leaf) => (leaf && typeof leaf.getIsPinned === 'function' ? leaf.getIsPinned() : undefined) === side);

/** @param {any} [column] @param {any} [side] @returns {any} */
const getOffsetLeaf = (column, side) => {
    /** @type {any} */
    const pinnedLeaves = getPinnedLeafColumns(column, side);
    if (pinnedLeaves.length === 0) return null;
    return side === 'left' ? pinnedLeaves[0] : pinnedLeaves[pinnedLeaves.length - 1];
};

/** @param {any} [column] @param {any} [side] @returns {any} */
const getBoundaryLeaf = (column, side) => {
    /** @type {any} */
    const pinnedLeaves = getPinnedLeafColumns(column, side);
    if (pinnedLeaves.length === 0) return null;
    return side === 'left' ? pinnedLeaves[pinnedLeaves.length - 1] : pinnedLeaves[0];
};

/** @param {any} [theme] @param {any} [leftCols] @param {any} [rightCols] @returns {any} */
const useStickyStyles = (theme, leftCols, rightCols) => {
    /** @type {any} */
    const stickyOffsets = useMemo(() => {
        /** @type {Record<string, any>} */
        const offsets = {};
        let leftAcc = 0;
        leftCols.forEach(/** @param {any} [col] @returns {any} */ col => {
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

    /** @type {(...args: any[]) => any} */
    const resolvePinnedSide = useCallback((column, renderSection) => {
        /** @type {any} */
        const pinState = column && typeof column.getIsPinned === 'function' ? column.getIsPinned() : undefined;
        if (pinState === 'left' || pinState === 'right') {
            return pinState;
        }
        if (renderSection === 'left' || renderSection === 'right') {
            return getPinnedLeafColumns(column, renderSection).length > 0 ? renderSection : false;
        }
        return false;
    }, []);

    /** @type {(...args: any[]) => any} */
    const hasBoundary = useCallback((column, side) => {
        /** @type {any} */
        const boundaryLeaf = getBoundaryLeaf(column, side);
        if (!boundaryLeaf) return false;

        /** @type {any} */
        const boundaryByApi = side === 'left'
            ? (typeof boundaryLeaf.getIsLastColumn === 'function' ? boundaryLeaf.getIsLastColumn('left') : undefined)
            : (typeof boundaryLeaf.getIsFirstColumn === 'function' ? boundaryLeaf.getIsFirstColumn('right') : undefined);
        if (typeof boundaryByApi === 'boolean') return boundaryByApi;

        return side === 'left'
            ? (leftCols[leftCols.length - 1] ? leftCols[leftCols.length - 1].id === boundaryLeaf.id : false)
            : (rightCols[0] ? rightCols[0].id === boundaryLeaf.id : false);
    }, [leftCols, rightCols]);

    /** @type {(...args: any[]) => any} */
    const getBoundaryStyle = useCallback((side) => ({
        boxShadow: side === 'left'
            ? `2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
            : `-2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`,
        borderRight: side === 'left' ? `1px solid ${theme.border}` : undefined,
        borderLeft: side === 'right' ? `1px solid ${theme.border}` : undefined
    }), [theme.border, theme.pinnedBoundaryShadow]);

    /** @type {(...args: any[]) => any} */
    const getStickyStyle = useCallback((column, bg) => {
        /** @type {any} */
        const pinnedSide = resolvePinnedSide(column);
        if (!pinnedSide) return { background: bg || theme.background };

        /** @type {any} */
        const offsetLeaf = getOffsetLeaf(column, pinnedSide) || column;
        /** @type {any} */
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

    /** @type {(...args: any[]) => any} */
    const getHeaderStickyStyle = useCallback((header, level, renderSection = 'center', backgroundOverride) => {
        const column = header.column;
        /** @type {any} */
        const pinnedSide = resolvePinnedSide(column, renderSection);
        /** @type {any} */
        const background = backgroundOverride || theme.headerBg;

        if (!pinnedSide) return { background };

        /** @type {any} */
        const offsetLeaf = getOffsetLeaf(column, pinnedSide) || column;
        /** @type {any} */
        const offset = stickyOffsets[offsetLeaf.id] || 0;
        /** @type {any} */
        const edgeBoundary = hasBoundary(column, pinnedSide);

        /** @type {Record<string, any>} */
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

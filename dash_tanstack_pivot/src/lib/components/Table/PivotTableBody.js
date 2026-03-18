import React from 'react';
import SkeletonRow from '../SkeletonRow';
import StatusBar from './StatusBar';

const getTotalRowBackground = (theme) =>
    theme.totalBg || theme.select || theme.background;

/**
 * PivotTableBody — the main scroll container and virtual-scroll table body.
 *
 * Contains: sticky header (left/center/right), top-pinned rows, virtualized
 * center rows (with skeleton, expand/collapse loaders), bottom-pinned rows,
 * floating filters, column-loading skeletons, and the StatusBar.
 */
export function PivotTableBody({
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
    // Table instance / column groups
    table,
    leftCols,
    centerCols,
    rightCols,
    centerColIndexMap,
    visibleLeafIndexSet,
    // Display options
    rowHeight,
    showFloatingFilters,
    stickyHeaders,
    showColTotals,
    grandTotalPosition,
    showColumnLoadingSkeletons,
    pendingColumnSkeletonCount,
    columnSkeletonWidth,
    // Theme / styles
    theme,
    styles,
    // Render helpers
    renderCell,
    renderHeaderCell,
    // Filter interaction
    filters,
    handleHeaderFilter,
    // Status bar
    selectedCells,
    rowCount,
    isRequestPending,
}) {
    const pinnedRowIdSet = React.useMemo(() => {
        const ids = new Set();
        [...effectiveTopRows, ...effectiveBottomRows].forEach((row) => {
            if (row && row.id) ids.add(row.id);
        });
        return ids;
    }, [effectiveTopRows, effectiveBottomRows]);
    const pinnedVirtualIndexes = React.useMemo(() => {
        if (!serverSide) return [];
        const indexes = new Set();
        [...effectiveTopRows, ...effectiveBottomRows].forEach((row) => {
            const virtualIndex = row && row.original && typeof row.original.__virtualIndex === 'number'
                ? row.original.__virtualIndex
                : null;
            if (virtualIndex !== null) indexes.add(virtualIndex);
        });
        return Array.from(indexes).sort((a, b) => a - b);
    }, [serverSide, effectiveTopRows, effectiveBottomRows]);
    const pinnedVirtualIndexSet = React.useMemo(
        () => new Set(pinnedVirtualIndexes),
        [pinnedVirtualIndexes]
    );
    const getPinnedRowsBefore = React.useCallback((virtualIndex) => {
        if (!serverSide || pinnedVirtualIndexes.length === 0) return 0;
        let count = 0;
        for (const pinnedIndex of pinnedVirtualIndexes) {
            if (pinnedIndex >= virtualIndex) break;
            count += 1;
        }
        return count;
    }, [serverSide, pinnedVirtualIndexes]);

    const centerRowLookup = React.useMemo(() => {
        const lookup = new Map();
        effectiveCenterRows.forEach((row) => {
            if (row && row.id) lookup.set(row.id, row);
        });
        return lookup;
    }, [effectiveCenterRows]);

    const leftPinnedWidth = React.useMemo(
        () => leftCols.reduce((sum, column) => sum + column.getSize(), 0),
        [leftCols]
    );
    const rightPinnedWidth = React.useMemo(
        () => rightCols.reduce((sum, column) => sum + column.getSize(), 0),
        [rightCols]
    );
    const visibleCenterWidth = React.useMemo(
        () => virtualCenterCols.reduce((sum, virtualCol) => {
            const centerColumn = centerCols[virtualCol.index];
            return sum + (centerColumn ? centerColumn.getSize() : 0);
        }, 0),
        [virtualCenterCols, centerCols]
    );

    const getRenderedRowIndex = (row, fallbackIndex = 0) => {
        if (row && typeof row.index === 'number') return row.index;
        if (row && row.original && typeof row.original.__virtualIndex === 'number') {
            return row.original.__virtualIndex;
        }
        return fallbackIndex;
    };

    const topRowCount = effectiveTopRows.length;
    const centerRowCount = effectiveCenterRows.length;
    const virtualBodyHeight = Math.max(
        rowVirtualizer.getTotalSize() - (pinnedVirtualIndexes.length * rowHeight),
        0
    );
    const centerDisplayIndexById = React.useMemo(() => {
        const lookup = new Map();
        effectiveCenterRows.forEach((row, index) => {
            if (row && row.id) lookup.set(row.id, topRowCount + index);
        });
        return lookup;
    }, [effectiveCenterRows, topRowCount]);

    const getVisibleCellMap = (row) => {
        if (!row) return new Map();
        const visibleCells = typeof row.getVisibleCells === 'function'
            ? row.getVisibleCells()
            : [
                ...(typeof row.getLeftVisibleCells === 'function' ? row.getLeftVisibleCells() : []),
                ...(typeof row.getCenterVisibleCells === 'function' ? row.getCenterVisibleCells() : []),
                ...(typeof row.getRightVisibleCells === 'function' ? row.getRightVisibleCells() : []),
            ];
        return new Map(visibleCells.map((cell) => [cell.column.id, cell]));
    };

    const renderCenterVirtualCells = (row, virtualRowIndex, isVirtualRow, cellById) => {
        const resolvedCellById = cellById || getVisibleCellMap(row);

        return virtualCenterCols.map(virtualCol => {
            const centerColumn = centerCols[virtualCol.index];
            if (!centerColumn) return null;
            const cell = resolvedCellById.get(centerColumn.id) || null;
            return renderCell(cell, virtualRowIndex, isVirtualRow, { disableSticky: true });
        });
    };

    const renderPinnedSectionCells = (row, columns, virtualRowIndex, isVirtualRow, cellById) =>
        columns.map((column) =>
            renderCell(cellById.get(column.id) || null, virtualRowIndex, isVirtualRow, { disableSticky: true })
        );

    const renderSkeletonSections = (rowBackground, skeletonStyle = {}) => (
        <>
            {leftPinnedWidth > 0 && (
                <div
                    style={{
                        display: 'flex',
                        width: leftPinnedWidth,
                        minWidth: leftPinnedWidth,
                        flexShrink: 0,
                        position: 'sticky',
                        left: 0,
                        zIndex: 72,
                        background: rowBackground,
                        boxShadow: `2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
                    }}
                >
                    <SkeletonRow style={{ width: '100%', ...skeletonStyle }} rowHeight={rowHeight} />
                </div>
            )}
            <div style={{ display: 'flex', flexShrink: 0, background: rowBackground }}>
                <div style={{ width: beforeWidth, flexShrink: 0 }} />
                <SkeletonRow
                    style={{
                        width: `${visibleCenterWidth}px`,
                        minWidth: `${visibleCenterWidth}px`,
                        flexShrink: 0,
                        ...skeletonStyle
                    }}
                    rowHeight={rowHeight}
                />
                <div style={{ width: afterWidth, flexShrink: 0 }} />
            </div>
            {rightPinnedWidth > 0 && (
                <div
                    style={{
                        display: 'flex',
                        width: rightPinnedWidth,
                        minWidth: rightPinnedWidth,
                        flexShrink: 0,
                        position: 'sticky',
                        right: 0,
                        zIndex: 72,
                        background: rowBackground,
                        boxShadow: `-2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
                    }}
                >
                    <SkeletonRow style={{ width: '100%', ...skeletonStyle }} rowHeight={rowHeight} />
                </div>
            )}
        </>
    );

    const renderRowSections = (row, virtualRowIndex, isVirtualRow, renderOptions = {}) => {
        const rowBackground = renderOptions.rowBackground || (theme.surfaceBg || theme.background);
        const disablePinnedColumnStickiness = !!renderOptions.disablePinnedColumnStickiness;
        const cellById = getVisibleCellMap(row);

        return (
            <>
                {leftPinnedWidth > 0 && (
                    <div
                        style={{
                            display: 'flex',
                            width: leftPinnedWidth,
                            minWidth: leftPinnedWidth,
                            flexShrink: 0,
                            position: disablePinnedColumnStickiness ? 'relative' : 'sticky',
                            left: disablePinnedColumnStickiness ? undefined : 0,
                            zIndex: disablePinnedColumnStickiness ? undefined : 72,
                            background: rowBackground,
                            boxShadow: disablePinnedColumnStickiness
                                ? 'none'
                                : `2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
                        }}
                    >
                        {leftCols.map((column) =>
                            renderCell(cellById.get(column.id) || null, virtualRowIndex, isVirtualRow, { disableSticky: true })
                        )}
                    </div>
                )}
                <div style={{ display: 'flex', flexShrink: 0, background: rowBackground }}>
                    <div style={{ width: beforeWidth, flexShrink: 0 }} />
                    {renderCenterVirtualCells(row, virtualRowIndex, isVirtualRow, cellById)}
                    <div style={{ width: afterWidth, flexShrink: 0 }} />
                </div>
                {rightPinnedWidth > 0 && (
                    <div
                        style={{
                            display: 'flex',
                            width: rightPinnedWidth,
                            minWidth: rightPinnedWidth,
                            flexShrink: 0,
                            position: disablePinnedColumnStickiness ? 'relative' : 'sticky',
                            right: disablePinnedColumnStickiness ? undefined : 0,
                            zIndex: disablePinnedColumnStickiness ? undefined : 72,
                            background: rowBackground,
                            boxShadow: disablePinnedColumnStickiness
                                ? 'none'
                                : `-2px 0 5px -2px ${theme.pinnedBoundaryShadow || 'rgba(0,0,0,0.2)'}`
                        }}
                    >
                        {rightCols.map((column) =>
                            renderCell(cellById.get(column.id) || null, virtualRowIndex, isVirtualRow, { disableSticky: true })
                        )}
                    </div>
                )}
            </>
        );
    };

    const renderPinnedRowGroup = (rowsForPin, pinPosition) => {
        if (!rowsForPin || rowsForPin.length === 0) return null;
        const isTop = pinPosition === 'top';
        const stickyOffset = isTop ? (stickyHeaders ? stickyHeaderHeight : 0) : 0;
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
                {rowsForPin.map((row, index) => {
                    const isEdgeRow = isTop ? index === rowsForPin.length - 1 : index === 0;
                    const displayRowIndex = isTop ? index : topRowCount + centerRowCount + index;
                    const rowBackground = theme.surfaceBg || theme.background;
                    const cellById = getVisibleCellMap(row);
                    const extraShadow = isEdgeRow
                        ? (isTop ? `0 2px 4px -2px ${theme.border}80` : `0 -4px 6px -1px rgba(15,23,42,0.06)`)
                        : 'none';
                    return (
                        <div
                            key={`${pinPosition}_${row.id}`}
                            role="row"
                            style={{
                                ...styles.row,
                                position: 'relative',
                                width: `${totalLayoutWidth}px`,
                                minWidth: `${totalLayoutWidth}px`,
                                height: rowHeight,
                                background: rowBackground,
                                borderBottom: `1px solid ${theme.border}`,
                                boxShadow: extraShadow,
                            }}
                        >
                            {leftCols.map((column) =>
                                renderCell(cellById.get(column.id) || null, displayRowIndex, false)
                            )}
                            <div style={{ width: beforeWidth, flexShrink: 0 }} />
                            {renderCenterVirtualCells(row, displayRowIndex, false, cellById)}
                            <div style={{ width: afterWidth, flexShrink: 0 }} />
                            {rightCols.map((column) =>
                                renderCell(cellById.get(column.id) || null, displayRowIndex, false)
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
                     height: `${virtualBodyHeight + stickyHeaderHeight + (effectiveTopRows.length + effectiveBottomRows.length) * rowHeight}px`,
                     position: 'relative',
                     borderRadius: `0 0 ${theme.radius || '16px'} ${theme.radius || '16px'}`,
                     overflow: 'visible'
                 }}>
                     {/* Sticky Header */}
                     <div
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
                             {table.getLeftHeaderGroups().map((group, level) => (
                                     <div key={group.id} style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`}}>
                                     {group.headers.map((header) => renderHeaderCell(header, level, 'left', null, true))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`, background: theme.background}}>
                                     {leftCols.map((column, idx) => (
                                         <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: rowHeight, padding: '2px 4px', borderRight: idx === leftCols.length - 1 ? `1px solid ${theme.border}` : 'none'}}>
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
                         <div style={{position: 'relative'}}>
                             {table.getCenterHeaderGroups().map((group, level) => {
                                 // Virtualize center headers: only render headers whose leaf
                                 // columns overlap the visible virtual column range, plus spacers.
                                 // centerColIndexMap and visibleLeafIndexSet are memoized above the render.
                                 const visibleHeaders = [];
                                 for (const header of group.headers) {
                                     const leafCols = header.column.getLeafColumns
                                         ? header.column.getLeafColumns()
                                         : [header.column];
                                     const centerLeafPairs = leafCols
                                         .map(lc => ({ col: lc, idx: centerColIndexMap.has(lc.id) ? centerColIndexMap.get(lc.id) : -1 }))
                                         .filter(p => p.idx >= 0);
                                     if (centerLeafPairs.length === 0) continue;
                                     const visiblePairs = centerLeafPairs.filter(p => visibleLeafIndexSet.has(p.idx));
                                     if (visiblePairs.length === 0) continue;
                                     const visWidth = visiblePairs.reduce((sum, p) => sum + p.col.getSize(), 0);
                                     visibleHeaders.push({ header, visWidth });
                                 }
                                 return (
                                     <div key={group.id} style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`}}>
                                         <div style={{ width: beforeWidth, flexShrink: 0 }} />
                                         {visibleHeaders.map(({ header, visWidth }) =>
                                             renderHeaderCell(header, level, 'center', visWidth)
                                         )}
                                         <div style={{ width: afterWidth, flexShrink: 0 }} />
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
                                         height: rowHeight,
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
                                 <div style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`, background: theme.background}}>
                                     <div style={{ width: beforeWidth, flexShrink: 0 }} />
                                     {virtualCenterCols.map(virtualCol => {
                                         const column = centerCols[virtualCol.index];
                                         if (!column) return null;
                                         return (
                                             <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: rowHeight, padding: '2px 4px'}}>
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
                                     <div style={{ width: afterWidth, flexShrink: 0 }} />
                                 </div>
                             )}
                         </div>

                         {/* Right Section */}
                         <div style={{position: 'sticky', right: 0, zIndex: 4, background: theme.headerBg}}>
                             {table.getRightHeaderGroups().map((group, level) => (
                                 <div key={group.id} style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`}}>
                                     {group.headers.map((header) => renderHeaderCell(header, level, 'right', null, true))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`, background: theme.background}}>
                                     {rightCols.map((column, idx) => (
                                         <div key={column.id} style={{...styles.headerCell, width: column.getSize(), height: rowHeight, padding: '2px 4px', borderLeft: idx === 0 ? `1px solid ${theme.border}` : 'none'}}>
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

                     {showColumnLoadingSkeletons && (
                         <div
                             aria-hidden="true"
                                style={{
                                    position: 'absolute',
                                    top: `${bodyRowsTopOffset}px`,
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
                     {virtualRows.map(virtualRow => {
                          const topOffset = bodyRowsTopOffset;
                          const adjustedTop = virtualRow.start - (getPinnedRowsBefore(virtualRow.index) * rowHeight) + topOffset;

                          let row;

                          if (serverSide) {
                              if (pinnedVirtualIndexSet.has(virtualRow.index)) {
                                  return null;
                              }

                              // 1. Fetch Data Directly from Cache (Source of Truth)
                              const cachedData = getRow(virtualRow.index);

                              if (!cachedData) {
                                  // Data not loaded yet -> Skeleton
                                 return (
                                     <div
                                        key={`skeleton_${virtualRow.index}`}
                                        style={{
                                            ...styles.row,
                                            height: virtualRow.size,
                                            top: `${adjustedTop}px`,
                                            width: `${totalLayoutWidth}px`,
                                            position: 'absolute',
                                            background: theme.surfaceBg || theme.background,
                                            borderBottom: `1px solid ${theme.border}`,
                                            display: 'flex', alignItems: 'center',
                                            zIndex: 90,
                                            pointerEvents: 'none'
                                        }}>
                                            {renderSkeletonSections(theme.surfaceBg || theme.background)}
                                        </div>                                 );
                             }

                             if (
                                 serverSidePinsGrandTotal &&
                                 (cachedData._isTotal || cachedData._path === '__grand_total__' || cachedData._id === 'Grand Total')
                             ) {
                                 return null;
                             }

                             // 2. Resolve Row Object via ID (Decoupled from Index)
                             // We reconstruct the ID exactly as getRowId does, but using the global index directly.
                             // Global Index = virtualRow.index
                             let rowId;
                             if (cachedData._isTotal || cachedData._path === '__grand_total__' || cachedData._id === 'Grand Total') {
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
                             const cachedPath = cachedData._isTotal ? '__grand_total__' : (cachedData._path || rowId);
                             const rowPath = row && row.original
                                 ? (row.original._isTotal ? '__grand_total__' : (row.original._path || row.id))
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
                               return (
                                 <div
                                    key={`skeleton_wait_${virtualRow.index}`}
                                    style={{
                                     ...styles.row,
                                     height: virtualRow.size,
                                     top: `${adjustedTop}px`,
                                     width: `${totalLayoutWidth}px`,
                                     position: 'absolute',
                                     background: (row && row.original && row.original._isTotal) ? getTotalRowBackground(theme) : (theme.surfaceBg || theme.background),
                                     borderBottom: `1px solid ${theme.border}`,
                                     display: 'flex', alignItems: 'center',
                                     zIndex: 90,
                                     pointerEvents: 'none'
                                 }}>
                                     {renderSkeletonSections(
                                         (row && row.original && row.original._isTotal)
                                             ? getTotalRowBackground(theme)
                                             : (theme.surfaceBg || theme.background)
                                     )}
                                 </div>
                             );
                         }

                         // 5. Feature: Hide Totals if requested (Server Side only workaround)
                         if (serverSide && !showColTotals && row.original._isTotal) {
                             return null;
                         }

                          const pendingTransitionMode = pendingRowTransitions.get(row.id);
                          const showRowTransitionLoader = !!pendingTransitionMode;

                          // Stable key: use row path/id for loaded rows so expand/collapse
                          // does not remount rows that merely shifted index (AG Grid getRowId pattern).
                          const stableRowKey = serverSide
                              ? (row.id || String(virtualRow.index))
                              : String(virtualRow.index);
                          const displayRowIndex = centerDisplayIndexById.get(row.id) ?? (topRowCount + virtualRow.index);

                          return (
                              <React.Fragment key={stableRowKey}>
                                  <div
                                     role="row"
                                     aria-rowindex={virtualRow.index}
                                     style={{
                                      ...styles.row,
                                      height: virtualRow.size,
                                      top: `${adjustedTop}px`,
                                     width: `${totalLayoutWidth}px`,
                                      background: (row.original && row.original._isTotal) ? getTotalRowBackground(theme) : (theme.surfaceBg || theme.background),
                                      borderBottom: `1px solid ${theme.border}`,
                                      transition: rowVirtualizer.isScrolling ? 'none' : 'background-color 0.2s'
                                   }}>
                                      {renderRowSections(row, displayRowIndex, true, {
                                          rowBackground: (row.original && row.original._isTotal)
                                              ? getTotalRowBackground(theme)
                                              : (theme.surfaceBg || theme.background)
                                      })}
                                  </div>
                                  {showRowTransitionLoader && (
                                      <div
                                         role="row"
                                         aria-hidden="true"
                                         style={{
                                          ...styles.row,
                                          pointerEvents: 'none',
                                          height: rowHeight,
                                          top: `${adjustedTop + virtualRow.size}px`,
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
                                          animation: 'pivot-row-loader-enter 220ms ease-out, pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                                      }}>
                                          {renderSkeletonSections(
                                              'var(--pivot-loading-row-gradient, linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%))',
                                              { opacity: 0.45 }
                                          )}
                                          <div
                                             style={{
                                              position: 'absolute',
                                              zIndex: 2,
                                              paddingLeft: `${((row.original && typeof row.original.depth === 'number' ? row.original.depth : row.depth || 0) + 1) * 24 + 8}px`,
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
            <StatusBar
                selectedCells={selectedCells}
                rowCount={rowCount}
                visibleRowsCount={rows.length}
                theme={theme}
                isLoading={isRequestPending}
            />
        </div>
    );
}



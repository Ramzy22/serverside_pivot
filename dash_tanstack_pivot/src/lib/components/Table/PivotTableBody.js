import React from 'react';
import SkeletonRow from '../SkeletonRow';
import StatusBar from './StatusBar';
import InlineDetailPanel from './InlineDetailPanel';
import { usePivotTheme } from '../../contexts/PivotThemeContext';
import { usePivotConfig } from '../../contexts/PivotConfigContext';
import { buildRowSpanPlan, collectRowSpanColumns } from '../../utils/rowSpanning';
import { getPivotPerformanceNow, recordPivotMeasure } from '../../utils/pivotProfiler';
import { usePivotRenderCounter } from '../../hooks/usePivotRenderCounter';

const getTotalRowBackground = (theme) =>
    theme.totalBg || theme.select || theme.background;

const getLeafColumnIds = (column, leafIdCache) => {
    if (!column) return [];
    const cacheKey = column.id || column;
    if (leafIdCache.has(cacheKey)) return leafIdCache.get(cacheKey);

    const leafIds = [];
    const stack = [column];
    while (stack.length > 0) {
        const current = stack.pop();
        if (!current) continue;
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
        column.getLeafColumns().forEach((leafColumn) => {
            if (leafColumn && leafColumn.id) leafIds.push(leafColumn.id);
        });
    }
    if (leafIds.length === 0 && column.id) {
        leafIds.push(column.id);
    }

    leafIdCache.set(cacheKey, leafIds);
    return leafIds;
};

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
 */
export const PivotTableBody = React.memo(function PivotTableBody({
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
    const { theme, styles } = usePivotTheme();
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
    // centerHeaderGroups are passed as a pre-computed prop from the parent
    // to avoid passing the unstable `table` instance which defeats React.memo.
    const centerHeaderGroups = centerHeaderGroupsProp;
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
    const centerColumnWidthPrefix = React.useMemo(() => {
        const prefix = new Array(centerCols.length + 1);
        prefix[0] = 0;
        for (let index = 0; index < centerCols.length; index += 1) {
            const column = centerCols[index];
            prefix[index + 1] = prefix[index] + (column ? column.getSize() : 0);
        }
        return prefix;
    }, [centerCols, totalLayoutWidth]);

    const headerLeafPairsMap = React.useMemo(() => {
        const startedAt = getPivotPerformanceNow();
        const map = new Map();
        const leafIdCache = new Map();
        for (const group of centerHeaderGroups) {
            for (const header of group.headers) {
                const leafIds = getLeafColumnIds(header.column, leafIdCache);
                const indices = [];
                for (let i = 0; i < leafIds.length; i += 1) {
                    const leafId = leafIds[i];
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

    // ── Memoized base styles to avoid creating new objects per row/render ──
    const baseRowStyle = React.useMemo(() => ({
        ...styles.row,
        width: `${totalLayoutWidth}px`,
        borderBottom: `1px solid ${theme.border}`,
    }), [styles.row, totalLayoutWidth, theme.border]);

    const skeletonRowStyle = React.useMemo(() => ({
        ...baseRowStyle,
        position: 'absolute',
        display: 'flex',
        alignItems: 'center',
        zIndex: 90,
        pointerEvents: 'none',
    }), [baseRowStyle]);

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

    const defaultRowBg = theme.surfaceBg || theme.background;
    const totalRowBg = getTotalRowBackground(theme);

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

    const centerFlexStyle = React.useMemo(() => ({
        display: 'flex', flexShrink: 0,
    }), []);

    const beforeSpacerStyle = React.useMemo(() => ({
        width: beforeWidth, flexShrink: 0,
    }), [beforeWidth]);

    const afterSpacerStyle = React.useMemo(() => ({
        width: afterWidth, flexShrink: 0,
    }), [afterWidth]);

    const headerRowStyle = React.useMemo(() => ({
        display: 'flex', height: rowHeight, borderBottom: `1px solid ${theme.border}`,
    }), [rowHeight, theme.border]);

    const centerHeaderRenderPlan = React.useMemo(
        () => {
            const startedAt = getPivotPerformanceNow();
            const plan = centerHeaderGroups.map((group) => {
                const visibleHeaders = [];
                for (const header of group.headers) {
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
                        const visibleStart = Math.max(centerLeafEntry.minIdx, visibleCenterRange.start);
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

    const resolveVirtualRow = React.useCallback((virtualIndex) => {
        if (serverSide) {
            const cachedData = getRow(virtualIndex);
            if (!cachedData) return null;
            const rowId = (cachedData._isTotal || cachedData._path === '__grand_total__' || cachedData._id === 'Grand Total')
                ? '__grand_total__'
                : (cachedData._path || (cachedData.id ? cachedData.id : String(virtualIndex)));
            return centerRowLookup.get(rowId) || null;
        }
        return effectiveCenterRows[virtualIndex] || null;
    }, [centerRowLookup, effectiveCenterRows, getRow, serverSide]);

    const rowSpanColumns = React.useMemo(() => {
        if (layoutMode !== 'tabular') return [];
        return collectRowSpanColumns([...leftCols, ...centerCols, ...rightCols]);
    }, [layoutMode, leftCols, centerCols, rightCols]);
    const rowSpanEnabled = rowSpanColumns.length > 0;
    const rowSpanRowEntries = React.useMemo(() => {
        if (!rowSpanEnabled) return [];
        return virtualRows.reduce((entries, virtualRow) => {
            if (serverSide && pinnedVirtualIndexSet.has(virtualRow.index)) return entries;
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
    const rowSpanPlan = React.useMemo(
        () => buildRowSpanPlan({ rowEntries: rowSpanRowEntries, rowSpanColumns }),
        [rowSpanRowEntries, rowSpanColumns]
    );
    const getCellRenderOptions = React.useCallback((column, renderOptions = {}) => {
        const rowSpanByColumnId = renderOptions.rowSpanByColumnId || null;
        const rowSpan = rowSpanByColumnId ? rowSpanByColumnId.get(column.id) : null;
        return rowSpan ? { disableSticky: true, rowSpan } : { disableSticky: true };
    }, []);

    const stickyTreeRow = React.useMemo(() => {
        if (viewMode !== 'tree' || treeSuppressGroupRowsSticky || virtualRows.length === 0) return null;
        const firstVisibleRow = virtualRows
            .map((virtualRow) => resolveVirtualRow(virtualRow.index))
            .find((row) => row && row.original && !row.original._isTotal);
        if (!firstVisibleRow || !firstVisibleRow.original) return null;

        const path = typeof firstVisibleRow.original._path === 'string' ? firstVisibleRow.original._path : '';
        const pathParts = path.split('|||').filter(Boolean);
        if (pathParts.length <= 1) return null;

        for (let depth = pathParts.length - 1; depth > 0; depth -= 1) {
            const ancestorPath = pathParts.slice(0, depth).join('|||');
            const ancestorRow = centerRowLookup.get(ancestorPath);
            if (ancestorRow && ancestorRow.id !== firstVisibleRow.id) {
                return ancestorRow;
            }
        }

        return null;
    }, [centerRowLookup, resolveVirtualRow, treeSuppressGroupRowsSticky, viewMode, virtualRows]);

    const renderCenterVirtualCells = (row, virtualRowIndex, isVirtualRow, renderOptions = {}) => {
        return virtualCenterCols.map(virtualCol => {
            const centerColumn = centerCols[virtualCol.index];
            if (!centerColumn) return null;
            return renderVirtualColumnCell(row, centerColumn, virtualRowIndex, isVirtualRow, getCellRenderOptions(centerColumn, renderOptions));
        });
    };

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

    const renderRowSections = (row, virtualRowIndex, isVirtualRow, renderOptions = {}) => {
        const rowBackground = renderOptions.rowBackground || defaultRowBg;
        const disablePinnedColumnStickiness = !!renderOptions.disablePinnedColumnStickiness;

        return (
            <>
                {leftPinnedSectionStyle && (
                    <div
                        style={disablePinnedColumnStickiness
                            ? { display: 'flex', width: leftPinnedWidth, minWidth: leftPinnedWidth, flexShrink: 0, position: 'relative', background: rowBackground, boxShadow: 'none' }
                            : { ...leftPinnedSectionStyle, background: rowBackground }
                        }
                    >
                        {leftCols.map((column) =>
                            renderVirtualColumnCell(row, column, virtualRowIndex, isVirtualRow, getCellRenderOptions(column, renderOptions))
                        )}
                    </div>
                )}
                <div style={{ ...centerFlexStyle, background: rowBackground }}>
                    <div style={beforeSpacerStyle} />
                    {renderCenterVirtualCells(row, virtualRowIndex, isVirtualRow, renderOptions)}
                    <div style={afterSpacerStyle} />
                </div>
                {rightPinnedSectionStyle && (
                    <div
                        style={disablePinnedColumnStickiness
                            ? { display: 'flex', width: rightPinnedWidth, minWidth: rightPinnedWidth, flexShrink: 0, position: 'relative', background: rowBackground, boxShadow: 'none' }
                            : { ...rightPinnedSectionStyle, background: rowBackground }
                        }
                    >
                        {rightCols.map((column) =>
                            renderVirtualColumnCell(row, column, virtualRowIndex, isVirtualRow, getCellRenderOptions(column, renderOptions))
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
                    const extraShadow = isEdgeRow
                        ? (isTop ? `0 2px 4px -2px ${theme.border}80` : `0 -4px 6px -1px rgba(15,23,42,0.06)`)
                        : 'none';
                    return (
                        <div
                            key={`${pinPosition}_${row.id}`}
                            role="row"
                            style={{
                                ...baseRowStyle,
                                position: 'relative',
                                minWidth: `${totalLayoutWidth}px`,
                                height: rowHeight,
                                background: rowBackground,
                                boxShadow: extraShadow,
                            }}
                        >
                            {leftCols.map((column) =>
                                renderVirtualColumnCell(row, column, displayRowIndex, false)
                            )}
                            <div style={beforeSpacerStyle} />
                            {renderCenterVirtualCells(row, displayRowIndex, false)}
                            <div style={afterSpacerStyle} />
                            {rightCols.map((column) =>
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
                             {leftHeaderGroups.map((group, level) => (
                                     <div key={group.id} style={headerRowStyle}>
                                     {group.headers.map((header) => renderHeaderCell(header, level, 'left', null, true))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{...headerRowStyle, background: theme.background}}>
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
                              {centerHeaderRenderPlan.map(({ groupId, visibleHeaders }, level) => {
                                  return (
                                      <div key={groupId} style={headerRowStyle}>
                                          <div style={beforeSpacerStyle} />
                                          {visibleHeaders.map(({ header, visWidth }) =>
                                              renderHeaderCell(header, level, 'center', visWidth)
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
                                 <div style={{...headerRowStyle, background: theme.background}}>
                                     <div style={beforeSpacerStyle} />
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
                                     <div style={afterSpacerStyle} />
                                 </div>
                             )}
                         </div>

                         {/* Right Section */}
                         <div style={{position: 'sticky', right: 0, zIndex: 4, background: theme.headerBg}}>
                             {rightHeaderGroups.map((group, level) => (
                                 <div key={group.id} style={headerRowStyle}>
                                     {group.headers.map((header) => renderHeaderCell(header, level, 'right', null, true))}
                                 </div>
                             ))}
                             {showFloatingFilters && (
                                 <div style={{...headerRowStyle, background: theme.background}}>
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
                    {stickyTreeRow && (
                        <div
                            style={{
                                position: 'sticky',
                                top: `${(stickyHeaders ? stickyHeaderHeight : 0) + (effectiveTopRows.length * rowHeight)}px`,
                                zIndex: 79,
                                display: 'flex',
                                width: `${totalLayoutWidth}px`,
                                minWidth: `${totalLayoutWidth}px`,
                                height: `${rowHeight}px`,
                                background: theme.surfaceBg || theme.background,
                                borderBottom: `1px solid ${theme.border}`,
                                boxShadow: `0 2px 4px -2px ${theme.border}80`,
                            }}
                        >
                            {renderRowSections(
                                stickyTreeRow,
                                centerDisplayIndexById.get(stickyTreeRow.id) ?? getRenderedRowIndex(stickyTreeRow, 0),
                                false,
                                { rowBackground: theme.surfaceBg || theme.background }
                            )}
                        </div>
                    )}

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

                          const rowBg = (row.original && row.original._isTotal) ? totalRowBg : defaultRowBg;
                          const rowSpanByColumnId = rowSpanEnabled && row.id != null
                              ? (rowSpanPlan.get(String(row.id)) || null)
                              : null;

                          return (
                              <React.Fragment key={stableRowKey}>
                                  <div
                                     role="row"
                                     aria-rowindex={virtualRow.index}
                                     style={{
                                      ...baseRowStyle,
                                      height: virtualRow.size,
                                      top: `${adjustedTop}px`,
                                      background: rowBg,
                                      transition: rowVirtualizer.isScrolling ? 'none' : 'background-color 0.2s'
                                   }}>
                                      {renderRowSections(row, displayRowIndex, true, {
                                          rowBackground: rowBg,
                                          rowSpanByColumnId,
                                      })}
                                  </div>
                                  {detailMode === 'inline' && detailState && detailState.anchorRowId === row.id && (
                                      <InlineDetailPanel
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
        </div>
    );
});


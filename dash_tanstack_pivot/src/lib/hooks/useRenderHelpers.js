import { useCallback } from 'react';
import React from 'react';
import Icons from '../components/Icons';
import FilterPopover from '../components/Filters/FilterPopover';
import { flexRender } from '@tanstack/react-table';
import { mergeStateStyles } from '../utils/styles';
import { formatDisplayLabel } from '../utils/helpers';

/**
 * useRenderHelpers — extracts renderCell and renderHeaderCell from DashTanstackPivot.
 *
 * CODE-03 stale closure audit for renderHeaderCell:
 *   - renderHeaderCell is a plain arrow function (not useCallback), so it always
 *     captures fresh values from the hook call boundary on each render.
 *   - handleHeaderContextMenu, autoSizeColumn, autoSizeBounds, setHoveredHeaderId,
 *     setFocusedHeaderId, onDragStart, handleFilterClick, handleHeaderFilter,
 *     activeFilterCol, filterAnchorEl, closeFilterPopover, activeFilterOptions,
 *     filters, selectedCols, leftCols, rightCols, centerCols, styles, rowHeight,
 *     hoveredHeaderId, focusedHeaderId, theme — all are read synchronously when
 *     renderHeaderCell is called, capturing the value from the current render pass.
 *     No ref-guard is missing because the function is recreated every render.
 */
export function useRenderHelpers({
    // renderCell dependencies
    serverSide,
    selectedCells,
    fillRange,
    dragStart,
    theme,
    getStickyStyle,
    isDarkTheme,
    handleCellMouseDown,
    handleCellMouseEnter,
    handleContextMenu,
    handleFillMouseDown,
    visibleLeafColIndexMap,
    lastSelected,
    styles,
    getConditionalStyle,
    // renderHeaderCell dependencies (all synchronous / render-time)
    rowHeight,
    table,
    leftCols,
    centerCols,
    rightCols,
    columnPinning,
    sorting,
    handleHeaderContextMenu,
    autoSizeColumn,
    autoSizeBounds,
    hoveredHeaderId,
    setHoveredHeaderId,
    focusedHeaderId,
    setFocusedHeaderId,
    onDragStart,
    handleFilterClick,
    handleHeaderFilter,
    activeFilterCol,
    filterAnchorEl,
    closeFilterPopover,
    activeFilterOptions,
    filters,
    selectedCols,
    getHeaderStickyStyle,
    dataBarsColumns,
    colorScaleStats,
    cellFormatRules,
}) {
    // --- Helper to Render a single Cell with useCallback ---
    const renderCell = useCallback((cell, virtualRowIndex, isVirtualRow = false) => {
        if (!cell) return null;

        const row = cell.row;
        const col = cell.column;
        const colIndex = visibleLeafColIndexMap.get(col.id) !== undefined ? visibleLeafColIndexMap.get(col.id) : -1;
        const isHierarchy = cell.column.id === 'hierarchy';
        const colParentHeader = col.parent && typeof col.parent.columnDef?.header === 'string' ? col.parent.columnDef.header : '';
        const isTotalCol = !isHierarchy && (colParentHeader === 'Grand Total' || colParentHeader.startsWith('Grand Total'));
        const isSelected = Object.prototype.hasOwnProperty.call(
            selectedCells || {},
            `${row.id}:${cell.column.id}`
        );
        const isLastSelected = lastSelected && lastSelected.rowIndex === virtualRowIndex && lastSelected.colIndex === colIndex; // Approximate check
        // Check for fill handle selection
        let isFillSelected = false;
        if (fillRange && dragStart) {
             const rMin = Math.min(dragStart.rowIndex, fillRange.rEnd); // simplified for demo
             // Precise range check would require row index mapping
             if (virtualRowIndex >= fillRange.rStart && virtualRowIndex <= fillRange.rEnd && colIndex >= fillRange.cStart && colIndex <= fillRange.cEnd) {
                 isFillSelected = true;
             }
        }

        // Per-cell format rule lookup
        const rowPath = row.original && row.original._path ? row.original._path : row.id;
        const cellKey = `${rowPath}:::${col.id}`;
        const cellFmt = cellFormatRules && cellFormatRules[cellKey];

        const isGrandTotalRow = !!(row.original && row.original._isTotal);
        const themeBackground = isGrandTotalRow
            ? (theme.totalBg || theme.select || theme.background)
            : isTotalCol
                ? (theme.totalBg || theme.select || theme.background)
                : isHierarchy
                    ? (isDarkTheme(theme) ? '#212121' : (theme.hierarchyBg || theme.surfaceInset || theme.surfaceBg || '#fff'))
                    : (isDarkTheme(theme) ? '#212121' : (theme.surfaceBg || '#fff'));
        const condStyle = getConditionalStyle(
            cell.column.id,
            cell.getValue(),
            row.original,
            row.id
        );
        // Cell format background takes priority over color-scale/conditional style
        const stickyBaseStyle = cellFmt && cellFmt.bg
            ? mergeStateStyles(condStyle, { background: cellFmt.bg })
            : mergeStateStyles({ background: themeBackground }, condStyle);
        const stickyStyle = getStickyStyle(cell.column, stickyBaseStyle.background);
        const selectedOverlayStyle = isSelected
            ? {
                background: theme.select,
                boxShadow: `inset 0 0 0 1px ${theme.primary}`
            }
            : {};
        const focusOverlayStyle = isLastSelected
            ? {
                outline: `1px solid ${theme.primary}`,
                outlineOffset: '-1px'
            }
            : {};
        const fillOverlayStyle = isFillSelected
            ? { boxShadow: `inset 0 0 0 1px ${theme.primary}` }
            : {};
        const cellStateStyle = mergeStateStyles(
            stickyBaseStyle,
            stickyStyle,
            selectedOverlayStyle,
            focusOverlayStyle,
            fillOverlayStyle
        );

        // Fix row number ordering
        let cellContent;
        if (cell.column.id === '__row_number__' && isVirtualRow) {
            cellContent = (row.original && typeof row.original.__virtualIndex === 'number')
                ? row.original.__virtualIndex + 1
                : virtualRowIndex + 1;
        } else {
            const rowData = row.original || {};
            const hasFetchedColumn = Object.prototype.hasOwnProperty.call(rowData, cell.column.id);
            const showPendingColumnPlaceholder = (
                serverSide &&
                !!rowData.__colPending &&
                !isHierarchy &&
                cell.column.id !== '__row_number__' &&
                !hasFetchedColumn
            );

            if (showPendingColumnPlaceholder) {
                cellContent = (
                    <span
                        aria-hidden="true"
                        style={{
                            width: '70%',
                            maxWidth: '120px',
                            height: '10px',
                            borderRadius: '999px',
                            background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%))',
                            backgroundSize: '220% 100%',
                            animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite'
                        }}
                    />
                );
            } else {
                cellContent = flexRender(cell.column.columnDef.cell, cell.getContext());
            }
        }

        // Data bars
        const rawNumValue = cell.getValue();
        const isDataBar = !isHierarchy
            && dataBarsColumns && dataBarsColumns.has(col.id)
            && typeof rawNumValue === 'number'
            && !Number.isNaN(rawNumValue)
            && !(row.original && row.original._isTotal);
        let dataBarEl = null;
        if (isDataBar) {
            const stats = colorScaleStats && colorScaleStats.byCol && colorScaleStats.byCol[col.id];
            if (stats) {
                // Linear scale from zero: bar width = |value| / max(|max|, |min|)
                const absMax = Math.max(Math.abs(stats.max), Math.abs(stats.min), 1e-9);
                const pct = Math.min(1, Math.abs(rawNumValue) / absMax);
                const isNeg = rawNumValue < 0;
                const barColor = isNeg
                    ? (isDarkTheme(theme) ? 'rgba(255,100,100,0.18)' : 'rgba(220,38,38,0.12)')
                    : (isDarkTheme(theme) ? 'rgba(100,180,255,0.22)' : 'rgba(37,99,235,0.13)');
                const barBorder = isNeg
                    ? (isDarkTheme(theme) ? 'rgba(255,100,100,0.5)' : 'rgba(220,38,38,0.35)')
                    : (isDarkTheme(theme) ? 'rgba(100,180,255,0.6)' : 'rgba(37,99,235,0.45)');
                dataBarEl = (
                    <div style={{
                        position: 'absolute',
                        left: 0,
                        top: 0,
                        bottom: 0,
                        width: `${pct * 100}%`,
                        background: barColor,
                        borderRight: pct > 0.01 ? `2px solid ${barBorder}` : 'none',
                        pointerEvents: 'none',
                        zIndex: 0,
                    }} />
                );
            }
        }

        return (
            <div
                key={cell.id}
                role="gridcell"
                aria-selected={isSelected}
                onMouseDown={(e) => handleCellMouseDown(e, virtualRowIndex, colIndex, row.id, cell.column.id, cell.getValue())}
                onMouseEnter={() => handleCellMouseEnter(virtualRowIndex, colIndex)}
                style={{
                    ...styles.cell,
                    width: col.getSize(),
                    height: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: isHierarchy ? 'flex-start' : 'flex-end',
                    fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                    fontVariantNumeric: isHierarchy ? undefined : 'tabular-nums',
                    fontWeight: cellFmt && cellFmt.bold ? 'bold' : (isGrandTotalRow ? 700 : isTotalCol ? 600 : ((isHierarchy && row.getIsGrouped()) ? 500 : 400)),
                    fontStyle: cellFmt && cellFmt.italic ? 'italic' : undefined,
                    color: cellFmt && cellFmt.color ? cellFmt.color : (isHierarchy ? undefined : ((isGrandTotalRow || isTotalCol) ? (theme.totalTextStrong || theme.primary) : theme.textSec)),
                    ...cellStateStyle,
                    userSelect: 'none',
                    position: cellStateStyle.position === 'sticky' ? 'sticky' : 'relative',
                }}
                onContextMenu={e => handleContextMenu(e, cell.getValue(), cell.column.id, row)}
            >
                {dataBarEl}
                <span style={dataBarEl ? { position: 'relative', zIndex: 1 } : undefined}>{cellContent}</span>
                {isLastSelected && Object.keys(selectedCells).length === 1 && isSelected && (
                    <div
                        onMouseDown={handleFillMouseDown}
                        style={{
                            position: 'absolute',
                            right: 0,
                            bottom: 0,
                            width: '8px',
                            height: '8px',
                            background: theme.primary,
                            cursor: 'crosshair',
                            zIndex: 100,
                            border: `1px solid ${isDarkTheme(theme) ? '#000' : '#fff'}`,                            borderRadius: '1px'
                        }}
                    />
                )}
            </div>
        );
    }, [
        serverSide,
        selectedCells,
        fillRange,
        dragStart,
        theme,
        getStickyStyle,
        isDarkTheme,
        handleCellMouseDown,
        handleCellMouseEnter,
        handleContextMenu,
        handleFillMouseDown,
        visibleLeafColIndexMap,
        lastSelected,
        styles,
        getConditionalStyle,
        dataBarsColumns,
        colorScaleStats,
        cellFormatRules,
    ]);

    // NEW: Render Header Cell for Split Sections
    // overrideWidth: when set, replaces the computed section width (used for partially-visible
    // group headers during center-column virtualization so the width matches only the visible leaves).
    const renderHeaderCell = (header, level, renderSection = 'center', overrideWidth = null) => {
        const isGroupHeader = header.column.columns && header.column.columns.length > 0;
        const isMeasureSubHeader = !isGroupHeader && header.column.id !== 'hierarchy' && header.column.id !== '__row_number__';
        const headerText = typeof header.column.columnDef.header === 'string' ? header.column.columnDef.header : '';
        const parentText = header.column.parent && typeof header.column.parent.columnDef?.header === 'string' ? header.column.parent.columnDef.header : '';
        const isTotalGroupHeader = isGroupHeader && (headerText === 'Grand Total' || headerText.startsWith('Grand Total'));
        const isUnderTotalGroup = isMeasureSubHeader && (parentText === 'Grand Total' || parentText.startsWith('Grand Total'));
        const isSorted = header.column.getIsSorted();
        const sortIndex = header.column.getSortIndex();
        const isMultiSort = table.getState().sorting.length > 1;
        const isResizingColumn = header.column.getIsResizing();
        const isHoveredHeader = hoveredHeaderId === header.column.id;
        const isFocusedHeader = focusedHeaderId === header.column.id;
        const isResizeHandleVisible = isResizingColumn || isHoveredHeader || isFocusedHeader;
        const isPinned = header.column.getIsPinned();
        const leafColumns = header.column.getLeafColumns ? header.column.getLeafColumns() : [header.column];
        const sectionLeafIds = new Set(
            (renderSection === 'left' ? leftCols : renderSection === 'right' ? rightCols : centerCols).map(column => column.id)
        );
        const sectionWidth = leafColumns
            .filter(column => sectionLeafIds.has(column.id))
            .reduce((sum, column) => sum + column.getSize(), 0);
        const headerWidth = overrideWidth !== null ? overrideWidth : (sectionWidth || header.getSize());
        const sortedHeaderStyle = !isGroupHeader && isSorted ? {
            background: theme.sortedHeaderBg || theme.select,
            borderBottom: `1px solid ${theme.sortedHeaderBorder || theme.primary}`,
            color: theme.sortedHeaderText || theme.primary,
            fontWeight: 700
        } : {};
        const isHeaderSelected = !isGroupHeader && selectedCols.has(header.column.id);
        const interactionOverlayStyle = !isGroupHeader && (isFocusedHeader || isHeaderSelected || isResizingColumn)
            ? { boxShadow: `inset 0 0 0 1px ${theme.primary}` }
            : {};
        const sortIconColor = isSorted ? (theme.sortedHeaderText || theme.primary) : theme.textSec;

        // Calculate sticky style for pinned headers using the hook
        const stickyStyle = getHeaderStickyStyle(
            header,
            level,
            renderSection,
            mergeStateStyles({ background: theme.headerBg }, sortedHeaderStyle).background
        );
        const headerStateStyle = mergeStateStyles(
            styles.headerCell,
            sortedHeaderStyle,
            stickyStyle,
            interactionOverlayStyle
        );

        return (
            <div key={header.id} style={{
                ...headerStateStyle,
                width: headerWidth,
                minWidth: headerWidth,
                flexShrink: 0,
                height: rowHeight,
                cursor: 'pointer',
                // Position is handled by getHeaderStickyStyle or parent container
                position: headerStateStyle.position || 'relative',
                ...(isTotalGroupHeader ? {
                    background: theme.totalBgStrong || (theme.totalBg || theme.select),
                    color: theme.totalTextStrong || theme.primary,
                    fontWeight: 700,
                } : isGroupHeader && !isSorted ? {
                    background: isDarkTheme(theme) ? theme.headerBg : (theme.headerSubtleBg || '#F9FAFB'),
                } : {}),
                ...(isMeasureSubHeader ? {
                    fontSize: '11px',
                    fontWeight: isUnderTotalGroup ? 700 : 500,
                    textTransform: 'uppercase',
                    letterSpacing: '0.04em',
                    color: isUnderTotalGroup ? (theme.totalTextStrong || theme.primary) : theme.textSec,
                    background: isUnderTotalGroup ? (theme.totalBgStrong || theme.totalBg || theme.select) : undefined,
                } : {})
            }}
            role="columnheader"
            aria-sort={isSorted || 'none'}
            aria-label={`${formatDisplayLabel(typeof header.column.columnDef.header === 'string' ? header.column.columnDef.header : header.column.id)}. Click or press Alt+Up/Down to sort.`}
            tabIndex={0}
            onKeyDown={(e) => {
                if (e.altKey && (e.key === 'ArrowUp' || e.key === 'ArrowDown')) {
                    e.preventDefault();
                    header.column.toggleSorting(e.key === 'ArrowDown', e.shiftKey);
                }
            }}
            draggable={!isGroupHeader && header.column.id !== '__row_number__' && !header.column.getIsResizing()}
            onDragStart={(e) => {
                // Prevent drag when resizing
                if (header.column.getIsResizing()) {
                    e.preventDefault();
                    return;
                }
                if (!isGroupHeader && header.column.id !== '__row_number__') {
                    onDragStart(e, header.column.id, 'cols', -1);
                }
            }}
            onContextMenu={(e) => handleHeaderContextMenu(e, header.column.id)}
            onMouseEnter={() => setHoveredHeaderId(header.column.id)}
            onMouseLeave={() => setHoveredHeaderId(current => (current === header.column.id ? null : current))}
            onFocus={() => setFocusedHeaderId(header.column.id)}
            onBlur={(e) => {
                if (!e.currentTarget.contains(e.relatedTarget)) {
                    setFocusedHeaderId(current => (current === header.column.id ? null : current));
                }
            }}
            onClick={header.column.getToggleSortingHandler()}>
                <div style={{
                    display:'flex',
                    alignItems:'center',
                    gap: '4px',
                    width: '100%',
                    justifyContent: header.column.id === 'hierarchy' ? 'flex-start' : isMeasureSubHeader ? 'flex-end' : 'center',
                    padding: '0 4px',
                    overflow: 'hidden',
                    minWidth: autoSizeBounds.minWidth
                }}>
                <span style={{
                    overflow:'hidden',
                    textOverflow:'ellipsis',
                    whiteSpace:'nowrap',
                    flex: 1,
                    minWidth: 0
                }}>
                    {header.isPlaceholder ? null : (typeof header.column.columnDef.header === 'string'
                        ? formatDisplayLabel(header.column.columnDef.header)
                        : flexRender(header.column.columnDef.header, header.getContext()))}
                </span>

                {!isGroupHeader && header.column.id !== 'hierarchy' && !header.isPlaceholder && (
                    <div
                        onClick={(e) => handleFilterClick(e, header.column.id)}
                        style={{
                            display:'flex',
                            alignItems: 'center',
                            padding: '2px',
                            borderRadius: '4px',
                            background: filters[header.column.id] ? theme.select : 'transparent',
                            color: filters[header.column.id] ? theme.primary : 'inherit'
                        }}
                        aria-label="Filter"
                    >
                        <Icons.Filter/>
                    </div>
                )}

                {!isGroupHeader && isSorted && (
                    <span style={{display: 'inline-flex', alignItems: 'center', color: sortIconColor}}>
                        {isSorted === 'asc' ? <Icons.SortAsc/> : <Icons.SortDesc/>}
                    </span>
                )}
                {!isGroupHeader && isSorted && isMultiSort && (
                    <span style={{fontSize: '9px', verticalAlign: 'super', marginLeft: '1px', opacity: 0.8, fontWeight: 700, color: sortIconColor}}>{sortIndex + 1}</span>
                )}

                <div
                    onClick={(e) => { e.stopPropagation(); handleHeaderContextMenu(e, header.column.id); }}
                    style={{
                        display:'flex',
                        alignItems: 'center',
                        padding: '2px',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        color: theme.textSec,
                        opacity: 0.6,
                        hover: { opacity: 1, background: '#eee' }
                    }}
                    aria-label="More options"
                >
                    <Icons.MoreVert/>
                </div>
                </div>

                {!isGroupHeader && activeFilterCol === header.column.id && (
                    <FilterPopover
                    column={header.column}
                    anchorEl={filterAnchorEl}
                    onClose={closeFilterPopover}
                    onFilter={(type, val) => handleHeaderFilter(header.column.id, type, val)}
                    currentFilter={filters[header.column.id]}
                    options={activeFilterCol === header.column.id ? activeFilterOptions : []}
                    theme={theme}
                    />
                )}
                {!isGroupHeader && filters[header.column.id] && filters[header.column.id].conditions && (
                <div style={{fontSize: '10px', color: theme.primary, paddingTop: '2px', textAlign: 'center'}}>
                    {filters[header.column.id].conditions.map(c => `${c.type}: ${c.value}${c.caseSensitive ? ' (Match Case)' : ''}`).join(` ${filters[header.column.id].operator} `)}
                </div>
                )}

                {header.column.getCanResize() && <div
                    onMouseDown={(e) => {
                        e.stopPropagation();
                        header.getResizeHandler()(e);
                    }}
                    onTouchStart={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                        header.getResizeHandler()(e);
                    }}
                    onClick={(e) => e.stopPropagation()}
                    onDoubleClick={(e) => {
                        e.stopPropagation();
                        autoSizeColumn(header.column.id);
                    }}
                    style={{
                        position: 'absolute',
                        right: -6,
                        top: 0,
                        bottom: 0,
                        width: 14,
                        cursor: 'col-resize',
                        touchAction: 'none',
                        zIndex: 4,
                        opacity: isResizeHandleVisible ? 1 : 0.14,
                        transition: 'opacity 120ms ease',
                        background: isResizeHandleVisible ? theme.primary : 'transparent'
                    }}
                />}
            </div>
        );
    };

    return { renderCell, renderHeaderCell };
}

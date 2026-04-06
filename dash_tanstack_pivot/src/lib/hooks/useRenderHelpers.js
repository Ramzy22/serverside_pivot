import { useCallback, useMemo, useRef } from 'react';
import React from 'react';
import Icons from '../components/Icons';
import FilterPopover from '../components/Filters/FilterPopover';
import { flexRender } from '@tanstack/react-table';
import { buildEditedCellVisualStyle, mergeStateStyles } from '../utils/styles';
import { formatDisplayLabel } from '../utils/helpers';
import { EDITED_CELL_FORMAT_KEY } from '../utils/formatting';

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
    tableRef,
    leftCols,
    centerCols,
    rightCols,
    columnPinning,
    sorting,
    handleHeaderContextMenu,
    autoSizeColumn,
    autoSizeBounds,
    hoveredHeaderIdRef,
    setHoveredHeaderId,
    focusedHeaderIdRef,
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
    resolveEditedCellMarker,
    resolveCellDisplayValue,
    editedCellEpoch,
}) {
    // --- Stable style objects extracted from the per-cell hot path ---
    // These are recreated only when theme/styles change, not per cell per scroll frame.
    const baseCellStyle = useMemo(() => ({
        ...styles.cell,
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
        userSelect: 'none',
    }), [styles.cell]);

    const selectedOverlayRef = useMemo(() => ({
        background: theme.select,
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.select, theme.primary]);

    const selectedEditedOverlayRef = useMemo(() => ({
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.primary]);

    const focusOverlayRef = useMemo(() => ({
        outline: `1px solid ${theme.primary}`,
        outlineOffset: '-1px',
    }), [theme.primary]);

    const fillOverlayRef = useMemo(() => ({
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.primary]);

    const fillHandleStyle = useMemo(() => ({
        position: 'absolute',
        right: 0,
        bottom: 0,
        width: '8px',
        height: '8px',
        background: theme.primary,
        cursor: 'crosshair',
        zIndex: 100,
        border: `1px solid ${isDarkTheme(theme) ? '#000' : '#fff'}`,
        borderRadius: '1px',
    }), [theme, isDarkTheme]);

    // Pre-computed theme backgrounds — avoids repeated ternary chains per cell.
    const themeBgs = useMemo(() => ({
        total: theme.totalBg || theme.select || theme.background,
        grandTotal: theme.totalBgStrong || theme.totalBg || theme.select || theme.background,
        hierarchy: theme.hierarchyBg || theme.surfaceInset || theme.surfaceBg || theme.background || '#fff',
        normal: theme.surfaceBg || theme.background || '#fff',
        totalText: theme.totalText || theme.text || theme.primary,
        grandTotalText: theme.totalTextStrong || theme.primary,
    }), [theme]);

    const EMPTY_STYLE = useMemo(() => ({}), []);
    const DATA_BAR_SPAN_STYLE = useMemo(() => ({ position: 'relative', zIndex: 1 }), []);
    const SKELETON_CELL_STYLE = useMemo(() => ({
        width: '70%',
        maxWidth: '120px',
        height: '10px',
        borderRadius: '999px',
        background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%))',
        backgroundSize: '220% 100%',
        animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite',
    }), []);

    const renderResolvedCell = useCallback((cellLike, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!cellLike) return null;
        const disableSticky = !!renderOptions.disableSticky;

        const row = cellLike.row;
        const col = cellLike.column;
        const colIndex = visibleLeafColIndexMap.get(col.id) !== undefined ? visibleLeafColIndexMap.get(col.id) : -1;
        const isHierarchy = col.id === 'hierarchy';
        const colParentHeader = col.parent && typeof col.parent.columnDef?.header === 'string' ? col.parent.columnDef.header : '';
        const isTotalCol = !isHierarchy && (colParentHeader === 'Grand Total' || colParentHeader.startsWith('Grand Total'));
        const isSelected = Object.prototype.hasOwnProperty.call(
            selectedCells || {},
            `${row.id}:${col.id}`
        );
        const isLastSelected = lastSelected && lastSelected.rowIndex === virtualRowIndex && lastSelected.colIndex === colIndex;
        let isFillSelected = false;
        if (fillRange && dragStart) {
             if (virtualRowIndex >= fillRange.rStart && virtualRowIndex <= fillRange.rEnd && colIndex >= fillRange.cStart && colIndex <= fillRange.cEnd) {
                 isFillSelected = true;
             }
        }

        const rowPath = row.original && row.original._path ? row.original._path : row.id;
        const cellKey = `${rowPath}:::${col.id}`;
        const rawCellValue = cellLike.getValue();
        const resolvedCellValue = typeof resolveCellDisplayValue === 'function'
            ? resolveCellDisplayValue(rowPath, col.id, rawCellValue)
            : rawCellValue;
        const displayCellLike = {
            ...cellLike,
            getValue: () => resolvedCellValue,
            renderValue: () => (resolvedCellValue ?? null),
        };
        displayCellLike.getContext = () => ({
            ...cellLike.getContext(),
            cell: displayCellLike,
            getValue: displayCellLike.getValue,
            renderValue: displayCellLike.renderValue,
        });
        const cellFmt = cellFormatRules && cellFormatRules[cellKey];
        const editedCellFmt = cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY];
        const editedMarker = typeof resolveEditedCellMarker === 'function'
            ? resolveEditedCellMarker(rowPath, col.id)
            : null;

        const isGrandTotalRow = !!(row.original && row.original._isTotal);
        const themeBackground = isGrandTotalRow
            ? themeBgs.grandTotal
            : isTotalCol
                ? themeBgs.total
                : isHierarchy ? themeBgs.hierarchy : themeBgs.normal;
        const condStyle = getConditionalStyle(col.id, resolvedCellValue, row.original, row.id);
        const editedVisualStyle = editedMarker
            ? buildEditedCellVisualStyle(theme, editedCellFmt, editedMarker, { emphasizeText: true })
            : null;
        const editedBaseStyle = editedVisualStyle || EMPTY_STYLE;
        const stickyBaseStyle = cellFmt && cellFmt.bg
            ? mergeStateStyles({ background: themeBackground }, condStyle, editedBaseStyle, { background: cellFmt.bg })
            : mergeStateStyles({ background: themeBackground }, condStyle, editedBaseStyle);
        const stickyStyle = disableSticky
            ? { background: stickyBaseStyle.background || stickyBaseStyle.backgroundColor }
            : getStickyStyle(cellLike.column, stickyBaseStyle.background || stickyBaseStyle.backgroundColor);
        const selectionOverlayStyle = isSelected
            ? (editedMarker ? selectedEditedOverlayRef : selectedOverlayRef)
            : EMPTY_STYLE;
        const cellStateStyle = mergeStateStyles(
            stickyBaseStyle,
            stickyStyle,
            selectionOverlayStyle,
            isLastSelected ? focusOverlayRef : EMPTY_STYLE,
            isFillSelected ? fillOverlayRef : EMPTY_STYLE,
        );

        let cellContent;
        if (col.id === '__row_number__' && isVirtualRow) {
            cellContent = (row.original && typeof row.original.__virtualIndex === 'number')
                ? row.original.__virtualIndex + 1
                : (typeof row.index === 'number' ? row.index + 1 : virtualRowIndex + 1);
        } else {
            const rowData = row.original || {};
            const hasFetchedColumn = Object.prototype.hasOwnProperty.call(rowData, col.id);
            const showPendingColumnPlaceholder = (
                serverSide &&
                !!rowData.__colPending &&
                !isHierarchy &&
                col.id !== '__row_number__' &&
                !hasFetchedColumn
            );

            if (showPendingColumnPlaceholder) {
                cellContent = <span aria-hidden="true" style={SKELETON_CELL_STYLE} />;
            } else {
                cellContent = flexRender(col.columnDef.cell, displayCellLike.getContext());
            }
        }

        // Data bars
        const rawNumValue = resolvedCellValue;
        const isDataBar = !isHierarchy
            && dataBarsColumns && dataBarsColumns.has(col.id)
            && typeof rawNumValue === 'number'
            && !Number.isNaN(rawNumValue)
            && !(row.original && row.original._isTotal);
        let dataBarEl = null;
        if (isDataBar) {
            const stats = colorScaleStats && colorScaleStats.byCol && colorScaleStats.byCol[col.id];
            if (stats) {
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
                key={cellLike.id}
                role="gridcell"
                aria-selected={isSelected}
                data-rowid={row.id}
                data-colid={col.id}
                onMouseDown={(e) => handleCellMouseDown(e, virtualRowIndex, colIndex, row.id, col.id, resolvedCellValue)}
                onMouseEnter={() => handleCellMouseEnter(virtualRowIndex, colIndex)}
                style={{
                    ...baseCellStyle,
                    width: col.getSize(),
                    justifyContent: isHierarchy ? 'flex-start' : 'flex-end',
                    fontVariantNumeric: isHierarchy ? undefined : 'tabular-nums',
                    fontWeight: cellFmt && cellFmt.bold
                        ? 'bold'
                        : (editedVisualStyle && editedVisualStyle.fontWeight)
                            ? editedVisualStyle.fontWeight
                            : (isGrandTotalRow ? 700 : isTotalCol ? 600 : ((isHierarchy && row.getIsGrouped()) ? 500 : 400)),
                    fontStyle: cellFmt && cellFmt.italic
                        ? 'italic'
                        : (editedVisualStyle && editedVisualStyle.fontStyle ? editedVisualStyle.fontStyle : undefined),
                    color: cellFmt && cellFmt.color
                        ? cellFmt.color
                        : (editedVisualStyle && editedVisualStyle.color)
                            ? editedVisualStyle.color
                            : (isHierarchy ? undefined : (
                                isGrandTotalRow
                                    ? themeBgs.grandTotalText
                                    : isTotalCol ? themeBgs.totalText : theme.textSec
                            )),
                    ...cellStateStyle,
                    position: !disableSticky && cellStateStyle.position === 'sticky' ? 'sticky' : 'relative',
                }}
                onContextMenu={e => handleContextMenu(e, resolvedCellValue, col.id, row)}
            >
                {dataBarEl}
                <span style={dataBarEl ? DATA_BAR_SPAN_STYLE : undefined}>{cellContent}</span>
                {isLastSelected && Object.keys(selectedCells).length === 1 && isSelected && (
                    <div onMouseDown={handleFillMouseDown} style={fillHandleStyle} />
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
        getConditionalStyle,
        dataBarsColumns,
        colorScaleStats,
        cellFormatRules,
        resolveEditedCellMarker,
        resolveCellDisplayValue,
        editedCellEpoch,
        baseCellStyle,
        selectedOverlayRef,
        selectedEditedOverlayRef,
        focusOverlayRef,
        fillOverlayRef,
        fillHandleStyle,
        themeBgs,
        EMPTY_STYLE,
        DATA_BAR_SPAN_STYLE,
        SKELETON_CELL_STYLE,
    ]);

    // --- Helper to Render a single real TanStack Cell with useCallback ---
    const renderCell = useCallback((cell, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!cell) return null;
        return renderResolvedCell(cell, virtualRowIndex, isVirtualRow, renderOptions);
    }, [renderResolvedCell]);

    // Render a lightweight pseudo-cell for the currently visible columns only.
    // This avoids forcing TanStack to materialize cells for every leaf column in a row.
    const renderVirtualColumnCell = useCallback((row, column, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!row || !column) return null;
        const getValue = () => row.getValue(column.id);
        const pseudoCell = {
            id: `${row.id}_${column.id}`,
            row,
            column,
            getValue,
            renderValue: () => getValue() ?? null,
        };
        pseudoCell.getContext = () => ({
            table: tableRef.current,
            column,
            row,
            cell: pseudoCell,
            getValue: pseudoCell.getValue,
            renderValue: pseudoCell.renderValue,
        });
        return renderResolvedCell(pseudoCell, virtualRowIndex, isVirtualRow, renderOptions);
    }, [renderResolvedCell, tableRef]);

    // Pre-compute section leaf ID sets so renderHeaderCell doesn't rebuild them per call.
    const leftColIdSet = useMemo(() => new Set(leftCols.map(c => c.id)), [leftCols]);
    const rightColIdSet = useMemo(() => new Set(rightCols.map(c => c.id)), [rightCols]);
    const centerColIdSet = useMemo(() => new Set(centerCols.map(c => c.id)), [centerCols]);
    // Cache getLeafColumns() results by column id to avoid repeated tree walks.
    // Cleared when column structure changes (tracked via the section sets above).
    const leafColumnsCacheRef = useRef(new Map());
    useMemo(() => { leafColumnsCacheRef.current = new Map(); }, [leftColIdSet, rightColIdSet, centerColIdSet]);

    // Stable header style objects — rebuilt only on theme change.
    const headerContentBaseStyle = useMemo(() => ({
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
        width: '100%',
        padding: '0 4px',
        overflow: 'hidden',
        minWidth: autoSizeBounds.minWidth,
    }), [autoSizeBounds.minWidth]);

    const headerTextStyle = useMemo(() => ({
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        flex: 1,
        minWidth: 0,
    }), []);

    const moreVertStyle = useMemo(() => ({
        display: 'flex',
        alignItems: 'center',
        padding: '2px',
        borderRadius: '4px',
        cursor: 'pointer',
        color: theme.textSec,
        opacity: 0.6,
    }), [theme.textSec]);

    // Render Header Cell for Split Sections
    // overrideWidth: when set, replaces the computed section width (used for partially-visible
    // group headers during center-column virtualization so the width matches only the visible leaves).
    // Wrapped in useCallback so PivotTableBody's React.memo can skip re-renders
    // when only unrelated parent state (sidebar, charts, context menus) changes.
    const renderHeaderCell = useCallback((header, level, renderSection = 'center', overrideWidth = null, disableSticky = false) => {
        const isGroupHeader = header.column.columns && header.column.columns.length > 0;
        const isHierarchyHeader = header.column.id === 'hierarchy';
        const isMeasureSubHeader = !isGroupHeader && !isHierarchyHeader && header.column.id !== '__row_number__';
        const headerText = typeof header.column.columnDef.header === 'string' ? header.column.columnDef.header : '';
        const parentText = header.column.parent && typeof header.column.parent.columnDef?.header === 'string' ? header.column.parent.columnDef.header : '';
        const isTotalGroupHeader = isGroupHeader && (headerText === 'Grand Total' || headerText.startsWith('Grand Total'));
        const isUnderTotalGroup = isMeasureSubHeader && (parentText === 'Grand Total' || parentText.startsWith('Grand Total'));
        const isSorted = header.column.getIsSorted();
        const sortIndex = header.column.getSortIndex();
        const isMultiSort = tableRef.current ? tableRef.current.getState().sorting.length > 1 : false;
        const isResizingColumn = header.column.getIsResizing();
        const isHoveredHeader = hoveredHeaderIdRef.current === header.column.id;
        const isFocusedHeader = focusedHeaderIdRef.current === header.column.id;
        const isResizeHandleVisible = isResizingColumn || isHoveredHeader || isFocusedHeader;
        const isPinned = header.column.getIsPinned();
        // Use cached leaf columns to avoid O(n) tree walk per header per render
        let leafColumns = leafColumnsCacheRef.current.get(header.column.id);
        if (!leafColumns) {
            leafColumns = header.column.getLeafColumns ? header.column.getLeafColumns() : [header.column];
            leafColumnsCacheRef.current.set(header.column.id, leafColumns);
        }
        const sectionLeafIds = renderSection === 'left' ? leftColIdSet
            : renderSection === 'right' ? rightColIdSet : centerColIdSet;
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
        const headerBaseBackground = isHierarchyHeader
            ? (theme.hierarchyBg || theme.headerSubtleBg || theme.headerBg)
            : theme.headerBg;

        // Calculate sticky style for pinned headers using the hook
        const stickyStyle = disableSticky
            ? { background: mergeStateStyles({ background: headerBaseBackground }, sortedHeaderStyle).background }
            : getHeaderStickyStyle(
                header,
                level,
                renderSection,
                mergeStateStyles({ background: headerBaseBackground }, sortedHeaderStyle).background
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
                } : isHierarchyHeader ? {
                    background: theme.hierarchyBg || theme.headerSubtleBg || theme.headerBg,
                    color: theme.text,
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
            data-header-column-id={String(header.column.id)}
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
            onContextMenu={(e) => handleHeaderContextMenu(e, header.column.id, header, level)}
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
                    ...headerContentBaseStyle,
                    justifyContent: header.column.id === 'hierarchy' ? 'flex-start' : isMeasureSubHeader ? 'flex-end' : 'center',
                }}
                data-header-content="true"
                data-header-column-id={String(header.column.id)}>
                <span style={headerTextStyle}
                data-header-text="true">
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
                    onClick={(e) => { e.stopPropagation(); handleHeaderContextMenu(e, header.column.id, header, level); }}
                    style={moreVertStyle}
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
    }, [
        theme,
        isDarkTheme,
        styles.headerCell,
        rowHeight,
        // tableRef, hoveredHeaderIdRef, focusedHeaderIdRef are stable refs —
        // read at call time, not listed as deps (identity never changes).
        selectedCols,
        filters,
        activeFilterCol,
        filterAnchorEl,
        activeFilterOptions,
        autoSizeColumn,
        handleHeaderContextMenu,
        handleFilterClick,
        handleHeaderFilter,
        closeFilterPopover,
        setHoveredHeaderId,
        setFocusedHeaderId,
        onDragStart,
        getHeaderStickyStyle,
        leftColIdSet,
        rightColIdSet,
        centerColIdSet,
        leafColumnsCacheRef,
        headerContentBaseStyle,
        headerTextStyle,
        moreVertStyle,
    ]);

    return { renderCell, renderVirtualColumnCell, renderHeaderCell };
}

import { useCallback, useEffect, useMemo, useRef } from 'react';
import React from 'react';
import Icons from '../utils/Icons';
import FilterPopover from '../components/Filters/FilterPopover';
import { flexRender } from '@tanstack/react-table';
import { buildEditedCellVisualStyle, mergeStateStyles, withAlpha, resolveDepthToken, resolveGridLineBorders, toCssPx } from '../utils/styles';
import { formatDisplayLabel } from '../utils/helpers';
import { EDITED_CELL_FORMAT_KEY } from '../utils/formatting';
import { findSortSpecifier, isAbsoluteSortSpecifier } from '../utils/sorting';
import {
    HEADER_STYLE_CONFIG_KEYS,
    addHeaderCandidate,
    hasHeaderStyleKeys,
    headerRuleKindMatches,
    headerRuleTargetMatches,
    normalizeHeaderStyle,
} from '../presentation/headerFormatting';
import { CellErrorBoundary, SafeCellContent } from '../presentation/safeCellContent';
import { resolveDataBarMaximum } from '../utils/dataBars';

/** @type {any} */
const HEADER_FORMATTING_RESERVED_KEYS = new Set([
    'default', 'defaultStyle', 'all', '*', 'columns', 'byColumn', 'columnStyles', 'rules', 'headerRules',
]);

/** @param {any} [header] @returns {any} */
export const resolveHeaderRowSpan = (header) => {
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


/**
 * useRenderHelpers — extracts renderCell and renderHeaderCell from DashTanstackPivot.
 *
 * CODE-03 stale closure audit for renderHeaderCell:
 *   - renderHeaderCell is a useCallback whose dependency list includes the
 *     values that can change between renders.
 *   - handleHeaderContextMenu, autoSizeColumn, autoSizeBounds, setHoveredHeaderId,
 *     setFocusedHeaderId, onDragStart, handleFilterClick, handleHeaderFilter,
 *     onHeaderDrop, onHeaderDragOver, onHeaderKeyboardMove, onHeaderKeyboardPin,
 *     activeFilterCol, filterAnchorEl, closeFilterPopover, activeFilterOptions,
 *     filters, selectedCols, leftCols, rightCols, centerCols, headerFormatting,
 *     styles, rowHeight,
 *     hoveredHeaderId, focusedHeaderId, theme — all are read synchronously when
 *     renderHeaderCell is called, capturing the value from the current render pass.
  @param {any} p0 @returns {any} */
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
    headerHeight: headerHeightProp,
    rowFields,
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
    onHeaderDrop,
    onHeaderDragOver,
    onHeaderDragEnd,
    onHeaderKeyboardMove,
    onHeaderKeyboardPin,
    dragOverHeaderId,
    dragOverSide,
    handleFilterClick,
    handleHeaderFilter,
    activeFilterCol,
    filterAnchorEl,
    closeFilterPopover,
    activeFilterOptions,
    activeFilterOptionMeta,
    requestFilterOptions,
    filters,
    selectedCols,
    getHeaderStickyStyle,
    dataBarsColumns,
    dataBarMaximums,
    colorScaleStats,
    cellFormatRules,
    resolveEditedCellMarker,
    resolveCellDisplayValue,
    editedCellEpoch,
    headerFormatting,
    getLocaleText,
    wrapHeaders,
    table,
    selectedRowKeys,
    toggleRowCheckbox,
    rowCheckboxMode,
}) {
    /** @type {(...args: any[]) => any} */
    const localize = useCallback((key, fallback, values = {}) => {
        /** @type {any} */
        const rawText = typeof getLocaleText === 'function'
            ? getLocaleText(key, fallback, values)
            : fallback;
        /** @type {any} */
        const text = rawText === null || rawText === undefined ? '' : String(rawText);
        if (!values || typeof values !== 'object') return text;
        return text.replace(/\{([A-Za-z0-9_]+)\}/g, /** @param {any} [match] @param {any} [valueKey] @returns {any} */ (match, valueKey) => (
            Object.prototype.hasOwnProperty.call(values, valueKey)
                ? String(values[valueKey])
                : match
        ));
    }, [getLocaleText]);

    /** @type {any} */
    const effectiveHeaderHeight = headerHeightProp || rowHeight;
    /** @type {any} */
    const gridBorders = useMemo(() => resolveGridLineBorders(theme), [theme]);

    // --- Stable style objects extracted from the per-cell hot path ---
    // These are recreated only when theme/styles change, not per cell per scroll frame.
    /** @type {any} */
    const baseCellStyle = useMemo(() => ({
        ...styles.cell,
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        fontFamily: theme.fontFamily || "var(--sp-font-sans, 'Inter', ui-sans-serif, system-ui, sans-serif)",
        fontSize: theme.fontSize !== undefined && theme.fontSize !== null
            ? `var(--sp-font-size, ${toCssPx(theme.fontSize, '13px')})`
            : 'var(--sp-font-size, inherit)',
        userSelect: 'none',
    }), [styles.cell, theme.fontFamily, theme.fontSize]);

    /** @type {any} */
    const selectedOverlayRef = useMemo(() => ({
        background: theme.select,
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.select, theme.primary]);

    /** @type {any} */
    const selectedEditedOverlayRef = useMemo(() => ({
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.primary]);

    /** @type {any} */
    const focusOverlayRef = useMemo(() => ({
        outline: `1px solid ${theme.primary}`,
        outlineOffset: '-1px',
    }), [theme.primary]);

    /** @type {any} */
    const fillOverlayRef = useMemo(() => ({
        boxShadow: `inset 0 0 0 1px ${theme.primary}`,
    }), [theme.primary]);

    /** @type {any} */
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

    /** @type {any} */
    const grandTotalBorderStyle = useMemo(() => {
        /** @type {any} */
        const rightBorderColor = gridBorders.columnBorderColor === 'transparent'
            ? 'transparent'
            : (withAlpha(theme.primary, 0.4) || theme.primary);
        if (theme.totalBorder) {
            /** @type {any} */
            const borderVal = String(theme.totalBorder).includes(' ') ? theme.totalBorder : `2px solid ${theme.totalBorder}`;
            return {
                borderTop: borderVal,
                borderBottom: borderVal,
                borderRight: `1px solid ${rightBorderColor}`,
            };
        }
        return {
            borderTop: `2px solid ${theme.primary}`,
            borderBottom: `2px solid ${theme.primary}`,
            borderRight: `1px solid ${rightBorderColor}`,
        };
    }, [theme.primary, theme.totalBorder, gridBorders.columnBorderColor]);

    // Pre-computed theme backgrounds — avoids repeated ternary chains per cell.
    /** @type {any} */
    const themeBgs = useMemo(() => ({
        total: theme.totalBg || theme.select || theme.background,
        grandTotal: theme.totalBgStrong || theme.totalBg || theme.select || theme.background,
        hierarchy: theme.hierarchyBg || theme.surfaceInset || theme.surfaceBg || theme.background || '#fff',
        normal: theme.leafBg || theme.surfaceBg || theme.background || '#fff',
        totalText: theme.totalText || theme.text || theme.primary,
        grandTotalText: theme.totalTextStrong || theme.totalText || theme.primary,
    }), [theme]);

    /** @type {any} */
    const EMPTY_STYLE = useMemo(() => ({}), []);
    /** @type {any} */
    const DATA_BAR_SPAN_STYLE = useMemo(() => ({ position: 'relative', zIndex: 1 }), []);
    /** @type {any} */
    const SKELETON_CELL_STYLE = useMemo(() => ({
        width: '70%',
        maxWidth: '120px',
        height: '10px',
        borderRadius: '999px',
        background: 'var(--pivot-loading-cell-gradient, linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%))',
        backgroundSize: '220% 100%',
        animation: 'pivot-skeleton-shimmer var(--pivot-loading-shimmer-duration, 2.8s) linear infinite',
    }), []);

    /** @type {(...args: any[]) => any} */
    const renderResolvedCell = useCallback((cellLike, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!cellLike) return null;
        const disableSticky = !!renderOptions.disableSticky;

        const row = cellLike.row;
        const col = cellLike.column;
        /** @type {any} */
        const rowSpanState = renderOptions.rowSpan || null;
        /** @type {any} */
        const rowSpanHeight = rowSpanState && rowSpanState.rowSpan > 1 && Number(rowSpanState.height) > 0
            ? Number(rowSpanState.height)
            : null;
        if (rowSpanState && rowSpanState.hidden) {
            return (
                <div
                    key={cellLike.id}
                    role="gridcell"
                    aria-hidden="true"
                    data-rowid={row.id}
                    data-colid={col.id}
                    data-rowspan-hidden="true"
                    style={{
                        ...baseCellStyle,
                        width: col.getSize(),
                        height: '100%',
                        visibility: 'hidden',
                        pointerEvents: 'none',
                    }}
                />
            );
        }
        /** @type {any} */
        const colIndex = visibleLeafColIndexMap.get(col.id) !== undefined ? visibleLeafColIndexMap.get(col.id) : -1;
        /** @type {any} */
        const isReportConfigGutter = Boolean(col.columnDef && col.columnDef.meta && col.columnDef.meta.reportConfigGutter);
        /** @type {any} */
        const isHierarchy = col.id === 'hierarchy';
        /** @type {any} */
        const colParentHeader = col.parent && typeof col.parent.columnDef?.header === 'string' ? col.parent.columnDef.header : '';
        /** @type {any} */
        const isTotalCol = !isHierarchy && !isReportConfigGutter && (colParentHeader === 'Grand Total' || colParentHeader.startsWith('Grand Total'));
        /** @type {any} */
        const isSelected = !isReportConfigGutter && Object.prototype.hasOwnProperty.call(
            selectedCells || {},
            `${row.id}:${col.id}`
        );
        /** @type {any} */
        const isLastSelected = lastSelected && (lastSelected.rowId === undefined ? lastSelected.rowIndex === virtualRowIndex : lastSelected.rowId === row.id) && (lastSelected.colId === undefined ? lastSelected.colIndex === colIndex : lastSelected.colId === col.id);
        let isFillSelected = false;
        if (fillRange && dragStart) {
             if (virtualRowIndex >= fillRange.rStart && virtualRowIndex <= fillRange.rEnd && colIndex >= fillRange.cStart && colIndex <= fillRange.cEnd) {
                 isFillSelected = true;
             }
        }

        /** @type {any} */
        const rowPath = row.original && row.original._path ? row.original._path : row.id;
        const cellKey = `${rowPath}:::${col.id}`;
        /** @type {any} */
        const rawCellValue = cellLike.getValue();
        /** @type {any} */
        const preserveAccessorDisplayValue = Boolean(
            col.columnDef
            && col.columnDef.meta
            && col.columnDef.meta.preserveAccessorDisplayValue
        );
        /** @type {any} */
        const resolvedCellValue = preserveAccessorDisplayValue
            ? rawCellValue
            : (
                typeof resolveCellDisplayValue === 'function'
                    ? resolveCellDisplayValue(rowPath, col.id, rawCellValue)
                    : rawCellValue
            );
        /** @type {Record<string, any>} */
        const displayCellLike = {
            ...cellLike,
            getValue: () => resolvedCellValue,
            renderValue: () => (resolvedCellValue ?? null),
        };
        /** @type {any} */
        const baseCellContext = typeof cellLike.getContext === 'function'
            ? cellLike.getContext()
            : {};
        /** @type {Record<string, any>} */
        const displayCellContext = {
            ...baseCellContext,
            cell: displayCellLike,
            getValue: displayCellLike.getValue,
            renderValue: displayCellLike.renderValue,
        };
        displayCellLike.getContext = () => displayCellContext;
        /** @type {any} */
        const cellFmt = cellFormatRules && cellFormatRules[cellKey];
        /** @type {any} */
        const editedCellFmt = cellFormatRules && cellFormatRules[EDITED_CELL_FORMAT_KEY];
        /** @type {any} */
        const editedMarker = typeof resolveEditedCellMarker === 'function'
            ? resolveEditedCellMarker(rowPath, col.id)
            : null;

        const isGrandTotalRow = !!(row.original && row.original._isTotal);
        /** @type {any} */
        const reportFormat = row.original
            && row.original._reportFormat
            && typeof row.original._reportFormat === 'object'
            && !isReportConfigGutter
            ? row.original._reportFormat
            : null;
        /** @type {any} */
        const reportCellStyle = reportFormat ? {} : EMPTY_STYLE;
        if (reportFormat && reportFormat.rowColor) {
            reportCellStyle.background = reportFormat.rowColor;
        }
        if (reportFormat && reportFormat.borderStyle) {
            /** @type {any} */
            const reportBorderWidth = Number.isFinite(Number(reportFormat.borderWidth))
                ? Math.max(1, Number(reportFormat.borderWidth))
                : 1;
            reportCellStyle.border = `${reportBorderWidth}px ${reportFormat.borderStyle} ${reportFormat.borderColor || theme.border}`;
        }
        /** @type {any} */
        const depth = Number(row.original && row.original.depth !== undefined ? row.original.depth : (row.depth || 0));
        /** @type {any} */
        const isGroupRow = !isGrandTotalRow && (Boolean(row.original && row.original._has_children) || (typeof row.getIsGrouped === 'function' && row.getIsGrouped()) || Boolean(row.subRows && row.subRows.length > 0));
        /** @type {any} */
        const groupRowBg = isGroupRow ? resolveDepthToken(theme.groupBg, depth, null) : null;
        /** @type {any} */
        const groupRowText = isGroupRow ? resolveDepthToken(theme.groupText, depth, null) : null;
        /** @type {any} */
        const groupRowWeight = isGroupRow ? resolveDepthToken(theme.groupFontWeight, depth, null) : null;
        /** @type {any} */
        const themeBackground = isGrandTotalRow
            ? themeBgs.grandTotal
            : isTotalCol
                ? themeBgs.total
                : isReportConfigGutter
                    ? (theme.background || theme.surfaceBg || '#fff')
                    : groupRowBg
                        ? groupRowBg
                        : (!isGroupRow && theme.leafBg)
                            ? theme.leafBg
                            : (isHierarchy ? themeBgs.hierarchy : themeBgs.normal);
        /** @type {any} */
        const isSparklineCol = Boolean(col.columnDef?.meta?.isSparklineSummary);
        /** @type {any} */
        const condStyle = (isReportConfigGutter || isSparklineCol) ? EMPTY_STYLE : getConditionalStyle(col.id, resolvedCellValue, row.original, row.id);
        /** @type {any} */
        const editedVisualStyle = editedMarker
            ? buildEditedCellVisualStyle(theme, editedCellFmt, editedMarker, { emphasizeText: true })
            : null;
        /** @type {any} */
        const editedBaseStyle = editedVisualStyle || EMPTY_STYLE;
        /** @type {any} */
        const hasExplicitCellBg = Boolean(
            (cellFmt && cellFmt.bg)
            || condStyle?.background
            || condStyle?.backgroundColor
            || editedVisualStyle
            || isSelected
            || isGrandTotalRow
            || isTotalCol
            || reportCellStyle.background
        );
        /** @type {any} */
        const stickyBaseStyle = cellFmt && cellFmt.bg
            ? mergeStateStyles({ background: themeBackground }, reportCellStyle, condStyle, editedBaseStyle, { background: cellFmt.bg })
            : mergeStateStyles({ background: themeBackground }, reportCellStyle, condStyle, editedBaseStyle);
        /** @type {any} */
        const resolvedStickyBg = (!hasExplicitCellBg && !isReportConfigGutter)
            ? `var(--sp-row-current-bg, ${stickyBaseStyle.background || stickyBaseStyle.backgroundColor || themeBackground})`
            : (stickyBaseStyle.background || stickyBaseStyle.backgroundColor);
        /** @type {any} */
        const stickyStyle = disableSticky
            ? { background: resolvedStickyBg }
            : getStickyStyle(cellLike.column, resolvedStickyBg);
        /** @type {any} */
        const selectionOverlayStyle = isSelected
            ? (editedMarker ? selectedEditedOverlayRef : selectedOverlayRef)
            : EMPTY_STYLE;
        /** @type {any} */
        const cellStateStyle = mergeStateStyles(
            stickyBaseStyle,
            stickyStyle,
            selectionOverlayStyle,
            isLastSelected ? focusOverlayRef : EMPTY_STYLE,
            isFillSelected ? fillOverlayRef : EMPTY_STYLE,
        );
        /** @type {any} */
        const cellTextAlign = cellFmt && ['left', 'center', 'right'].includes(cellFmt.textAlign)
            ? cellFmt.textAlign
            : null;
        /** @type {any} */
        const cellJustifyContent = cellTextAlign === 'left'
            ? 'flex-start'
            : (cellTextAlign === 'center' ? 'center' : (cellTextAlign === 'right' ? 'flex-end' : null));
        /** @type {any} */
        const cellBorderStyle = cellFmt && cellFmt.borderStyle
            ? {
                border: `${Math.max(1, Math.min(5, Number(cellFmt.borderWidth) || 1))}px ${cellFmt.borderStyle} ${cellFmt.borderColor || theme.border}`,
            }
            : EMPTY_STYLE;

        /** @type {any} */
        let cellContent;
        if (col.id === '__row_number__' && isVirtualRow) {
            cellContent = (row.original && typeof row.original.__virtualIndex === 'number')
                ? row.original.__virtualIndex + 1
                : (typeof row.index === 'number' ? row.index + 1 : virtualRowIndex + 1);
        } else {
            /** @type {any} */
            const rowData = row.original || {};
            /** @type {any} */
            const hasFetchedColumn = Object.prototype.hasOwnProperty.call(rowData, col.id);
            const showPendingColumnPlaceholder = (
                serverSide &&
                !!rowData.__colPending &&
                !isHierarchy &&
                !isReportConfigGutter &&
                col.id !== '__row_number__' &&
                !hasFetchedColumn
            );

            if (showPendingColumnPlaceholder) {
                cellContent = <span aria-hidden="true" style={SKELETON_CELL_STYLE} />;
            } else {
                /** @type {any} */
                const resetValue = resolvedCellValue === null || typeof resolvedCellValue !== 'object'
                    ? String(resolvedCellValue)
                    : '';
                cellContent = (
                    <CellErrorBoundary resetKey={`${cellLike.id}:${editedCellEpoch}:${resetValue}`}>
                        <SafeCellContent render={() => flexRender(col.columnDef.cell, displayCellLike.getContext())} />
                    </CellErrorBoundary>
                );
            }
        }

        // Data bars
        const rawNumValue = resolvedCellValue;
        /** @type {any} */
        const isDataBar = !isHierarchy
            && !isReportConfigGutter
            && dataBarsColumns && dataBarsColumns.has(col.id)
            && typeof rawNumValue === 'number'
            && !Number.isNaN(rawNumValue)
            && !(row.original && row.original._isTotal);
        /** @type {any} */
        let dataBarEl = null;
        if (isDataBar) {
            /** @type {any} */
            const stats = colorScaleStats && colorScaleStats.byCol && colorScaleStats.byCol[col.id];
            /** @type {any} */
            const absMax = resolveDataBarMaximum(dataBarMaximums && dataBarMaximums[col.id], stats);
            if (absMax !== null) {
                /** @type {any} */
                const pct = Math.min(1, Math.abs(rawNumValue) / absMax);
                /** @type {any} */
                const isNeg = rawNumValue < 0;
                /** @type {any} */
                const barOpacity = theme.dataBarOpacity !== undefined && theme.dataBarOpacity !== null ? Number(theme.dataBarOpacity) : null;
                /** @type {any} */
                const rawPosColor = theme.dataBarPositive || (isDarkTheme(theme) ? 'rgba(100,180,255,0.22)' : 'rgba(37,99,235,0.13)');
                /** @type {any} */
                const rawNegColor = theme.dataBarNegative || (isDarkTheme(theme) ? 'rgba(255,100,100,0.18)' : 'rgba(220,38,38,0.12)');
                /** @type {any} */
                const barColor = isNeg
                    ? ((theme.dataBarNegative && Number.isFinite(barOpacity)) ? (withAlpha(rawNegColor, barOpacity, rawNegColor) || rawNegColor) : rawNegColor)
                    : ((theme.dataBarPositive && Number.isFinite(barOpacity)) ? (withAlpha(rawPosColor, barOpacity, rawPosColor) || rawPosColor) : rawPosColor);
                /** @type {any} */
                const barBorder = isNeg
                    ? (theme.dataBarNegative ? (withAlpha(rawNegColor, 0.55, rawNegColor) || rawNegColor) : (isDarkTheme(theme) ? 'rgba(255,100,100,0.5)' : 'rgba(220,38,38,0.35)'))
                    : (theme.dataBarPositive ? (withAlpha(rawPosColor, 0.65, rawPosColor) || rawPosColor) : (isDarkTheme(theme) ? 'rgba(100,180,255,0.6)' : 'rgba(37,99,235,0.45)'));
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

        /** @type {any} */
        const dataMeasureAttr = (!isHierarchy && !isReportConfigGutter && col.id !== '__row_number__' && col.id !== '__row_checkbox__')
            ? (col.columnDef?.meta?.measureField || col.id)
            : undefined;
        /** @type {any} */
        const resolvedCellFontFamily = (!isHierarchy && !isReportConfigGutter && col.id !== '__row_number__' && theme.numericFontFamily)
            ? theme.numericFontFamily
            : undefined;

        return (
            <div
                key={cellLike.id}
                role="gridcell"
                aria-selected={isSelected}
                data-rowid={row.id}
                data-colid={col.id}
                data-measure={dataMeasureAttr}
                data-databar={dataBarEl ? 'true' : undefined}
                data-rowspan={rowSpanHeight ? rowSpanState.rowSpan : undefined}
                data-report-config-gutter={isReportConfigGutter ? 'true' : undefined}
                onMouseDown={isReportConfigGutter ? undefined : (e) => handleCellMouseDown(e, virtualRowIndex, colIndex, row.id, col.id, resolvedCellValue)}
                onMouseEnter={isReportConfigGutter ? undefined : () => handleCellMouseEnter(virtualRowIndex, colIndex)}
                style={{
                    ...baseCellStyle,
                    ...(resolvedCellFontFamily ? { fontFamily: resolvedCellFontFamily } : {}),
                    width: col.getSize(),
                    height: rowSpanHeight || '100%',
                    minHeight: rowSpanHeight || undefined,
                    alignSelf: rowSpanHeight ? 'flex-start' : undefined,
                    justifyContent: isReportConfigGutter ? 'center' : (cellJustifyContent || (isHierarchy ? 'flex-start' : 'flex-end')),
                    textAlign: cellTextAlign || (isHierarchy ? 'left' : 'right'),
                    fontVariantNumeric: (isHierarchy || isReportConfigGutter) ? undefined : (theme.tabularNums === false ? 'normal' : 'tabular-nums'),
                    fontWeight: cellFmt && cellFmt.bold
                        ? 'bold'
                        : (editedVisualStyle && editedVisualStyle.fontWeight)
                            ? editedVisualStyle.fontWeight
                            : (reportFormat && reportFormat.bold)
                                ? 700
                                : (isGrandTotalRow
                                    ? (theme.totalFontWeight ?? 700)
                                    : isTotalCol
                                        ? 600
                                        : (isGroupRow
                                            ? (groupRowWeight ?? (isHierarchy ? 500 : (theme.leafFontWeight ?? 400)))
                                            : (theme.leafFontWeight ?? 400))),
                    fontStyle: cellFmt && cellFmt.italic
                        ? 'italic'
                        : (editedVisualStyle && editedVisualStyle.fontStyle ? editedVisualStyle.fontStyle : undefined),
                    color: cellFmt && cellFmt.color
                        ? cellFmt.color
                        : (editedVisualStyle && editedVisualStyle.color)
                            ? editedVisualStyle.color
                            : (dataBarEl && theme.dataBarText && theme.dataBarText !== 'inherit')
                                ? theme.dataBarText
                                : isGrandTotalRow
                                    ? themeBgs.grandTotalText
                                    : (isGroupRow && groupRowText)
                                        ? groupRowText
                                        : ((isHierarchy || isReportConfigGutter)
                                            ? (theme.text || undefined)
                                            : (isTotalCol ? themeBgs.totalText : (theme.textSec || theme.text))),
                    ...cellStateStyle,
                    position: !disableSticky && cellStateStyle.position === 'sticky' ? 'sticky' : 'relative',
                    zIndex: rowSpanHeight
                        ? Math.max(Number(cellStateStyle.zIndex) || 0, 6)
                        : cellStateStyle.zIndex,
                    ...(isGrandTotalRow ? grandTotalBorderStyle : {}),
                    ...cellBorderStyle,
                    ...(isReportConfigGutter ? {
                        padding: 0,
                        borderLeft: 'none',
                        borderRight: 'none',
                        borderBottom: `1px solid ${theme.border}`,
                        background: theme.background || theme.surfaceBg || '#fff',
                        boxShadow: 'none',
                        color: theme.textSec,
                    } : {}),
                }}
                onContextMenu={isReportConfigGutter ? undefined : e => handleContextMenu(e, resolvedCellValue, col.id, row)}
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
        dataBarMaximums,
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
        grandTotalBorderStyle,
        EMPTY_STYLE,
        DATA_BAR_SPAN_STYLE,
        SKELETON_CELL_STYLE,
    ]);

    // --- Helper to Render a single real TanStack Cell with useCallback ---
    /** @type {(...args: any[]) => any} */
    const renderCell = useCallback((cell, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!cell) return null;
        return renderResolvedCell(cell, virtualRowIndex, isVirtualRow, renderOptions);
    }, [renderResolvedCell]);

    // Render a lightweight pseudo-cell for the currently visible columns only.
    // This avoids forcing TanStack to materialize cells for every leaf column in a row.
    /** @type {(...args: any[]) => any} */
    const renderVirtualColumnCell = useCallback((row, column, virtualRowIndex, isVirtualRow = false, renderOptions = {}) => {
        if (!row || !column) return null;
        /** @returns {any} */
        const getValue = () => row.getValue(column.id);
        /** @type {Record<string, any>} */
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
    /** @type {any} */
    const leftColIdSet = useMemo(() => new Set(leftCols.map(/** @param {any} [c] @returns {any} */ c => c.id)), [leftCols]);
    /** @type {any} */
    const rightColIdSet = useMemo(() => new Set(rightCols.map(/** @param {any} [c] @returns {any} */ c => c.id)), [rightCols]);
    /** @type {any} */
    const centerColIdSet = useMemo(() => new Set(centerCols.map(/** @param {any} [c] @returns {any} */ c => c.id)), [centerCols]);
    // Cache getLeafColumns() results by column id to avoid repeated tree walks.
    // Cleared when column structure changes (tracked via the section sets above).
    /** @type {any} */
    const leafColumnsCacheRef = useRef(new Map());
    useEffect(() => { leafColumnsCacheRef.current = new Map(); }, [leftColIdSet, rightColIdSet, centerColIdSet]);

    // Stable header style objects — rebuilt only on theme change.
    /** @type {any} */
    const headerContentBaseStyle = useMemo(() => ({
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
        width: '100%',
        padding: '0 4px',
        overflow: 'hidden',
        minWidth: 0,
    }), []);

    /** @type {any} */
    const effectiveHeaderWrap = theme.headerWrap !== undefined ? Boolean(theme.headerWrap) : wrapHeaders;
    /** @type {any} */
    const headerTextStyle = useMemo(() => effectiveHeaderWrap ? ({
        flex: 1,
        minWidth: 0,
        whiteSpace: 'normal',
        wordBreak: 'normal',
        overflowWrap: 'break-word',
        lineHeight: theme.headerLineHeight !== undefined && theme.headerLineHeight !== null ? theme.headerLineHeight : 1.3,
    }) : ({
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        flex: 1,
        minWidth: 0,
        ...(theme.headerLineHeight !== undefined && theme.headerLineHeight !== null ? { lineHeight: theme.headerLineHeight } : {}),
    }), [effectiveHeaderWrap, theme.headerLineHeight]);

    /** @type {any} */
    const moreVertStyle = useMemo(() => ({
        display: 'flex',
        alignItems: 'center',
        padding: '2px',
        borderRadius: '4px',
        cursor: 'pointer',
        color: theme.textSec,
        opacity: 0.6,
    }), [theme.textSec]);

    /** @type {any} */
    const sortPriorityBadgeStyle = useMemo(() => ({
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        minWidth: '16px',
        height: '16px',
        padding: '0 4px',
        borderRadius: '8px',
        fontSize: '10px',
        lineHeight: 1,
        fontWeight: 800,
        color: '#fff',
        background: theme.sortedHeaderBorder || theme.primary,
        flexShrink: 0,
    }), [theme.primary, theme.sortedHeaderBorder]);

    /** @type {any} */
    const absoluteSortBadgeStyle = useMemo(() => ({
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '16px',
        padding: '0 4px',
        borderRadius: '6px',
        border: `1px solid ${theme.sortedHeaderBorder || theme.primary}`,
        color: theme.sortedHeaderText || theme.primary,
        fontSize: '9px',
        lineHeight: 1,
        fontWeight: 800,
        flexShrink: 0,
    }), [theme.primary, theme.sortedHeaderBorder, theme.sortedHeaderText]);

    /** @type {any} */
    const sortingSignature = Array.isArray(sorting)
        ? sorting.map((sortSpec) => `${sortSpec && sortSpec.id}:${sortSpec && sortSpec.desc ? 'desc' : 'asc'}:${isAbsoluteSortSpecifier(sortSpec) ? 'abs' : ''}`).join('|')
        : '';

    /** @type {any} */
    const normalizedHeaderFormatting = useMemo(() => {
        /** @type {Record<string, any>} */
        const normalized = {
            defaultStyle: {},
            columnStyles: new Map(),
            rules: [],
        };
        /** @param {any} [style] @returns {any} */
        const mergeDefault = (style) => {
            /** @type {any} */
            const normalizedStyle = normalizeHeaderStyle(style);
            if (Object.keys(normalizedStyle).length > 0) {
                normalized.defaultStyle = { ...normalized.defaultStyle, ...normalizedStyle };
            }
        };
        /** @param {any} [key] @param {any} [style] @returns {any} */
        const addColumnStyle = (key, style) => {
            if (key === undefined || key === null) return;
            /** @type {any} */
            const normalizedStyle = normalizeHeaderStyle(style);
            if (Object.keys(normalizedStyle).length === 0) return;
            /** @type {any} */
            const rawKey = String(key).trim();
            if (!rawKey) return;
            normalized.columnStyles.set(rawKey, {
                ...(normalized.columnStyles.get(rawKey) || {}),
                ...normalizedStyle,
            });
            normalized.columnStyles.set(rawKey.toLowerCase(), {
                ...(normalized.columnStyles.get(rawKey.toLowerCase()) || {}),
                ...normalizedStyle,
            });
        };

        if (Array.isArray(headerFormatting)) {
            normalized.rules = headerFormatting.filter(rule => rule && typeof rule === 'object');
            return normalized;
        }
        if (!headerFormatting || typeof headerFormatting !== 'object') {
            return normalized;
        }

        mergeDefault(headerFormatting.default);
        mergeDefault(headerFormatting.defaultStyle);
        mergeDefault(headerFormatting.all);
        mergeDefault(headerFormatting['*']);

        /** @type {any} */
        const explicitColumnMap = headerFormatting.columns || headerFormatting.byColumn || headerFormatting.columnStyles;
        if (explicitColumnMap && typeof explicitColumnMap === 'object' && !Array.isArray(explicitColumnMap)) {
            Object.entries(explicitColumnMap).forEach(([key, style]) => addColumnStyle(key, style));
        }

        /** @type {any} */
        const explicitRules = Array.isArray(headerFormatting.rules)
            ? headerFormatting.rules
            : (Array.isArray(headerFormatting.headerRules) ? headerFormatting.headerRules : []);
        normalized.rules = explicitRules.filter(/** @param {any} [rule] @returns {any} */ rule => rule && typeof rule === 'object');

        if (hasHeaderStyleKeys(headerFormatting)) {
            mergeDefault(headerFormatting);
        }

        if (!explicitColumnMap) {
            Object.entries(headerFormatting).forEach(([key, style]) => {
                if (HEADER_FORMATTING_RESERVED_KEYS.has(key) || HEADER_STYLE_CONFIG_KEYS.has(key)) return;
                addColumnStyle(key, style);
            });
        }

        return normalized;
    }, [headerFormatting]);

    /** @type {(...args: any[]) => any} */
    const resolveHeaderFormatStyle = useCallback((header, level, headerKind) => {
        /** @type {Record<string, any>} */
        const style = {};
        /** @param {any} [candidateStyle] @returns {any} */
        const mergeStyle = (candidateStyle) => {
            /** @type {any} */
            const normalizedStyle = normalizeHeaderStyle(candidateStyle);
            if (Object.keys(normalizedStyle).length > 0) {
                Object.assign(style, normalizedStyle);
            }
        };
        /** @type {any} */
        const columnDef = header && header.column ? header.column.columnDef || {} : {};
        /** @type {any} */
        const candidates = new Set();
        if (header) addHeaderCandidate(candidates, header.id);
        if (header && header.column) {
            addHeaderCandidate(candidates, header.column.id);
            addHeaderCandidate(candidates, header.column.accessorKey);
        }
        addHeaderCandidate(candidates, columnDef.id);
        addHeaderCandidate(candidates, columnDef.accessorKey);
        addHeaderCandidate(candidates, columnDef.headerVal);
        addHeaderCandidate(candidates, columnDef.groupValue);
        if (typeof columnDef.header === 'string') {
            addHeaderCandidate(candidates, columnDef.header);
        }

        mergeStyle(normalizedHeaderFormatting.defaultStyle);
        candidates.forEach(/** @param {any} [candidateKey] @returns {any} */ (candidateKey) => {
            /** @type {any} */
            const candidateStyle = normalizedHeaderFormatting.columnStyles.get(candidateKey);
            if (candidateStyle) mergeStyle(candidateStyle);
        });
        normalizedHeaderFormatting.rules.forEach(/** @param {any} [rule] @returns {any} */ (rule) => {
            if (headerRuleTargetMatches(rule, candidates) && headerRuleKindMatches(rule, level, headerKind)) {
                mergeStyle(rule);
            }
        });
        if (columnDef.meta && columnDef.meta.headerStyle) {
            mergeStyle(columnDef.meta.headerStyle);
        }
        if (columnDef.headerStyle) {
            mergeStyle(columnDef.headerStyle);
        }
        return style;
    }, [normalizedHeaderFormatting]);

    // A context-menu anchor is a virtual rectangle, not a DOM filter button.
    // Give it one canonical real leaf owner across the split header groups.
    /** @type {any} */
    const activeFilterOwner = useMemo(() => {
        if (!activeFilterCol || !filterAnchorEl) return null;
        /** @type {any} */
        const clickedOwner = filterAnchorEl.getAttribute?.('data-pivot-filter-owner');
        if (clickedOwner) return clickedOwner;
        /** @type {any} */
        const currentTable = tableRef?.current || table;
        for (const [section, getter] of [['left', 'getLeftHeaderGroups'],
            ['center', 'getCenterHeaderGroups'], ['right', 'getRightHeaderGroups']]) {
            /** @type {any} */
            const coveredColIds = new Set();
            for (const group of currentTable?.[getter]?.() || []) {
                for (const candidate of group.headers) {
                    if (candidate.column.columns?.length) continue;
                    /** @type {any} */
                    const span = resolveHeaderRowSpan(candidate);
                    if (coveredColIds.has(candidate.column.id)) continue;
                    if (candidate.isPlaceholder && span <= 1) continue;
                    if (candidate.isPlaceholder && span > 1) {
                        coveredColIds.add(candidate.column.id);
                    }
                    /** @type {any} */
                    const field = candidate.column.id === 'hierarchy' ? rowFields?.[0] : candidate.column.id;
                    if (field === activeFilterCol) return `${section}:${candidate.id}`;
                }
            }
        }
        return null;
    }, [activeFilterCol, filterAnchorEl, tableRef, table, rowFields, leftCols, centerCols, rightCols]);

    // Render Header Cell for Split Sections
    // overrideWidth: when set, replaces the computed section width (used for partially-visible
    // group headers during center-column virtualization so the width matches only the visible leaves).
    // Wrapped in useCallback so PivotTableBody's React.memo can skip re-renders
    // when only unrelated parent state (sidebar, charts, context menus) changes.
    /** @type {(...args: any[]) => any} */
    const renderHeaderCell = useCallback((header, level, renderSection = 'center', overrideWidth = null, disableSticky = false, headerRenderMeta = null) => {
        // A leaf column also occurs in placeholder groups and pinned sections.
        // Only the header whose filter button opened the popover owns its state.
        const filterOwner = `${renderSection}:${header.id}`;
        /** @type {any} */
        const isGroupHeader = header.column.columns && header.column.columns.length > 0;
        /** @type {any} */
        const headerRowSpan = resolveHeaderRowSpan(header);
        /** @type {any} */
        const isSpanningLeaf = Boolean(header.isPlaceholder && headerRowSpan > 1 && !isGroupHeader);
        /** @type {any} */
        const isEmptyPlaceholder = Boolean(header.isPlaceholder && !isSpanningLeaf);
        /** @type {any} */
        const isReportConfigGutter = Boolean(header.column.columnDef && header.column.columnDef.meta && header.column.columnDef.meta.reportConfigGutter);
        /** @type {any} */
        const isHierarchyHeader = header.column.id === 'hierarchy';
        /** @type {any} */
        const filterColumnId = isHierarchyHeader
            ? (Array.isArray(rowFields) && rowFields.length ? rowFields[0] : null)
            : header.column.id;
        /** @type {any} */
        const isMeasureSubHeader = !isGroupHeader && !isHierarchyHeader && !isReportConfigGutter && header.column.id !== '__row_number__';
        /** @type {any} */
        const headerText = typeof header.column.columnDef.header === 'string' ? header.column.columnDef.header : '';
        /** @type {any} */
        const parentText = header.column.parent && typeof header.column.parent.columnDef?.header === 'string' ? header.column.parent.columnDef.header : '';
        /** @type {any} */
        const isTotalGroupHeader = isGroupHeader && (headerText === 'Grand Total' || headerText.startsWith('Grand Total'));
        /** @type {any} */
        const isUnderTotalGroup = isMeasureSubHeader && (parentText === 'Grand Total' || parentText.startsWith('Grand Total'));
        /** @type {any} */
        const headerKind = isHierarchyHeader
            ? 'hierarchy'
            : (isTotalGroupHeader || isUnderTotalGroup)
                ? 'total'
                : isGroupHeader
                    ? 'group'
                    : isMeasureSubHeader
                        ? 'measure'
                        : 'leaf';
        /** @type {any} */
        const headerFormatStyle = resolveHeaderFormatStyle(header, level, headerKind);
        /** @type {any} */
        const isSorted = !isEmptyPlaceholder && header.column.getIsSorted();
        /** @type {any} */
        const sortIndex = header.column.getSortIndex();
        /** @type {any} */
        const sortSpecifier = findSortSpecifier(sorting, header.column.id);
        /** @type {any} */
        const isAbsoluteSort = isAbsoluteSortSpecifier(sortSpecifier);
        /** @type {any} */
        const hasSortPriority = !isGroupHeader && !isEmptyPlaceholder && isSorted && Number.isFinite(Number(sortIndex)) && Number(sortIndex) >= 0;
        /** @type {any} */
        const isResizingColumn = header.column.getIsResizing();
        /** @type {any} */
        const isHoveredHeader = hoveredHeaderIdRef.current === header.column.id;
        /** @type {any} */
        const isFocusedHeader = focusedHeaderIdRef.current === header.column.id;
        /** @type {any} */
        const isResizeHandleVisible = isResizingColumn || isHoveredHeader || isFocusedHeader;
        /** @type {any} */
        const isPinned = header.column.getIsPinned();
        // Use cached leaf columns to avoid O(n) tree walk per header per render
        /** @type {any} */
        let leafColumns = leafColumnsCacheRef.current.get(header.column.id);
        if (!leafColumns) {
            leafColumns = header.column.getLeafColumns ? header.column.getLeafColumns() : [header.column];
            leafColumnsCacheRef.current.set(header.column.id, leafColumns);
        }
        /** @type {any} */
        const sectionLeafIds = renderSection === 'left' ? leftColIdSet
            : renderSection === 'right' ? rightColIdSet : centerColIdSet;
        /** @type {any} */
        const sectionWidth = leafColumns
            .filter(/** @param {any} [column] @returns {any} */ column => sectionLeafIds.has(column.id))
            .reduce(/** @param {any} [sum] @param {any} [column] @returns {any} */ (sum, column) => sum + column.getSize(), 0);
        /** @type {any} */
        const headerWidth = overrideWidth !== null ? overrideWidth : (sectionWidth || header.getSize());
        if (header.column.id === '__row_checkbox__') {
            /** @type {any} */
            const isMulti = rowCheckboxMode === 'multiple';
            /** @type {any} */
            const allDataRows = table ? table.getRowModel().rows.filter(
                /** @param {any} [r] @returns {any} */ r => r.original && !r.original._isTotal && r.original._path !== '__grand_total__'
            ) : [];
            /** @type {any} */
            const allKeys = allDataRows.map(/** @param {any} [r] @returns {any} */ r => r.original._path || r.id);
            /** @type {any} */
            const allChecked = allKeys.length > 0 && allKeys.every(/** @param {any} [k] @returns {any} */ k => selectedRowKeys && selectedRowKeys.has(k));
            /** @type {any} */
            const someChecked = allKeys.some(/** @param {any} [k] @returns {any} */ k => selectedRowKeys && selectedRowKeys.has(k));
            /** @type {any} */
            const cbBg = theme.headerBg || theme.surfaceBg || '#fff';
            /** @type {any} */
            const checkboxStickyStyle = disableSticky ? { background: cbBg } : getHeaderStickyStyle(header, level, renderSection, cbBg);
            return (
                <div
                    key={header.id}
                    style={{
                        ...styles.headerCell,
                        ...checkboxStickyStyle,
                        width: headerWidth,
                        minWidth: headerWidth,
                        flexShrink: 0,
                        height: effectiveHeaderHeight,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        cursor: isMulti ? 'pointer' : 'default',
                        borderBottom: `1px solid ${gridBorders.headerBorderBottomColor}`,
                        position: checkboxStickyStyle.position || 'relative',
                    }}
                    data-header-column-id="__row_checkbox__"
                    role="columnheader"
                    aria-label={isMulti ? localize('selectAllRows', 'Select all rows') : localize('rowSelection', 'Row selection')}
                >
                    {isMulti && allKeys.length > 0 && (
                        <input
                            type="checkbox"
                            checked={allChecked}
                            ref={(el) => { if (el) el.indeterminate = someChecked && !allChecked; }}
                            onChange={() => {
                                if (typeof toggleRowCheckbox === 'function') {
                                    allKeys.forEach(/** @param {any} [k] @returns {any} */ k => {
                                        const shouldSelect = !allChecked;
                                        /** @type {any} */
                                        const isSelected = selectedRowKeys && selectedRowKeys.has(k);
                                        if (shouldSelect !== isSelected) toggleRowCheckbox(k);
                                    });
                                }
                            }}
                            style={{ cursor: 'pointer', width: 14, height: 14 }}
                            aria-label={localize('selectAllRows', 'Select all rows')}
                        />
                    )}
                </div>
            );
        }

        if (isReportConfigGutter) {
            /** @type {any} */
            const gutterBackground = theme.background || theme.surfaceBg || '#fff';
            /** @type {any} */
            const gutterStickyStyle = disableSticky
                ? { background: gutterBackground }
                : getHeaderStickyStyle(header, level, renderSection, gutterBackground);
            return (
                <div
                    key={header.id}
                    style={{
                        ...styles.headerCell,
                        ...gutterStickyStyle,
                        width: headerWidth,
                        minWidth: headerWidth,
                        flexShrink: 0,
                        height: effectiveHeaderHeight,
                        padding: 0,
                        cursor: 'default',
                        background: gutterBackground,
                        borderLeft: 'none',
                        borderRight: 'none',
                        borderBottom: `1px solid ${gridBorders.headerBorderBottomColor}`,
                        boxShadow: 'none',
                        color: theme.textSec,
                        position: gutterStickyStyle.position || 'relative',
                    }}
                    data-header-column-id={String(header.column.id)}
                    data-report-config-gutter="true"
                    role="columnheader"
                    aria-label="Report row settings"
                />
            );
        }
        /** @type {any} */
        const sortIndicatorMode = theme.sortIndicator || 'arrow';
        /** @type {any} */
        const showSortPriorityBadge = theme.showSortPriority !== false;
        /** @type {any} */
        const headerControlsMode = theme.headerControls || 'always';
        const headerMeasureField = header.column.columnDef?.meta?.measureField;
        /** @type {any} */
        const rawHeaderAlign = typeof theme.headerAlign === 'string'
            ? theme.headerAlign
            : (theme.headerAlign && typeof theme.headerAlign === 'object'
                ? (theme.headerAlign[header.column.id] ?? (headerMeasureField ? theme.headerAlign[headerMeasureField] : undefined) ?? theme.headerAlign.default ?? null)
                : null);
        /** @type {any} */
        const resolvedHeaderJustify = rawHeaderAlign === 'left'
            ? 'flex-start'
            : rawHeaderAlign === 'center'
                ? 'center'
                : rawHeaderAlign === 'right'
                    ? 'flex-end'
                    : (header.column.id === 'hierarchy' ? 'flex-start' : isMeasureSubHeader ? 'flex-end' : 'center');
        /** @type {any} */
        const resolvedHeaderTextAlign = rawHeaderAlign === 'left'
            ? 'left'
            : rawHeaderAlign === 'center'
                ? 'center'
                : rawHeaderAlign === 'right'
                    ? 'right'
                    : (header.column.id === 'hierarchy' ? 'left' : isMeasureSubHeader ? 'right' : 'center');
        /** @type {any} */
        const sortedHeaderStyle = !isGroupHeader && isSorted ? {
            background: theme.sortedHeaderBg || theme.select,
            borderBottom: `${sortIndicatorMode === 'underline' ? '2px' : '1px'} solid ${theme.sortedHeaderBorder || theme.primary}`,
            color: theme.sortedHeaderText || theme.primary,
            fontWeight: 700
        } : {};
        /** @type {any} */
        const isHeaderSelected = !isGroupHeader && selectedCols.has(header.column.id);
        /** @type {any} */
        const interactionOverlayStyle = !isGroupHeader && (isFocusedHeader || isHeaderSelected || isResizingColumn)
            ? { boxShadow: `inset 0 0 0 1px ${theme.primary}` }
            : {};
        /** @type {any} */
        const sortIconColor = isSorted ? (theme.sortedHeaderText || theme.primary) : theme.textSec;
        /** @type {any} */
        const headerBaseBackground = isHierarchyHeader
            ? (theme.hierarchyBg || theme.headerSubtleBg || theme.headerBg)
            : theme.headerBg;
        /** @type {any} */
        const headerLabel = typeof header.column.columnDef.header === 'string'
            ? (header.column.columnDef.meta?.preserveHeaderLabel
                ? header.column.columnDef.header
                : formatDisplayLabel(header.column.columnDef.header))
            : String(header.column.id);
        /** @type {any} */
        const sortAriaDescription = isSorted
            ? ` Sorted ${isSorted === 'desc' ? 'descending' : 'ascending'}${hasSortPriority ? ` with priority ${sortIndex + 1}` : ''}${isAbsoluteSort ? ' by absolute value' : ''}.`
            : '';
        /** @type {any} */
        const sortInstruction = localize('headerSortInstruction', 'Click or press Alt+Up/Down to sort.');
        /** @type {any} */
        const moveInstruction = localize('headerMoveInstruction', 'Press Ctrl+Left or Ctrl+Right to move. Press Alt+Shift+Left or Alt+Shift+Right to pin.');
        /** @type {any} */
        const headerAriaLabel = localize('headerAriaLabel', '{label}.{sort} {sortInstruction} {moveInstruction}', {
            label: headerLabel,
            sort: sortAriaDescription,
            sortInstruction,
            moveInstruction,
        });

        // Calculate sticky style for pinned headers using the hook
        /** @type {any} */
        const stickyStyle = disableSticky
            ? { background: mergeStateStyles({ background: headerBaseBackground }, sortedHeaderStyle, headerFormatStyle).background }
            : getHeaderStickyStyle(
                header,
                level,
                renderSection,
                mergeStateStyles({ background: headerBaseBackground }, sortedHeaderStyle, headerFormatStyle).background
            );
        /** @type {any} */
        const headerStateStyle = mergeStateStyles(
            styles.headerCell,
            sortedHeaderStyle,
            stickyStyle,
            interactionOverlayStyle
        );

        if (headerRenderMeta && headerRenderMeta.coveredByPlaceholder) {
            return (
                <div
                    key={header.id}
                    aria-hidden="true"
                    data-header-spacer="true"
                    style={{
                        width: headerWidth,
                        minWidth: headerWidth,
                        flexShrink: 0,
                        pointerEvents: 'none',
                        borderBottom: 'none',
                    }}
                />
            );
        }

        /** @type {any} */
        const spanTargetHeight = (headerRenderMeta && Number.isFinite(headerRenderMeta.spanHeight) && headerRenderMeta.spanHeight > 0)
            ? headerRenderMeta.spanHeight
            : (effectiveHeaderHeight * headerRowSpan);
        /** @type {any} */
        const isSpanningHeaderCell = isSpanningLeaf && headerRowSpan > 1;
        /** @type {any} */
        const computedHeaderHeight = isSpanningHeaderCell ? spanTargetHeight : effectiveHeaderHeight;
        const headerCellElement = (
            <div key={isSpanningHeaderCell ? undefined : header.id} style={{
                ...headerStateStyle,
                width: headerWidth,
                minWidth: headerWidth,
                flexShrink: 0,
                height: isSpanningHeaderCell
                    ? computedHeaderHeight
                    : (effectiveHeaderWrap ? 'auto' : computedHeaderHeight),
                minHeight: computedHeaderHeight,
                cursor: isEmptyPlaceholder ? 'default' : 'pointer',
                // Position is handled by getHeaderStickyStyle or spanning wrapper
                position: isSpanningHeaderCell ? 'absolute' : (headerStateStyle.position || 'relative'),
                ...(isSpanningHeaderCell ? {
                    top: 0,
                    left: 0,
                    zIndex: Math.max(Number(headerStateStyle.zIndex || 0), 3),
                } : {}),
                ...(theme.headerFontSize !== undefined && theme.headerFontSize !== null ? { fontSize: toCssPx(theme.headerFontSize, undefined) } : {}),
                ...(theme.headerFontWeight !== undefined && theme.headerFontWeight !== null ? { fontWeight: theme.headerFontWeight } : {}),
                ...(theme.headerTextTransform !== undefined && theme.headerTextTransform !== null ? { textTransform: theme.headerTextTransform } : {}),
                ...(theme.headerLetterSpacing !== undefined && theme.headerLetterSpacing !== null ? {
                    letterSpacing: typeof theme.headerLetterSpacing === 'number' ? `${theme.headerLetterSpacing}px` : theme.headerLetterSpacing,
                } : {}),
                ...(isTotalGroupHeader ? {
                    background: theme.totalBgStrong || (theme.totalBg || theme.select),
                    color: theme.totalTextStrong || theme.totalText || theme.primary,
                    fontWeight: theme.totalFontWeight ?? 700,
                } : isHierarchyHeader ? {
                    background: theme.hierarchyBg || theme.headerSubtleBg || theme.headerBg,
                    color: theme.text,
                } : isGroupHeader && !isSorted ? {
                    background: theme.headerGroupBg || (isDarkTheme(theme) ? theme.headerBg : (theme.headerSubtleBg || '#F9FAFB')),
                    ...(theme.headerGroupText ? { color: theme.headerGroupText } : {}),
                    ...(theme.headerGroupFontSize !== undefined && theme.headerGroupFontSize !== null ? { fontSize: toCssPx(theme.headerGroupFontSize, undefined) } : {}),
                } : {}),
                ...(isMeasureSubHeader ? {
                    fontWeight: theme.headerFontWeight !== undefined ? theme.headerFontWeight : (isUnderTotalGroup ? 700 : 500),
                    textTransform: theme.headerTextTransform !== undefined ? theme.headerTextTransform : 'uppercase',
                    letterSpacing: theme.headerLetterSpacing !== undefined && theme.headerLetterSpacing !== null
                        ? (typeof theme.headerLetterSpacing === 'number' ? `${theme.headerLetterSpacing}px` : theme.headerLetterSpacing)
                        : '0.04em',
                    color: isUnderTotalGroup ? (theme.totalTextStrong || theme.primary) : (theme.textSec || theme.text),
                    background: isUnderTotalGroup ? (theme.totalBgStrong || theme.totalBg || theme.select) : undefined,
                } : {}),
                ...(isEmptyPlaceholder ? { borderBottom: 'none', cursor: 'default' } : {}),
                ...headerFormatStyle
            }}
            data-header-column-id={String(header.column.id)}
            data-header-row-span={headerRowSpan > 1 ? headerRowSpan : undefined}
            data-pinned={isPinned || undefined}
            role="columnheader"
            aria-sort={isSorted || 'none'}
            aria-label={headerAriaLabel}
            tabIndex={0}
            onKeyDown={(e) => {
                if (isEmptyPlaceholder) return;
                if (e.altKey && !e.shiftKey && (e.key === 'ArrowUp' || e.key === 'ArrowDown')) {
                    e.preventDefault();
                    header.column.toggleSorting(e.key === 'ArrowDown', e.shiftKey);
                    return;
                }
                if (!isGroupHeader && (e.ctrlKey || e.metaKey) && !e.altKey && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
                    e.preventDefault();
                    if (typeof onHeaderKeyboardMove === 'function') {
                        onHeaderKeyboardMove(header.column.id, e.key === 'ArrowLeft' ? -1 : 1);
                    }
                    return;
                }
                if (!isGroupHeader && e.altKey && e.shiftKey && (e.key === 'ArrowLeft' || e.key === 'ArrowRight' || e.key === 'ArrowDown')) {
                    e.preventDefault();
                    if (typeof onHeaderKeyboardPin === 'function') {
                        /** @type {any} */
                        const side = e.key === 'ArrowLeft' ? 'left' : (e.key === 'ArrowRight' ? 'right' : false);
                        onHeaderKeyboardPin(header.column.id, side);
                    }
                }
            }}
            draggable={!isEmptyPlaceholder && !isGroupHeader && header.column.id !== '__row_number__' && !header.column.getIsResizing()}
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
            onDragOver={(e) => {
                if (typeof onHeaderDragOver === 'function') {
                    onHeaderDragOver(e, header.column.id, header);
                }
            }}
            onDrop={(e) => {
                if (typeof onHeaderDrop === 'function') {
                    onHeaderDrop(e, header.column.id, header);
                }
            }}
            onDragEnd={(e) => {
                if (typeof onHeaderDragEnd === 'function') {
                    onHeaderDragEnd(e, header.column.id, header);
                }
            }}
            onContextMenu={(e) => handleHeaderContextMenu(e, header.column.id, header, level)}
            onMouseEnter={() => setHoveredHeaderId(header.column.id)}
            onMouseLeave={() => setHoveredHeaderId(/** @param {any} [current] @returns {any} */ current => (current === header.column.id ? null : current))}
            onFocus={() => setFocusedHeaderId(header.column.id)}
            onBlur={(e) => {
                if (!e.currentTarget.contains(e.relatedTarget)) {
                    setFocusedHeaderId(/** @param {any} [current] @returns {any} */ current => (current === header.column.id ? null : current));
                }
            }}
            onClick={isEmptyPlaceholder ? undefined : header.column.getToggleSortingHandler()}>
                <div style={{
                    ...headerContentBaseStyle,
                    justifyContent: resolvedHeaderJustify,
                    textAlign: resolvedHeaderTextAlign,
                }}
                data-header-content="true"
                data-header-column-id={String(header.column.id)}>
                {headerControlsMode !== 'never' && !isEmptyPlaceholder && !isGroupHeader && header.column.id !== '__row_number__' && header.column.id !== 'hierarchy' && (
                    <span
                        className="sp-header-control"
                        style={{ color: theme.textSec, opacity: 0.4, cursor: 'grab', fontSize: '13px', marginRight: '2px', flexShrink: 0, lineHeight: 1 }}
                        title="Drag to reorder"
                        aria-hidden="true"
                    >⠿</span>
                )}
                <span style={{ ...headerTextStyle, textAlign: resolvedHeaderTextAlign }}
                data-header-text="true">
                    {isEmptyPlaceholder ? null : (typeof header.column.columnDef.header === 'string'
                        ? (header.column.columnDef.meta?.shortLabel && headerWidth < 80
                            ? header.column.columnDef.meta.shortLabel
                            : (header.column.columnDef.meta?.preserveHeaderLabel
                                ? header.column.columnDef.header
                                : formatDisplayLabel(header.column.columnDef.header)))
                        : flexRender(header.column.columnDef.header, header.getContext()))}
                </span>

                {headerControlsMode !== 'never' && !isGroupHeader && filterColumnId && header.column.id !== '__row_number__' && !isEmptyPlaceholder && (
                    <div
                        className="sp-header-control"
                        onClick={(e) => {
                            e.stopPropagation();
                            handleFilterClick(e, filterColumnId);
                        }}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault();
                                e.stopPropagation();
                                handleFilterClick(e, filterColumnId);
                            }
                        }}
                        role="button"
                        tabIndex={0}
                        data-pivot-filter-owner={filterOwner}
                        style={{
                            display:'flex',
                            alignItems: 'center',
                            padding: '2px',
                            borderRadius: '4px',
                            background: filters[filterColumnId] ? theme.select : 'transparent',
                            color: filters[filterColumnId] ? theme.primary : 'inherit'
                        }}
                        aria-label={localize('filterColumn', 'Filter Column')}
                        title={localize('filterColumn', 'Filter Column')}
                    >
                        <Icons.Filter/>
                    </div>
                )}

                {!isGroupHeader && isSorted && sortIndicatorMode === 'arrow' && (
                    <span style={{display: 'inline-flex', alignItems: 'center', color: sortIconColor}}>
                        {isSorted === 'asc' ? <Icons.SortAsc/> : <Icons.SortDesc/>}
                    </span>
                )}
                {hasSortPriority && showSortPriorityBadge && (
                    <span style={sortPriorityBadgeStyle} title={`Sort priority ${sortIndex + 1}`}>{sortIndex + 1}</span>
                )}
                {!isGroupHeader && isSorted && isAbsoluteSort && (
                    <span style={absoluteSortBadgeStyle} title="Absolute value sort">|x|</span>
                )}

                {headerControlsMode !== 'never' && !isEmptyPlaceholder && !isGroupHeader && (
                    <div
                        className="sp-header-control"
                        onClick={(e) => { e.stopPropagation(); handleHeaderContextMenu(e, header.column.id, header, level); }}
                        onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault();
                                e.stopPropagation();
                                handleHeaderContextMenu(e, header.column.id, header, level);
                            }
                        }}
                        role="button"
                        tabIndex={0}
                        style={moreVertStyle}
                        aria-label={localize('moreColumnOptions', 'More Column Options')}
                        title={localize('moreColumnOptions', 'More Column Options')}
                    >
                        <Icons.MoreVert/>
                    </div>
                )}
                </div>

                {!isGroupHeader && !isEmptyPlaceholder && activeFilterCol === filterColumnId
                    && activeFilterOwner === filterOwner && (
                    <FilterPopover
                    column={isHierarchyHeader ? { id: filterColumnId, header: headerText } : header.column}
                    anchorEl={filterAnchorEl}
                    onClose={closeFilterPopover}
                    onFilter={/** @param {any} [type] @param {any} [val] @returns {any} */ (type, val) => handleHeaderFilter(filterColumnId, type, val)}
                    currentFilter={filters[filterColumnId]}
                    options={activeFilterCol === filterColumnId ? activeFilterOptions : []}
                    optionMeta={activeFilterCol === filterColumnId ? activeFilterOptionMeta : null}
                    onSearchOptions={serverSide ? /** @param {any} [search] @returns {any} */ (search) => requestFilterOptions && requestFilterOptions(filterColumnId, { search, offset: 0 }) : undefined}
                    onLoadMoreOptions={serverSide ? /** @param {any} [search] @param {any} [offset] @returns {any} */ (search, offset) => requestFilterOptions && requestFilterOptions(filterColumnId, { search, offset }) : undefined}
                    theme={theme}
                    />
                )}
                {!isGroupHeader && filterColumnId && filters[filterColumnId] && filters[filterColumnId].conditions && (
                <div style={{fontSize: '10px', color: theme.primary, paddingTop: '2px', textAlign: 'center'}}>
                    {filters[filterColumnId].conditions.map(/** @param {any} [c] @returns {any} */ c => `${c.type}: ${c.value}${c.caseSensitive ? ' (Match Case)' : ''}`).join(` ${filters[filterColumnId].operator} `)}
                </div>
                )}

                {dragOverHeaderId === header.column.id && (
                    <div style={{
                        position: 'absolute',
                        top: 0, bottom: 0,
                        [dragOverSide === 'before' ? 'left' : 'right']: 0,
                        width: '2px',
                        background: theme.primary,
                        zIndex: 200,
                        pointerEvents: 'none',
                    }} />
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

        if (isSpanningHeaderCell) {
            return (
                <div
                    key={header.id}
                    data-header-span-slot="true"
                    style={{
                        width: headerWidth,
                        minWidth: headerWidth,
                        flexShrink: 0,
                        position: 'relative',
                        height: '100%',
                    }}
                >
                    {headerCellElement}
                </div>
            );
        }

        return headerCellElement;
    }, [
        theme,
        isDarkTheme,
        styles.headerCell,
        rowHeight,
        effectiveHeaderHeight,
        effectiveHeaderWrap,
        gridBorders,
        rowFields,
        // tableRef, hoveredHeaderIdRef, focusedHeaderIdRef are stable refs —
        // read at call time, not listed as deps (identity never changes).
        selectedCols,
        filters,
        activeFilterCol,
        activeFilterOwner,
        filterAnchorEl,
        activeFilterOptions,
        activeFilterOptionMeta,
        autoSizeColumn,
        handleHeaderContextMenu,
        handleFilterClick,
        handleHeaderFilter,
        requestFilterOptions,
        closeFilterPopover,
        setHoveredHeaderId,
        setFocusedHeaderId,
        onDragStart,
        onHeaderDrop,
        onHeaderDragOver,
        onHeaderDragEnd,
        onHeaderKeyboardMove,
        onHeaderKeyboardPin,
        dragOverHeaderId,
        dragOverSide,
        localize,
        getHeaderStickyStyle,
        leftColIdSet,
        rightColIdSet,
        centerColIdSet,
        leafColumnsCacheRef,
        headerContentBaseStyle,
        headerTextStyle,
        moreVertStyle,
        sortPriorityBadgeStyle,
        absoluteSortBadgeStyle,
        resolveHeaderFormatStyle,
        sortingSignature,
        wrapHeaders,
        table,
        selectedRowKeys,
        toggleRowCheckbox,
        rowCheckboxMode,
    ]);

    return { renderCell, renderVirtualColumnCell, renderHeaderCell };
}

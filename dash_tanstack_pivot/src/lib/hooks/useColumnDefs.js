import { useMemo } from 'react';
import React from 'react';
import Icons from '../components/Icons';
import EditableCell from '../components/Table/EditableCell';
import { formatValue, formatDisplayLabel, getKey } from '../utils/helpers';

const debugLog = process.env.NODE_ENV !== 'production'
    ? (...args) => console.log('[pivot-grid]', ...args)
    : () => {};

/**
 * useColumnDefs — extracts the columns useMemo from DashTanstackPivot.
 *
 * Closed-over values that are NOT in the dep array (intentionally excluded or
 * come from stable refs) are listed below for CODE-03 stale-closure audit:
 *   - filteredData: intentionally excluded (see comment in dep array)
 *   - renderedOffset: used inside a cell render fn (executes at render time,
 *     not at useMemo compute time), so the value seen is always fresh
 *   - handleContextMenu, handleRowSelect, handleRowRangeSelect,
 *     setIsRowSelecting, setRowDragStart: stable callbacks (useCallback)
 *     — not included in dep array but do not cause stale behaviour because
 *     the cell functions re-run every render pass from flexRender
 *   - defaultColumnWidths, debugLog, theme, validationRules, setProps,
 *     isColExpanded, toggleCol, pendingRowTransitions: read at render time
 *     from current scope; same stable-ref / render-time-read pattern
 */
export function useColumnDefs({
    sortOptions,
    serverSide,
    showRowNumbers,
    layoutMode,
    rowFields,
    colFields,
    valConfigs,
    minMax,
    colorScaleMode,
    colExpanded,
    isRowSelecting,
    rowDragStart,
    props,
    cachedColSchema,
    filteredData,
    // Render-time closures (not in dep array — stable refs or render-time reads)
    theme,
    defaultColumnWidths,
    validationRules,
    setProps,
    handleContextMenu,
    handleRowSelect,
    handleRowRangeSelect,
    setIsRowSelecting,
    setRowDragStart,
    renderedOffset,
    isColExpanded,
    toggleCol,
    pendingRowTransitions,
    decimalPlaces,
    columnDecimalOverrides,
    cellFormatRules,
}) {
    return useMemo(() => {
        // Helper: render a numeric cell value with decimal precision and negative-red coloring
        const renderNumericCell = (value, fmt, rowPath, colId) => {
            const cellDec = colId !== undefined && columnDecimalOverrides && columnDecimalOverrides[colId] !== undefined
                ? columnDecimalOverrides[colId]
                : decimalPlaces;
            const formatted = formatValue(value, fmt, cellDec);
            const isNegative = typeof value === 'number' && value < 0;
            const cellKey = rowPath && colId ? `${rowPath}:::${colId}` : null;
            const cellFmt = cellFormatRules && cellKey ? cellFormatRules[cellKey] : null;
            const color = cellFmt && cellFmt.color ? cellFmt.color : (isNegative ? 'red' : undefined);
            const bgColor = cellFmt && cellFmt.bg ? cellFmt.bg : undefined;
            const fontWeight = cellFmt && cellFmt.bold ? 'bold' : undefined;
            const fontStyle = cellFmt && cellFmt.italic ? 'italic' : undefined;
            const extraStyle = {};
            if (color) extraStyle.color = color;
            if (bgColor) extraStyle.background = bgColor;
            if (fontWeight) extraStyle.fontWeight = fontWeight;
            if (fontStyle) extraStyle.fontStyle = fontStyle;
            return { formatted, extraStyle };
        };

        // Enhanced Sorting Logic (Tree-aware + Natural + Customization)
        const customSortingFn = (rowA, rowB, columnId) => {
            try {
                // Safety check for loading rows (server-side)
                if (!rowA.original || !rowB.original) return 0;

                // 1. Special handling for grand total - it should always be at the end
                // Check multiple ways to identify the grand total
                const aIsGrandTotal = rowA.id === '__grand_total__' ||
                                     rowA.original.__isGrandTotal__ ||
                                     rowA.original._path === '__grand_total__' ||
                                     rowA.original._id === 'Grand Total';
                const bIsGrandTotal = rowB.id === '__grand_total__' ||
                                     rowB.original.__isGrandTotal__ ||
                                     rowB.original._path === '__grand_total__' ||
                                     rowB.original._id === 'Grand Total';

                // If one is grand total and the other is not, grand total goes last
                if (aIsGrandTotal && !bIsGrandTotal) return 1;
                if (!aIsGrandTotal && bIsGrandTotal) return -1;

                // If both are grand totals, they are equal
                if (aIsGrandTotal && bIsGrandTotal) return 0;

                // 2. Regular totals (but not grand total) should come after non-totals
                const aIsRegularTotal = (rowA.original && rowA.original._isTotal) && !aIsGrandTotal;
                const bIsRegularTotal = (rowB.original && rowB.original._isTotal) && !bIsGrandTotal;

                if (aIsRegularTotal && !bIsRegularTotal) return 1;
                if (!aIsRegularTotal && bIsRegularTotal) return -1;

                // Both are regular totals (not grand totals) - they can be equal for sorting purposes
                if (aIsRegularTotal && bIsRegularTotal) return 0;

                const valA = rowA.getValue(columnId);
                const valB = rowB.getValue(columnId);

                // 2. Column-Specific Customization
                const colSortOptions = (sortOptions.columnOptions && sortOptions.columnOptions[columnId]) || {};
                const isNatural = colSortOptions.naturalSort !== undefined ? colSortOptions.naturalSort : (sortOptions.naturalSort !== false);
                const isCaseSensitive = colSortOptions.caseSensitive !== undefined ? colSortOptions.caseSensitive : sortOptions.caseSensitive;

                if (isNatural) {
                    const sensitivity = isCaseSensitive ? 'variant' : 'base';
                    return new Intl.Collator(undefined, { numeric: true, sensitivity }).compare(String(valA || ''), String(valB || ''));
                }

                // Default Alphanumeric with configured sensitivity
                const defaultSensitivity = isCaseSensitive ? 'variant' : 'base';
                return String(valA || '').localeCompare(String(valB || ''), undefined, { numeric: true, sensitivity: defaultSensitivity });
            } catch (err) {
                console.error('Sorting error:', err);
                return 0;
            }
        };

        const sortingFn = serverSide ? 'auto' : customSortingFn;
        const hierarchyCols = [];

        if (showRowNumbers) {
                hierarchyCols.push({
                    id: '__row_number__',
                    header: '#',
                    size: defaultColumnWidths.rowNumber,
                    enablePinning: false, // User Request: Cannot be changed
                cell: ({ row }) => (
                    <div
                        style={{
                            width: '100%',
                            height: '100%',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            background: theme.headerSubtleBg || '#f5f5f5',
                            cursor: 'pointer',
                            fontSize: '11px',
                            color: theme.textSec,
                            borderRight: `1px solid ${theme.border}`,
                            userSelect: 'none'
                        }}
                        onMouseDown={(e) => {
                            if (e.button !== 0) return;
                            e.stopPropagation();
                            setIsRowSelecting(true);
                            setRowDragStart(row.index);
                            handleRowSelect(row, e.shiftKey, e.ctrlKey || e.metaKey);
                        }}
                        onMouseEnter={() => {
                            if (isRowSelecting && rowDragStart !== null) {
                                handleRowRangeSelect(rowDragStart, row.index);
                            }
                        }}
                    >
                        {(row.original && (row.original._isTotal || row.original._path === '__grand_total__' || row.original._id === 'Grand Total'))
                            ? (props.rowCount != null ? props.rowCount : '∑')
                            : (row.index + 1 + (serverSide ? (renderedOffset || 0) : 0))}
                    </div>
                )
            });
        }

        if (layoutMode === 'hierarchy') {
            if (rowFields.length > 0) {
                hierarchyCols.push({
                    id: 'hierarchy',
                    accessorFn: row => row._id,
                    header: rowFields.map(formatDisplayLabel).join(' > '),
                    size: defaultColumnWidths.hierarchy,
                    sortingFn, // Apply sort
                    cell: ({ row }) => {
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        return (
                            <div
                                style={{
                                    paddingLeft: `${depth * 24}px`,
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%'
                                    // isSelected styling will be applied in renderCell
                                }}
                            >
                                 {row.getCanExpand() && !(row.original && row.original._isTotal) ? (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            debugLog('Toggling expansion (hierarchy) for', row.id);
                                            row.getToggleExpandedHandler()(e);
                                        }}
                                        onMouseDown={(e) => e.stopPropagation()}
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:theme.textSec,display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                ) : <span style={{width:'18px'}}/>}
                                <span style={{ fontWeight: (row.original && row.original._isTotal) ? 700 : 400 }}>{row.original ? row.original._id : ''}</span>
                            </div>
                        );
                    }
                });
            }
        } else {
            rowFields.forEach((field, i) => {
                hierarchyCols.push({
                    id: field,
                    accessorKey: field,
                    header: formatDisplayLabel(field),
                    size: defaultColumnWidths.dimension,
                    enablePinning: true,
                    sortingFn,
                    cell: ({ row, getValue }) => {
                        const val = getValue();
                        // Note: We removed selectedCells from this dependency to avoid unnecessary re-renders
                        // isSelected is calculated dynamically in the renderCell function instead
                        const depth = (row.original.depth !== undefined) ? row.original.depth : (row.depth || 0);

                        // Outline: Show only if current column matches depth (step layout)
                        // Tabular: Show if column is <= depth (repeat labels)
                        let showValue = true;
                        if (layoutMode === 'outline') {
                            if (i !== depth) showValue = false;
                        } else {
                            // Tabular
                            if (i > depth) showValue = false;
                        }

                        // Expander only on the active level column
                        const showExpander = (i === depth) && row.getCanExpand() && !(row.original && row.original._isTotal);

                        return (
                            <div
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    width: '100%',
                                    height: '100%',
                                    fontWeight: (row.original && row.original._isTotal) ? 700 : 400
                                    // isSelected styling will be applied in renderCell
                                }}
                            >
                                {showExpander && (
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            debugLog('Toggling expansion (mode) for', row.id);
                                            row.getToggleExpandedHandler()(e);
                                        }}
                                        onMouseDown={(e) => e.stopPropagation()}
                                        style={{border:'none',background:'none',cursor:'pointer',padding:0,marginRight:'6px',color:theme.textSec,display:'flex'}}
                                    >
                                        {row.getIsExpanded() ? <Icons.ChevronDown/> : <Icons.ChevronRight/>}
                                        {pendingRowTransitions.has(row.id) && (
                                            <span style={{fontSize: '10px', opacity: 0.75, marginLeft: '3px'}}>...</span>
                                        )}
                                    </button>
                                )}
                                {showValue ? val : ''}
                            </div>
                        );
                    }
                });
            });
        }

        let dataCols = [];
        if (colFields.length === 0) {
            dataCols = valConfigs.map(c => ({
                id: getKey('', c.field, c.agg),
                accessorFn: row => row[getKey('', c.field, c.agg)] ,
                header: `${formatDisplayLabel(c.field)} (${c.agg})`,
                size: defaultColumnWidths.measure,
                enablePinning: true,
                sortingFn,
                cell: info => {
                    const rowPath = info.row.original && info.row.original._path;
                    const { formatted, extraStyle } = renderNumericCell(info.getValue(), c.format, rowPath, info.column.id);
                    return (
                        <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', ...extraStyle}} onContextMenu={e => handleContextMenu(e, info.getValue(), info.column.id, info.row)}>
                            {formatted}
                        </div>
                    );
                }
            }));
        } else if (serverSide) {
            // Authoritative center-column order must come from backend __col_schema.
            // If we re-sort independently on the client, col_start/col_end indices
            // drift from backend window slicing and cells appear blank.
            const orderedIds = [];
            const seenIds = new Set();
            const pushId = (rawId) => {
                const id = typeof rawId === 'string' ? rawId : null;
                if (!id || id === '__col_schema' || seenIds.has(id)) return;
                seenIds.add(id);
                orderedIds.push(id);
            };

            if (cachedColSchema && cachedColSchema.columns && cachedColSchema.columns.length > 0) {
                cachedColSchema.columns.forEach(c => pushId(c && c.id));
            }
            if (props.columns && props.columns.length > 0) {
                props.columns.forEach(c => pushId(c && c.id));
            } else if (filteredData.length > 0) {
                filteredData.forEach(row => Object.keys(row || {}).forEach(pushId));
            }

            if (orderedIds.length > 0) {
                const ignoreKeys = new Set(['_id', 'depth', '_isTotal', '_path', 'uuid', ...rowFields, ...colFields]);

                // Helper to determine if a column is relevant for the grid
                const measureSuffixes = valConfigs.map(v => `_${v.field}_${v.agg}`);
                const measureIds = new Set(valConfigs.map(v => getKey('', v.field, v.agg)));

                const flatCols = [];
                orderedIds.forEach(k => {
                    if (ignoreKeys.has(k)) return;

                    // Filter: Only show active measures, row totals, or pivoted measure columns
                    let isRelevant = false;
                    if (measureIds.has(k)) isRelevant = true;
                    else if (k.startsWith('__RowTotal__')) isRelevant = true;
                    else if (measureSuffixes.some(s => k.endsWith(s))) isRelevant = true;

                    if (!isRelevant) return;

                    flatCols.push({
                        id: k,
                        accessorFn: row => row[k],
                        header: k,
                        size: defaultColumnWidths.subtotal,
                        sortingFn,
                        cell: info => {
                            const v = info.getValue();
                            let fmt = null;
                            if (valConfigs) {
                                for (const c of valConfigs) {
                                    if (k.includes(c.field)) {
                                        fmt = c.format;
                                        // Auto-format percentage window functions as percent
                                        if (!fmt && c.windowFn && c.windowFn.startsWith('percent_')) {
                                            fmt = 'percent';
                                        }
                                        break;
                                    }
                                }
                            }
                            const rowPath = info.row.original && info.row.original._path;
                            const { formatted, extraStyle } = renderNumericCell(v, fmt, rowPath, info.column.id);
                            return (
                                <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', ...extraStyle}} onContextMenu={e => handleContextMenu(e, v, info.column.id, info.row)}>
                                    {formatted}
                                </div>
                            );
                        }
                    });
                });

                const rowTotalCols = flatCols.filter(c => c.id.startsWith('__RowTotal__'));
                const pivotCols = flatCols.filter(c => !c.id.startsWith('__RowTotal__'));

                const buildRecursiveTree = (cols) => {
                    const root = { columns: [] };
                    cols.forEach(col => {
                        const key = col.id;
                        if (!key) return;
                        let dimStr = key;
                        let measureStr = "";
                        let matchedConfig = null;
                        if (valConfigs) {
                            for (const config of valConfigs) {
                                const suffix = `_${config.field}_${config.agg}`;
                                if (key.toLowerCase().endsWith(suffix.toLowerCase())) {
                                    matchedConfig = config;
                                    measureStr = `${formatDisplayLabel(config.field)} (${config.agg})`;
                                    dimStr = key.substring(0, key.length - suffix.length);
                                    break;
                                }
                            }
                        }
                        if (!matchedConfig) {
                             const parts = key.split('_');
                             if (parts.length > 1) {
                                 dimStr = parts.slice(0, parts.length - 2).join('_');
                                 measureStr = parts.slice(parts.length - 2).join(' ');
                                 if (!dimStr) dimStr = "Total";
                             }
                        }
                        const dimPath = dimStr ? dimStr.split('|') : [];
                        let current = root;
                        let pathKey = '';
                        let parentCollapsed = false;
                        for (let idx = 0; idx < dimPath.length; idx++) {
                            const val = dimPath[idx].trim();
                            if (idx > 0 && !isColExpanded(pathKey)) {
                                parentCollapsed = true;
                                break;
                            }
                            const currentPathKey = pathKey ? `${pathKey}|||${val}` : val;
                            pathKey = currentPathKey;
                            let node = current.columns.find(c => c.headerVal === val);
                            if (!node) {
                                node = {
                                    id: `group_${currentPathKey}`,
                                    headerVal: formatDisplayLabel(val),
                                    header: (
                                        <div style={{display:'flex', alignItems:'center', gap:4, width:'100%', overflow:'hidden'}}>
                                            <span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}} title={formatDisplayLabel(val)}>{formatDisplayLabel(val)}</span>
                                            <span onClick={(e) => { e.stopPropagation(); toggleCol(currentPathKey); }} style={{cursor:'pointer', display:'flex', opacity:0.6, flexShrink:0}}>
                                                {isColExpanded(currentPathKey) ? <Icons.ColCollapse/> : <Icons.ColExpand/>}
                                            </span>
                                        </div>
                                    ),
                                    columns: [],
                                    enablePinning: true
                                };
                                current.columns.push(node);
                            }
                            current = node;
                        }
                        if (parentCollapsed) {
                             if (current.columns.length === 0) {
                                 current.columns.push({
                                    id: pathKey + "_collapsed",
                                    header: "...",
                                    size: defaultColumnWidths.collapsedPlaceholder,
                                    accessorFn: () => "",
                                    cell: () => <div style={{color:'#999', textAlign:'center'}}>...</div>
                                });
                             }
                             return;
                        }
                        if (isColExpanded(pathKey) || dimPath.length === 0) {
                            const newCol = { ...col, header: measureStr || col.header, enablePinning: true };
                            if (matchedConfig && matchedConfig.format) {
                                newCol.cell = info => (
                                    <EditableCell
                                        getValue={info.getValue}
                                        row={info.row}
                                        column={info.column}
                                        format={matchedConfig.format}
                                        validationRules={validationRules}
                                        setProps={setProps}
                                        handleContextMenu={handleContextMenu}
                                    />
                                );
                            }
                            current.columns.push(newCol);
                        } else if (current.columns.length === 0) {
                             current.columns.push({
                                id: pathKey + "_collapsed",
                                header: "...",
                                size: defaultColumnWidths.collapsedPlaceholder,
                                accessorFn: () => "",
                                cell: () => <div style={{color:'#999', textAlign:'center'}}>...</div>
                            });
                        }
                    });
                    return root.columns;
                };
                dataCols = buildRecursiveTree(pivotCols);

                if (rowTotalCols.length > 0) {
                    rowTotalCols.forEach(c => {
                         if (c.header.startsWith('__RowTotal__')) {
                             c.header = c.header.replace('__RowTotal__', 'Total ');
                         }
                                              c.cell = info => {
                                                 const config = valConfigs.find(v => c.id.includes(v.field));
                                                 const rowPath = info.row.original && info.row.original._path;
                                                 const { formatted, extraStyle } = renderNumericCell(info.getValue(), config ? config.format : null, rowPath, info.column.id);
                                                 return (
                                                     <div style={{width:'100%', height:'100%', display:'flex', alignItems:'center', justifyContent:'flex-end', paddingRight:'8px', fontWeight:'bold', ...extraStyle}} onContextMenu={e => handleContextMenu(e, info.getValue(), info.column.id, info.row)}>
                                                         {formatted}
                                                     </div>
                                                 );
                                              };                         dataCols.push(c);
                    });
                }
            }
        }
        if (hierarchyCols.length === 0 && dataCols.length === 0) {
             dataCols.push({ id: 'no_data', header: 'No Data', cell: () => 'No Data' });
        }

        const buildColumns = (cols) => {
            return cols.map(col => {
                if (col.columns) {
                    return {
                        ...col,
                        columns: buildColumns(col.columns)
                    };
                }
                return col;
            });
        };

        return buildColumns([...hierarchyCols, ...dataCols]);
    // filteredData is intentionally excluded: in server-side mode columns come from props.columns /
    // cachedColSchema and filteredData changes on every viewport scroll, causing the entire column
    // tree to rebuild. filteredData is used only as a last-resort fallback (client-side, no schema).
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rowFields, colFields, valConfigs, minMax, colorScaleMode, colExpanded, serverSide, layoutMode, showRowNumbers, isRowSelecting, rowDragStart, props.columns, cachedColSchema, decimalPlaces, columnDecimalOverrides, cellFormatRules]);
}

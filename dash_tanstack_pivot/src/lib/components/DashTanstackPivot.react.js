// DashTanstackPivot - Enterprise Grade Pivot Table
import React, { useMemo, useState, useRef, useEffect, useLayoutEffect, useCallback } from 'react';
import PropTypes from 'prop-types';
import {
    useReactTable,
    getCoreRowModel,
    getExpandedRowModel,
    getGroupedRowModel,
} from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import * as XLSX from 'xlsx';
import { saveAs } from 'file-saver';
import { themes, getStyles, isDarkTheme, gridDimensionTokens } from '../utils/styles';
import Icons from './Icons';
const debugLog = process.env.NODE_ENV !== 'production'
    ? (...args) => console.log('[pivot-grid]', ...args)
    : () => {};
import Notification from './Notification';
import useStickyStyles from '../hooks/useStickyStyles';
import { useServerSideRowModel } from '../hooks/useServerSideRowModel';
import { useColumnVirtualizer } from '../hooks/useColumnVirtualizer';
import { formatValue, getAllLeafIdsFromColumn, isGroupColumn } from '../utils/helpers';
import ContextMenu from './Table/ContextMenu';
import { PivotAppBar } from './PivotAppBar';
import { SidebarPanel } from './Sidebar/SidebarPanel';
import DrillThroughModal from './Table/DrillThroughModal';
import { useColumnDefs } from '../hooks/useColumnDefs';
import { useRenderHelpers } from '../hooks/useRenderHelpers';
import { PivotTableBody } from './Table/PivotTableBody';
import PivotErrorBoundary from './PivotErrorBoundary';
import { usePersistence } from '../hooks/usePersistence';
import { useFilteredData } from '../hooks/useFilteredData';

const getOrCreateSessionId = (componentId = 'pivot-grid') => {
    if (typeof window === 'undefined') {
        return `${componentId}-server-session`;
    }

    const storageKey = `${componentId}-client-session-id`;
    try {
        const fromStorage = window.sessionStorage.getItem(storageKey);
        if (fromStorage) return fromStorage;
    } catch (e) {
        // no-op: storage may be blocked in some browser privacy modes
    }

    let generated = null;
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
        generated = window.crypto.randomUUID();
    } else {
        generated = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    }

    try {
        window.sessionStorage.setItem(storageKey, generated);
    } catch (e) {
        // no-op
    }

    return generated;
};

const createClientInstanceId = (componentId = 'pivot-grid') => {
    if (typeof window !== 'undefined' && window.crypto && typeof window.crypto.randomUUID === 'function') {
        return `${componentId}-${window.crypto.randomUUID()}`;
    }
    return `${componentId}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
};

const loadingAnimationStyles = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@300;500&family=Plus+Jakarta+Sans:wght@300;500;800&display=swap');
@keyframes pivot-row-loader-enter {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes pivot-skeleton-shimmer {
    0% { background-position: 220% 0; }
    100% { background-position: -220% 0; }
}
@keyframes pivot-spinner-rotate {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
`;

const getStickyHeaderHeight = (headerGroupCount, rowHeight, showFloatingFilters) =>
    (headerGroupCount * rowHeight) + (showFloatingFilters ? rowHeight : 0);

export default function DashTanstackPivot(props) {
    const { 
        id, 
        data = [], 
        style = {}, 
        setProps, 
        serverSide = false, 
        rowCount,
        rowFields: initialRowFields = [],
        colFields: initialColFields = [],
        valConfigs: initialValConfigs = [],
        filters: initialFilters = {},
        sorting: initialSorting = [],
        expanded: initialExpanded = {},
        showRowTotals: initialShowRowTotals = true,
        showColTotals: initialShowColTotals = true,
        grandTotalPosition = 'top',
        filterOptions = {},
        conditionalFormatting = [],
        validationRules = {},
        columnPinning: initialColumnPinning = { left: ['hierarchy'], right: [] },
        rowPinning: initialRowPinning = { top: [], bottom: [] },
        persistence,
        persistence_type = 'local',
        pinningOptions = {},
        pinningPresets = [],
        sortOptions = {},
        columnVisibility: initialColumnVisibility = {},
        reset,
        sortLock = false,
        availableFieldList,
        table: tableName,
        dataOffset = 0,
        dataVersion = 0,
        drillEndpoint = '/api/drill-through',
        viewState = null,
    } = props;



    // --- Persistence Helper ---
    const { load: loadPersistedState, save: savePersistedState } = usePersistence(id, persistence, persistence_type);

        const [notification, setNotification] = useState(null);

        useEffect(() => {
            if (notification) {
                const timer = setTimeout(() => setNotification(null), 3000);
                return () => clearTimeout(timer);
            }
        }, [notification]);

        const showNotification = React.useCallback((msg, type='info') => {
            setNotification({ message: msg, type });
        }, []);

        // --- State ---

        const availableFields = useMemo(() => {
            if (availableFieldList && availableFieldList.length > 0) return availableFieldList;
            if (serverSide && props.columns) return props.columns.filter(c => c.id !== '__col_schema').map(c => c.id || c);

            return data && data.length ? Object.keys(data[0]) : [];

        }, [data, props.columns, serverSide, availableFieldList]);

        // Theme State
        const [themeName, setThemeName] = useState('balham');
        const theme = useMemo(() => themes[themeName], [themeName]);
        const styles = useMemo(() => getStyles(theme), [theme]);
        const loadingCssVars = useMemo(() => {
            if (isDarkTheme(theme)) {
                return {
                    '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(57,88,132,0.62) 0%, rgba(88,126,178,0.9) 48%, rgba(57,88,132,0.62) 100%)',
                    '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(50,77,116,0.58) 0%, rgba(82,118,170,0.86) 45%, rgba(50,77,116,0.58) 100%)',
                    '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(35,53,82,0.88) 0%, rgba(49,74,112,0.95) 50%, rgba(35,53,82,0.88) 100%)',
                    '--pivot-loading-border': 'rgba(130, 165, 215, 0.45)',
                    '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(120,170,240,0) 0%, rgba(140,190,255,0.92) 45%, rgba(120,170,240,0) 100%)',
                    '--pivot-loading-shimmer-duration': '2.8s',
                };
            }
            return {
                '--pivot-loading-header-gradient': 'linear-gradient(90deg, rgba(233,243,255,0.92) 0%, rgba(193,220,255,0.98) 48%, rgba(233,243,255,0.92) 100%)',
                '--pivot-loading-cell-gradient': 'linear-gradient(90deg, rgba(232,242,255,0.7) 0%, rgba(190,218,255,0.94) 45%, rgba(232,242,255,0.7) 100%)',
                '--pivot-loading-row-gradient': 'linear-gradient(90deg, rgba(246,250,255,0.96) 0%, rgba(228,241,255,0.98) 50%, rgba(246,250,255,0.96) 100%)',
                '--pivot-loading-border': 'rgba(153, 187, 238, 0.5)',
                '--pivot-loading-progress-gradient': 'linear-gradient(90deg, rgba(75,139,245,0) 0%, rgba(75,139,245,0.9) 45%, rgba(75,139,245,0) 100%)',
                '--pivot-loading-shimmer-duration': '2.8s',
            };
        }, [theme]);

        const [rowFields, setRowFields] = useState(initialRowFields);
        const [colFields, setColFields] = useState(initialColFields);
        const [valConfigs, setValConfigs] = useState(initialValConfigs);
        const [filters, setFilters] = useState(initialFilters);
        const [sorting, setSorting] = useState(initialSorting);
        const [expanded, setExpanded] = useState(initialExpanded);
        const [columnPinning, setColumnPinning] = useState(() => loadPersistedState('columnPinning', initialColumnPinning));
        const [rowPinning, setRowPinning] = useState(() => loadPersistedState('rowPinning', initialRowPinning));
        const [layoutMode, setLayoutMode] = useState('hierarchy'); // hierarchy, tabular
        const [columnVisibility, setColumnVisibility] = useState(() => loadPersistedState('columnVisibility', initialColumnVisibility));
        const [columnSizing, setColumnSizing] = useState(() => loadPersistedState('columnSizing', {}));
        const [announcement, setAnnouncement] = useState("");
        const [drillModal, setDrillModal] = useState(null);
        // drillModal shape: { loading, rows, page, totalRows, path, sortCol, sortDir, filterText } | null
        const tableRef = useRef(null);

    // Reset Effect
    useEffect(() => {
        if (reset) {
            setRowFields(initialRowFields);
            setColFields(initialColFields);
            setValConfigs(initialValConfigs);
            setFilters({});
            setSorting([]);
            setExpanded({});
            setColumnPinning(initialColumnPinning);
            setRowPinning(initialRowPinning);
            setColumnVisibility({});
            setColumnSizing({});

            if (setPropsRef.current) {
                setPropsRef.current({
                    rowFields: initialRowFields,
                    colFields: initialColFields,
                    valConfigs: initialValConfigs,
                    filters: {},
                    sorting: [],
                    expanded: {},
                    columnPinning: initialColumnPinning,
                    rowPinning: initialRowPinning,
                    columnVisibility: {},
                    columnSizing: {},
                    reset: null
                });
            }
        }
    }, [reset, initialRowFields, initialColFields, initialValConfigs, initialColumnPinning, initialRowPinning]);

        // Save Persistence
        useEffect(() => {
            if (!persistence) return;
            savePersistedState('columnPinning', columnPinning);
            savePersistedState('rowPinning', rowPinning);
            savePersistedState('columnVisibility', columnVisibility);
            savePersistedState('columnSizing', columnSizing);
        }, [id, columnPinning, rowPinning, columnVisibility, columnSizing, persistence, persistence_type]);

        useEffect(() => {
            const handleResize = () => {
                if (window.innerWidth < 768 && columnPinning.right && columnPinning.right.length > 0) {
                     setColumnPinning(prev => ({ ...prev, right: [] }));
                     showNotification("Right pinned columns hidden due to screen size.", "warning");
                }
            };
            window.addEventListener('resize', handleResize);
            return () => window.removeEventListener('resize', handleResize);
        }, [columnPinning.right, showNotification]);

    const [showRowTotals, setShowRowTotals] = useState(initialShowRowTotals);
    const [showColTotals, setShowColTotals] = useState(initialShowColTotals);
    const [showRowNumbers, setShowRowNumbers] = useState(false);
    const [sidebarOpen, setSidebarOpen] = useState(true);
    const [activeFilterCol, setActiveFilterCol] = useState(null);
    const [filterAnchorEl, setFilterAnchorEl] = useState(null);
    const [sidebarTab, setSidebarTab] = useState('fields'); // 'fields', 'columns'
    const [showFloatingFilters, setShowFloatingFilters] = useState(false);
    const [colSearch, setColSearch] = useState('');
    const [colTypeFilter, setColTypeFilter] = useState('all');
    const [selectedCols, setSelectedCols] = useState(new Set());
    const [hoveredHeaderId, setHoveredHeaderId] = useState(null);
    const [focusedHeaderId, setFocusedHeaderId] = useState(null);
    
    // Global Keyboard Shortcuts
    useEffect(() => {
        const handleGlobalKeyDown = (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'b') {
                e.preventDefault();
                setSidebarOpen(prev => !prev);
            }
        };
        window.addEventListener('keydown', handleGlobalKeyDown);
        return () => window.removeEventListener('keydown', handleGlobalKeyDown);
    }, []);

    const [colorScaleMode, setColorScaleMode] = useState('off');
    const [colorPalette, setColorPalette] = useState('redGreen');
    const [dataBarsColumns, setDataBarsColumns] = useState(new Set());

    // Font / display controls
    const [fontFamily, setFontFamily] = useState("'Inter', system-ui, sans-serif");
    const [fontSize, setFontSize] = useState('13px');
    const [decimalPlaces, setDecimalPlaces] = useState(2);
    const [columnDecimalOverrides, setColumnDecimalOverrides] = useState({});
    const [cellFormatRules, setCellFormatRules] = useState({});
    const [hoveredRowPath, setHoveredRowPath] = useState(null);

    // Derive per-cell keys ("rowPath:::colId") for Format Cell popover
    const selectedCellKeys = useMemo(() => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return [];
        const visibleRows = tableRef.current ? tableRef.current.getRowModel().rows : [];
        const rowIdToPath = {};
        visibleRows.forEach(r => {
            if (r.original && r.original._path) rowIdToPath[r.id] = r.original._path;
        });
        return keys.map(key => {
            const colonIdx = key.indexOf(':');
            const rowId = key.substring(0, colonIdx);
            const colId = key.substring(colonIdx + 1);
            const rowPath = rowIdToPath[rowId] || rowId;
            return `${rowPath}:::${colId}`;
        });
    }, [selectedCells]);

    // Column IDs of currently selected cells (for Data Bars)
    const selectedCellColIds = useMemo(() => {
        const ids = new Set();
        Object.keys(selectedCells || {}).forEach(key => {
            const idx = key.indexOf(':');
            if (idx >= 0) ids.add(key.substring(idx + 1));
        });
        return ids;
    }, [selectedCells]);

    // Decimal formatting: with selection → adjust per-column overrides; without → adjust global default
    const handleDecimalChange = useCallback((delta) => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) {
            setDecimalPlaces(prev => Math.max(0, Math.min(6, prev + delta)));
            return;
        }
        const colIds = [...new Set(keys.map(k => {
            const idx = k.indexOf(':');
            return idx >= 0 ? k.substring(idx + 1) : k;
        }))];
        setColumnDecimalOverrides(prev => {
            const next = { ...prev };
            colIds.forEach(colId => {
                const cur = next[colId] !== undefined ? next[colId] : decimalPlaces;
                next[colId] = Math.max(0, Math.min(6, cur + delta));
            });
            return next;
        });
    }, [selectedCells, decimalPlaces]);

    // Show the decimal value of the first selected column, or global default
    const displayDecimal = useMemo(() => {
        const keys = Object.keys(selectedCells || {});
        if (keys.length === 0) return decimalPlaces;
        const k = keys[0];
        const idx = k.indexOf(':');
        const colId = idx >= 0 ? k.substring(idx + 1) : k;
        return columnDecimalOverrides[colId] !== undefined ? columnDecimalOverrides[colId] : decimalPlaces;
    }, [selectedCells, columnDecimalOverrides, decimalPlaces]);

    const [spacingMode, setSpacingMode] = useState(0);
    const spacingLabels = gridDimensionTokens.density.spacingLabels;
    const rowHeights = gridDimensionTokens.density.rowHeights;
    const defaultColumnWidths = gridDimensionTokens.columnWidths;
    const autoSizeBounds = gridDimensionTokens.autoSize;
    
    const [colExpanded, setColExpanded] = useState({});
    const [contextMenu, setContextMenu] = useState(null);
    const [selectedCells, setSelectedCells] = useState({});
    const [lastSelected, setLastSelected] = useState(null);
    const [isDragging, setIsDragging] = useState(false);
    const [dragStart, setDragStart] = useState(null);
    const [isFilling, setIsFilling] = useState(false);
        const [fillRange, setFillRange] = useState(null);

        // --- Data Management State ---
    const [history, setHistory] = useState([]);
    const [future, setFuture] = useState([]);

    const handleUndo = () => {
        if (history.length === 0) return;
        const previous = history[history.length - 1];
        setHistory(history.slice(0, -1));
        setFuture([selectedCells, ...future]);
        if (setProps) setProps({ undo: true, timestamp: Date.now() });
    };

    const handleRedo = () => {
        if (future.length === 0) return;
        const next = future[0];
        setFuture(future.slice(1));
        setHistory([...history, selectedCells]);
        if (setProps) setProps({ redo: true, timestamp: Date.now() });
    };

    const handleRefresh = () => {
        if (setProps) setProps({ refresh: Date.now() });
    };

    useEffect(() => {
        if (!viewState || typeof viewState !== 'object') return;
        const restored = (viewState.state && typeof viewState.state === 'object')
            ? viewState.state
            : viewState;

        const sanitizedFilters = (() => {
            if (!restored.filters || typeof restored.filters !== 'object') return null;
            const next = { ...restored.filters };
            delete next.__request_unique__;
            return next;
        })();

        if (Array.isArray(restored.rowFields)) setRowFields(restored.rowFields);
        if (Array.isArray(restored.colFields)) setColFields(restored.colFields);
        if (Array.isArray(restored.valConfigs)) setValConfigs(restored.valConfigs);
        if (sanitizedFilters) setFilters(sanitizedFilters);
        if (Array.isArray(restored.sorting)) setSorting(restored.sorting);
        if (restored.expanded && typeof restored.expanded === 'object') setExpanded(restored.expanded);
        if (typeof restored.showRowTotals === 'boolean') setShowRowTotals(restored.showRowTotals);
        if (typeof restored.showColTotals === 'boolean') setShowColTotals(restored.showColTotals);
        if (typeof restored.showRowNumbers === 'boolean') setShowRowNumbers(restored.showRowNumbers);
        if (typeof restored.sidebarOpen === 'boolean') setSidebarOpen(restored.sidebarOpen);
        if (typeof restored.sidebarTab === 'string') setSidebarTab(restored.sidebarTab);
        if (typeof restored.showFloatingFilters === 'boolean') setShowFloatingFilters(restored.showFloatingFilters);
        if (typeof restored.colSearch === 'string') setColSearch(restored.colSearch);
        if (typeof restored.colTypeFilter === 'string') setColTypeFilter(restored.colTypeFilter);
        if (typeof restored.themeName === 'string' && themes[restored.themeName]) setThemeName(restored.themeName);
        if (typeof restored.layoutMode === 'string') setLayoutMode(restored.layoutMode);
        if (typeof restored.colorScaleMode === 'string') setColorScaleMode(restored.colorScaleMode);
        if (typeof restored.spacingMode === 'number') setSpacingMode(restored.spacingMode);
        if (restored.columnPinning && typeof restored.columnPinning === 'object') setColumnPinning(restored.columnPinning);
        if (restored.rowPinning && typeof restored.rowPinning === 'object') setRowPinning(restored.rowPinning);
        if (restored.columnVisibility && typeof restored.columnVisibility === 'object') setColumnVisibility(restored.columnVisibility);
        if (restored.columnSizing && typeof restored.columnSizing === 'object') setColumnSizing(restored.columnSizing);
        if (restored.colExpanded && typeof restored.colExpanded === 'object') setColExpanded(restored.colExpanded);
        if (typeof restored.decimalPlaces === 'number') setDecimalPlaces(restored.decimalPlaces);
        if (restored.columnDecimalOverrides && typeof restored.columnDecimalOverrides === 'object') setColumnDecimalOverrides(restored.columnDecimalOverrides);
        if (restored.cellFormatRules && typeof restored.cellFormatRules === 'object') setCellFormatRules(restored.cellFormatRules);

        if (viewState.viewport && typeof viewState.viewport === 'object') {
            latestViewportRef.current = {
                ...latestViewportRef.current,
                ...viewState.viewport,
            };
        }

        const savedScroll = restored.scroll && typeof restored.scroll === 'object' ? restored.scroll : null;
        if (savedScroll && parentRef.current) {
            requestAnimationFrame(() => {
                if (!parentRef.current) return;
                if (typeof savedScroll.top === 'number') parentRef.current.scrollTop = savedScroll.top;
                if (typeof savedScroll.left === 'number') parentRef.current.scrollLeft = savedScroll.left;
            });
        }
    }, [viewState]);

    // Clipboard Paste
    useEffect(() => {
        const handlePaste = (e) => {
            if (!lastSelected) return;
            e.preventDefault();
            const clipboardData = e.clipboardData.getData('text');
            const rows = clipboardData.split(/\r\n|\n/).map(r => r.split('\t'));

            if (setPropsRef.current && lastSelected.rowIndex !== undefined && lastSelected.colIndex !== undefined) {
                const visibleLeafColumns = (tableRef.current && tableRef.current.getVisibleLeafColumns) ? tableRef.current.getVisibleLeafColumns() : [];
                const visibleRows = (tableRef.current && tableRef.current.getRowModel) ? (tableRef.current.getRowModel().rows || []) : [];

                const startRow = visibleRows[lastSelected.rowIndex];
                const startCol = visibleLeafColumns[lastSelected.colIndex];

                if (startRow && startCol) {
                    setPropsRef.current({
                        paste: {
                            startRowId: startRow.id,
                            startColId: startCol.id,
                            data: rows
                        }
                    });
                }
            }
        };

        window.addEventListener('paste', handlePaste);
        return () => window.removeEventListener('paste', handlePaste);
    }, [lastSelected]);

    // Validation helper
    const validateCell = (val, rule) => {
        if (!rule) return true;
        if (rule.type === 'regex') return new RegExp(rule.pattern).test(val);
        if (rule.type === 'numeric') return !isNaN(parseFloat(val));
        if (rule.type === 'required') return val !== null && val !== '' && val !== undefined;
        return true;
    };

    const getRuleBasedStyle = (colId, value) => {
        if (typeof value !== 'number') return {};
        const rules = conditionalFormatting.filter(r => r.column === colId || !r.column);
        let style = {};
        for (const rule of rules) {
            let match = false;
            if (rule.condition === '>') match = value > rule.value;
            else if (rule.condition === '<') match = value < rule.value;
            else if (rule.condition === '>=') match = value >= rule.value;
            else if (rule.condition === '<=') match = value <= rule.value;
            else if (rule.condition === '==') match = value === rule.value;

            if (match) {
                style = { ...style, ...rule.style };
            }
        }
        return style;
    };

    const handleKeyDown = (e) => {
        const visibleLeafColumnsAll = (tableRef.current && tableRef.current.getVisibleLeafColumns) ? tableRef.current.getVisibleLeafColumns() : [];
        const visibleRowsAll = (tableRef.current && tableRef.current.getRowModel) ? tableRef.current.getRowModel().rows : rows;

        // Ctrl+A: select all visible rows and columns
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
            e.preventDefault();
            const allSelection = {};
            visibleRowsAll.forEach(r => {
                visibleLeafColumnsAll.forEach(c => {
                    allSelection[`${r.id}:${c.id}`] = r.getValue(c.id);
                });
            });
            setSelectedCells(allSelection);
            if (visibleRowsAll.length > 0 && visibleLeafColumnsAll.length > 0) {
                setLastSelected({ rowIndex: 0, colIndex: 0 });
                setDragStart({ rowIndex: 0, colIndex: 0 });
            }
            return;
        }

        // Ctrl+C: copy selected cells as TSV
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') {
            const keys = Object.keys(selectedCells);
            if (keys.length === 0) return;
            e.preventDefault();
            const data = getSelectedData(false);
            if (data) {
                copyToClipboard(data);
                showNotification('Copied!', 'success');
            }
            return;
        }

        if (!lastSelected) return;

        const { rowIndex, colIndex } = lastSelected;
        let nextRow = rowIndex;
        let nextCol = colIndex;

        const visibleLeafColumns = visibleLeafColumnsAll;

        // Helper: Excel-style Ctrl+Arrow — jumps to edge of contiguous data block.
        // Rules (mirroring Excel):
        //   - current cell empty  → jump to next non-empty in direction (or edge if all empty)
        //   - current non-empty, next is empty → jump to next non-empty past the gap (or edge)
        //   - current non-empty, next is non-empty → jump to last non-empty before a gap (end of block)
        const ctrlArrowRow = (dir) => {
            const colId = visibleLeafColumns[colIndex]?.id;
            if (!colId) return dir > 0 ? visibleRowsAll.length - 1 : 0;
            const isEmpty = (r) => { const v = r?.getValue(colId); return v === null || v === undefined || v === ''; };
            const curEmpty = isEmpty(visibleRowsAll[rowIndex]);
            if (curEmpty) {
                // jump to next non-empty
                for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleRowsAll[i])) return i;
                }
                return dir > 0 ? visibleRowsAll.length - 1 : 0;
            }
            const nextIdx = rowIndex + dir;
            if (nextIdx < 0 || nextIdx >= visibleRowsAll.length) return dir > 0 ? visibleRowsAll.length - 1 : 0;
            if (isEmpty(visibleRowsAll[nextIdx])) {
                // next is empty — jump past gap to next non-empty
                for (let i = nextIdx + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleRowsAll[i])) return i;
                }
                return dir > 0 ? visibleRowsAll.length - 1 : 0;
            }
            // next is non-empty — find end of contiguous block
            let last = rowIndex;
            for (let i = rowIndex + dir; dir > 0 ? i < visibleRowsAll.length : i >= 0; i += dir) {
                if (isEmpty(visibleRowsAll[i])) break;
                last = i;
            }
            return last;
        };

        const ctrlArrowCol = (dir) => {
            const rowObj = visibleRowsAll[rowIndex];
            if (!rowObj) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            const isEmpty = (c) => { const v = rowObj.getValue(c?.id); return v === null || v === undefined || v === ''; };
            const curEmpty = isEmpty(visibleLeafColumns[colIndex]);
            if (curEmpty) {
                for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleLeafColumns[i])) return i;
                }
                return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            }
            const nextIdx = colIndex + dir;
            if (nextIdx < 0 || nextIdx >= visibleLeafColumns.length) return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            if (isEmpty(visibleLeafColumns[nextIdx])) {
                for (let i = nextIdx + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                    if (!isEmpty(visibleLeafColumns[i])) return i;
                }
                return dir > 0 ? visibleLeafColumns.length - 1 : 0;
            }
            let last = colIndex;
            for (let i = colIndex + dir; dir > 0 ? i < visibleLeafColumns.length : i >= 0; i += dir) {
                if (isEmpty(visibleLeafColumns[i])) break;
                last = i;
            }
            return last;
        };

        if (e.key.startsWith('Arrow')) {
            e.preventDefault();
            if (e.ctrlKey || e.metaKey) {
                if (e.key === 'ArrowDown')  nextRow = ctrlArrowRow(1);
                else if (e.key === 'ArrowUp')   nextRow = ctrlArrowRow(-1);
                else if (e.key === 'ArrowRight') nextCol = ctrlArrowCol(1);
                else if (e.key === 'ArrowLeft')  nextCol = ctrlArrowCol(-1);
            } else {
                if (e.key === 'ArrowUp')    nextRow = Math.max(0, rowIndex - 1);
                else if (e.key === 'ArrowDown')  nextRow = Math.min(visibleRowsAll.length - 1, rowIndex + 1);
                else if (e.key === 'ArrowLeft')  nextCol = Math.max(0, colIndex - 1);
                else if (e.key === 'ArrowRight') nextCol = Math.min(visibleLeafColumns.length - 1, colIndex + 1);
            }
        } else if (e.key === 'Tab') {
            e.preventDefault();
            nextCol = e.shiftKey ? Math.max(0, colIndex - 1) : Math.min(visibleLeafColumns.length - 1, colIndex + 1);
        } else {
            return;
        }

        const nextRowObj = visibleRowsAll[nextRow];
        const nextColObj = visibleLeafColumns[nextCol];

        if (nextRowObj && nextColObj) {
            const key = `${nextRowObj.id}:${nextColObj.id}`;
            const val = nextRowObj.getValue(nextColObj.id);

            if (e.shiftKey && e.key.startsWith('Arrow')) {
                 const newRange = selectRange(dragStart || lastSelected, { rowIndex: nextRow, colIndex: nextCol });
                 setSelectedCells(newRange);
            } else {
                 setSelectedCells({ [key]: val });
                 setDragStart({ rowIndex: nextRow, colIndex: nextCol });
            }
            setLastSelected({ rowIndex: nextRow, colIndex: nextCol });

            // Scroll into view if needed
            if (rowVirtualizer.scrollToIndex) rowVirtualizer.scrollToIndex(nextRow);
            if (columnVirtualizer.scrollToIndex) columnVirtualizer.scrollToIndex(nextCol);
        }
    };

    const selectRange = (start, end) => {
        const rStart = Math.min(start.rowIndex, end.rowIndex);
        const rEnd = Math.max(start.rowIndex, end.rowIndex);
        const cStart = Math.min(start.colIndex, end.colIndex);
        const cEnd = Math.max(start.colIndex, end.colIndex);

        const visibleRows = table.getRowModel().rows;
        const visibleCols = table.getVisibleLeafColumns();
        const newSelection = {};

        for (let r = rStart; r <= rEnd; r++) {
            for (let c = cStart; c <= cEnd; c++) {
                const rRow = visibleRows[r];
                const cCol = visibleCols[c];
                if (rRow && cCol) {
                    newSelection[`${rRow.id}:${cCol.id}`] = rRow.getValue(cCol.id);
                }
            }
        }
        return newSelection;
    };

    const [isRowSelecting, setIsRowSelecting] = useState(false);
    const [rowDragStart, setRowDragStart] = useState(null);

    // Stop row selection on mouse up
    useEffect(() => {
        const handleMouseUp = () => {
            setIsRowSelecting(false);
            setRowDragStart(null);
        };
        window.addEventListener('mouseup', handleMouseUp);
        return () => window.removeEventListener('mouseup', handleMouseUp);
    }, []);

    const handleRowRangeSelect = useCallback((startIdx, endIdx) => {
        if (!tableRef.current) return;
        const visibleCols = tableRef.current.getVisibleLeafColumns();
        const rows = tableRef.current.getRowModel().rows;
        const min = Math.min(startIdx, endIdx);
        const max = Math.max(startIdx, endIdx);
        
        const rangeSelection = {};
        for(let i=min; i<=max; i++) {
            const r = rows[i];
            if(r) {
                visibleCols.forEach(col => {
                    rangeSelection[`${r.id}:${col.id}`] = r.getValue(col.id);
                });
            }
        }
        // Merge with existing if ctrl held? No, drag usually replaces or extends from anchor.
        // For simplicity, let's just set selection to this range.
        setSelectedCells(rangeSelection);
    }, []);

    const handleRowSelect = useCallback((row, isShift, isCtrl) => {
        if (!tableRef.current) return;
        const visibleCols = tableRef.current.getVisibleLeafColumns();
        const rowId = row.id;
        const newSelection = {};
        
        visibleCols.forEach((col) => {
            newSelection[`${rowId}:${col.id}`] = row.getValue(col.id);
        });

        if (isCtrl) {
            setSelectedCells(prev => ({...prev, ...newSelection}));
            setLastSelected({ rowIndex: row.index, colIndex: 0 });
        } else if (isShift && lastSelected) {
             const startRowIndex = lastSelected.rowIndex;
             const endRowIndex = row.index;
             const rows = tableRef.current.getRowModel().rows;
             const min = Math.min(startRowIndex, endRowIndex);
             const max = Math.max(startRowIndex, endRowIndex);
             
             const rangeSelection = {};
             for(let i=min; i<=max; i++) {
                 const r = rows[i];
                 if(r) {
                    visibleCols.forEach(col => {
                        rangeSelection[`${r.id}:${col.id}`] = r.getValue(col.id);
                    });
                 }
             }
             setSelectedCells(rangeSelection);
        } else {
            setSelectedCells(newSelection);
            setLastSelected({ rowIndex: row.index, colIndex: 0 });
        }
    }, [lastSelected]);

    const handleCellMouseDown = useCallback((e, rowIndex, colIndex, rowId, colId, value) => {
        if (e.button === 2) return; // Ignore right-click

        if (e.shiftKey) {
            e.preventDefault(); // Prevent text selection
            const start = lastSelected || { rowIndex, colIndex };
            const newSelection = selectRange(start, { rowIndex, colIndex });
            // Merge if ctrl key, else replace
            if (e.ctrlKey || e.metaKey) {
                setSelectedCells(prev => ({...prev, ...newSelection}));
            } else {
                setSelectedCells(newSelection);
            }
            return;
        }

        setIsDragging(true);
        setDragStart({ rowIndex, colIndex });
        setLastSelected({ rowIndex, colIndex });
        // Track hovered row path for Format Row feature
        const clickedRow = tableRef.current && tableRef.current.getRowModel
            ? tableRef.current.getRowModel().rows[rowIndex]
            : null;
        if (clickedRow && clickedRow.original && clickedRow.original._path) {
            setHoveredRowPath(clickedRow.original._path);
        }

        const key = `${rowId}:${colId}`;
        if (e.ctrlKey || e.metaKey) {
             const newSelection = { ...selectedCells };
             newSelection[key] = value;
             setSelectedCells(newSelection);
        } else {
            // Clear and start new
            setSelectedCells({ [key]: value });
        }
    }, [lastSelected, selectedCells]);

    const handleCellMouseEnter = (rowIndex, colIndex) => {
        if (isDragging && dragStart) {
             const newRange = selectRange(dragStart, { rowIndex, colIndex });
             setSelectedCells(newRange); 
        }
        if (isFilling && dragStart) {
            const rStart = Math.min(dragStart.rowIndex, rowIndex);
            const rEnd = Math.max(dragStart.rowIndex, rowIndex);
            const cStart = Math.min(dragStart.colIndex, colIndex);
            const cEnd = Math.max(dragStart.colIndex, colIndex);
            setFillRange({ rStart, rEnd, cStart, cEnd });
        }
    };

    const handleFillMouseDown = (e) => {
        e.stopPropagation();
        e.preventDefault();
        setIsFilling(true);
    };

    const handleFillMouseUp = () => {
        if (isFilling && fillRange && setProps) {
            const startValue = table.getRowModel().rows[dragStart.rowIndex].getVisibleCells()[dragStart.colIndex].getValue();
            const updates = [];
            const visibleRows = table.getRowModel().rows;
            const visibleCols = table.getVisibleLeafColumns();
            for (let r = fillRange.rStart; r <= fillRange.rEnd; r++) {
                for (let c = fillRange.cStart; c <= fillRange.cEnd; c++) {
                    const row = visibleRows[r];
                    const col = visibleCols[c];
                    if (row && col) {
                        updates.push({ rowId: row.id, colId: col.id, value: startValue });
                    }
                }
            }
            if (updates.length > 0) {
                setProps({ cellUpdates: updates });
            }
        }
        setIsFilling(false);
        setFillRange(null);
    };

    useEffect(() => {
        const handleMouseUp = () => {
            setIsDragging(false);
            setDragStart(null);
            if (isFilling) {
                handleFillMouseUp();
            }
        };
        window.addEventListener('mouseup', handleMouseUp);
        return () => window.removeEventListener('mouseup', handleMouseUp);
    }, [isDragging, isFilling, fillRange, dragStart]);
    
    // Global Ctrl+C is handled inside handleKeyDown (attached to table container)
    // This window-level listener acts as fallback when the table isn't focused
    useEffect(() => {
        const handleGlobalCopy = (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'c') {
                const keys = Object.keys(selectedCells);
                if (keys.length === 0) return;
                e.preventDefault();
                const data = getSelectedData(false);
                if (data) {
                    copyToClipboard(data);
                    showNotification('Copied!', 'success');
                }
            }
        };
        window.addEventListener('keydown', handleGlobalCopy);
        return () => window.removeEventListener('keydown', handleGlobalCopy);
    }, [selectedCells]);

    const toggleCol = (key) => {
        setColExpanded(prev => ({
            ...prev,
            [key]: prev[key] === undefined ? false : !prev[key]
        }));
    };

    const isColExpanded = (key) => colExpanded[key] !== false;

    const [dragItem, setDragItem] = useState(null);
    const [dropLine, setDropLine] = useState(null);
    const sessionIdRef = useRef(getOrCreateSessionId(id || 'pivot-grid'));
    const clientInstanceRef = useRef(createClientInstanceId(id || 'pivot-grid'));
    const requestVersionRef = useRef(Number(dataVersion) || 0);
    const latestDataVersionRef = useRef(Number(dataVersion) || 0);
    const pendingRequestVersionsRef = useRef(new Set());
    const loadingDelayTimerRef = useRef(null);
    const stateEpochRef = useRef(0);
    const abortGenerationRef = useRef(0);
    const structuralPendingVersionRef = useRef(null);
    const expandAllDebounceRef = useRef(false);
    const latestViewportRef = useRef({ start: 0, end: 99, count: 100 });
    const [stateEpoch, setStateEpoch] = useState(0);
    const [cachedColSchema, setCachedColSchema] = useState(null);
    const colSchemaEpochRef = useRef(-1);
    const [visibleColRange, setVisibleColRange] = useState({ start: 0, end: 0 });
    const colRequestStartRef = useRef(null);
    const colRequestEndRef = useRef(null);
    const needsColSchemaRef = useRef(true);
    const [abortGeneration, setAbortGeneration] = useState(0);
    const [structuralInFlight, setStructuralInFlight] = useState(false);
    const [pendingRowTransitions, setPendingRowTransitions] = useState(() => new Map());
    const [pendingColumnSkeletonCount, setPendingColumnSkeletonCount] = useState(0);
    const [isRequestPending, setIsRequestPending] = useState(false);

    const markRequestPending = useCallback((version) => {
        const numericVersion = Number(version);
        if (Number.isFinite(numericVersion)) {
            pendingRequestVersionsRef.current.add(numericVersion);
        }
        if (isRequestPending || loadingDelayTimerRef.current !== null) return;
        loadingDelayTimerRef.current = setTimeout(() => {
            loadingDelayTimerRef.current = null;
            if (pendingRequestVersionsRef.current.size > 0) {
                setIsRequestPending(true);
            }
        }, 200);
    }, [isRequestPending]);

    useEffect(() => {
        const numericVersion = Number(dataVersion);
        if (!Number.isFinite(numericVersion)) return;
        latestDataVersionRef.current = numericVersion;
        if (numericVersion > requestVersionRef.current) {
            requestVersionRef.current = numericVersion;
        }
        for (const pendingVersion of Array.from(pendingRequestVersionsRef.current)) {
            if (pendingVersion <= numericVersion) {
                pendingRequestVersionsRef.current.delete(pendingVersion);
            }
        }
        if (pendingRequestVersionsRef.current.size === 0) {
            if (loadingDelayTimerRef.current !== null) {
                clearTimeout(loadingDelayTimerRef.current);
                loadingDelayTimerRef.current = null;
            }
            setIsRequestPending(false);
        }
    }, [dataVersion]);

    useEffect(() => {
        return () => {
            if (loadingDelayTimerRef.current !== null) {
                clearTimeout(loadingDelayTimerRef.current);
                loadingDelayTimerRef.current = null;
            }
        };
    }, []);

    useEffect(() => {
        if (!isRequestPending) return;
        const timeoutId = setTimeout(() => {
            pendingRequestVersionsRef.current.clear();
            setIsRequestPending(false);
        }, 15000);
        return () => clearTimeout(timeoutId);
    }, [isRequestPending]);

    // Clear schema on structural change so we re-derive from fresh data
    useEffect(() => {
        if (!serverSide) return;
        setCachedColSchema(null);
    }, [stateEpoch, serverSide]);

    // Extract authoritative col_schema embedded by the server as a sentinel entry in props.columns.
    // This is more robust than inferring schema from row keys (handles windowed responses correctly).
    useEffect(() => {
        if (!serverSide || !props.columns) return;
        const schemaEntry = props.columns.find(c => c.id === '__col_schema');
        if (schemaEntry && schemaEntry.col_schema) {
            setCachedColSchema(schemaEntry.col_schema);
            colSchemaEpochRef.current = stateEpoch;
        }
    }, [serverSide, props.columns, stateEpoch]);

    // Derive schema from row keys — only used in client-side mode.
    // In server-side mode the authoritative schema always comes from the __col_schema sentinel
    // embedded in props.columns by the server.  Allowing row-key inference in server-side mode
    // risks schema drift on windowed/partial payloads (only the visible col slice is present).
    useEffect(() => {
        if (serverSide || cachedColSchema) return;
        if (!filteredData || filteredData.length === 0) return;
        const rowMetaKeys = new Set(['_id', '_path', '_isTotal', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'depth', 'uuid', 'subRows', '__virtualIndex']);
        const ignoredIds = new Set([...rowFields, ...colFields, '_isTotal']);
        const colIds = [];
        const colIdSet = new Set();
        for (const row of filteredData) {
            if (!row) continue;
            for (const key of Object.keys(row)) {
                if (!colIdSet.has(key) && !rowMetaKeys.has(key) && !ignoredIds.has(key)) {
                    colIds.push(key);
                    colIdSet.add(key);
                }
            }
        }
        if (colIds.length > 0) {
            setCachedColSchema({
                total_center_cols: colIds.length,
                columns: colIds.map((id, i) => ({ index: i, id, size: defaultColumnWidths.schemaFallback }))
            });
            colSchemaEpochRef.current = stateEpoch;
        }
    }, [serverSide, filteredData, cachedColSchema, stateEpoch, rowFields, colFields, defaultColumnWidths.schemaFallback]);

    const needsColSchema = !cachedColSchema || colSchemaEpochRef.current !== stateEpoch;
    const totalCenterCols = cachedColSchema ? cachedColSchema.total_center_cols : null;

    // Strict visible-only request window: request exactly the currently visible center range.
    // Before schema is known, request full payload once to populate __col_schema.
    const colRequestStart = (serverSide && cachedColSchema && !needsColSchema && totalCenterCols !== null)
        ? Math.max(0, visibleColRange.start)
        : null;

    const colRequestEnd = (serverSide && cachedColSchema && !needsColSchema && totalCenterCols !== null)
        ? Math.min(totalCenterCols - 1,
            visibleColRange.end)
        : null;

    // Keep refs in sync for use in field-zone effect closures
    colRequestStartRef.current = colRequestStart;
    colRequestEndRef.current = colRequestEnd;
    needsColSchemaRef.current = needsColSchema;

    const beginStructuralTransaction = useCallback(() => {
        stateEpochRef.current += 1;
        abortGenerationRef.current += 1;
        const baselineVersion = Math.max(requestVersionRef.current, latestDataVersionRef.current);
        const nextVersion = baselineVersion + 1;
        requestVersionRef.current = nextVersion;

        setStateEpoch(stateEpochRef.current);
        setAbortGeneration(abortGenerationRef.current);
        setStructuralInFlight(true);
        structuralPendingVersionRef.current = {
            version: nextVersion,
            startDataVersion: latestDataVersionRef.current
        };

        return {
            stateEpoch: stateEpochRef.current,
            abortGeneration: abortGenerationRef.current,
            version: nextVersion
        };
    }, []);

    // Lightweight expansion request: clears inflight (via abortGeneration bump) but
    // does NOT change stateEpoch, so the existing cache stays valid and rows remain
    // visible instead of flashing to skeletons.
    const beginExpansionRequest = useCallback(() => {
        abortGenerationRef.current += 1;
        const newVersion = requestVersionRef.current + 1;
        requestVersionRef.current = newVersion;
        setAbortGeneration(abortGenerationRef.current);
        return {
            abortGeneration: abortGenerationRef.current,
            stateEpoch: stateEpochRef.current,
            version: newVersion
        };
    }, []);

    const setPropsRef = useRef(setProps);
    useEffect(() => {
        setPropsRef.current = setProps;
    }, [setProps]);

    const buildCurrentViewState = useCallback(() => {
        const normalizedFilters = (() => {
            const next = { ...(filters || {}) };
            delete next.__request_unique__;
            return next;
        })();
        return {
            version: 1,
            table: tableName || null,
            viewport: latestViewportRef.current || null,
            state: {
                rowFields,
                colFields,
                valConfigs,
                filters: normalizedFilters,
                sorting,
                expanded,
                showRowTotals,
                showColTotals,
                showRowNumbers,
                sidebarOpen,
                sidebarTab,
                showFloatingFilters,
                colSearch,
                colTypeFilter,
                themeName,
                layoutMode,
                colorScaleMode,
                spacingMode,
                columnPinning,
                rowPinning,
                columnVisibility,
                columnSizing,
                colExpanded,
                decimalPlaces,
                columnDecimalOverrides,
                cellFormatRules,
                scroll: parentRef.current
                    ? { top: parentRef.current.scrollTop, left: parentRef.current.scrollLeft }
                    : null,
            }
        };
    }, [
        tableName,
        rowFields,
        colFields,
        valConfigs,
        filters,
        sorting,
        expanded,
        showRowTotals,
        showColTotals,
        showRowNumbers,
        sidebarOpen,
        sidebarTab,
        showFloatingFilters,
        colSearch,
        colTypeFilter,
        themeName,
        layoutMode,
        colorScaleMode,
        spacingMode,
        columnPinning,
        rowPinning,
        columnVisibility,
        columnSizing,
        colExpanded,
        decimalPlaces,
        columnDecimalOverrides,
        cellFormatRules,
    ]);

    const handleSaveView = useCallback(() => {
        const snapshot = buildCurrentViewState();
        if (setPropsRef.current) {
            setPropsRef.current({ savedView: snapshot });
        }
        showNotification('View snapshot saved', 'success');
    }, [buildCurrentViewState, showNotification]);

    const lastPropsRef = useRef({
        rowFields: initialRowFields,
        colFields: initialColFields,
        valConfigs: initialValConfigs,
        filters: {},
        sorting: [],
        expanded: {},
        showRowTotals: initialShowRowTotals,
        showColTotals: initialShowColTotals,
        columnPinning: initialColumnPinning,
        rowPinning: initialRowPinning,
        columnVisibility: {},
        columnSizing: {}
    });

    React.useEffect(() => {
        const nextProps = {
            rowFields, colFields, valConfigs, filters, sorting, expanded,
            showRowTotals, showColTotals, columnPinning, rowPinning, columnVisibility, columnSizing
        };
        const colFieldsChanged = JSON.stringify(nextProps.colFields) !== JSON.stringify(lastPropsRef.current.colFields);

        const changedKeys = Object.keys(nextProps).filter(key => {
            const val = nextProps[key];
            const lastVal = lastPropsRef.current[key];
            return JSON.stringify(val) !== JSON.stringify(lastVal);
        });
        const changed = changedKeys.length > 0;

        if (setPropsRef.current && changed) {
            debugLog('Sync to Dash Triggered', nextProps);

            // Detect expansion-only: only `expanded` changed, no structural fields.
            // In that case we keep the existing cache (no stateEpoch bump) so rows
            // remain visible. A loading row appears below the expanded row via
            // pendingRowTransitions, and the viewport snaps in place.
            const structuralKeys = ['rowFields', 'colFields', 'valConfigs', 'filters', 'sorting',
                'showRowTotals', 'showColTotals', 'columnPinning', 'rowPinning', 'columnVisibility', 'columnSizing'];
            const uiOnlyKeys = new Set(['columnPinning', 'rowPinning', 'columnVisibility', 'columnSizing']);
            const isExpansionOnly = serverSide && structuralKeys.every(
                key => JSON.stringify(nextProps[key]) === JSON.stringify(lastPropsRef.current[key])
            );
            const isUiOnlyChange = changedKeys.length > 0 && changedKeys.every(key => uiOnlyKeys.has(key));

            lastPropsRef.current = nextProps;

            // Column resize/pin/visibility are local UI concerns and should not
            // trigger backend loading or viewport fetches.
            if (isUiOnlyChange) {
                setPropsRef.current(nextProps);
                return;
            }

            if (isExpansionOnly) {
                // Cancel any pending scroll restore — the viewport stays exactly in place.
                expansionScrollRestoreRef.current = null;
                if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
                    cancelAnimationFrame(expansionScrollRestoreRafRef.current);
                    expansionScrollRestoreRafRef.current = null;
                }
                const tx = beginExpansionRequest();
                markRequestPending(tx.version);
                const viewportSnapshot = latestViewportRef.current || { start: 0, end: 99, count: 100 };

                // Extend the row window to cover the block immediately after the anchor block.
                // When expanding a row near the END of its block (e.g. row 95 in block 0),
                // new children overflow into block N+1. Without this extension, those rows have
                // no cache entry → they flash with skeleton loaders until a follow-up fetch lands.
                // pendingExpansionRef.current is already set by onExpandedChange (same event,
                // before this effect runs), so anchorBlock is available here.
                const anchorBlockHint = pendingExpansionRef.current?.anchorBlock ?? -1;
                const expansionBlockSize = 100; // must match blockSize prop
                const extendedEnd = anchorBlockHint >= 0
                    ? Math.max(viewportSnapshot.end, (anchorBlockHint + 2) * expansionBlockSize - 1)
                    : viewportSnapshot.end;
                const extendedCount = extendedEnd - viewportSnapshot.start + 1;

                // Record the last block the expansion response will cover so the deferred
                // effect knows to start soft-invalidating from the block AFTER it, rather
                // than re-dirtying block N+1 that we just filled with fresh data.
                if (pendingExpansionRef.current) {
                    pendingExpansionRef.current.extendedToBlock =
                        anchorBlockHint >= 0 ? anchorBlockHint + 1 : -1;
                }

                setPropsRef.current({
                    ...nextProps,
                    viewport: {
                        table: tableName || undefined,
                        start: viewportSnapshot.start,
                        end: extendedEnd,
                        count: extendedCount,
                        version: tx.version,
                        window_seq: tx.version,
                        state_epoch: tx.stateEpoch,
                        session_id: sessionIdRef.current,
                        client_instance: clientInstanceRef.current,
                        abort_generation: tx.abortGeneration,
                        intent: 'expansion',
                        col_start: colRequestStartRef.current !== null ? colRequestStartRef.current : undefined,
                        col_end: colRequestEndRef.current !== null ? colRequestEndRef.current : undefined,
                        needs_col_schema: needsColSchemaRef.current && serverSide || undefined,
                        include_grand_total: (serverSide && showColTotals) || undefined,
                    }
                });
                return;
            }

            // Structural change: full transaction (new stateEpoch clears cache).
            if (serverSide && colFieldsChanged) {
                const prevCount = Array.isArray(lastPropsRef.current.colFields) ? lastPropsRef.current.colFields.length : 0;
                const nextCount = Array.isArray(nextProps.colFields) ? nextProps.colFields.length : 0;
                setPendingColumnSkeletonCount(Math.max(0, nextCount - prevCount));
            } else {
                setPendingColumnSkeletonCount(0);
            }
            const tx = beginStructuralTransaction();
            markRequestPending(tx.version);
            const viewportSnapshot = latestViewportRef.current || { start: 0, end: 99, count: 100 };
            setPropsRef.current({
                ...nextProps,
                viewport: {
                    table: tableName || undefined,
                    start: viewportSnapshot.start,
                    end: viewportSnapshot.end,
                    count: viewportSnapshot.count,
                    version: tx.version,
                    window_seq: tx.version,
                    state_epoch: tx.stateEpoch,
                    session_id: sessionIdRef.current,
                    client_instance: clientInstanceRef.current,
                    abort_generation: tx.abortGeneration,
                    intent: 'structural',
                    needs_col_schema: serverSide || undefined,
                    include_grand_total: (serverSide && showColTotals) || undefined,
                }
            });
        }
    }, [rowFields, colFields, valConfigs, filters, sorting, expanded, showRowTotals, showColTotals, columnPinning, rowPinning, columnVisibility, columnSizing, beginStructuralTransaction, beginExpansionRequest, serverSide, tableName]);

    useEffect(() => {
        const handleClick = () => setContextMenu(null);
        document.addEventListener('click', handleClick);
        return () => document.removeEventListener('click', handleClick);
    }, []);

    const copyToClipboard = (text) => {
        navigator.clipboard.writeText(text);
    };

    const getSelectedData = (withHeaders = false, selectionMap = selectedCells) => {
        const keys = Object.keys(selectionMap || {});
        if (keys.length === 0) return null;

        const visibleRows = table.getRowModel().rows;
        const visibleCols = table.getVisibleLeafColumns();
        
        // Map indices
        const rowIdMap = {};
        visibleRows.forEach((r, i) => rowIdMap[r.id] = i);
        const colIdMap = {};
        visibleCols.forEach((c, i) => colIdMap[c.id] = i);

        let minR = Infinity, maxR = -1, minC = Infinity, maxC = -1;
        const selectedGrid = {};

        keys.forEach(key => {
            const separatorIndex = key.lastIndexOf(':');
            if (separatorIndex <= 0) return;
            const rid = key.slice(0, separatorIndex);
            const cid = key.slice(separatorIndex + 1);
            const rIdx = rowIdMap[rid];
            const cIdx = colIdMap[cid];
            
            if (rIdx !== undefined && cIdx !== undefined) {
                minR = Math.min(minR, rIdx);
                maxR = Math.max(maxR, rIdx);
                minC = Math.min(minC, cIdx);
                maxC = Math.max(maxC, cIdx);
                if (!selectedGrid[rIdx]) selectedGrid[rIdx] = {};
                selectedGrid[rIdx][cIdx] = selectionMap[key];
            }
        });

        if (minR === Infinity) return null;

        let tsv = "";
        
        if (withHeaders) {
            const headerRow = [];
            for (let c = minC; c <= maxC; c++) {
                headerRow.push(visibleCols[c].columnDef.header);
            }
            tsv += headerRow.join("\t") + "\n";
        }

        for (let r = minR; r <= maxR; r++) {
            const rowVals = [];
            for (let c = minC; c <= maxC; c++) {
                const val = selectedGrid[r] && selectedGrid[r][c];
                rowVals.push(val !== undefined && val !== null ? String(val) : "");
            }
            tsv += rowVals.join("\t") + "\n";
        }
        return tsv;
    };

    const hasSelectionKey = useCallback((selectionMap, selectionKey) => {
        if (!selectionMap || !selectionKey) return false;
        return Object.prototype.hasOwnProperty.call(selectionMap, selectionKey);
    }, []);

    const getSelectedColumnIds = useCallback((selectionMap = selectedCells) => {
        const colIds = new Set();
        Object.keys(selectionMap || {}).forEach((selectionKey) => {
            const separatorIndex = selectionKey.lastIndexOf(':');
            if (separatorIndex <= 0) return;
            const colId = selectionKey.slice(separatorIndex + 1);
            if (colId) colIds.add(colId);
        });
        return Array.from(colIds);
    }, [selectedCells]);

    const getSelectedMeasureIndexes = useCallback((selectedColIds) => {
        const matchedIndexes = [];
        valConfigs.forEach((cfg, idx) => {
            const suffix = `_${cfg.field}_${cfg.agg}`;
            const matches = selectedColIds.some(colId => (
                colId === cfg.field ||
                colId.endsWith(suffix) ||
                colId.includes(`_${cfg.field}_`)
            ));
            if (matches) matchedIndexes.push(idx);
        });
        return matchedIndexes;
    }, [valConfigs]);

    const getDefaultFormatForSelection = useCallback((selectionMap) => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        const matchedIndexes = getSelectedMeasureIndexes(selectedColIds);
        if (matchedIndexes.length === 0) return 'fixed:2';

        const formats = matchedIndexes
            .map(idx => valConfigs[idx] && valConfigs[idx].format)
            .filter(fmt => typeof fmt === 'string' && fmt.trim() !== '')
            .map(fmt => fmt.trim());
        const uniqueFormats = Array.from(new Set(formats));
        if (uniqueFormats.length === 1) return uniqueFormats[0];
        return 'fixed:2';
    }, [getSelectedColumnIds, getSelectedMeasureIndexes, valConfigs]);

    const applyDataBarsFromSelection = useCallback((selectionMap, mode = 'col') => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        if (selectedColIds.length === 0) {
            showNotification('Select at least one value cell first.', 'warning');
            return;
        }

        if (mode === 'off') {
            setDataBarsColumns(new Set());
            setColorScaleMode('off');
            showNotification('Data bars disabled.', 'info');
            return;
        }

        setColorScaleMode(mode);
        setDataBarsColumns(prev => {
            const next = new Set(prev || []);
            selectedColIds.forEach(colId => next.add(colId));
            return next;
        });
        if (mode === 'row') {
            showNotification('Data bars enabled (row mode).', 'success');
        } else if (mode === 'table') {
            showNotification('Data bars enabled (table mode).', 'success');
        } else {
            showNotification('Data bars enabled (column mode).', 'success');
        }
    }, [getSelectedColumnIds, setColorScaleMode, setDataBarsColumns, showNotification]);

    const applyFormatToSelection = useCallback((selectionMap, formatOverride = null) => {
        const selectedColIds = getSelectedColumnIds(selectionMap)
            .filter(id => id !== 'hierarchy' && id !== '__row_number__');
        if (selectedColIds.length === 0) {
            showNotification('Select at least one value cell first.', 'warning');
            return;
        }

        let normalizedFormat = '';
        if (formatOverride !== null && formatOverride !== undefined) {
            normalizedFormat = String(formatOverride).trim();
        } else {
            const promptDefault = getDefaultFormatForSelection(selectionMap);
            const formatInput = window.prompt(
                'Format selected values.\nExamples: fixed:2, currency, percent, compact',
                promptDefault
            );
            if (formatInput === null) return;
            normalizedFormat = String(formatInput).trim();
        }

        setValConfigs(prev => {
            let matchedAny = false;
            const matchedIndexes = [];
            prev.forEach((cfg, idx) => {
                const suffix = `_${cfg.field}_${cfg.agg}`;
                const matches = selectedColIds.some(colId => (
                    colId === cfg.field ||
                    colId.endsWith(suffix) ||
                    colId.includes(`_${cfg.field}_`)
                ));
                if (matches) matchedIndexes.push(idx);
            });
            const next = prev.map((cfg, idx) => {
                const matches = matchedIndexes.includes(idx);
                if (!matches) return cfg;
                matchedAny = true;
                if (!normalizedFormat) {
                    const { format, ...rest } = cfg;
                    return rest;
                }
                return { ...cfg, format: normalizedFormat };
            });
            if (!matchedAny) {
                showNotification('No value columns matched the current selection.', 'warning');
                return prev;
            }
            return next;
        });

        if (!normalizedFormat) {
            showNotification('Format cleared for selected values.', 'info');
        } else {
            showNotification(`Format applied: ${normalizedFormat}`, 'success');
        }
    }, [getSelectedColumnIds, getDefaultFormatForSelection, setValConfigs, showNotification]);

    const getPinningState = (colId) => {
        const { left, right } = columnPinning;
        if ((left || []).includes(colId)) return 'left';
        if ((right || []).includes(colId)) return 'right';
        return false;
    };

    const handlePinColumn = useCallback((columnId, side) => {
        const table = tableRef.current;
        if (!table) return;
        
        const col = table.getColumn(columnId);
        if (!col) return;

        // Get all IDs to pin/unpin (leaves + potential collapsed placeholder)
        const idsToUpdate = new Set();
        
        // 1. Add leaf IDs
        const isGroup = col.columns && col.columns.length > 0;
        const leafIds = isGroup ? getAllLeafIdsFromColumn(col) : [columnId];
        leafIds.forEach(id => idsToUpdate.add(id));

        // 2. Add collapsed placeholder ID if it's a pivot group
        if (columnId.startsWith('group_')) {
            const rawPathKey = columnId.replace('group_', '');
            idsToUpdate.add(`${rawPathKey}_collapsed`);
        }

        const idsArray = Array.from(idsToUpdate);

        setColumnPinning(prev => {
            const next = { left: [...(prev.left || [])], right: [...(prev.right || [])] };

            // Remove all relevant IDs from both sides first
            next.left = next.left.filter(id => !idsToUpdate.has(id));
            next.right = next.right.filter(id => !idsToUpdate.has(id));

            // Add to new side
            if (side === 'left') next.left.push(...idsArray);
            if (side === 'right') next.right.push(...idsArray);

            return next;
        });
    }, []);


    const handlePinRow = (rowId, pinState) => {
        setRowPinning(prev => {
            const next = { ...prev, top: [...prev.top], bottom: [...prev.bottom] };
            next.top = next.top.filter(d => d !== rowId);
            next.bottom = next.bottom.filter(d => d !== rowId);
            if (pinState === 'top') next.top.push(rowId);
            if (pinState === 'bottom') next.bottom.push(rowId);
            return next;
        });

        // Fire Pinning Event
        if (setProps) {
            setProps({
                rowPinned: {
                    rowId: rowId,
                    pinState: pinState,
                    timestamp: Date.now()
                }
            });
        }
    };

    useEffect(() => {
        setColumnPinning(prev => {
            let nextLeft = [...(prev.left || [])];
            let changed = false;

            // 1. Enforce Hierarchy Pinning
            if (layoutMode === 'hierarchy' && rowFields.length > 0) {
                if (!nextLeft.includes('hierarchy')) {
                     nextLeft = ['hierarchy', ...nextLeft];
                     changed = true;
                }
            }

            // 2. Enforce Row Number Pinning (User Request: Always utmost left)
            if (showRowNumbers) {
                if (!nextLeft.includes('__row_number__')) {
                    nextLeft = ['__row_number__', ...nextLeft];
                    changed = true;
                }
                // Ensure it is first (utmost left)
                const idx = nextLeft.indexOf('__row_number__');
                if (idx > 0) {
                    nextLeft.splice(idx, 1);
                    nextLeft.unshift('__row_number__');
                    changed = true;
                }
            } else {
                 // If hidden, remove from pinned? (Optional, but clean)
                 if (nextLeft.includes('__row_number__')) {
                     nextLeft = nextLeft.filter(id => id !== '__row_number__');
                     changed = true;
                 }
            }

            if (changed) {
                debugLog('Pinning Enforcement Triggered', nextLeft);
                return { ...prev, left: nextLeft };
            }
            return prev;
        });
    }, [layoutMode, rowFields.length, showRowNumbers]);

    // 4. FIXED: handleHeaderContextMenu with proper group detection
    const handleHeaderContextMenu = (e, colId) => {
        e.preventDefault();
        const actions = [];
        const column = table.getColumn(colId);

        if (!column) {
            return;
        }

        const { left, right } = columnPinning;

        // Determine if this is a group column
        const isGroup = isGroupColumn(column);

        // Only show sort options for leaf columns
        if (!isGroup) {
            actions.push({
                label: 'Sort Ascending',
                icon: <Icons.SortAsc/>,
                onClick: () => column.toggleSorting(false)
            });
            actions.push({
                label: 'Sort Descending',
                icon: <Icons.SortDesc/>,
                onClick: () => column.toggleSorting(true)
            });
            actions.push({
                label: 'Clear Sort',
                onClick: () => column.clearSorting()
            });

            actions.push('separator');
            actions.push({
                label: 'Filter...',
                icon: <Icons.Filter/>,
                onClick: () => setActiveFilterCol(colId)
            });
            actions.push({
                label: 'Clear Filter',
                onClick: () => handleHeaderFilter(colId, null)
            });

            actions.push('separator');
        }

        // Pin options for both leaf and group columns
        let isPinned = false;

        if (isGroup) {
            // For group columns, check if ALL leaf columns are pinned
            const leafIds = getAllLeafIdsFromColumn(column);

            const allPinnedLeft = leafIds.length > 0 && leafIds.every(id => (left || []).includes(id));
            const allPinnedRight = leafIds.length > 0 && leafIds.every(id => (right || []).includes(id));

            if (allPinnedLeft) isPinned = 'left';
            else if (allPinnedRight) isPinned = 'right';

            actions.push({
                label: 'Pin All Children Left',
                onClick: () => handlePinColumn(colId, 'left')
            });
            actions.push({
                label: 'Pin All Children Right',
                onClick: () => handlePinColumn(colId, 'right')
            });
            if (isPinned) {
                actions.push({
                    label: 'Unpin All Children',
                    onClick: () => handlePinColumn(colId, false)
                });
            }
        } else {
            // For leaf columns, check directly
            if ((left || []).includes(colId)) isPinned = 'left';
            else if ((right || []).includes(colId)) isPinned = 'right';

            actions.push({
                label: 'Pin Column Left',
                onClick: () => handlePinColumn(colId, 'left')
            });
            actions.push({
                label: 'Pin Column Right',
                onClick: () => handlePinColumn(colId, 'right')
            });
            if (isPinned) {
                actions.push({
                    label: 'Unpin Column',
                    onClick: () => handlePinColumn(colId, false)
                });
            }
        }

        actions.push('separator');
        actions.push({
            label: 'Expand All Rows',
            onClick: () => handleExpandAllRows(true)
        });
        actions.push({
            label: 'Collapse All Rows',
            onClick: () => handleExpandAllRows(false)
        });

        actions.push('separator');
        actions.push({
            label: 'Auto-size Column',
            onClick: () => autoSizeColumn(colId)
        });
        actions.push({
            label: 'Export to Excel',
            icon: <Icons.Export/>,
            onClick: exportPivot
        });

        setContextMenu({
            x: e.clientX,
            y: e.clientY,
            actions: actions
        });
    };

    const handleContextMenu = (e, value, colId, row) => {
        e.preventDefault();
        const rowId = row ? row.id : null;
        const key = rowId ? `${rowId}:${colId}` : null;
        // Track the row path for "Format Row" feature
        if (row && row.original && row.original._path) {
            setHoveredRowPath(row.original._path);
        }

        // Right-click should operate on the clicked cell immediately.
        // If it was not selected yet, promote it to the active selection.
        let selectionForMenu = selectedCells;
        if (rowId && key && !hasSelectionKey(selectedCells, key)) {
            selectionForMenu = { [key]: value };
            setSelectedCells(selectionForMenu);
            const visibleCols = table.getVisibleLeafColumns();
            const colIndex = visibleCols.findIndex(c => c.id === colId);
            if (row && typeof row.index === 'number' && colIndex >= 0) {
                setLastSelected({ rowIndex: row.index, colIndex });
            }
        }

        const hasSelection = Object.keys(selectionForMenu).length > 0;
        
        const getTableData = (withHeaders) => {
            const visibleRows = table.getRowModel().rows;
            const visibleCols = table.getVisibleLeafColumns();
            let tsv = "";
            if (withHeaders) {
                tsv += visibleCols.map(c => typeof c.columnDef.header === 'string' ? c.columnDef.header : c.id).join('\t') + '\n';
            }
            visibleRows.forEach(r => {
                const vals = visibleCols.map(c => {
                    const v = r.getValue(c.id);
                    return v !== undefined && v !== null ? String(v) : "";
                });
                tsv += vals.join('\t') + '\n';
            });
            return tsv;
        };

        const actions = [
            { label: 'Copy Table', icon: <Icons.DragIndicator/>, onClick: () => copyToClipboard(getTableData(false)) },
            { label: 'Copy Table with Headers', onClick: () => copyToClipboard(getTableData(true)) },
        ];

        if (hasSelection) {
            actions.push('separator');
            actions.push({ label: 'Copy Selection', onClick: () => {
                const data = getSelectedData(false, selectionForMenu);
                if (data) copyToClipboard(data);
            }});
            actions.push({ label: 'Copy Selection with Headers', onClick: () => {
                const data = getSelectedData(true, selectionForMenu);
                if (data) copyToClipboard(data);
            }});
        }

        actions.push('separator');
        actions.push({ label: `Filter by "${value}"`, icon: <Icons.Filter/>, onClick: () => {
            handleHeaderFilter(colId, {
                operator: 'AND',
                conditions: [{ type: 'eq', value: String(value), caseSensitive: false }]
            });
        }});
        actions.push({ label: 'Clear Filter', onClick: () => handleHeaderFilter(colId, null) });

        actions.push('separator');
        actions.push({ label: 'Drill Through', icon: <Icons.Search/>, onClick: () => {
             if (row && row.original && row.original._path && row.original._path !== '__grand_total__' && !row.original._isTotal) {
                 fetchDrillData(row.original._path, 0, null, 'asc', '');
             }
        }});

        actions.push('separator');
        if (row && serverSide && row.getCanExpand() && row.original && row.original._path && rowFields.length > 1) {
            actions.push({
                label: 'Expand All Children',
                icon: <Icons.ChevronDown/>,
                onClick: () => {
                    const rowPath = row.original._path;
                    subtreeExpandRef.current = { path: rowPath, expandedPaths: new Set([rowPath]) };
                    captureExpansionScrollPosition();
                    clearCache();
                    setExpanded(prev => {
                        const base = (prev !== null && typeof prev === 'object') ? prev : {};
                        return { ...base, [rowPath]: true };
                    });
                }
            });
        }

        actions.push('separator');
        if (rowId) {
            const isPinnedTop = rowPinning.top.includes(rowId);
            const isPinnedBottom = rowPinning.bottom.includes(rowId);

            actions.push({ label: 'Pin Row Top', onClick: () => handlePinRow(rowId, 'top') });
            actions.push({ label: 'Pin Row Bottom', onClick: () => handlePinRow(rowId, 'bottom') });
            if (isPinnedTop || isPinnedBottom) {
                actions.push({ label: 'Unpin Row', onClick: () => handlePinRow(rowId, false) });
            }
        }

        setContextMenu({
            x: e.clientX,
            y: e.clientY,
            actions: actions
        });
    };

    // Extract color scale stats sentinel injected by the server, strip it from rows
    const { cleanData, serverColorScaleStats } = useMemo(() => {
        const sentinelIdx = data.findIndex(r => r && r._path === '__color_scale_stats__');
        if (sentinelIdx === -1) return { cleanData: data, serverColorScaleStats: null };
        const stats = data[sentinelIdx]._colorScaleStats || null;
        const rows = data.filter((_, i) => i !== sentinelIdx);
        return { cleanData: rows, serverColorScaleStats: stats };
    }, [data]);

    const filteredData = useFilteredData(cleanData, filters, serverSide);

    const staticTotal = useMemo(() => ({ _isTotal: true, _path: '__grand_total__', _id: 'Grand Total', __isGrandTotal__: true }), []);
    const staticMinMax = useMemo(() => ({}), []);

    const { nodes, total, minMax } = useMemo(() => {
        return { nodes: filteredData, total: staticTotal, minMax: staticMinMax };
    }, [filteredData, staticTotal, staticMinMax]);

    const handleHeaderFilter = (columnId, filterValue) => {
        setFilters(prev => {
            const newFilters = {...prev};
            if (filterValue === null || filterValue.conditions.length === 0) {
                delete newFilters[columnId];
            } else {
                newFilters[columnId] = filterValue;
            }
            return newFilters;
        });
    };

    const autoSizeColumn = (columnId) => {
        const rows = table.getRowModel().rows;
        const sampleRows = rows.slice(0, 100); 
        let maxWidth = 0;
        
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        context.font = '13px Roboto, Helvetica, Arial, sans-serif'; 
        
        const column = table.getColumn(columnId);
        const header = column.columnDef.header;
        const headerText = typeof header === 'string' ? header : columnId;
        maxWidth = context.measureText(headerText).width + autoSizeBounds.headerPadding;
        
        sampleRows.forEach(row => {
            const cellValue = row.getValue(columnId);
            const text = formatValue(cellValue); 
            const width = context.measureText(text).width + autoSizeBounds.cellPadding;
            if (width > maxWidth) maxWidth = width;
        });
        
        maxWidth = Math.min(maxWidth, autoSizeBounds.maxWidth);
        maxWidth = Math.max(maxWidth, autoSizeBounds.minWidth);
        
        table.setColumnSizing(old => ({
            ...old,
            [columnId]: maxWidth
        }));
    };

    const handleFilterClick = (e, columnId) => {
        e.stopPropagation();
        setActiveFilterCol(columnId);
        setFilterAnchorEl(e.currentTarget);
        if (setProps) {
            setProps({
                filters: { ...filters, '__request_unique__': columnId }
            });
        }
    };

    const closeFilterPopover = () => {
        setActiveFilterCol(null);
        setFilterAnchorEl(null);
    };

    useEffect(() => {
        const activeLeafIds = new Set();
        const stack = [...columns];

        while (stack.length > 0) {
            const columnDef = stack.pop();
            if (!columnDef) continue;

            if (Array.isArray(columnDef.columns) && columnDef.columns.length > 0) {
                columnDef.columns.forEach(child => stack.push(child));
                continue;
            }

            const leafId = columnDef.id || columnDef.accessorKey;
            if (leafId) {
                activeLeafIds.add(String(leafId));
            }
        }

        setColumnSizing(prev => {
            if (!prev || typeof prev !== 'object' || Object.keys(prev).length === 0) {
                return prev;
            }

            let changed = false;
            const next = {};
            Object.keys(prev).forEach(key => {
                if (activeLeafIds.has(key)) {
                    next[key] = prev[key];
                } else {
                    changed = true;
                }
            });

            return changed ? next : prev;
        });
    }, [columns]);

    const parentRef = useRef(null);
    const expansionScrollRestoreRef = useRef(null);
    const expansionScrollRestoreRafRef = useRef(null);
    // Tracks an in-progress "expand all children" operation.
    // { path: string, expandedPaths: Set<string> }
    // We use a ref (not state) to avoid dependency cycles in the watcher effect.
    const subtreeExpandRef = useRef(null);
    // After a single-row expansion, holds the anchor block index so we can
    // invalidate subsequent blocks once the expansion response has landed.
    // Doing it after the response avoids a concurrent viewport request that
    // would race with (and stale-reject) the expansion request.
    const pendingExpansionRef = useRef(null);
    const rowHeight = rowHeights[spacingMode] || rowHeights[0];

    // Cache key: only structural changes that require a full cache wipe.
    // Expansion and rowCount are intentionally excluded — expansion uses targeted
    // block invalidation (invalidateFromBlock) so rows before the toggled node
    // stay cached, and rowCount is a derived result that changes with expansion.
    const serverSideCacheKey = useMemo(() => JSON.stringify({
        sorting,
        filters,
        rowFields,
        colFields,
        valConfigs,
    }), [sorting, filters, rowFields, colFields, valConfigs]);
    // Viewport reset key: only changes that semantically restart the user's view
    // (new sort/filter/fields). rowCount is excluded because it changes when rows
    // are expanded, which must NOT scroll back to the top.
    const serverSideViewportResetKey = useMemo(() => JSON.stringify({
        sorting,
        filters,
        rowFields,
        colFields,
        valConfigs,
    }), [sorting, filters, rowFields, colFields, valConfigs]);

    const serverSidePinsGrandTotal = serverSide && showColTotals;
    const effectiveRowCount = serverSidePinsGrandTotal && rowCount ? Math.max(rowCount - 1, 0) : rowCount;
    const statusRowCount = serverSidePinsGrandTotal && rowCount ? Math.max(rowCount - 1, 0) : rowCount;

    const captureExpansionScrollPosition = useCallback(() => {
        if (!serverSide || !parentRef.current) return;
        expansionScrollRestoreRef.current = {
            scrollTop: parentRef.current.scrollTop,
            restorePassesRemaining: 3
        };
    }, [serverSide]);

    const { rowVirtualizer, getRow, renderedData, renderedOffset, clearCache, invalidateFromBlock, softInvalidateFromBlock, grandTotalRow } = useServerSideRowModel({
        parentRef,
        serverSide,
        rowCount: effectiveRowCount,
        rowHeight,
        data: filteredData,
        dataOffset: dataOffset || 0,
        dataVersion: dataVersion || 0,
        setProps,
        blockSize: 100,
        cacheKey: serverSideCacheKey,
        excludeGrandTotal: serverSidePinsGrandTotal,
        stateEpoch,
        sessionId: sessionIdRef.current,
        clientInstance: clientInstanceRef.current,
        abortGeneration,
        structuralInFlight,
        requestVersionRef,
        tableName,
        colStart: colRequestStart,
        colEnd: colRequestEnd,
        needsColSchema: needsColSchema && serverSide,
        onViewportRequest: markRequestPending,
    });

    const columns = useColumnDefs({
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
        // Render-time closures (stable refs or render-time reads)
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
    });

    // Auto-size new columns to fit their header text on spawn
    useEffect(() => {
        if (!table || !table.getVisibleLeafColumns) return;
        const visLeafCols = table.getVisibleLeafColumns();
        if (visLeafCols.length === 0) return;
        const canvas = document.createElement('canvas');
        const ctx = canvas.getContext('2d');
        ctx.font = `600 13px system-ui, Arial, sans-serif`;
        const newSizes = {};
        visLeafCols.forEach(col => {
            if (columnSizing[col.id] !== undefined) return; // already user-set
            const headerText = typeof col.columnDef.header === 'string' ? col.columnDef.header : col.id;
            const measured = ctx.measureText(headerText).width + autoSizeBounds.headerPadding;
            const clamped = Math.min(Math.max(measured, autoSizeBounds.minWidth), 300);
            if (clamped > (col.columnDef.size || autoSizeBounds.minWidth)) {
                newSizes[col.id] = clamped;
            }
        });
        if (Object.keys(newSizes).length > 0) {
            setColumnSizing(prev => ({ ...newSizes, ...prev }));
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [colFields, valConfigs, rowFields]);

    useEffect(() => {
        if (!serverSide || !structuralInFlight) return;
        const pending = structuralPendingVersionRef.current;
        const numericVersion = Number(dataVersion);
        if (!pending || !Number.isFinite(numericVersion)) return;
        if (numericVersion > pending.startDataVersion && numericVersion >= pending.version) {
            setStructuralInFlight(false);
            structuralPendingVersionRef.current = null;
            setPendingRowTransitions(new Map());
            setPendingColumnSkeletonCount(0);
        }
    }, [dataVersion, serverSide, structuralInFlight]);

    useEffect(() => {
        if (!serverSide || !structuralInFlight) return;
        const timeoutId = setTimeout(() => {
            setStructuralInFlight(false);
            structuralPendingVersionRef.current = null;
            setPendingRowTransitions(new Map());
            setPendingColumnSkeletonCount(0);
        }, 10000);
        return () => clearTimeout(timeoutId);
    }, [serverSide, structuralInFlight, stateEpoch]);

    const tableData = useMemo(() => {
        if (serverSide) {
             const centerData = renderedData.filter(row => (
                 row &&
                 !row._isTotal &&
                 row._path !== '__grand_total__' &&
                 row._id !== 'Grand Total'
             ));
             if (showColTotals && grandTotalRow) {
                 return [...centerData, grandTotalRow];
             }
             return centerData;
        }

        let baseData = (filteredData.length ? [...nodes] : []);

        if (!showColTotals) {
            baseData = baseData.filter(r => !r._isTotal);
        }

        // Add the grand total to the end of the data array to ensure it appears at the bottom
        // Only add if not in serverSide mode and showColTotals is true
        if (!serverSide && showColTotals) {
            baseData = [...baseData, total];
        }

        return baseData;
    }, [nodes, total, filteredData, serverSide, showColTotals, renderedData, grandTotalRow]);

    // Color scale palettes: [lowColor, highColor, darkTextLow, darkTextHigh]
    const COLOR_PALETTES = {
        redGreen:   { low: [248,105,107], high: [99,190,123],  darkLow: '#5c0001', darkHigh: '#1a4d2a' },
        greenRed:   { low: [99,190,123],  high: [248,105,107], darkLow: '#1a4d2a', darkHigh: '#5c0001' },
        blueWhite:  { low: [65,105,225],  high: [255,255,255], darkLow: '#0d2b6e', darkHigh: '#333'    },
        yellowBlue: { low: [255,220,0],   high: [30,90,200],   darkLow: '#5a4700', darkHigh: '#0a2a6e' },
        orangePurp: { low: [255,140,0],   high: [120,50,200],  darkLow: '#5c3000', darkHigh: '#2a0060' },
    };

    // Client-side stats (non-server mode): exclude totals and row/col fields
    const clientColorScaleStats = useMemo(() => {
        if (serverSide) return { byCol: {}, byRow: {}, table: null };
        const metaKeys = new Set([
            '_id', '_path', '_isTotal', 'depth', '_depth', '_level', '_expanded',
            '_parentPath', '_has_children', '_is_expanded', 'subRows', 'uuid', '__virtualIndex',
            '__colPending', '__isGrandTotal__'
        ]);
        rowFields.forEach(f => metaKeys.add(f));
        colFields.forEach(f => metaKeys.add(f));

        const byCol = {};
        const byRow = {};
        let tableMin = Number.POSITIVE_INFINITY;
        let tableMax = Number.NEGATIVE_INFINITY;

        // Use filteredData (not tableData) so grand total synthetic row is never included
        const sourceData = filteredData.length > 0 ? filteredData : nodes;
        sourceData.forEach((row, idx) => {
            if (!row || typeof row !== 'object') return;
            // Skip all total rows
            if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return;

            const rowKey = String(row._path || row._id || idx);
            let rowMin = Number.POSITIVE_INFINITY;
            let rowMax = Number.NEGATIVE_INFINITY;

            Object.entries(row).forEach(([key, raw]) => {
                if (metaKeys.has(key)) return;
                if (typeof raw !== 'number' || Number.isNaN(raw)) return;

                if (!byCol[key]) {
                    byCol[key] = { min: raw, max: raw };
                } else {
                    byCol[key].min = Math.min(byCol[key].min, raw);
                    byCol[key].max = Math.max(byCol[key].max, raw);
                }

                rowMin = Math.min(rowMin, raw);
                rowMax = Math.max(rowMax, raw);
                tableMin = Math.min(tableMin, raw);
                tableMax = Math.max(tableMax, raw);
            });

            if (Number.isFinite(rowMin) && Number.isFinite(rowMax)) {
                byRow[rowKey] = { min: rowMin, max: rowMax };
            }
        });

        return {
            byCol,
            byRow,
            table: Number.isFinite(tableMin) && Number.isFinite(tableMax)
                ? { min: tableMin, max: tableMax }
                : null,
        };
    }, [filteredData, nodes, rowFields, colFields, serverSide]);

    // Use server-provided stats in server mode, client-computed stats otherwise
    const colorScaleStats = serverSide && serverColorScaleStats ? serverColorScaleStats : clientColorScaleStats;

    const getConditionalStyle = useCallback((colId, value, rowData, rowId) => {
        const ruleStyle = getRuleBasedStyle(colId, value);
        if (colorScaleMode === 'off' || typeof value !== 'number' || Number.isNaN(value)) {
            return ruleStyle;
        }
        // Skip total rows in color scale
        if (rowData && (rowData._isTotal || rowData._path === '__grand_total__' || rowData._id === 'Grand Total')) {
            return ruleStyle;
        }

        let stats = null;
        if (colorScaleMode === 'col') {
            stats = (colorScaleStats.byCol && colorScaleStats.byCol[colId]) || null;
        } else if (colorScaleMode === 'row') {
            const rowKey = String((rowData && rowData._path) || rowId || '');
            stats = (colorScaleStats.byRow && colorScaleStats.byRow[rowKey]) || null;
        } else if (colorScaleMode === 'table') {
            stats = colorScaleStats.table || null;
        }

        if (!stats || !Number.isFinite(stats.min) || !Number.isFinite(stats.max) || stats.max === stats.min) {
            return ruleStyle;
        }

        const palette = COLOR_PALETTES[colorPalette] || COLOR_PALETTES.redGreen;
        const { low, high, darkLow, darkHigh } = palette;

        // Handle zero-crossing: when data spans negative and positive,
        // use 0 as the neutral (transparent) midpoint
        let posInRange; // 0=low extreme, 1=high extreme, 0.5=neutral
        const hasZeroCrossing = stats.min < 0 && stats.max > 0;
        if (hasZeroCrossing) {
            // Map negative values [min,0] → [0,0.5] and positive [0,max] → [0.5,1]
            if (value <= 0) {
                posInRange = 0.5 * (value - stats.min) / (0 - stats.min);
            } else {
                posInRange = 0.5 + 0.5 * value / stats.max;
            }
        } else {
            posInRange = (value - stats.min) / (stats.max - stats.min);
        }
        const clamped = Math.max(0, Math.min(1, posInRange));

        // Transparency: fully transparent at midpoint (0.5), max opacity at extremes
        // alpha range 0.06 → 0.82 for a clean gradient feel
        const distFromMid = Math.abs(clamped - 0.5) * 2; // 0 at mid, 1 at extremes
        const alpha = 0.06 + distFromMid * 0.76;

        const [r, g, b] = clamped <= 0.5 ? low : high;
        const darkText = clamped <= 0.5 ? darkLow : darkHigh;

        const heatStyle = {
            background: `rgba(${r},${g},${b},${alpha.toFixed(3)})`,
            color: alpha > 0.55 ? darkText : undefined,
        };
        return { ...heatStyle, ...ruleStyle };
    }, [colorScaleMode, colorPalette, colorScaleStats, getRuleBasedStyle]);

    const getRowId = useCallback((row, relativeIndex) => {
        if (!row) return `skeleton_${relativeIndex}`; // Handle skeleton rows
        if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return '__grand_total__';
        if (serverSide && typeof row.__virtualIndex === 'number') {
            return row._path || (row.id ? row.id : String(row.__virtualIndex));
        }
        
        // Use renderedOffset if available (from virtualizer cache), else fallback to dataOffset
        const effectiveOffset = (serverSide && renderedOffset !== undefined) ? renderedOffset : (dataOffset || 0);
        const actualIndex = serverSide ? relativeIndex + effectiveOffset : relativeIndex;
        
        return row._path || (row.id ? row.id : String(actualIndex));
    }, [serverSide, dataOffset, renderedOffset]);
    const getSubRows = useCallback(r => r ? r.subRows : undefined, []);
    const getRowCanExpand = useCallback(row => {
        if (!row.original) return false;
        // Prevent expansion of any total rows, including grand totals
        if (row.original && row.original._isTotal) return false;
        
        if (serverSide) {
             // Use server-provided flag if available for accurate child detection
             if (row.original._has_children !== undefined) return row.original._has_children;
             return (row.original.depth || 0) < rowFields.length - 1;
        }
        
        return row.subRows && row.subRows.length > 0;
    }, [serverSide, rowFields.length]);

    const getIsRowExpanded = useCallback(row => {
        if (!row.original) return false;
        if (row.original && row.original._isTotal) return false;

        if (serverSide) {
             // 1. "Expand All" mode
             if (expanded === true) return true;
             
             // 2. Explicit Local State (Optimistic)
             // We check if the key exists in the expanded object to respect user interactions
             if (expanded && Object.prototype.hasOwnProperty.call(expanded, row.id)) {
                 return !!expanded[row.id];
             }
             
             // 3. Server State (Fallback/Source of Truth)
             // If local state doesn't know about this row yet (e.g. initial load), trust the server
             if (row.original._is_expanded !== undefined) {
                 return row.original._is_expanded;
             }
        }

        // Standard Client-Side Logic
        if (expanded === true) return true;
        // Otherwise check if this specific row is expanded
        return !!expanded[row.id];
    }, [expanded, serverSide]);

    const tableState = useMemo(() => {
        // Automatically pin Grand Total to top or bottom based on grandTotalPosition prop
        let finalRowPinning = rowPinning;
        const grandTotalId = '__grand_total__';
        const pinToBottom = grandTotalPosition === 'bottom';

        // Find the actual Grand Total row in the data and get its real ID
        let actualGrandTotalRowId = null;
        if (tableData) {
            for (const row of tableData) {
                if (!row) continue;
                if (row.__isGrandTotal__ || row._path === '__grand_total__' || row._id === 'Grand Total') {
                    if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') {
                        actualGrandTotalRowId = '__grand_total__';
                        break;
                    }
                }
            }
        }

        if (actualGrandTotalRowId) {
            const topWithoutGrandTotal = (rowPinning.top || []).filter(id => id !== actualGrandTotalRowId);
            const bottomWithoutGrandTotal = (rowPinning.bottom || []).filter(id => id !== actualGrandTotalRowId);

            finalRowPinning = {
                ...rowPinning,
                top: pinToBottom ? topWithoutGrandTotal : [...topWithoutGrandTotal, actualGrandTotalRowId],
                bottom: pinToBottom ? [...bottomWithoutGrandTotal, actualGrandTotalRowId] : bottomWithoutGrandTotal,
            };
        } else {
            // If GT is NOT in data, ensure it is NOT pinned (to avoid crash)
            const cleanPinning = {
                top: (rowPinning.top || []).filter(id => id !== grandTotalId),
                bottom: (rowPinning.bottom || []).filter(id => id !== grandTotalId),
            };
            if (cleanPinning.top.length !== (rowPinning.top || []).length ||
                cleanPinning.bottom.length !== (rowPinning.bottom || []).length) {
                finalRowPinning = { ...rowPinning, ...cleanPinning };
            }
        }

        return {
            sorting,
            expanded,
            columnPinning,
            rowPinning: finalRowPinning,
            grouping: rowFields,
            columnVisibility,
            columnSizing
        };
    }, [sorting, expanded, columnPinning, rowPinning, rowFields, columnVisibility, columnSizing, tableData, grandTotalPosition]);



    const handleExpandAllRows = (shouldExpand) => {
        // Guard against rapid double-clicks: second click before server responds
        // would call clearCache() again but the net expanded state change may be
        // batched away by React, leaving the cache empty with no request sent.
        if (expandAllDebounceRef.current) return;
        expandAllDebounceRef.current = true;
        setTimeout(() => { expandAllDebounceRef.current = false; }, 800);

        if (serverSide) {
            captureExpansionScrollPosition();
            // Expanding/collapsing ALL rows changes every row index — full cache wipe.
            // (This path bypasses onExpandedChange so invalidateFromBlock won't run.)
            clearCache();
            setExpanded(shouldExpand ? true : {});
            return;
        }

        if (shouldExpand) {
            // Expand all rows by creating an object with all row IDs set to true
            const allRows = table.getCoreRowModel().rows;
            const newExpanded = {};

            allRows.forEach(row => {
                // Only add rows that can be expanded and are not totals
                if (row.getCanExpand() && !(row.original && row.original._isTotal)) {
                    newExpanded[row.id] = true;

                    // Also expand sub-rows recursively
                    const expandSubRows = (subRows) => {
                        subRows.forEach(subRow => {
                            if (subRow.getCanExpand() && !(subRow.original && subRow.original._isTotal)) {
                                newExpanded[subRow.id] = true;
                                if (subRow.subRows && subRow.subRows.length > 0) {
                                    expandSubRows(subRow.subRows);
                                }
                            }
                        });
                    };

                    if (row.subRows && row.subRows.length > 0) {
                        expandSubRows(row.subRows);
                    }
                }
            });

            setExpanded(newExpanded);
        } else {
            // Collapse all by setting empty object
            setExpanded({});
        }
    };

    const handleSortingChange = (updater) => {
        const newSorting = typeof updater === 'function' ? updater(sorting) : updater;
        setSorting(newSorting);

        // Fire sort event to backend
        if (setPropsRef.current) {
            setPropsRef.current({
                sorting: newSorting,
                sortEvent: {
                    type: 'change',
                    status: 'applied',
                    sorting: newSorting,
                    timestamp: Date.now()
                }
            });
        }
    };

    const table = useReactTable({
        data: tableData,
        columns,
        state: tableState,
        onSortingChange: (updater) => { handleSortingChange(updater); },
        onExpandedChange: (updater) => {
            captureExpansionScrollPosition();
            const newExpanded = typeof updater === 'function' ? updater(expanded) : updater;

            if (serverSide) {
                // Find which path was toggled so we know which block to defer-invalidate
                // after the expansion response lands (see pendingExpansionRef effect).
                // Value-diff: detect any key whose boolean value flipped (covers
                // both key-add/remove AND false→true / true→false toggles).
                const oldExp = expanded || {};
                const newExp = newExpanded || {};
                const allKeys = new Set([...Object.keys(oldExp), ...Object.keys(newExp)]);
                const changedPath = [...allKeys].find(k => !!oldExp[k] !== !!newExp[k]);
                if (changedPath) {
                    const isNowExpanded = !!(newExpanded && newExpanded[changedPath]);
                    setPendingRowTransitions(prev => {
                        const next = new Map(prev);
                        next.set(changedPath, isNowExpanded ? 'expand' : 'collapse');
                        return next;
                    });
                }

                // -1 signals "row not in viewport — do a full cache clear".
                let anchorBlock = -1;
                let expandedRowVirtualIndex = undefined;
                if (changedPath) {
                    const toggledRow = renderedData.find(r => r && r._path === changedPath);
                    if (toggledRow && typeof toggledRow.__virtualIndex === 'number') {
                        anchorBlock = Math.floor(toggledRow.__virtualIndex / 100);
                        // Record the virtual index for viewport anchor preservation.
                        // When rows are inserted/removed ABOVE the current scroll position
                        // we adjust scrollTop so the same logical rows remain in view.
                        expandedRowVirtualIndex = toggledRow.__virtualIndex;
                    }
                    // Do NOT fall back to the scroll position when the row is not in the
                    // rendered viewport.  Using the viewport block as the anchor leaves all
                    // blocks between the expanded row and the viewport with stale (shifted)
                    // row indices.  A full cache clear (anchorBlock = -1) is safer.
                }
                // Don't invalidate now — doing so fires a concurrent viewport request
                // that races with the expanded sync request and causes a stale rejection.
                // Record the anchor so the deferred effect invalidates subsequent blocks
                // once the expansion response has landed (dataVersion bump).
                pendingExpansionRef.current = { anchorBlock, expandedRowVirtualIndex, oldRowCount: rowCount };
            }

            setExpanded(newExpanded);
        },
        onColumnPinningChange: (updater) => { debugLog('onColumnPinningChange'); setColumnPinning(updater); },
        onRowPinningChange: (updater) => { debugLog('onRowPinningChange'); setRowPinning(updater); },
        onColumnVisibilityChange: (updater) => { debugLog('onColumnVisibilityChange'); setColumnVisibility(updater); },
        onColumnSizingChange: (updater) => { debugLog('onColumnSizingChange'); setColumnSizing(updater); },
        getRowId,
        getCoreRowModel: getCoreRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getGroupedRowModel: getGroupedRowModel(),
        getSubRows,
        enableRowPinning: true, // Enable Row Pinning
        enableColumnResizing: true,
        enableMultiSort: true, // Explicitly enable multi-sort
        columnResizeMode: 'onChange',
        manualPagination: serverSide,
        manualSorting: serverSide,
        manualFiltering: serverSide,
        manualGrouping: serverSide,
        manualExpanding: serverSide,
        pageCount: serverSide ? Math.ceil((rowCount || 0) / 100) : undefined,
        getRowCanExpand,
        getIsRowExpanded,
        enableColumnPinning: true,
    });

    // Update the ref with the current table instance
    useEffect(() => {
        tableRef.current = table;
    }, [table]);

    // Accessibility & Event System Effect for Sorting
    useEffect(() => {
        if (sorting.length > 0) {
            const sortDesc = sorting[0].desc ? 'descending' : 'ascending';
            const colId = sorting[0].id;
            const col = tableRef.current.getColumn(colId);
            const colName = col ? (typeof col.columnDef.header === 'string' ? col.columnDef.header : colId) : colId;
            setAnnouncement(`Sorted by ${colName} ${sortDesc}`);
        } else {
            setAnnouncement("Sorting cleared");
        }
        
        // Fire sort event
        if (setPropsRef.current) {
             setPropsRef.current({
                sortEvent: {
                    type: 'change',
                    status: 'applied',
                    sorting: sorting,
                    timestamp: Date.now()
                }
            });
        }
    }, [sorting]); // Removed table and setProps from dependencies

    const toggleAllColumnsPinned = (pinState) => {
        const leafColumns = table.getAllLeafColumns();
        const newPinning = { left: [], right: [] };
        
        if (pinState === 'left') {
            newPinning.left = leafColumns.map(c => c.id).filter(id => id !== 'no_data');
        } else if (pinState === 'right') {
            newPinning.right = leafColumns.map(c => c.id).filter(id => id !== 'no_data');
        } else {
            if (layoutMode === 'hierarchy') {
                newPinning.left = ['hierarchy'];
            }
        }
        
        setColumnPinning(newPinning);
    };

    const activeFilterOptions = useMemo(() => {
        if (!activeFilterCol) return [];
        if (filterOptions[activeFilterCol]) return filterOptions[activeFilterCol];
        
        const col = table.getColumn(activeFilterCol);
        if (!col) return [];
        
        const unique = new Set();
        const rows = table.getCoreRowModel().rows;
        rows.forEach(row => {
            const val = row.getValue(activeFilterCol);
            if (val !== null && val !== undefined) unique.add(val);
        });
        
        return Array.from(unique).sort();
    }, [activeFilterCol, filterOptions, table]);

    const { rows } = table.getRowModel();
    const topRows = table.getTopRows();
    const bottomRows = table.getBottomRows();
    const centerRows = table.getCenterRows();
    const lastStableRowModelRef = useRef({
        topRows: [],
        centerRows: [],
        bottomRows: []
    });
    const hasRenderedData = renderedData.some(Boolean);

    useEffect(() => {
        if (!serverSide) return;
        if (centerRows.length > 0 || topRows.length > 0 || bottomRows.length > 0) {
            lastStableRowModelRef.current = { topRows, centerRows, bottomRows };
        }
    }, [serverSide, topRows, centerRows, bottomRows]);

    useEffect(() => {
        if (!serverSide) return;
        lastStableRowModelRef.current = {
            topRows: [],
            centerRows: [],
            bottomRows: []
        };
        // Expansion should still refetch server-side data, but it should not force the viewport back to the top.
        expansionScrollRestoreRef.current = null;
        if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
            cancelAnimationFrame(expansionScrollRestoreRafRef.current);
            expansionScrollRestoreRafRef.current = null;
        }
        if (parentRef.current) {
            parentRef.current.scrollTop = 0;
        }
    }, [serverSide, serverSideViewportResetKey, parentRef]);

    useLayoutEffect(() => {
        if (!serverSide || expansionScrollRestoreRef.current === null || !parentRef.current) return;
        if (!hasRenderedData && centerRows.length === 0 && topRows.length === 0 && bottomRows.length === 0) return;

        const restoreTarget = expansionScrollRestoreRef.current;
        if (!restoreTarget) return;

        const applyScrollRestore = () => {
            if (!parentRef.current || !expansionScrollRestoreRef.current) return;

            const nextTarget = expansionScrollRestoreRef.current;
            const targetScrollTop = nextTarget.scrollTop;
            if (rowVirtualizer.scrollToOffset) {
                rowVirtualizer.scrollToOffset(targetScrollTop);
            }
            if (Math.abs(parentRef.current.scrollTop - targetScrollTop) > 1) {
                parentRef.current.scrollTop = targetScrollTop;
            }

            if (nextTarget.restorePassesRemaining <= 1) {
                expansionScrollRestoreRef.current = null;
                expansionScrollRestoreRafRef.current = null;
                return;
            }

            expansionScrollRestoreRef.current = {
                ...nextTarget,
                restorePassesRemaining: nextTarget.restorePassesRemaining - 1
            };

            if (typeof requestAnimationFrame === 'function') {
                expansionScrollRestoreRafRef.current = requestAnimationFrame(applyScrollRestore);
            } else {
                applyScrollRestore();
            }
        };

        applyScrollRestore();

        return () => {
            if (expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
                cancelAnimationFrame(expansionScrollRestoreRafRef.current);
                expansionScrollRestoreRafRef.current = null;
            }
        };
    }, [serverSide, hasRenderedData, centerRows.length, topRows.length, bottomRows.length, renderedOffset, dataVersion, rowVirtualizer]);

    const effectiveTopRows = (serverSide && hasRenderedData && topRows.length === 0 && centerRows.length === 0)
        ? lastStableRowModelRef.current.topRows
        : topRows;
    const effectiveCenterRows = (serverSide && hasRenderedData && centerRows.length === 0)
        ? lastStableRowModelRef.current.centerRows
        : centerRows;
    const effectiveBottomRows = (serverSide && hasRenderedData && centerRows.length === 0 && bottomRows.length === 0)
        ? lastStableRowModelRef.current.bottomRows
        : bottomRows;
    const rowModelLookup = useMemo(() => {
        const lookup = new Map();
        [...effectiveTopRows, ...effectiveCenterRows, ...effectiveBottomRows].forEach(row => {
            if (row && row.id) {
                lookup.set(row.id, row);
            }
        });
        return lookup;
    }, [effectiveTopRows, effectiveCenterRows, effectiveBottomRows]);

    // Progressive subtree expansion: each time the backend returns new data,
    // scan it for descendants of the target path that still have children and
    // haven't been expanded yet. Auto-expand them and let the cycle continue
    // until every reachable descendant is expanded.
    useEffect(() => {
        if (!serverSide || !data || !subtreeExpandRef.current) return;
        const { path, expandedPaths } = subtreeExpandRef.current;

        const toExpand = data.filter(row => {
            if (!row || !row._path || !row._has_children) return false;
            const inSubtree = row._path === path || row._path.startsWith(path + '|||');
            return inSubtree && !expandedPaths.has(row._path);
        });

        if (toExpand.length === 0) {
            subtreeExpandRef.current = null;
            return;
        }

        toExpand.forEach(row => expandedPaths.add(row._path));

        captureExpansionScrollPosition();
        clearCache();
        setExpanded(prev => {
            const base = (prev !== null && typeof prev === 'object') ? prev : {};
            const next = { ...base };
            toExpand.forEach(row => { next[row._path] = true; });
            return next;
        });
    }, [data, serverSide]); // intentionally omits captureExpansionScrollPosition/clearCache/setExpanded — stable refs

    // Deferred block invalidation after single-row expansion.
    // We wait for the expansion response to land (dataVersion bumps) before
    // invalidating subsequent blocks. This ensures only ONE backend request fires
    // (the expanded sync), with no concurrent viewport request to race against it.
    // After the anchor block is updated with fresh data, blocks beyond it are
    // deleted so they get re-fetched on next scroll (their row indices shifted).
    useEffect(() => {
        if (!serverSide || !pendingExpansionRef.current) return;
        const { anchorBlock, expandedRowVirtualIndex, oldRowCount, extendedToBlock = -1 } = pendingExpansionRef.current;
        pendingExpansionRef.current = null;
        if (anchorBlock < 0) {
            // The expanded row was not in the viewport when the user toggled it.
            // We can't know which anchor block shifted, but a hard clear causes
            // a full skeleton flash.  Soft-invalidate all blocks from 0 instead
            // so existing rows stay visible (stale-while-revalidate) until fresh
            // data lands (finding #6).
            if (softInvalidateFromBlock) softInvalidateFromBlock(0);
        } else {
            // The expansion request was extended to cover through extendedToBlock
            // (anchorBlock + 1 when the anchor is known).  Those blocks were filled
            // with fresh data by the data-sync effect, so we must NOT re-dirty them.
            // Start soft-invalidating from the first block BEYOND the fresh coverage.
            const firstStaleBlock = extendedToBlock >= 0 ? extendedToBlock + 1 : anchorBlock + 1;
            if (softInvalidateFromBlock) softInvalidateFromBlock(firstStaleBlock);
        }
        // Clear the transition loader now that the expansion response has landed.
        setPendingRowTransitions(new Map());

        // Viewport anchor preservation.
        // When rows are inserted or removed ABOVE the current scroll position, the
        // virtualizer re-layouts and the same pixel offset now shows a different
        // logical row. Compensate by shifting scrollTop so that the user continues
        // to see the same rows they were looking at before the toggle.
        if (
            parentRef.current &&
            typeof expandedRowVirtualIndex === 'number' &&
            typeof oldRowCount === 'number' &&
            rowHeight > 0
        ) {
            const rowDelta = (rowCount || 0) - (oldRowCount || 0);
            if (rowDelta !== 0) {
                // Y position of the expanded/collapsed row (uniform row heights).
                const expandedRowY = expandedRowVirtualIndex * rowHeight;
                const currentScrollTop = parentRef.current.scrollTop;
                // Only compensate when the anchor row is entirely ABOVE the viewport.
                // If it is at or inside the viewport the inserted children appear
                // naturally below it and no scroll adjustment is needed.
                if (expandedRowY + rowHeight <= currentScrollTop) {
                    const newScrollTop = Math.max(0, currentScrollTop + rowDelta * rowHeight);
                    parentRef.current.scrollTop = newScrollTop;
                    if (rowVirtualizer.scrollToOffset) {
                        rowVirtualizer.scrollToOffset(newScrollTop);
                    }
                }
            }
        }
    }, [dataVersion, serverSide, rowCount, rowHeight, parentRef, rowVirtualizer]); // fires when expansion response arrives

    // Debug effect removed (finding #10 — hot-path logging).

    const visibleLeafColumns = table.getVisibleLeafColumns();

    // 1. Row Virtualizer (Managed by useServerSideRowModel)
    const virtualRows = rowVirtualizer.getVirtualItems();
    const showColumnLoadingSkeletons = serverSide && (structuralInFlight || isRequestPending) && pendingColumnSkeletonCount > 0;
    const columnSkeletonWidth = defaultColumnWidths.schemaFallback;
    const stickyHeaderHeight = getStickyHeaderHeight(table.getHeaderGroups().length, rowHeight, showFloatingFilters);
    const bodyRowsTopOffset = stickyHeaderHeight + (effectiveTopRows.length * rowHeight);

    useEffect(() => {
        if (!serverSide || virtualRows.length === 0) return;
        const firstRow = virtualRows[0].index;
        const lastRow = virtualRows[virtualRows.length - 1].index;
        latestViewportRef.current = {
            start: firstRow,
            end: lastRow,
            count: Math.max(1, lastRow - firstRow + 1)
        };
    }, [serverSide, virtualRows]);

    // 2. Column Virtualizer (Extracted)
    const {
        columnVirtualizer,
        virtualCenterCols,
        beforeWidth,
        afterWidth,
        totalLayoutWidth,
        leftCols,
        rightCols,
        centerCols
    } = useColumnVirtualizer({
        parentRef,
        table
    });

    useLayoutEffect(() => {
        if (!parentRef.current || !columnVirtualizer) return;
        // Keep horizontal virtualization in sync after pivot group collapse/expand.
        // Without this clamp, right-edge stale indices can point past center columns
        // and render blank numeric cells.
        const scrollEl = parentRef.current;
        const maxScrollLeft = Math.max(0, scrollEl.scrollWidth - scrollEl.clientWidth);
        if (scrollEl.scrollLeft > maxScrollLeft) {
            // Clamp the DOM scroll position.
            scrollEl.scrollLeft = maxScrollLeft;
            // Synchronously update the virtualizer's internal scrollOffset BEFORE
            // calling measure(). measure() calls notify() which triggers a React
            // re-render; if scrollOffset is still the old out-of-bounds value at
            // that point the re-render will show only the last column (blank
            // numbers in all others) until the async DOM scroll event fires.
            // Direct mutation is safe here — scrollOffset is a plain instance
            // property that getScrollOffset() reads directly.
            columnVirtualizer.scrollOffset = maxScrollLeft;
        }
        columnVirtualizer.measure();
    }, [columnVirtualizer, centerCols.length, totalLayoutWidth]);

    // Memoized lookup structures for the header render path.
    // centerColIndexMap: O(1) id→index lookup; only rebuilt when the column list changes.
    // visibleLeafIndexSet: O(1) membership check; only rebuilt when the virtual window shifts.
    const centerColIndexMap = useMemo(
        () => new Map(centerCols.map((c, i) => [c.id, i])),
        [centerCols]
    );
    const visibleLeafIndexSet = useMemo(
        () => new Set(virtualCenterCols.map(v => v.index)),
        [virtualCenterCols]
    );

    // O(1) colId → visible-leaf-index map for renderCell; rebuilt only when column list changes.
    const visibleLeafColIndexMap = useMemo(
        () => new Map(table.getVisibleLeafColumns().map((c, i) => [c.id, i])),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [table.getVisibleLeafColumns()]
    );

    // Sync the column virtualizer's visible range into state so useServerSideRowModel
    // can detect column window changes and trigger re-fetches.
    useEffect(() => {
        if (virtualCenterCols.length === 0) return;
        const newStart = virtualCenterCols[0].index;
        const newEnd = virtualCenterCols[virtualCenterCols.length - 1].index;
        setVisibleColRange(prev => {
            if (prev.start === newStart && prev.end === newEnd) return prev;
            return { start: newStart, end: newEnd };
        });
    }, [virtualCenterCols]);

    // Use the custom hook
    const { getHeaderStickyStyle, getStickyStyle } = useStickyStyles(
        theme,
        leftCols,
        rightCols
    );


    const getFieldZone = (id) => {
        if (id === 'hierarchy') return 'rows';
        if (rowFields.includes(id)) return 'rows';
        if (colFields.includes(id)) return 'cols';
        if (valConfigs.find(v => id.includes(v.field))) return 'vals';
        return null;
    };

    const getFieldIndex = (id, zone) => {
        if (zone === 'rows') return rowFields.indexOf(id);
        if (zone === 'cols') return colFields.indexOf(id);
        if (zone === 'vals') return valConfigs.findIndex(v => id.includes(v.field));
        return -1;
    };

    const onHeaderDrop = (e, targetColId) => {
        e.preventDefault();
        if (!dragItem) return;
        const { field, zone: srcZone } = dragItem;
        const fieldName = typeof field === 'string' ? field : field.field;
        
        // Handle dropping on the same column (no-op)
        if (fieldName === targetColId) {
            setDragItem(null);
            return;
        }

        const targetZone = getFieldZone(targetColId);
        if (!targetZone) {
            setDragItem(null);
            return;
        }

        // Check for Pinning (Drag-to-Pin)
        const targetIsPinned = getPinningState(targetColId);
        if (targetIsPinned) {
             handlePinColumn(fieldName, targetIsPinned);
        } else {
             // If dropping on unpinned, maybe unpin?
             handlePinColumn(fieldName, false);
        }

        // Reordering or Pivoting
        if (srcZone === targetZone) {
             const srcIdx = getFieldIndex(fieldName, srcZone);
             const targetIdx = getFieldIndex(targetColId, targetZone);
             if (srcIdx !== -1 && targetIdx !== -1 && srcIdx !== targetIdx) {
                 const move = (list, setList) => {
                    const n = [...list]; 
                    const [moved] = n.splice(srcIdx, 1);
                    n.splice(targetIdx, 0, moved); 
                    setList(n);
                };
                if (srcZone==='rows') move(rowFields, setRowFields);
                if (srcZone==='cols') move(colFields, setColFields);
                if (srcZone==='vals') move(valConfigs, setValConfigs);
             }
        } else {
            // Pivoting (Moving between zones)
            const targetIdx = getFieldIndex(targetColId, targetZone);
            // Remove from source
            if (srcZone==='rows') setRowFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='cols') setColFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='vals') setValConfigs(p=>p.filter(f=>f.field!==fieldName));

            // Insert into target
            const insert = (list, setList, item) => {
                const n = [...list];
                n.splice(targetIdx, 0, item);
                setList(n);
            };
            if (targetZone==='rows') insert(rowFields, setRowFields, fieldName);
            if (targetZone==='cols') insert(colFields, setColFields, fieldName);
            if (targetZone==='vals') insert(valConfigs, setValConfigs, {field: fieldName, agg:'sum'});
        }
        setDragItem(null);
    };

    const onDragStart = (e, field, zone, idx) => {
        setDragItem({ field, zone, idx });
        e.dataTransfer.effectAllowed = 'move';
    };
    const onDragOver = (e, zone, idx) => {
        e.preventDefault();
        // If hovering over a header, zone might be 'cols' or 'rows' derived from ID
        // For sidebar, we use dropLine logic
        if (['rows', 'cols', 'vals', 'filter'].includes(zone)) {
            const rect = e.currentTarget.getBoundingClientRect();
            const mid = rect.top + rect.height / 2;
            setDropLine({ zone, idx: e.clientY > mid ? idx + 1 : idx });
        }
    };
    const onDrop = (e, targetZone) => {
        e.preventDefault();
        if (!dragItem) return;
        const { field, zone: srcZone, idx: srcIdx } = dragItem;
        const targetIdx = (dropLine && dropLine.idx) || 0;
        const insertItem = (list, idx, item) => { const n = [...list]; n.splice(idx, 0, item); return n; };
        const fieldName = typeof field === 'string' ? field : field.field;
        if (!fieldName || typeof fieldName !== 'string') { setDragItem(null); setDropLine(null); return; }
        if (srcZone !== targetZone) {
            if (srcZone==='rows') setRowFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='cols') setColFields(p=>p.filter(f=>f!==fieldName));
            if (srcZone==='vals') setValConfigs(p=>p.filter((_,i)=>i!==srcIdx));
            if (targetZone==='rows') setRowFields(p=>p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName));
            if (targetZone==='cols') setColFields(p=>p.includes(fieldName) ? p : insertItem(p, targetIdx, fieldName));
            if (targetZone==='vals') setValConfigs(p=>insertItem(p, targetIdx, {field: fieldName, agg:'sum'}));
            if (targetZone==='filter' && !filters.hasOwnProperty(fieldName)) setFilters(p=>({...p, [fieldName]: ''}));
        } else {
            const move = (list, setList) => {
                const n = [...list]; const [moved] = n.splice(srcIdx, 1);
                let ins = targetIdx; if (srcIdx < targetIdx) ins -= 1;
                n.splice(ins, 0, moved); setList(n);
            };
            if (targetZone==='rows') move(rowFields, setRowFields);
            if (targetZone==='cols') move(colFields, setColFields);
            if (targetZone==='vals') move(valConfigs, setValConfigs);
        }
        setDragItem(null); setDropLine(null);
    };





    const buildExportAoa = (allRows) => {
        // Use table.getHeaderGroups() so we get the real multi-level header structure
        // with correct parent/child relationships and colSpans set by TanStack.
        const headerGroups = table.getHeaderGroups();

        // Identify leaf (data) columns from the last header group, excluding
        // internal/UI-only columns that should not appear in the export.
        const SKIP_COL_IDS = new Set(['__row_number__']);
        const leafHeaders = (headerGroups[headerGroups.length - 1]?.headers ?? [])
            .filter(h => !SKIP_COL_IDS.has(h.column.id) && !h.isPlaceholder);

        const leafCount = leafHeaders.length;

        // Build one AOA row per header group.
        // For each header row, we fill a flat array of length leafCount.
        // A header with colSpan > 1 occupies that many leaf slots; placeholders fill gaps.
        const headerAoaRows = [];
        const allMerges = [];

        headerGroups.forEach((hg, rowIdx) => {
            const aoaRow = new Array(leafCount).fill('');
            let leafPos = 0;
            hg.headers.forEach(h => {
                if (SKIP_COL_IDS.has(h.column.id)) return;
                const span = h.colSpan ?? 1;
                if (!h.isPlaceholder) {
                    // Resolve header text — prefer columnDef.header string, fall back to id
                    const colDef = h.column.columnDef;
                    let headerText = '';
                    if (typeof colDef.header === 'string') {
                        headerText = colDef.header;
                    } else if (typeof h.column.id === 'string') {
                        // Strip group_ prefix and internal path separators for cleaner output
                        headerText = h.column.id
                            .replace(/^group_/, '')
                            .replace(/\|\|\|/g, ' > ');
                    }
                    aoaRow[leafPos] = headerText;
                    if (span > 1 && rowIdx < headerGroups.length - 1) {
                        // Merge across the span; row 0-indexed in the final aoa
                        allMerges.push({ s: { r: rowIdx, c: leafPos }, e: { r: rowIdx, c: leafPos + span - 1 } });
                    }
                }
                leafPos += span;
            });
            headerAoaRows.push(aoaRow);
        });

        // If there is only one header group and it looks identical to itself
        // (no real parent grouping), just keep one header row to avoid duplication.
        const dedupedHeaderRows = headerAoaRows.length > 1
            ? headerAoaRows
            : headerAoaRows;  // keep as-is for single group (flat table)

        // Build data rows — include ALL rows (totals + data rows)
        // Track max content width per column for auto-sizing.
        const colWidths = leafHeaders.map(h => {
            const colDef = h.column.columnDef;
            return typeof colDef.header === 'string' ? colDef.header.length : (h.column.id ?? '').length;
        });

        const dataRows = allRows.map(r => {
            return leafHeaders.map((h, ci) => {
                const col = h.column;
                const colId = col.id;
                const colDef = col.columnDef;

                let val;
                if (colId === 'hierarchy') {
                    // Hierarchy column: indent using spaces to reflect depth
                    const depth = r.original?.depth ?? r.depth ?? 0;
                    const label = r.original?._isTotal ? (r.original?._id ?? 'Total') : (r.original?._id ?? '');
                    val = '\u00A0\u00A0'.repeat(depth) + label;  // non-breaking spaces for Excel visibility
                } else if (typeof colDef.accessorFn === 'function') {
                    // Use accessorFn to get the value (same as TanStack does internally)
                    val = colDef.accessorFn(r.original, r.index);
                } else if (colDef.accessorKey) {
                    val = r.original?.[colDef.accessorKey];
                } else {
                    val = '';
                }

                // Normalize: undefined/null → empty string; keep numbers as numbers
                if (val === undefined || val === null) val = '';

                // Track max width for column auto-sizing
                const cellLen = String(val).length;
                if (cellLen > colWidths[ci]) colWidths[ci] = cellLen;

                return val;
            });
        });

        // Build ws['!cols'] — cap at 60 chars to avoid overly wide columns
        const wsCols = colWidths.map(w => ({ wch: Math.min(Math.max(w + 2, 8), 60) }));

        return {
            aoa: [...dedupedHeaderRows, ...dataRows],
            merges: allMerges,
            wsCols,
            headerRowCount: dedupedHeaderRows.length,
        };
    };

    const fetchDrillData = useCallback(async (rowPath, page = 0, sortCol = null, sortDir = 'asc', filterText = '') => {
        const params = new URLSearchParams({
            table: tableName,
            row_path: rowPath,
            row_fields: rowFields.join(','),
            page: String(page),
            page_size: '100',
        });
        if (sortCol) { params.set('sort_col', sortCol); params.set('sort_dir', sortDir); }
        if (filterText) params.set('filter', filterText);

        setDrillModal(prev => ({ ...(prev || { path: rowPath, rows: [], page: 0, totalRows: 0, sortCol, sortDir, filterText }), loading: true }));
        try {
            const resp = await fetch(`${drillEndpoint}?${params.toString()}`);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const json = await resp.json();
            setDrillModal({ loading: false, path: rowPath, rows: json.rows || [], page: json.page || 0, totalRows: json.total_rows || 0, sortCol, sortDir, filterText });
        } catch (err) {
            console.error('Drill-through fetch failed:', err);
            setDrillModal(null);
        }
    }, [tableName, rowFields, drillEndpoint]);

    const handleCellDrillThrough = useCallback((row, colId) => {
        if (!row || !row.original) return;
        const rowPath = row.original._path;
        if (!rowPath || rowPath === '__grand_total__') return;  // skip total rows
        fetchDrillData(rowPath, 0, null, 'asc', '');
    }, [fetchDrillData]);

    const exportPivot = useCallback(() => {
        const XLSX_LIMIT = 500000;
        const allRows = table.getRowModel().rows;  // full row model, not just virtual window

        const isCSV = (rowCount || 0) > XLSX_LIMIT;

        if (isCSV) {
            // CSV path — flat, no merge support needed
            // Use TanStack visible leaf columns so we match what's shown on screen,
            // and skip internal UI-only columns.
            const SKIP_CSV = new Set(['__row_number__']);
            const leafCols = table.getVisibleLeafColumns().filter(c => !SKIP_CSV.has(c.id));

            const escape = (v) => {
                if (v == null) return '';
                const s = String(v);
                return (s.includes(',') || s.includes('"') || s.includes('\n'))
                    ? `"${s.replace(/"/g, '""')}"` : s;
            };
            const header = leafCols.map(c => {
                const h = c.columnDef?.header;
                return escape(typeof h === 'string' ? h : (c.id ?? ''));
            }).join(',');
            const lines = allRows.map(r =>
                leafCols.map(c => {
                    if (c.id === 'hierarchy') {
                        const depth = r.original?.depth ?? r.depth ?? 0;
                        return escape('  '.repeat(depth) + (r.original?._id ?? ''));
                    }
                    const val = typeof c.columnDef?.accessorFn === 'function'
                        ? c.columnDef.accessorFn(r.original, r.index)
                        : (c.columnDef?.accessorKey ? r.original?.[c.columnDef.accessorKey] : '');
                    return escape(val ?? '');
                }).join(',')
            );
            const blob = new Blob([[header, ...lines].join('\n')], { type: 'text/csv;charset=utf-8;' });
            saveAs(blob, 'pivot.csv');
        } else {
            // XLSX path — multi-level headers + hierarchy indent
            const { aoa, merges, wsCols } = buildExportAoa(allRows);
            const ws = XLSX.utils.aoa_to_sheet(aoa);
            if (merges.length > 0) ws['!merges'] = merges;
            if (wsCols && wsCols.length > 0) ws['!cols'] = wsCols;
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, 'Pivot');
            const buf = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
            saveAs(new Blob([buf], { type: 'application/octet-stream' }), 'pivot.xlsx');
        }
    }, [rows, columns, rowCount, table]);

    const { renderCell, renderHeaderCell } = useRenderHelpers({
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
        // renderHeaderCell dependencies (synchronous / render-time reads)
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
    });

    const srOnly = {
        position: 'absolute',
        width: '1px',
        height: '1px',
        padding: 0,
        margin: '-1px',
        overflow: 'hidden',
        clip: 'rect(0, 0, 0, 0)',
        whiteSpace: 'nowrap',
        border: 0,
        pointerEvents: 'none'
    };

    // Add focus styles to interactive elements
    const focusStyle = {
        outline: `2px solid ${theme.primary}`,
        outlineOffset: '2px'
    };

    return (
        <div id={id} style={{ ...styles.root, ...loadingCssVars, ...style }}>
            <style>{loadingAnimationStyles}</style>
            <div style={srOnly} role="status" aria-live="polite">{announcement}</div>
            {/* PivotAppBar is intentionally outside PivotErrorBoundary so that
                the global search input (and other toolbar controls) are not
                unmounted on every dataVersion change. */}
            <PivotAppBar
                sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}
                themeName={themeName} setThemeName={setThemeName}
                showRowNumbers={showRowNumbers} setShowRowNumbers={setShowRowNumbers}
                showFloatingFilters={showFloatingFilters} setShowFloatingFilters={setShowFloatingFilters}
                showRowTotals={showRowTotals} setShowRowTotals={setShowRowTotals}
                showColTotals={showColTotals} setShowColTotals={setShowColTotals}
                spacingMode={spacingMode} setSpacingMode={setSpacingMode} spacingLabels={spacingLabels}
                layoutMode={layoutMode} setLayoutMode={setLayoutMode}
                colorScaleMode={colorScaleMode} setColorScaleMode={setColorScaleMode}
                colorPalette={colorPalette} setColorPalette={setColorPalette}
                rowCount={rowCount} exportPivot={exportPivot}
                theme={theme} styles={styles}
                filters={filters} setFilters={setFilters}
                onSaveView={handleSaveView}
                pivotTitle={props.pivotTitle}
                fontFamily={fontFamily} setFontFamily={setFontFamily}
                fontSize={fontSize} setFontSize={setFontSize}
                displayDecimal={displayDecimal} onDecimalChange={handleDecimalChange}
                hasSelection={Object.keys(selectedCells).length > 0}
                cellFormatRules={cellFormatRules} setCellFormatRules={setCellFormatRules}
                selectedCellKeys={selectedCellKeys}
                dataBarsColumns={dataBarsColumns} setDataBarsColumns={setDataBarsColumns}
                selectedCellColIds={selectedCellColIds}
            />
        <PivotErrorBoundary key={dataVersion}>
            <div style={{display:'flex', flex:1, overflow:'hidden', fontFamily: fontFamily, fontSize: fontSize}}>
                {sidebarOpen && (
                    <SidebarPanel
                        sidebarTab={sidebarTab} setSidebarTab={setSidebarTab}
                        rowFields={rowFields} setRowFields={setRowFields}
                        colFields={colFields} setColFields={setColFields}
                        valConfigs={valConfigs} setValConfigs={setValConfigs}
                        filters={filters} setFilters={setFilters}
                        columnVisibility={columnVisibility} setColumnVisibility={setColumnVisibility}
                        columnPinning={columnPinning} setColumnPinning={setColumnPinning}
                        availableFields={availableFields}
                        table={table}
                        pinningPresets={pinningPresets}
                        theme={theme} styles={styles}
                        showNotification={showNotification}
                        filterAnchorEl={filterAnchorEl} setFilterAnchorEl={setFilterAnchorEl}
                        colSearch={colSearch} setColSearch={setColSearch}
                        colTypeFilter={colTypeFilter} setColTypeFilter={setColTypeFilter}
                        selectedCols={selectedCols} setSelectedCols={setSelectedCols}
                        dropLine={dropLine}
                        onDragStart={onDragStart} onDragOver={onDragOver} onDrop={onDrop}
                        handleHeaderFilter={handleHeaderFilter}
                        handleFilterClick={handleFilterClick}
                        handleExpandAllRows={handleExpandAllRows}
                        handlePinColumn={handlePinColumn}
                        toggleAllColumnsPinned={toggleAllColumnsPinned}
                        activeFilterCol={activeFilterCol}
                        closeFilterPopover={closeFilterPopover}
                        filterOptions={filterOptions}
                        data={data}
                    />
                )}
                <PivotTableBody
                    parentRef={parentRef}
                    handleKeyDown={handleKeyDown}
                    rows={rows}
                    visibleLeafColumns={visibleLeafColumns}
                    totalLayoutWidth={totalLayoutWidth}
                    beforeWidth={beforeWidth}
                    afterWidth={afterWidth}
                    bodyRowsTopOffset={bodyRowsTopOffset}
                    stickyHeaderHeight={stickyHeaderHeight}
                    effectiveTopRows={effectiveTopRows}
                    effectiveBottomRows={effectiveBottomRows}
                    effectiveCenterRows={effectiveCenterRows}
                    virtualRows={virtualRows}
                    virtualCenterCols={virtualCenterCols}
                    rowVirtualizer={rowVirtualizer}
                    rowModelLookup={rowModelLookup}
                    getRow={getRow}
                    serverSide={serverSide}
                    serverSidePinsGrandTotal={serverSidePinsGrandTotal}
                    pendingRowTransitions={pendingRowTransitions}
                    table={table}
                    leftCols={leftCols}
                    centerCols={centerCols}
                    rightCols={rightCols}
                    centerColIndexMap={centerColIndexMap}
                    visibleLeafIndexSet={visibleLeafIndexSet}
                    rowHeight={rowHeight}
                    showFloatingFilters={showFloatingFilters}
                    showColTotals={showColTotals}
                    grandTotalPosition={grandTotalPosition}
                    showColumnLoadingSkeletons={showColumnLoadingSkeletons}
                    pendingColumnSkeletonCount={pendingColumnSkeletonCount}
                    columnSkeletonWidth={columnSkeletonWidth}
                    theme={theme}
                    styles={styles}
                    renderCell={renderCell}
                    renderHeaderCell={renderHeaderCell}
                    filters={filters}
                    handleHeaderFilter={handleHeaderFilter}
                    selectedCells={selectedCells}
                    rowCount={statusRowCount}
                    isRequestPending={isRequestPending}
                />
            </div>
            {contextMenu && <ContextMenu {...contextMenu} onClose={() => setContextMenu(null)} />}
            {notification && <Notification message={notification.message} type={notification.type} onClose={() => setNotification(null)} />}
            <DrillThroughModal
                drillState={drillModal}
                onClose={() => setDrillModal(null)}
                onPageChange={(newPage) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, newPage, drillModal.sortCol, drillModal.sortDir, drillModal.filterText);
                }}
                onSort={(col, dir) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, 0, col, dir, drillModal.filterText);
                }}
                onFilter={(text) => {
                    if (!drillModal) return;
                    fetchDrillData(drillModal.path, 0, drillModal.sortCol, drillModal.sortDir, text);
                }}
            />
        </PivotErrorBoundary>
        </div>
    );
};

DashTanstackPivot.propTypes = {
    id: PropTypes.string,
    table: PropTypes.string,
    pivotTitle: PropTypes.string,
        data: PropTypes.arrayOf(PropTypes.object),
        setProps: PropTypes.func,
        style: PropTypes.object,
        serverSide: PropTypes.bool,
        rowCount: PropTypes.number,
        rowFields: PropTypes.array,
        colFields: PropTypes.array,
        valConfigs: PropTypes.array,
        filters: PropTypes.object,
        sorting: PropTypes.array,
        expanded: PropTypes.oneOfType([PropTypes.object, PropTypes.bool]),
        columns: PropTypes.array,
    
    showRowTotals: PropTypes.bool,
    showColTotals: PropTypes.bool,
    grandTotalPosition: PropTypes.oneOf(['top', 'bottom']),
    filterOptions: PropTypes.object,
    viewport: PropTypes.object,
    cellUpdate: PropTypes.object,
    cellUpdates: PropTypes.arrayOf(PropTypes.object),
    rowMove: PropTypes.object,
    drillThrough: PropTypes.object,
    drillEndpoint: PropTypes.string,
    viewState: PropTypes.object,
    savedView: PropTypes.object,
    conditionalFormatting: PropTypes.arrayOf(PropTypes.object),
    validationRules: PropTypes.object,
    columnPinning: PropTypes.shape({
        left: PropTypes.arrayOf(PropTypes.string),
        right: PropTypes.arrayOf(PropTypes.string)
    }),
    rowPinning: PropTypes.shape({
        top: PropTypes.arrayOf(PropTypes.string),
        bottom: PropTypes.arrayOf(PropTypes.string)
    }),
    columnPinned: PropTypes.object,
    rowPinned: PropTypes.object,
    columnVisibility: PropTypes.object,
    columnSizing: PropTypes.object,
    reset: PropTypes.any,
    persistence: PropTypes.oneOfType([PropTypes.bool, PropTypes.string, PropTypes.number]),
    persistence_type: PropTypes.oneOf(['local', 'session', 'memory']),
    pinningOptions: PropTypes.shape({
        maxPinnedLeft: PropTypes.number,
        maxPinnedRight: PropTypes.number,
        suppressMovable: PropTypes.bool,
        lockPinned: PropTypes.bool
    }),
    pinningPresets: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string,
        config: PropTypes.object
    })),
    sortOptions: PropTypes.shape({
        naturalSort: PropTypes.bool,
        caseSensitive: PropTypes.bool,
        columnOptions: PropTypes.object
    }),
    sortLock: PropTypes.bool,
    sortEvent: PropTypes.object,
    availableFieldList: PropTypes.arrayOf(PropTypes.string),
    dataOffset: PropTypes.number,
    dataVersion: PropTypes.number,
};


// @ts-nocheck -- composed leaf hooks are progressively typed at their owners.
// Viewport-stage composition. Focused sub-hooks own individual state groups.
import React, { useMemo, useState, useRef, useEffect, useLayoutEffect, useCallback } from 'react';
import { useReactTable, getCoreRowModel, getExpandedRowModel, getGroupedRowModel, getPaginationRowModel } from '@tanstack/react-table';
import { useVirtualizer } from '@tanstack/react-virtual';
import { buildTsvFromRawRows } from '../utils/rawTableTsv';
import { areStringArraysEqual, collectColumnDefLeafIds, mergeColumnOrderWithIds } from '../utils/columnOrder';
import { compareFilterOptionValues } from '../utils/filterOptions';
import Icons from '../utils/Icons';
import { MAX_AUTO_SIZE_SAMPLE_ROWS, MAX_PENDING_ROW_TRANSITIONS, GRAND_TOTAL_ROW_ID, resolveTransportRowId, normalizeRowPinningState, getPinnedSideForRow } from '../hooks/usePivotNormalization';
const DEFAULT_SERVER_SIDE_BLOCK_SIZE = 48;
const EXPAND_ALL_STATE_KEY = '__ALL__';
const normalizeLayoutModeValue = value => {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'outline' || normalized === 'tabular' ? normalized : 'hierarchy';
};
const debugLog = (...args) => {
  const buildDebugEnabled = process.env.NODE_ENV !== 'production';
  let runtimeDebugEnabled = false;
  if (typeof window !== 'undefined') {
    try {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true || window.localStorage.getItem('pivot-debug') === '1';
    } catch (error) {
      runtimeDebugEnabled = window.__PIVOT_DEBUG__ === true;
    }
  }
  if (!buildDebugEnabled && !runtimeDebugEnabled) return;
  console.log('[pivot-grid]', ...args);
};
import useStickyStyles from '../hooks/useStickyStyles';
import { useServerSideRowModel } from '../hooks/useServerSideRowModel';
import { useColumnVirtualizer } from '../hooks/useColumnVirtualizer';
import { getAllLeafIdsFromColumn, isGroupColumn } from '../utils/helpers';
import { useColumnDefs } from '../hooks/useColumnDefs';
import { normalizeSortingState, updateSortingForColumn } from '../utils/sorting';
import { useRuntimeChartModels } from '../runtime/useRuntimeChartModels';
import { useRuntimeColumnAutoSizing } from '../runtime/useRuntimeColumnAutoSizing';
import { useRuntimeDataRetry } from './useRuntimeDataRetry';
import { useRuntimeFilterPopover } from '../runtime/useRuntimeFilterPopover';
import { useRuntimeColorScalePresentation } from '../runtime/useRuntimeColorScalePresentation';
import { useRuntimeViewportTableData } from '../runtime/useRuntimeViewportTableData';
import { useRuntimeCellSelection } from '../runtime/useRuntimeCellSelection';
import { useRuntimeClipboardPaste } from '../runtime/useRuntimeClipboardPaste';
import { useRuntimeClipboardExportPresentation } from '../runtime/useRuntimeClipboardExportPresentation';
import { useRuntimeColumnCommands } from '../runtime/useRuntimeColumnCommands';
import { useRuntimeColumnMeasurement } from '../runtime/useRuntimeColumnMeasurement';
import { useRuntimeSelectionFormatting } from '../runtime/useRuntimeSelectionFormatting';
import { useRuntimeViewSnapshot } from '../runtime/useRuntimeViewSnapshot';
import { useRuntimeDisplayNavigation } from '../runtime/useRuntimeDisplayNavigation';
import { getDisplayOrderedLeafColumns } from './displayColumnOrder';
import { useRuntimeViewRestoreScroll } from '../runtime/useRuntimeViewRestoreScroll';
import { createRuntimeExportCommands } from '../runtime/createRuntimeExportCommands';
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartModal
})));
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ '../components/Charts/PivotCharts').then(module => ({
  default: module.PivotChartPanel
})));
const PivotChartFallback = () => null;
const PivotChartModal = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartModal {...props} />
    </React.Suspense>;
const PivotChartPanel = props => <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartPanel {...props} />
    </React.Suspense>;
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
export function useViewportRuntimeStage(input, transport) {
  // Viewport-only assembly must not mutate the command domain. Its local
  // work is confined to this render pass and the exported snapshot is frozen.
  const stage = { ...input };
  useEffect(() => {
    stage.setColumnPinning(prev => {
      let nextLeft = [...(prev.left || [])];
      let changed = false;
      const shouldPinHierarchy = stage.layoutMode === 'hierarchy' && stage.rowFields.length > 0 || stage.pivotMode === 'report';
      const shouldShowReportConfigGutter = stage.pivotMode === 'report' && stage.showReportConfigColumn !== false && !stage.immersiveMode;

      // 1. Enforce Hierarchy Pinning
      if (shouldPinHierarchy) {
        if (!nextLeft.includes('hierarchy')) {
          nextLeft = ['hierarchy', ...nextLeft];
          changed = true;
        }
      }

      // 2. Keep report row settings in a detached gutter immediately left of the hierarchy.
      if (shouldShowReportConfigGutter) {
        if (!nextLeft.includes('__report_config__')) {
          const hierarchyIndex = nextLeft.indexOf('hierarchy');
          if (hierarchyIndex >= 0) {
            nextLeft.splice(hierarchyIndex, 0, '__report_config__');
          } else {
            nextLeft.push('__report_config__');
          }
          changed = true;
        }
        const reportConfigIndex = nextLeft.indexOf('__report_config__');
        const hierarchyIndex = nextLeft.indexOf('hierarchy');
        if (reportConfigIndex >= 0 && hierarchyIndex >= 0 && reportConfigIndex !== hierarchyIndex - 1) {
          nextLeft.splice(reportConfigIndex, 1);
          const nextHierarchyIndex = nextLeft.indexOf('hierarchy');
          nextLeft.splice(nextHierarchyIndex, 0, '__report_config__');
          changed = true;
        }
      } else if (nextLeft.includes('__report_config__')) {
        nextLeft = nextLeft.filter(id => id !== '__report_config__');
        changed = true;
      }

      // 3. Enforce Row Number Pinning (User Request: Always utmost left)
      if (stage.showRowNumbers) {
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
        return {
          ...prev,
          left: nextLeft
        };
      }
      return prev;
    });
  }, [stage.immersiveMode, stage.layoutMode, stage.pivotMode, stage.rowFields.length, stage.showReportConfigColumn, stage.showRowNumbers]);
  stage.applyHeaderSort = (colId, desc, sortMetadata = {}, append = false) => {
    if (!colId || colId === '__row_number__' || colId === '__report_config__') return;
    const nextSorting = updateSortingForColumn({
      sorting: stage.sorting,
      columnId: colId,
      desc,
      sortMetadata,
      append
    });
    stage.setSorting(nextSorting);
    // In server-side mode the sync effect below emits sorting together with
    // runtimeRequest. Sending sorting here would trigger a stale prop-refresh
    // backend request before the real sort request is queued.
    if (!stage.serverSide && stage.setPropsRef.current) {
      stage.setPropsRef.current({
        sorting: nextSorting,
        sortEvent: {
          type: 'change',
          status: 'applied',
          sorting: nextSorting,
          timestamp: Date.now()
        }
      });
    }
  };
  stage.handleHeaderContextMenu = (e, colId, header = null, level = 0) => {
    e.preventDefault();
    const menuX = e.clientX;
    const menuY = e.clientY;
    const actions = [];
    const column = stage.table.getColumn(colId);
    if (!column) {
      return;
    }
    const {
      left,
      right
    } = stage.columnPinning;

    // Determine if this is a group column
    const isGroup = isGroupColumn(column);

    // Only show sort/filter options for leaf columns
    if (!isGroup) {
      const hasOtherSorts = Array.isArray(stage.sorting) && stage.sorting.some(s => s && s.id !== colId);
      const existingSortIndex = Array.isArray(stage.sorting) ? stage.sorting.findIndex(s => s && s.id === colId) : -1;
      const existingSortPriority = existingSortIndex >= 0 ? existingSortIndex + 1 : null;
      const multiVerb = existingSortPriority ? `Update Priority ${existingSortPriority}` : 'Add';
      const absortMeta = {
        sortType: 'absolute',
        absoluteSort: true
      };
      const isSortedAsc = existingSortIndex >= 0 && !stage.sorting[existingSortIndex]?.desc;
      const isSortedDesc = existingSortIndex >= 0 && stage.sorting[existingSortIndex]?.desc;
      const sortChildren = [{
        label: (isSortedAsc ? '✓ ' : '') + 'Ascending',
        icon: <Icons.SortAsc />,
        onClick: () => stage.applyHeaderSort(colId, false)
      }, {
        label: (isSortedDesc ? '✓ ' : '') + 'Descending',
        icon: <Icons.SortDesc />,
        onClick: () => stage.applyHeaderSort(colId, true)
      }];
      if (hasOtherSorts) {
        sortChildren.push('separator');
        sortChildren.push({
          label: `${multiVerb} Ascending`,
          icon: <Icons.SortAsc />,
          onClick: () => stage.applyHeaderSort(colId, false, {}, true)
        });
        sortChildren.push({
          label: `${multiVerb} Descending`,
          icon: <Icons.SortDesc />,
          onClick: () => stage.applyHeaderSort(colId, true, {}, true)
        });
      }
      sortChildren.push('separator');
      sortChildren.push({
        label: 'Absolute Ascending',
        icon: <Icons.SortAsc />,
        onClick: () => stage.applyHeaderSort(colId, false, absortMeta)
      });
      sortChildren.push({
        label: 'Absolute Descending',
        icon: <Icons.SortDesc />,
        onClick: () => stage.applyHeaderSort(colId, true, absortMeta)
      });
      if (hasOtherSorts) {
        sortChildren.push({
          label: `${multiVerb} Absolute Asc`,
          icon: <Icons.SortAsc />,
          onClick: () => stage.applyHeaderSort(colId, false, absortMeta, true)
        });
        sortChildren.push({
          label: `${multiVerb} Absolute Desc`,
          icon: <Icons.SortDesc />,
          onClick: () => stage.applyHeaderSort(colId, true, absortMeta, true)
        });
      }
      if (existingSortIndex >= 0) {
        sortChildren.push('separator');
        sortChildren.push({
          label: 'Clear Sort',
          onClick: () => column.clearSorting()
        });
      }
      actions.push({
        label: 'Sort',
        icon: <Icons.SortAsc />,
        children: sortChildren
      });
      actions.push({
        label: 'Filter...',
        icon: <Icons.Filter />,
        onClick: () => {
          stage.setFilterAnchorEl({
            getBoundingClientRect: () => ({
              left: menuX,
              right: menuX,
              top: menuY,
              bottom: menuY,
              width: 0,
              height: 0
            })
          });
          stage.setActiveFilterCol(colId);
          stage.requestFilterOptions(colId);
        }
      });
      actions.push({
        label: 'Clear Filter',
        onClick: () => stage.handleHeaderFilter(colId, null)
      });
      actions.push('separator');
    }
    const pivotSortField = stage.getPivotColumnSortField(header, level);
    const pivotSortState = stage.getPivotColumnSortState(pivotSortField);
    if (pivotSortField) {
      const pivotSortChildren = [{
        label: 'Labels A → Z',
        icon: <Icons.SortAsc />,
        onClick: () => stage.applyPivotColumnSort(pivotSortField, 'label', 'asc')
      }, {
        label: 'Labels Z → A',
        icon: <Icons.SortDesc />,
        onClick: () => stage.applyPivotColumnSort(pivotSortField, 'label', 'desc')
      }, 'separator', {
        label: 'Totals Largest First',
        icon: <Icons.SortDesc />,
        onClick: () => stage.applyPivotColumnSort(pivotSortField, 'total', 'desc')
      }, {
        label: 'Totals Smallest First',
        icon: <Icons.SortAsc />,
        onClick: () => stage.applyPivotColumnSort(pivotSortField, 'total', 'asc')
      }];
      if (pivotSortState) {
        pivotSortChildren.push('separator');
        pivotSortChildren.push({
          label: 'Clear Column Order',
          onClick: () => stage.applyPivotColumnSort(pivotSortField, null, null)
        });
      }
      actions.push({
        label: 'Column Order',
        icon: <Icons.SortAsc />,
        children: pivotSortChildren
      });
      actions.push('separator');
    }

    // --- Pin submenu ---
    let isPinned = false;
    const pinChildren = [];
    if (isGroup) {
      const leafIds = getAllLeafIdsFromColumn(column);
      const allPinnedLeft = leafIds.length > 0 && leafIds.every(id => (left || []).includes(id));
      const allPinnedRight = leafIds.length > 0 && leafIds.every(id => (right || []).includes(id));
      if (allPinnedLeft) isPinned = 'left';else if (allPinnedRight) isPinned = 'right';
      pinChildren.push({
        label: (isPinned === 'left' ? '✓ ' : '') + 'Pin All Left',
        icon: <Icons.PinLeft />,
        onClick: () => stage.handlePinColumn(colId, 'left')
      });
      pinChildren.push({
        label: (isPinned === 'right' ? '✓ ' : '') + 'Pin All Right',
        icon: <Icons.PinRight />,
        onClick: () => stage.handlePinColumn(colId, 'right')
      });
      if (isPinned) pinChildren.push({
        label: 'Unpin All',
        onClick: () => stage.handlePinColumn(colId, false)
      });
    } else {
      if ((left || []).includes(colId)) isPinned = 'left';else if ((right || []).includes(colId)) isPinned = 'right';
      pinChildren.push({
        label: (isPinned === 'left' ? '✓ ' : '') + 'Pin Left',
        icon: <Icons.PinLeft />,
        onClick: () => stage.handlePinColumn(colId, 'left')
      });
      pinChildren.push({
        label: (isPinned === 'right' ? '✓ ' : '') + 'Pin Right',
        icon: <Icons.PinRight />,
        onClick: () => stage.handlePinColumn(colId, 'right')
      });
      if (isPinned) pinChildren.push({
        label: 'Unpin',
        onClick: () => stage.handlePinColumn(colId, false)
      });
    }
    actions.push({
      label: isPinned ? `Pin (${isPinned})` : 'Pin',
      icon: <Icons.PinLeft />,
      children: pinChildren
    });

    // --- Move submenu ---
    const menuLeafIds = (isGroup ? getAllLeafIdsFromColumn(column) : [colId]).filter(id => id && stage.availableColumnOrderIds.includes(id));
    const currentOrderForMenu = mergeColumnOrderWithIds(stage.effectiveColumnOrder, stage.availableColumnOrderIds);
    const menuLeafIndexes = menuLeafIds.map(id => currentOrderForMenu.indexOf(id)).filter(index => index >= 0);
    const menuBlockStart = menuLeafIndexes.length > 0 ? Math.min(...menuLeafIndexes) : -1;
    const menuBlockEnd = menuLeafIndexes.length > 0 ? Math.max(...menuLeafIndexes) : -1;
    const canMoveMenuColumn = menuLeafIds.length > 0 && currentOrderForMenu.length > menuLeafIds.length;
    const moveMenuColumnBlock = placement => {
      if (!canMoveMenuColumn) return;
      const leafSet = new Set(menuLeafIds);
      stage.setColumnOrderWithHistory(previousOrder => {
        const order = mergeColumnOrderWithIds(previousOrder, stage.availableColumnOrderIds);
        const block = order.filter(id => leafSet.has(id));
        if (block.length === 0) return order;
        const remainder = order.filter(id => !leafSet.has(id));
        const firstIndex = order.findIndex(id => leafSet.has(id));
        const lastIndex = order.reduce((latest, id, index) => leafSet.has(id) ? index : latest, -1);
        let insertIndex = 0;
        if (placement === 'last') {
          insertIndex = remainder.length;
        } else if (placement === 'left') {
          const previousId = firstIndex > 0 ? [...order.slice(0, firstIndex)].reverse().find(id => !leafSet.has(id)) : null;
          insertIndex = previousId ? Math.max(0, remainder.indexOf(previousId)) : 0;
        } else if (placement === 'right') {
          const nextId = lastIndex >= 0 ? order.slice(lastIndex + 1).find(id => !leafSet.has(id)) : null;
          insertIndex = nextId ? Math.min(remainder.length, remainder.indexOf(nextId) + 1) : remainder.length;
        }
        return [...remainder.slice(0, insertIndex), ...block, ...remainder.slice(insertIndex)];
      }, `layout:column-order-${placement}`);
    };
    actions.push({
      label: isGroup ? 'Move Group' : 'Move Column',
      icon: <Icons.DragIndicator />,
      children: [{
        label: stage.getLocaleText('menuMoveColumnLeft', isGroup ? 'Move Group Left' : 'Move Left'),
        disabled: !canMoveMenuColumn || menuBlockStart <= 0,
        onClick: () => moveMenuColumnBlock('left')
      }, {
        label: stage.getLocaleText('menuMoveColumnRight', isGroup ? 'Move Group Right' : 'Move Right'),
        disabled: !canMoveMenuColumn || menuBlockEnd < 0 || menuBlockEnd >= currentOrderForMenu.length - 1,
        onClick: () => moveMenuColumnBlock('right')
      }, {
        label: stage.getLocaleText('menuMoveColumnToStart', isGroup ? 'Move Group to Start' : 'Move to Start'),
        disabled: !canMoveMenuColumn || menuBlockStart <= 0,
        onClick: () => moveMenuColumnBlock('first')
      }, {
        label: stage.getLocaleText('menuMoveColumnToEnd', isGroup ? 'Move Group to End' : 'Move to End'),
        disabled: !canMoveMenuColumn || menuBlockEnd < 0 || menuBlockEnd >= currentOrderForMenu.length - 1,
        onClick: () => moveMenuColumnBlock('last')
      }, 'separator', {
        label: stage.getLocaleText('menuResetColumnOrder', 'Reset Column Order'),
        disabled: stage.columnOrder.length === 0,
        onClick: () => stage.setColumnOrderWithHistory([], 'layout:column-order-reset')
      }]
    });

    // --- Visibility ---
    actions.push('separator');
    if (!isGroup && column.getCanHide && column.getCanHide()) {
      actions.push({
        label: stage.getLocaleText('menuHideColumn', 'Hide Column'),
        icon: <Icons.VisibilityOff />,
        onClick: () => column.toggleVisibility(false)
      });
    }
    actions.push({
      label: stage.getLocaleText('menuShowAllColumns', 'Show All Columns'),
      icon: <Icons.Visibility />,
      onClick: () => {
        if (stage.table && typeof stage.table.getAllLeafColumns === 'function') {
          stage.table.getAllLeafColumns().forEach(leafColumn => {
            if (leafColumn && leafColumn.getCanHide && leafColumn.getCanHide()) {
              leafColumn.toggleVisibility(true);
            }
          });
        }
      }
    });

    // --- Expand/Collapse ---
    actions.push('separator');
    actions.push({
      label: 'Expand All Rows',
      onClick: () => stage.handleExpandAllRows(true)
    });
    actions.push({
      label: 'Collapse All Rows',
      onClick: () => stage.handleExpandAllRows(false)
    });

    // --- Column size submenu + export ---
    actions.push('separator');
    actions.push({
      label: 'Column Size',
      children: [{
        label: stage.getLocaleText('menuAutoSizeColumn', 'Auto-size Column'),
        onClick: () => stage.autoSizeColumn(colId)
      }, {
        label: stage.getLocaleText('menuAutoSizeAllVisibleColumns', 'Auto-size All'),
        onClick: () => {
          stage.table.getVisibleLeafColumns().forEach(leafColumn => stage.autoSizeColumn(leafColumn.id));
        }
      }]
    });
    actions.push({
      label: stage.getLocaleText('menuExportToExcel', 'Export to Excel'),
      icon: <Icons.Export />,
      onClick: () => stage.exportPivot()
    });
    stage.setContextMenu({
      x: menuX,
      y: menuY,
      actions: actions
    });
  };
  stage.handleContextMenu = (e, value, colId, row) => {
    e.preventDefault();
    const rowId = row ? row.id : null;
    const key = rowId ? `${rowId}:${colId}` : null;
    // Track the row path for "Format Row" feature
    if (row && row.original && row.original._path) {
      stage.setHoveredRowPath(row.original._path);
    }

    // Right-click should operate on the clicked cell immediately.
    // If it was not selected yet, promote it to the active selection.
    let selectionForMenu = stage.selectedCells;
    if (rowId && key && !stage.hasSelectionKey(stage.selectedCells, key)) {
      selectionForMenu = {
        [key]: value
      };
      stage.setSelectedCells(selectionForMenu);
      const visibleCols = getDisplayOrderedLeafColumns(stage.table);
      const colIndex = visibleCols.findIndex(c => c.id === colId);
      const displayRowIndex = row ? stage.resolveDisplayRowIndex(row.id, row.index) : -1;
      if (row && displayRowIndex >= 0 && colIndex >= 0) {
        stage.setLastSelected({
          rowIndex: displayRowIndex,
          colIndex
        });
      }
    }
    const hasSelection = Object.keys(selectionForMenu).length > 0;
    const getTableData = (withHeaders, mergedHeaders = false) => {
      const visibleCols = stage.table.getVisibleLeafColumns().filter(c => stage.isExportableColumnId(c.id));
      let tsv = '';
      if (withHeaders) {
        const headerRow = visibleCols.map(c => {
          if (!mergedHeaders) {
            return typeof c.columnDef.header === 'string' ? c.columnDef.header : c.id;
          }
          // Merge all ancestor labels into one; strip agg suffix (e.g. " (Sum)") from leaf
          const stack = stage.getDisplayHeaderTextStackForColumn(c);
          if (stack.length <= 1) {
            return stack[0] || (typeof c.columnDef.header === 'string' ? c.columnDef.header : c.id);
          }
          const leafLabel = (stack[stack.length - 1] || '').replace(/\s*\([^)]+\)$/, '');
          return [...stack.slice(0, -1), leafLabel].filter(Boolean).join(stage.mergedHeaderSeparator);
        });
        tsv += headerRow.join('\t') + '\n';
      }

      // Data body — server-side: use all cached blocks; client-side: use full row model
      if (stage.serverSide && Array.isArray(stage.loadedRows) && stage.loadedRows.length > 0) {
        return tsv + (buildTsvFromRawRows(stage.loadedRows, false, visibleCols) || '');
      }
      const visibleRows = stage.getDisplayRows();
      visibleRows.forEach(r => {
        const vals = visibleCols.map(c => {
          const v = r.getValue(c.id);
          return v !== undefined && v !== null ? String(v) : "";
        });
        tsv += vals.join('\t') + '\n';
      });
      return tsv;
    };
    const copyCurrentTableServerSide = (withHeaders, mergedHeaders) => {
      stage.copyCurrentTableToClipboard(withHeaders, mergedHeaders);
    };
    const copyAllRecordsServerSide = (withHeaders, mergedHeaders) => {
      stage.copyAllRecordsToClipboard(withHeaders, mergedHeaders);
    };
    const copyTableLocal = (withHeaders, mergedHeaders) => {
      const payload = stage.buildTableClipboardPayload({
        withHeaders,
        mergedHeaders
      });
      if (payload) stage.copyRichToClipboard(payload);
    };
    const copyAllRecordsLocal = (withHeaders, mergedHeaders) => {
      const payload = stage.buildAllRowsClipboardPayload({
        withHeaders,
        mergedHeaders
      });
      if (payload) stage.copyRichToClipboard(payload);
    };
    const actions = [{
      label: 'Copy Current Table',
      icon: <Icons.DragIndicator />,
      title: 'Copy the current expanded/collapsed table, with its active sorting, filters, and visible column order.',
      onClick: () => stage.serverSide ? copyCurrentTableServerSide(true, false) : copyTableLocal(true, false)
    }, {
      label: 'Copy All Records',
      icon: <Icons.DragIndicator />,
      title: 'Copy every matching record, including rows currently collapsed in the table.',
      onClick: () => stage.serverSide ? copyAllRecordsServerSide(true, false) : copyAllRecordsLocal(true, false)
    }, {
      label: 'Copy Options',
      children: [{
        label: 'Current Table without Headers',
        onClick: () => stage.serverSide ? copyCurrentTableServerSide(false, false) : copyTableLocal(false, false)
      }, {
        label: 'Current Table with Merged Headers',
        onClick: () => stage.serverSide ? copyCurrentTableServerSide(true, true) : copyTableLocal(true, true)
      }, {
        label: 'All Records without Headers',
        onClick: () => stage.serverSide ? copyAllRecordsServerSide(false, false) : copyAllRecordsLocal(false, false)
      }, {
        label: 'All Records with Merged Headers',
        onClick: () => stage.serverSide ? copyAllRecordsServerSide(true, true) : copyAllRecordsLocal(true, true)
      }]
    }];
    if (hasSelection) {
      actions.push('separator');
      actions.push({
        label: 'Copy Selection',
        onClick: () => {
          const payload = stage.buildSelectedClipboardPayload(false, selectionForMenu);
          if (payload) stage.copyRichToClipboard(payload);
        }
      });
      actions.push({
        label: 'Copy Selection (Raw Values)',
        onClick: () => {
          const payload = stage.buildSelectedClipboardPayload(false, selectionForMenu, { rawValues: true });
          if (payload) stage.copyRichToClipboard(payload);
        }
      });
      actions.push({
        label: 'Copy Selection with Headers',
        onClick: () => {
          const payload = stage.buildSelectedClipboardPayload(true, selectionForMenu);
          if (payload) stage.copyRichToClipboard(payload);
        }
      });
      actions.push({
        label: 'Create Range Chart',
        icon: <Icons.Chart />,
        onClick: () => stage.openSelectionChart(selectionForMenu)
      });
    }
    actions.push('separator');
    actions.push({
      label: `Filter by "${value}"`,
      icon: <Icons.Filter />,
      onClick: () => {
        stage.handleHeaderFilter(colId, {
          operator: 'AND',
          conditions: [{
            type: 'eq',
            value: String(value),
            caseSensitive: false
          }]
        });
      }
    });
    actions.push({
      label: 'Clear Filter',
      onClick: () => stage.handleHeaderFilter(colId, null)
    });
    actions.push('separator');
    actions.push({
      label: 'Drill Through',
      icon: <Icons.Search />,
      onClick: () => {
        if (row && row.original && row.original._path && row.original._path !== '__grand_total__' && !row.original._isTotal) {
          stage.handleCellDrillThrough(row, colId);
        }
      }
    });
    actions.push('separator');
    if (row && stage.serverSide && row.getCanExpand() && row.original && row.original._path && row.original._has_children) {
      actions.push({
        label: 'Expand All Children',
        icon: <Icons.ChevronDown />,
        onClick: () => {
          const rowPath = row.original._path;
          stage.subtreeExpandRef.current = {
            path: rowPath,
            expandedPaths: new Set([rowPath])
          };
          stage.captureExpansionScrollPosition();
          stage.clearCache();
          stage.setExpandedWithHistory(prev => {
            const base = prev !== null && typeof prev === 'object' ? prev : {};
            return {
              ...base,
              [rowPath]: true
            };
          }, 'layout:expand-subtree');
        }
      });
    }
    actions.push('separator');
    if (rowId) {
      const currentPinState = rowId === GRAND_TOTAL_ROW_ID ? stage.effectiveGrandTotalPinState : getPinnedSideForRow(stage.rowPinning, rowId);
      const isPinnedTop = currentPinState === 'top';
      const isPinnedBottom = currentPinState === 'bottom';
      actions.push({
        label: 'Pin Row Top',
        onClick: () => stage.handlePinRow(rowId, 'top')
      });
      actions.push({
        label: 'Pin Row Bottom',
        onClick: () => stage.handlePinRow(rowId, 'bottom')
      });
      if (isPinnedTop || isPinnedBottom) {
        actions.push({
          label: 'Unpin Row',
          onClick: () => stage.handlePinRow(rowId, false)
        });
      }
    }
    stage.setContextMenu({
      x: e.clientX,
      y: e.clientY,
      actions: actions
    });
  };
  useEffect(() => {
    const nextRowPaths = [];
    const seenPaths = new Set();
    stage.cleanData.forEach(row => {
      const rowId = resolveTransportRowId(row);
      if (!rowId || seenPaths.has(rowId)) return;
      seenPaths.add(rowId);
      nextRowPaths.push(rowId);
    });
    stage.visiblePatchRowPathsRef.current = nextRowPaths;
  }, [stage.cleanData]);
  useEffect(() => {
    const groupingIds = new Set(Array.isArray(stage.rowFields) ? stage.rowFields : []);
    const nextCenterIds = [];
    const seenIds = new Set();
    (Array.isArray(stage.responseColumns) ? stage.responseColumns : []).forEach(column => {
      const columnId = column && typeof column === 'object' ? column.id : column;
      const normalizedId = String(columnId || '').trim();
      if (!normalizedId || normalizedId === '__col_schema' || groupingIds.has(normalizedId) || seenIds.has(normalizedId)) {
        return;
      }
      seenIds.add(normalizedId);
      nextCenterIds.push(normalizedId);
    });
    stage.visiblePatchCenterIdsRef.current = nextCenterIds;
  }, [stage.responseColumns, stage.rowFields]);
  stage.staticTotal = useMemo(() => ({
    _isTotal: true,
    _path: '__grand_total__',
    _id: 'Grand Total',
    __isGrandTotal__: true
  }), []);
  stage.staticMinMax = useMemo(() => ({}), []);
  ({
    nodes: stage.nodes,
    total: stage.total,
    minMax: stage.minMax
  } = useMemo(() => {
    return {
      nodes: stage.filteredData,
      total: stage.staticTotal,
      minMax: stage.staticMinMax
    };
  }, [stage.filteredData, stage.staticTotal, stage.staticMinMax]));
  const filterPopover = useRuntimeFilterPopover({
    activeFilterCol: stage.activeFilterCol,
    dispatchServerSideRuntimeSetProps: stage.dispatchServerSideRuntimeSetProps,
    filterOptionMeta: stage.filterOptionMeta,
    filterOptions: stage.filterOptions,
    pendingServerFilterOptionsRef: stage.pendingServerFilterOptionsRef,
    serverSide: stage.serverSide,
    setActiveFilterCol: stage.setActiveFilterCol,
    setFilterAnchorEl: stage.setFilterAnchorEl,
    setFilters: stage.setFilters,
    setTransportFilterOptionMetaState: stage.setTransportFilterOptionMetaState,
    showFilters: stage.uiConfig.showFilters,
  });
  for (const [key, value] of Object.entries(filterPopover)) stage[key] = value;
  useEffect(() => {
    const activeLeafIds = new Set();
    const stack = [...stage.columns];
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
    stage.setColumnSizing(prev => {
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
  }, [stage.columns]);
  stage.parentRef = useRef(null);
  stage.expansionScrollRestoreRef = useRef(null);
  stage.expansionScrollRestoreRafRef = useRef(null);
  stage.subtreeExpandRef = useRef(null);
  stage.pendingExpansionRef = useRef(null);
  const displayNavigation = useRuntimeDisplayNavigation({
    activeRowVirtualizerRef: stage.activeRowVirtualizerRef,
    columnVirtualizerRef: stage.columnVirtualizerRef,
    parentRef: stage.parentRef,
    pinnedColumnMetaRef: stage.pinnedColumnMetaRef,
    pinnedDisplayMetaRef: stage.pinnedDisplayMetaRef
  });
  for (const [key, value] of Object.entries(displayNavigation)) stage[key] = value;
  useRuntimeViewRestoreScroll({
    parentRef: stage.parentRef,
    viewState: stage.viewState
  });
  const viewSnapshot = useRuntimeViewSnapshot({
    activeChartDefinitionId: stage.activeChartDefinitionId,
    activeReportId: stage.activeReportId,
    cellFormatRules: stage.cellFormatRules,
    chartCanvasPanes: stage.chartCanvasPanes,
    chartPanelAxisMode: stage.chartPanelAxisMode,
    chartPanelBarLayout: stage.chartPanelBarLayout,
    chartPanelColumnLimit: stage.chartPanelColumnLimit,
    chartPanelFloating: stage.chartPanelFloating,
    chartPanelFloatingRect: stage.chartPanelFloatingRect,
    chartPanelGraphHeight: stage.chartPanelGraphHeight,
    chartPanelHierarchyLevel: stage.chartPanelHierarchyLevel,
    chartPanelInteractionMode: stage.chartPanelInteractionMode,
    chartPanelLayers: stage.chartPanelLayers,
    chartPanelLocked: stage.chartPanelLocked,
    chartPanelLockedModel: stage.chartPanelLockedModel,
    chartPanelLockedRequest: stage.chartPanelLockedRequest,
    chartPanelOpen: stage.chartPanelOpen,
    chartPanelOrientation: stage.chartPanelOrientation,
    chartPanelRowLimit: stage.chartPanelRowLimit,
    chartPanelServerScope: stage.chartPanelServerScope,
    chartPanelSortMode: stage.chartPanelSortMode,
    chartPanelSource: stage.chartPanelSource,
    chartPanelTitle: stage.chartPanelTitle,
    chartPanelType: stage.chartPanelType,
    chartPanelWidth: stage.chartPanelWidth,
    colExpanded: stage.colExpanded,
    colFields: stage.colFields,
    colSearch: stage.colSearch,
    colTypeFilter: stage.colTypeFilter,
    colorPalette: stage.colorPalette,
    colorScaleMode: stage.colorScaleMode,
    columnDecimalOverrides: stage.columnDecimalOverrides,
    columnFormatOverrides: stage.columnFormatOverrides,
    columnGroupSeparatorOverrides: stage.columnGroupSeparatorOverrides,
    columnOrder: stage.columnOrder,
    columnPinning: stage.columnPinning,
    columnSizing: stage.columnSizing,
    columnVisibility: stage.columnVisibility,
    customDimensions: stage.customDimensions,
    dataBarsColumns: stage.dataBarsColumns,
    decimalPlaces: stage.decimalPlaces,
    defaultValueFormat: stage.defaultValueFormat,
    detailConfig: stage.detailConfig,
    detailMode: stage.detailMode,
    editValueDisplayMode: stage.editValueDisplayMode,
    expanded: stage.expanded,
    fieldPanelSizes: stage.fieldPanelSizes,
    filters: stage.filters,
    frozenPivotConfig: stage.frozenPivotConfig,
    grandTotalPinOverride: stage.grandTotalPinOverride,
    latestViewportRef: stage.latestViewportRef,
    layoutMode: stage.layoutMode,
    managedChartDefinitions: stage.managedChartDefinitions,
    normalizedChartServerWindow: stage.normalizedChartServerWindow,
    normalizedInitialValConfigs: stage.normalizedInitialValConfigs,
    numberGroupSeparator: stage.numberGroupSeparator,
    parentRef: stage.parentRef,
    pivotColumnSorting: stage.pivotColumnSorting,
    reportDef: stage.reportDef,
    rowFields: stage.rowFields,
    rowPinning: stage.rowPinning,
    runtimeMeasureAxis: stage.runtimeMeasureAxis,
    saveViewTrigger: stage.saveViewTrigger,
    savedReports: stage.savedReports,
    serializedEditComparisonState: stage.serializedEditComparisonState,
    setPropsRef: stage.setPropsRef,
    showColTotals: stage.showColTotals,
    showFloatingFilters: stage.showFloatingFilters,
    showNotification: stage.showNotification,
    showReportConfigColumn: stage.showReportConfigColumn,
    showRowNumbers: stage.showRowNumbers,
    showRowTotals: stage.showRowTotals,
    showSubtotals: stage.showSubtotals,
    sidebarOpen: stage.sidebarOpen,
    sidebarTab: stage.sidebarTab,
    sidebarWidth: stage.sidebarWidth,
    sorting: stage.sorting,
    spacingMode: stage.spacingMode,
    stickyHeaders: stage.stickyHeaders,
    tableCanvasSize: stage.tableCanvasSize,
    tableName: stage.tableName,
    themeName: stage.themeName,
    themeOverrides: stage.themeOverrides,
    treeConfig: stage.treeConfig,
    valConfigs: stage.valConfigs,
    viewMode: stage.viewMode,
    zoomLevel: stage.zoomLevel,
  });
  for (const [key, value] of Object.entries(viewSnapshot)) stage[key] = value;
  stage.rowHeight = stage.rowHeights[stage.spacingMode] || stage.rowHeights[0];
  stage.handleTransposePivot = useCallback(() => {
    const nextRowFields = [...stage.colFields];
    const nextColFields = [...stage.rowFields];
    stage.requestLayoutHistoryCapture('layout:transpose');
    stage.closeFilterPopover();
    stage.setContextMenu(null);
    stage.setExpanded({});
    stage.setSorting([]);
    stage.setColExpanded({});
    stage.setSelectedCells({});
    stage.setLastSelected(null);
    stage.setSelectedCols(new Set());
    stage.setDragStart(null);
    stage.setFillRange(null);
    stage.setIsDragging(false);
    stage.setIsFilling(false);
    stage.setShowRowTotals(stage.showColTotals);
    stage.setShowColTotals(stage.showRowTotals);
    stage.setGrandTotalPinOverride(null);
    stage.setRowFields(nextRowFields);
    stage.setColFields(nextColFields);
    if (stage.parentRef.current) {
      stage.parentRef.current.scrollTop = 0;
      stage.parentRef.current.scrollLeft = 0;
    }
    stage.showNotification('Pivot transposed', 'success');
  }, [stage.closeFilterPopover, stage.colFields, stage.rowFields, stage.requestLayoutHistoryCapture, stage.showColTotals, stage.showRowTotals, stage.showNotification]);
  stage.serverSideCacheKey = useMemo(() => JSON.stringify({
    sorting: stage.sorting,
    filters: stage.filters,
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    valConfigs: stage.valConfigs,
    customDimensions: stage.customDimensions
  }), [stage.sorting, stage.filters, stage.rowFields, stage.colFields, stage.valConfigs, stage.customDimensions]);
  stage.serverSideViewportResetKey = useMemo(() => JSON.stringify({
    sorting: stage.sorting,
    filters: stage.filters,
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    valConfigs: stage.valConfigs,
    customDimensions: stage.customDimensions
  }), [stage.sorting, stage.filters, stage.rowFields, stage.colFields, stage.valConfigs, stage.customDimensions]);
  stage.effectiveRowCount = stage.serverSidePinsGrandTotal && stage.rowCount ? Math.max(stage.rowCount - 1, 0) : stage.rowCount;
  stage.statusRowCount = stage.serverSidePinsGrandTotal && stage.rowCount ? Math.max(stage.rowCount - 1, 0) : stage.rowCount;
  const recordProfilerQueuedRequest = transport && transport.profiling && typeof transport.profiling.recordQueued === 'function'
    ? transport.profiling.recordQueued
    : () => undefined;
  stage.handleViewportRequestMeta = useCallback(requestMeta => {
    stage.markRequestPending(requestMeta);
    recordProfilerQueuedRequest(requestMeta);
  }, [recordProfilerQueuedRequest, stage.markRequestPending]);
  stage.captureExpansionScrollPosition = useCallback(() => {
    if (!stage.serverSide || !stage.parentRef.current) return;
    stage.expansionScrollRestoreRef.current = {
      scrollTop: stage.parentRef.current.scrollTop,
      restorePassesRemaining: 3
    };
  }, [stage.serverSide]);
  ({
    rowVirtualizer: stage.rowVirtualizer,
    getRow: stage.getRow,
    renderedData: stage.renderedData,
    renderedOffset: stage.renderedOffset,
    clearCache: stage.clearCache,
    invalidateFromBlock: stage.invalidateFromBlock,
    grandTotalRow: stage.grandTotalRow,
    loadedRows: stage.loadedRows,
    requestUrgentColumnViewport: stage.requestUrgentColumnViewport,
    requestVisibleViewportRefresh: stage.requestVisibleViewportRefresh
  } = useServerSideRowModel({
    parentRef: stage.parentRef,
    serverSide: stage.serverSide,
    rowCount: stage.effectiveRowCount,
    rowHeight: stage.rowHeight,
    data: stage.filteredData,
    dataOffset: stage.dataOffset || 0,
    dataVersion: stage.dataVersion || 0,
    dataStateEpoch: stage.responseStateEpoch,
    setProps: stage.dispatchServerSideRuntimeSetProps,
    blockSize: stage.serverSideBlockSize,
    maxBlocksInCache: stage.normalizedPerformanceConfig.maxBlocksInCache || 500,
    blockLoadDebounceMs: stage.normalizedPerformanceConfig.blockLoadDebounceMs,
    rowOverscan: stage.normalizedPerformanceConfig.rowOverscan,
    prefetchColumns: stage.normalizedPerformanceConfig.prefetchColumns,
    cacheKey: stage.serverSideCacheKey,
    excludeGrandTotal: stage.serverSidePinsGrandTotal,
    immersiveMode: stage.immersiveMode,
    stateEpoch: stage.stateEpoch,
    sessionId: stage.sessionIdRef.current,
    clientInstance: stage.clientInstanceRef.current,
    abortGeneration: stage.abortGeneration,
    structuralInFlight: stage.structuralInFlight,
    requestVersionRef: stage.requestVersionRef,
    tableName: stage.tableName,
    colStart: stage.colRequestStart,
    colEnd: stage.colRequestEnd,
    columnRangeUrgencyToken: stage.columnRangeUrgencyToken,
    responseColStart: stage.responseSchemaWindow.start,
    responseColEnd: stage.responseSchemaWindow.end,
    needsColSchema: stage.needsColSchema && stage.serverSide,
    onViewportRequest: stage.handleViewportRequestMeta,
    layoutMode: stage.layoutMode,
    showSubtotals: stage.showSubtotals
  }));
  stage.requestUrgentColumnViewportRef.current = stage.requestUrgentColumnViewport;
  ({ retryRuntimeData: stage.retryRuntimeData, runtimeDataError: stage.runtimeDataError } = useRuntimeDataRetry({
    runtimeDataError: stage.runtimeDataError,
    setRuntimeDataError: stage.setRuntimeDataError,
    requestVisibleViewportRefresh: stage.requestVisibleViewportRefresh,
    requestVersionRef: stage.requestVersionRef,
    stateEpochRef: stage.stateEpochRef,
    clientInstanceRef: stage.clientInstanceRef,
  }));
  stage.scheduleSilentViewportRefresh = useCallback(() => {
    if (!stage.serverSide || typeof stage.requestVisibleViewportRefresh !== 'function') return;
    if (stage.deferredViewportRefreshTimeoutRef.current !== null) {
      clearTimeout(stage.deferredViewportRefreshTimeoutRef.current);
    }
    stage.deferredViewportRefreshTimeoutRef.current = setTimeout(() => {
      stage.deferredViewportRefreshTimeoutRef.current = null;
      stage.requestVisibleViewportRefresh();
    }, 0);
  }, [stage.requestVisibleViewportRefresh, stage.serverSide]);
  useEffect(() => () => {
    if (stage.deferredViewportRefreshTimeoutRef.current !== null) {
      clearTimeout(stage.deferredViewportRefreshTimeoutRef.current);
    }
  }, []);
  stage.columns = useColumnDefs({
    sortOptions: stage.effectiveSortOptions,
    sorting: stage.sorting,
    serverSide: stage.serverSide,
    showRowNumbers: stage.showRowNumbers,
    layoutMode: stage.layoutMode,
    viewMode: stage.viewMode,
    rowFields: stage.rowFields,
    colFields: stage.colFields,
    valConfigs: stage.valConfigs,
    customDimensions: stage.customDimensions,
    treeConfig: stage.treeConfig,
    minMax: stage.minMax,
    colorScaleMode: stage.colorScaleMode,
    colExpanded: stage.colExpanded,
    isRowSelecting: stage.isRowSelecting,
    rowDragStart: stage.rowDragStart,
    props: stage.props,
    measureAxis: stage.runtimeMeasureAxis,
    cachedColSchema: stage.cachedColSchema,
    responseColumns: stage.responseColumns,
    filteredData: stage.filteredData,
    rowCount: stage.rowCount,
    // Render-time closures (stable refs or render-time reads)
    theme: stage.theme,
    defaultColumnWidths: stage.defaultColumnWidths,
    validationRules: stage.validationRules,
    onCellEdit: stage.editingEnabled ? stage.dispatchInlineCellEdit : undefined,
    onEditBlocked: stage.editingEnabled ? stage.handleBlockedCellEdit : undefined,
    resolveEditorPresentation: stage.editingEnabled ? stage.resolveEditorPresentation : undefined,
    rowEditMode: stage.editingEnabled ? stage.normalizedEditingConfig.mode : 'cell',
    getRowEditSession: stage.editingEnabled ? stage.getRowEditSession : undefined,
    onRequestRowStart: stage.editingEnabled ? stage.startRowEditSession : undefined,
    onRowDraftChange: stage.editingEnabled ? stage.updateRowDraftValue : undefined,
    onRequestRowSave: stage.editingEnabled ? stage.saveRowEditSession : undefined,
    onRequestRowCancel: stage.editingEnabled ? stage.cancelRowEditSession : undefined,
    requestEditorOptions: stage.editingEnabled ? stage.requestEditorOptions : undefined,
    renderRowEditActions: stage.editingEnabled ? stage.renderRowEditActions : undefined,
    handleContextMenu: stage.handleContextMenu,
    handleRowSelect: stage.handleRowSelect,
    handleRowRangeSelect: stage.handleRowRangeSelect,
    setIsRowSelecting: stage.setIsRowSelecting,
    setRowDragStart: stage.setRowDragStart,
    renderedOffset: stage.renderedOffset,
    isColExpanded: stage.isColExpanded,
    toggleCol: stage.toggleCol,
    pendingRowTransitions: stage.pendingRowTransitions,
    decimalPlaces: stage.decimalPlaces,
    defaultValueFormat: stage.defaultValueFormat,
    numberGroupSeparator: stage.numberGroupSeparator,
    columnDecimalOverrides: stage.columnDecimalOverrides,
    columnFormatOverrides: stage.columnFormatOverrides,
    columnGroupSeparatorOverrides: stage.columnGroupSeparatorOverrides,
    cellFormatRules: stage.cellFormatRules,
    pivotMode: stage.pivotMode,
    reportDef: stage.reportDef,
    showReportConfigColumn: stage.showReportConfigColumn !== false && !stage.immersiveMode,
    onConfigureReportLine: stage.handleConfigureReportLine,
    detailMode: stage.detailMode,
    isDetailOpenForRow: rowPath => Boolean(stage.detailSurface && stage.detailSurface.rowPath === rowPath),
    onToggleDetail: row => {
      if (stage.toggleDetailForRowRef.current) {
        stage.toggleDetailForRowRef.current(row);
      }
    },
    resolveCellDisplayValue: stage.resolveDisplayedCellValue,
    editValueDisplayMode: stage.editValueDisplayMode,
    editingEnabled: stage.editingEnabled,
    openSparklineDataModalRef: stage.openSparklineDataModalRef,
    rowCheckboxSelection: stage.rowCheckboxMode,
    selectedRowKeys: stage.selectedRowKeys,
    toggleRowCheckbox: stage.toggleRowCheckbox
  });
  stage.availableColumnOrderIds = useMemo(() => collectColumnDefLeafIds(stage.columns), [stage.columns]);
  stage.effectiveColumnOrder = useMemo(() => mergeColumnOrderWithIds(stage.columnOrder, stage.availableColumnOrderIds), [stage.availableColumnOrderIds, stage.columnOrder]);
  stage.columnOrderKey = useMemo(() => stage.effectiveColumnOrder.join('\u001f'), [stage.effectiveColumnOrder]);
  useEffect(() => {
    if (stage.columnOrder.length === 0) return;
    if (!areStringArraysEqual(stage.columnOrder, stage.effectiveColumnOrder)) {
      stage.setColumnOrder(stage.effectiveColumnOrder);
    }
  }, [stage.columnOrder, stage.effectiveColumnOrder]);

  // Auto-size new columns to fit their header text on spawn
  useEffect(() => {
    if (!stage.table || !stage.table.getVisibleLeafColumns) return;
    const visLeafCols = stage.table.getVisibleLeafColumns();
    if (visLeafCols.length === 0) return;
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    ctx.font = `600 ${stage.fontSize || '14px'} ${stage.fontFamily || "'Inter', system-ui, sans-serif"}`;
    const newSizes = {};
    visLeafCols.forEach(col => {
      if (stage.columnSizing[col.id] !== undefined) return; // already user-set
      if (col.columnDef && col.columnDef.meta && col.columnDef.meta.isSchemaPlaceholder) return;
      const measuredFromText = stage.getDisplayHeaderTextStackForColumn(col).reduce((maxWidth, headerText) => Math.max(maxWidth, ctx.measureText(headerText).width + stage.autoSizeBounds.headerPadding + stage.autoSizeBounds.headerOverscan), stage.autoSizeBounds.minWidth);
      const measuredFromDom = stage.getRenderedHeaderWidthForColumn(col.id);
      const measuredFromCells = col.id !== 'hierarchy' && col.id !== '__row_number__' && col.id !== '__report_config__' ? stage.getRenderedCellWidthForColumn(col.id) : null;
      const measured = Number.isFinite(measuredFromDom) ? Math.max(measuredFromText, measuredFromDom) : measuredFromText;
      const measuredWithCells = Number.isFinite(measuredFromCells) ? Math.max(measured, measuredFromCells) : measured;
      const clamped = Math.min(Math.max(measuredWithCells, stage.autoSizeBounds.minWidth), stage.autoSizeBounds.maxWidth);
      if (clamped > (col.columnDef.size || stage.autoSizeBounds.minWidth)) {
        newSizes[col.id] = clamped;
      }
    });
    if (Object.keys(newSizes).length > 0) {
      stage.setColumnSizing(prev => ({
        ...prev,
        ...newSizes
      }));
      Object.entries(newSizes).forEach(([columnId, width]) => {
        stage.queueHeaderFitValidation(columnId, width);
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stage.autoSizeBounds.maxWidth, stage.colFields, stage.fontFamily, stage.fontSize, stage.getDisplayHeaderTextStackForColumn, stage.getRenderedCellWidthForColumn, stage.getRenderedHeaderWidthForColumn, stage.queueHeaderFitValidation, stage.rowFields, stage.valConfigs]);
  useEffect(() => {
    if (!stage.serverSide || !stage.structuralInFlight) return;
    const pending = stage.structuralPendingVersionRef.current;
    const numericVersion = Number(stage.dataVersion);
    if (!pending || !Number.isFinite(numericVersion)) return;
    if (numericVersion > pending.startDataVersion && numericVersion >= pending.version) {
      stage.setStructuralInFlight(false);
      stage.structuralPendingVersionRef.current = null;
      stage.setPendingRowTransitions(new Map());
      stage.setPendingColumnSkeletonCount(0);
    }
  }, [stage.dataVersion, stage.serverSide, stage.structuralInFlight]);
  useEffect(() => {
    if (!stage.serverSide || !stage.structuralInFlight) return;
    const timeoutId = setTimeout(() => {
      stage.setStructuralInFlight(false);
      stage.structuralPendingVersionRef.current = null;
      stage.setPendingRowTransitions(new Map());
      stage.setPendingColumnSkeletonCount(0);
    }, 10000);
    return () => clearTimeout(timeoutId);
  }, [stage.serverSide, stage.structuralInFlight, stage.stateEpoch]);
  useEffect(() => {
    if (!stage.serverSide || stage.pendingRowTransitions.size === 0) return;
    const timeoutId = setTimeout(() => {
      stage.setPendingRowTransitions(new Map());
    }, 10000);
    return () => clearTimeout(timeoutId);
  }, [stage.pendingRowTransitions, stage.serverSide]);
  const viewportTableData = useRuntimeViewportTableData({
    comparisonCellCountsRef: stage.comparisonCellCountsRef,
    comparisonCellEpoch: stage.comparisonCellEpoch,
    comparisonCellOriginalValuesRef: stage.comparisonCellOriginalValuesRef,
    comparisonValueState: stage.comparisonValueState,
    editValueDisplayMode: stage.editValueDisplayMode,
    filteredData: stage.filteredData,
    grandTotalRow: stage.grandTotalRow,
    nodes: stage.nodes,
    pinnedRowCacheRef: stage.pinnedRowCacheRef,
    renderedData: stage.renderedData,
    rowPinning: stage.rowPinning,
    serverSide: stage.serverSide,
    serverSidePinsGrandTotal: stage.serverSidePinsGrandTotal,
    showColTotals: stage.showColTotals,
    tableDataRef: stage.tableDataRef,
    total: stage.total,
  });
  for (const [key, value] of Object.entries(viewportTableData)) stage[key] = value;
    // Handle zero-crossing: when data spans negative and positive,
    // use 0 as the neutral (transparent) midpoint
      // Map negative values [min,0] â†’ [0,0.5] and positive [0,max] â†’ [0.5,1]

    // Transparency: fully transparent at midpoint (0.5), max opacity at extremes
    // alpha range 0.06 â†’ 0.82 for a clean gradient feel

    // Use luminance-based neutral text instead of palette's hued dark colors,
    // which blend into the background at high alpha and become unreadable.
    // sRGB perceived-luminance weights (ITU-R BT.601)
  const colorScalePresentation = useRuntimeColorScalePresentation({
    colorPalette: stage.colorPalette,
    colorScaleMode: stage.colorScaleMode,
    colFields: stage.colFields,
    filteredData: stage.filteredData,
    getRuleBasedStyle: stage.getRuleBasedStyle,
    nodes: stage.nodes,
    rowFields: stage.rowFields,
    serverColorScaleStats: stage.serverColorScaleStats,
  });
  for (const [key, value] of Object.entries(colorScalePresentation)) stage[key] = value;
  stage.getRowId = useCallback((row, relativeIndex) => {
    if (!row) return `skeleton_${relativeIndex}`; // Handle skeleton rows
    if (row._isTotal || row._path === '__grand_total__' || row._id === 'Grand Total') return '__grand_total__';
    if (stage.serverSide && typeof row.__virtualIndex === 'number') {
      return row._path || (row.id ? row.id : String(row.__virtualIndex));
    }

    // Use renderedOffset if available (from virtualizer cache), else fallback to dataOffset
    const effectiveOffset = stage.serverSide && stage.renderedOffset !== undefined ? stage.renderedOffset : stage.dataOffset || 0;
    const actualIndex = stage.serverSide ? relativeIndex + effectiveOffset : relativeIndex;
    return row._path || (row.id ? row.id : String(actualIndex));
  }, [stage.serverSide, stage.dataOffset, stage.renderedOffset]);
  stage.getSubRows = useCallback(r => r ? r.subRows : undefined, []);
  stage.getRowCanExpand = useCallback(row => {
    if (!row.original) return false;
    // Prevent expansion of any total rows, including grand totals
    if (row.original && row.original._isTotal) return false;
    if (stage.serverSide) {
      // Use server-provided flag if available for accurate child detection
      if (row.original._has_children !== undefined) return row.original._has_children;
      return (row.original.depth || 0) < stage.rowFields.length - 1;
    }
    return row.subRows && row.subRows.length > 0;
  }, [stage.serverSide, stage.rowFields.length]);
  stage.getIsRowExpanded = useCallback(row => {
    if (!row.original) return false;
    if (row.original && row.original._isTotal) return false;
    if (stage.serverSide) {
      // 1. Explicit Local State (Optimistic)
      // We check if the key exists in the expanded object to respect user interactions
      if (stage.expanded && Object.prototype.hasOwnProperty.call(stage.expanded, row.id)) {
        return !!stage.expanded[row.id];
      }

      // 2. "Expand All" mode. The object form retains this default while
      // recording explicit false overrides for individually collapsed rows.
      if (stage.expanded === true || stage.expanded && stage.expanded[EXPAND_ALL_STATE_KEY] === true) return true;

      // 3. Server State (Fallback/Source of Truth)
      // If local state doesn't know about this row yet (e.g. initial load), trust the server
      if (row.original._is_expanded !== undefined) {
        return row.original._is_expanded;
      }
    }

    // Standard Client-Side Logic
    if (stage.expanded === true) return true;
    // Otherwise check if this specific row is expanded
    return !!stage.expanded[row.id];
  }, [stage.expanded, stage.serverSide]);
  stage.normalizeExpandedStateForTreeDefaults = useCallback((nextExpanded, previousExpanded) => {
    if (!stage.serverSide || stage.viewMode !== 'tree' || !stage.treeHasDefaultExpansion) return nextExpanded;
    if (nextExpanded === true || !nextExpanded || typeof nextExpanded !== 'object') return nextExpanded;
    if (nextExpanded[EXPAND_ALL_STATE_KEY] === true) return nextExpanded;
    const normalizedExpanded = {
      ...nextExpanded
    };
    const previous = previousExpanded && typeof previousExpanded === 'object' ? previousExpanded : {};
    const allKeys = new Set([...Object.keys(previous), ...Object.keys(normalizedExpanded)]);
    allKeys.forEach(key => {
      if (!Object.prototype.hasOwnProperty.call(normalizedExpanded, key)) {
        normalizedExpanded[key] = false;
      }
    });
    return normalizedExpanded;
  }, [stage.serverSide, stage.treeHasDefaultExpansion, stage.viewMode]);
  [stage.paginationState, stage.setPaginationState] = useState({
    pageIndex: 0,
    pageSize: stage.paginationConfig.pageSize
  });
  useEffect(() => {
    stage.setPaginationState(prev => prev.pageSize === stage.paginationConfig.pageSize ? prev : {
      ...prev,
      pageIndex: 0,
      pageSize: stage.paginationConfig.pageSize
    });
  }, [stage.paginationConfig.pageSize]);
  stage.tableState = useMemo(() => {
    const availableRowIds = new Set((stage.tableData || []).map(stage.getStableDataRowId).filter(Boolean));
    let finalRowPinning = {
      ...normalizeRowPinningState(stage.rowPinning),
      top: (stage.rowPinning && stage.rowPinning.top || []).filter(id => availableRowIds.has(id)),
      bottom: (stage.rowPinning && stage.rowPinning.bottom || []).filter(id => availableRowIds.has(id))
    };
    const topWithoutGrandTotal = (finalRowPinning.top || []).filter(id => id !== GRAND_TOTAL_ROW_ID);
    const bottomWithoutGrandTotal = (finalRowPinning.bottom || []).filter(id => id !== GRAND_TOTAL_ROW_ID);
    const hasGrandTotalRow = availableRowIds.has(GRAND_TOTAL_ROW_ID);
    finalRowPinning = {
      ...finalRowPinning,
      top: stage.effectiveGrandTotalPinState === 'top' && hasGrandTotalRow ? [...topWithoutGrandTotal, GRAND_TOTAL_ROW_ID] : topWithoutGrandTotal,
      bottom: stage.effectiveGrandTotalPinState === 'bottom' && hasGrandTotalRow ? [...bottomWithoutGrandTotal, GRAND_TOTAL_ROW_ID] : bottomWithoutGrandTotal
    };
    const state = {
      sorting: stage.sorting,
      expanded: stage.expanded,
      columnPinning: stage.columnPinning,
      rowPinning: finalRowPinning,
      grouping: stage.viewMode === 'pivot' ? stage.rowFields : [],
      columnVisibility: stage.columnVisibility,
      columnOrder: stage.effectiveColumnOrder,
      columnSizing: stage.columnSizing
    };
    if (stage.paginationConfig.enabled) {
      state.pagination = stage.paginationState;
    }
    return state;
  }, [stage.sorting, stage.expanded, stage.columnPinning, stage.rowPinning, stage.rowFields, stage.viewMode, stage.columnVisibility, stage.effectiveColumnOrder, stage.columnSizing, stage.tableData, stage.effectiveGrandTotalPinState, stage.getStableDataRowId, stage.paginationConfig.enabled, stage.paginationState]);
  stage.handleExpandAllRows = shouldExpand => {
    // Guard against rapid double-clicks: second click before server responds
    // would call clearCache() again but the net expanded state change may be
    // batched away by React, leaving the cache empty with no request sent.
    if (stage.expandAllDebounceRef.current) return;
    stage.expandAllDebounceRef.current = true;
    setTimeout(() => {
      stage.expandAllDebounceRef.current = false;
    }, 800);
    if (stage.serverSide) {
      stage.captureExpansionScrollPosition();
      // Expanding/collapsing ALL rows changes every row index — full cache wipe.
      // (This path bypasses onExpandedChange so invalidateFromBlock won't run.)
      stage.clearCache();
      if (!shouldExpand && stage.viewMode === 'tree' && stage.treeHasDefaultExpansion) {
        const collapsedPaths = {};
        stage.table.getRowModel().rows.forEach(row => {
          if (row && row.id && row.getCanExpand() && !(row.original && row.original._isTotal)) {
            collapsedPaths[row.id] = false;
          }
        });
        stage.setExpandedWithHistory(collapsedPaths, 'layout:expand-all');
        return;
      }
      stage.setExpandedWithHistory(shouldExpand ? true : {}, 'layout:expand-all');
      return;
    }
    if (shouldExpand) {
      // Expand all rows by creating an object with all row IDs set to true
      const allRows = stage.table.getCoreRowModel().rows;
      const newExpanded = {};
      allRows.forEach(row => {
        // Only add rows that can be expanded and are not totals
        if (row.getCanExpand() && !(row.original && row.original._isTotal)) {
          newExpanded[row.id] = true;

          // Also expand sub-rows recursively
          const expandSubRows = subRows => {
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
      stage.setExpandedWithHistory(newExpanded, 'layout:expand-all');
    } else {
      // Collapse all by setting empty object
      stage.setExpandedWithHistory({}, 'layout:expand-all');
    }
  };
  stage.handleSortingChange = updater => {
    const rawNextSorting = typeof updater === 'function' ? updater(stage.sorting) : updater;
    const newSorting = normalizeSortingState(rawNextSorting, stage.sorting);
    stage.setSorting(newSorting);

    // Fire sort event to backend
    // In server-side mode, avoid a sorting-only Dash update. The sync effect
    // will send sorting and the runtimeRequest in one update.
    if (!stage.serverSide && stage.setPropsRef.current) {
      stage.setPropsRef.current({
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
  stage.table = useReactTable({
    data: stage.tableData,
    columns: stage.columns,
    state: stage.tableState,
    onSortingChange: updater => {
      stage.handleSortingChange(updater);
    },
    onExpandedChange: updater => {
      stage.captureExpansionScrollPosition();
      const usesExpandAllDefault = stage.expanded === true || stage.expanded && typeof stage.expanded === 'object' && stage.expanded[EXPAND_ALL_STATE_KEY] === true;
      let updaterBase = stage.expanded;
      if (usesExpandAllDefault && typeof updater === 'function') {
        updaterBase = {};
        const currentOverrides = stage.expanded && typeof stage.expanded === 'object' ? stage.expanded : {};
        Object.keys(stage.table.getRowModel().rowsById).forEach(rowId => {
          if (currentOverrides[rowId] !== false) updaterBase[rowId] = true;
        });
      }
      const rawExpanded = typeof updater === 'function' ? updater(updaterBase) : updater;
      let toggledExpandedPath = null;
      let normalizedRawExpanded = rawExpanded;
      if (usesExpandAllDefault && rawExpanded && typeof rawExpanded === 'object' && rawExpanded !== true) {
        const updaterKeys = new Set([...Object.keys(updaterBase || {}), ...Object.keys(rawExpanded)]);
        toggledExpandedPath = [...updaterKeys].find(key => !!(updaterBase && updaterBase[key]) !== !!rawExpanded[key]) || null;
        const previousOverrides = stage.expanded && typeof stage.expanded === 'object' ? stage.expanded : {};
        normalizedRawExpanded = {
          ...previousOverrides,
          [EXPAND_ALL_STATE_KEY]: true
        };
        if (toggledExpandedPath) {
          if (rawExpanded[toggledExpandedPath]) {
            delete normalizedRawExpanded[toggledExpandedPath];
          } else {
            normalizedRawExpanded[toggledExpandedPath] = false;
          }
        }
      }
      const newExpanded = stage.normalizeExpandedStateForTreeDefaults(normalizedRawExpanded, stage.expanded);
      if (stage.serverSide) {
        // Find which path was toggled so we know which block to defer-invalidate
        // after the expansion response lands (see pendingExpansionRef effect).
        // Value-diff: detect any key whose boolean value flipped (covers
        // both key-add/remove AND falseâ†’true / trueâ†’false toggles).
        const oldExp = stage.expanded && typeof stage.expanded === 'object' ? stage.expanded : {};
        const newExp = newExpanded || {};
        const allKeys = new Set([...Object.keys(oldExp), ...Object.keys(newExp)]);
        const changedPath = toggledExpandedPath || [...allKeys].find(key => key !== EXPAND_ALL_STATE_KEY && (Object.prototype.hasOwnProperty.call(oldExp, key) !== Object.prototype.hasOwnProperty.call(newExp, key) || !!oldExp[key] !== !!newExp[key]));
        if (changedPath) {
          const isNowExpanded = !!(newExpanded && newExpanded[changedPath]);
          stage.setPendingRowTransitions(prev => {
            const next = new Map(prev);
            next.set(changedPath, isNowExpanded ? 'expand' : 'collapse');
            while (next.size > MAX_PENDING_ROW_TRANSITIONS) {
              const oldestKey = next.keys().next().value;
              next.delete(oldestKey);
            }
            return next;
          });
        }

        // -1 signals "row not in viewport — do a full cache clear".
        let anchorBlock = -1;
        let expandedRowVirtualIndex = undefined;
        if (changedPath) {
          const toggledRow = stage.renderedData.find(r => r && r._path === changedPath);
          if (toggledRow && typeof toggledRow.__virtualIndex === 'number') {
            anchorBlock = Math.floor(toggledRow.__virtualIndex / stage.serverSideBlockSize);
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
        stage.pendingExpansionRef.current = {
          anchorBlock,
          expandedRowVirtualIndex,
          oldRowCount: stage.rowCount
        };
      }
      stage.setExpandedWithHistory(newExpanded, 'layout:expanded');
    },
    onColumnPinningChange: updater => {
      debugLog('onColumnPinningChange');
      stage.setColumnPinningWithHistory(updater, 'layout:column-pinning');
    },
    onRowPinningChange: updater => {
      debugLog('onRowPinningChange');
      stage.setRowPinningWithHistory(updater, 'layout:row-pinning');
    },
    onColumnVisibilityChange: updater => {
      debugLog('onColumnVisibilityChange');
      stage.setColumnVisibility(updater);
    },
    onColumnOrderChange: updater => {
      debugLog('onColumnOrderChange');
      stage.setColumnOrderWithHistory(previousOrder => {
        const baseOrder = mergeColumnOrderWithIds(previousOrder, stage.availableColumnOrderIds);
        const nextOrder = typeof updater === 'function' ? updater(baseOrder) : updater;
        return mergeColumnOrderWithIds(nextOrder, stage.availableColumnOrderIds);
      }, 'layout:column-order');
    },
    onColumnSizingChange: updater => {
      debugLog('onColumnSizingChange');
      stage.setColumnSizing(updater);
    },
    getRowId: stage.getRowId,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: stage.serverSide ? undefined : getExpandedRowModel(),
    getGroupedRowModel: stage.serverSide ? undefined : getGroupedRowModel(),
    getPaginationRowModel: stage.paginationConfig.enabled ? getPaginationRowModel() : undefined,
    onPaginationChange: stage.paginationConfig.enabled ? stage.setPaginationState : undefined,
    getSubRows: stage.getSubRows,
    enableRowPinning: true,
    // Enable Row Pinning
    enableColumnResizing: true,
    enableMultiSort: true,
    // Explicitly enable multi-sort
    columnResizeMode: 'onChange',
    manualPagination: stage.serverSide,
    manualSorting: stage.serverSide,
    manualFiltering: stage.serverSide,
    manualGrouping: stage.serverSide,
    manualExpanding: stage.serverSide,
    pageCount: stage.serverSide ? Math.ceil((stage.rowCount || 0) / 100) : undefined,
    getRowCanExpand: stage.getRowCanExpand,
    getIsRowExpanded: stage.getIsRowExpanded,
    enableColumnPinning: true
  });

  // The horizontal virtualizer, header groups, and sticky styles all consume
  // the table plus the viewport's scroll container. Keep this render-domain
  // projection in the viewport stage; workspace consumes its named outputs.
  ({
    columnVirtualizer: stage.columnVirtualizer,
    virtualCenterCols: stage.virtualCenterCols,
    beforeWidth: stage.beforeWidth,
    afterWidth: stage.afterWidth,
    totalLayoutWidth: stage.totalLayoutWidth,
    leftCols: stage.leftCols,
    rightCols: stage.rightCols,
    centerCols: stage.centerCols,
    preciseVisibleColRange: stage.preciseVisibleColRange
  } = useColumnVirtualizer({
    parentRef: stage.parentRef,
    table: stage.table,
    serverSide: stage.serverSide,
    totalCenterCols: stage.totalCenterCols,
    columnOverscan: stage.normalizedPerformanceConfig.columnOverscan,
    stateEpoch: stage.stateEpoch,
    estimateColumnWidth: stage.defaultColumnWidths.schemaFallback,
    onHorizontalScrollMetrics: stage.handleHorizontalScrollMetrics,
    onPreciseVisibleColRange: nextRange => stage.syncPreciseVisibleColRange(nextRange, {
      preserveWiderRange: stage.chartPanelOpen || stage.chartCanvasPanes.length > 0
    }),
    columnVisibility: stage.columnVisibility,
    columnPinning: stage.columnPinning,
    columnOrder: stage.columnOrderKey,
    columnSizing: stage.columnSizing,
    columns: stage.columns
  }));
  stage.columnVirtualizerRef.current = stage.columnVirtualizer;
  stage.pinnedColumnMetaRef.current = {
    leftCount: stage.leftCols.length,
    centerCount: stage.centerCols.length,
    rightCount: stage.rightCols.length
  };
  useLayoutEffect(() => {
    if (stage.columnVirtualizerRef.current && typeof stage.columnVirtualizerRef.current.measure === 'function') {
      stage.columnVirtualizerRef.current.measure();
    }
  }, [stage.columnOrderKey, stage.columnPinning, stage.columnSizing, stage.totalLayoutWidth]);

  stage.centerColIndexMap = useMemo(() => new Map(stage.centerCols.map((column, index) => [column.id, index])), [stage.centerCols]);
  stage.leftHeaderGroups = useMemo(() => stage.table.getLeftHeaderGroups(), [stage.columns, stage.columnVisibility, stage.columnPinning, stage.columnOrderKey, stage.columnSizing]);
  stage.rightHeaderGroups = useMemo(() => stage.table.getRightHeaderGroups(), [stage.columns, stage.columnVisibility, stage.columnPinning, stage.columnOrderKey, stage.columnSizing]);
  stage.centerHeaderGroups = useMemo(() => stage.table.getCenterHeaderGroups(), [stage.columns, stage.columnVisibility, stage.columnPinning, stage.columnOrderKey, stage.columnSizing]);
  useEffect(() => {
    if (!stage.serverSide || stage.virtualCenterCols.length === 0) return;
    debugLog('horizontal-scroll-state', {
      scrollLeft: stage.parentRef.current ? stage.parentRef.current.scrollLeft : null,
      visibleColRange: stage.visibleColRange,
      virtualRange: {
        start: stage.preciseVisibleColRange.start,
        end: stage.preciseVisibleColRange.end,
        count: Math.max(0, stage.preciseVisibleColRange.end - stage.preciseVisibleColRange.start + 1)
      },
      requestedColRange: {
        start: stage.colRequestStart,
        end: stage.colRequestEnd,
        needsColSchema: stage.needsColSchema
      },
      totalCenterCols: stage.totalCenterCols,
      pendingColumnSkeletonCount: stage.pendingColumnSkeletonCount,
      pendingHorizontalColumnCount: stage.pendingHorizontalColumnCount,
      showColumnLoadingSkeletons: stage.showColumnLoadingSkeletons,
      isHorizontalColumnRequestPending: stage.isHorizontalColumnRequestPending,
      isRequestPending: stage.isRequestPending,
      structuralInFlight: stage.structuralInFlight
    });
  }, [stage.serverSide, stage.virtualCenterCols, stage.preciseVisibleColRange.start, stage.preciseVisibleColRange.end, stage.visibleColRange, stage.colRequestStart, stage.colRequestEnd, stage.needsColSchema, stage.totalCenterCols, stage.pendingColumnSkeletonCount, stage.pendingHorizontalColumnCount, stage.showColumnLoadingSkeletons, stage.isHorizontalColumnRequestPending, stage.isRequestPending, stage.structuralInFlight, stage.parentRef]);

  ({
    getHeaderStickyStyle: stage.getHeaderStickyStyle,
    getStickyStyle: stage.getStickyStyle
  } = useStickyStyles(stage.theme, stage.leftCols, stage.rightCols));

  const selectionFormatting = useRuntimeSelectionFormatting({
    getDisplayRows: stage.getDisplayRows,
    maxAutoSizeSampleRows: MAX_AUTO_SIZE_SAMPLE_ROWS,
    normalizedInitialValConfigs: stage.normalizedInitialValConfigs,
    selectedCells: stage.selectedCells,
    setColorScaleMode: stage.setColorScaleMode,
    setDataBarsColumns: stage.setDataBarsColumns,
    setValConfigs: stage.setValConfigs,
    showNotification: stage.showNotification,
    table: stage.table,
    valConfigs: stage.valConfigs,
  });
  for (const [key, value] of Object.entries(selectionFormatting)) stage[key] = value;
  const columnMeasurement = useRuntimeColumnMeasurement({
    autoSizeBounds: stage.autoSizeBounds,
    id: stage.id,
    parentRef: stage.parentRef,
    table: stage.table,
  });
  for (const [key, value] of Object.entries(columnMeasurement)) stage[key] = value;
  const columnAutoSizing = useRuntimeColumnAutoSizing({
    autoSizeBounds: stage.autoSizeBounds,
    columnDecimalOverrides: stage.columnDecimalOverrides,
    columnFormatOverrides: stage.columnFormatOverrides,
    columnGroupSeparatorOverrides: stage.columnGroupSeparatorOverrides,
    decimalPlaces: stage.decimalPlaces,
    defaultValueFormat: stage.defaultValueFormat,
    fontFamily: stage.fontFamily,
    fontSize: stage.fontSize,
    getAutoSizeSampleRows: stage.getAutoSizeSampleRows,
    getDisplayHeaderTextStackForColumn: stage.getDisplayHeaderTextStackForColumn,
    getRenderedCellWidthForColumn: stage.getRenderedCellWidthForColumn,
    getRenderedHeaderWidthForColumn: stage.getRenderedHeaderWidthForColumn,
    matchesSelectedColumnToValueConfig: stage.matchesSelectedColumnToValueConfig,
    numberGroupSeparator: stage.numberGroupSeparator,
    queueHeaderFitValidation: stage.queueHeaderFitValidation,
    table: stage.table,
    valConfigs: stage.valConfigs,
  });
  for (const [key, value] of Object.entries(columnAutoSizing)) stage[key] = value;
  const clipboardExportPresentation = useRuntimeClipboardExportPresentation({
    cachedColSchema: stage.cachedColSchema,
    cellFormatRules: stage.cellFormatRules,
    colorPalette: stage.colorPalette,
    colorScaleMode: stage.colorScaleMode,
    colorScaleStats: stage.colorScaleStats,
    columnDecimalOverrides: stage.columnDecimalOverrides,
    columnFormatOverrides: stage.columnFormatOverrides,
    columnGroupSeparatorOverrides: stage.columnGroupSeparatorOverrides,
    conditionalFormatting: stage.conditionalFormatting,
    decimalPlaces: stage.decimalPlaces,
    defaultValueFormat: stage.defaultValueFormat,
    fontFamily: stage.fontFamily,
    fontSize: stage.fontSize,
    getConditionalStyle: stage.getConditionalStyle,
    getDisplayHeaderTextForColumn: stage.getDisplayHeaderTextForColumn,
    getDisplayHeaderTextStackForColumn: stage.getDisplayHeaderTextStackForColumn,
    getDisplayRows: stage.getDisplayRows,
    HIERARCHY_INDENT_PX: stage.HIERARCHY_INDENT_PX,
    headerFormatting: stage.headerFormatting,
    loadedRows: stage.loadedRows,
    matchesSelectedColumnToValueConfig: stage.matchesSelectedColumnToValueConfig,
    mergedHeaderSeparator: stage.mergedHeaderSeparator,
    numberGroupSeparator: stage.numberGroupSeparator,
    resolveDisplayedCellValue: stage.resolveDisplayedCellValue,
    rowHeight: stage.rowHeight,
    selectedCells: stage.selectedCells,
    serverSide: stage.serverSide,
    showNotification: stage.showNotification,
    table: stage.table,
    theme: stage.theme,
    totalCenterCols: stage.totalCenterCols,
    valConfigs: stage.valConfigs,
  });
  for (const [key, value] of Object.entries(clipboardExportPresentation)) stage[key] = value;
  const exportCommands = createRuntimeExportCommands({
    abortGenerationRef: stage.abortGenerationRef,
    activeExportRequestIdRef: stage.activeExportRequestIdRef,
    buildExportStyleProfile: stage.buildExportStyleProfile,
    cancelledExportRequestIdsRef: stage.cancelledExportRequestIdsRef,
    clientInstanceRef: stage.clientInstanceRef,
    COPY_ROW_LIMIT: stage.COPY_ROW_LIMIT,
    dispatchServerSideRuntimeSetProps: stage.dispatchServerSideRuntimeSetProps,
    EXPORT_ROW_LIMIT: stage.EXPORT_ROW_LIMIT,
    exportRequestSequenceRef: stage.exportRequestSequenceRef,
    getDisplayHeaderTextForColumn: stage.getDisplayHeaderTextForColumn,
    getDisplayHeaderTextStackForColumn: stage.getDisplayHeaderTextStackForColumn,
    getDisplayRows: stage.getDisplayRows,
    getExportCellDisplayValue: stage.getExportCellDisplayValue,
    getExportColumns: stage.getExportColumns,
    getExportHeaderLabel: stage.getExportHeaderLabel,
    getExportRawCellValue: stage.getExportRawCellValue,
    getServerExportColumnTarget: stage.getServerExportColumnTarget,
    layoutMode: stage.layoutMode,
    mergedHeaderSeparator: stage.mergedHeaderSeparator,
    partModal: stage.partModal,
    pendingCopyIntentRef: stage.pendingCopyIntentRef,
    rowCount: stage.rowCount,
    serverSide: stage.serverSide,
    setExportProgress: stage.setExportProgress,
    setIsCopying: stage.setIsCopying,
    setIsExporting: stage.setIsExporting,
    setPartModal: stage.setPartModal,
    table: stage.table,
    tableName: stage.tableName,
    theme: stage.theme,
  });
  stage.buildServerExportPayload = exportCommands.buildServerExportPayload;
  stage.cancelActiveExportRequest = exportCommands.cancelActiveExportRequest;
  stage.copyAllRecordsToClipboard = exportCommands.copyAllRecordsToClipboard;
  stage.copyCurrentTableToClipboard = exportCommands.copyCurrentTableToClipboard;
  stage.dispatchExportPart = exportCommands.dispatchExportPart;
  stage.dispatchServerExportRequest = exportCommands.dispatchServerExportRequest;
  stage.exportPivot = exportCommands.exportPivot;
  stage.getServerExportStyleIds = exportCommands.getServerExportStyleIds;
  const columnCommands = useRuntimeColumnCommands({
    colFields: stage.colFields,
    columnPinning: stage.columnPinning,
    columnVirtualizerRef: stage.columnVirtualizerRef,
    effectiveSortOptions: stage.effectiveSortOptions,
    parentRef: stage.parentRef,
    resetVisibleColRange: stage.resetVisibleColRange,
    rowFields: stage.rowFields,
    serverSide: stage.serverSide,
    setPivotColumnSorting: stage.setPivotColumnSorting,
    showNotification: stage.showNotification,
    visibleColRange: stage.visibleColRange,
  });
  for (const [key, value] of Object.entries(columnCommands)) stage[key] = value;

  const cellSelection = useRuntimeCellSelection({
    buildSelectedClipboardPayload: stage.buildSelectedClipboardPayload,
    copyRichToClipboard: stage.copyRichToClipboard,
    copyCurrentTableToClipboard: stage.copyCurrentTableToClipboard,
    dispatchBatchUpdateRequest: stage.dispatchBatchUpdateRequest,
    dragStart: stage.dragStart,
    fillRange: stage.fillRange,
    getDisplayRows: stage.getDisplayRows,
    handleRedo: stage.handleRedo,
    handleUndo: stage.handleUndo,
    isDragging: stage.isDragging,
    isFilling: stage.isFilling,
    isSelectAllServerRef: stage.isSelectAllServerRef,
    lastSelected: stage.lastSelected,
    loadedRows: stage.loadedRows,
    scrollToDisplayColumn: stage.scrollToDisplayColumn,
    scrollToDisplayRow: stage.scrollToDisplayRow,
    selectedCells: stage.selectedCells,
    serverSide: stage.serverSide,
    setDragStart: stage.setDragStart,
    setFillRange: stage.setFillRange,
    setHoveredRowPath: stage.setHoveredRowPath,
    setIsDragging: stage.setIsDragging,
    setIsFilling: stage.setIsFilling,
    setLastSelected: stage.setLastSelected,
    setSelectedCells: stage.setSelectedCells,
    setProps: stage.setProps,
    table: stage.table,
    tableRef: stage.tableRef,
  });
  for (const [key, value] of Object.entries(cellSelection)) stage[key] = value;
  useRuntimeClipboardPaste({
    dispatchBatchUpdateRequest: stage.dispatchBatchUpdateRequest,
    getDisplayRows: stage.getDisplayRows,
    lastSelected: stage.lastSelected,
    tableRef: stage.tableRef,
  });

  // Update the ref with the current table instance
  useEffect(() => {
    stage.tableRef.current = stage.table;
  }, [stage.table]);

  // Accessibility & Event System Effect for Sorting
  useEffect(() => {
    if (stage.sorting.length > 0) {
      const sortDesc = stage.sorting[0].desc ? 'descending' : 'ascending';
      const colId = stage.sorting[0].id;
      const col = stage.tableRef.current.getColumn(colId);
      const colName = col ? typeof col.columnDef.header === 'string' ? col.columnDef.header : colId : colId;
      stage.setAnnouncement(`Sorted by ${colName} ${sortDesc}`);
    } else {
      stage.setAnnouncement("Sorting cleared");
    }

    // Fire sort event
    if (stage.setPropsRef.current) {
      stage.setPropsRef.current({
        sortEvent: {
          type: 'change',
          status: 'applied',
          sorting: stage.sorting,
          timestamp: Date.now()
        }
      });
    }
  }, [stage.sorting]); // Removed table and setProps from dependencies
  stage.activeFilterOptions = useMemo(() => {
    if (!stage.activeFilterCol) return [];
    if (stage.filterOptions[stage.activeFilterCol]) return stage.filterOptions[stage.activeFilterCol];
    if (!stage.serverSide && stage.clientFilterOptionMap[stage.activeFilterCol]) return stage.clientFilterOptionMap[stage.activeFilterCol];
    const col = stage.table.getColumn(stage.activeFilterCol);
    if (!col) return [];
    const unique = new Set();
    const rows = stage.table.getCoreRowModel().rows;
    rows.forEach(row => {
      const val = row.getValue(stage.activeFilterCol);
      if (val !== null && val !== undefined) unique.add(val);
    });
    return Array.from(unique).sort(compareFilterOptionValues);
  }, [stage.activeFilterCol, stage.clientFilterOptionMap, stage.filterOptions, stage.serverSide, stage.table]);
  ({
    rows: stage.rows
  } = stage.table.getRowModel());
  stage.topRows = [];
  stage.bottomRows = [];
  stage.centerRows = stage.rows;
  try {
    stage.topRows = stage.table.getTopRows();
    stage.bottomRows = stage.table.getBottomRows();
    stage.centerRows = stage.table.getCenterRows();
  } catch (error) {
    if (process.env.NODE_ENV !== 'production') {
      console.warn('[pivot-client] row pinning fallback', {
        error,
        rowPinning: stage.tableState.rowPinning,
        tableDataSize: stage.tableData.length
      });
    }
    const rowLookup = new Map(stage.rows.map(row => [row.id, row]));
    stage.topRows = (stage.tableState.rowPinning && stage.tableState.rowPinning.top || []).map(id => rowLookup.get(id)).filter(Boolean);
    stage.bottomRows = (stage.tableState.rowPinning && stage.tableState.rowPinning.bottom || []).map(id => rowLookup.get(id)).filter(Boolean);
    const pinnedIds = new Set([...stage.topRows, ...stage.bottomRows].map(row => row.id));
    stage.centerRows = stage.rows.filter(row => !pinnedIds.has(row.id));
  }
  stage.lastStableRowModelRef = useRef({
    topRows: [],
    centerRows: [],
    bottomRows: []
  });
  stage.hasRenderedData = stage.renderedData.some(Boolean);
  useEffect(() => {
    if (!stage.serverSide) return;
    if (stage.centerRows.length > 0 || stage.topRows.length > 0 || stage.bottomRows.length > 0) {
      stage.lastStableRowModelRef.current = {
        topRows: stage.topRows,
        centerRows: stage.centerRows,
        bottomRows: stage.bottomRows
      };
    }
  }, [stage.serverSide, stage.topRows, stage.centerRows, stage.bottomRows]);
  useEffect(() => {
    [...stage.topRows, ...stage.centerRows, ...stage.bottomRows].forEach(row => {
      if (row && row.id) {
        stage.pinnedRowCacheRef.current.set(row.id, row);
      }
    });
  }, [stage.topRows, stage.centerRows, stage.bottomRows]);
  useEffect(() => {
    if (!stage.serverSide) return;
    stage.lastStableRowModelRef.current = {
      topRows: [],
      centerRows: [],
      bottomRows: []
    };
    // Expansion should still refetch server-side data, but it should not force the viewport back to the top.
    stage.expansionScrollRestoreRef.current = null;
    if (stage.expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
      cancelAnimationFrame(stage.expansionScrollRestoreRafRef.current);
      stage.expansionScrollRestoreRafRef.current = null;
    }
    if (stage.parentRef.current) {
      stage.parentRef.current.scrollTop = 0;
    }
  }, [stage.serverSide, stage.serverSideViewportResetKey, stage.parentRef]);
  useLayoutEffect(() => {
    if (!stage.serverSide || stage.expansionScrollRestoreRef.current === null || !stage.parentRef.current) return;
    if (!stage.hasRenderedData && stage.centerRows.length === 0 && stage.topRows.length === 0 && stage.bottomRows.length === 0) return;
    const restoreTarget = stage.expansionScrollRestoreRef.current;
    if (!restoreTarget) return;
    const applyScrollRestore = () => {
      if (!stage.parentRef.current || !stage.expansionScrollRestoreRef.current) return;
      const nextTarget = stage.expansionScrollRestoreRef.current;
      const targetScrollTop = nextTarget.scrollTop;
      if (stage.rowVirtualizer.scrollToOffset) {
        stage.rowVirtualizer.scrollToOffset(targetScrollTop);
      }
      if (Math.abs(stage.parentRef.current.scrollTop - targetScrollTop) > 1) {
        stage.parentRef.current.scrollTop = targetScrollTop;
      }
      if (nextTarget.restorePassesRemaining <= 1) {
        stage.expansionScrollRestoreRef.current = null;
        stage.expansionScrollRestoreRafRef.current = null;
        return;
      }
      stage.expansionScrollRestoreRef.current = {
        ...nextTarget,
        restorePassesRemaining: nextTarget.restorePassesRemaining - 1
      };
      if (typeof requestAnimationFrame === 'function') {
        stage.expansionScrollRestoreRafRef.current = requestAnimationFrame(applyScrollRestore);
      } else {
        applyScrollRestore();
      }
    };
    applyScrollRestore();
    return () => {
      if (stage.expansionScrollRestoreRafRef.current !== null && typeof cancelAnimationFrame === 'function') {
        cancelAnimationFrame(stage.expansionScrollRestoreRafRef.current);
        stage.expansionScrollRestoreRafRef.current = null;
      }
    };
  }, [stage.serverSide, stage.hasRenderedData, stage.centerRows.length, stage.topRows.length, stage.bottomRows.length, stage.renderedOffset, stage.dataVersion, stage.rowVirtualizer]);
  stage.appliedRowPinning = stage.tableState.rowPinning || {
    top: [],
    bottom: []
  };
  stage.resolvedTopRows = useMemo(() => {
    if (!(stage.appliedRowPinning.top && stage.appliedRowPinning.top.length > 0)) return stage.topRows;
    return stage.appliedRowPinning.top.map(id => stage.topRows.find(row => row && row.id === id) || stage.centerRows.find(row => row && row.id === id) || stage.bottomRows.find(row => row && row.id === id) || stage.pinnedRowCacheRef.current.get(id)).filter(Boolean);
  }, [stage.appliedRowPinning.top, stage.topRows, stage.centerRows, stage.bottomRows]);
  stage.resolvedBottomRows = useMemo(() => {
    if (!(stage.appliedRowPinning.bottom && stage.appliedRowPinning.bottom.length > 0)) return stage.bottomRows;
    return stage.appliedRowPinning.bottom.map(id => stage.bottomRows.find(row => row && row.id === id) || stage.centerRows.find(row => row && row.id === id) || stage.topRows.find(row => row && row.id === id) || stage.pinnedRowCacheRef.current.get(id)).filter(Boolean);
  }, [stage.appliedRowPinning.bottom, stage.topRows, stage.centerRows, stage.bottomRows]);
  stage.effectiveTopRows = stage.serverSide && stage.hasRenderedData && stage.resolvedTopRows.length === 0 && stage.centerRows.length === 0 ? stage.lastStableRowModelRef.current.topRows : stage.resolvedTopRows;
  stage.effectiveCenterRows = stage.serverSide && stage.hasRenderedData && stage.centerRows.length === 0 ? stage.lastStableRowModelRef.current.centerRows : stage.centerRows;
  stage.effectiveBottomRows = stage.serverSide && stage.hasRenderedData && stage.centerRows.length === 0 && stage.resolvedBottomRows.length === 0 ? stage.lastStableRowModelRef.current.bottomRows : stage.resolvedBottomRows;
  stage.clientRowVirtualizer = useVirtualizer({
    count: stage.effectiveCenterRows.length,
    getScrollElement: () => stage.parentRef.current,
    estimateSize: () => stage.rowHeight,
    overscan: 12
  });
  stage.activeRowVirtualizer = stage.serverSide ? stage.rowVirtualizer : stage.clientRowVirtualizer;
  stage.activeRowVirtualizerRef.current = stage.activeRowVirtualizer;
  useLayoutEffect(() => {
    if (stage.serverSide) return;
    stage.activeRowVirtualizer.measure();
  }, [stage.serverSide, stage.activeRowVirtualizer, stage.effectiveCenterRows.length, stage.rowHeight]);
  stage.rowModelLookup = useMemo(() => {
    const lookup = new Map();
    [...stage.effectiveTopRows, ...stage.effectiveCenterRows, ...stage.effectiveBottomRows].forEach(row => {
      if (row && row.id) {
        lookup.set(row.id, row);
      }
    });
    return lookup;
  }, [stage.effectiveTopRows, stage.effectiveCenterRows, stage.effectiveBottomRows]);
  useEffect(() => {
    const orderedRows = [...stage.effectiveTopRows, ...stage.effectiveCenterRows, ...stage.effectiveBottomRows];
    stage.displayRowsRef.current = orderedRows;
    stage.displayRowIndexRef.current = new Map(orderedRows.filter(row => row && row.id).map((row, index) => [row.id, index]));
    stage.pinnedDisplayMetaRef.current = {
      topCount: stage.effectiveTopRows.length,
      centerCount: stage.effectiveCenterRows.length
    };
  }, [stage.effectiveTopRows, stage.effectiveCenterRows, stage.effectiveBottomRows]);

  // Progressive subtree expansion: each time the backend returns new data,
  // scan it for descendants of the target path that still have children and
  // haven't been expanded yet. Auto-expand them and let the cycle continue
  // until every reachable descendant is expanded.
  useEffect(() => {
    if (!stage.serverSide || !stage.data || !stage.subtreeExpandRef.current) return;
    const {
      path,
      expandedPaths
    } = stage.subtreeExpandRef.current;
    const toExpand = stage.data.filter(row => {
      if (!row || !row._path || !row._has_children) return false;
      const inSubtree = row._path === path || row._path.startsWith(path + '|||');
      return inSubtree && !expandedPaths.has(row._path);
    });
    if (toExpand.length === 0) {
      stage.subtreeExpandRef.current = null;
      return;
    }
    toExpand.forEach(row => expandedPaths.add(row._path));
    stage.captureExpansionScrollPosition();
    stage.clearCache();
    stage.setExpanded(prev => {
      const base = prev !== null && typeof prev === 'object' ? prev : {};
      const next = {
        ...base
      };
      toExpand.forEach(row => {
        next[row._path] = true;
      });
      return next;
    });
  }, [stage.data, stage.serverSide]); // intentionally omits captureExpansionScrollPosition/clearCache/setExpanded — stable refs

  // Deferred block invalidation after single-row expansion.
  // We wait for the expansion response to land (dataVersion bumps) before
  // invalidating subsequent blocks. This ensures only ONE backend request fires
  // (the expanded sync), with no concurrent viewport request to race against it.
  // After the anchor block is updated with fresh data, blocks beyond it are
  // deleted so they get re-fetched on next scroll (their row indices shifted).
  useEffect(() => {
    if (!stage.serverSide || !stage.pendingExpansionRef.current) return;
    const {
      anchorBlock,
      expandedRowVirtualIndex,
      oldRowCount,
      extendedToBlock = -1
    } = stage.pendingExpansionRef.current;
    stage.pendingExpansionRef.current = null;
    if (anchorBlock < 0) {
      // The expanded row was not in the viewport when the user toggled it.
      // We cannot know which cached index first shifted, so remove every
      // cached block. Keeping those rows during revalidation paints values
      // from the previous hierarchy at their now-invalid virtual indexes.
      if (stage.invalidateFromBlock) stage.invalidateFromBlock(0);
    } else {
      // The expansion request was extended to cover through extendedToBlock
      // (anchorBlock + 1 when the anchor is known).  Those blocks were filled
      // with fresh data by the data-sync effect, so retain them and remove
      // every later block whose virtual row indexes may have shifted.
      const firstStaleBlock = extendedToBlock >= 0 ? extendedToBlock + 1 : anchorBlock + 1;
      if (stage.invalidateFromBlock) stage.invalidateFromBlock(firstStaleBlock);
    }
    // Clear the transition loader now that the expansion response has landed.
    stage.setPendingRowTransitions(new Map());

    // Viewport anchor preservation.
    // When rows are inserted or removed ABOVE the current scroll position, the
    // virtualizer re-layouts and the same pixel offset now shows a different
    // logical row. Compensate by shifting scrollTop so that the user continues
    // to see the same rows they were looking at before the toggle.
    if (stage.parentRef.current && typeof expandedRowVirtualIndex === 'number' && typeof oldRowCount === 'number' && stage.rowHeight > 0) {
      const rowDelta = (stage.rowCount || 0) - (oldRowCount || 0);
      if (rowDelta !== 0) {
        // Y position of the expanded/collapsed row (uniform row heights).
        const expandedRowY = expandedRowVirtualIndex * stage.rowHeight;
        const currentScrollTop = stage.parentRef.current.scrollTop;
        // Only compensate when the anchor row is entirely ABOVE the viewport.
        // If it is at or inside the viewport the inserted children appear
        // naturally below it and no scroll adjustment is needed.
        if (expandedRowY + stage.rowHeight <= currentScrollTop) {
          const newScrollTop = Math.max(0, currentScrollTop + rowDelta * stage.rowHeight);
          stage.parentRef.current.scrollTop = newScrollTop;
          if (stage.rowVirtualizer.scrollToOffset) {
            stage.rowVirtualizer.scrollToOffset(newScrollTop);
          }
        }
      }
    }
  }, [stage.dataVersion, stage.serverSide, stage.rowCount, stage.rowHeight, stage.parentRef, stage.rowVirtualizer]); // fires when expansion response arrives

  // Debug effect removed (finding #10 — hot-path logging).
  const chartModels = useRuntimeChartModels({
    buildChartRequestBase: stage.buildChartRequestBase,
    buildChartStateOverrideSnapshot: stage.buildChartStateOverrideSnapshot,
    chartCanvasPanes: stage.chartCanvasPanes,
    chartPaneDataById: stage.chartPaneDataById,
    chartPanelColumnLimit: stage.chartPanelColumnLimit,
    chartPanelHierarchyLevel: stage.chartPanelHierarchyLevel,
    chartPanelInteractionMode: stage.chartPanelInteractionMode,
    chartPanelLayers: stage.chartPanelLayers,
    chartPanelLocked: stage.chartPanelLocked,
    chartPanelLockedModel: stage.chartPanelLockedModel,
    chartPanelLockedRequest: stage.chartPanelLockedRequest,
    chartPanelOpen: stage.chartPanelOpen,
    chartPanelOrientation: stage.chartPanelOrientation,
    chartPanelRowLimit: stage.chartPanelRowLimit,
    chartPanelServerScope: stage.chartPanelServerScope,
    chartPanelSource: stage.chartPanelSource,
    chartPanelSortMode: stage.chartPanelSortMode,
    chartPanelType: stage.chartPanelType,
    chartModal: stage.chartModal,
    colFields: stage.colFields,
    columnOrderKey: stage.columnOrderKey,
    columnPinning: stage.columnPinning,
    columnSizing: stage.columnSizing,
    columnVisibility: stage.columnVisibility,
    columns: stage.columns,
    effectiveBottomRows: stage.effectiveBottomRows,
    effectiveCenterRows: stage.effectiveCenterRows,
    effectiveTopRows: stage.effectiveTopRows,
    getDisplayRows: stage.getDisplayRows,
    handleRowSelect: stage.handleRowSelect,
    loadedRows: stage.loadedRows,
    resolveDisplayRowIndex: stage.resolveDisplayRowIndex,
    rowFields: stage.rowFields,
    scrollToDisplayColumn: stage.scrollToDisplayColumn,
    scrollToDisplayRow: stage.scrollToDisplayRow,
    selectedCells: stage.selectedCells,
    serverSide: stage.serverSide,
    setChartPanelLocked: stage.setChartPanelLocked,
    setChartPanelLockedModel: stage.setChartPanelLockedModel,
    setChartPanelLockedRequest: stage.setChartPanelLockedRequest,
    setFilters: stage.setFilters,
    setPropsRef: stage.setPropsRef,
    showNotification: stage.showNotification,
    table: stage.table,
    updateChartCanvasPane: stage.updateChartCanvasPane,
  });
  stage.chartModelingActive = chartModels.chartModelingActive;
  stage.visibleLeafColumns = chartModels.visibleLeafColumns;
  stage.chartDisplayRows = chartModels.chartDisplayRows;
  stage.liveSelectionChartModel = chartModels.liveSelectionChartModel;
  stage.livePivotChartModel = chartModels.livePivotChartModel;
  stage.overlayChartData = chartModels.overlayChartData;
  stage.activeChartPanelModel = chartModels.activeChartPanelModel;
  stage.chartCanvasPaneModels = chartModels.chartCanvasPaneModels;
  stage.handleToggleChartPanelLock = chartModels.handleToggleChartPanelLock;
  stage.handleToggleChartCanvasPaneLock = chartModels.handleToggleChartCanvasPaneLock;
  stage.applyChartTargetFilters = chartModels.applyChartTargetFilters;
  stage.activateChartCategory = chartModels.activateChartCategory;
  stage.handleChartCategoryActivate = chartModels.handleChartCategoryActivate;
  return Object.freeze(stage);
}

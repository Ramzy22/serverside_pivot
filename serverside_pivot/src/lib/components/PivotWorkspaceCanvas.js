import React from 'react';

import {
  CHART_PANEL_VERTICAL_CHROME_HEIGHT,
  DEFAULT_CHART_GRAPH_HEIGHT,
  DEFAULT_DOCKED_CHART_PANE_HEIGHT,
  MIN_CHART_CANVAS_PANE_WIDTH,
  MIN_TABLE_PANEL_WIDTH,
} from '../hooks/usePivotNormalization';
import { ChartWorkspace } from './Charts/ChartWorkspace';
import { PaginationBar } from './PivotSmallComponents';
import { PivotTableBody } from './Table/PivotTableBody';
import { RuntimeDataError } from './RuntimeDataError';

/** @type {any} */
const LazyDetailDrawer = React.lazy(() => import(/* webpackChunkName: "pivot-detail-drawer" */ './Table/DetailDrawer'));
/** @type {any} */
const LazyDetailSidePanel = React.lazy(() => import(/* webpackChunkName: "pivot-detail-sidepanel" */ './Table/DetailSidePanel'));
/** @type {any} */
const LazyEditSidePanel = React.lazy(() => import(/* webpackChunkName: "pivot-edit-panel" */ './Table/EditSidePanel'));

/** Owns the chart/table canvas presentation and its adjacent detail surfaces.  @param {any} p0 @returns {any} */
export function PivotWorkspaceCanvas({ canvas }) {
  const {
    activeColumnSkeletonCount,
    activeDetailDisplayMode,
    activeRowVirtualizer,
    afterWidth,
    beforeWidth,
    bodyRowsTopOffset,
    centerColIndexMap,
    centerCols,
    centerHeaderGroups,
    chartCanvasLayoutRef,
    chartLayoutRef,
    chartCanvasPaneWidthHints,
    columnSkeletonWidth,
    detailConfig,
    detailSurface,
    dockedChartCanvasPanesByPosition,
    editPanelCells,
    editPanelEnabled,
    editPanelOpen,
    editValueDisplayMode,
    effectiveBottomRows,
    effectiveCenterRows,
    effectiveTopRows,
    floatingChartCanvasPanes,
    getRow,
    grandTotalPosition,
    handleCancelPropagation,
    handleConfirmPropagation,
    handleDetailClose,
    handleDetailFilter,
    handleDetailPageChange,
    handleDetailSort,
    handleHeaderFilter,
    handleKeyDown,
    handlePropagationMethodChange,
    handleReapplyPropagation,
    handleRevertAll,
    handleRevertCell,
    handleRevertSelected,
    handleStartChartCanvasResize,
    handleStartChartCanvasVerticalResize,
    immersiveMode,
    isRequestPending,
    runtimeDataError,
    retryRuntimeData,
    layoutMode,
    leftCols,
    leftHeaderGroups,
    paginationConfig,
    paginationState,
    parentRef,
    pendingPropagationUpdates,
    pendingRowTransitions,
    propagationLogRef,
    propagationMethodUI,
    renderCell,
    renderChartCanvasPane,
    renderChartWorkspaceModal,
    renderFloatingChartCanvasPane,
    renderHeaderCell,
    renderVirtualColumnCell,
    rightCols,
    rightHeaderGroups,
    rowHeight,
    rowModelLookup,
    rows,
    selectedCells,
    serverSide,
    serverSidePinsGrandTotal,
    setDetailSurface,
    setEditPanelOpen,
    setEditValueDisplayMode,
    setPaginationState,
    showColumnLoadingSkeletons,
    showSubtotals,
    statusAccessoryActions,
    statusAccessoryModel,
    statusRowCount,
    stickyHeaderHeight,
    table,
    tableCanvasSize,
    theme,
    totalCenterCols,
    totalLayoutWidth,
    treeConfig,
    uiConfig,
    viewMode,
    virtualCenterCols,
    virtualRows,
    visibleLeafColumns,
  } = canvas;

  return <ChartWorkspace
    chartCanvasLayoutRef={chartCanvasLayoutRef}
    chartLayoutRef={chartLayoutRef}
    showCharts={uiConfig.showCharts}
    dockedPanesByPosition={dockedChartCanvasPanesByPosition}
    floatingPanes={floatingChartCanvasPanes}
    paneWidthHints={chartCanvasPaneWidthHints}
    tableCanvasSize={tableCanvasSize}
    minTablePanelWidth={MIN_TABLE_PANEL_WIDTH}
    minChartPaneWidth={MIN_CHART_CANVAS_PANE_WIDTH}
    defaultDockedPaneHeight={DEFAULT_DOCKED_CHART_PANE_HEIGHT}
    defaultChartGraphHeight={DEFAULT_CHART_GRAPH_HEIGHT}
    chartPanelVerticalChromeHeight={CHART_PANEL_VERTICAL_CHROME_HEIGHT}
    theme={theme}
    renderChartPane={renderChartCanvasPane}
    renderFloatingPane={renderFloatingChartCanvasPane}
    renderModal={renderChartWorkspaceModal}
    tableAdjacentContent={<>
      {activeDetailDisplayMode === 'sidepanel' && detailSurface && (
        <React.Suspense fallback={null}>
          <LazyDetailSidePanel
          detailState={detailSurface}
          onClose={() => setDetailSurface(null)}
          onPageChange={handleDetailPageChange}
          onSort={handleDetailSort}
          onFilter={handleDetailFilter}
          theme={theme}
          width={detailConfig.sidepanelWidth}
          />
        </React.Suspense>
      )}
      {editPanelEnabled && ((editPanelOpen && editPanelCells.length > 0) || pendingPropagationUpdates) ? (
        <React.Suspense fallback={null}>
          <LazyEditSidePanel
          editedCells={editPanelCells}
          propagationLog={propagationLogRef.current}
          onReapplyPropagation={handleReapplyPropagation}
          onRevertCell={handleRevertCell}
          onRevertSelected={handleRevertSelected}
          onRevertAll={handleRevertAll}
          onToggleDisplayMode={() => setEditValueDisplayMode(/** @param {any} [previous] @returns {any} */ previous => previous === 'original' ? 'edited' : 'original')}
          displayMode={editValueDisplayMode}
          onClose={() => { setEditPanelOpen(false); handleCancelPropagation(); }}
          theme={theme}
          width={320}
          pendingPropagation={pendingPropagationUpdates}
          propagationMethod={propagationMethodUI}
          onPropagationMethodChange={handlePropagationMethodChange}
          onConfirmPropagation={handleConfirmPropagation}
          onCancelPropagation={handleCancelPropagation}
          />
        </React.Suspense>
      ) : null}
    </>}
    onHorizontalResizeStart={handleStartChartCanvasResize}
    onVerticalResizeStart={handleStartChartCanvasVerticalResize}
  >
    <div style={{ display: 'flex', flex: 1, minWidth: 0, minHeight: 0, overflow: 'hidden', flexDirection: 'column' }}>
      <RuntimeDataError error={runtimeDataError} onRetry={retryRuntimeData} theme={theme} />
    <div style={{ display: 'flex', flex: 1, minWidth: 0, minHeight: 0, overflow: 'hidden' }}>
      <PivotTableBody
        key={`pivot-body-${editValueDisplayMode}-${layoutMode}-${showSubtotals ? 'with-subtotals' : 'no-subtotals'}`}
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
        rowVirtualizer={activeRowVirtualizer}
        rowModelLookup={rowModelLookup}
        getRow={getRow}
        serverSide={serverSide}
        serverSidePinsGrandTotal={serverSidePinsGrandTotal}
        pendingRowTransitions={pendingRowTransitions}
        leftHeaderGroups={leftHeaderGroups}
        centerHeaderGroups={centerHeaderGroups}
        rightHeaderGroups={rightHeaderGroups}
        leftCols={leftCols}
        centerCols={centerCols}
        rightCols={rightCols}
        centerColIndexMap={centerColIndexMap}
        totalCenterColumns={totalCenterCols}
        rowHeight={rowHeight}
        headerHeight={canvas.headerHeight || rowHeight}
        layoutMode={layoutMode}
        grandTotalPosition={grandTotalPosition}
        showColumnLoadingSkeletons={showColumnLoadingSkeletons}
        pendingColumnSkeletonCount={activeColumnSkeletonCount}
        columnSkeletonWidth={columnSkeletonWidth}
        renderCell={renderCell}
        renderVirtualColumnCell={renderVirtualColumnCell}
        renderHeaderCell={renderHeaderCell}
        handleHeaderFilter={handleHeaderFilter}
        selectedCells={selectedCells}
        rowCount={statusRowCount}
        isRequestPending={isRequestPending}
        statusModel={statusAccessoryModel}
        statusActions={statusAccessoryActions}
        treeSuppressGroupRowsSticky={Boolean(viewMode === 'tree' && treeConfig && treeConfig.suppressGroupRowsSticky)}
        immersiveMode={immersiveMode}
        editValueDisplayMode={editValueDisplayMode}
        detailMode={activeDetailDisplayMode}
        detailState={activeDetailDisplayMode === 'inline' ? detailSurface : null}
        detailInlineHeight={detailConfig.inlineHeight}
        onDetailClose={handleDetailClose}
        onDetailPageChange={handleDetailPageChange}
        onDetailSort={handleDetailSort}
        onDetailFilter={handleDetailFilter}
      />
      {paginationConfig.enabled && !serverSide && (
        <PaginationBar
          table={table}
          theme={theme}
          pageSize={paginationState.pageSize}
          onPageSizeChange={/** @param {any} [size] @returns {any} */ size => setPaginationState({ pageIndex: 0, pageSize: size })}
        />
      )}
      {activeDetailDisplayMode === 'drawer' && detailSurface && (
        <React.Suspense fallback={null}>
          <LazyDetailDrawer
          detailState={detailSurface}
          onClose={() => setDetailSurface(null)}
          onPageChange={handleDetailPageChange}
          onSort={handleDetailSort}
          onFilter={handleDetailFilter}
          theme={theme}
          height={detailConfig.drawerHeight}
          />
        </React.Suspense>
      )}
    </div>
    </div>
  </ChartWorkspace>;
}

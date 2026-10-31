import React from 'react';

import { PivotAppBar } from './PivotAppBar';

// The field/filter/report editor is several times larger than the toolbar.
// Loading it on first sidebar render lets the grid mount and become
// interactive before that optional UI is parsed and evaluated.
const LazySidebarPanel = React.lazy(() => import(/* webpackChunkName: "pivot-sidebar" */ './Sidebar/SidebarPanel').then(module => ({ default: module.SidebarPanel })));

/** Rendering-only adapters for the two workspace controls.
 *
 * Each adapter accepts its bounded presentation model rather than the flat
 * compatibility record used while the runtime migration is in progress.
 */
export function PivotWorkspaceToolbar({ toolbar }) {
  const {
    activeChartDefinitionId,
    autoSizeIncludesHeaderNext,
    canApplySelectionValueFormat,
    cellFormatRules,
    colorPalette,
    colorScaleMode,
    columnDecimalOverrides,
    columnFormatOverrides,
    columnGroupSeparatorOverrides,
    dataBarsColumns,
    decimalPlaces,
    defaultValueFormat,
    editValueDisplayMode,
    exportPivot,
    fontFamily,
    fontSize,
    handleAddChartCanvasPane,
    handleAutoSizeToolbarClick,
    handleRedo,
    parentRef,
    handleSaveView,
    handleTransposePivot,
    handleUndo,
    hasComparedValues,
    immersiveMode,
    layoutMode,
    onCreateSelectionChart,
    pivotTitle,
    rowCount,
    rowFields,
    colFields,
    selectedCells,
    selectionValueFormat,
    setCellFormatRules,
    setColorPalette,
    setColorScaleMode,
    setColumnDecimalOverrides,
    setColumnFormatOverrides,
    setColumnGroupSeparatorOverrides,
    setDataBarsColumns,
    setDecimalPlaces,
    setDefaultValueFormat,
    setEditValueDisplayMode,
    setFontFamily,
    setFontSize,
    setImmersiveMode,
    setLayoutMode,
    setSidebarOpen,
    setSpacingMode,
    setThemeName,
    setThemeOverrides,
    setZoomLevel,
    sidebarOpen,
    spacingLabels,
    spacingMode,
    themeName,
    themeOverrides,
    transactionHistoryState,
    uiConfig,
    zoomLevel,
    applySelectionValueFormat,
    isExporting,
  } = toolbar;

  return <PivotAppBar
    immersiveMode={immersiveMode} setImmersiveMode={setImmersiveMode}
    sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}
    themeName={themeName} setThemeName={setThemeName}
    themeOverrides={themeOverrides} setThemeOverrides={setThemeOverrides}
    spacingMode={spacingMode} setSpacingMode={setSpacingMode} spacingLabels={spacingLabels}
    layoutMode={layoutMode} setLayoutMode={setLayoutMode}
    onAutoSizeColumns={handleAutoSizeToolbarClick}
    autoSizeIncludesHeaderNext={autoSizeIncludesHeaderNext}
    onTransposePivot={handleTransposePivot}
    canTranspose={rowFields.length > 0 || colFields.length > 0}
    colorScaleMode={colorScaleMode} setColorScaleMode={setColorScaleMode}
    onReturnFocusToGrid={() => parentRef?.current?.focus({preventScroll: true})}
    colorPalette={colorPalette} setColorPalette={setColorPalette}
    rowCount={rowCount} exportPivot={exportPivot} isExporting={isExporting}
    onSaveView={handleSaveView}
    pivotTitle={pivotTitle}
    fontFamily={fontFamily} setFontFamily={setFontFamily}
    fontSize={fontSize} setFontSize={setFontSize}
    zoomLevel={zoomLevel} setZoomLevel={setZoomLevel}
    decimalPlaces={decimalPlaces} setDecimalPlaces={setDecimalPlaces}
    defaultValueFormat={defaultValueFormat} setDefaultValueFormat={setDefaultValueFormat}
    columnDecimalOverrides={columnDecimalOverrides} setColumnDecimalOverrides={setColumnDecimalOverrides}
    columnFormatOverrides={columnFormatOverrides} setColumnFormatOverrides={setColumnFormatOverrides}
    columnGroupSeparatorOverrides={columnGroupSeparatorOverrides} setColumnGroupSeparatorOverrides={setColumnGroupSeparatorOverrides}
    cellFormatRules={cellFormatRules} setCellFormatRules={setCellFormatRules}
    selectedCells={selectedCells}
    selectionValueFormat={selectionValueFormat}
    canApplySelectionValueFormat={canApplySelectionValueFormat}
    onApplySelectionValueFormat={applySelectionValueFormat}
    dataBarsColumns={dataBarsColumns} setDataBarsColumns={setDataBarsColumns}
    canUndoTransactions={transactionHistoryState.undoCount > 0}
    canRedoTransactions={transactionHistoryState.redoCount > 0}
    transactionHistoryPending={transactionHistoryState.pending}
    onUndoTransaction={handleUndo}
    onRedoTransaction={handleRedo}
    editValueDisplayMode={editValueDisplayMode}
    setEditValueDisplayMode={setEditValueDisplayMode}
    hasComparedValues={hasComparedValues}
    canCreateSelectionChart={Object.keys(selectedCells || {}).length > 0}
    onCreateSelectionChart={onCreateSelectionChart}
    onAddChartPane={handleAddChartCanvasPane}
    uiConfig={uiConfig}
  />;
}

export function PivotWorkspaceSidebar({ sidebar }) {
  const {
    activeFilterCol,
    applyFieldZoneMove,
    availableFieldsWithCustomDimensions,
    clientFilterOptionMap,
    closeFilterPopover,
    colFields,
    colSearch,
    colTypeFilter,
    columnPinning,
    columnVisibility,
    customAwareData,
    displayedFormulaColumnOptions,
    dropLine,
    fieldPanelSizes,
    filterAnchorEl,
    filterOptionMeta,
    filterOptions,
    fixedSparklineValueKeys,
    formulaErrors,
    handleExpandAllRows,
    handleFilterClick,
    handleHeaderFilter,
    handlePinColumn,
    measureAxis,
    onDragOver,
    onDragStart,
    onDrop,
    onHeaderDragEnd,
    pinningPresets,
    reportEditorSelection,
    requestFilterOptions,
    rowFields,
    selectedCols,
    serverSide,
    setColFieldsWithHistory,
    setColSearch,
    setColTypeFilter,
    setColumnPinningWithHistory,
    setColumnVisibility,
    setFieldPanelSizes,
    setFilterAnchorEl,
    setMeasureAxis,
    setRowFieldsWithHistory,
    setSelectedCols,
    setShowReportConfigColumn,
    setSidebarTab,
    setSidebarWidth,
    setValConfigsWithHistory,
    showNotification,
    showReportConfigColumn,
    sidebarTab,
    sidebarWidth,
    sparklineFields,
    table,
    toggleAllColumnsPinned,
    valConfigs,
  } = sidebar;

  return <React.Suspense fallback={null}>
    <LazySidebarPanel
    sidebarTab={sidebarTab} setSidebarTab={setSidebarTab}
    rowFields={rowFields} setRowFields={setRowFieldsWithHistory}
    colFields={colFields} setColFields={setColFieldsWithHistory}
    valConfigs={valConfigs} setValConfigs={setValConfigsWithHistory}
    measureAxis={measureAxis} setMeasureAxis={setMeasureAxis}
    fixedSparklineValueKeys={fixedSparklineValueKeys}
    displayedColumnOptions={displayedFormulaColumnOptions}
    columnVisibility={columnVisibility} setColumnVisibility={setColumnVisibility}
    columnPinning={columnPinning} setColumnPinning={setColumnPinningWithHistory}
    availableFields={availableFieldsWithCustomDimensions}
    sparklineFields={sparklineFields}
    table={table}
    pinningPresets={pinningPresets}
    showNotification={showNotification}
    filterAnchorEl={filterAnchorEl} setFilterAnchorEl={setFilterAnchorEl}
    colSearch={colSearch} setColSearch={setColSearch}
    colTypeFilter={colTypeFilter} setColTypeFilter={setColTypeFilter}
    selectedCols={selectedCols} setSelectedCols={setSelectedCols}
    dropLine={dropLine}
    onDragStart={onDragStart} onDragOver={onDragOver} onDrop={onDrop} onDragEnd={onHeaderDragEnd}
    onKeyboardFieldDrop={applyFieldZoneMove}
    handleHeaderFilter={handleHeaderFilter}
    handleFilterClick={handleFilterClick}
    requestFilterOptions={requestFilterOptions}
    handleExpandAllRows={handleExpandAllRows}
    handlePinColumn={handlePinColumn}
    toggleAllColumnsPinned={toggleAllColumnsPinned}
    activeFilterCol={activeFilterCol}
    closeFilterPopover={closeFilterPopover}
    filterOptions={filterOptions}
    filterOptionMeta={filterOptionMeta}
    clientFilterOptionMap={clientFilterOptionMap}
    data={customAwareData}
    sidebarWidth={sidebarWidth}
    setSidebarWidth={setSidebarWidth}
    fieldPanelSizes={fieldPanelSizes}
    setFieldPanelSizes={setFieldPanelSizes}
    reportEditorSelection={reportEditorSelection}
    showReportConfigColumn={showReportConfigColumn}
    setShowReportConfigColumn={setShowReportConfigColumn}
    formulaErrors={formulaErrors}
    serverSide={serverSide}
    />
  </React.Suspense>;
}

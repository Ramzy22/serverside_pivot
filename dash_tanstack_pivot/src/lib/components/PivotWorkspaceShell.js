// DashTanstackPivot - Enterprise Grade Pivot Table
import React from 'react';
import Notification from './Notification';
import { PivotWorkspaceSidebar, PivotWorkspaceToolbar } from './PivotWorkspaceControls';
import { PivotWorkspaceCanvas } from './PivotWorkspaceCanvas';
import PivotErrorBoundary from './PivotErrorBoundary';
import { PivotThemeProvider } from '../contexts/PivotThemeContext';
import { PivotConfigProvider } from '../contexts/PivotConfigContext';
import { PivotValueDisplayProvider } from '../contexts/PivotValueDisplayContext';
import {
    DEFAULT_CHART_GRAPH_HEIGHT,
    resolveChartAvailableColumns,
} from '../hooks/usePivotNormalization';
import {
    formatSparklineDetailExtremum,
    formatSparklineDetailValue,
} from '../utils/sparklineDetail';
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ './Charts/PivotCharts').then(module => ({ default: module.PivotChartModal })));
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ './Charts/PivotCharts').then(module => ({ default: module.PivotChartPanel })));
const LazyContextMenu = React.lazy(() => import(/* webpackChunkName: "pivot-context-menu" */ './Table/ContextMenu'));
const LazySparklineDetailGraph = React.lazy(() => import(/* webpackChunkName: "pivot-sparkline-detail" */ './Table/SparklineDetailGraph').then(module => ({ default: module.SparklineDetailGraph })));

const PivotChartFallback = () => null;
const PivotChartModal = (props) => (
    <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartModal {...props} />
    </React.Suspense>
);
const PivotChartPanel = (props) => (
    <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartPanel {...props} />
    </React.Suspense>
);

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

export function ExportDeliveryOverlay({exportProgress, isCopying, theme, cancelActiveExportRequest, retryExportJob, dismissExportJob}) {
    const progress = exportProgress && typeof exportProgress === 'object' ? exportProgress : null;
    const phase = progress?.phase;
    const delivering = progress?.delivery === 'pending';
    const terminal = !delivering && ['cancelled', 'completed', 'failed', 'interrupted'].includes(phase);
    const percent = progress ? progress.percent : typeof exportProgress === 'number' ? exportProgress : null;
    const labels = {queued: 'Queued', running: 'Running', cancelling: 'Cancelling — waiting for server cleanup',
        cancelled: 'Cancelled', completed: delivering ? 'Preparing download…' : progress?.deliveryFailed ? 'Completed — download failed' : 'Completed', failed: 'Failed', interrupted: 'Interrupted — retry'};
    return (
        <div role="dialog" aria-label="Export progress" aria-modal="true" data-export-phase={phase || 'running'} style={{position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 10003, display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
            <div style={{background: theme.background, color: theme.text, borderRadius: 14, padding: '32px 40px 28px', boxShadow: '0 12px 40px rgba(0,0,0,0.35)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, minWidth: 280, border: `1px solid ${theme.border}`}}>
                <span role="status" aria-live="polite" style={{fontSize: 14, fontWeight: 600}}>{isCopying ? 'Copying to clipboard…' : labels[phase] || 'Exporting all rows…'}</span>
                {!terminal && <div role="progressbar" aria-label="Export rows" aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent === null ? undefined : percent}
                    style={{width: '100%', height: 6, background: theme.border || '#e5e7eb', borderRadius: 3, overflow: 'hidden', position: 'relative'}}>
                    <style>{`@keyframes pivot-bar-slide{0%{transform:translateX(-150%)}100%{transform:translateX(350%)}}`}</style>
                    <div style={{position: 'absolute', left: 0, top: 0, height: '100%', width: percent === null ? '40%' : `${percent}%`, borderRadius: 3,
                        background: theme.accent || '#4f8ef7', animation: percent === null ? 'pivot-bar-slide 1.4s ease-in-out infinite' : undefined}}/>
                </div>}
                {progress && <span style={{fontSize: 12}}>{progress.rowsWritten.toLocaleString()} rows · {progress.bytesWritten.toLocaleString()} bytes</span>}
                {!terminal && percent !== null && <span style={{fontSize: 12}}>{percent}%</span>}
                {terminal ? <div style={{display: 'flex', gap: 12}}>
                    {['failed', 'interrupted', 'cancelled'].includes(phase) || progress?.deliveryFailed ? <button onClick={retryExportJob}>Retry</button> : null}
                    <button onClick={dismissExportJob}>Close</button>
                </div> : <button onClick={cancelActiveExportRequest} disabled={delivering || phase === 'cancelling'}
                    style={{padding: '7px 28px', background: '#dc2626', color: '#fff', border: 'none', borderRadius: 7, cursor: delivering || phase === 'cancelling' ? 'wait' : 'pointer'}}>
                    {delivering ? 'Downloading…' : phase === 'cancelling' ? 'Cancelling…' : 'Cancel'}
                </button>}
            </div>
        </div>
    );
}

export function PivotWorkspaceShell({ workspace }) {
    // The shell intentionally receives only presentation props. Runtime
    // coordination, request state, and caches stay behind the workspace-model
    // boundary even while the JSX continues to be rendered by this component.
    const model = workspace.presentation;
    const {
        COPY_ROW_LIMIT, EXPORT_ROW_LIMIT, activateChartCategory, activeReportId, announcement,
        cancelActiveExportRequest, chartCanvasPaneModels, chartCanvasPanes, chartPaneDataById,
        comparisonValueState, contextMenu, customDimensions, dispatchExportPart, editValueDisplayMode,
        exportProgress, retryExportJob, dismissExportJob, filters, fontFamily, fontSize, handleCancelPreviewChartPane,
        handleConfirmPreviewChartPane, handleSetPivotMode, id, immersiveMode, immersiveModeHovering,
        isCopying, isExporting, loadingCssVars, notification, numberGroupSeparator, partModal,
        pivotMode, previewChartPaneId, reportDef, resolveCurrentCellValue, resolveDisplayedCellValue,
        rows, savedReports, serverSide, setActiveReportId, setContextMenu, setCustomDimensions,
        setEditValueDisplayMode, setFilters, setImmersiveMode, setImmersiveModeHovering,
        setNotification, setNumberGroupSeparator, setPartModal, setReportDef, setSavedReports,
        setShowColTotals, setShowFloatingFilters, setShowRowNumbers, setShowRowTotals,
        setShowSubtotals, setSparklineDataModal, setStickyHeaders, showColTotals,
        showFloatingFilters, showRowNumbers, showRowTotals, showSubtotals, sidebarOpen,
        sparklineDataModal, srOnly, stickyHeaders, style, styles, table, theme, uiConfig,
        updateChartCanvasPane, viewMode, visibleLeafColumns, zoomLevel,
        setColFields, setValConfigs,
    } = model;

return (
        <div
            id={id}
            style={{ ...styles.root, ...loadingCssVars, position: 'relative', ...style }}
            onMouseEnter={() => setImmersiveModeHovering(true)}
            onMouseLeave={() => setImmersiveModeHovering(false)}
        >
            <style>{loadingAnimationStyles}</style>
            <div style={srOnly} role="status" aria-live="polite">{announcement}</div>
            <PivotThemeProvider theme={theme} styles={styles}>
            <PivotConfigProvider
                filters={filters} setFilters={setFilters}
                pivotMode={pivotMode} setPivotMode={handleSetPivotMode}
                reportDef={reportDef} setReportDef={setReportDef}
                setColFields={setColFields} setValConfigs={setValConfigs}
                savedReports={savedReports} setSavedReports={setSavedReports}
                activeReportId={activeReportId} setActiveReportId={setActiveReportId}
                customDimensions={customDimensions} setCustomDimensions={setCustomDimensions}
                showFloatingFilters={showFloatingFilters} setShowFloatingFilters={setShowFloatingFilters}
                stickyHeaders={stickyHeaders} setStickyHeaders={setStickyHeaders}
                showColTotals={showColTotals} setShowColTotals={setShowColTotals}
                showRowTotals={showRowTotals} setShowRowTotals={setShowRowTotals}
                showSubtotals={showSubtotals} setShowSubtotals={setShowSubtotals}
                showRowNumbers={showRowNumbers} setShowRowNumbers={setShowRowNumbers}
                numberGroupSeparator={numberGroupSeparator} setNumberGroupSeparator={setNumberGroupSeparator}
                viewMode={viewMode}
            >
            {/* Immersive mode exit button — visible only when immersive mode is active and not locked */}
            {immersiveMode && !uiConfig.lockImmersiveMode && (
                <button
                    data-immersive-exit-overlay
                    onClick={() => setImmersiveMode(false)}
                    title="Exit Immersive Mode"
                    style={{
                        position: 'absolute', top: 12, right: 12, zIndex: 9999,
                        background: 'rgba(0,0,0,0.55)', color: '#fff',
                        border: 'none', borderRadius: 8, padding: '6px 14px',
                        cursor: 'pointer', fontSize: 13, fontWeight: 600,
                        display: 'flex', alignItems: 'center', gap: 6,
                        backdropFilter: 'blur(4px)',
                        opacity: immersiveModeHovering ? 0.92 : 0,
                        pointerEvents: immersiveModeHovering ? 'auto' : 'none',
                        transform: immersiveModeHovering ? 'translateY(0)' : 'translateY(-6px)',
                        transition: 'opacity 0.18s ease, transform 0.18s ease',
                    }}
                >
                    ✕ Exit Immersive
                </button>
            )}
            <PivotValueDisplayProvider
                editValueDisplayMode={editValueDisplayMode}
                resolveCellDisplayValue={resolveDisplayedCellValue}
                resolveCurrentCellValue={resolveCurrentCellValue}
                setEditValueDisplayMode={setEditValueDisplayMode}
                hasComparedValues={comparisonValueState.activeCount > 0}
            >
            {/* PivotAppBar stays outside PivotErrorBoundary so toolbar controls
                remain usable even if the table renderer hits an error state. */}
            <div style={{display:'flex', flexDirection:'column', flex:1, overflow:'hidden', zoom: zoomLevel / 100}}>
            {!immersiveMode && uiConfig.showToolbar && <PivotWorkspaceToolbar toolbar={workspace.controls.toolbar} />}
        <PivotErrorBoundary>
            <div style={{display:'flex', flex:1, overflow:'hidden', fontFamily: fontFamily, fontSize: fontSize}}>
                {!immersiveMode && uiConfig.showSidebar && sidebarOpen && (
                    <PivotWorkspaceSidebar sidebar={workspace.controls.sidebar} />
                )}
                <PivotWorkspaceCanvas canvas={workspace.canvas} />
            </div>
            {contextMenu && (
                <React.Suspense fallback={null}>
                    <LazyContextMenu {...contextMenu} theme={theme} onClose={() => setContextMenu(null)} />
                </React.Suspense>
            )}
            {notification && <Notification message={notification.message} type={notification.type} onClose={() => setNotification(null)} />}
            {previewChartPaneId && (() => {
                const previewPane = chartCanvasPanes.find((p) => p.id === previewChartPaneId);
                if (!previewPane) return null;
                const btnBase = { padding: '7px 16px', borderRadius: theme.radiusSm || '8px', fontSize: '12px', fontWeight: 700, cursor: 'pointer', border: 'none', display: 'inline-flex', alignItems: 'center', gap: '6px' };
                return (
                    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.52)', zIndex: 10050, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <div style={{ background: theme.background, border: `1px solid ${theme.border}`, borderRadius: theme.radius || '16px', boxShadow: '0 12px 48px rgba(0,0,0,0.32)', width: '86vw', maxWidth: '1160px', height: '80vh', maxHeight: '840px', display: 'flex', flexDirection: 'column', overflow: 'hidden', fontFamily: fontFamily }}>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 20px', borderBottom: `1px solid ${theme.border}`, flexShrink: 0, background: theme.headerBg || theme.surfaceBg || theme.background }}>
                                <span style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>New Chart Pane</span>
                                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                    <button onClick={handleCancelPreviewChartPane} style={{ ...btnBase, background: theme.headerSubtleBg || theme.hover, color: theme.text, border: `1px solid ${theme.border}` }}>Cancel</button>
                                    <button onClick={handleConfirmPreviewChartPane} style={{ ...btnBase, background: theme.primary, color: '#fff' }}>Open in Panel</button>
                                </div>
                            </div>
                            <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex' }}>
                                <PivotChartPanel
                                    open
                                    onClose={handleCancelPreviewChartPane}
                                    source={previewPane.source}
                                    onSourceChange={(value) => updateChartCanvasPane(previewPane.id, { source: value })}
                                    chartType={previewPane.chartType}
                                    onChartTypeChange={(value) => updateChartCanvasPane(previewPane.id, { chartType: value })}
                                    chartLayers={previewPane.chartLayers}
                                    onChartLayersChange={(value) => updateChartCanvasPane(previewPane.id, { chartLayers: value })}
                                    availableColumns={resolveChartAvailableColumns(chartPaneDataById[previewPane.id], visibleLeafColumns)}
                                    barLayout={previewPane.barLayout}
                                    onBarLayoutChange={(value) => updateChartCanvasPane(previewPane.id, { barLayout: value })}
                                    axisMode={previewPane.axisMode}
                                    onAxisModeChange={(value) => updateChartCanvasPane(previewPane.id, { axisMode: value })}
                                    orientation={previewPane.orientation}
                                    onOrientationChange={(value) => updateChartCanvasPane(previewPane.id, { orientation: value })}
                                    hierarchyLevel={previewPane.hierarchyLevel}
                                    onHierarchyLevelChange={(value) => updateChartCanvasPane(previewPane.id, { hierarchyLevel: value })}
                                    chartTitle={previewPane.chartTitle || previewPane.name}
                                    onChartTitleChange={(value) => updateChartCanvasPane(previewPane.id, { chartTitle: value })}
                                    rowLimit={previewPane.rowLimit}
                                    onRowLimitChange={(value) => updateChartCanvasPane(previewPane.id, { rowLimit: value })}
                                    columnLimit={previewPane.columnLimit}
                                    onColumnLimitChange={(value) => updateChartCanvasPane(previewPane.id, { columnLimit: value })}
                                    chartHeight={previewPane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT}
                                    onChartHeightChange={(value) => {
                                        const nextHeight = Number(value);
                                        updateChartCanvasPane(previewPane.id, {
                                            chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT,
                                        });
                                    }}
                                    sortMode={previewPane.sortMode}
                                    onSortModeChange={(value) => updateChartCanvasPane(previewPane.id, { sortMode: value })}
                                    interactionMode={previewPane.interactionMode}
                                    onInteractionModeChange={(value) => updateChartCanvasPane(previewPane.id, { interactionMode: value })}
                                    serverScope={previewPane.serverScope}
                                    onServerScopeChange={(value) => updateChartCanvasPane(previewPane.id, { serverScope: value })}
                                    showServerScope={serverSide}
                                    model={chartCanvasPaneModels[previewPane.id] || null}
                                    theme={theme}
                                    onCategoryActivate={(target) => activateChartCategory(previewPane.source, previewPane.interactionMode, target)}
                                    floating={false}
                                    initialSettings={previewPane.chartSettings || {}}
                                    onSettingsChange={(settings) => updateChartCanvasPane(previewPane.id, { chartSettings: settings })}
                                    settingsPanelWidth={previewPane.settingsPanelWidth || undefined}
                                    standalone
                                    showResizeHandle={false}
                                    title={previewPane.name}
                                    showDefinitionManager={false}
                                    locked={false}
                                    dockPosition="right"
                                    initialSettingsPaneOpen
                                />
                            </div>
                        </div>
                    </div>
                );
            })()}
            {partModal && (() => {
                const { mode, rowsPerPart, colsPerPart, totalRows, totalCols, processingPartId, completedParts } = partModal;
                const rowBatches = Math.ceil(totalRows / rowsPerPart);
                const colBatches = colsPerPart < totalCols ? Math.ceil(totalCols / colsPerPart) : 1;
                const totalParts = rowBatches * colBatches;
                const tooMany = totalParts > 50;
                const limit = mode === 'copy' ? COPY_ROW_LIMIT : EXPORT_ROW_LIMIT;
                const inp = { display:'block', width:'100%', marginTop:'4px', padding:'6px 8px', borderRadius:'6px', border:`1px solid ${theme.border}`, background: theme.input || theme.background, color:theme.text, fontSize:'13px', boxSizing:'border-box' };
                const btnBase = { padding:'6px 12px', borderRadius:'6px', border:'none', cursor:'pointer', fontSize:'12px', fontWeight:600, transition:'opacity .15s' };
                return (
                    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.5)', zIndex:10002, display:'flex', alignItems:'center', justifyContent:'center' }}>
                        <div style={{ background:theme.background, color:theme.text, borderRadius:'12px', padding:'24px', boxShadow:'0 8px 32px rgba(0,0,0,0.3)', width:'440px', maxWidth:'94vw', maxHeight:'85vh', display:'flex', flexDirection:'column', gap:'14px', fontFamily:"'Inter',ui-sans-serif,system-ui,sans-serif" }}>
                            <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between' }}>
                                <span style={{ fontWeight:700, fontSize:'15px' }}>{mode === 'copy' ? 'Copy Table — Large Dataset' : 'Export — Large Dataset'}</span>
                                <button onClick={cancelActiveExportRequest} style={{ background:'none', border:'none', cursor:'pointer', color:theme.textSec, fontSize:'20px', lineHeight:1, padding:'0 4px' }}>×</button>
                            </div>
                            <div style={{ fontSize:'13px', color:theme.textSec || theme.text, opacity:.75 }}>
                                {totalRows.toLocaleString()} rows × {totalCols} columns — limit is {limit.toLocaleString()} rows. Split into parts below.
                            </div>
                            <div style={{ display:'flex', gap:'12px' }}>
                                <label style={{ flex:1, fontSize:'13px' }}>
                                    Rows per part
                                    <input type="number" min={100} max={totalRows} value={rowsPerPart}
                                        onChange={e => setPartModal(prev => ({ ...prev, rowsPerPart: Math.max(100, Math.min(totalRows, parseInt(e.target.value) || 100)) }))}
                                        style={inp} />
                                </label>
                                <label style={{ flex:1, fontSize:'13px' }}>
                                    Columns per part
                                    <input type="number" min={1} max={totalCols} value={colsPerPart}
                                        onChange={e => setPartModal(prev => ({ ...prev, colsPerPart: Math.max(1, Math.min(totalCols, parseInt(e.target.value) || 1)) }))}
                                        style={inp} />
                                </label>
                            </div>
                            <div style={{ fontSize:'12px', color:theme.textSec || theme.text, opacity:.7 }}>
                                → {totalParts} part{totalParts !== 1 ? 's' : ''} ({rowBatches} row × {colBatches} col batch{colBatches !== 1 ? 'es' : ''})
                            </div>
                            {tooMany ? (
                                <div style={{ fontSize:'12px', color:'#e07700', padding:'8px 12px', background:'rgba(224,119,0,.1)', borderRadius:'6px' }}>
                                    Too many parts ({totalParts}). Increase rows or columns per part to reduce below 50.
                                </div>
                            ) : (
                                <div style={{ display:'flex', flexWrap:'wrap', gap:'8px', overflowY:'auto', maxHeight:'200px' }}>
                                    {Array.from({ length: rowBatches }, (_, rb) =>
                                        Array.from({ length: colBatches }, (_, cb) => {
                                            const pid = `r${rb}_c${cb}`;
                                            const done = completedParts.has(pid);
                                            const loading = processingPartId === pid;
                                            const rowFrom = rb * rowsPerPart + 1;
                                            const rowTo = Math.min((rb + 1) * rowsPerPart, totalRows);
                                            const label = colBatches > 1
                                                ? `R${rb+1}–C${cb+1} (${rowFrom.toLocaleString()}–${rowTo.toLocaleString()})`
                                                : `Part ${rb + 1}  (rows ${rowFrom.toLocaleString()}–${rowTo.toLocaleString()})`;
                                            return (
                                                <button key={pid} disabled={!!processingPartId}
                                                    onClick={() => dispatchExportPart(rb, cb)}
                                                    style={{ ...btnBase, background: done ? (theme.accent || '#4f8ef7') : theme.headerBg || theme.border, color: done ? '#fff' : theme.text, opacity: processingPartId && !loading ? 0.5 : 1 }}>
                                                    {loading ? '⏳ ' : done ? '✓ ' : ''}{label}
                                                </button>
                                            );
                                        })
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                );
            })()}
            {(isExporting || isCopying || (exportProgress && typeof exportProgress === 'object')) && (
                <ExportDeliveryOverlay {...{exportProgress, isCopying, theme, cancelActiveExportRequest, retryExportJob, dismissExportJob}}/>
            )}
            {sparklineDataModal && (
                <div
                    style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 10000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                    onClick={() => setSparklineDataModal(null)}
                >
                    <div
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background: theme.background,
                            border: `1px solid ${theme.border}`,
                            borderRadius: '12px',
                            boxShadow: '0 8px 32px rgba(0,0,0,0.22)',
                            width: 'min(940px, 94vw)',
                            maxWidth: '94vw',
                            maxHeight: '82vh',
                            display: 'flex',
                            flexDirection: 'column',
                            fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                            overflow: 'hidden',
                        }}
                    >
                        {/* Modal header */}
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 18px 10px', borderBottom: `1px solid ${theme.border}` }}>
                            <span style={{ fontSize: '13px', fontWeight: 700, color: theme.text }}>
                                {sparklineDataModal.headerLabel || 'Trend Data'}
                            </span>
                            <button
                                type="button"
                                onClick={() => setSparklineDataModal(null)}
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textSec, fontSize: '18px', lineHeight: 1, padding: '2px 4px' }}
                            >×</button>
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', overflow: 'hidden', flex: 1, minHeight: 0 }}>
                            <React.Suspense fallback={null}>
                                <LazySparklineDetailGraph modal={sparklineDataModal} theme={theme} />
                            </React.Suspense>
                            <div
                                data-pivot-sparkline-detail-table="true"
                                style={{
                                    flex: '1 1 280px',
                                    minWidth: '260px',
                                    maxHeight: 'calc(82vh - 118px)',
                                    overflow: 'auto',
                                }}
                            >
                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                                    <thead>
                                        <tr style={{ background: theme.headerBg || theme.headerSubtleBg }}>
                                            <th style={{ textAlign: 'left', padding: '7px 14px', color: theme.textSec, fontWeight: 600, fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: `1px solid ${theme.border}` }}>Period</th>
                                            <th style={{ textAlign: 'right', padding: '7px 14px', color: theme.textSec, fontWeight: 600, fontSize: '10px', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: `1px solid ${theme.border}` }}>Value</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(sparklineDataModal.points || []).map((point, i) => (
                                            <tr key={i} style={{ borderBottom: `1px solid ${theme.border}`, background: i % 2 === 0 ? 'transparent' : (theme.headerSubtleBg || 'rgba(0,0,0,0.02)') }}>
                                                <td style={{ padding: '6px 14px', color: theme.textSec }}>{point.label}</td>
                                                <td style={{ padding: '6px 14px', textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontWeight: 600, color: theme.text }}>{formatSparklineDetailValue(point.value, 6)}</td>
                                            </tr>
                                        ))}
                                        {(sparklineDataModal.points || []).length === 0 && (
                                            <tr><td colSpan={2} style={{ padding: '16px 14px', color: theme.textSec, textAlign: 'center' }}>No data</td></tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                        {/* Footer summary */}
                        {(sparklineDataModal.points || []).length > 0 && (
                            <div style={{ borderTop: `1px solid ${theme.border}`, padding: '8px 14px', display: 'flex', flexWrap: 'wrap', gap: '16px', fontSize: '11px', color: theme.textSec, background: theme.headerSubtleBg || theme.headerBg }}>
                                <span>{(sparklineDataModal.points || []).length} points</span>
                                <span>Min: <b style={{ color: theme.text }}>{formatSparklineDetailExtremum(sparklineDataModal.points, 'min')}</b></span>
                                <span>Max: <b style={{ color: theme.text }}>{formatSparklineDetailExtremum(sparklineDataModal.points, 'max')}</b></span>
                                <span>Last: <b style={{ color: theme.primary }}>{formatSparklineDetailValue((sparklineDataModal.points || [])[(sparklineDataModal.points || []).length - 1]?.value)}</b></span>
                            </div>
                        )}
                    </div>
                </div>
            )}
        </PivotErrorBoundary>
            </div>{/* end zoom wrapper */}
            </PivotValueDisplayProvider>
            </PivotConfigProvider>
            </PivotThemeProvider>
        </div>
    );
}

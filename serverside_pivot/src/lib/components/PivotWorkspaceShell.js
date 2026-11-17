// DashTanstackPivot - Enterprise Grade Pivot Table
import React from 'react';
import Notification from './Notification';
import { PivotWorkspaceSidebar, PivotWorkspaceToolbar } from './PivotWorkspaceControls';
import { PivotWorkspaceCanvas } from './PivotWorkspaceCanvas';
import PivotErrorBoundary from './PivotErrorBoundary';
import { PivotThemeProvider } from '../contexts/PivotThemeContext';
import { PivotConfigProvider } from '../contexts/PivotConfigContext';
import ShimmerFallback from './ShimmerFallback';
import { PivotValueDisplayProvider } from '../contexts/PivotValueDisplayContext';
import {
    DEFAULT_CHART_GRAPH_HEIGHT,
    resolveChartAvailableColumns,
} from '../hooks/usePivotNormalization';
import {
    formatSparklineDetailExtremum,
    formatSparklineDetailValue,
} from '../utils/sparklineDetail';
/** @type {any} */
const LazyPivotChartModal = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ './Charts/PivotCharts').then(module => ({ default: module.PivotChartModal })));
/** @type {any} */
const LazyPivotChartPanel = React.lazy(() => import(/* webpackChunkName: "pivot-charts" */ './Charts/PivotCharts').then(module => ({ default: module.PivotChartPanel })));
/** @type {any} */
const LazyContextMenu = React.lazy(() => import(/* webpackChunkName: "pivot-context-menu" */ './Table/ContextMenu'));
/** @type {any} */
const LazySparklineDetailGraph = React.lazy(() => import(/* webpackChunkName: "pivot-sparkline-detail" */ './Table/SparklineDetailGraph').then(module => ({ default: module.SparklineDetailGraph })));

/** @returns {any} */
const PivotChartFallback = () => <ShimmerFallback variant="chart" />;
/** @param {any} [props] @returns {any} */
const PivotChartModal = (props) => (
    <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartModal {...props} />
    </React.Suspense>
);
/** @param {any} [props] @returns {any} */
const PivotChartPanel = (props) => (
    <React.Suspense fallback={<PivotChartFallback />}>
        <LazyPivotChartPanel {...props} />
    </React.Suspense>
);

const GOOGLE_FONTS_IMPORT_STYLE = `@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@300;500&family=Plus+Jakarta+Sans:wght@300;500;800&display=swap');\n`;
const baseAnimationStyles = `
[data-pivot-workspace-root], [data-pivot-workspace-root] *, [data-pivot-workspace-root] *::before, [data-pivot-workspace-root] *::after { box-sizing: border-box; }
[data-pivot-workspace-root] [role="row"]:hover { --sp-row-current-bg: var(--sp-row-hover-bg); }
[data-pivot-workspace-root] [role="row"]:hover [data-grid-cell-tint="true"] {
    background-image: linear-gradient(var(--sp-row-hover-tint, rgba(15,23,42,0.032)), var(--sp-row-hover-tint, rgba(15,23,42,0.032)));
}
[data-pivot-workspace-root] [data-pivot-scroll-container] { overscroll-behavior: contain; }
[data-pivot-workspace-root][data-header-controls="hover"] .sp-header-control {
    opacity: 0;
    max-width: 0;
    overflow: hidden;
    margin-left: 0 !important;
    margin-right: 0 !important;
    padding-left: 0 !important;
    padding-right: 0 !important;
    pointer-events: none;
    transform: scale(0.88);
    transition: opacity 150ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)), max-width 150ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)), transform 150ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1));
}
[data-pivot-workspace-root][data-header-controls="hover"] [role="columnheader"]:not(:hover):not(:focus-within) .sp-header-control.sp-header-badge {
    border-left-width: 0 !important;
    border-right-width: 0 !important;
}
[data-pivot-workspace-root][data-header-controls="hover"] [role="columnheader"]:hover .sp-header-control,
[data-pivot-workspace-root][data-header-controls="hover"] [role="columnheader"]:focus-within .sp-header-control {
    opacity: 1;
    max-width: 32px;
    padding-left: 2px !important;
    padding-right: 2px !important;
    pointer-events: auto;
    transform: scale(1);
}
[data-pivot-workspace-root][data-header-controls="never"] .sp-header-control { display: none !important; }
[data-pivot-workspace-root] .sp-btn {
    transition: background-color 140ms var(--sp-ease-apple, ease), border-color 140ms var(--sp-ease-apple, ease), box-shadow 140ms var(--sp-ease-apple, ease), transform 110ms var(--sp-ease-apple, ease), opacity 140ms ease, filter 140ms ease;
}
[data-pivot-workspace-root] .sp-btn:not(:disabled):active { transform: scale(0.975); }
[data-pivot-workspace-root] .sp-btn-ghost:not(:disabled):hover { background: var(--sp-hover) !important; }
[data-pivot-workspace-root] .sp-btn-subtle:not(.sp-btn-active):not([aria-pressed="true"]):not(:disabled):hover {
    background: var(--sp-hover-strong, var(--sp-hover)) !important;
    border-color: var(--sp-border-strong, var(--sp-border)) !important;
}
[data-pivot-workspace-root] .sp-btn-active:not(:disabled):hover,
[data-pivot-workspace-root] .sp-btn[aria-pressed="true"]:not(:disabled):hover {
    filter: brightness(1.03);
}
[data-pivot-workspace-root] .sp-btn-primary:not(:disabled):hover {
    filter: brightness(1.07);
    box-shadow: 0 4px 14px var(--sp-focus-ring, rgba(37,99,235,0.28)) !important;
}
[data-pivot-workspace-root] .sp-btn-primary-soft:not(:disabled):hover {
    filter: brightness(1.04);
    border-color: var(--sp-primary) !important;
    box-shadow: 0 2px 10px var(--sp-focus-ring, rgba(37,99,235,0.18)) !important;
}
[data-pivot-workspace-root] [data-sidebar-field-chip] {
    transition: transform 140ms var(--sp-ease-apple, ease), box-shadow 140ms var(--sp-ease-apple, ease), border-color 140ms var(--sp-ease-apple, ease), background-color 140ms var(--sp-ease-apple, ease);
}
[data-pivot-workspace-root] [data-sidebar-field-chip]:hover {
    transform: translateY(-0.5px);
    box-shadow: var(--sp-shadow-sm);
}
[data-pivot-workspace-root] [data-sidebar-field-chip]:active {
    cursor: grabbing;
    transform: scale(0.985);
}
[data-pivot-workspace-root] .sp-search-shell {
    transition: border-color 150ms var(--sp-ease-apple, ease), box-shadow 150ms var(--sp-ease-apple, ease), background-color 150ms var(--sp-ease-apple, ease);
}
[data-pivot-workspace-root] .sp-search-shell:focus-within {
    border-color: var(--sp-primary) !important;
    box-shadow: 0 0 0 3px var(--sp-focus-ring, rgba(37,99,235,0.2)) !important;
}
[data-pivot-workspace-root] .sp-search-shell input:focus-visible { outline: none !important; }
[data-pivot-workspace-root] .sp-resize-pill {
    transition: background-color 140ms var(--sp-ease-apple, ease), opacity 140ms var(--sp-ease-apple, ease), box-shadow 140ms var(--sp-ease-apple, ease);
}
[data-pivot-workspace-root] .sp-resize-handle:hover .sp-resize-pill,
[data-pivot-workspace-root] .sp-resize-handle[data-resizing="true"] .sp-resize-pill {
    opacity: 1 !important;
    background: var(--sp-primary) !important;
    box-shadow: 0 0 8px var(--sp-focus-ring, rgba(37,99,235,0.35));
}
[data-pivot-workspace-root] .sp-expander-btn {
    border-radius: 5px;
    transition: background-color 120ms ease, transform 110ms var(--sp-ease-apple, ease);
}
[data-pivot-workspace-root] .sp-expander-btn:hover { background: var(--sp-hover); }
[data-pivot-workspace-root] .sp-expander-btn:active { transform: scale(0.92); }
[data-pivot-workspace-root] .sp-tab {
    transition: color 140ms ease, border-color 180ms var(--sp-ease-apple, ease), background-color 140ms ease;
}
[data-pivot-workspace-root] .sp-tab:hover { color: var(--sp-text) !important; }
[data-pivot-workspace-root] .sp-menu-surface {
    animation: pivot-menu-enter 140ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both;
    transform-origin: top left;
}
[data-pivot-workspace-root] .sp-menu-item {
    border-radius: 6px;
    padding: 7px 10px;
    transition: background-color 100ms ease, color 100ms ease;
}
[data-pivot-workspace-root] .sp-menu-item:not([aria-disabled="true"]):hover,
[data-pivot-workspace-root] .sp-menu-item:not([aria-disabled="true"]):focus-visible {
    background-color: var(--sp-hover);
    outline: none;
}
[data-pivot-workspace-root] .sp-dialog-backdrop {
    animation: pivot-backdrop-enter 160ms ease-out both;
}
[data-pivot-workspace-root] .sp-dialog-surface {
    animation: pivot-surface-enter 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both;
}
[data-pivot-workspace-root] .sp-toast-surface {
    animation: pivot-toast-enter 220ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both;
}
[data-pivot-workspace-root] button:focus-visible,
[data-pivot-workspace-root] [role="button"]:focus-visible,
[data-pivot-workspace-root] [role="tab"]:focus-visible,
[data-pivot-workspace-root] select:focus-visible,
[data-pivot-workspace-root] summary:focus-visible,
[data-pivot-workspace-root] input:focus-visible,
[data-pivot-workspace-root] [data-sidebar-drop-zone]:focus-visible,
[data-pivot-workspace-root] [data-sidebar-field-chip]:focus-visible {
    outline: 2px solid var(--sp-primary);
    outline-offset: 2px;
}
@keyframes pivot-surface-enter {
    from { opacity: 0; transform: translateY(6px) scale(0.985); }
    to { opacity: 1; transform: translateY(0) scale(1); }
}
@keyframes pivot-backdrop-enter {
    from { opacity: 0; }
    to { opacity: 1; }
}
@keyframes pivot-menu-enter {
    from { opacity: 0; transform: scale(0.96) translateY(-3px); }
    to { opacity: 1; transform: scale(1) translateY(0); }
}
@keyframes pivot-toast-enter {
    from { opacity: 0; transform: translateX(-50%) translateY(10px) scale(0.97); }
    to { opacity: 1; transform: translateX(-50%) translateY(0) scale(1); }
}
@keyframes pivot-row-loader-enter {
    from { opacity: 0; transform: translateY(-6px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes pivot-row-reveal {
    0% { opacity: 0; transform: translate3d(0, -10px, 0); }
    65% { opacity: 1; }
    100% { opacity: 1; transform: none; }
}
@keyframes pivot-row-exit {
    0% { opacity: 1; transform: translate3d(0, 0, 0); }
    100% { opacity: 0; transform: translate3d(0, -8px, 0); }
}
@keyframes pivot-hairline-sweep {
    0% { transform: translateX(-110%); }
    100% { transform: translateX(320%); }
}
@keyframes pivot-band-enter {
    from { opacity: 0; clip-path: inset(0 0 100% 0); }
    to { opacity: 1; clip-path: inset(0 0 0% 0); }
}
@keyframes pivot-halo-pulse {
    0% { transform: scale(0.92); opacity: 0.85; }
    70% { transform: scale(1.24); opacity: 0; }
    100% { transform: scale(1.24); opacity: 0; }
}
@keyframes pivot-wave-dot {
    0%, 80%, 100% { transform: translateY(0); opacity: 0.45; }
    40% { transform: translateY(-2.5px); opacity: 1; }
}
[data-pivot-workspace-root] .sp-expander-fetching {
    position: relative;
    background: var(--sp-select, rgba(79,70,229,0.1)) !important;
    color: var(--sp-primary) !important;
}
[data-pivot-workspace-root] .sp-expander-fetching::after {
    content: '';
    position: absolute;
    inset: -2px;
    border-radius: 6px;
    border: 1.5px solid var(--sp-primary);
    pointer-events: none;
    animation: pivot-halo-pulse 900ms cubic-bezier(0.16, 1, 0.3, 1) infinite;
}
@media (prefers-reduced-motion: reduce) {
    [data-pivot-workspace-root] .pivot-row-animated {
        animation: none !important;
        transition: none !important;
    }
    [data-pivot-workspace-root] .sp-expander-fetching::after {
        animation: none !important;
    }
}
@keyframes pivot-skeleton-shimmer {
    0% { background-position: 220% 0; }
    100% { background-position: -220% 0; }
}
@keyframes pivot-spinner-rotate {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
[data-pivot-workspace-root],
[data-pivot-workspace-root] * {
    scrollbar-width: thin;
    scrollbar-color: var(--sp-scrollbar-thumb, rgba(148,163,184,0.35)) transparent;
}
[data-pivot-workspace-root] *::-webkit-scrollbar {
    width: 9px;
    height: 9px;
}
[data-pivot-workspace-root] *::-webkit-scrollbar-track {
    background: transparent;
}
[data-pivot-workspace-root] *::-webkit-scrollbar-corner {
    background: transparent;
}
[data-pivot-workspace-root] *::-webkit-scrollbar-thumb {
    background-color: var(--sp-scrollbar-thumb, rgba(148,163,184,0.32));
    border-radius: 999px;
    border: 2px solid transparent;
    background-clip: content-box;
    transition: background-color 150ms ease;
}
[data-pivot-workspace-root] *::-webkit-scrollbar-thumb:hover {
    background-color: var(--sp-scrollbar-thumb-hover, rgba(148,163,184,0.52));
}
[data-pivot-workspace-root] .sp-disclosure-grid {
    display: grid;
    grid-template-rows: 0fr;
    opacity: 0;
    transition: grid-template-rows 220ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)), opacity 180ms ease;
}
[data-pivot-workspace-root] .sp-disclosure-grid[data-expanded="true"] {
    grid-template-rows: 1fr;
    opacity: 1;
}
[data-pivot-workspace-root] .sp-disclosure-inner {
    overflow: hidden;
    min-height: 0;
}
[data-pivot-workspace-root] .sp-chip-remove {
    opacity: 0.55;
    border-radius: 5px;
    padding: 2px;
    transition: opacity 130ms ease, background-color 130ms ease, color 130ms ease, transform 110ms var(--sp-ease-apple, ease);
}
[data-pivot-workspace-root] .sp-chip-remove:hover {
    opacity: 1 !important;
    background: rgba(239, 68, 68, 0.14);
    color: #ef4444 !important;
}
[data-pivot-workspace-root] .sp-chip-remove:active {
    transform: scale(0.9);
}
[data-pivot-workspace-root] .sp-drop-line {
    box-shadow: 0 0 10px var(--sp-focus-ring, rgba(59,130,246,0.45));
    animation: pivot-backdrop-enter 120ms ease both;
}
[data-pivot-workspace-root] .sp-toolbar-drawer {
    animation: pivot-menu-enter 160ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both;
}
[data-pivot-workspace-root] .sp-drawer-slide {
    animation: pivot-surface-enter 200ms var(--sp-ease-apple, cubic-bezier(0.22, 1, 0.36, 1)) both;
}
@media (prefers-reduced-motion: reduce) {
    [data-pivot-workspace-root] *,
    [data-pivot-workspace-root] *::before,
    [data-pivot-workspace-root] *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
    }
}
`;

/** @param {any} p0 @returns {any} */
export function ExportDeliveryOverlay({exportProgress, isCopying, theme, cancelActiveExportRequest, retryExportJob, dismissExportJob}) {
    /** @type {any} */
    const progress = exportProgress && typeof exportProgress === 'object' ? exportProgress : null;
    const phase = progress?.phase;
    /** @type {any} */
    const delivering = progress?.delivery === 'pending';
    /** @type {any} */
    const terminal = !delivering && ['cancelled', 'completed', 'failed', 'interrupted'].includes(phase);
    /** @type {any} */
    const percent = progress ? progress.percent : typeof exportProgress === 'number' ? exportProgress : null;
    /** @type {Record<string, any>} */
    const labels = {queued: 'Queued', running: 'Running', cancelling: 'Cancelling — waiting for server cleanup',
        cancelled: 'Cancelled', completed: delivering ? 'Preparing download…' : progress?.deliveryFailed ? 'Completed — download failed' : 'Completed', failed: 'Failed', interrupted: 'Interrupted — retry'};
    const safeBg = theme?.surfaceBg || theme?.background || '#fff';
    const safeBorder = theme?.border || 'rgba(128,128,128,0.25)';
    const safePrimary = theme?.primary || theme?.accent || '#2563eb';
    const safeSubtleBg = theme?.headerSubtleBg || theme?.hover || 'rgba(128,128,128,0.12)';
    const safeText = theme?.text || 'inherit';
    return (
        <div role="dialog" aria-label="Export progress" aria-modal="true" data-export-phase={phase || 'running'} className="sp-dialog-backdrop" style={{position: 'fixed', inset: 0, background: 'rgba(15, 23, 42, 0.44)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)', zIndex: 10003, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16}}>
            <div className="sp-dialog-surface" style={{background: safeBg, color: safeText, borderRadius: 14, padding: '28px 34px 24px', boxShadow: theme?.shadowMd || '0 18px 48px rgba(0,0,0,0.32)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, minWidth: 300, maxWidth: '92vw', border: `1px solid ${safeBorder}`}}>
                <span role="status" aria-live="polite" style={{fontSize: 14, fontWeight: 600}}>{isCopying ? 'Copying to clipboard…' : labels[phase] || 'Exporting all rows…'}</span>
                {!terminal && <div role="progressbar" aria-label="Export rows" aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent === null ? undefined : percent}
                    style={{width: '100%', height: 6, background: safeBorder, borderRadius: 999, overflow: 'hidden', position: 'relative'}}>
                    <style>{`@keyframes pivot-bar-slide{0%{transform:translateX(-150%)}100%{transform:translateX(350%)}}`}</style>
                    <div style={{position: 'absolute', left: 0, top: 0, height: '100%', width: percent === null ? '40%' : `${percent}%`, borderRadius: 999,
                        background: safePrimary, transition: percent === null ? undefined : 'width 180ms cubic-bezier(0.22, 1, 0.36, 1)', animation: percent === null ? 'pivot-bar-slide 1.4s ease-in-out infinite' : undefined}}/>
                </div>}
                {progress && <span style={{fontSize: 12, color: theme?.textSec || safeText, fontVariantNumeric: 'tabular-nums'}}>{progress.rowsWritten.toLocaleString()} rows · {progress.bytesWritten.toLocaleString()} bytes</span>}
                {!terminal && percent !== null && <span style={{fontSize: 12, fontWeight: 600, fontVariantNumeric: 'tabular-nums'}}>{percent}%</span>}
                {terminal ? <div style={{display: 'flex', gap: 10, marginTop: 4}}>
                    {['failed', 'interrupted', 'cancelled'].includes(phase) || progress?.deliveryFailed ? <button type="button" onClick={retryExportJob} className="sp-btn sp-btn-primary" style={{padding: '7px 18px', background: safePrimary, color: '#fff', border: `1px solid ${safePrimary}`, borderRadius: 8, fontSize: 12, fontWeight: 600, cursor: 'pointer'}}>Retry</button> : null}
                    <button type="button" onClick={dismissExportJob} className="sp-btn sp-btn-subtle" style={{padding: '7px 18px', background: safeSubtleBg, color: safeText, border: `1px solid ${safeBorder}`, borderRadius: 8, fontSize: 12, fontWeight: 600, cursor: 'pointer'}}>Close</button>
                </div> : <button type="button" onClick={cancelActiveExportRequest} disabled={delivering || phase === 'cancelling'}
                    className="sp-btn sp-btn-primary"
                    style={{padding: '7px 26px', background: '#dc2626', color: '#fff', border: 'none', borderRadius: 8, fontSize: 12, fontWeight: 600, opacity: delivering || phase === 'cancelling' ? 0.6 : 1, cursor: delivering || phase === 'cancelling' ? 'wait' : 'pointer'}}>
                    {delivering ? 'Downloading…' : phase === 'cancelling' ? 'Cancelling…' : 'Cancel'}
                </button>}
            </div>
        </div>
    );
}

/** @param {any} p0 @returns {any} */
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
        sparklineDataModal, srOnly, stickyHeaders, style, styles, table, theme, themeCssVars, uiConfig,
        updateChartCanvasPane, viewMode, visibleLeafColumns, zoomLevel,
        loadWebFonts = true, immersiveExitPosition = 'top-right',
        effectiveFontFamily, effectiveFontSize,
        rowHeight, headerHeight,
        setColFields, setValConfigs,
    } = model;

    /** @type {any} */
    const shouldLoadWebFonts = loadWebFonts !== false && !(theme && theme.fontFamily);
    /** @type {any} */
    const computedAnimationStyles = (shouldLoadWebFonts ? GOOGLE_FONTS_IMPORT_STYLE : '') + baseAnimationStyles;
    /** @type {any} */
    const resolvedExitPosition = immersiveExitPosition || (theme && theme.immersiveExitPosition) || 'top-right';
    /** @type {any} */
    const resolvedFontFamily = effectiveFontFamily || (theme && theme.fontFamily) || fontFamily;
    /** @type {any} */
    const resolvedFontSize = effectiveFontSize || fontSize;
    /** @type {Record<string, any>} */
    const resolvedThemeCssVars = {
        colorScheme: theme && theme.isDark ? 'dark' : 'light',
        '--sp-scrollbar-thumb': theme && theme.isDark ? 'rgba(255, 255, 255, 0.22)' : 'rgba(15, 23, 42, 0.18)',
        '--sp-scrollbar-thumb-hover': theme && theme.isDark ? 'rgba(255, 255, 255, 0.38)' : 'rgba(15, 23, 42, 0.34)',
        ...(Number.isFinite(Number(rowHeight)) && Number(rowHeight) > 0 ? { '--sp-row-height': `${Number(rowHeight)}px` } : {}),
        ...(Number.isFinite(Number(headerHeight || rowHeight)) && Number(headerHeight || rowHeight) > 0
            ? { '--sp-header-height': `${Number(headerHeight || rowHeight)}px` }
            : {}),
        ...(themeCssVars || {}),
    };

return (
        <div
            id={id}
            data-pivot-workspace-root="true"
            data-header-controls={(theme && theme.headerControls) || 'always'}
            style={{ colorScheme: theme && theme.isDark ? 'dark' : 'light', ...styles.root, fontFamily: resolvedFontFamily, fontSize: resolvedFontSize, ...resolvedThemeCssVars, ...loadingCssVars, position: 'relative', ...style }}
            onMouseEnter={() => setImmersiveModeHovering(true)}
            onMouseLeave={() => setImmersiveModeHovering(false)}
        >
            <style>{computedAnimationStyles}</style>
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
            {immersiveMode && !uiConfig.lockImmersiveMode && resolvedExitPosition !== 'hidden' && (
                <button
                    data-immersive-exit-overlay
                    onClick={() => setImmersiveMode(false)}
                    title="Exit Immersive Mode"
                    style={{
                        position: 'absolute',
                        ...(resolvedExitPosition === 'bottom-right' ? { bottom: 12, right: 12 } : { top: 12, right: 12 }),
                        zIndex: 9999,
                        background: 'rgba(18, 22, 30, 0.72)', color: '#fff',
                        border: '1px solid rgba(255,255,255,0.16)', borderRadius: 999, padding: '7px 14px',
                        boxShadow: '0 8px 24px rgba(0,0,0,0.28)',
                        cursor: 'pointer', fontSize: 12, fontWeight: 600,
                        display: 'flex', alignItems: 'center', gap: 6,
                        backdropFilter: 'blur(14px) saturate(180%)',
                        WebkitBackdropFilter: 'blur(14px) saturate(180%)',
                        opacity: immersiveModeHovering ? 0.96 : 0,
                        pointerEvents: immersiveModeHovering ? 'auto' : 'none',
                        transform: immersiveModeHovering
                            ? 'translateY(0)'
                            : (resolvedExitPosition === 'bottom-right' ? 'translateY(6px)' : 'translateY(-6px)'),
                        transition: 'opacity 180ms cubic-bezier(0.22, 1, 0.36, 1), transform 180ms cubic-bezier(0.22, 1, 0.36, 1)',
                    }}
                >
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>
                    <span>Exit Immersive</span>
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
            <div style={{display:'flex', flex:1, overflow:'hidden', fontFamily: resolvedFontFamily, fontSize: resolvedFontSize}}>
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
                /** @type {any} */
                const previewPane = chartCanvasPanes.find(/** @param {any} [p] @returns {any} */ (p) => p.id === previewChartPaneId);
                if (!previewPane) return null;
                /** @type {Record<string, any>} */
                const btnBase = { padding: '7px 16px', borderRadius: theme.radiusSm || '8px', fontSize: '12px', fontWeight: 700, cursor: 'pointer', border: 'none', display: 'inline-flex', alignItems: 'center', gap: '6px' };
                return (
                    <div className="sp-dialog-backdrop" style={{ position: 'fixed', inset: 0, background: 'rgba(15, 23, 42, 0.45)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)', zIndex: 10050, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <div className="sp-dialog-surface" style={{ background: theme.surfaceBg || theme.background, border: `1px solid ${theme.border}`, borderRadius: theme.radius || '16px', boxShadow: theme.shadowMd || '0 16px 48px rgba(0,0,0,0.32)', width: '86vw', maxWidth: '1160px', height: '80vh', maxHeight: '840px', display: 'flex', flexDirection: 'column', overflow: 'hidden', fontFamily: fontFamily }}>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 20px', borderBottom: `1px solid ${theme.border}`, flexShrink: 0, background: theme.headerBg || theme.surfaceBg || theme.background }}>
                                <span style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>New Chart Pane</span>
                                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                    <button onClick={handleCancelPreviewChartPane} className="sp-btn sp-btn-subtle" style={{ ...btnBase, background: theme.headerSubtleBg || theme.hover, color: theme.text, border: `1px solid ${theme.border}` }}>Cancel</button>
                                    <button onClick={handleConfirmPreviewChartPane} className="sp-btn sp-btn-primary" style={{ ...btnBase, background: theme.primary, color: '#fff' }}>Open in Panel</button>
                                </div>
                            </div>
                            <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex' }}>
                                <PivotChartPanel
                                    open
                                    onClose={handleCancelPreviewChartPane}
                                    source={previewPane.source}
                                    onSourceChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { source: value })}
                                    chartType={previewPane.chartType}
                                    onChartTypeChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { chartType: value })}
                                    chartLayers={previewPane.chartLayers}
                                    onChartLayersChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { chartLayers: value })}
                                    availableColumns={resolveChartAvailableColumns(chartPaneDataById[previewPane.id], visibleLeafColumns)}
                                    barLayout={previewPane.barLayout}
                                    onBarLayoutChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { barLayout: value })}
                                    axisMode={previewPane.axisMode}
                                    onAxisModeChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { axisMode: value })}
                                    orientation={previewPane.orientation}
                                    onOrientationChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { orientation: value })}
                                    hierarchyLevel={previewPane.hierarchyLevel}
                                    onHierarchyLevelChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { hierarchyLevel: value })}
                                    chartTitle={previewPane.chartTitle || previewPane.name}
                                    onChartTitleChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { chartTitle: value })}
                                    rowLimit={previewPane.rowLimit}
                                    onRowLimitChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { rowLimit: value })}
                                    columnLimit={previewPane.columnLimit}
                                    onColumnLimitChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { columnLimit: value })}
                                    chartHeight={previewPane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT}
                                    onChartHeightChange={/** @param {any} [value] @returns {any} */ (value) => {
                                        /** @type {any} */
                                        const nextHeight = Number(value);
                                        updateChartCanvasPane(previewPane.id, {
                                            chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT,
                                        });
                                    }}
                                    sortMode={previewPane.sortMode}
                                    onSortModeChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { sortMode: value })}
                                    interactionMode={previewPane.interactionMode}
                                    onInteractionModeChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { interactionMode: value })}
                                    serverScope={previewPane.serverScope}
                                    onServerScopeChange={/** @param {any} [value] @returns {any} */ (value) => updateChartCanvasPane(previewPane.id, { serverScope: value })}
                                    showServerScope={serverSide}
                                    model={chartCanvasPaneModels[previewPane.id] || null}
                                    theme={theme}
                                    onCategoryActivate={/** @param {any} [target] @returns {any} */ (target) => activateChartCategory(previewPane.source, previewPane.interactionMode, target)}
                                    floating={false}
                                    initialSettings={previewPane.chartSettings || {}}
                                    onSettingsChange={/** @param {any} [settings] @returns {any} */ (settings) => updateChartCanvasPane(previewPane.id, { chartSettings: settings })}
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
                /** @type {any} */
                const rowBatches = Math.ceil(totalRows / rowsPerPart);
                /** @type {any} */
                const colBatches = colsPerPart < totalCols ? Math.ceil(totalCols / colsPerPart) : 1;
                /** @type {any} */
                const totalParts = rowBatches * colBatches;
                /** @type {any} */
                const tooMany = totalParts > 50;
                /** @type {any} */
                const limit = mode === 'copy' ? COPY_ROW_LIMIT : EXPORT_ROW_LIMIT;
                /** @type {Record<string, any>} */
                const inp = { display:'block', width:'100%', marginTop:'4px', padding:'6px 8px', borderRadius:'6px', border:`1px solid ${theme.border}`, background: theme.input || theme.background, color:theme.text, fontSize:'13px', boxSizing:'border-box' };
                /** @type {Record<string, any>} */
                const btnBase = { padding:'6px 12px', borderRadius:'6px', border:'none', cursor:'pointer', fontSize:'12px', fontWeight:600, transition:'opacity .15s' };
                return (
                    <div className="sp-dialog-backdrop" style={{ position:'fixed', inset:0, background:'rgba(15, 23, 42, 0.45)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)', zIndex:10002, display:'flex', alignItems:'center', justifyContent:'center' }}>
                        <div className="sp-dialog-surface" style={{ background:theme.surfaceBg || theme.background, border: `1px solid ${theme.border}`, color:theme.text, borderRadius:'14px', padding:'24px', boxShadow: theme.shadowMd || '0 12px 40px rgba(0,0,0,0.3)', width:'440px', maxWidth:'94vw', maxHeight:'85vh', display:'flex', flexDirection:'column', gap:'14px', fontFamily: resolvedFontFamily || "ui-sans-serif,system-ui,sans-serif" }}>
                            <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between' }}>
                                <span style={{ fontWeight:700, fontSize:'15px' }}>{mode === 'copy' ? 'Copy Table — Large Dataset' : 'Export — Large Dataset'}</span>
                                <button type="button" onClick={cancelActiveExportRequest} aria-label="Close" className="sp-btn sp-btn-ghost" style={{ background:'transparent', border:'none', cursor:'pointer', color:theme.textSec, display:'inline-flex', alignItems:'center', justifyContent:'center', width: 28, height: 28, borderRadius: 7 }}><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg></button>
                            </div>
                            <div style={{ fontSize:'13px', color:theme.textSec || theme.text, opacity:.75 }}>
                                {totalRows.toLocaleString()} rows × {totalCols} columns — limit is {limit.toLocaleString()} rows. Split into parts below.
                            </div>
                            <div style={{ display:'flex', gap:'12px' }}>
                                <label style={{ flex:1, fontSize:'13px' }}>
                                    Rows per part
                                    <input type="number" min={100} max={totalRows} value={rowsPerPart}
                                        onChange={e => setPartModal(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, rowsPerPart: Math.max(100, Math.min(totalRows, parseInt(e.target.value) || 100)) }))}
                                        style={inp} />
                                </label>
                                <label style={{ flex:1, fontSize:'13px' }}>
                                    Columns per part
                                    <input type="number" min={1} max={totalCols} value={colsPerPart}
                                        onChange={e => setPartModal(/** @param {any} [prev] @returns {any} */ prev => ({ ...prev, colsPerPart: Math.max(1, Math.min(totalCols, parseInt(e.target.value) || 1)) }))}
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
                                            /** @type {any} */
                                            const done = completedParts.has(pid);
                                            /** @type {any} */
                                            const loading = processingPartId === pid;
                                            /** @type {any} */
                                            const rowFrom = rb * rowsPerPart + 1;
                                            /** @type {any} */
                                            const rowTo = Math.min((rb + 1) * rowsPerPart, totalRows);
                                            /** @type {any} */
                                            const label = colBatches > 1
                                                ? `R${rb+1}–C${cb+1} (${rowFrom.toLocaleString()}–${rowTo.toLocaleString()})`
                                                : `Part ${rb + 1}  (rows ${rowFrom.toLocaleString()}–${rowTo.toLocaleString()})`;
                                            return (
                                                <button key={pid} disabled={!!processingPartId}
                                                    type="button"
                                                    className="sp-btn sp-btn-subtle"
                                                    onClick={() => dispatchExportPart(rb, cb)}
                                                    style={{
                                                        ...btnBase,
                                                        display: 'inline-flex',
                                                        alignItems: 'center',
                                                        justifyContent: 'center',
                                                        gap: '6px',
                                                        background: done ? (theme.accent || theme.primary || '#0a84ff') : (theme.headerBg || theme.border),
                                                        color: done ? '#fff' : theme.text,
                                                        opacity: processingPartId && !loading ? 0.5 : 1,
                                                    }}>
                                                    {loading && (
                                                        <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ animation: 'pivot-spinner-rotate 0.75s linear infinite', flexShrink: 0 }}>
                                                            <circle cx="8" cy="8" r="6" stroke="currentColor" strokeOpacity="0.25" strokeWidth="2" />
                                                            <path d="M14 8a6 6 0 0 0-6-6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                                                        </svg>
                                                    )}
                                                    {!loading && done && (
                                                        <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
                                                            <path d="M3.5 8.5L6.5 11.5L12.5 4.5" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" />
                                                        </svg>
                                                    )}
                                                    <span>{label}</span>
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
                    className="sp-dialog-backdrop"
                    style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.42)', backdropFilter: 'blur(6px)', WebkitBackdropFilter: 'blur(6px)', zIndex: 10000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
                    onClick={() => setSparklineDataModal(null)}
                >
                    <div
                        className="sp-dialog-surface"
                        role="dialog"
                        aria-modal="true"
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background: theme.surfaceBg || theme.background,
                            border: `1px solid ${theme.border}`,
                            borderRadius: '14px',
                            boxShadow: theme.shadow || '0 20px 56px rgba(0,0,0,0.28)',
                            width: 'min(940px, 94vw)',
                            maxWidth: '94vw',
                            maxHeight: '82vh',
                            display: 'flex',
                            flexDirection: 'column',
                            fontFamily: resolvedFontFamily || "ui-sans-serif, system-ui, sans-serif",
                            overflow: 'hidden',
                        }}
                    >
                        {/* Modal header */}
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 18px 12px', borderBottom: `1px solid ${theme.border}` }}>
                            <span style={{ fontSize: '13px', fontWeight: 700, letterSpacing: '-0.01em', color: theme.text }}>
                                {sparklineDataModal.headerLabel || 'Trend Data'}
                            </span>
                            <button
                                type="button"
                                className="sp-btn sp-btn-ghost"
                                aria-label="Close trend data dialog"
                                onClick={() => setSparklineDataModal(null)}
                                style={{ background: 'transparent', border: 'none', borderRadius: '6px', cursor: 'pointer', color: theme.textSec, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: '28px', height: '28px', padding: 0 }}
                            >
                                <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true">
                                    <path d="M4 4l8 8M12 4l-8 8" />
                                </svg>
                            </button>
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
                                        {(sparklineDataModal.points || []).map(/** @param {any} [point] @param {any} [i] @returns {any} */ (point, i) => (
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

/**
 * PivotChartRenderer
 * 
 * Renders chart canvas panes, dock groups, and chart modals for pivot table charts.
 * Handles:
 * - Docked chart panes (left, right, top, bottom positions)
 * - Chart modal display (4 positions)
 * - Resize handles between panes
 * - Floating chart panes
 * 
 * Extracted from DashTanstackPivot.react.js (Phase 4 of refactoring)
 */

import React from 'react';
import PropTypes from 'prop-types';
import { PivotChartPanel, PivotChartModal } from './Charts/PivotCharts';
import { normalizeChartDockPosition, resolveChartAvailableColumns } from '../hooks/usePivotNormalization';

// Constants (imported from normalization utilities)
const MIN_CHART_CANVAS_PANE_WIDTH = 320;
const DEFAULT_DOCKED_CHART_PANE_HEIGHT = 420;
const DEFAULT_CHART_GRAPH_HEIGHT = 320;

/**
 * Renders a single chart canvas pane with proper sizing and docking
 */
const renderChartCanvasPane = (
    pane, 
    dockPosition, 
    {
        theme,
        chartCanvasPaneWidthHints,
        chartPaneDataById,
        visibleLeafColumns,
        serverSide,
        chartCanvasPaneModels,
        onRemoveChartCanvasPane,
        onUpdateChartCanvasPane,
        onToggleChartCanvasPaneFloating,
        onStartChartCanvasPaneFloatingDrag,
        onStartChartCanvasPaneFloatingResize,
        onToggleChartCanvasPaneLock,
        onActivateChartCategory,
        onSettingsWidthBudgetChange,
    }
) => {
    const normalizedDockPosition = normalizeChartDockPosition(dockPosition || pane.dockPosition, 'right');
    const widthHint = Number.isFinite(Number(chartCanvasPaneWidthHints[pane.id]))
        ? Math.max(MIN_CHART_CANVAS_PANE_WIDTH, Math.floor(Number(chartCanvasPaneWidthHints[pane.id])))
        : null;
    const isVerticalDock = normalizedDockPosition === 'top' || normalizedDockPosition === 'bottom';
    const basePaneStyle = isVerticalDock
        ? {
            display: 'flex',
            width: '100%',
            height: `${Math.max(DEFAULT_DOCKED_CHART_PANE_HEIGHT, Math.floor((pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + 188))}px`,
            minHeight: `${Math.max(280, Math.floor((pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT) + 120))}px`,
            minWidth: 0,
            overflow: 'hidden',
            borderTop: normalizedDockPosition === 'bottom' ? `1px solid ${theme.border}` : 'none',
            borderBottom: normalizedDockPosition === 'top' ? `1px solid ${theme.border}` : 'none',
            flexShrink: 0,
        }
        : {
            display: 'flex',
            flexGrow: widthHint === null ? pane.size : 0,
            flexBasis: widthHint === null ? 0 : `${widthHint}px`,
            width: widthHint === null ? undefined : `${widthHint}px`,
            minWidth: widthHint === null ? `${MIN_CHART_CANVAS_PANE_WIDTH}px` : `${widthHint}px`,
            minHeight: 0,
            overflow: 'hidden',
            borderLeft: normalizedDockPosition === 'right' ? `1px solid ${theme.border}` : 'none',
            borderRight: normalizedDockPosition === 'left' ? `1px solid ${theme.border}` : 'none',
        };

    return (
        <div
            key={pane.id}
            data-docked-chart-pane={pane.id}
            data-docked-chart-pane-position={normalizedDockPosition}
            style={basePaneStyle}
        >
            <PivotChartPanel
                open
                onClose={() => onRemoveChartCanvasPane(pane.id)}
                source={pane.source}
                onSourceChange={(value) => onUpdateChartCanvasPane(pane.id, { source: value })}
                chartType={pane.chartType}
                onChartTypeChange={(value) => onUpdateChartCanvasPane(pane.id, { chartType: value })}
                chartLayers={pane.chartLayers}
                onChartLayersChange={(value) => onUpdateChartCanvasPane(pane.id, { chartLayers: value })}
                availableColumns={resolveChartAvailableColumns(chartPaneDataById[pane.id], visibleLeafColumns)}
                barLayout={pane.barLayout}
                onBarLayoutChange={(value) => onUpdateChartCanvasPane(pane.id, { barLayout: value })}
                axisMode={pane.axisMode}
                onAxisModeChange={(value) => onUpdateChartCanvasPane(pane.id, { axisMode: value })}
                orientation={pane.orientation}
                onOrientationChange={(value) => onUpdateChartCanvasPane(pane.id, { orientation: value })}
                hierarchyLevel={pane.hierarchyLevel}
                onHierarchyLevelChange={(value) => onUpdateChartCanvasPane(pane.id, { hierarchyLevel: value })}
                chartTitle={pane.chartTitle || pane.name}
                onChartTitleChange={(value) => onUpdateChartCanvasPane(pane.id, { chartTitle: value })}
                rowLimit={pane.rowLimit}
                onRowLimitChange={(value) => onUpdateChartCanvasPane(pane.id, { rowLimit: value })}
                columnLimit={pane.columnLimit}
                onColumnLimitChange={(value) => onUpdateChartCanvasPane(pane.id, { columnLimit: value })}
                chartHeight={pane.chartHeight || DEFAULT_CHART_GRAPH_HEIGHT}
                onChartHeightChange={(value) => {
                    const nextHeight = Number(value);
                    onUpdateChartCanvasPane(pane.id, {
                        chartHeight: Number.isFinite(nextHeight) ? Math.max(180, Math.floor(nextHeight)) : DEFAULT_CHART_GRAPH_HEIGHT,
                    });
                }}
                sortMode={pane.sortMode}
                onSortModeChange={(value) => onUpdateChartCanvasPane(pane.id, { sortMode: value })}
                interactionMode={pane.interactionMode}
                onInteractionModeChange={(value) => onUpdateChartCanvasPane(pane.id, { interactionMode: value })}
                serverScope={pane.serverScope}
                onServerScopeChange={(value) => onUpdateChartCanvasPane(pane.id, { serverScope: value })}
                showServerScope={serverSide}
                model={chartCanvasPaneModels[pane.id] || null}
                theme={theme}
                onCategoryActivate={(target) => onActivateChartCategory(pane.source, pane.interactionMode, target)}
                floating={false}
                onToggleFloating={() => onToggleChartCanvasPaneFloating(pane.id)}
                floatingRect={pane.floatingRect}
                onFloatingDragStart={(event) => onStartChartCanvasPaneFloatingDrag(pane.id, event)}
                onFloatingResizeStart={(direction, event) => onStartChartCanvasPaneFloatingResize(pane.id, direction, event)}
                standalone
                showResizeHandle={false}
                title={pane.name}
                showDefinitionManager={false}
                locked={pane.locked}
                onToggleLock={() => onToggleChartCanvasPaneLock(pane.id)}
                immersiveMode={Boolean(pane.immersiveMode)}
                onImmersiveModeChange={(value) => onUpdateChartCanvasPane(pane.id, { immersiveMode: Boolean(value) })}
                dockPosition={normalizedDockPosition}
                onDockPositionChange={(value) => onUpdateChartCanvasPane(pane.id, { dockPosition: normalizeChartDockPosition(value, normalizedDockPosition) })}
                onSettingsWidthBudgetChange={(nextWidthHint) => onSettingsWidthBudgetChange(
                    pane.id,
                    normalizedDockPosition === 'left' || normalizedDockPosition === 'right'
                        ? nextWidthHint
                        : null
                )}
            />
        </div>
    );
};

/**
 * Renders resize handle for horizontal docking
 */
const HorizontalResizeHandle = ({ leftKey, rightKey, onStartChartCanvasResize, theme }) => (
    <div
        onMouseDown={(event) => {
            event.preventDefault();
            onStartChartCanvasResize(leftKey, rightKey, event);
        }}
        style={{
            width: '8px',
            cursor: 'col-resize',
            background: 'transparent',
            position: 'relative',
            flexShrink: 0,
        }}
        title="Resize workspace panes"
    >
        <div style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: '3px',
            height: '78px',
            borderRadius: '999px',
            background: theme.border,
            opacity: 0.92,
        }} />
    </div>
);

HorizontalResizeHandle.propTypes = {
    leftKey: PropTypes.string.isRequired,
    rightKey: PropTypes.string.isRequired,
    onStartChartCanvasResize: PropTypes.func.isRequired,
    theme: PropTypes.object.isRequired,
};

/**
 * Renders a group of horizontally docked chart panes (left or right)
 */
const renderHorizontalDockGroup = (panes, groupPosition, renderPane, onStartChartCanvasResize, showCharts, theme) => {
    if (!showCharts) return null;
    if (!Array.isArray(panes) || panes.length === 0) return null;
    return (
        <div
            data-docked-chart-group={groupPosition}
            style={{ display: 'flex', minWidth: 0, minHeight: 0, overflow: 'hidden', flexShrink: 0 }}
        >
            {panes.map((pane, index) => {
                const previousPane = index > 0 ? panes[index - 1] : null;
                const nextPane = index < panes.length - 1 ? panes[index + 1] : null;
                const resizeLeftKey = groupPosition === 'left'
                    ? pane.id
                    : (index === 0 ? 'table' : previousPane.id);
                const resizeRightKey = groupPosition === 'left'
                    ? (nextPane ? nextPane.id : 'table')
                    : pane.id;
                const shouldRenderBeforeHandle = groupPosition === 'right';
                const shouldRenderAfterHandle = groupPosition === 'left';

                return (
                    <React.Fragment key={pane.id}>
                        {shouldRenderBeforeHandle ? (
                            <HorizontalResizeHandle
                                leftKey={resizeLeftKey}
                                rightKey={resizeRightKey}
                                onStartChartCanvasResize={onStartChartCanvasResize}
                                theme={theme}
                            />
                        ) : null}
                        {renderPane(pane, groupPosition)}
                        {shouldRenderAfterHandle ? (
                            <HorizontalResizeHandle
                                leftKey={resizeLeftKey}
                                rightKey={resizeRightKey}
                                onStartChartCanvasResize={onStartChartCanvasResize}
                                theme={theme}
                            />
                        ) : null}
                    </React.Fragment>
                );
            })}
        </div>
    );
};

/**
 * Renders resize handle for vertical docking
 */
const VerticalResizeHandle = ({ paneId, groupPosition, onStartChartCanvasVerticalResize, theme }) => (
    <div
        onMouseDown={(event) => {
            event.preventDefault();
            onStartChartCanvasVerticalResize(paneId, groupPosition, event);
        }}
        style={{
            height: '8px',
            cursor: 'row-resize',
            background: 'transparent',
            position: 'relative',
            flexShrink: 0,
            width: '100%',
        }}
        title="Resize chart pane"
    >
        <div style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            height: '3px',
            width: '78px',
            borderRadius: '999px',
            background: theme.border,
            opacity: 0.92,
        }} />
    </div>
);

VerticalResizeHandle.propTypes = {
    paneId: PropTypes.string.isRequired,
    groupPosition: PropTypes.oneOf(['top', 'bottom']).isRequired,
    onStartChartCanvasVerticalResize: PropTypes.func.isRequired,
    theme: PropTypes.object.isRequired,
};

/**
 * Renders a group of vertically docked chart panes (top or bottom)
 */
const renderVerticalDockGroup = (panes, groupPosition, renderPane, onStartChartCanvasVerticalResize, showCharts, theme) => {
    if (!showCharts) return null;
    if (!Array.isArray(panes) || panes.length === 0) return null;
    return (
        <div
            data-docked-chart-group={groupPosition}
            style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, overflow: 'hidden', flexShrink: 0 }}
        >
            {panes.map((pane) => (
                <React.Fragment key={pane.id}>
                    {groupPosition === 'bottom' ? (
                        <VerticalResizeHandle
                            paneId={pane.id}
                            groupPosition={groupPosition}
                            onStartChartCanvasVerticalResize={onStartChartCanvasVerticalResize}
                            theme={theme}
                        />
                    ) : null}
                    {renderPane(pane, groupPosition)}
                    {groupPosition === 'top' ? (
                        <VerticalResizeHandle
                            paneId={pane.id}
                            groupPosition={groupPosition}
                            onStartChartCanvasVerticalResize={onStartChartCanvasVerticalResize}
                            theme={theme}
                        />
                    ) : null}
                </React.Fragment>
            ))}
        </div>
    );
};

/**
 * Main PivotChartRenderer component
 */
export function PivotChartRenderer({
    chartCanvasPanes,
    chartModal,
    chartModalPosition,
    chartCanvasPaneWidthHints,
    chartPaneDataById,
    visibleLeafColumns,
    theme,
    showCharts,
    serverSide,
    chartCanvasPaneModels,
    onRemoveChartCanvasPane,
    onUpdateChartCanvasPane,
    onToggleChartCanvasPaneFloating,
    onStartChartCanvasPaneFloatingDrag,
    onStartChartCanvasPaneFloatingResize,
    onToggleChartCanvasPaneLock,
    onStartChartCanvasResize,
    onStartChartCanvasVerticalResize,
    onActivateChartCategory,
    onSetChartModal,
    onSetChartModalPosition,
    onSettingsWidthBudgetChange,
}) {
    // Filter docked and floating panes
    const dockedChartCanvasPanes = chartCanvasPanes.filter((pane) => !pane.floating);
    
    // Group docked panes by position
    const dockedChartCanvasPanesByPosition = {
        left: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'left'),
        right: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'right'),
        top: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'top'),
        bottom: dockedChartCanvasPanes.filter((pane) => normalizeChartDockPosition(pane.dockPosition, 'right') === 'bottom'),
    };

    // Create bound render function
    const renderPane = (pane, dockPosition) => renderChartCanvasPane(pane, dockPosition, {
        theme,
        chartCanvasPaneWidthHints,
        chartPaneDataById,
        visibleLeafColumns,
        serverSide,
        chartCanvasPaneModels,
        onRemoveChartCanvasPane,
        onUpdateChartCanvasPane,
        onToggleChartCanvasPaneFloating,
        onStartChartCanvasPaneFloatingDrag,
        onStartChartCanvasPaneFloatingResize,
        onToggleChartCanvasPaneLock,
        onActivateChartCategory,
        onSettingsWidthBudgetChange,
    });

    return (
        <>
            {/* Chart Modal - Top Position */}
            {showCharts && chartModal && chartModalPosition === 'top' ? (
                <PivotChartModal
                    chartState={chartModal}
                    onClose={() => onSetChartModal(null)}
                    theme={theme}
                    position={chartModalPosition}
                    onPositionChange={onSetChartModalPosition}
                />
            ) : null}

            {/* Top Dock Group */}
            {renderVerticalDockGroup(dockedChartCanvasPanesByPosition.top, 'top', renderPane, onStartChartCanvasVerticalResize, showCharts, theme)}

            <div style={{ display: 'flex', flex: 1, minWidth: 0, minHeight: 0, overflow: 'hidden', position: 'relative' }}>
                {/* Chart Modal - Left Position */}
                {showCharts && chartModal && chartModalPosition === 'left' ? (
                    <PivotChartModal
                        chartState={chartModal}
                        onClose={() => onSetChartModal(null)}
                        theme={theme}
                        position={chartModalPosition}
                        onPositionChange={onSetChartModalPosition}
                    />
                ) : null}

                {/* Left Dock Group */}
                {renderHorizontalDockGroup(dockedChartCanvasPanesByPosition.left, 'left', renderPane, onStartChartCanvasResize, showCharts, theme)}

                {/* Table canvas container would go here (rendered by parent) */}

                {/* Right Dock Group */}
                {renderHorizontalDockGroup(dockedChartCanvasPanesByPosition.right, 'right', renderPane, onStartChartCanvasResize, showCharts, theme)}

                {/* Chart Modal - Right Position */}
                {showCharts && chartModal && chartModalPosition === 'right' ? (
                    <PivotChartModal
                        chartState={chartModal}
                        onClose={() => onSetChartModal(null)}
                        theme={theme}
                        position={chartModalPosition}
                        onPositionChange={onSetChartModalPosition}
                    />
                ) : null}
            </div>

            {/* Bottom Dock Group */}
            {renderVerticalDockGroup(dockedChartCanvasPanesByPosition.bottom, 'bottom', renderPane, onStartChartCanvasVerticalResize, showCharts, theme)}

            {/* Chart Modal - Bottom Position */}
            {showCharts && chartModal && chartModalPosition === 'bottom' ? (
                <PivotChartModal
                    chartState={chartModal}
                    onClose={() => onSetChartModal(null)}
                    theme={theme}
                    position={chartModalPosition}
                    onPositionChange={onSetChartModalPosition}
                />
            ) : null}
        </>
    );
}

PivotChartRenderer.propTypes = {
    // Chart State
    chartCanvasPanes: PropTypes.array.isRequired,
    chartModal: PropTypes.object,
    chartModalPosition: PropTypes.oneOf(['top', 'left', 'right', 'bottom']),
    chartCanvasPaneWidthHints: PropTypes.object.isRequired,
    chartPaneDataById: PropTypes.object.isRequired,
    visibleLeafColumns: PropTypes.array.isRequired,
    
    // Chart Actions
    onRemoveChartCanvasPane: PropTypes.func.isRequired,
    onUpdateChartCanvasPane: PropTypes.func.isRequired,
    onToggleChartCanvasPaneFloating: PropTypes.func.isRequired,
    onStartChartCanvasPaneFloatingDrag: PropTypes.func.isRequired,
    onStartChartCanvasPaneFloatingResize: PropTypes.func.isRequired,
    onToggleChartCanvasPaneLock: PropTypes.func.isRequired,
    onStartChartCanvasResize: PropTypes.func.isRequired,
    onStartChartCanvasVerticalResize: PropTypes.func.isRequired,
    onActivateChartCategory: PropTypes.func.isRequired,
    onSetChartModal: PropTypes.func.isRequired,
    onSetChartModalPosition: PropTypes.func.isRequired,
    onSettingsWidthBudgetChange: PropTypes.func.isRequired,
    
    // Display Config
    theme: PropTypes.object.isRequired,
    showCharts: PropTypes.bool.isRequired,
    serverSide: PropTypes.bool.isRequired,
    chartCanvasPaneModels: PropTypes.object.isRequired,
};

export default PivotChartRenderer;

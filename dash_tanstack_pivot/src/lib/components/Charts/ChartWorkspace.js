import React from 'react';

import { normalizeChartDockPosition } from '../../hooks/usePivotNormalization';

/**
 * Split a canvas pane collection into the four dock positions and the floating
 * layer. The composition root also consumes this model for status reporting,
 * while ChartWorkspace is the only renderer of the dock layout.
 */
export const groupChartWorkspacePanes = (panes) => {
    const dockedByPosition = {
        left: [],
        right: [],
        top: [],
        bottom: [],
    };
    const docked = [];
    const floating = [];

    (Array.isArray(panes) ? panes : []).forEach((pane) => {
        if (!pane || typeof pane !== 'object' || pane.isPreview) return;
        if (pane.floating) {
            floating.push(pane);
            return;
        }
        const dockPosition = normalizeChartDockPosition(pane.dockPosition, 'right');
        docked.push(pane);
        dockedByPosition[dockPosition].push(pane);
    });

    return { docked, floating, dockedByPosition };
};

const resolvePaneWidthHint = (pane, paneWidthHints, minChartPaneWidth) => {
    const widthHint = Number(paneWidthHints && paneWidthHints[pane.id]);
    return Number.isFinite(widthHint)
        ? Math.max(minChartPaneWidth, Math.floor(widthHint))
        : null;
};

/**
 * Owns the live four-position chart workspace while accepting the table and
 * chart content as render slots. Chart data, panel configuration, and
 * interaction state remain with the composition root.
 */
export function ChartWorkspace({
    children,
    chartCanvasLayoutRef,
    chartLayoutRef,
    showCharts,
    dockedPanesByPosition,
    floatingPanes,
    paneWidthHints,
    tableCanvasSize,
    minTablePanelWidth,
    minChartPaneWidth,
    defaultDockedPaneHeight,
    defaultChartGraphHeight,
    chartPanelVerticalChromeHeight,
    theme,
    renderChartPane,
    renderFloatingPane,
    renderModal,
    tableAdjacentContent,
    onHorizontalResizeStart,
    onVerticalResizeStart,
}) {
    const paneGroups = dockedPanesByPosition || {};
    const leftPanes = Array.isArray(paneGroups.left) ? paneGroups.left : [];
    const rightPanes = Array.isArray(paneGroups.right) ? paneGroups.right : [];
    const topPanes = Array.isArray(paneGroups.top) ? paneGroups.top : [];
    const bottomPanes = Array.isArray(paneGroups.bottom) ? paneGroups.bottom : [];
    const resolvedFloatingPanes = Array.isArray(floatingPanes) ? floatingPanes : [];

    const renderModalAt = (position) => (
        showCharts && typeof renderModal === 'function' ? renderModal(position) : null
    );

    const renderDockedPane = (pane, dockPosition) => {
        const normalizedDockPosition = normalizeChartDockPosition(dockPosition || pane.dockPosition, 'right');
        const widthHint = resolvePaneWidthHint(pane, paneWidthHints, minChartPaneWidth);
        const isVerticalDock = normalizedDockPosition === 'top' || normalizedDockPosition === 'bottom';
        const chartHeight = Number(pane.chartHeight) || defaultChartGraphHeight;
        const paneStyle = isVerticalDock
            ? {
                display: 'flex',
                width: '100%',
                height: `${Math.max(defaultDockedPaneHeight, Math.floor(chartHeight + chartPanelVerticalChromeHeight))}px`,
                minHeight: `${Math.max(280, Math.floor(chartHeight + 120))}px`,
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
                minWidth: widthHint === null ? `${minChartPaneWidth}px` : `${widthHint}px`,
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
                style={paneStyle}
            >
                {typeof renderChartPane === 'function'
                    ? renderChartPane(pane, normalizedDockPosition)
                    : null}
            </div>
        );
    };

    const renderHorizontalResizeHandle = (leftKey, rightKey) => (
        <div
            onMouseDown={(event) => {
                event.preventDefault();
                if (typeof onHorizontalResizeStart === 'function') {
                    onHorizontalResizeStart(leftKey, rightKey, event);
                }
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

    const renderHorizontalDockGroup = (panes, groupPosition) => {
        if (!showCharts || panes.length === 0) return null;
        const totalGroupSize = panes.reduce((sum, pane) => sum + (Number(pane.size) || 1), 0);
        const paneHints = panes.map((pane) => ({
            pane,
            widthHint: resolvePaneWidthHint(pane, paneWidthHints, minChartPaneWidth),
        }));
        const hasPaneWidthHint = paneHints.some(({ widthHint }) => widthHint !== null);
        const hintedGroupWidth = hasPaneWidthHint
            ? paneHints.reduce((sum, { pane, widthHint }) => {
                const fallbackWidth = Math.max(
                    minChartPaneWidth,
                    Math.floor(Number(pane.width) || minChartPaneWidth)
                );
                return sum + (widthHint === null ? fallbackWidth : widthHint);
            }, panes.length * 8)
            : null;
        const groupStyle = hasPaneWidthHint
            ? {
                display: 'flex',
                minWidth: `${hintedGroupWidth}px`,
                width: `${hintedGroupWidth}px`,
                minHeight: 0,
                overflow: 'hidden',
                flex: `0 0 ${hintedGroupWidth}px`,
                flexShrink: 0,
            }
            : {
                display: 'flex',
                minWidth: 0,
                minHeight: 0,
                overflow: 'hidden',
                flexShrink: 0,
                flexGrow: totalGroupSize,
                flexBasis: 0,
            };

        return (
            <div data-docked-chart-group={groupPosition} style={groupStyle}>
                {panes.map((pane, index) => {
                    const previousPane = index > 0 ? panes[index - 1] : null;
                    const nextPane = index < panes.length - 1 ? panes[index + 1] : null;
                    const resizeLeftKey = groupPosition === 'left'
                        ? pane.id
                        : (index === 0 ? 'table' : previousPane.id);
                    const resizeRightKey = groupPosition === 'left'
                        ? (nextPane ? nextPane.id : 'table')
                        : pane.id;

                    return (
                        <React.Fragment key={pane.id}>
                            {groupPosition === 'right'
                                ? renderHorizontalResizeHandle(resizeLeftKey, resizeRightKey)
                                : null}
                            {renderDockedPane(pane, groupPosition)}
                            {groupPosition === 'left'
                                ? renderHorizontalResizeHandle(resizeLeftKey, resizeRightKey)
                                : null}
                        </React.Fragment>
                    );
                })}
            </div>
        );
    };

    const renderVerticalResizeHandle = (paneId, groupPosition) => (
        <div
            onMouseDown={(event) => {
                event.preventDefault();
                if (typeof onVerticalResizeStart === 'function') {
                    onVerticalResizeStart(paneId, groupPosition, event);
                }
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

    const renderVerticalDockGroup = (panes, groupPosition) => {
        if (!showCharts || panes.length === 0) return null;
        return (
            <div
                data-docked-chart-group={groupPosition}
                style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, overflow: 'hidden', flexShrink: 0 }}
            >
                {panes.map((pane) => (
                    <React.Fragment key={pane.id}>
                        {groupPosition === 'bottom' ? renderVerticalResizeHandle(pane.id, groupPosition) : null}
                        {renderDockedPane(pane, groupPosition)}
                        {groupPosition === 'top' ? renderVerticalResizeHandle(pane.id, groupPosition) : null}
                    </React.Fragment>
                ))}
            </div>
        );
    };

    return (
        <div
            ref={chartCanvasLayoutRef}
            style={{
                display: 'flex',
                flexDirection: 'column',
                flex: 1,
                minWidth: 0,
                minHeight: 0,
                overflowX: 'hidden',
                overflowY: topPanes.length > 0 || bottomPanes.length > 0 ? 'auto' : 'hidden',
                position: 'relative',
            }}
        >
            {renderModalAt('top')}
            {renderVerticalDockGroup(topPanes, 'top')}
            <div style={{ display: 'flex', flex: 1, minWidth: 0, minHeight: 0, overflow: 'hidden', position: 'relative' }}>
                {renderModalAt('left')}
                {renderHorizontalDockGroup(leftPanes, 'left')}
                <div
                    data-docked-table-canvas
                    ref={chartLayoutRef}
                    style={{
                        display: 'flex',
                        flexDirection: 'column',
                        flexGrow: tableCanvasSize,
                        flexBasis: 0,
                        minWidth: `${minTablePanelWidth}px`,
                        minHeight: 0,
                        overflow: 'hidden',
                        position: 'relative',
                    }}
                >
                    {children}
                </div>
                {tableAdjacentContent}
                {renderHorizontalDockGroup(rightPanes, 'right')}
                {renderModalAt('right')}
            </div>
            {renderVerticalDockGroup(bottomPanes, 'bottom')}
            {renderModalAt('bottom')}
            {showCharts && typeof renderFloatingPane === 'function'
                ? resolvedFloatingPanes.map((pane) => renderFloatingPane(pane))
                : null}
        </div>
    );
}

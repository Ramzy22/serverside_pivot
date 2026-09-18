import { useState } from 'react';

/**
 * useChartState — extracts all chart-related state from DashTanstackPivot.
 *
 * Returns the exact same variable names used in the main component so the
 * migration diff is a pure move with no renames needed.
 */
export function useChartState({
    initialChartDefinition,
    initialChartDefinitions,
    externalChartCanvasPanes,
    externalTableCanvasSize,
    loadPersistedState,
    clampFloatingChartRect,
    normalizeLockedChartRequest,
    sanitizeChartCanvasPanes,
    DEFAULT_TABLE_CANVAS_SIZE,
}) {
    const [chartPanelOpen, setChartPanelOpen] = useState(false);
    const [chartPanelSource, setChartPanelSource] = useState(initialChartDefinition.source);
    const [chartPanelType, setChartPanelType] = useState(initialChartDefinition.chartType);
    const [chartPanelBarLayout, setChartPanelBarLayout] = useState(initialChartDefinition.barLayout);
    const [chartPanelAxisMode, setChartPanelAxisMode] = useState(initialChartDefinition.axisMode);
    const [chartPanelOrientation, setChartPanelOrientation] = useState(initialChartDefinition.orientation);
    const [chartPanelHierarchyLevel, setChartPanelHierarchyLevel] = useState(initialChartDefinition.hierarchyLevel);
    const [chartPanelTitle, setChartPanelTitle] = useState(initialChartDefinition.chartTitle);
    const [chartPanelLayers, setChartPanelLayers] = useState(Array.isArray(initialChartDefinition.chartLayers) ? initialChartDefinition.chartLayers : []);
    const [chartPanelRowLimit, setChartPanelRowLimit] = useState(initialChartDefinition.rowLimit);
    const [chartPanelColumnLimit, setChartPanelColumnLimit] = useState(initialChartDefinition.columnLimit);
    const [chartPanelWidth, setChartPanelWidth] = useState(initialChartDefinition.width);
    const [chartPanelGraphHeight, setChartPanelGraphHeight] = useState(initialChartDefinition.chartHeight);
    const [chartPanelFloating, setChartPanelFloating] = useState(() => Boolean(loadPersistedState('chartPanelFloatingLayout', {}).floating));
    const [chartPanelFloatingRect, setChartPanelFloatingRect] = useState(() => {
        const persistedLayout = loadPersistedState('chartPanelFloatingLayout', {});
        return clampFloatingChartRect(persistedLayout.rect, null);
    });
    const [chartPanelSortMode, setChartPanelSortMode] = useState(initialChartDefinition.sortMode);
    const [chartPanelInteractionMode, setChartPanelInteractionMode] = useState(initialChartDefinition.interactionMode);
    const [chartPanelServerScope, setChartPanelServerScope] = useState(initialChartDefinition.serverScope);
    const [chartPanelLocked, setChartPanelLocked] = useState(() => Boolean(loadPersistedState('chartPanelLockState', {}).locked));
    const [chartPanelLockedModel, setChartPanelLockedModel] = useState(() => loadPersistedState('chartPanelLockState', {}).lockedModel || null);
    const [chartPanelLockedRequest, setChartPanelLockedRequest] = useState(() => normalizeLockedChartRequest(loadPersistedState('chartPanelLockState', {}).lockedRequest));
    const [isChartPanelResizing, setIsChartPanelResizing] = useState(false);
    const [chartModal, setChartModal] = useState(null);
    const [chartModalPosition, setChartModalPosition] = useState('right');
    const [managedChartDefinitions, setManagedChartDefinitions] = useState(initialChartDefinitions);
    const [activeChartDefinitionId, setActiveChartDefinitionId] = useState(() => initialChartDefinitions[0] ? initialChartDefinitions[0].id : 'live-chart-panel');
    const [chartCanvasPanes, setChartCanvasPanes] = useState(() => (
        Array.isArray(externalChartCanvasPanes)
            ? sanitizeChartCanvasPanes(externalChartCanvasPanes, initialChartDefinition)
            : sanitizeChartCanvasPanes(loadPersistedState('chartCanvasPanes', []), initialChartDefinition)
    ));
    const [tableCanvasSize, setTableCanvasSize] = useState(() => {
        const externalSize = Number(externalTableCanvasSize);
        if (Number.isFinite(externalSize) && externalSize > 0) {
            return externalSize;
        }
        const persistedSize = Number(loadPersistedState('tableCanvasSize', DEFAULT_TABLE_CANVAS_SIZE));
        return Number.isFinite(persistedSize) && persistedSize > 0 ? persistedSize : DEFAULT_TABLE_CANVAS_SIZE;
    });
    const [chartPaneDataById, setChartPaneDataById] = useState({});

    return {
        chartPanelOpen, setChartPanelOpen,
        chartPanelSource, setChartPanelSource,
        chartPanelType, setChartPanelType,
        chartPanelBarLayout, setChartPanelBarLayout,
        chartPanelAxisMode, setChartPanelAxisMode,
        chartPanelOrientation, setChartPanelOrientation,
        chartPanelHierarchyLevel, setChartPanelHierarchyLevel,
        chartPanelTitle, setChartPanelTitle,
        chartPanelLayers, setChartPanelLayers,
        chartPanelRowLimit, setChartPanelRowLimit,
        chartPanelColumnLimit, setChartPanelColumnLimit,
        chartPanelWidth, setChartPanelWidth,
        chartPanelGraphHeight, setChartPanelGraphHeight,
        chartPanelFloating, setChartPanelFloating,
        chartPanelFloatingRect, setChartPanelFloatingRect,
        chartPanelSortMode, setChartPanelSortMode,
        chartPanelInteractionMode, setChartPanelInteractionMode,
        chartPanelServerScope, setChartPanelServerScope,
        chartPanelLocked, setChartPanelLocked,
        chartPanelLockedModel, setChartPanelLockedModel,
        chartPanelLockedRequest, setChartPanelLockedRequest,
        isChartPanelResizing, setIsChartPanelResizing,
        chartModal, setChartModal,
        chartModalPosition, setChartModalPosition,
        managedChartDefinitions, setManagedChartDefinitions,
        activeChartDefinitionId, setActiveChartDefinitionId,
        chartCanvasPanes, setChartCanvasPanes,
        tableCanvasSize, setTableCanvasSize,
        chartPaneDataById, setChartPaneDataById,
    };
}

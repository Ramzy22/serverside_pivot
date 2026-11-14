import { useState } from 'react';

/**
 * useChartState — extracts all chart-related state from DashTanstackPivot.
 *
 * Returns the exact same variable names used in the main component so the
 * migration diff is a pure move with no renames needed.
  @param {any} p0 @returns {any} */
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
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelOpen, setChartPanelOpen] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelSource, setChartPanelSource] = useState(initialChartDefinition.source);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelType, setChartPanelType] = useState(initialChartDefinition.chartType);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelBarLayout, setChartPanelBarLayout] = useState(initialChartDefinition.barLayout);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelAxisMode, setChartPanelAxisMode] = useState(initialChartDefinition.axisMode);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelOrientation, setChartPanelOrientation] = useState(initialChartDefinition.orientation);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelHierarchyLevel, setChartPanelHierarchyLevel] = useState(initialChartDefinition.hierarchyLevel);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelTitle, setChartPanelTitle] = useState(initialChartDefinition.chartTitle);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelLayers, setChartPanelLayers] = useState(Array.isArray(initialChartDefinition.chartLayers) ? initialChartDefinition.chartLayers : []);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelRowLimit, setChartPanelRowLimit] = useState(initialChartDefinition.rowLimit);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelColumnLimit, setChartPanelColumnLimit] = useState(initialChartDefinition.columnLimit);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelWidth, setChartPanelWidth] = useState(initialChartDefinition.width);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelGraphHeight, setChartPanelGraphHeight] = useState(initialChartDefinition.chartHeight);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelFloating, setChartPanelFloating] = useState(() => Boolean(loadPersistedState('chartPanelFloatingLayout', {}).floating));
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelFloatingRect, setChartPanelFloatingRect] = useState(() => {
        /** @type {any} */
        const persistedLayout = loadPersistedState('chartPanelFloatingLayout', {});
        return clampFloatingChartRect(persistedLayout.rect, null);
    });
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelSortMode, setChartPanelSortMode] = useState(initialChartDefinition.sortMode);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelInteractionMode, setChartPanelInteractionMode] = useState(initialChartDefinition.interactionMode);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelServerScope, setChartPanelServerScope] = useState(initialChartDefinition.serverScope);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelLocked, setChartPanelLocked] = useState(() => Boolean(loadPersistedState('chartPanelLockState', {}).locked));
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelLockedModel, setChartPanelLockedModel] = useState(() => loadPersistedState('chartPanelLockState', {}).lockedModel || null);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartPanelLockedRequest, setChartPanelLockedRequest] = useState(() => normalizeLockedChartRequest(loadPersistedState('chartPanelLockState', {}).lockedRequest));
    /** @type {[any, (...args: any[]) => any]} */
    const [isChartPanelResizing, setIsChartPanelResizing] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartModal, setChartModal] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [chartModalPosition, setChartModalPosition] = useState('right');
    /** @type {[any, (...args: any[]) => any]} */
    const [managedChartDefinitions, setManagedChartDefinitions] = useState(initialChartDefinitions);
    /** @type {[any, (...args: any[]) => any]} */
    const [activeChartDefinitionId, setActiveChartDefinitionId] = useState(() => initialChartDefinitions[0] ? initialChartDefinitions[0].id : 'live-chart-panel');
    /** @type {[any, (...args: any[]) => any]} */
    const [chartCanvasPanes, setChartCanvasPanes] = useState(() => (
        Array.isArray(externalChartCanvasPanes)
            ? sanitizeChartCanvasPanes(externalChartCanvasPanes, initialChartDefinition)
            : sanitizeChartCanvasPanes(loadPersistedState('chartCanvasPanes', []), initialChartDefinition)
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [tableCanvasSize, setTableCanvasSize] = useState(() => {
        /** @type {any} */
        const externalSize = Number(externalTableCanvasSize);
        if (Number.isFinite(externalSize) && externalSize > 0) {
            return externalSize;
        }
        /** @type {any} */
        const persistedSize = Number(loadPersistedState('tableCanvasSize', DEFAULT_TABLE_CANVAS_SIZE));
        return Number.isFinite(persistedSize) && persistedSize > 0 ? persistedSize : DEFAULT_TABLE_CANVAS_SIZE;
    });
    /** @type {[any, (...args: any[]) => any]} */
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

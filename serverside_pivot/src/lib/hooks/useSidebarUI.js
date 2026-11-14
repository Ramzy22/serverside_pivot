import { useState } from 'react';

/**
 * useSidebarUI — extracts sidebar/field-panel UI state from DashTanstackPivot.
 *
 * Covers: sidebar open/width, field panel sizes, filter column, sidebar tab,
 * floating filters, sticky headers, column search/type filter, selected columns.
  @param {any} p0 @returns {any} */
export function useSidebarUI({
    loadPersistedState,
    clampSidebarWidth,
    externalFieldPanelSizes,
    normalizedInitialFieldPanelSizes,
    sanitizeFieldPanelSizes,
    DEFAULT_FIELD_PANEL_SIZES,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [sidebarOpen, setSidebarOpen] = useState(true);
    /** @type {[any, (...args: any[]) => any]} */
    const [sidebarWidth, setSidebarWidth] = useState(() => clampSidebarWidth(loadPersistedState('sidebarWidth', 288)));
    /** @type {[any, (...args: any[]) => any]} */
    const [fieldPanelSizes, setFieldPanelSizes] = useState(() => (
        externalFieldPanelSizes && typeof externalFieldPanelSizes === 'object'
            ? normalizedInitialFieldPanelSizes
            : sanitizeFieldPanelSizes(loadPersistedState('fieldPanelSizes', DEFAULT_FIELD_PANEL_SIZES))
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [activeFilterCol, setActiveFilterCol] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [filterAnchorEl, setFilterAnchorEl] = useState(null);
    /** @type {[any, (...args: any[]) => any]} */
    const [sidebarTab, setSidebarTab] = useState('fields');
    /** @type {[any, (...args: any[]) => any]} */
    const [showFloatingFilters, setShowFloatingFilters] = useState(false);
    /** @type {[any, (...args: any[]) => any]} */
    const [stickyHeaders, setStickyHeaders] = useState(true);
    /** @type {[any, (...args: any[]) => any]} */
    const [colSearch, setColSearch] = useState('');
    /** @type {[any, (...args: any[]) => any]} */
    const [colTypeFilter, setColTypeFilter] = useState('all');
    /** @type {[any, (...args: any[]) => any]} */
    const [selectedCols, setSelectedCols] = useState(new Set());

    return {
        sidebarOpen, setSidebarOpen,
        sidebarWidth, setSidebarWidth,
        fieldPanelSizes, setFieldPanelSizes,
        activeFilterCol, setActiveFilterCol,
        filterAnchorEl, setFilterAnchorEl,
        sidebarTab, setSidebarTab,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        colSearch, setColSearch,
        colTypeFilter, setColTypeFilter,
        selectedCols, setSelectedCols,
    };
}

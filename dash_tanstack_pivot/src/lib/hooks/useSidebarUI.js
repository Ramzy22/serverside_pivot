import { useState } from 'react';

/**
 * useSidebarUI — extracts sidebar/field-panel UI state from DashTanstackPivot.
 *
 * Covers: sidebar open/width, field panel sizes, filter column, sidebar tab,
 * floating filters, sticky headers, column search/type filter, selected columns.
 */
export function useSidebarUI({
    loadPersistedState,
    clampSidebarWidth,
    externalFieldPanelSizes,
    normalizedInitialFieldPanelSizes,
    sanitizeFieldPanelSizes,
    DEFAULT_FIELD_PANEL_SIZES,
}) {
    const [sidebarOpen, setSidebarOpen] = useState(true);
    const [sidebarWidth, setSidebarWidth] = useState(() => clampSidebarWidth(loadPersistedState('sidebarWidth', 288)));
    const [fieldPanelSizes, setFieldPanelSizes] = useState(() => (
        externalFieldPanelSizes && typeof externalFieldPanelSizes === 'object'
            ? normalizedInitialFieldPanelSizes
            : sanitizeFieldPanelSizes(loadPersistedState('fieldPanelSizes', DEFAULT_FIELD_PANEL_SIZES))
    ));
    const [activeFilterCol, setActiveFilterCol] = useState(null);
    const [filterAnchorEl, setFilterAnchorEl] = useState(null);
    const [sidebarTab, setSidebarTab] = useState('fields');
    const [showFloatingFilters, setShowFloatingFilters] = useState(false);
    const [stickyHeaders, setStickyHeaders] = useState(true);
    const [colSearch, setColSearch] = useState('');
    const [colTypeFilter, setColTypeFilter] = useState('all');
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

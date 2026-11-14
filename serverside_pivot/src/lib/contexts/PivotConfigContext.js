import React, { createContext, useContext, useMemo } from 'react';

const PivotConfigContext = createContext(/** @type {any} */ (null));

/**
 * @param {Record<string, any>} props
 */
export function PivotConfigProvider({
    filters, setFilters,
    pivotMode, setPivotMode,
    reportDef, setReportDef,
    setColFields, setValConfigs,
    savedReports, setSavedReports,
    activeReportId, setActiveReportId,
    customDimensions, setCustomDimensions,
    showFloatingFilters, setShowFloatingFilters,
    stickyHeaders, setStickyHeaders,
    showColTotals, setShowColTotals,
    showRowTotals, setShowRowTotals,
    showSubtotals, setShowSubtotals,
    showRowNumbers, setShowRowNumbers,
    numberGroupSeparator, setNumberGroupSeparator,
    viewMode,
    children,
}) {
    const value = useMemo(() => ({
        filters, setFilters,
        pivotMode, setPivotMode,
        reportDef, setReportDef,
        setColFields, setValConfigs,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        customDimensions, setCustomDimensions,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        showColTotals, setShowColTotals,
        showRowTotals, setShowRowTotals,
        showSubtotals, setShowSubtotals,
        showRowNumbers, setShowRowNumbers,
        numberGroupSeparator, setNumberGroupSeparator,
        viewMode,
    }), [
        filters, setFilters,
        pivotMode, setPivotMode,
        reportDef, setReportDef,
        setColFields, setValConfigs,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        customDimensions, setCustomDimensions,
        showFloatingFilters, setShowFloatingFilters,
        stickyHeaders, setStickyHeaders,
        showColTotals, setShowColTotals,
        showRowTotals, setShowRowTotals,
        showSubtotals, setShowSubtotals,
        showRowNumbers, setShowRowNumbers,
        numberGroupSeparator, setNumberGroupSeparator,
        viewMode,
    ]);
    return (
        <PivotConfigContext.Provider value={value}>
            {children}
        </PivotConfigContext.Provider>
    );
}

export function usePivotConfig() {
    const ctx = useContext(PivotConfigContext);
    if (!ctx) throw new Error('usePivotConfig must be used within PivotConfigProvider');
    return ctx;
}

import { useState } from 'react';

/**
 * useReportMode — extracts report mode state from DashTanstackPivot.
 *
 * Covers: viewMode, reportDef, savedReports, activeReportId, frozenPivotConfig.
  @param {any} p0 @returns {any} */
export function useReportMode({
    hasExternalViewMode,
    normalizedInitialViewMode,
    normalizeViewModeValue,
    normalizeLegacyPivotModeValue,
    hasExternalReportDef,
    normalizedInitialReportDef,
    normalizeReportDefValue,
    hasExternalSavedReports,
    normalizedInitialSavedReports,
    normalizeSavedReportsValue,
    hasExternalActiveReportId,
    normalizedInitialActiveReportId,
    normalizeActiveReportIdValue,
    loadPersistedState,
}) {
    /** @type {[any, (...args: any[]) => any]} */
    const [viewMode, setViewMode] = useState(() => (
        hasExternalViewMode
            ? normalizedInitialViewMode
            : (
                normalizeViewModeValue(
                    loadPersistedState(
                        'viewMode',
                        normalizeLegacyPivotModeValue(loadPersistedState('pivotMode', 'pivot')) || 'pivot'
                    )
                ) || 'pivot'
            )
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [reportDef, setReportDef] = useState(() => (
        hasExternalReportDef
            ? normalizedInitialReportDef
            : normalizeReportDefValue(loadPersistedState('reportDef', { levels: [] }))
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [savedReports, setSavedReports] = useState(() => (
        hasExternalSavedReports
            ? normalizedInitialSavedReports
            : normalizeSavedReportsValue(loadPersistedState('savedReports', []))
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [activeReportId, setActiveReportId] = useState(() => (
        hasExternalActiveReportId
            ? normalizedInitialActiveReportId
            : normalizeActiveReportIdValue(loadPersistedState('activeReportId', null))
    ));
    /** @type {[any, (...args: any[]) => any]} */
    const [frozenPivotConfig, setFrozenPivotConfig] = useState(() => loadPersistedState('frozenPivotConfig', null));

    return {
        viewMode, setViewMode,
        reportDef, setReportDef,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        frozenPivotConfig, setFrozenPivotConfig,
    };
}

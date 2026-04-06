import { useState } from 'react';

/**
 * useReportMode — extracts report mode state from DashTanstackPivot.
 *
 * Covers: viewMode, reportDef, savedReports, activeReportId, frozenPivotConfig.
 */
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
    const [reportDef, setReportDef] = useState(() => (
        hasExternalReportDef
            ? normalizedInitialReportDef
            : normalizeReportDefValue(loadPersistedState('reportDef', { levels: [] }))
    ));
    const [savedReports, setSavedReports] = useState(() => (
        hasExternalSavedReports
            ? normalizedInitialSavedReports
            : normalizeSavedReportsValue(loadPersistedState('savedReports', []))
    ));
    const [activeReportId, setActiveReportId] = useState(() => (
        hasExternalActiveReportId
            ? normalizedInitialActiveReportId
            : normalizeActiveReportIdValue(loadPersistedState('activeReportId', null))
    ));
    const [frozenPivotConfig, setFrozenPivotConfig] = useState(() => loadPersistedState('frozenPivotConfig', null));

    return {
        viewMode, setViewMode,
        reportDef, setReportDef,
        savedReports, setSavedReports,
        activeReportId, setActiveReportId,
        frozenPivotConfig, setFrozenPivotConfig,
    };
}

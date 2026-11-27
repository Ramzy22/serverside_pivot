import React, { useMemo } from 'react';
import Icons from '../../utils/Icons';
import { ToolbarMenu, ToolbarMenuItem } from '../ToolbarMenu';
import { formatValue } from '../../utils/helpers';
import { withAlpha } from '../../utils/styles';

/** @param {any} [value] @param {any} [decimals] @param {any} [numberGroupSeparator] @returns {any} */
const fmt = (value, decimals = 0, numberGroupSeparator) => {
    if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '--';
    return formatValue(Number(value), null, decimals, numberGroupSeparator);
};

/** @param {any} [value] @returns {any} */
const toFiniteNumber = (value) => {
    /** @type {any} */
    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : null;
};

/** @param {any} [theme] @param {any} [tone] @returns {any} */
const getPanelToneColors = (theme, tone = 'default') => {
    if (tone === 'warning') {
        return {
            background: 'rgba(245, 158, 11, 0.12)',
            border: 'rgba(217, 119, 6, 0.28)',
            accent: '#B45309',
        };
    }
    if (tone === 'info') {
        return {
            background: theme.select || 'rgba(59, 130, 246, 0.10)',
            border: withAlpha(theme.primary, 0.24, 'rgba(59, 130, 246, 0.22)'),
            accent: theme.primary || '#2563EB',
        };
    }
    if (tone === 'success') {
        return {
            background: 'rgba(16, 185, 129, 0.10)',
            border: 'rgba(5, 150, 105, 0.24)',
            accent: '#047857',
        };
    }
    return {
        background: theme.surfaceBg || theme.background,
        border: theme.border,
        accent: theme.text,
    };
};

/** @param {any} [selectionKey] @returns {any} */
const parseSelectionKey = (selectionKey) => {
    /** @type {any} */
    const separatorIndex = typeof selectionKey === 'string' ? selectionKey.lastIndexOf(':') : -1;
    if (separatorIndex < 0) return { rowId: selectionKey, colId: null };
    return {
        rowId: selectionKey.slice(0, separatorIndex),
        colId: selectionKey.slice(separatorIndex + 1),
    };
};

/** @param {any} [selectedCells] @returns {any} */
const summarizeSelection = (selectedCells) => {
    /** @type {any} */
    const selectionMap = selectedCells && typeof selectedCells === 'object' ? selectedCells : {};
    /** @type {any} */
    const selectionKeys = Object.keys(selectionMap);
    /** @type {any} */
    const rowIds = new Set();
    /** @type {any} */
    const colIds = new Set();
    /** @type {any[]} */
    const numericValues = [];

    selectionKeys.forEach(/** @param {any} [selectionKey] @returns {any} */ (selectionKey) => {
        /** @type {any} */
        const { rowId, colId } = parseSelectionKey(selectionKey);
        if (rowId) rowIds.add(rowId);
        if (colId) colIds.add(colId);
        /** @type {any} */
        const numericValue = toFiniteNumber(selectionMap[selectionKey]);
        if (numericValue !== null) numericValues.push(numericValue);
    });

    numericValues.sort((left, right) => left - right);
    const numericCount = numericValues.length;
    const totalSelected = selectionKeys.length;
    if (numericCount === 0) {
        return {
            totalSelected,
            distinctRows: rowIds.size,
            distinctCols: colIds.size,
            numericCount: 0,
        };
    }

    /** @type {any} */
    const sum = numericValues.reduce((acc, value) => acc + value, 0);
    /** @type {any} */
    const avg = sum / numericCount;
    /** @type {any} */
    const median = numericCount % 2 === 1
        ? numericValues[(numericCount - 1) / 2]
        : (numericValues[(numericCount / 2) - 1] + numericValues[numericCount / 2]) / 2;
    /** @type {any} */
    const variance = numericValues.reduce((acc, value) => acc + ((value - avg) ** 2), 0) / numericCount;

    return {
        totalSelected,
        distinctRows: rowIds.size,
        distinctCols: colIds.size,
        numericCount,
        sum,
        avg,
        median,
        stdDev: Math.sqrt(variance),
    };
};

/** @param {any} p0 @returns {any} */
const StatusMetric = ({ label, value, accent = null }) => (
    <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: '5px', whiteSpace: 'nowrap' }}>
        <span style={{ fontSize: '11px', fontWeight: 700, letterSpacing: '0.04em', textTransform: 'uppercase', opacity: 0.72 }}>
            {label}
        </span>
        <span style={{ fontSize: '13px', fontWeight: 700, color: accent || 'inherit' }}>
            {value}
        </span>
    </span>
);

/** @param {any} p0 @returns {any} */
const StatusPanel = ({ id, theme, icon, label, tone = 'default', children, title = null }) => {
    /** @type {any} */
    const colors = getPanelToneColors(theme, tone);
    return (
        <div
            data-pivot-status-panel={id}
            title={title || undefined}
            style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '8px',
                padding: '5px 10px',
                borderRadius: '7px',
                border: `1px solid ${colors.border}`,
                background: colors.background,
                color: theme.text,
                whiteSpace: 'nowrap',
                fontSize: '11px',
            }}
        >
            <span style={{ color: colors.accent, display: 'inline-flex', alignItems: 'center', flexShrink: 0 }}>{icon}</span>
            <span style={{ fontWeight: 700, fontSize: '11px', color: theme.text }}>{label}</span>
            {children}
        </div>
    );
};

/** @param {any} p0 @returns {any} */
const StatusActionButton = ({ id, label, onClick, disabled = false, theme, icon = null, tone = 'default', iconOnly = false, ariaExpanded = undefined }) => {
    /** @type {any} */
    const colors = getPanelToneColors(theme, tone);
    return (
        <button
            type="button"
            data-pivot-status-action={id}
            onClick={onClick}
            disabled={disabled}
            aria-label={iconOnly ? label : undefined}
            aria-expanded={ariaExpanded}
            title={iconOnly ? label : undefined}
            style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: '5px',
                minHeight: '30px',
                borderRadius: '7px',
                border: `1px solid ${disabled ? theme.border : colors.border}`,
                background: disabled ? (theme.headerSubtleBg || theme.hover) : colors.background,
                color: disabled ? theme.textSec : (tone === 'default' ? theme.text : colors.accent),
                padding: '0 10px',
                fontSize: '11px',
                fontWeight: 700,
                cursor: disabled ? 'not-allowed' : 'pointer',
                opacity: disabled ? (theme.isDark ? 0.52 : 0.72) : 1,
                whiteSpace: 'nowrap',
            }}
        >
            {icon ? <span style={{ display: 'inline-flex', alignItems: 'center' }}>{icon}</span> : null}
            {iconOnly ? null : <span>{label}</span>}
        </button>
    );
};

/** @param {any} p0 @returns {any} */
const StatusBar = ({
    statusModel,
    statusActions,
    theme,
    numberGroupSeparator,
    compact = false,
}) => {
    /** @type {[any, (...args: any[]) => any]} */
    const [showDetails, setShowDetails] = React.useState(false);
    /** @type {any} */
    const selectedCells = statusModel && statusModel.selection ? statusModel.selection.selectedCells : {};
    /** @type {any} */
    const selectionSummary = useMemo(
        () => summarizeSelection(selectedCells),
        [selectedCells]
    );

    /** @type {(value: any, decimals?: number) => string} */
    const fmtStat = statusModel && statusModel.selection && typeof statusModel.selection.formatValue === 'function'
        ? statusModel.selection.formatValue
        : (value, decimals = 2) => fmt(value, decimals, numberGroupSeparator);
    /** @type {any} */
    const dataSummary = statusModel && statusModel.data ? statusModel.data : {};
    /** @type {any} */
    const runtimeSummary = statusModel && statusModel.runtime ? statusModel.runtime : {};
    /** @type {any} */
    const editSummary = statusModel && statusModel.editing ? statusModel.editing : {};
    /** @type {any} */
    const chartSummary = statusModel && statusModel.charts ? statusModel.charts : {};
    /** @type {any} */
    const actionSummary = statusActions && typeof statusActions === 'object' ? statusActions : {};

    /** @type {(...args: any[]) => any} */
    const handleToggleDetails = React.useCallback(() => {
        setShowDetails(/** @param {any} [previousValue] @returns {any} */ (previousValue) => !previousValue);
    }, []);

    /** @type {any} */
    const activeSearchText = dataSummary.globalSearch ? `Search: ${dataSummary.globalSearch}` : null;
    /** @type {any} */
    const cacheCoverage = (
        Number.isFinite(Number(dataSummary.rowCount))
        && Number(dataSummary.rowCount) > 0
        && Number.isFinite(Number(runtimeSummary.loadedRowCount))
    )
        ? Math.max(0, Math.min(100, Math.round((Number(runtimeSummary.loadedRowCount) / Number(dataSummary.rowCount)) * 100)))
        : null;

    return (
        <div
            data-pivot-status-bar="true"
            data-pivot-status-details-open={showDetails ? 'true' : 'false'}
            style={{
                position: 'relative',
                zIndex: 110,
                borderTop: `1px solid ${theme.border}`,
                background: theme.headerSubtleBg || theme.headerBg,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'stretch',
                gap: '4px',
                padding: '4px 10px',
                fontSize: '11px',
                color: theme.textSec,
                boxShadow: theme.shadowInset || 'none',
            }}
        >
            {showDetails ? (
                <div
                    data-pivot-status-details="true"
                    style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', minWidth: 0, alignItems: 'center' }}
                >
                    <StatusPanel id="scope" theme={theme} icon={<Icons.Database />} label={runtimeSummary.serverSide ? 'Server' : 'Local'} tone={runtimeSummary.loading ? 'info' : 'default'}>
                        <StatusMetric label="Rows" value={fmt(dataSummary.rowCount, 0, numberGroupSeparator)} />
                        <StatusMetric label="Visible" value={fmt(dataSummary.visibleRowsCount, 0, numberGroupSeparator)} />
                        <StatusMetric label="Cols" value={fmt(dataSummary.totalCenterColumns, 0, numberGroupSeparator)} />
                        {runtimeSummary.loading ? <StatusMetric label="" value="Loading" accent={theme.primary || '#2563EB'} /> : null}
                    </StatusPanel>
                    <StatusPanel id="layout" theme={theme} icon={<Icons.Columns />} label="Layout">
                        <StatusMetric label="Rows" value={fmt(dataSummary.rowFieldCount, 0, numberGroupSeparator)} />
                        <StatusMetric label="Cols" value={fmt(dataSummary.columnFieldCount, 0, numberGroupSeparator)} />
                        <StatusMetric label="Values" value={fmt(dataSummary.measureCount, 0, numberGroupSeparator)} />
                        <StatusMetric label="Sorts" value={fmt(dataSummary.sortingCount, 0, numberGroupSeparator)} />
                    </StatusPanel>
                    <StatusPanel id="pipeline" theme={theme} icon={<Icons.Filter />} label="Filters" tone={dataSummary.activeFilterCount > 0 ? 'info' : 'default'}>
                        <StatusMetric label="" value={fmt(dataSummary.activeFilterCount, 0, numberGroupSeparator)} />
                        {activeSearchText ? <StatusMetric label="Search" value="On" accent={theme.primary || '#2563EB'} /> : null}
                    </StatusPanel>
                    {selectionSummary.totalSelected > 0 ? (
                        <StatusPanel id="selection" theme={theme} icon={<Icons.Sigma />} label="Selection" tone="info">
                            <StatusMetric label="Cells" value={fmt(selectionSummary.totalSelected, 0, numberGroupSeparator)} />
                            {selectionSummary.numericCount > 0 ? (
                                <>
                                    <StatusMetric label="Sum" value={fmtStat(selectionSummary.sum, 2)} />
                                    <StatusMetric label="Avg" value={fmtStat(selectionSummary.avg, 2)} />
                                    <StatusMetric label="Median" value={fmtStat(selectionSummary.median, 2)} />
                                </>
                            ) : null}
                        </StatusPanel>
                    ) : null}
                    {(editSummary.comparedValueCount > 0 || editSummary.activeRowEditCount > 0) ? (
                        <StatusPanel id="editing" theme={theme} icon={<Icons.Edit />} label="Editing" tone="success">
                            <StatusMetric label="Diffs" value={fmt(editSummary.comparedValueCount, 0, numberGroupSeparator)} />
                            <StatusMetric label="Dirty" value={fmt(editSummary.dirtyRowCount, 0, numberGroupSeparator)} />
                            <StatusMetric label="Undo" value={fmt(editSummary.undoCount, 0, numberGroupSeparator)} />
                            <StatusMetric label="Redo" value={fmt(editSummary.redoCount, 0, numberGroupSeparator)} />
                        </StatusPanel>
                    ) : null}
                    {chartSummary.paneCount > 0 ? (
                        <StatusPanel id="charts" theme={theme} icon={<Icons.Chart />} label="Charts" tone="info">
                            <StatusMetric label="Panes" value={fmt(chartSummary.paneCount, 0, numberGroupSeparator)} />
                        </StatusPanel>
                    ) : null}
                    {dataSummary.columnAdvisory ? (
                        <StatusPanel id="columnAdvisory" theme={theme} icon={<Icons.Filter />} label={dataSummary.columnAdvisory.label || 'Advisory'} tone="warning" title={dataSummary.columnAdvisory.notification}>
                            <StatusMetric label="Cols" value={fmt(dataSummary.totalCenterColumns, 0, numberGroupSeparator)} />
                        </StatusPanel>
                    ) : null}
                </div>
            ) : null}

            <div
                data-pivot-status-actions="true"
                style={{
                    display: 'flex',
                    flexWrap: compact ? 'nowrap' : 'wrap',
                    gap: '8px 12px',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    minWidth: 0,
                }}
            >
                <div
                    data-pivot-status-summary="true"
                    style={{
                        display: 'flex',
                        flexWrap: compact ? 'nowrap' : 'wrap',
                        alignItems: 'center',
                        gap: '12px',
                        minWidth: 0,
                        overflow: compact ? 'hidden' : 'visible',
                    }}
                >
                    <StatusActionButton
                        id="toggle-details"
                        label="Details"
                        icon={showDetails ? <Icons.VisibilityOff /> : <Icons.Visibility />}
                        onClick={handleToggleDetails}
                        theme={theme}
                        tone={showDetails ? 'info' : 'default'}
                        iconOnly
                        ariaExpanded={showDetails}
                    />
                    {runtimeSummary.loading ? (
                        <span
                            data-pivot-loading-indicator="status"
                            style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '6px',
                                padding: '2px 8px',
                                borderRadius: '999px',
                                background: theme.select || 'rgba(59, 130, 246, 0.12)',
                                color: theme.primary || '#2563EB',
                                fontWeight: 700,
                            }}
                        >
                            Loading
                        </span>
                    ) : null}
                    <StatusMetric label="Rows" value={fmt(dataSummary.rowCount, 0, numberGroupSeparator)} />
                    {compact ? null : <StatusMetric label="Visible" value={fmt(dataSummary.visibleRowsCount, 0, numberGroupSeparator)} />}
                    {compact ? null : <StatusMetric label="Cols" value={fmt(dataSummary.totalCenterColumns, 0, numberGroupSeparator)} />}
                    <StatusMetric label="Filters" value={fmt(dataSummary.activeFilterCount, 0, numberGroupSeparator)} />
                    {selectionSummary.totalSelected > 0 ? (
                        <StatusMetric label="Cells" value={fmt(selectionSummary.totalSelected, 0, numberGroupSeparator)} />
                    ) : null}
                    {!compact && selectionSummary.numericCount > 0 ? (
                        <>
                            <StatusMetric label="Sum" value={fmtStat(selectionSummary.sum, 2)} />
                            <StatusMetric label="Mean" value={fmtStat(selectionSummary.avg, 2)} />
                            <StatusMetric label="Std Dev" value={fmtStat(selectionSummary.stdDev, 2)} />
                        </>
                    ) : null}
                </div>

                {compact ? <ToolbarMenu label="⋯" ariaLabel="Status actions" theme={theme} align="right" chevron={false} placement="above" buttonStyle={{ padding: '2px 8px', color: theme.text }}>
                    {close => <>
                        <ToolbarMenuItem theme={theme} disabled={!actionSummary.canClearSelection} onSelect={() => { actionSummary.onClearSelection?.(); close(); }}>Clear Selection</ToolbarMenuItem>
                        <ToolbarMenuItem theme={theme} disabled={!actionSummary.canClearFilters} onSelect={() => { actionSummary.onClearFilters?.(); close(); }}>Clear Filters</ToolbarMenuItem>
                        <ToolbarMenuItem theme={theme} disabled={!actionSummary.canRefreshViewport} onSelect={() => { actionSummary.onRefreshViewport?.(); close(); }}>Refresh View</ToolbarMenuItem>
                    </>}
                </ToolbarMenu> : (
                <div style={{ display: 'flex', flex: '0 0 auto', flexWrap: 'nowrap', gap: compact ? '4px' : '8px', alignItems: 'center', justifyContent: 'flex-end' }}>
                    <StatusActionButton
                        id="clear-selection"
                        label="Clear Selection"
                        icon={<Icons.Close />}
                        onClick={actionSummary.onClearSelection}
                        disabled={!actionSummary.canClearSelection}
                        theme={theme}
                        iconOnly={compact}
                    />
                    <StatusActionButton
                        id="clear-filters"
                        label="Clear Filters"
                        icon={<Icons.Filter />}
                        onClick={actionSummary.onClearFilters}
                        disabled={!actionSummary.canClearFilters}
                        theme={theme}
                        iconOnly={compact}
                    />
                    <StatusActionButton
                        id="refresh"
                        label="Refresh View"
                        icon={<Icons.Database />}
                        onClick={actionSummary.onRefreshViewport}
                        disabled={!actionSummary.canRefreshViewport}
                        theme={theme}
                        iconOnly={compact}
                    />
                </div>
                )}
            </div>
        </div>
    );
};

export default StatusBar;

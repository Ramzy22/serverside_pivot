import React, { useMemo } from 'react';
import Icons from '../Icons';
import { formatValue } from '../../utils/helpers';

const fmt = (value, decimals = 0, numberGroupSeparator) => {
    if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '--';
    return formatValue(Number(value), null, decimals, numberGroupSeparator);
};

const toFiniteNumber = (value) => {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : null;
};

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
            border: theme.primary ? `${theme.primary}33` : 'rgba(59, 130, 246, 0.22)',
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

const parseSelectionKey = (selectionKey) => {
    const separatorIndex = typeof selectionKey === 'string' ? selectionKey.lastIndexOf(':') : -1;
    if (separatorIndex < 0) return { rowId: selectionKey, colId: null };
    return {
        rowId: selectionKey.slice(0, separatorIndex),
        colId: selectionKey.slice(separatorIndex + 1),
    };
};

const summarizeSelection = (selectedCells) => {
    const selectionMap = selectedCells && typeof selectedCells === 'object' ? selectedCells : {};
    const selectionKeys = Object.keys(selectionMap);
    const rowIds = new Set();
    const colIds = new Set();
    const numericValues = [];

    selectionKeys.forEach((selectionKey) => {
        const { rowId, colId } = parseSelectionKey(selectionKey);
        if (rowId) rowIds.add(rowId);
        if (colId) colIds.add(colId);
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

    const sum = numericValues.reduce((acc, value) => acc + value, 0);
    const avg = sum / numericCount;
    const median = numericCount % 2 === 1
        ? numericValues[(numericCount - 1) / 2]
        : (numericValues[(numericCount / 2) - 1] + numericValues[numericCount / 2]) / 2;
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

const StatusPanel = ({ id, theme, icon, label, tone = 'default', children, title = null }) => {
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

const StatusActionButton = ({ id, label, onClick, disabled = false, theme, icon = null, tone = 'default' }) => {
    const colors = getPanelToneColors(theme, tone);
    return (
        <button
            type="button"
            data-pivot-status-action={id}
            onClick={onClick}
            disabled={disabled}
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
                opacity: disabled ? 0.5 : 1,
                whiteSpace: 'nowrap',
            }}
        >
            {icon ? <span style={{ display: 'inline-flex', alignItems: 'center' }}>{icon}</span> : null}
            <span>{label}</span>
        </button>
    );
};

const StatusBar = ({
    statusModel,
    statusActions,
    theme,
    numberGroupSeparator,
}) => {
    const [showDetails, setShowDetails] = React.useState(false);
    const selectedCells = statusModel && statusModel.selection ? statusModel.selection.selectedCells : {};
    const selectionSummary = useMemo(
        () => summarizeSelection(selectedCells),
        [selectedCells]
    );

    const dataSummary = statusModel && statusModel.data ? statusModel.data : {};
    const runtimeSummary = statusModel && statusModel.runtime ? statusModel.runtime : {};
    const editSummary = statusModel && statusModel.editing ? statusModel.editing : {};
    const chartSummary = statusModel && statusModel.charts ? statusModel.charts : {};
    const actionSummary = statusActions && typeof statusActions === 'object' ? statusActions : {};

    const handleToggleDetails = React.useCallback(() => {
        setShowDetails((previousValue) => !previousValue);
    }, []);

    const activeSearchText = dataSummary.globalSearch ? `Search: ${dataSummary.globalSearch}` : null;
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
                borderTop: `1px solid ${theme.border}`,
                background: theme.headerBg,
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
                                    <StatusMetric label="Sum" value={fmt(selectionSummary.sum, 2, numberGroupSeparator)} />
                                    <StatusMetric label="Avg" value={fmt(selectionSummary.avg, 2, numberGroupSeparator)} />
                                    <StatusMetric label="Median" value={fmt(selectionSummary.median, 2, numberGroupSeparator)} />
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

            {!showDetails ? (
                <div
                    data-pivot-status-summary="true"
                    style={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        alignItems: 'center',
                        gap: '12px',
                        minWidth: 0,
                        padding: '2px 4px',
                    }}
                >
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
                    <StatusMetric label="Visible" value={fmt(dataSummary.visibleRowsCount, 0, numberGroupSeparator)} />
                    <StatusMetric label="Cols" value={fmt(dataSummary.totalCenterColumns, 0, numberGroupSeparator)} />
                    <StatusMetric label="Filters" value={fmt(dataSummary.activeFilterCount, 0, numberGroupSeparator)} />
                    <StatusMetric label="Cells" value={fmt(selectionSummary.totalSelected, 0, numberGroupSeparator)} />
                    <StatusMetric label="Mean" value={selectionSummary.numericCount > 0 ? fmt(selectionSummary.avg, 2, numberGroupSeparator) : '--'} />
                    <StatusMetric label="Std Dev" value={selectionSummary.numericCount > 0 ? fmt(selectionSummary.stdDev, 2, numberGroupSeparator) : '--'} />
                </div>
            ) : null}

            <div
                data-pivot-status-actions="true"
                style={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: '8px',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                }}
            >
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                    <StatusActionButton
                        id="toggle-details"
                        label={showDetails ? 'Hide Status' : 'Show Status'}
                        icon={showDetails ? <Icons.VisibilityOff /> : <Icons.Visibility />}
                        onClick={handleToggleDetails}
                        theme={theme}
                        tone={showDetails ? 'info' : 'default'}
                    />
                    {!showDetails ? (
                        <span style={{ fontSize: '11px', color: theme.textSec, whiteSpace: 'nowrap', lineHeight: '1.4' }}>
                            Detailed panels are off by default.
                        </span>
                    ) : null}
                </div>

                <div style={{ display: 'flex', flex: '0 1 auto', flexWrap: 'wrap', gap: '8px', alignItems: 'flex-start', justifyContent: 'flex-end' }}>
                    <StatusActionButton
                        id="clear-selection"
                        label="Clear Selection"
                        icon={<Icons.Close />}
                        onClick={actionSummary.onClearSelection}
                        disabled={!actionSummary.canClearSelection}
                        theme={theme}
                    />
                    <StatusActionButton
                        id="clear-filters"
                        label="Clear Filters"
                        icon={<Icons.Filter />}
                        onClick={actionSummary.onClearFilters}
                        disabled={!actionSummary.canClearFilters}
                        theme={theme}
                    />
                    <StatusActionButton
                        id="refresh"
                        label="Refresh View"
                        icon={<Icons.Database />}
                        onClick={actionSummary.onRefreshViewport}
                        disabled={!actionSummary.canRefreshViewport}
                        theme={theme}
                    />
                    <StatusActionButton
                        id="range-chart"
                        label="Range Chart"
                        icon={<Icons.Chart />}
                        onClick={actionSummary.onCreateSelectionChart}
                        disabled={!actionSummary.canCreateSelectionChart}
                        theme={theme}
                        tone="info"
                    />
                    <StatusActionButton
                        id="undo"
                        label="Undo"
                        onClick={actionSummary.onUndo}
                        disabled={!actionSummary.canUndo}
                        theme={theme}
                    />
                    <StatusActionButton
                        id="redo"
                        label="Redo"
                        onClick={actionSummary.onRedo}
                        disabled={!actionSummary.canRedo}
                        theme={theme}
                    />
                </div>
            </div>
        </div>
    );
};

export default StatusBar;

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import Icons from '../../utils/Icons';
import { formatAggLabel, formatDisplayLabel } from '../../utils/helpers';

const REPORT_PATH_SEPARATOR = '|||';
const REPORT_META_FIELD_SET = new Set([
    'depth',
    '_id',
    '_path',
    '_levelLabel',
    '_levelField',
    '_pathFields',
    '_has_children',
    '_is_expanded',
    '__col_schema',
]);

const createNodeId = () => `report-node-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;

const createNode = (field = '') => ({
    _id: createNodeId(),
    field,
    label: '',
    topN: null,
    sortBy: null,
    sortDir: 'desc',
    defaultChild: null,
    childrenByValue: {},
});

const normalizeNode = (value) => {
    const source = value && typeof value === 'object' ? value : {};
    const children = source.childrenByValue && typeof source.childrenByValue === 'object'
        ? source.childrenByValue
        : {};
    return {
        ...source,
        _id: typeof source._id === 'string' ? source._id : createNodeId(),
        field: typeof source.field === 'string' ? source.field : '',
        label: typeof source.label === 'string' ? source.label : '',
        topN: Number.isFinite(Number(source.topN)) && Number(source.topN) > 0 ? Math.floor(Number(source.topN)) : null,
        sortBy: typeof source.sortBy === 'string' && source.sortBy.trim() ? source.sortBy.trim() : null,
        sortDir: source.sortDir === 'asc' ? 'asc' : 'desc',
        defaultChild: source.defaultChild ? normalizeNode(source.defaultChild) : null,
        childrenByValue: Object.entries(children).reduce((acc, [key, child]) => {
            if (!key) return acc;
            acc[String(key)] = normalizeNode(child);
            return acc;
        }, {}),
    };
};

const cloneNode = (node) => (node ? normalizeNode(JSON.parse(JSON.stringify(node))) : null);

const countNodes = (node) => {
    if (!node) return 0;
    let total = 1;
    if (node.defaultChild) total += countNodes(node.defaultChild);
    Object.values(node.childrenByValue || {}).forEach((child) => { total += countNodes(child); });
    return total;
};

const parsePathValues = (row) => {
    if (!row || typeof row !== 'object') return [];
    if (typeof row._path !== 'string' || !row._path || row._path === '__grand_total__') return [];
    return row._path.split(REPORT_PATH_SEPARATOR).map((value) => String(value));
};

const pathMatches = (rowPathValues, ancestorValues) => {
    if (rowPathValues.length < ancestorValues.length) return false;
    for (let index = 0; index < ancestorValues.length; index += 1) {
        if (String(rowPathValues[index]) !== String(ancestorValues[index])) return false;
    }
    return true;
};

const getRowBranchValue = (row, field) => {
    if (row && row[field] !== undefined && row[field] !== null && row[field] !== '') {
        return String(row[field]);
    }
    if (row && row._id !== undefined && row._id !== null && row._id !== '') {
        return String(row._id);
    }
    const pathValues = parsePathValues(row);
    return pathValues.length > 0 ? String(pathValues[pathValues.length - 1]) : '';
};

const collectVisibleBranchValues = (rows, ancestorValues, field) => {
    if (!field) return [];
    const seen = new Set();
    const output = [];
    (Array.isArray(rows) ? rows : []).forEach((row) => {
        if (!row || typeof row !== 'object' || row._isTotal) return;
        if (String(row._levelField || '') !== String(field)) return;
        const depth = Number.isFinite(Number(row.depth)) ? Number(row.depth) : 0;
        if (depth !== ancestorValues.length) return;
        const pathValues = parsePathValues(row);
        if (!pathMatches(pathValues, ancestorValues)) return;
        const value = getRowBranchValue(row, field);
        if (!value || seen.has(value)) return;
        seen.add(value);
        output.push({
            value,
            isExpanded: Boolean(row._is_expanded),
            hasChildren: Boolean(row._has_children),
        });
    });
    return output;
};

const getNodeTitle = (node) => (
    node.label && node.label.trim()
        ? node.label.trim()
        : (node.field ? formatDisplayLabel(node.field) : 'Choose a field')
);

const makeInputStyle = (theme) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: '9px',
    padding: '8px 10px',
    fontSize: '12px',
    background: theme.background || '#fff',
    color: theme.text,
    outline: 'none',
    width: '100%',
});

const getReportSelectableFields = (availableFields, valConfigs) => {
    const aggregatedValueIds = new Set(
        (Array.isArray(valConfigs) ? valConfigs : [])
            .filter((config) => config && config.field && config.agg)
            .map((config) => (config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`))
    );
    return (Array.isArray(availableFields) ? availableFields : []).filter((field) => {
        if (!field || typeof field !== 'string') return false;
        if (REPORT_META_FIELD_SET.has(field)) return false;
        if (field.startsWith('_')) return false;
        if (aggregatedValueIds.has(field)) return false;
        return true;
    });
};

function FieldSelectorPanel({ theme, label, hint, availableFields, value = '', onChange, compact = false }) {
    const inputStyle = makeInputStyle(theme);
    const options = useMemo(() => {
        const base = Array.isArray(availableFields) ? availableFields : [];
        if (value && !base.includes(value)) return [value, ...base];
        return base;
    }, [availableFields, value]);
    return (
        <div
            data-report-field-selector={label}
            style={{
                border: `1px solid ${theme.border}`,
                borderRadius: '12px',
                background: theme.background || '#fff',
                padding: compact ? '10px 12px' : '14px',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px',
            }}
        >
            <div style={{ fontSize: compact ? '12px' : '13px', fontWeight: 700, color: theme.text }}>
                {label}
            </div>
            {hint ? (
                <div style={{ fontSize: '11px', color: theme.textSec, lineHeight: 1.45 }}>
                    {hint}
                </div>
            ) : null}
            <select
                value={value || ''}
                onChange={(event) => {
                    if (!event.target.value) return;
                    onChange(event.target.value);
                }}
                style={{ ...inputStyle, cursor: 'pointer' }}
            >
                <option value="">Choose field...</option>
                {options.map((field) => (
                    <option key={field} value={field}>
                        {formatDisplayLabel(field)}
                    </option>
                ))}
            </select>
        </div>
    );
}

function ReportNodeCard({
    node,
    title,
    branchValue = null,
    availableFields,
    sortByOptions,
    theme,
    reportRows,
    ancestorValues,
    templateMode = false,
    onChange,
    onRemove,
}) {
    const [selectedBranchValue, setSelectedBranchValue] = useState('');
    const current = useMemo(() => normalizeNode(node), [node]);
    const inputStyle = useMemo(() => makeInputStyle(theme), [theme]);

    const visibleBranches = useMemo(
        () => (templateMode ? [] : collectVisibleBranchValues(reportRows, ancestorValues, current.field)),
        [ancestorValues, current.field, reportRows, templateMode]
    );

    const configuredBranchKeys = useMemo(
        () => Object.keys(current.childrenByValue || {}),
        [current.childrenByValue]
    );

    const displayedBranchKeys = useMemo(() => {
        const ordered = [];
        const seen = new Set();
        visibleBranches.forEach(({ value }) => {
            if (seen.has(value)) return;
            seen.add(value);
            ordered.push(value);
        });
        configuredBranchKeys.forEach((value) => {
            if (seen.has(value)) return;
            seen.add(value);
            ordered.push(value);
        });
        return ordered;
    }, [configuredBranchKeys, visibleBranches]);

    useEffect(() => {
        if (displayedBranchKeys.length === 0) {
            if (selectedBranchValue) setSelectedBranchValue('');
            return;
        }
        if (!displayedBranchKeys.includes(selectedBranchValue)) {
            setSelectedBranchValue(displayedBranchKeys[0]);
        }
    }, [displayedBranchKeys, selectedBranchValue]);

    const updateNode = useCallback((updates) => {
        onChange({ ...current, ...updates });
    }, [current, onChange]);

    const setDefaultChild = useCallback((nextChild) => {
        updateNode({ defaultChild: nextChild ? normalizeNode(nextChild) : null });
    }, [updateNode]);

    const setBranchChild = useCallback((value, nextChild) => {
        const nextChildren = { ...(current.childrenByValue || {}) };
        if (nextChild) nextChildren[value] = normalizeNode(nextChild);
        else delete nextChildren[value];
        updateNode({ childrenByValue: nextChildren });
    }, [current.childrenByValue, updateNode]);

    const handleDefaultFieldDrop = useCallback((field) => {
        const nextChild = current.defaultChild
            ? { ...cloneNode(current.defaultChild), field }
            : createNode(field);
        setDefaultChild(nextChild);
    }, [current.defaultChild, setDefaultChild]);

    const materializeBranchChild = useCallback((value, updates = {}) => {
        if (!value) return;
        const existing = current.childrenByValue && current.childrenByValue[value];
        const baseNode = cloneNode(existing || current.defaultChild || createNode('')) || createNode('');
        setBranchChild(value, { ...baseNode, ...updates });
    }, [current.childrenByValue, current.defaultChild, setBranchChild]);

    const selectedExplicitChild = selectedBranchValue
        ? (current.childrenByValue && current.childrenByValue[selectedBranchValue]) || null
        : null;
    const selectedInheritedChild = selectedBranchValue && !selectedExplicitChild && current.defaultChild ? current.defaultChild : null;
    const selectedBranchChild = selectedExplicitChild || selectedInheritedChild || null;
    const selectedBranchAncestorValues = templateMode || !selectedBranchValue
        ? ancestorValues
        : [...ancestorValues, selectedBranchValue];
    const selectedBranchNode = useMemo(
        () => cloneNode(selectedBranchChild) || createNode(''),
        [selectedBranchChild]
    );

    const cardStyle = {
        border: `1px solid ${theme.border}`,
        borderRadius: '14px',
        background: theme.surfaceBg || theme.background || '#fff',
        padding: '14px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
    };
    const sectionLabelStyle = {
        fontSize: '10px',
        fontWeight: 800,
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: theme.textSec,
        marginBottom: '6px',
    };
    const inputLabelStyle = {
        fontSize: '10px',
        fontWeight: 800,
        letterSpacing: '0.04em',
        textTransform: 'uppercase',
        color: theme.textSec,
        marginBottom: '4px',
    };
    const sectionCardStyle = {
        border: `1px solid ${theme.border}`,
        borderRadius: '12px',
        background: theme.background || '#fff',
        padding: '12px',
        display: 'flex',
        flexDirection: 'column',
        gap: '10px',
    };
    const secondaryButtonStyle = {
        background: theme.hover,
        border: `1px solid ${theme.border}`,
        borderRadius: '9px',
        padding: '7px 10px',
        fontSize: '11px',
        fontWeight: 700,
        color: theme.text,
        cursor: 'pointer',
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
    };

    return (
        <div
            data-report-node={current._id}
            data-report-title={title}
            data-report-branch={branchValue === null ? '' : String(branchValue)}
            style={cardStyle}
        >
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '10px' }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec }}>
                        {title}
                    </div>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text, marginTop: '4px' }}>
                        {getNodeTitle(current)}
                    </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                    {branchValue !== null ? (
                        <span style={{ fontSize: '11px', fontWeight: 700, color: theme.primary || '#2563EB', background: `${theme.primary || '#2563EB'}14`, borderRadius: '999px', padding: '4px 9px' }}>
                            {branchValue}
                        </span>
                    ) : null}
                    {onRemove ? (
                        <button type="button" onClick={onRemove} style={{ ...secondaryButtonStyle, color: '#DC2626' }}>
                            <Icons.Delete />
                        </button>
                    ) : null}
                </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 0.7fr 1fr 1fr', gap: '8px' }}>
                <div>
                    <div style={inputLabelStyle}>Field</div>
                    <select
                        value={current.field}
                        onChange={(event) => updateNode({ field: event.target.value })}
                        style={{ ...inputStyle, cursor: 'pointer' }}
                    >
                        <option value="">Select field...</option>
                        {availableFields.map((field) => (
                            <option key={field} value={field}>
                                {formatDisplayLabel(field)}
                            </option>
                        ))}
                    </select>
                </div>
                <div>
                    <div style={inputLabelStyle}>Top N</div>
                    <input
                        type="number"
                        min="0"
                        value={current.topN || ''}
                        onChange={(event) => {
                            const nextValue = event.target.value === '' ? null : parseInt(event.target.value, 10);
                            updateNode({ topN: nextValue && nextValue > 0 ? nextValue : null });
                        }}
                        placeholder="All"
                        style={inputStyle}
                    />
                </div>
                <div>
                    <div style={inputLabelStyle}>Sort By</div>
                    <select
                        value={current.sortBy || ''}
                        onChange={(event) => updateNode({ sortBy: event.target.value || null })}
                        style={{ ...inputStyle, cursor: 'pointer' }}
                    >
                        {sortByOptions.map((option) => (
                            <option key={option.value} value={option.value}>
                                {option.label}
                            </option>
                        ))}
                    </select>
                </div>
                <div>
                    <div style={inputLabelStyle}>Order</div>
                    <select
                        value={current.sortDir || 'desc'}
                        onChange={(event) => updateNode({ sortDir: event.target.value })}
                        style={{ ...inputStyle, cursor: 'pointer' }}
                    >
                        <option value="desc">Desc</option>
                        <option value="asc">Asc</option>
                    </select>
                </div>
            </div>

            {current.field ? (
                <>
                    <div style={sectionCardStyle}>
                        <div style={sectionLabelStyle}>Default</div>
                        {!current.defaultChild ? (
                            <FieldSelectorPanel
                                theme={theme}
                                compact
                                label="Below"
                                hint=""
                                availableFields={availableFields}
                                onChange={handleDefaultFieldDrop}
                            />
                        ) : (
                            <div style={{ paddingLeft: '14px', borderLeft: `2px solid ${theme.border}` }}>
                                <ReportNodeCard
                                    node={current.defaultChild}
                                    title="Next"
                                    availableFields={availableFields}
                                    sortByOptions={sortByOptions}
                                    theme={theme}
                                    reportRows={reportRows}
                                    ancestorValues={ancestorValues}
                                    templateMode
                                    onChange={setDefaultChild}
                                    onRemove={() => setDefaultChild(null)}
                                />
                            </div>
                        )}
                    </div>

                    {!templateMode ? (
                        <div style={sectionCardStyle}>
                            <div style={sectionLabelStyle}>Line</div>
                            {displayedBranchKeys.length === 0 ? (
                                <select value="" disabled style={{ ...inputStyle, cursor: 'not-allowed', opacity: 0.6 }}>
                                    <option>No lines yet</option>
                                </select>
                            ) : (
                                <>
                                    <select
                                        value={selectedBranchValue}
                                        onChange={(event) => setSelectedBranchValue(event.target.value)}
                                        style={{ ...inputStyle, cursor: 'pointer' }}
                                    >
                                        {displayedBranchKeys.map((value) => (
                                            <option key={`${current._id}-branch-option-${value}`} value={value}>
                                                {value}
                                            </option>
                                        ))}
                                    </select>

                                    {selectedBranchValue ? (
                                        <div style={{ paddingLeft: '14px', borderLeft: `2px solid ${theme.border}` }}>
                                            <ReportNodeCard
                                                node={selectedBranchNode}
                                                title="Next"
                                                branchValue={selectedBranchValue}
                                                availableFields={availableFields}
                                                sortByOptions={sortByOptions}
                                                theme={theme}
                                                reportRows={reportRows}
                                                ancestorValues={selectedBranchAncestorValues}
                                                templateMode={templateMode}
                                                onChange={(nextChild) => materializeBranchChild(selectedBranchValue, nextChild)}
                                                onRemove={selectedExplicitChild ? () => setBranchChild(selectedBranchValue, null) : null}
                                            />
                                        </div>
                                    ) : null}
                                </>
                            )}
                        </div>
                    ) : null}
                </>
            ) : null}
        </div>
    );
}

export function ReportEditor({
    reportDef,
    setReportDef,
    availableFields,
    theme,
    data,
    valConfigs,
    savedReports,
    setSavedReports,
    activeReportId,
    setActiveReportId,
    showNotification,
}) {
    const [reportName, setReportName] = useState('');
    const currentReport = useMemo(
        () => ({ ...(reportDef || {}), root: reportDef && reportDef.root ? normalizeNode(reportDef.root) : null }),
        [reportDef]
    );
    const selectableFields = useMemo(
        () => getReportSelectableFields(availableFields, valConfigs),
        [availableFields, valConfigs]
    );

    const sortByOptions = useMemo(() => {
        const options = [{ value: '', label: '(default)' }];
        (valConfigs || []).forEach((config) => {
            if (!config || !config.field || !config.agg) return;
            options.push({
                value: config.agg === 'formula' ? config.field : `${config.field}_${config.agg}`,
                label: config.agg === 'formula'
                    ? (config.label || formatDisplayLabel(config.field))
                    : `${formatDisplayLabel(config.field)} (${formatAggLabel(config.agg, config.weightField)})`,
            });
        });
        return options;
    }, [valConfigs]);

    const setRoot = useCallback((root) => {
        setReportDef({ ...(currentReport || {}), root });
    }, [currentReport, setReportDef]);

    const createRootFromField = useCallback((field) => {
        setRoot(createNode(field));
    }, [setRoot]);

    const saveReport = useCallback(() => {
        if (!currentReport.root || !currentReport.root.field) return;
        const id = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        const name = reportName.trim() || `Report ${(savedReports || []).length + 1}`;
        setSavedReports([
            ...(savedReports || []),
            {
                id,
                name,
                reportDef: JSON.parse(JSON.stringify(currentReport)),
                createdAt: new Date().toISOString(),
            },
        ]);
        setActiveReportId(id);
        setReportName('');
        if (showNotification) showNotification(`Report "${name}" saved`, 'info');
    }, [currentReport, reportName, savedReports, setActiveReportId, setSavedReports, showNotification]);

    const sectionLabelStyle = {
        fontSize: '10px',
        fontWeight: 800,
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: theme.textSec,
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        marginBottom: '10px',
    };
    const buttonStyle = {
        background: theme.hover,
        border: `1px solid ${theme.border}`,
        borderRadius: '9px',
        padding: '8px 12px',
        fontSize: '12px',
        fontWeight: 700,
        color: theme.text,
        cursor: 'pointer',
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', padding: '6px 0 4px' }}>
            {(savedReports || []).length > 0 ? (
                <div>
                    <div style={sectionLabelStyle}>
                        <Icons.Save />
                        Saved Reports
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {(savedReports || []).map((report) => (
                            <div
                                key={report.id}
                                onClick={() => {
                                    setReportDef(JSON.parse(JSON.stringify(report.reportDef)));
                                    setActiveReportId(report.id);
                                }}
                                style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '10px',
                                    padding: '10px 12px',
                                    borderRadius: '12px',
                                    background: report.id === activeReportId ? (theme.select || '#E8F0FE') : (theme.background || '#fff'),
                                    border: `1px solid ${report.id === activeReportId ? (theme.primary || '#2563EB') : theme.border}`,
                                    cursor: 'pointer',
                                }}
                            >
                                <Icons.Report />
                                <div style={{ flex: 1, minWidth: 0 }}>
                                    <div style={{ fontSize: '12px', fontWeight: report.id === activeReportId ? 800 : 600, color: theme.text }}>
                                        {report.name}
                                    </div>
                                    <div style={{ fontSize: '11px', color: theme.textSec }}>
                                        {countNodes(report.reportDef && report.reportDef.root)} steps
                                    </div>
                                </div>
                                <button
                                    type="button"
                                    onClick={(event) => {
                                        event.stopPropagation();
                                        const nextId = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
                                        setSavedReports([
                                            ...(savedReports || []),
                                            {
                                                ...JSON.parse(JSON.stringify(report)),
                                                id: nextId,
                                                name: `${report.name} (copy)`,
                                                createdAt: new Date().toISOString(),
                                            },
                                        ]);
                                    }}
                                    style={{ ...buttonStyle, padding: '6px 8px' }}
                                >
                                    <Icons.Duplicate />
                                </button>
                                <button
                                    type="button"
                                    onClick={(event) => {
                                        event.stopPropagation();
                                        setSavedReports((savedReports || []).filter((item) => item.id !== report.id));
                                        if (activeReportId === report.id) setActiveReportId(null);
                                    }}
                                    style={{ ...buttonStyle, padding: '6px 8px', color: '#DC2626' }}
                                >
                                    <Icons.Delete />
                                </button>
                            </div>
                        ))}
                    </div>
                </div>
            ) : null}

            <div>
                <div style={sectionLabelStyle}>
                    <Icons.ReportLevel />
                    Report Builder
                </div>
                <div
                    style={{
                        border: `1px solid ${theme.border}`,
                        borderRadius: '14px',
                        background: theme.surfaceBg || theme.background || '#fff',
                        padding: '14px',
                        minHeight: '240px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '12px',
                    }}
                >
                    <div>
                        <div style={{ fontSize: '10px', fontWeight: 800, letterSpacing: '0.08em', textTransform: 'uppercase', color: theme.textSec }}>
                            Flow
                        </div>
                        <div style={{ fontSize: '13px', color: theme.text, fontWeight: 800, marginTop: '4px' }}>
                            {currentReport.root ? getNodeTitle(currentReport.root) : 'Pick the first field'}
                        </div>
                    </div>

                    {!currentReport.root ? (
                        <FieldSelectorPanel
                            theme={theme}
                            label="First field"
                            hint=""
                            availableFields={selectableFields}
                            onChange={createRootFromField}
                        />
                    ) : (
                        <ReportNodeCard
                            node={currentReport.root}
                            title="Step 1"
                            availableFields={selectableFields}
                            sortByOptions={sortByOptions}
                            theme={theme}
                            reportRows={data}
                            ancestorValues={[]}
                            onChange={setRoot}
                            onRemove={() => setRoot(null)}
                        />
                    )}
                </div>
            </div>

            <div style={{ borderTop: `1px solid ${theme.border}`, paddingTop: '12px' }}>
                <div style={sectionLabelStyle}>
                    <Icons.Save />
                    Save Report
                </div>
                <div style={{ display: 'flex', gap: '8px' }}>
                    <input
                        value={reportName}
                        onChange={(event) => setReportName(event.target.value)}
                        onKeyDown={(event) => {
                            if (event.key === 'Enter') saveReport();
                        }}
                        placeholder="Report name..."
                        style={{ ...makeInputStyle(theme), flex: 1 }}
                    />
                    <button
                        type="button"
                        onClick={saveReport}
                        disabled={!currentReport.root || !currentReport.root.field}
                        style={{
                            ...buttonStyle,
                            background: theme.primary || '#2563EB',
                            borderColor: theme.primary || '#2563EB',
                            color: '#fff',
                            opacity: (!currentReport.root || !currentReport.root.field) ? 0.55 : 1,
                        }}
                    >
                        <Icons.Save />
                        Save
                    </button>
                </div>
            </div>
        </div>
    );
}

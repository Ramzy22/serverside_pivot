import React, { useState, useCallback, useMemo } from 'react';
import Icons from '../../utils/Icons';
import { formatAggLabel, formatDisplayLabel } from '../../utils/helpers';

// ─── IDs ──────────────────────────────────────────────────────────────────────
const uid = () => `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

// ─── Operators ────────────────────────────────────────────────────────────────
const OPERATORS = [
    { value: 'eq',          label: '= equals' },
    { value: 'not_eq',      label: '≠ not equals' },
    { value: 'in',          label: '∈ in list' },
    { value: 'not_in',      label: '∉ not in list' },
    { value: 'contains',    label: '~ contains' },
    { value: 'not_contains',label: '!~ not contains' },
    { value: 'gt',          label: '> greater than' },
    { value: 'gte',         label: '≥ greater or equal' },
    { value: 'lt',          label: '< less than' },
    { value: 'lte',         label: '≤ less or equal' },
];
const LIST_OPS = new Set(['in', 'not_in']);
const OP_SYMBOL = { eq:'=', not_eq:'≠', in:'∈', not_in:'∉', contains:'~', not_contains:'!~', gt:'>', gte:'≥', lt:'<', lte:'≤' };

const REPORT_META = new Set([
    'depth','_id','_path','_levelLabel','_levelField','_pathFields',
    '_has_children','_is_expanded','__col_schema',
]);

// ─── Constructors ─────────────────────────────────────────────────────────────
const createClause  = (field = '') => ({ _id: uid(), field, operator: 'eq', value: '', values: [] });
const createCondition = ()         => ({ op: 'AND', clauses: [] });
const createBranch  = ()           => ({ _id: uid(), label: '', condition: createCondition(), child: null });
const createNode    = (field = '') => ({
    _id: uid(), field, label: '', topN: null,
    sortBy: null, sortDir: 'desc', defaultChild: null, branches: [],
});

// ─── Normalize ────────────────────────────────────────────────────────────────
const normalizeClause = (c) => {
    if (!c || typeof c.field !== 'string' || !c.field) return null;
    return {
        _id: typeof c._id === 'string' ? c._id : uid(),
        field: c.field,
        operator: OPERATORS.find(o => o.value === c.operator) ? c.operator : 'eq',
        value: (c.value !== undefined && c.value !== null) ? String(c.value) : '',
        values: Array.isArray(c.values) ? c.values.map(String).filter(Boolean) : [],
    };
};

const normalizeCondition = (c) => ({
    op: c && c.op === 'OR' ? 'OR' : 'AND',
    clauses: Array.isArray(c && c.clauses) ? c.clauses.map(normalizeClause).filter(Boolean) : [],
});

const normalizeBranch = (b) => {
    if (!b || typeof b !== 'object') return null;
    return {
        _id: typeof b._id === 'string' ? b._id : uid(),
        label: typeof b.label === 'string' ? b.label : '',
        condition: normalizeCondition(b.condition),
        // eslint-disable-next-line no-use-before-define
        child: b.child ? normalizeNode(b.child) : null,
    };
};

function normalizeNode(n) {
    if (!n || typeof n !== 'object') return null;

    // Migration: old childrenByValue dict → branches array
    let branches = Array.isArray(n.branches) ? n.branches.map(normalizeBranch).filter(Boolean) : [];
    if (branches.length === 0 && n.childrenByValue && typeof n.childrenByValue === 'object') {
        branches = Object.entries(n.childrenByValue)
            .filter(([key]) => key)
            .map(([key, child]) => normalizeBranch({
                label: key,
                condition: { op: 'AND', clauses: [{ field: n.field || '', operator: 'eq', value: key, values: [] }] },
                child,
            }))
            .filter(Boolean);
    }

    return {
        _id: typeof n._id === 'string' ? n._id : uid(),
        field: typeof n.field === 'string' ? n.field : '',
        label: typeof n.label === 'string' ? n.label : '',
        topN: Number.isFinite(Number(n.topN)) && Number(n.topN) > 0 ? Math.floor(Number(n.topN)) : null,
        sortBy: typeof n.sortBy === 'string' && n.sortBy.trim() ? n.sortBy.trim() : null,
        sortDir: n.sortDir === 'asc' ? 'asc' : 'desc',
        defaultChild: n.defaultChild ? normalizeNode(n.defaultChild) : null,
        branches,
    };
}

// ─── Field helpers ────────────────────────────────────────────────────────────
const getSelectableFields = (availableFields, valConfigs) => {
    const aggIds = new Set(
        (valConfigs || []).filter(c => c && c.field && c.agg)
            .map(c => c.agg === 'formula' ? c.field : `${c.field}_${c.agg}`)
    );
    return (availableFields || []).filter(f =>
        f && typeof f === 'string' && !REPORT_META.has(f) && !f.startsWith('_') && !aggIds.has(f)
    );
};

const buildSortOptions = (valConfigs) => {
    const opts = [{ value: '', label: '(default)' }];
    (valConfigs || []).forEach(cfg => {
        if (!cfg || !cfg.field || !cfg.agg) return;
        const value = cfg.agg === 'formula' ? cfg.field : `${cfg.field}_${cfg.agg}`;
        const label = cfg.agg === 'formula'
            ? (cfg.label || formatDisplayLabel(cfg.field))
            : `${formatDisplayLabel(cfg.field)} (${formatAggLabel(cfg.agg, cfg.weightField)})`;
        opts.push({ value, label });
    });
    return opts;
};

// ─── Condition summary ────────────────────────────────────────────────────────
const formatConditionSummary = (condition) => {
    if (!condition || !condition.clauses || condition.clauses.length === 0) return 'No filter (default)';
    const parts = condition.clauses.map(c => {
        const field = formatDisplayLabel(c.field);
        const op = OP_SYMBOL[c.operator] || c.operator;
        if (LIST_OPS.has(c.operator)) {
            const shown = c.values.slice(0, 3);
            const extra = c.values.length > 3 ? ` +${c.values.length - 3}` : '';
            return `${field} ${op} [${shown.join(', ')}${extra}]`;
        }
        return `${field} ${op} ${c.value || '…'}`;
    });
    return parts.join(` ${condition.op} `);
};

// ─── Shared style helpers ─────────────────────────────────────────────────────
const inp = (theme, extra = {}) => ({
    border: `1px solid ${theme.border}`,
    borderRadius: '8px',
    padding: '5px 8px',
    fontSize: '12px',
    background: theme.background || '#fff',
    color: theme.text,
    outline: 'none',
    ...extra,
});

const ghostBtn = (theme, active = false, danger = false) => ({
    display: 'inline-flex', alignItems: 'center', gap: '4px',
    background: danger ? '#FEF2F2' : active ? `${theme.primary || '#2563EB'}12` : theme.hover,
    border: `1px solid ${danger ? '#FECACA' : active ? `${theme.primary || '#2563EB'}50` : theme.border}`,
    borderRadius: '7px', padding: '4px 9px',
    fontSize: '11px', fontWeight: 700,
    color: danger ? '#DC2626' : active ? (theme.primary || '#2563EB') : theme.textSec,
    cursor: 'pointer',
});

// ─── ConditionModal ───────────────────────────────────────────────────────────
function ConditionModal({ condition, allFields, onSave, onClose, theme }) {
    const [local, setLocal] = useState(() => normalizeCondition(condition));

    const addClause = useCallback(() => {
        setLocal(prev => ({ ...prev, clauses: [...prev.clauses, createClause(allFields[0] || '')] }));
    }, [allFields]);

    const updateClause = useCallback((idx, patch) => {
        setLocal(prev => ({ ...prev, clauses: prev.clauses.map((c, i) => i === idx ? { ...c, ...patch } : c) }));
    }, []);

    const removeClause = useCallback((idx) => {
        setLocal(prev => ({ ...prev, clauses: prev.clauses.filter((_, i) => i !== idx) }));
    }, []);

    const inputStyle = inp(theme);

    return (
        <div
            style={{ position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.40)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
            onClick={onClose}
        >
            <div
                style={{
                    background: theme.surfaceBg || theme.background || '#fff',
                    border: `1px solid ${theme.border}`,
                    borderRadius: '16px', padding: '22px',
                    width: '500px', maxWidth: '95vw', maxHeight: '85vh', overflowY: 'auto',
                    boxShadow: '0 24px 64px rgba(0,0,0,0.18)',
                    display: 'flex', flexDirection: 'column', gap: '14px',
                }}
                onClick={e => e.stopPropagation()}
            >
                {/* Header */}
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <div style={{ fontSize: '14px', fontWeight: 800, color: theme.text }}>Edit Condition</div>
                    <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.textSec }}>
                        <Icons.Close />
                    </button>
                </div>

                {/* AND / OR toggle */}
                {local.clauses.length > 1 && (
                    <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                        <span style={{ fontSize: '11px', color: theme.textSec, fontWeight: 700 }}>Match:</span>
                        {['AND', 'OR'].map(op => (
                            <button
                                key={op}
                                onClick={() => setLocal(prev => ({ ...prev, op }))}
                                style={{
                                    padding: '4px 14px', borderRadius: '20px',
                                    fontSize: '11px', fontWeight: 800, border: 'none', cursor: 'pointer',
                                    background: local.op === op ? (theme.primary || '#2563EB') : theme.hover,
                                    color: local.op === op ? '#fff' : theme.textSec,
                                }}
                            >
                                {op}
                            </button>
                        ))}
                        <span style={{ fontSize: '11px', color: theme.textSec }}>of the following</span>
                    </div>
                )}

                {/* Clauses */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {local.clauses.length === 0 && (
                        <div style={{ fontSize: '12px', color: theme.textSec, fontStyle: 'italic', padding: '4px 0' }}>
                            No clauses — this branch matches all rows (acts as default).
                        </div>
                    )}
                    {local.clauses.map((clause, idx) => (
                        <div
                            key={clause._id}
                            style={{
                                display: 'flex', gap: '6px', alignItems: 'flex-start',
                                padding: '10px', background: theme.hover, borderRadius: '10px',
                            }}
                        >
                            {/* AND/OR badge (for idx > 0) */}
                            <div style={{ width: '28px', flexShrink: 0, paddingTop: '6px', textAlign: 'center' }}>
                                {idx > 0 && (
                                    <span style={{ fontSize: '9px', fontWeight: 900, color: theme.primary || '#2563EB', letterSpacing: '0.04em' }}>
                                        {local.op}
                                    </span>
                                )}
                            </div>

                            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                {/* Field + operator */}
                                <div style={{ display: 'flex', gap: '6px' }}>
                                    <select
                                        value={clause.field}
                                        onChange={e => updateClause(idx, { field: e.target.value })}
                                        style={{ ...inputStyle, flex: 1 }}
                                    >
                                        {allFields.map(f => (
                                            <option key={f} value={f}>{formatDisplayLabel(f)}</option>
                                        ))}
                                    </select>
                                    <select
                                        value={clause.operator}
                                        onChange={e => {
                                            const next = e.target.value;
                                            const toList = LIST_OPS.has(next);
                                            const wasScalar = !LIST_OPS.has(clause.operator);
                                            updateClause(idx, {
                                                operator: next,
                                                values: toList ? (wasScalar && clause.value ? [clause.value] : clause.values) : clause.values,
                                                value: !toList ? (clause.values[0] || clause.value || '') : clause.value,
                                            });
                                        }}
                                        style={{ ...inputStyle, flexShrink: 0 }}
                                    >
                                        {OPERATORS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                                    </select>
                                </div>

                                {/* Value input */}
                                {LIST_OPS.has(clause.operator) ? (
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                        {clause.values.length > 0 && (
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {clause.values.map((v, vi) => (
                                                    <span key={vi} style={{
                                                        display: 'inline-flex', alignItems: 'center', gap: '4px',
                                                        background: `${theme.primary || '#2563EB'}18`,
                                                        color: theme.primary || '#2563EB',
                                                        borderRadius: '6px', padding: '2px 7px',
                                                        fontSize: '11px', fontWeight: 700,
                                                    }}>
                                                        {v}
                                                        <button
                                                            onClick={() => updateClause(idx, { values: clause.values.filter((_, i) => i !== vi) })}
                                                            style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0, color: 'inherit', lineHeight: 1, fontSize: '13px' }}
                                                        >×</button>
                                                    </span>
                                                ))}
                                            </div>
                                        )}
                                        <input
                                            placeholder="Type value and press Enter to add..."
                                            style={{ ...inputStyle, width: '100%' }}
                                            onKeyDown={e => {
                                                if (e.key === 'Enter' && e.target.value.trim()) {
                                                    const v = e.target.value.trim();
                                                    if (!clause.values.includes(v)) {
                                                        updateClause(idx, { values: [...clause.values, v] });
                                                    }
                                                    e.target.value = '';
                                                    e.preventDefault();
                                                }
                                            }}
                                        />
                                    </div>
                                ) : (
                                    <input
                                        value={clause.value}
                                        onChange={e => updateClause(idx, { value: e.target.value })}
                                        placeholder="Value..."
                                        style={{ ...inputStyle, width: '100%' }}
                                    />
                                )}
                            </div>

                            <button
                                onClick={() => removeClause(idx)}
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#DC2626', padding: '4px', flexShrink: 0 }}
                            >
                                <Icons.Delete />
                            </button>
                        </div>
                    ))}
                </div>

                {/* Add clause */}
                <button
                    onClick={addClause}
                    style={{
                        ...ghostBtn(theme), width: '100%', justifyContent: 'center',
                        border: `1px dashed ${theme.border}`,
                    }}
                >
                    <Icons.Add /> Add clause
                </button>

                {/* Footer */}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', borderTop: `1px solid ${theme.border}`, paddingTop: '14px' }}>
                    <button onClick={onClose} style={{ ...inp(theme), cursor: 'pointer', fontWeight: 700 }}>
                        Cancel
                    </button>
                    <button
                        onClick={() => { onSave(local); onClose(); }}
                        style={{
                            background: theme.primary || '#2563EB', border: 'none', borderRadius: '8px',
                            padding: '7px 18px', fontSize: '12px', fontWeight: 800, color: '#fff', cursor: 'pointer',
                        }}
                    >
                        Apply
                    </button>
                </div>
            </div>
        </div>
    );
}

// ─── Branch Row ───────────────────────────────────────────────────────────────
function BranchRow({ branch, depth, stepCounter, allFields, sortByOptions, theme, onUpdate, onRemove }) {
    const [editingCond, setEditingCond] = useState(false);
    const [pathOpen, setPathOpen] = useState(branch.child !== null);
    const hasCustomPath = branch.child !== null;
    const summary = formatConditionSummary(branch.condition);
    const primary = theme.primary || '#2563EB';

    const open = pathOpen || hasCustomPath;

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {/* Branch header */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
                {/* Condition badge */}
                <button
                    onClick={() => setEditingCond(true)}
                    title={summary}
                    style={{
                        display: 'inline-flex', alignItems: 'center', gap: '5px',
                        background: `${primary}12`, border: `1px solid ${primary}35`,
                        borderRadius: '7px', padding: '4px 9px',
                        fontSize: '11px', fontWeight: 700, color: primary, cursor: 'pointer',
                        maxWidth: '220px', overflow: 'hidden', whiteSpace: 'nowrap', textOverflow: 'ellipsis',
                    }}
                >
                    <Icons.Filter />
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{summary}</span>
                </button>

                <span style={{ fontSize: '12px', color: theme.textSec }}>→</span>

                {/* Display-as label */}
                <input
                    value={branch.label}
                    onChange={e => onUpdate({ ...branch, label: e.target.value })}
                    placeholder="Display as..."
                    title="Rename: how matching rows appear in the report"
                    style={{ ...inp(theme), width: '110px' }}
                />

                {/* Custom path toggle */}
                <button
                    onClick={() => setPathOpen(o => !o)}
                    style={ghostBtn(theme, hasCustomPath)}
                    title="Define a custom sub-path for rows matching this condition"
                >
                    {hasCustomPath ? 'Custom path ▾' : 'Custom path?'}
                </button>

                {/* Remove branch */}
                <button onClick={onRemove} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#DC2626', padding: '2px' }}>
                    <Icons.Delete />
                </button>
            </div>

            {/* Custom sub-path */}
            {open && (
                <div style={{ paddingLeft: '16px', borderLeft: `2px dashed ${primary}30` }}>
                    {!branch.child ? (
                        <button
                            onClick={() => {
                                onUpdate({ ...branch, child: createNode(allFields[0] || '') });
                                setPathOpen(true);
                            }}
                            style={{
                                ...ghostBtn(theme, true), border: `1px dashed ${primary}60`,
                                width: '100%', justifyContent: 'center', padding: '8px',
                            }}
                        >
                            <Icons.Add /> Add custom drilldown for this path
                        </button>
                    ) : (
                        <PipelineStep
                            node={branch.child}
                            stepNumber={stepCounter}
                            depth={depth + 1}
                            allFields={allFields}
                            sortByOptions={sortByOptions}
                            theme={theme}
                            onChange={nextChild => onUpdate({ ...branch, child: nextChild })}
                            onRemoveChain={() => onUpdate({ ...branch, child: null })}
                        />
                    )}
                </div>
            )}

            {editingCond && (
                <ConditionModal
                    condition={branch.condition}
                    allFields={allFields}
                    theme={theme}
                    onSave={nextCond => onUpdate({ ...branch, condition: nextCond })}
                    onClose={() => setEditingCond(false)}
                />
            )}
        </div>
    );
}

// ─── Pipeline Step ────────────────────────────────────────────────────────────
function PipelineStep({ node, stepNumber, depth, allFields, sortByOptions, theme, onChange, onRemoveChain }) {
    const [branchesOpen, setBranchesOpen] = useState(false);
    const current = useMemo(() => normalizeNode(node), [node]);
    const inputStyle = inp(theme);
    const primary = theme.primary || '#2563EB';
    const bulletColor = depth === 0 ? primary : '#8B5CF6';

    const update = useCallback(patch => onChange({ ...current, ...patch }), [current, onChange]);
    const addBranch = useCallback(() => { update({ branches: [...current.branches, createBranch()] }); setBranchesOpen(true); }, [current.branches, update]);
    const updateBranch = useCallback((idx, b) => update({ branches: current.branches.map((x, i) => i === idx ? b : x) }), [current.branches, update]);
    const removeBranch = useCallback((idx) => update({ branches: current.branches.filter((_, i) => i !== idx) }), [current.branches, update]);
    const addChild = useCallback(() => { if (!current.defaultChild) update({ defaultChild: createNode(allFields[0] || '') }); }, [current.defaultChild, allFields, update]);

    const hasBranches = current.branches.length > 0;

    return (
        <div style={{ display: 'flex', flexDirection: 'column' }}>
            <div style={{ display: 'flex', gap: '10px', alignItems: 'stretch' }}>
                {/* Left track */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flexShrink: 0, width: '22px' }}>
                    <div style={{
                        width: '22px', height: '22px', borderRadius: '50%',
                        background: bulletColor, color: '#fff',
                        fontSize: '10px', fontWeight: 900,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        flexShrink: 0, boxShadow: `0 0 0 3px ${bulletColor}20`,
                    }}>
                        {stepNumber}
                    </div>
                    <div style={{ width: '2px', flex: 1, minHeight: '16px', background: `${bulletColor}25`, marginTop: '3px' }} />
                </div>

                {/* Step card */}
                <div style={{
                    flex: 1, minWidth: 0,
                    border: `1px solid ${theme.border}`,
                    borderRadius: '10px',
                    background: theme.surfaceBg || theme.background || '#fff',
                    padding: '10px 12px',
                    marginBottom: '6px',
                    display: 'flex', flexDirection: 'column', gap: '8px',
                }}>
                    {/* Card header */}
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <span style={{ fontSize: '9px', fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.08em', color: theme.textSec }}>
                            {depth === 0 ? 'Break down by' : 'Then by'}
                        </span>
                        <button onClick={onRemoveChain} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#DC2626', padding: '2px' }}>
                            <Icons.Delete />
                        </button>
                    </div>

                    {/* Field / Top N / Sort / Dir */}
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 52px 1fr 28px', gap: '6px', alignItems: 'center' }}>
                        <select
                            value={current.field}
                            onChange={e => update({ field: e.target.value })}
                            style={{ ...inputStyle, fontWeight: 700 }}
                        >
                            <option value="">Select field…</option>
                            {allFields.map(f => <option key={f} value={f}>{formatDisplayLabel(f)}</option>)}
                        </select>

                        <input
                            type="number" min="1"
                            value={current.topN || ''}
                            onChange={e => {
                                const v = e.target.value === '' ? null : parseInt(e.target.value, 10);
                                update({ topN: v && v > 0 ? v : null });
                            }}
                            placeholder="All"
                            title="Top N rows (leave empty for all)"
                            style={{ ...inputStyle, textAlign: 'center' }}
                        />

                        <select
                            value={current.sortBy || ''}
                            onChange={e => update({ sortBy: e.target.value || null })}
                            style={inputStyle}
                        >
                            {sortByOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                        </select>

                        <button
                            onClick={() => update({ sortDir: current.sortDir === 'asc' ? 'desc' : 'asc' })}
                            style={{
                                background: theme.hover, border: `1px solid ${theme.border}`,
                                borderRadius: '8px', padding: '5px',
                                cursor: 'pointer', color: theme.text,
                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                            }}
                            title={current.sortDir === 'asc' ? 'Ascending' : 'Descending'}
                        >
                            {current.sortDir === 'asc' ? <Icons.SortAsc /> : <Icons.SortDesc />}
                        </button>
                    </div>

                    {/* Display label */}
                    <div>
                        <div style={{ fontSize: '9px', fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.06em', color: theme.textSec, marginBottom: '4px' }}>
                            Header label
                        </div>
                        <input
                            value={current.label}
                            onChange={e => update({ label: e.target.value })}
                            placeholder={current.field ? formatDisplayLabel(current.field) : 'Label for this level…'}
                            title="Rename this level's header in the report output"
                            style={{ ...inputStyle, width: '100%' }}
                        />
                    </div>

                    {/* Branch controls */}
                    <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                        <button onClick={addBranch} style={ghostBtn(theme)}>
                            <Icons.Branch /> Add split
                        </button>
                        {hasBranches && (
                            <button
                                onClick={() => setBranchesOpen(o => !o)}
                                style={ghostBtn(theme, true)}
                            >
                                <Icons.Branch />
                                {current.branches.length} split{current.branches.length !== 1 ? 's' : ''} {branchesOpen ? '▲' : '▼'}
                            </button>
                        )}
                    </div>

                    {/* Branches section */}
                    {hasBranches && branchesOpen && (
                        <div style={{
                            borderTop: `1px solid ${theme.border}`, paddingTop: '10px',
                            display: 'flex', flexDirection: 'column', gap: '10px',
                        }}>
                            <div style={{ fontSize: '9px', fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.06em', color: theme.textSec }}>
                                Conditional Splits
                                <span style={{ fontWeight: 500, textTransform: 'none', marginLeft: '6px', color: theme.textSec }}>— first match wins, others use default path</span>
                            </div>
                            {current.branches.map((branch, idx) => (
                                <BranchRow
                                    key={branch._id}
                                    branch={branch}
                                    depth={depth}
                                    stepCounter={`${stepNumber}.${idx + 1}`}
                                    allFields={allFields}
                                    sortByOptions={sortByOptions}
                                    theme={theme}
                                    onUpdate={nb => updateBranch(idx, nb)}
                                    onRemove={() => removeBranch(idx)}
                                />
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Next step in default chain */}
            <div style={{ paddingLeft: '32px' }}>
                {current.defaultChild ? (
                    <PipelineStep
                        node={current.defaultChild}
                        stepNumber={stepNumber + 1}
                        depth={depth}
                        allFields={allFields}
                        sortByOptions={sortByOptions}
                        theme={theme}
                        onChange={nextChild => update({ defaultChild: nextChild })}
                        onRemoveChain={() => update({ defaultChild: null })}
                    />
                ) : (
                    <button
                        onClick={addChild}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '6px',
                            background: 'none', border: `1px dashed ${theme.border}`,
                            borderRadius: '8px', padding: '7px 14px', cursor: 'pointer',
                            fontSize: '11px', fontWeight: 700, color: theme.textSec,
                            marginBottom: '8px',
                        }}
                    >
                        <Icons.Add /> Add next level
                    </button>
                )}
            </div>
        </div>
    );
}

// ─── ReportEditor (main export) ───────────────────────────────────────────────
export function ReportEditor({
    reportDef,
    setReportDef,
    availableFields,
    theme,
    valConfigs,
    savedReports,
    setSavedReports,
    activeReportId,
    setActiveReportId,
    showNotification,
}) {
    const [reportName, setReportName] = useState('');

    const root = useMemo(
        () => reportDef && reportDef.root ? normalizeNode(reportDef.root) : null,
        [reportDef]
    );
    const allFields = useMemo(() => getSelectableFields(availableFields, valConfigs), [availableFields, valConfigs]);
    const sortByOptions = useMemo(() => buildSortOptions(valConfigs), [valConfigs]);

    const setRoot = useCallback(nextRoot => {
        setReportDef({ ...(reportDef || {}), root: nextRoot });
    }, [reportDef, setReportDef]);

    const saveReport = useCallback(() => {
        if (!root || !root.field) return;
        const id = `report-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        const name = reportName.trim() || `Report ${(savedReports || []).length + 1}`;
        setSavedReports([
            ...(savedReports || []),
            { id, name, reportDef: JSON.parse(JSON.stringify(reportDef)), createdAt: new Date().toISOString() },
        ]);
        setActiveReportId(id);
        setReportName('');
        if (showNotification) showNotification(`Report "${name}" saved`, 'info');
    }, [root, reportName, savedReports, reportDef, setSavedReports, setActiveReportId, showNotification]);

    const inputStyle = inp(theme);
    const labelStyle = {
        fontSize: '9px', fontWeight: 800, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: theme.textSec,
        display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '10px',
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '18px', padding: '2px 0 8px' }}>
            {/* Saved reports */}
            {(savedReports || []).length > 0 && (
                <div>
                    <div style={labelStyle}><Icons.Save /> Saved Reports</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
                        {(savedReports || []).map(report => (
                            <div
                                key={report.id}
                                onClick={() => { setReportDef(JSON.parse(JSON.stringify(report.reportDef))); setActiveReportId(report.id); }}
                                style={{
                                    display: 'flex', alignItems: 'center', gap: '8px',
                                    padding: '8px 10px', borderRadius: '10px', cursor: 'pointer',
                                    background: report.id === activeReportId ? (theme.select || `${theme.primary || '#2563EB'}14`) : (theme.hover || '#F9FAFB'),
                                    border: `1px solid ${report.id === activeReportId ? (theme.primary || '#2563EB') : theme.border}`,
                                }}
                            >
                                <Icons.Report />
                                <div style={{ flex: 1, minWidth: 0, fontSize: '12px', fontWeight: 700, color: theme.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                    {report.name}
                                </div>
                                <button
                                    type="button"
                                    onClick={e => {
                                        e.stopPropagation();
                                        setSavedReports((savedReports || []).filter(r => r.id !== report.id));
                                        if (activeReportId === report.id) setActiveReportId(null);
                                    }}
                                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#DC2626', padding: '2px', flexShrink: 0 }}
                                >
                                    <Icons.Delete />
                                </button>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Pipeline builder */}
            <div>
                <div style={labelStyle}><Icons.ReportLevel /> Report Pipeline</div>

                {!root ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        <div style={{ fontSize: '12px', color: theme.textSec }}>Choose the first field to break down by:</div>
                        <select
                            value=""
                            onChange={e => { if (e.target.value) setRoot(createNode(e.target.value)); }}
                            style={{ ...inputStyle, width: '100%' }}
                        >
                            <option value="">Select first field…</option>
                            {allFields.map(f => <option key={f} value={f}>{formatDisplayLabel(f)}</option>)}
                        </select>
                    </div>
                ) : (
                    <PipelineStep
                        node={root}
                        stepNumber={1}
                        depth={0}
                        allFields={allFields}
                        sortByOptions={sortByOptions}
                        theme={theme}
                        onChange={setRoot}
                        onRemoveChain={() => setRoot(null)}
                    />
                )}
            </div>

            {/* Save */}
            <div style={{ borderTop: `1px solid ${theme.border}`, paddingTop: '14px' }}>
                <div style={labelStyle}><Icons.Save /> Save Report</div>
                <div style={{ display: 'flex', gap: '8px' }}>
                    <input
                        value={reportName}
                        onChange={e => setReportName(e.target.value)}
                        onKeyDown={e => { if (e.key === 'Enter') saveReport(); }}
                        placeholder="Report name…"
                        style={{ ...inputStyle, flex: 1 }}
                    />
                    <button
                        type="button"
                        onClick={saveReport}
                        disabled={!root || !root.field}
                        style={{
                            display: 'inline-flex', alignItems: 'center', gap: '6px',
                            background: theme.primary || '#2563EB', border: 'none',
                            borderRadius: '8px', padding: '7px 14px',
                            fontSize: '12px', fontWeight: 800, color: '#fff', cursor: 'pointer',
                            opacity: (!root || !root.field) ? 0.5 : 1,
                        }}
                    >
                        <Icons.Save /> Save
                    </button>
                </div>
            </div>
        </div>
    );
}

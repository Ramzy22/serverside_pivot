import React from 'react';
import FilterPopover from '../Filters/FilterPopover';
import SidebarFilterItem from './SidebarFilterItem';
import ToolPanelSection from './ToolPanelSection';
import ColumnTreeItem from './ColumnTreeItem';
import Icons from '../Icons';
import { isGroupColumn, getAllLeafIdsFromColumn, hasChildrenInZone } from '../../utils/helpers';
import { formatDisplayLabel, isWeightedAverageAgg } from '../../utils/helpers';
import {
    getFieldPanelLimits,
    mergeFieldPanelSize,
    sanitizeFieldPanelSizeEntry,
} from '../../utils/fieldPanelLayout';
import { ReportEditor } from './ReportEditor';

const FORMULA_REFERENCE_RE = /\b[A-Za-z_][A-Za-z0-9_]*\b/g;

function escapeRegExp(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeFormulaReferenceKey(value, fallback = 'formula') {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '')
        .replace(/[^a-z0-9_]/g, '');
    const base = normalized || String(fallback || 'formula').toLowerCase().replace(/[^a-z0-9_]/g, '') || 'formula';
    return /^[a-z_]/.test(base) ? base : `f_${base}`;
}

function getFormulaReferenceKey(config) {
    if (!config || typeof config !== 'object') return '';
    return String(
        config.formulaRef
        || normalizeFormulaReferenceKey(config.label || '', config.field || 'formula')
        || config.field
        || ''
    ).trim();
}

function buildFormulaReferenceKey(label, valConfigs, fallback = 'formula') {
    const base = normalizeFormulaReferenceKey(label, fallback);
    const taken = new Set(
        (Array.isArray(valConfigs) ? valConfigs : [])
            .map((config) => {
                if (!config) return null;
                return config.agg === 'formula'
                    ? getFormulaReferenceKey(config)
                    : config.field;
            })
            .filter(Boolean)
            .map((token) => String(token).toLowerCase())
    );
    if (!taken.has(base)) return base;
    let nextIndex = 2;
    let candidate = `${base}${nextIndex}`;
    while (taken.has(candidate)) {
        nextIndex += 1;
        candidate = `${base}${nextIndex}`;
    }
    return candidate;
}

function replaceIdentifierToken(expression, oldToken, newToken) {
    if (!expression || !oldToken || oldToken === newToken) return expression;
    return String(expression).replace(new RegExp(`\\b${escapeRegExp(oldToken)}\\b`, 'g'), newToken);
}

function updateValueConfig(setValConfigs, idx, updater) {
    setValConfigs((prev) => {
        const next = [...prev];
        if (!next[idx]) return prev;
        next[idx] = updater(next[idx]);
        return next;
    });
}

function buildSuggestedFormula(baseValues) {
    if (!Array.isArray(baseValues) || baseValues.length === 0) return '';
    if (baseValues.length === 1) return `${baseValues[0].field} * 100`;
    return `${baseValues[0].field} - ${baseValues[1].field}`;
}

function createFormulaFieldId(valConfigs) {
    const takenIds = new Set(
        (Array.isArray(valConfigs) ? valConfigs : [])
            .map((config) => config && config.field)
            .filter(Boolean)
    );
    let nextIndex = 1;
    while (takenIds.has(`formula_${nextIndex}`)) nextIndex += 1;
    return `formula_${nextIndex}`;
}

function inspectFormulaExpression(formula, valueConfigs, currentField = null, currentFormulaRef = null) {
    const trimmed = String(formula || '').trim();
    if (!trimmed) {
        return {
            tone: 'muted',
            text: 'Use arithmetic with the value fields below. Example: sales - cost',
        };
    }

    const allConfigs = Array.isArray(valueConfigs) ? valueConfigs.filter((value) => value && value.field) : [];
    if (allConfigs.length === 0) {
        return {
            tone: 'warning',
            text: 'Add at least one value or formula reference before creating formulas.',
        };
    }

    let parenDepth = 0;
    for (const char of trimmed) {
        if (char === '(') parenDepth += 1;
        if (char === ')') parenDepth -= 1;
        if (parenDepth < 0) {
            return { tone: 'warning', text: 'Parentheses are unbalanced.' };
        }
    }
    if (parenDepth !== 0) {
        return { tone: 'warning', text: 'Parentheses are unbalanced.' };
    }

    const formulaConfigs = allConfigs.filter((config) => config.agg === 'formula');
    const formulaReferenceMap = new Map();
    formulaConfigs.forEach((config) => {
        const token = config.field === currentField
            ? normalizeFormulaReferenceKey(currentFormulaRef || getFormulaReferenceKey(config), config.field)
            : getFormulaReferenceKey(config);
        if (!token) return;
        formulaReferenceMap.set(token, token);
        formulaReferenceMap.set(token.toLowerCase(), token);
    });
    const measureTokens = allConfigs
        .filter((config) => config.agg !== 'formula')
        .map((config) => config.field)
        .filter(Boolean);
    const measureTokenSet = new Set(measureTokens);
    const availableTokens = new Set([...measureTokens, ...formulaConfigs.map((config) => (
        config.field === currentField
            ? normalizeFormulaReferenceKey(currentFormulaRef || getFormulaReferenceKey(config), config.field)
            : getFormulaReferenceKey(config)
    )).filter(Boolean)]);
    const references = Array.from(new Set(trimmed.match(FORMULA_REFERENCE_RE) || []));
    const canonicalReferences = references.map((token) => formulaReferenceMap.get(token) || formulaReferenceMap.get(token.toLowerCase()) || token);
    const unknownReferences = canonicalReferences.filter((token) => !availableTokens.has(token) && !measureTokenSet.has(token));
    if (unknownReferences.length > 0) {
        return {
            tone: 'warning',
            text: `Unknown fields: ${unknownReferences.join(', ')}`,
        };
    }
    const currentToken = currentField
        ? normalizeFormulaReferenceKey(currentFormulaRef || currentField, currentField)
        : null;
    if (currentField && currentToken) {
        const duplicateReference = allConfigs.some((config) => {
            if (!config || config.field === currentField) return false;
            const token = config.agg === 'formula' ? getFormulaReferenceKey(config) : config.field;
            return String(token || '').toLowerCase() === currentToken.toLowerCase();
        });
        if (duplicateReference) {
            return {
                tone: 'warning',
                text: `Reference key "${currentToken}" is already in use.`,
            };
        }
    }
    if (currentToken && canonicalReferences.includes(currentToken)) {
        return {
            tone: 'warning',
            text: 'A formula cannot reference itself.',
        };
    }
    if (currentField && currentToken) {
        const dependencyMap = {};
        formulaConfigs.forEach((config) => {
            const configToken = config.field === currentField
                ? currentToken
                : getFormulaReferenceKey(config);
            const expr = config.field === currentField ? trimmed : String(config.formula || '');
            const exprTokens = Array.from(new Set(expr.match(FORMULA_REFERENCE_RE) || []))
                .map((token) => formulaReferenceMap.get(token) || formulaReferenceMap.get(token.toLowerCase()) || token)
                .filter((token) => token !== configToken && formulaConfigs.some((candidate) => getFormulaReferenceKey(candidate) === token || (candidate.field === currentField && currentToken === token)));
            dependencyMap[configToken] = exprTokens;
        });
        const visiting = new Set();
        const visited = new Set();
        const hasCycle = (field) => {
            if (visiting.has(field)) return true;
            if (visited.has(field)) return false;
            visiting.add(field);
            const refs = dependencyMap[field] || [];
            for (const ref of refs) {
                if (hasCycle(ref)) return true;
            }
            visiting.delete(field);
            visited.add(field);
            return false;
        };
        if (hasCycle(currentToken)) {
            return {
                tone: 'warning',
                text: 'Circular formula reference detected.',
            };
        }
    }
    if (references.length === 0) {
        return {
            tone: 'warning',
            text: 'Insert at least one measure or formula reference.',
        };
    }
    return {
        tone: 'success',
        text: `Using ${canonicalReferences.join(', ')}`,
    };
}

function FormulaChip({ item, idx, valConfigs, setValConfigs, theme, styles, dropLine, zoneId }) {
    const inputRef = React.useRef(null);
    const debounceRef = React.useRef(null);
    const [draftFormula, setDraftFormula] = React.useState(item.formula || '');

    React.useEffect(() => {
        setDraftFormula(item.formula || '');
    }, [item.formula, item.field]);

    React.useEffect(() => (
        () => clearTimeout(debounceRef.current)
    ), []);

    const nonFormulaVals = React.useMemo(
        () => valConfigs.filter((config) => config.agg !== 'formula'),
        [valConfigs]
    );
    const allValsForValidation = React.useMemo(
        () => valConfigs.filter((config) => config && config.field),
        [valConfigs]
    );
    const formulaStatus = React.useMemo(
        () => inspectFormulaExpression(draftFormula, allValsForValidation, item.field, item.formulaRef || null),
        [draftFormula, allValsForValidation, item.field, item.formulaRef]
    );

    const queueCommit = React.useCallback((nextFormula) => {
        clearTimeout(debounceRef.current);
        debounceRef.current = setTimeout(() => {
            updateValueConfig(setValConfigs, idx, (current) => ({ ...current, formula: nextFormula }));
        }, 300);
    }, [idx, setValConfigs]);

    const commit = React.useCallback((nextFormula = draftFormula) => {
        clearTimeout(debounceRef.current);
        updateValueConfig(setValConfigs, idx, (current) => ({ ...current, formula: nextFormula }));
    }, [draftFormula, idx, setValConfigs]);

    const insertAtCursor = React.useCallback((text) => {
        const el = inputRef.current;
        if (!el) return;
        const start = el.selectionStart ?? draftFormula.length;
        const end = el.selectionEnd ?? draftFormula.length;
        const nextFormula = `${draftFormula.slice(0, start)}${text}${draftFormula.slice(end)}`;
        setDraftFormula(nextFormula);
        queueCommit(nextFormula);
        const nextCursor = start + text.length;
        setTimeout(() => {
            if (!inputRef.current) return;
            inputRef.current.focus();
            inputRef.current.setSelectionRange(nextCursor, nextCursor);
        }, 0);
    }, [draftFormula, queueCommit]);

    const opBtnStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.background,
        color: theme.text,
        borderRadius: '4px',
        cursor: 'pointer',
        padding: '3px 8px',
        fontSize: '11px',
        fontFamily: 'monospace',
        lineHeight: '18px',
        flexShrink: 0,
    };

    const placeholder = nonFormulaVals.length >= 2
        ? `${nonFormulaVals[0].field} - ${nonFormulaVals[1].field}`
        : nonFormulaVals.length === 1
            ? `${nonFormulaVals[0].field} * 100`
            : 'expression...';

    const templateButtons = nonFormulaVals.length >= 2
        ? [
            { label: 'Difference', value: `${nonFormulaVals[0].field} - ${nonFormulaVals[1].field}` },
            { label: 'Ratio %', value: `(${nonFormulaVals[0].field} / ${nonFormulaVals[1].field}) * 100` },
            { label: 'Margin %', value: `((${nonFormulaVals[0].field} - ${nonFormulaVals[1].field}) / ${nonFormulaVals[0].field}) * 100` },
        ]
        : nonFormulaVals.length === 1
            ? [{ label: 'Scale x100', value: `${nonFormulaVals[0].field} * 100` }]
            : [];

    const statusColors = {
        muted: {
            text: theme.textSec,
            border: theme.border,
            background: theme.headerSubtleBg || theme.background,
        },
        success: {
            text: theme.primary,
            border: `${theme.primary}33`,
            background: `${theme.primary}10`,
        },
        warning: {
            text: '#B45309',
            border: '#F59E0B55',
            background: '#F59E0B12',
        },
    };
    const statusStyle = statusColors[formulaStatus.tone] || statusColors.muted;

    return (
        <div style={{
            border: `1px solid ${theme.primary}55`,
            borderRadius: '10px',
            background: theme.headerBg || theme.background,
            padding: '10px',
            margin: '4px 0',
            position: 'relative',
            boxShadow: theme.isDark ? 'none' : '0 6px 18px rgba(15, 23, 42, 0.06)',
        }}>
            {dropLine && dropLine.zone === zoneId && dropLine.idx === idx && <div style={{ ...styles.dropLine, top: -2 }} />}
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                <span style={{ fontSize: '10px', fontWeight: 700, color: theme.primary, background: `${theme.primary}18`, borderRadius: '999px', padding: '3px 7px', letterSpacing: '0.5px', flexShrink: 0 }}>fx</span>
                <input
                    value={item.label || ''}
                    onChange={(e) => updateValueConfig(setValConfigs, idx, (current) => ({ ...current, label: e.target.value }))}
                    placeholder="Formula column name"
                    style={{ flex: 1, fontWeight: 600, fontSize: '12px', background: 'transparent', border: 'none', borderBottom: `1px solid ${theme.border}`, outline: 'none', color: theme.text, minWidth: 0 }}
                />
                <span onClick={() => setValConfigs((p) => p.filter((_, i) => i !== idx))} style={{ cursor: 'pointer', color: '#9CA3AF', display: 'flex', alignItems: 'center', flexShrink: 0 }}><Icons.Close /></span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', marginBottom: '8px', fontSize: '10px', color: theme.textSec }}>
                <span>Output id: <code style={{ fontFamily: 'monospace', color: theme.text }}>{item.field}</code></span>
                <span>Post-aggregation</span>
            </div>
            {nonFormulaVals.length > 0 && (
                <div style={{ marginBottom: '8px' }}>
                    <div style={{ fontSize: '9px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.6px', marginBottom: '4px' }}>Fields — click to insert</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                        {nonFormulaVals.map(v => (
                            <button key={v.field} type="button" onClick={() => insertAtCursor(v.field)} style={{
                                border: `1px solid ${theme.primary}88`,
                                background: `${theme.primary}14`,
                                color: theme.primary,
                                borderRadius: '999px',
                                cursor: 'pointer',
                                padding: '3px 9px',
                                fontSize: '11px',
                                fontFamily: 'monospace',
                                lineHeight: '18px',
                            }} title={`Insert: ${v.field}`}>
                                {formatDisplayLabel(v.field)}
                            </button>
                        ))}
                    </div>
                </div>
            )}
            {templateButtons.length > 0 && (
                <div style={{ marginBottom: '8px' }}>
                    <div style={{ fontSize: '9px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.6px', marginBottom: '4px' }}>Templates</div>
                    <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                        {templateButtons.map((template) => (
                            <button key={template.label} type="button" onClick={() => { setDraftFormula(template.value); commit(template.value); }} style={{ border: `1px solid ${theme.border}`, background: theme.background, color: theme.text, borderRadius: '999px', cursor: 'pointer', padding: '3px 9px', fontSize: '10px', lineHeight: '16px' }}>
                                {template.label}
                            </button>
                        ))}
                    </div>
                </div>
            )}
            <div style={{ display: 'flex', gap: '3px', marginBottom: '8px', flexWrap: 'wrap' }}>
                {['+', '-', '*', '/', '(', ')', '* 100'].map(op => (
                    <button key={op} type="button" onClick={() => insertAtCursor(op)} style={opBtnStyle}>{op}</button>
                ))}
            </div>
            <textarea
                ref={inputRef}
                key={item.field}
                value={draftFormula}
                onChange={(e) => {
                    const nextFormula = e.target.value;
                    setDraftFormula(nextFormula);
                    queueCommit(nextFormula);
                }}
                onKeyDown={(e) => {
                    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') commit(draftFormula);
                }}
                onBlur={() => commit(draftFormula)}
                placeholder={placeholder}
                rows={3}
                style={{
                    width: '100%', boxSizing: 'border-box',
                    resize: 'vertical',
                    fontFamily: 'monospace', fontSize: '12px',
                    lineHeight: 1.45,
                    padding: '8px 10px',
                    border: `1px solid ${theme.border}`,
                    borderRadius: '8px',
                    background: theme.background,
                    color: theme.text,
                    outline: 'none',
                }}
            />
            <div style={{ marginTop: '8px', padding: '6px 8px', borderRadius: '8px', border: `1px solid ${statusStyle.border}`, background: statusStyle.background, color: statusStyle.text, fontSize: '10px', lineHeight: 1.4 }}>
                {formulaStatus.text}
            </div>
            {dropLine && dropLine.zone === zoneId && dropLine.idx === idx + 1 && <div style={{ ...styles.dropLine, bottom: -2 }} />}
        </div>
    );
}

function buildFormulaTemplates(baseValues) {
    if (!Array.isArray(baseValues) || baseValues.length === 0) return [];
    if (baseValues.length === 1) {
        return [{ label: 'Scale x100', value: `${baseValues[0].field} * 100` }];
    }
    return [
        { label: 'Difference', value: `${baseValues[0].field} - ${baseValues[1].field}` },
        { label: 'Ratio %', value: `(${baseValues[0].field} / ${baseValues[1].field}) * 100` },
        { label: 'Margin %', value: `((${baseValues[0].field} - ${baseValues[1].field}) / ${baseValues[0].field}) * 100` },
    ];
}

function getFormulaStatusStyles(theme, tone) {
    const statusColors = {
        muted: {
            text: theme.textSec,
            border: theme.border,
            background: theme.headerSubtleBg || theme.background,
        },
        success: {
            text: theme.primary,
            border: `${theme.primary}33`,
            background: `${theme.primary}10`,
        },
        warning: {
            text: '#B45309',
            border: '#F59E0B55',
            background: '#F59E0B12',
        },
    };
    return statusColors[tone] || statusColors.muted;
}

function FormulaListItem({ item, idx, selected, onSelect, onRemove, theme, styles, dropLine, zoneId }) {
    const baseBorder = selected ? `${theme.primary}77` : theme.border;
    const background = selected ? (theme.headerBg || `${theme.primary}0f`) : (theme.background || 'transparent');
    const preview = String(item.formula || '').trim() || 'Empty formula';

    return (
        <div
            onClick={() => onSelect(item.field)}
            style={{
                ...styles.chip,
                border: `1px solid ${baseBorder}`,
                background,
                borderLeft: selected ? `3px solid ${theme.primary}` : `3px solid transparent`,
                boxShadow: 'none',
                cursor: 'pointer',
            }}
            title="Select formula"
        >
            {dropLine && dropLine.zone === zoneId && dropLine.idx === idx && <div style={{ ...styles.dropLine, top: -2 }} />}
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0, flex: 1 }}>
                <Icons.DragIndicator />
                <span style={{
                    fontSize: '10px',
                    fontWeight: 700,
                    color: theme.primary,
                    background: `${theme.primary}18`,
                    borderRadius: '999px',
                    padding: '3px 7px',
                    letterSpacing: '0.5px',
                    flexShrink: 0,
                }}>fx</span>
                <div style={{ minWidth: 0, flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                        <b style={{ fontWeight: 600, display: 'block', minWidth: 0, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                            {item.label || 'Formula'}
                        </b>
                        <span style={{ fontSize: '10px', color: theme.textSec, fontFamily: 'monospace', flexShrink: 0 }}>
                            {getFormulaReferenceKey(item)}
                        </span>
                    </div>
                    <div style={{
                        marginTop: '2px',
                        fontSize: '10px',
                        lineHeight: 1.35,
                        color: theme.textSec,
                        fontFamily: 'monospace',
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                    }}>
                        ={preview}
                    </div>
                </div>
            </div>
            <div style={{ display: 'flex', gap: '4px', marginLeft: '8px', alignItems: 'center', flexShrink: 0 }}>
                {selected && (
                    <span style={{
                        fontSize: '9px',
                        color: theme.primary,
                        background: `${theme.primary}14`,
                        borderRadius: '999px',
                        padding: '2px 6px',
                        flexShrink: 0,
                    }}>
                        Active
                    </span>
                )}
                <span
                    onClick={(event) => {
                        event.stopPropagation();
                        onRemove(idx);
                    }}
                    style={{ cursor: 'pointer', color: '#9CA3AF', display: 'flex', alignItems: 'center' }}
                >
                    <Icons.Close />
                </span>
            </div>
            {dropLine && dropLine.zone === zoneId && dropLine.idx === idx + 1 && <div style={{ ...styles.dropLine, bottom: -2 }} />}
        </div>
    );
}

function FormulaEditor({ item, idx, valConfigs, setValConfigs, theme, onRemove }) {
    const containerRef = React.useRef(null);
    const inputRef = React.useRef(null);
    const draftRef = React.useRef({ label: item.label || '', formula: item.formula || '' });
    const [draft, setDraft] = React.useState({ label: item.label || '', formula: item.formula || '' });
    const [expanded, setExpanded] = React.useState(false);
    const [templateValue, setTemplateValue] = React.useState('');

    React.useEffect(() => {
        const nextDraft = { label: item.label || '', formula: item.formula || '' };
        draftRef.current = nextDraft;
        setDraft(nextDraft);
        setTemplateValue('');
    }, [item.field, item.label, item.formula]);

    React.useEffect(() => {
        if (!inputRef.current) return;
        inputRef.current.focus();
        const cursor = inputRef.current.value.length;
        inputRef.current.setSelectionRange(cursor, cursor);
    }, [item.field]);

    const nonFormulaVals = React.useMemo(
        () => valConfigs.filter((config) => config && config.agg !== 'formula'),
        [valConfigs]
    );
    const formulaStatus = React.useMemo(
        () => inspectFormulaExpression(draft.formula, nonFormulaVals),
        [draft.formula, nonFormulaVals]
    );
    const statusStyle = React.useMemo(
        () => getFormulaStatusStyles(theme, formulaStatus.tone),
        [formulaStatus.tone, theme]
    );
    const placeholder = React.useMemo(
        () => buildSuggestedFormula(nonFormulaVals) || 'expression...',
        [nonFormulaVals]
    );
    const templateButtons = React.useMemo(
        () => buildFormulaTemplates(nonFormulaVals),
        [nonFormulaVals]
    );
    const hasPendingChanges = draft.label !== (item.label || '') || draft.formula !== (item.formula || '');

    const commitDraft = React.useCallback((nextDraft = draftRef.current) => {
        draftRef.current = nextDraft;
        updateValueConfig(setValConfigs, idx, (current) => ({ ...current, ...nextDraft }));
    }, [idx, setValConfigs]);

    const updateDraft = React.useCallback((patch, options = {}) => {
        setDraft((prev) => {
            const nextDraft = { ...prev, ...patch };
            draftRef.current = nextDraft;
            if (options.commitImmediately) {
                commitDraft(nextDraft);
            }
            return nextDraft;
        });
    }, [commitDraft]);

    const insertAtCursor = React.useCallback((text) => {
        const el = inputRef.current;
        if (!el) return;
        const formulaText = draftRef.current.formula || '';
        const start = el.selectionStart ?? formulaText.length;
        const end = el.selectionEnd ?? formulaText.length;
        const nextFormula = `${formulaText.slice(0, start)}${text}${formulaText.slice(end)}`;
        updateDraft({ formula: nextFormula });
        const nextCursor = start + text.length;
        setTimeout(() => {
            if (!inputRef.current) return;
            inputRef.current.focus();
            inputRef.current.setSelectionRange(nextCursor, nextCursor);
        }, 0);
    }, [updateDraft]);

    const resetDraft = React.useCallback(() => {
        const nextDraft = { label: item.label || '', formula: item.formula || '' };
        draftRef.current = nextDraft;
        setDraft(nextDraft);
        setTemplateValue('');
    }, [item.formula, item.label]);

    const handleEditorBlur = React.useCallback((event) => {
        const nextFocus = event.relatedTarget;
        if (nextFocus && containerRef.current && containerRef.current.contains(nextFocus)) return;
        if (hasPendingChanges) commitDraft();
    }, [commitDraft, hasPendingChanges]);

    const handleTemplateChange = React.useCallback((event) => {
        const nextValue = event.target.value;
        if (!nextValue) return;
        setTemplateValue('');
        updateDraft({ formula: nextValue });
        setTimeout(() => {
            if (!inputRef.current) return;
            inputRef.current.focus();
            const cursor = nextValue.length;
            inputRef.current.setSelectionRange(cursor, cursor);
        }, 0);
    }, [updateDraft]);

    const chipButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.background,
        color: theme.text,
        borderRadius: '999px',
        cursor: 'pointer',
        padding: '2px 8px',
        fontSize: '10px',
        fontFamily: 'monospace',
        lineHeight: '17px',
    };
    const opButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.background,
        color: theme.text,
        borderRadius: '6px',
        cursor: 'pointer',
        padding: '2px 7px',
        fontSize: '10px',
        fontFamily: 'monospace',
        lineHeight: '17px',
        flexShrink: 0,
    };
    const metaPillStyle = {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        padding: '3px 8px',
        borderRadius: '999px',
        border: `1px solid ${theme.border}`,
        background: theme.background,
        color: theme.textSec,
        fontSize: '10px',
        lineHeight: '16px',
    };

    return (
        <div
            ref={containerRef}
            onBlurCapture={handleEditorBlur}
            style={{
                position: 'sticky',
                top: '8px',
                zIndex: 2,
                margin: '0 0 10px 0',
                border: `1px solid ${theme.border}`,
                borderRadius: '10px',
                background: theme.headerSubtleBg || theme.background,
                boxShadow: theme.isDark ? 'none' : '0 6px 20px rgba(15, 23, 42, 0.07)',
                overflow: 'hidden',
            }}
        >
            <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '8px',
                padding: '8px 10px',
                borderBottom: `1px solid ${theme.border}`,
                background: theme.background,
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: 0 }}>
                    <span style={{
                        width: '22px',
                        height: '22px',
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        fontSize: '11px',
                        fontWeight: 700,
                        color: theme.primary,
                        background: `${theme.primary}18`,
                        borderRadius: '6px',
                        flexShrink: 0,
                    }}>fx</span>
                    <div style={{ minWidth: 0 }}>
                        <div style={{ fontSize: '11px', fontWeight: 700, color: theme.text }}>Formula Bar</div>
                        <div style={{ fontSize: '10px', color: theme.textSec }}>
                            Excel-style editing for the selected formula column.
                        </div>
                    </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexShrink: 0 }}>
                    <button
                        type="button"
                        onClick={() => setExpanded((prev) => !prev)}
                        style={{
                            border: `1px solid ${theme.border}`,
                            background: theme.background,
                            color: theme.textSec,
                            borderRadius: '6px',
                            cursor: 'pointer',
                            padding: '4px 8px',
                            fontSize: '10px',
                            lineHeight: '14px',
                        }}
                    >
                        {expanded ? 'Collapse' : 'Expand'}
                    </button>
                    <button
                        type="button"
                        onClick={() => onRemove(idx)}
                        style={{
                            border: `1px solid ${theme.border}`,
                            background: theme.background,
                            color: theme.textSec,
                            borderRadius: '6px',
                            cursor: 'pointer',
                            padding: '4px 8px',
                            fontSize: '10px',
                            lineHeight: '14px',
                        }}
                    >
                        Remove
                    </button>
                </div>
            </div>
            <div style={{ padding: '10px' }}>
                <div style={{ display: 'flex', gap: '8px', alignItems: expanded ? 'stretch' : 'flex-end', flexWrap: 'wrap', marginBottom: '8px' }}>
                    <label style={{ display: 'flex', flexDirection: 'column', gap: '4px', width: '118px', flexShrink: 0 }}>
                        <span style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Name</span>
                        <input
                            value={draft.label}
                            onChange={(event) => updateDraft({ label: event.target.value })}
                            placeholder="Formula column name"
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.background,
                                color: theme.text,
                                borderRadius: '6px',
                                padding: '7px 9px',
                                fontSize: '12px',
                                fontWeight: 600,
                                outline: 'none',
                            }}
                        />
                    </label>
                    <div style={{ minWidth: '0', flex: 1 }}>
                        <div style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '4px' }}>Formula</div>
                        <div style={{
                            display: 'flex',
                            alignItems: 'stretch',
                            border: `1px solid ${theme.border}`,
                            borderRadius: '6px',
                            background: theme.background,
                            overflow: 'hidden',
                        }}>
                            <div style={{
                                width: '34px',
                                flexShrink: 0,
                                display: 'flex',
                                alignItems: expanded ? 'flex-start' : 'center',
                                justifyContent: 'center',
                                paddingTop: expanded ? '9px' : 0,
                                borderRight: `1px solid ${theme.border}`,
                                color: theme.primary,
                                background: theme.headerSubtleBg || theme.background,
                                fontWeight: 700,
                                fontSize: '11px',
                            }}>
                                =
                            </div>
                            {expanded ? (
                                <textarea
                                    ref={inputRef}
                                    value={draft.formula}
                                    onChange={(event) => updateDraft({ formula: event.target.value })}
                                    onKeyDown={(event) => {
                                        if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') commitDraft();
                                    }}
                                    placeholder={placeholder}
                                    rows={4}
                                    style={{
                                        width: '100%',
                                        boxSizing: 'border-box',
                                        resize: 'vertical',
                                        border: 'none',
                                        background: 'transparent',
                                        color: theme.text,
                                        fontFamily: 'monospace',
                                        fontSize: '12px',
                                        lineHeight: 1.45,
                                        padding: '8px 10px',
                                        outline: 'none',
                                    }}
                                />
                            ) : (
                                <input
                                    ref={inputRef}
                                    value={draft.formula}
                                    onChange={(event) => updateDraft({ formula: event.target.value })}
                                    onKeyDown={(event) => {
                                        if (event.key === 'Enter') commitDraft();
                                    }}
                                    placeholder={placeholder}
                                    style={{
                                        width: '100%',
                                        boxSizing: 'border-box',
                                        border: 'none',
                                        background: 'transparent',
                                        color: theme.text,
                                        fontFamily: 'monospace',
                                        fontSize: '12px',
                                        lineHeight: 1.45,
                                        padding: '8px 10px',
                                        outline: 'none',
                                    }}
                                />
                            )}
                        </div>
                    </div>
                    <div style={{ display: 'flex', gap: '6px', alignItems: 'flex-end', flexShrink: 0 }}>
                        <button
                            type="button"
                            onClick={() => commitDraft()}
                            disabled={!hasPendingChanges}
                            style={{
                                border: `1px solid ${hasPendingChanges ? `${theme.primary}66` : theme.border}`,
                                background: hasPendingChanges ? `${theme.primary}12` : theme.background,
                                color: hasPendingChanges ? theme.primary : theme.textSec,
                                borderRadius: '6px',
                                cursor: hasPendingChanges ? 'pointer' : 'default',
                                padding: '7px 10px',
                                fontSize: '10px',
                                fontWeight: 700,
                                lineHeight: '14px',
                                opacity: hasPendingChanges ? 1 : 0.7,
                            }}
                        >
                            Apply
                        </button>
                        <button
                            type="button"
                            onClick={resetDraft}
                            disabled={!hasPendingChanges}
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.background,
                                color: theme.textSec,
                                borderRadius: '6px',
                                cursor: hasPendingChanges ? 'pointer' : 'default',
                                padding: '7px 10px',
                                fontSize: '10px',
                                lineHeight: '14px',
                                opacity: hasPendingChanges ? 1 : 0.7,
                            }}
                        >
                            Reset
                        </button>
                    </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', flexWrap: 'wrap', marginBottom: '8px' }}>
                    <div style={{ display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' }}>
                        <span style={metaPillStyle}>
                            ID <code style={{ fontFamily: 'monospace', color: theme.text }}>{item.field}</code>
                        </span>
                        <span style={metaPillStyle}>
                            {hasPendingChanges ? 'Draft changes' : 'Saved'}
                        </span>
                    </div>
                    <div style={{
                        padding: '4px 8px',
                        borderRadius: '999px',
                        border: `1px solid ${statusStyle.border}`,
                        background: statusStyle.background,
                        color: statusStyle.text,
                        fontSize: '10px',
                        lineHeight: '14px',
                    }}>
                        {formulaStatus.text}
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap', marginBottom: '8px' }}>
                    {templateButtons.length > 0 && (
                        <select
                            value={templateValue}
                            onChange={handleTemplateChange}
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.background,
                                color: theme.text,
                                borderRadius: '6px',
                                padding: '4px 8px',
                                fontSize: '10px',
                                lineHeight: '16px',
                                outline: 'none',
                            }}
                            title="Formula templates"
                        >
                            <option value="">Templates</option>
                            {templateButtons.map((template) => (
                                <option key={template.label} value={template.value}>{template.label}</option>
                            ))}
                        </select>
                    )}
                    <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                        {['+', '-', '*', '/', '(', ')', '* 100'].map((op) => (
                            <button
                                key={op}
                                type="button"
                                onMouseDown={(event) => event.preventDefault()}
                                onClick={() => insertAtCursor(op)}
                                style={opButtonStyle}
                            >
                                {op}
                            </button>
                        ))}
                    </div>
                    <div style={{ fontSize: '10px', color: theme.textSec }}>
                        {expanded ? 'Ctrl/Cmd + Enter to apply' : 'Enter to apply'}
                    </div>
                </div>
                {nonFormulaVals.length > 0 && (
                    <div>
                        <div style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '4px' }}>
                            Insert measure
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                            {nonFormulaVals.map((valueConfig) => (
                                <button
                                    key={valueConfig.field}
                                    type="button"
                                    onMouseDown={(event) => event.preventDefault()}
                                    onClick={() => insertAtCursor(valueConfig.field)}
                                    style={chipButtonStyle}
                                    title={`Insert: ${valueConfig.field}`}
                                >
                                    {formatDisplayLabel(valueConfig.field)}
                                </button>
                            ))}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

function FormulaEditorModal({ item, idx, valConfigs, setValConfigs, theme, onClose, onRemove }) {
    const inputRef = React.useRef(null);
    const [draft, setDraft] = React.useState({
        label: item.label || '',
        formula: item.formula || '',
        formulaRef: getFormulaReferenceKey(item),
    });

    React.useEffect(() => {
        setDraft({
            label: item.label || '',
            formula: item.formula || '',
            formulaRef: getFormulaReferenceKey(item),
        });
    }, [item.field, item.formula, item.formulaRef, item.label]);

    React.useEffect(() => {
        if (!inputRef.current) return;
        inputRef.current.focus();
        const cursor = inputRef.current.value.length;
        inputRef.current.setSelectionRange(cursor, cursor);
    }, [item.field]);

    const referenceConfigs = React.useMemo(
        () => valConfigs.filter((config) => config && config.field),
        [valConfigs]
    );
    const measureReferences = React.useMemo(
        () => referenceConfigs.filter((config) => config.agg !== 'formula'),
        [referenceConfigs]
    );
    const formulaReferences = React.useMemo(
        () => referenceConfigs.filter((config) => config.agg === 'formula' && config.field !== item.field),
        [item.field, referenceConfigs]
    );
    const formulaStatus = React.useMemo(
        () => inspectFormulaExpression(draft.formula, referenceConfigs, item.field, draft.formulaRef),
        [draft.formula, draft.formulaRef, item.field, referenceConfigs]
    );
    const statusStyle = React.useMemo(
        () => getFormulaStatusStyles(theme, formulaStatus.tone),
        [formulaStatus.tone, theme]
    );
    const templateButtons = React.useMemo(
        () => buildFormulaTemplates(measureReferences),
        [measureReferences]
    );
    const placeholder = React.useMemo(
        () => buildSuggestedFormula(measureReferences) || `${draft.formulaRef || item.field} * 100`,
        [draft.formulaRef, item.field, measureReferences]
    );
    const hasPendingChanges = draft.label !== (item.label || '')
        || draft.formula !== (item.formula || '')
        || draft.formulaRef !== getFormulaReferenceKey(item);

    const applyDraft = React.useCallback(() => {
        const nextFormulaRef = normalizeFormulaReferenceKey(draft.formulaRef, item.field);
        setValConfigs((prev) => {
            const next = [...prev];
            const current = next[idx];
            if (!current) return prev;
            const oldFormulaRef = getFormulaReferenceKey(current);
            let uniqueFormulaRef = nextFormulaRef;
            const taken = new Set(
                next
                    .filter((config, configIndex) => config && configIndex !== idx)
                    .map((config) => (config.agg === 'formula' ? getFormulaReferenceKey(config) : config.field))
                    .filter(Boolean)
                    .map((token) => String(token).toLowerCase())
            );
            if (taken.has(uniqueFormulaRef.toLowerCase())) {
                let suffix = 2;
                let candidate = `${uniqueFormulaRef}${suffix}`;
                while (taken.has(candidate.toLowerCase())) {
                    suffix += 1;
                    candidate = `${uniqueFormulaRef}${suffix}`;
                }
                uniqueFormulaRef = candidate;
            }

            next[idx] = {
                ...current,
                label: draft.label,
                formula: draft.formula,
                formulaRef: uniqueFormulaRef,
            };

            if (oldFormulaRef && oldFormulaRef !== uniqueFormulaRef) {
                for (let configIndex = 0; configIndex < next.length; configIndex += 1) {
                    if (configIndex === idx) continue;
                    const config = next[configIndex];
                    if (!config || config.agg !== 'formula' || !config.formula) continue;
                    next[configIndex] = {
                        ...config,
                        formula: replaceIdentifierToken(config.formula, oldFormulaRef, uniqueFormulaRef),
                    };
                }
            }
            return next;
        });
    }, [draft.formula, draft.formulaRef, draft.label, idx, item.field, setValConfigs]);

    const closeModal = React.useCallback(() => {
        setDraft({
            label: item.label || '',
            formula: item.formula || '',
            formulaRef: getFormulaReferenceKey(item),
        });
        onClose();
    }, [item.field, item.formula, item.formulaRef, item.label, onClose]);

    const insertAtCursor = React.useCallback((text) => {
        const el = inputRef.current;
        if (!el) return;
        const formulaText = draft.formula || '';
        const start = el.selectionStart ?? formulaText.length;
        const end = el.selectionEnd ?? formulaText.length;
        const nextFormula = `${formulaText.slice(0, start)}${text}${formulaText.slice(end)}`;
        setDraft((prev) => ({ ...prev, formula: nextFormula }));
        const nextCursor = start + text.length;
        setTimeout(() => {
            if (!inputRef.current) return;
            inputRef.current.focus();
            inputRef.current.setSelectionRange(nextCursor, nextCursor);
        }, 0);
    }, [draft.formula]);

    const applyAndClose = React.useCallback(() => {
        applyDraft();
        onClose();
    }, [applyDraft, onClose]);

    const tokenButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.background,
        color: theme.text,
        borderRadius: '999px',
        cursor: 'pointer',
        padding: '3px 9px',
        fontSize: '10px',
        lineHeight: '16px',
    };
    const operatorButtonStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.background,
        color: theme.text,
        borderRadius: '6px',
        cursor: 'pointer',
        padding: '3px 8px',
        fontSize: '10px',
        lineHeight: '16px',
        fontFamily: 'monospace',
    };

    return (
        <div
            onClick={closeModal}
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 10006,
                background: 'rgba(2, 6, 23, 0.45)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '24px',
            }}
        >
            <div
                onClick={(event) => event.stopPropagation()}
                onKeyDown={(event) => {
                    if (event.key === 'Escape') closeModal();
                    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') applyAndClose();
                }}
                style={{
                    width: 'min(760px, 92vw)',
                    maxHeight: '80vh',
                    overflowY: 'auto',
                    background: theme.surfaceBg || theme.background || '#fff',
                    border: `1px solid ${theme.border}`,
                    borderRadius: theme.radius || '16px',
                    boxShadow: theme.shadowMd || '0 18px 40px rgba(0,0,0,0.28)',
                    padding: '18px',
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px', marginBottom: '14px' }}>
                    <div>
                        <div style={{ fontSize: '15px', fontWeight: 800, color: theme.text }}>Formula Editor</div>
                        <div style={{ fontSize: '11px', color: theme.textSec }}>
                            Use a stable reference key for formulas. Renaming the key updates dependent formulas.
                        </div>
                    </div>
                    <button
                        type="button"
                        onClick={closeModal}
                        style={{
                            border: `1px solid ${theme.border}`,
                            background: theme.headerSubtleBg || theme.background,
                            color: theme.text,
                            borderRadius: '8px',
                            padding: '7px 10px',
                            fontSize: '11px',
                            fontWeight: 700,
                            cursor: 'pointer',
                        }}
                    >
                        Close
                    </button>
                </div>

                <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', marginBottom: '12px' }}>
                    <label style={{ display: 'flex', flexDirection: 'column', gap: '4px', flex: 1, minWidth: '180px' }}>
                        <span style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Label</span>
                        <input
                            value={draft.label}
                            onChange={(event) => setDraft((prev) => ({ ...prev, label: event.target.value }))}
                            placeholder="Formula label"
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.background,
                                color: theme.text,
                                borderRadius: '8px',
                                padding: '8px 10px',
                                fontSize: '12px',
                                fontWeight: 600,
                                outline: 'none',
                            }}
                        />
                    </label>
                    <label style={{ minWidth: '180px', flex: '0 0 auto', display: 'flex', flexDirection: 'column', gap: '4px' }}>
                        <span style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Reference key</span>
                        <input
                            value={draft.formulaRef}
                            onChange={(event) => setDraft((prev) => ({ ...prev, formulaRef: normalizeFormulaReferenceKey(event.target.value, item.field) }))}
                            placeholder="formula1"
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.headerSubtleBg || theme.background,
                                color: theme.text,
                                borderRadius: '8px',
                                padding: '8px 10px',
                                fontSize: '12px',
                                fontFamily: 'monospace',
                                outline: 'none',
                            }}
                        />
                    </label>
                </div>

                <div style={{ marginBottom: '12px' }}>
                    <div style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '4px' }}>Formula</div>
                    <textarea
                        ref={inputRef}
                        value={draft.formula}
                        onChange={(event) => setDraft((prev) => ({ ...prev, formula: event.target.value }))}
                        placeholder={placeholder}
                        rows={4}
                        style={{
                            width: '100%',
                            boxSizing: 'border-box',
                            resize: 'vertical',
                            border: `1px solid ${theme.border}`,
                            background: theme.background,
                            color: theme.text,
                            borderRadius: '10px',
                            fontFamily: 'monospace',
                            fontSize: '12px',
                            lineHeight: 1.5,
                            padding: '10px 12px',
                            outline: 'none',
                        }}
                    />
                </div>

                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', flexWrap: 'wrap', marginBottom: '12px' }}>
                    <div style={{
                        padding: '5px 9px',
                        borderRadius: '999px',
                        border: `1px solid ${statusStyle.border}`,
                        background: statusStyle.background,
                        color: statusStyle.text,
                        fontSize: '10px',
                        lineHeight: '14px',
                    }}>
                        {formulaStatus.text}
                    </div>
                    <div style={{ fontSize: '10px', color: theme.textSec }}>
                        Ctrl/Cmd + Enter applies
                    </div>
                </div>

                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginBottom: '12px' }}>
                    {templateButtons.map((template) => (
                        <button
                            key={template.label}
                            type="button"
                            onClick={() => setDraft((prev) => ({ ...prev, formula: template.value }))}
                            style={tokenButtonStyle}
                        >
                            {template.label}
                        </button>
                    ))}
                    {['+', '-', '*', '/', '(', ')', '* 100'].map((op) => (
                        <button
                            key={op}
                            type="button"
                            onMouseDown={(event) => event.preventDefault()}
                            onClick={() => insertAtCursor(op)}
                            style={operatorButtonStyle}
                        >
                            {op}
                        </button>
                    ))}
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: '12px', marginBottom: '14px' }}>
                    <div>
                        <div style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '6px' }}>Measures</div>
                        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                            {measureReferences.map((config) => (
                                <button
                                    key={config.field}
                                    type="button"
                                    onMouseDown={(event) => event.preventDefault()}
                                    onClick={() => insertAtCursor(config.field)}
                                    style={tokenButtonStyle}
                                    title={`Insert ${config.field}`}
                                >
                                    {config.field}
                                </button>
                            ))}
                        </div>
                    </div>
                    {formulaReferences.length > 0 && (
                        <div>
                            <div style={{ fontSize: '10px', color: theme.textSec, textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: '6px' }}>Formula References</div>
                            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                                {formulaReferences.map((config) => (
                                    <button
                                        key={config.field}
                                        type="button"
                                        onMouseDown={(event) => event.preventDefault()}
                                        onClick={() => insertAtCursor(getFormulaReferenceKey(config))}
                                        style={tokenButtonStyle}
                                        title={`${getFormulaReferenceKey(config)}${config.label ? ` (${config.label})` : ''}`}
                                    >
                                        {getFormulaReferenceKey(config)}
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', flexWrap: 'wrap' }}>
                    <button
                        type="button"
                        onClick={() => {
                            onRemove(idx);
                            onClose();
                        }}
                        style={{
                            border: `1px solid ${theme.border}`,
                            background: theme.background,
                            color: theme.textSec,
                            borderRadius: '8px',
                            padding: '8px 12px',
                            fontSize: '11px',
                            fontWeight: 700,
                            cursor: 'pointer',
                        }}
                    >
                        Delete Formula
                    </button>
                    <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                        <button
                            type="button"
                            onClick={closeModal}
                            style={{
                                border: `1px solid ${theme.border}`,
                                background: theme.background,
                                color: theme.textSec,
                                borderRadius: '8px',
                                padding: '8px 12px',
                                fontSize: '11px',
                                fontWeight: 700,
                                cursor: 'pointer',
                            }}
                        >
                            Cancel
                        </button>
                        <button
                            type="button"
                            onClick={applyAndClose}
                            disabled={!hasPendingChanges}
                            style={{
                                border: `1px solid ${hasPendingChanges ? `${theme.primary}66` : theme.border}`,
                                background: hasPendingChanges ? `${theme.primary}14` : theme.background,
                                color: hasPendingChanges ? theme.primary : theme.textSec,
                                borderRadius: '8px',
                                padding: '8px 12px',
                                fontSize: '11px',
                                fontWeight: 700,
                                cursor: hasPendingChanges ? 'pointer' : 'default',
                                opacity: hasPendingChanges ? 1 : 0.7,
                            }}
                        >
                            Apply
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}

function ResizableFieldPanel({
    panelId,
    panelLabel,
    size,
    onSizeChange,
    theme,
    outerStyle = {},
    contentStyle = {},
    children,
}) {
    const panelRef = React.useRef(null);
    const cleanupRef = React.useRef(null);
    const limits = React.useMemo(() => getFieldPanelLimits(panelId), [panelId]);
    const resolvedSize = React.useMemo(
        () => sanitizeFieldPanelSizeEntry(panelId, size),
        [panelId, size]
    );

    const stopActiveResize = React.useCallback(() => {
        if (cleanupRef.current) {
            const cleanup = cleanupRef.current;
            cleanupRef.current = null;
            cleanup();
        }
    }, []);

    React.useEffect(() => (
        () => stopActiveResize()
    ), [stopActiveResize]);

    const startResize = React.useCallback((event, direction) => {
        if (!panelRef.current || typeof onSizeChange !== 'function') return;
        event.preventDefault();
        event.stopPropagation();
        stopActiveResize();

        const rect = panelRef.current.getBoundingClientRect();
        const parentRect = panelRef.current.parentElement
            ? panelRef.current.parentElement.getBoundingClientRect()
            : rect;
        const startState = {
            startX: event.clientX,
            startY: event.clientY,
            startWidth: rect.width,
            startHeight: rect.height,
            maxWidth: Math.max(limits.minWidth, Math.min(limits.maxWidth, Math.floor(parentRect.width))),
        };
        const previousCursor = document.body.style.cursor;
        const previousUserSelect = document.body.style.userSelect;
        document.body.style.cursor = direction === 'both'
            ? 'nwse-resize'
            : (direction === 'width' ? 'ew-resize' : 'ns-resize');
        document.body.style.userSelect = 'none';

        const onMouseMove = (moveEvent) => {
            const deltaX = moveEvent.clientX - startState.startX;
            const deltaY = moveEvent.clientY - startState.startY;
            const nextWidth = direction === 'height'
                ? resolvedSize.width
                : Math.max(limits.minWidth, Math.min(startState.maxWidth, Math.round(startState.startWidth + deltaX)));
            const nextHeight = direction === 'width'
                ? resolvedSize.height
                : Math.max(limits.minHeight, Math.min(limits.maxHeight, Math.round(startState.startHeight + deltaY)));
            onSizeChange(panelId, { width: nextWidth, height: nextHeight });
        };

        const cleanup = () => {
            document.body.style.cursor = previousCursor;
            document.body.style.userSelect = previousUserSelect;
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
        };

        const onMouseUp = () => stopActiveResize();
        cleanupRef.current = cleanup;
        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    }, [
        limits.maxHeight,
        limits.maxWidth,
        limits.minHeight,
        limits.minWidth,
        onSizeChange,
        panelId,
        resolvedSize.height,
        resolvedSize.width,
        stopActiveResize,
    ]);

    const handleColor = `${theme.primary}55`;

    return (
        <div
            ref={panelRef}
            style={{
                position: 'relative',
                width: resolvedSize.width === null ? '100%' : `${resolvedSize.width}px`,
                maxWidth: '100%',
                height: `${resolvedSize.height}px`,
                minHeight: `${limits.minHeight}px`,
                maxHeight: `${limits.maxHeight}px`,
                boxSizing: 'border-box',
                overflow: 'hidden',
                ...outerStyle,
            }}
        >
            <div
                style={{
                    width: '100%',
                    height: '100%',
                    overflow: 'auto',
                    boxSizing: 'border-box',
                    ...contentStyle,
                }}
            >
                {children}
            </div>
            <div
                onMouseDown={(event) => startResize(event, 'width')}
                style={{
                    position: 'absolute',
                    top: 0,
                    right: 0,
                    bottom: '12px',
                    width: '12px',
                    cursor: 'ew-resize',
                    zIndex: 4,
                    touchAction: 'none',
                }}
                title={`Resize ${panelLabel} width`}
            >
            </div>
            <div
                onMouseDown={(event) => startResize(event, 'height')}
                style={{
                    position: 'absolute',
                    left: 0,
                    right: '12px',
                    bottom: 0,
                    height: '12px',
                    cursor: 'ns-resize',
                    zIndex: 4,
                    touchAction: 'none',
                }}
                title={`Resize ${panelLabel} height`}
            >
            </div>
            <div
                onMouseDown={(event) => startResize(event, 'both')}
                style={{
                    position: 'absolute',
                    right: 0,
                    bottom: 0,
                    width: '14px',
                    height: '14px',
                    cursor: 'nwse-resize',
                    zIndex: 5,
                    touchAction: 'none',
                }}
                title={`Resize ${panelLabel}`}
            >
                <div
                    style={{
                        position: 'absolute',
                        right: '4px',
                        bottom: '4px',
                        width: '3px',
                        height: '3px',
                        borderRadius: '999px',
                        background: handleColor,
                        boxShadow: `-4px 0 0 ${handleColor}, 0 -4px 0 ${handleColor}, -4px -4px 0 ${handleColor}`,
                    }}
                />
            </div>
        </div>
    );
}

export function SidebarPanel({
    sidebarTab, setSidebarTab,
    rowFields, setRowFields,
    colFields, setColFields,
    valConfigs, setValConfigs,
    filters, setFilters,
    columnVisibility, setColumnVisibility,
    columnPinning, setColumnPinning,
    availableFields,
    table,
    pinningPresets,
    theme, styles,
    showNotification,
    filterAnchorEl, setFilterAnchorEl,
    colSearch, setColSearch,
    colTypeFilter, setColTypeFilter,
    selectedCols, setSelectedCols,
    dropLine, onDragStart, onDragOver, onDrop,
    handleHeaderFilter, handleFilterClick, requestFilterOptions,
    handleExpandAllRows, handlePinColumn,
    toggleAllColumnsPinned,
    activeFilterCol, closeFilterPopover, filterOptions,
    data,
    sidebarWidth, setSidebarWidth,
    fieldPanelSizes, setFieldPanelSizes,
    pivotMode = 'pivot',
    reportDef,
    setReportDef,
    savedReports = [],
    setSavedReports,
    activeReportId,
    setActiveReportId,
}) {
    const [sidebarFilterState, setSidebarFilterState] = React.useState({ columnId: null, anchorEl: null });
    const [activeFormulaField, setActiveFormulaField] = React.useState(null);
    const [formulaModalField, setFormulaModalField] = React.useState(null);
    const sidebarRef = React.useRef(null);
    const sidebarScrollTopRef = React.useRef(0);
    const resizeDragRef = React.useRef(null);
    const formulaConfigs = React.useMemo(
        () => valConfigs.filter((config) => config && config.agg === 'formula'),
        [valConfigs]
    );
    const formulaCount = React.useMemo(
        () => formulaConfigs.length,
        [formulaConfigs]
    );
    const regularValueCount = React.useMemo(
        () => valConfigs.filter((config) => config && config.agg !== 'formula').length,
        [valConfigs]
    );

    // Sidebar resize drag handlers
    const onResizeMouseDown = React.useCallback((e) => {
        e.preventDefault();
        const startX = e.clientX;
        const startWidth = sidebarWidth || 288;
        resizeDragRef.current = { startX, startWidth };
        const onMouseMove = (ev) => {
            const delta = ev.clientX - resizeDragRef.current.startX;
            const next = Math.max(200, Math.min(520, resizeDragRef.current.startWidth + delta));
            setSidebarWidth(next);
        };
        const onMouseUp = () => {
            resizeDragRef.current = null;
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
        };
        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    }, [sidebarWidth, setSidebarWidth]);
    const handleFieldPanelSizeChange = React.useCallback((panelId, nextSize) => {
        if (typeof setFieldPanelSizes !== 'function') return;
        setFieldPanelSizes((prev) => {
            const next = mergeFieldPanelSize(prev, panelId, nextSize);
            const previousEntry = prev && prev[panelId] ? prev[panelId] : null;
            const nextEntry = next[panelId];
            if (
                previousEntry
                && previousEntry.width === nextEntry.width
                && previousEntry.height === nextEntry.height
            ) {
                return prev;
            }
            return next;
        });
    }, [setFieldPanelSizes]);
    const dropZonePadding = (styles.dropZone && styles.dropZone.padding) || '8px';
    const dropZoneBaseStyle = React.useMemo(() => {
        const baseStyle = { ...(styles.dropZone || {}) };
        delete baseStyle.minHeight;
        delete baseStyle.maxHeight;
        delete baseStyle.overflowY;
        delete baseStyle.padding;
        delete baseStyle.border;
        delete baseStyle.boxShadow;
        return baseStyle;
    }, [styles.dropZone]);
    const getFilterOptionsForColumn = React.useCallback((columnId) => {
        if (filterOptions && filterOptions[columnId]) return filterOptions[columnId];
        if (!table || !table.getColumn) return [];
        const col = table.getColumn(columnId);
        if (!col) return [];

        const unique = new Set();
        const rows = table.getCoreRowModel ? table.getCoreRowModel().rows : [];
        rows.forEach((row) => {
            const val = row.getValue(columnId);
            if (val !== null && val !== undefined && val !== '') unique.add(val);
        });

        return Array.from(unique).sort();
    }, [filterOptions, table]);
    const valueSelectStyle = {
        border: `1px solid ${theme.border}`,
        background: theme.headerSubtleBg || theme.surfaceInset || theme.background,
        color: theme.text,
        cursor: 'pointer',
        borderRadius: theme.radiusSm || '8px',
        fontSize: '9px',
        lineHeight: 1.2,
        padding: '1px 4px',
        minHeight: '20px',
        outline: 'none',
    };
    const handleValueAggChange = React.useCallback((measureIndex, nextAgg) => {
        setValConfigs((prev) => {
            const next = [...prev];
            const current = next[measureIndex];
            if (!current) return prev;

            if (isWeightedAverageAgg(nextAgg) && !current.weightField) {
                const suggestedWeightField = Array.isArray(availableFields) && availableFields.length > 0
                    ? (availableFields.find((field) => field !== current.field) || availableFields[0])
                    : '';
                const promptValue = typeof window !== 'undefined'
                    ? window.prompt(
                        `Weight field for weighted average of ${formatDisplayLabel(current.field)}`,
                        current.weightField || suggestedWeightField || ''
                    )
                    : current.weightField || suggestedWeightField || '';
                const chosenWeightField = typeof promptValue === 'string' ? promptValue.trim() : '';

                if (!chosenWeightField) {
                    if (showNotification) showNotification('Weighted average requires a weight field.', 'warning');
                    return prev;
                }
                if (Array.isArray(availableFields) && availableFields.length > 0 && !availableFields.includes(chosenWeightField)) {
                    if (showNotification) showNotification(`Unknown weight field: ${chosenWeightField}`, 'warning');
                    return prev;
                }

                next[measureIndex] = { ...current, agg: nextAgg, weightField: chosenWeightField };
                return next;
            }

            next[measureIndex] = { ...current, agg: nextAgg };
            return next;
        });
    }, [availableFields, setValConfigs, showNotification]);
    const handleValueWeightFieldChange = React.useCallback((measureIndex, nextWeightField) => {
        if (!nextWeightField) {
            if (showNotification) showNotification('Weighted average requires a weight field.', 'warning');
            return;
        }
        setValConfigs((prev) => {
            const next = [...prev];
            const current = next[measureIndex];
            if (!current) return prev;
            next[measureIndex] = { ...current, weightField: nextWeightField };
            return next;
        });
    }, [setValConfigs, showNotification]);
    const handleAddFormulaValue = React.useCallback(() => {
        const nextFieldId = createFormulaFieldId(valConfigs);
        const baseValues = valConfigs.filter((config) => config && config.agg !== 'formula');
        const nextLabel = `Formula ${formulaCount + 1}`;
        const nextFormula = buildSuggestedFormula(baseValues);
        const nextFormulaRef = buildFormulaReferenceKey(nextLabel, valConfigs, nextFieldId);
        setActiveFormulaField(nextFieldId);
        setFormulaModalField(nextFieldId);
        setValConfigs((prev) => [
            ...prev,
            {
                field: nextFieldId,
                agg: 'formula',
                label: nextLabel,
                formula: nextFormula,
                formulaRef: nextFormulaRef,
            },
        ]);
        if (showNotification) {
            if (baseValues.length === 0) {
                showNotification('Added a formula column. Add at least one regular value measure to reference in it.', 'warning');
            } else {
                showNotification(`Added ${nextLabel}.`, 'success');
            }
        }
    }, [formulaCount, setFormulaModalField, setValConfigs, setActiveFormulaField, showNotification, valConfigs]);
    const openSidebarFilter = (e, columnId) => {
        e.stopPropagation();
        if (sidebarRef.current) {
            sidebarScrollTopRef.current = sidebarRef.current.scrollTop;
        }
        setSidebarFilterState((prev) => (
            prev.columnId === columnId
                ? { columnId: null, anchorEl: null }
                : { columnId, anchorEl: e.currentTarget }
        ));
        if (requestFilterOptions) {
            requestFilterOptions(columnId);
        }
    };
    const closeSidebarFilter = () => setSidebarFilterState({ columnId: null, anchorEl: null });

    React.useEffect(() => {
        if (!sidebarRef.current) return;
        if (!sidebarFilterState.columnId) return;
        sidebarRef.current.scrollTop = sidebarScrollTopRef.current;
    }, [filterOptions, sidebarFilterState.columnId]);

    React.useEffect(() => {
        if (formulaConfigs.length === 0) {
            if (activeFormulaField !== null) setActiveFormulaField(null);
            if (formulaModalField !== null) setFormulaModalField(null);
            return;
        }
        const hasActiveFormula = formulaConfigs.some((config) => config.field === activeFormulaField);
        if (!hasActiveFormula) {
            setActiveFormulaField(formulaConfigs[formulaConfigs.length - 1].field);
        }
        if (formulaModalField && !formulaConfigs.some((config) => config.field === formulaModalField)) {
            setFormulaModalField(null);
        }
    }, [activeFormulaField, formulaConfigs, formulaModalField]);

    const formulaModalIndex = React.useMemo(
        () => valConfigs.findIndex((config) => config && config.agg === 'formula' && config.field === formulaModalField),
        [formulaModalField, valConfigs]
    );
    const formulaModalConfig = formulaModalIndex >= 0 ? valConfigs[formulaModalIndex] : null;
    const openFormulaModal = React.useCallback((field) => {
        setActiveFormulaField(field);
        setFormulaModalField(field);
    }, []);
    const removeValueAtIndex = React.useCallback((removeIndex) => {
        const removingFormulaField = valConfigs[removeIndex] && valConfigs[removeIndex].agg === 'formula'
            ? valConfigs[removeIndex].field
            : null;
        setValConfigs((prev) => prev.filter((_, index) => index !== removeIndex));
        if (removingFormulaField && removingFormulaField === activeFormulaField) {
            const remainingFormula = formulaConfigs.find((config) => config.field !== removingFormulaField);
            setActiveFormulaField(remainingFormula ? remainingFormula.field : null);
        }
        if (removingFormulaField && removingFormulaField === formulaModalField) {
            setFormulaModalField(null);
        }
    }, [activeFormulaField, formulaConfigs, formulaModalField, setValConfigs, valConfigs]);

    const w = sidebarWidth || 288;
    const isReportMode = pivotMode === 'report';
    return (
        <div style={{position:'relative', display:'flex', flexShrink:0}}>
                <div ref={sidebarRef} style={{...styles.sidebar, width:`${w}px`, minWidth:`${w}px`}} role="complementary" aria-label="Tool Panel">
                    {isReportMode ? (
                        <div style={{display: 'flex', borderBottom: `1px solid ${theme.border}`, marginBottom: '16px'}}>
                            <div
                                style={{
                                    padding: '8px 16px', cursor: 'default',
                                    borderBottom: `2px solid ${theme.primary}`,
                                    fontWeight: 600,
                                    color: theme.primary,
                                    display: 'flex', alignItems: 'center', gap: '6px',
                                }}
                            >
                                <Icons.Report /> Report Builder
                            </div>
                        </div>
                    ) : (
                    <div style={{display: 'flex', borderBottom: `1px solid ${theme.border}`, marginBottom: '16px'}}>
                        <div
                            onClick={() => setSidebarTab('fields')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer',
                                borderBottom: sidebarTab === 'fields' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'fields' ? 600 : 400,
                                color: sidebarTab === 'fields' ? theme.primary : theme.textSec
                            }}
                        >Fields</div>
                        <div
                            onClick={() => setSidebarTab('filters')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer',
                                borderBottom: sidebarTab === 'filters' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'filters' ? 600 : 400,
                                color: sidebarTab === 'filters' ? theme.primary : theme.textSec,
                                display: 'flex', alignItems: 'center', gap: '6px'
                            }}
                        >
                            Filters
                            {Object.keys(filters).length > 0 && (
                                <div style={{width: '6px', height: '6px', borderRadius: '50%', background: '#d32f2f'}} />
                            )}
                        </div>
                        <div
                            onClick={() => setSidebarTab('columns')}
                            style={{
                                padding: '8px 16px', cursor: 'pointer',
                                borderBottom: sidebarTab === 'columns' ? `2px solid ${theme.primary}` : 'none',
                                fontWeight: sidebarTab === 'columns' ? 600 : 400,
                                color: sidebarTab === 'columns' ? theme.primary : theme.textSec
                            }}
                        >Columns</div>
                    </div>
                    )}

                    {isReportMode ? (
                        <div style={{flex: 1, overflowY: 'auto', padding: '0 12px 12px'}}>
                            <ReportEditor
                                reportDef={reportDef}
                                setReportDef={setReportDef}
                                availableFields={availableFields}
                                theme={theme}
                                styles={styles}
                                data={data}
                                valConfigs={valConfigs}
                                savedReports={savedReports}
                                setSavedReports={setSavedReports}
                                activeReportId={activeReportId}
                                setActiveReportId={setActiveReportId}
                                showNotification={showNotification}
                            />
                        </div>
                    ) : sidebarTab === 'filters' ? (
                        <div style={{flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', padding: '8px'}}>
                            <div style={{marginBottom: '10px', display: 'flex', alignItems: 'center', background: theme.background, borderRadius: '6px', padding: '4px 8px', border: `1px solid ${theme.border}`}}>
                                <Icons.Search />
                                <input 
                                    placeholder="Search columns..."
                                    value={colSearch} 
                                    onChange={e => setColSearch(e.target.value)} 
                                    style={{border:'none', background:'transparent', marginLeft:'10px', outline:'none', width:'100%', color: theme.text, fontSize: '13px'}}
                                />
                            </div>
                            {(() => {
                                const allFields = availableFields;
                                const colsForDisplay = allFields.map(field => {
                                    const tableCol = table.getColumn(field);
                                    if (tableCol) return tableCol;
                                    return { id: field, header: formatDisplayLabel(field), columnDef: { header: formatDisplayLabel(field) } };
                                });
                                const filtered = colsForDisplay.filter(col => {
                                    const header = (col.columnDef && typeof col.columnDef.header === 'string') ? col.columnDef.header : (typeof col.header === 'string' ? col.header : col.id);
                                    return String(header).toLowerCase().includes(colSearch.toLowerCase()) || col.id.toLowerCase().includes(colSearch.toLowerCase());
                                });
                                return (
                                    <div style={{display: 'flex', flexDirection: 'column'}}>
                                        {filtered.map(col => (
                                            <SidebarFilterItem
                                                key={col.id}
                                                column={col}
                                                theme={theme}
                                                styles={styles}
                                                onFilter={(val) => handleHeaderFilter(col.id, val)}
                                                currentFilter={filters[col.id]}
                                                options={getFilterOptionsForColumn(col.id)}
                                                onOpen={requestFilterOptions}
                                            />
                                        ))}
                                    </div>
                                );
                            })()}
                        </div>
                    ) : sidebarTab !== 'columns' ? (
                        <>
                            {sidebarTab === 'fields' && (
                                <div>
                                    <div style={styles.sectionTitleSm}><Icons.Database/> Available Fields</div>
                                    <ResizableFieldPanel
                                        panelId="availableFields"
                                        panelLabel="Available Fields"
                                        size={fieldPanelSizes && fieldPanelSizes.availableFields}
                                        onSizeChange={handleFieldPanelSizeChange}
                                        theme={theme}
                                        outerStyle={dropZoneBaseStyle}
                                        contentStyle={{
                                            display: 'flex',
                                            flexWrap: 'wrap',
                                            gap: '4px',
                                            alignContent: 'flex-start',
                                            padding: '2px 12px 12px 2px',
                                        }}
                                    >
                                        {availableFields.map(f => (
                                            <div key={f} draggable onDragStart={e=>onDragStart(e,f,'pool')} style={{
                                                padding: '8px 12px',
                                                fontSize: '12px',
                                                lineHeight: 1.2,
                                                fontWeight: 500,
                                                fontFamily: "'Inter', ui-sans-serif, system-ui, sans-serif",
                                                borderRadius: theme.radiusSm || theme.radius || '10px',
                                                border: `1px solid ${theme.isDark ? theme.border : '#D7DDEA'}`,
                                                background: theme.isDark ? theme.background : (theme.surfaceInset || '#ffffff'),
                                                color: theme.isDark ? theme.text : '#334155',
                                                display: 'inline-flex',
                                                alignItems: 'center',
                                                boxShadow: theme.isDark ? 'none' : (theme.shadowInset || '0 1px 2px rgba(0,0,0,0.04)'),
                                                cursor: 'grab',
                                                userSelect: 'none',
                                                minHeight: '34px',
                                            }}>
                                                {formatDisplayLabel(f)}
                                            </div>
                                        ))}
                                    </ResizableFieldPanel>
                                </div>
                            )}
                            {[
                                {id:'rows', label:'Rows', icon: <Icons.List/>},
                                {id:'cols', label:'Columns', icon: <Icons.Columns/>},
                                {id:'vals', label:'Values', icon: <Icons.Sigma/>},
                                {id:'filter', label:'Filters', icon: <Icons.Filter/>}
                            ].map(zone => (
                                <div key={zone.id} style={{marginBottom: '20px'}}>
                                    <div style={styles.sectionTitle}>{zone.icon}{zone.label}</div>
                                    {zone.id === 'vals' && (
                                        <div style={{display:'flex', alignItems:'center', gap:'8px', padding:'0 4px 8px 4px', flexWrap:'wrap'}}>
                                            <button
                                                type="button"
                                                onClick={handleAddFormulaValue}
                                                style={{
                                                    border: `1px solid ${theme.primary}66`,
                                                    background: `${theme.primary}12`,
                                                    color: theme.primary,
                                                    borderRadius: '999px',
                                                    cursor: 'pointer',
                                                    padding: '4px 10px',
                                                    fontSize: '11px',
                                                    fontWeight: 700,
                                                }}
                                            >
                                                + Add Formula
                                            </button>
                                            <span style={{fontSize:'10px', color: theme.textSec}}>
                                                {regularValueCount} value measure{regularValueCount === 1 ? '' : 's'} available
                                            </span>
                                            {formulaCount > 0 && (
                                                <span style={{fontSize:'10px', color: theme.textSec}}>
                                                    {formulaCount} formula{formulaCount === 1 ? '' : 's'}
                                                </span>
                                            )}
                                        </div>
                                    )}
                                    {zone.id === 'rows' && rowFields.length > 0 && (
                                        <div style={{display:'flex', gap:'4px', padding:'0 4px 6px 4px'}}>
                                            <button
                                                title="Expand all hierarchy levels"
                                                onClick={() => handleExpandAllRows(true)}
                                                style={{flex:1, border:`1px solid ${theme.primary}`, background:theme.select, cursor:'pointer', padding:'3px 6px', fontSize:'11px', color:theme.primary, borderRadius:'4px', fontWeight:600}}
                                            >+ Expand All</button>
                                            <button
                                                title="Collapse all rows"
                                                onClick={() => handleExpandAllRows(false)}
                                                style={{flex:1, border:`1px solid ${theme.border}`, background:theme.background, cursor:'pointer', padding:'3px 6px', fontSize:'11px', color:theme.textSec, borderRadius:'4px'}}
                                            >- Collapse All</button>
                                        </div>
                                    )}
                                    <ResizableFieldPanel
                                        panelId={zone.id}
                                        panelLabel={zone.label}
                                        size={fieldPanelSizes && fieldPanelSizes[zone.id]}
                                        onSizeChange={handleFieldPanelSizeChange}
                                        theme={theme}
                                        outerStyle={dropZoneBaseStyle}
                                        contentStyle={{padding: `${dropZonePadding} 12px 12px ${dropZonePadding}`}}
                                    >
                                        <div onDragOver={e=>e.preventDefault()} onDrop={e=>onDrop(e, zone.id)}>
                                            {(zone.id==='filter' ? Object.keys(filters).filter(k=>k!=='global') : zone.id==='rows'?rowFields:zone.id==='cols'?colFields:valConfigs).map((item, idx) => {
                                                const label = zone.id==='vals' ? item.field : item;
                                                const displayLabel = zone.id === 'vals'
                                                    ? (item.agg === 'formula' ? (item.label || 'Formula') : formatDisplayLabel(item.field))
                                                    : formatDisplayLabel(item);
                                                return (
                                                    <div key={idx} draggable onDragStart={e=>onDragStart(e,item,zone.id,idx)} onDragOver={e=>onDragOver(e,zone.id,idx)}>
                                                        {zone.id === 'vals' && item.agg === 'formula' ? (
                                                            <FormulaListItem
                                                                item={item}
                                                                idx={idx}
                                                                selected={item.field === activeFormulaField}
                                                                onSelect={openFormulaModal}
                                                                onRemove={removeValueAtIndex}
                                                                theme={theme}
                                                                styles={styles}
                                                                dropLine={dropLine}
                                                                zoneId={zone.id}
                                                            />
                                                        ) : (
                                                            <div style={styles.chip}>
                                                                {dropLine && dropLine.zone===zone.id && dropLine.idx===idx && <div style={{...styles.dropLine,top:-2}}/>}
                                                                <div style={{display:'flex',alignItems:'center',gap:'8px', minWidth: 0, flex: 1}}>
                                                                    <Icons.DragIndicator/>
                                                                    <b style={{fontWeight:500, display:'block', minWidth: 0, whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{displayLabel}</b>
                                                                </div>
                                                                {zone.id === 'vals' && (
                                                                    <div style={{display:'flex', alignItems:'center', gap:'4px', marginLeft:'8px', flexWrap:'nowrap', flexShrink: 0}}>
                                                                        <select value={item.agg} onChange={e=>handleValueAggChange(idx, e.target.value)} style={{...valueSelectStyle, width:'74px'}} title="Aggregation">
                                                                            <option value="sum">Sum</option><option value="avg">Avg</option><option value="count">Cnt</option><option value="min">Min</option><option value="max">Max</option><option value="weighted_avg">WAvg</option>
                                                                        </select>
                                                                        {isWeightedAverageAgg(item.agg) && (
                                                                            <select value={item.weightField || ''} onChange={e => handleValueWeightFieldChange(idx, e.target.value)} style={{...valueSelectStyle, width:'92px'}} title="Weight field">
                                                                                <option value="" disabled>Weight</option>
                                                                                {availableFields.map((fieldName) => (
                                                                                    <option key={fieldName} value={fieldName}>{formatDisplayLabel(fieldName)}</option>
                                                                                ))}
                                                                            </select>
                                                                        )}
                                                                        <select value={item.windowFn || 'none'} onChange={e=>{const n=[...valConfigs];n[idx].windowFn=e.target.value==='none'?null:e.target.value;setValConfigs(n)}} style={{...valueSelectStyle, width:'56px'}}>
                                                                            <option value="none">Norm</option><option value="percent_of_row">%Row</option><option value="percent_of_col">%Col</option><option value="percent_of_grand_total">%Tot</option>
                                                                        </select>
                                                                    </div>
                                                                )}
                                                                <div style={{display:'flex', gap:'4px', marginLeft:'auto', alignItems: 'center'}}>
                                                                    {zone.id==='filter' && (
                                                                        <div onClick={(e) => openSidebarFilter(e, label)} style={{cursor:'pointer', display:'flex', alignItems:'center', padding: '2px', borderRadius: '4px'}}><Icons.Filter /></div>
                                                                    )}
                                                                    <span onClick={()=>{ if (zone.id==='filter'){const n={...filters};delete n[label];setFilters(n)} if (zone.id==='rows') setRowFields(p=>p.filter(x=>x!==label)); if (zone.id==='cols') setColFields(p=>p.filter(x=>x!==label)); if (zone.id==='vals') removeValueAtIndex(idx); }} style={{cursor:'pointer', color:'#9CA3AF', display:'flex', alignItems:'center'}}><Icons.Close/></span>
                                                                </div>
                                                                {zone.id === 'filter' && sidebarFilterState.columnId === label && (
                                                                    <FilterPopover column={{header: displayLabel, id: label}} anchorEl={sidebarFilterState.anchorEl} onClose={closeSidebarFilter} onFilter={(filterValue) => handleHeaderFilter(label, filterValue)} currentFilter={filters[label]} options={getFilterOptionsForColumn(label)} theme={theme} />
                                                                )}
                                                                {dropLine && dropLine.zone===zone.id && dropLine.idx===idx+1 && <div style={{...styles.dropLine,bottom:-2}}/>}
                                                            </div>
                                                        )}
                                                        {zone.id ==='filter' && filters[label] && filters[label].conditions && (
                                                            <div style={{fontSize: '10px', color: theme.primary, padding: '0 8px 4px 8px', marginTop: '-4px'}}>
                                                                {filters[label].conditions.map(c => `${c.type}: ${c.value}${c.caseSensitive ? ' (Match Case)' : ''}`).join(` ${filters[label].operator} `)}
                                                            </div>
                                                        )}
                                                    </div>
                                                )
                                            })}
                                            {(zone.id==='filter' ? Object.keys(filters).filter(k=>k!=='global') : zone.id==='rows'?rowFields:zone.id==='cols'?colFields:valConfigs).length === 0 && (
                                                <div style={{opacity:0.5, fontSize:'11px', padding:'8px', textAlign:'center', pointerEvents:'none'}}>Drag fields here</div>
                                            )}
                                            <div style={{height:20}} onDragOver={e=>onDragOver(e,zone.id,(zone.id==='rows'?rowFields:zone.id==='cols'?colFields:zone.id==='vals'?valConfigs:Object.keys(filters).filter(k=>k!=='global')).length)} />
                                        </div>
                                    </ResizableFieldPanel>
                                </div>
                            ))}
                        </>
                    ) : (
                        <div style={{display: 'flex', flexDirection: 'column', gap: '16px', height: '100%', overflow: 'hidden'}}>
                            {/* Enhanced Search Header */}
                            <div style={{display: 'flex', flexDirection: 'column', gap: '10px', padding: '8px', background: theme.headerBg, borderRadius: '8px', boxShadow: '0 1px 3px rgba(0,0,0,0.08)'}}>
                                <div style={{display: 'flex', alignItems: 'center', background: theme.background, borderRadius: '6px', padding: '8px 12px', border: `2px solid ${theme.border}`, transition: 'border-color 0.2s'}}>
                                    <Icons.Search />
                                    <input 
                                        placeholder="Search columns..."
                                        value={colSearch} 
                                        onChange={e => setColSearch(e.target.value)} 
                                        style={{
                                            border:'none', 
                                            background:'transparent', 
                                            marginLeft:'10px', 
                                            outline:'none', 
                                            width:'100%', 
                                            color: theme.text, 
                                            fontSize: '13px',
                                            fontWeight: 500
                                        }} 
                                    />
                                    {colSearch && (
                                        <span 
                                            onClick={() => setColSearch('')} 
                                            style={{
                                                cursor: 'pointer', 
                                                display: 'flex', 
                                                padding: '4px',
                                                borderRadius: '4px',
                                                background: theme.hover,
                                                transition: 'background 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.background = theme.select}
                                            onMouseLeave={e => e.currentTarget.style.background = theme.hover}
                                        >
                                            <Icons.Close />
                                        </span>
                                    )}
                                </div>
                                
                                {/* Type Filter Pills */}
                                <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                                    {[{"value": "all", "label": "All", "icon": "📊"},
                                        {"value": "number", "label": "Numbers", "icon": "🔢"},
                                        {"value": "string", "label": "Text", "icon": "📝"},
                                        {"value": "date", "label": "Dates", "icon": "📅"}
                                    ].map(type => (
                                        <button
                                            key={type.value}
                                            onClick={() => setColTypeFilter(type.value)}
                                            style={{
                                                padding: '6px 12px',
                                                borderRadius: '6px',
                                                border: 'none',
                                                background: colTypeFilter === type.value ? theme.primary : theme.background,
                                                color: colTypeFilter === type.value ? '#fff' : theme.text,
                                                cursor: 'pointer',
                                                fontSize: '11px',
                                                fontWeight: 600,
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: '4px',
                                                transition: 'all 0.2s',
                                                boxShadow: colTypeFilter === type.value ? '0 2px 4px rgba(0,0,0,0.1)' : 'none'
                                            }}
                                            onMouseEnter={e => {
                                                if (colTypeFilter !== type.value) {
                                                    e.currentTarget.style.background = theme.hover;
                                                }
                                            }}
                                            onMouseLeave={e => {
                                                if (colTypeFilter !== type.value) {
                                                    e.currentTarget.style.background = theme.background;
                                                }
                                            }}
                                        >
                                            <span>{type.icon}</span>
                                            <span>{type.label}</span>
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Enhanced Action Buttons */}
                            <div style={{display: 'flex', gap: '6px', padding: '0 8px'}}>
                                <button 
                                    onClick={() => table.toggleAllColumnsVisible(true)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.Visibility style={{fontSize: '14px'}} />
                                    <span>Show All</span>
                                </button>
                                <button 
                                    onClick={() => table.toggleAllColumnsVisible(false)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.VisibilityOff style={{fontSize: '14px'}} />
                                    <span>Hide All</span>
                                </button>
                                <button 
                                    onClick={() => toggleAllColumnsPinned(false)} 
                                    style={{
                                        ...styles.btn, 
                                        padding: '8px 12px', 
                                        fontSize: '11px', 
                                        flex: 1, 
                                        justifyContent: 'center',
                                        background: theme.background,
                                        fontWeight: 600,
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                        transition: 'all 0.2s'
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                    onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                >
                                    <Icons.Unpin style={{fontSize: '14px'}} />
                                    <span>Unpin All</span>
                                </button>
                            </div>

                            {/* Enhanced Selection Bar */}
                            {selectedCols.size > 0 && (
                                <div style={{
                                    display: 'flex', 
                                    flexDirection: 'column',
                                    gap: '8px', 
                                    background: `linear-gradient(135deg, ${theme.select}ee, ${theme.select}dd)`, 
                                    padding: '12px', 
                                    borderRadius: '8px', 
                                    border: `2px solid ${theme.primary}66`,
                                    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
                                    margin: '0 8px'
                                }}>
                                    <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                                        <span style={{
                                            fontSize: '12px', 
                                            fontWeight: 700, 
                                            padding: '4px 10px', 
                                            color: theme.primary, 
                                            background: theme.background,
                                            borderRadius: '6px',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
                                        }}>
                                            {selectedCols.size} Column{selectedCols.size > 1 ? 's' : ''} Selected
                                        </span>
                                        <button 
                                            onClick={() => setSelectedCols(new Set())} 
                                            style={{
                                                border: 'none', 
                                                background: theme.background, 
                                                cursor: 'pointer', 
                                                display: 'flex', 
                                                color: theme.textSec,
                                                padding: '4px',
                                                borderRadius: '4px',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.background = theme.hover}
                                            onMouseLeave={e => e.currentTarget.style.background = theme.background}
                                        >
                                            <Icons.Close/>
                                        </button>
                                    </div>
                                    <div style={{display: 'flex', gap: '6px', flexWrap: 'wrap'}}>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, 'left')); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.PinLeft />
                                            <span>Pin Left</span>
                                        </button>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, false)); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.Unpin />
                                            <span>Unpin</span>
                                        </button>
                                        <button 
                                            onClick={() => {
                                                Array.from(selectedCols).forEach(id => handlePinColumn(id, 'right')); 
                                                setSelectedCols(new Set()); 
                                            }} 
                                            style={{
                                                ...styles.btn, 
                                                padding: '6px 12px', 
                                                fontSize: '11px', 
                                                background: theme.background,
                                                flex: 1,
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                                            onMouseLeave={e => e.currentTarget.style.boxShadow = '0 1px 2px rgba(0,0,0,0.05)'}
                                        >
                                            <Icons.PinRight />
                                            <span>Pin Right</span>
                                        </button>
                                    </div>
                                </div>
                            )}

                            <div style={{flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column'}}>
                                {(() => {
                                    const allCols = table.getAllColumns().filter(c => !c.parent && c.id !== 'no_data');
                                    
                                    const filteredCols = allCols.filter(col => {
                                        if (colTypeFilter === 'all') return true;
                                        let type = 'string';
                                        
                                        // Check if it's a value column (aggregated)
                                        const config = valConfigs.find(v => col.id.includes(v.field));
                                        
                                        if (config) {
                                            type = 'number';
                                        } else if (data && data.length > 0) {
                                            // Check the first row's data for this column
                                            // Handle both flat data and nested/grouped data structures
                                            const firstRow = data[0];
                                            let val = firstRow[col.id];
                                            
                                            // If value is undefined in flat data, try to find it via accessor if possible, or skip
                                            if (val === undefined && col.accessorFn) {
                                                try {
                                                    val = col.accessorFn(firstRow);
                                                } catch (e) { /* ignore */ }
                                            }

                                            if (typeof val === 'number') {
                                                type = 'number';
                                            } else if (val instanceof Date) {
                                                type = 'date';
                                            } else if (typeof val === 'string') {
                                                if (!isNaN(Number(val)) && val.trim() !== '') {
                                                    type = 'number';
                                                } else if (!isNaN(Date.parse(val)) && val.includes('-')) {
                                                    type = 'date';
                                                }
                                            }
                                        }
                                        return type === colTypeFilter;
                                    });

                                    const leftPinned = filteredCols.filter(c => hasChildrenInZone(c, 'left'));
                                    const rightPinned = filteredCols.filter(c => hasChildrenInZone(c, 'right'));
                                    const unpinned = filteredCols.filter(c => hasChildrenInZone(c, 'unpinned'));

                                    const renderColList = (cols, sectionId) => cols.map(column => (
                                        <ColumnTreeItem 
                                            key={column.id} 
                                            column={column} 
                                            level={0} 
                                            theme={theme} 
                                            styles={styles} 
                                            handlePinColumn={handlePinColumn}
                                            colSearch={colSearch}
                                            selectedCols={selectedCols}
                                            setSelectedCols={setSelectedCols}
                                            onDrop={handleToolPanelDrop}
                                            sectionId={sectionId}
                                        />
                                    ));

                                    const handleToolPanelDrop = (colId, sectionId, targetColId) => {
                                        let targetIndex = undefined;
                                        const currentPinning = columnPinning || { left: [], right: [] };

                                        if (targetColId) {
                                            const list = sectionId === 'left' ? currentPinning.left : (sectionId === 'right' ? currentPinning.right : null);
                                            if (list) {
                                                targetIndex = list.indexOf(targetColId);
                                                
                                                // If not found, maybe it's a group?
                                                if (targetIndex === -1) {
                                                    const targetCol = table.getColumn(targetColId);
                                                    if (targetCol && isGroupColumn(targetCol)) {
                                                        const leaves = getAllLeafIdsFromColumn(targetCol);
                                                        const firstPinnedLeaf = leaves.find(id => list.includes(id));
                                                        if (firstPinnedLeaf) {
                                                            targetIndex = list.indexOf(firstPinnedLeaf);
                                                        }
                                                    }
                                                }

                                                if (targetIndex === -1) targetIndex = undefined;
                                            }
                                        }

                                        if (sectionId === 'left') handlePinColumn(colId, 'left', targetIndex);
                                        else if (sectionId === 'right') handlePinColumn(colId, 'right', targetIndex);
                                        else handlePinColumn(colId, false);
                                    };

                                    return (
                                        <>
                                            {leftPinned.length > 0 && (
                                                <ToolPanelSection 
                                                    title="Pinned Left" 
                                                    count={leftPinned.length} 
                                                    theme={theme} 
                                                    styles={styles}
                                                    sectionId="left"
                                                    onDrop={handleToolPanelDrop}
                                                >
                                                    {renderColList(leftPinned, 'left')}
                                                </ToolPanelSection>
                                            )}

                                            {rightPinned.length > 0 && (
                                                <ToolPanelSection 
                                                    title="Pinned Right" 
                                                    count={rightPinned.length} 
                                                    theme={theme} 
                                                    styles={styles}
                                                    sectionId="right"
                                                    onDrop={handleToolPanelDrop}
                                                >
                                                    {renderColList(rightPinned, 'right')}
                                                </ToolPanelSection>
                                            )}
                                            
                                            <ToolPanelSection 
                                                title="Columns" 
                                                count={unpinned.length} 
                                                theme={theme} 
                                                styles={styles}
                                                sectionId="unpinned"
                                                onDrop={handleToolPanelDrop}
                                            >
                                                {renderColList(unpinned, 'unpinned')}
                                            </ToolPanelSection>
                                        </>
                                    );
                                })()}
                            </div>
                            
                            {pinningPresets && pinningPresets.length > 0 && (
                                <div style={{padding: '8px', borderTop: `1px solid ${theme.border}`}}>
                                    <div style={styles.sectionTitle}>Pinning Presets</div>
                                    <div style={{display: 'flex', gap: '4px', flexWrap: 'wrap'}}>
                                        {pinningPresets.map((preset, i) => (
                                            <button 
                                                key={i}
                                                onClick={() => {
                                                    setColumnPinning(preset.config);
                                                    showNotification(`Applied preset: ${preset.name}`);
                                                }}
                                                style={{...styles.btn, fontSize: '11px', background: theme.headerBg}}
                                            >
                                                {preset.name}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            )}

                            <div style={{fontSize: '11px', color: theme.textSec, fontStyle: 'italic', padding: '8px', background: theme.headerBg, borderTop: `1px solid ${theme.border}44`}}>
                                <Icons.Lock style={{ verticalAlign: 'middle', marginRight: '4px', opacity: 0.5 }} />
                                Drag columns or use pin icons to freeze areas.
                            </div>
                        </div>
                    )}
                </div>
            {formulaModalConfig && (
                <FormulaEditorModal
                    item={formulaModalConfig}
                    idx={formulaModalIndex}
                    valConfigs={valConfigs}
                    setValConfigs={setValConfigs}
                    theme={theme}
                    onClose={() => setFormulaModalField(null)}
                    onRemove={removeValueAtIndex}
                />
            )}
            {/* Resize handle */}
            <div
                onMouseDown={onResizeMouseDown}
                style={{
                    position:'absolute', right:0, top:0, bottom:0, width:'5px',
                    cursor:'col-resize', zIndex:10,
                    background:'transparent',
                }}
                title="Drag to resize panel"
            >
                <div style={{
                    position:'absolute', right:0, top:0, bottom:0, width:'2px',
                    background: theme.border,
                    transition: 'background 0.15s',
                }} />
            </div>
        </div>
    );
}

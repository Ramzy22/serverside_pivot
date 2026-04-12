import React from 'react';
import Icons from '../../utils/Icons';

const fmt = (v, dec = 4) => {
    if (v === null || v === undefined || v === '') return '--';
    const n = Number(v);
    return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: dec }) : String(v);
};

/** Edit side panel for row-level editing sessions. */
function EditSidePanel({
    editedCells,
    propagationLog,
    onRevertCell,
    onRevertAll,
    onRevertSelected,
    onReapplyPropagation,
    onToggleDisplayMode,
    displayMode,
    onClose,
    theme,
    width = 320,
    pendingPropagation,
    propagationMethod,
    onPropagationMethodChange,
    onConfirmPropagation,
    onCancelPropagation,
}) {
    const hasPending = pendingPropagation && pendingPropagation.length > 0;
    const hasEdited = editedCells && editedCells.length > 0;
    const hasDirectEdits = hasEdited && editedCells.some((c) => c.direct);
    const canReapply = !hasPending && hasDirectEdits && typeof onReapplyPropagation === 'function';
    if (!hasEdited && !hasPending) return null;

    const panelWidth = Math.max(260, Math.min(Number(width) || 320, 480));
    const bg = theme.surfaceBg || theme.background;
    const borderColor = theme.border;
    const accent = theme.primary || '#4F46E5';

    return (
        <div
            data-pivot-edit-panel="true"
            style={{
                width: `${panelWidth}px`,
                minWidth: `${panelWidth}px`,
                borderLeft: `1px solid ${borderColor}`,
                background: bg,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
                overflow: 'hidden',
                fontSize: '11px',
                color: theme.text,
            }}
        >
            {/* Header */}
            <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '6px 10px',
                borderBottom: `1px solid ${borderColor}`,
                background: theme.headerBg || bg,
                flexShrink: 0,
            }}>
                <span style={{ fontWeight: 700, fontSize: '12px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                    <Icons.Edit />
                    Edit Inspector
                </span>
                <button
                    type="button"
                    onClick={onClose}
                    style={{
                        border: 'none',
                        background: 'transparent',
                        cursor: 'pointer',
                        color: theme.textSec,
                        padding: '2px',
                        display: 'flex',
                    }}
                >
                    <Icons.Close />
                </button>
            </div>

            {/* Action bar — Original / Revert All at the TOP */}
            {hasEdited && (
                <div style={{
                    display: 'flex',
                    gap: '6px',
                    padding: '6px 10px',
                    borderBottom: `1px solid ${borderColor}`,
                    flexShrink: 0,
                    background: theme.headerBg || bg,
                }}>
                    <button
                        type="button"
                        onClick={onToggleDisplayMode}
                        style={{
                            flex: 1,
                            border: `1px solid ${borderColor}`,
                            borderRadius: '5px',
                            background: displayMode === 'original' ? `${accent}15` : 'transparent',
                            color: theme.text,
                            padding: '4px 8px',
                            fontSize: '10px',
                            fontWeight: 600,
                            cursor: 'pointer',
                        }}
                    >
                        {displayMode === 'original' ? 'Show Edited' : 'Show Original'}
                    </button>
                    <button
                        type="button"
                        onClick={() => {
                            if (typeof onRevertSelected === 'function' && editedCells && editedCells.length > 0) {
                                onRevertSelected(editedCells);
                            } else if (typeof onRevertAll === 'function') {
                                onRevertAll();
                            }
                        }}
                        style={{
                            flex: 1,
                            border: '1px solid rgba(220, 38, 38, 0.3)',
                            borderRadius: '5px',
                            background: 'rgba(220, 38, 38, 0.08)',
                            color: '#B91C1C',
                            padding: '4px 8px',
                            fontSize: '10px',
                            fontWeight: 600,
                            cursor: 'pointer',
                        }}
                    >
                        Revert Selected ({editedCells ? editedCells.length : 0})
                    </button>
                </div>
            )}

            {/* Propagation method selector — shown when edit needs method pick */}
            {hasPending && (
                <div style={{
                    padding: '8px 10px',
                    borderBottom: `1px solid ${borderColor}`,
                    background: `${accent}08`,
                    flexShrink: 0,
                }}>
                    <div style={{ fontWeight: 700, fontSize: '11px', marginBottom: '6px', color: theme.text }}>
                        Propagation Method
                    </div>
                    <div style={{ fontSize: '10px', color: theme.textSec, marginBottom: '8px' }}>
                        Editing a group row will distribute the change to child rows.
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', marginBottom: '8px' }}>
                        {[
                            { value: 'equal', label: 'Equal', desc: 'Distribute change evenly across child rows' },
                            { value: 'proportional', label: 'Proportional', desc: 'Preserve current child ratios' },
                        ].map((opt) => (
                            <label key={opt.value} style={{
                                display: 'flex', alignItems: 'center', gap: '6px',
                                padding: '4px 8px', borderRadius: '4px', cursor: 'pointer',
                                background: propagationMethod === opt.value ? `${accent}14` : 'transparent',
                                border: `1px solid ${propagationMethod === opt.value ? accent + '40' : borderColor}`,
                            }}>
                                <input
                                    type="radio"
                                    name="propagation-method"
                                    value={opt.value}
                                    checked={propagationMethod === opt.value}
                                    onChange={() => onPropagationMethodChange(opt.value)}
                                    style={{ margin: 0, accentColor: accent }}
                                />
                                <div>
                                    <div style={{ fontWeight: 600, fontSize: '10px', color: theme.text }}>{opt.label}</div>
                                    <div style={{ fontSize: '9px', color: theme.textSec }}>{opt.desc}</div>
                                </div>
                            </label>
                        ))}
                    </div>
                    <div style={{ display: 'flex', gap: '6px' }}>
                        <button
                            type="button"
                            onClick={onConfirmPropagation}
                            style={{
                                flex: 1,
                                border: `1px solid ${accent}40`,
                                borderRadius: '5px',
                                background: `${accent}18`,
                                color: accent,
                                padding: '5px 8px',
                                fontSize: '10px',
                                fontWeight: 700,
                                cursor: 'pointer',
                            }}
                        >
                            Apply Edit
                        </button>
                        <button
                            type="button"
                            onClick={onCancelPropagation}
                            style={{
                                flex: 1,
                                border: `1px solid ${borderColor}`,
                                borderRadius: '5px',
                                background: 'transparent',
                                color: theme.textSec,
                                padding: '5px 8px',
                                fontSize: '10px',
                                fontWeight: 600,
                                cursor: 'pointer',
                            }}
                        >
                            Cancel
                        </button>
                    </div>
                </div>
            )}

            {/* Edited cells list */}
            {hasEdited && (
                <div style={{ flex: 1, overflowY: 'auto', padding: '4px 0' }}>
                    {editedCells.map((cell) => (
                        <div
                            key={`${cell.rowId}:::${cell.colId}`}
                            style={{
                                padding: '5px 10px',
                                borderBottom: `1px solid ${borderColor}22`,
                                display: 'flex',
                                flexDirection: 'column',
                                gap: '2px',
                            }}
                        >
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                                <span style={{ fontWeight: 600, color: theme.text, fontSize: '10px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }}>
                                    {cell.colId}
                                </span>
                                <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                                    {cell.propagated && (
                                        <span style={{
                                            fontSize: '9px',
                                            padding: '1px 4px',
                                            borderRadius: '3px',
                                            background: `${accent}18`,
                                            color: accent,
                                            fontWeight: 600,
                                        }}>
                                            propagated
                                        </span>
                                    )}
                                    {cell.direct && (
                                        <span style={{
                                            fontSize: '9px',
                                            padding: '1px 4px',
                                            borderRadius: '3px',
                                            background: 'rgba(16, 185, 129, 0.12)',
                                            color: '#047857',
                                            fontWeight: 600,
                                        }}>
                                            direct
                                        </span>
                                    )}
                                    {onRevertCell && (
                                        <button
                                            type="button"
                                            onClick={() => onRevertCell(cell.rowId, cell.colId)}
                                            title="Revert this cell to original"
                                            style={{
                                                border: 'none',
                                                background: 'transparent',
                                                cursor: 'pointer',
                                                color: theme.textSec,
                                                padding: '1px',
                                                fontSize: '10px',
                                                display: 'flex',
                                            }}
                                        >
                                            <Icons.Close />
                                        </button>
                                    )}
                                </div>
                            </div>
                            <div style={{ display: 'flex', gap: '8px', fontSize: '10px', color: theme.textSec }}>
                                <span>Row: <span style={{ color: theme.text, fontWeight: 500 }}>{String(cell.rowId).split('|||').pop()}</span></span>
                            </div>
                            <div style={{ display: 'flex', gap: '10px', fontSize: '10px' }}>
                                <span style={{ color: theme.textSec }}>
                                    Original: <span style={{ color: theme.text }}>{fmt(cell.originalValue)}</span>
                                </span>
                                {cell.currentValue !== undefined && (
                                    <span style={{ color: theme.textSec }}>
                                        Now: <span style={{ color: accent, fontWeight: 600 }}>{fmt(cell.currentValue)}</span>
                                    </span>
                                )}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Re-apply propagation for selected edited cells */}
            {canReapply && (
                <div style={{
                    padding: '6px 10px',
                    borderTop: `1px solid ${borderColor}`,
                    borderBottom: `1px solid ${borderColor}`,
                    background: `${accent}06`,
                    flexShrink: 0,
                }}>
                    <div style={{ fontWeight: 700, fontSize: '10px', marginBottom: '4px', color: theme.text }}>
                        Change Propagation
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
                        {[
                            { value: 'equal', label: 'Equal' },
                            { value: 'proportional', label: 'Proportional' },
                        ].map((opt) => (
                            <label key={opt.value} style={{
                                display: 'flex', alignItems: 'center', gap: '5px',
                                padding: '3px 6px', borderRadius: '3px', cursor: 'pointer', fontSize: '10px',
                                background: propagationMethod === opt.value ? `${accent}14` : 'transparent',
                                border: `1px solid ${propagationMethod === opt.value ? accent + '40' : borderColor}`,
                            }}>
                                <input type="radio" name="reapply-method" value={opt.value}
                                    checked={propagationMethod === opt.value}
                                    onChange={() => {
                                        onPropagationMethodChange(opt.value);
                                        onReapplyPropagation(editedCells.filter((c) => c.direct), opt.value);
                                    }}
                                    style={{ margin: 0, accentColor: accent }} />
                                <span style={{ fontWeight: 600 }}>{opt.label}</span>
                            </label>
                        ))}
                    </div>
                </div>
            )}

            {/* Propagation log */}
            {propagationLog && propagationLog.length > 0 && (
                <div style={{
                    borderTop: `1px solid ${borderColor}`,
                    flexShrink: 0,
                    maxHeight: '120px',
                    overflowY: 'auto',
                }}>
                    <div style={{
                        padding: '4px 10px',
                        fontWeight: 700,
                        fontSize: '10px',
                        color: theme.textSec,
                        background: theme.headerBg || bg,
                        borderBottom: `1px solid ${borderColor}22`,
                        position: 'sticky',
                        top: 0,
                    }}>
                        Propagation Log ({propagationLog.length})
                    </div>
                    {propagationLog.slice().reverse().slice(0, 10).map((entry, idx) => (
                        <div
                            key={idx}
                            style={{
                                padding: '3px 10px',
                                fontSize: '10px',
                                display: 'flex',
                                gap: '6px',
                                alignItems: 'center',
                                color: theme.textSec,
                                borderBottom: `1px solid ${borderColor}11`,
                            }}
                        >
                            <span style={{ fontWeight: 600, color: accent }}>{entry.strategy || 'equal'}</span>
                            <span>{entry.targetColumn || '?'}</span>
                            <span>{fmt(entry.fromValue, 2)} → {fmt(entry.toValue, 2)}</span>
                            <span>({entry.updatedRowCount || 0}r)</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

export default EditSidePanel;

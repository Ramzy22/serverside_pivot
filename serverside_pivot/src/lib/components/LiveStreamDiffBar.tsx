/**
 * Institutional Live Stream & Baseline Diff Control Strip.
 *
 * Renders directly above the pivot grid ONLY when streaming or baseline diff is active.
 * Subscribes exclusively to `ViewportSubscriptionStore.subscribeStatus`, isolating
 * toolbar updates from grid cells.
 * Sized to fit cleanly on a single row even inside constrained 580px center panes.
 */

import React from 'react';
import {
    useExternalStoreSnapshot,
    type ViewportSubscriptionStore,
} from '../runtime/useViewportSubscriptionStore';
import type { DiffDisplayMode } from '../runtime/streamingTypes';

export interface LiveStreamDiffBarProps {
    readonly store?: ViewportSubscriptionStore | null;
    readonly theme?: Record<string, unknown> | null;
}

const CADENCE_OPTIONS = [100, 250, 500, 1000] as const;
const DISPLAY_MODES: ReadonlyArray<{ readonly id: DiffDisplayMode; readonly label: string; readonly title: string }> = [
    { id: 'value', label: 'Val', title: 'Show live value' },
    { id: 'delta', label: 'Δ', title: 'Show absolute variance vs baseline/tick' },
    { id: 'pctDelta', label: '%Δ', title: 'Show percentage / pp variance' },
    { id: 'split', label: 'Split', title: 'Show live value + signed variance badge' },
];

export const LiveStreamDiffBar: React.FC<LiveStreamDiffBarProps> = React.memo(({ store, theme }) => {
    const status = useExternalStoreSnapshot(
        store ? store.subscribeStatus : () => () => undefined,
        store ? store.getStatus : () => null,
    );

    if (!store || !status || !status.active) return null;

    const isDark = Boolean(theme && theme.isDark);
    const border = String((theme && theme.border) || (isDark ? 'rgba(255,255,255,0.12)' : 'rgba(15,23,42,0.12)'));
    const surfaceBg = String((theme && (theme.headerSubtleBg || theme.headerBg || theme.surfaceBg)) || (isDark ? '#111827' : '#f8fafc'));
    const text = String((theme && theme.text) || (isDark ? '#f1f5f9' : '#0f172a'));
    const textSec = String((theme && theme.textSec) || (isDark ? '#94a3b8' : '#475569'));
    const primary = String((theme && theme.primary) || '#2563eb');
    const pillBg = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(15,23,42,0.05)';

    const isExactCdc = (status.capabilityTier === 'exact_cdc' || status.capabilityTier === 'tier_1_exact_cdc')
        && status.supportsExactRowDeltas;
    const isConnected = Boolean(status.subscriptionId)
        && status.connectionStatus !== 'error'
        && status.connectionStatus !== 'auth_revoked';

    let statusBadgeText = isExactCdc ? 'LIVE' : 'POLL';
    let statusDotColor = isExactCdc ? '#10b981' : '#0ea5e9';
    let statusPillBg = isExactCdc ? 'rgba(16,185,129,0.14)' : 'rgba(14,165,233,0.14)';
    let statusPillColor = isExactCdc ? (isDark ? '#34d399' : '#047857') : (isDark ? '#38bdf8' : '#0369a1');

    if (status.connectionStatus === 'auth_revoked') {
        statusBadgeText = 'REVOKED';
        statusDotColor = '#ef4444';
        statusPillBg = 'rgba(239,68,68,0.16)';
        statusPillColor = isDark ? '#fca5a5' : '#b91c1c';
    } else if (status.connectionStatus === 'error') {
        statusBadgeText = 'OFFLINE';
        statusDotColor = '#f59e0b';
        statusPillBg = 'rgba(245,158,11,0.16)';
        statusPillColor = isDark ? '#fcd34d' : '#b45309';
    } else if (status.frozenSnapshotId) {
        statusBadgeText = `FROZEN R${status.frozenRevision ?? status.sourceRevision}`;
        statusDotColor = '#06b6d4';
        statusPillBg = 'rgba(6,182,212,0.16)';
        statusPillColor = isDark ? '#67e8f9' : '#0e7490';
    } else if (status.paused) {
        statusBadgeText = status.bufferedUpdates > 0
            ? `PAUSED +${status.bufferedUpdates}`
            : 'PAUSED';
        statusDotColor = '#f59e0b';
        statusPillBg = 'rgba(245,158,11,0.16)';
        statusPillColor = isDark ? '#fcd34d' : '#b45309';
    } else if (status.connectionStatus === 'connecting' || status.connectionStatus === 'resyncing') {
        statusBadgeText = status.connectionStatus === 'resyncing' ? 'RESYNC' : 'SYNC';
        statusDotColor = '#6366f1';
        statusPillBg = 'rgba(99,102,241,0.14)';
        statusPillColor = isDark ? '#a5b4fc' : '#4338ca';
    }

    const btnStyle: React.CSSProperties = {
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '3px',
        height: '22px',
        padding: '0 6px',
        borderRadius: '5px',
        border: `1px solid ${border}`,
        background: pillBg,
        color: text,
        fontSize: '10.5px',
        fontWeight: 600,
        cursor: 'pointer',
        whiteSpace: 'nowrap',
        flexShrink: 0,
        fontVariantNumeric: 'tabular-nums',
    };

    const disabledBtnStyle: React.CSSProperties = {
        opacity: 0.45,
        cursor: 'not-allowed',
    };

    const dividerStyle: React.CSSProperties = {
        width: '1px',
        height: '13px',
        background: border,
        margin: '0 1px',
        flexShrink: 0,
    };

    return (
        <div
            data-live-stream-diff-bar="true"
            data-connection-status={status.connectionStatus}
            data-capability-tier={status.capabilityTier}
            style={{
                display: 'flex',
                alignItems: 'center',
                flexWrap: 'nowrap',
                overflowX: 'auto',
                gap: '4px',
                padding: '4px 7px',
                borderBottom: `1px solid ${border}`,
                background: surfaceBg,
                color: text,
                fontSize: '10.5px',
                lineHeight: 1.2,
                userSelect: 'none',
                scrollbarWidth: 'none',
            }}
        >
            {/* Honest Status & Revision Pill */}
            <span
                data-live-status-pill="true"
                title={status.lastError || `LIVE CDC · Revision ${status.sourceRevision} · Tier: ${status.capabilityTier}`}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '4px',
                    height: '22px',
                    padding: '0 7px',
                    borderRadius: '999px',
                    background: statusPillBg,
                    color: statusPillColor,
                    fontWeight: 700,
                    letterSpacing: '0.02em',
                    whiteSpace: 'nowrap',
                    flexShrink: 0,
                    fontVariantNumeric: 'tabular-nums',
                }}
            >
                <span
                    style={{
                        width: '6px',
                        height: '6px',
                        borderRadius: '50%',
                        background: statusDotColor,
                        boxShadow: status.connectionStatus === 'live' ? `0 0 0 2.5px ${statusPillBg}` : undefined,
                    }}
                />
                <span>{statusBadgeText}</span>
                {status.sourceRevision > 0 && !status.frozenSnapshotId && (
                    <span style={{ opacity: 0.78, fontWeight: 500 }}>· R{status.sourceRevision}</span>
                )}
            </span>

            {status.connectionStatus === 'error' && (
                <button
                    type="button"
                    className="sp-btn sp-btn-subtle"
                    data-live-reconnect-btn="true"
                    onClick={store.reconnect}
                    title={status.lastError || 'Reconnect live subscription'}
                    style={{ ...btnStyle, borderColor: '#f59e0b', color: isDark ? '#fcd34d' : '#b45309' }}
                >
                    <span>↻</span>
                </button>
            )}

            {/* Pause Live (visual hold) vs Freeze Snapshot (retained immutable snapshot) */}
            <button
                type="button"
                className="sp-btn sp-btn-subtle"
                data-live-pause-btn="true"
                aria-label={status.paused ? 'Resume Live' : 'Pause Live'}
                disabled={!isConnected || Boolean(status.frozenSnapshotId)}
                aria-pressed={status.paused}
                onClick={status.paused ? store.resumeLive : store.pauseLive}
                title={status.frozenSnapshotId
                    ? 'Unfreeze snapshot to use Pause Live'
                    : (!isConnected
                        ? 'Stream disconnected'
                        : (status.paused ? 'Resume live updates and apply buffered ticks' : 'Pause live visual updates (buffers incoming ticks)'))}
                style={{
                    ...btnStyle,
                    ...(!isConnected || Boolean(status.frozenSnapshotId) ? disabledBtnStyle : {}),
                    ...(status.paused ? { borderColor: '#f59e0b', color: isDark ? '#fcd34d' : '#b45309' } : {}),
                }}
            >
                <span>{status.paused ? '▶' : '⏸'}</span>
            </button>

            <button
                type="button"
                className="sp-btn sp-btn-subtle"
                data-live-freeze-btn="true"
                aria-label={status.frozenSnapshotId ? 'Unfreeze Snapshot' : 'Freeze Snapshot'}
                disabled={!isConnected}
                aria-pressed={Boolean(status.frozenSnapshotId)}
                onClick={status.frozenSnapshotId ? store.unfreezeSnapshot : store.freezeSnapshot}
                title={!isConnected
                    ? 'Stream disconnected'
                    : (status.frozenSnapshotId
                        ? 'Release frozen snapshot and return to live stream'
                        : 'Capture a retained immutable snapshot for consistent historical scrolling & export')}
                style={{
                    ...btnStyle,
                    ...(!isConnected ? disabledBtnStyle : {}),
                    ...(status.frozenSnapshotId ? { borderColor: '#06b6d4', color: isDark ? '#67e8f9' : '#0e7490' } : {}),
                }}
            >
                <span>{status.frozenSnapshotId ? '🔥' : '❄'}</span>
            </button>

            {/* Cadence selector */}
            <select
                aria-label="Stream cadence"
                value={status.cadenceMs}
                onChange={(e) => store.setCadenceMs(Number(e.target.value) || 250)}
                style={{
                    height: '22px',
                    padding: '0 4px',
                    borderRadius: '5px',
                    border: `1px solid ${border}`,
                    background: isDark ? '#1e293b' : '#ffffff',
                    color: text,
                    fontSize: '10.5px',
                    fontWeight: 600,
                    cursor: 'pointer',
                    flexShrink: 0,
                }}
            >
                {CADENCE_OPTIONS.map((ms) => (
                    <option key={ms} value={ms}>{ms >= 1000 ? `${ms / 1000}s` : `${ms}ms`}</option>
                ))}
            </select>

            {/* Sort Lock */}
            <button
                type="button"
                className="sp-btn sp-btn-subtle"
                data-live-sort-lock="true"
                aria-label={status.sortLock === 'in_place' ? 'In-Place Sort Lock' : 'Live Rank'}
                aria-pressed={status.sortLock === 'in_place'}
                onClick={() => store.setSortLock(status.sortLock === 'in_place' ? 'live_rank' : 'in_place')}
                title={status.sortLock === 'in_place'
                    ? 'In-Place Sort Lock: rows stay pinned while numbers tick (click for Live Rank)'
                    : 'Live Rank: rows dynamically re-sort as values change (click to pin order)'}
                style={{
                    ...btnStyle,
                    ...(status.sortLock === 'in_place' ? { borderColor: primary, color: primary } : {}),
                }}
            >
                <span>{status.sortLock === 'in_place' ? '📌' : '⇅'}</span>
            </button>

            <span style={dividerStyle} />

            {/* Pin Baseline (T0) & Baseline Selector */}
            <button
                type="button"
                className="sp-btn sp-btn-subtle"
                data-pin-baseline-btn="true"
                onClick={() => {
                    const now = new Date();
                    const hhmm = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
                    store.captureBaseline(`T₀ ${hhmm}`);
                }}
                title="Capture an immutable baseline snapshot (T₀) to compare live changes against"
                style={{
                    ...btnStyle,
                    background: isDark ? 'rgba(37,99,235,0.2)' : 'rgba(37,99,235,0.1)',
                    borderColor: primary,
                    color: isDark ? '#93c5fd' : primary,
                }}
            >
                <span>📸 T₀</span>
            </button>

            {status.baselines.length > 0 && (
                <select
                    aria-label="Select comparison baseline"
                    data-baseline-select="true"
                    value={status.activeBaselineId || ''}
                    onChange={(e) => store.selectBaseline(e.target.value || null)}
                    style={{
                        height: '22px',
                        lineHeight: '20px',
                        maxWidth: '120px',
                        padding: '0 4px',
                        borderRadius: '5px',
                        border: `1px solid ${status.activeBaselineId ? primary : border}`,
                        background: isDark ? '#1e293b' : '#ffffff',
                        color: text,
                        fontSize: '10.5px',
                        fontWeight: 600,
                        cursor: 'pointer',
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        flexShrink: 0,
                    }}
                >
                    <option value="">No Baseline</option>
                    {status.baselines.map((b) => (
                        <option key={b.snapshotId} value={b.snapshotId}>
                            {b.label} (R{b.sourceRevision})
                        </option>
                    ))}
                </select>
            )}

            {/* 4-Way Diff Display Mode */}
            <div
                role="group"
                aria-label="Diff display mode"
                data-diff-display-mode={status.diffDisplayMode}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    borderRadius: '5px',
                    border: `1px solid ${border}`,
                    overflow: 'hidden',
                    background: pillBg,
                    flexShrink: 0,
                }}
            >
                {DISPLAY_MODES.map((m) => {
                    const active = status.diffDisplayMode === m.id;
                    return (
                        <button
                            key={m.id}
                            type="button"
                            title={m.title}
                            aria-pressed={active}
                            onClick={() => store.setDiffDisplayMode(m.id)}
                            style={{
                                height: '20px',
                                padding: '0 6px',
                                border: 'none',
                                background: active ? primary : 'transparent',
                                color: active ? '#ffffff' : textSec,
                                fontSize: '10.5px',
                                fontWeight: active ? 700 : 600,
                                cursor: 'pointer',
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {m.label}
                        </button>
                    );
                })}
            </div>

            {/* Changed Only Toggle */}
            <button
                type="button"
                className="sp-btn sp-btn-subtle"
                data-changed-only-btn="true"
                aria-label="Changed Only"
                aria-pressed={status.changedOnly}
                onClick={() => store.setChangedOnly(!status.changedOnly)}
                title="Show only rows that differ from the active baseline"
                style={{
                    ...btnStyle,
                    ...(status.changedOnly
                        ? {
                            background: isDark ? 'rgba(245,158,11,0.22)' : 'rgba(245,158,11,0.14)',
                            borderColor: '#f59e0b',
                            color: isDark ? '#fcd34d' : '#b45309',
                        }
                        : {}),
                }}
            >
                <span>⚡</span>
            </button>
        </div>
    );
});

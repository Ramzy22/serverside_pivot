/**
 * Docked Live PnL Explain & Trajectory Side Panel.
 *
 * Opened by clicking any cell's hover PnL Explain card (just like docked chart / detail
 * side panels). Subscribes directly to the pinned `(rowKey, colId)` inside
 * `ViewportSubscriptionStore`, updating live on every tick with zero grid re-renders.
 */

import React, { useCallback, useEffect } from 'react';
import {
    useExternalStoreSnapshot,
    type ViewportSubscriptionStore,
} from '../runtime/useViewportSubscriptionStore';
import {
    decodeWireNumeric,
    formatWireDecimalString,
    isWireDecimal,
    type CellLiveMeta,
    type DiffDisplayMode,
} from '../runtime/streamingTypes';

export interface LiveExplainSidePanelProps {
    readonly store?: ViewportSubscriptionStore | null;
    readonly theme?: Record<string, unknown> | null;
    readonly width?: number;
}

const DISPLAY_MODES: ReadonlyArray<{ readonly id: DiffDisplayMode; readonly label: string }> = [
    { id: 'value', label: 'Val' },
    { id: 'delta', label: 'Δ' },
    { id: 'pctDelta', label: '%Δ' },
    { id: 'split', label: 'Split' },
];

function formatCompactSigned(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (isWireDecimal(value)) {
        const s = formatWireDecimalString(value);
        const sign = !s.startsWith('-') && Number(value.__decimal__) > 0 ? '+' : '';
        return `${sign}${s}`;
    }
    const num = decodeWireNumeric(value);
    if (num === null) return '—';
    const sign = num > 0 ? '+' : '';
    const abs = Math.abs(num);
    if (abs >= 1e9) return `${sign}${(num / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${sign}${(num / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) {
        return `${sign}${num.toLocaleString('en-US', { maximumFractionDigits: 2 })}`;
    }
    return `${sign}${num.toLocaleString('en-US', { maximumFractionDigits: 4 })}`;
}

function formatPctOrPp(pctDelta: number | null, deltaUnit: 'abs' | 'pp'): string {
    if (pctDelta === null || !Number.isFinite(pctDelta)) return '—';
    const sign = pctDelta > 0 ? '+' : '';
    if (deltaUnit === 'pp') {
        return `${sign}${(pctDelta * 100).toFixed(2)} pp`;
    }
    return `${sign}${(pctDelta * 100).toFixed(2)}%`;
}

function formatScalarPlain(value: unknown): string {
    if (value === null || value === undefined) return '—';
    if (isWireDecimal(value)) return formatWireDecimalString(value);
    const num = decodeWireNumeric(value);
    if (num === null) return String(value);
    return num.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function renderDetailedTrajectoryChart(
    points: readonly number[],
    isDark: boolean,
    border: string,
    textSec: string,
): React.ReactNode {
    if (!points || points.length < 2) {
        return (
            <div
                style={{
                    height: '112px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '11px',
                    color: textSec,
                    borderRadius: '8px',
                    border: `1px dashed ${border}`,
                }}
            >
                Waiting for live ticks to build trajectory…
            </div>
        );
    }

    const width = 296;
    const height = 112;
    const padX = 8;
    const padY = 12;
    let min = points[0];
    let max = points[0];
    for (let i = 1; i < points.length; i += 1) {
        if (points[i] < min) min = points[i];
        if (points[i] > max) max = points[i];
    }
    const span = max - min || 1;
    const coords = points.map((v, idx) => {
        const x = padX + (idx / (points.length - 1)) * (width - padX * 2);
        const y = height - padY - ((v - min) / span) * (height - padY * 2);
        return { x, y };
    });
    const linePoints = coords.map((p) => `${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
    const areaPath = [
        `M ${coords[0].x.toFixed(1)} ${(height - padY).toFixed(1)}`,
        ...coords.map((p) => `L ${p.x.toFixed(1)} ${p.y.toFixed(1)}`),
        `L ${coords[coords.length - 1].x.toFixed(1)} ${(height - padY).toFixed(1)}`,
        'Z',
    ].join(' ');

    const trendUp = points[points.length - 1] >= points[0];
    const stroke = trendUp ? (isDark ? '#34d399' : '#059669') : (isDark ? '#fb7185' : '#dc2626');
    const areaFill = trendUp
        ? (isDark ? 'rgba(52, 211, 153, 0.16)' : 'rgba(5, 150, 105, 0.12)')
        : (isDark ? 'rgba(251, 113, 133, 0.16)' : 'rgba(220, 38, 38, 0.12)');
    const lastPt = coords[coords.length - 1];
    const windowDelta = points[points.length - 1] - points[0];

    return (
        <div>
            <svg
                width="100%"
                height={height}
                viewBox={`0 0 ${width} ${height}`}
                preserveAspectRatio="none"
                style={{
                    display: 'block',
                    borderRadius: '8px',
                    background: isDark ? 'rgba(15, 23, 42, 0.55)' : 'rgba(15, 23, 42, 0.03)',
                    border: `1px solid ${border}`,
                }}
                aria-label="Live tick trajectory chart"
            >
                <line x1={padX} y1={padY} x2={width - padX} y2={padY} stroke={border} strokeDasharray="3 3" strokeWidth="1" />
                <line x1={padX} y1={height / 2} x2={width - padX} y2={height / 2} stroke={border} strokeDasharray="3 3" strokeWidth="1" />
                <line x1={padX} y1={height - padY} x2={width - padX} y2={height - padY} stroke={border} strokeDasharray="3 3" strokeWidth="1" />
                <path d={areaPath} fill={areaFill} />
                <polyline
                    fill="none"
                    stroke={stroke}
                    strokeWidth="2"
                    strokeLinejoin="round"
                    strokeLinecap="round"
                    points={linePoints}
                />
                <circle cx={lastPt.x} cy={lastPt.y} r="3.5" fill={stroke} />
            </svg>
            <div
                style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(3, 1fr)',
                    gap: '6px',
                    marginTop: '6px',
                    fontSize: '10.5px',
                    fontVariantNumeric: 'tabular-nums',
                }}
            >
                <div>
                    <div style={{ color: textSec, fontSize: '9.5px', textTransform: 'uppercase' }}>Min</div>
                    <div style={{ fontWeight: 600 }}>{formatScalarPlain(min)}</div>
                </div>
                <div>
                    <div style={{ color: textSec, fontSize: '9.5px', textTransform: 'uppercase' }}>Max</div>
                    <div style={{ fontWeight: 600 }}>{formatScalarPlain(max)}</div>
                </div>
                <div style={{ textAlign: 'right' }}>
                    <div style={{ color: textSec, fontSize: '9.5px', textTransform: 'uppercase' }}>Window Δ</div>
                    <div style={{ fontWeight: 700, color: stroke }}>{formatCompactSigned(windowDelta)}</div>
                </div>
            </div>
        </div>
    );
}

export const LiveExplainSidePanel: React.FC<LiveExplainSidePanelProps> = React.memo(({
    store,
    theme,
    width = 336,
}) => {
    const status = useExternalStoreSnapshot(
        store ? store.subscribeStatus : () => () => undefined,
        store ? store.getStatus : () => null,
    );
    const pinned = status?.pinnedExplainCell ?? null;

    const subscribeCell = useCallback(
        (listener: () => void) => (
            store && pinned
                ? store.subscribeCell(pinned.rowKey, pinned.colId, listener)
                : () => undefined
        ),
        [store, pinned?.rowKey, pinned?.colId],
    );

    const getCellSnapshot = useCallback(
        () => (store && pinned ? store.getCellMeta(pinned.rowKey, pinned.colId) : null),
        [store, pinned?.rowKey, pinned?.colId],
    );

    const meta: CellLiveMeta | null = useExternalStoreSnapshot(subscribeCell, getCellSnapshot);

    useEffect(() => {
        if (!store || !pinned) return undefined;
        const onKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                store.closeExplainPanel();
            }
        };
        document.addEventListener('keydown', onKeyDown);
        return () => document.removeEventListener('keydown', onKeyDown);
    }, [store, pinned]);

    if (!store || !status || !pinned) return null;

    const isDark = Boolean(theme && theme.isDark);
    const border = String((theme && theme.border) || (isDark ? 'rgba(255,255,255,0.12)' : 'rgba(15,23,42,0.12)'));
    const surfaceBg = String((theme && (theme.surfaceBg || theme.background)) || (isDark ? '#0b1220' : '#ffffff'));
    const headerBg = String((theme && (theme.headerSubtleBg || theme.headerBg)) || (isDark ? '#111827' : '#f8fafc'));
    const cardBg = isDark ? 'rgba(255,255,255,0.035)' : 'rgba(15,23,42,0.025)';
    const text = String((theme && theme.text) || (isDark ? '#f1f5f9' : '#0f172a'));
    const textSec = String((theme && theme.textSec) || (isDark ? '#94a3b8' : '#64748b'));
    const primary = String((theme && theme.primary) || '#2563eb');

    const activeDelta = status.activeBaselineId ? meta?.baselineDelta : meta?.tickDelta;
    const activePct = status.activeBaselineId ? meta?.baselinePct : meta?.tickPct;
    const activeDeltaNum = decodeWireNumeric(activeDelta);
    const deltaColor = activeDeltaNum === null || activeDeltaNum === 0
        ? textSec
        : (activeDeltaNum > 0
            ? (isDark ? '#34d399' : '#059669')
            : (isDark ? '#fb7185' : '#dc2626'));
    const tickDeltaNum = decodeWireNumeric(meta?.tickDelta);
    const tickColor = tickDeltaNum === null || tickDeltaNum === 0
        ? textSec
        : (tickDeltaNum > 0
            ? (isDark ? '#34d399' : '#059669')
            : (isDark ? '#fb7185' : '#dc2626'));

    const drivers = meta?.varianceDrivers || [];
    const maxDriverAbs = drivers.reduce((acc, d) => {
        const n = Math.abs(decodeWireNumeric(d.delta) || 0);
        return n > acc ? n : acc;
    }, 0) || 1;

    return (
        <aside
            role="complementary"
            aria-label="Live PnL Explain Inspector"
            data-live-explain-sidepanel="true"
            style={{
                width: `${width}px`,
                minWidth: `${width}px`,
                maxWidth: `${width}px`,
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                borderLeft: `1px solid ${border}`,
                background: surfaceBg,
                color: text,
                fontSize: '12px',
                overflow: 'hidden',
                flexShrink: 0,
                fontVariantNumeric: 'tabular-nums',
            }}
        >
            {/* Header */}
            <div
                style={{
                    display: 'flex',
                    alignItems: 'flex-start',
                    justifyContent: 'space-between',
                    gap: '8px',
                    padding: '10px 12px',
                    borderBottom: `1px solid ${border}`,
                    background: headerBg,
                }}
            >
                <div style={{ minWidth: 0, flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '3px' }}>
                        <span
                            style={{
                                width: '7px',
                                height: '7px',
                                borderRadius: '50%',
                                background: status.connectionStatus === 'live' ? '#10b981' : '#f59e0b',
                                flexShrink: 0,
                            }}
                        />
                        <span style={{ fontSize: '10px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', color: primary }}>
                            PnL Explain · R{status.sourceRevision}
                        </span>
                    </div>
                    <div
                        title={pinned.rowLabel}
                        style={{
                            fontSize: '13px',
                            fontWeight: 700,
                            color: text,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                        }}
                    >
                        {pinned.rowLabel || pinned.rowKey}
                    </div>
                    <div style={{ fontSize: '11px', color: textSec, marginTop: '1px' }}>
                        {pinned.colLabel || pinned.colId}
                    </div>
                </div>

                <button
                    type="button"
                    className="sp-btn sp-btn-ghost"
                    aria-label="Close Live Explain Panel"
                    data-live-explain-close="true"
                    onClick={store.closeExplainPanel}
                    style={{
                        width: '26px',
                        height: '26px',
                        borderRadius: '6px',
                        border: `1px solid ${border}`,
                        background: 'transparent',
                        color: textSec,
                        cursor: 'pointer',
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        fontSize: '13px',
                        flexShrink: 0,
                    }}
                    title="Close panel (Esc)"
                >
                    ✕
                </button>
            </div>

            {/* Scrollable Body */}
            <div style={{ flex: 1, overflowY: 'auto', padding: '12px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
                {/* Hero Metric Card */}
                <div
                    style={{
                        padding: '12px',
                        borderRadius: '10px',
                        background: cardBg,
                        border: `1px solid ${border}`,
                    }}
                >
                    <div style={{ fontSize: '10px', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: textSec }}>
                        Current Value
                    </div>
                    <div style={{ fontSize: '22px', fontWeight: 800, letterSpacing: '-0.02em', marginTop: '2px', color: text }}>
                        {formatScalarPlain(meta?.value)}
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginTop: '10px', paddingTop: '9px', borderTop: `1px solid ${border}` }}>
                        <div>
                            <div style={{ fontSize: '10px', color: textSec }}>Last Tick</div>
                            <div style={{ fontSize: '12.5px', fontWeight: 700, color: tickColor, marginTop: '2px' }}>
                                {meta?.tickDelta !== null && meta?.tickDelta !== undefined
                                    ? formatCompactSigned(meta.tickDelta)
                                    : '0'}
                            </div>
                        </div>
                        {status.activeBaselineId ? (
                            <div>
                                <div style={{ fontSize: '10px', color: textSec }}>
                                    {status.activeBaselineLabel || 'Baseline T₀'}
                                </div>
                                <div style={{ fontSize: '12.5px', fontWeight: 600, color: text, marginTop: '2px' }}>
                                    {formatScalarPlain(meta?.baselineValue)}
                                </div>
                            </div>
                        ) : (
                            <div>
                                <div style={{ fontSize: '10px', color: textSec }}>Ticks Observed</div>
                                <div style={{ fontSize: '12.5px', fontWeight: 600, color: text, marginTop: '2px' }}>
                                    {meta?.sparklineHistory.length ?? 0}
                                </div>
                            </div>
                        )}
                    </div>

                    {status.activeBaselineId && (
                        <div
                            style={{
                                marginTop: '9px',
                                padding: '7px 9px',
                                borderRadius: '7px',
                                background: activeDeltaNum !== null && activeDeltaNum < 0
                                    ? 'rgba(239,68,68,0.12)'
                                    : 'rgba(16,185,129,0.12)',
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                            }}
                        >
                            <span style={{ fontSize: '10.5px', fontWeight: 600, color: textSec }}>Cumulative Δ vs T₀</span>
                            <span style={{ fontSize: '12px', fontWeight: 700, color: deltaColor }}>
                                {formatCompactSigned(meta?.baselineDelta)} ({formatPctOrPp(activePct ?? null, meta?.deltaUnit || 'abs')})
                            </span>
                        </div>
                    )}
                </div>

                {/* Live Trajectory Chart */}
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                        <span style={{ fontSize: '10.5px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', color: textSec }}>
                            Live Trajectory
                        </span>
                        <span style={{ fontSize: '10.5px', color: textSec }}>
                            {meta?.sparklineHistory.length ?? 0} / 30 ticks
                        </span>
                    </div>
                    {renderDetailedTrajectoryChart(meta?.sparklineHistory || [], isDark, border, textSec)}
                </div>

                {/* Top Variance Drivers (PnL Explain) */}
                <div>
                    <div style={{ fontSize: '10.5px', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', color: textSec, marginBottom: '6px' }}>
                        Top Drivers (PnL Explain)
                    </div>
                    {drivers.length > 0 ? (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '7px' }}>
                            {drivers.map((d) => {
                                const dNum = decodeWireNumeric(d.delta) ?? 0;
                                const isNeg = dNum < 0;
                                const dCol = isNeg
                                    ? (isDark ? '#fb7185' : '#dc2626')
                                    : (isDark ? '#34d399' : '#059669');
                                const barPct = Math.min(100, Math.max(4, (Math.abs(dNum) / maxDriverAbs) * 100));
                                return (
                                    <div
                                        key={d.path}
                                        style={{
                                            padding: '7px 9px',
                                            borderRadius: '7px',
                                            background: cardBg,
                                            border: `1px solid ${border}`,
                                        }}
                                    >
                                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', marginBottom: '4px' }}>
                                            <span style={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                {d.label}
                                            </span>
                                            <span style={{ color: dCol, fontWeight: 700, flexShrink: 0 }}>
                                                {formatCompactSigned(d.delta)}
                                            </span>
                                        </div>
                                        <div style={{ height: '4px', borderRadius: '999px', background: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(15,23,42,0.08)', overflow: 'hidden' }}>
                                            <div style={{ width: `${barPct}%`, height: '100%', borderRadius: '999px', background: dCol }} />
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    ) : (
                        <div style={{ fontSize: '11px', color: textSec, padding: '8px 10px', borderRadius: '7px', background: cardBg, border: `1px solid ${border}` }}>
                            Leaf node or no child variance drivers recorded yet for this cell.
                        </div>
                    )}
                </div>

                {/* Quick Controls Footer */}
                <div style={{ marginTop: 'auto', paddingTop: '8px', borderTop: `1px solid ${border}`, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '6px' }}>
                    <div style={{ display: 'inline-flex', borderRadius: '6px', border: `1px solid ${border}`, overflow: 'hidden' }}>
                        {DISPLAY_MODES.map((m) => {
                            const active = status.diffDisplayMode === m.id;
                            return (
                                <button
                                    key={m.id}
                                    type="button"
                                    aria-pressed={active}
                                    onClick={() => store.setDiffDisplayMode(m.id)}
                                    style={{
                                        height: '24px',
                                        padding: '0 8px',
                                        border: 'none',
                                        background: active ? primary : 'transparent',
                                        color: active ? '#fff' : textSec,
                                        fontSize: '11px',
                                        fontWeight: active ? 700 : 600,
                                        cursor: 'pointer',
                                    }}
                                >
                                    {m.label}
                                </button>
                            );
                        })}
                    </div>
                    <button
                        type="button"
                        className="sp-btn sp-btn-subtle"
                        onClick={() => {
                            const now = new Date();
                            const hhmm = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
                            store.captureBaseline(`T₀ ${hhmm}`);
                        }}
                        style={{
                            height: '24px',
                            padding: '0 8px',
                            borderRadius: '6px',
                            border: `1px solid ${primary}`,
                            background: isDark ? 'rgba(37,99,235,0.2)' : 'rgba(37,99,235,0.1)',
                            color: isDark ? '#93c5fd' : primary,
                            fontSize: '11px',
                            fontWeight: 600,
                            cursor: 'pointer',
                        }}
                    >
                        📸 Pin T₀
                    </button>
                </div>
            </div>
        </aside>
    );
});

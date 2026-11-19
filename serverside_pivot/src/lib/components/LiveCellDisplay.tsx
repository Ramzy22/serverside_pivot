/**
 * Granular Per-Cell Reactive Live & Diff Renderer (Component 6).
 *
 * Subscribes ONLY to its own `(rowKey, colId)` entry inside `ViewportSubscriptionStore`:
 * - Triggers GPU-composited `Element.animate()` green/red tick flashes with ZERO React timers.
 * - Renders 4-way Diff Display (`Value`, `Δ`, `%Δ` or `pp`, `Split`) with tabular-nums.
 * - Renders `NEW` / `REMOVED` row badges on hierarchy cells & `softStale` syncing indicator.
 * - Renders an institutional **PnL Explain & 30-Point Rolling Sparkline** popover on hover.
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import ReactDOM from 'react-dom';
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

export interface LiveCellDisplayProps {
    readonly store: ViewportSubscriptionStore;
    readonly rowKey: string;
    readonly colId: string;
    readonly rowLabel?: string;
    readonly colLabel?: string;
    readonly isHierarchy?: boolean;
    readonly softStale?: boolean;
    readonly isDark?: boolean;
    readonly theme?: Record<string, unknown> | null;
    readonly fallbackContent: React.ReactNode;
}

function formatCompactSigned(value: unknown, suffix = ''): string {
    if (value === null || value === undefined) return '—';
    if (isWireDecimal(value)) {
        const s = formatWireDecimalString(value);
        const sign = !s.startsWith('-') && Number(value.__decimal__) > 0 ? '+' : '';
        return `${sign}${s}${suffix}`;
    }
    const num = decodeWireNumeric(value);
    if (num === null) return '—';
    const sign = num > 0 ? '+' : '';
    const abs = Math.abs(num);
    if (abs >= 1e9) return `${sign}${(num / 1e9).toFixed(2)}B${suffix}`;
    if (abs >= 1e6) return `${sign}${(num / 1e6).toFixed(2)}M${suffix}`;
    if (abs >= 1e3) {
        return `${sign}${num.toLocaleString('en-US', { maximumFractionDigits: 2 })}${suffix}`;
    }
    return `${sign}${num.toLocaleString('en-US', { maximumFractionDigits: 4 })}${suffix}`;
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
    return num.toLocaleString('en-US', { maximumFractionDigits: 4 });
}

function renderMiniSparkline(points: readonly number[], isDark: boolean): React.ReactNode {
    if (!points || points.length < 2) return null;
    const width = 168;
    const height = 30;
    let min = points[0];
    let max = points[0];
    for (let i = 1; i < points.length; i += 1) {
        if (points[i] < min) min = points[i];
        if (points[i] > max) max = points[i];
    }
    const span = max - min || 1;
    const coords = points.map((v, idx) => {
        const x = (idx / (points.length - 1)) * (width - 4) + 2;
        const y = height - 3 - ((v - min) / span) * (height - 6);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    const trendUp = points[points.length - 1] >= points[0];
    const stroke = trendUp ? (isDark ? '#34d399' : '#059669') : (isDark ? '#fb7185' : '#dc2626');
    return (
        <svg
            width={width}
            height={height}
            viewBox={`0 0 ${width} ${height}`}
            style={{ display: 'block', marginTop: '4px' }}
            aria-hidden="true"
        >
            <polyline
                fill="none"
                stroke={stroke}
                strokeWidth="1.6"
                strokeLinejoin="round"
                strokeLinecap="round"
                points={coords.join(' ')}
            />
        </svg>
    );
}

const prefersReducedMotion = (): boolean => (
    typeof window !== 'undefined'
    && typeof window.matchMedia === 'function'
    && window.matchMedia('(prefers-reduced-motion: reduce)').matches
);

// Ensure at most one PnL Explain popover is mounted at any moment across rapid cell hovers.
let activePopoverDismiss: (() => void) | null = null;

export const LiveCellDisplay: React.FC<LiveCellDisplayProps> = React.memo(({
    store,
    rowKey,
    colId,
    rowLabel,
    colLabel,
    isHierarchy = false,
    softStale = false,
    isDark = false,
    theme,
    fallbackContent,
}) => {
    const subscribeCell = useCallback(
        (listener: () => void) => (
            isHierarchy
                ? store.subscribeRow(rowKey, listener)
                : store.subscribeCell(rowKey, colId, listener)
        ),
        [store, rowKey, colId, isHierarchy],
    );

    const getCellSnapshot = useCallback(
        () => (isHierarchy ? null : store.getCellMeta(rowKey, colId)),
        [store, rowKey, colId, isHierarchy],
    );

    const getRowLifecycleSnapshot = useCallback(
        () => store.getRowLifecycle(rowKey),
        [store, rowKey],
    );

    const meta: CellLiveMeta | null = useExternalStoreSnapshot(subscribeCell, getCellSnapshot);
    const rowLifecycle = useExternalStoreSnapshot(
        useCallback((listener: () => void) => store.subscribeRow(rowKey, listener), [store, rowKey]),
        getRowLifecycleSnapshot,
    );

    const hostRef = useRef<HTMLSpanElement | null>(null);
    const lastAnimatedSeqRef = useRef<number>(meta ? meta.tickSeq : 0);
    const [popoverAnchor, setPopoverAnchor] = useState<{ x: number; y: number } | null>(null);
    const closeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

    const cancelCloseTimer = useCallback(() => {
        if (closeTimerRef.current !== null) {
            clearTimeout(closeTimerRef.current);
            closeTimerRef.current = null;
        }
    }, []);

    const dismissSelf = useCallback(() => {
        cancelCloseTimer();
        setPopoverAnchor(null);
    }, [cancelCloseTimer]);

    useEffect(() => () => {
        cancelCloseTimer();
        if (activePopoverDismiss === dismissSelf) {
            activePopoverDismiss = null;
        }
    }, [cancelCloseTimer, dismissSelf]);

    const status = store.getStatus();
    const displayMode: DiffDisplayMode = status.diffDisplayMode;

    // GPU-composited Element.animate() directional flash — zero React timers!
    useEffect(() => {
        if (!meta || isHierarchy) return;
        if (meta.tickSeq <= lastAnimatedSeqRef.current) return;
        lastAnimatedSeqRef.current = meta.tickSeq;
        if (!status.flashEnabled || !meta.tickDir || prefersReducedMotion()) return;
        const el = hostRef.current;
        if (!el || typeof el.animate !== 'function') return;

        const flashColor = meta.tickDir === 'up'
            ? (isDark ? 'rgba(16, 185, 129, 0.34)' : 'rgba(16, 185, 129, 0.24)')
            : (isDark ? 'rgba(244, 63, 94, 0.34)' : 'rgba(239, 68, 68, 0.22)');

        try {
            el.animate(
                [
                    { backgroundColor: flashColor },
                    { backgroundColor: 'transparent' },
                ],
                {
                    duration: Math.max(150, status.flashDurationMs || 550),
                    easing: 'cubic-bezier(0.22, 1, 0.36, 1)',
                },
            );
        } catch {
            // Non-fatal in JSDOM / older webviews
        }
    }, [meta?.tickSeq, meta?.tickDir, isHierarchy, isDark, status.flashEnabled, status.flashDurationMs]);

    // Hierarchy cell: decorate with NEW / REMOVED lifecycle badge & syncing dot
    if (isHierarchy) {
        const isNew = rowLifecycle === 'new';
        const isRemoved = rowLifecycle === 'removed';
        return (
            <span
                data-row-lifecycle={rowLifecycle}
                data-freshness={softStale ? 'syncing' : 'live'}
                style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '6px',
                    width: '100%',
                    textDecoration: isRemoved ? 'line-through' : undefined,
                    opacity: isRemoved ? 0.62 : (softStale ? 0.85 : 1),
                }}
            >
                <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {fallbackContent}
                </span>
                {isNew && (
                    <span
                        data-lifecycle-badge="new"
                        style={{
                            fontSize: '9px',
                            fontWeight: 700,
                            padding: '1px 5px',
                            borderRadius: '4px',
                            background: 'rgba(16,185,129,0.18)',
                            color: isDark ? '#34d399' : '#047857',
                            letterSpacing: '0.04em',
                            flexShrink: 0,
                        }}
                    >
                        NEW
                    </span>
                )}
                {isRemoved && (
                    <span
                        data-lifecycle-badge="removed"
                        style={{
                            fontSize: '9px',
                            fontWeight: 700,
                            padding: '1px 5px',
                            borderRadius: '4px',
                            background: 'rgba(239,68,68,0.18)',
                            color: isDark ? '#fca5a5' : '#b91c1c',
                            letterSpacing: '0.04em',
                            flexShrink: 0,
                        }}
                    >
                        REMOVED
                    </span>
                )}
                {softStale && (
                    <span
                        title="Syncing latest revision…"
                        style={{
                            width: '5px',
                            height: '5px',
                            borderRadius: '50%',
                            background: '#f59e0b',
                            flexShrink: 0,
                        }}
                    />
                )}
            </span>
        );
    }

    if (!meta) {
        return <>{fallbackContent}</>;
    }

    const activeDelta = status.activeBaselineId ? meta.baselineDelta : meta.tickDelta;
    const activePct = status.activeBaselineId ? meta.baselinePct : meta.tickPct;
    const activeDeltaNum = decodeWireNumeric(activeDelta);
    const deltaColor = activeDeltaNum === null || activeDeltaNum === 0
        ? (isDark ? '#94a3b8' : '#64748b')
        : (activeDeltaNum > 0
            ? (isDark ? '#34d399' : '#059669')
            : (isDark ? '#fb7185' : '#dc2626'));

    const hasExplainData = Boolean(
        (status.showSparklineTooltip && meta.sparklineHistory.length >= 2)
        || (status.showPnlExplain && (meta.baselineValue !== null || meta.varianceDrivers.length > 0 || meta.tickSeq > 0)),
    );

    const handleMouseEnter = (e: React.MouseEvent<HTMLSpanElement>) => {
        if (!hasExplainData) return;
        cancelCloseTimer();
        if (activePopoverDismiss && activePopoverDismiss !== dismissSelf) {
            activePopoverDismiss();
        }
        activePopoverDismiss = dismissSelf;
        const rect = e.currentTarget.getBoundingClientRect();
        const x = Math.min(window.innerWidth - 264, Math.max(8, rect.left));
        const y = rect.bottom + 4 + 190 > window.innerHeight
            ? Math.max(8, rect.top - 196)
            : rect.bottom + 4;
        setPopoverAnchor({ x, y });
    };

    const scheduleDismiss = () => {
        cancelCloseTimer();
        closeTimerRef.current = setTimeout(() => {
            closeTimerRef.current = null;
            if (activePopoverDismiss === dismissSelf) {
                activePopoverDismiss = null;
            }
            setPopoverAnchor(null);
        }, 180);
    };

    const handleOpenSidePanel = (e: React.MouseEvent) => {
        e.stopPropagation();
        dismissSelf();
        store.openExplainPanel({
            rowKey,
            colId,
            rowLabel: rowLabel || rowKey.replace(/\|\|\|/g, ' › '),
            colLabel: colLabel || colId,
        });
    };

    let primaryRendered: React.ReactNode = fallbackContent;
    if (displayMode === 'delta') {
        primaryRendered = (
            <span style={{ color: deltaColor, fontWeight: 600 }}>
                {meta.deltaUnit === 'pp' && status.activeBaselineId
                    ? formatPctOrPp(activePct, 'pp')
                    : formatCompactSigned(activeDelta)}
            </span>
        );
    } else if (displayMode === 'pctDelta') {
        primaryRendered = (
            <span style={{ color: deltaColor, fontWeight: 600 }}>
                {formatPctOrPp(activePct, meta.deltaUnit)}
            </span>
        );
    } else if (displayMode === 'split') {
        primaryRendered = (
            <span
                style={{
                    display: 'inline-flex',
                    flexDirection: 'column',
                    alignItems: 'flex-end',
                    justifyContent: 'center',
                    lineHeight: 1.12,
                    gap: '1px',
                    maxWidth: '100%',
                    overflow: 'hidden',
                }}
            >
                <span style={{ whiteSpace: 'nowrap' }}>{fallbackContent}</span>
                {activeDeltaNum !== null && activeDeltaNum !== 0 && (
                    <span
                        style={{
                            fontSize: '9.5px',
                            fontWeight: 600,
                            color: deltaColor,
                            whiteSpace: 'nowrap',
                            lineHeight: 1.05,
                        }}
                    >
                        {meta.deltaUnit === 'pp' && status.activeBaselineId
                            ? formatPctOrPp(activePct, 'pp')
                            : formatCompactSigned(activeDelta)}
                    </span>
                )}
            </span>
        );
    }

    const popoverBg = String((theme && (theme.surfaceBg || theme.background)) || (isDark ? '#0f172a' : '#ffffff'));
    const popoverBorder = String((theme && theme.border) || (isDark ? 'rgba(255,255,255,0.15)' : 'rgba(15,23,42,0.14)'));
    const popoverText = String((theme && theme.text) || (isDark ? '#f1f5f9' : '#0f172a'));
    const popoverSec = String((theme && theme.textSec) || (isDark ? '#94a3b8' : '#64748b'));

    const popoverNode = popoverAnchor && hasExplainData ? (
        <div
            role="dialog"
            aria-label="PnL Explain preview — click to dock in side panel"
            data-pnl-explain-popover="true"
            onMouseEnter={cancelCloseTimer}
            onMouseLeave={scheduleDismiss}
            onClick={handleOpenSidePanel}
            title="Click to open live PnL Explain in side panel"
            style={{
                position: 'fixed',
                left: `${popoverAnchor.x}px`,
                top: `${popoverAnchor.y}px`,
                zIndex: 10050,
                width: '254px',
                padding: '9px 11px',
                borderRadius: '10px',
                background: popoverBg,
                border: `1px solid ${popoverBorder}`,
                boxShadow: '0 14px 36px rgba(15,23,42,0.42)',
                color: popoverText,
                fontSize: '11px',
                textAlign: 'left',
                pointerEvents: 'auto',
                cursor: 'pointer',
                fontVariantNumeric: 'tabular-nums',
            }}
        >
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    marginBottom: '6px',
                    paddingBottom: '5px',
                    borderBottom: `1px solid ${popoverBorder}`,
                }}
            >
                <span style={{ fontSize: '10px', fontWeight: 700, color: popoverText, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '152px' }}>
                    {rowLabel || rowKey.replace(/\|\|\|/g, ' › ')}
                </span>
                <span
                    data-pnl-explain-dock-btn="true"
                    style={{
                        fontSize: '10px',
                        fontWeight: 700,
                        color: String((theme && theme.primary) || '#3b82f6'),
                        whiteSpace: 'nowrap',
                        flexShrink: 0,
                    }}
                >
                    Open side ↗
                </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                <span style={{ color: popoverSec }}>Current</span>
                <strong>{formatScalarPlain(meta.value)}</strong>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                <span style={{ color: popoverSec }}>Last Tick</span>
                <span style={{ color: deltaColor, fontWeight: 600 }}>
                    {meta.tickDelta !== null && meta.tickDelta !== undefined
                        ? formatCompactSigned(meta.tickDelta)
                        : '0'}
                </span>
            </div>
            {status.activeBaselineId && (
                <>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: popoverSec }}>{status.activeBaselineLabel || 'Baseline T₀'}</span>
                        <span>{formatScalarPlain(meta.baselineValue)}</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ color: popoverSec }}>Cumulative Δ</span>
                        <span style={{ color: deltaColor, fontWeight: 700 }}>
                            {formatCompactSigned(meta.baselineDelta)} ({formatPctOrPp(meta.baselinePct, meta.deltaUnit)})
                        </span>
                    </div>
                </>
            )}
            {status.showSparklineTooltip && meta.sparklineHistory.length >= 2 && (
                <div style={{ marginTop: '6px', paddingTop: '5px', borderTop: `1px solid ${popoverBorder}` }}>
                    <div style={{ fontSize: '10px', color: popoverSec }}>
                        Live Trajectory ({meta.sparklineHistory.length} ticks)
                    </div>
                    {renderMiniSparkline(meta.sparklineHistory, isDark)}
                </div>
            )}
            {status.showPnlExplain && meta.varianceDrivers.length > 0 && (
                <div style={{ marginTop: '6px', paddingTop: '5px', borderTop: `1px solid ${popoverBorder}` }}>
                    <div style={{ fontSize: '10px', fontWeight: 700, color: popoverSec, marginBottom: '3px', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                        Top Drivers (PnL Explain)
                    </div>
                    {meta.varianceDrivers.slice(0, 3).map((d) => {
                        const dNum = decodeWireNumeric(d.delta);
                        const dCol = dNum !== null && dNum < 0
                            ? (isDark ? '#fb7185' : '#dc2626')
                            : (isDark ? '#34d399' : '#059669');
                        return (
                            <div key={d.path} style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', marginTop: '2px' }}>
                                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{d.label}</span>
                                <span style={{ color: dCol, fontWeight: 600, flexShrink: 0 }}>{formatCompactSigned(d.delta)}</span>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    ) : null;

    return (
        <span
            ref={hostRef}
            data-live-cell="true"
            data-tick-dir={meta.tickDir || undefined}
            data-freshness={softStale || meta.softStale ? 'syncing' : 'live'}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={scheduleDismiss}
            style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'flex-end',
                width: '100%',
                borderRadius: '3px',
                padding: '1px 2px',
                textDecoration: rowLifecycle === 'removed' ? 'line-through' : undefined,
                opacity: rowLifecycle === 'removed' ? 0.62 : 1,
            }}
        >
            {primaryRendered}
            {popoverNode && typeof document !== 'undefined' && document.body
                ? ReactDOM.createPortal(popoverNode, document.body)
                : popoverNode}
        </span>
    );
});

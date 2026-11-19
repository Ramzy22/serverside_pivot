/**
 * Reactive External Viewport Subscription Store & Transport Orchestrator (Component 6).
 *
 * Design principles:
 * - Single Value Truth: incoming PATCH cell values are written directly into `useRowCache`
 *   (via `onPatchRows`) while visual diff/tick/sparkline metadata lives in this external store.
 * - Per-Cell Granular Subscription: each rendered leaf cell subscribes ONLY to its own
 *   `cellKey = `${rowId}::${colId}`` so ticking 3 cells triggers 3 leaf re-renders, never a
 *   full-table re-render.
 * - React 16/17/18/19 compatible (`useSyncExternalStore` when present, safe state fallback otherwise).
 * - Unified across all 3 transports:
 *   1. FastAPI SSE (`POST /v1/runtime/stream` + `POST /v1/runtime/subscription`)
 *   2. Dash Same-Origin Fast-Lane (`POST /_serverside_pivot/subscription`)
 *   3. Client-Side Engine (`ClientPivotTransport.getSubscriptionEngine()`)
 */

import React, { useEffect, useRef, useState } from 'react';
import {
    decodeWireNumeric,
    type BaselineCellDiff,
    type BaselineDescriptor,
    type CellLiveMeta,
    type ConnectionStatus,
    type DiffConfig,
    type DiffDisplayMode,
    type PinnedExplainCell,
    type ProtocolV2Frame,
    type RowLifecycleKind,
    type SortLockMode,
    type StreamCellPatch,
    type StreamingConfig,
    type StreamUpdateBatch,
    type SubscriptionStatusSnapshot,
    type VarianceDriver,
} from './streamingTypes';
import type { ClientPivotTransport } from './client/clientPivotTransport';
import type { NormalizedValConfig } from './client/types';

const MAX_SPARKLINE_POINTS = 30;

export function makeLiveCellKey(rowKey: string, colId: string): string {
    return `${rowKey}::${colId}`;
}

function appendSparklinePoint(prev: readonly number[], rawValue: unknown): number[] {
    const num = decodeWireNumeric(rawValue);
    if (num === null) return prev.slice();
    const next = prev.length >= MAX_SPARKLINE_POINTS ? prev.slice(prev.length - MAX_SPARKLINE_POINTS + 1) : prev.slice();
    next.push(num);
    return next;
}

/** React 16/17/18/19 safe external-store subscription hook. */
export function useExternalStoreSnapshot<T>(
    subscribe: (listener: () => void) => () => void,
    getSnapshot: () => T,
): T {
    const reactAny = React as unknown as {
        useSyncExternalStore?: <S>(sub: (l: () => void) => () => void, snap: () => S, serverSnap?: () => S) => S;
    };
    if (typeof reactAny.useSyncExternalStore === 'function') {
        return reactAny.useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
    }
    const [value, setValue] = useState<T>(getSnapshot);
    useEffect(() => {
        setValue(getSnapshot());
        return subscribe(() => {
            setValue(getSnapshot());
        });
    }, [subscribe, getSnapshot]);
    return value;
}

export interface ViewportSubscriptionCallbacks {
    onPatchRows?: (
        patchesByRowKey: Map<string, Record<string, unknown>>,
        sourceRevision: number,
        visibleRowKeys: Set<string>,
    ) => void;
    onAuthoritativeSnapshot?: (frame: ProtocolV2Frame) => void;
    onTriggerViewportRefresh?: () => void;
}

export interface SubscriptionQueryContext {
    readonly table: string;
    readonly rowFields: readonly string[];
    readonly colFields: readonly string[];
    readonly valConfigs: readonly NormalizedValConfig[];
    readonly filters?: Record<string, unknown>;
    readonly sorting?: readonly { id: string; desc?: boolean }[];
    readonly expanded?: boolean | Record<string, unknown>;
    readonly viewportStart?: number;
    readonly viewportEnd?: number;
}

/**
 * Imperative external store managing per-cell tick/diff metadata, rolling sparklines,
 * row lifecycle badges, and toolbar control state.
 */
export class ViewportSubscriptionStore {
    private cellMap: Map<string, CellLiveMeta>;
    private rowLifecycleMap: Map<string, RowLifecycleKind>;
    private currentViewportRowKeys: Set<string>;
    private changedRowKeys: Set<string>;
    private cellListeners: Map<string, Set<() => void>>;
    private rowListeners: Map<string, Set<() => void>>;
    private statusListeners: Set<() => void>;
    private status: SubscriptionStatusSnapshot;
    private callbacks: ViewportSubscriptionCallbacks;

    // Transport control delegates wired by the active hook
    private controlHandlers: {
        pause?: () => void;
        resume?: () => void;
        freeze?: () => void;
        unfreeze?: () => void;
        captureBaseline?: (label?: string) => void;
        selectBaseline?: (baselineId: string | null) => void;
        setCadence?: (ms: number) => void;
        setSortLock?: (mode: SortLockMode) => void;
        setChangedOnly?: (enabled: boolean) => void;
        modifyViewport?: () => void;
        reconnect?: () => void;
    };
    private transitionInFlight: boolean;

    public constructor(initial?: Partial<SubscriptionStatusSnapshot>) {
        this.cellMap = new Map();
        this.rowLifecycleMap = new Map();
        this.currentViewportRowKeys = new Set();
        this.changedRowKeys = new Set();
        this.cellListeners = new Map();
        this.rowListeners = new Map();
        this.statusListeners = new Set();
        this.callbacks = {};
        this.controlHandlers = {};
        this.transitionInFlight = false;
        this.status = {
            active: false,
            connectionStatus: 'idle',
            capabilityTier: 'tier_1_exact_cdc',
            supportsExactRowDeltas: true,
            subscriptionId: null,
            serverEpochId: null,
            sourceRevision: 0,
            lastAckSeq: 0,
            queryEpoch: 0,
            paused: false,
            bufferedUpdates: 0,
            frozenSnapshotId: null,
            frozenRevision: null,
            cadenceMs: 250,
            sortLock: 'in_place',
            diffDisplayMode: 'value',
            changedOnly: false,
            activeBaselineId: null,
            activeBaselineLabel: null,
            baselines: [],
            lastError: null,
            lastFrameAt: null,
            tickCount: 0,
            flashEnabled: true,
            flashDurationMs: 550,
            showSparklineTooltip: true,
            showPnlExplain: true,
            pinnedExplainCell: null,
            ...(initial || {}),
        };
    }

    public setCallbacks(callbacks: ViewportSubscriptionCallbacks): void {
        this.callbacks = callbacks;
    }

    public setControlHandlers(handlers: ViewportSubscriptionStore['controlHandlers']): void {
        this.controlHandlers = handlers;
    }

    public setTransitionInFlight(inFlight: boolean): void {
        this.transitionInFlight = Boolean(inFlight);
    }

    public isTransitionInFlight(): boolean {
        return this.transitionInFlight;
    }

    /**
     * Clear cell/row maps on structural query changes (filter, sort, dimensions)
     * so cells immediately display the authoritative new grid response without
     * lingering pre-filter/pre-sort values.
     */
    public resetForQueryChange(): void {
        this.cellMap.clear();
        this.rowLifecycleMap.clear();
        this.currentViewportRowKeys.clear();
        this.changedRowKeys.clear();
        this.cellListeners.forEach((set) => {
            set.forEach((fn) => fn());
        });
        this.rowListeners.forEach((set) => {
            set.forEach((fn) => fn());
        });
    }

    public syncViewport = (): void => {
        if (this.controlHandlers.modifyViewport) {
            this.controlHandlers.modifyViewport();
        }
    };

    public getStatus = (): SubscriptionStatusSnapshot => this.status;

    public subscribeStatus = (listener: () => void): (() => void) => {
        this.statusListeners.add(listener);
        return () => {
            this.statusListeners.delete(listener);
        };
    };

    public getCellMeta(rowKey: string, colId: string): CellLiveMeta | null {
        return this.cellMap.get(makeLiveCellKey(rowKey, colId)) ?? null;
    }

    public subscribeCell(rowKey: string, colId: string, listener: () => void): () => void {
        const key = makeLiveCellKey(rowKey, colId);
        let set = this.cellListeners.get(key);
        if (!set) {
            set = new Set();
            this.cellListeners.set(key, set);
        }
        set.add(listener);
        return () => {
            const current = this.cellListeners.get(key);
            if (!current) return;
            current.delete(listener);
            if (current.size === 0) this.cellListeners.delete(key);
        };
    }

    public getRowLifecycle(rowKey: string): RowLifecycleKind {
        return this.rowLifecycleMap.get(rowKey) ?? 'normal';
    }

    public hasRowChanged(rowKey: string): boolean {
        if (!rowKey) return false;
        return this.changedRowKeys.has(rowKey);
    }

    public getChangedRowKeyCount(): number {
        return this.changedRowKeys.size;
    }

    public subscribeRow(rowKey: string, listener: () => void): () => void {
        let set = this.rowListeners.get(rowKey);
        if (!set) {
            set = new Set();
            this.rowListeners.set(rowKey, set);
        }
        set.add(listener);
        return () => {
            const current = this.rowListeners.get(rowKey);
            if (!current) return;
            current.delete(listener);
            if (current.size === 0) this.rowListeners.delete(rowKey);
        };
    }

    public patchStatus(partial: Partial<SubscriptionStatusSnapshot>): void {
        this.status = { ...this.status, ...partial };
        this.statusListeners.forEach((fn) => fn());
    }

    private emitCell(key: string): void {
        const set = this.cellListeners.get(key);
        if (set) {
            set.forEach((fn) => fn());
        }
    }

    private emitRow(rowKey: string): void {
        const set = this.rowListeners.get(rowKey);
        if (set) {
            set.forEach((fn) => fn());
        }
    }

    public setDiffDisplayMode = (mode: DiffDisplayMode): void => {
        if (this.status.diffDisplayMode === mode) return;
        this.patchStatus({ diffDisplayMode: mode });
        // Notify all subscribed cells so they re-render with the new display mode
        this.cellListeners.forEach((set) => {
            set.forEach((fn) => fn());
        });
    };

    public pauseLive = (): void => {
        if (this.controlHandlers.pause) this.controlHandlers.pause();
    };

    public resumeLive = (): void => {
        if (this.controlHandlers.resume) this.controlHandlers.resume();
    };

    public freezeSnapshot = (): void => {
        if (this.controlHandlers.freeze) this.controlHandlers.freeze();
    };

    public unfreezeSnapshot = (): void => {
        if (this.controlHandlers.unfreeze) this.controlHandlers.unfreeze();
    };

    public captureBaseline = (label?: string): void => {
        if (this.controlHandlers.captureBaseline) this.controlHandlers.captureBaseline(label);
    };

    public selectBaseline = (baselineId: string | null): void => {
        if (this.controlHandlers.selectBaseline) this.controlHandlers.selectBaseline(baselineId);
    };

    public setCadenceMs = (ms: number): void => {
        this.patchStatus({ cadenceMs: ms });
        if (this.controlHandlers.setCadence) this.controlHandlers.setCadence(ms);
    };

    public setSortLock = (mode: SortLockMode): void => {
        this.patchStatus({ sortLock: mode });
        if (this.controlHandlers.setSortLock) this.controlHandlers.setSortLock(mode);
    };

    public setChangedOnly = (enabled: boolean): void => {
        this.patchStatus({ changedOnly: enabled });
        if (this.controlHandlers.setChangedOnly) this.controlHandlers.setChangedOnly(enabled);
    };

    public reconnect = (): void => {
        if (this.controlHandlers.reconnect) this.controlHandlers.reconnect();
    };

    public openExplainPanel = (cell: PinnedExplainCell): void => {
        this.patchStatus({ pinnedExplainCell: cell });
    };

    public closeExplainPanel = (): void => {
        if (this.status.pinnedExplainCell === null) return;
        this.patchStatus({ pinnedExplainCell: null });
    };

    /** Ingest any Protocol V2 frame (`SNAPSHOT`, `PATCH`, `HEARTBEAT`, `INVALIDATE`, `AUTH_REVOKED`). */
    public ingestFrame(frame: ProtocolV2Frame): void {
        const now = Date.now();
        if (!frame || typeof frame !== 'object') return;

        if (frame.type === 'AUTH_REVOKED') {
            this.patchStatus({
                connectionStatus: 'auth_revoked',
                lastError: frame.reason || 'Authorization revoked mid-stream',
                lastFrameAt: now,
            });
            return;
        }

        if (frame.type === 'ERROR') {
            this.patchStatus({
                connectionStatus: 'error',
                lastError: frame.error || frame.reason || 'Streaming error',
                lastFrameAt: now,
            });
            return;
        }

        if (frame.type === 'INVALIDATE') {
            this.patchStatus({
                connectionStatus: 'resyncing',
                lastError: null,
                lastFrameAt: now,
            });
            if (this.callbacks.onTriggerViewportRefresh) {
                this.callbacks.onTriggerViewportRefresh();
            }
            return;
        }

        const rawFrame = frame as unknown as Record<string, unknown>;
        const freshnessMode = typeof rawFrame.freshnessMode === 'string'
            ? (rawFrame.freshnessMode as SubscriptionStatusSnapshot['capabilityTier'])
            : undefined;
        const rawPauseMode = typeof rawFrame.pauseMode === 'string'
            ? rawFrame.pauseMode.trim().toLowerCase()
            : undefined;
        const resolvedPaused = frame.paused !== undefined
            ? Boolean(frame.paused)
            : (rawPauseMode !== undefined
                ? rawPauseMode === 'pause_live'
                : this.status.paused);
        const resolvedFrozenId = frame.frozenSnapshotId !== undefined
            ? frame.frozenSnapshotId
            : (rawPauseMode === 'active' || rawPauseMode === 'pause_live'
                ? null
                : this.status.frozenSnapshotId);
        const resolvedTier = frame.capabilityTier ?? freshnessMode ?? this.status.capabilityTier;
        const isExactTier = resolvedTier === 'exact_cdc' || resolvedTier === 'tier_1_exact_cdc';
        const nextStatusBase: Partial<SubscriptionStatusSnapshot> = {
            subscriptionId: frame.subscriptionId ?? this.status.subscriptionId,
            serverEpochId: frame.serverEpochId ?? this.status.serverEpochId,
            sourceRevision: typeof frame.sourceRevision === 'number' ? frame.sourceRevision : this.status.sourceRevision,
            queryEpoch: typeof frame.queryEpoch === 'number' ? frame.queryEpoch : this.status.queryEpoch,
            lastAckSeq: typeof frame.seq === 'number' ? Math.max(this.status.lastAckSeq, frame.seq) : this.status.lastAckSeq,
            paused: resolvedPaused,
            bufferedUpdates: typeof frame.bufferedUpdates === 'number' ? frame.bufferedUpdates : this.status.bufferedUpdates,
            frozenSnapshotId: resolvedFrozenId,
            activeBaselineId: frame.baselineId !== undefined ? frame.baselineId : this.status.activeBaselineId,
            activeBaselineLabel: frame.baselineLabel !== undefined ? frame.baselineLabel : this.status.activeBaselineLabel,
            capabilityTier: resolvedTier,
            supportsExactRowDeltas: frame.supportsExactRowDeltas !== undefined
                ? Boolean(frame.supportsExactRowDeltas)
                : (freshnessMode ? isExactTier : this.status.supportsExactRowDeltas),
            lastError: null,
            lastFrameAt: now,
        };

        const resolvedConnection: ConnectionStatus = nextStatusBase.frozenSnapshotId
            ? 'frozen'
            : (nextStatusBase.paused ? 'paused' : 'live');

        if (frame.type === 'HEARTBEAT') {
            this.patchStatus({
                ...nextStatusBase,
                connectionStatus: resolvedConnection,
            });
            return;
        }

        if (frame.type === 'SNAPSHOT') {
            const rows = Array.isArray(frame.rows) ? frame.rows : [];
            const touchedCells = new Set<string>();
            const touchedRows = new Set<string>();
            const patchesByRowKey = new Map<string, Record<string, unknown>>();
            const visibleKeys = new Set<string>();
            const nextChangedKeys = new Set<string>();

            const ingestRow = (row: Record<string, unknown>) => {
                const displayKey = String(row._path || row._id || row._canonicalPath || '');
                const canonicalKey = String(row._canonicalPath || displayKey);
                if (!displayKey && !canonicalKey) return;
                const keysToIndex = Array.from(new Set([displayKey, canonicalKey].filter(Boolean)));
                keysToIndex.forEach((k) => visibleKeys.add(k));

                const rawLifecycle = row._lifecycle ?? row._diffStatus;
                const lifecycle: RowLifecycleKind = rawLifecycle === 'new'
                    ? 'new'
                    : (rawLifecycle === 'removed' ? 'removed' : 'normal');
                keysToIndex.forEach((k) => {
                    if (this.rowLifecycleMap.get(k) !== lifecycle) {
                        this.rowLifecycleMap.set(k, lifecycle);
                        touchedRows.add(k);
                    }
                });

                const baselineDiffs = (row._baselineDiff && typeof row._baselineDiff === 'object')
                    ? (row._baselineDiff as Record<string, BaselineCellDiff>)
                    : null;
                const srvBaseline = (row._baseline && typeof row._baseline === 'object')
                    ? (row._baseline as Record<string, unknown>)
                    : null;
                const srvDelta = (row._delta && typeof row._delta === 'object')
                    ? (row._delta as Record<string, unknown>)
                    : null;
                const srvPctDelta = (row._pctDelta && typeof row._pctDelta === 'object')
                    ? (row._pctDelta as Record<string, unknown>)
                    : null;
                const srvDeltaUnit = (row._deltaUnit && typeof row._deltaUnit === 'object')
                    ? (row._deltaUnit as Record<string, unknown>)
                    : null;
                const driversMap = (row._varianceDrivers && typeof row._varianceDrivers === 'object')
                    ? (row._varianceDrivers as Record<string, readonly VarianceDriver[]>)
                    : null;

                let rowHasNonZeroDelta = lifecycle === 'new' || lifecycle === 'removed';
                const rowPatchObj: Record<string, unknown> = {};
                Object.keys(row).forEach((colId) => {
                    if (colId.startsWith('_') || colId === 'depth') return;
                    const val = row[colId];
                    const numericVal = decodeWireNumeric(val);
                    rowPatchObj[colId] = numericVal !== null ? numericVal : val;
                    const bd = baselineDiffs ? baselineDiffs[colId] : undefined;
                    const bVal = bd ? bd.baselineValue : (srvBaseline ? (srvBaseline[colId] ?? null) : null);
                    const bDelta = bd ? bd.delta : (srvDelta ? (srvDelta[colId] ?? null) : null);
                    const bDeltaNum = decodeWireNumeric(bDelta);
                    if (bDeltaNum !== null && Math.abs(bDeltaNum) > 1e-9) {
                        rowHasNonZeroDelta = true;
                    }
                    const bPctRaw = bd ? bd.pctDelta : (srvPctDelta ? srvPctDelta[colId] : null);
                    const bPct = typeof bPctRaw === 'number' && Number.isFinite(bPctRaw) ? bPctRaw : null;
                    const dUnit: 'abs' | 'pp' = (bd?.deltaUnit === 'pp' || (srvDeltaUnit && srvDeltaUnit[colId] === 'pp'))
                        ? 'pp'
                        : 'abs';
                    const drivers = driversMap ? (driversMap[colId] || []) : [];

                    keysToIndex.forEach((rk) => {
                        const cellKey = makeLiveCellKey(rk, colId);
                        const prev = this.cellMap.get(cellKey);
                        const nextHistory = appendSparklinePoint(prev ? prev.sparklineHistory : [], val);
                        const nextNum = decodeWireNumeric(val);
                        const prevNum = prev ? decodeWireNumeric(prev.value) : null;
                        const computedTickDelta = prev && nextNum !== null && prevNum !== null && nextNum !== prevNum
                            ? nextNum - prevNum
                            : (prev ? prev.tickDelta : null);
                        const meta: CellLiveMeta = {
                            rowKey: rk,
                            colId,
                            value: val,
                            prevValue: prev ? prev.value : val,
                            tickSeq: prev ? prev.tickSeq : 0,
                            tickDir: prev ? prev.tickDir : null,
                            tickDelta: computedTickDelta,
                            tickPct: prev ? prev.tickPct : null,
                            baselineValue: bVal,
                            baselineDelta: bDelta,
                            baselinePct: bPct,
                            deltaUnit: dUnit,
                            sparklineHistory: nextHistory,
                            varianceDrivers: drivers,
                            rowLifecycle: lifecycle,
                            softStale: false,
                            updatedAt: now,
                        };
                        this.cellMap.set(cellKey, meta);
                        touchedCells.add(cellKey);
                    });
                });

                if (rowHasNonZeroDelta || rawLifecycle === 'changed') {
                    keysToIndex.forEach((rk) => nextChangedKeys.add(rk));
                }
                keysToIndex.forEach((rk) => {
                    patchesByRowKey.set(rk, rowPatchObj);
                });
            };

            rows.forEach(ingestRow);
            if (frame.grandTotal && typeof frame.grandTotal === 'object') {
                const gt = frame.grandTotal;
                ingestRow(gt);
                // Ensure Grand Total is also indexed under all canonical/display aliases
                ['__grand_total__', '__GRAND_TOTAL__', 'Grand Total', '[]'].forEach((alias) => {
                    ingestRow({ ...gt, _path: alias, _canonicalPath: alias });
                    nextChangedKeys.add(alias);
                });
            }

            this.currentViewportRowKeys = visibleKeys;
            this.changedRowKeys = nextChangedKeys;
            if (!this.transitionInFlight && this.callbacks.onPatchRows && patchesByRowKey.size > 0) {
                this.callbacks.onPatchRows(
                    patchesByRowKey,
                    nextStatusBase.sourceRevision ?? this.status.sourceRevision,
                    this.currentViewportRowKeys,
                );
            }
            if (this.callbacks.onAuthoritativeSnapshot) {
                this.callbacks.onAuthoritativeSnapshot(frame);
            }

            this.patchStatus({
                ...nextStatusBase,
                connectionStatus: resolvedConnection,
                frozenRevision: nextStatusBase.frozenSnapshotId
                    ? (nextStatusBase.sourceRevision ?? this.status.sourceRevision)
                    : null,
            });
            touchedCells.forEach((k) => this.emitCell(k));
            touchedRows.forEach((rk) => this.emitRow(rk));
            return;
        }

        if (frame.type === 'PATCH') {
            const cellPatches = Array.isArray(frame.cellPatches) ? frame.cellPatches : [];
            const touchedCells = new Set<string>();
            const patchesByRowKey = new Map<string, Record<string, unknown>>();
            const visibleKeys = new Set<string>();

            const applyCellPatch = (patch: StreamCellPatch) => {
                const rawP = patch as unknown as Record<string, unknown>;
                const resolvedColId = patch.colId || (typeof rawP.columnId === 'string' ? rawP.columnId : '');
                const keysToIndex = Array.from(new Set([
                    patch.displayPath || (typeof rawP.rowId === 'string' ? rawP.rowId : ''),
                    patch.rowKey || (typeof rawP.canonicalPath === 'string' ? rawP.canonicalPath : ''),
                ].filter(Boolean)));
                if (keysToIndex.length === 0 || !resolvedColId) return;

                keysToIndex.forEach((rk) => {
                    visibleKeys.add(rk);
                    const rowObj = patchesByRowKey.get(rk) || {};
                    const numericPatchVal = decodeWireNumeric(patch.value);
                    rowObj[resolvedColId] = numericPatchVal !== null ? numericPatchVal : patch.value;
                    patchesByRowKey.set(rk, rowObj);

                    const cellKey = makeLiveCellKey(rk, resolvedColId);
                    const prev = this.cellMap.get(cellKey);
                    const prevVal = patch.prevValue !== undefined ? patch.prevValue : (prev ? prev.value : null);
                    const nextNum = decodeWireNumeric(patch.value);
                    const prevNum = decodeWireNumeric(prevVal);
                    const wireTickDelta = patch.delta !== undefined ? patch.delta : rawP.tickDelta;
                    const tickDelta = wireTickDelta !== undefined
                        ? wireTickDelta
                        : (nextNum !== null && prevNum !== null ? nextNum - prevNum : null);
                    const tickDeltaNum = decodeWireNumeric(tickDelta);
                    const rawDir = patch.direction ?? rawP.tickDir;
                    const tickDir: 'up' | 'down' | null = rawDir === 'up'
                        ? 'up'
                        : (rawDir === 'down'
                            ? 'down'
                            : (tickDeltaNum !== null && tickDeltaNum !== 0
                                ? (tickDeltaNum > 0 ? 'up' : 'down')
                                : null));

                    const bd = patch.baseline;
                    const bVal = bd ? bd.baselineValue : (rawP.baselineValue !== undefined ? rawP.baselineValue : (prev ? prev.baselineValue : null));
                    const bDelta = bd ? bd.delta : (rawP.baselineDelta !== undefined ? rawP.baselineDelta : (prev ? prev.baselineDelta : null));
                    const rawBPct = bd ? bd.pctDelta : rawP.baselinePct;
                    const bPct = typeof rawBPct === 'number' && Number.isFinite(rawBPct)
                        ? rawBPct
                        : (prev ? prev.baselinePct : null);
                    const dUnit: 'abs' | 'pp' = (bd?.deltaUnit === 'pp' || rawP.deltaUnit === 'pp')
                        ? 'pp'
                        : (prev ? prev.deltaUnit : 'abs');
                    const rawTickPct = patch.pctDelta !== undefined ? patch.pctDelta : rawP.tickPct;
                    const tickPct = typeof rawTickPct === 'number' && Number.isFinite(rawTickPct) ? rawTickPct : null;
                    const drivers = Array.isArray(rawP.varianceDrivers)
                        ? (rawP.varianceDrivers as VarianceDriver[])
                        : (prev ? prev.varianceDrivers : []);

                    const meta: CellLiveMeta = {
                        rowKey: rk,
                        colId: resolvedColId,
                        value: patch.value,
                        prevValue: prevVal,
                        tickSeq: (prev ? prev.tickSeq : 0) + (tickDir !== null || patch.value !== prevVal ? 1 : 0),
                        tickDir,
                        tickDelta,
                        tickPct,
                        baselineValue: bVal,
                        baselineDelta: bDelta,
                        baselinePct: bPct,
                        deltaUnit: dUnit,
                        sparklineHistory: appendSparklinePoint(prev ? prev.sparklineHistory : [], patch.value),
                        varianceDrivers: drivers,
                        rowLifecycle: this.rowLifecycleMap.get(rk) ?? 'normal',
                        softStale: false,
                        updatedAt: now,
                    };
                    this.cellMap.set(cellKey, meta);
                    touchedCells.add(cellKey);
                    const bDeltaCheck = decodeWireNumeric(bDelta);
                    if ((bDeltaCheck !== null && Math.abs(bDeltaCheck) > 1e-9) || tickDir !== null) {
                        this.changedRowKeys.add(rk);
                    }
                });
            };

            cellPatches.forEach(applyCellPatch);

            // Ingest Python server `grandTotalPatch` map (per-column tick + baseline diff for Grand Total)
            const gtPatchMap = rawFrame.grandTotalPatch && typeof rawFrame.grandTotalPatch === 'object'
                ? (rawFrame.grandTotalPatch as Record<string, Record<string, unknown>>)
                : null;
            if (gtPatchMap) {
                Object.keys(gtPatchMap).forEach((colId) => {
                    const entry = gtPatchMap[colId];
                    if (!entry || typeof entry !== 'object') return;
                    const baseObj = entry.baselineValue !== undefined || entry.baselineDelta !== undefined
                        ? {
                            baselineValue: entry.baselineValue,
                            delta: entry.baselineDelta,
                            pctDelta: typeof entry.baselinePct === 'number' ? entry.baselinePct : null,
                            deltaUnit: (entry.deltaUnit === 'pp' ? 'pp' : 'abs') as 'abs' | 'pp',
                        }
                        : null;
                    ['__grand_total__', '__GRAND_TOTAL__', 'Grand Total', '[]'].forEach((alias) => {
                        applyCellPatch({
                            rowKey: alias,
                            displayPath: alias,
                            colId,
                            value: entry.value,
                            prevValue: entry.prevValue,
                            delta: entry.tickDelta,
                            pctDelta: typeof entry.tickPct === 'number' ? entry.tickPct : null,
                            direction: (entry.tickDir === 'up' || entry.tickDir === 'down' ? entry.tickDir : null),
                            baseline: baseObj,
                        });
                    });
                });
            }

            // Also ingest grandTotal variance drivers / baseline diffs if attached
            if (frame.grandTotal && typeof frame.grandTotal === 'object') {
                const gt = frame.grandTotal;
                const gtKey = String(gt._path || gt._id || '__GRAND_TOTAL__');
                const rowObj = patchesByRowKey.get(gtKey) || {};
                Object.keys(gt).forEach((colId) => {
                    if (colId.startsWith('_') || colId === 'depth') return;
                    const gNum = decodeWireNumeric(gt[colId]);
                    rowObj[colId] = gNum !== null ? gNum : gt[colId];
                });
                patchesByRowKey.set(gtKey, rowObj);
                visibleKeys.add(gtKey);
            }

            if (!this.transitionInFlight && this.callbacks.onPatchRows && patchesByRowKey.size > 0) {
                const knownViewportKeys = this.currentViewportRowKeys.size > 0
                    ? this.currentViewportRowKeys
                    : visibleKeys;
                this.callbacks.onPatchRows(
                    patchesByRowKey,
                    nextStatusBase.sourceRevision ?? this.status.sourceRevision,
                    knownViewportKeys,
                );
            }

            this.patchStatus({
                ...nextStatusBase,
                connectionStatus: resolvedConnection,
                tickCount: this.status.tickCount + (cellPatches.length > 0 ? 1 : 0),
            });
            touchedCells.forEach((k) => this.emitCell(k));
        }
    }
}

export interface UseViewportSubscriptionOptions {
    readonly streamingConfig?: StreamingConfig | null;
    readonly diffConfig?: DiffConfig | null;
    readonly streamUpdates?: StreamUpdateBatch | null;
    readonly serverSide: boolean;
    readonly clientTransport?: ClientPivotTransport | null;
    readonly transitionInFlight?: boolean;
    readonly queryContext: SubscriptionQueryContext;
    readonly callbacks?: ViewportSubscriptionCallbacks;
}

/**
 * React hook owning a stable `ViewportSubscriptionStore` and binding it to
 * FastAPI SSE, Dash fast-lane, or ClientPivotTransport depending on runtime mode.
 */
export function useViewportSubscriptionStore(
    options: UseViewportSubscriptionOptions,
): ViewportSubscriptionStore {
    const {
        streamingConfig,
        diffConfig,
        streamUpdates,
        serverSide,
        clientTransport,
        transitionInFlight = false,
        queryContext,
        callbacks,
    } = options;

    const streamingEnabled = Boolean(streamingConfig?.enabled || streamUpdates);
    const diffEnabled = Boolean(diffConfig?.enabled || diffConfig?.baselineId);
    const isActive = streamingEnabled || diffEnabled;

    const storeRef = useRef<ViewportSubscriptionStore | null>(null);
    if (!storeRef.current) {
        storeRef.current = new ViewportSubscriptionStore({
            active: isActive,
            cadenceMs: streamingConfig?.cadenceMs ?? 250,
            sortLock: streamingConfig?.sortLock ?? 'in_place',
            diffDisplayMode: diffConfig?.displayMode ?? 'value',
            changedOnly: Boolean(diffConfig?.changedOnly),
            activeBaselineId: diffConfig?.baselineId ?? null,
            activeBaselineLabel: diffConfig?.baselineLabel ?? null,
            flashEnabled: streamingConfig?.flashEnabled !== false,
            flashDurationMs: streamingConfig?.flashDurationMs ?? 550,
            showSparklineTooltip: streamingConfig?.showSparklineTooltip !== false,
            showPnlExplain: diffConfig?.showPnlExplain !== false,
        });
    }
    const store = storeRef.current;

    const latestQueryContextRef = useRef<SubscriptionQueryContext>(queryContext);
    latestQueryContextRef.current = queryContext;

    const transitionInFlightRef = useRef<boolean>(Boolean(transitionInFlight));
    transitionInFlightRef.current = Boolean(transitionInFlight);
    store.setTransitionInFlight(Boolean(transitionInFlight));

    const pendingStartRef = useRef<(() => void) | null>(null);

    useEffect(() => {
        store.setCallbacks(callbacks || {});
    }, [store, callbacks]);

    useEffect(() => {
        store.patchStatus({
            active: isActive,
            flashEnabled: streamingConfig?.flashEnabled !== false,
            flashDurationMs: streamingConfig?.flashDurationMs ?? 550,
            showSparklineTooltip: streamingConfig?.showSparklineTooltip !== false,
            showPnlExplain: diffConfig?.showPnlExplain !== false,
        });
    }, [
        store,
        isActive,
        streamingConfig?.flashEnabled,
        streamingConfig?.flashDurationMs,
        streamingConfig?.showSparklineTooltip,
        diffConfig?.showPnlExplain,
    ]);

    // Apply declarative `streamUpdates` prop batches in client-side mode
    const lastBatchRef = useRef<StreamUpdateBatch | null>(null);
    useEffect(() => {
        if (!streamUpdates || streamUpdates === lastBatchRef.current) return;
        lastBatchRef.current = streamUpdates;
        if (!serverSide && clientTransport && typeof clientTransport.applyStreamUpdates === 'function') {
            clientTransport.applyStreamUpdates({
                ...streamUpdates,
                keyFields: streamUpdates.keyFields || streamingConfig?.keyFields,
            });
        }
    }, [streamUpdates, serverSide, clientTransport, streamingConfig?.keyFields]);

    // Query shape signature so we re-subscribe cleanly on structural changes
    const querySignature = JSON.stringify({
        table: queryContext.table,
        rowFields: queryContext.rowFields,
        colFields: queryContext.colFields,
        valConfigs: queryContext.valConfigs.map((v) => ({
            id: v.id,
            field: v.field,
            agg: v.agg,
            weightField: v.weightField,
            windowFn: v.windowFn,
            formula: v.formula,
        })),
        filters: queryContext.filters || {},
        sorting: queryContext.sorting || [],
    });

    // Viewport & expansion signature so expand/collapse/scroll updates the active subscription via modify_viewport
    const viewportSignature = JSON.stringify({
        expanded: queryContext.expanded || {},
        viewportStart: queryContext.viewportStart ?? 0,
        viewportEnd: queryContext.viewportEnd ?? 100,
    });
    const lastSyncedViewportSigRef = useRef<string>(viewportSignature);

    const lastQuerySignatureRef = useRef<string>(querySignature);
    if (querySignature !== lastQuerySignatureRef.current) {
        lastQuerySignatureRef.current = querySignature;
        store.resetForQueryChange();
    }

    const resolvedTransport = streamingConfig?.transport && streamingConfig.transport !== 'auto'
        ? streamingConfig.transport
        : (!serverSide
            ? 'client'
            : (streamingConfig?.streamEndpoint ? 'sse' : 'dash'));

    useEffect(() => {
        if (!isActive || !queryContext.table) {
            store.patchStatus({ active: false, connectionStatus: 'idle' });
            return undefined;
        }

        // --- Mode 1: Client-Side Engine (DuckDB-Wasm / In-Memory) ---
        if (resolvedTransport === 'client' && clientTransport) {
            const engine = clientTransport.getSubscriptionEngine();
            const shape = {
                table: queryContext.table,
                rowFields: queryContext.rowFields,
                colFields: queryContext.colFields,
                valConfigs: queryContext.valConfigs,
                sorting: queryContext.sorting,
            };
            const cur = store.getStatus();
            const { subscription, initialFrame } = engine.subscribe(shape, {
                baselineId: cur.activeBaselineId,
                sortLock: cur.sortLock,
                changedOnly: cur.changedOnly,
                listener: (frame) => {
                    store.ingestFrame(frame);
                    if (typeof frame.seq === 'number') {
                        engine.acknowledge(subscription.subscriptionId, frame.seq);
                    }
                },
            });
            store.ingestFrame(initialFrame);
            store.patchStatus({ baselines: engine.listBaselines() });

            store.setControlHandlers({
                pause: () => {
                    const frame = engine.pause(subscription.subscriptionId);
                    if (frame) store.ingestFrame(frame);
                },
                resume: () => {
                    const frame = engine.resume(subscription.subscriptionId);
                    if (frame) store.ingestFrame(frame);
                },
                freeze: () => {
                    const res = engine.freeze(subscription.subscriptionId);
                    if (res) store.ingestFrame(res.frame);
                },
                unfreeze: () => {
                    const frame = engine.unfreeze(subscription.subscriptionId);
                    if (frame) store.ingestFrame(frame);
                },
                captureBaseline: (label) => {
                    const desc = engine.captureSnapshot(label, 'baseline');
                    store.patchStatus({
                        baselines: engine.listBaselines(),
                        activeBaselineId: desc.snapshotId,
                        activeBaselineLabel: desc.label,
                    });
                    const frame = engine.setBaseline(subscription.subscriptionId, desc.snapshotId);
                    if (frame) store.ingestFrame(frame);
                },
                selectBaseline: (baselineId) => {
                    const found = engine.listBaselines().find((b) => b.snapshotId === baselineId) || null;
                    store.patchStatus({
                        activeBaselineId: baselineId,
                        activeBaselineLabel: found ? found.label : null,
                    });
                    const frame = engine.setBaseline(subscription.subscriptionId, baselineId);
                    if (frame) store.ingestFrame(frame);
                },
                setSortLock: (mode) => {
                    const frame = engine.updateOptions(subscription.subscriptionId, { sortLock: mode });
                    if (frame) store.ingestFrame(frame);
                },
                setChangedOnly: (enabled) => {
                    const frame = engine.updateOptions(subscription.subscriptionId, { changedOnly: enabled });
                    if (frame) store.ingestFrame(frame);
                },
            });

            return () => {
                engine.unsubscribe(subscription.subscriptionId);
            };
        }

        // --- Mode 2 & 3: Server-Side FastAPI SSE or Dash Fast-Lane ---
        if (typeof fetch !== 'function') return undefined;

        const abortController = typeof AbortController !== 'undefined' ? new AbortController() : null;
        let disposed = false;
        let activeSubId: string | null = null;
        let pollTimer: ReturnType<typeof setTimeout> | null = null;
        let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
        let reconnectAttempts = 0;

        const controlEndpoint = resolvedTransport === 'sse'
            ? (streamingConfig?.subscriptionEndpoint || '/v1/runtime/subscription')
            : (streamingConfig?.dashEndpoint || '/_serverside_pivot/subscription');
        const sseEndpoint = streamingConfig?.streamEndpoint || '/v1/runtime/stream';
        const extraHeaders = streamingConfig?.headers || {};

        const postControl = async (body: Record<string, unknown>): Promise<Record<string, unknown> | null> => {
            const resp = await fetch(controlEndpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', ...extraHeaders },
                body: JSON.stringify(body),
                credentials: 'same-origin',
                signal: abortController?.signal,
            });
            if (resp.status === 403 || resp.status === 401) {
                store.ingestFrame({ type: 'AUTH_REVOKED', reason: `HTTP ${resp.status}` });
                return null;
            }
            if (resp.status === 410) {
                const err = new Error('Snapshot or baseline expired (HTTP 410)');
                (err as Error & { status?: number }).status = 410;
                throw err;
            }
            if (!resp.ok) {
                throw new Error(`Subscription control HTTP ${resp.status}`);
            }
            return (await resp.json()) as Record<string, unknown>;
        };

        const buildStatePayload = () => {
            const ctx = latestQueryContextRef.current;
            return {
                rowFields: ctx.rowFields,
                colFields: ctx.colFields,
                valConfigs: ctx.valConfigs.map((v) => ({
                    id: v.id || `${v.field}_${v.agg || 'sum'}`,
                    field: v.field,
                    agg: v.agg || 'sum',
                    weightField: v.weightField || null,
                    windowFn: v.windowFn || null,
                    formula: v.formula || null,
                    format: v.format || null,
                })),
                filters: ctx.filters || {},
                sorting: ctx.sorting || [],
                expanded: ctx.expanded || {},
            };
        };

        const buildViewportPayload = (overrides: Record<string, unknown> = {}) => {
            const ctx = latestQueryContextRef.current;
            const cur = store.getStatus();
            return {
                startRow: ctx.viewportStart ?? 0,
                endRow: ctx.viewportEnd ?? 100,
                baselineId: cur.activeBaselineId,
                sortMode: cur.sortLock === 'in_place' ? 'in_place' : 'live_sort',
                changedOnly: cur.changedOnly,
                cadenceMs: cur.cadenceMs,
                ...overrides,
            };
        };

        const refreshBaselinesList = async () => {
            try {
                const res = await postControl({ action: 'list_baselines', table: queryContext.table });
                const rawList = res && (Array.isArray(res.snapshots) ? res.snapshots : (Array.isArray(res.baselines) ? res.baselines : null));
                if (rawList) {
                    store.patchStatus({
                        baselines: (rawList as Record<string, unknown>[]).map((b) => ({
                            snapshotId: String(b.snapshotId || b.snapshot_id || b.baselineId || ''),
                            label: String(b.label || ''),
                            kind: (b.kind === 'freeze_snapshot' ? 'freeze_snapshot' : 'baseline'),
                            sourceRevision: Number(b.sourceRevision ?? b.source_revision ?? 0),
                            strategy: typeof b.strategy === 'string' ? b.strategy : undefined,
                            createdAt: Number(b.createdAt ?? b.created_at ?? Date.now()),
                            immutableVerified: Boolean(b.immutableVerified ?? b.immutable_verified ?? true),
                        })),
                    });
                }
            } catch {
                // Non-fatal baseline list refresh
            }
        };

        const startServerSubscription = async () => {
            store.patchStatus({ connectionStatus: 'connecting', lastError: null });
            try {
                const subscribeBody = {
                    table: queryContext.table,
                    action: 'subscribe',
                    state: buildStatePayload(),
                    payload: buildViewportPayload(),
                };

                if (resolvedTransport === 'sse') {
                    // Open POST SSE stream directly (atomic bootstrap + live push)
                    const resp = await fetch(sseEndpoint, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            Accept: 'text/event-stream',
                            ...extraHeaders,
                        },
                        body: JSON.stringify(subscribeBody),
                        credentials: 'same-origin',
                        signal: abortController?.signal,
                    });
                    if (resp.status === 403 || resp.status === 401) {
                        store.ingestFrame({ type: 'AUTH_REVOKED', reason: `HTTP ${resp.status}` });
                        return;
                    }
                    if (!resp.ok || !resp.body) {
                        throw new Error(`SSE stream HTTP ${resp.status}`);
                    }
                    refreshBaselinesList();

                    const reader = resp.body.getReader();
                    const decoder = new TextDecoder();
                    let buffer = '';
                    while (!disposed) {
                        const { done, value } = await reader.read();
                        if (done) break;
                        buffer += decoder.decode(value, { stream: true });
                        const chunks = buffer.split('\n\n');
                        buffer = chunks.pop() || '';
                        for (let i = 0; i < chunks.length; i += 1) {
                            const lines = chunks[i].split('\n');
                            const dataLines: string[] = [];
                            for (let j = 0; j < lines.length; j += 1) {
                                if (lines[j].startsWith('data:')) {
                                    dataLines.push(lines[j].slice(5).trim());
                                }
                            }
                            if (dataLines.length === 0) continue;
                            try {
                                const parsed = JSON.parse(dataLines.join('\n')) as ProtocolV2Frame;
                                if (parsed.subscriptionId) activeSubId = parsed.subscriptionId;
                                store.ingestFrame(parsed);
                                if (activeSubId && typeof parsed.seq === 'number' && parsed.type !== 'HEARTBEAT') {
                                    // Fire-and-forget application-level ACK for backpressure conflation
                                    postControl({
                                        table: queryContext.table,
                                        action: 'ack',
                                        payload: {
                                            subscriptionId: activeSubId,
                                            seq: parsed.seq,
                                        },
                                    }).catch(() => undefined);
                                }
                                if (parsed.type === 'AUTH_REVOKED') {
                                    reader.cancel().catch(() => undefined);
                                    return;
                                }
                            } catch {
                                // Ignore malformed SSE keep-alive comment
                            }
                        }
                    }
                } else {
                    // Dash same-origin fast-lane polling loop (bypasses Dash's callback prop serialization!)
                    const initRes = await postControl(subscribeBody);
                    if (disposed || !initRes) return;
                    const initFrame = (initRes.frame || initRes) as unknown as ProtocolV2Frame;
                    activeSubId = initFrame.subscriptionId || (typeof initRes.subscriptionId === 'string' ? initRes.subscriptionId : null);
                    lastSyncedViewportSigRef.current = JSON.stringify({
                        expanded: latestQueryContextRef.current.expanded || {},
                        viewportStart: latestQueryContextRef.current.viewportStart ?? 0,
                        viewportEnd: latestQueryContextRef.current.viewportEnd ?? 100,
                    });
                    store.ingestFrame(initFrame);
                    refreshBaselinesList();

                    const schedulePoll = () => {
                        if (disposed || !activeSubId) return;
                        const delay = Math.max(80, store.getStatus().cadenceMs || 250);
                        pollTimer = setTimeout(async () => {
                            if (disposed || !activeSubId) return;
                            // Suspend polling while a structural/expansion action is in flight so
                            // user actions never contend with live ticks.
                            if (transitionInFlightRef.current) {
                                schedulePoll();
                                return;
                            }
                            try {
                                const curStatus = store.getStatus();
                                const res = await postControl({
                                    table: latestQueryContextRef.current.table,
                                    action: 'poll',
                                    payload: {
                                        subscriptionId: activeSubId,
                                        ackSeq: curStatus.lastAckSeq,
                                        serverEpochId: curStatus.serverEpochId,
                                    },
                                });
                                if (disposed || !res) return;
                                const frame = (res.frame || res) as unknown as ProtocolV2Frame;
                                store.ingestFrame(frame);
                                if (frame.type === 'AUTH_REVOKED') return;
                                if (frame.type === 'INVALIDATE') {
                                    startServerSubscription();
                                    return;
                                }
                            } catch (err) {
                                if (!disposed && !(err instanceof Error && err.name === 'AbortError')) {
                                    store.patchStatus({
                                        connectionStatus: 'error',
                                        lastError: err instanceof Error ? err.message : String(err),
                                    });
                                }
                            }
                            schedulePoll();
                        }, delay);
                    };
                    schedulePoll();
                }
            } catch (err) {
                if (!disposed && !(err instanceof Error && err.name === 'AbortError')) {
                    const statusCode = (err as Error & { status?: number })?.status;
                    // Graceful fail-closed recovery on HTTP 410 (expired baseline/frozen snapshot):
                    // clear the expired snapshot reference and immediately resubscribe live!
                    if (statusCode === 410 && (store.getStatus().activeBaselineId || store.getStatus().frozenSnapshotId)) {
                        store.patchStatus({
                            activeBaselineId: null,
                            activeBaselineLabel: null,
                            frozenSnapshotId: null,
                            frozenRevision: null,
                            lastError: null,
                        });
                        refreshBaselinesList();
                        startServerSubscription();
                        return;
                    }
                    store.patchStatus({
                        connectionStatus: 'error',
                        lastError: err instanceof Error ? err.message : String(err),
                    });
                    if (reconnectAttempts < 5) {
                        const backoffMs = Math.min(8000, 1500 * Math.pow(2, reconnectAttempts));
                        reconnectAttempts += 1;
                        if (reconnectTimer) clearTimeout(reconnectTimer);
                        reconnectTimer = setTimeout(() => {
                            if (!disposed) startServerSubscription();
                        }, backoffMs);
                    }
                }
            }
        };

        store.setControlHandlers({
            pause: () => {
                if (!activeSubId) return;
                postControl({ table: queryContext.table, action: 'pause', payload: { subscriptionId: activeSubId } })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            resume: () => {
                if (!activeSubId) return;
                postControl({ table: queryContext.table, action: 'resume', payload: { subscriptionId: activeSubId } })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            freeze: () => {
                if (!activeSubId) return;
                postControl({ table: queryContext.table, action: 'freeze', payload: { subscriptionId: activeSubId } })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            unfreeze: () => {
                if (!activeSubId) return;
                postControl({ table: queryContext.table, action: 'unfreeze', payload: { subscriptionId: activeSubId } })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            captureBaseline: (label) => {
                postControl({ table: queryContext.table, action: 'capture_baseline', payload: { label } })
                    .then(async (res) => {
                        await refreshBaselinesList();
                        const snap = (res && (res.snapshot || res)) as Record<string, unknown> | null;
                        const snapId = snap ? String(snap.snapshotId || snap.snapshot_id || snap.baselineId || '') : '';
                        const snapLabel = snap && typeof snap.label === 'string' ? snap.label : (label || 'Baseline T₀');
                        if (snapId) {
                            store.patchStatus({
                                activeBaselineId: snapId,
                                activeBaselineLabel: snapLabel,
                            });
                            if (activeSubId) {
                                const updated = await postControl({
                                    table: queryContext.table,
                                    action: 'modify_viewport',
                                    state: buildStatePayload(),
                                    payload: buildViewportPayload({ subscriptionId: activeSubId, baselineId: snapId }),
                                });
                                if (updated) {
                                    store.ingestFrame((updated.frame || updated) as unknown as ProtocolV2Frame);
                                }
                            } else {
                                startServerSubscription();
                            }
                        }
                    })
                    .catch(() => undefined);
            },
            selectBaseline: (baselineId) => {
                const found = store.getStatus().baselines.find((b) => b.snapshotId === baselineId) || null;
                store.patchStatus({
                    activeBaselineId: baselineId,
                    activeBaselineLabel: found ? found.label : null,
                });
                if (!activeSubId) {
                    startServerSubscription();
                    return;
                }
                postControl({
                    table: queryContext.table,
                    action: 'modify_viewport',
                    state: buildStatePayload(),
                    payload: buildViewportPayload({ subscriptionId: activeSubId, baselineId }),
                })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            setSortLock: (mode) => {
                if (!activeSubId) return;
                postControl({
                    table: queryContext.table,
                    action: 'modify_viewport',
                    state: buildStatePayload(),
                    payload: buildViewportPayload({
                        subscriptionId: activeSubId,
                        sortMode: mode === 'in_place' ? 'in_place' : 'live_sort',
                    }),
                })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            setChangedOnly: (enabled) => {
                if (!activeSubId) return;
                postControl({
                    table: latestQueryContextRef.current.table,
                    action: 'modify_viewport',
                    state: buildStatePayload(),
                    payload: buildViewportPayload({
                        subscriptionId: activeSubId,
                        changedOnly: enabled,
                    }),
                })
                    .then((res) => { if (res) store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame); })
                    .catch(() => undefined);
            },
            modifyViewport: () => {
                if (disposed || !activeSubId || transitionInFlightRef.current) return;
                postControl({
                    table: latestQueryContextRef.current.table,
                    action: 'modify_viewport',
                    state: buildStatePayload(),
                    payload: buildViewportPayload({
                        subscriptionId: activeSubId,
                    }),
                })
                    .then((res) => {
                        if (!disposed && res) {
                            store.ingestFrame((res.frame || res) as unknown as ProtocolV2Frame);
                        }
                    })
                    .catch(() => undefined);
            },
            reconnect: () => {
                if (pollTimer) clearTimeout(pollTimer);
                if (reconnectTimer) clearTimeout(reconnectTimer);
                reconnectAttempts = 0;
                startServerSubscription();
            },
        });

        if (transitionInFlightRef.current) {
            pendingStartRef.current = () => {
                if (!disposed) {
                    startServerSubscription();
                }
            };
        } else {
            pendingStartRef.current = null;
            startServerSubscription();
        }

        return () => {
            disposed = true;
            pendingStartRef.current = null;
            if (pollTimer) clearTimeout(pollTimer);
            if (reconnectTimer) clearTimeout(reconnectTimer);
            if (abortController) abortController.abort();
            if (activeSubId) {
                fetch(controlEndpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', ...extraHeaders },
                    body: JSON.stringify({
                        table: latestQueryContextRef.current.table,
                        action: 'unsubscribe',
                        payload: { subscriptionId: activeSubId },
                    }),
                    credentials: 'same-origin',
                    keepalive: true,
                }).catch(() => undefined);
            }
        };
    }, [
        isActive,
        resolvedTransport,
        clientTransport,
        querySignature,
        store,
        streamingConfig?.subscriptionEndpoint,
        streamingConfig?.streamEndpoint,
        streamingConfig?.dashEndpoint,
    ]);

    // Flush deferred subscription start or viewport/expansion sync once in-flight structural/expansion actions complete
    useEffect(() => {
        if (!isActive || transitionInFlight) return;
        if (pendingStartRef.current) {
            const startFn = pendingStartRef.current;
            pendingStartRef.current = null;
            startFn();
            return;
        }
        if (viewportSignature !== lastSyncedViewportSigRef.current) {
            lastSyncedViewportSigRef.current = viewportSignature;
            store.syncViewport();
        }
    }, [isActive, transitionInFlight, viewportSignature, store]);

    return store;
}

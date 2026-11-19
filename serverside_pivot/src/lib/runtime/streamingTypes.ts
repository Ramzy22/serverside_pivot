/**
 * Protocol V2 streaming & baseline diff contracts shared across:
 * - Server-side FastAPI SSE (`/v1/runtime/stream` + `/v1/runtime/subscription`)
 * - Server-side Dash same-origin fast-lane (`/_serverside_pivot/subscription`)
 * - Client-side in-browser engine (`ClientSubscriptionEngine` over DuckDB-Wasm / memory)
 */

export type DiffDisplayMode = 'value' | 'delta' | 'pctDelta' | 'split';

export type SortLockMode = 'in_place' | 'live_rank';

export type SourceCapabilityTier =
    | 'exact_cdc'
    | 'tier_1_exact_cdc'
    | 'change_token'
    | 'tier_2_change_token'
    | 'time_travel_only'
    | 'tier_3_time_travel_only'
    | 'periodic_refresh'
    | 'tier_4_periodic_refresh';

export type SubscriptionTransportKind = 'auto' | 'sse' | 'dash' | 'client';

export interface WireDecimal {
    readonly __decimal__: string;
    readonly scale?: number;
}

export function isWireDecimal(value: unknown): value is WireDecimal {
    return (
        value !== null
        && typeof value === 'object'
        && !Array.isArray(value)
        && typeof (value as { __decimal__?: unknown }).__decimal__ === 'string'
    );
}

/** Decode a Protocol V2 wire value into a JS number (for charts/bars) or keep primitive. */
export function decodeWireNumeric(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    if (isWireDecimal(value)) {
        const parsed = Number(value.__decimal__);
        return Number.isFinite(parsed) ? parsed : null;
    }
    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (!trimmed) return null;
        const parsed = Number(trimmed);
        return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
}

/** Format an exact WireDecimal string with optional grouping without IEEE-754 rounding loss. */
export function formatWireDecimalString(wire: WireDecimal, decimalPlaces?: number | null): string {
    const raw = wire.__decimal__.trim();
    const neg = raw.startsWith('-');
    const unsigned = neg ? raw.slice(1) : raw;
    const dotIdx = unsigned.indexOf('.');
    const intPart = dotIdx >= 0 ? unsigned.slice(0, dotIdx) : unsigned;
    let fracPart = dotIdx >= 0 ? unsigned.slice(dotIdx + 1) : '';
    const groupedInt = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    if (typeof decimalPlaces === 'number' && Number.isFinite(decimalPlaces) && decimalPlaces >= 0) {
        const d = Math.floor(decimalPlaces);
        if (d === 0) return `${neg ? '-' : ''}${groupedInt}`;
        if (fracPart.length < d) {
            fracPart = fracPart.padEnd(d, '0');
        } else if (fracPart.length > d) {
            fracPart = fracPart.slice(0, d);
        }
    }
    return fracPart ? `${neg ? '-' : ''}${groupedInt}.${fracPart}` : `${neg ? '-' : ''}${groupedInt}`;
}

export interface StreamingConfig {
    readonly enabled?: boolean;
    readonly transport?: SubscriptionTransportKind;
    /** FastAPI subscription endpoint (default `/v1/runtime/subscription`). */
    readonly subscriptionEndpoint?: string;
    /** FastAPI SSE endpoint (default `/v1/runtime/stream`). */
    readonly streamEndpoint?: string;
    /** Dash fast-lane endpoint (default `/_serverside_pivot/subscription`). */
    readonly dashEndpoint?: string;
    /** Flush / poll cadence in milliseconds (default 250). */
    readonly cadenceMs?: number;
    /** Default sort lock mode (`in_place` prevents row jumping during live ticks). */
    readonly sortLock?: SortLockMode;
    /** Enable GPU directional flash animations on cell ticks (default true). */
    readonly flashEnabled?: boolean;
    /** Flash duration in milliseconds (default 550). */
    readonly flashDurationMs?: number;
    /** Show live rolling 30-point sparkline inside cell hover tooltip (default true). */
    readonly showSparklineTooltip?: boolean;
    /** Primary key fields for client-side stream mutation batches. */
    readonly keyFields?: readonly string[];
    /** Optional HTTP headers forwarded to FastAPI endpoints. */
    readonly headers?: Readonly<Record<string, string>>;
}

export interface DiffConfig {
    readonly enabled?: boolean;
    readonly baselineId?: string | null;
    readonly baselineLabel?: string | null;
    readonly displayMode?: DiffDisplayMode;
    readonly changedOnly?: boolean;
    readonly showPnlExplain?: boolean;
}

export interface StreamUpdateBatch {
    readonly mode?: 'insert' | 'update' | 'upsert' | 'delete';
    readonly rows?: readonly Record<string, unknown>[];
    readonly keyFields?: readonly string[];
    readonly batchId?: string | number;
}

export interface VarianceDriver {
    readonly path: string;
    readonly label: string;
    readonly delta: unknown;
    readonly pctDelta?: number | null;
}

export interface BaselineCellDiff {
    readonly baselineValue: unknown;
    readonly delta: unknown;
    readonly pctDelta: number | null;
    readonly deltaUnit?: 'abs' | 'pp';
}

export interface StreamCellPatch {
    readonly rowKey: string;
    readonly displayPath?: string;
    readonly colId: string;
    readonly value: unknown;
    readonly prevValue?: unknown;
    readonly delta?: unknown;
    readonly pctDelta?: number | null;
    readonly direction?: 'up' | 'down' | 'flat' | null;
    readonly baseline?: BaselineCellDiff | null;
}

export interface StreamRowPatch {
    readonly op: 'insert' | 'update' | 'remove';
    readonly rowKey: string;
    readonly displayPath?: string;
    readonly row?: Record<string, unknown>;
    readonly index?: number;
}

export interface BaselineDescriptor {
    readonly snapshotId: string;
    readonly label: string;
    readonly kind: 'baseline' | 'freeze_snapshot';
    readonly sourceRevision: number;
    readonly schemaVersion?: string;
    readonly strategy?: string;
    readonly createdAt: number;
    readonly expiresAt?: number;
    readonly immutableVerified?: boolean;
}

export interface ProtocolV2Frame {
    readonly type: 'SNAPSHOT' | 'PATCH' | 'HEARTBEAT' | 'INVALIDATE' | 'AUTH_REVOKED' | 'ERROR';
    readonly protocolVersion?: number;
    readonly subscriptionId?: string;
    readonly serverEpochId?: string;
    readonly queryEpoch?: number;
    readonly schemaVersion?: string;
    readonly sourceRevision?: number;
    readonly seq?: number;
    readonly baseSeq?: number;
    readonly totalRows?: number;
    readonly rows?: readonly Record<string, unknown>[];
    readonly grandTotal?: Record<string, unknown> | null;
    readonly cellPatches?: readonly StreamCellPatch[];
    readonly rowPatches?: readonly StreamRowPatch[];
    readonly bufferedUpdates?: number;
    readonly paused?: boolean;
    readonly frozenSnapshotId?: string | null;
    readonly baselineId?: string | null;
    readonly baselineLabel?: string | null;
    readonly sortLock?: SortLockMode;
    readonly capabilityTier?: SourceCapabilityTier;
    readonly supportsExactRowDeltas?: boolean;
    readonly reason?: string;
    readonly error?: string;
    readonly timestamp?: number;
}

export type RowLifecycleKind = 'normal' | 'new' | 'removed';

export interface CellLiveMeta {
    readonly rowKey: string;
    readonly colId: string;
    readonly value: unknown;
    readonly prevValue: unknown;
    readonly tickSeq: number;
    readonly tickDir: 'up' | 'down' | null;
    readonly tickDelta: unknown;
    readonly tickPct: number | null;
    readonly baselineValue: unknown;
    readonly baselineDelta: unknown;
    readonly baselinePct: number | null;
    readonly deltaUnit: 'abs' | 'pp';
    readonly sparklineHistory: readonly number[];
    readonly varianceDrivers: readonly VarianceDriver[];
    readonly rowLifecycle: RowLifecycleKind;
    readonly softStale: boolean;
    readonly updatedAt: number;
}

export type ConnectionStatus =
    | 'idle'
    | 'connecting'
    | 'live'
    | 'paused'
    | 'frozen'
    | 'resyncing'
    | 'auth_revoked'
    | 'error';

export interface PinnedExplainCell {
    readonly rowKey: string;
    readonly colId: string;
    readonly rowLabel: string;
    readonly colLabel: string;
}

export interface SubscriptionStatusSnapshot {
    readonly active: boolean;
    readonly connectionStatus: ConnectionStatus;
    readonly capabilityTier: SourceCapabilityTier;
    readonly supportsExactRowDeltas: boolean;
    readonly subscriptionId: string | null;
    readonly serverEpochId: string | null;
    readonly sourceRevision: number;
    readonly lastAckSeq: number;
    readonly queryEpoch: number;
    readonly paused: boolean;
    readonly bufferedUpdates: number;
    readonly frozenSnapshotId: string | null;
    readonly frozenRevision: number | null;
    readonly cadenceMs: number;
    readonly sortLock: SortLockMode;
    readonly diffDisplayMode: DiffDisplayMode;
    readonly changedOnly: boolean;
    readonly activeBaselineId: string | null;
    readonly activeBaselineLabel: string | null;
    readonly baselines: readonly BaselineDescriptor[];
    readonly lastError: string | null;
    readonly lastFrameAt: number | null;
    readonly tickCount: number;
    readonly flashEnabled: boolean;
    readonly flashDurationMs: number;
    readonly showSparklineTooltip: boolean;
    readonly showPnlExplain: boolean;
    readonly pinnedExplainCell: PinnedExplainCell | null;
}

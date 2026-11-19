/**
 * Client-side implementation of the Protocol V2 Snapshot + Diff Subscription Engine.
 *
 * Mirrors the Python backend's exact mathematical & lifecycle contracts in the browser
 * for `ClientPivotTransport` (in-memory & DuckDB-Wasm execution modes):
 * - Monotonic `sourceRevision`, `eventSeq` (`baseSeq -> seq`), `queryEpoch`, `serverEpochId`
 * - Exact valid-masked weighted averages (`x != null && w != null`)
 * - Measure-specific removed-group diff rules (`SUM`/`COUNT` -> 0 / -B / -100%; `AVG`/`WAVG`/`MIN`/`MAX`/ratio -> null)
 * - `0 -> 0` safe percentage delta (`delta = 0`, `pctDelta = null`)
 * - Top-3 child variance attribution (`_varianceDrivers` for PnL Explain)
 * - Distinct `Pause Live` (visual hold + buffered updates) vs `Freeze Snapshot` (immutable frozen view)
 * - `Sort Lock` (`in_place` vs `live_rank`) & `changedOnly` filtering
 */

import type {
    BaselineCellDiff,
    BaselineDescriptor,
    ProtocolV2Frame,
    SortLockMode,
    StreamCellPatch,
    StreamRowPatch,
    StreamUpdateBatch,
    VarianceDriver,
} from '../streamingTypes';
import type { NormalizedValConfig } from './types';

export interface ClientQueryShape {
    readonly table: string;
    readonly rowFields: readonly string[];
    readonly colFields: readonly string[];
    readonly valConfigs: readonly NormalizedValConfig[];
    readonly sorting?: readonly { id: string; desc?: boolean }[];
}

interface SufficientAcc {
    rowCount: number;
    nnCount: number;
    sum: number;
    wsum: number;
    weight: number;
    min: number | null;
    max: number | null;
}

interface GroupAggRecord {
    readonly rowKey: string;
    readonly displayPath: string;
    readonly depth: number;
    readonly pathValues: readonly unknown[];
    readonly label: string;
    readonly values: Record<string, number | null>;
    readonly rowCount: number;
}

interface RetainedSnapshot {
    readonly descriptor: BaselineDescriptor;
    readonly rows: readonly Record<string, unknown>[];
}

export interface ClientSubscriptionState {
    readonly subscriptionId: string;
    shape: ClientQueryShape;
    seq: number;
    lastAckSeq: number;
    queryEpoch: number;
    paused: boolean;
    bufferedUpdates: number;
    frozenSnapshotId: string | null;
    baselineId: string | null;
    sortLock: SortLockMode;
    changedOnly: boolean;
    pinnedRowOrder: string[];
    lastRowsByKey: Map<string, Record<string, unknown>>;
    lastGrandTotal: Record<string, unknown> | null;
    listener?: (frame: ProtocolV2Frame) => void;
}

const GRAND_TOTAL_KEY = '__GRAND_TOTAL__';

function makeId(prefix: string): string {
    return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function encodeCanonicalPath(values: readonly unknown[]): string {
    const typed = values.map((v) => {
        if (v === null || v === undefined) return ['null', null];
        if (typeof v === 'boolean') return ['bool', v];
        if (typeof v === 'number') return ['num', v];
        return ['str', String(v)];
    });
    return JSON.stringify(typed);
}

function displayPath(values: readonly unknown[]): string {
    if (values.length === 0) return GRAND_TOTAL_KEY;
    return values.map((v) => (v === null || v === undefined ? '' : String(v))).join('|||');
}

function toFinite(v: unknown): number | null {
    if (v === null || v === undefined || v === '') return null;
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function makeEmptyAcc(): SufficientAcc {
    return { rowCount: 0, nnCount: 0, sum: 0, wsum: 0, weight: 0, min: null, max: null };
}

function isAdditiveZeroOnDelete(agg: string): boolean {
    const norm = (agg || 'sum').trim().toLowerCase();
    return norm === 'sum' || norm === 'count';
}

function isRatioMeasure(vc: NormalizedValConfig): boolean {
    const agg = (vc.agg || '').trim().toLowerCase();
    const fmt = (vc.format || '').trim().toLowerCase();
    return Boolean(vc.windowFn) || fmt.includes('percent') || agg === 'formula';
}

function computeCellValue(acc: SufficientAcc, vc: NormalizedValConfig): number | null {
    const agg = (vc.agg || 'sum').trim().toLowerCase();
    if (agg === 'count') return acc.nnCount;
    if (agg === 'sum') return acc.nnCount > 0 ? acc.sum : null;
    if (agg === 'avg' || agg === 'mean') return acc.nnCount > 0 ? acc.sum / acc.nnCount : null;
    if (agg === 'wavg' || agg === 'weighted_avg' || agg === 'weighted_mean') {
        if (acc.nnCount === 0 || acc.weight === 0) return null;
        return acc.wsum / acc.weight;
    }
    if (agg === 'min') return acc.min;
    if (agg === 'max') return acc.max;
    return acc.nnCount > 0 ? acc.sum : null;
}

function measureColumnId(colKey: string, vc: NormalizedValConfig): string {
    const alias = vc.id || `${vc.field}_${vc.agg || 'sum'}`;
    return colKey ? `${colKey}_${alias}` : alias;
}

function extractColKey(row: Record<string, unknown>, colFields: readonly string[]): string {
    if (colFields.length === 0) return '';
    return colFields.map((f) => {
        const v = row[f];
        return v === null || v === undefined ? '' : String(v);
    }).join('_');
}

function aggregateRowsHierarchy(
    rawRows: readonly Record<string, unknown>[],
    shape: ClientQueryShape,
): {
    groups: Map<string, GroupAggRecord>;
    grandTotal: Record<string, unknown>;
    colIds: string[];
    measureByColId: Map<string, NormalizedValConfig>;
} {
    const rowFields = shape.rowFields || [];
    const colFields = shape.colFields || [];
    const valConfigs = shape.valConfigs || [];
    const accMap = new Map<string, {
        pathValues: unknown[];
        depth: number;
        rowCount: number;
        cells: Map<string, SufficientAcc>;
    }>();
    const colIdSet = new Set<string>();
    const measureByColId = new Map<string, NormalizedValConfig>();

    const ensureNode = (pathValues: unknown[], depth: number) => {
        const key = pathValues.length === 0 ? GRAND_TOTAL_KEY : encodeCanonicalPath(pathValues);
        let node = accMap.get(key);
        if (!node) {
            node = { pathValues, depth, rowCount: 0, cells: new Map() };
            accMap.set(key, node);
        }
        return node;
    };

    ensureNode([], 0);

    for (let rIdx = 0; rIdx < rawRows.length; rIdx += 1) {
        const row = rawRows[rIdx];
        if (!row || typeof row !== 'object') continue;
        const colKey = extractColKey(row, colFields);

        // Update Grand Total (depth 0) + every ancestor depth 1..rowFields.length
        for (let depth = 0; depth <= rowFields.length; depth += 1) {
            const pathValues: unknown[] = [];
            for (let d = 0; d < depth; d += 1) {
                pathValues.push(row[rowFields[d]] ?? null);
            }
            const node = ensureNode(pathValues, depth);
            node.rowCount += 1;

            for (let mIdx = 0; mIdx < valConfigs.length; mIdx += 1) {
                const vc = valConfigs[mIdx];
                const colId = measureColumnId(colKey, vc);
                colIdSet.add(colId);
                measureByColId.set(colId, vc);

                let acc = node.cells.get(colId);
                if (!acc) {
                    acc = makeEmptyAcc();
                    node.cells.set(colId, acc);
                }
                acc.rowCount += 1;
                const agg = (vc.agg || 'sum').trim().toLowerCase();
                if (agg === 'count') {
                    const rawVal = vc.field ? row[vc.field] : 1;
                    if (rawVal !== null && rawVal !== undefined) {
                        acc.nnCount += 1;
                    }
                } else if (agg === 'wavg' || agg === 'weighted_avg' || agg === 'weighted_mean') {
                    // Correction 1A: BOTH numerator AND denominator MUST share valid predicate (x != null && w != null).
                    const x = toFinite(row[vc.field]);
                    const w = vc.weightField ? toFinite(row[vc.weightField]) : null;
                    if (x !== null && w !== null) {
                        acc.nnCount += 1;
                        acc.wsum += x * w;
                        acc.weight += w;
                    }
                } else {
                    const x = toFinite(row[vc.field]);
                    if (x !== null) {
                        acc.nnCount += 1;
                        acc.sum += x;
                        if (acc.min === null || x < acc.min) acc.min = x;
                        if (acc.max === null || x > acc.max) acc.max = x;
                    }
                }
            }
        }
    }

    const colIds = Array.from(colIdSet);
    const groups = new Map<string, GroupAggRecord>();
    const grandTotal: Record<string, unknown> = {
        _id: GRAND_TOTAL_KEY,
        _path: GRAND_TOTAL_KEY,
        _canonicalPath: GRAND_TOTAL_KEY,
        _isTotal: true,
        depth: 0,
    };

    accMap.forEach((node, key) => {
        const values: Record<string, number | null> = {};
        colIds.forEach((colId) => {
            const vc = measureByColId.get(colId);
            const acc = node.cells.get(colId);
            values[colId] = vc && acc ? computeCellValue(acc, vc) : null;
        });

        if (key === GRAND_TOTAL_KEY) {
            Object.assign(grandTotal, values);
            return;
        }
        const dPath = displayPath(node.pathValues);
        const lastVal = node.pathValues[node.pathValues.length - 1];
        groups.set(key, {
            rowKey: key,
            displayPath: dPath,
            depth: node.depth,
            pathValues: node.pathValues,
            label: lastVal === null || lastVal === undefined ? '(Blank)' : String(lastVal),
            values,
            rowCount: node.rowCount,
        });
    });

    return { groups, grandTotal, colIds, measureByColId };
}

function buildBaselineDiff(
    currentVal: number | null,
    baseVal: number | null,
    vc: NormalizedValConfig | undefined,
    rowRemoved: boolean,
): BaselineCellDiff {
    const isRatio = vc ? isRatioMeasure(vc) : false;
    const deltaUnit: 'abs' | 'pp' = isRatio ? 'pp' : 'abs';

    if (rowRemoved) {
        if (vc && isAdditiveZeroOnDelete(vc.agg)) {
            const b = baseVal ?? 0;
            const delta = -b;
            const pctDelta = b === 0 ? null : -1.0;
            return { baselineValue: baseVal, delta, pctDelta, deltaUnit };
        }
        return { baselineValue: baseVal, delta: null, pctDelta: null, deltaUnit };
    }

    if (currentVal === null || baseVal === null) {
        return {
            baselineValue: baseVal,
            delta: currentVal !== null && baseVal === null && vc && isAdditiveZeroOnDelete(vc.agg) ? currentVal : null,
            pctDelta: null,
            deltaUnit,
        };
    }

    const delta = currentVal - baseVal;
    const pctDelta = baseVal === 0 ? null : (isRatio ? delta : delta / Math.abs(baseVal));
    return { baselineValue: baseVal, delta, pctDelta, deltaUnit };
}

export class ClientSubscriptionEngine {
    public readonly serverEpochId: string;
    private sourceRevision: number;
    private schemaVersion: string;
    private rows: Record<string, unknown>[];
    private snapshots: Map<string, RetainedSnapshot>;
    private subscriptions: Map<string, ClientSubscriptionState>;
    private onRowsReplaced?: (rows: Record<string, unknown>[]) => void;

    public constructor(
        initialRows: readonly Record<string, unknown>[] = [],
        onRowsReplaced?: (rows: Record<string, unknown>[]) => void,
    ) {
        this.serverEpochId = makeId('client_epoch');
        this.sourceRevision = 1;
        this.rows = initialRows.map((r) => ({ ...r }));
        this.schemaVersion = this.computeSchemaVersion(this.rows);
        this.snapshots = new Map();
        this.subscriptions = new Map();
        this.onRowsReplaced = onRowsReplaced;
    }

    private computeSchemaVersion(rows: readonly Record<string, unknown>[]): string {
        if (!rows || rows.length === 0) return 'empty';
        const keys = Object.keys(rows[0] || {}).sort();
        return keys.join('|');
    }

    public getSourceRevision(): number {
        return this.sourceRevision;
    }

    public setRows(nextRows: readonly Record<string, unknown>[]): void {
        this.rows = nextRows.map((r) => ({ ...r }));
        const nextSchema = this.computeSchemaVersion(this.rows);
        const schemaChanged = nextSchema !== this.schemaVersion;
        this.schemaVersion = nextSchema;
        this.sourceRevision += 1;
        this.broadcastUpdates(schemaChanged);
    }

    public getRows(): Record<string, unknown>[] {
        return this.rows;
    }

    /** Apply an atomic insert/update/upsert/delete batch to the client dataset and notify subscribers. */
    public applyStreamUpdates(batch: StreamUpdateBatch): number {
        const incoming = Array.isArray(batch.rows) ? batch.rows : [];
        if (incoming.length === 0) return this.sourceRevision;

        const mode = batch.mode || 'upsert';
        const keyFields = (batch.keyFields && batch.keyFields.length > 0)
            ? batch.keyFields
            : (Object.prototype.hasOwnProperty.call(incoming[0] || {}, 'id') ? ['id'] : []);

        const makePk = (row: Record<string, unknown>): string | null => {
            if (keyFields.length === 0) return null;
            return JSON.stringify(keyFields.map((k) => row[k] ?? null));
        };

        const nextRows = this.rows.slice();
        if (keyFields.length > 0) {
            const indexByPk = new Map<string, number>();
            for (let i = 0; i < nextRows.length; i += 1) {
                const pk = makePk(nextRows[i]);
                if (pk !== null) indexByPk.set(pk, i);
            }

            for (let i = 0; i < incoming.length; i += 1) {
                const rec = incoming[i];
                if (!rec || typeof rec !== 'object') continue;
                const pk = makePk(rec);
                const existingIdx = pk !== null ? indexByPk.get(pk) : undefined;

                if (mode === 'delete') {
                    if (existingIdx !== undefined) {
                        nextRows[existingIdx] = null as unknown as Record<string, unknown>;
                        if (pk !== null) indexByPk.delete(pk);
                    }
                } else if (mode === 'update') {
                    if (existingIdx !== undefined) {
                        nextRows[existingIdx] = { ...nextRows[existingIdx], ...rec };
                    }
                } else if (mode === 'insert') {
                    const newIdx = nextRows.length;
                    nextRows.push({ ...rec });
                    if (pk !== null) indexByPk.set(pk, newIdx);
                } else {
                    // upsert
                    if (existingIdx !== undefined) {
                        nextRows[existingIdx] = { ...nextRows[existingIdx], ...rec };
                    } else {
                        const newIdx = nextRows.length;
                        nextRows.push({ ...rec });
                        if (pk !== null) indexByPk.set(pk, newIdx);
                    }
                }
            }
            this.rows = nextRows.filter(Boolean);
        } else {
            // Append-only fallback if no keyFields exist
            for (let i = 0; i < incoming.length; i += 1) {
                if (incoming[i] && typeof incoming[i] === 'object') {
                    nextRows.push({ ...incoming[i] });
                }
            }
            this.rows = nextRows;
        }

        const nextSchema = this.computeSchemaVersion(this.rows);
        const schemaChanged = nextSchema !== this.schemaVersion;
        this.schemaVersion = nextSchema;
        this.sourceRevision += 1;
        if (this.onRowsReplaced) {
            this.onRowsReplaced(this.rows);
        }
        this.broadcastUpdates(schemaChanged);
        return this.sourceRevision;
    }

    public captureSnapshot(label?: string, kind: 'baseline' | 'freeze_snapshot' = 'baseline'): BaselineDescriptor {
        const now = Date.now();
        const snapshotId = makeId(kind === 'baseline' ? 'snap' : 'freeze');
        const descriptor: BaselineDescriptor = {
            snapshotId,
            label: label || `${kind === 'baseline' ? 'Baseline' : 'Frozen'} @ Rev ${this.sourceRevision}`,
            kind,
            sourceRevision: this.sourceRevision,
            schemaVersion: this.schemaVersion,
            strategy: 'client_immutable_snapshot',
            createdAt: now,
            immutableVerified: true,
        };
        this.snapshots.set(snapshotId, {
            descriptor,
            rows: this.rows.map((r) => ({ ...r })),
        });
        return descriptor;
    }

    public listBaselines(): BaselineDescriptor[] {
        const result: BaselineDescriptor[] = [];
        this.snapshots.forEach((snap) => {
            if (snap.descriptor.kind === 'baseline') {
                result.push(snap.descriptor);
            }
        });
        return result.sort((a, b) => b.createdAt - a.createdAt);
    }

    public subscribe(
        shape: ClientQueryShape,
        options: {
            baselineId?: string | null;
            sortLock?: SortLockMode;
            changedOnly?: boolean;
            listener?: (frame: ProtocolV2Frame) => void;
        } = {},
    ): { subscription: ClientSubscriptionState; initialFrame: ProtocolV2Frame } {
        const subscriptionId = makeId('csub');
        const sub: ClientSubscriptionState = {
            subscriptionId,
            shape,
            seq: 0,
            lastAckSeq: 0,
            queryEpoch: 1,
            paused: false,
            bufferedUpdates: 0,
            frozenSnapshotId: null,
            baselineId: options.baselineId ?? null,
            sortLock: options.sortLock ?? 'in_place',
            changedOnly: Boolean(options.changedOnly),
            pinnedRowOrder: [],
            lastRowsByKey: new Map(),
            lastGrandTotal: null,
            listener: options.listener,
        };
        this.subscriptions.set(subscriptionId, sub);
        const initialFrame = this.buildSnapshotFrame(sub);
        return { subscription: sub, initialFrame };
    }

    public unsubscribe(subscriptionId: string): void {
        this.subscriptions.delete(subscriptionId);
    }

    public acknowledge(subscriptionId: string, seq: number): void {
        const sub = this.subscriptions.get(subscriptionId);
        if (sub && seq > sub.lastAckSeq) {
            sub.lastAckSeq = seq;
        }
    }

    public pause(subscriptionId: string): ProtocolV2Frame | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        sub.paused = true;
        return this.buildHeartbeatFrame(sub);
    }

    public resume(subscriptionId: string): ProtocolV2Frame | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        sub.paused = false;
        sub.bufferedUpdates = 0;
        return this.buildSnapshotFrame(sub);
    }

    public freeze(subscriptionId: string, label?: string): { descriptor: BaselineDescriptor; frame: ProtocolV2Frame } | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        const descriptor = this.captureSnapshot(label, 'freeze_snapshot');
        sub.frozenSnapshotId = descriptor.snapshotId;
        sub.paused = false;
        sub.bufferedUpdates = 0;
        const frame = this.buildSnapshotFrame(sub);
        return { descriptor, frame };
    }

    public unfreeze(subscriptionId: string): ProtocolV2Frame | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        sub.frozenSnapshotId = null;
        return this.buildSnapshotFrame(sub);
    }

    public setBaseline(subscriptionId: string, baselineId: string | null): ProtocolV2Frame | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        sub.baselineId = baselineId;
        return this.buildSnapshotFrame(sub);
    }

    public updateOptions(
        subscriptionId: string,
        opts: { sortLock?: SortLockMode; changedOnly?: boolean; shape?: ClientQueryShape },
    ): ProtocolV2Frame | null {
        const sub = this.subscriptions.get(subscriptionId);
        if (!sub) return null;
        if (opts.sortLock !== undefined) sub.sortLock = opts.sortLock;
        if (opts.changedOnly !== undefined) sub.changedOnly = opts.changedOnly;
        if (opts.shape !== undefined) {
            sub.shape = opts.shape;
            sub.pinnedRowOrder = [];
        }
        return this.buildSnapshotFrame(sub);
    }

    private materializeRows(sub: ClientSubscriptionState): {
        rows: Record<string, unknown>[];
        grandTotal: Record<string, unknown>;
        sourceRevision: number;
    } {
        const frozenSnap = sub.frozenSnapshotId ? this.snapshots.get(sub.frozenSnapshotId) : undefined;
        const currentRows = frozenSnap ? frozenSnap.rows : this.rows;
        const currentRev = frozenSnap ? frozenSnap.descriptor.sourceRevision : this.sourceRevision;
        const liveAgg = aggregateRowsHierarchy(currentRows, sub.shape);

        const baselineSnap = sub.baselineId ? this.snapshots.get(sub.baselineId) : undefined;
        const baseAgg = baselineSnap ? aggregateRowsHierarchy(baselineSnap.rows, sub.shape) : null;

        // Union of live + baseline row keys (so deleted baseline groups appear as REMOVED)
        const allKeys = new Set<string>();
        liveAgg.groups.forEach((_v, k) => allKeys.add(k));
        if (baseAgg) {
            baseAgg.groups.forEach((_v, k) => allKeys.add(k));
        }
        const allColIds = Array.from(new Set<string>([
            ...liveAgg.colIds,
            ...(baseAgg ? baseAgg.colIds : []),
        ]));

        const assembled: Record<string, unknown>[] = [];
        const deltasByPathAndCol = new Map<string, number>();

        allKeys.forEach((rowKey) => {
            const liveRec = liveAgg.groups.get(rowKey);
            const baseRec = baseAgg ? baseAgg.groups.get(rowKey) : undefined;
            const ref = liveRec || baseRec;
            if (!ref) return;

            const isRemoved = !liveRec && Boolean(baseRec);
            const isNew = Boolean(liveRec) && Boolean(baseAgg) && !baseRec;
            const outRow: Record<string, unknown> = {
                _id: ref.displayPath,
                _path: ref.displayPath,
                _canonicalPath: rowKey,
                depth: ref.depth,
                _label: ref.label,
                _lifecycle: isRemoved ? 'removed' : (isNew ? 'new' : 'normal'),
            };
            // Populate row dimension fields for TanStack accessors
            for (let d = 0; d < sub.shape.rowFields.length; d += 1) {
                outRow[sub.shape.rowFields[d]] = ref.pathValues[d] ?? null;
            }

            const baselineDiffs: Record<string, BaselineCellDiff> = {};
            let anyChanged = isNew || isRemoved;

            allColIds.forEach((colId) => {
                const vc = liveAgg.measureByColId.get(colId) || (baseAgg ? baseAgg.measureByColId.get(colId) : undefined);
                const currVal = liveRec ? (liveRec.values[colId] ?? null) : (vc && isAdditiveZeroOnDelete(vc.agg) ? 0 : null);
                outRow[colId] = currVal;
                if (baseAgg) {
                    const bVal = baseRec ? (baseRec.values[colId] ?? null) : null;
                    const diff = buildBaselineDiff(currVal, bVal, vc, isRemoved);
                    baselineDiffs[colId] = diff;
                    if (typeof diff.delta === 'number' && Math.abs(diff.delta) > 1e-12) {
                        anyChanged = true;
                        deltasByPathAndCol.set(`${rowKey}::${colId}`, diff.delta);
                    }
                }
            });

            if (baseAgg) {
                outRow._baselineDiff = baselineDiffs;
            }
            if (!sub.changedOnly || !baseAgg || anyChanged) {
                assembled.push(outRow);
            }
        });

        // Compute Top-3 child variance drivers (PnL Explain) for parent rows when baseline is active
        if (baseAgg && sub.shape.rowFields.length > 1) {
            const childrenByParent = new Map<string, Record<string, unknown>[]>();
            assembled.forEach((row) => {
                const depth = Number(row.depth) || 0;
                if (depth <= 1) return;
                const dPath = String(row._path || '');
                const sepIdx = dPath.lastIndexOf('|||');
                if (sepIdx <= 0) return;
                const parentDisplay = dPath.slice(0, sepIdx);
                const list = childrenByParent.get(parentDisplay) || [];
                list.push(row);
                childrenByParent.set(parentDisplay, list);
            });

            assembled.forEach((row) => {
                const kids = childrenByParent.get(String(row._path || ''));
                if (!kids || kids.length === 0) return;
                const driversByCol: Record<string, VarianceDriver[]> = {};
                allColIds.forEach((colId) => {
                    const ranked = kids
                        .map((k) => {
                            const bd = (k._baselineDiff as Record<string, BaselineCellDiff> | undefined)?.[colId];
                            const deltaNum = typeof bd?.delta === 'number' ? bd.delta : null;
                            return deltaNum !== null && Math.abs(deltaNum) > 1e-12
                                ? {
                                    path: String(k._path || ''),
                                    label: String(k._label || k._id || ''),
                                    delta: deltaNum,
                                    pctDelta: bd?.pctDelta ?? null,
                                }
                                : null;
                        })
                        .filter((x): x is NonNullable<typeof x> => x !== null)
                        .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
                        .slice(0, 3);
                    if (ranked.length > 0) {
                        driversByCol[colId] = ranked;
                    }
                });
                if (Object.keys(driversByCol).length > 0) {
                    row._varianceDrivers = driversByCol;
                }
            });
        }

        // Apply Sort Lock (`in_place` preserves pinned row order; `live_rank` re-sorts dynamically)
        if (sub.sortLock === 'in_place' && sub.pinnedRowOrder.length > 0) {
            const rankMap = new Map<string, number>();
            sub.pinnedRowOrder.forEach((k, idx) => rankMap.set(k, idx));
            assembled.sort((a, b) => {
                const ra = rankMap.get(String(a._canonicalPath)) ?? Number.MAX_SAFE_INTEGER;
                const rb = rankMap.get(String(b._canonicalPath)) ?? Number.MAX_SAFE_INTEGER;
                if (ra !== rb) return ra - rb;
                return String(a._path || '').localeCompare(String(b._path || ''));
            });
        } else {
            const primarySort = sub.shape.sorting && sub.shape.sorting[0];
            assembled.sort((a, b) => {
                if (primarySort && primarySort.id) {
                    const va = toFinite(a[primarySort.id]) ?? -Infinity;
                    const vb = toFinite(b[primarySort.id]) ?? -Infinity;
                    if (va !== vb) return primarySort.desc ? vb - va : va - vb;
                }
                return String(a._path || '').localeCompare(String(b._path || ''));
            });
            sub.pinnedRowOrder = assembled.map((r) => String(r._canonicalPath));
        }

        // Grand Total baseline diff
        if (baseAgg) {
            const gtDiffs: Record<string, BaselineCellDiff> = {};
            allColIds.forEach((colId) => {
                const vc = liveAgg.measureByColId.get(colId) || baseAgg.measureByColId.get(colId);
                const cVal = toFinite(liveAgg.grandTotal[colId]);
                const bVal = toFinite(baseAgg.grandTotal[colId]);
                gtDiffs[colId] = buildBaselineDiff(cVal, bVal, vc, false);
            });
            liveAgg.grandTotal._baselineDiff = gtDiffs;
        }

        return { rows: assembled, grandTotal: liveAgg.grandTotal, sourceRevision: currentRev };
    }

    private buildSnapshotFrame(sub: ClientSubscriptionState): ProtocolV2Frame {
        const { rows, grandTotal, sourceRevision } = this.materializeRows(sub);
        sub.seq += 1;
        sub.lastRowsByKey = new Map(rows.map((r) => [String(r._canonicalPath), r]));
        sub.lastGrandTotal = grandTotal;
        const baselineSnap = sub.baselineId ? this.snapshots.get(sub.baselineId) : undefined;
        return {
            type: 'SNAPSHOT',
            protocolVersion: 2,
            subscriptionId: sub.subscriptionId,
            serverEpochId: this.serverEpochId,
            queryEpoch: sub.queryEpoch,
            schemaVersion: this.schemaVersion,
            sourceRevision,
            seq: sub.seq,
            totalRows: rows.length,
            rows,
            grandTotal,
            paused: sub.paused,
            bufferedUpdates: sub.bufferedUpdates,
            frozenSnapshotId: sub.frozenSnapshotId,
            baselineId: sub.baselineId,
            baselineLabel: baselineSnap?.descriptor.label ?? null,
            sortLock: sub.sortLock,
            capabilityTier: 'tier_1_exact_cdc',
            supportsExactRowDeltas: true,
            timestamp: Date.now(),
        };
    }

    private buildHeartbeatFrame(sub: ClientSubscriptionState): ProtocolV2Frame {
        return {
            type: 'HEARTBEAT',
            protocolVersion: 2,
            subscriptionId: sub.subscriptionId,
            serverEpochId: this.serverEpochId,
            queryEpoch: sub.queryEpoch,
            schemaVersion: this.schemaVersion,
            sourceRevision: this.sourceRevision,
            seq: sub.seq,
            paused: sub.paused,
            bufferedUpdates: sub.bufferedUpdates,
            frozenSnapshotId: sub.frozenSnapshotId,
            baselineId: sub.baselineId,
            sortLock: sub.sortLock,
            capabilityTier: 'tier_1_exact_cdc',
            supportsExactRowDeltas: true,
            timestamp: Date.now(),
        };
    }

    private broadcastUpdates(schemaChanged: boolean): void {
        this.subscriptions.forEach((sub) => {
            if (!sub.listener) return;
            if (sub.frozenSnapshotId) {
                // Frozen snapshot stays completely immutable; emit heartbeat only.
                sub.listener(this.buildHeartbeatFrame(sub));
                return;
            }
            if (sub.paused) {
                sub.bufferedUpdates += 1;
                sub.listener(this.buildHeartbeatFrame(sub));
                return;
            }
            if (schemaChanged) {
                sub.queryEpoch += 1;
                sub.listener(this.buildSnapshotFrame(sub));
                return;
            }

            const { rows, grandTotal, sourceRevision } = this.materializeRows(sub);
            const prevKeys = Array.from(sub.lastRowsByKey.keys());
            const nextKeys = rows.map((r) => String(r._canonicalPath));
            const structuralChanged = prevKeys.length !== nextKeys.length
                || prevKeys.some((k, idx) => k !== nextKeys[idx]);

            if (structuralChanged) {
                // Structural add/remove/reorder -> emit authoritative SNAPSHOT (or rowPatches)
                sub.listener(this.buildSnapshotFrame(sub));
                return;
            }

            const cellPatches: StreamCellPatch[] = [];
            const rowPatches: StreamRowPatch[] = [];

            const diffSingleRow = (prevRow: Record<string, unknown> | null, nextRow: Record<string, unknown>) => {
                const rowKey = String(nextRow._canonicalPath || nextRow._path || '');
                const dPath = String(nextRow._path || nextRow._id || '');
                const baselineMap = (nextRow._baselineDiff as Record<string, BaselineCellDiff> | undefined) || undefined;
                Object.keys(nextRow).forEach((colId) => {
                    if (colId.startsWith('_') || colId === 'depth') return;
                    if (sub.shape.rowFields.includes(colId)) return;
                    const nextVal = nextRow[colId];
                    const prevVal = prevRow ? prevRow[colId] : undefined;
                    if (nextVal !== prevVal) {
                        const nNum = toFinite(nextVal);
                        const pNum = toFinite(prevVal);
                        const delta = nNum !== null && pNum !== null ? nNum - pNum : null;
                        const pctDelta = delta !== null && pNum !== null && pNum !== 0
                            ? delta / Math.abs(pNum)
                            : null;
                        const direction = delta === null
                            ? null
                            : (delta > 0 ? 'up' : (delta < 0 ? 'down' : 'flat'));
                        cellPatches.push({
                            rowKey,
                            displayPath: dPath,
                            colId,
                            value: nextVal,
                            prevValue: prevVal,
                            delta,
                            pctDelta,
                            direction,
                            baseline: baselineMap ? (baselineMap[colId] ?? null) : null,
                        });
                    }
                });
            };

            rows.forEach((nextRow) => {
                const key = String(nextRow._canonicalPath);
                const prevRow = sub.lastRowsByKey.get(key) || null;
                diffSingleRow(prevRow, nextRow);
            });
            if (grandTotal) {
                diffSingleRow(sub.lastGrandTotal, grandTotal);
            }

            if (cellPatches.length === 0 && rowPatches.length === 0) {
                sub.listener(this.buildHeartbeatFrame(sub));
                return;
            }

            const baseSeq = sub.seq;
            sub.seq += 1;
            sub.lastRowsByKey = new Map(rows.map((r) => [String(r._canonicalPath), r]));
            sub.lastGrandTotal = grandTotal;
            const baselineSnap = sub.baselineId ? this.snapshots.get(sub.baselineId) : undefined;
            sub.listener({
                type: 'PATCH',
                protocolVersion: 2,
                subscriptionId: sub.subscriptionId,
                serverEpochId: this.serverEpochId,
                queryEpoch: sub.queryEpoch,
                schemaVersion: this.schemaVersion,
                sourceRevision,
                baseSeq,
                seq: sub.seq,
                totalRows: rows.length,
                cellPatches,
                rowPatches,
                grandTotal,
                paused: sub.paused,
                bufferedUpdates: sub.bufferedUpdates,
                frozenSnapshotId: sub.frozenSnapshotId,
                baselineId: sub.baselineId,
                baselineLabel: baselineSnap?.descriptor.label ?? null,
                sortLock: sub.sortLock,
                capabilityTier: 'tier_1_exact_cdc',
                supportsExactRowDeltas: true,
                timestamp: Date.now(),
            });
        });
    }
}

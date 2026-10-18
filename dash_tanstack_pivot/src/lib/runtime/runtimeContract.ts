/**
 * The one host-neutral wire contract for the pivot runtime.
 *
 * The JSON schema beside this module is the cross-language source of truth.
 * These types keep frontend producers and consumers explicit while the Python
 * codec validates the same V1 envelope at the Dash boundary.
 */

import type { PivotRuntimeProtocolRequestV1 } from './runtime-contract.generated';
import type { PivotRuntimeProtocolResponseV1 } from './runtime-response-contract.generated';

export const RUNTIME_SCHEMA_VERSION = 1 as const;

export const RUNTIME_COMMAND_KINDS = [
    'data',
    'filter_options',
    'chart',
    'detail',
    'drill',
    'update',
    'transaction',
    'export',
] as const;

export type RuntimeCommandKind = typeof RUNTIME_COMMAND_KINDS[number];
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue | undefined };

export interface PivotViewStateSnapshot {
    rowFields?: string[];
    colFields?: string[];
    valConfigs?: JsonObject[];
    filters?: JsonObject;
    customDimensions?: JsonObject[];
    measureAxis?: JsonObject;
    sorting?: JsonObject[];
    sortOptions?: JsonObject;
    expanded?: boolean | JsonObject;
    immersiveMode?: boolean;
    showRowTotals?: boolean;
    showColTotals?: boolean;
    showSubtotals?: boolean;
    viewMode?: string;
    detailMode?: string;
    treeConfig?: JsonObject | null;
    detailConfig?: JsonObject | null;
    reportDef?: JsonObject | null;
}

interface RuntimeRequestEnvelopeV1 {
    schemaVersion: typeof RUNTIME_SCHEMA_VERSION;
    requestId: string;
    table?: string;
    sessionId?: string;
    clientInstance?: string;
    stateEpoch?: number;
    windowSeq?: number;
    abortGeneration?: number;
    cacheKey?: string;
    state?: PivotViewStateSnapshot;
}

export type ExportRequestPayload =
    | (JsonObject & { action?: undefined; jobId?: never })
    | (JsonObject & { action: 'start'; jobId?: never })
    | (JsonObject & { action: 'status' | 'cancel'; jobId: string });

export type RuntimeRequestV1 = RuntimeRequestEnvelopeV1 & (
    | { kind: 'export'; payload: ExportRequestPayload }
    | { kind: Exclude<RuntimeCommandKind, 'export'>; payload: JsonObject }
);

export type ExportJobPhase = 'queued' | 'running' | 'cancelling' | 'cancelled' | 'completed' | 'failed' | 'interrupted';
export type ExportJobPayload = JsonObject & {
    jobId: string;
    rowsWritten: number;
    bytesWritten: number;
    totalRows: number | null;
} & (
    | { phase: 'completed'; payloadRef: JsonObject }
    | { phase: Exclude<ExportJobPhase, 'completed'>; payloadRef?: never }
);

/** Generated directly from the canonical JSON Schema for boundary consumers. */
export type RuntimeRequestSchemaV1 = PivotRuntimeProtocolRequestV1;

export interface RuntimeResponseV1 {
    schemaVersion: typeof RUNTIME_SCHEMA_VERSION;
    kind: RuntimeCommandKind;
    status: string;
    requestId?: string;
    table?: string;
    sessionId?: string;
    clientInstance?: string;
    stateEpoch?: number;
    windowSeq?: number;
    message?: string;
    payload: JsonObject;
    profile?: JsonObject;
}

/** Generated directly from the response schema used by all host transports. */
export type RuntimeResponseSchemaV1 = PivotRuntimeProtocolResponseV1;

type RuntimeRequestInput = Record<string, unknown>;

const commandAliases: Record<string, RuntimeCommandKind> = {
    batch_update: 'update',
    cell_updates: 'update',
};

const asRecord = (value: unknown): Record<string, unknown> => (
    value && typeof value === 'object' && !Array.isArray(value)
        ? value as Record<string, unknown>
        : {}
);

const stringValue = (value: unknown): string | undefined => {
    if (value === undefined || value === null) return undefined;
    const normalized = String(value);
    return normalized || undefined;
};

const nonNegativeInteger = (value: unknown): number | undefined => {
    const numeric = Number(value);
    return Number.isInteger(numeric) && numeric >= 0 ? numeric : undefined;
};

const resolveKind = (value: unknown): RuntimeCommandKind => {
    const normalized = String(value || 'data').trim().toLowerCase();
    const resolved = commandAliases[normalized] || normalized;
    return (RUNTIME_COMMAND_KINDS as readonly string[]).includes(resolved)
        ? resolved as RuntimeCommandKind
        : 'data';
};

const resolvePayload = (request: RuntimeRequestInput): JsonObject => {
    const payload = asRecord(request.payload);
    if (Object.keys(payload).length > 0) return payload as JsonObject;
    const envelopeKeys = new Set([
        'schemaVersion', 'kind', 'requestId', 'request_id', 'table', 'sessionId',
        'session_id', 'clientInstance', 'client_instance', 'stateEpoch',
        'state_epoch', 'windowSeq', 'window_seq', 'abortGeneration',
        'abort_generation', 'cacheKey', 'cache_key', 'state',
    ]);
    return Object.entries(request).reduce<JsonObject>((result, [key, value]) => {
        if (!envelopeKeys.has(key)) result[key] = value as JsonValue;
        return result;
    }, {});
};

export interface RuntimeRequestDefaults {
    tableName?: string | null;
    sessionId?: string | null;
    clientInstance?: string | null;
    viewMode?: string | null;
    detailMode?: string | null;
    treeConfig?: JsonObject | null;
    detailConfig?: JsonObject | null;
    profile?: boolean;
}

/** Normalize all existing client call-sites into the only emitted wire shape. */
export function createRuntimeRequest(
    input: RuntimeRequestInput,
    defaults: RuntimeRequestDefaults = {},
): RuntimeRequestV1 {
    const payload = resolvePayload(input);
    const rawState = asRecord(input.state);
    const legacyState = asRecord(payload.stateOverride || payload.state_override);
    const state = Object.keys(rawState).length > 0 ? rawState : legacyState;
    delete payload.stateOverride;
    delete payload.state_override;

    if (defaults.viewMode && payload.viewMode === undefined) payload.viewMode = defaults.viewMode;
    if (defaults.detailMode && payload.detailMode === undefined) payload.detailMode = defaults.detailMode;
    if (defaults.treeConfig && payload.treeConfig === undefined) payload.treeConfig = defaults.treeConfig as JsonValue;
    if (defaults.detailConfig && payload.detailConfig === undefined) payload.detailConfig = defaults.detailConfig as JsonValue;
    if (defaults.profile && payload.profile === undefined) payload.profile = true;

    const requestId = stringValue(
        input.requestId || input.request_id || payload.requestId || payload.request_id
        || payload.request_signature || payload.windowSeq || payload.window_seq
        || payload.version || payload.nonce,
    ) || `${resolveKind(input.kind)}:${Date.now()}`;

    return {
        schemaVersion: RUNTIME_SCHEMA_VERSION,
        kind: resolveKind(input.kind),
        requestId,
        table: stringValue(input.table || payload.table || defaults.tableName),
        sessionId: stringValue(input.sessionId || input.session_id || payload.sessionId || payload.session_id || defaults.sessionId),
        clientInstance: stringValue(input.clientInstance || input.client_instance || payload.clientInstance || payload.client_instance || defaults.clientInstance),
        stateEpoch: nonNegativeInteger(input.stateEpoch ?? input.state_epoch ?? payload.stateEpoch ?? payload.state_epoch),
        windowSeq: nonNegativeInteger(input.windowSeq ?? input.window_seq ?? payload.windowSeq ?? payload.window_seq ?? payload.version),
        abortGeneration: nonNegativeInteger(input.abortGeneration ?? input.abort_generation ?? payload.abortGeneration ?? payload.abort_generation),
        cacheKey: stringValue(input.cacheKey || input.cache_key || payload.cacheKey || payload.cache_key),
        ...(Object.keys(state).length > 0 ? { state: state as PivotViewStateSnapshot } : {}),
        payload,
    } as RuntimeRequestV1;
}

const isExportRequestPayload = (value: unknown): boolean => {
    const payload = asRecord(value);
    if (!('action' in payload)) return !('jobId' in payload);
    if (payload.action === 'start') return !('jobId' in payload);
    return (payload.action === 'status' || payload.action === 'cancel')
        && typeof payload.jobId === 'string' && payload.jobId.length > 0;
};

const isExportJobPayload = (value: unknown): boolean => {
    const payload = asRecord(value);
    const phases: readonly string[] = ['queued', 'running', 'cancelling', 'cancelled', 'completed', 'failed', 'interrupted'];
    const allowed = new Set(['jobId', 'phase', 'rowsWritten', 'bytesWritten', 'totalRows', 'payloadRef']);
    const counter = (item: unknown): boolean => typeof item === 'number' && Number.isInteger(item) && item >= 0;
    return typeof payload.jobId === 'string' && payload.jobId.length > 0
        && typeof payload.phase === 'string' && phases.includes(payload.phase)
        && counter(payload.rowsWritten) && counter(payload.bytesWritten)
        && (payload.totalRows === null || counter(payload.totalRows))
        && Object.keys(payload).every(key => allowed.has(key))
        && (payload.phase === 'completed'
            ? payload.payloadRef !== null && typeof payload.payloadRef === 'object'
                && !Array.isArray(payload.payloadRef) && Object.keys(payload.payloadRef).length > 0
            : !('payloadRef' in payload));
};

export function isRuntimeRequestV1(value: unknown): value is RuntimeRequestV1 {
    const candidate = asRecord(value);
    return candidate.schemaVersion === RUNTIME_SCHEMA_VERSION
        && typeof candidate.requestId === 'string'
        && (RUNTIME_COMMAND_KINDS as readonly string[]).includes(String(candidate.kind))
        && Object.prototype.toString.call(candidate.payload) === '[object Object]'
        && (candidate.kind !== 'export' || isExportRequestPayload(candidate.payload));
}

/** Runtime responses are always V1; hosts do not need version fallbacks. */
export function isRuntimeResponseV1(value: unknown): value is RuntimeResponseV1 {
    const candidate = asRecord(value);
    // Match the canonical response schema at the HTTP boundary, including
    // optional metadata: malformed identity/message fields must never reach
    // shared response processing simply because the required fields exist.
    const identityFields = ['requestId', 'table', 'sessionId', 'clientInstance'];
    const sequenceFields = ['stateEpoch', 'windowSeq'];
    const allowedFields = new Set([
        'schemaVersion', 'kind', 'status', 'payload', 'message', 'profile',
        ...identityFields, ...sequenceFields,
    ]);
    return candidate.schemaVersion === RUNTIME_SCHEMA_VERSION
        && typeof candidate.status === 'string'
        && candidate.status.length > 0
        && typeof candidate.payload === 'object'
        && candidate.payload !== null
        && !Array.isArray(candidate.payload)
        && typeof candidate.kind === 'string'
        && (RUNTIME_COMMAND_KINDS as readonly string[]).includes(candidate.kind)
        && Object.keys(candidate).every(key => allowedFields.has(key))
        && identityFields.every(key => !(key in candidate)
            || typeof candidate[key] === 'string' && candidate[key].length > 0)
        && sequenceFields.every(key => {
            const sequence = candidate[key];
            return !(key in candidate)
                || typeof sequence === 'number' && Number.isInteger(sequence) && sequence >= 0;
        })
        && (!('message' in candidate) || typeof candidate.message === 'string')
        && (candidate.kind !== 'export' || candidate.status !== 'export_job'
            && !('jobId' in asRecord(candidate.payload) || 'phase' in asRecord(candidate.payload))
            || isExportJobPayload(candidate.payload))
        && (!('profile' in candidate) || candidate.profile !== null
            && typeof candidate.profile === 'object' && !Array.isArray(candidate.profile));
}

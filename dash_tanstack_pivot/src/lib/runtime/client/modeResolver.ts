import type { ClientEngineKind, ClientRuntimeConfig, ExecutionMode } from './types';

export interface ModeResolutionInput {
    serverSide?: boolean;
    data?: unknown;
    table?: string;
    runtimeResponse?: unknown;
    performanceConfig?: ClientRuntimeConfig | null;
}

export interface ModeResolutionOptions {
    performanceConfig?: ClientRuntimeConfig | null;
    hasExternalServerTransport?: boolean;
}

export function normalizeExecutionMode(value: unknown): ExecutionMode {
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (normalized === 'client') return 'client';
        if (normalized === 'server') return 'server';
        if (normalized === 'auto') return 'auto';
    }
    return 'auto';
}

export function normalizeClientEngineKind(value: unknown): ClientEngineKind {
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (normalized === 'duckdb-wasm' || normalized === 'duckdb' || normalized === 'wasm') {
            return 'duckdb-wasm';
        }
        if (normalized === 'memory' || normalized === 'js' || normalized === 'in-memory') {
            return 'memory';
        }
        if (normalized === 'auto') {
            return 'auto';
        }
    }
    return 'auto';
}

export function resolveExecutionMode(
    props: ModeResolutionInput = {},
    options: ModeResolutionOptions = {}
): 'client' | 'server' {
    const perfConfig = options.performanceConfig || props.performanceConfig || {};
    const explicitMode = normalizeExecutionMode(perfConfig.executionMode);

    // 1. Explicit executionMode in performanceConfig takes highest precedence
    if (explicitMode === 'client') {
        return 'client';
    }
    if (explicitMode === 'server') {
        return 'server';
    }

    // 2. Explicit boolean prop serverSide takes next precedence when executionMode is not explicitly forced
    if (props.serverSide === true) {
        return 'server';
    }
    if (props.serverSide === false) {
        return 'client';
    }

    // 3. Automatic mode detection ('auto' or serverSide === undefined)
    const hasDataArray = Array.isArray(props.data) && props.data.length > 0;
    const hasEmptyDataArray = Array.isArray(props.data) && props.data.length === 0;
    const hasDataUrl = typeof perfConfig.dataUrl === 'string' && perfConfig.dataUrl.trim().length > 0;
    const hasTable = typeof props.table === 'string' && props.table.trim().length > 0;
    const clientRowThreshold = Number.isFinite(Number(perfConfig.clientRowThreshold))
        ? Math.max(1, Number(perfConfig.clientRowThreshold))
        : 100_000;

    if (options.hasExternalServerTransport === true) {
        if ((hasDataArray || hasDataUrl) && !hasTable) {
            return 'client';
        }
        return 'server';
    }

    if (hasDataUrl) {
        return 'client';
    }

    if (hasDataArray) {
        const rowCount = (props.data as unknown[]).length;
        if (hasTable && props.runtimeResponse != null) {
            return 'server';
        }
        if (hasTable && rowCount > clientRowThreshold) {
            return 'server';
        }
        return 'client';
    }

    if (hasTable) {
        return 'server';
    }

    if (hasEmptyDataArray) {
        return 'client';
    }

    return 'client';
}

export function resolveClientEngineKind(
    config: ClientRuntimeConfig | null | undefined,
    rowCount: number,
    wasmReady = true
): 'duckdb-wasm' | 'memory' {
    const perfConfig = config || {};
    const requestedEngine = normalizeClientEngineKind(perfConfig.clientEngine);
    const hasDataUrl = typeof perfConfig.dataUrl === 'string' && perfConfig.dataUrl.trim().length > 0;
    const threshold = Number.isFinite(Number(perfConfig.duckdbWasmThreshold))
        ? Math.max(0, Number(perfConfig.duckdbWasmThreshold))
        : 10_000;

    if (requestedEngine === 'memory') {
        return 'memory';
    }
    if (requestedEngine === 'duckdb-wasm') {
        return 'duckdb-wasm';
    }
    if (hasDataUrl) {
        return 'duckdb-wasm';
    }
    if (wasmReady && rowCount >= threshold) {
        return 'duckdb-wasm';
    }
    return 'memory';
}

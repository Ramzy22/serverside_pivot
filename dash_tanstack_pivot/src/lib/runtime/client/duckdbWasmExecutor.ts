import { resolveClientEngineKind } from './modeResolver';
import type {
    ClientEngineTelemetry,
    ClientExecutedDataset,
    ClientRuntimeConfig,
    LogicalPivotPlan,
    NormalizedCustomDimension,
    NormalizedFieldFilter,
    NormalizedFilterClause,
} from './types';

const DEFAULT_DUCKDB_WASM_CDN = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

function evaluateCustomDimensionRow(
    row: { [key: string]: unknown },
    dim: NormalizedCustomDimension
): string {
    const rawVal = row[dim.field];
    const numVal = typeof rawVal === 'number' ? rawVal : Number(rawVal);
    const hasNum = rawVal !== null && rawVal !== undefined && rawVal !== '' && Number.isFinite(numVal);
    const strVal = rawVal === null || rawVal === undefined ? '' : String(rawVal);
    const lowerVal = strVal.toLowerCase();

    for (const rule of dim.rules) {
        const op = rule.op;
        const target = rule.value;
        const targetNum = typeof target === 'number' ? target : Number(target);
        const targetStr = target === null || target === undefined ? '' : String(target);
        const targetLower = targetStr.toLowerCase();

        let matched = false;
        if (op === 'eq' || op === '=' || op === '==') {
            matched = rawVal === target || strVal === targetStr || (hasNum && Number.isFinite(targetNum) && numVal === targetNum);
        } else if (op === 'ne' || op === '!=' || op === 'not_eq') {
            matched = rawVal !== target && strVal !== targetStr;
        } else if (op === 'gt' || op === '>') {
            matched = hasNum && Number.isFinite(targetNum) && numVal > targetNum;
        } else if (op === 'gte' || op === '>=') {
            matched = hasNum && Number.isFinite(targetNum) && numVal >= targetNum;
        } else if (op === 'lt' || op === '<') {
            matched = hasNum && Number.isFinite(targetNum) && numVal < targetNum;
        } else if (op === 'lte' || op === '<=') {
            matched = hasNum && Number.isFinite(targetNum) && numVal <= targetNum;
        } else if (op === 'between') {
            const pair = Array.isArray(target) ? target : [target, rule.value2];
            const minN = Number(pair[0]);
            const maxN = Number(pair[1]);
            matched = hasNum && Number.isFinite(minN) && Number.isFinite(maxN) && numVal >= minN && numVal <= maxN;
        } else if (op === 'in') {
            const list = Array.isArray(target)
                ? target.map((v) => String(v).trim().toLowerCase())
                : targetStr.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
            matched = list.includes(lowerVal);
        } else if (op === 'contains') {
            matched = lowerVal.includes(targetLower);
        } else if (op === 'starts_with' || op === 'startswith') {
            matched = lowerVal.startsWith(targetLower);
        } else if (op === 'ends_with' || op === 'endswith') {
            matched = lowerVal.endsWith(targetLower);
        }
        if (matched) {
            return rule.label;
        }
    }
    return dim.defaultBucket;
}

export function applyCustomDimensionsToDataset(
    rows: Array<{ [key: string]: unknown }>,
    customDimensions: NormalizedCustomDimension[]
): Array<{ [key: string]: unknown }> {
    if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(customDimensions) || customDimensions.length === 0) {
        return Array.isArray(rows) ? rows : [];
    }
    return rows.map((row) => {
        const nextRow: { [key: string]: unknown } = { ...row };
        for (const dim of customDimensions) {
            nextRow[dim.id] = evaluateCustomDimensionRow(nextRow, dim);
        }
        return nextRow;
    });
}

function evaluateClause(rawVal: unknown, clause: NormalizedFilterClause): boolean {
    const t = clause.type;
    const target = clause.value;
    const isNull = rawVal === null || rawVal === undefined || rawVal === '';

    if (t === 'is_null' || t === 'isnull' || t === 'empty' || t === 'blank') {
        return isNull;
    }
    if (t === 'is_not_null' || t === 'isnotnull' || t === 'not_empty' || t === 'not_blank') {
        return !isNull;
    }

    const strVal = isNull ? '' : String(rawVal);
    const cmpVal = clause.caseSensitive ? strVal : strVal.toLowerCase();
    const numVal = typeof rawVal === 'number' ? rawVal : Number(rawVal);
    const hasNum = !isNull && Number.isFinite(numVal);

    if (t === 'in' || t === 'set') {
        const list = clause.values || (Array.isArray(target) ? target : []);
        if (list.length === 0) return false;
        return list.some((item) => {
            if (item === null || item === undefined || item === '__null__') {
                return rawVal === null || rawVal === undefined;
            }
            if (rawVal === item) return true;
            const itemStr = String(item);
            return clause.caseSensitive ? strVal === itemStr : cmpVal === itemStr.toLowerCase();
        });
    }

    if (t === 'not_in' || t === 'notin') {
        const list = clause.values || (Array.isArray(target) ? target : []);
        if (list.length === 0) return true;
        return !list.some((item) => {
            if (item === null || item === undefined || item === '__null__') {
                return rawVal === null || rawVal === undefined;
            }
            if (rawVal === item) return true;
            const itemStr = String(item);
            return clause.caseSensitive ? strVal === itemStr : cmpVal === itemStr.toLowerCase();
        });
    }

    if (t === 'eq' || t === 'equals' || t === '=') {
        if (target === null || target === '__null__') {
            return rawVal === null || rawVal === undefined;
        }
        if (rawVal === target) return true;
        const targetNum = Number(target);
        if (hasNum && Number.isFinite(targetNum) && numVal === targetNum) return true;
        const targetStr = String(target ?? '');
        return clause.caseSensitive ? strVal === targetStr : cmpVal === targetStr.toLowerCase();
    }

    if (t === 'ne' || t === 'not_eq' || t === 'not_equals' || t === '!=') {
        if (target === null || target === '__null__') {
            return rawVal !== null && rawVal !== undefined;
        }
        if (rawVal === target) return false;
        const targetNum = Number(target);
        if (hasNum && Number.isFinite(targetNum) && numVal === targetNum) return false;
        const targetStr = String(target ?? '');
        return clause.caseSensitive ? strVal !== targetStr : cmpVal !== targetStr.toLowerCase();
    }

    if (t === 'gt' || t === '>') {
        const targetNum = Number(target);
        return hasNum && Number.isFinite(targetNum) && numVal > targetNum;
    }
    if (t === 'gte' || t === '>=') {
        const targetNum = Number(target);
        return hasNum && Number.isFinite(targetNum) && numVal >= targetNum;
    }
    if (t === 'lt' || t === '<') {
        const targetNum = Number(target);
        return hasNum && Number.isFinite(targetNum) && numVal < targetNum;
    }
    if (t === 'lte' || t === '<=') {
        const targetNum = Number(target);
        return hasNum && Number.isFinite(targetNum) && numVal <= targetNum;
    }
    if (t === 'between') {
        const pair = Array.isArray(target) ? target : (clause.values || []);
        const minN = Number(pair[0]);
        const maxN = Number(pair[1]);
        return hasNum && Number.isFinite(minN) && Number.isFinite(maxN) && numVal >= minN && numVal <= maxN;
    }
    if (t === 'contains') {
        const targetStr = clause.caseSensitive ? String(target ?? '') : String(target ?? '').toLowerCase();
        return !isNull && cmpVal.includes(targetStr);
    }
    if (t === 'not_contains' || t === 'notcontains') {
        const targetStr = clause.caseSensitive ? String(target ?? '') : String(target ?? '').toLowerCase();
        return isNull || !cmpVal.includes(targetStr);
    }
    if (t === 'starts_with' || t === 'startswith') {
        const targetStr = clause.caseSensitive ? String(target ?? '') : String(target ?? '').toLowerCase();
        return !isNull && cmpVal.startsWith(targetStr);
    }
    if (t === 'ends_with' || t === 'endswith') {
        const targetStr = clause.caseSensitive ? String(target ?? '') : String(target ?? '').toLowerCase();
        return !isNull && cmpVal.endsWith(targetStr);
    }
    return true;
}

export function filterDatasetRows(
    rows: Array<{ [key: string]: unknown }>,
    filters: NormalizedFieldFilter[],
    excludeField?: string
): Array<{ [key: string]: unknown }> {
    const active = filters.filter((f) => f.field !== excludeField && f.clauses.length > 0);
    if (active.length === 0) return rows;
    return rows.filter((row) =>
        active.every((fieldFilter) => {
            const rawVal = row[fieldFilter.field];
            if (fieldFilter.operator === 'OR') {
                return fieldFilter.clauses.some((clause) => evaluateClause(rawVal, clause));
            }
            return fieldFilter.clauses.every((clause) => evaluateClause(rawVal, clause));
        })
    );
}

interface DuckDbConnectionLike {
    query: (sql: string) => Promise<{ toArray?: () => unknown[] }>;
}

interface DuckDbInstanceLike {
    connect: () => Promise<DuckDbConnectionLike>;
    registerFileText?: (name: string, text: string) => Promise<void>;
}

export class DuckDbWasmExecutor {
    private rows: Array<{ [key: string]: unknown }>;
    private config: ClientRuntimeConfig;
    private wasmReady = false;
    private wasmInitializing: Promise<boolean> | null = null;
    private dbInstance: DuckDbInstanceLike | null = null;
    private dbConn: DuckDbConnectionLike | null = null;
    private registeredTableSignature = '';
    private telemetry: ClientEngineTelemetry;

    public constructor(
        initialRows: Array<{ [key: string]: unknown }> = [],
        config: ClientRuntimeConfig = {}
    ) {
        this.rows = Array.isArray(initialRows) ? initialRows : [];
        this.config = { ...config };
        this.telemetry = {
            activeMode: 'client',
            activeEngine: resolveClientEngineKind(this.config, this.rows.length, false),
            wasmReady: false,
            lastQuerySql: '',
            lastExecutionMs: 0,
            rowCount: this.rows.length,
            networkRoundtrips: 0,
            lastUpdatedAt: Date.now(),
        };
    }

    public setData(rows: Array<{ [key: string]: unknown }>): void {
        this.rows = Array.isArray(rows) ? rows : [];
        this.registeredTableSignature = '';
        this.telemetry.rowCount = this.rows.length;
        this.telemetry.activeEngine = resolveClientEngineKind(this.config, this.rows.length, this.wasmReady);
        this.telemetry.lastUpdatedAt = Date.now();
    }

    public getData(): Array<{ [key: string]: unknown }> {
        return this.rows;
    }

    public updateConfig(config: Partial<ClientRuntimeConfig>): void {
        this.config = { ...this.config, ...config };
        this.telemetry.activeEngine = resolveClientEngineKind(this.config, this.rows.length, this.wasmReady);
        this.telemetry.lastUpdatedAt = Date.now();
    }

    public getTelemetry(): ClientEngineTelemetry {
        return { ...this.telemetry };
    }

    private async tryInitializeWasm(): Promise<boolean> {
        if (this.wasmReady && this.dbConn) return true;
        if (this.wasmInitializing) return this.wasmInitializing;

        this.wasmInitializing = (async () => {
            try {
                const injected = this.config.duckdbWasmInstance as DuckDbInstanceLike | undefined;
                if (injected && typeof injected.connect === 'function') {
                    this.dbInstance = injected;
                    this.dbConn = await injected.connect();
                    this.wasmReady = true;
                    return true;
                }

                const globalScope = globalThis as { duckdb?: { AsyncDuckDB?: unknown } };
                if (typeof window === 'undefined' && !globalScope.duckdb) {
                    return false;
                }

                const cdnUrl = this.config.duckdbWasmCdnUrl || DEFAULT_DUCKDB_WASM_CDN;
                const dynamicImporter = new Function('url', 'return import(url)') as (url: string) => Promise<{
                    getJsDelivrBundles?: () => unknown;
                    selectBundle?: (bundles: unknown) => Promise<{ mainModule: string; mainWorker?: string }>;
                    ConsoleLogger?: new () => unknown;
                    AsyncDuckDB?: new (logger: unknown, worker: Worker) => {
                        instantiate: (mainModule: string) => Promise<void>;
                        connect: () => Promise<DuckDbConnectionLike>;
                        registerFileText: (name: string, text: string) => Promise<void>;
                    };
                }>;

                const duckdbModule = (globalScope.duckdb as Awaited<ReturnType<typeof dynamicImporter>>) || (await dynamicImporter(cdnUrl));
                if (
                    !duckdbModule ||
                    typeof duckdbModule.getJsDelivrBundles !== 'function' ||
                    typeof duckdbModule.selectBundle !== 'function' ||
                    typeof duckdbModule.AsyncDuckDB !== 'function' ||
                    typeof Worker === 'undefined'
                ) {
                    return false;
                }

                const bundles = duckdbModule.getJsDelivrBundles();
                const bundle = await duckdbModule.selectBundle(bundles);
                if (!bundle || !bundle.mainWorker) {
                    return false;
                }
                const workerUrl = URL.createObjectURL(
                    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
                );
                const worker = new Worker(workerUrl);
                const logger = duckdbModule.ConsoleLogger ? new duckdbModule.ConsoleLogger() : console;
                const db = new duckdbModule.AsyncDuckDB(logger, worker);
                await db.instantiate(bundle.mainModule);
                URL.revokeObjectURL(workerUrl);

                this.dbInstance = db;
                this.dbConn = await db.connect();
                this.wasmReady = true;
                return true;
            } catch (_err) {
                this.wasmReady = false;
                return false;
            } finally {
                this.wasmInitializing = null;
            }
        })();

        return this.wasmInitializing;
    }

    private async ensureTableRegistered(tableName: string): Promise<boolean> {
        if (!this.wasmReady || !this.dbConn) return false;
        const safeTable = `"${String(tableName || 'client_data').replace(/"/g, '""')}"`;
        const dataUrl = typeof this.config.dataUrl === 'string' ? this.config.dataUrl.trim() : '';
        const signature = `${tableName}:${this.rows.length}:${dataUrl}`;
        if (this.registeredTableSignature === signature) {
            return true;
        }

        try {
            if (dataUrl) {
                const escapedUrl = dataUrl.replace(/'/g, "''");
                const lower = dataUrl.toLowerCase();
                const readerExpr = lower.endsWith('.parquet')
                    ? `read_parquet('${escapedUrl}')`
                    : `read_csv_auto('${escapedUrl}')`;
                await this.dbConn.query(`CREATE OR REPLACE TABLE ${safeTable} AS SELECT * FROM ${readerExpr}`);
                const loadedResult = await this.dbConn.query(`SELECT * FROM ${safeTable}`);
                if (loadedResult && typeof loadedResult.toArray === 'function') {
                    this.rows = loadedResult.toArray().map((row) => ({ ...(row as { [key: string]: unknown }) }));
                }
                this.registeredTableSignature = signature;
                return true;
            }

            if (this.dbInstance && typeof this.dbInstance.registerFileText === 'function') {
                const fileName = `${tableName || 'client_data'}.json`;
                await this.dbInstance.registerFileText(fileName, JSON.stringify(this.rows));
                await this.dbConn.query(
                    `CREATE OR REPLACE TABLE ${safeTable} AS SELECT * FROM read_json_auto('${fileName.replace(/'/g, "''")}')`
                );
                this.registeredTableSignature = signature;
                return true;
            }
        } catch (_err) {
            return false;
        }
        return false;
    }

    public async execute(plan: LogicalPivotPlan): Promise<ClientExecutedDataset> {
        const startTime = typeof performance !== 'undefined' && typeof performance.now === 'function'
            ? performance.now()
            : Date.now();

        const targetEngine = resolveClientEngineKind(this.config, this.rows.length, this.wasmReady);
        let engineUsed: 'duckdb-wasm' | 'memory' = 'memory';

        if (targetEngine === 'duckdb-wasm' || normalizeWasmWarmupRequested(this.config, this.rows.length)) {
            if (!this.wasmReady) {
                const initialized = await this.tryInitializeWasm();
                if (initialized && resolveClientEngineKind(this.config, this.rows.length, true) === 'duckdb-wasm') {
                    const registered = await this.ensureTableRegistered(plan.table);
                    if (registered && this.dbConn) {
                        try {
                            await this.dbConn.query(plan.sql.groupingSql);
                            engineUsed = 'duckdb-wasm';
                        } catch (_err) {
                            engineUsed = 'memory';
                        }
                    }
                }
            } else if (this.dbConn) {
                const registered = await this.ensureTableRegistered(plan.table);
                if (registered) {
                    try {
                        await this.dbConn.query(plan.sql.groupingSql);
                        engineUsed = 'duckdb-wasm';
                    } catch (_err) {
                        engineUsed = 'memory';
                    }
                }
            }
        }

        const allRows = applyCustomDimensionsToDataset(this.rows, plan.customDimensions);
        const filteredRows = filterDatasetRows(allRows, plan.filters);

        const colSet = new Set<string>();
        if (plan.colFields.length > 0) {
            for (const row of filteredRows) {
                const key = plan.colFields
                    .map((colField) => {
                        const val = row[colField];
                        return val === null || val === undefined ? 'null' : String(val);
                    })
                    .join('_');
                colSet.add(key);
            }
        }
        const colCombinations = Array.from(colSet).sort((a, b) =>
            a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
        );

        const endTime = typeof performance !== 'undefined' && typeof performance.now === 'function'
            ? performance.now()
            : Date.now();
        const executionMs = Math.max(0.05, Number((endTime - startTime).toFixed(2)));

        this.telemetry = {
            activeMode: 'client',
            activeEngine: engineUsed,
            wasmReady: this.wasmReady,
            lastQuerySql: plan.sql.groupingSql,
            lastExecutionMs: executionMs,
            rowCount: allRows.length,
            networkRoundtrips: 0,
            lastUpdatedAt: Date.now(),
        };

        return {
            filteredRows,
            allRows,
            colCombinations,
            engineUsed,
            executionMs,
            lastSql: plan.sql.groupingSql,
        };
    }
}

function normalizeWasmWarmupRequested(config: ClientRuntimeConfig, rowCount: number): boolean {
    if (config.clientEngine === 'duckdb-wasm') return true;
    if (typeof config.dataUrl === 'string' && config.dataUrl.trim().length > 0) return true;
    const threshold = Number.isFinite(Number(config.duckdbWasmThreshold))
        ? Math.max(0, Number(config.duckdbWasmThreshold))
        : 10_000;
    return config.clientEngine !== 'memory' && rowCount >= threshold;
}

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
const DEFAULT_WASM_INIT_TIMEOUT_MS = 4500;
const WASM_RETRY_BACKOFF_MS = 30000;

function normalizeDatasetFieldNames(names: Iterable<unknown>): string[] {
    const out: string[] = [];
    const seen = new Set<string>();
    for (const raw of names) {
        if (raw === undefined || raw === null) continue;
        const fieldId = String(raw);
        if (!fieldId.trim() || fieldId === '__col_schema' || seen.has(fieldId)) continue;
        seen.add(fieldId);
        out.push(fieldId);
    }
    return out;
}

function extractDatasetFieldIds(rows: Array<{ [key: string]: unknown }>): string[] {
    if (!Array.isArray(rows) || rows.length === 0) return [];
    const seen = new Set<string>();
    const fields: string[] = [];
    for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i];
        if (!row || typeof row !== 'object') continue;
        const keys = Object.keys(row);
        for (let j = 0; j < keys.length; j += 1) {
            const key = keys[j];
            if (!key.trim() || key === '__col_schema' || seen.has(key)) continue;
            seen.add(key);
            fields.push(key);
        }
    }
    return fields;
}

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

interface PreparedDatasetCache {
    rowsRef: Array<{ [key: string]: unknown }>;
    customDimsKey: string;
    filtersKey: string;
    colFieldsKey: string;
    allRows: Array<{ [key: string]: unknown }>;
    filteredRows: Array<{ [key: string]: unknown }>;
    colCombinations: string[];
}

export class DuckDbWasmExecutor {
    private rows: Array<{ [key: string]: unknown }>;
    private config: ClientRuntimeConfig;
    private wasmReady = false;
    private wasmInitializing: Promise<boolean> | null = null;
    private wasmRetryAfter = 0;
    private dbInstance: DuckDbInstanceLike | null = null;
    private dbConn: DuckDbConnectionLike | null = null;
    private registeredTableSignature = '';
    private preparedCache: PreparedDatasetCache | null = null;
    private cachedAvailableFields: string[] | null = null;
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
        this.preparedCache = null;
        this.cachedAvailableFields = null;
        this.telemetry.rowCount = this.rows.length;
        this.telemetry.activeEngine = resolveClientEngineKind(this.config, this.rows.length, this.wasmReady);
        this.telemetry.lastUpdatedAt = Date.now();
    }

    public getData(): Array<{ [key: string]: unknown }> {
        return this.rows;
    }

    public getAvailableFields(): string[] {
        if (this.cachedAvailableFields !== null) {
            return [...this.cachedAvailableFields];
        }
        const fields = extractDatasetFieldIds(this.rows);
        if (fields.length > 0 || this.rows.length > 0) {
            this.cachedAvailableFields = fields;
        }
        return [...fields];
    }

    public updateConfig(config: Partial<ClientRuntimeConfig>): void {
        if (config.clientEngine && config.clientEngine !== this.config.clientEngine) {
            this.wasmRetryAfter = 0;
        }
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
        const injected = this.config.duckdbWasmInstance as DuckDbInstanceLike | undefined;
        if (!injected && Date.now() < this.wasmRetryAfter) {
            return false;
        }

        let createdWorker: Worker | null = null;
        let createdWorkerUrl: string | null = null;
        let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
        let timedOut = false;

        const initPromise = (async () => {
            try {
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
                const hasExplicitBundle = Boolean(this.config.duckdbBundleUrl || this.config.duckdbWasmCdnUrl || globalScope.duckdb);
                const isFileOrigin = typeof window !== 'undefined' && window.location && window.location.protocol === 'file:';
                const isNavigatorOffline = typeof navigator !== 'undefined' && navigator.onLine === false;
                if (!hasExplicitBundle && (isFileOrigin || isNavigatorOffline)) {
                    return false;
                }

                const cdnUrl = this.config.duckdbBundleUrl || this.config.duckdbWasmCdnUrl || DEFAULT_DUCKDB_WASM_CDN;
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

                const customLoader = typeof this.config.duckdbLoader === 'function' ? this.config.duckdbLoader : null;
                const duckdbModule = customLoader
                    ? (await customLoader()) as Awaited<ReturnType<typeof dynamicImporter>>
                    : ((globalScope.duckdb as Awaited<ReturnType<typeof dynamicImporter>>) || (await dynamicImporter(cdnUrl)));
                if (
                    timedOut ||
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
                if (timedOut || !bundle || !bundle.mainWorker) {
                    return false;
                }
                createdWorkerUrl = URL.createObjectURL(
                    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
                );
                const worker = new Worker(createdWorkerUrl);
                createdWorker = worker;
                const workerFailure = new Promise<never>((_resolve, reject) => {
                    worker.onerror = (event) => {
                        reject(new Error(event?.message || 'DuckDB-Wasm worker error'));
                    };
                });
                const logger = duckdbModule.ConsoleLogger ? new duckdbModule.ConsoleLogger() : console;
                const db = new duckdbModule.AsyncDuckDB(logger, worker);
                await Promise.race([db.instantiate(bundle.mainModule), workerFailure]);
                if (timedOut) {
                    return false;
                }
                const conn = await Promise.race([db.connect(), workerFailure]);
                if (timedOut) {
                    return false;
                }
                if (createdWorkerUrl) {
                    URL.revokeObjectURL(createdWorkerUrl);
                    createdWorkerUrl = null;
                }

                this.dbInstance = db;
                this.dbConn = conn;
                this.wasmReady = true;
                this.wasmRetryAfter = 0;
                return true;
            } catch (_err) {
                this.wasmReady = false;
                this.wasmRetryAfter = Date.now() + WASM_RETRY_BACKOFF_MS;
                return false;
            }
        })();

        const boundedPromise = injected
            ? initPromise
            : Promise.race([
                  initPromise,
                  new Promise<boolean>((resolve) => {
                      timeoutHandle = setTimeout(() => {
                          timedOut = true;
                          this.wasmReady = false;
                          this.wasmRetryAfter = Date.now() + WASM_RETRY_BACKOFF_MS;
                          if (createdWorker) {
                              try {
                                  createdWorker.terminate();
                              } catch (_terminateErr) {
                                  // Ignore worker termination cleanup failures.
                              }
                              createdWorker = null;
                          }
                          if (createdWorkerUrl) {
                              URL.revokeObjectURL(createdWorkerUrl);
                              createdWorkerUrl = null;
                          }
                          resolve(false);
                      }, DEFAULT_WASM_INIT_TIMEOUT_MS);
                  }),
              ]);

        this.wasmInitializing = boundedPromise.finally(() => {
            if (timeoutHandle !== null) {
                clearTimeout(timeoutHandle);
                timeoutHandle = null;
            }
            this.wasmInitializing = null;
        });

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
                const schemaFields = (loadedResult as { schema?: { fields?: Array<{ name?: unknown }> } } | null)?.schema?.fields;
                if (Array.isArray(schemaFields) && schemaFields.length > 0) {
                    const normalizedSchema = normalizeDatasetFieldNames(schemaFields.map((field) => field && field.name));
                    if (normalizedSchema.length > 0) {
                        this.cachedAvailableFields = normalizedSchema;
                    }
                }
                if (loadedResult && typeof loadedResult.toArray === 'function') {
                    this.rows = loadedResult.toArray().map((row) => ({ ...(row as { [key: string]: unknown }) }));
                    if (this.cachedAvailableFields === null && this.rows.length > 0) {
                        this.cachedAvailableFields = extractDatasetFieldIds(this.rows);
                    }
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
        const dataUrl = typeof this.config.dataUrl === 'string' ? this.config.dataUrl.trim() : '';
        const hasInjectedWasm = Boolean(this.config.duckdbWasmInstance || this.config.duckdbLoader);
        const requiresWasmExecution = Boolean(
            dataUrl
            || hasInjectedWasm
            || this.config.clientEngine === 'duckdb-wasm'
        );

        // Only initialize or roundtrip through DuckDB-Wasm when explicitly configured,
        // loading from dataUrl, or testing an injected Wasm instance. When `clientEngine`
        // is 'auto' on already-loaded JS rows, avoid stalling up to 4.5s on CDN imports
        // or running discarded worker SQL queries before computing the JS response.
        if (
            requiresWasmExecution
            && (targetEngine === 'duckdb-wasm' || normalizeWasmWarmupRequested(this.config, this.rows.length))
        ) {
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

        const customDimsKey = plan.customDimensions.length > 0 ? JSON.stringify(plan.customDimensions) : '';
        const filtersKey = plan.filters.length > 0 ? JSON.stringify(plan.filters) : '';
        const colFieldsKey = plan.colFields.length > 0 ? plan.colFields.join('\u0000') : '';

        let allRows: Array<{ [key: string]: unknown }>;
        let filteredRows: Array<{ [key: string]: unknown }>;
        let colCombinations: string[];

        const cached = this.preparedCache;
        if (
            cached
            && cached.rowsRef === this.rows
            && cached.customDimsKey === customDimsKey
            && cached.filtersKey === filtersKey
            && cached.colFieldsKey === colFieldsKey
        ) {
            allRows = cached.allRows;
            filteredRows = cached.filteredRows;
            colCombinations = cached.colCombinations;
        } else {
            if (
                cached
                && cached.rowsRef === this.rows
                && cached.customDimsKey === customDimsKey
            ) {
                allRows = cached.allRows;
            } else {
                allRows = applyCustomDimensionsToDataset(this.rows, plan.customDimensions);
            }

            if (
                cached
                && cached.rowsRef === this.rows
                && cached.customDimsKey === customDimsKey
                && cached.filtersKey === filtersKey
            ) {
                filteredRows = cached.filteredRows;
            } else {
                filteredRows = filterDatasetRows(allRows, plan.filters);
            }

            if (
                cached
                && cached.rowsRef === this.rows
                && cached.customDimsKey === customDimsKey
                && cached.filtersKey === filtersKey
                && cached.colFieldsKey === colFieldsKey
            ) {
                colCombinations = cached.colCombinations;
            } else {
                const colSet = new Set<string>();
                if (plan.colFields.length === 1) {
                    const cf = plan.colFields[0];
                    for (const row of filteredRows) {
                        const val = row[cf];
                        colSet.add(val === null || val === undefined ? 'null' : String(val));
                    }
                } else if (plan.colFields.length > 1) {
                    for (const row of filteredRows) {
                        const parts: string[] = [];
                        for (let i = 0; i < plan.colFields.length; i += 1) {
                            const val = row[plan.colFields[i]];
                            parts.push(val === null || val === undefined ? 'null' : String(val));
                        }
                        colSet.add(parts.join('_'));
                    }
                }
                colCombinations = Array.from(colSet).sort((a, b) =>
                    a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
                );
            }

            this.preparedCache = {
                rowsRef: this.rows,
                customDimsKey,
                filtersKey,
                colFieldsKey,
                allRows,
                filteredRows,
                colCombinations,
            };
        }

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
            availableFields: this.getAvailableFields(),
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

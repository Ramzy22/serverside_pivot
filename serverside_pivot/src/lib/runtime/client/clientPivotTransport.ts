import type { PayloadReference, RuntimeTransportRequestOptions } from '../httpTransport';
import { RUNTIME_SCHEMA_VERSION, type JsonObject, type RuntimeRequestV1, type RuntimeResponseV1 } from '../runtimeContract';
import {
    buildPreparedExportPayload,
    ClientResponsePostProcessor,
    encodeBytesBase64,
    encodeBytesToUtf8,
} from './clientResponsePostProcessor';
import { ClientSqlPlanner } from './clientSqlPlanner';
import { applyCustomDimensionsToDataset, DuckDbWasmExecutor } from './duckdbWasmExecutor';
import type {
    ClientEngineTelemetry,
    ClientPivotTransportInterface,
    ClientRuntimeConfig,
    DrillThroughQuery,
    DrillThroughResult,
} from './types';

export interface ClientPivotTransportOptions extends ClientRuntimeConfig {
    data?: Array<{ [key: string]: unknown }>;
    tableName?: string;
}

interface StoredClientExport {
    content: string | Uint8Array;
    mimeType: string;
    filename?: string;
    byteLength: number;
}

interface StoredClientExportJob {
    jobId: string;
    status: 'completed' | 'cancelled';
    table: string;
    rowsWritten: number;
    bytesWritten: number;
    totalRows: number;
    payloadRef: PayloadReference;
}

interface SheetJsModuleLike {
    utils: {
        aoa_to_sheet: (data: unknown[][]) => { [key: string]: unknown };
        book_new: () => unknown;
        book_append_sheet: (wb: unknown, ws: unknown, name: string) => void;
    };
    write: (wb: unknown, opts: { bookType: 'xlsx'; type: 'array' }) => ArrayBuffer | Uint8Array | number[];
}

const MAX_RETAINED_CLIENT_EXPORTS = 25;
let cachedXlsxWriterPromise: Promise<SheetJsModuleLike> | null = null;

function defaultLoadXlsxWriter(): Promise<SheetJsModuleLike> {
    if (!cachedXlsxWriterPromise) {
        // @ts-ignore SheetJS mini build ships types only via package root exports under ES5/Node moduleResolution.
        cachedXlsxWriterPromise = import(/* webpackChunkName: "pivot-xlsx" */ 'xlsx/dist/xlsx.mini.min.js')
            .then((mod: any) => (mod && mod.default ? mod.default : mod) as SheetJsModuleLike);
    }
    return cachedXlsxWriterPromise;
}

export class ClientPivotTransport implements ClientPivotTransportInterface {
    private executor: DuckDbWasmExecutor;
    private config: ClientRuntimeConfig;
    private payloadStore: Map<string, JsonObject>;
    private exportStore: Map<string, StoredClientExport>;
    private exportJobStore: Map<string, StoredClientExportJob>;

    public constructor(options: ClientPivotTransportOptions = {}) {
        const { data = [], ...config } = options;
        this.config = { ...config };
        this.executor = new DuckDbWasmExecutor(Array.isArray(data) ? data : [], this.config);
        this.payloadStore = new Map();
        this.exportStore = new Map();
        this.exportJobStore = new Map();
    }

    public setData(rows: Array<{ [key: string]: unknown }>): void {
        this.executor.setData(Array.isArray(rows) ? rows : []);
    }

    public getData(): Array<{ [key: string]: unknown }> {
        return this.executor.getData();
    }

    public updateConfig(config: Partial<ClientRuntimeConfig>): void {
        this.config = { ...this.config, ...config };
        this.executor.updateConfig(config);
    }

    public getTelemetry(): ClientEngineTelemetry {
        return this.executor.getTelemetry();
    }

    private retainExportPayload(token: string, entry: StoredClientExport): void {
        this.exportStore.set(token, entry);
        while (this.exportStore.size > MAX_RETAINED_CLIENT_EXPORTS) {
            const oldestKey = this.exportStore.keys().next().value;
            if (oldestKey === undefined) break;
            this.exportStore.delete(oldestKey);
        }
    }

    private retainExportJob(jobId: string, entry: StoredClientExportJob): void {
        this.exportJobStore.set(jobId, entry);
        while (this.exportJobStore.size > MAX_RETAINED_CLIENT_EXPORTS) {
            const oldestKey = this.exportJobStore.keys().next().value;
            if (oldestKey === undefined) break;
            this.exportJobStore.delete(oldestKey);
        }
    }

    private async loadXlsxWriter(): Promise<SheetJsModuleLike> {
        if (typeof this.config.xlsxLoader === 'function') {
            const mod: any = await this.config.xlsxLoader();
            return (mod && mod.default ? mod.default : mod) as SheetJsModuleLike;
        }
        return defaultLoadXlsxWriter();
    }

    public async request(
        request: RuntimeRequestV1,
        options: RuntimeTransportRequestOptions = {}
    ): Promise<RuntimeResponseV1> {
        if (options.signal && options.signal.aborted) {
            throw new DOMException('Request aborted', 'AbortError');
        }
        const defaultTable = this.config.tableName || 'client_data';

        if (request && request.kind === 'export') {
            const reqPayload = (request.payload && typeof request.payload === 'object')
                ? (request.payload as { [key: string]: unknown })
                : {};
            const action = typeof reqPayload.action === 'string' ? reqPayload.action.trim().toLowerCase() : '';
            const requestId = request.requestId || `export:${Date.now()}`;
            const tableName = (typeof reqPayload.table === 'string' && reqPayload.table.trim())
                ? reqPayload.table.trim()
                : ((typeof request.table === 'string' && request.table.trim()) ? request.table.trim() : defaultTable);

            if (action === 'status' || action === 'cancel') {
                const jobId = typeof reqPayload.jobId === 'string' ? reqPayload.jobId : '';
                const existing = jobId ? this.exportJobStore.get(jobId) : undefined;
                if (!existing) {
                    return {
                        schemaVersion: RUNTIME_SCHEMA_VERSION,
                        kind: 'export',
                        status: 'error',
                        requestId,
                        table: tableName,
                        payload: { code: 'export_job_not_found' },
                    } as RuntimeResponseV1;
                }
                return {
                    schemaVersion: RUNTIME_SCHEMA_VERSION,
                    kind: 'export',
                    status: 'export_job',
                    requestId,
                    table: existing.table,
                    payload: {
                        jobId: existing.jobId,
                        phase: existing.status,
                        rowsWritten: existing.rowsWritten,
                        bytesWritten: existing.bytesWritten,
                        totalRows: existing.totalRows,
                        ...(existing.status === 'completed' ? { payloadRef: existing.payloadRef as unknown as JsonObject } : {}),
                    },
                } as RuntimeResponseV1;
            }

            const plan = ClientSqlPlanner.compile(request, defaultTable);
            const executed = await this.executor.execute(plan);
            const prepared = ClientResponsePostProcessor.prepareExport(plan, executed);

            let storedContent: string | Uint8Array;
            let byteLength = 0;
            let base64Data = '';

            if (prepared.format === 'xlsx') {
                const XLSX = await this.loadXlsxWriter();
                if (options.signal && options.signal.aborted) {
                    throw new DOMException('Request aborted', 'AbortError');
                }
                const aoa = [...prepared.headerRows, ...prepared.rows];
                const worksheet = XLSX.utils.aoa_to_sheet(aoa);
                if (prepared.merges.length > 0) {
                    worksheet['!merges'] = prepared.merges;
                }
                if (prepared.colWidths.length > 0) {
                    worksheet['!cols'] = prepared.colWidths.map((width) => ({
                        wch: Math.min(Math.max(width + 2, 8), 60),
                    }));
                }
                const workbook = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(workbook, worksheet, 'Pivot');
                const rawBytes = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
                const bytes = rawBytes instanceof Uint8Array ? rawBytes : new Uint8Array(rawBytes);
                storedContent = bytes;
                byteLength = bytes.byteLength;
                base64Data = encodeBytesBase64(bytes);
            } else {
                const utf8Bytes = encodeBytesToUtf8(prepared.content);
                storedContent = prepared.content;
                byteLength = utf8Bytes.byteLength;
                base64Data = encodeBytesBase64(utf8Bytes);
            }

            const token = `client_export_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            this.retainExportPayload(token, {
                content: storedContent,
                mimeType: prepared.mimeType,
                filename: prepared.filename,
                byteLength,
            });
            const payloadRef: PayloadReference = {
                id: token,
                url: token,
                filename: prepared.filename,
                contentType: prepared.mimeType,
                bytes: byteLength,
            };

            if (action === 'start') {
                const jobId = `client_job_${token}`;
                this.retainExportJob(jobId, {
                    jobId,
                    status: 'completed',
                    table: plan.table,
                    rowsWritten: prepared.rowCount,
                    bytesWritten: byteLength,
                    totalRows: prepared.rowCount,
                    payloadRef,
                });
                return ClientResponsePostProcessor.buildEnvelope(plan, 'export', 'export_job', {
                    jobId,
                    phase: 'completed',
                    rowsWritten: prepared.rowCount,
                    bytesWritten: byteLength,
                    totalRows: prepared.rowCount,
                    payloadRef: payloadRef as unknown as JsonObject,
                });
            }

            const okPayload = {
                ...buildPreparedExportPayload(prepared, base64Data),
                payloadRef: payloadRef as unknown as JsonObject,
            };
            return ClientResponsePostProcessor.buildEnvelope(plan, 'export', 'ok', okPayload, {
                format: prepared.format,
                filename: prepared.filename,
                mimeType: prepared.mimeType,
                content: typeof storedContent === 'string' ? storedContent : '',
                rowCount: prepared.rowCount,
            });
        }

        const plan = ClientSqlPlanner.compile(request, defaultTable);
        const executed = await this.executor.execute(plan);
        return ClientResponsePostProcessor.buildResponse(plan, executed);
    }

    public async queryDrillThrough(query: DrillThroughQuery): Promise<DrillThroughResult> {
        const syntheticRequest = {
            schemaVersion: 1 as const,
            requestId: `drill:${Date.now()}`,
            kind: 'drill' as const,
            payload: {
                table: query.table || this.config.tableName || 'client_data',
                rowFields: query.rowFields,
                filters: (query.filters || {}) as unknown as JsonObject,
                customDimensions: (query.customDimensions || []) as unknown as JsonObject[],
                row_path: query.rowPath,
                col_field: query.colField || null,
                col_val: query.colVal || null,
                page: query.page ?? 0,
                page_size: query.pageSize ?? 100,
                sort_col: query.sortCol || null,
                sort_dir: query.sortDir || 'asc',
                filter_text: query.filterText || '',
            },
        };
        const plan = ClientSqlPlanner.compile(syntheticRequest as unknown as RuntimeRequestV1, this.config.tableName || 'client_data');
        const allRows = applyCustomDimensionsToDataset(this.executor.getData(), plan.customDimensions);
        return ClientResponsePostProcessor.executeDrillThroughQuery(allRows, plan, query);
    }

    public async fetchPayload(
        reference: PayloadReference,
        _options?: RuntimeTransportRequestOptions
    ): Promise<JsonObject> {
        const key = reference && typeof reference === 'object' ? (reference.id || reference.url) : String(reference);
        return this.payloadStore.get(key) ?? {};
    }

    public async fetchExportPayload(
        reference: PayloadReference,
        _options?: RuntimeTransportRequestOptions
    ): Promise<Blob> {
        const key = reference && typeof reference === 'object' ? (reference.url || reference.id || '') : String(reference || '');
        const entry = this.exportStore.get(key);
        if (!entry) {
            throw new Error(`Client export payload not found for reference "${key}".`);
        }
        const blobPart: BlobPart = entry.content instanceof Uint8Array
            ? entry.content.slice().buffer
            : entry.content;
        return new Blob([blobPart], { type: entry.mimeType || 'text/plain;charset=utf-8;' });
    }
}

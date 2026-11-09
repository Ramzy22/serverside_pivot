import type { PayloadReference, RuntimeTransportRequestOptions } from '../httpTransport';
import type { JsonObject, RuntimeRequestV1, RuntimeResponseV1 } from '../runtimeContract';
import { ClientResponsePostProcessor } from './clientResponsePostProcessor';
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

export class ClientPivotTransport implements ClientPivotTransportInterface {
    private executor: DuckDbWasmExecutor;
    private config: ClientRuntimeConfig;
    private payloadStore: Map<string, JsonObject>;
    private exportStore: Map<string, { content: string; mimeType: string; filename?: string }>;

    public constructor(options: ClientPivotTransportOptions = {}) {
        const { data = [], ...config } = options;
        this.config = { ...config };
        this.executor = new DuckDbWasmExecutor(Array.isArray(data) ? data : [], this.config);
        this.payloadStore = new Map();
        this.exportStore = new Map();
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

    public async request(
        request: RuntimeRequestV1,
        options: RuntimeTransportRequestOptions = {}
    ): Promise<RuntimeResponseV1> {
        if (options.signal && options.signal.aborted) {
            throw new DOMException('Request aborted', 'AbortError');
        }
        const defaultTable = this.config.tableName || 'client_data';
        const plan = ClientSqlPlanner.compile(request, defaultTable);
        const executed = await this.executor.execute(plan);
        const response = ClientResponsePostProcessor.buildResponse(plan, executed);
        if (response.kind === 'export' && typeof (response as unknown as { content?: string }).content === 'string') {
            const token = `client_export_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            const rawResp = response as unknown as { content: string; mimeType?: string; filename?: string };
            this.exportStore.set(token, {
                content: rawResp.content,
                mimeType: String(rawResp.mimeType || 'text/csv;charset=utf-8;'),
                filename: typeof rawResp.filename === 'string' ? rawResp.filename : undefined,
            });
        }
        return response;
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
        const key = reference && typeof reference === 'object' ? (reference.id || reference.url) : String(reference);
        const entry = this.exportStore.get(key);
        const content = entry ? entry.content : '';
        const mimeType = entry ? entry.mimeType : 'text/plain;charset=utf-8;';
        return new Blob([content], { type: mimeType });
    }
}

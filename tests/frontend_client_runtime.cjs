const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');

const frontend = path.resolve(__dirname, '../serverside_pivot');
const localRequire = createRequire(path.join(frontend, 'package.json'));
const babel = localRequire('@babel/core');
const presets = [
    [localRequire('@babel/preset-env'), { targets: { node: 'current' }, modules: 'commonjs' }],
    localRequire('@babel/preset-typescript'),
    localRequire('@babel/preset-react'),
];

function loadClientModule() {
    const cache = new Map();
    function load(filename) {
        const candidates = ['', '.ts', '.tsx', '.js', '/index.ts', '/index.js'];
        const resolved = candidates
            .map((ext) => filename + ext)
            .find((file) => fs.existsSync(file) && fs.statSync(file).isFile());
        if (!resolved) {
            throw new Error(`Cannot resolve module: ${filename}`);
        }
        if (cache.has(resolved)) return cache.get(resolved).exports;
        const mod = { exports: {} };
        cache.set(resolved, mod);
        const code = babel.transformSync(fs.readFileSync(resolved, 'utf8'), {
            filename: resolved,
            presets,
        }).code;
        const requireSource = (specifier) => {
            if (specifier.startsWith('.')) {
                return load(path.resolve(path.dirname(resolved), specifier));
            }
            return localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    }
    return load(path.join(frontend, 'src/lib/runtime/client/index.ts'));
}

const SAMPLE_DATASET = [
    { id: 1, parent_id: null, name: 'Global HQ', region: 'AMER', country: 'USA', year: '2024', revenue: 1000, cost: 600, units: 10, weight: 2 },
    { id: 2, parent_id: 1, name: 'US East', region: 'AMER', country: 'USA', year: '2025', revenue: 1500, cost: 800, units: 15, weight: 3 },
    { id: 3, parent_id: 1, name: 'Canada Ops', region: 'AMER', country: 'Canada', year: '2024', revenue: 500, cost: 300, units: 5, weight: 1 },
    { id: 4, parent_id: 1, name: 'Canada Ops 2', region: 'AMER', country: 'Canada', year: '2025', revenue: 700, cost: 400, units: 7, weight: 2 },
    { id: 5, parent_id: null, name: 'EMEA HQ', region: 'EMEA', country: 'UK', year: '2024', revenue: 800, cost: 500, units: 8, weight: 2 },
    { id: 6, parent_id: 5, name: 'UK North', region: 'EMEA', country: 'UK', year: '2025', revenue: 1200, cost: 700, units: 12, weight: 4 },
    { id: 7, parent_id: 5, name: 'France Ops', region: 'EMEA', country: 'France', year: '2024', revenue: 400, cost: 250, units: 4, weight: 1 },
    { id: 8, parent_id: 5, name: 'France Ops 2', region: 'EMEA', country: 'France', year: '2025', revenue: 600, cost: 350, units: 6, weight: 1 },
];

// Optional batch runner mode for Python differential parity suite
if (process.env.PIVOT_CLIENT_BATCH_INPUT && process.env.PIVOT_CLIENT_BATCH_OUTPUT) {
    (async () => {
        const clientRuntime = loadClientModule();
        const { ClientPivotTransport } = clientRuntime;
        const input = JSON.parse(fs.readFileSync(process.env.PIVOT_CLIENT_BATCH_INPUT, 'utf8'));
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            clientEngine: input.clientEngine || 'auto',
            data: input.data || SAMPLE_DATASET,
            tableName: input.tableName || 'sales',
        });
        const responses = [];
        for (const req of input.requests || []) {
            if (req.__kind === 'queryDrillThrough') {
                responses.push(await transport.queryDrillThrough(req.query));
            } else {
                responses.push(await transport.request(req));
            }
        }
        fs.writeFileSync(
            process.env.PIVOT_CLIENT_BATCH_OUTPUT,
            JSON.stringify({
                responses,
                telemetry: transport.getTelemetry(),
            }),
            'utf8'
        );
        process.exit(0);
    })().catch((err) => {
        console.error(err);
        process.exit(1);
    });
} else {
    test('modeResolver resolves auto, forced client, forced server, and engine tiers', () => {
        const {
            resolveExecutionMode,
            resolveClientEngineKind,
        } = loadClientModule();

        // 1. Auto mode with in-memory data only -> client
        assert.equal(resolveExecutionMode({ data: SAMPLE_DATASET }), 'client');

        // 2. Auto mode with dataUrl only -> client
        assert.equal(
            resolveExecutionMode({ performanceConfig: { dataUrl: 'https://example.com/sales.parquet' } }),
            'client'
        );

        // 3. Auto mode with server table only -> server
        assert.equal(resolveExecutionMode({ table: 'warehouse_sales' }), 'server');

        // 4. Auto mode with both data (small) and table -> client (below 50k threshold)
        assert.equal(
            resolveExecutionMode({
                data: SAMPLE_DATASET,
                table: 'warehouse_sales',
                performanceConfig: { executionMode: 'auto', clientRowThreshold: 50000 },
            }),
            'client'
        );

        // 5. Auto mode with data exceeding threshold and table -> server
        assert.equal(
            resolveExecutionMode({
                data: SAMPLE_DATASET,
                table: 'warehouse_sales',
                performanceConfig: { executionMode: 'auto', clientRowThreshold: 5 },
            }),
            'server'
        );

        // 6. Forced client overrides serverSide=true and table
        assert.equal(
            resolveExecutionMode({
                serverSide: true,
                table: 'warehouse_sales',
                performanceConfig: { executionMode: 'client' },
            }),
            'client'
        );

        // 7. Forced server overrides serverSide=false and data
        assert.equal(
            resolveExecutionMode({
                serverSide: false,
                data: SAMPLE_DATASET,
                performanceConfig: { executionMode: 'server' },
            }),
            'server'
        );

        // 8. Engine tier resolution
        assert.equal(resolveClientEngineKind({ clientEngine: 'auto' }, 200), 'memory');
        assert.equal(resolveClientEngineKind({ clientEngine: 'auto', duckdbWasmThreshold: 500 }, 600), 'duckdb-wasm');
        assert.equal(resolveClientEngineKind({ clientEngine: 'auto', dataUrl: '/data.parquet' }, 10), 'duckdb-wasm');
        assert.equal(resolveClientEngineKind({ clientEngine: 'duckdb-wasm' }, 10), 'duckdb-wasm');
        assert.equal(resolveClientEngineKind({ clientEngine: 'memory' }, 50000), 'memory');
    });

    test('ClientSqlPlanner compiles CTE custom dimensions, filters, and OLAP aggregations to DuckDB SQL', () => {
        const { ClientSqlPlanner } = loadClientModule();
        const plan = ClientSqlPlanner.compile(
            {
                schemaVersion: 1,
                requestId: 'plan-1',
                kind: 'data',
                table: 'sales',
                state: {
                    rowFields: ['region', 'deal_tier'],
                    colFields: ['year'],
                    valConfigs: [
                        { field: 'revenue', agg: 'sum' },
                        { field: 'revenue', agg: 'weighted_avg', weightField: 'weight' },
                        { field: 'country', agg: 'count_distinct' },
                    ],
                    customDimensions: [
                        {
                            id: 'deal_tier',
                            field: 'revenue',
                            name: 'Deal Tier',
                            defaultBucket: 'Standard',
                            rules: [{ label: 'Enterprise', op: 'gte', value: 1000 }],
                        },
                    ],
                    filters: {
                        region: {
                            operator: 'AND',
                            conditions: [{ type: 'in', values: ['AMER', 'EMEA'] }],
                        },
                    },
                },
                payload: { start: 0, end: 47 },
            },
            'sales'
        );

        assert.match(plan.sql.groupingSql, /WITH __source AS/i);
        assert.match(plan.sql.groupingSql, /CASE WHEN TRY_CAST\("revenue" AS DOUBLE\) >= 1000 THEN 'Enterprise' ELSE 'Standard' END AS "deal_tier"/);
        assert.match(plan.sql.groupingSql, /WHERE CAST\("region" AS VARCHAR\) IN \('AMER', 'EMEA'\)/);
        assert.match(plan.sql.groupingSql, /SUM\(TRY_CAST\("revenue" AS DOUBLE\)\) AS "revenue_sum"/);
        assert.match(plan.sql.groupingSql, /COUNT\(DISTINCT "country"\) AS "country_count_distinct"/);
        assert.match(plan.sql.colValuesSql, /SELECT DISTINCT "year" FROM __source/);
    });

    test('ClientPivotTransport executes pivot, hierarchy expansion, totals, formulas, windowFn, and measureAxis', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
        });

        const response = await transport.request({
            schemaVersion: 1,
            requestId: 'req-pivot-1',
            kind: 'data',
            table: 'sales',
            state: {
                viewMode: 'pivot',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [
                    { field: 'revenue', agg: 'sum' },
                    { field: 'cost', agg: 'sum' },
                    {
                        field: '__formula__',
                        agg: 'formula',
                        id: 'profit_calc',
                        label: 'Profit',
                        formula: 'revenue_sum - cost_sum',
                        formulaScope: 'measures',
                    },
                    {
                        field: 'revenue',
                        agg: 'sum',
                        id: 'rev_pct',
                        label: 'Rev % of Total',
                        windowFn: 'percent_of_grand_total',
                    },
                ],
                expanded: { AMER: true },
                showRowTotals: true,
                showColTotals: true,
            },
            payload: {
                start: 0,
                end: 47,
                layout_mode: 'hierarchy',
            },
        });

        assert.equal(response.schemaVersion, 1);
        assert.equal(response.kind, 'data');
        assert.equal(response.status, 'data');
        assert.ok(Array.isArray(response.data));

        // AMER (expanded -> USA, Canada), EMEA (collapsed), Grand Total
        const amerRow = response.data.find((r) => r._path === 'AMER');
        const usaRow = response.data.find((r) => r._path === 'AMER|||USA');
        const canadaRow = response.data.find((r) => r._path === 'AMER|||Canada');
        const emeaRow = response.data.find((r) => r._path === 'EMEA');
        const gtRow = response.data.find((r) => r.__isGrandTotal__);

        assert.ok(amerRow, 'AMER group row exists');
        assert.ok(usaRow, 'Expanded USA child row exists');
        assert.ok(canadaRow, 'Expanded Canada child row exists');
        assert.ok(emeaRow, 'EMEA group row exists');
        assert.ok(gtRow, 'Grand Total row exists');

        // Check AMER 2024 & 2025 revenue_sum and formula profit_calc
        assert.equal(amerRow['2024_revenue_sum'], 1500);
        assert.equal(amerRow['2025_revenue_sum'], 2200);
        assert.equal(amerRow['2024_cost_sum'], 900);
        assert.equal(amerRow['2024_profit_calc'], 600);
        assert.equal(amerRow['__RowTotal____revenue_sum'], 3700);
        assert.equal(gtRow['__RowTotal____revenue_sum'], 6700);

        // Check percent_of_grand_total sums to 1.0 on grand total row total (and 2700/6700 on 2024 col)
        assert.ok(Math.abs(gtRow['__RowTotal__rev_pct'] - 1.0) < 1e-9);
        assert.ok(Math.abs(gtRow['2024_rev_pct'] - (2700 / 6700)) < 1e-9);

        // Check telemetry reports 0 network roundtrips
        const telemetry = transport.getTelemetry();
        assert.equal(telemetry.networkRoundtrips, 0);
        assert.equal(telemetry.rowCount, SAMPLE_DATASET.length);
    });

    test('ClientPivotTransport supports tree, report, table, filter_options, chart, detail, drill, and export', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
        });

        // 1. Tree mode (adjacency parentIdField / idField)
        const treeRes = await transport.request({
            schemaVersion: 1,
            requestId: 'tree-1',
            kind: 'data',
            state: {
                viewMode: 'tree',
                treeConfig: {
                    mode: 'adjacency',
                    idField: 'id',
                    parentIdField: 'parent_id',
                    labelField: 'name',
                },
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: { 1: true },
            },
            payload: { start: 0, end: 47 },
        });
        assert.equal(treeRes.status, 'data');
        assert.ok(treeRes.data.some((r) => r.hierarchy === 'Global HQ'));
        assert.ok(treeRes.data.some((r) => r.hierarchy === 'US East' && r.depth === 1));

        // 2. Report mode with topN and Other row
        const reportRes = await transport.request({
            schemaVersion: 1,
            requestId: 'report-1',
            kind: 'data',
            state: {
                viewMode: 'report',
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                reportDef: {
                    root: {
                        field: 'country',
                        label: 'Top Countries',
                        topN: 2,
                        sortBy: 'revenue_sum',
                        sortDir: 'desc',
                        includeOther: true,
                    },
                },
            },
            payload: { start: 0, end: 47 },
        });
        assert.equal(reportRes.status, 'data');
        assert.ok(reportRes.data.some((r) => r.__isOther__ === true));

        // 3. Filter options
        const filterRes = await transport.request({
            schemaVersion: 1,
            requestId: 'filter-1',
            kind: 'filter_options',
            state: {},
            payload: { columnId: 'country' },
        });
        assert.equal(filterRes.status, 'ok');
        assert.deepEqual(filterRes.payload.options, ['Canada', 'France', 'UK', 'USA']);

        // 4. Chart request
        const chartRes = await transport.request({
            schemaVersion: 1,
            requestId: 'chart-1',
            kind: 'chart',
            state: {
                rowFields: ['region'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
            },
            payload: { pane_id: 'pane-1' },
        });
        assert.equal(chartRes.status, 'chart_data');
        assert.equal(chartRes.payload.rows.length, 2);

        // 5. Detail & Drill-through
        const drillRes = await transport.queryDrillThrough({
            rowPath: 'AMER|||USA',
            rowFields: ['region', 'country'],
            page: 0,
            pageSize: 10,
        });
        assert.equal(drillRes.totalRows, 2);
        assert.ok(drillRes.rows.every((r) => r.country === 'USA'));

        // 6. Export (CSV)
        const exportRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-1',
            kind: 'export',
            state: {
                rowFields: ['region'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
            },
            payload: { format: 'csv', filename: 'client_sales.csv' },
        });
        assert.equal(exportRes.status, 'ok');
        assert.equal(exportRes.payload.filename, 'client_sales.csv');
        assert.ok(typeof exportRes.payload.data === 'string' && exportRes.payload.data.length > 0);
        assert.match(exportRes.content, /AMER/);
        assert.match(exportRes.content, /3700/);
    });

    test('ClientPivotTransport supports Expand All with single-row collapse overrides (__ALL__ + false and __COLLAPSED__)', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
        });

        const expandAllCollapsedAmer = await transport.request({
            schemaVersion: 1,
            requestId: 'exp-all-collapse-1',
            kind: 'data',
            state: {
                viewMode: 'pivot',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: { __ALL__: true, AMER: false },
                showRowTotals: true,
                showColTotals: true,
            },
            payload: { start: 0, end: 47, version: 7, window_seq: 7, state_epoch: 2 },
        });

        assert.equal(expandAllCollapsedAmer.status, 'data');
        assert.equal(expandAllCollapsedAmer.payload.dataVersion, 7);
        assert.equal(expandAllCollapsedAmer.payload.version, 7);
        assert.equal(expandAllCollapsedAmer.payload.windowSeq, 7);
        assert.equal(expandAllCollapsedAmer.payload.stateEpoch, 2);

        const paths = expandAllCollapsedAmer.data.map((r) => r._path);
        assert.ok(paths.includes('AMER'), 'AMER group row remains visible');
        assert.ok(!paths.includes('AMER|||USA'), 'AMER child USA is collapsed');
        assert.ok(!paths.includes('AMER|||Canada'), 'AMER child Canada is collapsed');
        assert.ok(paths.includes('EMEA'), 'EMEA group row remains visible');
        assert.ok(paths.includes('EMEA|||UK'), 'EMEA child UK remains expanded');
        assert.ok(paths.includes('EMEA|||France'), 'EMEA child France remains expanded');

        const amerRec = expandAllCollapsedAmer.data.find((r) => r._path === 'AMER');
        const emeaRec = expandAllCollapsedAmer.data.find((r) => r._path === 'EMEA');
        assert.equal(amerRec._is_expanded, false);
        assert.equal(emeaRec._is_expanded, true);

        const arrayTokenRes = await transport.request({
            schemaVersion: 1,
            requestId: 'exp-all-collapse-2',
            kind: 'data',
            state: {
                viewMode: 'pivot',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: ['__ALL__', ['__COLLAPSED__', 'EMEA']],
                showRowTotals: true,
                showColTotals: true,
            },
            payload: { start: 0, end: 47, version: 8, window_seq: 8, state_epoch: 2 },
        });

        const arrayPaths = arrayTokenRes.data.map((r) => r._path);
        assert.ok(arrayPaths.includes('AMER|||USA'), 'AMER child USA expanded via __ALL__');
        assert.ok(!arrayPaths.includes('EMEA|||UK'), 'EMEA child UK collapsed via __COLLAPSED__');
    });

    test('ClientPivotTransport matches server hierarchy/outline vs tabular subtotal parity when show_subtotal_footers is true', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
        });

        for (const layoutMode of ['hierarchy', 'outline']) {
            const res = await transport.request({
                schemaVersion: 1,
                requestId: `subtotal-parity-${layoutMode}`,
                kind: 'data',
                state: {
                    viewMode: 'pivot',
                    rowFields: ['region', 'country'],
                    colFields: ['year'],
                    valConfigs: [{ field: 'revenue', agg: 'sum' }],
                    expanded: { __ALL__: true },
                    showRowTotals: true,
                    showColTotals: true,
                    show_subtotal_footers: true,
                    layoutMode,
                },
                payload: { start: 0, end: 47, version: 1, window_seq: 1, state_epoch: 1 },
            });

            const nonGrandRows = res.data.filter((r) => r._path !== '__grand_total__' && r._id !== 'Grand Total');
            assert.equal(nonGrandRows.length, 6, `${layoutMode} should emit 2 parents + 4 leaves without footer copies`);
            assert.ok(
                nonGrandRows.every((r) => !r._isTabularSubtotal && !String(r._path || '').includes('__subtotal__')),
                `${layoutMode} must not emit duplicate __subtotal__ footer rows`
            );
        }

        const tabularRes = await transport.request({
            schemaVersion: 1,
            requestId: 'subtotal-parity-tabular',
            kind: 'data',
            state: {
                viewMode: 'pivot',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: {},
                showRowTotals: true,
                showColTotals: true,
                show_subtotal_footers: true,
                layoutMode: 'tabular',
            },
            payload: { start: 0, end: 47, version: 2, window_seq: 2, state_epoch: 1 },
        });

        const tabularSubtotals = tabularRes.data.filter((r) => r._isTabularSubtotal === true);
        assert.equal(tabularSubtotals.length, 2, 'tabular with show_subtotal_footers should emit 2 post-order region subtotals');
        const tabularLeaves = tabularRes.data.filter((r) => !r._isTotal && !r._isTabularSubtotal);
        assert.equal(tabularLeaves.length, 4, 'tabular should expand all 4 country leaves even when expanded is empty');
    });

    test('DuckDbWasmExecutor falls back to memory engine when WASM initialization fails', async () => {
        const { ClientSqlPlanner, DuckDbWasmExecutor } = loadClientModule();
        const executor = new DuckDbWasmExecutor(SAMPLE_DATASET, {
            executionMode: 'client',
            clientEngine: 'duckdb-wasm',
            tableName: 'sales',
            duckdbLoader: async () => {
                throw new Error('WASM initialization timed out after 4500ms.');
            },
        });

        const plan = ClientSqlPlanner.compile(
            {
                schemaVersion: 1,
                requestId: 'wasm-fallback-1',
                kind: 'data',
                state: {
                    viewMode: 'pivot',
                    rowFields: ['region'],
                    colFields: ['year'],
                    valConfigs: [{ field: 'revenue', agg: 'sum' }],
                    expanded: {},
                },
                payload: { start: 0, end: 10, version: 1, window_seq: 1, state_epoch: 1 },
            },
            'sales'
        );

        const executed = await executor.execute(plan);
        assert.equal(executed.engineUsed, 'memory');
        assert.equal(executor.getTelemetry().activeEngine, 'memory');
        assert.equal(executor.getTelemetry().wasmReady, false);
    });

    test('ClientPivotTransport supports UI export_job lifecycle for CSV and real XLSX workbooks', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const xlsxLib = localRequire('xlsx/dist/xlsx.mini.min.js');
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
            xlsxLoader: async () => xlsxLib,
        });

        // 1. UI export_job CSV with colIds including hierarchy, allRows: true, and custom headerLabels
        const csvJobRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-job-csv-1',
            kind: 'export',
            state: {
                viewMode: 'pivot',
                layoutMode: 'outline',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: {},
                showRowTotals: true,
                showColTotals: true,
            },
            payload: {
                action: 'start',
                format: 'csv',
                allRows: true,
                colIds: ['hierarchy', '2024_revenue_sum', '2025_revenue_sum', '__RowTotal__revenue_sum'],
                styleOptions: {
                    includeHeaders: true,
                    headerLabels: {
                        hierarchy: 'Geography',
                        '2024_revenue_sum': '2024 Revenue',
                        '2025_revenue_sum': '2025 Revenue',
                        '__RowTotal__revenue_sum': 'Total Revenue',
                    },
                },
            },
        });
        assert.equal(csvJobRes.status, 'export_job');
        assert.equal(csvJobRes.payload.phase, 'completed');
        assert.ok(typeof csvJobRes.payload.jobId === 'string' && csvJobRes.payload.jobId.length > 0);
        assert.ok(csvJobRes.payload.rowsWritten >= 6);
        assert.ok(csvJobRes.payload.payloadRef && csvJobRes.payload.payloadRef.url);
        assert.match(csvJobRes.payload.payloadRef.filename, /\.csv$/);

        const csvBlob = await transport.fetchExportPayload(csvJobRes.payload.payloadRef);
        const csvText = await csvBlob.text();
        assert.match(csvText, /^Geography,2024 Revenue,2025 Revenue,Total Revenue/m);
        assert.match(csvText, /AMER/);
        assert.match(csvText, /  USA/);
        assert.match(csvText, /Grand Total/);

        // 2. status & cancel polling for known and missing job IDs
        const statusRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-job-status-1',
            kind: 'export',
            payload: { action: 'status', jobId: csvJobRes.payload.jobId, table: 'sales' },
        });
        assert.equal(statusRes.status, 'export_job');
        assert.equal(statusRes.payload.phase, 'completed');

        const missingRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-job-status-missing',
            kind: 'export',
            payload: { action: 'status', jobId: 'nonexistent-job', table: 'sales' },
        });
        assert.equal(missingRes.status, 'error');
        assert.equal(missingRes.payload.code, 'export_job_not_found');

        // 3. UI export_job XLSX generates genuine ZIP/OOXML workbook with merged headers & numeric values
        const xlsxJobRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-job-xlsx-1',
            kind: 'export',
            state: {
                viewMode: 'pivot',
                layoutMode: 'outline',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                expanded: {},
                showRowTotals: true,
                showColTotals: true,
            },
            payload: {
                action: 'start',
                format: 'xlsx',
                allRows: true,
                colIds: ['hierarchy', '2024_revenue_sum', '2025_revenue_sum', '__RowTotal__revenue_sum'],
                styleOptions: {
                    includeHeaders: true,
                    mergedHeaders: true,
                    headerLabels: {
                        hierarchy: 'Geography',
                        '2024_revenue_sum': '2024 Revenue',
                        '2025_revenue_sum': '2025 Revenue',
                        '__RowTotal__revenue_sum': 'Total Revenue',
                    },
                    mergedHeaderRows: [
                        [
                            { label: 'Geography', columnIds: ['hierarchy'], colSpan: 1, rowSpan: 2 },
                            { label: 'Fiscal Years', columnIds: ['2024_revenue_sum', '2025_revenue_sum'], colSpan: 2, rowSpan: 1 },
                            { label: 'Total', columnIds: ['__RowTotal__revenue_sum'], colSpan: 1, rowSpan: 2 },
                        ],
                        [
                            { label: '2024', columnIds: ['2024_revenue_sum'], colSpan: 1, rowSpan: 1 },
                            { label: '2025', columnIds: ['2025_revenue_sum'], colSpan: 1, rowSpan: 1 },
                        ],
                    ],
                },
            },
        });
        assert.equal(xlsxJobRes.status, 'export_job');
        assert.equal(xlsxJobRes.payload.phase, 'completed');
        assert.match(xlsxJobRes.payload.payloadRef.filename, /\.xlsx$/);

        const xlsxBlob = await transport.fetchExportPayload(xlsxJobRes.payload.payloadRef);
        const xlsxBytes = new Uint8Array(await xlsxBlob.arrayBuffer());
        // Valid ZIP/OOXML magic header "PK\x03\x04"
        assert.equal(xlsxBytes[0], 0x50);
        assert.equal(xlsxBytes[1], 0x4b);

        const xlsxFull = localRequire('xlsx');
        const wb = xlsxFull.read(xlsxBytes, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]];
        assert.ok(Array.isArray(ws['!merges']) && ws['!merges'].length >= 2);
        const sheetRows = xlsxFull.utils.sheet_to_json(ws, { header: 1 });
        assert.equal(sheetRows[0][0], 'Geography');
        assert.equal(sheetRows[0][1], 'Fiscal Years');
        assert.equal(sheetRows[1][1], '2024');
        assert.equal(sheetRows[1][2], '2025');
        assert.ok(sheetRows.some((r) => String(r[0]).includes('AMER') && r[3] === 3700));

        // 4. Tabular part export expands hierarchy -> rowFields and respects rowStart/rowEnd
        const tabularPartRes = await transport.request({
            schemaVersion: 1,
            requestId: 'export-part-tabular-1',
            kind: 'export',
            state: {
                viewMode: 'pivot',
                layoutMode: 'tabular',
                rowFields: ['region', 'country'],
                colFields: ['year'],
                valConfigs: [{ field: 'revenue', agg: 'sum' }],
                showRowTotals: true,
                showColTotals: false,
            },
            payload: {
                action: 'start',
                format: 'csv',
                partId: 'r0_c0',
                partLabel: 'rows1-2',
                rowStart: 0,
                rowEnd: 2,
                allRows: false,
                colIds: ['hierarchy', '2024_revenue_sum', '__RowTotal__revenue_sum'],
            },
        });
        assert.equal(tabularPartRes.status, 'export_job');
        assert.equal(tabularPartRes.payload.rowsWritten, 2);
        const partCsv = await (await transport.fetchExportPayload(tabularPartRes.payload.payloadRef)).text();
        assert.match(partCsv, /^region,country,/i);
    });

    test('createRuntimeExportCommands + useRuntimeResponseLifecycle delivers client XLSX/CSV exports end-to-end', async () => {
        const { ClientPivotTransport } = loadClientModule();
        const xlsxLib = localRequire('xlsx/dist/xlsx.mini.min.js');
        const transport = new ClientPivotTransport({
            executionMode: 'client',
            data: SAMPLE_DATASET,
            tableName: 'sales',
            xlsxLoader: async () => xlsxLib,
        });

        const cache = new Map();
        const presets = [
            [localRequire('@babel/preset-env'), { targets: { node: 'current' }, modules: 'commonjs' }],
            localRequire('@babel/preset-typescript'),
            localRequire('@babel/preset-react'),
        ];
        function loadRuntime(name) {
            const base = path.join(frontend, 'src/lib/runtime', name);
            const filename = ['', '.js', '.ts', '.tsx'].map((ext) => base + ext).find((f) => fs.existsSync(f));
            if (cache.has(filename)) return cache.get(filename).exports;
            const mod = { exports: {} };
            cache.set(filename, mod);
            const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), { filename, presets }).code;
            const requireSource = (spec) => spec === 'react'
                ? { useEffect: (ef) => ef(), useRef: (init) => ({ current: init }) }
                : (spec.startsWith('.') ? loadRuntime(path.relative(path.join(frontend, 'src/lib/runtime'), path.resolve(path.dirname(filename), spec))) : localRequire(spec));
            new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
            return mod.exports;
        }

        const { createRuntimeExportCommands } = loadRuntime('createRuntimeExportCommands');
        const { useRuntimeResponseLifecycle } = loadRuntime('useRuntimeResponseLifecycle');

        const downloads = [];
        const notices = [];
        const exportState = { exporting: false, progress: null };
        const exportJobControllerRef = { current: null };
        const handledKeys = { current: new Set() };
        const originalCreateObjectURL = URL.createObjectURL;
        const originalRevokeObjectURL = URL.revokeObjectURL;
        const createdBlobs = [];
        URL.createObjectURL = (blob) => {
            const idx = createdBlobs.push(blob);
            return `blob:client-test-${idx}`;
        };
        URL.revokeObjectURL = () => {};

        const pendingPromises = [];
        const opts = {
            abortGenerationRef: { current: 1 },
            activeExportRequestIdRef: { current: null },
            cancelledExportRequestIdsRef: { current: new Set() },
            clientInstanceRef: { current: 'client-test' },
            exportRequestSequenceRef: { current: 0 },
            exportJobControllerRef,
            pendingCopyIntentRef: { current: null },
            buildExportStyleProfile: () => ({ includeHeaders: true, headerLabels: { hierarchy: 'Region', revenue_sum: 'Revenue' } }),
            getServerExportColumnTarget: () => ({ mode: 'ids', colIds: ['hierarchy', 'revenue_sum'], knownColIds: ['hierarchy', 'revenue_sum'], totalCols: 2 }),
            dispatchServerSideRuntimeSetProps: (update) => {
                const req = {
                    ...update.runtimeRequest,
                    state: {
                        viewMode: 'pivot',
                        layoutMode: 'outline',
                        rowFields: ['region'],
                        colFields: [],
                        valConfigs: [{ field: 'revenue', agg: 'sum' }],
                        expanded: {},
                    },
                };
                const p = transport.request(req).then((response) => {
                    useRuntimeResponseLifecycle({
                        ...opts,
                        payloadTransport: transport,
                        runtimeResponse: response,
                        handledRuntimeResponseKeysRef: handledKeys,
                        recordProfilerResponse: () => {},
                        showNotification: (...args) => notices.push(args),
                        triggerExportDownload: (url, filename) => downloads.push({ url, filename }),
                    });
                });
                pendingPromises.push(p);
            },
            COPY_ROW_LIMIT: 10,
            EXPORT_ROW_LIMIT: 1000,
            rowCount: 4,
            tableName: 'sales',
            layoutMode: 'outline',
            serverSide: true,
            partModal: null,
            setExportProgress: (v) => { exportState.progress = v; },
            setIsExporting: (v) => { exportState.exporting = v; },
            setIsCopying: () => {},
            setPartModal: () => {},
        };

        try {
            const commands = createRuntimeExportCommands(opts);
            commands.exportPivot('xlsx');
            await Promise.all(pendingPromises);
            await new Promise((resolve) => setImmediate(resolve));

            assert.equal(exportState.exporting, false);
            assert.equal(exportState.progress.phase, 'completed');
            assert.equal(exportState.progress.delivery, 'delivered');
            assert.equal(downloads.length, 1);
            assert.match(downloads[0].filename, /\.xlsx$/);
            assert.equal(createdBlobs.length, 1);
            const downloadedBytes = new Uint8Array(await createdBlobs[0].arrayBuffer());
            assert.equal(downloadedBytes[0], 0x50);
            assert.equal(downloadedBytes[1], 0x4b);
            assert.ok(notices.some(([, level]) => level === 'success'));
        } finally {
            exportJobControllerRef.current?.dispose();
            URL.createObjectURL = originalCreateObjectURL;
            URL.revokeObjectURL = originalRevokeObjectURL;
        }
    });
}

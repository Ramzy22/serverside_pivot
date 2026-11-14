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

function createSourceLoader() {
    const cache = new Map();
    function load(filename) {
        const candidates = ['', '.ts', '.tsx', '.js', '.jsx', '/index.ts', '/index.js'];
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
    return {
        load: (relPath) => load(path.join(frontend, relPath)),
    };
}

const loader = createSourceLoader();

test('Part A: resolveThemeOverrides, depth tokens, grid borders, CSS vars, and offline fonts', () => {
    const {
        themes,
        resolveThemeOverrides,
        resolveDepthToken,
        resolveGridLineBorders,
        buildThemeCssVars,
        getStyles,
    } = loader.load('src/lib/utils/styles.js');

    const overrides = {
        flash: {
            fontFamily: 'Arial, Helvetica, sans-serif',
            fontSize: 12,
            rowHeight: 26,
            headerHeight: 30,
            gridLines: 'horizontal',
            rowBorder: '#eef1f3',
            primary: '#0f6e78',
            groupBg: ['#e9f1f2', '#f3f7f8'],
            groupFontWeight: [700, 600],
            negativeText: '#b3362c',
            headerControls: 'hover',
            showSortPriority: false,
            headerGroupBg: '#ffffff',
            headerGroupText: '#17212b',
        },
        bloomberg_black: {
            rowHeight: 26,
            gridLines: 'horizontal',
            groupBg: ['#161616', '#0b0b0b'],
            negativeText: '#ff5a4f',
            dataBarText: '#ffffff',
        },
    };

    const flashTheme = {
        ...themes.flash,
        ...resolveThemeOverrides('flash', overrides),
    };
    assert.equal(flashTheme.fontFamily, 'Arial, Helvetica, sans-serif');
    assert.equal(flashTheme.rowHeight, 26);
    assert.equal(flashTheme.headerHeight, 30);
    assert.equal(flashTheme.negativeText, '#b3362c');
    assert.equal(flashTheme.headerControls, 'hover');
    assert.equal(flashTheme.showSortPriority, false);

    // Flat override applies to any active theme
    const flatTheme = {
        ...themes.flash,
        ...resolveThemeOverrides('flash', { fontSize: 11, treeIndent: 16 }),
    };
    assert.equal(flatTheme.fontSize, 11);
    assert.equal(flatTheme.treeIndent, 16);

    const darkTheme = {
        ...themes.bloomberg_black,
        ...resolveThemeOverrides('bloomberg_black', overrides),
    };
    assert.equal(darkTheme.negativeText, '#ff5a4f');
    assert.equal(darkTheme.dataBarText, '#ffffff');
    assert.deepEqual(darkTheme.groupBg, ['#161616', '#0b0b0b']);

    // resolveDepthToken repeats the deepest value for deeper levels
    assert.equal(resolveDepthToken(flashTheme.groupBg, 0, '#fff'), '#e9f1f2');
    assert.equal(resolveDepthToken(flashTheme.groupBg, 1, '#fff'), '#f3f7f8');
    assert.equal(resolveDepthToken(flashTheme.groupBg, 3, '#fff'), '#f3f7f8');
    assert.equal(resolveDepthToken(flashTheme.groupFontWeight, 0, 400), 700);
    assert.equal(resolveDepthToken(flashTheme.groupFontWeight, 2, 400), 600);

    // resolveGridLineBorders
    const horiz = resolveGridLineBorders({ ...flashTheme, gridLines: 'horizontal' });
    assert.equal(horiz.rowBorderColor, '#eef1f3');
    assert.equal(horiz.columnBorderColor, 'transparent');
    const none = resolveGridLineBorders({ ...flashTheme, gridLines: 'none' });
    assert.equal(none.rowBorderColor, 'transparent');
    assert.equal(none.columnBorderColor, 'transparent');

    // buildThemeCssVars publishes --sp-* tokens
    const vars = buildThemeCssVars(flashTheme);
    assert.equal(vars['--sp-header-bg'], flashTheme.headerBg);
    assert.equal(vars['--sp-row-height'], '26px');
    assert.equal(vars['--sp-header-height'], '30px');
    assert.equal(vars['--sp-negative-text'], '#b3362c');
    assert.equal(vars['--sp-header-group-bg'], '#ffffff');
    assert.equal(vars['--sp-chevron-color'], '#0f6e78');

    // Verify styles object & offline font guard in PivotWorkspaceShell
    const styleObj = getStyles(flashTheme);
    assert.equal(styleObj.root.fontFamily, 'Arial, Helvetica, sans-serif');
    assert.equal(styleObj.root.fontSize, '12px');
    const shellSource = fs.readFileSync(path.join(frontend, 'src/lib/components/PivotWorkspaceShell.js'), 'utf8');
    assert.ok(shellSource.includes('loadWebFonts'));
    assert.ok(shellSource.includes('fonts.googleapis.com'));
});

test('B5 & B7: compact number format, valueScale, unitSuffix, and -0 normalization', () => {
    const {
        formatValue,
        normalizeNegativeZero,
        resolveNumericDisplayValue,
        isDisplayNegative,
    } = loader.load('src/lib/utils/helpers.js');

    // B7: -0 normalization after rounding
    assert.equal(Object.is(normalizeNegativeZero(-0), 0), true);
    assert.equal(formatValue(-0.3, 'number', 0), '0');
    assert.equal(isDisplayNegative(-0.3, 'number', 0), false);
    assert.equal(formatValue(-0.004, 'currency', 2), '$0.00');
    assert.equal(isDisplayNegative(-0.004, 'currency', 2), false);
    assert.equal(formatValue(-0.6, 'number', 0), '-1');
    assert.equal(isDisplayNegative(-0.6, 'number', 0), true);
    assert.equal(formatValue(-0.0001, 'compact', 0), '0');
    assert.equal(formatValue(-0.0001, 'compact', 1), '0.0');
    assert.equal(isDisplayNegative(-0.0001, 'compact', 1), false);

    // B5: compact notation
    assert.equal(formatValue(1234, 'compact', 1), '1.2K');
    assert.equal(formatValue(65500000, 'compact', 1), '65.5M');
    assert.equal(formatValue(-245000, 'compact', 0), '-245K');
    assert.equal(formatValue(1200000000, 'compact', 1), '1.2B');

    // B5: valueScale divisor & unitSuffix
    assert.equal(resolveNumericDisplayValue(1250000, null, null, { valueScale: 1000 }), 1250);
    assert.equal(
        formatValue(1250000, 'number', 1, 'comma', { valueScale: 1000, unitSuffix: 'k' }),
        '1,250.0k'
    );
    assert.equal(isDisplayNegative(-400, 'number', 0, { valueScale: 1000 }), false);
    assert.equal(formatValue(-400, 'number', 0, 'comma', { valueScale: 1000 }), '0');
});

test('B5: computeFitToWidthLayout shrinks widths and fontSize proportionally on phone containers', () => {
    const { computeFitToWidthLayout } = loader.load('src/lib/utils/pivotLayoutUtils.js');

    const cols = [
        { id: 'hierarchy', baseSize: 160 },
        { id: 'ytd', baseSize: 90 },
        { id: 'mtd', baseSize: 90 },
        { id: 'd5', baseSize: 80 },
        { id: 'd1', baseSize: 80 },
    ];
    const layout = computeFitToWidthLayout({
        columns: cols,
        containerWidth: 375,
        baseFontSize: 12,
        minFontSize: 10,
    });
    assert.ok(layout.fontSize <= 12 && layout.fontSize >= 10);
    const totalFittedWidth = Object.values(layout.columnSizing).reduce((acc, w) => acc + w, 0);
    assert.ok(totalFittedWidth <= 375, `Expected fitted width <= 375, got ${totalFittedWidth}`);
});

test('B4: selection formatting supports labels for all measures and formula without id', () => {
    const React = localRequire('react');
    const ReactDOMServer = localRequire('react-dom/server');
    const { useRuntimeSelectionFormatting } = loader.load('src/lib/runtime/useRuntimeSelectionFormatting.js');

    const valConfigs = [
        { field: 'ytd_pnl', agg: 'sum', label: 'YtD PnL' },
        { field: 'other_pnl', agg: 'formula', label: 'Other PnL Change', formula: 'a + b' },
    ];

    let captured = null;
    function Probe() {
        captured = useRuntimeSelectionFormatting({
            colFields: [],
            decimalPlaces: 2,
            defaultValueFormat: 'number',
            displayColOrder: [],
            localeText: {},
            measureAxis: null,
            mergedHeaderSeparator: ' / ',
            nodes: [],
            numberGroupSeparator: 'comma',
            rowFields: ['level_1'],
            serverColumnLabelLookup: {},
            showNotification: () => {},
            table: null,
            valConfigs,
        });
        return null;
    }
    ReactDOMServer.renderToStaticMarkup(React.createElement(Probe));

    assert.equal(captured.matchesSelectedColumnToValueConfig('ytd_pnl_sum', valConfigs[0]), true);
    assert.equal(captured.matchesSelectedColumnToValueConfig('other_pnl_formula', valConfigs[1]), true);
    assert.equal(captured.getDisplayHeaderTextForColumn({ id: 'ytd_pnl_sum', columnDef: {} }), 'YtD PnL');
    assert.equal(captured.getDisplayHeaderTextForColumn({ id: 'other_pnl_formula', columnDef: {} }), 'Other PnL Change');
});

test('B3: exportUtils buildExportAoa creates multi-row merged headers cleanly for grouped measures', () => {
    const { buildExportAoa } = loader.load('src/lib/utils/exportUtils.js');

    const mockTable = {
        getHeaderGroups: () => [
            {
                headers: [
                    { column: { id: 'hierarchy', columnDef: { header: 'Hierarchy / Hedge' } }, colSpan: 1, isPlaceholder: true },
                    { column: { id: 'ytd_pnl_sum', columnDef: { header: 'YtD PnL' } }, colSpan: 1, isPlaceholder: true },
                    { column: { id: '__group_PnL Attribution', columnDef: { header: 'PnL Attribution Effects' } }, colSpan: 2, isPlaceholder: false },
                    { column: { id: 'curr', columnDef: { header: 'Curr.' } }, colSpan: 1, isPlaceholder: true },
                ],
            },
            {
                headers: [
                    { column: { id: 'hierarchy', columnDef: { header: 'Hierarchy / Hedge' } }, colSpan: 1, isPlaceholder: false },
                    { column: { id: 'ytd_pnl_sum', columnDef: { header: 'YtD PnL' } }, colSpan: 1, isPlaceholder: false },
                    { column: { id: 'total_price_sum', columnDef: { header: 'Total Price' } }, colSpan: 1, isPlaceholder: false },
                    { column: { id: 'quantity_sum', columnDef: { header: 'Quantity' } }, colSpan: 1, isPlaceholder: false },
                    { column: { id: 'curr', columnDef: { header: 'Curr.' } }, colSpan: 1, isPlaceholder: false },
                ],
            },
        ],
    };
    const rows = [
        {
            original: { hierarchy: 'Root', ytd_pnl_sum: 100, total_price_sum: 60, quantity_sum: 40, curr: 'USD' },
            getValue: (colId) => rows[0].original[colId],
        },
    ];
    const { aoa, merges, headerRowCount } = buildExportAoa(rows, mockTable);
    assert.equal(headerRowCount, 2);
    assert.deepEqual(aoa[0], ['', '', 'PnL Attribution Effects', '', '']);
    assert.deepEqual(aoa[1], ['Hierarchy / Hedge', 'YtD PnL', 'Total Price', 'Quantity', 'Curr.']);
    assert.deepEqual(merges, [{ s: { r: 0, c: 2 }, e: { r: 0, c: 3 } }]);
});

test('B1: ClientPivotTransport handles groupAllowUnbalanced and unbalancedValues in pivot mode', async () => {
    const { ClientPivotTransport } = loader.load('src/lib/runtime/client/index.ts');

    const raggedRows = [
        { level_1: 'Root', level_2: 'Desk', level_3: null, hedge: 'Hedge A', ytd_pnl: 100 },
        { level_1: 'Root', level_2: null, level_3: null, hedge: 'Hedge B', ytd_pnl: -250 },
        { level_1: 'Root', level_2: 'Desk', level_3: 'SubDesk', hedge: 'Hedge C', ytd_pnl: 50 },
        { level_1: 'DirectRoot', level_2: null, level_3: '', hedge: null, ytd_pnl: 30 },
        { level_1: '', level_2: 'OrphanDesk', level_3: null, hedge: 'Hedge D', ytd_pnl: 15 },
    ];

    const transport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: raggedRows,
        tableName: 'pnl',
        duckdbBundleUrl: './offline/duckdb.js',
    });

    const resAll = await transport.request({
        kind: 'data',
        requestId: 'b1-all',
        table: 'pnl',
        payload: {
            startRow: 0,
            endRow: 50,
            state_override: {
                rowFields: ['level_1', 'level_2', 'level_3', 'hedge'],
                colFields: [],
                valConfigs: [{ field: 'ytd_pnl', agg: 'sum', label: 'YtD PnL' }],
                expanded: { __ALL__: true },
                showColTotals: true,
                groupAllowUnbalanced: true,
                sorting: [{ id: 'ytd_pnl_sum', desc: true, absoluteSort: true }],
            },
        },
    });

    const dataRows = resAll.payload.data.filter((r) => r._path !== '__color_scale_stats__');
    const labels = dataRows.map((r) => r._id);
    // No null or empty group rows created
    assert.equal(labels.includes(null), false);
    assert.equal(labels.includes(''), false);

    // Check Hedge B attaches directly under Root (depth 1), Hedge A under Desk (depth 2), Hedge C under SubDesk (depth 3)
    const byId = Object.fromEntries(dataRows.map((r) => [r._id, r]));
    assert.equal(byId.Root.depth, 0);
    assert.equal(byId.Root.ytd_pnl_sum, -100);
    assert.equal(byId['Hedge B'].depth, 1);
    assert.equal(byId['Hedge B']._path, 'Root|||Hedge B');
    assert.equal(byId['Hedge B']._has_children, false);
    assert.equal(byId.Desk.depth, 1);
    assert.equal(byId['Hedge A'].depth, 2);
    assert.equal(byId['Hedge A']._path, 'Root|||Desk|||Hedge A');
    assert.equal(byId.SubDesk.depth, 2);
    assert.equal(byId['Hedge C'].depth, 3);
    assert.equal(byId.DirectRoot.depth, 0);
    assert.equal(byId.DirectRoot._has_children, false);
    assert.equal(byId.OrphanDesk.depth, 0);
    assert.equal(byId['Hedge D'].depth, 1);
    assert.equal(byId['Grand Total'].ytd_pnl_sum, -55);

    // Sibling sorting under Root with absoluteSort desc: |Hedge B|=250 before |Desk|=150
    const rootChildren = dataRows.filter((r) => r.depth === 1 && r._path.startsWith('Root|||'));
    assert.deepEqual(rootChildren.map((r) => r._id), ['Hedge B', 'Desk']);

    // Custom unbalancedValues
    const customRows = [
        { level_1: 'Root', level_2: '-', hedge: 'Hedge X', ytd_pnl: 10 },
        { level_1: 'Root', level_2: 'N/A', hedge: 'Hedge Y', ytd_pnl: 20 },
    ];
    const customTransport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: customRows,
        tableName: 'pnl',
    });
    const resCustom = await customTransport.request({
        kind: 'data',
        requestId: 'b1-custom',
        table: 'pnl',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                rowFields: ['level_1', 'level_2', 'hedge'],
                colFields: [],
                valConfigs: [{ field: 'ytd_pnl', agg: 'sum' }],
                expanded: { __ALL__: true },
                showColTotals: false,
                groupAllowUnbalanced: true,
                unbalancedValues: ['-', 'N/A'],
            },
        },
    });
    const customIds = resCustom.payload.data
        .filter((r) => r._path !== '__color_scale_stats__')
        .map((r) => r._id);
    assert.deepEqual(customIds.sort(), ['Hedge X', 'Hedge Y', 'Root']);
});

test('B2: ClientPivotTransport tree mode supports sorting, absoluteSort, and showGrandTotal', async () => {
    const { ClientPivotTransport } = loader.load('src/lib/runtime/client/index.ts');

    const treeRows = [
        { path: 'Root', ytd_pnl: -100 },
        { path: 'Root/Desk A', ytd_pnl: 40 },
        { path: 'Root/Desk B', ytd_pnl: -140 },
        { path: 'Alpha', ytd_pnl: 50 },
    ];

    const transport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: treeRows,
        tableName: 'tree_pnl',
    });

    const res = await transport.request({
        kind: 'data',
        requestId: 'b2-tree',
        table: 'tree_pnl',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                viewMode: 'tree',
                treeConfig: {
                    sourceType: 'path',
                    pathField: 'path',
                    pathSeparator: '/',
                    valueFields: ['ytd_pnl'],
                },
                expanded: { Root: true },
                sorting: [{ id: 'ytd_pnl', desc: true, absoluteSort: true }],
                showGrandTotal: true,
            },
        },
    });

    const rows = res.payload.data.filter((r) => r._path !== '__color_scale_stats__');
    // Root |-100|=100 before Alpha |50|=50; under Root: Desk B |-140|=140 before Desk A |40|=40; Grand Total sums roots (-100 + 50 = -50)
    assert.deepEqual(rows.map((r) => r._id), ['Root', 'Desk B', 'Desk A', 'Alpha', 'Grand Total']);
    const gt = rows.find((r) => r._path === '__grand_total__');
    assert.ok(gt);
    assert.equal(gt._isTotal, true);
    assert.equal(gt.ytd_pnl, -50);
});

test('Regression: B3 TanStack v8 placeholder spanning resolves full multi-row height', () => {
    const {
        createTable,
        getCoreRowModel,
    } = localRequire('@tanstack/react-table');
    const { applyMeasureHeaderGroups } = loader.load('src/lib/hooks/useColumnDefs.js');
    const { resolveHeaderRowSpan } = loader.load('src/lib/hooks/useRenderHelpers.js');
    const { resolveSectionHeaderSpanMeta } = loader.load('src/lib/components/Table/PivotTableBody.js');

    const baseCols = [
        { id: 'hierarchy', header: 'Hierarchy / Hedge', accessorKey: 'hierarchy' },
        { id: 'ytd_pnl_sum', header: 'YtD PnL', accessorKey: 'ytd_pnl_sum', meta: { valConfig: { field: 'ytd_pnl', agg: 'sum', label: 'YtD PnL' } } },
        { id: 'price_sum', header: 'Total Price', accessorKey: 'price_sum', meta: { headerGroup: 'PnL Attribution Effects', valConfig: { field: 'price', agg: 'sum', headerGroup: 'PnL Attribution Effects' } } },
        { id: 'qty_sum', header: 'Quantity', accessorKey: 'qty_sum', meta: { headerGroup: 'PnL Attribution Effects', valConfig: { field: 'qty', agg: 'sum', headerGroup: 'PnL Attribution Effects' } } },
    ];
    const groupedCols = applyMeasureHeaderGroups(baseCols);

    const table = createTable({
        data: [{ hierarchy: 'Root', ytd_pnl_sum: 10, price_sum: 5, qty_sum: 2 }],
        columns: groupedCols,
        state: {
            columnPinning: { left: [], right: [] },
        },
        onStateChange: () => {},
        renderFallbackValue: null,
        getCoreRowModel: getCoreRowModel(),
    });
    const groups = table.getHeaderGroups();
    assert.equal(groups.length, 2);

    const hTop = groups[0].headers.find((h) => h.column.id === 'hierarchy');
    const ytdTop = groups[0].headers.find((h) => h.column.id === 'ytd_pnl_sum');
    assert.equal(hTop.rowSpan, 0); // TanStack v8 sets placeholder rowSpan to 0
    assert.equal(resolveHeaderRowSpan(hTop), 2);
    assert.equal(resolveHeaderRowSpan(ytdTop), 2);

    const meta = resolveSectionHeaderSpanMeta(groups, [34, 28], 30);
    assert.equal(meta.coveredByLevel[0].has('hierarchy'), false);
    assert.equal(meta.coveredByLevel[1].has('hierarchy'), true);
    assert.equal(meta.coveredByLevel[1].has('ytd_pnl_sum'), true);
    assert.equal(meta.coveredByLevel[1].has('price_sum'), false);
    assert.equal(meta.spanHeightsByHeaderId.get(hTop.id), 62);
    assert.equal(meta.spanHeightsByHeaderId.get(ytdTop.id), 62);
});

test('Regression: B5 fitToWidth shrinks fontSize below 640px and B6 file:// skips CDN hang', async () => {
    const { computeFitToWidthLayout } = loader.load('src/lib/utils/pivotLayoutUtils.js');
    const fitted = computeFitToWidthLayout({
        containerWidth: 500,
        baseFontSize: 13,
        minFontSize: 9,
        columns: [
            { id: 'hierarchy', baseSize: 220 },
            { id: 'pnl_sum', baseSize: 160 },
            { id: 'mtd_sum', baseSize: 160 },
            { id: 'qty_sum', baseSize: 160 },
            { id: 'fx_sum', baseSize: 160 },
        ],
    });
    assert.ok(fitted.fontSize < 13, `Expected fontSize < 13 at 500px container, got ${fitted.fontSize}`);
    assert.ok(fitted.fontSize >= 9);

    const { DuckDbWasmExecutor } = loader.load('src/lib/runtime/client/duckdbWasmExecutor.ts');
    const prevWindow = global.window;
    try {
        global.window = { location: { protocol: 'file:' } };
        const exec = new DuckDbWasmExecutor({ executionMode: 'client', clientEngine: 'duckdb-wasm' });
        const startMs = Date.now();
        await exec.setData([{ a: 'R', v: 10 }]);
        const res = await exec.execute({
            kind: 'data',
            table: 't',
            viewMode: 'pivot',
            layoutMode: 'hierarchy',
            showSubtotalFooters: false,
            includeGrandTotal: true,
            showRowTotals: true,
            showColTotals: true,
            grandTotalPosition: 'bottom',
            rowFields: ['a'],
            colFields: [],
            valConfigs: [{ field: 'v', agg: 'sum', id: 'v_sum', label: 'V' }],
            customDimensions: [],
            filters: [],
            sorting: [],
            sortOptions: { naturalSort: true, fieldOptions: {} },
            expandedAll: true,
            expandedPaths: [],
            collapsedPaths: [],
            measureAxis: { placement: 'columns', measureColId: '__m__', valueColId: '__v__', label: 'Measure', valueLabel: 'Value' },
            treeConfig: null,
            reportDef: null,
            detailConfig: null,
            layoutManifest: null,
            start: 0,
            end: 50,
            colStart: null,
            colEnd: null,
            needsColSchema: true,
            version: 1,
            windowSeq: 1,
            stateEpoch: 1,
            abortGeneration: 0,
            sessionId: 's',
            clientInstance: 'c',
            intent: 'structural',
            sql: { baseFilteredSql: '', groupingSql: '', colValuesSql: '', grandTotalSql: '', filterOptionsSql: '', drillSql: '' },
        });
        const elapsedMs = Date.now() - startMs;
        assert.equal(res.engineUsed, 'memory');
        assert.ok(elapsedMs < 1000, `Expected offline file:// fallback < 1000ms, got ${elapsedMs}ms`);
    } finally {
        if (prevWindow === undefined) {
            delete global.window;
        } else {
            global.window = prevWindow;
        }
    }
});

test('Regression: B1 same-label across unbalanced fields & B2 treeConfig.sortBy/sortDir/absoluteSort', async () => {
    const { ClientPivotTransport } = loader.load('src/lib/runtime/client/index.ts');

    const unbalancedTransport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: [
            { l1: 'USD', l2: null, hedge: 'H1', pnl: 10 },
            { l1: null, l2: 'USD', hedge: 'H2', pnl: 25 },
        ],
        tableName: 'unbal',
    });
    const unbalRes = await unbalancedTransport.request({
        kind: 'data',
        requestId: 'b1-collide',
        table: 'unbal',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                rowFields: ['l1', 'l2', 'hedge'],
                valConfigs: [{ field: 'pnl', agg: 'sum' }],
                groupAllowUnbalanced: true,
                expanded: true,
            },
        },
    });
    const unbalRows = unbalRes.payload.data.filter((r) => r._id === 'USD');
    assert.equal(unbalRows.length, 2);
    assert.deepEqual(unbalRows.map((r) => r.pnl_sum).sort((a, b) => a - b), [10, 25]);

    const treeTransport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: [
            { path: 'Root', pnl: -100 },
            { path: 'Root/Desk A', pnl: 40 },
            { path: 'Root/Desk B', pnl: -140 },
            { path: 'Alpha', pnl: 50 },
        ],
        tableName: 'tree_sort_cfg',
    });
    const treeRes = await treeTransport.request({
        kind: 'data',
        requestId: 'b2-tree-cfg-sort',
        table: 'tree_sort_cfg',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                viewMode: 'tree',
                valConfigs: [{ field: 'pnl', agg: 'sum', label: 'PnL' }],
                treeConfig: {
                    sourceType: 'path',
                    pathField: 'path',
                    pathSeparator: '/',
                    valueFields: ['pnl'],
                    sortBy: 'pnl',
                    sortDir: 'desc',
                    absoluteSort: true,
                    showGrandTotal: true,
                },
                expanded: { Root: true },
            },
        },
    });
    const treeVisible = treeRes.payload.data.filter((r) => r._path !== '__color_scale_stats__');
    assert.deepEqual(treeVisible.map((r) => r._id), ['Root', 'Desk B', 'Desk A', 'Alpha', 'Grand Total']);
});

const harnessCodeCache = new Map();

function createRuntimeHarness() {
    const prevWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
    const prevDocument = Object.getOwnPropertyDescriptor(globalThis, 'document');
    const prevRaf = Object.getOwnPropertyDescriptor(globalThis, 'requestAnimationFrame');
    const prevCaf = Object.getOwnPropertyDescriptor(globalThis, 'cancelAnimationFrame');
    const docHead = { appendChild() {}, querySelectorAll: () => [] };
    const docShim = {
        head: docHead,
        createElement: () => ({
            style: {},
            setAttribute() {},
            getContext: () => ({ font: '', measureText: (t) => ({ width: String(t || '').length * 8 }) }),
        }),
        querySelector: () => null,
        querySelectorAll: () => [],
        addEventListener() {},
        removeEventListener() {},
    };
    const winShim = {
        location: { protocol: 'https:', href: 'https://example.test/' },
        performance: globalThis.performance,
        console: globalThis.console,
        setTimeout: globalThis.setTimeout,
        clearTimeout: globalThis.clearTimeout,
        MessageChannel: globalThis.MessageChannel,
        addEventListener() {},
        removeEventListener() {},
        innerWidth: 1280,
        innerHeight: 800,
        devicePixelRatio: 1,
        requestAnimationFrame: (cb) => setTimeout(cb, 0),
        cancelAnimationFrame: (id) => clearTimeout(id),
        localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    };
    globalThis.window = winShim;
    globalThis.document = docShim;
    globalThis.requestAnimationFrame = winShim.requestAnimationFrame;
    globalThis.cancelAnimationFrame = winShim.cancelAnimationFrame;

    const React = localRequire('react');
    const createScheduler = () => {
        const slots = [];
        let cursor = 0;
        let pending = [];
        const same = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
        let idCounter = 0;
        const api = {
            ...React,
            useRef(value) {
                const index = cursor++;
                return (slots[index] ||= { current: value });
            },
            useState(value) {
                const index = cursor++;
                slots[index] ||= { value: typeof value === 'function' ? value() : value };
                return [
                    slots[index].value,
                    (next) => {
                        slots[index].value = typeof next === 'function' ? next(slots[index].value) : next;
                    },
                ];
            },
            useReducer(reducer, initialArg, init) {
                const [state, setState] = api.useState(() => (typeof init === 'function' ? init(initialArg) : initialArg));
                const dispatch = api.useCallback((action) => setState((prev) => reducer(prev, action)), [reducer]);
                return [state, dispatch];
            },
            useMemo(fn, deps) {
                const index = cursor++;
                if (!slots[index] || !same(slots[index].deps, deps)) {
                    slots[index] = { value: fn(), deps };
                }
                return slots[index].value;
            },
            useCallback(fn, deps) {
                return api.useMemo(() => fn, deps);
            },
            useContext(ctx) {
                return ctx ? ctx._currentValue : undefined;
            },
            useId() {
                const index = cursor++;
                return (slots[index] ||= `:r${idCounter++}:`);
            },
            useDebugValue() {},
            useDeferredValue(value) { return value; },
            useTransition() { return [false, (fn) => fn()]; },
            useSyncExternalStore(subscribe, getSnapshot) { return getSnapshot(); },
            useEffect(fn, deps) {
                const index = cursor++;
                if (!slots[index] || !same(slots[index].deps, deps)) {
                    pending.push(() => {
                        if (slots[index] && typeof slots[index].cleanup === 'function') {
                            slots[index].cleanup();
                        }
                        slots[index] = { deps, cleanup: fn() };
                    });
                }
            },
        };
        api.useLayoutEffect = api.useEffect;
        api.useInsertionEffect = api.useEffect;
        return {
            api,
            render(fn) {
                cursor = 0;
                pending = [];
                const internals = React.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED;
                const prevDispatcher = internals && internals.ReactCurrentDispatcher ? internals.ReactCurrentDispatcher.current : null;
                if (internals && internals.ReactCurrentDispatcher) {
                    internals.ReactCurrentDispatcher.current = api;
                }
                let result;
                try {
                    result = fn();
                } finally {
                    if (internals && internals.ReactCurrentDispatcher) {
                        internals.ReactCurrentDispatcher.current = prevDispatcher;
                    }
                }
                return {
                    result,
                    flushEffects() {
                        const runs = pending;
                        pending = [];
                        runs.forEach((run) => run());
                    },
                };
            },
            cleanup() {
                slots.forEach((slot) => {
                    if (slot && typeof slot.cleanup === 'function') slot.cleanup();
                });
            },
        };
    };

    const wsScheduler = createScheduler();
    const rootScheduler = createScheduler();

    const makeLoader = (reactApi) => {
        const cache = new Map();
        function load(filename) {
            const candidates = ['', '.ts', '.tsx', '.js', '.jsx', '/index.ts', '/index.js'];
            const resolved = candidates
                .map((ext) => filename + ext)
                .find((file) => fs.existsSync(file) && fs.statSync(file).isFile());
            if (!resolved) {
                throw new Error(`Cannot resolve module: ${filename}`);
            }
            if (cache.has(resolved)) return cache.get(resolved).exports;
            const mod = { exports: {} };
            cache.set(resolved, mod);
            let code = harnessCodeCache.get(resolved);
            if (!code) {
                code = babel.transformSync(fs.readFileSync(resolved, 'utf8'), {
                    filename: resolved,
                    presets,
                }).code;
                harnessCodeCache.set(resolved, code);
            }
            const requireSource = (specifier) => {
                if (specifier === 'react') return reactApi;
                if (specifier === 'react-dom') {
                    return {
                        createPortal: (node) => node,
                        flushSync: (fn) => (typeof fn === 'function' ? fn() : undefined),
                    };
                }
                if (specifier === '@tanstack/react-virtual') {
                    return {
                        useVirtualizer: ({ count = 0, estimateSize = () => 36 }) => ({
                            getVirtualItems: () => Array.from({ length: Math.min(count, 20) }, (_, index) => ({
                                index,
                                start: index * estimateSize(index),
                                size: estimateSize(index),
                                key: index,
                            })),
                            getTotalSize: () => count * estimateSize(0),
                            measure: () => {},
                            scrollToIndex: () => {},
                            scrollToOffset: () => {},
                        }),
                    };
                }
                if (specifier.endsWith('.css')) return {};
                if (specifier.startsWith('.')) {
                    return load(path.resolve(path.dirname(resolved), specifier));
                }
                return localRequire(specifier);
            };
            new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
            return mod.exports;
        }
        return (relPath) => load(path.join(frontend, relPath));
    };

    const wsLoad = makeLoader(wsScheduler.api);
    const rootLoad = makeLoader(rootScheduler.api);
    const { PivotWorkspace } = wsLoad('src/lib/components/PivotWorkspace.tsx');
    const { ClientPivotTransport } = wsLoad('src/lib/runtime/client/index.ts');
    const { useCoreRuntime } = rootLoad('src/lib/runtime/useCoreRuntime.ts');
    const { useTransportChartRequestDispatch, useTransportResponseRuntime, useTransportRuntime } = rootLoad('src/lib/runtime/useTransportRuntime.ts');
    const { useCommandRuntime } = rootLoad('src/lib/runtime/useCommandRuntime.ts');
    const { useViewportRuntime } = rootLoad('src/lib/runtime/useViewportRuntime.ts');
    const { useWorkspaceRuntime } = rootLoad('src/lib/runtime/useWorkspaceRuntime.ts');
    const { createWorkspaceViewModel } = rootLoad('src/lib/runtime/workspaceViewModel.ts');
    const { PivotWorkspaceShell } = rootLoad('src/lib/components/PivotWorkspaceShell.js');

    let lastCore = null;
    let lastViewport = null;
    let lastWorkspace = null;

    function renderCycle(wsProps) {
        const wsPass = wsScheduler.render(() => PivotWorkspace(wsProps));
        const rootElementProps = wsPass.result.props;
        const { payloadTransport, ...runtimeProps } = rootElementProps;
        const rootPass = rootScheduler.render(() => {
            const core = useCoreRuntime(runtimeProps, payloadTransport);
            const transport = useTransportRuntime(core);
            const commands = useCommandRuntime(core, transport);
            const viewport = useViewportRuntime(commands, transport);
            const workspaceDomain = useWorkspaceRuntime(viewport);
            useTransportResponseRuntime(core, transport, viewport, payloadTransport);
            useTransportChartRequestDispatch(workspaceDomain, transport);
            const domains = Object.freeze({ core, transport, commands, viewport, workspace: workspaceDomain });
            lastCore = core;
            lastViewport = viewport;
            lastWorkspace = createWorkspaceViewModel(domains);
            return PivotWorkspaceShell({ workspace: lastWorkspace });
        });
        wsPass.flushEffects();
        rootPass.flushEffects();
        return {
            shellElement: rootPass.result,
            payloadTransport,
            core: lastCore,
            viewport: lastViewport,
            workspace: lastWorkspace,
        };
    }

    return {
        ClientPivotTransport,
        async settle(wsProps, passes = 5) {
            let result;
            for (let i = 0; i < passes; i += 1) {
                result = renderCycle(wsProps);
                await new Promise((resolve) => setImmediate(resolve));
            }
            return result;
        },
        cleanup() {
            wsScheduler.cleanup();
            rootScheduler.cleanup();
            if (prevWindow) Object.defineProperty(globalThis, 'window', prevWindow);
            else delete globalThis.window;
            if (prevDocument) Object.defineProperty(globalThis, 'document', prevDocument);
            else delete globalThis.document;
            if (prevRaf) Object.defineProperty(globalThis, 'requestAnimationFrame', prevRaf);
            else delete globalThis.requestAnimationFrame;
            if (prevCaf) Object.defineProperty(globalThis, 'cancelAnimationFrame', prevCaf);
            else delete globalThis.cancelAnimationFrame;
        },
    };
}

test('Audit fixes: external ClientPivotTransport without data prop renders aggregated rows and publishes CSS height vars', async () => {
    const harness = createRuntimeHarness();
    try {
        const rows = [
            { region: 'North', category: 'Widget', sales: 120 },
            { region: 'North', category: 'Gadget', sales: 80 },
            { region: 'South', category: 'Widget', sales: 50 },
        ];
        const externalTransport = new harness.ClientPivotTransport({
            executionMode: 'client',
            clientEngine: 'memory',
            data: rows,
            tableName: 'sales',
        });
        const setDataCalls = [];
        const origSetData = externalTransport.setData.bind(externalTransport);
        externalTransport.setData = (nextRows) => {
            setDataCalls.push(nextRows);
            return origSetData(nextRows);
        };

        const rendered = await harness.settle({
            transport: externalTransport,
            rowFields: ['region'],
            valConfigs: [{ field: 'sales', agg: 'sum', label: 'Sales' }],
            showColTotals: true,
        });

        assert.equal(setDataCalls.length, 0, 'external transport data must not be overwritten when data prop is omitted');
        assert.equal(externalTransport.getData().length, 3);
        assert.equal(rendered.core.clientTransportRef.current, externalTransport);
        assert.equal(rendered.payloadTransport, externalTransport);

        const rowIds = rendered.workspace.presentation.effectiveCenterRows
            .map((r) => r && r.original && r.original._id)
            .filter(Boolean);
        assert.deepEqual(rowIds.sort(), ['North', 'South']);

        const northRow = rendered.workspace.presentation.effectiveCenterRows.find((r) => r.original._id === 'North');
        assert.equal(northRow.original.sales_sum, 200);
        const gtRow = rendered.workspace.presentation.grandTotalRow
            || rendered.workspace.presentation.effectiveTopRows.find((r) => r.original && r.original._isTotal);
        assert.ok(gtRow, 'Grand Total row should be present');
        assert.equal(gtRow.original.sales_sum, 250);

        // --sp-row-height and --sp-header-height are always published on [data-pivot-workspace-root]
        const rootStyle = rendered.shellElement.props.style;
        assert.equal(rootStyle['--sp-row-height'], `${rendered.workspace.presentation.rowHeight}px`);
        assert.equal(rootStyle['--sp-header-height'], `${rendered.workspace.presentation.headerHeight}px`);
        assert.equal(rootStyle['--sp-row-height'], '44px');
        assert.equal(rootStyle['--sp-header-height'], '44px');
        assert.equal(rendered.shellElement.props['data-pivot-workspace-root'], 'true');

        // Reusing internal transport created by PivotWorkspace when transport prop is omitted (fresh mount)
        const internalHarness = createRuntimeHarness();
        try {
            const internalRendered = await internalHarness.settle({
                data: rows,
                rowFields: ['region'],
                valConfigs: [{ field: 'sales', agg: 'sum', label: 'Sales' }],
            });
            assert.equal(internalRendered.core.clientTransportRef.current, internalRendered.payloadTransport);
        } finally {
            internalHarness.cleanup();
        }

        // Part B props are included in serverSideCacheKey, serverSideViewportResetKey, and structural sync triggers
        const capturedRequests = [];
        const origRequest = externalTransport.request.bind(externalTransport);
        externalTransport.request = async (req) => {
            capturedRequests.push(req);
            return origRequest(req);
        };

        const toggledGrandTotal = await harness.settle({
            transport: externalTransport,
            rowFields: ['region'],
            valConfigs: [{ field: 'sales', agg: 'sum', label: 'Sales' }],
            showColTotals: true,
            showGrandTotal: false,
        }, 4);
        assert.ok(
            capturedRequests.some((r) => r && r.state && r.state.showGrandTotal === false),
            'toggling showGrandTotal alone must trigger a structural request'
        );

        capturedRequests.length = 0;
        const toggledUnbalanced = await harness.settle({
            transport: externalTransport,
            rowFields: ['region'],
            valConfigs: [{ field: 'sales', agg: 'sum', label: 'Sales' }],
            showColTotals: true,
            showGrandTotal: false,
            groupAllowUnbalanced: true,
            unbalancedValues: ['N/A'],
        }, 4);
        assert.ok(
            capturedRequests.some((r) => {
                const st = r && r.state;
                return st && st.groupAllowUnbalanced === true && Array.isArray(st.unbalancedValues) && st.unbalancedValues[0] === 'N/A';
            }),
            'toggling groupAllowUnbalanced and unbalancedValues must trigger a structural request'
        );

        const baseCacheKey = rendered.viewport.serverSideCacheKey;
        const baseResetKey = rendered.viewport.serverSideViewportResetKey;
        const updatedPartB = await harness.settle({
            transport: externalTransport,
            rowFields: ['region'],
            valConfigs: [{ field: 'sales', agg: 'sum', label: 'Sales' }],
            groupAllowUnbalanced: true,
            unbalancedValues: ['N/A'],
            showGrandTotal: false,
            viewMode: 'tree',
            treeConfig: { sourceType: 'path', pathField: 'region' },
        }, 5);
        assert.notEqual(updatedPartB.viewport.serverSideCacheKey, baseCacheKey);
        assert.notEqual(updatedPartB.viewport.serverSideViewportResetKey, baseResetKey);
        assert.ok(toggledUnbalanced.viewport.serverSideCacheKey !== baseCacheKey);
        assert.equal(updatedPartB.workspace.presentation.grandTotalRow, undefined);
        assert.equal(
            updatedPartB.workspace.presentation.effectiveTopRows.some((r) => r && r.original && r.original._isTotal),
            false
        );
        const parsedKey = JSON.parse(updatedPartB.viewport.serverSideCacheKey);
        assert.equal(parsedKey.groupAllowUnbalanced, true);
        assert.deepEqual(parsedKey.unbalancedValues, ['N/A']);
        assert.equal(parsedKey.showGrandTotal, false);
        assert.equal(parsedKey.viewMode, 'tree');
        assert.equal(parsedKey.treeConfig.sourceType, 'path');
        assert.equal(parsedKey.treeConfig.pathField, 'region');
        const parsedResetKey = JSON.parse(updatedPartB.viewport.serverSideViewportResetKey);
        assert.equal(parsedResetKey.groupAllowUnbalanced, true);
        assert.deepEqual(parsedResetKey.unbalancedValues, ['N/A']);
        assert.equal(parsedResetKey.showGrandTotal, false);
        assert.equal(parsedResetKey.viewMode, 'tree');
        assert.equal(parsedResetKey.treeConfig.pathField, 'region');
    } finally {
        harness.cleanup();
    }
});

test('Audit fix: formula measure with custom id and colFields.length > 0 aligns row keys, schema, row totals, and measureAxis rows', async () => {
    const { ClientPivotTransport } = loader.load('src/lib/runtime/client/index.ts');
    const React = localRequire('react');
    const ReactDOMServer = localRequire('react-dom/server');
    const { useColumnDefs } = loader.load('src/lib/hooks/useColumnDefs.js');

    const rows = [
        { category: 'Alpha', region: 'East', sales: 100, cost: 60 },
        { category: 'Alpha', region: 'West', sales: 80, cost: 30 },
        { category: 'Beta', region: 'East', sales: 50, cost: 20 },
    ];
    const transport = new ClientPivotTransport({
        executionMode: 'client',
        clientEngine: 'memory',
        data: rows,
        tableName: 'margin_tbl',
    });

    const valConfigs = [
        { field: 'sales', agg: 'sum', label: 'Sales' },
        { field: 'cost', agg: 'sum', label: 'Cost' },
        { id: 'net_margin', field: 'margin', agg: 'formula', formula: 'sales - cost', label: 'Net Margin', format: 'currency' },
        { id: 'east_minus_west', field: 'ew_diff', agg: 'formula', formulaScope: 'columns', formula: 'East_sales_sum - West_sales_sum', label: 'East - West' },
    ];

    const res = await transport.request({
        kind: 'data',
        requestId: 'formula-custom-id',
        table: 'margin_tbl',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                rowFields: ['category'],
                colFields: ['region'],
                valConfigs,
                showRowTotals: true,
                showColTotals: true,
            },
        },
    });

    const colIds = res.payload.columns.map((c) => c.id);
    assert.ok(colIds.includes('East_net_margin'));
    assert.ok(colIds.includes('West_net_margin'));
    assert.ok(colIds.includes('east_minus_west'));
    assert.ok(colIds.includes('__RowTotal__net_margin'));
    assert.equal(colIds.includes('ew_diff_formula'), false);
    assert.equal(colIds.includes('__RowTotal__margin_formula'), false);

    const alphaRow = res.payload.data.find((r) => r._id === 'Alpha');
    assert.equal(alphaRow.East_net_margin, 40);
    assert.equal(alphaRow.West_net_margin, 50);
    assert.equal(alphaRow.east_minus_west, 20);
    assert.equal(alphaRow.__RowTotal__net_margin, 90);

    // Measure axis on rows must also resolve custom id suffix
    const measureRowsRes = await transport.request({
        kind: 'data',
        requestId: 'formula-custom-id-rows-axis',
        table: 'margin_tbl',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                rowFields: ['category'],
                colFields: ['region'],
                valConfigs: valConfigs.slice(0, 3),
                measureAxis: { placement: 'rows', measureColId: '__measure_label__' },
                showRowTotals: true,
            },
        },
    });
    const alphaMarginMeasureRow = measureRowsRes.payload.data.find(
        (r) => r._id === 'Alpha' && r.__measure_label__ === 'Net Margin'
    );
    assert.ok(alphaMarginMeasureRow);
    assert.equal(alphaMarginMeasureRow.East, 40);
    assert.equal(alphaMarginMeasureRow.West, 50);
    assert.equal(alphaMarginMeasureRow.__RowTotal__, 90);

    // useColumnDefs builds proper column headers and row total formatting for custom formula id
    let builtCols = null;
    function ColProbe() {
        builtCols = useColumnDefs({
            sortOptions: {},
            sorting: [],
            serverSide: true,
            showRowNumbers: false,
            layoutMode: 'hierarchy',
            viewMode: 'pivot',
            rowFields: ['category'],
            colFields: ['region'],
            valConfigs,
            customDimensions: [],
            minMax: {},
            colorScaleMode: 'none',
            colExpanded: {},
            props: {},
            cachedColSchema: res.payload.colSchema,
            responseColumns: res.payload.columns,
            filteredData: [],
            rowCount: 2,
            theme: {},
            defaultColumnWidths: { schemaFallback: 180, rowNumber: 56, hierarchy: 240, dimension: 180, measure: 140, subtotal: 145, collapsedPlaceholder: 60 },
            isColExpanded: () => true,
            toggleCol: () => {},
            decimalPlaces: 0,
            defaultValueFormat: 'number',
            numberGroupSeparator: 'comma',
            handleContextMenu: () => {},
            renderEditableCell: () => null,
        });
        return null;
    }
    ReactDOMServer.renderToStaticMarkup(React.createElement(ColProbe));
    const eastGroup = builtCols.find((c) => c.groupValue === 'East');
    assert.ok(eastGroup);
    const eastFormulaCol = eastGroup.columns.find((c) => c.id === 'East_net_margin');
    assert.ok(eastFormulaCol);
    assert.equal(eastFormulaCol.header, 'Net Margin');
    const colScopeFormulaCol = builtCols.find((c) => c.id === 'east_minus_west');
    assert.ok(colScopeFormulaCol, 'column-scope formula must remain a top-level leaf column');
    assert.equal(colScopeFormulaCol.header, 'East - West');
    assert.equal(
        builtCols.some((c) => String(c.groupValue || '').toLowerCase() === 'ew'),
        false,
        'column-scope formula id ending in _<field> must not be split into a bogus header group'
    );
    const rowTotalFormulaCol = builtCols.find((c) => c.id === '__RowTotal__net_margin');
    assert.ok(rowTotalFormulaCol);
    const rtCell = rowTotalFormulaCol.cell({
        row: { id: 'Alpha', original: alphaRow },
        column: { id: '__RowTotal__net_margin' },
        getValue: () => alphaRow.__RowTotal__net_margin,
    });
    const rtHtml = ReactDOMServer.renderToStaticMarkup(rtCell);
    assert.match(rtHtml, /\$90/);

    // Column-scope [East] - [West] formula when base measure uses a custom id
    const customBaseRes = await transport.request({
        kind: 'data',
        requestId: 'col-scope-custom-base-id',
        table: 'margin_tbl',
        payload: {
            startRow: 0,
            endRow: 20,
            state_override: {
                rowFields: ['category'],
                colFields: ['region'],
                valConfigs: [
                    { id: 'rev_custom', field: 'sales', agg: 'sum', label: 'Revenue' },
                    { id: 'ew_custom_diff', field: 'diff', agg: 'formula', formulaScope: 'columns', formula: '[East] - [West]', label: 'E - W' },
                ],
                showRowTotals: false,
                showColTotals: false,
            },
        },
    });
    const alphaCustomBase = customBaseRes.payload.data.find((r) => r._id === 'Alpha');
    assert.equal(alphaCustomBase.East_rev_custom, 100);
    assert.equal(alphaCustomBase.West_rev_custom, 80);
    assert.equal(alphaCustomBase.ew_custom_diff, 20);

    let customBaseCols = null;
    function CustomBaseColProbe() {
        customBaseCols = useColumnDefs({
            sortOptions: {},
            sorting: [],
            serverSide: true,
            showRowNumbers: false,
            layoutMode: 'hierarchy',
            viewMode: 'pivot',
            rowFields: ['category'],
            colFields: ['region'],
            valConfigs: [
                { id: 'rev_custom', field: 'sales', agg: 'sum', label: 'Revenue' },
                { id: 'ew_custom_diff', field: 'diff', agg: 'formula', formulaScope: 'columns', formula: '[East] - [West]', label: 'E - W' },
            ],
            customDimensions: [],
            minMax: {},
            colorScaleMode: 'none',
            colExpanded: {},
            props: {},
            cachedColSchema: customBaseRes.payload.colSchema,
            responseColumns: customBaseRes.payload.columns,
            filteredData: [],
            rowCount: 2,
            theme: {},
            defaultColumnWidths: { schemaFallback: 180, rowNumber: 56, hierarchy: 240, dimension: 180, measure: 140, subtotal: 145, collapsedPlaceholder: 60 },
            isColExpanded: () => true,
            toggleCol: () => {},
            decimalPlaces: 0,
            defaultValueFormat: 'number',
            numberGroupSeparator: 'comma',
            handleContextMenu: () => {},
            renderEditableCell: () => null,
        });
        return null;
    }
    ReactDOMServer.renderToStaticMarkup(React.createElement(CustomBaseColProbe));
    const ewCustomCol = customBaseCols.find((c) => c.id === 'ew_custom_diff');
    assert.ok(ewCustomCol, 'column-scope formula id ending with _<field> must stay a top-level leaf column');
    assert.equal(ewCustomCol.header, 'E - W');
    assert.equal(
        customBaseCols.some((c) => String(c.groupValue || '').toLowerCase() === 'ew_custom'),
        false,
        'column-scope formula id ending with _<field> must not create a bogus group'
    );
});

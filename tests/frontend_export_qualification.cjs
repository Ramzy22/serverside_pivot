const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));

function loadFrontendModule(relativePath, dependencyStubs = {}) {
    const sourcePath = path.join(frontendRoot, relativePath);
    const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
        filename: sourcePath,
        presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
    }).code;
    const sourceModule = {exports: {}};
    const localRequire = request => (
        Object.prototype.hasOwnProperty.call(dependencyStubs, request)
            ? dependencyStubs[request]
            : require(request)
    );
    new Function('module', 'exports', 'require', transformed)(
        sourceModule,
        sourceModule.exports,
        localRequire,
    );
    return sourceModule.exports;
}

const jobControllers = [];
function makeCommandOptions(overrides = {}) {
    const options = {
        abortGenerationRef: {current: 0},
        activeExportRequestIdRef: {current: null},
        buildExportStyleProfile: () => ({}),
        cancelledExportRequestIdsRef: {current: new Set()},
        clientInstanceRef: {current: 'qualification'},
        COPY_ROW_LIMIT: 100,
        dispatchServerSideRuntimeSetProps: () => undefined,
        EXPORT_ROW_LIMIT: 100,
        exportRequestSequenceRef: {current: 0},
        getDisplayHeaderTextForColumn: () => '',
        getDisplayHeaderTextStackForColumn: () => [],
        getDisplayRows: () => [{original: {region: 'North', amount: 42}}],
        getExportCellDisplayValue: row => row.original.region,
        getExportColumns: () => [{id: 'region'}],
        getExportHeaderLabel: column => column.id,
        getExportRawCellValue: row => row.original.region,
        getServerExportColumnTarget: () => ({
            mode: 'ids',
            colIds: ['region'],
            knownColIds: ['region'],
            totalCols: 1,
        }),
        layoutMode: 'outline',
        mergedHeaderSeparator: ' / ',
        partModal: null,
        pendingCopyIntentRef: {current: null},
        rowCount: 1,
        serverSide: true,
        setExportProgress: () => undefined,
        setIsCopying: () => undefined,
        setIsExporting: () => undefined,
        setPartModal: () => undefined,
        table: {},
        tableName: 'sales',
        theme: {},
        ...overrides,
    };
    options.exportJobControllerRef ||= {current: null};
    jobControllers.push(options.exportJobControllerRef);
    return options;
}

(async () => {
    const writerCalls = [];
    const {createRuntimeExportCommands} = loadFrontendModule(
        'src/lib/runtime/createRuntimeExportCommands.js',
        {
            '../utils/exportUtils': {
                exportPivotTable: async (...args) => { writerCalls.push({format: 'xlsx', args}); },
                exportPivotTableCsv: async (...args) => { writerCalls.push({format: 'csv', args}); },
            },
        },
    );

    const dispatchedPayloads = [];
    const xlsxJobRef = {current: null};
    const csvJobRef = {current: null};
    const commands = createRuntimeExportCommands(makeCommandOptions({
        exportJobControllerRef: xlsxJobRef,
        dispatchServerSideRuntimeSetProps: update => {
            dispatchedPayloads.push(update.runtimeRequest.payload);
        },
    }));

    commands.exportPivot();
    assert.equal(dispatchedPayloads.at(-1).format, 'xlsx');
    // A running server export owns its request until terminal delivery.
    // Exercise CSV through a separate grid instance instead of trying to
    // replace that active job with an unrelated format.
    const csvCommands = createRuntimeExportCommands(makeCommandOptions({
        exportJobControllerRef: csvJobRef,
        dispatchServerSideRuntimeSetProps: update => dispatchedPayloads.push(update.runtimeRequest.payload),
    }));
    csvCommands.exportPivot('csv');
    assert.equal(dispatchedPayloads.at(-1).format, 'csv');
    assert.throws(() => commands.exportPivot('tsv'), /xlsx.*csv|csv.*xlsx/i);

    const liveColumnOrder = [
        'hierarchy',
        'Q1_unités_sum',
        'Q1_ventes_sum',
        '__RowTotal__sum',
    ];
    const {useRuntimeClipboardExportPresentation} = loadFrontendModule(
        'src/lib/runtime/useRuntimeClipboardExportPresentation.js',
        {
            react: {useCallback: callback => callback},
            '../utils/exportUtils': {
                buildMergedHeaderRows: () => ({rows: []}),
                buildStyledHtmlTableExport: () => ({}),
                writeClipboardPayload: () => Promise.resolve(),
            },
            '../utils/helpers': {
                formatDisplayLabel: value => String(value),
                formatValue: value => String(value),
            },
        },
    );
    const exportPresentation = useRuntimeClipboardExportPresentation({
        cachedColSchema: {
            total_center_cols: 2,
            columns: [{id: 'Q1_ventes_sum'}, {id: 'Q1_unités_sum'}],
        },
        serverSide: true,
        table: {
            getVisibleLeafColumns: () => liveColumnOrder.map(id => ({id})),
        },
        theme: {},
        totalCenterCols: 2,
    });
    assert.deepStrictEqual(
        exportPresentation.getCompleteServerExportColIds(),
        liveColumnOrder,
    );
    const orderedTarget = exportPresentation.getServerExportColumnTarget();
    assert.deepStrictEqual(orderedTarget.colIds, liveColumnOrder);

    const orderedCsvPayloads = [];
    const orderedCsvCommands = createRuntimeExportCommands(makeCommandOptions({
        getServerExportColumnTarget: exportPresentation.getServerExportColumnTarget,
        dispatchServerSideRuntimeSetProps: update => {
            orderedCsvPayloads.push(update.runtimeRequest.payload);
        },
    }));
    orderedCsvCommands.exportPivot('csv');
    assert.equal(orderedCsvPayloads.at(-1).format, 'csv');
    assert.deepStrictEqual(orderedCsvPayloads.at(-1).colIds, liveColumnOrder);

    let openedPartModal = null;
    const partitionedCommands = createRuntimeExportCommands(makeCommandOptions({
        rowCount: 101,
        setPartModal: modal => { openedPartModal = modal; },
    }));
    partitionedCommands.exportPivot('csv');
    assert.equal(openedPartModal.exportFormat, 'csv');

    const partPayloads = [];
    const partCommands = createRuntimeExportCommands(makeCommandOptions({
        rowCount: 101,
        partModal: openedPartModal,
        dispatchServerSideRuntimeSetProps: update => {
            partPayloads.push(update.runtimeRequest.payload);
        },
        setPartModal: () => undefined,
    }));
    partCommands.dispatchExportPart(0, 0);
    assert.equal(partPayloads.at(-1).format, 'csv');

    const originalSetTimeout = global.setTimeout;
    let timeoutWork = null;
    global.setTimeout = callback => {
        timeoutWork = Promise.resolve().then(callback);
        return 1;
    };
    try {
        const localCommands = createRuntimeExportCommands(makeCommandOptions({serverSide: false}));
        localCommands.exportPivot('csv');
        await timeoutWork;
    } finally {
        global.setTimeout = originalSetTimeout;
    }
    assert.deepStrictEqual(writerCalls.map(call => call.format), ['csv']);
    assert.equal(writerCalls[0].args[3].filename, 'pivot.csv');

    const savedFiles = [];
    const {buildCsvExport} = loadFrontendModule(
        'src/lib/utils/exportUtils.js',
        {'file-saver': {saveAs: (blob, filename) => savedFiles.push({blob, filename})}},
    );
    await buildCsvExport({
        rows: [{original: {region: 'North, "East"', amount: 42}}],
        columns: [{id: 'region'}, {id: 'amount'}],
        getHeaderLabel: column => column.id === 'region' ? 'Region, area' : 'Amount',
        getCellValue: () => 'formatted value',
        getCellRawValue: (row, column) => row.original[column.id],
        filename: 'pivot.csv',
    });
    assert.equal(savedFiles.length, 1);
    assert.equal(savedFiles[0].filename, 'pivot.csv');
    assert.equal(savedFiles[0].blob.type, 'text/csv;charset=utf-8');
    const csvBytes = new Uint8Array(await savedFiles[0].blob.arrayBuffer());
    assert.deepStrictEqual(Array.from(csvBytes.slice(0, 3)), [0xef, 0xbb, 0xbf]);
    assert.equal(
        new TextDecoder().decode(csvBytes.slice(3)),
        '"Region, area",Amount\r\n"North, ""East""",42',
    );

    // A numeric formatter must not replace logical values in the explicit raw action.
    // The same selected rectangle must retain displayed row/column order in both MIME forms.
    const exportUtils = loadFrontendModule('src/lib/utils/exportUtils.js', {
        'file-saver': {saveAs: () => undefined},
    });
    const sales = {id: 'sales', columnDef: {header: 'Sales'}};
    const units = {id: 'units', columnDef: {header: 'Units'}};
    const rows = [
        {id: 'munich', original: {sales: 49.75, units: 3}, getValue: id => ({sales: 49.75, units: 3})[id]},
        {id: 'eclair', original: {sales: 100.25, units: 2}, getValue: id => ({sales: 100.25, units: 2})[id]},
    ];
    const selected = {
        'munich:units': 3, 'munich:sales': 49.75,
        'eclair:units': 2, 'eclair:sales': 100.25,
    };
    const {useRuntimeClipboardExportPresentation: makePresentation} = loadFrontendModule(
        'src/lib/runtime/useRuntimeClipboardExportPresentation.js',
        {
            react: {useCallback: callback => callback},
            '../utils/exportUtils': exportUtils,
            '../utils/helpers': {
                formatDisplayLabel: value => String(value),
                formatValue: value => String(Math.round(value)),
            },
        },
    );
    const selectionPresentation = makePresentation({
        getDisplayRows: () => rows,
        getDisplayHeaderTextForColumn: column => column.columnDef.header,
        getDisplayHeaderTextStackForColumn: column => [column.columnDef.header],
        matchesSelectedColumnToValueConfig: () => false,
        resolveDisplayedCellValue: (_rowId, _columnId, value) => value,
        selectedCells: selected,
        table: {getVisibleLeafColumns: () => [units, sales]},
        theme: {},
    });
    const displayedSelection = selectionPresentation.buildSelectedClipboardPayload(false, selected);
    assert.equal(displayedSelection.text, '3\t50\n2\t100');
    const rawSelection = selectionPresentation.buildSelectedClipboardPayload(false, selected, {rawValues: true});
    assert.equal(rawSelection.text, '3\t49.75\n2\t100.25');
    assert.match(rawSelection.html, /<td[^>]*>3<\/td><td[^>]*>49\.75<\/td>/);
    assert.match(rawSelection.html, /<td[^>]*>2<\/td><td[^>]*>100\.25<\/td>/);
    const rawWithHeaders = selectionPresentation.buildSelectedClipboardPayload(true, selected, {rawValues: true});
    assert.equal(rawWithHeaders.text, 'Units\tSales\n3\t49.75\n2\t100.25');
    assert.match(rawWithHeaders.html, /<th[^>]*>Units<\/th><th[^>]*>Sales<\/th>/);

    const contractOrderPresentation = makePresentation({
        getDisplayRows: () => [rows[1], rows[0]],
        getDisplayHeaderTextForColumn: column => column.columnDef.header,
        getDisplayHeaderTextStackForColumn: column => [column.columnDef.header],
        matchesSelectedColumnToValueConfig: () => false,
        resolveDisplayedCellValue: (_rowId, _columnId, value) => value,
        table: {getVisibleLeafColumns: () => [sales, units]},
        theme: {},
    });
    const contractDisplayed = contractOrderPresentation.buildSelectedClipboardPayload(false, selected);
    const contractRaw = contractOrderPresentation.buildSelectedClipboardPayload(false, selected, {rawValues: true});
    assert.equal(contractDisplayed.text, '100\t2\n50\t3');
    assert.equal(contractRaw.text, '100.25\t2\n49.75\t3');
    assert.match(contractDisplayed.html, /<td[^>]*>100<\/td><td[^>]*>2<\/td>/);
    assert.match(contractRaw.html, /<td[^>]*>100\.25<\/td><td[^>]*>2<\/td>/);
    assert.match(contractRaw.html, /<td[^>]*>49\.75<\/td><td[^>]*>3<\/td>/);

    const sparsePresentation = makePresentation({
        getDisplayRows: () => rows,
        getDisplayHeaderTextForColumn: column => column.columnDef.header,
        getDisplayHeaderTextStackForColumn: column => [column.columnDef.header],
        matchesSelectedColumnToValueConfig: () => false,
        resolveDisplayedCellValue: (rowId, columnId, value) => rowId === 'munich' && columnId === 'sales' ? 12.375 : rowId === 'eclair' && columnId === 'units' ? 999 : value,
        table: {getVisibleLeafColumns: () => [units, sales, sales]},
        theme: {},
    });
    const sparse = {
        'munich:units': 0, 'munich:sales': 49.75,
        'eclair:sales': null,
    };
    const sparseRaw = sparsePresentation.buildSelectedClipboardPayload(false, sparse, {rawValues: true});
    assert.equal(sparseRaw.text, '0\t12.375\n\t');
    assert.match(sparseRaw.html, /<td[^>]*>0<\/td><td[^>]*>12\.375<\/td>/);

    // A pinned measure can appear after an unpinned measure in TanStack's
    // core leaf list while being rendered first. Current-table copy and local
    // export must follow the rendered order without duplicating pinned cells.
    const q1 = {id: 'Q1', columnDef: {header: 'Q1'}};
    const q2 = {id: 'Q2', columnDef: {header: 'Q2'}};
    const hierarchy = {id: 'hierarchy', columnDef: {header: 'Region'}};
    const total = {id: '__RowTotal__sum', columnDef: {header: 'Total'}};
    const pinnedRow = {id: 'north', original: {_label: 'North', Q1: 10, Q2: 20, __RowTotal__sum: 30, hidden: 40}};
    const pinnedTable = {
        getVisibleLeafColumns: () => [hierarchy, q1, q2, total, q2],
        getLeftVisibleLeafColumns: () => [hierarchy, q2],
        getCenterVisibleLeafColumns: () => [q1, q2],
        getRightVisibleLeafColumns: () => [total],
    };
    const pinnedPresentation = makePresentation({
        cachedColSchema: {total_center_cols: 3, columns: [{id: 'Q1'}, {id: 'Q2'}, {id: 'offscreen'}]},
        getDisplayRows: () => [pinnedRow],
        getDisplayHeaderTextForColumn: column => column.columnDef.header,
        getDisplayHeaderTextStackForColumn: column => [column.columnDef.header],
        matchesSelectedColumnToValueConfig: () => false,
        resolveDisplayedCellValue: (_rowId, _columnId, value) => value,
        serverSide: true,
        table: pinnedTable,
        theme: {},
        totalCenterCols: 3,
    });
    const renderedIds = ['hierarchy', 'Q2', 'Q1', '__RowTotal__sum'];
    assert.deepStrictEqual(pinnedPresentation.getInstantiatedExportColIds(), renderedIds);
    assert.deepStrictEqual(pinnedPresentation.getExportColumns().map(column => column.id), renderedIds);
    assert.deepStrictEqual(pinnedPresentation.getServerExportColumnTarget({currentTable: true}).colIds, renderedIds);
    assert.deepStrictEqual(pinnedPresentation.getCompleteServerExportColIds(), ['hierarchy', 'Q2', 'Q1', 'offscreen', '__RowTotal__sum']);
    const pinnedClipboard = pinnedPresentation.buildTableClipboardPayload({withHeaders: true});
    assert.equal(pinnedClipboard.text, 'Region\tQ2\tQ1\tTotal\nNorth\t20\t10\t30');
    assert.doesNotMatch(pinnedClipboard.text, /Hidden|40/);
    assert.match(pinnedClipboard.html, /<th[^>]*>Region<\/th><th[^>]*>Q2<\/th><th[^>]*>Q1<\/th><th[^>]*>Total<\/th>/);

    const pinnedPayloads = [];
    const pinnedCommands = createRuntimeExportCommands(makeCommandOptions({
        getServerExportColumnTarget: pinnedPresentation.getServerExportColumnTarget,
        dispatchServerSideRuntimeSetProps: update => pinnedPayloads.push(update.runtimeRequest.payload),
    }));
    pinnedCommands.copyCurrentTableToClipboard();
    assert.deepStrictEqual(pinnedPayloads.at(-1).colIds, renderedIds);

    // TanStack header groups retain core order after pinning. Rich HTML must
    // derive merged headers from the same displayed leaves as its body.
    const groupedPinnedPresentation = makePresentation({
        getDisplayRows: () => [pinnedRow],
        getDisplayHeaderTextForColumn: column => column.columnDef.header,
        getDisplayHeaderTextStackForColumn: column => column.id === 'hierarchy' || column.id === '__RowTotal__sum'
            ? [column.columnDef.header] : [column.columnDef.header, 'Sales'],
        matchesSelectedColumnToValueConfig: () => false,
        resolveDisplayedCellValue: (_rowId, _columnId, value) => value,
        serverSide: true,
        table: {
            ...pinnedTable,
            getHeaderGroups: () => [
                {headers: [hierarchy, q1, q2, total].map(column => ({column, colSpan: 1, rowSpan: 1}))},
                {headers: [q1, q2].map(column => ({column, colSpan: 1, rowSpan: 1}))},
            ],
        },
        theme: {},
    });
    const groupedHtml = groupedPinnedPresentation.buildTableClipboardPayload({withHeaders: true}).html;
    assert.match(groupedHtml, /<th[^>]*>Region<\/th><th[^>]*>Q2<\/th><th[^>]*>Q1<\/th><th[^>]*>Total<\/th>/);
    assert.match(groupedHtml, /<tr><th[^>]*>Sales<\/th><th[^>]*>Sales<\/th><\/tr>/);

    // A right-pinned measure must follow even a virtual/offscreen center
    // measure in complete exports; the instantiated list alone cannot infer
    // that boundary.
    const q3 = {id: 'Q3', columnDef: {header: 'Q3'}};
    const rightPinnedPresentation = makePresentation({
        cachedColSchema: {total_center_cols: 4, columns: [{id: 'Q1'}, {id: 'Q2'}, {id: 'offscreen'}, {id: 'Q3'}]},
        serverSide: true,
        table: {
            getVisibleLeafColumns: () => [hierarchy, q1, q2, q3, total],
            getLeftVisibleLeafColumns: () => [hierarchy, q2],
            getCenterVisibleLeafColumns: () => [q1],
            getRightVisibleLeafColumns: () => [q3, total],
        },
        theme: {},
        totalCenterCols: 4,
    });
    assert.deepStrictEqual(rightPinnedPresentation.getInstantiatedExportColIds(), ['hierarchy', 'Q2', 'Q1', 'Q3', '__RowTotal__sum']);
    assert.deepStrictEqual(rightPinnedPresentation.getCompleteServerExportColIds(), ['hierarchy', 'Q2', 'Q1', 'offscreen', 'Q3', '__RowTotal__sum']);

    for (const ref of jobControllers) ref.current?.dispose();
    console.log('frontend export qualification passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});

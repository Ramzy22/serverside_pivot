const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
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

function makeCommandOptions(overrides = {}) {
    return {
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
    const commands = createRuntimeExportCommands(makeCommandOptions({
        dispatchServerSideRuntimeSetProps: update => {
            dispatchedPayloads.push(update.runtimeRequest.payload);
        },
    }));

    commands.exportPivot();
    assert.equal(dispatchedPayloads.at(-1).format, 'xlsx');
    commands.exportPivot('csv');
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

    console.log('frontend export qualification passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});

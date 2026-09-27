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

    console.log('frontend export qualification passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});

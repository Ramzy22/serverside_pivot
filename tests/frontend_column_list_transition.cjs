const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'hooks', 'useColumnVirtualizer.js');
const source = fs.readFileSync(sourcePath, 'utf8').replace(
    'const useStableColumnList =',
    'export const useStableColumnList =',
);
const transformed = babel.transformSync(source, {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;

// Minimal hook harness: useStableColumnList only needs useRef, but retaining the
// other exports lets the complete production module evaluate without a DOM.
const refSlots = [];
let refCursor = 0;
const reactStub = {
    useRef(initialValue) {
        const slot = refCursor;
        refCursor += 1;
        if (!refSlots[slot]) refSlots[slot] = {current: initialValue};
        return refSlots[slot];
    },
    useCallback: (value) => value,
    useEffect: () => undefined,
    useLayoutEffect: () => undefined,
    useMemo: (factory) => factory(),
};
const sourceModule = {exports: {}};
const localRequire = (request) => {
    if (request === 'react') return reactStub;
    if (request === '@tanstack/react-virtual') return {useVirtualizer: () => ({})};
    return require(request);
};
new Function('module', 'exports', 'require', transformed)(
    sourceModule,
    sourceModule.exports,
    localRequire,
);

const {useStableColumnList} = sourceModule.exports;
const fields = ['asset_class', 'option_right', 'trade_date', 'instrument'];
const nasdaqLeaf = {
    asset_class: 'Cash Equity',
    option_right: '',
    trade_date: '2026-03-12',
    instrument: 'AAPL US Equity',
    _path: 'Cash Equity||||||2026-03-12|||AAPL US Equity',
    _pathFields: fields,
    depth: 3,
};

const makeRuntimeColumns = (layoutMode, definitions) => fields.map((field, index) => ({
    id: field,
    getSize: () => 140,
    columnDef: {
        meta: {preserveAccessorDisplayValue: layoutMode === 'tabular'},
        cell: (row) => (
            layoutMode === 'outline' && index !== row.depth ? '' : row[field]
        ),
        sourceDefinition: definitions[index],
    },
}));
const renderStableList = (runtimeColumns, definitions) => {
    refCursor = 0;
    return useStableColumnList(runtimeColumns, definitions);
};

const outlineDefinitions = fields.map((field) => ({id: field, layoutMode: 'outline'}));
const outlineColumns = makeRuntimeColumns('outline', outlineDefinitions);
const stableOutline = renderStableList(outlineColumns, outlineDefinitions);
assert.deepStrictEqual(
    stableOutline.map((column) => column.columnDef.cell(nasdaqLeaf)),
    ['', '', '', 'AAPL US Equity'],
);

// This is the live failure transition: ids and widths are identical, while the
// source definitions contain new Tabular accessor/cell closures and metadata.
const tabularDefinitions = fields.map((field) => ({id: field, layoutMode: 'tabular'}));
const tabularColumns = makeRuntimeColumns('tabular', tabularDefinitions);
const stableTabular = renderStableList(tabularColumns, tabularDefinitions);

assert.strictEqual(stableTabular, tabularColumns);
assert.notStrictEqual(stableTabular, stableOutline);
assert(stableTabular.every((column) => column.columnDef.meta.preserveAccessorDisplayValue));
assert.deepStrictEqual(
    stableTabular.map((column) => column.columnDef.cell(nasdaqLeaf)),
    ['Cash Equity', '', '2026-03-12', 'AAPL US Equity'],
);

console.log('column virtualizer layout transition passed');

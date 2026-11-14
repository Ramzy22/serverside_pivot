const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'utils', 'layoutRows.js');
const source = fs.readFileSync(sourcePath, 'utf8');
const transformed = babel.transformSync(source, {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(
    sourceModule,
    sourceModule.exports,
    require,
);

const {resolveTabularRowFieldValue} = sourceModule.exports;
const fields = ['region', 'country', 'instrument'];
const resolveRow = (row) => fields.map((field, index) => (
    resolveTabularRowFieldValue(row, field, index, 'tabular')
));

// Exact leaf-row shape observed from the real in-memory adapter response.
const completeRuntimeLeaf = {
    _id: 'Bond',
    _path: 'North|||Canada|||Bond',
    _pathFields: fields,
    depth: 2,
    region: 'North',
    country: 'Canada',
    instrument: 'Bond',
};
assert.deepStrictEqual(resolveRow(completeRuntimeLeaf), ['North', 'Canada', 'Bond']);

// Hierarchical controllers may omit ancestor keys while preserving the same
// transport path contract. Tabular must still repeat every ancestor value.
const pathOnlyRuntimeLeaf = {
    _id: 'Bond',
    _path: 'North|||Canada|||Bond',
    _pathFields: fields,
    depth: 2,
    instrument: 'Bond',
};
assert.deepStrictEqual(resolveRow(pathOnlyRuntimeLeaf), ['North', 'Canada', 'Bond']);

const outlineValues = fields.map((field, index) => (
    resolveTabularRowFieldValue(pathOnlyRuntimeLeaf, field, index, 'outline')
));
assert.deepStrictEqual(outlineValues, [undefined, undefined, 'Bond']);

console.log('tabular hierarchy value resolution passed');

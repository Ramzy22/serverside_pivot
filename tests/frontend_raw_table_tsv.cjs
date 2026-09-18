const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'utils', 'rawTableTsv.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, { targets: { node: 'current' }, modules: 'commonjs' }]],
}).code;
const sourceModule = { exports: {} };
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const columns = [
    { id: 'hierarchy', columnDef: {} },
    { id: 'amount', columnDef: { header: 'Amount', accessorKey: 'amount' } },
    { id: 'derived', columnDef: { accessorFn: (row, index) => `${row.amount}-${index}` } },
];
assert.equal(
    sourceModule.exports.buildTsvFromRawRows([{ _id: 'Europe', depth: 1, amount: 12 }], true, columns),
    'hierarchy\tAmount\tderived\n  Europe\t12\t12-0\n'
);
assert.equal(sourceModule.exports.buildTsvFromRawRows([], false, columns), null);

console.log('raw table TSV modularization passed');

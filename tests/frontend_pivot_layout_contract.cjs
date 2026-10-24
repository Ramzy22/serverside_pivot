const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..', 'dash_tanstack_pivot');
const babel = require(path.join(root, 'node_modules', '@babel/core'));

function load(relativePath) {
  const file = path.join(root, relativePath);
  const transformed = babel.transformSync(fs.readFileSync(file, 'utf8'), {
    filename: file,
    presets: [[require(path.join(root, 'node_modules', '@babel/preset-env')), {
      targets: {node: 'current'}, modules: 'commonjs',
    }]],
  }).code;
  const module = {exports: {}};
  const localRequire = name => name === '../utils/helpers'
    ? {formatDisplayLabel: value => value, formatAggLabel: value => value}
    : require(name);
  new Function('module', 'exports', 'require', transformed)(module, module.exports, localRequire);
  return module.exports;
}

const {reconcilePivotLayoutOrder, pivotLayoutRequestFields} = load('src/lib/utils/columnOrder.js');
const {mergeSparseColSchema} = load('src/lib/hooks/usePivotNormalization.js');
const schema = {layoutRevision: 'a'.repeat(64), total_center_cols: 3, columns: [
  {index: 0, id: 'A_sales', layoutId: 'p:A'},
  {index: 1, id: 'B_sales', layoutId: 'p:B'},
  {index: 2, id: 'C_sales', layoutId: 'p:C'},
]};

assert.deepEqual(reconcilePivotLayoutOrder(['p:missing', 'p:A', 'p:B'],
  ['B_sales', 'A_sales'], schema), ['p:missing', 'p:B', 'p:A']);
const changed = pivotLayoutRequestFields(['p:B', 'p:A'], 'a'.repeat(64), '',
  ['C_sales'], schema);
assert.deepEqual(changed.layoutManifest, ['p:B', 'p:A']);
assert.deepEqual(changed.pinnedColumnIds, ['p:C']);
const acknowledged = pivotLayoutRequestFields(['p:B', 'p:A'], 'a'.repeat(64),
  'p:B\u001fp:A', ['offscreen'], schema, false, {offscreen: 'p:offscreen'});
assert.equal(acknowledged.layoutManifest, undefined);
assert.deepEqual(acknowledged.pinnedColumnIds, ['p:offscreen']);

const oldSchema = {layoutRevision: 'a'.repeat(64), total_center_cols: 3,
  columns: [{index: 0, id: 'old'}, {index: 1, id: 'B_sales'}]};
const nextSchema = mergeSparseColSchema(oldSchema, {layoutRevision: 'b'.repeat(64),
  total_center_cols: 3, columns: [{index: 0, id: 'C_sales'}]});
assert.equal(nextSchema.columns[0].id, 'C_sales');
assert.equal(nextSchema.columns[1], null);

console.log('Pivot layout client contract passed');

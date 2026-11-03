/* Actual chart runtime assertions; qualificationEvidence:false. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../dash_tanstack_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
const cache = new Map();
function load(relative) {
  let filename = path.resolve(root, 'src/lib', relative);
  if (!path.extname(filename)) filename += '.js';
  if (cache.has(filename)) return cache.get(filename).exports;
  const module = {exports: {}};
  cache.set(filename, module);
  const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
    presets: [[req('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}], req('@babel/preset-react')]}).code;
  new Function('module', 'exports', 'require', code)(module, module.exports, name =>
    name.startsWith('.') ? load(path.relative(path.join(root, 'src/lib'), path.resolve(path.dirname(filename), name))) : req(name));
  return module.exports;
}
const {buildSelectionChartModel} = load('utils/chartModelBuilders.js');
const ids = ['Q1_ventes_€_sum', 'Q1_unités_sum'];
const cols = ids.map((id, i) => ({id, columnDef: {header: i ? 'unités (sum)' : 'ventes_€ (sum)'}}));
const rows = ['Éclair', 'München', 'São Paulo'].map(id => ({id, original: {_path: id, _depth: 0}}));
const selected = {'Éclair:Q1_ventes_€_sum': 100.25, 'Éclair:Q1_unités_sum': 2,
  'München:Q1_ventes_€_sum': 49.75, 'München:Q1_unités_sum': 3};
const options = {orientation: 'rows', rowFields: ['region', 'account'], colFields: ['period']};
const initial = buildSelectionChartModel(selected, rows, cols, options);
assert.deepEqual(initial.categories, ['Éclair', 'München']);
assert.deepEqual(initial.series.map(s => s.values), [[100.25, 49.75], [2, 3]]);
assert.equal(initial.series.length, 2);
const next = buildSelectionChartModel(Object.fromEntries(Object.entries(selected).filter(([key]) => key.startsWith('München:'))), rows, cols, options);
assert.deepEqual(next.categories, ['München'], 'By Rows must retain row categories after a one-row reselection');
assert.deepEqual(next.series.map(s => s.name), initial.series.map(s => s.name));
assert.deepEqual(next.series.map(s => s.values), [[49.75], [3]]);
// Preserve the public auto orientation for callers that did not choose rows.
const auto = buildSelectionChartModel({'München:Q1_ventes_€_sum': 49.75, 'München:Q1_unités_sum': 3}, rows, cols, {...options, orientation:'auto'});
assert.equal(auto.series.length, 1);
console.log('flagship chart runtime passed');

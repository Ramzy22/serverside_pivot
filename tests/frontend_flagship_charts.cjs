/* Actual chart runtime assertions; qualificationEvidence:false. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../serverside_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
const cache = new Map();
function load(relative) {
  let filename = path.resolve(root, 'src/lib', relative);
  if (!path.extname(filename)) filename = ['.js', '.jsx', '.ts', '.tsx'].map(ext => filename + ext).find(candidate => fs.existsSync(candidate)) || filename + '.js';
  if (cache.has(filename)) return cache.get(filename).exports;
  const module = {exports: {}};
  cache.set(filename, module);
  const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
    presets: [[req('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}], req('@babel/preset-react'), ...(/\.tsx?$/.test(filename) ? [[req('@babel/preset-typescript'), {isTSX: filename.endsWith('.tsx'), allExtensions: true}]] : [])]}).code;
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

// Pivot chart default: top level, first measure, series = column members (no leaf explosion).
const {buildPivotChartModel, buildUniqueRowLabels} = load('utils/chartModelBuilders.js');
const quarterCols = ['Q4', 'Q3'].flatMap(q => ['revenue_sum', 'units_sum'].map(m => ({
  id: `${q}_${m}`, columnDef: {header: m === 'revenue_sum' ? 'Revenue' : 'Units'}, parent: {id: `group_${q}`, columnDef: {header: q}},
})));
const pivotRow = (pathParts, depth, base) => ({id: pathParts.join('|||'), original: {
  _id: pathParts[pathParts.length - 1], _path: pathParts.join('|||'), _depth: depth,
  Q4_revenue_sum: base, Q4_units_sum: base / 10, Q3_revenue_sum: base - 1, Q3_units_sum: base / 20}});
const pivotRows = [];
['APAC', 'EMEA'].forEach((region, r) => {
  pivotRows.push(pivotRow([region], 0, 1000 + r));
  ['Commercial', 'Enterprise'].forEach((segment, s) => {
    pivotRows.push(pivotRow([region, segment], 1, 500 + s));
    ['Atlas', 'Beacon'].forEach((product, p) => pivotRows.push(pivotRow([region, segment, product], 2, 100 + p)));
  });
});
const defaults = buildPivotChartModel(pivotRows, quarterCols, {orientation: 'rows', hierarchyLevel: 1, measureIds: 'auto', maxRows: 50, maxColumns: 10});
assert.deepEqual(defaults.categories, ['APAC', 'EMEA'], 'default chart categories are the top-level rows');
assert.deepEqual(defaults.series.map(entry => entry.name), ['Q4', 'Q3'], 'default series are the first measure per column member');
assert.deepEqual(defaults.availableMeasures, ['Revenue', 'Units']);
assert.deepEqual(defaults.activeMeasures, ['Revenue']);
const both = buildPivotChartModel(pivotRows, quarterCols, {orientation: 'rows', hierarchyLevel: 1, measureIds: 'all', maxRows: 50, maxColumns: 10});
assert.equal(both.series.length, 4);
const picked = buildPivotChartModel(pivotRows, quarterCols, {orientation: 'rows', hierarchyLevel: 1, measureIds: ['Units'], maxRows: 50, maxColumns: 10});
assert.deepEqual(picked.activeMeasures, ['Units']);
// Rows-as-series are capped at 12 and leaf names that repeat carry their parent path.
const leafSeries = buildPivotChartModel(pivotRows, quarterCols, {orientation: 'columns', hierarchyLevel: 'all', measureIds: 'all', maxRows: 50, maxColumns: 10});
const names = leafSeries.series.map(entry => entry.name);
assert.ok(names.length <= 12);
assert.equal(new Set(names).size, names.length, `series names must be unique: ${names}`);
assert.ok(names.includes('APAC › Commercial › Atlas'), names.join(', '));
assert.deepEqual(buildUniqueRowLabels([pivotRows[0], pivotRows[2]]), ['APAC', 'Atlas']);

// Anchored placement: below by default, capped to the space there; above only when cramped.
const {computeAnchoredPlacement} = load('utils/anchoredPlacement.js');
const view = {width: 320, viewportWidth: 1600, viewportHeight: 1000, margin: 12, gap: 4};
const roomy = computeAnchoredPlacement({top: 100, bottom: 140, left: 50}, {...view, contentHeight: 400});
assert.equal(roomy.placement, 'below'); assert.equal(roomy.top, 144);
const lowHeader = computeAnchoredPlacement({top: 660, bottom: 700, left: 50}, {...view, contentHeight: 400});
assert.equal(lowHeader.placement, 'below', 'a header with ~280px below still opens below');
assert.equal(lowHeader.maxHeight, 1000 - 700 - 4 - 12);
const cramped = computeAnchoredPlacement({top: 900, bottom: 940, left: 50}, {...view, contentHeight: 400});
assert.equal(cramped.placement, 'above');
assert.ok(cramped.top >= 12 && cramped.top + Math.min(400, cramped.maxHeight) <= 900);
const edge = computeAnchoredPlacement({top: 100, bottom: 140, left: 1500}, {...view});
assert.equal(edge.left, 1600 - 320 - 12, 'clamped inside the viewport');

console.log('flagship chart runtime passed');

/* Actual report layout implementation; supporting runtime tests, no browser evidence. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../dash_tanstack_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
function compile(relative) {
  const filename = path.join(root, 'src/lib', relative);
  return babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
    presets: [[req('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
      req('@babel/preset-react'), ...(filename.endsWith('.tsx') ? [req('@babel/preset-typescript')] : [])]}).code;
}
const moduleUnderTest = {exports: {}};
new Function('module', 'exports', compile('utils/savedReports.js'))(moduleUnderTest, moduleUnderTest.exports);
const {captureReportLayout, applySavedReport} = moduleUnderTest.exports;
const values = [{field: 'sales', agg: 'sum', label: 'Sales'}];
const columns = ['period'];
const report = {id: 'report-1', reportDef: {root: {field: 'region'}}, layout: captureReportLayout(columns, values)};
columns.push('account'); values[0].agg = 'avg';
let state = {colFields: ['account'], valConfigs: [{field: 'units', agg: 'count'}], reportDef: {root: {field: 'account'}}};
const setters = Object.fromEntries(['ReportDef', 'ActiveReportId', 'ColFields', 'ValConfigs'].map(name =>
  ['set' + name, value => {state[name[0].toLowerCase() + name.slice(1)] = value}]));
assert.equal(applySavedReport(report, setters), true);
assert.deepEqual(state, {colFields: ['period'], valConfigs: [{field: 'sales', agg: 'sum', label: 'Sales'}],
  reportDef: {root: {field: 'region'}}, activeReportId: 'report-1'});
state.valConfigs[0].agg = 'max'; state.reportDef.root.field = 'account'; state.colFields.push('account');
assert.deepEqual(report.layout, {colFields: ['period'], valConfigs: [{field: 'sales', agg: 'sum', label: 'Sales'}]});
assert.equal(report.reportDef.root.field, 'region');
assert.equal(applySavedReport({id: 'legacy', reportDef: {root: {field: 'account'}}}, setters), true);
assert.deepEqual(state.colFields, []); // Legacy record keeps its flat report rendering.
assert.equal(state.valConfigs[0].agg, 'max');
const before = JSON.stringify(state);
assert.equal(applySavedReport(null, setters), false);
assert.equal(JSON.stringify(state), before);
for (const file of ['components/Sidebar/ReportEditor.js', 'components/Sidebar/SidebarPanel.js',
  'components/PivotAppBar.js', 'components/PivotWorkspaceShell.js', 'contexts/PivotConfigContext.js',
  'runtime/useCoreRuntimeStage.tsx']) compile(file);
console.log('saved report runtime passed');

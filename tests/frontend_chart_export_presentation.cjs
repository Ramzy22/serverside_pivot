const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'components', 'Charts', 'chartExportPresentation.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const {layoutLegendItems, normalizeChartPresentation} = sourceModule.exports;
assert.deepStrictEqual(normalizeChartPresentation({title: '  Sales ', legendPosition: 'middle'}), {
    title: 'Sales',
    legendPosition: 'bottom',
    legendItems: [],
    background: '#ffffff',
    textColor: '#111827',
    secondaryTextColor: '#4b5563',
});
const rows = layoutLegendItems([{label: 'Alpha'}, {label: 'Beta'}], 100, label => label.length * 10);
assert.equal(rows.length, 2);
assert.equal(rows[0][0].label, 'Alpha');

console.log('chart export presentation modularization passed');

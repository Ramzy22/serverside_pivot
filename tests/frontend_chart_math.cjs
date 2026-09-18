const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'components', 'Charts', 'chartMath.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const {
    canStackAreaSeries,
    clampChartPanelBaseWidth,
    computeLinearTrendline,
    computeMovingAverage,
} = sourceModule.exports;

assert.equal(clampChartPanelBaseWidth('999', 430, 240, 700), 700);
assert.equal(clampChartPanelBaseWidth('invalid', 430, 240, 700), 430);
assert.deepStrictEqual(computeLinearTrendline([1, 3, 5]), [1, 3, 5]);
assert.deepStrictEqual(computeMovingAverage([2, 4, 8], 2), [2, 3, 6]);
assert.equal(canStackAreaSeries({series: [{}, {}]}), true);
assert.equal(canStackAreaSeries({series: [{}]}), false);

console.log('chart math modularization passed');

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'utils', 'sparklineDetail.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const {formatSparklineDetailExtremum, resolveSparklineDetailRange, scaleSparklineDetailValue} = sourceModule.exports;
assert.deepStrictEqual(resolveSparklineDetailRange([5, 5], 'line'), {min: 4, max: 6});
assert.deepStrictEqual(resolveSparklineDetailRange([-4, 8], 'column'), {min: -4, max: 8});
assert.equal(scaleSparklineDetailValue(5, 0, 10, 20, 120), 70);
assert.equal(formatSparklineDetailExtremum([{value: 8}, {value: -2}], 'min'), '-2');

console.log('sparkline detail modularization passed');

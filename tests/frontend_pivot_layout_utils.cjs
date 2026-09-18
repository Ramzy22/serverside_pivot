const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'utils', 'pivotLayoutUtils.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const {formatLocaleTextTemplate, getStickyHeaderHeight} = sourceModule.exports;
assert.equal(getStickyHeaderHeight(2, 32, true), 96);
assert.equal(formatLocaleTextTemplate('Showing {count} of {total}', {count: 3}), 'Showing 3 of {total}');

console.log('pivot layout utility modularization passed');

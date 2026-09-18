const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'components', 'Sidebar', 'formulaUtils.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const {getFormulaReferenceKey, isColumnFormulaScope, normalizeFormulaReferenceKey, normalizeFormulaScope} = sourceModule.exports;
assert.equal(normalizeFormulaReferenceKey('Spread %', 'formula'), 'spread');
assert.equal(normalizeFormulaReferenceKey('4Q Spread', 'formula'), 'f_4qspread');
assert.equal(getFormulaReferenceKey({label: 'Coupon Rate', field: 'coupon'}), 'couponrate');
assert.equal(normalizeFormulaScope('displayed_columns'), 'columns');
assert.equal(isColumnFormulaScope('measures'), false);

console.log('sidebar formula utils modularization passed');

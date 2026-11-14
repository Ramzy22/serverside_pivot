const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'components', 'Sidebar', 'customCategoryUtils.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
const localRequire = (request) => {
    if (request.includes('utils/helpers')) return {formatDisplayLabel: value => String(value).replace(/_/g, ' ')};
    if (request.includes('usePivotNormalization')) {
        return {
            applyCustomDimensionsToRows: rows => rows,
            buildCustomCategoryFieldId: id => `custom:${id}`,
            evaluateCustomCategoryCondition: (row, condition) => row && row.match === condition.match,
            normalizeCustomCategoryId: value => String(value).toLowerCase().replace(/[^a-z0-9]+/g, '_'),
            normalizeCustomDimensionsValue: values => values,
        };
    }
    return require(request);
};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, localRequire);

const {
    createDateGroupDraft,
    createNumberGroupDraft,
    getAllowedCustomCategoryDependencies,
    getCustomCategoryRuleMatchSummary,
    splitListInput,
} = sourceModule.exports;

assert.equal(createDateGroupDraft(['trade_date'], [{trade_date: '2026-01-20'}]).sourceField, 'trade_date');
assert.equal(createNumberGroupDraft(['sales'], [{sales: 10}]).binSize, 10);
assert.deepStrictEqual(splitListInput(' North, South ,,East '), ['North', 'South', 'East']);
assert.deepStrictEqual(getAllowedCustomCategoryDependencies([{field: 'region'}, {field: 'desk'}], {__editingField: 'desk'}), [{field: 'region'}]);
assert.deepStrictEqual(getCustomCategoryRuleMatchSummary([{match: 'x'}, {match: 'y'}, {match: 'x'}], {
    rules: [{label: 'Match x', condition: {match: 'x'}}],
}), [{label: 'Match x', count: 2}]);

console.log('custom category utility modularization passed');

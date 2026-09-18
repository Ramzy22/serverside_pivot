const assert = require('assert');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env'));
const sourcePath = path.join(frontendRoot, 'src', 'lib', 'utils', 'horizontalViewport.js');
const transformed = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;
const sourceModule = {exports: {}};
new Function('module', 'exports', 'require', transformed)(sourceModule, sourceModule.exports, require);

const { estimateUrgentHorizontalColumnRange } = sourceModule.exports;

// A 500px hierarchy rail is sticky. Its width must not be subtracted from
// scrollLeft: the browser scroll offset already addresses the center track.
const wideRailJump = estimateUrgentHorizontalColumnRange({
    scrollLeft: 1400,
    clientWidth: 500,
    scrollWidth: 10500,
    averageColumnWidth: 100,
    centerColumnCount: 100,
});
assert.equal(wideRailJump.start, 12);
assert.equal(wideRailJump.end, 21);
assert(wideRailJump.start <= 14 && wideRailJump.end >= 18);

const rightEdge = estimateUrgentHorizontalColumnRange({
    scrollLeft: 9500,
    clientWidth: 500,
    scrollWidth: 10000,
    averageColumnWidth: 100,
    centerColumnCount: 100,
});
assert.equal(rightEdge.atRightEdge, true);
assert.equal(rightEdge.end, 99);
assert(rightEdge.start <= 95);

assert.equal(estimateUrgentHorizontalColumnRange({
    scrollLeft: 0,
    clientWidth: 500,
    scrollWidth: 500,
    averageColumnWidth: 100,
    centerColumnCount: 0,
}), null);

console.log('horizontal viewport range calculations passed');

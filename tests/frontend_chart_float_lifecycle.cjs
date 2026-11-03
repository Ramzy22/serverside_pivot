/* Execute real chart composition call sites and leaf hooks; qualificationEvidence:false. */
const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const req = createRequire(path.join(frontend, 'package.json'));
const babel = req('@babel/core');
const React = req('react');
const presets = [[req('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
    req('@babel/preset-typescript'), req('@babel/preset-react')];
const effects = [];
const hooks = {...React, useCallback: callback => callback, useMemo: callback => callback(),
    useEffect: callback => effects.push(callback), useLayoutEffect: callback => effects.push(callback)};
const cache = new Map();
function load(relative) {
    let filename = path.resolve(frontend, 'src/lib', relative);
    if (!path.extname(filename)) filename += '.js';
    if (cache.has(filename)) return cache.get(filename).exports;
    const module = {exports: {}};
    cache.set(filename, module);
    const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
    new Function('module', 'exports', 'require', code)(module, module.exports, name =>
        name === 'react' ? hooks : name.startsWith('.')
            ? load(path.relative(path.join(frontend, 'src/lib'), path.resolve(path.dirname(filename), name))) : req(name));
    return module.exports;
}
const ports = {
    ...load('runtime/useRuntimeChartCanvasCommands.js'),
    ...load('runtime/useRuntimeChartFloatingInteractions.js'),
};
// Compile the actual chart hook calls and their publication loops in their
// production stage order. Unrelated grid/transport hooks need no simulation.
// This catches an early closure capturing an absent command, unlike testing
// the floating leaf with an independently injected command.
function composeChartStage(relative, stage) {
    const filename = path.join(frontend, 'src/lib/runtime', relative);
    const ast = babel.parseSync(fs.readFileSync(filename, 'utf8'), {filename, presets});
    const owner = ast.program.body.find(node => node.type === 'ExportNamedDeclaration' &&
        node.declaration?.type === 'FunctionDeclaration').declaration;
    const selected = [];
    const results = new Set();
    for (const node of owner.body.body) {
        if (node.type === 'VariableDeclaration' && node.declarations.length === 1) {
            const declaration = node.declarations[0];
            if (declaration.init?.type === 'CallExpression' && ports[declaration.init.callee.name]) {
                selected.push(node);
                results.add(declaration.id.name);
            }
        }
        if (node.type === 'ForOfStatement' && node.right.type === 'CallExpression' &&
            node.right.callee?.object?.name === 'Object' && node.right.callee?.property?.name === 'entries' &&
            results.has(node.right.arguments[0]?.name)) selected.push(node);
    }
    const program = babel.types.file(babel.types.program(selected));
    const code = babel.transformFromAstSync(program, undefined, {filename, presets}).code;
    new Function('stage', ...Object.keys(ports), code)(stage, ...Object.values(ports));
    return stage;
}
const {normalizeChartCanvasPane} = load('hooks/usePivotNormalization.js');
const {useChartWorkspacePresentation} = load('runtime/useChartWorkspacePresentation.js');
const noop = () => {};
const ref = current => ({current});
function fixture() {
    let panes = [normalizeChartCanvasPane({id: 'sales', name: 'Revenue', source: 'selection',
        chartType: 'bar', chartHeight: 320, width: 480, size: 2, dockPosition: 'left',
        floatingRect: {left: 36, top: 36, width: 480, height: 540}}, {})];
    const model = {categories: ['Paris'], series: [{name: 'Sales', values: [42]}]};
    const base = {
        chartCanvasFloatingDragRef: ref(null), chartCanvasFloatingResizeRef: ref(null),
        chartPanelFloatingDragRef: ref(null), chartPanelFloatingResizeRef: ref(null),
        chartCanvasLayoutRef: ref({getBoundingClientRect: () => ({width: 1400, height: 900})}),
        chartLayoutRef: ref(null), chartPanelFloating: false,
        initialChartDefinition: {}, completedChartRequestSignaturesRef: ref({}),
        setChartCanvasPanes: next => {panes = typeof next === 'function' ? next(panes) : next;},
        setChartPaneDataById: noop, setPreviewChartPaneId: noop, setTableCanvasSize: noop,
        setChartPanelFloating: noop, setChartPanelFloatingRect: noop,
        setChartPanelGraphHeight: noop, setChartPanelWidth: noop, clampChartPanelWidth: value => value,
    };
    function render() {
        const core = composeChartStage('useCoreRuntimeStage.tsx', {...base, chartCanvasPanes: panes});
        const command = composeChartStage('useCommandRuntimeStage.tsx', {...core});
        return {command, presentation: useChartWorkspacePresentation({...command,
            chartCanvasPaneModels: {sales: model}, chartPaneDataById: {},
            visibleLeafColumns: [], setChartCanvasPaneWidthHints: noop})};
    }
    return {render, panes: () => panes, model};
}

test('the docked pane Float callback reaches the command owner and Dock preserves its source/model', () => {
    const app = fixture();
    let {presentation} = app.render();
    assert.deepEqual(presentation.chartWorkspacePanes.docked.map(pane => pane.id), ['sales']);
    const docked = presentation.renderChartCanvasPane(app.panes()[0], 'left');
    assert.doesNotThrow(() => docked.props.onToggleFloating(), 'Float must receive the materialized pane update command');
    ({presentation} = app.render());
    assert.equal(presentation.chartWorkspacePanes.docked.length, 0);
    assert.deepEqual(presentation.floatingChartCanvasPanes.map(pane => pane.id), ['sales']);
    const floating = presentation.renderFloatingChartCanvasPane(app.panes()[0]);
    assert.equal(floating.props.floating, true);
    assert.equal(floating.props.source, 'selection');
    assert.equal(floating.props.model, app.model);
    floating.props.onToggleFloating();
    ({presentation} = app.render());
    assert.equal(presentation.floatingChartCanvasPanes.length, 0);
    assert.deepEqual(presentation.dockedChartCanvasPanesByPosition.left.map(pane => pane.id), ['sales']);
    assert.equal(app.panes()[0].floating, false);
    assert.equal(app.panes()[0].source, 'selection');
    assert.equal(app.panes()[0].size, 2);
});

test('floating drag and resize update the same pane through its command owner', () => {
    const app = fixture();
    let {command} = app.render();
    command.handleToggleChartCanvasPaneFloating('sales');
    ({command} = app.render());
    const previousWindow = global.window;
    const listeners = new Map();
    global.window = {addEventListener: (name, fn) => listeners.set(name, fn),
        removeEventListener: noop};
    effects.length = 0;
    app.render();
    const cleanups = effects.map(effect => effect()).filter(value => typeof value === 'function');
    try {
        const event = {clientX: 100, clientY: 100, preventDefault: noop, stopPropagation: noop};
        command.handleStartChartCanvasPaneFloatingDrag('sales', event);
        listeners.get('mousemove')({clientX: 120, clientY: 130});
        assert.deepEqual(app.panes()[0].floatingRect, {left: 56, top: 66, width: 480, height: 540});
        listeners.get('mouseup')();
        ({command} = app.render());
        command.handleStartChartCanvasPaneFloatingResize('sales', 'corner', event);
        listeners.get('mousemove')({clientX: 140, clientY: 150});
        assert.deepEqual(app.panes()[0].floatingRect, {left: 56, top: 66, width: 520, height: 590});
        assert.equal(app.panes()[0].width, 520);
        assert.equal(app.panes()[0].chartHeight, 370);
        assert.equal(app.panes()[0].id, 'sales');
        assert.equal(app.panes()[0].source, 'selection');
    } finally {
        cleanups.forEach(cleanup => cleanup());
        global.window = previousWindow;
    }
});

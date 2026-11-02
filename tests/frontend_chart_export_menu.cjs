// Source regression only; installed-host native-click qualification remains separate.
const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const sourcePath = path.join(frontend, 'src/lib/components/Charts/PivotCharts.js');

test('chart export escapes the chart pane clipping boundary through a portal', () => {
    const source = fs.readFileSync(sourcePath, 'utf8');
    assert.ok(/createPortal\(/.test(source), 'An inline dropdown is clipped by the chart surface and workspace panes');
    assert.ok(/useChartExportMenu\(/.test(source));
    assert.ok(/position: 'fixed'/.test(source));
});

const babel = require(path.join(frontend, 'node_modules/@babel/core'));
const source = path.join(frontend, 'src/lib/components/Charts/useChartExportMenu.js');
function load(hooks = {}) {
    const module = {exports: {}};
    const code = babel.transformSync(fs.readFileSync(source, 'utf8'), {
        filename: source,
        presets: [[require(path.join(frontend, 'node_modules/@babel/preset-env')), {targets: {node: 'current'}, modules: 'commonjs'}]],
    }).code;
    new Function('module', 'exports', 'require', code)(module, module.exports, () => hooks);
    return module.exports;
}
const {placeChartExportMenu} = load();

test('bottom-edge chart menu flips above the trigger, keeping CSV inside the viewport', () => {
    const position = placeChartExportMenu({top: 870, bottom: 900, right: 1358}, {width: 190, height: 165}, {width: 1658, height: 1046});
    assert.equal(position.top, 699);
    assert.equal(position.left, 1168);
    assert.ok(position.top + 165 <= 1046 - 8);
});

test('menu opens below with enough space and clamps both horizontal edges', () => {
    const menu = {width: 190, height: 165}, viewport = {width: 800, height: 600};
    assert.equal(placeChartExportMenu({top: 20, bottom: 50, right: 100}, menu, viewport).left, 8);
    const position = placeChartExportMenu({top: 20, bottom: 50, right: 1000}, menu, viewport);
    assert.equal(position.left, 602);
    assert.equal(position.top, 56);
});

test('small iframe viewport constrains menu width and makes tall menu scrollable', () => {
    const position = placeChartExportMenu({top: 80, bottom: 110, right: 130}, {width: 190, height: 165}, {width: 150, height: 180});
    assert.equal(position.maxWidth, 134);
    assert.equal(position.maxHeight, 66);
    assert.equal(position.top, 8);
    assert.equal(position.left, 8);
});

function harness({fullscreen = false} = {}) {
    const listeners = new Map(), removed = [], writes = [], items = [];
    const view = {
        addEventListener: (key, fn) => listeners.set('window:' + key, fn),
        removeEventListener: key => removed.push('window:' + key),
    };
    const doc = {
        body: {}, fullscreenElement: fullscreen ? {} : null, defaultView: view,
        documentElement: {clientWidth: 800, clientHeight: 600},
        addEventListener: (key, fn) => listeners.set(key, fn),
        removeEventListener: key => removed.push(key),
    };
    const anchor = {
        ownerDocument: doc, contains: value => value === anchor,
        getBoundingClientRect: () => ({top: 520, bottom: 550, right: 700}),
        focus: () => { doc.activeElement = anchor; },
    };
    for (let i = 0; i < 2; i++) items.push({focus: () => {doc.activeElement = items[i];}});
    const menu = {
        offsetWidth: 190, scrollHeight: 163,
        contains: value => value === menu || items.includes(value),
        querySelectorAll: () => items,
        querySelector: () => items[0],
    };
    const refs = [{current: anchor}, {current: menu}];
    let refIndex = 0, effects = [], latestPosition = null;
    const hooks = {
        useRef: () => refs[refIndex++],
        useState: () => [latestPosition, value => {latestPosition = value;}],
        useLayoutEffect: fn => {effects.push(fn);},
    };
    const {useChartExportMenu} = load(hooks);
    const setOpen = value => writes.push(value);
    const result = useChartExportMenu(true, setOpen);
    const cleanup = effects[0]();
    effects[1]();
    assert.equal(doc.activeElement, undefined, 'Hidden menu must not receive focus before measurement');
    refIndex = 0;
    effects = [];
    useChartExportMenu(true, setOpen);
    effects[1]();
    return {result, listeners, removed, writes, items, anchor, menu, doc, cleanup, position: () => latestPosition};
}

test('portal uses owner document, repositions on scroll/resize, and removes its listeners', () => {
    const h = harness();
    assert.equal(h.result.portalTarget, h.doc.body);
    assert.equal(h.position().top, 349);
    h.doc.documentElement.clientHeight = 900;
    h.listeners.get('scroll')();
    assert.equal(h.position().top, 556);
    h.doc.documentElement.clientWidth = 500;
    h.listeners.get('window:resize')();
    assert.equal(h.position().left, 302);
    h.cleanup();
    assert.deepEqual(h.removed.sort(), ['keydown', 'mousedown', 'scroll', 'window:resize']);
});

test('shadow-retargeted trigger and menu clicks stay open; outside clicks dismiss', () => {
    const h = harness();
    h.listeners.get('mousedown')({target: {}, composedPath: () => [h.anchor, {}]});
    h.listeners.get('mousedown')({target: h.items[1], composedPath: () => [h.items[1], h.menu]});
    assert.deepEqual(h.writes, []);
    h.listeners.get('mousedown')({target: {}, composedPath: () => [{}]});
    assert.deepEqual(h.writes, [false]);
    h.cleanup();
});

test('keyboard focus skips disabled entries, navigates, and Escape restores trigger focus', () => {
    const h = harness({fullscreen: true});
    assert.equal(h.result.portalTarget, h.doc.fullscreenElement);
    assert.equal(h.doc.activeElement, h.items[0]);
    const key = key => h.listeners.get('keydown')({key, target: h.doc.activeElement, preventDefault() {}, stopPropagation() {}});
    key('ArrowDown');
    assert.equal(h.doc.activeElement, h.items[1]);
    key('Home');
    assert.equal(h.doc.activeElement, h.items[0]);
    key('End');
    assert.equal(h.doc.activeElement, h.items[1]);
    key('Escape');
    assert.equal(h.doc.activeElement, h.anchor);
    assert.deepEqual(h.writes, [false]);
    h.cleanup();
});

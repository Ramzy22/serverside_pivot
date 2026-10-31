// Source regression only, never installed-artifact/browser qualification.
const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const dependencies = createRequire(path.join(process.env.PIVOT_FRONTEND_TEST_DEPENDENCIES || frontend, 'package.json'));
const babel = dependencies('@babel/core');
const presetEnv = dependencies('@babel/preset-env');
const presetReact = dependencies('@babel/preset-react');

function harness({serverAll = false} = {}) {
    const effects = [], writes = [], cleanups = [], cache = new Map();
    const windowTarget = new EventTarget();
    global.window = windowTarget;
    const hooks = {useCallback: fn => fn, useEffect: fn => effects.push(fn)};
    function load(filename) {
        if (cache.has(filename)) return cache.get(filename);
        const module = {exports: {}};
        cache.set(filename, module.exports);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {
            filename, presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}], presetReact],
        }).code;
        new Function('module', 'exports', 'require', code)(module, module.exports, request => {
            if (request === 'react') return hooks;
            if (request.startsWith('.')) return load(path.resolve(path.dirname(filename), request + '.js'));
            return dependencies(request);
        });
        return module.exports;
    }
    const {useRuntimeCellSelection} = load(path.join(frontend, 'src/lib/runtime/useRuntimeCellSelection.js'));
    const table = {getVisibleLeafColumns: () => [{id:'A'}]};
    const selection = useRuntimeCellSelection({
        table, tableRef:{current:table}, getDisplayRows:() => [], selectedCells:{'0:A':42},
        isSelectAllServerRef:{current:serverAll}, serverSide:serverAll,
        buildSelectedClipboardPayload:() => ({text:'42',html:'<table><tr><td>42</td></tr></table>'}),
        copyRichToClipboard:payload => writes.push({kind:'selection',payload}),
        copyCurrentTableToClipboard:(...args) => writes.push({kind:'table',args}),
    });
    for (const effect of effects) cleanups.push(effect());
    const grid = new EventTarget();
    grid.addEventListener('keydown', selection.handleKeyDown);
    function key({meta=false, editable=null, prevented=false} = {}) {
        const event = new Event('keydown', {cancelable:true, bubbles:true});
        Object.assign(event, {key:'c',ctrlKey:!meta,metaKey:meta});
        if (editable) event.composedPath = () => [{tagName:editable}, grid, windowTarget];
        if (prevented) event.preventDefault();
        return event;
    }
    return {writes,grid,windowTarget,key,cleanup:() => cleanups.forEach(fn => fn?.())};
}

for (const meta of [false,true]) for (const serverAll of [false,true]) {
    test(`${meta?'Cmd':'Ctrl'}+C ${serverAll?'server select-all':'selected range'} has one write per distinct bubbling event`, () => {
        const h = harness({serverAll});
        try {
            // The same cancelable Node Event reaches the actual grid callback
            // and actual window listener, in DOM propagation order.
            for (let count=1;count<=2;count++) {
                const event = h.key({meta});
                h.grid.dispatchEvent(event);
                assert.equal(event.defaultPrevented,true);
                h.windowTarget.dispatchEvent(event);
                assert.equal(h.writes.length,count,'One keypress was copied by both grid and global listeners');
            }
            assert.equal(h.writes[0].kind,serverAll?'table':'selection');
        } finally {h.cleanup();}
    });
}

test('unhandled window shortcut still copies and another control can own default prevention', () => {
    const h = harness();
    try {
        h.windowTarget.dispatchEvent(h.key());
        assert.equal(h.writes.length,1);
        h.windowTarget.dispatchEvent(h.key({prevented:true}));
        assert.equal(h.writes.length,1);
    } finally {h.cleanup();}
});

for (const editable of ['INPUT','TEXTAREA','SELECT']) {
    test(`${editable} native shortcut remains unprevented and never invokes grid copying`, () => {
        const h = harness();
        try {
            const event=h.key({editable});
            h.grid.dispatchEvent(event);
            h.windowTarget.dispatchEvent(event);
            assert.equal(event.defaultPrevented,false);
            assert.deepEqual(h.writes,[]);
        } finally {h.cleanup();}
    });
}

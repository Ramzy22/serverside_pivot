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
    const writes = [], cache = new Map(), slots = [];
    const previousWindow = Object.getOwnPropertyDescriptor(global, 'window');
    let cursor = 0, pending = [], dirty = false, selection;
    const sameDeps = (a, b) => a && b && a.length === b.length && a.every((value, i) => Object.is(value, b[i]));
    const windowTarget = new EventTarget();
    global.window = windowTarget;
    // One shared hook cursor models React's ordered callback/effect slots.
    // Commit changed effects only, running their previous cleanup first.
    const hooks = {
        useCallback(fn, deps) {
            const index = cursor++, previous = slots[index];
            if (previous && sameDeps(previous.deps, deps)) return previous.fn;
            slots[index] = {fn, deps};
            return fn;
        },
        useEffect(fn, deps) {
            const index = cursor++, previous = slots[index];
            if (!previous || !sameDeps(previous.deps, deps)) {
                pending.push(() => {
                    previous?.cleanup?.();
                    slots[index] = {deps, cleanup: fn()};
                });
            }
        },
    };
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
    const row = {id:'0', original:{__virtualIndex:0}, getValue:() => 42};
    const table = {getVisibleLeafColumns:() => [{id:'A'}], getRow:() => row};
    const state = {
        selectedCells:{'stale:A':99}, lastSelected:{rowId:'stale'}, dragStart:{rowId:'stale'},
        isDragging:true, isFilling:true, fillRange:{rStart:99}, hoveredRowPath:'stale',
    };
    const ports = {};
    for (const name of Object.keys(state)) {
        ports[`set${name[0].toUpperCase()}${name.slice(1)}`] = value => {
            const next = typeof value === 'function' ? value(state[name]) : value;
            if (!Object.is(next, state[name])) { state[name] = next; dirty = true; }
        };
    }
    const payload = {text:'42',html:'<table><tr><td>42</td></tr></table>'};
    const isSelectAllServerRef = {current:true};
    const props = {
        ...ports, table, tableRef:{current:table}, getDisplayRows:() => [row],
        isSelectAllServerRef, serverSide:serverAll, selectionScopeKey:'ownership-scope',
        buildSelectedClipboardPayload:(includeHeaders, cells) => {
            assert.equal(includeHeaders, false);
            assert.deepEqual(cells, {'0:A':42});
            return payload;
        },
        copyRichToClipboard:payload => writes.push({kind:'selection',payload}),
        copyCurrentTableToClipboard:(...args) => writes.push({kind:'table',args}),
    };
    const grid = new EventTarget();
    let gridListener;
    function render() {
        do {
            dirty = false;
            cursor = 0;
            pending = [];
            selection = useRuntimeCellSelection({...props, ...state});
            if (gridListener) grid.removeEventListener('keydown', gridListener);
            gridListener = selection.handleKeyDown;
            grid.addEventListener('keydown', gridListener);
            for (const commit of pending) commit();
        } while (dirty);
    }
    render();
    // Execute the real scope effect, then render its cleared state before
    // making any selection. A pre-mount stale closure must never provide it.
    assert.deepEqual(state.selectedCells, {});
    assert.equal(state.lastSelected, null);
    assert.equal(state.dragStart, null);
    assert.equal(state.isDragging, false);
    assert.equal(state.isFilling, false);
    assert.equal(state.fillRange, null);
    assert.equal(isSelectAllServerRef.current, false);
    if (serverAll) {
        const selectAll = new Event('keydown', {cancelable:true});
        Object.assign(selectAll, {key:'a', ctrlKey:true});
        grid.dispatchEvent(selectAll);
        assert.equal(selectAll.defaultPrevented, true);
    } else {
        selection.handleCellMouseDown({button:0}, 0, 0, '0', 'A', 42);
    }
    render();
    assert.deepEqual(state.selectedCells, {'0:A':42});
    assert.equal(isSelectAllServerRef.current, serverAll);
    const selected = state.selectedCells;
    render(); // Same scope must preserve the actual post-mount interaction.
    assert.equal(state.selectedCells, selected);
    assert.equal(isSelectAllServerRef.current, serverAll);
    function key({meta=false, editable=null, prevented=false} = {}) {
        const event = new Event('keydown', {cancelable:true, bubbles:true});
        Object.assign(event, {key:'c',ctrlKey:!meta,metaKey:meta});
        if (editable) event.composedPath = () => [{tagName:editable}, grid, windowTarget];
        if (prevented) event.preventDefault();
        return event;
    }
    return {writes,grid,windowTarget,key,payload,cleanup:() => {
        grid.removeEventListener('keydown', gridListener);
        for (const slot of slots) slot?.cleanup?.();
        if (previousWindow) Object.defineProperty(global, 'window', previousWindow);
        else delete global.window;
    }};
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
            assert.deepEqual(h.writes, Array.from({length:2}, () => serverAll
                ? {kind:'table',args:[true,false]} : {kind:'selection',payload:h.payload}));
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

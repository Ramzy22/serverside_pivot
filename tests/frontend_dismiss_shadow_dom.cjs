// useDismiss treats a click inside a shadow root (Streamlit) as inside its overlay, not outside.
const assert = require('assert');
const path = require('path');
const { buildSync } = require(path.join(__dirname, '..', 'serverside_pivot', 'node_modules', 'esbuild'));

const effects = [];
const react = {
    useRef: (initial) => ({ current: initial }),
    useMemo: (fn) => fn(),
    useEffect: (effect) => { effects.push(effect); },
};
react.default = react;
const out = buildSync({
    entryPoints: [path.join(__dirname, '..', 'serverside_pivot', 'src', 'lib', 'hooks', 'useDismiss.js')],
    bundle: true, write: false, format: 'cjs', platform: 'node', external: ['react', 'react-dom'],
    loader: { '.js': 'jsx' },
});
const mod = { exports: {} };
new Function('module', 'exports', 'require', out.outputFiles[0].text)(mod, mod.exports, (name) => {
    if (name === 'react') return react;
    if (name === 'react-dom') return { createPortal: (x) => x };
    return require(name);
});

// A light DOM: the shadow host is in the document; the menu lives in its shadow root.
const node = (name, parent = null) => ({
    name, parent, nodeType: 1, isConnected: true, focus() {},
    contains(other) {
        // Like the DOM: a non-Node argument (window) throws.
        if (!other || typeof other.nodeType !== 'number') throw new TypeError("parameter 1 is not of type 'Node'");
        for (let n = other; n; n = n.parent) if (n === this) return true;
        return false;
    },
});
const windowLike = { name: 'window' };
const host = node('streamlit-host');
const menu = node('menu');          // inside the shadow root: host.contains(menu) is false
const item = node('csv-item', menu);
const elsewhere = node('page-body');
const listeners = {};
global.document = {
    nodeType: 9,
    activeElement: host, body: elsewhere,
    addEventListener: (type, fn) => { listeners[type] = fn; },
    removeEventListener: (type) => { delete listeners[type]; },
};

let closed = 0;
mod.exports.useDismiss({ open: true, onClose: () => { closed += 1; }, refs: [{ current: menu }] });
effects.splice(0).forEach((effect) => effect());

// Document listeners see the shadow host as target; the composed path still holds the real nodes.
listeners.mousedown({ target: host, composedPath: () => [item, menu, host, global.document, windowLike] });
assert.strictEqual(closed, 0, 'a click on a menu item inside a shadow root keeps the menu open');

listeners.mousedown({ target: elsewhere, composedPath: () => [elsewhere, global.document, windowLike] });
assert.strictEqual(closed, 1, 'a click outside still dismisses');

delete global.document;
console.log('useDismiss sees through shadow-root retargeting');

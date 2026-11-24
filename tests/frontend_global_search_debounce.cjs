// The global search box commits one filter (one data request) per typing pause, not per keystroke.
const assert = require('assert');
const path = require('path');
const { buildSync } = require(path.join(__dirname, '..', 'serverside_pivot', 'node_modules', 'esbuild'));

// A minimal hook runtime standing in for React: enough to drive one function component.
function hookRuntime() {
    const slots = [];
    let index = 0;
    const pending = [];
    const react = {
        useState(initial) {
            const i = index++;
            if (!(i in slots)) slots[i] = initial;
            return [slots[i], (next) => { slots[i] = typeof next === 'function' ? next(slots[i]) : next; }];
        },
        useRef(initial) {
            const i = index++;
            if (!(i in slots)) slots[i] = { current: initial };
            return slots[i];
        },
        useEffect(effect, deps) {
            const i = index++;
            const previous = slots[i];
            if (!previous || !deps || deps.some((d, k) => d !== previous.deps[k])) {
                pending.push(() => {
                    if (previous && previous.cleanup) previous.cleanup();
                    slots[i] = { deps, cleanup: effect() || null };
                });
            }
        },
        createElement: (type, props) => ({ type, props }),
    };
    react.default = react;
    return {
        react,
        render(component, props) {
            index = 0;
            const element = component(props);
            pending.splice(0).forEach((run) => run());
            return element;
        },
    };
}

const runtime = hookRuntime();
const out = buildSync({
    entryPoints: [path.join(__dirname, '..', 'serverside_pivot', 'src', 'lib', 'components', 'GlobalSearchInput.js')],
    bundle: true, write: false, format: 'cjs', platform: 'node', external: ['react'], jsx: 'transform',
    loader: { '.js': 'jsx' },
});
const mod = { exports: {} };
new Function('module', 'exports', 'require', out.outputFiles[0].text)(mod, mod.exports, (name) => {
    if (name === 'react') return runtime.react;
    return require(name);
});
const { GlobalSearchInput, GLOBAL_SEARCH_COMMIT_DELAY_MS } = mod.exports;

const realTimers = { setTimeout: global.setTimeout, clearTimeout: global.clearTimeout };
let now = 0;
let timers = [];
global.setTimeout = (fn, ms) => { const t = { fn, at: now + ms }; timers.push(t); return t; };
global.clearTimeout = (t) => { timers = timers.filter((x) => x !== t); };
const advance = (ms) => {
    now += ms;
    const due = timers.filter((t) => t.at <= now);
    timers = timers.filter((t) => t.at > now);
    due.forEach((t) => t.fn());
};

const commits = [];
let value = '';
const props = () => ({ value, onCommit: (v) => { commits.push(v); value = v; } });
let element = runtime.render(GlobalSearchInput, props());
const type = (text) => {
    element.props.onChange({ target: { value: text } });
    element = runtime.render(GlobalSearchInput, props());
};

for (const text of ['A', 'AP', 'APA', 'APAC']) { type(text); advance(40); }
assert.deepStrictEqual(commits, [], 'nothing commits while typing continues');
assert.strictEqual(element.props.value, 'APAC', 'the box shows every keystroke at once');
advance(GLOBAL_SEARCH_COMMIT_DELAY_MS);
element = runtime.render(GlobalSearchInput, props());
assert.deepStrictEqual(commits, ['APAC'], 'one commit per typing pause');

type('APAC 2');
element = runtime.render(GlobalSearchInput, props());
assert.strictEqual(element.props.value, 'APAC 2', 'our own earlier commit echoing back keeps the newer draft');
advance(GLOBAL_SEARCH_COMMIT_DELAY_MS);
assert.deepStrictEqual(commits, ['APAC', 'APAC 2']);

type('');
assert.deepStrictEqual(commits, ['APAC', 'APAC 2', ''], 'clearing commits at once');

type('EM');
value = 'saved-view';  // a value set elsewhere (saved view, reset) replaces the draft and drops the pending commit
runtime.render(GlobalSearchInput, props());  // the effect sees the new value and resets the draft
element = runtime.render(GlobalSearchInput, props());  // React re-renders after that state update
assert.strictEqual(element.props.value, 'saved-view');
advance(GLOBAL_SEARCH_COMMIT_DELAY_MS * 2);
assert.deepStrictEqual(commits, ['APAC', 'APAC 2', ''], 'a superseded draft never commits');

Object.assign(global, realTimers);
console.log('global search commits once per typing pause');

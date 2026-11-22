// One canonical row identity (serverside_pivot/src/lib/runtime/rowIdentity.ts) in the live store.
const assert = require('assert');
const path = require('path');
const { buildSync } = require(path.join(__dirname, '..', 'serverside_pivot', 'node_modules', 'esbuild'));
const load = (rel) => {
    const src = path.join(__dirname, '..', 'serverside_pivot', 'src', 'lib', 'runtime', rel);
    const out = buildSync({ entryPoints: [src], bundle: true, write: false, format: 'cjs', platform: 'node', external: ['react'] });
    const mod = { exports: {} };
    new Function('module', 'exports', 'require', out.outputFiles[0].text)(mod, mod.exports, require);
    return mod.exports;
};
const { rowIdentity, ambiguousDisplayKeys } = load('rowIdentity.ts');
const { ViewportSubscriptionStore } = load('useViewportSubscriptionStore.ts');

const key = (...vals) => JSON.stringify(vals.map((v) => JSON.stringify(
    v === null ? { type: 'none', value: null, version: 1 } : { type: 'str', value: v, version: 1 })));
const NULL_ROW = key('A', null), EMPTY_ROW = key('A', ''), X_ROW = key('X');

assert.strictEqual(rowIdentity({ _rowKey: EMPTY_ROW, _path: '' }), EMPTY_ROW);
assert.strictEqual(rowIdentity({ _canonicalPath: NULL_ROW, _path: '' }), NULL_ROW);
assert.strictEqual(rowIdentity({ _path: 'legacy' }), 'legacy', 'rows without a canonical key keep their display path');
assert.deepStrictEqual([...ambiguousDisplayKeys([{ _path: '', _rowKey: NULL_ROW }, { _path: '', _rowKey: EMPTY_ROW },
    { _path: 'X', _rowKey: X_ROW }])], ['']);

// Children NULL and "" of A share the display path "A|||": live values must stay on their own rows.
const store = new ViewportSubscriptionStore();
store.ingestFrame({ type: 'SNAPSHOT', seq: 1, rows: [
    { _path: 'A|||', _canonicalPath: NULL_ROW, v_sum: 1 },
    { _path: 'A|||', _canonicalPath: EMPTY_ROW, v_sum: 2 },
    { _path: 'X', _canonicalPath: X_ROW, v_sum: 3 },
] });
assert.strictEqual(store.getCellMeta(NULL_ROW, 'v_sum').value, 1);
assert.strictEqual(store.getCellMeta(EMPTY_ROW, 'v_sum').value, 2);
assert.strictEqual(store.getCellMeta('A|||', 'v_sum'), null, 'an ambiguous display path identifies no row');
assert.strictEqual(store.getCellMeta('X', 'v_sum').value, 3, 'an unambiguous display alias still resolves');

console.log('row identity is canonical and display aliases never collide');

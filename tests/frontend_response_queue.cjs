// Ordered delivery of runtime responses (serverside_pivot/src/lib/runtime/responseQueue.ts).
const assert = require('assert');
const path = require('path');
const { buildSync } = require(path.join(__dirname, '..', 'serverside_pivot', 'node_modules', 'esbuild'));
const src = path.join(__dirname, '..', 'serverside_pivot', 'src', 'lib', 'runtime', 'responseQueue.ts');
const out = buildSync({ entryPoints: [src], bundle: true, write: false, format: 'cjs', platform: 'node', external: ['react'] });
const mod = { exports: {} };
new Function('module', 'exports', 'require', out.outputFiles[0].text)(mod, mod.exports, require);
const { EMPTY_RESPONSE_QUEUE, enqueueResponse, advanceResponse } = mod.exports;

const data = { kind: 'data', requestId: 'd1' };
const options = { kind: 'filter_options', requestId: 'f1' };
const detail = { kind: 'detail', requestId: 'x1' };

// Two responses in one tick: both are delivered, in arrival order (the single slot kept only the last).
let s = enqueueResponse(enqueueResponse(EMPTY_RESPONSE_QUEUE, options), data);
const seen = [s.head];
for (s = advanceResponse(s); s.head !== seen[seen.length - 1]; s = advanceResponse(s)) seen.push(s.head);
assert.deepStrictEqual(seen, [options, data]);

// Three, one later: order kept; the delivered head stays until something new arrives.
s = enqueueResponse(enqueueResponse(enqueueResponse(EMPTY_RESPONSE_QUEUE, data), options), detail);
assert.strictEqual(s.head, data);
s = advanceResponse(s); assert.strictEqual(s.head, options);
s = advanceResponse(s); assert.strictEqual(s.head, detail);
assert.strictEqual(advanceResponse(s), s, 'nothing pending: no re-render');

// Re-delivering the response already shown is a no-op; null is ignored.
assert.strictEqual(enqueueResponse(s, detail), s);
assert.strictEqual(enqueueResponse(s, null), s);

console.log('response queue delivers every response in order');

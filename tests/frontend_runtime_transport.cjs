const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');

const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const localRequire = createRequire(path.join(frontend, 'package.json'));
const babel = localRequire('@babel/core');
const presets = [
    [localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
    localRequire('@babel/preset-typescript'),
    localRequire('@babel/preset-react'),
];

// React's effect scheduler is replaced so the real response effect can be
// exercised without a DOM. Transport, codec, freshness checks and commits run
// from their actual source modules.
function loadRuntime(react = {}) {
    const cache = new Map();
    function load(filename) {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => {
            if (specifier === 'react') return react;
            // Workspace composition is outside this boundary test; inspect the
            // actual props emitted by the real workspace into its runtime root.
            if (specifier === '../runtime/PivotRuntimeRoot') return {PivotRuntimeRoot: function Root() {}};
            return specifier.startsWith('.') ? load(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    }
    return relative => load(path.join(frontend, 'src/lib', relative));
}

const request = {schemaVersion: 1, kind: 'data', requestId: 'window-1', payload: {intent: 'viewport'}};
const exhausted = {
    schemaVersion: 1, kind: 'data', status: 'error', requestId: 'window-1',
    payload: {code: 'resource_exhausted', resourceKind: 'query', retryAfterMs: 1501},
};
const jsonResponse = (body, status) => new Response(JSON.stringify(body), {
    status, headers: {'Content-Type': 'application/json'},
});

test('HTTP preserves V1 structured non-2xx errors for the shared consumer', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const transport = new HttpPivotTransport({fetchImpl: async () => jsonResponse(exhausted, 503)});
    assert.deepEqual(await transport.request(request), exhausted);
});

test('HTTP rejects malformed or non-V1 failures with safe errors', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    for (const response of [
        jsonResponse({detail: 'SECRET backend failure'}, 503),
        jsonResponse({...exhausted, schemaVersion: 2, message: 'SECRET'}, 503),
        jsonResponse({...exhausted, status: 'data', message: 'SECRET'}, 503),
        new Response('SECRET <html>failure</html>', {status: 502, headers: {'Content-Type': 'text/html'}}),
        new Response('SECRET not JSON', {status: 503, headers: {'Content-Type': 'application/json'}}),
    ]) {
        const transport = new HttpPivotTransport({fetchImpl: async () => response});
        await assert.rejects(transport.request(request), error => {
            assert.equal(error.name, 'Error');
            assert.match(error.message, /Runtime request failed with status (502|503)\./);
            assert.doesNotMatch(error.message, /SECRET/);
            return true;
        });
    }
});

test('HTTP still validates successful V1 envelopes and resolves payload references', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const transport = new HttpPivotTransport({endpoint: 'https://runtime.example/v1/runtime', fetchImpl: async () => jsonResponse({
        ...exhausted, status: 'data', payload: {payloadRef: {id: 'blob', url: '/v1/runtime/payload/blob'}},
    }, 200)});
    assert.equal((await transport.request(request)).payload.payloadRef.url, 'https://runtime.example/v1/runtime/payload/blob');
    await assert.rejects(new HttpPivotTransport({fetchImpl: async () => jsonResponse({}, 200)}).request(request), /invalid V1/);
});

test('HTTP body cancellation remains AbortError instead of becoming a runtime failure', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const cancellation = new DOMException('Request superseded', 'AbortError');
    const stream = new ReadableStream({start(controller) {controller.error(cancellation);}});
    const transport = new HttpPivotTransport({fetchImpl: async () => new Response(stream, {
        status: 503, headers: {'Content-Type': 'application/json'},
    })});
    await assert.rejects(transport.request(request), error => error === cancellation);
});

test('workspace passes the configured payload transport privately to the runtime root', () => {
    const react = {
        useState: initializer => [initializer(), () => {}], useRef: current => ({current}),
        useEffect: () => {}, useCallback: fn => fn,
        createElement: (type, props) => ({type, props}),
    };
    const {PivotWorkspace} = loadRuntime(react)('components/PivotWorkspace.tsx');
    const transport = {request: async () => exhausted, fetchPayload: async () => ({data: []})};
    const root = PivotWorkspace({transport, table: 'sales'});
    assert.equal(root.props.payloadTransport, transport);
});

function lifecycleHarness(payloadTransport, overrides = {}) {
    let effect;
    const load = loadRuntime({useEffect: fn => {effect = fn;}, useCallback: fn => fn});
    const {useRuntimeResponseLifecycle} = load('runtime/useRuntimeResponseLifecycle.js');
    const {useRuntimeDataCommit} = load('hooks/useRuntimeDataCommit.js');
    const committed = [];
    const resolved = [];
    const notifications = [];
    let refreshes = 0;
    const noop = () => {};
    const state = {
        clientInstanceRef: {current: 'tab-a'}, stateEpochRef: {current: 3}, latestDataVersionRef: {current: 7},
        pendingDataProfilerCommitRef: {current: null}, finalizeTransactionHistoryResponse: noop,
        hydrateVisibleEditOverlay: noop, reconcileOptimisticCellValuesWithPayload: noop,
        setFormulaErrors: noop, setTransportDataState: update => committed.push(update({})),
    };
    const commit = useRuntimeDataCommit(state);
    const args = {
        ...state, ...commit, payloadTransport,
        activeRuntimePayloadFetchRef: {current: null}, handledRuntimeResponseKeysRef: {current: new Set()},
        recordProfilerResponse: noop, resolvePendingRequest: response => resolved.push(response.requestId),
        showNotification: (...args) => notifications.push(args), scheduleSilentViewportRefresh: () => {refreshes++;},
        runtimeResponse: {
            ...exhausted, status: 'data', clientInstance: 'tab-a', stateEpoch: 3,
            payload: {data: [], dataVersion: 7, payloadRef: {id: 'blob', url: '/v1/runtime/payload/blob', requestId: 'window-1'}},
        },
        ...overrides,
    };
    useRuntimeResponseLifecycle(args);
    const cleanup = effect();
    return {args, state, committed, resolved, notifications, cleanup, get refreshes() {return refreshes;}};
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('externalized data fetch uses configured headers, origin, credentials and cleanup abort', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    let seen;
    const transport = new HttpPivotTransport({endpoint: 'https://runtime.example/v1/runtime', headers: {Authorization: 'Bearer test'},
        fetchImpl: async (url, options) => {
            seen = {url, options};
            return jsonResponse({data: [{id: 'row-a', amount: 12}], dataVersion: 7, rowCount: 1}, 200);
        },
    });
    const run = lifecycleHarness(transport);
    await settle();
    assert.equal(run.committed.length, 1);
    assert.equal(run.committed[0].data[0].amount, 12);
    assert.equal(seen.url, 'https://runtime.example/v1/runtime/payload/blob');
    assert.equal(seen.options.headers.Authorization, 'Bearer test');
    assert.equal(seen.options.credentials, 'same-origin');
    assert.equal(seen.options.signal.aborted, false);
    run.cleanup();
    assert.equal(seen.options.signal.aborted, true);
    assert.equal(run.args.activeRuntimePayloadFetchRef.current, null);
});

test('Dash retained data uses the explicit same-origin HTTP transport by default', async () => {
    const originalFetch = global.fetch;
    let seen;
    global.fetch = async (url, options) => {
        seen = {url, options};
        return jsonResponse({data: [{id: 'dash-row', amount: 23}], dataVersion: 7, rowCount: 1}, 200);
    };
    try {
        const run = lifecycleHarness(undefined);
        await settle();
        assert.equal(run.committed[0].data[0].amount, 23);
        assert.equal(seen.url, '/v1/runtime/payload/blob');
        assert.equal(seen.options.credentials, 'same-origin');
        assert.equal(seen.options.headers.Accept, 'application/json');
        run.cleanup();
        assert.equal(seen.options.signal.aborted, true);
    } finally {global.fetch = originalFetch;}
});

test('payload resolution preserves current client, epoch, version and active-fetch guards', async () => {
    for (const stale of ['client', 'epoch', 'version', 'request', 'active', 'cleanup']) {
        let release;
        const run = lifecycleHarness({fetchPayload: () => new Promise(resolve => {release = resolve;})});
        if (stale === 'client') run.state.clientInstanceRef.current = 'tab-b';
        if (stale === 'epoch') run.state.stateEpochRef.current = 4;
        if (stale === 'version') run.state.latestDataVersionRef.current = 8;
        if (stale === 'request') run.args.runtimeResponse.payload.payloadRef.requestId = 'different';
        if (stale === 'active') run.args.activeRuntimePayloadFetchRef.current = {token: 'new-blob'};
        if (stale === 'cleanup') run.cleanup();
        assert.equal(typeof release, 'function', 'configured fetchPayload must run');
        release({data: [{id: 'old-row'}], dataVersion: 7, rowCount: 1});
        await settle();
        assert.equal(run.committed.length, 0, `${stale} payload must not commit`);
        if (!['active', 'cleanup'].includes(stale)) assert.deepEqual(run.resolved, ['window-1']);
        run.cleanup();
    }
});

test('configured payload failures never fall back to bare fetch and abort is silent', async () => {
    const originalFetch = global.fetch;
    let bareCalls = 0;
    global.fetch = async () => {bareCalls++; throw new Error('bare fetch forbidden');};
    const originalError = console.error;
    console.error = () => {};
    try {
        for (const aborted of [false, true]) {
            const error = new Error('unavailable');
            if (aborted) error.name = 'AbortError';
            const run = lifecycleHarness({fetchPayload: async () => {throw error;}});
            await settle();
            assert.equal(run.committed.length, 0);
            assert.equal(run.notifications.length, aborted ? 0 : 1);
            assert.equal(run.refreshes, aborted ? 0 : 1);
            assert.deepEqual(run.resolved, aborted ? [] : ['window-1']);
            run.cleanup();
        }
        assert.equal(bareCalls, 0);
    } finally {global.fetch = originalFetch; console.error = originalError;}
});

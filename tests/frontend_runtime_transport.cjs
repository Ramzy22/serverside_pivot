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

test('HTTP binds missing freshness metadata to its originating request', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const origin = {...request, clientInstance: 'browser-tab-a', stateEpoch: 3, windowSeq: 7};
    for (const [status, body] of [[503, exhausted], [403, exhausted],
        [200, {...exhausted, status: 'data'}], [200, {...exhausted, status: 'stale'}]]) {
        const transport = new HttpPivotTransport({fetchImpl: async () => jsonResponse(body, status)});
        assert.deepEqual(await transport.request(origin), {
            ...body, clientInstance: origin.clientInstance, stateEpoch: 3, windowSeq: 7,
        });
    }
    const {requestId, ...unkeyed} = exhausted;
    const transport = new HttpPivotTransport({fetchImpl: async () => jsonResponse(unkeyed, 503)});
    assert.equal((await transport.request(origin)).requestId, origin.requestId);
    const explicit = {...exhausted, clientInstance: 'other-client', stateEpoch: 0, windowSeq: 0};
    const foreign = new HttpPivotTransport({fetchImpl: async () => jsonResponse(explicit, 503)});
    assert.deepEqual(await foreign.request(origin), explicit, 'explicit server identity must remain available for rejection');
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

test('HTTP rejects malformed optional V1 error metadata with a safe status error', async t => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    for (const [field, value] of [
        ['requestId', {secret: 'SECRET'}], ['message', {secret: 'SECRET'}],
        ['stateEpoch', -1], ['windowSeq', -1], ['stateEpoch', 1.5], ['windowSeq', 1.5],
        ['table', {secret: 'SECRET'}], ['sessionId', []], ['clientInstance', 42],
        ['requestId', ''], ['table', ''], ['sessionId', ''], ['clientInstance', ''],
        ['profile', []], ['profile', null], ['unexpectedField', 'SECRET'],
    ]) {
        await t.test(`${field}=${JSON.stringify(value)}`, async () => {
            const transport = new HttpPivotTransport({fetchImpl: async () => jsonResponse({...exhausted, [field]: value}, 503)});
            await assert.rejects(transport.request(request), error => {
                assert.equal(error.message, 'Runtime request failed with status 503.');
                return true;
            });
        });
    }
});

test('HTTP preserves valid optional error metadata including zero sequence and empty message', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const response = {...exhausted, table: 'sales', sessionId: 'principal:alice', clientInstance: 'tab-a',
        stateEpoch: 0, windowSeq: 0, message: '', profile: {query: {totalMs: 1}},
    };
    const transport = new HttpPivotTransport({fetchImpl: async () => jsonResponse(response, 503)});
    assert.deepEqual(await transport.request(request), response);
});

test('HTTP still validates successful V1 envelopes and resolves payload references', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const transport = new HttpPivotTransport({endpoint: 'https://runtime.example/v1/runtime', fetchImpl: async () => jsonResponse({
        ...exhausted, status: 'data', payload: {payloadRef: {id: 'blob', url: '/v1/runtime/payload/blob'}},
    }, 200)});
    assert.equal((await transport.request(request)).payload.payloadRef.url, 'https://runtime.example/v1/runtime/payload/blob');
    await assert.rejects(new HttpPivotTransport({fetchImpl: async () => jsonResponse({}, 200)}).request(request), /invalid V1/);
});

test('HTTP cross-origin payload references resolve against the endpoint through actual delivery', async () => {
    const {HttpPivotTransport} = loadRuntime()('runtime/httpTransport.ts');
    const previousWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
    Object.defineProperty(globalThis, 'window', {configurable: true, value: {
        location: {href: 'https://embedded.example/app/frame'},
    }});
    try {
        // Literal expectations distinguish root-relative, endpoint-relative,
        // and absolute references from the embedding page's origin/path.
        for (const [url, expected] of [
            ['/v1/runtime/payload/blob', 'https://runtime.example/v1/runtime/payload/blob'],
            ['payload/blob?token=retained', 'https://runtime.example/gateway/payload/blob?token=retained'],
            ['https://cdn.example/blob', 'https://cdn.example/blob'],
        ]) {
            const calls = [];
            const controller = new AbortController();
            const origin = {...request, clientInstance: 'embedded-tab', stateEpoch: 3, windowSeq: 7};
            const reference = {id: 'blob', url, bytes: 128, contentType: 'application/json'};
            const body = {schemaVersion: 1, kind: 'data', status: 'data', payload: {payloadRef: reference}};
            const payload = {data: [{id: 'cross-origin-row', amount: 12}], rowCount: 1, dataVersion: 7};
            const transport = new HttpPivotTransport({
                endpoint: 'https://runtime.example/gateway/runtime', headers: {Authorization: 'Bearer test'},
                fetchImpl: async (target, options) => {
                    calls.push({target, options});
                    return jsonResponse(options.method === 'POST' ? body : payload, 200);
                },
            });
            const response = await transport.request(origin, {signal: controller.signal});
            assert.deepEqual(response, {...body, requestId: 'window-1', clientInstance: 'embedded-tab',
                stateEpoch: 3, windowSeq: 7, payload: {payloadRef: {...reference, url: expected}}});
            assert.deepEqual(await transport.fetchPayload(response.payload.payloadRef, {signal: controller.signal}), payload);
            // Also accept the original wire reference: callers may retain it
            // independently of the normalized response returned by request().
            assert.deepEqual(await transport.fetchPayload(reference, {signal: controller.signal}), payload);
            assert.deepEqual(calls.map(call => [call.options.method, call.target]), [
                ['POST', 'https://runtime.example/gateway/runtime'], ['GET', expected], ['GET', expected],
            ]);
            assert.deepEqual(JSON.parse(calls[0].options.body), origin);
            for (const {options} of calls) {
                assert.equal(options.signal, controller.signal);
                assert.equal(options.credentials, 'same-origin');
                assert.equal(options.headers.Authorization, 'Bearer test');
            }
            controller.abort();
            assert.ok(calls.every(call => call.options.signal.aborted), 'caller retains cancellation of both phases');
        }
    } finally {
        if (previousWindow) Object.defineProperty(globalThis, 'window', previousWindow);
        else delete globalThis.window;
    }
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
    const handledUnkeyedDataErrorsRef = {current: new WeakSet()};
    let effect;
    let dependencies;
    let effectPending = false;
    let cleanup;
    const load = loadRuntime({
        useEffect: (fn, nextDependencies) => {
            if (!dependencies || nextDependencies.some((value, index) => !Object.is(value, dependencies[index]))) {
                effect = fn;
                dependencies = nextDependencies;
                effectPending = true;
            }
        },
        useCallback: fn => fn,
        useRef: () => handledUnkeyedDataErrorsRef,
    });
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
    function render(nextArgs = {}) {
        Object.assign(args, nextArgs);
        useRuntimeResponseLifecycle(args);
        if (effectPending) {
            if (cleanup) cleanup();
            cleanup = effect();
            effectPending = false;
        }
    }
    render();
    return {args, state, committed, resolved, notifications, render,
        cleanup: () => {if (cleanup) cleanup(); cleanup = undefined;},
        get refreshes() {return refreshes;},
    };
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

test('transport identity changes restart an unfinished retained delivery without stranding it or committing stale rows', async () => {
    let firstSignal;
    let releaseFirst;
    let replacementCalls = 0;
    const firstTransport = {fetchPayload: (_reference, {signal}) => {
        firstSignal = signal;
        // Custom transports may finish despite abort; the stale result must
        // still be excluded after the replacement fetch starts.
        return new Promise(resolve => {
            releaseFirst = resolve;
        });
    }};
    const nextTransport = {fetchPayload: async () => {
        replacementCalls++;
        return {data: [{id: 'second-row', amount: 24}], dataVersion: 7, rowCount: 1};
    }};
    const run = lifecycleHarness(firstTransport);
    run.render({payloadTransport: nextTransport});
    await settle();
    assert.equal(firstSignal.aborted, true);
    assert.equal(replacementCalls, 1, 'an interrupted delivery must restart with the current transport');
    assert.equal(run.committed.length, 1);
    assert.equal(run.committed[0].data[0].amount, 24);
    releaseFirst({data: [{id: 'first-row', amount: 12}], dataVersion: 7, rowCount: 1});
    await settle();
    assert.equal(run.committed.length, 1);
    assert.equal(run.committed[0].data[0].amount, 24);
    run.render({showNotification: () => {}});
    await settle();
    assert.equal(replacementCalls, 1, 'completed delivery must not replay on callback changes');
    run.render({runtimeResponse: {...run.args.runtimeResponse, requestId: 'window-2', payload: {
        ...run.args.runtimeResponse.payload, payloadRef: {id: 'blob-2', url: '/v1/runtime/payload/blob-2', requestId: 'window-2'},
    }}});
    await settle();
    assert.equal(firstSignal.aborted, true, 'a new response still cleans up the previous delivery');
    assert.equal(replacementCalls, 2);
    assert.equal(run.committed.length, 2);
    assert.equal(run.committed[1].data[0].amount, 24);
    assert.equal(run.notifications.length, 0);
    run.cleanup();
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

test('workspace aborts all owned dispatches on unmount without disposing a shared transport', async () => {
    const effects = [], updates = [], calls = [];
    let disposed = false;
    const react = {
        useState: initializer => [initializer(), update => updates.push(update)],
        useRef: current => ({current}), useEffect: fn => effects.push(fn), useCallback: fn => fn,
        createElement: (type, props) => ({type, props}),
    };
    const {PivotWorkspace} = loadRuntime(react)('components/PivotWorkspace.tsx');
    const transport = {request: (envelope, options) => {
        calls.push({envelope, options});
        return new Promise((resolve, reject) => options?.signal?.addEventListener('abort', () => reject(new DOMException('Unmounted', 'AbortError')), {once:true}));
    }, dispose: () => {disposed = true;}};
    const root = PivotWorkspace({transport, table:'sales'});
    const cleanups = effects.map(fn => fn()).filter(Boolean);
    root.props.setProps({runtimeRequest:request});
    root.props.setProps({runtimeRequest:{...request, requestId:'window-2'}});
    assert.equal(calls.length, 2);
    for (const call of calls) assert.ok(call.options?.signal instanceof AbortSignal, 'dispatch must own an actual AbortSignal');
    const before = updates.length;
    cleanups.forEach(fn => fn());
    assert.ok(calls.every(call => call.options.signal.aborted));
    await settle();
    assert.equal(updates.length, before, 'abort must not publish a runtime error after cleanup');
    assert.equal(disposed, false, 'caller owns the shared transport');
});

test('scheduler coalescing retains the latest caller signal and releases its abort listener', async () => {
    const {LatestViewportScheduler} = loadRuntime()('runtime/httpTransport.ts');
    const first = new AbortController(), latest = new AbortController();
    let seen, released = 0;
    const remove = latest.signal.removeEventListener.bind(latest.signal);
    latest.signal.removeEventListener = (...args) => {released++; return remove(...args);};
    const scheduler = new LatestViewportScheduler({request:(envelope, options) => {
        seen = {envelope, options};
        return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once:true}));
    }});
    const a = scheduler.request(request, {signal:first.signal}).catch(error => error);
    const b = scheduler.request({...request, requestId:'window-2'}, {signal:latest.signal}).catch(error => error);
    await Promise.resolve();
    first.abort();
    assert.equal(seen.envelope.requestId, 'window-2');
    assert.equal(seen.options.signal.aborted, false, 'superseded caller must not abort latest request');
    latest.abort();
    assert.equal(seen.options.signal.aborted, true, 'latest caller must cancel actual underlying request');
    assert.equal((await a).name, 'AbortError');
    assert.equal((await b).name, 'AbortError');
    assert.equal(released, 1);
});

test('workspace keeps local viewport and chart pane telemetry out of the durable host channel', async () => {
    const published=[];
    const react={useState: initializer=>[initializer(),()=>{}],useRef:current=>({current}),
        useEffect:()=>{},useCallback:fn=>fn,createElement:(type,props)=>({type,props})};
    const {PivotWorkspace}=loadRuntime(react)('components/PivotWorkspace.tsx');
    const root=PivotWorkspace({table:'sales',transport:{request:async()=>exhausted},onStateChange:value=>published.push(value)});
    root.props.setProps({runtimeRequest:request});
    root.props.setProps({chartCanvasPanes:[{id:'pane',width:450}],tableCanvasSize:3});
    assert.deepEqual(published,[], 'scroll/resize must stay local to the consumer');
    root.props.setProps({filters:{global:'real'},runtimeRequest:{...request,requestId:'window-3'}});
    assert.deepEqual(published,[{filters:{global:'real'}}], 'durable model changes publish once without viewport traffic');
    await settle();
});

test('workspace ignores a cancelled dispatch even when effects mount again before an uncooperative transport settles', async () => {
    const effects=[],updates=[];
    let resolve;
    const react={useState:initializer=>[initializer(),update=>updates.push(update)],useRef:current=>({current}),
        useEffect:fn=>effects.push(fn),useCallback:fn=>fn,createElement:(type,props)=>({type,props})};
    const {PivotWorkspace}=loadRuntime(react)('components/PivotWorkspace.tsx');
    const root=PivotWorkspace({table:'sales',transport:{request:()=>new Promise(done=>{resolve=done;})}});
    const cleanup=effects.map(fn=>fn()).filter(Boolean);
    root.props.setProps({runtimeRequest:request});
    cleanup.forEach(fn=>fn());
    // StrictMode can set mountedRef true again while old promises still exist.
    effects.forEach(fn=>fn());
    const before=updates.length;
    resolve({...exhausted,status:'data'});
    await settle();
    assert.equal(updates.length,before,'cancelled generation must not publish after effects remount');
});

const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');

const frontend = path.resolve(__dirname, '../serverside_pivot');
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

test('workspace initial bootstrap survives StrictMode unmount/remount with LatestViewportScheduler and HttpPivotTransport', async () => {
    const RealReact = localRequire('react');
    const noop = () => {};
    const makeScheduler = () => {
        const scopes = new Map();
        let activeScope = null;
        const depsEqual = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
        const getScope = key => {
            if (!scopes.has(key)) scopes.set(key, {slots: [], effects: [], cleanups: []});
            return scopes.get(key);
        };
        const api = {
            ...RealReact,
            createElement: (type, props, ...children) => ({type, props: {...props, children}}),
            useId: () => 'sp-test-id',
            useRef: initial => {
                const i = activeScope.cursor++;
                return activeScope.slots[i] ||= {current: initial};
            },
            useState: initial => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) scope.slots[i] = {value: typeof initial === 'function' ? initial() : initial};
                return [scope.slots[i].value, next => {
                    scope.slots[i].value = typeof next === 'function' ? next(scope.slots[i].value) : next;
                }];
            },
            useReducer: (reducer, initialArg, init) => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) scope.slots[i] = {value: init ? init(initialArg) : initialArg};
                return [scope.slots[i].value, action => {
                    scope.slots[i].value = reducer(scope.slots[i].value, action);
                }];
            },
            useMemo: (fn, deps) => {
                const i = activeScope.cursor++;
                const slot = activeScope.slots[i];
                if (!slot || !depsEqual(slot.deps, deps)) activeScope.slots[i] = {value: fn(), deps};
                return activeScope.slots[i].value;
            },
            useCallback: (fn, deps) => api.useMemo(() => fn, deps),
            useEffect: (fn, deps) => {
                const scope = activeScope;
                const i = scope.cursor++;
                const prev = scope.slots[i];
                const changed = !prev || !depsEqual(prev.deps, deps);
                const record = {index: i, fn, deps, dirty: changed};
                scope.slots[i] = {deps, cleanup: prev?.cleanup};
                scope.allEffects.push(record);
                if (changed) scope.pendingEffects.push(record);
            },
            useLayoutEffect: (fn, deps) => api.useEffect(fn, deps),
            useImperativeHandle: noop,
            useDebugValue: noop,
            readContext: ctx => api.useContext(ctx),
            // React 18 hooks: the real ones resolve the dispatcher, which is this object (infinite recursion).
            useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(),
            useDeferredValue: value => value,
            useTransition: () => [false, fn => fn()],
            useInsertionEffect: (fn, deps) => api.useEffect(fn, deps),
        };
        const dispatcher = RealReact.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED?.ReactCurrentDispatcher;
        return {
            api,
            renderComponent(key, fn) {
                const scope = getScope(key);
                activeScope = scope;
                scope.cursor = 0;
                scope.pendingEffects = [];
                scope.allEffects = [];
                const prevDispatcher = dispatcher ? dispatcher.current : null;
                if (dispatcher) dispatcher.current = api;
                try {
                    return fn();
                } finally {
                    if (dispatcher) dispatcher.current = prevDispatcher;
                    activeScope = null;
                }
            },
            commitMount(key) {
                const scope = getScope(key);
                scope.cleanups = scope.pendingEffects.map(rec => {
                    const prevCleanup = scope.slots[rec.index]?.cleanup;
                    if (typeof prevCleanup === 'function') prevCleanup();
                    const cleanup = rec.fn();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup};
                    return cleanup;
                });
            },
            runCleanups(key) {
                const scope = getScope(key);
                for (const cleanup of scope.cleanups) {
                    if (typeof cleanup === 'function') cleanup();
                }
                scope.cleanups = [];
                for (const slot of scope.slots) {
                    if (slot && typeof slot.cleanup === 'function') slot.cleanup = undefined;
                }
            },
            remountAllEffects(key) {
                const scope = getScope(key);
                scope.cleanups = scope.allEffects.map(rec => {
                    const cleanup = rec.fn();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup};
                    return cleanup;
                });
            },
        };
    };

    const scheduler = makeScheduler();
    const cache = new Map();
    let latestRootProps = null;
    let latestWorkspace = null;
    const loadFull = filename => {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => {
            if (specifier === 'react') return scheduler.api;
            if (specifier === '@tanstack/react-virtual') {
                return {useVirtualizer: () => ({getTotalSize: () => 0, getVirtualItems: () => [], scrollToIndex: noop, measure: noop})};
            }
            if (specifier === '../components/PivotWorkspaceShell') {
                return {PivotWorkspaceShell: ({workspace}) => {latestWorkspace = workspace; return null;}};
            }
            if (specifier.endsWith('.css')) return {};
            return specifier.startsWith('.') ? loadFull(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    };

    const {PivotWorkspace} = loadFull(path.join(frontend, 'src/lib/components/PivotWorkspace.tsx'));
    const {PivotRuntimeRoot} = loadFull(path.join(frontend, 'src/lib/runtime/PivotRuntimeRoot.tsx'));
    const {HttpPivotTransport, LatestViewportScheduler} = loadFull(path.join(frontend, 'src/lib/runtime/httpTransport.ts'));

    const fetchCalls = [];
    const sampleRows = [
        {_id: 'APAC', _path: 'APAC', _depth: 0, _has_children: true, _is_expanded: false, Region: 'APAC', Sales_sum: 100},
        {_id: 'EMEA', _path: 'EMEA', _depth: 0, _has_children: true, _is_expanded: false, Region: 'EMEA', Sales_sum: 200},
        {_id: '__grand_total__', _isTotal: true, Sales_sum: 300},
    ];
    const http = new HttpPivotTransport({
        endpoint: '/v1/runtime',
        fetchImpl: async (url, init) => {
            const envelope = JSON.parse(init.body);
            fetchCalls.push({url, aborted: init.signal?.aborted, envelope});
            return jsonResponse({
                schemaVersion: 1,
                kind: 'data',
                status: 'data',
                requestId: envelope.requestId,
                clientInstance: envelope.clientInstance,
                stateEpoch: envelope.stateEpoch,
                windowSeq: envelope.windowSeq,
                payload: {
                    dataVersion: Math.max(1, Number(envelope.stateEpoch) || 1),
                    stateEpoch: envelope.stateEpoch,
                    data_state_epoch: envelope.stateEpoch,
                    dataOffset: 0,
                    rowCount: 3,
                    totalCenterCols: 2,
                    colStart: 0,
                    colEnd: 1,
                    columns: [
                        {id: 'Region', header: 'Region', field: 'Region', kind: 'hierarchy'},
                        {id: 'Sales_sum', header: 'Sales', field: 'Sales_sum', kind: 'metric'},
                    ],
                    rows: sampleRows,
                    data: sampleRows,
                    colSchema: {total_center_cols: 2, columns: [{id: 'Sales_sum', field: 'Sales_sum', header: 'Sales'}]},
                    grandTotalRow: sampleRows[2],
                },
            }, 200);
        },
    });
    const transport = new LatestViewportScheduler(http);

    const workspaceProps = {
        id: 'strict-test-pivot',
        table: 'flagship_sales',
        transport,
        serverSide: true,
        rowFields: ['Region'],
        colFields: [],
        valConfigs: [{field: 'Sales', agg: 'sum'}],
    };

    const renderTree = () => {
        const rootEl = scheduler.renderComponent('workspace', () => PivotWorkspace(workspaceProps));
        latestRootProps = rootEl.props;
        const shellEl = scheduler.renderComponent('root', () => PivotRuntimeRoot(rootEl.props));
        if (shellEl && shellEl.props && shellEl.props.workspace) {
            latestWorkspace = shellEl.props.workspace;
        }
    };

    const prevWindow = global.window;
    const prevDocument = global.document;
    const prevLocalStorage = global.localStorage;
    const prevRaf = global.requestAnimationFrame;
    const prevCaf = global.cancelAnimationFrame;
    const prevResizeObserver = global.ResizeObserver;
    const fakeStorage = {getItem: () => null, setItem: noop, removeItem: noop};
    global.localStorage = fakeStorage;
    global.requestAnimationFrame = cb => setTimeout(() => cb(Date.now()), 0);
    global.cancelAnimationFrame = id => clearTimeout(id);
    global.ResizeObserver = class ResizeObserver { observe() {} unobserve() {} disconnect() {} };
    global.window = {
        addEventListener: noop,
        removeEventListener: noop,
        requestAnimationFrame: global.requestAnimationFrame,
        cancelAnimationFrame: global.cancelAnimationFrame,
        ResizeObserver: global.ResizeObserver,
        getComputedStyle: () => ({getPropertyValue: () => ''}),
        location: {pathname: '/', search: ''},
        localStorage: fakeStorage,
        matchMedia: () => ({matches: false, addEventListener: noop, removeEventListener: noop, addListener: noop, removeListener: noop}),
    };
    global.document = {
        addEventListener: noop,
        removeEventListener: noop,
        getElementById: () => null,
        querySelector: () => null,
        querySelectorAll: () => [],
        createElement: () => ({
            style: {},
            getContext: () => ({font: '', measureText: text => ({width: String(text || '').length * 8})}),
        }),
        activeElement: null,
        body: {style: {}},
    };
    try {
        // React 18 StrictMode Mount 1 -> Cleanup 1 -> Mount 2 (synchronously before microtasks flush)
        renderTree();
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');
        scheduler.runCleanups('root');
        scheduler.runCleanups('workspace');
        scheduler.remountAllEffects('root');
        scheduler.remountAllEffects('workspace');

        await settle();
        assert.ok(fetchCalls.length >= 1, 'StrictMode remount must still dispatch initial bootstrap POST /v1/runtime');
        assert.equal(fetchCalls[0].aborted, false, 'Mount 2 initial bootstrap request must use a fresh non-aborted signal');

        renderTree();
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');
        await settle();
        renderTree();
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');

        assert.ok(latestWorkspace, 'PivotRuntimeRoot must produce a valid WorkspaceViewModel after StrictMode bootstrap');
        assert.equal(latestWorkspace.canvas.statusRowCount, 2);
        assert.ok(Array.isArray(latestWorkspace.canvas.effectiveCenterRows));
    } finally {
        scheduler.runCleanups('root');
        scheduler.runCleanups('workspace');
        global.window = prevWindow;
        global.document = prevDocument;
        global.localStorage = prevLocalStorage;
        global.requestAnimationFrame = prevRaf;
        global.cancelAnimationFrame = prevCaf;
        global.ResizeObserver = prevResizeObserver;
    }
});

test('workspace fieldPanelSizes updates settle cleanly without echo loop or snap-back under StrictMode', async () => {
    const RealReact = localRequire('react');
    const noop = () => {};
    let stateChanged = false;
    const makeScheduler = () => {
        const scopes = new Map();
        let activeScope = null;
        const depsEqual = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
        const getScope = key => {
            if (!scopes.has(key)) scopes.set(key, {slots: [], effects: [], pendingEffects: [], allEffects: [], cleanups: []});
            return scopes.get(key);
        };
        const api = {
            ...RealReact,
            createElement: (type, props, ...children) => ({type, props: {...props, children}}),
            useId: () => 'sp-resize-test-id',
            useRef: initial => {
                const i = activeScope.cursor++;
                return activeScope.slots[i] ||= {current: initial};
            },
            useState: initial => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) {
                    const slot = {
                        value: typeof initial === 'function' ? initial() : initial,
                        set: next => {
                            const prevVal = slot.value;
                            const nextVal = typeof next === 'function' ? next(prevVal) : next;
                            if (!Object.is(prevVal, nextVal)) {
                                slot.value = nextVal;
                                stateChanged = true;
                            }
                        },
                    };
                    scope.slots[i] = slot;
                }
                return [scope.slots[i].value, scope.slots[i].set];
            },
            useReducer: (reducer, initialArg, init) => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) {
                    const slot = {
                        value: init ? init(initialArg) : initialArg,
                        dispatch: action => {
                            const prevVal = slot.value;
                            const nextVal = slot.reducer(prevVal, action);
                            if (!Object.is(prevVal, nextVal)) {
                                slot.value = nextVal;
                                stateChanged = true;
                            }
                        },
                        reducer,
                    };
                    scope.slots[i] = slot;
                } else {
                    scope.slots[i].reducer = reducer;
                }
                return [scope.slots[i].value, scope.slots[i].dispatch];
            },
            useMemo: (fn, deps) => {
                const i = activeScope.cursor++;
                const slot = activeScope.slots[i];
                if (!slot || !depsEqual(slot.deps, deps)) activeScope.slots[i] = {value: fn(), deps};
                return activeScope.slots[i].value;
            },
            useCallback: (fn, deps) => api.useMemo(() => fn, deps),
            useEffect: (fn, deps) => {
                const scope = activeScope;
                const i = scope.cursor++;
                const prev = scope.slots[i];
                const changed = !prev || !depsEqual(prev.deps, deps);
                const record = {index: i, fn, deps, dirty: changed};
                scope.slots[i] = {deps, cleanup: prev?.cleanup};
                scope.allEffects.push(record);
                if (changed) scope.pendingEffects.push(record);
            },
            useLayoutEffect: (fn, deps) => api.useEffect(fn, deps),
            useImperativeHandle: noop,
            useDebugValue: noop,
            readContext: ctx => api.useContext(ctx),
            // React 18 hooks: the real ones resolve the dispatcher, which is this object (infinite recursion).
            useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(),
            useDeferredValue: value => value,
            useTransition: () => [false, fn => fn()],
            useInsertionEffect: (fn, deps) => api.useEffect(fn, deps),
        };
        const dispatcher = RealReact.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED?.ReactCurrentDispatcher;
        return {
            api,
            renderComponent(key, fn) {
                const scope = getScope(key);
                activeScope = scope;
                scope.cursor = 0;
                scope.pendingEffects = [];
                scope.allEffects = [];
                const prevDispatcher = dispatcher ? dispatcher.current : null;
                if (dispatcher) dispatcher.current = api;
                try {
                    return fn();
                } finally {
                    if (dispatcher) dispatcher.current = prevDispatcher;
                    activeScope = null;
                }
            },
            commitMount(key) {
                const scope = getScope(key);
                scope.cleanups = scope.pendingEffects.map(rec => {
                    const prevCleanup = scope.slots[rec.index]?.cleanup;
                    if (typeof prevCleanup === 'function') prevCleanup();
                    const cleanup = rec.fn();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup};
                    return cleanup;
                });
            },
            runCleanups(key) {
                const scope = getScope(key);
                for (const cleanup of scope.cleanups) {
                    if (typeof cleanup === 'function') cleanup();
                }
                scope.cleanups = [];
                for (const slot of scope.slots) {
                    if (slot && typeof slot.cleanup === 'function') slot.cleanup = undefined;
                }
            },
            remountAllEffects(key) {
                const scope = getScope(key);
                scope.cleanups = scope.allEffects.map(rec => {
                    const cleanup = rec.fn();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup};
                    return cleanup;
                });
            },
        };
    };

    const scheduler = makeScheduler();
    const cache = new Map();
    let latestWorkspace = null;
    const loadFull = filename => {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => {
            if (specifier === 'react') return scheduler.api;
            if (specifier === '@tanstack/react-virtual') {
                return {useVirtualizer: () => ({getTotalSize: () => 0, getVirtualItems: () => [], scrollToIndex: noop, measure: noop})};
            }
            if (specifier === '../components/PivotWorkspaceShell') {
                return {PivotWorkspaceShell: ({workspace}) => {latestWorkspace = workspace; return null;}};
            }
            if (specifier.endsWith('.css')) return {};
            return specifier.startsWith('.') ? loadFull(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    };

    const {PivotWorkspace} = loadFull(path.join(frontend, 'src/lib/components/PivotWorkspace.tsx'));
    const {PivotRuntimeRoot} = loadFull(path.join(frontend, 'src/lib/runtime/PivotRuntimeRoot.tsx'));
    const {HttpPivotTransport, LatestViewportScheduler} = loadFull(path.join(frontend, 'src/lib/runtime/httpTransport.ts'));
    const {mergeFieldPanelSize} = loadFull(path.join(frontend, 'src/lib/utils/fieldPanelLayout.js'));

    const http = new HttpPivotTransport({
        endpoint: '/v1/runtime',
        fetchImpl: async (_url, init) => {
            const envelope = JSON.parse(init.body);
            return jsonResponse({
                schemaVersion: 1,
                kind: 'data',
                status: 'data',
                requestId: envelope.requestId,
                clientInstance: envelope.clientInstance,
                stateEpoch: envelope.stateEpoch,
                windowSeq: envelope.windowSeq,
                payload: {
                    dataVersion: 1,
                    stateEpoch: envelope.stateEpoch,
                    data_state_epoch: envelope.stateEpoch,
                    dataOffset: 0,
                    rowCount: 1,
                    totalCenterCols: 2,
                    colStart: 0,
                    colEnd: 1,
                    columns: [
                        {id: 'Region', header: 'Region', field: 'Region', kind: 'hierarchy'},
                        {id: 'Sales_sum', header: 'Sales', field: 'Sales_sum', kind: 'metric'},
                    ],
                    rows: [{_id: 'APAC', _path: 'APAC', _depth: 0, Region: 'APAC', Sales_sum: 100}],
                    data: [{_id: 'APAC', _path: 'APAC', _depth: 0, Region: 'APAC', Sales_sum: 100}],
                    colSchema: {total_center_cols: 2, columns: [{id: 'Sales_sum', field: 'Sales_sum', header: 'Sales'}]},
                },
            }, 200);
        },
    });
    const transport = new LatestViewportScheduler(http);

    let workspaceProps = {
        id: 'resize-loop-test-pivot',
        table: 'sales',
        transport,
        serverSide: true,
        rowFields: ['Region'],
        colFields: [],
        valConfigs: [{field: 'Sales', agg: 'sum'}],
    };

    const renderPass = () => {
        stateChanged = false;
        const rootEl = scheduler.renderComponent('workspace', () => PivotWorkspace(workspaceProps));
        const shellEl = scheduler.renderComponent('root', () => PivotRuntimeRoot(rootEl.props));
        if (shellEl && shellEl.props && shellEl.props.workspace) {
            latestWorkspace = shellEl.props.workspace;
        }
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');
    };
    const flushUntilSettled = (maxCycles = 12) => {
        let cycles = 0;
        do {
            cycles += 1;
            assert.ok(cycles <= maxCycles, `Detected infinite fieldPanelSizes render/effect loop (> ${maxCycles} cycles)`);
            renderPass();
        } while (stateChanged);
        return cycles;
    };

    const prevWindow = global.window;
    const prevDocument = global.document;
    const prevLocalStorage = global.localStorage;
    const prevRaf = global.requestAnimationFrame;
    const prevCaf = global.cancelAnimationFrame;
    const prevResizeObserver = global.ResizeObserver;
    const fakeStorage = {getItem: () => null, setItem: noop, removeItem: noop};
    global.localStorage = fakeStorage;
    global.requestAnimationFrame = cb => setTimeout(() => cb(Date.now()), 0);
    global.cancelAnimationFrame = id => clearTimeout(id);
    global.ResizeObserver = class ResizeObserver { observe() {} unobserve() {} disconnect() {} };
    global.window = {
        addEventListener: noop,
        removeEventListener: noop,
        requestAnimationFrame: global.requestAnimationFrame,
        cancelAnimationFrame: global.cancelAnimationFrame,
        ResizeObserver: global.ResizeObserver,
        getComputedStyle: () => ({getPropertyValue: () => ''}),
        location: {pathname: '/', search: ''},
        localStorage: fakeStorage,
        matchMedia: () => ({matches: false, addEventListener: noop, removeEventListener: noop, addListener: noop, removeListener: noop}),
    };
    global.document = {
        addEventListener: noop,
        removeEventListener: noop,
        getElementById: () => null,
        querySelector: () => null,
        querySelectorAll: () => [],
        createElement: () => ({
            style: {},
            getContext: () => ({font: '', measureText: text => ({width: String(text || '').length * 8})}),
        }),
        activeElement: null,
        body: {style: {}},
    };
    try {
        // React 18 StrictMode Mount 1 -> Cleanup 1 -> Mount 2
        const wsEl1 = scheduler.renderComponent('workspace', () => PivotWorkspace(workspaceProps));
        scheduler.renderComponent('root', () => PivotRuntimeRoot(wsEl1.props));
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');
        scheduler.runCleanups('root');
        scheduler.runCleanups('workspace');
        scheduler.remountAllEffects('root');
        scheduler.remountAllEffects('workspace');
        flushUntilSettled();

        const setFieldPanelSizes = latestWorkspace.controls.sidebar.setFieldPanelSizes;
        assert.equal(typeof setFieldPanelSizes, 'function');

        // Rapid sequential resizes before props echo stabilizes
        setFieldPanelSizes(prev => mergeFieldPanelSize(prev, 'availableFields', {height: 284}));
        renderPass();
        setFieldPanelSizes(prev => mergeFieldPanelSize(prev, 'availableFields', {height: 320}));
        const cycles1 = flushUntilSettled();
        assert.ok(cycles1 <= 4, `Expected rapid settle without ping-pong (cycles=${cycles1})`);
        assert.equal(latestWorkspace.controls.sidebar.fieldPanelSizes.availableFields.height, 320);

        // Sequential width then height update on rows preserves both axes without snap-back
        setFieldPanelSizes(prev => mergeFieldPanelSize(prev, 'rows', {width: 220}));
        flushUntilSettled();
        setFieldPanelSizes(prev => mergeFieldPanelSize(prev, 'rows', {height: 210}));
        const cycles2 = flushUntilSettled();
        assert.ok(cycles2 <= 4, `Expected partial resize settle without loop (cycles=${cycles2})`);
        assert.deepEqual(latestWorkspace.controls.sidebar.fieldPanelSizes.rows, {width: 220, height: 210});

        // External controlled prop update still syncs cleanly
        workspaceProps = {
            ...workspaceProps,
            fieldPanelSizes: {
                ...latestWorkspace.controls.sidebar.fieldPanelSizes,
                filter: {width: 190, height: 180},
            },
        };
        const cycles3 = flushUntilSettled();
        assert.ok(cycles3 <= 4);
        assert.deepEqual(latestWorkspace.controls.sidebar.fieldPanelSizes.filter, {width: 190, height: 180});
        assert.equal(latestWorkspace.controls.sidebar.fieldPanelSizes.availableFields.height, 320);
    } finally {
        scheduler.runCleanups('root');
        scheduler.runCleanups('workspace');
        global.window = prevWindow;
        global.document = prevDocument;
        global.localStorage = prevLocalStorage;
        global.requestAnimationFrame = prevRaf;
        global.cancelAnimationFrame = prevCaf;
        global.ResizeObserver = prevResizeObserver;
    }
});

test('ResizableFieldPanel handles width, height, corner drag, auto-sidebar expansion, and blur cleanup', () => {
    const RealReact = localRequire('react');
    const noop = () => {};
    const makeScheduler = () => {
        const scopes = new Map();
        let activeScope = null;
        const depsEqual = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
        const getScope = key => {
            if (!scopes.has(key)) scopes.set(key, {slots: [], effects: [], pendingEffects: [], cleanups: []});
            return scopes.get(key);
        };
        const api = {
            ...RealReact,
            memo: fn => fn,
            createElement: (type, props, ...children) => ({type, props: {...props, children}}),
            useRef: initial => {
                const i = activeScope.cursor++;
                return activeScope.slots[i] ||= {current: initial};
            },
            useState: initial => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) scope.slots[i] = {value: typeof initial === 'function' ? initial() : initial};
                return [scope.slots[i].value, next => {
                    scope.slots[i].value = typeof next === 'function' ? next(scope.slots[i].value) : next;
                }];
            },
            useMemo: (fn, deps) => {
                const i = activeScope.cursor++;
                const slot = activeScope.slots[i];
                if (!slot || !depsEqual(slot.deps, deps)) activeScope.slots[i] = {value: fn(), deps};
                return activeScope.slots[i].value;
            },
            useCallback: (fn, deps) => api.useMemo(() => fn, deps),
            useEffect: (fn, deps) => {
                const scope = activeScope;
                const i = scope.cursor++;
                const prev = scope.slots[i];
                const changed = !prev || !depsEqual(prev.deps, deps);
                const record = {index: i, fn, deps};
                scope.slots[i] = {deps, cleanup: prev?.cleanup};
                if (changed) scope.pendingEffects.push(record);
            },
        };
        const dispatcher = RealReact.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED?.ReactCurrentDispatcher;
        return {
            api,
            render(key, fn) {
                const scope = getScope(key);
                activeScope = scope;
                scope.cursor = 0;
                scope.pendingEffects = [];
                const prevDispatcher = dispatcher ? dispatcher.current : null;
                if (dispatcher) dispatcher.current = api;
                let tree;
                try {
                    tree = fn();
                } finally {
                    if (dispatcher) dispatcher.current = prevDispatcher;
                    activeScope = null;
                }
                for (const rec of scope.pendingEffects) {
                    const prevCleanup = scope.slots[rec.index]?.cleanup;
                    if (typeof prevCleanup === 'function') prevCleanup();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup: rec.fn()};
                }
                return tree;
            },
            cleanupAll() {
                for (const scope of scopes.values()) {
                    for (const slot of scope.slots) {
                        if (slot && typeof slot.cleanup === 'function') slot.cleanup();
                    }
                }
            },
        };
    };

    const scheduler = makeScheduler();
    const {mergeFieldPanelSize, DEFAULT_FIELD_PANEL_SIZES} = loadRuntime()('utils/fieldPanelLayout.js');
    assert.deepEqual(
        mergeFieldPanelSize({rows: {width: 220, height: 140}}, 'rows', {width: undefined, height: 210}).rows,
        {width: 220, height: 210},
        'mergeFieldPanelSize must ignore undefined axis fields'
    );

    const listeners = new Map();
    const prevWindow = global.window;
    const prevDocument = global.document;
    const prevRaf = global.requestAnimationFrame;
    const prevCaf = global.cancelAnimationFrame;
    const bodyStyle = {cursor: 'auto', userSelect: 'text'};
    global.requestAnimationFrame = () => 1;
    global.cancelAnimationFrame = noop;
    global.window = {
        addEventListener: (type, fn) => {
            if (!listeners.has(type)) listeners.set(type, new Set());
            listeners.get(type).add(fn);
        },
        removeEventListener: (type, fn) => {
            listeners.get(type)?.delete(fn);
        },
        requestAnimationFrame: global.requestAnimationFrame,
        cancelAnimationFrame: global.cancelAnimationFrame,
    };
    global.document = {body: {style: bodyStyle}};

    const dummyComp = () => null;
    const iconProxy = new Proxy({}, {get: () => dummyComp});
    const sidebarCache = new Map();
    const loadSidebar = filename => {
        filename = ['', '.js', '.jsx', '.ts', '.tsx'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (sidebarCache.has(filename)) return sidebarCache.get(filename).exports;
        const mod = {exports: {}};
        sidebarCache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => {
            if (specifier === 'react') return scheduler.api;
            if (specifier.endsWith('PivotThemeContext')) {
                return {
                    usePivotTheme: () => ({
                        theme: {primary: '#2563EB', border: '#E5E7EB', text: '#111827', textSec: '#6B7280', background: '#FFFFFF', isDark: false, select: '#EFF6FF'},
                        styles: {
                            sidebar: {},
                            sectionTitle: {},
                            sectionTitleSm: {},
                            dropZone: {padding: '8px', borderRadius: '10px', background: '#F9FAFB'},
                        },
                    }),
                };
            }
            if (specifier.endsWith('PivotConfigContext')) {
                return {
                    usePivotConfig: () => ({
                        filters: {}, setFilters: noop,
                        pivotMode: 'pivot',
                        reportDef: {}, setReportDef: noop,
                        savedReports: [], setSavedReports: noop,
                        activeReportId: null, setActiveReportId: noop,
                        customDimensions: [], setCustomDimensions: noop,
                    }),
                };
            }
            if (specifier.endsWith('Icons')) return iconProxy;
            if (specifier.endsWith('ThemedSelect') || specifier.endsWith('FilterPopover') || specifier.endsWith('SidebarFilterItem') || specifier.endsWith('ToolPanelSection') || specifier.endsWith('ColumnTreeItem')) {
                return dummyComp;
            }
            if (specifier.endsWith('usePivotRenderCounter')) return {usePivotRenderCounter: noop};
            if (specifier.endsWith('fieldPanelLayout')) {
                return loadRuntime()('utils/fieldPanelLayout.js');
            }
            return specifier.startsWith('.') ? loadSidebar(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    };

    const SidebarPanelModule = loadSidebar(path.join(frontend, 'src/lib/components/Sidebar/SidebarPanel.js'));
    const SidebarPanelExport = SidebarPanelModule.SidebarPanel || SidebarPanelModule.default;
    const SidebarPanel = typeof SidebarPanelExport === 'function' ? SidebarPanelExport : SidebarPanelExport.type;
    const findNodes = (node, predicate, out = []) => {
        if (!node || typeof node !== 'object') return out;
        if (Array.isArray(node)) {
            node.forEach(item => findNodes(item, predicate, out));
            return out;
        }
        if (predicate(node)) out.push(node);
        if (node.props && node.props.children) findNodes(node.props.children, predicate, out);
        return out;
    };

    let sidebarWidth = 288;
    let fieldPanelSizes = structuredClone(DEFAULT_FIELD_PANEL_SIZES);
    const setSidebarWidth = next => {
        sidebarWidth = typeof next === 'function' ? next(sidebarWidth) : next;
    };
    const setFieldPanelSizes = next => {
        fieldPanelSizes = typeof next === 'function' ? next(fieldPanelSizes) : next;
    };

    const renderPanel = panelId => {
        const sidebarTree = scheduler.render('sidebar', () => SidebarPanel({
            rowFields: ['Region'],
            colFields: [],
            valConfigs: [{field: 'Sales', agg: 'sum'}],
            columnVisibility: {}, setColumnVisibility: noop,
            columnPinning: {}, setColumnPinning: noop,
            availableFields: ['Region', 'Sales', 'Category'],
            table: null,
            pinningPresets: {},
            showNotification: noop,
            filterAnchorEl: null, setFilterAnchorEl: noop,
            colSearch: '', setColSearch: noop,
            colTypeFilter: 'all', setColTypeFilter: noop,
            selectedCols: {}, setSelectedCols: noop,
            dropLine: null, onDragStart: noop, onDragOver: noop, onDrop: noop, onDragEnd: noop,
            onKeyboardFieldDrop: noop,
            handleHeaderFilter: noop, handleFilterClick: noop, requestFilterOptions: noop,
            handleExpandAllRows: noop, handlePinColumn: noop, toggleAllColumnsPinned: noop,
            activeFilterCol: null, closeFilterPopover: noop, filterOptions: {},
            data: [],
            sidebarWidth, setSidebarWidth,
            sidebarTab: 'fields',
            fieldPanelSizes, setFieldPanelSizes,
        }));
        const panels = findNodes(sidebarTree, el => el?.props?.panelId === panelId);
        assert.equal(panels.length, 1, `Expected 1 ResizableFieldPanel for ${panelId}`);
        return scheduler.render(`panel-${panelId}`, () => panels[0].type(panels[0].props));
    };

    try {
        // 1. Width resize past parent container width auto-expands sidebarWidth and commits cleanly on mouseup
        let afTree = renderPanel('availableFields');
        assert.equal(afTree.props.style.flexShrink, 0);
        assert.equal(afTree.props.style.width, '100%');
        assert.equal(afTree.props.style.height, '164px');

        const panelEl = {
            style: {...afTree.props.style},
            parentElement: {getBoundingClientRect: () => ({width: 244, height: 164})},
            getBoundingClientRect: () => ({
                width: fieldPanelSizes.availableFields.width ?? 244,
                height: fieldPanelSizes.availableFields.height,
            }),
        };
        afTree.props.ref.current = panelEl;

        const widthHandle = findNodes(afTree, el => el?.props?.title === 'Resize Available Fields width')[0];
        const heightHandle = findNodes(afTree, el => el?.props?.title === 'Resize Available Fields height')[0];
        const cornerHandle = findNodes(afTree, el => el?.props?.title === 'Resize Available Fields')[0];
        assert.equal(widthHandle.props.className, 'sp-resize-handle');
        assert.equal(heightHandle.props.className, 'sp-resize-handle');
        assert.equal(cornerHandle.props.className, 'sp-resize-handle');

        widthHandle.props.onMouseDown({clientX: 250, clientY: 120, preventDefault: noop, stopPropagation: noop});
        assert.equal(bodyStyle.cursor, 'ew-resize');
        assert.equal(bodyStyle.userSelect, 'none');

        const dispatchWindow = (type, payload) => {
            for (const fn of [...(listeners.get(type) || [])]) fn(payload);
        };
        dispatchWindow('mousemove', {clientX: 330, clientY: 120});
        assert.equal(panelEl.style.width, '324px', 'DOM width must update immediately during drag beyond 244px');
        afTree = renderPanel('availableFields');
        assert.equal(findNodes(afTree, el => el?.props?.title === 'Resize Available Fields width')[0].props['data-resizing'], 'true');

        dispatchWindow('mouseup', {clientX: 330, clientY: 120});
        assert.equal(fieldPanelSizes.availableFields.width, 324);
        assert.equal(fieldPanelSizes.availableFields.height, 164);
        assert.equal(sidebarWidth, 368, 'sidebarWidth must expand smoothly when panel width exceeds container width');
        assert.equal(bodyStyle.cursor, 'auto');
        assert.equal(bodyStyle.userSelect, 'text');
        assert.equal(listeners.get('mousemove')?.size || 0, 0);

        // 2. Height drag committed via window blur preserves existing custom width
        afTree = renderPanel('availableFields');
        afTree.props.ref.current = panelEl;
        findNodes(afTree, el => el?.props?.title === 'Resize Available Fields height')[0].props.onMouseDown({
            clientX: 160, clientY: 220, preventDefault: noop, stopPropagation: noop,
        });
        assert.equal(bodyStyle.cursor, 'ns-resize');
        dispatchWindow('mousemove', {clientX: 160, clientY: 320});
        assert.equal(panelEl.style.height, '264px');
        dispatchWindow('blur', {});
        assert.deepEqual(fieldPanelSizes.availableFields, {width: 324, height: 264});
        assert.equal(bodyStyle.cursor, 'auto');
        assert.equal(listeners.get('blur')?.size || 0, 0);

        // 3. Corner drag with purely vertical motion on a width:null panel preserves width:null (100%)
        let rowsTree = renderPanel('rows');
        const rowsEl = {
            style: {...rowsTree.props.style},
            parentElement: {getBoundingClientRect: () => ({width: 324, height: 140})},
            getBoundingClientRect: () => ({width: 324, height: 140}),
        };
        rowsTree.props.ref.current = rowsEl;
        findNodes(rowsTree, el => el?.props?.title === 'Resize Rows')[0].props.onMouseDown({
            clientX: 320, clientY: 500, preventDefault: noop, stopPropagation: noop,
        });
        dispatchWindow('mousemove', {clientX: 321, clientY: 550});
        dispatchWindow('mouseup', {clientX: 321, clientY: 550});
        assert.deepEqual(fieldPanelSizes.rows, {width: null, height: 190});

        // 4. Narrowing a previously auto-expanded panel contracts sidebarWidth back toward baseline, and double-click resets size
        afTree = renderPanel('availableFields');
        afTree.props.ref.current = panelEl;
        findNodes(afTree, el => el?.props?.title === 'Resize Available Fields width')[0].props.onMouseDown({
            clientX: 324, clientY: 120, preventDefault: noop, stopPropagation: noop,
        });
        dispatchWindow('mousemove', {clientX: 224, clientY: 120});
        dispatchWindow('mouseup', {clientX: 224, clientY: 120});
        assert.equal(fieldPanelSizes.availableFields.width, 224);
        assert.equal(sidebarWidth, 288, 'sidebarWidth must contract back to baseline when panel is narrowed');

        afTree = renderPanel('availableFields');
        afTree.props.ref.current = panelEl;
        findNodes(afTree, el => el?.props?.title === 'Resize Available Fields')[0].props.onDoubleClick({
            preventDefault: noop, stopPropagation: noop,
        });
        assert.deepEqual(fieldPanelSizes.availableFields, {width: null, height: 164});
    } finally {
        scheduler.cleanupAll();
        global.window = prevWindow;
        global.document = prevDocument;
        global.requestAnimationFrame = prevRaf;
        global.cancelAnimationFrame = prevCaf;
    }
});

test('removing a row or column field dispatches only 1 structural computation without post-schema cache wipe or duplicate viewport request', async () => {
    const RealReact = localRequire('react');
    const noop = () => {};
    let stateChanged = false;
    const makeScheduler = () => {
        const scopes = new Map();
        let activeScope = null;
        const depsEqual = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
        const getScope = key => {
            if (!scopes.has(key)) scopes.set(key, {slots: [], pendingEffects: [], allEffects: [], cleanups: []});
            return scopes.get(key);
        };
        const api = {
            ...RealReact,
            memo: fn => fn,
            useContext: ctx => ctx ? ctx._currentValue : undefined,
            useRef: initial => {
                const i = activeScope.cursor++;
                return activeScope.slots[i] ||= {current: initial};
            },
            useState: initial => {
                const scope = activeScope;
                const i = scope.cursor++;
                if (!(i in scope.slots)) {
                    scope.slots[i] = {value: typeof initial === 'function' ? initial() : initial};
                }
                const setState = next => {
                    const resolved = typeof next === 'function' ? next(scope.slots[i].value) : next;
                    if (!Object.is(resolved, scope.slots[i].value)) {
                        scope.slots[i].value = resolved;
                        stateChanged = true;
                    }
                };
                return [scope.slots[i].value, setState];
            },
            useReducer: (reducer, initialArg, init) => {
                const [state, setState] = api.useState(() => (typeof init === 'function' ? init(initialArg) : initialArg));
                return [state, action => setState(prev => reducer(prev, action))];
            },
            useMemo: (fn, deps) => {
                const i = activeScope.cursor++;
                const slot = activeScope.slots[i];
                if (!slot || !depsEqual(slot.deps, deps)) activeScope.slots[i] = {value: fn(), deps};
                return activeScope.slots[i].value;
            },
            useCallback: (fn, deps) => api.useMemo(() => fn, deps),
            useEffect: (fn, deps) => {
                const scope = activeScope;
                const i = scope.cursor++;
                const prev = scope.slots[i];
                const changed = !prev || !depsEqual(prev.deps, deps);
                const record = {index: i, fn, deps, dirty: changed};
                scope.slots[i] = {deps, cleanup: prev?.cleanup};
                scope.allEffects.push(record);
                if (changed) scope.pendingEffects.push(record);
            },
            useLayoutEffect: (fn, deps) => api.useEffect(fn, deps),
            useImperativeHandle: noop,
            useDebugValue: noop,
            readContext: ctx => api.useContext(ctx),
            // React 18 hooks: the real ones resolve the dispatcher, which is this object (infinite recursion).
            useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(),
            useDeferredValue: value => value,
            useTransition: () => [false, fn => fn()],
            useInsertionEffect: (fn, deps) => api.useEffect(fn, deps),
        };
        const dispatcher = RealReact.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED?.ReactCurrentDispatcher;
        return {
            api,
            renderComponent(key, fn) {
                const scope = getScope(key);
                activeScope = scope;
                scope.cursor = 0;
                scope.pendingEffects = [];
                scope.allEffects = [];
                const prevDispatcher = dispatcher ? dispatcher.current : null;
                if (dispatcher) dispatcher.current = api;
                try {
                    return fn();
                } finally {
                    if (dispatcher) dispatcher.current = prevDispatcher;
                    activeScope = null;
                }
            },
            commitMount(key) {
                const scope = getScope(key);
                scope.cleanups = scope.pendingEffects.map(rec => {
                    const prevCleanup = scope.slots[rec.index]?.cleanup;
                    if (typeof prevCleanup === 'function') prevCleanup();
                    const cleanup = rec.fn();
                    scope.slots[rec.index] = {...scope.slots[rec.index], cleanup};
                    return cleanup;
                });
            },
            runCleanups(key) {
                const scope = getScope(key);
                for (const cleanup of scope.cleanups) {
                    if (typeof cleanup === 'function') cleanup();
                }
                scope.cleanups = [];
            },
        };
    };

    const scheduler = makeScheduler();
    const cache = new Map();
    let latestWorkspace = null;
    const virtualItems = [{index: 0, start: 0, size: 32}];
    const loadFull = filename => {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => {
            if (specifier === 'react') return scheduler.api;
            if (specifier === '@tanstack/react-virtual') {
                return {useVirtualizer: () => ({getTotalSize: () => 32, getVirtualItems: () => virtualItems, scrollToIndex: noop, measure: noop})};
            }
            if (specifier === '../components/PivotWorkspaceShell') {
                return {PivotWorkspaceShell: ({workspace}) => {latestWorkspace = workspace; return null;}};
            }
            if (specifier.endsWith('.css')) return {};
            return specifier.startsWith('.') ? loadFull(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    };

    const {PivotWorkspace} = loadFull(path.join(frontend, 'src/lib/components/PivotWorkspace.tsx'));
    const {PivotRuntimeRoot} = loadFull(path.join(frontend, 'src/lib/runtime/PivotRuntimeRoot.tsx'));
    const {HttpPivotTransport, LatestViewportScheduler} = loadFull(path.join(frontend, 'src/lib/runtime/httpTransport.ts'));

    const sentEnvelopes = [];
    let responseCounter = 0;
    const http = new HttpPivotTransport({
        endpoint: '/v1/runtime',
        fetchImpl: async (_url, init) => {
            const envelope = JSON.parse(init.body);
            sentEnvelopes.push(envelope);
            responseCounter += 1;
            return jsonResponse({
                schemaVersion: 1,
                kind: 'data',
                status: 'data',
                requestId: envelope.requestId,
                clientInstance: envelope.clientInstance,
                stateEpoch: envelope.stateEpoch,
                windowSeq: envelope.windowSeq,
                payload: {
                    dataVersion: responseCounter,
                    stateEpoch: envelope.stateEpoch,
                    data_state_epoch: envelope.stateEpoch,
                    dataOffset: 0,
                    rowCount: 1,
                    totalCenterCols: 1,
                    colStart: 0,
                    colEnd: 0,
                    columns: [
                        {id: 'Region', header: 'Region', field: 'Region', kind: 'hierarchy'},
                        {id: 'Sales_sum', header: 'Sales', field: 'Sales_sum', kind: 'metric'},
                    ],
                    rows: [{_id: 'APAC', _path: 'APAC', _depth: 0, Region: 'APAC', Sales_sum: 100}],
                    data: [{_id: 'APAC', _path: 'APAC', _depth: 0, Region: 'APAC', Sales_sum: 100}],
                    colSchema: {
                        total_center_cols: 1,
                        layoutRevision: `rev_${envelope.stateEpoch}_${responseCounter}`,
                        columns: [{id: 'Sales_sum', field: 'Sales_sum', header: 'Sales', size: 132}],
                    },
                },
            }, 200);
        },
    });
    const transport = new LatestViewportScheduler(http);

    const workspaceProps = {
        id: 'single-computation-field-remove-test',
        table: 'sales',
        transport,
        serverSide: true,
        rowFields: ['Region', 'Category'],
        colFields: ['SubCategory'],
        valConfigs: [{field: 'Sales', agg: 'sum'}],
    };

    const renderPass = () => {
        stateChanged = false;
        const rootEl = scheduler.renderComponent('workspace', () => PivotWorkspace(workspaceProps));
        const shellEl = scheduler.renderComponent('root', () => PivotRuntimeRoot(rootEl.props));
        if (shellEl && shellEl.props && shellEl.props.workspace) {
            latestWorkspace = shellEl.props.workspace;
        }
        scheduler.commitMount('root');
        scheduler.commitMount('workspace');
    };
    const flushAsyncAndRender = async (maxCycles = 12) => {
        for (let cycle = 0; cycle < maxCycles; cycle += 1) {
            renderPass();
            await new Promise(resolve => setTimeout(resolve, 5));
            if (!stateChanged) {
                renderPass();
                if (!stateChanged) break;
            }
        }
    };

    const prevWindow = global.window;
    const prevDocument = global.document;
    const prevLocalStorage = global.localStorage;
    const prevRaf = global.requestAnimationFrame;
    const prevCaf = global.cancelAnimationFrame;
    const prevResizeObserver = global.ResizeObserver;
    const fakeStorage = {getItem: () => null, setItem: noop, removeItem: noop};
    global.localStorage = fakeStorage;
    global.requestAnimationFrame = cb => setTimeout(() => cb(Date.now()), 0);
    global.cancelAnimationFrame = id => clearTimeout(id);
    global.ResizeObserver = class ResizeObserver { observe() {} unobserve() {} disconnect() {} };
    global.window = {
        addEventListener: noop,
        removeEventListener: noop,
        requestAnimationFrame: global.requestAnimationFrame,
        cancelAnimationFrame: global.cancelAnimationFrame,
        ResizeObserver: global.ResizeObserver,
        getComputedStyle: () => ({getPropertyValue: () => ''}),
        location: {pathname: '/', search: ''},
        localStorage: fakeStorage,
        matchMedia: () => ({matches: false, addEventListener: noop, removeEventListener: noop, addListener: noop, removeListener: noop}),
    };
    global.document = {
        addEventListener: noop,
        removeEventListener: noop,
        getElementById: () => null,
        querySelector: () => null,
        querySelectorAll: () => [],
        createElement: () => ({
            style: {},
            getContext: () => ({font: '', measureText: text => ({width: String(text || '').length * 8})}),
        }),
        activeElement: null,
        body: {style: {}},
    };

    try {
        await flushAsyncAndRender();
        assert.equal(sentEnvelopes.length, 1, `Expected 1 initial structural computation, got ${sentEnvelopes.length} (${sentEnvelopes.map(e => e.payload?.intent).join(', ')})`);
        assert.equal(sentEnvelopes[0].payload?.intent, 'structural');

        // Remove a row field ('Category') and verify only 1 structural computation is sent
        sentEnvelopes.length = 0;
        latestWorkspace.controls.sidebar.setRowFieldsWithHistory(prev => prev.filter(f => f !== 'Category'));
        await flushAsyncAndRender();
        assert.equal(sentEnvelopes.length, 1, `Expected 1 structural computation on row field removal, got ${sentEnvelopes.length} (${sentEnvelopes.map(e => e.payload?.intent).join(', ')})`);
        assert.equal(sentEnvelopes[0].payload?.intent, 'structural');
        assert.deepEqual(sentEnvelopes[0].state?.rowFields, ['Region']);

        // Remove the last row field ('Region') and verify only 1 structural computation is sent
        sentEnvelopes.length = 0;
        latestWorkspace.controls.sidebar.setRowFieldsWithHistory(prev => prev.filter(f => f !== 'Region'));
        await flushAsyncAndRender();
        assert.equal(sentEnvelopes.length, 1, `Expected 1 structural computation on last row field removal, got ${sentEnvelopes.length} (${sentEnvelopes.map(e => e.payload?.intent).join(', ')})`);
        assert.equal(sentEnvelopes[0].payload?.intent, 'structural');
        assert.deepEqual(sentEnvelopes[0].state?.rowFields, []);

        // Remove a column field ('SubCategory') and verify only 1 structural computation is sent
        sentEnvelopes.length = 0;
        latestWorkspace.controls.sidebar.setColFieldsWithHistory(prev => prev.filter(f => f !== 'SubCategory'));
        await flushAsyncAndRender();
        assert.equal(sentEnvelopes.length, 1, `Expected 1 structural computation on column field removal, got ${sentEnvelopes.map(e => e.payload?.intent).join(', ')})`);
        assert.equal(sentEnvelopes[0].payload?.intent, 'structural');
        assert.deepEqual(sentEnvelopes[0].state?.colFields, []);
    } finally {
        scheduler.runCleanups('root');
        scheduler.runCleanups('workspace');
        global.window = prevWindow;
        global.document = prevDocument;
        global.localStorage = prevLocalStorage;
        global.requestAnimationFrame = prevRaf;
        global.cancelAnimationFrame = prevCaf;
        global.ResizeObserver = prevResizeObserver;
    }
});

const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');

const frontend = path.resolve(__dirname, '../serverside_pivot');
const localRequire = createRequire(path.join(frontend, 'package.json'));
const React = localRequire('react');
const {renderToStaticMarkup} = localRequire('react-dom/server');
const babel = localRequire('@babel/core');
function loadSource(react, virtualizer, workspaceOnly = false) {
    const cache = new Map();
    function load(filename) {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (!filename) throw new Error('Missing source module');
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets: [
            [localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
            localRequire('@babel/preset-typescript'), localRequire('@babel/preset-react'),
        ]}).code;
        const requireSource = name => {
            if (name === 'react') return react;
            if (workspaceOnly && name === '../runtime/PivotRuntimeRoot') return {PivotRuntimeRoot: function Root() {}};
            // Only browser geometry is controlled; the row model and cache run unchanged.
            if (name === '@tanstack/react-virtual' && virtualizer) return {useVirtualizer: () => virtualizer};
            if (name.endsWith('.css')) return {};
            return name.startsWith('.') ? load(path.resolve(path.dirname(filename), name)) : localRequire(name);
        };
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    }
    return relative => load(path.join(frontend, 'src/lib', relative));
}

// Schedule state/effects explicitly while retaining real lifecycle, freshness,
// normalization, request generation, row cache and rendered presentation code.
function hooks() {
    const slots = [];
    let cursor = 0;
    let pending = [];
    const same = (a, b) => a && b && a.length === b.length && b.every((v, i) => Object.is(v, a[i]));
    const api = {...React,
        useRef(value) {const index = cursor++; return slots[index] ||= {current: value};},
        useState(value) {
            const index = cursor++;
            slots[index] ||= {value: typeof value === 'function' ? value() : value};
            return [slots[index].value, next => {slots[index].value = typeof next === 'function' ? next(slots[index].value) : next;}];
        },
        useMemo(fn, deps) {
            const index = cursor++;
            if (!slots[index] || !same(slots[index].deps, deps)) slots[index] = {value: fn(), deps};
            return slots[index].value;
        },
        useCallback(fn, deps) {return api.useMemo(() => fn, deps);},
        useEffect(fn, deps) {
            const index = cursor++;
            if (!slots[index] || !same(slots[index].deps, deps)) pending.push(() => {
                slots[index]?.cleanup?.();
                slots[index] = {deps, cleanup: fn()};
            });
        },
    };
    api.useLayoutEffect = api.useEffect;
    return {api, render(fn, effects = true) {
        cursor = 0; pending = [];
        const result = fn();
        if (effects) pending.forEach(run => run());
        return result;
    }, cleanup() {slots.forEach(slot => slot?.cleanup?.());}};
}

const noop = () => {};
const failure = {
    schemaVersion: 1, kind: 'data', status: 'error', requestId: 'viewport:s:tab-a:7',
    clientInstance: 'tab-a', stateEpoch: 3, windowSeq: 7,
    message: 'Traceback password=SECRET db://admin:SECRET@internal',
    payload: {code: 'resource_exhausted', resourceKind: 'query', retryAfterMs: 1501},
};
const retryIdentity = (windowSeq, stateEpoch = 3) => ({
    requestId: `viewport:s:tab-a:${windowSeq}`, windowSeq, stateEpoch, clientInstance: 'tab-a',
});
function lifecycle() {
    const scheduler = hooks();
    const load = loadSource(scheduler.api);
    const {useRuntimeResponseLifecycle} = load('runtime/useRuntimeResponseLifecycle.js');
    const {useRuntimeDataCommit} = load('hooks/useRuntimeDataCommit.js');
    let error = null;
    const committed = [], resolved = [], notifications = [];
    let refreshes = 0;
    const args = {
        clientInstanceRef: {current: 'tab-a'}, stateEpochRef: {current: 3},
        latestDataVersionRef: {current: 6}, requestVersionRef: {current: 7},
        layoutRevisionRef: {current: null}, layoutAcknowledgedManifestRef: {current: ''},
        pivotLayoutOrderRef: {current: []}, latestLayoutSentManifestRef: {current: null},
        pendingDataProfilerCommitRef: {current: null}, finalizeTransactionHistoryResponse: noop,
        hydrateVisibleEditOverlay: noop, reconcileOptimisticCellValuesWithPayload: noop,
        setFormulaErrors: noop, setTransportDataState: update => committed.push(update({})),
        activeRuntimePayloadFetchRef: {current: null}, handledRuntimeResponseKeysRef: {current: new Set()},
        profilingEnabledRef: {current: false}, recordProfilerResponse: noop,
        clearPendingTransactionHistoryRequest: noop,
        resolvePendingRequest: response => resolved.push(response.requestId),
        setRuntimeDataError: update => {error = typeof update === 'function' ? update(error) : update;},
        showNotification: (...values) => notifications.push(values),
        scheduleSilentViewportRefresh: () => refreshes++,
        runtimeResponse: null,
    };
    function render(response, overrides = {}) {
        Object.assign(args, overrides, {runtimeResponse: response});
        scheduler.render(() => useRuntimeResponseLifecycle({...args, ...useRuntimeDataCommit(args)}));
    }
    return {render, args, committed, resolved, notifications, cleanup: scheduler.cleanup,
        get error() {return error;}, get refreshes() {return refreshes;}};
}
const success = (windowSeq, value = 'current') => ({...failure, status: 'data',
    requestId: `viewport:s:tab-a:${windowSeq}`, windowSeq,
    payload: {data: [{id: value, amount: 42}], dataVersion: windowSeq, rowCount: 1},
});

test('an admission response cannot attach to a newer window', () => {
    // The Python integration test supplies a real TestClient response; the
    // standalone Node suite runs the same acceptance check with a literal case.
    const response = process.env.PIVOT_HTTP_ADMISSION_RESPONSE
        ? JSON.parse(fs.readFileSync(process.env.PIVOT_HTTP_ADMISSION_RESPONSE, 'utf8'))
        : {...failure, clientInstance: 'browser-tab-a'};
    const newer = lifecycle();
    newer.args.clientInstanceRef.current = 'browser-tab-a';
    newer.args.requestVersionRef.current = 8;
    newer.render(response);
    assert.equal(newer.error, null, 'a rejected window 7 must not show an alert for window 8');
    newer.cleanup();
    const current = lifecycle();
    current.args.clientInstanceRef.current = 'browser-tab-a';
    current.render(response);
    assert.equal(current.error.failedRequest.windowSeq, 7);
    current.cleanup();
});

test('current generic and structured data failures expose safe inline recovery and resolve pending', () => {
    for (const payload of [{}, failure.payload]) {
        const run = lifecycle();
        run.render({...failure, payload});
        assert.ok(run.error, 'the terminal data failure needs inline recovery');
        assert.equal(run.error.retrying, false);
        assert.deepEqual(run.error.failedRequest, retryIdentity(7));
        assert.doesNotMatch(JSON.stringify(run.error), /SECRET|Traceback|db:\/\//);
        assert.deepEqual(run.resolved, ['viewport:s:tab-a:7']);
        assert.deepEqual(run.notifications, []);
        assert.equal(run.refreshes, 0);
    }
});

test('unknown worker layout token retries with the saved manifest without showing a data error', () => {
    const run = lifecycle();
    run.render({...failure, status: 'layout_required', payload: {}});
    assert.equal(run.args.layoutAcknowledgedManifestRef.current, '__layout_retry__');
    assert.equal(run.refreshes, 1);
    assert.equal(run.error, null);
    assert.deepEqual(run.resolved, ['viewport:s:tab-a:7']);
});

test('a self-consistent response for the previous column order cannot commit', () => {
    const run = lifecycle();
    const oldRevision = 'a'.repeat(64);
    run.args.requestVersionRef.current = 8;
    run.args.layoutRevisionRef.current = oldRevision;
    run.args.layoutAcknowledgedManifestRef.current = 'p:old';
    run.args.pivotLayoutOrderRef.current = ['p:new'];
    run.args.latestLayoutSentManifestRef.current = run.args.pivotLayoutOrderRef.current;
    run.render({...success(8), payload: {
        ...success(8).payload, layoutRevision: oldRevision,
        colSchema: {layoutRevision: oldRevision, columns: []},
    }});
    assert.equal(run.committed.length, 0);
});

test('an unchanged-order worker retry can return the same layout revision', () => {
    const run = lifecycle();
    const revision = 'a'.repeat(64);
    run.args.requestVersionRef.current = 8;
    run.args.layoutRevisionRef.current = revision;
    run.args.layoutAcknowledgedManifestRef.current = '__layout_retry__';
    run.args.pivotLayoutOrderRef.current = ['p:current'];
    run.args.latestLayoutSentManifestRef.current = run.args.pivotLayoutOrderRef.current;
    run.render({...success(8), payload: {
        ...success(8).payload, layoutRevision: revision,
        colSchema: {layoutRevision: revision, columns: []},
    }});
    assert.equal(run.committed.length, 1);
});

test('stale, superseded, foreign-client and old-epoch failures stay silent', () => {
    for (const response of [
        {...failure, status: 'stale'}, {...failure, windowSeq: 6},
        {...failure, clientInstance: 'other-tab'}, {...failure, stateEpoch: 2},
        {...failure, window_seq: 6, windowSeq: undefined},
    ]) {
        const run = lifecycle(); run.render(response);
        assert.equal(run.error, null);
        assert.equal(run.committed.length, 0);
    }
});

test('a successful current retry clears the error and late failures or rows cannot overwrite it', () => {
    const run = lifecycle();
    run.render(failure);
    assert.ok(run.error);
    run.args.requestVersionRef.current = 8;
    run.render(success(7, 'late-before-retry'));
    assert.ok(run.error, 'an old success must not dismiss recovery');
    assert.equal(run.committed.length, 0, 'an old success must not replace current cells');
    run.render(success(8));
    assert.equal(run.error, null);
    assert.equal(run.committed[0].data[0].id, 'current');
    run.render({...failure, requestId: 'late-error', windowSeq: 7});
    run.render({...success(7, 'late-after-retry'), requestId: 'late-success'});
    assert.equal(run.error, null);
    assert.equal(run.committed.length, 1);
});

test('retained delivery abort stays silent and expiry retains its existing toast and automatic refresh', async () => {
    const tick = () => new Promise(resolve => setImmediate(resolve));
    const originalError = console.error;
    console.error = noop;
    try {
        for (const name of ['AbortError', 'Error']) {
            const run = lifecycle();
            run.render({...success(7), payload: {payloadRef: {id: 'blob', url: '/payload/blob'}}}, {
                payloadTransport: {fetchPayload: async () => {const error = new Error('SECRET'); error.name = name; throw error;}},
            });
            await tick();
            assert.equal(run.error, null);
            assert.equal(run.notifications.length, name === 'AbortError' ? 0 : 1);
            assert.equal(run.refreshes, name === 'AbortError' ? 0 : 1);
            run.cleanup();
        }
    } finally {console.error = originalError;}
});

test('late retained data from before retry cannot commit or clear the current error', async () => {
    const run = lifecycle();
    let release;
    run.render({...success(7), payload: {payloadRef: {id: 'blob', url: '/payload/blob'}}}, {
        payloadTransport: {fetchPayload: () => new Promise(resolve => {release = resolve;})},
    });
    run.args.requestVersionRef.current = 8;
    release({data: [{id: 'obsolete'}], dataVersion: 7});
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(run.committed.length, 0);
    run.render({...failure, requestId: 'retry-failed', windowSeq: 8});
    assert.ok(run.error);
    run.cleanup();
});

test('a current retry with retained data dismisses recovery only after its payload commits', async () => {
    const run = lifecycle();
    run.render(failure);
    run.args.requestVersionRef.current = 8;
    let release;
    run.render({...success(8), payload: {payloadRef: {id: 'retry-blob', url: '/payload/retry'}}}, {
        payloadTransport: {fetchPayload: () => new Promise(resolve => {release = resolve;})},
    });
    assert.ok(run.error, 'delivery has not completed yet');
    assert.equal(run.committed.length, 0);
    release({data: [{id: 'current-retained'}], dataVersion: 8, rowCount: 1});
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(run.error, null);
    assert.equal(run.committed[0].data[0].id, 'current-retained');
    run.cleanup();
});

function elements(node) {
    if (!node || typeof node !== 'object') return [];
    return [node, ...React.Children.toArray(node.props?.children).flatMap(elements)];
}

test('the shared canvas renders one alert with a keyboard-native Retry action and safe copy', () => {
    const load = loadSource(React);
    const {PivotWorkspaceCanvas} = load('components/PivotWorkspaceCanvas.js');
    const {createWorkspacePresentation} = load('runtime/createWorkspacePresentation.ts');
    const canvas = createWorkspacePresentation({
        runtimeDataError: {retrying: false, message: 'SECRET'}, retryRuntimeData: noop,
        uiConfig: {}, theme: {}, detailConfig: {}, paginationConfig: {},
    });
    const tree = PivotWorkspaceCanvas({canvas});
    // Resolve the actual error component only; charts/table require DOM geometry.
    const alerts = elements(tree).flatMap(node => {
        if (node.props?.role === 'alert') return [node];
        if (typeof node.type === 'function' && node.type.name === 'RuntimeDataError') {
            return elements(node.type(node.props)).filter(child => child.props?.role === 'alert');
        }
        return [];
    });
    assert.equal(alerts.length, 1);
    const html = renderToStaticMarkup(alerts[0]);
    assert.match(html, /could not|couldn&#x27;t|unable|failed/i);
    assert.doesNotMatch(html, /SECRET|resource_exhausted|Traceback/);
    const retry = elements(alerts[0]).find(node => node.type === 'button');
    assert.equal(retry.props.type, 'button');
    assert.equal(retry.props.children, 'Retry');
    assert.equal(retry.props.onClick, noop);
    const emptyTree = PivotWorkspaceCanvas({canvas: {...canvas, runtimeDataError: null}});
    assert.equal(elements(emptyTree).filter(node => node.props?.role === 'alert').length, 0);
});

test('Retry immediately creates a fresh current viewport request despite failed inflight deduplication', () => {
    const scheduler = hooks();
    const virtualizer = {getVirtualItems: () => [], measure: noop};
    const load = loadSource(scheduler.api, virtualizer);
    const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
    const requests = [], pending = [];
    const scroll = {scrollTop: 0, clientHeight: 320};
    const args = {parentRef: {current: scroll}, serverSide: true, rowCount: 1000, rowHeight: 32,
        data: [], requestVersionRef: {current: 6}, stateEpoch: 3, abortGeneration: 2,
        sessionId: 's', clientInstance: 'tab-a', colStart: 2, colEnd: 5,
        setProps: value => requests.push(value.runtimeRequest), onViewportRequest: value => pending.push(value),
    };
    // Effects need a browser. The public refresh command itself runs the real
    // viewport block selection, invalidation, deduplication and request generator.
    const model = scheduler.render(() => useServerSideRowModel(args), false);
    assert.equal(model.requestVisibleViewportRefresh(), true);
    assert.equal(requests[0].requestId, 'viewport:s:tab-a:7');
    assert.equal(model.requestVisibleViewportRefresh(undefined, undefined, {retry: retryIdentity(7)}), true,
        'a failed inflight entry must not suppress the user retry');
    assert.equal(requests[1].requestId, 'viewport:s:tab-a:8');
    assert.equal(pending[1].silent, false, 'user retry retains the ordinary loading path');
    scroll.scrollTop = 3200;
    assert.equal(model.requestVisibleViewportRefresh(12, 15, {retry: retryIdentity(8)}), true);
    assert.equal(requests[2].requestId, 'viewport:s:tab-a:9');
    assert.equal(requests[2].payload.start, 96, 'retry derives row window from current scroll');
    assert.equal(requests[2].payload.state_epoch, 3);
    assert.equal(requests[2].payload.abort_generation, 2);
    assert.equal(requests[2].payload.col_start, 10);
    assert.equal(requests[2].payload.col_end, 17);
});

test('the rendered Retry action drives real viewport generation, pending UI and terminal recovery', () => {
    const scheduler = hooks();
    const virtualizer = {getVirtualItems: () => [], measure: noop};
    const load = loadSource(scheduler.api, virtualizer);
    const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
    const {useRuntimeDataRetry} = load('runtime/useRuntimeDataRetry.js');
    const {RuntimeDataError} = load('components/RuntimeDataError.js');
    const run = lifecycle();
    const requests = [];
    const args = {parentRef: {current: {scrollTop: 0, clientHeight: 320}}, serverSide: true,
        rowCount: 1000, rowHeight: 32, data: [], requestVersionRef: run.args.requestVersionRef,
        stateEpoch: 3, sessionId: 's', clientInstance: 'tab-a', setProps: value => requests.push(value.runtimeRequest),
    };
    run.render(failure);
    function render() {
        return scheduler.render(() => {
            const viewport = useServerSideRowModel(args);
            const recovery = useRuntimeDataRetry({runtimeDataError: run.error,
                setRuntimeDataError: run.args.setRuntimeDataError,
                requestVersionRef: run.args.requestVersionRef, stateEpochRef: run.args.stateEpochRef,
                clientInstanceRef: run.args.clientInstanceRef,
                requestVisibleViewportRefresh: viewport.requestVisibleViewportRefresh});
            return RuntimeDataError({error: recovery.runtimeDataError, onRetry: recovery.retryRuntimeData, theme: {}});
        }, false);
    }
    let alert = render();
    elements(alert).find(node => node.type === 'button').props.onClick();
    assert.equal(requests.length, 1);
    assert.equal(requests[0].requestId, 'viewport:s:tab-a:8');
    alert = render();
    const pendingButton = elements(alert).find(node => node.type === 'button');
    assert.equal(pendingButton.props.disabled, true);
    pendingButton.props.onClick();
    assert.equal(requests.length, 1, 'retry must not dispatch again while pending');
    run.render({...failure, requestId: requests[0].requestId, windowSeq: 8});
    alert = render();
    assert.equal(elements(alert).find(node => node.type === 'button').props.disabled, false);
    elements(alert).find(node => node.type === 'button').props.onClick();
    assert.equal(requests[1].requestId, 'viewport:s:tab-a:9');
    run.render(success(9));
    assert.equal(render(), null);
    assert.equal(run.committed[0].data[0].id, 'current');
});

test('canonical and Dash error envelopes clear their real pending viewport loading state', () => {
    for (const response of [failure, {...failure, windowSeq: undefined, window_seq: 7}]) {
        const scheduler = hooks();
        const load = loadSource(scheduler.api);
        const {useServerSideViewportController} = load('hooks/useServerSideViewportController.js');
        const {coerceTransportNumber} = load('hooks/usePivotNormalization.js');
        const args = {serverSide: true, effectiveRowCount: 10, responseColumns: [], dataVersion: 6,
            stateEpoch: 3, filteredData: [], rowFields: [], colFields: [],
            structuralPendingVersionRef: {current: null}, requestVersionRef: {current: 7},
            latestDataVersionRef: {current: 6}, coerceTransportNumber,
            mergeSparseColSchema: noop, isSparseSchemaRangeLoaded: () => false,
        };
        const render = () => scheduler.render(() => useServerSideViewportController(args), false);
        const controller = render();
        controller.markRequestPending({version: 7});
        try {
            assert.equal(controller.resolvePendingRequest(response), true,
                'a V1 canonical error must retire its actual pending request');
            assert.equal(render().isRequestPending, false);
        } finally {controller.resolvePendingRequest({version: 7});}
    }
});

test('an error without optional request identity remains stable across response callback changes', () => {
    const run = lifecycle();
    const response = {...failure, requestId: undefined};
    run.render(response);
    assert.ok(run.error);
    const first = run.error;
    run.render(response, {recordProfilerResponse: () => {}});
    assert.equal(run.error, first, 'reprocessing an unkeyed error must not trigger a render loop');
});

test('explicit error recovery can retry a failed structural request before its loading timeout', () => {
    const scheduler = hooks();
    const load = loadSource(scheduler.api, {getVirtualItems: () => [], measure: noop});
    const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
    const requests = [];
    const model = scheduler.render(() => useServerSideRowModel({
        parentRef: {current: {scrollTop: 640, clientHeight: 320}}, serverSide: true,
        rowCount: 1000, rowHeight: 32, data: [], requestVersionRef: {current: 7},
        stateEpoch: 4, structuralInFlight: true, needsColSchema: true,
        sessionId: 's', clientInstance: 'tab-a', setProps: value => requests.push(value.runtimeRequest),
    }), false);
    assert.equal(model.requestVisibleViewportRefresh(), false);
    assert.equal(model.requestVisibleViewportRefresh(undefined, undefined, {retry: retryIdentity(7, 4)}), true,
        'a failed structural request must not leave Retry blocked for ten seconds');
    assert.equal(requests[0].requestId, 'viewport:s:tab-a:8');
    assert.equal(requests[0].payload.state_epoch, 4);
    assert.equal(requests[0].payload.needs_col_schema, true);
});

test('direct HTTP overload and thrown failures reach shared safe recovery while AbortError is silent', async () => {
    for (const scenario of ['overload', 'failure', 'abort']) {
        const scheduler = hooks();
        // Only the workspace's presentation root is suppressed; request dispatch,
        // HTTP decoding, error conversion and shared response processing are real.
        const load = loadSource(scheduler.api, undefined, true);
        const {PivotWorkspace} = load('components/PivotWorkspace.tsx');
        const {HttpPivotTransport} = load('runtime/httpTransport.ts');
        const transport = scenario === 'overload'
            ? new HttpPivotTransport({fetchImpl: async () => new Response(JSON.stringify(failure), {
                status: 503, headers: {'Content-Type': 'application/json'},
            })})
            : {request: async () => {
                if (scenario === 'abort') throw new DOMException('SECRET', 'AbortError');
                throw new Error('Traceback db://admin:SECRET@internal');
            }};
        const render = () => scheduler.render(() => PivotWorkspace({transport, table: 'sales'}));
        const root = render();
        root.props.setProps({runtimeRequest: {schemaVersion: 1, kind: 'data',
            requestId: 'viewport:s:tab-a:7', clientInstance: 'tab-a', stateEpoch: 3, windowSeq: 7,
            payload: {intent: 'viewport'},
        }});
        await new Promise(resolve => setImmediate(resolve));
        const response = render().props.runtimeResponse;
        if (scenario === 'abort') {
            assert.equal(response, undefined);
        } else {
            assert.equal(response.status, 'error');
            const run = lifecycle(); run.render(response);
            assert.ok(run.error);
            const {RuntimeDataError} = load('components/RuntimeDataError.js');
            const html = renderToStaticMarkup(RuntimeDataError({error: run.error, onRetry: noop, theme: {}}));
            assert.doesNotMatch(html, /SECRET|Traceback|internal/);
            assert.match(html, /Retry/);
        }
        scheduler.cleanup();
    }
});

test('an old alert cannot retire a newer active viewport request but the current failure remains retryable', () => {
    const scheduler = hooks();
    const load = loadSource(scheduler.api, {getVirtualItems: () => [], measure: noop});
    const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
    const {useRuntimeDataRetry} = load('runtime/useRuntimeDataRetry.js');
    const {RuntimeDataError} = load('components/RuntimeDataError.js');
    const run = lifecycle();
    run.args.requestVersionRef.current = 6;
    const requests = [];
    const scroll = {scrollTop: 0, clientHeight: 320};
    const args = {parentRef: {current: scroll}, serverSide: true, rowCount: 1000, rowHeight: 32,
        data: [], requestVersionRef: run.args.requestVersionRef, stateEpoch: 3,
        sessionId: 's', clientInstance: 'tab-a', setProps: value => requests.push(value.runtimeRequest),
    };
    function render() {
        return scheduler.render(() => {
            const model = useServerSideRowModel(args);
            const recovery = useRuntimeDataRetry({runtimeDataError: run.error,
                setRuntimeDataError: run.args.setRuntimeDataError,
                requestVersionRef: run.args.requestVersionRef, stateEpochRef: run.args.stateEpochRef,
                clientInstanceRef: run.args.clientInstanceRef,
                requestVisibleViewportRefresh: model.requestVisibleViewportRefresh});
            return {model, alert: RuntimeDataError({error: recovery.runtimeDataError, onRetry: recovery.retryRuntimeData, theme: {}})};
        }, false);
    }
    const originalNow = Date.now;
    let now = 10000;
    Date.now = () => now;
    try {
        const first = render();
        assert.equal(first.model.requestVisibleViewportRefresh(), true);
        run.render(failure);
        const failed = render();
        const oldRetry = elements(failed.alert).find(node => node.type === 'button').props.onClick;
        now += 500;
        scroll.scrollTop = 3200;
        assert.equal(failed.model.requestVisibleViewportRefresh(), true);
        assert.equal(requests[1].requestId, 'viewport:s:tab-a:8');
        oldRetry(); // A click already queued before React rerenders must also be safe.
        assert.equal(requests.length, 2, 'the old alert must not supersede active request 8 with request 9');
        const active = render();
        assert.equal(elements(active.alert).find(node => node.type === 'button').props.disabled, true);
        run.render({...failure, requestId: 'viewport:s:tab-a:8', windowSeq: 8});
        const current = render();
        const retryCurrent = elements(current.alert).find(node => node.type === 'button');
        assert.equal(retryCurrent.props.disabled, false);
        retryCurrent.props.onClick();
        assert.equal(requests[2].requestId, 'viewport:s:tab-a:9');
        run.render(success(9));
        assert.equal(render().alert, null);
    } finally {Date.now = originalNow; scheduler.cleanup();}
});

test('the viewport recovery boundary rejects stale generation, epoch, client and unscoped bypass tokens', () => {
    for (const retry of [retryIdentity(6), retryIdentity(7, 2), {...retryIdentity(7), clientInstance: 'other-tab'}, true]) {
        const scheduler = hooks();
        const load = loadSource(scheduler.api, {getVirtualItems: () => [], measure: noop});
        const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
        const requests = [];
        const model = scheduler.render(() => useServerSideRowModel({
            parentRef: {current: {scrollTop: 0, clientHeight: 320}}, serverSide: true,
            rowCount: 1000, rowHeight: 32, data: [], requestVersionRef: {current: 7},
            stateEpoch: 3, sessionId: 's', clientInstance: 'tab-a',
            setProps: value => requests.push(value.runtimeRequest),
        }), false);
        assert.equal(model.requestVisibleViewportRefresh(undefined, undefined, {retry}), false);
        assert.equal(requests.length, 0, 'an invalid retry token cannot fall through to ordinary dispatch');
        assert.equal(model.requestVisibleViewportRefresh(undefined, undefined, {retry: retryIdentity(7)}), true);
        assert.equal(requests[0].requestId, 'viewport:s:tab-a:8');
    }
});

test('an unkeyed error keeps its received generation across callback reruns and distinct later errors remain retryable', () => {
    const scheduler = hooks();
    const load = loadSource(scheduler.api, {getVirtualItems: () => [], measure: noop});
    const {useServerSideRowModel} = load('hooks/useServerSideRowModel.js');
    const {useRuntimeDataRetry} = load('runtime/useRuntimeDataRetry.js');
    const {RuntimeDataError} = load('components/RuntimeDataError.js');
    const run = lifecycle();
    run.args.requestVersionRef.current = 6;
    const requests = [];
    const scroll = {scrollTop: 0, clientHeight: 320};
    const args = {parentRef: {current: scroll}, serverSide: true, rowCount: 1000, rowHeight: 32,
        data: [], requestVersionRef: run.args.requestVersionRef, stateEpoch: 3,
        sessionId: 's', clientInstance: 'tab-a', setProps: value => requests.push(value.runtimeRequest),
    };
    function render() {
        return scheduler.render(() => {
            const model = useServerSideRowModel(args);
            const recovery = useRuntimeDataRetry({runtimeDataError: run.error,
                setRuntimeDataError: run.args.setRuntimeDataError,
                requestVersionRef: run.args.requestVersionRef, stateEpochRef: run.args.stateEpochRef,
                clientInstanceRef: run.args.clientInstanceRef,
                requestVisibleViewportRefresh: model.requestVisibleViewportRefresh});
            return {model, alert: RuntimeDataError({error: recovery.runtimeDataError, onRetry: recovery.retryRuntimeData, theme: {}})};
        }, false);
    }
    const originalNow = Date.now;
    let now = 10000;
    Date.now = () => now;
    try {
        render().model.requestVisibleViewportRefresh();
        // Both identity fields are optional in a valid V1 error response.
        const unkeyed = {...failure};
        delete unkeyed.requestId;
        delete unkeyed.windowSeq;
        run.render(unkeyed);
        assert.equal(run.error.failedRequest.windowSeq, 7);
        const captured = run.error;
        now += 500;
        scroll.scrollTop = 3200;
        render().model.requestVisibleViewportRefresh();
        assert.equal(requests[1].requestId, 'viewport:s:tab-a:8');
        run.render(unkeyed, {recordProfilerResponse: () => {}});
        const oldButton = elements(render().alert).find(node => node.type === 'button');
        oldButton.props.onClick();
        assert.equal(requests.length, 2, 'reprocessing an unkeyed error cannot rebind Retry to active request 8');
        assert.equal(run.error, captured);
        assert.equal(run.error.failedRequest.windowSeq, 7);
        assert.equal(oldButton.props.disabled, true);

        // A genuinely new received error, even with identical contents and no
        // optional identity, belongs to the current request and remains usable.
        const later = {...unkeyed};
        run.render(later);
        assert.equal(run.error.failedRequest.windowSeq, 8);
        const currentButton = elements(render().alert).find(node => node.type === 'button');
        assert.equal(currentButton.props.disabled, false);
        currentButton.props.onClick();
        assert.equal(requests[2].requestId, 'viewport:s:tab-a:9');
        run.render(success(9));
        assert.equal(render().alert, null);
        run.render(unkeyed, {recordProfilerResponse: () => {}});
        assert.equal(render().alert, null, 'an already-handled error cannot reappear after current success');
    } finally {Date.now = originalNow; scheduler.cleanup();}
});

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

function loadLifecycle() {
    const cache = new Map();
    function load(filename) {
        filename = ['', '.ts', '.tsx', '.js'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets}).code;
        const requireSource = specifier => specifier === 'react'
            ? {useEffect: effect => effect(), useRef: initial => ({current: initial})}
            : specifier.startsWith('.') ? load(path.resolve(path.dirname(filename), specifier)) : localRequire(specifier);
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    }
    return load(path.join(frontend, 'src/lib/runtime/useRuntimeResponseLifecycle.js')).useRuntimeResponseLifecycle;
}

function runPartResponse({status = 'ok', payload = {}, includePartId = true, activeRequestId = 'export-1',
    copyIntent = false, copyRichToClipboard = async () => true} = {}) {
    const useRuntimeResponseLifecycle = loadLifecycle();
    let modal = {processingPartId: 'part-1', completedParts: new Set()};
    const notifications = [];
    const downloads = [];
    const noop = () => {};
    const activeExportRequestIdRef = {current: activeRequestId};
    const cancelledExportRequestIdsRef = {current: new Set()};
    useRuntimeResponseLifecycle({
        runtimeResponse: {
            schemaVersion: 1, kind: 'export', status, requestId: 'export-1',
            payload: {rows: 7, filename: 'part.csv', ...(includePartId ? {partId: 'part-1'} : {}), ...payload},
        },
        activeExportRequestIdRef,
        cancelledExportRequestIdsRef,
        handledRuntimeResponseKeysRef: {current: new Set()},
        pendingCopyIntentRef: {current: copyIntent ? {kind: 'part'} : null},
        setPartModal: update => {modal = update(modal);},
        setIsExporting: noop,
        setIsCopying: noop,
        copyRichToClipboard,
        triggerExportDownload: (...args) => downloads.push(args),
        showNotification: (...args) => notifications.push(args),
        recordProfilerResponse: noop,
        profilingEnabledRef: {current: false},
    });
    return {get modal() {return modal;}, notifications, downloads,
        cancel: () => {
            cancelledExportRequestIdsRef.current.add('export-1');
            activeExportRequestIdRef.current = null;
            modal = null;
        },
    };
}

const settle = () => new Promise(resolve => setImmediate(resolve));

test('failed V1 export part clears processing without marking part complete', () => {
    const run = runPartResponse({status: 'error', payload: {code: 'resource_exhausted'}});
    assert.equal(run.modal.processingPartId, null);
    assert.equal(run.modal.completedParts.has('part-1'), false);
    assert.deepEqual(run.downloads, []);
    assert.deepEqual(run.notifications, [['Export failed', 'error']]);
});

test('current export errors lacking partId clear the active part without completing it', () => {
    for (const status of ['error', 'stale']) {
        const run = runPartResponse({status, includePartId: false, payload: {code: 'resource_exhausted'}});
        assert.equal(run.modal.processingPartId, null);
        assert.equal(run.modal.completedParts.has('part-1'), false);
        assert.deepEqual(run.downloads, []);
    }
});

test('an unrelated export error lacking partId cannot clear a newer active part', () => {
    const run = runPartResponse({status: 'error', includePartId: false, activeRequestId: 'export-2'});
    assert.equal(run.modal.processingPartId, 'part-1');
    assert.equal(run.modal.completedParts.has('part-1'), false);
});

test('retained export part stays retryable when payload download fails', async () => {
    const originalFetch = global.fetch;
    global.fetch = async () => new Response('expired', {status: 410});
    try {
        const run = runPartResponse({payload: {payloadRef: {id: 'payload-1', url: '/v1/runtime/payload/payload-1'}}});
        await settle();
        assert.equal(run.modal.processingPartId, null);
        assert.equal(run.modal.completedParts.has('part-1'), false);
        assert.deepEqual(run.downloads, []);
        assert.deepEqual(run.notifications, [['Export failed', 'error']]);
    } finally {global.fetch = originalFetch;}
});

test('retained export part completes only after its Blob download starts', async () => {
    const originalFetch = global.fetch;
    const originalCreateObjectURL = URL.createObjectURL;
    const originalWindow = global.window;
    let resolveFetch;
    global.fetch = () => new Promise(resolve => {resolveFetch = resolve;});
    URL.createObjectURL = () => 'blob:part-1';
    global.window = {setTimeout: () => 1};
    try {
        const run = runPartResponse({payload: {payloadRef: {id: 'payload-1', url: '/v1/runtime/payload/payload-1'}}});
        assert.equal(run.modal.completedParts.has('part-1'), false);
        resolveFetch(new Response('region,value\nNorth,7\n', {status: 200}));
        await settle();
        assert.equal(run.modal.processingPartId, null);
        assert.equal(run.modal.completedParts.has('part-1'), true);
        assert.deepEqual(run.downloads, [['blob:part-1', 'part.csv']]);
        assert.deepEqual(run.notifications, [['Downloaded 7 rows', 'success']]);
    } finally {
        global.fetch = originalFetch;
        URL.createObjectURL = originalCreateObjectURL;
        global.window = originalWindow;
    }
});

test('copy part is not complete when clipboard write rejects the payload', async () => {
    const run = runPartResponse({copyIntent: true, payload: {data: btoa('region,value\nNorth,7\n'), format: 'csv'}, copyRichToClipboard: async () => false});
    await settle();
    assert.equal(run.modal.processingPartId, null);
    assert.equal(run.modal.completedParts.has('part-1'), false);
});

test('malformed inline payload clears the pending part instead of throwing or completing it', () => {
    const run = runPartResponse({payload: {data: '%%%not-base64%%%', format: 'csv'}});
    assert.equal(run.modal.processingPartId, null);
    assert.equal(run.modal.completedParts.has('part-1'), false);
    assert.deepEqual(run.downloads, []);
    assert.deepEqual(run.notifications, [['Export failed', 'error']]);
});

test('malformed inline copy payload clears the pending part without a completed check', async () => {
    const run = runPartResponse({copyIntent: true, payload: {data: '%%%not-base64%%%', format: 'html'}});
    await settle();
    assert.equal(run.modal.processingPartId, null);
    assert.equal(run.modal.completedParts.has('part-1'), false);
    assert.deepEqual(run.downloads, []);
    assert.deepEqual(run.notifications, [['Copy failed', 'error']]);
});

test('cancelled retained part cannot start a download or report success after fetch settles', async () => {
    const originalFetch = global.fetch;
    const originalCreateObjectURL = URL.createObjectURL;
    const originalWindow = global.window;
    let resolveFetch;
    global.fetch = () => new Promise(resolve => {resolveFetch = resolve;});
    URL.createObjectURL = () => 'blob:part-1';
    global.window = {setTimeout: () => 1};
    try {
        const run = runPartResponse({payload: {payloadRef: {id: 'payload-1', url: '/v1/runtime/payload/payload-1'}}});
        run.cancel();
        resolveFetch(new Response('region,value\nNorth,7\n', {status: 200}));
        await settle();
        assert.deepEqual(run.downloads, []);
        assert.deepEqual(run.notifications, []);
    } finally {
        global.fetch = originalFetch;
        URL.createObjectURL = originalCreateObjectURL;
        global.window = originalWindow;
    }
});

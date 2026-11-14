const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const frontend = path.resolve(__dirname, '../serverside_pivot');
const localRequire = createRequire(path.join(frontend, 'package.json'));
const babel = localRequire('@babel/core');

function modules() {
    const cache = new Map();
    function load(filename) {
        filename = ['', '.js', '.ts', '.tsx'].map(ext => filename + ext).find(file => fs.existsSync(file));
        if (cache.has(filename)) return cache.get(filename).exports;
        const mod = {exports: {}};
        cache.set(filename, mod);
        const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename, presets: [
            [localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
            localRequire('@babel/preset-typescript'), localRequire('@babel/preset-react'),
        ]}).code;
        const requireSource = name => name === 'react'
            ? {useEffect: effect => effect(), useRef: initial => ({current: initial})}
            : name.startsWith('.') ? load(path.resolve(path.dirname(filename), name)) : localRequire(name);
        new Function('module', 'exports', 'require', code)(mod, mod.exports, requireSource);
        return mod.exports;
    }
    return name => load(path.join(frontend, 'src/lib/runtime', name));
}

function harness({partModal = null, payloadTransport, beforeDispatch} = {}) {
    const load = modules();
    const requests = [], downloads = [], notices = [];
    const state = {progress: null, exporting: false, modal: partModal};
    const ref = {current: null};
    const opts = {
        abortGenerationRef: {current: 7}, activeExportRequestIdRef: {current: null},
        cancelledExportRequestIdsRef: {current: new Set()}, clientInstanceRef: {current: 'browser'},
        exportRequestSequenceRef: {current: 0}, exportJobControllerRef: ref,
        pendingCopyIntentRef: {current: null}, buildExportStyleProfile: () => ({}),
        getServerExportColumnTarget: () => ({colIds: ['region', 'sales'], totalCols: 2}),
        dispatchServerSideRuntimeSetProps: update => {beforeDispatch?.(update); requests.push(update.runtimeRequest);},
        COPY_ROW_LIMIT: 10, EXPORT_ROW_LIMIT: 100, rowCount: 8, tableName: 'sales',
        layoutMode: 'outline', serverSide: true, partModal, payloadTransport,
        setExportProgress: value => {state.progress = value;},
        setIsExporting: value => {state.exporting = value;}, setIsCopying: () => {},
        setPartModal: value => {state.modal = typeof value === 'function' ? value(state.modal) : value;},
    };
    const commands = load('createRuntimeExportCommands').createRuntimeExportCommands(opts);
    const handled = {current: new Set()};
    const respond = (payload, request = requests.at(-1), status = 'export_job') => {
        load('useRuntimeResponseLifecycle').useRuntimeResponseLifecycle({
            ...opts, runtimeResponse: {schemaVersion: 1, kind: 'export', requestId: request.requestId, status, payload},
            handledRuntimeResponseKeysRef: handled, recordProfilerResponse: () => {},
            showNotification: (...args) => notices.push(args), triggerExportDownload: (...args) => downloads.push(args),
        });
    };
    const snapshot = (phase, extra = {}) => ({jobId: 'job-1', phase, rowsWritten: 0, bytesWritten: 0, totalRows: null, ...extra});
    return {commands, respond, snapshot, requests, downloads, notices, state, ref, opts};
}

const settle = () => new Promise(resolve => setImmediate(resolve));
function shellModule() {
    const filename = path.join(frontend, 'src/lib/components/PivotWorkspaceShell.js');
    const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
        presets: [[localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}], localRequire('@babel/preset-react')]}).code;
    const mod = {exports: {}};
    const empty = () => null;
    const imports = new Proxy({__esModule: true, default: empty}, {get: (target, name) => target[name] || empty});
    new Function('module', 'exports', 'require', code)(mod, mod.exports, name => name === 'react' ? localRequire('react') : imports);
    return mod.exports;
}
function elements(tree) {
    if (!tree || typeof tree !== 'object') return [];
    return [tree, ...localRequire('react').Children.toArray(tree.props?.children).flatMap(elements)];
}
function fakeTimers() {
    const originalSet = global.setTimeout, originalClear = global.clearTimeout;
    const pending = new Map(); let id = 0;
    global.setTimeout = (callback, ms) => {pending.set(++id, {callback, ms}); return id;};
    global.clearTimeout = key => pending.delete(key);
    return {pending, run(ms) {const entry = [...pending].find(([, value]) => value.ms === ms); assert.ok(entry, `timer ${ms}`); pending.delete(entry[0]); entry[1].callback();}, restore() {global.setTimeout = originalSet; global.clearTimeout = originalClear;}};
}

test('shared commands send V1 start/status/cancel via the existing dispatch port', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        assert.equal(typeof h.commands.startExportJob, 'function');
        h.commands.startExportJob('csv', {allRows: false});
        assert.equal(h.requests[0].kind, 'export');
        assert.equal(h.requests[0].schemaVersion, 1);
        assert.equal(h.requests[0].payload.action, 'start');
        assert.equal(h.requests[0].payload.table, 'sales');
        assert.equal(h.requests[0].payload.allRows, false);
        assert.deepEqual(h.requests[0].payload.colIds, ['region', 'sales']);
        h.respond(h.snapshot('queued'));
        h.commands.pollExportJob('job-1');
        assert.deepEqual(h.requests.at(-1).payload, {action: 'status', jobId: 'job-1', table: 'sales'});
        h.commands.cancelExportJob('job-1');
        assert.deepEqual(h.requests.at(-1).payload, {action: 'cancel', jobId: 'job-1', table: 'sales'});
        assert.equal(h.state.progress.phase, 'cancelling');
        assert.equal(h.state.exporting, true);
        h.respond(h.snapshot('cancelling'));
        assert.equal(h.state.progress.phase, 'cancelling');
        timers.run(500);
        h.respond(h.snapshot('cancelled'));
        assert.equal(h.state.progress.phase, 'cancelled');
        assert.equal(h.state.exporting, false);
        assert.deepEqual(h.downloads, []);
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('measured progress cannot decrease and unknown total stays indeterminate', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot('csv');
        h.respond(h.snapshot('running', {rowsWritten: 8, bytesWritten: 120}));
        assert.equal(h.state.progress.percent, null);
        h.commands.pollExportJob('job-1');
        h.respond(h.snapshot('queued', {rowsWritten: 2, bytesWritten: 30}));
        assert.equal(h.state.progress.phase, 'running');
        assert.equal(h.state.progress.rowsWritten, 8);
        assert.equal(h.state.progress.bytesWritten, 120);
        h.commands.pollExportJob('job-1');
        h.respond(h.snapshot('running', {rowsWritten: 9, totalRows: 10}));
        assert.equal(h.state.progress.percent, 90);
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('cancel before start acknowledgement sends server cancellation once identity arrives', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot(); h.commands.cancelActiveExportRequest();
        assert.equal(h.state.progress.phase, 'cancelling');
        h.respond(h.snapshot('queued'));
        assert.equal(h.requests.at(-1).payload.action, 'cancel');
        assert.equal(h.state.progress.phase, 'cancelling');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('lost start acknowledgement replays one logical start and cancels its recovered job', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot('csv');
        const first = h.requests.at(-1);
        timers.run(15000);
        assert.equal(h.state.progress.phase, 'queued', 'a lost response does not prove server interruption');
        assert.equal(h.state.exporting, true);
        const replay = h.requests.at(-1);
        assert.equal(replay.payload.action, 'start');
        assert.equal(replay.requestId, first.requestId, 'recovery must identify the original server job');
        h.commands.cancelActiveExportRequest();
        assert.equal(h.state.progress.phase, 'cancelling');
        h.respond(h.snapshot('running'), replay);
        assert.equal(h.requests.at(-1).payload.action, 'cancel');
        h.respond(h.snapshot('cancelled'));
        assert.equal(h.state.progress.phase, 'cancelled');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('lost status response keeps polling the known job and blocks duplicate starts', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('running'));
        h.commands.pollExportJob('job-1');
        timers.run(15000);
        assert.equal(h.state.progress.phase, 'running');
        const count = h.requests.length;
        h.commands.startExportJob('xlsx');
        assert.equal(h.requests.length, count, 'an active job cannot be silently superseded');
        timers.run(500);
        assert.equal(h.requests.at(-1).payload.action, 'status');
        assert.equal(h.requests.at(-1).payload.jobId, 'job-1');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('completed status arriving after the polling timeout still delivers once', async () => {
    const timers = fakeTimers(); const h = harness();
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = async () => new Response('done'); URL.createObjectURL = () => 'blob:late';
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('running'));
        h.commands.pollExportJob('job-1'); const status = h.requests.at(-1);
        timers.run(15000);
        h.respond(h.snapshot('completed', {rowsWritten: 8, payloadRef: {url: '/late', filename: 'late.csv'}}), status);
        await settle();
        assert.equal(h.state.progress.phase, 'completed');
        assert.deepEqual(h.downloads, [['blob:late', 'late.csv']]);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('replayed start can advance from running to completed under one request identity', async () => {
    const timers = fakeTimers(); const h = harness();
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = async () => new Response('done'); URL.createObjectURL = () => 'blob:replay';
    try {
        h.commands.exportPivot('csv'); const start = h.requests.at(-1);
        h.respond(h.snapshot('running'), start);
        h.respond(h.snapshot('completed', {rowsWritten: 8, payloadRef: {url: '/done', filename: 'done.csv'}}), start);
        await settle();
        assert.equal(h.state.progress.phase, 'completed');
        assert.deepEqual(h.downloads, [['blob:replay', 'done.csv']]);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('stale completed poll cannot download while cancellation acknowledgement is pending', async () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('running'));
        h.commands.pollExportJob('job-1'); const poll = h.requests.at(-1);
        h.commands.cancelActiveExportRequest(); const cancel = h.requests.at(-1);
        h.respond(h.snapshot('completed', {payloadRef: {url: '/old'}}), poll);
        assert.equal(h.state.progress.phase, 'cancelling');
        h.respond(h.snapshot('cancelled'), cancel); await settle();
        assert.equal(h.state.progress.phase, 'cancelled'); assert.deepEqual(h.downloads, []);
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('completion winning cancel race downloads once from acknowledged completed reference', async () => {
    const timers = fakeTimers(); const h = harness();
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = async () => new Response('region,sales\nNorth,8\n'); URL.createObjectURL = () => 'blob:complete';
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('running'));
        h.commands.cancelActiveExportRequest();
        const cancel = h.requests.at(-1);
        const completed = h.snapshot('completed', {rowsWritten: 8, payloadRef: {url: '/done', filename: 'sales.csv'}});
        h.respond(completed, cancel); h.respond(completed, cancel); await settle();
        assert.deepEqual(h.downloads, [['blob:complete', 'sales.csv']]);
        assert.equal(h.state.progress.phase, 'completed');
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('interrupted jobs retry fresh and missing in-flight status becomes interrupted', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('running'));
        h.commands.pollExportJob('job-1');
        h.respond({code: 'export_job_not_found'}, undefined, 'error');
        assert.equal(h.state.progress.phase, 'interrupted');
        h.commands.retryExportJob();
        assert.equal(h.requests.at(-1).payload.action, 'start');
        assert.equal(h.requests.at(-1).payload.format, 'csv');
        assert.equal(h.state.progress.phase, 'queued');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('retry and unmount abort download and clear poll/request timers; late responses do nothing', async () => {
    const timers = fakeTimers(); const h = harness();
    const originalFetch = global.fetch; let finishFetch, signal;
    global.fetch = (_url, options) => {signal = options.signal; return new Promise(resolve => {finishFetch = resolve;});};
    try {
        h.commands.exportPivot('csv');
        const oldRequest = h.requests.at(-1);
        h.respond(h.snapshot('completed', {payloadRef: {url: '/old'}}));
        h.commands.startExportJob('csv', {allRows: true});
        assert.equal(signal.aborted, true);
        finishFetch(new Response('old')); await settle();
        h.respond(h.snapshot('completed', {payloadRef: {url: '/old'}}), oldRequest);
        assert.deepEqual(h.downloads, []); assert.equal(h.state.progress.phase, 'queued');
        h.ref.current.dispose();
        assert.equal(timers.pending.size, 0);
        h.respond(h.snapshot('running', {jobId: 'job-new'}));
        assert.equal(timers.pending.size, 0);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch;}
});

test('part file jobs retain row bounds and complete the selected part only after download', async () => {
    const timers = fakeTimers();
    const h = harness({partModal: {mode: 'export', rowsPerPart: 3, colsPerPart: 2, totalRows: 8,
        totalCols: 2, visibleColIds: ['region', 'sales'], exportFormat: 'csv', completedParts: new Set()}});
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = async () => new Response('a,b'); URL.createObjectURL = () => 'blob:part';
    try {
        h.commands.dispatchExportPart(1, 0);
        assert.equal(h.requests.at(-1).payload.action, 'start');
        assert.equal(h.requests.at(-1).payload.rowStart, 3);
        assert.equal(h.requests.at(-1).payload.rowEnd, 6);
        assert.equal(h.requests.at(-1).payload.allRows, false);
        h.respond(h.snapshot('completed', {payloadRef: {url: '/part'}}));
        assert.equal(h.state.modal.completedParts.size, 0); await settle();
        assert.equal(h.state.modal.completedParts.has('r1_c0'), true);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('overlay renders measured progress, indeterminate totals and acknowledged terminal controls', () => {
    const filename = path.join(frontend, 'src/lib/components/PivotWorkspaceShell.js');
    const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
        presets: [[localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}], localRequire('@babel/preset-react')]}).code;
    const mod = {exports: {}};
    new Function('module', 'exports', 'require', code)(mod, mod.exports, name => name === 'react' ? localRequire('react') : {});
    const React = localRequire('react');
    const render = progress => localRequire('react-dom/server').renderToStaticMarkup(React.createElement(mod.exports.ExportDeliveryOverlay, {
        exportProgress: progress, isCopying: false, theme: {}, cancelActiveExportRequest: () => {},
        retryExportJob: () => {}, dismissExportJob: () => {},
    }));
    const running = render({phase: 'running', rowsWritten: 42, bytesWritten: 1024, totalRows: null, percent: null});
    assert.match(running, /Running/); assert.match(running, /42 rows/);
    assert.doesNotMatch(running, /aria-valuenow|42%/);
    const known = render({phase: 'running', rowsWritten: 42, bytesWritten: 1024, totalRows: 100, percent: 42});
    assert.match(known, /aria-valuenow="42"/);
    const cancelling = render({phase: 'cancelling', rowsWritten: 42, bytesWritten: 1024, totalRows: null, percent: null});
    assert.match(cancelling, /Cancelling/); assert.match(cancelling, /disabled=""/); assert.doesNotMatch(cancelling, /Cancelled/);
    const interrupted = render({phase: 'interrupted', rowsWritten: 42, bytesWritten: 1024, totalRows: null, percent: null});
    assert.match(interrupted, /Interrupted/); assert.match(interrupted, /Retry/);
});

test('workspace projection exposes retry and close actions to the rendered overlay', () => {
    const load = modules(); let retried = 0, dismissed = 0;
    const presentation = load('createWorkspacePresentation').createWorkspacePresentation({
        retryExportJob: () => {retried++;}, dismissExportJob: () => {dismissed++;},
    });
    assert.equal(typeof presentation.retryExportJob, 'function');
    assert.equal(typeof presentation.dismissExportJob, 'function');
    presentation.retryExportJob(); presentation.dismissExportJob();
    assert.equal(retried, 1); assert.equal(dismissed, 1);
});

test('lost cancellation acknowledgement keeps cancelling and polls until server termination', () => {
    const timers = fakeTimers(); const h = harness();
    try {
        h.commands.exportPivot(); h.respond(h.snapshot('running'));
        h.commands.cancelActiveExportRequest(); timers.run(15000);
        assert.equal(h.state.progress.phase, 'cancelling');
        assert.equal(h.state.exporting, true);
        timers.run(500); h.respond(h.snapshot('cancelled'));
        assert.equal(h.state.progress.phase, 'cancelled');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('failed part retry reselects its processing part and completes after the new download', async () => {
    const timers = fakeTimers();
    const h = harness({partModal: {mode: 'export', rowsPerPart: 3, colsPerPart: 2, totalRows: 8,
        totalCols: 2, visibleColIds: ['region', 'sales'], exportFormat: 'csv', completedParts: new Set()}});
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = async () => new Response('a,b'); URL.createObjectURL = () => 'blob:retry';
    try {
        h.commands.dispatchExportPart(1, 0); h.respond(h.snapshot('failed'));
        assert.equal(h.state.modal.processingPartId, null);
        h.commands.retryExportJob();
        assert.equal(h.state.modal.processingPartId, 'r1_c0');
        h.respond(h.snapshot('completed', {jobId: 'job-2', payloadRef: {url: '/retry'}})); await settle();
        assert.equal(h.state.modal.completedParts.has('r1_c0'), true);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('completed job downloads through configured HTTP origin, auth headers and abort signal', async () => {
    const timers = fakeTimers(); const load = modules(); const calls = [];
    const {HttpPivotTransport} = load('httpTransport');
    const transport = new HttpPivotTransport({endpoint: 'https://runtime.example/v1/runtime',
        headers: {Authorization: 'Bearer fixture-token', 'X-Principal': 'alice'},
        fetchImpl: async (url, options) => {calls.push({url, options}); return new Response('region,sales\nNorth,8\n');}});
    const h = harness({payloadTransport: transport});
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = () => {throw new Error('Configured export must not use bare fetch');};
    URL.createObjectURL = () => 'blob:authenticated';
    try {
        h.commands.exportPivot('csv');
        h.respond(h.snapshot('completed', {rowsWritten: 8, payloadRef: {id: 'token', url: '/v1/runtime/payload/token', filename: 'sales.csv'}}));
        await settle();
        assert.deepEqual(h.downloads, [['blob:authenticated', 'sales.csv']]);
        assert.equal(calls[0].url, 'https://runtime.example/v1/runtime/payload/token');
        assert.equal(calls[0].options.headers.Authorization, 'Bearer fixture-token');
        assert.equal(calls[0].options.headers['X-Principal'], 'alice');
        assert.equal(calls[0].options.credentials, 'same-origin');
        assert.equal(calls[0].options.signal.aborted, false);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('configured binary delivery is aborted on retry and cannot download after it resolves late', async () => {
    const timers = fakeTimers(); const load = modules(); let release, signal;
    const {HttpPivotTransport} = load('httpTransport');
    const transport = new HttpPivotTransport({endpoint: 'https://runtime.example/v1/runtime',
        headers: {Authorization: 'Bearer fixture-token'}, fetchImpl: (_url, options) => {
            signal = options.signal; return new Promise(resolve => {release = resolve;});
        }});
    const h = harness({payloadTransport: transport});
    try {
        h.commands.exportPivot('csv'); h.respond(h.snapshot('completed', {payloadRef: {id: 'old', url: '/old'}}));
        assert.ok(signal instanceof AbortSignal);
        h.commands.startExportJob('csv'); assert.equal(signal.aborted, true);
        release(new Response('old')); await settle();
        assert.deepEqual(h.downloads, []); assert.equal(h.state.progress.phase, 'queued');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

test('part job progress and cancel controls render above the retained part selector', () => {
    const shell = shellModule();
    const progress = {phase: 'running', rowsWritten: 2, bytesWritten: 10, totalRows: null, percent: null};
    const tree = shell.PivotWorkspaceShell({workspace: {presentation: {
        styles: {}, theme: {}, uiConfig: {}, comparisonValueState: {activeCount: 0}, chartCanvasPanes: [],
        isExporting: true, exportProgress: progress, EXPORT_ROW_LIMIT: 100,
        partModal: {mode: 'export', rowsPerPart: 3, colsPerPart: 2, totalRows: 8, totalCols: 2, processingPartId: 'r1_c0', completedParts: new Set()},
    }}});
    const all = elements(tree);
    const selector = all.find(node => node.props?.style?.position === 'fixed' && elements(node).some(child => child.type === 'input'));
    const overlayComponent = all.find(node => node.type === shell.ExportDeliveryOverlay);
    assert.ok(selector); assert.ok(overlayComponent);
    const overlay = shell.ExportDeliveryOverlay(overlayComponent.props);
    assert.ok(overlay.props.style.zIndex > selector.props.style.zIndex, 'job overlay must receive pointer events above part selection');
    const cancel = elements(overlay).find(node => node.type === 'button');
    assert.equal(cancel.props.disabled, false);
});

test('server completion remains delivering until the file downloads and Close cannot discard it', async () => {
    const timers = fakeTimers(); const h = harness(); let release, signal;
    const originalFetch = global.fetch, originalCreate = URL.createObjectURL;
    global.fetch = (_url, options) => {signal = options.signal; return new Promise(resolve => {release = resolve;});};
    URL.createObjectURL = () => 'blob:delivered';
    try {
        h.commands.exportPivot('csv');
        h.respond(h.snapshot('completed', {payloadRef: {id: 'file', url: '/file', filename: 'part.csv'}}));
        assert.equal(h.state.progress.phase, 'completed');
        assert.equal(h.state.progress.delivery, 'pending');
        assert.equal(h.state.exporting, true);
        assert.deepEqual(h.notices, [], 'server completion cannot announce download success before binary delivery');
        const overlay = shellModule().ExportDeliveryOverlay({exportProgress: h.state.progress, theme: {}, dismissExportJob: h.commands.dismissExportJob});
        const buttons = elements(overlay).filter(node => node.type === 'button');
        assert.ok(buttons.every(button => button.props.disabled), 'delivery does not expose Close or Cancel');
        h.commands.dismissExportJob();
        assert.equal(signal.aborted, false);
        release(new Response('a,b')); await settle();
        assert.deepEqual(h.downloads, [['blob:delivered', 'part.csv']]);
        assert.deepEqual(h.notices, [['Downloaded 0 rows', 'success']]);
        assert.equal(h.state.progress.delivery, 'delivered');
        assert.equal(h.state.exporting, false);
        h.commands.dismissExportJob(); assert.equal(h.state.progress, null);
    } finally {h.ref.current?.dispose(); timers.restore(); global.fetch = originalFetch; URL.createObjectURL = originalCreate;}
});

test('lost cancel retries the idempotent cancel when status still reports queued or running', () => {
    for (const phase of ['queued', 'running']) {
        const timers = fakeTimers(); const h = harness();
        try {
            h.commands.exportPivot(); h.respond(h.snapshot(phase));
            h.commands.cancelActiveExportRequest();
            h.respond({code: 'transport_unavailable'}, undefined, 'error');
            timers.run(500); h.respond(h.snapshot(phase));
            assert.equal(h.requests.at(-1).payload.action, 'cancel');
            assert.equal(h.requests.filter(request => request.payload.action === 'cancel').length, 2);
            assert.equal(h.state.progress.phase, 'cancelling');
            h.respond(h.snapshot('cancelled'));
            assert.equal(h.state.progress.phase, 'cancelled');
            assert.deepEqual(h.downloads, []);
        } finally {h.ref.current?.dispose(); timers.restore();}
    }
});

test('a synchronous cancel dispatch failure keeps the job cancellable and retries after status', () => {
    const timers = fakeTimers(); let attempts = 0;
    const h = harness({beforeDispatch: update => {
        if (update.runtimeRequest.payload.action === 'cancel' && ++attempts === 1) throw new Error('host dispatch failed');
    }});
    try {
        h.commands.exportPivot(); h.respond(h.snapshot('running'));
        assert.doesNotThrow(() => h.commands.cancelActiveExportRequest());
        assert.equal(h.state.progress.phase, 'cancelling');
        timers.run(500); h.respond(h.snapshot('running'));
        assert.equal(attempts, 2);
        assert.equal(h.requests.at(-1).payload.action, 'cancel');
        h.respond(h.snapshot('cancelled')); assert.equal(h.state.progress.phase, 'cancelled');
    } finally {h.ref.current?.dispose(); timers.restore();}
});

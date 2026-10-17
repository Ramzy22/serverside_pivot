const assert = require('node:assert/strict');
const {test} = require('node:test');
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');

const repoRoot = path.resolve(__dirname, '..');
const frontendRoot = path.join(repoRoot, 'dash_tanstack_pivot');
const babel = require(path.join(frontendRoot, 'node_modules', '@babel', 'core'));
const sourcePath = path.join(frontendRoot, 'src/lib/runtime/useRuntimeColumnMeasurement.js');
const hookSource = babel.transformSync(fs.readFileSync(sourcePath, 'utf8'), {
    filename: sourcePath,
    presets: [[require(path.join(frontendRoot, 'node_modules', '@babel', 'preset-env')), {
        targets: {chrome: '120'}, modules: 'commonjs',
    }]],
}).code;

function chromePath() {
    const candidates = process.platform === 'win32'
        ? ['C:/Program Files/Google/Chrome/Application/chrome.exe', 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe']
        : ['/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser'];
    return [process.env.CHROME_BIN, process.env.CHROMIUM_PATH, ...candidates]
        .find(candidate => candidate && fs.existsSync(candidate));
}

function removeTempDir(directory) {
    const resolved = path.resolve(directory);
    const tempRoot = path.resolve(os.tmpdir());
    assert.equal(path.dirname(resolved), tempRoot);
    assert.match(path.basename(resolved), /^pivot-shadow-measurement-/);
    fs.rmSync(resolved, {recursive: true, force: true});
}

async function availablePort() {
    const server = net.createServer();
    await new Promise((resolve, reject) => server.listen(0, '127.0.0.1', error => error ? reject(error) : resolve()));
    const port = server.address().port;
    await new Promise(resolve => server.close(resolve));
    return port;
}

async function browserSession() {
    const binary = chromePath();
    if (!binary) return null;
    const port = await availablePort();
    const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pivot-shadow-measurement-'));
    const chrome = spawn(binary, [
        '--headless=new', '--no-first-run', '--no-default-browser-check', '--disable-gpu',
        '--disable-extensions', `--remote-debugging-port=${port}`, `--user-data-dir=${userDataDir}`,
        'about:blank',
    ], {stdio: 'ignore'});
    let target;
    for (let attempt = 0; attempt < 100; attempt++) {
        if (chrome.exitCode !== null) break;
        try {
            const response = await fetch(`http://127.0.0.1:${port}/json`);
            target = (await response.json()).find(item => item.type === 'page');
            if (target) break;
        } catch {}
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    if (!target) {
        chrome.kill();
        removeTempDir(userDataDir);
        throw new Error('Chrome DevTools page was unavailable');
    }
    const socket = new WebSocket(target.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
        socket.addEventListener('open', resolve, {once: true});
        socket.addEventListener('error', reject, {once: true});
    });
    let nextId = 0;
    const pending = new Map();
    socket.addEventListener('message', event => {
        const message = JSON.parse(event.data);
        const request = pending.get(message.id);
        if (!request) return;
        pending.delete(message.id);
        if (message.error) request.reject(new Error(message.error.message));
        else request.resolve(message.result);
    });
    return {
        async evaluate(expression) {
            const id = ++nextId;
            const response = new Promise((resolve, reject) => pending.set(id, {resolve, reject}));
            socket.send(JSON.stringify({id, method: 'Runtime.evaluate', params: {
                expression, awaitPromise: true, returnByValue: true,
            }}));
            const result = await response;
            if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
            return result.result.value;
        },
        async close() {
            socket.close();
            chrome.kill();
            await new Promise(resolve => chrome.once('exit', resolve));
            removeTempDir(userDataDir);
        },
    };
}

test('measures and corrects a column rendered inside an open ShadowRoot', async t => {
    const session = await browserSession();
    if (!session) return t.skip('Chrome or Chromium is required for real ShadowRoot measurement');
    t.after(() => session.close());
    const result = await session.evaluate(`(async () => {
        const host = document.createElement('div');
        document.body.appendChild(host);
        const decoy = document.createElement('div');
        decoy.setAttribute('role', 'columnheader');
        decoy.dataset.headerColumnId = 'region';
        decoy.innerHTML = '<span data-header-content="true"><span data-header-text="true" ' +
            'style="font: 60px monospace">An unrelated grid has a much longer heading</span></span>';
        document.body.appendChild(decoy);
        const shadow = host.attachShadow({mode: 'open'});
        shadow.innerHTML = '<style>[data-header-text="true"] { font: 30px monospace; white-space: nowrap; }</style>' +
            '<div id="pivot-grid"><div role="columnheader" data-header-column-id="region" style="padding: 0 8px; border: 1px solid black; width:140px; box-sizing:border-box">' +
            '<span data-header-content="true" style="display:flex; gap:4px; padding:0 8px; overflow:hidden; width:100%; box-sizing:border-box"><span data-header-text="true" style="overflow:hidden; text-overflow:ellipsis; min-width:0; flex:1">Shadow Heading</span><span style="width:32px;flex:none">⋮</span></span></div>' +
            '<div role="gridcell" data-colid="region" style="padding: 0 8px"><span style="font: 30px monospace; white-space: nowrap">WIDE CELL</span></div></div>';
        const grid = shadow.querySelector('#pivot-grid');
        const module = {exports: {}};
        new Function('module', 'exports', 'require', ${JSON.stringify(hookSource)})(
            module, module.exports, name => name === 'react'
                ? {useCallback: callback => callback}
                : (() => {throw new Error('Unexpected dependency: ' + name)})()
        );
        let correctedWidth = null;
        const table = {
            getColumn: () => ({getSize: () => 80}),
            setColumnSizing: update => {correctedWidth = update({}).region;},
        };
        const measurement = module.exports.useRuntimeColumnMeasurement({
            id: 'pivot-grid', parentRef: {current: grid}, table,
            autoSizeBounds: {minWidth: 40, maxWidth: 500, headerOverscan: 4, cellOverscan: 4},
        });
        const headerWidth = measurement.getRenderedHeaderWidthForColumn('region');
        const header = shadow.querySelector('[role="columnheader"]');
        const content = header.querySelector('[data-header-content]');
        const label = header.querySelector('[data-header-text]');
        const chrome = element => ['paddingLeft','paddingRight','borderLeftWidth','borderRightWidth']
            .reduce((sum,key) => sum + (parseFloat(getComputedStyle(element)[key]) || 0), 0);
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        context.font = getComputedStyle(label).font;
        const controls = [...content.children].filter(child => child !== label)
            .reduce((sum,child) => sum + child.getBoundingClientRect().width, 0);
        const requiredWidth = Math.ceil(context.measureText(label.innerText.trim()).width + controls
            + chrome(header) + chrome(content) + chrome(label));
        const cellWidth = measurement.getRenderedCellWidthForColumn('region');
        measurement.queueHeaderFitValidation('region', 80);
        await new Promise(resolve => setTimeout(resolve, 100));
        return {headerWidth, cellWidth, correctedWidth, requiredWidth};
    })()`);
    assert.ok(result.headerWidth > 230, `shadow-styled heading should exceed 230px: ${JSON.stringify(result)}`);
    assert.ok(result.headerWidth < 400, `unrelated document header must not affect width: ${JSON.stringify(result)}`);
    assert.ok(result.headerWidth >= result.requiredWidth, `shadow header chrome and controls must fit: ${JSON.stringify(result)}`);
    assert.ok(result.cellWidth > 100, `rendered cell should be measured: ${JSON.stringify(result)}`);
    assert.ok(result.correctedWidth >= result.headerWidth, `queued correction should fit heading: ${JSON.stringify(result)}`);
});

test('keeps the document-scoped Dash and React measurement path', async t => {
    const session = await browserSession();
    if (!session) return t.skip('Chrome or Chromium is required for DOM measurement');
    t.after(() => session.close());
    const result = await session.evaluate(`(() => {
        document.body.innerHTML = '<div id="pivot-grid"><div role="columnheader" data-header-column-id="region" ' +
            'style="padding: 0 8px"><span data-header-content="true"><span data-header-text="true" ' +
            'style="font: 24px monospace; white-space: nowrap">Document Heading</span></span></div>' +
            '<div role="gridcell" data-colid="region" style="padding: 0 8px"><span ' +
            'style="font: 24px monospace; white-space: nowrap">DOCUMENT CELL</span></div></div>';
        const module = {exports: {}};
        new Function('module', 'exports', 'require', ${JSON.stringify(hookSource)})(
            module, module.exports, name => name === 'react'
                ? {useCallback: callback => callback}
                : (() => {throw new Error('Unexpected dependency: ' + name)})()
        );
        const measurement = module.exports.useRuntimeColumnMeasurement({
            id: 'pivot-grid', parentRef: {current: null}, table: null,
            autoSizeBounds: {minWidth: 40, maxWidth: 500, headerOverscan: 4, cellOverscan: 4},
        });
        return {
            headerWidth: measurement.getRenderedHeaderWidthForColumn('region'),
            cellWidth: measurement.getRenderedCellWidthForColumn('region'),
        };
    })()`);
    assert.ok(result.headerWidth > 200, `document heading should be measured: ${JSON.stringify(result)}`);
    assert.ok(result.cellWidth > 150, `document cell should be measured: ${JSON.stringify(result)}`);
});

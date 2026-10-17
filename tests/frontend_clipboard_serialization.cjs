const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');

const frontend = path.resolve(__dirname, '../dash_tanstack_pivot');
const localRequire = createRequire(path.join(frontend, 'package.json'));
const babel = localRequire('@babel/core');
const sourceCache = new Map();
function loadSource(filename) {
    if (sourceCache.has(filename)) return sourceCache.get(filename).exports;
    const sourceModule = {exports: {}};
    sourceCache.set(filename, sourceModule);
    const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {
        filename,
        presets: [
            [localRequire('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}],
            localRequire('@babel/preset-react'),
        ],
    }).code;
    const requireSource = specifier => specifier.startsWith('.')
        ? loadSource(path.resolve(path.dirname(filename), `${specifier}.js`))
        : localRequire(specifier);
    new Function('module', 'exports', 'require', code)(sourceModule, sourceModule.exports, requireSource);
    return sourceModule.exports;
}
const {buildStyledHtmlTableExport, escapeTsv} = loadSource(path.join(frontend, 'src/lib/utils/exportUtils.js'));
const {useRuntimeClipboardExportPresentation} = loadSource(path.join(frontend, 'src/lib/runtime/useRuntimeClipboardExportPresentation.js'));
const {formatDisplayLabel} = loadSource(path.join(frontend, 'src/lib/utils/helpers.js'));
const React = localRequire('react');
const {renderToStaticMarkup} = localRequire('react-dom/server');

function hierarchyPresentation(label, {useId = false} = {}) {
    const columns = [{id: 'hierarchy', columnDef: {header: 'Group'}}, {id: 'neighbor', columnDef: {header: 'Neighbor'}}];
    const original = {depth: 1, neighbor: 'next', ...(useId ? {_id: label} : {_label: label, _id: 'stable-id'})};
    const row = {id: 'row-1', original, getValue: id => id === 'hierarchy' ? formatDisplayLabel(label) : original[id]};
    let presentation;
    function Harness() {
        presentation = useRuntimeClipboardExportPresentation({
            table: {getVisibleLeafColumns: () => columns, getPreExpandedRowModel: () => ({flatRows: [row]})},
            getDisplayRows: () => [row],
            getDisplayHeaderTextForColumn: column => column.columnDef.header,
            getDisplayHeaderTextStackForColumn: column => [column.columnDef.header],
            selectedCells: {'row-1:hierarchy': row.getValue('hierarchy'), 'row-1:neighbor': 'next'},
            theme: {},
            HIERARCHY_INDENT_PX: 16,
        });
        return null;
    }
    renderToStaticMarkup(React.createElement(Harness));
    return {presentation, row, columns};
}

test('TSV quotes fields containing separators or quotes without changing their contents', () => {
    const cases = [
        ['left\tright', '"left\tright"'],
        ['line one\nline two', '"line one\nline two"'],
        ['line one\r\nline two', '"line one\r\nline two"'],
        ['line one\rline two', '"line one\rline two"'],
        ['say "hello"', '"say ""hello"""'],
        ['\t"München"\r\n東京\n', '"\t""München""\r\n東京\n"'],
    ];
    for (const [value, expected] of cases) {
        assert.equal(escapeTsv(value), expected);
    }
});

test('TSV leaves ordinary Unicode, spaces, punctuation, numbers and empty values unchanged', () => {
    const cases = [
        ['plain text', 'plain text'],
        ['  leading and trailing  ', '  leading and trailing  '],
        ['Éclair, München & 東京 🧭', 'Éclair, München & 東京 🧭'],
        ['', ''],
        [null, ''],
        [undefined, ''],
        [0, '0'],
        [false, 'false'],
    ];
    for (const [value, expected] of cases) {
        assert.equal(escapeTsv(value), expected);
    }
});

test('clipboard table text preserves special headers and cells beside ordinary and empty cells', () => {
    const payload = buildStyledHtmlTableExport({
        columns: [
            {id: 'left', columnDef: {header: 'Left'}},
            {id: 'value', columnDef: {header: 'Header\t"quoted"\r\n東京'}},
            {id: 'empty', columnDef: {header: 'Empty'}},
            {id: 'right', columnDef: {header: 'Right'}},
        ],
        rows: [
            {left: 'before', value: 'tabs\there', empty: '', right: 'after'},
            {left: 'Éclair', value: 'line 1\nline 2', empty: null, right: '東京'},
            {left: 'start', value: 'CRLF\r\n"<&>"', right: 'end'},
        ],
    });

    assert.equal(payload.text,
        'Left\t"Header\t""quoted""\r\n東京"\tEmpty\tRight\n'
        + 'before\t"tabs\there"\t\tafter\n'
        + 'Éclair\t"line 1\nline 2"\t\t東京\n'
        + 'start\t"CRLF\r\n""<&>"""\t\tend');
    assert.equal(payload.rowCount, 3);
    assert.equal(payload.columnCount, 4);
    assert.ok(payload.html.includes('>Header\t&quot;quoted&quot;\r\n東京</th>'));
    assert.ok(payload.html.includes('>tabs\there</td>'));
    assert.ok(payload.html.includes('>line 1\nline 2</td>'));
    assert.ok(payload.html.includes('>CRLF\r\n&quot;&lt;&amp;&gt;&quot;</td>'));
});

test('headerless clipboard selections preserve embedded separators and neighboring cells', () => {
    const payload = buildStyledHtmlTableExport({
        columns: [{id: 'first'}, {id: 'second'}, {id: 'third'}],
        rows: [{first: 'left', second: 'one\t"two"\nthree', third: ''}],
        includeHeaders: false,
    });
    assert.equal(payload.text, 'left\t"one\t""two""\nthree"\t');
    assert.equal(payload.html.includes('<th '), false);
});

test('ordinary clipboard table text keeps its existing unquoted TSV layout', () => {
    const payload = buildStyledHtmlTableExport({
        columns: [{id: 'city', columnDef: {header: 'City'}}, {id: 'amount', columnDef: {header: 'Amount'}}],
        rows: [{city: 'München', amount: 0}, {city: '東京', amount: null}],
    });
    assert.equal(payload.text, 'City\tAmount\nMünchen\t0\n東京\t');
});

test('hierarchy copy preserves source control characters through current, all and selected payloads', () => {
    const cases = [
        ['left\tright', '"  left\tright"\tnext', '>  left\tright</td>'],
        ['first\nsecond', '"  first\nsecond"\tnext', '>  first\nsecond</td>'],
        ['first\r\n"<&>"', '"  first\r\n""<&>"""\tnext', '>  first\r\n&quot;&lt;&amp;&gt;&quot;</td>'],
        ['München\r東京', '"  München\r東京"\tnext', '>  München\r東京</td>'],
    ];
    for (const [label, expectedText, expectedHtmlCell] of cases) {
        const {presentation} = hierarchyPresentation(label);
        const payloads = [
            presentation.buildTableClipboardPayload({withHeaders: false}),
            presentation.buildAllRowsClipboardPayload({withHeaders: false}),
            presentation.buildSelectedClipboardPayload(),
        ];
        for (const payload of payloads) {
            assert.equal(payload.text, expectedText);
            assert.ok(payload.html.includes(expectedHtmlCell));
        }
    }
});

test('hierarchy copy retains a control character label supplied only through _id', () => {
    const {presentation} = hierarchyPresentation('left\tright', {useId: true});
    assert.equal(presentation.buildTableClipboardPayload({withHeaders: false}).text, '"  left\tright"\tnext');
    assert.equal(presentation.buildSelectedClipboardPayload().text, '"  left\tright"\tnext');
});

test('ordinary hierarchy labels retain their display formatting and selected overrides', () => {
    const {presentation} = hierarchyPresentation('  ordinary_label  ');
    assert.equal(presentation.buildTableClipboardPayload({withHeaders: false}).text, '  Ordinary Label\tnext');
    assert.equal(presentation.buildSelectedClipboardPayload().text, '  Ordinary Label\tnext');
    assert.equal(presentation.buildSelectedClipboardPayload(false, {'row-1:hierarchy': 'Custom label'}).text, '  Custom label');
});

test('clipboard source recovery does not change the accessors used by file exports', () => {
    const {presentation, row, columns} = hierarchyPresentation('left\tright');
    assert.equal(presentation.getExportRawCellValue(row, columns[0], 0), 'Left Right');
    assert.equal(presentation.getExportCellDisplayValue(row, columns[0], 0), '  Left Right');
    assert.equal(presentation.getExportRawCellValue(row, columns[0], 0, 'Custom label'), 'Custom label');
});

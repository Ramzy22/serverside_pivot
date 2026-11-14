const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');

const frontend = path.resolve(__dirname, '../serverside_pivot');
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


test('hidden leaves in an older selection do not add headers, values or placeholders', () => {
    const visible = [{id:'sales',columnDef:{header:'Sales'}},{id:'units',columnDef:{header:'Units'}}];
    const original = {sales:'100.25',variance:'SECRET-HIDDEN',units:'2.00'};
    const row = {id:'r1',original,getValue:id=>original[id]};
    let presentation;
    function Harness() {
        presentation = useRuntimeClipboardExportPresentation({
            table:{getVisibleLeafColumns:()=>visible},getDisplayRows:()=>[row],
            getDisplayHeaderTextForColumn:column=>column.columnDef.header,
            getDisplayHeaderTextStackForColumn:column=>[column.columnDef.header],
            selectedCells:{'r1:sales':'100.25','r1:variance':'SECRET-HIDDEN','r1:units':'2.00'},theme:{},
        });
        return null;
    }
    renderToStaticMarkup(React.createElement(Harness));
    const body=presentation.buildSelectedClipboardPayload();
    const headers=presentation.buildSelectedClipboardPayload(true);
    assert.equal(body.text,'100.25\t2.00');
    assert.equal(headers.text,'Sales\tUnits\n100.25\t2.00');
    for(const payload of [body,headers]) {
        assert.equal(payload.columnCount,2);
        assert.equal(payload.rowCount,1);
        assert.ok(!payload.html.includes('SECRET-HIDDEN'));
    }
});

test('selected values preserve Unicode codepoints and interior/trailing empty cells before OS delivery', () => {
    for (const text of ['\u00c9clair M\u00fcnchen \u6771\u4eac \u0627\u0644\u0642\u0627\u0647\u0631\u0629 \u{1f680}', 'E\u0301clair Mu\u0308nchen \u6771\u4eac \u0627\u0644\u0642\u0627\u0647\u0631\u0629 \u{1f680}']) {
        const ids = ['copyUnicode_first', 'copyNull_first', 'rowId_first', 'copyEmpty_first'];
        const values = [text, null, 'q2-munchen', ''];
        const original = Object.fromEntries(ids.map((id, index) => [id, values[index]]));
        const row = {id: 'q2-munchen', original, getValue: id => original[id]};
        const columns = ids.map(id => ({id, columnDef:{header:id}}));
        let presentation;
        function Harness() {
            presentation = useRuntimeClipboardExportPresentation({
                table:{getVisibleLeafColumns:()=>columns}, getDisplayRows:()=>[row],
                getDisplayHeaderTextForColumn: column=>column.id,
                getDisplayHeaderTextStackForColumn: column=>[column.id],
                selectedCells:Object.fromEntries(ids.map((id,index)=>['q2-munchen:'+id,values[index]])), theme:{},
            });
            return null;
        }
        renderToStaticMarkup(React.createElement(Harness));
        const payload = presentation.buildSelectedClipboardPayload();
        assert.equal(payload.text, text+'\t\tq2-munchen\t');
        assert.equal(payload.rowCount,1);
        assert.equal(payload.columnCount,4);
    }
});

test('selected merged headers clip ancestry to a partial two-period selection', () => {
    const columns = ['Q1_units','Q1_sales','Q1_variance','Q2_units'].map(id=>({id,columnDef:{header:id.split('_')[1]}}));
    const row={id:'r',original:{},getValue:id=>({Q1_units:2,Q1_sales:100.25,Q1_variance:-12.5,Q2_units:1})[id]};
    let presentation;
    function Harness(){presentation=useRuntimeClipboardExportPresentation({
        table:{getVisibleLeafColumns:()=>columns},getDisplayRows:()=>[row],theme:{},
        getDisplayHeaderTextForColumn:c=>c.columnDef.header,
        getDisplayHeaderTextStackForColumn:c=>[c.id.split('_')[0],c.columnDef.header],
        selectedCells:{'r:Q1_sales':100.25,'r:Q1_variance':-12.5,'r:Q2_units':1},
    });return null;}
    renderToStaticMarkup(React.createElement(Harness));
    const result=presentation.buildSelectedClipboardPayload(true,undefined,{mergedHeaders:true,rawValues:true});
    assert.match(result.html, /colspan="2"[^>]*>Q1<\/th>/);
    assert.match(result.html, />Q2<\/th>/);
    assert.equal((result.html.match(/>units<\/th>/g)||[]).length,1);
    assert.equal(result.columnCount,3);
    assert.equal(result.text.split('\n').at(-1),'100.25\t-12.5\t1');
    assert.deepEqual([...result.html.matchAll(/<th\b[^>]*>(.*?)<\/th>/g)].map(match=>match[1]),
        ['Q1','Q2','sales','variance','units']);
    assert.ok(!presentation.buildSelectedClipboardPayload(true).html.includes('colspan="2"'));
});

test('complete consecutive server selections still produce all logical cells with raw headers',()=>{
    const notices=[];
    const rows=[10,11,12].map(index=>({id:`r${index}`,original:{__virtualIndex:index},getValue:()=>index+.25}));
    let presentation;
    function Harness(){presentation=useRuntimeClipboardExportPresentation({
        table:{getVisibleLeafColumns:()=>[{id:'sales',columnDef:{header:'Sales'}}]},getDisplayRows:()=>rows,
        serverSide:true,theme:{},decimalPlaces:0,getDisplayHeaderTextForColumn:c=>c.columnDef.header,
        showNotification:(...args)=>notices.push(args),
        selectedCells:Object.fromEntries(rows.map(row=>[`${row.id}:sales`,row.getValue()])),
    });return null;}
    renderToStaticMarkup(React.createElement(Harness));
    assert.equal(presentation.buildSelectedClipboardPayload(true,undefined,{rawValues:true}).text,
        'Sales\n10.25\n11.25\n12.25');
    assert.equal(notices.length,0);
});

test('selected copy rejects missing row IDs and virtual holes without producing a clipboard payload',()=>{
    for(const missing of [true,false]) {
        const notices=[];
        const rows=[{id:'a',original:{__virtualIndex:0},getValue:()=>1},
            {id:'c',original:{__virtualIndex:100},getValue:()=>3}];
        let presentation;
        function Harness(){presentation=useRuntimeClipboardExportPresentation({
            table:{getVisibleLeafColumns:()=>[{id:'sales'}]},getDisplayRows:()=>missing?rows.slice(0,1):rows,
            serverSide:true,theme:{},showNotification:(...args)=>notices.push(args),
            selectedCells:{'a:sales':1,'c:sales':3},
        });return null;}
        renderToStaticMarkup(React.createElement(Harness));
        assert.equal(presentation.buildSelectedClipboardPayload(),null);
        assert.match(notices[0][0],/unavailable.*loaded/i);
        assert.equal(notices[0][1],'warning');
    }
});
const {useRuntimeCellSelection} = loadSource(path.join(frontend, 'src/lib/runtime/useRuntimeCellSelection.js'));
function movingWindowSelection(initialIndices) {
    const row = index => ({id: `東京:row-${index}`, original: {__virtualIndex:index}, getValue: () => index});
    let rows = initialIndices.map(row);
    const state = {selected: {}, anchor:null, drag:null, scope:'dataset-A', writes:[], notices:[]};
    const columns = [{id:'sales',columnDef:{header:'Sales'}}];
    const table = {getVisibleLeafColumns:()=>columns, getRowModel:()=>({rows})};
    function render() {
        let selection, presentation;
        function Harness() {
            presentation = useRuntimeClipboardExportPresentation({
                table,getDisplayRows:()=>rows,serverSide:true,theme:{},selectedCells:state.selected,
                getDisplayHeaderTextForColumn:c=>c.columnDef.header,
                showNotification:(...args)=>state.notices.push(args),
            });
            selection = useRuntimeCellSelection({
                table,tableRef:{current:table},getDisplayRows:()=>rows,serverSide:true,selectionScopeKey:state.scope,
                lastSelected:state.anchor,dragStart:state.drag,selectedCells:state.selected,
                isSelectAllServerRef:{current:false},isDragging:true,
                setSelectedCells:value=>{state.selected=typeof value==='function'?value(state.selected):value;},
                setLastSelected:value=>{state.anchor=value;},setDragStart:value=>{state.drag=value;},
                setIsDragging:()=>{},setHoveredRowPath:()=>{},
                scrollToDisplayRow:()=>{},scrollToDisplayColumn:()=>{},
                buildSelectedClipboardPayload:presentation.buildSelectedClipboardPayload,
                copyRichToClipboard:payload=>state.writes.push(payload),
            });
            return null;
        }
        renderToStaticMarkup(React.createElement(Harness));
        return {selection,presentation};
    }
    const click = (index,shiftKey=false)=>render().selection.handleCellMouseDown(
        {button:0,shiftKey,preventDefault(){}},index,0,rows[index].id,'sales',rows[index].getValue());
    const copy = ()=>render().selection.handleKeyDown({key:'c',ctrlKey:true,preventDefault(){},target:{tagName:'TD',closest:()=>null}});
    const contextClick = index => {
        const stage = {...render().selection, table, selectedCells:state.selected,
            hasSelectionKey:(cells,key)=>Object.hasOwn(cells,key),
            setSelectedCells:value=>{state.selected=value;},setLastSelected:value=>{state.anchor=value;},
            resolveDisplayRowIndex:id=>rows.findIndex(row=>row.id===id)};
        // Execute the real viewport context-menu promotion branch, before menu assembly.
        const source=fs.readFileSync(path.join(frontend,'src/lib/runtime/useViewportRuntimeStage.tsx'),'utf8');
        const body=source.split('stage.handleContextMenu = (e, value, colId, row) => {')[1].split('    const hasSelection =')[0];
        const {getDisplayOrderedLeafColumns}=loadSource(path.join(frontend,'src/lib/runtime/displayColumnOrder.js'));
        new Function('stage','getDisplayOrderedLeafColumns','e','value','colId','row',body)(
            stage,getDisplayOrderedLeafColumns,{preventDefault(){}},rows[index].getValue(),'sales',rows[index]);
    };
    return {state,render,click,copy,contextClick,move:indices=>{rows=indices.map(row);}};
}

test('actual mouse anchor survives replacement cache window and copy refuses absent endpoint',()=>{
    const f=movingWindowSelection([0,1,2]); f.click(0); f.move([900,901,902]); f.click(2,true);
    assert.ok(Object.hasOwn(f.state.selected,'東京:row-0:sales'));
    f.copy(); assert.equal(f.state.writes.length,0);
    assert.equal(f.state.notices[0][0],'Selected range unavailable: some rows are not loaded. Load the complete range before copying.');
});

test('retained anchor resolves by ID after cache prefix shifts; missing middle refuses',()=>{
    const f=movingWindowSelection([0,1,2]);f.click(2);f.move([2,900,901]);f.click(2,true);
    assert.ok(Object.hasOwn(f.state.selected,'東京:row-2:sales'));
    f.copy();assert.equal(f.state.writes.length,0);
    assert.equal(f.state.notices.length,1);
});

test('fully loaded shifted window selects exact stable endpoints and headers',()=>{
    const f=movingWindowSelection([0,1,2]);f.click(2);f.move([1,2,3,4]);f.click(3,true);
    assert.deepEqual(Object.keys(f.state.selected),['東京:row-2:sales','東京:row-3:sales','東京:row-4:sales']);
    assert.equal(f.render().presentation.buildSelectedClipboardPayload(true,undefined,{rawValues:true}).text,'Sales\n2\n3\n4');
});

test('drag across a huge missing window retains anchor without materializing the gap',()=>{
    const f=movingWindowSelection([0]);f.click(0);f.move([1e12,1e12+1]);
    f.render().selection.handleCellMouseEnter(1,0);
    assert.ok(Object.hasOwn(f.state.selected,'東京:row-0:sales'));
    assert.ok(Object.keys(f.state.selected).length<=3);
    f.copy();assert.equal(f.state.writes.length,0);
});

test('changed selection scope does not extend an old logical anchor',()=>{
    const f=movingWindowSelection([0,1,2]);f.click(0);f.state.scope='dataset-B';f.click(2,true);
    assert.deepEqual(Object.keys(f.state.selected),['東京:row-2:sales']);
});

test('keyboard resolves stable row ID after window shift and never rebinds an unloaded anchor',()=>{
    const f=movingWindowSelection([0,1,2]);f.click(2);f.move([1,2,3,4]);
    const key={key:'ArrowDown',preventDefault(){},target:{tagName:'TD',closest:()=>null}};
    f.render().selection.handleKeyDown(key);
    assert.deepEqual(Object.keys(f.state.selected),['東京:row-3:sales']);
    f.move([100,101]);f.render().selection.handleKeyDown(key);
    assert.deepEqual(Object.keys(f.state.selected),['東京:row-3:sales']);
    f.copy();assert.equal(f.state.writes.length,0);
});

test('sort, filter, expansion and dataset scope changes reject the old mouse anchor',()=>{
    const {cellSelectionScopeKey}=loadSource(path.join(frontend,'src/lib/runtime/cellSelectionAnchor.js'));
    const scope=(dataset='A',sorting=[],filters={},expanded={})=>cellSelectionScopeKey(true,dataset,JSON.stringify({sorting,filters}),expanded,['pivot']);
    const original=scope();
    for(const next of [scope('B'),scope('A',[{id:'sales',desc:true}]),scope('A',[],{sales:1}),scope('A',[],{}, {'東京':true})]) {
        const f=movingWindowSelection([0,1,2]);f.state.scope=original;f.click(0);
        f.state.scope=next;f.click(2,true);
        assert.deepEqual(Object.keys(f.state.selected),['東京:row-2:sales']);
    }
});

test('refused shifted-window copy leaves the delivery port untouched for 1.5 seconds',async()=>{
    const f=movingWindowSelection([0,1]);f.click(0);f.move([900,901]);f.click(1,true);
    const sentinel={text:'clipboard-sentinel'};
    f.state.writes.push(sentinel);f.copy();
    const deadline=Date.now()+1500;
    do {
        assert.deepEqual(f.state.writes,[sentinel]);
        await new Promise(resolve=>setTimeout(resolve,25));
    } while(Date.now()<deadline);
    assert.deepEqual(f.state.writes,[sentinel]);
    assert.equal(f.state.notices.length,1);
});

test('actual context-menu promotion survives shifted and missing endpoint windows',()=>{
    for (const indices of [[1,2,3,4],[900,901,902]]) {
        const f=movingWindowSelection([0,1,2]);f.contextClick(2);const anchorKey=Object.keys(f.state.selected)[0];
        f.move(indices);f.click(indices.length-1,true);
        assert.ok(Object.hasOwn(f.state.selected,anchorKey));
        if(indices[0]===900){f.copy();assert.equal(f.state.writes.length,0);assert.equal(f.state.notices.length,1);}
        else assert.deepEqual(Object.values(f.state.selected),[2,3,4]);
    }
});

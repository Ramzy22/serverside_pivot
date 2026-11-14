const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');

const frontendRoot = path.resolve(__dirname, '../serverside_pivot');
const babel = require(path.join(frontendRoot, 'node_modules/@babel/core'));
const presetEnv = require(path.join(frontendRoot, 'node_modules/@babel/preset-env'));
const {createTable, getCoreRowModel} = require(path.join(frontendRoot, 'node_modules/@tanstack/react-table'));
const hooks = {useCallback: fn => fn, useMemo: fn => fn(), useEffect: () => {}};

function loadRuntime(name) {
    const filename = path.join(frontendRoot, 'src/lib/runtime', name + '.js');
    const transformed = babel.transformSync(fs.readFileSync(filename, 'utf8'), {
        filename, presets: [[presetEnv, {targets: {node: 'current'}, modules: 'commonjs'}]],
    }).code;
    const mod = {exports: {}};
    const localRequire = request => {
        if (request === 'react') return hooks;
        // Run the real keyboard ownership helpers for noneditable table cells.
        if (request === '../hooks/usePivotNormalization') return loadRuntime('../hooks/usePivotNormalization');
        // Formatting helpers and chart builders are outside these inactive-chart fixtures.
        if (request === '../utils/helpers' || request === '../utils/sparklines' || request === '../utils/chartModelBuilders') return {};
        if (request.startsWith('./')) return loadRuntime(request.slice(2));
        return require(request);
    };
    new Function('module', 'exports', 'require', transformed)(mod, mod.exports, localRequire);
    return mod.exports;
}

function fixture(withGutter = false) {
    // Core order A,B,C,D; rendered order C,A,D,B, plus an optional leading gutter.
    const ids = [...(withGutter ? ['__report_config__'] : []), 'A', 'B', 'C', 'D'];
    const table = createTable({
        data: [{A: 10, B: 20, C: 30, D: 40}, {A: 11, B: 21, C: 31, D: 41}],
        columns: ids.map(id => ({id, accessorKey: id, header: id})),
        getCoreRowModel: getCoreRowModel(),
        state: {columnPinning: {left: [...(withGutter ? ['__report_config__'] : []), 'C'], right: ['B']}, columnOrder: ['A', 'B', 'C', 'D']},
        onStateChange: () => {}, renderFallbackValue: null,
    });
    const rows = table.getRowModel().rows;
    const state = {selected: {}, anchor: null, dragStart: null, columnsScrolled: []};
    const {useRuntimeCellSelection} = loadRuntime('useRuntimeCellSelection');
    const selection = overrides => useRuntimeCellSelection({
        table, tableRef: {current: table}, getDisplayRows: () => rows,
        selectedCells: state.selected, lastSelected: state.anchor, dragStart: state.dragStart,
        isSelectAllServerRef: {current: false},
        setSelectedCells: value => {state.selected = typeof value === 'function' ? value(state.selected) : value;},
        setLastSelected: value => {state.anchor = value;},
        setDragStart: value => {state.dragStart = value;},
        setIsDragging: () => {}, setHoveredRowPath: () => {}, setIsFilling:()=>{}, setFillRange:()=>{},
        scrollToDisplayRow: () => {}, scrollToDisplayColumn: index => state.columnsScrolled.push(index),
        ...overrides,
    });
    return {table, rows, state, selection};
}

function key(key, extras = {}) {
    const target = {tagName: 'TD', closest: () => null};
    return {key, target, composedPath: () => [target], preventDefault() {}, ...extras};
}

test('mouse rectangle follows pinned display order and excludes the positional report gutter', () => {
    const {selection} = fixture(true);
    assert.deepEqual(selection().selectRange({rowIndex: 0, colIndex: 0}, {rowIndex: 1, colIndex: 2}), {
        '0:C': 30, '0:A': 10, '1:C': 31, '1:A': 11,
    });
});

test('Shift+Arrow extends from the rendered left column through the center', () => {
    const {selection, state} = fixture();
    state.anchor = {rowIndex: 0, colIndex: 0};
    selection().handleKeyDown(key('ArrowRight', {shiftKey: true}));
    assert.deepEqual(state.selected, {'0:C': 30, '0:A': 10});
    assert.deepEqual(state.anchor, {rowIndex: 0, colIndex: 1, rowId:'0', colId:'A', virtualIndex:undefined, scopeKey:''});
});

test('Tab and Shift+Tab cross pinned boundaries using rendered indices with the gutter', () => {
    const {selection, state} = fixture(true);
    state.anchor = {rowIndex: 0, colIndex: 3}; // D -> B at the right boundary
    selection().handleKeyDown(key('Tab'));
    assert.deepEqual(state.selected, {'0:B': 20});
    assert.deepEqual(state.anchor, {rowIndex: 0, colIndex: 4, rowId:'0', colId:'B', virtualIndex:undefined, scopeKey:''});
    selection().handleKeyDown(key('Tab', {shiftKey: true}));
    assert.deepEqual(state.selected, {'0:D': 40});
    assert.deepEqual(state.columnsScrolled, [4, 3]);
});

test('ArrowLeft, Shift+Tab and Ctrl+ArrowLeft stop at first data column after gutter', () => {
    for (const event of [key('ArrowLeft'), key('Tab', {shiftKey: true}), key('ArrowLeft', {ctrlKey: true})]) {
        const {selection, state} = fixture(true);
        state.anchor = {rowIndex: 0, colIndex: 1};
        selection().handleKeyDown(event);
        assert.deepEqual(state.selected, {'0:C': 30});
        assert.deepEqual(state.anchor, {rowIndex: 0, colIndex: 1, rowId:'0', colId:'C', virtualIndex:undefined, scopeKey:''});
    }
});

test('select all anchors first displayed data column without collapsing gutter position', () => {
    const {selection, state} = fixture(true);
    selection().handleKeyDown(key('a', {ctrlKey: true}));
    assert.deepEqual(state.anchor, {rowIndex: 0, colIndex: 1, rowId:'0', colId:'C', virtualIndex:undefined, scopeKey:''});
    assert.deepEqual(state.dragStart, state.anchor);
    assert.deepEqual(Object.keys(state.selected), ['0:C', '0:A', '0:D', '0:B', '1:C', '1:A', '1:D', '1:B']);
});

test('selected TSV is ordered left-center-right and ignores report configuration cells', () => {
    const {table, rows} = fixture(true);
    const {useRuntimeSelectionFormatting} = loadRuntime('useRuntimeSelectionFormatting');
    const formatting = useRuntimeSelectionFormatting({
        table, getDisplayRows: () => rows, valConfigs: [], selectedCells: {'0:A': 10, '0:B': 20, '0:C': 30, '0:D': 40, '0:__report_config__': 'gutter'},
    });
    assert.equal(formatting.getSelectedData(true), 'C\tA\tD\tB\n30\t10\t40\t20\n');
});

test('chart category navigation publishes and scrolls rendered indices including gutter', () => {
    const {table} = fixture(true);
    const scrolled = [];
    const {useRuntimeChartModels} = loadRuntime('useRuntimeChartModels');
    const charts = useRuntimeChartModels({
        table, chartCanvasPanes: [], chartPaneDataById: {}, selectedCells: {},
        rowFields: [], setPropsRef: {current: null},
        scrollToDisplayColumn: index => scrolled.push(index),
    });
    assert.deepEqual(charts.visibleLeafColumns.map(column => column.id), ['__report_config__', 'C', 'A', 'D', 'B']);
    charts.activateChartCategory('pivot', 'select', {kind: 'column', columnId: 'D'});
    assert.deepEqual(scrolled, [3]);
});

test('semantic scope effect clears cells, anchors, drag and server select-all',()=>{
    const effects=[];
    const previous=hooks.useEffect;
    hooks.useEffect=(callback,deps)=>effects.push({callback,deps});
    try {
        const {selection,state}=fixture();
        state.selected={'0:C':30};state.anchor={rowIndex:0,colIndex:0};state.dragStart=state.anchor;
        const selectAll={current:true};
        selection({selectionScopeKey:'new-semantic-scope',isSelectAllServerRef:selectAll});
        const reset=effects.find(effect=>effect.deps[0]==='new-semantic-scope');
        assert.ok(reset);reset.callback();
        assert.deepEqual(state.selected,{});assert.equal(state.anchor,null);assert.equal(state.dragStart,null);
        assert.equal(selectAll.current,false);
    } finally {hooks.useEffect=previous;}
});

test('stable column anchor follows reordered pinned columns and hidden anchor is not reinterpreted',()=>{
    const {selection,state,table}=fixture();
    selection().handleCellMouseDown({button:0},0,0,'0','C',30);
    table.setOptions(previous=>({...previous,state:{...previous.state,columnPinning:{left:['A'],right:['B']}}}));
    selection().handleCellMouseDown({button:0,shiftKey:true,preventDefault(){}},1,2,'1','D',41);
    assert.deepEqual(state.selected,{'0:C':30,'0:D':40,'1:C':31,'1:D':41});
    table.setOptions(previous=>({...previous,state:{...previous.state,columnVisibility:{C:false}}}));
    selection().handleCellMouseDown({button:0,shiftKey:true,preventDefault(){}},1,1,'1','D',41);
    assert.deepEqual(state.selected,{});
});

test('scope invalidation cancels active fill and refuses stale fill completion',()=>{
    const effects=[];const previous=hooks.useEffect;hooks.useEffect=(callback,deps)=>effects.push({callback,deps});
    try {
        const {selection,state}=fixture();const updates=[];
        state.dragStart={rowIndex:0,colIndex:0,rowId:'0',colId:'C',scopeKey:'old'};
        let filling=true,range={rStart:0,rEnd:1,cStart:0,cEnd:0};
        const render=()=>selection({selectionScopeKey:'new',isFilling:filling,fillRange:range,setProps:()=>{},
            setIsFilling:value=>{filling=value;},setFillRange:value=>{range=value;},
            dispatchBatchUpdateRequest:(...args)=>updates.push(args)});
        const stale=render();effects.find(effect=>effect.deps[0]==='new').callback();
        assert.equal(filling,false);assert.equal(range,null);assert.equal(state.dragStart,null);
        assert.doesNotThrow(()=>stale.handleFillMouseUp());
        assert.doesNotThrow(()=>render().handleFillMouseUp());assert.deepEqual(updates,[]);
        filling=true;range={rStart:0,rEnd:1,cStart:0,cEnd:0};
        assert.doesNotThrow(()=>render().handleFillMouseUp());assert.deepEqual(updates,[]);
    } finally {hooks.useEffect=previous;}
});

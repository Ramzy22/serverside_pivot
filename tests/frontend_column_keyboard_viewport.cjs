const assert = require('node:assert/strict');
const {test} = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const frontend = path.resolve(__dirname, '../serverside_pivot');
const deps = createRequire(path.join(frontend, 'package.json'));
const core = deps('@tanstack/virtual-core');
global.window = {addEventListener() {}, removeEventListener() {}};
const filename = path.join(frontend, 'src/lib/hooks/useColumnVirtualizer.js');
const code = deps('@babel/core').transformSync(fs.readFileSync(filename, 'utf8'), {
    filename, presets: [[deps('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}]],
}).code;

// Only React's hook scheduling is a harness. Sizing, measurement, auto alignment,
// offset clamping and scrollToIndex execute the actual installed TanStack core.
function render(initial) {
    let config, result, actual, notifyOffset;
    const frames = [], layouts = [], slots = [];
    let cursor = 0, subscriptions = 0, resizeNotify;
    // Real elements expose client geometry and scroll extents independently of
    // their border box. Core reads clientHeight and uses scrollHeight to tell an
    // Element from a Window when computing even a horizontal scroll limit.
    const element = {clientHeight: 555, offsetHeight: 570, scrollHeight: 555,
        scrollLeft: initial.offset || 0,
        ownerDocument: {defaultView: {ResizeObserver: class {constructor(cb){resizeNotify=cb;}observe(){}disconnect(){}}, requestAnimationFrame: cb => {frames.push(cb); return frames.length;}, cancelAnimationFrame() {}}}};
    const memo = (factory, deps) => {
        const i=cursor++, previous=slots[i];
        if (!previous || !deps || deps.some((v,j) => !Object.is(v, previous.deps[j]))) slots[i]={value: factory(), deps};
        return slots[i].value;
    };
    const react = {useRef(value) {const i=cursor++; if(!slots[i]) slots[i]={current:value}; return slots[i];},
        useMemo: memo, useCallback: (f,deps) => memo(() => f,deps), useEffect() {},
        useLayoutEffect(f,deps) {memo(() => {layouts.push(f); return null;},deps);}};
    const observeOffset = (instance, cb) => {notifyOffset=cb;cb(element.scrollLeft,false);return()=>{};};
    const scrollToFn = value => {element.scrollLeft=Math.max(0, Math.min(element.scrollWidth-element.clientWidth,value));if(notifyOffset)notifyOffset(element.scrollLeft,false);};
    const observeRect = (instance, cb) => {subscriptions++; return core.observeElementRect(instance,cb);};
    const hook = {exports:{}};
    new Function('module','exports','require',code)(hook,hook.exports,name=>{
        if(name==='react')return react;
        if(name==='@tanstack/react-virtual')return {...core,observeElementRect: observeRect,useVirtualizer(options){
            const merged={observeElementRect:observeRect,observeElementOffset:observeOffset,scrollToFn,...options};
            if(!actual)actual=new core.Virtualizer(merged);else actual.setOptions(merged);
            actual._willUpdate();return actual;
        }};
        return deps(name);
    });
    const columns=(values,prefix)=>values.map((size,i)=>({id:prefix+i,getSize:()=>size,getIsVisible:()=>true}));
    const parentRef={current:element};
    function rerender(next) {
        config={width:1221,left:[320],right:[],...next};cursor=0;
        element.clientWidth=config.width; element.offsetWidth=config.borderWidth ?? config.width;
        element.scrollWidth=[...config.left,...config.sizes,...config.right].reduce((a,b)=>a+b,0);
        const table={getLeftLeafColumns:()=>columns(config.left,'left'),getRightLeafColumns:()=>columns(config.right,'right'),getCenterLeafColumns:()=>columns(config.sizes,'center')};
        result=hook.exports.useColumnVirtualizer({table,parentRef,columns:[]});
        while(layouts.length)layouts.shift()();
        return result;
    }
    rerender(initial);
    return {get result(){return result;},get actual(){return actual;},element,rerender,
        get subscriptions(){return subscriptions;},
        resize(width) {element.clientWidth=width;element.offsetWidth=width;resizeNotify([{borderBoxSize:[{inlineSize:width,blockSize:555}]}]);},
        flush(){let n=0;while(frames.length&&n++<30)frames.shift()();assert(n<30);}};
}

const recorded = [189, 140, 185, 189, 140, 185, 189, 234, 185];

test('actual core reads client geometry and the physical element scroll limit', () => {
    const model = render({sizes: recorded, borderWidth: 1236});
    assert.deepEqual(model.actual.scrollRect, {width: 901, height: 555});
    assert.equal(model.actual.getMaxScrollOffset(), 735);
    assert.notEqual(model.actual.scrollRect.height, model.element.offsetHeight);
});

function assertPhysicalVisibility(model, index, left, right) {
    model.actual.scrollToIndex(index);
    model.flush();
    const item = model.actual.getMeasurements()[index];
    const start = left + item.start - model.element.scrollLeft;
    const end = left + item.end - model.element.scrollLeft;
    assert(start >= left, `column ${index} starts behind left pins: ${start} < ${left}`);
    assert(end <= model.element.clientWidth-right, `column ${index} clipped at physical right: ${end} > ${model.element.clientWidth-right}`);
}

test('recorded next Q2 column is revealed without shifting center measurements', () => {
    const model = render({sizes: recorded});
    assert.equal(model.actual.getMeasurements()[5].end, 1028);
    assertPhysicalVisibility(model, 5, 320, 0);
    assert.equal(model.element.scrollLeft, 127);
    assert.equal(model.result.centerTotalWidth, 1636);
    assert.equal(model.result.totalLayoutWidth, 1956);
    assert.equal(model.actual.getMeasurements()[0].start, 0);
    assert.equal(model.result.beforeWidth, 0);
});

for (const [name, left, right] of [['none', [], []], ['left', [320], []], ['right', [], [115]], ['both', [210, 90], [115, 45]]]) {
    test(`actual auto alignment reveals every variable-width center column with ${name} pins`, () => {
        const model = render({sizes: recorded, left, right});
        const leftWidth = left.reduce((a,b) => a+b, 0), rightWidth = right.reduce((a,b) => a+b, 0);
        for (let index=0; index<recorded.length; index++) assertPhysicalVisibility(model, index, leftWidth, rightWidth);
        for (let index=recorded.length-1; index>=0; index--) assertPhysicalVisibility(model, index, leftWidth, rightWidth);
        assert.equal(model.result.centerTotalWidth, recorded.reduce((a,b) => a+b, 0));
        assert.equal(model.result.totalLayoutWidth, model.element.scrollWidth);
    });
}

test('resize and reordered variable widths preserve center coordinates and last-column visibility', () => {
    for (const scenario of [
        {width: 990, left: [380], right: [70], sizes: recorded},
        {width: 1430, left: [275], right: [130], sizes: [234, 185, 140, 189, 185, 189, 140, 185, 189]},
        {width: 1100, left: [410], right: [], sizes: [260, 90, 210, 155, 140, 185, 189, 234, 220]},
    ]) {
        const model = render(scenario);
        const left = scenario.left.reduce((a,b) => a+b, 0), right = scenario.right.reduce((a,b) => a+b, 0);
        assertPhysicalVisibility(model, scenario.sizes.length-1, left, right);
        assertPhysicalVisibility(model, 0, left, right);
        assert.equal(model.actual.getMeasurements()[0].start, 0);
    }
});


test('pin widths update a mounted virtualizer at unchanged physical parent width', () => {
    const model=render({sizes:recorded,left:[],right:[]});
    const identity=model.actual;
    const subscriptions=model.subscriptions;
    for(const scenario of [
        {sizes:recorded,left:[320],right:[]},
        {sizes:recorded,left:[250,70],right:[110]},
        {sizes:recorded,left:[410],right:[145]},
        {sizes:recorded,left:[],right:[]},
    ]) {
        model.rerender(scenario);
        assert.equal(model.actual,identity);
        assert.equal(model.subscriptions,subscriptions,'core must not need a scroll-element replacement');
        const left=scenario.left.reduce((a,b)=>a+b,0),right=scenario.right.reduce((a,b)=>a+b,0);
        assert.equal(model.actual.scrollRect.height,555);
        assert.equal(model.actual.scrollRect.width,Math.max(0,1221-left-right));
        assertPhysicalVisibility(model,8,left,right);
        assertPhysicalVisibility(model,0,left,right);
        assert.equal(model.actual.getMeasurements()[0].start,0);
        assert.equal(model.result.totalLayoutWidth,model.element.scrollWidth);
    }
});

test('fully occupied pinned width clamps center viewport to zero and preserves height', () => {
    const model=render({width:500,left:[400],right:[150],sizes:recorded});
    assert.equal(model.actual.scrollRect.width,0);
    assert.equal(model.actual.scrollRect.height,555);
});


test('live physical resize and scrollbar border width use the actual client viewport', () => {
    const model=render({width:1221,borderWidth:1236,left:[320],right:[90],sizes:recorded});
    assert.equal(model.actual.scrollRect.width,811);
    const identity=model.actual, subscriptions=model.subscriptions;
    for(const width of [1000,1450,1221]) {
        model.resize(width);
        assert.equal(model.actual,identity);
        assert.equal(model.subscriptions,subscriptions);
        assert.equal(model.actual.scrollRect.width,width-410);
        assert.equal(model.actual.scrollRect.height,555);
        assertPhysicalVisibility(model,8,320,90);
        assertPhysicalVisibility(model,0,320,90);
    }
});

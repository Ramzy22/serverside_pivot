/* Repository runtime tests, not installed-host qualification evidence. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../serverside_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
const React = req('react');
const hookRuntime = {...React, useMemo: fn => fn(), useCallback: fn => fn,
  useRef: value => ({current: value}), useEffect: () => {}};
const cache = new Map();
function load(relative) {
  let filename = path.resolve(root, 'src/lib', relative);
  if (!path.extname(filename)) filename += '.js';
  if (cache.has(filename)) return cache.get(filename).exports;
  const module = {exports: {}};
  cache.set(filename, module);
  const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
    presets: [[req('@babel/preset-env'), {targets: {node: 'current'}, modules: 'commonjs'}], req('@babel/preset-react')]}).code;
  new Function('module', 'exports', 'require', code)(module, module.exports, name => {
    if (name === 'react') return hookRuntime;
    // Unrendered leaf UI components do not participate in the algorithms below.
    if (name.endsWith('/Icons') || name.endsWith('/FilterPopover')) return () => null;
    return name.startsWith('.') ? load(path.relative(path.join(root, 'src/lib'), path.resolve(path.dirname(filename), name))) : req(name);
  });
  return module.exports;
}

const {formatValue} = load('utils/helpers.js');
for (const [places, expected] of [[2, ['100.25', '20.00', '-100.25']], [1, ['100.3', '20.0', '-100.3']]]) {
  assert.deepEqual([100.25, 20, -100.25].map(value => formatValue(value, '', places, 'none')), expected);
  assert.deepEqual(expected, [100.25, 20, -100.25].map(value => new Intl.NumberFormat('en-GB', {
    style: 'decimal', minimumFractionDigits: places, maximumFractionDigits: places,
    useGrouping: false, roundingMode: 'halfExpand'}).format(value)));
}
assert.equal(formatValue(12345.25, '', 2, 'none'), '12345.25');

// Exercise the actual state hook, retaining cells across simulated renders.
const stateCells=[]; let stateCursor=0;
hookRuntime.useState=initial=>{
  const index=stateCursor++;
  if(!(index in stateCells))stateCells[index]=initial;
  return [stateCells[index],value=>{stateCells[index]=typeof value==='function'?value(stateCells[index]):value;}];
};
const {useFormatState}=load('hooks/useFormatState.js');
const renderFormat=()=>{stateCursor=0;return useFormatState({normalizedInitialDecimalPlaces:2,normalizedInitialDefaultValueFormat:'',normalizedInitialNumberGroupSeparator:'none'});};
let format=renderFormat();
assert.equal(format.colorScaleColumns,null);
format.setColorScaleMode('col',['sales','sales']);format=renderFormat();
assert.equal(format.colorScaleMode,'col');assert.deepEqual(format.colorScaleColumns,['sales']);
format.setColorScaleMode('row');format=renderFormat();
assert.equal(format.colorScaleColumns,null,'legacy/global mode has no selection scope');
format.setColorScaleColumns(['units']);format=renderFormat();
assert.deepEqual(format.colorScaleColumns,['units'],'saved explicit scope restores without changing mode');
assert.deepEqual(format.dataBarMaximums,{});
format.setDataBarMaximums({sales:200,units:-1,bad:Infinity,string:'200'});format=renderFormat();
assert.deepEqual(format.dataBarMaximums,{sales:200},'restore rejects invalid maximums');
const {updateDataBarMaximums,resolveDataBarMaximum}=load('utils/dataBars.js');
format.setDataBarMaximums(previous=>updateDataBarMaximums(previous,['units'],300));format=renderFormat();
assert.deepEqual(format.dataBarMaximums,{sales:200,units:300},'bounds preserve unselected columns');
format.setDataBarMaximums(previous=>updateDataBarMaximums(previous,['sales'],null));format=renderFormat();
assert.deepEqual(format.dataBarMaximums,{units:300},'blank Auto removes only the selected override');
format.setDataBarMaximums(undefined);format=renderFormat();
assert.deepEqual(format.dataBarMaximums,{},'legacy views restore automatic bounds');
assert.equal(resolveDataBarMaximum(200,{min:-100,max:300}),200);
assert.equal(resolveDataBarMaximum(undefined,{min:-100,max:300}),300);
assert.equal(resolveDataBarMaximum(200,null),200,'fixed bounds do not require server statistics');
assert.equal(resolveDataBarMaximum(undefined,null),null);

// A document listener sees the shadow host as target, while composedPath keeps
// the real popover ancestry. An ordinary outside event must still close it.
const {isEventInsideElement}=load('utils/domEvents.js');
const inside={},host={},outside={};
const popover={contains:target=>target===inside};
assert(isEventInsideElement({target:inside},popover));
assert(isEventInsideElement({target:host,composedPath:()=>[inside,popover,host]},popover));
assert(!isEventInsideElement({target:outside,composedPath:()=>[outside,host]},popover));
assert(!isEventInsideElement({target:inside},null));

const {gridDimensionTokens, themes} = load('utils/styles.js');
assert.deepEqual(gridDimensionTokens.density.rowHeights, [36, 44, 56]);
assert.equal(themes.dark.pageBg, '#0A0C10');
assert.equal(themes.dark.text, '#F8FAFC');

const {useRuntimeColorScalePresentation} = load('runtime/useRuntimeColorScalePresentation.js');
const scale = useRuntimeColorScalePresentation({
  colorPalette: 'redGreen', colorScaleMode: 'col', colFields: [], rowFields: ['region'],
  nodes: [], filteredData: [0, 100, 200].map((sales, index) => ({_id: String(index), sales, control: 10 + index})),
  serverColorScaleStats: null, getRuleBasedStyle: () => ({}),
});
assert.deepEqual(scale.colorScaleStats.byCol.sales, {min: 0, max: 200});
assert.deepEqual([0, 100, 200].map(value => scale.getConditionalStyle('sales', value, {}, 'r').background),
  ['rgba(248,105,107,0.820)', 'rgba(248,105,107,0.060)', 'rgba(99,190,123,0.820)']);
assert.equal(scale.getConditionalStyle('sales', -100, {}, 'r').background, 'rgba(248,105,107,0.820)');
assert.equal(scale.getConditionalStyle('sales', 300, {}, 'r').background, 'rgba(99,190,123,0.820)');
assert.deepEqual(scale.getConditionalStyle('sales', 100, {_isTotal: true}, 'total'), {});
const scopedScale = useRuntimeColorScalePresentation({
  colorPalette:'redGreen',colorScaleMode:'col',colorScaleColumns:['sales'],
  colFields:[],rowFields:['region'],nodes:[],
  filteredData:[{sales:0,control:10},{sales:100,control:20},{sales:200,control:30}],
  serverColorScaleStats:null,getRuleBasedStyle:()=>({fontWeight:600}),
});
assert.deepEqual(scopedScale.getConditionalStyle('control',20,{},'r'), {fontWeight:600});
assert.equal(scopedScale.getConditionalStyle('sales',100,{},'r').background,'rgba(248,105,107,0.060)');

const {useRenderHelpers} = load('hooks/useRenderHelpers.js');
const opts = {serverSide: true, selectedCells: {}, theme: themes.light, isDarkTheme: () => false,
  styles: {cell: {}}, visibleLeafColIndexMap: new Map([['sales', 0]]),
  getStickyStyle: () => ({}), getConditionalStyle: () => ({}), rowFields: ['region'],
  tableRef: {current: {}}, leftCols: [], centerCols: [], rightCols: [],
  dataBarsColumns: new Set(['sales']), dataBarMaximums:{sales:200}, colorScaleStats: {byCol: {sales: {min: -100, max: 300}}},
  autoSizeBounds: {minWidth:40,maxWidth:600},
  filters: {}, selectedCols: new Set(), columnPinning: {left: [], right: []}};
const helpers = useRenderHelpers(opts);
function render(value, overrides = {}) {
  const row = {id: 'r', original: {_path: 'r', ...overrides}, getIsGrouped: () => false};
  const column = {id: 'sales', columnDef: {cell: context => String(context.getValue())}, getSize: () => 170};
  const cell = {id: 'r_sales', row, column, getValue: () => value, getContext: () => ({})};
  return helpers.renderCell(cell, 0);
}
for (const [value, percent] of [[0, 0], [100, 50], [200, 100], [300, 100], [-100, 50], [-300, 100]]) {
  const output = render(value);
  const [bar, text] = output.props.children;
  assert.equal(bar.props.style.width, `${percent}%`, `Data-bar clamping failed for ${value}`);
  assert.equal(bar.props.style.pointerEvents, 'none');
  assert.equal(bar.props.style.zIndex, 0);
  assert.equal(text.props.style.zIndex, 1);
  assert.equal(bar.props.style.background, value < 0 ? 'rgba(220,38,38,0.12)' : 'rgba(37,99,235,0.13)');
}
assert.equal(render(100, {_isTotal: true}).props.children[0], null);
const automatic = useRenderHelpers({...opts,dataBarMaximums:{}});
const autoCell={id:'r_sales',row:{id:'r',original:{_path:'r'},getIsGrouped:()=>false},
  column:{id:'sales',columnDef:{cell:()=>''},getSize:()=>170},getValue:()=>100,getContext:()=>({})};
assert(Math.abs(parseFloat(automatic.renderCell(autoCell,0).props.children[0].props.style.width)-100/3)<1e-9,
  'Auto still uses real column extrema');
console.log('flagship presentation runtime passed');

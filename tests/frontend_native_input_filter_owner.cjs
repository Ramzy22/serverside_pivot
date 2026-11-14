/* Source runtime regression only; not installed-browser qualification evidence. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../serverside_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
const React = req('react');
const hooks = {...React, useMemo: fn => fn(), useCallback: fn => fn,
  useRef: value => ({current: value}), useEffect: () => {}};
function FilterPopover() {}
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
    if (name === 'react') return hooks;
    if (name.endsWith('/FilterPopover')) return FilterPopover;
    return name.startsWith('.') ? load(path.relative(path.join(root, 'src/lib'), path.resolve(path.dirname(filename), name))) : req(name);
  });
  return module.exports;
}
const {useRenderHelpers} = load('hooks/useRenderHelpers.js');
const {themes} = load('utils/styles.js');
const column = {id:'hierarchy', columnDef:{header:'Region > Account'},
  getIsSorted:()=>false, getSortIndex:()=>-1, getIsResizing:()=>false,
  getIsPinned:()=> 'left', getLeafColumns:()=>[column], getSize:()=>240,
  getCanResize:()=>false, getToggleSortingHandler:()=>()=>{}};
const header = (id, placeholder=false) => ({id, column, isPlaceholder:placeholder, getSize:()=>240});
function descendants(node, type, result=[]) {
  if (Array.isArray(node)) node.forEach(item=>descendants(item,type,result));
  else if (node && typeof node === 'object') {
    if (node.type===type) result.push(node);
    descendants(node.props?.children,type,result);
  }
  return result;
}
const base = {serverSide:true, theme:themes.light, styles:{headerCell:{}},
  isDarkTheme:()=>false, rowHeight:36, rowFields:['Region','Account'],
  leftCols:[column],centerCols:[],rightCols:[],filters:{},selectedCols:new Set(),
  sorting:[], hoveredHeaderIdRef:{current:null},focusedHeaderIdRef:{current:null},
  columnPinning:{left:['hierarchy'],right:[]},autoSizeBounds:{minWidth:40,maxWidth:600},
  tableRef:{current:{}}, getHeaderStickyStyle:()=>({}),
  activeFilterCol:'Region',filterAnchorEl:null};
// TanStack emits hierarchy placeholders at upper groups and a real leaf below.
// Both refer to the identical leaf column and therefore to the same filter ID.
for (const owner of ['leaf','other-leaf']) {
  const anchor = {getAttribute:name=>name==='data-pivot-filter-owner' ? `left:${owner}` : null};
  const render=useRenderHelpers({...base,filterAnchorEl:anchor}).renderHeaderCell;
  const trees=[render(header('placeholder',true),0,'left'),
    render(header('leaf'),1,'left'),render(header('other-leaf'),2,'left')];
  const popovers=descendants(trees,FilterPopover);
  assert.equal(popovers.length,1,'Only clicked header may mount a filter input');
  assert.equal(popovers[0].props.anchorEl,anchor);
  const ownerTree=trees[owner==='leaf'?1:2];
  assert.equal(descendants(ownerTree,FilterPopover).length,1);
  const buttons=descendants(ownerTree,'div').filter(node=>node.props['aria-label']==='Filter Column');
  assert.equal(buttons[0].props['data-pivot-filter-owner'],`left:${owner}`);
  const oppositeSection=render(header(owner),1,'center');
  assert.equal(descendants(oppositeSection,FilterPopover).length,0,'Another section cannot duplicate the owner');
}
const inactive=useRenderHelpers({...base,activeFilterCol:null}).renderHeaderCell(header('leaf'),1,'left');
assert.equal(descendants(inactive,FilterPopover).length,0);
// Reproduce the pinned hierarchy placeholder emitted by the real table library.
const {createTable,getCoreRowModel}=req('@tanstack/react-table');
const table=createTable({data:[], columns:[{id:'hierarchy',header:'Region > Account'},
  {id:'Q1',columns:[{id:'sales',accessorKey:'sales'},{id:'units',accessorKey:'units'}]}],
  state:{columnPinning:{left:['hierarchy'],right:[]},columnOrder:[],columnVisibility:{},sorting:[],
    columnSizing:{},columnSizingInfo:{}},
  getCoreRowModel:getCoreRowModel()});
const groups=table.getLeftHeaderGroups();
assert.equal(groups.length,2);
assert(groups[0].headers[0].isPlaceholder);
assert(!groups[1].headers[0].isPlaceholder);
const clicked=groups[1].headers[0];
const anchor={getAttribute:name=>name==='data-pivot-filter-owner' ? `left:${clicked.id}` : null};
const events=[];
const renderer=useRenderHelpers({...base,table,leftCols:table.getLeftLeafColumns(),filterAnchorEl:anchor,
  handleFilterClick:(event,id)=>events.push([event.currentTarget,id])}).renderHeaderCell;
const renderedGroups=groups.map((group,level)=>group.headers.map(item=>renderer(item,level,'left')));
assert.equal(descendants(renderedGroups,FilterPopover).length,1,'Real TanStack placeholder must not mount a second popup');
const filterButton=descendants(renderedGroups,'div').find(node=>node.props['aria-label']==='Filter Column');
const event={currentTarget:anchor,stopPropagation(){},preventDefault(){}};
filterButton.props.onClick(event);
filterButton.props.onKeyDown({...event,key:'Enter'});
filterButton.props.onKeyDown({...event,key:' '});
assert.deepEqual(events,[[anchor,'Region'],[anchor,'Region'],[anchor,'Region']],
  'Mouse and keyboard opens retain the clicked anchor and logical hierarchy filter');
// Context-menu Filter uses a virtual rectangle, without DOM Element methods.
const virtualAnchor={getBoundingClientRect:()=>({left:20,right:20,top:30,bottom:30,width:0,height:0})};
for(const field of ['Region','sales']) {
  const renderVirtual=useRenderHelpers({...base,table,tableRef:{current:table},
    activeFilterCol:field,filterAnchorEl:virtualAnchor}).renderHeaderCell;
  const trees=['left','center','right'].flatMap(section=>{
    const getter={left:'getLeftHeaderGroups',center:'getCenterHeaderGroups',right:'getRightHeaderGroups'}[section];
    return table[getter]().flatMap((group,level)=>group.headers.map(h=>renderVirtual(h,level,section)));
  });
  const popups=descendants(trees,FilterPopover);
  assert.equal(popups.length,1,'A virtual context-menu anchor must open exactly one eligible leaf popup');
  assert.equal(popups[0].props.anchorEl,virtualAnchor);
  assert.equal(popups[0].props.column.id,field);
}
console.log('native-input filter ownership runtime passed');

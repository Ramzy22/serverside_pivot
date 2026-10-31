const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createRequire} = require('node:module');
const root = path.resolve(__dirname, '../dash_tanstack_pivot');
const req = createRequire(path.join(root, 'package.json'));
const babel = req('@babel/core');
function load(relative) {
  const filename = path.join(root, 'src/lib', relative);
  const code = babel.transformSync(fs.readFileSync(filename, 'utf8'), {filename,
    presets:[[req('@babel/preset-env'),{targets:{node:'current'},modules:'commonjs'}]]}).code;
  const module = {exports:{}};
  new Function('module','exports','require',code)(module,module.exports,name=> {
    if(name==='react') return {useCallback:fn=>fn,useMemo:fn=>fn(),useRef:value=>({current:value}),useEffect:()=>{}};
    if(name.includes('sparklines')) return {normalizeSparklineValConfigsForView:value=>value};
    return req(name);
  });
  return module.exports;
}
const storage = new Map();
global.window = {localStorage:{getItem:key=>storage.get(key)??null,setItem:(key,value)=>storage.set(key,value)},
  sessionStorage:{getItem:key=>storage.get('session:'+key)??null,setItem:(key,value)=>storage.set('session:'+key,value)}};
const {usePersistence,resolvePersistedViewState} = load('hooks/usePersistence.js');
const {useRuntimeViewSnapshot} = load('runtime/useRuntimeViewSnapshot.js');
const persistence = usePersistence('first',true,'local');
const state={rowFields:['period'],colFields:['region','account'],columnSizing:{sales:173},spacingMode:0};
const saved={version:1,table:'revenue',state};
persistence.save('savedView',saved);
assert.deepEqual(resolvePersistedViewState(null,persistence.load('savedView',null),'revenue'),saved);
const explicit={state:{rowFields:['account']}};
assert.equal(resolvePersistedViewState(explicit,saved,'revenue'),explicit);
for(const invalid of [null,[],{}, {...saved,version:2},{...saved,table:'another'}, {...saved,state:[]}, {...saved,state:null}])
 assert.equal(resolvePersistedViewState(null,invalid,'revenue'),null);
assert.equal(usePersistence('second',true,'local').load('savedView',null),null);
assert.equal(usePersistence('first',true,'session').load('savedView',null),null);
assert.equal(usePersistence('first',false,'local').load('savedView',null),null);
storage.clear();
let emitted;
const opts={...state,tableName:'revenue',valConfigs:[],normalizedInitialValConfigs:[],managedChartDefinitions:[],
 chartCanvasPanes:[],normalizedChartServerWindow:{enabled:false},latestViewportRef:{current:null},parentRef:{current:null},
 setPropsRef:{current:value=>emitted=value},showNotification:()=>{},savePersistedState:persistence.save};
useRuntimeViewSnapshot(opts).handleSaveView();
assert.deepEqual(persistence.load('savedView',null),JSON.parse(JSON.stringify(emitted.savedView)),'Save View must persist the emitted complete snapshot');
assert.deepEqual(emitted.savedView.state.columnSizing,{sales:173});
assert.equal(emitted.savedView.state.spacingMode,0);
assert.deepEqual(emitted.savedView.state.rowFields,['period']);
assert.deepEqual(resolvePersistedViewState(null,persistence.load('savedView',null),'revenue'),JSON.parse(JSON.stringify(emitted.savedView)));
storage.clear();
useRuntimeViewSnapshot({...opts,savePersistedState:usePersistence('first',false,'local').save}).handleSaveView();
assert.equal(storage.size,0,'persistence opt-out must not write the snapshot');

const core = fs.readFileSync(path.join(root,'src/lib/runtime/useCoreRuntimeStage.tsx'),'utf8');
const resetCode = core.slice(core.indexOf('  // Reset Effect'),core.indexOf('  // Save Persistence'));
persistence.save('savedView',saved);
const resetStage = new Proxy({reset:true,valConfigsRef:{current:[]},setPropsRef:{current:null},layoutRevisionRef:{current:null},
 chartPanelOrientationAutoRef:{current:false},initialChartDefinition:{},initialChartDefinitions:[],normalizedInitialValConfigs:[],
 savePersistedState:persistence.save},{get:(target,key)=>key in target?target[key]:String(key).startsWith('set')?()=>{}:undefined});
new Function('stage','useEffect','normalizeMeasureAxisValue','normalizeSparklineValConfigsForView','getPinnedSideForRow',
 'GRAND_TOTAL_ROW_ID','clampFloatingChartRect','DEFAULT_FLOATING_CHART_PANEL_HEIGHT','DEFAULT_TABLE_CANVAS_SIZE',resetCode)(
 resetStage,fn=>fn(),()=>null,x=>x,()=>null,'Grand Total',x=>x,300,{});
assert.equal(persistence.load('savedView',null),null,'explicit reset must clear the saved full view');


const restoreStart = core.indexOf('  const appliedViewStateRef');
const restoreEndToken = '  }, [stage.viewState, stage.initialChartDefinition, stage.normalizedInitialValConfigs]);';
const restoreCode = core.slice(core.indexOf('  useEffect', restoreStart),core.indexOf(restoreEndToken,restoreStart)+restoreEndToken.length);
const applied = {current:null}, restoredRows=[];
const viewStage = new Proxy({viewState:saved,initialChartDefinition:{},chartPanelOrientationAutoRef:{current:false},setRowFields:value=>restoredRows.push(value)},
 {get:(target,key)=>key in target?target[key]:String(key).startsWith('set')?()=>{}:undefined});
const applyRestore = new Function('stage','appliedViewStateRef','useEffect','VALID_CHART_TYPES','VALID_CHART_SORT_MODES',
 'VALID_CHART_INTERACTION_MODES','VALID_CHART_SERVER_SCOPES','getPinnedSideForRow','GRAND_TOTAL_ROW_ID','getPreferredChartOrientation',restoreCode);
const apply = ()=>applyRestore(viewStage,applied,fn=>fn(),new Set(),new Set(),new Set(),new Set(),()=>null,'Grand Total',()=>"vertical");
apply();
viewStage.initialChartDefinition={changed:true};
apply();
assert.equal(restoredRows.length,1,'prop echoes after live layout changes must not reapply a consumed snapshot');
viewStage.viewState={state:{rowFields:['account']}};
apply();
assert.deepEqual(restoredRows,[['period'],['account']],'a fresh external viewState must still restore');

console.log('saved view persistence passed');

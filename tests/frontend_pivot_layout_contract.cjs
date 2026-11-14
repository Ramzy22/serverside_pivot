const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..', 'serverside_pivot');
const babel = require(path.join(root, 'node_modules', '@babel/core'));

function load(relativePath) {
  const file = path.join(root, relativePath);
  const transformed = babel.transformSync(fs.readFileSync(file, 'utf8'), {
    filename: file,
    presets: [[require(path.join(root, 'node_modules', '@babel/preset-env')), {
      targets: {node: 'current'}, modules: 'commonjs',
    }]],
  }).code;
  const module = {exports: {}};
  const localRequire = name => {
    if (name === '../utils/helpers') return {formatDisplayLabel: value => value, formatAggLabel: value => value};
    if (name === 'react') return {useCallback: callback => callback};
    if (name === './usePivotNormalization') return load('src/lib/hooks/usePivotNormalization.js');
    return require(name);
  };
  new Function('module', 'exports', 'require', transformed)(module, module.exports, localRequire);
  return module.exports;
}

const {reconcilePivotLayoutOrder, pivotLayoutRequestFields} = load('src/lib/utils/columnOrder.js');
const {mergeSparseColSchema} = load('src/lib/hooks/usePivotNormalization.js');
const schema = {layoutRevision: 'a'.repeat(64), total_center_cols: 3, columns: [
  {index: 0, id: 'A_sales', layoutId: 'p:A'},
  {index: 1, id: 'B_sales', layoutId: 'p:B'},
  {index: 2, id: 'C_sales', layoutId: 'p:C'},
]};

assert.deepEqual(reconcilePivotLayoutOrder(['p:missing', 'p:A', 'p:B'],
  ['B_sales', 'A_sales'], schema), ['p:missing', 'p:B', 'p:A']);
const changed = pivotLayoutRequestFields(['p:B', 'p:A'], 'a'.repeat(64), '',
  ['C_sales'], schema);
assert.deepEqual(changed.layoutManifest, ['p:B', 'p:A']);
assert.deepEqual(changed.pinnedColumnIds, ['p:C']);
const acknowledged = pivotLayoutRequestFields(['p:B', 'p:A'], 'a'.repeat(64),
  'p:B\u001fp:A', ['offscreen'], schema, false, {offscreen: 'p:offscreen'});
assert.equal(acknowledged.layoutManifest, undefined);
assert.deepEqual(acknowledged.pinnedColumnIds, ['p:offscreen']);

const oldSchema = {layoutRevision: 'a'.repeat(64), total_center_cols: 3,
  columns: [{index: 0, id: 'old'}, {index: 1, id: 'B_sales'}]};
const nextSchema = mergeSparseColSchema(oldSchema, {layoutRevision: 'b'.repeat(64),
  total_center_cols: 3, columns: [{index: 0, id: 'C_sales'}]});
assert.equal(nextSchema.columns[0].id, 'C_sales');
assert.equal(nextSchema.columns[1], null);

const {useRuntimeDataCommit} = load('src/lib/hooks/useRuntimeDataCommit.js');
const savedManifest = ['p:A', 'p:B'];
const currentManifest = {current: savedManifest};
const latestSent = {current: savedManifest};
const {runtimePayloadCommitIsCurrent} = useRuntimeDataCommit({
  clientInstanceRef: {current: 'current-client'}, stateEpochRef: {current: 5},
  requestVersionRef: {current: 9}, latestDataVersionRef: {current: 11},
  layoutRevisionRef: {current: 'a'.repeat(64)}, layoutAcknowledgedManifestRef: {current: ''},
  pivotLayoutOrderRef: currentManifest, latestLayoutSentManifestRef: latestSent,
});
const newestResponse = {kind: 'data', clientInstance: 'current-client', stateEpoch: 5, windowSeq: 9};
const newestPayload = {dataVersion: 11, layoutRevision: 'b'.repeat(64),
  colSchema: {layoutRevision: 'b'.repeat(64)}};
assert.equal(runtimePayloadCommitIsCurrent(newestResponse, newestPayload), true);
assert.equal(runtimePayloadCommitIsCurrent(newestResponse,
  {...newestPayload, colSchema: {layoutRevision: 'a'.repeat(64)}}), false);
assert.equal(runtimePayloadCommitIsCurrent({...newestResponse, windowSeq: 8}, newestPayload), false);
assert.equal(runtimePayloadCommitIsCurrent({...newestResponse, stateEpoch: 4}, newestPayload), false);
currentManifest.current = ['p:B', 'p:A'];
assert.equal(runtimePayloadCommitIsCurrent(newestResponse, newestPayload), false);

console.log('Pivot layout client contract passed');

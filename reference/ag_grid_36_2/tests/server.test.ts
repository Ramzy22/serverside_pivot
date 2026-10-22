import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { after, test } from 'node:test';
import { createReferenceHandler } from '../src/server.ts';

const server = createServer(createReferenceHandler());
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
after(() => server.close());
const address = server.address();
assert(address && typeof address !== 'string');
const origin = `http://127.0.0.1:${address.port}`;

test('reference config reports pinned options, modules and raw fixture SHA-256', async () => {
  const response = await fetch(`${origin}/reference-config.json`);
  assert.equal(response.status, 200);
  const config = await response.json();
  const raw = readFileSync(new URL('../../../tests/data/flagship_parity_rows.json', import.meta.url));
  assert.equal(config.fixtureSha256, createHash('sha256').update(raw).digest('hex'));
  assert.equal(config.options.rowModelType, 'serverSide');
  assert.equal(config.options.pivotMode, true);
  assert.deepEqual(config.filterValues.region, ['Éclair', 'München', 'São Paulo']);
  assert.deepEqual(config.filterValues.account, ['東京', 'القاهرة', '北京']);
  assert.deepEqual(config.filterValues.scope, ['Included', 'Excluded']);
  assert.deepEqual(config.filterValues.period, ['Q1', 'Q2']);
});

test('HTTP datasource processes and records an actual SSRM getRows request', async () => {
  const request = {
    startRow: 0, endRow: 1,
    rowGroupCols: [{ id: 'region', field: 'region' }],
    groupKeys: [], pivotCols: [], valueCols: [{ id: 'ventes_€', field: 'ventes_€', aggFunc: 'sum' }],
    pivotMode: false, filterModel: { scope: { filterType: 'set', values: ['Included'] } }, sortModel: [],
  };
  const response = await fetch(`${origin}/api/rows`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(request),
  });
  assert.equal(response.status, 200);
  const result = await response.json();
  assert.equal(result.rowCount, 2);
  assert.equal(result.rowData.length, 1);
  assert.equal(result.rowData[0].region, 'Éclair');
  assert.equal(result.rowData[0]['ventes_€'], 120.25);
  const trace = await (await fetch(`${origin}/request-trace.json`)).json();
  assert.equal(trace.at(-1).request.startRow, 0);
  assert.equal(trace.at(-1).response.rowCount, 2);
});

test('HTTP datasource applies application search before grouping and records the term', async () => {
  const request = {
    startRow: 0, endRow: 50,
    rowGroupCols: [{ id: 'region', field: 'region' }], groupKeys: [], pivotCols: [],
    valueCols: [{ id: 'ventes_\u20ac', field: 'ventes_\u20ac', aggFunc: 'sum' }],
    pivotMode: false, filterModel: { scope: { filterType: 'set', values: ['Included'] } },
    sortModel: [], globalSearch: 'M\u00fcnchen',
  };
  const response = await fetch(`${origin}/api/rows`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(request),
  });
  assert.equal(response.status, 200);
  const result = await response.json();
  assert.equal(result.rowCount, 1);
  assert.deepEqual(result.rowData.map((row: { region: string }) => row.region), ['M\u00fcnchen']);
  assert.equal(result.grandTotalData['ventes_\u20ac'], 79.75);
  const trace = await (await fetch(`${origin}/request-trace.json`)).json();
  assert.equal(trace.at(-1).request.globalSearch, 'M\u00fcnchen');
  assert.equal(trace.at(-1).response.rowCount, 1);
});

test('Set Filter values reflect the active other-column filter context', async () => {
  const filtered = await fetch(`${origin}/api/filter-values`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ field: 'region', filterModel: { scope: { filterType: 'set', values: ['Included'] } } }),
  });
  assert.equal(filtered.status, 200);
  assert.deepEqual((await filtered.json()).values, ['Éclair', 'München']);
  const scope = await fetch(`${origin}/api/filter-values`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ field: 'scope', filterModel: { scope: { filterType: 'set', values: ['Included'] } } }),
  });
  assert.deepEqual((await scope.json()).values, ['Included', 'Excluded']);
});

test('HTTP SSRM aggregation honors first and last rather than summing', async () => {
  const request = {
    startRow: 0, endRow: 50,
    rowGroupCols: [{ id: 'region', field: 'region' }], groupKeys: [],
    pivotCols: [], pivotMode: false,
    filterModel: { scope: { filterType: 'set', values: ['Included'] } }, sortModel: [],
  };
  for (const [aggFunc, expected] of [['first', 100.25], ['last', 20]] as const) {
    const response = await fetch(`${origin}/api/rows`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ...request, valueCols: [{ id: 'ventes_€', field: 'ventes_€', aggFunc }] }),
    });
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.rowData.find((row: { region: string }) => row.region === 'Éclair')['ventes_€'], expected);
  }
});

test('HTTP SSRM blank Set Filter includes both empty and null source cells', async () => {
  const request = {
    startRow: 0, endRow: 50, rowGroupCols: [], groupKeys: [], pivotCols: [], valueCols: [],
    pivotMode: false, sortModel: [],
    filterModel: { scope: { filterType: 'set', values: ['Included'] }, copyEmpty: { filterType: 'set', values: [null] } },
  };
  const empty = await fetch(`${origin}/api/rows`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(request),
  });
  assert.equal(empty.status, 200);
  assert.deepEqual((await empty.json()).rowData.map((row: { rowId: string }) => row.rowId), ['q2-munchen']);
  const missing = await fetch(`${origin}/api/rows`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...request, filterModel: { ...request.filterModel, copyEmpty: undefined, copyNull: { filterType: 'set', values: [null] } } }),
  });
  assert.equal(missing.status, 200);
  assert.deepEqual((await missing.json()).rowData.map((row: { rowId: string }) => row.rowId), ['q2-munchen']);
});

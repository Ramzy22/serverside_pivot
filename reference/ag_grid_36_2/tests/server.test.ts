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

import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { after, test } from 'node:test';
import { createReferenceHandler } from '../src/server.ts';
import { buildReferenceColumnDefs } from '../src/grid-config.ts';

const server = createServer(createReferenceHandler());
await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
after(() => server.close());
const address = server.address();
assert(address && typeof address !== 'string');
const origin = `http://127.0.0.1:${address.port}`;
const config = await (await fetch(`${origin}/reference-config.json`)).json();

test('every SSRM Set Filter fetches values for the active context from the server', async () => {
  const columns = buildReferenceColumnDefs(config, origin);
  for (const field of ['region', 'account', 'scope', 'period']) {
    const column = columns.find((candidate) => candidate.field === field);
    assert.equal(column?.filter, 'agSetColumnFilter');
    assert.equal(typeof column.filterParams?.values, 'function');
    assert.equal(column.filterParams?.refreshValuesOnOpen, true);
    const values = await new Promise<unknown[]>((resolve) => {
      column.filterParams.values({
        api: { getFilterModel: () => ({ scope: { filterType: 'set', values: ['Included'] } }) },
        success: resolve,
      });
    });
    const expected: Record<string, unknown[]> = {
      region: ['Éclair', 'München'],
      account: ['東京', 'القاهرة'],
      scope: ['Included', 'Excluded'],
      period: ['Q1', 'Q2'],
    };
    assert.deepEqual(values, expected[field]);
  }
});

test('measure columns offer only aggregations the SSRM backend implements', () => {
  const columns = buildReferenceColumnDefs(config);
  const sales = columns.find((candidate) => candidate.field === 'ventes_€');
  assert.deepEqual(sales?.allowedAggFuncs, ['sum', 'avg', 'min', 'max', 'count', 'first', 'last']);
  assert.equal(sales?.filter, 'agNumberColumnFilter');
  assert.equal(config.options.defaultColDef.filter, 'agTextColumnFilter');
});

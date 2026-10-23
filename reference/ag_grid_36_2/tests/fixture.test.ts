import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { queryRows } from '../src/fixture.ts';

const fixture = JSON.parse(readFileSync(new URL('../../../tests/data/flagship_parity_rows.json', import.meta.url), 'utf8'));
const scrollFixture = JSON.parse(readFileSync(new URL('../../../tests/data/flagship_scroll_rows.json', import.meta.url), 'utf8'));
const columns = (fields: string[]) => fields.map((field) => ({ id: field, field, displayName: field }));
const baseRequest = {
  startRow: 0,
  endRow: 50,
  rowGroupCols: columns(['region', 'account']),
  groupKeys: [],
  pivotCols: columns(['period']),
  valueCols: columns(['ventes_€', 'unités', 'écart']).map((col) => ({ ...col, aggFunc: 'sum' })),
  pivotMode: true,
  filterModel: { scope: { filterType: 'set', values: ['Included'] } },
  sortModel: [],
};

test('wide SSRM scenario orders pivot periods like the product and windows later groups', () => {
  const first = queryRows(scrollFixture.rows, baseRequest, { descendingPivotKeys: true });
  assert.equal(first.rowCount, 120);
  assert.deepEqual(first.pivotResultFields?.slice(0, 3), ['P23|ventes_€', 'P23|unités', 'P23|écart']);
  assert.deepEqual(first.pivotResultFields?.slice(-3), ['Total|ventes_€', 'Total|unités', 'Total|écart']);
  assert.equal(first.rowData[0]['P23|ventes_€'], 23.25);
  const later = queryRows(scrollFixture.rows, { ...baseRequest, startRow: 100, endRow: 120 },
    { descendingPivotKeys: true });
  assert.equal(later.rowData.at(-1)?.region, 'R119');
  assert.equal(later.rowData.at(-1)?.['P23|ventes_€'], 11923.25);
});

test('SSRM root groups aggregate filtered pivot values and report a row count', () => {
  const result = queryRows(fixture.rows, baseRequest);
  assert.equal(result.rowCount, 2);
  assert.deepEqual(result.pivotResultFields, [
    'Q1|ventes_€', 'Q1|unités', 'Q1|écart',
    'Q2|ventes_€', 'Q2|unités', 'Q2|écart',
    'Total|ventes_€', 'Total|unités', 'Total|écart',
  ]);
  assert.deepEqual(result.rowData.map((row) => [row.region, row['Q1|ventes_€'], row['Q2|ventes_€']]), [
    ['Éclair', 100.25, 20], ['München', 49.75, 30],
  ]);
});

test('application search narrows SSRM groups and grand totals without dropping the scope filter', () => {
  const searched = queryRows(fixture.rows, { ...baseRequest, globalSearch: ' M\u00dcNCHEN ' });
  assert.equal(searched.rowCount, 1);
  assert.deepEqual(searched.rowData.map((row) => row.region), ['M\u00fcnchen']);
  assert.equal(searched.rowData[0]['Total|ventes_\u20ac'], 79.75);
  assert.equal(searched.grandTotalData?.['Total|ventes_\u20ac'], 79.75);
  const cleared = queryRows(fixture.rows, { ...baseRequest, globalSearch: '' });
  assert.equal(cleared.rowCount, 2);
  assert.equal(cleared.grandTotalData?.['Total|ventes_\u20ac'], 200);
});

test('SSRM root supplies independently aggregated grand totals for each pivot and measure', () => {
  const result = queryRows(fixture.rows, baseRequest);
  assert.equal(result.grandTotalData?.['Q1|ventes_€'], 150);
  assert.equal(result.grandTotalData?.['Q2|ventes_€'], 50);
  assert.equal(result.grandTotalData?.['Q1|unités'], 5);
  assert.equal(result.grandTotalData?.['Q2|unités'], 5);
  assert.equal(result.grandTotalData?.['Q1|écart'], -8.25);
  assert.equal(result.grandTotalData?.['Q2|écart'], 1.5);
  assert.equal(result.grandTotalData?.['ventes_€'], 200);
  assert.equal(result.grandTotalData?.['unités'], 10);
  assert.equal(result.grandTotalData?.['écart'], -6.75);
});

test('SSRM pivot result exposes period and total columns for each grouped row', () => {
  const result = queryRows(fixture.rows, baseRequest);
  assert.ok(result.pivotResultFields?.includes('Total|ventes_€'));
  assert.ok(result.pivotResultFields?.includes('Total|unités'));
  assert.ok(result.pivotResultFields?.includes('Total|écart'));
  assert.equal(result.rowData.find((row) => row.region === 'Éclair')?.['Total|ventes_€'], 120.25);
  assert.equal(result.rowData.find((row) => row.region === 'München')?.['Total|ventes_€'], 79.75);
  assert.equal(result.grandTotalData?.['Total|ventes_€'], 200);
});

test('SSRM child route uses group keys and requested window', () => {
  const request = {
    ...baseRequest,
    groupKeys: ['Éclair'],
    startRow: 0,
    endRow: 1,
  };
  const result = queryRows(fixture.rows, request);
  assert.equal(result.rowCount, 1);
  assert.equal(result.rowData.length, 1);
  assert.equal(result.rowData[0].account, '東京');
  assert.equal(result.rowData[0]['Q1|ventes_€'], 100.25);
});

test('SSRM leaf route retains source row identity and copy edge strings', () => {
  const result = queryRows(fixture.rows, { ...baseRequest, groupKeys: ['München', 'القاهرة'] });
  assert.equal(result.rowCount, 2);
  assert.deepEqual(result.rowData.map((row) => row.rowId), ['q1-munchen', 'q2-munchen']);
  assert.equal(result.rowData[1].copyUnicode, 'Éclair München 東京 القاهرة 🚀');
  assert.equal(result.rowData[1].copyNull, null);
});

test('SSRM returns distinct sorted groups after a set filter and windows the result', () => {
  const result = queryRows(fixture.rows, {
    ...baseRequest,
    pivotMode: false,
    pivotCols: [],
    filterModel: { scope: { filterType: 'set', values: ['Included'] } },
    sortModel: [{ colId: 'region', sort: 'desc' }],
    startRow: 1,
    endRow: 2,
  });
  assert.equal(result.rowCount, 2);
  assert.deepEqual(result.rowData.map((row) => row.region), ['Éclair']);
});

test('SSRM first and last aggregation use source order within a group', () => {
  const request = { ...baseRequest, pivotMode: false, pivotCols: [], valueCols: [{ id: 'ventes_€', field: 'ventes_€', aggFunc: 'first' }] };
  const first = queryRows(fixture.rows, request);
  assert.equal(first.rowData.find((row) => row.region === 'Éclair')?.['ventes_€'], 100.25);
  const last = queryRows(fixture.rows, { ...request, valueCols: [{ id: 'ventes_€', field: 'ventes_€', aggFunc: 'last' }] });
  assert.equal(last.rowData.find((row) => row.region === 'Éclair')?.['ventes_€'], 20);
  assert.throws(
    () => queryRows(fixture.rows, { ...request, valueCols: [{ id: 'ventes_€', field: 'ventes_€', aggFunc: 'median' }] }),
    /Unsupported SSRM aggregation: median/,
  );
});

test('SSRM Set Filter blank model matches empty strings and nulls', () => {
  const request = { ...baseRequest, rowGroupCols: [], pivotMode: false, pivotCols: [], valueCols: [] };
  const empty = queryRows(fixture.rows, { ...request, filterModel: {
    scope: { filterType: 'set', values: ['Included'] },
    copyEmpty: { filterType: 'set', values: [null] },
  } });
  assert.deepEqual(empty.rowData.map((row) => row.rowId), ['q2-munchen']);
  const missing = queryRows(fixture.rows, { ...request, filterModel: {
    scope: { filterType: 'set', values: ['Included'] },
    copyNull: { filterType: 'set', values: [null] },
  } });
  assert.deepEqual(missing.rowData.map((row) => row.rowId), ['q2-munchen']);
});

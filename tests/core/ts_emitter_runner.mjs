// Line protocol for tests/core/test_ts_emitter_conformance.py:
// stdin: one JSON {query, table, columns} per line; stdout: one JSON {sql} or {error} per line.
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

const bundle = process.argv[2];
const { emitPivotSql } = await import(pathToFileURL(bundle).href);

const rl = createInterface({ input: process.stdin });
for await (const line of rl) {
    if (!line.trim()) continue;
    try {
        const { query, table, columns } = JSON.parse(line);
        process.stdout.write(JSON.stringify({ sql: emitPivotSql(query, { table, columns }) }) + '\n');
    } catch (err) {
        process.stdout.write(JSON.stringify({ error: String(err && err.message || err) }) + '\n');
    }
}

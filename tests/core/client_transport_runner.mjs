// Drives the real ClientPivotTransport with a DuckDB-Wasm stand-in whose engine is the Python DuckDB.
// in : {"rows": [...], "request": RuntimeRequestV1, "config": {...}}
// out: {"file": name, "text": json} | {"sql": "..."}   (python replies {"ok": true} | {"rows": [...]} | {"error": ...})
// out: {"response": RuntimeResponseV1, "telemetry": {...}} | {"error": "..."}
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

const { ClientPivotTransport } = await import(pathToFileURL(process.argv[2]).href);
const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const next = async () => {
    const { value, done } = await lines.next();
    if (done) process.exit(0);
    return JSON.parse(value);
};
const send = (obj) => process.stdout.write(JSON.stringify(obj, (_k, v) => (typeof v === 'bigint' ? Number(v) : v)) + '\n');
const ask = async (obj) => {
    send(obj);
    const reply = await next();
    if (reply.error) throw new Error(reply.error);
    return reply;
};
const instance = {
    async connect() {
        return { query: async (sql) => { const r = await ask({ sql }); return { toArray: () => r.rows }; } };
    },
    async registerFileText(name, text) { await ask({ file: name, text }); },
};

for (;;) {
    const { rows, request, config: rawConfig } = await next();
    const config = rawConfig || {};
    try {
        const transport = new ClientPivotTransport({ executionMode: 'client', clientEngine: 'duckdb-wasm',
            duckdbWasmInstance: instance, tableName: 'client_t', ...(config || {}) });
        transport.setData(rows);
        let response = await transport.request(request);
        if (config && config.disposeBetween) {
            transport.dispose();
            if (transport.getTelemetry().wasmReady) throw new Error('dispose left DuckDB ready');
            response = await transport.request(request);
        }
        send({ response, telemetry: transport.getTelemetry() });
    } catch (err) {
        send({ error: String((err && err.stack) || err) });
    }
}

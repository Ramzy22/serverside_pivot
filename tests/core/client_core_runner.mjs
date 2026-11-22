// Line protocol for tests/core/test_client_core_parity.py.
// in : {"request": RuntimeRequestV1, "table": name}            -> runs ClientSqlPlanner.compile + runClientCore
// out: {"sql": "..."}  (python replies {"rows": [...]} or {"error": "..."})   while the client queries DuckDB
// out: {"result": {...}} | {"unsupported": "..."} | {"error": "..."}          when the request is done
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

const { ClientSqlPlanner, runClientCore, EmitterUnsupported } = await import(pathToFileURL(process.argv[2]).href);

const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();
const next = async () => {
    const { value, done } = await lines.next();
    if (done) process.exit(0);
    return JSON.parse(value);
};
const send = (obj) => process.stdout.write(JSON.stringify(obj) + '\n');

const conn = {
    async query(sql) {
        send({ sql });
        const reply = await next();
        if (reply.error) throw new Error(reply.error);
        return { toArray: () => reply.rows };
    },
};

for (;;) {
    const { request, table } = await next();
    try {
        const plan = ClientSqlPlanner.compile(request, table);
        const result = await runClientCore(plan, conn, table);
        send({ result });
    } catch (err) {
        if (err instanceof EmitterUnsupported) send({ unsupported: err.message });
        else send({ error: String((err && err.stack) || err) });
    }
}

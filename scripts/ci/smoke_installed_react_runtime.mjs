/** Exercise a packed-and-installed React transport against a real local HTTP route. */

import assert from 'node:assert/strict';
import http from 'node:http';
import { HttpPivotTransport } from 'dash_tanstack_pivot';

let receivedRequest = null;
const server = http.createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    receivedRequest = {
        method: request.method,
        headers: request.headers,
        body: JSON.parse(Buffer.concat(chunks).toString('utf8')),
    };
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({
        schemaVersion: 1,
        kind: 'data',
        status: 'data',
        requestId: 'installed-react-request',
        payload: { data: [{ region: 'North', sales: 100 }] },
    }));
});

await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
});

try {
    const { port } = server.address();
    const transport = new HttpPivotTransport({ endpoint: `http://127.0.0.1:${port}/v1/runtime` });
    const response = await transport.request({
        schemaVersion: 1,
        kind: 'data',
        requestId: 'installed-react-request',
        table: 'sales',
        payload: { intent: 'viewport', start: 0, end: 0 },
    });
    assert.equal(receivedRequest.method, 'POST');
    assert.equal(receivedRequest.headers['content-type'], 'application/json');
    assert.equal(receivedRequest.body.table, 'sales');
    assert.deepEqual(response.payload.data, [{ region: 'North', sales: 100 }]);
    console.log('installed-react-runtime-smoke: transport request OK');
} finally {
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
}

import { createHash } from 'node:crypto';
import { createReadStream, existsSync, readFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type RequestListener, type ServerResponse } from 'node:http';
import { extname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer as createViteServer } from 'vite';
import { queryRows, type QueryRequest } from './fixture.ts';

const appRoot = fileURLToPath(new URL('..', import.meta.url));
const fixturePath = fileURLToPath(new URL('../../../tests/data/flagship_parity_rows.json', import.meta.url));
const configPath = join(appRoot, 'reference-config.json');
const rawFixture = readFileSync(fixturePath);
const fixture = JSON.parse(rawFixture.toString('utf8'));
const config = JSON.parse(readFileSync(configPath, 'utf8'));
const fixtureSha256 = createHash('sha256').update(rawFixture).digest('hex');
const trace: Array<Record<string, unknown>> = [];

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  response.end(JSON.stringify(body));
}

async function readRequest(request: IncomingMessage): Promise<unknown> {
  let body = '';
  for await (const chunk of request) {
    body += chunk;
    if (body.length > 1_000_000) throw new Error('Request exceeds local reference limit');
  }
  return JSON.parse(body);
}

function staticFallback(request: IncomingMessage, response: ServerResponse): void {
  const pathname = new URL(request.url || '/', 'http://localhost').pathname;
  const requested = pathname === '/' ? 'index.html' : pathname.slice(1);
  const path = resolve(appRoot, 'dist', requested);
  if (!path.startsWith(resolve(appRoot, 'dist') + '\\') && !path.startsWith(resolve(appRoot, 'dist') + '/')) {
    response.writeHead(404).end();
    return;
  }
  if (!existsSync(path)) {
    response.writeHead(404).end();
    return;
  }
  const mime: Record<string, string> = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };
  response.writeHead(200, { 'content-type': `${mime[extname(path)] || 'application/octet-stream'}; charset=utf-8` });
  createReadStream(path).pipe(response);
}

export function createReferenceHandler(fallback: RequestListener = staticFallback): RequestListener {
  return (request, response) => {
    void (async () => {
      const pathname = new URL(request.url || '/', 'http://localhost').pathname;
      if (pathname === '/reference-config.json' && request.method === 'GET') {
        json(response, 200, { ...config, fixtureSha256 });
        return;
      }
      if (pathname === '/request-trace.json' && request.method === 'GET') {
        json(response, 200, trace);
        return;
      }
      if (pathname === '/api/rows' && request.method === 'POST') {
        const payload = await readRequest(request) as QueryRequest;
        if (!Number.isInteger(payload.startRow) || !Number.isInteger(payload.endRow)
            || payload.startRow < 0 || payload.endRow < payload.startRow
            || !Array.isArray(payload.rowGroupCols) || !Array.isArray(payload.groupKeys)
            || !Array.isArray(payload.pivotCols) || !Array.isArray(payload.valueCols)) {
          json(response, 400, { error: 'Invalid SSRM request' });
          return;
        }
        const result = queryRows(fixture.rows, payload);
        trace.push({ at: new Date().toISOString(), request: payload, response: result });
        json(response, 200, result);
        return;
      }
      fallback(request, response);
    })().catch(() => json(response, 400, { error: 'Invalid local reference request' }));
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.REFERENCE_PORT || 4179);
  const production = process.env.NODE_ENV === 'production';
  const vite = production ? undefined : await createViteServer({
    configFile: join(appRoot, 'vite.config.ts'),
    root: appRoot,
    server: { middlewareMode: true },
    appType: 'spa',
  });
  const handler = createReferenceHandler(vite ? (request, response) => vite.middlewares(request, response) : staticFallback);
  createServer(handler).listen(port, '127.0.0.1', () => {
    process.stdout.write(`AG Grid SSRM local reference: http://127.0.0.1:${port}\n`);
  });
}

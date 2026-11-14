# Flagship host performance

This report records repeatable local measurements for the published Dash,
React, and Streamlit showcases. It deliberately separates three things that
are often conflated:

1. Shared pivot-engine work.
2. Direct V1 HTTP transport.
3. Browser scheduling and paint.

Only the first two have numbers below. No browser was attached to the local
test session on 2026-09-12, so browser-to-paint p50/p95 values are intentionally
not invented. The commands in [Browser measurement](#browser-measurement) add
those figures once Chrome or the in-app browser is available.

## Environment and method

| Item | Value |
| --- | --- |
| Machine | Intel Core i7-11800H, 8 cores / 16 logical processors, 15.8 GiB RAM |
| Python | 3.11.5 |
| DuckDB / PyArrow | 1.5.0 / 23.0.1 |
| Node | 22.20.0 |
| Date | 2026-09-12 |

All figures are local, sequential observations, not a production SLO. The
reported p95 uses nearest-rank selection; for seven core samples it is the
slowest sample, and for 30 HTTP samples it is the second-slowest sample.
Rerun on the target deployment hardware before adopting any budget.

## Shared pivot engine: applies to Dash, React, and Streamlit

Command:

```bash
python scripts/benchmark_pivot_runtime.py --rows 100000 --repeat-runs 7 --json
python scripts/benchmark_pivot_runtime.py --rows 1000000 --repeat-runs 7 --json
```

The deterministic benchmark groups `region -> product`, pivots four quarter
values, emits up to 5,000 result rows, and fetches one 200-row viewport page.
Data construction happens before timing; `load` is Arrow-to-engine loading.

| Input rows | Arrow load | Cold pivot | Repeat p50 | Repeat p95 | 200-row viewport | Cold throughput | Repeat throughput |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 95.161 ms | 122.431 ms | 60.269 ms | 135.714 ms | 36.820 ms | 816,785 rows/s | 1,659,222 rows/s |
| 1,000,000 | 422.557 ms | 118.578 ms | 77.773 ms | 148.184 ms | 49.848 ms | 8,433,253 rows/s | 12,857,916 rows/s |

Both runs observed seven cache hits and two misses. The 1M cold-pivot result
being close to the 100k result is a property of this bounded 5,000-row grouped
result and this machine's vectorized execution; it is not a promise that every
high-cardinality or wide pivot has constant query time.

## Direct V1 transport: React and Streamlit

The React and Streamlit workspaces use the same `POST /v1/runtime` request
format and direct browser transport. The test uses the live flagship state:
`region -> segment -> product`, `quarter` columns, four measures, and the
initial collapsed structural window. It returns five visible root rows, 23
columns, and a 10,386-byte JSON response.

Command (after `examples/launch_flagship_demos.ps1`):

```bash
python scripts/benchmark_flagship_transport.py --requests 30 --warmup 5 --json
# Or from serverside_pivot/:
npm run benchmark:flagship-transport -- --json
```

| Browser host route | Client HTTP p50 / p95 | Runtime gateway p50 / p95 | Payload p50 / p95 | What the number includes |
| --- | ---: | ---: | ---: | --- |
| React development host: `:5174/v1/runtime` | 73.878 / 85.401 ms | 24.352 / 27.059 ms | 10,386 / 10,386 B | Python HTTP client, Vite development proxy, runtime service, and JSON serialization |
| Streamlit direct host: `:8765/v1/runtime` | 56.283 / 72.176 ms | 24.660 / 29.522 ms | 10,386 / 10,386 B | Python HTTP client, runtime service, and JSON serialization |

The 17.595 ms p50 difference is local Vite development-proxy overhead, not a
React rendering cost. A production React deployment should route `/v1/runtime`
through its production reverse proxy and be measured there. Streamlit does not
put viewport requests through its rerun channel: its browser calls the direct
V1 endpoint, so these endpoint numbers describe its data path.

## Dash path

Dash uses the same pivot engine but transports viewport work through its Dash
callback adapter rather than the direct V1 endpoint. Its comparable benchmark
is intentionally browser-based because a meaningful result must include the
callback, browser delivery, and React commit:

```bash
python scripts/benchmark_browser_runtime.py --rows 100000 --requests 30
python scripts/benchmark_browser_runtime.py --rows 1000000 --requests 20 --clients 2
```

It reports initial hydration separately from scroll samples, plus end-to-end,
Dash callback, pivot-service, and React-commit p50/p95 values. The shared-engine
numbers above remain relevant to Dash, but they exclude callback and browser
cost, so they must not be labelled as Dash end-to-end latency.

## Browser measurement

The browser profiler works in all three hosts. Before loading a page, enable it
in DevTools and reload:

```js
localStorage.setItem('pivot-profile', '1');
location.reload();
```

After the initial request settles, clear its startup sample, make at least 20
distinct viewport interactions, then read its summary:

```js
window.__pivotProfiler.clear('flagship-react-pivot');
// scroll / expand / change a viewport at least 20 times
window.__pivotProfiler.summary('flagship-react-pivot');
```

Use `flagship-dash-pivot` for Dash and `flagship-streamlit-pivot` for
Streamlit. The summary's `p50TotalMs` and `p95TotalMs` are browser dispatch to
committed layout; `p95ResponseMs` isolates the transport/server portion and
`p95RenderMs` isolates response-to-commit work. Preserve the initial sample
separately so a warm-scroll p95 does not hide startup regressions.

## Reading the results by host

| Host | Viewport data path | Avoidable cost to watch | Primary measurement |
| --- | --- | --- | --- |
| Dash | Browser -> Dash callback -> pivot engine -> Dash response -> React | Callback serialization and server worker queueing | `benchmark_browser_runtime.py` and profiler callback fields |
| React | Browser -> `/v1/runtime` -> pivot engine -> React | Reverse-proxy and browser render cost | Direct transport benchmark plus browser profiler |
| Streamlit | Browser -> `/v1/runtime` -> pivot engine -> React in Streamlit V2 component | Endpoint network cost and component mount/render; scrolling must not cause reruns | Direct transport benchmark plus browser profiler |

The V1 transport benchmark purposefully excludes browser work. This keeps a
direct-host networking regression visible even if a fast machine masks it with
quick rendering, and it prevents runtime-service improvements from being
misreported as a rendering gain.

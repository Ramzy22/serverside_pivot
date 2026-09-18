# Flagship host examples

These are publishing-ready showcases of the same Revenue Operations workspace.
They are deliberately comparable: each begins with the same 12,000-row data
model and opens with `region → segment → product`, quarter columns, and four
measures.

| Host | Entry point | What it demonstrates |
| --- | --- | --- |
| Dash | `python examples/flagship_dash.py` | The traditional Dash component/callback adapter. |
| React | `node dash_tanstack_pivot/node_modules/vite/bin/vite.js --config examples/flagship_react/vite.config.ts` | The public `PivotWorkspace` React API with direct HTTP transport. |
| Streamlit | `streamlit run examples/flagship_streamlit.py --server.port 8502` | The Streamlit V2 host: browser viewport requests do not trigger reruns. |

React and Streamlit use the same host-neutral V1 service. Start it first:

```bash
python -m uvicorn examples.flagship_runtime_server:app --port 8765
```

For a local launch of every page, run `powershell -File examples/launch_flagship_demos.ps1` from the repository root. The launcher writes logs to `.runlogs/`.

See [flagship host performance](../docs/flagship-performance.md) for the
measured shared-engine and direct-transport baselines, host-specific measurement
boundaries, and the reproducible browser p50/p95 procedure.

The sample endpoint intentionally uses a fixed demo identity and keeps
viewport payloads inline. It is not an authentication template. In production,
provide `register_runtime_http_transport` with your authenticated principal
dependency and allow the default user-scoped payload references.

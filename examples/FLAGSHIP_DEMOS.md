# Flagship & Industry Showcases

`serverside-pivot` provides publishing-ready showcases tailored for production use cases, demonstrating sub-second server-side aggregation, virtual scrolling, and durable view state over DuckDB in-memory OLAP.

---

## 🌟 Industry Showcases (Ready to Run)

Interactive industry-specific dashboards highlighting financial risk modeling, organizational analytics, and B2B SaaS revenue intelligence:

| Showcase | Command | Port | Key Dimensions & Measures | Theme |
| --- | --- | --- | --- | --- |
| 📈 **Trading & Risk Portfolio** | `python examples/demo_trader.py` | `8051` | 50,000 positions · Asset Class, Desk, Ticker, Tenor · VaR (95%), DV01, Daily PnL | Financial Dark Terminal |
| 👥 **People & HR Analytics** | `python examples/demo_hr.py` | `8052` | 35,000 employees · Dept, Sub-Team, Seniority Level · Headcount, Base Comp, Bonus Pool, Attrition Risk | Clean Corporate Flash |
| 🚀 **B2B SaaS Growth & Cohorts** | `python examples/demo_saas.py` | `8053` | 40,000 accounts · Customer Segment, Tier, Channel · ARR, MRR, NRR, LTV/CAC | Obsidian Violet Dark |
| 🎛️ **Unified Showcase Hub** | `python examples/demo_showcase_hub.py` | `8054` | All three industries unified in a tabbed multi-workspace switcher | Multi-theme |
| ☁️ **Cloud Data Warehouse & Ibis Pushdown** | `python examples/demo_cloud_warehouse.py` | `8055` | 40,000 workloads · Zero-Copy Lazy `ibis.Table` · Live Snowflake/BigQuery/Postgres/MotherDuck Capability & SQL Inspector | Enterprise Cloud Flash |
| ⚡ **Client-Side DuckDB-Wasm & Hybrid Mode** | `python examples/demo_clientside_version.py` | `8056` | Auto-Detect (`auto`) vs Forced `client` / `server` · Browser `DuckDbWasmExecutor` + `ClientPivotTransport` · Zero Server Roundtrips | Hybrid Flash |

---

## 🌐 Flagship Host Architecture Examples

Showcases demonstrating host-neutral execution across Dash, React, and Streamlit using the same Revenue Operations workspace (12,000 rows):

| Host | Entry point | What it demonstrates |
| --- | --- | --- |
| **Dash** | `python examples/flagship_dash.py` | Traditional Dash component/callback adapter. |
| **React** | `node serverside_pivot/node_modules/vite/bin/vite.js --config examples/flagship_react/vite.config.ts` | Public `PivotWorkspace` React API with direct HTTP transport. |
| **Streamlit** | `streamlit run examples/flagship_streamlit.py --server.port 8502` | Streamlit zero-rerun component: viewport requests do not trigger Python re-execution. |

React and Streamlit share the host-neutral V1 service:

```bash
python -m uvicorn examples.flagship_runtime_server:app --port 8765
```

For an automated local launch of all flagship pages, run:
```powershell
powershell -File examples/launch_flagship_demos.ps1
```

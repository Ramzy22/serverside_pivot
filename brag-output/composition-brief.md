# Hyperframes Composition Brief: serverside-pivot

## Objective
Create a high-impact, dynamic 22-second launch video for `serverside-pivot` with GSAP kinetic typography, 3D camera choreography, whip-pan transitions, beat-synced reveals, and verified live UI captures.

## Output
- Composition directory: `brag-output/composition/`
- Rendered video: `brag-output/brag.mp4`
- Format: landscape — 1920x1080
- Duration: 22 seconds

## Source Material
- Project root: `c:\Users\ramzy\Downloads\serverside_pivot`
- Primary files read: `README.md`, `examples/demo_datasets.py`, `examples/demo_trader.py`, `examples/demo_hr.py`, `examples/demo_saas.py`
- Product name: `serverside-pivot`
- Tagline / strongest claim: `2 Lines of Python. Any Database. 100M+ Rows at 60 FPS.`
- Key UI or visual moments to recreate:
  1. 2-line Python setup (`create_tanstack_adapter(backend_uri)` + `register_pivot_app(app, ...)`).
  2. Data pipeline connecting local `.parquet` / `.csv` files OR massive 100M+ row Snowflake, BigQuery, Postgres, and DuckDB warehouses.
  3. Live Institutional Trading Desk UI (`shot_trader.png` & `shot_trader_full.png`) with inline PnL area sparklines, column heatmaps, data bars, pinned hierarchy, and docked ECharts bar graph.
  4. Live Global HR Workforce UI (`shot_hr.png`) & B2B SaaS Cohort Intelligence UI (`shot_saas.png`) with table-wide heatmaps, column sparklines, donut/area charts, and infinite virtual scrolling.
- Copy that must appear verbatim:
  - `2 LINES OF PYTHON`
  - `create_tanstack_adapter("snowflake://..." | "local.parquet")`
  - `register_pivot_app(app, adapter_getter=get_adapter)`
  - `100M+ ROWS · 12ms DUCKDB & ARROW ENGINE`
  - `pip install serverside-pivot`

## Creative Direction
- Tone preset: `cinematic` / `app-store`
- Creative direction: High-energy developer launch film with kinetic impact typography, 3D perspective browser choreography, glowing data callouts, and beat-synced cuts.
- Visual Identity:
  - Background: `#060913` -> `#0B1120`
  - Text: `#F8FAFC`
  - Accents: `#38BDF8` (Cyan), `#10B981` (Emerald), `#8B5CF6` (Violet), `#F43F5E` (Rose)
  - Display font: `Plus Jakarta Sans` / `Inter` (800/900 weight)
  - Monospace font: `JetBrains Mono`

## Storyboard (22.0s total)
1. **Scene 1 (0.0s – 4.5s) — THE 2-LINE LOW-CODE HOOK:**
   - Kinetic impact slam: `"100M+ ROWS."` -> `"2 LINES OF PYTHON."`
   - Sleek 3D floating IDE card with syntax-highlighted typing animation of `create_tanstack_adapter(...)` and `register_pivot_app(...)` + `ZERO SQL BOILERPLATE` badge.
2. **Scene 2 (4.5s – 8.5s) — ANY DATA SOURCE (SMALL FILE TO BIG WAREHOUSE):**
   - Whip-pan transition into animated data architecture hub:
   - Left: `Small Local Files` (`trades.parquet`, ` cohorts.csv`, `in-memory Arrow`).
   - Right: `Massive Cloud Warehouses` (`DuckDB`, `Snowflake`, `BigQuery`, `PostgreSQL` — `100,000,000+ rows`).
   - Animated glowing data pulses converging into `serverside-pivot Engine (O(D) Query Batching · 12ms Latency)` -> `Dash · React · Streamlit`.
3. **Scene 3 (8.5s – 13.5s) — LIVE WEB REVEAL: INSTITUTIONAL TRADING DESK:**
   - 3D perspective browser reveal of `shot_trader_full.png` transitioning into a dynamic camera zoom onto `shot_trader.png`.
   - Animated callout badges pointing directly to:
     - `INLINE SPARKLINE CELLS` (Area PnL Trends)
     - `COLUMN HEATMAP & DATA BARS`
     - `PINNED HIERARCHY & FILTER BADGE`
     - `DOCKED LIVE BAR CHART`
4. **Scene 4 (13.5s – 18.0s) — MULTI-INDUSTRY SHOWCASE: HR ANALYTICS & SAAS COHORTS:**
   - Split/sliding 3D showcase of `shot_hr.png` (Light Flash Theme, Table Heatmap, Column Sparklines, Donut Chart) and `shot_saas.png` (Dark Violet Theme, Cohort ARR Area Chart, Data Bars, Immersive Canvas).
   - Impact callout bar: `INFINITE VIRTUAL SCROLL · O(D) HIERARCHY · FULL EXCEL XLSX EXPORT`.
5. **Scene 5 (18.0s – 22.0s) — THE VERDICT & OUTRO:**
   - High-contrast comparison slam:
     - `Perspective`: `Crashes Browser WASM (>2GB RAM)`
     - `AG Grid Enterprise`: `$2,500/dev Empty Shell (Write Your Own SQL)`
     - `serverside-pivot`: `Full-Stack Engine Included · Open Source`
   - Final hero command card: `pip install serverside-pivot`.

## Audio
- Music: `assets/music/happy-beats-business-moves-vol-1-by-ende-dot-app.mp3` (120.19 BPM)
- Beat locks: `4.53s`, `8.52s`, `13.51s`, `18.02s`
- SFX: keyboard clicks during code typing (`0.8s-2.4s`), impact bells/thuds on scene transitions (`0.1s`, `4.5s`, `8.5s`, `13.5s`, `18.0s`), card slides on callouts.

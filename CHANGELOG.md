# Changelog

All notable changes to `serverside-pivot` (PyPI) and `serverside_pivot` (npm) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.99] - 2026-11-27

### Added
- Native DuckDB direct-aggregate level-table tier (`core/physical/level_tables.py`, `long_form.py`, `native_view.py`, `parameters.py`, `core/catalog.py`) with bound interaction parameters, long-form pivot storage, and per-grant scope binding (`core/authority/level_binding.py`).

### Changed
- Optimized level-table query compilation and execution: direct DuckDB `GROUPING SETS` with two-stage additive rollups, Top-N pre-order paging with `list_contains`, equi-key Hash Joins for hierarchical sorts, filtered depth-1 reuse, Arrow widening/window slicing, and leaner response edge mapping.
- Updated workspace UI controls, toolbar menus, filter popovers, charts, and Dash/React/Streamlit frontend bundles.

## [0.0.98] - 2026-11-26

0.0.97 was tagged but never published (its release qualification failed); 0.0.98 supersedes it.

### Removed
- The partial-cube cache tier and its pivot-column merges (`core/cube_cache.py`, `core.cube` namespace): measured 1.5–2.0x slower than per-level aggregation at 1M rows, scenario for scenario, against the AG Grid SSRM harness (`docs/architecture/M6_PERFORMANCE_PLAN.md` §3.1).

### Changed
- The physical planner chooses `leaf_partials` only with two or more visible levels and no pivot columns, where it measured faster (10M fully expanded: 304 vs 468 ms); `union_all` serves every other query.

### Fixed
- A pinned pivot column no longer makes the grid re-request its viewport endlessly once scrolled away from it: the delivered column window is the leading index run of the schema reply, not its last entry (the server appends pinned columns, so `[57..74, 0]` read as `57..57`). The loop starved later work, such as "Copy Current Table", on slower machines.

## [0.0.97] - 2026-11-26

### Changed
- Golden core read path (M6.1–M6.3): one statement per view returns the window, its total and the grand total; dead formula-source rows, the separate count statement and per-row JSON keys are removed; column members are resolved once per request.
- Child levels are aggregated only over their visible parents' records (parent restriction), in every strategy and in sort-key lookups.
- Partial-cube strategy: mergeable measures (sum, count, min, max, avg), including pivot cells and member-key sorts, are merged from one long-form scan, with a cube cache tier bound to the data epoch, scope and cache budget. A forced refresh bypasses it.
- Upgraded the React toolchain to React 18 and removed the `styled-components` peer dependency; the workspace shell gains a compact layout under 720 px.
- Added the server benchmark harness against AG Grid's SSRM datasource (`scripts/benchmark_core_vs_aggrid.py`).

### Fixed
- SQLite: deep core trees no longer hit "parser stack overflow" (derived tables are flattened into CTEs); `starts_with`/`ends_with` filters no longer treat `%`/`_` as wildcards.
- Polars: percentile pivot cells over the measure axis are refused with a typed capability error instead of failing mid-request.

## [0.0.96] - 2026-11-18

### Changed
- Added a dedicated Python/Dash/Streamlit README (`serverside_pivot/README_PYPI.md`) for PyPI while keeping `serverside_pivot/README.md` focused on the React/npm package.
- Updated workspace components, client-side runtime, and frontend bundles.

### Fixed
- Allowed explicit viewport error recovery retries during pending structural transitions in `useServerSideRowModel.js`.

## [0.0.95] - 2026-11-14

### Fixed
- Isolated wheel import checks in `scripts/flagship_artifacts.py` (`-I` / stripped `PYTHONPATH`) and removed global `PYTHONPATH` export from `.github/workflows/release.yml` so installed release qualification verifies wheel `site-packages` cleanly.

## [0.0.94] - 2026-11-14

### Fixed
- Updated `scripts/flagship_artifacts.py` to bind the copied `examples/flagship_react/package-lock.json` tarball integrity to the freshly packed `serverside_pivot` tarball during release qualification.

## [0.0.93] - 2026-11-14

### Changed
- Aligned Python and npm package layout under `serverside_pivot`.
- Added single-source versioning (`VERSION` + `scripts/sync_version.py`) across Python, Dash, Streamlit, React, and example manifests.
- Enforced atomic dual-registry publishing (PyPI and npm), post-publish clean-room verification, and automated GitHub Release notes in `.github/workflows/release.yml`.

## [0.0.91] - 2026-11-09

### Added
- Dual-engine client-side pivot execution (`DuckDB-WASM` and in-memory JavaScript execution) alongside hybrid mode (`clientPivotTransport`, `clientSqlPlanner`, `duckdbWasmExecutor`, `modeResolver`, `clientResponsePostProcessor`).
- Standalone React package entry (`serverside_pivot` / `serverside_pivot/react`) with TypeScript declarations and runtime transport exports.
- Cloud data warehouse Ibis compatibility tests and backend capabilities catalog (`capabilities.py`, `table_catalog.py`, `client_mode.py`).
- Edit propagation and mutation executor (`mutation_executor.py`) for hierarchical pivot cell updates.

### Changed
- Updated Dash, React, and Streamlit component builds and manifests to `0.0.91`.

## [0.0.90] - 2026-11-07

### Changed
- Aligned Dash, React, and Streamlit package manifests to `0.0.90`.

## [0.0.88] - 2026-08-26

### Released
- Published `serverside-pivot==0.0.88` to PyPI.

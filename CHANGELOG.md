# Changelog

All notable changes to `serverside-pivot` (PyPI) and `serverside_pivot` (npm) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

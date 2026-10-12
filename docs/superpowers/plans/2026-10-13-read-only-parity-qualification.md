# Read-only Parity and Release Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a requirement-level, live AG Grid comparison and a fail-closed browser qualification result for the exact read-only Dash, React, and Streamlit release artifacts.

**Architecture:** A validated requirement contract drives a generated parity matrix and required test IDs. An isolated AG Grid reference app records comparator observations; an installed-artifact runner executes product browsers and writes a hash-bound per-requirement manifest. Shared assertions and fixture oracles are reused, while host-specific transport/lifecycle assertions remain explicit.

**Tech Stack:** Python 3, pytest, Selenium/Chrome, Vite/TypeScript, AG Grid Enterprise 36.2.0, Dash, React, Streamlit, npm pack, Python wheel, SHA-256.

**Spec:** `docs/superpowers/specs/2026-10-13-read-only-parity-qualification-design.md`

## Global Constraints

- Reference AG Grid Enterprise **36.2.0**, JavaScript **Server-Side Row Model**; name any different row model per requirement.
- The reference is local evaluation only; no AG Grid key or Enterprise bundle ships in our packages.
- Editing is explicitly unavailable and is not a read-only release requirement.
- Each supported inventory workflow maps to at least one atomic requirement; host-only workflows apply only to their named host.
- Required `skipped`, `blocked`, missing, stale, or hash-mismatched cases fail qualification; `not_applicable` requires a declared host exclusion.
- Product release qualification and verified-parity status are separate results; neither may infer success from historical runs.
- An observed product regression fails its promised requirement; do not downgrade `required` or narrow the public promise merely to turn a failed test green.
- Raw evidence stays private; the published manifest is allowlisted and sanitized using the existing export-qualification rules.
- Use a fresh checkout/build and installed wheel/npm tarball; the React browser must load a production build from the packed package, not Vite's source alias.

## Review Focus

1. A supported inventory ID disappears from the requirement file: Task 1's orphan-coverage test must fail validation.
2. One host silently skips a required Selenium case: Task 5's JUnit/result-merging test must mark the release failed.
3. A server returns a success toast but clipboard/download content is wrong: Tasks 7 and 9 must compare parsed browser outputs with an independent fixture oracle.
4. A cancelled request frees admission while execution continues: Task 6 must observe actual worker cleanup before accepting the retry.
5. A React page loads a previous bundle while a new tarball was built: Task 4's served-asset hash test must reject the run.

---

## File map and interfaces

- `tests/flagship_requirements.json`: source-of-truth atomic contract data; contains inventory IDs, AG Grid references/configuration, product promises, constraints, applicability, and test IDs.
- `tests/flagship_contract.py`: define `Requirement` and `CaseResult`, parse/validate contract and render the Markdown parity matrix. Public API: `load_requirements(path: Path) -> tuple[Requirement, ...]`, `validate_requirements(requirements: tuple[Requirement, ...], capabilities: tuple[FlagshipCapability, ...]) -> None`, `render_matrix(requirements: tuple[Requirement, ...], results: Mapping[str, object] | None = None) -> str`.
- `tests/data/flagship_parity_rows.json`: single deterministic source fixture, loaded by the reference app and by the product runtime when `PIVOT_FLAGSHIP_PARITY_DATA=1`.
- `reference/ag_grid_36_2/`: isolated pinned package and Vite app, with an SSRM datasource over the shared fixture and recorded grid options. It is never included in distributable package files.
- `tests/test_flagship_ag_grid_reference.py`: opt-in local comparator browser cases, emitting one case result per requirement and reference condition.
- `scripts/flagship_artifacts.py`: clean build/install helpers and `ArtifactBinding` (commit, wheel/npm tarball, loaded bundles/chunks, fixture hashes, tool/host/browser versions).
- `scripts/flagship_manifest.py`: merge expected requirements with reference/product case records and evaluate separate `releaseResult` and `parityResult` fields. Public API: `evaluate_manifest(requirements: tuple[Requirement, ...], binding: ArtifactBinding, cases: Sequence[CaseResult]) -> dict[str, object]` and `publish_safe_manifest(raw: Mapping[str, object], destination: Path) -> None`.
- `scripts/certify_flagship.py`: orchestrate comparator, clean installed hosts, browser suites, and manifest; retain the existing `--export-qualification` behavior.
- `tests/flagship_browser_support.py`: reusable Selenium fixture/actions and safe case-evidence writer, extracted only as new tests need them.
- `tests/test_flagship_workflows.py`: new product browser requirement/journey cases; keep existing export tests in `tests/test_flagship_certification.py` and attach their result IDs.
- `docs/flagship-functionality-certification.md`: describe the new gate and generated matrix truthfully.

`Requirement` has `id`, `inventory_id`, `task`, `expected`, `ag_grid` (version/edition/row_model/options/docs/observed behavior/reference test ID, or explicit `not_applicable` for host-only integration), `product_behavior`, `difference`, `constraints`, `hosts`, `required`, `shared_tests`, and `browser_tests`. `CaseResult` has `requirement_id`, `system` (`ag_grid` or `product`), `host` (product only), `test_id`, `result`, `observed`, `browser`, and private evidence paths. Stable IDs are never inferred from a pytest filename.

### Task 1: Contract validator and complete inventory mapping

**Files:** Create `tests/flagship_contract.py`, `tests/flagship_requirements.json`, `tests/test_flagship_contract.py`; modify `tests/flagship_capabilities.py`, `docs/flagship-functionality-certification.md`.

**Interfaces:** Produces `Requirement`, `CaseResult`, `load_requirements`, `validate_requirements`, and `render_matrix` exactly as in the file map. No browser dependency.

- [ ] **Step 1: Write failing contract tests.** In `tests/test_flagship_contract.py`, assert all 44 supported inventory IDs are covered, `editing` is excluded, host-only IDs have one applicable host and an explicit AG Grid `not_applicable` reason, every other requirement has the stated reference fields, and at least ten selected-range-copy edge requirements have distinct IDs. Add parametrized failures for duplicate ID, missing inventory ID, unknown host, empty expected/constraints/reference config, missing test mapping, and changed supported inventory coverage.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_contract.py -q`.** Expect failures for absent loader/contract.
- [ ] **Step 3: Implement the frozen `Requirement` parser, validator, JSON contract and renderer.** Atomic requirements must describe exact outcomes and may classify a product-specific operation as different/unsupported. Seed all inventory workflows now; use `unverified` where behavioral evidence is absent, never an invented pass.
- [ ] **Step 4: Run `python -m pytest tests/test_flagship_contract.py tests/test_flagship_certification.py -q -m 'not flagship'`.** Expect pass; verify the generated matrix explicitly shows unverified rows rather than blanket “required” pass claims.
- [ ] **Step 5: Commit only Task 1 files.** Message: `feat: define validated read-only parity contract`.

### Task 2: Deterministic reference fixture and AG Grid SSRM app

**Files:** Create `reference/ag_grid_36_2/package.json`, `package-lock.json`, `index.html`, `vite.config.ts`, `src/main.ts`, `src/server.ts`, `src/fixture.ts`, `tests/data/flagship_parity_rows.json`, `tests/test_flagship_reference_fixture.py`; modify `examples/flagship_demo_data.py` to load that fixture under `PIVOT_FLAGSHIP_PARITY_DATA=1`.

**Interfaces:** Reference app exposes `/reference-config.json` with version, module/options and fixture SHA-256; its datasource serves the deterministic rows and records `getRows` requests without product-specific data transformations.

- [ ] **Step 1: Write failing fixture tests.** Assert AG Grid packages equal `36.2.0`, `rowModelType` equals `serverSide`, `pivotMode` is enabled, required cell/clipboard/export/chart/state modules are registered, duplicate-extrema/Unicode/tab/newline/empty fixture cases are stable, and the reference directory is excluded from Python/npm package contents.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_reference_fixture.py -q`.** Expect failure because the reference app does not exist.
- [ ] **Step 3: Add the isolated app, pinned lockfile, shared JSON fixture and guarded product-fixture loader.** Use AG Grid's local unlicensed evaluation mode; document the watermark/console-warning allowlist. A genuine SSRM datasource handles group, pivot, filter, sort and windows over the same JSON rows and locale as the product. Do not use a client-side row model and label it SSRM.
- [ ] **Step 4: Run fixture tests and a local production build.** `python -m pytest tests/test_flagship_reference_fixture.py -q`; `npm --prefix reference/ag_grid_36_2 ci`; `npm --prefix reference/ag_grid_36_2 run build`. Expect pass and a non-empty `dist`.
- [ ] **Step 5: Commit only Task 2 files.** Message: `test: add pinned AG Grid SSRM reference fixture`.

### Task 3: Live reference browser observations

**Files:** Create `tests/test_flagship_ag_grid_reference.py`, `tests/flagship_reference_support.py`; modify `reference/ag_grid_36_2/src/main.ts` only for observable test hooks.

**Interfaces:** Each passing comparator case writes a `CaseResult` with `system='ag_grid'`; live observations supplement, never replace, AG Grid documentation links in the contract.

- [ ] **Step 1: Write failing reference tests.** Parametrize required comparator actions for startup/pivot/aggregate, filter/sort, hierarchy, selection/copy-with-headers (including pin/hidden/order/special text/unloaded row), sizing/pinning, state restore, and export/chart behavior. Assert displayed state and parsed output, not a click or request alone. Capture SSRM `getRows` traffic.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_ag_grid_reference.py -q -m flagship` with the local app.** Expect absent hooks/case evidence to fail; an unavailable browser is `blocked`, not pass.
- [ ] **Step 3: Implement stable hooks and evidence recording.** Capture screenshot, browser version, reference config, datasource trace, parsed clipboard/download, and expected watermark diagnostics. A behavior that cannot be exercised remains `unverified` in the contract.
- [ ] **Step 4: Rerun the reference suite and inspect its case-result JSON.** All required comparable IDs must have one valid observation or the parity gate remains failed; host-only `not_applicable` IDs are exempt by contract.
- [ ] **Step 5: Commit only Task 3 files.** Message: `test: record live AG Grid reference behavior`.

### Task 4: Build and serve installed release artifacts

**Files:** Create `scripts/flagship_artifacts.py`, `tests/test_flagship_artifacts.py`, `examples/flagship_react/package-lock.json`; modify `scripts/certify_flagship.py`, `examples/flagship_react/vite.config.ts`, `examples/flagship_react/package.json`.

**Interfaces:** `build_artifacts(source: Path, output: Path) -> ArtifactBinding` builds wheel/npm tarball and all host bundles; `start_installed_hosts(binding: ArtifactBinding, output: Path)` serves Dash/React/Streamlit without importing source packages. `ArtifactBinding` exposes `sha256`, paths, versions and the set of expected loaded asset hashes.

- [ ] **Step 1: Write failing artifact tests.** Assert the React build resolves the packed `dash_tanstack_pivot/react` in a temporary install, serves `vite preview`/static `dist` rather than `/@vite/client`, and rejects a served asset whose SHA-256 differs from binding. Assert wheel/Streamlit asset hashes and fixture hash are recorded.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_artifacts.py -q`.** Expect failure for missing helper and current Vite development launch.
- [ ] **Step 3: Implement isolated builds and host startup.** Reuse the existing export-qualification build path without changing its safety semantics; keep `--reuse-running` outside release certification or label it non-release diagnostics. Avoid path aliases to checkout `dist/react` in the release React app.
- [ ] **Step 4: Run artifact tests and one installed-package smoke request in each host.** Confirm loaded hashes match the wheel/npm tarball/build record and the React network trace has no Vite dev client.
- [ ] **Step 5: Commit only Task 4 files.** Message: `build: certify installed flagship artifacts`.

### Task 5: Fail-closed per-requirement manifest

**Files:** Create `scripts/flagship_manifest.py`, `tests/test_flagship_manifest.py`; modify `scripts/certify_flagship.py`; retain existing export-safe publication logic.

**Interfaces:** `evaluate_manifest` and `publish_safe_manifest` exactly as in the file map; return independent `releaseResult` and `parityResult`, and per-requirement/per-host results with evidence and binding references.

- [ ] **Step 1: Write failing evaluator tests.** A full synthetic case set passes; omit one host case, skip a required case, use `not_applicable` on an applicable host, mismatch a loaded bundle hash, omit live AG Grid reference, reuse another commit's case, or include a secret/raw output in the public manifest. Assert the appropriate gate fails and the secret is absent from publication. Test JUnit skipped count cannot be ignored.
- [ ] **Step 2: Run `python -m pytest tests/test_flagship_manifest.py -q`.** Expect missing evaluator failures.
- [ ] **Step 3: Implement case merge, binding validation, separate gate evaluation and allowlisted serialization.** Preserve safe internal evidence references and explicit skip/block reasons; no broad `except` may turn missing evidence into pass.
- [ ] **Step 4: Run manifest tests plus existing export-qualification runner tests.** `python -m pytest tests/test_flagship_manifest.py tests/test_flagship_certification.py -q -m 'not flagship'`. Expect pass.
- [ ] **Step 5: Commit only Task 5 files.** Message: `test: fail closed on missing flagship evidence`.

### Task 6: Error and cancellation recovery qualification

**Files:** Create `tests/test_flagship_workflows.py`, `tests/flagship_browser_support.py`; modify `examples/flagship_runtime_server.py` only to add deterministic test-only failure/delay controls guarded from normal demos.

**Interfaces:** Browser tests write `CaseResult` records through a shared `record_case(requirement_id: str, host: str, observed: Mapping[str, object], evidence_dir: Path) -> None` helper; no raw token/row data in the public record.

- [ ] **Step 1: Write failing three-host tests for `runtime-error` and `large-delivery`.** Inject a request failure, assert understandable visible error, retry, no stale cells and successful result. Start costly delivery, cancel, observe backend execution completion and admission reservation/slot lifetime, then successfully retry. Add a shared cancellation assertion that fails if admission is released while executor work remains.
- [ ] **Step 2: Run those tests under the installed-host runner.** Expect missing assertions/hooks to fail rather than skip.
- [ ] **Step 3: Add minimal guarded fixture controls and shared browser evidence helpers.** Correlate action, request ID, response, visible state, and cleanup; assert rejected/failed operations do not create downloads.
- [ ] **Step 4: Rerun the two workflows across Dash, React and Streamlit.** Expect six per-host passes with browser/network traces and successful retry evidence.
- [ ] **Step 5: Commit only Task 6 files.** Message: `test: qualify flagship recovery and cancellation`.

### Task 7: Exact selection, clipboard and delivery qualification

**Files:** Modify `tests/test_flagship_workflows.py`, `tests/test_flagship_certification.py`, `tests/flagship_browser_support.py`; modify contract results only if observed behavior is narrower than promised.

**Interfaces:** Reuse `record_case` and the existing export oracle `tests/export_qualification_fixture.py`; all clipboard and download checks parse browser-observed bytes/text.

- [ ] **Step 1: Write failing three-host tests.** Cover selected logical cells, pinned-no-duplicate/order, hidden columns, formatted/raw values, tabs/newlines/Unicode/empty, headers opt-in, unloaded rows, keyboard copy in grid and native copy in search/dialog; separately cover Copy Current Table, Copy All Records, CSV/XLSX totals/order/merged headers, and constrained-limit rejection. Assert exact parsed output rather than toast text.
- [ ] **Step 2: Run the selection/delivery cases.** Expect any missing behavior or wrong output to fail; record a confirmed limitation as `narrower`/`unsupported` in the comparison without changing the product promise or required test absent a separately approved scope change.
- [ ] **Step 3: Reuse and attach existing export qualification evidence to stable requirement IDs; implement only test support needed for new cases.** Keep raw files private and sanitized summaries public.
- [ ] **Step 4: Rerun selection/delivery cases across three hosts and the `filter → pin → horizontal scroll → select → copy with headers` journey.** Compare exact clipboard content against the fixture oracle.
- [ ] **Step 5: Commit only Task 7 files.** Message: `test: qualify read-only selection and delivery`.

### Task 8: Navigation, layout, filtering and persistence qualification

**Files:** Modify `tests/test_flagship_workflows.py`, `tests/flagship_browser_support.py`, `tests/flagship_requirements.json` only for observed constraints.

**Interfaces:** Reuse `record_case`; geometry assertions report measured widths/scroll positions and specified tolerances.

- [ ] **Step 1: Write failing three-host cases for inventory IDs `hierarchy-expand`, `hierarchy-expand-all`, both virtual scrolls, `saved-scroll`, `layout-field-zones`, `layout-values`, `layout-swap`, `global-filter`, `header-filter`, `sorting`, `filter-options`, `totals`, `row-numbers`, `layout-modes`, `density`, `column-sizing`, `column-pinning`, `persistence`, and `undo-redo`.** Assert resulting order/values/totals/indicators and correlated server requests, not only UI actions.
- [ ] **Step 2: Run these cases; inspect failures by requirement ID.** A missing selector or browser capability is `blocked`, not pass.
- [ ] **Step 3: Add exact fixture oracles and stable geometry probes.** Cover resize/auto-size/pin during horizontal scrolling; verify saved filters/layout/scroll after reload, with observed width/position tolerance in the contract.
- [ ] **Step 4: Rerun all cases and the `resize → density → save → reload → restore` journey.** Require model and visible geometry to match; run analytical layout `aggregation → expand → swap axes → export` with independent totals.
- [ ] **Step 5: Commit only Task 8 files.** Message: `test: qualify navigation and saved layout state`.

### Task 9: Presentation, charts, advanced sidebar and host lifecycle qualification

**Files:** Modify `tests/test_flagship_workflows.py`, `tests/flagship_browser_support.py`, `tests/flagship_requirements.json`; add focused host-specific test module `tests/test_flagship_host_lifecycle.py`.

**Interfaces:** Reuse `record_case`; chart results include source range, update/sizing/cleanup observations and artifact summary.

- [ ] **Step 1: Write failing cases for inventory IDs `number-format`, `cell-format`, `color-scales`, `themes`, `zoom-immersive`, `formula-columns`, `custom-dimensions`, `report-mode`, `range-chart`, `chart-pane`, `chart-export`, and three host-only IDs.** Assert raw-value/export semantics remain unchanged after presentation changes; chart source and refreshed data are correct; pane cleanup leaves grid usable.
- [ ] **Step 2: Run them across applicable hosts.** Expect missing behavior to fail; record a confirmed narrower comparison without changing a product promise absent a separately approved scope change.
- [ ] **Step 3: Add focused assertions for Dash callbacks, React mount/unmount/direct V1 transport, and Streamlit iframe resize/no rerun.** Parse chart export rather than accepting an export toast.
- [ ] **Step 4: Rerun the complete presentation/chart/host set.** Check no host-only case is counted as an inapplicable-host pass.
- [ ] **Step 5: Commit only Task 9 files.** Message: `test: qualify presentation charts and host lifecycles`.

### Task 10: Complete coverage audit and current-release run

**Files:** Modify `scripts/certify_flagship.py`, `tests/test_flagship_contract.py`, `docs/flagship-functionality-certification.md`; create `docs/qualification/2026-10-13-historical-flagship-run.md`; generated evidence stays outside Git unless its safe manifest is intentionally published.

**Interfaces:** CLI `python scripts/certify_flagship.py --release-qualification --artifact-dir <new-empty-dir>` returns zero only when release and required parity gates pass; legacy export-qualification command retains its existing behavior.

- [ ] **Step 1: Write failing end-to-end gate tests.** Prove the CLI rejects a missing requirement/browser case, old-commit case, skipped case, reused running dev host, and absent browser; assert the matrix has no supported inventory orphan and every required ID has an applicable case mapping.
- [ ] **Step 2: Run unit gate tests and document the older failed run.** Inspect retained logs/traces from the 2026-09-27 failed run; state the proved cause or the exact evidence gap, never infer a pass from the 2026-09-13 successful run.
- [ ] **Step 3: Wire all suites into the new release CLI and update the public qualification documentation.** Preserve explicit outcome `failed` if any mandatory case is missing, even when pytest's process exits zero.
- [ ] **Step 4: Run typecheck, Python unit/runtime tests, production builds, export qualification, live AG Grid comparator, and the full installed-host release qualification in a new artifact directory.** Inspect manifest commit/hash/browser/host versions, every requirement result, all skips/failures, screenshots/traces, parsed clipboard/download summaries and loaded-asset hash list. Report actual failures; do not mark completion if a gate is red.
- [ ] **Step 5: Commit Task 10 code/docs only; report the manifest path and exact result.** Message: `test: gate releases on full read-only parity evidence`.

## Plan self-review

Each spec section maps to a task: fixed comparator and fixture (2–3), atomic contract (1), installed artifacts (4), safe fail-closed manifest (5), risk-first and combination workflows (6–9), historical investigation and current run (10). The task interfaces use the same `Requirement`, `CaseResult`, `ArtifactBinding`, and `record_case` names throughout. The five Review Focus failures have explicit tests in Tasks 1, 4, 5, 6, and 7/9. The reference and product gates remain separate; a missing AG Grid observation can never be called verified parity, and a product-host miss can never pass release qualification.

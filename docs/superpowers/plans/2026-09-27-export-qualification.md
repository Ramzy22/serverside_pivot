# Export Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify CSV/XLSX downloads and TSV/HTML clipboard delivery across Dash, React, and Streamlit, with evidence that gates release publishing.

**Architecture:** Preserve the runtime as the source of export semantics and limits. Extend the shared frontend command boundary to select CSV or XLSX, drive all three hosts from one deterministic qualification fixture, and use a two-phase browser runner to produce normal and limit-rejection evidence. The release job invokes that runner after host builds and before publishing.

**Tech Stack:** Python 3.11, pytest, Selenium/Chrome, Dash, React 18/Vite, Streamlit component V2, FastAPI runtime transport, ZIP/XML XLSX inspection, CSV standard library.

**Spec:** `docs/superpowers/specs/2026-09-27-export-qualification-design.md`

## Global Constraints

- Published host download formats are exactly CSV and XLSX; XLSX remains the default for existing export commands.
- TSV and HTML remain clipboard/API representations and are never advertised as host download choices.
- Dash, React, and Streamlit must dispatch the same effective export state and receive one server-owned row/byte limit policy.
- `PIVOT_EXPORT_MAX_ROWS` and `PIVOT_EXPORT_MAX_BYTES` retain their current defaults and enforcement owner.
- Browser qualification must fail, never skip, when explicitly run for release evidence.
- Browser logs, network traces, downloads, parsed-document summaries, screenshots, and an aggregate manifest must be retained for both normal and constrained phases.
- Do not log export row values, authentication tokens, or backend exception text in browser/network evidence.
- Do not touch the user-owned `artifacts/`, `dash_tanstack_pivot/dist/`, `investigation_2026_09_14/`, or `security/reports/` working directories.

## Review Focus

- Omitted/legacy `xls` formats retain an XLSX default/alias; unknown formats produce a stable rejection rather than silently becoming CSV — Task 1 tests this.
- A CSV value containing a comma, newline, quote, or non-ASCII character round-trips through UTF-8 BOM parsing without changing column position — Task 1 tests this.
- XLSX numeric cells remain numeric, including negative and decimal totals, rather than becoming formatted strings — Task 1 and Task 3 test this.
- A user-visible filter, non-default column order, and collapsed hierarchy are reflected identically in CSV/XLSX output — Task 3 tests this per host.
- A row or byte rejection creates no completed or partial download and leaves the workspace usable — Task 4 tests this per host.

---

## File structure

| File | Responsibility |
| --- | --- |
| `pivot_engine/runtime/export_service.py` | Normalize/export the four protocol formats without silent downgrade; preserve typed XLSX and byte/row enforcement. |
| `tests/export_qualification_fixture.py` | Literal deterministic rows, expected headers/order/totals, and CSV/XLSX parsing/assertion helpers shared by runtime and Selenium tests. |
| `tests/test_export_qualification_runtime.py` | Runtime/API qualification for all formats, Unicode/numeric/order/filter/totals, and size limits. |
| `src/lib/runtime/createRuntimeExportCommands.js` | Accept `exportPivot(format)` and issue matching CSV/XLSX requests or local fallback documents. |
| `src/lib/components/PivotAppBar.js` | Accessible CSV/XLSX download menu using stable test IDs. |
| `tests/frontend_export_qualification.cjs` | Behavioral command-boundary test for default XLSX and explicit CSV request construction. |
| `examples/flagship_demo_data.py` | Offer an environment-gated compact deterministic qualification data set to all three demo hosts. |
| `tests/test_flagship_certification.py` | Browser actions, file parsing, clipboard checks, and constrained-limit assertions for every host. |
| `scripts/certify_flagship.py` | Build/launch normal and constrained host phases, require non-skipped JUnit cases, emit an evidence manifest. |
| `.github/workflows/release.yml` | Install browser dependency, invoke export qualification before publish, and upload its artifacts even on failure. |
| `tests/flagship_capabilities.py`, `docs/flagship-functionality-certification.md`, `docs/OPERATIONS.md`, `README.md` | State the exact public format, clipboard, limit, and evidence contract. |

### Task 1: Runtime export contract and deterministic qualification fixture

**Files:**
- Create: `tests/export_qualification_fixture.py`
- Create: `tests/test_export_qualification_runtime.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_service.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py` only if explicit invalid-format errors need a stable router response
- Test: `tests/test_export_limits.py`

**Interfaces:**
- Produces: `QUALIFICATION_ROWS`, `qualification_state()`, `expected_export()`, `read_csv_export(path)`, and `read_xlsx_export(path)` from `tests.export_qualification_fixture`.
- Produces: `RuntimeExportService.normalize_export_format(value: object) -> str`, accepting `csv`, `tsv`, `html`, `xlsx`, and legacy `xls` as `xlsx`; omitted format remains `csv` for protocol compatibility.
- Consumes: existing `RuntimeExportService.build_export_payload(response, state, export_request)` and `ExportLimits` behavior.

- [ ] **Step 1: Write failing format-normalization and document-fidelity tests**

```python
@pytest.mark.parametrize("requested, expected", [(None, "csv"), ("xls", "xlsx"), ("TSV", "tsv")])
def test_runtime_export_normalizes_supported_format(requested, expected):
    assert RuntimeExportService.normalize_export_format(requested) == expected

def test_runtime_export_rejects_unknown_format_without_creating_a_file(tmp_path):
    with pytest.raises(ValueError, match="Unsupported export format"):
        RuntimeExportService().build_export_payload(response, state, {"format": "pdf"})

@pytest.mark.parametrize("export_format", ["csv", "tsv", "html", "xlsx"])
def test_runtime_exports_fixture_with_order_filter_totals_numeric_and_unicode(export_format):
    payload = service.build_export_payload(filtered_response, state, {"format": export_format, "colIds": EXPECTED_ORDER})
    assert_document_matches_expected(payload, export_format)
```

- [ ] **Step 2: Run the new test module to verify it fails**

Run: `python -m pytest tests/test_export_qualification_runtime.py -q`

Expected: FAIL because the fixture/helpers and format normalizer do not exist, and unsupported runtime formats currently downgrade to CSV.

- [ ] **Step 3: Implement literal fixture data and format normalization**

Implement `normalize_export_format()` in `RuntimeExportService` and route both materialized and streamed format selection through it. The fixture must use literal, hand-calculated expected values for filtered totals, ordered columns, numeric values, and Unicode text; do not use the exporter to calculate expected results. Parse XLSX only with ZIP/XML and CSV with `csv.reader(encoding="utf-8-sig")`, so qualification adds no spreadsheet dependency.

- [ ] **Step 4: Add row/byte boundary cases for all four runtime formats**

Add parameterized tests to `tests/test_export_limits.py` (or the new runtime module) that assert a configured row limit rejects before execution and an encoded byte limit deletes any temporary file for CSV, TSV, HTML, and XLSX. Assert the limit message identifies the relevant configured setting and no `contentPath` remains.

- [ ] **Step 5: Run runtime export verification**

Run: `python -m pytest tests/test_export_qualification_runtime.py tests/test_export_limits.py tests/test_runtime_service.py -q`

Expected: PASS; all four formats preserve their documented contract and reject unsafe requests consistently.

- [ ] **Step 6: Commit**

```bash
git add tests/export_qualification_fixture.py tests/test_export_qualification_runtime.py tests/test_export_limits.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/export_service.py dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py
git commit -m "feat: qualify runtime export formats"
```

### Task 2: Shared CSV/XLSX host download command

**Files:**
- Modify: `dash_tanstack_pivot/src/lib/runtime/createRuntimeExportCommands.js`
- Modify: `dash_tanstack_pivot/src/lib/utils/exportUtils.js`
- Modify: `dash_tanstack_pivot/src/lib/components/PivotAppBar.js`
- Modify: `dash_tanstack_pivot/src/lib/components/PivotWorkspaceControls.js` only if its presentation model requires the new callback signature
- Modify: `dash_tanstack_pivot/src/lib/runtime/useViewportRuntimeStage.tsx` to preserve the existing XLSX context-menu default and expose CSV only where appropriate
- Create: `tests/frontend_export_qualification.cjs`
- Modify: `tests/test_frontend_contract.py`

**Interfaces:**
- Consumes: `exportPivot(format?: "xlsx" | "csv")` supplied from `createRuntimeExportCommands`.
- Produces: toolbar actions `[data-testid="pivot-export-xlsx"]` and `[data-testid="pivot-export-csv"]`, both disabled while an export is active.
- Produces: server requests whose payload `format` is exactly the selected format; local fallback writes `pivot.xlsx` or `pivot.csv` with matching content type.

- [ ] **Step 1: Write a failing command test for default XLSX and selected CSV**

```javascript
commands.exportPivot();
assert.equal(dispatchedPayloads.at(-1).format, 'xlsx');
commands.exportPivot('csv');
assert.equal(dispatchedPayloads.at(-1).format, 'csv');
```

Also assert a non-server-side CSV action uses a CSV writer rather than loading the XLSX writer.

- [ ] **Step 2: Run the command/contract tests to verify they fail**

Run: `node tests/frontend_export_qualification.cjs; python -m pytest tests/test_frontend_contract.py -q`

Expected: FAIL because `exportPivot` accepts no format and the toolbar has no CSV action.

- [ ] **Step 3: Implement format-aware command dispatch and local fallback**

Change `exportPivot` to accept an optional format and validate it against `xlsx`/`csv`. Preserve zero-argument XLSX behavior. Thread the selected format into normal and part export requests; extend the client-side fallback with a CSV file writer that uses the same visible columns, header labels, raw values, and total handling as the XLSX fallback.

- [ ] **Step 4: Implement accessible toolbar actions**

Replace the duplicated export button markup with one reusable export control: default XLSX action plus an accessible menu/list containing `Export XLSX` and `Export CSV`. Use stable test IDs, keyboard-focusable buttons, explicit labels, and the existing `isExporting` disabled state. Retain the context-menu Excel action as `exportPivot()` so its behavior remains XLSX.

- [ ] **Step 5: Run frontend build and contract verification**

Run: `node tests/frontend_export_qualification.cjs; python -m pytest tests/test_frontend_contract.py -q; npm run build`

Working directory for build: `dash_tanstack_pivot`

Expected: PASS; built Dash, React, and Streamlit bundles contain both actions and existing XLSX callers retain their behavior.

- [ ] **Step 6: Commit**

```bash
git add dash_tanstack_pivot/src/lib/runtime/createRuntimeExportCommands.js dash_tanstack_pivot/src/lib/utils/exportUtils.js dash_tanstack_pivot/src/lib/components/PivotAppBar.js dash_tanstack_pivot/src/lib/components/PivotWorkspaceControls.js dash_tanstack_pivot/src/lib/runtime/useViewportRuntimeStage.tsx tests/frontend_export_qualification.cjs tests/test_frontend_contract.py
git commit -m "feat: add CSV export to shared host toolbar"
```

### Task 3: Deterministic cross-host download and clipboard qualification

**Files:**
- Modify: `examples/flagship_demo_data.py`
- Modify: `tests/test_flagship_certification.py`
- Modify: `tests/flagship_capabilities.py`
- Test: `tests/test_flagship_certification.py`

**Interfaces:**
- Consumes: `PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA=1` to select compact literal qualification data for all three examples.
- Consumes: fixture parsing/assertion helpers from `tests.export_qualification_fixture`.
- Produces: one browser case per `(dash|react|streamlit) × (csv|xlsx)` and per clipboard representation, with parsed evidence summaries under the configured artifact directory.

- [ ] **Step 1: Write failing browser-qualification assertions against the current host UI**

Add a flagged browser case that opens the qualification dataset, applies the documented filter, chooses a non-default visible column order, downloads CSV/XLSX, and calls the fixture assertions. Add a clipboard case that checks TSV text and HTML table headers, Unicode, current-table scope, and all-record scope. Add `export-csv` to the capability matrix.

- [ ] **Step 2: Run the browser module in qualification mode to verify the missing CSV control fails**

Run: `PIVOT_RUN_FLAGSHIP_BROWSER_TESTS=1 PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA=1 python -m pytest tests/test_flagship_certification.py -m flagship -q`

Expected: FAIL until Task 2 provides the CSV action and the fixture data is selected. If Chrome/WebDriver is not installed, record the environment gap; Task 4 makes that condition fatal in the release runner.

- [ ] **Step 3: Add the environment-gated qualification data set**

Extend `examples/flagship_demo_data.py` with a compact data mode selected only by `PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA=1`. Include exact literal hierarchy/filter values, accented and non-Latin text, integer/decimal/negative values, and hand-calculated total expectations exposed through test-only fixture data. The default 12,000-row flagship demo remains unchanged.

- [ ] **Step 4: Implement download and clipboard evidence assertions**

In `tests/test_flagship_certification.py`, use the existing host/shadow-root helpers and download waiting logic. For each host, assert the selected filter/order state produces parsed CSV/XLSX documents matching the fixture, including numeric XLSX cells and merged headers. Persist a sanitized parsed-document JSON summary with the existing screenshot/browser/network evidence. Check clipboard through browser permissions without writing clipboard values to logs.

- [ ] **Step 5: Run cross-host browser certification**

Run: `python scripts/certify_flagship.py --export-qualification --skip-build`

Expected: PASS only when all three running hosts produce qualified CSV/XLSX and clipboard evidence. Do not treat skipped tests as a pass.

- [ ] **Step 6: Commit**

```bash
git add examples/flagship_demo_data.py tests/test_flagship_certification.py tests/flagship_capabilities.py tests/export_qualification_fixture.py
git commit -m "test: qualify exports across flagship hosts"
```

### Task 4: Constrained-limit evidence and fail-closed certification runner

**Files:**
- Modify: `scripts/certify_flagship.py`
- Modify: `tests/test_flagship_certification.py`
- Modify: `.github/workflows/release.yml`
- Test: `tests/test_flagship_certification.py`
- Test: `tests/test_docs_examples_contract.py`

**Interfaces:**
- Consumes: `--export-qualification`, `--skip-build`, and optional `--reuse-running` runner flags.
- Produces: `<artifact-dir>/export-qualification.json`, JUnit XML per phase, per-host downloads/screenshots/logs/network traces, and sanitized parsed-document summaries.
- Produces: environment flags `PIVOT_RUN_EXPORT_QUALIFICATION=1`, `PIVOT_REQUIRE_BROWSER_EVIDENCE=1`, and `PIVOT_EXPORT_QUALIFICATION_PHASE=normal|constrained` for browser behavior.

- [ ] **Step 1: Write failing runner tests for evidence validation**

Create focused subprocess/unit tests that simulate a JUnit report with a skipped case, missing required host artifact, and a non-zero browser result. Each must make the certification runner return non-zero and write a failed manifest. Add a browser constrained-phase test that records the download directory before an oversized CSV/XLSX request, waits for the stable limit error, and proves no new completed/partial file exists.

- [ ] **Step 2: Run runner tests to verify they fail**

Run: `python -m pytest tests/test_flagship_certification.py tests/test_docs_examples_contract.py -q`

Expected: FAIL because the current runner accepts skipped browser tests and has no constrained export phase or export manifest.

- [ ] **Step 3: Implement normal/constrained host phases and evidence validation**

Refactor `certify_flagship.py` around a reusable host-launch function. In export-qualification mode, run normal data/limits first, then a fresh constrained process group with `PIVOT_EXPORT_MAX_ROWS=1` and a byte limit below a generated document. Invoke pytest with built-in `--junitxml`; parse it to reject skips/errors/failures and require all three host summaries/download evidence. Always write an aggregate JSON manifest with phase status and paths, but never include exported cell contents, credentials, tokens, or raw backend errors.

- [ ] **Step 4: Gate release publishing and preserve failure evidence**

Install Selenium in the release environment, add `python scripts/certify_flagship.py --export-qualification` after built-host validation and before artifact publication, and add an `if: always()` upload for the export qualification artifact directory. The release step must fail if Chrome/WebDriver is unavailable or evidence is incomplete.

- [ ] **Step 5: Run runner and workflow-contract verification**

Run: `python -m pytest tests/test_flagship_certification.py tests/test_docs_examples_contract.py -q; python scripts/certify_flagship.py --export-qualification --skip-build`

Expected: local browser-supported environments produce a normal and constrained manifest. Environments without a browser fail the certification command rather than reporting qualified output.

- [ ] **Step 6: Commit**

```bash
git add scripts/certify_flagship.py tests/test_flagship_certification.py .github/workflows/release.yml tests/test_docs_examples_contract.py
git commit -m "ci: gate release on export qualification evidence"
```

### Task 5: Publish the qualified export contract

**Files:**
- Modify: `README.md`
- Modify: `docs/flagship-functionality-certification.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `tests/test_docs_examples_contract.py`

**Interfaces:**
- Consumes: the final `scripts/certify_flagship.py --export-qualification` command and manifest schema from Task 4.
- Produces: user-facing documentation that distinguishes host downloads (CSV/XLSX), clipboard formats (TSV/HTML), runtime/API formats (CSV/TSV/HTML/XLSX), default limits, and the fail-closed release-evidence command.

- [ ] **Step 1: Write failing documentation contract tests**

```python
def test_export_operations_documentation_names_formats_limits_and_release_command():
    text = (ROOT / "docs" / "OPERATIONS.md").read_text(encoding="utf-8")
    assert "CSV and XLSX" in text
    assert "TSV and HTML" in text
    assert "scripts/certify_flagship.py --export-qualification" in text
```

- [ ] **Step 2: Run the documentation test to verify it fails**

Run: `python -m pytest tests/test_docs_examples_contract.py -q`

Expected: FAIL because the exact public export and evidence boundary is not documented.

- [ ] **Step 3: Document the contract and release evidence procedure**

Update README and flagship certification docs with the two host download options and clipboard/API distinction. Add an Operations section naming the default row/byte limits, how to run normal/constrained certification, required artifact contents, and the rule that deployment evidence remains separate from this browser qualification.

- [ ] **Step 4: Run documentation and relevant regression tests**

Run: `python -m pytest tests/test_docs_examples_contract.py tests/test_export_qualification_runtime.py tests/test_frontend_contract.py -q`

Expected: PASS; docs cannot drift from the public qualified format contract.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/flagship-functionality-certification.md docs/OPERATIONS.md tests/test_docs_examples_contract.py
git commit -m "docs: publish qualified export contract"
```

### Task 6: Final release-equivalent verification

**Files:**
- Modify only if the preceding checks reveal a scoped regression.

**Interfaces:**
- Consumes: built host assets, export evidence command, release workflow changes, and all preceding contracts.
- Produces: final test/build/evidence results suitable for handoff.

- [ ] **Step 1: Run format, frontend, host, and release workflow tests**

Run:

```bash
python -m pytest tests/test_export_qualification_runtime.py tests/test_export_limits.py tests/test_runtime_service.py tests/test_frontend_contract.py tests/test_flagship_certification.py tests/test_docs_examples_contract.py -q
cd dash_tanstack_pivot && npm run build
cd .. && python scripts/ci/verify_bundle_contract.py && python scripts/ci/verify_host_build_contract.py --package-dir dash_tanstack_pivot
```

Expected: PASS.

- [ ] **Step 2: Run built-host normal and constrained qualification**

Run: `python scripts/certify_flagship.py --export-qualification`

Expected: PASS with normal/constrained manifests and complete evidence for Dash, React, and Streamlit. Any skipped browser case, missing artifact, host mismatch, partial download, or limit-bypass fails the command.

- [ ] **Step 3: Run the full Python suite and security release gate**

Run:

```bash
python -m pytest -q
bandit -r dash_tanstack_pivot/pivot_engine/pivot_engine
```

Expected: all tests pass and Bandit reports no findings.

- [ ] **Step 4: Inspect release scope and commit any final scoped correction**

Run: `git diff --check; git status --short`

Expected: no unintended source changes; leave user-owned untracked artifact/report directories untouched.

## Self-review

- **Spec coverage:** Tasks 1–2 implement the exact public format boundary; Task 3 verifies normal host delivery/clipboard semantics; Task 4 verifies limits, fail-closed evidence, and release gating; Task 5 publishes the contract; Task 6 proves the integrated result.
- **Step scan:** Each task begins with a failing behavioral test, contains the exact files/interfaces it changes, and ends with a focused verification and commit.
- **Type consistency:** The frontend command is consistently named `exportPivot(format?: "xlsx" | "csv")`; test IDs are `pivot-export-xlsx` and `pivot-export-csv`; runner mode is `--export-qualification`.
- **Review focus:** Each of the five highest-risk format, encoding, numeric, state, and limit-rejection cases is assigned to Tasks 1, 3, or 4.
- **Proportion:** The plan specifies boundaries and observable contracts without prescribing implementation bodies beyond the decisions that affect compatibility and release evidence.

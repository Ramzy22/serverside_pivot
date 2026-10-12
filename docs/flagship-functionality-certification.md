# Flagship functionality certification

The read-only parity source of truth is the versioned workflow contract in
`tests/flagship_requirements.json`. Its 68 atomic requirements cover all 44
supported entries in `tests/flagship_capabilities.py`, including selection-copy
edge cases and four combined journeys. Editing has one explicitly excluded
entry. A supported inventory entry, wired control, or passing validator is not
proof that the workflow works in a released artifact.

The contract pins AG Grid Enterprise 36.2.0 with SSRM. Reference configurations
are expanded by `tests/flagship_contract.py`; each comparable requirement names
documentation, an unobserved reference behavior and a stable reference case ID.
All comparable parity classifications start as `unverified`. The three
host-specific integration entries are `not_comparable`, apply only to their
named host, and have an explicit AG Grid `not_applicable` reason.

Generate the matrix from the contract (without inventing observations) with:

```powershell
python -c "from tests.flagship_capabilities import markdown_matrix; print(markdown_matrix())"
```

`load_requirements(path)` parses the versioned contract;
`validate_requirements(requirements, capabilities)` rejects invalid IDs, missing
acceptance criteria, reference configuration, constraints, verification mapping,
host applicability and incomplete inventory coverage. `render_matrix` accepts
independent `CaseResult` observations. A missing mapped browser case remains
`unverified`; failures, blocks and skips remain visible. It never publishes
private observed values or raw evidence paths. The generated matrix is not
maintained as a second source of truth in this document.

Case IDs in the seed contract are planned verification mappings, not assertions
that those tests have already run or been implemented. Reference results and
product-host results are separate. Passing a product case alone cannot change a
parity classification to `equivalent`. Current release qualification additionally
needs installed-artifact/hash binding, versions and evidence for every mandatory
case in each applicable host. The historical failed certification and older
successful run are not current requirement-level evidence.

Run the complete built-artifact gate with:

```powershell
python scripts/certify_flagship.py
```

It builds the three frontend artifacts, launches the local V1 runtime and all
three hosts, runs the Selenium certification suite, and writes screenshots,
browser logs, network evidence, downloads, JSON, and Markdown to
`artifacts/flagship-certification/`.

The existing runner covers startup/control smoke paths, clipboard delivery,
downloads, sidebar/search, report switching and chart entrypoints. Those broad
checks do not provide the seed contract's full atomic workflow evidence. Until
case execution and current artifact binding are complete, the contract matrix
continues to report missing observations as `unverified`.

## Export qualification contract

The release promise for host downloads is **CSV and XLSX** across Dash, React,
and Streamlit. Existing zero-argument export actions remain XLSX. Clipboard
actions are **TSV and HTML**, and are separate from the runtime/API formats,
which support CSV, TSV, HTML, and XLSX (including the legacy `xls` alias for
XLSX). TSV and HTML are not advertised as host file-download choices.

The qualification runner is the release gate:

```powershell
python scripts/certify_flagship.py --export-qualification
```

It runs normal and constrained phases against fresh host processes. Normal
cases parse CSV/XLSX and inspect clipboard TSV/HTML for headers, filters,
sorting, hierarchy scope, visible column order, Unicode, and numeric values.
Constrained cases set the server-owned `PIVOT_EXPORT_MAX_ROWS` and
`PIVOT_EXPORT_MAX_BYTES` budgets and require a stable limit rejection with no
completed or partial file. A missing browser, skipped test, incomplete host
evidence, stale phase, or limit bypass fails the command. The sanitized
`export-qualification.json` manifest and phase evidence are suitable for
release review; they do not contain exported cell values or credentials.

Cell and row editing are intentionally unavailable in the public flagship
promise until they are restored as a supported feature and receive browser
coverage.

## Clipboard modes

The table context menu has two primary clipboard commands:

- **Copy Current Table** copies the table as it is now: active filters and
  sorting, expanded versus collapsed branches, current layout, and the
  currently visible column order.
- **Copy All Records** requests the complete matching hierarchy, including
  branches currently collapsed in the grid.

Both primary commands include headers. The **Copy Options** submenu keeps the
no-header and merged-header variants for either scope.

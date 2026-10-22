# Flagship functionality certification

The read-only parity source of truth is the versioned workflow contract in
`tests/flagship_requirements.json`. Its 71 atomic requirements cover all 44
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

Runtime-error recovery is scoped to a visible Retry after a terminal error:
the clicked action must produce a distinct successful request, clear the error,
and restore the fixture totals. A terminal HTTP error cannot subsequently
return data. Ignoring a delayed older response after a newer success is a
separate, required `runtime-error.superseded-response` workflow. Its browser
case holds a genuine row-bearing response for an Éclair search, lets a newer
München response render, releases the old response, and checks both the V1
completion order and every visible fixture cell in each installed host. The
observation starts when the old response is released and samples for 1.5 seconds;
it cannot prove the absence of changes outside that window. A metadata-only
response cannot satisfy the case. The AG Grid reference has the analogous
controlled SSRM case. Passing either case in an unbound diagnostic run does not
pass current-artifact or overall release qualification.

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

## Cancellable export qualification

Build exact committed wheel/npm artifacts and run the three job journeys in
each installed host with:

```powershell
python scripts/certify_flagship.py --installed-release --cancellable-exports --artifact-dir artifacts/cancellable-exports
```

This uses a separate deterministic 512-row flat/table fixture and never changes
the complex parity fixture. Required browser cases correlate V1 start/status/
cancel requests and server job IDs, verify monotonic observed counters and an
indeterminate indicator for unknown totals, parse CSV/XLSX headers, Unicode and
numeric cells, and verify no partial download after cancellation. Cancel tests
sample a real native DuckDB statement and export admission through cleanup,
then start smaller filtered work. Server samples and raw traces remain private.
The artifact binding records source commit, wheel/npm/fixture and loaded asset
SHA-256 values plus browser version and per-case evidence hashes.

The 512-row fixture includes Japanese text. Its grouped, sorted DuckDB query
reproduces an upstream Unicode error with DuckDB 1.5.0 but passes the focused
job test with 1.5.6; the installed-release manifest records the exact DuckDB
version used. Passing evidence for one version must not be projected onto
another supported version.

Bounded large jobs support flat/table DuckDB-Ibis layouts. Complex pivot columns,
subtotals, formula/window measures and unsafe providers reject up front. Redis
is optional for one worker; multi-worker cancellation and interrupted retry
require distributed Redis. Restart means a fresh retry, never resumption. S3
and resumable artifacts remain optional future work.

Nine passing export cases are only export evidence. Missing shared cases,
other required workflows, unavailable infrastructure, skipped cases or missing
live reference evidence still fail the complete release/parity manifest; this
command cannot promote an incomplete matrix to a qualified release.

## Clipboard modes

The table context menu has two primary clipboard commands:

- **Copy Current Table** copies the table as it is now: active filters and
  sorting, expanded versus collapsed branches, current layout, and the
  currently visible column order.
- **Copy All Records** requests the complete matching hierarchy, including
  branches currently collapsed in the grid.

Both primary commands include headers. The **Copy Options** submenu keeps the
no-header and merged-header variants for either scope.

Clipboard plain text now uses quoted TSV for values or headers containing a
tab, line break, or quote; quotes inside a field are doubled. Ordinary fields
remain unchanged. The shared serializer retains source LF and CRLF bytes, and
the server HTML-to-TSV copy path protects carriage returns before browser HTML
parsing. Installed Dash, React, and Streamlit tests verify that tabs and line
breaks stay within one logical copied cell for selection and that Current Table
and All Records retain table geometry. Windows Chromium itself converts LF to
CRLF in `text/plain` and CRLF to LF in `text/html`, so byte-exact line-ending
identity is not promised after the OS clipboard boundary. The pinned AG Grid
36.2 SSRM reference fixture's plain-text copy left these delimiters unescaped
and failed its TSV round-trip oracle; this is a narrow observed advantage, not
an overall AG Grid comparison or a release-parity pass.

For `copy-selection.display-order`, the promise is that copy follows the actual
displayed rows and columns after descending Region sort and moving units before
sales. Sorting under AG Grid 36.2 SSRM belongs to its server: the reference
fixture uses `Intl.Collator('en-GB')` and displays München then Éclair, whereas
the product's backend-native sort displays Éclair then München. This deliberate
server-specific collation observation does not imply either order is wrong or
that AG Grid mandates English collation. Both comparators remain unchanged.

The product browser case independently asserts Éclair then München and the
literal 2x2 TSV body `2.00\t100.25` then `3.00\t49.75`, with units before sales.
It retains physical sort/reorder actions, sort indicator and network evidence,
and a real clipboard byte check. The reference case retains its München-first
en-GB oracle. The installed run at commit `50dbe0d` failed the three product
cases against the former München-first expectation; those failures are RED
evidence for this contract correction. They do not establish current workflow
success or an overall parity pass; the contract remains `unverified` pending
current reference and installed-host qualification.

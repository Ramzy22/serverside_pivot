# Flagship functionality certification

The Dash, React, and Streamlit showcases are release claims. The generated
matrix makes every user-visible workflow explicit, including the one feature
that is intentionally unavailable. A control is never considered working
solely because a lower-level contract test passes.

Run the complete built-artifact gate with:

```powershell
python scripts/certify_flagship.py
```

It builds the three frontend artifacts, launches the local V1 runtime and all
three hosts, runs the Selenium certification suite, and writes screenshots,
browser logs, network evidence, downloads, JSON, and Markdown to
`artifacts/flagship-certification/`.

The source-of-truth matrix is `tests/flagship_capabilities.py`. It includes
startup, navigation, model composition, filtering, table controls, selection,
delivery, presentation, saved work, charts, and host-specific behavior. The
browser gate exercises the cross-host regression paths with the greatest
failure risk: populated startup and controls, scrolling/control interaction,
clipboard delivery, real XLSX download, sidebar/search, report switching, and
chart-pane entry/exit. The rest of the matrix is backed by its named runtime,
component, and contract suites; additional one-workflow-per-case browser
scenarios can be added without changing the public matrix.

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

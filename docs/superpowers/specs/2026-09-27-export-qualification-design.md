# Export Qualification Design

## Goal

Make export a qualified release capability rather than a collection of
lower-level tests. The release must prove that every published host delivers
the same filtered pivot as an XLSX or CSV file, preserves values and Unicode,
and rejects configured size limits without creating a partial download.

The published hosts are Dash, React, and Streamlit. They must share the same
result semantics even though Dash uses callbacks and React/Streamlit use the
host-neutral runtime transport.

## Public format contract

The host toolbar will expose exactly two download choices:

| Surface | Formats | Reason |
| --- | --- | --- |
| Dash, React, Streamlit download menu | XLSX, CSV | XLSX preserves rendered pivot structure; CSV is portable analysis data. |
| Clipboard commands | TSV text, HTML table | These are paste representations, not download products. |
| Runtime/API | CSV, TSV, HTML, XLSX | Integration clients retain the existing complete format set. |

The existing `Export XLSX` control becomes an accessible export menu with
separate XLSX and CSV actions. The existing default remains XLSX so saved
automation and direct command calls retain their current behavior.

No host will advertise TSV or HTML as a downloadable file. This avoids a
misleading UI while retaining their API and clipboard contracts.

## Shared export semantics

Each host issues the same normalized export request:

- `format` is `xlsx` or `csv` from the chosen menu action.
- The request derives from the current effective pivot state, including
  active filters, sorting, column order, layout, and expansion state.
- The download response carries a matching format, content type, filename,
  row count, and payload reference.
- Current-table export contains only the rendered hierarchy; all-record export
  explicitly requests all matching branches.
- `PIVOT_EXPORT_MAX_ROWS` and `PIVOT_EXPORT_MAX_BYTES` remain service-owned
  limits. A rejected request creates no browser download and presents the
  stable, non-sensitive limit error.

The server remains the source of XLSX formatting, merged headers, numeric
cell types, totals, and size enforcement. Hosts only select the supported
download format and trigger the authenticated payload reference.

## Qualification fixture and assertions

A deterministic export fixture will include:

- a two-level hierarchy, a filterable dimension, a non-default column order,
  expanded and collapsed branches, subtotals and a grand total;
- numeric values that exercise integer, decimal, negative, and large-value
  formatting without converting numeric XLSX cells to text;
- Unicode text in both headers and cells, including accented and non-Latin
  names; and
- enough rows and content to test both normal success and configured row/byte
  rejection paths.

The fixture is used by runtime contract tests and the three flagship browser
hosts, so expected headers, filtered row membership, totals, and values are
derived from one literal data set rather than from host-specific assertions.

For CSV, qualification parses the UTF-8 BOM file with the standard CSV reader
and asserts ordered headers, literal Unicode cells, filtered records, and
numeric text values. For XLSX, qualification opens the workbook ZIP/XML and
asserts merged pivot headers, shared Unicode strings, numeric cell types and
values, header order, and totals. Tests must compare the downloaded document
with the known fixture output, not merely check that a file exists.

For clipboard, TSV and HTML remain separately qualified as copy
representations: headers, Unicode values, and current-table/all-record scope
must remain intact. They are not counted as downloadable host formats.

## Release gate and evidence

Add an opt-in browser suite dedicated to export qualification. It runs two
isolated phases against freshly built Dash, React, and Streamlit artifacts:

1. **Normal limits**: perform filtered CSV and XLSX downloads on each host,
   parse them, and assert ordering, totals, numeric types/values, and Unicode
   fidelity. Validate TSV/HTML clipboard output through the same browser.
2. **Constrained limits**: launch all hosts with deliberately small row and
   byte ceilings. Attempt each host download, assert the stable limit error,
   assert no new file appears in the download directory, and retain the
   browser/network evidence.

The certification command writes a machine-readable manifest alongside
per-host downloads, parsed-document summaries, screenshots, browser logs, and
network traces. It fails if Selenium/Chrome is unavailable, a browser case is
skipped, a required artifact is absent, or any host diverges. A skipped browser
test is not release evidence.

The release workflow installs the browser dependency, runs this command after
building all host bundles, and uploads the evidence even if the gate fails.
Publishing occurs only after the command passes.

## Components

| Component | Responsibility |
| --- | --- |
| Frontend export command/presentation | Expose XLSX and CSV actions; preserve default XLSX behavior and stable download lifecycle. |
| Runtime export service/router | Normalize formats, enforce one set of limits, produce correct typed/file output. |
| Qualification fixture helpers | Supply literal data and independent expected totals, order, numeric values, and Unicode values. |
| Browser qualification suite | Drive all three built hosts and parse actual clipboard/download artifacts. |
| Certification runner and release workflow | Build, launch normal/constrained phases, reject skipped evidence, preserve artifacts, and gate publish. |
| Documentation/capability matrix | State the exact download, clipboard, API-format, and evidence promises. |

## Non-goals

- Adding edit features or changing authorization behavior.
- Advertising HTML or TSV as host download formats.
- Claiming that browser evidence substitutes for deployment-specific operator
  validation such as TLS, authentication, or production network controls.
- Raising the default export limits. The qualification suite tests configured
  boundaries without changing production defaults.

## Acceptance criteria

1. Every published host visibly exposes CSV and XLSX download actions and
   defaults to XLSX where an existing default is used.
2. Browser tests prove both downloaded formats preserve the known filtered
   pivot's headers/order, row membership, totals, numeric fidelity, and
   Unicode values for Dash, React, and Streamlit.
3. Browser tests prove TSV/HTML clipboard fidelity for current-table and
   all-record scopes on every host.
4. Runtime/API tests cover CSV, TSV, HTML, and XLSX format contracts and
   row/byte limit behavior.
5. A low-limit browser phase produces stable rejection behavior and no partial
   file on every host.
6. The certification command fails closed on unavailable/skipped browser
   coverage and writes auditable success/failure evidence.
7. The release workflow runs the export certification command before publish
   and uploads its evidence.

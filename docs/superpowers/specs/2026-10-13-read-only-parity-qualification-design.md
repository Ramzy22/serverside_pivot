# Read-only parity and release qualification design

Date: 2026-10-13

## Intent and scope

The deliverable is an auditable answer to two separate questions: how each
published read-only customer task compares with a fixed AG Grid reference, and
whether that task works in the exact Dash, React, and Streamlit artifacts being
considered for release. A wired control is not proof of behavior. The result
must identify unsupported behavior, constraints, missing evidence, and failures
without promoting any of them to a pass.

The existing 45-entry flagship inventory is the starting inventory, not a
certification result. Its editing entry remains explicitly unavailable and is
not a release requirement. Host-only entries are required only in their named
host. The scope is read-only behavior, including layout, selection, copy,
export, presentation, charts, persistence, errors, cancellation, and host
integration; it does not introduce editing or new customer-facing features.

## Comparison baseline

Pin the reference to AG Grid Enterprise 36.2.0, JavaScript, Server-Side Row
Model (SSRM), server-side grouping, aggregation, filtering, sorting, and
pivoting, with cell selection, clipboard, Excel export, charts, and grid state
enabled where a workflow uses them. Record every option and module in a
versioned reference fixture. If a behavior exists only under another AG Grid
row model, name that model and compare it as a separate reference; never imply
that a Client-Side Row Model demonstration proves SSRM behavior. Compare the
same deterministic dataset, locale, browser, and intended user action.

AG Grid permits local Enterprise testing without a licence key. The local
reference may display its expected watermark and licence console warning;
those conditions are recorded and excluded only from reference-app error
assertions. The harness is for local evaluation, not publication or production
deployment. Do not store a trial key or distribute an AG Grid Enterprise bundle
as part of this product.

Primary reference documentation:

- Release: https://github.com/ag-grid/ag-grid/releases/tag/v36.2.0
- SSRM datasource: https://www.ag-grid.com/javascript-data-grid/server-side-model-datasource/
- SSRM pivoting: https://www.ag-grid.com/javascript-data-grid/server-side-model-pivoting/
- Cell selection: https://www.ag-grid.com/javascript-data-grid/cell-selection/
- Clipboard: https://www.ag-grid.com/javascript-data-grid/clipboard/
- Grid state: https://www.ag-grid.com/javascript-data-grid/grid-state/
- Excel export: https://www.ag-grid.com/javascript-data-grid/excel-export/
- Local Enterprise evaluation: https://www.ag-grid.com/javascript-data-grid/license-install/

## Options considered

1. **Versioned contract plus live reference and product-host qualification
   (chosen).** This distinguishes competitor observations from our release
   evidence and makes unsupported cases explicit. It costs more browser time
   but answers the customer's parity question.
2. Documentation-only AG Grid comparison. Faster, but exact edge behavior and
   configuration-dependent differences would remain unverified.
3. One monolithic end-to-end suite for all reference and host checks. It would
   be difficult to diagnose and would encourage an aggregate pass to hide
   missing workflow coverage.

## Contract and result model

A versioned, machine-readable workflow contract is the source of truth. Each
requirement has a stable ID and contains:

- Customer task and observable expected behavior, including exact data/order,
  visual state, interaction, and output rules where applicable.
- AG Grid version, edition, row model, configuration, reference behavior,
  documentation link, and live reference-test ID/result.
- Product-supported behavior and a difference classification: `equivalent`,
  `deliberately_different`, `narrower`, `unsupported`, or `unverified`.
- Constraints, prerequisites, unsupported combinations, applicable hosts, and
  whether the requirement is mandatory for release.
- Shared-test ID, host browser-test IDs, evidence paths, observed result,
  browser/host versions, and release binding.

The human-readable matrix is generated from this contract and result data; it
must not be maintained as a second authority. Each broad inventory row maps to
one or more atomic requirements. The contract validator rejects missing IDs,
duplicate IDs, unknown hosts, missing expected behavior, missing reference
configuration, missing constraints, missing verification mapping, and any
supported inventory workflow with no requirements. Editing maps only to an
excluded entry. A host-only workflow does not acquire fictitious results in
the other two hosts.

For selected-range copy, separate requirements cover the exact logical cells,
display order, pinned columns without duplication, hidden-column policy,
formatted-versus-raw values, tabs/newlines/Unicode/empty values, optional
headers, selection beyond loaded rows, and preserving native input/dialog copy.
“Copy Current Table” and “Copy All Records” are different tasks and remain
separate from selection copy. If AG Grid lacks the same product-specific
operation under SSRM, classify it as a difference rather than inventing
equivalence.

Parity is a claim about an atomic requirement, not a whole feature label.
`Equivalent` requires matching tested observable behavior under the recorded
configurations. A documentation statement alone can support a reference
description but cannot upgrade an unobserved edge case to verified equivalence.

## Test architecture and artifact flow

The reference harness is an isolated local app and SSRM datasource over the
same deterministic fixture used by product qualification. It records AG Grid
package version, relevant grid options, datasource request/response traces,
browser version, screenshot, and parsed clipboard or download output for
reference cases. Reference observations are not substituted for product tests.

Product qualification has three layers:

1. Shared unit/runtime tests verify calculations, serialization, state
   transitions, request semantics, exact export data, and cancellation cleanup.
2. Browser workflows run against installed release artifacts in Dash, React,
   and Streamlit. Every step starts with a known fixture/UI state, performs a
   real interaction, asserts the visible result, captures relevant backend
   traffic, and checks reload/cancel/retry behavior when the requirement calls
   for it.
3. Host-specific tests cover Dash callback transport, React mount/unmount and
   direct transport, and Streamlit rerun/iframe sizing and permissions.

Build each host from a clean source checkout. Install the Python wheel in an
isolated environment and the React package tarball in a separate example app;
serve the React *production* build, not Vite's development server or a source
alias. Streamlit must load its frontend from the installed wheel. Record SHA-256
of the source commit, wheel, npm tarball, built entrypoints/chunks and fixture,
plus exact Python, Node, browser, driver, Dash, React, and Streamlit versions.
Reject a run whose loaded asset hash differs from the manifest's hash.

Capture a result per atomic requirement and applicable host. Allowed results
are `passed`, `failed`, `skipped`, `blocked`, and `not_applicable`. A required
`skipped`, `blocked`, missing, stale, or hash-mismatched result fails the
qualification. `not_applicable` is valid only for a declared host exclusion.
Reference cases have independent results; absence of required live reference
evidence prevents a “verified parity” claim even if our host tests pass. The
product release gate and parity-status gate are distinct, with both reported.

Store raw screenshots, browser/network traces, parsed clipboard/download
outputs, and failure diagnostics in a private evidence tree. Publish only an
allowlisted sanitized manifest and safe evidence summaries. Existing export
qualification redaction rules remain in force; no raw customer data, tokens,
exception text, or browser secrets enter a public release artifact.

## Required journeys and priority

Start with the weak or partial rows: runtime error display/recovery;
selection/keyboard copy; resize/auto-size/pinning; large-delivery cancellation;
and saved-view restoration. Then cover axis swap and layout undo/redo,
formatting/themes, and chart source/pane lifecycle. Continue until *every*
supported inventory workflow has atomic requirements and passing evidence in
every applicable host.

Representative combination journeys are mandatory, not substitutes for atomic
checks:

- **Selection:** filter, pin, horizontally scroll, select a range, copy with
  headers, and compare the exact clipboard representation.
- **Analysis:** change aggregation, expand hierarchy, swap axes, export, and
  compare values and totals with an independent fixture oracle.
- **Persistence:** resize columns, change density, save, reload, restore, and
  verify model state and visible geometry.
- **Recovery:** start expensive work, cancel, start another operation, and
  verify UI state, actual execution cleanup, retained admission accounting,
  absence of stale results, and a successful retry.

Geometry checks use stable measurements and tolerances stated in the contract,
not screenshot-only assertions. Requests must be correlated to the action and
response, not merely counted. Clipboard and download checks parse the actual
browser-observed output; they do not infer success from a notification.

## Existing evidence and failure handling

The historical `artifacts/flagship-certification/flagship-certification.json`
reports failure, while an older `release-run` reports success. Neither has
complete requirement-level results or current artifact binding. Investigate
the older failure using its retained traces/logs and document the finding or
the exact evidence gap. Do not relabel it as passed, and do not use the older
successful run as current evidence. Rerun qualification on the current built
artifacts after the contract and runner are implemented.

Failures retain a stable requirement ID, host, action, expected/observed
summary, sanitized diagnostics, screenshot, trace, and parsed output path.
Infrastructure errors and absent dependencies are `blocked`, not success.
Transient failures may be retried with a fresh browser and fixture, but both
attempts remain in the manifest. A final pass requires the successful rerun
to be bound to the same release artifacts.

## Completion criteria

- Every supported read-only inventory workflow maps to atomic requirements;
  editing is explicitly excluded.
- Each requirement names a fixed AG Grid reference configuration and its
  observed/documented basis, our precise promise, difference, and constraints.
- Every mandatory requirement passes in each applicable installed product
  host, including combination and host-specific checks. No skipped or missing
  mandatory case counts as pass.
- The live reference harness executes its required observable cases locally;
  undocumented or unobserved cases remain `unverified`, never `equivalent`.
- The current-release manifest binds source and loaded artifacts by hash and
  includes versions, per-case results, limitations, failures, and safe evidence
  paths. The older failed run is accounted for separately.

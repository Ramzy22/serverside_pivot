# Pivot column layout contract and verification

The runtime uses server-resolved pivot order for sparse viewport delivery,
clipboard documents and exports. Widths and pinning remain separate browser
preferences. Editing is outside this release contract.

| Requirement | Implemented behavior | Executable checks |
| --- | --- | --- |
| Stable identities | A typed pivot path and measure produce a SHA-256 layout ID. Labels and viewport positions do not participate. Unique source measures retain their slot when their aggregation changes; duplicate source measures remain distinct. | `test_identity_uses_typed_path_and_measure_instead_of_label_or_cell_index`, `test_custom_measure_order_survives_aggregation_change`, `test_duplicate_source_measures_keep_distinct_layout_identities` |
| Reconciliation | Surviving groups and leaves retain saved relative order within the grouping rules. New leaves append within their group and new groups append in server default order. Missing identities stay in the saved manifest without taking a visible slot and regain their saved rank when they return. | `test_saved_group_and_leaf_order_reconciles_missing_and_new_columns` |
| Revision binding | The resolved catalog, normalized manifest and structural query determine the revision. Schema and cells carry it together. Frontend request generations and manifest freshness reject superseded delivery; schema merging does not reuse slots from another revision. Viewport cache keys include layout and pinning. | `test_request_factory_and_all_viewport_cache_keys_bind_revision_and_pinning`, `frontend_pivot_layout_contract.cjs`, `test_v1_data_envelope_binds_cells_and_schema_to_layout_revision` |
| Shared order | The viewport resolver also supplies full-export order. Clipboard and export apply their own visibility and scope choices to that resolved order. | `test_saved_layout_orders_sparse_window_and_fetches_off_window_pin`, installed `browser.custom-pivot-order.sparse-window` journey |
| Grouping | Users move pivot groups and reorder measure leaves within a group. Cross-group leaf permutations normalize into contiguous groups. Row totals remain after ordinary pivot groups. | `test_interleaved_leaf_moves_are_normalized_into_contiguous_groups_and_totals_last` |
| Server validation | Only the current authorized catalog can resolve to output columns. Duplicates normalize to their first occurrence; unknown or unavailable IDs produce no output column. The manifest is bounded to 20,000 IDs and 512 KiB; pins are bounded to 64 IDs. | `test_duplicate_unknown_and_unauthorized_ids_are_ignored_without_schema_disclosure`, `test_manifest_count_and_bytes_are_bounded`, `test_v1_context_rejects_invalid_or_unbounded_layout_metadata` |
| Pins outside the window | Requests send pinned stable IDs separately from the scrolling range. The server unions both sets without duplicate cells. | `test_flagship_custom_order_sparse_window_and_off_window_pin`, `test_saved_layout_orders_sparse_window_and_fetches_off_window_pin` |
| Compact scroll requests | The manifest is sent after order or structural scope changes, or an unknown-revision response. Ordinary acknowledged requests carry the revision and pins. Worker-local revision caches are optional accelerators; another worker can request the manifest again. | `test_unchanged_scope_uses_compact_revision`, `test_filter_scope_change_resends_manifest_without_reusing_revision`, `test_revision_store_requires_same_server_scope_and_supports_worker_miss_retry` |
| Saved views | Order IDs, column widths and pinning are persisted separately and restored together. | installed `browser.custom-pivot-order.sparse-window` journey |

## Observed installed candidate

Candidate `6f5eada9bff2a7bcd99ac405c900799d9ddccf4b`, run
`fde623775e194e78953ced322d1e41af`, passed the custom-order browser journey in
Dash, React and Streamlit using Chrome `156.0.8078.76`. That journey checked a
reordered wide sparse grid, an off-window pin, exact clipboard headers and
values, XLSX ordering and merged headers, and saved width/order/pin restoration.
Each observation includes loaded-asset hashes. The binding and observations
are under `artifacts/qualification/candidate-6f5eada-full-2026-10-31/`.

The strict manifest passes the complete custom-order, subtotal and aggregation
requirement groups for both release behavior and reference comparison. All 19
mapped installed shared-assertion groups, nine scrolling tests, nine export
tests and 34 pinned AG Grid reference tests passed.

The main browser phase had 71 passes and four failures. One React startup
failure displayed a grid load error and remains unresolved; it did not recur
in 20 reloads and ten fresh-browser diagnostics. Three add/remove tests failed
because their interactions assumed all columns were visible and counted a
grouped total header twice; Streamlit also needed an explicit scroll before
clicking a control. Corrected interactions passed separately in all three
installed hosts. Those supplemental diagnostics never amend frozen case
records and require a new frozen run for release credit.

The complete manifest remains failed: 16 of 72 requirement groups passed
release qualification, 13 passed reference comparison, and one was not
applicable. The runner inventory still has 38 workflow groups without browser
mappings and 52 required shared-test IDs without mappings. These counts describe
missing executable qualification, rather than proving those features broken.

## Follow-up revision regression

A same-manifest regression reproduced a revision collision when changing the
secondary measure's aggregation: its saved layout ID stayed stable, but its
cell key changed without a new revision. The resolver fingerprint now includes
both the saved layout ID and the current cell key. Pure resolver and real
runtime regressions cover this change, and the installed aggregation mapping
requires the secondary-measure test. Candidate `6f5eada` ran both installed
aggregation assertions successfully. The saved identity remains unchanged and
the changed cell key now produces a new revision.

This report verifies the layout contract and records current evidence. It does
not qualify the complete release, deployment topology or operational controls.

## Startup and error freshness follow-up

The original runtime log contains HTTP 503 responses around startup. Its failed
browser observation did not retain the response body. Forty supplemental fresh
starts (normal, added network latency, CPU slowdown and resize during loading)
passed; they do not clear that historical failure.

A separate regression reproduced a concrete cross-layer defect: an actual HTTP
admission rejection omitted client, epoch and window metadata. The shared React
lifecycle then attached window 7's busy error to window 8. HTTP rejection
responses now preserve those fields. The direct transport also fills missing
optional identity from its originating request, preserving explicit server
identity for mismatch rejection. Current busy rejections retain the existing
Retry behavior and server resource limits.

Broader checks also reproduced acceptance of an old layout revision after an
order change. The commit guard now rejects it while allowing an unchanged-order
manifest retry on another worker.

The production browser regression compared the old packed React client with the
corrected client. Both received real filtered data followed by a deliberately
late, metadata-free V1 busy response simulating an older peer. The old client
showed an error after the newer result; the corrected client kept the new cells
and showed no error. Asset digests, traces and screenshots are retained under
`artifacts/startup-fix-browser-2026-10-31/`. These are supplemental diagnostics,
using a source backend and packed clients, not a clean installed-release run.

The installed shared recovery mapping now requires a real exhausted-governor
HTTP rejection, preserved freshness identity, release of capacity and an exact
successful retry. The historical startup observation and full release status
still require a fresh installed candidate with the updated assertions.

## Frozen candidate `96973e4`

Run `71e1031b940b4a5bb12855db8269b125` built and installed candidate
`96973e480d5ca94ec4d3446047b3ef59068ab441` outside the source checkout.
Its wheel SHA-256 is
`91af46106d04dd85a7ae73affb82b1219e1efc6c931bfa2e92021c00382c9871`;
its npm tarball SHA-256 is
`7bf2406406215227df93e4cc4e95cd666616cbed34ec841cd99f5cc06dac9bf0`.
The committed contract hash is
`df277b525050b11e59ac3597295c6cc432b343daee1b1a1fc9ffb7b049e05750`.
The binding and observation-level loaded asset digests are retained under
`artifacts/qualification/candidate-96973e4-full-2026-10-31/`.

All 75 ordinary workflow cases, nine scrolling/sparse-order cases, nine export
job cases, 19 installed shared-assertion groups and 34 pinned AG Grid reference
cases passed. No browser case failed or skipped. This includes the corrected
measure add/remove interactions and stale data/error response checks across
Dash, React and Streamlit. These observations qualify the covered behaviors;
they do not establish the cause of the older startup failure.

The strict manifest accepted the observed case bindings but rejected complete
release qualification for missing cases. It reports 17 passed release groups,
54 failed/incomplete groups and one not applicable; reference comparison reports
14 passed, 57 failed/incomplete and one not applicable. There are no invalid,
unsuccessful, duplicate or unexpected product browser records in its coverage
report. The candidate has 99 of 207 required browser mappings, leaving 108 host
cases across 38 workflows unmapped; 52 shared assertion IDs also remain unmapped.
The complete release remains on hold.

## Exact download qualification additions

The next runner revision adds six executable cases for
`export-csv.exact-document` and `export-xlsx.exact-workbook`: both formats in all
three hosts. Each downloads the actual filtered hierarchy, compares all five
rows and 45 measure cells with literal expected values, verifies column/header
order and Unicode, and records the downloaded file hash and parsed document.
XLSX also requires numeric cell types, the exact two-row header matrix and all
six hierarchy, pivot and total header merge spans. The observed export request
must retain the scope filter and request all rows. Unique request/response IDs,
job completion, payload URL/status and payload/blob/file hashes must correlate;
duplicate or partial downloads fail the case.

All six passed against separately installed candidate `96973e4` hosts under
`artifacts/exact-downloads-reviewed-2026-10-31/`. They are supplemental diagnostics
with `qualificationEvidence: false`, using newer test code than the frozen
candidate; its manifest and case envelope remain unchanged. An initial run's
three CSV failures remain retained: the oracle incorrectly expected the Sum
suffix on combined period/measure CSV headers. Source inspection confirmed
that CSV deliberately flattens those header stacks without that suffix, while
total headers retain it. The corrected oracle checks the exact flattened
headers and rejects swapped periods; the XLSX expectation remains unchanged.

Independent review found permissive workbook header assertions and incomplete
request-to-file correlation in the initial test implementation. Negative
controls reproduced both gaps before fixes; the final six live cases passed
with the stricter header, correlation and duplicate-download assertions. The
earlier diagnostic runs remain separate and do not prove those later checks.

The workflow, oracle, manifest and runner unit suite passed 194 tests. The new
runner has 105 of 207 required host cases mapped, leaving 36 browser workflow
groups and the same 52 shared assertion IDs unmapped. These new browser cases
require a future frozen run for release credit.

## Shared scrolling and cached revisions

Two previously unmapped shared scrolling requirements now run HTTP assertions
against the committed 120-group, 24-period fixture. The vertical case checks
rows 100 through 119, unique ordered identities, exact measure values and
restoration. The horizontal case checks sparse P23, P00 and total columns,
their server indices, absence of unrequested measure cells and exact totals.
Both also require a full-block/subwindow regression.

The stronger assertions exposed a production defect: cached response cloning
and row-block reassembly dropped `layout_revision`, even when the cached schema
still carried its revision. Both cache paths now preserve the original revision
without rediscovering columns or issuing another query. All three HTTP
regressions fail when the three preservation lines are removed and pass with
the fix restored. The targeted HTTP/layout/cache/shared suite passed 110 tests.
The red-run diagnostics are under `artifacts/shared-scroll-cache-2026-10-31/`.

These additions reduce unmapped shared assertion IDs from 52 to 50. Browser
mapping coverage remains 105 of 207 host cases, with 36 workflow groups still
unmapped. The new shared mappings and production cache fix require installed
candidate qualification; the earlier frozen manifest is unchanged.

## Frozen candidate `4055980` and totals driver follow-up

Run `75694f67f9134e8caedbf0351c32fa2b` tested candidate
`40559802dfca46f5f2449c838c83d76fbf4046f4`. Its wheel SHA-256 is
`5e73bf8e2e9b4f8e5041fe812b218d17679dea319e097a0da74a57233321ec98`;
the npm tarball and contract hashes match candidate `96973e4` above.
Evidence is retained under
`artifacts/qualification/candidate-4055980-full-2026-10-31/`.

All six exact download cases passed with current loaded asset observations.
Both new shared scrolling groups passed their two installed HTTP assertions,
including cached revision preservation. The strict manifest passes the complete
CSV, XLSX, vertical-scroll and horizontal-scroll requirements for both release
behavior and reference comparison. All 21 mapped shared groups and all 34 AG
Grid reference tests passed. Product browsers reported 98 passes, one failure
and zero skips across 99 selected tests.

React's totals driver encountered a detached element during the initial render,
before any totals action or state observation. Its button enumeration held
references across several Selenium calls. The driver now looks up each control atomically across shadow
roots, then uses a native click with bounded reacquisition only for stale
references. A successful click ends the retry immediately; interception errors,
missing controls and failed value assertions remain failures. Three focused
driver tests and the 64-test driver/workflow suite passed, and the corrected
totals journey passed separately in Dash, React and Streamlit under
`artifacts/totals-controls-rerun-2026-10-31/`.

Those three reruns are supplemental with `qualificationEvidence: false` and
never replace the frozen failure. Candidate `4055980` remains failed, with 20
passed release groups, 51 failed/incomplete and one not applicable; reference
comparison reports 18 passed, 53 failed/incomplete and one not applicable. The
remaining 36 unmapped browser workflows and 50 unmapped shared assertion IDs
still block complete qualification. The totals driver correction needs a new
frozen run for release credit.

## Embedded-delimiter browser coverage

Six new executable browser mappings cover selected cells containing tabs and
newlines across Dash, React and Streamlit. Each selects exactly the depth-two
hierarchy cell and its adjacent Q1 units cell, then checks the actual clipboard
against literal quoted TSV, including indentation, the neighbor and the exact
one-row/two-column geometry. The LF scenario also verifies a trailing empty
neighbor. The newline journey requires both LF and CRLF source scenarios in
fresh browsers; it permits only CRLF-to-LF normalization at the OS boundary.

All six cases (nine source scenarios) passed against separately installed
candidate `4055980` artifacts under
`artifacts/embedded-clipboard-2026-10-31/`. Raw clipboard strings, parsed rows,
screenshots and per-browser loaded asset digests are retained. These results
are supplemental with `qualificationEvidence: false`; they never amend a
frozen candidate's manifest. Independent review found no blockers. The tooling
suite passed 175 tests; the expanded literal oracle suite passed 16 tests,
including corrupted separators, prefixes, neighboring values and shapes.

The runner now maps 111 of 207 required host cases, leaving 34 browser workflow
groups unmapped. The 50 unmapped shared IDs are unchanged: these browser cases
do not qualify byte-exact LF/CRLF serialization before the OS boundary. The new
mappings require a fresh frozen run for release credit.

## Axis-swap qualification additions

The runner now maps `layout-swap.transpose` to a real runtime round trip and
three native browser journeys. Literal oracles check all 27 measure cells in
each orientation, including units, variance and totals. Browser observations
require current request axes and scope, exact displayed values, complete
schema column identities/indices, matching cell/schema revisions, and revision
sequence `[original, transposed, original]` with a changed middle revision.
Swapping back also restores the displayed column identities.

The runtime assertion passed against the separately installed `4055980` wheel,
and Dash, React and Streamlit all passed under
`artifacts/axis-swap-rerun-2026-10-31/`. The pinned AG Grid 36.2 SSRM comparator
passed separately under `artifacts/axis-swap-reference-final-2026-10-31/`.
That comparison uses AG Grid's public column-state API to reconfigure axes,
scrolls the actual grid viewport, and checks displayed sales/totals, requested
axes and restoration. Its pinned modules and configuration remain unchanged.

Earlier diagnostic failures remain retained. Dash's first attempt exposed a
defined color-statistics sidecar in its payload, which the oracle now validates
and excludes from data-row counting; duplicate/unknown sidecars still fail.
Review also reproduced acceptance of stale schema columns with matching
revision strings, corrected by explicit schema identity checks. The first
reference attempts assumed an off-screen column was rendered and then called
an optional Scroll API absent from the pinned profile; the final test uses
the existing browser scroller. These were qualification corrections, with no
production implementation changes.

The focused runtime/workflow/runner/manifest suite passed 196 tests; independent
review found no remaining blockers. These live results are supplemental with
`qualificationEvidence: false` and never replace frozen candidate records.
Mapping coverage is now 114 of 207 host cases, with 33 browser workflow groups
and 49 shared assertion IDs still unmapped. A new frozen run remains required
for release credit.

## Layout-mode qualification additions

`layout-modes.switch` now maps to an installed runtime assertion and native
browser journeys for Dash, React and Streamlit. The expanded, subtotal-enabled
fixture cycles Hierarchy → Outline → Tabular → Hierarchy. Each state checks
five exact region/account paths, all 45 numeric cells, scope-filtered request
correlation, schema identities and matching schema/data layout revisions.
Hierarchy requires indented labels; Outline requires stepped dimensions;
Tabular repeats the region on account rows and places each subtotal after its
child. Row bounds must align without overlap, each group must remain contiguous,
and physical row order must match the response. Returning restores the original
column identities and group order. This qualifies that fixture and subtotal
configuration, rather than every possible layout combination.

The separately installed `4055980` wheel assertion and all three Chrome
156.0.8078.76 browser journeys passed under
`artifacts/layout-modes-rerun-2026-10-31/`. Each browser observation retains four
states, raw rows, cell bounds, screenshots and loaded-asset SHA-256 digests.
The pinned AG Grid 36.2 SSRM comparison also passed there: its public
`groupDisplayType` cycles singleColumn/multipleColumns/singleColumn with an
explicit server-side purge/reload and re-expansion, checking actual dimension
labels, ancestry, sales and totals. No direct AG Grid analogue for the product's
Tabular mode is claimed.

Initial failures remain under `artifacts/layout-modes-2026-10-31/`. Streamlit's
Expand All control needed scrolling into view before its native click. AG Grid's
existing row nodes retained group labels under the previous auto-column IDs;
the comparator now reloads those nodes before checking the new presentation.
Neither correction changes the production implementation. Review additionally
exposed false acceptance of overlapping or interleaved rows; six negative cases
failed before the oracle fix and now pass. Independent final review found no
blocking findings.

The selected tooling/runtime suite passed 396 tests, with two opt-in checks
skipped and 119 flagship cases deselected. These skips provide no release credit.
All live results above are supplemental (`qualificationEvidence: false`), never
amend the frozen candidate's records, and require a fresh frozen run for release
credit. Coverage is now 117 of 207 required host cases; 32 browser workflow groups
and 48 shared assertion IDs remain unmapped. Full release qualification remains
on hold.

## Read-only layout-history qualification additions

`undo-redo.layout` now has executable installed-host mappings. Starting from a
sales-only pivot prepared through sidebar controls, the browser adds units,
transposes axes, undoes both changes, redoes both changes, undoes transposition,
then adds variance as a new branch. All nine states require exact logical rows,
measure columns, displayed values and authoritative raw values/totals, current
request axes/aggregations/scope, and matching schema/data revisions. The restored
states must recover their original column identities, row order and revision;
the new branch must disable redo. This checks 153 numeric values across the nine
states per host. It qualifies layout history only, with no cell-edit history
claim.

Dash, React and Streamlit all passed against separately installed `4055980`
artifacts under `artifacts/layout-history-2026-10-31/`. Each observation retains
nine screenshots/states, request traces, raw/displayed cells and loaded asset
digests. The installed shared assertion also passed: it verifies authoritative
responses for the restored models, while browser tests exercise actual history
controls. The selected tooling/runtime suite passed 302 tests, with 122 live
cases deselected; independent review found no actionable issues.

The pinned AG Grid 36.2 SSRM comparator passed under
`artifacts/layout-history-reference-rerun-2026-10-31/`. This comparison explicitly
uses application-managed undo/redo stacks and primary axis/value column-state
snapshots restored through public APIs. It checks all nine models, raw and
visible values, totals, exact displayed column identities and branch clearing.
It does not claim built-in AG Grid layout history. The initial failed reference
run is retained in the product-run directory: full column-state snapshots
included generated pivot-result columns unavailable after transposition. The
wrapper now persists primary layout fields and lets SSRM regenerate result
columns; no production code was changed.

These observations remain supplemental (`qualificationEvidence: false`) and
never amend the frozen candidate. A fresh frozen candidate run is required for
release credit. Mapping coverage is now 120 of 207 required host cases, with 31
browser workflow groups and 47 shared assertion IDs still unmapped. Full release
qualification remains on hold.

## Local saved-view persistence correction and qualification scope

The previous Save View action emitted a complete snapshot to the host but local
persistence stored only selected settings. The shared runtime now stores the
explicitly saved version-1 snapshot when persistence is enabled and restores it
for the same component ID, storage type and table. An external `viewState`
takes precedence. Snapshot restoration is consumed once per object identity,
so subsequent axis/measure changes cannot be rolled back by a prop echo; a new
external snapshot still restores. Explicit reset clears the local snapshot.
Persistence opt-out continues to emit host snapshots without writing storage.
No new public props or host-specific storage implementations are introduced.

Qualification targets are `persistence.reload` and the combined
`journey-persistence.resize-density-reload`: transpose, physically widen sales
by 40 CSS px, choose compact rows, save, reload and compare exact values/model,
column identities, width within 2 px and height within 1 px. A further live
transpose checks that the consumed snapshot does not override new interaction.
Fresh browser profiles isolate each workflow; the parity fixture enables local
persistence to exercise the same opt-in as the normal demo. The shared backend
assertion checks restored-model equivalence, while the real-hook Node test and
installed browsers own storage, lifecycle and geometry verification.

The AG Grid comparator uses application-owned local storage with public
`getState`/`setState`; row-height and resized pivot-width preferences are stored
alongside Grid State. The width is applied through public Column State after
the matching result column exists.
This is an application wrapper comparison, not a claim that AG Grid owns
local-storage persistence. At the implementation commit, live qualification
was pending a fresh artifact build;
the new runner mappings alone provide no release credit.

The first `3c9c126` installed reload run exposed a real effect-ordering defect:
the snapshot restored two decimal places, then initial prop synchronization
overwrote it with zero. All three hosts reproduced this, with correct raw data
and visibly rounded values. The guarded snapshot restore now runs after initial
prop synchronization. A source-effect execution regression reproduced `0`
instead of `2` before the move and passes afterwards. This runtime change needs
a new artifact build; prior failed observations are retained. Earlier harness
failures distinguished 43 px cells from their 44 px bordered row containers and
required opening Theme before the real Save View action. The tests now measure
row containers and retain cell-border checks. The AG wrapper serializes its
snapshot before comparing it and discloses deferred pivot-width restoration.

### Targeted installed results

Candidate `cf81ed7b19842cad4a23a04dc613acfa1745863f` built fresh Dash, React and
Streamlit bundles plus the wheel and npm package. Both package-contract checks
passed, and the restored-model assertion passed against the installed wheel.
Dash, React and Streamlit all passed the complete browser journey under
`artifacts/saved-view-cf81-validation-2026-10-31/run-final/`. Every host restored
the widened 180 px column (140 + 40), 36 px compact rows, exact saved axes,
column identities/revision and all 27 numeric cells, then kept a subsequent
live transpose stable. Observations retain snapshots, three model/value states,
geometry, screenshots and loaded-asset SHA-256 digests. The initial `cf81ed7`
attempt reached correct restored values but its harness needed to reopen View
before reading the density control; those failed records remain retained.

Both pinned AG Grid comparator cases passed under
`artifacts/saved-view-candidate-final-2026-10-31/reference/`. The explicit
application-wrapper scope above applies, including deferred width restoration.
Independent review found no remaining blockers. The effect-order fix passed
202 frontend/ownership/persistence tests plus TypeScript and checked-JavaScript
gates; the literal geometry/snapshot suite passed 14 tests.

These targeted runs are supplemental (`qualificationEvidence: false`), not a
complete release qualification, and never amend earlier frozen manifests.
Runner coverage is 126 of 207 required host cases, with 29 browser workflow
groups and 45 shared assertion IDs still unmapped. A complete frozen-candidate
run and the remaining release checks are still required before publishing.

## Unicode and empty selected-cell qualification scope

`copy-selection.unicode` and `copy-selection.empty-values` now map to one
concrete browser journey per host. A known saved-view configuration groups the
unchanged edge fixture by row ID and requests `first` for four string fields.
Only configuration is seeded; rows, responses, selection and clipboard are
never injected. The V1 response must retain all four Included rows and the
distinct raw null and empty-string values. A native drag selects exactly four
cells on `q2-munchen`, then the real Copy Selection action must produce literal
TSV containing `Éclair München 東京 القاهرة 🚀`, an interior empty field, the
row-ID anchor and a trailing empty field. Exact codepoints, bytes and 1×4 shape
are checked independently of the returned clipboard. Existing AG Grid special
text cases are reused for the pinned comparison rather than replaced by new
placeholder mappings.

The source setup uncovered a shipped Ibis compatibility defect: Ibis 12 rejects
`arbitrary(how=...)`. The expression builder now detects that API capability once
at import, retaining the legacy reduction call where supported and using modern
`first`/`last` reductions otherwise. This preserves the older distinction from
window methods documented in the [Ibis 4 source](https://github.com/ibis-project/ibis/blob/4.0.0/ibis/expr/types/generic.py).
The [current Column API](https://ibis-project.org/reference/expression-generic#ibis.expr.types.generic.Column.first)
describes modern reductions and their backend-dependent ordering without an
ordering key. These tests use singleton groups and make no ordered multirow
first/last claim. Real runtime tests failed before the fix and now preserve
Unicode, null and empty values for both aggregations. Legacy API-shape tests
retain that branch; a complete historical Ibis-version matrix is not claimed.

The shared installed assertions verify authoritative source values and schema;
browser tests own actual selection and clipboard delivery. The existing Node
serializer suite now also checks selected values with precomposed and decomposed
Unicode, emoji and both empty positions before the OS boundary. Independent
review found no blocker. Fresh installed artifact execution is still pending;
runner mappings and repository unit checks alone provide no release credit.

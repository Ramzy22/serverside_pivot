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

Candidate `2a3d998a314b6147d1be54200059061a3fc7bfa2`, run
`8ee6b1ce8f774e42bf3bb86c29bf33bb`, passed the custom-order browser journey in
Dash, React and Streamlit using Chrome `156.0.8078.73`. That journey checked a
reordered wide sparse grid, an off-window pin, exact clipboard headers and
values, XLSX ordering and merged headers, and saved width/order/pin restoration.
Each observation includes loaded-asset hashes. The binding and observations
are under `artifacts/qualification/candidate-2a3d998-full-2026-10-29/`.

All 87 mapped product browser tests and 34 pinned AG Grid reference tests passed
in that candidate. The complete release manifest remains failed: 14 of 72
requirement groups passed release qualification, 57 lacked complete evidence,
and one was not applicable. The custom-order shared assertion was unmapped in
that run. Its new mapping and the subtotal browser mapping require a fresh
installed-candidate run; local passing tests do not amend historical evidence.

## Follow-up revision regression

A same-manifest regression reproduced a revision collision when changing the
secondary measure's aggregation: its saved layout ID stayed stable, but its
cell key changed without a new revision. The resolver fingerprint now includes
both the saved layout ID and the current cell key. Pure resolver and real
runtime regressions cover this change, and the installed aggregation mapping
requires the secondary-measure test. Fresh installed evidence is still required
for this correction; the earlier candidate did not exercise it.

This report verifies the layout contract and records current evidence. It does
not qualify the complete release, deployment topology or operational controls.

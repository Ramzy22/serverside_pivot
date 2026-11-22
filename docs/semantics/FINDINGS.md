# Audit findings → requirements → tests

Status values:
- **fixed**: the test passes.
- **gated**: the path is denied for protected datasets until proven (REQ-SEC-005).
- **legacy**: a strict xfail that must flip when the core replaces the engine (M2).
- **pending**: the test arrives with the named milestone.

| Id | Finding | REQ | Test | Status |
|---|---|---|---|---|
| F-SEC-DASH | Dash drill / detail / tree / data / chart / filter options ignored the server grant | SEC-001 | `tests/security/test_tenant_isolation_matrix.py` (dash) | fixed (S0) |
| F-SEC-CASE | Scope compared case-insensitively (`a` matched `A`) | SEC-003 | `test_tenant_isolation_matrix.py`, `test_scope_authority.py::test_scope_is_exact_and_case_sensitive` | fixed (S0) |
| F-SEC-FAILOPEN | Unknown scope column ignored; lost context read unscoped | SEC-001, SEC-002 | `test_scope_authority.py` | fixed (S0) |
| F-SEC-PRUNE | Pruned / progressive hierarchy, rollups and prefetch bypassed scope | SEC-005 | `test_rest_isolation_matrix.py`, `test_scope_authority.py::test_unproven_path_denied_only_under_protecting_scope` | gated (S0) |
| F-SEC-LIVE-IDOR | Any principal could drive another's subscription by id; delete another's baseline | LIVE-SEC-001 | `test_live_isolation_matrix.py` | fixed (S0) |
| F-SEC-LIVE-CASE | Live snapshots used the case-folding filter | SEC-003 | `test_live_isolation_matrix.py::test_subscribe_snapshot_is_scoped` | fixed (S0) |
| F-SEC-DASH-DEFAULT | Dash served data with no authorization configured | SEC-006 | `test_scope_authority.py::test_dash_requires_authorization_by_default` | fixed (S0) |
| F-NULL-NAN | SQL NULL serialized as NaN; HTTP drill / detail / tree crashed | NULL-001 | `test_tenant_isolation_matrix.py` (http detail / drill / tree) | fixed (S0) |
| F-NULL-GT | NULL dimension rendered as "Grand Total"; real total and NULL subtree lost | NULL-002 | vector `null-group-is-not-grand-total`, `test_legacy_findings.py::test_null_group_and_grand_total_are_both_present`, `tests/oracle/test_properties.py` | legacy |
| F-NULL-LEAF | NULL member at the leaf level dropped | NULL-002 | `tests/oracle/test_properties.py` | legacy |
| F-TOT-MF-PARENT | Parents and grand total include filtered-out leaves | TOT-003 | vector `leaf-measure-filter-visible-totals`, `test_engine_invariants.py::test_measure_filter_visible_totals_match_oracle`, `test_legacy_findings.py` | legacy |
| F-TOT-CACHE | Grand total lost on a cache hit | TOT-006 | `test_legacy_findings.py::test_grand_total_survives_a_cache_hit` | legacy |
| F-TOT-PAGE | Grand total lost on page 2 | TOT-006 | `test_legacy_findings.py::test_grand_total_survives_page_two` | legacy |
| F-HAVING-IGNORED | `having` silently ignored | TOT-001 | `test_legacy_findings.py::test_having_is_applied` | legacy |
| F-TOT-FALLBACK | Fallback hierarchy sums avg / max / distinct for parents | TOT-003 | `test_legacy_findings.py::test_fallback_hierarchy_parents_are_aggregated_from_records` | legacy (the runtime path passes: vector `parent-avg-max-distinct-from-records`) |
| F-RANK | Rank is 0-based and in row order, not by measure | AGG-005 | `test_legacy_findings.py::test_rank_is_one_based_by_measure` | legacy |
| F-MOVING-AVG | Moving average errors and returns NULL | AGG-005 | `test_legacy_findings.py::test_moving_average_is_computed` | legacy |
| F-ROLLUP-CRASH | ROLLUP / CUBE crash (schema mismatch in the union) | — | `test_legacy_findings.py::test_rollup_and_cube_execute` | legacy |
| F-PIVOT-MEDIAN | Pivot drops non-additive measures (median) | AGG-001 | `test_legacy_findings.py::test_pivot_keeps_every_measure` | legacy |
| F-PIVOT-DELIM | Pivot column keys joined with `|` collide | ID-001 | `test_legacy_findings.py::test_pivot_column_keys_with_delimiters_do_not_collide` | legacy (row identity on the runtime path passes: vector `delimiter-values-do-not-collide`) |
| F-CACHES | ~20 ad-hoc caches, some keyed by `id()` | SEC-004 | `test_tenant_isolation_matrix.py::test_cache_is_not_shared_across_tenants`, `tests/core/test_cache.py` | core manager done (M1: keys include the scope fingerprint and revision); legacy caches are removed in M4 |
| F-SILENT | Failures returned as empty tables or NULL columns | AGG-001 | `tests/core/test_core_semantics.py::test_invalid_queries_are_typed_errors`, `tests/core/test_spec.py` | fixed in the core (M1); legacy paths are removed in M4 |
| F-FRONT-RACE | Out-of-order responses, column-window stamping, duplicate identities in the frontend | PAGE-004, ID-001 | M3 tests | pending M3 |
| F-PAGE-DEEP | (found in the M1 benchmark) Deep pages of a fully expanded deep tree recompute the whole tree: 6 levels x 10M rows took 40 s for 200 rows at offset 5M | PAGE-002, PAGE-003 | `tests/core/test_pagination.py` | fixed in the core (M1): adaptive `materialized` paging holds the ordered tree server-side once a scroll is under way, so each page is a 0.03-0.14 s range scan; trees over `max_held_rows` get a typed error |
| F-DRILL-NULL | (found in M2) Drill-through on a NULL member drops the path condition (an empty display part is skipped), so the page lists every record of the parent | DET-001 | `tests/core/test_core_detail.py::test_pages_partition_the_node_records` | fixed in the core (M2): typed `rowKey`; a display path with an empty member is not served by the core |
| F-DRILL-ORDER | (found in M2 shadow) Unsorted drill pages have no defined order, so consecutive pages can repeat or skip records | DET-001 | `test_core_detail.py::test_pages_partition_the_node_records` | fixed in the core (M2): total order (sort keys, then every column) |
| F-TOT-EMPTY | (found in M2 shadow) Over an empty result legacy returns no grand total and `total_rows = 0` | TOT-008 | `tests/core/test_core_semantics.py::test_grand_total_exists_over_empty_data` | fixed in the core (M2) |
| F-LIVE-NULL-ORDER | (found in M2) The live tree orders the NULL member first; the static view orders it last | BACK-001 | `tests/core/test_live_parity.py::test_live_tree_orders_null_members_last` | legacy (strict xfail; values and nodes already equal the core: `test_live_tree_equals_core_after_every_batch`) |
| F-DRILL-FIELDS | (found in M2 shadow) Legacy drill reads only `row_fields`/`pathFields`; a payload with `rowFields` (the form `RuntimeDrillService.normalize_request` emits) loses its path and returns every record in scope | DET-001 | `tests/core/test_core_bridge.py::test_legacy_drill_honours_row_fields` | legacy (strict xfail); the core reads `rowFields` |
| F-CLIENT-WASM-DISCARDED | (found in M3) Client mode runs the planner's SQL on DuckDB-Wasm and discards the result; every number is aggregated in JavaScript | AGG-001 | `tests/core/test_client_core_parity.py` | fixed: client data requests are served by `runClientCore` (emitted SQL on the DuckDB connection) |
| F-CLIENT-COLLATION | (found in M3) Client mode orders members and pivot columns with `localeCompare` (numeric, case-insensitive), the server with binary collation | BACK-002 | `tests/core/test_client_core_parity.py::test_client_mode_equals_server_mode` | fixed on the core path |
| F-CLIENT-NULL-LABEL | (found in M3) Client grouping labels NULL as the string `"null"` and groups by `String(value)`, so NULL and "null", and 1 and "1", merge | NULL-001, ID-001 | `tests/core/test_client_core_parity.py::test_client_core_keys_are_canonical` | fixed on the core path (typed canonical `_rowKey`) |
| F-EXPAND-EMPTY | (found in M3 parity) Expansion state is keyed by display path, which cannot address the NULL or "" member: the client parser drops the empty part (expanding the parent's path instead), the server coerces it to "" (a NULL node can never expand) | ID-001 | `tests/core/test_client_core_parity.py::test_ambiguous_expansion_is_not_guessed` | core paths decline it (exact legacy fallback, reason recorded); fix = expansion keyed by canonical `_rowKey` |
| F-FIRST-LAST | (found in M4) Legacy `first`/`last` return whichever row the scan yields first or last (`arbitrary(how=...)`), which SQL leaves undefined: the value can differ between pages, caches and backends | AGG-001 | none: the core declines them (reason `aggregation 'first'` / `'last'`) | **decision needed before legacy is deleted**: define them over an explicit order column, or reject them |
| F-CAT-CASE | (found in M4) Legacy custom-dimension conditions use the case-folding legacy filter builder, membership included | DIM-001, NULL-003 | `tests/core/test_core_bridge.py::test_custom_dimension_is_core_served_and_matches_legacy` (exact-case data) | the core is exact unless a clause asks for case-insensitivity (agreed F-CASE rule) |

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
| F-PAGE-DEEP | (found in M1 benchmark) A viewport over a fully expanded deep tree sorts the whole flattened tree before windowing: 6 levels × 100M rows = 206 s for 200 rows | PAGE-002 | `scripts/benchmark_core_strategies.py` (artifacts/benchmarks) | open: level-wise / keyset windowing in `core/logical` before M2 cutover of the scroll paths |

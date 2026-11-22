# Pivot semantics: normative requirements

This document defines what a correct answer is. Every engine (the Python core,
the DuckDB-Wasm client emitter, and the legacy engines until they are deleted)
is judged against it by the oracle (`tests/oracle/`) and the conformance
vectors (`tests/conformance/`). Every requirement id has at least one test that
cites it (`tests/test_requirements_traceability.py`).

The key words MUST, MUST NOT and SHOULD are used as in RFC 2119.

---

## 1. Security (REQ-SEC)

| Id | Requirement |
|---|---|
| REQ-SEC-001 | Row scope is attached to **dataset access**, not to a request shape. Every relation resolved for a protected dataset MUST be `base ∩ scope`. If a protected dataset is resolved with no active grant (lost context, a background thread, a new entry point), the resolution MUST fail with `UnscopedAccessError`. It MUST NOT fail open. |
| REQ-SEC-002 | A grant that names a column the dataset does not have MUST deny (`ScopeColumnError`). It MUST NOT be ignored. |
| REQ-SEC-003 | Scope predicates are exact: binary, case-sensitive equality, values converted to the column type without loss. Grant values are non-null scalars, because the row policy rejects NULL or empty claims. A row whose scope column is NULL is therefore never visible to a claims-scoped principal. (The predicate compiler still treats a NULL value as matching only NULL, defensively.) An empty value list denies. |
| REQ-SEC-004 | A cache entry (result, viewport, plan, table expression, hierarchy, snapshot) MUST NOT be served to a request whose grant fingerprint differs from the one that produced it. |
| REQ-SEC-005 | Deny until proven. An execution path that has no passing case in the isolation matrix MUST be denied for protected datasets (`PathNotProvenError`). Every external entry point (FastAPI routes, runtime kinds, Dash callbacks, Flight actions, subscription actions) MUST appear in the matrix, and the suite fails when one does not. |
| REQ-SEC-006 | Dash reads require a dataset policy and a principal resolver by default. Unauthenticated access is an explicit development opt-in and is never honoured in production. |
| REQ-SEC-007 | Raw SQL that references a protected dataset is refused while a protecting grant is active, because raw SQL cannot be proven scoped. |
| REQ-LIVE-SEC-001 | Only the principal and grant that created a subscription may poll, acknowledge, modify, pause or close it. Only a caller with the snapshot's grant fingerprint may list, acquire or delete that snapshot. |

## 2. Filters and totals (REQ-TOT)

Definitions, for a request over dataset `D`:

* **E₀** = records of `D` satisfying `scope ∧ row filters` (dimension predicates evaluated before aggregation).
* For each hierarchy level `ℓ` that carries measure filters: a group `g` at `ℓ` **passes** when its measures, aggregated over `E₀ ∩ g`, satisfy every filter at `ℓ`.
* **E** = records of `E₀` whose group passes at *every* filtered level.

| Id | Requirement |
|---|---|
| REQ-TOT-001 | Measure filters are evaluated on E₀. They are never evaluated on an already filtered subset, so the result does not depend on the order levels are evaluated in. |
| REQ-TOT-002 | A node `n` is shown iff `E ∩ n ≠ ∅`. |
| REQ-TOT-003 | `totals = visible` (default): every subtotal and grand total is `agg(E ∩ n)` and is computed from records, never by re-aggregating child aggregates (avg, distinct, median and min/max over filtered children all depend on this). |
| REQ-TOT-004 | `totals = all_data`: subtotals and grand total are `agg(E₀ ∩ n)`. |
| REQ-TOT-005 | Expansion state never changes eligibility or totals. It only changes which rows are materialized. |
| REQ-TOT-006 | The grand total is the same on every page, on a cache hit, and with or without measure filters. |
| REQ-TOT-007 | A measure filter that names no level (the UI's column filter on a measure) applies at the deepest row level; parents and the grand total are then visible totals (REQ-TOT-003). |

## 3. Aggregation (REQ-AGG)

| Id | Requirement |
|---|---|
| REQ-AGG-001 | Every aggregation is defined once in the registry (`spec/aggregation-registry.json`), with its input types, its state, merge/retract/finalize, and its empty-input and all-NULL results. An unknown aggregation is an error. It MUST NOT silently fall back to `sum`. |
| REQ-AGG-002 | NULL inputs are ignored by `sum`, `avg`, `min`, `max`, `count(col)`, `count_distinct`, `median`, `percentile` and `string_agg`. `count(*)` counts rows. `sum` of an empty or all-NULL input is NULL. `count` of one is 0. |
| REQ-AGG-003 | `count_distinct` is exact by default. Approximate distinct is an explicit, labelled opt-in. |
| REQ-AGG-004 | Decimal inputs keep exact decimal arithmetic. Integer sums do not overflow silently. They widen, or they error. |
| REQ-AGG-005 | Window measures: `rank` is 1-based, ordered by the measure with ties sharing a rank, and partitioned by the parent node. Moving averages use exactly the declared frame. |

## 4. NULL and identity (REQ-NULL, REQ-ID)

| Id | Requirement |
|---|---|
| REQ-NULL-001 | SQL NULL stays NULL end to end: JSON `null`, never `NaN`, `""`, `"null"` or `"Grand Total"`. |
| REQ-NULL-002 | A NULL dimension value is its own group, distinct from the empty string and from the grand total. |
| REQ-NULL-003 | Row filters: membership (`in`, `not_in`) is set membership, so a NULL in the list selects the NULL group and `not_in` keeps NULL rows unless NULL is listed. Comparisons, `between` and string operators never match NULL. Case folding (`caseSensitive: false`) applies to `=`, `!=` and string operators on string columns; membership is always exact, because case variants are distinct groups. |
| REQ-ID-001 | A row's identity is `(dataset, presence_mask, [(field_id, typed value)…])`, encoded with `CanonicalPathCodec`. The grand total is the empty path, and a NULL member is a typed null token, distinct from `""`. Identity never comes from joining strings, so values containing `|`, `_` or `/` cannot collide. |
| REQ-ID-002 | Absent (a rolled-up level) and NULL (a member whose value is NULL) are different identities. |

## 5. Pagination and consistency (REQ-PAGE)

| Id | Requirement |
|---|---|
| REQ-PAGE-001 | Hierarchy pagination is over the flattened visible tree, in a total order: the hierarchical order key, then the full sort tuple, then the identity. Concatenating pages equals the unpaged result. |
| REQ-PAGE-002 | A cursor binds: query fingerprint, state epoch, source revision or snapshot id, grant fingerprint and policy version, the full sort tuple, and the expansion hash. A cursor that does not match MUST be rejected. It MUST NOT be reinterpreted. |
| REQ-PAGE-003 | Consistency modes: `snapshot` pages a pinned snapshot; `materialized` pages a server-held result; `restart_on_change` invalidates the cursor when the revision moves. |
| REQ-PAGE-004 | Column windows: concatenating the column pages equals all columns, and every page carries the `colWindow` it answers. |

## 6. Live (REQ-LIVE)

| Id | Requirement |
|---|---|
| REQ-LIVE-001 | In live mode, row structure stays stable while values refresh. Rows are reordered only on an explicit refresh or with live sort. |
| REQ-LIVE-002 | A patch applied to the previous frame equals a full recomputation at the new revision (checked by the differential fuzz tests). |

## 7. Backend differences (REQ-BACK)

| Id | Requirement |
|---|---|
| REQ-BACK-001 | Default NULL ordering is NULLS LAST for ascending and descending sorts, on every backend. |
| REQ-BACK-002 | String comparison and grouping use binary, case-sensitive collation. |
| REQ-BACK-003 | Timestamps are compared in UTC. Each backend states its decimal scale and rounding in its capability entry. |

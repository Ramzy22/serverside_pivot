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
| REQ-TOT-007 | A measure filter that names no level (the UI's column filter on a measure) applies at the deepest row level; parents and the grand total are then visible totals (REQ-TOT-003). A flat AND group that mixes dimension and measure conditions is the conjunction of its parts: each dimension condition is a row filter (it defines E0), each measure condition a measure filter at the deepest level, whatever the term order. An OR group, or a nested group, that names a measure has no exact meaning and is a typed error. A `having` entry is a level-less measure filter of the same kind; one that names a non-measure field is a typed error. Filter operators accept the edge spellings (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `equals`, ...) as synonyms of the same operators. |
| REQ-TOT-008 | When totals are enabled, the grand total row always exists, even over an empty E (its measures follow the registry's empty-input rules), so `total_rows` is at least 1. |

## 3. Aggregation (REQ-AGG)

| Id | Requirement |
|---|---|
| REQ-AGG-001 | Every aggregation is defined once in the registry (`spec/aggregation-registry.json`), with its input types, its state, merge/retract/finalize, and its empty-input and all-NULL results. An unknown aggregation is an error. It MUST NOT silently fall back to `sum`. |
| REQ-AGG-002 | NULL inputs are ignored by `sum`, `avg`, `min`, `max`, `count(col)`, `count_distinct`, `median`, `percentile` and `string_agg`. `count(*)` counts rows. `sum` of an empty or all-NULL input is NULL. `count` of one is 0. |
| REQ-AGG-003 | `count_distinct` is exact by default. Approximate distinct is an explicit, labelled opt-in. |
| REQ-AGG-004 | Decimal inputs keep exact decimal arithmetic. Integer sums do not overflow silently. They widen, or they error. |
| REQ-AGG-005 | Window measures: `rank` is 1-based, ordered by the measure with ties sharing a rank, and partitioned by the parent node. Moving averages use exactly the declared frame. |
| REQ-AGG-006 | `stddev` and `variance` are the sample statistics and are NULL with fewer than two inputs. `percentile` takes `q` in [0, 1] and interpolates linearly (QUANTILE_CONT). `weighted_avg` uses only complete (value, weight) pairs and is NULL when their total weight is 0. |
| REQ-AGG-007 | Derived measures are computed over the visible tree. A `ratio` is `num / den`, and a NULL or zero denominator gives the declared `null_value` (default NULL). `percent_of_total` divides by the grand total's value. `cumulative` and `moving_avg` run over siblings in display order within the declared rows frame (default: from the first sibling to the node), ignoring NULLs. |
| REQ-AGG-008 | `first` / `last` need an order field. Among the node's records where both the value and the order value are present, `first` is the value at the smallest order value and `last` the value at the largest. Ties at that order value resolve to the smallest (`first`) or largest (`last`) value, so the result is always the same. With no such record the result is NULL. A `first` / `last` measure without an order field is a typed validation error: an arbitrary record is never returned. |
| REQ-AGG-009 | `array_agg` collects a node's non-NULL values into a list in a defined order. With an order field, the list is ordered by it, and ties at one order value are ordered by the value. Records whose order value is NULL are skipped. Without an order field, the list is ordered by the value itself; a struct value orders by its fields in turn, so `{x, y}` points are ordered by `x`. With no such record the result is NULL. A series is shown, never compared or computed with: a measure filter, a sort or a derived measure over a series measure is a typed validation error. |
| REQ-AGG-010 | A formula measure is an expression over other measures, evaluated on each node's own values: a group row, a pivot cell, the row-total column and the grand total alike. A total is the formula of the total's measures, never a sum of its children's formula values, so North with sales 200 and cost 140 has margin (200 − 140) / 200 = 0.3. The expression has numbers, references to measures or other formulas, `+ − × ÷`, a power with a constant exponent, unary minus, `abs`, `ceil`, `floor`, `trunc` (legacy `int`), `round` to 0–12 digits (half away from zero) and `min` and `max` of 2–8 operands. A NULL operand makes the result NULL. So do division by zero, a negative base with a fractional exponent, zero with a negative exponent and any result that is not finite. A formula that names an unknown measure, cannot be parsed, or lies in or depends on a reference cycle is NULL. A formula can be sorted on and filtered on like a measure; a formula over a series measure is a typed validation error. |

## 3a. Order (REQ-SORT)

| Id | Requirement |
|---|---|
| REQ-SORT-001 | Siblings are ordered by the sort entries in precedence order. A dimension entry orders only its own level by member value. A measure entry orders the siblings at every level by the node's own value. Ties, and levels without a dimension entry, fall back to the member value ascending. NULL sorts last in both directions, for members and for measure values. A parent precedes its children, unless REQ-LAYOUT-001 places it after them. |
| REQ-SORT-002 | An `absolute` sort entry orders a numeric dimension or measure by its magnitude, with the same direction and NULLS LAST rules. It is invalid on non-numeric values. |
| REQ-SORT-003 | A dimension entry with a member key `by` orders that level's members by MIN(`by`) over each member's value records (E∩n under `visible`, E₀∩n under `all_data`), with NULLS LAST in both directions, and ties broken by the member value ascending. Legacy `sortKeyField`, such as tenor labels ordered by a day count, maps to it. A key column that is not in the table is ignored, as in legacy. A supplied key is the order whatever the entry's sort type: an absolute or semantic sort type does not change it, because keys are ranks. |
| REQ-SORT-004 | A measure entry with a column `member` orders the siblings at every level by the node's value within that column member: a pivot cell, computed over the node's records as for the cell itself, whether or not the member is in the returned column window. NULLS LAST, ties as in REQ-SORT-001. Legacy pivot sort ids map to it: `__RowTotal__<alias>` is the row total, `<column key>_<alias>` is that cell. A plain measure alias, or a column key that names no member, names no column of a pivoted view and is ignored, as in legacy. So is, in any view, an entry naming a source field that is neither a row nor a column dimension, or a measure's `<field>_<agg>` name that is not its alias. An entry on a display-column formula is ordered by the adapter after evaluation and never reaches the core. |
| REQ-SORT-005 | A semantic sort orders a text row dimension by a key computed from each member's value. The one kind is `tenor`. The key is the member's days as a tenor: a number and a unit D, W, M or Y, at 1, 7, 30.4375 and 365.25 days. Otherwise it is the member as an ISO date `YYYY-MM-DD`, or as a month and year (`Jan 2025`, by the month name's first three letters), counted in days from 1970 at the same rates. Whitespace around a value is ignored. A member that matches none of these has no key and comes last in both directions. Members with equal keys are ordered by their value, ascending. This is REQ-SORT-003 with a computed key. A supplied member-key column (`sortKeyField`) takes precedence over the semantic type. |
| REQ-SORT-006 | A sort entry can put its NULLs first (`nulls: "first"`): its NULL values come before every value in both directions. NULLS LAST is the default (REQ-BACK-001). This applies to every kind of entry: a dimension, a member key, a semantic key, a measure and a pivot cell. The tie-break by member value keeps NULLS LAST. Any other `nulls` value is unsupported. |
| REQ-LAYOUT-001 | Subtotal position: `before` (default) is pre-order, so the grand total comes first and each group precedes its descendants. `after` is post-order, so each group follows its descendants and the grand total comes last. Sibling order (REQ-SORT-001), the set of nodes and their values do not change. The tabular layout with subtotals is post-order over the fully expanded tree, with group rows marked `_isTabularSubtotal`. In the hierarchy and outline layouts, "subtotal footers" add no rows: the expandable group row is the subtotal. |
| REQ-LAYOUT-002 | Subtotal position `none` (the tabular layout without subtotals) has no group rows: the nodes are the grand total and the leaves. The leaves are ordered by the sort entries in precedence, each over the leaf's own values (a dimension's value or member key, a measure, or a pivot cell), then by every row dimension ascending, NULLS LAST throughout. The grand total comes last. Expansion does not apply. Leaves report `_has_children = false`, and their identity is the full typed path (legacy truncates the display path at the first NULL member). |

## 3b. Column pivot (REQ-PIV)

| Id | Requirement |
|---|---|
| REQ-PIV-001 | A column pivot has explicit, ordered column members (the column window). Each cell is the measure over the node's records whose column values equal the member, with null-safe typed equality, so a NULL member is the NULL group. Every registry aggregation is supported in cells, including holistic ones. The node's own measure is the row total. The column catalog is the distinct members over E0, ordered by value with NULLS LAST unless an order is requested (REQ-PIV-002); a measure-axis label column is ordered by the member order instead (REQ-DIM-006). Two members whose display keys collide are an error, never a merge. |
| REQ-PIV-002 | Each column dimension may request an order for its catalog: by **label** (the member value; the default for the request, ascending), by **total** (the first measure, descending unless a direction is given), by a **member key** (`sortKeyField`, REQ-SORT-003; an unknown key is ignored), or by **tenor** (REQ-SORT-005), in either direction, optionally by absolute value and NULLS FIRST (REQ-SORT-006). A total is the measure over the E0 records of the member: it includes the row filters and the scope, never the measure filters, so filtering nodes out of the view never reorders the catalog. Ties are broken by the member value ascending, NULLS LAST. Each entry carries its display key; a column cursor returns the entries whose display key sorts after the cursor. |

## 3d. Custom dimensions (REQ-DIM)

| Id | Requirement |
|---|---|
| REQ-DIM-001 | A category is a column computed for every record before filters, in definition order (a rule may reference an earlier category). Its value is the label of the first rule whose condition holds, else the fallback. Conditions are row filters (REQ-NULL-003/004), so a comparison never matches NULL. Labels and the fallback share one type: text, integer or floating. A category can be a row or column dimension, a filter field, a drill-through path field and a filter-options column. Its rules are part of the query's identity. Legacy custom dimensions map to categories with the same labels and fallback. Their "creation" and "numeric" orders map to a hidden numeric category, `__sortkey__<field>`, used as a REQ-SORT-003 member key. Legacy evaluates conditions case-insensitively, including membership; the core is exact unless a clause asks for case-insensitivity (F-CASE). |
| REQ-DIM-002 | The measure axis presents measures as members of a dimension. Each record becomes one record per member, carrying every column plus the member's label, its source value and its order. With `drop_nulls`, a record whose value is NULL is dropped. The axis is applied after the categories and before the row filters, so the label is an ordinary dimension (ordered by the member order as a member key, REQ-SORT-003) and the value column an ordinary measure field. Values share one type: floating when every source is numeric, else the sources' common type, else text. A node above the label level aggregates every member's values. This is meaningful only for comparable measures, and it is what legacy's hierarchy shows. When the members have different aggregations, REQ-DIM-003 applies instead. The axis on columns and zero suppression are REQ-DIM-006. |
| REQ-DIM-003 | In a measure axis whose members each have their own aggregation, every member is a measure of its own source field. A node at or below the label level holds the records of one member, and its value is that member's aggregation over them (the axis value measure has the aggregation `member`). A node above the label level has no value (NULL), because the members' aggregations cannot be combined into one number; the grand total is such a node. Members either all produce a series (REQ-AGG-009) or none do, since they share one value column. |
| REQ-DIM-006 | The measure axis label can be a column dimension instead of a row level. It is then an ordinary column dimension of the unpivoted records (REQ-PIV-001). It is the innermost column unless the column fields place it, and its members are ordered by the member order. A cell holds one member's value at one column member. Under REQ-DIM-003 a cell is at the label, so it holds that member's own aggregation, and the row total (above the label) has no value. With zero suppression, a member whose value is zero or NULL is suppressed. On rows, the suppressed unit is the label node: a level measure filter `value != 0` at the label level. On columns, it is the cell: a measure filter at the cell grain, grouped by the leaf row path and every column dimension. A suppressed member's records leave E, so its cell is empty (NULL) and a node left without records is not shown. Legacy instead showed a suppressed cell as 0 next to other members and dropped only rows whose every cell was zero. A cell-grain filter is a measure filter whose group is the level's path and every column dimension. |
| REQ-DIM-007 | A per-member measure axis (REQ-DIM-003) can have formula members. A formula member has no source field. Its records are a copy of every record, with a NULL value, and suppressing empty members never drops them. At or below the label, its value is the formula (REQ-AGG-010) evaluated on the member's records, which are the node's records, so its references take the node's own values. A formula member references plain measures only: not the axis value, other formulas (the bridge inlines a referenced formula, and a cycle is NULL) or series. It needs a numeric axis. Above the label it has no value, like every member. |
| REQ-DIM-005 | In a ragged (unbalanced) hierarchy over the row fields, each record's path is its present values of those fields, in order. A value is missing when it is NULL or NaN, or when it is one of the listed missing values; with none listed, the empty text is missing. A node at depth d holds the records whose path has at least d entries and shares the node's first d. A record whose path has ended has no node below that depth, and a record with no present value appears only in the grand total. Nodes are identified by their values position by position, matching the display paths the frontend sends. So equal values from different fields at the same position are one node, as in legacy. Each node reports the field its value came from (the first in row order if they differ) and whether any of its records continues deeper. A level measure filter applies only to records that have a node at that level. Row fields of different types, a flat layout, a measure axis and drill-through are not part of this rule (the bridge declines them by name). |

## 3c. Records (REQ-DET)

| Id | Requirement |
|---|---|
| REQ-DET-001 | Drill-through and detail list the E0 records whose path dimensions equal the node's typed path, with null-safe equality, so a NULL member selects the NULL group. Pages follow a total order: the requested sort keys (NULLS LAST), then every column ascending, so pages concatenate to the full list without overlap. Search is a case-insensitive substring over every column's text form and never matches NULL. |
| REQ-DET-002 | Filter options are the distinct values of a column over E0, ordered by value with NULL last, and paged. Search matches a case-insensitive substring of the value's text and never matches NULL. NULL is returned as null, never NaN. |

## 4. NULL and identity (REQ-NULL, REQ-ID)

| Id | Requirement |
|---|---|
| REQ-NULL-001 | SQL NULL stays NULL end to end: JSON `null`, never `NaN`, `""`, `"null"` or `"Grand Total"`. |
| REQ-NULL-002 | A NULL dimension value is its own group, distinct from the empty string and from the grand total. |
| REQ-NULL-003 | Row filters: membership (`in`, `not_in`) is set membership, so a NULL in the list selects the NULL group and `not_in` keeps NULL rows unless NULL is listed. Comparisons, `between` and string operators never match NULL. Case folding (`caseSensitive: false`) applies to `=`, `!=` and string operators on string columns; membership is always exact, because case variants are distinct groups. |
| REQ-NULL-004 | `is_empty` is true for NULL, and for a text value made only of spaces (or empty). `is_not_empty` is its negation. On non-text columns, `is_empty` is `is_null`. These are the custom-category UI's "empty" operators. Ordinary `is_null` and `is_not_null` stay exact. |
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
| REQ-BACK-004 | On every supported backend a query either returns exactly the oracle's rows or is refused before it runs with a typed `unsupported_query` error (`BackendCapabilityError`) that names the missing operations. A wrong value, a raw driver error or a partial result is a defect. DuckDB, the reference backend, never refuses. Certified locally on DuckDB, SQLite and Polars. |

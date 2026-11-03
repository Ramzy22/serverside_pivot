# Period Axis -- Native Widget Support (`dash_tanstack_pivot` v0.0.72)

> **Status**: design / implementation guide
> **Audience**: developer picking up the widget upgrade after the server-side `periodAxis` feature has shipped
> **Pre-requisite reading**: [`server_side_pivot_contract.md`](./server_side_pivot_contract.md), section "Period axis"

---

## 1. Executive summary

The server-side `PeriodAxis` feature is already complete: a `PeriodAxisExpander` rewrites `valConfigs` into one cell per `(measure, member)` pair, period-membership is encoded as 0/1 indicator columns in the prepared view, and saved-view round-trip works through `viewState.periodAxis`. The single remaining gap is **the user has no drag-and-drop way to activate it** -- they must either pass a Python kwarg or hand-edit a saved view JSON.

This document specifies the JS-side upgrade (`dash_tanstack_pivot` v0.0.71 -> v0.0.72) that closes that gap **without** changing the SQL contract, the performance model, or the Python data layer.

In one sentence: the widget grows a real `periodAxis` prop and a "Period" field-list chip, the server-side expander stays identical, and the workspace gains drag-and-drop period analysis with zero scan-cost regression.

---

## 2. Background and current state

### 2.1 What lives where today

| Concern | Owner | Code |
|---|---|---|
| Period-membership truth | `PeriodMembershipRegistry` (DuckDB) | `Frontend/helpers/services/period_membership.py` |
| Indicator-column SQL emission | `PeriodIndicatorColumnAugmenter` | `Frontend/helpers/services/period_pivot_columns.py` |
| `(measure, period)` cross-product expansion | `PeriodAxisExpander` | `Frontend/helpers/services/period_axis.py` |
| Saved-view round-trip | `sanitize_greeks_pivot_view_state` | `Frontend/helpers/callbacks/greeks_helper.py:1271` |
| Widget contract | `DashTanstackPivotPropContract` | `Frontend/helpers/services/pivot_prop_contract.py` |
| Widget itself | `DashTanstackPivot` v0.0.71 (JS) | external package |

### 2.2 What already works (do not break)

- One-scan aggregation: `SUM(measure * indicator)` per period in a single SELECT.
- Funnel ordering: `Period -> YTD -> MTD -> Today` in SQL output order.
- Saved-view persistence: `viewState.periodAxis` JSON survives save/load.
- The expander pre-rewrites `valConfigs`; the widget renders them as normal cells.

### 2.3 What is missing

- No `periodAxis` prop declared on the JS widget. Passing it at the top level raises `TypeError` (the bug we just fixed by removing it from `component_kwargs`).
- No drag-and-drop chip for "Period" in the field list.
- No header grouping; the user sees a flat row of `"PnL | YTD"`, `"PnL | MTD"`, ... columns instead of a banded `PnL` header with `YTD / MTD / Today` sub-headers.
- No way for the user to toggle individual members (e.g., hide `Today`) without editing JSON.

---

## 3. Goal

After v0.0.72 ships, the following user journey works end-to-end with no JSON editing:

1. User opens a Greeks tab.
2. A "Period" chip appears in the field-list alongside the regular dimensions.
3. User drags "Period" onto **Columns** (or **Rows**).
4. The widget emits a `periodAxisEvent` describing the drop.
5. Python callback rebuilds the pivot with `periodAxis.placement = "columns"`; the expander fans `valConfigs` into the cross-product; the widget re-renders.
6. The user sees `PnL` grouped under a banded "Period" header with sub-cells `Period | YTD | MTD | Today`.
7. User clicks a member chip in the header to toggle visibility. State persists in `viewState.periodAxis.members[].enabled`.
8. User saves a favorite. The favorite restores with periodAxis active.

Performance must remain at parity with today's indicator-multiplier path (within +/- 5 ms on the 1M-row bench).

---

## 4. Architecture overview

```
+----------------------------+         +---------------------------+
|  workspace UI              |  drag   |  DashTanstackPivot v0.0.72|
|  (greeks tab toolbar +     | -----> |  field list, drop targets,|
|  pivot canvas)             |         |  header bands             |
+-------------+--------------+         +-------------+-------------+
              |                                      |
              | periodAxisEvent                      | viewState round-trip
              v                                      v
+----------------------------+         +---------------------------+
|  Dash callback              |        |  viewState.periodAxis     |
|  cb_period_axis_event       |        |  (persistence envelope)   |
+-------------+--------------+         +-------------+-------------+
              |                                      |
              | rebuild trigger                      | sanitize on load
              v                                      v
+----------------------------------------------------+--------------+
|  build_greeks_pivot_component(period_axis=...)                    |
|  -> normalize_period_axis -> PeriodAxisExpander.expand(valConfigs)|
|  -> DashTanstackPivot(**kwargs with expanded valConfigs +         |
|                       periodAxis prop)                            |
+--------------------------------+----------------------------------+
                                 |
                                 | SQL emit (funnel-ordered)
                                 v
+-------------------------------------------------------------------+
|  PeriodIndicatorColumnAugmenter (1 scan, N indicator multiplies)  |
+-------------------------------------------------------------------+
```

**Invariant**: the widget never inspects indicator columns or rewrites `valConfigs`. The widget renders cells the server already prepared and decorates the layout per the `periodAxis` prop. **All SQL work stays server-side**.

---

## 5. Data contract

This section is the source of truth for the JSON exchange between Python and JS. Both sides must validate against these shapes.

### 5.1 `periodAxis` prop (Python <-> JS, persisted in `viewState`)

```typescript
type PeriodAxis = {
  enabled: boolean;                  // master switch; false = expander is a no-op
  placement: "rows" | "columns" | "none";
  labelField: string;                // header label of the synthetic axis, default "Period"
  members: PeriodAxisMember[];       // broadest-first order, must be non-empty if active
};

type PeriodAxisMember = {
  label: string;                     // display name shown in header sub-band
  scope: null | "YTD" | "MTD" | "Today";  // null = unscoped total
  enabled?: boolean;                 // default true; false hides the member without removing it
};
```

**Validation rules** (enforced server-side by `normalize_period_axis`, must be mirrored client-side):

- `members` is ordered broadest-first. A `null` scope (the total) must come first if present.
- Member `label`s are unique within the config (dedupe by label, keep first).
- Unknown `scope` strings are dropped with a warning.
- Empty `members` -> `is_active = false` regardless of `enabled`.
- `placement = "none"` -> `is_active = false`.
- The canonical default is `default_period_axis_config()` at `Frontend/helpers/services/period_axis.py:240`.

### 5.2 `valConfig` metadata extensions (set by the expander, read by JS)

After the expander runs, each scoped cell carries two new keys the widget must understand:

```typescript
type ValConfig = {
  field: string;          // synthetic field name, e.g. "PnL__period_YTD"
  agg: "formula";         // scoped cells are always formula cells
  expression: string;     // e.g. '"PnL" * "YTD"'
  label?: string;         // composed display label, e.g. "PnL | YTD"

  // --- expander-set metadata (NEW for v0.0.72) ---
  sourceField: string;    // "PnL" -- the original measure
  periodLabel: string;    // "YTD" -- maps to PeriodAxisMember.label
};
```

The widget identifies "this cell belongs to a period band" by the presence of both `sourceField` and `periodLabel`. Cells without those keys render normally (unchanged behaviour for non-periodAxis tabs).

### 5.3 `periodAxisEvent` (JS -> Python, new in v0.0.72)

Emitted whenever the user interacts with periodAxis through the UI:

```typescript
type PeriodAxisEvent = {
  type: "place" | "remove" | "toggleMember" | "reorder";
  placement?: "rows" | "columns" | "none";  // for "place" / "remove"
  memberLabel?: string;                     // for "toggleMember"
  memberEnabled?: boolean;                  // for "toggleMember"
  memberOrder?: string[];                   // for "reorder"
  // Always echo the resulting config for idempotent server rebuilds:
  nextConfig: PeriodAxis;
};
```

The server treats `nextConfig` as canonical -- it runs `normalize_period_axis` on it and rebuilds. The individual event fields are informational (logging, analytics).

---

## 6. JS implementation (`dash_tanstack_pivot` v0.0.72)

### 6.1 New declared prop

In the component's prop-types declaration, add:

```javascript
periodAxis: PropTypes.shape({
  enabled: PropTypes.bool,
  placement: PropTypes.oneOf(["rows", "columns", "none"]),
  labelField: PropTypes.string,
  members: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string.isRequired,
      scope: PropTypes.oneOf(["YTD", "MTD", "Today", null]),
      enabled: PropTypes.bool,
    })
  ),
}),
periodAxisEvent: PropTypes.shape({ /* see section 5.3 */ }),
```

Register `periodAxisEvent` as a Dash-emitted prop (analogous to `sortEvent` / `chartEvent`).

### 6.2 Field-list integration: the "Period" chip

The field-list panel renders one chip per item in `availableFieldList`. Add a special-case chip when `periodAxis` is present (even if disabled) -- it is **not** a member of `availableFieldList`; it is rendered separately:

```javascript
// In the field-list component:
const periodChip = props.periodAxis
  ? renderSpecialChip({
      label: props.periodAxis.labelField || "Period",
      icon: "calendar",
      kind: "periodAxis",                       // discriminator for drop handlers
      disabled: !props.periodAxis.enabled,
    })
  : null;
```

- The chip is draggable but never appears in `rowFields` / `colFields` directly. It is captured by drop handlers (section 6.3).
- A small "off" badge appears when `periodAxis.enabled === false`, signalling "drop me to activate".

### 6.3 Drop-target wiring

Both the Rows and Columns drop zones must distinguish a regular field drop from a periodAxis drop. Branch on `dragItem.kind`:

```javascript
function onDropOnRows(dragItem) {
  if (dragItem.kind === "periodAxis") {
    emitPeriodAxisEvent({
      type: "place",
      placement: "rows",
      nextConfig: { ...props.periodAxis, placement: "rows", enabled: true },
    });
    return;  // do NOT add to rowFields
  }
  // ... existing field drop logic
}
```

Symmetric handler for Columns. Dropping the chip back onto the field list emits `{type: "remove", placement: "none"}`.

### 6.4 Event emission

Use a single internal helper that always sets both the event prop and updates the widget's local `periodAxis` state optimistically (so the UI doesn't wait for the server round-trip to feel responsive):

```javascript
function emitPeriodAxisEvent(evt) {
  this.setState({ periodAxis: evt.nextConfig });  // optimistic
  this.props.setProps({ periodAxisEvent: evt });
}
```

### 6.5 Header grouping rendering

This is the visual upgrade. The header builder must recognise the period band and emit a two-row header for the cells that carry `periodLabel`.

Algorithm (pseudo-code):

```javascript
function buildHeaderRows(valConfigs, periodAxis) {
  if (!periodAxis || periodAxis.placement !== "columns") {
    return [buildFlatHeader(valConfigs)];  // unchanged path
  }

  // Group cells by sourceField, preserving order
  const groups = new Map();
  for (const cell of valConfigs) {
    const key = cell.sourceField || cell.field;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(cell);
  }

  const topRow = [];   // measure bands
  const bottomRow = [];  // period sub-headers
  for (const [measure, cells] of groups.entries()) {
    topRow.push({ label: measure, colSpan: cells.length });
    for (const cell of cells) {
      bottomRow.push({
        label: cell.periodLabel || cell.label || cell.field,
        cellField: cell.field,
      });
    }
  }

  return [topRow, bottomRow];
}
```

For `placement === "rows"`, mirror the same idea on the row axis (banded row group header per measure, sub-rows per member).

The bottom-row labels are clickable -- on click, emit `{type: "toggleMember", memberLabel, memberEnabled: !current}`. The cell column itself stays in the DOM but is hidden via CSS when `enabled === false`; this avoids a full re-layout on toggle.

### 6.6 `viewState` round-trip

The widget already reads/writes `viewState` opaquely. After v0.0.72, the widget MUST:

- On mount: if `props.periodAxis` is undefined but `props.viewState.periodAxis` exists, use the latter as the initial value (saved-view restore).
- On `periodAxisEvent`: write the new config back into `viewState.periodAxis` before triggering `saveViewTrigger`.

### 6.7 Module-level structure (suggested)

```
src/
  DashTanstackPivot.tsx
  axes/
    measureAxis/                 // existing
    periodAxis/                  // NEW
      PeriodAxisChip.tsx
      PeriodAxisDropHandler.ts
      PeriodAxisHeaderBuilder.ts
      PeriodAxisMemberToggle.tsx
      types.ts                   // mirrors section 5.1
      validate.ts                // mirrors normalize_period_axis
```

`validate.ts` is the JS twin of `Frontend/helpers/services/period_axis.py::normalize_period_axis`. Keep them strictly in sync; a CI check should diff their accepted shapes against a shared fixture file.

---

## 7. Python implementation (Frontend side)

### 7.1 Restore `periodAxis` to the widget contract

Once the widget declares the prop, undo the two safety removals from the v0.0.71 fix:

```python
# Frontend/helpers/services/pivot_prop_contract.py
PROPS = (
    ...
    "measureAxis",
    "periodAxis",          # re-add
    "numberGroupSeparator",
    ...
)

CORE_SCHEMA_PROPS = frozenset((
    ...
    "measureAxis",
    "periodAxis",          # re-add
    "customDimensions",
    ...
))

# Add the event prop to the validator's "extra any props":
VALIDATOR_EXTRA_ANY_PROPS = frozenset((
    "cellUpdate",
    "chartEvent",
    "editLifecycleEvent",
    "periodAxisEvent",     # NEW
    "id",
    ...
))
```

Then re-add the widget kwarg in `build_greeks_pivot_component`:

```python
# Frontend/helpers/callbacks/greeks_helper.py:~1902
component_kwargs = {
    ...
    "measureAxis": resolved_measure_axis,
    "periodAxis": period_axis_to_jsonable(resolved_period_axis),  # re-add
    ...
}
```

The regression test `TestPeriodAxisIsServerSideOnly` must be replaced by a new suite asserting that `periodAxis` IS passed and matches the resolved config -- see section 10.1.

### 7.2 Wire the `periodAxisEvent` callback

A new Dash callback intercepts `periodAxisEvent` and triggers a rebuild:

```python
# Frontend/pages/greeks/callback/callback.py

@app.callback(
    Output({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "viewState"),
    Output({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "valConfigs"),
    Output({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "periodAxis"),
    Input({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "periodAxisEvent"),
    State({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "viewState"),
    State({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "valConfigs"),
    prevent_initial_call=True,
)
def cb_period_axis_event(event, view_state, val_configs):
    if not isinstance(event, dict) or "nextConfig" not in event:
        raise PreventUpdate

    next_config = normalize_period_axis(event["nextConfig"])
    expanded = PeriodAxisExpander(next_config).expand(_strip_periodaxis_cells(val_configs))

    new_view_state = dict(view_state or {})
    new_view_state["periodAxis"] = period_axis_to_jsonable(next_config)

    return new_view_state, expanded, period_axis_to_jsonable(next_config)
```

`_strip_periodaxis_cells` is a small helper that removes existing scoped cells (identified by `periodLabel`) from `val_configs` before re-expansion, so toggling members or moving placement does not stack expansions.

```python
def _strip_periodaxis_cells(val_configs):
    """Return only the original (non-expander-generated) cells."""
    return [
        cell for cell in (val_configs or [])
        if isinstance(cell, dict) and "periodLabel" not in cell
    ]
```

### 7.3 Member visibility toggle

`PeriodAxisExpander.expand` already respects `member.enabled` (see `Frontend/helpers/services/period_axis.py`). The callback above already forwards the new config; no further server change needed beyond honoring the flag in the expander (verify the existing code path; add a test if missing).

### 7.4 Workspace toolbar quick-toggle (optional but recommended)

A single switch on the greeks tab toolbar that flips `periodAxis.enabled`. Pure UX sugar -- no new data contract:

```python
# pseudo-code
@app.callback(
    Output({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "periodAxis"),
    Input({"type": "greeks_period_axis_toggle", "index": MATCH}, "checked"),
    State({"type": GR_TANSTACK_PIVOT_ID, "index": MATCH}, "periodAxis"),
)
def cb_period_axis_quick_toggle(checked, current):
    current = current or period_axis_to_jsonable(default_period_axis_config())
    return {**current, "enabled": bool(checked)}
```

---

## 8. Migration and backward compatibility

### 8.1 Old saved views (v0.0.71 era)

Saved views from before v0.0.72 fall into two categories:

| Saved-view shape | v0.0.72 behaviour |
|---|---|
| `viewState.periodAxis` present and active | restored, drag-state UI reflects active placement |
| `viewState.periodAxis` absent or `enabled = false` | chip appears in field list but is inactive, valConfigs unaffected |
| Old hand-edited views with literal expanded cells but no `periodAxis` config | rendered as-is; the chip shows "off"; user can re-activate to refresh |

No migration script required. `sanitize_greeks_pivot_view_state` already validates and normalises every load.

### 8.2 Mixed-version workspaces

If a workspace runs v0.0.71 widget (e.g., a cached browser) against a v0.0.72 server, the server-side passes `periodAxis` as a kwarg and the widget rejects it.

**Mitigation**: gate the kwarg behind a runtime feature flag for one release:

```python
PIVOT_NATIVE_PERIOD_AXIS = os.environ.get("OMEGA_PIVOT_NATIVE_PERIOD_AXIS", "1") == "1"

# in build_greeks_pivot_component:
if PIVOT_NATIVE_PERIOD_AXIS:
    component_kwargs["periodAxis"] = period_axis_to_jsonable(resolved_period_axis)
```

Operators flip the env var to `0` if they need to fall back. Remove the flag in v0.0.73.

---

## 9. Performance considerations

### 9.1 What does not change (the hot path)

- **SQL emission**: still one scan with `SUM(measure * indicator)` per period.
- **Indicator materialisation**: still one CASE per period in the prepared view's projection.
- **Expander cost**: pure dict transform, O(N_measures x N_members) Python-side, sub-millisecond at typical sizes.

### 9.2 What might change (the warm path)

- **Header rendering**: building two-row banded headers is O(N_columns) JS work. On a 32-column pivot (8 measures x 4 periods), the cost is negligible. On a 1000-column pivot (extreme), profile before shipping; if needed, virtualise by measure-band.
- **Drag-and-drop re-render**: a single periodAxis change triggers one full pivot rebuild. Equivalent in cost to changing a measureAxis placement today. Bench at 1M rows showed ~45 ms for an active periodAxis rebuild; that is the user-perceived latency from drop to redraw.

### 9.3 Lazy indicator materialisation (recommended companion change)

`PeriodIndicatorColumnAugmenter` currently emits all four indicators unconditionally. With drag-and-drop UX, users will toggle members frequently. Add a member-aware emitter that only materialises the indicators the active config references:

```python
class PeriodIndicatorColumnAugmenter:
    def __init__(self, ..., active_member_scopes: Iterable[str] | None = None):
        self._active_scopes = (
            None if active_member_scopes is None
            else frozenset(s for s in active_member_scopes if s)
        )

    def emit_columns(self, schema):
        for policy in self._registry.policies():
            if self._active_scopes is not None and policy.label not in self._active_scopes:
                continue
            yield policy.indicator_column(schema, self._anchor)
```

Wire it in `build_greeks_pivot_component` from `resolved_period_axis.active_scopes()`. Bench expectation: ~10-15% scan-time reduction when the user has disabled members.

---

## 10. Testing plan

### 10.1 Python unit tests

Add to `Frontend/test/unit/test_greeks_pivot_adapter.py`:

```python
class TestPeriodAxisIsNativeWidgetProp:
    """v0.0.72 wires periodAxis as a real widget kwarg."""

    def test_active_period_axis_passes_through_as_kwarg(self, monkeypatch):
        ...
        assert "periodAxis" in component.kwargs
        assert component.kwargs["periodAxis"]["placement"] == "columns"

    def test_inactive_period_axis_passes_none_or_disabled(self, monkeypatch):
        ...
        carried = component.kwargs.get("periodAxis")
        assert carried is None or carried.get("enabled") is False
```

This replaces the v0.0.71-era `TestPeriodAxisIsServerSideOnly` regression guard. Delete the old class once v0.0.72 ships.

Add a callback test for the round-trip:

```python
def test_cb_period_axis_event_strips_and_re_expands(monkeypatch):
    event = {
        "type": "place",
        "placement": "rows",
        "nextConfig": {
            "enabled": True,
            "placement": "rows",
            "members": [{"label": "Today", "scope": "Today"}],
        },
    }
    val_configs = [
        {"field": "PnL", "agg": "sum"},
        {"field": "PnL__period_YTD", "agg": "formula", "periodLabel": "YTD", "sourceField": "PnL"},
    ]
    new_state, new_vals, new_axis = cb_period_axis_event(event, {}, val_configs)
    # Old scoped cell stripped, new one for Today emitted
    assert all(cell.get("periodLabel") in (None, "Today") for cell in new_vals)
    assert new_axis["placement"] == "rows"
```

### 10.2 JS unit tests (Storybook + Jest)

- `PeriodAxisChip` renders with correct disabled state.
- `PeriodAxisDropHandler.onDropOnRows` emits the right event shape.
- `PeriodAxisHeaderBuilder.buildHeaderRows` produces correct rowspan/colspan for a 2x4 layout, falls back to flat header when `placement === "none"`.
- `validate.ts` parses every fixture from `tests/fixtures/periodAxis/*.json` identically to the Python `normalize_period_axis` golden output. (Shared fixture set in `Frontend/test/fixtures/period_axis/` -- new directory.)

### 10.3 Integration test (Playwright)

```
test("drag Period chip onto Columns activates the axis", async ({ page }) => {
  await openGreeksTab(page, "Rates");
  await dragField(page, "Period", "ColumnsDropZone");
  await expect(page.locator(".pivot-header-band")).toContainText("Period");
  await expect(page.locator(".pivot-header-subband")).toContainText(["Period","YTD","MTD","Today"]);
});

test("toggle Today member hides its column", async ({ page }) => {
  ...
  await page.click("text=Today");  // sub-header chip
  await expect(page.locator("[data-period=Today]")).toBeHidden();
});
```

### 10.4 Performance benchmarks

Re-run `Frontend/dev_tools/bench_period_augmentation.py` against v0.0.72-mode builds:

- Add a new `NativeWidgetPeriodAxisScenario` that constructs the component with `periodAxis` as a real kwarg.
- Assert median SQL time within +/- 5 ms of `PeriodAxisCompositionScenario`.
- Add a new metric: render-to-paint (JS-side) for the 1M-row case. Target: < 60 ms p95 on drop-to-redraw.

---

## 11. Rollout plan

| Phase | Duration | Action | Rollback |
|---|---|---|---|
| 0 | -- | Today: server-side feature shipped, widget rejects prop, fix in place | n/a |
| 1 | 1 sprint | Ship v0.0.72 to internal beta; feature-flag `OMEGA_PIVOT_NATIVE_PERIOD_AXIS=1`; both server and widget understand the new prop | set flag to `0` |
| 2 | 1 sprint | Roll to all users; monitor `periodAxisEvent` callback latency and any saved-view restore errors | set flag to `0`, redeploy |
| 3 | next minor | Remove the feature flag; delete legacy expander-only test guards; remove v0.0.71 compatibility comments | revert that PR |
| 4 | later | Implement lazy indicator materialisation (section 9.3) | feature-flag the augmenter behavior |

Roll forward incrementally. At no point is the server-side expander deactivated -- it is the source of truth even after the widget ships.

---

## 12. Open questions

1. **Hierarchical period drilldown**: should clicking the "Period" total cell expand to show YTD/MTD/Today inline (tree behaviour) instead of as siblings? Out of scope for v0.0.72; consider for v0.0.73 alongside `treeConfig` integration.

2. **Member set per measure**: should `Increment` (a weighted-avg measure) expose a different member set than `PnL`? Today both expose the same four members; user might want `Increment` to only show `Period` since the weighted-avg-of-indicator-product is semantically lossy. Decide before v0.0.72 ships; if yes, add `PeriodAxisMember.excludeMeasures: string[]`.

3. **Custom calendars**: WTD, QTD, fiscal-YTD. Adding members to the canonical factory is a one-line change once `PeriodMembershipRegistry` learns the policy. Plan to ship in v0.0.73 with the lazy-indicator work.

4. **Mobile / narrow viewport**: banded headers consume vertical space. Should we auto-collapse to flat when viewport width < threshold? Out of scope for v0.0.72.

---

## 13. Glossary

| Term | Definition |
|---|---|
| **Period axis** | Server-side virtual dimension whose members are period scopes (Today/MTD/YTD/Total) |
| **Member** | One value of the period axis; backed by either no indicator (Period total) or one 0/1 indicator column |
| **Scope** | The temporal predicate of a non-total member: `Today`, `MTD`, `YTD` |
| **Expander** | `PeriodAxisExpander` -- the server-side class that rewrites `valConfigs` into the `(measure, member)` cross-product |
| **Indicator column** | A 0/1 BIGINT column materialised in the prepared view, e.g. `Today = CASE WHEN ref_date1 = period_anchor THEN 1 ELSE 0 END` |
| **Funnel ordering** | Broadest-first member order in SQL output: `Period -> YTD -> MTD -> Today`, matching natural reading order |
| **Banded header** | Two-row pivot column header where the top row groups columns by measure and the bottom row labels each member |

---

## 14. References

- Server-side feature implementation: `Frontend/helpers/services/period_axis.py`
- Indicator augmenter: `Frontend/helpers/services/period_pivot_columns.py`
- Wire-up in component builder: `Frontend/helpers/callbacks/greeks_helper.py:1691`
- Saved-view sanitiser: `Frontend/helpers/callbacks/greeks_helper.py:1271`
- Contract: `Frontend/helpers/services/pivot_prop_contract.py`
- Widget prop documentation: `Frontend/docs/server_side_pivot_contract.md`
- Benchmark suite: `Frontend/dev_tools/bench_period_augmentation.py`
- Existing v0.0.71 regression guard: `Frontend/test/unit/test_greeks_pivot_adapter.py::TestPeriodAxisIsServerSideOnly`
- Server-side expander unit tests: `Frontend/test/unit/test_period_axis.py`

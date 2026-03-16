# serverside-pivot

An enterprise-grade pivot table Dash component powered by TanStack Table v8 and a high-performance Ibis backend. Add a fully interactive, server-side pivot table to any Dash application in under 10 lines of code — no JavaScript, no database configuration, no performance tuning required.

---

## Installation

```bash
pip install serverside-pivot
```

For Redis-backed caching in production:

```bash
pip install "serverside-pivot[redis]"
```

---

## 10-line Quickstart

```python
import pyarrow as pa
from dash import Dash, html
from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRuntimeService, SessionRequestGate, register_dash_pivot_transport_callback

app = Dash(__name__)
adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow("sales", pa.Table.from_pydict({"region": ["North", "South"], "sales": [100, 200]}))
service = PivotRuntimeService(adapter_getter=lambda: adapter, session_gate=SessionRequestGate())
app.layout = html.Div([DashTanstackPivot(id="pivot", table="sales", rowFields=["region"], valConfigs=[{"field": "sales", "agg": "sum"}])])
register_dash_pivot_transport_callback(app, lambda: service, pivot_id="pivot")
if __name__ == "__main__":
    app.run(debug=True)
```

---

## Props Reference

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `id` | `string` | — | Required. Unique Dash component ID. Must be distinct per instance in a multi-instance layout. |
| `table` | `string` | — | Backend table name to query. All requests from this instance are scoped to this table. |
| `rowFields` | `list` | `[]` | Fields used as row dimensions (hierarchy levels). |
| `colFields` | `list` | `[]` | Fields used as column dimensions (cross-tab pivot columns). |
| `valConfigs` | `list of dict` | `[]` | Measure definitions. Each dict has `field`, `agg` (`sum`, `avg`, `count`, `min`, `max`), and optional `alias`. |
| `filters` | `dict` | `{}` | Active filter state keyed by field name. |
| `filterOptions` | `dict` | `{}` | Available filter values for each filterable field, populated by the backend. |
| `sorting` | `list` | `[]` | Active sort state as a list of `{id, desc}` objects. |
| `expanded` | `dict or bool` | `{}` | Row expansion state. Keys are stringified row paths. `true` means expand all. |
| `columns` | `list` | `[]` | Column definitions payload from backend. Populated by transport callback. |
| `data` | `list of dict` | `[]` | Row data payload from backend. Populated by transport callback. |
| `dataOffset` | `number` | `0` | Virtual scroll offset of the first row in `data`. |
| `dataVersion` | `number` | `0` | Monotonic version counter to force data refresh. |
| `rowCount` | `number` | `0` | Total number of logical rows for virtual scroll sizing. |
| `viewport` | `dict` | `{}` | Current visible window sent to the backend. Contains `start`, `end`, `session_id`, `client_instance`, `window_seq`, `state_epoch`, `abort_generation`, `intent`. |
| `showRowTotals` | `boolean` | `false` | Show row-level subtotal rows in the hierarchy. |
| `showColTotals` | `boolean` | `true` | Show column-level total column at the right edge. |
| `grandTotalPosition` | `'top' or 'bottom'` | `'bottom'` | Position of the grand total row. |
| `serverSide` | `boolean` | `true` | Enable server-side data fetching via the transport callback. |
| `columnPinning` | `dict` | `{}` | Column pinning state. Keys `left` and `right` hold lists of column IDs. |
| `columnVisibility` | `dict` | `{}` | Column visibility state. Keys are column IDs, values are booleans. |
| `drillEndpoint` | `string` | `""` | REST URL for the drill-through data endpoint. |
| `drillThrough` | `dict` | `{}` | Current drill-through request payload (set by right-click context menu). |
| `availableFieldList` | `list of string` | `[]` | Full list of available fields for the field zone UI sidebar. |
| `sortOptions` | `dict` | `{}` | Sort configuration options (`naturalSort`, `caseSensitive`, `columnOptions`). |
| `sortLock` | `boolean` | `false` | Lock the sort state to prevent user interaction. |
| `sortEvent` | `dict` | `{}` | Sort event emitted by a column header click. |
| `conditionalFormatting` | `list of dict` | `[]` | Conditional formatting rules applied to cell rendering. |
| `cellUpdate` | `dict` | `{}` | Single cell edit payload emitted by an inline edit. |
| `cellUpdates` | `list of dict` | `[]` | Batch cell edit payloads. |
| `validationRules` | `dict` | `{}` | Field-level validation rules for inline editing. |
| `rowMove` | `dict` | `{}` | Row drag-and-drop move event payload. |
| `rowPinning` | `dict` | `{}` | Row pinning state. Keys `top` and `bottom` hold lists of row IDs. |
| `rowPinned` | `dict` | `{}` | Row pinned event emitted by a user action. |
| `columnPinned` | `dict` | `{}` | Column pinned event emitted by a user action. |
| `pinningOptions` | `dict` | `{}` | Pinning constraints: `maxPinnedLeft`, `maxPinnedRight`, `suppressMovable`, `lockPinned`. |
| `pinningPresets` | `list of dict` | `[]` | Named pinning presets. Each dict has `name` and `config`. |
| `reset` | `any` | `null` | Set to any truthy value to reset view state (column sizing, visibility, pinning). |
| `viewState` | `dict` | `{}` | Full serialized view state for save/restore. |
| `savedView` | `dict` | `{}` | Restored view state payload loaded from persistence. |
| `persistence` | `bool, str, or number` | `false` | Dash persistence key for view state. |
| `persistence_type` | `'local', 'session', 'memory'` | `'local'` | Storage medium for Dash persistence. |

---

## Multi-instance Safety Contract

`serverside-pivot` is designed to run **multiple pivot instances in a single Dash layout** with full state isolation. This section defines the guarantees and required wiring.

### Identity Keys

Every instance requires three distinct identity values:

| Key | Where | Purpose |
|-----|-------|---------|
| `id` | `DashTanstackPivot(id=...)` | Dash component ID — must be unique per layout |
| `session_id` | Emitted in `viewport` prop | Per-page-load session identifier — scopes gate state to one browser tab |
| `client_instance` | Emitted in `viewport` prop | Per-mount instance stamp — resets on re-mount to prevent cross-instance stale poisoning |

The `session_id` and `client_instance` values are generated automatically by the frontend on mount. **You must use distinct `id` and `table` values for each instance in Python.** The gate uses `(session_id, client_instance)` as a composite key so two pivot grids sharing a session cannot interfere with each other's request sequences.

### Table-scoped Requests (table-scoped)

All backend requests carry a `table` field that matches the `table` prop set at component creation. The `PivotRuntimeService` dispatches each request to the correct data source by table name. Two instances pointing at different tables produce entirely independent query plans and result sets.

### Filter and Sort Isolation

Each instance maintains its own `filters`, `sorting`, `expanded`, and `viewport` Dash props. These props are never shared between instances. Changing the filter on `pivot-a` does not touch the state of `pivot-b`.

### Interleaved Request Concurrency

The `SessionRequestGate` tracks `(session_id, client_instance, state_epoch, window_seq, abort_generation)` independently per instance. When requests from two instances arrive interleaved:

- Each instance's gate checks its own sequence independently.
- A stale response for instance A is silently dropped; instance B's current response is unaffected.
- An `abort_generation` bump on instance A rejects only A's in-flight requests.

### Two-instance Wiring Example

See [`examples/example_dash_sql_multi_instance.py`](examples/example_dash_sql_multi_instance.py) for a complete working example with two pivot instances wired to separate tables with isolated callbacks.

---

## Running the Examples

```bash
# Basic single-instance DataFrame example
python examples/example_dash_basic.py

# Hierarchical row-expansion example
python examples/example_dash_hierarchical.py

# Two-instance SQL-connected isolation example
python examples/example_dash_sql_multi_instance.py
```

---

## Testing

```bash
# Contract tests for examples and multi-instance isolation
python -m pytest tests/test_docs_examples_contract.py tests/test_multi_instance_isolation.py -v

# Full runtime and session gate tests
python -m pytest tests/test_runtime_service.py tests/test_session_request_gate.py tests/test_dash_runtime_callbacks.py -v
```

---

## License

MIT (c) 2025 Ramzy22

"""Live semantics (REQ-LIVE-001)."""

from __future__ import annotations

import pyarrow as pa

from pivot_engine.runtime.subscription_service import RuntimeSubscriptionService
from pivot_engine.tanstack_adapter import create_tanstack_adapter


def test_value_refresh_keeps_row_structure_even_when_sort_order_changes():  # REQ-LIVE-001
    adapter = create_tanstack_adapter()
    adapter.controller.load_data_from_arrow("t", pa.table({"id": [1, 2, 3], "r": ["A", "B", "C"], "v": [1, 2, 30]}))
    service = RuntimeSubscriptionService(adapter_getter=lambda: adapter)
    state = {
        "rowFields": ["r"],
        "valConfigs": [{"id": "v_sum", "field": "v", "agg": "sum"}],
        "sorting": [{"id": "v_sum", "desc": True}],
        "expanded": {},
    }
    snap = service.handle_command(table="t", action="subscribe", state=state, payload={"startRow": 0, "endRow": 50})
    assert [r["_path"] for r in snap["rows"]] == ["C", "B", "A"]

    adapter.push_stream_changes("t", [{"id": 1, "v": 100}], mode="update", key_fields=["id"])  # A now sorts first
    patch = service.handle_command(
        table="t", action="poll", state=state,
        payload={"subscriptionId": snap["subscriptionId"], "ackSeq": snap["seq"]},
    )
    assert patch["type"] == "PATCH"
    assert patch["rowOps"] == []  # structure stable: no reorder on a value refresh
    assert [(c["rowId"], c["value"]) for c in patch["cellPatches"]] == [("A", 100)]
    assert patch["grandTotalPatch"]["v_sum"]["value"] == 132

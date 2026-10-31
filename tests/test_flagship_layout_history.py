"""Negative controls for layout-history sequencing and request semantics."""

import copy
import pytest
from tests.flagship_axis_swap import MEASURES
from tests.flagship_layout_history import HISTORY_STATES, assert_history_sequence, history_snapshot


def sequence():
    return [{"transposed": transposed, "measures": list(MEASURES[:count]), "redoEnabled": redo,
             "requestId": str(index), "layoutRevision": f"{transposed}:{count}",
             "columnIds": [f"{transposed}:{count}"], "visibleRows": [str(transposed)]}
            for index, (transposed, count, redo) in enumerate(HISTORY_STATES)]


@pytest.mark.parametrize("damage", [None, "short", "reorder", "redo-not-cleared", "measure", "request", "revision", "columns", "rows"])
def test_history_sequence_requires_full_round_trip_and_cleared_branch(damage):
    states = sequence()
    if damage == "short": states.pop()
    elif damage == "reorder": states[3], states[4] = states[4], states[3]
    elif damage == "redo-not-cleared": states[-1]["redoEnabled"] = True
    elif damage == "measure": states[1]["measures"][-1] = "unknown"
    elif damage == "request": states[3]["requestId"] = states[1]["requestId"]
    elif damage == "revision": states[4]["layoutRevision"] = states[1]["layoutRevision"]
    elif damage == "columns": states[4]["columnIds"] = ["stale"]
    elif damage == "rows": states[3]["visibleRows"] = ["stale"]
    if damage is None:
        assert_history_sequence(states)
    else:
        with pytest.raises(AssertionError):
            assert_history_sequence(states)


@pytest.mark.parametrize("damage", [None, "wrong-field", "wrong-aggregate", "duplicate-request", "redo-enabled", "hidden", "no-snapshot"])
def test_history_snapshot_requires_matching_request_values_and_redo_state(monkeypatch, damage):
    import tests.flagship_layout_history as history
    from tests import test_flagship_certification as certification

    snapshot = {"requestId": "request"}
    request = {"phase": "request", "requestId": "request",
               "valueAggregations": [{"field": field, "agg": "sum"} for field in MEASURES[:2]]}
    if damage == "wrong-field": request["valueAggregations"][1]["field"] = "unknown"
    elif damage == "wrong-aggregate": request["valueAggregations"][0]["agg"] = "avg"

    class Button:
        def is_displayed(self): return damage != "hidden"
        def is_enabled(self): return damage == "redo-enabled"

    class Driver:
        def execute_script(self, _): return [request, copy.deepcopy(request)] if damage == "duplicate-request" else [request]

    monkeypatch.setattr(history, "axis_snapshot", lambda *args, **kwargs: False if damage == "no-snapshot" else snapshot)
    monkeypatch.setattr(certification, "_workspace_elements", lambda *args: [Button()])
    observed = history_snapshot(Driver(), 1, transposed=False, count=2, redo_enabled=False)
    assert bool(observed) is (damage is None)

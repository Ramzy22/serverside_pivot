"""Consumer-contract snapshots: end-to-end adapter answers frozen as golden values.

At the M4 cutover each snapshotted case was proven equal between the semantic
core and the legacy engines (the M2 shadow-parity tests).  Legacy is now
deleted, and the frozen answer is the consumer contract the core must keep.
A deliberate change of the contract re-records it:

    PIVOT_CONTRACT_UPDATE=1 python -m pytest -p no:xdist tests/core/test_core_bridge.py

and the diff of ``consumer_contract.json`` is reviewed like code.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any

SNAPSHOTS = pathlib.Path(__file__).with_name("consumer_contract.json")


def _normalized(value: Any) -> Any:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def assert_contract(request: Any, value: Any) -> None:
    key = request.node.nodeid.split("::", 1)[1]
    got = _normalized(value)
    stored = json.loads(SNAPSHOTS.read_text(encoding="utf-8")) if SNAPSHOTS.exists() else {}
    if os.environ.get("PIVOT_CONTRACT_UPDATE") == "1":
        stored[key] = got
        SNAPSHOTS.write_text(json.dumps(stored, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        return
    assert key in stored, f"no consumer-contract snapshot for {key}; record it with PIVOT_CONTRACT_UPDATE=1"
    assert got == stored[key], (got, stored[key])

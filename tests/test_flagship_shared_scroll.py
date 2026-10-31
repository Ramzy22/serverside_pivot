"""Installed HTTP assertions for the same wide fixture used by browser scrolling."""

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
from pathlib import Path

import pyarrow as pa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import PivotRuntimeService, RuntimeDatasetAuthorizer, register_runtime_http_transport


FIELDS = ("ventes_\u20ac", "unit\u00e9s", "\u00e9cart")


@dataclass
class Viewer:
    id: str = "scroll-viewer"
    role: str = "viewer"
    attributes: dict = field(default_factory=dict)


@contextmanager
def scroll_client():
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    try:
        source = Path(__file__).with_name("data") / "flagship_scroll_rows.json"
        rows = json.loads(source.read_text(encoding="utf-8"))["rows"]
        assert len(rows) == 120 * 24
        adapter.controller.load_data_from_arrow("scroll_shared", pa.Table.from_pylist(rows))
        service = PivotRuntimeService(adapter_getter=lambda: adapter)
        app = FastAPI()
        register_runtime_http_transport(
            app, lambda: service, current_user_dependency=lambda: Viewer(),
            dataset_authorizer=RuntimeDatasetAuthorizer({
                "scroll_shared": {"roles": {"viewer"}, "operations": {"read"},
                                  "rowSecurity": {"mode": "global"}},
            }), inline_payload_bytes=5_000_000,
        )
        with TestClient(app) as client:
            yield client
    finally:
        adapter.controller.close()


def window(client, sequence, *, start=0, end=4, col_start=0, col_end=2, needs_schema=True):
    request_id = f"scroll-{sequence}"
    response = client.post("/v1/runtime", json={
        "schemaVersion": 1, "kind": "data", "requestId": request_id,
        "table": "scroll_shared", "sessionId": "scroll-session", "clientInstance": "scroll-client",
        "stateEpoch": 1, "windowSeq": sequence,
        "state": {"rowFields": ["region", "account"], "colFields": ["period"],
                  "valConfigs": [{"field": value, "agg": "sum"} for value in FIELDS],
                  "filters": {"scope": {"operator": "AND", "conditions": [{"type": "eq", "value": "Included"}]}},
                  "expanded": {}, "sorting": [], "showRowTotals": True, "showColTotals": True},
        "payload": {"intent": "viewport", "start": start, "end": end,
                    "colStart": col_start, "colEnd": col_end, "needsColSchema": needs_schema,
                    "includeGrandTotal": True},
    })
    assert response.status_code == 200, response.text
    envelope = response.json()
    assert (envelope["requestId"], envelope["clientInstance"], envelope["stateEpoch"], envelope["windowSeq"]) == (
        request_id, "scroll-client", 1, sequence)
    assert envelope["status"] == "data", envelope
    payload = envelope["payload"]
    assert payload["dataOffset"] == start
    assert payload["layoutRevision"]
    if needs_schema:
        assert payload["layoutRevision"] == payload["colSchema"]["layoutRevision"]
    return payload


def assert_rows(payload, start, end, period):
    rows = [row for row in payload["data"] if row["_id"] != "Grand Total"]
    expected_ids = [f"R{index:03d}" for index in range(start, end + 1)]
    assert [row["_id"] for row in rows] == expected_ids
    assert len({row["_path"] for row in rows}) == len(rows)
    for index, row in zip(range(start, end + 1), rows):
        expected = (index * 100 + period + .25, 1, float(index))
        assert [row[f"P{period:02d}_{measure}_sum"] for measure in FIELDS] == list(expected)
    grand, = [row for row in payload["data"] if row["_id"] == "Grand Total"]
    assert [grand[f"P{period:02d}_{measure}_sum"] for measure in FIELDS] == [714000 + 120 * (period + .25), 120, 7140]


def test_vertical_window_round_trip():
    with scroll_client() as client:
        first = window(client, 1)
        later = window(client, 2, start=100, end=119)
        restored = window(client, 3)
    for response, start, end in ((first, 0, 4), (later, 100, 119), (restored, 0, 4)):
        assert_rows(response, start, end, 23)
        assert response["rowCount"] == 121
    assert restored["data"] == first["data"]


def test_horizontal_window_round_trip():
    with scroll_client() as client:
        first = window(client, 1)
        last_period = window(client, 2, col_start=69, col_end=71)
        totals = window(client, 3, col_start=72, col_end=74)
        restored = window(client, 4)
    for response, period in ((first, 23), (last_period, 0), (restored, 23)):
        assert_rows(response, 0, 4, period)
    for response, start, period in ((first, 0, 23), (last_period, 69, 0), (totals, 72, None)):
        columns = [column for column in response["colSchema"]["columns"] if column]
        expected = [f"P{period:02d}_{measure}_sum" if period is not None else f"__RowTotal__{measure}_sum"
                    for measure in FIELDS]
        assert [(column["index"], column["id"]) for column in columns] == list(enumerate(expected, start))
        for row in response["data"]:
            assert {key for key in row if key.endswith("_sum")} == set(expected)
    for index, row in enumerate(row for row in totals["data"] if row["_id"] != "Grand Total"):
        assert [row[f"__RowTotal__{measure}_sum"] for measure in FIELDS] == [2400 * index + 282, 24, 24 * index]
    grand, = [row for row in totals["data"] if row["_id"] == "Grand Total"]
    assert [grand[f"__RowTotal__{measure}_sum"] for measure in FIELDS] == [17169840, 2880, 171360]
    assert len({response["layoutRevision"] for response in (first, last_period, totals, restored)}) == 1
    assert restored["data"] == first["data"]
    assert restored["colSchema"] == first["colSchema"]


def test_cached_row_block_preserves_layout_revision():
    with scroll_client() as client:
        initial = window(client, 1, end=99)
        cached_slice = window(client, 2, start=5, end=9, needs_schema=False)
        restored = window(client, 3, start=5, end=9, needs_schema=False)
    for response in (cached_slice, restored):
        assert_rows(response, 5, 9, 23)
        assert response["layoutRevision"] == initial["layoutRevision"]
        assert response["colSchema"] is None

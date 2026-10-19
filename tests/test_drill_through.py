"""
Tests for the /api/drill-through REST endpoint (DRILL-03 through DRILL-06).
They use the Dash test client (app.server.test_client()) to hit the endpoint directly.

Coverage:
  DRILL-03 - /api/drill-through endpoint accessible via HTTP GET
  DRILL-04 - Server-side pagination (page/page_size params)
  DRILL-05 - sort_col/sort_dir and filter params applied
  DRILL-06 - Pivot coordinate filters (row_path / row_fields) applied by DuckDB
"""
import sys
import os

# The conftest.py at the repo root already inserts pivot_engine/ onto sys.path.
# For the Dash app, we also need dash_presentation/ on the path.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dash_presentation')))

import pytest
import pyarrow as pa
import pyarrow.compute as pc


DRILL_TABLE = "nasdaq_trader_demo_history"
ROOT_FIELD = "desk"
ROOT_VALUE = "Consumer Tactical"
CHILD_FIELD = "strategy"
CHILD_VALUE = "Event Driven"
SORT_FIELD = "day_pnl"
TEXT_FILTER = "TSLA"


# ---------------------------------------------------------------------------
# Fixture: Flask test client backed by deterministic in-memory Arrow data
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """Exercise the real route and controller without optional demo files."""
    import app as app_module

    table = pa.table({
        'desk': [ROOT_VALUE] * 12 + ['Other Desk', 'Other Desk'],
        'strategy': [CHILD_VALUE] * 6 + ['Momentum'] * 6 + [CHILD_VALUE, 'Momentum'],
        'day_pnl': [30, -20, 100, 10, -50, 70, 0, 40, 90, 20, 60, 80, 500, -500],
        'symbol': ['TSLA', 'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META',
                   'TSLA', 'GOOG', 'IBM', 'ORCL', 'TSLA', 'INTC', 'TSLA', 'AMD'],
    })

    def load_test_data(adapter):
        adapter.controller.load_data_from_arrow(DRILL_TABLE, table)
        assert table.filter(pc.equal(table[ROOT_FIELD], ROOT_VALUE)).num_rows >= 11

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(app_module, '_adapter', None)
        patch.setattr(app_module, 'load_initial_data', load_test_data)
        patch.setitem(app_module.app.server.config, 'TESTING', True)
        adapter = app_module.get_adapter()
        try:
            with app_module.app.server.test_client() as c:
                yield c
        finally:
            adapter.controller.close()


# ---------------------------------------------------------------------------
# DRILL-03: Endpoint returns HTTP 200 with a "rows" key
# ---------------------------------------------------------------------------

def test_endpoint_returns_rows(client):
    """GET /api/drill-through returns the requested root rows."""
    resp = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "page": 0,
            "page_size": 10,
        },
    )
    assert resp.status_code == 200, (
        f"Expected 200, got {resp.status_code}. "
        "Route /api/drill-through must return successfully."
    )
    data = resp.get_json()
    assert data is not None, "Response body is not JSON"
    assert 'rows' in data, f"'rows' key missing from response: {data.keys()}"
    assert isinstance(data['rows'], list), f"'rows' should be a list, got {type(data['rows'])}"
    assert len(data['rows']) == 10
    assert all(row[ROOT_FIELD] == ROOT_VALUE for row in data['rows'])


# ---------------------------------------------------------------------------
# DRILL-04: Pagination — page=0 and page=1 return non-overlapping row sets
# ---------------------------------------------------------------------------

def test_pagination(client):
    """Sorted five-row pages contain the expected non-overlapping records."""
    page_size = 5
    resp0 = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "page": 0,
            "page_size": page_size,
            "sort_col": SORT_FIELD,
            "sort_dir": "asc",
        },
    )
    resp1 = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "page": 1,
            "page_size": page_size,
            "sort_col": SORT_FIELD,
            "sort_dir": "asc",
        },
    )
    assert resp0.status_code == 200, f"page=0 got {resp0.status_code}"
    assert resp1.status_code == 200, f"page=1 got {resp1.status_code}"

    rows0 = resp0.get_json()['rows']
    rows1 = resp1.get_json()['rows']

    assert len(rows0) <= page_size, f"page 0 returned {len(rows0)} rows > page_size {page_size}"
    assert len(rows1) <= page_size, f"page 1 returned {len(rows1)} rows > page_size {page_size}"

    # Pages must be non-overlapping (at least one field value differs between pages)
    assert rows0 != rows1, (
        "page=0 and page=1 returned identical rows — pagination is not working"
    )
    assert [row[SORT_FIELD] for row in rows0] == [-50, -20, 0, 10, 20]
    assert [row[SORT_FIELD] for row in rows1] == [30, 40, 60, 70, 80]


# ---------------------------------------------------------------------------
# DRILL-05: sort_col / sort_dir and text filter params accepted
# ---------------------------------------------------------------------------

def test_sort_and_filter(client):
    """Sorting orders the root rows; text filtering returns every TSLA row."""
    # Check exact descending values so ignoring or reversing the sort fails.
    resp_sort = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "sort_col": SORT_FIELD,
            "sort_dir": "desc",
            "page": 0,
            "page_size": 10,
        },
    )
    assert resp_sort.status_code == 200, f"sort request got {resp_sort.status_code}"
    sort_rows = resp_sort.get_json()['rows']
    assert isinstance(sort_rows, list), "'rows' must be a list"
    assert [row[SORT_FIELD] for row in sort_rows] == [100, 90, 80, 70, 60, 40, 30, 20, 10, 0]

    # Filter check: filter=TSLA should return rows containing "TSLA"
    resp_filter = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": "",
            "row_fields": ROOT_FIELD,
            "filter": TEXT_FILTER,
            "page": 0,
            "page_size": 10,
        },
    )
    assert resp_filter.status_code == 200, f"filter request got {resp_filter.status_code}"
    filter_rows = resp_filter.get_json()['rows']
    assert isinstance(filter_rows, list), "'rows' must be a list"
    assert len(filter_rows) == 4
    assert all(row['symbol'] == TEXT_FILTER for row in filter_rows)
    assert sorted(row[SORT_FIELD] for row in filter_rows) == [0, 30, 60, 500]


# ---------------------------------------------------------------------------
# DRILL-06: Pivot coordinate filters applied — row_path / row_fields
# ---------------------------------------------------------------------------

def test_coordinate_filters(client):
    """Passing row_path with two field values returns exactly matching rows."""
    resp = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": f"{ROOT_VALUE}|||{CHILD_VALUE}",
            "row_fields": f"{ROOT_FIELD},{CHILD_FIELD}",
            "page": 0,
            "page_size": 20,
        },
    )
    assert resp.status_code == 200, f"coordinate filter request got {resp.status_code}"
    rows = resp.get_json()['rows']
    assert isinstance(rows, list), "'rows' must be a list"
    assert sorted(row[SORT_FIELD] for row in rows) == [-50, -20, 10, 30, 70, 100]
    for row in rows:
        assert row.get(ROOT_FIELD) == ROOT_VALUE, (
            f"Expected {ROOT_FIELD}={ROOT_VALUE!r}, got {row.get(ROOT_FIELD)} in row {row}"
        )
        assert row.get(CHILD_FIELD) == CHILD_VALUE, (
            f"Expected {CHILD_FIELD}={CHILD_VALUE!r}, got {row.get(CHILD_FIELD)} in row {row}"
        )


# ---------------------------------------------------------------------------
# DRILL-04 (extended): total_rows count in response
# ---------------------------------------------------------------------------

def test_total_rows_count_in_response(client):
    """The total counts all root matches, including those beyond this page."""
    resp = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "page": 0,
            "page_size": 10,
        },
    )
    assert resp.status_code == 200, f"Got {resp.status_code}"
    data = resp.get_json()
    assert 'total_rows' in data, (
        f"'total_rows' key missing from response: {list(data.keys())}"
    )
    assert isinstance(data['total_rows'], int), (
        f"'total_rows' should be an int, got {type(data['total_rows'])}"
    )
    assert data['total_rows'] >= len(data.get('rows', [])), (
        f"total_rows ({data['total_rows']}) < len(rows) ({len(data.get('rows', []))})"
    )
    assert data['total_rows'] == 12

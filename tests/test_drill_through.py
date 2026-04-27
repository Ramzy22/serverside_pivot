"""
RED-phase tests for the /api/drill-through REST endpoint (DRILL-03 through DRILL-06).

These tests FAIL until Plan 02 adds the Flask route at /api/drill-through.
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


DRILL_TABLE = "nasdaq_trader_demo_history"
ROOT_FIELD = "desk"
ROOT_VALUE = "Consumer Tactical"
CHILD_FIELD = "strategy"
CHILD_VALUE = "Event Driven"
SORT_FIELD = "day_pnl"
TEXT_FILTER = "TSLA"


# ---------------------------------------------------------------------------
# Fixture: Flask test client (module-scoped so we pay data-gen cost once)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """Return a Flask test client for the Dash app.

    If the app module cannot be imported (e.g., missing deps or the route has
    not been added yet), the fixture raises ImportError which pytest reports as
    an ERROR on every test in this module — that counts as RED state.
    """
    from app import app, get_adapter  # noqa: F401  (get_adapter triggers data load)
    get_adapter()  # pre-warm: loads the 2M-row simulation table
    # Set TESTING on Flask's underlying server, not Dash's config wrapper
    app.server.config['TESTING'] = True
    with app.server.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# DRILL-03: Endpoint returns HTTP 200 with a "rows" key
# ---------------------------------------------------------------------------

def test_endpoint_returns_rows(client):
    """GET /api/drill-through returns 200 and JSON body with a 'rows' list.

    FAILS (404) until the Flask route is registered in Plan 02.
    """
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
        "Route /api/drill-through does not exist yet — add it in Plan 02."
    )
    data = resp.get_json()
    assert data is not None, "Response body is not JSON"
    assert 'rows' in data, f"'rows' key missing from response: {data.keys()}"
    assert isinstance(data['rows'], list), f"'rows' should be a list, got {type(data['rows'])}"


# ---------------------------------------------------------------------------
# DRILL-04: Pagination — page=0 and page=1 return non-overlapping row sets
# ---------------------------------------------------------------------------

def test_pagination(client):
    """Page 0 and page 1 return different, non-overlapping rows.

    Also checks that each page returns at most page_size rows.

    FAILS (404) until the Flask route is registered in Plan 02.
    """
    page_size = 5
    resp0 = client.get(
        "/api/drill-through",
        query_string={
            "table": DRILL_TABLE,
            "row_path": ROOT_VALUE,
            "row_fields": ROOT_FIELD,
            "page": 0,
            "page_size": page_size,
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


# ---------------------------------------------------------------------------
# DRILL-05: sort_col / sort_dir and text filter params accepted
# ---------------------------------------------------------------------------

def test_sort_and_filter(client):
    """Passing sort_col and sort_dir returns rows; passing filter returns matching rows.

    For sort: the endpoint must accept the params without a 500 error and return rows.
    For filter: at least one returned row must contain the filter string in some field.

    FAILS (404) until the Flask route is registered in Plan 02.
    """
    # Sort check: just verify the endpoint accepts the params and returns rows
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
    # At least one row must contain the filter text in any field value
    if filter_rows:
        has_match = any(
            TEXT_FILTER in str(v)
            for row in filter_rows
            for v in row.values()
        )
        assert has_match, (
            f"filter={TEXT_FILTER} was passed but none of the returned rows contain it. "
            f"First row: {filter_rows[0]}"
        )


# ---------------------------------------------------------------------------
# DRILL-06: Pivot coordinate filters applied — row_path / row_fields
# ---------------------------------------------------------------------------

def test_coordinate_filters(client):
    """Passing row_path with two field values returns only matching rows.

    FAILS (404) until the Flask route is registered in Plan 02.
    """
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
    """Response JSON contains a 'total_rows' integer >= len(rows).

    FAILS (404 or missing key) until the Flask route is implemented in Plan 02.
    """
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

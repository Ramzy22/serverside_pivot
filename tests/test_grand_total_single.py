"""
Regression tests for BUG-14: duplicate grand total row in hierarchical and virtual-scroll responses.

All four tests are intentionally RED against the unfixed codebase:
- test_debug_logging_captures_request_and_response: FAILS because create_tanstack_adapter
  does not accept a `debug` parameter.
- test_single_grand_total_in_hierarchical_response: FAILS because handle_hierarchical_request
  emits two grand total rows instead of one.
- test_single_grand_total_in_virtual_scroll_response: FAILS for the same duplication reason.
- test_no_grand_total_when_totals_false: May pass or fail depending on current code.
"""
import logging
import pytest
import pyarrow as pa
from pivot_engine.tanstack_adapter import (
    create_tanstack_adapter,
    TanStackRequest,
    TanStackOperation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sales_table() -> pa.Table:
    return pa.table(
        {
            "region": pa.array(["North", "South", "East"]),
            "sales": pa.array([100, 200, 300], type=pa.int64()),
        }
    )


def _make_request(totals: bool) -> TanStackRequest:
    return TanStackRequest(
        operation=TanStackOperation.GET_DATA,
        table="sales",
        columns=[
            {"id": "region"},
            {
                "id": "sales_sum",
                "aggregationField": "sales",
                "aggregationFn": "sum",
            },
        ],
        filters={},
        sorting=[],
        grouping=["region"],
        aggregations=[],
        totals=totals,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def small_adapter():
    """Adapter loaded with a tiny 3-row sales dataset (no debug flag)."""
    adapter = create_tanstack_adapter(":memory:")
    data = _make_sales_table()
    adapter.controller.load_data_from_arrow("sales", data)
    return adapter


@pytest.fixture
def debug_adapter():
    """Adapter created with debug=True — intentionally RED because the factory
    does not yet accept that keyword argument."""
    adapter = create_tanstack_adapter(":memory:", debug=True)
    data = _make_sales_table()
    adapter.controller.load_data_from_arrow("sales", data)
    return adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_debug_logging_captures_request_and_response(debug_adapter, caplog):
    """
    When the adapter is created with debug=True a DEBUG log record from
    logger "pivot_engine.adapter" must be emitted that contains at minimum
    the keys "method", "row_count", and "total_row_count".

    RED: create_tanstack_adapter does not accept a debug parameter today,
    so the fixture itself will raise a TypeError before this body runs.
    """
    with caplog.at_level(logging.DEBUG, logger="pivot_engine.adapter"):
        await debug_adapter.handle_hierarchical_request(_make_request(True), [])

    assert len(caplog.records) > 0, (
        "Expected at least one DEBUG log record from 'pivot_engine.adapter'"
    )
    assert any(r.name == "pivot_engine.adapter" for r in caplog.records), (
        "No record from logger 'pivot_engine.adapter' found"
    )


@pytest.mark.asyncio
async def test_single_grand_total_in_hierarchical_response(small_adapter):
    """
    A hierarchical response with totals=True must contain exactly one grand
    total row (identified by _isTotal==True or _id=='Grand Total').

    RED: the current code emits two grand total rows due to BUG-14.
    """
    response = await small_adapter.handle_hierarchical_request(_make_request(True), [])
    total_rows = [
        r for r in response.data
        if r.get("_isTotal") or r.get("_id") == "Grand Total"
    ]
    assert len(total_rows) == 1, (
        f"Expected 1 grand total, got {len(total_rows)}. "
        f"IDs: {[r.get('_id') for r in response.data]}"
    )


@pytest.mark.asyncio
async def test_single_grand_total_in_virtual_scroll_response(small_adapter):
    """
    A virtual-scroll response with totals=True must also contain exactly one
    grand total row.

    RED: same duplication bug surfaces via handle_virtual_scroll_request.
    """
    response = await small_adapter.handle_virtual_scroll_request(
        _make_request(True), 0, 100, []
    )
    total_rows = [
        r for r in response.data
        if r.get("_isTotal") or r.get("_id") == "Grand Total"
    ]
    assert len(total_rows) == 1, (
        f"Expected 1 grand total, got {len(total_rows)}. "
        f"IDs: {[r.get('_id') for r in response.data]}"
    )


@pytest.mark.asyncio
async def test_no_grand_total_when_totals_false(small_adapter):
    """
    When totals=False the response must contain zero grand total rows.
    """
    response = await small_adapter.handle_hierarchical_request(_make_request(False), [])
    total_rows = [
        r for r in response.data
        if r.get("_isTotal") or r.get("_id") == "Grand Total"
    ]
    assert len(total_rows) == 0, (
        f"Expected 0 grand total rows, got {len(total_rows)}"
    )

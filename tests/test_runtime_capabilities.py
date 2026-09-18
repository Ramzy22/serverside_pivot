import asyncio

import pytest

from pivot_engine.runtime.lifecycle import RuntimeRequestLifecycle
from pivot_engine.runtime.models import PivotRequestContext, PivotViewState
from pivot_engine.runtime.request_factory import RuntimeRequestFactory
from pivot_engine.runtime.resilience import CircuitBreaker, PivotRequestTimeout
from pivot_engine.export_limits import ExportLimitError


def _context(**overrides):
    values = {
        "table": "sales",
        "session_id": "session-a",
        "client_instance": "grid-a",
        "state_epoch": 2,
        "window_seq": 7,
        "abort_generation": 1,
        "intent": "viewport",
        "viewport_active": True,
        "start_row": 40,
        "end_row": 87,
    }
    values.update(overrides)
    return PivotRequestContext(**values)


def test_request_factory_owns_sort_metadata_and_viewport_pagination():
    state = PivotViewState(
        row_fields=["region"],
        sorting=[{"id": "hierarchy", "desc": True, "absoluteSort": True}],
        sort_options={
            "columnOptions": {
                "region": {"sortKeyField": "region_order", "nulls": "last"}
            }
        },
    )

    request = RuntimeRequestFactory.build_request(state, _context())

    assert request.sorting == [
        {
            "id": "hierarchy",
            "desc": True,
            "sortKeyField": "region_order",
            "nulls": "last",
            "absoluteSort": True,
        }
    ]
    assert request.pagination == {
        "pageIndex": 0,
        "pageSize": 48,
        "startRow": 40,
        "endRow": 87,
    }


def test_request_factory_adds_implicit_formula_measure_references():
    columns = RuntimeRequestFactory.build_columns(
        ["region"],
        [],
        [
            {
                "field": "margin",
                "agg": "formula",
                "formula": "[revenue] - cost",
                "formulaRef": "margin",
            }
        ],
    )

    implicit = {
        column["aggregationField"]
        for column in columns
        if column.get("_isImplicitFormulaRef")
    }
    assert implicit == {"revenue", "cost"}


def test_request_factory_preserves_expand_all_exceptions():
    assert RuntimeRequestFactory.parse_expanded_paths(
        {"__ALL__": True, "North|||UK": False, "South": True}
    ) == [["__ALL__"], ["__COLLAPSED__", "North", "UK"]]


@pytest.mark.asyncio
async def test_lifecycle_times_out_and_records_backend_failure():
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=30)
    lifecycle = RuntimeRequestLifecycle(
        request_timeout_seconds=0.01,
        circuit_breaker=breaker,
    )

    async def slow_operation():
        await asyncio.sleep(0.1)

    with pytest.raises(PivotRequestTimeout):
        await lifecycle.run_backend_operation(_context(), "data", slow_operation)

    assert breaker.snapshot(("sales", "data"))["open"] is True


@pytest.mark.asyncio
async def test_lifecycle_does_not_trip_circuit_for_export_limit_rejection():
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=30)
    lifecycle = RuntimeRequestLifecycle(circuit_breaker=breaker)

    async def rejected_export():
        raise ExportLimitError("row budget exceeded")

    with pytest.raises(ExportLimitError):
        await lifecycle.run_backend_operation(_context(intent="export"), "export", rejected_export)

    assert breaker.snapshot(("sales", "export"))["open"] is False


@pytest.mark.asyncio
async def test_lifecycle_keeps_cancellation_lane_scoped_to_client_instance():
    lifecycle = RuntimeRequestLifecycle()
    assert lifecycle.lane(_context(client_instance="grid-a")) == "data"
    assert lifecycle.lane(_context(client_instance="grid-b", intent="chart")) == "chart"
    assert lifecycle.lane(_context(intent="export")) is None

"""ReportEditor metric keys must rank real overall aggregates with pivot axes."""

import pyarrow as pa
import pytest
from types import SimpleNamespace

from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime import (
    PivotRequestContext, PivotRuntimeService, PivotViewState, RuntimeDatasetAuthorizer,
)


class _ObservedAdapter:
    """Record real adapter calls without replacing aggregate execution."""

    def __init__(self, adapter, calls):
        self.adapter, self.calls = adapter, calls

    def __getattr__(self, name):
        return getattr(self.adapter, name)

    async def handle_request(self, request, **kwargs):
        self.calls.append(("aggregate", request, kwargs))
        return await self.adapter.handle_request(request, **kwargs)

    async def handle_virtual_scroll_request(self, request, *args, **kwargs):
        self.calls.append(("pivot", request, kwargs))
        return await self.adapter.handle_virtual_scroll_request(request, *args, **kwargs)


def _report(rows, *, col_fields, show_row_totals, measures=None, sort_by="sales_sum",
            calls=None, root_changes=None, principal=None, row_scope=None, filters=None):
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("report_metrics", pa.Table.from_pylist(rows))
    observed = _ObservedAdapter(adapter, calls if calls is not None else [])
    root = {"field": "region", "sortBy": sort_by, "sortDir": "desc", "topN": 1}
    root.update(root_changes or {})
    response = PivotRuntimeService(adapter_getter=lambda: observed).process(
        PivotViewState(
            row_fields=["region"], col_fields=col_fields,
            val_configs=measures or [{"field": "sales", "agg": "sum"}],
            show_row_totals=show_row_totals, show_col_totals=True,
            view_mode="report", report_def={"root": root}, expanded=True,
            filters=filters or {},
        ),
        PivotRequestContext(table="report_metrics", viewport_active=True,
                            start_row=0, end_row=100, needs_col_schema=True,
                            include_grand_total=True, principal=principal, row_scope=row_scope),
    )
    assert response.status == "data", response.error
    return response


@pytest.mark.parametrize("col_fields", [[], ["period"]], ids=["flat", "period"])
@pytest.mark.parametrize("show_row_totals", [True, False])
def test_native_sum_topn_selects_highest_overall_metric(col_fields, show_row_totals):
    calls = []
    response = _report([
        {"region": "A-low", "period": "Q1", "sales": 1},
        {"region": "Z-high", "period": "Q1", "sales": 100},
    ], col_fields=col_fields, show_row_totals=show_row_totals, calls=calls)
    assert [row["_id"] for row in response.data] == ["Z-high", "Other", "Grand Total"]
    metric = "Q1_sales_sum" if col_fields else "sales_sum"
    assert [row[metric] for row in response.data] == [100, 1, 101]
    if col_fields:
        public_metrics = {"Q1_sales_sum"}
        if show_row_totals:
            public_metrics.add("__RowTotal__sales_sum")
        assert {column["id"] for column in response.col_schema["columns"]} == public_metrics
        assert response.layout_revision == response.col_schema["layoutRevision"]
        assert all("sales_sum" not in row for row in response.data)
        assert all({key for key in row if key.startswith("__RowTotal__")} ==
                   (public_metrics - {"Q1_sales_sum"}) for row in response.data)
    ranking_calls = [request for kind, request, _ in calls
                     if kind == "aggregate" and request.grouping == ["region"]]
    assert len(ranking_calls) == (0 if col_fields and show_row_totals else 1)
    assert len(calls) == (4 if col_fields and not show_row_totals else 3)
    assert all(request.pagination == {"pageIndex": 0, "pageSize": 100000}
               for request in ranking_calls)


@pytest.mark.parametrize("show_row_totals", [True, False])
@pytest.mark.parametrize("formula", [False, True], ids=["avg", "formula-of-avg"])
def test_native_metric_topn_aggregates_source_rows_not_period_averages(show_row_totals, formula):
    # A-low has mean 10, despite period means 100 and 0 (naive mean 50).
    # Z-high has mean 20. Squaring the overall mean must also rank Z-high.
    rows = [{"region": "A-low", "period": "Q1", "sales": 100}]
    rows += [{"region": "A-low", "period": "Q2", "sales": 0}] * 9
    rows += [{"region": "Z-high", "period": period, "sales": 20}
             for period in ("Q1", "Q2")]
    measures = [{"field": "sales", "agg": "avg"}]
    if formula:
        measures.append({"field": "squared", "agg": "formula",
                         "formula": "[sales] * [sales]", "formulaScope": "measures"})
    calls = []
    response = _report(rows, col_fields=["period"], show_row_totals=show_row_totals,
                       measures=measures, sort_by="squared" if formula else "sales_avg", calls=calls)
    assert [row["_id"] for row in response.data] == ["Z-high", "Other", "Grand Total"]
    assert [row["Q1_sales_avg"] for row in response.data] == [20, 100, 60]
    assert [row["Q2_sales_avg"] for row in response.data] == [20, 0, 2]
    if show_row_totals:
        assert [row["__RowTotal__sales_avg"] for row in response.data] == pytest.approx([20, 10, 140 / 12])
    else:
        assert all(not any(key.startswith("__RowTotal__") for key in row) for row in response.data)
    assert all("sales_avg" not in row and "squared" not in row for row in response.data)
    ranking_calls = [request for kind, request, _ in calls
                     if kind == "aggregate" and request.grouping == ["region"]]
    assert len(ranking_calls) == (1 if formula or not show_row_totals else 0)


@pytest.mark.parametrize("show_row_totals", [True, False])
def test_native_metric_sort_without_topn_maps_configured_measure_alias(show_row_totals):
    response = _report([
        {"region": "A-low", "period": "Q1", "sales": 1},
        {"region": "Z-high", "period": "Q1", "sales": 100},
    ], col_fields=["period"], show_row_totals=show_row_totals,
        measures=[{"field": "sales", "agg": "sum", "alias": "revenue"}],
        root_changes={"topN": None})
    assert [row["_id"] for row in response.data] == ["Z-high", "A-low", "Grand Total"]
    assert [row["Q1_revenue"] for row in response.data] == [100, 1, 101]


def test_materialized_column_formula_keeps_its_native_sort_value_without_flat_query():
    calls = []
    response = _report([
        {"region": "A-low", "period": "Q1", "sales": 1},
        {"region": "Z-high", "period": "Q1", "sales": 100},
    ], col_fields=["period"], show_row_totals=False, sort_by="doubled", calls=calls,
        measures=[{"field": "sales", "agg": "sum"}, {
            "field": "doubled", "agg": "formula", "formulaScope": "columns",
            "formula": "[Q1_sales_sum] * 2",
        }])
    assert [row["_id"] for row in response.data] == ["Z-high", "Other", "Grand Total"]
    assert [row["doubled"] for row in response.data] == [200, 2, 202]
    assert not any(kind == "aggregate" and request.grouping for kind, request, _ in calls)


@pytest.mark.parametrize("sort_dir,sort_abs", [("asc", False), ("desc", True)])
def test_native_metric_sort_retains_direction_and_absolute_options(sort_dir, sort_abs):
    response = _report([
        {"region": "A-negative", "period": "Q1", "sales": -100},
        {"region": "Z-positive", "period": "Q2", "sales": 20},
    ], col_fields=["period"], show_row_totals=False,
        root_changes={"sortDir": sort_dir, "sortAbs": sort_abs})
    assert [row["_id"] for row in response.data] == ["A-negative", "Other", "Grand Total"]


@pytest.mark.parametrize("show_row_totals", [True, False])
def test_native_metric_sparse_child_other_grand_and_ranking_are_scoped(show_row_totals):
    user = SimpleNamespace(id="viewer-A", role="viewer", attributes={"tenant": "A"})
    scope = RuntimeDatasetAuthorizer({"report_metrics": {
        "roles": {"viewer"}, "operations": {"read"},
        "rowSecurity": {"mode": "claims", "claims": {"tenant": "tenant"}},
    }}).authorize(principal=user, table="report_metrics", operation="read")
    calls = []
    response = _report([
        {"tenant": "A", "scope": "Included", "region": "North", "account": "First", "period": "Q1", "sales": 10},
        {"tenant": "A", "scope": "Included", "region": "North", "account": "Second", "period": "Q2", "sales": 30},
        {"tenant": "B", "scope": "Included", "region": "North", "account": "PRIVATE", "period": "PRIVATE", "sales": 999},
        {"tenant": "A", "scope": "Excluded", "region": "South", "account": "FILTERED", "period": "FILTERED", "sales": 1000},
    ], col_fields=["period"], show_row_totals=show_row_totals,
        calls=calls, principal=user, row_scope=scope,
        filters={"scope": {"type": "text", "operator": "equals", "value": "Included"}},
        root_changes={"sortBy": "__field__", "topN": None, "defaultChild": {
            "field": "account", "sortBy": "sales_sum", "sortDir": "desc", "topN": 1,
        }})
    assert [row["_path"] for row in response.data] == [
        "North", "North|||Second", "North|||__other__", "__grand_total__",
    ]
    assert [(row["Q1_sales_sum"], row["Q2_sales_sum"]) for row in response.data] == [
        (10, 30), (None, 30), (10, None), (10, 30),
    ]
    headers = {"Q1_sales_sum", "Q2_sales_sum"}
    if show_row_totals:
        headers.add("__RowTotal__sales_sum")
        assert [row["__RowTotal__sales_sum"] for row in response.data] == [40, 30, 10, 40]
    assert {column["id"] for column in response.col_schema["columns"]} == headers
    assert response.layout_revision == response.col_schema["layoutRevision"]
    assert all(set(row) & {"sales_sum", "__RowTotal__sales_sum"} ==
               ({"__RowTotal__sales_sum"} if show_row_totals else set()) for row in response.data)
    assert all("PRIVATE" not in str(row) and "FILTERED" not in str(row) for row in response.data)
    assert len(calls) == (4 if show_row_totals else 5)
    assert all(kwargs["user"] is user and kwargs["row_scope"] is scope for _, _, kwargs in calls)
    ranking_calls = [request for kind, request, _ in calls
                     if kind == "aggregate" and request.grouping == ["account"]]
    assert len(ranking_calls) == (0 if show_row_totals else 1)
    if ranking_calls:
        assert ranking_calls[0].pagination == {"pageIndex": 0, "pageSize": 100000}
        assert ranking_calls[0].filters["region"] == {"type": "eq", "value": "North"}
        assert ranking_calls[0].filters["scope"]["value"] == "Included"

"""The server's pivot display order is independent of labels and viewport indices."""

import pytest

from pivot_engine.column_layout import (
    MAX_LAYOUT_MANIFEST_BYTES,
    MAX_LAYOUT_MANIFEST_IDS,
    LayoutRevisionStore,
    PivotColumnIdentity,
    UnknownLayoutRevision,
    catalog_from_pivot_values,
    resolve_column_layout,
)
from pivot_engine.runtime.models import PivotRequestContext
from pivot_engine.runtime.models import PivotViewState
from pivot_engine.runtime.export_service import RuntimeExportService
from pivot_engine.runtime.request_factory import RuntimeRequestFactory
from pivot_engine.adapter_viewport_cache import AdapterViewportKeyBuilder


def leaf(period, measure, *, label="Same label"):
    return PivotColumnIdentity(
        pivot_path=(("period", period),),
        measure_key=measure,
        cell_id=f"{period}_{measure}",
        label=label,
    )


def total(measure):
    return PivotColumnIdentity(
        pivot_path=(), measure_key=measure,
        cell_id=f"__RowTotal__{measure}", is_row_total=True,
    )


def test_identity_uses_typed_path_and_measure_instead_of_label_or_cell_index():
    a = leaf("P_1", "sales", label="Revenue")
    b = leaf("P_1", "units", label="Revenue")
    c = leaf("P_1", "sales", label="Renamed")
    d = PivotColumnIdentity((("period", 1),), "sales", "one_sales")
    e = PivotColumnIdentity((("period", "1"),), "sales", "one_sales")
    assert a.layout_id != b.layout_id
    assert a.layout_id == c.layout_id
    assert d.layout_id != e.layout_id


def test_saved_group_and_leaf_order_reconciles_missing_and_new_columns():
    p0_sales, p0_units = leaf("P0", "sales"), leaf("P0", "units")
    p1_sales, p1_units = leaf("P1", "sales"), leaf("P1", "units")
    p2_sales, p2_units = leaf("P2", "sales"), leaf("P2", "units")
    saved = [p1_units.layout_id, p0_units.layout_id, p1_sales.layout_id,
             p0_sales.layout_id, p2_sales.layout_id]
    initial = resolve_column_layout(
        [p0_sales, p0_units, p1_sales, p1_units], saved, structural_key="q1",
    )
    assert [col.cell_id for col in initial.columns] == [
        "P1_units", "P1_sales", "P0_units", "P0_sales",
    ]
    # P1 is temporarily absent; its saved ranks do not consume visual slots.
    absent = resolve_column_layout(
        [p0_sales, p0_units, p2_sales, p2_units], saved, structural_key="q2",
    )
    assert [col.cell_id for col in absent.columns] == [
        "P0_units", "P0_sales", "P2_sales", "P2_units",
    ]
    returned = resolve_column_layout(
        [p0_sales, p0_units, p1_sales, p1_units, p2_sales, p2_units],
        saved, structural_key="q3",
    )
    assert [col.cell_id for col in returned.columns] == [
        "P1_units", "P1_sales", "P0_units", "P0_sales", "P2_sales", "P2_units",
    ]
    assert len({initial.revision, absent.revision, returned.revision}) == 3


def test_duplicate_unknown_and_unauthorized_ids_are_ignored_without_schema_disclosure():
    a, b = leaf("A", "sales"), leaf("B", "sales")
    order = [b.layout_id, b.layout_id, "p:unknown", a.layout_id]
    result = resolve_column_layout([a, b], order, allowed_ids={a.layout_id}, structural_key="authorized")
    assert [col.layout_id for col in result.columns] == [a.layout_id]
    assert "unknown" not in repr(result)


def test_interleaved_leaf_moves_are_normalized_into_contiguous_groups_and_totals_last():
    p0_sales, p0_units = leaf("P0", "sales"), leaf("P0", "units")
    p1_sales, p1_units = leaf("P1", "sales"), leaf("P1", "units")
    subtotal = total("sales")
    result = resolve_column_layout(
        [p0_sales, p0_units, p1_sales, p1_units, subtotal],
        [subtotal.layout_id, p1_units.layout_id, p0_units.layout_id,
         p1_sales.layout_id, p0_sales.layout_id], structural_key="q",
    )
    assert [col.cell_id for col in result.columns] == [
        "P1_units", "P1_sales", "P0_units", "P0_sales", "__RowTotal__sales",
    ]


def test_manifest_count_and_bytes_are_bounded():
    a = leaf("A", "sales")
    with pytest.raises(ValueError, match="count"):
        resolve_column_layout([a], [a.layout_id] * (MAX_LAYOUT_MANIFEST_IDS + 1))
    with pytest.raises(ValueError, match="bytes"):
        resolve_column_layout([a], ["x" * (MAX_LAYOUT_MANIFEST_BYTES + 1)])


def test_revision_store_requires_same_server_scope_and_supports_worker_miss_retry():
    a, b = leaf("A", "sales"), leaf("B", "sales")
    manifest = [b.layout_id, a.layout_id]
    layout = resolve_column_layout([a, b], manifest, structural_key="query-a")
    first = LayoutRevisionStore()
    second = LayoutRevisionStore()
    first.put("owner-a", "query-a", layout.revision, manifest)
    assert first.get("owner-a", "query-a", layout.revision) == tuple(manifest)
    for store, owner, query in ((first, "owner-b", "query-a"),
                                (first, "owner-a", "query-b"),
                                (second, "owner-a", "query-a")):
        with pytest.raises(UnknownLayoutRevision):
            store.get(owner, query, layout.revision)
    second.put("owner-a", "query-a", layout.revision, manifest)
    assert second.get("owner-a", "query-a", layout.revision) == tuple(manifest)


def test_revision_store_evicts_old_layouts_without_changing_revision_identity():
    store = LayoutRevisionStore(max_entries=1)
    store.put("owner", "q", "revision-1", ["one"])
    store.put("owner", "q", "revision-2", ["two"])
    with pytest.raises(UnknownLayoutRevision):
        store.get("owner", "q", "revision-1")
    assert store.get("owner", "q", "revision-2") == ("two",)


def test_v1_context_parses_bounded_layout_token_manifest_and_pinning_separately():
    a, b = leaf("A", "sales"), leaf("B", "sales")
    token = "a" * 64
    context = PivotRequestContext.from_frontend(
        table="sales", trigger_prop="grid.viewport",
        viewport={"start": 0, "end": 50, "layoutRevision": token,
                  "layoutManifest": [b.layout_id, a.layout_id],
                  "pinnedColumnIds": [a.layout_id, a.layout_id]},
    )
    assert context.layout_revision == token
    assert context.layout_manifest == (b.layout_id, a.layout_id)
    assert context.pinned_column_ids == (a.layout_id,)


@pytest.mark.parametrize("metadata", [
    {"layoutRevision": "unknown"},
    {"layoutManifest": "not-an-array"},
    {"layoutManifest": ["x"] * (MAX_LAYOUT_MANIFEST_IDS + 1)},
    {"pinnedColumnIds": ["x"] * 65},
    {"pinnedColumnIds": [""]},
])
def test_v1_context_rejects_invalid_or_unbounded_layout_metadata(metadata):
    with pytest.raises(ValueError):
        PivotRequestContext.from_frontend(
            table="sales", trigger_prop="grid.viewport",
            viewport={"start": 0, "end": 50, **metadata},
        )


def test_request_factory_and_all_viewport_cache_keys_bind_revision_and_pinning():
    a, b = leaf("A", "sales"), leaf("B", "sales")
    state = PivotViewState(col_fields=["period"], val_configs=[{"field": "sales", "agg": "sum"}])
    def build(revision, pinned):
        context = PivotRequestContext.from_frontend(
            table="sales", trigger_prop="grid.viewport",
            viewport={"start": 0, "end": 20, "layoutRevision": revision,
                      "pinnedColumnIds": pinned},
        )
        return RuntimeRequestFactory.build_request(state, context)
    first = build("a" * 64, [a.layout_id])
    changed = build("b" * 64, [a.layout_id])
    repinned = build("a" * 64, [b.layout_id])
    assert first.layout_revision == "a" * 64
    assert first.pinned_column_ids == (a.layout_id,)
    keys = AdapterViewportKeyBuilder(lambda: 1)
    for left, right in ((first, changed), (first, repinned)):
        assert keys.response_window_key(left, 0, 20, [], 0, 10, True, False, None) != (
            keys.response_window_key(right, 0, 20, [], 0, 10, True, False, None)
        )
        assert keys.row_block_key(left, 0, [], 0, 10, None) != (
            keys.row_block_key(right, 0, [], 0, 10, None)
        )


def test_discovered_catalog_uses_actual_typed_path_values_for_layout_ids():
    columns = catalog_from_pivot_values(
        [{"_col_key": "1", "period": 1}, {"_col_key": "01", "period": "01"}],
        ["period"], ["sales_sum", "units_sum"], row_totals=True,
    )
    assert [col.cell_id for col in columns] == [
        "1_sales_sum", "1_units_sum", "01_sales_sum", "01_units_sum",
        "__RowTotal__sales_sum", "__RowTotal__units_sum",
    ]
    assert columns[0].pivot_path == (("period", 1),)
    assert columns[2].pivot_path == (("period", "01"),)
    assert columns[0].layout_id != columns[2].layout_id


def test_ambiguous_planner_cell_keys_fail_instead_of_merging_distinct_paths():
    with pytest.raises(ValueError, match="ambiguous"):
        catalog_from_pivot_values(
            [{"_col_key": "a|b", "region": "a", "period": "b"},
             {"_col_key": "a|b", "region": "a|b", "period": ""}],
            ["region", "period"], ["sales_sum"],
        )


def test_copy_and_export_resolve_selected_fields_in_server_display_order():
    fields = RuntimeExportService._resolve_export_field_order(
        [{"A_sales": 1, "B_sales": 2}],
        [{"id": "B_sales"}, {"id": "A_sales"}],
        ["A_sales", "B_sales"], PivotViewState(),
    )
    assert fields == ["B_sales", "A_sales"]

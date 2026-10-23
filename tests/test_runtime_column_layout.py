"""The server's pivot display order is independent of labels and viewport indices."""

import pytest

from pivot_engine.runtime.column_layout import (
    MAX_LAYOUT_MANIFEST_BYTES,
    MAX_LAYOUT_MANIFEST_IDS,
    PivotColumnIdentity,
    resolve_column_layout,
)


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

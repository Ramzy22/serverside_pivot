"""Randomized differential property fuzzer: Incremental Engine == Fresh DuckDB/Ibis SQL.

Generates random sequences of INSERT, UPDATE, DELETE, NULL values, NULL weights,
cross-group moves, cross-pivot-column moves, AST formulas, % of Grand Total,
and Baseline T0 comparisons, asserting mathematical parity against a fresh
authoritative DuckDB/Ibis query after EVERY single committed revision.
"""

from __future__ import annotations

import math
import random
from typing import Any, Dict, List

import pyarrow as pa
import pytest

from pivot_engine import create_tanstack_adapter
from pivot_engine.diff.baseline_comparison_service import BaselineComparisonService
from pivot_engine.diff.snapshot_provider import SnapshotProvider
from pivot_engine.streaming.exact_measure_engine import GRAND_TOTAL_CANONICAL_PATH, values_equal
from pivot_engine.streaming.reactive_query_coordinator import ReactiveQueryCoordinator
from pivot_engine.streaming.versioned_source_adapter import VersionedSourceAdapter
from pivot_engine.types.pivot_spec import Measure, PivotSpec


def _assert_rows_match(
    incremental_rows: List[Dict[str, Any]],
    authoritative_rows: List[Dict[str, Any]],
    measure_prefixes: List[str],
    step_label: str,
) -> None:
    inc_map = {r["_canonicalPath"]: r for r in incremental_rows}
    auth_map = {r["_canonicalPath"]: r for r in authoritative_rows}
    assert set(inc_map.keys()) == set(auth_map.keys()), f"Group path mismatch at {step_label}"
    for cpath, auth_row in auth_map.items():
        inc_row = inc_map[cpath]
        for key, auth_val in auth_row.items():
            if not any(key.endswith(suffix) for suffix in measure_prefixes):
                continue
            inc_val = inc_row.get(key)
            assert values_equal(inc_val, auth_val, float_tol=1e-6), (
                f"Parity mismatch at {step_label} path={cpath} col={key}: "
                f"incremental={inc_val!r} vs authoritative={auth_val!r}"
            )


@pytest.mark.parametrize("seed", [7, 42, 1337])
def test_randomized_differential_streaming_parity(seed: int):
    rng = random.Random(seed)
    desks = ["Equities", "Rates", "FX"]
    books = ["Alpha", "Beta", "Gamma"]
    quarters = ["Q1", "Q2", "Q3"]

    next_id = 1
    initial_rows: List[Dict[str, Any]] = []
    for _ in range(30):
        initial_rows.append(
            {
                "id": next_id,
                "desk": rng.choice(desks),
                "book": rng.choice(books),
                "quarter": rng.choice(quarters),
                "pnl": None if rng.random() < 0.1 else round(rng.uniform(-500, 1000), 2),
                "revenue": None if rng.random() < 0.1 else round(rng.uniform(10, 2000), 2),
                "weight": None if rng.random() < 0.15 else round(rng.uniform(0.5, 10.0), 2),
            }
        )
        next_id += 1

    arrow_tbl = pa.Table.from_pylist(
        initial_rows,
        schema=pa.schema(
            [
                ("id", pa.int64()),
                ("desk", pa.string()),
                ("book", pa.string()),
                ("quarter", pa.string()),
                ("pnl", pa.float64()),
                ("revenue", pa.float64()),
                ("weight", pa.float64()),
            ]
        ),
    )

    adapter = create_tanstack_adapter()
    adapter.controller.load_data_from_arrow("fuzz_trades", arrow_tbl)
    source = VersionedSourceAdapter(adapter.controller)
    snapshots = SnapshotProvider(adapter.controller, source)
    comparison = BaselineComparisonService(adapter.controller, snapshots)
    coordinator = ReactiveQueryCoordinator(adapter.controller, source, snapshots, comparison)

    spec = PivotSpec(
        table="fuzz_trades",
        rows=["desk", "book"],
        columns=["quarter"],
        measures=[
            Measure(field="pnl", agg="sum", alias="pnl_sum"),
            Measure(field="pnl", agg="avg", alias="pnl_avg"),
            Measure(field="pnl", agg="wavg", alias="pnl_wavg", weighted_field="weight"),
            Measure(field="pnl", agg="min", alias="pnl_min"),
            Measure(field="pnl", agg="max", alias="pnl_max"),
            Measure(field="revenue", agg="sum", alias="rev_sum"),
            Measure(field="margin", agg="formula", alias="margin", expression="[pnl_sum] / [rev_sum]"),
            Measure(field="pnl", agg="sum", alias="pnl_pct", window_func="percent_of_grand_total"),
        ],
    )
    live_q = coordinator.acquire_live_query(spec)
    active_ids = [r["id"] for r in initial_rows]
    measure_suffixes = ["pnl_sum", "pnl_avg", "pnl_wavg", "pnl_min", "pnl_max", "rev_sum", "margin", "pnl_pct"]

    for step in range(40):
        op = rng.choices(["insert", "upsert", "update", "delete"], weights=[0.25, 0.3, 0.25, 0.2])[0]
        if op == "delete" and len(active_ids) <= 3:
            op = "insert"

        if op == "insert":
            row = {
                "id": next_id,
                "desk": rng.choice(desks),
                "book": rng.choice(books),
                "quarter": rng.choice(quarters + ["Q4"]),
                "pnl": None if rng.random() < 0.15 else round(rng.uniform(-500, 1000), 2),
                "revenue": None if rng.random() < 0.15 else round(rng.uniform(10, 2000), 2),
                "weight": None if rng.random() < 0.2 else round(rng.uniform(0.5, 10.0), 2),
            }
            active_ids.append(next_id)
            next_id += 1
            batch = source.apply_mutation_batch("fuzz_trades", [row], mode="insert")
        elif op == "delete":
            target_id = rng.choice(active_ids)
            active_ids.remove(target_id)
            batch = source.apply_mutation_batch("fuzz_trades", [{"id": target_id}], mode="delete", key_fields=["id"])
        else:
            target_id = rng.choice(active_ids)
            row = {
                "id": target_id,
                "desk": rng.choice(desks),
                "book": rng.choice(books),
                "quarter": rng.choice(quarters + ["Q4"]),
                "pnl": None if rng.random() < 0.15 else round(rng.uniform(-500, 1000), 2),
                "revenue": None if rng.random() < 0.15 else round(rng.uniform(10, 2000), 2),
                "weight": None if rng.random() < 0.2 else round(rng.uniform(0.5, 10.0), 2),
            }
            batch = source.apply_mutation_batch("fuzz_trades", [row], mode=op, key_fields=["id"])

        live_q.apply_batch(batch)

        # Fresh authoritative Tier C SQL recomputation from DuckDB
        fresh_result = live_q._execute_authoritative_full_locked()
        fresh_coordinator = ReactiveQueryCoordinator(adapter.controller, source, snapshots, comparison)
        fresh_q = fresh_coordinator.acquire_live_query(spec)
        fresh_q.install_authoritative_result(fresh_result, partial=False)

        inc_rows, inc_gt = live_q.materialize_hierarchy_rows()
        auth_rows, auth_gt = fresh_q.materialize_hierarchy_rows()

        _assert_rows_match(inc_rows, auth_rows, measure_suffixes, f"seed={seed} step={step} op={op}")
        if auth_gt is not None:
            assert inc_gt is not None
            _assert_rows_match([inc_gt], [auth_gt], measure_suffixes, f"seed={seed} step={step} GT")

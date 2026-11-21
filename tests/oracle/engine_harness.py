"""Drive the runtime engine the way Dash does, and normalise its rows.

Kept separate from oracle.py so the oracle never imports engine code.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pyarrow as pa


def build_adapter(records: Sequence[Dict[str, Any]], *, dims=("r", "c"), measure="v"):
    from pivot_engine.tanstack_adapter import create_tanstack_adapter

    columns = {d: pa.array([x.get(d) for x in records], pa.string()) for d in dims}
    columns[measure] = pa.array([x.get(measure) for x in records], pa.int64())
    for extra in sorted({k for x in records for k in x} - set(dims) - {measure}):
        columns[extra] = pa.array([x.get(extra) for x in records], pa.string())
    adapter = create_tanstack_adapter(backend_uri=":memory:")
    adapter.controller.load_data_from_arrow("t", pa.table(columns))
    return adapter


def run_view(
    adapter,
    *,
    rows: List[str],
    agg: str,
    start: int = 0,
    end: int = 10_000,
    filters: Optional[Dict[str, Any]] = None,
    row_scope: Any = None,
    principal: Any = None,
) -> List[Dict[str, Any]]:
    from pivot_engine.runtime import PivotRequestContext, PivotRuntimeService, PivotViewState

    service = PivotRuntimeService(adapter_getter=lambda: adapter)
    state = PivotViewState(
        row_fields=list(rows), col_fields=[], val_configs=[{"field": "v", "agg": agg}],
        filters=dict(filters or {}), expanded=True, show_col_totals=True,
    )
    ctx = PivotRequestContext(
        table="t", trigger_prop="p.viewport", start_row=start, end_row=end,
        viewport_active=True, include_grand_total=True, principal=principal, row_scope=row_scope,
    )
    response = asyncio.run(service.process_async(state, ctx))
    return list(response.data or [])


def row_path(row: Dict[str, Any]) -> Tuple[Any, ...]:
    if row.get("_isTotal") and row.get("_path") == "__grand_total__":
        return ()
    return tuple(row.get(f) for f in row.get("_pathFields") or [])


def by_path(rows: Sequence[Dict[str, Any]], agg: str) -> Dict[Tuple[Any, ...], Any]:
    return {row_path(r): r.get(f"v_{agg}") for r in rows}

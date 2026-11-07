"""
End-to-end sort test: Curve Pillar as root field AND as a child, simulating
the exact frontend call sequence (default -> ASC -> DESC -> default again).

Uses PivotRuntimeService + PivotRequestContext.from_frontend()  (viewport_active=True)
so we exercise the LIVE DASH code path, not the standalone path.

Runs two variants of every scenario:
  - ASYNC  variant  : await svc.process_async()   (async Dash callback / this test's event loop)
  - SYNC   variant  : svc.process() in a thread   (standard synchronous Dash/Flask callback)
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "dash_tanstack_pivot", "pivot_engine")))

import asyncio
import concurrent.futures
import pandas as pd

from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.tanstack_adapter import TanStackPivotAdapter
from pivot_engine.runtime.service import PivotRuntimeService
from pivot_engine.runtime.models import PivotViewState, PivotRequestContext

# ---------------------------------------------------------------------------
# Tenor data: lexicographic order vs expected tenor order differ dramatically
# Lex order  : 10M 10Y 100Y 11M 11Y 1D 1M 1Y 2Y 3M 5Y 6M
# Tenor order: 1D  1M  3M   6M  10M 11M 1Y 2Y 5Y 10Y 11Y 100Y
# ---------------------------------------------------------------------------
PILLARS = [
    ("1D",   1),
    ("1M",   30),
    ("3M",   91),
    ("6M",   182),
    ("10M",  304),
    ("11M",  334),
    ("1Y",   365),
    ("2Y",   730),
    ("5Y",   1825),
    ("10Y",  3650),
    ("11Y",  4015),
    ("100Y", 36500),
]

TENOR_ORDER     = [p for p, _ in PILLARS]
TENOR_ORDER_REV = list(reversed(TENOR_ORDER))
LEX_ORDER       = sorted(TENOR_ORDER)          # what we see when the bug is present

SORT_OPTIONS = {
    "columnOptions": {
        "Curve Pillar": {"sortKeyField": "__sortkey__Curve Pillar"}
    }
}

def _viewport(seq: int = 1):
    return {
        "start": 0, "end": 49, "count": 50,
        "window_seq": seq, "state_epoch": 1,
        "abort_generation": 0,
        "session_id": "e2e-test",
        "client_instance": "e2e",
        "intent": "structural",
        "needs_col_schema": True,
        "include_grand_total": False,
    }

def _build_df_flat():
    rows = []
    for pillar, skey in PILLARS:
        for metric in ["Delta", "Gamma"]:
            rows.append({
                "Curve Pillar": pillar,
                "__sortkey__Curve Pillar": skey,
                "Metric": metric,
                "Sensi": float(skey * (1 if metric == "Delta" else 2)),
            })
    return pd.DataFrame(rows)

def _build_df_nested():
    rows = []
    for pillar, skey in PILLARS:
        rtype = "IR" if skey < 1000 else "FX"
        rows.append({
            "Risk Type": rtype,
            "Curve Pillar": pillar,
            "__sortkey__Curve Pillar": skey,
            "Sensi": float(skey),
        })
    return pd.DataFrame(rows)

def _root_pillars(data):
    return [
        r["Curve Pillar"]
        for r in (data or [])
        if isinstance(r, dict)
        and r.get("Curve Pillar") is not None
        and not r.get("_isTotal")
        and r.get("depth", 0) == 0
    ]

def _child_pillars(data, parent):
    return [
        r.get("Curve Pillar")
        for r in (data or [])
        if isinstance(r, dict)
        and r.get("Risk Type") == parent
        and r.get("Curve Pillar") is not None
        and not r.get("_isTotal")
        and r.get("depth", 0) == 1
    ]

ALL_PASS = []

def _check(label, actual, expected, *, indent="  "):
    ok = actual == expected
    status = "PASS" if ok else "FAIL"
    print(f"{indent}[{status}] {label}")
    if not ok:
        print(f"{indent}       expected : {expected}")
        print(f"{indent}       actual   : {actual}")
    ALL_PASS.append(ok)
    return ok


# ---------------------------------------------------------------------------
# Helper: run svc.process() safely from async context (simulate Flask thread)
# ---------------------------------------------------------------------------
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def _sync_process(svc, state, ctx):
    """Call svc.process() in a separate thread (no running event loop there)."""
    future = _thread_pool.submit(svc.process, state, ctx)
    return future.result(timeout=30)


# ---------------------------------------------------------------------------
# Scenario A: Curve Pillar as the ONLY row field (root level)
# ---------------------------------------------------------------------------
async def scenario_a(svc):
    print("\n" + "=" * 60)
    print("SCENARIO A: Curve Pillar = only row field (root level)")
    print("=" * 60)

    def _state(sorting):
        return PivotViewState(
            row_fields=["Curve Pillar"],
            col_fields=[],
            val_configs=[{"field": "Sensi", "agg": "sum"}],
            filters={},
            sorting=sorting,
            sort_options=SORT_OPTIONS,
            expanded={},
            show_row_totals=False,
            show_col_totals=False,
        )

    def _ctx(seq):
        return PivotRequestContext.from_frontend(
            table="BUCKETED_RHO",
            trigger_prop="pivot.viewport",
            viewport=_viewport(seq),
        )

    for variant, call in [
        ("ASYNC", lambda s, c: asyncio.ensure_future(svc.process_async(s, c))),
        ("SYNC",  lambda s, c: _sync_process(svc, s, c)),
    ]:
        print(f"\n  -- {variant} path --")

        # [1] Default (empty sorting) — THE MAIN BUG CASE
        if variant == "ASYNC":
            result = await svc.process_async(_state([]), _ctx(10))
        else:
            result = _sync_process(svc, _state([]), _ctx(20))
        _check(f"[{variant}] 1. default sort -> tenor order", _root_pillars(result.data), TENOR_ORDER)

        # [2] Explicit ASC
        if variant == "ASYNC":
            result = await svc.process_async(_state([{"id": "Curve Pillar", "desc": False}]), _ctx(11))
        else:
            result = _sync_process(svc, _state([{"id": "Curve Pillar", "desc": False}]), _ctx(21))
        _check(f"[{variant}] 2. explicit ASC -> tenor order", _root_pillars(result.data), TENOR_ORDER)

        # [3] Explicit DESC
        if variant == "ASYNC":
            result = await svc.process_async(_state([{"id": "Curve Pillar", "desc": True}]), _ctx(12))
        else:
            result = _sync_process(svc, _state([{"id": "Curve Pillar", "desc": True}]), _ctx(22))
        _check(f"[{variant}] 3. explicit DESC -> reversed tenor", _root_pillars(result.data), TENOR_ORDER_REV)

        # [4] Reset back to default — user clicks sort header twice
        if variant == "ASYNC":
            result = await svc.process_async(_state([]), _ctx(13))
        else:
            result = _sync_process(svc, _state([]), _ctx(23))
        pillars = _root_pillars(result.data)
        _check(f"[{variant}] 4. reset to default -> tenor order again", pillars, TENOR_ORDER)
        _check(f"[{variant}] 5. NOT lexicographic after reset", pillars != LEX_ORDER, True)


# ---------------------------------------------------------------------------
# Scenario B: Curve Pillar as CHILD (Risk Type -> Curve Pillar)
# ---------------------------------------------------------------------------
async def scenario_b(svc):
    print("\n" + "=" * 60)
    print("SCENARIO B: Curve Pillar as child of Risk Type (expanded)")
    print("=" * 60)

    expanded = {"IR": True, "FX": True}

    def _state(sorting):
        return PivotViewState(
            row_fields=["Risk Type", "Curve Pillar"],
            col_fields=[],
            val_configs=[{"field": "Sensi", "agg": "sum"}],
            filters={},
            sorting=sorting,
            sort_options=SORT_OPTIONS,
            expanded=expanded,
            show_row_totals=False,
            show_col_totals=False,
        )

    def _ctx(seq):
        return PivotRequestContext.from_frontend(
            table="NESTED_RHO",
            trigger_prop="pivot.viewport",
            viewport=_viewport(seq),
        )

    ir_exp = [p for p, s in PILLARS if s < 1000]
    fx_exp = [p for p, s in PILLARS if s >= 1000]

    for variant, call in [
        ("ASYNC", None),
        ("SYNC",  None),
    ]:
        print(f"\n  -- {variant} path --")

        async def _run(state, ctx):
            if variant == "ASYNC":
                return await svc.process_async(state, ctx)
            return _sync_process(svc, state, ctx)

        # [1] Default
        result = await _run(_state([]), _ctx(30 if variant == "ASYNC" else 40))
        _check(f"[{variant}] 1. default -> IR children tenor order", _child_pillars(result.data, "IR"), ir_exp)
        _check(f"[{variant}] 1. default -> FX children tenor order", _child_pillars(result.data, "FX"), fx_exp)

        # [2] ASC
        result = await _run(_state([{"id": "Curve Pillar", "desc": False}]), _ctx(31 if variant == "ASYNC" else 41))
        _check(f"[{variant}] 2. ASC -> IR tenor order", _child_pillars(result.data, "IR"), ir_exp)
        _check(f"[{variant}] 2. ASC -> FX tenor order", _child_pillars(result.data, "FX"), fx_exp)

        # [3] DESC
        result = await _run(_state([{"id": "Curve Pillar", "desc": True}]), _ctx(32 if variant == "ASYNC" else 42))
        _check(f"[{variant}] 3. DESC -> IR reversed", _child_pillars(result.data, "IR"), list(reversed(ir_exp)))
        _check(f"[{variant}] 3. DESC -> FX reversed", _child_pillars(result.data, "FX"), list(reversed(fx_exp)))

        # [4] Reset
        result = await _run(_state([]), _ctx(33 if variant == "ASYNC" else 43))
        _check(f"[{variant}] 4. reset -> IR tenor order again", _child_pillars(result.data, "IR"), ir_exp)
        _check(f"[{variant}] 4. reset -> FX tenor order again", _child_pillars(result.data, "FX"), fx_exp)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    df_flat   = _build_df_flat()
    df_nested = _build_df_nested()

    ctrl = ScalablePivotController(backend_uri=":memory:")
    con  = ctrl.planner.con
    con.create_table("BUCKETED_RHO", df_flat,   overwrite=True)
    con.create_table("NESTED_RHO",   df_nested, overwrite=True)

    adapter = TanStackPivotAdapter(ctrl)
    svc = PivotRuntimeService(lambda: adapter)

    await scenario_a(svc)
    await scenario_b(svc)

    passed = sum(ALL_PASS)
    total  = len(ALL_PASS)
    failed = total - passed

    print("\n" + "=" * 60)
    print(f"RESULTS: {passed}/{total} passed,  {failed} failed")
    print("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

"""
Replicates app.py's curve-pivot-grid pipeline exactly and prints every key
value at each stage so we can see where the sort breaks down.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
    "dash_tanstack_pivot", "pivot_engine")))

import pyarrow as pa
from pivot_engine import create_tanstack_adapter
from pivot_engine.runtime.service import PivotRuntimeService
from pivot_engine.runtime.models import PivotViewState, PivotRequestContext
from pivot_engine.runtime import SessionRequestGate

# ---------- monkey-patch _build_group_rows_sort to trace it ----------
from pivot_engine.scalable_pivot_controller import ScalablePivotController
_orig_bgrs = ScalablePivotController._build_group_rows_sort
def _traced_bgrs(group_rows, base_sort, column_sort_options=None):
    result = _orig_bgrs(group_rows, base_sort, column_sort_options)
    print(f"  [_build_group_rows_sort]")
    print(f"    group_rows          = {group_rows}")
    print(f"    base_sort (input)   = {base_sort}")
    print(f"    column_sort_options = {column_sort_options}")
    print(f"    result              = {result}")
    return result
ScalablePivotController._build_group_rows_sort = staticmethod(_traced_bgrs)

# ---------- same data as app.py ----------
curve_pillar_table = pa.Table.from_pydict({
    "desk": ["Rates","Rates","Rates","Rates","Rates",
             "Credit","Credit","Credit","Credit","Credit"],
    "Curve Pillar": ["1M","2W","1D","6Y","3M","1M","2W","1D","6Y","10Y"],
    "__sortkey__Curve Pillar": [30,14,1,2190,90,30,14,1,2190,3650],
    "pv01": [0.12,0.05,0.01,1.80,0.35,0.10,0.04,0.02,1.60,2.10],
    "dv01": [0.08,0.03,0.005,1.20,0.22,0.07,0.025,0.01,1.05,1.40],
})

adapter = create_tanstack_adapter(backend_uri=":memory:")
adapter.controller.load_data_from_arrow("curve_data", curve_pillar_table)

gate = SessionRequestGate()
svc  = PivotRuntimeService(lambda: adapter, session_gate=gate)

SORT_OPTIONS = {"columnOptions": {"Curve Pillar": {"sortKeyField": "__sortkey__Curve Pillar"}}}

def _viewport(seq):
    return {"start":0,"end":14,"count":15,"window_seq":seq,"state_epoch":1,
            "abort_generation":0,"session_id":"dbg","client_instance":"dbg",
            "intent":"structural","needs_col_schema":True,"include_grand_total":False}

def run(label, sorting, sort_options, seq):
    print(f"\n{'='*60}")
    print(f"CALL: {label}")
    print(f"  sorting      = {sorting}")
    print(f"  sort_options = {sort_options}")
    state = PivotViewState(
        row_fields=["Curve Pillar"],
        col_fields=[],
        val_configs=[{"field":"pv01","agg":"sum"}],
        filters={},
        sorting=sorting,
        sort_options=sort_options,
        expanded={},
        show_row_totals=False, show_col_totals=False,
    )
    ctx = PivotRequestContext.from_frontend(
        table="curve_data",
        trigger_prop="curve-pivot-grid.viewport",
        viewport=_viewport(seq),
    )
    print(f"  viewport_active = {ctx.viewport_active}")
    result = svc.process(state, ctx)
    pillars = [r.get("Curve Pillar") for r in (result.data or [])
               if isinstance(r,dict) and r.get("Curve Pillar") and not r.get("_isTotal")]
    print(f"  ORDER RETURNED : {pillars}")
    return pillars

# Simulate the exact Dash callback sequence:
# 1. Initial load — sorting set in Python layout, sortOptions from State prop
run("1. Initial (sorting=[ASC], sort_options=correct)",
    [{"id":"Curve Pillar","desc":False}], SORT_OPTIONS, 1)

# 2. User clicks header -> DESC
run("2. User clicks header -> DESC",
    [{"id":"Curve Pillar","desc":True}], SORT_OPTIONS, 2)

# 3. User clicks header again -> empty (reset to default)
run("3. User clicks header again -> reset (sorting=[])",
    [], SORT_OPTIONS, 3)

# 4. What if sort_options comes back as None from Dash State?
run("4. sort_options=None (State not hydrated / component bug)",
    [], None, 4)

# 5. What if sort_options comes back as empty dict?
run("5. sort_options={} (empty)",
    [], {}, 5)

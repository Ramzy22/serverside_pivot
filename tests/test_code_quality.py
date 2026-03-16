"""
Regression tests for code quality issues QUAL-03 and QUAL-04.

QUAL-03: PivotController had duplicate run_pivot_arrow() method (method shadowing bug).
QUAL-04: update_cell and update_record used string interpolation of values into SQL (injection risk).
"""
import inspect
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pivot_engine'))


def test_no_duplicate_run_pivot_arrow():
    """QUAL-03: PivotController must define run_pivot_arrow exactly once."""
    from pivot_engine.pivot_engine.controller import PivotController
    methods = [name for name, _ in inspect.getmembers(PivotController, predicate=inspect.isfunction)
               if name == 'run_pivot_arrow']
    assert len(methods) == 1, f"Expected 1 run_pivot_arrow, found {len(methods)}"


def test_update_cell_parameterized():
    """QUAL-04: update_cell must use parameterized query con.execute(sql, [...])."""
    import inspect as ins
    from pivot_engine.pivot_engine.scalable_pivot_controller import ScalablePivotController
    src = ins.getsource(ScalablePivotController.update_cell)
    assert "con.execute(sql, [" in src, "update_cell must use parameterized query con.execute(sql, [...])"
    assert "val_str}" not in src, "update_cell must not interpolate val_str into SQL"


def test_update_record_parameterized():
    """QUAL-04: update_record must use parameterized query con.execute(sql, [...])."""
    import inspect as ins
    from pivot_engine.pivot_engine.scalable_pivot_controller import ScalablePivotController
    src = ins.getsource(ScalablePivotController.update_record)
    assert "con.execute(sql, [" in src, "update_record must use parameterized query con.execute(sql, [...])"
    assert "val_str}" not in src, "update_record must not interpolate val_str into SQL"

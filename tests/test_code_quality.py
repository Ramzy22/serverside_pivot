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
    from pivot_engine.controller import PivotController
    methods = [name for name, _ in inspect.getmembers(PivotController, predicate=inspect.isfunction)
               if name == 'run_pivot_arrow']
    assert len(methods) == 1, f"Expected 1 run_pivot_arrow, found {len(methods)}"


def test_update_cell_parameterized():
    """QUAL-04: update_cell must delegate values to parameterized mutation execution."""
    import inspect as ins
    from pivot_engine.scalable_pivot_controller import ScalablePivotController
    update_source = ins.getsource(ScalablePivotController.update_cell)
    mutation_source = ins.getsource(ScalablePivotController._execute_parameterized_mutation)
    assert "_execute_parameterized_mutation(sql, [value, row_id])" in update_source
    assert "con.execute(sql, [*params])" in mutation_source
    assert "val_str}" not in update_source, "update_cell must not interpolate val_str into SQL"


def test_update_record_parameterized():
    """QUAL-04: update_record must use the parameterized row-transaction path."""
    import inspect as ins
    from pivot_engine.scalable_pivot_controller import ScalablePivotController
    update_source = ins.getsource(ScalablePivotController.update_record)
    records_source = ins.getsource(ScalablePivotController.update_records)
    transaction_source = ins.getsource(ScalablePivotController.apply_row_transaction)
    assert "await self.update_records(" in update_source
    assert "await self.apply_row_transaction(" in records_source
    assert "_execute_parameterized_mutation(" in transaction_source
    assert "[*updates.values(), *where_params]" in transaction_source
    assert "val_str}" not in update_source, "update_record must not interpolate val_str into SQL"

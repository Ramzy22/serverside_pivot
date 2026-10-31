"""Keep legacy arbitrary(how) reductions distinct from old window methods."""
import pytest
from pivot_engine.common import ibis_expression_builder as expression
from pivot_engine.types.pivot_spec import Measure


@pytest.mark.parametrize("aggregation", ["first", "last"])
@pytest.mark.parametrize("legacy", [True, False])
def test_first_last_selects_supported_reduction_api(monkeypatch, aggregation, legacy):
    calls = []
    class Reduction:
        def name(self, alias):
            calls.append(("alias", alias)); return self
    class Column:
        def arbitrary(self, *, how):
            assert legacy
            calls.append(("arbitrary", how)); return Reduction()
        def first(self):
            assert not legacy
            calls.append(("first",)); return Reduction()
        def last(self):
            assert not legacy
            calls.append(("last",)); return Reduction()
    monkeypatch.setattr(expression, "_ARBITRARY_SUPPORTS_HOW", legacy)
    result = expression.IbisExpressionBuilder(None).build_measure_aggregation(
        {"text": Column()}, Measure(field="text", agg=aggregation, alias="result"))
    assert isinstance(result, Reduction)
    assert calls == ([("arbitrary", aggregation)] if legacy else [(aggregation,)]) + [("alias", "result")]

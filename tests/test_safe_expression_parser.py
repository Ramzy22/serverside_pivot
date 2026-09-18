"""Resource-boundary regressions for server-side formula evaluation."""

from __future__ import annotations

import pytest

from pivot_engine.planner.expression_parser import SafeExpressionParser


def test_safe_expression_parser_keeps_normal_financial_formulas_working():
    parser = SafeExpressionParser()

    assert parser.evaluate("round((sales - cost) / sales * 100, 2)", {"sales": 125, "cost": 25}) == 80.0


@pytest.mark.parametrize(
    ("expression", "reason"),
    [
        ("1 + " * 1_100 + "1", "expression exceeds the maximum length"),
        ("2 ** 100000", "power exponent exceeds the maximum"),
        ("max(1, 2, 3, 4, 5, 6, 7, 8, 9)", "function call exceeds the maximum"),
    ],
)
def test_safe_expression_parser_rejects_resource_exhaustion_inputs(expression, reason):
    with pytest.raises(ValueError, match=reason):
        SafeExpressionParser().evaluate(expression, {})

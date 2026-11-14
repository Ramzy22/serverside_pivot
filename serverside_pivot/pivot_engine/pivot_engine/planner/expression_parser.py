"""A deliberately small, bounded arithmetic expression evaluator."""

from __future__ import annotations

import ast
import math
import operator
from typing import Any, Dict


class SafeExpressionParser:
    """Evaluate a restricted numeric AST without unbounded computation.

    Formulae are user-controlled input. Whitelisting AST operations prevents
    code execution, while deterministic limits prevent disproportionate parser,
    CPU, or memory use from otherwise-safe formulae.
    """

    MAX_EXPRESSION_LENGTH = 4_096
    MAX_AST_NODES = 128
    MAX_AST_DEPTH = 32
    MAX_FUNCTION_ARGUMENTS = 8
    MAX_POWER_EXPONENT = 64
    MAX_INTEGER_BITS = 1_024
    MAX_ABSOLUTE_FLOAT = 1e100

    operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.USub: operator.neg,
        ast.Pow: operator.pow,
    }

    safe_functions = {
        "abs": abs,
        "round": round,
        "ceil": math.ceil,
        "floor": math.floor,
        "max": max,
        "min": min,
        "int": int,
        "float": float,
    }

    def evaluate(self, expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate a numeric expression with a trusted value map.

        Formula text is intentionally omitted from failures: it can itself be
        tenant data and validation messages are frequently shown to clients.
        """
        if not isinstance(expression, str):
            raise ValueError("Invalid expression: expression must be text.")
        if len(expression) > self.MAX_EXPRESSION_LENGTH:
            raise ValueError("Invalid expression: expression exceeds the maximum length.")
        try:
            node = ast.parse(expression, mode="eval")
            self._validate_tree(node)
            return self._eval_node(node.body, context)
        except ValueError as exc:
            raise ValueError(f"Invalid expression: {exc}") from None
        except (ArithmeticError, MemoryError, OverflowError, RecursionError, SyntaxError, TypeError) as exc:
            raise ValueError(f"Invalid expression: {type(exc).__name__.lower()}.") from None

    def _validate_tree(self, root: ast.AST) -> None:
        """Bound AST size/depth before recursive evaluation starts."""
        nodes_seen = 0
        pending: list[tuple[ast.AST, int]] = [(root, 1)]
        while pending:
            node, depth = pending.pop()
            nodes_seen += 1
            if nodes_seen > self.MAX_AST_NODES:
                raise ValueError("expression exceeds the maximum AST node count.")
            if depth > self.MAX_AST_DEPTH:
                raise ValueError("expression exceeds the maximum AST depth.")
            pending.extend((child, depth + 1) for child in ast.iter_child_nodes(node))

    def _bounded_number(self, value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("formula values must be finite numeric scalars.")
        if isinstance(value, int):
            if value.bit_length() > self.MAX_INTEGER_BITS:
                raise ValueError("integer result exceeds the maximum size.")
            return value
        if not math.isfinite(value) or abs(value) > self.MAX_ABSOLUTE_FLOAT:
            raise ValueError("floating-point result is outside the allowed range.")
        return value

    def _context_value(self, value: Any) -> Any:
        """Validate concrete values while preserving trusted planner symbols."""
        if isinstance(value, (int, float, bool)):
            return self._bounded_number(value)
        # Ibis and other query planners place server-created symbolic
        # expressions in the context. They are not evaluated here and must
        # retain their native arithmetic operators.
        return value

    def _bounded_result(self, value: Any) -> Any:
        if isinstance(value, (int, float, bool)):
            return self._bounded_number(value)
        return value

    def _validate_power(self, left: Any, right: Any) -> None:
        if not isinstance(right, (int, float)) or isinstance(right, bool):
            return
        if abs(right) > self.MAX_POWER_EXPONENT:
            raise ValueError("power exponent exceeds the maximum.")
        if isinstance(left, int) and isinstance(right, int) and right >= 0:
            base_bits = max(left.bit_length(), 1)
            if base_bits * right > self.MAX_INTEGER_BITS:
                raise ValueError("power result exceeds the maximum integer size.")

    def _eval_node(self, node: ast.AST, context: Dict[str, Any]) -> Any:
        if isinstance(node, ast.Constant):
            return self._bounded_number(node.value)
        if isinstance(node, ast.Name):
            if node.id in context:
                return self._context_value(context[node.id])
            raise ValueError(f"Unknown variable: {node.id}")
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in self.operators:
                raise ValueError(f"Unsupported operator: {op_type.__name__}")
            left = self._eval_node(node.left, context)
            right = self._eval_node(node.right, context)
            if op_type is ast.Pow:
                self._validate_power(left, right)
            return self._bounded_result(self.operators[op_type](left, right))
        if isinstance(node, ast.UnaryOp):
            op_type = type(node.op)
            if op_type not in self.operators:
                raise ValueError(f"Unsupported unary operator: {op_type.__name__}")
            return self._bounded_result(self.operators[op_type](self._eval_node(node.operand, context)))
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in self.safe_functions:
                func_name = node.func.id if isinstance(node.func, ast.Name) else "unknown"
                allowed = ", ".join(sorted(self.safe_functions))
                raise ValueError(f"Unsupported function: {func_name}. Allowed: {allowed}")
            if node.keywords:
                raise ValueError("keyword function arguments are not supported.")
            if len(node.args) > self.MAX_FUNCTION_ARGUMENTS:
                raise ValueError("function call exceeds the maximum argument count.")
            args = [self._eval_node(arg, context) for arg in node.args]
            return self._bounded_result(self.safe_functions[node.func.id](*args))
        raise ValueError(f"Unsupported syntax: {type(node).__name__}")

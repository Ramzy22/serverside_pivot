import ast
import operator
from typing import Dict, Any, Union

class SafeExpressionParser:
    """
    Safely parses and evaluates arithmetic expressions using Python's AST.
    Replaces unsafe eval() calls.
    """
    
    # Whitelisted operators
    operators = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.USub: operator.neg,
        ast.Pow: operator.pow,
    }

    def evaluate(self, expression: str, context: Dict[str, Any]) -> Any:
        """
        Evaluate a math expression with a given context (variable map).
        """
        try:
            node = ast.parse(expression, mode='eval')
            return self._eval_node(node.body, context)
        except Exception as e:
            raise ValueError(f"Invalid expression '{expression}': {e}")

    def _eval_node(self, node: ast.AST, context: Dict[str, Any]) -> Any:
        if isinstance(node, ast.Num): # Python < 3.8
            return node.n
        elif isinstance(node, ast.Constant): # Python >= 3.8
            return node.value
        elif isinstance(node, ast.Name):
            if node.id in context:
                return context[node.id]
            raise ValueError(f"Unknown variable: {node.id}")
        elif isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type in self.operators:
                left = self._eval_node(node.left, context)
                right = self._eval_node(node.right, context)
                return self.operators[op_type](left, right)
            raise ValueError(f"Unsupported operator: {op_type}")
        elif isinstance(node, ast.UnaryOp):
            op_type = type(node.op)
            if op_type in self.operators:
                operand = self._eval_node(node.operand, context)
                return self.operators[op_type](operand)
            raise ValueError(f"Unsupported unary operator: {op_type}")
        else:
            raise ValueError(f"Unsupported syntax: {type(node)}")

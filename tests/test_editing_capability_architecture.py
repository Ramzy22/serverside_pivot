import ast
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENGINE_ROOT = (
    REPOSITORY_ROOT
    / "dash_tanstack_pivot"
    / "pivot_engine"
    / "pivot_engine"
)
ADAPTER_PATH = ENGINE_ROOT / "tanstack_adapter.py"
TRANSACTION_PATH = ENGINE_ROOT / "editing" / "transaction_adapter.py"


def test_transaction_capability_owns_adapter_transaction_methods():
    adapter_tree = ast.parse(ADAPTER_PATH.read_text(encoding="utf-8"))
    transaction_tree = ast.parse(TRANSACTION_PATH.read_text(encoding="utf-8"))
    adapter_class = next(
        node
        for node in adapter_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TanStackPivotAdapter"
    )
    transaction_class = next(
        node
        for node in transaction_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TransactionAdapterMixin"
    )

    adapter_methods = {
        node.name
        for node in adapter_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    transaction_methods = {
        node.name
        for node in transaction_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert "handle_transaction" not in adapter_methods
    assert "handle_updates" not in adapter_methods
    assert {"handle_transaction", "handle_updates"} <= transaction_methods


def test_transaction_capability_does_not_import_adapter_facade():
    tree = ast.parse(TRANSACTION_PATH.read_text(encoding="utf-8"))
    assert not any(
        isinstance(node, ast.ImportFrom)
        and "tanstack_adapter" in (node.module or "")
        for node in ast.walk(tree)
    )


def test_transaction_capability_stays_below_god_object_limit():
    line_count = len(TRANSACTION_PATH.read_text(encoding="utf-8").splitlines())
    assert line_count <= 1_500


def test_editing_package_is_included_in_distribution():
    pyproject = (REPOSITORY_ROOT / "dash_tanstack_pivot" / "pyproject.toml").read_text(
        encoding="utf-8"
    )
    assert '"pivot_engine.editing"' in pyproject


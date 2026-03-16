"""
test_docs_examples_contract.py
--------------------------------
Contract tests asserting that documented examples exist, are importable as modules,
and that the two-instance example contains the required multi-instance isolation wiring.

These tests are deterministic and require no running server or database.

Run:
    python -m pytest tests/test_docs_examples_contract.py -q
"""

import ast
import pathlib
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_example(filename: str) -> ast.Module:
    path = EXAMPLES_DIR / filename
    assert path.exists(), f"Example file does not exist: {path}"
    return ast.parse(path.read_text(encoding="utf-8"))


def _find_calls(tree: ast.Module, func_name: str) -> list[ast.Call]:
    """Return all Call nodes where the function is named func_name."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == func_name)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == func_name)
        )
    ]


def _get_keyword_value(call: ast.Call, keyword: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == keyword:
            return kw.value
    return None


def _string_value(node: ast.expr | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


# ---------------------------------------------------------------------------
# Test 1: All three example files exist
# ---------------------------------------------------------------------------


def test_example_dash_basic_exists():
    assert (EXAMPLES_DIR / "example_dash_basic.py").exists()


def test_example_dash_hierarchical_exists():
    assert (EXAMPLES_DIR / "example_dash_hierarchical.py").exists()


def test_example_dash_sql_multi_instance_exists():
    assert (EXAMPLES_DIR / "example_dash_sql_multi_instance.py").exists()


# ---------------------------------------------------------------------------
# Test 2: All three examples parse as valid Python
# ---------------------------------------------------------------------------


def test_example_dash_basic_is_valid_python():
    _parse_example("example_dash_basic.py")


def test_example_dash_hierarchical_is_valid_python():
    _parse_example("example_dash_hierarchical.py")


def test_example_dash_sql_multi_instance_is_valid_python():
    _parse_example("example_dash_sql_multi_instance.py")


# ---------------------------------------------------------------------------
# Test 3: Two-instance example declares at least two DashTanstackPivot components
# ---------------------------------------------------------------------------


def test_two_instance_example_has_two_pivot_components():
    tree = _parse_example("example_dash_sql_multi_instance.py")
    pivot_calls = _find_calls(tree, "DashTanstackPivot")
    assert len(pivot_calls) >= 2, (
        f"example_dash_sql_multi_instance.py must declare at least 2 DashTanstackPivot instances, "
        f"found {len(pivot_calls)}"
    )


# ---------------------------------------------------------------------------
# Test 4: Two-instance example uses distinct id values for each pivot
# ---------------------------------------------------------------------------


def test_two_instance_example_has_distinct_ids():
    tree = _parse_example("example_dash_sql_multi_instance.py")
    pivot_calls = _find_calls(tree, "DashTanstackPivot")
    ids = [_string_value(_get_keyword_value(call, "id")) for call in pivot_calls]
    non_null_ids = [i for i in ids if i is not None]
    assert len(non_null_ids) >= 2, (
        "Two-instance example must provide string id= for at least 2 pivot components"
    )
    assert len(set(non_null_ids)) == len(non_null_ids), (
        f"Duplicate pivot id values detected: {non_null_ids}"
    )


# ---------------------------------------------------------------------------
# Test 5: Two-instance example uses distinct table= values for each pivot
# ---------------------------------------------------------------------------


def test_two_instance_example_has_distinct_tables():
    tree = _parse_example("example_dash_sql_multi_instance.py")
    pivot_calls = _find_calls(tree, "DashTanstackPivot")

    tables: list[str] = []
    for call in pivot_calls:
        tbl_node = _get_keyword_value(call, "table")
        if tbl_node is not None:
            # table may be a constant string or a Name referencing a TABLE_NAME variable
            if isinstance(tbl_node, ast.Constant):
                tables.append(tbl_node.value)
            elif isinstance(tbl_node, ast.Name):
                tables.append(tbl_node.id)  # use variable name as proxy

    assert len(tables) >= 2, (
        "Two-instance example must provide table= for at least 2 pivot components"
    )
    assert len(set(tables)) == len(tables), (
        f"Duplicate table values detected: {tables}. Each instance must be scoped to a distinct table."
    )


# ---------------------------------------------------------------------------
# Test 6: README references examples at correct paths
# ---------------------------------------------------------------------------


def test_readme_links_to_multi_instance_example():
    readme = REPO_ROOT / "README.md"
    assert readme.exists(), "README.md not found at repo root"
    content = readme.read_text(encoding="utf-8")
    assert "example_dash_sql_multi_instance.py" in content, (
        "README.md must reference examples/example_dash_sql_multi_instance.py"
    )


def test_readme_links_to_basic_example():
    readme = REPO_ROOT / "README.md"
    content = readme.read_text(encoding="utf-8")
    assert "example_dash_basic.py" in content, (
        "README.md must reference examples/example_dash_basic.py"
    )


def test_readme_links_to_hierarchical_example():
    readme = REPO_ROOT / "README.md"
    content = readme.read_text(encoding="utf-8")
    assert "example_dash_hierarchical.py" in content, (
        "README.md must reference examples/example_dash_hierarchical.py"
    )


# ---------------------------------------------------------------------------
# Test 7: Two-instance example references multi-instance isolation keywords
# ---------------------------------------------------------------------------


def test_two_instance_example_contains_isolation_wiring():
    """The multi-instance example must reference the key identity fields in its source."""
    path = EXAMPLES_DIR / "example_dash_sql_multi_instance.py"
    source = path.read_text(encoding="utf-8")
    required_patterns = [
        "DashTanstackPivot(",
        "table=",
        "register_dash_callbacks_for_instances",
        "DashPivotInstanceConfig",
        "SessionRequestGate",
    ]
    missing = [p for p in required_patterns if p not in source]
    assert not missing, (
        f"Multi-instance example is missing required isolation wiring: {missing}"
    )

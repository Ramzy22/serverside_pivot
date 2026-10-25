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


def test_operations_document_explains_worker_and_accounting_budgets():
    operations = (REPO_ROOT / "docs" / "OPERATIONS.md").read_text(encoding="utf-8")
    for required in (
        "PIVOT_TENANT_MAX_BYTES", "PIVOT_SYSTEM_MAX_BYTES",
        "PIVOT_CDC_TENANT_MAX_BYTES", "PIVOT_CDC_JOB_TTL_SECONDS",
        "per-worker fairness", "accounting_tenant",
    ):
        assert required in operations

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


def test_distributed_runtime_operations_documentation_declares_required_controls():
    path = REPO_ROOT / "docs" / "operations" / "distributed-runtime.md"
    assert path.exists(), "distributed runtime operations documentation is missing"
    text = path.read_text(encoding="utf-8")
    for required in (
        "PIVOT_DISTRIBUTED_RUNTIME",
        "PIVOT_RUNTIME_REDIS_URL",
        "PIVOT_RUNTIME_REDIS_PREFIX",
        "PIVOT_DASH_SESSION_SECRET",
        "editing_not_available_in_distributed_mode",
    ):
        assert required in text


def test_exact_cdc_operations_documentation_declares_safety_and_performance_contract():
    path = REPO_ROOT / "docs" / "operations" / "exact-cdc.md"
    text = path.read_text(encoding="utf-8")
    for required in (
        "event_id",
        "sequence",
        "row_identity",
        "old_row",
        "new_row",
        "PollingCDCProvider",
        "invalidated",
        "PIVOT_RUNTIME_REDIS_URL",
        "source_checkpoints",
        "source_rows",
        "authoritative snapshot",
        "CDC_EXACT_STATE_STORE",
        "at or below the saved position",
    ):
        assert required in text


def test_release_gates_publish_on_complete_installed_qualification_and_uploads_evidence():
    workflow = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    qualification = workflow.index("python scripts/certify_flagship.py --installed-release")
    publish = workflow.index("- name: Publish to PyPI")
    assert qualification < publish
    assert 'dash_tanstack_pivot[dev,api,redis,streamlit,security]' in workflow[:qualification]
    qualification_command = workflow[qualification:workflow.index("\n", qualification)]
    assert "--cancellable-exports" not in qualification_command
    assert "--artifact-dir \"$PIVOT_RELEASE_QUALIFICATION_DIR\"" in qualification_command
    assert "PIVOT_RELEASE_QUALIFICATION_DIR=\"$RUNNER_TEMP/installed-release-qualification-" in workflow[:qualification]
    assert ">> \"$GITHUB_ENV\"" in workflow[:qualification]
    upload = workflow.index("name: release-installed-qualification-evidence")
    upload_block = workflow[upload:publish]
    assert "if: always()" in upload_block
    assert "path: ${{ env.PIVOT_RELEASE_QUALIFICATION_DIR }}/" in upload_block
    assert "--export-qualification" not in workflow[:publish]


def test_dev_extra_declares_direct_python_test_dependencies():
    import tomllib

    project = tomllib.loads((REPO_ROOT / "dash_tanstack_pivot" / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = "\n".join(project["project"]["optional-dependencies"]["dev"]).lower()
    for name in ("pytest", "pytest-asyncio", "httpx", "fakeredis[lua]", "selenium",
                 "openpyxl", "requests", "packaging", "flask", "werkzeug", "starlette", "tomli"):
        assert name in dependencies


def test_export_operations_documentation_names_formats_limits_and_release_command():
    """The public docs must state the host/API format boundary and gate command."""
    operations = (REPO_ROOT / "docs" / "OPERATIONS.md").read_text(encoding="utf-8")
    certification = (
        REPO_ROOT / "docs" / "flagship-functionality-certification.md"
    ).read_text(encoding="utf-8")
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    for text in (operations, certification, readme):
        assert "CSV and XLSX" in text
        assert "TSV and HTML" in text

    assert "PIVOT_EXPORT_MAX_ROWS" in operations
    assert "PIVOT_EXPORT_MAX_BYTES" in operations
    assert "scripts/certify_flagship.py --export-qualification" in operations
    assert "normal" in operations and "constrained" in operations
    assert "export-qualification.json" in operations

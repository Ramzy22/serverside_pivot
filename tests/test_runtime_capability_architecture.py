import ast
from pathlib import Path


RUNTIME_ROOT = (
    Path(__file__).resolve().parents[1]
    / "dash_tanstack_pivot"
    / "pivot_engine"
    / "pivot_engine"
    / "runtime"
)


def _source(name: str) -> str:
    return (RUNTIME_ROOT / name).read_text(encoding="utf-8")


def test_runtime_service_is_a_compatibility_facade():
    source = _source("service.py")
    compatibility = _source("compatibility.py")
    assert len(source.splitlines()) <= 500
    assert "return await self._router.process_async(state, context)" in source
    assert "csv.writer" not in source
    assert "async def fetch_node_rows" not in source
    assert "RuntimeExportService._build_export_payload" in compatibility
    assert "RuntimeReportService._apply_report_annotations" in compatibility


def test_runtime_capability_modules_do_not_import_service_facade():
    capability_files = [
        "lifecycle.py",
        "profile.py",
        "request_factory.py",
        "export_service.py",
        "report_service.py",
        "drill_service.py",
        "router.py",
        "compatibility.py",
    ]
    for filename in capability_files:
        tree = ast.parse(_source(filename), filename=filename)
        imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        assert not any(
            isinstance(node, ast.ImportFrom)
            and (node.module or "").endswith("runtime.service")
            for node in imports
        ), filename


def test_runtime_capabilities_stay_below_god_object_limit():
    for filename in [
        "service.py",
        "router.py",
        "lifecycle.py",
        "profile.py",
        "request_factory.py",
        "export_service.py",
        "report_service.py",
        "drill_service.py",
        "compatibility.py",
    ]:
        line_count = len(_source(filename).splitlines())
        assert line_count <= 1_500, f"{filename} grew to {line_count} lines"


def test_capability_architecture_and_cleanup_decisions_are_recorded():
    repository_root = Path(__file__).resolve().parents[1]
    architecture = (repository_root / "CAPABILITY_ARCHITECTURE.md").read_text(
        encoding="utf-8"
    )
    cleanup = (repository_root / "REFACTOR_CLEANUP_LEDGER.md").read_text(
        encoding="utf-8"
    )
    assert "Compatibility boundary" in architecture
    assert "Dependency direction" in architecture
    assert "Private adapter/controller/runtime helper" in cleanup

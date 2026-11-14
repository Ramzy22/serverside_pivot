import ast
from pathlib import Path


ENGINE_ROOT = (
    Path(__file__).resolve().parents[1]
    / "serverside_pivot"
    / "pivot_engine"
    / "pivot_engine"
)
TRANSPORT_ROOT = ENGINE_ROOT / "transport"


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _method(source: str, name: str) -> ast.FunctionDef:
    tree = ast.parse(source)
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def test_adapter_reexports_stable_transport_contracts():
    from pivot_engine.tanstack_adapter import (
        TanStackOperation as AdapterOperation,
        TanStackRequest as AdapterRequest,
        TanStackResponse as AdapterResponse,
    )
    from pivot_engine.transport import (
        TanStackOperation,
        TanStackRequest,
        TanStackResponse,
    )

    assert AdapterOperation is TanStackOperation
    assert AdapterRequest is TanStackRequest
    assert AdapterResponse is TanStackResponse


def test_adapter_delegates_request_and_response_mapping():
    source = _source(ENGINE_ROOT / "tanstack_adapter.py")
    request_method = _method(source, "convert_tanstack_request_to_pivot_spec")
    response_method = _method(source, "convert_pivot_result_to_tanstack_format")

    assert request_method.end_lineno - request_method.lineno + 1 <= 10
    assert response_method.end_lineno - response_method.lineno + 1 <= 15
    assert "self._request_mapper.convert_tanstack_request_to_pivot_spec" in source
    assert "self._response_mapper.convert_pivot_result_to_tanstack_format" in source


def test_transport_mappers_do_not_import_adapter_facade():
    for filename in ["contracts.py", "request_mapper.py", "response_mapper.py"]:
        tree = ast.parse(_source(TRANSPORT_ROOT / filename), filename=filename)
        imports = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        assert not any(
            isinstance(node, ast.ImportFrom)
            and "tanstack_adapter" in (node.module or "")
            for node in imports
        ), filename


def test_transport_capabilities_stay_below_god_object_limit():
    for filename in ["contracts.py", "request_mapper.py", "response_mapper.py"]:
        line_count = len(_source(TRANSPORT_ROOT / filename).splitlines())
        assert line_count <= 1_500, f"{filename} grew to {line_count} lines"

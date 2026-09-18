"""Tests for the optional Streamlit V2 host adapter."""

import importlib
import sys
import types
from pathlib import Path

import pytest


_PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "dash_tanstack_pivot"


def _module(monkeypatch):
    monkeypatch.syspath_prepend(str(_PACKAGE_ROOT))
    sys.modules.pop("streamlit_pivot", None)
    return importlib.import_module("streamlit_pivot")


def test_streamlit_host_is_optional_until_it_is_rendered(monkeypatch):
    module = _module(monkeypatch)
    monkeypatch.setitem(sys.modules, "streamlit", None)

    with pytest.raises(ImportError, match=r"serverside-pivot\[streamlit\]"):
        module.pivot_workspace("sales")


def test_streamlit_host_uses_the_manifest_entry_even_with_stale_hashed_assets(monkeypatch, tmp_path):
    module = _module(monkeypatch)
    (tmp_path / "pivot-workspace-old.js").write_text("old", encoding="utf-8")
    (tmp_path / "pivot-workspace-current.js").write_text("current", encoding="utf-8")
    (tmp_path / "pivot-workspace-manifest.json").write_text(
        '{"src/index.tsx":{"file":"pivot-workspace-current.js","isEntry":true}}',
        encoding="utf-8",
    )

    assert module._component_script(tmp_path) == "pivot-workspace-current.js"


def test_streamlit_host_registers_v2_component_and_keeps_runtime_direct(monkeypatch):
    module = _module(monkeypatch)
    calls = []

    def component(*args, **kwargs):
        calls.append((args, kwargs))

        def render(**render_kwargs):
            calls.append(("render", render_kwargs))
            return render_kwargs["data"]

        return render

    fake_streamlit = types.SimpleNamespace(
        components=types.SimpleNamespace(v2=types.SimpleNamespace(component=component))
    )
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    result = module.pivot_workspace(
        "sales",
        key="pivot",
        row_fields=["region"],
        view_state={"viewMode": "table"},
        transport_headers={"X-Client": "test"},
    )

    assert calls[0][0] == ("serverside-pivot.pivot_workspace",)
    assert calls[0][1]["js"].startswith("pivot-workspace-")
    assert calls[0][1]["js"].endswith(".js")
    assert calls[0][1]["isolate_styles"] is True
    expected_data = {
        "runtimeEndpoint": "/v1/runtime",
        "transportHeaders": {"X-Client": "test"},
        "viewStateEnabled": False,
        "configuration": {
            "table": "sales",
            "serverSide": True,
            "rowFields": ["region"],
            "colFields": [],
            "valConfigs": [],
            "viewMode": "table",
        },
    }
    assert calls[1][1] == {"key": "pivot", "data": expected_data}
    assert result == expected_data


def test_streamlit_host_requires_a_table(monkeypatch):
    module = _module(monkeypatch)
    with pytest.raises(ValueError, match="non-empty table"):
        module.pivot_workspace("  ")


def test_streamlit_host_preserves_shared_workspace_layout_fields(monkeypatch):
    """A host-neutral React preset must not be reset to empty Python defaults."""
    module = _module(monkeypatch)
    calls = []

    def component(*_args, **_kwargs):
        def render(**render_kwargs):
            calls.append(render_kwargs)
            return render_kwargs["data"]

        return render

    fake_streamlit = types.SimpleNamespace(
        components=types.SimpleNamespace(v2=types.SimpleNamespace(component=component))
    )
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    module.pivot_workspace(
        "sales",
        rowFields=["region", "segment"],
        colFields=["quarter"],
        valConfigs=[{"field": "revenue", "agg": "sum"}],
    )

    configuration = calls[0]["data"]["configuration"]
    assert configuration["rowFields"] == ["region", "segment"]
    assert configuration["colFields"] == ["quarter"]
    assert configuration["valConfigs"] == [{"field": "revenue", "agg": "sum"}]


def test_streamlit_view_state_output_is_an_explicit_opt_in(monkeypatch):
    module = _module(monkeypatch)
    calls = []

    def component(*_args, **_kwargs):
        def render(**render_kwargs):
            calls.append(render_kwargs)
            return render_kwargs["data"]

        return render

    fake_streamlit = types.SimpleNamespace(
        components=types.SimpleNamespace(v2=types.SimpleNamespace(component=component))
    )
    monkeypatch.setitem(sys.modules, "streamlit", fake_streamlit)

    def handle_view_state_change():
        pass

    module.pivot_workspace(
        "sales",
        view_state={"viewMode": "table"},
        on_view_state_change=handle_view_state_change,
    )

    assert calls[0]["data"]["viewStateEnabled"] is True
    assert calls[0]["default"] == {"view_state": {"viewMode": "table"}}
    assert calls[0]["on_view_state_change"] is handle_view_state_change

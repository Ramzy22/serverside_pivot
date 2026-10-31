"""Shared save/restore behavior and literal snapshot validation."""
from pathlib import Path
import shutil
import subprocess
import pytest
from tests.flagship_persistence import assert_saved_snapshot


def test_frontend_saved_view_persistence_executes_shared_hooks():
    node = shutil.which("node")
    assert node, "Node is required to verify shared persistence hooks"
    result = subprocess.run([node, str(Path(__file__).with_name("frontend_saved_view_persistence.cjs"))],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "saved view persistence passed" in result.stdout


@pytest.mark.parametrize("damage", [None, "table", "version", "axes", "density", "decimals", "width", "scope"])
def test_saved_snapshot_requires_actual_saved_model_and_geometry(damage):
    column = "sales"
    snapshot = {"version": 1, "table": "revenue_operations", "state": {
        "rowFields": ["period"], "colFields": ["region", "account"],
        "spacingMode": 0, "decimalPlaces": 2, "columnSizing": {column: 170},
        "filters": {"scope": {"conditions": [{"value": "Included"}]}}}}
    if damage == "table": snapshot["table"] = "another"
    elif damage == "version": snapshot["version"] = 2
    elif damage == "axes": snapshot["state"]["rowFields"] = ["region"]
    elif damage == "density": snapshot["state"]["spacingMode"] = 1
    elif damage == "decimals": snapshot["state"]["decimalPlaces"] = 0
    elif damage == "width": snapshot["state"]["columnSizing"][column] = 130
    elif damage == "scope": snapshot["state"]["filters"]["scope"]["conditions"][0]["value"] = "Excluded"
    if damage is None:
        assert_saved_snapshot(snapshot, column, 170)
    else:
        with pytest.raises(AssertionError):
            assert_saved_snapshot(snapshot, column, 170)


@pytest.mark.parametrize("damage", [None, "width", "height", "density", "absent"])
def test_restored_geometry_requires_saved_width_height_and_density(monkeypatch, damage):
    from tests import flagship_persistence as persistence
    saved = {"width": 170, "height": 36, "density": "Compact"}
    current = dict(saved)
    if damage == "width": current["width"] = 130
    elif damage == "height": current["height"] = 44
    elif damage == "density": current["density"] = "Normal"
    elif damage == "absent": current = None
    monkeypatch.setattr(persistence, "persistence_geometry", lambda *args: current)
    assert bool(persistence.geometry_if_restored(None, "sales", saved)) is (damage is None)

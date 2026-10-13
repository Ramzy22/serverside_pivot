"""Executable contract for the isolated AG Grid reference data and setup."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess

from examples import flagship_demo_data


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "reference" / "ag_grid_36_2"
FIXTURE = ROOT / "tests" / "data" / "flagship_parity_rows.json"
CONTRACT = json.loads((ROOT / "tests" / "flagship_requirements.json").read_text(encoding="utf-8"))


def test_reference_packages_are_pinned_to_contract_version():
    expected = CONTRACT["references"]["ssrm-read-only-v1"]["version"]
    package = json.loads((REFERENCE / "package.json").read_text(encoding="utf-8"))
    lock = json.loads((REFERENCE / "package-lock.json").read_text(encoding="utf-8"))
    for name in ("ag-grid-community", "ag-grid-enterprise"):
        assert package["dependencies"][name] == expected
        assert lock["packages"][f"node_modules/{name}"]["version"] == expected
    assert package["dependencies"]["ag-charts-enterprise"] == lock["packages"]["node_modules/ag-charts-enterprise"]["version"]


def test_reference_config_uses_real_ssrm_and_required_modules():
    config = json.loads((REFERENCE / "reference-config.json").read_text(encoding="utf-8"))
    expected = CONTRACT["references"]["ssrm-read-only-v1"]["options"]
    assert config["version"] == "36.2.0"
    assert config["edition"] == "Enterprise"
    assert config["options"]["rowModelType"] == "serverSide"
    assert config["options"]["pivotMode"] is True
    assert config["options"]["cellSelection"] is True
    assert config["options"]["locale"] == "en-GB"
    assert set(expected["modules"]).issubset(config["modules"])
    assert "FiltersToolPanelModule" in config["modules"]


def test_fixture_preserves_oracle_and_copy_edge_cases():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["id"] == "flagship-qualification-v1"
    assert fixture["locale"] == "en-GB"
    rows = fixture["rows"]
    assert len(rows) == 5
    included = [row for row in rows if row["scope"] == "Included"]
    assert sum(row["ventes_€"] for row in included) == 200
    assert sum(row["unités"] for row in included) == 10
    assert sum(row["écart"] for row in included) == -6.75
    assert {row["region"] for row in included} == {"Éclair", "München"}
    assert {row["account"] for row in included} == {"東京", "القاهرة"}
    assert [row["extrema"] for row in rows] == [100, 100, -100, -100, 0]
    assert rows[0]["copyTab"] == "left\tright"
    assert rows[1]["copyLF"] == "first\nsecond"
    assert rows[2]["copyCRLF"] == "first\r\nsecond"
    assert rows[3]["copyUnicode"] == "Éclair München 東京 القاهرة 🚀"
    assert rows[3]["copyNull"] is None
    assert rows[3]["copyEmpty"] == ""


def test_parity_env_uses_shared_rows_without_changing_default(monkeypatch):
    monkeypatch.delenv("PIVOT_FLAGSHIP_EXPORT_QUALIFICATION_DATA", raising=False)
    monkeypatch.delenv("PIVOT_FLAGSHIP_PARITY_DATA", raising=False)
    assert flagship_demo_data.build_demo_table(3).num_rows == 3
    monkeypatch.setenv("PIVOT_FLAGSHIP_PARITY_DATA", "1")
    rows = json.loads(FIXTURE.read_text(encoding="utf-8"))["rows"]
    assert flagship_demo_data.build_demo_table(3).to_pylist() == rows
    config = flagship_demo_data.workspace_config("dash")
    assert config["rowFields"] == ["region", "account"]
    assert config["colFields"] == ["period"]
    assert "copyTab" in config["availableFieldList"]


def test_reference_is_outside_product_distributions():
    npm_package = json.loads((ROOT / "dash_tanstack_pivot" / "package.json").read_text(encoding="utf-8"))
    assert all(not path.startswith("reference") for path in npm_package["files"])
    assert REFERENCE.is_relative_to(ROOT)
    assert not REFERENCE.is_relative_to(ROOT / "dash_tanstack_pivot")
    ignored = subprocess.run(
        ["git", "check-ignore", "reference/ag_grid_36_2/dist/index.html"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert ignored.returncode == 0

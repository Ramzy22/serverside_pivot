"""Wiring and cell parity of the M6 gate harness (no timing assertions)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "benchmark_core_vs_aggrid.py"


def test_harness_runs_every_scenario_with_cell_parity():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--sizes", "3000", "--runs", "1", "--quiet"],
        cwd=ROOT, capture_output=True, text=True, timeout=300, check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:] or result.stdout[-4000:]
    reports = json.loads(result.stdout.strip().splitlines()[-1])
    assert [(r["shape"], r["rows"]) for r in reports] == [("hierarchy", 3000), ("wide", 3000)]
    for report in reports:
        names = [s["name"] for s in report["scenarios"]]
        assert "initial (collapsed)" in names and "sort P000|units desc" in names
        assert all(s["cellsChecked"] > 0 for s in report["scenarios"])
        assert report["expandAfterView"]["productMs"] > 0

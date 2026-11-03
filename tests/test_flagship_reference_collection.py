"""Catch an incomplete reference runner before an expensive release build.

The final manifest still verifies each observed requirement identity and its
evidence. This check exercises actual pytest collection, including imported
functions and parameter expansion, without opening browsers or running fixtures.
"""

import json
import os
from pathlib import Path
import subprocess
import sys

from tests.flagship_contract import load_requirements


def test_reference_runner_collects_one_case_per_applicable_requirement():
    root = Path(__file__).resolve().parents[1]
    expected = {
        requirement.ag_grid["test_id"]
        for requirement in load_requirements(root / "tests/flagship_requirements.json")
        if requirement.ag_grid.get("result") != "not_applicable"
    }
    script = """
import json
import pytest
class Collected:
    def pytest_collection_finish(self, session):
        print('REFERENCE_COLLECTION=' + json.dumps([item.nodeid for item in session.items]))
raise SystemExit(pytest.main([
    'tests/test_flagship_ag_grid_reference.py', '--collect-only', '-q', '-m', 'flagship',
], plugins=[Collected()]))
"""
    environment = {
        **os.environ,
        "PYTEST_ADDOPTS": "",
        "PYTHONIOENCODING": "utf-8",
        "AG_GRID_REFERENCE_LIVE": "0",
    }
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=root, env=environment,
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    observations = [line.removeprefix("REFERENCE_COLLECTION=")
                    for line in result.stdout.splitlines() if line.startswith("REFERENCE_COLLECTION=")]
    assert len(observations) == 1, result.stdout
    nodes = json.loads(observations[0])
    assert expected and len(nodes) == len(set(nodes)) == len(expected), (
        f"Reference runner collects {len(nodes)} cases for {len(expected)} applicable IDs. "
        "Final qualification also requires exact observed identity/evidence matching."
    )

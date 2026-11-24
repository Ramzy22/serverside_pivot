"""The CI version-matrix job runs exactly the combinations scripts/ci/version_matrix.py defines."""

from __future__ import annotations

import importlib.util
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _matrix_module():
    spec = importlib.util.spec_from_file_location("version_matrix", ROOT / "scripts" / "ci" / "version_matrix.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ci_job_runs_every_combination():
    jobs = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))["jobs"]
    assert sorted(jobs["version-matrix"]["strategy"]["matrix"]["combination"]) == sorted(_matrix_module().MATRIX)


def test_matrix_floors_are_the_pyproject_lower_bounds():
    pyproject = (ROOT / "serverside_pivot" / "pyproject.toml").read_text(encoding="utf-8")
    floor = _matrix_module().MATRIX["floor"]
    for pkg, version in floor.items():
        found = re.search(rf'"{re.escape(pkg)}(?:\[[^\]]*\])?>=([0-9.]+)', pyproject)
        assert found, pkg
        assert tuple(map(int, version.split("."))) [:len(found.group(1).split("."))] == tuple(
            map(int, found.group(1).split("."))), (pkg, version, found.group(1))


def test_suites_exist():
    for suite in _matrix_module().SUITES:
        assert (ROOT / suite).exists(), suite

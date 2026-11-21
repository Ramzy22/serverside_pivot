"""Architecture contracts for the core (import-linter + static checks)."""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG_ROOT = ROOT / "serverside_pivot" / "pivot_engine"


def test_import_contracts_are_kept():
    exe = shutil.which("lint-imports")
    if exe is None:
        pytest.skip("import-linter is not installed (dev extra)")
    result = subprocess.run(
        [exe, "--config", str(ROOT / "serverside_pivot" / ".importlinter"), "--no-cache"],
        cwd=PKG_ROOT, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]


def test_core_has_no_raw_sql_outside_physical_and_authority():
    core = PKG_ROOT / "pivot_engine" / "core"
    offenders = []
    for path in core.rglob("*.py"):
        rel = path.relative_to(core).as_posix()
        if rel.startswith(("physical/", "authority/")):
            continue
        text = path.read_text(encoding="utf-8")
        if ".raw_sql(" in text or ".sql(" in text:
            offenders.append(rel)
    assert offenders == []


def test_core_passes_ruff_and_mypy_strict():
    core = PKG_ROOT / "pivot_engine" / "core"
    ruff = subprocess.run([sys.executable, "-m", "ruff", "check", str(core)], capture_output=True, text=True)
    if "No module named ruff" in ruff.stderr:
        pytest.skip("ruff is not installed (dev extra)")
    assert ruff.returncode == 0, ruff.stdout[-2000:]
    mypy = subprocess.run(
        [sys.executable, "-m", "mypy", "-p", "pivot_engine.core", "--strict", "--ignore-missing-imports",
         "--follow-imports=silent", "--explicit-package-bases"],
        cwd=PKG_ROOT, env={**__import__("os").environ, "MYPYPATH": "."}, capture_output=True, text=True, timeout=600,
    )
    if "No module named mypy" in mypy.stderr:
        pytest.skip("mypy is not installed (dev extra)")
    assert mypy.returncode == 0, mypy.stdout[-3000:]

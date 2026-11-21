"""Every requirement in docs/semantics/SEMANTICS.md is cited by a test."""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
REQ = re.compile(r"REQ-[A-Z]+(?:-[A-Z]+)?-\d{3}")

# Requirements whose implementation lands in a later milestone.  Each entry
# must stay uncovered until then; once a test cites it, remove it here.
PENDING = {
    "REQ-BACK-003": "M1 core/physical capability registry: timezone, decimal scale",
    "REQ-PAGE-002": "M1 core/logical cursor model",
    "REQ-PAGE-003": "M1 core/logical consistency modes",
    "REQ-PAGE-004": "M2 column windows through the core",
}


def _defined():
    return set(REQ.findall((ROOT / "docs" / "semantics" / "SEMANTICS.md").read_text(encoding="utf-8")))


def _cited():
    me = pathlib.Path(__file__).resolve()
    cited = set()
    for path in (ROOT / "tests").rglob("*"):
        if path.suffix in {".py", ".json", ".cjs"} and path.resolve() != me:
            cited |= set(REQ.findall(path.read_text(encoding="utf-8", errors="ignore")))
    return cited


def test_every_requirement_is_tested_or_explicitly_pending():
    missing = _defined() - _cited() - set(PENDING)
    assert not missing, f"requirements without a test: {sorted(missing)}"


def test_pending_list_is_not_stale():
    defined, cited = _defined(), _cited()
    stale = {r for r in PENDING if r in cited or r not in defined}
    assert not stale, f"remove from PENDING (now tested or undefined): {sorted(stale)}"


def test_tests_cite_only_defined_requirements():
    unknown = _cited() - _defined()
    assert not unknown, f"tests cite undefined requirements: {sorted(unknown)}"

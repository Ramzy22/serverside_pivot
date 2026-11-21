import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ENGINE_PATH = _ROOT / "serverside_pivot" / "pivot_engine"
if str(_ENGINE_PATH) not in sys.path:
    sys.path.insert(0, str(_ENGINE_PATH))

import os

# Dash reads require a dataset policy by default (REQ-SEC-006).  Legacy tests
# that register unauthenticated pivots opt in explicitly; security tests that
# need the default unset it locally.
os.environ.setdefault("PIVOT_DEV_UNAUTHENTICATED", "1")

collect_ignore_glob = [
    "serverside_pivot/pytest-cache-files-*",
]


def pytest_ignore_collect(collection_path=None, path=None, config=None):
    """Ignore temporary pytest cache directories if present."""
    raw_path = collection_path if collection_path is not None else path
    if raw_path is None:
        return False
    candidate = Path(str(raw_path))
    return candidate.name.startswith("pytest-cache-files-")


import pytest as _pytest


@_pytest.fixture(autouse=True)
def _no_row_scope_leaks():
    """A request's row grant must never outlive the request (REQ-SEC-001)."""
    from pivot_engine.core.authority.scope import _CURRENT, ScopeContext, current_scope

    before = current_scope()
    yield
    after = current_scope()
    if after is not before:
        _CURRENT.set(before)
        raise AssertionError(f"row scope leaked out of the test: {dict(after.grants)}")

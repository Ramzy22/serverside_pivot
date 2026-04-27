import os
import sys
from pathlib import Path


# The pivot_engine lives in dash_tanstack_pivot/pivot_engine/.
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "dash_tanstack_pivot",
        "pivot_engine",
    ),
)


_LEGACY_IMPORT_TESTS = {
    "test_advanced_planning.py",
    "test_config_main.py",
    "test_microservices.py",
}

_ROOT = Path(__file__).resolve().parent
_RUN_LEGACY_TESTS = os.environ.get("RUN_LEGACY_TESTS", "").lower() in {"1", "true", "yes"}

collect_ignore = [] if _RUN_LEGACY_TESTS else [
    str(_ROOT / "tests" / name)
    for name in sorted(_LEGACY_IMPORT_TESTS)
]

collect_ignore_glob = [
    "dash_tanstack_pivot/pytest-cache-files-*",
]


def pytest_ignore_collect(collection_path=None, path=None, config=None):
    """Keep default collection focused on the maintained runtime surface."""
    raw_path = collection_path if collection_path is not None else path
    if raw_path is None:
        return False

    candidate = Path(str(raw_path))
    if candidate.name.startswith("pytest-cache-files-"):
        return True

    if not _RUN_LEGACY_TESTS and candidate.name in _LEGACY_IMPORT_TESTS:
        return True

    return False

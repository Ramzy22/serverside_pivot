import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ENGINE_PATH = _ROOT / "serverside_pivot" / "pivot_engine"
if str(_ENGINE_PATH) not in sys.path:
    sys.path.insert(0, str(_ENGINE_PATH))

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

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ENGINE_PATH = _ROOT / "dash_tanstack_pivot" / "pivot_engine"
if str(_ENGINE_PATH) not in sys.path:
    sys.path.insert(0, str(_ENGINE_PATH))


_LEGACY_IMPORT_TESTS = {
    "test_advanced_planning.py",
    "test_config_main.py",
    "test_microservices.py",
}

_RUN_LEGACY_TESTS = os.environ.get("RUN_LEGACY_TESTS", "").lower() in {"1", "true", "yes"}

collect_ignore = [] if _RUN_LEGACY_TESTS else [
    str(Path(__file__).resolve().parent / name)
    for name in sorted(_LEGACY_IMPORT_TESTS)
]

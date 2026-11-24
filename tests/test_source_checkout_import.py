"""The source-checkout wrapper (``serverside_pivot/pivot_engine/__init__.py``) imports like the package."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_the_source_checkout_wrapper_exports_the_public_api():
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); import pivot_engine as p; "
        "assert p.PivotController is p.ScalablePivotController; "
        "from pivot_engine.runtime import PivotViewState; print(p.__file__)"
    )
    done = subprocess.run([sys.executable, "-I", "-c", code, str(ROOT / "serverside_pivot")],
                          capture_output=True, text=True, timeout=120, cwd=str(ROOT))
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip().endswith(str(Path("serverside_pivot", "pivot_engine", "__init__.py")))

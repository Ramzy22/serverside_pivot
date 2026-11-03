"""Source event regression only; installed browser qualification is separate."""
from pathlib import Path
import shutil
import subprocess

import pytest


def test_keyboard_copy_event_ownership_executes_actual_runtime_listeners():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the source event regression")
    script = Path(__file__).with_name("frontend_keyboard_copy_ownership.cjs")
    result = subprocess.run([node, "--test", str(script)], cwd=script.resolve().parents[1],
        capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "# pass 8" in result.stdout and "# fail 0" in result.stdout and "# skipped 0" in result.stdout

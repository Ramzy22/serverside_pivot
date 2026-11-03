"""Source component regression; native installed-artifact evidence is separate."""
from pathlib import Path
import shutil
import subprocess

import pytest


def test_notification_executes_actual_component_pointer_and_dismiss_contract():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the source component regression")
    script = Path(__file__).with_name("frontend_notification_interaction.cjs")
    result = subprocess.run([node, "--test", str(script)], cwd=script.resolve().parents[1],
                            capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "# pass 8" in result.stdout and "# fail 0" in result.stdout and "# skipped 0" in result.stdout

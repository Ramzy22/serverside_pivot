"""Run the shared clipboard serializer against literal TSV regression cases."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_shared_clipboard_serialization_preserves_cell_text():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for the frontend source regression")

    script = Path(__file__).with_name("frontend_clipboard_serialization.cjs")
    result = subprocess.run(
        [node, "--test", str(script)],
        cwd=script.resolve().parents[1],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr

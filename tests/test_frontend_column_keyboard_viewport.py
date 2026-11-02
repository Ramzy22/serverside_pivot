"""Run the real TanStack column-navigation geometry regression through pytest."""
from pathlib import Path
import shutil
import subprocess

import pytest


def test_pinned_column_viewport_alignment_with_actual_tanstack():
    node = shutil.which('node')
    if not node:
        pytest.skip('Node.js is required for the frontend regression')
    script = Path(__file__).with_name('frontend_column_keyboard_viewport.cjs')
    result = subprocess.run([node, '--test', str(script)], cwd=script.resolve().parents[1],
                            capture_output=True, text=True, encoding='utf-8', timeout=30, check=False)
    assert result.returncode == 0, result.stdout + result.stderr

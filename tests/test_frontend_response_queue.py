"""Runtime responses are delivered in order and none is lost (M3; see runtime/responseQueue.ts)."""

import pathlib
import shutil
import subprocess

import pytest


def test_response_queue_delivers_every_response_in_order():
    node = shutil.which("node")
    if node is None:
        pytest.skip("node unavailable")
    script = pathlib.Path(__file__).with_name("frontend_response_queue.cjs")
    result = subprocess.run([node, str(script)], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr

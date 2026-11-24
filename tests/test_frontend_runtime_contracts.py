"""M3 frontend runtime contracts: ordered response delivery (runtime/responseQueue.ts) and one canonical
row identity in the live store (runtime/rowIdentity.ts)."""

import pathlib
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("script", ["frontend_response_queue.cjs", "frontend_row_identity.cjs", "frontend_global_search_debounce.cjs", "frontend_dismiss_shadow_dom.cjs"])
def test_frontend_runtime_contract(script):
    node = shutil.which("node")
    if node is None:
        pytest.skip("node unavailable")
    script = pathlib.Path(__file__).with_name(script)
    result = subprocess.run([node, str(script)], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr

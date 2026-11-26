"""Execute transport regressions against the actual frontend sources."""

from pathlib import Path
import subprocess


def test_runtime_transport_frontend_boundaries():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["node", "--test", str(Path(__file__).with_name("frontend_runtime_transport.cjs"))],
        cwd=root, capture_output=True, text=True, timeout=240, check=False,  # ~20 s idle; slower under -n auto
    )
    assert result.returncode == 0, result.stdout + result.stderr

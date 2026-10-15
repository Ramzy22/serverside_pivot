"""Run shared inline recovery regressions against the real frontend sources."""

from pathlib import Path
import subprocess


def test_runtime_recovery_frontend_boundaries():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["node", "--test", str(Path(__file__).with_name("frontend_runtime_recovery.cjs"))],
        cwd=root, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr

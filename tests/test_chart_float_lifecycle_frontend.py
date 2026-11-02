"""Behavioral chart Float/Dock owner regression; qualificationEvidence:false."""
from pathlib import Path
import shutil
import subprocess


def test_chart_float_dock_and_gestures_reach_the_materialized_command_owner():
    node = shutil.which("node")
    assert node, "Node.js is required for the chart source regression"
    result = subprocess.run(
        [node, "--test", str(Path(__file__).with_name("frontend_chart_float_lifecycle.cjs"))],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr

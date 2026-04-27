"""Run Dash component generation and fail on react-docgen parser errors."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


DOCGEN_ERROR_MARKERS = (
    "Error with path",
    "did not recognize object of type",
    "Multiple exported component definitions found",
    "Cannot convert undefined or null to object",
)


def main() -> int:
    package_root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        "-m",
        "dash.development.component_generator",
        "./src/lib/components",
        "dash_tanstack_pivot",
        "-p",
        "package-info.json",
    ]
    result = subprocess.run(
        command,
        cwd=package_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout or ""
    if output:
        print(output, end="" if output.endswith("\n") else "\n")
    if result.returncode != 0:
        return result.returncode
    if any(marker in output for marker in DOCGEN_ERROR_MARKERS):
        print("Dash component docgen emitted parser errors; failing build.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

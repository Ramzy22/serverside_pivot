"""Reject distribution wheels that contain local build artifacts.

``twine check`` validates metadata, but it intentionally does not enforce this
project's package boundary.  Keeping that boundary explicit prevents a prior
frontend or setuptools build from silently inflating the published wheel.
"""

from __future__ import annotations

import sys
import zipfile
import re
from pathlib import Path


REQUIRED_MEMBERS = frozenset(
    {
        "dash_tanstack_pivot/DashTanstackPivot.py",
        "pivot_engine/scalable_pivot_controller.py",
    }
)
FORBIDDEN_SEGMENTS = ("/build/", "/node_modules/", "/__pycache__/")
REQUIRED_DEPENDENCIES = frozenset(
    {
        "dash",
        "duckdb",
        "ibis-framework",
        "pandas",
        "pyarrow",
    }
)


def _metadata_dependencies(archive: zipfile.ZipFile) -> set[str]:
    metadata_name = next(
        (name for name in archive.namelist() if name.endswith(".dist-info/METADATA")),
        None,
    )
    if metadata_name is None:
        return set()
    metadata = archive.read(metadata_name).decode("utf-8")
    dependencies: set[str] = set()
    for line in metadata.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        requirement = line.split(":", 1)[1].strip()
        match = re.match(r"[A-Za-z0-9_.-]+", requirement)
        if match:
            dependencies.add(match.group(0).lower())
    return dependencies


def verify_wheel(wheel_path: Path) -> list[str]:
    """Return human-readable contract violations for ``wheel_path``."""
    with zipfile.ZipFile(wheel_path) as archive:
        names = set(archive.namelist())
        violations = [
            f"contains generated path: {name}"
            for name in sorted(names)
            if any(segment in f"/{name}" for segment in FORBIDDEN_SEGMENTS)
        ]
        violations.extend(
            f"missing required member: {name}"
            for name in sorted(REQUIRED_MEMBERS - names)
        )
        dependencies = _metadata_dependencies(archive)
        violations.extend(
            f"missing runtime dependency metadata: {name}"
            for name in sorted(REQUIRED_DEPENDENCIES - dependencies)
        )
        return violations


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_wheel_contract.py PATH_TO_WHEEL", file=sys.stderr)
        return 2

    wheel_path = Path(argv[1])
    if not wheel_path.is_file():
        print(f"wheel not found: {wheel_path}", file=sys.stderr)
        return 2

    violations = verify_wheel(wheel_path)
    if violations:
        print(f"Wheel contract failed for {wheel_path}:", file=sys.stderr)
        print("\n".join(f"- {violation}" for violation in violations), file=sys.stderr)
        return 1

    print(f"Wheel contract OK: {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

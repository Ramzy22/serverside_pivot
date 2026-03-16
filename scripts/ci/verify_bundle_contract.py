#!/usr/bin/env python3
"""
verify_bundle_contract.py
--------------------------
Asserts that the expected minified bundle files exist after `npm run build`
inside dash_tanstack_pivot/.

The contract: after a successful build, these files must be present:
  dash_tanstack_pivot/dash_tanstack_pivot/dash_tanstack_pivot.min.js

Optional but checked if previously present:
  dash_tanstack_pivot/dash_tanstack_pivot/package-info.json
  dash_tanstack_pivot/dash_tanstack_pivot/metadata.json

Exits 0 on success, 1 on any missing required artifact.

Usage:
    python scripts/ci/verify_bundle_contract.py
    python scripts/ci/verify_bundle_contract.py --bundle-dir path/to/override
"""

import argparse
import sys
from pathlib import Path


# Required artifacts — build is considered broken if any of these are missing.
REQUIRED_ARTIFACTS = [
    "dash_tanstack_pivot.min.js",
]

# Optional artifacts — logged as warnings but do not fail the check.
OPTIONAL_ARTIFACTS = [
    "package-info.json",
    "metadata.json",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify JS bundle artifact contract.")
    parser.add_argument(
        "--bundle-dir",
        default=None,
        help=(
            "Path to the directory containing built artifacts. "
            "Default: dash_tanstack_pivot/dash_tanstack_pivot/ relative to repo root."
        ),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.parent

    if args.bundle_dir:
        bundle_dir = Path(args.bundle_dir)
    else:
        bundle_dir = repo_root / "dash_tanstack_pivot" / "dash_tanstack_pivot"

    if not bundle_dir.is_dir():
        print(
            f"ERROR: Bundle directory does not exist: {bundle_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    errors: list[str] = []
    warnings: list[str] = []

    for artifact in REQUIRED_ARTIFACTS:
        path = bundle_dir / artifact
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print(f"  FOUND (required): {artifact} ({size_kb:.1f} KB)")
        else:
            errors.append(f"MISSING (required): {artifact}")
            print(f"  MISSING (required): {artifact}", file=sys.stderr)

    for artifact in OPTIONAL_ARTIFACTS:
        path = bundle_dir / artifact
        if path.exists():
            print(f"  FOUND (optional): {artifact}")
        else:
            warnings.append(f"MISSING (optional): {artifact}")
            print(f"  WARNING (optional not found): {artifact}")

    if errors:
        print(
            f"\nverify_bundle_contract: FAILED — {len(errors)} required artifact(s) missing.",
            file=sys.stderr,
        )
        sys.exit(1)

    if warnings:
        print(f"\nverify_bundle_contract: OK (with {len(warnings)} optional warning(s))")
    else:
        print("\nverify_bundle_contract: OK — all required artifacts present.")

    sys.exit(0)


if __name__ == "__main__":
    main()

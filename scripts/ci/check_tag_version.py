#!/usr/bin/env python3
"""
check_tag_version.py
--------------------
Deterministic semantic-version guard for CI release workflows.

Reads the current git tag (from GITHUB_REF_NAME or `git describe`)
and compares it against the package version declared in
dash_tanstack_pivot/pyproject.toml.

Fails with a non-zero exit code if:
- The git tag is not a valid semver (vX.Y.Z or X.Y.Z).
- The package version does not match the tag version.

Usage:
    python scripts/ci/check_tag_version.py
    python scripts/ci/check_tag_version.py --allow-no-tag   # passes when no tag is found (local dev)
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


SEMVER_RE = re.compile(r"^v?(\d+\.\d+\.\d+)$")
PYPROJECT_PATH = Path(__file__).parent.parent.parent / "dash_tanstack_pivot" / "pyproject.toml"


def get_package_version() -> str:
    """Extract version from dash_tanstack_pivot/pyproject.toml."""
    text = PYPROJECT_PATH.read_text(encoding="utf-8")
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("version"):
            # matches: version = "0.0.2"
            match = re.search(r'version\s*=\s*"([^"]+)"', stripped)
            if match:
                return match.group(1)
    raise RuntimeError(f"Could not find version in {PYPROJECT_PATH}")


def get_git_tag(allow_no_tag: bool) -> str | None:
    """
    Determine the current git tag.

    Checks GITHUB_REF_NAME first (set by GitHub Actions on tag pushes),
    then falls back to `git describe --exact-match --tags HEAD`.
    Returns None if no tag is found and allow_no_tag is True.
    """
    # GitHub Actions sets GITHUB_REF_NAME to the tag name on tag push events.
    ref_name = os.environ.get("GITHUB_REF_NAME", "").strip()
    if ref_name and SEMVER_RE.match(ref_name):
        return ref_name

    # Fallback: git describe
    try:
        result = subprocess.run(
            ["git", "describe", "--exact-match", "--tags", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        tag = result.stdout.strip()
        if tag:
            return tag
    except subprocess.CalledProcessError:
        pass

    if allow_no_tag:
        return None

    print("ERROR: No git tag found and --allow-no-tag was not set.", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate git tag / package version parity.")
    parser.add_argument(
        "--allow-no-tag",
        action="store_true",
        default=False,
        help="Exit 0 (pass) when no git tag is present (useful for local dry-runs).",
    )
    args = parser.parse_args()

    tag = get_git_tag(allow_no_tag=args.allow_no_tag)

    if tag is None:
        print("check_tag_version: no tag present — skipping (--allow-no-tag set).")
        sys.exit(0)

    # Normalise: strip leading 'v'
    m = SEMVER_RE.match(tag)
    if not m:
        print(
            f"ERROR: Tag '{tag}' is not a valid semantic version (expected vX.Y.Z or X.Y.Z).",
            file=sys.stderr,
        )
        sys.exit(1)

    tag_version = m.group(1)
    pkg_version = get_package_version()

    if tag_version != pkg_version:
        print(
            f"ERROR: Tag version '{tag_version}' does not match package version '{pkg_version}'.\n"
            f"  pyproject.toml: version = \"{pkg_version}\"\n"
            f"  git tag:        {tag}\n"
            "Update dash_tanstack_pivot/pyproject.toml to match the tag before releasing.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"check_tag_version: OK — tag={tag!r} matches package version={pkg_version!r}")
    sys.exit(0)


if __name__ == "__main__":
    main()

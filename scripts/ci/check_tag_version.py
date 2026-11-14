#!/usr/bin/env python3
"""
check_tag_version.py
--------------------
Deterministic semantic-version guard for CI release workflows.

Reads the current git tag (from GITHUB_REF_NAME or `git describe`)
and compares it against the authoritative root VERSION file, all
synchronized Python/npm package manifests, and CHANGELOG.md.

Fails with a non-zero exit code if:
- The git tag is not a valid semver (vX.Y.Z or X.Y.Z).
- VERSION does not match the git tag.
- Any Python or npm manifest or CHANGELOG.md is out of sync with VERSION.

Usage:
    python scripts/ci/check_tag_version.py
    python scripts/ci/check_tag_version.py --allow-no-tag   # passes when no tag is found (local dev)
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.sync_version import read_authoritative_version, sync_versions  # noqa: E402


SEMVER_RE = re.compile(r"^v?(\d+\.\d+\.\d+)$")


def get_package_version(root: Path = ROOT) -> str:
    """Return the authoritative version from VERSION."""
    return read_authoritative_version(root)


def get_git_tag(allow_no_tag: bool) -> str | None:
    """
    Determine the current git tag.

    Checks GITHUB_REF_NAME first (set by GitHub Actions on tag pushes),
    then falls back to `git describe --exact-match --tags HEAD`.
    Returns None if no tag is found and allow_no_tag is True.
    """
    ref_name = os.environ.get("GITHUB_REF_NAME", "").strip()
    if ref_name and SEMVER_RE.match(ref_name):
        return ref_name

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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Validate git tag / package version parity.")
    parser.add_argument(
        "--allow-no-tag",
        action="store_true",
        default=False,
        help="Exit 0 (pass) when no git tag is present (useful for local dry-runs).",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repository root directory.",
    )
    args = parser.parse_args(argv)

    tag = get_git_tag(allow_no_tag=args.allow_no_tag)

    pkg_version, issues = sync_versions(args.root, check=True, check_changelog=True)
    if issues:
        print("ERROR: Package manifests or CHANGELOG.md are out of sync with VERSION:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        sys.exit(1)

    if tag is None:
        print(
            f"check_tag_version: no tag present — skipping tag comparison "
            f"(--allow-no-tag set; manifests verified at {pkg_version!r})."
        )
        sys.exit(0)

    m = SEMVER_RE.match(tag)
    if not m:
        print(
            f"ERROR: Tag '{tag}' is not a valid semantic version (expected vX.Y.Z or X.Y.Z).",
            file=sys.stderr,
        )
        sys.exit(1)

    tag_version = m.group(1)

    if tag_version != pkg_version:
        print(
            f"ERROR: Tag version '{tag_version}' does not match VERSION '{pkg_version}'.\n"
            f"  VERSION:  {pkg_version}\n"
            f"  git tag:  {tag}\n"
            "Run `python scripts/sync_version.py --set <version>` and update CHANGELOG.md before releasing.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"check_tag_version: OK — tag={tag!r} matches VERSION={pkg_version!r}, "
        "all Python/npm manifests, and CHANGELOG.md"
    )
    sys.exit(0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Synchronize or verify all package versions from the root VERSION file."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Callable


SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
DEFAULT_ROOT = Path(__file__).resolve().parent.parent


def read_authoritative_version(root: Path) -> str:
    """Read and validate the single authoritative version from VERSION."""
    version_path = root / "VERSION"
    if not version_path.is_file():
        raise FileNotFoundError(f"Authoritative VERSION file not found: {version_path}")
    version = version_path.read_text(encoding="utf-8").strip()
    if not SEMVER_RE.fullmatch(version):
        raise ValueError(
            f"Invalid semantic version {version!r} in {version_path} (expected X.Y.Z)."
        )
    return version


def _sync_pyproject(path: Path, version: str, check: bool) -> bool:
    original = path.read_text(encoding="utf-8")
    pattern = re.compile(r'^(version\s*=\s*")([^"]+)(")', re.MULTILINE)
    match = pattern.search(original)
    if not match:
        raise ValueError(f"Could not locate version field in {path}")
    if match.group(2) == version:
        return True
    if check:
        return False
    updated = pattern.sub(rf"\g<1>{version}\3", original, count=1)
    path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def _sync_streamlit_init(path: Path, version: str, check: bool) -> bool:
    original = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'(ComponentManifest\(\s*name="serverside-pivot",\s*version=")([^"]+)(")',
        re.MULTILINE,
    )
    match = pattern.search(original)
    if not match:
        raise ValueError(f"Could not locate ComponentManifest version in {path}")
    if match.group(2) == version:
        return True
    if check:
        return False
    updated = pattern.sub(rf"\g<1>{version}\3", original, count=1)
    path.write_text(updated, encoding="utf-8", newline="\n")
    return True


def _sync_json_version(path: Path, version: str, check: bool) -> bool:
    original = path.read_text(encoding="utf-8")
    data = json.loads(original)
    changed = False

    if data.get("version") != version:
        data["version"] = version
        changed = True

    packages = data.get("packages")
    if isinstance(packages, dict):
        root_pkg = packages.get("")
        if isinstance(root_pkg, dict) and root_pkg.get("version") != version:
            root_pkg["version"] = version
            changed = True
        local_dep = packages.get("node_modules/serverside_pivot")
        if isinstance(local_dep, dict) and local_dep.get("version") != version:
            local_dep["version"] = version
            changed = True

    if not changed:
        return True
    if check:
        return False
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8", newline="\n")
    return True


VERSION_TARGETS: tuple[tuple[str, Callable[[Path, str, bool], bool]], ...] = (
    ("serverside_pivot/pyproject.toml", _sync_pyproject),
    ("serverside_pivot/package.json", _sync_json_version),
    ("serverside_pivot/package-lock.json", _sync_json_version),
    ("serverside_pivot/package-info.json", _sync_json_version),
    ("serverside_pivot/serverside_pivot/package-info.json", _sync_json_version),
    ("serverside_pivot/serverside_pivot/package.json", _sync_json_version),
    ("serverside_pivot/streamlit_pivot/pyproject.toml", _sync_pyproject),
    ("serverside_pivot/streamlit_pivot/__init__.py", _sync_streamlit_init),
    ("serverside_pivot/streamlit_pivot/frontend/package.json", _sync_json_version),
    ("serverside_pivot/streamlit_pivot/frontend/package-lock.json", _sync_json_version),
    ("examples/flagship_react/package.json", _sync_json_version),
    ("examples/flagship_react/package-lock.json", _sync_json_version),
)


def extract_changelog_section(changelog_text: str, version: str) -> str | None:
    """Return the markdown body for ``## [version]`` or ``## version`` if present."""
    header_re = re.compile(
        rf"^##\s+\[?{re.escape(version)}\]?(?:\s+-\s+.*)?\s*$",
        re.MULTILINE,
    )
    match = header_re.search(changelog_text)
    if not match:
        return None
    start = match.end()
    next_header = re.search(r"^##\s+", changelog_text[start:], re.MULTILINE)
    end = start + next_header.start() if next_header else len(changelog_text)
    body = changelog_text[start:end].strip()
    return body or None


def sync_versions(
    root: Path = DEFAULT_ROOT,
    *,
    check: bool = False,
    set_version: str | None = None,
    check_changelog: bool = True,
) -> tuple[str, list[str]]:
    """Sync or verify all manifest versions against ``VERSION``.

    Returns ``(authoritative_version, mismatched_or_missing_items)``.
    """
    if set_version is not None:
        if not SEMVER_RE.fullmatch(set_version):
            raise ValueError(f"Invalid semantic version {set_version!r} (expected X.Y.Z).")
        (root / "VERSION").write_text(f"{set_version}\n", encoding="utf-8", newline="\n")

    version = read_authoritative_version(root)
    issues: list[str] = []

    for rel_path, handler in VERSION_TARGETS:
        target = root / rel_path
        if not target.is_file():
            issues.append(f"missing manifest: {rel_path}")
            continue
        ok = handler(target, version, check=check)
        if not ok:
            issues.append(f"version mismatch: {rel_path} != {version}")

    if check and check_changelog:
        changelog_path = root / "CHANGELOG.md"
        if not changelog_path.is_file():
            issues.append("missing CHANGELOG.md")
        else:
            section = extract_changelog_section(
                changelog_path.read_text(encoding="utf-8"), version
            )
            if not section:
                issues.append(f"missing or empty CHANGELOG.md section for {version}")

    return version, issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify all manifests and CHANGELOG.md match VERSION without modifying files.",
    )
    parser.add_argument(
        "--set",
        dest="set_version",
        default=None,
        help="Set VERSION to X.Y.Z and propagate it to all package manifests.",
    )
    parser.add_argument(
        "--skip-changelog",
        action="store_true",
        help="Skip CHANGELOG.md section verification during --check.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Repository root directory.",
    )
    args = parser.parse_args(argv)

    if args.check and args.set_version is not None:
        parser.error("--check and --set cannot be used together.")

    try:
        version, issues = sync_versions(
            args.root,
            check=args.check,
            set_version=args.set_version,
            check_changelog=not args.skip_changelog,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"sync_version: ERROR - {error}", file=sys.stderr)
        return 1

    if issues:
        print("sync_version: FAILED", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        if args.check:
            print("Run `python scripts/sync_version.py` to synchronize manifests.", file=sys.stderr)
        return 1

    mode = "verified" if args.check else "synchronized"
    print(f"sync_version: {mode} all manifests at {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Validate a packed standalone React distribution, not its source tree."""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import sys
import tarfile
from pathlib import Path


_PACKAGE_PREFIX = "package/"
_REQUIRED_MEMBERS = frozenset(
    {
        "package/package.json",
        "package/dist/react/index.mjs",
        "package/dist/react/react.d.ts",
    }
)
_IMPORT_PATTERN = re.compile(r"(?:from\s*|import\s*\()\s*['\"]([^'\"]+)['\"]")


def _member_for_relative(source_name: str, relative: str) -> str | None:
    if not relative.startswith("."):
        return None
    normalized = posixpath.normpath(
        posixpath.join(posixpath.dirname(source_name), relative)
    )
    if not normalized.startswith(_PACKAGE_PREFIX):
        return None
    return normalized


def verify_npm_package(archive_path: Path) -> list[str]:
    """Return consumer-visible package contract violations."""
    try:
        archive = tarfile.open(archive_path, "r:gz")
    except (OSError, tarfile.TarError) as error:
        return [f"cannot open npm package archive: {error}"]
    with archive:
        members = {
            member.name: member
            for member in archive.getmembers()
            if member.isfile()
        }
        errors = [
            f"missing required npm package member: {name}"
            for name in sorted(_REQUIRED_MEMBERS - set(members))
        ]
        if errors:
            return errors
        try:
            package_json = json.load(archive.extractfile(members["package/package.json"]))
        except (AttributeError, OSError, json.JSONDecodeError):
            return ["npm package package.json is invalid."]
        exports = package_json.get("exports") if isinstance(package_json, dict) else None
        for export_name in (".", "./react"):
            target = exports.get(export_name) if isinstance(exports, dict) else None
            if not isinstance(target, dict) or target.get("import") != "./dist/react/index.mjs":
                errors.append(f"npm package export {export_name} does not expose the React entry.")

        pending = ["package/dist/react/index.mjs"]
        visited: set[str] = set()
        while pending:
            source_name = pending.pop()
            if source_name in visited:
                continue
            visited.add(source_name)
            try:
                source = archive.extractfile(members[source_name]).read().decode("utf-8")
            except (AttributeError, OSError, UnicodeDecodeError):
                errors.append(f"cannot read npm package module: {source_name}")
                continue
            for relative in _IMPORT_PATTERN.findall(source):
                target_name = _member_for_relative(source_name, relative)
                if target_name is None:
                    continue
                if target_name not in members:
                    errors.append(f"npm package module is missing: {target_name}")
                else:
                    pending.append(target_name)
        return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path, help="npm pack .tgz archive")
    args = parser.parse_args(argv)
    errors = verify_npm_package(args.archive)
    if errors:
        print("npm-package-contract: FAILED", file=sys.stderr)
        print("\n".join(f"- {error}" for error in errors), file=sys.stderr)
        return 1
    print("npm-package-contract: standalone React package is complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

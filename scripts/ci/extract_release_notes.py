#!/usr/bin/env python3
"""Extract release notes for a version from CHANGELOG.md."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.sync_version import extract_changelog_section, read_authoritative_version  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version",
        default=None,
        help="Semantic version (X.Y.Z or vX.Y.Z). Defaults to root VERSION.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repository root directory.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output path for the extracted markdown.",
    )
    args = parser.parse_args(argv)

    version = (
        args.version.lstrip("v")
        if args.version
        else read_authoritative_version(args.root)
    )
    changelog_path = args.root / "CHANGELOG.md"
    if not changelog_path.is_file():
        print(f"ERROR: CHANGELOG.md not found at {changelog_path}", file=sys.stderr)
        return 1

    body = extract_changelog_section(
        changelog_path.read_text(encoding="utf-8"), version
    )
    if not body:
        print(
            f"ERROR: No non-empty release notes found in CHANGELOG.md for {version}.",
            file=sys.stderr,
        )
        return 1

    content = f"## v{version}\n\n{body}\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content, encoding="utf-8", newline="\n")
    else:
        print(content, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""The dependency version matrix the semantic core is certified on (M5).

One source of truth for the CI ``version-matrix`` job: every supported range in
``serverside_pivot/pyproject.toml`` is exercised at its floor and at its
ceiling, together with the optional Ibis backends the cross-backend suite runs
on (SQLite, Polars).

    python scripts/ci/version_matrix.py --list            # GitHub matrix JSON
    python scripts/ci/version_matrix.py --pins floor      # pip requirement pins
    python scripts/ci/version_matrix.py --verify floor    # installed == pinned
"""

from __future__ import annotations

import argparse
import json
import sys
from importlib import metadata
from typing import Dict

# Floors are the lower bounds in pyproject.toml; ceilings are the newest
# release inside each upper bound (update when a new patch release ships).
# PyArrow and the Polars engine are resolved by pip within their ranges.
MATRIX: Dict[str, Dict[str, str]] = {
    "floor": {"duckdb": "1.5.0", "ibis-framework": "12.0.0", "sqlglot": "29.0.0"},
    "ceiling": {"duckdb": "1.5.6", "ibis-framework": "12.0.0", "sqlglot": "29.0.1"},
}

# The certification suites each combination must pass.
SUITES = (
    "tests/core",
    "tests/conformance",
    "tests/oracle",
    "tests/security",
    "tests/test_streaming_diff_correctness.py",
    "tests/test_streaming_differential_fuzz.py",
)


def pins(name: str) -> str:
    extras = {"ibis-framework": "ibis-framework[duckdb,sqlite,polars]"}
    return " ".join(f"{extras.get(pkg, pkg)}=={version}" for pkg, version in sorted(MATRIX[name].items()))


def verify(name: str) -> int:
    wrong = {}
    for pkg, version in MATRIX[name].items():
        try:
            installed = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            installed = None
        if installed != version:
            wrong[pkg] = (installed, version)
    for pkg, (installed, version) in sorted(wrong.items()):
        print(f"{pkg}: installed {installed}, matrix {name!r} pins {version}", file=sys.stderr)
    return 1 if wrong else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true")
    group.add_argument("--pins", choices=sorted(MATRIX))
    group.add_argument("--verify", choices=sorted(MATRIX))
    group.add_argument("--suites", action="store_true")
    args = parser.parse_args(argv)
    if args.list:
        print(json.dumps({"combination": sorted(MATRIX)}))
    elif args.pins:
        print(pins(args.pins))
    elif args.suites:
        print(" ".join(SUITES))
    else:
        return verify(args.verify)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

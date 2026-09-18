#!/usr/bin/env python3
"""
verify_bundle_contract.py
--------------------------
Asserts that the expected minified bundle files exist after `npm run build`
inside dash_tanstack_pivot/.

The contract: after a successful build, these files must be present:
  dash_tanstack_pivot/dash_tanstack_pivot/dash_tanstack_pivot.min.js

The contract establishes separate budgets for the initial bundle, normal lazy
features, and the two intentionally heavy opt-in dependencies. Keeping them
here makes regressions visible in CI before a release reaches Dash users.

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
    "metadata.json",
    "package-info.json",
]

# The initial viewer, normal charts, and report builder should stay lean. XLSX
# and ECharts/WebGL are deliberately lazy because they are only needed after a
# user starts an export or selects a specialised chart type.
DEFAULT_MAX_MAIN_BYTES = 1_100_000
DEFAULT_MAX_STANDARD_ASYNC_BYTES = 300_000
DEFAULT_MAX_ASYNC_BYTES = 1_750_000
OPTIONAL_ASYNC_BUDGETS = {
    # ECharts shares its rendering core between 2D and WebGL feature chunks.
    # The shared chunk is still marked `dynamic` in the Dash package metadata,
    # so it is fetched only after a chart feature is selected.
    "dash_tanstack_pivot_shared.dash_tanstack_pivot.min.js": 550_000,
    "dash_tanstack_pivot_async_pivot-echarts-2d.dash_tanstack_pivot.min.js": 100_000,
    "dash_tanstack_pivot_async_pivot-echarts-3d.dash_tanstack_pivot.min.js": 500_000,
    # Standard workbook export is writer-only; the legacy HTML-table parser
    # is emitted as a second, explicitly requested compatibility chunk.
    # Current writer and legacy-parser releases require these tightly bounded
    # limits. Both dependencies remain opt-in lazy chunks.
    "dash_tanstack_pivot_async_pivot-xlsx.dash_tanstack_pivot.min.js": 275_000,
    "dash_tanstack_pivot_async_pivot-xlsx-html.dash_tanstack_pivot.min.js": 500_000,
}


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
    parser.add_argument(
        "--max-main-bytes",
        type=int,
        default=DEFAULT_MAX_MAIN_BYTES,
        help="Maximum allowed size of the initial minified bundle.",
    )
    parser.add_argument(
        "--max-async-bytes",
        type=int,
        default=DEFAULT_MAX_ASYNC_BYTES,
        help="Absolute maximum allowed size of any asynchronous minified bundle.",
    )
    parser.add_argument(
        "--max-standard-async-bytes",
        type=int,
        default=DEFAULT_MAX_STANDARD_ASYNC_BYTES,
        help="Maximum allowed size for a normal (non opt-in dependency) async bundle.",
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
    for artifact in REQUIRED_ARTIFACTS:
        path = bundle_dir / artifact
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print(f"  FOUND (required): {artifact} ({size_kb:.1f} KB)")
        else:
            errors.append(f"MISSING (required): {artifact}")
            print(f"  MISSING (required): {artifact}", file=sys.stderr)

    main_bundle = bundle_dir / "dash_tanstack_pivot.min.js"
    if main_bundle.exists():
        main_size = main_bundle.stat().st_size
        print(f"  SIZE (main): {main_size / 1024 / 1024:.2f} MiB")
        if main_size > args.max_main_bytes:
            errors.append(
                f"Initial bundle is {main_size} bytes; budget is {args.max_main_bytes}."
            )

    async_bundles = sorted(
        path
        for path in bundle_dir.glob("*.dash_tanstack_pivot.min.js")
        if path.name != main_bundle.name
    )
    if not async_bundles:
        errors.append("No asynchronous bundle was emitted; chart code must remain lazy-loaded.")
    for bundle in async_bundles:
        size = bundle.stat().st_size
        print(f"  SIZE (async): {bundle.name} ({size / 1024 / 1024:.2f} MiB)")
        if size > args.max_async_bytes:
            errors.append(
                f"Async bundle {bundle.name} is {size} bytes; absolute budget is {args.max_async_bytes}."
            )
        feature_budget = OPTIONAL_ASYNC_BUDGETS.get(
            bundle.name, args.max_standard_async_bytes
        )
        if size > feature_budget:
            errors.append(
                f"Async bundle {bundle.name} is {size} bytes; feature budget is {feature_budget}."
            )

    if errors:
        print(
            f"\nverify_bundle_contract: FAILED — {len(errors)} contract violation(s).",
            file=sys.stderr,
        )
        sys.exit(1)

    print("\nverify_bundle_contract: OK — artifacts and bundle budgets are valid.")

    sys.exit(0)


if __name__ == "__main__":
    main()

"""Verify the build outputs required by every advertised UI host."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _streamlit_errors(build_dir: Path) -> list[str]:
    manifest_path = build_dir / "pivot-workspace-manifest.json"
    if not manifest_path.is_file():
        return [f"missing Streamlit build manifest: {manifest_path}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return [f"invalid Streamlit build manifest: {manifest_path}"]
    if not isinstance(manifest, dict):
        return [f"invalid Streamlit build manifest: {manifest_path}"]

    errors: list[str] = []
    entry = manifest.get("src/index.tsx")
    if not isinstance(entry, dict) or not isinstance(entry.get("file"), str):
        errors.append("Streamlit manifest has no src/index.tsx entry.")
    for manifest_key, artifact in manifest.items():
        if not isinstance(artifact, dict):
            errors.append(f"Streamlit manifest entry is invalid: {manifest_key}")
            continue
        file_name = artifact.get("file")
        if not isinstance(file_name, str) or not file_name or Path(file_name).is_absolute():
            errors.append(f"Streamlit manifest entry has no valid file: {manifest_key}")
            continue
        if not (build_dir / file_name).is_file():
            errors.append(f"missing Streamlit build asset: {file_name}")
    return errors


def verify_host_build(package_dir: Path) -> list[str]:
    """Return host-build violations without relying on a developer's cache."""
    react_dir = package_dir / "dist" / "react"
    errors = [
        f"missing React package artifact: {react_dir / name}"
        for name in ("index.mjs", "react.d.ts")
        if not (react_dir / name).is_file()
    ]
    errors.extend(
        _streamlit_errors(package_dir / "streamlit_pivot" / "frontend" / "build")
    )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    errors = verify_host_build(args.package_dir)
    if errors:
        print("host-build-contract: FAILED", file=sys.stderr)
        print("\n".join(f"- {error}" for error in errors), file=sys.stderr)
        return 1
    print("host-build-contract: React and Streamlit assets are complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

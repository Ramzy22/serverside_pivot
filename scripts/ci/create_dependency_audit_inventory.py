"""Attest the first-party editable checkout and pin every installed third party."""

from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit
from urllib.request import url2pathname

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 development environments
    import tomli as tomllib

from packaging.version import InvalidVersion, Version


def _name(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", value):
        raise ValueError(f"Invalid installed distribution name: {value!r}")
    return re.sub(r"[-_.]+", "-", value).lower()


def _version(value: str) -> str:
    try:
        Version(value)
    except (InvalidVersion, TypeError) as exc:
        raise ValueError(f"Invalid installed distribution version: {value!r}") from exc
    if any(char.isspace() for char in value):
        raise ValueError(f"Invalid installed distribution version: {value!r}")
    return value


def create_inventory(project_root: Path, distributions=None) -> tuple[list[str], dict]:
    """Exclude exactly one verified checkout; never omit unknown third parties."""
    project_root = project_root.resolve(strict=True)
    project_bytes = (project_root / "pyproject.toml").read_bytes()
    project = tomllib.loads(project_bytes.decode("utf-8"))["project"]
    project_name = _name(project["name"])
    project_version = _version(project["version"])
    installed = list(metadata.distributions() if distributions is None else distributions)
    candidates = [dist for dist in installed if _name(dist.metadata.get("Name")) == project_name]
    if len(candidates) != 1:
        raise ValueError("Expected exactly one installed first-party distribution.")
    first_party = candidates[0]
    if first_party.version != project_version:
        raise ValueError("Installed first-party version does not match pyproject.toml.")
    try:
        direct_url = json.loads(first_party.read_text("direct_url.json") or "null")
        if not isinstance(direct_url, dict) or direct_url.get("dir_info", {}).get("editable") is not True:
            raise ValueError("First-party installation must be editable.")
        url = urlsplit(direct_url["url"])
        if url.scheme != "file" or url.netloc not in ("", "localhost") or url.query or url.fragment:
            raise ValueError("First-party origin must be a local file URI.")
        origin = Path(url2pathname(url.path)).resolve(strict=True)
        if origin != project_root:
            raise ValueError("First-party origin does not match the project root.")
    except (KeyError, TypeError, AttributeError, json.JSONDecodeError, OSError) as exc:
        raise ValueError("Cannot attest first-party direct_url.json.") from exc

    third_party = {}
    for dist in installed:
        if dist is first_party:
            continue
        name = _name(dist.metadata.get("Name"))
        version = _version(dist.version)
        if name in third_party and third_party[name] != version:
            raise ValueError(f"Installed distribution has conflicting versions: {name}")
        third_party[name] = version
    commit = subprocess.check_output(
        ["git", "-C", str(project_root), "rev-parse", "HEAD"], text=True
    ).strip()
    provenance = {
        "name": project_name,
        "version": project_version,
        "project_root": str(project_root),
        "direct_url": direct_url,
        "source_commit": commit,
        "pyproject_sha256": hashlib.sha256(project_bytes).hexdigest(),
        "qualification": "first-party source: Bandit, full environment SBOM, and release qualification gates",
        "third_party_distribution_count": len(third_party),
    }
    return [f"{name}=={version}" for name, version in sorted(third_party.items())], provenance


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        pins, provenance = create_inventory(args.project_root)
        args.requirements.write_text("\n".join(pins) + "\n", encoding="utf-8")
        args.provenance.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        print(f"dependency inventory: {exc}", file=sys.stderr)
        return 2
    print(f"Attested {provenance['name']}@{provenance['version']}; audit {len(pins)} installed third parties.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

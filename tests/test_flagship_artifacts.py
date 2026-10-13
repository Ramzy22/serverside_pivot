"""Release artifact binding for the three installed flagship hosts."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

import pytest

from scripts.flagship_artifacts import (
    ArtifactBinding,
    build_artifacts,
    start_installed_hosts,
    verify_loaded_asset,
)


ROOT = Path(__file__).resolve().parents[1]


def test_loaded_asset_rejects_bytes_other_than_the_bound_release(tmp_path: Path) -> None:
    asset = tmp_path / "assets" / "entry.js"
    asset.parent.mkdir()
    asset.write_bytes(b"known production bundle")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": digest}})

    assert verify_loaded_asset(binding, "react", "/assets/entry.js", b"known production bundle") == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(binding, "react", "/assets/entry.js", b"changed bundle")


def test_loaded_asset_rejects_vite_development_client() -> None:
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": "0" * 64}})
    with pytest.raises(ValueError, match="Vite development client"):
        verify_loaded_asset(binding, "react", "/@vite/client", b"dev")


def test_loaded_asset_requires_a_recorded_path() -> None:
    binding = ArtifactBinding(expected_loaded_assets={"react": {"/assets/entry.js": "0" * 64}})
    with pytest.raises(ValueError, match="unbound asset"):
        verify_loaded_asset(binding, "react", "/assets/other.js", b"anything")


@pytest.mark.skipif(os.environ.get("PIVOT_RUN_ARTIFACT_TESTS") != "1", reason="real release build is opt-in")
def test_clean_artifacts_serve_installed_hosts_and_match_loaded_bytes(tmp_path: Path) -> None:
    binding = build_artifacts(ROOT, tmp_path / "release")
    assert binding.source_commit and len(binding.source_commit) == 40
    assert binding.sha256["wheel"] and binding.sha256["npm_tarball"]
    assert binding.sha256["fixture"] == hashlib.sha256(
        (ROOT / "tests" / "data" / "flagship_parity_rows.json").read_bytes()
    ).hexdigest()
    assert binding.expected_loaded_assets["dash"]
    assert binding.expected_loaded_assets["react"]
    assert binding.expected_loaded_assets["streamlit"]

    with zipfile.ZipFile(binding.paths["wheel"]) as wheel:
        names = wheel.namelist()
        assert any(name.startswith("streamlit_pivot/frontend/build/") and name.endswith(".js") for name in names)
        assert any(name.startswith("dash_tanstack_pivot/") and name.endswith(".min.js") for name in names)

    installed_origin = subprocess.check_output(
        [str(binding.paths["python"]), "-c", "import streamlit_pivot; print(streamlit_pivot.__file__)"],
        cwd=binding.paths["app"], text=True,
    ).strip()
    assert str(binding.paths["app"]) not in installed_origin
    assert "site-packages" in installed_origin

    with start_installed_hosts(binding, tmp_path / "hosts") as urls:
        for host in ("dash", "react", "streamlit"):
            with urllib.request.urlopen(urls[host], timeout=10) as response:
                assert response.status == 200
        with urllib.request.urlopen(urls["react"], timeout=10) as response:
            index = response.read()
        assert b"/@vite/client" not in index
        react_path = next(path for path in binding.expected_loaded_assets["react"] if path.endswith(".js"))
        with urllib.request.urlopen(urls["react"] + react_path, timeout=10) as response:
            verify_loaded_asset(binding, "react", react_path, response.read())

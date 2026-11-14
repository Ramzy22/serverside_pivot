"""Tests for single-source versioning, release workflow parity, and release notes."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from scripts import sync_version
from scripts.ci import check_tag_version, extract_release_notes, verify_published_release


ROOT = Path(__file__).resolve().parent.parent


def _copy_version_tree(tmp_path: Path) -> Path:
    for rel_path, _ in sync_version.VERSION_TARGETS:
        src = ROOT / rel_path
        dst = tmp_path / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    shutil.copy2(ROOT / "VERSION", tmp_path / "VERSION")
    shutil.copy2(ROOT / "CHANGELOG.md", tmp_path / "CHANGELOG.md")
    return tmp_path


def test_repo_manifests_and_changelog_match_authoritative_version() -> None:
    version, issues = sync_version.sync_versions(ROOT, check=True, check_changelog=True)
    assert version == (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    assert issues == []


def test_sync_version_sets_and_detects_drift(tmp_path: Path) -> None:
    repo = _copy_version_tree(tmp_path)
    (repo / "CHANGELOG.md").write_text(
        "# Changelog\n\n## [0.0.99] - 2026-11-13\n\n### Added\n- Test release.\n",
        encoding="utf-8",
    )

    version, issues = sync_version.sync_versions(repo, set_version="0.0.99")
    assert version == "0.0.99"
    assert issues == []

    _, check_issues = sync_version.sync_versions(repo, check=True, check_changelog=True)
    assert check_issues == []

    # Introduce drift in npm package.json only.
    pkg_json = repo / "serverside_pivot" / "package.json"
    data = json.loads(pkg_json.read_text(encoding="utf-8"))
    data["version"] = "0.0.98"
    pkg_json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    _, drift_issues = sync_version.sync_versions(repo, check=True, check_changelog=True)
    assert drift_issues == ["version mismatch: serverside_pivot/package.json != 0.0.99"]


def test_check_tag_version_accepts_matching_tag_and_rejects_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _copy_version_tree(tmp_path)
    version = sync_version.read_authoritative_version(repo)

    monkeypatch.setenv("GITHUB_REF_NAME", f"v{version}")
    with pytest.raises(SystemExit) as exc:
        check_tag_version.main(["--root", str(repo)])
    assert exc.value.code == 0

    monkeypatch.setenv("GITHUB_REF_NAME", "v9.9.9")
    with pytest.raises(SystemExit) as exc:
        check_tag_version.main(["--root", str(repo)])
    assert exc.value.code == 1


def test_extract_release_notes_reads_current_version_and_fails_on_missing(tmp_path: Path) -> None:
    out_file = tmp_path / "notes.md"
    rc = extract_release_notes.main(["--root", str(ROOT), "--output", str(out_file)])
    assert rc == 0
    content = out_file.read_text(encoding="utf-8")
    version = sync_version.read_authoritative_version(ROOT)
    assert content.startswith(f"## v{version}\n")
    assert len(content.splitlines()) >= 3

    assert extract_release_notes.main(["--root", str(ROOT), "--version", "9.9.9"]) == 1


def test_verify_published_release_wait_and_commit_check(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"pypi": 0, "npm": 0}

    def fake_fetch(url: str, timeout: float = 15.0):
        if "pypi.org" in url:
            calls["pypi"] += 1
            return {"info": {"version": "0.0.91"}}
        calls["npm"] += 1
        return {"version": "0.0.91", "gitHead": "abc123"}

    monkeypatch.setattr(verify_published_release, "_fetch_json", fake_fetch)
    pypi_data, npm_data = verify_published_release.wait_for_registry_versions(
        pypi_package="serverside-pivot",
        npm_package="serverside_pivot",
        version="0.0.91",
        timeout_seconds=5,
        poll_interval=0,
    )
    assert pypi_data["info"]["version"] == "0.0.91"
    assert npm_data["gitHead"] == "abc123"


def test_release_and_ci_workflows_enforce_dual_registry_publish_and_verification() -> None:
    release_text = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "skipping npm publish" not in release_text

    release_wf = yaml.safe_load(release_text)
    steps = release_wf["jobs"]["validate-and-publish"]["steps"]
    step_names = [step.get("name", "") for step in steps]

    assert "Require npm publish credentials before publishing" in step_names
    assert "Publish to PyPI" in step_names
    assert "Publish to npm" in step_names
    assert "Verify published PyPI and npm artifacts in clean environments" in step_names
    assert "Extract and verify release notes" in step_names
    assert "Publish GitHub Release notes" in step_names

    assert (
        step_names.index("Require npm publish credentials before publishing")
        < step_names.index("Publish to PyPI")
        < step_names.index("Publish to npm")
        < step_names.index("Verify published PyPI and npm artifacts in clean environments")
    )

    ci_wf = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    ci_steps = ci_wf["jobs"]["python-tests"]["steps"]
    assert any(
        "scripts/sync_version.py --check" in step.get("run", "")
        for step in ci_steps
    )

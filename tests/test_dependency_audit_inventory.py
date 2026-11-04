"""Audit every installed third party while attesting the unpublished checkout."""

import hashlib
import importlib.util
from importlib.metadata import PathDistribution
import json
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_helper():
    path = ROOT / "scripts/ci/create_dependency_audit_inventory.py"
    assert path.exists(), "installed audit inventory helper has not been implemented"
    spec = importlib.util.spec_from_file_location("audit_inventory", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "checkout with spaces"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "serverside-pivot"\nversion = "0.0.89"\n', encoding="utf-8"
    )
    subprocess.run(["git", "init", str(root)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "add", "pyproject.toml"], check=True)
    subprocess.run(
        ["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
         "commit", "-m", "fixture"], check=True, capture_output=True,
    )
    return root


def distribution(tmp_path, name, version, direct_url=None, suffix=""):
    info = tmp_path / f"{name}-{version}{suffix}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Name: {name}\nVersion: {version}\n", encoding="utf-8")
    if direct_url is not None:
        (info / "direct_url.json").write_text(json.dumps(direct_url), encoding="utf-8")
    return PathDistribution(info)


def first_party(tmp_path, project, **overrides):
    values = dict(name="serverside-pivot", version="0.0.89",
                  direct_url={"url": project.as_uri(), "dir_info": {"editable": True}})
    values.update(overrides)
    return distribution(tmp_path, **values)


def test_only_attested_checkout_is_excluded_and_build_tools_are_included(project, tmp_path):
    helper = load_helper()
    installed = [first_party(tmp_path, project)]
    installed += [distribution(tmp_path, name, "1.2.3") for name in
                  ("pip", "setuptools", "wheel", "unknown-local", "Third_Party")]
    pins, provenance = helper.create_inventory(project, installed)
    assert pins == ["pip==1.2.3", "setuptools==1.2.3", "third-party==1.2.3",
                    "unknown-local==1.2.3", "wheel==1.2.3"]
    assert provenance["name"] == "serverside-pivot"
    assert provenance["version"] == "0.0.89"
    assert provenance["project_root"] == str(project.resolve())
    assert provenance["pyproject_sha256"] == hashlib.sha256((project / "pyproject.toml").read_bytes()).hexdigest()
    assert provenance["source_commit"] == subprocess.check_output(
        ["git", "-C", str(project), "rev-parse", "HEAD"], text=True).strip()
    assert provenance["direct_url"]["dir_info"]["editable"] is True


@pytest.mark.parametrize("case", ["root", "version", "name", "noneditable", "missing", "remote", "query"])
def test_first_party_requires_matching_editable_checkout(project, tmp_path, case):
    helper = load_helper()
    options = {}
    if case == "root":
        options["direct_url"] = {"url": tmp_path.as_uri(), "dir_info": {"editable": True}}
    elif case == "version":
        options["version"] = "0.0.88"
    elif case == "name":
        options["name"] = "lookalike"
    elif case == "noneditable":
        options["direct_url"] = {"url": project.as_uri(), "dir_info": {"editable": False}}
    elif case == "missing":
        options["direct_url"] = None
    elif case == "remote":
        options["direct_url"] = {"url": "https://example.invalid/project", "dir_info": {"editable": True}}
    elif case == "query":
        options["direct_url"] = {"url": project.as_uri() + "?query", "dir_info": {"editable": True}}
    with pytest.raises(ValueError):
        helper.create_inventory(project, [first_party(tmp_path, project, **options)])


def test_unknown_local_third_party_is_retained_for_strict_audit(project, tmp_path):
    helper = load_helper()
    local = distribution(tmp_path, "unpublished-third-party", "2.0+local", {
        "url": tmp_path.as_uri(), "dir_info": {"editable": True}})
    pins, _ = helper.create_inventory(project, [first_party(tmp_path, project), local])
    assert pins == ["unpublished-third-party==2.0+local"]


def test_conflicting_normalized_distribution_versions_fail(project, tmp_path):
    helper = load_helper()
    installed = [first_party(tmp_path, project), distribution(tmp_path, "Other_Pkg", "1"),
                 distribution(tmp_path, "other-pkg", "2")]
    with pytest.raises(ValueError, match="conflicting"):
        helper.create_inventory(project, installed)


def test_duplicate_first_party_metadata_fails(project, tmp_path):
    helper = load_helper()
    installed = [first_party(tmp_path, project), first_party(tmp_path, project, suffix="-duplicate")]
    with pytest.raises(ValueError, match="exactly one"):
        helper.create_inventory(project, installed)


def test_command_writes_pinned_inventory_and_first_party_provenance(project, tmp_path, monkeypatch):
    helper = load_helper()
    installed = [first_party(tmp_path, project), distribution(tmp_path, "pip", "26.0")]
    monkeypatch.setattr(helper.metadata, "distributions", lambda: iter(installed))
    pins = tmp_path / "requirements.txt"
    provenance = tmp_path / "provenance.json"
    assert helper.main(["--project-root", str(project), "--requirements", str(pins),
                        "--provenance", str(provenance)]) == 0
    assert pins.read_text(encoding="utf-8") == "pip==26.0\n"
    assert json.loads(provenance.read_text(encoding="utf-8"))["third_party_distribution_count"] == 1


def test_command_fails_without_writing_inventory_for_untrusted_checkout(project, tmp_path, monkeypatch):
    helper = load_helper()
    monkeypatch.setattr(helper.metadata, "distributions", lambda: [first_party(tmp_path, project, version="0.0.88")])
    pins = tmp_path / "requirements.txt"
    provenance = tmp_path / "provenance.json"
    assert helper.main(["--project-root", str(project), "--requirements", str(pins),
                        "--provenance", str(provenance)]) == 2
    assert not pins.exists()
    assert not provenance.exists()


@pytest.mark.parametrize("workflow", ["security.yml", "release.yml"])
def test_workflows_strictly_audit_inventory_and_upload_provenance(workflow):
    text = (ROOT / ".github/workflows" / workflow).read_text(encoding="utf-8")
    assert "python scripts/ci/create_dependency_audit_inventory.py" in text
    assert "--requirements security/reports/third-party-requirements.txt" in text
    assert "--provenance security/reports/first-party-provenance.json" in text
    assert "pip-audit -r security/reports/third-party-requirements.txt --no-deps --disable-pip --strict" in text
    assert "path: security/reports/" in text

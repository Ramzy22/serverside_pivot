"""Release artifact binding for the three installed flagship hosts."""

from __future__ import annotations

import hashlib
import base64
import json
import os
import subprocess
import urllib.request
import zipfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

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


def test_dash_fingerprinted_asset_verifies_the_wheel_bytes() -> None:
    digest = hashlib.sha256(b"installed Dash component").hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={
        "dash": {"/_dash-component-suites/dash_tanstack_pivot/dash_tanstack_pivot.min.js": digest},
    })
    url = "/_dash-component-suites/dash_tanstack_pivot/dash_tanstack_pivot.v0_0_88mabc123.min.js"
    assert verify_loaded_asset(binding, "dash", url, b"installed Dash component") == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(binding, "dash", url, b"stale component")


def test_streamlit_component_url_verifies_the_wheel_asset_bytes() -> None:
    digest = hashlib.sha256(b"installed component").hexdigest()
    binding = ArtifactBinding(expected_loaded_assets={
        "streamlit": {"/streamlit_pivot/frontend/build/pivot-workspace-123.js": digest},
    })
    assert verify_loaded_asset(
        binding, "streamlit", "/_stcore/component/serverside-pivot/pivot-workspace-123.js", b"installed component"
    ) == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_loaded_asset(
            binding, "streamlit", "/_stcore/component/serverside-pivot/pivot-workspace-123.js", b"changed"
        )


@pytest.mark.parametrize("conflicting_flag", ["--reuse-running", "--skip-build", "--export-qualification"])
def test_installed_release_rejects_flags_that_bypass_its_fresh_build(tmp_path: Path, conflicting_flag: str) -> None:
    from scripts import certify_flagship

    assert certify_flagship.main([
        "--installed-release", conflicting_flag, "--artifact-dir", str(tmp_path / "evidence"),
    ]) == 1
    assert not (tmp_path / "evidence").exists()


def test_installed_release_binds_browser_environment_and_preserves_failure(tmp_path: Path, monkeypatch) -> None:
    from scripts import certify_flagship
    from scripts import flagship_artifacts

    binding = ArtifactBinding(source_commit="a" * 40, sha256={"fixture": "b" * 64})
    monkeypatch.setattr(flagship_artifacts, "build_artifacts", lambda source, output: binding)

    @contextmanager
    def installed_hosts(actual_binding, output):
        assert actual_binding is binding
        yield {"dash": "http://127.0.0.1:39001", "react": "http://127.0.0.1:39002", "streamlit": "http://127.0.0.1:8502"}

    monkeypatch.setattr(flagship_artifacts, "start_installed_hosts", installed_hosts)

    def browser_suite(evidence, environment):
        assert environment["PIVOT_FLAGSHIP_REACT_URL"] == "http://127.0.0.1:39002"
        assert environment["PIVOT_FLAGSHIP_DASH_URL"] == "http://127.0.0.1:39001"
        assert environment["PIVOT_FLAGSHIP_STREAMLIT_URL"] == "http://127.0.0.1:8502"
        assert "PYTHONPATH" not in environment
        record = json.loads(Path(environment["PIVOT_FLAGSHIP_ARTIFACT_BINDING"]).read_text())
        assert record["sourceCommit"] == "a" * 40
        assert record["sha256"]["fixture"] == "b" * 64
        assert "paths" not in record
        return 7

    monkeypatch.setattr(certify_flagship, "_run_browser_suite", browser_suite)
    evidence = tmp_path / "evidence"
    assert certify_flagship.main(["--installed-release", "--artifact-dir", str(evidence)]) == 7
    assert json.loads((evidence / "flagship-certification.json").read_text())["result"] == "failed"


def _assert_browser_loaded_bound_assets(binding: ArtifactBinding, urls: dict[str, str], evidence: Path) -> None:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait

    options = Options()
    for argument in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-background-networking", "--disable-component-update", "--window-size=1680,1200"):
        options.add_argument(argument)
    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})
    observations = {}
    for host, url in urls.items():
        driver = webdriver.Chrome(options=options)
        try:
            driver.execute_cdp_cmd("Network.enable", {"maxTotalBufferSize": 50000000, "maxResourceBufferSize": 50000000})
            driver.get(url)
            cells = WebDriverWait(driver, 45).until(lambda browser: browser.execute_script("""
                const find = root => {
                    const cells = [...root.querySelectorAll('[role="gridcell"]')];
                    for (const element of root.querySelectorAll('*')) {
                        if (element.shadowRoot) cells.push(...find(element.shadowRoot));
                    }
                    return cells;
                };
                return find(document).map(cell => cell.textContent).filter(text => text.trim());
            """))
            loaded = {}
            for entry in driver.get_log("performance"):
                event = json.loads(entry["message"])["message"]
                if event["method"] != "Network.responseReceived":
                    continue
                resource = event["params"]["response"]["url"]
                path = urlsplit(resource).path
                assert not path.startswith("/@vite/"), resource
                own_asset = (
                    host == "react" and path.startswith("/assets/")
                    or host == "dash" and path.startswith("/_dash-component-suites/dash_tanstack_pivot/")
                    or host == "streamlit" and Path(path).name in {Path(name).name for name in binding.expected_loaded_assets[host]}
                )
                if own_asset:
                    response = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": event["params"]["requestId"]})
                    body = base64.b64decode(response["body"]) if response.get("base64Encoded") else response["body"].encode()
                    loaded[path] = verify_loaded_asset(binding, host, resource, body)
            assert loaded, f"No installed {host} component assets observed"
            observations[host] = {"browserVersion": driver.capabilities["browserVersion"], "cellCount": len(cells), "loadedAssetSha256": loaded}
        finally:
            driver.quit()
    (evidence / "browser-assets.json").write_text(json.dumps({
        "sourceCommit": binding.source_commit, "sha256": binding.sha256,
        "observations": observations,
    }, indent=2), encoding="utf-8")


@pytest.mark.skipif(os.environ.get("PIVOT_RUN_ARTIFACT_TESTS") != "1", reason="real release build is opt-in")
def test_clean_artifacts_serve_installed_hosts_and_match_loaded_bytes(tmp_path: Path) -> None:
    binding = build_artifacts(ROOT, tmp_path / "release")
    assert binding.source_commit and len(binding.source_commit) == 40
    assert binding.sha256["wheel"] and binding.sha256["npm_tarball"]
    committed_fixture = subprocess.check_output(
        ["git", "show", f"{binding.source_commit}:tests/data/flagship_parity_rows.json"], cwd=ROOT,
    )
    assert binding.sha256["fixture"] == hashlib.sha256(committed_fixture).hexdigest()
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
    installed_streamlit = Path(installed_origin).parent
    for bound_path, digest in binding.expected_loaded_assets["streamlit"].items():
        installed_asset = installed_streamlit / bound_path.removeprefix("/streamlit_pivot/")
        assert installed_asset.is_file()
        assert hashlib.sha256(installed_asset.read_bytes()).hexdigest() == digest

    with start_installed_hosts(binding, tmp_path / "hosts") as urls:
        for host in ("dash", "react", "streamlit"):
            with urllib.request.urlopen(urls[host], timeout=10) as response:
                assert response.status == 200
        with urllib.request.urlopen(urls["react"], timeout=10) as response:
            index = response.read()
        assert b"/@vite/client" not in index
        dash_path = next(path for path in binding.expected_loaded_assets["dash"] if path.endswith(".js"))
        with urllib.request.urlopen(urls["dash"] + dash_path, timeout=10) as response:
            verify_loaded_asset(binding, "dash", dash_path, response.read())
        react_path = next(path for path in binding.expected_loaded_assets["react"] if path.endswith(".js"))
        with urllib.request.urlopen(urls["react"] + react_path, timeout=10) as response:
            verify_loaded_asset(binding, "react", react_path, response.read())
        with urllib.request.urlopen(urls["react"] + "/v1/workspace-config", timeout=10) as response:
            assert json.loads(response.read())["table"]
        _assert_browser_loaded_bound_assets(binding, urls, tmp_path)

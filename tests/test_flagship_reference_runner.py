"""Exercise lifecycle and provenance against a real tiny HTTP child."""
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys
from urllib.request import urlopen

import pytest

from scripts import flagship_reference_runner as runner


def test_windows_checkout_preserves_exact_committed_fixture_bytes(tmp_path):
    """Git newline conversion must not invalidate the installed fixture binding."""
    root = Path(__file__).resolve().parents[1]
    source = tmp_path / "checkout"
    source.mkdir()
    fixture_name = "tests/data/flagship_parity_rows.json"
    fixture = source / fixture_name
    fixture.parent.mkdir(parents=True)
    committed_bytes = subprocess.check_output(["git", "show", f"HEAD:{fixture_name}"], cwd=root)
    fixture.write_bytes(committed_bytes)
    attributes = root / ".gitattributes"
    if attributes.exists():
        (source / ".gitattributes").write_bytes(attributes.read_bytes())
    subprocess.run(["git", "init", "--quiet", str(source)], check=True)
    subprocess.run(["git", "-c", "core.autocrlf=false", "add", "."], cwd=source, check=True)
    fixture.unlink()
    subprocess.run(["git", "-c", "core.autocrlf=true", "checkout-index", "--all", "--force"],
                   cwd=source, check=True)

    assert fixture.read_bytes() == committed_bytes, "checkout changed the hash-bound fixture bytes"


@pytest.fixture
def harness(tmp_path, monkeypatch):
    source = tmp_path / "source"
    app = source / "reference/ag_grid_36_2"
    app.mkdir(parents=True)
    fixture = source / "tests/data/flagship_parity_rows.json"
    fixture.parent.mkdir(parents=True)
    fixture.write_bytes(b'{"rows":[]}\n')
    scroll_fixture = source / "tests/data/flagship_scroll_rows.json"
    scroll_fixture.write_bytes(b'{"rows":[{"region":"R000"}]}\n')
    config = {"version": "36.2.0", "options": {"pivotMode": True}}
    (app / "reference-config.json").write_text(json.dumps(config), encoding="utf-8")
    versions = {"ag-grid-community": "36.2.0", "ag-grid-enterprise": "36.2.0", "ag-charts-enterprise": "14.2.0"}
    (app / "package.json").write_text(json.dumps({"dependencies": versions}))
    state = {"source": source, "app": app, "evidence": tmp_path / "private", "commands": [], "children": [],
             "payload": {**config, "filterValues": {}, "fixtureSha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
                         "scrollFixtureSha256": hashlib.sha256(scroll_fixture.read_bytes()).hexdigest()},
             "timeout": False, "fail_build": False, "compete": False, "competitors": []}
    real_popen = subprocess.Popen

    def run(command, *, cwd, check, stdout, stderr, **kwargs):
        assert Path(cwd) == app and check
        state["commands"].append(command[1:])
        stdout.write(b"fake command output\n")
        if command[1:] == ["ci"]:
            for name, version in versions.items():
                package = app / "node_modules" / name / "package.json"
                package.parent.mkdir(parents=True, exist_ok=True)
                package.write_text(json.dumps({"version": version}))
        if state["fail_build"] and command[1:] == ["run", "build"]:
            raise subprocess.CalledProcessError(1, command)

    def popen(command, *, cwd, env, stdout, stderr, **kwargs):
        assert Path(cwd) == app
        assert env["NODE_ENV"] == "production"
        assert command[1:] == ["--import", "tsx", "src/server.ts"]
        state["commands"].append(["start"])
        script = (
            "from http.server import HTTPServer, BaseHTTPRequestHandler\n"
            f"body = {json.dumps(state['payload']).encode()!r}\n"
            "class Handler(BaseHTTPRequestHandler):\n"
            " def do_GET(self):\n"
            "  self.send_response(200); self.end_headers(); self.wfile.write(body)\n"
            f"server = HTTPServer(('127.0.0.1', {int(env['REFERENCE_PORT'])}), Handler)\n"
            f"print('AG Grid SSRM local reference: http://127.0.0.1:{int(env['REFERENCE_PORT'])}', flush=True)\n"
            "server.serve_forever()\n"
        )
        if state["compete"]:
            # Another launch wins the released port while our owned child is
            # still importing. Its configuration and fixture match exactly.
            competitor = real_popen([sys.executable, "-c", script],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            state["competitors"].append(competitor)
            state["competitor_url"] = f"http://127.0.0.1:{env['REFERENCE_PORT']}/"
        if state["timeout"] or state["compete"]:
            script = "import time; time.sleep(60)"
        child = real_popen([sys.executable, "-c", script], stdout=stdout, stderr=stderr)
        state["children"].append(child)
        return child

    monkeypatch.setattr(runner.subprocess, "run", run)
    monkeypatch.setattr(runner.subprocess, "Popen", popen)
    monkeypatch.setattr(runner, "READINESS_TIMEOUT", 2)
    try:
        yield state
    finally:
        for child in state["competitors"]:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)


def test_runs_in_order_and_yields_exact_provenance_then_stops_owned_child(harness):
    h = harness
    with socket.socket() as existing:
        existing.bind(("127.0.0.1", 0))
        existing.listen()
        with runner.start_reference_server(h["source"], h["evidence"]) as server:
            assert server.url.startswith("http://127.0.0.1:")
            assert f":{existing.getsockname()[1]}/" not in server.url
            assert h["commands"] == [["ci"], ["run", "build"], ["start"]]
            assert server.config_sha256 == hashlib.sha256((h["app"] / "reference-config.json").read_bytes()).hexdigest()
            assert server.fixture_sha256 == h["payload"]["fixtureSha256"]
            assert server.versions == {"ag-grid-community": "36.2.0", "ag-grid-enterprise": "36.2.0", "ag-charts-enterprise": "14.2.0"}
            assert urlopen(server.url + "reference-config.json").status == 200
        assert h["children"][0].poll() is not None
        with socket.create_connection(existing.getsockname(), timeout=1):
            pass
    assert any(b"fake command output" in p.read_bytes() for p in h["evidence"].rglob("*.log"))


@pytest.mark.parametrize("field,value", [("version", "36.1.0"), ("fixtureSha256", "0" * 64),
                                         ("scrollFixtureSha256", "0" * 64), ("options", {"pivotMode": False})])
def test_rejects_wrong_served_provenance_and_cleans_up(harness, field, value):
    harness["payload"][field] = value
    with pytest.raises(ValueError, match="reference"):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            pytest.fail("invalid reference yielded")
    assert harness["children"][0].poll() is not None


def test_timeout_cleans_up_child(harness):
    harness["timeout"] = True
    with pytest.raises(TimeoutError):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            pytest.fail("unready child yielded")
    assert harness["children"][0].poll() is not None


def test_rejects_matching_competing_listener_while_owned_child_is_starting(harness):
    harness["compete"] = True
    with pytest.raises(TimeoutError):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            pytest.fail("yielded a competing listener without proof owned child bound")
    assert harness["children"][0].poll() is not None
    assert harness["competitors"][0].poll() is None
    with urlopen(harness["competitor_url"] + "reference-config.json", timeout=1) as response:
        assert json.load(response) == harness["payload"]


def test_caller_failure_cleans_up_child(harness):
    with pytest.raises(RuntimeError, match="caller"):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            raise RuntimeError("caller")
    assert harness["children"][0].poll() is not None


def test_rejects_unpinned_package_before_launch(harness):
    path = harness["app"] / "package.json"
    package = json.loads(path.read_text())
    package["dependencies"]["ag-grid-community"] = "^36.2.0"
    path.write_text(json.dumps(package))
    with pytest.raises(ValueError, match="36.2.0"):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            pytest.fail("unpinned package yielded")
    assert harness["commands"] == []


def test_build_failure_does_not_launch_child(harness):
    harness["fail_build"] = True
    with pytest.raises(subprocess.CalledProcessError):
        with runner.start_reference_server(harness["source"], harness["evidence"]):
            pytest.fail("failed build yielded")
    assert harness["children"] == []

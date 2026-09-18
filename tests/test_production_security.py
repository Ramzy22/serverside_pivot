"""Production security configuration and host-neutral middleware regressions."""

import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

from pivot_engine.complete_rest_api import CompletePivotAPI
from pivot_engine.production_security import (
    InMemoryFixedWindowRateLimiter,
    ProductionSecuritySettings,
    install_asgi_security,
    install_flask_security,
)
from pivot_engine.runtime import RuntimeDatasetAuthorizer
from pivot_engine.scalable_pivot_controller import ScalablePivotController
from pivot_engine.security import User, get_current_user


def _production_env(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("PIVOT_PUBLIC_ORIGIN", "https://pivot.example.test")
    monkeypatch.setenv("PIVOT_TRUSTED_HOSTS", "pivot.example.test")
    monkeypatch.setenv("PIVOT_TRUSTED_PROXY_NETWORKS", "127.0.0.0/8")
    monkeypatch.setenv(
        "PIVOT_CSP",
        "default-src 'self'; frame-ancestors 'none'; base-uri 'self'",
    )
    monkeypatch.setenv(
        "PIVOT_RATE_LIMIT_REDIS_URL",
        "rediss://rate-user:rate-password@redis.example.test:6380/0",
    )
    monkeypatch.setenv("PIVOT_JWT_ACTIVE_KID", "current")
    monkeypatch.setenv(
        "PIVOT_JWT_KEYS_JSON",
        '{"current":"current-secret-with-at-least-thirty-two-bytes"}',
    )
    monkeypatch.setenv("JWT_ISSUER", "https://issuer.example.test")
    monkeypatch.setenv("JWT_AUDIENCE", "pivot-api")


def test_production_settings_reports_all_missing_controls(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    for name in (
        "PIVOT_PUBLIC_ORIGIN",
        "PIVOT_TRUSTED_HOSTS",
        "PIVOT_TRUSTED_PROXY_NETWORKS",
        "PIVOT_CSP",
        "PIVOT_RATE_LIMIT_REDIS_URL",
        "PIVOT_JWT_ACTIVE_KID",
        "PIVOT_JWT_KEYS_JSON",
        "JWT_ISSUER",
        "JWT_AUDIENCE",
    ):
        monkeypatch.delenv(name, raising=False)

    try:
        ProductionSecuritySettings.from_env().validate()
    except RuntimeError as exc:
        message = str(exc)
    else:  # pragma: no cover - documents the required failure mode
        raise AssertionError("production settings unexpectedly validated")

    assert "PIVOT_PUBLIC_ORIGIN" in message
    assert "PIVOT_RATE_LIMIT_REDIS_URL" in message
    assert "PIVOT_JWT_KEYS_JSON" in message
    assert "JWT_AUDIENCE" in message


def test_conflicting_environment_aliases_are_a_startup_error(monkeypatch):
    monkeypatch.setenv("ENV", "production")
    monkeypatch.setenv("DASH_ENV", "development")

    with pytest.raises(RuntimeError, match="Conflicting environment"):
        ProductionSecuritySettings.from_env()


def test_asgi_security_enforces_headers_csrf_and_fixed_window_rate_limit(monkeypatch):
    _production_env(monkeypatch)
    settings = ProductionSecuritySettings.from_env()
    settings.validate()
    app = FastAPI()
    install_asgi_security(
        app,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=2, window_seconds=60),
    )

    @app.post("/mutate")
    async def mutate():
        return {"ok": True}

    client = TestClient(app, base_url="https://pivot.example.test")
    csrf = "csrf-token"
    client.cookies.set(settings.csrf_cookie_name, csrf, domain="pivot.example.test")
    common = {"Origin": "https://pivot.example.test", settings.csrf_header_name: csrf}

    first = client.post("/mutate", headers=common)
    second = client.post("/mutate", headers=common)
    limited = client.post("/mutate", headers=common)
    rejected_csrf = client.post("/mutate", headers={"Origin": "https://evil.example.test"})

    assert first.status_code == 200
    assert first.headers["strict-transport-security"]
    assert "frame-ancestors 'none'" in first.headers["content-security-policy"]
    assert first.headers["x-content-type-options"] == "nosniff"
    assert first.headers["referrer-policy"]
    assert second.status_code == 200
    assert limited.status_code == 429
    assert rejected_csrf.status_code == 403


def test_asgi_security_limits_get_requests_and_requires_csrf_for_cookie_clients(monkeypatch):
    _production_env(monkeypatch)
    settings = ProductionSecuritySettings.from_env()
    app = FastAPI()
    install_asgi_security(
        app,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=2, window_seconds=60),
    )

    @app.get("/read")
    async def read():
        return {"ok": True}

    @app.post("/mutate")
    async def mutate():
        return {"ok": True}

    client = TestClient(app, base_url="https://pivot.example.test")
    seeded = client.get("/read")
    csrf = client.cookies.get(settings.csrf_cookie_name)
    second_read = client.get("/read")
    limited_read = client.get("/read")
    missing_token = client.post("/mutate", headers={"Origin": settings.public_origin})
    wrong_token = client.post(
        "/mutate",
        headers={"Origin": settings.public_origin, settings.csrf_header_name: "wrong"},
    )

    assert seeded.status_code == 200
    assert csrf
    assert settings.csrf_cookie_name in seeded.headers["set-cookie"]
    assert second_read.status_code == 200
    assert limited_read.status_code == 429
    assert missing_token.status_code == 403
    assert wrong_token.status_code == 403


def test_production_settings_rejects_universal_trusted_proxy_network(monkeypatch):
    _production_env(monkeypatch)
    monkeypatch.setenv("PIVOT_TRUSTED_PROXY_NETWORKS", "0.0.0.0/0,::/0")

    with pytest.raises(RuntimeError, match="must not trust all addresses"):
        ProductionSecuritySettings.from_env().validate()


def test_asgi_security_rejects_chunked_body_before_endpoint_runs(monkeypatch):
    _production_env(monkeypatch)
    monkeypatch.setenv("PIVOT_MAX_REQUEST_BYTES", "12")
    settings = ProductionSecuritySettings.from_env()
    app = FastAPI()
    called = False
    install_asgi_security(
        app,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=10, window_seconds=60),
    )

    @app.post("/chunked")
    async def endpoint():
        nonlocal called
        called = True
        return {"ok": True}

    messages = [
        {"type": "http.request", "body": b"a" * 8, "more_body": True},
        {"type": "http.request", "body": b"b" * 8, "more_body": False},
    ]
    sent = []

    async def receive():
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/chunked",
        "raw_path": b"/chunked",
        "query_string": b"",
        "headers": [(b"host", b"pivot.example.test")],
        "client": ("127.0.0.1", 12345),
        "server": ("pivot.example.test", 443),
    }
    asyncio.run(app(scope, receive, send))

    assert called is False
    assert next(message for message in sent if message["type"] == "http.response.start")["status"] == 413


def test_asgi_security_rejects_declared_oversized_body_without_reading_it(monkeypatch):
    _production_env(monkeypatch)
    monkeypatch.setenv("PIVOT_MAX_REQUEST_BYTES", "12")
    settings = ProductionSecuritySettings.from_env()
    app = FastAPI()
    install_asgi_security(
        app,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=10, window_seconds=60),
    )
    receive_calls = 0
    sent = []

    async def receive():
        nonlocal receive_calls
        receive_calls += 1
        return {"type": "http.request", "body": b"a" * 16, "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(
        app(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "POST",
                "scheme": "https",
                "path": "/not-dispatched",
                "raw_path": b"/not-dispatched",
                "query_string": b"",
                "headers": [(b"host", b"pivot.example.test"), (b"content-length", b"16")],
                "client": ("127.0.0.1", 12345),
                "server": ("pivot.example.test", 443),
            },
            receive,
            send,
        )
    )

    assert receive_calls == 0
    assert next(message for message in sent if message["type"] == "http.response.start")["status"] == 413


def test_complete_api_hides_unexpected_backend_error_details():
    controller = ScalablePivotController(backend_uri=":memory:")
    api = CompletePivotAPI(
        controller,
        dataset_authorizer=RuntimeDatasetAuthorizer(
            {"sales": {"roles": {"viewer"}, "rowSecurity": {"mode": "global"}}}
        ),
    )

    async def current_user():
        return User(id="viewer", username="viewer", role="viewer", attributes={})

    async def broken_pivot(*_args, **_kwargs):
        raise RuntimeError("postgres password=secret")

    controller.run_pivot_async = broken_pivot
    import pivot_engine.complete_rest_api as rest_api_module
    import pivot_engine.security as security_module

    api.app.dependency_overrides[rest_api_module.get_current_user] = current_user
    api.app.dependency_overrides[security_module.get_current_user] = current_user
    response = TestClient(api.get_app(), raise_server_exceptions=False).post(
        "/pivot",
        json={"table": "sales", "measures": []},
    )

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["code"] == "internal_error"
    assert detail["correlationId"]
    assert "postgres" not in response.text
    assert "secret" not in response.text


def test_flask_security_is_idempotent_and_protects_unsafe_requests(monkeypatch):
    _production_env(monkeypatch)
    settings = ProductionSecuritySettings.from_env()
    server = Flask(__name__)

    @server.post("/mutate")
    def mutate():
        return {"ok": True}

    install_flask_security(
        server,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=10, window_seconds=60),
    )
    before_request_count = len(server.before_request_funcs.get(None, ()))
    install_flask_security(
        server,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=10, window_seconds=60),
    )

    client = server.test_client()
    accepted = client.post(
        "/mutate",
        base_url="https://pivot.example.test",
        headers={"Origin": "https://pivot.example.test"},
    )
    rejected = client.post(
        "/mutate",
        base_url="https://pivot.example.test",
        headers={"Origin": "https://evil.example.test"},
    )

    assert getattr(server, "_pivot_production_security_installed") is True
    assert len(server.before_request_funcs.get(None, ())) == before_request_count
    assert server.config["MAX_CONTENT_LENGTH"] == settings.max_request_bytes
    assert server.config["SESSION_COOKIE_SECURE"] is True
    assert accepted.status_code == 200
    assert accepted.headers["strict-transport-security"]
    assert rejected.status_code == 403


def test_flask_security_rejects_oversized_callback_body_before_route_runs(monkeypatch):
    _production_env(monkeypatch)
    monkeypatch.setenv("PIVOT_MAX_REQUEST_BYTES", "12")
    settings = ProductionSecuritySettings.from_env()
    server = Flask(__name__)
    calls = 0

    @server.post("/callback")
    def callback():
        nonlocal calls
        calls += 1
        return {"ok": True}

    install_flask_security(
        server,
        settings,
        rate_limiter=InMemoryFixedWindowRateLimiter(limit=10, window_seconds=60),
    )
    response = server.test_client().post(
        "/callback",
        data=b"a" * 16,
        content_type="application/json",
        base_url="https://pivot.example.test",
        headers={"Origin": "https://pivot.example.test"},
    )

    assert response.status_code == 413
    assert calls == 0


def test_dependency_audit_gate_requires_exact_current_exception_records(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "ci" / "verify_dependency_audit.py"
    assert script.is_file()
    audit_path = tmp_path / "audit.json"
    exceptions_path = tmp_path / "exceptions.json"
    audit_path.write_text(
        json.dumps(
            [
                {
                    "name": "example-package",
                    "version": "1.2.3",
                    "vulns": [{"id": "PYSEC-2026-123"}],
                }
            ]
        ),
        encoding="utf-8",
    )

    def check(exceptions):
        exceptions_path.write_text(json.dumps({"exceptions": exceptions}), encoding="utf-8")
        return subprocess.run(
            [
                sys.executable,
                str(script),
                "--audit",
                str(audit_path),
                "--exceptions",
                str(exceptions_path),
                "--as-of",
                "2026-09-17",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    assert check([]).returncode != 0
    matching = {
        "id": "PYSEC-2026-123",
        "package": "example-package",
        "version": "1.2.3",
        "owner": "security-owner",
        "ticket": "SEC-123",
        "rationale": "Temporary containment while the vendor publishes a fix.",
        "expires": "2026-10-01",
    }
    assert check([matching]).returncode == 0
    expired = dict(matching, expires="2026-09-17")
    assert check([expired]).returncode != 0
    invalid = dict(matching)
    invalid.pop("owner")
    assert check([invalid]).returncode != 0

    audit_path.write_text(
        json.dumps(
            [
                {
                    "name": "skipped-package",
                    "version": "1.0.0",
                    "skip_reason": "not supported by scanner",
                    "vulns": [],
                }
            ]
        ),
        encoding="utf-8",
    )
    assert check([]).returncode != 0


def test_npm_audit_gate_rejects_vulnerabilities_and_malformed_reports(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "ci" / "verify_npm_audit.py"
    audit_path = tmp_path / "npm-audit.json"

    def check(payload):
        audit_path.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.run(
            [sys.executable, str(script), "--audit", str(audit_path)],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    assert check({"auditReportVersion": 2, "vulnerabilities": {}}).returncode == 0
    assert check(
        {
            "auditReportVersion": 2,
            "vulnerabilities": {
                "example-package": {
                    "name": "example-package",
                    "severity": "high",
                    "via": ["GHSA-example"],
                }
            },
        }
    ).returncode != 0
    assert check({"auditReportVersion": 2}).returncode != 0


def test_release_workflow_gates_publish_on_security_scanners():
    root = Path(__file__).resolve().parents[1]
    release = (root / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")

    assert "verify_dependency_audit.py" in release
    assert "verify_npm_audit.py" in release
    assert "pip-audit" in release
    assert "npm audit --omit=dev --json" in release
    assert "bandit" in release
    assert "cyclonedx" in release.lower()
    assert "PIVOT_RELEASE_EVIDENCE_PATH" in release
    assert "verify_security_release_evidence.py" in release
    assert release.index("verify_dependency_audit.py") < release.rindex("Publish to PyPI")
    assert release.index("verify_npm_audit.py") < release.rindex("Publish to PyPI")
    assert release.index("verify_security_release_evidence.py") < release.rindex("Publish to PyPI")
    assert "continue-on-error" not in release


def test_release_evidence_validator_requires_current_complete_operator_evidence(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "verify_security_release_evidence.py"
    assert script.is_file()
    matrix_path = tmp_path / "matrix.json"
    evidence_path = tmp_path / "evidence.json"
    attachment_path = tmp_path / "tls-scan.txt"
    attachment_path.write_text("verified edge scan", encoding="utf-8")
    digest = hashlib.sha256(attachment_path.read_bytes()).hexdigest()
    local_control = {
        "id": "AUTHORIZATION",
        "asvs": [
            {
                "id": "v5.0.0-V8.3.1",
                "title": "Verify that the application enforces authorization rules at a trusted service layer and doesn't rely on controls that an untrusted consumer could manipulate, such as client-side JavaScript.",
            }
        ],
        "title": "Authorization regression tests",
        "state": "verified_locally",
        "owner": "application-security",
        "cadence": "each release",
        "localEvidence": ["tests/test_production_security.py"],
        "operatorEvidence": [],
    }
    operator_control = {
        "id": "TLS_EDGE",
        "asvs": [
            {
                "id": "v5.0.0-V12.2.1",
                "title": "Verify that TLS is used for all connectivity between a client and external facing, HTTP-based services, and does not fall back to insecure or unencrypted communications.",
            }
        ],
        "title": "Production TLS and edge verification",
        "state": "operator_evidence_required",
        "owner": "platform",
        "cadence": "each release",
        "localEvidence": [],
        "operatorEvidence": ["tls-scan"],
    }

    def check(controls, attachments):
        matrix_path.write_text(
            json.dumps(
                {
                    "standard": {
                        "name": "OWASP ASVS",
                        "version": "5.0.0",
                        "catalog": "security/asvs-5.0.0-catalog.json",
                    },
                    "controls": controls,
                    "operationalControls": [],
                }
            ),
            encoding="utf-8",
        )
        evidence_path.write_text(
            json.dumps(
                {
                    "release": "v0.0.0-test",
                    "generatedAt": "2026-09-17T12:00:00Z",
                    "attachments": attachments,
                }
            ),
            encoding="utf-8",
        )
        return subprocess.run(
            [
                sys.executable,
                str(script),
                "--matrix",
                str(matrix_path),
                "--evidence",
                str(evidence_path),
                "--repository-root",
                str(root),
                "--as-of",
                "2026-09-17",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )

    assert check([local_control], []).returncode == 0
    assert check([local_control, operator_control], []).returncode != 0
    current_attachment = {
        "id": "tls-scan",
        "path": "tls-scan.txt",
        "owner": "platform",
        "expiresAt": "2026-10-01",
        "sha256": digest,
    }
    assert check([local_control, operator_control], [current_attachment]).returncode == 0
    assert check(
        [local_control, operator_control],
        [dict(current_attachment, expiresAt="2026-09-17")],
    ).returncode != 0
    assert check(
        [local_control, operator_control],
        [dict(current_attachment, sha256="0" * 64)],
    ).returncode != 0

    assert check([], []).returncode != 0
    assert check(
        [dict(local_control, asvs=[dict(local_control["asvs"][0], title="Wrong title")])],
        [],
    ).returncode != 0


def test_asvs_matrix_uses_pinned_v5_controls_with_accountable_operator_owners():
    root = Path(__file__).resolve().parents[1]
    matrix = json.loads(
        (root / "security" / "certification-matrix.json").read_text(encoding="utf-8")
    )

    controls = matrix["controls"]
    assert controls
    catalog = json.loads(
        (root / "security" / "asvs-5.0.0-catalog.json").read_text(encoding="utf-8")
    )
    requirements = {
        (requirement["id"], requirement["title"])
        for requirement in catalog["requirements"]
    }
    assert all(control["asvs"] for control in controls)
    assert all(
        (reference["id"], reference["title"]) in requirements
        for control in controls
        for reference in control["asvs"]
    )
    for control in controls:
        if control["state"] == "operator_evidence_required":
            assert control["owner"]
            assert control["cadence"]
            assert control["operatorEvidence"]


def test_production_config_rejects_plain_or_unauthenticated_redis(monkeypatch):
    from pivot_engine.config import ScalablePivotConfig

    _production_env(monkeypatch)
    monkeypatch.setenv("CACHE_TYPE", "redis")
    monkeypatch.setenv("REDIS_URL", "redis://redis.example.test:6379/0")

    config = ScalablePivotConfig().from_env()
    try:
        config.validate()
    except ValueError as exc:
        assert "Production Redis cache" in str(exc)
    else:  # pragma: no cover - documents the required failure mode
        raise AssertionError("plaintext Redis unexpectedly validated in production")


def test_production_security_evidence_artifacts_cover_the_deployment_boundary():
    root = Path(__file__).resolve().parents[1]
    nginx = (root / "deploy" / "nginx" / "serverside-pivot.conf").read_text(encoding="utf-8")
    verifier = root / "scripts" / "verify_production_deployment.py"
    requirements = root / "security" / "requirements-security.txt"
    runbook = root / "docs" / "operations" / "production-security-runbook.md"
    workflow = root / ".github" / "workflows" / "security.yml"

    assert "return 308 https://" in nginx
    assert "ssl_protocols TLSv1.2 TLSv1.3" in nginx
    assert "Strict-Transport-Security" in nginx
    assert "Content-Security-Policy" in nginx
    assert verifier.is_file()
    assert "--base-url" in verifier.read_text(encoding="utf-8")
    assert "pip-audit" in requirements.read_text(encoding="utf-8")
    assert "cyclonedx" in requirements.read_text(encoding="utf-8").lower()
    assert "backup restore" in runbook.read_text(encoding="utf-8").lower()
    assert "penetration test" in runbook.read_text(encoding="utf-8").lower()
    assert "pip-audit" in workflow.read_text(encoding="utf-8")

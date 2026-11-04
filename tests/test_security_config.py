import asyncio
import importlib
import json
import logging
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "dash_tanstack_pivot"))
sys.path.append(str(ROOT / "dash_tanstack_pivot" / "pivot_engine"))


def _reload_security():
    security = importlib.import_module("pivot_engine.security")
    return importlib.reload(security)


def _restore_security_module():
    _reload_security()


def test_api_jwt_dependency_uses_maintained_crypto_backend():
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.10
        import tomli as tomllib
    from packaging.requirements import Requirement

    config = tomllib.loads((ROOT / "dash_tanstack_pivot/pyproject.toml").read_text())
    requirements = [Requirement(value) for value in config["project"]["optional-dependencies"]["api"]]
    assert not any(value.name.lower() in {"python-jose", "ecdsa"} for value in requirements)
    assert any(value.name.lower() == "pyjwt" and "crypto" in value.extras for value in requirements)


@pytest.mark.parametrize("algorithm", ["RS256", "ES256"])
@pytest.mark.parametrize("certificate", [False, True])
def test_jwt_backend_accepts_production_asymmetric_keys_and_certificates(monkeypatch, algorithm, certificate):
    from datetime import datetime, timedelta, timezone
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, rsa
    from cryptography.x509.oid import NameOID

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048) if algorithm == "RS256" else ec.generate_private_key(ec.SECP256R1())
    if certificate:
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "jwt-test")])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(private.public_key())
                .serial_number(1).not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1)).sign(private, hashes.SHA256()))
        public = cert.public_bytes(serialization.Encoding.PEM)
    else:
        public = private.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.delenv("JWT_SECRET_KEY", raising=False)
        env.setenv("PIVOT_JWT_KEYS_JSON", json.dumps({"current": public.decode()}))
        env.setenv("PIVOT_JWT_ACTIVE_KID", "current")
        env.setenv("JWT_ALGORITHM", algorithm)
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        security = _reload_security()
        claims = {"sub": "viewer", "iss": security.JWT_ISSUER, "aud": "pivot-api", "exp": int(time.time()) + 60}
        token = security.jwt.encode(claims, private, algorithm=algorithm, headers={"kid": "current"})
        assert asyncio.run(security.authenticate_user_credentials(token=token)).username == "viewer"
        # Public-key bytes must never become a permitted HMAC credential.
        der = private.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        import base64
        import hashlib
        import hmac
        def segment(value):
            return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=")
        signing_input = segment({"alg": "HS256", "kid": "current"}) + b"." + segment(claims)
        forged = (signing_input + b"." + base64.urlsafe_b64encode(hmac.new(der, signing_input, hashlib.sha256).digest()).rstrip(b"=")).decode()
        with pytest.raises(security.HTTPException) as error:
            asyncio.run(security.authenticate_user_credentials(token=forged))
        assert error.value.status_code == 401
    _restore_security_module()


@pytest.mark.parametrize("change", [
    {"exp": None}, {"iss": None}, {"aud": None},
    {"exp": 1}, {"iss": "https://attacker.example"}, {"aud": "other-api"},
    {"nbf": 9999999999}, {"sub": None},
])
def test_jwt_backend_preserves_required_claims_and_validation(monkeypatch, change):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("JWT_SECRET_KEY", "a-test-secret-with-at-least-thirty-two-bytes")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        security = _reload_security()
        claims = {"sub": "viewer", "iss": "https://issuer.example.test", "aud": "pivot-api", "exp": int(time.time()) + 60}
        claims.update(change)
        claims = {key: value for key, value in claims.items() if value is not None}
        token = security.jwt.encode(claims, security.JWT_SECRET_KEY, algorithm="HS256")
        with pytest.raises(security.HTTPException) as error:
            asyncio.run(security.authenticate_user_credentials(token=token))
        assert error.value.status_code == 401
    _restore_security_module()


@pytest.mark.parametrize("algorithm", ["HS512", "none"])
def test_jwt_backend_rejects_algorithm_outside_server_allowlist(monkeypatch, algorithm):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("JWT_SECRET_KEY", "a-test-secret-with-at-least-sixty-four-bytes-for-the-hs512-test-only")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_ALGORITHM", "HS256")
        security = _reload_security()
        token = security.jwt.encode({"sub": "viewer", "iss": security.JWT_ISSUER, "exp": int(time.time()) + 60},
                                    None if algorithm == "none" else security.JWT_SECRET_KEY, algorithm=algorithm)
        with pytest.raises(security.HTTPException) as error:
            asyncio.run(security.authenticate_user_credentials(token=token))
        assert error.value.status_code == 401
    _restore_security_module()


def test_security_rejects_missing_jwt_secret_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.delenv("JWT_SECRET_KEY", raising=False)
        with pytest.raises(RuntimeError, match="JWT_SECRET_KEY must be set"):
            _reload_security()

    _restore_security_module()


def test_security_rejects_default_jwt_secret_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.setenv("JWT_SECRET_KEY", "dev-jwt-secret")
        with pytest.raises(RuntimeError, match="JWT_SECRET_KEY must be set"):
            _reload_security()

    _restore_security_module()


def test_security_accepts_explicit_jwt_secret_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.setenv("JWT_SECRET_KEY", "test-production-secret-with-at-least-thirty-two-bytes")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        security = _reload_security()
        assert security.JWT_SECRET_KEY == "test-production-secret-with-at-least-thirty-two-bytes"
        assert security.JWT_ISSUER == "https://issuer.example.test"

    _restore_security_module()


def test_security_allows_a_production_key_ring_without_a_legacy_shared_secret(monkeypatch):
    """A rotated key ring is the production credential source, not a fallback."""
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.delenv("JWT_SECRET_KEY", raising=False)
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        env.setenv("PIVOT_JWT_ACTIVE_KID", "current")
        env.setenv(
            "PIVOT_JWT_KEYS_JSON",
            '{"current":"current-secret-with-at-least-thirty-two-bytes"}',
        )

        security = _reload_security()

        assert security.JWT_SECRET_KEY is None
        assert security.JwtKeyRing.from_env().active_kid == "current"

    _restore_security_module()


def test_security_rejects_weak_hmac_key_ring_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.delenv("JWT_SECRET_KEY", raising=False)
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        env.setenv("PIVOT_JWT_ACTIVE_KID", "current")
        env.setenv("PIVOT_JWT_KEYS_JSON", '{"current":"short"}')
        security = _reload_security()

        with pytest.raises(RuntimeError, match="at least 32"):
            security.JwtKeyRing.from_env()

    _restore_security_module()


def test_security_rejects_invalid_asymmetric_key_ring_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.delenv("JWT_SECRET_KEY", raising=False)
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        env.setenv("JWT_ALGORITHM", "RS256")
        env.setenv("PIVOT_JWT_ACTIVE_KID", "current")
        env.setenv("PIVOT_JWT_KEYS_JSON", '{"current":"not-a-public-key"}')
        security = _reload_security()

        with pytest.raises(RuntimeError, match="public verification keys"):
            security.JwtKeyRing.from_env()

    _restore_security_module()


def test_security_disables_jwt_when_secret_and_explicit_dev_opt_in_are_missing(monkeypatch):
    with monkeypatch.context() as env:
        for name in ("ENV", "APP_ENV", "FLASK_ENV", "DASH_ENV", "JWT_SECRET_KEY", "ALLOW_DEV_AUTH"):
            env.delenv(name, raising=False)
        security = _reload_security()
        assert security.JWT_SECRET_KEY is None

    _restore_security_module()


def test_security_rejects_dev_api_key_fallback_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.setenv(
            "JWT_SECRET_KEY",
            "test-production-secret-with-at-least-thirty-two-bytes",
        )
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key="dev-key", token=None))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_rejects_dev_auth_fallback_without_explicit_allow(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "development")
        env.delenv("ALLOW_DEV_AUTH", raising=False)
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key=None, token=None))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_rejects_dev_auth_fallback_in_staging_even_with_allow_flag(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "staging")
        env.setenv("ALLOW_DEV_AUTH", "1")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()
        assert security.JWT_SECRET_KEY is None

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key=None, token=None))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_normalizes_dev_alias_for_explicit_development_auth(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "dev")
        env.setenv("ALLOW_DEV_AUTH", "1")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()
        assert security.JWT_SECRET_KEY == "dev-jwt-secret"

        assert asyncio.run(
            security.get_current_user(api_key="dev-key", token=None)
        ).username == "dev_admin"

    _restore_security_module()


def test_security_allows_dev_auth_only_with_explicit_local_flag_and_logs(monkeypatch, caplog):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("ALLOW_DEV_AUTH", "1")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()
        assert security.JWT_SECRET_KEY == "dev-jwt-secret"

        with caplog.at_level(logging.WARNING, logger=security.__name__):
            user = asyncio.run(security.get_current_user(api_key=None, token=None))

        assert user.username == "dev_admin"
        assert user.scopes == ["*"]
        assert "development authentication fallback" in caplog.text

    _restore_security_module()


def test_security_rejects_public_default_secret_forgery_without_local_opt_in(monkeypatch):
    with monkeypatch.context() as env:
        for name in ("ENV", "APP_ENV", "FLASK_ENV", "DASH_ENV", "JWT_SECRET_KEY", "ALLOW_DEV_AUTH"):
            env.delenv(name, raising=False)
        security = _reload_security()
        if not security._jwt_available:
            pytest.skip("PyJWT is not installed")
        forged_token = security.jwt.encode(
            {"sub": "attacker", "role": "admin", "scopes": ["*"]},
            "dev-jwt-secret",
            algorithm="HS256",
        )

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key=None, token=forged_token))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_rejects_a_jwt_without_an_expiration_claim(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("JWT_SECRET_KEY", "test-jwt-expiration-secret")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.delenv("ALLOW_DEV_AUTH", raising=False)
        security = _reload_security()
        if not security._jwt_available:
            pytest.skip("PyJWT is not installed")

        token_without_expiry = security.jwt.encode(
            {"sub": "viewer", "role": "viewer", "iss": "https://issuer.example.test"},
            "test-jwt-expiration-secret",
            algorithm="HS256",
        )
        valid_token = security.jwt.encode(
            {
                "sub": "viewer",
                "role": "viewer",
                "iss": "https://issuer.example.test",
                "exp": int(time.time()) + 60,
            },
            "test-jwt-expiration-secret",
            algorithm="HS256",
        )

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key=None, token=token_without_expiry))
        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

        user = asyncio.run(security.get_current_user(api_key=None, token=valid_token))
        assert user.username == "viewer"

    _restore_security_module()


def test_security_requires_an_explicit_jwt_issuer_when_jwt_is_configured(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("JWT_SECRET_KEY", "test-jwt-issuer-secret")
        env.delenv("JWT_ISSUER", raising=False)
        with pytest.raises(RuntimeError, match="JWT_ISSUER"):
            _reload_security()

    _restore_security_module()


def test_security_validates_jwt_issuer_claim(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("JWT_SECRET_KEY", "test-jwt-issuer-secret")
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        security = _reload_security()
        if not security._jwt_available:
            pytest.skip("PyJWT is not installed")

        def token(issuer):
            claims = {
                "sub": "viewer",
                "role": "viewer",
                "exp": int(time.time()) + 60,
            }
            if issuer is not None:
                claims["iss"] = issuer
            return security.jwt.encode(
                claims,
                "test-jwt-issuer-secret",
                algorithm="HS256",
            )

        for issuer in (None, "https://attacker.example.test"):
            with pytest.raises(security.HTTPException) as exc_info:
                asyncio.run(security.get_current_user(api_key=None, token=token(issuer)))
            assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

        user = asyncio.run(
            security.get_current_user(
                api_key=None,
                token=token("https://issuer.example.test"),
            )
        )
        assert user.username == "viewer"

    _restore_security_module()


def test_jwt_key_ring_accepts_active_and_previous_keys_but_rejects_unknown_kid(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.setenv(
            "JWT_SECRET_KEY",
            "compatibility-production-secret-with-at-least-thirty-two-bytes",
        )
        env.setenv("JWT_ISSUER", "https://issuer.example.test")
        env.setenv("JWT_AUDIENCE", "pivot-api")
        env.setenv("PIVOT_JWT_ACTIVE_KID", "current")
        env.setenv(
            "PIVOT_JWT_KEYS_JSON",
            json.dumps(
                {
                    "current": "current-secret-with-at-least-thirty-two-bytes",
                    "previous": "previous-secret-with-at-least-thirty-two-bytes",
                }
            ),
        )
        security = _reload_security()
        if not security._jwt_available:
            pytest.skip("PyJWT is not installed")

        def token(secret, kid):
            return security.jwt.encode(
                {
                    "sub": "viewer",
                    "role": "viewer",
                    "iss": "https://issuer.example.test",
                    "aud": "pivot-api",
                    "exp": int(time.time()) + 60,
                },
                secret,
                algorithm="HS256",
                headers={"kid": kid},
            )

        assert asyncio.run(
            security.get_current_user(
                api_key=None,
                token=token("current-secret-with-at-least-thirty-two-bytes", "current"),
            )
        ).username == "viewer"
        assert asyncio.run(
            security.get_current_user(
                api_key=None,
                token=token("previous-secret-with-at-least-thirty-two-bytes", "previous"),
            )
        ).username == "viewer"
        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(
                security.get_current_user(
                    api_key=None,
                    token=token("current-secret-with-at-least-thirty-two-bytes", "unknown"),
                )
            )
        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_apply_rls_to_spec_replaces_conflicting_user_filter():
    security = _reload_security()
    spec = security.PivotSpec(
        table="sales",
        filters=[
            {"field": "region", "op": "=", "value": "East"},
            {"field": "segment", "op": "=", "value": "Enterprise"},
        ],
    )
    user = security.User(
        id="user-1",
        username="analyst",
        role=security.UserRole.VIEWER,
        attributes={"region": "North"},
    )

    result = security.apply_rls_to_spec(spec, user)

    assert result is spec
    assert spec.filters == [
        {"field": "segment", "op": "=", "value": "Enterprise"},
        {"field": "region", "op": "=", "value": "North"},
    ]


def test_apply_rls_to_spec_replaces_composite_filter_that_mentions_rls_field():
    security = _reload_security()
    spec = security.PivotSpec(
        table="sales",
        filters=[
            {
                "op": "OR",
                "conditions": [
                    {"field": "region", "op": "=", "value": "East"},
                    {"field": "segment", "op": "=", "value": "Enterprise"},
                ],
            },
            {"field": "year", "op": "=", "value": 2026},
        ],
    )
    user = security.User(
        id="user-1",
        username="analyst",
        role=security.UserRole.VIEWER,
        attributes={"region": "North"},
    )

    security.apply_rls_to_spec(spec, user)

    assert spec.filters == [
        {"field": "year", "op": "=", "value": 2026},
        {"field": "region", "op": "=", "value": "North"},
    ]


def test_apply_rls_to_spec_is_idempotent_for_rls_fields():
    security = _reload_security()
    spec = security.PivotSpec(
        table="sales",
        filters=[{"field": "region", "op": "=", "value": "North"}],
    )
    user = security.User(
        id="user-1",
        username="analyst",
        role=security.UserRole.VIEWER,
        attributes={"region": "North"},
    )

    security.apply_rls_to_spec(spec, user)
    security.apply_rls_to_spec(spec, user)

    assert spec.filters == [{"field": "region", "op": "=", "value": "North"}]


def test_accounting_tenant_is_not_treated_as_a_dataset_filter():
    security = _reload_security()
    spec = security.PivotSpec(table="sales", filters=[])
    user = security.User(
        id="user-1", username="analyst", role=security.UserRole.VIEWER,
        attributes={"tenant": "data-a", "accounting_tenant": "payer-a"},
    )

    security.apply_rls_to_spec(spec, user)

    assert spec.filters == [{"field": "tenant", "op": "=", "value": "data-a"}]

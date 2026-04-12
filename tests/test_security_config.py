import asyncio
import importlib
import logging
import sys
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
        env.setenv("JWT_SECRET_KEY", "test-production-secret")
        security = _reload_security()
        assert security.JWT_SECRET_KEY == "test-production-secret"

    _restore_security_module()


def test_security_keeps_development_jwt_secret_fallback(monkeypatch):
    with monkeypatch.context() as env:
        for name in ("ENV", "APP_ENV", "FLASK_ENV", "DASH_ENV", "JWT_SECRET_KEY"):
            env.delenv(name, raising=False)
        security = _reload_security()
        assert security.JWT_SECRET_KEY == "dev-jwt-secret"

    _restore_security_module()


def test_security_rejects_dev_api_key_fallback_in_production(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "production")
        env.setenv("JWT_SECRET_KEY", "test-production-secret")
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

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key=None, token=None))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_rejects_dev_auth_fallback_in_dev_alias_even_with_allow_flag(monkeypatch):
    with monkeypatch.context() as env:
        env.setenv("ENV", "dev")
        env.setenv("ALLOW_DEV_AUTH", "1")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()

        with pytest.raises(security.HTTPException) as exc_info:
            asyncio.run(security.get_current_user(api_key="dev-key", token=None))

        assert exc_info.value.status_code == security.status.HTTP_401_UNAUTHORIZED

    _restore_security_module()


def test_security_allows_dev_auth_only_with_explicit_local_flag_and_logs(monkeypatch, caplog):
    with monkeypatch.context() as env:
        env.setenv("ENV", "local")
        env.setenv("ALLOW_DEV_AUTH", "1")
        env.delenv("PIVOT_API_KEY", raising=False)
        security = _reload_security()

        with caplog.at_level(logging.WARNING, logger=security.__name__):
            user = asyncio.run(security.get_current_user(api_key=None, token=None))

        assert user.username == "dev_admin"
        assert user.scopes == ["*"]
        assert "development authentication fallback" in caplog.text

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

"""Platform-neutral production security settings and HTTP host middleware.

The library cannot operate an organization's TLS termination, Redis cluster, or
incident process.  It can, however, make the required configuration explicit,
provide safe host hooks, and reject unsafe production startup deterministically.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol
from urllib.parse import urlparse


_ENVIRONMENT_VARIABLES = ("ENV", "APP_ENV", "FLASK_ENV", "DASH_ENV")
_ENVIRONMENT_ALIASES = {
    "prod": "production",
    "production": "production",
    "dev": "development",
    "development": "development",
    "local": "development",
}
_PRODUCTION_VALUES = frozenset({"production"})
_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def resolve_runtime_environment(
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    """Return one canonical environment or reject conflicting aliases.

    Hosts sometimes populate more than one framework-specific environment
    variable. Treating the first one as authoritative allows a later value to
    silently disable production protections, so non-empty aliases must agree.
    """
    source = os.environ if environ is None else environ
    configured: dict[str, str] = {}
    for name in _ENVIRONMENT_VARIABLES:
        raw_value = str(source.get(name) or "").strip().lower()
        if raw_value:
            configured[name] = _ENVIRONMENT_ALIASES.get(raw_value, raw_value)
    values = set(configured.values())
    if len(values) > 1:
        rendered = ", ".join(
            f"{name}={value}" for name, value in sorted(configured.items())
        )
        raise RuntimeError(f"Conflicting environment configuration: {rendered}.")
    return next(iter(values), "")


def _csv_env(name: str) -> tuple[str, ...]:
    return tuple(
        value.strip()
        for value in str(os.getenv(name) or "").split(",")
        if value.strip()
    )


@dataclass(frozen=True)
class ProductionSecuritySettings:
    """Environment-owned controls required by an internet-facing deployment."""

    environment: str
    public_origin: str
    trusted_hosts: tuple[str, ...]
    trusted_proxy_networks: tuple[str, ...]
    content_security_policy: str
    rate_limit_redis_url: str
    rate_limit_requests: int
    rate_limit_window_seconds: int
    max_request_bytes: int
    csrf_cookie_name: str
    csrf_header_name: str
    jwt_active_kid: str
    jwt_keys_json: str
    jwt_issuer: str
    jwt_audience: str

    @property
    def is_production(self) -> bool:
        return self.environment in _PRODUCTION_VALUES

    @classmethod
    def from_env(cls) -> "ProductionSecuritySettings":
        environment = resolve_runtime_environment()
        def integer(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except (TypeError, ValueError):
                return 0

        return cls(
            environment=environment,
            public_origin=str(os.getenv("PIVOT_PUBLIC_ORIGIN") or "").strip().rstrip("/"),
            trusted_hosts=_csv_env("PIVOT_TRUSTED_HOSTS"),
            trusted_proxy_networks=_csv_env("PIVOT_TRUSTED_PROXY_NETWORKS"),
            content_security_policy=str(os.getenv("PIVOT_CSP") or "").strip(),
            rate_limit_redis_url=str(os.getenv("PIVOT_RATE_LIMIT_REDIS_URL") or "").strip(),
            rate_limit_requests=integer("PIVOT_RATE_LIMIT_REQUESTS", 120),
            rate_limit_window_seconds=integer("PIVOT_RATE_LIMIT_WINDOW_SECONDS", 60),
            max_request_bytes=integer("PIVOT_MAX_REQUEST_BYTES", 1_048_576),
            # Dash Renderer reads this exact cookie and mirrors it in this
            # exact header for every callback request.  Keeping the
            # framework-compatible default preserves strict double-submit
            # validation without replacing Dash's transport.
            csrf_cookie_name=str(os.getenv("PIVOT_CSRF_COOKIE_NAME") or "_csrf_token"),
            csrf_header_name=str(os.getenv("PIVOT_CSRF_HEADER_NAME") or "X-CSRFToken"),
            jwt_active_kid=str(os.getenv("PIVOT_JWT_ACTIVE_KID") or "").strip(),
            jwt_keys_json=str(os.getenv("PIVOT_JWT_KEYS_JSON") or "").strip(),
            jwt_issuer=str(os.getenv("JWT_ISSUER") or "").strip(),
            jwt_audience=str(os.getenv("JWT_AUDIENCE") or "").strip(),
        )

    def validate(self) -> None:
        """Collect every unsafe production setting before refusing startup."""
        if not self.is_production:
            return
        errors: list[str] = []
        origin = urlparse(self.public_origin)
        if origin.scheme != "https" or not origin.netloc:
            errors.append("PIVOT_PUBLIC_ORIGIN must be an https:// origin")
        if not self.trusted_hosts:
            errors.append("PIVOT_TRUSTED_HOSTS must list public hostnames")
        if not self.trusted_proxy_networks:
            errors.append("PIVOT_TRUSTED_PROXY_NETWORKS must list trusted proxy CIDRs")
        else:
            for network in self.trusted_proxy_networks:
                try:
                    parsed_network = ipaddress.ip_network(network, strict=False)
                except ValueError:
                    errors.append("PIVOT_TRUSTED_PROXY_NETWORKS contains an invalid CIDR")
                    break
                if parsed_network.prefixlen == 0:
                    errors.append("PIVOT_TRUSTED_PROXY_NETWORKS must not trust all addresses")
                    break
        if not self.content_security_policy or "frame-ancestors" not in self.content_security_policy:
            errors.append("PIVOT_CSP must define frame-ancestors")
        rate_url = urlparse(self.rate_limit_redis_url)
        if rate_url.scheme != "rediss" or not rate_url.hostname or not rate_url.username or not rate_url.password:
            errors.append("PIVOT_RATE_LIMIT_REDIS_URL must be authenticated rediss:// URL")
        if self.rate_limit_requests <= 0 or self.rate_limit_window_seconds <= 0:
            errors.append("PIVOT rate-limit request and window values must be positive")
        if self.max_request_bytes <= 0:
            errors.append("PIVOT_MAX_REQUEST_BYTES must be positive")
        try:
            key_values = json.loads(self.jwt_keys_json)
        except json.JSONDecodeError:
            key_values = None
        if not isinstance(key_values, dict) or not key_values:
            errors.append("PIVOT_JWT_KEYS_JSON must contain verification keys")
        elif not self.jwt_active_kid or not isinstance(key_values.get(self.jwt_active_kid), str):
            errors.append("PIVOT_JWT_ACTIVE_KID must name a configured verification key")
        elif str(os.getenv("JWT_ALGORITHM", "HS256") or "").strip().upper().startswith("HS"):
            if any(
                not isinstance(value, str) or len(value.encode("utf-8")) < 32
                for value in key_values.values()
            ):
                errors.append("Production HMAC JWT verification keys must be at least 32 UTF-8 bytes")
        elif str(os.getenv("JWT_ALGORITHM", "HS256") or "").strip().upper().startswith(("RS", "ES")):
            if any(
                not isinstance(value, str)
                or (
                    "-----BEGIN PUBLIC KEY-----" not in value
                    and "-----BEGIN CERTIFICATE-----" not in value
                )
                for value in key_values.values()
            ):
                errors.append("Production asymmetric JWT public verification keys must be PEM encoded")
        if not self.jwt_issuer:
            errors.append("JWT_ISSUER must be configured")
        if not self.jwt_audience:
            errors.append("JWT_AUDIENCE must be configured")
        if errors:
            raise RuntimeError("Production security configuration is incomplete: " + "; ".join(errors))


class FixedWindowRateLimiter(Protocol):
    def check(self, key: str) -> bool: ...


class InMemoryFixedWindowRateLimiter:
    """Deterministic local/test limiter; production uses RedisFixedWindowRateLimiter."""

    def __init__(self, *, limit: int, window_seconds: int) -> None:
        self.limit = int(limit)
        self.window_seconds = int(window_seconds)
        self._entries: dict[str, tuple[int, int]] = {}
        self._lock = threading.Lock()

    def check(self, key: str) -> bool:
        now = int(time.time())
        window = now // self.window_seconds
        with self._lock:
            previous_window, count = self._entries.get(key, (window, 0))
            if previous_window != window:
                count = 0
            count += 1
            self._entries[key] = (window, count)
            return count <= self.limit


class RedisFixedWindowRateLimiter:
    """Shared fixed-window limiter backed by atomic Redis INCR/EXPIRE calls."""

    def __init__(self, redis_url: str, *, limit: int, window_seconds: int) -> None:
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - optional deployment extra
            raise RuntimeError("Redis rate limiting requires the redis package.") from exc
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=False,
            ssl_cert_reqs="required",
        )
        self.limit = int(limit)
        self.window_seconds = int(window_seconds)

    def check(self, key: str) -> bool:
        window = int(time.time()) // self.window_seconds
        cache_key = f"pivot:rate:v1:{window}:{key}"
        pipeline = self._client.pipeline()
        pipeline.incr(cache_key)
        pipeline.expire(cache_key, self.window_seconds + 1, nx=True)
        count, _ = pipeline.execute()
        return int(count) <= self.limit


class _RequestBodyLimitMiddleware:
    """Buffer and replay one ASGI request only after its body is within limit."""

    def __init__(self, app: Any, *, max_request_bytes: int) -> None:
        self.app = app
        self.max_request_bytes = int(max_request_bytes)

    @staticmethod
    async def _send_json_error(send: Any, *, status: int, detail: str) -> None:
        body = json.dumps({"detail": detail}, separators=(",", ":")).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    def _content_length(scope: Any) -> Optional[str]:
        for raw_name, raw_value in scope.get("headers", ()):
            name = raw_name.decode("latin-1") if isinstance(raw_name, bytes) else str(raw_name)
            if name.lower() == "content-length":
                return (
                    raw_value.decode("latin-1")
                    if isinstance(raw_value, bytes)
                    else str(raw_value)
                )
        return None

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        content_length = self._content_length(scope)
        if content_length:
            try:
                if int(content_length) > self.max_request_bytes:
                    await self._send_json_error(
                        send,
                        status=413,
                        detail="Request body is too large.",
                    )
                    return
            except ValueError:
                await self._send_json_error(send, status=400, detail="Invalid request size.")
                return

        buffered: list[dict[str, Any]] = []
        received_bytes = 0
        while True:
            message = await receive()
            message_type = message.get("type")
            buffered.append(message)
            if message_type == "http.request":
                received_bytes += len(message.get("body") or b"")
                if received_bytes > self.max_request_bytes:
                    await self._send_json_error(
                        send,
                        status=413,
                        detail="Request body is too large.",
                    )
                    return
                if not message.get("more_body", False):
                    break
            elif message_type == "http.disconnect":
                break

        buffered_index = 0

        async def replay_receive() -> dict[str, Any]:
            nonlocal buffered_index
            if buffered_index < len(buffered):
                message = buffered[buffered_index]
                buffered_index += 1
                return message
            return {"type": "http.disconnect"}

        await self.app(scope, replay_receive, send)


def _is_trusted_forwarded_request(request: Any, settings: ProductionSecuritySettings) -> bool:
    client_host = str(
        getattr(getattr(request, "client", None), "host", "")
        or getattr(request, "remote_addr", "")
        or ""
    )
    try:
        client_ip = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    return any(client_ip in ipaddress.ip_network(network, strict=False) for network in settings.trusted_proxy_networks)


def _csrf_request_is_valid(
    *,
    origin: str,
    cookies: Mapping[str, Any],
    csrf_cookie: Optional[str],
    csrf_header: Optional[str],
    settings: ProductionSecuritySettings,
) -> bool:
    """Require double-submit CSRF only for cookie-authenticated browser traffic."""
    if origin and origin != settings.public_origin:
        return False
    if not cookies:
        return True
    return bool(
        origin == settings.public_origin
        and csrf_cookie
        and csrf_header
        and hmac.compare_digest(str(csrf_cookie), str(csrf_header))
    )


def _set_csrf_cookie(response: Any, request_cookies: Mapping[str, Any], settings: ProductionSecuritySettings) -> None:
    if settings.csrf_cookie_name not in request_cookies:
        response.set_cookie(
            settings.csrf_cookie_name,
            secrets.token_urlsafe(32),
            secure=True,
            httponly=False,
            samesite="strict",
            path="/",
        )


def install_asgi_security(
    app: Any,
    settings: Optional[ProductionSecuritySettings] = None,
    *,
    rate_limiter: Optional[FixedWindowRateLimiter] = None,
) -> Any:
    """Install headers, HTTPS, CSRF, request-size, and rate protections on ASGI.

    A production settings instance creates a Redis limiter by default. Tests and
    embedded hosts may inject a compatible limiter explicitly.
    """
    settings = settings or ProductionSecuritySettings.from_env()
    settings.validate()
    if not settings.is_production:
        return app
    try:
        from fastapi.responses import JSONResponse
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.middleware.trustedhost import TrustedHostMiddleware
    except ImportError as exc:  # pragma: no cover - optional host dependency
        raise RuntimeError("ASGI production security requires FastAPI/Starlette.") from exc

    limiter = rate_limiter or RedisFixedWindowRateLimiter(
        settings.rate_limit_redis_url,
        limit=settings.rate_limit_requests,
        window_seconds=settings.rate_limit_window_seconds,
    )

    class _SecurityMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Any, call_next: Any) -> Any:
            content_length = request.headers.get("content-length")
            if content_length:
                try:
                    if int(content_length) > settings.max_request_bytes:
                        return JSONResponse({"detail": "Request body is too large."}, status_code=413)
                except ValueError:
                    return JSONResponse({"detail": "Invalid request size."}, status_code=400)

            scheme = request.url.scheme
            if _is_trusted_forwarded_request(request, settings):
                scheme = str(request.headers.get("x-forwarded-proto") or scheme).lower()
            if scheme != "https":
                return JSONResponse({"detail": "HTTPS is required."}, status_code=400)

            if request.method.upper() in _UNSAFE_METHODS:
                origin = str(request.headers.get("origin") or "").rstrip("/")
                csrf_cookie = request.cookies.get(settings.csrf_cookie_name)
                csrf_header = request.headers.get(settings.csrf_header_name)
                if not _csrf_request_is_valid(
                    origin=origin,
                    cookies=request.cookies,
                    csrf_cookie=csrf_cookie,
                    csrf_header=csrf_header,
                    settings=settings,
                ):
                    return JSONResponse({"detail": "CSRF validation failed."}, status_code=403)

            remote = str(getattr(getattr(request, "client", None), "host", "anonymous") or "anonymous")
            if not limiter.check(f"{remote}:{request.method.upper()}:{request.url.path}"):
                return JSONResponse({"detail": "Rate limit exceeded."}, status_code=429, headers={"Retry-After": str(settings.rate_limit_window_seconds)})

            response = await call_next(request)
            _set_csrf_cookie(response, request.cookies, settings)
            response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
            response.headers["Content-Security-Policy"] = settings.content_security_policy
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
            return response

    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(settings.trusted_hosts))
    app.add_middleware(_SecurityMiddleware)
    # This is outermost so a chunked payload is fully bounded before any
    # framework middleware or route can parse it.
    app.add_middleware(
        _RequestBodyLimitMiddleware,
        max_request_bytes=settings.max_request_bytes,
    )
    return app


def install_flask_security(
    server: Any,
    settings: Optional[ProductionSecuritySettings] = None,
    *,
    rate_limiter: Optional[FixedWindowRateLimiter] = None,
) -> Any:
    """Install equivalent header and cookie-origin checks on a Flask/Dash host."""
    settings = settings or ProductionSecuritySettings.from_env()
    settings.validate()
    if not settings.is_production:
        return server
    if getattr(server, "_pivot_production_security_installed", False):
        return server
    try:
        from flask import abort, request
    except ImportError as exc:  # pragma: no cover - optional Dash dependency
        raise RuntimeError("Flask production security requires Flask.") from exc

    limiter = rate_limiter or RedisFixedWindowRateLimiter(
        settings.rate_limit_redis_url,
        limit=settings.rate_limit_requests,
        window_seconds=settings.rate_limit_window_seconds,
    )

    @server.before_request
    def _validate_flask_request():
        if request.host.split(":", 1)[0] not in settings.trusted_hosts:
            abort(400)
        content_length = request.content_length
        if content_length is not None and content_length > settings.max_request_bytes:
            abort(413)
        scheme = str(request.scheme or "").lower()
        if _is_trusted_forwarded_request(request, settings):
            scheme = str(request.headers.get("X-Forwarded-Proto") or scheme).lower()
        if scheme != "https":
            abort(400)
        if request.method in _UNSAFE_METHODS:
            origin = str(request.headers.get("Origin") or "").rstrip("/")
            csrf_cookie = request.cookies.get(settings.csrf_cookie_name)
            csrf_header = request.headers.get(settings.csrf_header_name)
            if not _csrf_request_is_valid(
                origin=origin,
                cookies=request.cookies,
                csrf_cookie=csrf_cookie,
                csrf_header=csrf_header,
                settings=settings,
            ):
                abort(403)
        remote = str(request.remote_addr or "anonymous")
        if not limiter.check(f"{remote}:{request.method.upper()}:{request.path}"):
            abort(429)

    @server.after_request
    def _add_flask_headers(response: Any):
        _set_csrf_cookie(response, request.cookies, settings)
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
        response.headers["Content-Security-Policy"] = settings.content_security_policy
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        return response

    server.config.update(
        MAX_CONTENT_LENGTH=settings.max_request_bytes,
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )
    setattr(server, "_pivot_production_security_installed", True)
    return server


__all__ = [
    "FixedWindowRateLimiter",
    "InMemoryFixedWindowRateLimiter",
    "ProductionSecuritySettings",
    "RedisFixedWindowRateLimiter",
    "resolve_runtime_environment",
    "install_asgi_security",
    "install_flask_security",
]

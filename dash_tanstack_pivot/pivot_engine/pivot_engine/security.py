"""
security.py - Security and Authentication for Scalable Pivot Engine
Supports API Key (Service-to-Service) and OAuth2/JWT (User-to-Service).
"""
import os
import logging
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Optional, List, Dict, Any, Mapping, Set

from .production_security import resolve_runtime_environment

try:
    from fastapi import HTTPException, Security, status, Depends
    from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
    from starlette.requests import HTTPConnection
    _fastapi_available = True
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
except ImportError:
    _fastapi_available = False
    HTTPException = Security = status = Depends = None
    api_key_header = oauth2_scheme = None
    HTTPConnection = Any

try:
    from pydantic import BaseModel
    _pydantic_available = True
except ImportError:
    _pydantic_available = False
    from dataclasses import dataclass, field
    BaseModel = object

try:
    import jwt
    from jwt.exceptions import PyJWTError as JWTError
    _jwt_available = True
except ImportError:
    _jwt_available = False
    jwt = JWTError = None

# Local-development fallback only; production rejects it in JwtKeyRing.from_env.
DEFAULT_JWT_SECRET_KEY = "dev-jwt-secret"  # nosec B105
_LOGGER = logging.getLogger(__name__)


class MutationAuthorizationError(PermissionError):
    """A mutation escaped neither its server policy nor its row scope."""


@dataclass(frozen=True)
class JwtKeyRing:
    """Immutable signing-key selection for active and rotating JWT keys."""

    active_kid: str
    verification_keys: Mapping[str, str]
    issuer: str
    audience: Optional[str]
    algorithm: str
    require_kid: bool

    @classmethod
    def from_env(cls) -> Optional["JwtKeyRing"]:
        raw_keys = str(os.getenv("PIVOT_JWT_KEYS_JSON") or "").strip()
        production = _is_production_environment()
        if raw_keys:
            try:
                parsed = json.loads(raw_keys)
            except json.JSONDecodeError as exc:
                raise RuntimeError("PIVOT_JWT_KEYS_JSON must be a JSON object.") from exc
            if not isinstance(parsed, dict) or not parsed:
                raise RuntimeError("PIVOT_JWT_KEYS_JSON must contain verification keys.")
            keys = {
                str(kid).strip(): str(key)
                for kid, key in parsed.items()
                if str(kid).strip() and isinstance(key, str) and key
            }
            active_kid = str(os.getenv("PIVOT_JWT_ACTIVE_KID") or "").strip()
            if not active_kid or active_kid not in keys:
                raise RuntimeError("PIVOT_JWT_ACTIVE_KID must name a configured verification key.")
        elif JWT_SECRET_KEY:
            if production:
                raise RuntimeError(
                    "PIVOT_JWT_KEYS_JSON and PIVOT_JWT_ACTIVE_KID are required for JWT authentication in production."
                )
            active_kid = "local"
            keys = {active_kid: JWT_SECRET_KEY}
        else:
            return None

        issuer = str(os.getenv("JWT_ISSUER") or "").strip()
        audience = str(os.getenv("JWT_AUDIENCE") or "").strip() or None
        if not issuer:
            raise RuntimeError("JWT_ISSUER must be set whenever JWT authentication is configured.")
        if production and not audience:
            raise RuntimeError("JWT_AUDIENCE must be set for JWT authentication in production.")
        algorithm = str(os.getenv("JWT_ALGORITHM", "HS256") or "").strip()
        if algorithm not in {"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}:
            raise RuntimeError("JWT_ALGORITHM is not an allowed signing algorithm.")
        if production and algorithm.startswith("HS"):
            weak_kids = sorted(
                kid for kid, key in keys.items() if len(key.encode("utf-8")) < 32
            )
            if weak_kids:
                raise RuntimeError(
                    "Production HMAC JWT verification keys must be at least 32 UTF-8 bytes "
                    f"(weak key ids: {', '.join(weak_kids)})."
                )
        if production and algorithm.startswith(("RS", "ES")):
            invalid_kids = sorted(
                kid
                for kid, key in keys.items()
                if "-----BEGIN PUBLIC KEY-----" not in key
                and "-----BEGIN CERTIFICATE-----" not in key
            )
            if invalid_kids:
                raise RuntimeError(
                    "Production asymmetric JWT public verification keys must be PEM encoded "
                    f"(invalid key ids: {', '.join(invalid_kids)})."
                )
        return cls(
            active_kid=active_kid,
            verification_keys=MappingProxyType(keys),
            issuer=issuer,
            audience=audience,
            algorithm=algorithm,
            require_kid=production,
        )

    def key_for_token(self, token: str) -> str:
        try:
            header = jwt.get_unverified_header(token)
        except Exception as exc:
            raise JWTError("Invalid JWT header.") from exc
        kid = str((header or {}).get("kid") or "").strip()
        if not kid and not self.require_kid:
            kid = self.active_kid
        if not kid or kid not in self.verification_keys:
            raise JWTError("Unknown JWT signing key.")
        key = self.verification_keys[kid]
        if self.algorithm.startswith(("RS", "ES")) and "-----BEGIN CERTIFICATE-----" in key:
            # Certificates are an existing key-ring input; PyJWT expects the
            # embedded public key, rather than the X.509 envelope.
            from cryptography import x509
            from cryptography.hazmat.primitives import serialization
            try:
                public_key = x509.load_pem_x509_certificate(key.encode("utf-8")).public_key()
                return public_key.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")
            except ValueError as exc:
                raise JWTError("Invalid JWT verification certificate.") from exc
        return key


def _is_production_environment() -> bool:
    return resolve_runtime_environment() == "production"


def _is_development_auth_environment() -> bool:
    return resolve_runtime_environment() == "development"


def _allow_development_auth_fallbacks() -> bool:
    return os.getenv("ALLOW_DEV_AUTH") == "1" and _is_development_auth_environment()


def _get_development_auth_fallback_user() -> "User":
    _LOGGER.warning(
        "Using development authentication fallback; disable ALLOW_DEV_AUTH outside local development."
    )
    return _get_dev_user()


def _load_jwt_secret_key() -> Optional[str]:
    configured_secret = os.getenv("JWT_SECRET_KEY")
    if configured_secret and configured_secret != DEFAULT_JWT_SECRET_KEY:
        if _is_production_environment() and len(configured_secret.encode("utf-8")) < 32:
            raise RuntimeError("Production JWT_SECRET_KEY must be at least 32 UTF-8 bytes.")
        return configured_secret
    # Production JWT verification is normally supplied by a rotating key ring.
    # Do not require an unused compatibility secret when the ring is configured;
    # ``JwtKeyRing.from_env`` remains the authoritative validator and decoder.
    if str(os.getenv("PIVOT_JWT_KEYS_JSON") or "").strip():
        return None
    if _is_production_environment():
        raise RuntimeError(
            "JWT_SECRET_KEY must be set to a non-default value in production."
        )
    if _allow_development_auth_fallbacks():
        _LOGGER.warning(
            "Using the default JWT secret in an explicitly enabled local "
            "development environment."
        )
        return DEFAULT_JWT_SECRET_KEY
    # JWT authentication remains disabled when it is not configured. This is
    # fail-closed without making API-key-only or embedded library use fail at
    # import time.
    return None


def _load_jwt_issuer(secret_key: Optional[str]) -> Optional[str]:
    """Require an issuer whenever signed JWT authentication is enabled."""
    configured_issuer = str(os.getenv("JWT_ISSUER") or "").strip()
    if secret_key and not configured_issuer:
        raise RuntimeError(
            "JWT_ISSUER must be set whenever JWT authentication is configured."
        )
    return configured_issuer or None


# Configuration
JWT_SECRET_KEY = _load_jwt_secret_key()
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_ISSUER = _load_jwt_issuer(JWT_SECRET_KEY)
PIVOT_API_KEY = os.getenv("PIVOT_API_KEY")


def _load_pivot_api_key() -> Optional[str]:
    """Resolve API key at request time so tests and app factories can set env late."""
    return os.getenv("PIVOT_API_KEY") or PIVOT_API_KEY

class UserRole(str):
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"

if _pydantic_available:
    class User(BaseModel):
        id: str
        username: str
        role: str
        scopes: List[str] = []
        attributes: Dict[str, Any] = {}
else:
    from dataclasses import dataclass, field as _field

    @dataclass
    class User:
        id: str
        username: str
        role: str
        scopes: List[str] = _field(default_factory=list)
        attributes: Dict[str, Any] = _field(default_factory=dict)

async def authenticate_user_credentials(
    *,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
) -> User:
    """Authenticate API-key or bearer credentials at an HTTP or WebSocket boundary."""
    configured_api_key = _load_pivot_api_key()
    
    # 1. API Key Authentication (Service Account)
    if api_key:
        if not configured_api_key:
            # Dev mode fallback if no key configured
            if _allow_development_auth_fallbacks() and api_key == "dev-key":
                return _get_development_auth_fallback_user()
        
        if api_key == configured_api_key:
            # Service account typically has full admin rights or specific scope
            return User(
                id="service-account",
                username="service_admin",
                role=UserRole.ADMIN,
                scopes=["*"],
                attributes={}
            )
            
    # 2. JWT Authentication (Human User / OIDC)
    if token:
        if not _jwt_available:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="JWT authentication support is unavailable",
            )
        try:
            key_ring = JwtKeyRing.from_env()
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="JWT authentication is not configured",
            ) from exc
        if key_ring is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT authentication is not configured",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not key_ring.issuer:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="JWT issuer validation is not configured",
            )
        try:
            # ``verify_exp`` only checks an expiration claim when it exists.
            # Require the claim as well, otherwise a correctly signed bearer
            # token can remain valid forever.
            payload = jwt.decode(
                token,
                key_ring.key_for_token(token),
                algorithms=[key_ring.algorithm],
                issuer=key_ring.issuer,
                audience=key_ring.audience,
                options={
                    "require": ["exp", "iss"] + (["aud"] if key_ring.audience else []),
                },
            )
            username: str = payload.get("sub")
            if username is None:
                raise HTTPException(status_code=401, detail="Invalid token payload")
            
            # Extract RLS attributes from token claims
            # e.g. "https://myapp.com/claims/region": "North"
            attributes = payload.get("pivot_attributes", {})
            role = payload.get("role", UserRole.VIEWER)
            
            return User(
                id=payload.get("uid", username),
                username=username,
                role=role,
                scopes=payload.get("scopes", []),
                attributes=attributes
            )
        except (JWTError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # 3. Development Fallback (if no auth provided and in dev mode)
    if not configured_api_key and _allow_development_auth_fallbacks():
        return _get_development_auth_fallback_user()

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing or invalid authentication credentials"
    )


async def get_current_user(
    connection: HTTPConnection = None,
    api_key: Optional[str] = None,
    token: Optional[str] = None,
) -> User:
    """Authenticate HTTP and WebSocket connections through the same policy.

    ``APIKeyHeader`` and ``OAuth2PasswordBearer`` are HTTP ``Request``-only in
    current FastAPI versions, so using them in the application-wide dependency
    prevents a WebSocket endpoint from ever reaching its own authentication
    boundary. ``HTTPConnection`` covers both protocols while preserving the
    direct ``api_key``/``token`` parameters used by embedded callers and tests.
    """
    if connection is not None:
        api_key = connection.headers.get("X-API-Key")
        authorization = str(connection.headers.get("Authorization") or "").strip()
        token = authorization[7:].strip() if authorization.lower().startswith("bearer ") else None
    return await authenticate_user_credentials(api_key=api_key, token=token)

def _get_dev_user():
    return User(
        id="dev-user",
        username="dev_admin",
        role=UserRole.ADMIN,
        scopes=["*"],
        attributes={} 
    )

from .types.pivot_spec import PivotSpec


def _filter_references_fields(filter_spec: Any, protected_fields: Set[str]) -> bool:
    if not isinstance(filter_spec, dict) or not protected_fields:
        return False
    field_name = filter_spec.get("field")
    if field_name is not None and str(field_name) in protected_fields:
        return True
    return any(
        isinstance(condition, dict)
        and condition.get("field") is not None
        and str(condition.get("field")) in protected_fields
        for condition in (filter_spec.get("conditions") or [])
    )


def get_user_with_rls(user: User = (Depends(get_current_user) if _fastapi_available else None)) -> User:
    """
    Dependency to get user with RLS context applied.
    """
    return user

def apply_rls_to_spec(
    spec: PivotSpec,
    user: User,
    *,
    row_scope: Any = None,
) -> PivotSpec:
    """
    Apply Row-Level Security filters from User attributes to a PivotSpec.
    Modifies the spec in-place and returns it.
    """
    scope_values = getattr(row_scope, "field_values", None)
    if isinstance(scope_values, dict) or hasattr(scope_values, "items"):
        protected_fields = {str(field) for field in scope_values.keys()}
        spec.filters = [
            filter_spec
            for filter_spec in list(spec.filters or [])
            if not _filter_references_fields(filter_spec, protected_fields)
        ]
        for field, values in scope_values.items():
            values = list(values)
            if len(values) > 1:
                spec.filters.append({
                    "field": field,
                    "op": "in",
                    "value": values,
                })
                continue
            spec.filters.append({
                "field": field,
                "op": "=",
                "value": values[0] if values else None,
            })
        return spec
    if user.attributes:
        # Workload accounting is not a dataset column or row-security claim.
        row_attributes = {
            field: value for field, value in user.attributes.items()
            if field != "accounting_tenant"
        }
        protected_fields = {str(field) for field in row_attributes.keys()}
        spec.filters = [
            filter_spec
            for filter_spec in list(spec.filters or [])
            if not _filter_references_fields(filter_spec, protected_fields)
        ]
        for field, value in row_attributes.items():
            if isinstance(value, (list, tuple, set, frozenset)):
                values = list(value)
                if isinstance(value, (set, frozenset)):
                    values = sorted(values, key=lambda item: str(item))
                spec.filters.append({
                    "field": field,
                    "op": "in",
                    "value": values,
                })
                continue
            spec.filters.append({
                "field": field,
                "op": "=",
                "value": value
            })
    return spec

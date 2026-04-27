"""
security.py - Security and Authentication for Scalable Pivot Engine
Supports API Key (Service-to-Service) and OAuth2/JWT (User-to-Service).
"""
import os
import logging
from typing import Optional, List, Dict, Any, Set

try:
    from fastapi import HTTPException, Security, status, Depends
    from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
    _fastapi_available = True
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
except ImportError:
    _fastapi_available = False
    HTTPException = Security = status = Depends = None
    api_key_header = oauth2_scheme = None

try:
    from pydantic import BaseModel
    _pydantic_available = True
except ImportError:
    _pydantic_available = False
    from dataclasses import dataclass, field
    BaseModel = object

try:
    from jose import jwt, JWTError
    _jose_available = True
except ImportError:
    _jose_available = False
    jwt = JWTError = None

DEFAULT_JWT_SECRET_KEY = "dev-jwt-secret"
_PRODUCTION_ENV_VALUES = {"production", "prod"}
_DEVELOPMENT_AUTH_ENV_VALUES = {"development", "local"}
_ENVIRONMENT_VARIABLES = ("ENV", "APP_ENV", "FLASK_ENV", "DASH_ENV")
_LOGGER = logging.getLogger(__name__)


def _is_production_environment() -> bool:
    for variable_name in _ENVIRONMENT_VARIABLES:
        value = str(os.getenv(variable_name) or "").strip().lower()
        if value in _PRODUCTION_ENV_VALUES:
            return True
    return False


def _is_development_auth_environment() -> bool:
    if _is_production_environment():
        return False
    return any(
        str(os.getenv(variable_name) or "").strip().lower() in _DEVELOPMENT_AUTH_ENV_VALUES
        for variable_name in _ENVIRONMENT_VARIABLES
    )


def _allow_development_auth_fallbacks() -> bool:
    return os.getenv("ALLOW_DEV_AUTH") == "1" and _is_development_auth_environment()


def _get_development_auth_fallback_user() -> "User":
    _LOGGER.warning(
        "Using development authentication fallback; disable ALLOW_DEV_AUTH outside local development."
    )
    return _get_dev_user()


def _load_jwt_secret_key() -> str:
    configured_secret = os.getenv("JWT_SECRET_KEY")
    if configured_secret and configured_secret != DEFAULT_JWT_SECRET_KEY:
        return configured_secret
    if _is_production_environment():
        raise RuntimeError(
            "JWT_SECRET_KEY must be set to a non-default value in production."
        )
    return configured_secret or DEFAULT_JWT_SECRET_KEY


# Configuration
JWT_SECRET_KEY = _load_jwt_secret_key()
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
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

async def get_current_user(
    api_key: str = (Security(api_key_header) if _fastapi_available else None),
    token: str = (Security(oauth2_scheme) if _fastapi_available else None)
) -> User:
    """
    Authenticate user via API Key or JWT Token.
    API Key takes precedence.
    """
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
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
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
        except JWTError:
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

def apply_rls_to_spec(spec: PivotSpec, user: User) -> PivotSpec:
    """
    Apply Row-Level Security filters from User attributes to a PivotSpec.
    Modifies the spec in-place and returns it.
    """
    if user.attributes:
        protected_fields = {str(field) for field in user.attributes.keys()}
        spec.filters = [
            filter_spec
            for filter_spec in list(spec.filters or [])
            if not _filter_references_fields(filter_spec, protected_fields)
        ]
        for field, value in user.attributes.items():
            spec.filters.append({
                "field": field,
                "op": "=",
                "value": value
            })
    return spec

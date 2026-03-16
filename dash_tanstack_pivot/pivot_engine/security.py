"""
security.py - Security and Authentication for Scalable Pivot Engine
"""
import os
from typing import Optional, List, Dict, Any

try:
    from fastapi import HTTPException, Security, status, Depends
    from fastapi.security import APIKeyHeader
    from pydantic import BaseModel
    _FASTAPI_AVAILABLE = True
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
except ImportError:
    _FASTAPI_AVAILABLE = False
    # Minimal stubs so the module can be imported without fastapi/pydantic
    HTTPException = None
    Security = lambda x: None  # noqa: E731
    status = type("status", (), {"HTTP_401_UNAUTHORIZED": 401})()
    Depends = lambda x: None  # noqa: E731
    api_key_header = None

    class BaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

class UserRole(str):
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"

class User(BaseModel):
    id: str
    username: str
    role: str
    scopes: List[str] = []
    attributes: Dict[str, Any] = {} # For RLS, e.g. {'region': 'North'}

def get_api_key(api_key_header: str = None) -> str:
    """
    Validate API Key and return it.
    """
    expected_key = os.getenv("PIVOT_API_KEY")
    
    if not expected_key:
        return "dev-key"

    if api_key_header == expected_key:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key"
    )

def get_current_user(api_key: str = None) -> "User":
    """
    Get the current user based on the API Key.
    In a real system, this would look up the key in a DB.
    Here we mock it based on the key or default to Admin if dev mode.
    """
    if api_key == "dev-key":
        return User(
            id="dev-admin",
            username="dev_admin",
            role=UserRole.ADMIN,
            scopes=["*"],
            attributes={}
        )
    
    # Mock user for valid key
    return User(
        id="api-user",
        username="api_user",
        role=UserRole.ADMIN, # Default to admin for the single key
        scopes=["*"],
        attributes={}
    )

from pivot_engine.types.pivot_spec import PivotSpec

def get_user_with_rls(user: "User" = None) -> "User":
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
        # Avoid duplicate filters if possible, but for safety just append
        # Logic to check existing filters could be added here
        for field, value in user.attributes.items():
            spec.filters.append({
                "field": field,
                "op": "=",
                "value": value
            })
    return spec
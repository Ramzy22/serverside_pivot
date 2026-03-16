"""
security.py - Security and Authentication for Scalable Pivot Engine
Supports API Key (Service-to-Service) and OAuth2/JWT (User-to-Service).
"""
import os
from typing import Optional, List, Dict, Any
from fastapi import HTTPException, Security, status, Depends
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
from pydantic import BaseModel
from jose import jwt, JWTError

# Define authentication schemes
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

# Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-jwt-secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
PIVOT_API_KEY = os.getenv("PIVOT_API_KEY")

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

async def get_current_user(
    api_key: str = Security(api_key_header),
    token: str = Security(oauth2_scheme)
) -> User:
    """
    Authenticate user via API Key or JWT Token.
    API Key takes precedence.
    """
    
    # 1. API Key Authentication (Service Account)
    if api_key:
        if not PIVOT_API_KEY:
            # Dev mode fallback if no key configured
            if api_key == "dev-key":
                return _get_dev_user()
        
        if api_key == PIVOT_API_KEY:
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
    if not PIVOT_API_KEY and os.getenv("ENV") != "production":
        return _get_dev_user()

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

from pivot_engine.types.pivot_spec import PivotSpec

def get_user_with_rls(user: User = Depends(get_current_user)) -> User:
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
        for field, value in user.attributes.items():
            # Check if filter already exists to avoid duplication?
            # For now, simplistic append. 
            spec.filters.append({
                "field": field,
                "op": "=",
                "value": value
            })
    return spec

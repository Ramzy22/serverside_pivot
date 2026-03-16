"""
Admin API for Pivot Engine Management
"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any
import os
import psutil
import time
from datetime import datetime

from pivot_engine.security import get_current_user, User, UserRole
from pivot_engine.lifecycle import get_task_manager
from pivot_engine.config import get_config

admin_router = APIRouter(prefix="/admin", tags=["admin"])

def require_admin(user: User = Depends(get_current_user)):
    """Dependency to ensure the user is an admin"""
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return user

@admin_router.get("/system/health", dependencies=[Depends(require_admin)])
async def get_system_health():
    """Get system resource usage and health status"""
    process = psutil.Process(os.getpid())
    config = get_config()
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": time.time() - process.create_time(),
        "resources": {
            "cpu_percent": process.cpu_percent(),
            "memory_usage_mb": process.memory_info().rss / 1024 / 1024,
            "system_cpu": psutil.cpu_percent(),
            "system_memory": psutil.virtual_memory().percent
        },
        "configuration": {
            "backend": config.backend_type,
            "cache": "redis" if config.redis_host else "memory",
            "features": {
                "streaming": config.enable_streaming,
                "cdc": config.enable_cdc,
                "incremental": config.enable_incremental_views
            }
        }
    }

@admin_router.get("/jobs/background", dependencies=[Depends(require_admin)])
async def list_background_tasks():
    """List active background tasks managed by TaskManager"""
    task_manager = get_task_manager()
    tasks = []
    
    for task in task_manager.active_tasks:
        tasks.append({
            "name": task.get_name(),
            "cancelled": task.cancelled(),
            "done": task.done()
        })
        
    return {"active_count": len(tasks), "tasks": tasks}

@admin_router.get("/cache/stats", dependencies=[Depends(require_admin)])
async def get_cache_stats():
    """Get cache statistics (if available)"""
    # This assumes we can access the controller from somewhere, or we check global state.
    # For now, we return mock/placeholder or connect to Redis if configured.
    config = get_config()
    stats = {"type": "memory", "keys": 0, "hit_rate": 0}
    
    if config.redis_host:
        stats["type"] = "redis"
        try:
            import redis
            r = redis.StrictRedis(host=config.redis_host, port=config.redis_port, db=0)
            info = r.info()
            stats["keys"] = r.dbsize()
            stats["used_memory_human"] = info.get("used_memory_human")
            stats["connected_clients"] = info.get("connected_clients")
        except:
            stats["status"] = "unreachable"
            
    return stats

# Mock Data for demo purposes - in production, connect to real persistence
MOCK_API_KEYS = [
    {"id": "key_1", "name": "Dashboard App", "role": "editor", "created": "2023-01-01"},
    {"id": "key_2", "name": "Analyst Script", "role": "viewer", "created": "2023-02-15"},
]

@admin_router.get("/security/keys", dependencies=[Depends(require_admin)])
async def list_api_keys():
    """List active API keys"""
    return MOCK_API_KEYS

@admin_router.post("/security/keys", dependencies=[Depends(require_admin)])
async def generate_api_key(name: str, role: str = "viewer"):
    """Generate a new API key"""
    import secrets
    new_key = {
        "id": f"key_{len(MOCK_API_KEYS) + 1}",
        "name": name,
        "role": role,
        "key": secrets.token_urlsafe(32),
        "created": datetime.now().isoformat()
    }
    MOCK_API_KEYS.append(new_key)
    return new_key

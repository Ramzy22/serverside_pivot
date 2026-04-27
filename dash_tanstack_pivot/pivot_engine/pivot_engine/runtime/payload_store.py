"""Runtime payload storage for large Dash transport responses."""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from ..cache_coordinator import CacheCoordinator, CacheNamespaces


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:
            pass
    return str(value)


@dataclass
class RuntimePayload:
    token: str
    body: bytes
    content_type: str
    expires_at: float
    metadata: Dict[str, Any]
    file_path: Optional[str] = None
    size: int = 0


class RuntimePayloadStore:
    """Small in-process payload store used to keep large rows out of Dash props."""

    def __init__(
        self,
        *,
        default_ttl_seconds: int = 120,
        max_entries: int = 256,
        max_bytes: int = 128 * 1024 * 1024,
        cache_coordinator: Optional[CacheCoordinator] = None,
    ) -> None:
        self.default_ttl_seconds = max(1, int(default_ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(1024, int(max_bytes))
        self.cache_coordinator = cache_coordinator or CacheCoordinator()
        self._items: OrderedDict[str, RuntimePayload] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()
        self.cache_coordinator.register_namespace(
            CacheNamespaces.RUNTIME_PAYLOAD,
            owner="runtime",
            key_dimensions=("payload_token", "content_type", "request_id", "state_epoch"),
            max_entries=self.max_entries,
            max_bytes=self.max_bytes,
            ttl_seconds=self.default_ttl_seconds,
            size_getter=self._size_snapshot,
            invalidator=lambda _event: self.clear(),
        )

    def put_json(
        self,
        payload: Dict[str, Any],
        *,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        body = json.dumps(payload, separators=(",", ":"), default=_json_default).encode("utf-8")
        token = secrets.token_urlsafe(24)
        now = time.time()
        ttl = max(1, int(ttl_seconds or self.default_ttl_seconds))
        item = RuntimePayload(
            token=token,
            body=body,
            content_type="application/json",
            expires_at=now + ttl,
            metadata=dict(metadata or {}),
            size=len(body),
        )
        with self._lock:
            expired_count, expired_bytes = self._cleanup_expired_locked(now)
            self._items[token] = item
            self._items.move_to_end(token)
            self._bytes += len(body)
            evicted_count, evicted_bytes = self._evict_locked()
            self._record_expirations(expired_count, expired_bytes)
            self._record_store()
            self._record_evictions(evicted_count, evicted_bytes)
        return {
            "id": token,
            "format": "json",
            "url": f"/_dash_tanstack_pivot/payload/{token}",
            "bytes": len(body),
            "expiresAt": int(item.expires_at * 1000),
            **item.metadata,
        }

    def put_bytes(
        self,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        if isinstance(body, str):
            body = body.encode("utf-8")
        elif not isinstance(body, (bytes, bytearray)):
            body = bytes(body or b"")
        body_bytes = bytes(body)
        token = secrets.token_urlsafe(24)
        now = time.time()
        ttl = max(1, int(ttl_seconds or self.default_ttl_seconds))
        item = RuntimePayload(
            token=token,
            body=body_bytes,
            content_type=str(content_type or "application/octet-stream"),
            expires_at=now + ttl,
            metadata=dict(metadata or {}),
            size=len(body_bytes),
        )
        with self._lock:
            expired_count, expired_bytes = self._cleanup_expired_locked(now)
            self._items[token] = item
            self._items.move_to_end(token)
            self._bytes += len(body_bytes)
            evicted_count, evicted_bytes = self._evict_locked()
            self._record_expirations(expired_count, expired_bytes)
            self._record_store()
            self._record_evictions(evicted_count, evicted_bytes)
        return {
            "id": token,
            "format": "bytes",
            "url": f"/_dash_tanstack_pivot/payload/{token}",
            "bytes": len(body_bytes),
            "contentType": item.content_type,
            "expiresAt": int(item.expires_at * 1000),
            **item.metadata,
        }

    def put_file(
        self,
        path: str,
        *,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        file_path = os.path.abspath(path)
        size = os.path.getsize(file_path)
        token = secrets.token_urlsafe(24)
        now = time.time()
        ttl = max(1, int(ttl_seconds or self.default_ttl_seconds))
        item = RuntimePayload(
            token=token,
            body=b"",
            content_type=str(content_type or "application/octet-stream"),
            expires_at=now + ttl,
            metadata=dict(metadata or {}),
            file_path=file_path,
            size=size,
        )
        with self._lock:
            expired_count, expired_bytes = self._cleanup_expired_locked(now)
            self._items[token] = item
            self._items.move_to_end(token)
            self._bytes += size
            evicted_count, evicted_bytes = self._evict_locked()
            self._record_expirations(expired_count, expired_bytes)
            self._record_store()
            self._record_evictions(evicted_count, evicted_bytes)
        return {
            "id": token,
            "format": "file",
            "url": f"/_dash_tanstack_pivot/payload/{token}",
            "bytes": size,
            "contentType": item.content_type,
            "expiresAt": int(item.expires_at * 1000),
            **item.metadata,
        }

    def get(self, token: str) -> Optional[RuntimePayload]:
        now = time.time()
        with self._lock:
            expired_count, expired_bytes = self._cleanup_expired_locked(now)
            self._record_expirations(expired_count, expired_bytes)
            item = self._items.get(str(token))
            if item is None:
                self.cache_coordinator.record_lookup(CacheNamespaces.RUNTIME_PAYLOAD, hit=False)
                return None
            if item.expires_at <= now:
                removed_size = self._remove_locked(item.token)
                self._record_expirations(1, removed_size)
                self.cache_coordinator.record_lookup(CacheNamespaces.RUNTIME_PAYLOAD, hit=False, expired=True)
                return None
            self._items.move_to_end(item.token)
            self.cache_coordinator.record_lookup(CacheNamespaces.RUNTIME_PAYLOAD, hit=True)
            return item

    def clear(self) -> None:
        with self._lock:
            removed_count = len(self._items)
            removed_bytes = self._bytes
            for token in list(self._items):
                self._remove_locked(token)
            if removed_count:
                self.cache_coordinator.record_delete(
                    CacheNamespaces.RUNTIME_PAYLOAD,
                    count=removed_count,
                    bytes_removed=removed_bytes,
                    entries=len(self._items),
                    bytes_used=self._bytes,
                    reason="clear",
                )

    def metrics_snapshot(self) -> Dict[str, Any]:
        return self.cache_coordinator.snapshot((CacheNamespaces.RUNTIME_PAYLOAD,))

    def _size_snapshot(self) -> Dict[str, int]:
        with self._lock:
            return {"entries": len(self._items), "bytes": self._bytes}

    def _cleanup_expired_locked(self, now: float) -> tuple[int, int]:
        expired = [token for token, item in self._items.items() if item.expires_at <= now]
        removed_bytes = 0
        for token in expired:
            removed_bytes += self._remove_locked(token)
        return len(expired), removed_bytes

    def _evict_locked(self) -> tuple[int, int]:
        evicted = 0
        removed_bytes = 0
        while len(self._items) > self.max_entries or (self._bytes > self.max_bytes and len(self._items) > 1):
            token, _item = next(iter(self._items.items()))
            removed_bytes += self._remove_locked(token)
            evicted += 1
        return evicted, removed_bytes

    def _remove_locked(self, token: str) -> int:
        item = self._items.pop(token, None)
        if item is not None:
            removed_size = int(item.size or len(item.body or b""))
            self._bytes = max(0, self._bytes - removed_size)
            if item.file_path:
                try:
                    os.remove(item.file_path)
                except OSError:
                    pass
            return removed_size
        return 0

    def _record_store(self) -> None:
        self.cache_coordinator.record_store(
            CacheNamespaces.RUNTIME_PAYLOAD,
            entries=len(self._items),
            bytes_used=self._bytes,
        )

    def _record_evictions(self, count: int, bytes_removed: int) -> None:
        if count:
            self.cache_coordinator.record_eviction(
                CacheNamespaces.RUNTIME_PAYLOAD,
                count=count,
                bytes_removed=bytes_removed,
                entries=len(self._items),
                bytes_used=self._bytes,
            )

    def _record_expirations(self, count: int, bytes_removed: int) -> None:
        if count:
            self.cache_coordinator.record_expiration(
                CacheNamespaces.RUNTIME_PAYLOAD,
                count=count,
                bytes_removed=bytes_removed,
                entries=len(self._items),
                bytes_used=self._bytes,
            )

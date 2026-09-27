"""Runtime payload storage for large Dash transport responses."""

from __future__ import annotations

import gzip
import json
import math
import os
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Protocol

from ..cache_coordinator import CacheCoordinator, CacheNamespaces
from ..hierarchy_cache_registry import estimate_container_bytes
from ..resource_governor import ResourceExhaustedError, ResourceGovernor, ResourceKind
from .distributed import RedisRuntimeState, distributed_runtime_from_env


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except Exception:  # nosec B110
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


class RuntimePayloadTooLarge(ResourceExhaustedError, ValueError):
    """Raised before a distributed payload exceeds its configured byte bound."""

    def __init__(self, message: str = "payload_max_bytes") -> None:
        super().__init__(ResourceKind.PAYLOAD, reason=message)


class PayloadStoreBackend(Protocol):
    """Persistent payload storage used only by the distributed runtime."""

    def put(self, item: RuntimePayload) -> None: ...

    def get(self, token: str) -> Optional[RuntimePayload]: ...

    def delete(self, token: str) -> None: ...

    def clear(self) -> None: ...


class RedisPayloadBackend:
    """Store one versioned, byte-safe runtime payload envelope per Redis key."""

    _FORMAT = b"pivot:runtime-payload:v1\x00"

    def __init__(self, state: RedisRuntimeState) -> None:
        self.state = state

    def _key(self, token: str) -> str:
        return self.state.key("payload", str(token))

    def _mutate(self, *, item: Optional[RuntimePayload] = None,
                envelope: Optional[bytes] = None, token: Optional[str] = None,
                clear: bool = False) -> None:
        """Atomically reconcile expiry, admit bytes, and mutate shared storage.

        WATCH/MULTI provides one namespace-wide reservation across workers.
        The ledger charges serialized envelopes (not Redis allocator overhead).
        Its non-expiring records allow cleanup after payload keys expire.
        """
        from redis.exceptions import WatchError

        ledger_key = self.state.key("payload-budget", "entries")
        counter_key = self.state.key("payload-budget", "bytes")

        def transaction():
            for _attempt in range(32):
                with self.state.client.pipeline() as pipe:
                    try:
                        pipe.watch(ledger_key, counter_key)
                        records = pipe.hgetall(ledger_key)
                        live = {}
                        removed = []
                        now = time.time()
                        for raw_key, raw_record in records.items():
                            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
                            size, expires_at = json.loads(raw_record)
                            if clear or expires_at <= now or not pipe.exists(key):
                                removed.append(key)
                            else:
                                live[key] = int(size)
                        target = self._key(item.token if item is not None else token) if item is not None or token is not None else None
                        total = sum(live.values())
                        previous = live.get(target, 0)
                        charge = len(envelope) if envelope is not None else 0
                        accepted = item is None or total - previous + charge <= self.state.settings.payload_total_max_bytes
                        if accepted and target is not None:
                            total -= previous
                            removed.append(target)
                        pipe.multi()
                        for key in removed:
                            pipe.delete(key)
                            pipe.hdel(ledger_key, key)
                        if accepted and item is not None:
                            ttl = max(1, math.ceil(float(item.expires_at) - now))
                            pipe.set(target, envelope, ex=ttl)
                            pipe.hset(ledger_key, target, json.dumps([charge, item.expires_at]))
                            total += charge
                        pipe.set(counter_key, max(0, total))
                        pipe.execute()
                        return accepted
                    except WatchError:
                        continue
            return False

        accepted = self.state._call(transaction)
        if not accepted:
            raise ResourceExhaustedError(ResourceKind.PAYLOAD, reason="payload_total_bytes")

    @staticmethod
    def _serialize_header(item: RuntimePayload, body_size: int) -> bytes:
        return json.dumps(
            {
                "content_type": str(item.content_type),
                "expires_at": float(item.expires_at),
                "metadata": dict(item.metadata or {}),
                "size": body_size,
            },
            default=_json_default,
            separators=(",", ":"),
        ).encode("utf-8")

    def file_body_limit(self, item: RuntimePayload) -> int:
        """Bound a file read by the body capacity of an empty namespace."""
        settings = self.state.settings
        limit = min(settings.payload_max_bytes, settings.payload_total_max_bytes)
        # The largest possible body's size field reserves enough decimal digits
        # even if the file grows after stat. Use the actual header fields so JSON
        # escaping and variable metadata/content types are included in the bound.
        overhead = len(self._FORMAT) + 4 + len(self._serialize_header(item, limit))
        return min(limit, settings.payload_total_max_bytes - overhead)

    def put(self, item: RuntimePayload) -> None:
        body = bytes(item.body or b"")
        if len(body) > self.state.settings.payload_max_bytes:
            raise RuntimePayloadTooLarge(
                "Runtime payload exceeds PIVOT_RUNTIME_PAYLOAD_MAX_BYTES."
            )
        header = self._serialize_header(item, len(body))
        envelope = self._FORMAT + len(header).to_bytes(4, "big") + header + body
        self._mutate(item=item, envelope=envelope)

    def get(self, token: str) -> Optional[RuntimePayload]:
        raw = self.state._call(lambda: self.state.client.get(self._key(token)))
        if not isinstance(raw, bytes) or not raw.startswith(self._FORMAT):
            self._mutate()
            return None
        header_size_offset = len(self._FORMAT)
        header_start = header_size_offset + 4
        if len(raw) < header_start:
            return None
        header_size = int.from_bytes(raw[header_size_offset:header_start], "big")
        header_end = header_start + header_size
        if header_size <= 0 or header_end > len(raw):
            return None
        try:
            header = json.loads(raw[header_start:header_end].decode("utf-8"))
            body = raw[header_end:]
            expires_at = float(header["expires_at"])
            metadata = header["metadata"]
            content_type = header["content_type"]
            size = int(header["size"])
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if (
            expires_at <= time.time()
            or not isinstance(metadata, dict)
            or not isinstance(content_type, str)
            or size != len(body)
        ):
            self._mutate()
            return None
        return RuntimePayload(
            token=str(token),
            body=body,
            content_type=content_type,
            expires_at=expires_at,
            metadata=metadata,
            file_path=None,
            size=size,
        )

    def delete(self, token: str) -> None:
        self._mutate(token=str(token))

    def clear(self) -> None:
        self._mutate(clear=True)


class RuntimePayloadStore:
    """Payload store with an in-process default and injectable shared backend."""

    def __init__(
        self,
        *,
        default_ttl_seconds: int = 120,
        max_entries: int = 256,
        max_bytes: int = 128 * 1024 * 1024,
        cache_coordinator: Optional[CacheCoordinator] = None,
        backend: Optional[PayloadStoreBackend] = None,
        governor: Optional[ResourceGovernor] = None,
    ) -> None:
        self.default_ttl_seconds = max(1, int(default_ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(1, int(max_bytes))
        self.governor = governor
        self.cache_coordinator = cache_coordinator or CacheCoordinator()
        self._backend = backend
        self._items: OrderedDict[str, RuntimePayload] = OrderedDict()
        self._charges = {}
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
        serialized_body: Optional[bytes] = None,
        compress: bool = False,
        url_prefix: str = "/_dash_tanstack_pivot/payload",
    ) -> Dict[str, Any]:
        raw_body = (
            bytes(serialized_body)
            if serialized_body is not None
            else self.serialize_json(payload)
        )
        body = gzip.compress(raw_body, compresslevel=1) if compress else raw_body
        item_metadata = dict(metadata or {})
        if compress:
            item_metadata["contentEncoding"] = "gzip"
            item_metadata["uncompressedBytes"] = len(raw_body)
        token = secrets.token_urlsafe(24)
        now = time.time()
        ttl = max(1, int(ttl_seconds or self.default_ttl_seconds))
        item = RuntimePayload(
            token=token,
            body=body,
            content_type="application/json",
            expires_at=now + ttl,
            metadata=item_metadata,
            size=len(body),
        )
        self._store_item(item, now)
        return {
            "id": token,
            "format": "json",
            "url": f"{url_prefix.rstrip('/')}/{token}",
            "bytes": len(body),
            "expiresAt": int(item.expires_at * 1000),
            **item.metadata,
        }

    @staticmethod
    def serialize_json(payload: Dict[str, Any]) -> bytes:
        """Encode a JSON payload once so callers can size it before storage."""
        return json.dumps(payload, separators=(",", ":"), default=_json_default).encode("utf-8")

    def put_bytes(
        self,
        body: bytes,
        *,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
        url_prefix: str = "/_dash_tanstack_pivot/payload",
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
        self._store_item(item, now)
        return {
            "id": token,
            "format": "bytes",
            "url": f"{url_prefix.rstrip('/')}/{token}",
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
        url_prefix: str = "/_dash_tanstack_pivot/payload",
    ) -> Dict[str, Any]:
        file_path = os.path.abspath(path)
        size = os.path.getsize(file_path)
        token = secrets.token_urlsafe(24)
        now = time.time()
        ttl = max(1, int(ttl_seconds or self.default_ttl_seconds))
        if self._backend is not None:
            item = RuntimePayload(
                token=token,
                body=b"",
                content_type=str(content_type or "application/octet-stream"),
                expires_at=now + ttl,
                metadata=dict(metadata or {}),
                file_path=None,
                size=size,
            )
            limit = self.max_bytes
            if isinstance(self._backend, RedisPayloadBackend):
                limit = self._backend.file_body_limit(item)
            if size > limit:
                raise ResourceExhaustedError(ResourceKind.PAYLOAD, reason="payload_bytes")
            with open(file_path, "rb") as source:
                body = source.read(limit + 1)
            if len(body) > limit:
                raise ResourceExhaustedError(ResourceKind.PAYLOAD, reason="payload_bytes")
            item.body = body
            item.size = len(body)
            self._store_item(item, now)
            try:
                os.remove(file_path)
            except OSError:
                pass
            return {
                "id": token,
                "format": "file",
                "url": f"{url_prefix.rstrip('/')}/{token}",
                "bytes": len(body),
                "contentType": item.content_type,
                "expiresAt": int(item.expires_at * 1000),
                **item.metadata,
            }
        item = RuntimePayload(
            token=token,
            body=b"",
            content_type=str(content_type or "application/octet-stream"),
            expires_at=now + ttl,
            metadata=dict(metadata or {}),
            file_path=file_path,
            size=size,
        )
        self._store_item(item, now)
        return {
            "id": token,
            "format": "file",
            "url": f"{url_prefix.rstrip('/')}/{token}",
            "bytes": size,
            "contentType": item.content_type,
            "expiresAt": int(item.expires_at * 1000),
            **item.metadata,
        }

    def get(self, token: str) -> Optional[RuntimePayload]:
        if self._backend is not None:
            item = self._backend.get(str(token))
            self.cache_coordinator.record_lookup(
                CacheNamespaces.RUNTIME_PAYLOAD,
                hit=item is not None,
            )
            return item
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
        if self._backend is not None:
            self._backend.clear()
            self.cache_coordinator.record_delete(
                CacheNamespaces.RUNTIME_PAYLOAD,
                reason="clear",
            )
            return
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

    def delete(self, token: str) -> None:
        if self._backend is not None:
            self._backend.delete(str(token))
            return
        with self._lock:
            existed = str(token) in self._items
            removed = self._remove_locked(str(token))
            if existed:
                self.cache_coordinator.record_delete(
                    CacheNamespaces.RUNTIME_PAYLOAD, count=1, bytes_removed=removed,
                    entries=len(self._items), bytes_used=self._bytes,
                )

    def metrics_snapshot(self) -> Dict[str, Any]:
        return self.cache_coordinator.snapshot((CacheNamespaces.RUNTIME_PAYLOAD,))

    def _size_snapshot(self) -> Dict[str, int]:
        if self._backend is not None:
            return {}
        with self._lock:
            return {"entries": len(self._items), "bytes": self._bytes}

    def _store_item(self, item: RuntimePayload, now: float) -> None:
        if self._backend is not None:
            self._backend.put(item)
            self.cache_coordinator.record_store(CacheNamespaces.RUNTIME_PAYLOAD)
            return
        charge = estimate_container_bytes((item.body, item.token, item.content_type,
                                           item.metadata, item.file_path))
        if charge is None or charge > self.max_bytes:
            raise ResourceExhaustedError(ResourceKind.PAYLOAD, reason="payload_bytes")
        with self._lock:
            expired_count, expired_bytes = self._cleanup_expired_locked(now)
            self._remove_locked(item.token, delete_file=False)
            evicted_count = evicted_bytes = 0
            while self._items and (len(self._items) >= self.max_entries or self._bytes + charge > self.max_bytes):
                evicted_bytes += self._remove_locked(next(iter(self._items)))
                evicted_count += 1
            lease = self.governor.reserve(ResourceKind.PAYLOAD, charge) if self.governor and charge else None
            self._items[item.token] = item
            self._items.move_to_end(item.token)
            self._charges[item.token] = (charge, lease)
            self._bytes += charge
            self._record_expirations(expired_count, expired_bytes)
            self._record_store()
            self._record_evictions(evicted_count, evicted_bytes)

    def _cleanup_expired_locked(self, now: float) -> tuple[int, int]:
        expired = [token for token, item in self._items.items() if item.expires_at <= now]
        removed_bytes = 0
        for token in expired:
            removed_bytes += self._remove_locked(token)
        return len(expired), removed_bytes

    def _remove_locked(self, token: str, *, delete_file: bool = True) -> int:
        item = self._items.pop(token, None)
        if item is not None:
            removed_size, lease = self._charges.pop(token)
            self._bytes -= removed_size
            if lease is not None:
                lease.release()
            if item.file_path and delete_file:
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


def runtime_payload_store_from_env(
    *,
    redis_client: Optional[Any] = None,
    runtime_state: Optional[RedisRuntimeState] = None,
    cache_coordinator: Optional[CacheCoordinator] = None,
    default_ttl_seconds: int = 120,
    max_entries: int = 256,
    max_bytes: int = 128 * 1024 * 1024,
) -> RuntimePayloadStore:
    """Build the configured payload store without silent distributed fallback."""

    state = runtime_state or distributed_runtime_from_env(client=redis_client)
    if state is None:
        return RuntimePayloadStore(
            default_ttl_seconds=default_ttl_seconds,
            max_entries=max_entries,
            max_bytes=max_bytes,
            cache_coordinator=cache_coordinator,
        )
    return RuntimePayloadStore(
        default_ttl_seconds=state.settings.payload_ttl_seconds,
        max_bytes=state.settings.payload_max_bytes,
        cache_coordinator=cache_coordinator,
        backend=RedisPayloadBackend(state),
    )

"""Validated Redis primitives for multi-worker Pivot runtime deployments.

This module intentionally owns only short-lived runtime coordination.  It is
not a general cache and it never serializes Python objects: shared values are
explicit JSON or bytes managed by the consuming runtime domain.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlparse

from ..cache_coordinator import CacheInvalidationEvent


_LOGGER = logging.getLogger(__name__)
_ENABLED_VALUES = frozenset({"1", "true", "yes", "on"})
_TLS_VERIFICATION_VALUES = frozenset({"required", "cert_required"})
_KEY_PREFIX_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}\Z")


class DistributedRuntimeConfigurationError(RuntimeError):
    """Raised when a distributed deployment has unsafe or incomplete config."""


class DistributedRuntimeUnavailable(RuntimeError):
    """Raised when required shared state cannot be reached."""


def _positive_int(source: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(source.get(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise DistributedRuntimeConfigurationError(
            f"{name} must be a positive integer."
        ) from exc
    if value <= 0:
        raise DistributedRuntimeConfigurationError(f"{name} must be a positive integer.")
    return value


@dataclass(frozen=True)
class DistributedRuntimeSettings:
    """Environment-owned settings for the shared runtime-state store."""

    enabled: bool
    redis_url: str = ""
    key_prefix: str = ""
    tls_verify: str = "required"
    payload_ttl_seconds: int = 120
    payload_max_bytes: int = 128 * 1024 * 1024
    payload_total_max_bytes: int = 128 * 1024 * 1024

    @classmethod
    def from_env(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "DistributedRuntimeSettings":
        source = os.environ if environ is None else environ
        enabled = str(source.get("PIVOT_DISTRIBUTED_RUNTIME", "0") or "").strip().lower() in _ENABLED_VALUES
        if not enabled:
            return cls(enabled=False)

        redis_url = str(source.get("PIVOT_RUNTIME_REDIS_URL") or "").strip()
        prefix = str(source.get("PIVOT_RUNTIME_REDIS_PREFIX") or "").strip()
        tls_verify = str(source.get("PIVOT_RUNTIME_REDIS_TLS_VERIFY", "required") or "").strip().lower()
        parsed = urlparse(redis_url)

        if (
            parsed.scheme != "rediss"
            or not parsed.hostname
            or not parsed.username
            or not parsed.password
        ):
            raise DistributedRuntimeConfigurationError(
                "Distributed runtime requires an authenticated rediss:// PIVOT_RUNTIME_REDIS_URL."
            )
        if not _KEY_PREFIX_RE.fullmatch(prefix):
            raise DistributedRuntimeConfigurationError(
                "Distributed runtime requires a non-empty safe PIVOT_RUNTIME_REDIS_PREFIX."
            )
        if tls_verify not in _TLS_VERIFICATION_VALUES:
            raise DistributedRuntimeConfigurationError(
                "Distributed runtime requires Redis certificate verification."
            )

        return cls(
            enabled=True,
            redis_url=redis_url,
            key_prefix=prefix,
            tls_verify=tls_verify,
            payload_ttl_seconds=_positive_int(
                source, "PIVOT_RUNTIME_PAYLOAD_TTL_SECONDS", 120
            ),
            payload_max_bytes=_positive_int(
                source, "PIVOT_RUNTIME_PAYLOAD_MAX_BYTES", 128 * 1024 * 1024
            ),
            payload_total_max_bytes=_positive_int(
                source, "PIVOT_RUNTIME_PAYLOAD_TOTAL_MAX_BYTES", 128 * 1024 * 1024
            ),
        )


class RedisRuntimeState:
    """One deployment-prefixed Redis namespace for distributed runtime state."""

    _INVALIDATION_CHANNEL = ("cache", "invalidations")
    _GENERATION_KEY = ("cache", "generation")

    def __init__(
        self,
        settings: DistributedRuntimeSettings,
        *,
        client: Optional[Any] = None,
    ) -> None:
        if not settings.enabled:
            raise DistributedRuntimeConfigurationError(
                "RedisRuntimeState requires distributed runtime to be enabled."
            )
        self.settings = settings
        self.client = client if client is not None else self._connect(settings)
        self._subscriptions: list[Callable[[], None]] = []
        self._closed = False
        try:
            self.client.ping()
        except Exception as exc:
            raise DistributedRuntimeUnavailable(
                "Distributed runtime Redis is unavailable."
            ) from exc

    @staticmethod
    def _connect(settings: DistributedRuntimeSettings) -> Any:
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise DistributedRuntimeConfigurationError(
                "Distributed runtime requires the serverside-pivot[redis] extra."
            ) from exc
        try:
            return redis.Redis.from_url(
                settings.redis_url,
                decode_responses=False,
                ssl_cert_reqs=settings.tls_verify,
            )
        except Exception as exc:
            raise DistributedRuntimeUnavailable(
                "Distributed runtime Redis client could not be configured."
            ) from exc

    def _require_open(self) -> None:
        if self._closed:
            raise DistributedRuntimeUnavailable("Distributed runtime state is closed.")

    def key(self, *parts: str) -> str:
        normalized = [str(part).strip(":") for part in parts]
        if not normalized or any(not part for part in normalized):
            raise ValueError("Distributed runtime Redis keys require non-empty parts.")
        return ":".join((self.settings.key_prefix, *normalized))

    def _call(self, operation: Callable[[], Any]) -> Any:
        self._require_open()
        try:
            return operation()
        except DistributedRuntimeUnavailable:
            raise
        except Exception as exc:
            raise DistributedRuntimeUnavailable(
                "Distributed runtime Redis is unavailable."
            ) from exc

    def generation(self) -> int:
        raw = self._call(lambda: self.client.get(self.key(*self._GENERATION_KEY)))
        if raw in (None, b"", ""):
            return 0
        try:
            return max(0, int(raw.decode("ascii") if isinstance(raw, bytes) else raw))
        except (TypeError, ValueError, UnicodeDecodeError) as exc:
            raise DistributedRuntimeUnavailable(
                "Distributed runtime cache generation is invalid."
            ) from exc

    @staticmethod
    def _event_payload(event: CacheInvalidationEvent, generation: int) -> bytes:
        return json.dumps(
            {
                "version": 1,
                "generation": generation,
                "event": event.event,
                "namespaces": list(event.namespaces),
                "reason": event.reason,
                "table": event.table,
                "structural": event.structural,
                "metadata": event.metadata,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    @staticmethod
    def _decode_event(raw: Any) -> Optional[CacheInvalidationEvent]:
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not isinstance(raw, bytes):
            return None
        try:
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or value.get("version") != 1:
                return None
            namespaces = value.get("namespaces")
            if not isinstance(namespaces, list) or not all(
                isinstance(namespace, str) and namespace for namespace in namespaces
            ):
                return None
            generation = int(value.get("generation"))
            if generation < 1 or not isinstance(value.get("event"), str):
                return None
            metadata = value.get("metadata")
            return CacheInvalidationEvent(
                sequence=generation,
                event=value["event"],
                namespaces=tuple(namespaces),
                reason=str(value.get("reason") or ""),
                table=(str(value["table"]) if value.get("table") is not None else None),
                structural=bool(value.get("structural", False)),
                metadata=dict(metadata) if isinstance(metadata, dict) else {},
            )
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    def invalidate(self, event: CacheInvalidationEvent) -> int:
        def publish() -> int:
            generation = int(self.client.incr(self.key(*self._GENERATION_KEY)))
            self.client.publish(
                self.key(*self._INVALIDATION_CHANNEL),
                self._event_payload(event, generation),
            )
            return generation

        return int(self._call(publish))

    def subscribe(
        self,
        callback: Callable[[CacheInvalidationEvent], None],
    ) -> Callable[[], None]:
        """Subscribe a best-effort local invalidator; generation polling is authoritative."""

        self._require_open()
        try:
            pubsub = self.client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(self.key(*self._INVALIDATION_CHANNEL))
        except Exception as exc:
            raise DistributedRuntimeUnavailable(
                "Distributed runtime invalidation subscription is unavailable."
            ) from exc

        stop = threading.Event()

        def run() -> None:
            while not stop.is_set():
                try:
                    message = pubsub.get_message(timeout=0.2)
                except Exception:
                    _LOGGER.warning(
                        "event=distributed_invalidation_receive_failed action=stop"
                    )
                    return
                if not isinstance(message, dict) or message.get("type") != "message":
                    continue
                event = self._decode_event(message.get("data"))
                if event is not None:
                    try:
                        callback(event)
                    except Exception:
                        # Callback exceptions are local observer failures. The
                        # generation guard remains authoritative, so report
                        # the failure and accept later invalidations.
                        _LOGGER.warning(
                            "event=distributed_invalidation_callback_failed action=continue"
                        )

        worker = threading.Thread(target=run, name="pivot-runtime-redis", daemon=True)
        worker.start()

        def close() -> None:
            if stop.is_set():
                return
            stop.set()
            try:
                pubsub.unsubscribe(self.key(*self._INVALIDATION_CHANNEL))
            except Exception:
                _LOGGER.warning(
                    "event=distributed_invalidation_unsubscribe_failed action=continue"
                )
            try:
                pubsub.close()
            except Exception:
                _LOGGER.warning(
                    "event=distributed_invalidation_close_failed action=continue"
                )
            worker.join(timeout=1.0)

        self._subscriptions.append(close)
        return close

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subscription in tuple(self._subscriptions):
            subscription()
        self._subscriptions.clear()


def distributed_runtime_from_env(*, client: Optional[Any] = None) -> Optional[RedisRuntimeState]:
    """Return shared runtime state only when distributed mode is explicitly enabled."""

    settings = DistributedRuntimeSettings.from_env()
    if not settings.enabled:
        return None
    return RedisRuntimeState(settings, client=client)

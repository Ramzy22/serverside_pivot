# Distributed Runtime Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make payload delivery, causal request ordering, and cache invalidation correct across multiple application workers while explicitly disabling editing in that topology.

**Architecture:** A validated `DistributedRuntime` object owns one authenticated Redis client, deployment-prefixed keys, payload envelopes, request-gate storage, and monotonic invalidation generations. `RuntimePayloadStore`, `SessionRequestGate`, controller caches, and adapter viewport caches consume narrow interfaces from that object. PostgreSQL-backed editing remains outside this release; all edit/history commands fail before an in-memory edit domain can be reached.

**Tech Stack:** Python 3.10+, redis-py, Redis 7 service in CI, fakeredis for deterministic unit tests, FastAPI, Flask/Dash, pytest.

**Spec:** `docs/superpowers/specs/2026-09-19-distributed-runtime-design.md`

## Global Constraints

- `PIVOT_DISTRIBUTED_RUNTIME=1` must fail closed unless `PIVOT_RUNTIME_REDIS_URL` is an authenticated `rediss://` URL with certificate verification and `PIVOT_RUNTIME_REDIS_PREFIX` is non-empty.
- Redis stores only versioned JSON/binary payload data; no pickle or server-local file path may be deserialized by a different worker.
- Process-local memory remains L1 only. A missed Pub/Sub event must not permit stale data because every L1 read synchronizes a Redis generation.
- Redis payloads are opaque high-entropy tokens, expire with a single TTL, and remain bound to the authenticated HTTP principal or shared Dash session subject.
- In distributed mode, `update`, `transaction`, `undo`, `redo`, and history transaction actions return `editing_not_available_in_distributed_mode` before adapter edit state is accessed.
- The distributed release supports Redis-backed recovery after an **application-worker** restart. Redis loss/flush expires payloads and request ordering by design; it is not transparent recovery.

## Review Focus

- A token issued by worker A and fetched by worker B must return the exact bytes, metadata, and content encoding, including exported files whose original path no longer exists.
- A matching token held by a different authenticated subject must have the identical 404 result as an unknown token.
- A missing, malformed, non-TLS, unauthenticated, or unprefixed Redis configuration must fail distributed startup rather than silently choosing memory storage.
- Worker B must clear cache state after worker A invalidates even when B receives no Pub/Sub notification.
- A distributed edit/history request must not call `adapter.handle_transaction`, `adapter.handle_updates`, or `edit_domain`.

---

### Task 1: Centralize validated distributed-runtime configuration and Redis primitives

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/distributed.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py`
- Modify: `dash_tanstack_pivot/pyproject.toml`
- Modify: `.github/workflows/ci.yml`
- Test: `tests/test_distributed_runtime.py`

**Interfaces:**
- Produces `DistributedRuntimeSettings.from_env(environ: Mapping[str, str] | None = None) -> DistributedRuntimeSettings`.
- Produces `RedisRuntimeState(settings: DistributedRuntimeSettings, client: redis.Redis | None = None)` with `key(*parts: str) -> str`, `generation() -> int`, `invalidate(event: CacheInvalidationEvent) -> int`, `subscribe(callback: Callable[[CacheInvalidationEvent], None]) -> Callable[[], None]`, and `close() -> None`.
- Produces `distributed_runtime_from_env(*, client: Any | None = None) -> RedisRuntimeState | None`; it returns `None` only when distributed mode is disabled and raises `DistributedRuntimeConfigurationError` when enabled configuration is unsafe or Redis cannot be reached.

- [ ] **Step 1: Write failing configuration tests**

```python
def test_distributed_runtime_rejects_missing_or_insecure_redis_configuration(monkeypatch):
    monkeypatch.setenv("PIVOT_DISTRIBUTED_RUNTIME", "1")
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_URL", raising=False)
    monkeypatch.delenv("PIVOT_RUNTIME_REDIS_PREFIX", raising=False)
    with pytest.raises(DistributedRuntimeConfigurationError):
        DistributedRuntimeSettings.from_env()

    monkeypatch.setenv("PIVOT_RUNTIME_REDIS_URL", "redis://localhost/0")
    monkeypatch.setenv("PIVOT_RUNTIME_REDIS_PREFIX", "test")
    with pytest.raises(DistributedRuntimeConfigurationError):
        DistributedRuntimeSettings.from_env()


def test_runtime_state_namespaces_keys_and_generations_with_injected_redis():
    state = RedisRuntimeState(
        DistributedRuntimeSettings(enabled=True, redis_url="rediss://user:pass@example/0", key_prefix="release-a"),
        client=fakeredis.FakeRedis(decode_responses=False),
    )
    assert state.key("payload", "token") == "release-a:payload:token"
    assert state.generation() == 0
    assert state.invalidate(CacheInvalidationEvent(1, "mutation", ("controller.query",), table="sales")) == 1
    assert state.generation() == 1
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k "configuration or namespaces"`

Expected: FAIL because the distributed runtime module and configuration contract do not exist.

- [ ] **Step 3: Implement the configuration and primitives**

```python
@dataclass(frozen=True)
class DistributedRuntimeSettings:
    enabled: bool
    redis_url: str = ""
    key_prefix: str = ""
    payload_ttl_seconds: int = 120
    payload_max_bytes: int = 128 * 1024 * 1024

    @classmethod
    def from_env(cls, environ=None) -> "DistributedRuntimeSettings":
        source = os.environ if environ is None else environ
        enabled = str(source.get("PIVOT_DISTRIBUTED_RUNTIME", "0")).lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return cls(enabled=False)
        redis_url = str(source.get("PIVOT_RUNTIME_REDIS_URL") or "").strip()
        prefix = str(source.get("PIVOT_RUNTIME_REDIS_PREFIX") or "").strip()
        parsed = urlparse(redis_url)
        verified = str(source.get("PIVOT_RUNTIME_REDIS_TLS_VERIFY", "required")).lower()
        if parsed.scheme != "rediss" or not parsed.username or not parsed.password or not parsed.hostname:
            raise DistributedRuntimeConfigurationError("Distributed runtime requires authenticated rediss Redis.")
        if not prefix or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,127}", prefix):
            raise DistributedRuntimeConfigurationError("Distributed runtime requires a safe Redis key prefix.")
        if verified not in {"required", "cert_required"}:
            raise DistributedRuntimeConfigurationError("Distributed runtime requires Redis certificate verification.")
        return cls(enabled=True, redis_url=redis_url, key_prefix=prefix)


class RedisRuntimeState:
    def invalidate(self, event: CacheInvalidationEvent) -> int:
        generation = int(self.client.incr(self.key("cache", "generation")))
        message = json.dumps({
            "version": 1, "generation": generation, "event": event.event,
            "namespaces": list(event.namespaces), "reason": event.reason,
            "table": event.table, "structural": event.structural,
        }, separators=(",", ":")).encode("utf-8")
        self.client.publish(self.key("cache", "invalidations"), message)
        return generation
```

Use `redis.Redis.from_url(settings.redis_url, decode_responses=False, ssl_cert_reqs="required")`. Encode Pub/Sub events with JSON primitive values only; reject malformed inbound events without executing data. Do not create a Redis client or make a network call while distributed mode is disabled. Expose a closeable subscriber so worker shutdown does not leak a background Pub/Sub thread.

- [ ] **Step 4: Add deterministic test dependencies and a real Redis CI service**

Add `fakeredis>=2.0` to the `dev` extra and to CI’s Python dependency installation. Add a Redis 7 service container to `python-tests`, set `PIVOT_TEST_REDIS_URL=redis://localhost:6379/15` for that job, and reserve database 15 only for tests. The production runtime must still reject that non-TLS URL; it is injected directly only by the integration test client.

- [ ] **Step 5: Run focused unit and Redis integration tests**

Run: `python -m pytest -q tests/test_distributed_runtime.py`

Expected: PASS, including a test guarded by `PIVOT_TEST_REDIS_URL` that creates a real `redis.Redis.from_url` client, invalidates once, and observes the generation from a separately constructed state object.

- [ ] **Step 6: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/distributed.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/__init__.py \
  dash_tanstack_pivot/pyproject.toml .github/workflows/ci.yml \
  tests/test_distributed_runtime.py
git commit -m "feat: add validated distributed runtime state"
```

### Task 2: Make runtime payloads shared, restart-safe, and byte-safe

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/payload_store.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py`
- Test: `tests/test_distributed_runtime.py`
- Test: `tests/test_runtime_http_transport.py`
- Test: `tests/test_dash_runtime_callbacks.py`

**Interfaces:**
- Produces `RedisPayloadBackend(state: RedisRuntimeState)` implementing `put(item: RuntimePayload) -> None`, `get(token: str) -> RuntimePayload | None`, and `delete(token: str) -> None`.
- Extends `RuntimePayloadStore(default_ttl_seconds: int = 120, max_entries: int = 256, max_bytes: int = 128 * 1024 * 1024, cache_coordinator: CacheCoordinator | None = None, backend: PayloadStoreBackend | None = None)` while preserving the existing in-memory default for single-worker deployments.
- Produces `runtime_payload_store_from_env(*, redis_client: Any | None = None) -> RuntimePayloadStore`, returning the Redis-backed store only in enabled distributed mode.

- [ ] **Step 1: Write failing cross-worker payload tests**

```python
def test_redis_payload_store_survives_producer_worker_loss(fake_redis):
    state_a = _distributed_state(fake_redis)
    state_b = _distributed_state(fake_redis)
    producer = RuntimePayloadStore(backend=RedisPayloadBackend(state_a), default_ttl_seconds=30)
    consumer = RuntimePayloadStore(backend=RedisPayloadBackend(state_b), default_ttl_seconds=30)

    reference = producer.put_bytes(b"shared export", metadata={"subject": "alice", "filename": "x.csv"})
    del producer
    item = consumer.get(reference["id"])

    assert item is not None
    assert item.body == b"shared export"
    assert item.metadata["subject"] == "alice"


def test_redis_payload_store_copies_export_file_before_worker_local_path_disappears(tmp_path, fake_redis):
    export = tmp_path / "export.xlsx"
    export.write_bytes(b"workbook")
    store_a = RuntimePayloadStore(backend=RedisPayloadBackend(_distributed_state(fake_redis)))
    store_b = RuntimePayloadStore(backend=RedisPayloadBackend(_distributed_state(fake_redis)))
    reference = store_a.put_file(str(export), metadata={"subject": "alice"})
    export.unlink()
    assert store_b.get(reference["id"]).body == b"workbook"
```

Add transport tests that issue an externalized request through gateway A and download it through gateway B as the same principal, then receive 404 for a different principal. Add a Dash test with two Flask app instances configured with the same explicit `PIVOT_DASH_SESSION_SECRET`, a token created through one instance, and retrieval through the other.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k payload`

Expected: FAIL because `RuntimePayloadStore` uses an `OrderedDict` and a worker-local file path.

- [ ] **Step 3: Implement Redis payload envelopes**

```python
class RedisPayloadBackend:
    _FORMAT = b"pivot:runtime-payload:v1\x00"

    def put(self, item: RuntimePayload) -> None:
        if len(item.body) > self.state.settings.payload_max_bytes:
            raise RuntimePayloadTooLarge("Runtime payload exceeds PIVOT_RUNTIME_PAYLOAD_MAX_BYTES.")
        header = json.dumps({
            "content_type": item.content_type, "expires_at": item.expires_at,
            "metadata": item.metadata, "size": item.size,
        }, default=_json_default, separators=(",", ":")).encode("utf-8")
        envelope = self._FORMAT + len(header).to_bytes(4, "big") + header + item.body
        ttl = max(1, math.ceil(item.expires_at - time.time()))
        self.state.client.set(self.state.key("payload", item.token), envelope, ex=ttl)

    def get(self, token: str) -> RuntimePayload | None:
        raw = self.state.client.get(self.state.key("payload", token))
        if not isinstance(raw, bytes) or not raw.startswith(self._FORMAT):
            return None
        header_size_offset = len(self._FORMAT)
        header_size = int.from_bytes(raw[header_size_offset:header_size_offset + 4], "big")
        header_start = header_size_offset + 4
        header = json.loads(raw[header_start:header_start + header_size].decode("utf-8"))
        return RuntimePayload(token=token, body=raw[header_start + header_size:], file_path=None, **header)
```

For Redis-backed `put_file`, read and store the file bytes before returning the reference, set `file_path=None`, and delete the temporary source only after Redis confirms the write. For Redis-backed `clear`, delete only keys below this deployment’s `payload:` prefix using `SCAN`; never run `FLUSHDB`. Preserve gzip headers, content type, size, ETag inputs, and filename metadata. Retain local LRU/byte-budget behavior only in `InMemoryPayloadBackend`; Redis TTL is the cleanup authority in distributed mode.

Make HTTP registration and Dash callback setup call `runtime_payload_store_from_env()` once per host application. In distributed Dash mode, require an explicit `PIVOT_DASH_SESSION_SECRET` of at least 32 bytes and assign it to `app.server.secret_key`; never generate a different process-local secret. Preserve current randomly generated Dash secret behavior only when distributed mode is disabled.

- [ ] **Step 4: Run payload, HTTP, and Dash regression tests**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k payload tests/test_runtime_http_transport.py tests/test_dash_runtime_callbacks.py`

Expected: PASS. Confirm no assertion relies on `RuntimePayload.file_path` after a Redis-backed store write.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/payload_store.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/dash_callbacks.py \
  tests/test_distributed_runtime.py tests/test_runtime_http_transport.py \
  tests/test_dash_runtime_callbacks.py
git commit -m "feat: share runtime payloads across workers"
```

### Task 3: Consolidate Redis-backed request ordering and fail closed

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/session_gate.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`
- Test: `tests/test_distributed_runtime.py`
- Test: `tests/test_session_request_gate.py`
- Test: `tests/test_runtime_request_coordinator.py`

**Interfaces:**
- Changes `SessionRequestGate(runtime_state: RedisRuntimeState | None = None)` to use the shared client/key prefix when provided.
- Removes `PIVOT_SESSION_REDIS_URL` as an independent distributed configuration source; retain it only as a documented single-worker backwards-compatible alias when `PIVOT_DISTRIBUTED_RUNTIME` is disabled.
- Changes `PivotRuntimeService(adapter_getter: Callable[[], Any], session_gate: SessionRequestGate | None = None, debug: bool = False, request_timeout_seconds: float | None = None, circuit_breaker: CircuitBreaker | None = None, circuit_breaker_failures: int = 5, circuit_breaker_cooldown_seconds: float = 30.0, request_coordinator: RuntimeRequestCoordinator | None = None, distributed_runtime: RedisRuntimeState | None = None)` so all lifecycle gates use the same state object passed to host transport registration.

- [ ] **Step 1: Write failing two-worker causal-order tests**

```python
def test_two_workers_share_causal_request_ordering(fake_redis):
    first = SessionRequestGate(runtime_state=_distributed_state(fake_redis))
    second = SessionRequestGate(runtime_state=_distributed_state(fake_redis))

    assert first.register_request("alice", 4, 10, 1, "viewport", "mount")
    assert not second.register_request("alice", 4, 9, 1, "viewport", "mount")
    assert second.response_is_current("alice", 4, 10, 1, "viewport", "mount")


def test_distributed_runtime_never_silently_falls_back_after_redis_failure(fake_redis):
    gate = SessionRequestGate(runtime_state=_distributed_state(fake_redis))
    fake_redis.connection_pool.disconnect()
    with pytest.raises(DistributedRuntimeUnavailable):
        gate.register_request("alice", 1, 1, 0, "viewport")
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k "ordering or falls_back"`

Expected: FAIL because the current gate creates its own optional client from `PIVOT_SESSION_REDIS_URL` and silently switches to its local dictionary.

- [ ] **Step 3: Refactor the gate to consume the central runtime state**

Keep the existing Lua compare-and-set transition exactly, but obtain its client and key with `runtime_state.client` and `runtime_state.key("session", session_id, client_instance)`. Convert Redis read/eval errors to `DistributedRuntimeUnavailable`; do not execute `_sessions` fallback code when `runtime_state` is present. Give `RuntimeHttpGateway` an optional `distributed_runtime` constructor argument and build its payload store and runtime service with the same object so a process cannot accidentally direct payloads and ordering to different Redis namespaces.

- [ ] **Step 4: Run regression tests**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k ordering tests/test_session_request_gate.py tests/test_runtime_request_coordinator.py tests/test_runtime_service.py`

Expected: PASS for both in-memory default and injected Redis paths.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/session_gate.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py \
  tests/test_distributed_runtime.py tests/test_session_request_gate.py \
  tests/test_runtime_request_coordinator.py
git commit -m "feat: coordinate runtime ordering across workers"
```

### Task 4: Invalidate worker-local controller and viewport caches with a shared generation

**Files:**
- Create: `dash_tanstack_pivot/pivot_engine/pivot_engine/cache/generation_guard.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/cache_coordinator.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/cache/memory_cache.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/adapter_viewport_cache.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py`
- Test: `tests/test_distributed_runtime.py`
- Test: `tests/test_cache_coordinator.py`
- Test: `tests/test_controller.py`

**Interfaces:**
- Produces `GenerationGuard(state: RedisRuntimeState | None, clear: Callable[[], None])` with `sync() -> int` and `invalidate(event: CacheInvalidationEvent) -> int`.
- Produces `GenerationGuardedCache(backing: CacheProtocol, guard: GenerationGuard)` that calls `guard.sync()` before every `get`, `set`, `delete`, and `get_all_keys`.
- Extends `CacheCoordinator(max_events: int = 100, generation_guard: GenerationGuard | None = None)` and `AdapterViewportCache(ttl_seconds: float, pivot_catalog_size: int, response_window_size: int, row_block_cache_size: int, cache_coordinator: CacheCoordinator | None = None, generation_guard: GenerationGuard | None = None)`.

- [ ] **Step 1: Write failing missed-Pub/Sub and cross-worker invalidation tests**

```python
def test_worker_b_discards_l1_cache_after_worker_a_invalidation_without_pubsub(fake_redis):
    state_a, state_b = _distributed_state(fake_redis), _distributed_state(fake_redis)
    cache_a = _guarded_memory_cache(state_a)
    cache_b = _guarded_memory_cache(state_b)
    cache_b.set("query", {"sales": 100})

    CacheCoordinator(generation_guard=GenerationGuard(state_a, cache_a.clear)).invalidate(
        event="mutation", reason="test", table="sales"
    )

    assert cache_b.get("query") is None


def test_remote_invalidation_pubsub_clears_viewport_cache_before_next_lookup(fake_redis):
    adapter_a = _adapter_with_distributed_state(_distributed_state(fake_redis))
    adapter_b = _adapter_with_distributed_state(_distributed_state(fake_redis))
    adapter_b._locked_cache_store(adapter_b._response_window_cache, "window", {"rows": []}, 8, 60)
    adapter_a.controller._clear_mutation_caches("sales")
    _wait_until(lambda: adapter_b._locked_cache_lookup(adapter_b._response_window_cache, "window") is None)
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k "invalidation or pubsub"`

Expected: FAIL because each worker’s `CacheCoordinator`, `MemoryCache`, and `AdapterViewportCache` currently own independent generations.

- [ ] **Step 3: Implement monotonic generation synchronization**

`GenerationGuard.sync()` reads the Redis generation. If it differs from the last seen value, it clears its local backing cache and records the new version before returning. `GenerationGuard.invalidate()` atomically increments the remote generation, publishes a versioned event, and clears the originating local cache. The Pub/Sub callback invokes the same local clear path for low latency; it must ignore its own event version and tolerate malformed or duplicate events. Correctness comes from `sync()`, not subscriber delivery.

Wrap controller query/cache access with `GenerationGuardedCache`. Give every `AdapterViewportCache` a guard and call `sync()` before each local lookup, store, center-column read/write, and prefetch state change. Build viewport cache keys from the guard generation after synchronization. Construct controller and adapter guards from the same `RedisRuntimeState` when distributed mode is enabled; retain exact existing behavior when it is disabled. Do not change cache keys to omit tenant/row-scope inputs already encoded by request structure.

- [ ] **Step 4: Run cache regression tests**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k invalidation tests/test_cache_coordinator.py tests/test_controller.py tests/test_runtime_service.py`

Expected: PASS. The test suite must prove behavior both with a delivered Pub/Sub message and with the subscriber explicitly closed before the next worker-B cache read.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/cache/generation_guard.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/cache_coordinator.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/cache/memory_cache.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/scalable_pivot_controller.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/adapter_viewport_cache.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/tanstack_adapter.py \
  tests/test_distributed_runtime.py tests/test_cache_coordinator.py \
  tests/test_controller.py
git commit -m "feat: synchronize worker-local cache generations"
```

### Task 5: Reject editing centrally in distributed mode

**Files:**
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py`
- Modify: `dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py`
- Test: `tests/test_distributed_runtime.py`
- Test: `tests/test_runtime_http_transport.py`
- Test: `tests/test_edit_session_architecture.py`

**Interfaces:**
- Produces `DistributedEditingUnavailable(RuntimeError)` with public code `editing_not_available_in_distributed_mode`.
- `RuntimeRequestRouter.process_async()` returns `PivotServiceResponse(status="error", message="editing_not_available_in_distributed_mode")` for update/transaction/history actions before calling `self._adapter_getter()`.

- [ ] **Step 1: Write failing no-adapter-call tests**

```python
@pytest.mark.asyncio
async def test_distributed_transaction_is_rejected_before_adapter_state_is_accessed(fake_redis):
    accessed = False

    def adapter_getter():
        nonlocal accessed
        accessed = True
        raise AssertionError("editing must be rejected before adapter lookup")

    service = PivotRuntimeService(adapter_getter=adapter_getter, distributed_runtime=_distributed_state(fake_redis))
    response = await service.process_async(_transaction_state(), _transaction_context())

    assert response.status == "error"
    assert response.message == "editing_not_available_in_distributed_mode"
    assert not accessed
```

Add HTTP tests for `kind="update"`, ordinary `kind="transaction"`, and transactions whose payload action is `undo`, `redo`, or `history`. Each returns the stable runtime error payload and never reaches the mutation authorization/adapter execution path.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k editing tests/test_runtime_http_transport.py -k "transaction or update"`

Expected: FAIL because the router currently obtains the adapter and can invoke its edit domain.

- [ ] **Step 3: Implement central capability denial**

Determine trigger kind from `PivotViewState` and `PivotRequestContext` before adapter lookup in `RuntimeRequestRouter.process_async`. When `distributed_runtime is not None`, deny `update`, `transaction`, and payload actions `undo`, `redo`, and `history` with the exact stable error. Keep single-worker authorization and edit behavior unchanged. Have `RuntimeHttpGateway` expose that as a schema-valid `status="error"` response, not an internal exception or a backend-specific message.

- [ ] **Step 4: Run edit and transport regressions**

Run: `python -m pytest -q tests/test_distributed_runtime.py -k editing tests/test_runtime_http_transport.py tests/test_edit_session_architecture.py`

Expected: PASS. Existing non-distributed edit tests remain green.

- [ ] **Step 5: Commit**

```bash
git add dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/router.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/service.py \
  dash_tanstack_pivot/pivot_engine/pivot_engine/runtime/http_transport.py \
  tests/test_distributed_runtime.py tests/test_runtime_http_transport.py \
  tests/test_edit_session_architecture.py
git commit -m "feat: disable editing in distributed runtime mode"
```

### Task 6: Publish operational contract and verify packaged distributed support

**Files:**
- Create: `docs/operations/distributed-runtime.md`
- Modify: `README.md`
- Modify: `scripts/ci/smoke_installed_python_runtime.py`
- Modify: `.github/workflows/ci.yml`
- Test: `tests/test_docs_examples_contract.py`
- Test: `tests/test_distributed_runtime.py`

**Interfaces:**
- Documents required environment variables, Redis persistence/availability expectations, worker-restart behavior, payload TTL/size limits, and the deliberate distributed editing denial.
- Adds `PIVOT_TEST_REDIS_URL` as an integration-test-only variable; it must never appear in application configuration examples.

- [ ] **Step 1: Write failing documentation and installed-package smoke tests**

```python
def test_distributed_runtime_operations_documentation_declares_required_controls():
    text = Path("docs/operations/distributed-runtime.md").read_text(encoding="utf-8")
    for required in (
        "PIVOT_DISTRIBUTED_RUNTIME",
        "PIVOT_RUNTIME_REDIS_URL",
        "PIVOT_RUNTIME_REDIS_PREFIX",
        "PIVOT_DASH_SESSION_SECRET",
        "editing_not_available_in_distributed_mode",
    ):
        assert required in text
```

Extend the wheel smoke script to create two `RuntimePayloadStore` objects against `PIVOT_TEST_REDIS_URL`, store through one and retrieve through the other. It must skip only when that test-only variable is absent, and CI must set it during package smoke.

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest -q tests/test_docs_examples_contract.py -k distributed`

Expected: FAIL because the operational contract and packaged cross-worker smoke do not yet exist.

- [ ] **Step 3: Write operational documentation and wire package smoke**

Document that Redis must be a highly available, authenticated TLS deployment; worker restarts preserve data until Redis TTL; Redis flushes expire outstanding payloads; L1 cache is non-authoritative; exports are stored in Redis bytes rather than local paths; and edits are intentionally unavailable. Include a deployment checklist that requires the same Dash session secret on every worker. Do not claim PostgreSQL edit persistence in this release.

Add the Redis service and `PIVOT_TEST_REDIS_URL` to the package-smoke job so the built wheel is installed in its clean environment before performing the two-worker payload request. Keep the smoke request authenticated and verify a second subject receives 404.

- [ ] **Step 4: Run release verification**

Run: `python -m pytest -q`

Run: `python -m build dash_tanstack_pivot/`

Run: `python scripts/ci/verify_wheel_contract.py dash_tanstack_pivot/dist/*.whl`

Run: `python -m twine check dash_tanstack_pivot/dist/*.whl`

Expected: all tests pass; the distribution builds and validates; CI exercises a real Redis service for runtime and installed-wheel worker handoff.

- [ ] **Step 5: Commit**

```bash
git add docs/operations/distributed-runtime.md README.md \
  scripts/ci/smoke_installed_python_runtime.py .github/workflows/ci.yml \
  tests/test_docs_examples_contract.py tests/test_distributed_runtime.py
git commit -m "docs: publish distributed runtime deployment contract"
```

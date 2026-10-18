# Task 4 independent-review fixes

All four reported blockers were reproduced against commit `a4d2673` before implementing fixes.

## Changes

- Dash export gateways now belong to individual callback registrations. Each retains its own service getter, dataset policy, principal resolver, lazy gateway, and initialization lock. Two pivots can export using distinct services and table policies.
- Gateway lazy job stores, runners, payload stores, and fallback lifecycles are initialized under a reentrant lock. Dash's shared payload-store initialization is also synchronized. Concurrent initial starts keep both jobs reachable for polling.
- Completed Dash job payload metadata includes the owning registration key. Download authorization resolves that registration's principal and export policy, then checks the stored subject, accounting account, and signed browser session. Another pivot's resolver or policy cannot replace these checks.
- Each Dash app registers one idempotent cleanup callable, `app._pivot_runtime_close_export_jobs()`. It closes every registration's gateway, cancels active execution, waits for cleanup, and releases export admission. Closed gateways cannot lazily create new runners.
- Cleanup is also registered for normal interpreter shutdown and app finalization. Python's optional private `threading._register_atexit` facility is isolated in one helper because `ThreadPoolExecutor` joins workers before ordinary `atexit` callbacks. A subprocess regression demonstrated that ordinary `atexit` alone hangs with a running export, then passed with cancellation before thread joining.

## Verification

Failing regressions first reproduced service capture (`[2, 0]` adapter queries instead of `[1, 1]`), first-policy capture, a revoked owner still downloading through another registration, concurrent starts losing a job, duplicate lazy resources, missing shutdown cleanup, and interpreter shutdown hanging until subprocess timeout.

Fresh final command:

```text
python -m pytest tests/test_runtime_http_transport.py tests/test_dash_runtime_callbacks.py tests/test_runtime_dataset_authorization.py tests/test_runtime_payload_store.py tests/test_runtime_contracts.py tests/test_runtime_export_jobs.py tests/test_runtime_export_job_store.py tests/test_runtime_export_job_runner.py -q
```

Result: **263 passed, 13 skipped, 71 warnings in 20.15s**, exit 0. Warnings are Ibis/DuckDB `fetch_arrow_table()` deprecations. Coverage includes existing cross-subject/account/table denial, no backend query on status/cancel, cross-worker cancellation, payload retrieval, runner cleanup, and new per-registration/concurrency/shutdown regressions.

An exploratory `python -m pytest -q` was stopped at the coordinating agent's instruction at approximately 23% after progress showed five setup errors and one failure. It did not emit a final summary before termination, so individual failing node IDs are unavailable from that run. This is not a full-suite pass; full release qualification remains with Task 6. Existing untracked `artifacts/` contents were preserved.

## Lifecycle limits

Flask request teardown is not an application shutdown event and is deliberately not used. Hosts should invoke `app._pivot_runtime_close_export_jobs()` during graceful shutdown. Normal interpreter exit additionally closes work on Python runtimes providing the pre-thread-join hook; without that optional facility, explicit host cleanup is required while jobs are active, and ordinary `atexit`/finalization are best-effort fallbacks. Abrupt process termination or a backend refusing cancellation cannot be claimed as completed cleanup; existing job interruption/lease-expiry behavior still applies. The existing requirement that a Dash app's payload store share its runtime worker governor remains enforced.

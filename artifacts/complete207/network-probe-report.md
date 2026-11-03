# Runtime network observer diagnostics

Base: `49fba3808c332bab252b1591795dbe8c05dd043e`. Branch: `complete207-network-probe`.

Changed only AXIS_PROBE observation, its executable regression test, and this report. Existing request/response trace object construction, browser journeys, readiness/retry behavior, assertions, source pipelines, and binding loaders remain unchanged. No supplemental loader monkeypatch was copied.

Distinct `network-*` phases now record actual fetch HTTP status, V1 error-envelope status/code/message (including HTTP 200), safe resourceKind/retryAfterMs, fetch rejection/abort names, clone/read failures, and XHR load/error/abort/timeout status. The original Response and readable body are returned; original rejection objects propagate unchanged. Synchronous clone failure is contained in the observer rather than replacing a successfully returned response. XHR open/send arguments, receiver and return values remain intact. New phases cannot satisfy existing axis request/response oracles.

Diagnostics retain only the canonical `/v1/runtime` path, bounded known V1 scalar fields and allowlisted exception names. They exclude URL credentials/query/fragment, headers, arbitrary response text/unknown fields, arbitrary exception messages and internal admission reasons. The resource metadata matches `build_resource_exhausted_response` in runtime/contracts.py. Top-level V1 message takes precedence over a payload message.

Validation: Node v22.20.0 executed the actual injected JavaScript through Python pytest. Initial 10 scenarios failed against the baseline for absent diagnostics/clone transparency. An added top-level-message/resource-metadata scenario failed before that correction. Final command: `python -m pytest tests/test_axis_network_probe.py tests/test_flagship_axis_swap.py -q`: **30 passed**. `git diff --check` clean. Regression coverage verifies response identity/body readability, exact rejection identity, HTTP503 and HTTP200 error metadata, abort, clone throw/rejected read, XHR errors/abort/timeout, original request/response records, transport arguments and diagnostic secret exclusion.

No browsers or full suite were run, as requested. No source behavior was changed. The existing official run remains 206/207; the Reactnumber-format second reload gridRetry failure remains unresolved. The six supplemental React reruns are observations only and establish no causal attribution. This commit enables narrower evidence on a future authorized run; it does not claim a release fix or new qualification result.

# Security Boundary Closure Design

## Goal

Close the remaining release-assurance and resource-boundary gaps without
claiming that operator-owned production controls have been performed locally.

## Release evidence

An evidence manifest will attest to a concrete release: immutable tag,
immutable source commit, deployment environment, and HTTPS deployment origin.
It will also contain the SHA-256 of the exact control matrix and the complete
set of control identifiers it covers. The validator receives the expected
values from the protected release workflow and rejects a partial scope,
unrelated tag/commit, untrusted environment, or origin mismatch.

## Security events

A small dependency-free event logger will emit structured warning records for
denied authorization decisions. Events include event type, normalized subject,
dataset, operation, and a stable reason code; they never include claims,
credentials, row values, requests, or exception messages. REST, runtime HTTP,
Flight, Dash drill-through, and realtime subscription boundaries call it when
they deny access.

## External deployment verifier

The verifier will require a two-year HSTS duration with subdomains and preload,
an effective restrictive CSP (`base-uri 'self'`, `frame-ancestors 'none'`, and
`object-src 'none'`), `X-Frame-Options: DENY`, a safe referrer policy, and the
existing no-sniff and permissions policies. Its HTTP redirect target must stay
on the configured HTTPS origin.

## WebSocket and formula limits

The realtime server will generate registry keys after authentication so a
client-provided path identifier cannot own or replace a connection. Formula
evaluation will reject overly long expressions, excessive AST nodes/depth,
large powers, excessive function arguments, and non-finite/intermediate
numeric values before they consume unbounded resources. These are deterministic
per-expression limits, with no process-global timeout or background worker.

## Production evidence

TLS, ingress isolation, IdP configuration, backup restoration, incident
exercise, and penetration testing remain external controls. The release gate
requires current, scope-bound attachments for them; no repository test will
assert that a real deployment has completed them.

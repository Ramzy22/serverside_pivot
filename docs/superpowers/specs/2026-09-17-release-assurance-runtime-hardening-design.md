# Release Assurance and Runtime Hardening Design

## Goal

Close the remaining repository-owned release and runtime security gaps without
representing deployment evidence as completed. A published package must be
blocked unless its software scans and protected operator evidence pass, while
the HTTP and Flight boundaries fail closed in production.

## Decisions

### ASVS evidence contract

The baseline is the released OWASP ASVS 5.0.0 catalog. The repository vendors a
small immutable catalog of the requirements cited by its matrix, including the
upstream release URL and SHA-256. Every matrix mapping names an identifier and
canonical ASVS title from that catalog. An empty matrix, duplicate mapping, or
unknown requirement is invalid. This is an evidence assessment, not an ASVS
certification claim.

### Release gate

The release workflow requires an evidence manifest path supplied by a protected
release environment. It refuses the example manifest and cannot default to a
checked-in success file. The existing verifier continues to check attachment
path containment, checksum, expiry, and required attachment IDs. Therefore TLS
and network deployment, identity-provider setup, tenant isolation in the target
environment, audit-log delivery, recovery exercises, incident exercise, and
security sign-off remain explicitly operator-provided evidence.

Python and JavaScript dependency reports are parsed fail closed: malformed,
partial, skipped, or unreviewed findings fail the gate. Python exceptions must
be exact and current; JavaScript findings currently have no exception path.
Bandit runs as a gate rather than a report-only step. Any remaining inline
suppression has to name its Bandit rule and explain the reviewed safe boundary.

### HTTP request boundary

The fixed-window limiter consumes a token for every application request,
including `GET`, before dispatch. Unsafe cookie-authenticated browser requests
require the configured public origin and an exact double-submit CSRF cookie and
header. Bearer/API-key requests remain usable without a browser CSRF token, but
an unsafe request presenting browser/session cookies is never treated as
non-browser traffic. The response middleware seeds a non-HttpOnly CSRF cookie
only for same-origin browser clients; host integrations must echo it in the
configured request header.

Trusted forwarding accepts only explicitly configured, non-universal CIDRs. A
configuration that trusts the entire IPv4 or IPv6 internet is rejected in
production.

### Arrow Flight

Production Flight requires its location to use `grpc+tls://` and requires a
non-empty TLS certificate configuration in addition to the existing
per-principal authentication resolver and dataset authorizer. Plain Flight and
the shared token path remain development-only / rejected paths.

## Non-goals

Repository code cannot prove that a deployment has real TLS termination,
network segmentation, an IdP configuration, functional backups, or an incident
process. It makes a release fail when the protected operator evidence for those
controls is absent, stale, or altered.

## Verification

Tests prove that an empty or wrong ASVS mapping fails, ignored audit entries
fail, JavaScript audit findings fail, release publishing is ordered after all
gates, GETs are rate-limited, cookie-authenticated unsafe requests require
same-origin double-submit CSRF, universal proxy CIDRs fail configuration, and
production Flight rejects plaintext or missing TLS certificates. The full
Python suite, frontend typecheck/build, security scans, and diff validation are
run before handoff.

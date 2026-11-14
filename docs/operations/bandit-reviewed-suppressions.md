# Reviewed Bandit Suppressions

The release workflow runs Bandit as a blocking scanner.  The narrow `# nosec`
markers in the pivot engine are reviewed exceptions, not a global scanner
exclusion.  This register is part of the release evidence for every change to
one of those lines.

## B608: dynamic SQL construction

The SQL in `controller.py` and `scalable_pivot_controller.py` uses dynamic
identifiers to support a configured dataset schema.  Values remain DB-API
parameters.  Table and column identifiers originate from the backend schema,
the validated pivot specification, or the server-owned transaction
authorization boundary; they are not interpolated directly from HTTP input.
The relevant mutation paths are covered by the tenant and dataset
authorization regression tests.  Any new `B608` marker requires review of
identifier provenance and parameter binding before release.

## B110 and B112: intentional fallback / continuation

These markers are confined to optional backend capabilities, best-effort
resource cleanup, rendering conversion, cache telemetry, and prefetching.
They are not permitted on authorization, identity, mutation, persistence, or
audit-log paths.  The fallback must retain a safe result (for example, use the
next supported representation or leave the primary request unaffected), never
turn an error into authorization.

The locations are in backend capability adapters, layout metadata discovery,
hierarchy roll-up and pruning optimizations, Arrow conversion, and optional
cache/presentation work.  A change that broadens this set needs a named test
showing the safe fallback.

## B105: non-secret sentinel literals

The hierarchy/report tokens are protocol sentinels, not credentials.
`DEFAULT_JWT_SECRET_KEY` is development-only; production startup rejects
missing key configuration and weak production HMAC keys.  It must never be
used as a deployment credential.

## Review procedure

1. Run `bandit -r serverside_pivot/pivot_engine/pivot_engine -q -f json`.
2. Confirm the JSON report has no unsuppressed findings.
3. Review every changed `# nosec` line against this register and the affected
   authorization tests.
4. Reject a new suppression if a structural fix, parameter binding, or a
   narrower exception type is practical.

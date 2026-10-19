# Reproducible read-only release gate

Date: 2026-10-19

## Intent and boundary

Make the existing read-only parity and release qualification executable from a
clean checkout. A passing result must describe the exact built Dash, React,
and Streamlit artifacts and a real AG Grid Enterprise 36.2.0 SSRM browser run.
Do not turn a missing service, skipped required case, stale file, or missing
operator attestation into a pass. Editing remains outside the release promise.

This spec closes harness and test-fixture gaps in the approved
`2026-10-13-read-only-parity-qualification-design.md`; it does not weaken that
spec's requirement for every mandatory mapped workflow to have real evidence.
Production deployment approval remains a separate protected gate because a
local checkout cannot prove a live certificate, network policy, restore,
penetration test, or incident exercise.

## Present failure and chosen approach

The installed-release qualifier creates an `ag_grid` case-envelope path but
never starts the pinned reference app or executes its browser tests. A normal
`pytest` invocation executes those live tests despite no service on port 4179.
Consequently the former reports `case_envelope_invalid` and the latter raises
connection-refused errors. The product browser selection is also narrower
than the mandatory workflow map, so a green nine-case export subset is not a
green read-only release. These are evidence gaps, not parity passes.

Use one managed qualification path: install the pinned reference dependencies
from its tracked lockfile, build its local-only production bundle, start its
SSRM server on an available loopback port, check its config/version and raw
fixture hash, then run the reference browser suite against that server. Record
case observations before converting them to a run/commit/hash-bound envelope.
An unreachable server, bad config, failed browser, or missing case fails the
parity result. The product hosts retain their existing clean wheel/npm build
and loaded-asset checks. Release qualification executes the complete mapped
product browser set; targeted export qualification remains an explicitly
partial subset and cannot qualify the whole release.

The alternatives are rejected: manual server startup is not reproducible;
marking missing reference results `not_applicable` or copying old evidence
would fabricate parity. A single undifferentiated browser suite would make
reference and product failures hard to distinguish.

## Components and ownership

- `reference/ag_grid_36_2` remains an evaluation-only app, never part of the
  product wheel or npm tarball. Its exact package versions and fixture hash are
  checked before any browser case can pass.
- The qualification orchestrator owns reference install/build/start/stop and
  process diagnostics. It allocates a loopback port, waits for readiness, and
  closes only processes it started. It never kills a pre-existing listener.
- The reference browser tests own observable UI/SSRM assertions and raw
  screenshots, traces, clipboard/download parses, browser/driver versions,
  and case-level failure diagnostics.
- The manifest adapter writes an `ag_grid` envelope with the same current
  `run_id`, source commit, complete artifact-hash set, per-file evidence
  hashes, and reference-test IDs as the product envelope. It binds each
  record before evaluation; it does not relabel a `blocked` or failed case.
- The evaluator remains the final authority. It rejects missing, duplicate,
  stale, unbound, skipped, blocked, and failed mandatory cases. The sanitized
  public report omits raw paths, clipboard contents, customer-like data,
  credentials, and exception text; raw evidence stays in the private tree.

## Ordinary tests versus release evidence

Ordinary `pytest` exercises deterministic Python/JS contracts without
requiring an already-running AG Grid app. Live reference tests run only when
the managed qualification explicitly enables them; an ordinary-run skip is
not accepted as a release pass. The release runner must enable and execute
every reference test mapped by the committed contract and reject an incomplete
JUnit or case envelope. This is a test-infrastructure distinction, not a
change to the advertised feature promise.

The drill-through endpoint tests use a small deterministic Arrow table
registered through the real controller at test setup. They must not depend on
the optional Yahoo-backed trader demonstration files or live market data.
The public demo continues to require its explicitly generated files; tests do
not silently substitute synthetic data in production code.

## Packaging and security artifacts

Track the root component `package-lock.json` and use `npm ci` to prove that it
matches `package.json`, including `react-docgen`. Track the already-authored
production security runbook so a clean checkout contains the file its tests
and release process require. The repository's local Git exclude currently
ignores both paths, so adding them must be explicit. Neither generated
`node_modules` nor built AG Grid Enterprise assets are tracked or shipped.

The runbook and local verifier prove only that controls and procedures are
defined. The protected release evidence manifest must still be tied to the
final tag, commit, deployment origin, current scan reports, and actual
operator-owned attachments. No local script writes a fake production sign-off.

## Failure handling and verification

Tests first reproduce: missing reference server, missing bound envelope,
missing root lockfile, missing trader demo files, and absent runbook in a
clean checkout. The fixes must make deterministic tests pass without relying
on files only present in one developer's workspace. Run the pinned reference
unit suite and live browser cases, the complete mapped product browser cases
on installed artifacts, typecheck/build/metadata generation, and the full
Python suite. Save the exact source commit, wheel/npm/bundle/fixture hashes,
browser/host versions, per-case results, and failure traces.

An installation, browser, or reference case failure remains a failed or
blocked qualification. A required skip never counts as pass. If mapped
product behavior is still unimplemented or flaky, report the specific
requirement and leave the release gate red rather than changing the contract
to hide it. External production sign-off remains `not_verified_locally` until
the operator supplies genuine protected evidence for the final artifact.

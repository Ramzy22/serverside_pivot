# Cancellable large exports

## Intent and release boundary

Large CSV/XLSX exports must have truthful progress and a Cancel action that
stops server execution, prevents output publication, cleans temporary files,
and retains query/export admission until cleanup has actually finished. The
same read-only behavior must work in Dash, React, and Streamlit through the
host-neutral runtime protocol. A new export may start after cancellation.

Redis is **optional** for a single-worker installation. A multi-worker
installation that promises cross-worker cancellation uses the existing
distributed Redis configuration. A worker restart interrupts, rather than
resumes, an in-flight export: the user sees **Interrupted — retry** and may
start a fresh export. Neither S3 nor another object store is required for this
release. Editing and resumable downloads are outside scope.

## Chosen approach and alternatives

Use a bounded, server-owned export job with V1 export `start`, `status`, and
`cancel` actions. The job owner performs the existing query and export work;
shared job state carries control and observations, not bulk export bytes.
Keep the existing authenticated payload store for completed files, subject to
its configured per-file and total byte caps. A file that cannot fit the
configured delivery budget fails predictably, without a success notification
or partial browser download. Existing part-export UX remains available.

Browser request abortion alone is rejected because it cannot establish that
native database work stopped. A durable job queue plus S3-compatible output
storage is deferred: it could later add resumable exports, longer retention,
and larger cross-worker downloads, but would impose infrastructure not needed
for truthful cancellation now. The job interface must not encode local file
paths or Redis details so a future artifact-store implementation can replace
the completed-file handoff without changing host UI semantics.

## Protocol and ownership

Extend the existing `schemaVersion: 1`, `kind: "export"` payload with an
explicit action; do not introduce a second runtime protocol version. `start`
uses the current table, snapshot, filters, scope, format, and row/column
selection. The server returns a server-generated opaque `jobId` promptly;
`status` and `cancel` target that ID. Every action reauthenticates the
principal and checks the same dataset export permission. Status and cancel
are bound to the server-resolved subject/account, not to a client-provided
tenant, session, or filename. An unauthorized or unknown ID reveals no job
metadata. Concurrent/replayed starts and cancels are idempotent where safe.

The state machine is:

`queued → running → cancelling → cancelled`

with `running → completed | failed | interrupted`, and queued cancellation
going directly to `cancelled` once no worker owns it. Terminal states never
move back to running. A cancel request may return `cancelling`; only the job
owner may publish `cancelled`, after database cancellation acknowledgement,
writer exit, temporary-file cleanup, and resource-lease release. If completion
wins the race, cancel returns the completed state; it cannot retract an
already published file. No successful result is published after a cancellation
decision wins the race. If native interruption does not acknowledge within the
configured deadline, the job remains `cancelling`, retains admission, and
raises an operator-visible failure signal rather than claiming cleanup.

## Execution, progress, and resource safety

Export execution uses a bounded worker-local executor and the existing
resource governor. Admission is acquired before scheduling, remains charged
through query/writer cleanup, and is released exactly once. The owner checks
the cancellation signal between query batches and writer batches and invokes
the backend's actual interruption/timeout path while a native statement is
running. A mere cancelled Python waiter or closed browser connection is not
accepted as execution cleanup. Temporary output is written in a dedicated
managed directory; only a validated completed file enters the authenticated
payload store. Startup and expiry cleanup remove orphaned temporary files
from that directory without touching unrelated files.

Status contains a monotonic phase, observed rows and bytes, and an optional
total only when the server has a defensible denominator. The UI shows an
indeterminate indicator when totals are unknown; it never derives percent
from elapsed time. Querying, encoding, and delivering may have distinct
phases; encoding XLSX may remain indeterminate when its total work is unknown.
Progress completion is distinct from a successful,
downloadable output. CSV and XLSX keep the existing row/byte limits, numeric
and Unicode fidelity, column ordering, and merged-header behavior. Any
intermediate materialization that cannot obey admission and byte bounds must
reject before a worker exceeds its configured budget.

## Local and distributed coordination

Single-worker mode uses a process-local job registry. Restart loses active
registry entries, so the client treats a missing in-flight job after a lost
connection as interrupted and offers Retry. Completed files keep only the
existing payload-token lifetime.

Distributed mode stores small job records, cancellation intent, owner lease,
and heartbeat in Redis under the deployment namespace. Status and cancel may
reach any worker; only the lease owner touches the native query and temporary
file. State transitions use atomic compare-and-set/fencing so a late owner
cannot publish a result after cancellation or lease loss. On heartbeat expiry
or restart, a different worker reports `interrupted`, not `cancelled` or
`completed`; the user starts a new export. Redis unavailability fails closed
for new distributed jobs and does not turn an unverified cancellation into a
success. Bulk file bytes are never put into the job-status record.

## Shared frontend and host behavior

The shared runtime export command owns start, polling, cancel, and terminal
response handling. Dash, React, and Streamlit use the same V1 action semantics
through their transport adapters. The overlay distinguishes Running,
Cancelling, Cancelled, Completed, Failed, and Interrupted. Cancel stays
available during queued/running work, then becomes disabled while cleanup is
pending; the UI never says Cancelled merely because the click was accepted.
Late progress or completed responses from a superseded/cancelled job cannot
trigger a clipboard write or download. Native copy commands remain separate
from file export jobs unless their size crosses the same large-delivery
threshold and they can satisfy the same cleanup contract.

## Qualification and release evidence

Unit tests cover the state machine, idempotence, races, owner fencing,
authorization, accounting lifetime, byte/row limits, and orphan cleanup.
Backend tests use a deliberately slow query to prove native execution stops
and its connection remains usable; they exercise cancellation from another
worker, worker termination/restart, Redis outage, and an immediate smaller
retry. Browser tests in all three installed hosts inspect real status and
network requests, visible phase changes, absence of partial downloads,
correct retry output, and exact artifact hashes. Progress checks validate
observed rows/bytes and refuse fabricated percentages. The required
`large-delivery.progress`, `large-delivery.cancel-retry`, and
`journey-recovery.cancel-new-work` cases cannot pass from UI state alone.

## Deferred S3-compatible extension

A future optional shared artifact store may retain completed files beyond a
worker's lifetime and support resumable download or job resumption. It would
implement the same completed-artifact handoff behind the job interface, with
tenant-bound authorization, encryption, retention/expiry, quota accounting,
and cleanup. This is deliberately not a release dependency or a current
capability claim.

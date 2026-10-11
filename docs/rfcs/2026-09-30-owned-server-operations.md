---
rfc: "2026-09-30-owned-server-operations"
title: "Owned server operations"
track: maintainer
status: accepted
implementation: complete
authors:
  - OmniGraph maintainers
created: 2026-09-30
updated: 2026-10-07
discussion: https://github.com/ModernRelay/omnigraph/pull/824
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Owned server operations

## Decision

An admitted HTTP write belongs to the server until its operation returns, even
when its caller disconnects. Admission bounds the number and retained inputs of
these operations. Shutdown closes admission and waits for registered operations,
response bodies and server producers under one absolute deadline. The CLI reports
what a failed whole command establishes and never automatically repeats it.

This accepts A3 and an operation-ownership foundation for B in
[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md).
It does not qualify B's complete storage-I/O settlement or completion-memory and
local-I/O reserves. No online deployment, new ledger, durable request store,
background repair queue or graph reset is introduced.

## Ownership and shutdown

Admission first reserves reversible workload capacity. The operation registry
then checks its closed state and registers the operation under one synchronous
ordering boundary before spawning it. If closure wins between reservation and
registration, the request refuses, releases its reservations and executes no
operation. The two mutexes are never held together. The owned future is executed
once with immutable inputs, its trusted actor and Session settings, in the
request's tracing span. Server response producers retain that span too.
Disconnect drops the response waiter, not the owned future or its reservations.
Every admitted write uses this path: mutation, stored mutation, load, ingest,
branch controls and any supported schema-apply door. A merge with optional source
deletion has one owner through both effects; their separate authorization and
[exact outcomes](2026-09-30-exact-merge-receipts.md) remain intact.

A completed result has one bounded delivery slot. A vanished receiver does not
retain a result indefinitely. No timeout wraps the owned engine future. This
decision adds neither a data-request execution deadline nor post-disconnect
result lookup; a disconnected caller can still have an unknown outcome.

The same ordering boundary closes admissions permanently. A request losing the
close/acquire race refuses. Read observers retain their registration through the
response body and any server-spawned producer; dropping a body alone cannot
release a producer's registration. Shutdown observes read and write response lanes and known operation owners after
HTTP connections stop. The signal and any fatal operation failure share the original
absolute shutdown deadline and existing process watchdog. Cutoff exits nonzero;
no renewed deadline, late success, old-epoch reopening or automatic replay is
permitted.

After complete bounded body collection, HTTP read handlers also execute in
server-owned tasks. The admitted observer and input reservation survive loss
of the result waiter. MCP tool execution separately retains its observer,
input and concurrency permit, so its cancellation and 30-second deadline bound
response waiting rather than engine execution. Completed delivery slots remain
observed until consumed or abandoned. Read errors and panics remain nonfatal;
the engine joins registered graph workers before propagating an execution
panic. This does not own opaque native work beyond the engine future, and does
not change export/baseline producer cancellation on body abandonment.

A panic in an owned write, lost write ownership or an indeterminate effectful result closes
admission and signals process shutdown. Keep unresolved operation reservations
charged until process termination rather than recycling capacity on a false
completion claim. A normal validation refusal is not by itself such a failure.
Exact completed publication evidence remains exact even when delivery fails.
An opaque execution error unexpectedly returned by an owned write is conservatively
unknown completion, not evidence of corruption. Typed compiler and ordinary
validation refusals remain nonfatal; relaxing opaque errors requires typed engine
proof rather than inspecting HTTP status or diagnostic text. The engine supplies
operation-local `BeforeEffect` or `Uncertain` evidence separately from the typed
cause. Only audited owning boundaries mint a pre-effect proof; preparation after
earlier effects or attempts cannot inherit a sub-operation's
proof. Recovery-required, initialization-indeterminate and publication-in-doubt
outcomes dominate a nested pre-effect wrapper. A failed remainder after branch
retirement publication or an injected post-publication merge failure likewise
carries explicit uncertainty. Unwrapped opaque errors remain conservative.
Native branch-name validation is an ordinary bad-request cause, independent of
completion evidence. Earlier effects strip a later sub-operation's pre-effect
proof without turning an ordinary validation cause into an opaque failure.
Schema contracts now publish atomically; there is no pending installation on open.

The failure closes admission for all graphs in the process. Shutdown waits for
the remaining known operations and response/producer owners, then exits 2 without
waiting out unused grace. The original watchdog is still the upper bound.
Uncertain reservations remain retained until process termination. Neither early
exit nor completed logical owners establishes native-I/O settlement or permits
engine reuse.

## Bounded admission

Reserve independent read and write ingress capacity before collecting a body.
Write bodies default to 64 slots and 256 MiB; read bodies, including MCP, default
to 64 slots and 64 MiB. The independent ingress allowances total 320 MiB by
default; the existing ingress variables now control writes, with separate read
variables. Bodyless reads consume no ingress reservation. Read
response observers default to 128 slots and write response observers to 64,
independently, so slow readers cannot block write admission. Both lanes close
under the same operation-runtime ordering boundary. Registered route identity
selects body limits; a stored query's name cannot grant a bulk limit. Its typed
registry kind selects its lane only after the invocation authorization gate. Per-body limits
remain in force; an incomplete or oversized body cannot enter the engine.
Operation admission separately bounds aggregate operation count and retained
input bytes, plus existing per-actor counts and bytes. Actor records have a finite
cardinality limit and idle records can be retired without replacing a live actor's
accounting. These admission acquisitions are immediate or refuse; no request queue
is added.

Input ownership and its reservation transfer to the registered operation before
execution. They remain charged after disconnect. Read body/producer registrations
remain charged for their actual server-owned lifetimes. Status paths remain
available without consuming ordinary operation capacity. Configuration values,
defaults and their validation are defined once in the server's workload module
and documented in the operator guide with the implementation.

These are bounds on named server resources, not a universal engine memory or I/O
budget. Decoded Arrow data, query workers, Lance buffers, catalog history and
native accepted I/O need their own accounting. This increment cannot authorize
schema activation, reclamation or retry merely because registered futures joined
or the server's counters reached zero.

## Whole-command failure outcomes

CLI data-write failures preserve available structured error fields and add
`command_outcome { execution, effects, action }`. The evidence describes the whole
invocation, including earlier subrequests and writable opens. A final refusal
cannot erase uncertainty or effects from earlier work. JSON mode writes the
structured failure to stdout; human mode gives the same action on stderr. The
execution values are `not_started` and `unknown`; effects are `none` and `unknown`;
actions are `retry`, `refresh`, `recover` and `reconcile`. When available,
`http_status` and the original opaque `retry_after` string accompany the error.
These additions belong to the CLI result envelope, not the HTTP `ErrorOutput`.

| Exit | Evidence and permitted action |
|---|---|
| 0 | Exact success or no-op, including successful merge with separately reported optional deletion failure. |
| 4 | A verified HTTP 412 conditional mismatch for the requested precondition, with no earlier work able to produce effects; read and choose a new precondition. |
| 75 | A verified, typed HTTP 429 `too_many_requests` refusal before operation admission, with no earlier whole-command work able to produce effects. Preserve the original `Retry-After`; the caller bounds any new attempt. |
| 1 | Other failure, including generic 409/503, a storage failure, resource exhaustion, completion-required error, malformed or truncated success, and unknown delivery. |

This narrows the umbrella's initially proposed retry allowance: standalone
embedded append/merge load preparation conflicts remain exit 1 until separately
qualified. An embedded conditional mismatch also remains exit 1 until its
whole-command pre-effect proof is qualified. Neither an HTTP status alone nor
error text proves safe retry. A
response must satisfy the v0.12 wire contract and typed admission evidence before
exit 75 is permitted. Classification adds no automatic retry and changes no
precondition behind the caller's back.

## Substrate boundary and remaining work

Pinned Lance 11.0.0 owns table writes. Its multipart writer can spawn abort work
from `Drop`; a local writer can have a blocking persistence operation outstanding
after its calling future is dropped. Local direct I/O does not all traverse an
object-store wrapper, and ordinary in-flight metrics are not settlement receipts.
Engine query producers likewise have lifetimes beyond their immediate caller.
The complete upstream object-store, observability, transaction and read/write
guides and the corresponding pinned implementation informed this boundary.

Keeping the complete write future alive closes cancellation by the HTTP waiter.
It does not prove that every returned error or panic has no later storage effect.
Full B remains gated on independently observed ownership and settlement of native
accepted I/O, protected completion memory/local-I/O capacity and workload bounds.
That proof must cover local and supported cloud paths, including failure and
blocking work, before granting generic runtime disposal/reuse. The umbrella's
proposed same-engine E1 transition has a separate qualification gate for coherent
request bindings, interfering effects and retained resources. It cannot clear
uncertainty or reopen fatal/shutdown admission. Neither transition is qualified
by this foundation. No storage format or graph publication protocol changes here.

## Evidence and rollout

Extend the existing owners; these are transport, process, lifetime and resource
assertions rather than new query semantics or GQT grammar.

| Owner | Required evidence |
|---|---|
| Server `data_routes`, `auth_policy`, `schema_routes`, `stored_queries` | Disconnect an admitted request waiting at an engine gate; join the cancelled waiter before releasing the gate, retain capacity, finish once and permit a same-process sentinel write. Merge plus optional source deletion must both finish after waiter cancellation. Separately inject owner panic before/after publication and require fail-stop. Refused bodies/admissions have no effects. Native branch validation and read-only namespace failures preserve subsequent read/write admission. Pre-effect deletion storage failures preserve admission, exact merge receipts and a sentinel write under slow reads. |
| Workload/runtime/ingress unit owners; server `mcp` | Atomic close/acquire race, aggregate and per-actor caps, actor-cardinality bound, no early permit release, body/Bytes/producer lifetimes, authenticated MCP admission, write panic closes admission, and bounded result delivery. Read disconnect and MCP cancellation/deadline retain execution/input capacity and delay shutdown; read error/panic remains nonfatal; an unconsumed completed result keeps its observer. |
| Server `boot_settings` and existing process support | Real TCP disconnect and process shutdown with an admitted write; no new admission, one deadline, nonzero cutoff and early fatal containment before/after publication; remaining known owners finish before a nonclean drain. The actual export/baseline producer and response lifetimes are verified separately in `data_routes`. |
| CLI `cli_data`, `parity_matrix`, `system_remote` | Typed 429 versus generic/malformed errors, exact Retry-After, compound earlier effects, conditional outcomes and one-submission census after uncertain delivery. |
| Engine/core error, `lifecycle`, `branching`, `failpoints`, loader/schema owners | Initial read refusals across write families retain typed causes and pre-effect evidence; pending completion or prior work cannot leak that proof. Explicit uncertainty dominates ordinary causes after failed release or publication, while validation after successful completion remains nonfatal. |
| Server `openapi`; repository metadata checks | Generated API contract and user guidance match implemented behavior. |

T5/T7 and the relevant T10 server-lifetime rows in the
[server-testing proposal](2026-09-26-self-contained-server-testing.md#server-coverage-requirements)
organize this evidence. T6 native-I/O settlement and T10 completion reserves remain
open gates; a held engine future does not satisfy them.

Qualification after review hardening on 2026-09-30 passed the disconnect/compound-write, sealed-admission,
body/producer lifetime, workload-limit and whole-command outcome regressions.
The process fixture passed clean SIGTERM drain, forced cutoff, and owner panics
before and after publication; independent reopened history distinguished the
unpublished and published outcomes. The canonical workspace test command with
engine/cluster failpoints reported 3,413 passes (including subprocess helper
results) and one failure: an unchanged compiler source guard scanned unrelated
ignored nested worktrees. That exact guard passed in a clean managed worktree
at the same source revision; unrelated nested worktrees were preserved.

The complete GQT run (434 tests), AWS-feature server run (467 tests), all 15
ignored loopback CLI process tests, generated OpenAPI (105 tests), both workspace
Clippy graphs and GQT Clippy passed. The existing DST object-store cost golden
passed unchanged. Formatting, documentation, source/pin checks,
typos and the dependency audit passed. These are local transport and lifecycle
proofs; native-I/O settlement, completion reserves, server-DST scheduling,
performance and live-cloud qualification remain open.

## Alternatives and compatibility

Keeping a request future alive only while its socket remains connected preserves
the bug. Spawning without bounded ownership leaks admission accounting and lets
shutdown exit early. Treating a join as a complete storage drain would enable
unsafe runtime replacement. This decision retains the future, accounts for its
server lifetime and refuses unsupported conclusions.

Bounded admission and explicit CLI failure outcomes belong to the current
[HTTP contract](2026-09-30-v012-http-admission.md). There are no older-client adapters or alternate error aliases. Embedded Rust
callers matching `OmniError` account for its new `Completion` wrapper; use
`into_completion_evidence` to recover the original typed cause and evidence.
The wrapper preserves display, diagnostics and storage/memory classification.
Malformed branch-create names now return HTTP 400 rather than an internal 500.
This ownership mechanism adds no data or cluster-state migration. Current online
deployment and ledger-conversion rules remain with the
[server runtime decision](2026-09-29-server-runtime-and-online-deployment.md).

## Decision log

- 2026-10-02: Extended Ownership and shutdown to read handler execution and
  MCP tool execution after caller loss, including retained delivery slots.
  Evidence now distinguishes nonfatal read failures from write uncertainty.
  Registered graph-worker joins also cover execution and destructor panics;
  opaque native read tails remain outside this implemented boundary.

- 2026-10-02: Ownership and Whole-command failure outcomes replace the pending
  schema-installation examples and writable-open completion rationale with the
  unchanged rule that a sub-operation's proof cannot establish whole-command
  safety. Substrate boundary replaces "before a drained runtime can be reused
  for online activation" with generic disposal/reuse qualification and the
  umbrella's separately proposed same-engine E1 gate. Atomic contract publication
  removes installation work, not uncertain-write containment or retry boundaries.

- 2026-09-30: Accepted the broader server-owned operation and CLI outcome slice
  on the maintainer's instruction to proceed. Ownership, bounded admission and
  shared shutdown land together. The umbrella's initial embedded-load exit-75
  allowance is deferred; full native-I/O settlement, completion reserves and online
  deployment remain outside this increment's qualification.

- 2026-09-30: Completed local qualification for this bounded ownership and CLI
  contract; native-I/O settlement, completion reserves and online deployment
  remain separate gates.

- 2026-09-30: Amended on the maintainer's instruction to harden the reviewed
  stack: engine-owned pre-effect/uncertainty evidence, independent read/write
  capacity, registered-route body limits, request tracing and early nonzero
  containment after remaining logical owners finish. Native-I/O settlement
  and runtime reuse remain unqualified.

- 2026-10-07: Replaced the Compatibility paragraph's v0.12 qualifier with the current HTTP admission decision, and the obsolete restart-required sentence with the current online-deployment and ledger-conversion authority.

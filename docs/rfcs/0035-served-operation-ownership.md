---
type: spec
title: "RFC-035 — Served operation ownership"
description: Generation-pinned read observation and server-owned write execution, with two one-way admission lanes, exact drain, and one extensible shutdown deadline.
status: draft
tags: [eng, rfc, server, cancellation, shutdown, admission, lifecycle, omnigraph]
timestamp: 2026-08-13
owner: OmniGraph maintainers
---

# RFC-035: Served operation ownership

**Status:** Draft
**Date:** 2026-08-13
**Author track:** Maintainer design series
**Depends on:** existing engine write and recovery contracts; no runtime-activation or recovery-supervision design.
**Replaces:** [PR #490](https://github.com/ModernRelay/omnigraph/pull/490), retaining its cancellation evidence rather than its stacked implementation.
**Audience:** server, engine, API, operations, and test maintainers.

---

## 0. Decision summary

Once admitted, a graph write belongs to the server until engine-terminal result, caught panic, or the shutdown deadline. Disconnect, HTTP/2 reset, timeout, or a dropped result receiver changes delivery only; it does not cancel or replay.

Reads remain request-owned and cancel normally. Before target observation they capture the exact generation and a `ReadObserver` permit through body/stream end.

V1 adds one opaque `ServedAdmissionCell` per served generation:

1. Independently closeable `ReadObserver` and `Write` lanes issue typed permits.
2. Read capture is not detachment: handler/body drop requests ordinary read cancellation and the permit ends after any scoped producer settles.
3. `OwnedOperationExecutor` moves the write permit, workload guard, exact cell/generation, actor, and inputs into a tracked task before returning its receiver.
4. Lane close is linearizable and one-way. Existing permits drain on their exact generation; every replacement epoch receives a fresh cell and token.
5. No owned write closure is invoked twice. `RecoveryRequired` or panic emits an opaque wake but does not classify recovery, replay, or activate replacement.
6. A bounded pre-effect `TransitionDrain` yields exact proof or leaves selected lanes closed and reports pending counts; timeout never starts recovery/replay.
7. HTTP connections, served generations, and any separate `ShutdownParticipant` share one absolute `ShutdownDeadline`, never participant-local timeouts.
8. Cutoff reports unfinished work and hard-exits without claiming success; existing engine durability and next-open recovery remain authoritative.

This server-lifetime contract adds no storage format, durable request ledger, transaction manager, replay queue, or recovery protocol.

## 1. Problem

An Axum handler currently owns and awaits the engine future, workload guard, and inputs. Peer disconnect or timeout drops that handler and therefore the engine future.

That is safe only before a durable effect. Writers may arm recovery and move Lance table heads before graph-wide manifest publication. Cancelling the caller wait is not rollback; afterward only terminal completion or an explicit unknown outcome under the existing recovery protocol is correct.

Automatic replay is equally wrong: disconnect, panic, timeout, or ambiguous store response does not prove no first effect. Replay can duplicate a mutation or use a different head; cancellation and retry policy are not transaction evidence.

Reads cancel normally but still need admission accounting. A generation `Arc` pins memory, not closed admission. Handler return is also too early for polled Blob/export bodies, so a typed permit follows the body without detaching the read.

At process scale, tracked-task timeout is not a bound if handlers or connection drain get another timeout. One rooted budget must cover every participant.

## 2. Lance facts that constrain the design

Lance [transactions](https://lance.org/format/table/transaction/) create immutable versions atomically; caller cancellation is not abort. Lance may rebase one commit, while application retry must re-read. [Object-store requests](https://lance.org/guide/object_store/) retry independently, so receiver loss proves no outcome. Immutable versions pin reads, while the [cleanup contract](https://lance.org/guide/read_and_write/#cleanup-old-versions) makes unverified destructive cleanup unsafe while work may remain.

The server therefore keeps admitted writes alive and delegates commit truth to the engine/Lance; it adds no cancellation rollback or acknowledgement system.

## 3. Scope

### 3.1 In scope

- Lightweight generation capture for reads and target-observing response bodies.
- Cancellation-independent ownership of every HTTP-reachable graph write.
- Two independently closeable, one-way admission lanes and exact selected drain.
- A bounded, pre-effect transition drain distinct from process shutdown.
- Retention of workload accounting for the whole engine operation.
- Separation of engine execution from result delivery.
- One-shot panic containment and opaque `RecoveryRequired`/panic notification.
- An opaque admission bridge for a separate activation/recovery owner.
- A generic shutdown-participant interface sharing one process deadline.
- Tests and CI that prove real transport cancellation, not only dropped Rust futures.

### 3.2 Out of scope

- Recovery classification, roll-forward/compensation, retry, authority, or execution.
- Runtime-generation construction, validation, supervision, or activation.
- Cache generation or cache invalidation design.
- Replay, idempotency keys, durable status lookup, or a result ledger.
- Detached ownership, replay, or changed cancellation semantics for reads.
- A distributed writer fence or a change to the supported writer topology.
- Changes to Lance transaction, manifest, or object-store semantics.
- Substrate/storage failure classification; lifecycle outcomes in §9 belong here.

## 4. Vocabulary

| Term | Meaning |
|---|---|
| Served generation token | Opaque, process-local, non-reusable identity for one activated admission cell and graph generation. |
| Admission cell | Indivisible token, generation handle, two typed lane gates, and write executor. A replacement epoch receives a fresh cell. |
| Read-observer permit | Lightweight proof that request-owned target observation may use the captured generation until its body/stream terminates. |
| Write permit | Proof transferred into one cancellation-independent owned write task. |
| Owned write | Registered task owning its cell/generation, write permit, workload guard, actor, inputs, and engine future. |
| Result receiver | One-shot write-result receiver held by the handler; its disappearance does not change write execution lifetime. |
| Recovery wake | Best-effort in-process hint containing only graph key and generation token; never authority, retry instruction, or receipt. |
| Transition drain | Finite wait after selected lanes close; returns exact proof or a non-proof snapshot of active permits. |
| Drain-ready wake | Opaque process-local hint that the last selected permit dropped after a pending transition wait. |
| Shutdown deadline | One absolute monotonic instant created once; every participant consumes time remaining to that instant. |
| Shutdown participant | Independently owned task/connection set that synchronously closes admission and asynchronously drains under that deadline. |

## 5. Hard lifetime invariants

1. **Preparation is not engine entry.** Authentication, bounded parsing, Cedar, and syntax checks precede owned writes; no target access lacks a typed capture.
2. **Generation identity is exact.** One cell binds token, generation, lanes, and executor. Closed capture fails instead of reloading the registry.
3. **One request uses one class.** Observers use `ReadObserver`; changes use `Write`, including nested reads. Permits never change class or cell.
4. **Close is class-scoped and linearizable.** Acquire either yields a permit in that class's drain or refuses before target work; other classes do not move.
5. **Cells never reopen.** Lanes move only Open -> Closing -> Drained/Cutoff; resumed service, even for identical runtime content, gets a fresh cell/token.
6. **Read observation stays caller-owned.** Its permit follows body and scoped producer; disconnect/timeout/drop cancels normally and releases after settling.
7. **Write transfer has no await gap.** Acquire, registration, and spawn are synchronous; after `try_start_write`, caller cancellation owns nothing.
8. **At most one top-level write invocation.** Lifecycle events never call it again; cutoff may interrupt it, while engine-internal attempts stay inside.
9. **Permit completion is exact.** Normal read end or write terminal/panic drops its permit. Terminal and cutoff race once; cutoff suppresses late delivery.
10. **Delivery is lossy; write execution is not.** Failed result send is a metric, not an engine error or replay reason.
11. **Write panic is contained.** It releases permits, reports lifecycle-owned unknown outcome to the result receiver, and emits one wake without unwinding peers.
12. **`RecoveryRequired` is not replayable.** The original result and one wake pass through; the executor neither parses authority nor chooses recovery/retry.
13. **Drain is exact and generation-scoped.** `drained(mask)` needs every selected lane closed and permit released; another graph/unselected lane is unaffected.
14. **Transition timeout is not cutoff.** It returns no proof, leaves lanes closed, and cannot abort, replay, recover, replace, or reinterpret active work.
15. **Shutdown has one owner and clock.** Resources stay counted under one deadline until disposal or atomic registered-participant transfer; no gap resets it.
16. **No false acknowledgement.** Only engine-terminal success may return success; disconnect, cutoff, or unfinished work is never recorded/reported as success.

## 6. Architecture

### 6.1 `ServedAdmissionCell` and lane state

Registry publication exposes one opaque, indivisible cell:

```rust
struct ServedAdmissionCell<G> {
    graph: GraphKey,
    token: ServedGenerationToken,
    generation: Arc<G>, // complete opaque runtime generation
    read_observers: AdmissionLane,
    writes: AdmissionLane,
    write_executor: OwnedOperationExecutor,
}

enum AdmissionClass { ReadObserver, Write }
enum AdmissionMask { ReadObserver, Write, All }
```

Each lane independently follows the same state machine:

```text
Open { N } -- acquire/drop --> Open { N +/- 1 }
Open { 0 } -- close --> Drained
Open { N > 0 } -- close --> Closing { N }
Closing { N > 1 } -- drop --> Closing { N - 1 }
Closing { 1 } -- drop --> Drained
Closing { N > 0 } -- transition deadline --> Closing { N } + DrainPending
Closing { N > 0 } -- shutdown cutoff --> Cutoff { unfinished: N }
```

`close(mask)` moves all selected lanes in one cell critical section and returns a
token/mask-bound drain handle; other lanes do not move. Close is idempotent, no
lane reopens, `Drained` proves graceful release, transition timeout changes no
lane state, and `Cutoff` never becomes `Drained`. State changes notify, not poll.

### 6.2 Opaque capture and drain bridge

The bridge exposes only typed capture and class-selective close/drain:

```rust
fn try_capture_read<G>(cell: &Arc<ServedAdmissionCell<G>>)
    -> Result<ReadCapture, GateClosed>;
fn try_start_write<T, G>(cell: &Arc<ServedAdmissionCell<G>>, workload: AdmissionGuard,
    operation: OwnedOperationFn<T>) -> Result<ResultReceiver<T>, GateClosed>;
fn close<G>(cell: &ServedAdmissionCell<G>, mask: AdmissionMask) -> DrainHandle;
```

`ReadCapture` indivisibly contains a strong cell lease, token, exact generation
`Arc`, and one shared permit. Immutable request/config inspection may precede it,
but target access may not. A close race refuses; middleware never reloads.

Unary/streaming bodies hold the capture until EOF, error, or drop; handler return
is not terminal. Body and bounded scoped producer share one logical lease: body
drop requests normal cancellation, and the last scoped holder releases it. There
is no cancellation-independent read executor.

The external transition owner chooses `AdmissionMask`; this RFC defines the
lifecycle proof, not why a transition requires one class or both. No lane reopens.

### 6.3 Write handoff

A write prepares bounded owned input, acquires workload, then calls non-async
`try_start_write`, which acquires its permit, registers, and spawns on the exact
generation with no I/O/`.await`. Failed construction releases both guards. Actor,
values, branch/schema and merge-delete inputs move in; bearer plaintext never does.

Optional merge source deletion preauthorizes both actions. Merge denial is an
effect-free 403; delete denial enters the task, which still merges and returns
nonfatal `branch_deleted: false`. Allowed deletion remains engine-enforced/nonfatal.

`OwnedOperationContext::observe_engine` wraps every engine call, preserves its
result, and latches `RecoveryRequired`; task exit wakes once even if composite
optional work maps that error into an otherwise successful response.

### 6.4 Owned-write state machine

```text
Prepared (effect-free, request-owned)
   -- try_start_write --> Owned + registered (write/workload permits retained)
          +--> shutdown cutoff: unfinished; abort requested; hard exit
          +--> one top-level closure (engine owns internal attempts)
                    --> terminal/panic --> send if receiver exists
                                       --> opaque wake iff required
                                       --> release registration + permits
```

Receiver drop only discards eventual send. Optional source deletion stays in the
owned task, preserving “merge committed, deletion may fail” and honest drain.

### 6.5 Covered routes and structural guard

The write choke point covers `/mutate`, `/mutate/if-graph-commit`, deprecated
`/change`, effectful `/queries/{name}`, `/queries/{name}/if-graph-commit`,
`/schema/apply`, `/load`, `/load/ndjson`, deprecated `/ingest`, and branch
create/delete/merge with optional source deletion.

Other target-observing graph routes use `ReadObserver`: read/query/stored reads;
snapshot/schema/query/branch/commit catalogs; Blob GET/HEAD; and export through
body/stream terminal. Health and configured-entry listing do not capture a generation.

Source guards fail CI for writers outside `try_start_write` or target observers
outside typed capture. Future Blob/maintenance writers join `Write`.

### 6.6 Opaque recovery wake

The write executor attempts one non-blocking `wake(graph, token)` on
`RecoveryRequired` or panic. Wakes may coalesce; sink failure is isolated and
observable but cannot retain permits or alter the engine result.

The sink accepts no mode, transaction interpretation, replacement, retry closure,
or durable authority. Its consumer re-derives truth; the wake is only a hint.

## 7. Generation handoff and rolling behavior

The separate transition owner supplies the mask: a safe transition may close `Write` only; a transition needing exclusive target quiescence closes `All`. RFC-035 provides this protocol without choosing why:

1. `close(mask)` linearizes on the old cell.
2. Existing selected permits remain pinned and participate in selected drain; unselected admission is unchanged.
3. A request that loaded the old token but lost the acquire race is refused. It never re-resolves onto a replacement.
4. The mask-bound drain handle resolves only after every selected permit drops.
5. Any resumed lane belongs to a newly tokened cell, even if the runtime payload is otherwise identical. The old cell remains closed forever.

### 7.1 Bounded pre-effect `TransitionDrain`

Transition waiting has its own finite monotonic deadline and result types:

```rust
struct TransitionDeadline(Instant); // never a ShutdownDeadline
struct TransitionDrain<G> { cell: Arc<ServedAdmissionCell<G>>, handle: DrainHandle }
enum TransitionDrainOutcome {
    Drained(DrainedProof),
    DrainPending { counts: PermitCounts },
}
struct DrainReadyWake { graph: GraphKey, token: ServedGenerationToken, mask: AdmissionMask }
```

`begin_transition_drain(cell, mask)` synchronously closes selected lanes before returning the strong, token/mask-bound `TransitionDrain`. `wait_until(deadline)` linearizes final-drop against expiry: it returns an unforgeable `DrainedProof` if all selected permits released, otherwise `DrainPending` with exact, nonzero per-class counts at that instant. Expiry leaves lane state `Closing`; counts are diagnostic and never proof.

Pending never aborts/cancels/replays a write and never authorizes generation build, replacement, or recovery. Request-owned reads may cancel normally, but a stalled body/scoped producer remains counted until it actually settles. The graph stays blocked for selected admission.

The owner parks the strong `TransitionDrain` in graph-local state and may release scarce global scheduler capacity. When the final selected permit drops after a pending result, the cell emits one coalescible opaque `DrainReadyWake`; the wake is not proof, and the owner rechecks the handle to obtain proof. Parking neither reopens the gate nor deregisters the cell. Shutdown still sees its strong cell via the registration latch.

`TransitionDeadline` budgets one pre-effect wait only. It is never passed to a `ShutdownParticipant`, never changes to cutoff, and cannot extend or replace the process-wide `ShutdownDeadline`. If process shutdown wins, §8 independently closes `All` and may cutoff at its own absolute deadline.

### 7.2 Publication and process boundary

Process shutdown and publication share one `ServingRegistrationLatch`:

```text
Running { weak cells } -- close All + stop(t0, deadline) --> Stopping { deadline }
```

The latch holds a bounded weak index; registry/retirement/capture/task owners keep
live cells strong. Before any close it prunes dead entries and refuses Stopping or
capacity exhaustion. Initial activation uses `register_and_publish(new,
no_fail_store)`; replacement consumes the matching selected-lane proof through
`replace_and_publish(old, new, proof, no_fail_store)`. Under one no-I/O lock it
closes old `All`, registers new, then invokes the infallible store, so no old
capture succeeds afterward. Publication enters shutdown's set or returns
`ServerStopping` with no effect.

The sole removal, `deregister_drained(token, AllDrainedProof)`, shares the latch:
a stop race snapshots a live strong cell or sees proof that no work remains.

The transition owner constructs candidates and chooses masks; this RFC supplies
only cell/latch/drain. Its proof is process-local, never a distributed fence.

Within one process and unchanged writer authority, replacement is allowed after
the operation's required `DrainedProof`: it publishes a fresh cell/token through
`replace_and_publish`; the old cell remains closed and pinned observers finish on
their old generation. This is not overlapping independent writers.

Stop-before-start applies to **cross-process process replacement**: the old
process closes `All`, drains, and exits before another process opens read-write or
receives traffic. No process-local proof, operation, or receiver crosses that
boundary; overlap needs a separate distributed fence. After disconnect, outcome
remains unknown unless ordinary graph state proves it.

## 8. One bounded shutdown budget

V1 adds `--shutdown-grace-seconds <u64>`, defaulting to 25 and injectable in
tests. Zero requests immediate cutoff. The outer orchestrator termination grace
must remain strictly longer.

The lifecycle API is deliberately substrate-neutral:

```rust
struct ShutdownDeadline(Instant); // only the coordinator constructs it

trait ShutdownParticipant: Send + Sync {
    fn begin_shutdown(&self, deadline: ShutdownDeadline) -> DrainFuture;
    fn force_cutoff(&self) -> CutoffReport; // bounded, synchronous, non-blocking
}
```

`begin_shutdown` synchronously and idempotently closes that participant's own
admission before returning its drain future; it cannot await, invent a timeout, or
spawn untracked work. Long-lived sets register only while Running; Stopping
refuses them. Their futures are polled concurrently.

Completed outputs/guards stay counted until drop or atomic registered-participant adoption; drain cannot cross a handoff.

Initial participants are HTTP connections and served generations. Latch stop
already closes cell `All`; the generation participant owns their exact drains and
write abort handles. RFC-036's separate `RecoveryTaskSet` may implement the trait;
RFC-035 knows no task type, recovery call/result, or ownership rule.

At signal receipt `ts`, the coordinator creates `ShutdownDeadline(ts + grace)` and
arms its already-running watchdog before any wait. `latch.stop(deadline)` then
upgrades its bounded live set and closes every cell `All` before transitioning and
snapshotting at `t0`. A concurrent activation orders before `t0` and is closed/
included, or fails afterward. The coordinator drops readiness/listener acceptance,
then calls every participant; all drains share the same remaining budget.

Clean aggregate drain keeps the watchdog armed and invokes immediate platform
exit with status zero; it never returns into runtime/destructor/supervisor
teardown. At the deadline, atomic cutoff wins unfinished races and the watchdog
hard-exits nonzero. Reports, abort/cancel, and diagnostics are best-effort; Tokio
abort/runtime drop is not a bound, and neither exit path waits or invokes callbacks.

Hard exit is crash-equivalent for unfinished work. Existing durability/recovery
remains load-bearing; this layer deletes nothing and performs no last-second repair.

A participant may finish early, but it creates no second allowance. The bound is
from signal receipt to process exit, not from one participant's close to a log.

## 9. Failure and observability contract

RFC-035 owns lifecycle outcomes; PR #491 classifies only engine/substrate
failures. `ErrorOutput` gains an optional rolling-safe `lifecycle` detail whose
`kind` and `outcome` are strings (unknown values must deserialize). The stable
mapping is:

| Event | HTTP / transport | `kind` | `outcome` |
|---|---|---|---|
| Selected lane closed/draining, including after transition timeout | 503; `code` absent | `generation_draining` | `not_started` |
| `TransitionDrain` deadline expires | no initiating HTTP response; selected requests use the 503 above | `transition_drain_pending` diagnostic | `blocked` |
| New request/registration sees Stopping latch | 503; `code` absent | `server_stopping` | `not_started` |
| Caught write panic or result channel closes without a terminal value | 500; `code: Internal` | `operation_outcome_unknown` | `unknown` |
| Result receiver was already dropped | executor emits nothing; write stays owned | — | `unknown_to_client` |
| Cutoff wins an active operation | force-close/body error; never synthesize success | `shutdown_cutoff` in diagnostics only | `unknown` |

Stopping is checked before lane state. Both 503s precede engine entry and neither
they nor `DrainPending` carries replay/retry advice; the 500 must not claim
effect-free. Pending counts stay internal diagnostics. After cutoff,
a write cannot send 2xx even if its future later succeeds. A streamed read may
already have headers, so it ends by body error/close, not replacement status.

An engine-terminal error retains its independently owned mapping. In particular,
this RFC forwards `RecoveryRequired` unchanged and emits its wake; it does not
classify its cause or redefine its durable detail.

Process-local attempt identifiers may correlate logs but are not durable
idempotency keys and are never accepted back from a client.

Minimum metrics, with bounded graph labels and no token label, are:

- read/write permits admitted/active; lane state and close-to-drain duration;
- transition drained/pending outcomes, pending counts, and drain-ready wakes;
- read bodies canceled and write result receivers dropped before completion;
- recovery-wake calls, coalesces, and unavailable-sink outcomes;
- shutdown participants completed and unfinished work by class at cutoff.

No metric label carries bearer material, query text, row data, arbitrary error
text, or durable operation ids. Lance's object-store request/error/in-flight
metrics remain the substrate view; server-operation metrics do not replace them.

## 10. Resource and security posture

Read permits are lightweight lifecycle counts, not a new byte/concurrency budget;
existing Blob/export transport bounds still apply. A write task holds both its
per-actor workload guard and write permit. Neither class waits for a gate permit.

Lifecycle lock order is latch -> cell lanes; request capture never takes the latch.
Effect-free parsing/Cedar follows route disclosure order, with capture before any
engine-assisted auth/target access. Writes then order workload, registration, and
engine gates; rejection reverses it, and bearer plaintext enters neither owner.

## 11. Rejected alternatives

| Alternative | Rejection |
|---|---|
| Await directly in handlers | Request cancellation remains unsafe operation cancellation. |
| Detach only after cancellation | A race remains between future drop and rescue ownership. |
| Detach ordinary reads | Adds task ownership where normal transport cancellation is correct. |
| Use only a generation `Arc` for reads | Pins memory but cannot close admission or prove target observers drained. |
| One undifferentiated lane | Safe write-only transitions would unnecessarily refuse/read-drain every observer. |
| One process-global gate | One graph stalls peers and old-generation drain cannot be proved. |
| Reopen a closed gate | Creates generation ABA; a replacement needs a new token and gate. |
| Move work to the latest handle | Retargets authorization, schema, catalog, and head assumptions. |
| Abort writes on disconnect/timeout | Neither proves the operation effect-free. |
| Replay on error, panic, or receiver loss | The first execution may have committed; exactly-once needs a separate durable protocol. |
| Use shutdown cutoff for transition timeout | A scheduling budget cannot abort work or manufacture quiescence proof. |
| Participant-local drain timeouts | Sequential allowances violate the process deadline. |
| Durable queue or result ledger | Adds a second authority and recovery surface without solving ownership more safely. |

## 12. Implementation sequence

1. Add the two-lane cell, typed permits, `TransitionDrain`, owned-write executor,
   body-held read capture, wake sinks, and deterministic primitive tests.
2. Cut every current route over at once and extend source guards; add the
   `ServingRegistrationLatch` and opaque activation bridge before replacement is
   enabled.
3. Add `ShutdownDeadline`, participant registry, connection/generation
   participants, independent hard watchdog, and lifecycle wire outcomes.
4. Land transport/failpoint/subprocess evidence, metrics, docs, and release notes.

These steps require no storage migration. A later RFC may consume the close/drain and opaque-wake interfaces unchanged.

## 13. Acceptance and test ownership

| Contract | Test owner |
|---|---|
| Per-lane close/acquire linearization, selected drain, no missed wake, no reopen | server in-source operation-lifetime tests |
| `close(Write)` leaves read admission unchanged; `close(All)` drains both classes | server admission bridge tests |
| Read capture token/Arc/permit match; response EOF/error/drop and scoped producer release exactly once | `blob_transport.rs`, `export_transport.rs`, and bridge tests |
| Read disconnect cancels normally while write-result receiver drop retains owned execution | server transport and operation-lifetime tests |
| Exclusive/recovery-selected close racing target resolution admits-and-drains or refuses before observation | server registry/admission failpoint test |
| Activation/retirement versus shutdown t0 cannot publish or omit a live cell after snapshot | server registry/shutdown race test |
| Fresh publication of the same runtime gets a new cell/token; old lanes remain closed | server registry/admission tests |
| Transition expiry with a stalled read stream returns non-proof read counts and 503s new observers; cancellation plus producer settlement emits wake then proof | Blob/export transport and admission failpoint tests |
| Transition expiry with a stalled write returns non-proof write counts, releases scheduler capacity, and neither aborts/replays/recovers; terminal drop emits wake then proof | write-cancellation and transition-scheduler failpoint tests |
| Same-process replacement under unchanged writer authority requires selected proof and a fresh cell; cross-process replacement remains stop-before-start | registry/transition subprocess tests |
| External task-set output/guard handoff is atomic, shares the deadline, and cannot falsely drain | shutdown-participant unit test |
| `RecoveryRequired` and panic emit one opaque wake; ordinary errors do not | server in-source operation-lifetime tests |
| Merge-delete preauthorizes both; delete denial stays 200/`branch_deleted:false`, and nested `RecoveryRequired` wakes | branch route and operation-lifetime tests |
| Every writer uses the executor and every target-observing route uses a typed capture | `crates/omnigraph/tests/forbidden_apis.rs` plus server source guards |
| HTTP/1 disconnect, HTTP/2 reset, and timeout after durable arm do not cancel or replay | new `crates/omnigraph-server/tests/write_cancellation.rs`, extending engine failpoint seams |
| Graph A close/drain does not affect graph B | `crates/omnigraph-server/tests/multi_graph.rs` |
| Short operation completes and responds during graceful shutdown | real-server shutdown test |
| Non-cooperative work, parked connection, or post-drain teardown stall cannot extend process lifetime | subprocess real-server shutdown test |
| Cutoff residue follows existing next-open recovery behavior | server failpoint test plus the existing engine recovery owner; do not duplicate recovery classification |
| Closed lane and Stopping latch map to lifecycle 503; panic/channel loss to unknown 500; cutoff never sends success | server API/OpenAPI and subprocess tests |
| Denied/malformed input never starts owned work; post-close keep-alive requests never enter target work | server route and operation-lifetime tests |
| Immediate completion/panic cannot race registration or cutoff accounting | server in-source operation-lifetime tests |

The cancellation suite must be feature-gated on server failpoints and must run
non-vacuously in CI. Add:

```toml
[features]
failpoints = ["omnigraph/failpoints"]
```

The canonical feature-superset command becomes:

```bash
cargo test --workspace --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints,omnigraph-server/failpoints
```

The failpoint-superset Clippy job must use the same three-feature list.

CI explicitly runs `write_cancellation` or verifies its test names, preventing a
zero-test feature mismatch. Local-FS owns deterministic cancellation/process
bounds; configured RustFS remains object-store recovery evidence.

Unit state machines use paused Tokio time; only subprocess exit uses a generous
wall bound, asserting one shared deadline and retaining child logs on failure.

## 14. Compatibility and documentation

No disk/manifest/Lance/query/success shape changes. Optional
`ErrorOutput.lifecycle` is additive; closed `ErrorCode` stays unchanged, with
OpenAPI/old-client tests. PR #491 owns none of these meanings.

Observable changes are additive lifecycle failures and an admitted write may
finish after disconnect/timeout. Docs state transport cancellation is not
transaction cancellation and clients may need to inspect graph state before
retrying. Read cancellation and merge-delete's nonfatal 200 behavior remain;
only read generation pinning through body/stream becomes explicit.

Shutdown documentation must define the single grace setting, readiness change,
participant-wide deadline, force-close point, and requirement that the outer
orchestrator grace exceed server grace. Operator docs require stop-before-start
for cross-process replacement under the current writer boundary and describe
proof-gated same-process replacement. Release notes promise neither
uninterrupted recovery, exactly-once retry, nor durable operation lookup.

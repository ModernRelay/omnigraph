---
type: spec
title: "RFC-029 — Write-protocol lifetime ownership and recovery resolution barriers"
description: Shields armed write protocols from caller cancellation, resolves rollback-class recovery residuals at the write-entry gate instead of process restart, and turns server-boot quarantine into an observable, converging state.
status: draft
tags: [eng, rfc, writes, recovery, cancellation, server, availability, omnigraph]
timestamp: 2026-07-20
owner: OmniGraph maintainers
---

# RFC-029: Write-protocol lifetime ownership and recovery resolution barriers

**Status:** Draft
**Date:** 2026-07-20
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s recovery-v9 envelope,
gate ordering, and `Armed`/`EffectsConfirmed` classification;
[RFC-028](0028-stable-schema-identity.md)'s identity-keyed sidecars
**Surveyed:** OmniGraph 0.8.1 (`main`); Lance 9.0.0-rc.1 at git rev
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d` (upstream
`rust/lance/src/io/commit.rs`, `rust/lance/src/dataset.rs`,
`rust/lance-table/src/io/commit.rs` read at that rev)
**Audience:** engine write-path, recovery, and server maintainers

---

## 0. Decision summary

The RFC-022 commit protocol is sound: every write arms an identity-bearing
recovery intent before its first durable effect, publishes graph visibility in
one `__manifest` CAS, and resolves interruptions all-or-nothing. This RFC does
not change that protocol. It changes **who owns the protocol's lifetime** and
**where its residuals may be resolved**, closing two structural defects that
turn a correct protocol into an availability incident:

1. **A cancellable caller future owns a multi-step durable protocol.** The
   server awaits `mutate_as` inside the request future
   (`crates/omnigraph-server/src/handlers.rs:697`), and no `Drop` guard exists
   anywhere in the staging/publisher/recovery path. A client disconnect
   therefore aborts the engine mid-protocol and parks an `Armed` sidecar.
2. **The only full-resolution barrier is process restart.** The live-handle
   heal is deliberately roll-forward-only
   (`crates/omnigraph/src/db/manifest/recovery.rs:425-429`): sidecars that
   need restore or abort are "LEFT ON DISK for the next ReadWrite open". An
   `Armed` residual on a live server wedges writes to its tables for the rest
   of the process lifetime, and the restart that resolves it performs a single
   unsupervised open attempt per graph — a transient object-store error during
   that open quarantines the graph until the next restart
   (`crates/omnigraph-server/src/lib.rs:1284-1343`).

Three coordinated, independently landable changes:

- **W1 — Engine-owned write lifetime (cancellation shielding).** Once a write
  operation begins its gated protocol, its execution is owned by a spawned
  task, not the caller's future. Caller cancellation means "stopped waiting
  for the result", never "abandoned the protocol". Staged: the server boundary
  first (the established spawn-and-clone idiom), then engine-level `'static`
  write execution as the durable close for embedded SDK callers.
- **W2 — Full recovery at the write-entry barrier.** The write-entry heal
  escalates rollback-class residuals to `RecoveryMode::Full` under *exclusive*
  admission plus the existing schema → branch → table gates. The property
  restore actually requires is exclusive authority over the affected tables;
  process start was only ever a proxy for it. `RecoveryRequired` becomes a
  transient condition resolved by the next write, not an operator event.
- **W3 — Boot supervision.** A graph whose open fails enters an observable
  `quarantined` registry state with a capped-backoff re-open loop, instead of
  silently dropping out of the handle set until redeploy. The engine's
  fail-closed recovery classification is untouched; the retry unit is the
  whole (idempotent) open.

Together they establish one new standing rule, proposed for
[invariants.md](../dev/invariants.md):

> **Caller fate is not a protocol participant.** An armed graph-write protocol
> completes or compensates under engine ownership regardless of caller
> cancellation, and resolving any recovery residual requires only exclusive
> gate acquisition — never process restart.

## 1. Motivation: a validated field incident

A production report against 0.8.1 described a client-timeout-interrupted
multi-statement delete that left "torn state", wedged writes with
`stale view of 'edge:ProjectAsksQuestion': expected manifest table version 2
but current is 3`, and — after the next boot's cleanup hit a transient S3
error — a hard-quarantined graph that stayed offline until a second redeploy.

Every link of that chain was validated against current code, including Lance
upstream source at the pinned rev:

| Incident observation | Validated mechanism |
|---|---|
| Client 5-minute timeout aborted the delete mid-flight | `run_mutate` awaits `mutate_as` inline in the request future; hyper drops the handler future on disconnect; no `Drop` compensation exists in `exec/staging.rs`, `db/manifest/publisher.rs`, or `db/manifest/recovery.rs` |
| "stale view … expected 2 but current is 3" on later writes | `OmniError::manifest_expected_version_mismatch` (`src/error.rs:247`) — the typed OCC conflict raised while the orphaned table HEAD sits ahead of the manifest |
| Wedged until restart | The live-handle heal runs `RecoveryMode::RollForwardOnly`; the `Armed` residual is rollback-class and is parked for the next ReadWrite open (`recovery.rs:420-429`) |
| Boot "cleanup" wrote `_versions/18446744073709551611.manifest` | Ordinary recovery table commit: Lance V2 manifest naming writes `_versions/{u64::MAX − version, 020-padded}.manifest`; that filename decodes to **version 4** — exactly what a restore at drifted HEAD 3 commits (upstream `lance-table/src/io/commit.rs`, `ManifestNamingScheme::V2`; `Dataset::restore` commits `Operation::Restore` at latest + 1, `lance/src/dataset.rs:1272`) |
| "Failed to clean up orphaned transaction file…" | Verbatim Lance warning on the best-effort transaction-file delete after a failed commit (upstream `lance/src/io/commit.rs:114`); the orphaned `_transactions/` file is harmless residue reclaimed by cleanup |
| "graph quarantined during startup" until a "lucky" redeploy | One open attempt per graph, `warn!` and exclude, no retry loop, no runtime re-open, no health surface (`lib.rs:1284-1343`); the second redeploy's Full sweep was the designed recovery path finally running to completion |

No reader ever observed torn state — snapshot isolation over
manifest-published versions held throughout — and the final outcome was the
all-or-nothing one. What failed was availability: an interruption that the
protocol is designed to absorb instead cost the graph its writability for a
process lifetime and its entire service for a deploy cycle.

The long-run liability argument (the project's first principle): today, every
`await` point between sidecar arm and sidecar delete is a latent torn-state
site, and cancel-safety is maintained *by audit* — every future change to the
write path must re-reason about cancellation at every await. W1 replaces that
recurring obligation with a structural property. Similarly, W2 replaces
"remember that `Armed` residuals need a restart" — operational knowledge that
lives in runbooks and pages — with a barrier the system crosses on its own.

## 2. Current architecture (what this RFC preserves)

For reviewers: the boundaries this RFC deliberately does **not** move.

- The RFC-022 protocol order — resolve/refuse relevant intents, capture the
  base under admission → schema → branch → table gates, arm the recovery-v9
  sidecar before the first durable effect, commit exact per-table
  transactions, confirm achieved effects, publish once through the manifest
  CAS, delete the sidecar — is unchanged in every branch.
- `Armed` remains rollback-only for the established writers;
  `EffectsConfirmed` remains roll-forward-only under captured authority.
  Ambiguity still fails closed. No classification rule is weakened.
- The documented single-writer-process support boundary is unchanged. W2 does
  not make destructive recovery safe against a live *foreign process*; that
  still requires the distributed fence tracked in
  [invariants.md](../dev/invariants.md) Known Gaps.
- The sidecar wire format, recovery schema versions, and on-disk layout are
  untouched. Nothing in this RFC is a format change.
- Rollback already ends published: a successful roll-back "appends one restore
  commit and then publishes" the restored versions (`recovery.rs:3509-3512`),
  and the bound plan persists before the first restore so a retry replays it
  (`recovery.rs:1027`). W2 and W3 lean on that existing idempotency; they do
  not add a new compensation mechanism.

## 3. W1 — Engine-owned write-protocol lifetime

### 3.1 Problem shape

`Omnigraph`'s write entry points are `&self` async methods; the server holds
`Arc<Omnigraph>` and awaits them inline in the request handler. When the
client connection closes, hyper drops the handler future and the engine
future with it, at whatever await point it has reached. Post-arm, that parks
an `Armed` sidecar; the write-queue guards release on drop, so a *subsequent*
writer immediately trips over the residual (and, per current W2-less behavior,
cannot resolve it).

The existing cancellation test
(`writes.rs::cancelled_mutation_future_leaves_no_state`) cannot see this: its
final `Omnigraph::open` runs the Full sweep, which silently heals any leaked
sidecar before the assertions run, and its doc comment ("only orphaned Lance
fragments can remain") predates the RFC-022 sidecar protocol. That comment is
corrected as part of this work.

### 3.2 Stage 1 — server-boundary shield

The server already documents and uses the exact idiom needed:
`registry.rs:14-19` specifies that a request's own `Arc<GraphHandle>` clone
keeps the engine alive across registry swaps, and `server_export`
(`handlers.rs:620-637`) spawns engine work that survives the request future.
Stage 1 applies the same spawn-and-clone shape to every `_as` writer handler
(`run_mutate`, `run_ingest`, schema apply, branch create/delete/merge,
maintenance):

```rust
let engine = Arc::clone(&handle.engine);
let task = tokio::spawn(async move {
    engine.mutate_as(&branch, &query, &name, &params, actor_id.as_deref()).await
});
let result = task.await /* JoinError → 500 */ ?;
```

Semantics:

- The HTTP response contract is unchanged: the handler still awaits the
  result and returns the same status/body mapping. No 202/async-job surface
  is introduced.
- On client disconnect, the request future drops but the spawned task runs
  the protocol to its own terminal state: success (sidecar deleted) or error
  (compensated by the operation's existing error path). The result is
  discarded.
- The per-actor admission permit moves into the spawned task and is released
  at protocol completion, not at disconnect. This is intentional: admission
  bounds concurrent *work*, and the work genuinely continues. A disconnecting
  actor cannot free its own slot early by hanging up.
- Panic in the task surfaces as `JoinError` → 500, the same observability as
  today's unwind through the handler.

Stage 1 closes the production HTTP surface — the only caller class that
routinely drops futures mid-flight. The CLI is not shielded by Stage 1 and
does not need to be: a CLI future is only dropped by process death, which is
crash-class and already recovery-covered.

### 3.3 Stage 2 — engine-level `'static` write execution

Embedded SDK callers can still drop futures (timeout combinators, select
loops). The durable close moves the shield into the engine so the property
holds for every consumer.

Validated constraint: `Omnigraph` is not `Clone`, and `commit_all` takes
borrowed `&WriteTxn` / `&LineageIntent`, so this is a real restructure, not a
wrapper. The struct is already Arc-shaped where it matters — `coordinator:
Arc<RwLock<GraphCoordinator>>`, `storage: Arc<dyn StorageAdapter>`,
`read_caches: Arc<…>`, `schema_view: Arc<ArcSwap<…>>`, and the root-scoped
write queues are process-shared — so the design question is packaging, not
architecture. Two candidate shapes, resolved during implementation review:

- **(a) `Arc<Self>` receivers:** shielded variants
  `pub async fn mutate_as(self: &Arc<Self>, …)` clone the Arc into the
  spawned protocol task; existing `&self` methods delegate where possible.
- **(b) Inner-handle split:** move the shared fields into one
  `Arc<OmnigraphInner>`; `Omnigraph` becomes a cheap-clone façade and write
  entry points spawn with a clone of the inner Arc.

Either way, the write path's inputs become owned at the entry point (query
text, params, branch — all cheap clones), and the spawned future is
`Send + 'static` (engine futures already run inside hyper's spawned
connection tasks today, so `Send` holds).

Contract note: `crates/omnigraph/tests/forbidden_apis.rs` classifies every
public async inherent `Omnigraph` method. Any new or re-signed entry point
updates that registry in the same commit, per its protocol.

### 3.4 Non-goals

No detached "fire-and-forget" write API, no job queue, no server-side
completion watchdog beyond the existing Lance 30-minute commit timeout. The
caller still waits; only abandonment semantics change.

## 4. W2 — Full recovery at the write-entry barrier

### 4.1 Design

W1 prevents cancellation-created residuals, but crash-class residuals
(kill -9, OOM, power) remain, and "wedged until restart" must die
independently. The change: when the write-entry heal
(`heal_pending_sidecars_roll_forward`, `recovery.rs:3601`) encounters a
sidecar whose classification is rollback-class (today's parked case), it
escalates that sidecar to `RecoveryMode::Full` under strengthened gates
instead of parking it.

Mechanics, building on what the heal already does:

1. **Gates.** The heal already acquires admission → schema → branch → table
   guards in the one total order shared by writers, Full recovery, and live
   healing, and re-reads the sidecar under those gates so a completed writer's
   deletion or durable confirmation is respected (`recovery.rs:3617-3666`).
   One validated delta is required: ordinary sidecars currently take **shared**
   stream admission (`recovery.rs:3644-3648`); a Full-mode heal performing
   restore escalates to **exclusive** admission for the affected tables, the
   same class the open-time sweep effectively enjoys and the same shape
   RFC-026 enrollment/fold sidecars already use on this path.
2. **Dispatch.** `process_sidecar` already takes `RecoveryMode` as a
   parameter; the heal passes `Full` for the escalated sidecar. Roll-forward
   cases are unchanged (they already resolve in-process).
3. **Completion.** Rollback uses the existing machinery unchanged: persisted
   bound plan, restore commit at HEAD + 1, publication of the restored
   versions, recovery audit row, sidecar delete. No residual
   `HEAD > manifest` drift survives the heal, because publication of restored
   versions is already part of the rollback contract.
4. **Then the triggering write proceeds** from a fresh capture, exactly as if
   it had opened a clean graph.

### 4.2 Safety argument

The rule being relaxed is "restore only at the open-time barrier". The
property restore actually needs is **exclusive authority over the affected
tables while the restore-and-publish completes**:

- **In-process writers** are excluded by the exclusive admission + table
  gates — stronger than the open-time situation, where the barrier is merely
  "no traffic has started yet".
- **In-process readers** are safe by construction, not by exclusion: reads
  pin manifest-published versions, restore *appends* a new version (upstream
  `Dataset::restore`, validated at the pinned rev), and read caches are keyed
  by version/e_tag. A reader mid-query during a heal observes exactly what it
  observed before the residual existed.
- **Foreign processes** are exactly as (un)protected as at open time. The
  documented single-writer-process boundary owns that risk today for the
  restart path; W2 neither widens nor narrows it. The distributed fence
  remains future work with its own gate.

Process start was a proxy for exclusivity. The root-scoped
`WriteQueueManager` provides the real thing, so the barrier moves from
"restart" to "next write's gate acquisition".

### 4.3 Contract changes

`RecoveryRequired` (HTTP 503) becomes transient: the *next* write to the
affected tables resolves the residual instead of every write failing until
restart. The [errors.md](../user/operations/errors.md) guidance — "resolve the
sidecar through a read-write reopen/server restart" — is rewritten to
"resolves automatically on the next write or reopen; back off and retry".
Per Hyrum's law this narrowing is compatible (clients coded to the current
contract simply see their backoff succeed sooner), but it is an observable
behavior change and is documented as such in the release notes.

## 5. W3 — Boot supervision: quarantine as a converging state

### 5.1 Design

The engine keeps deciding once and failing closed; the server gains the
supervision loop it currently lacks. Division of labor:

- **Registry state.** A graph whose open fails enters
  `Quarantined { since, attempts, last_error, retry_at }` in the registry
  instead of vanishing from the handle set. (The registry's documented `Gone`
  direction already anticipates non-serving states.)
- **Re-open loop.** One supervision task per quarantined graph retries the
  full `open_single_graph` with capped exponential backoff plus jitter
  (proposed: 5 s initial, ×2, 10 min cap; final values are an unresolved
  question below). The retry unit is deliberately the *whole open*: recovery
  is idempotent by its replay-bounded plans and pinned by the
  convergence-idempotent roll-forward regression, so re-driving it is always
  safe, and no per-I/O retry policy is smeared through the recovery module.
- **Observability.** `GET /graphs` gains a per-graph `status` field
  (`serving` | `quarantined`) with the quarantine detail object. This is an
  additive OpenAPI change (`openapi.json` regenerated in the same PR) and
  answers the incident report's "quarantine is invisible except in logs".
- **Strict mode preserved.** `--require-all-graphs` still aborts startup on
  any failed graph; supervision applies only to the default
  serve-the-healthy-subset mode.

In the field incident, W3 alone turns "offline until a lucky redeploy" into
"offline for one backoff interval".

### 5.2 What W3 does not do

No engine change. No weakening of fail-closed classification. No retry of
*ambiguity* — a sidecar that classifies as indeterminate keeps failing the
open on every attempt (loudly, now visibly in `GET /graphs`) until an
operator intervenes; what the loop absorbs is transient substrate I/O.

## 6. Invariants & deny-list check

Walked against [invariants.md](../dev/invariants.md):

- **Inv 2 (one publication door):** untouched. W1/W2/W3 add no visibility
  path; W2 uses the existing restore-then-publish rollback.
- **Inv 3 (one coherent accepted view):** untouched; the escalated heal
  re-reads the sidecar and captures fresh authority under gates, as today.
- **Inv 5 (recovery is part of the commit protocol):** strengthened. Writers
  still resolve or refuse every relevant intent; W2 widens *where* resolution
  may complete, not *what* may be resolved. Ambiguity still fails closed.
- **Inv 6 (durability before acknowledgement):** untouched. W1 changes who
  waits, not what is durable before a success response.
- **Inv 13 (failures bounded, typed, observable):** improved — quarantine
  becomes typed observable state; `RecoveryRequired` keeps its type with a
  narrower lifetime; the boot loop is bounded by backoff caps.
- **Inv 15 (one source of truth):** untouched; no new derived copies. The
  quarantine registry entry is process-local supervision state, not graph
  authority.

Deny-list brushes, and why each is not a violation:

- *"Job queue for manifest-derivable state"* — the W3 loop supervises
  process-local open failures, not manifest-derivable graph state; the
  resolution barriers (write entry, open) remain event-driven. No timer-driven
  sidecar janitor is added (explicitly rejected, §9).
- *"Acknowledging before durable persistence"* — unchanged; W1's spawned task
  completes the same durable protocol before the same response.
- *"Shipping observable behavior as if it weren't contract"* — the 503
  lifetime change and the disconnect-surviving mutation are called out as
  contract changes with docs and release notes (§4.3, §8).

One stated rule is relaxed under this RFC's review: the recovery-mode doc
comment's "restore … for the next ReadWrite open" becomes "restore under
exclusive gate acquisition (write-entry heal or open)". This is the
invariant-review artifact for that relaxation.

## 7. Evidence plan

Per the repo's test-first rule, each change lands as a red test commit
followed by the fix commit.

**W1 (red first):**
- New failpoint cell: rendezvous-park the mutation protocol *post-arm,
  pre-confirmation*; drop the caller future; release; assert the operation
  reaches a terminal state and `__recovery/` is empty **without any reopen**
  (listing sidecars directly — the existing cancellation test is blind here
  because its `Omnigraph::open` sweeps first). Red today; green under the
  shield.
- Server-level test: spawned-server mutation with the client connection
  dropped mid-flight (failpoint-parked); assert the mutation publishes and a
  follow-up read observes it.
- Same-PR docs: correct the stale
  `cancelled_mutation_future_leaves_no_state` doc comment.

**W2 (red first):**
- Failpoint cell: manufacture an `Armed` residual (crash-injection, not
  cancellation, so it stays valid after W1); a *same-handle* write must fully
  heal it and then succeed. Today this asserts `RecoveryRequired`; the
  expectation flip is the deliberate, reviewed contract change.
- Concurrent-reader cell: a long-lived snapshot read spanning the heal
  observes identical results before and after the restore-and-publish.
- Admission cell: the escalated heal takes exclusive admission (extend the
  existing write-queue unit tests).
- Existing open-time cells (`ensure_indices_complete_armed_effects_roll_back`,
  the SchemaApply/Optimize/BranchMerge rollback matrix) remain untouched and
  green — open-time behavior is unchanged.

**W3 (red first):**
- Server test: inject one transient storage fault into boot-time recovery;
  assert the graph is `quarantined` in `GET /graphs`, then converges to
  `serving` without a restart.
- Strict-mode test: `--require-all-graphs` still aborts.
- OpenAPI regeneration committed (`OMNIGRAPH_UPDATE_OPENAPI=1`).

**Docs in the same PRs:** [writes.md](../dev/writes.md) (protocol lifetime
ownership), [invariants.md](../dev/invariants.md) (the new standing rule +
recovery-mode relaxation), [errors.md](../user/operations/errors.md) (503
guidance), [server.md](../user/operations/server.md) (quarantine status,
disconnect semantics), release notes.

## 8. Behavior-change ledger (Hyrum's law)

Observable changes shipped by this RFC, stated as contract:

| Change | Before | After |
|---|---|---|
| Mutation vs client disconnect | Work cancelled mid-protocol; outcome ambiguous *and* residual likely | Work completes server-side; outcome ambiguous to the disconnected client only (verify by read) |
| `RecoveryRequired` (503) lifetime | Until process restart | Until the next write to the affected tables (or reopen) |
| Admission slot on disconnect | Released at cancellation | Released at protocol completion |
| `GET /graphs` | Serving graphs only | All configured graphs with `status`; additive schema |
| Quarantine | Terminal until restart, log-only | Converging with backoff, visible in `GET /graphs` |

## 9. Drawbacks & alternatives

- **Do nothing.** The protocol is correct and self-heals at restart; clients
  can be taught generous timeouts. Rejected: cancel-safety-by-audit is a
  recurring cost on every write-path change, and the availability failure
  mode is disproportionate to its trigger (one impatient client).
- **Per-await cancel-guards / `select!` hardening.** Symptomatic: every new
  await point reopens the bug class. Rejected.
- **Drop-guard compensation (`impl Drop` spawning cleanup).** Compensation
  from a `Drop` impl cannot await and would race the very gates that make
  recovery safe. Rejected in favor of not being cancelled at all.
- **202-Accepted async job API.** A larger product-surface change that
  weakens the synchronous contract every existing client relies on; not
  needed to fix the defect. Rejected (revisitable independently).
- **Per-I/O retry inside recovery.** Blurs the fail-closed line between
  transient error and ambiguity, and smears retry policy through the module.
  Rejected: the idempotent retry unit is the whole open (W3).
- **Timer-driven background sidecar janitor.** Adds a third resolution path
  to keep consistent with two event-driven ones, for no coverage the
  write-entry barrier doesn't already give once W2 lands. Rejected.
- **Weakening `Armed` classification (roll forward unconfirmed effects).**
  Violates the partial-publish rule outright. Rejected without ceremony.

Costs accepted: one task spawn per write (microseconds, off the hot path's
semantics); a modest engine restructure in W1 Stage 2 with a
`forbidden_apis.rs` registry update; slightly longer admission occupancy for
disconnected writes (bounded by the write caps and the Lance commit timeout);
one supervision task per quarantined graph (zero when healthy).

## 10. Reversibility

Fully reversible. No on-disk, sidecar-schema, or wire-format change; the one
additive API field is `GET /graphs.status`. W2 can be reverted by passing
`RollForwardOnly` again at the heal site; W3 by removing the supervision
loop; W1 Stage 1 by unspawning the handlers. Per the project's reversibility
rule this demands test evidence rather than irreversibility-grade proof — the
RFC exists because W2 relaxes a stated recovery rule (invariant-review
process) and because the three changes form one contract users will observe
together.

## 11. Rollout

Three independent PR series, in leverage order, each red-test-first with docs:

1. **PR-1 (W1 Stage 1):** server-boundary shield + the two W1 tests + stale
   comment fix. Smallest change, kills the incident's trigger.
2. **PR-2 (W2):** exclusive-admission escalation + `Full` at the heal +
   expectation flips + errors.md/invariants.md updates.
3. **PR-3 (W3):** registry quarantine state + supervision loop + `GET
   /graphs` status + OpenAPI regen + server.md.
4. **PR-4 (W1 Stage 2):** engine `'static` write execution (`Arc<Self>`
   receivers or inner-handle split, per review), `forbidden_apis.rs` registry
   update, SDK-level cancellation test.

## 12. Unresolved questions

- **W1 Stage 2 shape:** `Arc<Self>` receivers vs an `Arc<OmnigraphInner>`
  split. The split is the cleaner long-run shape but touches every field
  access; receivers are surgical but bifurcate the public API surface.
- **Backoff policy for W3** (initial/cap/jitter) and whether attempts should
  be capped at all, or retry indefinitely at the cap interval.
- **Whether the shielded task needs its own deadline** beyond Lance's
  30-minute commit timeout, and what a deadline expiry should do (it must not
  cancel the protocol — at most it can alert).
- **Whether quarantine should also surface via a health/readiness endpoint**
  for orchestrators that never call `GET /graphs`.
- **Interaction with the future distributed fence:** when cross-process
  recovery fencing lands, the W2 exclusivity argument should be restated in
  terms of the fence rather than the process-local queue; flagged so the
  fence RFC picks it up.

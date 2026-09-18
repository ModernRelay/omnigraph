---
rfc: "2026-09-18-shared-schema-gate"
title: "Shared schema gate and the write critical section"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-09-18
updated: 2026-09-18
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "RFC 0067 (detached table commits) merging: the safety argument below is stated against the detached write path and does not hold on the sidecar-era engine."
---

# RFC: Shared schema gate and the write critical section

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

## Summary

Resolve [RFC 0067](0067-detached-table-commits.md)'s open question (#643): the
process-local ***schema gate*** — the write-queue key
`("__schema_apply__", None)` that every writer, every maintenance pass, every
branch control, and every read-view capture takes exclusively today — becomes a
shared/exclusive lock. A ***contract-lifecycle pass*** (schema apply, the
system-column upgrade, and every pass that installs, discards, or republishes
the accepted schema-contract view: open, refresh, settle, reload, branch sync)
takes it exclusive, exactly as it effectively does today. Everything else —
ordinary mutations and loads, merges, index maintenance, optimize, cleanup,
repair, branch control, and read-view captures — takes a ***shared permit***.

The branch gate stays: same-branch writers still serialize from revalidation
through publication, per RFC 0067's own recommendation. The per-table gates
stay: they keep two same-table stagers from wasting one staging. Promotion —
already correct with no gate held — moves after guard release as a separate,
independently revertible sub-decision.

The net effect is the second step of RFC 0067's throughput path: cross-branch
writers, independent merges, and reads stop serializing process-wide on one
mutex, while a schema apply still excludes every writer and every writer still
excludes a schema apply. Nothing cross-process changes; the manifest CAS
remains the only cross-process authority.

## Motivation

The concurrent-writes benchmark scenario (the whole-run instrument RFC 0067's
"What to measure" section commissioned; `benchmarks/README.md`,
"Concurrent-writes throughput diagnostics") has measured what the exclusive
gate costs. Sequential runs, both engines, closed-loop and therefore
diagnostic rather than claim-grade (RFC 0039 Rule 1), all writers on one
branch (`--write-branches 1`):

| regime | writers | commits/s (median) | service p50 | p95 |
|---|---|---|---|---|
| local FS, detached engine | 1 | 18.9 | 52 ms | 76 ms |
| local FS, detached engine | 8 | 17.2 (−9%) | 62 ms | 2.6 s |
| RustFS + 30 ms injected RTT, detached | 1 | 0.64 | ~1.5 s | 1.9 s |
| RustFS + 30 ms injected RTT, detached | 8 | **0.21 (−68%)** | ~2.6 s | 4.5 s |
| RustFS + 30 ms injected RTT, sidecar-era main | 8 | 0.15 (−76%) | ~3.4 s | 3.9 s |

Eight writers deliver *less* than one, and at object-store latency the
degradation is not a plateau but a collapse: every gated writer revalidates
against authority that moved N−1 times while it queued, and each internal
reprepare re-pays latency-priced reads (manifest reads per commit double from
~95 to ~178 between w1 and w8 at 30 ms). Every one of those writers crossed
the same exclusive schema gate regardless of which branch it wrote.

The same gate sits on the read path: `capture_read_view` and its historical
and current variants take it exclusively for the catalog-build window. Under
write load on a remote store, a read can wait behind a writer's entire
publish hold (about 1.5 s per commit at 30 ms RTT), because the writer's
guards are held from revalidation through promotion. The developer guide's
claim that "reads are snapshot-isolated and do not take write gates"
(`docs/dev/architecture.md`) is wrong today; this RFC makes it true for the
write gates that matter and corrects the sentence either way.

This step is also the enabler for the path's third step. Group commit batches
N ready publications into one CAS — but under the current gates, writers
never *arrive* at the publisher concurrently, so every batch would have size
one. Shrinking the serialization to the branch is what lets a same-branch
batch form at all and lets cross-branch writers stop forming one global queue.

## User and operational behavior

No API, format, wire, or configuration change. Observable differences:

- Cross-branch concurrent writers scale instead of serializing process-wide;
  same-branch writers keep today's ordering and conflict behavior.
- A read no longer waits for an unrelated writer's publish window to capture
  its catalog view; it waits only for an in-flight contract-lifecycle pass,
  as it must.
- A schema apply or system-column upgrade still waits for every in-flight
  shared holder to drain, then excludes all of them — the same fairness as
  today's mutex, made explicit by a write-preferring lock: once the exclusive
  acquisition is queued, new shared permits queue behind it, so a stream of
  writers cannot starve a schema apply.

## Design

### The lock

The schema gate leaves the ordinary `queues` map of `WriteQueueManager`
(`crates/omnigraph/src/db/write_queue.rs`) and becomes a dedicated
`tokio::sync::RwLock<()>` slot beside the existing `export_gate` — the
in-repo precedent for a shared/exclusive permit pair (`ExportCutPermit` /
`ExportDestructivePermit`). Two typed permits replace the anonymous
`QueueGuard` at the schema position:

- `SchemaSharedPermit` — read side; taken by commit_all, the no-op
  conditional-mutation CAS, merge, ensure_indices and the full-text rebuild,
  optimize, cleanup, repair, branch create/create-from/delete, and the
  read-view captures.
- `SchemaExclusivePermit` — write side; taken by schema apply, the
  system-column upgrade, and the contract-lifecycle passes on the handle:
  open, refresh, `settle_pending_schema_install`, `reload_schema_if_source_changed`,
  and `sync_branch`'s coordinator swap.

The classification rule is auditable in one sentence: **a pass that only
reads the accepted contract/catalog view shares; a pass that can change which
view is accepted excludes.** That is the same division the key's own
documentation already draws ("serializes every graph-global schema writer …
against the passes that install or discard a staged schema contract",
`crates/omnigraph/src/db/manifest.rs`).

Acquisition order is unchanged: schema permit → branch gate → sorted
per-table gates, and the permits ride in `CommittedMutation.guards` with the
same caller-held drop discipline. The gate remains non-reentrant in both
modes; the existing self-deadlock notes carry over.

### DST determinism

`QueueGuard` release currently takes a `dst_gate::turn()` and bumps a release
epoch so the simulation arbiter orders gate transitions deterministically.
The new slot keeps that protocol on both modes: shared and exclusive
acquisition pass through the same `scheduled_lock`-style turn point, and both
permit drops take a turn and bump the slot epoch. Without this, seeded
interleavings silently stop replaying; with it, the DST arbiter sees the same
event vocabulary it sees today.

### Promotion outside the guards

`promote_held_all` runs today inside all three gates although its own
contract states the write is already durable and graph-visible and a failure
is left for the next writer. This RFC moves promotion after guard release:
the branch gate frees one Lance-replay earlier, which at object-store
latency is a measurable slice of the hold. The sub-decision is independently
revertible (move one call back inside the guard scope) and is listed
separately in the evidence plan.

### Why the old rationale no longer binds

The write queue's module documentation rejects this split: "a
shared/exclusive split would add a writer-classification surface that's easy
to get wrong." Two facts changed under it.

First, RFC 0067 abolished the failure the exclusive gate was guarding. The
gate's stated purpose was closing the race where a mutation advanced a Lance
table HEAD and only then discovered an in-flight schema apply's lock
(`crates/omnigraph/src/db/omnigraph/schema_apply.rs`, the schema-control gate
comment). Mutations no longer advance HEADs before publication — every
pre-publication effect is a detached commit nothing references. A
misclassified site can no longer strand a moved HEAD behind a schema lock;
the worst a wrong shared classification yields is a stale-authority publish
attempt, which the manifest CAS refuses exactly as it refuses any other stale
writer. Correctness never rested on the gate; RFC 0067 says so explicitly
("nothing in this RFC depends on the gates for correctness").

Second, the classification surface is enumerable and closed. The 22
acquisition sites divide cleanly under the one-sentence rule above, the rule
is enforceable in review (a new site must name its permit type), and the
typed permit pair makes the choice visible at the call site instead of
implicit in a mutex.

## Invariants

- One publication door (invariant 2) is untouched: the CAS, its
  preconditions, and publisher OCC are unchanged.
- One coherent accepted view (invariant 3) is what the exclusive side
  protects: a contract-lifecycle pass still swaps the accepted view with no
  reader or writer in flight, because shared holders drain first.
- Recovery/pending-pin semantics (RFC 0067) are unchanged; promotion's
  gate-free correctness is already the engine's documented contract.
- The deny-list line "process-local locks presented as distributed writer
  fencing" is reaffirmed, not weakened: the gate remains an in-process
  contention structure; the durable `__schema_apply_lock__` sentinel, the
  mono-branch refusal, and the manifest CAS remain the cross-process story,
  byte-for-byte.

## Compatibility and reversibility

Process-local only. No storage, format, protocol, or API change; nothing an
old or foreign binary can observe. Fully reversible by reverting the lock
type and permit classification in one commit. Because reversibility is
total, no flag or staged rollout is warranted.

## Alternatives

- **Status quo.** Keeps the measured collapse: −68% at 8 writers under
  object-store latency, reads stalled behind publish windows, and group
  commit permanently starved of batches.
- **Per-branch schema gates.** Wrong shape: the schema contract is
  graph-global; a per-branch gate would let a writer share with the apply
  that is about to invalidate its catalog.
- **A storage-level shared lock.** Rejected on the deny-list: the gates are
  an in-process optimization, and promoting them to distributed fencing
  inverts the architecture's explicit warning. Cross-process exclusion
  remains the CAS.
- **Dropping the schema gate for writers entirely.** Rejected: a writer must
  still exclude a concurrent contract swap in-process, or it can execute
  against a catalog the apply is replacing mid-flight; the shared permit is
  exactly that exclusion at the correct grain.

## Evidence and tests

The acceptance bar for the implementation PR, mapped to existing owners:

- **Whole-run instrument, before/after.** The concurrent-writes scenario at
  `--writers 8 --write-branches {1,8}`, local FS and RustFS with the 30 ms
  injected-latency recipe. Success: w8×B8 scales toward B× the single-writer
  rate while w8×B1 stays branch-gate-bound and unchanged;
  `authority_conflicts` falls at B>1; the record shows the critical section
  shrank to revalidate-and-publish. The B-axis baselines are recorded before
  the change lands.
- **Existing pins that survive as-is:** the branch-gate exclusivity cell
  (`failpoints.rs`, cross-handle branch-gate serialization), optimize's
  main-gate hold over disjoint tables, and the 16-handle strict-load race in
  `consistency.rs`, which is gate-scope-agnostic by construction.
- **Pins to re-derive:** the mid-apply blocking test in `schema_apply.rs`
  (its assertion — a mutation stays pending while an apply is in flight —
  survives; its mechanism becomes reader-behind-writer and is re-verified),
  and the read-only-open and refresh gate-hold tests, which split along the
  new classification (capture shares; install/publication excludes).
- **New coverage this RFC commissions:** a DST concurrent-universe arm that
  races ordinary writers against a schema apply. Today's concurrent universe
  has no schema apply in its op mix, so the shared/exclusive boundary would
  otherwise land with zero deterministic-simulation coverage. The existing
  seeded seam scheduler and the write queue's turn/epoch hooks are the
  mechanism.
- **Cost contracts unchanged:** `write_cost.rs` per-op counts (manifest
  ceiling, the schema fence's exact read/exists counts) are asserted
  byte-identical — gate scope changes overlap, never operation counts.
- **Docs that move in the same change:** the gate-order section of
  `docs/dev/writes.md`, and the read-path sentence in
  `docs/dev/architecture.md`.

## Rollout

One implementation PR after acceptance: lock + permits + classification +
promotion move + test re-derivations + the DST arm + doc updates, with the
before/after instrument runs in the PR body. No flag; reversibility is a
one-commit revert.

## Unresolved questions

- Group commit (RFC 0067 path step 3) restructures the same critical section
  at the publisher; it composes with this change (it needs concurrent
  arrivals, which this change creates) and is a separate proposal.
- Whether optimize and cleanup should take the exclusive side out of caution
  rather than the shared side. They hold every table gate they touch, so
  they already serialize with writers at table grain; this RFC proposes
  shared and records the question for review.

## Decision log

- 2026-09-18 — Drafted against the detached-commit engine, with the
  concurrent-writes instrument's sequential cross-engine baselines as the
  motivating evidence. Blocked on RFC 0067 merging.

---
rfc: "2026-09-18-shared-schema-gate"
title: "Shared schema gate and the write critical section"
track: maintainer
status: accepted
implementation: complete
authors:
  - ragnorc
created: 2026-09-18
updated: 2026-09-19
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
independently revertible sub-decision (superseded: see Promotion outside the
guards).

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
- A read no longer waits for another handle's publish window to capture its
  catalog view; on the schema gate it waits only for an in-flight
  contract-lifecycle pass, as it must. A read through the writer's own
  handle still waits while that handle publishes on its bound branch: the
  publish holds the handle's coordinator lock across the manifest
  compare-and-swap, and the capture needs it (see Unresolved questions).
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
  read-view captures. The write capture (`open_write_txn`) takes it only
  while a schema-apply sentinel stands: it parks on the shared side until
  the apply releases, then recaptures; the common path takes no permit
  there.
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
> Superseded. The implementation measured this move as harmful (decision
> log, 2026-09-25: the successor writer promotes the same pin concurrently
> and the two replays serialize on Lance's commit path), and
> [Detached-only tables](2026-09-21-detached-only-tables.md) then removed
> promotion altogether, so there is nothing left to move. The
> `HeldWriteGates` envelope helper this section introduced stays.


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
- Recovery and pin semantics (RFC 0067, as amended by detached-only tables)
  are unchanged.
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
test re-derivations + the DST arm + doc updates, with the
before/after instrument runs in the PR body. No flag; reversibility is a
one-commit revert.

## Unresolved questions

- Group commit (RFC 0067 path step 3) restructures the same critical section
  at the publisher; it composes with this change (it needs concurrent
  arrivals, which this change creates) and is a separate proposal.
- Same-handle reads still wait on publication. A publish on a handle's bound
  branch holds that handle's coordinator lock across the manifest
  compare-and-swap (`commit_updates_on_branch_with_expected`), and a read
  capture takes the same lock. The server shares one handle per graph, so its
  reads on `main` still wait for its writes to `main`. Removing that wait means
  publishing without holding the coordinator lock and installing the new view
  afterwards; it is a separate change.

## Decision log

- 2026-09-18 — Drafted against the detached-commit engine, with the
  concurrent-writes instrument's sequential cross-engine baselines as the
  motivating evidence. Blocked on RFC 0067 merging.
- 2026-09-19 — Implemented. Optimize and cleanup take the shared side (they
  hold every table gate they touch, so they already serialize with writers
  at table grain); read-only open and reload stay exclusive as recorded
  conservatism. The mis-classification tripwire is
  `parked_writer_blocks_schema_apply`; the plain-mode fairness pin is
  `queued_schema_exclusive_blocks_later_shared`; the DST concurrent
  universe gained the schema-apply-racing-writers arm (strict replay: see
  2026-09-25).
- 2026-09-25 — Two findings from the DST arm's first runs, both now in the
  implementation. (1) The write capture sat outside the gate:
  `open_write_txn` checked the durable sentinel before any permit, so a
  writer arriving on another handle while an apply was in flight was refused
  with a typed conflict and retried hot until the apply ended, while one that
  had entered before the apply parked. A capture-scoped shared permit fixed
  that but cost every attempt two arbiter turns under the seam scheduler
  (the scheduler pin went from 0.8 s to 5.2 s, near its escape budget); the
  shipped shape parks on the shared side only when the sentinel probe is
  positive, then recaptures — zero cost on the common path, and the arm's
  seeds commit every write and every apply. (2) Promotion after guard
  release was measured harmful: with the envelope released first, the next
  same-branch writer finds the pin still pending and replays the same twin,
  so two promoters serialize on Lance's commit path — an engine-internal
  wait the seam arbiter cannot see (the scheduler pin escaped on every op,
  bisected to that commit alone). It was reverted; the `HeldWriteGates`
  helper stays. Strict replay for the schema arm is a
  hunt claim, not a CI pin: an apply's table rewrite runs on the single
  `lance-cpu` pool thread, invisible to the arbiter, so its stall budget
  trips under load; the plain-mode seeds are the pin and
  `dst_seam_scheduler_bite_and_replay` pins the permits' turn/epoch protocol.
- 2026-09-28 — Ported onto main after
  [detached-only tables](2026-09-21-detached-only-tables.md) removed
  promotion, which retires the promotion sub-decision outright, and after
  the `omnigraph-core` extraction. The 22 acquisition sites, the classification
  and the DST arm carry over unchanged; the write capture's sentinel probe
  now uses the coordinator's `schema_apply_locked`.
- 2026-09-28 — An independent review (Codex, `gpt-6-astra`) found three gaps,
  each verified in the code. (1) The documentation overstated the read path:
  a read on the writer's own handle still waits for that handle's coordinator
  during a publish on its bound branch; the text now says so and the
  remaining wait is listed under Unresolved questions. (2) The DST arm's
  non-vacuity check counted only writer alternations; the harness now counts
  applies that land between two writer commits, and requiring one exposed that
  none ever had: the schema actor applied back to back the moment the start
  barrier opened, and the write-preferring lock ran every apply before the
  first writer commit. The actor now pauses a seeded 2–31 ms before each apply,
  and every apply of every seed lands between writer commits. (3) The hunt ran
  reader actors whose read-only opens take the exclusive side with no arbiter
  hook, outside the turns `sched_escapes == 0` certifies; it runs without
  readers now.
- 2026-09-28 — The second half of RFC 0067's step 2, committing the detached
  table effects before taking the gates, was measured and not adopted. With
  timing probes in the write path, a single writer on one branch holds the
  gates for 7.3 ms locally (revalidation 0.6 ms, detached commit 1.1 ms,
  publication 5.6 ms) and 670 ms at +30 ms per round trip (298, 85 and 284 ms):
  the detached commit is 13–15% of the hold. Revalidation fails on any move of
  the branch head, so under same-branch contention nearly every attempt that
  waited for the gate loses: 6.6 failed revalidations per publication locally
  with eight writers, and 2.9 at +30 ms, where each failure holds the gate for
  about 300 ms before giving up (about 19 s of a 30 s window, more than the
  successful writes used). Committing detached before the gates would make
  each of those losers write a commit that is dead on arrival — roughly seven
  times the table writes, most of them garbage for the collector — to save at
  most 15% of the hold. The per-table gate plan in RFC 0067 would also invert
  the lock order: every production path takes table gates inside the branch
  gate, and the key includes the branch, so they add no exclusion today. The
  levers the measurement points to are a cheap in-process check that fails a
  stale attempt before revalidation's round trips, fewer round trips in
  revalidation itself, and footprint-granular admission, which is RFC 0067's
  group-commit rule.
- 2026-09-28 — The first lever was prototyped and measured, not adopted. A
  stale attempt was failed from the handle's in-memory view (same branch
  incarnation, a strictly newer manifest version than the capture, a
  different head) before revalidation's round trips; it caught every failed
  revalidation in both settings. With eight writers on one branch it raised
  throughput from 0.40 to 0.53 commits/s at +30 ms and cut manifest reads per
  commit from 60 to 43, but lowered it from 25.5 to 23.2 locally and widened
  the local p95 from 232 ms to 1.9 s: where revalidation is already cheap,
  failing faster only sends losers back to re-prepare and re-queue sooner.
  It also left the starvation of #784 unchanged. The measurement bounds this
  whole family of fixes. Because any publication invalidates every other
  attempt on the branch, eight writers on one branch cannot exceed one writer
  alone (31.7 commits/s locally, 0.87 at +30 ms); this change already reaches
  80% and 46% of that. Raising the ceiling requires same-branch writes that
  do not invalidate each other, which is the footprint admission of RFC
  0067's group commit (step 3), not further work on the gate.


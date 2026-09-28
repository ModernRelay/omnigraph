---
rfc: "2026-09-18-shared-schema-gate"
title: "Shared schema gate and the write critical section"
track: maintainer
status: accepted
implementation: complete
authors:
  - ragnorc
created: 2026-09-18
updated: 2026-09-29
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Shared schema gate and the write critical section

> A term set in ***bold italics*** is being defined at that exact spot; it is
> used plain everywhere after.

## Summary

Resolve the schema-gate half of [RFC 0067](0067-detached-table-commits.md)'s
open question ([#643](https://github.com/ModernRelay/omnigraph/issues/643)):
the process-local ***schema gate***, the write-queue key
`("__schema_apply__", None)` that every writer, every maintenance pass, every
branch control, and every read-view capture took exclusively before this
change, becomes a shared/exclusive lock. A ***contract-lifecycle pass***
(schema apply, the system-column upgrade, and every pass that installs,
discards, or republishes the accepted schema-contract view: open, refresh,
settle, reload, branch sync) takes it exclusive, exactly as it effectively did
before. Branch create and create-from take it exclusive too: their namespace
inventory changes the live branch-ref set with no CAS over the change (Design,
The lock). Everything else (ordinary mutations and loads, merges, index
maintenance, optimize, cleanup, repair, branch delete, and read-view captures)
takes a ***shared permit***. The merge case of that question, two merges
overlapping, has no evidence in this change (Evidence and tests).

The branch gate stays: same-branch writers still serialize from revalidation
through publication, per RFC 0067's own recommendation. The per-table gates
stay in the acquisition order, but they add no exclusion today: every
production path takes them inside the branch gate, and their key includes the
branch (decision log, 2026-09-28).

The net effect is the schema-gate half of the second step of RFC 0067's
throughput path (the other half, committing the detached effects before the
gates, was measured and not adopted: decision log, 2026-09-28): cross-branch
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
branch (`--write-branches 1`). The table's base commit and run date are not
recorded; the 2026-09-28 decision-log entries ran on a later base
(`b14c22c5` for the attribution entry) whose one-writer rates differ (31.7
and 0.87 commits/s there against 18.9 and 0.64 here), so the two sets do not
compare cell by cell:

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
guards are held from revalidation through publication. The developer guide's
claim that "reads are snapshot-isolated and do not take write gates"
(`docs/dev/architecture.md`) is wrong today; this RFC makes it true for the
write gates that matter and corrects the sentence either way.

This step is also part of the enabler for the path's third step. Group commit
batches N ready publications into one CAS, but under the former gate writers
never *arrived* at the publisher concurrently, so every batch would have size
one. Shrinking the serialization to the branch lets cross-branch writers stop
forming one global queue. It does not yet let a same-branch batch form:
same-branch writers still serialize at the branch gate before the publisher
(`commit_all`, `crates/omnigraph/src/exec/staging.rs`), so releasing the
branch gate ahead of the publisher is step-3 work.

## User and operational behavior

No API, format, wire, or configuration change. Observable differences:

- Cross-branch concurrent writers scale instead of serializing process-wide;
  same-branch writers keep today's ordering and conflict behavior.
- The server's reads on `main` still wait for its writes to `main`: the
  server shares one handle per graph, a publish on a handle's bound branch
  holds that handle's coordinator lock across the manifest compare-and-swap,
  and a read capture needs the same lock (see Unresolved questions). The gain
  on the read path reaches deployments with more than one handle: there a
  read no longer waits for another handle's publish window to capture its
  catalog view. On the schema gate a read waits only for an in-flight
  contract-lifecycle pass and, in plain mode, for a queued one: a queued
  exclusive acquisition (an apply, or an `Omnigraph::open` in the same
  process) parks every later shared permit until it runs, so a read can still
  wait behind another handle's publish window through that queue.
- A schema apply or system-column upgrade still waits for every in-flight
  shared holder to drain, then excludes all of them: the same fairness as the
  former mutex. In plain mode the lock is write-preferring: once the
  exclusive acquisition is queued, new shared permits queue behind it, so a
  stream of writers cannot starve a schema apply. Installed (DST) mode has no
  such preference (Design, DST determinism).

## Design

### The lock

The schema gate leaves the ordinary `queues` map of `WriteQueueManager`
(`crates/omnigraph/src/db/write_queue.rs`) and becomes a dedicated
`tokio::sync::RwLock<()>` slot beside the existing `export_gate` — the
in-repo precedent for a shared/exclusive permit pair (`ExportCutPermit` /
`ExportDestructivePermit`). Two typed permits replace the anonymous
`QueueGuard` at the schema position:

- `SchemaSharedPermit`, the read side: taken by `commit_all`, the no-op
  conditional-mutation CAS, `branch_merge_impl`, `ensure_indices` and the
  full-text rebuild, optimize, cleanup, repair, `branch_delete_as`,
  `ensure_no_pending_recovery`, the system-column upgrade under
  `options.check`, the read-view captures, and the test-only
  `reconcile_orphaned_branches`. The write capture (`open_write_txn`) takes
  it only while a schema-apply sentinel stands and the gate is busy: it
  parks on the shared side until the apply releases, then recaptures; the
  common path takes no permit there (Errors and budgets below).
- `SchemaExclusivePermit`, the write side: taken by schema apply, the
  system-column upgrade, the contract-lifecycle passes on the handle (open,
  refresh, `settle_pending_schema_install`,
  `reload_schema_if_source_changed`, and `sync_branch`'s coordinator swap),
  and `branch_create_as` and `branch_create_from_impl`.

The classification rule is auditable in one sentence: **a pass that only
reads the accepted contract/catalog view shares; a pass that can change which
view is accepted excludes, and so does a pass that changes the live
branch-ref set with no CAS over the change.** The second clause is branch
create: `create_branch_recoverably`
(`crates/omnigraph-core/src/branch_control.rs`) lists the refs, refuses a
`path_collision`, may reclaim a ref-less tree, then creates, and no CAS
covers that inventory, so two creates whose branch gates are disjoint
(`feature` from `main`, `feature/x` from `b`) both passed it under shared
permits. The rule lives on `SchemaGateSlot`
(`crates/omnigraph/src/db/write_queue.rs`); read-only open is the one site on
the exclusive side by conservatism rather than by the rule.

**Lock order and reentrancy.** The order is the schema permit, then the
branch gate or gates, then the sorted per-table gates, then the handle's
coordinator lock (`Arc<tokio::sync::RwLock<GraphCoordinator>>`);
`merge_authority_cache` is taken after the branch gate and before any
coordinator open (`branch_delete_as`, merge capture). A writer's permits ride
in `CommittedMutation.gates`, a `HeldWriteGates` (`_schema`, `_branch`,
`_tables`), with the same caller-held drop discipline. The gate is
non-reentrant in both modes on one task: an exclusive holder re-acquiring
either side self-deadlocks, and a shared holder re-acquiring the shared side
can park forever behind a queued exclusive in plain mode. No call path takes
the gate twice: `refresh_coordinator_only` serves holders that need a
coordinator refresh, and `refresh_for_reprepare` keeps the reprepare off the
exclusive side (decision log, 2026-09-29). `refresh` itself holds the
exclusive side twice in sequence, once around the coordinator refresh and
once inside `reload_schema_if_source_changed`.

**Errors and budgets.** Only `mutate` and `load` reach the write capture's
park; `branch_merge_impl`, `ensure_indices`, optimize, cleanup and repair
call `ensure_schema_apply_idle` first and get the typed `manifest_conflict`
one call earlier. In `open_write_txn` a standing sentinel
(`schema_apply_locked`, a live listing of `__manifest`'s `_refs/branches/`,
not an in-memory flag) is classified by the gate. If an exclusive permit is
held or queued in this process (`try_acquire_schema_shared` returns `None`),
the apply is local: the capture parks on the shared side, then recaptures
under the promoted contract. If the gate is free, the sentinel belongs to
another process's apply or a dead one: the capture returns the typed
`manifest_conflict` refusal once a second listing confirms the sentinel still
stands (an apply in this process may have released between the two). Parks
do not count against `MAX_CAPTURE_RETRIES` (8), which bounds torn captures
only; exhausting it returns `manifest_read_set_changed`. Nothing is emitted
while a capture is parked. Cost: the common path (no sentinel) takes no
permit and no extra request; each park costs one more listing, and the
refusal path one more again.

**Permit sites.** The 24 acquisition sites on this tree, one row per call
site (the write capture's park counts once; the system-column upgrade's two
sides count once each). Paths are under `crates/omnigraph/src/`.

| file | function | side |
|---|---|---|
| `db/omnigraph.rs` | `open_with_storage_and_mode` (read-write and read-only open) | exclusive |
| `db/omnigraph.rs` | `ensure_no_pending_recovery` | shared |
| `db/omnigraph.rs` | `open_write_txn` (the park, sentinel standing only) | shared |
| `db/omnigraph.rs` | `sync_branch` | exclusive |
| `db/omnigraph.rs` | `refresh` | exclusive |
| `db/omnigraph.rs` | `settle_pending_schema_install` | exclusive |
| `db/omnigraph.rs` | `reload_schema_if_source_changed` | exclusive |
| `db/omnigraph.rs` | `capture_read_view` | shared |
| `db/omnigraph.rs` | `capture_current_read_view` | shared |
| `db/omnigraph.rs` | `capture_historical_read_view` | shared |
| `db/omnigraph.rs` | `branch_create_as` | exclusive |
| `db/omnigraph.rs` | `branch_create_from_impl` | exclusive |
| `db/omnigraph.rs` | `branch_delete_as` | shared |
| `db/omnigraph/schema_apply.rs` | `apply_schema` | exclusive |
| `db/omnigraph/system_column_upgrade.rs` | `upgrade_system_columns`, `options.check` | shared |
| `db/omnigraph/system_column_upgrade.rs` | `upgrade_system_columns`, the upgrade | exclusive |
| `db/omnigraph/optimize.rs` | `optimize_all_datasets` | shared |
| `db/omnigraph/optimize.rs` | `cleanup_all_datasets` | shared |
| `db/omnigraph/optimize.rs` | `reconcile_orphaned_branches` (test-only) | shared |
| `db/omnigraph/repair.rs` | `repair_all_datasets` | shared |
| `db/omnigraph/table_ops.rs` | `maintain_indices_for_branch` (`ensure_indices`, the full-text rebuild) | shared |
| `exec/staging.rs` | `commit_all` | shared |
| `exec/mutation.rs` | `mutate_one_attempt` (the no-op conditional-mutation CAS) | shared |
| `exec/merge.rs` | `branch_merge_impl` | shared |

### DST determinism

`QueueGuard` release currently takes a `dst_gate::turn()` and bumps a release
epoch so the simulation arbiter orders gate transitions deterministically.
The new slot keeps that protocol on both modes: shared and exclusive
acquisition pass through the same `scheduled_lock`-style turn point, and both
permit drops take a turn and bump the slot epoch. Without this, seeded
interleavings silently stop replaying; with it, the DST arbiter sees the same
event vocabulary it sees today. Fairness differs by mode: plain mode
inherits tokio's write-preferring FIFO; installed mode never enters the
native waiter queue, both sides use `try_read_owned` and `try_write_owned`
inside the turn loop, so grant order and starvation-freedom are properties of
the seed, and the write preference is asserted only by the plain-mode unit
test `queued_schema_exclusive_blocks_later_shared`.

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
("nothing in this RFC depends on the gates for correctness"), and the code
carries it: `commit_all` (`crates/omnigraph/src/exec/staging.rs`) commits
every effect detached and revalidates the complete authority token under the
gates, and the publisher's `PublishPrecondition::ExactGraphHead`
(`crates/omnigraph/src/db/omnigraph/table_ops.rs`) refuses a stale head
whatever gates were held.

Second, the classification surface is enumerable and closed. The 24
acquisition sites (the permit table above) divide under the rule above, with
read-only open the one exclusive site held by conservatism; the rule is
enforceable in review (a new site must name its permit type), and the typed
permit pair makes the choice visible at the call site instead of implicit in
a mutex.

## Invariants

- One publication door (invariant 2) is untouched: the CAS, its
  preconditions, and publisher OCC are unchanged.
- One coherent accepted view (invariant 3) is what the exclusive side
  protects: shared holders drain before a contract-lifecycle pass swaps the
  accepted view, so no permit holder is in flight across the swap. Two
  readers of the contract stay outside the gate: the write capture
  (`open_write_txn` reads the contract and builds its catalog with no permit,
  safe through the sentinel probe, the trailing schema-state re-read, and
  revalidation under the shared permit at commit) and `resolved_target`,
  which reads the contract with no permit at all.
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

- **Whole-run instrument, as measured.** The concurrent-writes scenario at
  `--writers 8 --write-branches {1,8}`, local FS and RustFS with the 30 ms
  injected-latency recipe, three builds run back to back per cell on
  `b14c22c5`; no baseline was recorded before the change landed. The
  decision log is the record; no bench record lands in the tree. Measured:
  w8×B8 reached 154.0 commits/s locally and 2.13 at +30 ms, 4.9× and 2.4×
  the one-writer rate (31.7 and 0.87, 2026-09-28 entry) against B = 8;
  w8×B1 stayed branch-gate-bound locally (20 to 25 commits/s in every
  build; the +30 ms `main` cell is not recorded); `authority_conflicts` is
  not reported; the critical section did not shrink to
  revalidate-and-publish, since the detached commit stays inside the hold
  (2026-09-28 entry).
- **Existing pins that survive as-is:** the branch-gate exclusivity cell
  (`failpoints.rs`, cross-handle branch-gate serialization), optimize's
  main-gate hold over disjoint tables, and the 16-handle strict-load race in
  `consistency.rs`, which is gate-scope-agnostic by construction.
- **Pins re-derived and added.** The mid-apply blocking test in
  `schema_apply.rs`
  (`mutation_waits_for_mid_apply_schema_gate_then_reprepares`) keeps its
  assertion, a mutation stays pending while an apply is in flight, with a
  comment-only re-derivation to reader-behind-writer. The read-only-open and
  refresh gate-hold tests
  (`read_only_open_holds_schema_gate_through_catalog_capture`,
  `refresh_holds_schema_gate_through_catalog_publication`) are unchanged,
  because both sites stay exclusive. Added: `parked_writer_blocks_schema_apply`,
  the mis-classification tripwire, asserts that the apply does not reach
  `SCHEMA_APPLY_POST_SENTINEL` while a writer holds its shared permit (with
  the writer's permit dropped in `HeldWriteGates::new` it is red);
  `cross_branch_writers_overlap_inside_schema_gate` and
  `read_capture_proceeds_while_writer_parked` pin the two shared-side gains;
  `sibling_branch_creates_exclude_at_the_schema_gate` (`failpoints.rs`) pins
  branch create on the exclusive side, red with both create sites shared;
  `concurrent_reprepare_refresh_blocks_read.gqt` pins that a same-branch
  reprepare no longer parks the process's readers; four write-queue unit
  tests pin the slot, among them the plain-mode fairness pin
  `queued_schema_exclusive_blocks_later_shared`.
- **DST coverage, as shipped.** The plain-mode pin
  `dst_schema_apply_racing_writers_first_contact` (`seam_schedule: false`)
  races two writers against a schema actor on three seeds and asserts, per
  seed, `alternations >= 1` and `era_commits_between_data >= 1`. The fleet
  `dst_concurrent_fleet` gains the plain-mode arms `schema` and
  `schema+maint` (added; their first run is not yet recorded). The scheduler
  run `dst_schema_apply_racing_writers_hunt` is `#[ignore]` and in no
  workflow; strict replay for this arm is the hunt's claim, not a CI pin.
  `dst_seam_scheduler_bite_and_replay` has no schema actor: it pins the
  shared permit's turn/epoch protocol, and the exclusive side under the
  scheduler is pinned only by the ignored hunt.
- **Cost contracts.** The `write_cost.rs` per-op counts (manifest ceiling,
  the schema fence's exact read/exists counts) stay green unchanged; gate
  scope changes overlap, not the common-path operation counts. The write
  capture's park adds one `_refs/branches/` listing per park, on the
  sentinel-standing path only.
- **Not evidenced here:** independent merges overlapping. `branch_merge_impl`
  takes the shared permit, and no instrument or pin in this change runs two
  merges at once; the merge case of RFC 0067's question stays open there.
- **Docs that move in the same change:** the gate-order section of
  `docs/dev/writes.md`, and the read-path sentence in
  `docs/dev/architecture.md`.

## Rollout

One implementation PR after acceptance: lock + permits + classification +
the write capture's park behind an in-flight apply + the branch-create and
reprepare follow-ups + the pins listed under Evidence and tests + the DST
arm + doc updates. The mid-apply pin's re-derivation is comment-only; the
open and refresh pins are unchanged. The instrument runs are recorded in the
decision log, not in the tree. No flag; reversibility is a one-commit
revert.

## Unresolved questions

- Group commit (RFC 0067 path step 3) restructures the same critical section
  at the publisher; it composes with this change (cross-branch arrivals are
  concurrent now; same-branch arrivals still serialize at the branch gate,
  which step 3 must release ahead of the publisher) and is a separate
  proposal. Forcing event: that proposal opening. No decider is named.
- Same-handle reads still wait on publication. A publish on a handle's bound
  branch holds that handle's coordinator lock across the manifest
  compare-and-swap (`commit_updates_on_branch_with_expected`), and a read
  capture takes the same lock. The server shares one handle per graph, so its
  reads on `main` still wait for its writes to `main`. Removing that wait means
  publishing without holding the coordinator lock and installing the new view
  afterwards; it is a separate change. Forcing event: the server's
  same-handle wait on `main` measured as a bottleneck, or the group-commit
  proposal restructuring the publisher, whichever comes first. No decider is
  named.

## Decision log

- 2026-09-18 — Drafted against the detached-commit engine, with the
  concurrent-writes instrument's sequential cross-engine baselines as the
  motivating evidence. Blocked on RFC 0067 merging.
- 2026-09-19 — Implemented. Optimize and cleanup take the shared side (they
  serialize with writers at branch grain: optimize holds main's branch gate,
  cleanup holds every listed branch's gate, and `plan_collection` re-checks
  cleanup's branch listing under those gates and refuses on drift; corrected
  2026-09-29 from "table grain"); read-only open stays exclusive as recorded
  conservatism, and reload takes the exclusive side by the rule, since it
  republishes the accepted view (corrected 2026-09-29). The
  mis-classification tripwire is
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
  `dst_seam_scheduler_bite_and_replay` pins the shared permit's turn/epoch
  protocol (it has no schema actor; the exclusive side under the scheduler
  is pinned only by the ignored hunt).
- 2026-09-28 — Ported onto main after
  [detached-only tables](2026-09-21-detached-only-tables.md) removed
  promotion, which retires the promotion sub-decision outright, and after
  the `omnigraph-core` extraction. The 22 acquisition sites, the classification
  and the DST arm carry over unchanged; the write capture's sentinel probe
  now uses the coordinator's `schema_apply_locked`, a live listing of
  `__manifest`'s `_refs/branches/`, not an in-memory flag.
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
  reader actors whose read-only opens take the exclusive side; their gate
  transitions fall outside the turns `sched_escapes == 0` certifies, so it
  runs without readers now.
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
  each of those losers write a commit that is dead on arrival, 7.6 times the
  table writes locally and 3.9 times at +30 ms (one publication plus 6.6 and
  2.9 failed revalidations), most of them garbage for the collector, to save
  at most 15% of the hold. The per-table gate plan in RFC 0067 would also invert
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
  commit from 60 to 43. Locally it gave nothing: 23.2 commits/s sits inside
  the 20–25 band that interleaved reruns later showed for every build (see
  the attribution entry below). It also widened the local p95 from 232 ms to
  1.9 s, because where revalidation is already cheap, failing faster only
  sends losers back to re-prepare and re-queue sooner. It left the
  starvation of #784 unchanged. The measurement bounds this whole family of
  fixes. Because any publication invalidates every other attempt on the
  branch, eight writers on one branch cannot exceed one writer alone (31.7
  commits/s locally, 0.87 at +30 ms); eight writers reach about 65–80% and
  46% of that. Raising the ceiling requires same-branch writes that do not
  invalidate each other, which is the footprint admission of RFC 0067's group
  commit (step 3), not further work on the gate.
- 2026-09-28 — Attribution, one effect at a time. This change has two
  effects:
  - read-view captures stop waiting behind another handle's publication;
  - publications on different branches overlap.

  A diagnostic build separated them: this change plus one process-wide lock
  around every mutation and load publication, so captures overlap
  publications but publications still serialize. It was never merged.

  The three builds ran back to back in each cell, three runs per cell. The
  comparison does not mix in the copy-on-write catalog publication (#781):
  `main`'s side is `b14c22c5`, which is #781's own merge.

  | Cell | `main` | Diagnostic | This change |
  |---|---|---|---|
  | local, 8 branches | 69.4 | 68.6 | 154.0 |
  | +30 ms, 8 branches | 0.50 | 0.43 | 2.13 |

  The whole eight-branch gain comes from overlapping publications; overlapping
  captures adds nothing measurable. On one branch, locally, two interleaved
  rounds gave, in commits/s:
  - `main`: 24.4 and 20.3;
  - diagnostic: 21.7 and 22.2;
  - this change: 21.7 and 22.8.

  That is no difference. The earlier +36% (18.8 to 25.5) compared runs from
  two sessions, and that cell drifts by about 20% between sessions: it is
  dominated by re-prepares, with 116 to 178 exhausted re-prepare budgets per
  cell in every build. The capture overlap stays a mechanism that
  `read_capture_proceeds_while_writer_parked` pins, not a throughput claim.
- 2026-09-29 — Five changes from the implementation, and one not made.
  (1) Branch create and create-from moved to the exclusive side
  (`branch_create_as`, `branch_create_from_impl`); branch delete stays
  shared. `create_branch_recoverably` lists the refs, checks
  `path_collision`, may reclaim a ref-less tree, then creates, with no CAS
  over that inventory, so two creates with disjoint source and target gates
  both passed it under shared permits. The rule gained its second clause (a
  pass that changes the live branch-ref set with no CAS over the change
  excludes); the seam `BRANCH_CREATE_POST_INVENTORY_PRE_NATIVE` and the pin
  `sibling_branch_creates_exclude_at_the_schema_gate` show it: green on this
  tree, red with both sites put back on the shared side. Cleanup and optimize
  stay shared: cleanup's pre-gate branch listing is re-checked under its
  gates by `plan_collection`, which refuses on drift. (2) The reprepare no
  longer takes the exclusive side. `refresh_for_reprepare` runs
  `refresh_coordinator_only`, then one `inspect_staged_contract` probe, and
  calls `refresh` only on a published staging
  (`StagedContract::Marked { published: true, .. }`); the reprepare loops in
  `mutation.rs` and `loader/mod.rs` call it. Before, every `ReadSetChanged`
  reprepare called `refresh`, which holds the exclusive permit twice, so
  under the write-preferring lock each same-branch reprepare parked every
  new shared acquisition in the process. Pin:
  `concurrent_reprepare_refresh_blocks_read.gqt`, starved at entry `r1` on
  the earlier head, green now. (3) The write capture's park changed shape:
  with a standing sentinel and a free gate, the sentinel is another
  process's apply or a dead one, and the capture returns the typed refusal
  after a second listing confirms it; with a busy gate it parks on the
  shared side and recaptures. Parks no longer count against
  `MAX_CAPTURE_RETRIES`. This replaces the earlier shape, eight probes per
  cross-process apply and a `manifest_read_set_changed` when the sentinel
  cleared during the last park. (4) `upgrade_system_columns` under
  `options.check` takes the shared permit, exclusive otherwise.
  (5) `parked_writer_blocks_schema_apply` now asserts that
  `SCHEMA_APPLY_POST_SENTINEL` is not reached while the writer is parked;
  with the writer's permit dropped in `HeldWriteGates::new` it is red, where
  the earlier assertion stayed green. Not made: a debug assertion for
  non-reentrancy was tried and removed, because `tokio::task::try_id()`
  gives no identity under `block_on` and task identity is not call-path
  identity (`join!` inside one task would trip it); the rule stays
  documented on `SchemaGateSlot`. Body sentences this entry supersedes: the
  Summary's "Promotion, already correct with no gate held, moves after guard
  release as a separate, independently revertible sub-decision" and "The
  per-table gates stay: they keep two same-table stagers from wasting one
  staging"; the Motivation's "held from revalidation through promotion" (now
  "through publication") and "Shrinking the serialization to the branch is
  what lets a same-branch batch form at all"; the section "Promotion outside
  the guards", cut whole (the `HeldWriteGates` helper is now named under The
  lock); the shared list's "branch create/create-from/delete"; the Evidence
  bullet that promised the open and refresh pins "split along the new
  classification"; and invariant 3's "swaps the accepted view with no reader
  or writer in flight". The 2026-09-19 entry's grain and conservatism
  clauses are corrected in place and marked.


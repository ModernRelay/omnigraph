---
rfc: "2026-11-10-writes-carried-over-row-preserving-publications"
title: "Writes carried over row-preserving publications"
track: maintainer
status: draft
implementation: in-progress
authors:
  - ragnorc
created: 2026-11-10
updated: 2026-11-10
discussion: https://github.com/ModernRelay/omnigraph/issues/1331
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Writes carried over row-preserving publications

> A term set in ***bold italics*** is being defined at that exact spot.

## Summary

A mutation, a load or a Blob write prepares outside the writer gates and captures its branch's whole write authority. Under the gates it compares that authority with the current one, and today any publication in between sends it back: an insert or a load prepares again, an update, a delete or an overwrite load returns `ReadSetChanged` (HTTP 409). An optimize run's publication counts, although compaction changes no row.

This RFC makes a write ***carry*** over publications that changed no row. When every table pin that moved since the capture reaches the captured pin through `Rewrite` and `CreateIndex` commits alone, and the schema contract, the table set and every registration are unchanged, the captured authority still describes the current graph exactly. The write is then re-anchored on the current head before its first effect. A table it only read keeps its moved pin. A table it staged is carried over that pin's commits by Lance's own conflict resolver, `TransactionRebase`, the pass Lance runs for every linear commit; the staged transaction then records the table's current pin as the version it read, and commits detached there like any other write. Where Lance refuses (an update or a delete of a fragment a rewrite replaced), the write refuses exactly as it does today.

Unchanged: the publisher, its exact pin CAS and `ExactGraphHead`; the one manifest publication; the collector; the storage format; a caller's `--if-commit` head, which stays exact; unattached writes, which stay strict.

## Motivation

Issue #1331 measured the cost on RustFS (S3) with back-to-back optimize runs and five-row loads. With one writer, 84 of 85 writes during optimize prepared again, and throughput fell from 535 to 85 writes in 90 s, because the writer moved in lock-step with the runs. With two writers, write p99 went from 0.58 s to 12.8 s and the slowest write waited 14 s across 9 runs. On a local root the effect is invisible, since preparing takes milliseconds there.

Under invariant 7, compaction and index builds are derived physical state that "may change cost but not correctness". The write path contradicts it: a physical rewrite invalidates every overlapping logical write. Lance itself does not: its transaction specification makes `Append` compatible with `Rewrite`, and `Update` and `Delete` conflict with `Rewrite` "only if overlapping fragments", as a retryable conflict. Delta Lake (`dataChange = false` on `OPTIMIZE`) and Iceberg (`rewrite_data_files`) follow the same rule. Omnigraph is stricter than its substrate because it never asks the substrate: every graph-table write is a detached commit, which runs no conflict pass, and the graph-level check compares the whole authority token.

The served-optimize change (PR #1282) bounds the symptom with a writer yield: an optimize run waits up to `max_writer_yield` (10 s) for writes that started before the previous optimize publication. It keeps the documented bound, but every overlapping writer still prepares again once, and under steady writes with two or more writers most runs wait the full 10 s. This RFC removes the cause, after which the yield has nothing to wait for.

## User and operational behavior

| Write overlapping a publication that changed no row | Before | After |
|---|---|---|
| Mutation `insert`, load `append` | prepares again from the start | commits; nothing prepared again |
| Load `merge`, Blob write | prepares again from the start | commits, unless it updates a row in a fragment a rewrite replaced: then it prepares again, as before |
| Mutation `update`/`delete` on tables the publication did not move | `ReadSetChanged` (409) | commits |
| Mutation `update`/`delete` of rows in fragments the publication did not rewrite | `ReadSetChanged` (409) | commits |
| Mutation `update`/`delete` of rows in a fragment a rewrite replaced | `ReadSetChanged` (409) | `ReadSetChanged` (409), unchanged |
| Load `overwrite` | `ReadSetChanged` (409) | commits |
| Write with a caller head (`--if-commit`, `expected_head`) | `PreconditionFailed` (412) | `PreconditionFailed` (412), unchanged |
| Child unattached write (`as unattached branch on …`) | refuses | refuses, unchanged |

A publication ***changes no row*** when every table pin it moved reaches the previous pin through commits whose Lance operation is `Rewrite` or `CreateIndex`, and it changed neither the schema contract nor any registration's identity, location, native ref or row count. Optimize compaction, deletion materialization, dropped-column erasure, index folds and builds, full-text rebuilds and full-text declarations all publish such commits. The rule reads what the commits record, not who published them, so a later maintenance operation qualifies by the same proof without a code change, and any other commit, a row change, a schema change, a merge that moved a table, disqualifies.

The carry covers the publications revalidation sees under the gates. A write on its handle's bound branch probes the head beside its staging and, when nothing installed on the handle since, trusts that probe under the gates; a publication by another handle or process after the probe is then refused by the publisher's CAS, after the write's detached commits, and the write prepares again as today. A server's optimize runs on the handle its writes use, so its publication always reaches revalidation.

Any publication that changes a row still sends the write back as before. Several row-preserving publications in a row carry as one, up to 32 commits per table (`ROW_PRESERVING_CHAIN_MAX_LINKS`); past that the write prepares again.

The write's graph commit is parented on the head it was carried to, so lineage shows the optimize commit, then the write. The committed table keeps the compaction: its new pin is a detached commit that read the compacted version.

The engine's query cost probe counts carried writes (`QueryIoProbes::writes_carried_over_row_preserving`) beside the existing `mutation_reprepares`.

## Design

### The proof, from durable state

Under the writer gates, revalidation reads the current authority as it does today. If the branch identifier changed, nothing changes: the write refuses. If the head moved, the strict path refuses; a carrying writer instead checks, in order:

1. the schema IR hash, identity domain and identity version equal the captured ones;
2. the current snapshot registers the same table keys, each with the same table identity, dataset path and native dataset branch;
3. every table whose pin `(published_dataset_version, staged_version)` differs has the same `entity_count` and a version that did not go down;
4. for every such table, `row_preserving_chain` opens the current pin and walks its commits by their `read_version` links, reading each transaction, until a link reads the captured pin. Each link must be a `Rewrite` or a `CreateIndex`. The walk stops after 32 links.

Any failure returns the error the strict path returns (`ReadSetChanged` on `graph_head:<branch>`), so a refusal is indistinguishable from today's. The proof compares only the two endpoints: what matters is that the current graph equals the captured one, not the route between them.

Every input is durable `__manifest` state and Lance transaction files. Nothing records "this was optimize": a flag on the graph commit would be a format change that proves only who published, not what changed, and it would have to be kept true by every future maintenance writer.

### Re-anchoring before effects

The write's transaction is cloned with the current snapshot as its base, the current head as its authority and effective head, and the current manifest probe. The catalog, branch, caller head, policy decision and staged data are kept: the schema is the same, so the catalog is too. The staging witness every detached commit records is taken from the re-anchored authority, so it names the head the publication will compare against.

For each touched table whose pin moved:

- **Read, not staged.** Nothing to do. Validation that read it (endpoints, uniqueness, cardinality) read rows the current pin still holds.
- **Staged.** The staged transaction goes through `TransactionRebase::try_new` on the captured pin, `check_txn` for every link, then `finish` on the current pin; its `read_version` becomes the current pin's version, and it commits detached there. The table's expected version becomes the current one, and its update's `published_dataset_version` is that plus one, as for any write.
- **Touched with nothing staged** (a delete that matched no row). Its expectation becomes the current pin.

`check_txn` is where the operation matters. An insert-only `Update` (a strict insert or the inserting arm of a merge) and an `Append` are compatible with `Rewrite` and `CreateIndex`. An `Update` or `Delete` whose modified fragments a `Rewrite` replaced is a retryable conflict, which this design returns as today's `ReadSetChanged`. An `Overwrite` is compatible with both. For these operations against these links `finish` returns the transaction unchanged; it writes nothing, so a refusal leaves no effect. Lance assigns fragment ids and stable row ids when it builds the manifest from the version a commit reads, so rows inserted over a compaction take ids from the compacted version, which kept the row-id high-water mark.

### Publication, lineage and the collector

The publisher receives an ordinary write: `ExactGraphHead` on the current head and an exact pin expectation on each touched table's current pin. It needs no change. A publication that lands between the carry and the publish still loses the CAS, as any stale write does.

Lineage is the publisher's: the write's graph commit is parented on the current head, the maintenance commit.

Every table's history stays one chain of `read_version` links: the compaction reads the captured pin, the write reads the compaction. The collector's chain walk, the pure-insert merge proof and the change feed's transaction footprints follow it as they follow any write. A detached commit made under the carried authority and never published is judged by its witness, the current head, like any other abandoned stage.

## Invariants

1. **Respect the substrate.** The compatibility rule is Lance's: the decision is `TransactionRebase::check_txn`, the function `commit_transaction` calls, not an Omnigraph table of operations. Omnigraph adds only the graph-level proof that no table's rows changed. A Lance release that changes a rule changes this behavior with it; `lance_surface_guards.rs` pins the cases the engine relies on.
2. **One publication door.** Unchanged. The carry happens before the first detached commit; the write still publishes once through the publisher, and a table's linear HEAD never moves.
3. **One coherent accepted view.** The write combines facts captured at one head with a publication at another, which this invariant forbids unless the two describe the same graph. The proof establishes exactly that from durable state: same schema contract, same tables and registrations, same rows in every table. The refined reading is that a retry starts fresh unless the authority it would start from is provably the logical equal of the one it holds. Every input to the proof is read under the gates in one revalidation.
4. **A mutation publishes once.** Unchanged.
5. **Crash convergence.** Unchanged. Before the carry nothing durable exists; after it the write's detached commits are unreachable until the publication names them, with a witness the collector can judge. `detached_commit_matrix.rs` runs the carried writer through every window.
6. **Stable identity.** The proof compares table identities, never names or paths alone, and a schema change disqualifies.
7. **Physical acceleration is derived state.** This RFC is what makes it true for writers: compaction and index builds no longer change a write's outcome.
8. **Integrity failures are loud.** A conflict Lance reports is the typed `ReadSetChanged` it is today. Nothing is weakened: validation ran against rows the current pins still hold.
9. **Query semantics are typed.** Not affected.
10. **Trust and policy.** The policy decision was made for the actor and branch before preparing; neither changes.
11. **Bounded and observable.** The walk reads at most 32 transactions per moved table, each from its version's manifest; it is counted and traced. A refusal costs what it costs today plus the walk.
12. **One source of truth.** No new record. The proof is derived from `__manifest` and Lance transactions every time.

Deny-list: no custom transaction manager (the conflict rules are Lance's), no process-local authority (the proof reads storage), no silent retry (a carry is an exact equivalence, not a retry, and a conflict is reported).

## Compatibility and reversibility

No format change. A carried write's detached commit is an ordinary Lance transaction whose `read_version` is the table's current pin; every reader and older binary reads it. Reverting the change restores the strict comparison and leaves no residue.

## Alternatives

### A. Carry only when the staged tables did not move

Accept the moved head when every moved table is one the write only read; refuse otherwise. No rebase. It fixes writes to other tables, and nothing else: writes add the small fragments that optimize compacts, so the usual case is a write to the table optimize just compacted. This design contains A as its read-only case.

### B. Maintenance loses (the issue's first direction)

The write's detached version, built on the pre-compaction pin `V`, replaces the compacted pin `V′`: the publisher's exact CAS would accept "expected `V`, found a maintenance-only descendant of `V`", and the compaction of that table is discarded. Reading the code rules it out:

- **Compaction can starve.** The tables that take steady writes are the ones that accumulate small fragments, and with steady writes some write is always in flight when an optimize run publishes. That write throws the table's compaction away, the next run redoes it, and the next in-flight write throws it away again. Reads on the hottest tables keep paying for small fragments, which is invariant 11's unbounded cost in the place it hurts most.
- **The pin CAS gets weaker.** The publisher's exact table expectation is the one check that no other publication moved a table under a write. B turns it into a judgment the publisher must make about another publication's commits, on every attempt, inside the CAS loop.
- **Table histories fork.** The write's version reads `V` while the pin before it is `V′`. Every consumer that follows `read_version` links from a pin to the previous one then stops being able to: the pure-insert merge proof walks from a source pin to the base pin and would miss it, falling back to the general diff, and change discovery would meet a pin whose transaction does not read the pin before it.

### C. Rebase in the engine (the issue's second direction)

Write an Omnigraph conflict pass between a staged write and the commits behind a moved pin. This design is that direction with the conflict pass taken from Lance instead of written. The issue's concern was "resolving conflicts between detached commits, which the current protocol avoids on purpose". No detached commit is rebased here: the carry runs before the write's first detached commit, on a transaction that has not been committed, and the result records the version it read like every staged commit does.

### D. Mark maintenance commits

Record on each graph commit whether it was maintenance, and accept a moved head when every commit since the capture carries the mark. It needs a format change, it trusts the publisher's claim instead of checking the tables, and it must be kept true by every future maintenance path. The structural proof needs none of that and accepts exactly the publications that changed no row.

### E. Keep the writer yield

PR #1282's yield bounds the wait but keeps one re-preparation per overlapping write and makes optimize wait up to 10 s under steady writes. It remains correct after this change; its tickets, epoch and `max_writer_yield` become removable once both land.

## Evidence and tests

- `crates/omnigraph/tests/failpoints.rs`, the owner of pre-effect authority races: each `issue_1331` test parks a write after staging (`mutation.post_stage_pre_effect_gate`), runs optimize, and counts `mutation_reprepares` and carries. An insert beside a compaction of another table commits without preparing again; an update beside it succeeds; an insert into the compacted table commits without preparing again and its detached commit reads the compacted version, so the compaction stays; an update of a rewritten fragment still refuses; a row change after the optimize still sends the insert back; append and overwrite loads commit without preparing again.
- `crates/omnigraph/tests/detached_commit_matrix.rs`: a carried insert, through every window and recovery actor of the existing writer matrix.
- The DST collector oracle over a run that carries writes.
- `crates/omnigraph-dst/cost_table.txt`: the storage calls of a carried insert, against the calls of the same insert with nothing to carry.
- Lance surveyed: the transaction specification (conflict resolution, Rewrite, Update, Delete, Append), versioning, branch and tag, row-ID lineage, and the read/write guide; `lance-13.0.0` `io/commit/conflict_resolver.rs` (`TransactionRebase`), `io/commit.rs` (`commit_transaction`, `do_commit_detached_transaction`) and `lance-table` `transaction/manifest_build.rs` (fragment and row-id assignment at build).

## Rollout

One change: the proof, the carry and its tests land together, behind no setting. The writer yield of PR #1282 can be removed afterwards.

## Unresolved questions

- Whether an update or delete that Lance refuses over an overlapping rewrite should prepare again automatically instead of returning 409. Today's contract keeps strict read-modify-write writes caller-visible; this RFC does not change it.

## Decision log

- 2026-11-10: drafted for issue #1331. Chose Lance's resolver before effects over "maintenance loses" (compaction starvation, a weaker pin CAS, forked table histories) and over a maintenance flag (format change, claims instead of proof).

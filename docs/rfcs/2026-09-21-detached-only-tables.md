---
rfc: "2026-09-21-detached-only-tables"
title: "Detached-only tables"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - azimafroozeh
created: 2026-09-21
updated: 2026-09-25
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Inventory: the everything-blocked run. Under a `no-promotion` cargo feature on `omnigraph-engine`, where `promote_pin` returns the blocked outcome at every call site and `cleanup_all_datasets` sets `deferred` on every graph table, run `cargo test -p omnigraph-gqt`, `cargo test -p omnigraph-engine --features failpoints,no-promotion`, the DST scenarios, and the change-feed and merge cost owners. A red test is a dependency on the linear chain unless it asserts promotion itself; those are listed separately in this RFC as deleted with the promoters."
  - "Storage: the tracing collector with its DST oracle, two predicates over a run's inputs. Safety: for every `__manifest` version the run's `--keep` and `--older-than` policy retains on every live branch, every `staged_version` it registers opens and every path its manifest lists exists. Progress: every manifest in the published set that no retained `__manifest` version names is absent after a run with `--older-than 0`. Fixtures: a pin published then pruned by `--keep 1`; an unpublished staging left by the mutation pre-publication failpoint window; a merge whose chain has three chunks with only the tip pinned; a fork branch created after a pin, with the pin pruned on main and retained on the branch; a publication that lands between the collector's scan of a branch and its sweep; a repeat run after a pass that already reclaimed a chain tip; a crash inside a pass followed by a retry of the same run. Crash windows inside the sweep, on file, S3 and Azure."
  - "Change discovery: the replacement for the row-lineage stamp windows chosen from measured candidates, with the change-feed cost owner `tests/changes_cost.rs` and a merge cost owner still to be created, both flat against today's promoted path."
  - "Cost: `write_cost.rs` and `write_cost_s3` ceilings for a write without promotion, on a warm and on a cold handle, checked in with the thresholds under Evidence and tests."
  - "Compatibility: the format stamp, provisionally v11 until its order against RFC 0068 is decided, resolution of v10 rows under it, the refusal fence for older binaries, and the `cleanup` consumer fixture over the JSON result and `--keep` on a v10 and a v11 graph."
---

# RFC: Detached-only tables

> A term set in ***bold italics*** is being defined at that exact spot.

**Depends on:** [RFC 0067](0067-detached-table-commits.md) for detached
staging, the registration row's table pointer, the single `__manifest`
publication and every content writer's staging shape. This RFC removes one
part of it, promotion, and replaces what promotion was buying.
**Surveyed:** OmniGraph `main` at `ed145db4` (RFC 0067 as implemented);
RFC 0067's probe table, excluding probe 14, and its Decision log; draft
[RFC 0068](0068-graph-commit-record.md).

## Summary

Under RFC 0067 a write stages each table effect as a ***detached commit***,
a Lance commit written as `_versions/d{id}.manifest` that does not move the
table's linear HEAD, publishes every ***pin***, the table pointer a
`__manifest` registration row carries, with one `__manifest` publication,
and then ***promotes*** each pin: it replays the recorded transaction onto
the table's linear history so that a linear twin of the detached version
exists at `target_version`.

This RFC stops after the publication. A table effect stays a detached commit
for its whole life. Readers open the pin's `staged_version` when it carries
one. No writer makes a linear commit on a graph table again, except the `v1`
create of a new table, which Lance cannot stage detached. An external Lance
reader of the table directory sees its last linear version (Design, Pins),
never current rows; `omnigraph export`, which writes JSONL, is the external
route.

Three things replace what promotion provided:

1. Stock `cleanup_old_versions` is no longer called on graph tables. That
   removes two callers: `cleanup` and schema apply's hard-drop reclaim
   (`cleanup_dataset_old_versions`). `cleanup` becomes a tracing collector
   whose roots, as Design defines them, are the pins of every retained
   `__manifest` version on every live graph branch, and which deletes
   manifests, and their files, from two populations: those once published and
   no longer retained, and those never published and provably dead under the
   staging witness of Garbage collection.
2. The row-lineage stamp windows that the change feed and merge read are
   replaced, because a detached commit's stamps are not monotonic. The
   replacement is the change set the commit itself carries: its transaction
   file for inserts and updates, a transaction property for deletes, the
   exact path as fallback. Measured before acceptance; see Change discovery.
3. `published_dataset_version` keeps its number and its meaning as the
   table's logical version, `base + 1`, but no Lance version occupies it.

What is deleted: the promotion reconciler, the rule for which transaction
kinds may be staged, the twin proof and the detached-manifest reaper built
on it, GC deferral, the blocked outcome and its `repair` reporting, the
refusal of linear-HEAD writers behind a blocked pin, and the creation of
per-table native forks at a branch's first write, which the probe in
Unresolved questions may keep.

What does not change: detached staging, the three-field pin on the wire of
a registration row, the one publication door, lost-acknowledgement
resolution, query and merge semantics, policy enforcement, every HTTP
request and response, and every CLI command but `cleanup`, whose JSON result
gains fields and whose `--keep` changes meaning at the stamp (User and
operational behavior).

## Motivation

### What promotion was chosen for

RFC 0067's first draft pinned detached versions permanently. Its Decision
log records the revision of 2026-09-14: promotion by transaction replay
replaced permanent pins because it "removes the OmniGraph-owned garbage
collector and the change-discovery rewrite from the gates". Its Alternatives
section, before its amendment of 2026-09-21, named a third cost, a `Restore`
bridge for compaction; the amendment withdraws it.

### What the implementation showed

The implementation in `main` paid most of that cost anyway.

- **Garbage collection did not stay with Lance.** Stock
  `cleanup_old_versions` deletes data files that only a detached manifest
  references (RFC 0067 probe 3). So RFC 0067 already owns a detached-manifest
  reaper, proves each reclaim against the linear twin's transaction uuid,
  defers a table's stock GC while any pending or unproven detached manifest
  is retained, and accepts "indefinite retention of unproven abandoned
  staging". OmniGraph decides what is deleted today; it decides it through
  two collectors and a proof that couples them.
- **The `Restore` bridge is closed for both designs.** Compaction ships as
  one detached `Rewrite` (RFC 0067's Optimize row and its failure-matrix
  finding).
- **Promotion added a failure state that nothing resolves.** A foreign
  linear commit at a pin's `target_version` blocks that pin and every later
  pin on the table. RFC 0067's Unresolved questions say the block is
  "reported by `repair` and resolved by nothing". Behind a blocked pin,
  branch merge, index build, schema apply, the system-column upgrade,
  Optimize and a first-touch fork are refused.
- **Promotion constrains what may be staged.** A transaction kind may be its
  own detached commit only if its replay conflicts with its own twin. A bare
  `Append`, a `ReserveFragments` and a delete-only `UpdateConfig` do not, so
  each needed a design exception.
- **Promotion is on the write path.** Per write it is N further commits, one
  per touched table, each costing the three requests of a linear commit (RFC
  0067's Promotion section), and a writer after a crashed predecessor pays
  that predecessor's promotion. RFC 0067's own S3 measurement puts promotion
  at two further writes on a warm handle; the instrument that replaces both
  numbers is the cost gate in `blocked_on`, with its thresholds under
  Evidence and tests.

The remaining gate is change discovery. This RFC's position is that one
rewrite of change discovery costs less to own than the reconciler, the twin
proof, the blocked state and the staging rule together, and that draft RFC
0068 already sketches where change discovery should live.

### Why now

Draft RFC 0068 keeps promotion and makes it lazy: its write path is
"staging, one detached commit per touched table in parallel, and one
conditional create". That is this RFC's write path. Deciding promotion's
fate before RFC 0068 is accepted avoids carrying the reconciler into a
second format.

## User and operational behavior

- **Writes.** Same results, same errors. A write is N detached commits and
  one publication. No table effect has post-publication work; a schema apply
  still installs its contract files after its publication, which this RFC
  leaves untouched (Invariants, 5).
- **`repair`.** The blocked outcome no longer exists. On a graph table
  `repair` reports `no_drift` when the linear HEAD equals that table's
  last linear version (Design, Pins), and `foreign_drift` otherwise.
  `foreign_drift` prints that version and the HEAD, is never published, with
  or without `--confirm --force`, and does not fail the command.
  `verified_maintenance`, `suspicious` and `unverifiable` no longer
  apply to graph tables, and a linear head below `published_dataset_version`
  is the normal state of every table rather than `manifest_internal`. Drift
  does not affect reads or writes, because no read or write resolves a
  table's linear HEAD; only `repair` and the v10-row rule of Format and
  migration still read it.
- **`cleanup`.** Deletes manifests, and their files, from two populations:
  those that were published and are no longer retained, and those that were
  never published and are dead under the staging rule of Garbage collection.
  It never promotes and never defers a table.
  `--keep N` retains the newest N `__manifest` versions per live branch and
  `--older-than` moves that per-branch cutoff by time; neither is a second
  filter on table manifests. On a v10 graph `--keep N` keeps the meaning it
  has today, the newest N Lance versions of each dataset and not graph
  commits (`docs/user/operations/maintenance.md`); on a v11 graph it counts
  `__manifest` versions. The same command therefore retains something
  different after the stamp, a behavior change at the stamp rather than at the
  binary upgrade. Exit 0 means every table was visited and that
  nothing a retained `__manifest` version pins, and no file such a manifest
  references, was deleted. Per table the result row carries
  `manifests_removed`, `bytes_removed`, `unpublished_manifests`,
  `unpublished_bytes` and `foreign_versions`, and `error` is set only when
  that table's collection stopped early. The JSON result stays additive for
  one release after v11 ships: `old_versions_removed` remains beside
  `manifests_removed` and carries the manifests removed, `deferred` remains
  and is always false on a v11 graph, and the four new fields are added
  beside them. Both are removed at the next release boundary, which is the
  affected boundary and the consumer migration that
  `docs/dev/versioning.md` requires of an incompatible wire change.
- **External readers.** A Lance reader pointed at a table directory is not
  refused: it opens the table's last linear version, defined in Design, and
  reads stale rows silently. A table created under v11 shows `v1`, which is
  empty. `omnigraph export` is the only supported route, and it is JSONL of
  one branch's current rows, with no history and no Lance files; no
  Lance-format export exists. This is a behavior change: under RFC 0067 a
  promoted table is current for any Lance 4.0.0+ reader.
- **Auto-cleanup.** A detached commit never triggers Lance auto-cleanup:
  Lance 11 runs `auto_cleanup_hook` only from `commit_transaction`, never
  from `do_commit_detached_transaction`, and every engine commit passes
  `skip_auto_cleanup: true` (`table_store.rs`). No configuration strip is
  required, and a surface guard pins the hook site.

## Design

### Pins

The registration row is unchanged: `published_dataset_version`,
`staged_version`, `transaction_uuid`. `published_dataset_version` is
assigned as today, `base + 1`. It orders a table's versions inside one
`__manifest` lineage, so the manifest fold, the RFC 0062 registration clock,
expected-version checks and every numeric comparison on table versions stay
as they are. It stops naming a Lance version.

A table's ***last linear version*** is the highest linear version whose
transaction uuid matches a pin some `__manifest` row named, or `1` for a
table created after the stamp. It is recorded per registration row, under
the key `omnigraph.last_linear_version` in the row's version metadata map
beside `omnigraph.staged_version` and `omnigraph.transaction_uuid`, so its
scope is one `(dataset path, native_dataset_branch)` pair and a legacy fork
keeps its own value. The v11 upgrade writes it for every row on every live
branch, and `from_dataset` and every `with_staged` caller copy it forward.
That carry-forward is owed as evidence: `TableVersionMetadata`
(`db/manifest/metadata.rs`) is rebuilt at every publication, so every writer
that rebuilds a row must preserve the key.

### Resolution

`DatasetEntry::open` on a row that carries `staged_version` opens that
detached manifest, and only that one, and checks that its transaction file
name carries `transaction_uuid`. The order today, which opens
`target_version` first and probes `Latest` when it is missing
(`instrumentation.rs`), flips: under v11 that version is absent or foreign
for every row above the last linear version, so opening it first would buy
a failed read on every open. A missing detached manifest is reclaimed
history and fails with `HistoricalVersionReclaimed` naming the
`staged_version` id, unless the row's `published_dataset_version` is at or
below the last linear version, which takes the v10 path below. A row that
records no last linear version is older than the v11 upgrade (a historical
registration, or a current one the upgrade has not reached) and takes the
v10 path too, so a pin whose copy the upgrade reaped still resolves from
its twin; a table created after the stamp records `1` at creation, so none
of its rows is ever in that state. A row without `staged_version` is a
linear pin at or below the last linear version and opens as it does today.

The e-tag and the transaction uuid stay the identity witnesses. The rule
that a staged manifest "must not resurrect pruned history" holds by
construction: a manifest the collector deleted cannot be opened. The
read-handle cache keys `(path, native branch, logical version, staged
version, e-tag)`. The in-memory topology key and persisted topology stamp
also carry `staged_version`: two branches can share the logical counter and
native ref while pinning different detached commits. This identity remains
exact when a backend supplies no e-tag, including Windows local files.
Legacy linear pins retain their logical version with no staged version.

### Content writers

Every row of RFC 0067's Content writers table that ends in a promotion holds
with that step removed: a writer stages detached from the pin's detached
version and publishes. Branch merge chunks chain detached and the published
tip is the merge's table version: the whole chain publishes one
`published_dataset_version`, `base + 1`, every chunk writes that value into
the version columns, and the chain length is no longer added. Optimize plans
on the pin, stages its `Rewrite` and any chained index commits detached, and
publishes with the exact pin CAS. New-type creation stays a linear `v1`
create, and branch create and delete are already unchanged under RFC 0067.

The rule for what may be staged as its own detached commit is deleted with
promotion. A writer may stage any transaction kind Lance accepts detached.

On a publication the object store refused, a `ReadSetChanged` refusal of the
compare-and-swap rather than a timeout or an unavailable readback, the
writer deletes its own detached manifests before it reprepares. On an
in-doubt outcome it deletes nothing, because the publication may have landed.
Orphaned staging therefore has three sources: a crash, an in-doubt outcome,
including a request that ended on a timeout with the process alive, whose
publication never landed, and a delete that itself failed after a confirmed
refusal. Only a confirmed refusal whose deletes all succeeded leaves nothing
behind. Garbage collection reclaims the rest.

### Garbage collection

`cleanup` never calls `cleanup_old_versions` on a graph table. A run first
selects retained `__manifest` versions per live branch: `--keep N` retains the newest
N `__manifest` versions of each live branch, and `--older-than` moves that
per-branch cutoff by time. A graph branch also retains its oldest
`__manifest` version, the copy of the parent's version it was created from,
while the branch lives. The run also roots graph snapshots and their table pins
protected by native tags, exact tagged table versions (including detached and
main versions), and the selected merge base of every pair of live heads or
protected merge inputs.
Merge and cleanup share the same lazy lineage-import order and deterministic
base selection. Exact commit identity selects a historical native manifest;
a recreated logical name cannot substitute its same-numbered version.
Physical pruning of live `__manifest` histories is separate retention work.

Before capturing graph roots, cleanup freezes native tree deletion candidates
and captures native tags. It derives each live branch's registrations, lineage,
logical head and retained versions from one pinned `__manifest` dataset.
After all table inventories, it validates the complete branch set, every
captured version and incarnation, and every graph and table tag's name and
contents. A changed observation refuses the whole plan before deletion.
Later-created native trees cannot enter that frozen deletion inventory. Publications after the captured
view are invisible to its published set and are judged as staging below.

The run traces and sweeps every discovered table lifetime, including dropped
tables and an older identity whose public name was re-added. Per location it
traces in this order:

1. ***Published set***, the detached versions that any `__manifest` version
   still on the branch ever named. Collect `staged_version` from every
   registration row of the table in every such version, on every live graph
   branch; the rows are append-only and `read_manifest_entries` returns the
   historical registrations beside the current one (`db/manifest/state.rs`),
   so the set is read once from that snapshot's HEAD and includes the
   ids of manifests an earlier run already reclaimed. The walk and the sweep
   run only over the detached manifests the table's `_versions/` listing
   still shows (`list_detached_manifests`; today's reaper filters the
   historical entries against that same listing before it walks,
   `db/omnigraph/optimize.rs`). For each member the listing shows, follow
   `read_version` links back until another member of the set, a linear
   version, or an absent predecessor is reached; the links walked are the
   unpinned chunks of a merge chain and join the set. An absent non-root tip
   or predecessor is a manifest an earlier pass reclaimed: it ends that walk
   and is not an error, which is what makes a second run over the same
   historical ids discover the garbage the first run left. A root the
   listing does not show is an error the run reports for that table.
2. ***Roots*** are the table pins of retained graph snapshots and selected
   merge bases, plus exact versions retained by native table tags. A table
   tag protects its version even when no graph commit published it. Graph
   snapshots are opened at their exact native branch and version, without
   resolving a recreated logical name.
3. **Mark.** Open each root manifest and mark every base and overlay data
   file from `referenced_lance_files()`, deletion files, index directories
   (`load_indices()`, one extra read per root) and transaction file. A Lance
   manifest lists every fragment of its version (`lance-table`,
   `Manifest.fragments`), so marking needs no chain walk.
4. **Sweep.** For each manifest in the published set that is not a root,
   delete first every file it references that is unmarked and that no
   manifest outside the published set references, and then the manifest
   itself. A member of the set the listing no longer shows was swept by an
   earlier pass and is skipped. A pass that stops early leaves a non-root
   manifest the next pass sweeps again, and strands no file. The swept objects include that
   manifest's `_transactions/{read_version}-{uuid}.txn` file and the
   `_indices/{uuid}/` directories nothing else references, both of which
   stock cleanup used to delete. The test against manifests outside the
   published set is bounded by caching the file set of each frozen linear
   manifest, which cannot change. RFC 0067's reaper order, a chain's links
   oldest first and its tip last, is unchanged.
5. **Linear versions.** The last linear version is a permanent root. A
   linear version below it is a root while a retained graph snapshot or native
   tag pins it, and follows the same sweep rule otherwise. A linear version
   above it is foreign: the collector deletes neither its manifest nor its
   files, and the run reports it per table as `foreign_versions`.

A detached manifest outside the published set is unpublished staging: an
abandoned write or one in flight. One rule bounds how long it is kept, and it
compares what the publisher itself compares. A publication is a
compare-and-swap on the graph branch incarnation and the logical graph head
the writer captured when it staged: `db/omnigraph/table_ops.rs` captures both
into the publication precondition, and `db/manifest/publisher.rs` checks both
on every attempt before it touches table pins. It is not a swap on a physical
`__manifest` version. `compact_internal_table` advances a branch's physical
`__manifest` HEAD without moving either value (`db/omnigraph/optimize.rs`),
so a writer staged at physical version 20 and head H can still publish after
compaction produces physical 21 at the same head. Every detached commit
therefore records that authority in its transaction properties, under the keys
`omnigraph.staged_against_branch_incarnation` and
`omnigraph.staged_against_graph_head`, and the collector reads its owner from
those keys. It never infers a branch from the table location, which two graph
branches now share, nor from a branch name, which a later branch can reuse.

A staging is dead when either of two proofs holds. First, its canonical
recorded incarnation was not in the captured live views and is absent from
a complete live-identity inventory taken after that table's staged-manifest
inventory. Final branch/head/tag validation rejects an overlapping publication
or recreation that could have adopted the staging. Second, the incarnation
is live, its logical head in the original capture is strictly above the
recorded head, and no version through that capture names the staging. The
second inventory never replaces a captured live head. Equal heads retain the
staging. Missing, malformed or otherwise undecidable ownership retains it.

Retirement writes exact `BranchContents` to an immutable archive inside the
native tree before removing the active ref. A retry validates the archive
before unlinking; older retired refs are archived during cleanup. Normal
creation therefore reads no retired history. Cleanup retains needed native
ancestors and exact historical merge-base providers, then removes unneeded
trees together with their archives. Delayed staging does not require permanent
retirement history: its valid identity can be judged by the post-inventory cut.

A dead staging can never be published: its compare-and-swap is against an
authority the branch has left, and its writer restages rather than reusing it.
That restaging is narrower than the rule. `mutate` refreshes and reprepares
only an insert-only mutation after a pre-effect `ReadSetChanged`; an update or
a delete returns the conflict to the caller instead of replaying a stale
read-modify-write plan (`exec/mutation.rs`). The proof does not rest on which
of the two happens, because neither publishes the staging the collector
reclaims.

The staging rule retains ambiguous effects; reclamation progress is conditional.
On a branch that keeps publishing,
the rule bounds retention by the in-flight window. On an idle branch it does
not: a crashed writer's staging stays until a later publication moves that
branch's head, and if nothing publishes again, it stays. Bounded reclamation
there needs a protocol that invalidates a stale publication authority without
a later publication, which is later work. This narrows RFC 0067's position
("Durable writer fencing, rather than a larger timeout, is the prerequisite
for reclaiming that state") to the moving case: there the dead authority is
the proof, and no fence is needed.

An object inventory follows the manifest trace. Blob sidecars under
`data/<stem>/*.blob` inherit reachability from `data/<stem>.lance`; when that
parent is swept, the plan includes the actual sidecar objects. Files that
no listed manifest references are reclaimed only when each object's own age
exceeds `UNVERIFIED_THRESHOLD_DAYS`, seven days. This includes files staged
before their manifest and `_versions/.tmp*` temporary manifests. An aged
orphan data file does not authorize deleting a recent sidecar. An orphan
index directory is removed only when all its objects exceed the threshold.

A merge installs immutable native tags for its source, target and selected
base before freshly validating the captured source and target authority.
Only then is capture accepted. Each tag name contains a nonce, a digest of the
exact target incarnation and its canonical logical head. An earlier collector
retains those current inputs; a later collector captures the tags or refuses
a changed inventory. After acceptance, source advancement is allowed and the
merged parent remains the captured source commit.

The merge releases only acknowledged, nonce-owned tags after a definitive
outcome. A typed in-doubt publication, cancellation or ambiguous tag creation
keeps the tags; cleanup expires them only when the target witness is provably
dead. An unchanged or unknown future head retains them. Live heads plus
retained tagged heads form the frontier for pairwise merge-base retention. Native trees that supply imported
lineage or selected bases remain available after logical retirement.

These rules permit ordinary graph writers and merges to overlap cleanup.
Graph-branch creation/deletion retains RFC 0022's single-writer-process control
boundary. Native borrowed-file origins are protected across table locations;
an incomplete trace prevents physical-file sweeping. Auxiliary native trees
protect their files without using those file protections to retain themselves.

The delete decision therefore rests on two proofs over two populations. An
unrooted manifest in the published set is collectible because a graph version
once named it and no retained snapshot or tag needs it. An unrooted manifest
outside that set is collectible only when its staging authority is dead under
the rule above. Every root is retained, including a table version protected by
a native tag but never published by the graph. The twin proof, the head
comparison and GC deferral have no counterpart. An incremental sweep driven by
`__manifest` pruning, with the full mark as its fallback and as the oracle's
reference, is later work.

### Change discovery

Lance stamps every row with `_row_created_at_version` and
`_row_last_updated_at_version`. On a promoted twin they equal the linear
version, which equals `published_dataset_version`. On a detached commit they
are not monotonic (RFC 0067's probe table), and without promotion they stay
that way.

Six window predicates in `main` compare a stamp with
`published_dataset_version` bounds. RFC 0067 counts three; the other three
are the branch-merge pure-insert proof.

| Stamp | Where | Used by |
|---|---|---|
| `_row_last_updated_at_version` | `diff_table_same_lineage` (`changes/mod.rs`) | change feed, same-lineage diff |
| `_row_last_updated_at_version` | `CandidateUpserts::open` (`changes/candidate_scan.rs`) | change feed, candidate scan over new fragments |
| `_row_last_updated_at_version` | `Predicate::VersionWindow` lowered in `engine/push/scan.rs` | planned change-feed diff |
| `_row_created_at_version` | `scan_proven_insert_delta_bounded` (`table_store.rs`) | merge, pure-insert delta |
| `_row_created_at_version` | `scan_proven_insert_blob_row_ids` (`table_store.rs`) | merge, Blob row ids of the delta |
| `_row_created_at_version` | `scan_proven_pure_inserts_for_validation` (`exec/merge.rs`) | merge validation |

Today a detached `to` endpoint already leaves the windows:
`diff_table_same_lineage` returns `None` when the opened version is
detached, and `diff_snapshots` routes the interval to
`diff_table_cross_branch`, which streams both images in id order and
compares them. That path is exact and has no row or byte bound. Under this
RFC every endpoint is detached, so it cannot be the only path (invariant
11).

`__manifest` is not a graph table. It stays a linear Lance dataset, and its
registration clock, which reads the same stamp on `__manifest` rows, is
untouched.

The stamp is not the only per-commit record a table carries. Every commit,
detached or linear, writes a transaction file whose operation lists the
fragments the commit added and the parent fragments it rewrote or removed
(`Transaction::operation`: `Append { fragments }`, `Update {
removed_fragment_ids, updated_fragments, new_fragments }`, `Delete {
updated_fragments, deleted_fragment_ids }`). The pruned change-feed path of
RFC 0030 already derives one commit's inserts and updates from that footprint
(`changes/candidate_scan.rs`): candidate rows are the rows of the new
fragments, before-images come from the touched parents at the base, and the
exact merge is the fallback on any doubt. What ties that path to the linear
chain is not the mechanism but its admission, which requires the interval to
advance by exactly one Lance version and the transaction's `read_version` to
equal the base row's `published_dataset_version`, and a stamp filter it
applies inside the new fragments.

Candidates, measured on the change-feed and merge cost owners before
acceptance:

| Candidate | Shape | Cost | Open point |
|---|---|---|---|
| Change set carried by the commit | inserts and updates from the commit's transaction file as the pruned path reads it today; deletes from the ids the delete writes into the same commit's transaction properties; admission on the pin's `staged_version` and transaction uuid; the exact path as fallback | reads bounded by the commit's own fragments and one transaction file per commit in the window; no per-row data, no schema change, no second record | a merge proof over an N-commit branch reads N transaction files, which are independent objects fetched in parallel; a window that crosses a compaction `Rewrite` takes the exact path, as it does today |
| Version columns written by OmniGraph | two system columns per table under RFC 0040's namespace holding the `published_dataset_version` of the write that created and last wrote the row; a null reads as the Lance stamp | two integers per row on every write; the six predicates keep their shape | rejected 2026-09-22: a second copy of the graph version on every row, which every writer must keep in agreement with `__manifest` (invariant 12) |
| Exact path only | delete the windows, keep `diff_table_cross_branch` | none to build; O(rows on both sides) per interval | unbounded; rejected as the only path, kept as the fallback |
| Change sets recorded at commit | per-table inserted, updated and deleted ids recorded with the graph commit (the idea in draft RFC 0068, Change sets on the record) | removes candidate scans and the stamp dependency for feed and merge | larger than this RFC; a record beside the commit can drift from the rows, so invariant 12 keeps the exact path as its check |
| Row-lineage version set by the writer | an upstream Lance transaction option replacing `current_manifest.version + 1` in `build_manifest` (`rust/lance/src/dataset/transaction.rs`) with a caller-supplied value, so a detached commit stamps its rows with `published_dataset_version` | none in OmniGraph; the six predicates stay as they are | waits on a Lance release and a pin bump; kept as a later upstream ask, not a gate |

Recommended for this RFC: the change set carried by the commit. It adds no
column and no record: the transaction file is written by the commit that
writes the manifest, and its transaction properties are part of it, where
the `insert_absence` certificate (`exec/merge.rs`) and the staging witness of
Garbage collection already live. `__manifest` names the commit; the commit
describes itself.

Per commit and per table the change set is read as follows. An insert is a
row of the commit's new fragments whose id is absent from the touched
parents at the base; an update is one whose id is present there; both are
what the pruned path computes today, with the stamp filter inside the new
fragments removed, because under a detached commit every row of a new
fragment was written by that commit. A delete is an id the commit removed,
written by the writer at staging time under the transaction property
`omnigraph.deleted_ids` up to 64 KiB of encoded JSON, and above it into a
file the property `omnigraph.deleted_ids_path` names, written before the
commit and marked by the collector. The optimization admits at most 16 MiB
of encoded JSON. Larger records are omitted by the writer; readers reject
oversized inline records before deserialization and oversized spills before
downloading their body, then take the exact path. A deletion file names
positions, not ids; an admitted record supplies the missing ids. A load upsert and a merge-insert `Update` are inserts and
updates by the same rule. A chunked branch merge is read chunk by chunk along
the chain's `read_version` links; each chunk is its own commit.

The six predicates go with the stamp, and their sites are rewritten.
`CandidateUpserts::open` admits a plan when the `to` row's `staged_version`
opens and its transaction file carries the row's `transaction_uuid`, and the
transaction's `read_version` is the `from` row's opened version;
`diff_table_same_lineage` calls that path for every interval of one lineage
and the exact path when the transaction cannot prove the interval, which
includes a compaction `Rewrite` and any transaction kind the engine did not
author; the planned change-feed diff lowers a fragment window over the
commit's new fragments in place of `Predicate::VersionWindow`, or takes the
exact path, which the implementer decides. Merge's three proofs read the
transaction files of the branch's commits, fetched in parallel: the delta is
the union of their new fragments when every commit is an `Append` or carries
the `insert_absence` certificate, and the proof fails otherwise, as it fails
today when the created window is not pure. `scan_proven_insert_delta_bounded`
and `scan_proven_insert_blob_row_ids` scan that union;
`scan_proven_pure_inserts_for_validation` checks it against the same set.

Intervals that predate the switch keep the stamp path. An interval whose two
endpoints are both at or below the table's last linear version reads promoted
twins, whose stamps mean the version they name; one whose `to` endpoint is a
detached pin takes the commit path when that pin's transaction names the
`from` endpoint's opened version as its base, linear or detached, because the
proof is the transaction's `read_version` and reads no stamp; every other
interval takes the exact path.

Lineage for these intervals is the graph branch's `__manifest` lineage, never
`native_dataset_branch`, which two branches writing one dataset now share. A
commit path is valid only between two pins of one such lineage; between
lineages the exact path runs, as it does across branches today, and
`same_lineage` is rewritten to compare `__manifest` lineages.

### Branches

No new table fork is created. RFC 0067 forks a table at a branch's first
write, and the fork "needs a linear source version" (`table_ops.rs`,
`exec/merge.rs`), so the inherited pin is promoted first
(`open_owned_dataset_for_branch_write`, `open_first_touch_merge_target`,
`maintain_indices_for_branch`, and the deferred-fork path in
`exec/staging.rs`). Without promotion an inherited pin is detached and has
no linear version to fork from. The `lance_surface_guards.rs` probe in
Evidence answers the Lance half of that: `create_branch` accepts a detached
id and records it as the branch's parent, but the shallow clone keeps the
source's version number, so the branch's first manifest is itself detached
and the branch has no head; a linear commit on it is refused and only a
detached chain continues it. Keeping first-touch forks is therefore an
engine choice, not a Lance fact, and stays in Unresolved questions.

A branch write therefore stages detached on the dataset its inherited pin
already names, from that pin. Two graph branches writing the same table
produce two detached chains in one dataset. Neither moves a HEAD, so they
cannot conflict, which is owed as a surface guard; each branch's
`__manifest` lineage names its own chain. Fragment and row id counters
diverge per chain exactly as they diverge per fork today, and three-way
merge compares logical ids. Pointer adoption pins the source's
`staged_version` in the same dataset. The collector's roots already span
every live graph branch.

Ownership replaces the fork test. A branch writes on the dataset and native
branch that its inherited registration row names, and ownership of a table
is that row's `__manifest` lineage. `is_table_fork_of` therefore leaves the
three gates that key on it: `prepare_existing_merge_target`, the inherited
skip in index Ensure, and the mutation fast path.

An existing fork stays valid: a row that names a `native_dataset_branch`
opens there, and a later write on that graph branch stages detached on the
fork. The native ref create for a table fork is a conditional create in
Lance 11; its delete is a plain remove, which RFC 0067 notes is not
compare-and-swap in its RFC 0065 alternative. Both leave the write path.

### Format and migration

Storage stamp v11 (provisional until the order against RFC 0068 is decided
by the engine maintainers at the first of the two acceptances). Draft RFC
0068 assigns v11 to a different incompatible format, graph records replacing
`__manifest` (`docs/rfcs/0068-graph-commit-record.md`), and one stamp cannot
fence two formats: a binary that accepts v11 would accept a graph written for
the other and misread it. The allocation is sequential. Whichever of the two
is accepted first takes v11 and the other takes v12, and both RFCs are
updated at that decision.

The route is `omnigraph upgrade --to-format 11` on a standalone root with
every writer stopped, registered as the step `detached-only-v10-to-v11`
beside `detached-pins-v8-v9-to-v10`. A cluster-managed graph has no route in
this RFC: `omnigraph upgrade` refuses one by its resolved target and again by
the cluster membership of its URI (`crates/omnigraph-cli/src/upgrade.rs`),
and `docs/dev/versioning.md` limits qualified journeys to local standalone
roots, so stopping every writer does not admit one and a storage URI does not
bypass the refusal. A qualified cluster conversion route, with its owner and
its evidence, is a Rollout item before this stamp can be the default for a
managed graph. The route refuses a graph on which `repair`
reports `blocked_promotion`. It promotes every pending v10 pin once, the
last promotion the engine ever runs, and reaps each proven chain from its
oldest link to its registered tip. An interrupted pass leaves its discovery
tip available for retry. Every row that predates the switch carries Lance stamps from a
linear commit at or below the last linear version, which is what the null
rule of Change discovery reads. A later write can carry such a row unchanged
into a detached manifest; its stamps do not move, and the reading holds. It
then records `omnigraph.last_linear_version` for every
registration row on every live branch: that is a data publication per
branch, not the metadata-only restamp the v10 step was.

A v10 row resolves through its linear twin at `published_dataset_version`,
checked against `transaction_uuid`, which is the v10 rule; the recorded last
linear version confines that rule to rows at or below it. An older binary is
refused by the stamp: under v11 `published_dataset_version` names no Lance
version, which a v10 binary would misread as reclaimed history. Downgrade is
an export with the v11 binary followed by `init` and `load` with the v10
binary; history and branches are lost.

## Invariants

- 1 (respect the substrate): the hard case. Lance owns cleanup, and the
  deny-list rejects "a custom ... storage primitive already owned by Lance".
  The case is different because Lance's cleanup does not own detached
  manifests: it does not trace their references (probe 3), and the keep set
  it builds with `tracked_files` silently omits them, because Lance 11.0.0
  maps a `d{id}.manifest` it cannot parse as a linear version to nothing;
  upstream `main` refuses a detached dataset outright (Lance PR #8097). RFC
  0067 already ships a second collector for that reason. This RFC replaces
  two collectors with one and keeps the upstream ask that would make it
  thin: include referenced detached manifests in `tracked_files`.
  Upstream's position that visibility authority is the physical `_versions/`
  directory (#7222, #7264) is not met for external readers; see User and
  operational behavior.
- 2 (one publication door): unchanged in effect. Its sentence "promotion onto
  the linear history follows publication and is derived" is removed.
- 5 (crash convergence): narrower, not gone. No post-publication effect of a
  table write exists to converge, so one clause of one sentence goes. "An
  effect that is published must carry in the manifest itself what finishes
  it: a pin's target version, staged version and transaction uuid, a staged
  schema contract's publishing commit" keeps its schema clause and loses the
  pin clause, because nothing finishes a published table effect while a
  schema apply still installs its contract files after the publication
  (`db/omnigraph/schema_apply.rs`) and still recovers a pending install after
  a failure, which `docs/dev/recovery.md` requires. Schema apply's current
  one-mutation-process support boundary is unchanged by this RFC.
  "It retains an unpublished first-touch fork
  while the graph branch incarnation in its name is live" holds for the
  forks that exist, and none are created.
- 7 (physical acceleration is derived): promotion was derived state and is
  gone; change discovery's replacement is judged against this invariant in
  its own section.
- 11 (bounded, observable failure): the blocked state, the only RFC 0067
  condition with no resolution, is removed. Unpublished staging is retained
  only while it can still publish. On a branch that keeps publishing that is
  the in-flight window of Garbage collection; on an idle branch a crashed
  writer's staging stays until a later publication moves the head, which
  bounds progress rather than safety. It is reported per table either way.
- 12 (one source of truth): `__manifest` is the only lineage of a table.
  The linear chain stops being a second, derived history that can disagree
  with it.
- 3, 4, 6, 8, 9, 10: unchanged. 13: this RFC's gates.

Two deny-list items are touched. "Cold full-history reconstruction per
request" is what the published set costs, but per `cleanup` run and never
per request. That run has two cost terms, not one. Root marking is bounded by
retained `__manifest` versions times tables times live branches. Discovery
and sweep are proportional to the garbage examined: the historical published
rows the snapshot's HEAD still carries, the detached manifests and merge
links still present, the index read per root, and the files of each swept
manifest. Each table location has one manifest listing and one object
listing. Each live branch has its capture and one validation HEAD read,
plus reads of retained versions below the captured HEAD. Both terms have
`maintenance.rs` as their cost owner and a ceiling there. An incremental
cursor that would make discovery proportional to what
the prune changed is later work.
"A job queue where an idempotent reconciler suffices" is touched from the
other side: this RFC deletes a reconciler and adds no queue.

## Compatibility and reversibility

- Wire: the `cleanup` JSON result and the meaning of `cleanup --keep` both
  change, on the schedule set out under User and operational behavior: the
  four new per-table fields are additive, `old_versions_removed` and
  `deferred` are retained for one release and removed at the next release
  boundary, and `--keep` takes its new meaning at the stamp. No request
  field, no other response and no other command changes.
- Storage: stamp v11, provisional as above. No new pin field. One further
  value in `TableVersionMetadata` (`db/manifest/metadata.rs`) per
  registration row, the last linear version, and three transaction properties
  on detached commits: the staged-against branch incarnation and graph head
  on every one, and the deleted ids, inline or by path, on a delete.
- Downgrade: refused by stamp. The route is an export with the v11 binary,
  then `init` and `load` with the v10 binary, which loses history and
  branches.
- External Lance readers lose current data; see above.
- Reverting: export and rebuild is the only reversal this RFC promises.
  Replay is not one, and not because of the collector: a merge chain
  publishes one `published_dataset_version`, `base + 1`, whatever its length,
  while the promoter requires `base + chain.len() == target` and derives its
  replay targets from the chain length (`db/omnigraph/promotion.rs`). Replay
  of a three-chunk chain on frozen linear base 10 therefore writes physical
  11, 12 and 13 while the published pin 11 names the first chunk, before any
  reclamation happens, and two graph-branch chains in one dataset collide on
  those physical targets as well. A conversion that remapped every retained
  pin and its history and rebuilt independent native lineages, with
  round-trip evidence, is uncommitted future work. Export and rebuild is more
  expensive than RFC 0067's reversal, which is a stated cost of this
  decision.

## Alternatives

- **Do nothing (keep promotion).** Works today. Carries the reconciler, the
  twin proof, the staging rule, GC deferral and a blocked state with no
  resolution, and carries them into RFC 0068's format.
- **Lazy promotion (draft RFC 0068's position).** Draft RFC 0068 states it
  twice: its Summary runs promotion lazily, by the next writer or by
  `cleanup`, and its Publication step 6 keeps a best-effort promotion of
  pins inside the publisher after the conditional create. Either way it
  takes promotion off the write path, keeps every mechanism above, and
  lengthens the window in which a pin is pending. In that window the change
  feed takes `diff_table_cross_branch`, which has no row or byte bound.
- **Keep promotion only as an export step.** Promote on demand when an
  operator wants a table readable by external Lance tools. Keeps the
  reconciler's replay but none of its write-path, GC or blocking roles.
  Compatible with this RFC as later work; not proposed here because the
  collector reclaims the chain the replay needs, and because replay cannot
  reach the published target of a merge chain (Compatibility and
  reversibility). Decided by the engine
  maintainers at the first operator request for Lance-readable tables.
- **One Lance dataset per graph (RFC 0067's Alternatives).** RFC 0067
  records it as the stronger end state on both simplicity and latency. A
  graph commit becomes one Lance commit, which removes the linear chain, the
  collector, the version columns and the unpublished-staging question
  together. It is a larger irreversible format decision and needs its own
  RFC; this RFC is a step toward it rather than away from it, because it
  leaves `__manifest` as the one lineage of a table.
- **Collector without the published-set proof (age horizon).** Simpler
  sweep, reclaims abandoned staging. Rejected for RFC 0067's reason: a
  writer can outlive any age horizon between staging and publication.
- **Wait for upstream `tracked_files` over detached manifests.** Would let
  stock cleanup keep the delete decision. No upstream movement is recorded in
  RFC 0067's survey; this RFC keeps the ask and does not wait on it.

## Evidence and tests

Evidence already in `main` that this RFC relies on, from RFC 0067's probe
table: a second detached commit chains from a detached version and reopens
by id; a keyed merge-insert `Update`, a `Delete` and a `CreateIndex` each
commit detached from the previous detached version and the BTree index is
used there (probe 4); stock `cleanup_old_versions` deletes a data file that
only a detached manifest references (probe 3); compaction stages detached
(probe 13, which staged a `ReserveFragments` and a `Rewrite`; the shipped
step is the one `Rewrite` of RFC 0067's Optimize row).

New evidence, each an entry in `blocked_on`:

- The everything-blocked run and its red list. The run forces `promote_pin`
  to the blocked outcome at every call site and `cleanup_all_datasets` to
  defer every graph table, under the `no-promotion` feature, and then runs
  the GQT corpus, the failpoint suite, the DST scenarios and the cost
  owners; every test that goes red without asserting promotion itself is a
  dependency on the linear chain and is listed here. Run at `ed145db4`:
  every write on `main` publishes, because `resolve_pinned_for_write` stages
  behind a blocked pin, and every read, change feed and load over that state
  passes (GQT 275 of 322 green, all 97 corpus cases on `main`). The
  dependencies are the writers that plan on the linear HEAD and turn the
  blocked outcome into an error: the first-touch fork
  (`promote_inherited_pin`: every branch write, 39 GQT cases, 6 DST
  scenarios, most of the 222 engine rows), compaction (`optimize.rs`
  `promote_pending_pin`: 18 DST scenarios), schema apply (`schema_apply.rs`:
  2 DST scenarios, 15 engine tests), the system-column upgrade
  (`system_column_upgrade.rs`: 13), merge on an existing target
  (`exec/merge.rs`, never reached because the branch write fails first), the
  upgrade preflight and one RFC 0040 guard that open a table by linear
  version number. Red because they assert promotion, the reaper or the
  linear HEAD itself, deleted with the promoters: the four
  `promotion::tests`, the "pin stays pending until a writer promotes it" and
  "linear HEAD carries the promoted pin" cells of `failpoints` and
  `detached_commit_matrix`, the `maintenance.rs` orphan and residue
  fixtures, and the promotion cost cells of `write_cost` and
  `warm_read_cost`. The cost owners `changes_cost` and `merge_cost` are red
  on the fork, so the branch side of the inventory needs a fork shape before
  Change discovery is measured.
- The collector's DST oracle and crash windows; `maintenance.rs` for
  retention of unpublished staging whose recorded authority is still live,
  reclamation of unpublished staging that is dead under the staging rule, and
  reclamation of published, expired
  manifests. In the tree, report-only, at the second rollout step: the
  collector (`db/omnigraph/collector.rs`, run by `cleanup` before the RFC
  0067 reaper and alone by `Omnigraph::cleanup_plan`), the staging witness in
  every detached commit's transaction properties, the four collector fields
  on the `cleanup` rows and the CLI's `--json`, the `CollectorInvariant` DST
  oracle after every successful `Cleanup` and in the final audit, and the
  seven `blocked_on` fixtures: the pruned pin, the merge chain, the branch
  that retains a pruned pin and the repeat run in `maintenance.rs` (with the
  two ceilings below), and the seam-driven three, the stranded staging, the
  publication after the snapshot and the interrupted pass, in
  `failpoints.rs`, because the seams are process-wide and only that binary
  runs serially. One v10 fact the fixtures forced into the collector: the
  RFC 0067 reaper reclaims a promoted pin's proven copy even while the pin is
  current, so a root absent from the listing resolves through its linear
  twin (`published_dataset_version`) and is an error only when both are
  absent; the rule disappears with the reaper. Two facts the oracle found in
  the DST's substrate: the harness's `StorageAdapter` is a store of its own
  (`ObjectStorageAdapter::in_memory()`) beside the shared-memory store Lance
  writes to, so a probe of a table's files goes through that table's Lance
  object store (`Omnigraph::cleanup_plan_missing_paths`), and the RFC 0067
  reaper's delete of a proven copy went through the adapter and so never
  reclaimed one in any DST universe; it now deletes through the table's
  object store, which every sweep of this RFC will use too. `lance_surface_guards.rs` for: a detached commit never triggers
  auto-cleanup, a detached manifest lists every fragment of its version,
  detached ids do not collide across writers on one dataset, a `Rewrite`
  planned on and staged from a detached version, transaction properties
  survive on a detached manifest and are readable from it, two chains from
  one base with equal fragment and row ids open and scan correctly in one
  `Session` with no cross-chain `_rowaddr` consumed by merge, a dataset
  whose only linear manifest is `v1` opens by detached id, and a native
  branch created from a detached version has no head. The eight are in the
  tree as the `rfc_detached_only_*` probes and pass.
- Change-feed and merge cost owners for the chosen change-discovery
  replacement, and the carry-forward of `omnigraph.last_linear_version`
  through `from_dataset` and every `with_staged` caller. In the tree at the
  third rollout step, the feed half: `changes/candidate_scan.rs` admits an
  interval by one of two proofs, the linear stamp path as before or the
  commit path (a detached `to` opened at its pin's staged version whose
  manifest-recorded transaction identity carries the pin's uuid and names the
  `from` side's opened version), reads the stamp window only on the stamp
  path, and derives a `Delete` from its `omnigraph.deleted_ids` record with
  before-images from the parent fragments the delete touched; the snapshot
  diff (`changes/mod.rs`) takes the same plan for a detached `to` before the
  exact compare. `changes_cost.rs` under `no-promotion`: the Δ=1 update
  opens two pinned datasets and reads one transaction, flat in table extent;
  the recorded delete is flat with one transaction read where the linear
  delete grows with extent; the feed's backlog walk keeps its per-commit
  bound. Under the feature the resolution opens a staged pin's detached
  version directly, the rule of Resolution; the v10 target-first probe paid
  three opens per pin. The merge half waits for the fork shape of Branches.
- Cold-open cost of a detached pin against a linear pin (one direct GET by
  id against a version lookup).
- `detached_commit_matrix.rs` and `failpoints.rs`: promotion windows
  deleted; the windows `cleanup.sweep_pre_manifest_delete`,
  `cleanup.sweep_between_manifests` and `cleanup.sweep_pre_file_delete`
  added, each reached by the `cleanup` milestone recipe.

### Acceptance thresholds

- A write without promotion costs no more object-store operations per
  touched table than the same write under RFC 0067 minus its promotion
  commit, on a warm handle and on a cold one; `write_cost.rs` and
  `write_cost_s3` hold the ceilings.
- The change feed and merge cost owners are flat against today's promoted
  path on the same fixtures.
- A `cleanup` run's root marking is bounded by retained `__manifest` versions
  times tables times live branches. Its discovery and sweep are bounded by
  the garbage examined: historical published rows, present detached manifests
  and merge links, the index read per root, and the files of the swept
  manifests. Both ceilings live in `maintenance.rs` and are measured by the
  collector's owner.

## Rollout

1. Land the inventory run under the `no-promotion` feature; record the red
   list here.
2. Land the collector beside the existing cleanup, reporting what it would
   delete and deleting nothing, with the DST oracle comparing its mark set
   against every retained pin. Ships safely: no behavior change.
3. Land the change-discovery replacement with the exact path as fallback:
   the change feed and the snapshot diff here, since they need no fork;
   merge's three proofs with step 4, since they walk the fork boundary that
   the Branches section leaves to the engine maintainers.
4. Stamp v11 through the step `detached-only-v10-to-v11`: stop promoting,
   switch resolution, switch `cleanup` to the collector, delete the
   promoters in `promotion.rs` while keeping `walk_chain`, `table_location`
   and `detached_manifest_path` for the collector and resolution, delete the
   twin proof and reaper (`settle_pin_before_cleanup`,
   `reap_detached_manifests`), GC deferral and the blocked outcome. Amend
   RFC 0067 under README rule 4 (its Promotion, Garbage collection,
   Branches, Format and migration and Invariants sections and its `repair`
   and `cleanup` bullets), and update `writes.md`, `recovery.md`,
   `maintenance.md`, `troubleshooting.md`, `invariants.md` sections 2 and 5
   with its checklist, and `lance.md`'s compatibility table. Ship with it the
   `cleanup` consumer fixture: one JSON result asserted field by field on a
   v10 graph and on a v11 graph, covering `old_versions_removed` and
   `deferred` in their retained form, the four new fields beside them, and
   what `--keep` retains on each; the release notes of this stamp carry the
   `--keep` behavior change and the one-release deprecation, and the removal
   of the two retained fields is scheduled for the next release boundary. The
   stamp number is settled against RFC 0068 before this step, and the cluster
   conversion route named in Format and migration is decided here with its
   owner.
   `supersedes` stays empty, because detached staging and the pin are
   unchanged. `implementation` moves to `complete` here.

## Unresolved questions

- Whether the planned change-feed diff lowers a fragment window or takes the
  exact path. Open until the planned path carries an interval proof at all:
  today `QuerySource::adjacency_proof` is `None` in the engine, so
  `Predicate::VersionWindow` is never lowered and the planned diff reads no
  stamp. The size bound is decided: `omnigraph.deleted_ids` holds the JSON
  array inline up to 64 KiB; through 16 MiB,
  `omnigraph.deleted_ids_path` names
  `_omnigraph/deleted_ids/<transaction uuid>.json` under the table. Above
  16 MiB, the record optimization is skipped and discovery takes the exact path.
- Whether the upstream row-lineage option of Change discovery is asked for.
  It would let the six predicates return unchanged; nothing in this RFC
  waits on it.
- Whether first-touch forks are kept. Lance creates a native branch from a
  detached version but leaves it without a head (the Branches section and
  the probe in Evidence), so a kept fork needs engine handling: either the
  clone's commit handler renumbers the fork's origin to a linear version, or
  the fork stays headless and every write on it chains detached from the
  inherited id. Both shapes were measured on prototypes at a constant cost
  against history, while promoting the inherited pin first pays the whole
  deferred chain at the fork. Decided by the engine maintainers before
  Rollout step 2, against the Branches section's staging on the inherited
  dataset.
- Whether the table's last linear version is kept as readable history or
  dropped by an export and rebuild. Decided by the engine maintainers at the
  first operator request for a Lance-readable table.
- Whether this RFC lands before draft RFC 0068 or is folded into its
  rollout. Decided by the engine maintainers when RFC 0068 leaves draft,
  together with the stamp allocation of Format and migration. The protocol
  written here is `__manifest`-specific: the published set is read from
  append-only registration rows at a `__manifest` HEAD, the staging witness
  names a graph head that `__manifest` carries, and the upgrade rewrites
  registration rows, while RFC 0068 prunes records and deletes `__manifest`
  (`docs/rfcs/0068-graph-commit-record.md`). The root definition in Garbage
  collection names the graph authority rather than `__manifest` alone, which
  is necessary and not sufficient. A combined rollout is conditional on RFC
  0068 defining a stream witness in place of the graph head recorded here,
  retained-publication discovery in place of the historical registration
  rows, and preservation of the staging properties, with the collector's two
  proofs restated over them.

## Decision log

- 2026-09-21: drafted from RFC 0067 as implemented at `ed145db4`.
- 2026-09-22: change discovery re-decided. Supersedes the sentence
  "Recommended for this RFC: the version columns, because they keep the six
  consumers unchanged and leave recorded change sets free to replace them
  later" and the two rules that made the columns total. The replacement is
  the change set the commit carries (transaction file for inserts and
  updates, `omnigraph.deleted_ids` for deletes, exact path as fallback),
  because the columns were a second copy of the graph version on every row.
  The upstream row-lineage option is recorded as a later ask.
- 2026-09-24: the mixed interval of Change discovery takes the commit path,
  not the exact path. Supersedes "a mixed interval takes the exact path": the
  commit path's proof is the transaction's `read_version` against the `from`
  endpoint's opened version, which holds whether that version is linear or
  detached, so a pending v10 pin over a promoted base is read from its own
  transaction. Recorded deletes are read on the commit path only; a linear
  delete keeps the exact path so the v10 change-feed cost contract is
  unchanged, and admitting the record there is one condition once every
  commit carries it.
- 2026-09-24, phase 4: a row that records no last linear version takes the
  v10 twin path, not the direct detached open. Supersedes the reading of
  Resolution under which an unrecorded row opened `staged_version` first:
  every registration older than the upgrade has its proven copy reaped by
  the upgrade itself, so a historical `__manifest` version read after the
  switch must resolve those rows from their twins. To keep the direct open
  on every row written after the switch, a table records `1` at creation
  (init and schema apply), which Pins already stated, and every writer
  copies the value forward.
- 2026-09-24, phase 4: the v11 step runs its engine half before the fence
  through an admitted engine handle (Format and migration). The fence
  refuses every open, and the promotion, reaping and per-branch publication
  need the engine's table storage and publisher; a raw rewrite of
  registration rows under the fence would duplicate the publisher.
- 2026-09-24, phase 4, the collector deleting: a linear version below the
  last linear version that no registration ever named is kept, not swept.
  Garbage collection 5 says a linear version below the last one "follows the
  same sweep rule", and that rule deletes what a `__manifest` version named;
  an unnamed linear version was named by nothing, so the rule has no
  evidence for it and the collector leaves it, as it leaves foreign
  versions. Every table version a promoting writer ever published is named,
  so the case is a maintenance commit from before registration, and its
  files stay until a later rule. The sweep runs the files of a location
  before its manifests and each chain's links before its tip, so a pass
  that stops early strands nothing; `unpublished_manifests` counts the
  stagings the run found, dead ones included, and a dead staging reclaimed
  also counts in `manifests_removed`. Pruning each branch's `__manifest`
  history remains separate: the run computes what the policy retains,
  sweeps table storage on that basis, and deletes no `__manifest` version.
- 2026-09-24, phase 4, the collector deleting: a live graph branch retains
  its oldest `__manifest` version beside its HEAD (Garbage collection).
  Found by the DST under `--keep 1`: a merge of `b0` into main read
  reclaimed history, because the branch's fork-point registration lived only
  in the pruned versions of the two lineages and its pin, the merge base, was
  swept. With promotion the branch's table fork held a copy of that base and
  hid the dependency. Any future pruning of each branch's `__manifest`
  history must preserve the same floor.
- 2026-09-25: the storage implementation uses stamp v11 and the upgrade
  step `detached-only-v10-to-v11`
  (`db/manifest/upgrade.rs`, `db/manifest/upgrade/detached_only.rs`), so
  the provisional allocation of Format and migration is settled and RFC 0068
  takes the next number. Where the code differs from the body, the code
  wins and each sentence is named here. Compatibility and reversibility,
  Wire: "`deferred` remains and is always false on a v11 graph". The
  `cleanup` row carries no `deferred` field (`DatasetCleanupStats`,
  `db/omnigraph/optimize.rs`; the CLI `--json` row in
  `omnigraph-cli/src/main.rs`); `old_versions_removed` alone is retained
  for one release, with the value of `manifests_removed`. User and
  operational behavior, `repair`: `verified_maintenance`, `suspicious` and
  `unverifiable` still exist as classifications and are reached only by a
  registration that records no last linear version and no `staged_version`
  (`repair.rs`, `judge_against_last_linear_version`), which no current v11
  row is. Content writers: "the writer deletes its own detached manifests
  before it reprepares" has no counterpart in the code; a refused
  publication leaves its staging for the collector. Unresolved questions,
  first-touch forks: decided, none are kept; a branch write stages detached
  on the inherited dataset and native ref, `native_dataset_branch` stays
  inherited, and a fork from before the stamp stays valid (Branches). The
  cluster conversion route stays undecided: `omnigraph upgrade` still
  refuses a cluster-managed graph. Promotion runs once more, inside the
  upgrade step, for every pending v10 pin; the promoters, the twin proof,
  the reaper, GC deferral, the blocked outcome and the `*.post_publish_pre_promotion`,
  `fork.post_create_pre_open`, `cleanup.pre_reap`, `cleanup.reap_delete` and
  `promotion.pre_replay` seams are deleted. RFC 0067 is amended under README
  rule 4 in its own Decision log.
- 2026-09-25, collector snapshots and pin identity: the collector derives each
  branch view from one pinned dataset and validates all captured versions,
  incarnations and the live inventory before tracing. Only identity-validated
  retirements captured before those views prove an absent incarnation dead;
  unknown incarnations remain undecidable, and retirement refs survive until
  every table sweep succeeds. Historical table lifetimes are swept even when
  absent from the current catalog. Blob sidecars are explicit planned objects;
  unverified objects use their own seven-day ages, including temporary
  manifests. Discovery costs two listings per location and one validation
  HEAD per live branch. Handle and topology identities include the detached
  pin even without an e-tag; the persisted topology format is v3. Upgrade
  reaps proven chains oldest first, preserving the registered tip for retry.
  Deleted-id records admit at most 16 MiB of encoded JSON; larger records
  take exact discovery without loading an unbounded optimization record.
- 2026-09-25, retirement evidence lifetime: successful table sweeps do not
  prove that every old writer has finished staging. Preserve existing
  retired `__manifest` refs and histories indefinitely, including standalone
  reconciliation. This adds no durable record, but retains control history
  and required native ancestors for every retired branch. Ref-absent
  manifest trees and unneeded table storage remain collectible. Unrelated
  native table-fork reconciliation remains independent of table failures.
- 2026-09-25, retirement archives and retained roots: supersedes the
  retirement-evidence entry's requirement to retain retirement refs
  indefinitely. Exact archives precede active-ref unlink;
  frozen native candidates, post-list identity cuts and final graph/tag
  validation permit unneeded archive reclamation. Garbage collection's root
  and mark sentences include exact graph/table tags and base/overlay data
  files; table tag inventories also validate before deletion. The merge-tag
  release sentence now excludes typed in-doubt publication outcomes. Needed
  imported lineage and exact selected merge bases survive owner retirement;
  this does not retain every historical commit or change branch-control
  concurrency boundaries.

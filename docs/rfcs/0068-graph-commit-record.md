---
rfc: "0068"
title: "Graph commit record"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-09-15
updated: 2026-10-08
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Cost instrument: object operations per publication and per current-state read flat at depths 10, 100, 1,000 and 10,000 on file, S3 and Azure, checked in under the existing cost owners."
  - "Concurrency evidence: the probe-14 race and lost-acknowledgement cases on the configured S3 and Azure suites, and DST scenarios for concurrent publishers, stale hints and missing checkpoints."
  - "Conversion: a lossless offline route from a v14 graph (each branch's `__manifest` head and buffer and its `__history` files, the legacy area included) to records and checkpoints, and the refusal fence for older binaries."
---

# RFC 0068: Graph commit record

**Depends on:** [RFC 0067](0067-detached-table-commits.md) for private
table effects and the three-field pin, as amended by
[RFC: Detached-only tables](2026-09-21-detached-only-tables.md), which
removed promotion at storage stamp v11. This RFC changes only where the
graph commit is stored and how current state is read.
**Surveyed:** OmniGraph 0.11.0 on `main` at `d1dd8b97`; Lance 11.0.0 and
12.0.0-rc.1; RFCs 0013, 0022, 0024, 0028, 0030, 0038, 0042, 0058, 0062,
0063; Lance discussions #7176, #7222, #7260, #7264; object_store 0.14.1;
probe 14 in `crates/omnigraph/tests/wal_probe_investigation.rs`.

## Summary

A graph commit becomes one immutable object, the ***commit record***,
written with the storage crate's conditional create at a name that encodes
the next sequence number on its branch. The conditional create is the
publication door and the cross-process fence. The record carries the parent
commit, actor, schema identity, the pins of every table the commit touched
in RFC 0067's shape, catalog deltas and merge lineage.

Current state is read from the newest ***checkpoint***, an object holding
every table pin at one sequence number, plus the records after it. The read
cost is bounded by the checkpoint interval, not by history. A best-effort
***hint*** object names the latest sequence; readers probe upward from it.

In the throughput path recorded in RFC 0067 this is the fourth and last
step, the only one that moves the per-branch commit ceiling itself; live
maintenance, staging outside the gates and group commit come before it and
do not depend on it.

The `__manifest` Lance dataset, its per-branch native refs, its journal fold,
the registration clock of RFC 0062, and the clone adapter that repairs
inherited index bases for it are retired. Graph branches become
***streams*** of records with a metadata object each. Nothing follows a
publication: detached-only tables removed table-pin promotion, and a record
names the schema contract it accepts, as a `__manifest` publication carries
it since stamp v13. A content write is staging, one detached commit per
touched table in parallel, and one conditional create.

What does not change: per-type Lance datasets, RFC 0067's detached staging
as detached-only tables leave it, the query and merge semantics, policy enforcement, the HTTP
and CLI contracts, and every public error type.

## Motivation

Every graph commit today is a merge-insert into a Lance dataset, and every
writer and reader that needs current state folds that dataset's rows.
Measured on this branch with the cost harness, one write at shallow depth
performs 23 object reads, 18 of them on `__manifest`. The fold grows with
uncompacted history: RFC 0013 traced production writes of tens of seconds to
it, and a production server measured on 2026-09-04 spent most of a
multi-second one-row write reloading `__manifest` across hundreds of object
reads. RFC 0024 tried to make current state flat with head rows inside the
same dataset and rejected its own candidate because a filtered scan over a
history-sized Lance table is still history-sized physical work.

Upstream reached the same conclusion for its directory catalog: per-commit
writes through a Lance table measured 0.34 to 1.1 operations per second at
scale and flat under concurrency (#7176), and the mechanism was removed
(#7222). The replacement shape upstream converged on, and the one this RFC's
author proposed there (#7260), is a small immutable record published with
put-if-not-exists. The maintainer objection to that proposal was that a log
cannot fence external fast-path writers who never consult it. Inside the
graph that objection does not apply: the record is the only publication door
for graph state, and under RFC: Detached-only tables nothing writes a graph
table's linear history after the table is created.

RFC 0067 removes the recovery liability but leaves this cost in place. This
RFC removes the cost and, with it, the last piece of engine-owned Lance
machinery that exists only to coordinate other Lance datasets.

## User and operational behavior

- Every public API, error type and CLI command keeps its contract. Commit
  ids, branch names, snapshot addressing by commit id, and change-feed
  cursors keep their meaning.
- A write acknowledges after its record is durable. A lost response is
  resolved by reading the record at the attempted sequence and comparing
  its commit id; the engine reports success or a typed conflict, never
  unknown, unless the read itself fails.
- Reading a branch costs at most one hint read, a few existence probes, one
  checkpoint read and up to one checkpoint interval of record reads, at any
  history depth. Time travel to a commit costs the same from the checkpoint
  at or before it.
- `branch create` writes one stream metadata object and one checkpoint.
  `branch delete` marks the stream retired with a conditional update. Both
  are visible to every process at once.
- `cleanup` prunes records and checkpoints past the retention horizon that
  no retained checkpoint depends on, and protects table versions pinned by
  any retained record or checkpoint: the tracing collector of RFC:
  Detached-only tables takes its roots from retained records and checkpoints
  instead of retained `__manifest` versions. `--keep N` keeps its meaning
  over graph commits.
- `repair` reports a hint behind the true latest, a missing checkpoint, and a
  record whose parent is not the previous record, and can rewrite the hint
  and checkpoint. It never rewrites a record.
- Records and checkpoints are JSON and readable with any object-store
  client, which is the operator's view of graph history.
- Graphs written by this format refuse older binaries by storage stamp.
  Existing graphs reach the format through the offline conversion route or
  by export and rebuild.

## Design

### Layout

```text
{graph root}/_graph/
  streams/{stream}.json                  stream metadata (branch authority)
  {stream}/{u64::MAX - seq:020}.commit   one record per graph commit
  {stream}/checkpoints/{seq:020}.json    every table pin at seq
  {stream}/latest.json                   hint: {"seq": N}
```

A ***stream*** is one branch life, named `{logical}.{ULID}` as RFC 0042
names native refs today. The logical name stays the only public identity.
Reverse-sorted record names make the newest record sort first on stores
that list lexicographically (S3, Azure, GCS, memory); the local filesystem
and S3 Express do not, which is why the hint exists.

### Record

```json
{
  "format": 1,
  "seq": 4127,
  "commit_id": "01K5...",
  "parent_commit_id": "01K4...",
  "stream": "main",
  "actor": "act-ragnor",
  "created_at": 1757900000000,
  "schema": {"identity_domain": "...", "ir_hash": "sha256:...", "identity_version": 9,
             "content_hash": "sha256:..."},
  "pins": [
    {"stable_table_id": 12, "incarnation": 3, "table_key": "node:Person",
     "target_version": 1201, "staged_version": 9223372036854775901,
     "transaction_uuid": "...", "entity_count": 48012}
  ],
  "registrations": [], "tombstones": [], "renames": [],
  "lineage": {"kind": "merge", "source_stream": "review.01K3...", "source_seq": 88,
              "merged_parent_commit_id": "01K2..."},
  "idempotency_key": null
}
```

A record is O(touched tables); probe 14 measured 765 bytes for three
tables. Unknown fields are ignored on read and preserved on rewrite; a
`format` above the reader's is refused before interpretation, as the
recovery sidecar schema is today. `idempotency_key` is the surface issue
#513 asks for and is out of scope here except for the field.

`schema.content_hash` names the accepted contract's source and IR, archived
under `__history/schemas/<sha256>.schema` with a conditional create before
the record, the object a stamp-14 commit names today. Content-addressed and
immutable, the archive is unreachable until a record names it, so a record
publishes its schema contract with its pins, and schema acceptance needs no
step after publication.

### Checkpoint

A checkpoint is the complete fold at one sequence: every live table's pin,
its registration and its version metadata, plus the schema identity, its
content hash and the head commit id. Probe 14 measured 38 KB for 217 tables. The publisher
writes a checkpoint after its own record whenever `seq` is a multiple of the
interval, best effort; a checkpoint is a pure function of the records up to
its sequence, so a missing or torn one is rebuilt from the previous
checkpoint and the records between. Checkpoints are derived state under
invariant 7 and never publication authority.

### Publication

1. Capture authority exactly as RFC 0022 §2 describes, from one
   reconstruction of the stream: hint, probe upward, checkpoint, records.
   The captured token is the latest sequence and its commit id.
2. Prepare, validate and stage detached effects as RFC 0067 describes,
   and archive a changed schema contract's content (see Record).
3. Write the record at `seq + 1` with `write_text_if_absent`. Success is
   the graph commit. `Ok(false)` means another writer published `seq + 1`:
   discard the attempt and re-prepare from the new state, exactly as a
   read-set change does today. A strict API returns the typed conflict.
4. On an error whose outcome is unknown, read `seq + 1`. An equal commit id
   is success; a different one is the conflict; an absent object is a failed
   attempt with no effect.
5. Write the hint and, on an interval boundary, the checkpoint, best effort.
   Nothing else follows the publication.

The read set is arbitrated by the record name: two writers who captured the
same sequence contend on the same object, and the object store admits one.
This is the same coarse per-branch token RFC 0022 uses through the
`graph_head` row, with the same consequence that same-branch writers
serialize and cross-branch writers do not. Probe 14 shows eight concurrent
writers landing at sequences 1 through 8 with one winner per number.

### Reading current state

1. Read the hint; treat absence or a parse failure as sequence 0.
2. Probe `seq + 1`, `seq + 2`, ... until absent. A hint stale by `k` costs
   `k + 1` probes (probe 14: a hint three behind cost four).
3. Read the checkpoint at or before the latest; if absent, the previous one.
4. Read the records after it, concurrently, and fold.

The fold is the existing manifest projection with the journal replaced by
records: registrations keyed by stable identity and incarnation, the newest
record wins, tombstones retire, renames preserve identity. RFC 0062's
registration clock is the record sequence.

### Branches

Stream metadata carries the logical name, the parent stream and fork
sequence, the creator, and a retirement marker. `branch create` writes the
metadata with a conditional create, then writes the branch's first
checkpoint, which is the parent's fold at the fork sequence, so a branch
read never touches its parent. `branch delete` sets the retirement marker
with `write_text_if_match`. Listing branches lists the `streams/` prefix and
filters retired entries. The path-prefix rules and the native-ref
retirement metadata of RFC 0042 have no counterpart, because streams are
flat objects.

### Merge

A merge prepares against a captured source `(stream, seq)` and publishes on
the target stream with lineage naming that source, preserving RFC 0058's
retained ancestry and RFC 0063's self-contained lineage. Pointer adoption
copies the source pin. The source is an effect precondition, not a member
of the target's arbitration, exactly as RFC 0022 §6.2 states.

### Time travel and the change feed

A snapshot is `(stream, seq)`. The fold to a sequence uses the checkpoint at
or before it. The change feed's first-parent order is record order, and its
per-commit diff uses the pins of consecutive records as it uses consecutive
manifest versions today; the RFC 0030 cursor encodes the sequence.

### Change sets on the record

Recorded as an idea and its reasoning, not as part of this RFC's scope: the
record is the natural place to keep track of what each commit changed, so
that the change feed and merge stop recomputing it.

What recomputes today. The change feed finds the tables a commit touched by
diffing registration rows across manifest versions, reads each pin's
transaction, walks fragment metadata and deletion vectors, and prunes to
changed rows through the `_row_last_updated_at_version` stamps before
hydrating images. Merge gathers source and target candidates since the base
from deletion vectors and new fragments under the keyed-write byte cap, with
a proven-pure-insert shortcut that walks up to 1,024 versions of history and
a projection cache that refreshes incrementally. Both derive the delta from
Lance physical state on every call; the cost is flat in history only because
of those caps.

What the RFCs already give. RFC 0067 makes every commit's effect on a
table one recorded transaction with a uuid, an exact unit a change set can
name. RFC: Detached-only tables already derives each commit's change set
from that transaction, its fragments for inserts and updates and
`omnigraph.deleted_ids` for deletes, with the exact path as fallback, so
the feed no longer prunes through row stamps. This RFC's record already
carries the pins of every table a commit touched, which alone removes the feed's table-discovery scan, turns
the first-parent walk into reading records by sequence, and makes the
caught-up poll one hint read.

The idea. Extend the record with a per-table change set: the ids the commit
inserted, updated and deleted, inline up to a cap and otherwise in a delta
object the record references, the same shape as the inline-transaction
rule. Every writer knows its id set at publication: a mutation knows the
keys it inserted and updated, the delete planner materializes the exact id
list for its filter, a load knows its batch keys, and a merge has its
classified rows. The set is derived from what was actually committed and
lands in the same conditional create as the pins, so it cannot drift from
the commit it describes. Inserted and updated ids are also recoverable from
the new fragments, so the record strictly needs only the deleted ids, which
the transaction already carries today; on the record they would be read
without opening each table's transaction.

What the consumers would become. The feed serves a page by reading records
from the cursor forward and hydrating only the listed ids: no candidate
scans and no dependence on stamps, with cost
proportional to the change volume on the page. Merge takes the union of
change sets over the records on each side since the base, which is bounded
by divergence and is exactly the set it must classify anyway, then hydrates
current values for those ids; the projection cache and the pure-insert
history walk collapse into record reads. Retention follows records and
checkpoints, so a feed gap maps to record retention rather than to Lance
version pruning. Group commit publishes a batch as one graph commit (RFC:
Group commit), so a batch is one record and one feed block, and change sets
need no per-member split.

Why it is not in scope here. A change set duplicates something derivable
from Lance, and invariant 12 applies: the exact path must remain as the
fallback, and a DST instrument should recompute a sample of records from
physical state and compare before the feed or merge trusts the recorded set
alone. Record size, the spill threshold, and what a change set means for a
merge whose classification reads a Blob column also need their own
evidence. It belongs in an extension of this RFC or a small RFC after it.

### Retention

Records and checkpoints older than the retention horizon are pruned when no
retained checkpoint needs them as its fold base. Table versions pinned by any
retained record or checkpoint are protected from Lance cleanup, which is the
protection `cleanup` computes today with a simpler input. The retired
`__manifest` dataset is deleted by the conversion route once the first
checkpoint is durable.

### Failure classes and durability

A conditional-create conflict is `StorageFailure::Precondition`. A lost
response is resolved by read-back and never surfaced as unknown when the
read succeeds. Durability on return is the object store's; the local
backend stages a complete temporary file and hard-links it, which the
storage crate pins, and does not fsync, which matches Lance's own manifests
on the local filesystem.

## Invariants

- 2 (one publication door): the record's conditional create. Strengthened:
  the door is one object-store primitive with no fold behind it.
- 3 (one coherent view): a snapshot is `(stream, seq)`; a write captures one
  sequence and re-prepares on any movement.
- 4 (publish once): one record per graph commit.
- 5 (recovery is part of the commit protocol): publication has no
  pre-publication durable effect of its own; a lost acknowledgement is
  resolved by read-back; nothing follows the publication: no table-pin
  promotion (RFC: Detached-only tables), and the schema contract is named by
  the record, its archived content unreachable until a record names it.
- 7 (physical acceleration is derived): checkpoints and hints are derived,
  rebuildable, and never authority.
- 11 (bounded, observable): a current-state read is bounded by the
  checkpoint interval; contention retries are bounded and typed.
- 12 (one source of truth): the record stream replaces `__manifest` as the
  graph authority. Lance datasets and the accepted schema remain what they
  are. The invariants text names `__manifest`; accepting this RFC updates
  that sentence.
- 13 (evidence matches the boundary): the gates above.

Deny-list items: "a custom WAL or transaction manager" names machinery
that duplicates Lance's per-dataset transactions. The graph's own commit
journal is already OmniGraph's, implemented today as a Lance dataset; this
RFC changes its representation to the primitive Lance itself commits with
and removes a fold, it does not add a second log. "Cold full-history
reconstruction per request" is what the checkpoint bounds. No queue, no
shadow copy, no process-local lock presented as fencing.

## Compatibility and reversibility

- Wire: none.
- Storage: the next storage stamp after v14. New graphs write records only.
  Conversion of a v14 graph writes one record per graph commit on each
  branch, read from the branch's `__manifest` buffer and its `__history`
  files with the legacy area included, a checkpoint at each branch head,
  stream metadata for each live native ref, and then deletes `__manifest`
  and the commit files under `__history`, keeping the schema archive the
  records name; it runs offline under the same quiescence RFC 0064
  requires of explicit upgrades. Export and rebuild remains the alternative.
- Downgrade: refused by stamp. Reversal to v14 is a fold of the record
  stream into each branch's `__manifest` head and buffer and the `__history`
  files, feasible because records are complete,
  and is provided by the conversion tool in both directions until this RFC's
  implementation reaches complete.
- Support boundaries: unchanged from RFC 0067.

## Alternatives

- **Keep `__manifest` and compact it aggressively.** Compaction bounds
  fragment count, not the semantic row volume, and the merge-insert still
  reads the dataset on every publish. RFC 0013 and RFC 0024 measured this.
- **Durable heads inside `__manifest` (RFC 0024).** Rejected by its own
  Gate A: logically flat, physically not.
- **An external manifest store (DynamoDB or SQL).** Adds a service
  dependency for what put-if-not-exists on the object store already gives,
  which is why Lance keeps its external store optional.
- **Lance namespace directory catalog.** Its `__manifest` is the same shape
  with the same measured cost, which is why upstream removed per-commit
  writes from it.
- **Wait for upstream multi-table transactions (#7264).** Stalled since
  July; would stage on branches and flip through the catalog, which is the
  cost being removed here.
- **One dataset per graph.** Recorded in RFC 0067 as the stronger end state;
  it removes this record too, at the cost of a full rewrite. This RFC is the
  optimum that keeps per-type datasets.

## Evidence and tests

### Probe 14, storage crate, local backend, counting adapter

| Claim | Result |
|---|---|
| Eight concurrent writers each publishing one record by conditional create | sequences 1 through 8, one winner per number, no gaps; losers retried up to eight times |
| Reconstruct latest after 1,000 records with a checkpoint every 50 | one hint read, one existence probe, one checkpoint read |
| Time travel to sequence 437 | 38 objects |
| Hint stale by three | four existence probes, one checkpoint read |
| Record for three tables; checkpoint for 217 tables | 765 bytes; 38 KB |
| Local listing order; bounded listing | unordered; bounds cap, they do not order |

The S3 conditional put is object_store 0.14's default for S3 and the
handler Lance selects for s3, gs, az and memory; the storage crate's
conditional-create tests pass on the memory and local backends.

### Owners to extend

- `write_cost.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`,
  `changes_cost.rs`, `merge_cost.rs`: replace the `__manifest` read curves
  with record-stream curves and the new ceilings.
- `branching.rs`, `point_in_time.rs`, `merge_truth_table.rs`,
  `merge_fast_forward.rs`, `lineage_projection.rs`, `changes.rs`: same
  logical assertions over the new authority.
- `maintenance.rs`: retention of records and checkpoints, protection of
  pinned table versions, conversion round trip.
- `failpoints.rs`: crash after the record and before the hint, before the
  checkpoint; lost acknowledgement resolved by read-back.
- DST: concurrent publishers on one stream, stale hints, missing and torn
  checkpoints, conversion under injected faults.
- Server `s3.rs` and cluster `s3_cluster.rs`, and the Azure suites: the
  race and lost-acknowledgement cases.
- `forbidden_apis.rs`: the record writer joins the durable-call registry;
  the `__manifest` publisher leaves it.

### Acceptance thresholds

- Publication: one conditional create, plus one hint write and one
  amortized checkpoint write, at every depth.
- Current-state read: at most 3 + (interval - 1) object reads at depths 10,
  100, 1,000 and 10,000, on file, S3 and Azure.
- Branch create: at most three objects. Branch delete: one conditional
  update.
- DST: zero lost or duplicated commits across the concurrency scenarios.

## Rollout

1. RFC 0067 and RFC: Detached-only tables landed, with the pin shape this
   record carries (done).
2. Record, checkpoint, hint and stream readers and writers behind the next storage stamp,
   exercised on new graphs; the cost instrument checked in.
3. Conversion tool in both directions with its round-trip evidence.
4. Retire the `__manifest` publisher, state fold, namespace and layout
   modules, the `__manifest`-era upgrade routes for new graphs, native `__manifest`
   branches, and the clone adapter; update `writes.md`, `architecture.md`,
   `invariants.md` §12, `versioning.md`, `control-plane.md` where it names
   manifest objects, and the release notes.

Each stop leaves `main` shippable; the stamp gates activation.

## Unresolved questions

- Checkpoint interval: fixed at 50, or size-based so wide catalogs
  checkpoint less often.
- Whether stores that list lexicographically should skip the hint and use a
  bounded listing as the primary latest lookup.
- Whether stream metadata needs a graph-level index object, or listing the
  `streams/` prefix suffices for the branch counts the server expects.
- Group commit at the branch gate: RFC: Group commit (draft, PR #785)
  publishes a batch as one graph commit, so it would be one record
  with one commit id. Decided there.
- Whether the record carries per-table change sets (see Change sets on the
  record), and if so whether only deleted ids or the full inserted, updated
  and deleted sets, and at what size they spill to a delta object.

## Amendment: legacy commits of the stamp-13 upgrade

This section is implemented and stands apart from the draft above. Storage
stamp 14 kept `__manifest` and bounded its history another way: a branch's
`__manifest` holds the head and a byte-bounded buffer of commits, and older
commits are released into immutable Lance files under `__history`
([versioning.md](../dev/versioning.md#current-storage-contract)). The offline
upgrade from stamp 13 ([RFC 0064](0064-explicit-storage-upgrades.md)) has to
keep every pre-upgrade commit addressable by its id. Those ids are ULIDs, not
the `hb1.<block>.<slot>.<nonce>` ids whose name is their address, so the
upgrade writes a ***legacy area***, `__history/legacy/`, once, and nothing
writes there again.

### Legacy area layout

Every object is immutable, created with `put_if_absent`, and never deleted.

| Object | Path under `__history/` | Format | Content | Bound |
|---|---|---|---|---|
| Data file | `legacy/data/<n:08>.lance` | Lance V2_2 file with the columns of every `__history` extent | the complete `HistoryRecord`s of one writer's consecutive own commits, in chain order | commit fields ≤ `HISTORY_RELEASE_BYTES` (256 KiB); record bytes ≤ `LEGACY_FILE_RECORD_BYTES` (2 MiB); rows ≤ `HISTORY_BLOCK_SLOTS`; a lone larger record takes its own file |
| Directory | `legacy/locator/directory.oglx` | `OGLD0001`, layout version, source stamp, attempt ULID, counts, then one entry per data file (version range, rows, SHA-256 of its records), per id shard and per writer shard (key range, entries, SHA-256), the sorted absent merged parents, and a trailing SHA-256 | the whole locator | ≤ `TAIL_BYTES` (512 KiB) |
| Id shard | `legacy/locator/ids/<n:08>.oglx` | `OGLI0001`, count, 24-byte entries sorted by binary ULID: id, file, row | commit id to (file, row) | ≤ `TAIL_BYTES`: 21,844 entries |
| Writer shard | `legacy/locator/writers/<n:08>.oglx` | `OGLW0001`, entries sorted by SHA-256 of the native name: kind (`Main`, `Live`, `Retired`, `Orphaned`), name, parent name and version, head version and id, first file and file count | one entry per writer with own commits | ≤ `TAIL_BYTES` |
| Schema content | `schemas/<sha256>.schema` | the existing `OGSC0001` archive | each distinct contract a legacy record names | existing |

At about 430 bytes of commit fields and 6 KiB of record per commit the 2 MiB
cap binds first, near 340 commits per file:

| Legacy commits | Data files | Id shards | Directory | Lookup by id, first / later / warm | Full lineage walk |
|---|---|---|---|---|---|
| 10,000 | 30 | 1 (234 KiB) | about 2 KiB | 4 / 1 / 0 GETs | 1 LIST + 30 suffix GETs |
| 1,000,000 | about 2,950 | 46 | about 153 KiB | 4 / 1 or 2 / 0 GETs | 3 LIST pages + 2,950 suffix GETs |

The second row sizes the locator only: the stamp-13 census refuses above
about 75,000 commits on one lineage. The directory reaches `TAIL_BYTES` near
10,000 files, about 3.4 million commits. Two levels stay because a
single-level map of a million commits is 24 MiB, above what the extent cache
holds.

### Why this is not a deny-list shape

The [deny-list](../dev/invariants.md#deny-list) rejects a storage primitive
Lance already owns, maintained parallel truth and cold full-history
reconstruction per request. The legacy area is argued against each.

- **The records are Lance files.** A data file is an extent like a native
  block: the same columns, the same writer (`write_extent`), the same reader,
  one suffix GET of `TAIL_BYTES` for its whole lineage. Only its key and its
  validation (one writer, a first-parent chain, no `hb1` id) are new.
- **The locator is a map, written once.** The Lance-owned alternative is a
  dataset with a scalar index on the commit id. That brings a manifest, a
  version chain, a transaction log and index files for content that is
  written by one offline, fenced process and then never changes; a lookup
  would read a manifest and index pages where the flat form reads one shard
  of bounded size. No object here is updated, so there is no transaction to
  manage, no log and no concurrent writer. The shape is the one the merged
  schema archive already has: a magic, a byte-exact re-encode check on decode
  and SHA-256 digests.
- **It is not parallel truth.** After a ref is converted its `__manifest`
  holds the head alone; the legacy data files are the only copy of the
  earlier commits. The locator is derived from them, and cannot drift
  because neither is ever rewritten: the directory carries the digest of
  every shard and of every file's records, the upgrade intent carries the
  digest of the directory before the fence, and the upgrade reads every
  object again against those digests before it activates the graph. A
  converted head that is later released as a singleton is an equal copy,
  proven before the fence, and full reads require copies to be equal.
- **No read reconstructs history.** A lookup by id on a handle that has seen
  the directory is one shard GET and one data-file read; a full lineage walk
  is one LIST and one suffix GET per file, as for native blocks. A root born
  at stamp 14 pays one directory miss per handle on a commit id that is
  genuinely absent, and nothing otherwise.

Two details follow from the substrate. All locator objects share the prefix
`legacy/locator/` because a local `put_if_absent` can leave a
`<path>.tmp.<uuid>` file behind; the extent listing skips that one prefix and
still refuses any other unknown key. And the absence of the directory is the
one absence a handle keeps: the directory is written before activation and
never again, no handle can exist before activation because the pending key
refuses every open, and a root restore clears the handle's cache.

A merged parent that exists nowhere in the source (a merge followed by a
branch delete before stamp 8 reclaimed the tree) is kept verbatim on its
commit and listed in the directory; lineage reads treat a listed id as known
absent. A first-parent gap refuses the upgrade.

Reversal is restoring the pre-upgrade backup. A stamp-14 binary built before
this amendment reads no `legacy/` and reports pre-upgrade ids as not found.

## Decision log

- 2026-09-15: drafted as the sequel to RFC 0067 after the write-path
  investigation; probe 14 recorded as initial evidence.
- 2026-10-04: amendment added for the legacy area of the stamp-13 to stamp-14
  upgrade: Lance data files plus flat locator objects under
  `__history/legacy/`, in place of a Lance dataset with an id index. The
  draft's record stream is not changed by it.
- 2026-10-04: the legacy area of the amendment also holds the pre-upgrade
  commits of a stamp-8 or stamp-9 source (release v0.11.0), which the same
  upgrade now converts ([RFC 0064](0064-explicit-storage-upgrades.md)). The
  layout, the bounds and the argument of the amendment are unchanged; the
  directory's source stamp field is 8, 9 or 13. Two statements of the
  amendment hold for a stamp-13 source only. The schema content row lists
  each distinct contract a legacy record names: for a stamp-8 or stamp-9
  source that is one contract, the one at the graph root at the upgrade,
  named by every record. And the census bound of about 75,000 commits on
  one lineage is derived from the stamp-13 overwrite publish; a stamp-8 or
  stamp-9 `__manifest` keeps every row and every version, and its bound in
  commits has not been derived. The amendment's heading keeps its name so
  that existing links resolve.
- 2026-10-08: consistency pass after RFC: Detached-only tables (storage stamp
  v11), which removed promotion.
  - Promotion is gone from the dependency, the summary, the publication
    steps, the invariants, the test windows and the rollout.
  - The stamp becomes the next one after v14, and conversion starts from a
    v14 graph: each branch's `__manifest` head and buffer and its `__history`
    files, the legacy area of the amendment included.
  - The change-set section now starts from the transaction-carried change
    sets detached-only tables ship.
  - The group-commit question points to RFC: Group commit, and the
    change-set section follows its one commit per batch instead of a
    per-member feed.
  - "Nothing follows a publication" now rests on two facts: no table-pin
    promotion, and a record that names its accepted schema contract by
    content hash, archived before the record as stamp 14 archives it.
  - The stale "Number provisional" banner and a self-referential blocker
    ("RFC 0068 accepted") are removed.

  The record's design is unchanged.

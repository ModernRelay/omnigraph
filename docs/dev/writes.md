# Graph write protocol

**Audience:** engine and storage contributors
**Authority:** current graph-visible write path; what a crash leaves and who
finishes it is in [recovery.md](recovery.md)

Every successful graph-content write has one visibility point: a conditional
`__manifest` publication. Table effects happen earlier, but as detached Lance
commits of the pinned base that nothing references until that publication
([RFC 0067](../decisions/0067-detached-table-commits.md)). A detached commit is
the table's version for its whole life: nothing replays it onto the table's
linear history, and a graph table's linear HEAD stays at its creation version
([RFC: Detached-only tables](../decisions/2026-09-21-detached-only-tables.md)).

## The protocol

```text
capture accepted authority
        ↓
prepare logical change and validate it
        ↓
stage exact Lance transactions (no HEAD movement)
        ↓
acquire shared schema permit → branch → sorted-table gates, recheck the complete authority
        ↓
commit each participant as a detached version of its pin
        ↓
publish every pin + accepted schema contract + lineage in one __manifest CAS
```

An error before the manifest CAS leaves the graph unchanged (`E2`): the detached
versions are unpublished staging, which `cleanup`'s collector reclaims once
their recorded publication authority is provably gone. A mutation, load or
Blob write prepares again from its request
([Mutation and Load](#mutation-and-load)); other writers return the conflict
and the caller retries from a fresh snapshot. A successful publication is the whole write: no table
effect has post-publication work. A lost acknowledgement is resolved
by re-issuing the identical manifest request and reading the attempted
version back; unavailable readback stays indeterminate.
No writer arms a recovery record, and no write replans around a partial state, because
no partial state is ever visible. Schema apply and the system-column upgrade
include the replacement schema-contract row in the same publication as their
table references. A later `RecoveryRequired` outcome can identify a committed
publication; it does not leave durable contract files to install.

## Captured authority

A write attempt captures one immutable `WriteTxn` containing the accepted
schema/catalog, target graph branch, optional graph head, native branch
identity, table-incarnation identities, and expected table versions. Every
planning and validation step uses that view.

Ordinary non-bound branch writes retain their capture in the same one-entry
coordinator cache used by branch merge. Branch merge also uses the captured
target for physical table opens and publication. It never changes the `Omnigraph` handle's active branch while the
merge runs. Publication reuses the active coordinator, or takes the cached
non-active target coordinator, only when its branch identity, graph head, and
manifest version match the captured transaction; otherwise it opens the target
coordinator from durable state. The publisher independently reads fresh authority
and enforces the exact graph-head precondition on every attempt. Successful
publication returns a taken coordinator to the one-entry merge cache; failure
drops it. The intent nonce and timestamps are minted for the captured branch
without reloading manifest history; the publish wraps the nonce into the
addressable commit id `hb1.<block>.<slot>.<nonce>`. The schema and branch gates serialize
conflicting control operations. A merge holds its target's branch gate and
per-table queues through publication, and its source's branch gate only until
the source snapshot is captured, tagged and proved still current; it takes no
per-table queue on the source. Both gates are taken in sorted branch order, so
a merge whose source sorts first (`main` always does) holds the source gate
while it waits for a target another merge holds. Once the source is captured,
writes, index builds and deletes on the source do not wait for the merge, and
the merge's final re-read still refuses a source whose incarnation or schema
changed.

Native branch creation captures its source after the control gates
(`capture_branch_control_source` in `crates/omnigraph/src/db/omnigraph.rs`).
A start at a graph commit (`branch create … from commit "<id>"`) clones the
`__manifest` version that published it; its capture and the proof it runs
before any effect are `capture_commit_start_source` and `prove_commit_start`
there, and `create_branch_at` in `crates/omnigraph-catalog/src/lib.rs`.

At or above format 18 create admission reads `main`'s branch rows, held with
no request: a live row whose logical name is the target, or an ancestor or
descendant of it, refuses `branch_exists`; the row's publication on `main`'s
version decides between two creates of one name, and the loser reloads and
refuses before it has written a tree. The tree is then written as a shallow
clone of the fork point (`create_branch_tree`), and a tree no row names is the
collector's (RFC 2026-11-19, "Branch existence in the catalog"). Below the
stamp admission lists native ref names and reads bodies only for the source,
the target's incarnations and logical ancestors or descendants; physical path
checks include retired ref names; the exact target ref must be absent, and an
empty listing of its exact native tree allows creation without forced
reclamation; a nonempty tree and clone-only recovery keep Lance's full
dependency and tag checks.

For a captured main source at a canonical attached V2 manifest, the clone
handler reuses the known location, size and ETag while the clone call is
active. Only default built-in object-store handlers use this path. Custom
handlers, named sources, V1 or detached locations, and missing size or ETag
keep normal resolution. Lance still reads the source manifest and checks its
existence before publishing the new ref. This avoids repeated location HEADs;
it does not protect an input from concurrent cleanup.

Ref-name listing still grows with the physical ref inventory. Cold recreation
of the same logical name reads its retained incarnations to validate retirement
and liveness; source and hierarchy incarnations also remain relevant.
Unrelated retired ref bodies add no reads to an ordinary fresh create.

After a content publication, the publisher returns the projection (the
in-memory `__manifest` state folded from the journal) it already built from the
successful attempt's freshly read base. The coordinator retains
it only when that exact base matches its last coherent view and its graph
cache has adopted the published lineage. A foreign advance, unsupported base,
or failure before lineage adoption leaves the full-refresh fallback armed.
Registration replacement, rename, tombstone, and same-version physical-owner
handoff use the existing complete fold. This is disposable process memory;
`__manifest` remains the only durable graph authority.

`GraphNamespacePublisher` keeps the rows of its last acknowledged publication
(`PublishedRows` in `crates/omnigraph-catalog/src/publisher.rs`). The capture
validates the contract row of the manifest version it selected
(`open_write_txn` in `crates/omnigraph/src/db/omnigraph.rs`, or
`ensure_schema_state_valid` for control and maintenance entry points).

Finalization acquires the root-shared gate order:

1. the schema gate — a shared permit for ordinary writers (a
   contract-lifecycle pass such as schema apply or the system-column
   upgrade, branch create, whose namespace inventory no CAS covers, and the
   passes that reload the accepted view, such as `refresh` and
   `sync_branch`, take it exclusively, so cross-branch writers do not
   serialize on it; see
   [RFC 2026-09-18-shared-schema-gate](../decisions/2026-09-18-shared-schema-gate.md));
2. target branch;
3. `(table_key, branch)` queue entries in sorted order: the touched tables for
   a mutation, load or index build, the whole catalog for merge, optimize and
   schema apply;
4. coordinator publication.

These gates order work inside one process. Correctness still depends on the persisted manifest
precondition and the exact Lance transaction identity each pin records. A
retryable pre-effect attempt discards all staged work, captures a new
`WriteTxn`, and repeats boundedly; it never reuses batches against a new base.

## Writer epoch

A branch's writer epoch is the `writer_epoch` control-fact row of its own
`__manifest` ([storage format v17](versioning.md)). A branch without the
row, or with a row its fork source wrote, is at epoch 0. A process holds an
epoch for a branch only after a claim it published landed there. The holder
registry (`omnigraph-catalog/src/writer_epoch.rs`) is process-wide, keyed by
root identity and branch name, and records the native branch name of the
claim (the row's at or above format 18, the ref's below); no reload raises an
entry, a refusal clears it, the process's own delete of the branch drops it,
and otherwise it ends with the process.

Every publication that moves a branch's head asserts the epoch.
`publish_attempts` runs the check on each attempt over the rows that attempt
loaded, before `check_publish_precondition` and before the return of a
publish that would write nothing. A mutation that stages nothing returns
before it reaches the publisher, so the check does not refuse it; on a branch
its process holds it probes the head before it answers (below). A process
holding nothing proceeds on rows at epoch 0 and a process holding `E`
proceeds on rows at `E`. Any other pair
is refused as `WriterFenced { held, read, current }`, which is not retried
and clears the refused entry. The check issues no request: it compares the
loaded rows with the registry, and a process that holds no branch resolves
no key. An entry whose native name is not the loaded chain's is dropped, so
what a process held on a deleted branch never counts on a branch created
again under its name. `main` has no native name: after a root is created
again or restored while a process still holds its `main`, that entry is
cleared by the first refusal it meets, and until then it counts whenever
the root's epoch equals it.

A process that holds its handle's bound branch does not probe that branch's
head at the sites below: no other process's publication can land on it, and
the writers the fence does not cover are listed under
[what the fence covers](#what-the-fence-covers). The holder
entry records the `__manifest` version of the process's last publication on
the branch, the claim included. A handle whose installed view is at or above
that version reads and writes from the installed head with no request; a
handle below it, one that did not run that publication, reloads first, also
with no probe. The sites are a read that targets the branch
(`ReadTarget::Branch`), `resolve_snapshot`, a write's open and its
revalidation, which ask `GraphCoordinator::head_is_current`, its probe
beside staging, which asks `held_head`, and a plain branch listing on a
`main`-bound handle at or above format 18, which lists the held rows. A merge's authority capture and
revalidation, a branch create's capture of its source and the change-feed
poll still probe the bound branch. A publish attempt that returns an error,
or whose future is dropped, withdraws the recorded version, and a claim
withdraws it before its put: each may have landed, or met a head that moved,
with no record of it. Every handle then probes as before until the process's
next publication lands. A write that stages nothing reaches no publication,
so when its open took the recorded version for the head it probes once
before it answers; a head that is neither its view nor the version on record
now is prepared again on a fresh view. A held branch that is not the handle's
bound branch, and every branch the process holds nothing on, is probed as
before.

Two changes by another process reach the owner by no path but a probe, so
its handle serves reads from its installed snapshot until a publish of its
own loads the branch. After another process claims the branch, that publish
is refused, which clears the holder; a write of the replaced owner that
stages nothing probes and is prepared again on a fresh view. After another
process deletes the branch (a tombstone that asserts no epoch at or above
format 18, a ref delete below it), that publish is refused `branch_deleted` by
the tombstone its load meets, finds the branch gone, or finds the branch
created again under its name: there the entry's native name is not the loaded
chain's, so the entry is dropped and the publish proceeds at epoch 0. The
process's own delete of a branch withdraws the recorded version before it
tombstones the chain (retires the ref below the stamp) and drops the entry
once the retirement has landed, so its handles probe.

`Omnigraph::claim_branch_writer_as` claims under `branch_claim` on the branch
it names, the shared schema permit and the branch gate, through a
coordinator it opens under those gates. The version a claim writes is a
control-fact publication; a publication that changes `table` rows or the
schema contract still requires lineage. How a claim lands, races and settles
an unanswered put is the doc of `claim_writer_epoch` in
`crates/omnigraph-catalog/src/publisher.rs`.

Detached candidate versions (an unpublished commit or an unattached snapshot
before publication) carry the row and assert nothing. Writes outside the
publisher are not fenced ([what the fence covers](#what-the-fence-covers)).
Branch control at or above format 18 is three publications (RFC 2026-11-19,
"Branch existence in the catalog"): the branch row a create publishes on
`main` and the retired row a delete publishes there assert `main`'s epoch, so
`branch create` and `branch delete` are refused `writer_fenced` beside a
process that claimed `main`; the tombstone a delete publishes on the branch's
own chain asserts no epoch, since it ends the chain, so a delete of a branch
another process holds proceeds, the holder's next publish is refused
`branch_deleted`, and the chain keeps its last `writer_epoch` row.

### What the fence covers

The writer epoch fences publications that move a branch's head, on a branch
some process claimed. Only the embedded engine claims: no command or route
does, so a claimed branch refuses the server and the CLI. Branch create and
delete are fenced through `main`'s epoch at or above format 18: the branch
row and the retired row are publications on `main`, and the tombstone on the
branch's chain asserts no epoch.

Not fenced: the collector's deletes and its removal of a retired row's
entry, `optimize`'s configuration commit, the stamp-only route of
`omnigraph upgrade`, and below format 18 the ref metadata and ref deletes.
One mutation-capable writer process per cluster remains the supported
topology ([contract](contract.md#gaps) `G2`).

## Writer adapters

All current graph-visible writers share the publication door but have different
physical-effect proofs:

| Writer | Physical adapter | Publication |
|---|---|---|
| Mutation / Load | One exact staged keyed, overwrite, or delete transaction per touched table | One graph commit |
| SchemaApply | Existing-table metadata-only schema evolution (a detached `Project` for renamed and dropped columns, then a detached `Merge` adding nullable columns over the unchanged fragments; no data file written, no row read); original added-type Create or qualified reuse/detached replacement; complete table delta and replacement schema-contract row | One main-branch graph commit |
| BranchMerge | Per table, a pointer switch or a chain of detached chunk commits ([merge publication](merge.md#publication-and-recovery)) | One target-branch graph commit |
| EnsureIndices / full-text rebuild | One detached `CreateIndex` batch per productive table, published as a pin like a mutation's effect (RFC 0067); ordinary ensure leaves untrainable vector work pending, explicit FTS rebuild replaces postings from rows | One graph publication when work lands |
| Optimize | One detached compaction `Rewrite` per productive table (Lance's plan plus every fragment still holding a dropped column), chained with a detached whole rebuild of each index whose coverage lags and a detached build of each declared-but-unbuilt index, published as pins (RFC 0067) | One main-branch graph commit with an exact CAS on the pins the batch was planned from |
| Mutation or BranchMerge `as commit` | The writer's own physical adapter, unchanged | One unpublished commit ([RFC](../decisions/2026-10-10-unpublished-graph-commits.md)): the complete result rows written as a detached version `D` of the target's `__manifest` with the recipe of their publication in the transaction property `omnigraph.unpublished_graph_commit`, addressed by the reserved root tag `unpublished_<id>` created put-if-absent at `D` under the same `ExactGraphHead` authority; the branch head is unchanged, a merge keeps its merge-input tags, and `commit publish "<id>"` later replays the recipe through this same door under the captured authority, required to allocate the same id |
| Mutation `as unattached branch [on <id>]` | The writer's own physical adapter over the base's table pins: a root stages against the recorded target's live head, a child against its base snapshot's detached pins (`WriteTxn.unattached_base`, `revalidate_unattached_base`) | One unattached snapshot ([RFC](../decisions/2026-10-11-unattached-graph-branches.md)): the complete result rows written as a detached version `D` of the recorded target's `__manifest` with the version 2 candidate property (`UnattachedCommitProperty`: id, root, parent, no row clock), addressed by the reserved tag `unattached_<root>_<C>` created put-if-absent at `D` and acknowledged only after the root authority is revalidated (`stage_unattached` + `confirm_unattached`); the branch head is unchanged, a child's `parent_commit_id` is its base, `commit publish` answers `unattached_cannot_publish` (409), `commit drop` cascades descendants-first, and every tag of a root expires with the captured target authority; never published |

Native graph-branch create/delete is a control exception. At or above format
18 the `branch` row of `main`'s catalog is the logical authority (RFC
2026-11-19, "Branch existence in the catalog"): a tree no live row names is
derived physical state the collector reclaims, and the row, the tombstone and
the retired row are control facts that go through the publisher. Below the
stamp `BranchContents` is that authority, and clone/delete residue is
reclaimed only when its target is provable from it. Neither invents an
alternate graph-content publisher. Each branch life owns a native name
`{logical}.{ULID}` (see [the branch row in the catalog](../decisions/2026-11-19-branch-row-in-the-catalog.md)),
the key of its row and the path of its tree; a recreated branch therefore
never shares a path with its dead predecessor, and what the predecessor left
(its tree, its unpublished stagings, and the table forks a pre-v11 branch
owned) is reclaimed by `cleanup`.

## Mutation and Load

`MutationStaging` accumulates read-your-writes batches and delete predicates
in memory. It performs all type, value, uniqueness, endpoint, cardinality, and
resource validation before staging. `stage_all` opens each touched table at
its pin through the read-handle cache (the registration's `staged_version`
when it carries one) and produces one exact transaction per table without
moving HEAD. `commit_all` admits each dataset it commits to that cache under
the key its publication will register, so the next read or write of the table
opens no manifest; once a publication names the new pins, the writer demotes
each replaced base pin in that cache: it stays held for a reader or fork still
at that version while the cache's byte budget has room, and is the first out
under pressure, so a steadily written table's history never displaces a live
pin; the budget bounds the decoded manifest bytes the handles keep alive,
since a manifest lists every fragment. `commit_all` enters the gates,
revalidates the complete
authority (for the handle's bound branch, the probe taken beside the staging
puts serves when it answered current and no publish has installed on the
handle since, and a process that holds the branch's
[writer epoch](#writer-epoch) with its last publication on record probes
nowhere; for another branch, the
captured manifest probe first: an
unchanged version and branch identifier keep the captured authority, a
mismatch is a fresh coherent capture), and commits
every participant as a detached version of its pinned
base: no recovery sidecar is armed, a table's linear HEAD never moves, and
nothing can rebase. Every detached commit records the authority it was staged
against in its transaction properties,
`omnigraph.staged_against_branch_incarnation` and
`omnigraph.staged_against_graph_head`, and a delete records the ids it
removed under `omnigraph.deleted_ids` (inline up to 64 KiB) or
`omnigraph.deleted_ids_path` (`table_store.rs`); the collector and change
discovery read them. The manifest then publishes every pin as
`(base + 1, staged_version, transaction_uuid)` in one CAS; a publish that
loses the CAS returns the plain `ReadSetChanged`, and the staging it leaves
is unpublished until the collector reclaims it. A write carrying the caller's
`--if-commit` head turns that loss into `PreconditionFailed` naming the head
the publisher read, the answer the under-gate check gives, so the caller sees
one refusal wherever the race lands. A publish by another handle or process
between the beside-staging probe and the gates is refused there, after the
detached commits, where the under-gate probe refused before them: the price of
the lost race is one reclaimed detached commit per table. The probe reads only the
held native ref, so a second live incarnation of the branch name (two
processes creating it at once) is refused by the publisher's own branch
resolution after the detached commits, not before them; the graph is
unchanged and the staging is reclaimed the same way. The published pin is final:
readers open `staged_version` directly, and `published_dataset_version`
keeps its number, `base + 1`, as the table's logical version inside its
`__manifest` lineage without naming a Lance version. Every registration
records `omnigraph.last_linear_version`, the highest linear version a pin
ever reached (`1` for a table created under v11), and every writer that
rebuilds the row copies it forward (`TableVersionMetadata`,
`crates/omnigraph-core/src/metadata.rs`). A writer whose captured snapshot predates another
publication loses the CAS and reprepares (`ReadSetChanged`). Existing-table
writes do not read linear HEAD; the collector lists a foreign linear commit
above `omnigraph.last_linear_version` under `foreign_versions`
(`collector.rs`, `foreign_versions`). Added-type admission can
inspect an unregistered deterministic path's HEAD only to prove it remains
its qualified original empty version 1; it never adopts an advanced HEAD.

Detached manifests are reclaimed by the tracing collector `cleanup` runs;
its roots, sweep rule and the two properties every run holds are the module
doc of `crates/omnigraph/src/db/omnigraph/collector.rs` (see also
[Maintenance](../user/operations/maintenance.md#cleanup)).

Optimize plans each table's compaction once, from its pin
(`TableStore::plan_table_compaction`, whose doc comment says how the plan
erases a dropped column). That plan is measured against
the run's ceilings and handed to `stage_compaction`, which refuses a plan of
another version. It stages the rewrite detached
with fragment ids above the base's high-water mark, so it needs no
`ReserveFragments`; a lagging scalar or vector index is rebuilt whole as a
detached commit chained on the rewrite under its name (Lance 13 folds only
through a linear commit). A vector index keeps its partition count unless its
segments cover no fragments, in which case it uses one partition, as a new
declared index does. A declared index the table lacks is built as a third
link. A definite index failure leaves completed links eligible for the batch
publication, including an index build whose final coverage read failed. The
table reports `optimized` with reason `index_failed` when it has a committed
link. A definite failure before the first link commits fails the table.
Uncertain effects fail the run. The batch publishes once with an exact CAS on
every planned pin, and a pin a concurrent writer moved fails the run with a read-set
conflict so the next run re-plans. A failure before publication leaves
unpublished staging the collector reclaims once it is dead.

A mutation, load or Blob write prepared before such a publication is carried
over it ([RFC](../decisions/2026-11-10-writes-carried-over-row-preserving-publications.md)).
Under its gates, when the head moved, `revalidate_write_txn_carrying` checks
that the schema contract, the table set and every registration's identity,
location, native ref and row count are unchanged, and that every moved pin
reaches the captured one through `Rewrite` and `CreateIndex` commits alone
(`table_store::row_preserving_chain`, at most
`ROW_PRESERVING_CHAIN_MAX_LINKS` per table). The write is then re-anchored on
the current head before any effect. A table it only read keeps its moved pin;
a staged table goes through Lance's `TransactionRebase` over those commits
(`table_store::rebase_staged_write`), records the current pin as its read
version, and commits detached there, so the compaction stays and the table's
history stays one chain. An insert, an append and an overwrite carry; an
update, delete or upsert of a row in a fragment a rewrite replaced is a
conflict Lance reports, and the attempt is sent back, as one overtaken by a
row change is. So is one whose chain cannot be read: the links before a
table's pinned tip are named by no `__manifest` version, and a `cleanup`
between the write's capture and its gates deletes them, which is an
unreadable chain and not a storage fault. A caller's `--if-commit` head and
an unattached base are not
carried. On a branch its process does not hold, neither is a publication by
another handle that lands after the beside-staging probe: the publisher's CAS
refuses that one, as above. On a branch the process holds, a write takes no
probe, and a publication by any handle of the process is seen under the gates
([writer epoch](#writer-epoch)), so it is carried.
`QueryIoProbes::writes_carried_over_row_preserving` counts the attempts
carried, each staged table rebased.

A write sent back before its effects prepares again from its request; the
rule, its bound and its preparation scope are the module doc of
`crates/omnigraph/src/db/reprepare.rs`. The next attempt holds the shared
schema permit and the branch gate, `commit_all` adds the table gates, and no
maintenance run waits for writers; what the gated attempt costs other
writers is in that module doc too.

Optimize's authority capture, run bounds, cancellation and before-publish hook
are the docs of `AuthorityCapture`, `OptimizeLimits`, `TableCeilings` and
`OptimizeRequest` in `crates/omnigraph/src/db/omnigraph/optimize.rs`.

The index writer (`ensure_indices`, which `omnigraph index build` runs on any
branch, and the explicit full-text rebuild) follows the same protocol: it
opens each productive table at its pin, stages the complete BTREE/FTS/vector
batch before the gates, commits every batch as a detached version of the pin
and publishes the pins once (`maintain_indices_for_branch` in
`crates/omnigraph/src/db/omnigraph/table_ops.rs`). For `ensure_indices` one
table's batch holds the declared indexes the table lacks and a whole rebuild
of each BTREE or vector index whose coverage lags
(`TableStore::stage_index_reconciliation`), the two index links optimize
chains after its compaction; full-text coverage stays with the explicit
rebuild. Heavy steps share one budget per pass (`MaintenanceBudget` in
`crates/omnigraph/src/table_store.rs`). Optimize applies its per-table failure
rules: if a rewrite or fold already committed detached, its pin remains
eligible for the single graph publication with `index_failed`. A failure
before publication leaves no graph-visible residue; after publication reads,
including full-text search through the batch's certificate, serve from the
staged version.

Every writer commits a table effect through
`TableStore::commit_staged_detached`, which takes the table's declared
full-text columns and chains an untrained full-text segment where the
committed version would hold none; its doc comment and
`full_text_columns_without_segment` in `crates/omnigraph/src/table_store.rs`
give the rule. Schema apply declares a full-text `@index` it adds to a table
it does not evolve the same way, beside the contract
(`TableStore::commit_full_text_declarations`).

Schema apply changes an existing table's columns by metadata-only Lance
commits, planned once before the first effect
(`TableStore::plan_schema_evolution` and `TableStore::stage_schema_evolution`
in `crates/omnigraph/src/table_store.rs`). A dropped column's values
stay in the data files until the next optimize rewrites every fragment holding
them (above); `cleanup` then deletes the old files once no retained version
references them. The collector retains every live branch's head and creation
point and every tagged version, so a branch created before that optimize keeps
the old files, merged or not, until it is deleted.

An added type uses its identity-derived path, which a retry derives
again from the accepted allocator. Retry preserves that path and every
existing version. It can reuse an original empty, stable-row-ID version-one
Create with the desired physical schema. If that original Create has another
schema from an uncommitted attempt, it stages an empty detached Overwrite
based on version one. The replacement publishes at logical version 2 with its
detached version, transaction UUID and `last_linear_version = 1`, against
expected absent table version 0. Fresh or matching original Creates remain
logical version 1; nonempty or otherwise unqualified leftovers refuse.

One main-branch manifest CAS publishes the table delta, graph-lineage change
and replacement contract row. A competing stale attempt cannot delete an
existing table path; the final CAS decides publication. A pre-publication
failure leaves the previous contract and graph visible. After publication,
both are complete and only the disposable in-memory view may need refresh.
The system-column upgrade uses the same publication
boundary. See [Schema contract in the manifest](../decisions/2026-09-30-schema-contract-in-manifest.md).

A prepared schema apply (`prepare_schema_apply_as`, `apply_prepared_schema_as`
in `crates/omnigraph/src/db/omnigraph.rs`) publishes only at the version after
its captured base; those docs give the intent's contents and the no-op rule.

Reconciling and settling a prepared schema invocation are in
[recovery](recovery.md#schema-contract-publication).

Branch merge publishes through this same door, table by table; its routes and
publication are in [merge](merge.md#publication-and-recovery).

Existing-table constructive transactions stage independently with bounded
concurrency. The `stage_write_concurrency` session setting (`process` scope,
range `1..=64`; process default `OMNIGRAPH_LOAD_CONCURRENCY`, an invalid or
`0` value refusing startup instead of running the default) selects that
width for both Load and
ordinary insert/update mutations (default 8), and inside one `load csv`
overwrite the number of Lance writers (`TableStore::stage_overwrite_batches`
cuts the batches at Lance's file target into contiguous chunks, runs one
`execute_uncommitted` per chunk with that many in flight, and joins the
chunks' fragments in chunk order into the one `Operation::Overwrite`, so the
files and their order are those of a single call), and the most partitions of
its `CsvSourceExec` (the planner's `max_partitions`, one per 8 MiB of source
up to this width; the resolve pass cuts the capture on record ends into
slices, each partition claims the next slice from a shared counter, tags its
batches with the slice index, and `ValidateExec` drains every partition and
sorts the held batches by slice before judging them, so the rows reach the
sink in source order and a refusal names the lowest failed slice's record).
Delete transactions remain serial. The setting changes only
fragment preparation: every participant still commits detached and crosses
one graph-manifest publication.

The mutation-mode rule (`write:1`) keeps one mutation query constructive (insert/update) or
destructive (delete), never both. Compose mixed work through separate
mutations, or through a branch when a later merge must expose one combined
result.

## Keyed writes

Every graph table has exactly its non-null system id column (`__id`; `id` on
a legacy-vintage graph) as Lance's unenforced primary key; `id` below names
that column. Production strict insert and upsert route through the sealed,
exact-`id`, filter-bearing MergeInsert adapter:

- strict insert probes the pinned parent and returns `KeyConflict` for an
  existing ID;
- upsert updates or inserts without changing modes on retry. With an id
  BTREE it first runs the strict insert's exact-id probe, while the fragments
  the BTREE leaves uncovered times the source ids stay within one keyed batch
  (8,192), since Lance plans the probe's `IN` list once per uncovered
  fragment. Rows all new stage through the absence-proven insert writer; rows
  all present stage as a known-present update only while the BTREE covers
  more fragments than it leaves and those uncovered fragments' recorded
  data-file sizes are known and total at most 1 MiB
  (`KNOWN_PRESENT_TAIL_MAX_BYTES`): that indexed merge scans each uncovered
  fragment for every source column (Lance 13), so the fragment rule bounds
  its requests and the byte budget bounds its bytes whatever the row width.
  Otherwise the upsert takes Lance's full-table merge join
  (`use_index(false)`), which builds the inserted-row key filter;
- an `update` whose predicate has `<key> = v` on a single String `@key` adds
  the derived `id = v` conjunct to its match scan when the table has an id
  BTREE, as a node delete does, so the scan reads the index;
- one table's keyed input is bounded to 8,192 rows and the effective
  `write_max_bytes` (default 32 MiB) before its data is staged.

Insert/update mutations and keyed Append/Merge loads also cap the sum of
retained row data across tables at `write_max_bytes`
(`retained keyed batch bytes per operation`). Admission starts with
`get_array_memory_size`, subtracting only typed logical Blob payload buffer
capacity. Offsets, validity, URI descriptors and ordinary columns remain in
the row account; shared buffers may be counted more than once. Logical Blob
payload lengths have an independent allowance of the same size. Payload
accounts count logical payload lengths; builder over-capacity (up to about one
third) is outside them. Pending-aware
update scans charge both accounts, including their initial pending input.
Every batch such a scan keeps passes through `PendingScanAccount::retain`,
the account's only batch charge, which copies its rows into buffers of their
own before charging them: a Lance scanner batch can share buffers with rows it
does not contain, and admission must not depend on the rows stored beside a
match. A cumulative account always charges a compact copy; a per-candidate
check may keep an uncopied batch that already fits. Because `retain` always
copies, a row-rewrite trial in `scan_update_matches` briefly holds the carried
columns twice: the admitted scan it keeps for the column-patch fallback, and
the charged copies of the whole rows. Its peak can therefore reach about twice
the write budget, although each account still stops at one budget. That bound
is the trial's, not the whole scan's: when no Blob column is omitted, the
match scan itself keeps the matched rows whole, vectors included, before the
trial runs, and admission charges only the carried columns, so the number of
matched rows times the vector width bounds that memory, not the budget. A
refused copy is also made before its refusal (`retain` copies, then charges),
so a trial can pass the budget by one batch before it falls back to the patch.
The keyed parse spool separately caps its row estimate and decoded Blob bytes.
Inline and copied external payload lengths are admitted together before GETs;
materialized row and payload accounts are checked again before staging.

Delete mutations, cascades and Overwrite's removed-ID detection stream matches
under one `write_max_bytes` removed-ID allowance per operation, charging each
ID's UTF-8 length plus one 24-byte `String` slot before copying it. A delete's
id scan also returns each matched row's address at the table's pin, and
`stage_all` writes the deletion vectors for exactly those rows
(`TableStore::stage_delete_rows`, Lance's `DeleteBuilder` after its scan), so
a deleted table is scanned once; addresses from another version than the
reopened pin take Lance's scanning delete. A node removal reads each edge
table that references the type for the edges touching its removed ids (the
delete cascade, and the retained-edge check of an Overwrite, `load csv` or a
merge) through `validate::RemovalEdgeReads`. Lance gives every fragment that
no index answers a pushed filter on its own copy of the filter, built before
the scan reads. So the plan pushes the ids to every fragment, 8,192 per
filter, only when a BTREE on each probed endpoint covers the table, or when
one filter holds every id and the uncovered fragments times those ids stay at
most 8,192; otherwise it probes the covered fragments and reads the uncovered
ones once, whole, matching their rows against the removed ids in memory. A node delete whose predicate has
a `<key> = v` conjunct on the type's single String `@key` also filters on the
derived id when the pinned table has an id BTREE, as the read planner's key
narrowing does. Mutation validation reads the write's pinned base through the
shared session and the held-handle cache keyed by each exact pin. Overwrite
runs that detection only when its plan's `removal` is `scan`, a populated node
target an edge type references; under `none` it detects and charges nothing.
Overwrite's bulk input is not subject to the keyed aggregate row/batch limits.
These are representation limits, not a combined allocator/RSS budget; native
scan buffers, conversion copies and validation's derived state are outside them.
The captured `WriteBudget` also supplies `FileWriterOptions.data_cache_bytes`
for absence-proven strict inserts and staged overwrite batches. Lance divides
this accumulation target by top-level field count. At the default 32 MiB,
fewer than four fields receive a higher threshold than Lance's native 8 MiB
per field, and more than four receive a lower threshold. A field flushes after
an incoming array crosses its threshold. Nested encoders can each hold a
share, and concurrent table writers each receive the target. Buffered slices
keep copying enabled. These writes retain the existing dataset's V2_2 format
and set no encoded-page-size bound. Native merge-insert does not expose
writer options, so upserts and known-present updates retain native buffering
defaults.
Refusal precedes the current
operation's fragment staging and publication, though writable open may have
completed earlier schema work and a load may already have created its branch.

An insertion-only transaction may carry the internal
`omnigraph.insert_absence = "v1"` certificate; what branch merge accepts it
for is in [merge](merge.md#proven-insertion-route).

### Column-patch updates

An `update` that assigns no Blob, leaves at least one Blob or fixed-width
vector column unassigned, omits no column that a key or uniqueness group
names, and is the only statement of its mutation writing the type (no other
write can join it in the table's one keyed write) may stage a column patch: `KeyedWriteSemantics::KnownPresentColumnPatch`, Lance's
`MergeInsertWriteMode::RewriteColumns` with `WhenNotMatched::DoNothing`. Its
source is `id` plus the assigned columns. Lance attaches one data file per
touched fragment holding those columns for every row of the fragment,
tombstones their older copies, and never reads or writes the other columns.
Rows keep their fragment and stable row id; Lance restamps the patched rows'
`_row_last_updated_at_version` with the read version plus one, as a row
rewrite does. An omitted legacy (non-v2) Blob, or a vector of non-primitive
items, sends the update back to whole rows.

`TableStore::scan_update_matches` decides per update between the patch and
a row rewrite; the cost it compares and the admission rule are the docs of
`scan_update_matches`, `UpdateMatchRequest` and `UpdateWriteCost` in
`crates/omnigraph/src/table_store.rs`. The choice is a physical plan decision
and never decides the outcome (`write_max_bytes_update_carried_blob.gqt`).

A patch may withdraw touched fragments from an index on an assigned column
while they stay in the manifest, and the index keeps its old entries for them.
That is safe while a lagging index is rebuilt whole (`stage_index_fold`, the
"Index segments" fence in [lance-fences.md](lance-fences.md#index-segments)). An incremental reconciliation
that adds a delta segment over a patched fragment must treat the fragment as
moved, excluding the older segments' entries for it, or the stale entries pass
the coverage mask.

The `id` index keeps its coverage of the touched fragments
(`keep_key_index_coverage`, whose doc comment gives the reason). Every other index on an
unpatched column keeps its coverage of the touched fragments, unlike a row
rewrite, whose new fragment no index covers. A patch carries no inserted-row
filter and no insertion-absence certificate; the branch merge's pure-insert
proof treats it as an ordinary update. The change feed derives an adjacent
patch commit by row identity (`candidate_scan::interval_candidate_plan`)
rather than the touched-fragment suffix, which a patch leaves empty. The old
copies of the patched columns stay in the fragment's earlier data files until
a compaction rewrites the fragment, as a row rewrite's deleted rows stay until
their deletions are materialized. Dropped-column erasure is unaffected: the
newest copy of a column lives in a file that lists it, so a drop still marks
the fragment for `optimize`.

Full-text builds stage an artifact-scoped analyzer certificate before their
CreateIndex metadata is published. The explicit rebuild uses this same writer;
it does not rewrite retained snapshots or add a separate migration publisher.
See
[full-text compatibility](lance-fences.md#full-text-compatibility).

## Branch writes on inherited tables

A named graph branch reads the exact table version its `__manifest` lineage
inherited from its parent. Its writes stage detached on that inherited
dataset and native ref, from the inherited pin; the registration's
`native_dataset_branch` stays what it inherited, and no per-table fork is
created for a graph branch. Two graph branches writing the same table
produce two detached chains in one dataset; neither moves a HEAD, so they
cannot conflict, and each branch's `__manifest` lineage names its own chain.
Ownership of a table version is that lineage, never a native ref name.

A table fork a branch created before v11 (native name
`fork.{owner incarnation ULID}.m{base manifest version}.{graph commit ULID}`,
or `legacy` in the incarnation position) stays valid: a registration that
names it as `native_dataset_branch` opens there, a later write on that graph
branch stages detached on the fork, and
`TableVersionMetadata.table_fork_owner` still records its owner. `cleanup`
reclaims a fork no registration references once the graph branch incarnation
in its name is gone, and keeps a generated name it cannot parse.

Branch deletion starts no background table reclamation; its protocol at and
below format 18 is in [recovery](recovery.md#graph-branch-controls). Cached write
admission checks the branch's identifier (the row's, or the ref's with its
retirement metadata) through the existing lookup, with no additional storage
request. Branch enumeration reads `main`'s rows, no request on a held `main`
and otherwise a probe of `main`'s head and a read of its latest version when
the head moved; below the stamp it reads active refs. Creation does not scan retired
histories.

A detailed branch listing derives every field from the branch rows (or refs)
and each branch's `__manifest`, and writes nothing
(`list_graph_branches_detailed` in `crates/omnigraph-catalog/src/branch_detail.rs`).

Stable table/incarnation identity, not `table_key`, determines whether a
registration, rename, tombstone, or pointer belongs to the same lifetime.

## External Blob inputs

Blob URI admission is part of preparation. The graph's
`ExternalBlobPolicy` defaults to deny; served graphs retain only server-safe
bases. The adapter normalizes and coalesces authorized sources, bounds selected
reference count and URI metadata, probes each source once, and charges selected
payload ranges before reading bytes or staging any effect.

Overwrite can preserve an allowed external descriptor through Lance
`WriteParams`. Keyed writes and row-writing merge paths materialize selected
external bytes under the operation's `write_max_bytes` Blob payload budget because Lance's MergeInsert
surface has no equivalent reference-preservation hook. A pointer-only branch
adoption does no source I/O. An update never reads the Blobs it assigns; a
column-patch update keeps every other Blob unread; in a whole-row update a
carried stored external reference the policy refuses fails as
`StoredExternalBlobDenied` before effects. See [blob.md](blob.md).

## Failure outcomes

| Observation | Outcome |
|---|---|
| Parse, validation, policy, limit, or authority failure before effects | Typed error; no graph movement |
| Authority movement before effects through publications that changed no row (optimize, index builds) | Carry the attempt over them: re-anchor on the current head, rebase each moved staged table with Lance's resolver; when Lance refuses a table, send the attempt back |
| Any other authority movement before effects | Discard the complete attempt and prepare again from the request under the writer gates, whatever the write's kind, boundedly |
| A later attempt of a write that selects existing rows sees another branch incarnation or accepted schema than the first, or any write reaches the bound | `ReadSetChanged` |
| A publish or a claim on a branch whose writer epoch is not the one its process asserts | `WriterFenced { held, read, current }` before the manifest put; never retried; the process's holder for the branch is cleared, and a publish's detached staging stays unpublished for the collector |
| Exact duplicate on strict insert | `KeyConflict` |
| Mutation, load, Blob write, merge or optimize fails before publication, after detached effects | Typed error; no graph movement; the detached staging is unpublished and the collector reclaims it once its recorded authority is gone. The table store marks an opaque error (storage, I/O, internal) of a staged write or a detached commit `CompletionEvidence::Unpublished` at the source, and the mutation, load, Blob, merge and optimize writers mark the steps between (an optimize table's own failure, `BeforeEffect` in planning and `Unpublished` in staging, fails only that table), so a server does not treat that logical error as uncertain; native storage settlement is checked independently, and a typed refusal keeps its shape |
| A manifest request no re-issue settles | The write is reported in doubt and nothing records it. The version is created only if absent, so the request lands before the branch's next publication or never: that publication either loads it as the head and builds on it, or takes the version first, after which the late request fails its precondition (a race between the two is the ordinary version-CAS retry). The attempt's detached effects stay unreachable unless it lands, and the collector keeps them while their recorded publication authority is current |
| The publisher fails | Every publisher failure carries its evidence. Its reads, preparation and history archiving, and a manifest request that was never sent (a failed transaction-file put precedes it) or that the store refused outright, are `Unpublished`. A manifest request with an ambiguous answer is re-issued unchanged by the dataset's commit handler until the store answers it; Lance reads a held version back by its transaction. Lance deletes the attempt's transaction file only on a missing version the commit's own request record calls definitive, so a request that may still land keeps the file its manifest would name. A version that may hold the commit after that is read back, and only an outcome the read-back cannot settle is reported in doubt |
| Schema apply or the system-column upgrade reports an error after proven publication | `RecoveryRequired` can name the committed outcome; its tables and contract are durable, and refresh/reopen rebuilds only the in-memory view |
| A foreign linear commit lands above a table's `omnigraph.last_linear_version` | No read or write resolves it and nothing adopts the commit; the collector deletes neither its manifest nor its files and lists it under `foreign_versions`, which `cleanup` reports per table |
| A sidecar from a build that predates detached commits is present | A read-write open refuses until that build has resolved it |
| `commit publish` of an unpublished commit whose target head moved past its captured parent, or whose target incarnation is retired | `PreconditionFailed` naming the captured parent and the current head, or not found; the proposal stays retained until `cleanup` releases its expired tag |
| `commit publish` or `commit drop` of a published commit id | Publish answers the established published commit; drop is a conflict |
| A proposal tag's creation or deletion reports an ambiguous result | The tag is read back: a matching tag is success, a differing tag an integrity error, an unavailable readback `manifest_publish_in_doubt`. After a proposal or unattached snapshot tag is created, a failed authority probe remains in doubt even if rollback deletes the tag: an earlier conditional create may recreate it. Only proven expiry of the captured authority makes that refusal definite |

The `Unpublished` annotations cover the named writer envelopes, the table
store's staged-write and detached-commit sites, and every publisher failure
whose manifest request cannot land. Other opaque schema and maintenance
preparation errors without completion evidence remain uncertain. A
post-publication `RecoveryRequired` or an uncertain publication is never
changed into definite non-publication by these annotations.

An acknowledgement follows `E4`; for a write `as commit`, it comes after the
proposal tag is durable and the captured target authority was re-probed
unchanged.

## Policy and attribution

Every public mutating `_as` entry point enforces its Cedar action/scope/actor
gate in the engine (`E6`). Actor attribution
travels with the pre-minted graph lineage and is published with the same
manifest CAS.

Maintenance has no actor-less public form. `optimize_as`, `ensure_indices_as`,
`ensure_indices_on_as` and `rebuild_full_text_indices_on_as` check `maintain`
on the branch they maintain, `main` for optimize. `cleanup_as` checks `reclaim`
with no branch.
`forbidden_apis.rs::maintenance_entry_points_take_an_actor` keeps every
registered maintenance surface an `_as` form with an `actor` parameter.

Bearer authentication (`401 unauthorized`), the HTTP contract check
(`400 api_contract_mismatch`) and policy (`403 forbidden`) refuse a served
write only before its first durable effect. The server runs
the first two as middleware ahead of graph resolution, each write handler
authorizes before admission, and each engine `_as` entry point calls `enforce`
before it stages anything; a load's implicit fork checks `branch_create` before
the fork, and nothing after it checks policy. A refusal that follows an effect
answers a success with details instead, as the source deletion of
`branch merge --delete-branch` does. The CLI relies on this order: it reports
those three refusals with `effects: none`. `ApiError::from_omni`
cannot tell an early `OmniError::Policy` from a late one, so an `enforce` call
after a durable effect would make that report wrong. The server's
`auth_policy::signed_identities_obey_current_policy_and_attribute_writes` and
the CLI's `system_remote::remote_policy_enforces_branch_first_cli_workflow`
check that denied writes leave the graph unchanged.

## Maintenance of this protocol

A new writer must:

- declare its complete authority token and effect set;
- use the shared gate order and publication primitive;
- check policy before its first durable effect (see
  [Policy and attribution](#policy-and-attribution));
- stage every table effect as a detached commit of the pin, carrying the
  staged-against properties (and `omnigraph.deleted_ids` on a delete), and
  publish the pins in the one CAS; nothing follows the CAS;
- add its windows to `detached_commit_matrix.rs` and its crash cells to
  `failpoints.rs`;
- join the durable-call/source guards in `forbidden_apis.rs`;
- prove bounds and crash windows at the owning layer.

Design rationale and rejected alternatives live in
[RFC: Detached-only tables](../decisions/2026-09-21-detached-only-tables.md),
[RFC 0067](../decisions/0067-detached-table-commits.md),
[RFC 0022](../decisions/0022-unified-write-path.md),
[RFC 0023](../decisions/0023-key-conflict-fencing.md), and
[RFC 0028](../decisions/0028-stable-schema-identity.md).

# Graph write protocol

**Audience:** engine and storage contributors
**Authority:** current graph-visible write path; what a crash leaves and who
finishes it is in [recovery.md](recovery.md)

Every successful graph-content write has one visibility point: a conditional
`__manifest` publication. Table effects happen earlier, but as detached Lance
commits of the pinned base that nothing references until that publication
([RFC 0067](../rfcs/0067-detached-table-commits.md)). A detached commit is
the table's version for its whole life: nothing replays it onto the table's
linear history, and a graph table's linear HEAD stays at its creation version
([RFC: Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md)).

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

An error before the manifest CAS leaves the graph unchanged: the detached
versions are unpublished staging, which `cleanup`'s collector reclaims once
their recorded publication authority is provably gone. The caller retries
from a fresh snapshot. A successful publication is the whole write: no table
effect has post-publication work. A lost acknowledgement is resolved
against the exact attempted manifest; unavailable readback stays indeterminate.
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
addressable commit id `hb1.<block>.<slot>.<nonce>`. The existing schema and branch gates still serialize
conflicting control operations.

Native branch creation uses an operation-local capture of the bound coordinator
or that same one-entry cache after the control gates. Reuse
requires a fresh match of the complete manifest incarnation, including the
native branch lifetime; a stale or missing view takes the existing refresh/open
path. Captures share immutable lineage and the Lance session, copy current
table state, and leave the handle's active branch unchanged.

Create admission lists native ref names and reads bodies only for the source,
the target's incarnations and logical ancestors or descendants. Physical path
checks include retired ref names. The
exact target ref must be absent; an empty listing of its exact native tree
allows creation without forced reclamation. A nonempty tree and clone-only
recovery keep Lance's full dependency and tag checks.

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
it only when that exact base matches its previously coherent view and its graph
cache has adopted the published lineage. A foreign advance, unsupported base,
or failure before lineage adoption leaves the full-refresh fallback armed.
Registration replacement, rename, tombstone, and same-version physical-owner
handoff use the existing complete fold. This is disposable process memory;
`__manifest` remains the only durable graph authority.

`GraphNamespacePublisher` retains at most one complete stored row batch from
its last acknowledged publication. Every attempt opens the target branch's
latest `__manifest` again. Reuse requires matching native manifest location,
version, naming scheme, complete Lance manifest and schema metadata; known
ETag or size mismatches reject reuse. Dictionary-bearing schemas bypass this
cache. Cached rows pass through the same decoder and collision, expected-version,
lineage and exact-head checks as scanned rows. The zero-retry version CAS and
lost-acknowledgement readback remain unchanged. Foreign movement, missing rows
or an image above the 8 MiB accounted Arrow-buffer/key budget use a full scan.
This budget bounds retained cache accounting, not total process memory. The
copy-on-write publication still rewrites the complete live row set.

The capture (`open_write_txn`) obtains the accepted contract identity from
the selected manifest snapshot. It validates that snapshot's complete contract
row before reusing a catalog with the same identity. An unchanged captured
manifest image reuses its validation; newly observed images carry the contract
from their state scan, and acknowledged publications reuse their retained rows.
The cache is disposable; a missing or invalid row
cannot be repaired from root files. Captures and finalization retain the
process-local schema gate and complete-authority checks without a sentinel
branch or `ensure_schema_apply_idle` pass.

Finalization acquires the root-shared gate order:

1. the schema gate — a shared permit for ordinary writers (only a
   contract-lifecycle pass such as schema apply or the system-column
   upgrade, and branch create, whose namespace inventory no CAS covers,
   take it exclusively, so cross-branch writers do not serialize on it; see
   [RFC 2026-09-18-shared-schema-gate](../rfcs/2026-09-18-shared-schema-gate.md));
2. target branch;
3. touched `(table identity, physical branch)` entries in deterministic order;
4. coordinator publication.

These gates order work inside one process. Correctness still depends on the persisted manifest
precondition and the exact Lance transaction identity each pin records. A
retryable pre-effect attempt discards all staged work, captures a new
`WriteTxn`, and repeats boundedly; it never reuses batches against a new base.

## Writer adapters

All current graph-visible writers share the publication door but have different
physical-effect proofs:

| Writer | Physical adapter | Publication |
|---|---|---|
| Mutation / Load | One exact staged keyed, overwrite, or delete transaction per touched table | One graph commit |
| SchemaApply | Existing-table metadata-only schema evolution (a detached `Project` for renamed and dropped columns, then a detached `Merge` adding nullable columns over the unchanged fragments; no data file written, no row read); original added-type Create or qualified reuse/detached replacement; complete table delta and replacement schema-contract row | One main-branch graph commit |
| BranchMerge | Onto main: a pointer switch, main's registration taking the source's pin. Into a named branch: a chain of detached chunk commits (proven insertion chain or bounded ordered diff) published as one pin per table (`exec/merge.rs`) | One target-branch graph commit |
| EnsureIndices / full-text rebuild | One detached `CreateIndex` batch per productive table, published as a pin like a mutation's effect (RFC 0067); ordinary ensure leaves untrainable vector work pending, explicit FTS rebuild replaces postings from rows | One graph publication when work lands |
| Optimize | One detached compaction `Rewrite` per productive table (Lance's plan plus every fragment still holding a dropped column), chained with a detached whole rebuild of each index whose coverage lags and a detached build of each declared-but-unbuilt index, published as pins (RFC 0067) | One main-branch graph commit with an exact CAS on the pins the batch was planned from |

Native graph-branch create/delete is a control exception. `BranchContents` is
the logical authority; clone/delete residue is derived physical state and is
reclaimed only when its target is provable from that authority. It does not
invent an alternate graph-content publisher. Each branch life owns a native ref
named `{logical}.{ULID}` (see [RFC 0042](../rfcs/0042-incarnation-suffixed-branch-refs.md));
a recreated branch therefore never shares a path with its dead predecessor, and
what the predecessor left (its unpublished stagings, and the table forks a
pre-v11 branch owned) is reclaimed by `cleanup` rather than healed in place.

## Mutation and Load

`MutationStaging` accumulates read-your-writes batches and delete predicates
in memory. It performs all type, value, uniqueness, endpoint, cardinality, and
resource validation before staging. `stage_all` opens each touched table at
its pin through the read-handle cache (the registration's `staged_version`
when it carries one) and produces one exact transaction per table without
moving HEAD. `commit_all` enters the gates, revalidates the complete
authority (for a branch other than the handle's bound one, the captured
manifest probe first: an unchanged version and branch identifier keep the
captured authority, a mismatch is a fresh coherent capture), and commits
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
is unpublished until the collector reclaims it. The probe reads only the
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
writes do not read linear HEAD; `repair` reports a foreign linear commit above
`omnigraph.last_linear_version` as `foreign_drift`
(`repair.rs`, `judge_against_last_linear_version`). Added-type admission can
inspect an unregistered deterministic path's HEAD only to prove it remains
its qualified original empty version 1; it never adopts an advanced HEAD.

Detached manifests are reclaimed by the tracing collector `cleanup` runs
(`db/omnigraph/collector.rs`; `optimize.rs`, `cleanup_detached_only`). Its
roots are the pins of every `__manifest` version the run's `--keep` and
`--older-than` policy retains on every live branch; a detached manifest is
swept when a `__manifest` version once named it and no retained version
still does, or when no `__manifest` version ever named it and the authority
its transaction properties record is provably gone. Nothing defers a table's
collection; see [Maintenance](../user/operations/maintenance.md#cleanup).

Optimize plans each table's compaction from its pin
(`TableStore::plan_table_compaction`): Lance's plan, plus a task of its own
for every other fragment whose data or overlay files still list a field id the
schema lacks, the values of a dropped column. The rewrite scans the current
schema only, so its files hold no dropped value; this is how a drop is erased,
deterministically, whatever Lance's size and deletion heuristics select. It
stages the rewrite detached with fragment ids above the base's high-water
mark, so it needs no `ReserveFragments`; a lagging scalar or vector index is
rebuilt whole as a detached commit chained on the rewrite under its name
(Lance 11 folds only through a linear commit), keeping a vector index's
partition count; the batch publishes once with an exact CAS on every planned
pin, and a pin a concurrent writer moved fails the run with a read-set
conflict so the next run re-plans. A failure before publication leaves
unpublished staging the collector reclaims once it is dead. A strict mutation
prepared before Optimize's publication reports the same read-set conflict as
it would after any other writer.

The index writer (`ensure_indices` and the explicit full-text rebuild)
follows the same protocol: it opens each productive table at its pin, stages
the complete BTREE/FTS/vector batch before the gates, commits every batch as
a detached version of the pin and publishes the pins once. A failure before
publication leaves no graph-visible residue; after publication reads,
including full-text search through the batch's certificate, serve from the
staged version.

Schema apply changes an existing table's columns by metadata-only Lance
commits (`TableStore::stage_schema_evolution`). Renamed and dropped columns
commit as one detached `Operation::Project` of the table's pin, the surviving
columns keeping their field ids, metadata and indexes; added nullable columns
commit as a detached `Operation::Merge` over the unchanged fragments, with
field ids above every id the data files reference, which fragments that lack
them read as null. Lance refuses a Merge that renames a field, so a table with
both kinds chains the Merge on the Project, and the pin names the tip; either
way the table publishes one logical version past its published version. No
data file is written and no row or Blob payload is read, so the apply's memory
and I/O do not grow with the rows or payload bytes the table stores; they
still grow with its fragment, schema and index metadata, because planning
walks the fragment inventory, the Merge carries it and each detached commit
serializes it. Every surviving index keeps its coverage, and a stored external
Blob descriptor, ranged or not, is never rebuilt. A dropped column's values
stay in the data files until the next optimize rewrites every fragment holding
them (above); `cleanup` then deletes the old files once no retained version
references them. The collector retains every live branch's head and creation
point and every tagged version, so a branch created before that optimize keeps
the old files, merged or not, until it is deleted. Every evolved table's
commits are planned once, from its manifest, before the first effect, and the
effects stage exactly that plan, each step checking that its base has the
columns the previous step left. A column whose type or nullability would change refuses at
planning; the schema planner emits no such step. The physical column order
follows the catalog.

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
There are no staged root contract files, schema-apply sentinel or durable
post-publication install. The system-column upgrade uses the same publication
boundary. See [Schema contract in the manifest](../rfcs/2026-09-30-schema-contract-in-manifest.md).

`prepare_schema_apply_as` binds the canonical root and schema identity domain,
exact main-branch authority, numeric manifest base, desired contract, actor and
a preallocated intent nonce before table effects; the published commit id
`hb1.<block>.<slot>.<nonce>` wraps that nonce, so a lookup by the intent names
the commit through `commit_id_answers`, never by equality. Preparation supports a
read-only handle: it captures authority and plans, without issuing native writes.
`apply_prepared_schema_as` rechecks that authority and the current policy under
the existing gates. Intent version 2 uses `ExactGraphVersion`: a base at `M` can
publish only at `M + 1`, and metadata-only contention that preserves graph HEAD
still refuses rather than rebasing. Version-1 intents are rejected; this is an
internal protocol change, not a graph-storage format change. An effectful success
returns the operation's own `GraphCommit` and contract identity, not a later head
observation. The prepared
value is execution input, not an authorization grant or a distributed writer
fence.

A schema no-op requires the exact source and accepted contract. Changed comments
or formatting publish a replacement contract and lineage once while preserving
every table pin. An empty migration plan alone cannot certify the desired
source. The ordinary apply API uses the same preparation, execution and
exact-version rule, including refusal on head-preserving metadata contention.

`reconcile_schema_apply_as` only reads retained, exact publication evidence and
returns `Committed`, `NoOp` or `Unknown`. It checks the single candidate manifest
version immediately after the prepared base, without walking history. Missing
evidence, including metadata-only interposition, stays unknown; a no-op requires
the exact captured base still to be current. Reconciliation neither applies the
schema nor authorizes replay. Callers still own durable intent recording,
evidence retention and fencing the previous owner before they can establish
terminal non-publication.

For a durably accepted invocation, `prepare_schema_settlement_as` issues a
separate neutral lineage intent bound to that original and its one `M + 1`.
The caller persists it before `settle_prepared_schema_as` can publish its fence.
A verified occupied candidate or exact fence receipt proves `NotPublished`;
missing evidence remains `Unknown`. A stale no-op certificate is `NoOpRefused`.
Settlement never replays the original schema, authorizes adopting a foreign
contract into the achieved projection, or proves accepted native I/O has stopped. Prior-owner quiescence, current policy,
exclusive admission and protected evidence remain caller obligations. These
engine boundaries do not enable online server activation; the deployment
protocol remains in the
[server runtime RFC](../rfcs/2026-09-29-server-runtime-and-online-deployment.md).

Branch merge follows it too. A merge onto main is a pointer switch: main's
registration takes the source's pin, and the merge stages no fenced insert,
no keyed update and no payload copy; external blob descriptors stay
external, and an empty source delta leaves main's registration untouched. A
merge into a named branch opens each target table at its pin, and every
chunk of the proven insertion chain or the bounded ordered diff commits as a
detached version of the previous chunk (one link per chunk, within the
merge's transaction ceiling); the target publishes once, with each chained
table's pin naming `base + 1` as its `published_dataset_version` and the
chain's tip as its `staged_version`, whatever the chain's length. The
pure-insert proof walks the branch's commit chain by `read_version` links
from the source pin back to the base pin (`try_proven_pure_insert_history`,
`proven_chain_fragments` in `exec/merge.rs`); lineage candidate discovery
(`plan_lineage_merge`) compares the base and side manifests and walks no
chain. A failure anywhere before publication leaves the target untouched
and the chain as unpublished staging; a target that advanced meanwhile
makes the merge lose its manifest CAS and return the ordinary conflict.

Existing-table constructive transactions stage independently with bounded
concurrency. The `stage_write_concurrency` session setting (`process` scope,
range `1..=64`; process default `OMNIGRAPH_LOAD_CONCURRENCY`, an invalid or
`0` value refusing startup instead of running the default) selects that
width for both Load and
ordinary insert/update mutations (default 8). Delete transactions remain
serial. The setting changes only
fragment preparation: every participant still commits detached and crosses
one graph-manifest publication.

The D2 rule keeps one mutation query constructive (insert/update) or
destructive (delete), never both. Compose mixed work through separate
mutations, or through a branch when a later merge must expose one combined
result.

## Keyed writes

Every v6-or-later graph table has exactly the non-null physical `id` field as Lance's
unenforced primary key. Production strict insert and upsert route through the
sealed, exact-`id`, filter-bearing MergeInsert adapter:

- strict insert probes the pinned parent and returns `KeyConflict` for an
  existing ID;
- upsert updates or inserts without changing modes on retry;
- a bare Lance Append is not a production graph-table write;
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
The keyed parse spool separately caps its row estimate and decoded Blob bytes.
Inline and copied external payload lengths are admitted together before GETs;
materialized row and payload accounts are checked again before staging.

Delete mutations, cascades and Overwrite's removed-ID detection stream matches
under one `write_max_bytes` removed-ID allowance per operation, charging each
ID's UTF-8 length plus one 24-byte `String` slot before copying it.
Overwrite's bulk input is not subject to the keyed aggregate row/batch limits.
These are representation limits, not a combined allocator/RSS budget; native
scan buffers, conversion copies and validation's derived state are outside them.
Refusal precedes the current
operation's fragment staging and publication, though writable open may have
completed earlier schema work and a load may already have created its branch.

An insertion-only transaction may carry the internal
`omnigraph.insert_absence = "v1"` certificate after its absence and physical
shape are proven. Branch merge accepts the shortcut only across a complete,
contiguous, structurally valid history. The certificate is an optimization
capability, not an authenticity mechanism; unfamiliar or cleaned history falls
back to the general merge.

Full-text builds stage an artifact-scoped analyzer certificate before their
CreateIndex metadata is published. The explicit rebuild uses this same writer;
it does not rewrite retained snapshots or add a separate migration publisher.
See
[full-text compatibility](lance.md#full-text-compatibility).

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

Branch deletion starts no background table reclamation. It records retirement
metadata on the exact native `__manifest` ref, archives its `BranchContents`
inside the native tree, then unlinks the active ref. Required physical parent
history and imported merge-base records remain readable through the archive.
Cleanup reclaims unneeded trees and archives after checking their dependencies.
Delayed staging is judged by a complete live-identity inventory after the table
listing, with final validation of the graph and tag capture before deletion.
Cached write admission checks the ref's identifier and retirement metadata
through the existing lookup, with no additional storage request. Cold branch
enumeration reads active refs; creation does not scan retired histories.

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
carried stored external reference the policy refuses fails as
`StoredExternalBlobDenied` before effects. See [blob.md](blob.md).

## Failure outcomes

| Observation | Outcome |
|---|---|
| Parse, validation, policy, limit, or authority failure before effects | Typed error; no graph movement |
| Retryable authority movement before effects on a replay-safe adapter | Discard the complete attempt and reprepare boundedly |
| Strict read-set movement | `ReadSetChanged` |
| Exact duplicate on strict insert | `KeyConflict` |
| Any writer fails before publication, after any detached effect | Typed error; no graph movement; the detached staging is unpublished and the collector reclaims it once its recorded authority is gone |
| Schema apply or the system-column upgrade reports an error after proven publication | `RecoveryRequired` can name the committed outcome; its tables and contract are durable, and refresh/reopen rebuilds only the in-memory view |
| A foreign linear commit lands above a table's `omnigraph.last_linear_version` | No read or write resolves it; `repair` reports the table as `foreign_drift` and never adopts the commit; the collector deletes neither its manifest nor its files and lists it under `foreign_versions` |
| A sidecar from a build that predates detached commits is present | A read-write open refuses until that build has resolved it |

An acknowledgement is returned only after the manifest commit is durable and
visible.

## Policy and attribution

Every public mutating `_as` entry point enforces its Cedar action/scope/actor
gate in the engine. The trusted server resolves the actor; direct embedded
callers must pass one when a policy checker is installed. Actor attribution
travels with the pre-minted graph lineage and is published with the same
manifest CAS.

## Maintenance of this protocol

A new writer must:

- declare its complete authority token and effect set;
- use the shared gate order and publication primitive;
- stage every table effect as a detached commit of the pin, carrying the
  staged-against properties (and `omnigraph.deleted_ids` on a delete), and
  publish the pins in the one CAS; nothing follows the CAS;
- add its windows to `detached_commit_matrix.rs` and its crash cells to
  `failpoints.rs`;
- join the durable-call/source guards in `forbidden_apis.rs`;
- prove bounds and crash windows at the owning layer.

Design rationale and rejected alternatives live in
[RFC: Detached-only tables](../rfcs/2026-09-21-detached-only-tables.md),
[RFC 0067](../rfcs/0067-detached-table-commits.md),
[RFC 0022](../rfcs/0022-unified-write-path.md),
[RFC 0023](../rfcs/0023-key-conflict-fencing.md), and
[RFC 0028](../rfcs/0028-stable-schema-identity.md).

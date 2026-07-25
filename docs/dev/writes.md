# Direct-Publish Write Path

> History: the Run state machine and `__run__<id>` staging branches were
> removed in MR-771 (shipped v0.4.0). Writes now go directly to the target
> table; this document specifies that direct-publish path.

`mutate_as` and `load` prepare against one immutable branch-authority token,
write **directly to the target table**, and call
`ManifestBatchPublisher::publish` once at the end. The token is
`(Lance branch identifier, exact optional graph_head, accepted schema identity)`;
the exact `graph_head` check protects validation dependencies on tables the
write does not touch. Publisher row-level CAS on `__manifest` is the visibility
fence. Process-local branch/table queues reduce retries but are not distributed
authority.

## What this means in practice

- No `RunRecord`, no `_graph_runs.lance`, no `_graph_run_actors.lance`.
- No `omnigraph run *` CLI subcommands and no `/runs/*` HTTP endpoints.
- No `__run__<id>` staging branches; `__run__*` is no longer a reserved
  name. The branch-name guard was removed in MR-770. Historically, the v2→v3
  in-place migration swept stale `__run__*` entries; the current v8 strand is
  strict single-version, so older graphs are refused and rebuilt by
  export/init/load rather than migrated on open. (Inert `_graph_runs.lance`
  bytes in an old export source remain irrelevant to the rebuilt graph.)
- Cancelled mutation futures leave **no graph-visible state** unless the
  manifest publish already completed. Before that point they can leave
  reclaimable uncommitted Lance files, or sidecar-covered committed table
  effects that the next quiesced recovery rolls forward/compensates. A
  first-touch named-branch write can also leave a target table ref, but it is
  never created without ownership: the identity-bearing v9 sidecar is durable first and
  names that `(table_path, target ref)`. Reclaim and `cleanup` treat any
  matching pending sidecar as a hard stop. Quiesced full recovery accepts both
  logical crash shapes — sidecar durable with no ref yet, or an exact untouched
  ref at the inherited version. Because Lance creates a branch dataset before
  writing its authoritative `BranchContents`, the first shape may still contain
  a clone-only tree; recovery force-reclaims that absent-ref tree idempotently.
  It removes an exact untouched ref before deleting the empty intent. If several
  pending intents claim one ref, a no-effect intent discards only itself while
  any competitor remains; the last no-effect survivor cleans an untouched ref,
  or the effect-owning survivor recovers normally. `cleanup` remains the
  backstop for genuinely unclaimed legacy/stale refs; see the fork-reclaim note
  in [invariants.md](invariants.md).

## Mutation/load coarse OCC (RFC-022 first adapter)

Mutation and load use a closed prepare → effect → publish attempt:

1. run the branch-aware recovery barrier, then capture the target branch's native Lance
   `BranchIdentifier`, exact `graph_head:<branch>` (including absence on a
   fresh branch), accepted schema identity, and table snapshot;
2. run the complete validator and prepare every effect outside the effect gate.
   Existing-table transactions may stage reclaimable files here. A first-touch
   named-branch table retains its batch/predicate and pre-mints the transaction
   identity instead: Lance branch-local files cannot be staged until its target
   ref exists. Each keyed Mutation/Load table remains one transaction and is
   rejected here with typed `ResourceLimitExceeded` if its accumulated
   strict-insert or upsert input exceeds 8,192 rows or 32 MiB. The JSON loader
   also charges a conservative parsed-value lower bound before retaining rows
   and aggregate decoded base64 bytes before allocating their decoded copies;
   the exact accumulated Arrow size remains the final fence. A mutation update
   seeds its pending-aware scan with the table's already-retained rows/bytes,
   shadows committed rows by pending `id` before charging them, and streams the
   remaining matches into the same budget. Blob matches charge non-blob bytes
   before descriptor fetch and payload size before `BlobFile::read`;
3. acquire shared stream-admission leases for the exact touched table/ref
   domains, then the schema gate, branch gate, and sorted table queues; re-check for
   a relevant sidecar armed since step 1, then revalidate the token and require
   every existing physical target's live Lance HEAD to equal its manifest pin.
   Any unresolved relevant intent returns typed `RecoveryRequired`; uncovered
   HEAD drift points to `omnigraph repair`. Both fail before this attempt arms
   recovery;
4. on an unrelated pre-effect authority mismatch, discard the complete
   attempt. Retryable insert/upsert writers—including load `Append`, while
   remaining strict-insert—reprepare with a bounded retry; strict
   Update/Delete/Overwrite return typed `ReadSetChanged`. A detected existing
   or concurrent strict key conflict is terminal `KeyConflict`, not an
   authority retry and never a switch to upsert;
5. arm an identity-bearing v9 recovery sidecar. For each deferred first-touch table, create
   its target ref, stage branch-local files on that ref, and bind the staged
   transaction to the pre-minted UUID. Then commit every planned transaction
   with zero transparent conflict retries, confirm exact transaction UUIDs and
   table updates, and publish the pre-minted lineage intent under the same token.
   If a keyed commit reports a retryable conflict, the writer may finalize the
   attempt as effect-free only when every planned table still has no owned
   Lance effect. A strict insert then probes every attempted ID against fresh
   manifest-visible authority and returns `KeyConflict` only for an exact
   match; otherwise it returns an internal typed read-set conflict so the outer
   strict operation fully reprepares without changing mode, rather than
   inventing a logical duplicate from Lance's broader retryable class. Upsert
   also discards the whole attempt for bounded reprepare and revalidation. Any
   earlier effect or ambiguous ownership leaves the sidecar authoritative and
   returns `RecoveryRequired`.

Keyed StrictInsert/Upsert has one additional Phase-A blob rule. Lance's
`MergeInsertBuilder` has no `WriteParams` hook, so it cannot set
`allow_external_blob_outside_bases`. Before staging, the adapter sums external
URI ranges (or whole-object sizes), refuses an aggregate above 32 MiB before
reading payload bytes, and materializes accepted URI cells. Staged Overwrite
does accept `WriteParams` and retains external-reference semantics.

### Insertion-absence certificate

Successful keyed writes may leave an inductive proof link in Lance transaction
history: the exact property `omnigraph.insert_absence = "v1"`. The property
means that every key encoded by the transaction's exact-`id` inserted-row
filter was proven absent from that transaction's pinned parent. It is bound to
the persisted transaction, not kept as mutable runtime state.

`stage_keyed_write(StrictInsert)` mints v1 only after its exact target-ID
preflight and after staging verifies a pure insertion-only filtered
`Operation::Update`. An all-new Upsert may mint the same property only when
Lance's completed statistics report one attempt that inserted every input row
and updated, deleted, and skipped zero rows. Upsert certification is optional:
an unfamiliar transaction shape disables the optimization without failing the
logical upsert. A mixed or existing-row upsert is still a normal fenced write
and carries no certificate. StrictInsert fails closed because its absence and
filter are requested semantics.

The minting check binds the certificate to the exact parent `read_version`,
nonempty UUID, exact physical `id` field filter, `RewriteRows` mode, no removed
or updated fragments, no modified fields, no merged generations, no updated
offsets, and at least one new fragment. It also requires
`fields_for_preserving_frag_bitmap` to equal the table schema's complete nested
preorder of field IDs and requires every new fragment's `physical_rows` total
to equal the source row count. The full preorder is correctness-sensitive:
Lance uses it to keep existing indexes from claiming coverage of newly written
fragments, including indexes over nested fields.

BranchMerge consumes v1 only through a complete retained-history proof and an
opaque internally minted `ProvenInsertChunk`; the property alone grants no
capability. Its proven writer skips the otherwise redundant target-ID
preflight and target merge join. It uses public Lance `InsertBuilder` only to
stage immutable fragment files, then replaces the uncommitted `Append`
descriptor with the same filtered insertion-only `Update` before commit. That
output carries a newly validated v1 property, so the proof composes across a
second and later branch generation without ever committing an Append. Missing,
cleaned, or unfamiliar history falls back to the ordinary ordered diff and
general keyed adapter.

This marker is non-cryptographic and does not make raw Lance writers trusted.
Direct Lance mutation of graph tables is outside the supported writer topology;
the verifier additionally requires exact ancestry, identity, schema, row
counts, transaction structure, and final source/target native-ref authority.

The publisher checks the exact head and native branch identity on every CAS
attempt. It never reparents a validation-sensitive intent after contention. A
mismatch after any physical effect returns `RecoveryRequired` and leaves the
sidecar intact; it is not an ordinary retry loop.

This adapter preserves the documented single-writer-process support boundary.
The native branch identifier detects delete/recreate ABA but is not a Lance
conditional-ref fence, and destructive recovery remains unsafe beside a live
foreign process.

### Branch-merge authority and recovery adapter (RFC-022, v9 envelope)

Branch merge retains its writer-specific row classifier and multi-commit table
algorithms, but its authority, recovery, and visibility boundary now use the
RFC-022 adapter contract:

1. open source and target once as coherent `WriteTxn` captures. Each temporary
   coordinator derives manifest state and its lineage projection from the same
   `__manifest` row scan; the `WriteTxn` retains the exact manifest `Dataset`
   probe handle rather than reopening both branches to rediscover the same
   authority. The target token is `(BranchIdentifier, exact optional
   graph_head, accepted schema identity)`; the effective lineage head is
   captured separately because a fresh named branch can inherit a parent while
   its own `graph_head:<branch>` row is absent;
2. compute the merge base from those captured commit ids and classify against
   the immutable base/source/target snapshots outside table gates. For an
   existing-target, HEAD-advancing all-new adopt, first try the narrow
   Lance-history proof in [merge.md](merge.md): every contiguous transaction in
   the complete interval must carry the exact v1 insertion-absence certificate
   and pass its parent/filter/effect/full-schema-preorder/physical-row checks.
   That proof can stream the pinned `_row_created_at_version` range directly
   into the bounded recovery chain. Any unavailable or unfamiliar provenance
   falls back to the ordinary ordered row diff. A first-touch lazy target is
   not admitted to proven data replay; it keeps the existing ref-only fork
   path;
3. acquire the conservative all-catalog source/target table envelope, re-list
   recovery intent, and probe the retained source/target manifest `Dataset`
   handles for current physical incarnation. An unchanged probe keeps the
   coherent capture; a mismatch triggers a fresh full manifest capture and the
   existing typed revalidation/reprepare outcome rather than combining old
   planning state with a new tip. Before arming, every existing target ref that
   will receive a physical effect must also have live Lance HEAD equal to its
   captured target manifest pin; the verified handle is carried into the effect
   instead of being reopened. First-touch refs remain absent until after the
   sidecar. A target
   change returns typed `ReadSetChanged` before effects. A later source-head
   advance is allowed: the contract is "merge the captured source commit," never
   "substitute whatever source is latest." A certificate-proven source table
   also rechecks its exact native `BranchIdentifier` and live manifest/HEAD
   agreement here. Its existing target must still carry the exact native base
   `BranchIdentifier` against which absence was proved, in addition to the
   ordinary target manifest/HEAD baseline. A source or target ref
   delete/recreate after proof therefore fails before arm;
4. pre-mint the merge lineage and each table's ordered Lance data-transaction
   chain. Every keyed new-row or changed-row chunk is bounded to 8,192 rows and
   32 MiB using the actual buffered boundaries. The proven-insert shortcut
   derives those boundaries from a read-only normalized source-interval stream;
   each chunk uses the opaque proven-insert adapter, which performs neither a
   target ID preflight nor a merge join. Lance `InsertBuilder` stages the
   immutable files, then the still-uncommitted Append descriptor is replaced by
   the exact filtered `Update` and certified again. It does not substitute one
   whole-delta merge transaction or commit an Append. The normalizer is
   lazy and deliberately avoids pinned Lance's row-only `strict_batch_size`
   accumulator: normalized/writer chunks are hard-capped while the one upstream
   raw emission remains governed by Lance's approximate `batch_size_bytes`
   target and is covered by the process-RSS gate. A row above 32 MiB or a
   per-table logical data chain above 1,024 transactions is typed
   `ResourceLimitExceeded` before arm. The ordered base/source/target cursors
   also explicitly configure Lance at 8,192 rows and 32 MiB decoded bytes per
   scanner batch. Before the sidecar, validation streams only its projected
   scalar columns and charges each batch's exact Arrow memory size before
   retaining it against one deterministic 32 MiB budget shared by every merge
   candidate; deleted-ID clones are charged conservatively into that same
   budget. Exact recovery separately scans at most 1,026 versions, reserving
   headroom for one derived `CreateIndex` tail and one compensating `Restore`.
   Then arm an identity-bearing v9
   BranchMerge sidecar before the first HEAD advance or first-touch table ref.
   Logical data steps commit with those exact `(read_version, uuid)` identities
   and zero transparent conflict retries. Its physical-effect set can be
   smaller than its intended manifest delta:
   pointer-only table updates are still recorded so recovery publishes the
   complete logical merge;
5. after every multi-commit table effect completes, confirm exact final table
   versions, every logical `SubTableUpdate`, and every first-touch target
   `BranchIdentifier`; then publish once with `ExactGraphHead` and the captured
   table expectations.

Publisher retries cannot re-parent the prepared merge onto a newer target. Any
failure after the v9 sidecar is durable returns `RecoveryRequired`. That rule
includes a strict-insert conflict on the first chunk before a merge-owned table
effect lands: BranchMerge does not use Mutation/Load's `protocol_v3`
effect-free finalizer and does not semantically retry the merge around its
armed `protocol_v4` chain. Full recovery rolls confirmed effects forward only
while the captured target authority still matches; otherwise it compensates
the owned effects while preserving the target winner, or fails closed when
foreign/interleaved table state makes compensation unverifiable. An Armed
first-touch ref with no data HEAD movement is reclaimed without manufacturing
rollback lineage. Armed recovery accepts only a
contiguous prefix of the pre-minted data chain. Rebuildable `CreateIndex`
transactions may follow only the complete chain and are rollback-discardable
derived state; any other, unreadable, or non-contiguous transaction fails
closed. A compensating Lance `Restore` is also recognized by its exact target so
a crash after restore but before the manifest publish resumes without restoring
again.

The retained operation-local manifest probe handles and `merge_exclusive` mutex
are implementation details; neither is persistent authority. The final target
publisher still performs a fresh authority scan for every CAS attempt. Native
ref create/delete still lack conditional CAS, so first-touch destructive
recovery retains the documented single-writer-process boundary. `sync_branch`
continues to join the schema gate and cannot replace the merge capture during
its authority window.

The final predeclared five-pair production acceptance series passed the fixed
bulk-adopt gates. At 10K rows, production/comparator median operation time was
31/8 ms (**3.875×**) with maximum signed paired peak-RSS overhead 24,297,472
bytes. At 100K it was 136/35 ms (**about 3.886×**) with maximum overhead
32,604,160 bytes. Both are below 5× and 64 MiB; every route assertion,
exact-content check, and setup/operation/verification phase passed.

### Branch-delete orphaning exception

Branch deletion runs the healer first and then holds schema, the target branch,
and every accepted-catalog table gate through the native ref removal. An
unresolved sidecar scoped to that target does not permanently block deletion:
once those gates prove its in-process owner is no longer live, removing the
manifest branch makes its physical effects unreachable. The next write/open
records the orphan-discard recovery audit and deletes the sidecar. A
`SchemaApply` sidecar remains graph-global and blocks deletion. This exception
is specific to removing the authority that made the intent reachable; create,
merge, mutation, and load still reject relevant unresolved ownership.

### Native graph-branch control recovery

Graph branch create/delete do not use the graph-visible table-effect sidecar or
emit graph lineage. Their sole logical authority is Lance `BranchContents` for
the `__manifest` dataset, and Lance mutates that authority in two physical
phases:

- create shallow-clones `tree/{branch}` before writing `BranchContents`;
- delete removes `BranchContents` before reclaiming that tree.

Both controls first acquire shared stream-admission domains outside their
schema/branch/table envelope and re-check lifecycle authority under that
envelope. They refuse while any main-table lifecycle is `OPEN` or `DRAINING`.
They may proceed when all lifecycle rows are `SEALED`, because native ref
creation/deletion does not advance a graph table HEAD; this is the only Phase A
exception to the otherwise complete lifecycle effect fence.

Under the schema/branch/table control gates, create validates the name before
the clone and rejects a live graph name that is a physical path ancestor or
descendant of another live name. It then force-reclaims any absent-ref same-name
tree and performs at most two native attempts. An ambiguous result is accepted
only when fresh metadata has
the captured parent branch/version/incarnation plus exactly one new identifier
element and the target opens. Foreign or broken authoritative refs are never
deleted. Delete captures the exact target identifier; after an ambiguous error,
an absent ref is logical success, the same identifier preserves the original
error, and a different identifier is a typed delete/recreate conflict. Derived
tree cleanup is retried best-effort.

The accepted catalog is captured under the schema gate to derive the
conservative table envelope. After schema, source/target branch, and every
accepted-catalog table gate are held and recovery intent has been re-listed,
the control opens one operation-local coordinator and validates the catalog
against that same manifest state. Its namespace, source version/incarnation,
and exact delete identifier feed the native classifier. The handle-local active
coordinator is not refreshed before and after table-gate acquisition. After a
successful create or delete classification, derived read caches are invalidated
explicitly so a later same-name branch incarnation cannot reuse stale handles or
topology. This cache invalidation is derived-state hygiene, not the logical
authority transition.

All graph-dataset Lance opens share one process-wide `ObjectStoreRegistry`,
which reuses their object-store clients. Data tables use a graph-handle-scoped
cached `Session`; `__manifest` and other mutable-tip control datasets use a
zero-cache control `Session`. A full coordinator open or refresh still folds the
append-only manifest journal, but manifest state and lineage are decoded
together in one scan. This reduces duplicate opens and scans; it does not make
that remaining fold history-flat.

There is deliberately no branch-control sidecar: within the supported
single-writer-process topology, an absent ref makes a same-name tree unreachable
garbage; the path-prefix-disjoint namespace is what makes Lance's recursive
force cleanup exact. Same-name create is therefore the targeted reconciler.
First-touch data-table
forks remain sidecar-owned because they are physical effects of a graph-visible
mutation/load. Lance does not expose conditional ref create/delete, so this
classifier is not advertised as a cross-process branch-control fence.

Legacy prefix-overlap recovery is the one first-touch case that does not prove an
entire nested tree unreachable. If a Full sweep finds an ancestor first-touch
target with a live path-child, it keeps the sidecar. Open may complete for
leaf-first deletion only when the sidecar owns no physical table effect. A mixed
attempt that owns an effect plus an untouched fork must roll back as one recovery
outcome, so open fails closed while the child blocks fork cleanup. After an
existing handle or an offline Lance-level branch tool removes the child, a later
Full sweep reclaims the untouched fork, compensates the owned effect, and retires
the intent.

## Read-your-writes within a multi-statement mutation

A `.gq` query with multiple ops (e.g. `insert Person … insert Knows …`)
must observe earlier ops' writes when validating later ops (referential
integrity, edge cardinality). After MR-794 step 2+ this is implemented
via an in-memory `MutationStaging` accumulator in
[`crates/omnigraph/src/exec/staging.rs`](../../crates/omnigraph/src/exec/staging.rs),
shared by both `mutate_as` and the bulk loader:

- On the first touch of each table, the pre-write manifest version is
  captured into `expected_versions[table_key]` (the publisher's CAS
  fence at end-of-query).
- Each insert/update op pushes a `RecordBatch` into the per-table
  pending accumulator. Lance HEAD does **not** advance during op
  execution.
- Read sites (validation, predicate matching for `update`) consume
  `TableStore::scan_with_pending`, which scans committed via Lance
  and applies the same SQL filter to the pending batches via DataFusion
  `MemTable`. Same-query writes are visible to subsequent reads.
- Blob-bearing updates use the materializing variant: Lance reads only matched
  committed blob payloads as binary, the engine normalizes them back to the
  logical blob schema, and the full rows join the same pending-shadow union.
  Rewriting matched blob bytes costs I/O proportional to those bytes, but makes
  correctness independent of whether a physical index selects a different
  Lance merge plan.
- At end-of-query, `MutationStaging::stage_all` prepares exactly one staged
  transaction per touched table and `commit_all` commits it (concatenating accumulated
  batches; merge-mode dedupes by `id`, last-write-wins), and the publisher
  publishes the manifest atomically across all touched sub-tables. Existing
  tables stage before gate acquisition; a first-touch named-branch table stages
  after sidecar + fork under the gates so its uncommitted files live in the
  correct Lance branch tree. Cross-table conflicts surface as typed read-set or
  manifest conflicts.
- **Deletes stage too (MR-A).** Lance 7.0's
  `DeleteBuilder::execute_uncommitted` (#6658) makes delete a two-phase op,
  so deletes no longer inline-commit. Each delete records a predicate in
  `MutationStaging.delete_predicates`; at end-of-query `stage_all` combines a
  table's predicates into one `stage_delete` (a deletion-vector transaction,
  no HEAD advance) committed through the same `commit_staged` path as writes.
  A predicate matching zero rows stages nothing — no inline residual, and the
  zero-row drift class is closed by construction. The parse-time D₂ rule
  (below) still prevents inserts/updates from coexisting with deletes in one
  query.

This upholds the manifest-atomic mutation and read-your-writes invariants
tracked in [docs/dev/invariants.md](invariants.md).

### D₂ — parse-time mixed-mode rejection

A single mutation query is either insert/update-only or delete-only.
Mixed → rejected at parse time with a clear error directing the user to
split the query. This is a deliberate boundary, not a temporary limitation.
Inserts/updates accumulate as pending batches and deletes as predicates, and
both stage correctly; keeping a single query to one kind means read-your-writes
within that query stays unambiguous (a read never reconciles pending inserts
against same-query delete predicates) and each touched table commits at most one
version. Compose mixed operations by issuing separate atomic mutations (writes,
then deletes), or a branch + merge for one atomic commit. Allowing mixing would
instead require an in-query delete view, pending pruning, and per-table
two-commit ordering in the hot mutation path — complexity this boundary
deliberately avoids.

### MR-793 status (storage trait two-phase invariant) — complete

MR-793 hoists the staged-write pattern into a `TableStorage` trait
surface with sealed-trait enforcement and opaque `SnapshotHandle` /
`StagedHandle` types — see `crates/omnigraph/src/storage_layer.rs`.
The trait is the canonical surface for new engine code; existing call
sites route graph-visible effects through it. Capability and statistics
surfaces for planner decisions remain separate roadmap work.

Three writers have been migrated onto staged primitives:

* **`ensure_indices`** (`db/omnigraph/table_ops.rs::ensure_indices_for_branch`)
  — runs the branch-aware, roll-forward-only recovery barrier before schema-idle
  checking, base capture, or index planning. A roll-forward-eligible
  `EffectsConfirmed` predecessor whose captured token still holds therefore
  finishes on the same handle before this attempt starts, while an `Armed`
  intent that needs compensation returns `RecoveryRequired` before any new
  index artifact is staged. A typed `stage_create_indices` request then
  combines every missing BTree,
  Inverted/FTS, and full-table vector artifact for one table into one Lance
  `Operation::CreateIndex`, followed by one `commit_staged_exact`. Which index a
  `@index`/`@key` property gets is dispatched by type via
  `node_prop_index_kind` (enum + orderable scalar → BTree, free-text String →
  Inverted/FTS, Vector → vector). This build is
  existence-gated (it creates a *missing* index over current fragments); folding
  fragments appended afterward into an *existing* index is `optimize`'s
  `optimize_indices` pass — an inline-commit residual, not a staged write (Lance
  exposes no uncommitted index-optimize), covered by the optimize recovery
  sidecar (see [maintenance.md](../user/operations/maintenance.md)).
* **branch merge** (`exec/merge.rs`) — all keyed new and changed rows are
  buffered into actual chunks capped at 8,192 rows and 32 MiB; strict new-row
  chunks use `stage_keyed_write(StrictInsert)` and changed-row chunks use
  `stage_keyed_write(Upsert)`, all in one pre-minted recovery chain. The narrow
  complete-certificate route instead gives each source-interval chunk an
  internal `ProvenInsertChunk` and calls `stage_proven_strict_insert`; that
  adapter skips the target probe/join while still emitting the same filtered,
  certified `Update`. A row above 32 MiB—including cumulative materialized blob
  payloads—or a per-table chain above 1,024 data transactions fails before arm.
  Deleted IDs form exact escaped-filter chunks capped at 8,192 IDs and 32 MiB
  of filter text; the whole retained delete plan is separately capped at 32
  MiB, and every delete chunk consumes one of those 1,024 transactions.
  Both modes produce exact-`id` filters and then use `commit_staged`; deletes
  use `stage_delete` + `commit_staged` (MR-A). Bare Lance Append and the generic
  merge-insert helper are test-only; the proven adapter's temporary
  `InsertBuilder` Append descriptor is replaced before commit. The chunk chain
  is physical only: it has one v9 sidecar and one final graph publish, so a
  later-chunk failure is `RecoveryRequired`, never partial graph visibility.
* **`schema_apply` rewritten_tables** (`db/omnigraph/schema_apply.rs`)
  — rewrites use `stage_overwrite` + `commit_staged`, including empty-table
  rewrites via a zero-fragment Lance `Operation::Overwrite`.

Rust visibility is the primary graph-write boundary: the raw storage,
handle-cache, and coordinator modules are crate-private. Public snapshot access
returns a read-only table facade rather than Lance's writable `Dataset`, and its
scan facade executes the configured read without exposing Lance's raw `Scanner`
or physical plan (a scan execution node can otherwise reveal its dataset). A
defense-in-depth integration test (`tests/forbidden_apis.rs`) walks engine source
and rejects known direct Lance open/inline-commit shapes outside exact gateway
files. It classifies public async inherent `Omnigraph` methods and loader
conveniences, every crate-visible async coordinator method, and exact per-file
occurrences of registered durable-call shapes, including recovery. Its
syntax-tree checks cover the concrete method/UFCS/raw-Dataset and selected
rename/macro forms pinned by scanner self-tests; they are deliberately not a
substitute for Rust visibility or a claim to expand arbitrary macros and
function-pointer aliases. A new supported writer or durable gateway is therefore
an explicit registry change rather than an accidental call site.

The `failpoints` Cargo feature retains one doc-hidden, registry-classified
`TestOnly` manifest-publish helper for adversarial integration fixtures. It is
an explicit non-production exception: enabling failpoints already opts into
fault-injection behavior and is outside the supported SDK surface.

The "finalize → publisher residual" described below applies equally to
the migrated writers — Lance has no multi-dataset atomic commit primitive, so
the per-table `commit_staged` → manifest-publish gap is the same drift class.
The shipped sidecar recovery protocol closes that gap.

### The storage surface has no inline-commit residual

MR-793's acceptance criterion §1 ("`TableStore` (or successor) public API has
no method that performs a manifest commit as a side effect of writing") now
holds more narrowly **by construction**: `TableStore` / `TableStorage` are
crate-private, and their internal `db.storage()` surface exposes only staged
primitives + reads. Lance 7.0's `DeleteBuilder::execute_uncommitted`
([#6658](https://github.com/lance-format/lance/issues/6658)) moved delete to
`stage_delete`; pinned Lance's full-table index `execute_uncommitted` shape moved the
last vector build into `stage_create_indices`. `InlineCommitResidual`,
`storage_inline_residual()`, and inline `create_vector_index` are removed.
Generic multi-segment exact index publication remains covered by Lance
[#6666](https://github.com/lance-format/lance/issues/6666), but it is not used by
the current one-segment full-table shape.

`SnapshotHandle`'s Lance field is private. The few crate-private raw accessors
remain only for enumerated read/maintenance bridges and are exact-counted by the
same conformance guard. In particular, only Optimize's two physical compaction
paths take mutable Dataset ownership; read-only planning, repair classification,
cleanup enumeration/GC, export, and blob access use registered borrows/handles.
Adding another extraction changes its registered count and fails the guard
rather than silently reopening a data-write side door.

The parse-time D₂ rule remains a deliberate boundary (constructive XOR
destructive per query), not residual scaffolding. The
`tests/forbidden_apis.rs` guard catches direct `lance::*` inline-commit misuse
outside the storage layer and also pins the retired residual symbols absent.

### Load modes use staged, closed-semantics primitives

The bulk loader's Append, Merge, and Overwrite modes all use the
staged-write path described above. `LoadMode::Append` is a public mode name for
strict insert: it uses `stage_keyed_write(StrictInsert)`, rejects an existing or
freshly re-probed effect-free concurrently inserted `id` with `KeyConflict`,
never updates the existing row, and certifies a successfully staged pure
insertion link after its exact preflight. A broad retryable Lance conflict with
no fresh exact-ID match causes full strict-mode reprepare, not a false duplicate.
`LoadMode::Merge` uses `stage_keyed_write(Upsert)`; an
effect-free retryable conflict causes full reprepare and validation from a new
base, never replay of stale staged batches. An all-new one-attempt Merge may
receive the same optional certificate from its completed statistics; a mixed
upsert does not. Bare Lance Append is not a production graph-write route.
Append and Merge are both single-transaction per
touched table and fail before sidecar arm above 8,192 rows or 32 MiB. Operators
split larger incremental inputs into separate graph commits; initial bulk
replacement uses Overwrite. For Blob values supplied as external URIs, Append
and Merge copy the referenced payload under the same 32 MiB aggregate pre-read
ceiling; Overwrite retains the external URI cell as a reference.

`LoadMode::Overwrite` accumulates
replacement batches in memory, validates node/edge constraints, referential
integrity, and edge cardinality before any Lance HEAD movement, stages
each touched table with Lance `Operation::Overwrite`, then runs
`commit_staged` under the normal `SidecarKind::Load` recovery sidecar
before publishing `__manifest`. `OMNIGRAPH_LOAD_CONCURRENCY` applies to the
fragment-writing stage only; the commit and manifest publish run while holding
the root-shared stream-admission → schema → branch → sorted-table gates.
Empty-table overwrite is
represented as a valid zero-fragment Lance `Overwrite` transaction, not as
truncate-then-append.

### RFC-026 private stream foundation, B1 core, and B2a retain-all gate

Internal schema v8 activates the deliberately bounded, data-bearing MemWAL
format. Phase A's enrollment foundation and Phase B1's one-generation row/fold
core are always compiled but remain crate-private. Only the doc-hidden callable
integration-test seam is feature-gated; there is no schema, SDK, CLI, HTTP,
Cedar, OpenAPI, or production caller.

Phase A still owns physical enrollment:

- One crate-private adapter enrolls an existing canonical-main exact-`id` table
  into exactly one empty, unsharded Lance MemWAL shard. It requires a main-only
  graph.
- Enrollment holds an exclusive root-scoped process-local admission lease
  outside schema → branch → sorted-table gates. A recovery-v10
  `StreamEnrollment` sidecar binds exact main authority, stable table identity,
  the pre-enrollment HEAD witness, pre-minted enrollment/shard IDs, fixed
  lineage, and the sole allowed `N -> N + 1` initializer effect.
- Classification accepts only no effect, exact index-only, or exact
  index-plus-empty-shard. No effect retires the intent; index-only provisions
  the pre-minted empty shard; complete state publishes the pointer plus `OPEN`
  lifecycle in one manifest CAS. Once an effect exists, recovery only rolls
  forward. It never restores, deletes, reclaims, or adopts a
  compatible-looking MemWAL artifact.

Phase B1 adds the private row path:

- Independently opened handles share one root-scoped registry keyed by stable
  table identity, enrollment ID, and shard ID. The qualified B1 envelope permits
  one resident writer for the graph root, one generation, at most 8,192 rows,
  8,192 batches, and 32 MiB of Arrow payload. The worker config prevents Lance
  auto-rollover.
- A put accepts one non-empty, already-normalized physical `RecordBatch` and one
  contiguous caller-ordinal range. It rechecks schema, lifecycle, binding,
  current HEAD, and batch shape while holding shared admission. Cold admission
  acquires that lease before `mem_wal_writer` can claim a higher epoch; warm
  admission repeats the same checks immediately before invocation.
- Cheap raw row/byte bounds reject obviously over-cap input before recovery I/O;
  a raw-fit batch then receives exact post-tombstone validation at that same
  pre-recovery boundary. After any recovery/authority prelude, the exact charge
  is recomputed and reserved against the root's 32-MiB aggregate budget. Every
  put then follows one order: exact charge → shared admission → same-key input
  queue → worker-mode inspection, before detached ownership or cold claim.
  Queue-first acquisition is forbidden because it can deadlock with the fair
  exclusive fold admission path.
  The queued permit is released on effect-free refusal or transferred without
  double-counting into the resident generation. Thus concurrent callers cannot
  accumulate an unmeasured queue outside the stated memory ceiling.
- The worker invokes Lance `put_no_wait` in a detached, worker-owned task and
  acknowledges only after its durability watcher succeeds. Dropping the caller
  or reaching a deadline does not cancel an invoked Lance future or release the
  admission lease. Any post-invocation outcome that cannot prove the watcher
  result is `AckUnknown`; the worker retires and retains ownership through its
  quiesced abort path rather than reusing possibly affected state.
- A cold reopen classifies the exact shard state. Replayed or already-flushed
  unmerged residue is fold-only: it is never passed to another put. B1 uses the
  pinned public BatchStore replay-watermark bridge before reseal, avoiding RC.1's
  replay-prefix multiplication, and refuses the unsafe cross-generation watcher
  shape rather than relying on its writer-wide watermark. It installs exact
  recovered accounting and a fold-only marker before releasing the cold
  opener's queue. New callers are refused before adding charge; callers already
  charged before replay was observable drain normally, and that narrow overlap
  is recorded honestly even when the ledger temporarily exceeds the nominal
  root cap.
- Idle eviction performs a fresh authority check while retaining shared
  admission. Seal/fold takes exclusive admission and holds it continuously
  across cut, independent drain proof, scan, staged table effect, and manifest
  publication. No second writer can claim the shard in that interval. Before a
  cold fold invokes Lance's opener, it reserves the full 32-MiB generation plus
  resident/pending slots and the fold-only marker. The owned opener shares the
  original seal deadline; timeout retains the opener, exclusive authority,
  inflight permit, reservation, and opening slot until unclaimed release or
  claimed-writer retirement. Any ownership-transfer ambiguity fails closed.

The strict fold scans only the selected fresh generation with
`LsmScanner::without_base_table`. It charges each scanner batch by the logical
memory of its selected slices against the same 8,192-row/32-MiB generation
limit, then uses an Arrow take of every selected row to build dense owned arrays
before retaining that batch. This avoids carrying a small logical slice through
the fold with the scanner's much larger sparse parent buffers. The fold
validates those already-normalized rows against one captured graph snapshot and
stages an exact-`id` upsert plus exactly one `MergedGeneration` marker in the
same Lance `Update`. It then arms a
recovery-v11 `StreamFold` sidecar containing the physical cut, pre-minted
transaction identity, fixed lineage, and complete pointer/lifecycle outcome.
Under admission → schema → main → table gates, the final barrier re-lists all
main-branch recovery intents, revalidates authority, commits with zero
transparent conflict retries, durably confirms the exact effect, and publishes
one `__manifest` CAS. That CAS is the only graph-visibility point: it advances
the table pointer, `CurrentHeadWitness`, epoch floor/lifecycle, and graph lineage
together. Exact no-effect can retire and retry within the bounded attempt; exact
planned `N + 1` is rolled forward on recovery. A foreign, buried, differently
marked, or authority-mismatched effect fails closed.

B1 performs no fresh-tier reads and no generation GC. Acknowledged rows become
query-visible only after fold. The support boundary remains main-only,
unsharded, one resident stream worker, and one live writer process; the
admission lease is not a distributed fence.

Gate R0's closure defect is now repaired. The deterministic legal high-entropy
near-cap cell acknowledges without moving either manifest or base-table
version, materializes one generation, then folds 8,192 rows and publishes one
table version through one `__manifest` visibility CAS. The dense scanner-batch
copy keeps the accepted shape inside its logical 32-MiB limit. A separate
subprocess measurement recorded a 284,934,144-byte isolated fold RSS delta
(about 272 MiB), below the 384-MiB remeasurement tripwire. That tripwire guards
the measured implementation shape; changing the admission or fold strategy
requires remeasurement.

The private B2a gate now implements **unbounded retain-all**. It accepts that
stock-RC.1 materialization uses randomized generation roots and that a crash or
cold replay can leave more physical objects. OmniGraph adds no retained-byte,
object-count, file-count, or history quota, no quota-enforcement attempt ledger,
and never deletes a canonical durable `_mem_wal` object. Lance may remove only
its losing shard-manifest-CAS `.binpb.tmp.<uuid>` staging object; that object
never became shard authority or retained history. Complete and partial
unreferenced generation output remains non-authoritative. Parent shard discovery
may list its common prefix, but admission, retry, and cold reopen never descend
into, read, mutate, adopt, or delete the subtree. A shared strict classifier and
local/configured-RustFS provider-failure cells pin those rules and the typed
effect-free `Lance`, post-invocation `AckUnknown`, and post-cut
`RecoveryRequired` outcomes. Current-object LIST evidence remains a useful
retention/path-shape check but does not claim coverage of incomplete multipart
uploads, provider versions/delete markers, local staging, or billed bytes.
Provider exhaustion is surfaced as a storage failure and can halt admission,
fold, or recovery progress; it never becomes silent loss, an early
acknowledgement, or an alternate publication path. The 8,192-row/32-MiB logical
Arrow admission envelope, deadlines, retry counts, and ambiguity outcomes
remain bounded.
RC.1's missing cross-open materialization receipt and complete physical-output
envelope are therefore not blockers for this profile. They remain relevant to
the future B2b managed-reclamation design. The checked-in 1/8/32/128 local and
configured-RustFS instrument separates warm acknowledgement, cold replay, fold,
visibility, table, graph-manifest, adapter, advisory object, and whole-process
RSS terms. Warm acknowledgement retains a flat operation shape while serialized
authority and combined retained-history work grow; the observations are not a
quota, latency SLO, provider bill, or isolated WAL slope. B2a activates no new
format or product state.

RFC-026's common B2 inventory specifies—but does not implement—the next product
contract.
`@stream` declares eligibility but explicit, request-idempotent enrollment
creates the stream incarnation and physical binding. A caller-stable `write_id`
and exact predecessor then form an
opaque compare-and-chain token; trusted contributor/digest/tagged-origin and
same-generation chain-certificate metadata travels with the row. A
manifest-selected graph-global token dataset is the sole post-fold per-key
sequencing authority and publishes atomically with the base pointer under the
shared stream-token gate/recovery envelope. Because that participant is
graph-global, every token-moving sidecar blocks every manifest/main writer,
including table-disjoint writers. A final-barrier late discovery releases all
admission, schema, branch, token, and table gates and restarts from the root
barrier; it never recovers while retaining a leaf gate. Durable
`OPEN -> DRAINING -> SEALED -> OPEN` state carries a monotonic lifecycle
revision. Every external mutating management call compares that revision and
records a bounded, complete terminal receipt; roll-forward-only resume/abort and
bounded `REPLACE`/`WITHDRAW` correction use recovery-v12. The B2b
managed-reclamation profile requires Lance-owned durable reclamation with whole-cut proof,
attempt/receipt recovery, bounded history checkpointing, strong PUT/DELETE
inventory plus multipart accounting/abort or durable accounting, a post-success
epoch check, and a source-derived enforced
retained-object/byte admission watermark. Patched epoch claims are
sentinel-first and manifest-named: ordinary open/quiesce/resume/checkpoint
claims preserve the prior replay cursor and classify its tail; only a proved
whole-cut reclaim may advance to its new sentinel. The patch also needs a
genesis bootstrap plus a new stock-RC.1-rejected MemWAL details kind, bounded
ordinary-claim/reclaim receipt history, durable materialization-attempt limits,
exact multipart accounting/abort, and reserved control headroom. A single
bootstrap-selected reserve-first ledger serializes all shards per physical
binding before any WAL/upload effect and reconstructs/settles from exact
inventory; materialized bytes remain `observed` while only possible remainder
stays reserved. Versioned/soft-delete/Object-Lock stores are refused unless all
retained physical bytes are countable and eligible versions permanently
deletable. Measurements validate those bounds; they do not create them. Generic
Lance cleanup does not reclaim `_mem_wal`, and OmniGraph must never delete its
raw paths. Internal
schema v9/config-v3/state-v2/recovery-v12 and every public policy/wire surface
remain inactive until the common crash, no-delete/provider-failure,
cross-version, authorization, and parity evidence passes. The selected
unbounded retain-all profile has no physical watermark and no graph-history
quota. If a future B2b bounded/managed profile is pursued, its physical
watermark is per binding and does not bound base/token or shared manifest
history; that profile therefore also needs a separate
manifest-authoritative, graph-global `GraphHistoryBudget`. Every manifest
publisher—stream and ordinary—would take the graph-history gate, reserve the exact
logical publication plus a source-bounded physical-growth envelope before
effect, and settle pending recovery charges exactly once. Initial B2b would hold that
gate continuously from final reserve/sidecar arm through physical effects,
manifest CAS, and charge finalization, so only one expected budget revision is
live. Its dynamic per-stream
closure reserves cannot be consumed by ordinary commits or by another stream;
at the shared floor ordinary work returns `GraphRebuildRequired`, while the
named stream retains correction, quiesce, and `SEALED` export/rebuild headroom.
That future bounded profile's gate order is sorted relevant stream admission →
graph history → schema → main branch → stream token → sorted graph
tables. The selected unbounded profile omits the graph-history gate: sorted
relevant stream admission → schema → main branch → stream token → sorted
graph tables. Late global
authority discovery releases the complete suffix and restarts at the root
barrier. A writer with no relevant stream does not acquire a nonexistent
storage/history budget.
Indefinite in-place maintenance remains a later accepted strand.

### Open-time recovery sweep

The staged-write rewire eliminates one drift class **by construction at
the writer layer**: an op that fails before pushing to the in-memory
accumulator (validation errors, missing endpoints, parse-time D₂
rejection) leaves Lance HEAD untouched on every staged table. This is
the case the `partial_failure_leaves_target_queryable_and_unblocks_next_mutation`
test pins.

A second, narrower drift class — the **finalize → publisher window** —
is closed across one open cycle by the open-time recovery sweep:

`MutationStaging::stage_all` prepares the table transactions and `commit_all`
runs their independent HEAD advances before the publisher commits the manifest. Lance has
no multi-dataset atomic commit, so the per-table `commit_staged` calls
are independent operations: if commit_staged on table N+1 fails *after*
commit_staged on tables 1..N succeeded, or if the publisher's CAS
pre-check rejects *after* every commit_staged succeeded, tables 1..N
are left at `Lance HEAD = manifest_pinned + 1`.

**Recovery protocol** (lifecycle of the established graph-visible and
maintenance writers —
`MutationStaging::commit_all`, `schema_apply::apply_schema_with_lock`,
`branch_merge_on_current_target`, `ensure_indices_for_branch`,
`optimize_all_tables`):

Before Phase A, the writer holds shared stream-admission leases outside its
final schema → branch → table gates. Under those gates, every table must have no
stream lifecycle authority and existing physical targets must still match their
manifest pins. Ahead drift is never folded
or claimed by manufacturing a new sidecar; it is attributed to an existing recovery
intent or refused with explicit `omnigraph repair` guidance. First-touch targets use
the separate sidecar-before-ref protocol. SchemaApply also verifies that every AddType
target dataset path is absent, so recovery cannot register an orphan or foreign dataset
as if this apply created it. A pure type rename is metadata-only and retains the same
identity, incarnation, path, and Lance version.

1. **Phase A**: writer writes a sidecar JSON to
   `__recovery/{ulid}.json` BEFORE its first independently durable physical
   effect (including a first-touch Lance branch ref) or HEAD-advancing commit
   (`commit_staged`, or `compact_files` for `optimize_all_tables`,
   which advances the Lance HEAD via a reserve-fragments + rewrite
   commit rather than a staged write). The
   sidecar names every `(stable_table_id, incarnation_id, table_key,
   table_path, expected_version, post_commit_pin)` it intends to commit + the
   writer kind + actor_id.
   For a first-touch named-branch Mutation/Load table, Phase A is followed by
   target-ref creation and branch-local `stage_*`; the v9 sidecar already
   carries its pre-minted transaction identity. Branch merge's v9 envelope
   distinguishes multi-commit HEAD effects from ref-only forks, records each
   multi-commit effect's ordered exact transaction chain, and records the
   complete intended manifest delta, including pointer-only slots. SchemaApply's
   v9 envelope captures the main native branch identity, exact optional
   graph head, accepted schema identity, fixed original lineage + initiating
   actor, and fixed rollback id. Every existing-table overwrite and AddType
   first-touch create has one pre-minted Lance transaction identity; the latter
   is a strict read-version-zero create. The sidecar also carries the
   complete registration/update/tombstone delta, including metadata-only applies
   whose table-effect set is empty. EnsureIndices' v9 envelope captures native
   branch + graph-head + schema authority, fixed original and rollback lineage,
   one pre-minted mixed CreateIndex
   transaction per touched table, and the complete table-pointer delta. A
   first-touch effect also records its inherited source version and later binds
   the exact created ref identity. The persisted writer-specific payload field
   names (`protocol_v3`, `protocol_v4`, `protocol_v7`, and `protocol_v8`) remain
   for shape continuity, but every established writer in this list emits schema
   v9 and every
   ownership-bearing field carries the stable identity pair. Pre-v9 files are
   never upgraded by alias inference or serde defaults.
2. **Phase B**: writer's per-table `commit_staged` loop runs.
   - **Phase-B confirmation:** a v9 `BranchMerge` writer
     advances each table's HEAD by *several* exact commits (new-row strict-insert
     filtered Update → changed-row upsert filtered Update → delete). Recovery
     proves a contiguous prefix of the pre-armed transaction
     chain rather than inferring ownership from numeric HEAD movement. After the
     whole per-table loop finishes, the writer atomically confirms each exact
     achieved version, the complete logical manifest delta, and first-touch ref
     identities, then proceeds to Phase C. V9 Mutation/Load sidecars also
     confirm: each table must
     match the staged Lance transaction's `(read_version, uuid)`, and the
     sidecar records the exact `SubTableUpdate` plus original lineage intent.
     This is the commit point of the recovery WAL: a crash *after* confirmation
     rolls forward only when the captured branch token still matches; a crash
     *during* Phase B (sidecar still unconfirmed) rolls back. V9
     SchemaApply follows the same boundary with writer-specific effects: exact
     `Overwrite` for an existing table and exact version-one `Create` for a new
     target path, all committed with zero transparent conflict retries. After
     every achieved identity/version and all schema staging files are durable,
     it confirms the complete registration/update/tombstone delta and moves
     `Armed → EffectsConfirmed`. V9 EnsureIndices likewise requires one
     exact achieved transaction at `read_version + 1` per table, binds every
     first-touch ref identity, confirms the complete table-pointer delta, and
     only then moves `Armed → EffectsConfirmed`. Optimize also emits an
     identity-bearing v9 envelope, but its bounded maintenance payload
     intentionally has no exact caller-minted transaction confirmation boundary.
3. **Phase C**: publisher commits the manifest.
4. **Phase D**: writer deletes the sidecar.

> **Phase letter convention.** Throughout the recovery code, log
> messages, failpoint names (e.g. `branch_merge.post_phase_b_pre_manifest_commit`),
> and the per-writer integration tests, "Phase A/B/C/D" refers
> exclusively to the four-step lifecycle above. The per-table
> staged-write contract (`stage_*` then `commit_staged`, two steps)
> is referred to by those API verbs — never by phase letters — so a
> reader of `recovery.rs`, `failpoints.rs`, or this document only
> encounters phase letters in the per-writer context.

RFC-026's named “Phase A foundation”, “Phase B1 private core”, and “B2a
retain-all gate” are RFC slice names, not steps in that four-phase convention. Recovery-v10
`StreamEnrollment` uses a dedicated exact initializer classifier: no effect
retires, index-only provisions the fixed empty shard, and
index-plus-empty-shard publishes pointer + lifecycle. Recovery-v11 `StreamFold`
accepts only exact no effect or its pre-minted `N + 1` Update carrying the fixed
generation marker; a confirmed exact effect publishes its fixed
pointer/lifecycle/lineage outcome. Neither envelope grants permission to adopt
an ambiguous artifact.

A failure between Phase A and Phase D leaves the sidecar on disk. The
next `Omnigraph::open` (gated on `OpenMode::ReadWrite`) runs the
recovery sweep in `crates/omnigraph/src/db/manifest/recovery.rs`:

The established writers emit sidecar schema v9. RFC-026 additionally emits the
dedicated recovery-v10 `StreamEnrollment` and recovery-v11 `StreamFold`
envelopes. The JSON field names
`protocol_v3`, `protocol_v4`, `protocol_v7`, and `protocol_v8` are retained
payload-version names for mutation/load, BranchMerge, SchemaApply, and
EnsureIndices respectively; they do not mean the outer envelope is pre-v9.

- For each sidecar in `__recovery/`, compare every named table's
  Lance HEAD to the manifest pin. Classify per the all-or-nothing
  decision tree (RolledPastExpected / NoMovement / UnexpectedAtP1 /
  UnexpectedMultistep / IncompletePhaseB / InvariantViolation). For a
  `BranchMerge` payload, a moved HEAD with no `confirmed_version`
  classifies as `IncompletePhaseB` (a partial multi-commit publish) and forces
  roll-back; with a `confirmed_version`, roll-forward targets exactly that
  version. The v9 BranchMerge envelope's `protocol_v4` payload additionally requires the captured
  target token, fixed original/rollback lineage ids, the exact ordered data
  transaction chains, exact confirmed physical effects, first-touch ref
  identities, and the complete confirmed manifest delta. A changed target token
  is rollback-only and can never re-parent the merge onto the winner. Recovery
  refuses a foreign or non-contiguous transaction instead of restoring through
  it, and recognizes an already-landed exact compensation restore on restart.
  The v9 Mutation/Load envelope's `protocol_v3` payload additionally requires `EffectsConfirmed`, the exact
  Lance transaction identity at the confirmed version, the original immutable
  manifest delta, and a matching captured authority token. A changed token is
  rollback-only; an unknown/foreign effect is refused rather than adopted.
  The v9 SchemaApply envelope's `protocol_v7` payload applies the same exact-ownership rule to each existing
  overwrite and first-touch create, and additionally binds accepted + target
  schema identities, fixed original/rollback lineage, the initiating actor, and
  its complete registration/update/tombstone delta. `Armed` is rollback-only;
  `EffectsConfirmed` can roll forward only when every achieved transaction and
  output matches and the captured main authority is still live. A disjoint
  authority winner is preserved while recovery compensates only owned effects.
  If foreign movement buries an owned same-table effect, recovery fails closed
  with the sidecar intact instead of restoring through or adopting the winner.
  The v9 EnsureIndices envelope's `protocol_v8` payload applies the exact rule to each one-transaction mixed
  index batch: the observed transaction UUID/read version and achieved
  `expected + 1` version must match the plan, `EffectsConfirmed` must carry the
  complete fixed table-pointer delta, and a first-touch named branch must retain
  the confirmed Lance ref identity. Changed authority is rollback-only; a
  foreign or buried index commit is never adopted or restored through. A
  pre-v9 identity-less file is refused rather than upgraded or classified by
  mutable alias.
  Schema-v10 `StreamEnrollment` instead requires its exact canonical-main
  authority, identity/binding, `N -> N + 1` initializer witness, and pre-minted
  empty-shard state. No effect retires; either exact allowed effect converges
  forward through the dedicated completion path; any foreign, buried,
  data-bearing, mismatched, or ambiguous state fails closed with the sidecar
  intact.
  Schema-v11 `StreamFold` requires the exact binding, immutable generation cut,
  pre-minted transaction, `MergedGeneration` marker, fixed lineage, and complete
  pointer/lifecycle outcome. Exact no effect retires; only its planned `N + 1`
  effect rolls forward. Foreign, buried, differently marked, or
  authority-mismatched state fails closed with the sidecar intact.
  First-touch rollback deletes only the exact owned version-one dataset and only
  while no manifest registration or competing recovery claim owns the path. A
  foreign winner at an unregistered first-touch path is left untouched and is
  never adopted; recovery can still roll back this attempt's other owned effects.
  An Armed first-touch intent with no owned transaction is deferred by live
  roll-forward-only healing because another handle may still own it. Quiesced
  full recovery tolerates an absent target ref (either crash before clone or a
  clone-only tree with no `BranchContents`) and force-reclaims that absent-ref
  target idempotently. If an authoritative ref exists, recovery removes it only
  when it is exactly unchanged and no other pending sidecar claims it. With
  competing claims, the current no-effect sidecar discards itself without
  touching the ref; the final survivor owns cleanup/recovery.
  During partial rollback, no-effect refs are removed before the rollback
  outcome is published so a retry cannot strand them. If a legacy live
  path-child blocks that cleanup, rollback returns an error and read-write open
  fails closed; only a sidecar proven to own no table effect may defer cleanup
  while returning an open handle.
- If any table is `InvariantViolation` (Lance HEAD < manifest pinned —
  should be impossible), **abort** with a loud error and leave the
  sidecar on disk for operator review.
- Otherwise, if every table is `RolledPastExpected`, **roll forward**:
  a single `ManifestBatchPublisher::publish` call extends every pin
  atomically. For v9 SchemaApply, the exact fixed manifest outcome lands
  first; only then does the writer or recovery promote the matching staged
  source/IR/state contract. A crash after one or two renames is completed by
  proving that fixed commit + delta visible and validating every remaining/live
  file against the same target identity.
- Read-only open performs this check without repairing anything: it may serve an
  unpublished v9 attempt against the old manifest/schema pair, but it returns
  `RecoveryRequired` when the fixed original manifest outcome is visible and the
  target schema identity is not yet fully live. It always refuses a pending
  schema-v10 `StreamEnrollment` intent because enrollment may carry uncovered
  format effects, and it always refuses a pending schema-v11 `StreamFold`
  because that intent may own an unpublished table effect. A read-write open
  completes exact recoverable state.
- On a live handle, query, export, graph-index, and blob-read capture takes the
  process-local schema gate just long enough to bind one manifest snapshot to one
  immutable catalog rebuilt from the accepted contract. It never trusts the
  handle's warm ArcSwap after another handle may have applied a schema. Execution
  releases the gate and continues on that captured pair, so a long query does not
  block the whole schema apply. The gate does not serialize a long-lived reader
  in another process; that topology still requires the distributed
  schema-publication fence described in the known gaps.
- Otherwise **roll back**: per-table `Dataset::restore` to the
  manifest-pinned table version, then a single `ManifestBatchPublisher::publish`
  of the restored HEAD — symmetric with roll-forward, so `manifest == HEAD`
  after recovery (no residual drift). This convergence is what lets a
  failed-then-retried schema apply succeed instead of failing one version higher
  each iteration. The audit row's `to_version` records the logical
  rolled-back-to version (`manifest_pinned`); the manifest is published at the
  restore commit (`manifest_pinned + 1`, same content).
- After a successful roll-forward or roll-back, an internal
  `_graph_commit_recoveries.lance` row records `recovery_kind`,
  `recovery_for_actor` (the original sidecar's actor), `operation_id`, and
  exact per-table outcomes. V9 Mutation/Load, BranchMerge, SchemaApply, and
  EnsureIndices roll-forward publish the
  interrupted writer's fixed lineage intent, including its original actor.
  Schema-v10 `StreamEnrollment` likewise publishes its fixed lineage and
  records the exact `N -> N + 1` enrollment outcome when recovery completes an
  effect. Schema-v11 `StreamFold` publishes its fixed fold lineage and records
  the exact recovered fold outcome in the same recovery audit.
  Their rollback paths reuse pre-minted recovery commit ids and durable audit
  plans, with the recovery actor. Other rollback and legacy recovery commits use
  `actor_id = "omnigraph:recovery"`. Ordinary
  commit history is therefore not a complete recovery enumeration, and the
  CLI currently has no public query for the recovery-audit table.
- Sidecar deleted as the final step.

Triggers for the residual: transient Lance write errors during finalize
(object-store retry budget exhaustion, disk full); persistent publisher
contention exceeding `PUBLISHER_RETRY_BUDGET = 5` retries.

**Long-running servers**: the write entry points (`load_as`,
`mutate_as`, `apply_schema_as`, `branch_merge_as`), the explicit
`ensure_indices{,_on}` reconciler, and `Omnigraph::refresh` run
roll-forward-only recovery in-process
(`recovery::heal_pending_sidecars_roll_forward`) — the common
Phase B → Phase C residual closes on the next enrolled entry, without a
restart and without an explicit refresh. The heal lists `__recovery/`
(one `list_dir`; empty in the steady state) and, per sidecar, acquires
stream-admission domains (shared for ordinary effects, exclusive for
`StreamEnrollment`) → schema → branch → sorted-table gates that overlap the writer's guarded
sidecar lifetime. RFC-022 mutation/load writers hold the complete order. Branch
merge holds schema plus source/target branch authority for its whole attempt and
then the all-catalog source/target table envelope. SchemaApply holds admission
domains → schema → main branch → every live table; EnsureIndices holds admission
domains → schema → target branch → every table
in its durable work plan. SchemaApply's v9 envelope is its full exact adapter:
it holds fixed authority/lineage and exact table identities from arm through one
`ExactGraphHead` publish, including an empty effect set for metadata-only changes.
Its owned first-touch cleanup and partial-table rollback happen only during Full
recovery; the in-process healer remains roll-forward-only. EnsureIndices' current
v9 envelope holds fixed authority/lineage, one exact mixed-index transaction
per table, the complete manifest delta, and exact first-touch ownership from arm
through one `ExactGraphHead` publish. `Armed` is rollback-only;
`EffectsConfirmed` rolls forward only while the captured authority still matches.
Pre-v9 sidecars are never completed by inferring identity from their aliases.
Optimize uses bounded effect provenance inside an identity-bearing v9 graph-wide
visibility envelope. Its entry recovery probe is
a fast path; it then acquires admission domains → schema → main branch → every accepted-catalog table gate,
loads one operation-local accepted catalog, relists recovery, and plans productive work
from one fresh snapshot. All productive tables share one multi-pin Optimize sidecar;
their compact/reindex/index-create effects remain bounded-parallel, but no task publishes
or deletes recovery independently. After every effect settles, one maintenance-class
monotonic batch CAS publishes every still-needed pointer and one lineage commit. A
pointer already at or beyond Optimize's achieved version is converged and omitted rather
than forcing strict graph-head OCC. Any post-arm error returns `RecoveryRequired` and
leaves the shared intent for all-or-nothing v9 recovery. Main remains held through final
physical-only `__manifest` compaction so a new main recovery intent cannot arm before raw
manifest movement finishes. The bounded Optimize classifier has no exact
transaction/authority/fixed-lineage proof and therefore stays within the documented
single-writer-process recovery model. Replacing it with exact provenance is deferred
until Lance exposes a stable public caller-controlled transaction API for the complete
compact/reindex operation and OmniGraph has distributed recovery ownership or fencing;
transaction identity alone would not make destructive cross-process recovery safe.
The manager is shared by every
`Omnigraph` handle for one canonical local root identity (relative, absolute,
and symlink aliases converge; object-store/custom schemes stay opaque), so this
also serializes a refresh or separately-opened handle against a live writer instead of rolling its
in-flight sidecar forward from under it (a sidecar whose queues can be
acquired belongs to a writer that finished or died; an existence
re-check after the wait skips the finished case). Lock order is stream-admission
domains → schema → branch → sorted tables → coordinator, matching the writer
effect path.
Mutation/load, branch merge, SchemaApply, and EnsureIndices perform one additional
`list_dir` after acquiring their authority gates; that final check closes the
pre-gate recovery TOCTOU without moving validation or reclaimable staged-file
construction under the gate. Optimize's separate final `list_dir` runs under the
main branch gate because even table-disjoint main intents share graph-head authority.
Pinned by the four
`tests/failpoints.rs::*_after_finalize_publisher_failure_heals_without_reopen`
tests (load, mutation, schema apply, branch merge), plus
`recovery_rolls_forward_ensure_indices_on_feature_branch`, which retains the
next-read-write-open roll-forward boundary and then completes a second
roll-forward-eligible `EffectsConfirmed` predecessor under its unchanged token
on the same handle before new planning. The authority-clean
`ensure_indices_complete_armed_effects_roll_back` keeps the complete-effect
rollback rule isolated;
`ensure_indices_entry_barrier_refuses_partial_armed_before_staging` separately
proves that an `Armed` predecessor needing compensation is refused before the
remaining table's post-stage failpoint can fire. The maintenance
entries need the heal for more than liveness: without it, a schema
apply re-plans rewrites from the manifest pin and orphans the drifted
Phase-B commit (dropping its rows), and a branch merge publishes the
drift as an unattributed side effect — both while the stale sidecar
lingers to misclassify later.
Sidecars that would require a `Dataset::restore` (mixed / unexpected
state) are deferred to the next `OpenMode::ReadWrite` open. Full open-time
recovery uses the same root-scoped ordered gates and post-wait optional body reread,
so it cannot Restore/delete under a live writer owned by another handle in the
same process. Restore remains unsafe across processes because Lance's
`check_restore_txn` accepts
the restore against in-flight Append/Update/Delete commits and
silently orphans them (pinned by
`src/table_store/staged_tests.rs::lance_restore_loses_to_concurrent_append_via_orphaning`).
When such a deferred sidecar blocks a write, the commit-time drift
guard says so explicitly ("a pending recovery sidecar requires
rollback — reopen the graph read-write") instead of pointing at
`omnigraph repair`, which refuses while a sidecar is pending.
`cleanup` refuses pending sidecars at entry as well, before orphan reconciliation
or version GC: v3/v4 ownership and compensation recovery may need the retained
Lance transaction/version history, so garbage collection cannot outrun the
recovery barrier.
Continuous in-process recovery for the rollback path is the goal of a
future background reconciler. EnsureIndices' entry barrier remains
roll-forward-only: it never performs a `Dataset::restore` under a live handle.
It runs before schema-idle checking and `open_write_txn`, so unresolved
rollback state cannot be mistaken for a fresh base or trigger another expensive
index build. Its final under-gate sidecar relist remains separately required to
close the entry-barrier-to-effect TOCTOU; uncovered non-sidecar drift still
fails loudly under the existing strict preconditions.

For enrolled mutation/load, branch merge, SchemaApply, and EnsureIndices, the publisher rechecks the
attempt's exact native branch identity and `graph_head` as well as table
expectations. A
concurrent graph commit anywhere on the target branch therefore invalidates the
prepared authority instead of silently reparenting it. Before effects, an
insert-only mutation or Append/Merge load may fully reprepare with a bounded
retry after unrelated authority movement; load Append remains `StrictInsert`
through that retry. Strict Update/Delete/Overwrite and branch merge return
`ReadSetChanged`. For Mutation/Load, a detected effect-free strict key conflict
returns `KeyConflict` without semantic retry; after any effect, any later error
returns `RecoveryRequired` and leaves the fixed v9 intent durable. BranchMerge
is deliberately stricter: after its `protocol_v4` sidecar is armed, even a
first-chunk conflict before a merge-owned effect returns `RecoveryRequired`.
SchemaApply does not transparently reprepare after arming: a lost
authority token is resolved by exact recovery, preserving a disjoint winner or
failing closed on a buried same-table effect. EnsureIndices follows the same rule
without transparent post-arm reprepare. Optimize instead uses the bounded
maintenance payload inside the same v9 identity envelope; its exact-provenance upgrade is deferred
behind the upstream transaction-API and distributed-fencing triggers.

**Sidecar I/O failure semantics** (all sidecar I/O goes through the
backend-generic `StorageAdapter`; the contracts below are pinned by the
storage-fault failpoints `recovery.sidecar_{write,delete,list}` /
`recovery.record_audit` and their tests in `tests/failpoints.rs` and
`tests/recovery.rs`):

- **Phase A put fails** (S3 PutObject / fs write): the writer aborts
  before its first HEAD-advancing commit — no sidecar, no drift,
  nothing to recover; a transient fault never wedges later writes.
- **Phase D delete fails** (S3 DeleteObject): swallowed with a warning —
  the write already published, so failing the caller would report an
  error for a durable write. The stale sidecar is consumed by the next
  write's entry heal (or the next open) via the stale-sidecar
  audit-recovery path, recorded as `RolledForward`.
- **`__recovery/` list fails** (S3 ListObjectsV2): loud at every
  consumer — the write-entry heal fails the write, the open-time sweep
  fails the open. Silently skipping recovery would be consumer
  tolerance of drift.
- **A listed sidecar disappears before its body is read**: benign
  concurrent completion. A writer may publish and delete its sidecar
  between discovery's LIST and GET. The single-GET optional read maps
  only the backend's typed `NotFound` to absence and skips that URI;
  malformed/future sidecars and every other read failure remain loud.
- **Corrupt / unparseable sidecar**: refused loudly by heal and read-write
  open; the file stays on disk for operator inspection. Read-only open keeps
  its historical tolerance when no schema staging exists, but returns
  `RecoveryRequired` when any schema-staging artifact is present because the
  malformed intent may be the only proof of a committed-but-unpromoted
  SchemaApply.
- **Pre-v9 identity-less sidecar**: refused loudly. The v7 storage strand is
  rebuilt rather than upgraded in place, and recovery never infers table
  ownership from a mutable alias or path.
- **Audit append fails after a roll-forward publish**: that recovery
  attempt errors and keeps the sidecar; re-entry sees the
  already-published manifest, records exactly one `RolledForward`
  audit row, and deletes the sidecar (the retry tolerance documented
  on `record_audit`).

Backend notes (the adapter is one implementation over `object_store`
for every backend): local writes stage through `name#<digits>` temp
files that the backend filters from listings and refuses to address —
crash residue of that shape is invisible to the sweep, harmless, and
reclaimed by `delete_prefix`/manual cleanup. Ordinary `read_text`
keeps backend-wrapped errors. `read_text_if_exists` performs one GET
and returns `None` only for the underlying object-store `NotFound`;
recovery discovery uses it because a listed sidecar may be finalized
concurrently. It does not use an `exists()` → `read_text` check, which
would introduce another race. Other storage failures remain errors.
`exists()` itself is object-store semantics everywhere: only objects
(or non-empty prefixes) exist, and a permission failure is a loud
error, not a silent `false`.

## Conflict shape

For mutation/load, a changed authority detected before effects is
`ManifestConflictDetails::ReadSetChanged { member, expected, actual }`.
An operation excluded by durable stream authority receives
`ManifestConflictDetails::StreamLifecycleConflict { stable_table_id,
table_incarnation_id, table_key, lifecycle, operation }`; retrying the same
effect without a later lifecycle transition is not an authority retry.
Retryable insert/upsert/Append attempts may handle unrelated pre-effect
authority movement internally by fully repreparing. `OmniError::KeyConflict`
with `{ table_key, key }` is the terminal strict-insert result for a pre-existing
ID or an effect-free concurrent same-key winner confirmed by a fresh exact-ID
probe; HTTP returns **409 Conflict** with structured `key_conflict` details.
The wire field remains optional for compatibility, while v7 production
Mutation/Load emits the exact matched ID.
`RetryableCommitConflict` is the typed internal substrate signal used for the
upsert and strict-no-match reprepare paths; no logic parses Lance error strings.
A changed authority
discovered after a physical effect, an unclassifiable key conflict, or any
unresolved overlapping intent found at the synchronous recovery barrier is
`OmniError::RecoveryRequired { operation_id, … }`, mapped to **503
Service Unavailable** with structured `recovery_required`; retry only after the
sidecar has been resolved. Legacy, not-yet-enrolled writers may still surface
`ExpectedVersionMismatch` and `manifest_conflict`.

`OmniError::ResourceLimitExceeded { resource, limit, actual }` is a pre-arm
input-shaping error. It means the keyed Mutation/Load transaction exceeded
8,192 rows or 32 MiB (including a mutation update's bounded match set), a
BranchMerge materialized row/delete filter/retained delete plan or aggregate
retained validation delta exceeded 32 MiB, or the logical v4 data chain would
exceed 1,024 transactions. HTTP maps it to **413 Payload Too Large** with
structured `resource_limit` details. It never reports partial success.

## Commit actor history

`actor_id` lands in the graph commit lineage — the `graph_commit` rows in
`__manifest`, written in the publish CAS (RFC-013 Phase 7; previously
`_graph_commits.lance`). Ordinary commit/actor history is queried via
`omnigraph commit list`. Crash-recovery actions additionally live in the internal
`_graph_commit_recoveries.lance` table described above; that exact recovery log
does not yet have a public CLI query.

## Storage versioning (no in-place migration)

`db/manifest/migrations.rs` is the single place the on-disk `__manifest` shape is
reconciled with what the binary expects. Storage is **strict-single-version** (the
strand model): this binary reads exactly ONE internal-schema version
(`MIN_SUPPORTED == CURRENT == 8`), so there is no in-place migration.

- **Graph creation** stamps `omnigraph:internal_schema_version` at CURRENT, so a
  fresh graph always opens.
- **`Omnigraph::open`** (both read-write and read-only) reads main's stamp before
  the coordinator reads any branch state and calls `refuse_if_stamp_unsupported`:
  a stamp *below* CURRENT is refused with a rebuild-via-export/import message; a
  stamp *above* CURRENT is refused with "upgrade omnigraph". The publisher
  re-checks the stamp on its write path against the branch it targets, with no
  object-store writes, so the check is safe under a read-only open.
- The stamp + `refuse_if_stamp_unsupported` floor is the only seam a future
  in-place migration would re-introduce (re-add a dispatcher and lower
  `MIN_SUPPORTED`). Until a concrete graph demands it, that machinery is
  deliberately absent — see [versioning.md](versioning.md) (the compatibility
  policy) and [the upgrade guide](../user/operations/upgrade.md) (the rebuild
  recipe).

V7 maps to the 0.11.x release line. It preserves v5 stable identity and v6
exact-`id` key fencing, and adds the RFC-026 Phase A lifecycle/enrollment
foundation described above. A genuine v6 root moves to v7 only through
export/init/load into a different root; v7 refuses v6 and a v6 binary refuses
v7.

V8 maps to the 0.12.x release line. It preserves the v7 foundation and activates
the private RFC-026 B1 stream-config-v2 row/fold core plus recovery-v11. A v7
root moves to v8 only through export/init/load into a different root; v8 refuses
v7 and a v7 binary refuses v8. This format capability does not expose a public
streaming API.

The stamp history (v1 PK-less, v2 unenforced-PK, v3 `__run__*` sweep, v4 lineage
in `__manifest` with the commit-graph tables retired, v5 stable table identity,
v6 exact-`id` PK metadata plus fenced keyed routing, v7 identity-keyed stream
lifecycle authority plus the recoverable empty-enrollment foundation, v8
stream-config v2 plus the private recovery-v11 row/fold core) is recorded on the
`INTERNAL_MANIFEST_SCHEMA_VERSION` doc-comment; only v8 is served. An earlier-stamped
graph is rebuilt via export/import, not migrated in place.

## Mid-query partial failure: closed by MR-794

The pre-MR-794 design had a known limitation: a multi-statement `.gq`
mutation where op-N inline-committed a Lance fragment and op-N+1 then
failed left the touched table at `Lance HEAD = manifest_version + 1`,
blocking the next mutation with `ExpectedVersionMismatch`.

MR-794 (step 1 + step 2+) closed this for inserts/updates **by
construction at the writer layer**: insert and update batches accumulate
in memory; no Lance HEAD advance happens during op execution; one
`stage_*` + `commit_staged` per touched table runs at end-of-query, and
only after every op succeeded. A failed op leaves Lance HEAD untouched
on the staged tables, so the next mutation proceeds normally with no
drift to reconcile.

The cancellation case (future drop mid-mutation) inherits the same
guarantee — the in-memory accumulator evaporates with the dropped task
and no Lance write was ever issued.

Delete-touching mutations now inherit the same guarantee (MR-A). Deletes
accumulate as predicates and stage via `stage_delete` at end-of-query, so a
delete cascade that fails mid-way advances no Lance HEAD — the same
"untouched on failure" property as inserts/updates. The old narrow inline
window (and the retry/`cleanup` workaround it required) is gone. The
parse-time D₂ rule keeps inserts/updates from coexisting with deletes in one
query as a deliberate boundary (see the D₂ section above), so a mutation is
always purely constructive or purely destructive.

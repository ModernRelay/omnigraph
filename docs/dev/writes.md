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
  name. The branch-name guard was removed in MR-770, and any stale
  `__run__*` branch on an upgraded graph is swept off `__manifest` by the
  v2→v3 internal-schema migration on first read-write open. (The inert
  `_graph_runs.lance` bytes remain until a `delete_prefix` primitive lands.)
- Cancelled mutation futures leave **no graph-visible state** unless the
  manifest publish already completed. Before that point they can leave
  reclaimable uncommitted Lance files, or sidecar-covered committed table
  effects that the next quiesced recovery rolls forward/compensates. A
  first-touch named-branch write can also leave a target table ref, but it is
  never created without ownership: the schema-v3 sidecar is durable first and
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
   ref exists;
3. acquire the schema gate, branch gate, then sorted table queues; re-check for
   a relevant sidecar armed since step 1, then revalidate the token. Any
   unresolved relevant intent returns typed `RecoveryRequired` before effects;
4. on a pre-effect mismatch, discard the complete attempt. Append/Insert/Merge
   reprepare with a bounded retry; strict Update/Delete/Overwrite return typed
   `ReadSetChanged`;
5. arm a schema-v3 recovery sidecar. For each deferred first-touch table, create
   its target ref, stage branch-local files on that ref, and bind the staged
   transaction to the pre-minted UUID. Then commit every planned transaction
   with zero transparent conflict retries, confirm exact transaction UUIDs and
   table updates, and publish the pre-minted lineage intent under the same token.

The publisher checks the exact head and native branch identity on every CAS
attempt. It never reparents a validation-sensitive intent after contention. A
mismatch after any physical effect returns `RecoveryRequired` and leaves the
sidecar intact; it is not an ordinary retry loop.

This adapter preserves the documented single-writer-process support boundary.
The native branch identifier detects delete/recreate ABA but is not a Lance
conditional-ref fence, and destructive recovery remains unsafe beside a live
foreign process.

### Branch-merge authority fence (adapter bridge)

Branch merge still uses its writer-specific multi-commit table effects and
confirmation sidecar; it has not yet been converted to the RFC-022 exact-effect
adapter. It does, however, join the closed control boundary needed by this first
slice: after the strict recovery barrier it acquires the root-shared schema gate
and the sorted source/target branch gates, performs the final sidecar check,
loads one operation-local catalog from the accepted contract, captures both graph
heads plus the base/source/target snapshots, and holds those gates through table
effects and manifest publication. Planning stays outside table queues. Before
Phase A, merge acquires the conservative all-catalog table envelope for both
source and target, re-lists sidecars, and compares fresh source/target manifest
versions with the captured snapshots. A stale warm handle catalog or coordinator
snapshot is never accepted as that revalidation.

The source snapshot is a captured merge input, not authority that the target
manifest CAS can arbitrate. The current process-local source gate is a stronger
same-process fence around that capture, including delete/recreate ABA, but the
semantic contract is still "merge the captured source commit." A later source
advance does not invalidate an otherwise prepared target publish. Claiming
"latest source at target publish" would instead require a cross-process source
fence held through the target CAS.

That fence prevents a same-process target delete/recreate from reusing the branch
name underneath a merge plan. The race test deliberately recreates a target with
the same name and numeric Lance version but a different `BranchIdentifier`, so
version-only checking cannot accidentally satisfy it. This is a process-local
bridge, not a cross-process conditional-ref primitive and not a substitute for
the later full branch-merge read-set/reprepare adapter. `sync_branch` joins the
same root schema gate before replacing a handle's coordinator, so it cannot
overwrite merge's temporary target coordinator or change a native control's
active-branch authority mid-operation.

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

There is deliberately no branch-control sidecar: within the supported
single-writer-process topology, an absent ref makes a same-name tree unreachable
garbage; the path-prefix-disjoint namespace is what makes Lance's recursive
force cleanup exact. Same-name create is therefore the targeted reconciler.
First-touch data-table
forks remain sidecar-owned because they are physical effects of a graph-visible
mutation/load. Lance does not expose conditional ref create/delete, so this
classifier is not advertised as a cross-process branch-control fence.

Legacy prefix-overlap recovery is the one absent-ref case that does not prove an
entire nested tree unreachable. If a Full sweep finds an ancestor clone-only
first-touch target with a live path-child, it keeps the sidecar and permits open
to complete so the child can be deleted leaf-first. A later Full sweep, after the
child is gone, reclaims the ancestor tree and retires the intent.

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

### MR-793 status (storage trait two-phase invariant) — partial

MR-793 hoists the staged-write pattern into a `TableStorage` trait
surface with sealed-trait enforcement and opaque `SnapshotHandle` /
`StagedHandle` types — see `crates/omnigraph/src/storage_layer.rs`.
The trait is the canonical surface for new engine code; existing call
sites still use the inherent `TableStore` methods (mechanical migration
deferred to a follow-up cycle — tracked).

Three writers have been migrated onto staged primitives:

* **`ensure_indices`** (`db/omnigraph/table_ops.rs::build_indices_on_dataset_for_catalog`)
  — scalar indices (BTree, Inverted) use `stage_create_*_index` +
  `commit_staged`. Which index a `@index`/`@key` property gets is dispatched by
  type via `node_prop_index_kind` (enum + orderable scalar → BTree, free-text
  String → Inverted/FTS, Vector → vector). Vector indices stay inline (residual
  — Lance `build_index_metadata_from_segments` is `pub(crate)` in 6.0.1;
  companion ticket to lance-format/lance#6658 needed). This build is
  existence-gated (it creates a *missing* index over current fragments); folding
  fragments appended afterward into an *existing* index is `optimize`'s
  `optimize_indices` pass — an inline-commit residual, not a staged write (Lance
  exposes no uncommitted index-optimize), covered by the optimize recovery
  sidecar (see [maintenance.md](../user/operations/maintenance.md)).
* **`branch_merge::publish_rewritten_merge_table`**
  (`exec/merge.rs`) — merge_insert now uses `stage_merge_insert` +
  `commit_staged`; its deletes use `stage_delete` + `commit_staged` (MR-A).
* **`schema_apply` rewritten_tables** (`db/omnigraph/schema_apply.rs`)
  — rewrites use `stage_overwrite` + `commit_staged`, including empty-table
  rewrites via a zero-fragment Lance `Operation::Overwrite`.

A defense-in-depth integration test (`tests/forbidden_apis.rs`) walks
engine source and fails if non-allow-listed code calls Lance's
inline-commit APIs directly. The trait surface itself is the primary
enforcement (sealed + only-callable-via-trait once call sites land);
the grep test catches type-system bypass attempts.

The "finalize → publisher residual" described below applies equally to
the migrated writers — Lance has no multi-dataset atomic commit
primitive, so the per-table commit_staged → manifest publish gap is
the same drift class. Closing it requires either upstream Lance
multi-dataset commit OR the omnigraph-side recovery-on-open reconciler
described in `.context/mr-793-design.md` §15 (deferred to MR-795).

### Inline-commit residuals live on `InlineCommitResidual`, not `db.storage()` (MR-793 acceptance §1, by construction)

MR-793's acceptance criterion §1 ("`TableStore` (or successor) public API has no method that performs a manifest commit as a side effect of writing") holds **by construction** after MR-854. `db.storage()` (`&dyn TableStorage`) exposes only staged primitives + reads; the inline-commit writes Lance cannot yet stage live on a separate `InlineCommitResidual` trait reached via `Omnigraph::storage_inline_residual()`. A new engine writer cannot couple a write with a Lance HEAD advance through the default surface — it would have to name the residual accessor explicitly. The dead legacy methods (trait `append_batch` / `merge_insert_batches`, inherent `merge_insert_batch{,es}`, `create_{btree,inverted}_index`) were removed; appends/merges and scalar index builds all use the `stage_*` primitives.

One method remains on `InlineCommitResidual`, named honestly at its call site:

| Residual method | Inline-commit reason | Closes when |
|---|---|---|
| `create_vector_index` | Vector indices take Lance's "segment commit path"; `build_index_metadata_from_segments` is `pub(crate)` (Lance [#6666](https://github.com/lance-format/lance/issues/6666) still open) | Lance #6666 lands and `stage_create_vector_index` joins the staged surface |

`delete_where` used to be the second residual. Lance 7.0's
`DeleteBuilder::execute_uncommitted` ([#6658](https://github.com/lance-format/lance/issues/6658))
made delete a staged write, so MR-A migrated it to `TableStorage::stage_delete`
and removed `InlineCommitResidual::delete_where`. The parse-time D₂ rule is
retained as a deliberate boundary (constructive XOR destructive per query) — see
the D₂ section above.

The `tests/forbidden_apis.rs` guard still catches direct `lance::*` inline-commit misuse outside the storage layer; the trait split makes the staged-only default a type-system guarantee on top of it.

### `LoadMode::Overwrite` uses staged Lance `Overwrite`

The bulk loader's Append, Merge, and Overwrite modes all use the
staged-write path described above. `LoadMode::Overwrite` accumulates
replacement batches in memory, validates node/edge constraints, referential
integrity, and edge cardinality before any Lance HEAD movement, stages
each touched table with Lance `Operation::Overwrite`, then runs
`commit_staged` under the normal `SidecarKind::Load` recovery sidecar
before publishing `__manifest`. `OMNIGRAPH_LOAD_CONCURRENCY` applies to the
fragment-writing stage only; the commit and manifest publish run while holding
the root-shared schema → branch → sorted-table gates. Empty-table overwrite is
represented as a valid zero-fragment Lance `Overwrite` transaction, not as
truncate-then-append.

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

**Recovery protocol** (lifecycle of every staged-write writer —
`MutationStaging::commit_all`, `schema_apply::apply_schema_with_lock`,
`branch_merge_on_current_target`, `ensure_indices_for_branch`,
`optimize_all_tables`):

1. **Phase A**: writer writes a sidecar JSON to
   `__recovery/{ulid}.json` BEFORE its first independently durable physical
   effect (including a first-touch Lance branch ref) or HEAD-advancing commit
   (`commit_staged`, or `compact_files` for `optimize_all_tables`,
   which advances the Lance HEAD via a reserve-fragments + rewrite
   commit rather than a staged write). The
   sidecar names every `(table_key, table_path, expected_version,
   post_commit_pin)` it intends to commit + the writer kind +
   actor_id.
   For a first-touch named-branch Mutation/Load table, Phase A is followed by
   target-ref creation and branch-local `stage_*`; the sidecar already carries
   its pre-minted transaction identity.
2. **Phase B**: writer's per-table `commit_staged` loop runs.
   - **Phase-B confirmation:** a `BranchMerge` writer
     advances each table's HEAD by *several* commits (append → upsert →
     delete), so a bare "HEAD moved" is ambiguous — it could be a complete
     publish or one crashed mid-sequence. After the whole per-table loop
     finishes, the writer re-writes the sidecar stamping each pin's
     `confirmed_version` with the exact achieved version, then proceeds to
     Phase C. Schema-v3 Mutation/Load sidecars also confirm: each table must
     match the staged Lance transaction's `(read_version, uuid)`, and the
     sidecar records the exact `SubTableUpdate` plus original lineage intent.
     This is the commit point of the recovery WAL: a crash *after* confirmation
     rolls forward only when the captured branch token still matches; a crash
     *during* Phase B (sidecar still unconfirmed) rolls back. Remaining legacy
     writers don't confirm — their drift is derived state (index coverage,
     compaction) that a partial roll-forward never corrupts.
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

A failure between Phase A and Phase D leaves the sidecar on disk. The
next `Omnigraph::open` (gated on `OpenMode::ReadWrite`) runs the
recovery sweep in `crates/omnigraph/src/db/manifest/recovery.rs`:

- For each sidecar in `__recovery/`, compare every named table's
  Lance HEAD to the manifest pin. Classify per the all-or-nothing
  decision tree (RolledPastExpected / NoMovement / UnexpectedAtP1 /
  UnexpectedMultistep / IncompletePhaseB / InvariantViolation). For a
  `BranchMerge` sidecar, a moved HEAD with no `confirmed_version` classifies
  as `IncompletePhaseB` (a partial multi-commit publish) and forces roll-back;
  with a `confirmed_version`, roll-forward targets exactly that version.
  Schema-v3 Mutation/Load additionally requires `EffectsConfirmed`, the exact
  Lance transaction identity at the confirmed version, the original immutable
  manifest delta, and a matching captured authority token. A changed token is
  rollback-only; an unknown/foreign effect is refused rather than adopted.
  An Armed first-touch intent with no owned transaction is deferred by live
  roll-forward-only healing because another handle may still own it. Quiesced
  full recovery tolerates an absent target ref (either crash before clone or a
  clone-only tree with no `BranchContents`) and force-reclaims that absent-ref
  target idempotently. If an authoritative ref exists, recovery removes it only
  when it is exactly unchanged and no other pending sidecar claims it. With
  competing claims, the current no-effect sidecar discards itself without
  touching the ref; the final survivor owns cleanup/recovery.
  During partial rollback, no-effect refs are removed before the rollback
  outcome is published so a retry cannot strand them.
- If any table is `InvariantViolation` (Lance HEAD < manifest pinned —
  should be impossible), **abort** with a loud error and leave the
  sidecar on disk for operator review.
- Otherwise, if every table is `RolledPastExpected`, **roll forward**:
  a single `ManifestBatchPublisher::publish` call extends every pin
  atomically. `SchemaApply` sidecars are eligible only when schema-state
  recovery promoted the matching staging files in the same recovery pass;
  otherwise full open-time recovery rolls them back and refresh-time
  recovery leaves them for the next read-write open.
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
  exact per-table outcomes. A v3 roll-forward publishes the interrupted
  writer's fixed lineage intent, including its original actor; rollback and
  legacy recovery commits use `actor_id = "omnigraph:recovery"`. Ordinary
  commit history is therefore not a complete recovery enumeration, and the
  CLI currently has no public query for the recovery-audit table.
- Sidecar deleted as the final step.

Triggers for the residual: transient Lance write errors during finalize
(object-store retry budget exhaustion, disk full); persistent publisher
contention exceeding `PUBLISHER_RETRY_BUDGET = 5` retries.

**Long-running servers**: the write entry points (`load_as`,
`mutate_as`, `apply_schema_as`, `branch_merge_as`) and
`Omnigraph::refresh` run roll-forward-only recovery in-process
(`recovery::heal_pending_sidecars_roll_forward`) — the common
Phase B → Phase C residual closes on the next write, without a
restart and without an explicit refresh. The heal lists `__recovery/`
(one `list_dir`; empty in the steady state) and, per sidecar, acquires
schema → branch → sorted-table gates that overlap the writer's guarded
sidecar lifetime. RFC-022 mutation/load writers hold the complete order. Branch
merge now holds schema plus source/target branch authority for its whole attempt
and then the all-catalog source/target table envelope; other legacy adapters
serialize through their existing table or schema gate until their own adapter
slices land. The
manager is shared by every
`Omnigraph` handle for one canonical local root identity (relative, absolute,
and symlink aliases converge; object-store/custom schemes stay opaque), so this
also serializes a refresh or separately-opened handle against a live writer instead of rolling its
in-flight sidecar forward from under it (a sidecar whose queues can be
acquired belongs to a writer that finished or died; an existence
re-check after the wait skips the finished case). Lock order is
schema → branch → sorted tables → coordinator, matching the writer effect path.
Enrolled mutation/load attempts and branch merge perform one additional
`list_dir` after acquiring their authority gates; that final check closes the
pre-gate recovery TOCTOU without moving mutation/load validation or staged-file
construction under the gate.
Pinned by the four
`tests/failpoints.rs::*_after_finalize_publisher_failure_heals_without_reopen`
tests (load, mutation, schema apply, branch merge). The maintenance
entries need the heal for more than liveness: without it, a schema
apply re-plans rewrites from the manifest pin and orphans the drifted
Phase-B commit (dropping its rows), and a branch merge publishes the
drift as an unattributed side effect — both while the stale sidecar
lingers to misclassify later.
Sidecars that would require a `Dataset::restore` (mixed / unexpected
state) are deferred to the next `OpenMode::ReadWrite` open. Full open-time
recovery uses the same root-scoped ordered gates and post-wait existence check,
so it cannot Restore/delete under a live writer owned by another handle in the
same process. Restore remains unsafe across processes because Lance's
`check_restore_txn` accepts
the restore against in-flight Append/Update/Delete commits and
silently orphans them (pinned by
`tests/staged_writes.rs::lance_restore_loses_to_concurrent_append_via_orphaning`).
When such a deferred sidecar blocks a write, the commit-time drift
guard says so explicitly ("a pending recovery sidecar requires
rollback — reopen the graph read-write") instead of pointing at
`omnigraph repair`, which refuses while a sidecar is pending.
`cleanup` refuses pending sidecars at entry as well, before orphan reconciliation
or version GC: v3 ownership and compensation recovery may need the retained
Lance transaction/version history, so garbage collection cannot outrun the
recovery barrier.
Continuous in-process recovery for the rollback path is the goal of a
future background reconciler. `ensure_indices` does not heal at entry itself;
it is an explicit maintenance/reconciliation call, separate from mutation,
load, and schema apply, and its strict preconditions fail loudly on drift.

For enrolled mutation/load, the publisher rechecks the attempt's exact native
branch identity and `graph_head` as well as the touched-table versions. A
concurrent graph commit anywhere on the target branch therefore invalidates the
prepared authority instead of silently reparenting it. Before effects, an
insert-only mutation or Append/Merge load fully reprepares with a bounded retry; strict
Update/Delete/Overwrite returns `ReadSetChanged`; after any effect, any later
error returns `RecoveryRequired` and leaves the fixed v3 intent durable. Legacy
writers still arbitrate only their explicit touched-table expectations until
their adapters are enrolled.

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
- **Corrupt / unparseable sidecar**: refused loudly by heal and open
  alike; the file stays on disk for operator inspection (read-only
  opens still work — the sweep is skipped there).
- **Audit append fails after a roll-forward publish**: that recovery
  attempt errors and keeps the sidecar; re-entry sees the
  already-published manifest, records exactly one `RolledForward`
  audit row, and deletes the sidecar (the retry tolerance documented
  on `record_audit`).

Backend notes (the adapter is one implementation over `object_store`
for every backend): local writes stage through `name#<digits>` temp
files that the backend filters from listings and refuses to address —
crash residue of that shape is invisible to the sweep, harmless, and
reclaimed by `delete_prefix`/manual cleanup. Storage errors are
backend-wrapped text without a typed NotFound discriminant — callers
that need missing-vs-error (the cluster store) probe `exists()` first.
`exists()` itself is object-store semantics everywhere: only objects
(or non-empty prefixes) exist, and a permission failure is a loud
error, not a silent `false`.

## Conflict shape

For mutation/load, a changed authority detected before effects is
`ManifestConflictDetails::ReadSetChanged { member, expected, actual }`.
Retryable Insert/Merge/Append attempts handle this internally by fully
repreparing; strict writes surface **409 Conflict** with structured
`read_set_conflict` details. A changed authority discovered after a physical
effect, or any unresolved overlapping intent found at the synchronous recovery
barrier, is `OmniError::RecoveryRequired { operation_id, … }`, mapped to **503
Service Unavailable** with structured `recovery_required`; retry only after the
sidecar has been resolved. Legacy, not-yet-enrolled writers may still surface
`ExpectedVersionMismatch` and `manifest_conflict`.

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
(`MIN_SUPPORTED == CURRENT == 4`), so there is no in-place migration.

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

The stamp history (v1 PK-less, v2 unenforced-PK, v3 `__run__*` sweep, v4 lineage
in `__manifest` with the commit-graph tables retired) is recorded on the
`INTERNAL_MANIFEST_SCHEMA_VERSION` doc-comment; only v4 is served. An earlier-stamped
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

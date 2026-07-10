# Direct-Publish Write Path

> History: the Run state machine and `__run__<id>` staging branches were
> removed in MR-771 (shipped v0.4.0). Writes now go directly to the target
> table; this document specifies that direct-publish path.

`mutate_as` and `load` write **directly to the target table**
and call `ManifestBatchPublisher::publish` once at the end with
`expected_table_versions` (the per-table manifest versions captured before
the first write). Cross-table OCC is enforced inside the publisher; the
publisher's row-level CAS on `__manifest` is the single fence.

## What this means in practice

- No `RunRecord`, no `_graph_runs.lance`, no `_graph_run_actors.lance`.
- No `omnigraph run *` CLI subcommands and no `/runs/*` HTTP endpoints.
- No `__run__<id>` staging branches; `__run__*` is no longer a reserved
  name. The branch-name guard was removed in MR-770, and any stale
  `__run__*` branch on an upgraded graph is swept off `__manifest` by the
  v2→v3 internal-schema migration on first read-write open. (The inert
  `_graph_runs.lance` bytes remain until a `delete_prefix` primitive lands.)
- Cancelled mutation futures leave **no graph-visible state** — the manifest
  is never advanced. They can leave two kinds of unreferenced residue, both
  self-healing: orphaned Lance fragments (reclaimed by `omnigraph cleanup`),
  and — on the *first* write to a table on a branch, which forks it before the
  publish — a manifest-unreferenced branch ref. The next write to that table
  reclaims the stale fork and re-forks (`reclaim_orphaned_fork_and_refork`),
  and `cleanup`'s per-table reconciler is the guaranteed backstop; see the
  fork-reclaim note in [invariants.md](invariants.md).

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
- At end-of-query, `MutationStaging::finalize` issues exactly one
  `stage_*` + `commit_staged` per touched table (concatenating
  accumulated batches; merge-mode dedupes by `id`, last-write-wins),
  and the publisher publishes the manifest atomically across all
  touched sub-tables. Cross-table conflicts surface as
  `ManifestConflictDetails::ExpectedVersionMismatch`.
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

- **Field-level updates (partial-schema staging).** A node table whose ONLY op
  in the query is a single `update` stages a PARTIAL merge source — exactly
  (id + assigned) columns — as a **matched-only** merge
  (`WhenNotMatched::DoNothing`). The accumulated (pending) batch additionally
  carries the constraint-completion columns (every member of a `@unique` group
  intersecting the assigned set) so the end-of-query evaluator can validate the
  whole tuple — but those are **validation inputs only** and are projected out
  before staging: Lance counts every source column as modified, so staging an
  unassigned group member would patch it and prune its index for no semantic
  reason. Lance's partial-schema path patches the
  provided columns in place on the same fragment (`update_mode:
  RewriteColumns`), so unassigned columns are never read or rewritten and
  indexes over them keep fragment coverage (Lance prunes only
  `fields_modified`). Every other shape (insert+update on one table, multiple
  updates, etc.) falls back to whole-row staging: partial and full batches
  cannot share one uniform-schema merge source (a present column's null cell
  means "set NULL", so widening would null-overwrite), a later same-table op's
  read-your-writes scan needs full rows, and one table commits at most one
  version per query. The unique evaluator skips a `@unique` group with no
  column present in a batch (untouched by the write) and stays loud on a
  partially-present group. Known residual (pinned by
  `lance_surface_guards::partial_schema_merge_patches_in_place_and_prunes_only_modified_fields`
  and the `scalar_indexes` coverage cell): the join key rides the source, so
  the **id BTREE** alone still loses patched fragments (parity with the
  whole-row path) until upstream excludes ON columns from column patches.

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
fragment-writing stage only; the commit and manifest publish still run
under the per-table write queues. Empty-table overwrite is represented as
a valid zero-fragment Lance `Overwrite` transaction, not as
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

`MutationStaging::finalize` runs `stage_*` + `commit_staged` per touched
table sequentially, then the publisher commits the manifest. Lance has
no multi-dataset atomic commit, so the per-table `commit_staged` calls
are independent operations: if commit_staged on table N+1 fails *after*
commit_staged on tables 1..N succeeded, or if the publisher's CAS
pre-check rejects *after* every commit_staged succeeded, tables 1..N
are left at `Lance HEAD = manifest_pinned + 1`.

**Recovery protocol** (lifecycle of every staged-write writer —
`MutationStaging::finalize`, `schema_apply::apply_schema_with_lock`,
`branch_merge_on_current_target`, `ensure_indices_for_branch`,
`optimize_all_tables`):

1. **Phase A**: writer writes a sidecar JSON to
   `__recovery/{ulid}.json` BEFORE its first HEAD-advancing commit
   (`commit_staged`, or `compact_files` for `optimize_all_tables`,
   which advances the Lance HEAD via a reserve-fragments + rewrite
   commit rather than a staged write). The
   sidecar names every `(table_key, table_path, expected_version,
   post_commit_pin)` it intends to commit + the writer kind +
   actor_id.
2. **Phase B**: writer's per-table `commit_staged` loop runs.
   - **Phase-B confirmation (`BranchMerge` only)**: a `BranchMerge` writer
     advances each table's HEAD by *several* commits (append → upsert →
     delete), so a bare "HEAD moved" is ambiguous — it could be a complete
     publish or one crashed mid-sequence. After the whole per-table loop
     finishes, the writer re-writes the sidecar stamping each pin's
     `confirmed_version` with the exact achieved version, then proceeds to
     Phase C. This is the commit point of the recovery WAL: a crash *after*
     confirmation rolls forward to those versions; a crash *during* Phase B
     (sidecar still unconfirmed) rolls back. Other writers don't confirm —
     their drift is derived state (index coverage, compaction) that a partial
     roll-forward never corrupts.
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
- After a successful roll-forward or roll-back, an audit row is
  recorded — the graph commit lineage (the `graph_commit` rows in `__manifest`
  since RFC-013 Phase 7) carries a commit tagged
  `actor_id = "omnigraph:recovery"`, and a sibling
  `_graph_commit_recoveries.lance` row carries `recovery_kind`,
  `recovery_for_actor` (the original sidecar's actor), `operation_id`,
  per-table outcomes. Operators run `omnigraph commit list --filter
  actor=omnigraph:recovery` to find recoveries.
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
the same per-`(table_key, table_branch)` write queues every sidecar
writer holds from before `write_sidecar` until after `delete_sidecar` —
so it serializes against a live writer instead of rolling its
in-flight sidecar forward from under it (a sidecar whose queues can be
acquired belongs to a writer that finished or died; an existence
re-check after the wait skips the finished case). Lock order is
queues → coordinator, matching every writer's commit→publish path.
Pinned by the four
`tests/failpoints.rs::*_after_finalize_publisher_failure_heals_without_reopen`
tests (load, mutation, schema apply, branch merge). The maintenance
entries need the heal for more than liveness: without it, a schema
apply re-plans rewrites from the manifest pin and orphans the drifted
Phase-B commit (dropping its rows), and a branch merge publishes the
drift as an unattributed side effect — both while the stale sidecar
lingers to misclassify later.
Sidecars that would require a `Dataset::restore` (mixed / unexpected
state) are deferred to the next `OpenMode::ReadWrite` open: restore is
unsafe under concurrency because Lance's `check_restore_txn` accepts
the restore against in-flight Append/Update/Delete commits and
silently orphans them (pinned by
`tests/staged_writes.rs::lance_restore_loses_to_concurrent_append_via_orphaning`).
When such a deferred sidecar blocks a write, the commit-time drift
guard says so explicitly ("a pending recovery sidecar requires
rollback — reopen the graph read-write") instead of pointing at
`omnigraph repair`, which refuses while a sidecar is pending.
Continuous in-process recovery for the rollback path is the goal of a
future background reconciler. `ensure_indices` does not heal at entry
itself — it runs inside the load / schema-apply flows after their
entry heal, and its strict preconditions still fail loudly on drift
when invoked directly.

The publisher-CAS contract is unchanged: a *concurrent writer* that
advances any of our touched tables between snapshot capture and
publisher commit produces exactly one winner. The residual above is
about *our* abandoned commits in the failure path, not about
concurrency races.

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

Concurrent writers to the same `(table, branch)` produce exactly one
success and one failure. The losing writer's error is
`OmniError::Manifest` with kind `Conflict` and details
`ManifestConflictDetails::ExpectedVersionMismatch { table_key, expected,
actual }`. The HTTP server maps this to **409 Conflict** with body
`{"error": "...", "code": "conflict", "manifest_conflict": { "table_key":
"...", "expected": N, "actual": M }}` — see [docs/user/server.md](../user/operations/server.md).

## Audit

`actor_id` lands in the graph commit lineage — the `graph_commit` rows in
`__manifest`, written in the publish CAS (RFC-013 Phase 7; previously
`_graph_commits.lance`). Audit history is queried via `omnigraph commit list`.

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

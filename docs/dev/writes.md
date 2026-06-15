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
- Cancelled mutation futures leave **no graph-level state** — only orphaned
  Lance fragments, which the existing `omnigraph cleanup` pipe reclaims.

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
- **Deletes still inline-commit.** Lance's `Dataset::delete` is not
  exposed as a two-phase op in 6.0.1; deletes go through `delete_where`
  immediately and record their post-write state in
  `MutationStaging.inline_committed`. The parse-time D₂ rule (below)
  prevents inserts/updates from coexisting with deletes in one query,
  so the inline path is safe for delete-only mutations.

This upholds the manifest-atomic mutation and read-your-writes invariants
tracked in [docs/dev/invariants.md](invariants.md).

### D₂ — parse-time mixed-mode rejection

A single mutation query is either insert/update-only or delete-only.
Mixed → rejected at parse time with a clear error directing the user to
split the query. Reason: mixing creates ordering hazards
(insert→delete on the same row would silently no-op because the staged
insert isn't visible to delete; cascading deletes of just-inserted
edges break referential integrity). Until Lance exposes a two-phase
delete API, the parse-time rejection keeps both paths atomic and
correct. Tracked: MR-793, plus a Lance-upstream ticket.

### MR-793 status (storage trait two-phase invariant) — partial

MR-793 hoists the staged-write pattern into a `TableStorage` trait
surface with sealed-trait enforcement and opaque `SnapshotHandle` /
`StagedHandle` types — see `crates/omnigraph/src/storage_layer.rs`.
The trait is the canonical surface for new engine code; existing call
sites still use the inherent `TableStore` methods (mechanical migration
deferred to a follow-up cycle — tracked).

### Schema apply concurrency: queue-before-snapshot, per-table CAS, no global lease

`apply_schema_with_lock` (`db/omnigraph/schema_apply.rs`) follows the same
queue-before-read order `optimize` uses, so a concurrent compaction no longer
false-fails a migration (bug 4 / iss-schema-apply-optimize-occ):

1. **Classify the plan snapshot-free.** `classify_plan_steps` derives the
   touched-table sets (added / renamed / rewritten / dropped, plus property
   renames and the edge-touch flag) purely from the plan via `schema_table_key`
   — no manifest snapshot.
2. **Acquire the touched-existing-table write queues *before* the snapshot.**
   The set is `rewritten ∪ rename-source ∪ dropped` — the existing tables the
   apply rewrites in place or tombstones. New tables (added types, rename
   targets) have no existing dataset to race against, so they aren't acquired.
   The schema-apply serial key (`__schema_apply__`) is added whenever a sidecar
   will be written. `acquire_many` sorts + dedupes, so lock order is identical
   for every caller (the serial key sorts before any `node:`/`edge:` key;
   optimize's single-key acquire is a subset) — no inversion.
3. **Snapshot under the held queues.** A `refresh_coordinator_only` re-reads
   HEAD first so the pinned versions are exactly current. While the queues are
   held, no in-process optimize/mutate can move a touched table, so the
   per-table preconditions (`ensure_snapshot_entry_head_matches` in the
   rewrite/rename loops) pass by construction.
4. **Publish with a per-table expected-version CAS.**
   `commit_changes_with_expected_with_actor` fences the publish with the
   snapshot versions of every touched-existing table. The held queues make this
   pass in-process; the CAS is the loud cross-process backstop (the
   acknowledged in-process-only recovery-serialization gap) — a foreign mover
   surfaces as a typed `ExpectedVersionMismatch` rather than a publish onto
   moved state.

The old graph-global **"write lease"** check — comparing the whole manifest
version against the pre-apply version right before the publish — is **gone**.
It false-failed an apply whenever a concurrent compaction of an *untouched*
table advanced the global manifest version, even though that commit is a
disjoint manifest row the publisher rebases onto cleanly. "Operations that
don't conflict semantically no longer conflict mechanically": a compaction of
an untouched table no longer fails the apply, and a compaction of a touched
table serializes behind it. Deterministic proof:
`schema_apply_succeeds_under_concurrent_write_to_untouched_table` and
`concurrent_optimize_on_touched_table_serializes_behind_schema_apply`
(`tests/failpoints.rs`, via the `schema_apply.after_queue_acquire_pre_snapshot`
and `schema_apply.post_snapshot_pre_commit` failpoints).

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
  `commit_staged`. Deletes stay inline (Lance #6658 residual).
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

Two methods remain on `InlineCommitResidual`, each named honestly at its call site:

| Residual method | Inline-commit reason | Closes when |
|---|---|---|
| `delete_where` | `DeleteBuilder::execute_uncommitted` is not in Lance v6.0.1 (closed upstream as [#6658](https://github.com/lance-format/lance/issues/6658) but first ships in `v7.0.0-beta.10`); see [docs/dev/lance.md](lance.md) | MR-A: Lance v7.x bump migrates `delete_where` to staged, retires the parse-time D₂ mutation rule, and extends recovery sidecar coverage |
| `create_vector_index` | Vector indices take Lance's "segment commit path"; `build_index_metadata_from_segments` is `pub(crate)` (Lance [#6666](https://github.com/lance-format/lance/issues/6666) still open) | Lance #6666 lands and `stage_create_vector_index` joins the staged surface |

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
  UnexpectedMultistep / InvariantViolation).
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
  recorded — `_graph_commits.lance` carries
  a commit tagged `actor_id = "omnigraph:recovery"`, and a sibling
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

`actor_id` lands in `_graph_commits.lance` via `record_graph_commit` (no
intermediate run record). Audit history is queried via `omnigraph commit
list`.

## Migration code

`db/manifest/migrations.rs` carries the v2→v3 internal-schema step (MR-770):
a one-time sweep that deletes legacy `__run__*` staging branches off
`__manifest`. It runs in `Omnigraph::open(ReadWrite)` (via
`manifest::migrate_on_open`, before the coordinator reads branch state) and
again on the publisher's write path; both are idempotent once the stamp is at
v3. Deleting the inert `_graph_runs.lance` / `_graph_run_actors.lance` dataset
*bytes* is still deferred — it needs a `StorageAdapter::delete_prefix`
primitive — but those bytes are invisible to graph-level state.

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

For delete-touching mutations the legacy inline-commit shape is
preserved (Lance has no public two-phase delete in 6.0.1) — the same
narrow window remains. The parse-time D₂ rule prevents inserts/updates
from coexisting with deletes in one query, so a pure-delete failure
cannot drift any staged-table state. If a delete-only multi-table
mutation fails mid-cascade, the same workaround as before applies
(retry; rely on `omnigraph cleanup` once a later successful commit
moves HEAD past the orphan version). Closing this requires Lance to
expose `DeleteJob::execute_uncommitted`; tracked in MR-793 and a
Lance-upstream ticket.

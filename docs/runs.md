# Runs — REMOVED (MR-771)

The Run state machine and `__run__<id>` staging branches were removed in
MR-771. `mutate_as` and `load` now write **directly to the target table**
and call `ManifestBatchPublisher::publish` once at the end with
`expected_table_versions` (the per-table manifest versions captured before
the first write). Cross-table OCC is enforced inside the publisher; the
publisher's row-level CAS on `__manifest` is the single fence.

## What this means in practice

- No `RunRecord`, no `_graph_runs.lance`, no `_graph_run_actors.lance`.
- No `omnigraph run *` CLI subcommands and no `/runs/*` HTTP endpoints.
- No `__run__<id>` staging branches. (Legacy on-disk artifacts from
  pre-MR-771 repos are inert; MR-770 sweeps them in production.)
- Cancelled mutation futures leave **no graph-level state** — only orphaned
  Lance fragments, which the existing `omnigraph cleanup` pipe reclaims.

## Read-your-writes within a multi-statement mutation

A `.gq` query with multiple ops (e.g. `insert Person … insert Knows …`)
must observe earlier ops' writes when validating later ops (referential
integrity, edge cardinality). After MR-794 step 2+ this is implemented
via an in-memory `MutationStaging` accumulator in
[`crates/omnigraph/src/exec/staging.rs`](../crates/omnigraph/src/exec/staging.rs),
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
  exposed as a two-phase op in 4.0.0; deletes go through `delete_where`
  immediately and record their post-write state in
  `MutationStaging.inline_committed`. The parse-time D₂ rule (below)
  prevents inserts/updates from coexisting with deletes in one query,
  so the inline path is safe for delete-only mutations.

This upholds [docs/invariants.md §VI.23](invariants.md) (atomicity per
query) and §VI.25 (read-your-writes within a multi-statement mutation,
upheld).

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

Three writers have been migrated onto staged primitives:

* **`ensure_indices`** (`db/omnigraph/table_ops.rs::build_indices_on_dataset_for_catalog`)
  — scalar indices (BTree, Inverted) now use `stage_create_*_index` +
  `commit_staged`. Vector indices stay inline (residual — Lance
  `build_index_metadata_from_segments` is `pub(crate)` in 4.0.0;
  companion ticket to lance-format/lance#6658 needed).
* **`branch_merge::publish_rewritten_merge_table`**
  (`exec/merge.rs`) — merge_insert now uses `stage_merge_insert` +
  `commit_staged`. Deletes stay inline (Lance #6658 residual).
* **`schema_apply` rewritten_tables** (`db/omnigraph/schema_apply.rs`)
  — non-empty rewrites use `stage_overwrite` + `commit_staged`.
  Empty-batch rewrites stay inline (Lance `InsertBuilder::execute_uncommitted`
  rejects empty data; the empty case is rare and bounded by the
  schema-apply lock branch).

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

### `LoadMode::Overwrite` residual

The bulk loader's Append and Merge modes use the staged-write path
described above. `LoadMode::Overwrite` keeps the legacy inline-commit
path: truncate-then-append doesn't fit the staged shape cleanly in
Lance 4.0.0, and overwrite has no in-flight read-your-writes
requirement (the prior data is being wiped). A mid-overwrite failure
can leave Lance HEAD on a partially-truncated table; the next overwrite
will replace it. Operator-driven (rare in agent workloads); document
permanently until Lance exposes `Operation::Overwrite { fragments }` as
a two-phase op.

### Finalize → publisher residual

The staged-write rewire eliminates one drift class **by construction at
the writer layer**: an op that fails before pushing to the in-memory
accumulator (validation errors, missing endpoints, parse-time D₂
rejection) leaves Lance HEAD untouched on every staged table. This is
the case the `partial_failure_leaves_target_queryable_and_unblocks_next_mutation`
test pins.

A second, narrower drift class remains. `MutationStaging::finalize`
runs `stage_*` + `commit_staged` per touched table sequentially, then
the publisher commits the manifest. Lance has no multi-dataset atomic
commit, so the per-table `commit_staged` calls are independent
operations: if commit_staged on table N+1 fails *after* commit_staged
on tables 1..N succeeded, or if the publisher's CAS pre-check rejects
*after* every commit_staged succeeded, tables 1..N are left at
`Lance HEAD = manifest_pinned + 1`. The next mutation against those
tables surfaces `ManifestConflictDetails::ExpectedVersionMismatch` —
the same loud failure mode the rewire was designed to make rare, just
no longer "unreachable."

Triggers: transient Lance write errors during finalize (object-store
retry budget exhaustion, disk full); persistent publisher contention
exceeding `PUBLISHER_RETRY_BUDGET = 5` retries. Closing this requires
either a Lance multi-dataset atomic-commit primitive (filed upstream
alongside the two-phase delete request) or a manifest-layer journal
that replays staged commits on next open. Both are heavyweight; the
v1 stance is "narrowed window, documented residual, surface the loud
error when it fires."

The publisher-CAS contract is unchanged: a *concurrent writer* that
advances any of our touched tables between snapshot capture and
publisher commit produces exactly one winner. The residual above is
about *our* abandoned commits in the failure path, not about
concurrency races.

## Conflict shape

Concurrent writers to the same `(table, branch)` produce exactly one
success and one failure. The losing writer's error is
`OmniError::Manifest` with kind `Conflict` and details
`ManifestConflictDetails::ExpectedVersionMismatch { table_key, expected,
actual }`. The HTTP server maps this to **409 Conflict** with body
`{"error": "...", "code": "conflict", "manifest_conflict": { "table_key":
"...", "expected": N, "actual": M }}` — see [docs/server.md](server.md).

## Audit

`actor_id` lands in `_graph_commits.lance` via `record_graph_commit` (no
intermediate run record). Audit history is queried via `omnigraph commit
list`.

## Migration code

`db/manifest/migrations.rs` does not change. Active deletion of
`_graph_runs.lance` belongs in MR-770 (the production sweep) — this PR
stops *creating* run state but does not destroy legacy bytes on disk.

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
preserved (Lance has no public two-phase delete in 4.0.0) — the same
narrow window remains. The parse-time D₂ rule prevents inserts/updates
from coexisting with deletes in one query, so a pure-delete failure
cannot drift any staged-table state. If a delete-only multi-table
mutation fails mid-cascade, the same workaround as before applies
(retry; rely on `omnigraph cleanup` once a later successful commit
moves HEAD past the orphan version). Closing this requires Lance to
expose `DeleteJob::execute_uncommitted`; tracked in MR-793 and a
Lance-upstream ticket.

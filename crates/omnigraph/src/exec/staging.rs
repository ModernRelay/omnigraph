//! Per-query staging accumulator for direct-publish writes.
//!
//! `MutationStaging` accumulates per-table input batches in memory during a
//! `mutate_as` or `load` query, then at end-of-query commits each touched
//! table via Lance's distributed-write API (one `stage_*` + `commit_staged`
//! per table) and returns the publisher inputs (`SubTableUpdate` list +
//! `expected_table_versions`).
//!
//! Read-your-writes within the same query is satisfied by the in-memory
//! pending batches (see `pending_batches`) — read sites union the committed
//! Lance scan with the pending Arrow batches via DataFusion `MemTable` (see
//! `crate::table_store::TableStore::scan_with_pending`).
//!
//! This module is shared by the engine's mutation path (`exec/mutation.rs`)
//! and the bulk loader (`loader/mod.rs`); both feed insert/update batches
//! into `pending` and route end-of-query commits through `finalize`.
//! Deletes follow the inline-commit path and are recorded via
//! `record_inline` (parse-time D₂ rule prevents mixed insert/delete in a
//! single query, so no flushing is required).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::storage_layer::{SnapshotHandle, StagedHandle};
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use futures::stream::StreamExt;
use omnigraph_compiler::catalog::EdgeType;

use crate::db::manifest::{
    RecoverySidecarHandle, SidecarKind, SidecarTablePin, new_sidecar, write_sidecar,
};
use crate::db::{MutationOpKind, SubTableUpdate};
use crate::error::{OmniError, Result};

/// Whether the per-table accumulator should commit via `stage_append`,
/// `stage_merge_insert`, or `stage_overwrite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingMode {
    Append,
    Merge,
    Overwrite,
}

/// Per-table accumulator. Each insert/update op pushes a `RecordBatch` into
/// `batches`; at end-of-query the accumulated batches concat into a single
/// stage call.
#[derive(Debug)]
pub(crate) struct PendingTable {
    pub(crate) schema: SchemaRef,
    pub(crate) mode: PendingMode,
    pub(crate) batches: Vec<RecordBatch>,
}

impl PendingTable {
    fn new(schema: SchemaRef, mode: PendingMode) -> Self {
        Self {
            schema,
            mode,
            batches: Vec::new(),
        }
    }

    fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

/// Stable per-table identifiers captured on first touch and reused at
/// finalize time. Avoids re-resolving the dataset path / branch.
#[derive(Debug, Clone)]
pub(crate) struct StagedTablePath {
    pub(crate) full_path: String,
    pub(crate) table_branch: Option<String>,
}

/// Per-query staging state.
///
/// Replaces the legacy inline-commit `MutationStaging.latest` map with
/// an in-memory accumulator that defers all Lance HEAD advances to
/// end-of-query. After this rewire the bug class "Lance HEAD drifts ahead
/// of `__manifest`" is unreachable in `mutate_as` and `load` for inserts
/// and updates by construction.
#[derive(Default)]
pub(crate) struct MutationStaging {
    /// Pre-write manifest version per table — the publisher's CAS fence at
    /// end-of-query.
    pub(crate) expected_versions: HashMap<String, u64>,
    /// Per-table identifiers captured on first touch.
    pub(crate) paths: HashMap<String, StagedTablePath>,
    /// In-memory accumulated batches per table (insert/update path).
    pub(crate) pending: HashMap<String, PendingTable>,
    /// Inline-committed updates from delete-touching ops (D₂ guarantees no
    /// pending batches exist on a delete-touched table).
    pub(crate) inline_committed: HashMap<String, SubTableUpdate>,
    /// Strictest [`MutationOpKind`] seen per table within this query. Drives
    /// the op-kind-aware drift check in [`StagedMutation::commit_all`]: for
    /// tables whose first or any subsequent touch was a strict op
    /// (Update / Delete / SchemaRewrite), commit_all fails fast with 409
    /// when the staged dataset version drifts from the fresh manifest pin
    /// rather than letting Lance's `commit_staged` surface
    /// `RetryableCommitConflict` as a 500. See
    /// [`MutationOpKind::strict_pre_stage_version_check`].
    pub(crate) op_kinds: HashMap<String, MutationOpKind>,
}

impl MutationStaging {
    /// Capture pre-write metadata on first touch of a table. Subsequent
    /// touches preserve the original `paths` and `expected_versions`
    /// entries; `op_kinds` upgrades to the strictest kind seen so far so
    /// that mixed insert+update on the same table still fires the strict
    /// drift check at commit time.
    pub(crate) fn ensure_path(
        &mut self,
        table_key: &str,
        full_path: String,
        table_branch: Option<String>,
        expected_version: u64,
        op_kind: MutationOpKind,
    ) {
        self.paths
            .entry(table_key.to_string())
            .or_insert(StagedTablePath {
                full_path,
                table_branch,
            });
        self.expected_versions
            .entry(table_key.to_string())
            .or_insert(expected_version);
        self.op_kinds
            .entry(table_key.to_string())
            .and_modify(|existing| {
                // Upgrade to the stricter kind if a later op needs it.
                // Insert + later Update → Update wins; Update + later Insert
                // keeps Update.
                if op_kind.strict_pre_stage_version_check()
                    && !existing.strict_pre_stage_version_check()
                {
                    *existing = op_kind;
                }
            })
            .or_insert(op_kind);
    }

    /// Append a batch to the per-table accumulator.
    ///
    /// `mode` is asserted-consistent with prior pushes for the same table:
    /// `Append`+`Append` stays Append; any `Merge` upgrades the table to
    /// Merge (e.g. an `update Person` after `insert Knows from='X' to='Y'`
    /// when both produce content on `node:Person`). Once Merge is set,
    /// subsequent appends roll into the merge stream — `WhenNotMatched =
    /// InsertAll` correctly inserts append-shaped rows.
    pub(crate) fn append_batch(
        &mut self,
        table_key: &str,
        schema: SchemaRef,
        mode: PendingMode,
        batch: RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 && mode != PendingMode::Overwrite {
            // No-op for additive modes. For Overwrite, an empty batch is
            // observable: it means "replace this table with zero rows".
            return Ok(());
        }
        // If we've already accumulated a batch on this table, the new
        // batch's schema MUST match the existing accumulator's schema.
        // The mismatch case in practice is a blob-bearing table that
        // sees an `insert` (full schema, blob columns included) and
        // then an `update` whose `apply_assignments` output omits
        // unassigned blob columns (subset schema). Concat-time and
        // MemTable-construction errors would catch this later, but
        // surfacing it at the offending `append_batch` call gives the
        // caller a clearer point of failure attached to the specific
        // op that introduced the drift.
        if let Some(existing) = self.pending.get(table_key) {
            if existing.mode == PendingMode::Overwrite || mode == PendingMode::Overwrite {
                if existing.mode != mode {
                    return Err(OmniError::manifest_internal(format!(
                        "table '{}' cannot mix overwrite staging with append/merge staging",
                        table_key
                    )));
                }
            }
            if !schemas_compatible(&existing.schema, &batch.schema()) {
                return Err(OmniError::manifest(format!(
                    "table '{}' accumulated mutation batches with mismatched schemas: \
                     prior batches have {} columns, this batch has {}. \
                     This typically happens on a blob-bearing table when one \
                     op uses the full schema (e.g. an `insert`) and another \
                     omits unassigned blob columns (e.g. an `update` that \
                     doesn't set every blob property). Split the mutation \
                     into two queries: one for the inserts, one for the \
                     updates.",
                    table_key,
                    existing.schema.fields().len(),
                    batch.schema().fields().len(),
                )));
            }
        }
        let entry = self
            .pending
            .entry(table_key.to_string())
            .or_insert_with(|| PendingTable::new(schema.clone(), mode));
        // Upgrade Append -> Merge if any op needs merge semantics. Overwrite
        // is never mixed with additive modes (guarded above).
        if mode == PendingMode::Merge && entry.mode == PendingMode::Append {
            entry.mode = PendingMode::Merge;
        }
        entry.batches.push(batch);
        Ok(())
    }

    /// Record a delete that already inline-committed at the Lance layer.
    pub(crate) fn record_inline(&mut self, update: SubTableUpdate) {
        self.inline_committed
            .insert(update.table_key.clone(), update);
    }

    /// Read-your-writes accessor: the accumulated pending batches for
    /// `table_key`, or `&[]` if none.
    pub(crate) fn pending_batches(&self, table_key: &str) -> &[RecordBatch] {
        self.pending
            .get(table_key)
            .map(|p| p.batches.as_slice())
            .unwrap_or(&[])
    }

    /// Accumulator mode for `table_key`, if this query has touched it.
    pub(crate) fn pending_mode(&self, table_key: &str) -> Option<PendingMode> {
        self.pending.get(table_key).map(|p| p.mode)
    }

    /// Schema of the accumulated batches for `table_key`, or `None` if no
    /// op has touched the table. Used by `scan_with_pending` to construct
    /// the in-memory `MemTable`.
    pub(crate) fn pending_schema(&self, table_key: &str) -> Option<SchemaRef> {
        self.pending.get(table_key).map(|p| p.schema.clone())
    }

    /// `true` if neither pending nor inline_committed has any state — the
    /// query made no observable writes.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.inline_committed.is_empty()
    }

    /// Total count of pending rows across all tables. Used by tests and
    /// (eventually) memory-budget enforcement.
    #[allow(dead_code)]
    pub(crate) fn pending_row_count(&self) -> usize {
        self.pending.values().map(|p| p.total_rows()).sum()
    }

    /// End-of-query: for each pending table, concat batches and commit via
    /// **Phase A** of the two-phase commit: stage uncommitted fragments
    /// for every table in `pending`. No Lance HEAD movement, no sidecar,
    /// no manifest publish. Returns a [`StagedMutation`] carrying the
    /// staged transactions so a future MR-686 queue acquisition step can
    /// run between staging (slow S3 PUTs, no queue) and commit (fast,
    /// under per-`(table_key, branch)` queue).
    ///
    /// Sequential per-table for now — parallelizing across independent
    /// Lance datasets is a perf follow-up; same loop structure as the
    /// pre-split `finalize`.
    pub(crate) async fn stage_all(
        self,
        db: &crate::db::Omnigraph,
        branch: Option<&str>,
    ) -> Result<StagedMutation> {
        self.stage_all_with_concurrency(db, branch, 1).await
    }

    /// Loader-facing variant of [`stage_all`] that preserves
    /// `OMNIGRAPH_LOAD_CONCURRENCY` for the fragment-writing stage while
    /// still leaving all Lance HEAD movement to [`StagedMutation::commit_all`].
    pub(crate) async fn stage_all_with_concurrency(
        self,
        db: &crate::db::Omnigraph,
        _branch: Option<&str>,
        concurrency: usize,
    ) -> Result<StagedMutation> {
        let MutationStaging {
            expected_versions,
            paths,
            pending,
            inline_committed,
            op_kinds,
        } = self;

        let mut stage_inputs: Vec<(String, PendingTable, StagedTablePath, u64)> =
            Vec::with_capacity(pending.len());
        for (table_key, table) in pending {
            let path = paths.get(&table_key).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::stage_all: missing path for table '{}'",
                    table_key
                ))
            })?;
            let expected = *expected_versions.get(&table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::stage_all: missing expected version for table '{}'",
                    table_key
                ))
            })?;
            stage_inputs.push((table_key, table, path, expected));
        }
        let concurrency = concurrency.min(stage_inputs.len()).max(1);
        let staged_entries = futures::stream::iter(stage_inputs.into_iter().map(
            |(table_key, table, path, expected)| async move {
                stage_pending_table(db, table_key, table, path, expected).await
            },
        ))
        .buffered(concurrency)
        .collect::<Vec<Result<Option<StagedTableEntry>>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

        Ok(StagedMutation {
            inline_committed,
            staged: staged_entries,
            expected_versions,
            paths,
            op_kinds,
        })
    }
}

async fn stage_pending_table(
    db: &crate::db::Omnigraph,
    table_key: String,
    table: PendingTable,
    path: StagedTablePath,
    expected: u64,
) -> Result<Option<StagedTableEntry>> {
    // Reopen the dataset for staging. Append/Merge can be rebased later by
    // Lance + publisher CAS; Overwrite is a strict replacement and uses the
    // same SchemaRewrite policy as schema apply.
    let stage_kind = match table.mode {
        PendingMode::Append => crate::db::MutationOpKind::Insert,
        PendingMode::Merge => crate::db::MutationOpKind::Merge,
        PendingMode::Overwrite => crate::db::MutationOpKind::SchemaRewrite,
    };
    let ds = db
        .reopen_for_mutation(
            &table_key,
            &path.full_path,
            path.table_branch.as_deref(),
            expected,
            stage_kind,
        )
        .await?;

    if table.batches.is_empty() {
        return Ok(None);
    }

    let combined = match table.mode {
        PendingMode::Merge => dedupe_merge_batches_by_id(&table.schema, table.batches)?,
        PendingMode::Append | PendingMode::Overwrite => {
            if table.batches.len() == 1 {
                table.batches.into_iter().next().unwrap()
            } else {
                arrow_select::concat::concat_batches(&table.schema, &table.batches)
                    .map_err(|e| OmniError::Lance(e.to_string()))?
            }
        }
    };

    // Stage produces uncommitted fragments + transaction. No Lance HEAD
    // advance until `commit_all` runs `commit_staged`.
    let staged = match table.mode {
        PendingMode::Append => db.storage().stage_append(&ds, combined, &[]).await?,
        PendingMode::Merge => {
            db.storage()
                .stage_merge_insert(
                    ds.clone(),
                    combined,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?
        }
        PendingMode::Overwrite => db.storage().stage_overwrite(&ds, combined).await?,
    };
    Ok(Some(StagedTableEntry {
        table_key,
        path,
        expected_version: expected,
        dataset: ds,
        staged_write: staged,
    }))
}

/// Output of [`MutationStaging::stage_all`]. Carries the staged Lance
/// transactions (Phase A complete; uncommitted fragments written) plus
/// the per-table metadata needed to write the recovery sidecar, run
/// `commit_staged` (Phase B), and produce the publisher's input.
///
/// Splitting `stage_all` and `commit_all` is the structural prerequisite
/// for MR-686: a future commit can drop queue acquisition + manifest-pin
/// revalidation between Phase A and Phase B without touching staging
/// logic.
pub(crate) struct StagedMutation {
    /// Updates from delete-touching ops (D₂ parse-time rule keeps
    /// pending and inline_committed disjoint per table). Tables here
    /// have already advanced Lance HEAD via inline `delete_where`;
    /// `commit_all` builds sidecar pins for these too so the
    /// commit→publish residual is recoverable for delete-only paths
    /// (third-agent Finding 3).
    inline_committed: HashMap<String, SubTableUpdate>,
    /// One entry per table that had pending batches successfully staged.
    staged: Vec<StagedTableEntry>,
    /// Pre-write manifest version per table — the publisher's CAS fence.
    expected_versions: HashMap<String, u64>,
    /// Per-table identifiers from `MutationStaging::paths`. Carried
    /// through so `commit_all` can build sidecar pins for both staged
    /// and inline-committed tables.
    paths: HashMap<String, StagedTablePath>,
    /// Strictest op_kind per touched table, propagated from
    /// `MutationStaging::op_kinds` so `commit_all`'s drift check
    /// fires only on read-modify-write tables.
    op_kinds: HashMap<String, MutationOpKind>,
}

/// Per-table state captured during `stage_all` and consumed by
/// `commit_all`. Holds the opened snapshot (so `commit_staged` doesn't
/// re-open) plus the staged Lance transaction that `commit_staged`
/// will execute. Both held as opaque `TableStorage` handles per MR-793
/// §III.9 — the inner `lance::Dataset` / `StagedWrite` are not visible
/// to engine code outside the storage layer.
struct StagedTableEntry {
    table_key: String,
    path: StagedTablePath,
    expected_version: u64,
    dataset: SnapshotHandle,
    staged_write: StagedHandle,
}

impl StagedMutation {
    /// **Phase B** of the two-phase commit: acquire per-`(table_key,
    /// branch)` queues, revalidate manifest pins, write the recovery
    /// sidecar, run `commit_staged` per table to advance Lance HEAD, and
    /// return the publisher's input plus the queue guards.
    ///
    /// **Caller must hold the returned `_guards` Vec across the
    /// subsequent manifest publish.** Releasing guards before publish
    /// would let another writer interleave their commit_staged between
    /// ours and our publish — which would correctly fail our CAS but
    /// leave Lance HEAD advanced (the residual class MR-870 recovers
    /// from). Holding the guards across publish keeps the residual
    /// unreachable for op-execution failures on the happy path.
    ///
    /// Revalidation: between `stage_all` and `commit_all`, another
    /// writer (in the same process or another process sharing the
    /// graph) may have committed to one of our touched tables, advancing
    /// the manifest pin past our `expected_version`. We revalidate
    /// under the queue and fail-fast with `manifest_conflict` before
    /// any `commit_staged` so the orphaned uncommitted fragments stay
    /// unreferenced (cleaned by `cleanup_old_versions`'s age sweep)
    /// rather than being committed and creating a Lance-HEAD-ahead
    /// residual.
    /// `held_guards`: when the caller already holds the per-`(table_key,
    /// branch)` write queues for every touched table (the fork path acquires
    /// them up front, before the fork, and holds them through the manifest
    /// publish), it passes `(acquired_keys, guards)` here so `commit_all`
    /// reuses them instead of re-acquiring — the queue is a non-re-entrant
    /// `tokio::Mutex`, so re-acquiring a held key would self-deadlock.
    /// `None` (the steady-state path) means `commit_all` acquires them
    /// itself. `acquired_keys` must cover every key `commit_all` would
    /// acquire (debug-asserted below) — the guards from `acquire_many` don't
    /// carry their keys, so the caller hands the key set alongside them. The
    /// fork path guarantees coverage by keying every touched table uniformly
    /// by the resolved target branch.
    pub(crate) async fn commit_all(
        self,
        db: &crate::db::Omnigraph,
        branch: Option<&str>,
        sidecar_kind: SidecarKind,
        actor_id: Option<&str>,
        held_guards: Option<(
            Vec<(String, Option<String>)>,
            Vec<tokio::sync::OwnedMutexGuard<()>>,
        )>,
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<(
        Vec<SubTableUpdate>,
        HashMap<String, u64>,
        Option<RecoverySidecarHandle>,
        Vec<tokio::sync::OwnedMutexGuard<()>>,
        // Post-`commit_staged` handle per STAGED table (table_key → handle at
        // the just-committed version). Carried out (RFC-013 step 3b, collapse
        // #4) so the publish-prepare index build reuses it instead of a fresh
        // `reopen_for_mutation` at the same version. Inline-committed / delete
        // tables are NOT in the map (they have no staged handle).
        HashMap<String, SnapshotHandle>,
    )> {
        let StagedMutation {
            inline_committed,
            mut staged,
            mut expected_versions,
            paths,
            op_kinds,
        } = self;

        // Per-(table_key, branch) queues for every touched table — both
        // staged and inline-committed. Sorted by `acquire_many` internally
        // so all multi-table writers (mutation, branch_merge, schema_apply,
        // the fork path, recovery) agree on acquisition order — prevents
        // lock-order inversion deadlock.
        //
        // For inline-committed tables (delete-only mutations), Lance HEAD
        // has already advanced inside `delete_where` before `commit_all`
        // runs. Holding the queue here prevents another writer from
        // interleaving between our delete and our publish, which would
        // otherwise leave a Lance-HEAD-ahead residual the delete-only
        // sidecar (added below) would have to recover.
        let mut queue_keys: Vec<(String, Option<String>)> =
            Vec::with_capacity(staged.len() + inline_committed.len());
        for entry in &staged {
            queue_keys.push((entry.table_key.clone(), entry.path.table_branch.clone()));
        }
        for table_key in inline_committed.keys() {
            let path = paths.get(table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "StagedMutation::commit_all: missing path for inline-committed table '{}'",
                    table_key
                ))
            })?;
            queue_keys.push((table_key.clone(), path.table_branch.clone()));
        }
        // Reuse the caller's guards (fork path) when handed in, else acquire
        // our own. When reusing, every key we would acquire MUST already be
        // covered — re-acquiring a held non-re-entrant key would deadlock, and
        // a key we'd need but DON'T hold would commit unserialized. This is a
        // load-bearing safety invariant, so it is checked in ALL builds (not a
        // debug_assert) and fails the write loudly+safely rather than silently
        // proceeding unguarded if a future execution path ever touches a table
        // outside the caller's pre-computed set.
        let guards = match held_guards {
            Some((acquired_keys, guards)) => {
                let held: std::collections::HashSet<&(String, Option<String>)> =
                    acquired_keys.iter().collect();
                if let Some(missing) = queue_keys.iter().find(|k| !held.contains(k)) {
                    return Err(OmniError::manifest_internal(format!(
                        "commit_all: pre-held write-queue guards do not cover touched table \
                         '{}' on branch {:?} — the caller's up-front acquisition set diverged \
                         from the staged/inline set (a touched-table-set bug)",
                        missing.0, missing.1
                    )));
                }
                guards
            }
            None => db.write_queue().acquire_many(&queue_keys).await,
        };

        // Re-capture manifest pins under the queue (PR 2 / MR-686).
        //
        // expected_versions was captured when the mutation first opened
        // each table for mutation (the query's read-time pin). For
        // non-strict inserts / merge-style appends, a writer may advance
        // the table before we acquire the queue and Lance can still
        // safely rebase the write, so we refresh expected_versions to
        // the queued manifest pin.
        //
        // Strict read-modify-write ops (update / delete /
        // schema-rewrite) are different: the staged batch was computed
        // against the read-time pin, even if stage_all later re-opened
        // the dataset at HEAD. For those ops, compare read-time
        // expected_version to the queued manifest pin and fail before
        // any Lance HEAD movement if the target drifted. This can
        // over-reject a single mutation that inserts, then upgrades to
        // update, while another writer advances the table between the
        // two touches; that is safe-by-default and keeps one invariant
        // until `ensure_path` learns how to bump expected_version on
        // op-kind upgrade.
        //
        // Why a fresh per-branch snapshot (and not the bound-branch
        // `db.snapshot()` / `snapshot_for_branch()` fast path): a stale
        // engine handle may be bound to the same branch it is writing. For
        // non-strict Insert/Merge, that stale local view is allowed to rebase
        // to the live manifest pin under the queue; only uncovered Lance
        // HEAD>manifest drift is refused. For writes targeting a branch other
        // than the engine's bound branch (e.g., feature-branch ingest from a
        // server handle bound to main), the same helper also resolves the
        // correct branch pin. The cost is one fresh manifest read per mutation
        // plus one Lance HEAD open per staged table for the drift guard below.
        //
        // Multi-coordinator deployments (§VI.27 aspirational) get
        // genuine cross-process drift detection from this read for
        // free.
        //
        // This MUST be a FRESH per-branch manifest read (never the warm
        // cache) for the OCC re-capture below — but with a `WriteTxn` the
        // schema contract was already validated at capture, so use the
        // `_unchecked` variant, which drops the redundant
        // `ensure_schema_state_valid` AND the commit-graph load the OCC read
        // never consults (a fresh manifest read yields the same `Snapshot`).
        // Without a txn this is byte-identical to the prior checked call.
        let snapshot = match txn {
            Some(_) => db.fresh_snapshot_for_branch_unchecked(branch).await?,
            None => db.fresh_snapshot_for_branch(branch).await?,
        };
        for entry in staged.iter_mut() {
            let current = snapshot
                .entry(&entry.table_key)
                .map(|e| e.table_version)
                .ok_or_else(|| {
                    OmniError::manifest_conflict(format!(
                        "table '{}' missing from manifest at commit time",
                        entry.table_key,
                    ))
                })?;

            // Insert / Merge tables skip this check: concurrent inserts on
            // disjoint keys legitimately coexist via Lance's auto-rebase, so
            // the check would over-reject the existing Phase 2 same-key
            // insert path (`change_concurrent_inserts_same_key_serialize_without_409`).
            let strict = op_kinds
                .get(&entry.table_key)
                .map(|k| k.strict_pre_stage_version_check())
                .unwrap_or(false);
            if strict && entry.expected_version != current {
                return Err(OmniError::manifest_expected_version_mismatch(
                    entry.table_key.clone(),
                    entry.expected_version,
                    current,
                ));
            }

            // Separate manifest-visible concurrency from uncovered Lance drift.
            // Non-strict inserts/merges are allowed to rebase from their staged
            // read version to the fresh manifest pin above, but only if the
            // live Lance HEAD still equals that manifest pin. If an external
            // raw Lance write or a pre-fix maintenance path moved HEAD without
            // publishing `__manifest`, this write must not silently fold it.
            //
            // `latest_version_id` reads the latest manifest pointer off the
            // already-open staged handle (the #2 staging open) WITHOUT a fresh
            // `Dataset::open` — the same cheap live-HEAD probe
            // `ManifestCoordinator::probe_latest_version` uses. This replaces a
            // redundant `open_dataset_head_for_write` (RFC-013 step 3b, collapse
            // #3): the drift comparison below is byte-identical; only how `head`
            // is obtained changes (probe vs cold open).
            let head = entry
                .dataset
                .dataset()
                .latest_version_id()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            if head < current {
                return Err(OmniError::manifest_internal(format!(
                    "table '{}' Lance HEAD version {} is behind manifest version {}",
                    entry.table_key, head, current
                )));
            }
            if head > current {
                // Error path only: tell the operator which drift class
                // this is. Uncovered drift (external raw Lance write,
                // pre-fix maintenance) goes through `omnigraph repair`.
                // Sidecar-covered drift reaching this guard means the
                // write-entry heal deferred it (rollback-eligible), and
                // `repair` refuses while a sidecar is pending — the
                // recovery path is a read-write reopen. A list failure
                // must not mask the conflict — and must not pick a
                // class confidently either: "could not classify" names
                // both paths and the cause, never routing the operator
                // to a command that will refuse.
                let action = match crate::db::manifest::list_sidecars(
                    db.root_uri(),
                    db.storage_adapter(),
                )
                .await
                {
                    Ok(sidecars) => {
                        let covered = sidecars.iter().any(|sidecar| {
                            sidecar.tables.iter().any(|pin| {
                                // Branch-aware: a sidecar pinning the
                                // same table on ANOTHER branch does not
                                // cover this branch's drift — a reopen
                                // would recover that sidecar but leave
                                // this drift for `repair`.
                                pin.table_key == entry.table_key
                                    && pin.table_branch == entry.path.table_branch
                            })
                        });
                        if covered {
                            "a pending recovery sidecar requires rollback — reopen the \
                             graph read-write (e.g. restart the server) to recover"
                                .to_string()
                        } else {
                            "run `omnigraph repair` before writing".to_string()
                        }
                    }
                    Err(list_err) => format!(
                        "could not classify the drift (sidecar listing failed: {}); \
                         run `omnigraph repair`, or reopen the graph read-write if \
                         repair reports a pending recovery sidecar",
                        list_err
                    ),
                };
                return Err(OmniError::manifest_conflict(format!(
                    "table '{}' has Lance HEAD version {} ahead of manifest version {}; {}",
                    entry.table_key, head, current, action
                )));
            }

            entry.expected_version = current;
            expected_versions.insert(entry.table_key.clone(), current);
        }
        // Sidecar protocol: build the per-table pin list and write the
        // sidecar BEFORE any later error can return after Lance HEAD has
        // already moved. For staged tables this still happens before any
        // Lance commit_staged runs. For inline-committed delete tables,
        // Lance HEAD moved inside delete_where before commit_all, so the
        // sidecar must also exist before the inline manifest-version check
        // below can reject a stale query.
        //
        // Pins cover BOTH staged tables (Lance HEAD will advance below
        // when `commit_staged` runs) AND inline-committed tables
        // (Lance HEAD already advanced inside `delete_where` — we still
        // need a sidecar so that an upcoming publish failure is
        // recoverable on next open). This closes the third-agent
        // Finding 3 hazard: delete-only mutations would otherwise skip
        // the sidecar, leaving any commit→publish residual unreachable
        // by recovery.
        let mut pins: Vec<SidecarTablePin> =
            Vec::with_capacity(staged.len() + inline_committed.len());
        for entry in &staged {
            pins.push(SidecarTablePin {
                table_key: entry.table_key.clone(),
                table_path: entry.path.full_path.clone(),
                expected_version: entry.expected_version,
                post_commit_pin: entry.expected_version + 1,
                // Mutation/Load use strict single-commit classification, not
                // BranchMerge's Phase-B confirmation — left None.
                confirmed_version: None,
                table_branch: entry.path.table_branch.clone(),
            });
        }
        for (table_key, update) in &inline_committed {
            let path = paths.get(table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "StagedMutation::commit_all: missing path for inline-committed table '{}'",
                    table_key
                ))
            })?;
            let expected = *expected_versions.get(table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "StagedMutation::commit_all: missing expected version for inline-committed table '{}'",
                    table_key
                ))
            })?;
            pins.push(SidecarTablePin {
                table_key: table_key.clone(),
                table_path: path.full_path.clone(),
                expected_version: expected,
                // For inline-committed tables, the post-commit pin is
                // the actual post-delete version recorded by
                // `record_inline`, NOT `expected + 1` — `delete_where`
                // can advance HEAD by more than one version (e.g.,
                // when Lance internally compacts deletion vectors).
                post_commit_pin: update.table_version,
                confirmed_version: None,
                table_branch: path.table_branch.clone(),
            });
        }

        let sidecar_handle = if pins.is_empty() {
            None
        } else {
            let sidecar = new_sidecar(
                sidecar_kind,
                branch.map(|s| s.to_string()),
                actor_id.map(str::to_string),
                pins,
            );
            Some(write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar).await?)
        };

        for (table_key, _update) in inline_committed.iter() {
            let current = snapshot
                .entry(table_key)
                .map(|e| e.table_version)
                .ok_or_else(|| {
                    OmniError::manifest_conflict(format!(
                        "table '{}' missing from manifest at commit time",
                        table_key,
                    ))
                })?;
            let expected = expected_versions.get(table_key).copied().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "StagedMutation::commit_all: missing expected version for inline-committed table '{}'",
                    table_key
                ))
            })?;
            if expected != current {
                return Err(OmniError::manifest_expected_version_mismatch(
                    table_key.clone(),
                    expected,
                    current,
                ));
            }
            expected_versions.insert(table_key.clone(), current);
        }

        let mut updates: Vec<SubTableUpdate> = inline_committed.into_values().collect();

        // Carry each staged table's post-`commit_staged` handle out so the
        // publish-prepare index build reuses it (collapse #4) instead of
        // re-opening the dataset at the same just-committed version.
        let mut committed_handles: HashMap<String, SnapshotHandle> =
            HashMap::with_capacity(staged.len());

        for entry in staged {
            let StagedTableEntry {
                table_key,
                path,
                expected_version: _,
                dataset,
                staged_write,
            } = entry;

            let new_ds = db.storage().commit_staged(dataset, staged_write).await?;
            let state = db.storage().table_state(&path.full_path, &new_ds).await?;
            updates.push(SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: path.table_branch.clone(),
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
            committed_handles.insert(table_key, new_ds);
        }

        Ok((
            updates,
            expected_versions,
            sidecar_handle,
            guards,
            committed_handles,
        ))
    }
}

/// Walk `batches` in reverse, tracking seen `id` values; for each row
/// whose id we have NOT seen yet, mark it as a keeper. After the walk,
/// take the kept rows in forward (input) order and concat into one batch.
///
/// Result: a deduped batch where each `id` appears at most once, with
/// the LAST occurrence's column values. Required by `stage_merge_insert`,
/// which needs unique source keys (Lance's `MergeInsertBuilder` produces
/// arbitrary results on duplicates).
///
/// `batches` must be non-empty and all share `schema` (caller enforces).
/// Compare two schemas for the purposes of `MutationStaging::append_batch`'s
/// accumulator-compatibility check. We treat schemas as compatible if
/// they have the same field names and data types in the same order.
/// Nullability and field metadata differences are tolerated — Lance and
/// Arrow round-trip these freely and the accumulator's downstream
/// `concat_batches` is also permissive on those.
fn schemas_compatible(a: &SchemaRef, b: &SchemaRef) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    for (af, bf) in a.fields().iter().zip(b.fields().iter()) {
        if af.name() != bf.name() || af.data_type() != bf.data_type() {
            return false;
        }
    }
    true
}

fn dedupe_merge_batches_by_id(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(OmniError::manifest_internal(
            "dedupe_merge_batches_by_id: batches is empty".to_string(),
        ));
    }

    // Walk in reverse, tracking seen ids. For each row whose id we
    // haven't seen yet, record (batch_idx, row_idx) for the kept set.
    let mut seen: HashSet<String> = HashSet::new();
    let mut keep: Vec<Vec<u32>> = vec![Vec::new(); batches.len()];
    let mut any_duplicates = false;

    for (b_idx, batch) in batches.iter().enumerate().rev() {
        let id_col = batch
            .column_by_name("id")
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "dedupe_merge_batches_by_id: batch has no 'id' column".to_string(),
                )
            })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "dedupe_merge_batches_by_id: 'id' column is not Utf8".to_string(),
                )
            })?;
        for r_idx in (0..batch.num_rows()).rev() {
            if !id_col.is_valid(r_idx) {
                // NULL ids — keep all (NULL != NULL in Lance/SQL semantics).
                keep[b_idx].push(r_idx as u32);
                continue;
            }
            let id = id_col.value(r_idx);
            if seen.insert(id.to_string()) {
                keep[b_idx].push(r_idx as u32);
            } else {
                any_duplicates = true;
            }
        }
        // We pushed in reverse-row order; flip to forward order so the
        // emitted batch reflects insertion order.
        keep[b_idx].reverse();
    }

    // Fast path: no duplicates → simple concat.
    if !any_duplicates {
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }
        return arrow_select::concat::concat_batches(schema, &batches)
            .map_err(|e| OmniError::Lance(e.to_string()));
    }

    // Slow path: build per-batch slices via `take`, then concat.
    let mut sliced: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for (b_idx, idxs) in keep.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }
        let take_array = UInt32Array::from(idxs);
        let columns: Vec<Arc<dyn Array>> = batches[b_idx]
            .columns()
            .iter()
            .map(|col| arrow_select::take::take(col, &take_array, None))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let new_batch = RecordBatch::try_new(batches[b_idx].schema(), columns)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        sliced.push(new_batch);
    }
    if sliced.is_empty() {
        return Err(OmniError::manifest_internal(
            "dedupe_merge_batches_by_id: all rows were dropped (unexpected)".to_string(),
        ));
    }
    if sliced.len() == 1 {
        return Ok(sliced.into_iter().next().unwrap());
    }
    arrow_select::concat::concat_batches(schema, &sliced)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Cardinality helpers (shared by mutation + loader paths) ────────────────

/// Count edges per `src` value across committed (Lance scan) + pending
/// (in-memory). Caller supplies an opened committed dataset so the
/// mutation path (which already has one) and the loader path (which
/// opens via snapshot) share the same body. For overwrite staging, the
/// pending batches are the replacement table image, so committed rows are
/// intentionally skipped.
///
/// `dedupe_key_column` controls whether committed rows are shadowed by
/// pending:
/// - `None` — every committed row counts, every pending row counts.
///   Correct when committed and pending cannot share a primary key
///   (engine inserts always use fresh ULID edge ids; loader Append
///   mode uses fresh ids too).
/// - `Some(col)` — committed rows whose `col` value also appears in any
///   pending batch are EXCLUDED from the committed count, so a Merge-mode
///   load that *updates* an existing edge (potentially changing its
///   `src`) counts the post-update row exactly once. Without this,
///   `LoadMode::Merge` double-counts.
pub(crate) async fn count_src_per_edge(
    db: &crate::db::Omnigraph,
    committed_ds: &SnapshotHandle,
    table_key: &str,
    staging: &MutationStaging,
    dedupe_key_column: Option<&str>,
) -> Result<HashMap<String, u32>> {
    let mut counts: HashMap<String, u32> = HashMap::new();

    let pending_batches = staging.pending_batches(table_key);

    // Collect pending key values (for shadow-on-merge dedupe). Only when
    // dedupe is requested AND there's anything pending.
    let pending_keys: Option<HashSet<String>> = match dedupe_key_column {
        Some(col) if !pending_batches.is_empty() => {
            let mut set = HashSet::new();
            for batch in pending_batches {
                if let Some(arr) = batch
                    .column_by_name(col)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            set.insert(arr.value(i).to_string());
                        }
                    }
                }
            }
            Some(set)
        }
        _ => None,
    };

    let replace_committed = staging.pending_mode(table_key) == Some(PendingMode::Overwrite);
    if !replace_committed {
        // Committed side: scan `src` plus the dedupe key column when set, so
        // we can both count and shadow in one pass.
        let projection: Vec<&str> = match dedupe_key_column {
            Some(col) if pending_keys.as_ref().is_some_and(|s| !s.is_empty()) => vec!["src", col],
            _ => vec!["src"],
        };
        let committed = db
            .storage()
            .scan(committed_ds, Some(&projection), None, None)
            .await?;
        for batch in &committed {
            let srcs = batch
                .column_by_name("src")
                .ok_or_else(|| OmniError::Lance("missing 'src' column on edge table".into()))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::Lance("'src' column is not Utf8".into()))?;
            // Optional shadow-key column (only present when dedupe is on).
            let key_arr = match (&pending_keys, dedupe_key_column) {
                (Some(set), Some(col)) if !set.is_empty() => batch
                    .column_by_name(col)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
                _ => None,
            };
            for i in 0..srcs.len() {
                if !srcs.is_valid(i) {
                    continue;
                }
                // Shadow this committed row if its key is in pending.
                if let (Some(arr), Some(set)) = (key_arr, pending_keys.as_ref()) {
                    if arr.is_valid(i) && set.contains(arr.value(i)) {
                        continue;
                    }
                }
                *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
            }
        }
    }

    // Pending side: walk in-memory batches for `src`. When dedupe is on,
    // collapse rows that share `dedupe_key_column` to their last occurrence
    // — mirrors `dedupe_merge_batches_by_id`'s last-write-wins applied at
    // finalize time, so cardinality counts what `commit_staged` will
    // actually publish, not raw input duplicates.
    //
    // Without this, a Merge-mode load whose input JSONL has two rows with
    // the same edge id would be double-counted here, even though the
    // finalize-time dedupe would collapse them to one. The result: spurious
    // `@card` violations on perfectly valid Merge inputs.
    match dedupe_key_column {
        Some(key_col) => count_pending_src_with_dedupe(pending_batches, key_col, &mut counts)?,
        None => count_pending_src_naive(pending_batches, &mut counts),
    }

    Ok(counts)
}

/// Count pending edges per `src` with NO dedup. Correct when caller
/// guarantees pending rows have unique primary keys (engine inserts via
/// fresh ULID; loader Append mode).
fn count_pending_src_naive(pending_batches: &[RecordBatch], counts: &mut HashMap<String, u32>) {
    for batch in pending_batches {
        let Some(col) = batch.column_by_name("src") else {
            continue;
        };
        let Some(srcs) = col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..srcs.len() {
            if srcs.is_valid(i) {
                *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
            }
        }
    }
}

/// Count pending edges per `src` after deduping rows that share
/// `dedupe_key_column`. Last occurrence wins (mirrors
/// `dedupe_merge_batches_by_id`'s walk-in-reverse contract). Required for
/// `LoadMode::Merge` where the same edge id may appear multiple times in
/// one load and finalize will collapse them to the last value.
fn count_pending_src_with_dedupe(
    pending_batches: &[RecordBatch],
    dedupe_key_column: &str,
    counts: &mut HashMap<String, u32>,
) -> Result<()> {
    // Walk in reverse, track seen keys, keep one (key, src) pair per key.
    let mut seen: HashSet<String> = HashSet::new();
    let mut kept_srcs: Vec<String> = Vec::new();
    for batch in pending_batches.iter().rev() {
        let Some(key_col) = batch.column_by_name(dedupe_key_column) else {
            // Pending batch is missing the key column. By construction
            // this is unreachable: callers in dedupe mode always push
            // batches whose schema contains the key (loader Merge mode
            // builds via build_edge_batch which always emits `id`; the
            // append_batch schema-compatibility check at the call site
            // would also reject a heterogeneous mix). If it ever fires
            // it's a programmer error — fail loudly rather than skip
            // counting (which would let `@card` violations slip).
            return Err(OmniError::manifest_internal(format!(
                "count_pending_src_with_dedupe: pending batch missing dedup key column '{}' \
                 (schema-compat check at append_batch should have rejected this)",
                dedupe_key_column
            )));
        };
        let key_arr = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "count_src_per_edge: pending '{}' column is not Utf8",
                    dedupe_key_column
                ))
            })?;
        let src_arr = batch
            .column_by_name("src")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let Some(srcs) = src_arr else {
            continue;
        };
        for i in (0..batch.num_rows()).rev() {
            if !srcs.is_valid(i) {
                continue;
            }
            // NULL key: keep (NULL != NULL semantics — every NULL counts).
            if !key_arr.is_valid(i) {
                kept_srcs.push(srcs.value(i).to_string());
                continue;
            }
            let key = key_arr.value(i);
            if seen.insert(key.to_string()) {
                kept_srcs.push(srcs.value(i).to_string());
            }
        }
    }
    for src in kept_srcs {
        *counts.entry(src).or_insert(0) += 1;
    }
    Ok(())
}

/// Apply `@card(min..max)` bounds to a per-source count map.
///
/// Both bounds are checked. The `min` check produces a misleading error
/// during a per-op insert mid-query (a bound of `2..` requires both
/// edges to be inserted before validation passes), but the historical
/// behavior was to enforce min per-op anyway — keeping users from
/// accidentally publishing a graph that violates the schema. Consumers
/// that need end-of-query semantics call this from after all edge ops
/// are accumulated (the loader does, via Phase 3).
pub(crate) fn enforce_cardinality_bounds(
    edge_type: &EdgeType,
    counts: &HashMap<String, u32>,
) -> Result<()> {
    let card = &edge_type.cardinality;
    for (src, count) in counts {
        if let Some(max) = card.max {
            if *count > max {
                return Err(OmniError::manifest(format!(
                    "@card violation on edge {}: source '{}' has {} edges (max {})",
                    edge_type.name, src, count, max
                )));
            }
        }
        if *count < card.min {
            return Err(OmniError::manifest(format!(
                "@card violation on edge {}: source '{}' has {} edges (min {})",
                edge_type.name, src, count, card.min
            )));
        }
    }
    Ok(())
}

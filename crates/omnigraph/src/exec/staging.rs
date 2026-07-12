//! Per-query staging accumulator for direct-publish writes.
//!
//! `MutationStaging` accumulates per-table input batches in memory during a
//! `mutate_as` or `load` query. At end-of-query it prepares one staged Lance
//! transaction per touched existing ref (or a deferred first-touch plan), then
//! joins the RFC-022 protocol: acquire ordered gates, revalidate the complete
//! read set, arm durable recovery, apply the physical effects, and return the
//! exact publisher inputs while retaining the guards through manifest CAS.
//!
//! Read-your-writes within the same query is satisfied by the in-memory
//! pending batches (see `pending_batches`) — read sites union the committed
//! Lance scan with the pending Arrow batches via DataFusion `MemTable` (see
//! `crate::table_store::TableStore::scan_with_pending`).
//!
//! This module is shared by the engine's mutation path (`exec/mutation.rs`)
//! and the bulk loader (`loader/mod.rs`); both feed insert/update batches
//! into `pending` and route end-of-query work through `stage_all` then
//! `commit_all`, followed by one exact manifest publish.
//! Deletes accumulate as predicates in `delete_predicates` (via
//! `record_delete`) and stage through the same `stage_* → commit_staged`
//! path as writes — `stage_delete` produces a deletion-vector transaction
//! that advances no Lance HEAD until the end-of-query commit. The parse-time
//! D₂ rule keeps inserts/updates and deletes from mixing in one query, so
//! `pending` and `delete_predicates` never overlap on a table.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::storage_layer::{SnapshotHandle, StagedHandle};
use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use futures::stream::StreamExt;

use crate::db::manifest::{
    RecoveryAuthorityToken, RecoveryLineageIntent, RecoverySidecarHandle, SidecarKind,
    SidecarTablePin, confirm_occ_sidecar_phase_b, new_occ_sidecar, write_sidecar,
};
use crate::db::{MutationOpKind, SubTableUpdate};
use crate::error::{OmniError, Result};

/// Whether the per-table accumulator should commit via `stage_append`,
/// `stage_merge_insert`, or `stage_overwrite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingMode {
    Append,
    Merge,
    /// Field-level update: a Merge whose staged source is the batch
    /// schema MINUS the table's `partial_validation_only` columns, staged
    /// matched-only (`WhenNotMatched::DoNothing`). A distinct variant — not a
    /// side-flag on `Merge` — so every `match` on the mode is forced to
    /// disposition partial batches explicitly: treating one as a full-row
    /// Merge (a future coalescing change, a retry path) would null-overwrite
    /// unassigned columns, and the compiler now surfaces that decision.
    PartialUpdate,
    Overwrite,
}

/// Work that must be staged only after a first-touch target ref exists. Lance
/// writes uncommitted fragment/deletion files under the opened branch's tree;
/// staging against the inherited source and later committing on the target
/// would publish paths that do not exist in the target tree.
enum DeferredStagePlan {
    Pending {
        mode: PendingMode,
        batch: RecordBatch,
    },
    Delete {
        predicate: String,
    },
}

/// Per-table accumulator. Each insert/update op pushes a `RecordBatch` into
/// `batches`; at end-of-query the accumulated batches concat into a single
/// stage call.
#[derive(Debug)]
pub(crate) struct PendingTable {
    pub(crate) schema: SchemaRef,
    pub(crate) mode: PendingMode,
    pub(crate) batches: Vec<RecordBatch>,
    /// Field-level update: `Some(validation_only)` marks the
    /// accumulated batches as PARTIAL — they carry (merge key + assigned +
    /// constraint-completion) columns for the validation change-set, and the
    /// STAGED merge source is the batch schema MINUS these validation-only
    /// completion columns, staged matched-only (`WhenNotMatched::DoNothing`).
    /// Deriving the staged set by subtraction (rather than listing it) keeps
    /// it consistent with `apply_assignments`' own omissions — e.g. a Blob
    /// assigned a non-String value is omitted from the batch entirely.
    /// Completion columns are validation inputs, never merge inputs: Lance
    /// counts every source column as modified, so staging an unassigned
    /// `@unique`-group member would patch it and prune its index for no
    /// semantic reason. Set only by
    /// [`MutationStaging::append_partial_update_batch`], whose eligibility
    /// rule (the table's ONLY op in this query is a single `update`)
    /// guarantees no full-schema batch ever lands on the same table.
    pub(crate) partial_validation_only: Option<Vec<String>>,
}

impl PendingTable {
    fn new(schema: SchemaRef, mode: PendingMode) -> Self {
        Self {
            schema,
            mode,
            batches: Vec::new(),
            partial_validation_only: None,
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
    /// Final Lance ref recorded in the manifest update.
    pub(crate) table_branch: Option<String>,
    /// First-touch named-branch fork deferred until the v3 recovery sidecar is
    /// durable. Preparation reads the inherited `source_entry`; after arming,
    /// commit creates `target_branch` and stages branch-local files there.
    pub(crate) deferred_fork: Option<crate::db::DeferredTableFork>,
}

/// Per-query staging state.
///
/// Replaces the legacy per-statement inline-commit map with an in-memory
/// accumulator that defers every Lance HEAD advance to the query commit
/// boundary. Inserts, updates, and deletes share one effect set. Durable
/// recovery intent owns the remaining effect-to-manifest-CAS gap; statement
/// failures cannot create uncovered HEAD drift.
#[derive(Default)]
pub(crate) struct MutationStaging {
    /// Pre-write manifest version per table — the publisher's CAS fence at
    /// end-of-query.
    pub(crate) expected_versions: HashMap<String, u64>,
    /// Per-table identifiers captured on first touch.
    pub(crate) paths: HashMap<String, StagedTablePath>,
    /// In-memory accumulated batches per table (insert/update path).
    pub(crate) pending: HashMap<String, PendingTable>,
    /// Per-table delete predicates from delete-touching ops. D₂ guarantees a
    /// table is write-XOR-delete within one query, so this never overlaps
    /// `pending`. Staged as one combined `stage_delete` per table at
    /// end-of-query (no inline HEAD advance) — see `stage_delete_table`.
    pub(crate) delete_predicates: HashMap<String, Vec<String>>,
    /// Ids removed per table, captured by the delete ops as they scan their
    /// matched rows (so validation recounts the srcs a delete empties without
    /// re-resolving the predicates). Disjoint from `pending` by D₂; flows into
    /// the validation [`ChangeSet`](crate::validate::ChangeSet) via
    /// [`to_changeset`](Self::to_changeset). The combined `stage_delete` at
    /// commit still removes by predicate — these ids are validation-only.
    pub(crate) deleted_ids: HashMap<String, Vec<String>>,
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
        deferred_fork: Option<crate::db::DeferredTableFork>,
        expected_version: u64,
        op_kind: MutationOpKind,
    ) {
        self.paths
            .entry(table_key.to_string())
            .or_insert(StagedTablePath {
                full_path,
                table_branch,
                deferred_fork,
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
            if existing.mode == PendingMode::PartialUpdate {
                return Err(OmniError::manifest_internal(format!(
                    "append_batch: table '{}' holds a partial-update batch — the \
                     eligibility rule should have prevented a second op on this table",
                    table_key
                )));
            }
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

    /// Field-level update: stage a PARTIAL-schema update batch — (key + assigned +
    /// completion) columns — for a table whose only op in this query is one
    /// `update`. The caller (`execute_update`) enforces that eligibility rule
    /// from the lowered IR, so an existing accumulator entry here is an
    /// internal invariant breach, not a user error: partial and full batches
    /// cannot share one uniform-schema merge source, and silently widening
    /// (or narrowing) would corrupt the staged shape.
    pub(crate) fn append_partial_update_batch(
        &mut self,
        table_key: &str,
        schema: SchemaRef,
        batch: RecordBatch,
        validation_only: Vec<String>,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.pending.contains_key(table_key) {
            return Err(OmniError::manifest_internal(format!(
                "append_partial_update_batch: table '{}' already has pending batches — \
                 partial-update staging requires the update to be the table's only op \
                 (the eligibility rule in execute_named_mutation)",
                table_key
            )));
        }
        let mut entry = PendingTable::new(schema, PendingMode::PartialUpdate);
        entry.partial_validation_only = Some(validation_only);
        entry.batches.push(batch);
        self.pending.insert(table_key.to_string(), entry);
        Ok(())
    }

    /// Record a delete predicate for `table_key`. The caller must have already
    /// called `ensure_path` (via `open_table_for_mutation`) so the table's
    /// path/version/op-kind are captured. D₂ guarantees a delete-touched table
    /// has no pending write batches, so the predicates are staged as one
    /// combined `stage_delete` at end-of-query — no inline HEAD advance.
    pub(crate) fn record_delete(&mut self, table_key: &str, predicate: String) {
        self.delete_predicates
            .entry(table_key.to_string())
            .or_default()
            .push(predicate);
    }

    /// Record ids removed by a delete op on `table_key`, captured from the op's
    /// own scan, for validation (so cardinality recounts an emptied src). The
    /// caller scans with a dedup filter that excludes prior-scheduled matches, so
    /// no id is recorded twice across statements.
    pub(crate) fn record_deleted_ids(&mut self, table_key: &str, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        self.deleted_ids
            .entry(table_key.to_string())
            .or_default()
            .extend(ids.iter().cloned());
    }

    /// Delete predicates already recorded for `table_key` by earlier delete
    /// statements in this query. Read before recording the current statement's
    /// predicate so its `affected_*` count can exclude rows a prior statement
    /// already scheduled for deletion (deletes stage, so the committed snapshot
    /// is unchanged across statements — without this, overlapping predicates
    /// would double-count). `&[]` if none.
    pub(crate) fn recorded_delete_predicates(&self, table_key: &str) -> &[String] {
        self.delete_predicates
            .get(table_key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Read-your-writes accessor: the accumulated pending batches for
    /// `table_key`, or `&[]` if none.
    pub(crate) fn pending_batches(&self, table_key: &str) -> &[RecordBatch] {
        self.pending
            .get(table_key)
            .map(|p| p.batches.as_slice())
            .unwrap_or(&[])
    }

    /// Build the validation [`ChangeSet`](crate::validate::ChangeSet) for this
    /// staging: every touched table's accumulated rows as the `changed` delta
    /// (record-batch clone is Arc-cheap — no data copy). Shared by the mutation
    /// and loader write paths so their validation input cannot drift.
    pub(crate) fn to_changeset(&self) -> crate::validate::ChangeSet {
        let mut changeset = crate::validate::ChangeSet::new();
        for table_key in self.pending.keys() {
            let batches = self.pending_batches(table_key);
            if batches.is_empty() {
                continue;
            }
            let mut change = crate::validate::TableChange::default();
            change.changed.extend(batches.iter().cloned());
            changeset.insert(table_key.clone(), change);
        }
        // Deletes (disjoint from `pending` by D₂) carry their removed ids so the
        // evaluator recounts the srcs a delete empties (`@card`) and sees removed
        // rows for RI — the faithful change-set the merge path also builds.
        for (table_key, ids) in &self.deleted_ids {
            if ids.is_empty() {
                continue;
            }
            changeset.entry(table_key.clone()).or_default().deleted_ids = ids.clone();
        }
        changeset
    }

    /// Schema of the accumulated batches for `table_key`, or `None` if no
    /// op has touched the table. Used by `scan_with_pending` to construct
    /// the in-memory `MemTable`.
    pub(crate) fn pending_schema(&self, table_key: &str) -> Option<SchemaRef> {
        self.pending.get(table_key).map(|p| p.schema.clone())
    }

    /// `true` if neither pending writes nor delete predicates have any state —
    /// the query made no observable writes.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.delete_predicates.is_empty()
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
            delete_predicates,
            // Validation-only; consumed before staging, nothing to commit here.
            deleted_ids: _,
            op_kinds: _,
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
        let mut staged_entries: Vec<StagedTableEntry> =
            futures::stream::iter(stage_inputs.into_iter().map(
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

        // Second pass: stage deletes through the same staged path. D₂
        // guarantees a delete-touched table carries no pending write batches,
        // so `delete_predicates` and `pending` are disjoint — each is a fresh
        // `StagedTableEntry`, never a merge into a write entry above. Multiple
        // predicates on one table (a cascade hitting an edge table twice, or
        // two delete statements) combine into a single `(p₁) OR (p₂) …` staged
        // delete, so the table advances Lance HEAD exactly once at commit. A
        // predicate matching zero committed rows yields `None` and is skipped
        // (the staged equivalent of the old "skip record_inline on 0 rows" —
        // no inline HEAD advance, closing the zero-row drift class).
        for (table_key, predicates) in delete_predicates {
            let path = paths.get(&table_key).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::stage_all: missing path for delete table '{}'",
                    table_key
                ))
            })?;
            let expected = *expected_versions.get(&table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::stage_all: missing expected version for delete table '{}'",
                    table_key
                ))
            })?;
            let combined = if predicates.len() == 1 {
                predicates.into_iter().next().unwrap()
            } else {
                predicates
                    .iter()
                    .map(|p| format!("({})", p))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            };
            if let Some(entry) = stage_delete_table(db, table_key, combined, path, expected).await?
            {
                staged_entries.push(entry);
            }
        }

        Ok(StagedMutation {
            staged: staged_entries,
            expected_versions,
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
    // Reopen the pinned dataset. Existing-table effects stage on this handle
    // now. A deferred first-touch effect uses it only as the inherited source
    // pin; its files stage on the target handle after sidecar + fork.
    let stage_kind = match table.mode {
        PendingMode::Append => crate::db::MutationOpKind::Insert,
        // PartialUpdate reopens with the same non-strict policy as Merge;
        // strictness for the commit-time drift check comes from `op_kinds`
        // (the update op registered MutationOpKind::Update there).
        PendingMode::Merge | PendingMode::PartialUpdate => crate::db::MutationOpKind::Merge,
        PendingMode::Overwrite => crate::db::MutationOpKind::SchemaRewrite,
    };
    let ds = match path.deferred_fork.as_ref() {
        Some(fork) => {
            db.storage()
                .open_snapshot_at_entry(&fork.source_entry)
                .await?
        }
        None => {
            db.reopen_for_mutation(
                &table_key,
                &path.full_path,
                path.table_branch.as_deref(),
                expected,
                stage_kind,
            )
            .await?
        }
    };

    if table.batches.is_empty() {
        return Ok(None);
    }

    let combined = match table.mode {
        PendingMode::Merge | PendingMode::PartialUpdate => {
            dedupe_merge_batches_by_id(&table.schema, table.batches)?
        }
        PendingMode::Append | PendingMode::Overwrite => {
            if table.batches.len() == 1 {
                table.batches.into_iter().next().unwrap()
            } else {
                arrow_select::concat::concat_batches(&table.schema, &table.batches)
                    .map_err(|e| OmniError::Lance(e.to_string()))?
            }
        }
    };
    if path.deferred_fork.is_some() {
        // The recovery identity is minted before the fork; the actual Lance
        // transaction is staged on the new target ref after the sidecar is
        // durable, then bound to this UUID. Its read version remains Lance's
        // independently-derived value and is checked by the binder.
        // Field-level update: project the staged set here so the deferred
        // stage receives exactly what a non-deferred stage would.
        let combined = match (&table.mode, &table.partial_validation_only) {
            (PendingMode::PartialUpdate, Some(validation_only)) => {
                project_out_validation_only(combined, validation_only)?
            }
            (PendingMode::PartialUpdate, None) => {
                return Err(OmniError::manifest_internal(
                    "PartialUpdate entry without validation-only columns".to_string(),
                ));
            }
            _ => combined,
        };
        let planned_transaction = pre_minted_transaction_identity(expected);
        return Ok(Some(StagedTableEntry {
            table_key,
            path,
            expected_version: expected,
            dataset: ds,
            staged_write: None,
            deferred_stage: Some(DeferredStagePlan::Pending {
                mode: table.mode,
                batch: combined,
            }),
            planned_transaction,
        }));
    }

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
        PendingMode::PartialUpdate => {
            // Field-level update: staged source = batch schema − validation-only columns
            // (completion columns ride the validation change-set only; staging
            // them would patch unassigned columns and prune their indexes). A
            // subtraction cannot reference a column the batch lacks, so
            // apply-time omissions (unassigned blobs) are immune. Matched-only:
            // Lance patches the provided columns in place and never inserts —
            // a subset schema could not satisfy non-null target columns anyway,
            // and the update executor only stages rows it matched.
            let validation_only =
                table.partial_validation_only.as_deref().ok_or_else(|| {
                    OmniError::manifest_internal(
                        "PartialUpdate entry without validation-only columns".to_string(),
                    )
                })?;
            let projected = project_out_validation_only(combined, validation_only)?;
            db.storage()
                .stage_merge_insert(
                    ds.clone(),
                    projected,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::DoNothing,
                )
                .await?
        }
        PendingMode::Overwrite => db.storage().stage_overwrite(&ds, combined).await?,
    };
    let planned_transaction = staged.transaction_identity();
    Ok(Some(StagedTableEntry {
        table_key,
        path,
        expected_version: expected,
        dataset: ds,
        staged_write: Some(staged),
        deferred_stage: None,
        planned_transaction,
    }))
}

/// Stage a delete on `table_key` from a combined predicate, mirroring
/// [`stage_pending_table`] for the delete path. Reopens the dataset at the
/// pinned `expected` version (strict Delete op) and stages a deletion-vector
/// transaction via `TableStorage::stage_delete` — Phase A writes the deletion
/// file but advances no Lance HEAD until `commit_all` runs `commit_staged`.
/// Returns `None` when the predicate matches zero committed rows, so a no-op
/// delete stages nothing and never moves HEAD (the zero-row drift fix carried
/// onto the staged path).
async fn stage_delete_table(
    db: &crate::db::Omnigraph,
    table_key: String,
    predicate: String,
    path: StagedTablePath,
    expected: u64,
) -> Result<Option<StagedTableEntry>> {
    let ds = match path.deferred_fork.as_ref() {
        Some(fork) => {
            db.storage()
                .open_snapshot_at_entry(&fork.source_entry)
                .await?
        }
        None => {
            db.reopen_for_mutation(
                &table_key,
                &path.full_path,
                path.table_branch.as_deref(),
                expected,
                crate::db::MutationOpKind::Delete,
            )
            .await?
        }
    };
    if path.deferred_fork.is_some() {
        // Probe only. The actual deletion vector must be written under the
        // target branch tree after the durable fork intent creates that ref.
        if db
            .storage()
            .first_row_id_for_filter(&ds, &predicate)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        return Ok(Some(StagedTableEntry {
            table_key,
            path,
            expected_version: expected,
            dataset: ds,
            staged_write: None,
            deferred_stage: Some(DeferredStagePlan::Delete { predicate }),
            planned_transaction: pre_minted_transaction_identity(expected),
        }));
    }
    match db.storage().stage_delete(&ds, &predicate).await? {
        Some(staged) => Ok(Some(StagedTableEntry {
            table_key,
            path,
            expected_version: expected,
            dataset: ds,
            planned_transaction: staged.transaction_identity(),
            staged_write: Some(staged),
            deferred_stage: None,
        })),
        None => Ok(None),
    }
}

/// Output of [`MutationStaging::stage_all`]. Carries ready Lance transactions
/// for existing refs and complete deferred stage plans for first-touch named
/// refs, plus the metadata needed to arm recovery, apply Stage-F effects, and
/// produce the publisher's input.
///
/// Splitting `stage_all` and `commit_all` keeps reclaimable preparation outside
/// writer gates while the latter owns ordered acquisition, revalidation,
/// recovery arming, and effects.
pub(crate) struct StagedMutation {
    /// One entry per table that had pending write batches or delete
    /// predicates prepared (and, for an existing ref, successfully staged).
    /// Deletes flow through this same vector as inserts/updates/overwrites —
    /// there is no separate inline-commit path.
    staged: Vec<StagedTableEntry>,
    /// Pre-write manifest version per table — the publisher's CAS fence.
    expected_versions: HashMap<String, u64>,
}

/// Per-table state captured during `stage_all` and consumed by
/// `commit_all`. Holds the opened snapshot plus either a staged Lance
/// transaction or a deferred first-touch plan. Storage handles remain opaque
/// per MR-793 §III.9 — the inner `lance::Dataset` / `StagedWrite` are not
/// visible to engine code outside the storage layer.
struct StagedTableEntry {
    table_key: String,
    path: StagedTablePath,
    expected_version: u64,
    dataset: SnapshotHandle,
    staged_write: Option<StagedHandle>,
    deferred_stage: Option<DeferredStagePlan>,
    planned_transaction: crate::table_store::StagedTransactionIdentity,
}

fn pre_minted_transaction_identity(
    read_version: u64,
) -> crate::table_store::StagedTransactionIdentity {
    crate::table_store::StagedTransactionIdentity {
        read_version,
        // Lance treats the UUID as an opaque transaction identity/path
        // component. A ULID is equally unique and filesystem-safe while
        // avoiding a second UUID generator in the engine surface.
        uuid: format!("omnigraph-{}", ulid::Ulid::new()),
    }
}

/// Field-level update: staged source = batch schema − validation-only
/// completion columns (they ride the validation change-set only; staging them
/// would patch unassigned columns and prune their indexes). Subtraction from
/// the batch's actual schema keeps the projection immune to apply-time
/// omissions (e.g. a Blob assigned a non-String value is absent entirely).
fn project_out_validation_only(
    batch: RecordBatch,
    validation_only: &[String],
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !validation_only.contains(f.name()))
        .map(|(i, _)| i)
        .collect();
    batch
        .project(&indices)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

async fn stage_deferred_plan(
    db: &crate::db::Omnigraph,
    target: SnapshotHandle,
    plan: DeferredStagePlan,
    planned: &crate::table_store::StagedTransactionIdentity,
) -> Result<StagedHandle> {
    let mut staged = match plan {
        DeferredStagePlan::Pending { mode, batch } => match mode {
            PendingMode::Append => db.storage().stage_append(&target, batch, &[]).await?,
            PendingMode::Merge => {
                db.storage()
                    .stage_merge_insert(
                        target,
                        batch,
                        vec!["id".to_string()],
                        lance::dataset::WhenMatched::UpdateAll,
                        lance::dataset::WhenNotMatched::InsertAll,
                    )
                    .await?
            }
            // Field-level update: the batch was projected down to the staged
            // set (key + assigned) at pack time; matched-only so unmatched
            // partial rows can never insert.
            PendingMode::PartialUpdate => {
                db.storage()
                    .stage_merge_insert(
                        target,
                        batch,
                        vec!["id".to_string()],
                        lance::dataset::WhenMatched::UpdateAll,
                        lance::dataset::WhenNotMatched::DoNothing,
                    )
                    .await?
            }
            PendingMode::Overwrite => db.storage().stage_overwrite(&target, batch).await?,
        },
        DeferredStagePlan::Delete { predicate } => db
            .storage()
            .stage_delete(&target, &predicate)
            .await?
            .ok_or_else(|| {
                OmniError::manifest_read_set_changed(
                    "deferred_delete_match",
                    Some("matching row at inherited pin".to_string()),
                    Some("no matching row on exact target fork".to_string()),
                )
            })?,
    };
    staged.bind_transaction_identity(planned)?;
    Ok(staged)
}

/// Output of [`StagedMutation::commit_all`] after Stage F: the publisher's input
/// plus the queue guards the caller must hold through Stage G manifest publish.
pub(crate) struct CommittedMutation {
    /// Per-table updates to publish to the manifest.
    pub(crate) updates: Vec<SubTableUpdate>,
    /// Per-table physical pins captured in the immutable `WriteTxn`; the
    /// publisher checks them together with native branch identity, exact graph
    /// head, and schema identity as one authority precondition.
    pub(crate) expected_versions: HashMap<String, u64>,
    /// Recovery sidecar to delete during Stage H after manifest CAS succeeds
    /// (`None` when nothing staged).
    pub(crate) sidecar_handle: Option<RecoverySidecarHandle>,
    /// Root schema, coarse branch, and sorted `(table, branch)` guards. The
    /// caller MUST hold the complete set across manifest publish (see
    /// `commit_all`) so no same-process writer interleaves after revalidation.
    pub(crate) guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

impl StagedMutation {
    /// RFC-022 Stages C–F: acquire ordered schema/branch/table gates, revalidate
    /// the complete read set, arm the recovery sidecar, run `commit_staged` per
    /// table to advance Lance HEAD, and return the publisher input plus guards.
    ///
    /// **Caller must hold the returned `_guards` Vec across the
    /// subsequent manifest publish.** Releasing guards before publish
    /// would let another writer interleave its physical effects between ours
    /// and publish. The exact publisher precondition would still reject stale
    /// authority, but our already-advanced Lance HEAD would then require the
    /// durable v3 sidecar to recover. Holding the guards closes that avoidable
    /// same-process window; the sidecar remains the crash/foreign-process
    /// correctness authority.
    ///
    /// Revalidation: between `stage_all` and `commit_all`, another writer may
    /// change any member of the attempt's branch-wide read authority, including
    /// a table this attempt does not write. Under the ordered gates we first
    /// reject any newly armed overlapping sidecar, then compare the complete
    /// immutable token. A safe insert/Append/Merge caller discards and fully
    /// reprepares; strict Update/Delete/Overwrite returns `ReadSetChanged`.
    /// Both outcomes occur before `commit_staged`, so staged transaction files
    /// remain unreferenced and reclaimable.
    /// First-touch named-branch forks are also deferred to this phase. The
    /// method acquires schema → branch → table gates, revalidates the complete
    /// read token, arms v3 recovery, and only then creates any Lance refs.
    pub(crate) async fn commit_all(
        self,
        db: &crate::db::Omnigraph,
        branch: Option<&str>,
        sidecar_kind: SidecarKind,
        actor_id: Option<&str>,
        txn: &crate::db::WriteTxn,
        lineage_intent: &crate::db::manifest::LineageIntent,
    ) -> Result<CommittedMutation> {
        let StagedMutation {
            mut staged,
            expected_versions,
        } = self;

        // Per-(table_key, branch) queues for every touched table. Sorted by
        // `acquire_many` internally so all multi-table writers (mutation,
        // branch_merge, schema_apply, the fork path, recovery) agree on
        // acquisition order — prevents lock-order inversion deadlock. Deletes
        // are staged like every other write, so holding the queue from before
        // `commit_staged` through the publish keeps no Lance HEAD ahead of the
        // manifest on the happy path.
        let mut queue_keys: Vec<(String, Option<String>)> = Vec::with_capacity(staged.len());
        for entry in &staged {
            queue_keys.push((entry.table_key.clone(), entry.path.table_branch.clone()));
        }
        // Total order shared with schema apply: schema gate, branch gate, then
        // sorted per-table gates. Hold the full set through manifest publish.
        let schema_guard = db
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let branch_guard = db.write_queue().acquire_branch(branch).await;
        let mut guards = vec![schema_guard, branch_guard];
        guards.extend(db.write_queue().acquire_many(&queue_keys).await);

        // Stage A ran the roll-forward-only healer before preparation, while
        // reclaimable transaction files were staged outside these gates. A
        // different writer can therefore arm a recovery intent in that gap.
        // Re-list after acquiring schema -> branch -> table gates and reject
        // that intent before revalidation or any new sidecar/ref/table effect.
        // Do NOT invoke the healer here: recovery acquires this same ordered
        // gate set and would deadlock/re-enter the write protocol.
        //
        // OCC is branch-wide, so every unresolved sidecar for this normalized
        // graph branch is relevant even when it names a disjoint table. Schema
        // apply changes the catalog contract and conservatively blocks every
        // enrolled data branch. `list_sidecars` is URI-sorted; choosing the
        // first relevant operation makes conflict attribution deterministic.
        let enrolled_branch = branch.filter(|name| *name != "main");
        let pending_sidecars =
            crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter()).await?;
        if let Some(owner) = pending_sidecars.iter().find(|sidecar| {
            let sidecar_branch = sidecar.branch.as_deref().filter(|name| *name != "main");
            sidecar.writer_kind == SidecarKind::SchemaApply || sidecar_branch == enrolled_branch
        }) {
            return Err(OmniError::recovery_required(
                owner.operation_id.clone(),
                format!(
                    "pending {:?} recovery operation blocks writes on branch '{}'",
                    owner.writer_kind,
                    enrolled_branch.unwrap_or("main"),
                ),
            ));
        }

        // Re-capture manifest pins under the queue (PR 2 / MR-686).
        //
        // expected_versions was captured when the mutation first opened
        // each table for mutation (the query's read-time pin). For
        // Any writer may advance the branch while we stage reclaimable files
        // outside the gate. RFC-022 does not patch those prepared table pins or
        // transparently rebase a validated transaction: every mismatch rejects
        // the complete attempt before effects. Mutation inserts and load
        // append/merge may then reprepare at their outer bounded retry loop;
        // strict read-modify-write operations return the typed conflict.
        //
        // Revalidate the complete coarse authority under the branch/table
        // gates. The helper probe-reuses a current warm coordinator and opens
        // the target branch fresh on any mismatch, returning the snapshot from
        // that same authority view. No prepared table pin is patched forward.
        let snapshot = db.revalidate_write_txn(txn).await?;
        for entry in &staged {
            let current = snapshot
                .entry(&entry.table_key)
                .map(|e| e.table_version)
                .ok_or_else(|| {
                    OmniError::manifest_conflict(format!(
                        "table '{}' missing from manifest at commit time",
                        entry.table_key,
                    ))
                })?;

            // RFC-022-enrolled mutation/load attempts never patch a prepared
            // table expectation to a newer manifest pin. The complete branch
            // token was just revalidated; any remaining table mismatch is a
            // stale/internally-inconsistent read set and must restart the
            // whole validation attempt before effects.
            if entry.expected_version != current {
                return Err(OmniError::manifest_read_set_changed(
                    format!("table_head:{}", entry.table_key),
                    Some(entry.expected_version.to_string()),
                    Some(current.to_string()),
                ));
            }

            // A deferred fork is intentionally staged from the exact inherited
            // source entry. The source ref (often main) may advance after the
            // graph branch was cut; that is unrelated to this branch's pinned
            // snapshot. The target ref does not exist yet, so there is no live
            // target HEAD to compare until after the recovery intent is armed.
            if entry.path.deferred_fork.is_some() {
                if entry.dataset.version() != current {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("table_head:{}", entry.table_key),
                        Some(current.to_string()),
                        Some(entry.dataset.version().to_string()),
                    ));
                }
                continue;
            }

            // Separate manifest-visible concurrency from uncovered Lance drift.
            // The shared pre-arm check probes this already-open staged handle,
            // attributes covered drift to its exact recovery operation, and
            // routes uncovered drift through explicit operator repair. Keeping
            // the rule here and in control/maintenance adapters behind one
            // helper prevents a future writer from weakening ownership by
            // emitting its sidecar before checking the physical baseline.
            db.ensure_existing_effect_baseline(
                &entry.table_key,
                entry.path.table_branch.as_deref(),
                current,
                &entry.dataset,
            )
            .await?;
        }
        // An empty load/mutation has no independently durable table effect.
        // It may still publish its fixed lineage intent, but arming recovery
        // would manufacture an effect plan where none exists.
        if staged.is_empty() {
            return Ok(CommittedMutation {
                updates: Vec::new(),
                expected_versions,
                sidecar_handle: None,
                guards,
            });
        }

        // Sidecar protocol: build the per-table pin list and write the
        // sidecar BEFORE any `commit_staged` advances Lance HEAD, so any
        // commit→publish residual is recoverable on the next open. Deletes
        // are staged like every other write, so each delete table is a normal
        // `staged` entry here — one pin at `expected + 1` (a single staged
        // commit advances exactly one version), no inline special-casing.
        let mut pins: Vec<SidecarTablePin> = Vec::with_capacity(staged.len());
        let mut planned_transactions = HashMap::with_capacity(staged.len());
        for entry in &staged {
            planned_transactions.insert(entry.table_key.clone(), entry.planned_transaction.clone());
            pins.push(SidecarTablePin {
                table_key: entry.table_key.clone(),
                table_path: entry.path.full_path.clone(),
                expected_version: entry.expected_version,
                post_commit_pin: entry.expected_version + 1,
                confirmed_version: None,
                table_branch: entry.path.table_branch.clone(),
            });
        }

        let authority = RecoveryAuthorityToken {
            branch_identifier: txn.authority.branch_identifier.clone(),
            graph_head: txn.authority.graph_head.clone(),
            schema_ir_hash: txn.authority.schema_ir_hash.clone(),
            schema_identity_version: txn.authority.schema_identity_version,
        };
        let recovery_lineage = RecoveryLineageIntent {
            graph_commit_id: lineage_intent.graph_commit_id.clone(),
            branch: lineage_intent.branch.clone(),
            actor_id: lineage_intent.actor_id.clone(),
            merged_parent_commit_id: lineage_intent.merged_parent_commit_id.clone(),
            created_at: lineage_intent.created_at,
        };
        let mut sidecar = new_occ_sidecar(
            sidecar_kind,
            branch.map(str::to_string),
            actor_id.map(str::to_string),
            pins,
            authority,
            recovery_lineage,
            planned_transactions,
        )?;
        // Deterministic pre-effect race point: authority has been validated and
        // the intent is fully prepared in memory, but neither the durable
        // sidecar nor a target fork exists yet. A concurrent winner may publish;
        // this attempt then discards/reprepares on the collision below.
        crate::failpoints::maybe_fail(crate::failpoints::names::FORK_BEFORE_CLASSIFY)?;
        let sidecar_handle =
            Some(write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar).await?);
        let operation_id = sidecar.operation_id.clone();
        if staged
            .iter()
            .any(|entry| entry.path.deferred_fork.is_some())
        {
            crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_SIDECAR_PRE_FORK)
                .map_err(|error| {
                    OmniError::recovery_required(operation_id.clone(), error.to_string())
                })?;
        }

        // The v3 intent is now durable. Only now may a first-touch named-table
        // fork become visible in Lance. Its transaction is then staged on the
        // fresh target handle (so Lance writes branch-local file paths) and
        // bound to the pre-minted UUID already recorded in the sidecar.
        let mut created_any_fork = false;
        for entry in &mut staged {
            let Some(fork) = entry.path.deferred_fork.clone() else {
                continue;
            };
            match db
                .fork_dataset_from_entry_state_under_intent(
                    &entry.table_key,
                    &entry.path.full_path,
                    fork.source_entry.table_branch.as_deref(),
                    fork.source_entry.table_version,
                    &fork.target_branch,
                    Some(&operation_id),
                )
                .await
            {
                Ok(target) => {
                    entry.dataset = target;
                    created_any_fork = true;
                    let plan = entry.deferred_stage.take().ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "deferred fork for '{}' has no deferred stage plan",
                            entry.table_key
                        ))
                    })?;
                    let staged_write = stage_deferred_plan(
                        db,
                        entry.dataset.clone(),
                        plan,
                        &entry.planned_transaction,
                    )
                    .await
                    .map_err(|error| {
                        OmniError::recovery_required(
                            operation_id.clone(),
                            format!(
                                "staging on deferred table fork '{}' failed after recovery \
                                 intent was armed: {error}",
                                entry.table_key
                            ),
                        )
                    })?;
                    entry.staged_write = Some(staged_write);
                }
                Err(error) => {
                    // Once the intent is durable, a fork error is not proof that
                    // no physical effect occurred. Lance may have created the ref
                    // successfully and then failed while reopening/verifying it;
                    // reclaim-and-refork can likewise fail after deleting or
                    // recreating a ref. Keep the sidecar for exact Full recovery
                    // even when this is the first table. A future typed storage
                    // outcome may recover the pre-effect retry optimization only
                    // when it can *prove* the ref was never changed.
                    return Err(OmniError::recovery_required(
                        operation_id,
                        format!(
                            "deferred table fork failed after recovery intent was armed: {error}"
                        ),
                    ));
                }
            }
        }
        if created_any_fork {
            crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_FORK_PRE_COMMIT)
                .map_err(|error| {
                    OmniError::recovery_required(operation_id.clone(), error.to_string())
                })?;
        }

        let mut updates: Vec<SubTableUpdate> = Vec::with_capacity(staged.len());
        let mut committed_transactions = HashMap::with_capacity(staged.len());

        for entry in staged {
            let StagedTableEntry {
                table_key,
                path,
                expected_version: _,
                dataset,
                staged_write,
                deferred_stage: _,
                planned_transaction: _,
            } = entry;

            let staged_write = staged_write.ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "table '{}' reached commit without a staged transaction",
                    table_key
                ))
            })?;

            let outcome = db
                .storage()
                .commit_staged_exact(dataset, staged_write)
                .await
                .map_err(|error| {
                    OmniError::recovery_required(operation_id.clone(), error.to_string())
                })?;
            if !outcome.is_exact() {
                return Err(OmniError::recovery_required(
                    operation_id,
                    format!(
                        "table '{}' committed a rebased Lance transaction at version {} (planned {:?}, committed {:?})",
                        table_key,
                        outcome.committed_version(),
                        outcome.planned_transaction(),
                        outcome.committed_transaction(),
                    ),
                ));
            }
            committed_transactions
                .insert(table_key.clone(), outcome.committed_transaction().clone());
            let new_ds = outcome.into_snapshot();
            let state = db
                .storage()
                .table_state(&path.full_path, &new_ds)
                .await
                .map_err(|error| {
                    OmniError::recovery_required(operation_id.clone(), error.to_string())
                })?;
            updates.push(SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: path.table_branch.clone(),
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
            crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_TABLE_COMMIT)
                .map_err(|error| {
                    OmniError::recovery_required(operation_id.clone(), error.to_string())
                })?;
        }

        if let Err(error) = confirm_occ_sidecar_phase_b(
            db.root_uri(),
            db.storage_adapter(),
            &mut sidecar,
            &updates,
            &committed_transactions,
        )
        .await
        {
            return Err(OmniError::recovery_required(
                operation_id,
                error.to_string(),
            ));
        }

        Ok(CommittedMutation {
            updates,
            expected_versions,
            sidecar_handle,
            guards,
        })
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

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

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use lance::Dataset;
use omnigraph_compiler::catalog::EdgeType;

use crate::db::{MutationOpKind, SubTableUpdate};
use crate::db::manifest::{
    new_sidecar, write_sidecar, RecoverySidecarHandle, SidecarKind, SidecarTablePin,
};
use crate::error::{OmniError, Result};

/// Whether the per-table accumulator should commit via `stage_append`
/// (no @key inserts, edge inserts) or `stage_merge_insert` (any @key insert
/// or update). Once set to `Merge` for a table within a query, subsequent
/// inserts on that table are rolled into the same merge — a `WhenNotMatched
/// = InsertAll` merge is correct for both cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingMode {
    Append,
    Merge,
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
        self.paths.entry(table_key.to_string()).or_insert(StagedTablePath {
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
        if batch.num_rows() == 0 {
            // No-op — staging is purely additive; an empty batch should not
            // be appended.
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
        // Upgrade Append -> Merge if any op needs merge semantics.
        if mode == PendingMode::Merge {
            entry.mode = PendingMode::Merge;
        }
        entry.batches.push(batch);
        Ok(())
    }

    /// Record a delete that already inline-committed at the Lance layer.
    pub(crate) fn record_inline(&mut self, update: SubTableUpdate) {
        self.inline_committed.insert(update.table_key.clone(), update);
    }

    /// Read-your-writes accessor: the accumulated pending batches for
    /// `table_key`, or `&[]` if none.
    pub(crate) fn pending_batches(&self, table_key: &str) -> &[RecordBatch] {
        self.pending
            .get(table_key)
            .map(|p| p.batches.as_slice())
            .unwrap_or(&[])
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
        _branch: Option<&str>,
    ) -> Result<StagedMutation> {
        let MutationStaging {
            expected_versions,
            paths,
            pending,
            inline_committed,
            op_kinds,
        } = self;

        let mut staged_entries: Vec<StagedTableEntry> = Vec::with_capacity(pending.len());
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

            // Reopen the dataset for staging. The op_kind reflects the
            // accumulated PendingTable's mode: Append-mode batches are
            // INSERT-shaped (no key-based dedup at commit_staged); Merge-
            // mode batches are MERGE-shaped (key-dedup at commit_staged).
            // Both skip the strict pre-stage version check under the
            // [`MutationOpKind`] policy: Lance's natural rebase + the
            // per-(table, branch) queue + the publisher CAS in
            // `commit_all` handle drift; the strict check would
            // over-reject in-process concurrent inserts (PR 2 / MR-686
            // Phase 2).
            let stage_kind = match table.mode {
                PendingMode::Append => crate::db::MutationOpKind::Insert,
                PendingMode::Merge => crate::db::MutationOpKind::Merge,
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
                continue;
            }

            // For Merge mode, dedupe accumulated batches by `id`, keeping
            // the LAST occurrence (last-write-wins for the query). This
            // is required because Lance's `MergeInsertBuilder` produces
            // arbitrary results on duplicate keys in the source. Append
            // mode is exempt because no-key node and edge inserts use
            // ULID-generated ids that are unique within a query.
            let combined = match table.mode {
                PendingMode::Merge => {
                    dedupe_merge_batches_by_id(&table.schema, table.batches)?
                }
                PendingMode::Append => {
                    if table.batches.len() == 1 {
                        table.batches.into_iter().next().unwrap()
                    } else {
                        arrow_select::concat::concat_batches(
                            &table.schema,
                            &table.batches,
                        )
                        .map_err(|e| OmniError::Lance(e.to_string()))?
                    }
                }
            };

            // Stage produces uncommitted fragments + transaction. No
            // Lance HEAD advance until `commit_all` runs `commit_staged`.
            let staged = match table.mode {
                PendingMode::Append => {
                    db.table_store().stage_append(&ds, combined, &[]).await?
                }
                PendingMode::Merge => {
                    db.table_store()
                        .stage_merge_insert(
                            ds.clone(),
                            combined,
                            vec!["id".to_string()],
                            lance::dataset::WhenMatched::UpdateAll,
                            lance::dataset::WhenNotMatched::InsertAll,
                        )
                        .await?
                }
            };
            staged_entries.push(StagedTableEntry {
                table_key,
                path,
                expected_version: expected,
                dataset: ds,
                staged_write: staged,
            });
        }

        Ok(StagedMutation {
            inline_committed,
            staged: staged_entries,
            expected_versions,
            paths,
            op_kinds,
        })
    }
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
/// `commit_all`. Holds the opened `Dataset` so `commit_staged` doesn't
/// re-open, and the `StagedWrite` whose `transaction` `commit_staged`
/// will execute.
struct StagedTableEntry {
    table_key: String,
    path: StagedTablePath,
    expected_version: u64,
    dataset: lance::Dataset,
    staged_write: crate::table_store::StagedWrite,
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
    /// repo) may have committed to one of our touched tables, advancing
    /// the manifest pin past our `expected_version`. We revalidate
    /// under the queue and fail-fast with `manifest_conflict` before
    /// any `commit_staged` so the orphaned uncommitted fragments stay
    /// unreferenced (cleaned by `cleanup_old_versions`'s age sweep)
    /// rather than being committed and creating a Lance-HEAD-ahead
    /// residual.
    pub(crate) async fn commit_all(
        self,
        db: &crate::db::Omnigraph,
        branch: Option<&str>,
        sidecar_kind: SidecarKind,
        actor_id: Option<&str>,
    ) -> Result<(
        Vec<SubTableUpdate>,
        HashMap<String, u64>,
        Option<RecoverySidecarHandle>,
        Vec<tokio::sync::OwnedMutexGuard<()>>,
    )> {
        let StagedMutation {
            inline_committed,
            mut staged,
            mut expected_versions,
            paths,
            op_kinds,
        } = self;

        // Acquire per-(table_key, branch) queues for every touched
        // table — both staged and inline-committed. Sorted by
        // `acquire_many` internally so all multi-table writers
        // (mutation, branch_merge, schema_apply, future MR-870
        // recovery) agree on acquisition order — prevents lock-order
        // inversion deadlock.
        //
        // For inline-committed tables (delete-only mutations), Lance
        // HEAD has already advanced inside `delete_where` before
        // `commit_all` runs. Holding the queue here doesn't prevent
        // that interleaving (commit 6 will move queue acquisition into
        // `delete_where`'s call site); it does prevent another writer
        // from interleaving between our delete and our publish, which
        // would otherwise leave a Lance-HEAD-ahead residual the
        // delete-only sidecar (added below) would have to recover.
        let mut queue_keys: Vec<(String, Option<String>)> = Vec::with_capacity(
            staged.len() + inline_committed.len(),
        );
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
        let guards = db.write_queue().acquire_many(&queue_keys).await;

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
        // Why per-branch (and not the bound-branch `db.snapshot()`):
        // when the caller mutates a branch other than the engine's
        // bound branch (e.g., feature-branch ingest from a server
        // handle bound to main), `db.snapshot()` returns the bound
        // branch's view of each table — which is the wrong pin for
        // the publisher's CAS on a different branch. Using
        // `snapshot_for_branch(branch)` resolves the per-branch
        // entries correctly. The cost is one fresh manifest read per
        // mutation; PR 1b's regression came from this same read, but
        // that read is now strictly necessary for cross-branch
        // correctness. Single-table same-branch mutations could still
        // skip this read (queue exclusivity makes the publisher CAS a
        // no-op), but the conditional adds complexity for marginal
        // gain — left as a follow-up perf optimization.
        //
        // Multi-coordinator deployments (§VI.27 aspirational) get
        // genuine cross-process drift detection from this read for
        // free.
        let snapshot = db.snapshot_for_branch(branch).await?;
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
        let mut pins: Vec<SidecarTablePin> = Vec::with_capacity(
            staged.len() + inline_committed.len(),
        );
        for entry in &staged {
            pins.push(SidecarTablePin {
                table_key: entry.table_key.clone(),
                table_path: entry.path.full_path.clone(),
                expected_version: entry.expected_version,
                post_commit_pin: entry.expected_version + 1,
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

        for entry in staged {
            let StagedTableEntry {
                table_key,
                path,
                expected_version: _,
                dataset,
                staged_write,
            } = entry;

            let new_ds = db
                .table_store()
                .commit_staged(Arc::new(dataset), staged_write.transaction)
                .await?;
            let state = db
                .table_store()
                .table_state(&path.full_path, &new_ds)
                .await?;
            updates.push(SubTableUpdate {
                table_key,
                table_version: state.version,
                table_branch: path.table_branch.clone(),
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
        }

        Ok((updates, expected_versions, sidecar_handle, guards))
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
/// opens via snapshot) share the same body.
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
    committed_ds: &Dataset,
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

    // Committed side: scan `src` plus the dedupe key column when set, so
    // we can both count and shadow in one pass.
    let projection: Vec<&str> = match dedupe_key_column {
        Some(col) if pending_keys.as_ref().is_some_and(|s| !s.is_empty()) => vec!["src", col],
        _ => vec!["src"],
    };
    let committed = db
        .table_store()
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
fn count_pending_src_naive(
    pending_batches: &[RecordBatch],
    counts: &mut HashMap<String, u32>,
) {
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
        let key_arr = key_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
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

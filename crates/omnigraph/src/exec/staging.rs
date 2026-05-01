//! Per-query staging accumulator for direct-publish writes (MR-794 step 2+).
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

use crate::db::SubTableUpdate;
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
/// Replaces the post-MR-771 inline-commit `MutationStaging.latest` map with
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
}

impl MutationStaging {
    /// Capture pre-write metadata on first touch of a table. Subsequent
    /// touches are no-ops (paths and `expected_version` are stable for the
    /// lifetime of one query).
    pub(crate) fn ensure_path(
        &mut self,
        table_key: &str,
        full_path: String,
        table_branch: Option<String>,
        expected_version: u64,
    ) {
        self.paths.entry(table_key.to_string()).or_insert(StagedTablePath {
            full_path,
            table_branch,
        });
        self.expected_versions
            .entry(table_key.to_string())
            .or_insert(expected_version);
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
    /// `stage_append` or `stage_merge_insert` followed by `commit_staged`.
    /// Merge with inline-committed entries. Return `(updates,
    /// expected_versions)` for `commit_updates_on_branch_with_expected`.
    ///
    /// Sequential per-table — no cross-table dependency, but a parallel
    /// version is a perf optimization for multi-table writes (loader with
    /// many node + edge types). v1 ships sequential; the fan-out can land
    /// in a follow-up.
    pub(crate) async fn finalize(
        self,
        db: &crate::db::Omnigraph,
        _branch: Option<&str>,
    ) -> Result<(Vec<SubTableUpdate>, HashMap<String, u64>)> {
        let MutationStaging {
            expected_versions,
            paths,
            pending,
            inline_committed,
        } = self;

        let mut updates: Vec<SubTableUpdate> =
            inline_committed.into_values().collect();

        for (table_key, table) in pending {
            let path = paths.get(&table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::finalize: missing path for table '{}'",
                    table_key
                ))
            })?;
            let expected = *expected_versions.get(&table_key).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "MutationStaging::finalize: missing expected version for table '{}'",
                    table_key
                ))
            })?;

            // Reopen at the pre-write version. Lance HEAD has not advanced
            // since `ensure_path` captured it — no prior op committed to
            // this dataset.
            let ds = db
                .reopen_for_mutation(
                    &table_key,
                    &path.full_path,
                    path.table_branch.as_deref(),
                    expected,
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

            // Commit via Lance's two-phase write: stage produces
            // uncommitted fragments + transaction; commit advances HEAD.
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
            let new_ds = db
                .table_store()
                .commit_staged(Arc::new(ds), staged.transaction)
                .await?;
            let state = db
                .table_store()
                .table_state(&path.full_path, &new_ds)
                .await?;
            updates.push(SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch: path.table_branch.clone(),
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            });
        }

        Ok((updates, expected_versions))
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

//! Candidate-pruning optimization for the per-commit change enumerator
//! (RFC-030 §4.2/§4.3).
//!
//! The authority path in [`super::enumerate`] derives one commit's changes by a
//! full ordered-by-id merge of both pinned table versions — O(table extent).
//! When the commit's effect on a table is a mature, row-set-preserving
//! insert/update shape, the same logical changes can be derived in O(delta): a
//! candidate scan of the rows the commit touched (by Lance row-version columns)
//! plus a batched exact-id probe of the parent for before-images. This module
//! decides, per interval, whether that optimization is available; the caller
//! falls back to the exact full merge on any doubt.
//!
//! **Why the pruned path needs no delete handling.** A single graph commit
//! advances a touched table by exactly one Lance version (one transaction), and
//! the parse-time D2 rule keeps inserts/updates and deletes out of the same
//! mutation. So a commit's effect on one table is *either* a row-set-preserving
//! insert/update *or* a delete — never both. If every transaction in the
//! interval is `Append` or a row-set-preserving merge `Update`, then no live
//! logical id can disappear (neither op removes a row), so the interval has zero
//! logical deletes and the candidate scan is complete. Any operation that can
//! remove, reuse, or re-stamp rows (`Delete`, `Overwrite`, `Restore`,
//! compaction `Rewrite`, …) makes the whole interval fall back to the exact
//! merge, which classifies deletes correctly.

use lance::Dataset;
use lance::dataset::transaction::{Operation, UpdateMode};

use crate::db::SubTableEntry;
use crate::error::Result;

/// Scan bound on the transaction interval, mirroring the branch-merge
/// pure-insert history walk (`PURE_INSERT_HISTORY_MAX_VERSIONS`). A commit
/// normally advances a table by one version, so this is a generous ceiling that
/// still refuses to walk an unbounded interval.
const CANDIDATE_SCAN_MAX_VERSIONS: u64 = 1_024;

/// Whether one Lance transaction's operation preserves the live logical row set
/// — i.e. can only add or modify rows in place, never remove, reuse, or
/// re-stamp a logical id. Only such operations are safe to derive by candidate
/// scan + parent probe; everything else forces the exact ordered merge.
///
/// The match is exhaustive with **no wildcard arm**: a new Lance `Operation`
/// variant is a compile error that forces this classification to be reviewed
/// (RFC-030 §9 — new variants must fall back until reviewed).
pub(crate) fn operation_is_row_set_preserving(operation: &Operation) -> bool {
    match operation {
        // Append only adds fragments; it never removes or reuses a logical id.
        Operation::Append { .. } => true,
        // OmniGraph's keyed writes (strict-insert / upsert / known-present
        // update) are all `RewriteRows` merge_insert and never delete an
        // unmatched-by-source row — `WhenNotMatchedBySource::Delete` is absent
        // from the codebase (locked by the write-path guard test). A different
        // update mode is a foreign or unknown shape, so fall back.
        Operation::Update { update_mode, .. } => update_mode == &Some(UpdateMode::RewriteRows),
        // Everything that can remove, reuse, or re-stamp rows falls back to the
        // exact ordered merge. Listed explicitly so a new variant fails to
        // compile here.
        Operation::Delete { .. }
        | Operation::Overwrite { .. }
        | Operation::CreateIndex { .. }
        | Operation::Rewrite { .. }
        | Operation::DataReplacement { .. }
        | Operation::DataOverlay { .. }
        | Operation::Merge { .. }
        | Operation::Restore { .. }
        | Operation::ReserveFragments { .. }
        | Operation::Project { .. }
        | Operation::UpdateConfig { .. }
        | Operation::UpdateMemWalState { .. }
        | Operation::Clone { .. }
        | Operation::UpdateBases { .. } => false,
    }
}

/// Whether this changed interval can be derived by the O(delta) candidate path.
///
/// Requires: same table branch and immutable identity; the end version strictly
/// advances from begin within the scan bound; both pinned handles are at their
/// exact expected versions and use stable row IDs (so both row-version columns
/// are active); and every transaction in `(begin, end]` is row-set-preserving.
/// Any doubt — a branch/lineage change, a non-advancing or oversized interval,
/// an inactive row-version column, a missing/cleaned transaction, or an
/// unproven operation — returns `Ok(false)` so the caller uses the exact merge.
/// It never returns `Err` for a normal miss (e.g. cleaned history).
#[allow(dead_code)] // wired into the enumerator in a later stage
pub(crate) async fn interval_is_prunable(
    from_entry: &SubTableEntry,
    to_entry: &SubTableEntry,
    from_dataset: &Dataset,
    to_dataset: &Dataset,
) -> Result<bool> {
    if from_entry.table_branch != to_entry.table_branch || from_entry.identity != to_entry.identity {
        return Ok(false);
    }
    let Some(version_count) = to_entry
        .table_version
        .checked_sub(from_entry.table_version)
        .filter(|count| *count > 0 && *count <= CANDIDATE_SCAN_MAX_VERSIONS)
    else {
        return Ok(false);
    };
    if to_dataset.version().version != to_entry.table_version
        || from_dataset.version().version != from_entry.table_version
        || !to_dataset.manifest.uses_stable_row_ids()
        || !from_dataset.manifest.uses_stable_row_ids()
    {
        return Ok(false);
    }

    // Walk every transaction in (begin, end]. A build/list error or a missing
    // transaction is a normal miss (cleaned history) — not prunable, not an
    // error.
    let Ok(delta) = to_dataset
        .delta()
        .with_begin_version(from_entry.table_version)
        .with_end_version(to_entry.table_version)
        .build()
    else {
        return Ok(false);
    };
    let Ok(transactions) = delta.list_transactions().await else {
        return Ok(false);
    };
    if u64::try_from(transactions.len()).ok() != Some(version_count) {
        return Ok(false);
    }
    Ok(transactions
        .iter()
        .all(|transaction| operation_is_row_set_preserving(&transaction.operation)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance::dataset::transaction::Operation;
    use lance_table::format::Fragment;

    fn update(mode: Option<UpdateMode>) -> Operation {
        Operation::Update {
            removed_fragment_ids: Vec::new(),
            updated_fragments: Vec::new(),
            new_fragments: vec![Fragment::new(0)],
            fields_modified: Vec::new(),
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: Vec::new(),
            update_mode: mode,
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        }
    }

    #[test]
    fn append_and_rewrite_rows_update_are_row_set_preserving() {
        assert!(operation_is_row_set_preserving(&Operation::Append {
            fragments: Vec::new()
        }));
        // A merge Update that also modifies existing rows (non-empty
        // updated_fragments) is still row-set-preserving — unlike the
        // pure-insert certificate, this classifier accepts updates.
        let mut upsert = update(Some(UpdateMode::RewriteRows));
        if let Operation::Update {
            updated_fragments,
            removed_fragment_ids,
            ..
        } = &mut upsert
        {
            updated_fragments.push(Fragment::new(1));
            removed_fragment_ids.push(1);
        }
        assert!(operation_is_row_set_preserving(&upsert));
    }

    #[test]
    fn foreign_update_mode_and_removing_ops_fall_back() {
        // A non-RewriteRows update mode is a foreign/unknown shape.
        assert!(!operation_is_row_set_preserving(&update(Some(
            UpdateMode::RewriteColumns
        ))));
        assert!(!operation_is_row_set_preserving(&update(None)));
        // Operations that can remove, reuse, or re-stamp rows.
        assert!(!operation_is_row_set_preserving(&Operation::Delete {
            updated_fragments: Vec::new(),
            deleted_fragment_ids: Vec::new(),
            predicate: String::new(),
        }));
        assert!(!operation_is_row_set_preserving(&Operation::Restore {
            version: 0
        }));
        assert!(!operation_is_row_set_preserving(
            &Operation::ReserveFragments { num_fragments: 1 }
        ));
    }
}

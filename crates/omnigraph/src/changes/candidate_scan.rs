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
//! interval is an `Append` or a **provably** row-set-preserving merge `Update`,
//! then no live logical id can disappear (neither op removes a row), so the
//! interval has zero logical deletes and the candidate scan is complete. Any
//! operation that can remove, reuse, or re-stamp rows (`Delete`, `Overwrite`,
//! `Restore`, compaction `Rewrite`, …) makes the whole interval fall back to the
//! exact merge, which classifies deletes correctly.
//!
//! A `RewriteRows` `Update` is trusted as row-set-preserving only with a
//! **durable per-transaction provenance proof** — the `omnigraph.no_by_source_delete`
//! marker every general keyed MergeInsert update stamps, or the RFC-023
//! `insert_absence` certificate that proven strict inserts carry instead. The
//! op shape alone is not enough: `repair --force --confirm` can
//! adopt an external Lance merge whose delete-capable by-source arm persists as
//! `Update { RewriteRows }`, and its child-only candidate scan has no delete
//! pass. The source-walk guard (`no_delete_capable_merge_arm_in_engine_source`)
//! proves only that *current engine code* builds no such arm; the marker
//! authenticates the *persisted* transaction. An unproven `Update` falls back.
//! See [`transaction_is_row_set_preserving`].

use std::collections::{HashMap, VecDeque};

use datafusion::prelude::{col, lit};
use lance::Dataset;
use lance::dataset::transaction::{Operation, Transaction, UpdateMode};
use lance_table::format::Fragment;

use super::enumerate::{Emit, next_emit};
use super::model::ChangeFeedScope;
use super::row_compare::{OrderedRows, RawRow, rows_equal};
use crate::db::SubTableEntry;
use crate::error::Result;
use crate::table_store::{has_insert_absence_certificate, has_no_by_source_delete_marker};

/// Parent probe chunk size: one BTREE-backed `id IN (chunk)` lookup per chunk,
/// matching the keyed-write delta bound (`UNIQUE_PROBE_CHUNK_KEYS`).
const PARENT_PROBE_CHUNK: usize = 8_192;

/// Scan bound on the transaction interval, mirroring the branch-merge
/// pure-insert history walk (`PURE_INSERT_HISTORY_MAX_VERSIONS`). A commit
/// normally advances a table by one version, so this is a generous ceiling that
/// still refuses to walk an unbounded interval.
const CANDIDATE_SCAN_MAX_VERSIONS: u64 = 1_024;

/// Whether one Lance transaction's operation preserves the live logical row set
/// — i.e. can only add or modify rows in place, never remove, reuse, or re-stamp
/// a logical id. Only such operations are safe to derive by candidate scan +
/// parent probe; everything else forces the exact ordered merge.
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
        // unmatched-by-source row — no delete-capable by-source merge arm exists
        // in the engine (locked by the write-path guard in forbidden_apis.rs). A
        // different update mode is a foreign or unknown shape, so fall back.
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

/// Whether one transaction is safe to derive by the O(delta) candidate path.
///
/// It must have a row-set-preserving operation SHAPE
/// ([`operation_is_row_set_preserving`]) AND, for a `RewriteRows` `Update`, a
/// durable OmniGraph provenance proof that it removed no rows: either the
/// no-by-source-delete marker every keyed write stamps, or the RFC-023
/// `insert_absence` certificate (a pure insert deletes nothing). `Append` is
/// unconditionally additive and needs no marker.
///
/// The shape check alone is NOT sufficient: `repair --force --confirm` can adopt
/// an external Lance merge whose delete-capable by-source arm persists as
/// `Operation::Update { RewriteRows }`. Such a transaction carries neither proof,
/// so it falls back to the exact ordered merge — whose delete pass reports the
/// removed rows the child-only candidate scan would miss.
pub(crate) fn transaction_is_row_set_preserving(transaction: &Transaction) -> bool {
    if !operation_is_row_set_preserving(&transaction.operation) {
        return false;
    }
    match &transaction.operation {
        Operation::Append { .. } => true,
        Operation::Update { .. } => {
            has_no_by_source_delete_marker(transaction)
                || has_insert_absence_certificate(transaction)
        }
        // Unreachable: the shape guard above already rejected every other
        // variant. Keeping the match total means a future variant that
        // `operation_is_row_set_preserving` starts accepting must be classified
        // here too, rather than silently pruning.
        _ => false,
    }
}

/// The changed child fragments if this interval can be derived by the O(delta)
/// candidate path, or `None` to use the exact ordered merge.
///
/// Requires: same table branch and immutable identity; the end version strictly
/// advances from begin within the scan bound; both pinned handles are at their
/// exact expected versions and use stable row IDs (so both row-version columns
/// are active); and every transaction in `(begin, end]` is row-set-preserving.
/// Any doubt — a branch/lineage change, a non-advancing or oversized interval,
/// an inactive row-version column, a missing/cleaned transaction, or an unproven
/// operation — returns `Ok(None)` so the caller uses the exact merge. It never
/// returns `Err` for a normal miss (e.g. cleaned history).
///
/// The `(begin, end]` transaction walk is a LIVE read of numeric-path version
/// manifests — the one replaceable read on the pruned path (data and
/// transaction files are UUID-named). The sole caller is `plan_intervals`,
/// which runs it BEFORE the final `reprove_named_branch_heads` witness so a
/// branch delete/recreate cannot swap the classified history; the
/// `CHANGE_FEED_POST_HEAD_WITNESS` failpoint cell pins that ordering.
pub(crate) async fn interval_changed_fragments(
    from_entry: &SubTableEntry,
    to_entry: &SubTableEntry,
    from_dataset: &Dataset,
    to_dataset: &Dataset,
) -> Result<Option<Vec<Fragment>>> {
    if from_entry.table_branch != to_entry.table_branch || from_entry.identity != to_entry.identity
    {
        return Ok(None);
    }
    let Some(version_count) = to_entry
        .table_version
        .checked_sub(from_entry.table_version)
        .filter(|count| *count > 0 && *count <= CANDIDATE_SCAN_MAX_VERSIONS)
    else {
        return Ok(None);
    };
    if to_dataset.version().version != to_entry.table_version
        || from_dataset.version().version != from_entry.table_version
        || !to_dataset.manifest.uses_stable_row_ids()
        || !from_dataset.manifest.uses_stable_row_ids()
    {
        return Ok(None);
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
        return Ok(None);
    };
    let Ok(transactions) = delta.list_transactions().await else {
        return Ok(None);
    };
    if u64::try_from(transactions.len()).ok() != Some(version_count) {
        return Ok(None);
    }
    if !transactions.iter().all(transaction_is_row_set_preserving) {
        return Ok(None);
    }

    // The changed fragments are exactly the child fragments the parent does not
    // have: every proven op only ADDS fragments (append, or rewrite = remove old
    // + add new), so a child fragment absent from the parent carries this
    // interval's inserted/updated rows. Computed from the already-loaded
    // manifests — no object-store data reads — so the candidate scan reads only
    // O(delta) fragments.
    let parent_ids: std::collections::HashSet<u64> = from_dataset
        .fragments()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    let changed: Vec<Fragment> = to_dataset
        .fragments()
        .iter()
        .filter(|fragment| !parent_ids.contains(&fragment.id))
        .cloned()
        .collect();
    // Row-version metadata is correctness-bearing on the pruned path: the
    // candidate scan filters on `_row_last_updated_at_version`, and pinned
    // Lance 10 silently fills that column with 1 for a fragment whose sequence
    // is missing OR fails to load ("Default to version 1 if sequence not
    // provided" in lance-table's stream reader — a failed `load_sequence()` is
    // swallowed the same way). For any interval with `begin > 1` such rows
    // fall outside the candidate window and real updates vanish without an
    // error. Require every changed fragment to carry loadable, structurally
    // valid last-updated metadata; any gap is a normal miss that falls back to
    // the exact ordered merge (which does not consume the version column).
    if !changed.iter().all(fragment_version_metadata_is_loadable) {
        return Ok(None);
    }
    Ok(Some(changed))
}

/// Whether one changed fragment's `_row_last_updated_at_version` sequence is
/// present, decodable, and complete — the §4.2 "genuinely active" requirement
/// at the fragment level. `uses_stable_row_ids()` alone is a dataset-level
/// flag and cannot prove a specific fragment's sequence survives loading.
///
/// The Inline bytes are decoded directly rather than through
/// `load_sequence()`: pinned Lance 10's External arm of that method is
/// `todo!()` (it panics, it does not `Err`), so the variant must be
/// structurally unreachable here. And a clean decode alone does not prove the
/// sequence covers the fragment — Lance's single-run fast path stamps its one
/// version across every requested row without consulting the encoded length —
/// so the sequence must be exactly `physical_rows` long. Absent, external,
/// undecodable, short, or unmeasurable all mean "not provably complete" and
/// route the interval to the exact ordered merge.
fn fragment_version_metadata_is_loadable(fragment: &Fragment) -> bool {
    let Some(lance_table::rowids::version::RowDatasetVersionMeta::Inline(data)) =
        fragment.last_updated_at_version_meta.as_ref()
    else {
        return false;
    };
    let Ok(sequence) = lance_table::rowids::version::read_dataset_versions(data) else {
        return false;
    };
    fragment
        .physical_rows
        .is_some_and(|rows| sequence.len() == rows as u64)
}

/// Fetch full before-image rows for `ids` from the parent handle in one
/// BTREE-backed `id IN (chunk)` lookup (never per-row round trips, never a
/// string filter). The returned rows carry `_rowid`/`_rowaddr` and Blob
/// descriptions so `rows_equal` and `emitted_image` behave exactly as on the
/// full-merge path.
async fn probe_parent_images(parent: &Dataset, ids: &[String]) -> Result<HashMap<String, RawRow>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let filter = col("id").in_list(ids.iter().map(|id| lit(id.clone())).collect(), false);
    let mut rows = OrderedRows::open_filtered(parent.clone(), None, Some(filter)).await?;
    let mut images = HashMap::with_capacity(ids.len());
    while let Some(row) = rows.pop().await? {
        images.insert(row.id.clone(), row);
    }
    Ok(images)
}

/// The O(delta) emitter for a proven row-set-preserving interval: an id-ordered
/// scan of the rows the commit touched (by `_row_last_updated_at_version`) plus
/// a batched parent probe for before-images. It yields only inserts and updates
/// — a prunable interval has zero logical deletes (see the module docs), so no
/// delete pass is needed.
pub(crate) struct CandidateUpserts {
    parent: Dataset,
    candidates: OrderedRows,
    scope: ChangeFeedScope,
    ready: VecDeque<Emit>,
}

impl CandidateUpserts {
    async fn open(
        from_entry: &SubTableEntry,
        to_entry: &SubTableEntry,
        from_dataset: Dataset,
        to_dataset: Dataset,
        changed_fragments: Vec<Fragment>,
        after_id: Option<&str>,
        scope: ChangeFeedScope,
    ) -> Result<Self> {
        // Scan only the fragments this commit wrote (O(delta)), and within them
        // keep rows whose last update lands in (begin, end] — this drops the
        // carried-over rows a fragment rewrite pulled along, leaving exactly the
        // inserted and updated rows (the parent probe classifies which).
        let window = col("_row_last_updated_at_version")
            .gt(lit(from_entry.table_version))
            .and(col("_row_last_updated_at_version").lt_eq(lit(to_entry.table_version)));
        let candidates =
            OrderedRows::open_scan(to_dataset, after_id, Some(window), Some(changed_fragments))
                .await?;
        Ok(Self {
            parent: from_dataset,
            candidates,
            scope,
            ready: VecDeque::new(),
        })
    }

    fn parent_dataset(&self) -> &Dataset {
        &self.parent
    }

    fn child_dataset(&self) -> &Dataset {
        self.candidates.dataset()
    }

    async fn next(&mut self) -> Result<Option<Emit>> {
        loop {
            if let Some(emit) = self.ready.pop_front() {
                return Ok(Some(emit));
            }
            // Pull the next id-ordered chunk of candidates, then probe the
            // parent once for the whole chunk.
            let mut chunk: Vec<RawRow> = Vec::new();
            while chunk.len() < PARENT_PROBE_CHUNK {
                match self.candidates.pop().await? {
                    Some(row) => chunk.push(row),
                    None => break,
                }
            }
            if chunk.is_empty() {
                return Ok(None);
            }
            let ids: Vec<String> = chunk.iter().map(|row| row.id.clone()).collect();
            let parents = probe_parent_images(&self.parent, &ids).await?;
            for candidate in chunk {
                let emit = match parents.get(&candidate.id) {
                    // Absent in the parent -> a new logical id -> insert.
                    None => Emit::Insert(candidate),
                    // Present in the parent -> update unless the logical image is
                    // unchanged (a physical no-op / metadata-only movement).
                    Some(before) => {
                        if rows_equal(&self.parent, before, self.candidates.dataset(), &candidate)
                            .await?
                        {
                            continue;
                        }
                        Emit::Update {
                            before: before.clone(),
                            after: candidate,
                        }
                    }
                };
                if self.scope.wants_op(emit.op()) {
                    self.ready.push_back(emit);
                }
            }
        }
    }
}

/// Per-interval change emitter: the O(delta) candidate path when the interval is
/// provably row-set-preserving, else the exact full ordered merge. Both yield
/// the same id-ordered `Emit` stream; before-images come from the parent handle
/// and after-images from the child handle.
pub(crate) enum EmitSource {
    FullMerge {
        from: OrderedRows,
        to: OrderedRows,
        scope: ChangeFeedScope,
    },
    Pruned(CandidateUpserts),
}

impl EmitSource {
    /// Open the emitter for one interval. `changed_fragments` is the pruning
    /// decision [`interval_changed_fragments`] computed in `plan_intervals`
    /// UNDER the final head witness — this constructor performs no live
    /// history read, so a branch delete/recreate after the witness cannot
    /// reroute the interval.
    pub(crate) async fn plan(
        from_entry: &SubTableEntry,
        to_entry: &SubTableEntry,
        from_dataset: Dataset,
        to_dataset: Dataset,
        changed_fragments: Option<Vec<Fragment>>,
        after_id: Option<&str>,
        scope: &ChangeFeedScope,
    ) -> Result<Self> {
        if let Some(changed_fragments) = changed_fragments {
            Ok(Self::Pruned(
                CandidateUpserts::open(
                    from_entry,
                    to_entry,
                    from_dataset,
                    to_dataset,
                    changed_fragments,
                    after_id,
                    scope.clone(),
                )
                .await?,
            ))
        } else {
            let from = OrderedRows::open(from_dataset, after_id).await?;
            let to = OrderedRows::open(to_dataset, after_id).await?;
            Ok(Self::FullMerge {
                from,
                to,
                scope: scope.clone(),
            })
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Emit>> {
        match self {
            Self::FullMerge { from, to, scope } => next_emit(from, to, scope).await,
            Self::Pruned(candidates) => candidates.next().await,
        }
    }

    pub(crate) fn parent_dataset(&self) -> &Dataset {
        match self {
            Self::FullMerge { from, .. } => from.dataset(),
            Self::Pruned(candidates) => candidates.parent_dataset(),
        }
    }

    pub(crate) fn child_dataset(&self) -> &Dataset {
        match self {
            Self::FullMerge { to, .. } => to.dataset(),
            Self::Pruned(candidates) => candidates.child_dataset(),
        }
    }
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
            fragments: vec![Fragment::new(7)],
        }));
        // A merge Update that also modifies existing rows (non-empty
        // updated_fragments / removed_fragment_ids) is still row-set-preserving —
        // unlike the pure-insert certificate, this classifier accepts updates.
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

    fn txn(operation: Operation, properties: &[(&str, &str)]) -> Transaction {
        let transaction_properties = (!properties.is_empty()).then(|| {
            std::sync::Arc::new(
                properties
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        });
        Transaction {
            read_version: 0,
            uuid: "test".to_string(),
            operation,
            tag: None,
            transaction_properties,
        }
    }

    #[test]
    fn transaction_update_prunes_only_with_a_durable_no_delete_proof() {
        use crate::table_store::{
            INSERT_ABSENCE_PROPERTY, INSERT_ABSENCE_V1, NO_BY_SOURCE_DELETE_PROPERTY,
            NO_BY_SOURCE_DELETE_V1,
        };
        // Append is unconditionally additive — no marker required.
        assert!(transaction_is_row_set_preserving(&txn(
            Operation::Append {
                fragments: vec![Fragment::new(1)],
            },
            &[],
        )));
        // A RewriteRows Update prunes with the keyed-write no-delete marker ...
        assert!(transaction_is_row_set_preserving(&txn(
            update(Some(UpdateMode::RewriteRows)),
            &[(NO_BY_SOURCE_DELETE_PROPERTY, NO_BY_SOURCE_DELETE_V1)],
        )));
        // ... or the RFC-023 insert_absence certificate (a pure insert deletes
        // nothing) ...
        assert!(transaction_is_row_set_preserving(&txn(
            update(Some(UpdateMode::RewriteRows)),
            &[(INSERT_ABSENCE_PROPERTY, INSERT_ABSENCE_V1)],
        )));
        // ... but NOT without a durable proof. An external Lance merge with a
        // delete-capable by-source arm, adopted via `repair --force`, persists
        // this exact `Update{RewriteRows}` shape and carries no marker — this is
        // the data-loss regression the fix closes (the op-shape classifier alone
        // returns true here).
        assert!(operation_is_row_set_preserving(&update(Some(
            UpdateMode::RewriteRows
        ))));
        assert!(!transaction_is_row_set_preserving(&txn(
            update(Some(UpdateMode::RewriteRows)),
            &[],
        )));
        // An unrelated property is not a no-delete proof.
        assert!(!transaction_is_row_set_preserving(&txn(
            update(Some(UpdateMode::RewriteRows)),
            &[("lance.something", "x")],
        )));
        // The shape guard still fences a delete even if a marker were spoofed
        // onto it, so a stray marker can never make a removing op prune.
        assert!(!transaction_is_row_set_preserving(&txn(
            Operation::Delete {
                updated_fragments: Vec::new(),
                deleted_fragment_ids: Vec::new(),
                predicate: String::new(),
            },
            &[(NO_BY_SOURCE_DELETE_PROPERTY, NO_BY_SOURCE_DELETE_V1)],
        )));
    }

    #[test]
    fn missing_row_version_metadata_is_not_loadable() {
        // Pinned Lance 10 fills `_row_last_updated_at_version` with 1 when a
        // fragment's sequence is missing or fails to load, which would silently
        // empty the candidate window for begin > 1. A fragment without the
        // metadata (Lance's `Fragment::new` default) must therefore fail the
        // loadability gate so the interval falls back to the exact merge. The
        // positive case — every real OmniGraph-written changed fragment carries
        // a loadable sequence — is proven end-to-end by the pruned cost/image
        // tests staying flat/green (they would fall back and fail otherwise).
        assert!(!fragment_version_metadata_is_loadable(&Fragment::new(7)));
    }

    #[test]
    fn external_row_version_metadata_is_not_loadable() {
        // Pinned Lance 10's `RowDatasetVersionMeta::External` arm of
        // `load_sequence()` is `todo!()` — it panics rather than returning
        // `Err`. The gate must classify the variant structurally instead of
        // probing it, so an externally-stored sequence routes the interval to
        // the exact ordered merge instead of aborting the poll.
        let mut fragment = Fragment::new(7);
        fragment.last_updated_at_version_meta = Some(
            lance_table::rowids::version::RowDatasetVersionMeta::External(
                lance_table::format::ExternalFile {
                    path: "external.versions".to_string(),
                    offset: 0,
                    size: 64,
                },
            ),
        );
        assert!(!fragment_version_metadata_is_loadable(&fragment));
    }

    #[test]
    fn short_row_version_sequence_is_not_loadable() {
        // A sequence can decode cleanly yet cover fewer rows than the fragment
        // holds; pinned Lance 10's single-run fast path then stamps that run's
        // version across every requested row without consulting the encoded
        // length. Loadable therefore means decodable AND exactly
        // `physical_rows` long — anything shorter (or a fragment that does not
        // even record its physical row count) falls back to the exact merge.
        use lance_table::rowids::version::{
            RowDatasetVersionMeta, RowDatasetVersionSequence, write_dataset_versions,
        };

        let mut fragment = Fragment::new(7);
        fragment.physical_rows = Some(5);
        let short = RowDatasetVersionSequence::from_uniform_row_count(3, 42);
        fragment.last_updated_at_version_meta = Some(RowDatasetVersionMeta::Inline(
            write_dataset_versions(&short).into(),
        ));
        assert!(!fragment_version_metadata_is_loadable(&fragment));

        let exact = RowDatasetVersionSequence::from_uniform_row_count(5, 42);
        fragment.last_updated_at_version_meta = Some(RowDatasetVersionMeta::Inline(
            write_dataset_versions(&exact).into(),
        ));
        assert!(fragment_version_metadata_is_loadable(&fragment));

        fragment.physical_rows = None;
        assert!(!fragment_version_metadata_is_loadable(&fragment));
    }
}

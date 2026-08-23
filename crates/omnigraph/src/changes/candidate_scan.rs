//! Candidate-pruning optimization for the per-commit change enumerator
//! (RFC-030 §4.2/§4.3).
//!
//! The authority path in [`super::enumerate`] derives one commit's changes by a
//! full ordered-by-id merge of both pinned dataset versions — O(dataset
//! extent). When one adjacent Lance transaction has a mature,
//! row-set-preserving insert/update shape, the same logical changes can be
//! derived from that transaction's physical footprint: candidate rows come
//! from the newly assigned child-fragment suffix and before-images come from
//! only the parent fragments the transaction updated or removed. Both sides
//! are merged as ordered streams. This module decides, per interval, whether
//! that optimization is available; the caller falls back to the exact full
//! merge on any doubt.
//!
//! **Why the pruned path needs no delete handling.** Eligibility first requires
//! one graph-visible dataset interval to advance by exactly one Lance version.
//! The parse-time D2 rule keeps inserts/updates and deletes out of the same
//! mutation, so an engine-authored adjacent transaction is either a
//! row-set-preserving insert/update or a delete — never both. If that one
//! transaction is an `Append` or a **provably** row-set-preserving merge
//! `Update`, no live logical id can disappear and the candidate scan is
//! complete. Any wider interval or operation that can remove, reuse, or
//! re-stamp rows (`Delete`, `Overwrite`, `Restore`, compaction `Rewrite`, …)
//! falls back to the exact merge, which classifies deletes correctly.
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

use datafusion::prelude::{col, lit};
use lance::Dataset;
use lance::dataset::transaction::{Operation, Transaction, UpdateMode};
use lance_table::format::Fragment;

use super::enumerate::{Emit, next_emit};
use super::model::ChangeFeedScope;
use super::row_compare::{OrderedRows, ScanTargets, rows_equal};
use crate::db::DatasetEntry;
use crate::error::Result;
use crate::table_store::{has_insert_absence_certificate, has_no_by_source_delete_marker};

/// Whether one Lance transaction's operation preserves the live logical row set
/// — i.e. can only add or modify rows in place, never remove, reuse, or re-stamp
/// a logical id. Only such operations are safe to derive from candidate and
/// transaction-touched parent fragments; everything else forces the exact
/// ordered merge.
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

/// Whether one transaction is safe to derive by the touched-fragment candidate
/// path.
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

/// Immutable physical plan for a proven adjacent candidate interval. Both
/// vectors are bounded by the one transaction's touched-fragment footprint;
/// neither is a copy of the full manifest.
#[derive(Debug, Clone)]
pub(crate) struct CandidatePlan {
    child_fragments: Vec<Fragment>,
    parent_fragments: Vec<Fragment>,
}

/// Return the candidate plan for one adjacent, row-set-preserving Lance
/// transaction, or `None` to use the exact ordered merge.
///
/// The adjacency requirement is deliberate. A stateless page may be resumed
/// many times; walking up to 1,024 historical transactions on every page would
/// multiply history work by page count. An interval wider than one version now
/// falls back *before any transaction read*. For the accepted shape we read
/// only the already-open child's one transaction and require its `read_version`
/// to name the pinned parent exactly.
///
/// Fragment discovery uses Lance's manifest invariants: fragments are sorted
/// by id, ids never recycle, and one Append/Update assigns every new fragment
/// consecutively above the parent's high-water mark. The child suffix is found
/// by binary search; affected parent fragments are found by one binary search
/// per transaction-reported id. Work is therefore
/// O(log(manifest) + touched_fragments log(manifest)), with allocations bounded
/// by touched fragments rather than total dataset extent.
///
/// `read_transaction` follows the transaction reference already captured in
/// the pinned child manifest; Lance transaction objects are UUID-named. The
/// sole caller nevertheless stores the complete plan before the final
/// named-branch head witness, and emission performs no later history lookup.
pub(crate) async fn interval_candidate_plan(
    from_entry: &DatasetEntry,
    to_entry: &DatasetEntry,
    from_dataset: &Dataset,
    to_dataset: &Dataset,
) -> Result<Option<CandidatePlan>> {
    if from_entry.native_dataset_branch != to_entry.native_dataset_branch
        || from_entry.identity != to_entry.identity
    {
        return Ok(None);
    }
    if from_entry.published_dataset_version.checked_add(1)
        != Some(to_entry.published_dataset_version)
    {
        return Ok(None);
    }
    if to_dataset.version().version != to_entry.published_dataset_version
        || from_dataset.version().version != from_entry.published_dataset_version
        || !to_dataset.manifest.uses_stable_row_ids()
        || !from_dataset.manifest.uses_stable_row_ids()
    {
        return Ok(None);
    }

    crate::instrumentation::record_candidate_transaction_read();
    let Ok(Some(transaction)) = to_dataset.read_transaction().await else {
        return Ok(None);
    };
    if transaction.read_version != from_entry.published_dataset_version
        || !transaction_is_row_set_preserving(&transaction)
    {
        return Ok(None);
    }

    Ok(candidate_plan_from_transaction(
        &transaction.operation,
        from_dataset,
        to_dataset,
    ))
}

fn candidate_plan_from_transaction(
    operation: &Operation,
    from_dataset: &Dataset,
    to_dataset: &Dataset,
) -> Option<CandidatePlan> {
    let (new_fragment_count, mut parent_fragment_ids) = match operation {
        Operation::Append { fragments } => (fragments.len(), Vec::new()),
        Operation::Update {
            removed_fragment_ids,
            updated_fragments,
            new_fragments,
            ..
        } => {
            let mut ids = Vec::with_capacity(
                removed_fragment_ids
                    .len()
                    .saturating_add(updated_fragments.len()),
            );
            ids.extend(removed_fragment_ids.iter().copied());
            ids.extend(updated_fragments.iter().map(|fragment| fragment.id));
            (new_fragments.len(), ids)
        }
        _ => return None,
    };
    parent_fragment_ids.sort_unstable();
    parent_fragment_ids.dedup();

    let parent_high_water = from_dataset.manifest.max_fragment_id();
    let (suffix_start, mut metadata_steps) =
        first_fragment_after(to_dataset.fragments(), parent_high_water);
    let child_fragments = &to_dataset.fragments()[suffix_start..];
    metadata_steps = metadata_steps.saturating_add(child_fragments.len() as u64);
    if child_fragments.len() != new_fragment_count {
        crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
        return None;
    }

    let expected_first = match parent_high_water {
        Some(id) => id.checked_add(1)?,
        None => 0,
    };
    for (offset, fragment) in child_fragments.iter().enumerate() {
        let expected_id = expected_first.checked_add(u64::try_from(offset).ok()?)?;
        if fragment.id != expected_id {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return None;
        }
    }
    let expected_high_water = if new_fragment_count == 0 {
        parent_high_water
    } else {
        Some(expected_first.checked_add(u64::try_from(new_fragment_count - 1).ok()?)?)
    };
    if to_dataset.manifest.max_fragment_id() != expected_high_water {
        crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
        return None;
    }

    let mut parent_fragments = Vec::with_capacity(parent_fragment_ids.len());
    for fragment_id in parent_fragment_ids {
        let (fragment, steps) = find_fragment(from_dataset.fragments(), fragment_id);
        metadata_steps = metadata_steps.saturating_add(steps);
        let Some(fragment) = fragment else {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return None;
        };
        parent_fragments.push(fragment.clone());
    }

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
    if !child_fragments
        .iter()
        .all(fragment_version_metadata_is_loadable)
    {
        crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
        return None;
    }
    crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
    Some(CandidatePlan {
        child_fragments: child_fragments.to_vec(),
        parent_fragments,
    })
}

/// Binary-search the first manifest fragment above `high_water`, counting the
/// metadata comparisons for the checked-in fragment-scaling cost gate.
fn first_fragment_after(fragments: &[Fragment], high_water: Option<u64>) -> (usize, u64) {
    let Some(high_water) = high_water else {
        return (0, 0);
    };
    let mut left = 0usize;
    let mut right = fragments.len();
    let mut steps = 0u64;
    while left < right {
        steps = steps.saturating_add(1);
        let middle = left + (right - left) / 2;
        if fragments[middle].id <= high_water {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left, steps)
}

fn find_fragment(fragments: &[Fragment], id: u64) -> (Option<&Fragment>, u64) {
    let mut left = 0usize;
    let mut right = fragments.len();
    let mut steps = 0u64;
    while left < right {
        steps = steps.saturating_add(1);
        let middle = left + (right - left) / 2;
        match fragments[middle].id.cmp(&id) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return (Some(&fragments[middle]), steps),
        }
    }
    (None, steps)
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

/// Emitter for a proven adjacent row-set-preserving interval. Candidate child
/// rows and the transaction-touched parent fragments are both scanned in id
/// order and merged one row at a time. No BTREE is required, so a missing or
/// partially covered index cannot turn the parent lookup into a hidden
/// full-dataset scan. At most one prepared row from either stream is retained;
/// scanner batch targets come from the current page budget.
pub(crate) struct CandidateUpserts {
    parent_dataset: Dataset,
    parents: Option<OrderedRows>,
    candidates: OrderedRows,
    scope: ChangeFeedScope,
}

impl CandidateUpserts {
    async fn open(
        from_entry: &DatasetEntry,
        to_entry: &DatasetEntry,
        from_dataset: Dataset,
        to_dataset: Dataset,
        plan: CandidatePlan,
        after_id: Option<&str>,
        scope: ChangeFeedScope,
        scan_targets: ScanTargets,
    ) -> Result<Self> {
        crate::instrumentation::record_candidate_scan_targets(
            scan_targets.rows(),
            scan_targets.bytes(),
        );
        // Scan only the new fragments this commit wrote, and within them
        // keep rows whose last update lands in (begin, end] — this drops the
        // carried-over rows a fragment rewrite pulled along, leaving exactly the
        // inserted and updated rows (the touched-parent merge classifies which).
        let window = col("_row_last_updated_at_version")
            .gt(lit(from_entry.published_dataset_version))
            .and(
                col("_row_last_updated_at_version").lt_eq(lit(to_entry.published_dataset_version)),
            );
        let candidates = OrderedRows::open_scan(
            to_dataset,
            after_id,
            Some(window),
            Some(plan.child_fragments),
            scan_targets,
        )
        .await?;
        let parents = if plan.parent_fragments.is_empty() {
            None
        } else {
            Some(
                OrderedRows::open_scan(
                    from_dataset.clone(),
                    after_id,
                    None,
                    Some(plan.parent_fragments),
                    scan_targets,
                )
                .await?,
            )
        };
        Ok(Self {
            parent_dataset: from_dataset,
            parents,
            candidates,
            scope,
        })
    }

    fn parent_dataset(&self) -> &Dataset {
        &self.parent_dataset
    }

    fn child_dataset(&self) -> &Dataset {
        self.candidates.dataset()
    }

    async fn next(&mut self) -> Result<Option<Emit>> {
        loop {
            let Some(candidate) = self.candidates.pop().await? else {
                return Ok(None);
            };
            crate::instrumentation::record_candidate_row_examined();

            let mut before = None;
            if let Some(parents) = self.parents.as_mut() {
                loop {
                    let parent_id = parents.peek().await?.map(|row| row.id.clone());
                    match parent_id {
                        Some(parent_id) if parent_id < candidate.id => {
                            // An unrelated row carried by a touched source
                            // fragment; it cannot be this candidate's before image.
                            parents.pop().await?;
                        }
                        Some(parent_id) if parent_id == candidate.id => {
                            before = parents.pop().await?;
                            break;
                        }
                        _ => break,
                    }
                }
            }

            let emit = match before {
                None => Emit::Insert(candidate),
                Some(before) => {
                    if rows_equal(
                        &self.parent_dataset,
                        &before,
                        self.candidates.dataset(),
                        &candidate,
                    )
                    .await?
                    {
                        continue;
                    }
                    Emit::Update {
                        before,
                        after: candidate,
                    }
                }
            };
            if self.scope.wants_op(emit.op()) {
                return Ok(Some(emit));
            }
        }
    }
}

/// Per-interval change emitter: the touched-fragment candidate path when the
/// interval is provably row-set-preserving, else the exact full ordered merge.
/// Both yield the same id-ordered `Emit` stream; before-images come from the
/// parent handle and after-images from the child handle.
pub(crate) struct FullMergeRows {
    from: OrderedRows,
    to: OrderedRows,
    scope: ChangeFeedScope,
}

pub(crate) enum EmitSource {
    FullMerge(Box<FullMergeRows>),
    Pruned(Box<CandidateUpserts>),
}

impl EmitSource {
    /// Open the emitter for one interval. `candidate_plan` is the pruning
    /// decision [`interval_candidate_plan`] computed in `plan_intervals` before
    /// (and therefore covered by) the final head witness. This constructor
    /// performs no live history read, so a branch delete/recreate after the
    /// witness cannot reroute the interval.
    pub(crate) async fn plan(
        from_entry: &DatasetEntry,
        to_entry: &DatasetEntry,
        from_dataset: Dataset,
        to_dataset: Dataset,
        candidate_plan: Option<CandidatePlan>,
        after_id: Option<&str>,
        scope: &ChangeFeedScope,
        scan_targets: ScanTargets,
    ) -> Result<Self> {
        if let Some(candidate_plan) = candidate_plan {
            Ok(Self::Pruned(Box::new(
                CandidateUpserts::open(
                    from_entry,
                    to_entry,
                    from_dataset,
                    to_dataset,
                    candidate_plan,
                    after_id,
                    scope.clone(),
                    scan_targets,
                )
                .await?,
            )))
        } else {
            let from = OrderedRows::open(from_dataset, after_id).await?;
            let to = OrderedRows::open(to_dataset, after_id).await?;
            Ok(Self::FullMerge(Box::new(FullMergeRows {
                from,
                to,
                scope: scope.clone(),
            })))
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Emit>> {
        match self {
            Self::FullMerge(full) => next_emit(&mut full.from, &mut full.to, &full.scope).await,
            Self::Pruned(candidates) => candidates.next().await,
        }
    }

    pub(crate) fn parent_dataset(&self) -> &Dataset {
        match self {
            Self::FullMerge(full) => full.from.dataset(),
            Self::Pruned(candidates) => candidates.parent_dataset(),
        }
    }

    pub(crate) fn child_dataset(&self) -> &Dataset {
        match self {
            Self::FullMerge(full) => full.to.dataset(),
            Self::Pruned(candidates) => candidates.child_dataset(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
    fn fragment_discovery_is_binary_search_plus_delta() {
        let fragments = (0..65_536).map(Fragment::new).collect::<Vec<_>>();

        let (suffix, suffix_steps) = first_fragment_after(&fragments, Some(65_530));
        assert_eq!(suffix, 65_531);
        assert!(
            suffix_steps <= 17,
            "65k-fragment suffix lookup must stay logarithmic, got {suffix_steps} steps"
        );

        let (fragment, lookup_steps) = find_fragment(&fragments, 42_424);
        assert_eq!(fragment.map(|fragment| fragment.id), Some(42_424));
        assert!(
            lookup_steps <= 17,
            "65k-fragment parent lookup must stay logarithmic, got {lookup_steps} steps"
        );
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

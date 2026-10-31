//! Row-identity change discovery for an interval with no adjacency proof
//! (RFC "Change discovery by fragment identity"): fragments paired by id, live
//! rows paired by `_rowid` and compared by stamp, every emitted change checked
//! against the exact merge under `change_lineage = verify`.

use std::collections::HashMap;

use datafusion::prelude::{Expr, col, lit};
use lance::Dataset;
use lance::dataset::fragment::FileFragment;
use lance_table::format::{Fragment, RowIdMeta};
use lance_table::rowids::read_row_ids;
use lance_table::rowids::version::{RowDatasetVersionMeta, read_dataset_versions};

use super::candidate_scan::CandidatePlan;
use super::enumerate::Emit;
use super::row_compare::rows_equal;
use crate::error::Result;
use crate::table_store::{
    deletion_file_identical, fragment_data_identical, fragment_deletion_vector,
};

/// Largest `_rowid IN (...)` list one side's key scan takes; beyond it the
/// interval falls back to the exact ordered merge.
const ROW_IDENTITY_CANDIDATE_MAX: usize = 65_536;
/// Largest number of live rows whose stamps are held in memory per interval.
const ROW_IDENTITY_ROWS_MAX: usize = 8 * 1024 * 1024;

/// Candidate discovery for an interval with no adjacency proof: pair the two
/// manifests' fragments by id, skip the pairs with identical files and
/// deletion file, pair the live rows of the rest by `_rowid`; a pair with
/// equal `_row_last_updated_at_version` stamps is the same unchanged row.
///
/// The stamp a commit writes is its read manifest's version plus one, so two
/// sibling first commits after one fork stamp a row they both change with
/// the same value: equality proves a shared row only when one side's lineage
/// passed through the other, which the caller decides (`pair_rows_by_identity`
/// is `false` for a diff of diverged heads). The lineage facts this rule
/// rests on are pinned by `lance_surface_guards::row_identity_discovery_*`.
pub(crate) async fn row_identity_plan(
    from_dataset: &Dataset,
    to_dataset: &Dataset,
) -> Result<Option<CandidatePlan>> {
    let from_fragments: HashMap<u64, FileFragment> = from_dataset
        .get_fragments()
        .into_iter()
        .map(|fragment| (fragment.metadata().id, fragment))
        .collect();
    let to_fragments: HashMap<u64, FileFragment> = to_dataset
        .get_fragments()
        .into_iter()
        .map(|fragment| (fragment.metadata().id, fragment))
        .collect();

    let mut metadata_steps = 0u64;
    let mut from_touched: Vec<Fragment> = Vec::new();
    let mut to_touched: Vec<Fragment> = Vec::new();
    let mut from_rows: HashMap<u64, u64> = HashMap::new();
    let mut to_rows: HashMap<u64, u64> = HashMap::new();

    for (id, to_fragment) in &to_fragments {
        metadata_steps += 1;
        let paired = from_fragments.get(id);
        if let Some(from_fragment) = paired {
            if fragment_data_identical(
                from_dataset,
                from_fragment.metadata(),
                to_dataset,
                to_fragment.metadata(),
            ) && deletion_file_identical(from_fragment.metadata(), to_fragment.metadata())
            {
                continue;
            }
        }
        if !live_row_stamps(to_fragment, &mut to_rows).await? {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return Ok(None);
        }
        to_touched.push(to_fragment.metadata().clone());
        if let Some(from_fragment) = paired {
            if !live_row_stamps(from_fragment, &mut from_rows).await? {
                crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
                return Ok(None);
            }
            from_touched.push(from_fragment.metadata().clone());
        }
        if from_rows.len() + to_rows.len() > ROW_IDENTITY_ROWS_MAX {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return Ok(None);
        }
    }
    for (id, from_fragment) in &from_fragments {
        if to_fragments.contains_key(id) {
            continue;
        }
        metadata_steps += 1;
        if !live_row_stamps(from_fragment, &mut from_rows).await? {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return Ok(None);
        }
        from_touched.push(from_fragment.metadata().clone());
        if from_rows.len() + to_rows.len() > ROW_IDENTITY_ROWS_MAX {
            crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);
            return Ok(None);
        }
    }
    crate::instrumentation::record_candidate_fragment_metadata_steps(metadata_steps);

    let mut from_candidates: Vec<u64> = Vec::new();
    let mut to_candidates: Vec<u64> = Vec::new();
    for (row_id, to_stamp) in &to_rows {
        match from_rows.get(row_id) {
            Some(from_stamp) if from_stamp == to_stamp => {}
            Some(_) => {
                from_candidates.push(*row_id);
                to_candidates.push(*row_id);
            }
            None => to_candidates.push(*row_id),
        }
    }
    for row_id in from_rows.keys() {
        if !to_rows.contains_key(row_id) {
            from_candidates.push(*row_id);
        }
    }
    if from_candidates.len() > ROW_IDENTITY_CANDIDATE_MAX
        || to_candidates.len() > ROW_IDENTITY_CANDIDATE_MAX
    {
        return Ok(None);
    }
    from_candidates.sort_unstable();
    to_candidates.sort_unstable();
    let from_row_ids = (from_candidates.len() < from_rows.len()).then_some(from_candidates);
    let to_row_ids = (to_candidates.len() < to_rows.len()).then_some(to_candidates);
    from_touched.sort_by_key(|fragment| fragment.id);
    to_touched.sort_by_key(|fragment| fragment.id);
    Ok(Some(CandidatePlan::RowIdentity {
        from_fragments: from_touched,
        from_row_ids,
        to_fragments: to_touched,
        to_row_ids,
    }))
}

/// Append one fragment's live `(_rowid, _row_last_updated_at_version)` pairs,
/// decoded from its inline metadata with the deletion vector applied; `false`
/// when either sequence is absent, spilled, undecodable or short.
async fn live_row_stamps(fragment: &FileFragment, rows: &mut HashMap<u64, u64>) -> Result<bool> {
    let meta = fragment.metadata();
    let Some(RowIdMeta::Inline(row_id_bytes)) = meta.row_id_meta.as_ref() else {
        return Ok(false);
    };
    let Some(RowDatasetVersionMeta::Inline(stamp_bytes)) =
        meta.last_updated_at_version_meta.as_ref()
    else {
        return Ok(false);
    };
    let Ok(mut row_ids) = read_row_ids(&row_id_bytes[..]) else {
        return Ok(false);
    };
    let Ok(mut stamps) = read_dataset_versions(&stamp_bytes[..]) else {
        return Ok(false);
    };
    let physical_rows = meta.physical_rows.map(|rows| rows as u64);
    if physical_rows != Some(row_ids.len()) || physical_rows != Some(stamps.len()) {
        return Ok(false);
    }
    if meta.deletion_file.is_some() {
        let deletions = fragment_deletion_vector(fragment).await?;
        let positions: Vec<u32> = deletions.to_sorted_iter().collect();
        if row_ids.mask(positions.iter().copied()).is_err()
            || stamps.mask(positions.iter().copied()).is_err()
        {
            return Ok(false);
        }
    }
    if row_ids.len() != stamps.len() {
        return Ok(false);
    }
    rows.extend(row_ids.iter().zip(stamps.versions()));
    Ok(true)
}

pub(crate) fn row_id_filter(row_ids: Option<Vec<u64>>) -> Option<Expr> {
    row_ids.map(|ids| {
        if ids.is_empty() {
            lit(false)
        } else {
            col("_rowid").in_list(ids.into_iter().map(lit).collect(), false)
        }
    })
}

/// The exact merge (`expected`) and the row-identity emitter (`actual`) must
/// yield the same change: the same end, operation, id and images; the first
/// disagreement refuses the page.
pub(crate) async fn verify_emit_agreement(
    type_key: &str,
    parent: &Dataset,
    child: &Dataset,
    expected: Option<&Emit>,
    actual: Option<&Emit>,
) -> Result<()> {
    let divergence = |detail: String| {
        crate::error::OmniError::manifest_internal(format!(
            "row-identity change discovery verify divergence for '{type_key}': {detail}; set \
             change_lineage = off to run the exact merge only"
        ))
    };
    let describe = |emit: Option<&Emit>| match emit {
        None => "end of interval".to_string(),
        Some(Emit::Insert(row)) => format!("insert '{}'", row.id),
        Some(Emit::Delete(row)) => format!("delete '{}'", row.id),
        Some(Emit::Update { after, .. }) => format!("update '{}'", after.id),
    };
    let (expected, actual) = match (expected, actual) {
        (None, None) => return Ok(()),
        (Some(expected), Some(actual)) => (expected, actual),
        (expected, actual) => {
            return Err(divergence(format!(
                "exact merge yields {}, row identity yields {}",
                describe(expected),
                describe(actual)
            )));
        }
    };
    let same_images = match (expected, actual) {
        (Emit::Insert(left), Emit::Insert(right)) => {
            left.id == right.id && rows_equal(child, left, child, right).await?
        }
        (Emit::Delete(left), Emit::Delete(right)) => {
            left.id == right.id && rows_equal(parent, left, parent, right).await?
        }
        (
            Emit::Update {
                before: left_before,
                after: left_after,
            },
            Emit::Update {
                before: right_before,
                after: right_after,
            },
        ) => {
            left_after.id == right_after.id
                && rows_equal(parent, left_before, parent, right_before).await?
                && rows_equal(child, left_after, child, right_after).await?
        }
        _ => false,
    };
    if same_images {
        return Ok(());
    }
    Err(divergence(format!(
        "exact merge yields {}, row identity yields {}",
        describe(Some(expected)),
        describe(Some(actual))
    )))
}

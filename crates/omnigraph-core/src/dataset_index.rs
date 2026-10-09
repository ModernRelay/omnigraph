use std::collections::HashSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::prelude::Expr;
use lance::Dataset;
use lance::dataset::scanner::Scanner;
use lance::index::DatasetIndexExt;
use lance::index::scalar::IndexDetails;
use lance_index::is_system_index;
use lance_table::format::IndexMetadata;

use crate::error::{OmniError, Result};
use crate::fts_compat::verify_index;

/// Whether a `key_col IN (...)` scan on a dataset will be served by the
/// persisted scalar (BTREE) index, or silently fall back to a full filtered
/// scan. Detection-only (metadata, no IO); the scan returns the correct rows
/// either way. Surfaced by the indexed traversal path so the silent perf
/// fallback is observable, and available to a future cost-based planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexCoverage {
    /// The column has a usable BTREE and every fragment records `physical_rows`.
    Indexed,
    /// Lance will not use the scalar index for this scan (correct, full scan).
    Degraded { reason: String },
}

/// Which FTS-index columns a filter expression reads through
/// `contains_tokens(column, ...)`. `all_columns` is the fail-closed verdict
/// for a call whose first argument is not a plain column.
#[derive(Debug, Default)]
pub struct FtsFilterDemand {
    pub all_columns: bool,
    pub columns: HashSet<String>,
}

impl FtsFilterDemand {
    pub fn from_filter(filter: &Expr) -> Self {
        let mut demand = Self::default();
        filter
            .apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && function.name() == "contains_tokens"
                {
                    match function.args.first() {
                        Some(Expr::Column(column)) => {
                            demand.columns.insert(column.name.clone());
                        }
                        // An unfamiliar expression must not bypass the gate.
                        _ => demand.all_columns = true,
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("the visitor returns Ok on every node");
        demand
    }

    pub fn merge(&mut self, other: Self) {
        self.all_columns |= other.all_columns;
        self.columns.extend(other.columns);
    }

    pub fn is_empty(&self) -> bool {
        !self.all_columns && self.columns.is_empty()
    }
}

/// Check only full-text reads, against this exact snapshot's artifacts.
/// SQL SDK filters are inspected as typed expressions, not string-matched.
pub async fn validate_full_text_scan(
    ds: &Dataset,
    scanner: &Scanner,
    columns: Option<HashSet<String>>,
) -> Result<()> {
    let filter_demand = match scanner.get_expr_filter().map_err(OmniError::storage)? {
        Some(filter) => FtsFilterDemand::from_filter(&filter),
        None => FtsFilterDemand::default(),
    };
    validate_full_text_demand(ds, columns, filter_demand).await
}

/// The validation proper, over an already-derived demand: `columns` is
/// the full-text query's column set (`Some(empty)` = every FTS column),
/// `filter_demand` what the filters read through `contains_tokens`.
pub async fn validate_full_text_demand(
    ds: &Dataset,
    columns: Option<HashSet<String>>,
    filter_demand: FtsFilterDemand,
) -> Result<()> {
    crate::instrumentation::record_fts_validation();
    let all_columns = columns.as_ref().is_some_and(HashSet::is_empty) || filter_demand.all_columns;
    let mut requested = columns.unwrap_or_default();
    requested.extend(filter_demand.columns);
    if !all_columns && requested.is_empty() {
        return Ok(());
    }
    let fields: HashSet<_> = requested
        .iter()
        .filter_map(|column| ds.schema().field(column).map(|field| field.id))
        .collect();
    // Ordinary reads return above without allocating. Keep the optional
    // storage-validation future out of every caller's scan/count state.
    Box::pin(async move {
        let indices = ds.load_indices().await.map_err(OmniError::storage)?;
        for index in indices.iter().filter(|index| {
            (is_full_text_index(index) || index.index_details.is_none())
                && (all_columns || index.fields.iter().any(|field| fields.contains(field)))
        }) {
            verify_index(ds, index).await?;
        }
        Ok(())
    })
    .await
}

pub fn is_full_text_index(index: &IndexMetadata) -> bool {
    index
        .index_details
        .as_ref()
        .is_some_and(|details| IndexDetails(details.clone()).supports_fts())
}

/// Metadata-only check (no IO) of whether `scan_edges_by_endpoint` — a
/// `key_col IN (...)` filter — on `ds` will be served by the persisted BTREE
/// on `column`, or silently fall back to a full filtered scan. Mirrors
/// Lance's own decision: scalar indices are disabled for the whole scan if
/// ANY fragment lacks `physical_rows` (lance `dataset/scanner.rs`
/// `create_filter_plan`), and are obviously unused if no BTREE on the
/// column exists. The scan is correct (returns all rows) either way — this
/// only surfaces the perf cliff so the indexed traversal can warn on it.
pub async fn key_column_index_coverage(ds: &Dataset, column: &str) -> Result<IndexCoverage> {
    let Some(field_id) = ds.schema().field(column).map(|field| field.id) else {
        return Ok(IndexCoverage::Degraded {
            reason: format!("column '{}' not in schema", column),
        });
    };
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    let btree = indices
        .iter()
        .filter(|index| !is_system_index(index))
        .filter(|index| index.fields.len() == 1 && index.fields[0] == field_id)
        .find(|index| {
            index
                .index_details
                .as_ref()
                .map(|details| details.type_url.ends_with("BTreeIndexDetails"))
                .unwrap_or(false)
        });
    let Some(btree) = btree else {
        return Ok(IndexCoverage::Degraded {
            reason: format!("no BTREE index on '{}'", column),
        });
    };
    // Same check Lance runs: a fragment missing physical_rows disables
    // scalar indices for the entire scan (all-or-nothing).
    if ds.fragments().iter().any(|f| f.physical_rows.is_none()) {
        return Ok(IndexCoverage::Degraded {
            reason: "a fragment is missing physical_rows".to_string(),
        });
    }
    // An index only covers the fragments it was built over; fragments
    // appended afterward (edge-index creation is skipped once a BTREE exists)
    // are scanned unindexed. If any CURRENT fragment is absent from the
    // index's `fragment_bitmap`, the scan is partly a full scan — so the
    // chooser must not price it as fully indexed. A `None` bitmap means Lance
    // can't report coverage; don't over-degrade in that case.
    if let Some(bitmap) = btree.fragment_bitmap.as_ref() {
        let uncovered = ds
            .fragments()
            .iter()
            .filter(|f| !bitmap.contains(f.id as u32))
            .count();
        if uncovered > 0 {
            return Ok(IndexCoverage::Degraded {
                reason: format!(
                    "{} fragment(s) not covered by the index on '{}'",
                    uncovered, column
                ),
            });
        }
    }
    Ok(IndexCoverage::Indexed)
}

/// True if any non-system index on `ds` leaves at least one current
/// fragment uncovered, i.e. rows that the index does not yet account for
/// (appended after the index was built, or rewritten by compaction). Such
/// fragments are scanned unindexed until index maintenance covers them
/// (full-text requires explicit rebuilding). Returns false when every index covers every fragment, or
/// when the table has no (non-system) indices to optimize. A `None`
/// `fragment_bitmap` means Lance cannot report coverage for that index, so
/// we do not treat it as uncovered (mirrors `key_column_index_coverage`).
///
/// This reports physical coverage, not whether ordinary optimize can fix it.
pub async fn has_unindexed_fragments(ds: &Dataset) -> Result<bool> {
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    let frag_ids: Vec<u32> = ds.fragments().iter().map(|f| f.id as u32).collect();
    for index in indices.iter() {
        if is_system_index(index) {
            continue;
        }
        if let Some(bitmap) = index.fragment_bitmap.as_ref() {
            if frag_ids.iter().any(|id| !bitmap.contains(*id)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub async fn user_indices_for_column(ds: &Dataset, column: &str) -> Result<Vec<IndexMetadata>> {
    let field_id = ds
        .schema()
        .field(column)
        .map(|field| field.id)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "dataset is missing expected index column '{}'",
                column
            ))
        })?;
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    Ok(indices
        .iter()
        .filter(|index| !is_system_index(index))
        .filter(|index| index.fields.len() == 1 && index.fields[0] == field_id)
        .cloned()
        .collect())
}

pub async fn has_btree_index_on(ds: &Dataset, column: &str) -> Result<bool> {
    let indices = user_indices_for_column(ds, column).await?;
    Ok(indices.iter().any(|index| {
        index
            .index_details
            .as_ref()
            .map(|details| details.type_url.ends_with("BTreeIndexDetails"))
            .unwrap_or(false)
    }))
}

pub async fn has_fts_index_on(ds: &Dataset, column: &str) -> Result<bool> {
    let indices = user_indices_for_column(ds, column).await?;
    Ok(indices.iter().any(|index| {
        index
            .index_details
            .as_ref()
            .map(|details| IndexDetails(details.clone()).supports_fts())
            .unwrap_or(false)
    }))
}

/// Whether `index` is an untrained full-text segment: an empty fragment
/// bitmap and no postings. It only declares a declared column's analyzer,
/// which Lance applies to every row no segment covers, and indexes no row.
pub fn is_untrained_full_text(index: &IndexMetadata) -> bool {
    is_full_text_index(index)
        && index
            .fragment_bitmap
            .as_ref()
            .is_some_and(|bitmap| bitmap.is_empty())
}

/// Whether a full-text segment on `column` holds postings: it covers a
/// fragment, or its coverage is unknown ([`is_untrained_full_text`] does not).
pub async fn has_fts_postings_on(ds: &Dataset, column: &str) -> Result<bool> {
    let indices = user_indices_for_column(ds, column).await?;
    Ok(indices
        .iter()
        .any(|index| is_full_text_index(index) && !is_untrained_full_text(index)))
}

pub async fn has_vector_index_on(ds: &Dataset, column: &str) -> Result<bool> {
    let indices = user_indices_for_column(ds, column).await?;
    Ok(indices.iter().any(|index| {
        index
            .index_details
            .as_ref()
            .map(|details| IndexDetails(details.clone()).is_vector())
            .unwrap_or(false)
    }))
}

use std::collections::{BTreeMap, BTreeSet, HashSet};

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

/// The current fragments that the segments of one index cover together.
///
/// Lance stores an index as one or more segments under one name and serves a
/// scan from the union of their fragment bitmaps: `scalar_index_info` unions
/// each `(column, name)` entry and `unindexed_fragments` does the same. This is
/// the one place Omnigraph computes that union, for the planner's index facts,
/// the run-time key-column check and maintenance's lag reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentCoverage {
    /// Current fragments that a segment reporting its bitmap covers.
    pub covered: BTreeSet<u64>,
    /// Whether every segment reports its bitmap. When one does not, `covered`
    /// is a lower bound and Omnigraph treats the coverage as unknown. Each
    /// caller decides what unknown means for its decision.
    ///
    /// For scalar search this is more conservative than Lance 13.
    /// `unindexed_fragments` errors on such a segment, but
    /// `scalar_index_info` first filters out every segment whose bitmap is
    /// absent, empty, or holds no current fragment, except a full-text
    /// segment, and serves the union of the others. So a BTREE with one
    /// complete segment and one segment without a bitmap is used by Lance
    /// but is unknown (and run-time `Degraded`) here. Only a full-text
    /// segment without a bitmap makes Lance's own entry unknown. The
    /// difference is cost only.
    pub known: bool,
}

impl SegmentCoverage {
    pub fn of<'a>(
        segments: impl IntoIterator<Item = &'a IndexMetadata>,
        current: &BTreeSet<u64>,
    ) -> Self {
        let mut coverage = Self {
            covered: BTreeSet::new(),
            known: true,
        };
        for segment in segments {
            match segment.fragment_bitmap.as_ref() {
                Some(bitmap) => coverage.covered.extend(
                    bitmap
                        .iter()
                        .map(u64::from)
                        .filter(|fragment| current.contains(fragment)),
                ),
                None => coverage.known = false,
            }
        }
        coverage
    }

    /// Known coverage of every fragment in `current`.
    pub fn covers_every(&self, current: &BTreeSet<u64>) -> bool {
        self.known && self.covered.len() == current.len()
    }
}

/// The ids of the dataset's current fragments.
pub fn current_fragments(ds: &Dataset) -> BTreeSet<u64> {
    ds.fragments().iter().map(|fragment| fragment.id).collect()
}

/// Whether `index` is a BTREE, by the plugin its details name. Details Lance
/// cannot resolve to a plugin are not a BTREE, whatever their type URL says.
pub fn is_btree_index(index: &IndexMetadata) -> bool {
    index.index_details.as_ref().is_some_and(|details| {
        IndexDetails(details.clone())
            .get_plugin()
            .is_ok_and(|plugin| plugin.name() == "BTree")
    })
}

/// Metadata-only check (no IO) of whether a `key_col IN (...)` filter on `ds`
/// will be served by persisted BTREE coverage of the whole table, or read
/// some fragments by a full filtered scan. The scan returns the correct rows
/// either way; this decides cost and surfaces the fallback.
///
/// It is the planner's rule (`IndexFact::fully_covers_btree` over every
/// index keyed on the column, built in the engine's `plan_source::indexes`),
/// so a run that has to probe coverage decides as a plan would: every index
/// keyed on `column` is a BTREE whose segments together cover every current
/// fragment, coverage is known, the table has a fragment, and every fragment
/// records `physical_rows` (Lance disables scalar indices for the whole scan
/// otherwise, `dataset/scanner.rs` `create_filter_plan`).
pub async fn key_column_index_coverage(ds: &Dataset, column: &str) -> Result<IndexCoverage> {
    let Some(field_id) = ds.schema().field(column).map(|field| field.id) else {
        return Ok(IndexCoverage::Degraded {
            reason: format!("column '{}' not in schema", column),
        });
    };
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    Ok(key_column_coverage(
        &indices,
        field_id,
        column,
        &current_fragments(ds),
        ds.fragments()
            .iter()
            .all(|fragment| fragment.physical_rows.is_some()),
    ))
}

/// [`key_column_index_coverage`] over already-loaded metadata.
pub fn key_column_coverage(
    indices: &[IndexMetadata],
    field_id: i32,
    column: &str,
    current: &BTreeSet<u64>,
    physical_rows_known: bool,
) -> IndexCoverage {
    let degraded = |reason: String| IndexCoverage::Degraded { reason };
    let mut by_name = BTreeMap::<&str, Vec<&IndexMetadata>>::new();
    for index in indices
        .iter()
        .filter(|index| !is_system_index(index) && index.keyed_field() == Some(field_id))
    {
        by_name.entry(index.name.as_str()).or_default().push(index);
    }
    if !by_name
        .values()
        .any(|segments| segments.iter().all(|segment| is_btree_index(segment)))
    {
        return degraded(format!("no BTREE index on '{}'", column));
    }
    if !physical_rows_known {
        return degraded("a fragment is missing physical_rows".to_string());
    }
    if current.is_empty() {
        return degraded(format!("no fragment to index on '{}'", column));
    }
    for (name, segments) in by_name {
        if !segments.iter().all(|segment| is_btree_index(segment)) {
            return degraded(format!("index '{}' on '{}' is not a BTREE", name, column));
        }
        let coverage = SegmentCoverage::of(segments, current);
        if !coverage.known {
            return degraded(format!(
                "index '{}' on '{}' does not report its fragment coverage",
                name, column
            ));
        }
        if !coverage.covers_every(current) {
            return degraded(format!(
                "{} fragment(s) not covered by the index '{}' on '{}'",
                current.len() - coverage.covered.len(),
                name,
                column
            ));
        }
    }
    IndexCoverage::Indexed
}

/// True if any non-system index on `ds` leaves at least one current
/// fragment uncovered by all of its segments together, i.e. rows that the
/// index does not yet account for (appended after the index was built, or
/// rewritten by compaction). Such fragments are scanned unindexed until index
/// maintenance covers them (full-text requires explicit rebuilding). Returns
/// false when every index covers every fragment, or when the table has no
/// (non-system) indices to optimize. Unknown coverage (a segment without a
/// fragment bitmap) is not evidence of uncovered rows, as for the fold's lag.
///
/// This reports physical coverage, not whether ordinary optimize can fix it.
pub async fn has_unindexed_fragments(ds: &Dataset) -> Result<bool> {
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    let current = current_fragments(ds);
    let mut by_name = BTreeMap::<&str, Vec<&IndexMetadata>>::new();
    for index in indices.iter().filter(|index| !is_system_index(index)) {
        by_name.entry(index.name.as_str()).or_default().push(index);
    }
    Ok(by_name.into_values().any(|segments| {
        let coverage = SegmentCoverage::of(segments, &current);
        coverage.known && !coverage.covers_every(&current)
    }))
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

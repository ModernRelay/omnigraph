use std::collections::HashSet;

use arrow_array::{Array, RecordBatch};
use datafusion::prelude::Expr;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream, Scanner};
use lance_datafusion::exec::ExecutionStatsCallback;
use lance_index::scalar::FullTextSearchQuery;
use omnigraph_core::dataset_index::{
    FtsFilterDemand, user_indices_for_column, validate_full_text_demand, validate_full_text_scan,
};
use omnigraph_core::error::{OmniError, Result};

pub(crate) use omnigraph_core::dataset_index::IndexCoverage;

/// The scan configuration v1 sets after projection, filter and ordering.
pub(crate) struct ScanTuning<'a> {
    scanner: &'a mut Scanner,
    full_text_columns: Option<HashSet<String>>,
    /// FTS-index demand of the typed filters set through this surface, unioned
    /// over every `filter_expr` call although Lance keeps only the last filter.
    filter_demand: FtsFilterDemand,
}

impl ScanTuning<'_> {
    pub(crate) fn filter_expr(&mut self, filter: Expr) -> &mut Self {
        self.filter_demand
            .merge(FtsFilterDemand::from_filter(&filter));
        self.scanner.filter_expr(filter);
        self
    }

    pub(crate) fn prefilter(&mut self, should_prefilter: bool) -> &mut Self {
        self.scanner.prefilter(should_prefilter);
        self
    }

    pub(crate) fn full_text_search(
        &mut self,
        query: FullTextSearchQuery,
    ) -> std::result::Result<&mut Self, lance::Error> {
        let columns = if query.query.is_missing_column() {
            HashSet::new()
        } else {
            query.columns()
        };
        self.scanner.full_text_search(query)?;
        self.full_text_columns = Some(columns);
        Ok(self)
    }

    pub(crate) fn nearest(
        &mut self,
        column: &str,
        query: &dyn Array,
        limit: usize,
    ) -> std::result::Result<&mut Self, lance::Error> {
        self.scanner.nearest(column, query, limit)?;
        Ok(self)
    }

    pub(crate) fn maximum_nprobes(&mut self, n: usize) -> &mut Self {
        self.scanner.maximum_nprobes(n);
        self
    }

    pub(crate) fn use_index(&mut self, use_index: bool) -> &mut Self {
        self.scanner.use_index(use_index);
        self
    }

    pub(crate) fn scan_stats_callback(&mut self, callback: ExecutionStatsCallback) -> &mut Self {
        self.scanner.scan_stats_callback(callback);
        self
    }

    pub(crate) fn target_parallelism(&mut self, target_parallelism: usize) -> &mut Self {
        self.scanner.target_parallelism(target_parallelism);
        self
    }
}

/// v1's scan helpers over a Lance dataset opened from the catalog snapshot.
pub(crate) struct TableStore;

impl TableStore {
    pub(crate) fn scan_stream_with<F>(
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
        configure: F,
    ) -> BoxFuture<'static, Result<DatasetRecordBatchStream>>
    where
        F: FnOnce(&mut ScanTuning<'_>) -> Result<()>,
    {
        let prepared = Self::configure(ds, projection, filter, order_by, with_row_id, configure);
        let dataset = ds.clone();
        let has_sql_filter = filter.is_some();
        Box::pin(async move {
            let (scanner, full_text_columns, filter_demand) = prepared?;
            if has_sql_filter {
                validate_full_text_scan(&dataset, &scanner, full_text_columns).await?;
            } else if full_text_columns.is_some() || !filter_demand.is_empty() {
                validate_full_text_demand(&dataset, full_text_columns, filter_demand).await?;
            }
            scanner.try_into_stream().await.map_err(OmniError::storage)
        })
    }

    fn configure<F>(
        ds: &Dataset,
        projection: Option<&[&str]>,
        filter: Option<&str>,
        order_by: Option<Vec<ColumnOrdering>>,
        with_row_id: bool,
        configure: F,
    ) -> Result<(Scanner, Option<HashSet<String>>, FtsFilterDemand)>
    where
        F: FnOnce(&mut ScanTuning<'_>) -> Result<()>,
    {
        let mut scanner = ds.scan();
        if with_row_id {
            scanner.with_row_id();
        }
        if let Some(columns) = projection {
            scanner.project(columns).map_err(OmniError::storage)?;
        }
        if let Some(filter_sql) = filter {
            scanner.filter(filter_sql).map_err(OmniError::storage)?;
        }
        if let Some(ordering) = order_by {
            scanner
                .order_by(Some(ordering))
                .map_err(OmniError::storage)?;
        }
        let mut tuning = ScanTuning {
            scanner: &mut scanner,
            full_text_columns: None,
            filter_demand: FtsFilterDemand::default(),
        };
        configure(&mut tuning)?;
        let full_text_columns = tuning.full_text_columns;
        let filter_demand = tuning.filter_demand;
        Ok((scanner, full_text_columns, filter_demand))
    }

    /// Indexed neighbor lookup: the edge rows whose `key_col` is one of `keys`,
    /// projected to `[key_col, opposite_col]`. Empty `keys` scans nothing.
    pub(crate) async fn scan_edges_by_endpoint(
        ds: &Dataset,
        key_col: &str,
        opposite_col: &str,
        keys: &[String],
    ) -> Result<Vec<RecordBatch>> {
        Self::scan_edges_by_endpoint_projected(ds, key_col, opposite_col, &[], keys).await
    }

    pub(crate) async fn scan_edges_by_endpoint_projected(
        ds: &Dataset,
        key_col: &str,
        opposite_col: &str,
        extra_cols: &[&str],
        keys: &[String],
    ) -> Result<Vec<RecordBatch>> {
        use datafusion::prelude::{col, lit};

        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut projection: Vec<&str> = Vec::with_capacity(2 + extra_cols.len());
        projection.push(key_col);
        projection.push(opposite_col);
        projection.extend(
            extra_cols
                .iter()
                .copied()
                .filter(|extra| *extra != key_col && *extra != opposite_col),
        );
        let key_list: Vec<Expr> = keys.iter().map(|k| lit(k.clone())).collect();
        let filter_expr = col(key_col).in_list(key_list, false);
        Self::scan_stream_with(
            ds,
            Some(projection.as_slice()),
            None,
            None,
            false,
            |scanner| {
                scanner.filter_expr(filter_expr);
                Ok(())
            },
        )
        .await?
        .try_collect()
        .await
        .map_err(OmniError::storage)
    }

    pub(crate) async fn key_column_index_coverage(
        ds: &Dataset,
        column: &str,
    ) -> Result<IndexCoverage> {
        omnigraph_core::dataset_index::key_column_index_coverage(ds, column).await
    }

    /// Whether the FTS index entries on `column` together cover every current
    /// fragment of `ds`; `false` whenever coverage cannot be proven.
    pub(crate) async fn fts_covers_all_fragments(ds: &Dataset, column: &str) -> Result<bool> {
        let indices = user_indices_for_column(ds, column).await?;
        let fts_bitmaps: Vec<_> = indices
            .iter()
            .filter(|index| omnigraph_core::dataset_index::is_full_text_index(index))
            .filter_map(|index| index.fragment_bitmap.as_ref())
            .collect();
        if fts_bitmaps.is_empty() {
            return Ok(false);
        }
        Ok(ds.fragments().iter().all(|f| {
            fts_bitmaps
                .iter()
                .any(|bitmap| bitmap.contains(f.id as u32))
        }))
    }
}

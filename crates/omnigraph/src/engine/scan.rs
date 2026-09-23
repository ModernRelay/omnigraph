//! v2's copy of v1's node scan: the scanner configuration, the per-scan
//! ANN probe ladder, the scan-side filter lowering, the wide-batch helpers
//! (phase 4). Copied, never referenced.

use super::*;

use super::operators::memory::WorkMemory;
use arrow_schema::SchemaRef;
use datafusion::prelude::{Expr, col, lit as df_lit};
use lance_index::scalar::FullTextSearchQuery;

/// `id IN (ids)` as one structured DataFusion `Expr` — the scan-pushdown
/// shape shared by `hydrate_nodes` and the rrf prefilter gate's arm push.
/// The structured form routes the IN-list through the `id` BTREE scalar
/// index (index-search → take) rather than evaluating a string filter via
/// DataFusion `InListEval`, which is O(N×M) and was measured at 72× the
/// indexed cost on a 100k-node hop.
///
/// Likely future mechanism: Lance 11 grew
/// `Scanner::with_row_addr_prefilter(RowAddrMask)` — the caller hands the
/// scanner a precomputed row-address set directly, composing with FTS and
/// ANN, instead of an expression Lance must evaluate (BTREE probe per id,
/// re-done every query). Worth revisiting if the id→row-addr probe or the
/// gate's id-count cap (`GatePolicy::max_ids`, set where in-list
/// evaluation starts losing) ever shows up as the bottleneck: a mask built
/// from a cached id→addr mapping would lift both.
pub(super) fn id_in_list_expr(ids: &[String], id_col: &str) -> datafusion::prelude::Expr {
    let id_list: Vec<Expr> = ids.iter().map(|id| df_lit(id.clone())).collect();
    col(id_col).in_list(id_list, false)
}

/// Scan a node type under the supplied projection, filters and search mode.
/// Apply filters before search ranking, retain score columns, and widen an
/// underfilled ANN scan according to its reported probe outcomes.
pub(super) async fn execute_node_scan(
    type_name: &str,
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    search_mode: &SearchMode,
    scan_report: &mut ScanReport,
    binding_columns: Option<&NeededColumns>,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open_lance_dataset(&table_key).await?;

    let node_type = &catalog.node_types[type_name];

    let mut filter_expr = build_lance_filter_expr(filters, params, Some(&node_type.arrow_schema));

    if let Some(eligible_ids) = search_mode.eligible_ids() {
        let in_list = id_in_list_expr(eligible_ids, catalog.system_columns.id);
        filter_expr = Some(match filter_expr {
            Some(expr) => expr.and(in_list),
            None => in_list,
        });
    }

    let nearest_target = search_mode.nearest.as_ref().map(|target| {
        (
            target.property.clone(),
            Float32Array::from(target.vector.clone()),
            target.k,
        )
    });
    let ranking = search_mode.bm25.as_ref();
    let mut hoisted_fts_queries: Vec<FullTextSearchQuery> = Vec::new();
    for filter in filters {
        let Some(query) = search_filter_query(filter, params)? else {
            continue;
        };
        let ranked_matches_only = ranking.is_some_and(|target| {
            search_filter_is_ranking(filter, &target.property, &target.text, params)
        });
        if !ranked_matches_only {
            hoisted_fts_queries.push(query);
        }
    }
    let (fts_query, member_ids) = match (ranking, conjoin_fts_queries(hoisted_fts_queries)) {
        (
            Some(Bm25Target {
                property: prop,
                text,
            }),
            filter_query,
        ) => {
            let ids = match filter_query {
                Some(query) => Some(
                    search_filter_member_ids(
                        &ds,
                        filter_expr.as_ref(),
                        query,
                        catalog.system_columns.id,
                        memory,
                    )
                    .await?,
                ),
                None => None,
            };
            let ranking_query = FullTextSearchQuery::new(text.clone())
                .with_column(prop.clone())
                .map_err(|error| OmniError::storage_context("fts with_column", error))?;
            (Some(ranking_query), ids)
        }
        (None, filter_query) => (filter_query, None),
    };
    if let Some(ids) = &member_ids {
        let in_list = id_in_list_expr(ids, catalog.system_columns.id);
        filter_expr = Some(match filter_expr {
            Some(expr) => expr.and(in_list),
            None => in_list,
        });
    }
    let columns = ScanColumns::new(
        node_type,
        SearchColumns {
            distance: nearest_target.is_some(),
            score: fts_query.is_some(),
        },
        binding_columns,
    );
    let has_blobs = columns.has_blobs;
    let read_projection = columns.read_projection();
    let projection = read_projection.as_deref();
    let scan_proven_empty =
        search_mode.answer_proven_empty || member_ids.as_ref().is_some_and(Vec::is_empty);
    if !scan_proven_empty {
        crate::instrumentation::record_node_scan_projection(projection);
    }
    let mut probe_budget: Option<usize> = nearest_target.as_ref().and(search_mode.ann_probe_budget);
    let known_matches: Option<usize> = nearest_target
        .as_ref()
        .and_then(|_| search_mode.eligible_ids())
        .map(<[String]>::len);
    let dataset_rows: Option<u64> = match nearest_target.as_ref() {
        Some(_) if !scan_proven_empty => Some(
            ds.count_rows(None)
                .await
                .map_err(|error| OmniError::storage_context("count_rows", error))?
                as u64,
        ),
        _ => None,
    };
    let mut use_index = match (nearest_target.as_ref(), known_matches) {
        (Some(_), _) if search_mode.nearest_exact => false,
        (Some((_, _, k)), Some(matches)) => matches > *k,
        _ => true,
    };
    let mut last_rung: Option<(usize, u64)> = None;
    let mut final_summary: Option<(Option<u64>, Option<u64>)> = None;
    let (batches, attempt_memory) = loop {
        let attempt_memory = memory
            .child("v2 scan attempt")
            .map_err(|error| memory.error(error))?;
        if scan_proven_empty {
            break (Vec::new(), attempt_memory);
        }
        let scan_stats: Arc<
            std::sync::Mutex<Option<lance_datafusion::exec::ExecutionSummaryCounts>>,
        > = Arc::new(std::sync::Mutex::new(None));
        let stats_sink = scan_stats.clone();
        let batches: Vec<RecordBatch> = Box::pin(async {
            let plan = crate::table_store::TableStore::scan_plan_with(
                &ds,
                projection,
                None,
                false,
                |scanner| {
                    if let Some(ref expr) = filter_expr {
                        scanner.filter_expr(expr.clone());
                        scanner.prefilter(true);
                    }

                    if let Some(fts_query) = &fts_query {
                        scanner
                            .full_text_search(fts_query.clone())
                            .map_err(|error| {
                                OmniError::storage_context("full_text_search", error)
                            })?;
                    }

                    if let Some((prop, query_arr, k)) = nearest_target.as_ref() {
                        {
                            scanner
                                .nearest(prop, query_arr, *k)
                                .map_err(|error| OmniError::storage_context("nearest", error))?;
                            scanner.use_index(use_index);
                            if let Some(maximum) = probe_budget {
                                scanner.maximum_nprobes(maximum);
                            }
                            crate::instrumentation::record_ann_probe_budget(probe_budget);
                            scanner.scan_stats_callback(Arc::new(move |summary| {
                                *stats_sink
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some(summary.clone());
                            }));
                            scanner.target_parallelism(1);
                        }
                    }
                    Ok(())
                },
            )
            .await?;
            let (plan, stream) = attempt_memory
                .stream(plan)
                .map_err(|error| memory.error(error))?;
            let batches = attempt_memory
                .collect(stream)
                .await
                .map_err(|error| memory.error(error))?;
            let mut summary = lance_datafusion::exec::ExecutionSummaryCounts::default();
            lance_datafusion::exec::collect_execution_metrics(plan.as_ref(), &mut summary);
            *scan_stats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(summary);
            Ok::<_, OmniError>(batches)
        })
        .await?;

        let (Some((_, _, k)), Some(type_rows)) = (nearest_target.as_ref(), dataset_rows) else {
            break (batches, attempt_memory);
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        crate::instrumentation::record_ann_scan_rows(rows as u64);
        let summary: Option<(Option<u64>, Option<u64>)> = scan_stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|summary| {
                let counter = |name: &str| summary.all_counts.get(name).map(|count| *count as u64);
                (
                    counter(lance_datafusion::utils::PARTITIONS_SEARCHED_METRIC),
                    counter(lance_datafusion::utils::PARTITIONS_RANKED_METRIC),
                )
            });
        final_summary = summary;
        if let Some((Some(searched), Some(ranked))) = summary {
            crate::instrumentation::record_ann_partition_counters(searched, ranked);
        }
        scan_report.nearest_scan = Some(NearestScanReport {
            rows,
            k: *k,
            maximum_nprobes: probe_budget,
            exhausted: !use_index || known_matches.is_some_and(|matches| rows >= matches),
            dataset_rows: type_rows,
        });
        if !use_index {
            break (batches, attempt_memory);
        }
        match ladder_step(
            rows,
            *k,
            known_matches,
            dataset_rows,
            batches_hold_infinite_distance(&batches),
            summary,
            probe_budget,
            last_rung,
        ) {
            LadderStep::Stop => break (batches, attempt_memory),
            LadderStep::FlatRescan => {
                tracing::debug!(
                    variable,
                    k = *k,
                    rows,
                    "nearest scan holds prefilter-admitted rows at +inf distance; rescanning as the flat exact kNN"
                );
                crate::instrumentation::record_ann_rescan();
                crate::instrumentation::record_ann_flat_rescan();
                use_index = false;
                probe_budget = None;
            }
            LadderStep::RescanUncapped {
                summary_missing: true,
            } => {
                tracing::warn!(
                    variable,
                    k = *k,
                    rows,
                    maximum_nprobes = ?probe_budget,
                    summary_fired = summary.is_some(),
                    "nearest scan short under its probe cap without Lance's partition counters; rescanning uncapped"
                );
                crate::instrumentation::record_ann_summary_missing();
                crate::instrumentation::record_ann_rescan();
                probe_budget = None;
            }
            LadderStep::RescanUncapped {
                summary_missing: false,
            } => {
                tracing::warn!(
                    variable,
                    k = *k,
                    rows,
                    partition_counters = ?summary,
                    maximum_nprobes = ?probe_budget,
                    "nearest scan short under its probe cap; rescanning uncapped (the ladder's last rung)"
                );
                crate::instrumentation::record_ann_rescan();
                probe_budget = None;
            }
            LadderStep::Rescan(next) => {
                tracing::debug!(
                    variable,
                    k = *k,
                    rows,
                    partition_counters = ?summary,
                    maximum_nprobes = ?probe_budget,
                    next_maximum_nprobes = next,
                    "nearest scan short under its probe cap; rescanning wider"
                );
                crate::instrumentation::record_ann_rescan();
                last_rung = summary
                    .and_then(|(searched, _)| searched)
                    .map(|searched| (rows, searched));
                probe_budget = Some(next);
            }
        }
    };

    if let Some((searched, ranked)) = final_summary {
        if let Some(searched) = searched {
            memory.metric(
                lance_datafusion::utils::PARTITIONS_SEARCHED_METRIC,
                searched as usize,
            );
        }
        if let Some(ranked) = ranked {
            memory.metric(
                lance_datafusion::utils::PARTITIONS_RANKED_METRIC,
                ranked as usize,
            );
        }
    }
    if search_mode.bm25.is_some() {
        crate::instrumentation::record_bm25_scan_rows(
            batches.iter().map(|b| b.num_rows() as u64).sum(),
        );
    }

    let scan_result = if batches.is_empty() {
        columns.empty_batch(node_type)
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        attempt_memory
            .concat(&schema, &batches)
            .map_err(|error| memory.error(error))?
    };
    if has_blobs {
        memory
            .grow(
                node_type
                    .blob_properties
                    .len()
                    .saturating_mul(scan_result.num_rows().saturating_mul(8).saturating_add(128)),
            )
            .map_err(|error| memory.error(error))?;
        let result = add_null_blob_columns(&scan_result, node_type)?;
        memory.hold(&result).map_err(|error| memory.error(error))?;
        return Ok(result);
    }
    Ok(scan_result)
}

fn search_filter_is_ranking(
    filter: &IRFilter,
    property: &str,
    text: &str,
    params: &ParamMap,
) -> bool {
    match &filter.left {
        IRExpr::Search { field, query } | IRExpr::MatchText { field, query } => {
            extract_property(field).as_deref() == Some(property)
                && resolve_to_string(query, params).as_deref() == Some(text)
        }
        _ => false,
    }
}

/// Filter membership without adding the filter's score to the BM25 ranking.
async fn search_filter_member_ids(
    dataset: &Dataset,
    filter: Option<&Expr>,
    query: FullTextSearchQuery,
    id_column: &str,
    memory: &WorkMemory,
) -> Result<Vec<String>> {
    let plan = crate::table_store::TableStore::scan_plan_with(
        dataset,
        Some(&[id_column]),
        None,
        false,
        |scanner| {
            if let Some(filter) = filter {
                scanner.filter_expr(filter.clone());
                scanner.prefilter(true);
            }
            scanner
                .full_text_search(query)
                .map_err(|error| OmniError::storage_context("full_text_search", error))?;
            Ok(())
        },
    )
    .await?;
    let work = memory
        .child("search filter membership")
        .map_err(|error| memory.error(error))?;
    let (_, stream) = work.stream(plan).map_err(|error| memory.error(error))?;
    let batches = work
        .collect(stream)
        .await
        .map_err(|error| memory.error(error))?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name(id_column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal("search membership has no identity column")
            })?;
        memory
            .entries::<String>(column.len())
            .map_err(|error| memory.error(error))?;
        for id in column.iter().flatten() {
            memory.grow(id.len()).map_err(|error| memory.error(error))?;
            ids.push(id.to_owned());
        }
    }
    Ok(ids)
}

/// Every search predicate of one scan as the one full-text query a Lance
/// scanner takes: a lone query as is, several as a boolean query every
/// member of which must match (a scanner keeps its last `full_text_search`
/// only, so two calls would drop all but the last predicate).
pub(super) fn conjoin_fts_queries(
    queries: Vec<lance_index::scalar::FullTextSearchQuery>,
) -> Option<lance_index::scalar::FullTextSearchQuery> {
    use lance_index::scalar::inverted::query::{BooleanQuery, Occur};
    match queries.len() {
        0 => None,
        1 => queries.into_iter().next(),
        _ => Some(lance_index::scalar::FullTextSearchQuery::new_query(
            BooleanQuery::new(queries.into_iter().map(|query| (Occur::Must, query.query))).into(),
        )),
    }
}

/// The columns one node scan reads and emits. Blob columns are excluded from
/// the scan when a filter is present (Lance: BlobsDescriptions + filter
/// trips a projection assertion) and come back as null placeholders.
pub(super) struct ScanColumns<'n> {
    pub(super) has_blobs: bool,
    pub(super) non_blob_cols: Vec<&'n str>,
    /// `_distance` under a nearest target, `_score` under a text search.
    pub(super) search_cols: Vec<&'static str>,
    /// The plan's projection (`projection_pushdown`) plus the identity, the
    /// key and the search columns; `None` reads every non-blob column.
    pub(super) pruned_cols: Option<Vec<&'n str>>,
}

#[derive(Default)]
pub(in crate::engine) struct SearchColumns {
    pub distance: bool,
    pub score: bool,
}

impl<'n> ScanColumns<'n> {
    pub(in crate::engine) fn read_projection(&self) -> Option<Vec<&'n str>> {
        self.pruned_cols.clone().or_else(|| {
            self.has_blobs.then(|| {
                self.non_blob_cols
                    .iter()
                    .copied()
                    .chain(self.search_cols.iter().copied())
                    .collect()
            })
        })
    }

    pub(super) fn new(
        node_type: &'n omnigraph_compiler::catalog::NodeType,
        search: SearchColumns,
        binding_columns: Option<&NeededColumns>,
    ) -> Self {
        let has_blobs = !node_type.blob_properties.is_empty();
        let non_blob_cols: Vec<&'n str> = node_type
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| !node_type.blob_properties.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        let mut search_cols: Vec<&'static str> = Vec::with_capacity(2);
        if search.distance {
            search_cols.push("_distance");
        }
        if search.score {
            search_cols.push("_score");
        }
        let pruned_cols: Option<Vec<&'n str>> = binding_columns.map(|NeededColumns(columns)| {
            non_blob_cols
                .iter()
                .copied()
                .filter(|name| {
                    node_type
                        .key
                        .as_ref()
                        .is_some_and(|key| key.iter().any(|k| k == name))
                        || columns.contains(*name)
                })
                .chain(search_cols.iter().copied())
                .collect()
        });
        Self {
            has_blobs,
            non_blob_cols,
            search_cols,
            pruned_cols,
        }
    }

    /// The zero-row batch of a scan that returned nothing: the read columns
    /// in schema order, then the search columns.
    pub(super) fn empty_batch(
        &self,
        node_type: &omnigraph_compiler::catalog::NodeType,
    ) -> RecordBatch {
        let mut fields: Vec<_> = node_type
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| match &self.pruned_cols {
                Some(columns) => columns.contains(&f.name().as_str()),
                None => !node_type.blob_properties.contains(f.name()),
            })
            .map(|f| f.as_ref().clone())
            .collect();
        fields.extend(
            self.search_cols
                .iter()
                .map(|col| Field::new(*col, DataType::Float32, true)),
        );
        RecordBatch::new_empty(Arc::new(Schema::new(fields)))
    }
}

/// The schema `execute_node_scan` + `prefix_batch` produce for one scan,
/// known before it runs: what `ScanExec` declares.
pub(super) fn scan_output_schema(
    type_name: &str,
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    catalog: &Catalog,
    search_mode: &SearchMode,
    binding_columns: Option<&NeededColumns>,
) -> Result<SchemaRef> {
    let node_type = catalog
        .node_types
        .get(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
    let nearest = search_mode.nearest.is_some();
    let scores_fts = search_mode.bm25.is_some()
        || filters
            .iter()
            .filter(|filter| is_search_filter(filter))
            .any(|filter| build_fts_query(&filter.left, params).is_some());
    let columns = ScanColumns::new(
        node_type,
        SearchColumns {
            distance: nearest,
            score: scores_fts,
        },
        binding_columns,
    );
    let mut empty = columns.empty_batch(node_type);
    if columns.has_blobs {
        empty = add_null_blob_columns(&empty, node_type)?;
    }
    Ok(prefix_batch(&empty, variable)?.schema())
}

/// Add null Utf8 columns for blob properties excluded from a scan.
/// Uses column_by_name (not positional) so it's order-independent, and
/// silently skips non-blob fields absent from the batch — LOAD-BEARING for
/// pruned scans (#564), which legitimately omit undemanded non-blob columns.
/// Every column the scan produced beside the catalog's rides through after
/// the catalog columns, as the no-blob path (batch passed through untouched)
/// already delivers it: a search scan's `_distance`/`_score` must survive
/// this rebuild because the planned `Sort` leads with it.
pub(super) fn add_null_blob_columns(
    batch: &RecordBatch,
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let batch_schema = batch.schema();
    let mut fields = Vec::with_capacity(node_type.arrow_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(node_type.arrow_schema.fields().len());

    for field in node_type.arrow_schema.fields() {
        if node_type.blob_properties.contains(field.name()) {
            fields.push(Field::new(field.name(), DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![None::<&str>; num_rows])));
        } else if let Some(col) = batch.column_by_name(field.name()) {
            let batch_field = batch_schema
                .field_with_name(field.name())
                .map_err(OmniError::arrow_internal)?;
            fields.push(batch_field.clone());
            columns.push(col.clone());
        }
    }
    for (field, col) in batch_schema.fields().iter().zip(batch.columns()) {
        if node_type.arrow_schema.fields().find(field.name()).is_none() {
            fields.push(field.as_ref().clone());
            columns.push(col.clone());
        }
    }
    debug_assert_eq!(
        columns.len(),
        batch.num_columns()
            + node_type
                .arrow_schema
                .fields()
                .iter()
                .filter(|field| node_type.blob_properties.contains(field.name()))
                .count(),
        "add_null_blob_columns dropped or replaced a scan column"
    );

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

/// Build a FullTextSearchQuery from a search IR expression.
pub(super) fn build_fts_query(
    expr: &IRExpr,
    params: &ParamMap,
) -> Option<lance_index::scalar::FullTextSearchQuery> {
    match expr {
        IRExpr::Search { field, query } => {
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            lance_index::scalar::FullTextSearchQuery::new(q)
                .with_column(prop)
                .ok()
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            let edits = max_edits
                .as_ref()
                .and_then(|e| resolve_to_int(e, params))
                .unwrap_or(2) as u32;
            lance_index::scalar::FullTextSearchQuery::new_fuzzy(q, Some(edits))
                .with_column(prop)
                .ok()
        }
        IRExpr::MatchText { field, query } => {
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            lance_index::scalar::FullTextSearchQuery::new(q)
                .with_column(prop)
                .ok()
        }
        _ => None,
    }
}

/// Extract the property name from a PropAccess expression.
pub(super) fn extract_property(expr: &IRExpr) -> Option<String> {
    match expr {
        IRExpr::PropAccess { property, .. } => Some(property.clone()),
        _ => None,
    }
}

/// Resolve an expression to a string value (literal or param).
pub(super) fn resolve_to_string(expr: &IRExpr, params: &ParamMap) -> Option<String> {
    match expr {
        IRExpr::Literal(Literal::String(s)) => Some(s.clone()),
        IRExpr::Param(name) => match params.get(name)? {
            Literal::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Resolve an expression to an integer value (literal or param).
pub(super) fn resolve_to_int(expr: &IRExpr, params: &ParamMap) -> Option<i64> {
    match expr {
        IRExpr::Literal(Literal::Integer(n)) => Some(*n),
        IRExpr::Param(name) => match params.get(name)? {
            Literal::Integer(n) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

/// Convert IR filters to a single DataFusion `Expr` (AND-joined), or
/// `None` if no filter is pushable.
pub(super) fn build_lance_filter_expr(
    filters: &[IRFilter],
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::Expr;

    let mut acc: Option<Expr> = None;
    let mut pushed = 0u64;
    for f in filters {
        let Some(e) = ir_filter_to_expr(f, params, schema) else {
            continue;
        };
        pushed += 1;
        acc = Some(match acc {
            None => e,
            Some(prev) => Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(prev),
                Operator::And,
                Box::new(e),
            )),
        });
    }
    crate::instrumentation::record_pushed_filter_exprs(pushed);
    acc
}

/// Lower a pushable filter, matching scalar literals to the opposing column
/// type when known so the column remains indexable. The schema affects literal
/// types only; return `None` for search-mode filters and unsupported expressions.
pub(super) fn ir_filter_to_expr(
    filter: &IRFilter,
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::functions_nested::expr_fn::array_has;

    if is_search_filter(filter) {
        return None;
    }

    if matches!(filter.op, CompOp::Contains) {
        let left = ir_expr_to_expr(&filter.left, params, None)?;
        let right = ir_expr_to_expr(&filter.right, params, None)?;
        return Some(array_has(left, right));
    }

    if matches!(filter.op, CompOp::StartsWith | CompOp::StringContains) {
        use datafusion::functions::expr_fn::{contains, starts_with};
        let left = ir_expr_to_expr(&filter.left, params, None)?;
        let right = ir_expr_to_expr(&filter.right, params, None)?;
        return Some(match filter.op {
            CompOp::StartsWith => starts_with(left, right),
            _ => contains(left, right),
        });
    }

    let left_col_type = prop_data_type(&filter.left, schema);
    let right_col_type = prop_data_type(&filter.right, schema);
    let left = ir_expr_to_expr(&filter.left, params, right_col_type.as_ref())?;
    let right = ir_expr_to_expr(&filter.right, params, left_col_type.as_ref())?;
    Some(match filter.op {
        CompOp::Eq => left.eq(right),
        CompOp::Ne => left.not_eq(right),
        CompOp::Gt => left.gt(right),
        CompOp::Lt => left.lt(right),
        CompOp::Ge => left.gt_eq(right),
        CompOp::Le => left.lt_eq(right),
        CompOp::Contains | CompOp::StartsWith | CompOp::StringContains => {
            unreachable!("handled above")
        }
    })
}

/// Lower a property, literal or parameter for pushdown, preserving property case.
/// Coerce literals toward `target` when possible; return `None` for other shapes.
pub(super) fn ir_expr_to_expr(
    expr: &IRExpr,
    params: &ParamMap,
    target: Option<&arrow_schema::DataType>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::ident;
    match expr {
        IRExpr::PropAccess { property, .. } => Some(ident(property)),
        IRExpr::Literal(l) => literal_to_expr_coerced(l, target),
        IRExpr::Param(name) => params
            .get(name)
            .and_then(|l| literal_to_expr_coerced(l, target)),
        _ => None,
    }
}

/// The Arrow type of a `PropAccess` operand, looked up in the scan's schema, or
/// `None` if the expr is not a column or the schema/field is unavailable.
pub(super) fn prop_data_type(
    expr: &IRExpr,
    schema: Option<&Schema>,
) -> Option<arrow_schema::DataType> {
    match expr {
        IRExpr::PropAccess { property, .. } => schema?
            .field_with_name(property)
            .ok()
            .map(|f| f.data_type().clone()),
        _ => None,
    }
}

/// Lower a literal for pushdown, coercing it to `target` (the comparison
/// column's Arrow type) when known. Falls back to the natural-type
/// `literal_to_expr` on a missing target or any coercion failure, so a filter is
/// never demoted to `None` by coercion (a node scan has no in-memory fallback for
/// inline filters — see `execute_node_scan`).
pub(super) fn literal_to_expr_coerced(
    lit: &Literal,
    target: Option<&arrow_schema::DataType>,
) -> Option<datafusion::prelude::Expr> {
    if let Some(target) = target {
        if let Some(e) = literal_to_typed_expr(lit, target) {
            return Some(e);
        }
    }
    literal_to_expr(lit)
}

/// Build a literal as a typed Arrow scalar matching `target`, reusing the same
/// `literal_to_array` + `arrow_cast` path as the in-memory arm
/// (`projection.rs::evaluate_filter`) so the two arms agree. Returns `None` on
/// any failure (unbuildable literal, incompatible cast) — the caller then falls
/// back to the natural-type literal.
///
/// Lossless-only for integer targets: typecheck permits numeric cross-type
/// comparisons (`types_compatible`), so a fractional float or out-of-range
/// integer can reach here. Casting those to a narrower integer would truncate
/// (`2.7 -> 2`) or overflow to null, silently changing which rows match. We
/// round-trip the cast and, on mismatch, return `None` so the caller keeps the
/// natural literal — correct via DataFusion coercion, the index just goes unused
/// for that out-of-domain predicate. Float targets are exempt: narrowing
/// `F64 -> F32` is the column's own precision domain, not a value error.
pub(super) fn literal_to_typed_expr(
    lit: &Literal,
    target: &arrow_schema::DataType,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    use datafusion::scalar::ScalarValue;

    let arr = literal_to_array(lit, 1).ok()?;
    if arr.data_type() == target {
        return Some(df_lit(ScalarValue::try_from_array(&arr, 0).ok()?));
    }
    let casted = arrow_cast::cast::cast(&arr, target).ok()?;
    if target.is_integer() {
        let back = arrow_cast::cast::cast(&casted, arr.data_type()).ok()?;
        let original = ScalarValue::try_from_array(&arr, 0).ok()?;
        let round_tripped = ScalarValue::try_from_array(&back, 0).ok()?;
        if original != round_tripped {
            return None;
        }
    }
    Some(df_lit(ScalarValue::try_from_array(&casted, 0).ok()?))
}

/// Lower a scalar literal without a target column type, or refuse a list.
/// Dates remain strings for DataFusion coercion; typed scan predicates use
/// `literal_to_typed_expr` to preserve the column's indexable type.
pub(super) fn literal_to_expr(lit: &Literal) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    Some(match lit {
        Literal::Null => df_lit(datafusion::scalar::ScalarValue::Null),
        Literal::String(s) => df_lit(s.clone()),
        Literal::Integer(n) => df_lit(*n),
        Literal::Float(f) => df_lit(*f),
        Literal::Bool(b) => df_lit(*b),
        Literal::Date(s) => df_lit(s.clone()),
        Literal::DateTime(s) => df_lit(s.clone()),
        Literal::List(_) => return None,
    })
}

pub(super) fn prefix_batch(batch: &RecordBatch, variable: &str) -> Result<RecordBatch> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            Field::new(
                format!("{}.{}", variable, f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            )
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(OmniError::arrow_internal)
}

/// A column name present on both sides would let `column_by_name` silently
/// pick the left one (Arrow admits duplicate field names). The compiler's
/// plan check keeps this unreachable for lowered plans; this is the last
/// line, in every build (#605).
pub(super) fn refuse_duplicate_columns(left: &RecordBatch, right: &RecordBatch) -> Result<()> {
    let left_schema = left.schema();
    let left_names: HashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let right_schema = right.schema();
    for f in right_schema.fields() {
        if left_names.contains(f.name().as_str()) {
            return Err(OmniError::manifest_internal(format!(
                "duplicate column '{}' when joining batches",
                f.name()
            )));
        }
    }
    Ok(())
}

pub(super) fn hconcat_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    refuse_duplicate_columns(left, right)?;
    fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
    let mut columns: Vec<ArrayRef> = left.columns().to_vec();
    columns.extend(right.columns().to_vec());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

#[cfg(test)]
mod coercion_tests {
    use super::literal_to_expr_coerced;
    use arrow_schema::DataType;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use omnigraph_compiler::query::ast::Literal;

    /// GQT cannot inspect the typed literal that preserves a scan's index eligibility.
    #[test]
    fn coerced_literals_keep_lossless_integer_comparisons_and_float_precision() {
        for (literal, target, expected) in [
            (
                Literal::Float(2.7),
                Some(DataType::Int32),
                ScalarValue::Float64(Some(2.7)),
            ),
            (
                Literal::Float(2.0),
                Some(DataType::Int32),
                ScalarValue::Int32(Some(2)),
            ),
            (
                Literal::Integer(3_000_000_000),
                Some(DataType::Int32),
                ScalarValue::Int64(Some(3_000_000_000)),
            ),
            (
                Literal::Integer(5),
                Some(DataType::Int32),
                ScalarValue::Int32(Some(5)),
            ),
            (
                Literal::Float(0.1),
                Some(DataType::Float32),
                ScalarValue::Float32(Some(0.1)),
            ),
            (Literal::Integer(5), None, ScalarValue::Int64(Some(5))),
            (
                Literal::Null,
                Some(DataType::Int32),
                ScalarValue::Int32(None),
            ),
        ] {
            let expression =
                literal_to_expr_coerced(&literal, target.as_ref()).expect("scalar literal");
            let Expr::Literal(value, _) = expression else {
                panic!("expected a literal")
            };
            assert_eq!(value, expected, "{literal:?} against {target:?}");
        }
        assert!(literal_to_expr_coerced(&Literal::List(vec![Literal::Integer(1)]), None).is_none());
    }
}

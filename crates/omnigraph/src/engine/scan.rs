//! The node scan: the scanner configuration, the per-scan ANN probe ladder,
//! the scan-side filter lowering, the wide-batch helpers.

use super::*;

use super::operators::memory::WorkMemory;
use crate::instrumentation::record_node_scan_projection;
use crate::table_store::{PlannedScan, ScanTuning, TableStore};
use arrow_schema::SchemaRef;
use datafusion::prelude::{Expr, col, lit as df_lit};
use datafusion::scalar::ScalarValue;
use lance_core::ROW_ADDR;
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
pub(crate) fn id_in_list_expr(ids: &[String], id_col: &str) -> datafusion::prelude::Expr {
    let id_list: Vec<Expr> = ids.iter().map(|id| df_lit(id.clone())).collect();
    col(id_col).in_list(id_list, false)
}

/// Lance batches a pipelined read decodes ahead, which the pool does not see.
const PIPELINED_READAHEAD: usize = 2;

/// Bytes Lance may hold fetched and not yet decoded for one pipelined read,
/// which the pool does not see either. Lance's default is 32 MiB per I/O
/// thread (2 GiB on a cloud store), read ahead of a consumer that may stop
/// after its first batch; two 32 MiB pages keep its throughput.
const PIPELINED_IO_BUFFER_BYTES: u64 = 64 * 1024 * 1024;

/// One node scan resolved before any Lance read: the dataset, the pushed
/// filter (the literal filters, a gate's eligible set, a BM25 filter's member
/// ids), the hoisted full-text query and the columns it reads.
pub(super) struct NodeRead<'n> {
    ds: Dataset,
    pub(super) node_type: &'n omnigraph_compiler::catalog::NodeType,
    filter_expr: Option<Expr>,
    fts_query: Option<FullTextSearchQuery>,
    scalar_index: Option<bool>,
    pub(super) columns: ScanColumns<'n>,
    /// The gate proved the answer empty, or a BM25 filter matched no row: no
    /// Lance read runs.
    pub(super) proven_empty: bool,
    /// The pushed filter runs after the full-text search scores rows
    /// (Lance's postfilter) instead of before it.
    filter_after_scoring: bool,
}

impl<'n> NodeRead<'n> {
    fn validate(
        ds: &Dataset,
        node_type: &omnigraph_compiler::catalog::NodeType,
        table_key: &str,
        contract_covers: bool,
        filters: &[IRExpr],
        catalog: &Catalog,
        search_mode: &SearchMode,
        read_columns: &ScanColumns<'_>,
    ) -> Result<()> {
        super::typed_value::check_stored_schema(
            ds,
            &node_type.arrow_schema,
            table_key,
            contract_covers,
            read_columns
                .stored_columns()
                .into_iter()
                .chain([catalog.system_columns.id])
                .chain(
                    search_mode
                        .nearest
                        .iter()
                        .map(|target| target.property.as_str()),
                )
                .chain(
                    search_mode
                        .bm25
                        .iter()
                        .map(|target| target.property.as_str()),
                ),
        )?;
        super::typed_value::check_scan_leaves(
            ds,
            &node_type.arrow_schema,
            table_key,
            contract_covers,
            filters,
        )?;
        Ok(())
    }

    pub(super) fn for_index_split(
        ds: Dataset,
        type_name: &str,
        contract_covers: bool,
        filters: &[IRExpr],
        params: &ParamMap,
        catalog: &'n Catalog,
        binding_columns: Option<&NeededColumns>,
    ) -> Result<Self> {
        if filters.iter().any(is_search_filter) {
            return Err(OmniError::manifest_internal(
                "search predicates require runtime scan access",
            ));
        }
        let node_type = &catalog.node_types[type_name];
        let columns = ScanColumns::new(node_type, SearchColumns::default(), binding_columns);
        Self::validate(
            &ds,
            node_type,
            &format!("node:{type_name}"),
            contract_covers,
            filters,
            catalog,
            &SearchMode::default(),
            &columns,
        )?;
        Ok(Self {
            filter_expr: build_lance_filter_expr(filters, params, Some(&node_type.arrow_schema)),
            ds,
            node_type,
            fts_query: None,
            scalar_index: Some(true),
            columns,
            proven_empty: false,
            filter_after_scoring: false,
        })
    }

    pub(super) async fn resolve(
        type_name: &str,
        filters: &[IRExpr],
        params: &ParamMap,
        snapshot: &Snapshot,
        catalog: &'n Catalog,
        search_mode: &SearchMode,
        binding_columns: Option<&NeededColumns>,
        memory: &WorkMemory,
    ) -> Result<Self> {
        let table_key = format!("node:{}", type_name);
        let ds = snapshot.open_lance_dataset(&table_key).await?;

        let node_type = &catalog.node_types[type_name];
        let read_columns = ScanColumns::new(node_type, SearchColumns::default(), binding_columns);
        Self::validate(
            &ds,
            node_type,
            &table_key,
            snapshot.contract_covers(&table_key),
            filters,
            catalog,
            search_mode,
            &read_columns,
        )?;

        let mut filter_expr =
            build_lance_filter_expr(filters, params, Some(&node_type.arrow_schema));

        if let Some(eligible_ids) = search_mode.eligible_ids() {
            let in_list = id_in_list_expr(eligible_ids, catalog.system_columns.id);
            filter_expr = Some(match filter_expr {
                Some(expr) => expr.and(in_list),
                None => in_list,
            });
        }

        let ranking = search_mode.bm25.as_ref();
        let mut hoisted_fts_queries: Vec<FullTextSearchQuery> = Vec::new();
        for filter in filters {
            let Some(query) = search_filter_query(filter, params)? else {
                continue;
            };
            let ranked_matches_only = match ranking {
                Some(target) => {
                    search_filter_is_ranking(filter, &target.property, &target.text, params)?
                }
                None => false,
            };
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
                distance: search_mode.nearest.is_some(),
                score: fts_query.is_some(),
            },
            binding_columns,
        );
        let proven_empty =
            search_mode.answer_proven_empty || member_ids.as_ref().is_some_and(Vec::is_empty);
        if !proven_empty {
            record_node_scan_projection(columns.read_projection().as_deref());
        }
        Ok(Self {
            ds,
            node_type,
            filter_expr,
            fts_query,
            scalar_index: search_mode.scalar_index,
            columns,
            proven_empty,
            filter_after_scoring: search_mode.eligibility_after_scoring,
        })
    }

    fn configure(
        &self,
        scanner: &mut ScanTuning<'_>,
        pipelined_batch: Option<(usize, usize)>,
    ) -> Result<()> {
        if let Some(enabled) = self.scalar_index {
            scanner.use_scalar_index(enabled);
        }
        if let Some(expr) = &self.filter_expr {
            scanner.filter_expr(expr.clone());
            // A BM25 scan whose plan places eligibility after scoring filters
            // the scored rows instead of the candidates.
            scanner.prefilter(!(self.filter_after_scoring && self.fts_query.is_some()));
        }
        if let Some((rows, bytes)) = pipelined_batch {
            scanner.batch_size(rows);
            scanner.batch_size_bytes(bytes as u64);
            scanner.batch_readahead(PIPELINED_READAHEAD);
            scanner.io_buffer_size(PIPELINED_IO_BUFFER_BYTES);
        }
        if let Some(fts_query) = &self.fts_query {
            scanner
                .full_text_search(fts_query.clone())
                .map_err(|error| OmniError::storage_context("full_text_search", error))?;
        }
        Ok(())
    }

    pub(super) fn plan_with_filter(
        &self,
    ) -> futures::future::BoxFuture<'static, Result<PlannedScan>> {
        let projection = self.columns.read_projection();
        TableStore::scan_plan_with_filter(&self.ds, projection.as_deref(), |scanner| {
            self.configure(scanner, None)
        })
    }

    /// The Lance plan of this read: the projection, the pushed filter as a
    /// prefilter (as a postfilter on the scored rows where the plan places a
    /// BM25 scan's eligibility after scoring), the `(rows, bytes)` batch
    /// override, bounded readahead and I/O buffer of a pipelined read, the
    /// full-text query, then `configure` (nearest).
    pub(super) fn plan(
        &self,
        pipelined_batch: Option<(usize, usize)>,
        configure: impl FnOnce(&mut ScanTuning<'_>) -> Result<()>,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    > {
        let projection = self.columns.read_projection();
        TableStore::scan_plan_with(&self.ds, projection.as_deref(), None, false, |scanner| {
            self.configure(scanner, pipelined_batch)?;
            configure(scanner)
        })
    }
}

/// The breaker read of a ranked scan: scan a node type under the supplied
/// projection, filters and search mode, apply filters before search ranking,
/// retain score columns, and widen an underfilled ANN scan according to its
/// reported probe outcomes. An unranked read streams instead
/// (`operators::scan::pipelined`).
pub(super) async fn execute_node_scan(
    type_name: &str,
    variable: &str,
    filters: &[IRExpr],
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    search_mode: &SearchMode,
    scan_report: &mut ScanReport,
    binding_columns: Option<&NeededColumns>,
    memory: &WorkMemory,
) -> Result<RecordBatch> {
    let read = NodeRead::resolve(
        type_name,
        filters,
        params,
        snapshot,
        catalog,
        search_mode,
        binding_columns,
        memory,
    )
    .await?;
    let node_type = read.node_type;
    let nearest_target = search_mode.nearest.as_ref().map(|target| {
        (
            target.property.clone(),
            Float32Array::from(target.vector.clone()),
            target.k,
        )
    });
    let has_blobs = read.columns.has_blobs;
    let scan_proven_empty = read.proven_empty;
    let mut probe_budget: Option<usize> = nearest_target.as_ref().and(search_mode.ann_probe_budget);
    let known_matches: Option<usize> = nearest_target
        .as_ref()
        .and_then(|_| search_mode.eligible_ids())
        .map(<[String]>::len);
    let dataset_rows: Option<u64> = match nearest_target.as_ref() {
        Some(_) if !scan_proven_empty => Some(
            read.ds
                .count_rows(None)
                .await
                .map_err(|error| OmniError::storage_context("count_rows", error))?
                as u64,
        ),
        _ => None,
    };
    let policy = match nearest_target {
        Some(_) => Some(search_mode.nearest_policy.ok_or_else(|| {
            OmniError::manifest_internal("a nearest scan carries no declared policy")
        })?),
        None => None,
    };
    let mut use_index = match (nearest_target.as_ref(), known_matches, policy) {
        (Some(_), _, _) if search_mode.nearest_exact => false,
        (Some((_, _, k)), Some(matches), Some(policy)) => {
            !(policy.flat_when_eligible_within_fetch && matches <= *k)
        }
        _ => true,
    };
    // A vector index that does not rank exactly by squared L2 (quantized,
    // HNSW, another metric, unreadable details) is one the engine never
    // builds; using it would return approximate or differently ranked rows
    // as an exhaustive answer. Indexes are derived state, so the scan
    // answers from the flat exact kNN instead (issue #1297). The details
    // are read only when the scan would otherwise use the index.
    if use_index && !scan_proven_empty {
        if let Some((property, _, _)) = nearest_target.as_ref() {
            if !super::plan_source::indexes::vector_indexes_rank_exactly_by_l2(&read.ds, property)
                .await?
            {
                tracing::debug!(
                    variable,
                    "the vector index on the nearest column does not rank exactly by L2; \
                     scanning flat"
                );
                use_index = false;
            }
        }
    }
    scan_report.probes.clear();
    scan_report.ann_metrics.clear();
    scan_report.exact = false;
    let mut last_rung: Option<(usize, u64)> = None;
    let mut final_summary: Option<(Option<u64>, Option<u64>)> = None;
    let (batches, attempt_memory) = loop {
        let attempt_memory = memory
            .child("v2 scan attempt")
            .map_err(|error| memory.error(error))?;
        if scan_proven_empty {
            scan_report.exact = true;
            break (Vec::new(), attempt_memory);
        }
        let scan_stats: Arc<
            std::sync::Mutex<Option<lance_datafusion::exec::ExecutionSummaryCounts>>,
        > = Arc::new(std::sync::Mutex::new(None));
        let stats_sink = scan_stats.clone();
        let batches: Vec<RecordBatch> = Box::pin(async {
            let plan = read
                .plan(None, |scanner| {
                    if let Some((prop, query_arr, k)) = nearest_target.as_ref() {
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
                    }
                    Ok(())
                })
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
                if let Some(metrics) =
                    AnnAttemptMetrics::from_summary(scan_report.probes.len(), summary)
                {
                    scan_report.ann_metrics.push(metrics);
                }
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
        scan_report.probes.push(ProbeAttempt {
            maximum_nprobes: probe_budget,
            flat: !use_index,
            rows,
        });
        let unreached = batches_hold_infinite_distance(&batches);
        // The scan uses a vector index only when it is IVF_FLAT by L2 (any
        // other runs flat, above), which scores every row of a partition it
        // searches exactly, so an attempt that searched every partition
        // ranked exactly; Lance reports neither partition counter when no
        // index covers the column and it scores every row flat. A row the
        // prefilter admitted and the search did not reach comes back at
        // `+inf`, out of its place.
        let every_partition = match summary {
            Some((None, None)) => true,
            Some((Some(searched), Some(ranked))) => searched >= ranked,
            _ => false,
        };
        scan_report.exact = !use_index
            || (!unreached
                && (every_partition
                    || known_matches.is_some_and(|matches| rows >= matches)
                    || rows as u64 >= type_rows));
        if !use_index {
            break (batches, attempt_memory);
        }
        let Some(policy) = policy else {
            break (batches, attempt_memory);
        };
        match ladder_step(
            policy,
            rows,
            *k,
            known_matches,
            dataset_rows,
            unreached,
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
        read.columns.empty_batch(node_type)
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
    filter: &IRExpr,
    property: &str,
    text: &str,
    params: &ParamMap,
) -> Result<bool> {
    Ok(match search_call(filter) {
        Some(IRExpr::Search { field, query, .. } | IRExpr::MatchText { field, query, .. }) => {
            extract_property(field).as_deref() == Some(property)
                && resolve_to_string(query, params)? == text
        }
        Some(
            IRExpr::PropAccess { .. }
            | IRExpr::Nearest { .. }
            | IRExpr::Fuzzy { .. }
            | IRExpr::Bm25 { .. }
            | IRExpr::Rrf { .. }
            | IRExpr::Variable(_, _)
            | IRExpr::Param(_, _)
            | IRExpr::Literal(_, _)
            | IRExpr::Aggregate { .. }
            | IRExpr::AliasRef(_, _)
            | IRExpr::Binary { .. }
            | IRExpr::Not(_, _)
            | IRExpr::Cast { .. }
            | IRExpr::IsNull { .. },
        )
        | None => false,
    })
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

/// The columns one node scan reads and emits. Blob properties are never `.gq`
/// read values: typecheck refuses projecting, filtering, ordering or
/// aggregating one, so a `.gq` node scan leaves their descriptors unread.
/// They come back as null placeholders that keep the declared schema; Blob
/// values are read through `read_blob_at`.
pub(super) struct ScanColumns<'n> {
    pub(super) has_blobs: bool,
    pub(super) non_blob_cols: Vec<&'n str>,
    /// `_distance` under a nearest target, `_score` under a text search, then
    /// `_rowaddr` when the plan defers a return column to `HydrateColumns`.
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
    /// The stored fields this read consumes, which the opened dataset's
    /// schema must match: its projection, or every non-Blob column when it
    /// prunes none, without Lance's row address (read for a hydrated
    /// binding, never a stored field).
    pub(in crate::engine) fn stored_columns(&self) -> Vec<&'n str> {
        self.read_projection()
            .unwrap_or_else(|| self.non_blob_cols.clone())
            .into_iter()
            .filter(|column| *column != ROW_ADDR)
            .collect()
    }

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
        let mut search_cols: Vec<&'static str> = Vec::with_capacity(3);
        if search.distance {
            search_cols.push("_distance");
        }
        if search.score {
            search_cols.push("_score");
        }
        if binding_columns.is_some_and(|NeededColumns(columns)| columns.contains(ROW_ADDR)) {
            search_cols.push(ROW_ADDR);
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
        fields.extend(self.search_cols.iter().map(|col| {
            let data_type = if *col == ROW_ADDR {
                DataType::UInt64
            } else {
                DataType::Float32
            };
            Field::new(*col, data_type, true)
        }));
        RecordBatch::new_empty(Arc::new(Schema::new(fields)))
    }
}

/// The schema `execute_node_scan` + `prefix_batch` produce for one scan,
/// known before it runs: what `ScanExec` declares.
pub(super) fn scan_output_schema(
    type_name: &str,
    variable: &str,
    filters: &[IRExpr],
    params: &ParamMap,
    catalog: &Catalog,
    search_mode: &SearchMode,
    binding_columns: Option<&NeededColumns>,
) -> Result<SchemaRef> {
    let node_type = catalog
        .query_node_type(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
    let nearest = search_mode.nearest.is_some();
    let mut scores_fts = search_mode.bm25.is_some();
    for filter in filters {
        scores_fts |= search_filter_query(filter, params)?.is_some();
    }
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

/// Build a full-text query, refusing invalid supplied constant values.
pub(super) fn build_fts_query(
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<Option<lance_index::scalar::FullTextSearchQuery>> {
    let (prop, query) = match expr {
        IRExpr::Search { field, query, .. } | IRExpr::MatchText { field, query, .. } => {
            let Some(prop) = extract_property(field) else {
                return Ok(None);
            };
            let q = resolve_to_string(query, params)?;
            (prop, lance_index::scalar::FullTextSearchQuery::new(q))
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
            ty: _,
        } => {
            let Some(prop) = extract_property(field) else {
                return Ok(None);
            };
            let q = resolve_to_string(query, params)?;
            let edits = match max_edits.as_deref() {
                Some(expr) => u32::try_from(resolve_to_int(expr, params)?)
                    .map_err(|_| OmniError::manifest("fuzzy max_edits must fit U32"))?,
                None => 2,
            };
            (
                prop,
                lance_index::scalar::FullTextSearchQuery::new_fuzzy(q, Some(edits)),
            )
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Param(_, _)
        | IRExpr::Literal(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _)
        | IRExpr::Binary { .. }
        | IRExpr::Not(_, _)
        | IRExpr::Cast { .. }
        | IRExpr::IsNull { .. } => return Ok(None),
    };
    query
        .with_column(prop)
        .map(Some)
        .map_err(|error| OmniError::storage_context("full_text_search", error))
}

/// Extract the property name from a PropAccess expression.
pub(super) fn extract_property(expr: &IRExpr) -> Option<String> {
    match expr {
        IRExpr::PropAccess { property, .. } => Some(property.clone()),
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Param(_, _)
        | IRExpr::Literal(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _)
        | IRExpr::Binary { .. }
        | IRExpr::Not(_, _)
        | IRExpr::Cast { .. }
        | IRExpr::IsNull { .. } => None,
    }
}

/// Evaluate a constant in its stored type and require a non-null String.
pub(super) fn resolve_to_string(expr: &IRExpr, params: &ParamMap) -> Result<String> {
    let array = super::constant::evaluate_constant_array(expr, params)?;
    match ScalarValue::try_from_array(array.as_ref(), 0).map_err(OmniError::datafusion)? {
        ScalarValue::Utf8(Some(value)) => Ok(value),
        _ => Err(OmniError::manifest(
            "search query must resolve to a non-null String",
        )),
    }
}

/// Evaluate a public integer constant without losing its stored width or unsigned range.
pub(super) fn resolve_to_int(expr: &IRExpr, params: &ParamMap) -> Result<i128> {
    let array = super::constant::evaluate_constant_array(expr, params)?;
    match ScalarValue::try_from_array(array.as_ref(), 0).map_err(OmniError::datafusion)? {
        ScalarValue::Int32(Some(value)) => Ok(i128::from(value)),
        ScalarValue::Int64(Some(value)) => Ok(i128::from(value)),
        ScalarValue::UInt32(Some(value)) => Ok(i128::from(value)),
        ScalarValue::UInt64(Some(value)) => Ok(i128::from(value)),
        _ => Err(OmniError::manifest(
            "search option must resolve to a non-null integer",
        )),
    }
}

/// Convert IR filters to a single DataFusion `Expr` (AND-joined), or
/// `None` if no filter is pushable.
pub(super) fn build_lance_filter_expr(
    filters: &[IRExpr],
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::Expr;

    let mut acc: Option<Expr> = None;
    let mut pushed = 0u64;
    for f in filters {
        let Some(e) = ir_expr_to_df_expr(f, params, schema) else {
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

/// Lower recorded expression types and casts; the schema never selects a domain.
///
/// The lowered predicate is split on the nullness of each column it tests more
/// than once (see [`split_on_repeatedly_tested_columns`]). `schema` decides
/// only which columns need the split and the type of their null; whether the
/// predicate lowers at all depends on the expression alone.
///
/// The result is a filter, not a value: it selects the same rows as the
/// written predicate, but where that predicate is null it may be false. Use it
/// only where true selects a row and null and false both reject it: a scanner
/// or delete filter, a conjunct or disjunct of other filters, or under
/// `IS TRUE` / `IS NOT TRUE`. Never negate it or read its value.
pub(crate) fn ir_expr_to_df_expr(
    expr: &IRExpr,
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    if is_search_filter(expr) {
        return None;
    }
    expr.check_types().ok()?;
    split_on_repeatedly_tested_columns(typed_expr_to_df_expr(expr, params)?, schema)
}

/// The most columns one pushed predicate is split on. Each split doubles the
/// predicate, so a pushed predicate holds at most 2^4 = 16 copies of the
/// written one; a predicate that tests more columns repeatedly is not pushed.
pub(crate) const MAX_NULL_SPLIT_COLUMNS: usize = 4;

/// Lance runs DataFusion's `ExprSimplifier` on every pushed filter
/// (`Planner::optimize_expr`). In DataFusion 54 its IN-list set rules fold two
/// tests of one operand whose lists leave nothing (`x IN (a) AND x IN (b)`,
/// `x IN (a) AND x NOT IN (a, b)`, `x NOT IN (a) OR x NOT IN (b)`) to a bare
/// `false` or `true`. That constant is wrong for a row whose `x` is null, where
/// each test, and so the pair, is null; under a `NOT`, or as the `OR` form, the
/// row is then selected (#1277). The simplifier first turns `x = a OR x = b`
/// into `x IN (a, b)`, and `NOT (x != a)` into `x = a`, so `=` and `!=` feed the
/// same rules. apache/datafusion#24258 keeps the null; no DataFusion release
/// that Lance uses contains it.
///
/// For each nullable column `c` that two or more `=`, `!=` or `IN` tests read,
/// the predicate `p` becomes `(c IS NOT NULL AND p) OR (c IS NULL AND p[c := NULL])`.
/// That is the same predicate in three-valued logic. Every rewrite the
/// simplifier makes in the first branch is exact when `c` is not null, and the
/// branch is false when `c` is null; the second branch tests no `c` at all, so
/// constant folding answers it. When `p[c := NULL]` reads no other column it
/// is evaluated here (see [`constant_truth`]) and the predicate becomes
/// `c IS NULL OR p` when it is true and `c IS NOT NULL AND p` when it is false
/// or null, which keeps Lance's scalar index for the predicate; null becomes
/// false only at the root of a filter, where both reject the row. A column the
/// schema declares non-null needs no split. More than [`MAX_NULL_SPLIT_COLUMNS`] repeatedly tested columns,
/// nullable or not, answer `None` (not pushable), so the verdict never depends
/// on the schema. Remove the split at the Lance bump whose DataFusion contains
/// #24258.
fn split_on_repeatedly_tested_columns(
    expr: datafusion::prelude::Expr,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::common::Column;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::prelude::Expr;

    let repeated = repeatedly_tested_columns(&expr);
    if repeated.len() > MAX_NULL_SPLIT_COLUMNS {
        return None;
    }

    let mut expr = expr;
    for name in repeated {
        let field = schema.and_then(|schema| schema.field_with_name(&name).ok());
        if field.is_some_and(|field| !field.is_nullable()) {
            continue;
        }
        let null = field
            .and_then(|field| ScalarValue::try_from(field.data_type()).ok())
            .unwrap_or(ScalarValue::Null);
        let when_null = expr
            .clone()
            .transform(|node| {
                Ok(match node {
                    Expr::Column(column) if column.name == name => {
                        Transformed::yes(Expr::Literal(null.clone(), None))
                    }
                    other => Transformed::no(other),
                })
            })
            .ok()?
            .data;
        let column = Expr::Column(Column::from_name(name));
        expr = match constant_truth(&when_null) {
            // `p[c := NULL]` is true: `(c IS NULL OR p)` is exact.
            Some(Some(true)) => column.is_null().or(expr),
            // False, or null at the root of a filter, where null and false
            // both reject the row: `(c IS NOT NULL AND p)`.
            Some(Some(false) | None) => column.is_not_null().and(expr),
            None => column
                .clone()
                .is_not_null()
                .and(expr)
                .or(column.is_null().and(when_null)),
        };
    }
    Some(expr)
}

/// The value of an expression that reads no column, evaluated without the
/// simplifier: `Some(Some(b))` for a boolean, `Some(None)` for null, and
/// `None` when it reads a column or does not evaluate to one boolean.
///
/// A null branch `c IS NULL AND NULL` keeps Lance from using a scalar index:
/// it has no index for a literal, so the branch carries a refine expression,
/// and an `OR` with a refine on either side drops the index for the whole
/// predicate (`IndexedExpression::maybe_or`). Folding the branch first keeps
/// `x = a OR x = b` on a nullable indexed property an index probe.
fn constant_truth(expr: &datafusion::prelude::Expr) -> Option<Option<bool>> {
    use arrow_array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::ColumnarValue;

    if !expr.column_refs().is_empty() {
        return None;
    }
    let physical = datafusion::physical_expr::create_physical_expr(
        expr,
        &DFSchema::empty(),
        &Default::default(),
    )
    .ok()?;
    let batch = RecordBatch::try_new_with_options(
        std::sync::Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()?;
    match physical.evaluate(&batch).ok()? {
        ColumnarValue::Scalar(ScalarValue::Boolean(value)) => Some(value),
        ColumnarValue::Scalar(_) => None,
        ColumnarValue::Array(values) => {
            let values = values.as_any().downcast_ref::<BooleanArray>()?;
            (values.len() == 1).then(|| values.is_valid(0).then(|| values.value(0)))
        }
    }
}

/// The columns that two or more `=`, `!=` or `IN` tests with a literal read,
/// in name order.
fn repeatedly_tested_columns(expr: &datafusion::prelude::Expr) -> Vec<String> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use datafusion::prelude::Expr;
    use std::collections::BTreeMap;

    let mut tests: BTreeMap<String, usize> = BTreeMap::new();
    let _ = expr.apply(|node| {
        let operand = match node {
            Expr::InList(list) => Some(list.expr.as_ref()),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq | Operator::NotEq,
                right,
            }) => match (left.as_ref(), right.as_ref()) {
                (operand, Expr::Literal(..)) | (Expr::Literal(..), operand) => Some(operand),
                _ => None,
            },
            _ => None,
        };
        for column in operand.map(Expr::column_refs).unwrap_or_default() {
            *tests.entry(column.name.clone()).or_default() += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    tests
        .into_iter()
        .filter_map(|(name, count)| (count > 1).then_some(name))
        .collect()
}

/// Whether `expr` lowers but tests more than [`MAX_NULL_SPLIT_COLUMNS`]
/// columns repeatedly, the one reason [`ir_expr_to_df_expr`] refuses a
/// predicate it can otherwise express.
pub(crate) fn exceeds_null_split_columns(expr: &IRExpr, params: &ParamMap) -> bool {
    !is_search_filter(expr)
        && expr.check_types().is_ok()
        && typed_expr_to_df_expr(expr, params).is_some_and(|lowered| {
            repeatedly_tested_columns(&lowered).len() > MAX_NULL_SPLIT_COLUMNS
        })
}

fn typed_expr_to_df_expr(expr: &IRExpr, params: &ParamMap) -> Option<datafusion::prelude::Expr> {
    match expr {
        IRExpr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
            ty: _,
        } => comparison_to_df_expr(left, *op, right, params),
        IRExpr::Binary {
            left,
            op: BinaryOp::And,
            right,
            ty: _,
        } => Some(typed_expr_to_df_expr(left, params)?.and(typed_expr_to_df_expr(right, params)?)),
        IRExpr::Binary {
            left,
            op: BinaryOp::Or,
            right,
            ty: _,
        } => Some(typed_expr_to_df_expr(left, params)?.or(typed_expr_to_df_expr(right, params)?)),
        IRExpr::Not(inner, _) => Some(datafusion::logical_expr::not(typed_expr_to_df_expr(
            inner, params,
        )?)),
        IRExpr::IsNull {
            expr,
            negated,
            ty: _,
        } => {
            let operand = typed_expr_to_df_expr(expr, params)?;
            Some(if *negated {
                operand.is_not_null()
            } else {
                operand.is_null()
            })
        }
        IRExpr::Cast { expr: child, ty } => {
            if super::constant::is_constant(expr) {
                let array = super::constant::evaluate_constant_array(expr, params).ok()?;
                let value =
                    datafusion::scalar::ScalarValue::try_from_array(array.as_ref(), 0).ok()?;
                return Some(datafusion::prelude::lit(value));
            }
            Some(datafusion::logical_expr::cast(
                typed_expr_to_df_expr(child, params)?,
                ty.to_arrow()?,
            ))
        }
        IRExpr::PropAccess { property, .. } => Some(datafusion::prelude::ident(property)),
        IRExpr::Literal(literal, ty) => literal_to_expr(literal, ty),
        IRExpr::Param(name, ty) => literal_to_expr(params.get(name)?, ty),
        IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => None,
    }
}

fn comparison_to_df_expr(
    left: &IRExpr,
    op: CompOp,
    right: &IRExpr,
    params: &ParamMap,
) -> Option<datafusion::prelude::Expr> {
    if matches!(op, CompOp::Contains) {
        if super::constant::is_constant(left) {
            return list_membership_to_df_expr(left, right, params);
        }
        return Some(datafusion::functions_nested::expr_fn::array_has(
            typed_expr_to_df_expr(left, params)?,
            typed_expr_to_df_expr(right, params)?,
        ));
    }
    let left = typed_expr_to_df_expr(left, params)?;
    let right = typed_expr_to_df_expr(right, params)?;
    Some(match op {
        CompOp::Eq => left.eq(right),
        CompOp::Ne => left.not_eq(right),
        CompOp::Gt => left.gt(right),
        CompOp::Lt => left.lt(right),
        CompOp::Ge => left.gt_eq(right),
        CompOp::Le => left.lt_eq(right),
        CompOp::StartsWith => datafusion::functions::expr_fn::starts_with(left, right),
        CompOp::StringContains => datafusion::functions::expr_fn::contains(left, right),
        CompOp::Contains => unreachable!("handled above"),
    })
}

/// A constant list keeps one recorded element domain, including its explicit cast.
fn list_membership_to_df_expr(
    list: &IRExpr,
    needle: &IRExpr,
    params: &ParamMap,
) -> Option<datafusion::prelude::Expr> {
    let array = super::constant::evaluate_constant_array(list, params).ok()?;
    let list = array.as_any().downcast_ref::<ListArray>()?;
    if list.is_null(0) {
        return Some(df_lit(ScalarValue::Boolean(None)));
    }
    let items = list.value(0);
    if needle.ty().to_arrow().as_ref() != Some(items.data_type()) {
        return None;
    }
    let needle = typed_expr_to_df_expr(needle, params)?;
    let values = (0..items.len())
        .filter(|row| !items.is_null(*row))
        .map(|row| {
            ScalarValue::try_from_array(items.as_ref(), row)
                .ok()
                .map(df_lit)
        })
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return Some(needle.is_null().and(df_lit(ScalarValue::Boolean(None))));
    }
    Some(needle.in_list(values, false))
}

fn literal_to_expr(
    literal: &Literal,
    ty: &omnigraph_compiler::types::ExprType,
) -> Option<datafusion::prelude::Expr> {
    let array = typed_literal_to_array(literal, ty, 1).ok()?;
    let value = datafusion::scalar::ScalarValue::try_from_array(array.as_ref(), 0).ok()?;
    Some(datafusion::prelude::lit(value))
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
    use super::{ir_expr_to_df_expr, literal_to_expr};
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use omnigraph_compiler::ir::{IRExpr, ParamMap};
    use omnigraph_compiler::query::ast::{BinaryOp, CompOp, Literal};
    use omnigraph_compiler::types::{ExprType, PropType, ScalarType};

    /// GQ's JSON parameters refuse a null list element; an embedded `ParamMap` carries one.
    #[test]
    fn a_null_list_element_is_left_out_of_the_pushed_membership() {
        let lowered = |items: Vec<Literal>| {
            let filter = IRExpr::Binary {
                left: Box::new(IRExpr::Literal(
                    Literal::List(items),
                    ExprType::from_prop(&PropType::list_of(ScalarType::String, false)),
                )),
                op: BinaryOp::Compare(CompOp::Contains),
                ty: ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
                right: Box::new(IRExpr::PropAccess {
                    variable: "n".to_string(),
                    property: "name".to_string(),
                    ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
                }),
            };
            ir_expr_to_df_expr(&filter, &ParamMap::new(), None).expect("a pushable membership")
        };
        let a = Literal::String("a".to_string());
        assert_eq!(lowered(vec![a.clone(), Literal::Null]), lowered(vec![a]));
        assert_eq!(lowered(vec![Literal::Null]), lowered(vec![]));
    }

    #[test]
    fn pushed_literals_keep_their_recorded_type() {
        for (literal, scalar, expected) in [
            (
                Literal::Float(2.7),
                ScalarType::F64,
                ScalarValue::Float64(Some(2.7)),
            ),
            (
                Literal::Float(2.0),
                ScalarType::F64,
                ScalarValue::Float64(Some(2.0)),
            ),
            (
                Literal::Integer(3_000_000_000),
                ScalarType::I64,
                ScalarValue::Int64(Some(3_000_000_000)),
            ),
            (
                Literal::Integer(5),
                ScalarType::I32,
                ScalarValue::Int32(Some(5)),
            ),
            (
                Literal::Float(0.1),
                ScalarType::F32,
                ScalarValue::Float32(Some(0.1)),
            ),
            (Literal::Null, ScalarType::I32, ScalarValue::Int32(None)),
        ] {
            let ty =
                ExprType::from_prop(&PropType::scalar(scalar, matches!(literal, Literal::Null)));
            let expression = literal_to_expr(&literal, &ty).expect("typed scalar literal");
            let Expr::Literal(value, _) = expression else {
                panic!("expected a literal")
            };
            assert_eq!(value, expected, "{literal:?} as {scalar}");
        }
        assert!(
            literal_to_expr(
                &Literal::Integer(3_000_000_000),
                &ExprType::from_prop(&PropType::scalar(ScalarType::I32, false)),
            )
            .is_none()
        );
    }
}

/// What Lance does to a pushed filter, on a three-row table with a nullable
/// `s` (`null`, `"x"`, `"y"`), a nullable `n` (`null`, `1`, `2`) and a
/// non-null `k` (`"a"`, `"b"`, `"c"`).
#[cfg(test)]
pub(crate) mod lance_filter_support {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::Expr;
    use lance_datafusion::planner::Planner;

    pub(crate) fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new("k", DataType::Utf8, false),
        ]))
    }

    /// The rows `expr` holds true for: as written when `optimize` is false,
    /// and after Lance's `Planner::optimize_expr`, the preparation every
    /// scanner filter gets, when it is true.
    pub(crate) fn selected_rows(expr: Expr, optimize: bool) -> Vec<usize> {
        let planner = Planner::new(schema());
        let expr = if optimize {
            planner
                .optimize_expr(expr)
                .expect("Lance prepares the filter")
        } else {
            expr
        };
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec![None, Some("x"), Some("y")])),
                Arc::new(Int64Array::from(vec![None, Some(1), Some(2)])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("a valid batch");
        let values = planner
            .create_physical_expr(&expr)
            .expect("a physical filter")
            .evaluate(&batch)
            .expect("the filter evaluates")
            .into_array(batch.num_rows())
            .expect("one value per row");
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("a boolean filter");
        (0..values.len())
            .filter(|row| values.is_valid(*row) && values.value(*row))
            .collect()
    }
}

#[cfg(test)]
mod null_split_tests {
    use super::lance_filter_support::{schema, selected_rows};
    use super::{MAX_NULL_SPLIT_COLUMNS, split_on_repeatedly_tested_columns};
    use datafusion::prelude::{Expr, col, lit, not};

    fn shapes() -> Vec<(&'static str, Expr)> {
        let s_in = |value: &str| col("s").in_list(vec![lit(value)], false);
        let n_in =
            |values: &[i64]| col("n").in_list(values.iter().map(|v| lit(*v)).collect(), false);
        vec![
            (
                "not s in x or not s in y",
                not(s_in("x")).or(not(s_in("y"))),
            ),
            ("not (s in x and s in y)", not(s_in("x").and(s_in("y")))),
            (
                "not (n in 1 and not n in 1,2,0,5)",
                not(n_in(&[1]).and(not(n_in(&[1, 2, 0, 5])))),
            ),
            (
                "not ((s = x or s = w) and (s = y or s = v))",
                not((col("s").eq(lit("x")).or(col("s").eq(lit("w"))))
                    .and(col("s").eq(lit("y")).or(col("s").eq(lit("v"))))),
            ),
            (
                "not (s != x and s != y) and not (n = 1 and n = 2)",
                not(col("s").not_eq(lit("x")).and(col("s").not_eq(lit("y"))))
                    .and(not(col("n").eq(lit(1i64)).and(col("n").eq(lit(2i64))))),
            ),
            (
                "n = 1 or n = 2",
                col("n").eq(lit(1i64)).or(col("n").eq(lit(2i64))),
            ),
            (
                "(n = 1 or n = 2) and k = b",
                (col("n").eq(lit(1i64)).or(col("n").eq(lit(2i64)))).and(col("k").eq(lit("b"))),
            ),
        ]
    }

    /// Lance's preparation selects the null row for these shapes, and the
    /// split predicate selects exactly the rows the written one does. When
    /// the first assertion fails, Lance's DataFusion keeps the null
    /// (apache/datafusion#24258) and the split can go.
    #[test]
    fn the_split_keeps_null_rows_out_of_a_prepared_filter_issue_1277() {
        let mut lance_selects_the_null_row = false;
        for (name, shape) in shapes() {
            let written = selected_rows(shape.clone(), false);
            assert!(!written.contains(&0), "{name}: the null row is never true");
            lance_selects_the_null_row |= selected_rows(shape.clone(), true).contains(&0);
            let split = split_on_repeatedly_tested_columns(shape, Some(&schema()))
                .expect("within the column bound");
            assert_eq!(selected_rows(split, true), written, "{name}");
        }
        assert!(
            lance_selects_the_null_row,
            "Lance no longer folds a disjoint IN pair to a bare literal"
        );
    }

    #[test]
    fn only_repeatedly_tested_nullable_columns_are_split() {
        let once = col("s")
            .in_list(vec![lit("x")], false)
            .and(col("n").eq(lit(1i64)));
        assert_eq!(
            split_on_repeatedly_tested_columns(once.clone(), Some(&schema())),
            Some(once)
        );
        let non_null = not(col("k").eq(lit("a")).or(col("k").eq(lit("b"))));
        assert_eq!(
            split_on_repeatedly_tested_columns(non_null.clone(), Some(&schema())),
            Some(non_null)
        );
    }

    /// A null branch that reads no other column is folded to a constant, so
    /// the pushed predicate keeps one guard and Lance keeps its scalar index;
    /// one that reads another column keeps both branches.
    #[test]
    fn a_constant_null_branch_folds_into_one_guard() {
        let p = col("n").eq(lit(1i64)).or(col("n").eq(lit(2i64)));
        assert_eq!(
            split_on_repeatedly_tested_columns(p.clone(), Some(&schema())),
            Some(col("n").is_not_null().and(p))
        );
        let p = col("s")
            .is_null()
            .or(not(col("s").eq(lit("x")).and(col("s").eq(lit("y")))));
        let split = split_on_repeatedly_tested_columns(p.clone(), Some(&schema()));
        assert_eq!(split, Some(col("s").is_null().or(p.clone())));
        let written = selected_rows(p, false);
        assert!(written.contains(&0), "the null row is selected as written");
        assert_eq!(selected_rows(split.expect("split"), true), written);
        let p = col("n")
            .eq(lit(1i64))
            .or(col("n").eq(lit(2i64)))
            .or(col("k").eq(lit("a")));
        let split = split_on_repeatedly_tested_columns(p.clone(), Some(&schema()))
            .expect("within the column bound");
        assert_ne!(split, col("n").is_not_null().and(p.clone()));
        assert_ne!(split, col("n").is_null().or(p));
    }

    /// The bound is on repeatedly tested columns whatever their nullability,
    /// so the verdict is the same with and without a schema.
    #[test]
    fn more_repeatedly_tested_columns_than_the_bound_are_not_pushed() {
        let twice = |name: String| {
            col(name.as_str())
                .eq(lit("a"))
                .or(col(name.as_str()).eq(lit("b")))
        };
        let predicate = |columns: usize| {
            (0..columns)
                .map(|column| twice(format!("p{column}")))
                .reduce(Expr::and)
                .expect("at least one column")
        };
        for schema in [None, Some(schema())] {
            assert!(
                split_on_repeatedly_tested_columns(
                    predicate(MAX_NULL_SPLIT_COLUMNS),
                    schema.as_deref()
                )
                .is_some()
            );
            assert!(
                split_on_repeatedly_tested_columns(
                    predicate(MAX_NULL_SPLIT_COLUMNS + 1),
                    schema.as_deref()
                )
                .is_none()
            );
        }
    }
}

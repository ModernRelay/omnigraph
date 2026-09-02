use super::*;

use super::projection::{
    apply_filter, apply_ordering, project_return, projections_have_aggregates,
};

/// Bundles the per-handle embedding client cell with the optional injected
/// config (RFC-012 Phase 5) so the lazy init uses the injected config when
/// present, else `EmbeddingClient::from_env()`. Threaded through the query path
/// in place of the bare cell, preserving laziness (a graph that never embeds
/// builds no client and needs no key).
pub(crate) struct EmbeddingResolver<'a> {
    cell: &'a tokio::sync::OnceCell<EmbeddingClient>,
    config: Option<&'a crate::embedding::EmbeddingConfig>,
}

impl EmbeddingResolver<'_> {
    async fn resolve(&self) -> Result<&EmbeddingClient> {
        let config = self.config.cloned();
        self.cell
            .get_or_try_init(|| async move {
                match config {
                    Some(cfg) => EmbeddingClient::new(cfg),
                    None => EmbeddingClient::from_env(),
                }
            })
            .await
    }
}

impl Omnigraph {
    /// Run a named query against an explicit branch or snapshot target.
    pub async fn query(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        self.query_with_head(target, query_source, query_name, params)
            .await
            .map(|(result, _)| result)
    }

    /// [`Self::query`] additionally returning the graph head commit id of the
    /// exact snapshot the query executed against. A fresh named branch returns
    /// its inherited source commit even though it has no materialized
    /// branch-owned head row yet.
    ///
    /// The id comes from the same pinned version as every table read — the
    /// value a caller passes to [`Self::mutate_as_with_expected_head`] for a
    /// read-then-write compare-and-swap.
    pub async fn query_with_head(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<(QueryResult, Option<String>)> {
        // Capture the manifest snapshot and immutable catalog under the same
        // schema-publication gate. SchemaApply publishes its fixed manifest
        // outcome before promoting files/ArcSwap; without this gate a query on
        // the applying handle could pair that new snapshot with the old catalog.
        let (resolved, catalog) = self.capture_read_view(target).await?;

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(&catalog, &query_decl)?;
        let ir = lower_query(&catalog, &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        // Lazy: an index-served query with no AntiJoin never builds the CSR.
        let graph_index = if needs_graph {
            GraphIndexHandle::cached(
                self,
                &resolved,
                referenced_edge_types(&ir.pipeline, &catalog),
            )
        } else {
            GraphIndexHandle::none()
        };

        let head = resolved.graph_commit_id.clone();
        let result = execute_query(
            &ir,
            params,
            &resolved.snapshot,
            &graph_index,
            &catalog,
            &EmbeddingResolver {
                cell: self.embedding_cell(),
                config: self.embedding_config_ref(),
            },
        )
        .await?;
        Ok((result, head))
    }

    /// Run a named query against the graph as it existed at a prior graph-manifest version.
    ///
    /// Compiles the query normally, builds a temporary (non-cached) graph index
    /// if traversal is needed, and executes against the historical snapshot.
    pub async fn run_query_at(
        &self,
        version: u64,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        // Historical resolution still uses the current accepted catalog, so
        // capture both sides of that view under schema publication just like a
        // live-target query.
        let (snapshot, catalog) = self.capture_historical_read_view(version).await?;

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(&catalog, &query_decl)?;
        let ir = lower_query(&catalog, &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        // Lazy build against this historical snapshot (not the RuntimeCache,
        // which is keyed to live branch targets); only a CSR-path Expand or an
        // AntiJoin triggers it. Scoped to the edges this query traverses.
        let graph_index = if needs_graph {
            GraphIndexHandle::direct(&snapshot, referenced_edge_types(&ir.pipeline, &catalog))
        } else {
            GraphIndexHandle::none()
        };

        execute_query(
            &ir,
            params,
            &snapshot,
            &graph_index,
            &catalog,
            &EmbeddingResolver {
                cell: self.embedding_cell(),
                config: self.embedding_config_ref(),
            },
        )
        .await
    }
}

// ─── Search mode ─────────────────────────────────────────────────────────────

/// Describes how the query's ordering changes the scan mode.
#[derive(Debug, Default, Clone)]
struct SearchMode {
    /// Vector ANN search: (variable, property, query_vector, k).
    nearest: Option<(String, String, Vec<f32>, usize)>,
    /// BM25 full-text search: (variable, property, query_text).
    bm25: Option<(String, String, String)>,
    /// Row cap for the BM25 scan, the counterpart of `nearest`'s `k`; see
    /// `bm25_scan_limit` for the semantics.
    bm25_scan_limit: Option<usize>,
    /// RRF fusion: (primary, secondary, k_constant, limit).
    rrf: Option<RrfMode>,
    /// The rrf prefilter gate's eligible-id set for this arm's BM25 scan
    /// (`execute_rrf_fusion` sets it on an arm iff that arm carries an
    /// uncapped `bm25` target on the ranked variable — never on a `nearest`
    /// arm, whose constitutive `k` truncation would make a prefiltered run
    /// answer-DIFFERENT, not just cheaper). The set over-approximates the
    /// traversal's survivors (single owner of that invariant:
    /// `rrf_prefilter_gate`'s doc), so ANDing it into the scan is a cost
    /// change only. Not a cap: `to_uncapped` must NOT clear it.
    bm25_eligible_ids: Option<EligibleIds>,
}

/// Shared eligible-id set, `Debug`-opaque so a logged `SearchMode` prints the
/// cardinality instead of up to `DEFAULT_RRF_GATE_MAX_IDS` id strings.
#[derive(Clone)]
struct EligibleIds(Arc<Vec<String>>);

impl std::fmt::Debug for EligibleIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EligibleIds(len={})", self.0.len())
    }
}

impl SearchMode {
    /// This mode with the BM25 scan cap cleared (only a standalone `bm25()`
    /// ordering ever carries one — `rrf()` arms are never capped, see
    /// `extract_sub_search_mode`). Any future scan cap must be cleared here
    /// too. (`bm25_eligible_ids` is not a cap — a superset prefilter is
    /// answer-preserving — so it survives.)
    fn to_uncapped(&self) -> Self {
        Self {
            bm25_scan_limit: None,
            ..self.clone()
        }
    }

    /// The eligible-id set to AND into `variable`'s scan, if this mode is a
    /// bm25 arm targeting `variable` and the gate chose the prefilter plan.
    fn bm25_eligible_ids_for(&self, variable: &str) -> Option<&[String]> {
        match (&self.bm25, &self.bm25_eligible_ids) {
            (Some((var, ..)), Some(ids)) if var == variable => Some(ids.0.as_slice()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct RrfMode {
    primary: Box<SearchMode>,
    secondary: Box<SearchMode>,
    k: u32,
    limit: usize,
}

/// Multiplier on the query's limit for a capped BM25 scan; trades scan width
/// against how often the uncapped retry is needed (see `execute_query`).
const BM25_SCAN_OVERFETCH_FACTOR: usize = 4;

/// Row cap for the BM25 scan, or `None` to scan every matching document.
/// `None` for a limitless query, and for any aggregate return: an aggregate's
/// value is computed over the scanned rows, so a capped scan would change the
/// answer, not just the cost. Standalone `bm25()` orderings only — `rrf()`
/// arms never call this (see `extract_sub_search_mode`).
fn bm25_scan_limit(ir: &QueryIR) -> Option<usize> {
    if projections_have_aggregates(&ir.return_exprs) {
        return None;
    }
    // Secondary order keys disqualify the cap: they rank WITHIN score ties,
    // and a bounded scan chooses which tied rows exist at all — the secondary
    // sort over a cap-arbitrary subset would be a silently wrong answer the
    // under-fill retry cannot see (exactly `limit` rows still come back).
    if ir.order_by.len() > 1 {
        return None;
    }
    ir.limit.map(|rows| {
        usize::try_from(rows)
            .unwrap_or(usize::MAX)
            .saturating_mul(BM25_SCAN_OVERFETCH_FACTOR)
    })
}

/// Extract search ordering mode from the IR.
async fn extract_search_mode(
    ir: &QueryIR,
    params: &ParamMap,
    catalog: &Catalog,
    embedding: &EmbeddingResolver<'_>,
) -> Result<SearchMode> {
    if ir.order_by.is_empty() {
        return Ok(SearchMode::default());
    }
    let ordering = &ir.order_by[0];
    match &ordering.expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let vec = resolve_nearest_query_vec(
                ir, catalog, variable, property, query, params, embedding,
            )
            .await?;
            let k = usize::try_from(ir.limit.ok_or_else(|| {
                OmniError::manifest("nearest() ordering requires a limit clause".to_string())
            })?)
            .unwrap_or(usize::MAX);
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => {
                    return Err(OmniError::manifest(
                        "bm25 field must be a property access".to_string(),
                    ));
                }
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::manifest("bm25 query must resolve to a string".to_string())
            })?;
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                bm25_scan_limit: bm25_scan_limit(ir),
                ..Default::default()
            })
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            let limit = usize::try_from(ir.limit.ok_or_else(|| {
                OmniError::manifest("rrf() ordering requires a limit clause".to_string())
            })?)
            .unwrap_or(usize::MAX);
            let k_val = k
                .as_ref()
                .and_then(|e| resolve_to_int(e, params))
                .map(|k| u32::try_from(k).unwrap_or(u32::MAX))
                .unwrap_or(60);

            let primary_mode =
                extract_sub_search_mode(ir, primary, params, catalog, embedding).await?;
            let secondary_mode =
                extract_sub_search_mode(ir, secondary, params, catalog, embedding).await?;

            Ok(SearchMode {
                rrf: Some(RrfMode {
                    primary: Box::new(primary_mode),
                    secondary: Box::new(secondary_mode),
                    k: k_val,
                    limit,
                }),
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Extract a sub-search mode from a nested RRF expression (nearest or bm25).
async fn extract_sub_search_mode(
    ir: &QueryIR,
    expr: &IRExpr,
    params: &ParamMap,
    catalog: &Catalog,
    embedding: &EmbeddingResolver<'_>,
) -> Result<SearchMode> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let vec = resolve_nearest_query_vec(
                ir, catalog, variable, property, query, params, embedding,
            )
            .await?;
            let k = ir
                .limit
                .map(|rows| usize::try_from(rows).unwrap_or(usize::MAX))
                .unwrap_or(100);
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => {
                    return Err(OmniError::manifest(
                        "bm25 field must be a property access".to_string(),
                    ));
                }
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::manifest("bm25 query must resolve to a string".to_string())
            })?;
            // Never capped: an arm's cap window would be filled by text score
            // before traversals run, so join-ineligible rows can starve the
            // arm out of the fusion while the fused row count stays full — no
            // count-based retry can detect it, and the missing contributions
            // shift fused ranks (PR #574 review). Fusion needs the arm's
            // complete ranking.
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Resolve an expression to a nearest() query vector.
async fn resolve_nearest_query_vec(
    ir: &QueryIR,
    catalog: &Catalog,
    variable: &str,
    property: &str,
    expr: &IRExpr,
    params: &ParamMap,
    embedding: &EmbeddingResolver<'_>,
) -> Result<Vec<f32>> {
    let lit = resolve_literal_or_param(expr, params)?;
    match lit {
        Literal::List(_) => literal_to_f32_vec(&lit),
        Literal::String(text) => {
            let (expected_dim, recorded_model) =
                nearest_property_dim_and_model(ir, catalog, variable, property)?;
            // Lazily resolve the per-handle client once, then reuse it across
            // queries (keeps the provider connection pool warm); a graph that
            // never embeds never builds a client and needs no provider key.
            let client = embedding.resolve().await?;
            // Same-space guarantee: if the property recorded the model that
            // produced its stored vectors (`@embed("…", model="…")`), the query
            // embedder must resolve to that same model — otherwise the comparison
            // is across vector spaces. Reject loudly instead of ranking garbage.
            if let Some(recorded) = &recorded_model {
                let resolved = &client.config().model;
                if resolved != recorded {
                    return Err(OmniError::manifest(format!(
                        "nearest() on '{property}': its stored vectors were embedded with model \
                         '{recorded}', but the query embedder resolves to '{resolved}'. Set \
                         OMNIGRAPH_EMBED_MODEL='{recorded}' (and the matching provider) or re-embed \
                         the stored vectors."
                    )));
                }
            }
            client.embed_query_text(&text, expected_dim).await
        }
        _ => Err(OmniError::manifest(
            "nearest query must be a string or list of floats".to_string(),
        )),
    }
}

fn resolve_literal_or_param(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    Ok(match expr {
        IRExpr::Literal(lit) => lit.clone(),
        IRExpr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?,
        _ => {
            return Err(OmniError::manifest(
                "nearest query must be a literal or parameter".to_string(),
            ));
        }
    })
}

/// Resolve a literal vector expression to a Vec<f32>.
fn literal_to_f32_vec(lit: &Literal) -> Result<Vec<f32>> {
    match lit {
        Literal::List(items) => items
            .iter()
            .map(|item| match item {
                Literal::Float(f) => Ok(*f as f32),
                Literal::Integer(n) => Ok(*n as f32),
                _ => Err(OmniError::manifest(
                    "vector elements must be numeric".to_string(),
                )),
            })
            .collect(),
        _ => Err(OmniError::manifest(
            "nearest query must be a list of floats".to_string(),
        )),
    }
}

/// Resolve the nearest() target property's vector dimension and the embedding
/// model recorded for it via `@embed("…", model="…")` (`None` if unrecorded).
fn nearest_property_dim_and_model(
    ir: &QueryIR,
    catalog: &Catalog,
    variable: &str,
    property: &str,
) -> Result<(usize, Option<String>)> {
    let type_name = resolve_binding_type_name(&ir.pipeline, variable).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() variable '${}' is not bound to a node type in the lowered pipeline",
            variable
        ))
    })?;
    let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() binding '${}' resolved unknown node type '{}'",
            variable, type_name
        ))
    })?;
    let prop = node_type.properties.get(property).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() property '{}.{}' is missing from the catalog",
            type_name, property
        ))
    })?;
    let dim = match prop.scalar {
        ScalarType::Vector(dim) if !prop.list => dim as usize,
        _ => {
            return Err(OmniError::manifest_internal(format!(
                "nearest() property '{}.{}' is not a scalar vector",
                type_name, property
            )));
        }
    };
    let recorded_model = node_type
        .embed_sources
        .get(property)
        .and_then(|embed| embed.model.clone());
    Ok((dim, recorded_model))
}

fn resolve_binding_type_name<'a>(pipeline: &'a [IROp], variable: &str) -> Option<&'a str> {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable: bound_var,
                type_name,
                ..
            } if bound_var == variable => return Some(type_name.as_str()),
            IROp::Expand {
                dst_var, dst_type, ..
            } if dst_var == variable => return Some(dst_type.as_str()),
            IROp::AntiJoin { inner, .. } => {
                if let Some(type_name) = resolve_binding_type_name(inner, variable) {
                    return Some(type_name);
                }
            }
            _ => {}
        }
    }
    None
}

/// Execute a lowered QueryIR. Pure function — no state, no caches.
pub async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    embedding: &EmbeddingResolver<'_>,
) -> Result<QueryResult> {
    let mut resolved_params = None;
    for param in &ir.params {
        if !params.contains_key(&param.name) {
            if param.nullable {
                resolved_params
                    .get_or_insert_with(|| params.clone())
                    .insert(param.name.clone(), Literal::Null);
            } else {
                return Err(OmniError::manifest(format!(
                    "parameter '{}' not provided",
                    param.name
                )));
            }
        }
    }
    let params = resolved_params.as_ref().unwrap_or(params);

    let search_mode = extract_search_mode(ir, params, catalog, embedding).await?;

    // RRF requires forked execution. Its bm25 arms are never capped (see
    // `extract_sub_search_mode`), so no under-fill retry arises for it.
    if let Some(ref rrf) = search_mode.rrf {
        return execute_rrf_fusion(ir, params, snapshot, graph_index, catalog, rrf).await;
    }

    let result_batch =
        execute_query_once(ir, params, snapshot, graph_index, catalog, &search_mode).await?;

    // A capped BM25 scan can under-fill: rows that survive the scan are then
    // dropped by a traversal with no matching edge or by a filter that could
    // not be pushed into it. Retry uncapped so a short answer is never served
    // in place of a complete one. The row count cannot distinguish cap
    // starvation from a corpus with fewer matches than `limit`, so such
    // queries pay the double run on every execution. (Aggregate returns are
    // never capped — see `bm25_scan_limit` — so no retry arises for them.)
    if search_mode.bm25_scan_limit.is_some()
        && ir
            .limit
            .is_some_and(|limit| (result_batch.num_rows() as u64) < limit)
    {
        tracing::debug!(
            limit = ir.limit,
            capped_rows = result_batch.num_rows(),
            "bm25 scan cap under-filled; retrying uncapped"
        );
        crate::instrumentation::record_bm25_uncapped_retry();
        let uncapped = search_mode.to_uncapped();
        let retried =
            execute_query_once(ir, params, snapshot, graph_index, catalog, &uncapped).await?;
        return Ok(QueryResult::new(retried.schema(), vec![retried]));
    }

    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

/// One pass of a non-RRF query: pipeline, projection, ordering, limit.
/// Separate from `execute_query` so the under-fill retry can rerun it with a
/// different `search_mode`.
async fn execute_query_once(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    search_mode: &SearchMode,
) -> Result<RecordBatch> {
    let has_aggregates = projections_have_aggregates(&ir.return_exprs);

    // Limit pushdown into a final Expand (query-level half of the legality
    // check; the op-level half lives in `execute_pipeline`). An unordered
    // `limit` demands ANY n valid rows, so a traversal may stop once n pairs
    // are emitted. Ordering (explicit or search-imposed) needs the full row
    // set to rank, and an aggregate consumes every row — both disqualify.
    let final_expand_cap = match ir.limit {
        Some(limit)
            if ir.order_by.is_empty() && !has_aggregates && !is_search_ordered(search_mode) =>
        {
            usize::try_from(limit).ok()
        }
        _ => None,
    };

    let needed_columns = collect_needed_columns(ir);
    let mut wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut wide,
        search_mode,
        final_expand_cap,
        &needed_columns,
    )
    .await?;
    let wide_batch = wide.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty())));
    let mut result_batch = project_return(&wide_batch, &ir.return_exprs, params)?;

    // Apply ordering. Search-ordered plans sort on the appended score column
    // (mechanism and contract: `search_score_orderings`). Aggregated
    // search-ordered queries keep the historical no-sort behavior: the score
    // column does not survive aggregation. `fetch` is safe to pass on every
    // path here because the only step after ordering is the limit slice.
    let fetch = ir.limit.and_then(|limit| usize::try_from(limit).ok());
    if !ir.order_by.is_empty() && !is_search_ordered(search_mode) {
        result_batch = if has_aggregates {
            apply_ordering(
                result_batch.clone(),
                &ir.order_by,
                &result_batch,
                params,
                fetch,
            )?
        } else {
            apply_ordering(result_batch, &ir.order_by, &wide_batch, params, fetch)?
        };
    } else if !has_aggregates {
        if let Some(mut orderings) = search_score_orderings(search_mode) {
            // Guard on the invariant itself (score column present), not row
            // count: an empty scan's fallback schema legitimately lacks the
            // column (zero rows, nothing to order); rows WITHOUT the column
            // would mean the ranking is unrecoverable, and returning them
            // unranked would be a silent wrong answer — refuse instead.
            let score_col = match &orderings[0].expr {
                IRExpr::PropAccess { variable, property } => format!("{variable}.{property}"),
                _ => String::new(),
            };
            if wide_batch.column_by_name(&score_col).is_some() {
                // User-stated secondary keys (`order { nearest(...), $p.name
                // desc }`) apply after the score, before the id tie-break. A
                // search function in a non-leading position is refused with
                // its constraint named, not fed to `apply_ordering`'s opaque
                // unsupported-expression arm.
                for extra in ir.order_by.iter().skip(1) {
                    if matches!(
                        extra.expr,
                        IRExpr::Nearest { .. } | IRExpr::Bm25 { .. } | IRExpr::Rrf { .. }
                    ) {
                        return Err(OmniError::manifest(
                            "search functions must lead the order clause; keys after the \
                             search function must be plain expressions"
                                .to_string(),
                        ));
                    }
                }
                orderings.extend(ir.order_by.iter().skip(1).cloned());
                result_batch =
                    apply_ordering(result_batch, &orderings, &wide_batch, params, fetch)?;
            } else if result_batch.num_rows() > 0 {
                return Err(OmniError::manifest(format!(
                    "search-ordered query produced rows without its '{score_col}' ranking column"
                )));
            }
        }
    }

    // Apply limit
    if let Some(limit) = ir.limit {
        let len = result_batch.num_rows().min(limit as usize);
        result_batch = result_batch.slice(0, len);
    }

    Ok(result_batch)
}

/// Check if the query's ordering is search-imposed (`nearest()`/`bm25`).
fn is_search_ordered(search_mode: &SearchMode) -> bool {
    search_mode.nearest.is_some() || search_mode.bm25.is_some()
}

/// Synthetic orderings for a search-ordered plan: sort on the score column
/// Lance appended to the scan (`nearest` ranks by ascending `_distance`,
/// `bm25` by descending `_score`). The column rides the wide batch under the
/// search binding's prefix like any other property — hydration replicates it
/// onto every traversal row, so ranking is data on the rows and Expand
/// emission order is not load-bearing — and `apply_ordering`'s `.id`
/// tie-break makes the order total and deterministic. The bare names are
/// reserved property names at schema validation, so a user column can never
/// shadow them. Latent nulls note: `apply_ordering` places nulls first under
/// asc; no in-tree path produces a null score (T23 blocks edge-binding
/// nearest, hydration replicates non-null seed columns) — if one ever
/// appears, rank nulls last explicitly here.
fn search_score_orderings(search_mode: &SearchMode) -> Option<Vec<IROrdering>> {
    let (variable, property, descending) = if let Some((var, ..)) = &search_mode.nearest {
        (var.clone(), "_distance", false)
    } else if let Some((var, ..)) = &search_mode.bm25 {
        (var.clone(), "_score", true)
    } else {
        return None;
    };
    Some(vec![IROrdering {
        expr: IRExpr::PropAccess {
            variable,
            property: property.to_string(),
        },
        descending,
    }])
}

// ─── RRF prefilter gate ──────────────────────────────────────────────────────

/// Prefilter admission ratio: the gate's selective plan runs when
/// |eligible| / corpus is at or below this. Set by the gate benchmark
/// (`benches/scenarios.rs` `rrf-gate`, 2026-08-31): on a 100k-row corpus the
/// prefiltered plan's warm wall clock still beat the postfilter plan's at
/// 10% eligibility (31.5 ms vs 53.5 ms) and lost at 25% (85 ms vs 68.5 ms);
/// a 200 KiB-payload corpus crossed even higher. 0.10 is the conservative
/// (smaller) crossover across both corpora.
const DEFAULT_RRF_GATE_RATIO: f64 = 0.10;
/// Absolute ceiling on the eligible-id in-list: the per-id predicate cost
/// the ratio cannot see on huge corpora. Set by the same benchmark's 10^5 /
/// 10^6 microbench (1e6-row corpus): at 1e5 ids the prefiltered plan still
/// won (324.5 ms vs 360.5 ms warm) and at 1e6 it lost 1.7x (2.76 s vs
/// 1.63 s) — `Expr` construction itself stays negligible (31 ms at 1e6);
/// the loss is the in-list probe/filter evaluation.
const DEFAULT_RRF_GATE_MAX_IDS: usize = 100_000;

fn rrf_gate_ratio() -> f64 {
    std::env::var("OMNIGRAPH_RRF_GATE_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|r| r.is_finite() && *r >= 0.0)
        .unwrap_or(DEFAULT_RRF_GATE_RATIO)
}

fn rrf_gate_max_ids() -> usize {
    std::env::var("OMNIGRAPH_RRF_GATE_MAX_IDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_RRF_GATE_MAX_IDS)
}

/// The rrf gate's force hook. `OMNIGRAPH_RRF_PLAN` ∈ {auto (default),
/// force_prefilter, force_postfilter}; the scoped test seam
/// (`instrumentation::with_rrf_plan`) takes precedence over the
/// process-global env var, mirroring `traversal_indexed_override`. A force
/// overrides only the gate's THRESHOLD decision, never a correctness fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RrfPlanForce {
    Auto,
    Prefilter,
    Postfilter,
}

fn rrf_plan_force() -> RrfPlanForce {
    let mode = crate::instrumentation::rrf_plan_override()
        .map(str::to_string)
        .or_else(|| std::env::var("OMNIGRAPH_RRF_PLAN").ok());
    match mode.as_deref() {
        Some("force_prefilter") => RrfPlanForce::Prefilter,
        Some("force_postfilter") => RrfPlanForce::Postfilter,
        // A diagnosis knob must not fail silent while someone is diagnosing:
        // an unrecognized value runs auto, loudly.
        Some(other) if !other.is_empty() && other != "auto" => {
            tracing::warn!(
                value = other,
                "unrecognized OMNIGRAPH_RRF_PLAN value; running auto"
            );
            RrfPlanForce::Auto
        }
        _ => RrfPlanForce::Auto,
    }
}

/// Top-level Expand ops whose `src_var` is the ranked variable — the
/// admission table's eligibility sources, as (edge_type, direction) pairs.
/// `None` is the shape fall-back: the ranked variable is some Expand's dst
/// (its NodeScan never installs the search — pre-existing silent-ignore), it
/// is not introduced by a top-level NodeScan, or no top-level Expand
/// constrains it. Expands inside an AntiJoin inner are NEVER sources: the
/// anti-join inverts their meaning, and deriving eligibility from one is a
/// subset error — the one direction that changes answers (the superset
/// rule on `rrf_prefilter_gate`). A `min_hops == 0` Expand would make every node
/// eligible; typecheck T15 rejects it, and it is skipped defensively here
/// (skipping a source only widens the set).
fn rrf_gate_expand_sources<'a>(
    pipeline: &'a [IROp],
    ranked_var: &str,
) -> Option<Vec<(&'a str, Direction)>> {
    let mut introduced_by_scan = false;
    let mut sources: Vec<(&str, Direction)> = Vec::new();
    for op in pipeline {
        match op {
            IROp::NodeScan { variable, .. } if variable == ranked_var => {
                introduced_by_scan = true;
            }
            // Field-exhaustive on purpose (no `..`): a future Expand field
            // must be classified here — superset-safe or not — before this
            // walk compiles, the field-level twin of the exhaustive op match.
            // The elided-by-name fields are each shrink-only or
            // multiplicity-only: `dst_type` (typing, checked at the catalog
            // in the gate), `max_hops` (an upper bound never widens the
            // first-hop necessity), `dst_filters` (shrink survivors only),
            // `edge_binding` (row multiplicity, not membership).
            IROp::Expand {
                src_var,
                dst_var,
                edge_type,
                direction,
                min_hops,
                dst_type: _,
                max_hops: _,
                dst_filters: _,
                edge_binding: _,
            } => {
                if dst_var == ranked_var {
                    return None;
                }
                if src_var == ranked_var && *min_hops > 0 {
                    sources.push((edge_type.as_str(), *direction));
                }
            }
            IROp::NodeScan { .. } | IROp::Filter(_) | IROp::AntiJoin { .. } => {}
        }
    }
    if introduced_by_scan && !sources.is_empty() {
        Some(sources)
    } else {
        None
    }
}

/// The rrf prefilter gate: decide, before the arms run, between two ANSWER-IDENTICAL
/// plans — prefilter (the uncapped bm25 arms rank only the traversal's
/// eligible ids) and postfilter (the uncapped corpus-wide arms, v0.9 rrf
/// semantics).
///
/// INVARIANT (single owner): with bm25 arms prefiltered and nearest arms
/// untouched, over FTS-index-covered data,
/// up to BM25 score ties, the candidate plans are answer-identical;
/// cardinality decides cost only. A mis-estimate wastes time, never flips a
/// winner — re-coupling answer content to the estimate would recreate the
/// PR #574 cap starvation one level up. Every fence below guards that
/// identity:
/// - the eligible set MUST over-approximate the traversal's survivors (a
///   superset only costs speedup; a subset changes answers) — every
///   admitted shape in `rrf_gate_expand_sources` is an instance;
/// - full FTS fragment coverage (uncovered fragments are scored
///   filter-dependently, so a mask would change their scores);
/// - `nearest` arms are never prefiltered (their constitutive `k` makes a
///   prefiltered run answer-different) — the caller's threading rule;
/// - an empty eligible set runs postfilter (same empty join, and `IN ()`
///   edge semantics never arise).
///
/// Fallible steps fall back to postfilter — a query must never fail because
/// an optimization could not start. The gate reads the eligible COUNT only;
/// id strings materialize only after the prefilter plan is chosen, so the
/// broad regime never builds them. Every decision records a
/// `rrf_gate_verdicts` probe entry.
async fn rrf_prefilter_gate(
    ir: &QueryIR,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    rrf: &RrfMode,
) -> Option<EligibleIds> {
    use crate::instrumentation::{
        RrfGateFallback, RrfGatePlan, RrfGateVerdict, record_rrf_gate_verdict,
    };

    let fall_back =
        |fallback: RrfGateFallback, forced: bool, eligible: Option<u64>, corpus: Option<u64>| {
            // Production-visible trace beside the test-only probe: a
            // persistently failing fence (e.g. a CSR build error) silently
            // disables the optimization otherwise — the bm25 retry's
            // logging precedent.
            tracing::debug!(
                ?fallback,
                forced,
                "rrf prefilter gate fell back to the postfilter plan"
            );
            record_rrf_gate_verdict(RrfGateVerdict {
                plan: RrfGatePlan::Postfilter,
                fallback: Some(fallback),
                forced,
                eligible,
                corpus,
            });
        };

    let force = rrf_plan_force();
    if force == RrfPlanForce::Postfilter {
        fall_back(RrfGateFallback::Forced, true, None, None);
        return None;
    }
    let forced = force == RrfPlanForce::Prefilter;

    // Both arms must target one ranked variable, and at least one arm must
    // be bm25 — a nearest-only fusion has nothing the gate may prefilter.
    let arm_target = |mode: &SearchMode| {
        mode.bm25
            .as_ref()
            .map(|(v, ..)| v.clone())
            .or_else(|| mode.nearest.as_ref().map(|(v, ..)| v.clone()))
    };
    let (Some(primary_var), Some(secondary_var)) =
        (arm_target(&rrf.primary), arm_target(&rrf.secondary))
    else {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    };
    if primary_var != secondary_var {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    }
    let ranked_var = primary_var.as_str();
    let bm25_props: Vec<&str> = [&rrf.primary, &rrf.secondary]
        .into_iter()
        .filter_map(|arm| arm.bm25.as_ref().map(|(_, prop, _)| prop.as_str()))
        .collect();
    if bm25_props.is_empty() {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    }

    let Some(sources) = rrf_gate_expand_sources(&ir.pipeline, ranked_var) else {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    };
    let Some(ranked_type) = resolve_binding_type_name(&ir.pipeline, ranked_var) else {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    };

    // Gate input: the ranked type's manifest-resident entity count — the
    // expand cost model's own corpus spelling, no async count_rows.
    let node_key = format!("node:{}", ranked_type);
    let Some(node_entry) = snapshot.dataset(&node_key) else {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    };
    let corpus = node_entry.entity_count;

    // Correctness fence: the prefilter plan is admitted only when the ranked
    // table's FTS index covers ALL fragments (see
    // `TableStore::fts_covers_all_fragments`); a coverage probe failure is
    // conservatively treated as uncovered. Never overridden by force.
    match snapshot.open_lance_dataset(&node_key).await {
        Ok(ds) => {
            for prop in &bm25_props {
                match crate::table_store::TableStore::fts_covers_all_fragments(&ds, prop).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => {
                        fall_back(RrfGateFallback::Coverage, forced, None, Some(corpus));
                        return None;
                    }
                }
            }
        }
        Err(_) => {
            fall_back(RrfGateFallback::Coverage, forced, None, Some(corpus));
            return None;
        }
    }

    // On build Err the gate falls back — the postfilter plan needs no CSR up
    // front. (`GraphIndexHandle::get` is lazy and fallible; the traversal
    // will surface a real failure on its own terms later.)
    let graph = match graph_index.get().await {
        Ok(Some(graph)) => graph,
        Ok(None) | Err(_) => {
            fall_back(RrfGateFallback::BuildErr, forced, None, Some(corpus));
            return None;
        }
    };

    // The dense space is built from edge-table endpoints only: a ranked type
    // absent from it has no node with any edge — the eligible set is empty.
    let Some(idx) = graph.type_index(ranked_type) else {
        fall_back(
            RrfGateFallback::EmptyEligible,
            forced,
            Some(0),
            Some(corpus),
        );
        return None;
    };

    // Resolve each source Expand to the adjacency (CSR for Out, CSC for In,
    // union for Both) whose `has_neighbors` answers "does this node satisfy
    // the source's first edge". Several sources intersect — still a superset
    // of the traversal's survivors. An edge type with no built adjacency has
    // zero edges, so the intersection is empty. The endpoint-type check and
    // the width check are LOAD-BEARING: `has_neighbors` indexes
    // `offsets[n + 1]` unchecked, so a misaligned dense space would panic.
    let mut adjacencies: Vec<(
        Option<&crate::graph_index::CsrIndex>,
        Option<&crate::graph_index::CsrIndex>,
    )> = Vec::with_capacity(sources.len());
    for (edge_type, direction) in &sources {
        let Some(edge_def) = catalog.edge_types.get(*edge_type) else {
            fall_back(RrfGateFallback::Shape, forced, None, Some(corpus));
            return None;
        };
        let side_matches = match direction {
            Direction::Out => edge_def.from_type == ranked_type,
            Direction::In => edge_def.to_type == ranked_type,
            // Undirected is same-type only (typecheck T22).
            Direction::Both => edge_def.from_type == ranked_type && edge_def.to_type == ranked_type,
        };
        if !side_matches {
            fall_back(RrfGateFallback::Shape, forced, None, Some(corpus));
            return None;
        }
        let (out, incoming) = match direction {
            Direction::Out => (graph.csr(edge_type), None),
            Direction::In => (None, graph.csc(edge_type)),
            Direction::Both => (graph.csr(edge_type), graph.csc(edge_type)),
        };
        if out.is_none() && incoming.is_none() {
            fall_back(
                RrfGateFallback::EmptyEligible,
                forced,
                Some(0),
                Some(corpus),
            );
            return None;
        }
        for adjacency in [out, incoming].into_iter().flatten() {
            if adjacency.num_nodes() != idx.len() {
                fall_back(RrfGateFallback::BuildErr, forced, None, Some(corpus));
                return None;
            }
        }
        adjacencies.push((out, incoming));
    }

    let passes = |dense: u32| {
        adjacencies.iter().all(|(out, incoming)| {
            out.is_some_and(|adj| adj.has_neighbors(dense))
                || incoming.is_some_and(|adj| adj.has_neighbors(dense))
        })
    };

    // COUNT pass only — no allocation until the prefilter plan is chosen.
    let eligible_count = (0..idx.len() as u32).filter(|&dense| passes(dense)).count() as u64;
    if eligible_count == 0 {
        fall_back(
            RrfGateFallback::EmptyEligible,
            forced,
            Some(0),
            Some(corpus),
        );
        return None;
    }

    // Threshold: ratio AND absolute cap, both after the zero override; pure
    // cost tuning (the one decision a force may override).
    if !forced {
        let ratio_ok = corpus > 0 && (eligible_count as f64) <= rrf_gate_ratio() * (corpus as f64);
        let cap_ok = eligible_count <= rrf_gate_max_ids() as u64;
        if !(ratio_ok && cap_ok) {
            fall_back(
                RrfGateFallback::Threshold,
                false,
                Some(eligible_count),
                Some(corpus),
            );
            return None;
        }
    }

    let mut ids: Vec<String> = Vec::with_capacity(eligible_count as usize);
    for dense in 0..idx.len() as u32 {
        if passes(dense) {
            if let Some(id) = idx.to_id(dense) {
                ids.push(id.to_string());
            }
        }
    }
    // The count pass and this materialization share `passes`, and `to_id` is
    // total over the dense range, so a mismatch is unreachable — but if one
    // ever appears, a silently SHRUNK set would be a subset error (the one
    // answer-changing direction). Fall back instead: cost-only, like every
    // other impossible-state fence in this gate.
    if ids.len() as u64 != eligible_count {
        fall_back(
            RrfGateFallback::BuildErr,
            forced,
            Some(eligible_count),
            Some(corpus),
        );
        return None;
    }
    // Test-only red control (`instrumentation::with_rrf_gate_subset_drop`):
    // deliberately violates the superset rule so the differential oracle can
    // prove it detects a subset. Compiled out of release binaries with its
    // seam — an answer-corrupting hook must not exist in production.
    #[cfg(debug_assertions)]
    if let Some(dropped) = crate::instrumentation::rrf_gate_subset_drop() {
        ids.retain(|id| *id != dropped);
    }
    record_rrf_gate_verdict(RrfGateVerdict {
        plan: RrfGatePlan::Prefilter,
        fallback: None,
        forced,
        eligible: Some(eligible_count),
        corpus: Some(corpus),
    });
    Some(EligibleIds(Arc::new(ids)))
}

/// This arm's mode with the eligible-id prefilter attached iff the arm
/// carries a bm25 target (both arms when both are bm25). A
/// `nearest` arm passes through untouched and runs identical code in both
/// plans — prefiltering its `k`-truncated scan would change answers.
fn arm_with_bm25_prefilter(arm: &SearchMode, ids: &EligibleIds) -> SearchMode {
    if arm.bm25.is_some() {
        SearchMode {
            bm25_eligible_ids: Some(ids.clone()),
            ..arm.clone()
        }
    } else {
        arm.clone()
    }
}

/// One RRF pass: run both arms, fuse their ranks, reconstruct and limit.
///
/// INPUT CONTRACT: bm25 arms are complete rankings, never capped — see
/// `extract_sub_search_mode`. (The `nearest` arm was always truncated at `k`.)
async fn execute_rrf_fusion(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    rrf: &RrfMode,
) -> Result<QueryResult> {
    debug_assert!(
        rrf.primary.bm25_scan_limit.is_none() && rrf.secondary.bm25_scan_limit.is_none(),
        "rrf arms must be complete rankings (see extract_sub_search_mode)"
    );
    let mut needed_columns = collect_needed_columns(ir);
    fail_open_rrf_leg_targets(&mut needed_columns, rrf);

    // The prefilter gate: the eligible-id set is computed ONCE here, above
    // the arms, then threaded ASYMMETRICALLY — into an arm iff it carries an
    // uncapped bm25 target on the ranked variable (both arms when both are
    // bm25), never into a `nearest` arm, which runs identical code in both
    // plans. The asymmetry must live at this threading site:
    // `execute_node_scan` applies its filters unconditionally and has no arm
    // identity of its own. (The both-legs symmetry `fail_open_rrf_leg_targets`
    // enforces for COLUMNS deliberately does not extend to rows.)
    let eligible = rrf_prefilter_gate(ir, snapshot, graph_index, catalog, rrf).await;
    let gated = eligible.as_ref().map(|ids| {
        (
            arm_with_bm25_prefilter(&rrf.primary, ids),
            arm_with_bm25_prefilter(&rrf.secondary, ids),
        )
    });
    let (primary_mode, secondary_mode) = match &gated {
        Some((primary, secondary)) => (primary, secondary),
        None => (rrf.primary.as_ref(), rrf.secondary.as_ref()),
    };

    // Execute primary search
    let mut primary_wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut primary_wide,
        primary_mode,
        None,
        &needed_columns,
    )
    .await?;

    // Execute secondary search
    let mut secondary_wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut secondary_wide,
        secondary_mode,
        None,
        &needed_columns,
    )
    .await?;

    // For RRF, we need to find the main binding variable
    // (the one that both searches operate on)
    let primary_var = rrf
        .primary
        .nearest
        .as_ref()
        .map(|(v, ..)| v.as_str())
        .or_else(|| rrf.primary.bm25.as_ref().map(|(v, ..)| v.as_str()))
        .ok_or_else(|| OmniError::manifest("rrf primary must be nearest or bm25".to_string()))?;

    let primary_batch = primary_wide.as_ref().ok_or_else(|| {
        OmniError::manifest(format!(
            "rrf primary variable '{}' not in bindings",
            primary_var
        ))
    })?;
    let secondary_batch = secondary_wide.as_ref().ok_or_else(|| {
        // Name the secondary arm's own target — this message interpolated
        // the primary variable, misdirecting a secondary-leg failure.
        let secondary_var = rrf
            .secondary
            .nearest
            .as_ref()
            .map(|(v, ..)| v.as_str())
            .or_else(|| rrf.secondary.bm25.as_ref().map(|(v, ..)| v.as_str()))
            .unwrap_or(primary_var);
        OmniError::manifest(format!(
            "rrf secondary variable '{}' not in bindings",
            secondary_var
        ))
    })?;

    // Build entity-ID → rank maps. A downstream traversal may fan one
    // ranked entity out to several result rows; those rows all have the same
    // search rank and must not consume additional rank ordinals.
    let id_col_name = format!("{}.id", primary_var);
    let primary_ids = extract_id_column_by_name(primary_batch, &id_col_name)?;
    let secondary_ids = extract_id_column_by_name(secondary_batch, &id_col_name)?;

    let mut primary_rank: HashMap<String, usize> = HashMap::new();
    let mut primary_unique: Vec<String> = Vec::new();
    for id in &primary_ids {
        if !primary_rank.contains_key(id) {
            primary_rank.insert(id.clone(), primary_unique.len());
            primary_unique.push(id.clone());
        }
    }
    let mut secondary_rank: HashMap<String, usize> = HashMap::new();
    let mut secondary_unique: Vec<String> = Vec::new();
    for id in &secondary_ids {
        if !secondary_rank.contains_key(id) {
            secondary_rank.insert(id.clone(), secondary_unique.len());
            secondary_unique.push(id.clone());
        }
    }

    // Collect all unique IDs
    let mut all_ids: Vec<String> = primary_unique;
    for id in &secondary_unique {
        if !primary_rank.contains_key(id) {
            all_ids.push(id.clone());
        }
    }

    // Compute RRF scores. NOTE: each arm's rank is derived from the arm
    // batch's first-seen row order, which equals search rank today only
    // because the BFS emits each hop's rows in seed input order and every
    // seed's first in-bounds row lands at its first emitting hop. Under
    // `min_hops >= 2` with heterogeneous per-seed reach that coincidence
    // breaks and fused ranks drift; the durable fix is deriving arm ranks
    // from the arms' `_distance`/`_score` columns like the single-search
    // path.
    let k = rrf.k as f64;
    let mut scored: Vec<(String, f64)> = all_ids
        .iter()
        .map(|id| {
            let p = primary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            let s = secondary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            (id.clone(), p + s)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(rrf.limit);

    // Collect winning entity IDs in order. Every downstream row belonging to
    // a winner survives; fusion ranks entities, not arbitrary fanout rows.
    let winning_ids: Vec<String> = scored.iter().map(|(id, _)| id.clone()).collect();

    // Build a combined row source: prefer the primary arm for an entity it
    // contains, otherwise use the secondary arm. The downstream pipeline is
    // identical in both arms, so either contains the same fanout rows.
    let mut primary_rows: HashMap<String, Vec<u32>> = HashMap::new();
    for (i, id) in primary_ids.iter().enumerate() {
        primary_rows.entry(id.clone()).or_default().push(i as u32);
    }
    let mut secondary_rows: HashMap<String, Vec<u32>> = HashMap::new();
    for (i, id) in secondary_ids.iter().enumerate() {
        secondary_rows.entry(id.clone()).or_default().push(i as u32);
    }

    // Reconstruct a combined batch in fused entity order, retaining each
    // entity's rows in their pipeline order.
    let fused_batch = build_fused_batch(
        &winning_ids,
        primary_batch,
        &primary_rows,
        secondary_batch,
        &secondary_rows,
    )?;

    // Project directly from fused batch
    let mut result_batch = project_return(&fused_batch, &ir.return_exprs, params)?;
    // `rrf.limit` is the query's row limit. A winning entity can now own more
    // than one row after traversal, so enforce the limit after reconstruction.
    let len = result_batch.num_rows().min(rrf.limit);
    result_batch = result_batch.slice(0, len);

    // Already ordered by RRF score + already limited
    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

fn extract_id_column_by_name(batch: &RecordBatch, col_name: &str) -> Result<Vec<String>> {
    let col = batch.column_by_name(col_name).ok_or_else(|| {
        OmniError::manifest(format!("batch missing '{}' column for RRF", col_name))
    })?;
    let ids = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", col_name)))?;
    Ok((0..ids.len()).map(|i| ids.value(i).to_string()).collect())
}

fn build_fused_batch(
    ordered_ids: &[String],
    primary_batch: &RecordBatch,
    primary_rows: &HashMap<String, Vec<u32>>,
    secondary_batch: &RecordBatch,
    secondary_rows: &HashMap<String, Vec<u32>>,
) -> Result<RecordBatch> {
    if ordered_ids.is_empty() {
        return Ok(RecordBatch::new_empty(primary_batch.schema()));
    }

    // Gather every row for each winning entity, preserving both fused entity
    // order and the downstream row order within that entity.
    let mut row_slices: Vec<RecordBatch> = Vec::with_capacity(ordered_ids.len());
    for id in ordered_ids {
        if let Some(rows) = primary_rows.get(id) {
            row_slices.push(take_batch(primary_batch, &UInt32Array::from(rows.clone()))?);
        } else if let Some(rows) = secondary_rows.get(id) {
            row_slices.push(take_batch(
                secondary_batch,
                &UInt32Array::from(rows.clone()),
            )?);
        }
    }

    if row_slices.is_empty() {
        return Ok(RecordBatch::new_empty(primary_batch.schema()));
    }

    let schema = row_slices[0].schema();
    arrow_select::concat::concat_batches(&schema, &row_slices).map_err(OmniError::arrow_internal)
}

/// Check if a filter is a text search filter that needs Lance SQL pushdown.
fn is_search_filter(filter: &IRFilter) -> bool {
    matches!(
        &filter.left,
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }
    )
}

/// Extract the variable name from a search filter's field expression.
fn search_filter_variable(filter: &IRFilter) -> Option<&str> {
    let field = match &filter.left {
        IRExpr::Search { field, .. } => field,
        IRExpr::Fuzzy { field, .. } => field,
        IRExpr::MatchText { field, .. } => field,
        _ => return None,
    };
    match field.as_ref() {
        IRExpr::PropAccess { variable, .. } => Some(variable.as_str()),
        _ => None,
    }
}

/// Collect every binding variable referenced by an expression into `vars`.
fn collect_expr_variables(expr: &IRExpr, vars: &mut HashSet<String>) {
    match expr {
        IRExpr::PropAccess { variable, .. } => {
            vars.insert(variable.clone());
        }
        IRExpr::Nearest {
            variable, query, ..
        } => {
            vars.insert(variable.clone());
            collect_expr_variables(query, vars);
        }
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            collect_expr_variables(field, vars);
            collect_expr_variables(query, vars);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            collect_expr_variables(field, vars);
            collect_expr_variables(query, vars);
            if let Some(e) = max_edits {
                collect_expr_variables(e, vars);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            collect_expr_variables(primary, vars);
            collect_expr_variables(secondary, vars);
            if let Some(e) = k {
                collect_expr_variables(e, vars);
            }
        }
        IRExpr::Variable(v) => {
            vars.insert(v.clone());
        }
        IRExpr::Aggregate { arg, .. } => collect_expr_variables(arg, vars),
        IRExpr::Param(_) | IRExpr::Literal(_) | IRExpr::AliasRef(_) => {}
    }
}

/// The set of binding variables a filter references, across both operands.
///
/// A single-binding pushable filter (`starts_with`, string `contains`,
/// equality, range, …) is hoisted onto the op that introduces that binding,
/// where Lance can probe a covering index; a cross-variable filter references
/// two bindings and stays in the in-memory arm on the joined batch.
fn filter_variables(filter: &IRFilter) -> HashSet<String> {
    let mut vars = HashSet::new();
    collect_expr_variables(&filter.left, &mut vars);
    collect_expr_variables(&filter.right, &mut vars);
    vars
}

/// Columns a query references for one bound variable, accumulated by
/// [`collect_needed_columns`]. `All` is the fail-open verdict for an
/// entity-valued reference (a bare `$var`): that scan keeps the full
/// projection, so unattributable references never drop a column — only a
/// walk gap on an attributed binding can (see `collect_pipeline_columns`).
#[derive(Debug)]
enum NeededColumns {
    All,
    Columns(HashSet<String>),
}

/// Derive each binding's needed columns from the whole query: RETURN
/// expressions, `order {}`, and every filter in the pipeline, recursing into
/// `AntiJoin` inners. Keys are variable names, query-global — same-name
/// anti-join bindings merge by union (a superset; every scan re-intersects
/// with its own schema). Filter columns stay in the demand set even when the
/// filter hoists into the scanner: over-demand costs one scalar column.
fn collect_needed_columns(ir: &QueryIR) -> HashMap<String, NeededColumns> {
    // Destructured so a new column-bearing `QueryIR` field cannot be missed
    // silently (same discipline as the exhaustive matches below).
    let QueryIR {
        name: _,
        params: _,
        pipeline,
        return_exprs,
        order_by,
        limit: _,
    } = ir;
    let mut needed = HashMap::new();
    collect_pipeline_columns(pipeline, &mut needed);
    for IRProjection { expr, alias: _ } in return_exprs {
        collect_expr_columns(expr, &mut needed);
    }
    for IROrdering {
        expr,
        descending: _,
    } in order_by
    {
        collect_expr_columns(expr, &mut needed);
    }
    needed
}

fn collect_pipeline_columns(pipeline: &[IROp], needed: &mut HashMap<String, NeededColumns>) {
    for op in pipeline {
        // No `_` arm, and no `..` in any pattern: a new IROp — or a new FIELD
        // on an existing one — must decide here whether it references columns.
        // An unwalked reference prunes a column something still reads, which
        // the `All` fail-open cannot catch.
        match op {
            IROp::NodeScan {
                variable: _,
                type_name: _,
                filters,
            } => {
                for filter in filters {
                    collect_filter_columns(filter, needed);
                }
            }
            IROp::Expand {
                src_var: _,
                dst_var: _,
                edge_type: _,
                direction: _,
                dst_type: _,
                min_hops: _,
                max_hops: _,
                dst_filters,
                edge_binding: _,
            } => {
                for filter in dst_filters {
                    collect_filter_columns(filter, needed);
                }
            }
            IROp::Filter(filter) => collect_filter_columns(filter, needed),
            IROp::AntiJoin {
                outer_var: _,
                inner,
            } => collect_pipeline_columns(inner, needed),
        }
    }
}

fn collect_filter_columns(filter: &IRFilter, needed: &mut HashMap<String, NeededColumns>) {
    let IRFilter { left, op: _, right } = filter;
    collect_expr_columns(left, needed);
    collect_expr_columns(right, needed);
}

fn record_prop(needed: &mut HashMap<String, NeededColumns>, variable: &str, property: &str) {
    match needed
        .entry(variable.to_string())
        .or_insert_with(|| NeededColumns::Columns(HashSet::new()))
    {
        NeededColumns::All => {}
        NeededColumns::Columns(columns) => {
            columns.insert(property.to_string());
        }
    }
}

fn collect_expr_columns(expr: &IRExpr, needed: &mut HashMap<String, NeededColumns>) {
    match expr {
        IRExpr::PropAccess { variable, property } => record_prop(needed, variable, property),
        // Fail open — see `NeededColumns::All`.
        IRExpr::Variable(variable) => {
            needed.insert(variable.clone(), NeededColumns::All);
        }
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            record_prop(needed, variable, property);
            collect_expr_columns(query, needed);
        }
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            collect_expr_columns(field, needed);
            collect_expr_columns(query, needed);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            collect_expr_columns(field, needed);
            collect_expr_columns(query, needed);
            if let Some(max_edits) = max_edits {
                collect_expr_columns(max_edits, needed);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            collect_expr_columns(primary, needed);
            collect_expr_columns(secondary, needed);
            if let Some(k) = k {
                collect_expr_columns(k, needed);
            }
        }
        IRExpr::Aggregate { func: _, arg } => collect_expr_columns(arg, needed),
        // AliasRef resolves to another RETURN item, whose expression this
        // walk already visits directly; Param/Literal carry no columns.
        IRExpr::AliasRef(_) | IRExpr::Param(_) | IRExpr::Literal(_) => {}
    }
}

/// Both RRF legs run the same pipeline against one demand map, and
/// `build_fused_batch` concats winner rows from both legs under one schema —
/// so a binding that is a search target in EITHER leg must take the same
/// fail-open verdict in BOTH legs, keeping the legs' pruned BASE columns
/// identical. (Lance's autoprojected `_distance`/`_score` columns still
/// differ between mixed-kind legs — a pre-existing fusion hazard this
/// marking neither causes nor cures.)
fn fail_open_rrf_leg_targets(needed: &mut HashMap<String, NeededColumns>, rrf: &RrfMode) {
    for leg in [rrf.primary.as_ref(), rrf.secondary.as_ref()] {
        // Both marks run independently (not first-of): over-marking is always
        // sound, and this needs no leg-carries-one-target precondition.
        if let Some((variable, ..)) = leg.nearest.as_ref() {
            needed.insert(variable.clone(), NeededColumns::All);
        }
        if let Some((variable, ..)) = leg.bm25.as_ref() {
            needed.insert(variable.clone(), NeededColumns::All);
        }
    }
}

fn execute_pipeline<'a>(
    pipeline: &'a [IROp],
    params: &'a ParamMap,
    snapshot: &'a Snapshot,
    graph_index: &'a GraphIndexHandle<'a>,
    catalog: &'a Catalog,
    wide: &'a mut Option<RecordBatch>,
    search_mode: &'a SearchMode,
    // The query's `limit`, pushable into a final Expand as an emission bound.
    // `Some` only from `execute_query` when the query-level conditions hold
    // (unordered, aggregate-free, plain search mode); the op-level conditions
    // (effectively-last Expand, no destination filters, no edge binding) are
    // checked at the op site below. Anti-join inner pipelines and RRF arms
    // always pass `None`.
    final_expand_cap: Option<usize>,
    needed_columns: &'a HashMap<String, NeededColumns>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        // Pre-pass: hoist filters onto the op that introduces their binding.
        // Search filters always move to the binding's NodeScan (applied via
        // `scanner.full_text_search`). Scalar filters referencing exactly one
        // binding move to that binding's NodeScan (`filter_expr`, which also
        // arms `prefilter(true)` so a `nearest`/`bm25` on the same scanner is
        // filtered BEFORE top-k instead of starved after it) or into the
        // introducing Expand's `dst_filters` (applied during hydration). This
        // single-binding rule is what pushes `starts_with` / string `contains`
        // to a covering BTREE/NGRAM index while keeping a cross-variable
        // predicate in the in-memory arm.
        // Multi-binding filters (e.g. the cycle-closing `temp.id = dst.id`)
        // and filters on a variable not introduced here (an outer binding
        // inside an anti-join pipeline) keep their end-of-pipeline placement.
        let mut scan_vars: HashSet<&str> = HashSet::new();
        let mut expand_dst_vars: HashSet<&str> = HashSet::new();
        for op in pipeline {
            match op {
                IROp::NodeScan { variable, .. } => {
                    scan_vars.insert(variable.as_str());
                }
                IROp::Expand { dst_var, .. } => {
                    expand_dst_vars.insert(dst_var.as_str());
                }
                IROp::Filter(_) | IROp::AntiJoin { .. } => {}
            }
        }

        let mut hoisted_search_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();
        let mut hoisted_scan_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();
        let mut hoisted_dst_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();
        let mut hoisted_indices: HashSet<usize> = HashSet::new();
        for (i, op) in pipeline.iter().enumerate() {
            let IROp::Filter(filter) = op else { continue };
            if is_search_filter(filter) {
                if let Some(var) = search_filter_variable(filter) {
                    hoisted_search_filters
                        .entry(var.to_string())
                        .or_default()
                        .push(filter.clone());
                    hoisted_indices.insert(i);
                }
                continue;
            }
            let mut vars = filter_variables(filter).into_iter();
            let (Some(var), None) = (vars.next(), vars.next()) else {
                continue;
            };
            // Only pushable filters may leave their in-memory position:
            // `execute_node_scan` silently ignores filters `ir_filter_to_expr`
            // cannot lower (no post-scan fallback there). The schema arg only
            // affects a literal's type, never Some-vs-None, so `None` here
            // gives the same verdict as the scan site.
            if ir_filter_to_expr(filter, params, None).is_none() {
                continue;
            }
            let target = if scan_vars.contains(var.as_str()) {
                &mut hoisted_scan_filters
            } else if expand_dst_vars.contains(var.as_str()) {
                &mut hoisted_dst_filters
            } else {
                continue;
            };
            target.entry(var).or_default().push(filter.clone());
            hoisted_indices.insert(i);
        }

        // The last op that will actually execute (hoisted filters are skipped
        // in place). Only an Expand in this position may take the emission
        // cap: any later executing op could drop or multiply rows, making
        // emitted pairs and result rows diverge.
        let last_effective_idx = (0..pipeline.len())
            .rev()
            .find(|i| !hoisted_indices.contains(i));

        for (i, op) in pipeline.iter().enumerate() {
            // Skip hoisted search filters
            if hoisted_indices.contains(&i) {
                continue;
            }
            match op {
                IROp::NodeScan {
                    variable,
                    type_name,
                    filters,
                } => {
                    // Merge inline filters with hoisted search + scalar filters
                    let mut all_filters: Vec<IRFilter> = filters.clone();
                    if let Some(extra) = hoisted_search_filters.get(variable) {
                        all_filters.extend(extra.iter().cloned());
                    }
                    if let Some(extra) = hoisted_scan_filters.get(variable) {
                        all_filters.extend(extra.iter().cloned());
                    }
                    let batch = execute_node_scan(
                        type_name,
                        variable,
                        &all_filters,
                        params,
                        snapshot,
                        catalog,
                        search_mode,
                        needed_columns.get(variable.as_str()),
                    )
                    .await?;
                    let prefixed = prefix_batch(&batch, variable)?;
                    *wide = Some(match wide.take() {
                        None => prefixed,
                        Some(existing) => cross_join_batches(&existing, &prefixed)?,
                    });
                }
                IROp::Filter(filter) => {
                    if let Some(batch) = wide.as_mut() {
                        apply_filter(batch, filter, params)?;
                    }
                }
                IROp::Expand {
                    src_var,
                    dst_var,
                    edge_type,
                    direction,
                    dst_type,
                    min_hops,
                    max_hops,
                    dst_filters,
                    edge_binding,
                } => {
                    // Merge lowered destination filters with hoisted ones
                    let mut all_dst_filters: Vec<IRFilter> = dst_filters.clone();
                    if let Some(extra) = hoisted_dst_filters.get(dst_var) {
                        all_dst_filters.extend(extra.iter().cloned());
                    }
                    // Emission cap (limit pushdown): only for the effectively
                    // last op, and only when nothing can drop emitted pairs
                    // after the traversal — destination filters (lowered OR
                    // hoisted) drop rows at hydration, and a bound edge emits
                    // one row per edge ROW rather than per pair.
                    let emit_cap = match final_expand_cap {
                        Some(cap)
                            if Some(i) == last_effective_idx
                                && all_dst_filters.is_empty()
                                && edge_binding.is_none() =>
                        {
                            Some(cap)
                        }
                        _ => None,
                    };
                    if let Some(batch) = wide.as_mut() {
                        execute_expand(
                            batch,
                            graph_index,
                            snapshot,
                            catalog,
                            src_var,
                            dst_var,
                            edge_type,
                            *direction,
                            dst_type,
                            *min_hops,
                            *max_hops,
                            &all_dst_filters,
                            edge_binding.as_deref(),
                            params,
                            emit_cap,
                        )
                        .await?;
                    }
                }
                IROp::AntiJoin { outer_var, inner } => {
                    let gi = graph_index;
                    if let Some(batch) = wide.as_mut() {
                        execute_anti_join(
                            batch,
                            inner,
                            params,
                            snapshot,
                            gi,
                            catalog,
                            outer_var,
                            needed_columns,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    })
}

/// The edge types a query's pipeline actually traverses, mapped to their
/// `(from_type, to_type)` endpoints. Recurses through `AntiJoin` inner pipelines
/// (whose bulk fast path consumes the CSR for the inner `Expand`'s edge). The
/// CSR build is scoped to exactly this set instead of every edge type in the
/// catalog — otherwise a single-edge join (`$x identifiesPerson $p`) that lands
/// on the CSR path would scan the whole graph's edge data (every message,
/// relationship, … table), the cause of the cross-edge-join hang. Empty when the
/// only traversal is an `AntiJoin` with no inner `Expand` — that shape never asks
/// the handle for an index, so an empty build is never realized.
fn referenced_edge_types(
    pipeline: &[IROp],
    catalog: &Catalog,
) -> HashMap<String, (String, String)> {
    let mut names = std::collections::BTreeSet::new();
    collect_referenced_edge_names(pipeline, &mut names);
    names
        .into_iter()
        .filter_map(|name| {
            catalog
                .edge_types
                .get(&name)
                .map(|et| (name, (et.from_type.clone(), et.to_type.clone())))
        })
        .collect()
}

fn collect_referenced_edge_names(pipeline: &[IROp], out: &mut std::collections::BTreeSet<String>) {
    for op in pipeline {
        match op {
            IROp::Expand { edge_type, .. } => {
                out.insert(edge_type.clone());
            }
            IROp::AntiJoin { inner, .. } => collect_referenced_edge_names(inner, out),
            // Exhaustive on purpose (no `_` arm): a new edge-referencing IROp must
            // force a compile error here rather than silently under-scope the CSR
            // build — an omitted edge would fail at runtime with "no adjacency
            // index for edge". The non-traversal ops reference no edges.
            IROp::NodeScan { .. } | IROp::Filter(_) => {}
        }
    }
}

/// Lazily provides the in-memory CSR graph index, building it on first use and
/// memoizing for the rest of the query. Indexed-mode Expand never asks for it,
/// so a query that is entirely index-served and has no AntiJoin never pays the
/// O(|E|) CSR build (the whole point of the indexed path). The `Cached` builder
/// also reuses the cross-query `RuntimeCache` entry; `Direct` builds against an
/// arbitrary snapshot (time-travel reads); `None` is for queries with no
/// traversal at all.
pub struct GraphIndexHandle<'a> {
    cell: tokio::sync::OnceCell<Option<Arc<GraphIndex>>>,
    builder: GraphIndexBuilder<'a>,
}

enum GraphIndexBuilder<'a> {
    None,
    Cached(
        &'a Omnigraph,
        &'a crate::db::ResolvedTarget,
        HashMap<String, (String, String)>,
    ),
    Direct(&'a Snapshot, HashMap<String, (String, String)>),
}

impl<'a> GraphIndexHandle<'a> {
    fn none() -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::None,
        }
    }

    fn cached(
        db: &'a Omnigraph,
        resolved: &'a crate::db::ResolvedTarget,
        edge_types: HashMap<String, (String, String)>,
    ) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Cached(db, resolved, edge_types),
        }
    }

    fn direct(snapshot: &'a Snapshot, edge_types: HashMap<String, (String, String)>) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Direct(snapshot, edge_types),
        }
    }

    /// The CSR index, built on first call. `None` only when the query needs no
    /// traversal (the `None` builder).
    async fn get(&self) -> Result<Option<&GraphIndex>> {
        let built = self
            .cell
            .get_or_try_init(|| async {
                match &self.builder {
                    GraphIndexBuilder::None => Ok::<Option<Arc<GraphIndex>>, OmniError>(None),
                    GraphIndexBuilder::Cached(db, resolved, edge_types) => Ok(Some(
                        db.graph_index_for_resolved(resolved, edge_types).await?,
                    )),
                    GraphIndexBuilder::Direct(snapshot, edge_types) => Ok(Some(Arc::new(
                        GraphIndex::load_or_build(snapshot, edge_types, None).await?,
                    ))),
                }
            })
            .await?;
        Ok(built.as_deref())
    }

    /// Whether the in-memory CSR is already materialized for this query (a prior
    /// Expand or bulk AntiJoin realized it), so reusing it is ~free. Lets the
    /// cost chooser prefer the warm CSR over per-hop indexed scans.
    fn is_built(&self) -> bool {
        matches!(self.cell.get(), Some(Some(_)))
    }
}

/// Explicit traversal-mode override. `OMNIGRAPH_TRAVERSAL_MODE=indexed|csr`
/// forces the path (ops escape hatch + test hook). Both modes are semantically
/// identical, so the override only changes which path runs, never the result.
fn traversal_indexed_override() -> Option<bool> {
    // The scoped test seam (`with_traversal_mode`) takes precedence over the
    // process-global `OMNIGRAPH_TRAVERSAL_MODE` ops escape hatch.
    let mode = crate::instrumentation::traversal_mode_override()
        .map(str::to_string)
        .or_else(|| std::env::var("OMNIGRAPH_TRAVERSAL_MODE").ok());
    match mode.as_deref() {
        Some("indexed") => Some(true),
        Some("csr") => Some(false),
        _ => None,
    }
}

/// Max source-row frontier for which Expand uses the BTREE-indexed path.
/// Larger frontiers fall back to the in-memory CSR (dense / whole-graph). See
/// `docs/dev/execution.md`.
const DEFAULT_EXPAND_INDEXED_MAX_FRONTIER: usize = 1024;
/// Max hop count for the indexed path (each hop is one indexed scan; very deep
/// traversals fan out toward whole-graph and are better served by CSR).
const DEFAULT_EXPAND_INDEXED_MAX_HOPS: u32 = 6;

fn expand_indexed_max_frontier() -> usize {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_FRONTIER)
}

fn expand_indexed_max_hops() -> u32 {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_HOPS)
}

/// The two Expand execution paths the chooser dispatches between. Extensible:
/// a future persisted-adjacency artifact would become a third variant here, and
/// `choose_expand_mode` would learn to prefer it when covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpandMode {
    /// Per-hop neighbor lookup via the persisted src/dst BTREE. Work scales
    /// with the frontier, not |E| — best for selective traversals.
    IndexedScan,
    /// Whole-graph in-memory CSR (built once, reused). Best for dense / deep /
    /// large-frontier traversals, or when the index is degraded and a full
    /// scan would be paid per hop anyway.
    Csr,
}

/// Building the in-memory CSR costs more than a bare edge scan: it scans every
/// edge AND allocates + groups the adjacency. This factor expresses that
/// overhead so a one-off degraded single-hop scan can still edge out a full CSR
/// build. The crossover is insensitive to its exact value.
const CSR_BUILD_FACTOR: f64 = 1.5;

/// Cardinality inputs for the (pure, IO-free) traversal-mode cost model. Every
/// field is a cheap manifest-resident count or an already-in-hand value — the
/// chooser performs no scans.
#[derive(Debug, Clone)]
struct ExpandCostInputs {
    /// Current frontier size (`wide.num_rows()`).
    frontier_rows: usize,
    /// |E| for the edge type (manifest `row_count`).
    edge_count: u64,
    /// |V_src| — node count of the keyed endpoint type (manifest `row_count`).
    src_node_count: u64,
    /// Effective max hop count for this Expand.
    effective_max_hops: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`).
    max_hops_cap: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`).
    max_frontier_cap: usize,
    /// Whether `scan_edges_by_endpoint`'s `key_col IN (...)` is served by the
    /// BTREE (`Indexed`) or silently falls back to a full scan (`Degraded`).
    coverage: crate::table_store::IndexCoverage,
    /// Whether the cross-query CSR for this snapshot+edge-version is already
    /// built (making the CSR path ≈ free). Conservatively `false` until the
    /// cache-peek is wired (the plan's optional refinement).
    csr_cached: bool,
    /// Endpoint probes the indexed path issues per hop: 1 for a directed
    /// traversal, 2 for undirected (`Direction::Both` scans BOTH the src-keyed
    /// and dst-keyed orientations — see `endpoint_probes`). Without this the
    /// cost model priced an undirected traversal at half its probe count and
    /// half its effective degree.
    probe_factor: f64,
}

/// Pure cost-based traversal-mode chooser. Compares an estimate of the indexed
/// path's frontier-relative work against the cost of building (or reusing) the
/// whole-graph CSR, and picks the cheaper. Deterministic and IO-free so it is
/// unit-tested at the crossover; the caller supplies the manifest counts and the
/// (optionally degraded) index coverage.
///
/// Under `Indexed` coverage and a cold CSR the decision reduces to a clean
/// selectivity ratio — indexed wins when `hops * frontier < BUILD_FACTOR *
/// |V_src|`, i.e. when the frontier is a small fraction of the source vertex
/// set — which is independent of |E| (the flat-in-|E| property PR #149 shipped).
fn choose_expand_mode(i: &ExpandCostInputs) -> ExpandMode {
    // Hard ceilings: very deep or very large frontiers fan out toward
    // whole-graph and are always better served by CSR, regardless of the cost
    // estimate. These preserve the documented semantics of the two cap flags.
    if i.effective_max_hops > i.max_hops_cap || i.frontier_rows > i.max_frontier_cap {
        return ExpandMode::Csr;
    }

    let hops = i.effective_max_hops.max(1) as f64;
    let frontier = i.frontier_rows as f64;
    let edges = i.edge_count as f64;
    let src = i.src_node_count.max(1) as f64;
    let fanout = edges / src;

    // Indexed work scales with the frontier when the BTREE serves the IN-list;
    // a degraded scan is a full edge scan per hop instead (the C6 perf cliff).
    // Either way an undirected traversal pays every hop twice (both probes).
    let indexed_cost = match i.coverage {
        crate::table_store::IndexCoverage::Indexed => hops * frontier * fanout * i.probe_factor,
        crate::table_store::IndexCoverage::Degraded { .. } => hops * edges * i.probe_factor,
    };
    // A warm CSR is ~free to reuse; a cold one costs a build over all edges.
    let csr_cost = if i.csr_cached {
        0.0
    } else {
        CSR_BUILD_FACTOR * edges
    };

    if indexed_cost < csr_cost {
        ExpandMode::IndexedScan
    } else {
        ExpandMode::Csr
    }
}

/// Mid-traversal re-decision (issue #533): asked at the top of every indexed
/// hop after the first with the OBSERVED union frontier, where the dispatch
/// decision only ever saw the initial one. Pure so the crossover is
/// unit-tested like `choose_expand_mode`.
///
/// Two triggers, either sufficient:
///
/// 1. **The hard frontier ceiling.** Dispatch enforces it only against the
///    initial frontier; here it becomes an execution bound.
/// 2. **Remaining-work estimate.** Projects the frontier forward over the
///    remaining hops using the OBSERVED per-hop growth ratio when it exceeds
///    the manifest's average fanout — heavy-tailed graphs (the #533 shape)
///    blow through the average, and the observed ratio is the only estimator
///    that sees hubs. Each projected frontier saturates at |V_src| (BFS
///    `visited` pruning caps reach). Switch when that estimate exceeds what
///    the CSR path still costs (a build when cold, ~nothing when warm) —
///    correct precisely because the switch CONTINUES from carried state, so
///    switching pays `csr_cost` once while staying pays the whole estimate.
fn should_switch_to_csr(
    observed_frontier: usize,
    prev_frontier: usize,
    remaining_hops: u32,
    csr_ready: bool,
    i: &ExpandCostInputs,
) -> bool {
    if observed_frontier > i.max_frontier_cap {
        return true;
    }
    let edges = i.edge_count as f64;
    let src = i.src_node_count.max(1) as f64;
    let fanout = edges / src;
    let observed_growth = if prev_frontier > 0 {
        observed_frontier as f64 / prev_frontier as f64
    } else {
        fanout
    };
    let growth = observed_growth.max(fanout).max(1.0);

    let mut remaining_cost = 0.0;
    let mut frontier = observed_frontier as f64;
    for _ in 0..remaining_hops {
        remaining_cost += frontier * fanout * i.probe_factor;
        frontier = (frontier * growth).min(src);
    }
    let csr_cost = if csr_ready {
        0.0
    } else {
        CSR_BUILD_FACTOR * edges
    };
    remaining_cost > csr_cost
}

/// Hops the indexed path will actually run, for cost-model purposes. A cross-type
/// edge cannot chain, so `execute_expand_indexed` caps it at one hop regardless of
/// the requested range; the cost model must use that, or it over-estimates the
/// indexed cost of a cross-type variable-length expand and skews toward CSR.
fn cost_effective_hops(requested_max_hops: u32, same_type: bool) -> u32 {
    if same_type {
        requested_max_hops
    } else {
        requested_max_hops.min(1)
    }
}

/// Gather the cost-model inputs from cheap manifest counts. `None` when the
/// edge type, its source node type, or their manifest entries are absent (e.g.
/// a not-yet-materialized table) — the caller then falls back to the legacy
/// frontier/hop ceiling so the decision is always defined.
fn gather_cost_inputs(
    snapshot: &Snapshot,
    catalog: &Catalog,
    edge_type: &str,
    direction: Direction,
    frontier_rows: usize,
    effective_max_hops: u32,
    coverage: crate::table_store::IndexCoverage,
    csr_cached: bool,
) -> Option<ExpandCostInputs> {
    let edge_entry = snapshot.dataset(&format!("edge:{}", edge_type))?;
    let edge_def = catalog.edge_types.get(edge_type)?;
    // Match the indexed path's cross-type one-hop cap so the cost estimate
    // reflects what actually runs (see `cost_effective_hops`).
    let effective_max_hops =
        cost_effective_hops(effective_max_hops, edge_def.from_type == edge_def.to_type);
    // The frontier source vertices are the keyed endpoint's type: `from` for an
    // Out traversal (keyed on `src`), `to` for In (keyed on `dst`).
    let src_type = match direction {
        Direction::Out => &edge_def.from_type,
        Direction::In => &edge_def.to_type,
        // Both requires from_type == to_type (typecheck T22).
        Direction::Both => &edge_def.from_type,
    };
    let src_entry = snapshot.dataset(&format!("node:{}", src_type))?;
    Some(ExpandCostInputs {
        frontier_rows,
        edge_count: edge_entry.entity_count,
        src_node_count: src_entry.entity_count,
        effective_max_hops,
        max_hops_cap: expand_indexed_max_hops(),
        max_frontier_cap: expand_indexed_max_frontier(),
        coverage,
        csr_cached,
        probe_factor: direction_probe_factor(direction),
    })
}

/// Per-hop probe multiplier for the indexed path: `endpoint_probes` issues one
/// scan for a directed traversal and two for an undirected one.
fn direction_probe_factor(direction: Direction) -> f64 {
    match direction {
        Direction::Out | Direction::In => 1.0,
        Direction::Both => 2.0,
    }
}

/// Coverage value to feed the cost decision. A failed coverage probe is treated
/// as `Degraded` (conservative: don't over-favor the indexed path when we can't
/// confirm the BTREE will serve the scan).
fn coverage_for_decision(
    coverage: &Result<crate::table_store::IndexCoverage>,
) -> crate::table_store::IndexCoverage {
    match coverage {
        Ok(c) => c.clone(),
        Err(_) => crate::table_store::IndexCoverage::Degraded {
            reason: "coverage check failed".to_string(),
        },
    }
}

/// Surface the C6 silent scalar-index fallback (commit `5a7ab6d`): warn when the
/// per-hop `key_col IN (...)` won't route through the BTREE. Detection-only;
/// never fails the query. Behavior-identical to the inline check it replaced.
fn warn_on_degraded_coverage(
    coverage: &Result<crate::table_store::IndexCoverage>,
    key_col: &str,
    edge_type: &str,
) {
    match coverage {
        Ok(crate::table_store::IndexCoverage::Degraded { reason }) => tracing::warn!(
            target: "omnigraph::traverse",
            edge = %edge_type,
            key_col = key_col,
            reason = %reason,
            "indexed traversal falls back to a full edge scan (results correct, perf degraded)"
        ),
        Ok(crate::table_store::IndexCoverage::Indexed) => {}
        Err(e) => tracing::debug!(
            target: "omnigraph::traverse",
            error = %e,
            "index-coverage check failed; proceeding with traversal"
        ),
    }
}

/// The (key, opposite) endpoint columns for a traversal direction. Out follows
/// src -> dst (key on src); In follows the reverse. The persisted BTREE exists
/// on both columns.
fn endpoint_columns(direction: Direction) -> (&'static str, &'static str) {
    match direction {
        Direction::Out => ("src", "dst"),
        // Both: the primary orientation (used by the cost probe; the indexed
        // execution loop adds the reverse probe itself via endpoint_probes).
        Direction::In => ("dst", "src"),
        Direction::Both => ("src", "dst"),
    }
}

/// The pessimistic combination of two coverage probes: Degraded dominates
/// (an undirected traversal pays whichever of its two columns is worse).
fn worse_coverage(
    a: crate::table_store::IndexCoverage,
    b: crate::table_store::IndexCoverage,
) -> crate::table_store::IndexCoverage {
    use crate::table_store::IndexCoverage;
    match (a, b) {
        (IndexCoverage::Indexed, IndexCoverage::Indexed) => IndexCoverage::Indexed,
        (IndexCoverage::Degraded { reason }, _) | (_, IndexCoverage::Degraded { reason }) => {
            IndexCoverage::Degraded { reason }
        }
    }
}

/// All (key, opposite) probes a direction requires: one for Out/In, both
/// orientations for an undirected traversal.
fn endpoint_probes(direction: Direction) -> &'static [(&'static str, &'static str)] {
    match direction {
        Direction::Out => &[("src", "dst")],
        Direction::In => &[("dst", "src")],
        Direction::Both => &[("src", "dst"), ("dst", "src")],
    }
}

/// Execute a graph traversal (Expand). Dispatches to the BTREE-indexed
/// strategy (selective traversals — neighbor lookups via the persisted
/// src/dst index) or the in-memory CSR strategy (dense / whole-graph
/// traversals); both run in the shared `execute_expand_bfs` core, which can
/// also swap indexed → CSR mid-traversal when the frontier outgrows the
/// dispatch decision (issue #533). The CSR index is built lazily and only
/// requested when a CSR start or a mid-traversal switch needs it.
///
/// `emit_cap`, when set by the pipeline (an unordered trailing `limit` on a
/// final filterless Expand), bounds the traversal's emitted pairs. Hydration
/// can drop an emitted id that no longer resolves (a dangling edge), which
/// would under-fill a capped result; the guard re-runs uncapped in that rare
/// case, so the cap is a pure optimization, never a correctness change.
#[allow(clippy::too_many_arguments)]
async fn execute_expand(
    wide: &mut RecordBatch,
    graph_index: &GraphIndexHandle<'_>,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    min_hops: u32,
    max_hops: Option<u32>,
    dst_filters: &[IRFilter],
    edge_binding: Option<&str>,
    params: &ParamMap,
    emit_cap: Option<usize>,
) -> Result<()> {
    if let Some(cap) = emit_cap {
        // RecordBatch clones are Arc'd column handles — cheap insurance for
        // the rerun path.
        let original = wide.clone();
        let stopped_early = execute_expand_dispatch(
            wide,
            graph_index,
            snapshot,
            catalog,
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            min_hops,
            max_hops,
            dst_filters,
            edge_binding,
            params,
            Some(cap),
        )
        .await?;
        // The cap is only legal when there are no `dst_filters`, so an
        // under-fill here can only mean a hydrated dst id had no row — which
        // loader and mutation referential integrity make unreachable. The
        // rerun is deliberate defense-in-depth for out-of-band writes or
        // historical stores, and is intentionally untested: no fixture can
        // produce a dangling edge through the supported write paths.
        if stopped_early && wide.num_rows() < cap {
            tracing::debug!(
                target: "omnigraph::traverse",
                edge = %edge_type,
                rows = wide.num_rows(),
                cap,
                "capped expand under-filled after hydration; re-running uncapped",
            );
            *wide = original;
            execute_expand_dispatch(
                wide,
                graph_index,
                snapshot,
                catalog,
                src_var,
                dst_var,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                dst_filters,
                edge_binding,
                params,
                None,
            )
            .await?;
        }
        return Ok(());
    }
    execute_expand_dispatch(
        wide,
        graph_index,
        snapshot,
        catalog,
        src_var,
        dst_var,
        edge_type,
        direction,
        dst_type,
        min_hops,
        max_hops,
        dst_filters,
        edge_binding,
        params,
        None,
    )
    .await
    .map(|_| ())
}

/// The mode-dispatch half of Expand execution: pick the starting strategy,
/// then hand off to the shared BFS core. Returns whether the core stopped
/// early on `emit_cap`.
#[allow(clippy::too_many_arguments)]
async fn execute_expand_dispatch(
    wide: &mut RecordBatch,
    graph_index: &GraphIndexHandle<'_>,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    min_hops: u32,
    max_hops: Option<u32>,
    dst_filters: &[IRFilter],
    edge_binding: Option<&str>,
    params: &ParamMap,
    emit_cap: Option<usize>,
) -> Result<bool> {
    let frontier_rows = wide.num_rows();
    let effective_max_hops = max_hops.unwrap_or(min_hops.max(1));
    let (key_col, _) = endpoint_columns(direction);
    let edge_table_key = format!("edge:{}", edge_type);

    // A bound edge needs edge ROWS (per-row cardinality, property columns);
    // the CSR index holds topology only, so this path always scans the edge
    // dataset. Single-hop by typecheck (T23), so the multi-hop cost model
    // does not apply.
    if let Some(binding) = edge_binding {
        let edge_ds = snapshot.open_lance_dataset(&edge_table_key).await?;
        execute_expand_bound(
            wide,
            snapshot,
            catalog,
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            dst_filters,
            binding,
            params,
            edge_ds,
        )
        .await?;
        return Ok(false);
    }

    // Cardinality-first preliminary decision (no IO). The override wins; else the
    // cost model decides under *optimistic* coverage. Optimistic is what lets us
    // skip the dataset open on a clearly-CSR traversal: real coverage can only
    // make the indexed path costlier, so if even a perfectly-indexed scan loses
    // to CSR here, it loses for real.
    let forced = traversal_indexed_override();
    let lean_indexed = match forced {
        Some(v) => v,
        None => match gather_cost_inputs(
            snapshot,
            catalog,
            edge_type,
            direction,
            frontier_rows,
            effective_max_hops,
            crate::table_store::IndexCoverage::Indexed,
            graph_index.is_built(),
        ) {
            Some(inputs) => choose_expand_mode(&inputs) == ExpandMode::IndexedScan,
            // Manifest counts absent (e.g. not-yet-materialized table): fall back
            // to the legacy frontier/hop ceiling so the decision is defined.
            None => {
                frontier_rows <= expand_indexed_max_frontier()
                    && effective_max_hops <= expand_indexed_max_hops()
            }
        },
    };

    if !lean_indexed {
        tracing::debug!(
            target: "omnigraph::traverse",
            edge = %edge_type,
            frontier = frontier_rows,
            hops = effective_max_hops,
            mode = "csr",
            "expand mode chosen",
        );
        crate::instrumentation::record_expand_path(false);
        return execute_expand_bfs(
            wide,
            graph_index,
            snapshot,
            catalog,
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            min_hops,
            max_hops,
            dst_filters,
            params,
            None,
            HopPolicy::Off,
            emit_cap,
        )
        .await;
    }

    // Leaning indexed: open the edge dataset once, confirm real coverage, and
    // (unless forced) re-decide with it. The opened dataset is threaded into the
    // indexed path so it is never opened twice.
    let edge_ds = snapshot.open_lance_dataset(&edge_table_key).await?;
    // An undirected traversal scans BOTH endpoint columns; price it by the
    // worst coverage of the columns it will actually probe (a degraded dst
    // index must not be masked by a healthy src index).
    let mut coverage =
        crate::table_store::TableStore::key_column_index_coverage(&edge_ds, key_col).await;
    for &(extra_key, _) in endpoint_probes(direction).iter().skip(1) {
        let extra =
            crate::table_store::TableStore::key_column_index_coverage(&edge_ds, extra_key).await;
        coverage = match (coverage, extra) {
            (Ok(a), Ok(b)) => Ok(worse_coverage(a, b)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
    }

    if forced.is_none() {
        if let Some(inputs) = gather_cost_inputs(
            snapshot,
            catalog,
            edge_type,
            direction,
            frontier_rows,
            effective_max_hops,
            coverage_for_decision(&coverage),
            graph_index.is_built(),
        ) {
            if choose_expand_mode(&inputs) == ExpandMode::Csr {
                tracing::debug!(
                    target: "omnigraph::traverse",
                    edge = %edge_type,
                    frontier = frontier_rows,
                    hops = effective_max_hops,
                    mode = "csr",
                    reason = "index coverage degraded",
                    "expand mode chosen",
                );
                crate::instrumentation::record_expand_path(false);
                return execute_expand_bfs(
                    wide,
                    graph_index,
                    snapshot,
                    catalog,
                    src_var,
                    dst_var,
                    edge_type,
                    direction,
                    dst_type,
                    min_hops,
                    max_hops,
                    dst_filters,
                    params,
                    None,
                    HopPolicy::Off,
                    emit_cap,
                )
                .await;
            }
        }
    }

    tracing::debug!(
        target: "omnigraph::traverse",
        edge = %edge_type,
        frontier = frontier_rows,
        hops = effective_max_hops,
        mode = "indexed",
        "expand mode chosen",
    );
    crate::instrumentation::record_expand_path(true);
    // Surface the C6 silent scalar-index fallback once, now that coverage is known.
    warn_on_degraded_coverage(&coverage, key_col, edge_type);
    // Per-hop re-decision policy (issue #533): a forced mode is a contract and
    // never switches; the cost model re-decides per hop with observed frontier
    // cardinality; absent manifest counts, the hard frontier ceiling still
    // becomes an execution bound instead of a dispatch-only gate.
    let hop_policy = if forced.is_some() {
        HopPolicy::Off
    } else {
        match gather_cost_inputs(
            snapshot,
            catalog,
            edge_type,
            direction,
            frontier_rows,
            effective_max_hops,
            coverage_for_decision(&coverage),
            graph_index.is_built(),
        ) {
            Some(inputs) => HopPolicy::Full(inputs),
            None => HopPolicy::CapOnly,
        }
    };
    execute_expand_bfs(
        wide,
        graph_index,
        snapshot,
        catalog,
        src_var,
        dst_var,
        edge_type,
        direction,
        dst_type,
        min_hops,
        max_hops,
        dst_filters,
        params,
        Some(edge_ds),
        hop_policy,
        emit_cap,
    )
    .await
}

/// Single-hop expand with a bound edge variable (`$p $w:knows $f`). Differs
/// from the unbound paths in two contracted ways: output cardinality is one
/// row per matching edge ROW (parallel edges between the same endpoints stay
/// distinct, because each carries its own properties), and the edge's declared
/// property columns ride into the wide batch under the binding's prefix
/// (`w.since`), where the ordinary filter/projection machinery consumes them.
/// The physical edge `id` rides along as a hidden `w.id` column so ordering can
/// totally order parallel edge rows; typecheck keeps it out of user expressions.
/// Typecheck (T23) guarantees single-hop.
#[allow(clippy::too_many_arguments)]
async fn execute_expand_bound(
    wide: &mut RecordBatch,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    dst_filters: &[IRFilter],
    edge_binding: &str,
    params: &ParamMap,
    edge_ds: Dataset,
) -> Result<()> {
    let src_id_col_name = format!("{}.id", src_var);
    let src_ids = wide
        .column_by_name(&src_id_col_name)
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", src_id_col_name)))?
        .clone();

    let edge_def = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_type)))?;
    // Sorted for determinism. Blobs excluded: Lance rejects blob projection
    // in a filtered scan (node scans carry the same guard); typecheck rejects
    // the access. Physical `id` is always projected as hidden row identity,
    // including for property-less edges.
    let mut prop_cols: Vec<&str> = edge_def
        .properties
        .keys()
        .map(String::as_str)
        .filter(|c| !edge_def.blob_properties.contains(*c))
        .collect();
    prop_cols.sort_unstable();
    let mut attach_cols: Vec<&str> = Vec::with_capacity(1 + prop_cols.len());
    attach_cols.push("id");
    attach_cols.extend(prop_cols.iter().copied());
    let attach_fields: Vec<Field> = attach_cols
        .iter()
        .map(|name| {
            edge_def
                .arrow_schema
                .field_with_name(name)
                .cloned()
                .map_err(|e| OmniError::manifest(e.to_string()))
        })
        .collect::<Result<_>>()?;
    let attach_schema = Arc::new(Schema::new(attach_fields));

    // Wide rows grouped by src id: several wide rows may share one source node.
    let mut rows_by_src: HashMap<&str, Vec<u32>> = HashMap::new();
    for i in 0..src_ids.len() {
        rows_by_src
            .entry(src_ids.value(i))
            .or_default()
            .push(i as u32);
    }
    let union_keys: Vec<String> = rows_by_src.keys().map(|k| k.to_string()).collect();

    // Each match carries the incoming wide-row ordinal plus physical edge id.
    // Sorting by those keys preserves an upstream ANN/BM25 rank while giving
    // parallel edges a deterministic order independent of Lance scan layout.
    let mut matches: Vec<(u32, String, usize, usize, String)> = Vec::new();
    let mut scanned: Vec<RecordBatch> = Vec::new();

    for (probe_idx, &(key_col, opp_col)) in endpoint_probes(direction).iter().enumerate() {
        let batches = crate::table_store::TableStore::scan_edges_by_endpoint_projected(
            &edge_ds,
            key_col,
            opp_col,
            &attach_cols,
            &union_keys,
        )
        .await?;
        for batch in batches {
            let batch_idx = scanned.len();
            let keys = batch
                .column_by_name(key_col)
                .ok_or_else(|| OmniError::manifest(format!("edge batch missing '{}'", key_col)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest(format!("edge '{}' is not Utf8", key_col)))?
                .clone();
            let opps = batch
                .column_by_name(opp_col)
                .ok_or_else(|| OmniError::manifest(format!("edge batch missing '{}'", opp_col)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest(format!("edge '{}' is not Utf8", opp_col)))?
                .clone();
            let edge_ids = batch
                .column_by_name("id")
                .ok_or_else(|| OmniError::manifest("edge batch missing 'id'".to_string()))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest("edge 'id' is not Utf8".to_string()))?
                .clone();
            for r in 0..batch.num_rows() {
                // Undirected probes both orientations; a self-loop row would
                // match the same wide row through both, so emit it only once.
                if probe_idx == 1 && keys.value(r) == opps.value(r) {
                    continue;
                }
                let Some(wide_rows) = rows_by_src.get(keys.value(r)) else {
                    continue;
                };
                for &wide_row in wide_rows {
                    matches.push((
                        wide_row,
                        opps.value(r).to_string(),
                        batch_idx,
                        r,
                        edge_ids.value(r).to_string(),
                    ));
                }
            }
            scanned.push(batch);
        }
    }

    matches.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.4.cmp(&b.4))
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.3.cmp(&b.3))
    });
    let mut src_indices: Vec<u32> = Vec::with_capacity(matches.len());
    let mut dst_ids: Vec<String> = Vec::with_capacity(matches.len());
    // Edge row of each emitted pair, as (scanned batch, row); flattened after
    // the scan into one pair-parallel batch.
    let mut edge_rows: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
    for (src_row, dst_id, batch_idx, edge_row, _) in matches {
        src_indices.push(src_row);
        dst_ids.push(dst_id);
        edge_rows.push((batch_idx, edge_row));
    }

    // Pair-parallel batch of physical id + declared non-blob properties. Even
    // when there are zero matches, attach a typed zero-row batch: later filter,
    // projection, and ordering must still see the bound edge's schema.
    let edge_attach = if scanned.is_empty() {
        RecordBatch::new_empty(attach_schema)
    } else {
        let attach_only: Vec<RecordBatch> = scanned
            .iter()
            .map(|b| {
                let indices: Vec<usize> = attach_cols
                    .iter()
                    .map(|c| b.schema().index_of(c))
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|e| OmniError::manifest(e.to_string()))?;
                b.project(&indices)
                    .map_err(|e| OmniError::manifest(e.to_string()))
            })
            .collect::<Result<_>>()?;
        let schema = attach_only[0].schema();
        let combined = arrow_select::concat::concat_batches(&schema, &attach_only)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        // Flatten (batch, row) to rows in `combined`.
        let mut offsets: Vec<usize> = Vec::with_capacity(scanned.len());
        let mut acc = 0usize;
        for b in &scanned {
            offsets.push(acc);
            acc += b.num_rows();
        }
        let flat: Vec<u32> = edge_rows
            .iter()
            .map(|&(b, r)| (offsets[b] + r) as u32)
            .collect();
        take_batch(&combined, &UInt32Array::from(flat))?
    };

    expand_hydrate_and_align(
        wide,
        src_indices,
        dst_ids,
        snapshot,
        catalog,
        dst_type,
        dst_var,
        dst_filters,
        params,
        Some((edge_binding.to_string(), edge_attach)),
    )
    .await
}

/// Where the shared BFS core reads each hop's neighbors from. The two sources
/// are the same two execution strategies the dispatcher chooses between; the
/// core can swap Indexed → Csr BETWEEN hops (issue #533), carrying its BFS
/// state across the swap instead of restarting.
///
/// Id spaces differ per source: Indexed owns a per-traversal interner (both
/// endpoint types in ONE dense space — see the cross-type single-hop guard in
/// `execute_expand_bfs`), Csr borrows the graph index's per-type dictionaries.
/// A swap therefore translates all live state through the id strings once.
enum ActiveExpandSource<'g> {
    // Boxed: the indexed state (dataset handle + interner + per-hop map) is
    // hundreds of bytes against Csr's four refs, and exactly one instance
    // lives per expand.
    Indexed(Box<IndexedExpandSource>),
    Csr {
        adj: &'g crate::graph_index::CsrIndex,
        adj_rev: Option<&'g crate::graph_index::CsrIndex>,
        src_idx: &'g crate::graph_index::TypeIndex,
        dst_idx: &'g crate::graph_index::TypeIndex,
    },
}

struct IndexedExpandSource {
    edge_ds: Dataset,
    interner: crate::graph_index::TypeIndex,
    /// This hop's dense key -> dense neighbors (scan order; duplicates
    /// preserved, like CSR multi-edges). Rebuilt per hop.
    neighbor_map: HashMap<u32, Vec<u32>>,
}

/// Per-hop re-decision policy for a traversal that started on the indexed
/// path. `Off` for forced modes and CSR starts; `CapOnly` when manifest counts
/// were unavailable at dispatch (the legacy-ceiling fallback still deserves an
/// execution bound); `Full` re-runs the cost comparison with observed growth.
enum HopPolicy {
    Off,
    CapOnly,
    Full(ExpandCostInputs),
}

/// Resolve the CSR-side borrows for one edge type + direction from a built
/// graph index (shared by the CSR start and the mid-traversal switch).
fn resolve_csr_source<'g>(
    gi: &'g GraphIndex,
    edge_def: &omnigraph_compiler::catalog::EdgeType,
    edge_type: &str,
    direction: Direction,
) -> Result<ActiveExpandSource<'g>> {
    let (src_type_name, dst_type_name) = match direction {
        Direction::Out => (&edge_def.from_type, &edge_def.to_type),
        Direction::In => (&edge_def.to_type, &edge_def.from_type),
        // Both requires from_type == to_type (typecheck T22).
        Direction::Both => (&edge_def.from_type, &edge_def.from_type),
    };
    let src_idx = gi
        .type_index(src_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", src_type_name)))?;
    let dst_idx = gi
        .type_index(dst_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", dst_type_name)))?;
    let adj = match direction {
        Direction::Out | Direction::Both => gi.csr(edge_type),
        Direction::In => gi.csc(edge_type),
    }
    .ok_or_else(|| OmniError::manifest(format!("no adjacency index for edge '{}'", edge_type)))?;
    // Undirected: additionally walk incoming edges (CSC); the BFS gates below
    // dedup pairs that exist in both directions and self-loops.
    let adj_rev = match direction {
        Direction::Both => Some(gi.csc(edge_type).ok_or_else(|| {
            OmniError::manifest(format!("no adjacency index for edge '{}'", edge_type))
        })?),
        _ => None,
    };
    Ok(ActiveExpandSource::Csr {
        adj,
        adj_rev,
        src_idx,
        dst_idx,
    })
}

/// The one Expand BFS, shared by both execution strategies. Per hop it asks
/// the active source for neighbors — a batched `scan_edges_by_endpoint` per
/// orientation against the persisted src/dst BTREE (Indexed: cost scales with
/// the frontier, not |E|), or in-memory adjacency slices (Csr). Emission,
/// dedup, hop gating, and the hydrate+align tail are identical either way, so
/// both strategies produce the same `(src_row, dst_id)` pairs by construction.
///
/// Multi-hop only advances for same-type edges; a cross-type traversal is
/// structurally single-hop. The Indexed source enforces that BEFORE scanning:
/// it interns every endpoint string into ONE dense id space, so a cross-type
/// id-string collision (a Person and a Company sharing an id) would otherwise
/// let hop 2 de-intern a destination id back to the colliding source-type id
/// and match its edges, emitting rows the CSR source never produces.
///
/// Issue #533 lives here: `hop_policy` is consulted at the top of every
/// indexed hop after the first with the OBSERVED union frontier, and a
/// traversal that has outgrown the indexed path swaps to CSR mid-flight,
/// translating frontier/visited/seen state through the id strings once. Every
/// emitted destination so far is an edge endpoint, so it exists in the CSR
/// dictionaries — nothing is lost in translation; a frontier or visited entry
/// absent from the CSR dictionary has no edges at all and is dropped as
/// unreachable.
///
/// `emit_cap` bounds the number of emitted `(src_row, dst)` pairs; the loop
/// stops as soon as the cap is reached (limit pushdown — the caller only
/// engages it when result rows and emitted pairs are 1:1). Returns whether
/// the traversal stopped early on that cap.
#[allow(clippy::too_many_arguments)]
async fn execute_expand_bfs(
    wide: &mut RecordBatch,
    graph_index: &GraphIndexHandle<'_>,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    min_hops: u32,
    max_hops: Option<u32>,
    dst_filters: &[IRFilter],
    params: &ParamMap,
    start_indexed: Option<Dataset>,
    hop_policy: HopPolicy,
    emit_cap: Option<usize>,
) -> Result<bool> {
    let src_id_col_name = format!("{}.id", src_var);
    let src_ids = wide
        .column_by_name(&src_id_col_name)
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", src_id_col_name)))?
        .clone();

    let edge_def = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_type)))?;
    let same_type = edge_def.from_type == edge_def.to_type;
    let probes = endpoint_probes(direction);

    let max = max_hops.unwrap_or(min_hops.max(1));
    // Cross-type edges cannot chain (a Company is not a `WorksAt` source): see
    // the doc comment for why the Indexed source must enforce this before the
    // hop-2 scan rather than relying on it returning empty.
    let max = if same_type { max } else { max.min(1) };

    // The active neighbor source. Indexed starts own a per-traversal interner
    // (local id ↔ u32 dictionary — the GraphIndex/CSR is NOT built); CSR
    // starts borrow the built graph index's dictionaries.
    let mut active = match start_indexed {
        Some(edge_ds) => ActiveExpandSource::Indexed(Box::new(IndexedExpandSource {
            edge_ds,
            interner: crate::graph_index::TypeIndex::new(),
            neighbor_map: HashMap::new(),
        })),
        None => {
            let gi = graph_index.get().await?.ok_or_else(|| {
                OmniError::manifest("graph index required for CSR traversal".to_string())
            })?;
            resolve_csr_source(gi, edge_def, edge_type, direction)?
        }
    };

    // Per-source BFS state, dense in the ACTIVE source's id space so both
    // pure paths run exactly as they did before unification; only a
    // mid-traversal swap pays a one-time translation.
    let n = src_ids.len();
    let mut frontiers: Vec<Vec<u32>> = Vec::with_capacity(n);
    let mut visited: Vec<HashSet<u32>> = Vec::with_capacity(n);
    let mut seen_dst: Vec<HashSet<u32>> = Vec::with_capacity(n);
    for i in 0..n {
        let seed = match &mut active {
            ActiveExpandSource::Indexed(src) => Some(src.interner.get_or_insert(src_ids.value(i))),
            // A seed the CSR dictionary has never seen touches no edge; its
            // BFS is empty (mirrors the old CSR path's `continue`).
            ActiveExpandSource::Csr { src_idx, .. } => src_idx.to_dense(src_ids.value(i)),
        };
        let mut v = HashSet::new();
        // Only track visited in the destination namespace for same-type edges
        // (to avoid revisiting the source). For cross-type edges, dense indices
        // are in different namespaces so collision is impossible.
        if same_type {
            if let Some(s) = seed {
                v.insert(s);
            }
        }
        frontiers.push(seed.map(|s| vec![s]).unwrap_or_default());
        visited.push(v);
        seen_dst.push(HashSet::new());
    }

    // Emissions carry the destination as a STRING so they survive an id-space
    // swap unchanged. Allocation-neutral: both old paths stringified every
    // emitted pair for the hydrate tail anyway, just later.
    let mut emitted_src: Vec<u32> = Vec::new();
    let mut emitted_dst: Vec<String> = Vec::new();
    let cap = emit_cap.unwrap_or(usize::MAX);
    let mut stopped_early = false;
    let mut prev_union_len: usize = 0;

    'hops: for hop in 1..=max {
        // Union of all live frontiers (dense). Needed for the Indexed scan's
        // IN-list; also the observed cardinality the hop policy re-decides on.
        let mut union_dense: Vec<u32> = Vec::new();
        {
            let mut seen: HashSet<u32> = HashSet::new();
            for f in &frontiers {
                for &node in f {
                    if seen.insert(node) {
                        union_dense.push(node);
                    }
                }
            }
        }
        if union_dense.is_empty() {
            break;
        }

        // Issue #533: re-decide the mode with the OBSERVED frontier before
        // paying this hop's indexed scan. Hop 1's frontier is what dispatch
        // already decided on; later hops are what dispatch could never see.
        if hop > 1 && matches!(active, ActiveExpandSource::Indexed(_)) {
            let switch = match &hop_policy {
                HopPolicy::Off => false,
                HopPolicy::CapOnly => union_dense.len() > expand_indexed_max_frontier(),
                HopPolicy::Full(inputs) => should_switch_to_csr(
                    union_dense.len(),
                    prev_union_len,
                    max - hop + 1,
                    graph_index.is_built(),
                    inputs,
                ),
            };
            if switch {
                crate::instrumentation::record_traversal_mid_switch();
                crate::instrumentation::record_expand_path(false);
                let gi = graph_index.get().await?.ok_or_else(|| {
                    OmniError::manifest("graph index required for CSR traversal".to_string())
                })?;
                let csr_source = resolve_csr_source(gi, edge_def, edge_type, direction)?;
                let old = std::mem::replace(&mut active, csr_source);
                let ActiveExpandSource::Indexed(old_src) = old else {
                    unreachable!("switch only fires while the Indexed source is active");
                };
                let interner = old_src.interner;
                let ActiveExpandSource::Csr {
                    src_idx, dst_idx, ..
                } = &active
                else {
                    unreachable!("active source was just replaced with Csr");
                };
                // Translate all live state through the id strings once. An
                // entry the CSR dictionary lacks has no edges: droppable from
                // a frontier (expands to nothing) and from visited/seen (it
                // cannot be reached again through adjacency).
                let translate_set =
                    |set: &HashSet<u32>, idx: &crate::graph_index::TypeIndex| -> HashSet<u32> {
                        set.iter()
                            .filter_map(|&d| interner.to_id(d).and_then(|id| idx.to_dense(id)))
                            .collect()
                    };
                for i in 0..n {
                    frontiers[i] = frontiers[i]
                        .iter()
                        .filter_map(|&d| interner.to_id(d).and_then(|id| src_idx.to_dense(id)))
                        .collect();
                    visited[i] = translate_set(&visited[i], src_idx);
                    seen_dst[i] = translate_set(&seen_dst[i], dst_idx);
                }
                tracing::debug!(
                    target: "omnigraph::traverse",
                    edge = %edge_type,
                    hop,
                    frontier = union_dense.len(),
                    mode = "csr",
                    reason = "frontier outgrew the indexed path",
                    "expand mode switched mid-traversal",
                );
            }
        }
        prev_union_len = union_dense.len();

        // Indexed source: one batched BTREE scan per orientation for this
        // hop's union frontier (Both merges both orientations into one map;
        // the per-source `seen_dst` gate dedups pairs present both ways).
        if let ActiveExpandSource::Indexed(src) = &mut active {
            src.neighbor_map.clear();
            let union_keys: Vec<String> = union_dense
                .iter()
                .map(|&u| {
                    src.interner
                        .to_id(u)
                        .expect("interned frontier id must resolve")
                        .to_string()
                })
                .collect();
            for &(key_col, opp_col) in probes {
                let batches = crate::table_store::TableStore::scan_edges_by_endpoint(
                    &src.edge_ds,
                    key_col,
                    opp_col,
                    &union_keys,
                )
                .await?;
                for batch in &batches {
                    let keys = batch
                        .column_by_name(key_col)
                        .ok_or_else(|| {
                            OmniError::manifest(format!("edge batch missing '{}'", key_col))
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            OmniError::manifest(format!("edge '{}' is not Utf8", key_col))
                        })?;
                    let opps = batch
                        .column_by_name(opp_col)
                        .ok_or_else(|| {
                            OmniError::manifest(format!("edge batch missing '{}'", opp_col))
                        })?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            OmniError::manifest(format!("edge '{}' is not Utf8", opp_col))
                        })?;
                    for r in 0..batch.num_rows() {
                        let k = src.interner.get_or_insert(keys.value(r));
                        let o = src.interner.get_or_insert(opps.value(r));
                        src.neighbor_map.entry(k).or_default().push(o);
                    }
                }
            }
        }

        // Advance each source row's frontier independently (dense ids). The
        // emission contract is identical across sources: a self-edge is a
        // valid destination that reaches nothing new — emit it without
        // entering the frontier, so the seeded-source `visited` pre-mark
        // prunes only multi-hop cycle returns (same-type only: cross-type
        // dense ids live in different namespaces, where node == neighbor is
        // meaningless for Csr and collision-prone for Indexed).
        for i in 0..n {
            let cur = std::mem::take(&mut frontiers[i]);
            let mut next: Vec<u32> = Vec::new();
            for &node in &cur {
                let (fwd, rev): (&[u32], &[u32]) = match &active {
                    ActiveExpandSource::Indexed(src) => (
                        src.neighbor_map
                            .get(&node)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        &[],
                    ),
                    ActiveExpandSource::Csr { adj, adj_rev, .. } => (
                        adj.neighbors(node),
                        adj_rev.map(|a| a.neighbors(node)).unwrap_or(&[]),
                    ),
                };
                for &neighbor in fwd.iter().chain(rev) {
                    let is_self = same_type && neighbor == node;
                    if !is_self && same_type && !visited[i].insert(neighbor) {
                        continue;
                    }
                    if !is_self {
                        next.push(neighbor);
                    }
                    if hop >= min_hops && seen_dst[i].insert(neighbor) {
                        let dst_id = match &active {
                            ActiveExpandSource::Indexed(src) => Some(
                                src.interner
                                    .to_id(neighbor)
                                    .expect("interned dst id must resolve")
                                    .to_string(),
                            ),
                            // Dense ids from adjacency always resolve; drop
                            // defensively rather than panic (mirrors the old
                            // CSR tail).
                            ActiveExpandSource::Csr { dst_idx, .. } => {
                                dst_idx.to_id(neighbor).map(str::to_string)
                            }
                        };
                        if let Some(dst_id) = dst_id {
                            emitted_src.push(i as u32);
                            emitted_dst.push(dst_id);
                            if emitted_src.len() >= cap {
                                stopped_early = true;
                                crate::instrumentation::record_expand_cap_stop();
                                frontiers[i] = next;
                                break 'hops;
                            }
                        }
                    }
                }
            }
            frontiers[i] = next;
        }
    }

    expand_hydrate_and_align(
        wide,
        emitted_src,
        emitted_dst,
        snapshot,
        catalog,
        dst_type,
        dst_var,
        dst_filters,
        params,
        None,
    )
    .await?;
    Ok(stopped_early)
}

/// Shared tail for all Expand modes: hydrate the unique destination ids, align
/// the `(src_row, dst_id)` pairs back onto `wide`, hconcat, and apply
/// non-pushable destination filters in memory. `edge_attach`, present only for
/// a bound-edge expand, is a pair-parallel batch of edge property columns that
/// joins the wide batch under the binding's prefix.
#[allow(clippy::too_many_arguments)]
async fn expand_hydrate_and_align(
    wide: &mut RecordBatch,
    src_indices: Vec<u32>,
    dst_ids: Vec<String>,
    snapshot: &Snapshot,
    catalog: &Catalog,
    dst_type: &str,
    dst_var: &str,
    dst_filters: &[IRFilter],
    params: &ParamMap,
    edge_attach: Option<(String, RecordBatch)>,
) -> Result<()> {
    // Pushable destination filters are applied by `hydrate_nodes`; the rest
    // (`ir_filter_to_expr` → None) are applied in memory after hconcat. The
    // schema arg only affects a pushable literal's TYPE, never Some-vs-None, so
    // `None` here yields the same pushable/non-pushable split as `hydrate_nodes`.
    let non_pushable: Vec<&IRFilter> = dst_filters
        .iter()
        .filter(|f| ir_filter_to_expr(f, params, None).is_none())
        .collect();

    // Unique destination ids (first-seen order) for one batched hydration.
    let mut unique_dst_list: Vec<String> = Vec::new();
    {
        let mut seen: HashSet<&str> = HashSet::with_capacity(dst_ids.len());
        for id in &dst_ids {
            if seen.insert(id.as_str()) {
                unique_dst_list.push(id.clone());
            }
        }
    }
    let dst_batch = hydrate_nodes(
        snapshot,
        catalog,
        dst_type,
        &unique_dst_list,
        dst_filters,
        params,
    )
    .await?;

    // id -> row index in the hydrated batch.
    let dst_batch_id_col = dst_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("hydrated batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("hydrated 'id' column is not Utf8".to_string()))?;
    let mut id_to_row: HashMap<&str, u32> = HashMap::with_capacity(dst_batch_id_col.len());
    for row in 0..dst_batch_id_col.len() {
        id_to_row.insert(dst_batch_id_col.value(row), row as u32);
    }

    // Align pairs to (src_row, hydrated_dst_row), dropping ids hydration filtered out.
    let mut final_src_indices: Vec<u32> = Vec::with_capacity(src_indices.len());
    let mut dst_indices: Vec<u32> = Vec::with_capacity(src_indices.len());
    let mut surviving_pairs: Vec<u32> = Vec::with_capacity(src_indices.len());
    for (pair_idx, (&src_idx, dst_id)) in src_indices.iter().zip(dst_ids.iter()).enumerate() {
        if let Some(&dst_row) = id_to_row.get(dst_id.as_str()) {
            final_src_indices.push(src_idx);
            dst_indices.push(dst_row);
            surviving_pairs.push(pair_idx as u32);
        }
    }

    let src_take = UInt32Array::from(final_src_indices);
    let dst_take = UInt32Array::from(dst_indices);
    let expanded_wide = take_batch(wide, &src_take)?;
    let dst_prefixed = prefix_batch(&dst_batch, dst_var)?;
    let aligned_dst = take_batch(&dst_prefixed, &dst_take)?;
    *wide = hconcat_batches(&expanded_wide, &aligned_dst)?;

    if let Some((binding, edge_batch)) = edge_attach {
        let aligned_edge = take_batch(&edge_batch, &UInt32Array::from(surviving_pairs))?;
        let edge_prefixed = prefix_batch(&aligned_edge, &binding)?;
        *wide = hconcat_batches(wide, &edge_prefixed)?;
    }

    for f in &non_pushable {
        apply_filter(wide, f, params)?;
    }
    Ok(())
}

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
/// gate's id-count cap (`DEFAULT_RRF_GATE_MAX_IDS`, set where in-list
/// evaluation starts losing) ever shows up as the bottleneck: a mask built
/// from a cached id→addr mapping would lift both.
fn id_in_list_expr(ids: &[String]) -> datafusion::prelude::Expr {
    use datafusion::prelude::{col, lit};
    let id_list: Vec<datafusion::prelude::Expr> = ids.iter().map(|id| lit(id.clone())).collect();
    col("id").in_list(id_list, false)
}

/// Load full node rows for a set of IDs from a snapshot.
///
/// The `id IN (...)` predicate (`id_in_list_expr`) is AND'd with any
/// pushable `dst_filters` (destination-binding filters), then applied via
/// `Scanner::filter_expr`. Non-pushable `dst_filters` (`ir_filter_to_expr`
/// → None) are applied in memory by the caller after hydration.
async fn hydrate_nodes(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    ids: &[String],
    dst_filters: &[IRFilter],
    params: &ParamMap,
) -> Result<RecordBatch> {
    let node_type = catalog
        .node_types
        .get(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;

    if ids.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }

    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open_lance_dataset(&table_key).await?;

    // `id IN (ids)` AND any pushable destination filters, as a structured Expr.
    let mut filter_expr = id_in_list_expr(ids);
    if let Some(dst_expr) =
        build_lance_filter_expr(dst_filters, params, Some(&node_type.arrow_schema))
    {
        filter_expr = filter_expr.and(dst_expr);
    }

    let has_blobs = !node_type.blob_properties.is_empty();
    let non_blob_cols: Vec<&str> = node_type
        .arrow_schema
        .fields()
        .iter()
        .filter(|f| !node_type.blob_properties.contains(f.name()))
        .map(|f| f.name().as_str())
        .collect();
    let projection = has_blobs.then_some(non_blob_cols.as_slice());
    let batches = crate::table_store::TableStore::scan_stream_with(
        &ds,
        projection,
        None,
        None,
        false,
        |scanner| {
            scanner.filter_expr(filter_expr);
            Ok(())
        },
    )
    .await?
    .try_collect::<Vec<RecordBatch>>()
    .await
    .map_err(OmniError::storage)?;

    let scan_result = if batches.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(OmniError::arrow_internal)?
    };

    if has_blobs {
        return add_null_blob_columns(&scan_result, node_type);
    }
    Ok(scan_result)
}

/// Whether the inner pipeline is the bulk-anti-join shape: a single Expand from
/// the outer var with no destination filters (the only shape the CSR
/// `has_neighbors` fast path can serve). Pure — it does not touch the CSR — so
/// the caller can decide whether to realize the O(|E|) graph index at all.
fn bulk_anti_join_applies(inner_pipeline: &[IROp], outer_var: &str) -> bool {
    matches!(
        inner_pipeline,
        [IROp::Expand { src_var, dst_filters, min_hops, max_hops, .. }]
            if src_var == outer_var
                && dst_filters.is_empty()
                // `has_neighbors` is a ONE-hop existence test, so the fast path
                // is valid only for a single-hop expand. Multi-hop negations
                // (e.g. `not { $p knows{2,2} $x }`) fall to the slow path, whose
                // inner Expand runs the real bounded traversal.
                && *min_hops == 1
                && (*max_hops).unwrap_or(1) == 1
    )
}

/// Try bulk anti-join via CSR existence check. Returns Some(mask) if the inner
/// pipeline is a single Expand from outer_var (the common negation pattern).
fn try_bulk_anti_join_mask(
    wide: &RecordBatch,
    inner_pipeline: &[IROp],
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
) -> Option<BooleanArray> {
    if !bulk_anti_join_applies(inner_pipeline, outer_var) {
        return None;
    }
    let IROp::Expand {
        edge_type,
        direction,
        ..
    } = &inner_pipeline[0]
    else {
        return None;
    };
    let gi = graph_index?;
    let edge_def = catalog.edge_types.get(edge_type.as_str())?;

    let src_type_name = match direction {
        // Both grouped with Out: the primary adjacency below is `csr`, keyed
        // in from_type's dense namespace (equal to to_type under T22, but the
        // grouping must match `adj` so a future T22 relaxation cannot split
        // them silently).
        Direction::Out | Direction::Both => &edge_def.from_type,
        Direction::In => &edge_def.to_type,
    };
    let adj = match direction {
        Direction::Out | Direction::Both => gi.csr(edge_type),
        Direction::In => gi.csc(edge_type),
    }?;
    // Undirected anti-join: "no edge in EITHER direction".
    let adj_rev = match direction {
        Direction::Both => Some(gi.csc(edge_type)?),
        _ => None,
    };
    let type_idx = gi.type_index(src_type_name)?;

    let id_col_name = format!("{}.id", outer_var);
    let outer_ids = wide
        .column_by_name(&id_col_name)?
        .as_any()
        .downcast_ref::<StringArray>()?;

    let keep_mask: Vec<bool> = (0..outer_ids.len())
        .map(|i| {
            let id = outer_ids.value(i);
            match type_idx.to_dense(id) {
                Some(dense) => {
                    !adj.has_neighbors(dense)
                        && !adj_rev.map(|a| a.has_neighbors(dense)).unwrap_or(false)
                }
                None => true, // not in graph index = no edges = keep
            }
        })
        .collect();

    Some(BooleanArray::from(keep_mask))
}

/// Execute an AntiJoin: remove rows from wide batch where the inner pipeline finds matches.
async fn execute_anti_join(
    wide: &mut RecordBatch,
    inner_pipeline: &[IROp],
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    outer_var: &str,
    needed_columns: &HashMap<String, NeededColumns>,
) -> Result<()> {
    // Only the bulk fast path consumes the CSR; the slow path's inner Expand
    // chooses its own access path. Realize the O(|E|) graph index ONLY when the
    // inner-pipeline shape qualifies for the bulk check — a filtered/nested
    // anti-join over a large graph must not pay a whole-graph build it won't use.
    let gi = if bulk_anti_join_applies(inner_pipeline, outer_var) {
        graph_index.get().await?
    } else {
        None
    };
    // Fast path: bulk CSR existence check (O(N), zero Lance I/O)
    if let Some(mask) = try_bulk_anti_join_mask(wide, inner_pipeline, gi, catalog, outer_var) {
        *wide = arrow_select::filter::filter_record_batch(wide, &mask)
            .map_err(OmniError::arrow_internal)?;
        return Ok(());
    }

    // Slow path (filtered / non-bulk inner): run the inner pipeline ONCE over the
    // whole frontier — a set-oriented anti-semi-join — instead of row-by-row.
    // Each outer row is tagged with a synthetic index; an outer row matches iff
    // it produced at least one surviving inner row. No per-row dispatch, so the
    // inner Expand runs as a single set-at-a-time traversal over the full
    // frontier (its own chooser picks indexed vs CSR) rather than one Lance scan
    // per outer row.
    let num_rows = wide.num_rows();
    if num_rows == 0 {
        return Ok(());
    }

    // The tag rides through the inner pipeline: Expand's hconcat preserves
    // existing columns and Filter only drops rows, so each surviving row carries
    // its originating outer-row index. Correlating on the row index (not
    // `outer_var.id`) stays correct even if a dst-filter references other outer
    // bindings. Nested anti-joins reuse this slow path and an enclosing tag rides
    // through too; Arrow allows duplicate field names and `column_by_name`
    // returns the FIRST match, so choose a tag name not already present (each
    // nesting level then reads its own) instead of a fixed one.
    let tag_col: String = {
        let mut n = 0usize;
        loop {
            let candidate = format!("__antijoin_outer_row_{n}");
            if wide.schema().column_with_name(&candidate).is_none() {
                break candidate;
            }
            n += 1;
        }
    };
    let mut fields: Vec<Field> = wide
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(tag_col.as_str(), DataType::UInt32, false));
    let mut columns: Vec<ArrayRef> = wide.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from_iter_values(0..num_rows as u32)));
    let tagged = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(OmniError::arrow_internal)?;

    let mut inner_wide: Option<RecordBatch> = Some(tagged);
    let no_search = SearchMode::default();
    execute_pipeline(
        inner_pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut inner_wide,
        &no_search,
        None,
        needed_columns,
    )
    .await?;

    // Outer rows whose tag survived have >= 1 match. A produced-but-untagged
    // batch means the inner pipeline dropped the correlation column — fail loudly
    // rather than silently keeping every row (which would corrupt the anti-join).
    let mut matched: HashSet<u32> = HashSet::new();
    if let Some(batch) = inner_wide {
        if batch.num_rows() > 0 {
            let tags = batch
                .column_by_name(tag_col.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(
                        "anti-join inner pipeline dropped the correlation column".to_string(),
                    )
                })?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    OmniError::manifest(format!("'{}' column is not UInt32", tag_col))
                })?;
            for i in 0..tags.len() {
                matched.insert(tags.value(i));
            }
        }
    }

    let keep_mask: Vec<bool> = (0..num_rows as u32)
        .map(|i| !matched.contains(&i))
        .collect();
    let mask = BooleanArray::from(keep_mask);
    *wide = arrow_select::filter::filter_record_batch(wide, &mask)
        .map_err(OmniError::arrow_internal)?;
    Ok(())
}

/// Scan a node type's Lance dataset with optional filter pushdown and search modes.
async fn execute_node_scan(
    type_name: &str,
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    search_mode: &SearchMode,
    binding_columns: Option<&NeededColumns>,
) -> Result<RecordBatch> {
    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open_lance_dataset(&table_key).await?;

    let node_type = &catalog.node_types[type_name];

    // Lower the IR filters to a DataFusion `Expr` and apply via
    // `Scanner::filter_expr` inside the configure closure. The string
    // pushdown path (`build_lance_filter` → `scanner.filter(&str)`) is
    // gone for node scans — structured Expr unlocks `CompOp::Contains`
    // pushdown (via `array_has`) and lets DF 53's optimizer rules
    // (vectorized IN-list, PhysicalExprSimplifier, CASE-NULL shortcut)
    // reach our predicates. Passing the node's `arrow_schema` lets the lowering
    // coerce literals to each column's exact type so narrow-numeric BTREEs are
    // used. Other call sites that still take string SQL (count_rows, the
    // mutation delete path) migrate in follow-up MRs.
    let mut filter_expr = build_lance_filter_expr(filters, params, Some(&node_type.arrow_schema));

    // The rrf prefilter gate's selective plan: AND the traversal's
    // eligible-id set into this bm25 arm's scan — `hydrate_nodes`' proven
    // `id IN (...)` shape, routed through the `id` BTREE, ranked under
    // `prefilter(true)` (armed below by the filter's presence). The set is a
    // superset of the traversal's survivors and the arm stays uncapped, so
    // this changes cost, never the fused answer (up to BM25 score ties; the
    // gate's coverage fence keeps scores index-global).
    if let Some(eligible_ids) = search_mode.bm25_eligible_ids_for(variable) {
        let in_list = id_in_list_expr(eligible_ids);
        filter_expr = Some(match filter_expr {
            Some(expr) => expr.and(in_list),
            None => in_list,
        });
    }

    // Blob columns must be excluded from scan when a filter is present
    // (Lance bug: BlobsDescriptions + filter triggers a projection assertion).
    // We exclude blob columns and add metadata post-scan via take_blobs_by_indices.
    let has_blobs = !node_type.blob_properties.is_empty();
    let non_blob_cols: Vec<&str> = node_type
        .arrow_schema
        .fields()
        .iter()
        .filter(|f| !node_type.blob_properties.contains(f.name()))
        .map(|f| f.name().as_str())
        .collect();
    // #564: RETURN-derived projection. `Some(Columns)` prunes the scan to
    // the demanded set plus the always-keep set: `id` (Expand/AntiJoin join
    // key, RRF fusion key, `apply_ordering` tie-break) and the type's key
    // columns (the row's declared identity, retained as cheap insurance).
    // `None` (unreferenced binding) and `All` (bare `$var`) fail open to the
    // full non-blob projection; so do search scans, because Lance
    // autoprojects `_distance`/`_score` onto explicit projections that omit
    // them, with a deprecation warning. Only NodeScan bindings prune:
    // `hydrate_nodes` (Expand dst hydration) and edge-property attach keep
    // the full non-blob width.
    let is_search_scan = search_mode
        .nearest
        .as_ref()
        .is_some_and(|(var, ..)| var == variable)
        || search_mode
            .bm25
            .as_ref()
            .is_some_and(|(var, ..)| var == variable)
        || filters.iter().any(is_search_filter);
    let pruned_cols: Option<Vec<&str>> = match binding_columns {
        Some(NeededColumns::Columns(columns)) if !is_search_scan => Some(
            non_blob_cols
                .iter()
                .copied()
                .filter(|name| {
                    *name == "id"
                        || node_type
                            .key
                            .as_ref()
                            .is_some_and(|key| key.iter().any(|k| k == name))
                        || columns.contains(*name)
                })
                .collect(),
        ),
        _ => None,
    };
    let projection = match &pruned_cols {
        Some(columns) => Some(columns.as_slice()),
        None => has_blobs.then_some(non_blob_cols.as_slice()),
    };
    let batches = crate::table_store::TableStore::scan_stream_with(
        &ds,
        projection,
        None,
        None,
        false,
        |scanner| {
            // Apply the structured IR filter via Lance's Expr pushdown.
            if let Some(ref expr) = filter_expr {
                scanner.filter_expr(expr.clone());
                // The filter must run BEFORE any ANN/FTS search on this
                // scanner. Lance defaults to prefilter=false, which applies
                // the filter to the search's top-k results — "you may get
                // back fewer results than you ask for (or none at all)"
                // (lance scanner.rs) — i.e. `limit k` would mean top-k of
                // the whole table, silently starved by a selective filter.
                // One flag governs both the vector and FTS sources, and it
                // is unused by plain scans, so setting it whenever a filter
                // is present is safe. Prefiltering also re-enables scalar-
                // index acceleration for the predicate (Lance gates
                // use_scalar_index on prefilter when a nearest is present).
                scanner.prefilter(true);
            }

            // Apply FTS queries from hoisted search filters (search/fuzzy/match_text in match clause)
            for filter in filters {
                if is_search_filter(filter) {
                    if let Some(fts_query) = build_fts_query(&filter.left, params) {
                        scanner.full_text_search(fts_query).map_err(|error| {
                            OmniError::storage_context("full_text_search filter", error)
                        })?;
                    }
                }
            }

            // Apply nearest vector search if this variable is the target
            if let Some((ref var, ref prop, ref vec, k)) = search_mode.nearest {
                if var == variable {
                    let query_arr = Float32Array::from(vec.clone());
                    scanner
                        .nearest(prop, &query_arr, k)
                        .map_err(|error| OmniError::storage_context("nearest", error))?;
                    // Lance 11's late payload `LanceRead` drops the sorted
                    // candidate stream's ordering metadata. With more than
                    // one output partition, execute_plan may therefore use a
                    // scheduling-ordered coalescer and scramble large-k ANN
                    // or flat results. Keep one DataFusion output partition
                    // until Lance preserves/remaps that ordering. Reads and
                    // decoding inside the partition remain concurrent.
                    scanner.target_parallelism(1);
                }
            }

            // Apply BM25 full-text search if this variable is the target
            if let Some((ref var, ref prop, ref text)) = search_mode.bm25 {
                if var == variable {
                    let mut fts_query = lance_index::scalar::FullTextSearchQuery::new(text.clone())
                        .with_column(prop.clone())
                        .map_err(|error| OmniError::storage_context("fts with_column", error))?;
                    // Cap the ranked FTS scan (issue #563): unbounded, Lance
                    // hydrates every matching document, and a `limit`ed ranked
                    // read materializes the whole matched corpus past Arrow's
                    // 2 GiB i32 string-offset ceiling. Lance returns rows
                    // score-descending (the IR's declared order direction is
                    // ignored engine-wide), so up to score ties the capped
                    // rows are the uncapped scan's prefix. This runs after the
                    // search()-filter loop above and Lance's full_text_search
                    // REPLACES the scanner's query, so the capped one wins; a
                    // search() filter without bm25 ordering stays unbounded.
                    if let Some(rows) = search_mode.bm25_scan_limit {
                        // A negative limit would mean unlimited to Lance
                        // (it casts `as usize`); saturate instead.
                        fts_query = fts_query.limit(Some(i64::try_from(rows).unwrap_or(i64::MAX)));
                    }
                    scanner
                        .full_text_search(fts_query)
                        .map_err(|error| OmniError::storage_context("full_text_search", error))?;
                    // No target_parallelism(1) pin needed here, unlike the
                    // nearest arm above: the FTS plan sorts globally with a
                    // fetch and emits a single partition.
                }
            }
            Ok(())
        },
    )
    .await?
    .try_collect::<Vec<RecordBatch>>()
    .await
    .map_err(OmniError::storage)?;

    if search_mode
        .bm25
        .as_ref()
        .is_some_and(|(var, ..)| var == variable)
    {
        crate::instrumentation::record_bm25_scan_rows(
            batches.iter().map(|b| b.num_rows() as u64).sum(),
        );
    }

    let scan_result = if batches.is_empty() {
        // Build the schema the scan would have produced (the pruned
        // projection when one applied, all non-blob columns otherwise) so an
        // empty result is shaped like a non-empty one — except search scans,
        // whose non-empty batches also carry Lance's autoprojected
        // `_distance`/`_score`; those are absent here.
        let fields: Vec<_> = node_type
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| match &pruned_cols {
                Some(columns) => columns.contains(&f.name().as_str()),
                None => !node_type.blob_properties.contains(f.name()),
            })
            .map(|f| f.as_ref().clone())
            .collect();
        RecordBatch::new_empty(Arc::new(Schema::new(fields)))
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(OmniError::arrow_internal)?
    };
    // Add null placeholder columns for excluded blob properties
    if has_blobs {
        return add_null_blob_columns(&scan_result, node_type);
    }
    Ok(scan_result)
}

/// Add null Utf8 columns for blob properties excluded from a scan.
/// Uses column_by_name (not positional) so it's order-independent, and
/// silently skips non-blob fields absent from the batch — LOAD-BEARING for
/// pruned scans (#564), which legitimately omit undemanded non-blob columns.
fn add_null_blob_columns(
    batch: &RecordBatch,
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut fields = Vec::with_capacity(node_type.arrow_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(node_type.arrow_schema.fields().len());

    for field in node_type.arrow_schema.fields() {
        if node_type.blob_properties.contains(field.name()) {
            fields.push(Field::new(field.name(), DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![None::<&str>; num_rows])));
        } else if let Some(col) = batch.column_by_name(field.name()) {
            let batch_schema = batch.schema();
            let batch_field = batch_schema
                .field_with_name(field.name())
                .map_err(OmniError::arrow_internal)?;
            fields.push(batch_field.clone());
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

/// Build a FullTextSearchQuery from a search IR expression.
fn build_fts_query(
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
            // Use regular text search (phrase search not available in Lance 3.0 Rust API)
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
fn extract_property(expr: &IRExpr) -> Option<String> {
    match expr {
        IRExpr::PropAccess { property, .. } => Some(property.clone()),
        _ => None,
    }
}

/// Resolve an expression to a string value (literal or param).
fn resolve_to_string(expr: &IRExpr, params: &ParamMap) -> Option<String> {
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
fn resolve_to_int(expr: &IRExpr, params: &ParamMap) -> Option<i64> {
    match expr {
        IRExpr::Literal(Literal::Integer(n)) => Some(*n),
        IRExpr::Param(name) => match params.get(name)? {
            Literal::Integer(n) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn literal_to_sql(lit: &Literal) -> String {
    match lit {
        Literal::Null => "NULL".to_string(),
        Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Date(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::DateTime(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::List(_) => "NULL".to_string(), // Not supported in SQL pushdown
    }
}

// ---------------------------------------------------------------------------
// Structured DataFusion-Expr pushdown
//
// Parallel to the `ir_*_to_sql` family above, these helpers lower the same
// IR filter shapes to `datafusion::prelude::Expr` so we can call
// `Scanner::filter_expr(Expr)` instead of `Scanner::filter(&str)`. The
// structured form unlocks two things the string path could not express:
//
//   1. `CompOp::Contains` against list-typed columns (lowered to
//      `array_has(col, value)` — requires the `nested_expressions`
//      feature on the `datafusion` crate, enabled in the workspace).
//   2. Optimizer rules in DataFusion 54 that act on `Expr` shapes
//      (vectorized `IN`-list eq kernel, `PhysicalExprSimplifier`, the
//      `CASE WHEN x THEN y ELSE NULL` shortcut, etc.).
//
// Search predicates (`is_search_filter`) are still handled separately via
// `scanner.full_text_search(...)`, not via filter_expr — they stay None
// here (search predicates are never lowered to a scalar filter). The
// `literal_to_sql` path remains because the mutation/update layer
// (`exec/mutation.rs`) still produces SQL strings for `Dataset::delete(&str)`;
// that migration is MR-A's territory (Lance #6658 + delete two-phase).

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

/// Convert a single IR filter to a DataFusion `Expr`. Returns `None` for
/// search-mode filters (handled via `scanner.full_text_search`) or any
/// expression shape we can't pushdown.
pub(super) fn ir_filter_to_expr(
    filter: &IRFilter,
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::functions_nested::expr_fn::array_has;

    if is_search_filter(filter) {
        return None;
    }

    // List-contains: `prop CONTAINS value` lowers to `array_has(prop, value)`.
    // This is the case the old SQL-string pushdown had to return None for
    // ("Can't pushdown list contains"); with structured Expr it pushes down fine.
    // (Element-type coercion for the contained value is deferred — list columns
    // are not scalar-indexed, so the index-eligibility concern below does not apply.)
    if matches!(filter.op, CompOp::Contains) {
        let left = ir_expr_to_expr(&filter.left, params, None)?;
        let right = ir_expr_to_expr(&filter.right, params, None)?;
        return Some(array_has(left, right));
    }

    // Exact string predicates lower to the DataFusion `starts_with`/`contains`
    // scalar functions. The function NAMES are load-bearing: Lance's scalar
    // index expression parser matches them to probe a BTREE (`starts_with` →
    // LikePrefix) or an NGRAM index (`contains` → StringContains + recheck)
    // when one covers the column, and falls back to a plain filtered scan
    // when none does — correct either way.
    if matches!(filter.op, CompOp::StartsWith | CompOp::StringContains) {
        use datafusion::functions::expr_fn::{contains, starts_with};
        let left = ir_expr_to_expr(&filter.left, params, None)?;
        let right = ir_expr_to_expr(&filter.right, params, None)?;
        return Some(match filter.op {
            CompOp::StartsWith => starts_with(left, right),
            _ => contains(left, right),
        });
    }

    // A literal/param operand is coerced to the OTHER operand's column type so
    // the predicate stays a direct `col OP literal` and the scalar index is used.
    // Without this, DataFusion widens a narrow column (`CAST(col AS Int64)`),
    // which defeats the BTREE (validated by `probe_scalar_index_use_under_literal_type`).
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

/// Convert an IR expression to a DataFusion `Expr`. Returns `None` for
/// shapes we don't support in pushdown (search funcs, RRF, aggregates,
/// variable refs that aren't a property access).
pub(super) fn ir_expr_to_expr(
    expr: &IRExpr,
    params: &ParamMap,
    target: Option<&arrow_schema::DataType>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::ident;
    match expr {
        // #283: `ident()` preserves the identifier's case. `col()` would route
        // through SQL identifier normalization and lowercase an unquoted
        // camelCase column (`repoName` → `reponame`), which then fails to
        // resolve against the case-sensitive Lance/Arrow schema.
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
fn prop_data_type(expr: &IRExpr, schema: Option<&Schema>) -> Option<arrow_schema::DataType> {
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
fn literal_to_expr_coerced(
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
fn literal_to_typed_expr(
    lit: &Literal,
    target: &arrow_schema::DataType,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    use datafusion::scalar::ScalarValue;

    let arr = super::projection::literal_to_array(lit, 1).ok()?;
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

/// Convert a Literal to a DataFusion `Expr` in its NATURAL Arrow type. This is
/// the fallback used when the comparison column's type is unknown (no schema) or
/// when coercion to it fails; the typed, column-matched coercion that keeps
/// scalar indexes usable lives in `literal_to_typed_expr`. Returns `None` for
/// List (the SQL path also could not pushdown it — falls through to post-scan
/// in-memory application).
fn literal_to_expr(lit: &Literal) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    Some(match lit {
        Literal::Null => df_lit(datafusion::scalar::ScalarValue::Null),
        Literal::String(s) => df_lit(s.clone()),
        Literal::Integer(n) => df_lit(*n),
        Literal::Float(f) => df_lit(*f),
        Literal::Bool(b) => df_lit(*b),
        // Date/DateTime pass through as strings here. Against a typed Date
        // column DataFusion casts the LITERAL (`CAST(Utf8 AS Date32)`), which is
        // index-safe (proven by `scalar_index_use_requires_matched_literal_type`).
        // At real pushdown sites the schema is known, so `literal_to_typed_expr`
        // produces a typed Date32/Date64 anyway; this branch is only the
        // no-schema fallback.
        Literal::Date(s) => df_lit(s.clone()),
        Literal::DateTime(s) => df_lit(s.clone()),
        Literal::List(_) => return None,
    })
}

fn prefix_batch(batch: &RecordBatch, variable: &str) -> Result<RecordBatch> {
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

fn cross_join_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let n = left.num_rows();
    let m = right.num_rows();
    if n == 0 || m == 0 {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
        return Ok(RecordBatch::new_empty(Arc::new(Schema::new(fields))));
    }
    let left_indices: Vec<u32> = (0..n as u32)
        .flat_map(|i| std::iter::repeat_n(i, m))
        .collect();
    let right_indices: Vec<u32> = (0..n).flat_map(|_| 0..m as u32).collect();
    let left_expanded = take_batch(left, &UInt32Array::from(left_indices))?;
    let right_expanded = take_batch(right, &UInt32Array::from(right_indices))?;
    hconcat_batches(&left_expanded, &right_expanded)
}

fn hconcat_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    if cfg!(debug_assertions) {
        let left_schema = left.schema();
        let left_names: HashSet<&str> = left_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let right_schema = right.schema();
        for f in right_schema.fields() {
            debug_assert!(
                !left_names.contains(f.name().as_str()),
                "hconcat_batches: duplicate column '{}'",
                f.name()
            );
        }
    }
    fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
    let mut columns: Vec<ArrayRef> = left.columns().to_vec();
    columns.extend(right.columns().to_vec());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(OmniError::arrow_internal)
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col.as_ref(), indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(OmniError::arrow_internal)?;
    RecordBatch::try_new(batch.schema(), columns).map_err(OmniError::arrow_internal)
}

#[cfg(test)]
mod expand_chooser_tests {
    use super::*;
    use crate::table_store::IndexCoverage;

    /// Build cost inputs with generous hard caps, so the cost comparison (not a
    /// ceiling) is what the assertions exercise unless a test sets one on purpose.
    fn inputs(
        frontier_rows: usize,
        edge_count: u64,
        src_node_count: u64,
        effective_max_hops: u32,
        coverage: IndexCoverage,
    ) -> ExpandCostInputs {
        ExpandCostInputs {
            frontier_rows,
            edge_count,
            src_node_count,
            effective_max_hops,
            max_hops_cap: 6,
            max_frontier_cap: 1024,
            coverage,
            csr_cached: false,
            probe_factor: 1.0,
        }
    }

    #[test]
    fn undirected_probe_factor_doubles_indexed_cost() {
        // A directed traversal just under the crossover stays indexed (1 hop ×
        // frontier 100 × fanout 10 = 1,000 < 1.5·|E| = 1,500); the SAME
        // cardinalities traversed undirected pay both endpoint probes per hop
        // (2,000 > 1,500) and flip to CSR. Guards against pricing an
        // undirected traversal at half its probe count.
        let mut i = inputs(100, 1_000, 100, 1, IndexCoverage::Indexed);
        assert_eq!(choose_expand_mode(&i), ExpandMode::IndexedScan);
        i.probe_factor = 2.0;
        assert_eq!(choose_expand_mode(&i), ExpandMode::Csr);
    }

    #[test]
    fn hop_policy_switches_on_observed_frontier_over_cap() {
        // The hard ceiling becomes an execution bound: observed 2000 > 1024
        // switches regardless of the cost estimate.
        let i = inputs(1, 10_000_000, 1_000_000, 4, IndexCoverage::Indexed);
        assert!(should_switch_to_csr(2000, 100, 2, false, &i));
    }

    #[test]
    fn hop_policy_switches_on_projected_growth() {
        // The #533 shape at IMDb scale: an UNDIRECTED traversal (probe factor
        // 2, like `<coStarredWith>`), fanout ≈ 6.5, observed hop-2 frontier 238
        // growing from 1. Observed growth 238× saturates the projection at |V|
        // within a hop; the remaining 3 hops (~2×2.9M units) dwarf the CSR
        // build (~3.75M) — the switch fires at hop 2, one hop before the hard
        // ceiling would catch it. (The directed variant stays under the build
        // cost at hop 2 and is caught by the ceiling at hop 3 instead —
        // layered, both covered below.)
        let mut i = inputs(1, 2_500_000, 388_000, 4, IndexCoverage::Indexed);
        i.probe_factor = 2.0;
        assert!(should_switch_to_csr(238, 1, 3, false, &i));

        // Directed at hop 2: projection (~2.9M) is under the build cost — no
        // switch yet…
        i.probe_factor = 1.0;
        assert!(!should_switch_to_csr(238, 1, 3, false, &i));
        // …but hop 3's observed frontier (5,418) crosses the 1024 ceiling,
        // which the policy enforces as an execution bound.
        assert!(should_switch_to_csr(5_418, 238, 2, false, &i));
    }

    #[test]
    fn hop_policy_keeps_genuinely_selective_traversals_indexed() {
        // A frontier that stays tiny relative to |V| never switches: observed
        // growth ~2× on a 1M-node graph, 2 remaining hops, ~thousands of
        // scans vs a 15M-unit build.
        let i = inputs(1, 10_000_000, 1_000_000, 4, IndexCoverage::Indexed);
        assert!(!should_switch_to_csr(40, 20, 2, false, &i));
    }

    #[test]
    fn hop_policy_switches_cheaply_onto_a_warm_csr() {
        // With the CSR already built this query, any nonzero remaining indexed
        // work loses to ~free reuse.
        let i = inputs(1, 10_000_000, 1_000_000, 4, IndexCoverage::Indexed);
        assert!(should_switch_to_csr(40, 20, 2, true, &i));
    }

    #[test]
    fn hop_policy_growth_projection_saturates_at_source_count() {
        // Explosive observed growth must not project past |V_src|: with
        // saturation the 3-hop estimate is ~3·|V|·fanout; the switch verdict
        // holds but the estimate stays finite and comparable.
        let i = inputs(1, 1_000_000, 1_000, 4, IndexCoverage::Indexed);
        // observed 900 from 1 → growth 900×; |V| = 1000 caps each later hop.
        assert!(should_switch_to_csr(900, 1, 3, false, &i));
    }

    #[test]
    fn selective_frontier_on_large_graph_picks_indexed() {
        // 50 source rows against 1M source vertices, one hop: tiny selectivity —
        // the PR #149 win the chooser must preserve.
        let m = choose_expand_mode(&inputs(
            50,
            10_000_000,
            1_000_000,
            1,
            IndexCoverage::Indexed,
        ));
        assert_eq!(m, ExpandMode::IndexedScan);
    }

    #[test]
    fn flat_in_edge_count_same_selectivity_same_choice() {
        // Same selectivity (frontier/|V_src|), 1000× difference in |E|. Indexed
        // cost is independent of |E|, so the choice must not flip.
        let small = choose_expand_mode(&inputs(50, 100_000, 1_000_000, 1, IndexCoverage::Indexed));
        let huge = choose_expand_mode(&inputs(
            50,
            100_000_000,
            1_000_000,
            1,
            IndexCoverage::Indexed,
        ));
        assert_eq!(small, ExpandMode::IndexedScan);
        assert_eq!(huge, ExpandMode::IndexedScan);
    }

    #[test]
    fn frontier_large_fraction_of_source_picks_csr() {
        // hops*frontier (200) exceeds BUILD_FACTOR*|V_src| (1.5*100=150) → CSR,
        // and 200 is below the frontier cap, so it is the cost model deciding.
        let m = choose_expand_mode(&inputs(200, 1_000, 100, 1, IndexCoverage::Indexed));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn frontier_over_hard_cap_picks_csr() {
        // 2000 > 1024 ceiling, even though the selectivity is tiny.
        let m = choose_expand_mode(&inputs(
            2000,
            10_000_000,
            1_000_000,
            1,
            IndexCoverage::Indexed,
        ));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn hops_over_hard_cap_picks_csr() {
        let m = choose_expand_mode(&inputs(
            10,
            10_000_000,
            1_000_000,
            8,
            IndexCoverage::Indexed,
        ));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn degraded_single_hop_tiny_frontier_stays_indexed() {
        // One full degraded scan (1*|E|) still edges out a full CSR build
        // (1.5*|E|) for a one-off single hop.
        let m = choose_expand_mode(&inputs(
            5,
            10_000,
            10_000,
            1,
            IndexCoverage::Degraded {
                reason: "no btree".into(),
            },
        ));
        assert_eq!(m, ExpandMode::IndexedScan);
    }

    #[test]
    fn degraded_multi_hop_picks_csr() {
        // Two degraded scans (2*|E|) lose to one CSR build (1.5*|E|).
        let m = choose_expand_mode(&inputs(
            5,
            10_000,
            10_000,
            2,
            IndexCoverage::Degraded {
                reason: "no btree".into(),
            },
        ));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn warm_csr_is_always_reused() {
        // A maximally selective traversal still prefers an already-built CSR
        // (cost ~0) over re-scanning per hop.
        let mut i = inputs(1, 10_000_000, 1_000_000, 1, IndexCoverage::Indexed);
        i.csr_cached = true;
        assert_eq!(choose_expand_mode(&i), ExpandMode::Csr);
    }

    #[test]
    fn cost_model_caps_cross_type_hops() {
        // Same-type passes the requested range through; cross-type caps at 1,
        // matching execute_expand_indexed.
        assert_eq!(cost_effective_hops(5, true), 5);
        assert_eq!(cost_effective_hops(5, false), 1);
        assert_eq!(cost_effective_hops(1, false), 1);

        // Consequence: a selective frontier where the requested 5 hops would
        // (wrongly) flip cross-type to CSR, but the capped 1 hop — what actually
        // runs — keeps it indexed.
        let mut i = inputs(
            50,
            10_000,
            100,
            cost_effective_hops(5, false),
            IndexCoverage::Indexed,
        );
        assert_eq!(choose_expand_mode(&i), ExpandMode::IndexedScan);
        i.effective_max_hops = 5; // as if the cross-type cap were not applied
        assert_eq!(choose_expand_mode(&i), ExpandMode::Csr);
    }
}

#[cfg(test)]
mod referenced_edge_types_tests {
    use super::*;

    fn node_scan(var: &str, ty: &str) -> IROp {
        IROp::NodeScan {
            variable: var.to_string(),
            type_name: ty.to_string(),
            filters: Vec::new(),
        }
    }

    fn expand(edge: &str) -> IROp {
        IROp::Expand {
            src_var: "a".into(),
            dst_var: "b".into(),
            edge_type: edge.to_string(),
            direction: Direction::Out,
            dst_type: "X".into(),
            min_hops: 1,
            max_hops: Some(1),
            dst_filters: Vec::new(),
            edge_binding: None,
        }
    }

    fn names(pipeline: &[IROp]) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        collect_referenced_edge_names(pipeline, &mut set);
        set.into_iter().collect()
    }

    #[test]
    fn collects_a_single_expand_edge() {
        assert_eq!(
            names(&[node_scan("x", "ExternalID"), expand("identifiesPerson")]),
            vec!["identifiesPerson".to_string()]
        );
    }

    #[test]
    fn ignores_non_traversal_ops_and_dedups() {
        // A pipeline that touches one edge twice references exactly that one edge —
        // never the whole catalog (the cross-edge-join hang this scoping fixes).
        let pipeline = vec![
            node_scan("x", "ExternalID"),
            expand("identifiesPerson"),
            IROp::Filter(IRFilter {
                left: IRExpr::PropAccess {
                    variable: "p".into(),
                    property: "name".into(),
                },
                op: omnigraph_compiler::query::ast::CompOp::Eq,
                right: IRExpr::Literal(Literal::String("a".into())),
            }),
            expand("identifiesPerson"),
        ];
        assert_eq!(names(&pipeline), vec!["identifiesPerson".to_string()]);
    }

    #[test]
    fn recurses_through_anti_join_inner_pipeline() {
        // The bulk anti-join fast path consumes the CSR for the inner Expand's
        // edge, so its edge type must be in scope even though it is nested.
        let pipeline = vec![
            node_scan("p", "Person"),
            expand("knows"),
            IROp::AntiJoin {
                outer_var: "p".into(),
                inner: vec![expand("worksAt")],
            },
        ];
        assert_eq!(
            names(&pipeline),
            vec!["knows".to_string(), "worksAt".to_string()]
        );
    }

    #[test]
    fn recurses_through_nested_anti_joins() {
        let pipeline = vec![IROp::AntiJoin {
            outer_var: "p".into(),
            inner: vec![IROp::AntiJoin {
                outer_var: "c".into(),
                inner: vec![expand("deepEdge")],
            }],
        }];
        assert_eq!(names(&pipeline), vec!["deepEdge".to_string()]);
    }

    #[test]
    fn anti_join_with_no_inner_expand_references_no_edges() {
        // A predicate-only anti-join never asks the handle for an index, so the
        // empty set is correct — no whole-graph build is realized.
        let pipeline = vec![IROp::AntiJoin {
            outer_var: "p".into(),
            inner: vec![node_scan("c", "Company")],
        }];
        assert!(names(&pipeline).is_empty());
    }
}

#[cfg(test)]
mod literal_lowering_tests {
    use super::*;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    // With the column type known, the generic coercion types a date literal to
    // the column's Date32/Date64 (the live pushdown path). Without a target it
    // is the natural Utf8 fallback, which is still index-safe for dates because
    // DataFusion casts the LITERAL, not the column (proven by
    // `lance_surface_guards::scalar_index_use_requires_matched_literal_type`).
    #[test]
    fn date_literals_coerce_to_typed_arrow_scalars() {
        use arrow_schema::DataType;
        let dt = literal_to_expr_coerced(
            &Literal::DateTime("2024-06-01T12:00:00Z".into()),
            Some(&DataType::Date64),
        )
        .unwrap();
        assert!(
            matches!(dt, Expr::Literal(ScalarValue::Date64(Some(_)), ..)),
            "DateTime vs Date64 column must coerce to a typed Date64, got {dt:?}"
        );
        let d =
            literal_to_expr_coerced(&Literal::Date("2024-06-01".into()), Some(&DataType::Date32))
                .unwrap();
        assert!(
            matches!(d, Expr::Literal(ScalarValue::Date32(Some(_)), ..)),
            "Date vs Date32 column must coerce to a typed Date32, got {d:?}"
        );
        let nat = literal_to_expr_coerced(&Literal::Date("2024-06-01".into()), None).unwrap();
        assert!(
            matches!(nat, Expr::Literal(ScalarValue::Utf8(Some(_)), ..)),
            "no target should keep the natural Utf8 date literal, got {nat:?}"
        );
    }

    // A malformed date string makes coercion fail, so it falls back to the
    // natural Utf8 literal rather than dropping the predicate to None.
    #[test]
    fn malformed_date_literal_falls_back_to_string() {
        use arrow_schema::DataType;
        let bad = literal_to_expr_coerced(
            &Literal::DateTime("not-a-date".into()),
            Some(&DataType::Date64),
        )
        .unwrap();
        assert!(
            matches!(bad, Expr::Literal(ScalarValue::Utf8(Some(_)), ..)),
            "malformed DateTime literal should fall back to a Utf8 literal, got {bad:?}"
        );
    }

    // With a column target, a literal lowers to the column's EXACT Arrow type
    // (not its natural width), so DataFusion does not widen and cast the column
    // — keeping the scalar BTREE usable. See
    // `lance_surface_guards::scalar_index_use_requires_matched_literal_type`.
    #[test]
    fn integer_literal_coerces_to_narrow_column_type() {
        use arrow_schema::DataType;
        let i32_lit =
            literal_to_expr_coerced(&Literal::Integer(5), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(i32_lit, Expr::Literal(ScalarValue::Int32(Some(5)), ..)),
            "integer literal vs Int32 column must lower to Int32, got {i32_lit:?}"
        );
        let u32_lit =
            literal_to_expr_coerced(&Literal::Integer(7), Some(&DataType::UInt32)).unwrap();
        assert!(
            matches!(u32_lit, Expr::Literal(ScalarValue::UInt32(Some(7)), ..)),
            "integer literal vs UInt32 column must lower to UInt32, got {u32_lit:?}"
        );
    }

    #[test]
    fn float_literal_coerces_to_f32_column_type() {
        use arrow_schema::DataType;
        let f32_lit =
            literal_to_expr_coerced(&Literal::Float(1.5), Some(&DataType::Float32)).unwrap();
        assert!(
            matches!(f32_lit, Expr::Literal(ScalarValue::Float32(Some(_)), ..)),
            "float literal vs Float32 column must lower to Float32, got {f32_lit:?}"
        );
    }

    // Lossless guard: a fractional float against an integer column must NOT
    // truncate (2.7 -> 2). Fall back to the natural Float64 so the comparison
    // stays exact (no integer equals 2.7).
    #[test]
    fn fractional_float_vs_int_column_falls_back_not_truncate() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(2.7), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Float64(Some(_)), ..)),
            "fractional float vs Int32 must fall back to natural Float64, got {e:?}"
        );
    }

    // A whole-number float IS lossless against an integer column, so it coerces.
    #[test]
    fn whole_float_vs_int_column_coerces() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(2.0), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Int32(Some(2)), ..)),
            "whole-number float vs Int32 is lossless and must coerce to Int32(2), got {e:?}"
        );
    }

    // Lossless guard: an integer literal outside the column's range must NOT
    // overflow to null; fall back to the natural Int64 (correct via DataFusion).
    #[test]
    fn out_of_range_int_vs_narrow_column_falls_back() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Integer(3_000_000_000), Some(&DataType::Int32))
            .unwrap();
        assert!(
            matches!(
                e,
                Expr::Literal(ScalarValue::Int64(Some(3_000_000_000)), ..)
            ),
            "out-of-range integer vs Int32 must fall back to natural Int64, got {e:?}"
        );
    }

    // Float targets are exempt from the lossless guard: narrowing to the column's
    // own precision is the correct comparison domain, even when the value is not
    // exactly representable in F32 (0.1).
    #[test]
    fn float_vs_f32_column_coerces_even_when_not_exactly_representable() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(0.1), Some(&DataType::Float32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Float32(Some(_)), ..)),
            "float target must coerce 0.1 to Float32 (exempt from lossless guard), got {e:?}"
        );
    }

    // No target (caller without a schema) keeps the natural width — the existing
    // fallback, so behavior never regresses where the column type is unknown.
    #[test]
    fn literal_without_target_keeps_natural_width() {
        let nat = literal_to_expr_coerced(&Literal::Integer(5), None).unwrap();
        assert!(
            matches!(nat, Expr::Literal(ScalarValue::Int64(Some(5)), ..)),
            "no target should keep the natural Int64 width, got {nat:?}"
        );
    }

    // True if either operand of a binary comparison is an Int32 literal.
    fn binary_has_int32_literal(e: &Expr) -> bool {
        if let Expr::BinaryExpr(b) = e {
            [b.left.as_ref(), b.right.as_ref()]
                .iter()
                .any(|side| matches!(side, Expr::Literal(ScalarValue::Int32(Some(_)), ..)))
        } else {
            false
        }
    }

    fn int32_schema() -> arrow_schema::Schema {
        use arrow_schema::{DataType, Field};
        arrow_schema::Schema::new(vec![Field::new("count", DataType::Int32, true)])
    }

    fn count_prop() -> IRExpr {
        IRExpr::PropAccess {
            variable: "m".into(),
            property: "count".into(),
        }
    }

    // Coercion is operator-independent: a range comparison's literal coerces to
    // the column type just like equality does, so range filters on a narrow
    // numeric column keep the BTREE.
    #[test]
    fn ir_filter_coerces_literal_for_range_op() {
        let schema = int32_schema();
        let filter = IRFilter {
            left: count_prop(),
            op: CompOp::Ge,
            right: IRExpr::Literal(Literal::Integer(2)),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert!(
            binary_has_int32_literal(&expr),
            "range-op literal must coerce to the Int32 column type, got {expr:?}"
        );
    }

    // The column may be on either side; the literal coerces to the opposite
    // operand's column type regardless of order (`5 < count`).
    #[test]
    fn ir_filter_coerces_literal_when_column_is_on_the_right() {
        let schema = int32_schema();
        let filter = IRFilter {
            left: IRExpr::Literal(Literal::Integer(2)),
            op: CompOp::Lt,
            right: count_prop(),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert!(
            binary_has_int32_literal(&expr),
            "reversed-operand literal must coerce to the Int32 column type, got {expr:?}"
        );
    }

    // Name of the left operand's column in a binary comparison `col OP lit`.
    fn binary_left_column_name(e: &Expr) -> Option<String> {
        match e {
            Expr::BinaryExpr(b) => match b.left.as_ref() {
                Expr::Column(c) => Some(c.name.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    // #283: a camelCase property must reach the scan as its exact column name,
    // not a SQL-normalized (lowercased) one. `col()` lowercases unquoted
    // identifiers; the pushed-down column ref must stay `repoName`.
    #[test]
    fn ir_filter_preserves_camelcase_column_name() {
        use arrow_schema::{DataType, Field};
        let schema = arrow_schema::Schema::new(vec![Field::new("repoName", DataType::Utf8, true)]);
        let filter = IRFilter {
            left: IRExpr::PropAccess {
                variable: "d".into(),
                property: "repoName".into(),
            },
            op: CompOp::Eq,
            right: IRExpr::Literal(Literal::String("acme".into())),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert_eq!(
            binary_left_column_name(&expr).as_deref(),
            Some("repoName"),
            "camelCase column must be preserved (not lowercased to `reponame`), got {expr:?}"
        );
    }

    // Index preservation: a camelCase numeric column still coerces its literal
    // (so the scalar BTREE stays eligible) — the col→ident fix must not disturb
    // the coercion path (which resolves the column type via field_with_name).
    #[test]
    fn ir_filter_coerces_literal_for_camelcase_int_column() {
        use arrow_schema::{DataType, Field};
        let schema =
            arrow_schema::Schema::new(vec![Field::new("itemCount", DataType::Int32, true)]);
        let filter = IRFilter {
            left: IRExpr::PropAccess {
                variable: "m".into(),
                property: "itemCount".into(),
            },
            op: CompOp::Eq,
            right: IRExpr::Literal(Literal::Integer(2)),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert!(
            binary_has_int32_literal(&expr),
            "camelCase int column must keep its coerced Int32 literal (BTREE-eligible), got {expr:?}"
        );
    }
}

/// Always-on unit coverage for the needed-columns walk. IO-free and
/// parallel-safe, unlike the `#[ignore]`d byte gate in
/// `column_projection_tests`.
#[cfg(test)]
mod needed_columns_tests {
    use super::*;

    fn prop(variable: &str, property: &str) -> IRExpr {
        IRExpr::PropAccess {
            variable: variable.to_string(),
            property: property.to_string(),
        }
    }

    fn ir(pipeline: Vec<IROp>, return_exprs: Vec<IRExpr>, order_by: Vec<IRExpr>) -> QueryIR {
        QueryIR {
            name: "t".to_string(),
            params: vec![],
            pipeline,
            return_exprs: return_exprs
                .into_iter()
                .map(|expr| IRProjection { expr, alias: None })
                .collect(),
            order_by: order_by
                .into_iter()
                .map(|expr| IROrdering {
                    expr,
                    descending: false,
                })
                .collect(),
            limit: None,
        }
    }

    fn scan(variable: &str) -> IROp {
        IROp::NodeScan {
            variable: variable.to_string(),
            type_name: "T".to_string(),
            filters: vec![],
        }
    }

    fn columns_of<'a>(needed: &'a HashMap<String, NeededColumns>, var: &str) -> &'a NeededColumns {
        needed.get(var).expect("binding must be in the demand map")
    }

    fn assert_columns(needed: &HashMap<String, NeededColumns>, var: &str, expected: &[&str]) {
        match columns_of(needed, var) {
            NeededColumns::All => panic!("expected specific columns for '{var}', got All"),
            NeededColumns::Columns(cols) => {
                let mut got: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                got.sort_unstable();
                let mut want = expected.to_vec();
                want.sort_unstable();
                assert_eq!(got, want, "needed columns for '{var}'");
            }
        }
    }

    #[test]
    fn return_props_are_the_only_demand_for_a_plain_projection() {
        let q = ir(vec![scan("c")], vec![prop("c", "slug")], vec![]);
        let needed = collect_needed_columns(&q);
        assert_columns(&needed, "c", &["slug"]);
    }

    #[test]
    fn order_filters_and_aggregates_all_contribute() {
        let q = ir(
            vec![
                scan("c"),
                IROp::Filter(IRFilter {
                    left: prop("c", "state"),
                    op: CompOp::Eq,
                    right: IRExpr::Literal(Literal::String("open".into())),
                }),
            ],
            vec![IRExpr::Aggregate {
                func: AggFunc::Count,
                arg: Box::new(prop("c", "slug")),
            }],
            vec![prop("c", "rank")],
        );
        let needed = collect_needed_columns(&q);
        assert_columns(&needed, "c", &["slug", "state", "rank"]);
    }

    #[test]
    fn bare_variable_reference_fails_open_to_all() {
        let q = ir(
            vec![scan("c")],
            vec![IRExpr::Variable("c".to_string()), prop("c", "slug")],
            vec![],
        );
        let needed = collect_needed_columns(&q);
        assert!(
            matches!(columns_of(&needed, "c"), NeededColumns::All),
            "a bare $var must demand the whole row regardless of other refs"
        );
    }

    #[test]
    fn anti_join_inner_filters_attribute_to_their_bindings() {
        // not-exists inner pipeline referencing both an inner and the outer
        // binding: the outer scan must still read the outer column the inner
        // filter compares against.
        let inner = vec![
            scan("x"),
            IROp::Filter(IRFilter {
                left: prop("x", "kind"),
                op: CompOp::Eq,
                right: prop("c", "kind_ref"),
            }),
        ];
        let q = ir(
            vec![
                scan("c"),
                IROp::AntiJoin {
                    outer_var: "c".to_string(),
                    inner,
                },
            ],
            vec![prop("c", "slug")],
            vec![],
        );
        let needed = collect_needed_columns(&q);
        assert_columns(&needed, "c", &["slug", "kind_ref"]);
        assert_columns(&needed, "x", &["kind"]);
    }

    #[test]
    fn nearest_records_the_ranked_vector_property() {
        let q = ir(
            vec![scan("c")],
            vec![prop("c", "slug")],
            vec![IRExpr::Nearest {
                variable: "c".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Param("q".to_string())),
            }],
        );
        let needed = collect_needed_columns(&q);
        // Search bindings fail open at the scan; the demand set still
        // carries the ranked column so search-scan pruning can consume it
        // without re-walking.
        assert_columns(&needed, "c", &["slug", "embedding"]);
    }

    #[test]
    fn unreferenced_binding_has_no_demand_entry() {
        // Cross-join shape: `$d` is bound but never referenced — no demand
        // entry. `execute_node_scan` fails open to the full non-blob
        // projection for a missing entry.
        let q = ir(vec![scan("c"), scan("d")], vec![prop("c", "slug")], vec![]);
        let needed = collect_needed_columns(&q);
        assert!(needed.contains_key("c"));
        assert!(!needed.contains_key("d"));
    }

    #[test]
    fn expand_dst_filters_attribute_to_the_dst_binding() {
        let q = ir(
            vec![
                scan("a"),
                IROp::Expand {
                    src_var: "a".to_string(),
                    dst_var: "b".to_string(),
                    edge_type: "knows".to_string(),
                    direction: Direction::Out,
                    dst_type: "T".to_string(),
                    min_hops: 1,
                    max_hops: Some(1),
                    dst_filters: vec![IRFilter {
                        left: prop("b", "state"),
                        op: CompOp::Eq,
                        right: IRExpr::Literal(Literal::String("open".into())),
                    }],
                    edge_binding: None,
                },
            ],
            vec![prop("a", "slug")],
            vec![],
        );
        let needed = collect_needed_columns(&q);
        assert_columns(&needed, "a", &["slug"]);
        assert_columns(&needed, "b", &["state"]);
    }

    #[test]
    fn search_expression_arms_attribute_field_and_nested_columns() {
        // Nothing consumes these entries while search scans fail open; this
        // pins the walk's completeness — every operand (field, query,
        // max_edits, k) — independent of any consumer.
        let q = ir(
            vec![scan("c")],
            vec![prop("c", "slug")],
            vec![IRExpr::Rrf {
                primary: Box::new(IRExpr::Fuzzy {
                    field: Box::new(prop("c", "title")),
                    query: Box::new(prop("c", "probe")),
                    max_edits: Some(Box::new(prop("c", "edits"))),
                }),
                secondary: Box::new(IRExpr::Bm25 {
                    field: Box::new(prop("c", "body")),
                    query: Box::new(IRExpr::Literal(Literal::String("q".into()))),
                }),
                k: Some(Box::new(prop("c", "k_ref"))),
            }],
        );
        let needed = collect_needed_columns(&q);
        assert_columns(
            &needed,
            "c",
            &["slug", "title", "probe", "edits", "body", "k_ref"],
        );
    }

    #[test]
    fn rrf_leg_targets_fail_open_in_both_legs() {
        // Cross-variable RRF: both legs' wide batches feed one fused concat,
        // so each leg's search target must fail open in the OTHER leg too.
        let mut needed = HashMap::new();
        needed.insert(
            "a".to_string(),
            NeededColumns::Columns(HashSet::from(["x".to_string()])),
        );
        let rrf = RrfMode {
            primary: Box::new(SearchMode {
                nearest: Some(("a".to_string(), "emb".to_string(), vec![], 10)),
                ..Default::default()
            }),
            secondary: Box::new(SearchMode {
                bm25: Some(("b".to_string(), "text".to_string(), "q".to_string())),
                ..Default::default()
            }),
            k: 60,
            limit: 10,
        };
        fail_open_rrf_leg_targets(&mut needed, &rrf);
        assert!(matches!(needed.get("a"), Some(NeededColumns::All)));
        assert!(matches!(needed.get("b"), Some(NeededColumns::All)));
    }
}

#[cfg(test)]
mod column_projection_tests {
    use super::*;

    use crate::db::ReadTarget;
    use crate::loader::{LoadMode, load_jsonl};

    /// Embedding width. Wide enough (4 bytes/dim = 3 KiB/row) that the vector
    /// column dominates the table, without an unwieldy JSONL fixture.
    const DIM: usize = 768;
    const ROWS: usize = 400;

    const SCHEMA: &str = r#"
node Chunk {
    slug: String @key
    embedding: Vector(768)
}
"#;

    const QUERIES: &str = r#"
query list_slugs() {
    match { $c: Chunk }
    return { $c.slug }
}

query first_slug() {
    match { $c: Chunk }
    return { $c.slug }
    limit 1
}
"#;

    /// Deterministic pseudo-random embeddings: a constant vector compresses to
    /// nothing and would understate the column's real read cost.
    fn seed_data() -> String {
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut out = String::with_capacity(ROWS * DIM * 12);
        for row in 0..ROWS {
            out.push_str(&format!(
                r#"{{"type":"Chunk","data":{{"slug":"chunk-{row:05}","embedding":["#
            ));
            for dim in 0..DIM {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let value = (state >> 40) as f32 / 16_777_216.0;
                if dim > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{value:.6}"));
            }
            out.push_str("]}}\n");
        }
        out
    }

    /// What a measured read does after the `node:Chunk` handle is warm.
    enum Read {
        /// A GQ query, by name, and the rows it must return.
        Query(&'static str, usize),
        /// A Lance scan of the same pinned version, projected to `columns`.
        LanceProjected(&'static [&'static str]),
        /// A Lance scan of the same pinned version, no projection.
        LanceFull,
    }

    /// Object-store bytes one read costs, measured on Lance's own per-store
    /// `IOTracker` — the seam that sees local-file reads: `ObjectStore::open`
    /// routes the `file` scheme through `LocalObjectReader::open_with_tracker`,
    /// bypassing any wrapped `object_store` instrumentation.
    ///
    /// Each call opens its own `Omnigraph` handle, so every arm reads cold from
    /// its own `ReadCaches`/`Session`. The `node:Chunk` handle is opened first
    /// and its cost discarded, so the measurement covers the scan alone — and so
    /// the query below reuses that same cached `Dataset`, hence the same store
    /// and the same tracker.
    async fn read_bytes(uri: &str, read: Read) -> u64 {
        let db = Omnigraph::open(uri).await.unwrap();
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let dataset = snapshot.open_lance_dataset("node:Chunk").await.unwrap();
        let store = dataset.object_store(None).await.unwrap();
        let _ = store.io_stats_incremental();

        let (rows, expected) = match read {
            Read::Query(name, expected) => {
                let result = db
                    .query(ReadTarget::branch("main"), QUERIES, name, &ParamMap::new())
                    .await
                    .unwrap();
                (
                    result.batches().iter().map(|b| b.num_rows()).sum::<usize>(),
                    expected,
                )
            }
            Read::LanceProjected(columns) => {
                let mut scanner = dataset.scan();
                scanner.project(columns).unwrap();
                let batches: Vec<RecordBatch> = scanner
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                (batches.iter().map(|b| b.num_rows()).sum::<usize>(), ROWS)
            }
            Read::LanceFull => {
                let batches: Vec<RecordBatch> = dataset
                    .scan()
                    .try_into_stream()
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                (batches.iter().map(|b| b.num_rows()).sum::<usize>(), ROWS)
            }
        };
        assert_eq!(rows, expected, "each arm must return the rows it asked for");
        store.io_stats_incremental().read_bytes
    }

    /// `return { $c.slug }` must not read the `embedding` column.
    ///
    /// Three reads of one identical `Chunk` table: the GQ query, a Lance scan
    /// projected to the lightweight columns (what a column-pruned scan costs),
    /// and an unprojected Lance scan (what a full-row read costs). With column
    /// pruning on the scan the query stays within 2× of the projected scan
    /// (headroom for catalog/`__manifest` reads through the same store).
    ///
    /// Ignored in the parallel suite: the engine's process-wide
    /// `STORE_REGISTRY` (`lance_access.rs`) shares one `ObjectStore` per
    /// `file://` provider, so this test's `IOTracker` also counts every
    /// concurrent test's reads. The measurement is exact when the process is
    /// quiet. Lives in-source rather than beside `tests/helpers/cost.rs`
    /// (the designated home for object-store counters) because the
    /// per-store `IOTracker` seam this measurement needs is reachable only
    /// in-crate — `Snapshot::open_lance_dataset` is `pub(crate)`.
    #[tokio::test]
    #[ignore = "byte-cost gate; the local-FS IOTracker is process-shared — run solo via `cargo test -p omnigraph-engine --lib column_projection_tests -- --ignored --nocapture`"]
    async fn slug_projection_does_not_read_vector_column_issue_564() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Omnigraph::init(uri, SCHEMA).await.unwrap();
        load_jsonl(&db, &seed_data(), LoadMode::Overwrite)
            .await
            .unwrap();
        drop(db);

        let query = read_bytes(uri, Read::Query("list_slugs", ROWS)).await;
        let limited = read_bytes(uri, Read::Query("first_slug", 1)).await;
        let pruned = read_bytes(uri, Read::LanceProjected(&["id", "slug"])).await;
        let full = read_bytes(uri, Read::LanceFull).await;

        println!("gq return slug          = {query} bytes");
        println!("gq return slug limit 1  = {limited} bytes");
        println!("lance projected scan    = {pruned} bytes");
        println!("lance full scan         = {full} bytes");

        assert!(pruned > 0, "tracker must observe the projected scan");
        assert!(
            pruned * 4 <= full,
            "fixture must make the vector column dominate: pruned={pruned} full={full}"
        );
        assert!(
            query * 2 >= pruned,
            "the query's reads must reach the same tracker: query={query} pruned={pruned}"
        );
        assert!(
            limited <= pruned * 2,
            "limit 1 must stay within 2x of the projected scan: limited={limited} pruned={pruned}"
        );
        assert!(
            query <= pruned * 2,
            "a slug-only projection must not pay for the embedding column: \
             query={query} pruned={pruned} full={full}"
        );
    }
}

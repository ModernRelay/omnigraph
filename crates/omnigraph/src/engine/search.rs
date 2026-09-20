//! v2's copy of v1's search-mode extraction, prefilter gates and probe
//! ladders (phase 4). Copied, never referenced: the frozen `exec/query.rs`
//! stays upstream's bytes.

use super::*;

/// Describes how the query's ordering changes the scan mode.
#[derive(Debug, Default, Clone)]
pub(super) struct SearchMode {
    /// Vector ANN search: (variable, property, query_vector, k).
    pub(super) nearest: Option<(String, String, Vec<f32>, usize)>,
    /// Maximum number of IVF payload partitions a nearest scan may search,
    /// per index delta; `None` is uncapped. The scan-site ladder in
    /// `execute_node_scan` widens a maximum that starves the scan.
    pub(super) ann_probe_budget: Option<usize>,
    /// The nearest scan runs flat (`use_index(false)`, every row scored): the
    /// overfetch loop's exact pass.
    pub(super) nearest_exact: bool,
    /// The nearest prefilter gate proved the answer empty: no node of the
    /// ranked type satisfies an Expand's first hop. Set only by
    /// `nearest_prefilter_gate`, never on an RRF arm.
    pub(super) answer_proven_empty: bool,
    /// BM25 full-text search: (variable, property, query_text).
    pub(super) bm25: Option<(String, String, String)>,
    /// RRF fusion: (primary, secondary, k_constant, limit).
    pub(super) rrf: Option<RrfMode>,
    /// The set a gate ANDs into the ranked scan as `id IN (...)`. Never set
    /// on an RRF nearest arm.
    pub(super) eligible_ids: Option<EligibleIds>,
}

/// Shared eligible-id set, `Debug`-opaque so a logged `SearchMode` prints the
/// cardinality instead of up to `DEFAULT_RRF_GATE_MAX_IDS` id strings.
#[derive(Clone)]
pub(super) struct EligibleIds(Arc<Vec<String>>);

impl std::fmt::Debug for EligibleIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EligibleIds(len={})", self.0.len())
    }
}

/// What the nearest scan reported back to the query level: the LAST scan of
/// the ranked variable in a pass. `rows == k` means the scan was full, so a
/// shortfall above it can only be recovered by asking for more candidates.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ScanReport {
    pub(super) nearest_scan: Option<NearestScanReport>,
}

/// One nearest scan as `execute_node_scan`'s ladder left it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NearestScanReport {
    /// Rows the scan returned.
    pub(super) rows: usize,
    /// Candidates the scan asked for.
    pub(super) k: usize,
    /// The probe cap the scan ran under; `None` is uncapped.
    pub(super) maximum_nprobes: Option<usize>,
    /// The scan holds every row Lance could return for this query: it ran
    /// as the flat exact kNN (every admitted row scored), or it returned
    /// every row the gate's `id IN` list admits (`rows >= known_matches`).
    pub(super) exhausted: bool,
    /// Live rows in the ranked type (`count_rows`, deletions excluded): the
    /// `k` of the overfetch loop's exact pass.
    pub(super) dataset_rows: u64,
}

/// The next pass of `execute_query`'s overfetch loop after `factor` × `k0`
/// candidates left the answer short above a full scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OverfetchRung {
    /// Ask for `k` candidates under the cap that filled the previous pass.
    Wider {
        factor: usize,
        k: usize,
        maximum: Option<usize>,
    },
    /// The exact pass: every live row of the type, scored flat, no probe cap.
    Exact { k: usize },
}

/// Widen nearest's candidate limit by four, then scan the whole type exactly.
/// Lance returns at most `k` candidates regardless of probe width.
/// Return `None` when the previous scan already requested every live row.
pub(super) fn next_overfetch_rung(
    factor: usize,
    k0: usize,
    scan: NearestScanReport,
) -> Option<OverfetchRung> {
    let whole = usize::try_from(scan.dataset_rows).unwrap_or(usize::MAX);
    let next = factor.saturating_mul(ANN_OVERFETCH_STEP);
    let k = k0.saturating_mul(next);
    if next <= ANN_OVERFETCH_MAX_FACTOR && k < whole {
        return Some(OverfetchRung::Wider {
            factor: next,
            k,
            maximum: scan.maximum_nprobes,
        });
    }
    (whole > scan.k).then_some(OverfetchRung::Exact { k: whole })
}

impl SearchMode {
    /// This mode asking the nearest scan for `k` candidates under the probe
    /// cap `maximum` (`None` = uncapped). The cap is the rung that filled the
    /// previous pass's scan, so the rerun does not re-climb from the base cap.
    pub(super) fn with_nearest_k(&self, k: usize, maximum: Option<usize>) -> Self {
        let mut mode = self.clone();
        if let Some((_, _, _, current)) = mode.nearest.as_mut() {
            *current = k;
        }
        mode.ann_probe_budget = maximum;
        mode
    }

    /// This mode running the nearest scan flat over `k` = every live row of
    /// the type: the overfetch loop's exact pass.
    pub(super) fn with_exact_nearest(&self, k: usize) -> Self {
        let mut mode = self.with_nearest_k(k, None);
        mode.nearest_exact = true;
        mode
    }

    /// Whether `variable`'s scan is proven empty: `answer_proven_empty` on
    /// the mode that ranks `variable` by `nearest`.
    pub(super) fn scan_proven_empty(&self, variable: &str) -> bool {
        self.answer_proven_empty
            && self
                .nearest
                .as_ref()
                .is_some_and(|(var, ..)| var == variable)
    }

    /// The eligible-id set to AND into `variable`'s scan, if this mode ranks
    /// `variable` (a bm25 arm, or a standalone nearest) and a prefilter gate
    /// chose the prefilter plan.
    pub(super) fn eligible_ids_for(&self, variable: &str) -> Option<&[String]> {
        let ids = self.eligible_ids.as_ref()?;
        let ranks_variable = self.bm25.as_ref().is_some_and(|(var, ..)| var == variable)
            || self
                .nearest
                .as_ref()
                .is_some_and(|(var, ..)| var == variable);
        ranks_variable.then(|| ids.0.as_slice())
    }
}

#[derive(Debug, Clone)]
pub(super) struct RrfMode {
    pub(super) primary: Box<SearchMode>,
    pub(super) secondary: Box<SearchMode>,
    pub(super) k: u32,
    pub(super) limit: usize,
}

/// Extract search ordering mode from the IR.
pub(super) async fn extract_search_mode(
    ir: &QueryIR,
    params: &ParamMap,
    catalog: &Catalog,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
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
                ann_probe_budget: settings.ann_nprobes(),
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
                extract_sub_search_mode(ir, primary, params, catalog, embedding, settings).await?;
            let secondary_mode =
                extract_sub_search_mode(ir, secondary, params, catalog, embedding, settings)
                    .await?;

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

/// Extract a nearest or BM25 arm for RRF fusion. BM25 arms are uncapped:
/// pruning contributions can change fused ranks even when the result fills its limit.
pub(super) async fn extract_sub_search_mode(
    ir: &QueryIR,
    expr: &IRExpr,
    params: &ParamMap,
    catalog: &Catalog,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
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
                ann_probe_budget: settings.ann_nprobes(),
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
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Resolve a nearest query vector, embedding string inputs with the property's
/// recorded model. Explicit vectors do not require an embedding client.
pub(super) async fn resolve_nearest_query_vec(
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
            let client = embedding.resolve().await?;
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

pub(super) fn resolve_literal_or_param(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
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
pub(super) fn literal_to_f32_vec(lit: &Literal) -> Result<Vec<f32>> {
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
pub(super) fn nearest_property_dim_and_model(
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

pub(super) fn resolve_binding_type_name<'a>(
    pipeline: &'a [IROp],
    variable: &str,
) -> Option<&'a str> {
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

/// A value bound through the Rust `ParamMap` API skips the JSON param arm: refuse
/// a time-bearing `Date` string, and a non-`Date` literal on a `Date` parameter.
pub(super) fn check_param_date_literals(
    params: &ParamMap,
    declared: &[omnigraph_compiler::query::ast::Param],
) -> Result<()> {
    fn check(name: &str, lit: &Literal) -> Result<()> {
        match lit {
            Literal::Date(value) => omnigraph_compiler::check_date_literal(value)
                .map_err(|reason| OmniError::manifest(format!("param '{name}': {reason}"))),
            Literal::List(items) => items.iter().try_for_each(|item| check(name, item)),
            _ => Ok(()),
        }
    }
    params.iter().try_for_each(|(name, lit)| check(name, lit))?;
    for param in declared {
        let Some(lit) = params.get(&param.name) else {
            continue;
        };
        let is_date = |lit: &Literal| match lit {
            Literal::Date(_) => true,
            Literal::Null => param.nullable,
            _ => false,
        };
        let well_typed = match param.type_name.as_str() {
            "Date" => is_date(lit),
            "[Date]" => match lit {
                Literal::List(items) => items.iter().all(is_date),
                other => is_date(other),
            },
            _ => true,
        };
        if !well_typed {
            return Err(OmniError::manifest(format!(
                "param '{}': expected {}, got {lit:?}",
                param.name, param.type_name
            )));
        }
    }
    Ok(())
}

/// Check if the query's ordering is search-imposed (`nearest()`/`bm25`).
pub(super) fn is_search_ordered(search_mode: &SearchMode) -> bool {
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
pub(super) fn search_score_orderings(search_mode: &SearchMode) -> Option<Vec<IROrdering>> {
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

/// Prefilter admission ratio: the gate's selective plan runs when
/// |eligible| / corpus is at or below this. Set by the gate benchmark
/// (`benches/scenarios.rs` `rrf-gate`, 2026-08-31): on a 100k-row corpus the
/// prefiltered plan's warm wall clock still beat the postfilter plan's at
/// 10% eligibility (31.5 ms vs 53.5 ms) and lost at 25% (85 ms vs 68.5 ms);
/// a 200 KiB-payload corpus crossed even higher. 0.10 is the conservative
/// (smaller) crossover across both corpora.
pub(super) const DEFAULT_RRF_GATE_RATIO: f64 = 0.10;

/// Absolute ceiling on the eligible-id in-list: the per-id predicate cost
/// the ratio cannot see on huge corpora. Set by the same benchmark's 10^5 /
/// 10^6 microbench (1e6-row corpus): at 1e5 ids the prefiltered plan still
/// won (324.5 ms vs 360.5 ms warm) and at 1e6 it lost 1.7x (2.76 s vs
/// 1.63 s) — `Expr` construction itself stays negligible (31 ms at 1e6);
/// the loss is the in-list probe/filter evaluation.
pub(super) const DEFAULT_RRF_GATE_MAX_IDS: usize = 100_000;

pub(super) fn rrf_gate_ratio() -> f64 {
    std::env::var("OMNIGRAPH_RRF_GATE_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|r| r.is_finite() && *r >= 0.0)
        .unwrap_or(DEFAULT_RRF_GATE_RATIO)
}

pub(super) fn rrf_gate_max_ids() -> usize {
    std::env::var("OMNIGRAPH_RRF_GATE_MAX_IDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_RRF_GATE_MAX_IDS)
}

/// Required first hops from a top-level scanned ranked variable, or `None`
/// when it is an Expand destination or has no constraining Expand.
/// AntiJoin and zero-hop Expands cannot constrain this superset of survivors.
pub(super) fn rrf_gate_expand_sources<'a>(
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
pub(super) async fn rrf_prefilter_gate(
    ir: &QueryIR,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle,
    catalog: &Catalog,
    rrf: &RrfMode,
    rrf_plan: RrfPlan,
) -> Option<EligibleIds> {
    let fall_back =
        |fallback: RrfGateFallback, forced: bool, eligible: Option<u64>, corpus: Option<u64>| {
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

    if rrf_plan == RrfPlan::ForcePostfilter {
        fall_back(RrfGateFallback::Forced, true, None, None);
        return None;
    }
    let forced = rrf_plan == RrfPlan::ForcePrefilter;
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
    let node_key = format!("node:{}", ranked_type);
    let Some(node_entry) = snapshot.dataset(&node_key) else {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    };
    let corpus = node_entry.entity_count;
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

    #[cfg_attr(not(debug_assertions), allow(unused_mut))]
    let (mut ids, eligible_count) =
        match adjacency_eligible_ids(graph_index, catalog, ranked_type, &sources, corpus, forced)
            .await
        {
            EligibleOutcome::Ids { ids, eligible } => (ids, eligible),
            EligibleOutcome::FallBack { fallback, eligible } => {
                fall_back(fallback, forced, eligible, Some(corpus));
                return None;
            }
        };
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

/// Outcome of the adjacency eligible-id computation shared by the rrf and
/// nearest prefilter gates.
pub(super) enum EligibleOutcome {
    /// The eligible ids, materialized; `eligible` is their count.
    Ids { ids: Vec<String>, eligible: u64 },
    /// Fell back: the reason and the count when it was reached. The caller
    /// records the verdict on its own probe.
    FallBack {
        fallback: crate::instrumentation::RrfGateFallback,
        eligible: Option<u64>,
    },
}

/// Intersect required first-hop adjacency sets into a superset of survivors.
/// Validate endpoint types and dense-space widths before probing adjacency;
/// count before allocating ids, and fall back on incomplete materialization.
pub(super) async fn adjacency_eligible_ids(
    graph_index: &GraphIndexHandle,
    catalog: &Catalog,
    ranked_type: &str,
    sources: &[(&str, Direction)],
    corpus: u64,
    forced: bool,
) -> EligibleOutcome {
    let fall_back = |fallback: RrfGateFallback, eligible: Option<u64>| EligibleOutcome::FallBack {
        fallback,
        eligible,
    };
    let graph = match graph_index.get().await {
        Ok(Some(graph)) => graph,
        Ok(None) | Err(_) => return fall_back(RrfGateFallback::BuildErr, None),
    };
    let Some(idx) = graph.type_index(ranked_type) else {
        return fall_back(RrfGateFallback::EmptyEligible, Some(0));
    };
    let mut adjacencies: Vec<(
        Option<&crate::graph_index::CsrIndex>,
        Option<&crate::graph_index::CsrIndex>,
    )> = Vec::with_capacity(sources.len());
    for (edge_type, direction) in sources {
        let Some(edge_def) = catalog.edge_types.get(*edge_type) else {
            return fall_back(RrfGateFallback::Shape, None);
        };
        let side_matches = match direction {
            Direction::Out => edge_def.from_type == ranked_type,
            Direction::In => edge_def.to_type == ranked_type,
            Direction::Both => edge_def.from_type == ranked_type && edge_def.to_type == ranked_type,
        };
        if !side_matches {
            return fall_back(RrfGateFallback::Shape, None);
        }
        let (out, incoming) = match direction {
            Direction::Out => (graph.csr(edge_type), None),
            Direction::In => (None, graph.csc(edge_type)),
            Direction::Both => (graph.csr(edge_type), graph.csc(edge_type)),
        };
        if out.is_none() && incoming.is_none() {
            return fall_back(RrfGateFallback::EmptyEligible, Some(0));
        }
        for adjacency in [out, incoming].into_iter().flatten() {
            if adjacency.num_nodes() != idx.len() {
                return fall_back(RrfGateFallback::BuildErr, None);
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
    let eligible_count = (0..idx.len() as u32).filter(|&dense| passes(dense)).count() as u64;
    if eligible_count == 0 {
        return fall_back(RrfGateFallback::EmptyEligible, Some(0));
    }
    if !forced {
        let ratio_ok = corpus > 0 && (eligible_count as f64) <= rrf_gate_ratio() * (corpus as f64);
        let cap_ok = eligible_count <= rrf_gate_max_ids() as u64;
        if !(ratio_ok && cap_ok) {
            return fall_back(RrfGateFallback::Threshold, Some(eligible_count));
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
    if ids.len() as u64 != eligible_count {
        return fall_back(RrfGateFallback::BuildErr, Some(eligible_count));
    }
    EligibleOutcome::Ids {
        ids,
        eligible: eligible_count,
    }
}

/// The nearest prefilter gate (issue #567): a standalone `nearest` whose
/// ranked variable a top-level Expand constrains ANDs the traversal's
/// eligible-id superset into its scan.
pub(super) async fn nearest_prefilter_gate(
    ir: &QueryIR,
    snapshot: &Snapshot,
    graph_index: &GraphIndexHandle,
    catalog: &Catalog,
    mode: &SearchMode,
    rrf_plan: RrfPlan,
) -> NearestGatePlan {
    let Some((ranked_var, ..)) = mode.nearest.as_ref() else {
        return NearestGatePlan::Postfilter;
    };
    let forced = rrf_plan != RrfPlan::Auto;
    let fall_back = |fallback: RrfGateFallback, eligible: Option<u64>, corpus: Option<u64>| {
        tracing::debug!(
            ?fallback,
            forced,
            "nearest prefilter gate fell back to the unfiltered scan"
        );
        record_ann_prefilter_verdict(RrfGateVerdict {
            plan: RrfGatePlan::Postfilter,
            fallback: Some(fallback),
            forced,
            eligible,
            corpus,
        });
    };
    if rrf_plan == RrfPlan::ForcePostfilter {
        fall_back(RrfGateFallback::Forced, None, None);
        return NearestGatePlan::Postfilter;
    }
    let Some(sources) = rrf_gate_expand_sources(&ir.pipeline, ranked_var) else {
        fall_back(RrfGateFallback::Shape, None, None);
        return NearestGatePlan::Postfilter;
    };
    let Some(ranked_type) = resolve_binding_type_name(&ir.pipeline, ranked_var) else {
        fall_back(RrfGateFallback::Shape, None, None);
        return NearestGatePlan::Postfilter;
    };
    let node_key = format!("node:{}", ranked_type);
    let Some(node_entry) = snapshot.dataset(&node_key) else {
        fall_back(RrfGateFallback::Shape, None, None);
        return NearestGatePlan::Postfilter;
    };
    let corpus = node_entry.entity_count;
    match adjacency_eligible_ids(graph_index, catalog, ranked_type, &sources, corpus, forced).await
    {
        EligibleOutcome::Ids { ids, eligible } => {
            record_ann_prefilter_verdict(RrfGateVerdict {
                plan: RrfGatePlan::Prefilter,
                fallback: None,
                forced,
                eligible: Some(eligible),
                corpus: Some(corpus),
            });
            NearestGatePlan::Prefilter(EligibleIds(Arc::new(ids)))
        }
        EligibleOutcome::FallBack { fallback, eligible } => {
            fall_back(fallback, eligible, Some(corpus));
            if fallback == RrfGateFallback::EmptyEligible {
                NearestGatePlan::ProvenEmpty
            } else {
                NearestGatePlan::Postfilter
            }
        }
    }
}

/// What `nearest_prefilter_gate` decided for a standalone `nearest`.
pub(super) enum NearestGatePlan {
    /// The selective plan: AND these ids into the ranked scan.
    Prefilter(EligibleIds),
    /// The unfiltered scan; a traversal shortfall above it is bounded by
    /// `execute_query`'s overfetch loop.
    Postfilter,
    /// The eligible set is empty, which proves the answer empty: the ranked
    /// scan returns its zero-row batch without running Lance and no
    /// overfetch can add a row.
    ProvenEmpty,
}

/// Whether any top-level Expand leaves `variable`: the precondition for the
/// nearest prefilter gate to have anything to constrain the scan with. A
/// presence test only; `rrf_gate_expand_sources` decides superset-safety.
pub(super) fn pipeline_expands_from(pipeline: &[IROp], variable: &str) -> bool {
    pipeline
        .iter()
        .any(|op| matches!(op, IROp::Expand { src_var, .. } if src_var == variable))
}

/// This arm's mode with the eligible-id prefilter attached iff the arm
/// carries a bm25 target. A `nearest` arm passes through untouched:
/// prefiltering its `k`-truncated scan would change the fused answer.
pub(super) fn arm_with_bm25_prefilter(arm: &SearchMode, ids: &EligibleIds) -> SearchMode {
    if arm.bm25.is_some() {
        SearchMode {
            eligible_ids: Some(ids.clone()),
            ..arm.clone()
        }
    } else {
        arm.clone()
    }
}

/// Whether a filter's left operand is a full-text search call.
pub(super) fn is_search_filter(filter: &IRFilter) -> bool {
    matches!(
        &filter.left,
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }
    )
}

/// Whether `filter` is a full-text search call compared to `true`: the one
/// search shape a scan answers, as membership in the call's matches.
pub(crate) fn is_positive_search_filter(filter: &IRFilter) -> bool {
    is_search_filter(filter)
        && filter.op == CompOp::Eq
        && matches!(filter.right, IRExpr::Literal(Literal::Bool(true)))
}

/// The full-text query of a scan's search filter; `None` for every other
/// filter. A search call in any other comparison, or one whose property or
/// query text does not resolve, is refused by the root and dependent scans alike.
pub(crate) fn search_filter_query(
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<Option<lance_index::scalar::FullTextSearchQuery>> {
    if !is_search_filter(filter) {
        return Ok(None);
    }
    is_positive_search_filter(filter)
        .then(|| build_fts_query(&filter.left, params))
        .flatten()
        .map(Some)
        .ok_or_else(|| {
            OmniError::manifest(format!(
                "unsupported search filter `{filter}`: a scan answers search(), fuzzy() and \
                 match_text() only as a bare call or compared to true, over a property and a \
                 string query"
            ))
        })
}

/// The columns the plan's scan projection names for one bound variable
/// (`projection_pushdown`); a scan with no projection reads every
/// non-blob column.
#[derive(Debug, Clone)]
pub(super) struct NeededColumns(pub(super) HashSet<String>);

/// Map traversed edge types, including AntiJoin inner pipelines, to endpoints.
/// Scoping the graph-index build to these types avoids scanning unrelated edges.
pub(crate) fn referenced_edge_types(
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

pub(super) fn collect_referenced_edge_names(
    pipeline: &[IROp],
    out: &mut std::collections::BTreeSet<String>,
) {
    for op in pipeline {
        match op {
            IROp::Expand { edge_type, .. } => {
                out.insert(edge_type.clone());
            }
            IROp::AntiJoin { inner, .. } => collect_referenced_edge_names(inner, out),
            IROp::NodeScan { .. } | IROp::Filter(_) => {}
        }
    }
}

/// Per-rung multiplier of the probe ladder (20 → 80 → 320 → none).
pub(super) const ANN_PROBE_ESCALATION_FACTOR: usize = 4;

/// Per-rung multiplier and ceiling of the query-level overfetch loop
/// (`k` → 4k → 16k, then one exact pass over the whole type) for a
/// standalone `nearest` cut short of `limit` by an operator above a full scan.
pub(super) const ANN_OVERFETCH_STEP: usize = 4;

pub(super) const ANN_OVERFETCH_MAX_FACTOR: usize = 16;

/// The next rung of the probe ladder after `current` starved a scan: ×4, or
/// no cap once the next rung would cover the ranked partitions anyway.
pub(super) fn next_probe_budget(current: usize, partitions_ranked: usize) -> Option<usize> {
    let next = current.saturating_mul(ANN_PROBE_ESCALATION_FACTOR);
    (next < partitions_ranked).then_some(next)
}

/// One stop decision of the probe ladder in `execute_node_scan`, pure so
/// every arm is unit-tested (`ann_probe_budget_tests`): the loop runs the
/// scan, derives the inputs once, and dispatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LadderStep {
    /// The batches are final.
    Stop,
    /// The IVF scan holds prefilter-admitted rows at `_distance = +inf`:
    /// every matching row is here but out of `nearest` order. Rescan once as
    /// the flat exact kNN over the admitted rows, uncapped.
    FlatRescan,
    /// Rescan with no cap: the ladder's last rung (`summary_missing:
    /// false`), or the fail-closed rescan of a capped short scan whose
    /// execution summary lacks the partition counters (`true`).
    RescanUncapped { summary_missing: bool },
    /// Rescan under this cap, the next rung.
    Rescan(usize),
}

/// The ladder's decision after one nearest scan under `maximum` (`None` =
/// uncapped) returned `rows` of the `k` asked for. `summary` is Lance's
/// `(partitions_searched, partitions_ranked)`, each absent on a flat plan.
#[allow(clippy::too_many_arguments)]
pub(super) fn ladder_step(
    rows: usize,
    k: usize,
    known_matches: Option<usize>,
    dataset_rows: Option<u64>,
    has_infinite_distance: bool,
    summary: Option<(Option<u64>, Option<u64>)>,
    maximum: Option<usize>,
    last_rung: Option<(usize, u64)>,
) -> LadderStep {
    if has_infinite_distance {
        return LadderStep::FlatRescan;
    }
    let Some(maximum) = maximum else {
        return LadderStep::Stop;
    };
    if rows >= k {
        return LadderStep::Stop;
    }
    if known_matches.is_some_and(|matches| rows >= matches) {
        return LadderStep::Stop;
    }
    if dataset_rows.is_some_and(|count| rows as u64 >= count) {
        return LadderStep::Stop;
    }
    let (searched, ranked) = match summary {
        Some((None, None)) => return LadderStep::Stop,
        Some((Some(searched), Some(ranked))) => (searched, ranked),
        Some((Some(_), None)) | Some((None, Some(_))) | None => {
            return LadderStep::RescanUncapped {
                summary_missing: true,
            };
        }
    };
    if searched >= ranked {
        return LadderStep::Stop;
    }
    if last_rung == Some((rows, searched)) {
        return LadderStep::Stop;
    }
    match next_probe_budget(maximum, usize::try_from(ranked).unwrap_or(usize::MAX)) {
        Some(next) => LadderStep::Rescan(next),
        None => LadderStep::RescanUncapped {
            summary_missing: false,
        },
    }
}

/// Whether any row of a nearest scan carries `_distance = +inf`: Lance's
/// marker for a prefilter-admitted row its partition search did not reach
/// (the late search's match-count stop, `lance/src/io/exec/knn.rs`).
pub(super) fn batches_hold_infinite_distance(batches: &[RecordBatch]) -> bool {
    batches.iter().any(|batch| {
        batch
            .column_by_name("_distance")
            .and_then(|column| column.as_any().downcast_ref::<Float32Array>())
            .is_some_and(|distances| distances.iter().flatten().any(f32::is_infinite))
    })
}

#[cfg(test)]
mod ann_probe_budget_tests {
    use super::{
        LadderStep, NearestScanReport, OverfetchRung, SearchMode, ladder_step, next_overfetch_rung,
        next_probe_budget,
    };

    const IVF_SHORT: Option<(Option<u64>, Option<u64>)> = Some((Some(1), Some(1_000)));

    /// Rust test: no `.gqt` fixture trains an IVF index, so no case carries a probe cap.
    #[test]
    fn with_nearest_k_replaces_k_and_seeds_the_probe_cap() {
        let mode = SearchMode {
            nearest: Some(("d".into(), "embedding".into(), vec![0.5], 10)),
            ann_probe_budget: Some(7),
            ..Default::default()
        };
        let wider = mode.with_nearest_k(40, Some(28));
        assert_eq!(
            wider.nearest,
            Some(("d".into(), "embedding".into(), vec![0.5], 40))
        );
        assert_eq!(wider.ann_probe_budget, Some(28));
        let uncapped = mode.with_nearest_k(40, None);
        assert_eq!(uncapped.ann_probe_budget, None);
    }

    /// Rust test: no `.gqt` fixture trains an IVF index, so no case reaches a wider rung.
    #[test]
    fn overfetch_multiplies_then_runs_one_exact_pass() {
        let scan = |k: usize, maximum: Option<usize>, dataset_rows: u64| NearestScanReport {
            rows: k,
            k,
            maximum_nprobes: maximum,
            exhausted: false,
            dataset_rows,
        };
        assert_eq!(
            next_overfetch_rung(1, 10, scan(10, Some(20), 2_000)),
            Some(OverfetchRung::Wider {
                factor: 4,
                k: 40,
                maximum: Some(20)
            })
        );
        assert_eq!(
            next_overfetch_rung(4, 10, scan(40, None, 2_000)),
            Some(OverfetchRung::Wider {
                factor: 16,
                k: 160,
                maximum: None
            })
        );
        assert_eq!(
            next_overfetch_rung(16, 10, scan(160, Some(80), 2_000)),
            Some(OverfetchRung::Exact { k: 2_000 }),
            "past the ceiling the exact pass asks for the whole type"
        );
        assert_eq!(
            next_overfetch_rung(4, 10, scan(40, Some(80), 100)),
            Some(OverfetchRung::Exact { k: 100 }),
            "a rung that would ask for the whole type anyway is the exact pass"
        );
        assert_eq!(
            next_overfetch_rung(1, 160, scan(160, Some(20), 160)),
            None,
            "a full scan that asked for exactly the whole type returned every row"
        );
    }

    /// Rust test: no `.gqt` fixture trains an IVF index, so no case ranks partitions.
    #[test]
    fn probe_ladder_multiplies_then_uncaps() {
        assert_eq!(next_probe_budget(20, 1_000), Some(80));
        assert_eq!(next_probe_budget(80, 1_000), Some(320));
        assert_eq!(next_probe_budget(320, 1_000), None);
        assert_eq!(next_probe_budget(20, 60), None);
        assert_eq!(next_probe_budget(1, 100), Some(4));
    }

    /// Rust test: an infinite `_distance` needs a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_rescans_flat_on_an_infinite_distance_before_every_other_stop() {
        assert_eq!(
            ladder_step(10, 10, Some(10), Some(10), true, IVF_SHORT, None, None),
            LadderStep::FlatRescan
        );
        assert_eq!(
            ladder_step(3, 10, None, None, true, Some((None, None)), Some(20), None),
            LadderStep::FlatRescan
        );
    }

    /// Rust test: a short IVF scan needs a trained index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_without_a_cap() {
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, None, None),
            LadderStep::Stop
        );
    }

    /// Rust test: a probe-capped scan needs a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_when_the_scan_is_full() {
        assert_eq!(
            ladder_step(10, 10, None, None, false, IVF_SHORT, Some(20), None),
            LadderStep::Stop
        );
    }

    /// Rust test: a probe-capped scan needs a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_when_every_known_match_is_here() {
        assert_eq!(
            ladder_step(5, 10, Some(5), None, false, IVF_SHORT, Some(20), None),
            LadderStep::Stop
        );
        assert_eq!(
            ladder_step(4, 10, Some(5), None, false, IVF_SHORT, Some(20), None),
            LadderStep::Rescan(80)
        );
    }

    /// Rust test: a probe-capped scan needs a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_when_the_scan_holds_the_whole_dataset() {
        assert_eq!(
            ladder_step(7, 10, None, Some(7), false, IVF_SHORT, Some(20), None),
            LadderStep::Stop
        );
        assert_eq!(
            ladder_step(7, 10, None, Some(8), false, IVF_SHORT, Some(20), None),
            LadderStep::Rescan(80)
        );
    }

    /// Rust test: Lance's partition counters are scan metrics no `.gqt` expectation reads.
    #[test]
    fn ladder_treats_a_summary_with_neither_counter_as_a_flat_scan() {
        assert_eq!(
            ladder_step(3, 10, None, None, false, Some((None, None)), Some(20), None),
            LadderStep::Stop
        );
    }

    /// Rust test: a summary missing one counter is a Lance fault no `.gqt` fixture produces.
    #[test]
    fn ladder_fails_closed_without_both_counters() {
        for summary in [None, Some((Some(1), None)), Some((None, Some(8)))] {
            assert_eq!(
                ladder_step(3, 10, None, None, false, summary, Some(20), None),
                LadderStep::RescanUncapped {
                    summary_missing: true
                },
                "summary {summary:?}"
            );
        }
    }

    /// Rust test: partition counts need a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_when_every_ranked_partition_was_searched() {
        for summary in [Some((Some(8), Some(8))), Some((Some(9), Some(8)))] {
            assert_eq!(
                ladder_step(3, 10, None, None, false, summary, Some(20), None),
                LadderStep::Stop,
                "summary {summary:?}"
            );
        }
    }

    /// Rust test: two rungs of one scan need a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_stops_when_widening_changed_nothing() {
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, Some(80), Some((3, 1))),
            LadderStep::Stop
        );
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, Some(80), Some((3, 0))),
            LadderStep::Rescan(320)
        );
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, Some(80), Some((2, 1))),
            LadderStep::Rescan(320)
        );
    }

    /// Rust test: the rung sequence needs a trained IVF index; no `.gqt` fixture has one.
    #[test]
    fn ladder_climbs_then_uncaps() {
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, Some(20), None),
            LadderStep::Rescan(80)
        );
        assert_eq!(
            ladder_step(3, 10, None, None, false, IVF_SHORT, Some(320), None),
            LadderStep::RescanUncapped {
                summary_missing: false
            }
        );
        let narrow_index = Some((Some(1), Some(60)));
        assert_eq!(
            ladder_step(3, 10, None, None, false, narrow_index, Some(20), None),
            LadderStep::RescanUncapped {
                summary_missing: false
            }
        );
    }
}

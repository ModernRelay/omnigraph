//! The run-time side of a ranked scan: the `SearchMode` one `ScanExec` runs
//! under, built per scan node from the plan's `RankedAccess` and the bound
//! values; the per-pass `Pass` the overfetch ladder and the prefilter gates
//! write; the gates and probe ladders themselves (v2's copy of v1's, phase 4;
//! the frozen `exec/query.rs` stays upstream's bytes).

use omnigraph_planner::{
    GatePolicy, Hop, NodeId, OverfetchRung, Prefilter, PrefilterMode, RankKind,
};

use super::*;

/// How one scan's ranking runs: what the plan's `RankedAccess` asks for,
/// with the bound query value and this pass's widening and gate verdicts.
#[derive(Debug, Default, Clone)]
pub(super) struct SearchMode {
    /// Vector ANN search on the scan's binding.
    pub(super) nearest: Option<NearestTarget>,
    /// Maximum number of IVF payload partitions a nearest scan may search,
    /// per index delta; `None` is uncapped. The scan-site ladder in
    /// `execute_node_scan` widens a maximum that starves the scan.
    pub(super) ann_probe_budget: Option<usize>,
    /// The nearest scan runs flat (`use_index(false)`, every row scored): the
    /// overfetch loop's exact pass.
    pub(super) nearest_exact: bool,
    /// The nearest prefilter gate proved the answer empty: no node of the
    /// ranked type satisfies an Expand's first hop. Read off the `Pass` by
    /// `Lowering::search_mode`; an RRF arm's pass never carries it.
    pub(super) answer_proven_empty: bool,
    /// BM25 full-text search on the scan's binding.
    pub(super) bm25: Option<Bm25Target>,
    /// The set a gate ANDs into the ranked scan as `id IN (...)`, read off the
    /// `Pass` by `Lowering::search_mode`; the arms an `rrf()` prefilter feeds
    /// are the plan's `Prefilter.feeds`.
    pub(super) eligible_ids: Option<EligibleIds>,
}

/// `nearest($v.property, q)` as one scan runs it: the bound query vector and
/// the candidates asked for.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct NearestTarget {
    pub(super) property: String,
    pub(super) vector: Vec<f32>,
    pub(super) k: usize,
}

/// `bm25($v.property, q)` as one scan runs it: the bound query text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Bm25Target {
    pub(super) property: String,
    pub(super) text: String,
}

/// The overfetch ladder's rung for one nearest scan: the candidates it asks
/// for, the probe cap it runs under, and whether it runs flat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NearestRung {
    pub(super) k: usize,
    pub(super) maximum: Option<usize>,
    pub(super) exact: bool,
}

/// What one pass of the tree runs its ranked scans under, keyed by the scan's
/// node id: the ladder's rung, the gate's eligible set, the proven-empty
/// verdict. The plan says what is ranked; this says how this pass runs it.
#[derive(Debug, Default, Clone)]
pub(super) struct Pass {
    nearest: HashMap<NodeId, NearestRung>,
    eligible: HashMap<NodeId, EligibleIds>,
    proven_empty: HashSet<NodeId>,
}

impl Pass {
    /// This pass asking scan `id` for `k` candidates under the probe cap
    /// `maximum` (`None` = uncapped). The cap is the rung that filled the
    /// previous pass's scan, so the rerun does not re-climb from the base cap.
    pub(super) fn with_nearest_k(&self, id: NodeId, k: usize, maximum: Option<usize>) -> Self {
        let mut pass = self.clone();
        pass.nearest.insert(
            id,
            NearestRung {
                k,
                maximum,
                exact: false,
            },
        );
        pass
    }

    /// This pass running scan `id` flat over `k` = every live row of the
    /// type: the overfetch loop's exact pass.
    pub(super) fn with_exact_nearest(&self, id: NodeId, k: usize) -> Self {
        let mut pass = self.clone();
        pass.nearest.insert(
            id,
            NearestRung {
                k,
                maximum: None,
                exact: true,
            },
        );
        pass
    }

    pub(super) fn prefiltered(mut self, id: NodeId, ids: EligibleIds) -> Self {
        self.eligible.insert(id, ids);
        self
    }

    pub(super) fn proven_empty(mut self, id: NodeId) -> Self {
        self.proven_empty.insert(id);
        self
    }

    pub(super) fn rung(&self, id: NodeId) -> Option<NearestRung> {
        self.nearest.get(&id).copied()
    }

    pub(super) fn eligible(&self, id: NodeId) -> Option<&EligibleIds> {
        self.eligible.get(&id)
    }

    pub(super) fn is_proven_empty(&self, id: NodeId) -> bool {
        self.proven_empty.contains(&id)
    }

    /// Whether any scan of this pass was proven empty: the pipeline over it
    /// feeds nothing.
    pub(super) fn answer_proven_empty(&self) -> bool {
        !self.proven_empty.is_empty()
    }
}

/// What the `rrf` gate reads of one arm of an `rrf()`: the index it ranks
/// with and the ranked property, whose FTS coverage the gate checks.
#[derive(Debug, Clone, Copy)]
pub(super) struct ArmTarget<'a> {
    pub(super) kind: RankKind,
    pub(super) property: &'a str,
}

/// Shared eligible-id set, `Debug`-opaque so a logged `SearchMode` prints the
/// cardinality instead of up to `GatePolicy::max_ids` id strings.
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

/// The next pass of the overfetch ladder: the declared rung it takes (the
/// report's `rung`, `1` for the first rerun) with that rung's run-time values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NextPass {
    pub(super) rung: usize,
    pub(super) step: PassStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PassStep {
    /// Ask for `k` candidates under the cap that filled the previous pass.
    Wider { k: usize, maximum: Option<usize> },
    /// The exact pass: every live row of the type, scored flat, no probe cap.
    Exact { k: usize },
}

/// The first rung of the declared `ladder` at or after `from` (the rungs
/// before it were taken or skipped) that can still widen the scan: a `Wider`
/// rung whose `k` is below the type's live rows, else the `Exact` rung when
/// the scan asked for fewer rows than the type holds. Lance returns at most
/// `k` candidates regardless of probe width. `None` when the previous scan
/// already requested every live row or the ladder is spent.
pub(super) fn next_overfetch_rung(
    ladder: &[OverfetchRung],
    from: usize,
    scan: NearestScanReport,
) -> Option<NextPass> {
    let whole = usize::try_from(scan.dataset_rows).unwrap_or(usize::MAX);
    for (index, rung) in ladder.iter().enumerate().skip(from) {
        let step = match *rung {
            OverfetchRung::Wider { k, .. } if k < whole => PassStep::Wider {
                k,
                maximum: scan.maximum_nprobes,
            },
            OverfetchRung::Wider { .. } => continue,
            OverfetchRung::Exact if whole > scan.k => PassStep::Exact { k: whole },
            OverfetchRung::Exact => return None,
        };
        return Some(NextPass {
            rung: index + 1,
            step,
        });
    }
    None
}

impl SearchMode {
    /// The eligible-id set a prefilter gate chose for this scan.
    pub(super) fn eligible_ids(&self) -> Option<&[String]> {
        self.eligible_ids.as_ref().map(|ids| ids.0.as_slice())
    }
}

/// `rrf(a, b, k)` as the fusion runs it: the rank constant and the fused rows.
#[derive(Debug, Clone, Copy)]
pub(super) struct RrfMode {
    pub(super) k: u32,
    pub(super) limit: usize,
}

/// Resolve a nearest query vector, embedding string inputs with the property's
/// recorded model. Explicit vectors do not require an embedding client.
pub(super) async fn resolve_nearest_query_vec(
    catalog: &Catalog,
    type_name: &str,
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
                nearest_property_dim_and_model(catalog, type_name, property)?;
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
    catalog: &Catalog,
    type_name: &str,
    property: &str,
) -> Result<(usize, Option<String>)> {
    let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() scan resolved unknown node type '{type_name}'"
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

/// The rrf prefilter gate: decide, before the arms run, between two ANSWER-IDENTICAL
/// plans — prefilter (the uncapped bm25 arms rank only the traversal's
/// eligible ids) and postfilter (the uncapped corpus-wide arms, v0.9 rrf
/// semantics). The plan declares the pre-pass (`RankFuse.prefilter`: the
/// ranked type, the required first hops, the scans it feeds) and the policy
/// (`Assumptions.gate_policy`); the gate reads the store and decides.
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
///   shape the planner's `prefilter` admits is an instance;
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
    context: &EngineContext<'_>,
    arms: [ArmTarget<'_>; 2],
    prefilter: &Prefilter,
    policy: GatePolicy,
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

    if policy.mode == PrefilterMode::ForcePostfilter {
        fall_back(RrfGateFallback::Forced, true, None, None);
        return None;
    }
    let forced = policy.mode == PrefilterMode::ForcePrefilter;
    if !prefilter.admits() {
        fall_back(RrfGateFallback::Shape, forced, None, None);
        return None;
    }
    let bm25_props: Vec<&str> = arms
        .iter()
        .filter(|arm| arm.kind == RankKind::Bm25)
        .map(|arm| arm.property)
        .collect();
    let ranked_type = prefilter.ranked_type.as_str();
    let node_key = format!("node:{ranked_type}");
    let snapshot = context.snapshot;
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
    let (mut ids, eligible_count) = match adjacency_eligible_ids(
        &context.graph_index,
        context.catalog,
        ranked_type,
        &prefilter.hops,
        corpus,
        forced,
        policy,
    )
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
    hops: &[Hop],
    corpus: u64,
    forced: bool,
    policy: GatePolicy,
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
    )> = Vec::with_capacity(hops.len());
    for Hop {
        edge_type,
        direction,
    } in hops
    {
        let Some(edge_def) = catalog.edge_types.get(edge_type) else {
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
        let ratio_ok = corpus > 0 && (eligible_count as f64) <= policy.ratio * (corpus as f64);
        let cap_ok = eligible_count <= policy.max_ids;
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
/// eligible-id superset into its scan. The plan declares the pre-pass
/// (`RankedAccess.prefilter`) and the policy; the gate reads the store.
pub(super) async fn nearest_prefilter_gate(
    context: &EngineContext<'_>,
    prefilter: &Prefilter,
    policy: GatePolicy,
) -> NearestGatePlan {
    let forced = policy.mode != PrefilterMode::Auto;
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
    if policy.mode == PrefilterMode::ForcePostfilter {
        fall_back(RrfGateFallback::Forced, None, None);
        return NearestGatePlan::Postfilter;
    }
    if !prefilter.admits() {
        fall_back(RrfGateFallback::Shape, None, None);
        return NearestGatePlan::Postfilter;
    }
    let ranked_type = prefilter.ranked_type.as_str();
    let node_key = format!("node:{ranked_type}");
    let Some(node_entry) = context.snapshot.dataset(&node_key) else {
        fall_back(RrfGateFallback::Shape, None, None);
        return NearestGatePlan::Postfilter;
    };
    let corpus = node_entry.entity_count;
    match adjacency_eligible_ids(
        &context.graph_index,
        context.catalog,
        ranked_type,
        &prefilter.hops,
        corpus,
        forced,
        policy,
    )
    .await
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
        LadderStep, NearestRung, NearestScanReport, NextPass, OverfetchRung, Pass, PassStep,
        ladder_step, next_overfetch_rung, next_probe_budget,
    };

    const IVF_SHORT: Option<(Option<u64>, Option<u64>)> = Some((Some(1), Some(1_000)));

    /// Rust test: no `.gqt` fixture trains an IVF index, so no case carries a probe cap.
    #[test]
    fn with_nearest_k_replaces_k_and_seeds_the_probe_cap() {
        let pass = Pass::default();
        assert_eq!(pass.rung(4), None);
        let wider = pass.with_nearest_k(4, 40, Some(28));
        assert_eq!(
            wider.rung(4),
            Some(NearestRung {
                k: 40,
                maximum: Some(28),
                exact: false,
            })
        );
        assert_eq!(
            wider.rung(5),
            None,
            "the rung is the scan's, not the pass's"
        );
        let uncapped = wider.with_nearest_k(4, 160, None);
        assert_eq!(uncapped.rung(4).map(|rung| rung.maximum), Some(None));
        let exact = uncapped.with_exact_nearest(4, 2_000);
        assert_eq!(
            exact.rung(4),
            Some(NearestRung {
                k: 2_000,
                maximum: None,
                exact: true,
            })
        );
    }

    /// Rust test: no `.gqt` fixture trains an IVF index, so no case reaches a wider rung.
    #[test]
    fn overfetch_takes_the_declared_rungs_in_order_then_the_exact_pass() {
        let scan = |k: usize, maximum: Option<usize>, dataset_rows: u64| NearestScanReport {
            rows: k,
            k,
            maximum_nprobes: maximum,
            exhausted: false,
            dataset_rows,
        };
        let ladder = OverfetchRung::ladder(10);
        assert_eq!(
            ladder,
            [
                OverfetchRung::Wider { factor: 4, k: 40 },
                OverfetchRung::Wider { factor: 16, k: 160 },
                OverfetchRung::Exact,
            ]
        );
        assert_eq!(
            next_overfetch_rung(&ladder, 0, scan(10, Some(20), 2_000)),
            Some(NextPass {
                rung: 1,
                step: PassStep::Wider {
                    k: 40,
                    maximum: Some(20)
                }
            })
        );
        assert_eq!(
            next_overfetch_rung(&ladder, 1, scan(40, None, 2_000)),
            Some(NextPass {
                rung: 2,
                step: PassStep::Wider {
                    k: 160,
                    maximum: None
                }
            })
        );
        assert_eq!(
            next_overfetch_rung(&ladder, 2, scan(160, Some(80), 2_000)),
            Some(NextPass {
                rung: 3,
                step: PassStep::Exact { k: 2_000 }
            }),
            "past the ceiling the exact pass asks for the whole type"
        );
        assert_eq!(
            next_overfetch_rung(&ladder, 1, scan(40, Some(80), 100)),
            Some(NextPass {
                rung: 3,
                step: PassStep::Exact { k: 100 }
            }),
            "a rung that would ask for the whole type anyway is skipped for the exact pass"
        );
        assert_eq!(
            next_overfetch_rung(&OverfetchRung::ladder(160), 0, scan(160, Some(20), 160)),
            None,
            "a full scan that asked for exactly the whole type returned every row"
        );
        assert_eq!(
            next_overfetch_rung(&ladder, 3, scan(2_000, None, 2_000)),
            None,
            "a spent ladder reruns nothing"
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

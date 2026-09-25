//! Session and compiled-query read entry points. The `engine` setting selects
//! v1 (`super::execute_query`) or v2 (`crate::engine::execute_query`), each
//! with its own graph-index handle and embedding resolver. Explain requests
//! always describe v2.

use super::*;
use crate::engine;
use crate::runtime_cache::CompiledRead;
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::query::ast::{BinaryOp, CompOp, Literal};
use omnigraph_compiler::settings::Engine;

/// The tail of every refusal of a query v1 cannot run and v2 can, the
/// construct named in front (RFC 2026-09-24-shared-expression-model, "Engine
/// setting").
const V1_SWITCHES: &str = " are not supported on engine v1; engine v2 runs them: add \"set \
                           engine = v2;\" before the query, or start the server with \
                           OMNIGRAPH_ENGINE=v2";

/// What the v1 door refuses in a compiled read: each variant names the
/// construct in front of `V1_SWITCHES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V1Refusal {
    CompoundPredicate,
    ReturnOrOrderComparison,
    FilterShape,
    OrderKey,
}

impl V1Refusal {
    fn construct(self) -> &'static str {
        match self {
            Self::CompoundPredicate => "compound predicates (and, or, not, is null)",
            Self::ReturnOrOrderComparison => "comparisons in return or order",
            Self::FilterShape => "filters other than one comparison or search call",
            Self::OrderKey => {
                "order keys other than a property, a system field, an alias or the leading \
                 search key"
            }
        }
    }

    fn error(self) -> OmniError {
        CompilerError::Plan(format!("{}{V1_SWITCHES}", self.construct())).into()
    }
}

/// The first shape of `ir` engine v1 does not evaluate: a filter beyond one
/// comparison over property, literal and parameter operands or the search call,
/// a Boolean node in `return` or `order`, or an order key of another shape.
fn v1_refusal(ir: &QueryIR) -> Option<V1Refusal> {
    if let Some(refusal) = pipeline_refusal(&ir.pipeline) {
        return Some(refusal);
    }
    if ir
        .return_exprs
        .iter()
        .any(|projection| has_boolean_node(&projection.expr))
    {
        return Some(V1Refusal::ReturnOrOrderComparison);
    }
    for (index, key) in ir.order_by.iter().enumerate() {
        if has_boolean_node(&key.expr) {
            return Some(V1Refusal::ReturnOrOrderComparison);
        }
        let accepted = match &key.expr {
            IRExpr::PropAccess { .. } | IRExpr::AliasRef(_) => true,
            IRExpr::Nearest { .. } | IRExpr::Bm25 { .. } | IRExpr::Rrf { .. } => index == 0,
            _ => false,
        };
        if !accepted {
            return Some(V1Refusal::OrderKey);
        }
    }
    None
}

fn pipeline_refusal(pipeline: &[IROp]) -> Option<V1Refusal> {
    pipeline.iter().find_map(|op| match op {
        IROp::NodeScan { filters, .. }
        | IROp::Expand {
            dst_filters: filters,
            ..
        } => filters.iter().find_map(filter_refusal),
        IROp::Filter(filter) => filter_refusal(filter),
        IROp::AntiJoin { inner, .. } => pipeline_refusal(inner),
    })
}

fn filter_refusal(filter: &IRExpr) -> Option<V1Refusal> {
    let v1_operand = |expr: &IRExpr| {
        matches!(
            expr,
            IRExpr::PropAccess { .. } | IRExpr::Literal(_) | IRExpr::Param(_)
        )
    };
    match filter {
        IRExpr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            ..
        }
        | IRExpr::Not(_)
        | IRExpr::IsNull { .. } => Some(V1Refusal::CompoundPredicate),
        IRExpr::Binary {
            left,
            op: BinaryOp::Compare(op),
            right,
        } => {
            let search_call = matches!(
                **left,
                IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }
            ) && *op == CompOp::Eq
                && **right == IRExpr::Literal(Literal::Bool(true));
            (!search_call && !(v1_operand(left) && v1_operand(right)))
                .then_some(V1Refusal::FilterShape)
        }
        _ => Some(V1Refusal::FilterShape),
    }
}

fn has_boolean_node(expr: &IRExpr) -> bool {
    match expr {
        IRExpr::Binary { .. } | IRExpr::Not(_) | IRExpr::IsNull { .. } => true,
        IRExpr::Aggregate { arg, .. } | IRExpr::Nearest { query: arg, .. } => has_boolean_node(arg),
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => has_boolean_node(field) || has_boolean_node(query),
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            has_boolean_node(field)
                || has_boolean_node(query)
                || max_edits.as_deref().is_some_and(has_boolean_node)
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            has_boolean_node(primary)
                || has_boolean_node(secondary)
                || k.as_deref().is_some_and(has_boolean_node)
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Variable(_)
        | IRExpr::Param(_)
        | IRExpr::Literal(_)
        | IRExpr::AliasRef(_) => false,
    }
}

/// Where a route builds its CSR graph index when the query traverses:
/// the cross-query `RuntimeCache` entry of a live target, or a build against
/// a historical snapshot.
enum IndexSource<'a> {
    Cached(&'a crate::db::ResolvedTarget),
    Direct,
}

impl Session {
    /// Run a named query against an explicit branch or snapshot target under
    /// this session's settings and the source's `set` prefix.
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
    /// branch-owned head yet.
    ///
    /// The id comes from the same pinned snapshot as every data read — the
    /// value a caller passes to [`Self::mutate_as_with_expected_head`] for a
    /// read-then-write compare-and-swap.
    pub async fn query_with_head(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<(QueryResult, Option<String>)> {
        let settings = self.effective(query_source)?;
        let (resolved, catalog) = self.capture_read_view(target).await?;

        let compiled = self.compile_named_query(&catalog, query_source, query_name)?;
        let head = resolved.graph_commit_id.clone();
        if let CompiledRead::Explain(ir) = &compiled {
            let rows =
                engine::explain_rows(ir, params, &resolved.snapshot, &catalog, &settings).await?;
            return Ok((rows, head));
        }
        let result = self
            .execute_on_route(
                &settings,
                compiled.ir(),
                params,
                &resolved.snapshot,
                IndexSource::Cached(&resolved),
                &catalog,
            )
            .await?;
        Ok((result, head))
    }

    /// Run a named query against the graph at a historical snapshot version.
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
        let settings = self.effective(query_source)?;
        let (snapshot, catalog) = self.capture_historical_read_view(version).await?;

        let compiled = self.compile_named_query(&catalog, query_source, query_name)?;
        if let CompiledRead::Explain(ir) = &compiled {
            return engine::explain_rows(ir, params, &snapshot, &catalog, &settings).await;
        }
        self.execute_on_route(
            &settings,
            compiled.ir(),
            params,
            &snapshot,
            IndexSource::Direct,
            &catalog,
        )
        .await
    }

    /// Return a named query's v2 planner document without executing the query.
    ///
    /// Includes the logical and physical plans and optimizer passes, regardless
    /// of the selected execution engine; a traversal mode pinned on this
    /// session (`SessionSettings::with_traversal`) is the mode the document
    /// records. The source may declare the query or wrap it in `explain`. This
    /// document does not include a DataFusion tree.
    ///
    /// # Errors
    ///
    /// Returns an error if the target snapshot or its catalog cannot be resolved,
    /// the named read query cannot be parsed, type-checked or lowered, or the
    /// planner cannot build a plan from the query and supplied parameters.
    pub async fn explain_query(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<serde_json::Value> {
        let settings = self.effective(query_source)?;
        let (resolved, catalog) = self.capture_read_view(target).await?;
        let compiled = self.compile_named_query(&catalog, query_source, query_name)?;
        engine::explain_document(
            compiled.ir(),
            params,
            &catalog,
            &resolved.snapshot,
            &settings,
        )
        .await
    }

    /// The v2 inspection door of the GQT runner: [`Self::query`] answered as an
    /// `Executed`, the run with its own plan, explain and report.
    ///
    /// # Errors
    ///
    /// Beside the errors of [`Self::query`], refuses a source whose effective
    /// engine is not `v2`, and an `explain` statement, which runs nothing.
    #[doc(hidden)]
    pub async fn query_inspected(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<engine::Executed> {
        let settings = self.effective(query_source)?;
        if settings.engine() != Engine::V2 {
            return Err(OmniError::manifest(
                "the inspection door runs engine = v2 only",
            ));
        }
        let (resolved, catalog) = self.capture_read_view(target).await?;
        let CompiledRead::Query(ir) =
            self.compile_named_query(&catalog, query_source, query_name)?
        else {
            return Err(OmniError::manifest(
                "the inspection door runs no `explain` statement",
            ));
        };
        let traverses = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        let graph_index = if traverses {
            engine::GraphIndexHandle::cached(
                Arc::clone(&**self),
                resolved.clone(),
                engine::referenced_edge_types(&ir.pipeline, &catalog),
                catalog.system_columns,
            )
        } else {
            engine::GraphIndexHandle::none()
        };
        engine::execute_query_inspected(
            &ir,
            params,
            &resolved.snapshot,
            graph_index,
            &catalog,
            &engine::EmbeddingResolver::new(self.embedding_cell(), self.embedding_config_ref()),
            &settings,
        )
        .await
    }

    /// The replay door of the plan-replay tests: executes `bound`, a plan the
    /// inspection door returned and the test serialized and read back, with
    /// nothing else about its query: no session setting, no `QueryIR`. It
    /// builds the engine context `query_inspected` builds (the snapshot and
    /// catalog of `target`, a graph index scoped to the plan's `Expand`s),
    /// refuses a snapshot whose dataset versions are not the ones the plan's
    /// scans, counts and traversals pinned, and calls the same `execute`.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::query`] on the target, a snapshot the plan did
    /// not pin, and every run-time error of the plan.
    #[doc(hidden)]
    pub async fn replay_bound_plan(
        &self,
        target: impl Into<ReadTarget>,
        bound: omnigraph_planner::BoundPlan,
    ) -> Result<engine::PlanRun> {
        let (resolved, catalog) = self.capture_read_view(target).await?;
        engine::plan_pins_snapshot(&bound.plan, &resolved.snapshot)?;
        let graph_index = if engine::plan_traverses(&bound.plan) {
            engine::GraphIndexHandle::cached(
                Arc::clone(&**self),
                resolved.clone(),
                engine::plan_edge_types(&bound.plan, &catalog),
                catalog.system_columns,
            )
        } else {
            engine::GraphIndexHandle::none()
        };
        let context = engine::EngineContext {
            snapshot: &resolved.snapshot,
            catalog: &catalog,
            graph_index: Arc::new(graph_index),
        };
        engine::execute(bound, &context).await
    }

    /// One compiled query on the route `settings.engine()` names. Each route
    /// builds its own lazy graph-index handle: an index-served query with no
    /// `AntiJoin` never builds the CSR on either.
    async fn execute_on_route(
        &self,
        settings: &SessionSettings,
        ir: &QueryIR,
        params: &ParamMap,
        snapshot: &Snapshot,
        index_source: IndexSource<'_>,
        catalog: &Arc<Catalog>,
    ) -> Result<QueryResult> {
        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        match settings.engine() {
            Engine::V1 => {
                if let Some(refusal) = v1_refusal(ir) {
                    return Err(refusal.error());
                }
                let graph_index = match (&index_source, needs_graph) {
                    (_, false) => GraphIndexHandle::none(),
                    (IndexSource::Cached(resolved), true) => GraphIndexHandle::cached(
                        self,
                        resolved,
                        referenced_edge_types(&ir.pipeline, catalog),
                        catalog.system_columns,
                    ),
                    (IndexSource::Direct, true) => GraphIndexHandle::direct(
                        snapshot,
                        referenced_edge_types(&ir.pipeline, catalog),
                        catalog.system_columns,
                    ),
                };
                execute_query(
                    ir,
                    params,
                    snapshot,
                    &graph_index,
                    catalog,
                    &EmbeddingResolver {
                        cell: self.embedding_cell(),
                        config: self.embedding_config_ref(),
                    },
                    settings,
                )
                .await
            }
            Engine::V2 => {
                let graph_index = match (&index_source, needs_graph) {
                    (_, false) => engine::GraphIndexHandle::none(),
                    (IndexSource::Cached(resolved), true) => engine::GraphIndexHandle::cached(
                        Arc::clone(&**self),
                        (*resolved).clone(),
                        engine::referenced_edge_types(&ir.pipeline, catalog),
                        catalog.system_columns,
                    ),
                    (IndexSource::Direct, true) => engine::GraphIndexHandle::direct(
                        snapshot.clone(),
                        engine::referenced_edge_types(&ir.pipeline, catalog),
                        catalog.system_columns,
                    ),
                };
                engine::execute_query(
                    ir,
                    params,
                    snapshot,
                    graph_index,
                    catalog,
                    &engine::EmbeddingResolver::new(
                        self.embedding_cell(),
                        self.embedding_config_ref(),
                    ),
                    settings,
                )
                .await
            }
        }
    }
}

impl Omnigraph {
    /// Compile the read statement `query_name` of `query_source` (a declaration,
    /// or `explain` over one), cached in `ReadCaches::compiled_queries`; errors are
    /// never cached. A hit needs the memoized catalog `Arc` of the schema gate.
    fn compile_named_query(
        &self,
        catalog: &Arc<Catalog>,
        query_source: &str,
        query_name: &str,
    ) -> Result<CompiledRead> {
        let cache = &self.read_caches().compiled_queries;
        let key = crate::runtime_cache::CompiledQueryCache::key_for(query_source, query_name);
        if let Some(compiled) = cache.get(catalog, &key) {
            return Ok(compiled);
        }
        let statement = omnigraph_compiler::find_read_statement(query_source, query_name).map_err(
            |e| match e {
                // A compile diagnostic keeps its code, position and fix.
                omnigraph_compiler::RunInputError::Core(
                    query @ omnigraph_compiler::error::CompilerError::Query(_),
                ) => OmniError::Compiler(query),
                other => OmniError::manifest(other.to_string()),
            },
        )?;
        let type_ctx = typecheck_query(catalog, statement.decl())?;
        let ir = Arc::new(lower_query(catalog, statement.decl(), &type_ctx)?);
        let compiled = if statement.is_explain() {
            CompiledRead::Explain(ir)
        } else {
            CompiledRead::Query(ir)
        };
        crate::instrumentation::record_query_compile();
        cache.insert(catalog, key, compiled.clone());
        Ok(compiled)
    }
}

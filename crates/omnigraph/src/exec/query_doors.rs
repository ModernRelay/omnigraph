//! Session and compiled-query read entry points. Every read plans and runs
//! through `crate::engine::execute_query`; a test session may carry a
//! `ReadExecutor` that runs its reads instead (feature `test-util`).

use std::sync::Arc;

use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::error::CompilerError;
use omnigraph_compiler::ir::{IROp, ParamMap, QueryIR};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_compiler::{CheckedQuery, lower_query};

use crate::db::{Omnigraph, ReadTarget, Snapshot};
use crate::engine;
use crate::error::{OmniError, Result};
use crate::runtime_cache::{CompiledQuery, CompiledRead};
use crate::session::Session;

/// Where a route builds its CSR graph index when the query traverses:
/// the cross-query `RuntimeCache` entry of a live target, or a build against
/// a historical snapshot.
enum IndexSource<'a> {
    Cached(&'a crate::db::ResolvedTarget),
    Direct,
}

fn admit_wildcard_target(historical: bool, has_wildcard: bool) -> Result<()> {
    if historical && has_wildcard {
        return Err(CompilerError::Plan(
            "wildcard traversals are not supported on explicit historical targets; select explicit edge types".to_string(),
        ).into());
    }
    Ok(())
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
        admit_wildcard_target(
            matches!(&resolved.requested, ReadTarget::Snapshot(_)),
            compiled.ir().has_wildcard_traversal(),
        )?;
        let head = resolved.graph_commit_id.clone();
        if let CompiledRead::Explain(query) = &compiled {
            let rows = engine::explain_rows(query, params, &resolved.snapshot, &catalog, &settings)
                .await?;
            return Ok((rows, head));
        }
        let result = self
            .execute_on_route(
                &settings,
                compiled.query(),
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
        admit_wildcard_target(true, compiled.ir().has_wildcard_traversal())?;
        if let CompiledRead::Explain(query) = &compiled {
            return engine::explain_rows(query, params, &snapshot, &catalog, &settings).await;
        }
        self.execute_on_route(
            &settings,
            compiled.query(),
            params,
            &snapshot,
            IndexSource::Direct,
            &catalog,
        )
        .await
    }

    /// Return a named query's v2 planner document without executing the query.
    ///
    /// Includes the logical and physical plans and optimizer passes; a
    /// traversal mode pinned on this
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
        admit_wildcard_target(
            matches!(&resolved.requested, ReadTarget::Snapshot(_)),
            compiled.ir().has_wildcard_traversal(),
        )?;
        engine::explain_document(
            compiled.query(),
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
    /// Beside the errors of [`Self::query`], refuses an `explain` statement,
    /// which runs nothing.
    #[doc(hidden)]
    pub async fn query_inspected(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<engine::Executed> {
        let settings = self.effective(query_source)?;
        let (resolved, catalog) = self.capture_read_view(target).await?;
        let CompiledRead::Query(query) =
            self.compile_named_query(&catalog, query_source, query_name)?
        else {
            return Err(OmniError::manifest(
                "the inspection door runs no `explain` statement",
            ));
        };
        let ir = &query.ir;
        admit_wildcard_target(
            matches!(&resolved.requested, ReadTarget::Snapshot(_)),
            ir.has_wildcard_traversal(),
        )?;
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
            &query,
            params,
            &resolved.snapshot,
            graph_index,
            &catalog,
            &engine::EmbeddingResolver::new(self.embedding_cell(), self.embedding_config_ref()),
            &settings,
        )
        .await
    }

    /// The replay door of the plan-replay tests: accepts and executes the
    /// plan a replay envelope carries (an inspected run's bound plan with
    /// its query's source, name and accepted scope, serialized and read
    /// back), with no session setting. The envelope is decoded within the
    /// evidence byte limit, its versions checked first; the query is
    /// recompiled against `target`'s catalog and bound to the plan's own
    /// parameter values, and the plan is accepted again against the
    /// requirements derived from it. It builds the engine context
    /// `query_inspected` builds (the snapshot and catalog of `target`, a
    /// graph index scoped to the plan's `Expand`s), refuses a snapshot whose
    /// dataset versions are not the ones the plan's scans, counts and
    /// traversals pinned, and calls the same `execute`.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::query`] on the target; a replan-required
    /// conflict for an unsupported envelope version; a conflict for a
    /// snapshot the plan did not pin; a bad request for a malformed envelope
    /// or a plan that fails acceptance; every run-time error of the plan.
    #[doc(hidden)]
    pub async fn replay_bound_plan(
        &self,
        target: impl Into<ReadTarget>,
        envelope: &[u8],
    ) -> Result<engine::PlanRun> {
        let envelope = omnigraph_planner::decode_replay(
            envelope,
            omnigraph_planner::ValidationLimits::DEFAULT,
        )
        .map_err(engine::replay_refused)?;
        let (resolved, catalog) = self.capture_read_view(target).await?;
        let plan = &envelope.plan.plan;
        let has_wildcard = plan.assumptions().has_wildcard_traversal || plan.live().any(|(_, node)| {
            matches!(node, omnigraph_planner::PhysicalNode::Expand { edges, .. } if edges.is_wildcard())
        });
        admit_wildcard_target(
            matches!(&resolved.requested, ReadTarget::Snapshot(_)),
            has_wildcard,
        )?;
        engine::plan_pins_snapshot(plan, &resolved.snapshot)?;
        let CompiledRead::Query(query) =
            self.compile_named_query(&catalog, &envelope.query.source, &envelope.query.name)?
        else {
            return Err(OmniError::manifest(
                "a replay envelope names an `explain` statement, which runs nothing",
            ));
        };
        let accepted = engine::accept_replay(&query, envelope, &catalog)?;
        let plan = &accepted.bound().plan;
        let graph_index = if engine::plan_traverses(plan) {
            engine::GraphIndexHandle::cached(
                Arc::clone(&**self),
                resolved.clone(),
                engine::plan_edge_types(plan, &catalog),
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
        engine::execute(accepted, &context).await
    }

    /// One compiled query through the engine, or through the installed
    /// `ReadExecutor`. The lazy graph-index handle means an index-served query
    /// with no `AntiJoin` never builds the CSR.
    async fn execute_on_route(
        &self,
        settings: &SessionSettings,
        query: &CompiledQuery,
        params: &ParamMap,
        snapshot: &Snapshot,
        index_source: IndexSource<'_>,
        catalog: &Arc<Catalog>,
    ) -> Result<QueryResult> {
        let ir: &QueryIR = &query.ir;
        #[cfg(feature = "test-util")]
        if let Some(executor) = self.read_executor() {
            let request = omnigraph_catalog::read_executor::ReadRequest {
                ir,
                params,
                snapshot: snapshot.raw(),
                catalog,
                settings,
            };
            return executor.execute(request).await;
        }
        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
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
            query,
            params,
            snapshot,
            graph_index,
            catalog,
            &engine::EmbeddingResolver::new(self.embedding_cell(), self.embedding_config_ref()),
            settings,
        )
        .await
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
        let statement = omnigraph_compiler::find_read_statement(query_source, query_name)
            .map_err(super::query_lookup_error)?;
        let checked = CheckedQuery::check(catalog, statement.decl())?;
        let ir = Arc::new(lower_query(catalog, checked.decl(), checked.types())?);
        let query = CompiledQuery { ir, checked };
        let compiled = if statement.is_explain() {
            CompiledRead::Explain(query)
        } else {
            CompiledRead::Query(query)
        };
        crate::instrumentation::record_query_compile();
        cache.insert(catalog, key, compiled.clone());
        Ok(compiled)
    }
}

//! Session and compiled-query read entry points. The `engine` setting selects
//! v1 (`super::execute_query`) or v2 (`crate::engine::execute_query`), each
//! with its own graph-index handle and embedding resolver. Explain requests
//! always describe v2.

use super::*;
use crate::engine;
use crate::runtime_cache::CompiledRead;
use omnigraph_compiler::settings::Engine;

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
            settings.traversal(),
        )
        .await
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
        let statement = omnigraph_compiler::find_read_statement(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
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

//! The planner's view of a read query: the catalog and the pinned snapshot
//! behind `PlanSource`, the parameters behind the scanner's pushability
//! verdict, and the physical plan the engine's runner executes.

mod indexes;
mod scan_access;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use lance::dataset::statistics::DatasetStatisticsExt;
use lance::datatypes::Field;
use lance_file::version::ConcreteFileVersion;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, IROp, ParamMap, QueryIR};
use omnigraph_compiler::query::ast::Literal;
use omnigraph_compiler::settings::{RrfPlan, SessionSettings, Traversal};
use omnigraph_compiler::types::Direction;
use omnigraph_planner::{
    AdjacencyProof, Bounds, DatasetPin, Decision, EXPAND_INDEXED_MAX_FRONTIER_ENV,
    EXPAND_INDEXED_MAX_HOPS_ENV, ExpandStatistics, Explain, FragmentStat, GatePolicy, IndexFact,
    NodeTypeSpec, Operation, PhysicalPlan, PlanError, PlanSource, PrefilterMode, RouteOverride,
    SideId, TableRef, Unrouted,
};

use super::ResolvedParams;
use super::scan::ir_expr_to_df_expr;
use super::{fill_declared_params, validate_params};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::seams::{decide_seam, fail};

const KEY_WIDTH_BYTES: u64 = 8 + 8 + 32;

decide_seam! {
    /// Planning is about to open a query table to load its index metadata.
    /// Tests fail here to prove gathering fails rather than planning on an
    /// empty catalog.
    pub static QUERY_INDEX_FACTS_PRE_LOAD = ("query_index_facts.pre_load", Unreachable, [Fail]);
}

decide_seam! {
    /// The planner asked the source to split a filtered scan and the pinned
    /// table is not open yet. Tests fail here to prove the source's own error
    /// class reaches the caller.
    pub static QUERY_SCAN_ACCESS_PRE_TABLE_OPEN = ("query_scan_access.pre_table_open", Unreachable, [Fail]);
}

/// Max source-row frontier for which Expand uses the BTREE-indexed path.
/// Larger frontiers fall back to the in-memory CSR (dense / whole-graph). See
/// `docs/dev/execution.md`.
const DEFAULT_EXPAND_INDEXED_MAX_FRONTIER: u64 = 1024;

/// Max hop count for the indexed path (each hop is one indexed scan; very deep
/// traversals fan out toward whole-graph and are better served by CSR).
const DEFAULT_EXPAND_INDEXED_MAX_HOPS: u32 = 6;

/// The two indexed-path ceilings as the environment set them when the plan
/// was gathered, read once here and carried on every `Expand`'s cost inputs
/// and in the plan's assumptions.
#[derive(Debug, Clone, Copy)]
struct ExpandCaps {
    max_frontier: u64,
    max_hops: u32,
}

impl ExpandCaps {
    fn from_env() -> Self {
        Self {
            max_frontier: std::env::var(EXPAND_INDEXED_MAX_FRONTIER_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_FRONTIER),
            max_hops: std::env::var(EXPAND_INDEXED_MAX_HOPS_ENV)
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_HOPS),
        }
    }
}

/// The prefilter gates' policy as this process and session set it: the
/// `rrf_plan` setting and the two admission thresholds, read once here and
/// carried on the plan.
fn gate_policy(settings: &SessionSettings) -> GatePolicy {
    let defaults = GatePolicy::default();
    GatePolicy {
        mode: match settings.rrf_plan() {
            RrfPlan::Auto => PrefilterMode::Auto,
            RrfPlan::ForcePrefilter => PrefilterMode::ForcePrefilter,
            RrfPlan::ForcePostfilter => PrefilterMode::ForcePostfilter,
        },
        ratio: std::env::var("OMNIGRAPH_RRF_GATE_RATIO")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|r| r.is_finite() && *r >= 0.0)
            .unwrap_or(defaults.ratio),
        max_ids: std::env::var("OMNIGRAPH_RRF_GATE_MAX_IDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(defaults.max_ids),
    }
}

/// The one gathered view a read plans from, shared by the run and by its
/// explain: everything the planner reads, and the binding the run lowers with.
pub(crate) struct QuerySource<'a> {
    /// The compiled query with every constant of a filter position folded to
    /// its bound value (`engine::constant`); the planner reads no other form.
    pub ir: QueryIR,
    pub catalog: &'a Arc<Catalog>,
    pub snapshot: &'a Snapshot,
    pub params: ResolvedParams,
    pub settings: &'a SessionSettings,
    memory_limit: u64,
    gate_policy: GatePolicy,
    expand_caps: ExpandCaps,
    table_stats: HashMap<String, TableStatistics>,
    /// The error the source met answering the planner's last lookup, handed
    /// back to the caller in place of the planner's wrapper.
    source_failure: std::sync::Mutex<Option<OmniError>>,
}

struct TableStatistics {
    file_bytes: Option<u64>,
    column_bytes: HashMap<String, u64>,
    index_facts: Vec<IndexFact>,
}

impl<'a> QuerySource<'a> {
    /// The only I/O before planning: binds the parameters, reads the effective
    /// memory limit, the gate policy and the indexed-path ceilings once, and
    /// loads the Lance statistics the planner asks for.
    pub(crate) async fn gather(
        ir: &QueryIR,
        catalog: &'a Arc<Catalog>,
        snapshot: &'a Snapshot,
        params: &ParamMap,
        settings: &'a SessionSettings,
    ) -> Result<QuerySource<'a>> {
        let params = resolve_params(ir, params)?;
        let ir = super::constant::fold_query_constants(ir, params.shared())?;
        let table_stats = query_table_statistics(&ir, snapshot).await?;
        let mut source = QuerySource {
            ir,
            catalog,
            snapshot,
            params,
            settings,
            memory_limit: super::context::query_memory_limit(),
            gate_policy: gate_policy(settings),
            expand_caps: ExpandCaps::from_env(),
            table_stats,
            source_failure: std::sync::Mutex::new(None),
        };
        source
            .load_column_statistics(&Operation::Query(Box::new(source.ir.clone())))
            .await?;
        Ok(source)
    }

    /// The memory constants the planner declares against: the change-feed
    /// values, so one explain document reads the same on every operation, and
    /// the memory limit `gather` captured, which sizes the run's pool.
    pub(crate) fn bounds(&self) -> Bounds {
        Bounds {
            hydration_chunk_hard_bytes: 2 * crate::storage_layer::KEYED_WRITE_MAX_BYTES,
            key_width_bytes: KEY_WIDTH_BYTES,
            ordered_scan_memory_bytes: crate::table_store::ORDERED_SCAN_MEMORY_BYTES,
            ordered_scan_max_input_batch_bytes:
                crate::table_store::ORDERED_SCAN_MAX_INPUT_BATCH_BYTES,
            build_key_cap_rows: super::push::BUILD_KEY_CAP_ROWS as u64,
            query_memory_pool_bytes: self.memory_limit,
            late_materialization_only: false,
        }
    }

    fn take_source_failure(&self) -> Option<OmniError> {
        self.source_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    async fn load_column_statistics(&mut self, operation: &Operation) -> Result<()> {
        if self.table_stats.is_empty() {
            return Ok(());
        }
        let tables = omnigraph_planner::optimizer::column_statistics_needed(operation, self)
            .map_err(no_plan)?;
        for type_key in tables {
            let dataset = Arc::new(self.snapshot.open_lance_dataset(&type_key).await?);
            if dataset.manifest().data_storage_format.lance_file_format() == ConcreteFileVersion::V1
            {
                continue;
            }
            let fields = match dataset.calculate_data_stats().await {
                Ok(stats) => stats
                    .fields
                    .into_iter()
                    .map(|field| (field.id, field.bytes_on_disk))
                    .collect(),
                Err(error) => {
                    tracing::debug!(%error, %type_key, "column statistics unavailable; using manifest estimate");
                    continue;
                }
            };
            if let Some(stats) = self.table_stats.get_mut(&type_key) {
                for field in &dataset.schema().fields {
                    if let Some(size) = field_data_bytes(field, &fields) {
                        stats.column_bytes.insert(field.name.clone(), size);
                    }
                }
            }
        }
        Ok(())
    }
}

impl PlanSource for QuerySource<'_> {
    fn canonical_key_id(
        &self,
        type_key: &str,
        value: &IRExpr,
    ) -> std::result::Result<Option<String>, PlanError> {
        use omnigraph_compiler::{ExprType, ScalarType};
        let Some(node) = type_key
            .strip_prefix("node:")
            .and_then(|name| self.catalog.node_types.get(name))
        else {
            return Ok(None);
        };
        let Some([key]) = node.key.as_deref() else {
            return Ok(None);
        };
        if !node
            .properties
            .get(key)
            .is_some_and(|ty| ty.scalar == ScalarType::String && !ty.list)
            || !matches!(
                value.ty(),
                ExprType::Value {
                    scalar: ScalarType::String,
                    list: false,
                    ..
                }
            )
        {
            return Ok(None);
        }
        let literal = match value {
            IRExpr::Literal(value, _) => Some(value),
            IRExpr::Param(name, _) => self.params.shared().get(name),
            _ => None,
        };
        let Some(literal @ Literal::String(_)) = literal else {
            return Ok(None);
        };
        let array = super::typed_value::typed_literal_to_array(literal, value.ty(), 1)
            .map_err(|error| PlanError::Internal(error.to_string()))?;
        crate::loader::canonical_key_id(&[array], 0)
            .map_err(|error| PlanError::Internal(error.to_string()))
    }

    fn scan_runtime_input(
        &self,
        scan: &omnigraph_planner::ScanSpec,
    ) -> Option<omnigraph_planner::RuntimeInput> {
        scan.filter
            .as_ref()
            .is_some_and(|predicate| {
                predicate
                    .gq_filters()
                    .iter()
                    .any(super::search::is_search_filter)
            })
            .then_some(omnigraph_planner::RuntimeInput::SearchFilter)
    }

    fn index_split<'a>(
        &'a self,
        scan: &'a omnigraph_planner::ScanSpec,
    ) -> omnigraph_planner::IndexSplitFuture<'a> {
        Box::pin(async move {
            let split = async {
                fail(&QUERY_SCAN_ACCESS_PRE_TABLE_OPEN)?;
                scan_access::index_split(self, scan).await
            };
            split.await.map_err(|error| {
                let detail = error.to_string();
                *self
                    .source_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                PlanError::Source { detail }
            })
        })
    }

    fn index_facts(&self, dataset_key: &str) -> Vec<IndexFact> {
        self.table_stats
            .get(dataset_key)
            .map(|stats| stats.index_facts.clone())
            .unwrap_or_default()
    }

    fn traversal_work_limit(&self) -> Option<u64> {
        self.ir
            .has_edge_selections()
            .then(|| self.settings.traversal_work_limit())
    }

    fn schema(&self, side: SideId) -> std::result::Result<SchemaRef, PlanError> {
        Err(PlanError::Unresolved {
            detail: format!("a query plan names its scans by type, not by side {side:?}"),
        })
    }

    fn fragments(&self, _side: SideId) -> Vec<FragmentStat> {
        Vec::new()
    }

    fn adjacency_proof(&self) -> Option<&AdjacencyProof> {
        None
    }

    /// The pool every breaker of this query reserves from.
    fn query_memory_pool_bytes(&self) -> u64 {
        self.memory_limit
    }

    fn table_data_bytes(&self, type_key: &str) -> Option<u64> {
        self.table_stats.get(type_key)?.file_bytes
    }

    fn column_data_bytes(&self, type_key: &str, column: &str) -> Option<u64> {
        self.table_stats
            .get(type_key)?
            .column_bytes
            .get(column)
            .copied()
    }

    /// The scan keeps the catalog's key (`node:<type_name>`), never the
    /// entry's own: a historical read view binds a renamed type's old dataset
    /// under its current name, and the old name is unknown to the catalog.
    fn node_type(&self, type_name: &str) -> std::result::Result<NodeTypeSpec, PlanError> {
        let node_type =
            self.catalog
                .node_types
                .get(type_name)
                .ok_or_else(|| PlanError::Unresolved {
                    detail: format!("unknown node type `{type_name}`"),
                })?;
        let type_key = format!("node:{type_name}");
        let (table, version, row_count) = match self.snapshot.dataset(&type_key) {
            Some(entry) => (
                TableRef {
                    type_key,
                    dataset_path: entry.dataset_path.clone(),
                    native_branch: entry.native_dataset_branch.clone(),
                },
                Some(
                    entry
                        .version_metadata
                        .staged_version()
                        .unwrap_or(entry.published_dataset_version),
                ),
                Some(entry.entity_count),
            ),
            None => (
                TableRef {
                    type_key,
                    dataset_path: String::new(),
                    native_branch: None,
                },
                None,
                None,
            ),
        };
        Ok(NodeTypeSpec {
            table,
            version,
            columns: self.catalog.system_columns,
            schema: node_type.arrow_schema.clone(),
            key: node_type.key.clone().unwrap_or_default(),
            object_columns: node_type
                .node_object_fields()
                .map(|field| field.name().clone())
                .collect(),
            object_fields: node_type
                .node_object_members()
                .map(|(member, field)| {
                    arrow_schema::Field::new(member, field.data_type().clone(), field.is_nullable())
                })
                .collect(),
            row_count,
        })
    }

    fn list_parameter_len(&self, name: &str) -> Option<usize> {
        match self.params.shared().get(name)? {
            Literal::List(items) => Some(items.len()),
            _ => None,
        }
    }

    /// The scan lowers exactly the conjuncts `ir_expr_to_df_expr` can express;
    /// the schema argument only types a literal, never the verdict.
    fn filter_pushable(&self, filter: &IRExpr) -> bool {
        ir_expr_to_df_expr(filter, self.params.shared(), None).is_some()
    }

    /// The manifest's `entity_count` of the edge type and its two endpoint
    /// types in the pinned snapshot; `None` when any of the three tables is
    /// absent from it.
    fn expand_statistics(&self, edge_type: &str, direction: Direction) -> Option<ExpandStatistics> {
        let edge_def = self.catalog.edge_types.get(edge_type)?;
        let edge_count = self
            .snapshot
            .dataset(&format!("edge:{edge_type}"))?
            .entity_count;
        let (src_type, dst_type) = match direction {
            Direction::Out | Direction::Both => (&edge_def.from_type, &edge_def.to_type),
            Direction::In => (&edge_def.to_type, &edge_def.from_type),
        };
        let node_count = |type_name: &str| {
            self.snapshot
                .dataset(&format!("node:{type_name}"))
                .map(|entry| entry.entity_count)
        };
        Some(ExpandStatistics {
            edge_count,
            src_node_count: node_count(src_type)?,
            dst_node_count: node_count(dst_type)?,
            max_frontier_cap: self.expand_caps.max_frontier,
            max_hops_cap: self.expand_caps.max_hops,
        })
    }

    fn edge_dataset(&self, edge_type: &str) -> Option<DatasetPin> {
        self.snapshot
            .dataset(&super::edge_table_key(edge_type))
            .map(super::dataset_pin)
    }

    fn traversal(&self) -> Traversal {
        self.settings.traversal()
    }

    fn ann_nprobes(&self) -> Option<usize> {
        self.settings.ann_nprobes()
    }

    fn gate_policy(&self) -> GatePolicy {
        self.gate_policy
    }
}

fn no_plan(reason: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!(
        "the planner built no plan for this query: {reason}"
    ))
}

/// The query's parameters with every omitted nullable one bound to null and
/// `now()` bound to the clock; an omitted required one is the error the query
/// answers.
fn resolve_params(ir: &QueryIR, params: &ParamMap) -> Result<ResolvedParams> {
    let mut resolved = params.clone();
    fill_declared_params(&mut resolved, &ir.params)?;
    let now_name = omnigraph_compiler::query::ast::NOW_PARAM_NAME;
    if resolved.contains_key(now_name) {
        return Err(OmniError::manifest(format!(
            "param '{now_name}': reserved for now() and cannot be bound"
        )));
    }
    let now = time::OffsetDateTime::from(crate::dst_clock::system_time_now())
        .truncate_to_millisecond()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| OmniError::manifest(format!("failed to format now(): {error}")))?;
    resolved.insert(now_name.to_string(), Literal::DateTime(now));
    validate_params(&resolved, &ir.params)?;
    Ok(ResolvedParams(Arc::new(resolved)))
}

/// Build the physical plan for execution without explain diagnostics.
pub(crate) async fn plan_query(source: &QuerySource<'_>) -> Result<PhysicalPlan> {
    omnigraph_planner::plan_query(&source.ir, source, &source.bounds())
        .await
        .map_err(|reason| match source.take_source_failure() {
            Some(error) => error,
            None => match reason {
                Unrouted::UnsupportedQuery { message } => OmniError::manifest(message),
                reason => no_plan(reason.to_json()),
            },
        })
}

/// What the gate built for one compiled query: its explain document and the
/// physical plan the runner executes.
pub(crate) struct ExplainedQuery {
    pub explain: Explain,
    pub physical: PhysicalPlan,
}

/// A read query always gets a plan; a gate answer other than `Engine` is a
/// planner defect, never a fallback.
pub(crate) async fn explain_query(source: &QuerySource<'_>) -> Result<ExplainedQuery> {
    let operation = Operation::Query(Box::new(source.ir.clone()));
    match omnigraph_planner::route(
        &operation,
        source,
        RouteOverride::Registry,
        &source.bounds(),
    )
    .finalize(source)
    .await
    {
        Decision::Engine { plan, explain, .. } => Ok(ExplainedQuery {
            explain,
            physical: plan,
        }),
        Decision::PendingQuery(_) => Err(no_plan("query finalization remained pending")),
        Decision::Executor {
            reason: Unrouted::UnsupportedQuery { message },
            ..
        } => Err(OmniError::manifest(message)),
        Decision::Executor { reason, .. } => Err(source
            .take_source_failure()
            .unwrap_or_else(|| no_plan(reason.to_json()))),
        Decision::Routed { entry, .. } => Err(OmniError::manifest_internal(format!(
            "the registry routed a GQ query through entry `{}`; a read query runs only \
             the planner's own plan",
            entry.name
        ))),
    }
}

async fn query_table_statistics(
    ir: &QueryIR,
    snapshot: &Snapshot,
) -> Result<HashMap<String, TableStatistics>> {
    let mut types = BTreeMap::new();
    let mut pending = vec![ir.pipeline.as_slice()];
    while let Some(ops) = pending.pop() {
        for op in ops {
            match op {
                IROp::NodeScan {
                    type_name,
                    variable: _,
                    filters: _,
                } => {
                    types.entry(format!("node:{type_name}")).or_insert(false);
                }
                IROp::Expand {
                    edges,
                    src_type,
                    dst_type,
                    src_var: _,
                    dst_var: _,
                    min_hops: _,
                    max_hops: _,
                    dst_filters: _,
                    edge_binding: _,
                } => {
                    types.entry(format!("node:{src_type}")).or_insert(false);
                    types.insert(format!("node:{dst_type}"), true);
                    for member in edges.members() {
                        types
                            .entry(format!("edge:{}", member.edge_type))
                            .or_insert(false);
                    }
                }
                IROp::AntiJoin {
                    inner,
                    outer_var: _,
                    predicate: _,
                } => pending.push(inner),
                IROp::Filter(_) => {}
            }
        }
    }
    let mut bytes = HashMap::new();
    for (type_key, destination) in types {
        if snapshot.dataset(&type_key).is_none() {
            continue;
        }
        fail(&QUERY_INDEX_FACTS_PRE_LOAD)?;
        let dataset = snapshot.open_lance_dataset(&type_key).await?;
        let size = dataset
            .get_fragments()
            .iter()
            .flat_map(|fragment| fragment.metadata().files.iter())
            .try_fold(0u64, |total, file| {
                file.file_size_bytes
                    .get()
                    .map(|size| total.saturating_add(size.get()))
            });
        bytes.insert(
            type_key,
            TableStatistics {
                file_bytes: destination.then_some(size).flatten(),
                column_bytes: HashMap::new(),
                index_facts: indexes::gather(&dataset).await?,
            },
        );
    }
    Ok(bytes)
}

/// A projected parent includes each nested field's physical storage.
fn field_data_bytes(field: &Field, bytes: &HashMap<u32, u64>) -> Option<u64> {
    let own = *bytes.get(&u32::try_from(field.id).ok()?)?;
    field.children.iter().try_fold(own, |total, child| {
        field_data_bytes(child, bytes).map(|size| total.saturating_add(size))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use omnigraph_compiler::query::typecheck::typecheck_query;

    use crate::db::{Omnigraph, ReadTarget};
    use crate::engine::context::QueryContext;
    use crate::engine::expr::{ProjectionContext, collect_node_bindings};
    use crate::engine::lower::Lowering;
    use crate::engine::{EmbeddingResolver, EngineContext, GraphIndexHandle};
    use crate::instrumentation::with_query_memory_limit;

    const SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I64
}
node Doc {
    title: String @key
}
edge Likes: Person -> Doc
"#;

    const QUERIES: &str = r#"
query liked() {
    match { $p: Person $p likes $d }
    return { $p.name, $d.title }
}
query nobody_older() {
    match {
        $p: Person
        not {
            $q: Person
            $q.age > $p.age
        }
    }
    return { $p.name }
}
query likes_nothing() {
    match { $p: Person not { $p likes $d } }
    return { $p.name }
}
query people() { match { $p: Person } return { count($p) as n } }
"#;

    fn compile(catalog: &Catalog, name: &str) -> QueryIR {
        let statement = omnigraph_compiler::find_read_statement(QUERIES, name).unwrap();
        let checked = typecheck_query(catalog, statement.decl()).unwrap();
        omnigraph_compiler::lower_query(catalog, statement.decl(), &checked).unwrap()
    }

    /// GQT cannot inspect gathered facts or the catalog's version origin.
    #[tokio::test]
    async fn index_facts_cover_root_edge_and_nested_reads_at_the_pinned_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
                .await
                .unwrap(),
        );
        let session = crate::Session::from_defaults(Arc::clone(&db), SessionSettings::default());
        session
            .load_jsonl(
                concat!(
                    "{\"type\":\"Person\",\"data\":{\"name\":\"a\",\"age\":1}}\n",
                    "{\"type\":\"Doc\",\"data\":{\"title\":\"d\"}}\n",
                    "{\"edge\":\"Likes\",\"from\":\"a\",\"to\":\"d\"}\n"
                ),
                crate::loader::LoadMode::Overwrite,
            )
            .await
            .unwrap();
        session.ensure_indices().await.unwrap();
        let settings = SessionSettings::default();
        let (view, catalog) = db
            .capture_read_view(ReadTarget::branch("main"))
            .await
            .unwrap();
        for (name, expected) in [
            ("people", vec!["node:Person"]),
            ("liked", vec!["edge:Likes", "node:Doc", "node:Person"]),
            (
                "likes_nothing",
                vec!["edge:Likes", "node:Doc", "node:Person"],
            ),
        ] {
            let ir = compile(&catalog, name);
            let source =
                QuerySource::gather(&ir, &catalog, &view.snapshot, &ParamMap::new(), &settings)
                    .await
                    .unwrap();
            let mut keys: Vec<_> = source.table_stats.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(keys, expected, "{name}");
            let operation = Operation::Query(Box::new(source.ir.clone()));
            let decision = omnigraph_planner::route(
                &operation,
                &source,
                omnigraph_planner::RouteOverride::Registry,
                &source.bounds(),
            );
            let decision = decision.finalize(&source).await;
            let statistics = decision.explain().unwrap().statistics.as_ref().unwrap();
            for key in expected {
                let facts = source.index_facts(key);
                assert!(!facts.is_empty(), "{name} {key}");
                let statistic = statistics
                    .iter()
                    .filter(|s| s.statistic == format!("index_facts({key})"))
                    .collect::<Vec<_>>();
                assert_eq!(statistic.len(), 1, "{name} {key}");
                assert_eq!(
                    serde_json::from_str::<Vec<IndexFact>>(&statistic[0].value).unwrap(),
                    facts
                );
                assert!(statistic[0].origin.contains(&format!(
                    "version {}",
                    super::super::dataset_pin(view.snapshot.dataset(key).unwrap()).version
                )));
            }
        }
    }

    /// Rust and not `.gqt`: the claim is which map the lowering projects a bare
    /// binding through, and rows cannot tell the plan's map from the IR's.
    #[tokio::test]
    async fn plan_bindings_equal_the_ir_pipeline_bindings() {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        let (view, catalog) = db
            .capture_read_view(ReadTarget::branch("main"))
            .await
            .unwrap();
        let settings = SessionSettings::default();
        for (name, bound) in [
            ("liked", 2),
            ("nobody_older", 2),
            ("likes_nothing", 2),
            ("people", 1),
        ] {
            let ir = compile(&catalog, name);
            let source =
                QuerySource::gather(&ir, &catalog, &view.snapshot, &ParamMap::new(), &settings)
                    .await
                    .unwrap();
            let plan = plan_query(&source).await.unwrap();
            let mut from_ir = HashMap::new();
            collect_node_bindings(&ir.pipeline, &mut from_ir);
            assert_eq!(from_ir.len(), bound, "{name}");
            assert_eq!(
                ProjectionContext::for_plan(&catalog, &plan).bindings(),
                &from_ir,
                "{name}"
            );
        }
    }

    /// Rust and not `.gqt`: a case cannot change the ambient limit between
    /// gathering and running, which is the only time the two reads differ.
    #[tokio::test]
    async fn the_limit_gather_captured_sizes_the_plan_the_lowering_and_the_pool() {
        const CAPTURED: u64 = 3 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        let (view, catalog) = db
            .capture_read_view(ReadTarget::branch("main"))
            .await
            .unwrap();
        let settings = SessionSettings::default();
        let ir = compile(&catalog, "liked");
        let source = with_query_memory_limit(
            CAPTURED,
            QuerySource::gather(&ir, &catalog, &view.snapshot, &ParamMap::new(), &settings),
        )
        .await
        .unwrap();
        with_query_memory_limit(2 * CAPTURED, async {
            assert_eq!(source.bounds().query_memory_pool_bytes, CAPTURED);
            assert_eq!(source.query_memory_pool_bytes(), CAPTURED);
            let plan = plan_query(&source).await.unwrap();
            assert_eq!(plan.assumptions().memory_limit, CAPTURED);
            let bound = crate::engine::bind::bind(plan, &source, &EmbeddingResolver::explain())
                .await
                .unwrap();
            let context = EngineContext {
                snapshot: &view.snapshot,
                catalog: &catalog,
                graph_index: Arc::new(GraphIndexHandle::none()),
            };
            let lowering = Lowering::new(&bound, &context);
            assert_eq!(lowering.plan.assumptions().memory_limit, CAPTURED);
            let ctx = QueryContext::new(bound.plan.assumptions().memory_limit).unwrap();
            assert_eq!(ctx.memory_limit(), CAPTURED);
        })
        .await;
    }

    #[test]
    fn nested_field_statistics_follow_ids_and_require_every_child() {
        let arrow = arrow_schema::Field::new(
            "renamed",
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            true,
        );
        let mut field = Field::try_from(&arrow).unwrap();
        field.id = 4;
        field.children[0].id = 9;
        assert_eq!(
            field_data_bytes(&field, &HashMap::from([(4, 8), (9, 40)])),
            Some(48)
        );
        assert_eq!(field_data_bytes(&field, &HashMap::from([(4, 8)])), None);
        assert_eq!(
            field_data_bytes(&field, &HashMap::from([(4, 0), (9, 0)])),
            Some(0)
        );
    }
}

#[cfg(test)]
mod key_tests;

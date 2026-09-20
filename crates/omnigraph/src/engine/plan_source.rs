//! The planner's view of a read query: the catalog and the pinned snapshot
//! behind `PlanSource`, the parameters behind the scanner's pushability
//! verdict, and the physical plan the engine's runner executes.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use lance::dataset::statistics::DatasetStatisticsExt;
use lance::datatypes::Field;
use lance_file::version::ConcreteFileVersion;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRFilter, IROp, ParamMap, QueryIR};
use omnigraph_compiler::settings::Traversal;
use omnigraph_compiler::types::Direction;
use omnigraph_planner::{
    AdjacencyProof, Bounds, Decision, ExpandStatistics, Explain, FragmentStat, NodeTypeSpec,
    Operation, PhysicalPlan, PlanError, PlanSource, RouteOverride, SideId, TableRef,
};

use super::ResolvedParams;
use super::scan::ir_filter_to_expr;
use crate::db::Snapshot;
use crate::error::{OmniError, Result};

const KEY_WIDTH_BYTES: u64 = 8 + 8 + 32;

/// Max source-row frontier for which Expand uses the BTREE-indexed path.
/// Larger frontiers fall back to the in-memory CSR (dense / whole-graph). See
/// `docs/dev/execution.md`.
const DEFAULT_EXPAND_INDEXED_MAX_FRONTIER: u64 = 1024;

/// Max hop count for the indexed path (each hop is one indexed scan; very deep
/// traversals fan out toward whole-graph and are better served by CSR).
const DEFAULT_EXPAND_INDEXED_MAX_HOPS: u32 = 6;

pub(super) fn expand_indexed_max_frontier() -> u64 {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_FRONTIER)
}

pub(super) fn expand_indexed_max_hops() -> u32 {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_HOPS)
}

pub(crate) struct QuerySource<'a> {
    pub catalog: &'a Catalog,
    pub snapshot: &'a Snapshot,
    pub params: &'a ParamMap,
    pub traversal: Traversal,
    table_stats: HashMap<String, TableStatistics>,
}

struct TableStatistics {
    file_bytes: Option<u64>,
    column_bytes: HashMap<String, u64>,
}

impl QuerySource<'_> {
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
        super::context::query_memory_limit()
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
                Some(entry.published_dataset_version),
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
            row_count,
        })
    }

    /// The scan lowers exactly the filters `ir_filter_to_expr` can express;
    /// the schema argument only types a literal, never the verdict.
    fn filter_pushable(&self, filter: &IRFilter) -> bool {
        ir_filter_to_expr(filter, self.params, None).is_some()
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
            same_type: edge_def.from_type == edge_def.to_type,
            max_frontier_cap: expand_indexed_max_frontier(),
            max_hops_cap: expand_indexed_max_hops(),
        })
    }

    fn traversal(&self) -> Traversal {
        self.traversal
    }
}

/// The memory constants the planner declares against: the change-feed
/// values, so one explain document reads the same on every operation. A read
/// plan's nodes declare no retained memory against them.
fn bounds() -> Bounds {
    Bounds {
        hydration_chunk_hard_bytes: 2 * crate::storage_layer::KEYED_WRITE_MAX_BYTES,
        key_width_bytes: KEY_WIDTH_BYTES,
        ordered_scan_memory_bytes: crate::table_store::ORDERED_SCAN_MEMORY_BYTES,
        ordered_scan_max_input_batch_bytes: crate::table_store::ORDERED_SCAN_MAX_INPUT_BATCH_BYTES,
        build_key_cap_rows: super::push::BUILD_KEY_CAP_ROWS as u64,
        late_materialization_only: false,
    }
}

fn no_plan(reason: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!(
        "the planner built no plan for this query: {reason}"
    ))
}

/// Build the physical plan for execution without explain diagnostics.
pub(crate) async fn plan_query(
    ir: &QueryIR,
    params: &ResolvedParams,
    catalog: &Catalog,
    snapshot: &Snapshot,
    traversal: Traversal,
) -> Result<PhysicalPlan> {
    let mut source = QuerySource {
        catalog,
        snapshot,
        params: params.shared(),
        traversal,
        table_stats: destination_table_statistics(ir, snapshot).await?,
    };
    source
        .load_column_statistics(&Operation::Query(Box::new(ir.clone())))
        .await?;
    omnigraph_planner::plan_query(ir, &source, &bounds())
        .map_err(|reason| no_plan(reason.to_json()))
}

/// What the gate built for one compiled query: its explain document and the
/// physical plan the runner executes.
pub(crate) struct ExplainedQuery {
    pub explain: Explain,
    pub physical: PhysicalPlan,
}

/// A read query always gets a plan; a gate answer other than `Engine` is a
/// planner defect, never a fallback.
pub(crate) async fn explain_query(
    ir: &QueryIR,
    params: &ResolvedParams,
    catalog: &Catalog,
    snapshot: &Snapshot,
    traversal: Traversal,
) -> Result<ExplainedQuery> {
    let mut source = QuerySource {
        catalog,
        snapshot,
        params: params.shared(),
        traversal,
        table_stats: destination_table_statistics(ir, snapshot).await?,
    };
    let operation = Operation::Query(Box::new(ir.clone()));
    source.load_column_statistics(&operation).await?;
    match omnigraph_planner::route(&operation, &source, RouteOverride::Registry, &bounds()) {
        Decision::Engine { plan, explain, .. } => Ok(ExplainedQuery {
            explain,
            physical: plan,
        }),
        Decision::Executor { reason, .. } => Err(no_plan(reason.to_json())),
        Decision::Routed { entry, .. } => Err(OmniError::manifest_internal(format!(
            "the registry routed a GQ query through entry `{}`; a read query runs only \
             the planner's own plan",
            entry.name
        ))),
    }
}

async fn destination_table_statistics(
    ir: &QueryIR,
    snapshot: &Snapshot,
) -> Result<HashMap<String, TableStatistics>> {
    fn destinations(ops: &[IROp], types: &mut BTreeSet<String>) {
        for op in ops {
            match op {
                IROp::Expand { dst_type, .. } => {
                    types.insert(format!("node:{dst_type}"));
                }
                IROp::AntiJoin { inner, .. } => destinations(inner, types),
                _ => {}
            }
        }
    }
    let mut types = BTreeSet::new();
    destinations(&ir.pipeline, &mut types);
    let mut bytes = HashMap::new();
    for type_key in types {
        if snapshot.dataset(&type_key).is_none() {
            continue;
        }
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
                file_bytes: size,
                column_bytes: HashMap::new(),
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

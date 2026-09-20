//! The read-path plan (RFC 0068 milestone 3): a GQ query resolves to a tree
//! and `rewrite` writes each bound scan's projection from what the whole
//! tree reads through its binding. Synthetic sources expose planner states
//! and JSON structure that the GQT plan assertions cannot observe.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use omnigraph_compiler::SYSTEM_COLUMNS_V3;
use omnigraph_compiler::ir::{IRExpr, IRFilter, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal};
use omnigraph_compiler::settings::Traversal;
use omnigraph_compiler::types::Direction;
use omnigraph_planner::optimizer::resolve;
use omnigraph_planner::{
    AccessPath, AdjacencyProof, Bounds, ExpandMode, ExpandStatistics, FragmentStat, LogicalNode,
    LogicalPlan, MemorySource, NodeTypeSpec, Operation, PhysicalNode, PhysicalPlan, PlanError,
    PlanSource, SideId, TableRef, rewrite,
};

const PROPERTIES: &[&str] = &[
    "slug", "state", "rank", "title", "probe", "edits", "body", "k_ref", "kind", "kind_ref", "x",
    "text",
];

fn schema() -> SchemaRef {
    let mut fields = vec![Field::new(SYSTEM_COLUMNS_V3.id, DataType::Utf8, false)];
    fields.extend(
        PROPERTIES
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true)),
    );
    fields.push(Field::new(
        "embedding",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
        true,
    ));
    Arc::new(Schema::new(fields))
}

/// One node type `T`: key `slug`, every property an object member except the
/// `Vector` column `embedding`.
fn source() -> MemorySource {
    source_with_rows(None)
}

fn source_with_rows(row_count: Option<u64>) -> MemorySource {
    MemorySource::default().with_node_type("T", node_type("T", row_count))
}

/// A node type shaped like `T` (key `slug`, the same properties) under
/// `type_name`, with `row_count` rows.
fn node_type(type_name: &str, row_count: Option<u64>) -> NodeTypeSpec {
    let mut object_columns = vec![SYSTEM_COLUMNS_V3.id.to_string()];
    object_columns.extend(PROPERTIES.iter().map(|name| name.to_string()));
    NodeTypeSpec {
        table: TableRef {
            type_key: format!("node:{type_name}"),
            dataset_path: format!("node/{type_name}"),
            native_branch: None,
        },
        version: Some(7),
        columns: SYSTEM_COLUMNS_V3,
        schema: schema(),
        key: vec!["slug".to_string()],
        object_columns,
        row_count,
    }
}

fn scan(variable: &str) -> IROp {
    scan_of(variable, "T")
}

fn scan_of(variable: &str, type_name: &str) -> IROp {
    IROp::NodeScan {
        variable: variable.to_string(),
        type_name: type_name.to_string(),
        filters: vec![],
    }
}

fn prop(variable: &str, property: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: variable.to_string(),
        property: property.to_string(),
    }
}

fn ir(pipeline: Vec<IROp>, returns: Vec<IRExpr>, order_by: Vec<IRExpr>) -> Operation {
    Operation::Query(Box::new(QueryIR {
        name: "q".to_string(),
        params: vec![],
        pipeline,
        return_exprs: returns
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
        limit: Some(10),
    }))
}

fn planned(op: &Operation) -> (LogicalPlan, Vec<&'static str>) {
    let source = source();
    let mut plan = resolve(op, &source).expect("a query resolves");
    let fired = rewrite(&mut plan, &source).expect("rewrite runs");
    (plan, fired)
}

fn projection_of(plan: &LogicalPlan, binding: &str) -> BTreeSet<String> {
    plan.live()
        .find_map(|(_, node)| match node {
            LogicalNode::TableScan { spec, .. } if spec.binding.as_deref() == Some(binding) => {
                Some(
                    spec.projection
                        .clone()
                        .expect("the pass wrote a projection"),
                )
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no scan bound to `{binding}`"))
        .into_iter()
        .collect()
}

fn set(columns: &[&str]) -> BTreeSet<String> {
    columns.iter().map(|column| column.to_string()).collect()
}

fn object_columns() -> BTreeSet<String> {
    let mut columns = set(&[SYSTEM_COLUMNS_V3.id]);
    columns.extend(PROPERTIES.iter().map(|name| name.to_string()));
    columns
}

fn expand(src: &str, dst: &str, dst_filters: Vec<IRFilter>) -> IROp {
    IROp::Expand {
        src_var: src.to_string(),
        dst_var: dst.to_string(),
        edge_type: "knows".to_string(),
        direction: Direction::Out,
        dst_type: "T".to_string(),
        min_hops: 1,
        max_hops: Some(1),
        edge_binding: None,
        dst_filters,
    }
}

#[path = "support/bounds.rs"]
mod fixture_bounds;

fn bounds() -> Bounds {
    fixture_bounds::BOUNDS
}

/// The physical plan and the fired passes of `op` over `source`.
fn physical(op: &Operation, source: &dyn PlanSource) -> (PhysicalPlan, Vec<&'static str>) {
    let mut plan = resolve(op, source).expect("resolve traversal");
    let fired = rewrite(&mut plan, source).expect("rewrite traversal");
    let optimized = omnigraph_planner::physical_plan(&mut plan, source, &bounds(), fired)
        .expect("lower traversal");
    (optimized.physical, optimized.fired)
}

/// Synthetic same-type edges keep the fixture table cardinality consistent.
fn knows_statistics(rows: u64) -> ExpandStatistics {
    ExpandStatistics {
        edge_count: rows * 10,
        src_node_count: rows,
        dst_node_count: rows,
        same_type: true,
        max_frontier_cap: 1024,
        max_hops_cap: 6,
    }
}

/// Every physical `Expand` of the plan, in post-order, as `(mode, estimate)`.
fn expand_modes(plan: &PhysicalPlan) -> Vec<(ExpandMode, Option<u64>)> {
    plan.post_order()
        .into_iter()
        .filter_map(|id| match plan.node(id) {
            Some(PhysicalNode::Expand {
                mode,
                frontier_estimate,
                ..
            }) => Some((*mode, *frontier_estimate)),
            _ => None,
        })
        .collect()
}

/// The explain JSON of every physical `Expand`, in pre-order.
fn expand_json(node: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
    if node["node"] == "Expand" {
        out.push(node.clone());
    }
    for input in node["inputs"].as_array().into_iter().flatten() {
        expand_json(input, out);
    }
}

/// The access path of every dependent physical scan, in post-order, and the
/// `access` key of the explain JSON for the one dependent scan.
fn access_paths(plan: &PhysicalPlan) -> (Vec<AccessPath>, serde_json::Value) {
    let paths = plan
        .post_order()
        .into_iter()
        .filter_map(|id| match plan.node(id) {
            Some(PhysicalNode::Scan {
                source: omnigraph_planner::ScanInput::Dependent { access, .. },
                ..
            }) => Some(*access),
            _ => None,
        })
        .collect();
    let json = plan.to_json();
    let destination = &json["inputs"][0]["inputs"][0];
    assert_eq!(destination["id_restriction"], "input");
    (paths, destination["access"].clone())
}

const POOL_BYTES: u64 = 150 * 1024 * 1024;

/// `source` under the 150 MiB pool with 4 MiB of `node:T` data files: a
/// build side that fits one part in four of the pool.
fn pooled(source: MemorySource) -> MemorySource {
    source
        .with_query_memory_pool_bytes(POOL_BYTES)
        .with_table_data_bytes("node:T", 4 * 1024 * 1024)
}

#[path = "query_plan/cost.rs"]
mod cost;
#[path = "query_plan/explain.rs"]
mod explain;
#[path = "query_plan/logical.rs"]
mod logical;

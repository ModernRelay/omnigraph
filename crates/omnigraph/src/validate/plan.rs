//! The planner's word on a staged write: one `Operation::Write` per table the
//! write touches, routed to the executor with its physical plan, whose root
//! `Validate` node the judge executes. The route attaches what `plan_load`
//! attaches, every consulted table's row count and index facts at the
//! committed version, so planning a write reads metadata and never a row.

use std::collections::{BTreeSet, HashMap};

use omnigraph_compiler::catalog::{Catalog, NodeType};
use omnigraph_compiler::ir::{DeltaClass, WriteKind, lower_write_table};
use omnigraph_compiler::query::ast::LoadTarget;
use omnigraph_planner::{
    CommittedSide, Decision, Explain, IndexFact, MemorySource, NodeTypeSpec, Operation,
    PhysicalNode, RouteOverride, Side, SideId, route,
};

use super::load::JudgedTable;
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::loader::{csv, load_sides};

/// One table of a staged write as its route describes it to the planner: the
/// delta's class and exact rows, and the ids the write removes, which the
/// node's checks exempt and recount.
pub(crate) struct WriteTable {
    pub(crate) table: String,
    pub(crate) delta: DeltaClass,
    pub(crate) rows: u64,
    pub(crate) deleted_ids: Vec<String>,
}

/// Where `table` stands in a write's judgment: node tables before edge
/// tables, each in name order, so a node's own violation is refused before
/// a reference to the node.
pub(crate) fn judgment_rank(table: &str) -> (bool, &str) {
    (table.starts_with("edge:"), table)
}

/// Plan every table of a write under `catalog` against `snapshot`, in the
/// order given ([`judgment_rank`]): each table's `Validate` node with its
/// decided access becomes the judge's table, beside the planner's document
/// for the receipt.
pub(crate) async fn plan_write(
    snapshot: &Snapshot,
    catalog: &Catalog,
    kind: WriteKind,
    committed: CommittedSide,
    tables: Vec<WriteTable>,
    write_max_bytes: u64,
) -> Result<Vec<(JudgedTable, Explain)>> {
    let mut lowered = Vec::with_capacity(tables.len());
    let mut consulted = BTreeSet::new();
    for table in tables {
        let target = load_target(&table.table)?;
        let ir = lower_write_table(
            catalog,
            target.clone(),
            table.delta,
            !table.deleted_ids.is_empty(),
            table.rows,
        );
        let sides = load_sides(snapshot, catalog.system_columns, &table.table, &ir.checks);
        consulted.extend(
            sides
                .iter()
                .map(|side| side.table.type_key.clone())
                .filter(|key| snapshot.dataset(key).is_some()),
        );
        lowered.push((table, target, ir, sides));
    }
    let facts = futures::future::try_join_all(consulted.iter().map(|key| async move {
        let facts = crate::engine::plan_source::load_index_facts(snapshot, key).await?;
        Ok::<_, OmniError>((key.as_str(), facts))
    }))
    .await?;
    let facts: HashMap<&str, Vec<IndexFact>> = facts.into_iter().collect();
    let bounds = crate::engine::engine_bounds(write_max_bytes, write_max_bytes, 1);
    let mut planned = Vec::with_capacity(lowered.len());
    for (table, target, ir, sides) in lowered {
        let mut source = MemorySource::default()
            .with_schema(SideId::Child, csv::target_arrow_schema(catalog, &target)?);
        for side in &sides {
            let key = side.table.type_key.as_str();
            let Some(entry) = snapshot.dataset(key) else {
                continue;
            };
            if let Some((name, node)) = key
                .strip_prefix("node:")
                .and_then(|name| catalog.node_types.get(name).map(|node| (name, node)))
            {
                source = source.with_node_type(
                    name,
                    node_type_spec(side, node, catalog, Some(entry.entity_count)),
                );
            }
            let facts = facts.get(key).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "Validate: no index facts were read for the consulted table {key}"
                ))
            })?;
            source = source.with_index_facts(key, facts);
        }
        let operation = Operation::Write {
            ir: Box::new(ir),
            kind,
            tables: sides,
            committed,
        };
        let decision = route(&operation, &source, RouteOverride::Registry, &bounds);
        planned.push(judged_table(
            decision,
            table.table,
            table.delta,
            table.deleted_ids,
        )?);
    }
    Ok(planned)
}

/// The judge's table out of the planner's decision for `table`: the `Validate`
/// node of the physical plan the executor was handed (the root, or under a
/// `load csv` plan's `FragmentWrite`), with the document the receipt carries.
pub(crate) fn judged_table(
    decision: Decision,
    table: String,
    delta: DeltaClass,
    deleted_ids: Vec<String>,
) -> Result<(JudgedTable, Explain)> {
    let Decision::Executor {
        reason,
        explain,
        physical,
        ..
    } = decision
    else {
        return Err(OmniError::manifest_internal(format!(
            "Validate: the planner routed the write of {table} away from the executor"
        )));
    };
    let Some(physical) = physical else {
        return Err(OmniError::manifest_internal(format!(
            "Validate: the planner built no plan for the write of {table}: {}",
            reason.kind()
        )));
    };
    let validate = match physical.node(physical.root()) {
        Some(PhysicalNode::FragmentWrite { input, .. }) => physical.node(*input),
        root => root,
    };
    let Some(PhysicalNode::Validate {
        table: planned,
        checks,
        removal,
        ..
    }) = validate
    else {
        return Err(OmniError::manifest_internal(format!(
            "Validate: the write plan of {table} has no Validate root"
        )));
    };
    if *planned != table {
        return Err(OmniError::manifest_internal(format!(
            "Validate: the plan judges {planned} and the write stages {table}"
        )));
    }
    Ok((
        JudgedTable {
            table,
            checks: checks.clone(),
            removal: *removal,
            delta,
            deleted_ids,
        },
        explain,
    ))
}

/// The planner's view of a committed node table: its pinned side, schema,
/// key and row count, what `validate_access` and the endpoint probe read.
pub(crate) fn node_type_spec(
    side: &Side,
    node: &NodeType,
    catalog: &Catalog,
    row_count: Option<u64>,
) -> NodeTypeSpec {
    NodeTypeSpec {
        table: side.table.clone(),
        version: Some(side.version),
        columns: catalog.system_columns,
        schema: node.arrow_schema.clone(),
        key: node.key.clone().unwrap_or_default(),
        object_columns: node
            .node_object_fields()
            .map(|field| field.name().clone())
            .collect(),
        object_fields: node
            .node_object_members()
            .map(|(member, field)| {
                arrow_schema::Field::new(member, field.data_type().clone(), field.is_nullable())
            })
            .collect(),
        row_count,
    }
}

fn load_target(table: &str) -> Result<LoadTarget> {
    if let Some(name) = table.strip_prefix("node:") {
        Ok(LoadTarget::Node(name.to_string()))
    } else if let Some(name) = table.strip_prefix("edge:") {
        Ok(LoadTarget::Edge(name.to_string()))
    } else {
        Err(OmniError::manifest_internal(format!(
            "Validate: {table} is neither a node nor an edge table"
        )))
    }
}

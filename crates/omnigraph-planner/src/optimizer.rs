//! The fixed pass sequence, in three stages: logical rewrites, physical
//! selection, property derivation. The order is written once here with its
//! reason; a pass runs only when its trigger is present in the plan.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use omnigraph_compiler::ir::{IRExpr, IRFilter, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::AggFunc;
use omnigraph_compiler::settings::Traversal;
use omnigraph_compiler::types::Direction;

use crate::cost::{
    AccessPath, ExpandCostInputs, ExpandMode, ExpandPolicy, HASH_JOIN_POOL_DIVISOR, IndexCoverage,
    choose_access_path, choose_expand_mode, direction_probe_factor, estimate_rows, executed_hops,
    scan_row_estimate,
};
use crate::error::PlanError;
use crate::logical::{
    ColumnRef, GqFilter, IDENTITY_MEMBER, KeyJoinKind, LOGICAL_ID, LogicalId, LogicalNode,
    LogicalPlan, Predicate, ScanSpec, SearchArm, ordering_text,
};
use crate::operation::{Operation, Side};
use crate::physical::{
    Estimate, Hop, NodeId, OverfetchRung, PhysicalNode, PhysicalPlan, Prefilter, Properties,
    RankArm, RankKind, RankScope, RankedAccess, ScanInput, StatisticSource,
};
use crate::source::{NodeTypeSpec, PlanSource, SideId};

pub const ROW_ID: &str = "_rowid";
pub const ROW_ADDR: &str = "_rowaddr";

/// The engine's memory constants, handed in so the planner declares the
/// bounds its operators enforce without depending on the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub hydration_chunk_hard_bytes: u64,
    pub key_width_bytes: u64,
    /// The ordered scan's sort memory pool; a full-width sort past it spills.
    pub ordered_scan_memory_bytes: u64,
    /// The ordered scan's single input batch cap; one fragment of complete
    /// rows past it is refused.
    pub ordered_scan_max_input_batch_bytes: u64,
    /// Key rows a join's build side may hold before the executor refuses.
    pub build_key_cap_rows: u64,
    /// The effective memory limit of the query, captured once where the
    /// source is gathered; the run's memory pool is sized from this value.
    pub query_memory_pool_bytes: u64,
    /// Set by the engine after a one-pass shape was refused at open: pass 6
    /// then hydrates by address whatever the manifest says.
    pub late_materialization_only: bool,
}

pub const PASS_RESOLVE: &str = "resolve";
pub const PASS_PREDICATE_PUSHDOWN: &str = "predicate_pushdown";
pub const PASS_AGGREGATE_PUSHDOWN: &str = "aggregate_pushdown";
pub const PASS_PROJECTION_PUSHDOWN: &str = "projection_pushdown";
pub const PASS_FRAGMENT_SCOPE: &str = "fragment_scope";
pub const PASS_ADDRESS_SHORT_CIRCUIT: &str = "address_short_circuit";
pub const PASS_LATE_MATERIALIZATION: &str = "late_materialization";
pub const PASS_JOIN_ALGORITHM: &str = "join_algorithm";
pub const PASS_EXPAND_MODE: &str = "expand_mode";
pub const PASS_ACCESS_PATH: &str = "access_path";

/// The optimizer's output: the physical plan with every property declared,
/// the passes that fired, and every statistic a pass read.
#[derive(Debug, Clone)]
pub struct Optimized {
    pub physical: PhysicalPlan,
    pub fired: Vec<&'static str>,
    pub statistics: Vec<StatisticSource>,
}

/// Stage 1, pass 1. Build the logical plan an operation means and compute
/// every node's schema. The census the registry keys on is taken from this
/// output before any rewrite.
pub fn resolve(op: &Operation, source: &dyn PlanSource) -> Result<LogicalPlan, PlanError> {
    let mut plan = LogicalPlan::new();
    match op {
        Operation::CommitDiff {
            parent,
            child,
            resume,
            budget,
            ..
        } => {
            let parent_node = scan(&mut plan, SideId::Parent, parent, source)?;
            let child_node = scan(&mut plan, SideId::Child, child, source)?;
            let (left, right, kind) = match source.adjacency_proof() {
                Some(_) => (child_node, parent_node, KeyJoinKind::LeftOuter),
                None => (parent_node, child_node, KeyJoinKind::FullOuter),
            };
            let diff = diff_over_join(&mut plan, left, right, kind)?;
            let schema = schema_of(&plan, diff)?;
            let order = plan.add(
                LogicalNode::Ordered {
                    input: diff,
                    keys: vec![LOGICAL_ID.to_string()],
                },
                schema.clone(),
            );
            let limit = plan.add(
                LogicalNode::Page {
                    input: order,
                    rows: budget.rows,
                    bytes: budget.bytes,
                    resume: resume.clone(),
                },
                schema,
            );
            plan.set_root(limit);
        }
        Operation::SnapshotDiff { from, to } => {
            let from_node = scan(&mut plan, SideId::Parent, from, source)?;
            let to_node = scan(&mut plan, SideId::Child, to, source)?;
            let diff = diff_over_join(&mut plan, from_node, to_node, KeyJoinKind::FullOuter)?;
            let schema = schema_of(&plan, diff)?;
            let order = plan.add(
                LogicalNode::Ordered {
                    input: diff,
                    keys: vec![LOGICAL_ID.to_string()],
                },
                schema,
            );
            plan.set_root(order);
        }
        Operation::MergeClassify {
            base,
            source: merge_source,
            target,
        } => {
            let base_node = scan(&mut plan, SideId::Base, base, source)?;
            let source_node = scan(&mut plan, SideId::Parent, merge_source, source)?;
            let target_node = scan(&mut plan, SideId::Child, target, source)?;
            let schema = classify_schema(&schema_of(&plan, base_node)?);
            let classify = plan.add(
                LogicalNode::MergeClassify {
                    base: base_node,
                    source: source_node,
                    target: target_node,
                },
                schema,
            );
            plan.set_root(classify);
        }
        Operation::Query(ir) => resolve_query(&mut plan, ir, source)?,
    }
    Ok(plan)
}

/// Stage 1, pass 1 for a GQ query: the flat `IROp` pipeline folded into a
/// tree, then a leading search function, the `return`, the remaining
/// `order` keys and the `limit` above it.
fn resolve_query(
    plan: &mut LogicalPlan,
    ir: &QueryIR,
    source: &dyn PlanSource,
) -> Result<(), PlanError> {
    let QueryIR {
        name: _,
        params: _,
        pipeline,
        return_exprs,
        order_by,
        limit,
    } = ir;
    let mut next_binding = 0u16;
    let mut current = resolve_pipeline(plan, pipeline, source, &mut next_binding, None)?;
    let schema = schema_of(plan, current)?;
    let mut orderings: &[IROrdering] = order_by;
    let mut ranked = false;
    if let Some(leading) = orderings.first() {
        if let Some(node) = search_node(current, &leading.expr, *limit) {
            current = plan.add(node, schema.clone());
            orderings = &orderings[1..];
            ranked = true;
        }
    }
    let mut reads = Vec::new();
    for IRProjection { expr, alias: _ } in return_exprs {
        reads_of_expr(expr, &mut reads);
    }
    let has_aggregates = return_exprs
        .iter()
        .any(|projection| matches!(projection.expr, IRExpr::Aggregate { .. }));
    current = if has_aggregates {
        plan.add(
            LogicalNode::Aggregate {
                input: current,
                reads,
                return_exprs: return_exprs.clone(),
            },
            schema.clone(),
        )
    } else {
        plan.add(
            LogicalNode::Projection {
                input: current,
                reads,
                return_exprs: return_exprs.clone(),
            },
            schema.clone(),
        )
    };
    if ranked || !orderings.is_empty() {
        let mut keys = Vec::new();
        for IROrdering {
            expr,
            descending: _,
        } in orderings
        {
            order_keys(expr, &mut keys);
        }
        let tiebreak = sort_tiebreak(
            &scope_bindings(plan, current),
            order_by,
            return_exprs,
            has_aggregates,
        );
        current = plan.add(
            LogicalNode::Sort {
                input: current,
                keys,
                order_by: orderings.to_vec(),
                fetch: limit.and_then(|limit| usize::try_from(limit).ok()),
                tiebreak,
            },
            schema.clone(),
        );
    }
    if let Some(limit) = *limit {
        current = plan.add(
            LogicalNode::Limit {
                input: current,
                rows: usize::try_from(limit).unwrap_or(usize::MAX),
            },
            schema,
        );
    }
    plan.set_root(current);
    Ok(())
}

/// The pipeline folded into a tree over `seed`: the top-level pipeline starts
/// from nothing and its first op is a scan; a `not { … }` inner pipeline
/// starts from the `OuterReference`, so its first op may filter or expand.
fn resolve_pipeline(
    plan: &mut LogicalPlan,
    ops: &[IROp],
    source: &dyn PlanSource,
    next_binding: &mut u16,
    seed: Option<LogicalId>,
) -> Result<LogicalId, PlanError> {
    let mut current: Option<LogicalId> = seed;
    for op in ops {
        let node = match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let NodeTypeSpec {
                    table,
                    version,
                    columns,
                    schema,
                    ..
                } = source.node_type(type_name)?;
                let side = SideId::Binding(*next_binding);
                *next_binding += 1;
                let scan = plan.add(
                    LogicalNode::TableScan {
                        input: None,
                        spec: Box::new(ScanSpec {
                            side,
                            table,
                            version,
                            columns,
                            fragments: None,
                            projection: None,
                            filter: gq_predicate(filters),
                            binding: Some(variable.clone()),
                        }),
                    },
                    schema.clone(),
                );
                match current {
                    None => scan,
                    Some(left) => {
                        let joined = join_schema(
                            &schema_of(plan, left)?,
                            binding_of(plan, left).as_deref(),
                            &schema,
                            Some(variable.as_str()),
                        );
                        plan.add(LogicalNode::CrossJoin { left, right: scan }, joined)
                    }
                }
            }
            IROp::Filter(filter) => {
                let input = bound(current, "filter")?;
                let schema = schema_of(plan, input)?;
                let predicate = gq_predicate(std::slice::from_ref(filter))
                    .ok_or_else(|| PlanError::Internal("a filter op carries one filter".into()))?;
                plan.add(LogicalNode::Filter { input, predicate }, schema)
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
                let input = bound(current, "expand")?;
                let schema = schema_of(plan, input)?;
                let NodeTypeSpec {
                    table,
                    version,
                    columns,
                    ..
                } = source.node_type(dst_type)?;
                let side = SideId::Binding(*next_binding);
                *next_binding += 1;
                let expand = plan.add(
                    LogicalNode::Expand {
                        input,
                        src: src_var.clone(),
                        dst: dst_var.clone(),
                        edge_type: edge_type.clone(),
                        direction: *direction,
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        edge_binding: edge_binding.clone(),
                    },
                    schema.clone(),
                );
                let (pushed, residual): (Vec<_>, Vec<_>) = dst_filters
                    .iter()
                    .cloned()
                    .partition(|filter| dependent_scan_filter_pushable(filter, dst_var, source));
                let destination = plan.add(
                    LogicalNode::TableScan {
                        input: Some(expand),
                        spec: Box::new(ScanSpec {
                            side,
                            table,
                            version,
                            columns,
                            fragments: None,
                            projection: None,
                            filter: gq_predicate(&pushed),
                            binding: Some(dst_var.clone()),
                        }),
                    },
                    schema.clone(),
                );
                match gq_predicate(&residual) {
                    Some(predicate) => plan.add(
                        LogicalNode::Filter {
                            input: destination,
                            predicate,
                        },
                        schema,
                    ),
                    None => destination,
                }
            }
            IROp::AntiJoin { outer_var, inner } => {
                let input = bound(current, "anti-join")?;
                let schema = schema_of(plan, input)?;
                let outer = plan.add(
                    LogicalNode::OuterReference {
                        outer_var: outer_var.clone(),
                    },
                    schema.clone(),
                );
                let inner = resolve_pipeline(plan, inner, source, next_binding, Some(outer))?;
                plan.add(
                    LogicalNode::AntiJoin {
                        input,
                        inner,
                        outer_var: outer_var.clone(),
                    },
                    schema,
                )
            }
        };
        current = Some(node);
    }
    current.ok_or_else(|| PlanError::Internal("a GQ pipeline starts with a scan".to_string()))
}

fn bound(current: Option<LogicalId>, op: &str) -> Result<LogicalId, PlanError> {
    current.ok_or_else(|| PlanError::Internal(format!("a GQ {op} op has no input")))
}

fn schema_of(plan: &LogicalPlan, id: LogicalId) -> Result<SchemaRef, PlanError> {
    plan.schema(id)
        .cloned()
        .ok_or_else(|| PlanError::Internal(format!("logical node {id} has no schema")))
}

fn binding_of(plan: &LogicalPlan, id: LogicalId) -> Option<String> {
    match plan.node(id) {
        Some(LogicalNode::TableScan { input: None, spec }) => spec.binding.clone(),
        _ => None,
    }
}

fn gq_predicate(filters: &[IRFilter]) -> Option<Predicate> {
    filters
        .iter()
        .map(|filter| {
            let mut reads = Vec::new();
            reads_of_filter(filter, &mut reads);
            Predicate::Gq {
                reads,
                text: filter.to_string(),
                filter: GqFilter(filter.clone()),
            }
        })
        .reduce(Predicate::and)
}

/// The node a leading `order` search function becomes; `None` leaves the
/// expression to `Sort`, which then reads its columns like any other.
fn search_node(input: LogicalId, expr: &IRExpr, limit: Option<u64>) -> Option<LogicalNode> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let mut reads = Vec::new();
            reads_of_expr(query, &mut reads);
            Some(LogicalNode::Nearest {
                input,
                binding: variable.clone(),
                property: property.clone(),
                query: query.as_ref().clone(),
                k: limit,
                reads,
            })
        }
        IRExpr::Bm25 { field, query } => {
            let IRExpr::PropAccess { variable, property } = field.as_ref() else {
                return None;
            };
            let mut reads = Vec::new();
            reads_of_expr(query, &mut reads);
            Some(LogicalNode::TextSearch {
                input,
                binding: variable.clone(),
                property: property.clone(),
                query: query.as_ref().clone(),
                reads,
            })
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            let arms = [search_target(primary)?, search_target(secondary)?];
            let mut reads = Vec::new();
            for arm in [primary, secondary] {
                search_arm_reads(arm, &mut reads);
            }
            if let Some(k) = k {
                reads_of_expr(k, &mut reads);
            }
            Some(LogicalNode::RankFuse {
                input,
                arms,
                k: k.as_deref().cloned(),
                limit,
                reads,
            })
        }
        _ => None,
    }
}

fn search_target(expr: &IRExpr) -> Option<SearchArm> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => Some(SearchArm {
            binding: variable.clone(),
            property: property.clone(),
            kind: RankKind::Nearest,
            query: query.as_ref().clone(),
        }),
        IRExpr::Bm25 { field, query } => match field.as_ref() {
            IRExpr::PropAccess { variable, property } => Some(SearchArm {
                binding: variable.clone(),
                property: property.clone(),
                kind: RankKind::Bm25,
                query: query.as_ref().clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

/// A search arm reads only its query arguments: Lance ranks on the target
/// column itself and appends the score column the query sorts on.
fn search_arm_reads(expr: &IRExpr, out: &mut Vec<ColumnRef>) {
    match expr {
        IRExpr::Nearest { query, .. } | IRExpr::Bm25 { query, .. } => reads_of_expr(query, out),
        other => reads_of_expr(other, out),
    }
}

fn order_keys(expr: &IRExpr, out: &mut Vec<String>) {
    match expr {
        IRExpr::AliasRef(alias) => out.push(format!("{ALIAS_KEY}{alias}")),
        other => {
            let mut reads = Vec::new();
            reads_of_expr(other, &mut reads);
            out.extend(reads.iter().map(ToString::to_string));
        }
    }
}

/// Prefix of a `Sort` key that names a `return` alias, not a column.
pub const ALIAS_KEY: &str = "alias:";

/// The bindings whose rows a sort above `id` sees: every scan, traversal
/// destination and edge binding of the pipeline under it, an anti-join's
/// inner scope excluded, name-sorted and deduplicated.
fn scope_bindings(plan: &LogicalPlan, id: LogicalId) -> Vec<String> {
    fn walk(plan: &LogicalPlan, id: LogicalId, out: &mut Vec<String>) {
        let Some(node) = plan.node(id) else {
            return;
        };
        match node {
            LogicalNode::TableScan { input, spec } => {
                out.extend(spec.binding.iter().cloned());
                if let Some(input) = input {
                    walk(plan, *input, out);
                }
            }
            LogicalNode::Expand {
                input,
                dst,
                edge_binding,
                ..
            } => {
                out.push(dst.clone());
                out.extend(edge_binding.iter().cloned());
                walk(plan, *input, out);
            }
            LogicalNode::AntiJoin { input, .. } => walk(plan, *input, out),
            other => {
                for input in other.inputs() {
                    walk(plan, input, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(plan, id, &mut out);
    out.sort();
    out.dedup();
    out
}

/// The bindings whose ids a sort appends after `keys` so its order is total;
/// none for group rows, when every returned expression is a key (equal rows
/// are indistinguishable), and never a binding whose `@id` is a key.
fn sort_tiebreak(
    bindings: &[String],
    keys: &[IROrdering],
    returns: &[IRProjection],
    aggregate: bool,
) -> Vec<String> {
    if aggregate {
        return Vec::new();
    }
    let key_texts: Vec<String> = keys
        .iter()
        .map(|key| match &key.expr {
            IRExpr::AliasRef(alias) => returns
                .iter()
                .find(|projection| projection.alias.as_deref() == Some(alias))
                .map_or_else(|| alias.clone(), |projection| projection.expr.to_string()),
            expr => expr.to_string(),
        })
        .collect();
    let covered = returns
        .iter()
        .all(|projection| key_texts.contains(&projection.expr.to_string()));
    if covered {
        return Vec::new();
    }
    bindings
        .iter()
        .filter(|binding| {
            !keys.iter().any(|key| {
                matches!(
                    &key.expr,
                    IRExpr::PropAccess { variable, property }
                        if variable == *binding && property == IDENTITY_MEMBER
                )
            })
        })
        .cloned()
        .collect()
}

fn reads_of_filter(filter: &IRFilter, out: &mut Vec<ColumnRef>) {
    let IRFilter { left, op: _, right } = filter;
    reads_of_expr(left, out);
    reads_of_expr(right, out);
}

/// Every column an expression reads. `count($v)` reads the identity alone;
/// an alias, a parameter and a literal read nothing.
fn reads_of_expr(expr: &IRExpr, out: &mut Vec<ColumnRef>) {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            out.push(ColumnRef::property(variable, property));
        }
        IRExpr::Variable(variable) => out.push(ColumnRef::entity(variable)),
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            out.push(ColumnRef::property(variable, property));
            reads_of_expr(query, out);
        }
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            reads_of_expr(field, out);
            reads_of_expr(query, out);
        }
        IRExpr::Fuzzy {
            field,
            max_edits,
            query,
        } => {
            reads_of_expr(field, out);
            reads_of_expr(query, out);
            if let Some(max_edits) = max_edits {
                reads_of_expr(max_edits, out);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            reads_of_expr(primary, out);
            reads_of_expr(secondary, out);
            if let Some(k) = k {
                reads_of_expr(k, out);
            }
        }
        IRExpr::Aggregate { func, arg } => match (func, arg.as_ref()) {
            (AggFunc::Count, IRExpr::Variable(variable)) => {
                out.push(ColumnRef::property(variable, IDENTITY_MEMBER));
            }
            _ => reads_of_expr(arg, out),
        },
        IRExpr::AliasRef(_) | IRExpr::Param(_) | IRExpr::Literal(_) => {}
    }
}

fn scan(
    plan: &mut LogicalPlan,
    side_id: SideId,
    side: &Side,
    source: &dyn PlanSource,
) -> Result<LogicalId, PlanError> {
    let schema = source.schema(side_id)?;
    Ok(plan.add(
        LogicalNode::TableScan {
            input: None,
            spec: Box::new(ScanSpec {
                side: side_id,
                table: side.table.clone(),
                version: Some(side.version),
                columns: side.columns,
                fragments: None,
                projection: None,
                filter: None,
                binding: None,
            }),
        },
        schema,
    ))
}

fn diff_over_join(
    plan: &mut LogicalPlan,
    left: LogicalId,
    right: LogicalId,
    kind: KeyJoinKind,
) -> Result<LogicalId, PlanError> {
    let joined = join_schema(
        &schema_of(plan, left)?,
        logical_prefix(plan, left),
        &schema_of(plan, right)?,
        logical_prefix(plan, right),
    );
    let join = plan.add(
        LogicalNode::Join {
            left,
            right,
            kind,
            on: LOGICAL_ID.to_string(),
        },
        joined.clone(),
    );
    Ok(plan.add(
        LogicalNode::RowDiff {
            input: join,
            address_short_circuit: false,
        },
        diff_schema(&joined),
    ))
}

/// The side a logical node's columns belong to, when it is one scan; a join
/// input that is itself a join already carries prefixed columns.
fn logical_prefix(plan: &LogicalPlan, id: LogicalId) -> Option<&'static str> {
    match plan.node(id) {
        Some(LogicalNode::TableScan { input: None, spec }) => Some(spec.side.name()),
        _ => None,
    }
}

fn physical_prefix(plan: &PhysicalPlan, id: NodeId) -> Option<&str> {
    match plan.node(id) {
        Some(PhysicalNode::Scan {
            source: ScanInput::Table,
            spec,
            ..
        }) => Some(spec.binding.as_deref().unwrap_or(spec.side.name())),
        _ => None,
    }
}

/// A scan's full schema: the node type's for a `match` binding, the side's
/// for a diff or merge cursor.
fn scan_schema(spec: &ScanSpec, source: &dyn PlanSource) -> Result<SchemaRef, PlanError> {
    if spec.binding.is_some() {
        return Ok(node_type_of(spec, source)?.schema);
    }
    source.schema(spec.side)
}

/// The node type a bound scan reads.
fn node_type_of(spec: &ScanSpec, source: &dyn PlanSource) -> Result<NodeTypeSpec, PlanError> {
    let type_name = spec.table.node_type_name().ok_or_else(|| {
        PlanError::Internal(format!(
            "a bound scan reads `{}`, which is no node table",
            spec.table.type_key
        ))
    })?;
    source.node_type(type_name)
}

/// Two joined rows side by side: the left fields then the right fields, each
/// prefixed by its side so one logical id column can appear on every side.
/// A `None` prefix marks an input whose columns are already prefixed.
pub fn join_schema(
    left: &SchemaRef,
    left_prefix: Option<&str>,
    right: &SchemaRef,
    right_prefix: Option<&str>,
) -> SchemaRef {
    let mut fields = Vec::with_capacity(left.fields().len() + right.fields().len());
    for (schema, prefix) in [(left, left_prefix), (right, right_prefix)] {
        for field in schema.fields() {
            let name = match prefix {
                Some(prefix) => format!("{prefix}.{}", field.name()),
                None => field.name().clone(),
            };
            fields.push(Field::new(name, field.data_type().clone(), true));
        }
    }
    Arc::new(Schema::new(fields))
}

/// A joined row plus the change operation the comparison assigned it.
pub fn diff_schema(joined: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = joined
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new("_op", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

/// The selected row image under the table's own column names, the merge
/// outcome the classification assigned its id, and the side the image came
/// from.
pub fn classify_schema(base: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = base
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new("_outcome", DataType::Utf8, false));
    fields.push(Field::new("_side", DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

/// Decoded bytes per row the schema fixes: primitive and fixed-size-list
/// columns count their width, a Boolean one byte, variable-width columns
/// zero; callers take the larger of this and a data file's on-disk size.
pub fn fixed_row_width_bytes(schema: &SchemaRef) -> u64 {
    schema
        .fields()
        .iter()
        .map(|field| fixed_width_of(field.data_type()))
        .sum()
}

fn fixed_width_of(data_type: &DataType) -> u64 {
    match data_type {
        DataType::Boolean => 1,
        DataType::FixedSizeList(child, len) => {
            fixed_width_of(child.data_type()).saturating_mul(u64::try_from(*len).unwrap_or(0))
        }
        DataType::FixedSizeBinary(len) => u64::try_from(*len).unwrap_or(0),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| fixed_width_of(field.data_type()))
            .sum(),
        other => other
            .primitive_width()
            .map_or(0, |width| u64::try_from(width).unwrap_or(0)),
    }
}

fn key_schema(spec: &ScanSpec) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(spec.columns.id, DataType::Utf8, false),
        Field::new(ROW_ID, DataType::UInt64, false),
        Field::new(ROW_ADDR, DataType::UInt64, false),
    ]))
}

fn is_key_column(name: &str, spec: &ScanSpec) -> bool {
    name == spec.columns.id || name == ROW_ID || name == ROW_ADDR
}

/// Run stages 1 to 3 over a resolved plan.
pub fn optimize(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    bounds: &Bounds,
) -> Result<Optimized, PlanError> {
    let fired = rewrite(plan, source)?;
    physical_plan(plan, source, bounds, fired)
}

/// Stage 1: the logical rewrites. The gate runs them for every operation,
/// registered or not, so an explain on the executor route shows the plan
/// the executor obeys and the passes that shaped it.
pub fn rewrite(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
) -> Result<Vec<&'static str>, PlanError> {
    let mut fired = vec![PASS_RESOLVE];
    let query = is_query_plan(plan);
    let pushed = if query {
        place_query_filters(plan, source)
    } else {
        resume_pushdown(plan)
    };
    if pushed {
        fired.push(PASS_PREDICATE_PUSHDOWN);
    }
    if query && aggregate_pushdown(plan)? {
        fired.push(PASS_AGGREGATE_PUSHDOWN);
    }
    if query && projection_pushdown(plan, source)? {
        fired.push(PASS_PROJECTION_PUSHDOWN);
    }
    Ok(fired)
}

/// A GQ query plan: at least one scan bound to a `match` variable. Diff and
/// merge plans bind none.
fn is_query_plan(plan: &LogicalPlan) -> bool {
    plan.live().any(|(_, node)| match node {
        LogicalNode::TableScan { spec, .. } => spec.binding.is_some(),
        LogicalNode::MetadataCount { .. } => true,
        _ => false,
    })
}

/// Stages 2 and 3: physical selection and property derivation over a
/// rewritten plan; `fired` carries stage 1's record forward.
pub fn physical_plan(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    bounds: &Bounds,
    mut fired: Vec<&'static str>,
) -> Result<Optimized, PlanError> {
    let query = is_query_plan(plan);
    let before = plan
        .schema(plan.root())
        .cloned()
        .ok_or_else(|| PlanError::Internal("resolved plan root has no schema".to_string()))?;
    if fragment_scope(plan, source) {
        fired.push(PASS_FRAGMENT_SCOPE);
    }
    if address_short_circuit(plan, source) {
        fired.push(PASS_ADDRESS_SHORT_CIRCUIT);
    }
    let mut lowering = Lowering {
        logical: plan,
        source,
        bounds,
        physical: PhysicalPlan::new(),
        late_materialization: false,
        join_algorithm: false,
        expand_mode: false,
        access_path: false,
        csr_cached: false,
        decisions: Vec::new(),
        ranking: None,
        limit: None,
    };
    let root = lowering.lower(plan.root())?;
    let Lowering {
        mut physical,
        late_materialization,
        join_algorithm,
        expand_mode,
        access_path,
        decisions,
        ..
    } = lowering;
    physical.set_root(root);
    if late_materialization {
        fired.push(PASS_LATE_MATERIALIZATION);
    }
    if join_algorithm {
        fired.push(PASS_JOIN_ALGORITHM);
    }
    if expand_mode {
        fired.push(PASS_EXPAND_MODE);
    }
    if access_path {
        fired.push(PASS_ACCESS_PATH);
    }
    let mut statistics = decisions;
    statistics.extend(derive_properties(&mut physical, source, bounds)?);
    let after = physical
        .properties(physical.root())
        .map(|properties| properties.schema.clone())
        .ok_or_else(|| PlanError::Internal("physical root has no properties".to_string()))?;
    if !query {
        root_shape_kept(&before, &after)?;
    }
    Ok(Optimized {
        physical,
        fired,
        statistics,
    })
}

/// The nodes of the pipeline under `root` that constrain its rows: every
/// node reachable from `root` without entering an `AntiJoin`'s inner tree.
fn top_level(plan: &PhysicalPlan, root: NodeId) -> Vec<NodeId> {
    let mut nodes = Vec::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        let Some(node) = plan.node(id) else {
            continue;
        };
        nodes.push(id);
        match node {
            PhysicalNode::AntiJoin { input, .. } => pending.push(*input),
            other => pending.extend(other.inputs()),
        }
    }
    nodes
}

/// Diff lowering keeps the root schema; a merge derives both sides from
/// `classify_schema`, so the check binds diff plans only. A query plan's
/// schemas are the engine's to derive at run time, so the caller skips it.
fn root_shape_kept(before: &SchemaRef, after: &SchemaRef) -> Result<(), PlanError> {
    if !same_shape(before, after) {
        return Err(PlanError::Internal(
            "optimizer changed the plan's output schema".to_string(),
        ));
    }
    Ok(())
}

fn same_shape(left: &SchemaRef, right: &SchemaRef) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(l, r)| l.name() == r.name() && l.data_type() == r.data_type())
}

/// Stage 1, pass 2 on a diff plan: a `Limit`'s resume key becomes an
/// `IdAfter` filter on every scan beneath it. A diff or merge plan holds no
/// `Filter` node, so nothing else moves.
fn resume_pushdown(plan: &mut LogicalPlan) -> bool {
    let mut fired = false;
    let resume = plan.live().find_map(|(_, node)| match node {
        LogicalNode::Page {
            resume: Some(id), ..
        } => Some(id.clone()),
        _ => None,
    });
    if let Some(id) = resume {
        let scans: Vec<LogicalId> = plan
            .live()
            .filter(|(_, node)| matches!(node, LogicalNode::TableScan { .. }))
            .map(|(id, _)| id)
            .collect();
        for scan in scans {
            if let Some(LogicalNode::TableScan { spec, .. }) = plan.node_mut(scan) {
                spec.filter = Some(and_filter(
                    spec.filter.take(),
                    Predicate::IdAfter { id: id.clone() },
                ));
                fired = true;
            }
        }
    }
    fired
}

fn and_filter(existing: Option<Predicate>, added: Predicate) -> Predicate {
    match existing {
        Some(existing) => existing.and(added),
        None => added,
    }
}

/// Stage 1, pass 2 on a query plan, per scope (the top-level tree and each
/// `not { … }` inner tree on its own): every filter `placement_target`
/// places moves into its scan; the rest stay where the query wrote them.
fn place_query_filters(plan: &mut LogicalPlan, source: &dyn PlanSource) -> bool {
    let mut fired = false;
    let mut scopes = vec![plan.root()];
    while let Some(root) = scopes.pop() {
        let mut filters = Vec::new();
        let mut scans: HashMap<String, LogicalId> = HashMap::new();
        let mut dependent_scans: HashMap<String, LogicalId> = HashMap::new();
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            match plan.node(id) {
                Some(LogicalNode::Filter { input, .. }) => {
                    filters.push(id);
                    stack.push(*input);
                }
                Some(LogicalNode::TableScan { input, spec }) => {
                    if let Some(binding) = &spec.binding {
                        if input.is_some() {
                            dependent_scans.insert(binding.clone(), id);
                        } else {
                            scans.insert(binding.clone(), id);
                        }
                    }
                    stack.extend(*input);
                }
                Some(LogicalNode::AntiJoin { input, inner, .. }) => {
                    scopes.push(*inner);
                    stack.push(*input);
                }
                Some(node) => stack.extend(node.inputs()),
                None => {}
            }
        }
        filters.sort_unstable();
        for filter_id in filters {
            let Some(LogicalNode::Filter { input, predicate }) = plan.node(filter_id).cloned()
            else {
                continue;
            };
            let Some(filter) = predicate.single_gq() else {
                continue;
            };
            let target = match placement_target(filter, &scans, &dependent_scans, source) {
                Some(target) => target,
                None => continue,
            };
            let Some(LogicalNode::TableScan { spec, .. }) = plan.node_mut(target) else {
                continue;
            };
            spec.filter = Some(and_filter(spec.filter.take(), predicate.clone()));
            plan.splice_out(filter_id, input);
            fired = true;
        }
    }
    fired
}

/// Where one filter of a scope goes, if anywhere: a search filter to the scan
/// of its field's binding, including exact membership on dependent scans;
/// a scalar filter on one binding goes to the read that can evaluate it.
fn placement_target(
    filter: &IRFilter,
    scans: &HashMap<String, LogicalId>,
    dependent_scans: &HashMap<String, LogicalId>,
    source: &dyn PlanSource,
) -> Option<LogicalId> {
    if let Some(field) = search_filter_field(filter) {
        let IRExpr::PropAccess { variable, .. } = field else {
            return None;
        };
        let mut reads = Vec::new();
        reads_of_filter(filter, &mut reads);
        if reads.iter().any(|read| read.binding != *variable) {
            return None;
        }
        return scans.get(variable).copied().or_else(|| {
            dependent_scan_filter_pushable(filter, variable, source)
                .then(|| dependent_scans.get(variable).copied())
                .flatten()
        });
    }
    let mut reads = Vec::new();
    reads_of_filter(filter, &mut reads);
    let mut bindings: Vec<&str> = reads.iter().map(|read| read.binding.as_str()).collect();
    bindings.sort_unstable();
    bindings.dedup();
    let [binding] = bindings.as_slice() else {
        return None;
    };
    if !source.filter_pushable(filter) {
        return None;
    }
    scans
        .get(*binding)
        .or_else(|| dependent_scans.get(*binding))
        .copied()
}

/// A dependent scan evaluates one-binding scalar predicates and exact text
/// membership. Fuzzy and ranked retrieval retain their existing execution
/// contract; a dependent read must not create a top-k window per input batch.
fn dependent_scan_filter_pushable(
    filter: &IRFilter,
    binding: &str,
    source: &dyn PlanSource,
) -> bool {
    let mut reads = Vec::new();
    reads_of_filter(filter, &mut reads);
    if reads.iter().any(|read| read.binding != binding) {
        return false;
    }
    match &filter.left {
        IRExpr::Search { field, .. } | IRExpr::MatchText { field, .. } => {
            matches!(field.as_ref(), IRExpr::PropAccess { variable, .. } if variable == binding)
        }
        IRExpr::Fuzzy { .. } => false,
        _ => source.filter_pushable(filter),
    }
}

/// The field of a full-text filter (`search`, `fuzzy`, `match_text`).
fn search_filter_field(filter: &IRFilter) -> Option<&IRExpr> {
    match &filter.left {
        IRExpr::Search { field, .. }
        | IRExpr::Fuzzy { field, .. }
        | IRExpr::MatchText { field, .. } => Some(field.as_ref()),
        _ => None,
    }
}

fn aggregate_pushdown(plan: &mut LogicalPlan) -> Result<bool, PlanError> {
    let candidates: Vec<_> = plan
        .live()
        .filter_map(|(id, node)| {
            let LogicalNode::Aggregate {
                input,
                return_exprs,
                ..
            } = node
            else {
                return None;
            };
            let Some(LogicalNode::TableScan { input: None, spec }) = plan.node(*input) else {
                return None;
            };
            let Some(binding) = &spec.binding else {
                return None;
            };
            if spec.filter.is_some()
                || spec.fragments.is_some()
                || return_exprs.is_empty()
                || plan.parent_of(*input) != Some(id)
            {
                return None;
            }
            let eligible = return_exprs.iter().all(|projection| {
                matches!(
                    &projection.expr,
                    IRExpr::Aggregate { func: AggFunc::Count, arg }
                        if matches!(arg.as_ref(), IRExpr::Variable(variable) if variable == binding)
                )
            });
            eligible.then(|| (id, *input, spec.clone(), return_exprs.clone()))
        })
        .collect();
    let changed = !candidates.is_empty();
    for (aggregate, scan, spec, return_exprs) in candidates {
        let schema = metadata_count_schema(&return_exprs)?;
        let count = plan.add(LogicalNode::MetadataCount { spec, return_exprs }, schema);
        plan.splice_out(aggregate, count);
        plan.splice_out(scan, count);
    }
    Ok(changed)
}

fn metadata_count_schema(return_exprs: &[IRProjection]) -> Result<SchemaRef, PlanError> {
    let fields = return_exprs
        .iter()
        .map(|projection| {
            let IRExpr::Aggregate {
                func: AggFunc::Count,
                arg,
            } = &projection.expr
            else {
                return Err(PlanError::Internal(
                    "metadata count requires count aggregates".to_string(),
                ));
            };
            let IRExpr::Variable(variable) = arg.as_ref() else {
                return Err(PlanError::Internal(
                    "metadata count requires a node binding".to_string(),
                ));
            };
            Ok(Field::new(
                projection.alias.as_ref().unwrap_or(variable),
                DataType::Int64,
                false,
            ))
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Project independent and dependent query scans from binding demand; the id
/// column is a demand like any other (`@id`, a whole entity, `identity_reads`).
/// A diff or merge plan holds no `Projection` node and never reaches this pass.
fn projection_pushdown(plan: &mut LogicalPlan, source: &dyn PlanSource) -> Result<bool, PlanError> {
    let scans = plan
        .live()
        .filter_map(|(id, node)| match node {
            LogicalNode::TableScan { spec, .. } => spec
                .binding
                .clone()
                .map(|binding| Ok((id, binding, node_type_of(spec, source)?))),
            _ => None,
        })
        .collect::<Result<Vec<(LogicalId, String, NodeTypeSpec)>, PlanError>>()?;
    if scans.is_empty() {
        return Ok(false);
    }
    let mut demands: HashMap<String, Demand> = HashMap::new();
    for (_, node) in plan.live() {
        for read in node_reads(node) {
            let demand = demands.entry(read.binding.clone()).or_default();
            match read.property.as_deref() {
                None => demand.entity = true,
                Some(IDENTITY_MEMBER) => demand.identity = true,
                Some(property) => {
                    demand.properties.insert(property.to_string());
                }
            }
        }
        for binding in identity_reads(node) {
            demands.entry(binding).or_default().identity = true;
        }
    }
    for (id, binding, node_type) in scans {
        let demand = demands.get(&binding);
        let whole_object = demand.map_or_else(
            || {
                matches!(
                    plan.node(id),
                    Some(LogicalNode::TableScan { input: None, .. })
                )
            },
            |demand| demand.entity,
        );
        let identity = whole_object || demand.is_some_and(|demand| demand.identity);
        let is_key = |name: &str| node_type.key.iter().any(|key| key == name);
        let is_object = |name: &str| node_type.object_columns.iter().any(|column| column == name);
        let named = |name: &str| demand.is_some_and(|demand| demand.properties.contains(name));
        let projection: Vec<String> = node_type
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| {
                (identity && *name == node_type.columns.id)
                    || is_key(name)
                    || (whole_object && is_object(name))
                    || named(name)
            })
            .map(str::to_string)
            .collect();
        match plan.node_mut(id) {
            Some(LogicalNode::TableScan { spec, .. }) => {
                spec.projection = Some(projection);
            }
            _ => unreachable!("projection targets are scans"),
        }
    }
    Ok(true)
}

/// What one binding's scan must read, gathered from every node of the tree.
/// `identity` is the id column, read only when something keys rows by it.
#[derive(Debug, Default)]
struct Demand {
    entity: bool,
    identity: bool,
    properties: HashSet<String>,
}

/// The bindings whose id a node reads beside any column: a traversal's ends,
/// a dependent scan, an anti-join's outer rows, a ranked scan's binding, and
/// a sort's declared tie-breaks.
fn identity_reads(node: &LogicalNode) -> Vec<String> {
    match node {
        LogicalNode::TableScan {
            input: Some(_),
            spec,
        } => spec.binding.iter().cloned().collect(),
        LogicalNode::Expand { src, dst, .. } => vec![src.clone(), dst.clone()],
        LogicalNode::AntiJoin { outer_var, .. } => vec![outer_var.clone()],
        LogicalNode::Nearest { binding, .. } | LogicalNode::TextSearch { binding, .. } => {
            vec![binding.clone()]
        }
        LogicalNode::Sort { tiebreak, .. } => tiebreak.clone(),
        _ => Vec::new(),
    }
}

/// The columns one node reads through query bindings. A `RankFuse` reads its
/// targets whole in both arms: the arms' batches concatenate under one
/// schema, so a pruned target column in one arm would break the other.
fn node_reads(node: &LogicalNode) -> Vec<ColumnRef> {
    let mut out = Vec::new();
    match node {
        LogicalNode::TableScan { input: _, spec } => {
            let ScanSpec {
                side: _,
                table: _,
                version: _,
                columns: _,
                fragments: _,
                projection: _,
                filter,
                binding: _,
            } = spec.as_ref();
            if let Some(predicate) = filter {
                predicate_reads(predicate, &mut out);
            }
        }
        LogicalNode::Filter {
            input: _,
            predicate,
        } => predicate_reads(predicate, &mut out),
        LogicalNode::Projection {
            input: _,
            reads,
            return_exprs: _,
        } => {
            out.extend(reads.iter().cloned());
        }
        LogicalNode::Sort {
            input: _,
            keys,
            order_by: _,
            fetch: _,
            tiebreak: _,
        } => out.extend(
            keys.iter()
                .filter(|key| !key.starts_with(ALIAS_KEY))
                .map(|key| ColumnRef::parse(key)),
        ),
        LogicalNode::Nearest {
            input: _,
            binding: _,
            property: _,
            query: _,
            k: _,
            reads,
        }
        | LogicalNode::TextSearch {
            input: _,
            binding: _,
            property: _,
            query: _,
            reads,
        }
        | LogicalNode::Aggregate {
            input: _,
            reads,
            return_exprs: _,
        } => out.extend(reads.iter().cloned()),
        LogicalNode::RankFuse {
            input: _,
            arms,
            k: _,
            limit: _,
            reads,
        } => {
            out.extend(arms.iter().map(|arm| ColumnRef::entity(&arm.binding)));
            out.extend(reads.iter().cloned());
        }
        LogicalNode::MetadataCount {
            spec: _,
            return_exprs: _,
        }
        | LogicalNode::Expand {
            input: _,
            src: _,
            dst: _,
            edge_type: _,
            direction: _,
            dst_type: _,
            min_hops: _,
            max_hops: _,
            edge_binding: _,
        }
        | LogicalNode::AntiJoin {
            input: _,
            inner: _,
            outer_var: _,
        }
        | LogicalNode::OuterReference { outer_var: _ }
        | LogicalNode::Join {
            left: _,
            right: _,
            kind: _,
            on: _,
        }
        | LogicalNode::Limit { input: _, rows: _ }
        | LogicalNode::Page {
            input: _,
            rows: _,
            bytes: _,
            resume: _,
        }
        | LogicalNode::Ordered { input: _, keys: _ }
        | LogicalNode::CrossJoin { left: _, right: _ }
        | LogicalNode::RowDiff {
            input: _,
            address_short_circuit: _,
        }
        | LogicalNode::MergeClassify {
            base: _,
            source: _,
            target: _,
        } => {}
    }
    out
}

fn predicate_reads(predicate: &Predicate, out: &mut Vec<ColumnRef>) {
    match predicate {
        Predicate::Gq {
            reads,
            text: _,
            filter: _,
        } => out.extend(reads.iter().cloned()),
        Predicate::And { left, right } => {
            predicate_reads(left, out);
            predicate_reads(right, out);
        }
        Predicate::IdAfter { .. } | Predicate::VersionWindow { .. } => {}
    }
}

/// Stage 2, pass 4. With an adjacency proof, the `LeftOuter` diff's child scan
/// reads only the fragments the transaction wrote, filtered to the version
/// window, and its parent scan only the fragments the transaction touched.
fn fragment_scope(plan: &mut LogicalPlan, source: &dyn PlanSource) -> bool {
    let Some(proof) = source.adjacency_proof() else {
        return false;
    };
    let scoped_join = plan.live().find_map(|(_, node)| match node {
        LogicalNode::Join {
            left,
            right,
            kind: KeyJoinKind::LeftOuter,
            ..
        } => Some((*left, *right)),
        _ => None,
    });
    let Some((left, right)) = scoped_join else {
        return false;
    };
    let mut fired = false;
    for id in [left, right] {
        if let Some(LogicalNode::TableScan { spec, .. }) = plan.node_mut(id) {
            match spec.side {
                SideId::Child => {
                    spec.fragments = Some(proof.child_fragments.clone());
                    spec.filter = Some(and_filter(
                        spec.filter.take(),
                        Predicate::VersionWindow {
                            from: proof.version_window.0,
                            to: proof.version_window.1,
                        },
                    ));
                    fired = true;
                }
                SideId::Parent => {
                    spec.fragments = Some(proof.parent_fragments.clone());
                    fired = true;
                }
                SideId::Base | SideId::Binding(_) => {}
            }
        }
    }
    fired
}

/// Stage 2, pass 5. With an adjacency proof, a matched id with one `_rowaddr`
/// on both sides is unchanged; the `RowDiff` records that and the lowering
/// hands it to the join beneath, which drops such pairs before hydration.
fn address_short_circuit(plan: &mut LogicalPlan, source: &dyn PlanSource) -> bool {
    if source.adjacency_proof().is_none() {
        return false;
    }
    let diffs: Vec<LogicalId> = plan
        .live()
        .filter(|(_, node)| matches!(node, LogicalNode::RowDiff { .. }))
        .map(|(id, _)| id)
        .collect();
    let mut fired = false;
    for id in diffs {
        if let Some(LogicalNode::RowDiff {
            address_short_circuit,
            ..
        }) = plan.node_mut(id)
        {
            *address_short_circuit = true;
            fired = true;
        }
    }
    fired
}

/// Stage 2. Selects one physical node per logical node, applying late
/// materialization (pass 6) at every `Join` and the join algorithm rule
/// (pass 7): a fragment-scoped right input builds, everything else streams.
struct Lowering<'a> {
    logical: &'a LogicalPlan,
    source: &'a dyn PlanSource,
    bounds: &'a Bounds,
    physical: PhysicalPlan,
    late_materialization: bool,
    join_algorithm: bool,
    /// Pass 8 fired: an `Expand`'s mode came from the cost model.
    expand_mode: bool,
    /// Pass 9 fired: a dependent scan's access path came from the cost model.
    access_path: bool,
    /// An earlier lowered node of this plan realizes the query's CSR (an
    /// `Expand` in `Csr` mode or an `AntiJoin`), so a later `Expand` reuses
    /// it for free. Lowering order is execution order (inputs first).
    csr_cached: bool,
    decisions: Vec<StatisticSource>,
    /// The ranking lowered below the node being lowered, if any.
    ranking: Option<Ranking>,
    /// The `limit` above the node being lowered: the fetch of the score sort.
    limit: Option<usize>,
}

/// What a leading search function became in the physical plan: the score
/// keys the query's `Sort` leads with, or a fusion, which orders its own rows.
enum Ranking {
    Scores(Vec<IROrdering>),
    Fused,
}

impl Lowering<'_> {
    /// Mark the scan of `binding` under `root` as ranked by `access`, and
    /// return the score ordering it imposes with the ranked scan's id.
    fn rank(
        &mut self,
        root: NodeId,
        binding: &str,
        access: RankedAccess,
    ) -> Result<(IROrdering, NodeId), PlanError> {
        let ordering = access.ordering(binding);
        let scan = self.ranked_scan(root, binding)?;
        let destination = matches!(
            self.physical.node(scan),
            Some(PhysicalNode::Scan {
                source: ScanInput::Dependent { .. },
                ..
            })
        ) || self.physical.subtree(root).into_iter().any(|id| {
            matches!(self.physical.node(id), Some(PhysicalNode::HashJoin { build, .. }) if *build == scan)
        });
        if destination {
            let function = match access.kind {
                RankKind::Nearest => "nearest",
                RankKind::Bm25 => "bm25",
            };
            return Err(PlanError::Unsupported {
                detail: format!(
                    "`{function}()` orders `${binding}`, a traversal destination, which engine v2 does not support; order on the traversal's source binding, or match the destination with search()"
                ),
            });
        }
        match self.physical.node_mut(scan) {
            Some(PhysicalNode::Scan { ranked, .. }) if ranked.is_none() => {
                *ranked = Some(access);
                Ok((ordering, scan))
            }
            _ => Err(PlanError::Internal(format!(
                "the scan of `${binding}` is ranked twice"
            ))),
        }
    }

    /// The one scan of `binding` under `root`.
    fn ranked_scan(&self, root: NodeId, binding: &str) -> Result<NodeId, PlanError> {
        let scans: Vec<NodeId> = self
            .physical
            .subtree(root)
            .into_iter()
            .filter(|id| {
                matches!(
                    self.physical.node(*id),
                    Some(PhysicalNode::Scan { spec, .. }) if spec.binding.as_deref() == Some(binding)
                )
            })
            .collect();
        let [scan] = scans[..] else {
            return Err(PlanError::Internal(format!(
                "the ranked binding `${binding}` has {} scans under its search node",
                scans.len()
            )));
        };
        Ok(scan)
    }

    /// The pre-pass the top-level nodes `top` admit for `binding` (scanned by
    /// `scan`, feeding `feeds`): the `Expand`s leaving it with `min_hops > 0`,
    /// none when an `Expand` introduces it (the gate's shape fence).
    fn prefilter(
        &self,
        top: &[NodeId],
        binding: &str,
        scan: NodeId,
        feeds: Vec<NodeId>,
    ) -> Result<Prefilter, PlanError> {
        let ranked_type = match self.physical.node(scan) {
            Some(PhysicalNode::Scan { spec, .. }) => spec.table.node_type_name(),
            _ => None,
        }
        .ok_or_else(|| {
            PlanError::Internal(format!(
                "the ranked binding `${binding}` scans no node table"
            ))
        })?
        .to_string();
        let mut introduced_by_scan = false;
        let mut hops = Vec::new();
        for &id in top {
            match self.physical.node(id) {
                Some(PhysicalNode::Scan { spec, .. })
                    if spec.binding.as_deref() == Some(binding) =>
                {
                    introduced_by_scan = true;
                }
                Some(PhysicalNode::Expand { dst, .. }) if dst == binding => {
                    hops.clear();
                    introduced_by_scan = false;
                    break;
                }
                Some(PhysicalNode::Expand {
                    src,
                    edge_type,
                    direction,
                    min_hops,
                    ..
                }) if src == binding && *min_hops > 0 => hops.push(Hop {
                    edge_type: edge_type.clone(),
                    direction: *direction,
                }),
                _ => {}
            }
        }
        if !introduced_by_scan || feeds.is_empty() {
            hops.clear();
        }
        Ok(Prefilter {
            ranked_type,
            hops,
            feeds,
        })
    }

    /// The `Sort` a query's return runs under: the user's keys, led by the
    /// score keys of a ranking below; `None` where nothing runs it (a fusion
    /// orders its own rows, an aggregate carries no score column).
    fn sort_keys(&self, input: NodeId, order_by: &[IROrdering]) -> Option<Vec<IROrdering>> {
        match &self.ranking {
            None => Some(order_by.to_vec()),
            Some(Ranking::Fused) => None,
            Some(Ranking::Scores(_))
                if matches!(
                    self.physical.node(input),
                    Some(PhysicalNode::Aggregate { .. })
                ) =>
            {
                None
            }
            Some(Ranking::Scores(scores)) => {
                let mut keys = scores.clone();
                keys.extend(order_by.iter().cloned());
                Some(keys)
            }
        }
    }

    fn lower(&mut self, id: LogicalId) -> Result<NodeId, PlanError> {
        let logical = self.logical;
        let node = logical
            .node(id)
            .ok_or_else(|| PlanError::Internal(format!("logical node {id} is a tombstone")))?;
        match node {
            LogicalNode::MetadataCount { spec, return_exprs } => {
                Ok(self.physical.add(PhysicalNode::MetadataCount {
                    spec: spec.clone(),
                    return_exprs: return_exprs.clone(),
                }))
            }
            LogicalNode::TableScan { input, spec } => {
                let scan = |source| PhysicalNode::Scan {
                    source,
                    spec: spec.clone(),
                    ordered: false,
                    keys_only: false,
                    ranked: None,
                };
                let Some(input) = input else {
                    return Ok(self.physical.add(scan(ScanInput::Table)));
                };
                let access = self.access_path(*input, spec)?;
                let probe = self.lower(*input)?;
                match access {
                    AccessPath::IdLookup => Ok(self
                        .physical
                        .add(scan(ScanInput::Dependent { input: probe }))),
                    AccessPath::HashJoin => {
                        let binding = spec.binding.clone().ok_or_else(|| {
                            PlanError::Internal(
                                "a hash join's destination scan has no binding".to_string(),
                            )
                        })?;
                        let build = self.physical.add(scan(ScanInput::Table));
                        Ok(self.physical.add(PhysicalNode::HashJoin {
                            probe,
                            build,
                            binding,
                            fallback: access.declared_fallback(),
                        }))
                    }
                }
            }
            LogicalNode::Sort {
                input,
                order_by,
                fetch,
                tiebreak,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let Some(order_by) = self.sort_keys(lowered, order_by) else {
                    return Ok(lowered);
                };
                Ok(self.physical.add(PhysicalNode::Sort {
                    input: lowered,
                    order_by,
                    fetch: *fetch,
                    tiebreak: tiebreak.clone(),
                }))
            }
            LogicalNode::CrossJoin { left, right } => {
                let left_lowered = self.lower(*left)?;
                let right_lowered = self.lower(*right)?;
                Ok(self.physical.add(PhysicalNode::CrossJoin {
                    left: left_lowered,
                    right: right_lowered,
                }))
            }
            LogicalNode::Filter { input, predicate } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Filter {
                    input: lowered,
                    filters: predicate.gq_filters(),
                }))
            }
            LogicalNode::Projection {
                input,
                return_exprs,
                ..
            } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Projection {
                    input: lowered,
                    return_exprs: return_exprs.clone(),
                }))
            }
            LogicalNode::Aggregate {
                input,
                return_exprs,
                ..
            } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Aggregate {
                    input: lowered,
                    return_exprs: return_exprs.clone(),
                }))
            }
            LogicalNode::Expand {
                input,
                src,
                dst,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let (mode, frontier_estimate, policy) =
                    self.expand_mode(*input, edge_type, *direction, *min_hops, *max_hops);
                Ok(self.physical.add(PhysicalNode::Expand {
                    input: lowered,
                    src: src.clone(),
                    dst: dst.clone(),
                    edge_type: edge_type.clone(),
                    direction: *direction,
                    dst_type: dst_type.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    edge_binding: edge_binding.clone(),
                    mode,
                    frontier_estimate,
                    policy,
                    version: self.source.edge_dataset(edge_type).map(|pin| pin.version),
                }))
            }
            LogicalNode::AntiJoin {
                input,
                inner,
                outer_var,
            } => {
                let lowered = self.lower(*input)?;
                let inner_lowered = self.lower(*inner)?;
                self.csr_cached = true;
                Ok(self.physical.add(PhysicalNode::AntiJoin {
                    input: lowered,
                    inner: inner_lowered,
                    outer_var: outer_var.clone(),
                }))
            }
            LogicalNode::OuterReference { outer_var } => {
                Ok(self.physical.add(PhysicalNode::OuterReference {
                    outer_var: outer_var.clone(),
                }))
            }
            LogicalNode::Nearest {
                input,
                binding,
                property,
                query,
                k,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let fetch = k.and_then(|k| usize::try_from(k).ok());
                let access = RankedAccess {
                    kind: RankKind::Nearest,
                    property: property.clone(),
                    query: query.clone(),
                    fetch,
                    nprobes: self.source.ann_nprobes(),
                    scope: RankScope::Order,
                    overfetch: fetch.map(OverfetchRung::ladder).unwrap_or_default(),
                    prefilter: None,
                };
                let (score, scan) = self.rank(lowered, binding, access)?;
                let top = top_level(&self.physical, lowered);
                let expanded_from = top.iter().any(|id| {
                    matches!(self.physical.node(*id), Some(PhysicalNode::Expand { src, .. }) if src == binding)
                });
                if expanded_from {
                    let prefilter = self.prefilter(&top, binding, scan, vec![scan])?;
                    if let Some(PhysicalNode::Scan {
                        ranked: Some(ranked),
                        ..
                    }) = self.physical.node_mut(scan)
                    {
                        ranked.prefilter = Some(prefilter);
                    }
                }
                self.ranking = Some(Ranking::Scores(vec![score]));
                Ok(lowered)
            }
            LogicalNode::TextSearch {
                input,
                binding,
                property,
                query,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let access = RankedAccess {
                    kind: RankKind::Bm25,
                    property: property.clone(),
                    query: query.clone(),
                    fetch: None,
                    nprobes: None,
                    scope: RankScope::Order,
                    overfetch: Vec::new(),
                    prefilter: None,
                };
                let (score, _) = self.rank(lowered, binding, access)?;
                self.ranking = Some(Ranking::Scores(vec![score]));
                Ok(lowered)
            }
            LogicalNode::RankFuse {
                input,
                arms,
                k,
                limit,
                ..
            } => {
                let primary = self.lower(*input)?;
                let secondary = self.physical.duplicate(primary).ok_or_else(|| {
                    PlanError::Internal("the rrf arm subtree has a tombstone".to_string())
                })?;
                let limit = limit.and_then(|limit| usize::try_from(limit).ok());
                let [primary_arm, secondary_arm] = arms;
                let mut lowered_arms = Vec::with_capacity(2);
                let mut bm25_scans = Vec::new();
                let mut primary_scan = None;
                for (root, arm, scope) in [
                    (primary, primary_arm, RankScope::Primary),
                    (secondary, secondary_arm, RankScope::Secondary),
                ] {
                    let access = RankedAccess {
                        kind: arm.kind,
                        property: arm.property.clone(),
                        query: arm.query.clone(),
                        fetch: match arm.kind {
                            RankKind::Nearest => Some(limit.unwrap_or(RRF_NEAREST_ARM_K)),
                            RankKind::Bm25 => None,
                        },
                        nprobes: match arm.kind {
                            RankKind::Nearest => self.source.ann_nprobes(),
                            RankKind::Bm25 => None,
                        },
                        scope,
                        overfetch: Vec::new(),
                        prefilter: None,
                    };
                    let (_, scan) = self.rank(root, &arm.binding, access)?;
                    if arm.kind == RankKind::Bm25 {
                        bm25_scans.push(scan);
                    }
                    primary_scan.get_or_insert(scan);
                    lowered_arms.push(RankArm {
                        input: root,
                        binding: arm.binding.clone(),
                        kind: arm.kind,
                    });
                }
                let feeds = if primary_arm.binding == secondary_arm.binding {
                    bm25_scans
                } else {
                    Vec::new()
                };
                let primary_scan = primary_scan
                    .ok_or_else(|| PlanError::Internal("an rrf has two arms".to_string()))?;
                let prefilter = self.prefilter(
                    &top_level(&self.physical, primary),
                    &primary_arm.binding,
                    primary_scan,
                    feeds,
                )?;
                let [primary_arm, secondary_arm] = <[RankArm; 2]>::try_from(lowered_arms)
                    .map_err(|_| PlanError::Internal("an rrf has two arms".to_string()))?;
                self.ranking = Some(Ranking::Fused);
                Ok(self.physical.add(PhysicalNode::RankFuse {
                    arms: [primary_arm, secondary_arm],
                    k: k.clone(),
                    limit,
                    prefilter,
                }))
            }
            LogicalNode::Ordered { input, keys } => {
                let lowered = self.lower(*input)?;
                let declared = declared_ordering(&self.physical, lowered).unwrap_or_default();
                if declared.starts_with(keys) {
                    Ok(lowered)
                } else {
                    Err(PlanError::Internal(format!(
                        "no operator sorts by {keys:?}; the input declares {declared:?}"
                    )))
                }
            }
            LogicalNode::Limit { input, rows } => {
                self.limit = Some(*rows);
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Limit {
                    input: lowered,
                    rows: *rows,
                }))
            }
            LogicalNode::Page {
                input,
                rows,
                bytes,
                resume,
            } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Page {
                    input: lowered,
                    rows: *rows,
                    bytes: *bytes,
                    resume: resume.clone(),
                }))
            }
            LogicalNode::RowDiff {
                input,
                address_short_circuit,
            } => {
                let lowered = self.lower(*input)?;
                if *address_short_circuit {
                    self.mark_drop_equal_addresses(lowered);
                }
                Ok(self
                    .physical
                    .add(PhysicalNode::RowCompare { input: lowered }))
            }
            LogicalNode::Join {
                left,
                right,
                kind,
                on,
            } => {
                let left_lowered = self.lower(*left)?;
                let right_lowered = self.lower(*right)?;
                let build = self.scoped_scan(right_lowered);
                self.set_ordered(left_lowered, true);
                self.set_ordered(right_lowered, !build);
                self.join_algorithm = true;
                let hydrate = self.late_materialization_wins(*kind, &[left_lowered, right_lowered])
                    && (self.materialize_late(left_lowered, *left)
                        | self.materialize_late(right_lowered, *right));
                let build = build && hydrate;
                if !build {
                    self.set_ordered(right_lowered, true);
                }
                let join = self.physical.add(PhysicalNode::SortMergeJoin {
                    left: left_lowered,
                    right: right_lowered,
                    kind: *kind,
                    on: on.clone(),
                    build,
                    drop_equal_addresses: false,
                });
                if !hydrate {
                    return Ok(join);
                }
                let mut hydrated = join;
                for side in [left_lowered, right_lowered] {
                    if let Some(PhysicalNode::Scan { spec, .. }) = self.physical.node(side) {
                        let side = spec.side;
                        hydrated = self.physical.add(PhysicalNode::HydrateByAddress {
                            input: hydrated,
                            side,
                        });
                    }
                }
                Ok(hydrated)
            }
            LogicalNode::MergeClassify {
                base,
                source,
                target,
            } => {
                let sides = [
                    self.lower(*base)?,
                    self.lower(*source)?,
                    self.lower(*target)?,
                ];
                for physical_id in sides {
                    self.keys_only_side(physical_id);
                }
                self.decisions.push(StatisticSource {
                    statistic: "merge_side_shape".to_string(),
                    value: "keys then take-by-address on every side".to_string(),
                    origin: "fixed rule",
                });
                let [base, source, target] = sides;
                self.join_algorithm = true;
                let inner = self.physical.add(PhysicalNode::SortMergeJoin {
                    left: source,
                    right: base,
                    kind: KeyJoinKind::FullOuter,
                    on: LOGICAL_ID.to_string(),
                    build: false,
                    drop_equal_addresses: false,
                });
                let outer = self.physical.add(PhysicalNode::SortMergeJoin {
                    left: target,
                    right: inner,
                    kind: KeyJoinKind::FullOuter,
                    on: LOGICAL_ID.to_string(),
                    build: false,
                    drop_equal_addresses: false,
                });
                let mut hydrated = outer;
                for side in [SideId::Base, SideId::Parent, SideId::Child] {
                    hydrated = self.physical.add(PhysicalNode::HydrateByAddress {
                        input: hydrated,
                        side,
                    });
                }
                Ok(self
                    .physical
                    .add(PhysicalNode::ClassifyThreeWay { input: hydrated }))
            }
        }
    }

    fn scoped_scan(&self, id: NodeId) -> bool {
        matches!(
            self.physical.node(id),
            Some(PhysicalNode::Scan { spec, .. }) if spec.fragments.is_some()
        )
    }

    /// Pass 8, the traversal mode: the session's pin, else the cost model over
    /// the input's row-count estimate and the edge statistics (`Csr` and no
    /// estimate without them); the `ExpandPolicy` says what the run may do.
    fn expand_mode(
        &mut self,
        input: LogicalId,
        edge_type: &str,
        direction: Direction,
        min_hops: u32,
        max_hops: Option<u32>,
    ) -> (ExpandMode, Option<u64>, ExpandPolicy) {
        let forced = match self.source.traversal() {
            Traversal::Indexed => Some(ExpandMode::IndexedScan),
            Traversal::Csr => Some(ExpandMode::Csr),
            Traversal::Auto => None,
        };
        let input_rows = estimate_rows(self.logical, input, self.source);
        let cost = self
            .source
            .expand_statistics(edge_type, direction)
            .map(|statistics| ExpandCostInputs {
                frontier_rows: input_rows.unwrap_or(u64::MAX),
                edge_count: statistics.edge_count,
                src_node_count: statistics.src_node_count,
                effective_max_hops: executed_hops(min_hops, max_hops, statistics.same_type),
                max_hops_cap: statistics.max_hops_cap,
                max_frontier_cap: statistics.max_frontier_cap,
                coverage: IndexCoverage::Indexed,
                csr_cached: self.csr_cached,
                probe_factor: direction_probe_factor(direction),
            });
        let frontier_estimate = cost.as_ref().and(input_rows);
        let (mode, policy) = match (forced, cost) {
            (Some(mode), _) => (mode, ExpandPolicy::Pinned),
            (None, Some(inputs)) => {
                self.expand_mode = true;
                (choose_expand_mode(&inputs), ExpandPolicy::Costed { inputs })
            }
            (None, None) => (ExpandMode::Csr, ExpandPolicy::Uncosted),
        };
        if mode == ExpandMode::Csr {
            self.csr_cached = true;
        }
        (mode, frontier_estimate, policy)
    }

    /// Pass 9, the access path of a dependent scan: the cost model over the
    /// input's row-count estimate, the destination table's row count and its
    /// build-side bytes against the pool; `IdLookup` when a count is unknown.
    fn access_path(&mut self, input: LogicalId, spec: &ScanSpec) -> Result<AccessPath, PlanError> {
        let frontier = estimate_rows(self.logical, input, self.source);
        let node_type = node_type_of(spec, self.source)?;
        let rows = node_type.row_count;
        let build_bytes = rows.and_then(|rows| {
            build_side_bytes(
                rows,
                self.source.table_data_bytes(&spec.table.type_key),
                &node_type.schema,
                spec.projection.as_deref(),
                |column| self.source.column_data_bytes(&spec.table.type_key, column),
            )
        });
        let budget = self.source.query_memory_pool_bytes() / HASH_JOIN_POOL_DIVISOR;
        let Some(access) = choose_access_path(frontier, rows, build_bytes, budget) else {
            return Ok(AccessPath::IdLookup);
        };
        self.access_path = true;
        self.decisions.push(StatisticSource {
            statistic: format!("hash_join_build_bytes({})", spec.table.type_key),
            value: match build_bytes {
                Some(bytes) => format!("{bytes} estimated, budget {budget}"),
                None => format!("unknown, budget {budget}"),
            },
            origin: "projected widths and storage statistics",
        });
        Ok(access)
    }

    fn set_ordered(&mut self, id: NodeId, value: bool) {
        if let Some(PhysicalNode::Scan { ordered, .. }) = self.physical.node_mut(id) {
            *ordered = value;
        }
    }

    /// Pass 5's consequence in the physical plan: the join beneath the
    /// `RowDiff` drops matched pairs with equal addresses before hydration.
    fn mark_drop_equal_addresses(&mut self, mut id: NodeId) {
        loop {
            match self.physical.node_mut(id) {
                Some(PhysicalNode::HydrateByAddress { input, .. }) => id = *input,
                Some(PhysicalNode::SortMergeJoin {
                    drop_equal_addresses,
                    ..
                }) => {
                    *drop_equal_addresses = true;
                    return;
                }
                _ => return,
            }
        }
    }

    /// Pass 6's cost rule for a join: late materialization wins when the join
    /// is scoped, a statistic is unknown, the engine asked for it after a
    /// refused one-pass open, or a full-width sort would spill or be refused.
    fn late_materialization_wins(&mut self, kind: KeyJoinKind, scans: &[NodeId]) -> bool {
        if kind == KeyJoinKind::LeftOuter || self.bounds.late_materialization_only {
            return true;
        }
        let mut total_bytes = 0u64;
        let mut widest_fragment = 0u64;
        for id in scans {
            let Some(PhysicalNode::Scan { spec, .. }) = self.physical.node(*id) else {
                return true;
            };
            if spec.fragments.is_some() {
                return true;
            }
            let Ok(schema) = self.source.schema(spec.side) else {
                return true;
            };
            let fixed_width = fixed_row_width_bytes(&schema);
            for fragment in self.source.fragments(spec.side) {
                let (Some(rows), Some(file_bytes)) = (fragment.rows, fragment.bytes) else {
                    self.decisions.push(StatisticSource {
                        statistic: format!(
                            "fragment_bytes({:?}, fragment {})",
                            spec.side, fragment.id
                        ),
                        value: "unknown".to_string(),
                        origin: "manifest",
                    });
                    return true;
                };
                if rows > 0 {
                    let bytes = file_bytes.max(rows.saturating_mul(fixed_width));
                    total_bytes = total_bytes.saturating_add(bytes);
                    widest_fragment = widest_fragment.max(bytes);
                }
            }
        }
        let spills = total_bytes > self.bounds.ordered_scan_memory_bytes;
        let refused = widest_fragment > self.bounds.ordered_scan_max_input_batch_bytes;
        self.decisions.push(StatisticSource {
            statistic: "full_width_sort_bytes".to_string(),
            value: format!(
                "{total_bytes} total, {widest_fragment} widest fragment, pool {}, batch cap {}",
                self.bounds.ordered_scan_memory_bytes,
                self.bounds.ordered_scan_max_input_batch_bytes
            ),
            origin: "manifest",
        });
        spills || refused
    }

    /// Pass 6. A scan feeding the join with any column outside the key set
    /// becomes keys-only; the caller adds the `HydrateByAddress` above.
    fn materialize_late(&mut self, physical_id: NodeId, logical_id: LogicalId) -> bool {
        let needs = match (
            self.physical.node(physical_id),
            self.logical.schema(logical_id),
        ) {
            (Some(PhysicalNode::Scan { spec, .. }), Some(schema)) => match &spec.projection {
                Some(columns) => columns.iter().any(|column| !is_key_column(column, spec)),
                None => schema
                    .fields()
                    .iter()
                    .any(|field| !is_key_column(field.name(), spec)),
            },
            _ => false,
        };
        if !needs {
            return false;
        }
        if let Some(PhysicalNode::Scan { keys_only, .. }) = self.physical.node_mut(physical_id) {
            *keys_only = true;
        }
        self.late_materialization = true;
        true
    }

    /// Pass 6 for a merge side: keys then hydration whatever the width, a
    /// fixed rule; the two-sided diff's cost rule does not apply.
    fn keys_only_side(&mut self, physical_id: NodeId) {
        if let Some(PhysicalNode::Scan {
            keys_only, ordered, ..
        }) = self.physical.node_mut(physical_id)
        {
            *keys_only = true;
            *ordered = true;
        }
        self.late_materialization = true;
    }
}

/// Tables whose column statistics could admit a hash build rejected by the
/// manifest-only estimate; sparse frontiers and already-fitting builds need no I/O.
pub fn column_statistics_needed(
    operation: &Operation,
    source: &dyn PlanSource,
) -> Result<BTreeSet<String>, PlanError> {
    let mut logical = resolve(operation, source)?;
    rewrite(&mut logical, source)?;
    let budget = source.query_memory_pool_bytes() / HASH_JOIN_POOL_DIVISOR;
    let mut tables = BTreeSet::new();
    for (_, node) in logical.live() {
        let LogicalNode::TableScan {
            input: Some(input),
            spec,
        } = node
        else {
            continue;
        };
        let frontier = estimate_rows(&logical, *input, source);
        let node_type = node_type_of(spec, source)?;
        let Some(rows) = node_type.row_count else {
            continue;
        };
        let minimum = build_side_bytes(
            rows,
            None,
            &node_type.schema,
            spec.projection.as_deref(),
            |_| Some(0),
        );
        if budget == 0
            || choose_access_path(frontier, Some(rows), minimum, budget)
                != Some(AccessPath::HashJoin)
        {
            continue;
        }
        let current = build_side_bytes(
            rows,
            source.table_data_bytes(&spec.table.type_key),
            &node_type.schema,
            spec.projection.as_deref(),
            |column| source.column_data_bytes(&spec.table.type_key, column),
        );
        if choose_access_path(frontier, Some(rows), current, budget) != Some(AccessPath::HashJoin) {
            tables.insert(spec.table.type_key.clone());
        }
    }
    Ok(tables)
}

/// Hash-build sizing heuristic: projected fixed widths plus compressed bytes
/// and offsets for projected variable-width columns. Missing column statistics
/// fall back to whole-table bytes. Runtime limits cover decompression growth.
pub fn build_side_bytes(
    rows: u64,
    table_bytes: Option<u64>,
    schema: &SchemaRef,
    projection: Option<&[String]>,
    column_bytes: impl Fn(&str) -> Option<u64>,
) -> Option<u64> {
    let projected = |name: &str| projection.is_none_or(|columns| columns.iter().any(|c| c == name));
    let mut projected_fixed = 0u64;
    let mut variable_offsets = 0u64;
    let mut variable_bytes = Some(0u64);
    for field in schema.fields() {
        let width = fixed_width_of(field.data_type());
        if projected(field.name()) {
            projected_fixed = projected_fixed.saturating_add(width);
            let offsets = variable_offset_width(field.data_type());
            if offsets > 0 {
                variable_bytes = variable_bytes.and_then(|bytes| {
                    column_bytes(field.name()).map(|column| bytes.saturating_add(column))
                });
                variable_offsets = variable_offsets.saturating_add(offsets);
            }
        }
    }
    let fixed_bytes = rows.saturating_mul(projected_fixed);
    if variable_offsets == 0 {
        return Some(fixed_bytes);
    }
    let offsets = rows.saturating_add(1).saturating_mul(variable_offsets);
    Some(
        fixed_bytes
            .saturating_add(variable_bytes.or(table_bytes)?)
            .saturating_add(offsets),
    )
}

fn variable_offset_width(data_type: &DataType) -> u64 {
    match data_type {
        DataType::Struct(fields) => fields.iter().fold(0u64, |total, field| {
            total.saturating_add(variable_offset_width(field.data_type()))
        }),
        DataType::FixedSizeList(child, len) => variable_offset_width(child.data_type())
            .saturating_mul(u64::try_from(*len).unwrap_or(0)),
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => 8,
        DataType::Null => 0,
        other if fixed_width_of(other) == 0 => 4,
        _ => 0,
    }
}

/// The candidates a nearest arm of an `rrf()` asks for when the query has
/// no limit.
const RRF_NEAREST_ARM_K: usize = 100;

/// The ordering a ranked scan or a fusion carries: `nearest` ranks by
/// ascending `_distance`, `bm25` by descending `_score`, `rrf` by the fused rank.
fn search_ordering(node: &PhysicalNode) -> Option<Vec<String>> {
    match node {
        PhysicalNode::Scan {
            spec,
            ranked: Some(ranked),
            ..
        } => {
            let binding = spec.binding.as_deref()?;
            Some(vec![ordering_text(&ranked.ordering(binding))])
        }
        PhysicalNode::RankFuse { arms, .. } => {
            let targets: Vec<String> = arms.iter().map(|arm| format!("${}", arm.binding)).collect();
            Some(vec![format!("rrf({}) desc", targets.join(", "))])
        }
        _ => None,
    }
}

/// `keys` appended to `input`'s ordering, each key once: a sort's ordering
/// is its keys, and a ranked input's score key leads them already.
fn sorted_ordering(input: Option<Vec<String>>, keys: &[IROrdering]) -> Vec<String> {
    let mut ordering = input.unwrap_or_default();
    for key in keys.iter().map(ordering_text) {
        if !ordering.contains(&key) {
            ordering.push(key);
        }
    }
    ordering
}

/// The ordering a node declares by construction, in logical column names.
pub fn declared_ordering(plan: &PhysicalPlan, id: NodeId) -> Option<Vec<String>> {
    match plan.node(id)? {
        PhysicalNode::Scan {
            source: ScanInput::Dependent { input, .. },
            ..
        } => declared_ordering(plan, *input),
        node @ PhysicalNode::Scan {
            source: ScanInput::Table,
            ranked: Some(_),
            ..
        } => search_ordering(node),
        PhysicalNode::Scan {
            source: ScanInput::Table,
            ordered: true,
            ..
        } => Some(vec![LOGICAL_ID.to_string()]),
        PhysicalNode::Scan {
            source: ScanInput::Table,
            ordered: false,
            ..
        } => None,
        PhysicalNode::SortMergeJoin { on, .. } => Some(vec![on.clone()]),
        PhysicalNode::HashJoin { probe, .. } => declared_ordering(plan, *probe),
        PhysicalNode::HydrateByAddress { input, .. }
        | PhysicalNode::RowCompare { input, .. }
        | PhysicalNode::ClassifyThreeWay { input }
        | PhysicalNode::Page { input, .. }
        | PhysicalNode::Limit { input, .. }
        | PhysicalNode::Projection { input, .. } => declared_ordering(plan, *input),
        node @ PhysicalNode::RankFuse { .. } => search_ordering(node),
        PhysicalNode::Sort {
            input, order_by, ..
        } => Some(sorted_ordering(declared_ordering(plan, *input), order_by)),
        PhysicalNode::MetadataCount { .. }
        | PhysicalNode::CrossJoin { .. }
        | PhysicalNode::OuterReference { .. }
        | PhysicalNode::Filter { .. }
        | PhysicalNode::Expand { .. }
        | PhysicalNode::AntiJoin { .. }
        | PhysicalNode::Aggregate { .. } => None,
    }
}

/// Stage 3. Bottom-up: output schema, ordering, row estimate, estimated work
/// bytes, retained-memory limit and the statistic each derives from.
fn derive_properties(
    plan: &mut PhysicalPlan,
    source: &dyn PlanSource,
    bounds: &Bounds,
) -> Result<Vec<StatisticSource>, PlanError> {
    let mut read = Vec::new();
    for id in plan.post_order() {
        let node = plan
            .node(id)
            .cloned()
            .ok_or_else(|| PlanError::Internal(format!("physical node {id} is a tombstone")))?;
        let properties = match &node {
            PhysicalNode::MetadataCount { return_exprs, .. } => Properties {
                schema: metadata_count_schema(return_exprs)?,
                ordering: None,
                rows: Estimate::Known(1),
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: Vec::new(),
            },
            PhysicalNode::Scan {
                source: ScanInput::Dependent { input, .. },
                ..
            }
            | PhysicalNode::HashJoin { probe: input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: input.ordering.clone(),
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                ordered,
                keys_only: true,
                ..
            } => {
                let (rows, mut sources) = scan_rows(spec, source);
                let work_bytes = match rows {
                    Estimate::Known(rows) => {
                        Estimate::Known(rows.saturating_mul(bounds.key_width_bytes))
                    }
                    Estimate::Unknown => Estimate::Unknown,
                };
                sources.push(StatisticSource {
                    statistic: "key_width_bytes".to_string(),
                    value: bounds.key_width_bytes.to_string(),
                    origin: "engine constant",
                });
                Properties {
                    schema: key_schema(spec),
                    ordering: ordered.then(|| vec![LOGICAL_ID.to_string()]),
                    rows,
                    work_bytes,
                    retained_limit: None,
                    sources,
                }
            }
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                ordered,
                keys_only: false,
                ranked,
            } => {
                let (rows, sources) = if spec.binding.is_some() {
                    query_scan_rows(spec, source)
                } else {
                    scan_rows(spec, source)
                };
                let rows = match ranked.as_ref().and_then(|ranked| ranked.fetch) {
                    Some(fetch) => rows.capped(u64::try_from(fetch).unwrap_or(u64::MAX)),
                    None => rows,
                };
                let schema = scan_schema(spec, source)?;
                let schema = match &spec.projection {
                    Some(columns) => Arc::new(Schema::new(
                        schema
                            .fields()
                            .iter()
                            .filter(|field| columns.iter().any(|column| column == field.name()))
                            .map(|field| field.as_ref().clone())
                            .collect::<Vec<Field>>(),
                    )),
                    None => schema,
                };
                Properties {
                    schema,
                    ordering: search_ordering(&node)
                        .or_else(|| ordered.then(|| vec![LOGICAL_ID.to_string()])),
                    rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources,
                }
            }
            PhysicalNode::SortMergeJoin {
                left,
                right,
                kind,
                on,
                build,
                ..
            } => {
                let left_props = props(plan, *left)?;
                let right_props = props(plan, *right)?;
                let rows = match kind {
                    KeyJoinKind::LeftOuter => left_props.rows,
                    KeyJoinKind::FullOuter => sum_estimates(left_props.rows, right_props.rows),
                };
                let (retained_limit, sources) = match (*build, right_props.rows) {
                    (false, _) => (None, Vec::new()),
                    (true, Estimate::Known(rows)) => {
                        let limit = rows.saturating_mul(bounds.key_width_bytes);
                        (
                            Some(limit),
                            vec![StatisticSource {
                                statistic: "build_side_key_bytes".to_string(),
                                value: limit.to_string(),
                                origin: "manifest rows times key width",
                            }],
                        )
                    }
                    (true, Estimate::Unknown) => (
                        None,
                        vec![StatisticSource {
                            statistic: "build_side_key_bytes".to_string(),
                            value: "unknown".to_string(),
                            origin: "manifest holds no row count for a build fragment",
                        }],
                    ),
                };
                Properties {
                    schema: join_schema(
                        &left_props.schema,
                        physical_prefix(plan, *left),
                        &right_props.schema,
                        physical_prefix(plan, *right),
                    ),
                    ordering: Some(vec![on.clone()]),
                    rows,
                    work_bytes: sum_estimates(left_props.work_bytes, right_props.work_bytes),
                    retained_limit,
                    sources,
                }
            }
            PhysicalNode::HydrateByAddress { input, side } => {
                let input_props = props(plan, *input)?;
                let schema = expand_side(&input_props.schema, *side, &source.schema(*side)?);
                Properties {
                    schema,
                    ordering: input_props.ordering.clone(),
                    rows: input_props.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: Some(bounds.hydration_chunk_hard_bytes),
                    sources: vec![StatisticSource {
                        statistic: "retained_limit".to_string(),
                        value: bounds.hydration_chunk_hard_bytes.to_string(),
                        origin: "HYDRATION_CHUNK_HARD_BYTES",
                    }],
                }
            }
            PhysicalNode::RowCompare { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: diff_schema(&input.schema),
                    ordering: input.ordering.clone(),
                    rows: input.rows,
                    work_bytes: input.work_bytes,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::ClassifyThreeWay { input } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: classify_schema(&source.schema(SideId::Base)?),
                    ordering: Some(vec![LOGICAL_ID.to_string()]),
                    rows: input.rows,
                    work_bytes: input.work_bytes,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Page { input, rows, .. } | PhysicalNode::Limit { input, rows } => {
                let input = props(plan, *input)?;
                let page_rows = u64::try_from(*rows).unwrap_or(u64::MAX);
                let bounded = match input.rows {
                    Estimate::Known(count) => Estimate::Known(count.min(page_rows)),
                    Estimate::Unknown => Estimate::Known(page_rows),
                };
                Properties {
                    schema: input.schema.clone(),
                    ordering: input.ordering.clone(),
                    rows: bounded,
                    work_bytes: input.work_bytes,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::CrossJoin { left, right } => {
                let left_props = props(plan, *left)?;
                let right_props = props(plan, *right)?;
                let rows = match (left_props.rows, right_props.rows) {
                    (Estimate::Known(left), Estimate::Known(right)) => {
                        Estimate::Known(left.saturating_mul(right))
                    }
                    _ => Estimate::Unknown,
                };
                Properties {
                    schema: join_schema(
                        &left_props.schema,
                        physical_prefix(plan, *left),
                        &right_props.schema,
                        physical_prefix(plan, *right),
                    ),
                    ordering: None,
                    rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::OuterReference { .. } => Properties {
                schema: Arc::new(Schema::empty()),
                ordering: None,
                rows: Estimate::Unknown,
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: Vec::new(),
            },
            PhysicalNode::Sort {
                input, order_by, ..
            } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: Some(sorted_ordering(input.ordering.clone(), order_by)),
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Filter { input, .. }
            | PhysicalNode::Expand { input, .. }
            | PhysicalNode::AntiJoin { input, .. }
            | PhysicalNode::Aggregate { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: None,
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Projection { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: input.ordering.clone(),
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::RankFuse { arms, limit, .. } => {
                let input = props(plan, arms[0].input)?;
                let rows = match (input.rows, limit) {
                    (Estimate::Known(rows), Some(limit)) => {
                        Estimate::Known(rows.min(*limit as u64))
                    }
                    (rows, _) => rows,
                };
                Properties {
                    schema: input.schema.clone(),
                    ordering: search_ordering(&node),
                    rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
        };
        read.extend(properties.sources.iter().cloned());
        plan.set_properties(id, properties);
    }
    Ok(read)
}

fn props(plan: &PhysicalPlan, id: NodeId) -> Result<&Properties, PlanError> {
    plan.properties(id)
        .ok_or_else(|| PlanError::Internal(format!("physical node {id} has no properties yet")))
}

/// A query scan's rows: `scan_row_estimate`, the one number the cost passes
/// read, so explain and the cost model never disagree.
fn query_scan_rows(spec: &ScanSpec, source: &dyn PlanSource) -> (Estimate, Vec<StatisticSource>) {
    let rows = scan_row_estimate(spec, source).map_or(Estimate::Unknown, Estimate::Known);
    let sources = vec![StatisticSource {
        statistic: format!("scan_row_estimate({})", spec.table.type_key),
        value: match rows {
            Estimate::Known(rows) => rows.to_string(),
            Estimate::Unknown => "unknown".to_string(),
        },
        origin: "manifest row count, at most one row under a key equality",
    }];
    (rows, sources)
}

fn scan_rows(spec: &ScanSpec, source: &dyn PlanSource) -> (Estimate, Vec<StatisticSource>) {
    let fragments = source.fragments(spec.side);
    let in_scope: Vec<_> = match &spec.fragments {
        Some(ids) => fragments
            .into_iter()
            .filter(|fragment| ids.contains(&fragment.id))
            .collect(),
        None => fragments,
    };
    let rows = in_scope
        .iter()
        .try_fold(0u64, |total, fragment| {
            fragment.rows.map(|rows| total.saturating_add(rows))
        })
        .map_or(Estimate::Unknown, Estimate::Known);
    let sources = vec![StatisticSource {
        statistic: format!(
            "fragment_rows({:?}, {} fragments)",
            spec.side,
            in_scope.len()
        ),
        value: match rows {
            Estimate::Known(rows) => rows.to_string(),
            Estimate::Unknown => "unknown".to_string(),
        },
        origin: "manifest",
    }];
    (rows, sources)
}

fn sum_estimates(left: Estimate, right: Estimate) -> Estimate {
    match (left, right) {
        (Estimate::Known(l), Estimate::Known(r)) => Estimate::Known(l.saturating_add(r)),
        _ => Estimate::Unknown,
    }
}

/// The schema a `HydrateByAddress` restores: the input's, with the side's
/// key columns replaced in place by the side's complete schema.
fn expand_side(input: &SchemaRef, side: SideId, full: &SchemaRef) -> SchemaRef {
    let prefix = format!("{}.", side.name());
    let mut fields = Vec::with_capacity(input.fields().len() + full.fields().len());
    let mut expanded = false;
    for field in input.fields() {
        match field.name().strip_prefix(&prefix) {
            Some(name) if name == ROW_ID || name == ROW_ADDR => {}
            Some(_) if !expanded => {
                expanded = true;
                for full_field in full.fields() {
                    fields.push(Field::new(
                        format!("{prefix}{}", full_field.name()),
                        full_field.data_type().clone(),
                        true,
                    ));
                }
            }
            Some(_) => {}
            None => fields.push(field.as_ref().clone()),
        }
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::SystemColumns;

    use super::*;
    use crate::operation::{PageBudgetSpec, ScopeSpec, TableRef};
    use crate::source::{AdjacencyProof, FragmentStat, MemorySource};

    const COLUMNS: SystemColumns = SystemColumns {
        id: "id",
        src: "src",
        dst: "dst",
    };

    fn side(version: u64) -> Side {
        Side {
            table: TableRef {
                type_key: "node:Doc".to_string(),
                dataset_path: "tables/node_Doc".to_string(),
                native_branch: None,
            },
            version,
            columns: COLUMNS,
        }
    }

    fn doc_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
        ]))
    }

    fn commit_diff(resume: Option<&str>) -> Operation {
        Operation::CommitDiff {
            parent: side(7),
            child: side(8),
            scope: ScopeSpec {
                inserts: true,
                updates: true,
                deletes: true,
            },
            resume: resume.map(str::to_string),
            budget: PageBudgetSpec {
                rows: 1000,
                bytes: 4 * 1024 * 1024,
            },
        }
    }

    fn fragment(id: u64, rows: u64, row_width: u64) -> FragmentStat {
        FragmentStat {
            id,
            rows: Some(rows),
            bytes: Some(rows * row_width),
        }
    }

    fn source(proof: bool) -> MemorySource {
        source_with_width(proof, 64)
    }

    fn source_with_width(proof: bool, row_width: u64) -> MemorySource {
        let source = MemorySource::default()
            .with_schema(SideId::Parent, doc_schema())
            .with_schema(SideId::Child, doc_schema())
            .with_fragments(
                SideId::Parent,
                vec![fragment(0, 4, row_width), fragment(1, 6, row_width)],
            )
            .with_fragments(
                SideId::Child,
                vec![
                    fragment(0, 4, row_width),
                    fragment(1, 6, row_width),
                    fragment(2, 1, row_width),
                ],
            );
        if proof {
            source.with_proof(AdjacencyProof {
                child_fragments: vec![2],
                parent_fragments: vec![1],
                version_window: (7, 8),
            })
        } else {
            source
        }
    }

    use crate::fixture_bounds::BOUNDS;

    fn render(plan: &PhysicalPlan, id: NodeId, out: &mut String, depth: usize) {
        let node = plan.node(id).expect("live node");
        out.push_str(&"  ".repeat(depth));
        out.push_str(node.name());
        match node {
            PhysicalNode::SortMergeJoin {
                kind,
                build,
                drop_equal_addresses,
                ..
            } => {
                out.push_str(&format!("({kind:?}"));
                if *build {
                    out.push_str(", build");
                }
                if *drop_equal_addresses {
                    out.push_str(", drop_equal_addresses");
                }
                out.push(')');
            }
            PhysicalNode::Scan {
                spec,
                ordered,
                keys_only,
                ..
            } => {
                out.push_str(&format!(
                    "({:?}, fragments={:?}, {}, {})",
                    spec.side,
                    spec.fragments,
                    if *keys_only { "keys" } else { "full" },
                    if *ordered { "ordered" } else { "unordered" },
                ));
            }
            PhysicalNode::HydrateByAddress { side, .. } => {
                out.push_str(&format!("({side:?})"));
            }
            _ => {}
        }
        out.push('\n');
        for input in node.inputs() {
            render(plan, input, out, depth + 1);
        }
    }

    fn shape(optimized: &Optimized) -> String {
        let mut out = String::new();
        render(&optimized.physical, optimized.physical.root(), &mut out, 0);
        out
    }

    #[test]
    fn scoped_diff_builds_on_the_parent_fragments_and_probes_with_the_child() {
        let source = source(true);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        assert_eq!(
            plan.census().to_string(),
            "[Join(LeftOuter), Limit, RowDiff, Sort, TableScan, TableScan]"
        );
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        assert_eq!(
            shape(&optimized),
            "Page\n  RowCompare\n    HydrateByAddress(Parent)\n      HydrateByAddress(Child)\n        SortMergeJoin(LeftOuter, build, drop_equal_addresses)\n          Scan(Child, fragments=Some([2]), keys, ordered)\n          Scan(Parent, fragments=Some([1]), keys, unordered)\n"
        );
        assert_eq!(
            optimized.fired,
            vec![
                PASS_RESOLVE,
                PASS_FRAGMENT_SCOPE,
                PASS_ADDRESS_SHORT_CIRCUIT,
                PASS_LATE_MATERIALIZATION,
                PASS_JOIN_ALGORITHM,
            ]
        );
        let root = optimized
            .physical
            .properties(optimized.physical.root())
            .expect("root props");
        assert_eq!(root.rows, Estimate::Known(1));
        assert_eq!(root.ordering.as_deref(), Some(&["id".to_string()][..]));
        let pipelines = optimized.physical.pipelines_json();
        assert_eq!(pipelines.as_array().map(Vec::len), Some(2));
        assert_eq!(pipelines[0]["sink"], "SortMergeJoin(build)");
        assert_eq!(pipelines[1]["sink"], "consumer");
        assert_eq!(
            pipelines[1]["operators"],
            serde_json::json!([
                "SortMergeJoin(probe)",
                "HydrateByAddress(child)",
                "HydrateByAddress(parent)",
                "RowCompare",
                "Page"
            ])
        );
    }

    #[test]
    fn unscoped_diff_over_narrow_rows_streams_two_full_width_ordered_scans() {
        let source = source(false);
        let mut plan = resolve(&commit_diff(Some("k")), &source).expect("resolves");
        assert_eq!(
            plan.census().to_string(),
            "[Join(FullOuter), Limit, RowDiff, Sort, TableScan, TableScan]"
        );
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        assert!(optimized.fired.contains(&PASS_PREDICATE_PUSHDOWN));
        assert!(!optimized.fired.contains(&PASS_FRAGMENT_SCOPE));
        assert!(
            !optimized.fired.contains(&PASS_LATE_MATERIALIZATION),
            "narrow rows without a proof take the one-pass shape"
        );
        assert_eq!(
            shape(&optimized),
            "Page\n  RowCompare\n    SortMergeJoin(FullOuter)\n      Scan(Parent, fragments=None, full, ordered)\n      Scan(Child, fragments=None, full, ordered)\n"
        );
        let scans: Vec<&ScanSpec> = optimized
            .physical
            .live()
            .filter_map(|(_, node)| match node {
                PhysicalNode::Scan { spec, .. } => Some(spec.as_ref()),
                _ => None,
            })
            .collect();
        assert_eq!(scans.len(), 2);
        for scan in scans {
            assert!(scan.fragments.is_none());
            assert_eq!(
                scan.filter,
                Some(Predicate::IdAfter {
                    id: "k".to_string()
                })
            );
        }
        let root = optimized
            .physical
            .properties(optimized.physical.root())
            .expect("root props");
        assert_eq!(root.rows, Estimate::Known(21));
        assert!(
            optimized
                .statistics
                .iter()
                .any(|s| s.statistic == "full_width_sort_bytes")
        );
        let pipelines = optimized.physical.pipelines_json();
        assert_eq!(pipelines.as_array().map(Vec::len), Some(1));
        assert_eq!(pipelines[0]["source"]["node"], "SortMergeJoin(streaming)");
    }

    #[test]
    fn unscoped_diff_over_wide_rows_hydrates_by_address() {
        let source = source_with_width(false, 40 * 1024 * 1024);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        assert!(optimized.fired.contains(&PASS_LATE_MATERIALIZATION));
        assert_eq!(
            shape(&optimized),
            "Page\n  RowCompare\n    HydrateByAddress(Child)\n      HydrateByAddress(Parent)\n        SortMergeJoin(FullOuter)\n          Scan(Parent, fragments=None, keys, ordered)\n          Scan(Child, fragments=None, keys, ordered)\n"
        );
    }

    #[test]
    fn late_materialization_only_hydrates_narrow_rows_too() {
        let source = source(false);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let bounds = Bounds {
            late_materialization_only: true,
            ..BOUNDS
        };
        let optimized = optimize(&mut plan, &source, &bounds).expect("optimizes");
        assert!(optimized.fired.contains(&PASS_LATE_MATERIALIZATION));
        assert!(shape(&optimized).contains("HydrateByAddress(Child)"));
    }

    #[test]
    fn unscoped_diff_over_wide_vectors_hydrates_even_when_files_are_small() {
        let vector_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    3072,
                ),
                true,
            ),
        ]));
        let source = MemorySource::default()
            .with_schema(SideId::Parent, vector_schema.clone())
            .with_schema(SideId::Child, vector_schema)
            .with_fragments(SideId::Parent, vec![fragment(0, 20_000, 16)])
            .with_fragments(SideId::Child, vec![fragment(0, 20_000, 16)]);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        assert!(
            optimized.fired.contains(&PASS_LATE_MATERIALIZATION),
            "20,000 rows of 12 KiB vectors sort past the pool even when the files compress"
        );
    }

    #[test]
    fn unscoped_diff_with_unknown_bytes_hydrates_by_address() {
        let source = MemorySource::default()
            .with_schema(SideId::Parent, doc_schema())
            .with_schema(SideId::Child, doc_schema())
            .with_fragments(
                SideId::Parent,
                vec![FragmentStat {
                    id: 0,
                    rows: Some(4),
                    bytes: None,
                }],
            )
            .with_fragments(SideId::Child, vec![fragment(0, 4, 64)]);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        assert!(optimized.fired.contains(&PASS_LATE_MATERIALIZATION));
    }

    fn merge_classify() -> Operation {
        Operation::MergeClassify {
            base: side(3),
            source: side(5),
            target: side(6),
        }
    }

    fn merge_source(row_width: u64) -> MemorySource {
        MemorySource::default()
            .with_schema(SideId::Base, doc_schema())
            .with_schema(SideId::Parent, doc_schema())
            .with_schema(SideId::Child, doc_schema())
            .with_fragments(SideId::Base, vec![fragment(0, 4, row_width)])
            .with_fragments(
                SideId::Parent,
                vec![fragment(0, 4, row_width), fragment(1, 1, row_width)],
            )
            .with_fragments(
                SideId::Child,
                vec![fragment(0, 4, row_width), fragment(2, 2, row_width)],
            )
    }

    const MERGE_SHAPE: &str = "ClassifyThreeWay\n  HydrateByAddress(Child)\n    HydrateByAddress(Parent)\n      HydrateByAddress(Base)\n        SortMergeJoin(FullOuter)\n          Scan(Child, fragments=None, keys, ordered)\n          SortMergeJoin(FullOuter)\n            Scan(Parent, fragments=None, keys, ordered)\n            Scan(Base, fragments=None, keys, ordered)\n";

    #[test]
    fn merge_classify_hydrates_every_side_at_both_row_widths() {
        for row_width in [64, 40 * 1024 * 1024] {
            let source = merge_source(row_width);
            let mut plan = resolve(&merge_classify(), &source).expect("resolves");
            assert_eq!(
                plan.census().to_string(),
                "[MergeClassify, TableScan, TableScan, TableScan]"
            );
            let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
            assert_eq!(
                optimized.fired,
                vec![PASS_RESOLVE, PASS_LATE_MATERIALIZATION, PASS_JOIN_ALGORITHM],
                "narrow rows hydrate too: the merge has no one-pass shape"
            );
            assert_eq!(shape(&optimized), MERGE_SHAPE);
            assert!(
                optimized
                    .statistics
                    .iter()
                    .any(|s| s.statistic == "merge_side_shape")
            );
            let root = optimized
                .physical
                .properties(optimized.physical.root())
                .expect("root props");
            assert_eq!(root.rows, Estimate::Known(15));
            assert_eq!(root.ordering.as_deref(), Some(&["id".to_string()][..]));
            let names: Vec<&str> = root
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            assert_eq!(names, vec!["id", "payload", "_outcome", "_side"]);
            let pipelines = optimized.physical.pipelines_json();
            assert_eq!(pipelines.as_array().map(Vec::len), Some(1));
            let inner = &pipelines[0]["source"]["right"];
            assert_eq!(inner["source"]["node"], "SortMergeJoin(streaming)");
            assert_eq!(inner["operators"], serde_json::json!([]));
            assert_eq!(inner["source"]["left"]["source"]["node"], "Scan");
        }
    }

    #[test]
    fn resumed_page_has_the_same_census_as_a_first_page() {
        let source = source(true);
        let first = resolve(&commit_diff(None), &source).expect("resolves");
        let resumed = resolve(&commit_diff(Some("after")), &source).expect("resolves");
        assert_eq!(first.census(), resumed.census());
        assert_eq!(first.structural_hash(), resumed.structural_hash());
        assert!(matches!(
            first.node(first.root()),
            Some(LogicalNode::Page { .. })
        ));
        assert!(
            first
                .live()
                .any(|(_, node)| matches!(node, LogicalNode::Ordered { .. }))
        );
        assert!(
            !first.live().any(|(_, node)| matches!(
                node,
                LogicalNode::Limit { .. } | LogicalNode::Sort { .. }
            ))
        );
        let mut first = first;
        let optimized = optimize(&mut first, &source, &BOUNDS).expect("optimizes");
        assert!(matches!(
            optimized.physical.node(optimized.physical.root()),
            Some(PhysicalNode::Page { .. })
        ));
    }

    #[test]
    fn hydration_declares_the_hard_chunk_bound() {
        let source = source(true);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        let hydrate = optimized
            .physical
            .live()
            .find(|(_, node)| matches!(node, PhysicalNode::HydrateByAddress { .. }))
            .map(|(id, _)| id)
            .expect("hydration node");
        let properties = optimized.physical.properties(hydrate).expect("props");
        assert_eq!(
            properties.retained_limit,
            Some(BOUNDS.hydration_chunk_hard_bytes)
        );
        assert!(
            optimized
                .statistics
                .iter()
                .any(|s| s.origin == "HYDRATION_CHUNK_HARD_BYTES")
        );
    }

    #[test]
    fn build_side_declares_its_key_bytes() {
        let source = source(true);
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        let join = optimized
            .physical
            .live()
            .find(|(_, node)| matches!(node, PhysicalNode::SortMergeJoin { build: true, .. }))
            .map(|(id, _)| id)
            .expect("build join");
        let properties = optimized.physical.properties(join).expect("props");
        assert_eq!(properties.retained_limit, Some(6 * 48));
    }

    /// A build fragment with no manifest row count declares no limit and says
    /// so: a declared `0` would pass `over_bound` while bounding nothing.
    #[test]
    fn build_side_with_unknown_rows_declares_no_limit() {
        let source = source(true).with_fragments(
            SideId::Parent,
            vec![FragmentStat {
                id: 1,
                rows: None,
                bytes: None,
            }],
        );
        let mut plan = resolve(&commit_diff(None), &source).expect("resolves");
        let optimized = optimize(&mut plan, &source, &BOUNDS).expect("optimizes");
        let join = optimized
            .physical
            .live()
            .find(|(_, node)| matches!(node, PhysicalNode::SortMergeJoin { build: true, .. }))
            .map(|(id, _)| id)
            .expect("build join");
        let properties = optimized.physical.properties(join).expect("props");
        assert_eq!(properties.retained_limit, None);
        assert_eq!(properties.sources[0].statistic, "build_side_key_bytes");
        assert_eq!(properties.sources[0].value, "unknown");
        assert_eq!(crate::gate::over_bound(&optimized.physical, &BOUNDS), None);
    }

    /// FNV-1a over kind names and input counts: the literal holds on every
    /// toolchain, and a node's input count is part of the shape.
    #[test]
    fn structural_hash_is_a_fixed_function_of_the_shape() {
        let source = source(false);
        let plan = resolve(&commit_diff(None), &source).expect("resolves");
        assert_eq!(
            format!("{:016x}", plan.structural_hash()),
            "2ff59b16c5f2d308"
        );
        let resumed = resolve(&commit_diff(Some("k")), &source).expect("resolves");
        assert_eq!(plan.structural_hash(), resumed.structural_hash());
        let snapshot = Operation::SnapshotDiff {
            from: side(7),
            to: side(8),
        };
        let unpaged = resolve(&snapshot, &source).expect("resolves");
        assert_ne!(plan.structural_hash(), unpaged.structural_hash());
    }
}

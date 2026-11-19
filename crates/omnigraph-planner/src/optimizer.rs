//! The fixed pass sequence, in three stages: logical rewrites, physical
//! selection, property derivation. The order is written once here with its
//! reason; a pass runs only when its trigger is present in the plan.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::read_shape::{ArmRank, PatternStep, ReadShape};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use omnigraph_compiler::ir::{IRExpr, IROrdering, IRProjection};
use omnigraph_compiler::query::codes::{P002, P004};
use omnigraph_compiler::query::file::LoadStmtMode;
use omnigraph_compiler::query::value::AggFunc;
use omnigraph_compiler::settings::Traversal;
use omnigraph_compiler::traversal::{EDGE_TYPE_COLUMN, EdgeSelection};
use omnigraph_compiler::types::{Direction, ExprType};

use crate::cost::{
    AccessPath, AntiJoinStrategy, CoverageProvenance, EndpointProbe, ExpandCostInputs, ExpandMode,
    ExpandPolicy, HASH_JOIN_POOL_DIVISOR, HASH_JOIN_RATIO, HYDRATE_ROW_RATIO, IndexCoverage,
    ValidateAccess, ValidateCostInputs, ValidateIndexCoverage, ValidateRemoval, chain_rows,
    choose_access_path, choose_anti_join_strategy, choose_expand_mode, choose_validate_access,
    coverage_validate_access, direction_probe_factor, estimate_rows, executed_hops,
    hydrate_chunk_bytes, scan_row_estimate,
};
use crate::error::{PlanError, SET_TRAVERSAL_WORK_LIMIT};
use crate::key_set::{KeySetTree, key_set_tree};
use crate::logical::{
    ColumnRef, ConsultedImage, CsvSourceSpec, DeltaSourceSpec, EDGE_TYPE_MEMBER, FragmentWriteSpec,
    FuseInput, GqFilter, IDENTITY_MEMBER, KeyJoinKind, LOGICAL_ID, LogicalId, LogicalNode,
    LogicalPlan, Predicate, RuntimeFilterKind, RuntimeFilterSpec, ScanSpec, ValidateSpec,
};
use crate::lower::ContainsJoinFields;
use crate::operation::{Operation, Side};
use crate::output::{node_object_types, return_schema};
use crate::passes::{PassId, PassStage};
use crate::physical::{
    Assumptions, Estimate, FuseArm, Hop, HydratedBinding, HydratedColumn, NearestPolicy, NodeId,
    OrderKey, OverfetchRung, PhysicalNode, PhysicalPlan, Prefilter, Properties, RankScope,
    RankedAccess, ScanInput, StatisticSource, TextContains,
};
use crate::source::{NodeTypeSpec, PlanSource, SideId, charge_pass};
use crate::validate::subset::{Role, Rule, Tracer};

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
    /// The most parsers, probe chunks or writers a load operator runs at
    /// once; the phases are sequential, so the cap never multiplies.
    pub load_workers_max: u32,
    /// The bound every staged write input of a load is checked against.
    pub load_write_max_bytes: u64,
    /// The source bytes one parse partition of a load owns,
    /// `cost::SOURCE_PARTITION_BYTES` unless a test pins a smaller slice.
    pub source_partition_bytes: u64,
}

pub const PASS_RESOLVE: &str = PassId::Resolve.name();
pub const PASS_PREDICATE_PUSHDOWN: &str = PassId::PredicatePushdown.name();
pub const PASS_AGGREGATE_PUSHDOWN: &str = PassId::AggregatePushdown.name();
pub const PASS_PROJECTION_PUSHDOWN: &str = PassId::ProjectionPushdown.name();
pub const PASS_FRAGMENT_SCOPE: &str = PassId::FragmentScope.name();
pub const PASS_ADDRESS_SHORT_CIRCUIT: &str = PassId::AddressShortCircuit.name();
pub const PASS_LATE_MATERIALIZATION: &str = PassId::LateMaterialization.name();
pub const PASS_JOIN_ALGORITHM: &str = PassId::JoinAlgorithm.name();
pub const PASS_EXPAND_MODE: &str = PassId::ExpandMode.name();
pub const PASS_ACCESS_PATH: &str = PassId::AccessPath.name();
pub const PASS_ANTI_JOIN_STRATEGY: &str = PassId::AntiJoinStrategy.name();

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
        Operation::Load {
            ir,
            tables,
            source_bytes,
        } => resolve_load(&mut plan, ir, tables, *source_bytes, source)?,
        Operation::Write { ir, tables, .. } => resolve_write(&mut plan, ir, tables, source)?,
    }
    Ok(plan)
}

/// The IR's checks as logical specs: the consulted image at the version
/// `tables` pins, no access yet.
fn validate_specs(
    checks: &[omnigraph_compiler::ir::LoadCheck],
    tables: &[Side],
) -> Vec<ValidateSpec> {
    checks
        .iter()
        .map(|check| ValidateSpec {
            check: check.check.clone(),
            consults: check.consults.as_ref().map(|table| ConsultedImage {
                table: table.clone(),
                version: side_version(tables, table),
            }),
            access: None,
            probe: None,
        })
        .collect()
}

fn side_version(tables: &[Side], type_key: &str) -> Option<u64> {
    tables
        .iter()
        .find(|side| side.table.type_key == type_key)
        .map(|side| side.version)
}

/// Stage 1, pass 1 for a write's table: the staged delta as the leaf and one
/// `Validate` carrying the IR's checks in its order at the root; the
/// executor writes, so no node follows.
fn resolve_write(
    plan: &mut LogicalPlan,
    ir: &omnigraph_compiler::ir::WriteTableIR,
    tables: &[Side],
    source: &dyn PlanSource,
) -> Result<(), PlanError> {
    let target = ir.target.table_key();
    let schema = source.schema(SideId::Child)?;
    let delta = plan.add(
        LogicalNode::DeltaSource {
            spec: Box::new(DeltaSourceSpec {
                table: target.clone(),
                version: side_version(tables, &target),
                rows: ir.rows,
                delta: ir.delta,
                deletes: ir.deletes,
            }),
        },
        schema.clone(),
    );
    let validated = plan.add(
        LogicalNode::Validate {
            input: delta,
            table: target,
            checks: validate_specs(&ir.checks, tables),
            replaced: ir.delta.replaces(),
        },
        schema,
    );
    plan.set_root(validated);
    Ok(())
}

/// Stage 1, pass 1 for a load: the source leaf, one `Validate` carrying the
/// IR's checks in its order, the write at the root; the source's schema is the
/// target's typed delta, which the plan source answers for `SideId::Child`.
fn resolve_load(
    plan: &mut LogicalPlan,
    ir: &omnigraph_compiler::ir::LoadIR,
    tables: &[Side],
    source_bytes: u64,
    source: &dyn PlanSource,
) -> Result<(), PlanError> {
    let target = ir.target.table_key();
    let schema = source.schema(SideId::Child)?;
    let source_node = plan.add(
        LogicalNode::CsvSource {
            spec: Box::new(CsvSourceSpec {
                source: ir.source.clone(),
                source_bytes,
                estimated_rows: CsvSourceSpec::estimate_rows(source_bytes, ir.mapped_columns),
                delimiter: ir.delimiter,
                header: ir.header,
                columns: ir.columns.clone(),
                table: target.clone(),
                version: side_version(tables, &target),
            }),
        },
        schema.clone(),
    );
    let validated = plan.add(
        LogicalNode::Validate {
            input: source_node,
            table: target.clone(),
            checks: validate_specs(&ir.checks, tables),
            replaced: ir.mode == LoadStmtMode::Overwrite,
        },
        schema,
    );
    let write = plan.add(
        LogicalNode::FragmentWrite {
            input: validated,
            spec: Box::new(FragmentWriteSpec {
                table: target.clone(),
                version: side_version(tables, &target),
                mode: ir.mode,
            }),
        },
        write_count_schema(),
    );
    plan.set_root(write);
    Ok(())
}

/// The one row a write leaves: `count`, the rows it wrote.
pub fn write_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Stage 1, pass 1 for a GQ query: the flat `PatternStep` pipeline folded into a
/// tree, then a leading search function, the `return`, the remaining
/// `order` keys and the `limit` above it.
fn resolve_query(
    plan: &mut LogicalPlan,
    ir: &ReadShape,
    source: &dyn PlanSource,
) -> Result<(), PlanError> {
    plan.set_made_up(ir.made_up.clone());
    if ir.has_edge_selections() {
        let limit = source.traversal_work_limit().ok_or_else(|| {
            PlanError::refused(
                P002,
                "edge selections require a finite traversal_work_limit",
                Some(SET_TRAVERSAL_WORK_LIMIT),
            )
        })?;
        let assumptions = Assumptions {
            traversal_work_limit: Some(limit),
            ..Default::default()
        };
        assumptions.validated_traversal_work_limit()?;
        plan.set_traversal_work_limit(Some(limit));
        if source.traversal() == Traversal::Csr {
            return Err(PlanError::refused(
                P004,
                "edge selections do not support traversal = csr; use auto or indexed",
                Some("run the traversal with traversal mode `auto` or `indexed`"),
            ));
        }
    }
    let ReadShape {
        name: _,
        params: _,
        pipeline,
        return_exprs,
        distinct,
        order_by,
        limit,
        limit_param,
        written_columns,
        made_up: _,
        work: _,
    } = ir;
    if let (Some(param), None) = (limit_param, limit) {
        return Err(PlanError::Internal(format!(
            "`limit ${param}` reached the planner without its bound count"
        )));
    }
    let mut next_binding = 0u16;
    let mut current = resolve_pipeline(plan, pipeline, source, &mut next_binding, None)?;
    let schema = schema_of(plan, current)?;
    let mut orderings: &[IROrdering] = order_by;
    // A ranking drawn as a pattern step leads no sort.
    let windowed = pipeline
        .iter()
        .any(|step| matches!(step, PatternStep::Rank { .. }));
    let has_aggregates = return_exprs
        .iter()
        .any(|projection| contains_aggregate(&projection.expr));
    let grouped = has_aggregates || *distinct;
    let mut ranked = false;
    if let Some(leading) = orderings.first().filter(|_| !windowed) {
        if let Some(node) = search_node(current, &leading.expr, *limit, true, None) {
            current = plan.add(node, schema.clone());
            orderings = &orderings[1..];
            ranked = true;
        }
    }
    let schema = return_schema(return_exprs, &node_object_types(return_exprs, source)?)?;
    current = if grouped {
        let (grouped_exprs, above) = split_aggregate_return(return_exprs);
        let mut reads = Vec::new();
        for IRProjection { expr, .. } in &grouped_exprs {
            reads_of_expr(expr, &mut reads);
        }
        let grouped_schema =
            return_schema(&grouped_exprs, &node_object_types(&grouped_exprs, source)?)?;
        let aggregate = plan.add(
            LogicalNode::Aggregate {
                input: current,
                reads,
                return_exprs: grouped_exprs,
            },
            grouped_schema,
        );
        match above {
            Some(above) => plan.add(
                LogicalNode::Projection {
                    input: aggregate,
                    reads: Vec::new(),
                    return_exprs: above,
                },
                schema.clone(),
            ),
            None => aggregate,
        }
    } else {
        let mut reads = Vec::new();
        for IRProjection { expr, .. } in return_exprs {
            reads_of_expr(expr, &mut reads);
        }
        plan.add(
            LogicalNode::Projection {
                input: current,
                reads,
                return_exprs: return_exprs.clone(),
            },
            schema.clone(),
        )
    };
    // A `limit` cuts a total order: with no key written it is the tie
    // rule's alone (RFC, `Cut`), over pattern rows and group rows alike.
    if ranked || !orderings.is_empty() || limit.is_some() {
        let bound: Vec<IROrdering> = orderings
            .iter()
            .map(|ordering| IROrdering {
                expr: bind_order_key(&ordering.expr, return_exprs, grouped),
                ..ordering.clone()
            })
            .filter(|ordering| !matches!(ordering.expr, IRExpr::Literal(_, _)))
            .collect();
        let mut keys = Vec::new();
        for ordering in &bound {
            order_keys(&ordering.expr, &mut keys);
        }
        let tiebreak = sort_tiebreak(
            &tie_rule(plan, current, source, false, written_columns)?,
            order_by,
            return_exprs,
            grouped,
        );
        current = plan.add(
            LogicalNode::Sort {
                input: current,
                keys,
                order_by: bound,
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
    ops: &[PatternStep],
    source: &dyn PlanSource,
    next_binding: &mut u16,
    seed: Option<LogicalId>,
) -> Result<LogicalId, PlanError> {
    // The pattern block's one join order, the written one: the state this
    // planner explores; a join enumeration charges each order it considers.
    if let Some(work) = source.query_work() {
        work.explore(1)?;
    }
    let mut current: Option<LogicalId> = seed;
    for op in ops {
        if let Some(work) = source.query_work() {
            work.step()?;
        }
        let node = match op {
            PatternStep::NodeScan {
                variable,
                type_name,
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
                            access: None,
                            side,
                            table,
                            version,
                            columns,
                            fragments: None,
                            projection: None,
                            filter: None,
                            binding: Some(variable.clone()),
                            runtime_filter: None,
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
            PatternStep::Filter {
                pred: filter,
                fails,
            } => {
                let input = bound(current, "filter")?;
                let schema = schema_of(plan, input)?;
                filter_over(plan, input, std::slice::from_ref(filter), *fails, schema)
            }
            PatternStep::Expand {
                src_var,
                dst_var,
                edges,
                src_type,
                dst_type,
                min_hops,
                max_hops,
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
                        edges: edges.clone(),
                        src_type: src_type.clone(),
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        edge_binding: edge_binding.clone(),
                    },
                    schema.clone(),
                );
                plan.add(
                    LogicalNode::TableScan {
                        input: Some(expand),
                        spec: Box::new(ScanSpec {
                            access: None,
                            side,
                            table,
                            version,
                            columns,
                            fragments: None,
                            projection: None,
                            filter: None,
                            binding: Some(dst_var.clone()),
                            runtime_filter: None,
                        }),
                    },
                    schema,
                )
            }
            PatternStep::Rank {
                key,
                binding,
                window,
                recall,
                ..
            } => {
                let input = bound(current, "ranking")?;
                let schema = schema_of(plan, input)?;
                // A search over the ranked binding's scan, or rows that
                // already carry their exact distance, which the key reads.
                let (ranked, score) = match search_node(input, key, *window, false, *recall) {
                    Some(node) => (plan.add(node, schema.clone()), rank_score(binding)),
                    None => (input, key.clone()),
                };
                // The window: the rows of the `k` nearest targets, in
                // distance order with ties to the smaller identity.
                let targets = window.ok_or_else(|| {
                    PlanError::Internal("a `nearest` ranking step has no window".to_string())
                })?;
                let mut keys = Vec::new();
                order_keys(&score, &mut keys);
                let tiebreak = scope_tiebreaks(plan, ranked, source)?
                    .into_iter()
                    .map(|(column, _)| column)
                    .filter(|column| column.binding == *binding)
                    .collect();
                let sorted = plan.add(
                    LogicalNode::Sort {
                        input: ranked,
                        keys,
                        order_by: vec![IROrdering::new(score, false)],
                        fetch: None,
                        tiebreak,
                    },
                    schema.clone(),
                );
                plan.add(
                    LogicalNode::TargetCut {
                        input: sorted,
                        binding: binding.clone(),
                        targets,
                    },
                    schema,
                )
            }
            PatternStep::Score {
                binding,
                type_name,
                property,
                kind,
                query,
            } => {
                let input = bound(current, "score")?;
                let schema = schema_of(plan, input)?;
                // Each row is scored from its own value of the scored
                // property: `distance`'s vector, `bm25`'s text.
                let mut reads = vec![ColumnRef::property(binding, property)];
                reads_of_expr(query, &mut reads);
                reads.dedup();
                plan.add(
                    LogicalNode::Score {
                        input,
                        binding: binding.clone(),
                        type_name: type_name.clone(),
                        property: property.clone(),
                        kind: *kind,
                        query: query.clone(),
                        reads,
                    },
                    schema,
                )
            }
            PatternStep::Let { name, value } => {
                let input = bound(current, "let")?;
                let schema = schema_of(plan, input)?;
                let mut reads = Vec::new();
                reads_of_expr(value, &mut reads);
                plan.add(
                    LogicalNode::Let {
                        input,
                        name: name.clone(),
                        value: value.clone(),
                        reads,
                    },
                    schema,
                )
            }
            PatternStep::AntiJoin {
                outer_var,
                inner,
                predicate,
                fails,
            } => {
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
                        predicate: predicate.clone(),
                        fails: *fails,
                    },
                    schema,
                )
            }
            PatternStep::Union {
                arms,
                bindings,
                outer_var,
            } => {
                // Arms that read the enclosing scope each start from its
                // rows: the first from the correlated read's own leaf, the
                // others from leaves of their own over the same rows.
                let mut seeds: Vec<Option<LogicalId>> = Vec::with_capacity(arms.len());
                if let Some(outer_var) = outer_var {
                    let first = bound(current, "correlated union")?;
                    let schema = schema_of(plan, first)?;
                    seeds.push(Some(first));
                    for _ in 1..arms.len() {
                        seeds.push(Some(plan.add(
                            LogicalNode::OuterReference {
                                outer_var: outer_var.clone(),
                            },
                            schema.clone(),
                        )));
                    }
                } else {
                    seeds.resize(arms.len(), None);
                }
                let mut folded: Option<LogicalId> = None;
                for (arm, seed) in arms.iter().zip(seeds) {
                    let arm = resolve_pipeline(plan, arm, source, next_binding, seed)?;
                    folded = Some(match folded {
                        None => arm,
                        Some(left) => {
                            let schema = schema_of(plan, left)?;
                            plan.add(
                                LogicalNode::Union {
                                    left,
                                    right: arm,
                                    bindings: bindings.clone(),
                                },
                                schema,
                            )
                        }
                    });
                }
                let union =
                    folded.ok_or_else(|| PlanError::Internal("a union has no arm".to_string()))?;
                match current {
                    _ if outer_var.is_some() => union,
                    None => union,
                    Some(left) => {
                        let joined = join_schema(
                            &schema_of(plan, left)?,
                            None,
                            &schema_of(plan, union)?,
                            None,
                        );
                        plan.add(LogicalNode::CrossJoin { left, right: union }, joined)
                    }
                }
            }
            PatternStep::Correlate {
                outer_var,
                inner,
                read,
                fails,
            } => {
                let seed = match outer_var {
                    Some(outer_var) => {
                        let input = bound(current, "correlate")?;
                        let schema = schema_of(plan, input)?;
                        Some(plan.add(
                            LogicalNode::OuterReference {
                                outer_var: outer_var.clone(),
                            },
                            schema,
                        ))
                    }
                    None => None,
                };
                let inner_root = resolve_pipeline(plan, inner, source, next_binding, seed)?;
                let tiebreak = correlate_tiebreak(plan, inner_root, read, source)?;
                let mut reads = Vec::new();
                for expr in read.exprs() {
                    reads_of_expr(expr, &mut reads);
                }
                let schema = schema_of(plan, inner_root)?;
                let correlated = outer_var.is_some().then_some(()).and(current);
                let node = plan.add(
                    LogicalNode::Correlate {
                        input: correlated,
                        inner: inner_root,
                        outer_var: outer_var.clone(),
                        read: Box::new(read.clone()),
                        tiebreak,
                        reads,
                        fails: *fails,
                    },
                    schema,
                );
                match (current, outer_var) {
                    (Some(left), None) => {
                        let joined = join_schema(
                            &schema_of(plan, left)?,
                            None,
                            &schema_of(plan, node)?,
                            None,
                        );
                        plan.add(LogicalNode::CrossJoin { left, right: node }, joined)
                    }
                    _ => node,
                }
            }
            PatternStep::Fuse {
                arms,
                target,
                target_type,
                k,
                weights,
            } => {
                if current.is_some() {
                    return Err(PlanError::Internal(
                        "an `rrf` drawn after other elements, which the shaper refuses".to_string(),
                    ));
                }
                let mut inputs = Vec::with_capacity(arms.len());
                for arm in arms {
                    let root = resolve_pipeline(plan, &arm.steps, source, next_binding, None)?;
                    inputs.push(FuseInput {
                        input: ranked_arm(plan, root, target, arm.rank, source)?,
                        rank: arm.rank,
                    });
                }
                let mut reads = Vec::new();
                for expr in k.iter().chain(weights.iter()) {
                    reads_of_expr(expr, &mut reads);
                }
                let NodeTypeSpec { schema, .. } = source.node_type(target_type)?;
                plan.add(
                    LogicalNode::Fuse {
                        arms: inputs,
                        target: target.clone(),
                        k: k.clone(),
                        weights: weights.clone(),
                        reads,
                    },
                    schema,
                )
            }
            PatternStep::Optional {
                outer_var,
                inner,
                fails,
            } => {
                let input = bound(current, "optional")?;
                let schema = schema_of(plan, input)?;
                let outer = plan.add(
                    LogicalNode::OuterReference {
                        outer_var: outer_var.clone(),
                    },
                    schema,
                );
                let inner = resolve_pipeline(plan, inner, source, next_binding, Some(outer))?;
                let joined = schema_of(plan, inner)?;
                plan.add(
                    LogicalNode::OptionalJoin {
                        input,
                        inner,
                        outer_var: outer_var.clone(),
                        fails: *fails,
                    },
                    joined,
                )
            }
        };
        current = Some(node);
    }
    current.ok_or_else(|| PlanError::Internal("a GQ pipeline starts with a scan".to_string()))
}

/// A fusion arm's root, whose rows arrive in the arm's rank order: a
/// `nearest` window's `TargetCut` (distance, then the target's identity) and
/// an ordered read's `Correlate` (its keys, then its tie rule) as the arm's
/// steps planned them; a score's rows (an exact `distance` or `bm25`, or a
/// nested fusion) sorted by the score in its kind's direction, then the
/// target's identity, so each target's rows are adjacent at its rank.
fn ranked_arm(
    plan: &mut LogicalPlan,
    root: LogicalId,
    target: &str,
    rank: ArmRank,
    source: &dyn PlanSource,
) -> Result<LogicalId, PlanError> {
    let Some(score) = arm_score(target, rank) else {
        return Ok(root);
    };
    if rank == ArmRank::Nearest {
        return Ok(root);
    }
    let schema = schema_of(plan, root)?;
    let mut keys = Vec::new();
    order_keys(&score.expr, &mut keys);
    let tiebreak = scope_tiebreaks(plan, root, source)?
        .into_iter()
        .map(|(column, _)| column)
        .filter(|column| column.binding == target)
        .collect();
    Ok(plan.add(
        LogicalNode::Sort {
            input: root,
            keys,
            order_by: vec![score],
            fetch: None,
            tiebreak,
        },
        schema,
    ))
}

/// The score a fusion arm ranks its target by, in its kind's direction:
/// `_distance` ascending (`F32`), an exact `bm25`'s `_score` descending
/// (`F64`), a nested fusion's `_rrf` descending (`F64`); `None` for an
/// ordered read, ranked by its own order.
pub(crate) fn arm_score(target: &str, rank: ArmRank) -> Option<IROrdering> {
    let (column, descending) = rank.score()?;
    let scalar = match rank {
        ArmRank::Nearest | ArmRank::Distance => omnigraph_compiler::types::ScalarType::F32,
        ArmRank::Bm25 | ArmRank::Fused | ArmRank::Ordered => {
            omnigraph_compiler::types::ScalarType::F64
        }
    };
    Some(IROrdering::new(
        IRExpr::PropAccess {
            variable: target.to_string(),
            property: column.to_string(),
            ty: omnigraph_compiler::types::ExprType::from_prop(
                &omnigraph_compiler::types::PropType::scalar(scalar, false),
            ),
        },
        descending,
    ))
}

/// The columns a correlated read's tie rule reads after its `order` keys:
/// over its pattern's rows, every column of its pattern in column order
/// ([`tie_rule`], its own unreturned bindings counting where they are
/// bound, since its rows hold them until its projection); over its groups,
/// each reference key by its identity, then each value key by its column of
/// the read's `values`, each in column order.
fn correlate_tiebreak(
    plan: &LogicalPlan,
    inner: LogicalId,
    read: &crate::read_shape::InnerRead,
    source: &dyn PlanSource,
) -> Result<Vec<ColumnRef>, PlanError> {
    let candidates = tie_rule(plan, inner, source, true, &[])?;
    let Some(group) = &read.group else {
        return Ok(candidates.into_iter().map(|(column, _)| column).collect());
    };
    let mut out = Vec::new();
    for key in group {
        if let IRExpr::Variable(name, _) = &key.expr {
            out.extend(
                candidates
                    .iter()
                    .filter(|(column, _)| column.binding == *name)
                    .map(|(column, _)| column.clone()),
            );
        }
    }
    for key in group {
        match &key.expr {
            IRExpr::Aggregate { .. } | IRExpr::Variable(_, _) => {}
            _ => out.push(ColumnRef::property(&read.values, &key.column)),
        }
    }
    Ok(out)
}

fn bound(current: Option<LogicalId>, op: &str) -> Result<LogicalId, PlanError> {
    current.ok_or_else(|| PlanError::Internal(format!("a GQ {op} op has no input")))
}

fn schema_of(plan: &LogicalPlan, id: LogicalId) -> Result<SchemaRef, PlanError> {
    plan.schema(id)
        .cloned()
        .ok_or_else(|| PlanError::Internal(format!("logical node {id} has no schema")))
}

/// `filters` as one `Filter` node over `input`, its conjunct list in written
/// order, a repeated conjunct kept once; `input` itself when there is none. Where each conjunct runs is the
/// placement pass's decision, not this builder's.
fn filter_over(
    plan: &mut LogicalPlan,
    input: LogicalId,
    filters: &[IRExpr],
    fails: bool,
    schema: SchemaRef,
) -> LogicalId {
    let mut conjuncts: Vec<IRExpr> = Vec::new();
    for conjunct in filters.iter().cloned().flat_map(IRExpr::into_conjuncts) {
        if !conjuncts.contains(&conjunct) {
            conjuncts.push(conjunct);
        }
    }
    if conjuncts.is_empty() {
        input
    } else {
        plan.add(
            LogicalNode::Filter {
                input,
                conjuncts,
                fails,
            },
            schema,
        )
    }
}

/// The binding a root scan carries, looked at through the `Filter` nodes the
/// builder stacks on it.
fn binding_of(plan: &LogicalPlan, id: LogicalId) -> Option<String> {
    match plan.node(id) {
        Some(LogicalNode::TableScan { input: None, spec }) => spec.binding.clone(),
        Some(LogicalNode::Filter { input, .. }) => binding_of(plan, *input),
        _ => None,
    }
}

/// One conjunct as a scan predicate: its reads, its GQ text and itself.
pub(crate) fn gq_conjunct(conjunct: &IRExpr) -> Predicate {
    let mut reads = Vec::new();
    reads_of_expr(conjunct, &mut reads);
    Predicate::Gq {
        reads,
        text: conjunct.to_string(),
        filter: GqFilter(conjunct.clone()),
    }
}

/// The node a leading `order` search function becomes; `None` leaves the
/// expression to `Sort`, which then reads its columns like any other.
fn search_node(
    input: LogicalId,
    expr: &IRExpr,
    limit: Option<u64>,
    leads: bool,
    recall: Option<f64>,
) -> Option<LogicalNode> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
            ..
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
                leads,
                recall,
            })
        }
        _ => None,
    }
}

/// The distance column a `nearest` scan appends under `binding`.
pub(crate) fn rank_score(binding: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: binding.to_string(),
        property: omnigraph_compiler::query::value::DISTANCE_COLUMN.to_string(),
        ty: omnigraph_compiler::types::ExprType::from_prop(
            &omnigraph_compiler::types::PropType::scalar(
                omnigraph_compiler::types::ScalarType::F32,
                false,
            ),
        ),
    }
}

pub(crate) fn contains_aggregate(expr: &IRExpr) -> bool {
    let mut found = false;
    expr.visit_nodes(|node| found |= matches!(node, IRExpr::Aggregate { .. }));
    found
}

/// The `Aggregate` node's items (group keys, plain aggregates, and each
/// aggregate leaf of a Boolean item under the leaf's GQ text) and, when a
/// Boolean item compares aggregates, the `Projection` items reading them by alias.
fn split_aggregate_return(
    return_exprs: &[IRProjection],
) -> (Vec<IRProjection>, Option<Vec<IRProjection>>) {
    let is_plain = |projection: &IRProjection| {
        matches!(projection.expr, IRExpr::Aggregate { .. }) || !contains_aggregate(&projection.expr)
    };
    if return_exprs.iter().all(is_plain) {
        return (return_exprs.to_vec(), None);
    }
    let mut grouped: Vec<IRProjection> = return_exprs
        .iter()
        .filter(|p| is_plain(p))
        .cloned()
        .collect();
    let above = return_exprs
        .iter()
        .map(|projection| IRProjection {
            expr: if is_plain(projection) {
                IRExpr::AliasRef(projection.column.clone(), projection.ty.clone())
            } else {
                hoist_aggregates(&projection.expr, &mut grouped)
            },
            alias: projection.alias.clone(),
            column: projection.column.clone(),
            ty: projection.ty.clone(),
        })
        .collect();
    (grouped, Some(above))
}

fn hoist_aggregates(expr: &IRExpr, grouped: &mut Vec<IRProjection>) -> IRExpr {
    omnigraph_compiler::stack::deep(|| {
        match expr {
            IRExpr::Aggregate { .. } => {
                let ty = expr.ty().clone();
                let column = match grouped.iter().find(|p| p.expr == *expr) {
                    Some(projection) => projection.column.clone(),
                    None => {
                        let column = expr.to_string();
                        grouped.push(IRProjection {
                            expr: expr.clone(),
                            alias: None,
                            column: column.clone(),
                            ty: ty.clone(),
                        });
                        column
                    }
                };
                IRExpr::AliasRef(column, ty)
            }
            // Under any operator: unary `-`, a call and arithmetic alike.
            other => other
                .try_map_children(|child| {
                    Ok::<_, std::convert::Infallible>(hoist_aggregates(child, grouped))
                })
                .unwrap_or_else(|never| match never {}),
        }
    })
}

/// The user's keys, then the tie rule over group rows: every group key the
/// user did not write, ascending, the references (by identity) first and
/// the values after them, each in column order. An aggregate column follows
/// nothing, since the keys already tell two groups apart.
fn with_group_keys(
    order_by: &[IROrdering],
    return_exprs: &[IRProjection],
    aggregates: &[Option<crate::aggregate::AggregateSpec>],
) -> Vec<IROrdering> {
    let mut keys = order_by.to_vec();
    let groups = return_exprs
        .iter()
        .zip(aggregates)
        .filter(|(_, aggregate)| aggregate.is_none())
        .map(|(projection, _)| projection);
    let (references, values): (Vec<_>, Vec<_>) =
        groups.partition(|projection| is_reference_expr(&projection.expr));
    for projection in references.into_iter().chain(values) {
        let column = result_column(projection);
        let written = keys.iter().any(|key| match &key.expr {
            IRExpr::AliasRef(alias, _) => *alias == column,
            expr => *expr == projection.expr,
        });
        if !written {
            keys.push(IROrdering::new(
                IRExpr::AliasRef(column, projection.ty.clone()),
                false,
            ));
        }
    }
    keys
}

/// Whether `expr` is a reference column: a node or edge variable.
pub(crate) fn is_reference_expr(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::Variable(
            _,
            omnigraph_compiler::types::ExprType::Node { .. }
                | omnigraph_compiler::types::ExprType::Edge { .. }
        )
    )
}

/// A key the type checker bound to a `return` item (T42, RFC
/// 2026-09-24-shared-expression-model, "Order key binding") as the `AliasRef`
/// of that item's column; ungrouped property keys keep their source column.
pub(crate) fn bind_order_key(key: &IRExpr, return_exprs: &[IRProjection], grouped: bool) -> IRExpr {
    if matches!(key, IRExpr::AliasRef(_, _))
        || (!grouped && matches!(key, IRExpr::PropAccess { .. } | IRExpr::Literal(_, _)))
    {
        return key.clone();
    }
    return_exprs
        .iter()
        .find(|projection| projection.expr == *key)
        .map_or_else(
            || key.clone(),
            |projection| IRExpr::AliasRef(result_column(projection), projection.ty.clone()),
        )
}

/// The compiler-owned executed name of a return item.
pub(crate) fn result_column(projection: &IRProjection) -> String {
    projection.column.clone()
}

fn order_keys(expr: &IRExpr, out: &mut Vec<String>) {
    match expr {
        IRExpr::AliasRef(alias, _) => out.push(format!("{ALIAS_KEY}{alias}")),
        other => {
            let mut reads = Vec::new();
            reads_of_expr(other, &mut reads);
            out.extend(reads.iter().map(ToString::to_string));
        }
    }
}

/// Prefix of a `Sort` key that names a `return` alias, not a column.
pub const ALIAS_KEY: &str = "alias:";

/// Nodes visible in this scope. Correlated inner plans own separate bindings.
fn visible_scope(plan: &LogicalPlan, id: LogicalId) -> Vec<&LogicalNode> {
    fn walk<'a>(plan: &'a LogicalPlan, id: LogicalId, out: &mut Vec<&'a LogicalNode>) {
        let Some(node) = plan.node(id) else { return };
        out.push(node);
        if let LogicalNode::AntiJoin { input, .. } = node {
            walk(plan, *input, out);
        } else {
            for input in node.inputs() {
                walk(plan, input, out);
            }
        }
    }
    let mut nodes = Vec::new();
    walk(plan, id, &mut nodes);
    nodes
}

/// The names bound inside the inner trees of the correlated reads `id`
/// sees that the reads do not return: their rows leave without them.
fn correlated_hidden(plan: &LogicalPlan, id: LogicalId) -> HashSet<String> {
    let mut hidden = HashSet::new();
    for node in visible_scope(plan, id) {
        let LogicalNode::Correlate { inner, read, .. } = node else {
            continue;
        };
        let returned: HashSet<&str> = read.references().collect();
        for inner_node in visible_scope(plan, *inner) {
            let names: Vec<&String> = match inner_node {
                LogicalNode::TableScan { spec, .. } => spec.binding.iter().collect(),
                LogicalNode::Expand {
                    dst, edge_binding, ..
                } => std::iter::once(dst).chain(edge_binding).collect(),
                _ => Vec::new(),
            };
            hidden.extend(
                names
                    .into_iter()
                    .filter(|name| !returned.contains(name.as_str()))
                    .cloned(),
            );
        }
    }
    hidden
}

/// Candidate identity keys: every binding the query declares in
/// binding-name order, then the bindings the lowering made up (anonymous
/// endpoints, cycle temps), whose identities only order rows a reader cannot
/// tell apart. Physical spellings are used only to recognize user order
/// keys; the plan retains logical metadata.
fn scope_tiebreaks(
    plan: &LogicalPlan,
    id: LogicalId,
    source: &dyn PlanSource,
) -> Result<Vec<(ColumnRef, &'static str)>, PlanError> {
    let mut bindings = BTreeMap::new();
    for node in visible_scope(plan, id) {
        match node {
            LogicalNode::TableScan { spec, .. } => {
                if let Some(binding) = &spec.binding {
                    bindings.insert(binding.clone(), (spec.columns.id, false));
                }
            }
            LogicalNode::Expand {
                dst,
                dst_type,
                edges,
                edge_binding,
                ..
            } => {
                let id_column = source.node_type(dst_type)?.columns.id;
                bindings.insert(dst.clone(), (id_column, false));
                if let Some(binding) = edge_binding {
                    bindings.insert(binding.clone(), (id_column, edges.named().is_none()));
                }
            }
            _ => {}
        }
    }
    let hidden = correlated_hidden(plan, id);
    let mut bindings: Vec<_> = bindings
        .into_iter()
        .filter(|(binding, _)| !hidden.contains(binding))
        .collect();
    bindings.sort_by_key(|(binding, _)| plan.made_up(binding));
    let mut out = Vec::new();
    for (binding, (id_column, selected_edge)) in bindings {
        if selected_edge {
            out.push((
                ColumnRef::property(&binding, EDGE_TYPE_MEMBER),
                EDGE_TYPE_COLUMN,
            ));
        }
        out.push((ColumnRef::property(&binding, IDENTITY_MEMBER), id_column));
    }
    Ok(out)
}

/// The tie rule's columns over the rows `id` produces (RFC, `Order`): the
/// identity of every reference column the read names, in column order, the
/// order its pattern introduces them (a traversal's destination, then its
/// edge; a selected edge's `@type` before its `@id`), then every other
/// column in column order: a correlated read's or a union's value columns
/// by value, and the names the lowering made up by identity. Those are an
/// anonymous end, which declares no column, and an inlined read's
/// unreturned bindings, which stand for the values the read computes from
/// them. With `own`, the rows are an inline read's own, which hold its
/// unreturned bindings as columns until its projection, so every name
/// counts where it is bound. `written` is the read's written column order
/// ([`ReadShape::written_columns`]), empty for an inline read's own rows.
/// Each entry carries the physical column that recognizes a user key over
/// it.
fn tie_rule(
    plan: &LogicalPlan,
    id: LogicalId,
    source: &dyn PlanSource,
    own: bool,
    written: &[String],
) -> Result<Vec<(ColumnRef, String)>, PlanError> {
    let mut columns = TieColumns::default();
    columns.walk(plan, id, source)?;
    let (mut named, rest): (Vec<_>, Vec<_>) = columns
        .entries
        .into_iter()
        .partition(|entry| own || entry.is_named(plan));
    // The named references come in written column order, which the plan's
    // steps need not keep: an `optional` or a `union` runs after the
    // scope's traversals.
    named.sort_by_key(|entry| {
        written
            .iter()
            .position(|name| name == entry.binding())
            .unwrap_or(usize::MAX)
    });
    named.extend(rest);
    let mut out = Vec::new();
    for entry in named {
        match entry {
            TieColumn::Identity {
                binding,
                id_column,
                selected,
            } => {
                if selected {
                    out.push((
                        ColumnRef::property(&binding, EDGE_TYPE_MEMBER),
                        EDGE_TYPE_COLUMN.to_string(),
                    ));
                }
                out.push((ColumnRef::property(&binding, IDENTITY_MEMBER), id_column));
            }
            TieColumn::Value { binding, column } => {
                out.push((ColumnRef::property(&binding, &column), column));
            }
        }
    }
    Ok(out)
}

/// One column of a read's rows as the tie rule orders it.
#[derive(Debug, Clone)]
enum TieColumn {
    Identity {
        binding: String,
        id_column: String,
        selected: bool,
    },
    /// A value column of a correlated read or a union, `binding.column`.
    Value { binding: String, column: String },
}

impl TieColumn {
    /// A reference the query names, which leads the rule.
    fn is_named(&self, plan: &LogicalPlan) -> bool {
        matches!(self, Self::Identity { binding, .. } if !plan.made_up(binding))
    }

    fn binding(&self) -> &str {
        match self {
            Self::Identity { binding, .. } | Self::Value { binding, .. } => binding,
        }
    }
}

/// The columns of a subtree's rows in column order, each name once.
#[derive(Default)]
struct TieColumns {
    entries: Vec<TieColumn>,
    seen: HashSet<String>,
}

impl TieColumns {
    fn push(&mut self, entry: TieColumn) {
        let key = match &entry {
            TieColumn::Identity { binding, .. } => binding.clone(),
            TieColumn::Value { binding, column } => format!("{binding}.{column}"),
        };
        if self.seen.insert(key) {
            self.entries.push(entry);
        }
    }

    fn identity(&mut self, binding: &str, id_column: &str, selected: bool) {
        self.push(TieColumn::Identity {
            binding: binding.to_string(),
            id_column: id_column.to_string(),
            selected,
        });
    }

    /// Inputs before the node, so a name enters where the pattern binds it;
    /// a correlated block adds no column, a correlated read only what it
    /// returns, a union only its reference and value columns.
    fn walk(
        &mut self,
        plan: &LogicalPlan,
        id: LogicalId,
        source: &dyn PlanSource,
    ) -> Result<(), PlanError> {
        let Some(node) = plan.node(id) else {
            return Ok(());
        };
        match node {
            LogicalNode::TableScan { input, spec } => {
                if let Some(input) = input {
                    self.walk(plan, *input, source)?;
                }
                if let Some(binding) = &spec.binding {
                    self.identity(binding, spec.columns.id, false);
                }
            }
            LogicalNode::Expand {
                input,
                dst,
                dst_type,
                edges,
                edge_binding,
                ..
            } => {
                self.walk(plan, *input, source)?;
                let id_column = source.node_type(dst_type)?.columns.id;
                self.identity(dst, id_column, false);
                if let Some(edge) = edge_binding {
                    self.identity(edge, id_column, edges.named().is_none());
                }
            }
            LogicalNode::AntiJoin { input, .. } => self.walk(plan, *input, source)?,
            // A fusion's rows are its targets: each arm's other names stay
            // in the arm.
            LogicalNode::Fuse { arms, target, .. } => {
                let mut arm = TieColumns::default();
                if let Some(first) = arms.first() {
                    arm.walk(plan, first.input, source)?;
                }
                for entry in arm.entries {
                    if matches!(&entry, TieColumn::Identity { binding, .. } if binding == target) {
                        self.push(entry);
                    }
                }
            }
            LogicalNode::Correlate {
                input, inner, read, ..
            } => {
                if let Some(input) = input {
                    self.walk(plan, *input, source)?;
                }
                let mut own = TieColumns::default();
                own.walk(plan, *inner, source)?;
                for column in &read.columns {
                    match &column.expr {
                        IRExpr::Variable(name, _) => {
                            if let Some(entry) =
                                own.entries.iter().find(|entry| entry.binding() == name)
                            {
                                self.push(entry.clone());
                            }
                        }
                        _ => self.push(TieColumn::Value {
                            binding: read.values.clone(),
                            column: column.column.clone(),
                        }),
                    }
                }
            }
            LogicalNode::Union { left, bindings, .. } => {
                // Every arm binds the same columns under the first arm's
                // names: the leftmost arm's give their kind and order.
                let mut arm = TieColumns::default();
                arm.walk(plan, *left, source)?;
                for entry in arm.entries {
                    let leaves = match &entry {
                        TieColumn::Identity { binding, .. } => bindings.contains(binding),
                        TieColumn::Value { .. } => true,
                    };
                    if leaves {
                        self.push(entry);
                    }
                }
            }
            other => {
                for input in other.inputs() {
                    self.walk(plan, input, source)?;
                }
            }
        }
        Ok(())
    }
}

/// The metadata columns a sort appends after `keys` so its order is total;
/// none for group rows, when every returned expression is a key (equal rows
/// are indistinguishable), or for an individual column already used as a key.
fn sort_tiebreak(
    candidates: &[(ColumnRef, String)],
    keys: &[IROrdering],
    returns: &[IRProjection],
    aggregate: bool,
) -> Vec<ColumnRef> {
    if aggregate {
        return Vec::new();
    }
    let key_texts: Vec<String> = keys
        .iter()
        .map(|key| match &key.expr {
            IRExpr::AliasRef(alias, _) => returns
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
    candidates
        .iter()
        .filter(|(column, physical)| {
            !keys.iter().any(|key| {
                let expr = match &key.expr {
                    IRExpr::AliasRef(alias, _) => returns
                        .iter()
                        .find(|projection| projection.alias.as_deref() == Some(alias))
                        .map_or(&key.expr, |projection| &projection.expr),
                    expr => expr,
                };
                matches!(
                    expr,
                    IRExpr::PropAccess { variable, property , ty: _}
                        if variable == &column.binding && property == physical
                )
            })
        })
        .map(|(column, _)| column.clone())
        .collect()
}

/// Every column an expression reads. `count($v)` reads the identity alone;
/// an alias, a parameter and a literal read nothing.
pub(crate) fn reads_of_expr(expr: &IRExpr, out: &mut Vec<ColumnRef>) {
    omnigraph_compiler::stack::deep(|| {
        // A lexical predicate reads its binding's identity: the exact lexical
        // scan reads the field from the table itself and answers each row by its
        // identity, so its input never carries the field for it.
        if let IRExpr::Call {
            func: omnigraph_compiler::ir::expr::ScalarFunc::MatchTerms { .. },
            args,
            ..
        } = expr
            && let Some(IRExpr::PropAccess { variable, .. }) = args.first()
        {
            out.push(ColumnRef::property(variable, IDENTITY_MEMBER));
            args[1..].iter().for_each(|arg| reads_of_expr(arg, out));
            return;
        }
        match expr {
            IRExpr::Binary { left, right, .. } => {
                reads_of_expr(left, out);
                reads_of_expr(right, out);
            }
            IRExpr::Not(inner, _)
            | IRExpr::IsNull { expr: inner, .. }
            | IRExpr::Neg { expr: inner, .. }
            | IRExpr::Cast { expr: inner, .. } => reads_of_expr(inner, out),
            IRExpr::Call { args, .. } => args.iter().for_each(|arg| reads_of_expr(arg, out)),
            IRExpr::PropAccess {
                variable,
                property,
                ty: _,
            } => {
                out.push(ColumnRef::property(variable, property));
            }
            IRExpr::Variable(variable, _) => out.push(ColumnRef::entity(variable)),
            IRExpr::Nearest {
                variable,
                property,
                query,
                ..
            } => {
                out.push(ColumnRef::property(variable, property));
                reads_of_expr(query, out);
            }
            IRExpr::Aggregate { func, arg, .. } => match (func, arg.as_ref()) {
                (AggFunc::Count, IRExpr::Variable(variable, ty)) => {
                    // An edge over several types is identified by its type and
                    // its id together.
                    if matches!(ty, ExprType::Edge { type_name } if type_name.contains('|')) {
                        out.push(ColumnRef::property(variable, EDGE_TYPE_MEMBER));
                    }
                    out.push(ColumnRef::property(variable, IDENTITY_MEMBER));
                }
                _ => reads_of_expr(arg, out),
            },
            IRExpr::AliasRef(_, _) | IRExpr::Param(_, _) | IRExpr::Literal(_, _) => {}
        }
    })
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
                access: None,
                side: side_id,
                table: side.table.clone(),
                version: Some(side.version),
                columns: side.columns,
                fragments: None,
                projection: None,
                filter: None,
                binding: None,
                runtime_filter: None,
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
    rewrite_with(plan, source, &mut Tracer::default())
}

/// [`rewrite`], recording the rules it applies to an exact-fragment chain.
pub(crate) fn rewrite_with(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    tracer: &mut Tracer,
) -> Result<Vec<&'static str>, PlanError> {
    let mut fired = Vec::new();
    let query = is_query_plan(plan);
    for pass in PassId::of_stage(PassStage::Rewrite) {
        charge_pass(source)?;
        let applied = match pass {
            PassId::Resolve => true,
            PassId::PredicatePushdown => {
                if query {
                    crate::placement::place_conjuncts(plan, source, tracer)
                } else {
                    resume_pushdown(plan)
                }
            }
            PassId::AggregatePushdown => query && aggregate_pushdown(plan)?,
            PassId::ProjectionPushdown => query && projection_pushdown(plan, source, tracer)?,
            PassId::FragmentScope
            | PassId::AddressShortCircuit
            | PassId::LateMaterialization
            | PassId::JoinAlgorithm
            | PassId::ExpandMode
            | PassId::AccessPath
            | PassId::AntiJoinStrategy
            | PassId::KeyToId
            | PassId::ScanAccess => continue,
        };
        if applied {
            fired.push(pass.name());
        }
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
    fired: Vec<&'static str>,
) -> Result<Optimized, PlanError> {
    physical_plan_with(plan, source, bounds, fired, &mut Tracer::default())
}

/// [`physical_plan`], recording the rules it applies to an exact-fragment
/// chain.
pub(crate) fn physical_plan_with(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    bounds: &Bounds,
    mut fired: Vec<&'static str>,
    tracer: &mut Tracer,
) -> Result<Optimized, PlanError> {
    let query = is_query_plan(plan);
    // Every physical and finalizing pass, charged before the stage runs.
    for _ in PassId::of_stage(PassStage::Physical).chain(PassId::of_stage(PassStage::Finalize)) {
        charge_pass(source)?;
    }
    let before = plan
        .schema(plan.root())
        .cloned()
        .ok_or_else(|| PlanError::Internal("resolved plan root has no schema".to_string()))?;
    for pass in PassId::of_stage(PassStage::Physical) {
        let applied = match pass {
            PassId::FragmentScope => fragment_scope(plan, source),
            PassId::AddressShortCircuit => address_short_circuit(plan, source),
            PassId::LateMaterialization
            | PassId::JoinAlgorithm
            | PassId::ExpandMode
            | PassId::AccessPath
            | PassId::AntiJoinStrategy => break,
            PassId::Resolve
            | PassId::PredicatePushdown
            | PassId::AggregatePushdown
            | PassId::ProjectionPushdown
            | PassId::KeyToId
            | PassId::ScanAccess => continue,
        };
        if applied {
            fired.push(pass.name());
        }
    }
    let key_sets = key_set_trees(plan, source)?;
    let mut lowering = Lowering {
        logical: plan,
        source,
        bounds,
        key_sets: &key_sets,
        physical: PhysicalPlan::new(),
        late_materialization: false,
        join_algorithm: false,
        expand_mode: false,
        access_path: false,
        anti_join_strategy: false,
        csr_cached: false,
        decisions: Vec::new(),
        ranking: None,
        limit: None,
        tracer: std::mem::take(tracer),
    };
    let root = lowering.lower(plan.root());
    *tracer = std::mem::take(&mut lowering.tracer);
    let root = root?;
    let Lowering {
        mut physical,
        late_materialization,
        join_algorithm,
        expand_mode,
        access_path,
        anti_join_strategy,
        mut decisions,
        ..
    } = lowering;
    physical.set_root(root);
    let late_query =
        query && materialize_returns_late(&mut physical, plan, source, &mut decisions, tracer)?;
    let contains_join = physical
        .live()
        .any(|(_, node)| matches!(node, PhysicalNode::ContainsJoin { .. }));
    for pass in PassId::of_stage(PassStage::Physical) {
        let applied = match pass {
            PassId::LateMaterialization => late_materialization || late_query,
            PassId::JoinAlgorithm => join_algorithm || contains_join,
            PassId::ExpandMode => expand_mode,
            PassId::AccessPath => access_path,
            PassId::AntiJoinStrategy => anti_join_strategy,
            PassId::FragmentScope
            | PassId::AddressShortCircuit
            | PassId::Resolve
            | PassId::PredicatePushdown
            | PassId::AggregatePushdown
            | PassId::ProjectionPushdown
            | PassId::KeyToId
            | PassId::ScanAccess => continue,
        };
        if applied {
            fired.push(pass.name());
        }
    }
    let mut statistics = decisions;
    statistics.extend(derive_properties(&mut physical, source, bounds)?);
    let after = physical
        .properties(physical.root())
        .map(|properties| properties.schema.clone())
        .ok_or_else(|| PlanError::Internal("physical root has no properties".to_string()))?;
    root_shape_kept(&before, &after)?;
    crate::validate_aggregate_specs(&physical)?;
    crate::validate_output_schemas(&physical)?;
    Ok(Optimized {
        physical,
        fired,
        statistics,
    })
}

/// The key-set tree of every `AntiJoin` whose block qualifies (`key_set.rs`),
/// built before physical selection, which lowers it in place of the
/// correlated tree when pass 10 prices it lower.
fn key_set_trees(
    plan: &LogicalPlan,
    source: &dyn PlanSource,
) -> Result<HashMap<LogicalId, KeySetTree>, PlanError> {
    let mut trees = HashMap::new();
    for (id, node) in plan.live() {
        // A block that can fail stays correlated (`key_set_tree`); the
        // type checker's flag says which.
        if let LogicalNode::AntiJoin {
            inner,
            outer_var,
            predicate,
            fails: false,
            ..
        } = node
            && let Some(tree) = key_set_tree(plan, *inner, outer_var, predicate, source)?
        {
            trees.insert(id, tree);
        }
    }
    Ok(trees)
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

/// Query and diff lowering preserve the declared root schema. Merge roots
/// already derive their shape from the three-way classification.
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
        && left.fields().iter().zip(right.fields()).all(|(l, r)| {
            l.name() == r.name()
                && l.data_type() == r.data_type()
                && l.is_nullable() == r.is_nullable()
        })
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

/// A filtered `CrossJoin`'s sides, swapped when a `contains` searches only the
/// left scan's text for the right's and the right's known rows are no more:
/// the right side streams through the engine's needle filter.
fn searched_side_right(
    logical: &LogicalPlan,
    source: &dyn PlanSource,
    left: LogicalId,
    right: LogicalId,
    conjuncts: &[IRExpr],
) -> (LogicalId, LogicalId) {
    let scan = |id: LogicalId| match logical.node(id) {
        Some(LogicalNode::TableScan { input: None, spec }) => Some(spec),
        _ => None,
    };
    let (Some(left_scan), Some(right_scan)) = (scan(left), scan(right)) else {
        return (left, right);
    };
    let (Some(on_left), Some(on_right)) =
        (left_scan.binding.as_deref(), right_scan.binding.as_deref())
    else {
        return (left, right);
    };
    let searches = |haystack: &str, needle: &str| {
        conjuncts
            .iter()
            .filter_map(TextContains::of)
            .any(|shape| shape.haystack.0 == haystack && shape.needle.0 == needle)
    };
    let rows = |spec: &ScanSpec| {
        scan_row_estimate(spec, source).map_or(Estimate::Unknown, Estimate::Known)
    };
    let needle_side_no_larger = matches!(
        (rows(right_scan), rows(left_scan)),
        (Estimate::Known(needles), Estimate::Known(haystacks)) if needles <= haystacks
    );
    if needle_side_no_larger && searches(on_left, on_right) && !searches(on_right, on_left) {
        (right, left)
    } else {
        (left, right)
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
                || spec.table.is_catalogue()
                || return_exprs.is_empty()
                || plan.parent_of(*input) != Some(id)
            {
                return None;
            }
            let eligible = return_exprs.iter().all(|projection| {
                matches!(
                    &projection.expr,
                    IRExpr::Aggregate { func: AggFunc::Count, arg, .. }
                        if matches!(arg.as_ref(), IRExpr::Variable(variable, _) if variable == binding)
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
    return_schema(return_exprs, &[])
}

/// Project independent and dependent query scans from binding demand; the id
/// column is a demand like any other (`@id`, a whole entity, `identity_reads`).
/// A diff or merge plan holds no `Projection` node and never reaches this pass.
fn projection_pushdown(
    plan: &mut LogicalPlan,
    source: &dyn PlanSource,
    tracer: &mut Tracer,
) -> Result<bool, PlanError> {
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
        tracer.record(
            &[Role::Scan],
            Rule::PruneScanColumns {
                columns: projection.clone(),
            },
            &[],
        );
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
/// a dependent scan, an anti-join's outer rows, and a ranked scan's binding.
fn identity_reads(node: &LogicalNode) -> Vec<String> {
    match node {
        LogicalNode::TableScan {
            input: Some(_),
            spec,
        } => spec.binding.iter().cloned().collect(),
        LogicalNode::Expand { src, dst, .. } => vec![src.clone(), dst.clone()],
        LogicalNode::AntiJoin { outer_var, .. } => vec![outer_var.clone()],
        // A group keyed by a reference groups by its identity.
        LogicalNode::Correlate { read, .. } => read
            .group
            .iter()
            .flatten()
            .filter_map(|key| match &key.expr {
                IRExpr::Variable(name, _) => Some(name.clone()),
                _ => None,
            })
            .collect(),
        LogicalNode::Nearest { binding, .. } => vec![binding.clone()],
        // A full-text call a `Filter` or a `let` evaluates is membership of
        // the row's identity in the call's matches.
        LogicalNode::Filter { conjuncts, .. } => {
            let mut out = Vec::new();
            conjuncts
                .iter()
                .for_each(|conjunct| lexical_bindings(conjunct, &mut out));
            out
        }
        LogicalNode::Let { value, .. } => {
            let mut out = Vec::new();
            lexical_bindings(value, &mut out);
            out
        }
        _ => Vec::new(),
    }
}

/// Whether `expr` holds a full-text call (`search`, `fuzzy`, `match_text`,
/// `match_terms`).
pub(crate) fn has_lexical_call(expr: &IRExpr) -> bool {
    let mut bindings = Vec::new();
    lexical_bindings(expr, &mut bindings);
    !bindings.is_empty()
}

/// The binding of every full-text call (`search`, `fuzzy`, `match_text`,
/// `match_terms`) in `expr`, whose field is that binding's property.
fn lexical_bindings(expr: &IRExpr, out: &mut Vec<String>) {
    omnigraph_compiler::stack::deep(|| {
        if let Some(field) = expr.lexical_predicate_field() {
            if let IRExpr::PropAccess { variable, .. } = field
                && !out.contains(variable)
            {
                out.push(variable.clone());
            }
            return;
        }
        match expr {
            IRExpr::Binary { left, right, .. } => {
                lexical_bindings(left, out);
                lexical_bindings(right, out);
            }
            IRExpr::Not(inner, _)
            | IRExpr::Cast { expr: inner, .. }
            | IRExpr::Neg { expr: inner, .. }
            | IRExpr::IsNull { expr: inner, .. } => lexical_bindings(inner, out),
            IRExpr::Call { args, .. } => args.iter().for_each(|arg| lexical_bindings(arg, out)),
            // The predicates themselves were taken above.
            IRExpr::PropAccess { .. }
            | IRExpr::Nearest { .. }
            | IRExpr::Variable(..)
            | IRExpr::Param(..)
            | IRExpr::Literal(..)
            | IRExpr::Aggregate { .. }
            | IRExpr::AliasRef(..) => {}
        }
    })
}

/// The columns one node reads through query bindings. A `Fuse` reads its
/// target whole in every arm: each target leaves with the columns of the
/// first arm that selected it, under one schema, so a pruned target column
/// in one arm would break the others.
fn node_reads(node: &LogicalNode) -> Vec<ColumnRef> {
    let mut out = Vec::new();
    match node {
        LogicalNode::TableScan { input: _, spec } => {
            let ScanSpec {
                access: _,
                side: _,
                table: _,
                version: _,
                columns: _,
                fragments: _,
                projection: _,
                filter,
                binding: _,
                runtime_filter: _,
            } = spec.as_ref();
            if let Some(predicate) = filter {
                predicate_reads(predicate, &mut out);
            }
        }
        LogicalNode::Filter {
            input: _,
            conjuncts,
            ..
        } => {
            for conjunct in conjuncts {
                reads_of_expr(conjunct, &mut out);
            }
        }
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
            tiebreak,
        } => {
            out.extend(
                keys.iter()
                    .filter(|key| !key.starts_with(ALIAS_KEY))
                    .map(|key| ColumnRef::parse(key)),
            );
            out.extend(tiebreak.iter().cloned());
        }
        LogicalNode::Nearest {
            input: _,
            binding: _,
            property: _,
            query: _,
            k: _,
            reads,
            leads: _,
            recall: _,
        }
        | LogicalNode::Aggregate {
            input: _,
            reads,
            return_exprs: _,
        } => out.extend(reads.iter().cloned()),
        LogicalNode::Fuse {
            arms: _,
            target,
            k: _,
            weights: _,
            reads,
        } => {
            out.push(ColumnRef::entity(target));
            out.extend(reads.iter().cloned());
        }
        LogicalNode::AntiJoin {
            input: _,
            inner: _,
            outer_var: _,
            predicate,
            ..
        } => {
            if let Some(arg) = predicate.left.arg() {
                reads_of_expr(arg, &mut out);
            }
            reads_of_expr(&predicate.right, &mut out);
        }
        LogicalNode::TargetCut {
            input: _,
            binding,
            targets: _,
        } => out.push(ColumnRef::property(binding, IDENTITY_MEMBER)),
        LogicalNode::Score {
            input: _,
            binding: _,
            type_name: _,
            property: _,
            kind: _,
            query: _,
            reads,
        } => out.extend(reads.iter().cloned()),
        LogicalNode::Let {
            input: _,
            name: _,
            value: _,
            reads,
        } => out.extend(reads.iter().cloned()),
        LogicalNode::Correlate {
            input: _,
            inner: _,
            outer_var: _,
            read: _,
            tiebreak,
            reads,
            ..
        } => {
            out.extend(reads.iter().cloned());
            out.extend(tiebreak.iter().cloned());
        }
        LogicalNode::MetadataCount {
            spec: _,
            return_exprs: _,
        }
        | LogicalNode::Expand {
            input: _,
            src: _,
            dst: _,
            edges: _,
            src_type: _,
            dst_type: _,
            min_hops: _,
            max_hops: _,
            edge_binding: _,
        }
        | LogicalNode::OuterReference { outer_var: _ }
        | LogicalNode::OptionalJoin {
            input: _,
            inner: _,
            outer_var: _,
            fails: _,
        }
        | LogicalNode::Union {
            left: _,
            right: _,
            bindings: _,
        }
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
        }
        | LogicalNode::CsvSource { spec: _ }
        | LogicalNode::DeltaSource { spec: _ }
        | LogicalNode::Validate {
            input: _,
            table: _,
            checks: _,
            replaced: _,
        }
        | LogicalNode::FragmentWrite { input: _, spec: _ } => {}
    }
    out
}

fn predicate_reads(predicate: &Predicate, out: &mut Vec<ColumnRef>) {
    omnigraph_compiler::stack::deep(|| match predicate {
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
    })
}

/// Pass 6: a return column only the output reads leaves its binding's scan
/// (row address instead) for a `HydrateColumns` above the root `Limit` when
/// the scan estimates over `HYDRATE_ROW_RATIO` rows per kept row; a fusion's two arms together.
fn materialize_returns_late(
    physical: &mut PhysicalPlan,
    logical: &LogicalPlan,
    source: &dyn PlanSource,
    decisions: &mut Vec<StatisticSource>,
    tracer: &mut Tracer,
) -> Result<bool, PlanError> {
    let root = physical.root();
    let Some(&PhysicalNode::Limit { input, rows: limit }) = physical.node(root) else {
        return Ok(false);
    };
    if limit == 0 {
        return Ok(false);
    }
    let mut id = input;
    let mut alias_keys: HashSet<String> = HashSet::new();
    while let Some(PhysicalNode::Sort {
        input, order_by, ..
    }) = physical.node(id)
    {
        alias_keys.extend(order_by.iter().filter_map(|key| match &key.expr {
            IRExpr::AliasRef(alias, _) => Some(alias.clone()),
            _ => None,
        }));
        id = *input;
    }
    let Some(PhysicalNode::Projection { return_exprs, .. }) = physical.node(id) else {
        return Ok(false);
    };
    let return_exprs = return_exprs.clone();
    // A union's and a fusion's arms bind one name through several scans,
    // and an optional leaves its names' row addresses null on the rows it
    // kept without a match: none hydrates a return column by one binding's
    // address.
    if physical.live().any(|(_, node)| {
        matches!(
            node,
            PhysicalNode::Aggregate { .. }
                | PhysicalNode::Union { .. }
                | PhysicalNode::Fuse { .. }
                | PhysicalNode::OptionalJoin { .. }
                | PhysicalNode::Score { .. }
                | PhysicalNode::Let { .. }
                | PhysicalNode::Correlate { .. }
        )
    }) {
        return Ok(false);
    }
    let Some(returned) = logical_return_projection(logical) else {
        return Ok(false);
    };

    // What decides rows: every read outside the return, then the return's
    // own reads that are not a bare, unsorted property.
    let mut kept: HashSet<(String, String)> = HashSet::new();
    let mut whole: HashSet<String> = HashSet::new();
    let mut keep = |read: ColumnRef| match read.property {
        Some(property) => {
            kept.insert((read.binding, property));
        }
        None => {
            whole.insert(read.binding);
        }
    };
    for (logical_id, node) in logical.live() {
        if logical_id == returned {
            continue;
        }
        node_reads(node).into_iter().for_each(&mut keep);
    }
    let mut candidates: BTreeMap<String, Vec<HydratedColumn>> = BTreeMap::new();
    for (position, item) in return_exprs.iter().enumerate() {
        let output = result_column(item);
        match (&item.expr, output) {
            (
                IRExpr::PropAccess {
                    variable, property, ..
                },
                output,
            ) if !alias_keys.contains(&output) => {
                candidates
                    .entry(variable.clone())
                    .or_default()
                    .push(HydratedColumn {
                        position,
                        output,
                        property: property.clone(),
                    });
            }
            (expr, _) => {
                let mut reads = Vec::new();
                reads_of_expr(expr, &mut reads);
                reads.into_iter().for_each(&mut keep);
            }
        }
    }

    let threshold = u64::try_from(limit)
        .unwrap_or(u64::MAX)
        .saturating_mul(HYDRATE_ROW_RATIO);
    let mut bindings = Vec::new();
    let mut rewrites: Vec<(NodeId, Vec<String>)> = Vec::new();
    for (binding, columns) in candidates {
        if whole.contains(&binding) {
            continue;
        }
        let scans: Vec<NodeId> = physical
            .live()
            .filter(|(_, node)| {
                matches!(node, PhysicalNode::Scan { spec, .. }
                    if spec.binding.as_deref() == Some(binding.as_str()))
            })
            .map(|(scan, _)| scan)
            .collect();
        let [scan] = scans[..] else {
            continue;
        };
        let Some(PhysicalNode::Scan {
            spec,
            ranked,
            keys_only: false,
            ..
        }) = physical.node(scan)
        else {
            continue;
        };
        if ranked.is_some() {
            continue;
        }
        let Some(projection) = spec.projection.as_ref() else {
            continue;
        };
        if spec.table.is_catalogue() {
            continue;
        }
        let node_type = node_type_of(spec, source)?;
        let deferrable = |property: &str| {
            !kept.contains(&(binding.clone(), property.to_string()))
                && !node_type.key.iter().any(|key| key == property)
                && property != node_type.columns.id
                && projection.iter().any(|column| column == property)
                && node_type
                    .schema
                    .field_with_name(property)
                    .is_ok_and(|field| {
                        node_type.object_columns.iter().any(|name| name == property)
                            || matches!(field.data_type(), DataType::FixedSizeList(..))
                    })
        };
        let columns: Vec<HydratedColumn> = columns
            .into_iter()
            .filter(|column| deferrable(&column.property))
            .collect();
        if columns.is_empty() {
            continue;
        }
        let rows = scan_row_estimate(spec, source);
        decisions.push(StatisticSource {
            statistic: format!("hydrate_rows({binding})"),
            value: format!(
                "{} rows, limit {limit}, ratio {HYDRATE_ROW_RATIO}",
                rows.map_or_else(|| "unknown".to_string(), |rows| rows.to_string())
            ),
            origin: "manifest row count, at most one row under a key equality".to_string(),
        });
        if rows.is_some_and(|rows| rows <= threshold) {
            continue;
        }
        let mut read: Vec<String> = projection
            .iter()
            .filter(|column| !columns.iter().any(|deferred| &deferred.property == *column))
            .cloned()
            .collect();
        if !read.iter().any(|column| column == ROW_ADDR) {
            read.push(ROW_ADDR.to_string());
        }
        rewrites.extend(scans.iter().map(|scan| (*scan, read.clone())));
        bindings.push(HydratedBinding {
            binding,
            table: spec.table.clone(),
            columns,
        });
    }
    if bindings.is_empty() {
        return Ok(false);
    }
    for (scan, read) in rewrites {
        if let Some(PhysicalNode::Scan { spec, .. }) = physical.node_mut(scan) {
            spec.projection = Some(read);
        }
    }
    if let [hydrated] = bindings.as_slice() {
        tracer.record_with(
            &[Role::Scan],
            &[Role::Scan, Role::Hydrate],
            Rule::HydrateReturnColumns {
                columns: hydrated.columns.clone(),
            },
        );
    }
    let hydrate = physical.add(PhysicalNode::HydrateColumns {
        input: root,
        bindings,
    });
    physical.set_root(hydrate);
    Ok(true)
}

/// The query's return `Projection`: under the root `Limit` and `Sort`s.
fn logical_return_projection(plan: &LogicalPlan) -> Option<LogicalId> {
    let mut id = plan.root();
    loop {
        match plan.node(id)? {
            LogicalNode::Limit { input, .. } | LogicalNode::Sort { input, .. } => id = *input,
            LogicalNode::Projection { .. } => return Some(id),
            _ => return None,
        }
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
                    if let Some((from, to)) = proof.version_window {
                        spec.filter = Some(and_filter(
                            spec.filter.take(),
                            Predicate::VersionWindow { from, to },
                        ));
                    }
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
    /// The arena `lower` reads: the plan's, or a key-set tree's while one is
    /// lowered in place of its block's correlated tree.
    logical: &'a LogicalPlan,
    source: &'a dyn PlanSource,
    bounds: &'a Bounds,
    /// The key-set tree of each qualifying `AntiJoin`, by its id in the
    /// plan's arena. A tree's own ids overlap these, so a tree must hold no
    /// `AntiJoin` to look up while it is lowered (checked at the swap).
    key_sets: &'a HashMap<LogicalId, KeySetTree>,
    physical: PhysicalPlan,
    late_materialization: bool,
    join_algorithm: bool,
    /// Pass 8 fired: an `Expand`'s mode came from the cost model.
    expand_mode: bool,
    /// Pass 9 fired: a dependent scan's access path came from the cost model.
    access_path: bool,
    /// Pass 10 fired: a correlated block's strategy came from the cost model.
    anti_join_strategy: bool,
    /// An earlier lowered node of this plan realizes the query's CSR (an
    /// `Expand` in `Csr` mode or an `AntiJoin`), so a later `Expand` reuses
    /// it for free. Lowering order is execution order (inputs first).
    csr_cached: bool,
    decisions: Vec<StatisticSource>,
    /// The ranking lowered below the node being lowered, if any.
    ranking: Option<Ranking>,
    /// The `limit` above the node being lowered: the fetch of the score sort.
    limit: Option<usize>,
    /// The record of the rules lowering applies to an exact-fragment chain.
    tracer: Tracer,
}

/// What a leading search function became in the physical plan: the score
/// keys the query's `Sort` leads with, one per ranked scan.
enum Ranking {
    Scores(Vec<IROrdering>),
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
            // Lowering roots the component holding the ranked binding at that
            // binding, so a ranked scan a traversal reaches is its defect.
            return Err(PlanError::Internal(format!(
                "the ranked binding `${binding}` is a traversal destination; lowering roots a ranked binding's component at it"
            )));
        }
        self.unmark(scan);
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

    /// A ranked scan runs under a search mode and takes no runtime filter: the
    /// marker `filtered_cross_join` put on `scan` comes off, and the
    /// `ContainsJoin` above it goes back to the `CrossJoin` of every conjunct.
    fn unmark(&mut self, scan: NodeId) {
        let Some(PhysicalNode::Scan { spec, .. }) = self.physical.node_mut(scan) else {
            return;
        };
        if spec.runtime_filter.take().is_none() {
            return;
        }
        let join = self.physical.live().find_map(|(id, node)| match node {
            PhysicalNode::ContainsJoin { right, .. } if *right == scan => Some(id),
            _ => None,
        });
        let Some(join) = join else {
            return;
        };
        let Some(PhysicalNode::ContainsJoin {
            left,
            right,
            haystack,
            needle,
            conjunct,
            residual,
        }) = self.physical.node(join).cloned()
        else {
            return;
        };
        let fields = ContainsJoinFields {
            haystack: (&haystack.0, &haystack.1),
            needle: (&needle.0, &needle.1),
            conjunct: &conjunct,
            residual: &residual,
        };
        let mut filters = Vec::with_capacity(residual.len() + 1);
        filters.push(fields.conjunct());
        filters.extend(residual);
        if let Some(node) = self.physical.node_mut(join) {
            *node = PhysicalNode::CrossJoin {
                left,
                right,
                filters,
            };
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
        if self.logical.traversal_work_limit().is_some() {
            return Ok(Prefilter {
                ranked_type,
                hops: Vec::new(),
                feeds,
            });
        }
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
                    edges,
                    min_hops,
                    ..
                }) if src == binding && *min_hops > 0 => {
                    let Some(member) = edges.named() else {
                        return Err(PlanError::Internal(
                            "an unbudgeted prefilter contains an edge selection".to_string(),
                        ));
                    };
                    hops.push(Hop {
                        edge_type: member.edge_type.clone(),
                        direction: member.direction,
                    });
                }
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

    /// A `ContainsJoin` on the first conjunct `contains_join_applies` admits (pass 7), else a
    /// `CrossJoin` of every conjunct. The haystack scan must be on the right: written second,
    /// or swapped by `searched_side_right` (plain scans, `Known` rows, needles <= haystacks).
    fn filtered_cross_join(
        &mut self,
        left: NodeId,
        right: NodeId,
        conjuncts: &[IRExpr],
    ) -> Result<NodeId, PlanError> {
        let mut chosen = None;
        for (index, conjunct) in conjuncts.iter().enumerate() {
            if let Some(shape) = TextContains::of(conjunct)
                && self.contains_join_applies(left, right, shape)?
            {
                chosen = Some((index, shape));
                break;
            }
        }
        let Some((index, shape)) = chosen else {
            return Ok(self.physical.add(PhysicalNode::CrossJoin {
                left,
                right,
                filters: conjuncts.to_vec(),
            }));
        };
        let haystack = (shape.haystack.0.to_string(), shape.haystack.1.to_string());
        let needle = (shape.needle.0.to_string(), shape.needle.1.to_string());
        let residual = conjuncts
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, conjunct)| conjunct.clone())
            .collect();
        let Some(PhysicalNode::Scan { spec, .. }) = self.physical.node_mut(right) else {
            return Err(PlanError::Internal(format!(
                "the right side {right} of a contains join is not a table scan"
            )));
        };
        spec.runtime_filter = Some(RuntimeFilterSpec {
            column: haystack.1.clone(),
            needle: needle.clone(),
            kind: RuntimeFilterKind::TextContainsAny,
        });
        Ok(self.physical.add(PhysicalNode::ContainsJoin {
            left,
            right,
            haystack,
            needle,
            conjunct: conjuncts[index].clone(),
            residual,
        }))
    }

    /// Whether `right` is the unranked table scan of `shape`'s haystack
    /// binding, the needle binding is scanned under `left`, and both
    /// properties are `Utf8` in their node types' schemas.
    fn contains_join_applies(
        &self,
        left: NodeId,
        right: NodeId,
        shape: TextContains<'_>,
    ) -> Result<bool, PlanError> {
        let Some(PhysicalNode::Scan {
            source: ScanInput::Table,
            ranked: None,
            spec: haystack,
            ..
        }) = self.physical.node(right)
        else {
            return Ok(false);
        };
        if haystack.binding.as_deref() != Some(shape.haystack.0) || haystack.table.is_catalogue() {
            return Ok(false);
        }
        let Some(needle) = self.scan_of_binding(left, shape.needle.0) else {
            return Ok(false);
        };
        let text = |spec: &ScanSpec, property: &str| -> Result<bool, PlanError> {
            Ok(node_type_of(spec, self.source)?
                .schema
                .field_with_name(property)
                .is_ok_and(|field| field.data_type() == &DataType::Utf8))
        };
        Ok(text(haystack, shape.haystack.1)? && text(needle, shape.needle.1)?)
    }

    /// The spec of the scan bound to `binding` in the subtree under `id`.
    fn scan_of_binding(&self, id: NodeId, binding: &str) -> Option<&ScanSpec> {
        self.physical
            .subtree(id)
            .into_iter()
            .find_map(|node| match self.physical.node(node) {
                Some(PhysicalNode::Scan { spec, .. })
                    if spec.binding.as_deref() == Some(binding) =>
                {
                    Some(spec.as_ref())
                }
                _ => None,
            })
    }

    /// The `Sort` a query's return runs under (RFC 0047 §One total order): the
    /// ranking's score keys then the user's; over group rows (an aggregate or
    /// a `distinct` return, with or without a ranking), the user's keys then
    /// the tie rule's group keys ([`with_group_keys`]).
    fn sort_keys(
        &self,
        input: NodeId,
        order_by: &[IROrdering],
        fetch: Option<usize>,
    ) -> Option<Vec<IROrdering>> {
        let aggregate_at = |id: NodeId| match self.physical.node(id) {
            Some(PhysicalNode::Aggregate {
                return_exprs,
                aggregates,
                ..
            }) => Some((return_exprs, aggregates)),
            _ => None,
        };
        let grouped = aggregate_at(input).or_else(|| match self.physical.node(input) {
            Some(PhysicalNode::Projection { input: below, .. }) => aggregate_at(*below),
            _ => None,
        });
        if let Some((return_exprs, aggregates)) = grouped {
            // A search leads no sort of groups: with nothing written and no
            // `limit` they leave unordered.
            if self.ranking.is_some() && order_by.is_empty() && fetch.is_none() {
                return None;
            }
            return Some(with_group_keys(order_by, return_exprs, aggregates));
        }
        let Some(ranking) = &self.ranking else {
            return Some(order_by.to_vec());
        };
        let Ranking::Scores(scores) = ranking;
        let mut keys = scores.clone();
        keys.extend(order_by.iter().cloned());
        Some(keys)
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
                    self.tracer.record(&[Role::Scan], Rule::Lower, &[]);
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
                let Some(order_by) = self.sort_keys(lowered, order_by, *fetch) else {
                    return Ok(lowered);
                };
                self.tracer.record(&[Role::Sort], Rule::Lower, &[]);
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
                    filters: Vec::new(),
                }))
            }
            LogicalNode::Filter {
                input,
                conjuncts,
                fails,
            } => {
                // Conjuncts that cannot fail commute, so they may run as the
                // join's filters in any order. A node holding one that can
                // fail filters above the join, left to right as written: a
                // conjunct written after it must not narrow the rows it sees.
                if !*fails
                    && let Some(LogicalNode::CrossJoin { left, right }) = logical.node(*input)
                {
                    // A conjunct holding a full-text operand stays in a
                    // `Filter` above the join, the one operator that reads
                    // full-text calls as membership.
                    let (lexical, joined): (Vec<IRExpr>, Vec<IRExpr>) =
                        conjuncts.iter().cloned().partition(has_lexical_call);
                    let (left, right) =
                        searched_side_right(logical, self.source, *left, *right, &joined);
                    let left_lowered = self.lower(left)?;
                    let right_lowered = self.lower(right)?;
                    let join = self.filtered_cross_join(left_lowered, right_lowered, &joined)?;
                    if lexical.is_empty() {
                        return Ok(join);
                    }
                    self.tracer.record(&[Role::Filter], Rule::Lower, &[]);
                    return Ok(self.physical.add(PhysicalNode::Filter {
                        input: join,
                        filters: lexical,
                    }));
                }
                let lowered = self.lower(*input)?;
                self.tracer.record(&[Role::Filter], Rule::Lower, &[]);
                Ok(self.physical.add(PhysicalNode::Filter {
                    input: lowered,
                    filters: conjuncts.clone(),
                }))
            }
            LogicalNode::Projection {
                input,
                return_exprs,
                ..
            } => {
                let lowered = self.lower(*input)?;
                self.tracer.record(&[Role::Projection], Rule::Lower, &[]);
                Ok(self.physical.add(PhysicalNode::Projection {
                    input: lowered,
                    return_exprs: return_exprs.clone(),
                    node_objects: node_object_types(return_exprs, self.source)?,
                }))
            }
            LogicalNode::Aggregate {
                input,
                return_exprs,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let aggregates = return_exprs
                    .iter()
                    .map(|projection| match &projection.expr {
                        IRExpr::Aggregate {
                            func, signature, ..
                        } => crate::plan_aggregate(*func, signature).map(Some),
                        _ => Ok(None),
                    })
                    .collect::<Result<Vec<_>, PlanError>>()?;
                Ok(self.physical.add(PhysicalNode::Aggregate {
                    input: lowered,
                    return_exprs: return_exprs.clone(),
                    aggregates,
                    node_objects: node_object_types(return_exprs, self.source)?,
                }))
            }
            LogicalNode::Expand {
                input,
                src,
                dst,
                edges,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let (mode, frontier_estimate, policy) =
                    self.expand_mode(*input, edges, src_type, *min_hops, *max_hops)?;
                let versions = edges
                    .members()
                    .iter()
                    .map(|member| {
                        (
                            member.edge_type.clone(),
                            self.source
                                .edge_dataset(&member.edge_type)
                                .map(|pin| pin.version),
                        )
                    })
                    .collect();
                Ok(self.physical.add(PhysicalNode::Expand {
                    input: lowered,
                    src: src.clone(),
                    dst: dst.clone(),
                    edges: edges.clone(),
                    src_type: src_type.clone(),
                    dst_type: dst_type.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    edge_binding: edge_binding.clone(),
                    mode,
                    frontier_estimate,
                    policy,
                    versions,
                }))
            }
            LogicalNode::AntiJoin {
                input,
                inner,
                outer_var,
                predicate,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let key_set = self.key_sets.get(&id);
                let strategy = match key_set {
                    Some(tree) => self.anti_join_strategy(*input, *inner, outer_var, tree),
                    None => AntiJoinStrategy::Correlated,
                };
                let inner_lowered = match (strategy, key_set) {
                    (AntiJoinStrategy::KeySet, Some(tree)) => {
                        if tree
                            .plan
                            .live()
                            .any(|(_, node)| matches!(node, LogicalNode::AntiJoin { .. }))
                        {
                            return Err(PlanError::Internal(format!(
                                "the key-set tree of block `${outer_var}` holds a correlated block, whose id would name another arena's tree"
                            )));
                        }
                        let logical = std::mem::replace(&mut self.logical, &tree.plan);
                        let lowered = self.lower(tree.root);
                        self.logical = logical;
                        lowered?
                    }
                    _ => {
                        let lowered = self.lower(*inner)?;
                        self.csr_cached = true;
                        lowered
                    }
                };
                Ok(self.physical.add(PhysicalNode::AntiJoin {
                    input: lowered,
                    inner: inner_lowered,
                    outer_var: outer_var.clone(),
                    predicate: predicate.clone(),
                    aggregate: crate::plan_block_aggregate(&predicate.left)?,
                    strategy,
                }))
            }
            LogicalNode::Union {
                left,
                right,
                bindings,
            } => {
                let left = self.lower(*left)?;
                let right = self.lower(*right)?;
                Ok(self.physical.add(PhysicalNode::Union {
                    left,
                    right,
                    bindings: bindings.clone(),
                }))
            }
            LogicalNode::OptionalJoin {
                input,
                inner,
                outer_var,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let inner = self.lower(*inner)?;
                self.csr_cached = true;
                Ok(self.physical.add(PhysicalNode::OptionalJoin {
                    input: lowered,
                    inner,
                    outer_var: outer_var.clone(),
                }))
            }
            LogicalNode::Correlate {
                input,
                inner,
                outer_var,
                read,
                tiebreak,
                ..
            } => {
                let lowered = input.map(|input| self.lower(input)).transpose()?;
                let inner = self.lower(*inner)?;
                self.tracer.record(&[Role::Projection], Rule::Lower, &[]);
                Ok(self.physical.add(PhysicalNode::Correlate {
                    input: lowered,
                    inner,
                    outer_var: outer_var.clone(),
                    read: read.clone(),
                    tiebreak: tiebreak.clone(),
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
                leads,
                recall,
                ..
            } => {
                let lowered = self.lower(*input)?;
                let fetch = k.and_then(|k| usize::try_from(k).ok());
                let access = RankedAccess {
                    score: rank_score(binding),
                    property: property.clone(),
                    query: query.clone(),
                    fetch,
                    // `recall: 1` searches every partition: no probe cap, so
                    // the scan is exhaustive. Any other bound is resolved by
                    // the session's effort settings.
                    nprobes: if *recall == Some(1.0) {
                        None
                    } else {
                        self.source.ann_nprobes()
                    },
                    scope: if *leads {
                        RankScope::Order
                    } else {
                        RankScope::Window
                    },
                    overfetch: fetch.map(OverfetchRung::ladder).unwrap_or_default(),
                    prefilter: None,
                    policy: Some(NearestPolicy::DEFAULT),
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
                if *leads {
                    self.ranking = Some(Ranking::Scores(vec![score]));
                }
                Ok(lowered)
            }
            LogicalNode::Fuse {
                arms,
                target,
                k,
                weights,
                ..
            } => {
                let mut lowered = Vec::with_capacity(arms.len());
                for arm in arms {
                    lowered.push(FuseArm {
                        input: self.lower(arm.input)?,
                        rank: arm.rank,
                    });
                }
                Ok(self.physical.add(PhysicalNode::Fuse {
                    arms: lowered,
                    target: target.clone(),
                    k: k.clone(),
                    weights: weights.clone(),
                }))
            }
            LogicalNode::Ordered { input, keys } => {
                let lowered = self.lower(*input)?;
                let declared = derived_order(&self.physical, lowered).unwrap_or_default();
                let wanted: Vec<OrderKey> = keys.iter().cloned().map(OrderKey::Column).collect();
                if declared.starts_with(&wanted) {
                    Ok(lowered)
                } else {
                    Err(PlanError::Internal(format!(
                        "no operator sorts by {keys:?}; the input declares {declared:?}"
                    )))
                }
            }
            LogicalNode::Score {
                input,
                binding,
                type_name,
                property,
                kind,
                query,
                ..
            } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::Score {
                    input: lowered,
                    binding: binding.clone(),
                    type_name: type_name.clone(),
                    property: property.clone(),
                    kind: *kind,
                    query: query.clone(),
                }))
            }
            LogicalNode::Let {
                input, name, value, ..
            } => {
                let lowered = self.lower(*input)?;
                self.tracer.record(&[Role::Filter], Rule::Lower, &[]);
                Ok(self.physical.add(PhysicalNode::Let {
                    input: lowered,
                    name: name.clone(),
                    value: value.clone(),
                }))
            }
            LogicalNode::TargetCut {
                input,
                binding,
                targets,
            } => {
                let lowered = self.lower(*input)?;
                self.tracer.record(&[Role::Limit], Rule::Lower, &[]);
                Ok(self.physical.add(PhysicalNode::TargetCut {
                    input: lowered,
                    binding: binding.clone(),
                    targets: usize::try_from(*targets).unwrap_or(usize::MAX),
                }))
            }
            LogicalNode::Limit { input, rows } => {
                self.limit = Some(*rows);
                let lowered = self.lower(*input)?;
                self.tracer.record(&[Role::Limit], Rule::Lower, &[]);
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
                    origin: "fixed rule".to_string(),
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
            LogicalNode::CsvSource { spec } => {
                let max_workers = self.bounds.load_workers_max.max(1);
                let partition_bytes = self.bounds.source_partition_bytes.max(1);
                let partitions_by_size = spec.source_bytes.div_ceil(partition_bytes).max(1);
                let max_partitions =
                    u32::try_from(partitions_by_size.min(u64::from(max_workers))).unwrap_or(1);
                Ok(self.physical.add(PhysicalNode::CsvSource {
                    spec: spec.clone(),
                    max_partitions,
                    max_workers,
                }))
            }
            LogicalNode::DeltaSource { spec } => Ok(self
                .physical
                .add(PhysicalNode::DeltaSource { spec: spec.clone() })),
            LogicalNode::Validate {
                input,
                table,
                checks,
                replaced,
            } => {
                let estimated_rows = estimate_rows(self.logical, *input, self.source);
                let lowered = self.lower(*input)?;
                let overwrite = *replaced;
                let target_rows = table
                    .strip_prefix("node:")
                    .and_then(|name| self.source.node_type(name).ok())
                    .and_then(|node| node.row_count);
                let empty_target = overwrite && target_rows == Some(0);
                let mut checks: Vec<ValidateSpec> = checks
                    .iter()
                    .filter(|spec| {
                        !(empty_target
                            && matches!(
                                spec.check,
                                omnigraph_compiler::ir::ValidateCheck::RetainedEdges { .. }
                            ))
                    })
                    .map(|spec| ValidateSpec {
                        access: Some(self.validate_access(spec, estimated_rows, overwrite)),
                        ..spec.clone()
                    })
                    .collect();
                let probe = self.endpoint_probe(table, &checks);
                for spec in &mut checks {
                    if matches!(
                        spec.check,
                        omnigraph_compiler::ir::ValidateCheck::EdgeRi { .. }
                    ) {
                        spec.probe = probe;
                    }
                }
                let retained_edge_checks = checks
                    .iter()
                    .filter(|spec| {
                        matches!(
                            spec.check,
                            omnigraph_compiler::ir::ValidateCheck::RetainedEdges { .. }
                        )
                    })
                    .count();
                let removal = if overwrite && retained_edge_checks > 0 {
                    ValidateRemoval::Scan
                } else {
                    ValidateRemoval::None
                };
                self.decisions.push(StatisticSource {
                    statistic: "validate_removal".to_string(),
                    value: removal.as_str().to_string(),
                    origin: serde_json::json!({
                        "table": table,
                        "overwrite": overwrite,
                        "target_rows": target_rows,
                        "retained_edge_checks": retained_edge_checks,
                    })
                    .to_string(),
                });
                Ok(self.physical.add(PhysicalNode::Validate {
                    input: lowered,
                    table: table.clone(),
                    checks,
                    removal,
                    max_workers: self.bounds.load_workers_max.max(1),
                }))
            }
            LogicalNode::FragmentWrite { input, spec } => {
                let lowered = self.lower(*input)?;
                Ok(self.physical.add(PhysicalNode::FragmentWrite {
                    input: lowered,
                    spec: spec.clone(),
                    max_input_bytes: self.bounds.load_write_max_bytes,
                    max_workers: self.bounds.load_workers_max.max(1),
                }))
            }
        }
    }

    /// Where the `edge_ri` checks probe: `Swept` when every one reads its
    /// set whole over a one-integer `@key` node type, `Judged` otherwise,
    /// `None` when the load plans no endpoint check.
    fn endpoint_probe(&mut self, table: &str, checks: &[ValidateSpec]) -> Option<EndpointProbe> {
        let endpoints: Vec<(&str, Option<ValidateAccess>)> = checks
            .iter()
            .filter_map(|spec| match &spec.check {
                omnigraph_compiler::ir::ValidateCheck::EdgeRi { node_type, .. } => {
                    Some((node_type.as_str(), spec.access))
                }
                _ => None,
            })
            .collect();
        if endpoints.is_empty() {
            return None;
        }
        let integer_key = |node_type: &str| {
            self.source.node_type(node_type).ok().is_some_and(|node| {
                let [key] = node.key.as_slice() else {
                    return false;
                };
                node.schema.field_with_name(key).is_ok_and(|field| {
                    matches!(
                        field.data_type(),
                        DataType::Int32 | DataType::Int64 | DataType::UInt32
                    )
                })
            })
        };
        let swept = endpoints.iter().all(|(node_type, access)| {
            *access == Some(ValidateAccess::Scan) && integer_key(node_type)
        });
        let probe = if swept {
            EndpointProbe::Swept
        } else {
            EndpointProbe::Judged
        };
        self.decisions.push(StatisticSource {
            statistic: "endpoint_probe".to_string(),
            value: probe.as_str().to_string(),
            origin: serde_json::json!({
                "table": table,
                "endpoints": endpoints
                    .iter()
                    .map(|(node_type, access)| serde_json::json!({
                        "node_type": node_type,
                        "access": access.map(ValidateAccess::as_str),
                        "integer_key": integer_key(node_type),
                    }))
                    .collect::<Vec<_>>(),
            })
            .to_string(),
        });
        Some(probe)
    }

    /// Pass 9 costs the committed lookup from the accepted node count and
    /// index metadata when an overwrite replaces the table; append and merge
    /// keep it, so their lookups probe only under full index coverage.
    fn validate_access(
        &mut self,
        spec: &ValidateSpec,
        estimated_rows: Option<u64>,
        overwrite: bool,
    ) -> ValidateAccess {
        let Some(image) = &spec.consults else {
            return ValidateAccess::Delta;
        };
        let node = image
            .table
            .strip_prefix("node:")
            .and_then(|name| self.source.node_type(name).ok());
        let columns: Vec<&str> = match &spec.check {
            omnigraph_compiler::ir::ValidateCheck::Unique { columns, .. } => {
                columns.iter().map(String::as_str).collect()
            }
            omnigraph_compiler::ir::ValidateCheck::EdgeRi { .. } => node
                .as_ref()
                .map(|node| node.columns.id)
                .into_iter()
                .collect(),
            _ => Vec::new(),
        };
        let facts = self.source.index_facts(&image.table);
        let indexed = !columns.is_empty()
            && columns
                .iter()
                .all(|column| crate::source::btree_covers_column(&facts, column));
        let index_coverage = if indexed {
            ValidateIndexCoverage::Full
        } else if facts.iter().any(|fact| {
            columns.contains(&fact.column.as_str())
                && matches!(fact.kind, crate::source::IndexKind::Btree { usable: true })
                && fact.may_narrow()
        }) {
            ValidateIndexCoverage::Partial
        } else {
            ValidateIndexCoverage::Absent
        };
        let inputs = ValidateCostInputs {
            input_rows: estimated_rows,
            consulted_rows: node.as_ref().and_then(|node| node.row_count),
            index_coverage,
        };
        let access = match &spec.check {
            // An overwrite replaces the table, so each source of its prior
            // image keeps only its delta edges: under a minimum the check
            // scans those sources for one the delta leaves without an edge.
            omnigraph_compiler::ir::ValidateCheck::Cardinality { min, .. }
                if overwrite && *min > 0 =>
            {
                ValidateAccess::Scan
            }
            _ if overwrite => choose_validate_access(&spec.check, inputs),
            _ => coverage_validate_access(&spec.check, index_coverage),
        };
        let costs = inputs.costs(&spec.check);
        self.access_path = true;
        self.decisions.push(StatisticSource {
            statistic: format!("validate_access({})", spec.check.name()),
            value: access.as_str().to_string(),
            origin: serde_json::json!({
                "check": spec.check,
                "consulted_table": image.table,
                "consulted_version": image.version,
                "estimated_rows": estimated_rows,
                "consulted_rows": inputs.consulted_rows,
                "index_full_coverage": indexed,
                "index_coverage": index_coverage,
                "index_facts": facts,
                "indexed_row_factor": HASH_JOIN_RATIO,
                "probe_calls": costs.map(|(_, _, calls)| calls),
                "probe_cost": costs.map(|(probe, _, _)| probe),
                "scan_cost": costs.map(|(_, scan, _)| scan),
                "overwrite": overwrite,
            })
            .to_string(),
        });
        access
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
        edges: &EdgeSelection,
        src_type: &str,
        min_hops: u32,
        max_hops: Option<u32>,
    ) -> Result<(ExpandMode, Option<u64>, ExpandPolicy), PlanError> {
        if self.logical.traversal_work_limit().is_some() {
            return Ok((
                ExpandMode::IndexedScan,
                estimate_rows(self.logical, input, self.source),
                ExpandPolicy::Budgeted,
            ));
        }
        let member = edges.named().ok_or_else(|| {
            PlanError::refused(
                P002,
                "edge selections require a finite traversal_work_limit",
                Some(SET_TRAVERSAL_WORK_LIMIT),
            )
        })?;
        let edge_type = &member.edge_type;
        let direction = member.direction;
        let forced = match self.source.traversal() {
            Traversal::Indexed => Some(ExpandMode::IndexedScan),
            Traversal::Csr => Some(ExpandMode::Csr),
            Traversal::Auto => None,
        };
        let input_rows = estimate_rows(self.logical, input, self.source);
        let columns = self.source.node_type(src_type)?.columns;
        let facts = self.source.index_facts(&format!("edge:{edge_type}"));
        let covered = |column| crate::source::btree_covers_column(&facts, column);
        let full_coverage = match direction {
            Direction::Out => covered(columns.src),
            Direction::In => covered(columns.dst),
            Direction::Both => covered(columns.src) && covered(columns.dst),
        };
        let cost = self
            .source
            .expand_statistics(edge_type, direction)
            .map(|statistics| ExpandCostInputs {
                frontier_rows: input_rows.unwrap_or(u64::MAX),
                edge_count: statistics.edge_count,
                src_node_count: statistics.src_node_count,
                effective_max_hops: executed_hops(min_hops, max_hops),
                max_hops_cap: statistics.max_hops_cap,
                max_frontier_cap: statistics.max_frontier_cap,
                coverage: if full_coverage {
                    IndexCoverage::Indexed
                } else {
                    IndexCoverage::Degraded
                },
                coverage_provenance: CoverageProvenance::PinnedIndexFacts,
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
        Ok((mode, frontier_estimate, policy))
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
            origin: "projected widths and storage statistics".to_string(),
        });
        Ok(access)
    }

    /// Pass 10, the strategy of a correlated block with a key-set tree: the
    /// cost model over the rows each inner tree produces, the correlated one
    /// from the outer input's row-count estimate (`chain_rows`); `Correlated`
    /// when an estimate is unknown.
    fn anti_join_strategy(
        &mut self,
        input: LogicalId,
        inner: LogicalId,
        outer_var: &str,
        tree: &KeySetTree,
    ) -> AntiJoinStrategy {
        let outer_rows = estimate_rows(self.logical, input, self.source);
        let (Some(correlated), Some(key_set)) = (
            chain_rows(self.logical, inner, self.source, outer_rows),
            chain_rows(&tree.plan, tree.root, self.source, None),
        ) else {
            return AntiJoinStrategy::Correlated;
        };
        self.anti_join_strategy = true;
        self.decisions.push(StatisticSource {
            statistic: format!("anti_join_rows(${outer_var})"),
            value: format!("correlated {correlated} estimated, key_set {key_set} estimated"),
            origin: "row-count estimates of the block's two inner trees".to_string(),
        });
        choose_anti_join_strategy(correlated, key_set)
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
                        origin: "manifest".to_string(),
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
            origin: "manifest".to_string(),
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

/// The order a node's output leaves in, from the operator's definition and
/// its inputs' orders (`input`): each kind's `order:` token in the
/// `physical_nodes!` declaration ([`PhysicalNode::order`]).
pub fn node_order(
    node: &PhysicalNode,
    input: impl Fn(NodeId) -> Option<Vec<OrderKey>>,
) -> Option<Vec<OrderKey>> {
    node.order(input)
}

/// [`node_order`] of `id`, recomputed from the operators of its subtree and
/// never read from declared properties.
pub fn derived_order(plan: &PhysicalPlan, id: NodeId) -> Option<Vec<OrderKey>> {
    node_order(plan.node(id)?, |input| derived_order(plan, input))
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
        let ordering = node_order(&node, |input| {
            plan.properties(input)
                .and_then(|properties| properties.ordering.clone())
        });
        let properties = match &node {
            PhysicalNode::MetadataCount { return_exprs, .. } => Properties {
                schema: metadata_count_schema(return_exprs)?,
                ordering: ordering.clone(),
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
                    ordering: ordering.clone(),
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
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
                    origin: "engine constant".to_string(),
                });
                Properties {
                    schema: key_schema(spec),
                    ordering: ordering.clone(),
                    rows,
                    work_bytes,
                    retained_limit: None,
                    sources,
                }
            }
            PhysicalNode::Scan {
                source: ScanInput::Table,
                spec,
                keys_only: false,
                ranked,
                ..
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
                    ordering: ordering.clone(),
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
                                origin: "manifest rows times key width".to_string(),
                            }],
                        )
                    }
                    (true, Estimate::Unknown) => (
                        None,
                        vec![StatisticSource {
                            statistic: "build_side_key_bytes".to_string(),
                            value: "unknown".to_string(),
                            origin: "manifest holds no row count for a build fragment".to_string(),
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
                    ordering: ordering.clone(),
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
                    ordering: ordering.clone(),
                    rows: input_props.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: Some(bounds.hydration_chunk_hard_bytes),
                    sources: vec![StatisticSource {
                        statistic: "retained_limit".to_string(),
                        value: bounds.hydration_chunk_hard_bytes.to_string(),
                        origin: "HYDRATION_CHUNK_HARD_BYTES".to_string(),
                    }],
                }
            }
            PhysicalNode::HydrateColumns { input, .. } => {
                let input = props(plan, *input)?;
                let retained = hydrate_chunk_bytes(bounds.query_memory_pool_bytes);
                Properties {
                    schema: input.schema.clone(),
                    ordering: input.ordering.clone(),
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: Some(retained),
                    sources: vec![StatisticSource {
                        statistic: "retained_limit".to_string(),
                        value: retained.to_string(),
                        origin: "query memory pool / 8".to_string(),
                    }],
                }
            }
            PhysicalNode::RowCompare { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: diff_schema(&input.schema),
                    ordering: ordering.clone(),
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
                    ordering: ordering.clone(),
                    rows: input.rows,
                    work_bytes: input.work_bytes,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::TargetCut { input, .. }
            | PhysicalNode::Score { input, .. }
            | PhysicalNode::Let { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: ordering.clone(),
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
                    ordering: ordering.clone(),
                    rows: bounded,
                    work_bytes: input.work_bytes,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::CrossJoin {
                left,
                right,
                filters,
            } => {
                let left_props = props(plan, *left)?;
                let right_props = props(plan, *right)?;
                let rows = match (left_props.rows, right_props.rows) {
                    (Estimate::Known(left), Estimate::Known(right)) if filters.is_empty() => {
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
                    ordering: ordering.clone(),
                    rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::ContainsJoin { left, right, .. } => {
                let left_props = props(plan, *left)?;
                let right_props = props(plan, *right)?;
                Properties {
                    schema: join_schema(
                        &left_props.schema,
                        physical_prefix(plan, *left),
                        &right_props.schema,
                        physical_prefix(plan, *right),
                    ),
                    ordering: ordering.clone(),
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::OuterReference { .. } => Properties {
                schema: Arc::new(Schema::empty()),
                ordering: ordering.clone(),
                rows: Estimate::Unknown,
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: Vec::new(),
            },
            PhysicalNode::Sort { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: ordering.clone(),
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Correlate { input, inner, .. } => {
                let input = props(plan, input.unwrap_or(*inner))?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: ordering.clone(),
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Filter { input, .. }
            | PhysicalNode::Expand { input, .. }
            | PhysicalNode::AntiJoin { input, .. }
            | PhysicalNode::OptionalJoin { input, .. }
            | PhysicalNode::Union { left: input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: ordering.clone(),
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Aggregate {
                return_exprs,
                node_objects,
                ..
            } => Properties {
                schema: return_schema(return_exprs, node_objects)?,
                ordering: None,
                rows: Estimate::Unknown,
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: Vec::new(),
            },
            PhysicalNode::Projection {
                input,
                return_exprs,
                node_objects,
            } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: return_schema(return_exprs, node_objects)?,
                    ordering: ordering.clone(),
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::Fuse { arms, .. } => {
                let input = props(plan, arms[0].input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: None,
                    rows: Estimate::Unknown,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::CsvSource { spec, .. } => Properties {
                schema: source.schema(SideId::Child)?,
                ordering: None,
                rows: Estimate::Known(spec.estimated_rows),
                work_bytes: Estimate::Known(spec.source_bytes),
                retained_limit: None,
                sources: vec![StatisticSource {
                    statistic: "estimated_rows".to_string(),
                    value: spec.estimated_rows.to_string(),
                    origin: format!(
                        "source_bytes {} over the narrowest row, an upper bound",
                        spec.source_bytes
                    ),
                }],
            },
            PhysicalNode::DeltaSource { spec } => Properties {
                schema: source.schema(SideId::Child)?,
                ordering: None,
                rows: Estimate::Known(spec.rows),
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: vec![StatisticSource {
                    statistic: "estimated_rows".to_string(),
                    value: spec.rows.to_string(),
                    origin: format!(
                        "the staged {} delta of {}, exact{}",
                        spec.delta.as_str(),
                        spec.table,
                        if spec.deletes { ", with deletes" } else { "" }
                    ),
                }],
            },
            PhysicalNode::Validate { input, .. } => {
                let input = props(plan, *input)?;
                Properties {
                    schema: input.schema.clone(),
                    ordering: None,
                    rows: input.rows,
                    work_bytes: Estimate::Unknown,
                    retained_limit: None,
                    sources: Vec::new(),
                }
            }
            PhysicalNode::FragmentWrite { .. } => Properties {
                schema: write_count_schema(),
                ordering: None,
                rows: Estimate::Known(1),
                work_bytes: Estimate::Unknown,
                retained_limit: None,
                sources: Vec::new(),
            },
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
        origin: "manifest row count, at most one row under a key equality".to_string(),
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
        origin: "manifest".to_string(),
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
            table: TableRef::dataset("node:Doc", "tables/node_Doc", None),
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
                version_window: Some((7, 8)),
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
        assert_eq!(
            root.ordering.as_deref(),
            Some(&[OrderKey::Column("id".to_string())][..])
        );
        let pipelines = optimized.physical.pipelines_json();
        assert_eq!(pipelines.as_array().map(Vec::len), Some(2));
        assert_eq!(pipelines[0]["id"], 0);
        assert_eq!(pipelines[0]["sink"]["name"], "SortMergeJoin(build)");
        assert_eq!(pipelines[1]["id"], 1);
        assert_eq!(pipelines[1]["sink"]["name"], "consumer");
        assert_eq!(pipelines[1]["depends_on"], serde_json::json!([0]));
        let operators: Vec<&str> = pipelines[1]["operators"]
            .as_array()
            .unwrap()
            .iter()
            .map(|operator| operator["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            operators,
            [
                "SortMergeJoin(probe)",
                "HydrateByAddress(child)",
                "HydrateByAddress(parent)",
                "RowCompare",
                "Page"
            ]
        );
        assert_eq!(
            pipelines[0]["sink"]["id"], pipelines[1]["operators"][0]["id"],
            "the build sink and the probe operator are the same join node"
        );
        assert_eq!(pipelines[1]["source"]["node"], "Scan");
        let scan_id = pipelines[1]["source"]["id"].as_u64().unwrap() as usize;
        assert!(matches!(
            optimized.physical.node(scan_id),
            Some(PhysicalNode::Scan { .. })
        ));
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
            assert_eq!(
                root.ordering.as_deref(),
                Some(&[OrderKey::Column("id".to_string())][..])
            );
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

    /// FNV-1a over the plan version, kind names and input counts: the literal
    /// holds on every toolchain, and a node's input count is part of the
    /// shape. The literal moves with `LOGICAL_PLAN_VERSION`.
    #[test]
    fn structural_hash_is_a_fixed_function_of_the_shape() {
        let source = source(false);
        let plan = resolve(&commit_diff(None), &source).expect("resolves");
        assert_eq!(
            format!("{:016x}", plan.structural_hash()),
            "8b194d960cfb490d"
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

#[cfg(test)]
mod cast_reads_tests {
    use super::*;
    use omnigraph_compiler::{ExprType, PropType, ScalarType};

    #[test]
    fn a_cast_keeps_its_property_dependency() {
        let expr = IRExpr::Cast {
            expr: Box::new(IRExpr::PropAccess {
                variable: "p".into(),
                property: "age".into(),
                ty: ExprType::from_prop(&PropType::scalar(ScalarType::I64, true)),
            }),
            ty: ExprType::from_prop(&PropType::scalar(ScalarType::F64, true)),
        };
        let mut reads = Vec::new();
        reads_of_expr(&expr, &mut reads);
        assert_eq!(reads, vec![ColumnRef::property("p", "age")]);
    }
}

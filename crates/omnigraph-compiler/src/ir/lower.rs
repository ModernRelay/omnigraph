use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SYSTEM_COLUMNS_META, SystemColumns};
use crate::error::{CompilerError, Result};
use crate::query::ast::*;
use crate::query::typecheck::{
    BoundVariable, CheckedBinding, CheckedBlock, CheckedBlockAggregate, CheckedClause,
    CheckedMutationOp, CheckedMutationQuery, CheckedParam, CheckedReadQuery, CheckedScope,
    MutationTarget, ResolvedTraversal, ResolvedType, TypedExpr,
};
use crate::traversal::{EDGE_TYPE_COLUMN, EDGE_TYPE_META};
use crate::types::{PropType, ScalarType};

use super::*;

/// Fresh synthetic variable names for one query, shared across negation
/// inners (their pipelines run over the outer batch, so a name must be
/// unique query-wide). The executor names every column `<var>.<prop>` and
/// never reconciles two producers of one variable (#605), so each anonymous
/// `_` endpoint and each cycle-closing temp gets its own name.
#[derive(Default)]
struct FreshNames {
    anon: usize,
    temp: usize,
}

impl FreshNames {
    fn anon(&mut self) -> String {
        self.anon += 1;
        format!("__anon_{}", self.anon)
    }

    fn temp(&mut self, dst: &str) -> String {
        self.temp += 1;
        format!("__temp_{}_{}", dst, self.temp)
    }
}

/// What every expression site shares: the physical column spellings and
/// the bindings a variable names. Every emitted expression lowers through
/// one, so `contains` resolution and `fold` apply everywhere.
struct LowerCtx<'a> {
    catalog: &'a Catalog,
    bindings: Bindings<'a>,
}

/// The binding a variable names.
enum Bindings<'a> {
    /// Bindings visible in this lexical read scope.
    Read(&'a HashMap<String, BoundVariable>),
    /// A mutation: the target type, whose bare name stands where a read has
    /// a binding variable (`Expr::mutation_property`).
    Mutation(BoundVariable),
}

impl LowerCtx<'_> {
    fn system_columns(&self) -> SystemColumns {
        self.catalog.system_columns
    }

    fn binding(&self, variable: &str) -> Option<&BoundVariable> {
        match &self.bindings {
            Bindings::Read(bindings) => bindings.get(variable),
            Bindings::Mutation(target) => match target {
                BoundVariable::Node { type_name } => (variable == type_name).then_some(target),
                BoundVariable::Edge { type_names } => type_names
                    .iter()
                    .any(|name| variable == name)
                    .then_some(target),
            },
        }
    }

    /// The physical column a property leaf reads: `@id` through
    /// `physical_property` as a read does, a mutation target edge's `from`
    /// and `to` to the system endpoint columns.
    fn physical_column(&self, variable: &str, property: &str) -> String {
        let system_columns = self.system_columns();
        if let Some(BoundVariable::Edge { .. }) = self.binding(variable)
            && matches!(self.bindings, Bindings::Mutation(_))
        {
            match property {
                "from" => return system_columns.src.to_string(),
                "to" => return system_columns.dst.to_string(),
                _ => {}
            }
        }
        physical_property(property, system_columns)
    }

    /// `left <op> right`: `contains` over a scalar String left operand becomes
    /// `StringContains`, so execution dispatches on the IR op alone and never
    /// re-derives operand types; a literal-only node folds.
    fn binary(&self, left: IRExpr, op: BinaryOp, right: IRExpr) -> Result<IRExpr> {
        coerce::binary(left, op, right)
    }
}

/// Lower a checked read to typed IR. Every type comes from the checked
/// tree; lowering adds physical columns, traversal scheduling, fresh names,
/// score columns, comparison casts and folding, and validates what it emitted.
///
/// A mutation cannot reach this door: [`CheckedReadQuery`] is only ever the
/// read half of [`crate::query::typecheck::CheckedQuery`].
///
/// ```compile_fail
/// let checked: omnigraph_compiler::query::typecheck::CheckedMutationQuery = unimplemented!();
/// let catalog: omnigraph_compiler::catalog::Catalog = unimplemented!();
/// let _ = omnigraph_compiler::lower_query(&catalog, &checked);
/// ```
pub fn lower_query(catalog: &Catalog, query: &CheckedReadQuery) -> Result<QueryIR> {
    let mut pipeline = Vec::new();
    let mut bound_vars = HashSet::new();
    let mut fresh = FreshNames::default();

    lower_clauses(
        catalog,
        &query.scope,
        &mut pipeline,
        &mut bound_vars,
        &HashSet::new(),
        &mut fresh,
    )?;

    let ctx = LowerCtx {
        catalog,
        bindings: Bindings::Read(&query.scope.bindings),
    };
    let return_exprs: Vec<IRProjection> = query
        .returns
        .iter()
        .map(|item| {
            Ok(IRProjection {
                expr: lower_projection(&item.expr, &ctx)?,
                alias: item
                    .alias
                    .clone()
                    .or_else(|| item.expr.meta_field_result_key()),
                column: item.executed_column.clone(),
                ty: item.expr.expr_type(),
            })
        })
        .collect::<Result<_>>()?;

    let has_aggregates = query
        .returns
        .iter()
        .any(|item| matches!(&item.expr, TypedExpr::Aggregate { .. }));
    let aggregate_meta_columns: HashSet<String> = query
        .returns
        .iter()
        .filter(|item| has_aggregates && item.alias.is_none())
        .filter_map(|item| item.expr.meta_field_result_key())
        .collect();

    let order_by: Vec<IROrdering> = query
        .orders
        .iter()
        .enumerate()
        .map(|(index, order)| {
            let expr = order
                .expr
                .meta_field_result_key()
                .filter(|_| matches!(&order.expr, TypedExpr::PropAccess { .. }))
                .filter(|name| aggregate_meta_columns.contains(name))
                .map(|name| {
                    let ty = return_exprs
                        .iter()
                        .find(|projection| projection.column == name)
                        .expect("aggregate meta-column has a projection")
                        .ty
                        .clone();
                    IRExpr::AliasRef(name, ty)
                })
                .map_or_else(
                    || {
                        if index == 0
                            && matches!(
                                &order.expr,
                                TypedExpr::Nearest { .. }
                                    | TypedExpr::Bm25 { .. }
                                    | TypedExpr::Rrf { .. }
                            )
                        {
                            lower_expr(&order.expr, &ctx)
                        } else {
                            lower_projection(&order.expr, &ctx)
                        }
                    },
                    Ok,
                )?;
            Ok(IROrdering {
                expr,
                descending: order.descending,
            })
        })
        .collect::<Result<_>>()?;

    let ir = QueryIR {
        name: query.name.clone(),
        params: lower_params(&query.params),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
    };
    super::validate::validate_query(&ir)?;
    Ok(ir)
}

/// Lower a checked mutation to typed IR: each checked statement with its
/// target namespace, its typed assignments and its typed predicate.
///
/// Only the checker constructs a [`CheckedMutationQuery`]; its fields are
/// private, so no caller can hand this door a statement list and a target
/// list that disagree.
///
/// ```compile_fail
/// let checked = omnigraph_compiler::query::typecheck::CheckedMutationQuery {
///     name: String::new(),
///     params: Vec::new(),
///     ops: Vec::new(),
/// };
/// ```
pub fn lower_mutation_query(catalog: &Catalog, query: &CheckedMutationQuery) -> Result<MutationIR> {
    let ops = query
        .ops
        .iter()
        .map(|op| lower_single_mutation(catalog, op))
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationIR {
        name: query.name.clone(),
        params: lower_params(&query.params),
        ops,
        as_commit: query.as_commit,
    })
}

fn lower_params(params: &[CheckedParam]) -> Vec<IRParam> {
    params
        .iter()
        .map(|param| IRParam {
            declaration: param.declaration.clone(),
            ty: ExprType::from_prop(&param.ty),
        })
        .collect()
}

/// Lower one checked mutation statement in its target namespace.
fn lower_single_mutation(catalog: &Catalog, op: &CheckedMutationOp) -> Result<MutationOpIR> {
    let binding = match op.target() {
        MutationTarget::Node { type_name } => BoundVariable::Node {
            type_name: type_name.clone(),
        },
        MutationTarget::Edge { type_name } => BoundVariable::Edge {
            type_names: vec![type_name.clone()],
        },
    };
    let ctx = LowerCtx {
        catalog,
        bindings: Bindings::Mutation(binding),
    };
    let no_bindings = HashMap::new();
    let assignment_ctx = LowerCtx {
        catalog,
        bindings: Bindings::Read(&no_bindings),
    };
    let lower_assignments = |assignments: &[crate::query::typecheck::CheckedAssignment]| {
        assignments
            .iter()
            .map(|assignment| {
                Ok(IRAssignment {
                    property: assignment.property.clone(),
                    value: lower_expr(&assignment.value, &assignment_ctx)?,
                })
            })
            .collect::<Result<_>>()
    };
    match op {
        CheckedMutationOp::Insert {
            target,
            assignments,
        } => Ok(MutationOpIR::Insert {
            target: target.clone(),
            assignments: lower_assignments(assignments)?,
        }),
        CheckedMutationOp::Update {
            target,
            assignments,
            predicate,
        } => Ok(MutationOpIR::Update {
            target: target.clone(),
            assignments: lower_assignments(assignments)?,
            predicate: lower_expr(predicate, &ctx)?,
        }),
        CheckedMutationOp::Delete { target, predicate } => Ok(MutationOpIR::Delete {
            target: target.clone(),
            predicate: lower_expr(predicate, &ctx)?,
        }),
    }
}

fn lower_clauses(
    catalog: &Catalog,
    scope: &CheckedScope,
    pipeline: &mut Vec<IROp>,
    bound_vars: &mut HashSet<String>,
    outer_physical_names: &HashSet<String>,
    fresh: &mut FreshNames,
) -> Result<()> {
    let mut bindings: Vec<&CheckedBinding> = Vec::new();
    let mut traversals: Vec<&ResolvedTraversal> = Vec::new();
    let mut filters: Vec<&TypedExpr> = Vec::new();
    let mut blocks: Vec<&CheckedBlock> = Vec::new();

    for clause in &scope.clauses {
        match clause {
            CheckedClause::Binding(binding) => bindings.push(binding),
            CheckedClause::Traversal(traversal) => traversals.push(traversal),
            CheckedClause::Filter(filter) => filters.push(filter),
            CheckedClause::Block(block) => blocks.push(block),
        }
    }
    let ctx = LowerCtx {
        catalog,
        bindings: Bindings::Read(&scope.bindings),
    };

    // ── Determine which bindings are "deferred" ─────────────────────────
    //
    // When multiple bindings in the same match clause are connected by
    // traversals, only the first-declared binding needs a NodeScan; the
    // rest will be introduced by Expand operations.  Making them all
    // NodeScans triggers expensive cross-joins followed by cycle-closing
    // filters.
    //
    // Algorithm: build an undirected graph of variables connected by
    // traversals, then walk connected components in binding declaration
    // order.  The first binding in each component becomes the root (gets
    // a NodeScan); all other bindings in the same component are deferred
    // — their inline filters become post-Expand Filter ops.

    let binding_set: HashSet<&str> = bindings.iter().map(|b| b.variable.as_str()).collect();

    // Build undirected traversal adjacency (variable → neighbours).
    // Exclude the anonymous wildcard "_" so it cannot falsely bridge
    // otherwise-independent components.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for t in &traversals {
        let src = t.src.as_str();
        let dst = t.dst.as_str();
        if src != "_" && dst != "_" {
            adj.entry(src).or_default().push(dst);
            adj.entry(dst).or_default().push(src);
        }
    }

    // Walk components to find deferred binding variables
    let mut deferred_set: HashSet<String> = HashSet::new();
    let mut component_visited: HashSet<&str> = HashSet::new();
    let searched: HashSet<&str> = filters
        .iter()
        .filter_map(|f| f.text_search_subject())
        .collect();

    for binding in &bindings {
        if component_visited.contains(binding.variable.as_str()) {
            continue;
        }
        // BFS from this binding through the traversal graph
        let mut queue = VecDeque::new();
        queue.push_back(binding.variable.as_str());
        let mut component_bindings: Vec<&str> = Vec::new();
        let mut component_vars: Vec<&str> = Vec::new();

        while let Some(var) = queue.pop_front() {
            if !component_visited.insert(var) {
                continue;
            }
            component_vars.push(var);
            if binding_set.contains(var) {
                component_bindings.push(var);
            }
            if let Some(neighbours) = adj.get(var) {
                for &n in neighbours {
                    if !component_visited.contains(n) {
                        queue.push_back(n);
                    }
                }
            }
        }

        let reaches_outer = component_vars.iter().any(|var| bound_vars.contains(*var));
        let inner_bindings: Vec<&str> = component_bindings
            .iter()
            .copied()
            .filter(|var| !bound_vars.contains(*var))
            .collect();
        let root = scan_root(&inner_bindings, reaches_outer, &searched);
        for (index, var) in inner_bindings.into_iter().enumerate() {
            if Some(index) != root {
                deferred_set.insert(var.to_string());
            }
        }
    }

    // Build deferred filters map for variables introduced by traversals
    let mut deferred_filters: HashMap<String, Vec<IRExpr>> = HashMap::new();

    // A variable bound again after its first binding (`$p: Person` twice in
    // one match, or inside `not { }` over an outer `$p`): the typechecker
    // admits it as the same type, so it is a constraint on the existing
    // rows, not a second scan. Its inline filters become plain Filter ops.
    let mut rebind_filters: Vec<IRExpr> = Vec::new();

    // Lower bindings into NodeScan ops (skip deferred ones)
    for binding in &bindings {
        let binding_filters = build_binding_filters(binding, &ctx)?;

        // A variable the outer pattern already bound (a negation's inner
        // clauses run over the outer batch) is never deferred, whatever its
        // place in the component walk: deferred filters are emitted by the
        // Expand that introduces a variable, and nothing introduces this one
        // again, so they would be lost (#605).
        if bound_vars.contains(&binding.variable) {
            rebind_filters.extend(binding_filters);
            continue;
        }

        if deferred_set.contains(&binding.variable) {
            // Save filters for emission after the Expand that introduces
            // this variable.
            if !binding_filters.is_empty() {
                deferred_filters
                    .entry(binding.variable.clone())
                    .or_default()
                    .extend(binding_filters);
            }
            continue;
        }

        pipeline.push(IROp::NodeScan {
            variable: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            filters: binding_filters,
        });
        bound_vars.insert(binding.variable.clone());
    }

    // Lower traversals into Expand ops.
    //
    // Traversals are processed iteratively rather than in a single pass
    // because deferred bindings mean a traversal's source might not be
    // bound until a prior traversal introduces it.  Each pass processes
    // every traversal that has at least one bound endpoint; this repeats
    // until all traversals are consumed.
    let mut remaining: Vec<&ResolvedTraversal> = traversals.clone();
    while !remaining.is_empty() {
        let mut next_remaining = Vec::new();
        for traversal in &remaining {
            let src_bound = traversal.src != "_" && bound_vars.contains(&traversal.src);
            let dst_bound = traversal.dst != "_" && bound_vars.contains(&traversal.dst);
            if !src_bound && !dst_bound {
                next_remaining.push(*traversal);
                continue;
            }

            if src_bound && dst_bound {
                // Cycle closing: expand to a temp var, then filter temp.id = dst.id
                // (temp fresh per traversal, #605).
                let temp_var = fresh.temp(&traversal.dst);
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: temp_var.clone(),
                    edges: traversal.edges.clone(),
                    src_type: traversal.src_type.clone(),
                    dst_type: traversal.dst_type.clone(),
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: vec![],
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                pipeline.push(IROp::Filter(coerce::binary(
                    IRExpr::PropAccess {
                        variable: temp_var,
                        property: catalog.system_columns.id.to_string(),
                        ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
                    },
                    BinaryOp::Compare(CompOp::Eq),
                    IRExpr::PropAccess {
                        variable: traversal.dst.clone(),
                        property: catalog.system_columns.id.to_string(),
                        ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
                    },
                )?));
            } else if !src_bound && dst_bound {
                // Reverse expand: dst is bound, src is not.
                let introduced_filters =
                    deferred_filters.remove(&traversal.src).unwrap_or_default();
                let dst_var = if traversal.src == "_" {
                    fresh.anon()
                } else {
                    traversal.src.clone()
                };
                pipeline.push(IROp::Expand {
                    src_var: traversal.dst.clone(),
                    dst_var,
                    edges: traversal.edges.reversed(),
                    src_type: traversal.dst_type.clone(),
                    dst_type: traversal.src_type.clone(),
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                if traversal.src != "_" {
                    bound_vars.insert(traversal.src.clone());
                }
            } else {
                // Normal expand: src is bound, dst is not (an anonymous `_`
                // destination gets a fresh name per occurrence, #605).
                let introduced_filters =
                    deferred_filters.remove(&traversal.dst).unwrap_or_default();
                let dst_var = if traversal.dst == "_" {
                    fresh.anon()
                } else {
                    traversal.dst.clone()
                };
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var,
                    edges: traversal.edges.clone(),
                    src_type: traversal.src_type.clone(),
                    dst_type: traversal.dst_type.clone(),
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                if traversal.dst != "_" {
                    bound_vars.insert(traversal.dst.clone());
                }
            }
        }
        if next_remaining.len() == remaining.len() {
            return Err(CompilerError::Plan(
                "typechecked traversal has no executable endpoint binding".to_string(),
            ));
        }
        remaining = next_remaining;
    }

    // Re-binding filters run after every variable is introduced, like the
    // explicit filters below; the executor hoists the pushable ones onto the
    // introducing scan.
    pipeline.extend(rebind_filters.into_iter().map(IROp::Filter));

    // Lower explicit filters
    for filter in &filters {
        pipeline.push(IROp::Filter(lower_expr(filter, &ctx)?));
    }

    if blocks.is_empty() {
        return Ok(());
    }
    let mut physical_names = outer_physical_names.clone();
    for op in pipeline.iter() {
        match op {
            IROp::NodeScan {
                variable,
                type_name: _,
                filters: _,
            } => {
                physical_names.insert(variable.clone());
            }
            IROp::Expand {
                src_var: _,
                dst_var,
                edges: _,
                src_type: _,
                dst_type: _,
                min_hops: _,
                max_hops: _,
                dst_filters: _,
                edge_binding,
            } => {
                physical_names.insert(dst_var.clone());
                physical_names.extend(edge_binding.iter().cloned());
            }
            IROp::Filter(_)
            | IROp::AntiJoin {
                outer_var: _,
                inner: _,
                predicate: _,
            } => {}
        }
    }

    for block in blocks {
        let visible_outer: HashSet<_> = bound_vars
            .iter()
            .filter(|name| block.outer_bindings.contains_key(*name))
            .cloned()
            .collect();
        let outer_var = find_outer_var(&block.inner.clauses, &visible_outer);
        let mut collisions: Vec<_> = block
            .inner
            .bindings
            .keys()
            .filter(|name| {
                !block.outer_bindings.contains_key(*name) && physical_names.contains(*name)
            })
            .collect();
        collisions.sort();
        let renames: HashMap<_, _> = collisions
            .into_iter()
            .map(|name| (name.clone(), fresh.temp(name)))
            .collect();
        let mut inner_pipeline = Vec::new();
        let mut inner_bound = visible_outer;
        lower_clauses(
            catalog,
            &block.inner,
            &mut inner_pipeline,
            &mut inner_bound,
            &physical_names,
            fresh,
        )?;

        let inner_ctx = LowerCtx {
            catalog,
            bindings: Bindings::Read(&block.inner.bindings),
        };
        let outer_ctx = LowerCtx {
            catalog,
            bindings: Bindings::Read(&block.outer_bindings),
        };
        let mut left = match &block.aggregate {
            CheckedBlockAggregate::RowCount => BlockAggregateExpr::count_rows(),
            CheckedBlockAggregate::Aggregate {
                func,
                arg,
                signature,
            } => BlockAggregateExpr::Aggregate {
                func: *func,
                arg: Box::new(lower_expr(arg, &inner_ctx)?),
                signature: signature.clone(),
            },
        };
        rename_inner_bindings(&mut inner_pipeline, left.arg_mut(), &renames);
        let predicate = coerce::block(left, block.op, lower_expr(&block.right, &outer_ctx)?)?;

        pipeline.push(IROp::AntiJoin {
            outer_var: outer_var.unwrap_or_default(),
            inner: inner_pipeline,
            predicate,
        });
    }

    Ok(())
}

fn rename_inner_bindings<'a>(
    pipeline: &'a mut [IROp],
    argument: Option<&'a mut IRExpr>,
    renames: &HashMap<String, String>,
) {
    if renames.is_empty() {
        return;
    }
    let rename = |variable: &mut String| {
        if let Some(name) = renames.get(variable) {
            *variable = name.clone();
        }
    };
    let mut expressions: Vec<&mut IRExpr> = argument.into_iter().collect();
    let mut pending: Vec<_> = pipeline.iter_mut().collect();
    while let Some(op) = pending.pop() {
        match op {
            IROp::NodeScan {
                variable,
                type_name: _,
                filters,
            } => {
                rename(variable);
                expressions.extend(filters);
            }
            IROp::Expand {
                src_var,
                dst_var,
                edges: _,
                src_type: _,
                dst_type: _,
                min_hops: _,
                max_hops: _,
                dst_filters,
                edge_binding,
            } => {
                rename(src_var);
                rename(dst_var);
                if let Some(binding) = edge_binding {
                    rename(binding);
                }
                expressions.extend(dst_filters);
            }
            IROp::Filter(expr) => expressions.push(expr),
            IROp::AntiJoin {
                outer_var,
                inner,
                predicate,
            } => {
                rename(outer_var);
                expressions.extend(predicate.left.arg_mut());
                expressions.push(&mut predicate.right);
                pending.extend(inner);
            }
        }
    }
    while let Some(expr) = expressions.pop() {
        match expr {
            IRExpr::PropAccess {
                variable,
                property: _,
                ty: _,
            }
            | IRExpr::Variable(variable, _) => rename(variable),
            IRExpr::Nearest {
                variable,
                property: _,
                query,
                ty: _,
            } => {
                rename(variable);
                expressions.push(query);
            }
            IRExpr::Search {
                field,
                query,
                ty: _,
            }
            | IRExpr::MatchText {
                field,
                query,
                ty: _,
            }
            | IRExpr::Bm25 {
                field,
                query,
                ty: _,
            } => expressions.extend([field.as_mut(), query.as_mut()]),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits,
                ty: _,
            } => {
                expressions.extend([field.as_mut(), query.as_mut()]);
                expressions.extend(max_edits.as_deref_mut());
            }
            IRExpr::Rrf {
                primary,
                secondary,
                k,
                ty: _,
            } => {
                expressions.extend([primary.as_mut(), secondary.as_mut()]);
                expressions.extend(k.as_deref_mut());
            }
            IRExpr::Aggregate {
                func: _,
                arg,
                signature: _,
            }
            | IRExpr::Not(arg, _)
            | IRExpr::IsNull {
                expr: arg,
                negated: _,
                ty: _,
            }
            | IRExpr::Cast { expr: arg, ty: _ } => expressions.push(arg),
            IRExpr::Binary {
                left,
                op: _,
                right,
                ty: _,
            } => expressions.extend([left.as_mut(), right.as_mut()]),
            IRExpr::Literal(_, _) | IRExpr::Param(_, _) | IRExpr::AliasRef(_, _) => {}
        }
    }
}

/// Build IR filters from a binding's inline property matches: a list
/// property matches by membership, every other by equality.
fn build_binding_filters(binding: &CheckedBinding, ctx: &LowerCtx<'_>) -> Result<Vec<IRExpr>> {
    let mut filters = Vec::new();
    for pm in &binding.matches {
        let op = if pm.prop_type.list {
            CompOp::Contains
        } else {
            CompOp::Eq
        };
        filters.push(ctx.binary(
            IRExpr::PropAccess {
                variable: binding.variable.clone(),
                property: pm.property.clone(),
                ty: ExprType::from_prop(&pm.prop_type),
            },
            BinaryOp::Compare(op),
            lower_expr(&pm.value, ctx)?,
        )?);
    }
    Ok(filters)
}

/// The index of the component binding that keeps its `NodeScan`: the first one, or,
/// when the component reaches an outer-bound variable (#763), a searched binding
/// only, since every other binding is expanded from the outer row.
fn scan_root(
    inner_bindings: &[&str],
    reaches_outer: bool,
    searched: &HashSet<&str>,
) -> Option<usize> {
    if reaches_outer {
        inner_bindings.iter().position(|var| searched.contains(var))
    } else {
        inner_bindings.first().map(|_| 0)
    }
}

fn find_outer_var(clauses: &[CheckedClause], outer_bound: &HashSet<String>) -> Option<String> {
    for clause in clauses {
        match clause {
            CheckedClause::Traversal(t) => {
                if outer_bound.contains(&t.src) {
                    return Some(t.src.clone());
                }
                if outer_bound.contains(&t.dst) {
                    return Some(t.dst.clone());
                }
            }
            CheckedClause::Filter(f) => {
                if let Some(v) = outer_var_in_expr(f, outer_bound) {
                    return Some(v);
                }
            }
            CheckedClause::Binding(b) if outer_bound.contains(&b.variable) => {
                return Some(b.variable.clone());
            }
            CheckedClause::Binding(_) | CheckedClause::Block(_) => {}
        }
    }
    None
}

/// The first outer-bound variable an expression's leaves read, operand by
/// operand in written order.
fn outer_var_in_expr(expr: &TypedExpr, outer_bound: &HashSet<String>) -> Option<String> {
    match expr {
        TypedExpr::Binary { left, right, .. }
        | TypedExpr::In {
            needle: left,
            list: right,
            ..
        } => outer_var_in_expr(left, outer_bound).or_else(|| outer_var_in_expr(right, outer_bound)),
        TypedExpr::Not { inner, .. } | TypedExpr::IsNull { inner, .. } => {
            outer_var_in_expr(inner, outer_bound)
        }
        leaf => leaf
            .first_variable()
            .filter(|v| outer_bound.contains(*v))
            .map(str::to_string),
    }
}

/// A rank expression in a projection or secondary order key, at the root or
/// inside Boolean structure (`bm25($p.name, "x") > 0.0 as hit`), lowers to the score column the
/// retrieval appends (`TypedExpr::score_column`); T33 required `order` to execute it.
fn lower_projection(expr: &TypedExpr, ctx: &LowerCtx<'_>) -> Result<IRExpr> {
    let mut lowered = match expr {
        TypedExpr::Binary {
            left, op, right, ..
        } => ctx.binary(
            lower_projection(left, ctx)?,
            *op,
            lower_projection(right, ctx)?,
        )?,
        TypedExpr::Not { inner, .. } => fold::not(lower_projection(inner, ctx)?),
        TypedExpr::IsNull { inner, negated, .. } => {
            fold::is_null(lower_projection(inner, ctx)?, *negated)
        }
        TypedExpr::In { needle, list, .. } => {
            coerce::in_list(lower_projection(list, ctx)?, lower_projection(needle, ctx)?)?
        }
        _ => match expr.score_column() {
            Some((variable, property)) => IRExpr::PropAccess {
                variable: variable.to_string(),
                property: property.to_string(),
                ty: ExprType::from_prop(&PropType::scalar(expr.score_type(), false)),
            },
            None => lower_expr(expr, ctx)?,
        },
    };
    if let IRExpr::Literal(_, ty) = &mut lowered {
        *ty = expr.expr_type();
    }
    Ok(lowered)
}

/// Lower a query meta-field to the graph's physical spelling.
/// Bare names remain user properties.
fn physical_property(property: &str, system_columns: SystemColumns) -> String {
    match property {
        EDGE_TYPE_META => EDGE_TYPE_COLUMN.to_string(),
        name if name == SYSTEM_COLUMNS_META.id => system_columns.id.to_string(),
        name if name == SYSTEM_COLUMNS_META.src => system_columns.src.to_string(),
        name if name == SYSTEM_COLUMNS_META.dst => system_columns.dst.to_string(),
        other => other.to_string(),
    }
}

/// Every expression the compiler emits lowers through here: property leaves
/// on their physical columns (`LowerCtx::physical_column`), comparisons and
/// Boolean nodes through `LowerCtx::binary` and `fold`.
fn lower_expr(expr: &TypedExpr, ctx: &LowerCtx<'_>) -> Result<IRExpr> {
    let lower = |expr: &TypedExpr| lower_expr(expr, ctx);
    let mut lowered = match expr {
        TypedExpr::Now { .. } => IRExpr::Param(NOW_PARAM_NAME.to_string(), expr.expr_type()),
        TypedExpr::PropAccess {
            variable, property, ..
        } => {
            if property == EDGE_TYPE_META
                && let Some(BoundVariable::Edge { type_names }) = ctx.binding(variable)
                && let [name] = type_names.as_slice()
            {
                return Ok(IRExpr::Literal(
                    Literal::String(name.clone()),
                    expr.expr_type(),
                ));
            }
            IRExpr::PropAccess {
                variable: variable.clone(),
                property: ctx.physical_column(variable, property),
                ty: expr.expr_type(),
            }
        }
        TypedExpr::Nearest {
            variable,
            property,
            query,
            ..
        } => IRExpr::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: Box::new(lower(query)?),
            ty: expr.expr_type(),
        },
        TypedExpr::Search { field, query, .. } => IRExpr::Search {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: expr.expr_type(),
        },
        TypedExpr::Fuzzy {
            field,
            query,
            max_edits,
            ..
        } => IRExpr::Fuzzy {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            max_edits: max_edits.as_deref().map(lower).transpose()?.map(Box::new),
            ty: expr.expr_type(),
        },
        TypedExpr::MatchText { field, query, .. } => IRExpr::MatchText {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: expr.expr_type(),
        },
        TypedExpr::Bm25 { field, query, .. } => IRExpr::Bm25 {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: expr.expr_type(),
        },
        TypedExpr::Rrf {
            primary,
            secondary,
            k,
            ..
        } => IRExpr::Rrf {
            primary: Box::new(lower(primary)?),
            secondary: Box::new(lower(secondary)?),
            k: k.as_deref().map(lower).transpose()?.map(Box::new),
            ty: expr.expr_type(),
        },
        TypedExpr::Variable { name, ty } => match ty {
            ResolvedType::Scalar(_) => IRExpr::Param(name.clone(), expr.expr_type()),
            ResolvedType::Node(_) | ResolvedType::Aggregate(_) | ResolvedType::ForwardAlias => {
                IRExpr::Variable(name.clone(), expr.expr_type())
            }
        },
        TypedExpr::Literal { literal, .. } => IRExpr::Literal(literal.clone(), expr.expr_type()),
        TypedExpr::Aggregate {
            func,
            arg,
            signature,
        } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower(arg)?),
            signature: signature.clone(),
        },
        TypedExpr::AliasRef { name, .. } => IRExpr::AliasRef(name.clone(), expr.expr_type()),
        TypedExpr::Binary {
            left, op, right, ..
        } => ctx.binary(lower(left)?, *op, lower(right)?)?,
        TypedExpr::Not { inner, .. } => fold::not(lower(inner)?),
        TypedExpr::IsNull { inner, negated, .. } => fold::is_null(lower(inner)?, *negated),
        TypedExpr::In { needle, list, .. } => coerce::in_list(lower(list)?, lower(needle)?)?,
    };
    if let IRExpr::Literal(_, ty) = &mut lowered {
        *ty = expr.expr_type();
    }
    Ok(lowered)
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;

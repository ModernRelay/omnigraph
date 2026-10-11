use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SYSTEM_COLUMNS_META, SystemColumns};
use crate::error::{CompilerError, Result};
use crate::query::ast::*;
use crate::query::typecheck::{
    BoundVariable, ConstantClause, MutationTarget, MutationTypeContext, Scope, TypeContext,
    aggregate_signature, block_aggregate_signature, executed_column_name, expression_type,
    projection_type,
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

/// What every expression site shares: the parameters, the physical column
/// spellings and where a `contains` left operand's type lives. Every emitted
/// expression lowers through one, so `contains` resolution and `fold` apply everywhere.
struct LowerCtx<'a> {
    catalog: &'a Catalog,
    param_names: &'a HashSet<String>,
    param_types: &'a HashMap<String, PropType>,
    bindings: Bindings<'a>,
    type_ctx: &'a TypeContext,
    scope: Scope<'a>,
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

    fn expr_type(&self, expr: &Expr) -> Result<ExprType> {
        expression_type(
            self.catalog,
            expr,
            self.type_ctx,
            self.param_types,
            self.scope,
        )
    }

    /// `left <op> right`: `contains` over a scalar String left operand becomes
    /// `StringContains`, so execution dispatches on the IR op alone and never
    /// re-derives operand types; a literal-only node folds.
    fn binary(&self, left: IRExpr, op: BinaryOp, right: IRExpr) -> Result<IRExpr> {
        coerce::binary(left, op, right)
    }
}

pub fn lower_query(
    catalog: &Catalog,
    query: &QueryDecl,
    type_ctx: &TypeContext,
) -> Result<QueryIR> {
    if !query.mutations.is_empty() {
        return Err(crate::error::CompilerError::Plan(
            "cannot lower mutation query with read-query lowerer".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();
    let param_types = declared_param_types(query);

    let mut pipeline = Vec::new();
    let mut bound_vars = HashSet::new();
    let mut fresh = FreshNames::default();

    lower_clauses(
        catalog,
        &query.match_clause,
        type_ctx,
        &mut pipeline,
        &mut bound_vars,
        &HashSet::new(),
        &param_names,
        &param_types,
        &mut fresh,
    )?;

    let ctx = LowerCtx {
        catalog,
        param_names: &param_names,
        param_types: &param_types,
        bindings: Bindings::Read(&type_ctx.bindings),
        type_ctx,
        scope: Scope::Read,
    };
    let return_exprs: Vec<IRProjection> = query
        .return_clause
        .iter()
        .map(|p| {
            Ok(IRProjection {
                expr: lower_projection(&p.expr, &ctx)?,
                alias: p.alias.clone().or_else(|| meta_field_result_key(&p.expr)),
                column: executed_column_name(&p.expr, p.alias.as_deref()),
                ty: projection_type(
                    catalog,
                    &p.expr,
                    p.alias.as_deref(),
                    &query.order_clause,
                    type_ctx,
                    &param_types,
                )?,
            })
        })
        .collect::<Result<_>>()?;

    let has_aggregates = query
        .return_clause
        .iter()
        .any(|p| matches!(&p.expr, Expr::Aggregate { .. }));
    let aggregate_meta_columns: HashSet<String> = query
        .return_clause
        .iter()
        .filter(|p| has_aggregates && p.alias.is_none())
        .filter_map(|p| meta_field_result_key(&p.expr))
        .collect();

    let order_by: Vec<IROrdering> = query
        .order_clause
        .iter()
        .map(|o| {
            let expr = meta_field_result_key(&o.expr)
                .filter(|_| matches!(&o.expr, Expr::PropAccess { .. }))
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
                .map_or_else(|| lower_expr(&o.expr, &ctx), Ok)?;
            Ok(IROrdering {
                expr,
                descending: o.descending,
            })
        })
        .collect::<Result<_>>()?;

    let ir = QueryIR {
        name: query.name.clone(),
        params: lower_params(query, &param_types),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
    };
    super::validate::validate_query(&ir)?;
    Ok(ir)
}

pub fn lower_mutation_query(
    catalog: &Catalog,
    query: &QueryDecl,
    type_ctx: &MutationTypeContext,
) -> Result<MutationIR> {
    if query.mutations.is_empty() {
        return Err(crate::error::CompilerError::Plan(
            "query does not contain a mutation body".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();
    let param_types = declared_param_types(query);

    if query.mutations.len() != type_ctx.targets.len() {
        return Err(CompilerError::Plan(
            "mutation statements and checked targets differ".into(),
        ));
    }
    let ops = query
        .mutations
        .iter()
        .zip(&type_ctx.targets)
        .map(|(m, target)| lower_single_mutation(catalog, m, target, &param_names, &param_types))
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationIR {
        name: query.name.clone(),
        params: lower_params(query, &param_types),
        ops,
    })
}

fn lower_params(query: &QueryDecl, types: &HashMap<String, PropType>) -> Vec<IRParam> {
    query
        .params
        .iter()
        .map(|declaration| IRParam {
            declaration: declaration.clone(),
            ty: ExprType::from_prop(
                types
                    .get(&declaration.name)
                    .expect("parameter type checked before lowering"),
            ),
        })
        .collect()
}

/// Param types were validated during typecheck; unknown names simply don't
/// participate in `contains` overload resolution.
fn declared_param_types(query: &QueryDecl) -> HashMap<String, PropType> {
    query
        .params
        .iter()
        .filter_map(|p| {
            PropType::from_param_type_name(&p.type_name, p.nullable).map(|t| (p.name.clone(), t))
        })
        .collect()
}

/// Lower one mutation using its checked target namespace.
fn lower_single_mutation(
    catalog: &Catalog,
    mutation: &Mutation,
    checked_target: &MutationTarget,
    param_names: &HashSet<String>,
    param_types: &HashMap<String, PropType>,
) -> Result<MutationOpIR> {
    let type_name = match mutation {
        Mutation::Insert(insert) => &insert.type_name,
        Mutation::Update(update) => &update.type_name,
        Mutation::Delete(delete) => &delete.type_name,
    };
    if checked_target.type_name() != type_name.as_str() {
        return Err(CompilerError::Plan(format!(
            "checked mutation target '{}' differs from statement target '{type_name}'",
            checked_target.type_name()
        )));
    }
    let binding = match checked_target {
        MutationTarget::Node { type_name } => BoundVariable::Node {
            type_name: type_name.clone(),
        },
        MutationTarget::Edge { type_name } => BoundVariable::Edge {
            type_names: vec![type_name.clone()],
        },
    };
    let empty_scope = TypeContext::empty();
    let ctx = LowerCtx {
        catalog,
        param_names,
        param_types,
        bindings: Bindings::Mutation(binding),
        type_ctx: &empty_scope,
        scope: Scope::MutationWhere(checked_target),
    };
    let assignment_ctx = LowerCtx {
        scope: Scope::Constant {
            clause: ConstantClause::Assignment,
            type_name,
        },
        bindings: Bindings::Read(&empty_scope.bindings),
        ..ctx
    };
    let lower_assignments = |assignments: &[MutationAssignment]| {
        assignments
            .iter()
            .map(|a| {
                Ok(IRAssignment {
                    property: a.property.clone(),
                    value: lower_expr(&a.value, &assignment_ctx)?,
                })
            })
            .collect::<Result<_>>()
    };
    match mutation {
        Mutation::Insert(insert) => Ok(MutationOpIR::Insert {
            target: checked_target.clone(),
            assignments: lower_assignments(&insert.assignments)?,
        }),
        Mutation::Update(update) => Ok(MutationOpIR::Update {
            target: checked_target.clone(),
            assignments: lower_assignments(&update.assignments)?,
            predicate: lower_expr(&update.predicate, &ctx)?,
        }),
        Mutation::Delete(delete) => Ok(MutationOpIR::Delete {
            target: checked_target.clone(),
            predicate: lower_expr(&delete.predicate, &ctx)?,
        }),
    }
}

fn lower_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    type_ctx: &TypeContext,
    pipeline: &mut Vec<IROp>,
    bound_vars: &mut HashSet<String>,
    outer_physical_names: &HashSet<String>,
    param_names: &HashSet<String>,
    param_types: &HashMap<String, PropType>,
    fresh: &mut FreshNames,
) -> Result<()> {
    let mut bindings = Vec::new();
    let traversals = &type_ctx.traversals;
    let mut filters = Vec::new();
    let mut subqueries = Vec::new();
    let mut checked_subqueries = type_ctx.subqueries.iter();

    for clause in clauses {
        match clause {
            Clause::Binding(binding) => bindings.push(binding),
            Clause::Traversal(_) => {}
            Clause::Filter(filter) => filters.push(filter),
            Clause::Subquery(subquery) => subqueries.push((
                subquery,
                checked_subqueries
                    .next()
                    .expect("invariant: each subquery has a checked scope"),
            )),
        }
    }
    assert!(
        checked_subqueries.next().is_none(),
        "invariant: checked scopes match the query blocks"
    );
    let ctx = LowerCtx {
        catalog,
        param_names,
        param_types,
        bindings: Bindings::Read(&type_ctx.bindings),
        type_ctx,
        scope: Scope::Read,
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
    for t in traversals {
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
        .filter_map(|f| text_search_subject(f))
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
        let node_type = catalog
            .node_types
            .get(&binding.type_name)
            .expect("binding type was validated during typecheck");

        let binding_filters = build_binding_filters(binding, node_type, &ctx)?;

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
    let mut remaining: Vec<_> = traversals.iter().collect();
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
        pipeline.push(IROp::Filter(lower_expr(
            &(*filter).clone().with_search_predicates_spelled(),
            &ctx,
        )?));
    }

    if subqueries.is_empty() {
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

    for (subquery, checked) in subqueries {
        let block_clauses = subquery.clauses.as_slice();
        let visible_outer: HashSet<_> = bound_vars
            .iter()
            .filter(|name| checked.outer_bindings.contains_key(*name))
            .cloned()
            .collect();
        let outer_var = find_outer_var(block_clauses, &visible_outer);
        let mut collisions: Vec<_> = checked
            .inner
            .bindings
            .keys()
            .filter(|name| {
                !checked.outer_bindings.contains_key(*name) && physical_names.contains(*name)
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
            block_clauses,
            &checked.inner,
            &mut inner_pipeline,
            &mut inner_bound,
            &physical_names,
            param_names,
            param_types,
            fresh,
        )?;

        let inner_ctx = LowerCtx {
            catalog,
            param_names,
            param_types,
            bindings: Bindings::Read(&checked.inner.bindings),
            type_ctx: &checked.inner,
            scope: Scope::Read,
        };
        let mut outer_type_ctx = TypeContext::empty();
        outer_type_ctx.bindings = checked.outer_bindings.clone();
        let outer_ctx = LowerCtx {
            catalog,
            param_names,
            param_types,
            bindings: Bindings::Read(&checked.outer_bindings),
            type_ctx: &outer_type_ctx,
            scope: Scope::Read,
        };
        let mut left =
            match block_aggregate_signature(catalog, subquery, &checked.inner, param_types)? {
                None => BlockAggregateExpr::count_rows(),
                Some(signature) => {
                    let arg = subquery.arg.as_ref().ok_or_else(|| {
                        CompilerError::Plan("checked block aggregate has no argument".into())
                    })?;
                    BlockAggregateExpr::Aggregate {
                        func: subquery.func,
                        arg: Box::new(lower_expr(arg, &inner_ctx)?),
                        signature,
                    }
                }
            };
        rename_inner_bindings(&mut inner_pipeline, left.arg_mut(), &renames);
        let predicate = coerce::block(left, subquery.op, lower_expr(&subquery.right, &outer_ctx)?)?;

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

/// Build IR filters from a binding's inline property matches.
fn build_binding_filters(
    binding: &Binding,
    node_type: &crate::catalog::NodeType,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<IRExpr>> {
    let mut filters = Vec::new();
    for pm in &binding.prop_matches {
        let prop = node_type
            .properties
            .get(&pm.prop_name)
            .expect("binding property was validated during typecheck");
        let op = if prop.list {
            CompOp::Contains
        } else {
            CompOp::Eq
        };
        filters.push(ctx.binary(
            IRExpr::PropAccess {
                variable: binding.variable.clone(),
                property: pm.prop_name.clone(),
                ty: ExprType::from_prop(prop),
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

/// The variable a `search`, `fuzzy` or `match_text` call reads, anywhere in
/// the expression.
fn text_search_subject(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Search { field, .. } | Expr::Fuzzy { field, .. } | Expr::MatchText { field, .. } => {
            match field.as_ref() {
                Expr::PropAccess { variable, .. } => Some(variable.as_str()),
                _ => None,
            }
        }
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => text_search_subject(left).or_else(|| text_search_subject(right)),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => text_search_subject(inner),
        _ => None,
    }
}

fn find_outer_var(clauses: &[Clause], outer_bound: &HashSet<String>) -> Option<String> {
    for clause in clauses {
        match clause {
            Clause::Traversal(t) => {
                if outer_bound.contains(&t.src) {
                    return Some(t.src.clone());
                }
                if outer_bound.contains(&t.dst) {
                    return Some(t.dst.clone());
                }
            }
            Clause::Filter(f) => {
                if let Some(v) = outer_var_in_expr(f, outer_bound) {
                    return Some(v);
                }
            }
            Clause::Binding(b) if outer_bound.contains(&b.variable) => {
                return Some(b.variable.clone());
            }
            _ => {}
        }
    }
    None
}

/// The first outer-bound variable an expression's leaves read, operand by
/// operand in written order.
fn outer_var_in_expr(expr: &Expr, outer_bound: &HashSet<String>) -> Option<String> {
    match expr {
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => outer_var_in_expr(left, outer_bound).or_else(|| outer_var_in_expr(right, outer_bound)),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => {
            outer_var_in_expr(inner, outer_bound)
        }
        leaf => expr_var(leaf).filter(|v| outer_bound.contains(v)),
    }
}

fn expr_var(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Now => None,
        Expr::Binary { left, right, .. }
        | Expr::In {
            needle: left,
            list: right,
        } => expr_var(left).or_else(|| expr_var(right)),
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => expr_var(inner),
        Expr::PropAccess { variable, .. } => Some(variable.clone()),
        Expr::Variable(v) => Some(v.clone()),
        Expr::Nearest { variable, .. } => Some(variable.clone()),
        Expr::Search { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => expr_var(field)
            .or_else(|| expr_var(query))
            .or_else(|| max_edits.as_deref().and_then(expr_var)),
        Expr::MatchText { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Bm25 { field, query } => expr_var(field).or_else(|| expr_var(query)),
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => expr_var(primary)
            .or_else(|| expr_var(secondary))
            .or_else(|| k.as_deref().and_then(expr_var)),
        Expr::Aggregate { arg, .. } => expr_var(arg),
        _ => None,
    }
}

/// A projected rank expression, at the root or inside the Boolean structure
/// (`bm25($p.name, "x") > 0.0 as hit`), lowers to the score column the
/// retrieval appends (`Expr::score_column`); T33 required `order` to execute it.
fn lower_projection(expr: &Expr, ctx: &LowerCtx<'_>) -> Result<IRExpr> {
    let mut lowered = match expr {
        Expr::Binary { left, op, right } => ctx.binary(
            lower_projection(left, ctx)?,
            *op,
            lower_projection(right, ctx)?,
        )?,
        Expr::Not(inner) => fold::not(lower_projection(inner, ctx)?),
        Expr::IsNull { expr, negated } => fold::is_null(lower_projection(expr, ctx)?, *negated),
        Expr::In { needle, list } => {
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
        *ty = ctx.expr_type(expr)?;
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

/// The alias an unaliased meta-field projection needs. The executor keys a
/// projection by its physical column, which is vintage-specific, so the logical
/// `var.@id` rides as the alias and result columns read as the query wrote them.
fn meta_field_result_key(expr: &Expr) -> Option<String> {
    match expr {
        Expr::PropAccess { variable, property } if property.starts_with('@') => {
            Some(format!("{variable}.{property}"))
        }
        Expr::Aggregate { arg, .. } => meta_field_result_key(arg),
        _ => None,
    }
}

/// Every expression the compiler emits lowers through here: property leaves
/// on their physical columns (`LowerCtx::physical_column`), comparisons and
/// Boolean nodes through `LowerCtx::binary` and `fold`.
fn lower_expr(expr: &Expr, ctx: &LowerCtx<'_>) -> Result<IRExpr> {
    let lower = |expr: &Expr| lower_expr(expr, ctx);
    let mut lowered = match expr {
        Expr::Now => IRExpr::Param(NOW_PARAM_NAME.to_string(), ctx.expr_type(expr)?),
        Expr::PropAccess { variable, property } => {
            if property == EDGE_TYPE_META
                && let Some(BoundVariable::Edge { type_names }) = ctx.binding(variable)
                && let [name] = type_names.as_slice()
            {
                return Ok(IRExpr::Literal(
                    Literal::String(name.clone()),
                    ctx.expr_type(expr)?,
                ));
            }
            IRExpr::PropAccess {
                variable: variable.clone(),
                property: ctx.physical_column(variable, property),
                ty: ctx.expr_type(expr)?,
            }
        }
        Expr::Nearest {
            variable,
            property,
            query,
        } => IRExpr::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: Box::new(lower(query)?),
            ty: ctx.expr_type(expr)?,
        },
        Expr::Search { field, query } => IRExpr::Search {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: ctx.expr_type(expr)?,
        },
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => IRExpr::Fuzzy {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            max_edits: max_edits.as_deref().map(lower).transpose()?.map(Box::new),
            ty: ctx.expr_type(expr)?,
        },
        Expr::MatchText { field, query } => IRExpr::MatchText {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: ctx.expr_type(expr)?,
        },
        Expr::Bm25 { field, query } => IRExpr::Bm25 {
            field: Box::new(lower(field)?),
            query: Box::new(lower(query)?),
            ty: ctx.expr_type(expr)?,
        },
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => IRExpr::Rrf {
            primary: Box::new(lower(primary)?),
            secondary: Box::new(lower(secondary)?),
            k: k.as_deref().map(lower).transpose()?.map(Box::new),
            ty: ctx.expr_type(expr)?,
        },
        Expr::Variable(v) => {
            if ctx.param_names.contains(v) {
                IRExpr::Param(v.clone(), ctx.expr_type(expr)?)
            } else {
                IRExpr::Variable(v.clone(), ctx.expr_type(expr)?)
            }
        }
        Expr::Literal(l) => IRExpr::Literal(l.clone(), ctx.expr_type(expr)?),
        Expr::Aggregate { func, arg } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower(arg)?),
            signature: aggregate_signature(
                ctx.catalog,
                *func,
                arg,
                ctx.type_ctx,
                ctx.param_types,
                true,
            )?,
        },
        Expr::AliasRef(name) => IRExpr::AliasRef(name.clone(), ctx.expr_type(expr)?),
        Expr::Binary { left, op, right } => ctx.binary(lower(left)?, *op, lower(right)?)?,
        Expr::Not(inner) => fold::not(lower(inner)?),
        Expr::IsNull { expr, negated } => fold::is_null(lower(expr)?, *negated),
        Expr::In { needle, list } => coerce::in_list(lower(list)?, lower(needle)?)?,
    };
    if let IRExpr::Literal(_, ty) = &mut lowered {
        *ty = ctx.expr_type(expr)?;
    }
    Ok(lowered)
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;

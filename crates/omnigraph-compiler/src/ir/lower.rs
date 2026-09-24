use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SYSTEM_COLUMNS_META, SystemColumns};
use crate::error::Result;
use crate::query::ast::*;
use crate::query::typecheck::{BoundVariable, TypeContext};
use crate::types::{Direction, PropType, ScalarType};

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
}

/// The binding a variable names.
enum Bindings<'a> {
    /// A read: the clause list's own bindings and traversal endpoints first
    /// (a negation's inner clauses are typechecked into a discarded context
    /// clone, so the outer `TypeContext` never sees them), then the query's.
    Read {
        local: &'a HashMap<&'a str, BoundVariable>,
        type_ctx: &'a TypeContext,
    },
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
            Bindings::Read { local, type_ctx } => local
                .get(variable)
                .or_else(|| type_ctx.bindings.get(variable)),
            Bindings::Mutation(target) => match target {
                BoundVariable::Node { type_name } | BoundVariable::Edge { type_name } => {
                    (variable == type_name).then_some(target)
                }
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

    /// Whether a lowered left operand is a non-list String: a String literal,
    /// a String parameter, a system column, or a scalar String property of
    /// the binding its variable names. The test that resolves `contains`.
    fn is_scalar_string(&self, expr: &IRExpr) -> bool {
        let scalar_string =
            |prop: &PropType| !prop.list && matches!(prop.scalar, ScalarType::String);
        match expr {
            IRExpr::Literal(Literal::String(_)) => true,
            IRExpr::Param(name) => self.param_types.get(name).is_some_and(scalar_string),
            IRExpr::PropAccess { variable, property } => {
                let system_columns = self.system_columns();
                if [system_columns.id, system_columns.src, system_columns.dst]
                    .contains(&property.as_str())
                {
                    return true;
                }
                let declared = match self.binding(variable) {
                    Some(BoundVariable::Node { type_name }) => self
                        .catalog
                        .node_types
                        .get(type_name)
                        .and_then(|node_type| node_type.properties.get(property)),
                    Some(BoundVariable::Edge { type_name }) => self
                        .catalog
                        .lookup_edge_by_name(type_name)
                        .and_then(|edge_type| edge_type.properties.get(property)),
                    None => None,
                };
                declared.is_some_and(scalar_string)
            }
            _ => false,
        }
    }

    /// `left <op> right`: `contains` over a scalar String left operand becomes
    /// `StringContains`, so execution dispatches on the IR op alone and never
    /// re-derives operand types; a literal-only node folds.
    fn binary(&self, left: IRExpr, op: BinaryOp, right: IRExpr) -> IRExpr {
        let op = match op {
            BinaryOp::Compare(CompOp::Contains) if self.is_scalar_string(&left) => {
                BinaryOp::Compare(CompOp::StringContains)
            }
            other => other,
        };
        fold::binary(left, op, right)
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
        &param_names,
        &param_types,
        &mut fresh,
    )?;

    let no_local_bindings = HashMap::new();
    let ctx = LowerCtx {
        catalog,
        param_names: &param_names,
        param_types: &param_types,
        bindings: Bindings::Read {
            local: &no_local_bindings,
            type_ctx,
        },
    };
    let return_exprs: Vec<IRProjection> = query
        .return_clause
        .iter()
        .map(|p| IRProjection {
            expr: lower_projection(&p.expr, &ctx),
            alias: p.alias.clone().or_else(|| meta_field_result_key(&p.expr)),
        })
        .collect();

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
        .map(|o| IROrdering {
            expr: meta_field_result_key(&o.expr)
                .filter(|_| matches!(&o.expr, Expr::PropAccess { .. }))
                .filter(|name| aggregate_meta_columns.contains(name))
                .map(IRExpr::AliasRef)
                .unwrap_or_else(|| lower_expr(&o.expr, &ctx)),
            descending: o.descending,
        })
        .collect();

    let ir = QueryIR {
        name: query.name.clone(),
        params: query.params.clone(),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
    };
    super::validate::validate_query(&ir)?;
    Ok(ir)
}

pub fn lower_mutation_query(catalog: &Catalog, query: &QueryDecl) -> Result<MutationIR> {
    if query.mutations.is_empty() {
        return Err(crate::error::CompilerError::Plan(
            "query does not contain a mutation body".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();
    let param_types = declared_param_types(query);

    let ops = query
        .mutations
        .iter()
        .map(|m| lower_single_mutation(catalog, m, &param_names, &param_types))
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationIR {
        name: query.name.clone(),
        params: query.params.clone(),
        ops,
    })
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

/// One mutation statement; its target is the node type of that name, else
/// the edge type, the order the type checker resolves it in.
fn lower_single_mutation(
    catalog: &Catalog,
    mutation: &Mutation,
    param_names: &HashSet<String>,
    param_types: &HashMap<String, PropType>,
) -> Result<MutationOpIR> {
    let type_name = match mutation {
        Mutation::Insert(insert) => &insert.type_name,
        Mutation::Update(update) => &update.type_name,
        Mutation::Delete(delete) => &delete.type_name,
    };
    let target = if catalog.node_types.contains_key(type_name) {
        BoundVariable::Node {
            type_name: type_name.clone(),
        }
    } else {
        BoundVariable::Edge {
            type_name: type_name.clone(),
        }
    };
    let ctx = LowerCtx {
        catalog,
        param_names,
        param_types,
        bindings: Bindings::Mutation(target),
    };
    let lower_assignments = |assignments: &[MutationAssignment]| {
        assignments
            .iter()
            .map(|a| IRAssignment {
                property: a.property.clone(),
                value: lower_expr(&a.value, &ctx),
            })
            .collect()
    };
    match mutation {
        Mutation::Insert(insert) => Ok(MutationOpIR::Insert {
            type_name: insert.type_name.clone(),
            assignments: lower_assignments(&insert.assignments),
        }),
        Mutation::Update(update) => Ok(MutationOpIR::Update {
            type_name: update.type_name.clone(),
            assignments: lower_assignments(&update.assignments),
            predicate: lower_expr(&update.predicate, &ctx),
        }),
        Mutation::Delete(delete) => Ok(MutationOpIR::Delete {
            type_name: delete.type_name.clone(),
            predicate: lower_expr(&delete.predicate, &ctx),
        }),
    }
}

fn lower_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    type_ctx: &TypeContext,
    pipeline: &mut Vec<IROp>,
    bound_vars: &mut HashSet<String>,
    param_names: &HashSet<String>,
    param_types: &HashMap<String, PropType>,
    fresh: &mut FreshNames,
) -> Result<()> {
    // Separate clause types for ordering: bindings first, then traversals, then filters
    let mut bindings = Vec::new();
    let mut traversals = Vec::new();
    let mut filters = Vec::new();
    let mut subqueries: Vec<&Subquery> = Vec::new();

    for clause in clauses {
        match clause {
            Clause::Binding(b) => bindings.push(b),
            Clause::Traversal(t) => traversals.push(t),
            Clause::Filter(f) => filters.push(f),
            Clause::Subquery(subquery) => subqueries.push(subquery),
        }
    }

    let mut local_bindings: HashMap<&str, BoundVariable> = HashMap::new();
    for t in &traversals {
        if let Some(edge) = catalog.lookup_edge_by_name(&t.edge_name) {
            local_bindings
                .entry(t.src.as_str())
                .or_insert_with(|| BoundVariable::Node {
                    type_name: edge.from_type.clone(),
                });
            local_bindings
                .entry(t.dst.as_str())
                .or_insert_with(|| BoundVariable::Node {
                    type_name: edge.to_type.clone(),
                });
            if let Some(eb) = &t.edge_binding {
                local_bindings
                    .entry(eb.as_str())
                    .or_insert_with(|| BoundVariable::Edge {
                        type_name: edge.name.clone(),
                    });
            }
        }
    }
    for b in &bindings {
        local_bindings.insert(
            b.variable.as_str(),
            BoundVariable::Node {
                type_name: b.type_name.clone(),
            },
        );
    }
    let ctx = LowerCtx {
        catalog,
        param_names,
        param_types,
        bindings: Bindings::Read {
            local: &local_bindings,
            type_ctx,
        },
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

        let binding_filters = build_binding_filters(binding, node_type, &ctx);

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
    let mut remaining: Vec<&Traversal> = traversals.to_vec();
    while !remaining.is_empty() {
        let mut next_remaining = Vec::new();
        for traversal in &remaining {
            let src_bound = bound_vars.contains(&traversal.src);
            let dst_bound = bound_vars.contains(&traversal.dst);
            if !src_bound && !dst_bound {
                next_remaining.push(*traversal);
                continue;
            }

            let edge = catalog
                .lookup_edge_by_name(&traversal.edge_name)
                .ok_or_else(|| {
                    crate::error::CompilerError::Plan(format!(
                        "lowering traversal referenced missing edge '{}' after typecheck",
                        traversal.edge_name
                    ))
                })?;

            // Undirected is carried on the AST node itself — negation inners
            // are typechecked into a discarded context clone, so the
            // ResolvedTraversal lookup below cannot see their direction; the
            // syntax is the source of truth for Both.
            let direction = if traversal.undirected {
                Direction::Both
            } else {
                type_ctx
                    .traversals
                    .iter()
                    .find(|rt| {
                        rt.src == traversal.src
                            && rt.dst == traversal.dst
                            && rt.edge_type == edge.name
                    })
                    .map(|rt| rt.direction)
                    .unwrap_or(Direction::Out)
            };

            let dst_type = match direction {
                Direction::Out => edge.to_type.clone(),
                Direction::In => edge.from_type.clone(),
                // Undirected requires from_type == to_type (typecheck rule),
                // so either endpoint type is correct.
                Direction::Both => edge.to_type.clone(),
            };

            if src_bound && dst_bound {
                // Cycle closing: expand to a temp var, then filter temp.id = dst.id
                // (temp fresh per traversal, #605).
                let temp_var = fresh.temp(&traversal.dst);
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: temp_var.clone(),
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: vec![],
                    edge_binding: traversal
                        .edge_binding
                        .as_deref()
                        .filter(|binding| *binding != "_")
                        .map(str::to_string),
                });
                pipeline.push(IROp::Filter(IRExpr::comparison(
                    IRExpr::PropAccess {
                        variable: temp_var,
                        property: catalog.system_columns.id.to_string(),
                    },
                    CompOp::Eq,
                    IRExpr::PropAccess {
                        variable: traversal.dst.clone(),
                        property: catalog.system_columns.id.to_string(),
                    },
                )));
            } else if !src_bound && dst_bound {
                // Reverse expand: dst is bound, src is not.
                let reverse_dir = match direction {
                    Direction::Out => Direction::In,
                    Direction::In => Direction::Out,
                    // Symmetric: reversing an undirected expand is a no-op.
                    Direction::Both => Direction::Both,
                };
                let src_type = match direction {
                    Direction::Out => edge.from_type.clone(),
                    Direction::In => edge.to_type.clone(),
                    Direction::Both => edge.from_type.clone(),
                };
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
                    edge_type: edge.name.clone(),
                    direction: reverse_dir,
                    dst_type: src_type,
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
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
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
            break;
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
        )));
    }

    for subquery in subqueries {
        let block_clauses = subquery.clauses.as_slice();
        let predicate = SubqueryPredicate {
            func: subquery.func,
            arg: subquery_argument(subquery, &ctx),
            op: subquery.op,
            right: lower_expr(&subquery.right, &ctx),
        };
        let outer_var = find_outer_var(block_clauses, bound_vars);

        let mut inner_pipeline = Vec::new();
        let mut inner_bound = bound_vars.clone();
        lower_clauses(
            catalog,
            block_clauses,
            type_ctx,
            &mut inner_pipeline,
            &mut inner_bound,
            param_names,
            param_types,
            fresh,
        )?;

        pipeline.push(IROp::AntiJoin {
            outer_var: outer_var.unwrap_or_default(),
            inner: inner_pipeline,
            predicate,
        });
    }

    Ok(())
}

/// Build IR filters from a binding's inline property matches.
fn build_binding_filters(
    binding: &Binding,
    node_type: &crate::catalog::NodeType,
    ctx: &LowerCtx<'_>,
) -> Vec<IRExpr> {
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
        filters.push(IRExpr::comparison(
            IRExpr::PropAccess {
                variable: binding.variable.clone(),
                property: pm.prop_name.clone(),
            },
            op,
            lower_expr(&pm.value, ctx),
        ));
    }
    filters
}

/// The aggregate's argument; `count($m) { … }` over a binding counts rows,
/// as `count($m)` in a return counts the binding.
fn subquery_argument(subquery: &Subquery, ctx: &LowerCtx<'_>) -> Option<IRExpr> {
    match &subquery.arg {
        Some(Expr::Variable(v))
            if subquery.func == AggFunc::Count && !ctx.param_names.contains(v) =>
        {
            None
        }
        Some(arg) => Some(lower_expr(arg, ctx)),
        None => None,
    }
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
        Expr::Binary { left, right, .. } => {
            text_search_subject(left).or_else(|| text_search_subject(right))
        }
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
        Expr::Binary { left, right, .. } => {
            outer_var_in_expr(left, outer_bound).or_else(|| outer_var_in_expr(right, outer_bound))
        }
        Expr::Not(inner) | Expr::IsNull { expr: inner, .. } => {
            outer_var_in_expr(inner, outer_bound)
        }
        leaf => expr_var(leaf).filter(|v| outer_bound.contains(v)),
    }
}

fn expr_var(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Now => None,
        Expr::Binary { left, right, .. } => expr_var(left).or_else(|| expr_var(right)),
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
fn lower_projection(expr: &Expr, ctx: &LowerCtx<'_>) -> IRExpr {
    match expr {
        Expr::Binary { left, op, right } => ctx.binary(
            lower_projection(left, ctx),
            *op,
            lower_projection(right, ctx),
        ),
        Expr::Not(inner) => fold::not(lower_projection(inner, ctx)),
        Expr::IsNull { expr, negated } => fold::is_null(lower_projection(expr, ctx), *negated),
        _ => match expr.score_column() {
            Some((variable, property)) => IRExpr::PropAccess {
                variable: variable.to_string(),
                property: property.to_string(),
            },
            None => lower_expr(expr, ctx),
        },
    }
}

/// Lower a query meta-field to the graph's physical spelling.
/// Bare names remain user properties.
fn physical_property(property: &str, system_columns: SystemColumns) -> String {
    match property {
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
fn lower_expr(expr: &Expr, ctx: &LowerCtx<'_>) -> IRExpr {
    let lower = |expr: &Expr| lower_expr(expr, ctx);
    match expr {
        Expr::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
        Expr::PropAccess { variable, property } => IRExpr::PropAccess {
            variable: variable.clone(),
            property: ctx.physical_column(variable, property),
        },
        Expr::Nearest {
            variable,
            property,
            query,
        } => IRExpr::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: Box::new(lower(query)),
        },
        Expr::Search { field, query } => IRExpr::Search {
            field: Box::new(lower(field)),
            query: Box::new(lower(query)),
        },
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => IRExpr::Fuzzy {
            field: Box::new(lower(field)),
            query: Box::new(lower(query)),
            max_edits: max_edits.as_ref().map(|expr| Box::new(lower(expr))),
        },
        Expr::MatchText { field, query } => IRExpr::MatchText {
            field: Box::new(lower(field)),
            query: Box::new(lower(query)),
        },
        Expr::Bm25 { field, query } => IRExpr::Bm25 {
            field: Box::new(lower(field)),
            query: Box::new(lower(query)),
        },
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => IRExpr::Rrf {
            primary: Box::new(lower(primary)),
            secondary: Box::new(lower(secondary)),
            k: k.as_ref().map(|expr| Box::new(lower(expr))),
        },
        Expr::Variable(v) => {
            if ctx.param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
        Expr::Literal(l) => IRExpr::Literal(l.clone()),
        Expr::Aggregate { func, arg } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower(arg)),
        },
        Expr::AliasRef(name) => IRExpr::AliasRef(name.clone()),
        Expr::Binary { left, op, right } => ctx.binary(lower(left), *op, lower(right)),
        Expr::Not(inner) => fold::not(lower(inner)),
        Expr::IsNull { expr, negated } => fold::is_null(lower(expr), *negated),
    }
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;

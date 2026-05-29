use std::collections::{HashMap, HashSet, VecDeque};

use crate::catalog::Catalog;
use crate::error::Result;
use crate::query::ast::*;
use crate::query::typecheck::TypeContext;
use crate::types::Direction;

use super::*;

pub fn lower_query(
    catalog: &Catalog,
    query: &QueryDecl,
    type_ctx: &TypeContext,
) -> Result<QueryIR> {
    if !query.mutations.is_empty() {
        return Err(crate::error::NanoError::Plan(
            "cannot lower mutation query with read-query lowerer".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();

    let mut pipeline = Vec::new();
    let mut bound_vars = HashSet::new();

    lower_clauses(
        catalog,
        &query.match_clause,
        type_ctx,
        &mut pipeline,
        &mut bound_vars,
        &param_names,
    )?;

    let return_exprs: Vec<IRProjection> = query
        .return_clause
        .iter()
        .map(|p| IRProjection {
            expr: lower_expr(&p.expr, &param_names),
            alias: p.alias.clone(),
        })
        .collect();

    let order_by: Vec<IROrdering> = query
        .order_clause
        .iter()
        .map(|o| IROrdering {
            expr: lower_expr(&o.expr, &param_names),
            descending: o.descending,
        })
        .collect();

    Ok(QueryIR {
        name: query.name.clone(),
        params: query.params.clone(),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
    })
}

pub fn lower_mutation_query(query: &QueryDecl) -> Result<MutationIR> {
    if query.mutations.is_empty() {
        return Err(crate::error::NanoError::Plan(
            "query does not contain a mutation body".to_string(),
        ));
    }
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();

    let ops = query
        .mutations
        .iter()
        .map(|m| lower_single_mutation(m, &param_names))
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationIR {
        name: query.name.clone(),
        params: query.params.clone(),
        ops,
    })
}

fn lower_single_mutation(
    mutation: &Mutation,
    param_names: &HashSet<String>,
) -> Result<MutationOpIR> {
    match mutation {
        Mutation::Insert(insert) => Ok(MutationOpIR::Insert {
            type_name: insert.type_name.clone(),
            assignments: insert
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, param_names),
                })
                .collect(),
        }),
        Mutation::Update(update) => Ok(MutationOpIR::Update {
            type_name: update.type_name.clone(),
            assignments: update
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, param_names),
                })
                .collect(),
            predicate: IRMutationPredicate {
                property: update.predicate.property.clone(),
                op: update.predicate.op,
                value: lower_match_value(&update.predicate.value, param_names),
            },
        }),
        Mutation::Delete(delete) => Ok(MutationOpIR::Delete {
            type_name: delete.type_name.clone(),
            predicate: IRMutationPredicate {
                property: delete.predicate.property.clone(),
                op: delete.predicate.op,
                value: lower_match_value(&delete.predicate.value, param_names),
            },
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
) -> Result<()> {
    // Separate clause types for ordering: bindings first, then traversals, then filters
    let mut bindings = Vec::new();
    let mut traversals = Vec::new();
    let mut filters = Vec::new();
    let mut negations = Vec::new();

    for clause in clauses {
        match clause {
            Clause::Binding(b) => bindings.push(b),
            Clause::Traversal(t) => traversals.push(t),
            Clause::Filter(f) => filters.push(f),
            Clause::Negation(inner) => negations.push(inner),
        }
    }

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

    for binding in &bindings {
        if component_visited.contains(binding.variable.as_str()) {
            continue;
        }
        // BFS from this binding through the traversal graph
        let mut queue = VecDeque::new();
        queue.push_back(binding.variable.as_str());
        let mut component_bindings: Vec<&str> = Vec::new();

        while let Some(var) = queue.pop_front() {
            if !component_visited.insert(var) {
                continue;
            }
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

        // First binding in the component is the root; defer the rest.
        for var in component_bindings.into_iter().skip(1) {
            deferred_set.insert(var.to_string());
        }
    }

    // Build deferred filters map for variables introduced by traversals
    let mut deferred_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();

    // Lower bindings into NodeScan ops (skip deferred ones)
    for binding in &bindings {
        let node_type = catalog
            .node_types
            .get(&binding.type_name)
            .expect("binding type was validated during typecheck");

        let binding_filters = build_binding_filters(binding, node_type, param_names);

        if deferred_set.contains(&binding.variable) {
            // Save filters for emission after the Expand that introduces
            // this variable.
            if !binding_filters.is_empty() {
                deferred_filters.insert(binding.variable.clone(), binding_filters);
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
                    crate::error::NanoError::Plan(format!(
                        "lowering traversal referenced missing edge '{}' after typecheck",
                        traversal.edge_name
                    ))
                })?;

            let direction = type_ctx
                .traversals
                .iter()
                .find(|rt| {
                    rt.src == traversal.src && rt.dst == traversal.dst && rt.edge_type == edge.name
                })
                .map(|rt| rt.direction)
                .unwrap_or(Direction::Out);

            let dst_type = match direction {
                Direction::Out => edge.to_type.clone(),
                Direction::In => edge.from_type.clone(),
            };

            if src_bound && dst_bound {
                // Cycle closing: expand to a temp var, then filter temp.id = dst.id
                let temp_var = format!("__temp_{}", traversal.dst);
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: temp_var.clone(),
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: vec![],
                });
                pipeline.push(IROp::Filter(IRFilter {
                    left: IRExpr::PropAccess {
                        variable: temp_var,
                        property: "id".to_string(),
                    },
                    op: CompOp::Eq,
                    right: IRExpr::PropAccess {
                        variable: traversal.dst.clone(),
                        property: "id".to_string(),
                    },
                }));
            } else if !src_bound && dst_bound {
                // Reverse expand: dst is bound, src is not.
                let reverse_dir = match direction {
                    Direction::Out => Direction::In,
                    Direction::In => Direction::Out,
                };
                let src_type = match direction {
                    Direction::Out => edge.from_type.clone(),
                    Direction::In => edge.to_type.clone(),
                };
                let introduced_filters =
                    deferred_filters.remove(&traversal.src).unwrap_or_default();
                pipeline.push(IROp::Expand {
                    src_var: traversal.dst.clone(),
                    dst_var: traversal.src.clone(),
                    edge_type: edge.name.clone(),
                    direction: reverse_dir,
                    dst_type: src_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
                });
                if traversal.src != "_" {
                    bound_vars.insert(traversal.src.clone());
                }
            } else {
                // Normal expand: src is bound, dst is not.
                let introduced_filters =
                    deferred_filters.remove(&traversal.dst).unwrap_or_default();
                pipeline.push(IROp::Expand {
                    src_var: traversal.src.clone(),
                    dst_var: traversal.dst.clone(),
                    edge_type: edge.name.clone(),
                    direction,
                    dst_type,
                    min_hops: traversal.min_hops,
                    max_hops: traversal.max_hops,
                    dst_filters: introduced_filters,
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

    // Lower explicit filters
    for filter in &filters {
        pipeline.push(IROp::Filter(IRFilter {
            left: lower_expr(&filter.left, param_names),
            op: filter.op,
            right: lower_expr(&filter.right, param_names),
        }));
    }

    // Lower negations into AntiJoin ops
    for neg_clauses in &negations {
        // Find outer-bound variable referenced in the negation
        let outer_var = find_outer_var(neg_clauses, bound_vars);

        let mut inner_pipeline = Vec::new();
        let mut inner_bound = bound_vars.clone();
        lower_clauses(
            catalog,
            neg_clauses,
            type_ctx,
            &mut inner_pipeline,
            &mut inner_bound,
            param_names,
        )?;

        pipeline.push(IROp::AntiJoin {
            outer_var: outer_var.unwrap_or_default(),
            inner: inner_pipeline,
        });
    }

    Ok(())
}

/// Build IR filters from a binding's inline property matches.
fn build_binding_filters(
    binding: &Binding,
    node_type: &crate::catalog::NodeType,
    param_names: &HashSet<String>,
) -> Vec<IRFilter> {
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
        let right = match &pm.value {
            MatchValue::Literal(lit) => IRExpr::Literal(lit.clone()),
            MatchValue::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
            MatchValue::Variable(v) => {
                if param_names.contains(v) {
                    IRExpr::Param(v.clone())
                } else {
                    IRExpr::Variable(v.clone())
                }
            }
        };
        filters.push(IRFilter {
            left: IRExpr::PropAccess {
                variable: binding.variable.clone(),
                property: pm.prop_name.clone(),
            },
            op,
            right,
        });
    }
    filters
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
                if let Some(v) = expr_var(&f.left)
                    && outer_bound.contains(&v)
                {
                    return Some(v);
                }
                if let Some(v) = expr_var(&f.right)
                    && outer_bound.contains(&v)
                {
                    return Some(v);
                }
            }
            Clause::Binding(b) => {
                if outer_bound.contains(&b.variable) {
                    return Some(b.variable.clone());
                }
            }
            _ => {}
        }
    }
    None
}

fn expr_var(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Now => None,
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

fn lower_expr(expr: &Expr, param_names: &HashSet<String>) -> IRExpr {
    match expr {
        Expr::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
        Expr::PropAccess { variable, property } => IRExpr::PropAccess {
            variable: variable.clone(),
            property: property.clone(),
        },
        Expr::Nearest {
            variable,
            property,
            query,
        } => IRExpr::Nearest {
            variable: variable.clone(),
            property: property.clone(),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Search { field, query } => IRExpr::Search {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Fuzzy {
            field,
            query,
            max_edits,
        } => IRExpr::Fuzzy {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
            max_edits: max_edits
                .as_ref()
                .map(|expr| Box::new(lower_expr(expr, param_names))),
        },
        Expr::MatchText { field, query } => IRExpr::MatchText {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Bm25 { field, query } => IRExpr::Bm25 {
            field: Box::new(lower_expr(field, param_names)),
            query: Box::new(lower_expr(query, param_names)),
        },
        Expr::Rrf {
            primary,
            secondary,
            k,
        } => IRExpr::Rrf {
            primary: Box::new(lower_expr(primary, param_names)),
            secondary: Box::new(lower_expr(secondary, param_names)),
            k: k.as_ref()
                .map(|expr| Box::new(lower_expr(expr, param_names))),
        },
        Expr::Variable(v) => {
            if param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
        Expr::Literal(l) => IRExpr::Literal(l.clone()),
        Expr::Aggregate { func, arg } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower_expr(arg, param_names)),
        },
        Expr::AliasRef(name) => IRExpr::AliasRef(name.clone()),
    }
}

fn lower_match_value(value: &MatchValue, param_names: &HashSet<String>) -> IRExpr {
    match value {
        MatchValue::Now => IRExpr::Param(NOW_PARAM_NAME.to_string()),
        MatchValue::Literal(l) => IRExpr::Literal(l.clone()),
        MatchValue::Variable(v) => {
            if param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
    }
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;

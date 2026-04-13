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
                    rt.src == traversal.src
                        && rt.dst == traversal.dst
                        && rt.edge_type == edge.name
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
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::query::parser::parse_query;
    use crate::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
    use crate::schema::parser::parse_schema;

    fn setup() -> Catalog {
        let schema = parse_schema(
            r#"
node Person { name: String  age: I32? }
node Company { name: String }
edge Knows: Person -> Person { since: Date? }
edge WorksAt: Person -> Company
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    #[test]
    fn test_lower_basic() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name, $f.age }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert_eq!(ir.pipeline.len(), 2); // NodeScan + Expand
        assert_eq!(ir.return_exprs.len(), 2);
    }

    #[test]
    fn test_lower_negation() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert_eq!(ir.pipeline.len(), 2); // NodeScan + AntiJoin
        assert!(matches!(&ir.pipeline[1], IROp::AntiJoin { .. }));
    }

    #[test]
    fn test_lower_mutation_update() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        assert!(matches!(checked, CheckedQuery::Mutation(_)));

        let ir = lower_mutation_query(&qf.queries[0]).unwrap();
        match &ir.ops[0] {
            MutationOpIR::Update {
                type_name,
                assignments,
                predicate,
            } => {
                assert_eq!(type_name, "Person");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].property, "age");
                assert_eq!(predicate.property, "name");
            }
            _ => panic!("expected update mutation op"),
        }
    }

    #[test]
    fn test_lower_bounded_traversal() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{1,3} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();
        let expand = ir
            .pipeline
            .iter()
            .find_map(|op| match op {
                IROp::Expand {
                    min_hops, max_hops, ..
                } => Some((*min_hops, *max_hops)),
                _ => None,
            })
            .expect("expected expand op");
        assert_eq!(expand.0, 1);
        assert_eq!(expand.1, Some(3));
    }

    #[test]
    fn test_lower_now_uses_reserved_runtime_param() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query stamp() {
    match { $p: Person }
    return { now() as ts }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert!(matches!(
            ir.return_exprs[0].expr,
            IRExpr::Param(ref name) if name == NOW_PARAM_NAME
        ));
    }

    #[test]
    fn test_lower_mutation_now_uses_reserved_runtime_param() {
        let catalog = build_catalog(
            &parse_schema(
                r#"
node Event {
    slug: String @key
    updated_at: DateTime?
}
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let qf = parse_query(
            r#"
query stamp() {
    update Event set { updated_at: now() } where updated_at = now()
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        assert!(matches!(checked, CheckedQuery::Mutation(_)));

        let ir = lower_mutation_query(&qf.queries[0]).unwrap();
        match &ir.ops[0] {
            MutationOpIR::Update {
                assignments,
                predicate,
                ..
            } => {
                assert!(matches!(
                    assignments[0].value,
                    IRExpr::Param(ref name) if name == NOW_PARAM_NAME
                ));
                assert!(matches!(
                    predicate.value,
                    IRExpr::Param(ref name) if name == NOW_PARAM_NAME
                ));
            }
            _ => panic!("expected update mutation op"),
        }
    }

    #[test]
    fn test_lower_multi_mutation() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String, $age: I32, $friend: String) {
    insert Person { name: $name, age: $age }
    insert Knows { from: $name, to: $friend }
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        assert!(matches!(checked, CheckedQuery::Mutation(_)));

        let ir = lower_mutation_query(&qf.queries[0]).unwrap();
        assert_eq!(ir.ops.len(), 2);
        assert!(
            matches!(&ir.ops[0], MutationOpIR::Insert { type_name, .. } if type_name == "Person")
        );
        assert!(
            matches!(&ir.ops[1], MutationOpIR::Insert { type_name, .. } if type_name == "Knows")
        );
    }

    /// Destination binding is deferred: NodeScan + Expand + Filter (no cross-join).
    #[test]
    fn test_lower_traversal_with_destination_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p worksAt $c
        $c: Company { name: "Acme" }
    }
    return { $p.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Should be: NodeScan($p) → Expand($p→$c, dst_filters=[name=="Acme"])
        // NOT:       NodeScan($p) → NodeScan($c) → cross-join → cycle-close
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "p" && dst_var == "c" && dst_filters.len() == 1
        ));
    }

    /// Multi-hop chain: all intermediate and final bindings are deferred.
    #[test]
    fn test_lower_chain_defers_all_intermediate_bindings() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
        $f: Person { name: "Bob" }
        $f worksAt $c
        $c: Company { name: "Acme" }
    }
    return { $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Should be: NodeScan($p,[name=Alice]) → Expand($p→$f, [name==Bob])
        //            → Expand($f→$c, [name==Acme])
        assert_eq!(ir.pipeline.len(), 3);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "p" && dst_var == "f" && dst_filters.len() == 1
        ));
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "f" && dst_var == "c" && dst_filters.len() == 1
        ));
    }

    /// Reverse traversal: source binding is deferred when destination is the root.
    #[test]
    fn test_lower_reverse_traversal_defers_source_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $c: Company { name: "Acme" }
        $p worksAt $c
        $p: Person { name: "Alice" }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // $c is root (first declared). $p is deferred (connected via traversal).
        // Traversal $p worksAt $c: $c is bound, $p is not → reverse expand.
        // Pipeline: NodeScan($c,[name=Acme]) → Expand($c→$p, In, [name==Alice])
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "c"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "c" && dst_var == "p" && dst_filters.len() == 1
        ));
    }

    /// Independent bindings (no traversal) still cross-join.
    #[test]
    fn test_lower_independent_bindings_still_cross_join() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $c: Company
    }
    return { $p.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // No traversal connecting them → both get NodeScans (cross-join at runtime)
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(&ir.pipeline[1], IROp::NodeScan { variable, .. } if variable == "c"));
    }

    /// Destination binding without filters: no NodeScan, no post-expand filter.
    #[test]
    fn test_lower_destination_binding_without_filters() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p worksAt $c
        $c: Company
    }
    return { $p.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // $c binding is deferred (no filters) → just NodeScan + Expand
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, .. }
            if src_var == "p" && dst_var == "c"
        ));
    }

    /// Traversals declared in non-topological order are reordered automatically.
    #[test]
    fn test_lower_out_of_order_traversals() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $f worksAt $c
        $p knows $f
        $f: Person
        $c: Company { name: "Acme" }
    }
    return { $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Even though "$f worksAt $c" is declared before "$p knows $f",
        // the iterative lowering processes "$p knows $f" first (because $p
        // is bound) and then "$f worksAt $c" (once $f is bound).
        assert_eq!(ir.pipeline.len(), 3);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        // First expand: $p → $f (knows)
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, .. }
            if src_var == "p" && dst_var == "f"
        ));
        // Second expand: $f → $c (worksAt), with filter from $c binding
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "f" && dst_var == "c" && dst_filters.len() == 1
        ));
    }

    /// Wildcard $_ must not bridge unrelated components in the adjacency graph.
    #[test]
    fn test_lower_wildcard_does_not_bridge_components() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows $_
        $c: Company
    }
    return { $p.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // $p and $c are in separate components (connected only through $_).
        // Both must get their own NodeScan — $c must NOT be deferred.
        // Bindings are emitted first, then traversals.
        assert_eq!(ir.pipeline.len(), 3);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(&ir.pipeline[1], IROp::NodeScan { variable, .. } if variable == "c"));
        // The expand for $p knows $_ (wildcard destination)
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, .. }
            if src_var == "p" && dst_var == "_"
        ));
    }

    /// Fan-out: one root fans to two deferred destinations via different edges.
    #[test]
    fn test_lower_fan_out_topology() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
        $f: Person { name: "Bob" }
        $p worksAt $c
        $c: Company { name: "Acme" }
    }
    return { $f.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Root: $p. Deferred: $f, $c (both reachable from $p).
        assert_eq!(ir.pipeline.len(), 3);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "p" && dst_var == "f" && dst_filters.len() == 1
        ));
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "p" && dst_var == "c" && dst_filters.len() == 1
        ));
    }

    /// Fan-in: two sources converge on one destination; second source is
    /// introduced via reverse expand from the shared destination.
    #[test]
    fn test_lower_fan_in_topology() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $a: Person { name: "Alice" }
        $a knows $c
        $b: Person { name: "Bob" }
        $b knows $c
        $c: Person
    }
    return { $a.name, $b.name, $c.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Root: $a (first in component {a,b,c}). Deferred: $b, $c.
        // $a knows $c: expand(a→c). $b knows $c: reverse expand(c→b).
        assert_eq!(ir.pipeline.len(), 3);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "a"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "a" && dst_var == "c" && dst_filters.is_empty()
        ));
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "c" && dst_var == "b" && dst_filters.len() == 1
        ));
    }

    /// Genuine graph cycle: deferred binding is introduced by first traversal,
    /// second traversal triggers cycle-closing.
    #[test]
    fn test_lower_cycle_with_deferred_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $a: Person
        $a knows $b
        $b: Person { name: "Bob" }
        $b knows $a
    }
    return { $a.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // $b is deferred, introduced by first expand.
        // Second traversal ($b knows $a) is genuine cycle-closing.
        assert_eq!(ir.pipeline.len(), 4);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "a"));
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "a" && dst_var == "b" && dst_filters.len() == 1
        ));
        // Cycle-closing expand to __temp_a
        assert!(matches!(
            &ir.pipeline[2],
            IROp::Expand { src_var, dst_var, dst_filters, .. }
            if src_var == "b" && dst_var.starts_with("__temp_") && dst_filters.is_empty()
        ));
        // Cycle-closing filter: __temp_a.id == a.id
        assert!(matches!(&ir.pipeline[3], IROp::Filter(_)));
    }

    /// Multiple filters on a single deferred binding.
    #[test]
    fn test_lower_multiple_filters_on_deferred_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
        $f: Person { name: "Bob", age: 25 }
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Two prop_matches → two dst_filters on the Expand.
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { dst_filters, .. }
            if dst_filters.len() == 2
        ));
    }

    /// Parameter in a deferred binding filter (unit test level).
    #[test]
    fn test_lower_param_filter_on_deferred_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($company: String) {
    match {
        $p: Person
        $p worksAt $c
        $c: Company { name: $company }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(
            &ir.pipeline[1],
            IROp::Expand { dst_filters, .. }
            if dst_filters.len() == 1
        ));
        // The filter's right-hand side should be a Param, not a Literal
        if let IROp::Expand { dst_filters, .. } = &ir.pipeline[1] {
            assert!(matches!(&dst_filters[0].right, IRExpr::Param(name) if name == "company"));
        }
    }

    /// Negation with inner binding: inner binding is NOT deferred because
    /// bound_vars (from outer scope) is not in binding_set for the inner call.
    /// This documents current behavior — the inner pipeline uses a NodeScan +
    /// cycle-closing, which is correct but less efficient than deferral.
    #[test]
    fn test_lower_negation_with_inner_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        not {
            $p worksAt $c
            $c: Company { name: "Acme" }
        }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        // Outer: NodeScan($p) + AntiJoin
        assert_eq!(ir.pipeline.len(), 2);
        assert!(matches!(&ir.pipeline[0], IROp::NodeScan { variable, .. } if variable == "p"));
        let IROp::AntiJoin { inner, .. } = &ir.pipeline[1] else {
            panic!("expected AntiJoin");
        };
        // Inner pipeline: $c is NOT deferred (it's the only binding in the
        // inner scope), so it gets a NodeScan + cycle-closing (3 ops).
        assert_eq!(inner.len(), 3);
        assert!(matches!(&inner[0], IROp::NodeScan { variable, .. } if variable == "c"));
        assert!(matches!(&inner[1], IROp::Expand { .. }));
        assert!(matches!(&inner[2], IROp::Filter(_)));
    }
}

use std::collections::HashSet;

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
    if query.mutation.is_some() {
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
    let mutation = query.mutation.as_ref().ok_or_else(|| {
        crate::error::NanoError::Plan("query does not contain a mutation body".to_string())
    })?;
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();

    let op = match mutation {
        Mutation::Insert(insert) => MutationOpIR::Insert {
            type_name: insert.type_name.clone(),
            assignments: insert
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, &param_names),
                })
                .collect(),
        },
        Mutation::Update(update) => MutationOpIR::Update {
            type_name: update.type_name.clone(),
            assignments: update
                .assignments
                .iter()
                .map(|a| IRAssignment {
                    property: a.property.clone(),
                    value: lower_match_value(&a.value, &param_names),
                })
                .collect(),
            predicate: IRMutationPredicate {
                property: update.predicate.property.clone(),
                op: update.predicate.op,
                value: lower_match_value(&update.predicate.value, &param_names),
            },
        },
        Mutation::Delete(delete) => MutationOpIR::Delete {
            type_name: delete.type_name.clone(),
            predicate: IRMutationPredicate {
                property: delete.predicate.property.clone(),
                op: delete.predicate.op,
                value: lower_match_value(&delete.predicate.value, &param_names),
            },
        },
    };

    Ok(MutationIR {
        name: query.name.clone(),
        params: query.params.clone(),
        op,
    })
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

    // Lower bindings into NodeScan ops
    for binding in &bindings {
        let node_type = catalog
            .node_types
            .get(&binding.type_name)
            .expect("binding type was validated during typecheck");
        // Collect inline filters from prop matches
        let mut scan_filters = Vec::new();
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
            match &pm.value {
                MatchValue::Literal(lit) => {
                    scan_filters.push(IRFilter {
                        left: IRExpr::PropAccess {
                            variable: binding.variable.clone(),
                            property: pm.prop_name.clone(),
                        },
                        op,
                        right: IRExpr::Literal(lit.clone()),
                    });
                }
                MatchValue::Now => {
                    scan_filters.push(IRFilter {
                        left: IRExpr::PropAccess {
                            variable: binding.variable.clone(),
                            property: pm.prop_name.clone(),
                        },
                        op,
                        right: IRExpr::Param(NOW_PARAM_NAME.to_string()),
                    });
                }
                MatchValue::Variable(v) => {
                    let right = if param_names.contains(v) {
                        IRExpr::Param(v.clone())
                    } else {
                        IRExpr::Variable(v.clone())
                    };
                    scan_filters.push(IRFilter {
                        left: IRExpr::PropAccess {
                            variable: binding.variable.clone(),
                            property: pm.prop_name.clone(),
                        },
                        op,
                        right,
                    });
                }
            }
        }

        pipeline.push(IROp::NodeScan {
            variable: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            filters: scan_filters,
        });
        bound_vars.insert(binding.variable.clone());
    }

    // Lower traversals into Expand ops
    // Handle "cycle closing" — if both src and dst are already bound, use a filter
    for traversal in &traversals {
        let edge = catalog.lookup_edge_by_name(&traversal.edge_name).unwrap();

        // Determine direction from type context
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

        if bound_vars.contains(&traversal.src) && bound_vars.contains(&traversal.dst) {
            // Cycle closing: emit expand to a temp var, then filter temp.id = dst.id
            let temp_var = format!("__temp_{}", traversal.dst);
            pipeline.push(IROp::Expand {
                src_var: traversal.src.clone(),
                dst_var: temp_var.clone(),
                edge_type: edge.name.clone(),
                direction,
                dst_type,
                min_hops: traversal.min_hops,
                max_hops: traversal.max_hops,
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
        } else if !bound_vars.contains(&traversal.src) && bound_vars.contains(&traversal.dst) {
            // Reverse expand: dst is bound, src is not.
            // Swap direction and expand from dst to discover src.
            let reverse_dir = match direction {
                Direction::Out => Direction::In,
                Direction::In => Direction::Out,
            };
            let src_type = match direction {
                Direction::Out => edge.from_type.clone(),
                Direction::In => edge.to_type.clone(),
            };
            pipeline.push(IROp::Expand {
                src_var: traversal.dst.clone(),
                dst_var: traversal.src.clone(),
                edge_type: edge.name.clone(),
                direction: reverse_dir,
                dst_type: src_type,
                min_hops: traversal.min_hops,
                max_hops: traversal.max_hops,
            });
            if traversal.src != "_" {
                bound_vars.insert(traversal.src.clone());
            }
        } else {
            pipeline.push(IROp::Expand {
                src_var: traversal.src.clone(),
                dst_var: traversal.dst.clone(),
                edge_type: edge.name.clone(),
                direction,
                dst_type,
                min_hops: traversal.min_hops,
                max_hops: traversal.max_hops,
            });
            if traversal.dst != "_" {
                bound_vars.insert(traversal.dst.clone());
            }
        }
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
        match ir.op {
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
        match ir.op {
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
}

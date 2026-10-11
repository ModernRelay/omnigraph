//! Plan well-formedness: every variable the executor will turn into a column
//! prefix is introduced by exactly one operator, and every reference to a
//! variable comes after its introduction.
//!
//! The executor names columns `<variable>.<property>` and appends batches by
//! position (`hconcat_batches`), so two operators producing one variable put
//! two columns of one name into the wide batch (#605). Lowering keeps that
//! from happening for the shapes it knows; this check holds for every plan,
//! so a new lowering shape fails here with the variable named, not in the
//! executor after a scan has run.

use std::collections::HashSet;

use crate::error::{CompilerError, Result};

use super::{IRExpr, IROp, QueryIR};

/// Checks a lowered read query. A negation's inner pipeline runs over the
/// outer batch, so it sees the outer variables and may not introduce one of
/// them again; what it introduces stays inside it.
pub(crate) fn validate_query(ir: &QueryIR) -> Result<()> {
    let mut introduced = HashSet::new();
    validate_pipeline(&ir.pipeline, &mut introduced)?;
    for projection in &ir.return_exprs {
        projection.expr.check_types()?;
        if projection.expr.ty() != &projection.ty
            || matches!(projection.ty, crate::types::ExprType::ExactInteger { .. })
        {
            return Err(CompilerError::Plan(
                "projection type differs from its expression or exposes an internal carrier".into(),
            ));
        }
        check_expr(&projection.expr, &introduced)?;
    }
    for ordering in &ir.order_by {
        ordering.expr.check_types()?;
        check_expr(&ordering.expr, &introduced)?;
    }
    Ok(())
}

fn validate_pipeline(pipeline: &[IROp], introduced: &mut HashSet<String>) -> Result<()> {
    for op in pipeline {
        match op {
            // Every field spelled, so a field added later that carries a
            // variable is a compile error here, not an unchecked reference.
            IROp::NodeScan {
                variable,
                type_name: _,
                filters,
            } => {
                introduce(variable, introduced)?;
                check_filters(filters, introduced)?;
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
                check_reference(src_var, introduced)?;
                introduce(dst_var, introduced)?;
                if let Some(edge) = edge_binding {
                    introduce(edge, introduced)?;
                }
                check_filters(dst_filters, introduced)?;
            }
            IROp::Filter(filter) => check_expr(filter, introduced)?,
            IROp::AntiJoin {
                outer_var,
                inner,
                predicate,
            } => {
                // Lowering leaves `outer_var` empty when the negation shares
                // no variable with the outer pattern.
                if !outer_var.is_empty() {
                    check_reference(outer_var, introduced)?;
                }
                predicate.check_types()?;
                check_expr(&predicate.right, introduced)?;
                let mut inner_scope = introduced.clone();
                validate_pipeline(inner, &mut inner_scope)?;
                if let Some(arg) = predicate.left.arg() {
                    check_expr(arg, &inner_scope)?;
                }
            }
        }
    }
    Ok(())
}

fn introduce(variable: &str, introduced: &mut HashSet<String>) -> Result<()> {
    if !introduced.insert(variable.to_string()) {
        return Err(CompilerError::Plan(format!(
            "plan introduces variable `{variable}` twice; each variable is produced by one operator"
        )));
    }
    Ok(())
}

fn check_reference(variable: &str, introduced: &HashSet<String>) -> Result<()> {
    if !introduced.contains(variable) {
        return Err(CompilerError::Plan(format!(
            "plan references variable `{variable}` before any operator introduces it; \
             bind it (`${variable}: <Type>`) or reach it by a traversal from a bound variable"
        )));
    }
    Ok(())
}

fn check_filters(filters: &[IRExpr], introduced: &HashSet<String>) -> Result<()> {
    filters.iter().try_for_each(|f| check_expr(f, introduced))
}

/// Exhaustive over `IRExpr`, so a new variant that carries a variable is a
/// compile error here rather than an unchecked reference.
fn check_expr(expr: &IRExpr, introduced: &HashSet<String>) -> Result<()> {
    expr.check_types()?;
    let mut pending = vec![expr];
    while let Some(expr) = pending.pop() {
        match expr {
            IRExpr::PropAccess { variable, .. } => check_reference(variable, introduced)?,
            IRExpr::Nearest {
                variable, query, ..
            } => {
                check_reference(variable, introduced)?;
                pending.push(query);
            }
            IRExpr::Variable(variable, _) => check_reference(variable, introduced)?,
            IRExpr::Search { field, query, .. }
            | IRExpr::MatchText { field, query, .. }
            | IRExpr::Bm25 { field, query, .. } => pending.extend([field.as_ref(), query.as_ref()]),
            IRExpr::Fuzzy {
                field,
                query,
                max_edits,
                ..
            } => {
                pending.extend([field.as_ref(), query.as_ref()]);
                pending.extend(max_edits.as_deref());
            }
            IRExpr::Rrf {
                primary,
                secondary,
                k,
                ..
            } => {
                pending.extend([primary.as_ref(), secondary.as_ref()]);
                pending.extend(k.as_deref());
            }
            IRExpr::Aggregate { arg, .. }
            | IRExpr::Not(arg, _)
            | IRExpr::IsNull { expr: arg, .. }
            | IRExpr::Cast { expr: arg, .. } => pending.push(arg),
            IRExpr::Binary { left, right, .. } => pending.extend([left.as_ref(), right.as_ref()]),
            IRExpr::Param(_, _) | IRExpr::Literal(_, _) | IRExpr::AliasRef(_, _) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{AggFunc, CompOp, Literal};
    use crate::traversal::{EdgeMember, EdgeSelection};
    use crate::types::{AggSignature, Direction, ExprType, PropType, ScalarType};

    fn scan(variable: &str) -> IROp {
        IROp::NodeScan {
            variable: variable.to_string(),
            type_name: "Person".to_string(),
            filters: Vec::new(),
        }
    }

    fn expand(src: &str, dst: &str) -> IROp {
        IROp::Expand {
            src_var: src.to_string(),
            dst_var: dst.to_string(),
            edges: EdgeSelection::Named(EdgeMember {
                edge_type: "Knows".to_string(),
                direction: Direction::Out,
            }),
            src_type: "Person".to_string(),
            dst_type: "Person".to_string(),
            min_hops: 1,
            max_hops: Some(1),
            dst_filters: Vec::new(),
            edge_binding: None,
        }
    }

    fn prop(variable: &str) -> IRExpr {
        IRExpr::PropAccess {
            variable: variable.to_string(),
            property: "name".to_string(),
            ty: crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                crate::types::ScalarType::String,
                false,
            )),
        }
    }

    fn run(pipeline: Vec<IROp>) -> Result<()> {
        validate_pipeline(&pipeline, &mut HashSet::new())
    }

    fn refusal(pipeline: Vec<IROp>) -> String {
        run(pipeline)
            .expect_err("expected the plan to be refused")
            .to_string()
    }

    /// Malformed stored signatures cannot be supplied through GQ source.
    #[test]
    fn aggregate_types_reject_wrong_results_and_unsupported_arguments() {
        let integer = ExprType::from_prop(&PropType::scalar(ScalarType::I64, false));
        let float = ExprType::from_prop(&PropType::scalar(ScalarType::F64, true));
        for (arg, result, accepted) in [
            (integer.clone(), float.clone(), true),
            (integer.clone(), integer.clone(), false),
            (
                integer,
                ExprType::from_prop(&PropType::scalar(ScalarType::F64, false)),
                false,
            ),
            (
                ExprType::from_prop(&PropType::list_of(ScalarType::I64, false)),
                float.clone(),
                false,
            ),
            (
                ExprType::Node {
                    type_name: "Person".into(),
                },
                float,
                false,
            ),
        ] {
            let expression = IRExpr::null_test(
                IRExpr::Aggregate {
                    func: AggFunc::Sum,
                    arg: Box::new(IRExpr::Literal(
                        Literal::Integer(1),
                        crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                            crate::types::ScalarType::I64,
                            false,
                        )),
                    )),
                    signature: AggSignature { arg, result },
                },
                false,
            );
            assert_eq!(expression.check_types().is_ok(), accepted, "{expression:?}");
        }
    }

    #[test]
    fn accepts_a_scan_and_an_expand() {
        run(vec![scan("p"), expand("p", "q")]).unwrap();
    }

    #[test]
    fn refuses_a_second_producer_of_one_variable() {
        // The plan the pre-#605 lowering produced for `$p knows $_` twice.
        assert!(refusal(vec![scan("p"), expand("p", "_"), expand("p", "_")]).contains("`_` twice"));
        // And for `$p: Person` twice.
        assert!(refusal(vec![scan("p"), scan("p")]).contains("`p` twice"));
        // Fresh names per occurrence are what the fixed lowering emits.
        run(vec![
            scan("p"),
            expand("p", "__anon_1"),
            expand("p", "__anon_2"),
        ])
        .unwrap();
    }

    #[test]
    fn refuses_a_reference_before_introduction() {
        assert!(refusal(vec![expand("p", "q")]).contains("`p` before"));
        let filter = IROp::Filter(IRExpr::comparison(
            prop("q"),
            CompOp::Eq,
            IRExpr::Literal(
                crate::query::ast::Literal::String("x".to_string()),
                crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                    crate::types::ScalarType::String,
                    false,
                )),
            ),
        ));
        assert!(refusal(vec![scan("p"), filter]).contains("`q` before"));
    }

    #[test]
    fn edge_binding_is_a_producer_too() {
        let mut bound = expand("p", "q");
        if let IROp::Expand { edge_binding, .. } = &mut bound {
            *edge_binding = Some("k".to_string());
        }
        run(vec![scan("p"), bound.clone()]).unwrap();
        assert!(refusal(vec![scan("p"), scan("k"), bound]).contains("`k` twice"));
    }

    #[test]
    fn negation_inner_sees_the_outer_scope_and_keeps_its_own() {
        let anti = |inner: Vec<IROp>| IROp::AntiJoin {
            outer_var: "p".to_string(),
            inner,
            predicate: crate::ir::SubqueryPredicate::not_exists(),
        };
        // The inner pipeline may expand from the outer variable.
        run(vec![scan("p"), anti(vec![expand("p", "q")])]).unwrap();
        // It may not produce the outer variable again (the second #605 shape
        // inside `not { }`, before the fix lowered it to a filter).
        assert!(refusal(vec![scan("p"), anti(vec![scan("p")])]).contains("`p` twice"));
        // What it introduces does not leak: `q` is free again after it.
        run(vec![
            scan("p"),
            anti(vec![expand("p", "q")]),
            expand("p", "q"),
        ])
        .unwrap();
        // An anti-join over a variable nobody introduced is refused.
        assert!(refusal(vec![anti(vec![scan("q")])]).contains("`p` before"));
    }
}

//! Stored expression trees for explain. Text remains the public GQ rendering;
//! types are declarations carried by the IR, including explicit conversions.

use omnigraph_compiler::ir::{BlockAggregateExpr, IRExpr, IRProjection};
use omnigraph_compiler::query::ast::BinaryOp;
use serde_json::{Value, json};

use crate::logical::Predicate;

pub(crate) fn expr(root: &IRExpr) -> Value {
    enum Step<'a> {
        Visit(&'a IRExpr),
        Finish(&'a IRExpr, &'static str, usize),
    }
    let mut pending = vec![Step::Visit(root)];
    let mut completed = Vec::new();
    while let Some(step) = pending.pop() {
        match step {
            Step::Visit(expr) => {
                let (op, children): (&'static str, Vec<&IRExpr>) = match expr {
                    IRExpr::PropAccess { .. } => ("property", vec![]),
                    IRExpr::Nearest { query, .. } => ("nearest", vec![query]),
                    IRExpr::Search { field, query, .. } => ("search", vec![field, query]),
                    IRExpr::Fuzzy {
                        field,
                        query,
                        max_edits,
                        ..
                    } => {
                        let mut args = vec![field.as_ref(), query.as_ref()];
                        args.extend(max_edits.as_deref());
                        ("fuzzy", args)
                    }
                    IRExpr::MatchText { field, query, .. } => ("match_text", vec![field, query]),
                    IRExpr::Bm25 { field, query, .. } => ("bm25", vec![field, query]),
                    IRExpr::Rrf {
                        primary,
                        secondary,
                        k,
                        ..
                    } => {
                        let mut args = vec![primary.as_ref(), secondary.as_ref()];
                        args.extend(k.as_deref());
                        ("rrf", args)
                    }
                    IRExpr::Variable(_, _) => ("variable", vec![]),
                    IRExpr::Param(_, _) => ("param", vec![]),
                    IRExpr::Literal(_, _) => ("literal", vec![]),
                    IRExpr::Aggregate { arg, .. } => ("aggregate", vec![arg]),
                    IRExpr::AliasRef(_, _) => ("alias", vec![]),
                    IRExpr::Binary {
                        left, op, right, ..
                    } => (
                        match op {
                            BinaryOp::And => "and",
                            BinaryOp::Or => "or",
                            BinaryOp::Compare(_) => "compare",
                        },
                        vec![left, right],
                    ),
                    IRExpr::Not(inner, _) => ("not", vec![inner]),
                    IRExpr::IsNull { expr, .. } => ("is_null", vec![expr]),
                    IRExpr::Cast { expr, .. } => ("cast", vec![expr]),
                };

                pending.push(Step::Finish(expr, op, children.len()));
                pending.extend(children.into_iter().rev().map(Step::Visit));
            }
            Step::Finish(expr, op, count) => {
                let children = completed.split_off(completed.len() - count);
                let tree = json!({ "op": op, "gq": expr.to_string(), "type": expr.ty().spelling(), "args": children });
                if pending.is_empty() {
                    return tree;
                }
                completed.push(tree);
            }
        }
    }
    unreachable!("the root always schedules its finish step")
}

pub(crate) fn exprs(exprs: &[IRExpr]) -> Vec<Value> {
    exprs.iter().map(expr).collect()
}

pub(crate) fn returns(returns: &[IRProjection]) -> Vec<Value> {
    returns
        .iter()
        .map(|projection| expr(&projection.expr))
        .collect()
}

pub(crate) fn predicate(predicate: &Predicate) -> Vec<Value> {
    let mut pending = vec![predicate];
    let mut trees = Vec::new();
    while let Some(predicate) = pending.pop() {
        match predicate {
            Predicate::Gq { filter, .. } => trees.push(expr(&filter.0)),
            Predicate::And { left, right } => {
                pending.push(right);
                pending.push(left);
            }
            Predicate::IdAfter { .. } | Predicate::VersionWindow { .. } => {}
        }
    }
    trees
}

pub(crate) fn block(left: &BlockAggregateExpr) -> Value {
    let mut current = left;
    let mut casts = Vec::new();
    let (mut tree, gq) = loop {
        let (op, args) = match current {
            BlockAggregateExpr::CountRows { .. } => ("count_rows", vec![]),
            BlockAggregateExpr::Aggregate { arg, .. } => ("aggregate", vec![expr(arg)]),
            BlockAggregateExpr::Cast { expr, ty } => {
                casts.push(ty);
                current = expr;
                continue;
            }
        };
        let gq = current.to_string();
        break (
            json!({ "op": op, "gq": gq, "type": current.ty().spelling(), "args": args }),
            gq,
        );
    };
    for ty in casts.into_iter().rev() {
        tree = json!({ "op": "cast", "gq": gq, "type": ty.spelling(), "args": [tree] });
    }
    tree
}

pub(crate) fn block_aggregate(
    left: &BlockAggregateExpr,
    spec: Option<crate::AggregateSpec>,
) -> Value {
    match (left.leaf(), spec) {
        (
            BlockAggregateExpr::Aggregate {
                func, signature, ..
            },
            Some(spec),
        ) => json!({
            "gq": left.leaf().to_string(),
            "func": func.to_string(),
            "input": signature.arg.spelling(),
            "accumulator": spec.accumulator,
            "overflow": spec.overflow,
            "result": signature.result.spelling(),
        }),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::GqFilter;
    use omnigraph_compiler::query::ast::{CompOp, Literal};
    use omnigraph_compiler::{ExprType, PropType, ScalarType};

    fn ty(scalar: ScalarType, nullable: bool) -> ExprType {
        ExprType::from_prop(&PropType::scalar(scalar, nullable))
    }

    #[test]
    fn typed_tree_keeps_branch_order_while_finishing_children_first() {
        let cast = |name: &str, scalar| IRExpr::Cast {
            expr: Box::new(IRExpr::Param(name.into(), ty(scalar, false))),
            ty: ty(ScalarType::I64, false),
        };
        let comparison = IRExpr::comparison(
            cast("left", ScalarType::I32),
            CompOp::Lt,
            cast("right", ScalarType::U32),
        );
        let tree = expr(&comparison);
        assert_eq!(tree["op"], "compare");
        assert_eq!(tree["args"][0]["gq"], "$left");
        assert_eq!(tree["args"][0]["args"][0]["type"], "I32");
        assert_eq!(tree["args"][1]["gq"], "$right");
        assert_eq!(tree["args"][1]["args"][0]["type"], "U32");
        let fuzzy = IRExpr::Fuzzy {
            field: Box::new(IRExpr::PropAccess {
                variable: "p".into(),
                property: "name".into(),
                ty: ty(ScalarType::String, false),
            }),
            query: Box::new(IRExpr::Param("query".into(), ty(ScalarType::String, false))),
            max_edits: Some(Box::new(IRExpr::Literal(
                Literal::Integer(2),
                ty(ScalarType::I64, false),
            ))),
            ty: ty(ScalarType::Bool, false),
        };
        let tree = expr(&fuzzy);
        assert_eq!(tree["args"].as_array().unwrap().len(), 3);
        assert_eq!(tree["args"][0]["gq"], "$p.name");
        assert_eq!(tree["args"][1]["gq"], "$query");
        assert_eq!(tree["args"][2]["gq"], "2");
    }

    #[test]
    fn typed_tree_prints_stored_cast_types_and_keeps_original_text() {
        let left = IRExpr::Cast {
            expr: Box::new(IRExpr::PropAccess {
                variable: "p".into(),
                property: "age".into(),
                ty: ty(ScalarType::I64, true),
            }),
            ty: ty(ScalarType::F64, true),
        };
        let comparison = IRExpr::comparison(
            left,
            CompOp::Gt,
            IRExpr::Literal(Literal::Float(30.5), ty(ScalarType::F64, false)),
        );
        let tree = expr(&comparison);
        assert_eq!(tree["gq"], "$p.age > 30.5");
        assert_eq!(tree["type"], "Bool?");
        assert_eq!(
            tree["args"][0],
            json!({"op":"cast", "gq":"$p.age", "type":"F64?", "args":[{"op":"property", "gq":"$p.age", "type":"I64?", "args":[]}]})
        );
        let pred = Predicate::And {
            left: Box::new(Predicate::IdAfter {
                id: "before".into(),
            }),
            right: Box::new(Predicate::Gq {
                reads: vec![],
                text: comparison.to_string(),
                filter: GqFilter(comparison),
            }),
        };
        assert_eq!(predicate(&pred), vec![tree]);
    }
}

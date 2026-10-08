use super::{
    BlockAggregateExpr, IRExpr, IROp, IROrdering, IRProjection, QueryIR, SubqueryPredicate, untyped,
};

impl QueryIR {
    /// The read representation accepted by the frozen reference executor.
    pub fn erase(&self) -> untyped::QueryIR {
        let Self {
            name,
            params,
            pipeline,
            return_exprs,
            order_by,
            limit,
        } = self;
        untyped::QueryIR {
            name: name.clone(),
            params: params
                .iter()
                .map(|param| param.declaration.clone())
                .collect(),
            pipeline: pipeline.iter().map(IROp::erase).collect(),
            return_exprs: return_exprs.iter().map(IRProjection::erase).collect(),
            order_by: order_by.iter().map(IROrdering::erase).collect(),
            limit: *limit,
        }
    }
}

impl IROp {
    fn erase(&self) -> untyped::IROp {
        match self {
            Self::NodeScan {
                variable,
                type_name,
                filters,
            } => untyped::IROp::NodeScan {
                variable: variable.clone(),
                type_name: type_name.clone(),
                filters: filters.iter().map(IRExpr::erase).collect(),
            },
            Self::Expand {
                src_var,
                dst_var,
                edges,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                dst_filters,
                edge_binding,
            } => untyped::IROp::Expand {
                src_var: src_var.clone(),
                dst_var: dst_var.clone(),
                edges: edges.clone(),
                src_type: src_type.clone(),
                dst_type: dst_type.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                dst_filters: dst_filters.iter().map(IRExpr::erase).collect(),
                edge_binding: edge_binding.clone(),
            },
            Self::Filter(expr) => untyped::IROp::Filter(expr.erase()),
            Self::AntiJoin {
                outer_var,
                inner,
                predicate,
            } => untyped::IROp::AntiJoin {
                outer_var: outer_var.clone(),
                inner: inner.iter().map(IROp::erase).collect(),
                predicate: predicate.erase(),
            },
        }
    }
}

impl SubqueryPredicate {
    fn erase(&self) -> untyped::SubqueryPredicate {
        let Self { left, op, right } = self;
        let mut owner = left;
        let (func, arg) = loop {
            match owner {
                BlockAggregateExpr::CountRows { ty: _ } => {
                    break (crate::query::ast::AggFunc::Count, None);
                }
                BlockAggregateExpr::Aggregate {
                    func,
                    arg,
                    signature: _,
                } => break (*func, Some(arg.erase())),
                BlockAggregateExpr::Cast { expr, ty: _ } => owner = expr,
            }
        };
        untyped::SubqueryPredicate {
            func,
            arg,
            op: *op,
            right: right.erase(),
        }
    }
}

impl IRProjection {
    fn erase(&self) -> untyped::IRProjection {
        let Self {
            expr,
            alias,
            column: _,
            ty: _,
        } = self;
        untyped::IRProjection {
            expr: expr.erase(),
            alias: alias.clone(),
        }
    }
}

impl IROrdering {
    fn erase(&self) -> untyped::IROrdering {
        let Self { expr, descending } = self;
        untyped::IROrdering {
            expr: expr.erase(),
            descending: *descending,
        }
    }
}

impl IRExpr {
    fn erase(&self) -> untyped::IRExpr {
        match self {
            Self::Cast { expr, ty: _ } => expr.erase(),
            Self::PropAccess {
                variable,
                property,
                ty: _,
            } => untyped::IRExpr::PropAccess {
                variable: variable.clone(),
                property: property.clone(),
            },
            Self::Nearest {
                variable,
                property,
                query,
                ty: _,
            } => untyped::IRExpr::Nearest {
                variable: variable.clone(),
                property: property.clone(),
                query: Box::new(query.erase()),
            },
            Self::Search {
                field,
                query,
                ty: _,
            } => untyped::IRExpr::Search {
                field: Box::new(field.erase()),
                query: Box::new(query.erase()),
            },
            Self::Fuzzy {
                field,
                query,
                max_edits,
                ty: _,
            } => untyped::IRExpr::Fuzzy {
                field: Box::new(field.erase()),
                query: Box::new(query.erase()),
                max_edits: max_edits.as_ref().map(|expr| Box::new(expr.erase())),
            },
            Self::MatchText {
                field,
                query,
                ty: _,
            } => untyped::IRExpr::MatchText {
                field: Box::new(field.erase()),
                query: Box::new(query.erase()),
            },
            Self::Bm25 {
                field,
                query,
                ty: _,
            } => untyped::IRExpr::Bm25 {
                field: Box::new(field.erase()),
                query: Box::new(query.erase()),
            },
            Self::Rrf {
                primary,
                secondary,
                k,
                ty: _,
            } => untyped::IRExpr::Rrf {
                primary: Box::new(primary.erase()),
                secondary: Box::new(secondary.erase()),
                k: k.as_ref().map(|expr| Box::new(expr.erase())),
            },
            Self::Variable(name, _) => untyped::IRExpr::Variable(name.clone()),
            Self::Param(name, _) => untyped::IRExpr::Param(name.clone()),
            Self::Literal(literal, _) => untyped::IRExpr::Literal(literal.clone()),
            Self::Aggregate {
                func,
                arg,
                signature: _,
            } => untyped::IRExpr::Aggregate {
                func: *func,
                arg: Box::new(arg.erase()),
            },
            Self::AliasRef(alias, _) => untyped::IRExpr::AliasRef(alias.clone()),
            Self::Binary {
                left,
                op,
                right,
                ty: _,
            } => untyped::IRExpr::Binary {
                left: Box::new(left.erase()),
                op: *op,
                right: Box::new(right.erase()),
            },
            Self::Not(expr, _) => untyped::IRExpr::Not(Box::new(expr.erase())),
            Self::IsNull {
                expr,
                negated,
                ty: _,
            } => untyped::IRExpr::IsNull {
                expr: Box::new(expr.erase()),
                negated: *negated,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::IRParam;
    use crate::query::ast::{AggFunc, BinaryOp, CompOp, Literal, Param};
    use crate::traversal::{EdgeMember, EdgeSelection};
    use crate::types::{AggSignature, Direction, ExprType, PropType, ScalarType};

    #[test]
    fn casts_erase_to_the_original_expression() {
        let expression = IRExpr::Cast {
            expr: Box::new(IRExpr::Param(
                "amount".into(),
                ExprType::from_prop(&PropType::scalar(ScalarType::I32, true)),
            )),
            ty: ExprType::ExactInteger {
                list: false,
                nullable: true,
            },
        };
        assert_eq!(expression.erase(), untyped::IRExpr::Param("amount".into()));
    }

    #[test]
    fn every_expression_variant_preserves_its_fields() {
        let field = IRExpr::PropAccess {
            variable: "document".into(),
            property: "body".into(),
            ty: ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
        };
        let expected_field = untyped::IRExpr::PropAccess {
            variable: "document".into(),
            property: "body".into(),
        };
        let query = IRExpr::Param(
            "needle".into(),
            ExprType::from_prop(&PropType::scalar(ScalarType::String, false)),
        );
        let expected_query = untyped::IRExpr::Param("needle".into());
        let mut cases = vec![
            (field.clone(), expected_field.clone()),
            (query.clone(), expected_query.clone()),
            (
                IRExpr::Variable(
                    "person".into(),
                    ExprType::Node {
                        type_name: "Person".into(),
                    },
                ),
                untyped::IRExpr::Variable("person".into()),
            ),
            (
                IRExpr::AliasRef(
                    "score".into(),
                    ExprType::from_prop(&PropType::scalar(ScalarType::F32, false)),
                ),
                untyped::IRExpr::AliasRef("score".into()),
            ),
            (
                IRExpr::Nearest {
                    variable: "embedding_owner".into(),
                    property: "embedding".into(),
                    query: Box::new(query.clone()),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::F32,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::Nearest {
                    variable: "embedding_owner".into(),
                    property: "embedding".into(),
                    query: Box::new(expected_query.clone()),
                },
            ),
            (
                IRExpr::Search {
                    field: Box::new(field.clone()),
                    query: Box::new(query.clone()),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::Search {
                    field: Box::new(expected_field.clone()),
                    query: Box::new(expected_query.clone()),
                },
            ),
            (
                IRExpr::MatchText {
                    field: Box::new(field.clone()),
                    query: Box::new(query.clone()),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::MatchText {
                    field: Box::new(expected_field.clone()),
                    query: Box::new(expected_query.clone()),
                },
            ),
            (
                IRExpr::Bm25 {
                    field: Box::new(field.clone()),
                    query: Box::new(query.clone()),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::F32,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::Bm25 {
                    field: Box::new(expected_field.clone()),
                    query: Box::new(expected_query.clone()),
                },
            ),
            (
                IRExpr::logical_not(field.clone()),
                untyped::IRExpr::Not(Box::new(expected_field.clone())),
            ),
        ];
        for value in [None, Some(2)] {
            cases.push((
                IRExpr::Fuzzy {
                    field: Box::new(field.clone()),
                    query: Box::new(query.clone()),
                    max_edits: value.map(|n| {
                        Box::new(IRExpr::Literal(
                            Literal::Integer(n),
                            ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
                        ))
                    }),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::Bool,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::Fuzzy {
                    field: Box::new(expected_field.clone()),
                    query: Box::new(expected_query.clone()),
                    max_edits: value
                        .map(|n| Box::new(untyped::IRExpr::Literal(Literal::Integer(n)))),
                },
            ));
            cases.push((
                IRExpr::Rrf {
                    primary: Box::new(field.clone()),
                    secondary: Box::new(query.clone()),
                    k: value.map(|n| {
                        Box::new(IRExpr::Literal(
                            Literal::Integer(n),
                            ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
                        ))
                    }),
                    ty: crate::types::ExprType::Value {
                        scalar: crate::types::ScalarType::F64,
                        list: false,
                        nullable: false,
                    },
                },
                untyped::IRExpr::Rrf {
                    primary: Box::new(expected_field.clone()),
                    secondary: Box::new(expected_query.clone()),
                    k: value.map(|n| Box::new(untyped::IRExpr::Literal(Literal::Integer(n)))),
                },
            ));
        }
        for func in [
            AggFunc::Count,
            AggFunc::Sum,
            AggFunc::Avg,
            AggFunc::Min,
            AggFunc::Max,
        ] {
            cases.push((
                IRExpr::Aggregate {
                    func,
                    arg: Box::new(IRExpr::PropAccess {
                        variable: "document".into(),
                        property: "amount".into(),
                        ty: ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
                    }),
                    signature: crate::types::AggSignature {
                        arg: crate::types::ExprType::Value {
                            scalar: crate::types::ScalarType::I64,
                            list: false,
                            nullable: false,
                        },
                        result: crate::types::ExprType::Value {
                            scalar: if matches!(func, AggFunc::Sum | AggFunc::Avg) {
                                crate::types::ScalarType::F64
                            } else {
                                crate::types::ScalarType::I64
                            },
                            list: false,
                            nullable: true,
                        },
                    },
                },
                untyped::IRExpr::Aggregate {
                    func,
                    arg: Box::new(untyped::IRExpr::PropAccess {
                        variable: "document".into(),
                        property: "amount".into(),
                    }),
                },
            ));
        }
        for op in [BinaryOp::And, BinaryOp::Or, BinaryOp::Compare(CompOp::Ge)] {
            cases.push((
                IRExpr::binary(field.clone(), op, query.clone()),
                untyped::IRExpr::Binary {
                    left: Box::new(expected_field.clone()),
                    op,
                    right: Box::new(expected_query.clone()),
                },
            ));
        }
        for negated in [false, true] {
            cases.push((
                IRExpr::null_test(field.clone(), negated),
                untyped::IRExpr::IsNull {
                    expr: Box::new(expected_field.clone()),
                    negated,
                },
            ));
        }
        for literal in [
            Literal::Null,
            Literal::String("quoted\"\ntext".into()),
            Literal::Integer(i64::MIN),
            Literal::Float(-0.0),
            Literal::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
            Literal::Bool(true),
            Literal::Date("2026-10-05".into()),
            Literal::DateTime("2026-10-05T10:00:00Z".into()),
            Literal::List(vec![Literal::Integer(9_007_199_254_740_993), Literal::Null]),
        ] {
            cases.push((
                IRExpr::Literal(
                    literal.clone(),
                    ExprType::from_prop(&crate::query::typecheck::literal_type(&literal).unwrap()),
                ),
                untyped::IRExpr::Literal(literal),
            ));
        }
        for (source, expected) in cases {
            let actual = source.erase();
            assert_eq!(actual, expected, "{source:?}");
            assert_eq!(actual.to_string(), source.to_string());
            assert_eq!(
                actual
                    .comparison_parts()
                    .map(|(left, op, right)| { (left.to_string(), op, right.to_string()) }),
                source
                    .comparison_parts()
                    .map(|(left, op, right)| { (left.to_string(), op, right.to_string()) })
            );
        }
    }

    #[test]
    fn query_and_nested_pipeline_preserve_every_field() {
        let member = EdgeMember {
            edge_type: "Knows".into(),
            direction: Direction::In,
        };
        for (edges, max_hops, edge_binding, limit) in [
            (
                EdgeSelection::Named(member.clone()),
                Some(3),
                Some("edge".to_string()),
                Some(7),
            ),
            (
                EdgeSelection::Alternation(vec![member.clone()]),
                None,
                None,
                None,
            ),
            (
                EdgeSelection::Wildcard(vec![member]),
                Some(1),
                None,
                Some(0),
            ),
        ] {
            let source = QueryIR {
                name: "all_fields".into(),
                params: vec![IRParam {
                    declaration: Param {
                        name: "amount".into(),
                        type_name: "I32".into(),
                        nullable: true,
                    },
                    ty: ExprType::from_prop(&PropType::scalar(ScalarType::I32, true)),
                }],
                pipeline: vec![
                    IROp::NodeScan {
                        variable: "person".into(),
                        type_name: "Person".into(),
                        filters: vec![IRExpr::Literal(
                            Literal::Bool(true),
                            ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
                        )],
                    },
                    IROp::AntiJoin {
                        outer_var: "person".into(),
                        inner: vec![
                            IROp::Expand {
                                src_var: "person".into(),
                                dst_var: "friend".into(),
                                edges: edges.clone(),
                                src_type: "Person".into(),
                                dst_type: "Friend".into(),
                                min_hops: 1,
                                max_hops,
                                dst_filters: vec![IRExpr::Literal(
                                    Literal::Bool(false),
                                    ExprType::from_prop(&PropType::scalar(ScalarType::Bool, false)),
                                )],
                                edge_binding: edge_binding.clone(),
                            },
                            IROp::AntiJoin {
                                outer_var: "friend".into(),
                                inner: vec![IROp::Filter(IRExpr::null_test(
                                    IRExpr::Param(
                                        "amount".into(),
                                        ExprType::from_prop(&PropType::scalar(
                                            ScalarType::I32,
                                            true,
                                        )),
                                    ),
                                    true,
                                ))],
                                predicate: SubqueryPredicate::not_exists(),
                            },
                        ],
                        predicate: crate::ir::coerce::block(
                            BlockAggregateExpr::Aggregate {
                                func: AggFunc::Sum,
                                arg: Box::new(IRExpr::PropAccess {
                                    variable: "friend".into(),
                                    property: "age".into(),
                                    ty: ExprType::from_prop(&PropType::scalar(
                                        ScalarType::I32,
                                        true,
                                    )),
                                }),
                                signature: AggSignature {
                                    arg: ExprType::from_prop(&PropType::scalar(
                                        ScalarType::I32,
                                        true,
                                    )),
                                    result: ExprType::from_prop(&PropType::scalar(
                                        ScalarType::F64,
                                        true,
                                    )),
                                },
                            },
                            CompOp::Gt,
                            IRExpr::Param(
                                "amount".into(),
                                ExprType::from_prop(&PropType::scalar(ScalarType::I32, true)),
                            ),
                        )
                        .unwrap(),
                    },
                ],
                return_exprs: vec![
                    IRProjection {
                        expr: IRExpr::Variable(
                            "person".into(),
                            ExprType::Node {
                                type_name: "Person".into(),
                            },
                        ),
                        alias: None,
                        column: "person".into(),
                        ty: crate::types::ExprType::Node {
                            type_name: "Person".into(),
                        },
                    },
                    IRProjection {
                        expr: IRExpr::Param(
                            "amount".into(),
                            ExprType::from_prop(&PropType::scalar(ScalarType::I32, true)),
                        ),
                        alias: Some("answer".into()),
                        column: "answer".into(),
                        ty: crate::types::ExprType::from_prop(&crate::types::PropType::scalar(
                            crate::types::ScalarType::I32,
                            true,
                        )),
                    },
                ],
                order_by: vec![
                    IROrdering {
                        expr: IRExpr::AliasRef(
                            "answer".into(),
                            ExprType::from_prop(&PropType::scalar(ScalarType::I32, true)),
                        ),
                        descending: true,
                    },
                    IROrdering {
                        expr: IRExpr::Variable(
                            "person".into(),
                            ExprType::Node {
                                type_name: "Person".into(),
                            },
                        ),
                        descending: false,
                    },
                ],
                limit,
            };
            let expected = untyped::QueryIR {
                name: "all_fields".into(),
                params: vec![Param {
                    name: "amount".into(),
                    type_name: "I32".into(),
                    nullable: true,
                }],
                pipeline: vec![
                    untyped::IROp::NodeScan {
                        variable: "person".into(),
                        type_name: "Person".into(),
                        filters: vec![untyped::IRExpr::Literal(Literal::Bool(true))],
                    },
                    untyped::IROp::AntiJoin {
                        outer_var: "person".into(),
                        inner: vec![
                            untyped::IROp::Expand {
                                src_var: "person".into(),
                                dst_var: "friend".into(),
                                edges,
                                src_type: "Person".into(),
                                dst_type: "Friend".into(),
                                min_hops: 1,
                                max_hops,
                                dst_filters: vec![untyped::IRExpr::Literal(Literal::Bool(false))],
                                edge_binding,
                            },
                            untyped::IROp::AntiJoin {
                                outer_var: "friend".into(),
                                inner: vec![untyped::IROp::Filter(untyped::IRExpr::IsNull {
                                    expr: Box::new(untyped::IRExpr::Param("amount".into())),
                                    negated: true,
                                })],
                                predicate: untyped::SubqueryPredicate {
                                    func: AggFunc::Count,
                                    arg: None,
                                    op: CompOp::Eq,
                                    right: untyped::IRExpr::Literal(Literal::Integer(0)),
                                },
                            },
                        ],
                        predicate: untyped::SubqueryPredicate {
                            func: AggFunc::Sum,
                            arg: Some(untyped::IRExpr::PropAccess {
                                variable: "friend".into(),
                                property: "age".into(),
                            }),
                            op: CompOp::Gt,
                            right: untyped::IRExpr::Param("amount".into()),
                        },
                    },
                ],
                return_exprs: vec![
                    untyped::IRProjection {
                        expr: untyped::IRExpr::Variable("person".into()),
                        alias: None,
                    },
                    untyped::IRProjection {
                        expr: untyped::IRExpr::Param("amount".into()),
                        alias: Some("answer".into()),
                    },
                ],
                order_by: vec![
                    untyped::IROrdering {
                        expr: untyped::IRExpr::AliasRef("answer".into()),
                        descending: true,
                    },
                    untyped::IROrdering {
                        expr: untyped::IRExpr::Variable("person".into()),
                        descending: false,
                    },
                ],
                limit,
            };
            let actual = source.erase();
            assert_eq!(format!("{actual:#?}"), format!("{expected:#?}"));
            assert_eq!(actual.return_exprs, expected.return_exprs);
            assert_eq!(actual.order_by, expected.order_by);
            assert_eq!(actual.has_edge_selections(), source.has_edge_selections());
        }
    }

    #[test]
    fn block_casts_erase_without_changing_the_frozen_aggregate_owner() {
        let arg_type = ExprType::from_prop(&PropType::scalar(ScalarType::I64, true));
        let predicate = crate::ir::coerce::block(
            BlockAggregateExpr::Aggregate {
                func: AggFunc::Max,
                arg: Box::new(IRExpr::PropAccess {
                    variable: "child".into(),
                    property: "amount".into(),
                    ty: arg_type.clone(),
                }),
                signature: AggSignature {
                    arg: arg_type.clone(),
                    result: arg_type,
                },
            },
            CompOp::Gt,
            IRExpr::Param(
                "bound".into(),
                ExprType::from_prop(&PropType::scalar(ScalarType::F64, false)),
            ),
        )
        .unwrap();
        assert!(matches!(predicate.left, BlockAggregateExpr::Cast { .. }));
        let erased = predicate.erase();
        assert_eq!(
            erased,
            untyped::SubqueryPredicate {
                func: AggFunc::Max,
                arg: Some(untyped::IRExpr::PropAccess {
                    variable: "child".into(),
                    property: "amount".into()
                }),
                op: CompOp::Gt,
                right: untyped::IRExpr::Param("bound".into()),
            }
        );
        assert_eq!(predicate.to_string(), erased.to_string());
    }

    #[test]
    fn subquery_predicate_helpers_preserve_the_reference_gate() {
        for op in [
            CompOp::Eq,
            CompOp::Ne,
            CompOp::Gt,
            CompOp::Ge,
            CompOp::Lt,
            CompOp::Le,
        ] {
            for bound in [-1, 0, 1, 2] {
                let source = SubqueryPredicate {
                    left: BlockAggregateExpr::count_rows(),
                    op,
                    right: IRExpr::Literal(
                        Literal::Integer(bound),
                        ExprType::from_prop(&PropType::scalar(ScalarType::I64, false)),
                    ),
                };
                let actual = source.erase();
                assert_eq!(actual.is_not_exists(), source.is_not_exists());
                assert_eq!(actual.to_string(), source.to_string());
            }
        }
    }
}

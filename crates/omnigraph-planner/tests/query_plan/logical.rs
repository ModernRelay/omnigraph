use super::*;
use omnigraph_compiler::ir::expr::{ScalarFunc, TermsMode};
use omnigraph_planner::read_shape::{ArmRank, FuseArm};

/// Synthetic IR coverage: the executor currently rejects a secondary nearest
/// ordering, so a real-source GQT case cannot exercise this demand rule.
#[test]
fn nearest_in_a_later_ordering_position_reads_the_vector_column() {
    let op = ir(
        vec![scan("c")],
        vec![prop("c", "slug")],
        vec![
            prop("c", "rank"),
            IRExpr::Nearest {
                variable: "c".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Param(
                    "q".to_string(),
                    value_type(ScalarType::String, false),
                )),
                ty: value_type(ScalarType::F32, false),
            },
        ],
    );
    let (plan, _) = planned(&op);
    assert_eq!(
        projection_of(&plan, "c"),
        set(&["__id", "slug", "rank", "embedding"])
    );
}

/// Synthetic IR coverage: secondary nearest ordering cannot execute in GQT;
/// the entity demand still must retain an explicitly referenced vector.
#[test]
fn entity_reference_keeps_a_later_nearest_vector_column_demanded_by_name() {
    let op = ir(
        vec![scan("c")],
        vec![IRExpr::Variable(
            "c".to_string(),
            omnigraph_compiler::ExprType::Node {
                type_name: "T".into(),
            },
        )],
        vec![
            prop("c", "rank"),
            IRExpr::Nearest {
                variable: "c".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Param(
                    "q".to_string(),
                    value_type(ScalarType::String, false),
                )),
                ty: value_type(ScalarType::F32, false),
            },
        ],
    );
    let (plan, _) = planned(&op);
    let mut expected = object_columns();
    expected.insert("embedding".to_string());
    assert_eq!(projection_of(&plan, "c"), expected);
}

/// GQT cannot rerun rewrite on the same logical arena to check convergence.
#[test]
fn metadata_count_rewrite_converges() {
    let op = ir(
        vec![scan("c")],
        vec![IRExpr::Aggregate {
            fraction: None,
            func: AggFunc::Count,
            distinct: false,
            arg: Box::new(IRExpr::Variable(
                "c".to_string(),
                omnigraph_compiler::ExprType::Node {
                    type_name: "T".into(),
                },
            )),
            signature: AggSignature {
                arg: ExprType::Node {
                    type_name: "T".into(),
                },
                result: ExprType::from_prop(&PropType::scalar(ScalarType::I64, true)),
            },
        }],
        vec![],
    );
    let (mut plan, fired) = planned(&op);
    assert!(fired.contains(&"aggregate_pushdown"));
    assert!(
        plan.live()
            .any(|(_, node)| matches!(node, LogicalNode::MetadataCount { .. }))
    );
    assert!(!plan.live().any(|(_, node)| matches!(
        node,
        LogicalNode::TableScan { .. } | LogicalNode::Aggregate { .. }
    )));
    assert!(
        !rewrite(&mut plan, &source())
            .expect("rewrite converges")
            .contains(&"aggregate_pushdown")
    );
}

#[test]
fn fusion_targets_read_the_node_object_in_every_arm() {
    // Each arm ranks `a` over its own population; every arm's scan of the
    // target reads the node object the fusion carries from the first arm
    // that selected a target.
    let nearest = FuseArm {
        steps: vec![
            scan("a"),
            PatternStep::Rank {
                key: IRExpr::Nearest {
                    variable: "a".to_string(),
                    property: "embedding".to_string(),
                    query: Box::new(IRExpr::Param(
                        "q".to_string(),
                        value_type(ScalarType::String, false),
                    )),
                    ty: value_type(ScalarType::F32, false),
                },
                binding: "a".to_string(),
                window: Some(3),
                window_param: None,
                recall: None,
            },
        ],
        rank: ArmRank::Nearest,
    };
    let mut op = ir(nearest.steps.clone(), vec![prop("a", "x")], vec![]);
    let Operation::Query(shape) = &mut op else {
        unreachable!("a read")
    };
    shape.pipeline = vec![PatternStep::Fuse {
        arms: vec![nearest.clone(), nearest],
        target: "a".to_string(),
        target_type: "T".to_string(),
        k: None,
        weights: None,
    }];
    let (plan, _) = planned(&op);
    assert_eq!(projection_of(&plan, "a"), object_columns());
}

#[test]
fn sibling_negations_keep_destination_scan_filters_in_their_scopes() {
    let negation = |value: &str| PatternStep::AntiJoin {
        fails: false,
        outer_var: "a".to_string(),
        predicate: omnigraph_compiler::ir::SubqueryPredicate::not_exists(),
        inner: vec![
            expand("a", "x"),
            PatternStep::Filter {
                pred: IRExpr::comparison(
                    prop("x", "state"),
                    CompOp::Eq,
                    IRExpr::Literal(
                        Literal::String(value.to_string()),
                        value_type(ScalarType::String, false),
                    ),
                ),
                fails: false,
            },
        ],
    };
    let (plan, _) = planned(&ir(
        vec![scan("a"), negation("open"), negation("closed")],
        vec![prop("a", "slug")],
        vec![],
    ));
    let mut values = BTreeSet::new();
    for (_, node) in plan.live() {
        if let LogicalNode::TableScan {
            input: Some(expansion),
            spec,
        } = node
        {
            let filters = spec.filter.as_ref().expect("scoped predicate").gq_filters();
            assert_eq!(filters.len(), 1);
            let Some((_, _, IRExpr::Literal(Literal::String(value), _))) =
                filters[0].comparison_parts()
            else {
                panic!("literal state")
            };
            values.insert(value.clone());
            let Some(LogicalNode::Expand { input, .. }) = plan.node(*expansion) else {
                panic!("inner expansion")
            };
            assert!(matches!(
                plan.node(*input),
                Some(LogicalNode::OuterReference { .. })
            ));
        }
    }
    assert_eq!(values, set(&["open", "closed"]));
}

/// A lexical predicate whose query argument reads another binding is no scan
/// predicate of its field's binding: it stays where the query wrote it.
#[test]
fn a_lexical_filter_reading_a_second_binding_is_not_placed_on_a_root_scan() {
    let (plan, _) = planned(&ir(
        vec![
            scan("a"),
            scan("b"),
            PatternStep::Filter {
                pred: IRExpr::Call {
                    func: ScalarFunc::MatchTerms {
                        mode: TermsMode::Any,
                    },
                    args: vec![
                        prop("b", "text"),
                        prop("a", "probe"),
                        IRExpr::Literal(Literal::Integer(0), value_type(ScalarType::I64, false)),
                    ],
                    ty: value_type(ScalarType::Bool, false),
                },
                fails: false,
            },
        ],
        vec![prop("b", "slug")],
        vec![],
    ));
    for (_, node) in plan.live() {
        if let LogicalNode::TableScan { spec, .. } = node {
            assert!(spec.filter.is_none(), "{:?}", spec.binding);
        }
    }
    assert!(
        plan.live()
            .any(|(_, node)| matches!(node, LogicalNode::Filter { .. }))
    );
}

/// GQT cannot substitute a scanner that refuses every scalar filter.
#[test]
fn rejected_scalar_filters_stay_above_root_and_dependent_scans() {
    struct RefusingSource(MemorySource);
    impl PlanSource for RefusingSource {
        fn schema(&self, side: SideId) -> Result<SchemaRef, PlanError> {
            self.0.schema(side)
        }
        fn fragments(&self, side: SideId) -> Vec<FragmentStat> {
            self.0.fragments(side)
        }
        fn adjacency_proof(&self) -> Option<&AdjacencyProof> {
            self.0.adjacency_proof()
        }
        fn node_type(&self, name: &str) -> Result<NodeTypeSpec, PlanError> {
            self.0.node_type(name)
        }
        fn filter_pushable(&self, _: &IRExpr) -> bool {
            false
        }
        fn edge_dataset(&self, _: &str) -> Option<omnigraph_planner::DatasetPin> {
            None
        }
    }
    for dependent in [false, true] {
        let binding = if dependent { "b" } else { "a" };
        let filter = IRExpr::comparison(
            prop(binding, "state"),
            CompOp::Eq,
            IRExpr::Literal(
                Literal::String("open".into()),
                value_type(ScalarType::String, false),
            ),
        );
        let mut pipeline = if dependent {
            vec![scan("a"), expand("a", "b")]
        } else {
            vec![scan("a")]
        };
        pipeline.push(PatternStep::Filter {
            pred: filter,
            fails: false,
        });
        let op = ir(pipeline, vec![prop(binding, "slug")], vec![]);
        let source = RefusingSource(source());
        let mut plan = resolve(&op, &source).expect("query resolves");
        rewrite(&mut plan, &source).expect("rewrite");
        assert!(
            plan.live()
                .any(|(_, node)| matches!(node, LogicalNode::Filter { .. }))
        );
        for (_, node) in plan.live() {
            if let LogicalNode::TableScan { spec, .. } = node {
                assert!(spec.filter.is_none());
            }
        }
        // The `limit` cuts the tie rule's order, which reads the id.
        let expected: &[&str] = &["__id", "slug", "state"];
        assert_eq!(projection_of(&plan, binding), set(expected));
    }
}

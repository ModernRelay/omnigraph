use super::*;

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
                query: Box::new(IRExpr::Param("q".to_string())),
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
        vec![IRExpr::Variable("c".to_string())],
        vec![
            prop("c", "rank"),
            IRExpr::Nearest {
                variable: "c".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Param("q".to_string())),
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
            func: AggFunc::Count,
            arg: Box::new(IRExpr::Variable("c".to_string())),
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
fn search_expression_arms_attribute_field_and_nested_columns() {
    let op = ir(
        vec![scan("c")],
        vec![prop("c", "slug")],
        vec![IRExpr::Rrf {
            primary: Box::new(IRExpr::Fuzzy {
                field: Box::new(prop("c", "title")),
                query: Box::new(prop("c", "probe")),
                max_edits: Some(Box::new(prop("c", "edits"))),
            }),
            secondary: Box::new(IRExpr::Bm25 {
                field: Box::new(prop("c", "body")),
                query: Box::new(IRExpr::Literal(Literal::String("q".into()))),
            }),
            k: Some(Box::new(prop("c", "k_ref"))),
        }],
    );
    let (plan, _) = planned(&op);
    assert_eq!(
        projection_of(&plan, "c"),
        set(&["__id", "slug", "title", "probe", "edits", "body", "k_ref"])
    );
}

#[test]
fn rank_fuse_targets_read_the_node_object_in_both_arms() {
    let op = ir(
        vec![scan("a"), scan("b")],
        vec![prop("a", "x")],
        vec![IRExpr::Rrf {
            primary: Box::new(IRExpr::Nearest {
                variable: "a".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Param("q".to_string())),
            }),
            secondary: Box::new(IRExpr::Bm25 {
                field: Box::new(prop("b", "text")),
                query: Box::new(IRExpr::Literal(Literal::String("q".into()))),
            }),
            k: None,
        }],
    );
    let (plan, _) = planned(&op);
    assert_eq!(projection_of(&plan, "a"), object_columns());
    assert_eq!(projection_of(&plan, "b"), object_columns());
}

#[test]
fn dependent_scan_pushes_exact_search_membership_but_keeps_fuzzy_and_correlations() {
    let query = Box::new(IRExpr::Param("q".to_string()));
    let expressions = [
        (
            IRExpr::Search {
                field: Box::new(prop("b", "text")),
                query: query.clone(),
            },
            true,
        ),
        (
            IRExpr::MatchText {
                field: Box::new(prop("b", "text")),
                query: query.clone(),
            },
            true,
        ),
        (
            IRExpr::Fuzzy {
                field: Box::new(prop("b", "text")),
                query,
                max_edits: None,
            },
            false,
        ),
        (
            IRExpr::Search {
                field: Box::new(prop("b", "text")),
                query: Box::new(prop("a", "probe")),
            },
            false,
        ),
    ];
    for (expression, pushed) in expressions {
        for embedded in [false, true] {
            let filter = IRExpr::comparison(
                expression.clone(),
                CompOp::Eq,
                IRExpr::Literal(Literal::Bool(true)),
            );
            let mut pipeline = vec![
                scan("a"),
                expand(
                    "a",
                    "b",
                    if embedded {
                        vec![filter.clone()]
                    } else {
                        vec![]
                    },
                ),
            ];
            if !embedded {
                pipeline.push(IROp::Filter(filter));
            }
            let (plan, _) = planned(&ir(pipeline, vec![prop("b", "slug")], vec![]));
            let destination = plan
                .live()
                .find_map(|(_, node)| match node {
                    LogicalNode::TableScan {
                        input: Some(_),
                        spec,
                    } => Some(spec),
                    _ => None,
                })
                .expect("a dependent scan");
            assert_eq!(
                destination.filter.is_some(),
                pushed,
                "{expression:?}, embedded={embedded}"
            );
            assert_eq!(
                plan.live()
                    .any(|(_, node)| matches!(node, LogicalNode::Filter { .. })),
                !pushed
            );
            assert_eq!(projection_of(&plan, "b"), set(&["__id", "slug", "text"]));
        }
    }
}

#[test]
fn sibling_negations_keep_destination_scan_filters_in_their_scopes() {
    let negation = |value: &str| IROp::AntiJoin {
        outer_var: "a".to_string(),
        predicate: omnigraph_compiler::ir::SubqueryPredicate::not_exists(),
        inner: vec![
            expand("a", "x", vec![]),
            IROp::Filter(IRExpr::comparison(
                prop("x", "state"),
                CompOp::Eq,
                IRExpr::Literal(Literal::String(value.to_string())),
            )),
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
            let Some((_, _, IRExpr::Literal(Literal::String(value)))) =
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

/// A search filter whose query argument reads another binding is no scan
/// predicate of its field's binding: it stays where the query wrote it.
#[test]
fn a_search_filter_reading_a_second_binding_is_not_placed_on_a_root_scan() {
    let (plan, _) = planned(&ir(
        vec![
            scan("a"),
            scan("b"),
            IROp::Filter(IRExpr::comparison(
                IRExpr::Search {
                    field: Box::new(prop("b", "text")),
                    query: Box::new(prop("a", "probe")),
                },
                CompOp::Eq,
                IRExpr::Literal(Literal::Bool(true)),
            )),
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
        for embedded in [false, true] {
            if !dependent && embedded {
                continue;
            }
            let binding = if dependent { "b" } else { "a" };
            let filter = IRExpr::comparison(
                prop(binding, "state"),
                CompOp::Eq,
                IRExpr::Literal(Literal::String("open".into())),
            );
            let mut pipeline = if dependent {
                vec![
                    scan("a"),
                    expand(
                        "a",
                        "b",
                        if embedded {
                            vec![filter.clone()]
                        } else {
                            vec![]
                        },
                    ),
                ]
            } else {
                vec![IROp::NodeScan {
                    variable: "a".into(),
                    type_name: "T".into(),
                    filters: if embedded {
                        vec![filter.clone()]
                    } else {
                        vec![]
                    },
                }]
            };
            if !embedded {
                pipeline.push(IROp::Filter(filter));
            }
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
            let expected: &[&str] = if dependent {
                &["__id", "slug", "state"]
            } else {
                &["slug", "state"]
            };
            assert_eq!(projection_of(&plan, binding), set(expected));
        }
    }
}

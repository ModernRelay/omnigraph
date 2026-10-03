//! Selector admission, pinned members and replay metadata are planner contracts
//! that result-only GQT assertions cannot observe.

use super::*;
use omnigraph_compiler::query::ast::AggFunc;
use omnigraph_compiler::traversal::EDGE_TYPE_COLUMN;
use omnigraph_planner::ColumnRef;
use omnigraph_planner::logical::tiebreak_text;
use omnigraph_planner::mirror::EdgeSelectionMirror;
use omnigraph_planner::optimizer::validate_rank_fuse_row_tiebreaks;
use omnigraph_planner::physical::Assumptions;

/// A separate worker bounds synchronous planning; an async query timeout
/// cannot interrupt accidental iteration to u32::MAX before the future yields.
#[test]
fn issue_659_large_finite_bounds_stop_estimation_at_a_fixed_point() {
    let (completed, completion) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        for (rows, edges, hops, expected) in [
            (3, 10, 1, 6),
            (3, 10, 2, 12),
            (3, 10, 3, 16),
            (3, 10, u32::MAX, 16),
            (3, 5, u32::MAX, 3),
            (0, 10, u32::MAX, 0),
            (20, 5, u32::MAX, 16),
        ] {
            let source = source_with_rows(Some(rows))
                .with_traversal_work_limit(100)
                .with_expand_statistics(
                    "knows",
                    Direction::Out,
                    ExpandStatistics {
                        edge_count: edges,
                        src_node_count: 5,
                        dst_node_count: 16,
                        same_type: true,
                        max_frontier_cap: 100,
                        max_hops_cap: 6,
                    },
                );
            let mut named = expand("a", "b", vec![]);
            let IROp::Expand { max_hops, .. } = &mut named else {
                unreachable!()
            };
            *max_hops = Some(hops);
            let op = ir(
                vec![scan("a"), named, selected("b", "c", alternatives())],
                vec![prop("c", "slug")],
                vec![],
            );
            let logical = resolve(&op, &source).unwrap();
            let (id, _) = logical
            .live()
            .find(|(_, node)| matches!(node, LogicalNode::Expand { edges, .. } if edges.named().is_some()))
            .unwrap();
            assert_eq!(
                omnigraph_planner::estimate_rows(&logical, id, &source),
                Some(expected)
            );
            let (plan, _) = physical(&op, &source);
            let policies: Vec<_> = plan
                .live()
                .filter_map(|(_, node)| match node {
                    PhysicalNode::Expand { policy, mode, .. } => Some((policy, mode)),
                    _ => None,
                })
                .collect();
            assert_eq!(policies.len(), 2);
            assert!(policies.iter().all(|(policy, mode)| {
                **policy == ExpandPolicy::Budgeted && **mode == ExpandMode::IndexedScan
            }));
        }
        completed.send(()).unwrap();
    });
    completion
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("huge-bound planning must finish within ten seconds");
    worker.join().unwrap();
}

#[test]
fn issue_659_rrf_declares_typed_downstream_order_without_changing_fusion_identity() {
    for (selection, expected) in [
        (None, vec!["$b.@id"]),
        (
            Some(EdgeSelection::Named(member("knows", Direction::Out))),
            vec!["$b.@id", "$e.@id"],
        ),
        (Some(alternatives()), vec!["$b.@id", "$e.@type", "$e.@id"]),
        (
            Some(EdgeSelection::Wildcard(alternatives().members().to_vec())),
            vec!["$b.@id", "$e.@type", "$e.@id"],
        ),
        (
            Some(EdgeSelection::Wildcard(vec![])),
            vec!["$b.@id", "$e.@type", "$e.@id"],
        ),
    ] {
        let mut source = source();
        let downstream = if let Some(selection) = selection {
            if selection.named().is_none() {
                source = source.with_traversal_work_limit(100);
            }
            let mut expansion = selected("a", "b", selection);
            let IROp::Expand { edge_binding, .. } = &mut expansion else {
                unreachable!()
            };
            *edge_binding = Some("e".into());
            expansion
        } else {
            scan("b")
        };
        let arm = IRExpr::Bm25 {
            field: Box::new(prop("a", "text")),
            query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
        };
        let op = ir(
            vec![scan("a"), downstream],
            vec![prop("b", "slug")],
            vec![IRExpr::Rrf {
                primary: Box::new(arm.clone()),
                secondary: Box::new(arm),
                k: None,
            }],
        );
        let logical = resolve(&op, &source).unwrap();
        let logical_keys = logical
            .live()
            .find_map(|(_, node)| match node {
                LogicalNode::RankFuse { row_tiebreak, .. } => Some(row_tiebreak),
                _ => None,
            })
            .unwrap();
        assert_eq!(tiebreak_text(logical_keys), expected);
        let (plan, _) = physical(&op, &source);
        validate_rank_fuse_row_tiebreaks(&plan).unwrap();
        let (arms, keys) = plan
            .live()
            .find_map(|(_, node)| match node {
                PhysicalNode::RankFuse {
                    arms, row_tiebreak, ..
                } => Some((arms, row_tiebreak)),
                _ => None,
            })
            .unwrap();
        assert!(arms.iter().all(|arm| arm.binding == "a"));
        assert_eq!(tiebreak_text(keys), expected);
        assert!(
            !plan
                .live()
                .any(|(_, node)| matches!(node, PhysicalNode::Sort { .. }))
        );
        let bound = omnigraph_planner::BoundPlan {
            plan,
            values: Default::default(),
        };
        let encoded = serde_json::to_string(&bound).unwrap();
        let restored: omnigraph_planner::BoundPlan = serde_json::from_str(&encoded).unwrap();
        assert_eq!(restored, bound);
        validate_rank_fuse_row_tiebreaks(&restored.plan).unwrap();
    }
}

#[test]
fn rank_fuse_row_keys_cover_both_arms_without_correlated_locals_issue_659() {
    let mut query = query(correlated(alternatives()));
    query.pipeline[1] = scan("b");
    let IROp::AntiJoin { inner, .. } = &mut query.pipeline[2] else {
        panic!("the fixture ends with a correlated block");
    };
    let IROp::Expand { edge_binding, .. } = &mut inner[0] else {
        panic!("the correlated block binds a selected edge");
    };
    *edge_binding = Some("inner_edge".into());
    let arm = |binding| IRExpr::Bm25 {
        field: Box::new(prop(binding, "text")),
        query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
    };
    query.order_by = vec![IROrdering {
        expr: IRExpr::Rrf {
            primary: Box::new(arm("a")),
            secondary: Box::new(arm("b")),
            k: None,
        },
        descending: false,
    }];
    let source = source().with_traversal_work_limit(100);
    let mut plan = omnigraph_planner::plan_query(&query, &source, &bounds()).unwrap();
    let (fuse_id, secondary_input) = plan
        .live()
        .find_map(|(id, node)| match node {
            PhysicalNode::RankFuse {
                arms, row_tiebreak, ..
            } => {
                assert_eq!(arms[0].binding, "a");
                assert_eq!(arms[1].binding, "b");
                assert_eq!(tiebreak_text(row_tiebreak), ["$b.@id"]);
                Some((id, arms[1].input))
            }
            _ => None,
        })
        .unwrap();
    validate_rank_fuse_row_tiebreaks(&plan).unwrap();
    let mut extra = plan
        .live()
        .find_map(|(_, node)| matches!(node, PhysicalNode::Expand { .. }).then(|| node.clone()))
        .unwrap();
    let PhysicalNode::Expand {
        input,
        dst,
        edge_binding,
        ..
    } = &mut extra
    else {
        unreachable!()
    };
    *input = secondary_input;
    *dst = "extra".into();
    *edge_binding = None;
    let extra = plan.add(extra);
    let Some(PhysicalNode::RankFuse { arms, .. }) = plan.node_mut(fuse_id) else {
        unreachable!()
    };
    arms[1].input = extra;
    let error = validate_rank_fuse_row_tiebreaks(&plan).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("arm 1") && message.contains("$extra.@id"),
        "{message}"
    );
}

#[test]
fn every_nested_rank_fuse_validates_its_row_keys_issue_659() {
    let arm = IRExpr::Bm25 {
        field: Box::new(prop("a", "text")),
        query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
    };
    let op = ir(
        vec![scan("a"), scan("b")],
        vec![prop("b", "slug")],
        vec![IRExpr::Rrf {
            primary: Box::new(arm.clone()),
            secondary: Box::new(arm),
            k: None,
        }],
    );
    let (mut plan, _) = physical(&op, &source());
    let (inner_id, mut outer) = plan
        .live()
        .find_map(|(id, node)| {
            matches!(node, PhysicalNode::RankFuse { .. }).then(|| (id, node.clone()))
        })
        .unwrap();
    let PhysicalNode::RankFuse { arms, .. } = &mut outer else {
        unreachable!()
    };
    arms[0].input = inner_id;
    let outer_id = plan.add(outer);
    plan.set_root(outer_id);
    validate_rank_fuse_row_tiebreaks(&plan).unwrap();
    for invalid in [inner_id, outer_id] {
        let mut altered = plan.clone();
        let Some(PhysicalNode::RankFuse { row_tiebreak, .. }) = altered.node_mut(invalid) else {
            unreachable!()
        };
        row_tiebreak.clear();
        let error = validate_rank_fuse_row_tiebreaks(&altered).unwrap_err();
        assert!(
            error.to_string().contains(&format!("rank fuse {invalid} ")),
            "{error}"
        );
    }
}

#[test]
fn rank_fuse_without_downstream_bindings_keeps_an_empty_key_list_issue_659() {
    let arm = IRExpr::Bm25 {
        field: Box::new(prop("a", "text")),
        query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
    };
    let op = ir(
        vec![scan("a")],
        vec![prop("a", "slug")],
        vec![IRExpr::Rrf {
            primary: Box::new(arm.clone()),
            secondary: Box::new(arm),
            k: None,
        }],
    );
    let (mut plan, _) = physical(&op, &source());
    let fuse_id = plan
        .live()
        .find_map(|(id, node)| match node {
            PhysicalNode::RankFuse { row_tiebreak, .. } => {
                assert!(row_tiebreak.is_empty());
                Some(id)
            }
            _ => None,
        })
        .unwrap();
    validate_rank_fuse_row_tiebreaks(&plan).unwrap();
    let Some(PhysicalNode::RankFuse { arms, .. }) = plan.node_mut(fuse_id) else {
        unreachable!()
    };
    arms[1].input = usize::MAX;
    let error = validate_rank_fuse_row_tiebreaks(&plan).unwrap_err();
    assert!(error.to_string().contains("arm root"), "{error}");
}

#[test]
fn issue_659_selected_edge_sort_declares_type_even_when_id_is_ordered() {
    for selection in [
        alternatives(),
        EdgeSelection::Wildcard(alternatives().members().to_vec()),
    ] {
        for (order, edge_keys) in [
            (vec![prop("a", "rank")], vec!["@type", "@id"]),
            (vec![prop("e", SYSTEM_COLUMNS_V3.id)], vec!["@type"]),
            (vec![IRExpr::AliasRef("edge_id".into())], vec!["@type"]),
            (vec![prop("e", EDGE_TYPE_COLUMN)], vec!["@id"]),
            (vec![IRExpr::AliasRef("edge_type".into())], vec!["@id"]),
            (
                vec![
                    IRExpr::AliasRef("edge_type".into()),
                    IRExpr::AliasRef("edge_id".into()),
                ],
                vec![],
            ),
        ] {
            let mut expansion = selected("a", "b", selection.clone());
            let IROp::Expand { edge_binding, .. } = &mut expansion else {
                unreachable!()
            };
            *edge_binding = Some("e".into());
            let mut query = query(ir(
                vec![scan("a"), expansion],
                vec![
                    prop("a", "slug"),
                    prop("e", SYSTEM_COLUMNS_V3.id),
                    prop("e", EDGE_TYPE_COLUMN),
                ],
                order,
            ));
            query.return_exprs[1].alias = Some("edge_id".into());
            query.return_exprs[2].alias = Some("edge_type".into());
            let source = source().with_traversal_work_limit(100);
            let op = Operation::Query(Box::new(query));
            let (plan, _) = physical(&op, &source);
            let keys = plan
                .live()
                .find_map(|(_, node)| match node {
                    PhysicalNode::Sort { tiebreak, .. } => Some(tiebreak),
                    _ => None,
                })
                .unwrap();
            let mut expected = vec![
                ColumnRef::property("a", "@id"),
                ColumnRef::property("b", "@id"),
            ];
            expected.extend(
                edge_keys
                    .into_iter()
                    .map(|property| ColumnRef::property("e", property)),
            );
            assert_eq!(keys, &expected);
            assert_eq!(
                tiebreak_text(keys),
                expected
                    .iter()
                    .map(|key| format!("${key}"))
                    .collect::<Vec<_>>()
            );
            let bound = omnigraph_planner::BoundPlan {
                plan,
                values: Default::default(),
            };
            let encoded = serde_json::to_string(&bound).unwrap();
            let restored: omnigraph_planner::BoundPlan = serde_json::from_str(&encoded).unwrap();
            assert_eq!(restored, bound);
        }
    }
}

#[test]
fn literal_order_keys_keep_the_sort_and_remaining_identity_order() {
    let literal = IRExpr::Literal(Literal::String("Knows".into()));
    for (returns, order, expected_keys, expected_tiebreak) in [
        (
            vec![prop("a", "slug")],
            vec![literal.clone()],
            vec![],
            vec!["$a.@id"],
        ),
        (
            vec![prop("a", "slug")],
            vec![literal.clone(), prop("a", "rank")],
            vec![prop("a", "rank")],
            vec!["$a.@id"],
        ),
        (vec![literal.clone()], vec![literal], vec![], vec![]),
    ] {
        let op = ir(vec![scan("a")], returns, order);
        let (plan, _) = physical(&op, &source());
        let (order_by, fetch, tiebreak) = plan
            .live()
            .find_map(|(_, node)| match node {
                PhysicalNode::Sort {
                    order_by,
                    fetch,
                    tiebreak,
                    ..
                } => Some((order_by, fetch, tiebreak)),
                _ => None,
            })
            .expect("a constant-only ORDER still declares its sort");
        assert_eq!(
            order_by
                .iter()
                .map(|key| key.expr.clone())
                .collect::<Vec<_>>(),
            expected_keys
        );
        assert_eq!(*fetch, Some(10));
        assert_eq!(tiebreak_text(tiebreak), expected_tiebreak);
    }
}

#[test]
fn fused_row_identity_survives_eliminated_sort() {
    let ranked = || {
        let arm = IRExpr::Bm25 {
            field: Box::new(prop("a", "text")),
            query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
        };
        IRExpr::Rrf {
            primary: Box::new(arm.clone()),
            secondary: Box::new(arm),
            k: None,
        }
    };
    for (returns, order, expected) in [
        (vec![prop("b", "slug")], vec![ranked()], vec!["$b.@id"]),
        (
            vec![prop("b", "slug")],
            vec![ranked(), prop("b", SYSTEM_COLUMNS_V3.id)],
            vec!["$b.@id"],
        ),
        (
            vec![prop("b", "slug")],
            vec![ranked(), prop("b", "slug")],
            vec!["$b.@id"],
        ),
        (
            vec![IRExpr::Aggregate {
                func: AggFunc::Count,
                arg: Box::new(IRExpr::Variable("b".into())),
            }],
            vec![ranked()],
            vec!["$b.@id"],
        ),
    ] {
        let op = ir(vec![scan("a"), scan("b")], returns, order);
        let logical = resolve(&op, &source()).unwrap();
        let keys = logical
            .live()
            .find_map(|(_, node)| match node {
                LogicalNode::RankFuse { row_tiebreak, .. } => Some(row_tiebreak),
                _ => None,
            })
            .unwrap();
        assert_eq!(tiebreak_text(keys), expected);
        let (plan, _) = physical(&op, &source());
        let keys = plan
            .live()
            .find_map(|(_, node)| match node {
                PhysicalNode::RankFuse { row_tiebreak, .. } => Some(row_tiebreak),
                _ => None,
            })
            .unwrap();
        assert_eq!(tiebreak_text(keys), expected);
        assert!(
            !plan
                .live()
                .any(|(_, node)| matches!(node, PhysicalNode::Sort { .. }))
        );
    }
}

fn member(name: &str, direction: Direction) -> EdgeMember {
    EdgeMember {
        edge_type: name.to_string(),
        direction,
    }
}

fn selected(src: &str, dst: &str, edges: EdgeSelection) -> IROp {
    let mut op = expand(src, dst, vec![]);
    let IROp::Expand { edges: value, .. } = &mut op else {
        unreachable!()
    };
    *value = edges;
    op
}

fn query(op: Operation) -> QueryIR {
    let Operation::Query(query) = op else {
        unreachable!()
    };
    *query
}

fn alternatives() -> EdgeSelection {
    EdgeSelection::Alternation(vec![
        member("knows", Direction::Out),
        member("likes", Direction::In),
    ])
}

fn correlated(edges: EdgeSelection) -> Operation {
    ir(
        vec![
            scan("a"),
            expand("a", "b", vec![]),
            IROp::AntiJoin {
                outer_var: "b".to_string(),
                inner: vec![selected("b", "x", edges)],
                predicate: omnigraph_compiler::ir::SubqueryPredicate {
                    func: AggFunc::Count,
                    arg: None,
                    op: CompOp::Eq,
                    right: IRExpr::Literal(Literal::Integer(0)),
                },
            },
        ],
        vec![prop("b", "slug")],
        vec![],
    )
}

#[test]
fn issue_659_nested_selection_budgets_every_expand_and_pins_every_member() {
    let edges = EdgeSelection::Wildcard(vec![
        member("absent", Direction::Both),
        member("knows", Direction::Out),
        member("likes", Direction::In),
    ]);
    let query = query(correlated(edges.clone()));
    let source = source()
        .with_edge_version("knows", 5)
        .with_edge_version("likes", 9)
        .with_traversal_work_limit(123);
    let plan = omnigraph_planner::plan_query(&query, &source, &bounds()).unwrap();
    let expands: Vec<_> = plan
        .live()
        .filter_map(|(_, node)| match node {
            PhysicalNode::Expand {
                edges,
                src_type,
                versions,
                mode,
                policy,
                ..
            } => Some((edges, src_type, versions, mode, policy)),
            _ => None,
        })
        .collect();
    assert_eq!(expands.len(), 2);
    for (_, src_type, _, mode, policy) in &expands {
        assert_eq!(*src_type, "T");
        assert_eq!(**mode, ExpandMode::IndexedScan);
        assert_eq!(**policy, ExpandPolicy::Budgeted);
        assert!(policy.alternatives(**mode).is_empty());
    }
    let (_, _, versions, _, _) = expands
        .iter()
        .find(|(selection, ..)| selection.is_wildcard())
        .unwrap();
    assert_eq!(
        **versions,
        [
            ("knows".to_string(), Some(5)),
            ("likes".to_string(), Some(9)),
            ("absent".to_string(), None),
        ]
        .into_iter()
        .collect()
    );
    let assumptions = plan.assumptions();
    assert_eq!(assumptions.traversal_work_limit, Some(123));
    assert!(assumptions.has_wildcard_traversal);
    assert!(!assumptions.settings.contains_key("traversal_work_limit"));
    assert_eq!(
        assumptions.datasets["edge:knows"].as_ref().unwrap().version,
        5
    );
    assert_eq!(
        assumptions.datasets["edge:likes"].as_ref().unwrap().version,
        9
    );
    assert_eq!(assumptions.datasets.get("edge:absent"), Some(&None));

    let mut rendered = Vec::new();
    expand_json(&plan.to_json(), &mut rendered);
    for expand in &rendered {
        assert!(expand.get("edges").is_some());
        assert_eq!(expand["src_type"], "T");
        assert!(expand.get("versions").is_some());
        for old in ["edge_type", "direction", "version"] {
            assert!(expand.get(old).is_none(), "retired Expand key {old}");
        }
    }
    assert!(
        rendered
            .iter()
            .any(|expand| expand["edges"] == serde_json::json!({"kind":"wildcard","members":[{"edge_type":"absent","direction":"both"},{"edge_type":"knows","direction":"out"},{"edge_type":"likes","direction":"in"}]}))
    );
    let bound = omnigraph_planner::BoundPlan {
        plan,
        values: Default::default(),
    };
    let encoded = serde_json::to_string(&bound).unwrap();
    let restored: omnigraph_planner::BoundPlan = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored, bound);
}

#[test]
fn issue_659_selection_admission_precedes_lowering_of_outer_named_edges() {
    let op = correlated(alternatives());
    for (source, expected, code) in [
        (source(), "require a finite traversal_work_limit", "P002"),
        (
            source().with_traversal_work_limit(0),
            "must be in 1..=i64::MAX",
            "P003",
        ),
        (
            source().with_traversal_work_limit(i64::MAX as u64 + 1),
            "must be in 1..=i64::MAX",
            "P003",
        ),
        (
            source()
                .with_traversal_work_limit(100)
                .with_traversal(Traversal::Csr),
            "do not support traversal = csr",
            "P004",
        ),
    ] {
        let error = resolve(&op, &source).unwrap_err();
        let PlanError::Unsupported(diagnostic) = &error else {
            panic!("a refusal by design: {error:?}");
        };
        assert_eq!(diagnostic.code.as_str(), code, "{error}");
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn named_query_ignores_a_source_selection_allowance() {
    let query = query(ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    ));
    let source = source()
        .with_traversal_work_limit(0)
        .with_traversal(Traversal::Csr);
    let plan = omnigraph_planner::plan_query(&query, &source, &bounds()).unwrap();
    assert_eq!(plan.assumptions().traversal_work_limit, None);
    assert!(plan.live().any(|(_, node)| matches!(
        node,
        PhysicalNode::Expand {
            mode: ExpandMode::Csr,
            policy: ExpandPolicy::Pinned,
            ..
        }
    )));
}

#[test]
fn captured_cap_has_one_validated_authority() {
    let mut assumptions = Assumptions::default();
    assert_eq!(assumptions.validated_traversal_work_limit().unwrap(), None);
    for limit in [1, i64::MAX as u64] {
        assumptions.traversal_work_limit = Some(limit);
        assert_eq!(
            assumptions
                .validated_traversal_work_limit()
                .unwrap()
                .unwrap()
                .get(),
            limit
        );
    }
    for limit in [0, i64::MAX as u64 + 1, u64::MAX] {
        assumptions.traversal_work_limit = Some(limit);
        assert!(assumptions.validated_traversal_work_limit().is_err());
    }
    assumptions.traversal_work_limit = None;
    assumptions.has_wildcard_traversal = true;
    assert!(assumptions.validated_traversal_work_limit().is_err());
    assumptions.traversal_work_limit = Some(12);
    assumptions
        .settings
        .insert("traversal_work_limit".into(), "13".into());
    assert!(assumptions.validated_traversal_work_limit().is_err());
}

#[test]
fn selector_wire_spelling_and_invalid_member_lists_are_explicit() {
    for (selection, expected) in [
        (
            EdgeSelection::Named(member("Knows", Direction::Out)),
            serde_json::json!({"kind":"named","members":[{"edge_type":"Knows","direction":"out"}]}),
        ),
        (
            EdgeSelection::Alternation(vec![
                member("Knows", Direction::Both),
                member("Likes", Direction::In),
            ]),
            serde_json::json!({"kind":"alternation","members":[{"edge_type":"Knows","direction":"both"},{"edge_type":"Likes","direction":"in"}]}),
        ),
        (
            EdgeSelection::Wildcard(vec![]),
            serde_json::json!({"kind":"wildcard","members":[]}),
        ),
    ] {
        assert_eq!(
            serde_json::to_value(EdgeSelectionMirror::from(&selection)).unwrap(),
            expected
        );
        let mirror: EdgeSelectionMirror = serde_json::from_value(expected).unwrap();
        assert_eq!(EdgeSelection::try_from(mirror).unwrap(), selection);
    }
    for (kind, members) in [
        ("named", vec![]),
        ("alternation", vec![]),
        ("alternation", vec!["Knows", "Knows"]),
        ("wildcard", vec!["Likes", "Knows"]),
        ("named", vec!["Knows", "Likes"]),
        ("named", vec![""]),
    ] {
        let value = serde_json::json!({"kind":kind,"members":members.into_iter().map(|name| serde_json::json!({"edge_type":name,"direction":"out"})).collect::<Vec<_>>()});
        let mirror: EdgeSelectionMirror = serde_json::from_value(value).unwrap();
        assert!(EdgeSelection::try_from(mirror).is_err());
    }
}

#[test]
fn issue_659_union_estimate_does_not_use_one_members_fanout() {
    let source = source_with_rows(Some(10))
        .with_traversal_work_limit(100)
        .with_expand_statistics(
            "knows",
            Direction::Out,
            ExpandStatistics {
                edge_count: 10,
                src_node_count: 10,
                dst_node_count: 10,
                same_type: true,
                max_frontier_cap: 100,
                max_hops_cap: 6,
            },
        );
    let op = ir(
        vec![scan("a"), selected("a", "b", alternatives())],
        vec![prop("b", "slug")],
        vec![],
    );
    let logical = resolve(&op, &source).unwrap();
    let (id, _) = logical
        .live()
        .find(|(_, node)| matches!(node, LogicalNode::Expand { .. }))
        .unwrap();
    assert_eq!(
        omnigraph_planner::estimate_rows(&logical, id, &source),
        None
    );
}

#[test]
fn issue_659_budgeted_search_declares_no_topology_prefilter() {
    let source = source().with_traversal_work_limit(100);
    let nearest = IRExpr::Nearest {
        variable: "a".to_string(),
        property: "embedding".to_string(),
        query: Box::new(IRExpr::Param("q".to_string())),
    };
    let bm25 = IRExpr::Bm25 {
        field: Box::new(prop("a", "text")),
        query: Box::new(IRExpr::Param("q".to_string())),
    };
    for order in [
        nearest.clone(),
        IRExpr::Rrf {
            primary: Box::new(nearest),
            secondary: Box::new(bm25),
            k: None,
        },
    ] {
        let op = ir(
            vec![scan("a"), selected("a", "b", alternatives())],
            vec![prop("a", "slug")],
            vec![order],
        );
        let (plan, _) = physical(&op, &source);
        let mut declarations = 0;
        for (_, node) in plan.live() {
            match node {
                PhysicalNode::Scan {
                    ranked: Some(ranked),
                    ..
                } => {
                    if let Some(prefilter) = &ranked.prefilter {
                        declarations += 1;
                        assert!(!prefilter.admits());
                        assert!(prefilter.hops.is_empty());
                    }
                }
                PhysicalNode::RankFuse { prefilter, .. } => {
                    declarations += 1;
                    assert!(!prefilter.admits());
                    assert!(prefilter.hops.is_empty());
                }
                _ => {}
            }
        }
        assert!(
            declarations > 0,
            "the existing prefilter owner was exercised"
        );
    }
}

#[test]
fn issue_659_empty_wildcard_retains_historical_refusal_marker() {
    let query = query(ir(
        vec![
            scan("a"),
            selected("a", "b", EdgeSelection::Wildcard(vec![])),
        ],
        vec![prop("b", "slug")],
        vec![],
    ));
    let source = source().with_traversal_work_limit(100);
    let plan = omnigraph_planner::plan_query(&query, &source, &bounds()).unwrap();
    assert!(plan.assumptions().has_wildcard_traversal);
    assert!(
        plan.assumptions()
            .datasets
            .keys()
            .all(|key| !key.starts_with("edge:"))
    );
    let assumptions = serde_json::to_value(plan.assumptions()).unwrap();
    assert_eq!(assumptions["has_wildcard_traversal"], true);
    assert_eq!(assumptions["traversal_work_limit"], 100);
}

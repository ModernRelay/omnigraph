//! The bound plan's serialized form reads back as the same plan and value
//! table. Rust and not `.gqt`: the claim is about the replay boundary's
//! bytes, which no query result shows; nothing executes here.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::{ExprType, PropType, SYSTEM_COLUMNS_V3, ScalarType};
use omnigraph_planner::{
    BoundPlan, Bounds, MemorySource, NodeTypeSpec, PhysicalNode, PhysicalPlan, RankKind, RankScope,
    TableRef, ValueTable, plan_query,
};

#[path = "support/bounds.rs"]
mod fixture_bounds;

fn source() -> MemorySource {
    let schema = Arc::new(Schema::new(vec![
        Field::new(SYSTEM_COLUMNS_V3.id, DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            true,
        ),
    ]));
    MemorySource::default().with_node_type(
        "Doc",
        NodeTypeSpec {
            table: TableRef {
                type_key: "node:Doc".to_string(),
                dataset_path: "node/Doc".to_string(),
                native_branch: None,
            },
            version: Some(3),
            columns: SYSTEM_COLUMNS_V3,
            schema,
            key: vec!["slug".to_string()],
            object_columns: vec![
                SYSTEM_COLUMNS_V3.id.to_string(),
                "slug".to_string(),
                "text".to_string(),
            ],
            object_fields: vec![
                Field::new("@id", DataType::Utf8, false),
                Field::new("slug", DataType::Utf8, true),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
            row_count: Some(12),
        },
    )
}

fn value_type(scalar: ScalarType, nullable: bool) -> ExprType {
    ExprType::from_prop(&PropType::scalar(scalar, nullable))
}

fn prop(variable: &str, property: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: variable.to_string(),
        property: property.to_string(),
        ty: match property {
            "__id" => value_type(ScalarType::String, false),
            "slug" | "text" => value_type(ScalarType::String, true),
            "embedding" => value_type(ScalarType::Vector(4), true),
            other => panic!("untyped fixture property {other}"),
        },
    }
}

fn query(order_by: IRExpr) -> QueryIR {
    QueryIR {
        name: "q".to_string(),
        params: vec![],
        pipeline: vec![IROp::NodeScan {
            variable: "d".to_string(),
            type_name: "Doc".to_string(),
            filters: vec![],
        }],
        return_exprs: vec![IRProjection {
            expr: prop("d", "slug"),
            alias: None,
            column: "d.slug".into(),
            ty: omnigraph_compiler::ExprType::from_prop(&omnigraph_compiler::PropType::scalar(
                omnigraph_compiler::ScalarType::String,
                true,
            )),
        }],
        order_by: vec![IROrdering {
            expr: order_by,
            descending: false,
        }],
        limit: Some(3),
    }
}

fn plan(order_by: IRExpr) -> PhysicalPlan {
    futures::executor::block_on(plan_query(
        &query(order_by),
        &source(),
        &fixture_bounds::BOUNDS,
    ))
    .expect("the query plans")
}

fn ranked_scan(plan: &PhysicalPlan, scope: RankScope) -> usize {
    plan.live()
        .find(|(_, node)| node.ranked().is_some_and(|ranked| ranked.scope == scope))
        .map(|(id, _)| id)
        .expect("a ranked scan")
}

fn round_trip(bound: &BoundPlan) -> BoundPlan {
    let text = serde_json::to_string(bound).expect("the bound plan serializes");
    serde_json::from_str(&text).expect("the bound plan deserializes")
}

#[test]
fn saved_plan_version_refuses_both_legacy_and_future_readers() {
    let bound = BoundPlan {
        plan: plan(prop("d", "slug")),
        values: Default::default(),
    };
    let encoded = serde_json::to_value(&bound).unwrap();
    assert_eq!(encoded["bound_plan_version"], 6);
    assert!(encoded.get("plan").is_none());
    assert!(encoded["body"]["plan"].is_object());
    assert_eq!(round_trip(&bound), bound);

    for version in [1, 2, 3, 4, 5] {
        let mut older = encoded.clone();
        older["bound_plan_version"] = serde_json::json!(version);
        assert!(
            serde_json::from_value::<BoundPlan>(older)
                .unwrap_err()
                .to_string()
                .contains("regenerate")
        );
    }
    let legacy = encoded["body"].clone();
    let error = serde_json::from_value::<BoundPlan>(legacy).unwrap_err();
    assert!(error.to_string().contains("regenerate"), "{error}");
    let mut future = encoded.clone();
    future["bound_plan_version"] = serde_json::json!(7);
    let error = serde_json::from_value::<BoundPlan>(future).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported bound plan version 7"),
        "{error}"
    );

    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct OldReader {
        plan: serde_json::Value,
        values: serde_json::Value,
    }
    assert!(serde_json::from_value::<OldReader>(encoded).is_err());
}

#[test]
fn fused_saved_node_refuses_a_reader_without_declared_row_keys() {
    let arm = IRExpr::Bm25 {
        field: Box::new(prop("d", "text")),
        query: Box::new(IRExpr::Literal(
            Literal::String("needle".into()),
            value_type(ScalarType::String, false),
        )),
        ty: value_type(ScalarType::F32, false),
    };
    let bound = BoundPlan {
        plan: plan(IRExpr::Rrf {
            primary: Box::new(arm.clone()),
            secondary: Box::new(arm),
            k: None,
            ty: value_type(ScalarType::F64, false),
        }),
        values: Default::default(),
    };
    let encoded = serde_json::to_value(&bound).unwrap();
    let node = encoded["body"]["plan"]["slots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["node"] == "RankFuseWithTiebreak")
        .unwrap()
        .clone();
    assert_eq!(node["row_tiebreak"], serde_json::json!([]));
    let mut missing = encoded.clone();
    let slot = missing["body"]["plan"]["slots"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["node"] == "RankFuseWithTiebreak")
        .unwrap();
    slot.as_object_mut().unwrap().remove("row_tiebreak");
    let error = serde_json::from_value::<BoundPlan>(missing).unwrap_err();
    assert!(error.to_string().contains("row_tiebreak"), "{error}");

    #[derive(serde::Deserialize)]
    #[serde(tag = "node")]
    #[allow(dead_code)]
    enum OldReader {
        RankFuse {
            arms: serde_json::Value,
            k: Option<serde_json::Value>,
            limit: Option<usize>,
            prefilter: serde_json::Value,
        },
    }
    let error = serde_json::from_value::<OldReader>(node).err().unwrap();
    assert!(
        error
            .to_string()
            .contains("unknown variant `RankFuseWithTiebreak`"),
        "{error}"
    );
    assert_eq!(round_trip(&bound), bound);
}

#[test]
fn a_nearest_plan_with_its_vector_reads_back_equal() {
    let plan = plan(IRExpr::Nearest {
        variable: "d".to_string(),
        property: "embedding".to_string(),
        query: Box::new(IRExpr::Param(
            "q".to_string(),
            value_type(ScalarType::String, false),
        )),
        ty: value_type(ScalarType::F32, false),
    });
    let scan = ranked_scan(&plan, RankScope::Order);
    assert!(
        plan.live()
            .any(|(_, node)| matches!(node, PhysicalNode::Sort { .. })),
        "a search order plans a Sort"
    );
    let bound = BoundPlan {
        plan,
        values: ValueTable {
            params: Arc::new(
                [
                    ("q".to_string(), Literal::String("needle".to_string())),
                    (
                        "now".to_string(),
                        Literal::DateTime("2026-09-21T00:00:00Z".to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            vectors: BTreeMap::from([(scan, vec![0.25, 0.5, 0.75, 1.0])]),
        },
    };
    let back = round_trip(&bound);
    assert_eq!(back, bound);
    assert_eq!(back.plan.post_order(), bound.plan.post_order());
    assert_eq!(back.values.vectors[&scan], vec![0.25, 0.5, 0.75, 1.0]);
    let ranked = back.plan.node(scan).unwrap().ranked().unwrap();
    assert_eq!(ranked.kind, RankKind::Nearest);
    assert_eq!(ranked.fetch, Some(3));
    assert!(matches!(&ranked.query, IRExpr::Param(name, _) if name == "q"));
}

#[test]
fn a_fused_plan_reads_back_with_both_arms() {
    let plan = plan(IRExpr::Rrf {
        primary: Box::new(IRExpr::Nearest {
            variable: "d".to_string(),
            property: "embedding".to_string(),
            query: Box::new(IRExpr::Literal(
                Literal::List(vec![
                    Literal::Float(1.0),
                    Literal::Float(0.0),
                    Literal::Float(0.0),
                    Literal::Float(0.0),
                ]),
                ExprType::Value {
                    scalar: ScalarType::F64,
                    list: true,
                    nullable: false,
                },
            )),
            ty: value_type(ScalarType::F32, false),
        }),
        secondary: Box::new(IRExpr::Bm25 {
            field: Box::new(prop("d", "text")),
            query: Box::new(IRExpr::Literal(
                Literal::String("needle".to_string()),
                value_type(ScalarType::String, false),
            )),
            ty: value_type(ScalarType::F32, false),
        }),
        k: Some(Box::new(IRExpr::Literal(
            Literal::Integer(30),
            value_type(ScalarType::I64, false),
        ))),
        ty: value_type(ScalarType::F64, false),
    });
    let primary = ranked_scan(&plan, RankScope::Primary);
    let secondary = ranked_scan(&plan, RankScope::Secondary);
    assert_ne!(primary, secondary, "each arm has its own scan");
    let fuse = plan
        .live()
        .find_map(|(id, node)| matches!(node, PhysicalNode::RankFuse { .. }).then_some(id))
        .expect("a RankFuse");
    assert_eq!(plan.node(fuse).unwrap().inputs().len(), 2);
    let bound = BoundPlan {
        plan,
        values: ValueTable {
            params: Arc::new(Default::default()),
            vectors: BTreeMap::from([(primary, vec![1.0, 0.0, 0.0, 0.0])]),
        },
    };
    let back = round_trip(&bound);
    assert_eq!(back, bound);
    let changed = BoundPlan {
        values: ValueTable {
            vectors: BTreeMap::from([(primary, vec![0.0, 1.0, 0.0, 0.0])]),
            ..back.values.clone()
        },
        ..back.clone()
    };
    assert_ne!(changed, bound, "the value table is part of equality");
}

fn doc_scan(variable: &str) -> IROp {
    IROp::NodeScan {
        variable: variable.to_string(),
        type_name: "Doc".to_string(),
        filters: vec![],
    }
}

/// `$d` and `$e` over `Doc` under `filter`, returning `$d.slug`.
fn two_docs(filter: Option<IRExpr>) -> PhysicalPlan {
    let mut pipeline = vec![doc_scan("d"), doc_scan("e")];
    pipeline.extend(filter.map(IROp::Filter));
    let query = QueryIR {
        name: "q".to_string(),
        params: vec![],
        pipeline,
        return_exprs: vec![IRProjection {
            expr: prop("d", "slug"),
            alias: None,
            column: "d.slug".into(),
            ty: omnigraph_compiler::ExprType::from_prop(&omnigraph_compiler::PropType::scalar(
                omnigraph_compiler::ScalarType::String,
                true,
            )),
        }],
        order_by: vec![],
        limit: Some(3),
    };
    futures::executor::block_on(plan_query(&query, &source(), &fixture_bounds::BOUNDS))
        .expect("the query plans")
}

fn bound(plan: PhysicalPlan) -> BoundPlan {
    BoundPlan {
        plan,
        values: ValueTable {
            params: Arc::new(Default::default()),
            vectors: BTreeMap::new(),
        },
    }
}

/// `$e.text contains $d.slug and $d.slug != $e.slug` plans a `ContainsJoin` with its
/// residual and a marked right scan: node and marker read back, a plan without the
/// marker is another plan, and a document missing `residual` refuses.
#[test]
fn a_contains_join_plan_reads_back_with_its_scan_marker() {
    let other = IRExpr::comparison(prop("d", "slug"), CompOp::Ne, prop("e", "slug"));
    let plan = two_docs(Some(
        IRExpr::and_all([
            IRExpr::comparison(prop("e", "text"), CompOp::StringContains, prop("d", "slug")),
            other.clone(),
        ])
        .expect("two conjuncts"),
    ));
    let (join, right) = plan
        .live()
        .find_map(|(id, node)| match node {
            PhysicalNode::ContainsJoin {
                right, residual, ..
            } => {
                assert_eq!(residual, std::slice::from_ref(&other));
                Some((id, *right))
            }
            _ => None,
        })
        .expect("a ContainsJoin");
    let bound = bound(plan);
    let text = serde_json::to_string(&bound).expect("the bound plan serializes");
    assert!(
        text.contains(r#""node":"ContainsJoin""#) && text.contains(r#""runtime_filter":{"#),
        "{text}"
    );
    let back = round_trip(&bound);
    assert_eq!(back, bound);
    let Some(PhysicalNode::ContainsJoin { residual, .. }) = back.plan.node(join) else {
        panic!("the ContainsJoin reads back");
    };
    assert_eq!(residual, std::slice::from_ref(&other));
    let mut unmarked = back.clone();
    let Some(PhysicalNode::Scan { spec, .. }) = unmarked.plan.node_mut(right) else {
        panic!("the right side is a scan");
    };
    assert_eq!(
        spec.runtime_filter.take().map(|filter| filter.column),
        Some("text".to_string())
    );
    assert_ne!(
        unmarked, bound,
        "the scan's runtime filter is part of equality"
    );
    let mut keyless = serde_json::to_value(&bound).expect("the bound plan serializes");
    let join_slot = keyless["body"]["plan"]["slots"]
        .as_array_mut()
        .expect("the plan's slots")
        .iter_mut()
        .find(|slot| slot["node"] == "ContainsJoin")
        .expect("the ContainsJoin slot");
    join_slot
        .as_object_mut()
        .expect("a node object")
        .remove("residual")
        .expect("the residual key");
    let refused = serde_json::from_value::<BoundPlan>(keyless)
        .expect_err("a document missing `residual` refuses");
    assert!(
        refused.to_string().contains("missing field `residual`"),
        "{refused}"
    );
}

/// The `CrossJoin` node of the serialized `bound`, as JSON.
fn cross_join_json(bound: &BoundPlan, tag: &str) -> serde_json::Value {
    let value = serde_json::to_value(bound).expect("the bound plan serializes");
    value["body"]["plan"]["slots"]
        .as_array()
        .expect("the plan's slots")
        .iter()
        .find(|slot| slot["node"] == tag)
        .unwrap_or_else(|| panic!("no `{tag}` node in {value}"))
        .clone()
}

/// A filtered product serializes as `FilteredCrossJoin`, which a reader that
/// knows only `CrossJoin {left, right}` refuses, while a plain product keeps
/// that reader's exact shape: no `filters` key.
#[test]
fn a_filtered_cross_join_has_its_own_tag_and_a_plain_one_the_old_shape() {
    #[derive(serde::Deserialize)]
    #[serde(tag = "node")]
    #[allow(dead_code)]
    enum OldReader {
        CrossJoin { left: usize, right: usize },
    }
    let ne = IRExpr::comparison(prop("d", "slug"), CompOp::Ne, prop("e", "slug"));
    let filtered = bound(two_docs(Some(ne.clone())));
    let planned = filtered
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::CrossJoin { filters, .. } => Some(filters.clone()),
            _ => None,
        })
        .expect("a CrossJoin");
    assert_eq!(planned, [ne]);
    let node = cross_join_json(&filtered, "FilteredCrossJoin");
    assert_eq!(
        node["filters"],
        serde_json::json!([{
            "expr": "binary",
            "left": {"expr": "prop_access", "variable": "d", "property": "slug",
                "ty": {"kind": "value", "scalar": "String", "list": false, "nullable": true}},
            "op": {"compare": "ne"},
            "ty": {"kind": "value", "scalar": "Bool", "list": false, "nullable": true},
            "right": {"expr": "prop_access", "variable": "e", "property": "slug",
                "ty": {"kind": "value", "scalar": "String", "list": false, "nullable": true}},
        }])
    );
    let refused = serde_json::from_value::<OldReader>(node)
        .err()
        .expect("an old reader refuses");
    assert!(
        refused
            .to_string()
            .contains("unknown variant `FilteredCrossJoin`"),
        "{refused}"
    );
    assert_eq!(round_trip(&filtered), filtered);

    let plain = bound(two_docs(None));
    let node = cross_join_json(&plain, "CrossJoin");
    let (left, right) = (
        node["left"].as_u64().unwrap(),
        node["right"].as_u64().unwrap(),
    );
    let text = serde_json::to_string(&plain).expect("the bound plan serializes");
    let shape = format!(r#"{{"node":"CrossJoin","left":{left},"right":{right}}}"#);
    assert!(text.contains(&shape), "{shape} not in {text}");
    let old = serde_json::from_value::<OldReader>(node).expect("the old reader reads it");
    assert!(matches!(old, OldReader::CrossJoin { .. }));
    assert_eq!(round_trip(&plain), plain);
}

/// A top-k whose return-only column is fetched by row address reads back
/// with its `HydrateColumns` node and the scan's address column.
#[test]
fn a_hydrating_plan_reads_back_with_its_bindings() {
    let mut query = query(prop("d", "slug"));
    query.return_exprs.push(IRProjection {
        expr: prop("d", "text"),
        alias: None,
        column: "d.text".into(),
        ty: prop("d", "text").ty().clone(),
    });
    query.limit = Some(2);
    let plan = futures::executor::block_on(plan_query(&query, &source(), &fixture_bounds::BOUNDS))
        .expect("the query plans");
    assert!(matches!(
        plan.node(plan.root()),
        Some(PhysicalNode::HydrateColumns { bindings, .. })
            if bindings.len() == 1 && bindings[0].columns[0].property == "text"
    ));
    let bound = BoundPlan {
        plan,
        values: Default::default(),
    };
    assert_eq!(round_trip(&bound), bound);
}

#[test]
fn expression_mirror_preserves_every_typed_interior_and_nested_cast() {
    use omnigraph_planner::mirror::ExprMirror;
    let text = || {
        IRExpr::Literal(
            Literal::String("needle".into()),
            value_type(ScalarType::String, false),
        )
    };
    let field = || Box::new(prop("d", "text"));
    let rank = || IRExpr::Bm25 {
        field: field(),
        query: Box::new(text()),
        ty: value_type(ScalarType::F32, false),
    };
    let mut expressions = vec![
        IRExpr::Nearest {
            variable: "d".into(),
            property: "embedding".into(),
            query: Box::new(text()),
            ty: value_type(ScalarType::F32, false),
        },
        IRExpr::Search {
            field: field(),
            query: Box::new(text()),
            ty: value_type(ScalarType::Bool, false),
        },
        IRExpr::Fuzzy {
            field: field(),
            query: Box::new(text()),
            max_edits: Some(Box::new(IRExpr::Literal(
                Literal::Integer(1),
                value_type(ScalarType::I64, false),
            ))),
            ty: value_type(ScalarType::Bool, false),
        },
        IRExpr::MatchText {
            field: field(),
            query: Box::new(text()),
            ty: value_type(ScalarType::Bool, false),
        },
        rank(),
        IRExpr::Rrf {
            primary: Box::new(rank()),
            secondary: Box::new(rank()),
            k: Some(Box::new(IRExpr::Literal(
                Literal::Integer(60),
                value_type(ScalarType::I64, false),
            ))),
            ty: value_type(ScalarType::F64, false),
        },
        IRExpr::Not(
            Box::new(IRExpr::Param(
                "flag".into(),
                value_type(ScalarType::Bool, true),
            )),
            value_type(ScalarType::Bool, true),
        ),
        IRExpr::IsNull {
            expr: Box::new(IRExpr::Param(
                "flag".into(),
                value_type(ScalarType::Bool, true),
            )),
            negated: false,
            ty: value_type(ScalarType::Bool, false),
        },
        IRExpr::comparison(
            IRExpr::Cast {
                expr: Box::new(IRExpr::Literal(
                    Literal::Integer(-1),
                    value_type(ScalarType::I64, false),
                )),
                ty: ExprType::ExactInteger {
                    list: false,
                    nullable: false,
                },
            },
            CompOp::Lt,
            IRExpr::Cast {
                expr: Box::new(IRExpr::Param(
                    "unsigned".into(),
                    value_type(ScalarType::U64, true),
                )),
                ty: ExprType::ExactInteger {
                    list: false,
                    nullable: true,
                },
            },
        ),
    ];
    for nullable in [false, true] {
        expressions.push(IRExpr::Cast {
            expr: Box::new(IRExpr::Param(
                "items".into(),
                ExprType::Value {
                    scalar: ScalarType::I64,
                    list: true,
                    nullable,
                },
            )),
            ty: ExprType::ExactInteger {
                list: true,
                nullable,
            },
        });
    }
    for expression in expressions {
        let mirror = ExprMirror::from(&expression);
        let encoded = serde_json::to_vec(&mirror).unwrap();
        let decoded: ExprMirror = serde_json::from_slice(&encoded).unwrap();
        let restored = IRExpr::from(decoded);
        assert_eq!(restored, expression);
        assert_eq!(restored.ty(), expression.ty());
    }
}

/// Saved bytes must distinguish the Cast tag from its child. GQT cannot forge that child.
#[test]
fn cast_mirror_operand_is_the_child_consumed_by_type_validation() {
    use omnigraph_planner::mirror::ExprMirror;
    let cast = IRExpr::Cast {
        expr: Box::new(IRExpr::Literal(
            Literal::Float(30.0),
            value_type(ScalarType::F64, false),
        )),
        ty: value_type(ScalarType::I64, false),
    };
    cast.check_types().unwrap();
    let mut encoded = serde_json::to_value(ExprMirror::from(&cast)).unwrap();
    assert_eq!(encoded["expr"], "cast");
    assert_eq!(encoded["operand"]["expr"], "literal");
    assert_eq!(encoded["operand"]["ty"]["scalar"], "F64");
    assert_eq!(encoded["ty"]["scalar"], "I64");
    let restored = IRExpr::from(serde_json::from_value::<ExprMirror>(encoded.clone()).unwrap());
    assert_eq!(restored, cast);
    encoded["operand"] = serde_json::to_value(ExprMirror::from(&IRExpr::Param(
        "bound".into(),
        value_type(ScalarType::F64, false),
    )))
    .unwrap();
    let forged = IRExpr::from(serde_json::from_value::<ExprMirror>(encoded).unwrap());
    let IRExpr::Cast { expr, ty } = &forged else {
        panic!("Cast tag was preserved");
    };
    assert!(matches!(expr.as_ref(), IRExpr::Param(name, _) if name == "bound"));
    assert_eq!(ty, cast.ty());
    assert!(
        forged
            .check_types()
            .unwrap_err()
            .to_string()
            .contains("stored Cast")
    );
}

#[test]
fn output_validation_rejects_interior_drift_and_internal_exact_results() {
    let mut physical = plan(prop("d", "slug"));
    let id = physical
        .live()
        .find_map(|(id, node)| matches!(node, PhysicalNode::Projection { .. }).then_some(id))
        .unwrap();
    let Some(PhysicalNode::Projection { return_exprs, .. }) = physical.node_mut(id) else {
        panic!("projection");
    };
    return_exprs[0].expr = IRExpr::IsNull {
        expr: Box::new(prop("d", "slug")),
        negated: false,
        ty: value_type(ScalarType::Bool, false),
    };
    assert!(
        omnigraph_planner::validate_output_schemas(&physical)
            .unwrap_err()
            .to_string()
            .contains("expression type")
    );
    let Some(PhysicalNode::Projection { return_exprs, .. }) = physical.node_mut(id) else {
        panic!("projection");
    };
    let exact = ExprType::ExactInteger {
        list: false,
        nullable: false,
    };
    return_exprs[0].expr = IRExpr::Cast {
        expr: Box::new(IRExpr::Literal(
            Literal::Integer(1),
            value_type(ScalarType::I64, false),
        )),
        ty: exact.clone(),
    };
    return_exprs[0].ty = exact;
    assert!(
        omnigraph_planner::validate_output_schemas(&physical)
            .unwrap_err()
            .to_string()
            .contains("not a public result")
    );
}

fn block_bound(left: omnigraph_compiler::ir::BlockAggregateExpr, right: IRExpr) -> BoundPlan {
    let aggregate = omnigraph_planner::plan_block_aggregate(&left).unwrap();
    let mut plan = PhysicalPlan::new();
    let input = plan.add(PhysicalNode::OuterReference {
        outer_var: "p".into(),
    });
    let inner = plan.add(PhysicalNode::OuterReference {
        outer_var: "p".into(),
    });
    let root = plan.add(PhysicalNode::AntiJoin {
        input,
        inner,
        outer_var: "p".into(),
        aggregate,
        predicate: omnigraph_compiler::ir::SubqueryPredicate {
            left,
            op: CompOp::Gt,
            right,
        },
    });
    plan.set_root(root);
    BoundPlan {
        plan,
        values: ValueTable::default(),
    }
}

fn signed_sum_block() -> BoundPlan {
    use omnigraph_compiler::ir::BlockAggregateExpr;
    use omnigraph_compiler::query::ast::AggFunc;
    let arg = value_type(ScalarType::I64, true);
    block_bound(
        BlockAggregateExpr::Aggregate {
            func: AggFunc::Sum,
            arg: Box::new(IRExpr::PropAccess {
                variable: "c".into(),
                property: "amount".into(),
                ty: arg.clone(),
            }),
            signature: omnigraph_compiler::AggSignature {
                arg,
                result: value_type(ScalarType::F64, true),
            },
        },
        IRExpr::Literal(Literal::Float(0.0), value_type(ScalarType::F64, false)),
    )
}

fn block_parts(
    bound: &mut BoundPlan,
) -> (
    &mut omnigraph_compiler::ir::SubqueryPredicate,
    &mut Option<omnigraph_planner::AggregateSpec>,
) {
    let root = bound.plan.root();
    let Some(PhysicalNode::AntiJoin {
        predicate,
        aggregate,
        ..
    }) = bound.plan.node_mut(root)
    else {
        panic!("block root");
    };
    (predicate, aggregate)
}

fn refuses_block_before_execution(bound: &BoundPlan) {
    assert!(omnigraph_planner::validate_aggregate_specs(&bound.plan).is_err());
    let encoded = serde_json::to_vec(bound).unwrap();
    let restored: BoundPlan = serde_json::from_slice(&encoded).unwrap();
    assert!(omnigraph_planner::validate_aggregate_specs(&restored.plan).is_err());
}

#[test]
fn saved_block_tree_preserves_spec_signature_and_both_cast_roots() {
    use omnigraph_compiler::ir::BlockAggregateExpr;
    use omnigraph_compiler::query::ast::AggFunc;
    let exact = |nullable| ExprType::ExactInteger {
        list: false,
        nullable,
    };
    let arg = value_type(ScalarType::U64, true);
    let bound = block_bound(
        BlockAggregateExpr::Cast {
            expr: Box::new(BlockAggregateExpr::Aggregate {
                func: AggFunc::Max,
                arg: Box::new(IRExpr::PropAccess {
                    variable: "c".into(),
                    property: "large".into(),
                    ty: arg.clone(),
                }),
                signature: omnigraph_compiler::AggSignature {
                    arg: arg.clone(),
                    result: arg,
                },
            }),
            ty: exact(true),
        },
        IRExpr::Cast {
            expr: Box::new(IRExpr::Param(
                "bound".into(),
                value_type(ScalarType::I64, false),
            )),
            ty: exact(false),
        },
    );
    omnigraph_planner::validate_aggregate_specs(&bound.plan).unwrap();
    let restored = round_trip(&bound);
    omnigraph_planner::validate_aggregate_specs(&restored.plan).unwrap();
    let Some(PhysicalNode::AntiJoin {
        predicate,
        aggregate,
        ..
    }) = bound.plan.node(bound.plan.root())
    else {
        panic!("block");
    };
    let Some(PhysicalNode::AntiJoin {
        predicate: back,
        aggregate: back_spec,
        ..
    }) = restored.plan.node(restored.plan.root())
    else {
        panic!("block");
    };
    assert_eq!(predicate, back);
    assert_eq!(aggregate, back_spec);
    let explain = bound.plan.to_json();
    assert_eq!(explain["predicate"], "max($c.large) > $bound");
    assert_eq!(explain["typed_left"]["type"], "exact_integer?");
    assert_eq!(explain["typed_left"]["args"][0]["type"], "U64?");
    assert_eq!(explain["typed_right"]["type"], "exact_integer");
    assert_eq!(explain["aggregate"]["input"], "U64?");
    assert_eq!(explain["aggregate"]["result"], "U64?");
    assert_eq!(explain, restored.plan.to_json());
    let rows = block_bound(
        BlockAggregateExpr::Cast {
            expr: Box::new(BlockAggregateExpr::CountRows {
                ty: value_type(ScalarType::I64, false),
            }),
            ty: value_type(ScalarType::F64, false),
        },
        IRExpr::Param("bound".into(), value_type(ScalarType::F64, false)),
    );
    omnigraph_planner::validate_aggregate_specs(&rows.plan).unwrap();
    assert_eq!(round_trip(&rows), rows);
    assert!(rows.plan.to_json()["aggregate"].is_null());
    assert_eq!(
        rows.plan.to_json()["typed_left"]["args"][0]["op"],
        "count_rows"
    );
}

#[test]
fn saved_blocks_refuse_spec_signature_and_cast_drift_before_reading_rows() {
    use omnigraph_compiler::ir::BlockAggregateExpr;
    use omnigraph_planner::{Accumulator, AggregateSpec, Overflow};
    for wrong in [
        None,
        Some(AggregateSpec {
            accumulator: Accumulator::Float64,
            overflow: Overflow::RoundToNearest,
        }),
        Some(AggregateSpec {
            accumulator: Accumulator::ExactInteger,
            overflow: Overflow::Error,
        }),
    ] {
        let mut bound = signed_sum_block();
        *block_parts(&mut bound).1 = wrong;
        refuses_block_before_execution(&bound);
    }
    for argument in [true, false] {
        let mut bound = signed_sum_block();
        let BlockAggregateExpr::Aggregate { signature, .. } = &mut block_parts(&mut bound).0.left
        else {
            panic!("aggregate");
        };
        if argument {
            signature.arg = value_type(ScalarType::F64, true);
        } else {
            signature.result = value_type(ScalarType::I64, true);
        }
        refuses_block_before_execution(&bound);
    }
    let mut bound = signed_sum_block();
    let predicate = block_parts(&mut bound).0;
    predicate.left = BlockAggregateExpr::Cast {
        expr: Box::new(predicate.left.clone()),
        ty: value_type(ScalarType::I64, true),
    };
    predicate.right = IRExpr::Literal(Literal::Integer(0), value_type(ScalarType::I64, false));
    refuses_block_before_execution(&bound);
    let mut bound = signed_sum_block();
    let predicate = block_parts(&mut bound).0;
    predicate.left = BlockAggregateExpr::Cast {
        expr: Box::new(predicate.left.clone()),
        ty: value_type(ScalarType::F64, false),
    };
    refuses_block_before_execution(&bound);
    for right in [
        IRExpr::Literal(Literal::Integer(0), value_type(ScalarType::I64, false)),
        IRExpr::PropAccess {
            variable: "p".into(),
            property: "amount".into(),
            ty: value_type(ScalarType::F64, false),
        },
        IRExpr::AliasRef("bound".into(), value_type(ScalarType::F64, false)),
    ] {
        let mut bound = signed_sum_block();
        block_parts(&mut bound).0.right = right;
        refuses_block_before_execution(&bound);
    }
    let mut bound = signed_sum_block();
    block_parts(&mut bound).0.op = CompOp::Contains;
    refuses_block_before_execution(&bound);
}

#[test]
fn row_count_type_and_absent_spec_are_checked_on_every_saved_block() {
    use omnigraph_compiler::ir::{BlockAggregateExpr, SubqueryPredicate};
    let valid = block_bound(
        SubqueryPredicate::not_exists().left,
        IRExpr::Literal(Literal::Integer(0), value_type(ScalarType::I64, false)),
    );
    omnigraph_planner::validate_aggregate_specs(&valid.plan).unwrap();
    for ty in [
        value_type(ScalarType::I32, false),
        value_type(ScalarType::I64, true),
    ] {
        let mut bound = valid.clone();
        block_parts(&mut bound).0.left = BlockAggregateExpr::CountRows { ty };
        refuses_block_before_execution(&bound);
    }
    let mut bound = valid.clone();
    *block_parts(&mut bound).1 = Some(omnigraph_planner::AggregateSpec {
        accumulator: omnigraph_planner::Accumulator::Count,
        overflow: omnigraph_planner::Overflow::Error,
    });
    refuses_block_before_execution(&bound);
    let mut bound = valid;
    let mut hidden = bound.plan.node(bound.plan.root()).unwrap().clone();
    let PhysicalNode::AntiJoin { predicate, .. } = &mut hidden else {
        panic!("block");
    };
    predicate.left = BlockAggregateExpr::CountRows {
        ty: value_type(ScalarType::U64, false),
    };
    bound.plan.add(hidden);
    refuses_block_before_execution(&bound);
}

#[test]
fn block_mirrors_and_explain_preserve_every_nested_cast_in_order() {
    use omnigraph_compiler::ir::BlockAggregateExpr;
    let left = BlockAggregateExpr::Cast {
        expr: Box::new(BlockAggregateExpr::Cast {
            expr: Box::new(BlockAggregateExpr::CountRows {
                ty: value_type(ScalarType::I64, false),
            }),
            ty: ExprType::ExactInteger {
                list: false,
                nullable: false,
            },
        }),
        ty: value_type(ScalarType::F64, false),
    };
    let bound = block_bound(
        left,
        IRExpr::Param("bound".into(), value_type(ScalarType::F64, false)),
    );
    omnigraph_planner::validate_aggregate_specs(&bound.plan).unwrap();
    let restored = round_trip(&bound);
    let Some(PhysicalNode::AntiJoin { predicate, .. }) = bound.plan.node(bound.plan.root()) else {
        panic!("block");
    };
    let Some(PhysicalNode::AntiJoin {
        predicate: back, ..
    }) = restored.plan.node(restored.plan.root())
    else {
        panic!("block");
    };
    assert_eq!(predicate, back);
    let explain = restored.plan.to_json();
    assert_eq!(explain["predicate"], "count > $bound");
    assert_eq!(explain["typed_left"]["type"], "F64");
    assert_eq!(explain["typed_left"]["args"][0]["type"], "exact_integer");
    assert_eq!(explain["typed_left"]["args"][0]["args"][0]["type"], "I64");
    assert_eq!(
        explain["typed_left"]["args"][0]["args"][0]["op"],
        "count_rows"
    );
}

//! The bound plan's serialized form reads back as the same plan and value
//! table. Rust and not `.gqt`: the claim is about the replay boundary's
//! bytes, which no query result shows; nothing executes here.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::SYSTEM_COLUMNS_V3;
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_planner::{
    BOUND_PLAN_VERSION, BoundPlan, Bounds, MemorySource, NodeTypeSpec, PhysicalNode, PhysicalPlan,
    RankKind, RankScope, TableRef, ValueTable, plan_query,
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
            row_count: Some(12),
        },
    )
}

fn prop(variable: &str, property: &str) -> IRExpr {
    IRExpr::PropAccess {
        variable: variable.to_string(),
        property: property.to_string(),
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
        }],
        order_by: vec![IROrdering {
            expr: order_by,
            descending: false,
        }],
        limit: Some(3),
    }
}

fn plan(order_by: IRExpr) -> PhysicalPlan {
    plan_query(&query(order_by), &source(), &fixture_bounds::BOUNDS).expect("the query plans")
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
    assert_eq!(encoded["bound_plan_version"], BOUND_PLAN_VERSION);
    assert!(encoded.get("plan").is_none());
    assert!(encoded["body"]["plan"].is_object());
    assert_eq!(round_trip(&bound), bound);

    let legacy = encoded["body"].clone();
    let error = serde_json::from_value::<BoundPlan>(legacy).unwrap_err();
    assert!(error.to_string().contains("regenerate"), "{error}");
    for version in [BOUND_PLAN_VERSION - 1, BOUND_PLAN_VERSION + 1] {
        let mut other = encoded.clone();
        other["bound_plan_version"] = serde_json::json!(version);
        let error = serde_json::from_value::<BoundPlan>(other).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("unsupported bound plan version {version}")),
            "{error}"
        );
    }

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
        query: Box::new(IRExpr::Literal(Literal::String("needle".into()))),
    };
    let bound = BoundPlan {
        plan: plan(IRExpr::Rrf {
            primary: Box::new(arm.clone()),
            secondary: Box::new(arm),
            k: None,
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
        query: Box::new(IRExpr::Param("q".to_string())),
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
    assert!(matches!(&ranked.query, IRExpr::Param(name) if name == "q"));
}

#[test]
fn a_fused_plan_reads_back_with_both_arms() {
    let plan = plan(IRExpr::Rrf {
        primary: Box::new(IRExpr::Nearest {
            variable: "d".to_string(),
            property: "embedding".to_string(),
            query: Box::new(IRExpr::Literal(Literal::List(vec![
                Literal::Float(1.0),
                Literal::Float(0.0),
                Literal::Float(0.0),
                Literal::Float(0.0),
            ]))),
        }),
        secondary: Box::new(IRExpr::Bm25 {
            field: Box::new(prop("d", "text")),
            query: Box::new(IRExpr::Literal(Literal::String("needle".to_string()))),
        }),
        k: Some(Box::new(IRExpr::Literal(Literal::Integer(30)))),
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
        }],
        order_by: vec![],
        limit: Some(3),
    };
    plan_query(&query, &source(), &fixture_bounds::BOUNDS).expect("the query plans")
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
            "left": {"expr": "prop_access", "variable": "d", "property": "slug"},
            "op": {"compare": "ne"},
            "right": {"expr": "prop_access", "variable": "e", "property": "slug"},
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

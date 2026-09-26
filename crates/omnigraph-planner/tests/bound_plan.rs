//! The bound plan's serialized form reads back as the same plan and value
//! table. Rust and not `.gqt`: the claim is about the replay boundary's
//! bytes, which no query result shows; nothing executes here.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::SYSTEM_COLUMNS_V3;
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, QueryIR};
use omnigraph_compiler::query::ast::Literal;
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

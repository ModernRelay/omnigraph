use super::*;

#[test]
fn expand_destination_filters_and_projection_belong_to_its_dependent_scan() {
    let op = ir(
        vec![
            scan("a"),
            IROp::Expand {
                src_var: "a".to_string(),
                dst_var: "b".to_string(),
                edge_type: "knows".to_string(),
                direction: Direction::Out,
                dst_type: "T".to_string(),
                min_hops: 1,
                max_hops: Some(1),
                dst_filters: vec![IRExpr::comparison(
                    prop("b", "state"),
                    CompOp::Eq,
                    IRExpr::Literal(Literal::String("open".into())),
                )],
                edge_binding: None,
            },
        ],
        vec![prop("a", "slug")],
        vec![],
    );
    let (plan, _) = planned(&op);
    assert_eq!(projection_of(&plan, "a"), set(&["__id", "slug"]));
    assert_eq!(projection_of(&plan, "b"), set(&["__id", "slug", "state"]));
    let (input, spec) = plan
        .live()
        .find_map(|(_, node)| match node {
            LogicalNode::TableScan {
                input: Some(input),
                spec,
            } => Some((*input, spec)),
            _ => None,
        })
        .expect("a dependent destination scan");
    assert_eq!(spec.binding.as_deref(), Some("b"));
    assert_eq!(spec.version, Some(7));
    assert_eq!(spec.table.type_key, "node:T");
    assert_eq!(
        spec.filter
            .as_ref()
            .expect("destination filter")
            .gq_filters()
            .len(),
        1
    );
    assert!(matches!(plan.node(input), Some(LogicalNode::Expand { .. })));
    let json = plan.to_json();
    let destination = &json["inputs"][0]["inputs"][0];
    assert_eq!(destination["node"], "TableScan");
    assert_eq!(destination["id_restriction"], "input");
    assert_eq!(destination["version"], 7);
    assert_eq!(destination["inputs"][0]["node"], "Expand");
    assert!(destination["inputs"][0].get("projection").is_none());
    assert!(destination["inputs"][0].get("dst_filters").is_none());
}

#[test]
fn the_explain_document_names_the_binding_and_the_projection() {
    let op = ir(vec![scan("c")], vec![prop("c", "slug")], vec![]);
    let (plan, _) = planned(&op);
    let json = plan.to_json();
    let scan = json["inputs"][0]["inputs"][0].clone();
    assert_eq!(scan["node"], "TableScan");
    assert_eq!(scan["binding"], "c");
    assert_eq!(scan["table"], "node:T");
    assert_eq!(scan["projection"], serde_json::json!(["slug"]));
}

#[test]
fn physical_destination_scan_retains_its_input_and_does_not_claim_table_cardinality() {
    let source = source().with_fragments(
        SideId::Binding(1),
        vec![FragmentStat {
            id: 0,
            rows: Some(123),
            bytes: Some(1024),
        }],
    );
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let (physical, _) = physical(&op, &source);
    let physical = &physical;
    let (id, input, spec) = physical
        .live()
        .find_map(|(id, node)| match node {
            omnigraph_planner::PhysicalNode::Scan {
                source: omnigraph_planner::ScanInput::Dependent { input, .. },
                spec,
                ..
            } => Some((id, *input, spec)),
            _ => None,
        })
        .expect("dependent physical scan");
    assert_eq!(spec.binding.as_deref(), Some("b"));
    assert!(matches!(
        physical.node(input),
        Some(omnigraph_planner::PhysicalNode::Expand { .. })
    ));
    assert_eq!(
        physical.properties(id).expect("scan properties").rows,
        omnigraph_planner::Estimate::Unknown
    );
    assert!(
        physical
            .properties(id)
            .expect("scan properties")
            .sources
            .is_empty()
    );
    let json = physical.to_json();
    let destination = &json["inputs"][0]["inputs"][0];
    assert_eq!(destination["node"], "Scan");
    assert_eq!(destination["id_restriction"], "input");
    assert_eq!(destination["inputs"][0]["node"], "Expand");
}

/// An `Expand` carries the pinned version of its edge table into explain and
/// through the bound plan's serde round trip. Rust and not `.gqt`: the pinned
/// version is an engine replay fact no case observes.
#[test]
fn physical_expand_carries_its_pinned_edge_version() {
    let source = source().with_edge_version("knows", 5);
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let (physical, _) = physical(&op, &source);
    assert!(physical.live().any(|(_, node)| matches!(
        node,
        PhysicalNode::Expand {
            version: Some(5),
            ..
        }
    )));
    let json = physical.to_json();
    let expand = &json["inputs"][0]["inputs"][0]["inputs"][0];
    assert_eq!(expand["node"], "Expand");
    assert_eq!(expand["version"], 5);
    let bound = omnigraph_planner::BoundPlan {
        plan: physical,
        values: omnigraph_planner::ValueTable {
            params: Arc::new(Default::default()),
            vectors: Default::default(),
        },
    };
    let text = serde_json::to_string(&bound).expect("the bound plan serializes");
    let back: omnigraph_planner::BoundPlan =
        serde_json::from_str(&text).expect("the bound plan deserializes");
    assert_eq!(back, bound);
}

/// An `AntiJoin`'s predicate with a column argument and a parameter bound
/// survives the bound plan's serde round trip. Rust and not `.gqt`: the
/// replay boundary's bytes are what no query result shows.
#[test]
fn physical_anti_join_predicate_reads_back_equal() {
    let predicate = omnigraph_compiler::ir::SubqueryPredicate {
        func: AggFunc::Max,
        arg: Some(prop("x", "rank")),
        op: CompOp::Gt,
        right: IRExpr::Param("since".to_string()),
    };
    let op = ir(
        vec![
            scan("a"),
            IROp::AntiJoin {
                outer_var: "a".to_string(),
                inner: vec![expand("a", "x", vec![])],
                predicate: predicate.clone(),
            },
        ],
        vec![prop("a", "slug")],
        vec![],
    );
    let (physical, _) = physical(&op, &source());
    let printed = physical
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::AntiJoin { predicate, .. } => Some(predicate.to_string()),
            _ => None,
        })
        .expect("an AntiJoin");
    assert_eq!(printed, predicate.to_string());
    let bound = omnigraph_planner::BoundPlan {
        plan: physical,
        values: omnigraph_planner::ValueTable {
            params: Arc::new(
                [("since".to_string(), Literal::Integer(3))]
                    .into_iter()
                    .collect(),
            ),
            vectors: Default::default(),
        },
    };
    let text = serde_json::to_string(&bound).expect("the bound plan serializes");
    let back: omnigraph_planner::BoundPlan =
        serde_json::from_str(&text).expect("the bound plan deserializes");
    assert_eq!(back, bound);
    let restored = back
        .plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::AntiJoin { predicate, .. } => Some(predicate.to_string()),
            _ => None,
        })
        .expect("the AntiJoin reads back");
    assert_eq!(restored, printed);
}

/// A plan's words are GQ: `Sort` keys carry their direction, a leading search
/// function leads the declared ordering with its score column, and a query
/// node prints no `schema` and the root scan its pinned version.
#[test]
fn the_physical_document_prints_gq_orderings_and_no_query_schema() {
    let op = Operation::Query(Box::new(QueryIR {
        name: "q".to_string(),
        params: vec![],
        pipeline: vec![scan("c")],
        return_exprs: vec![IRProjection {
            expr: prop("c", "slug"),
            alias: None,
        }],
        order_by: vec![
            IROrdering {
                expr: IRExpr::Nearest {
                    variable: "c".to_string(),
                    property: "embedding".to_string(),
                    query: Box::new(IRExpr::Param("q".to_string())),
                },
                descending: false,
            },
            IROrdering {
                expr: prop("c", "rank"),
                descending: true,
            },
        ],
        limit: Some(10),
    }));
    let (logical, _) = planned(&op);
    assert!(matches!(
        logical.node(logical.root()),
        Some(LogicalNode::Limit { rows: 10, .. })
    ));
    assert!(
        !logical.live().any(|(_, node)| matches!(
            node,
            LogicalNode::Page { .. } | LogicalNode::Ordered { .. }
        ))
    );
    let (plan, _) = physical(&op, &source());
    assert!(matches!(
        plan.node(plan.root()),
        Some(PhysicalNode::Limit { rows: 10, .. })
    ));
    let json = plan.to_json();
    assert_eq!(json["node"], "Page");
    assert_eq!(json["bytes"], 0);
    assert!(json["resume"].is_null());
    let sort = &json["inputs"][0];
    assert_eq!(sort["node"], "Sort");
    assert_eq!(
        sort["keys"],
        serde_json::json!(["$c._distance asc", "$c.rank desc"])
    );
    assert_eq!(sort["fetch"], 10);
    assert_eq!(
        sort["properties"]["ordering"],
        serde_json::json!(["$c._distance asc", "$c.rank desc"])
    );
    assert!(sort["properties"].get("schema").is_none());
    let projection = &sort["inputs"][0];
    assert_eq!(projection["exprs"], serde_json::json!(["$c.slug"]));
    let root_scan = &projection["inputs"][0];
    assert_eq!(root_scan["node"], "Scan");
    assert_eq!(root_scan["version"], 7);
    assert_eq!(
        root_scan["ranked"],
        serde_json::json!({
            "kind": "nearest",
            "property": "embedding",
            "query": "$q",
            "fetch": 10,
            "nprobes": null,
            "scope": "order",
        })
    );
    assert_eq!(
        root_scan["properties"]["ordering"],
        serde_json::json!(["$c._distance asc"])
    );
    assert!(root_scan["properties"].get("schema").is_none());
}

/// A bare search order plans the score `Sort` the engine runs, with the
/// query's limit as its fetch; a fusion plans none, since it orders its own
/// rows, and each arm is its own subtree.
#[test]
fn a_search_order_plans_its_score_sort_and_a_fusion_two_arms() {
    let bm25 = IRExpr::Bm25 {
        field: Box::new(prop("c", "text")),
        query: Box::new(IRExpr::Param("t".to_string())),
    };
    let (plan, _) = physical(
        &ir(vec![scan("c")], vec![prop("c", "slug")], vec![bm25.clone()]),
        &source(),
    );
    let json = plan.to_json();
    assert_eq!(json["node"], "Page");
    let sort = &json["inputs"][0];
    assert_eq!(sort["node"], "Sort");
    assert_eq!(sort["keys"], serde_json::json!(["$c._score desc"]));
    assert_eq!(sort["fetch"], 10);
    assert_eq!(sort["inputs"][0]["node"], "Projection");
    let ranked = &sort["inputs"][0]["inputs"][0]["ranked"];
    assert_eq!(ranked["kind"], "bm25");
    assert_eq!(ranked["query"], "$t");
    assert!(ranked["fetch"].is_null());

    let rrf = IRExpr::Rrf {
        primary: Box::new(IRExpr::Nearest {
            variable: "c".to_string(),
            property: "embedding".to_string(),
            query: Box::new(IRExpr::Param("q".to_string())),
        }),
        secondary: Box::new(bm25),
        k: None,
    };
    let (plan, _) = physical(
        &ir(vec![scan("c")], vec![prop("c", "slug")], vec![rrf]),
        &source(),
    );
    let json = plan.to_json();
    assert_eq!(json["node"], "Page");
    assert_eq!(json["inputs"][0]["node"], "Projection");
    let fuse = &json["inputs"][0]["inputs"][0];
    assert_eq!(fuse["node"], "RankFuse");
    assert_eq!(fuse["limit"], 10);
    assert!(fuse["k"].is_null());
    assert_eq!(
        fuse["properties"]["ordering"],
        serde_json::json!(["rrf($c, $c) desc"])
    );
    let arms = fuse["inputs"].as_array().expect("two arm inputs");
    assert_eq!(arms.len(), 2);
    assert_ne!(arms[0]["id"], arms[1]["id"]);
    assert_eq!(arms[0]["ranked"]["kind"], "nearest");
    assert_eq!(arms[0]["ranked"]["scope"], "primary");
    assert_eq!(arms[0]["ranked"]["fetch"], 10);
    assert_eq!(arms[1]["ranked"]["kind"], "bm25");
    assert_eq!(arms[1]["ranked"]["scope"], "secondary");
    assert!(
        !plan
            .live()
            .any(|(_, node)| matches!(node, PhysicalNode::Sort { .. }))
    );
}

/// Both inputs of a `CrossJoin` keep their operators in `pipelines_json`, and
/// a query's explain carries no `pipelines`: the engine lowers it to one
/// DataFusion plan, which its own explain rows print.
#[test]
fn a_cross_join_side_keeps_its_operators_and_a_query_explain_has_no_pipelines() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![]), scan("c")],
        vec![prop("b", "slug"), prop("c", "slug")],
        vec![],
    );
    let (logical, _) = planned(&op);
    assert!(
        logical
            .live()
            .any(|(_, node)| matches!(node, LogicalNode::CrossJoin { .. }))
    );
    assert_eq!(
        logical.census().joins,
        vec![omnigraph_planner::JoinKind::Cross]
    );
    let (plan, _) = physical(&op, &source());
    let pipelines = plan.pipelines_json();
    let join = &pipelines[0]["source"];
    assert_eq!(join["node"], "CrossJoin");
    assert_eq!(
        join["left"]["operators"],
        serde_json::json!(["Expand", "Scan(input ids)"])
    );
    assert_eq!(join["left"]["source"]["node"], "Scan");
    assert_eq!(join["right"]["operators"], serde_json::json!([]));
    let decision = omnigraph_planner::route(
        &op,
        &source(),
        omnigraph_planner::RouteOverride::Registry,
        &bounds(),
    );
    let explain = decision.explain();
    assert_eq!(explain.route, "engine");
    assert!(explain.physical_plan.is_some());
    assert!(explain.pipelines.is_none());
    assert!(explain.to_value().get("pipelines").is_none());
}

use super::*;

#[test]
fn expand_mode_records_the_frontier_and_applies_its_hard_cap() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let small = source_with_rows(Some(10)).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(10),
    );
    let (plan, fired) = physical(&op, &small);
    assert_eq!(
        expand_modes(&plan),
        vec![(ExpandMode::IndexedScan, Some(10))]
    );
    assert!(fired.contains(&"expand_mode"), "{fired:?}");
    let mut expands = Vec::new();
    expand_json(&plan.to_json(), &mut expands);
    assert_eq!(expands[0]["mode"], "indexed_scan");
    assert_eq!(expands[0]["frontier_estimate"], 10);

    let large = source_with_rows(Some(5_000)).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(5000),
    );
    let (plan, fired) = physical(&op, &large);
    assert_eq!(expand_modes(&plan), vec![(ExpandMode::Csr, Some(5_000))]);
    assert!(fired.contains(&"expand_mode"), "{fired:?}");
    let mut expands = Vec::new();
    expand_json(&plan.to_json(), &mut expands);
    assert_eq!(expands[0]["mode"], "csr");
    assert_eq!(expands[0]["frontier_estimate"], 5_000);
}

#[test]
fn a_forced_traversal_overrides_the_cost_model() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let source = source_with_rows(Some(10))
        .with_expand_statistics("knows", Direction::Out, knows_statistics(10))
        .with_traversal(Traversal::Csr);
    let (plan, fired) = physical(&op, &source);
    assert_eq!(expand_modes(&plan), vec![(ExpandMode::Csr, Some(10))]);
    assert!(!fired.contains(&"expand_mode"), "{fired:?}");
    assert!(
        plan.live().any(|(_, node)| matches!(
            node,
            PhysicalNode::Expand {
                policy: ExpandPolicy::Pinned,
                ..
            }
        )),
        "a pinned mode declares no alternative"
    );
    let source = source_with_rows(Some(5_000))
        .with_expand_statistics("knows", Direction::Out, knows_statistics(5000))
        .with_traversal(Traversal::Indexed);
    let (plan, _) = physical(&op, &source);
    assert_eq!(
        expand_modes(&plan),
        vec![(ExpandMode::IndexedScan, Some(5_000))]
    );
}

#[test]
fn a_second_expand_after_a_csr_expand_reuses_the_warm_csr() {
    let op = ir(
        vec![
            scan("a"),
            expand("a", "b", vec![]),
            expand("b", "c", vec![]),
        ],
        vec![prop("c", "slug")],
        vec![],
    );
    let source = source_with_rows(Some(5_000)).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(5000),
    );
    let (plan, _) = physical(&op, &source);
    let modes = expand_modes(&plan);
    assert_eq!(modes[0], (ExpandMode::Csr, Some(5_000)));
    assert_eq!(modes[1].0, ExpandMode::Csr);
    let csr_cached = plan
        .post_order()
        .into_iter()
        .filter_map(|id| match plan.node(id) {
            Some(PhysicalNode::Expand { policy, .. }) => policy.cost().map(|cost| cost.csr_cached),
            _ => None,
        });
    assert_eq!(csr_cached.collect::<Vec<bool>>(), vec![false, true]);

    let selective = source_with_rows(Some(1)).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(1),
    );
    let (plan, _) = physical(&op, &selective);
    assert_eq!(
        expand_modes(&plan),
        vec![
            (ExpandMode::IndexedScan, Some(1)),
            (ExpandMode::IndexedScan, Some(1))
        ],
        "the dependent scan of `b` is bounded by T's one row"
    );
}

/// `rows <= frontier * 8` joins: 20,000 rows under a 20,000 frontier (the
/// benchmark fixture) and 6 under 6 (3 sources of `S`, 6 edges); 20 rows
/// under a frontier of 1 look ids up. The pass fires when both were known.
#[test]
fn access_path_follows_the_frontier_estimate_and_the_table_row_count() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let statistics = |edge_count, src_node_count, dst_node_count| ExpandStatistics {
        edge_count,
        src_node_count,
        dst_node_count,
        same_type: src_node_count == dst_node_count,
        max_frontier_cap: 1 << 20,
        max_hops_cap: 6,
    };
    let fixture = pooled(source_with_rows(Some(20_000))).with_expand_statistics(
        "knows",
        Direction::Out,
        statistics(100_000, 20_000, 20_000),
    );
    let (plan, fired) = physical(&op, &fixture);
    let (paths, json) = access_paths(&plan);
    assert_eq!(paths, vec![AccessPath::HashJoin]);
    assert_eq!(json, "hash_join");
    assert!(fired.contains(&"access_path"), "{fired:?}");

    let from_s = ir(
        vec![scan_of("a", "S"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let dense = pooled(source_with_rows(Some(6)))
        .with_node_type("S", node_type("S", Some(3)))
        .with_expand_statistics("knows", Direction::Out, statistics(6, 3, 6));
    let (plan, fired) = physical(&from_s, &dense);
    let (paths, json) = access_paths(&plan);
    assert_eq!(paths, vec![AccessPath::HashJoin]);
    assert_eq!(json, "hash_join");
    assert!(fired.contains(&"access_path"), "{fired:?}");

    let sparse = pooled(source_with_rows(Some(20)))
        .with_node_type("S", node_type("S", Some(1)))
        .with_expand_statistics("knows", Direction::Out, statistics(1, 1, 20));
    assert!(
        omnigraph_planner::optimizer::column_statistics_needed(&from_s, &sparse)
            .unwrap()
            .is_empty()
    );
    let (plan, fired) = physical(&from_s, &sparse);
    let (paths, json) = access_paths(&plan);
    assert_eq!(paths, vec![AccessPath::IdLookup]);
    assert_eq!(json, "id_lookup");
    assert!(fired.contains(&"access_path"), "{fired:?}");
    let json = plan.to_json();
    let root = &json["inputs"][0]["inputs"][0]["inputs"][0]["inputs"][0];
    assert_eq!(root["node"], "Scan");
    assert!(root.get("access").is_none());
}

/// The benchmark fixture's row ratio joins, so the build side decides: 40 MiB
/// of data files under the 150 MiB pool (budget 37.5 MiB), unknown data-file
/// bytes, and a source with no pool each look ids up; the pass still fires.
#[test]
fn access_path_looks_ids_up_when_the_build_side_does_not_fit_the_pool() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let fixture = || {
        source_with_rows(Some(20_000)).with_expand_statistics(
            "knows",
            Direction::Out,
            ExpandStatistics {
                edge_count: 100_000,
                src_node_count: 20_000,
                dst_node_count: 20_000,
                same_type: true,
                max_frontier_cap: 1 << 20,
                max_hops_cap: 6,
            },
        )
    };
    let too_wide = fixture()
        .with_query_memory_pool_bytes(POOL_BYTES)
        .with_table_data_bytes("node:T", 40 * 1024 * 1024);
    let unknown_bytes = fixture().with_query_memory_pool_bytes(POOL_BYTES);
    let no_pool = fixture().with_table_data_bytes("node:T", 4 * 1024 * 1024);
    for source in [too_wide, unknown_bytes, no_pool] {
        let optimized = {
            let mut plan = resolve(&op, &source).expect("resolve traversal");
            let fired = rewrite(&mut plan, &source).expect("rewrite traversal");
            omnigraph_planner::physical_plan(&mut plan, &source, &bounds(), fired)
                .expect("lower traversal")
        };
        let (paths, json) = access_paths(&optimized.physical);
        assert_eq!(paths, vec![AccessPath::IdLookup]);
        assert_eq!(json, "id_lookup");
        assert!(optimized.fired.contains(&"access_path"));
        assert!(
            optimized
                .statistics
                .iter()
                .any(|read| read.statistic == "hash_join_build_bytes(node:T)"),
            "{:?}",
            optimized.statistics
        );
    }
    let embedding_only = omnigraph_planner::optimizer::build_side_bytes(
        10,
        None,
        &schema(),
        Some(&["embedding".to_string()]),
        |_| None,
    );
    assert_eq!(
        embedding_only,
        Some(160),
        "4 x f32 per row, no data-file bytes needed"
    );
    let with_id = omnigraph_planner::optimizer::build_side_bytes(
        10,
        Some(1_160),
        &schema(),
        Some(&["__id".to_string(), "embedding".to_string()]),
        |_| None,
    );
    assert_eq!(
        with_id,
        Some(160 + 1_160 + 44),
        "file bytes are additive; decoded fixed widths cannot be subtracted from compressed data"
    );
    assert_eq!(
        omnigraph_planner::optimizer::build_side_bytes(
            10,
            Some(8),
            &schema(),
            Some(&["__id".to_string()]),
            |_| None,
        ),
        Some(8 + 44),
        "a compressed table smaller than its decoded fixed columns still charges variable storage"
    );
}

/// Unread vectors must not disqualify a narrow destination hash build.
#[test]
fn access_path_sizes_only_projected_columns() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let manifest = source_with_rows(Some(20_000))
        .with_expand_statistics(
            "knows",
            Direction::Out,
            ExpandStatistics {
                edge_count: 100_000,
                src_node_count: 20_000,
                dst_node_count: 20_000,
                same_type: true,
                max_frontier_cap: 1 << 20,
                max_hops_cap: 6,
            },
        )
        .with_query_memory_pool_bytes(POOL_BYTES)
        .with_table_data_bytes("node:T", 80 * 1024 * 1024);
    assert_eq!(
        omnigraph_planner::optimizer::column_statistics_needed(&op, &manifest).unwrap(),
        BTreeSet::from(["node:T".to_string()])
    );
    let source = manifest
        .with_column_data_bytes("node:T", "embedding", 78 * 1024 * 1024)
        .with_column_data_bytes("node:T", "__id", 512 * 1024)
        .with_column_data_bytes("node:T", "slug", 128 * 1024);
    let (plan, _) = physical(&op, &source);
    assert_eq!(access_paths(&plan).0, vec![AccessPath::HashJoin]);
    assert!(
        omnigraph_planner::optimizer::column_statistics_needed(&op, &source)
            .unwrap()
            .is_empty()
    );

    let wide = source.with_column_data_bytes("node:T", "slug", 40 * 1024 * 1024);
    let (plan, _) = physical(&op, &wide);
    assert_eq!(access_paths(&plan).0, vec![AccessPath::IdLookup]);
}

#[test]
fn hash_build_estimate_requires_all_projected_variable_statistics() {
    let projection = ["__id".to_string(), "slug".to_string()];
    let partial = |name: &str| (name == "__id").then_some(32);
    let estimate = omnigraph_planner::optimizer::build_side_bytes;
    assert_eq!(
        estimate(10, None, &schema(), Some(&projection), partial),
        None
    );
    assert_eq!(
        estimate(10, Some(1000), &schema(), Some(&projection), partial),
        Some(1088)
    );
    assert_eq!(
        omnigraph_planner::optimizer::build_side_bytes(
            10,
            None,
            &schema(),
            Some(&projection),
            |_| Some(32)
        ),
        Some(152)
    );

    let nested = Arc::new(Schema::new(vec![Field::new(
        "object",
        DataType::Struct(
            vec![
                Field::new("number", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    )]));
    assert_eq!(
        omnigraph_planner::optimizer::build_side_bytes(10, None, &nested, None, |_| None),
        None
    );
    assert_eq!(
        omnigraph_planner::optimizer::build_side_bytes(10, None, &nested, None, |_| Some(200)),
        Some(324)
    );
}

/// An equality on the whole `@key` reads at most one row, so a traversal from
/// it costs the indexed path and the id lookup whatever the type's size; any
/// other source filter leaves the type's row count.
#[test]
fn a_key_equality_on_the_source_scan_bounds_the_frontier_to_one_row() {
    let filtered = |property: &str| {
        ir(
            vec![
                IROp::NodeScan {
                    variable: "a".to_string(),
                    type_name: "T".to_string(),
                    filters: vec![IRFilter {
                        left: prop("a", property),
                        op: CompOp::Eq,
                        right: IRExpr::Literal(Literal::String("x".into())),
                    }],
                },
                expand("a", "b", vec![]),
            ],
            vec![prop("b", "slug")],
            vec![],
        )
    };
    let source = pooled(source_with_rows(Some(5_000))).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(5_000),
    );
    let (plan, _) = physical(&filtered("slug"), &source);
    assert_eq!(
        expand_modes(&plan),
        vec![(ExpandMode::IndexedScan, Some(1))]
    );
    assert_eq!(access_paths(&plan).0, vec![AccessPath::IdLookup]);
    let root_rows = |plan: &PhysicalPlan| {
        plan.live()
            .find_map(|(id, node)| match node {
                PhysicalNode::Scan {
                    source: omnigraph_planner::ScanInput::Table,
                    ..
                } => plan.properties(id).map(|p| p.rows),
                _ => None,
            })
            .expect("root scan properties")
    };
    assert_eq!(root_rows(&plan), omnigraph_planner::Estimate::Known(1));

    let (plan, _) = physical(&filtered("state"), &source);
    assert_eq!(expand_modes(&plan), vec![(ExpandMode::Csr, Some(5_000))]);
    assert_eq!(access_paths(&plan).0, vec![AccessPath::HashJoin]);
    assert_eq!(root_rows(&plan), omnigraph_planner::Estimate::Known(5_000));

    let (plan, _) = physical(&filtered("state"), &source_with_rows(None));
    assert_eq!(root_rows(&plan), omnigraph_planner::Estimate::Unknown);
}

/// Without statistics the frontier is unknown: the per-batch id lookup, no pass.
#[test]
fn no_statistics_keeps_the_id_lookup_and_records_no_access_pass() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let (plan, fired) = physical(&op, &source());
    let (paths, json) = access_paths(&plan);
    assert_eq!(paths, vec![AccessPath::IdLookup]);
    assert_eq!(json, "id_lookup");
    assert!(!fired.contains(&"access_path"), "{fired:?}");
    let (plan, fired) = physical(&op, &source_with_rows(Some(20)));
    let (paths, _) = access_paths(&plan);
    assert_eq!(
        paths,
        vec![AccessPath::IdLookup],
        "a row count without edge statistics leaves the frontier unknown"
    );
    assert!(!fired.contains(&"access_path"), "{fired:?}");
}

/// The source holds `T`'s row count, so the null estimate comes from the
/// missing edge statistics alone.
#[test]
fn no_statistics_records_csr_with_no_estimate() {
    let op = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("b", "slug")],
        vec![],
    );
    let (plan, fired) = physical(&op, &source_with_rows(Some(20)));
    assert_eq!(expand_modes(&plan), vec![(ExpandMode::Csr, None)]);
    assert!(!fired.contains(&"expand_mode"), "{fired:?}");
    let mut expands = Vec::new();
    expand_json(&plan.to_json(), &mut expands);
    assert_eq!(expands[0]["mode"], "csr");
    assert_eq!(
        expands[0].get("frontier_estimate"),
        Some(&serde_json::Value::Null)
    );
    assert!(matches!(
        plan.node(plan.post_order()[1]),
        Some(PhysicalNode::Expand {
            policy: ExpandPolicy::Uncosted,
            ..
        })
    ));
    assert_eq!(expands[0]["alternatives"], serde_json::json!([]));
}

/// Synthetic caps isolate the cold cost comparison from hard-cap decisions.
#[test]
fn two_hops_choose_a_cold_csr_below_both_caps() {
    let mut step = expand("a", "b", vec![]);
    if let IROp::Expand { max_hops, .. } = &mut step {
        *max_hops = Some(2);
    }
    let op = ir(vec![scan("a"), step], vec![prop("b", "slug")], vec![]);
    let source = source_with_rows(Some(1_000)).with_expand_statistics(
        "knows",
        Direction::Out,
        knows_statistics(1_000),
    );
    let (plan, _) = physical(&op, &source);
    assert_eq!(expand_modes(&plan), vec![(ExpandMode::Csr, Some(1_000))]);
    let cost = plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Expand { policy, .. } => policy.cost(),
            _ => None,
        })
        .expect("cost inputs");
    assert!(!cost.csr_cached);
    assert!(cost.frontier_rows <= cost.max_frontier_cap);
    assert!(cost.effective_max_hops <= cost.max_hops_cap);
}

use omnigraph_planner::{RuntimeFilterKind, RuntimeFilterSpec};

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
                    filters: vec![IRExpr::comparison(
                        prop("a", property),
                        CompOp::Eq,
                        IRExpr::Literal(Literal::String("x".into())),
                    )],
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

/// The bindings of the plan's join scans as `(left, right)`, with the join
/// node's name.
fn join_sides(plan: &PhysicalPlan) -> (String, String, &'static str) {
    let binding = |id| match plan.node(id) {
        Some(PhysicalNode::Scan { spec, .. }) => spec.binding.clone().expect("a bound scan"),
        other => panic!("a join side is a scan, not {other:?}"),
    };
    plan.live()
        .find_map(|(_, node)| match node {
            PhysicalNode::CrossJoin { left, right, .. }
            | PhysicalNode::ContainsJoin { left, right, .. } => {
                Some((binding(*left), binding(*right), node.name()))
            }
            _ => None,
        })
        .expect("a join")
}

/// `$m.title contains $w.title`, Matter written first: the searched Matter
/// scan streams on the right only when its known rows are at least Word's, and
/// the sides never swap when `$w.title contains $m.title` searches back too.
#[test]
fn a_contains_join_streams_the_searched_side_only_when_it_has_no_fewer_rows() {
    let contains = |haystack: &str, needle: &str| {
        IRExpr::comparison(
            prop(haystack, "title"),
            CompOp::StringContains,
            prop(needle, "title"),
        )
    };
    let query = |filter: IRExpr| {
        ir(
            vec![
                scan_of("m", "Matter"),
                scan_of("w", "Word"),
                IROp::Filter(filter),
            ],
            vec![prop("m", "slug"), prop("w", "slug")],
            vec![],
        )
    };
    let plan_sides = |op: &Operation, matters: Option<u64>, words: Option<u64>| {
        let source = MemorySource::default()
            .with_node_type("Matter", node_type("Matter", matters))
            .with_node_type("Word", node_type("Word", words));
        join_sides(&physical(op, &source).0)
    };
    let both_ways =
        query(IRExpr::and_all([contains("m", "w"), contains("w", "m")]).expect("two conjuncts"));
    assert_eq!(
        plan_sides(&both_ways, Some(1_000), Some(3)),
        ("m".to_string(), "w".to_string(), "ContainsJoin"),
        "a pair searching both ways keeps the written order"
    );
    let op = query(contains("m", "w"));
    let sides = |matters: Option<u64>, words: Option<u64>| plan_sides(&op, matters, words);
    let written = ("m".to_string(), "w".to_string(), "CrossJoin");
    let swapped = ("w".to_string(), "m".to_string(), "ContainsJoin");
    assert_eq!(
        sides(Some(3), Some(1_000)),
        written,
        "1,000 words collected as needles against 3 titles gain no filter"
    );
    assert_eq!(sides(Some(1_000), Some(3)), swapped);
    assert_eq!(
        sides(Some(3), Some(3)),
        swapped,
        "ties stream the searched side"
    );
    assert_eq!(sides(None, Some(3)), written);
    assert_eq!(sides(Some(1_000), None), written);
}

/// `$p.text contains $m.title` with the searched Passage scan on the right
/// plans a `ContainsJoin` with the other conjunct as residual and the scan
/// marked, under `join_algorithm`; a non-text `title` keeps the `CrossJoin`.
#[test]
fn a_contains_join_marks_its_right_scan_and_keeps_the_other_conjunct() {
    let contains = IRExpr::comparison(
        prop("p", "text"),
        CompOp::StringContains,
        prop("m", "title"),
    );
    let other = IRExpr::comparison(prop("m", "slug"), CompOp::Ne, prop("p", "slug"));
    let op = ir(
        vec![
            scan_of("m", "Matter"),
            scan_of("p", "Passage"),
            IROp::Filter(IRExpr::and_all([contains.clone(), other.clone()]).expect("two")),
        ],
        vec![prop("m", "slug"), prop("p", "slug")],
        vec![],
    );
    let typed = |title: DataType| {
        let mut matter = node_type("Matter", None);
        let fields: Vec<Field> = schema()
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "title" => Field::new("title", title.clone(), true),
                _ => field.as_ref().clone(),
            })
            .collect();
        matter.schema = Arc::new(Schema::new(fields));
        MemorySource::default()
            .with_node_type("Matter", matter)
            .with_node_type("Passage", node_type("Passage", None))
    };
    let (plan, fired) = physical(&op, &typed(DataType::Utf8));
    assert!(fired.contains(&"join_algorithm"), "{fired:?}");
    let (join, node) = plan
        .live()
        .find(|(_, node)| matches!(node, PhysicalNode::ContainsJoin { .. }))
        .expect("a ContainsJoin");
    let PhysicalNode::ContainsJoin {
        right,
        haystack,
        needle,
        residual,
        ..
    } = node
    else {
        unreachable!("selected above");
    };
    assert_eq!(haystack, &("p".to_string(), "text".to_string()));
    assert_eq!(needle, &("m".to_string(), "title".to_string()));
    assert_eq!(residual, std::slice::from_ref(&other));
    let Some(PhysicalNode::Scan { spec, .. }) = plan.node(*right) else {
        panic!("the right side is a scan");
    };
    assert_eq!(spec.binding.as_deref(), Some("p"));
    assert_eq!(
        spec.runtime_filter,
        Some(RuntimeFilterSpec {
            column: "text".to_string(),
            needle: ("m".to_string(), "title".to_string()),
            kind: RuntimeFilterKind::TextContainsAny,
        })
    );
    let json = plan.to_json();
    let printed = find_node(&json, "ContainsJoin").expect("the join prints");
    assert_eq!(printed["id"], join);
    assert_eq!(printed["haystack"], "$p.text");
    assert_eq!(printed["needle"], "$m.title");
    assert_eq!(printed["residual"], serde_json::json!([other.to_string()]));
    let scan = &printed["inputs"][1];
    assert_eq!(scan["binding"], "p");
    assert_eq!(scan["runtime_filter"]["column"], "text");
    assert_eq!(
        scan["runtime_filter"]["needle"],
        serde_json::json!(["m", "title"])
    );
    assert_eq!(scan["runtime_filter"]["kind"], "text_contains_any");
    assert!(
        printed["inputs"][0].get("runtime_filter").is_none(),
        "the collected side carries no filter"
    );

    let (plan, fired) = physical(&op, &typed(DataType::Int64));
    assert!(!fired.contains(&"join_algorithm"), "{fired:?}");
    let filters = plan
        .live()
        .find_map(|(_, node)| match node {
            PhysicalNode::CrossJoin { filters, .. } => Some(filters.clone()),
            _ => None,
        })
        .expect("a CrossJoin over a numeric needle");
    assert_eq!(filters, [contains, other]);
    let marked: Vec<Option<String>> = plan
        .live()
        .filter_map(|(_, node)| match node {
            PhysicalNode::Scan { spec, .. } if spec.runtime_filter.is_some() => {
                Some(spec.binding.clone())
            }
            _ => None,
        })
        .collect();
    assert!(marked.is_empty(), "no scan is marked, but {marked:?} are");
}

/// The first node named `name` in pre-order.
fn find_node<'j>(node: &'j serde_json::Value, name: &str) -> Option<&'j serde_json::Value> {
    if node["node"] == name {
        return Some(node);
    }
    node["inputs"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|input| find_node(input, name))
}

/// The physical scan of `binding`'s projection after every pass.
fn physical_projection(plan: &PhysicalPlan, binding: &str) -> BTreeSet<String> {
    plan.live()
        .find_map(|(_, node)| match node {
            PhysicalNode::Scan { spec, .. } if spec.binding.as_deref() == Some(binding) => {
                spec.projection.clone()
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no physical scan bound to `{binding}`"))
        .into_iter()
        .collect()
}

/// One hydrated binding: its name and `(return position, property)` per
/// fetched column.
type Hydrated = (String, Vec<(usize, String)>);

/// The root `HydrateColumns`: per binding, the properties it fetches with
/// the return positions they fill.
fn root_hydration(plan: &PhysicalPlan) -> Option<Vec<Hydrated>> {
    match plan.node(plan.root()) {
        Some(PhysicalNode::HydrateColumns { bindings, .. }) => Some(
            bindings
                .iter()
                .map(|binding| {
                    (
                        binding.binding.clone(),
                        binding
                            .columns
                            .iter()
                            .map(|column| (column.position, column.property.clone()))
                            .collect(),
                    )
                })
                .collect(),
        ),
        _ => None,
    }
}

/// A top-k over a table four times its limit or larger, or of unknown size,
/// fetches the column only its output reads by row address above the limit;
/// the scan reads the row address in its place. The logical plan keeps the
/// column, and a table within four times the limit keeps it on the scan.
#[test]
fn a_top_k_fetches_its_return_only_column_by_row_address() {
    let op = ir(
        vec![scan("c")],
        vec![prop("c", "slug"), prop("c", "body"), prop("c", "rank")],
        vec![prop("c", "rank")],
    );
    for rows in [Some(41), None] {
        let source = source_with_rows(rows);
        let mut logical = resolve(&op, &source).expect("resolve");
        let fired = rewrite(&mut logical, &source).expect("rewrite");
        let optimized = omnigraph_planner::physical_plan(&mut logical, &source, &bounds(), fired)
            .expect("lower");
        let plan = optimized.physical;
        assert!(
            optimized.fired.contains(&"late_materialization"),
            "{rows:?}: {:?}",
            optimized.fired
        );
        assert_eq!(
            root_hydration(&plan),
            Some(vec![("c".to_string(), vec![(1, "body".to_string())])]),
            "{rows:?}"
        );
        assert_eq!(
            physical_projection(&plan, "c"),
            set(&["__id", "slug", "rank", "_rowaddr"])
        );
        assert_eq!(
            projection_of(&logical, "c"),
            set(&["__id", "slug", "rank", "body"])
        );
        let json = plan.to_json();
        assert_eq!(json["node"], "HydrateColumns");
        assert_eq!(json["bindings"][0]["columns"], serde_json::json!(["body"]));
        assert!(json["properties"]["retained_limit"].as_u64().is_some());
    }
    let (plan, fired) = physical(&op, &source_with_rows(Some(40)));
    assert!(!fired.contains(&"late_materialization"), "{fired:?}");
    assert_eq!(root_hydration(&plan), None);
    assert_eq!(
        physical_projection(&plan, "c"),
        set(&["__id", "slug", "rank", "body"])
    );
}

/// Under a limit, a scan whose row estimate is above four times the limit
/// hydrates its return-only column, a bare table scan as much as a traversal
/// destination: Lance reads ahead of a consumer that stops early. A scan a key
/// equality bounds to one row keeps its column.
#[test]
fn a_limit_hydrates_every_large_scan_but_not_a_key_lookup() {
    let bare = ir(vec![scan("c")], vec![prop("c", "body")], vec![]);
    let (plan, fired) = physical(&bare, &source_with_rows(Some(1_000)));
    assert!(fired.contains(&"late_materialization"), "{fired:?}");
    assert_eq!(
        root_hydration(&plan),
        Some(vec![("c".to_string(), vec![(0, "body".to_string())])])
    );

    let lookup = ir(
        vec![IROp::NodeScan {
            variable: "c".to_string(),
            type_name: "T".to_string(),
            filters: vec![IRExpr::comparison(
                prop("c", "slug"),
                CompOp::Eq,
                IRExpr::Literal(Literal::String("one".to_string())),
            )],
        }],
        vec![prop("c", "body")],
        vec![],
    );
    let (plan, fired) = physical(&lookup, &source_with_rows(Some(1_000)));
    assert!(!fired.contains(&"late_materialization"), "{fired:?}");
    assert_eq!(root_hydration(&plan), None);

    let traversal = ir(
        vec![scan("a"), expand("a", "b", vec![])],
        vec![prop("a", "slug"), prop("b", "body")],
        vec![],
    );
    let (plan, fired) = physical(&traversal, &source_with_rows(Some(1_000)));
    assert!(fired.contains(&"late_materialization"), "{fired:?}");
    assert_eq!(
        root_hydration(&plan),
        Some(vec![("b".to_string(), vec![(1, "body".to_string())])])
    );
    assert!(physical_projection(&plan, "b").contains("_rowaddr"));
    assert!(!physical_projection(&plan, "b").contains("body"));
}

/// Only a bare return-only property is hydrated: the key, a sort key, a
/// column a filter reads, a computed return (named or not) and a return the
/// sort orders by alias stay on the scan, and an unnamed computed return does
/// not keep the others from hydrating.
#[test]
fn hydration_keeps_every_column_something_besides_the_output_reads() {
    let op = Operation::Query(Box::new(QueryIR {
        name: "q".to_string(),
        params: vec![],
        pipeline: vec![IROp::NodeScan {
            variable: "c".to_string(),
            type_name: "T".to_string(),
            filters: vec![IRExpr::comparison(
                prop("c", "state"),
                CompOp::Eq,
                IRExpr::Literal(Literal::String("open".to_string())),
            )],
        }],
        return_exprs: vec![
            IRProjection {
                expr: prop("c", "slug"),
                alias: None,
            },
            IRProjection {
                expr: prop("c", "rank"),
                alias: None,
            },
            IRProjection {
                expr: prop("c", "state"),
                alias: None,
            },
            IRProjection {
                expr: prop("c", "title"),
                alias: Some("t".to_string()),
            },
            IRProjection {
                expr: IRExpr::comparison(
                    prop("c", "kind"),
                    CompOp::Eq,
                    IRExpr::Literal(Literal::String("x".to_string())),
                ),
                alias: Some("is_x".to_string()),
            },
            IRProjection {
                expr: IRExpr::comparison(
                    prop("c", "edits"),
                    CompOp::Eq,
                    IRExpr::Literal(Literal::String("0".to_string())),
                ),
                alias: None,
            },
            IRProjection {
                expr: prop("c", "body"),
                alias: Some("text".to_string()),
            },
        ],
        order_by: vec![
            IROrdering {
                expr: prop("c", "rank"),
                descending: false,
            },
            IROrdering {
                expr: IRExpr::AliasRef("t".to_string()),
                descending: true,
            },
        ],
        limit: Some(5),
    }));
    let (plan, fired) = physical(&op, &source_with_rows(Some(1_000)));
    assert!(fired.contains(&"late_materialization"), "{fired:?}");
    assert_eq!(
        root_hydration(&plan),
        Some(vec![("c".to_string(), vec![(6, "body".to_string())])])
    );
    assert_eq!(
        physical_projection(&plan, "c"),
        set(&[
            "__id", "slug", "rank", "state", "title", "kind", "edits", "_rowaddr"
        ])
    );
}

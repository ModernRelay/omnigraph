//! A `load csv` statement resolves to `CsvSource → Validate → FragmentWrite`,
//! the one validation carrying every check in refusal order, and routes to
//! the engine under overwrite. Rust and not `.gqt`: the claims are the node
//! chain, accepted statistics and route decisions. GQT owns the rendered
//! access choices on stored tables.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::ir::{ValidateCheck, lower_load};
use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{SYSTEM_COLUMNS_V3, build_catalog};
use omnigraph_planner::optimizer::resolve;
use omnigraph_planner::{
    BoundPlan, Decision, LogicalNode, MemorySource, Operation, PROBE_CHUNK_KEYS, PhysicalNode,
    PhysicalPlan, RouteOverride, SOURCE_PARTITION_BYTES, Side, SideId, TableRef, Unrouted,
    ValidateAccess, ValueTable, route,
};

use crate::fixture_bounds::BOUNDS;

fn operation(target: LoadTarget, mode: LoadStmtMode, source_bytes: u64) -> Operation {
    let schema = parse_schema(
        r#"
node Person {
    id: String @key
    email: String @unique
}
edge Knows: Person -> Person {
    w: I64
}
"#,
    )
    .expect("schema parses");
    let catalog = build_catalog(&schema).expect("catalog builds");
    let stmt = LoadStmt {
        source: "s3://bucket/people.csv".to_string(),
        target,
        mode,
        delimiter: b',',
        header: true,
        columns: None,
    };
    Operation::Load {
        ir: Box::new(lower_load(&catalog, &stmt)),
        tables: vec![side("node:Person", 7), side("edge:Knows", 3)],
        source_bytes,
    }
}

fn side(type_key: &str, version: u64) -> Side {
    Side {
        table: TableRef::dataset(type_key, type_key.replace(':', "/"), None),
        version,
        columns: SYSTEM_COLUMNS_V3,
    }
}

fn source() -> MemorySource {
    MemorySource::default().with_schema(
        SideId::Child,
        Arc::new(Schema::new(vec![
            Field::new(SYSTEM_COLUMNS_V3.id, DataType::Utf8, false),
            Field::new("email", DataType::Utf8, true),
        ])),
    )
}

/// The physical nodes from the root down to the leaf.
fn chain(plan: &PhysicalPlan) -> Vec<&PhysicalNode> {
    let mut nodes = Vec::new();
    let mut id = Some(plan.root());
    while let Some(current) = id {
        let node = plan.node(current).expect("live node");
        nodes.push(node);
        id = node.inputs().first().copied();
    }
    nodes
}

/// Every check of the plan's one validation with its physical access.
fn accesses(plan: &PhysicalPlan) -> Vec<(String, ValidateAccess)> {
    let validations: Vec<&PhysicalNode> = chain(plan)
        .into_iter()
        .filter(|node| matches!(node, PhysicalNode::Validate { .. }))
        .collect();
    let [PhysicalNode::Validate { checks, .. }] = &validations[..] else {
        panic!("one validation per load plan, found {}", validations.len());
    };
    checks
        .iter()
        .map(|spec| {
            (
                spec.check.name().to_string(),
                spec.access.expect("a physical check carries its access"),
            )
        })
        .collect()
}

#[test]
fn node_overwrite_resolves_to_source_validate_chain_and_write() {
    let op = operation(
        LoadTarget::Node("Person".into()),
        LoadStmtMode::Overwrite,
        4096,
    );
    let logical = resolve(&op, &source()).expect("load resolves");
    let mut id = Some(logical.root());
    let mut kinds = Vec::new();
    while let Some(current) = id {
        let node = logical.node(current).expect("live node");
        kinds.push(match node {
            LogicalNode::FragmentWrite { spec, .. } => {
                assert_eq!(spec.table, "node:Person");
                assert_eq!(spec.version, Some(7));
                "FragmentWrite".to_string()
            }
            LogicalNode::Validate { table, checks, .. } => {
                assert_eq!(table, "node:Person");
                let checks: Vec<String> = checks
                    .iter()
                    .map(|spec| {
                        assert_eq!(spec.access, None, "a logical check has no access yet");
                        let consults = spec
                            .consults
                            .as_ref()
                            .map(|image| format!("{}@{:?}", image.table, image.version));
                        format!("{} {consults:?}", spec.check.name())
                    })
                    .collect();
                format!("Validate({})", checks.join(", "))
            }
            LogicalNode::CsvSource { spec } => {
                assert_eq!(spec.estimated_rows, 4096 / 4);
                assert_eq!(spec.version, Some(7));
                "CsvSource".to_string()
            }
            other => panic!("unexpected node {:?}", other.kind()),
        });
        id = node.inputs().first().copied();
    }
    assert_eq!(
        kinds,
        [
            "FragmentWrite",
            "Validate(value None, unique None, unique None, \
             retained_edges Some(\"edge:Knows@Some(3)\"), \
             retained_edges Some(\"edge:Knows@Some(3)\"))",
            "CsvSource",
        ]
    );
    let Decision::Engine { plan, explain, .. } =
        route(&op, &source(), RouteOverride::Registry, &BOUNDS)
    else {
        panic!("an overwrite load routes to the engine");
    };
    assert_eq!(
        accesses(&plan),
        [
            ("value".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Delta),
            ("retained_edges".to_string(), ValidateAccess::Probe),
            ("retained_edges".to_string(), ValidateAccess::Probe),
        ]
    );
    let document = explain.to_value();
    assert_eq!(document["explain_version"], 7);
    assert!(document["logical_plan_initial"].is_object());
    assert_eq!(document["route"], "engine");
    assert_eq!(document["operation"]["kind"], "load");
    assert_eq!(document["operation"]["mode"], "overwrite");
    assert_eq!(
        document["operation"]["tables"][0]["type_key"],
        "node:Person"
    );
    assert_eq!(document["physical_plan"]["node"], "FragmentWrite");
    assert_eq!(document["physical_plan"]["adapter"], "fragments");
    assert_eq!(
        document["physical_plan"]["max_input_bytes"],
        BOUNDS.load_write_max_bytes
    );
    let validation = &document["logical_plan"]["inputs"][0];
    assert_eq!(validation["node"], "Validate");
    assert_eq!(validation["table"], "node:Person");
    assert_eq!(validation["checks"].as_array().map(Vec::len), Some(5));
    assert!(validation["checks"][0]["access"].is_null());
    assert_eq!(
        document["physical_plan"]["inputs"][0]["checks"][4]["access"],
        "probe"
    );
    assert_eq!(
        document["physical_plan"]["inputs"][0]["removal"], "scan",
        "an unknown target count keeps the removal scan"
    );
    let leaf = &validation["inputs"][0];
    assert_eq!(leaf["node"], "CsvSource");
    assert_eq!(leaf["source"], "s3://bucket/people.csv");
    let pipelines = document["pipelines"]
        .as_array()
        .expect("a load plan has pipelines");
    assert_eq!(
        pipelines.len(),
        2,
        "the whole-input validation cuts the load in two"
    );
    assert_eq!(pipelines[0]["source"]["node"], "CsvSource");
    assert_eq!(pipelines[0]["operators"], serde_json::json!([]));
    assert_eq!(pipelines[0]["sink"]["name"], "Validate");
    assert_eq!(pipelines[1]["source"]["node"], "Validate");
    assert_eq!(
        pipelines[1]["source"]["id"], pipelines[0]["sink"]["id"],
        "the breaker is the sink of the first pipeline and the source of the second"
    );
    assert_eq!(pipelines[1]["operators"][0]["name"], "FragmentWrite");
    assert_eq!(pipelines[1]["sink"]["name"], "consumer");
    assert_eq!(pipelines[1]["depends_on"], serde_json::json!([0]));
}

/// A node target with an integer key and no incoming edge type carries only
/// checks judged as the rows pass and no removal scan: its validation streams
/// and the load is one pipeline; the whole-input validation above stays two.
#[test]
fn per_row_checks_plan_one_pipeline_through_the_write() {
    use omnigraph_planner::{PipelineShape, validate_streams};

    let schema = parse_schema(
        r#"
node Item {
    id: I64 @key
    note: String
}
"#,
    )
    .expect("schema parses");
    let catalog = build_catalog(&schema).expect("catalog builds");
    let stmt = LoadStmt {
        source: "s3://bucket/items.csv".to_string(),
        target: LoadTarget::Node("Item".into()),
        mode: LoadStmtMode::Overwrite,
        delimiter: b',',
        header: true,
        columns: None,
    };
    let op = Operation::Load {
        ir: Box::new(lower_load(&catalog, &stmt)),
        tables: vec![side("node:Item", 7)],
        source_bytes: 3 * SOURCE_PARTITION_BYTES,
    };
    let Decision::Engine { plan, explain, .. } =
        route(&op, &source(), RouteOverride::Registry, &BOUNDS)
    else {
        panic!("an overwrite load routes to the engine");
    };
    let validations: Vec<&PhysicalNode> = chain(&plan)
        .into_iter()
        .filter(|node| matches!(node, PhysicalNode::Validate { .. }))
        .collect();
    let [validation] = &validations[..] else {
        panic!("one validation per load plan, found {}", validations.len());
    };
    let PhysicalNode::Validate {
        checks, removal, ..
    } = validation
    else {
        unreachable!();
    };
    assert!(validate_streams(checks, *removal), "{checks:?} {removal:?}");
    assert_eq!(validation.shape(), PipelineShape::Streaming);
    let document = explain.to_value();
    let pipelines = document["pipelines"]
        .as_array()
        .expect("a load plan has pipelines");
    assert_eq!(
        pipelines.len(),
        1,
        "a validation judged per row cuts nothing: {pipelines:?}"
    );
    assert_eq!(pipelines[0]["source"]["node"], "CsvSource");
    assert_eq!(pipelines[0]["partitions"], 3);
    let operators: Vec<&str> = pipelines[0]["operators"]
        .as_array()
        .expect("operators")
        .iter()
        .map(|operator| operator["name"].as_str().expect("a name"))
        .collect();
    assert_eq!(operators, ["Validate", "FragmentWrite"]);
    assert_eq!(pipelines[0]["sink"]["name"], "consumer");
    assert_eq!(pipelines[0]["depends_on"], serde_json::json!([]));
}

/// A node target whose manifest count is zero removes nothing, so pass 9 plans
/// neither the removal scan nor the retained-edge checks; a populated or
/// uncounted target keeps both; an edge overwrite never scans.
#[test]
fn empty_overwrite_target_plans_no_removal_and_no_retained_edges() {
    use omnigraph_planner::{NodeTypeSpec, ValidateRemoval};

    let schema = parse_schema(
        r#"
node Person {
    id: String @key
    email: String @unique
}
edge Knows: Person -> Person {
    w: I64
}
"#,
    )
    .unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let node = catalog.node_types.get("Person").unwrap();
    let person = |rows: Option<u64>| NodeTypeSpec {
        table: side("node:Person", 7).table,
        version: Some(7),
        columns: SYSTEM_COLUMNS_V3,
        schema: node.arrow_schema.clone(),
        key: node.key.clone().unwrap_or_default(),
        object_columns: node
            .node_object_fields()
            .map(|f| f.name().clone())
            .collect(),
        object_fields: node
            .node_object_members()
            .map(|(member, field)| {
                Field::new(member, field.data_type().clone(), field.is_nullable())
            })
            .collect(),
        row_count: rows,
    };
    let validation = |plan: &PhysicalPlan| -> (ValidateRemoval, usize) {
        let validations: Vec<&PhysicalNode> = chain(plan)
            .into_iter()
            .filter(|node| matches!(node, PhysicalNode::Validate { .. }))
            .collect();
        let [
            PhysicalNode::Validate {
                removal, checks, ..
            },
        ] = &validations[..]
        else {
            panic!("one validation per load plan");
        };
        let retained = checks
            .iter()
            .filter(|spec| matches!(spec.check, ValidateCheck::RetainedEdges { .. }))
            .count();
        (*removal, retained)
    };
    let op = operation(
        LoadTarget::Node("Person".into()),
        LoadStmtMode::Overwrite,
        4096,
    );
    for (rows, removal, retained) in [
        (Some(0), ValidateRemoval::None, 0),
        (Some(3), ValidateRemoval::Scan, 2),
        (None, ValidateRemoval::Scan, 2),
    ] {
        let source = source().with_node_type("Person", person(rows));
        let Decision::Engine { plan, explain, .. } =
            route(&op, &source, RouteOverride::Registry, &BOUNDS)
        else {
            panic!("overwrite routes to the engine")
        };
        assert_eq!(validation(&plan), (removal, retained), "rows {rows:?}");
        let document = explain.to_value();
        assert_eq!(
            document["physical_plan"]["inputs"][0]["removal"],
            removal.as_str(),
            "rows {rows:?}"
        );
        assert_eq!(
            document["logical_plan"]["inputs"][0]["checks"]
                .as_array()
                .map(Vec::len),
            Some(5),
            "the logical validation keeps every check; pass 9 prunes"
        );
    }
    let op = operation(
        LoadTarget::Edge("Knows".into()),
        LoadStmtMode::Overwrite,
        4096,
    );
    let source = source().with_node_type("Person", person(Some(3)));
    let Decision::Engine { plan, .. } = route(&op, &source, RouteOverride::Registry, &BOUNDS)
    else {
        panic!("overwrite routes to the engine")
    };
    assert_eq!(validation(&plan), (ValidateRemoval::None, 0));
}

/// One partition per `SOURCE_PARTITION_BYTES`, capped by the bounds' load
/// workers, carried by the pipeline the source heads while the breaker's
/// runs one; GQT fixtures stay under one partition, so the arithmetic is here.
#[test]
fn source_partitions_are_the_slices_capped_by_the_load_workers() {
    for (source_bytes, partitions) in [
        (4096, 1),
        (SOURCE_PARTITION_BYTES, 1),
        (SOURCE_PARTITION_BYTES + 1, 2),
        (3 * SOURCE_PARTITION_BYTES, 3),
        (100 * SOURCE_PARTITION_BYTES, BOUNDS.load_workers_max),
    ] {
        let op = operation(
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Overwrite,
            source_bytes,
        );
        let Decision::Engine { plan, explain, .. } =
            route(&op, &source(), RouteOverride::Registry, &BOUNDS)
        else {
            panic!("an overwrite load routes to the engine");
        };
        let Some(PhysicalNode::CsvSource { max_partitions, .. }) = chain(&plan).pop() else {
            panic!("the leaf is the source");
        };
        assert_eq!(*max_partitions, partitions, "{source_bytes} bytes");
        let document = explain.to_value();
        assert_eq!(
            document["pipelines"][0]["partitions"], partitions,
            "{source_bytes} bytes"
        );
        assert_eq!(
            document["pipelines"][1]["partitions"], 1,
            "{source_bytes} bytes"
        );
    }
}

#[test]
fn endpoint_checks_keep_probe_without_consulted_statistics() {
    let mapped_columns = ["from", "to", "w"].len() as u64;
    let narrowest_row = 2 * mapped_columns;
    let within = PROBE_CHUNK_KEYS * narrowest_row;
    let past = within + narrowest_row;
    for (mode, source_bytes, expected) in [
        (LoadStmtMode::Overwrite, within, ValidateAccess::Probe),
        (LoadStmtMode::Overwrite, past, ValidateAccess::Probe),
        (LoadStmtMode::Append, past, ValidateAccess::Probe),
        (LoadStmtMode::Merge, past, ValidateAccess::Probe),
    ] {
        let op = operation(LoadTarget::Edge("Knows".into()), mode, source_bytes);
        let decision = route(&op, &source(), RouteOverride::Registry, &BOUNDS);
        let (plan, explain) = match (mode, decision) {
            (LoadStmtMode::Overwrite, Decision::Engine { plan, explain, .. }) => {
                (Some(plan), explain)
            }
            (
                LoadStmtMode::Append | LoadStmtMode::Merge,
                Decision::Executor {
                    reason,
                    explain,
                    logical,
                },
            ) => {
                assert_eq!(
                    reason,
                    Unrouted::ModeOnExecutor {
                        mode: mode.as_str()
                    }
                );
                assert!(logical.node(logical.root()).is_some());
                let value = explain.to_value();
                assert_eq!(value["route"], "executor");
                assert_eq!(value["reason"]["kind"], "mode_on_executor");
                assert_eq!(value["physical_plan"]["node"], "FragmentWrite");
                (None, explain)
            }
            (mode, other) => panic!("{} under {mode:?} decided {other:?}", op.kind()),
        };
        let value = explain.to_value();
        let validation = &value["physical_plan"]["inputs"][0];
        assert_eq!(
            validation["node"], "Validate",
            "{mode:?} at {source_bytes} bytes"
        );
        let validations: Vec<(String, String)> = validation["checks"]
            .as_array()
            .expect("the validation lists its checks")
            .iter()
            .map(|check| {
                (
                    check["check"].as_str().unwrap_or_default().to_string(),
                    check["access"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let expected_word = expected.as_str().to_string();
        assert_eq!(
            validations,
            [
                ("value".to_string(), "delta".to_string()),
                ("edge_ri".to_string(), expected_word.clone()),
                ("edge_ri".to_string(), expected_word),
            ],
            "{mode:?} at {source_bytes} bytes"
        );
        if let Some(plan) = plan {
            assert_eq!(
                accesses(&plan)
                    .into_iter()
                    .filter(|(check, _)| check == "edge_ri")
                    .map(|(_, access)| access)
                    .collect::<Vec<_>>(),
                [expected, expected]
            );
        }
    }
}

#[test]
fn load_nodes_survive_the_bound_plan_mirror() {
    let op = operation(
        LoadTarget::Node("Person".into()),
        LoadStmtMode::Overwrite,
        4096,
    );
    let Decision::Engine { plan, .. } = route(&op, &source(), RouteOverride::Registry, &BOUNDS)
    else {
        panic!("an overwrite load routes to the engine");
    };
    let bound = BoundPlan {
        plan,
        values: ValueTable::default(),
    };
    let text = serde_json::to_string(&bound).expect("bound plan serializes");
    let back: BoundPlan = serde_json::from_str(&text).expect("bound plan reads back");
    assert_eq!(back, bound);
    assert!(matches!(
        back.plan.node(back.plan.root()),
        Some(PhysicalNode::FragmentWrite { .. })
    ));
    let _ = ValidateCheck::Value {
        columns: Vec::new(),
    };
}

/// Synthetic snapshot counts make the table-size and unknown-statistics
/// boundaries deterministic without loading a 100-million-row fixture.
#[test]
fn endpoint_access_costs_the_consulted_table_and_index_coverage() {
    use omnigraph_planner::{FragmentCoverage, IndexFact, IndexKind, NodeTypeSpec};

    let input_rows = 10_000;
    let op = operation(
        LoadTarget::Edge("Knows".into()),
        LoadStmtMode::Overwrite,
        input_rows * 6,
    );
    for (rows, coverage, usable, expected) in [
        (Some(100_000_000), Some((1, 1)), true, ValidateAccess::Probe),
        (Some(1_000), Some((1, 1)), true, ValidateAccess::Scan),
        (Some(1_000), Some((0, 1)), true, ValidateAccess::Scan),
        (Some(1_000), Some((1, 2)), true, ValidateAccess::Scan),
        (
            Some(100_000_000),
            Some((25, 26)),
            true,
            ValidateAccess::Probe,
        ),
        (Some(1_000), None, true, ValidateAccess::Scan),
        (Some(1_000), Some((1, 1)), false, ValidateAccess::Scan),
        (None, Some((1, 1)), true, ValidateAccess::Probe),
    ] {
        let schema = parse_schema("node Person { id: String @key }").unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let node = catalog.node_types.get("Person").unwrap();
        let source = source()
            .with_node_type(
                "Person",
                NodeTypeSpec {
                    table: side("node:Person", 7).table,
                    version: Some(7),
                    columns: SYSTEM_COLUMNS_V3,
                    schema: node.arrow_schema.clone(),
                    key: node.key.clone().unwrap_or_default(),
                    object_columns: node
                        .node_object_fields()
                        .map(|f| f.name().clone())
                        .collect(),
                    object_fields: node
                        .node_object_members()
                        .map(|(member, field)| {
                            Field::new(member, field.data_type().clone(), field.is_nullable())
                        })
                        .collect(),
                    row_count: rows,
                },
            )
            .with_index_facts(
                "node:Person",
                vec![IndexFact {
                    name: "id_index".into(),
                    column: SYSTEM_COLUMNS_V3.id.into(),
                    kind: IndexKind::Btree { usable },
                    coverage: coverage.map(|(covered, total)| FragmentCoverage { covered, total }),
                }],
            );
        let Decision::Engine { plan, explain, .. } =
            route(&op, &source, RouteOverride::Registry, &BOUNDS)
        else {
            panic!("overwrite routes to the engine")
        };
        assert_eq!(
            accesses(&plan)
                .into_iter()
                .filter(|(kind, _)| kind == "edge_ri")
                .map(|(_, access)| access)
                .collect::<Vec<_>>(),
            [expected, expected],
            "rows={rows:?}, coverage={coverage:?}, usable={usable}",
        );
        let document = explain.to_value().to_string();
        assert!(document.contains("consulted_rows"), "{document}");
        assert!(document.contains("index_full_coverage"), "{document}");
        assert!(document.contains("probe_calls"), "{document}");
        if usable && coverage.is_none_or(|(covered, total)| covered > 0 && covered < total) {
            let stats: Vec<_> = explain
                .statistics
                .as_ref()
                .unwrap()
                .iter()
                .filter(|stat| stat.statistic == "validate_access(edge_ri)")
                .collect();
            assert_eq!(stats.len(), 2);
            for stat in stats {
                let origin: serde_json::Value = serde_json::from_str(&stat.origin).unwrap();
                assert_eq!(origin["index_coverage"], "partial");
                assert_eq!(origin["index_full_coverage"], false);
                assert_eq!(origin["probe_cost"], 90_000u64);
                assert_eq!(origin["scan_cost"], rows.unwrap() + input_rows);
            }
        }
    }
}

/// The endpoint checks probe on the parse partitions only when both read
/// their set whole over a one-integer `@key`; a probe access or a text key
/// keeps the judged route, and the decision is recorded with its inputs.
#[test]
fn endpoint_probe_is_swept_only_for_scan_over_integer_keys() {
    use omnigraph_planner::{EndpointProbe, FragmentCoverage, IndexFact, IndexKind, NodeTypeSpec};

    let input_rows = 10_000;
    let op = operation(
        LoadTarget::Edge("Knows".into()),
        LoadStmtMode::Overwrite,
        input_rows * 6,
    );
    for (key, rows, expected_access, expected_probe) in [
        (
            "I64",
            Some(1_000),
            ValidateAccess::Scan,
            EndpointProbe::Swept,
        ),
        (
            "I32",
            Some(1_000),
            ValidateAccess::Scan,
            EndpointProbe::Swept,
        ),
        (
            "String",
            Some(1_000),
            ValidateAccess::Scan,
            EndpointProbe::Judged,
        ),
        (
            "I64",
            Some(100_000_000),
            ValidateAccess::Probe,
            EndpointProbe::Judged,
        ),
    ] {
        let schema = parse_schema(&format!("node Person {{ id: {key} @key }}")).unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let node = catalog.node_types.get("Person").unwrap();
        let source = source()
            .with_node_type(
                "Person",
                NodeTypeSpec {
                    table: side("node:Person", 7).table,
                    version: Some(7),
                    columns: SYSTEM_COLUMNS_V3,
                    schema: node.arrow_schema.clone(),
                    key: node.key.clone().unwrap_or_default(),
                    object_columns: node
                        .node_object_fields()
                        .map(|f| f.name().clone())
                        .collect(),
                    object_fields: node
                        .node_object_members()
                        .map(|(member, field)| {
                            Field::new(member, field.data_type().clone(), field.is_nullable())
                        })
                        .collect(),
                    row_count: rows,
                },
            )
            .with_index_facts(
                "node:Person",
                vec![IndexFact {
                    name: "id_index".into(),
                    column: SYSTEM_COLUMNS_V3.id.into(),
                    kind: IndexKind::Btree { usable: true },
                    coverage: Some(FragmentCoverage {
                        covered: 1,
                        total: 1,
                    }),
                }],
            );
        let Decision::Engine { plan, explain, .. } =
            route(&op, &source, RouteOverride::Registry, &BOUNDS)
        else {
            panic!("overwrite routes to the engine")
        };
        let [PhysicalNode::Validate { checks, .. }] = chain(&plan)
            .into_iter()
            .filter(|node| matches!(node, PhysicalNode::Validate { .. }))
            .collect::<Vec<_>>()[..]
        else {
            panic!("one validation per load plan");
        };
        let endpoints: Vec<(ValidateAccess, Option<EndpointProbe>)> = checks
            .iter()
            .filter(|spec| spec.check.name() == "edge_ri")
            .map(|spec| (spec.access.unwrap(), spec.probe))
            .collect();
        assert_eq!(
            endpoints,
            [
                (expected_access, Some(expected_probe)),
                (expected_access, Some(expected_probe))
            ],
            "key={key}, rows={rows:?}"
        );
        assert!(
            checks
                .iter()
                .filter(|spec| spec.check.name() != "edge_ri")
                .all(|spec| spec.probe.is_none()),
            "only edge_ri checks carry a probe"
        );
        let document = explain.to_value();
        let validation = &document["physical_plan"]["inputs"][0];
        let probes: Vec<&str> = validation["checks"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|check| check["probe"].as_str())
            .collect();
        assert_eq!(
            probes,
            [expected_probe.as_str(), expected_probe.as_str()],
            "key={key}, rows={rows:?}"
        );
        let decision = explain
            .statistics
            .as_ref()
            .unwrap()
            .iter()
            .find(|stat| stat.statistic == "endpoint_probe")
            .expect("the probe decision is recorded");
        assert_eq!(decision.value, expected_probe.as_str());
        let origin: serde_json::Value = serde_json::from_str(&decision.origin).unwrap();
        assert_eq!(origin["endpoints"].as_array().unwrap().len(), 2);
        assert_eq!(
            origin["endpoints"][0]["integer_key"],
            key != "String",
            "key={key}"
        );
    }
}

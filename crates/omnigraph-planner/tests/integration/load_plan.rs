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
    PhysicalPlan, RouteOverride, Side, SideId, TableRef, Unrouted, ValidateAccess, ValueTable,
    route,
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
    assert_eq!(document["explain_version"], 6);
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
    let leaf = &validation["inputs"][0];
    assert_eq!(leaf["node"], "CsvSource");
    assert_eq!(leaf["source"], "s3://bucket/people.csv");
    assert!(document["pipelines"].is_null());
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

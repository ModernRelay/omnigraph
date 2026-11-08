//! A write the executor stages (a JSONL load, a mutation, a blob cell, a
//! branch merge) resolves one table at a time to `DeltaSource → Validate`
//! and routes to the executor with the decided physical plan. Rust and not
//! `.gqt`: the claims are the node chain, the check derivation from the
//! write's shape and the route decision. GQT owns the rendered access
//! choices on stored tables.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::ir::{DeltaClass, WriteKind, lower_write_table};
use omnigraph_compiler::query::ast::LoadTarget;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{SYSTEM_COLUMNS_V3, build_catalog};
use omnigraph_planner::optimizer::resolve;
use omnigraph_planner::{
    CommittedSide, Decision, FragmentCoverage, IndexFact, IndexKind, LogicalNode, MemorySource,
    NodeTypeSpec, Operation, PhysicalNode, PhysicalPlan, RouteOverride, Side, SideId, TableRef,
    Unrouted, ValidateAccess, route,
};

use crate::fixture_bounds::BOUNDS;

const PERSON_KNOWS: &str = r#"
node Person {
    id: String @key
    email: String @unique
}
edge Knows: Person -> Person {
    w: I64
}
"#;

const PERSON_KNOWS_CARD: &str = r#"
node Person {
    id: String @key
}
edge Knows: Person -> Person @card(1..) {
    w: I64
}
"#;

fn write(target: LoadTarget, delta: DeltaClass, deletes: bool, rows: u64) -> Operation {
    write_with(
        PERSON_KNOWS,
        WriteKind::Mutation,
        target,
        delta,
        deletes,
        rows,
    )
}

fn write_with(
    schema: &str,
    kind: WriteKind,
    target: LoadTarget,
    delta: DeltaClass,
    deletes: bool,
    rows: u64,
) -> Operation {
    let schema = parse_schema(schema).expect("schema parses");
    let catalog = build_catalog(&schema).expect("catalog builds");
    Operation::Write {
        ir: Box::new(lower_write_table(&catalog, target, delta, deletes, rows)),
        kind,
        tables: vec![side("node:Person", 7), side("edge:Knows", 3)],
        committed: CommittedSide::Live,
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

/// `source()` with the committed Person table's row count and one `__id`
/// BTREE covering `covered` of `total` fragments.
fn source_with_person_index(rows: u64, covered: u64, total: u64) -> MemorySource {
    let schema = parse_schema(PERSON_KNOWS).unwrap();
    let catalog = build_catalog(&schema).unwrap();
    let node = catalog.node_types.get("Person").unwrap();
    source()
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
                row_count: Some(rows),
            },
        )
        .with_index_facts(
            "node:Person",
            vec![IndexFact {
                name: "id_index".into(),
                column: SYSTEM_COLUMNS_V3.id.into(),
                kind: IndexKind::Btree { usable: true },
                coverage: Some(FragmentCoverage { covered, total }),
            }],
        )
}

/// The executor decision of a write with its physical plan.
fn planned(op: &Operation, source: &MemorySource) -> (PhysicalPlan, omnigraph_planner::Explain) {
    match route(op, source, RouteOverride::Registry, &BOUNDS) {
        Decision::Executor {
            reason,
            explain,
            physical: Some(physical),
            ..
        } => {
            let Operation::Write { kind, .. } = op else {
                unreachable!()
            };
            assert_eq!(
                reason,
                Unrouted::WriteOnExecutor {
                    kind: kind.as_str()
                }
            );
            (physical, explain)
        }
        other => panic!("a write routes to the executor with its plan, decided {other:?}"),
    }
}

/// Every check of the plan's one validation with its physical access.
fn accesses(plan: &PhysicalPlan) -> Vec<(String, ValidateAccess)> {
    let PhysicalNode::Validate { checks, .. } = plan.node(plan.root()).expect("live root") else {
        panic!("a write plan's root is its Validate node");
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
fn an_upsert_of_nodes_resolves_to_delta_source_and_validate() {
    let op = write(
        LoadTarget::Node("Person".into()),
        DeltaClass::Upsert,
        false,
        3,
    );
    let logical = resolve(&op, &source()).expect("write resolves");
    let mut id = Some(logical.root());
    let mut kinds = Vec::new();
    while let Some(current) = id {
        let node = logical.node(current).expect("live node");
        kinds.push(match node {
            LogicalNode::Validate {
                table,
                checks,
                replaced,
                ..
            } => {
                assert_eq!(table, "node:Person");
                assert!(!replaced, "an upsert keeps the table");
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
            LogicalNode::DeltaSource { spec } => {
                assert_eq!(spec.table, "node:Person");
                assert_eq!(spec.version, Some(7));
                assert_eq!(spec.rows, 3);
                assert_eq!(spec.delta, DeltaClass::Upsert);
                assert!(!spec.deletes);
                "DeltaSource".to_string()
            }
            other => panic!("unexpected node {:?}", other.kind()),
        });
        id = node.inputs().first().copied();
    }
    assert_eq!(
        kinds,
        [
            "Validate(value None, unique None, unique Some(\"node:Person@Some(7)\"))",
            "DeltaSource",
        ]
    );
    let (plan, explain) = planned(&op, &source());
    assert_eq!(
        accesses(&plan),
        [
            ("value".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Scan),
        ],
        "a kept table's lookup scans without index facts"
    );
    let document = explain.to_value();
    assert_eq!(document["explain_version"], 8);
    assert_eq!(document["route"], "executor");
    assert_eq!(document["reason"]["kind"], "write_on_executor");
    assert_eq!(document["reason"]["write"], "mutation");
    assert_eq!(document["operation"]["kind"], "write");
    assert_eq!(document["operation"]["write"], "mutation");
    assert_eq!(document["operation"]["committed"], "live");
    assert_eq!(document["physical_plan"]["node"], "Validate");
    assert_eq!(
        document["physical_plan"]["inputs"][0]["node"],
        "DeltaSource"
    );
    assert_eq!(document["physical_plan"]["inputs"][0]["rows"], 3);
    assert_eq!(document["physical_plan"]["inputs"][0]["delta"], "upsert");
}

#[test]
fn edge_inserts_probe_their_endpoints_only_under_full_btree_coverage() {
    let op = write(
        LoadTarget::Edge("Knows".into()),
        DeltaClass::Insert,
        false,
        5,
    );
    for (source, expected) in [
        (source(), ValidateAccess::Scan),
        (
            source_with_person_index(100_000, 4, 4),
            ValidateAccess::Probe,
        ),
        (
            source_with_person_index(100_000, 3, 4),
            ValidateAccess::Scan,
        ),
    ] {
        let (plan, _) = planned(&op, &source);
        assert_eq!(
            accesses(&plan),
            [
                ("value".to_string(), ValidateAccess::Delta),
                ("edge_ri".to_string(), expected),
                ("edge_ri".to_string(), expected),
            ]
        );
    }
}

#[test]
fn a_delete_only_node_table_judges_its_retained_edges() {
    let op = write(
        LoadTarget::Node("Person".into()),
        DeltaClass::Removal,
        true,
        0,
    );
    let (plan, explain) = planned(&op, &source());
    assert_eq!(
        accesses(&plan),
        [
            ("retained_edges".to_string(), ValidateAccess::Probe),
            ("retained_edges".to_string(), ValidateAccess::Probe),
        ],
        "no rows, so no value or uniqueness check; the removed ids may strand committed edges"
    );
    let document = explain.to_value();
    let checks = &document["physical_plan"]["checks"];
    assert_eq!(checks[0]["edge_table"], "edge:Knows");
    assert_eq!(checks[0]["endpoint"], "from");
    assert_eq!(checks[0]["consults"]["table"], "edge:Knows");
    assert_eq!(checks[0]["consults"]["version"], 3);
    assert_eq!(checks[1]["endpoint"], "to");
    assert_eq!(document["physical_plan"]["inputs"][0]["delta"], "removal");
    assert_eq!(document["physical_plan"]["inputs"][0]["deletes"], true);
}

#[test]
fn an_edge_table_with_deletes_recounts_cardinality() {
    let op = write_with(
        PERSON_KNOWS_CARD,
        WriteKind::Mutation,
        LoadTarget::Edge("Knows".into()),
        DeltaClass::Removal,
        true,
        0,
    );
    let (plan, _) = planned(&op, &source());
    assert_eq!(
        accesses(&plan),
        [("cardinality".to_string(), ValidateAccess::Probe)],
        "an edge delete under a `@card` minimum recounts the sources it empties"
    );
    let kept = write_with(
        PERSON_KNOWS_CARD,
        WriteKind::Mutation,
        LoadTarget::Edge("Knows".into()),
        DeltaClass::Insert,
        false,
        2,
    );
    let (plan, _) = planned(&kept, &source());
    assert_eq!(
        accesses(&plan),
        [
            ("value".to_string(), ValidateAccess::Delta),
            ("edge_ri".to_string(), ValidateAccess::Scan),
            ("edge_ri".to_string(), ValidateAccess::Scan),
            ("cardinality".to_string(), ValidateAccess::Probe),
        ]
    );
}

#[test]
fn a_replaced_table_plans_input_only_unique_checks_and_a_scan_removal() {
    let op = write_with(
        PERSON_KNOWS,
        WriteKind::Load,
        LoadTarget::Node("Person".into()),
        DeltaClass::Replace,
        false,
        1024,
    );
    let (plan, explain) = planned(&op, &source());
    assert_eq!(
        accesses(&plan),
        [
            ("value".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Delta),
            ("unique".to_string(), ValidateAccess::Delta),
            ("retained_edges".to_string(), ValidateAccess::Probe),
            ("retained_edges".to_string(), ValidateAccess::Probe),
        ],
        "a replaced table's `@unique` reads no committed image"
    );
    let document = explain.to_value();
    assert_eq!(document["operation"]["write"], "load");
    assert_eq!(document["physical_plan"]["removal"], "scan");
}

//! Registry admission guards and routing boundaries. Unregistered diff and
//! merge shapes stay on the executor under every override; read queries
//! build an engine plan independently of registry membership.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::{IRExpr, IROp, IRProjection, QueryIR};
use omnigraph_planner::optimizer::resolve;
use omnigraph_planner::registry::{COVERAGE, Coverage, REGISTRY, Route, lookup};
use omnigraph_planner::{
    AdjacencyProof, Bounds, Census, Decision, JoinKind, LogicalKind, MemorySource, NodeTypeSpec,
    Operation, PageBudgetSpec, RouteOverride, ScopeSpec, Side, SideId, TableRef, Unrouted, route,
};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn crate_dir(crate_name: &str) -> &'static str {
    match crate_name {
        "omnigraph-engine" => "crates/omnigraph",
        "omnigraph-planner" => "crates/omnigraph-planner",
        other => panic!("evidence names an unknown crate `{other}`"),
    }
}

/// The five `for entry in REGISTRY` guards below run no iteration while this
/// holds. The first entry makes this test red: replace it then with a guard
/// that names the entry, so the loops are known to execute.
#[test]
fn the_registry_is_empty_and_its_entry_guards_run_no_iteration() {
    assert!(REGISTRY.is_empty(), "an entry landed: {:?}", REGISTRY);
}

#[test]
fn every_evidence_path_exists_and_is_not_ignored() {
    for entry in REGISTRY {
        assert!(
            !entry.evidence.is_empty(),
            "entry `{}` names no evidence",
            entry.name
        );
        for evidence in entry.evidence {
            let mut parts = evidence.splitn(3, "::");
            let crate_name = parts.next().expect("crate");
            let file = parts.next().expect("file");
            let function = parts.next().expect("function");
            assert!(
                !file.ends_with(".gqt"),
                "entry `{}` names a GQT case as evidence: {evidence}",
                entry.name
            );
            let path = workspace_root().join(crate_dir(crate_name)).join(file);
            let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("entry `{}` evidence {evidence}: {error}", entry.name)
            });
            let needle = format!("fn {function}(");
            let position = text.find(&needle).unwrap_or_else(|| {
                panic!(
                    "entry `{}` evidence {evidence}: function not found",
                    entry.name
                )
            });
            let attributes = test_attributes(&text[..position]);
            assert!(
                attributes
                    .iter()
                    .any(|line| *line == "#[test]" || line.starts_with("#[tokio::test")),
                "entry `{}` evidence {evidence} is not a test",
                entry.name
            );
            assert!(
                !attributes.iter().any(|line| line.starts_with("#[ignore")),
                "entry `{}` evidence {evidence} is #[ignore]d",
                entry.name
            );
        }
    }
}

fn test_attributes(preceding: &str) -> Vec<&str> {
    let declaration_line = preceding.rfind('\n').map_or(0, |position| position + 1);
    preceding[..declaration_line]
        .lines()
        .rev()
        .map(str::trim)
        .take_while(|line| line.is_empty() || line.starts_with("#[") || line.starts_with("///"))
        .filter(|line| line.starts_with("#["))
        .collect()
}

/// GQT cannot express Rust source attributes on registry evidence functions.
#[test]
fn evidence_attributes_belong_to_the_named_function() {
    assert_eq!(test_attributes("#[test]\n"), vec!["#[test]"]);
    assert_eq!(
        test_attributes("#[tokio::test]\nasync "),
        vec!["#[tokio::test]"]
    );
    assert!(test_attributes("#[test]\nfn earlier() {}\n").is_empty());
    assert_eq!(
        test_attributes("#[test]\n#[ignore = \"heavy-repro: x\"]\n").len(),
        2
    );
}

#[test]
fn entries_are_pairwise_disjoint_and_fixtures_match_exactly() {
    for (index, entry) in REGISTRY.iter().enumerate() {
        for other in &REGISTRY[index + 1..] {
            assert!(
                entry.shape != other.shape,
                "entries `{}` and `{}` share a shape",
                entry.name,
                other.name
            );
        }
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(format!("{}.shape", entry.name));
        let expected = std::fs::read_to_string(&fixture).unwrap_or_else(|error| {
            panic!(
                "entry `{}` fixture {}: {error}",
                entry.name,
                fixture.display()
            )
        });
        assert_eq!(
            expected.trim(),
            entry.shape.census().to_string(),
            "entry `{}` matches a shape its fixture does not list",
            entry.name
        );
        assert_eq!(
            lookup(&entry.shape.census()).map(|found| found.name),
            Some(entry.name)
        );
    }
}

#[test]
fn every_negative_case_reaches_the_executor() {
    for entry in REGISTRY {
        assert_ne!(
            entry.negative_case, entry.shape,
            "entry `{}` negative case is its own shape",
            entry.name
        );
        assert!(
            lookup(&entry.negative_case.census()).is_none(),
            "entry `{}` negative case {} is routed",
            entry.name,
            entry.negative_case.census()
        );
    }
}

#[test]
fn every_logical_kind_is_in_the_coverage_list_once() {
    for kind in LogicalKind::ALL {
        let count = COVERAGE
            .iter()
            .filter(|(candidate, _)| *candidate == kind)
            .count();
        assert_eq!(
            count,
            1,
            "logical kind {} appears {count} times in the coverage list",
            kind.name()
        );
    }
}

#[test]
fn behind_flag_entries_are_not_stale() {
    let current = env!("CARGO_PKG_VERSION");
    let (current_major, current_minor) = major_minor(current);
    for entry in REGISTRY {
        if entry.route == Route::PlannerBehindFlag {
            let (since_major, since_minor) = major_minor(entry.since);
            assert!(
                current_major == since_major
                    && since_minor <= current_minor
                    && current_minor - since_minor <= 2,
                "entry `{}` has sat behind the flag since {} (now {current})",
                entry.name,
                entry.since
            );
        }
    }
}

fn major_minor(version: &str) -> (u64, u64) {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse().ok())
        .expect("semver major");
    let minor = parts
        .next()
        .and_then(|part| part.parse().ok())
        .expect("semver minor");
    (major, minor)
}

fn side(version: u64) -> Side {
    Side {
        table: TableRef {
            type_key: "node:Doc".to_string(),
            dataset_path: "tables/node_Doc".to_string(),
            native_branch: None,
        },
        version,
        columns: SystemColumns {
            id: "id",
            src: "src",
            dst: "dst",
        },
    }
}

fn commit_diff() -> Operation {
    Operation::CommitDiff {
        parent: side(1),
        child: side(2),
        scope: ScopeSpec {
            inserts: true,
            updates: true,
            deletes: true,
        },
        resume: None,
        budget: PageBudgetSpec {
            rows: 10,
            bytes: 1024,
        },
    }
}

fn source(proof: bool) -> MemorySource {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let source = MemorySource::default()
        .with_schema(SideId::Parent, schema.clone())
        .with_schema(SideId::Child, schema);
    if proof {
        source.with_proof(AdjacencyProof {
            child_fragments: vec![1],
            parent_fragments: vec![0],
            version_window: Some((1, 2)),
        })
    } else {
        source
    }
}

#[path = "support/bounds.rs"]
mod fixture_bounds;
use fixture_bounds::BOUNDS;

/// The engine has no diff/merge integration to execute a routed plan.
#[test]
fn unregistered_diff_and_merge_shapes_stay_on_the_executor_under_every_override() {
    let merge = Operation::MergeClassify {
        base: side(1),
        source: side(2),
        target: side(3),
    };
    let snapshot_diff = Operation::SnapshotDiff {
        from: side(1),
        to: side(3),
    };
    for (op, proof) in [
        (commit_diff(), false),
        (commit_diff(), true),
        (snapshot_diff, false),
        (merge, false),
    ] {
        let source = source(proof).with_schema(
            SideId::Base,
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("payload", DataType::Utf8, true),
            ])),
        );
        let census = resolve(&op, &source).expect("operation resolves").census();
        for override_ in [
            RouteOverride::Registry,
            RouteOverride::ForceExecutor,
            RouteOverride::ForcePlanner,
        ] {
            let decision = route(&op, &source, override_, &BOUNDS);
            let Decision::Executor {
                reason,
                explain,
                logical,
            } = decision
            else {
                panic!("unregistered {} routed under {override_:?}", op.kind());
            };
            assert_eq!(
                reason,
                Unrouted::NoMatchingEntry {
                    census: census.clone(),
                },
                "{} under {override_:?}",
                op.kind()
            );
            assert_eq!(explain.route, "executor");
            assert!(explain.entry.is_none());
            assert!(explain.physical_plan.is_none());
            assert!(logical.node(logical.root()).is_some());
            assert!(explain.logical_plan.is_some());
        }
    }
}

fn doc_source() -> MemorySource {
    let side = side(1);
    MemorySource::default().with_node_type(
        "Doc",
        NodeTypeSpec {
            table: side.table,
            version: Some(side.version),
            columns: side.columns,
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            key: Vec::new(),
            object_columns: vec!["id".to_string()],
            row_count: None,
        },
    )
}

fn documents_query() -> Operation {
    Operation::Query(Box::new(QueryIR {
        name: "documents".to_string(),
        params: Vec::new(),
        pipeline: vec![IROp::NodeScan {
            variable: "d".to_string(),
            type_name: "Doc".to_string(),
            filters: Vec::new(),
        }],
        return_exprs: vec![IRProjection {
            expr: IRExpr::Variable("d".to_string()),
            alias: None,
        }],
        order_by: Vec::new(),
        limit: None,
    }))
}

/// GQT sees rows and plan assertions, but cannot select registry overrides.
#[test]
fn read_queries_build_an_engine_plan_without_a_registry_entry() {
    let source = doc_source();
    let op = documents_query();
    for override_ in [
        RouteOverride::Registry,
        RouteOverride::ForceExecutor,
        RouteOverride::ForcePlanner,
    ] {
        let Decision::Engine { plan, explain, .. } = route(&op, &source, override_, &BOUNDS) else {
            panic!("read query did not build an engine plan under {override_:?}");
        };
        let Operation::Query(query) = &op else {
            unreachable!()
        };
        let execution =
            omnigraph_planner::plan_query(query, &source, &BOUNDS).expect("execution plan");
        assert_eq!(execution.to_json(), plan.to_json());
        assert!(plan.node(plan.root()).is_some());
        assert_eq!(explain.route, "engine");
        assert!(explain.entry.is_none());
        assert!(explain.physical_plan.is_some());
    }
}

fn physical_ids(node: &serde_json::Value, out: &mut Vec<u64>) {
    out.push(node["id"].as_u64().expect("a physical node carries its id"));
    for input in node["inputs"].as_array().into_iter().flatten() {
        physical_ids(input, out);
    }
}

/// Pins a wire constant and a wire key that no query result shows: readers
/// refuse any `explain_version` but 2 and join report rows on the node `id`.
#[test]
fn explain_version_is_two_and_every_physical_node_carries_its_id() {
    assert_eq!(omnigraph_planner::explain::EXPLAIN_VERSION, 2);
    let Decision::Engine { plan, explain, .. } = route(
        &documents_query(),
        &doc_source(),
        RouteOverride::Registry,
        &BOUNDS,
    ) else {
        panic!("read query did not build an engine plan");
    };
    let document = explain.to_value();
    assert_eq!(document["explain_version"], 2);
    let mut ids = Vec::new();
    physical_ids(&document["physical_plan"], &mut ids);
    ids.sort_unstable();
    let mut planned: Vec<u64> = plan.post_order().into_iter().map(|id| id as u64).collect();
    planned.sort_unstable();
    assert_eq!(ids, planned);
}

/// Census rendering is a planner diagnostic, not a GQ result surface.
#[test]
fn census_display_preserves_join_kinds_after_other_nodes() {
    let census = Census {
        kinds: vec![
            LogicalKind::Aggregate,
            LogicalKind::Filter,
            LogicalKind::Join,
            LogicalKind::Join,
            LogicalKind::TableScan,
        ],
        joins: vec![JoinKind::FullOuter, JoinKind::Cross],
    };
    assert_eq!(
        census.to_string(),
        "[Aggregate, Filter, Join(FullOuter), Join(Cross), TableScan]"
    );
}

#[test]
fn refused_by_name_kinds_never_match_an_entry() {
    for (kind, coverage) in COVERAGE {
        if *coverage != Coverage::RefusedByName {
            continue;
        }
        for entry in REGISTRY {
            assert!(
                !entry.shape.kinds.contains(kind),
                "entry `{}` names refused-by-name kind {}",
                entry.name,
                kind.name()
            );
        }
    }
}

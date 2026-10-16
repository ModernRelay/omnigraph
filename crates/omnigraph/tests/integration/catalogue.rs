//! The engine catalogue scope needs no store, and a plan over a catalogue
//! table pins the binary's fingerprint instead of a dataset. Rust and not
//! `.gqt`: a case always opens a store, and no case reaches the replay door.

use omnigraph::catalogue::CatalogueScope;
use omnigraph::db::Omnigraph;
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::settings::SessionSettings;

use crate::helpers::*;

const OPERATORS: &str = r#"query operators() {
    match { $o: @Operator }
    return { $o.name, $o.inputs }
    order { $o.name }
}"#;

#[tokio::test]
async fn catalogue_scope_answers_every_type_without_a_store() {
    let scope = CatalogueScope::new(SessionSettings::default()).unwrap();
    let params = ParamMap::new();
    let operators = scope.query(OPERATORS, "operators", &params).await.unwrap();
    assert_eq!(
        operators.num_rows(),
        omnigraph_planner::operator_rows().len()
    );
    let engine = scope
        .query(
            "query engine() { match { $e: @Engine } return { $e.version } }",
            "engine",
            &params,
        )
        .await
        .unwrap();
    assert_eq!(engine.num_rows(), 1);
    let explain = scope
        .explain(OPERATORS, "operators", &params)
        .await
        .unwrap();
    assert!(
        explain.to_string().contains("\"source\":\"catalogue\""),
        "{explain}"
    );
    let explained = scope
        .query(&format!("explain {OPERATORS}"), "operators", &params)
        .await
        .unwrap();
    assert!(explained.num_rows() > 0);
    for (block, expected_rows) in [
        ("not { $p: @Operator $p.name = $o.name }", 0),
        (
            "count { $p: @Operator $p.name = $o.name } = 1",
            operators.num_rows(),
        ),
    ] {
        let source = format!(
            "query correlated() {{ match {{ $o: @Operator {block} }} return {{ $o.name }} }}"
        );
        let result = scope.query(&source, "correlated", &params).await.unwrap();
        assert_eq!(result.num_rows(), expected_rows, "{source}");
    }
    let refused = scope
        .query(
            "query people() { match { $p: Person } return { $p.name } }",
            "people",
            &params,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("unknown node type `Person`"), "{refused}");
    assert_eq!(
        scope.schema_source(),
        omnigraph_compiler::catalogue_declarations()
    );
}

#[tokio::test]
async fn catalogue_replay_requires_fingerprints_and_executable_scans() {
    use omnigraph_planner::PhysicalNode;
    use omnigraph_planner::logical::{ColumnRef, GqFilter, Predicate};

    let dir = tempfile::tempdir().unwrap();
    let db = session(
        Omnigraph::init(
            dir.path().to_str().unwrap(),
            "node Person { name: String @key }\n",
        )
        .await
        .unwrap(),
    );
    let params = ParamMap::new();
    let first = db
        .query_inspected("main", OPERATORS, "operators", &params)
        .await
        .unwrap();
    let assumptions = first.plan.plan.assumptions();
    assert!(assumptions.catalogue.contains_key("node:@Operator"));
    assert!(assumptions.datasets.is_empty());
    let envelope = |plan| {
        omnigraph_planner::ReplayEnvelope::new(
            OPERATORS,
            "operators",
            plan,
            &first.evidence,
            first.catalog.clone(),
        )
        .to_bytes()
    };
    let replay = db
        .replay_bound_plan("main", &envelope(first.plan.clone()))
        .await
        .unwrap();
    assert_eq!(
        replay.result.to_rust_json().unwrap(),
        first.result.to_rust_json().unwrap()
    );
    for (fingerprint, expected) in [
        (Some("sha256:0000"), "catalogue fingerprint"),
        (None, "no recorded catalogue fingerprint"),
    ] {
        let mut altered = first.plan.clone();
        let mut assumptions = altered.plan.assumptions().clone();
        match fingerprint {
            Some(value) => {
                assumptions
                    .catalogue
                    .insert("node:@Operator".to_string(), value.to_string());
            }
            None => {
                assert!(assumptions.catalogue.remove("node:@Operator").is_some());
            }
        }
        altered.plan.set_assumptions(assumptions);
        let error = match db.replay_bound_plan("main", &envelope(altered)).await {
            Ok(_) => panic!("an altered catalogue fingerprint must refuse replay"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "{error}");
        assert!(error.contains("node:@Operator"), "{error}");
    }

    let query = r#"query selected_operator() {
        match { $o: @Operator $o.name = "Scan" }
        return { $o.name }
    }"#;
    let filtered = db
        .query_inspected("main", query, "selected_operator", &params)
        .await
        .unwrap();
    let expected = serde_json::json!([{"o.name": "Scan"}]);
    assert_eq!(filtered.result.to_rust_json().unwrap(), expected);
    let envelope = |plan| {
        omnigraph_planner::ReplayEnvelope::new(
            query,
            "selected_operator",
            plan,
            &filtered.evidence,
            filtered.catalog.clone(),
        )
        .to_bytes()
    };
    let replay = db
        .replay_bound_plan("main", &envelope(filtered.plan.clone()))
        .await
        .unwrap();
    assert_eq!(replay.result.to_rust_json().unwrap(), expected);
    let mut altered = filtered.plan.clone();
    let (filter_id, scan_id, conjunct) = altered
        .plan
        .live()
        .find_map(|(id, node)| match node {
            PhysicalNode::Filter { input, filters } => {
                assert_eq!(filters.len(), 1);
                Some((id, *input, filters[0].clone()))
            }
            _ => None,
        })
        .expect("the fresh plan retains the scalar predicate in a Filter");
    match altered.plan.node_mut(scan_id).unwrap() {
        PhysicalNode::Scan { spec, .. } => {
            assert!(spec.table.is_catalogue());
            spec.filter = Some(Predicate::Gq {
                reads: vec![ColumnRef::property("o", "name")],
                text: conjunct.to_string(),
                filter: GqFilter(conjunct),
            });
        }
        _ => panic!("the Filter must read the catalogue scan"),
    }
    altered.plan.splice_out(filter_id, scan_id);
    let error = match db.replay_bound_plan("main", &envelope(altered)).await {
        Ok(_) => panic!("a pushed catalogue predicate must refuse replay"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("pushed-down predicate"), "{error}");
    assert!(error.contains("node:@Operator"), "{error}");
}

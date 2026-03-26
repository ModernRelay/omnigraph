#![cfg(feature = "failpoints")]

mod helpers;

use fail::FailScenario;
use omnigraph::db::Omnigraph;
use omnigraph::failpoints::ScopedFailPoint;

use helpers::{MUTATION_QUERIES, mixed_params};

#[tokio::test]
async fn branch_create_failpoint_triggers() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, helpers::TEST_SCHEMA).await.unwrap();
    let _failpoint = ScopedFailPoint::new("branch_create.after_manifest_branch_create", "return");

    let err = db.branch_create("feature").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: branch_create.after_manifest_branch_create")
    );
}

#[tokio::test]
async fn graph_publish_failpoint_triggers_before_commit_append() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let mut db = Omnigraph::init(dir.path().to_str().unwrap(), helpers::TEST_SCHEMA)
        .await
        .unwrap();
    let _failpoint = ScopedFailPoint::new("graph_publish.before_commit_append", "return");

    let err = db
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("injected failpoint triggered: graph_publish.before_commit_append")
    );
}
